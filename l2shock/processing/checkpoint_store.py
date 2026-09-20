# l2shock/processing/checkpoint_store.py
"""Deterministic content-addressed checkpoint storage and discovery."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Final

from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.ingest.checkpoint_codec import (
    CHECKPOINT_FORMAT_VERSION,
    CheckpointCodecError,
    CheckpointEncodingInfo,
    checkpoint_encoding_info,
    encode_checkpoint,
    load_checkpoint_file,
    write_checkpoint_file,
)
from l2shock.ingest.replay import (
    OrderBookCheckpoint,
    ReplayContractError,
)
from l2shock.processing.errors import (
    CheckpointConflictError,
    CheckpointStoreError,
    ProcessingContractError,
)
from l2shock.processing.models import (
    ProcessingCancellationProbe,
    raise_if_processing_cancelled,
)
from l2shock.processing.source_repository import (
    ProcessingSourceArchive,
    ProcessingSourceRepository,
    SQLAlchemyProcessingSourceRepository,
)
from l2shock.timeutils import require_utc_hour

_CHECKPOINT_FILENAME_RE: Final[re.Pattern[str]] = re.compile(
    r"^checkpoint-v1-([0-9a-f]{64})\.l2checkpoint$"
)


def _validated_identity(
    name: str,
    value: object,
    *,
    uppercase: bool = False,
) -> str:
    text = str(value or "").strip()

    if uppercase:
        text = text.upper()
    else:
        text = text.lower()

    if not text:
        raise ProcessingContractError(f"{name} cannot be blank")

    if "/" in text or "\\" in text or text in {".", ".."} or ".." in text:
        raise ProcessingContractError(
            f"{name} is unsafe for checkpoint path construction"
        )

    return text


@dataclass(frozen=True, slots=True)
class CheckpointIdentity:
    """One exact checkpoint ownership identity."""

    provider: str
    venue: str
    instrument: str
    through_hour_utc: datetime
    format_version: int = CHECKPOINT_FORMAT_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider",
            _validated_identity(
                "provider",
                self.provider,
            ),
        )
        object.__setattr__(
            self,
            "venue",
            _validated_identity(
                "venue",
                self.venue,
            ),
        )
        object.__setattr__(
            self,
            "instrument",
            _validated_identity(
                "instrument",
                self.instrument,
                uppercase=True,
            ),
        )
        object.__setattr__(
            self,
            "through_hour_utc",
            require_utc_hour(
                "through_hour_utc",
                self.through_hour_utc,
            ),
        )

        if (
            isinstance(self.format_version, bool)
            or not isinstance(self.format_version, int)
            or self.format_version <= 0
        ):
            raise ProcessingContractError("format_version must be a positive integer")

        if self.format_version != CHECKPOINT_FORMAT_VERSION:
            raise ProcessingContractError(
                "Only the current checkpoint format version is supported"
            )

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: OrderBookCheckpoint,
    ) -> CheckpointIdentity:
        if not isinstance(checkpoint, OrderBookCheckpoint):
            raise TypeError("checkpoint must be an OrderBookCheckpoint")

        return cls(
            provider=checkpoint.provider,
            venue=checkpoint.venue,
            instrument=checkpoint.symbol,
            through_hour_utc=checkpoint.through_hour_utc,
        )


@dataclass(frozen=True, slots=True)
class CheckpointArtifact:
    """One verified immutable checkpoint artifact."""

    identity: CheckpointIdentity
    path: Path
    checkpoint: OrderBookCheckpoint
    encoding_info: CheckpointEncodingInfo

    def __post_init__(self) -> None:
        if not isinstance(self.identity, CheckpointIdentity):
            raise ProcessingContractError("identity must be a CheckpointIdentity")

        path = Path(self.path).expanduser().resolve()
        object.__setattr__(self, "path", path)

        if not isinstance(self.checkpoint, OrderBookCheckpoint):
            raise ProcessingContractError("checkpoint must be an OrderBookCheckpoint")

        if CheckpointIdentity.from_checkpoint(self.checkpoint) != self.identity:
            raise ProcessingContractError(
                "Checkpoint artifact identity does not match its checkpoint"
            )

        if not isinstance(
            self.encoding_info,
            CheckpointEncodingInfo,
        ):
            raise ProcessingContractError(
                "encoding_info must be CheckpointEncodingInfo"
            )


class CheckpointSearchStopReason(StrEnum):
    """Why bounded backward checkpoint discovery stopped."""

    CHECKPOINT_FOUND = "checkpoint_found"
    SOURCE_GAP = "source_gap"
    SEARCH_BOUND_REACHED = "search_bound_reached"
    PREDECESSOR_UNINITIALIZED = "predecessor_uninitialized"


@dataclass(frozen=True, slots=True)
class CheckpointSearchPlan:
    """Read-only result of bounded checkpoint/source-chain discovery."""

    target: SourceFileSpec
    replay_sources: tuple[ProcessingSourceArchive, ...]
    inspected_checkpoint_hours: tuple[datetime, ...]
    stop_reason: CheckpointSearchStopReason
    checkpoint: CheckpointArtifact | None = None
    missing_source_hour_utc: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.target, SourceFileSpec):
            raise ProcessingContractError("target must be a SourceFileSpec")

        if self.target.data_kind is not SourceDataKind.ORDERBOOK:
            raise ProcessingContractError(
                "Checkpoint search requires an orderbook target"
            )

        sources = tuple(self.replay_sources)
        object.__setattr__(self, "replay_sources", sources)

        if not sources:
            raise ProcessingContractError(
                "Checkpoint search plan must contain the target source"
            )

        if sources[-1].spec != self.target:
            raise ProcessingContractError(
                "The final replay source must be the target source"
            )

        for previous, current in zip(
            sources,
            sources[1:],
            strict=False,
        ):
            if (
                previous.spec.provider != current.spec.provider
                or previous.spec.venue != current.spec.venue
                or previous.spec.symbol != current.spec.symbol
                or previous.spec.data_kind is not current.spec.data_kind
            ):
                raise ProcessingContractError(
                    "Replay sources must share one exact source identity"
                )

            if previous.spec.hour_utc + timedelta(hours=1) != current.spec.hour_utc:
                raise ProcessingContractError(
                    "Replay sources must be chronologically contiguous"
                )

        inspected = tuple(
            require_utc_hour(
                "inspected_checkpoint_hour",
                value,
            )
            for value in self.inspected_checkpoint_hours
        )
        object.__setattr__(
            self,
            "inspected_checkpoint_hours",
            inspected,
        )

        try:
            stop_reason = CheckpointSearchStopReason(self.stop_reason)
        except (TypeError, ValueError) as exc:
            raise ProcessingContractError(
                "Unsupported checkpoint search stop reason"
            ) from exc

        object.__setattr__(self, "stop_reason", stop_reason)

        if stop_reason is CheckpointSearchStopReason.CHECKPOINT_FOUND:
            if self.checkpoint is None:
                raise ProcessingContractError(
                    "checkpoint_found requires a checkpoint artifact"
                )

            if self.missing_source_hour_utc is not None:
                raise ProcessingContractError(
                    "checkpoint_found cannot contain a missing source hour"
                )

            try:
                self.checkpoint.checkpoint.validate_for_source(sources[0].spec)
            except ReplayContractError as exc:
                raise ProcessingContractError(
                    "Discovered checkpoint cannot initialize the first " "replay source"
                ) from exc
        else:
            if self.checkpoint is not None:
                raise ProcessingContractError(
                    "A non-found search result cannot contain a checkpoint"
                )

        if stop_reason is CheckpointSearchStopReason.SOURCE_GAP:
            if self.missing_source_hour_utc is None:
                raise ProcessingContractError(
                    "source_gap requires missing_source_hour_utc"
                )

            object.__setattr__(
                self,
                "missing_source_hour_utc",
                require_utc_hour(
                    "missing_source_hour_utc",
                    self.missing_source_hour_utc,
                ),
            )
        elif self.missing_source_hour_utc is not None:
            raise ProcessingContractError(
                "Only source_gap may contain a missing source hour"
            )

    @property
    def initialized_by_checkpoint(self) -> bool:
        return self.checkpoint is not None


class CheckpointStore:
    """Content-addressed immutable checkpoint store.

    Layout:

        <cache_root>/checkpoints/
            <provider>/
                <venue>/
                    <instrument>/
                        YYYY-MM-DD/
                            HH/
                                checkpoint-v1-<content_sha256>.l2checkpoint
    """

    def __init__(self, cache_root: Path) -> None:
        root = Path(cache_root).expanduser().resolve()

        if root.exists() and not root.is_dir():
            raise CheckpointStoreError(
                "Configured cache root exists but is not a directory"
            )

        self._cache_root = root
        self._root = (root / "checkpoints").resolve()

        try:
            self._root.relative_to(self._cache_root)
        except ValueError as exc:
            raise CheckpointStoreError(
                "Checkpoint root escaped the configured cache root"
            ) from exc

    @property
    def cache_root(self) -> Path:
        return self._cache_root

    @property
    def root(self) -> Path:
        return self._root

    def directory_for(
        self,
        identity: CheckpointIdentity,
    ) -> Path:
        if not isinstance(identity, CheckpointIdentity):
            raise TypeError("identity must be a CheckpointIdentity")

        date_part = identity.through_hour_utc.strftime("%Y-%m-%d")
        hour_part = identity.through_hour_utc.strftime("%H")

        result = (
            self._root
            / identity.provider
            / identity.venue
            / identity.instrument
            / date_part
            / hour_part
        ).resolve()

        try:
            result.relative_to(self._root)
        except ValueError as exc:
            raise CheckpointStoreError(
                "Generated checkpoint directory escaped the checkpoint root"
            ) from exc

        return result

    def path_for(
        self,
        identity: CheckpointIdentity,
        content_sha256: str,
    ) -> Path:
        digest = str(content_sha256 or "").strip()

        if (
            digest != digest.lower()
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ProcessingContractError(
                "content_sha256 must be a canonical lowercase SHA-256"
            )

        return self.directory_for(identity) / (
            f"checkpoint-v{identity.format_version}-" f"{digest}.l2checkpoint"
        )

    def _load_artifact(
        self,
        path: Path,
        *,
        expected_identity: CheckpointIdentity,
    ) -> CheckpointArtifact:
        filename_match = _CHECKPOINT_FILENAME_RE.fullmatch(path.name)

        if filename_match is None:
            raise CheckpointStoreError(
                f"Checkpoint filename is not canonical: {path.name!r}"
            )

        filename_digest = filename_match.group(1)

        try:
            checkpoint = load_checkpoint_file(path)
            encoded = encode_checkpoint(checkpoint)
            info = checkpoint_encoding_info(encoded)
        except (CheckpointCodecError, OSError) as exc:
            raise CheckpointStoreError(
                f"Could not validate checkpoint artifact {path}"
            ) from exc

        if info.content_sha256 != filename_digest:
            raise CheckpointStoreError(
                "Checkpoint filename digest does not match checkpoint content"
            )

        actual_identity = CheckpointIdentity.from_checkpoint(checkpoint)

        if actual_identity != expected_identity:
            raise CheckpointStoreError(
                "Checkpoint file is stored under the wrong ownership path"
            )

        canonical_path = self.path_for(
            actual_identity,
            info.content_sha256,
        )

        if path.resolve() != canonical_path.resolve():
            raise CheckpointStoreError("Checkpoint file path is not canonical")

        return CheckpointArtifact(
            identity=actual_identity,
            path=path,
            checkpoint=checkpoint,
            encoding_info=info,
        )

    def find_exact(
        self,
        identity: CheckpointIdentity,
    ) -> CheckpointArtifact | None:
        """Find the one checkpoint for an exact ownership identity.

        Multiple valid artifacts for one ownership identity are treated as a
        conflict, even though their content-addressed filenames differ.
        """
        directory = self.directory_for(identity)

        if not directory.exists():
            return None

        if not directory.is_dir():
            raise CheckpointStoreError("Checkpoint identity path is not a directory")

        checkpoint_files = sorted(
            path for path in directory.iterdir() if path.name.endswith(".l2checkpoint")
        )

        noncanonical = [
            path.name
            for path in checkpoint_files
            if (
                not path.is_file()
                or _CHECKPOINT_FILENAME_RE.fullmatch(path.name) is None
            )
        ]

        if noncanonical:
            raise CheckpointStoreError(
                "Checkpoint identity directory contains noncanonical "
                f"artifacts: {noncanonical}"
            )

        candidates = checkpoint_files

        if not candidates:
            return None

        artifacts = tuple(
            self._load_artifact(
                candidate,
                expected_identity=identity,
            )
            for candidate in candidates
        )

        if len(artifacts) > 1:
            digests = sorted(
                artifact.encoding_info.content_sha256 for artifact in artifacts
            )
            raise CheckpointConflictError(
                "Multiple checkpoint contents exist for one source-hour "
                f"identity: {digests}"
            )

        return artifacts[0]

    def publish(
        self,
        checkpoint: OrderBookCheckpoint,
    ) -> CheckpointArtifact:
        """Idempotently publish one immutable content-addressed checkpoint."""
        if not isinstance(checkpoint, OrderBookCheckpoint):
            raise TypeError("checkpoint must be an OrderBookCheckpoint")

        identity = CheckpointIdentity.from_checkpoint(checkpoint)
        encoded = encode_checkpoint(checkpoint)
        info = checkpoint_encoding_info(encoded)
        destination = self.path_for(
            identity,
            info.content_sha256,
        )

        existing = self.find_exact(identity)

        if existing is not None:
            if existing.encoding_info.content_sha256 != info.content_sha256:
                raise CheckpointConflictError(
                    "A different checkpoint is already published for "
                    "the same source-hour identity"
                )

            return existing

        try:
            write_checkpoint_file(
                destination,
                checkpoint,
                overwrite=False,
            )
        except FileExistsError:
            # Another writer may have published the same content-addressed
            # artifact after the initial lookup. Verify it through the public
            # reader rather than assuming equivalence.
            pass
        except CheckpointCodecError as exc:
            raise CheckpointStoreError("Could not publish checkpoint artifact") from exc

        published = self.find_exact(identity)

        if published is None:
            raise CheckpointStoreError(
                "Checkpoint publication completed without a discoverable " "artifact"
            )

        if published.encoding_info.content_sha256 != info.content_sha256:
            raise CheckpointConflictError(
                "Published checkpoint content conflicts with requested content"
            )

        return published

    def build_search_plan(
        self,
        target: SourceFileSpec,
        source_repository: ProcessingSourceRepository,
        *,
        max_checkpoint_search_hours: int,
        cancellation_probe: ProcessingCancellationProbe | None = None,
    ) -> CheckpointSearchPlan:
        """Build a bounded, read-only predecessor discovery plan.

        The target source must exist. For each preceding hour, discovery first
        checks for a checkpoint through that hour. If no checkpoint exists, the
        corresponding source archive is added to the replay chain.

        Reaching the bound without a checkpoint is not an error. Batch 8A-2 may
        replay the returned contiguous archives and determine whether a source
        snapshot provides initialization.
        """
        if not isinstance(target, SourceFileSpec):
            raise TypeError("target must be a SourceFileSpec")

        if target.data_kind is not SourceDataKind.ORDERBOOK:
            raise ProcessingContractError(
                "Checkpoint search requires an orderbook target"
            )

        if not isinstance(
            source_repository,
            ProcessingSourceRepository,
        ):
            raise TypeError(
                "source_repository must implement " "ProcessingSourceRepository"
            )

        if (
            isinstance(max_checkpoint_search_hours, bool)
            or not isinstance(max_checkpoint_search_hours, int)
            or max_checkpoint_search_hours <= 0
        ):
            raise ProcessingContractError(
                "max_checkpoint_search_hours must be a positive integer"
            )

        raise_if_processing_cancelled(cancellation_probe)

        target_archive = source_repository.find_replayable(target)

        if target_archive is None:
            raise ProcessingContractError(
                "Target source archive is not locally replayable"
            )

        reverse_sources = [target_archive]
        inspected_hours: list[datetime] = []

        for lookback in range(
            1,
            max_checkpoint_search_hours + 1,
        ):
            raise_if_processing_cancelled(cancellation_probe)

            through_hour = target.hour_utc - timedelta(hours=lookback)
            inspected_hours.append(through_hour)

            predecessor_spec = SourceFileSpec(
                provider=target.provider,
                venue=target.venue,
                symbol=target.symbol,
                data_kind=SourceDataKind.ORDERBOOK,
                hour_utc=through_hour,
            )
            identity = CheckpointIdentity(
                provider=target.provider,
                venue=target.venue,
                instrument=target.symbol,
                through_hour_utc=through_hour,
            )
            checkpoint = self.find_exact(identity)

            if checkpoint is not None:
                durable_checkpoint_digest: str | None = None

                if isinstance(
                    source_repository,
                    SQLAlchemyProcessingSourceRepository,
                ):
                    durable_checkpoint_digest = (
                        source_repository
                        .find_durable_output_checkpoint_content_sha256(
                            predecessor_spec
                        )
                    )

                    if durable_checkpoint_digest is None:
                        # The file may be an orphan left by a transaction that
                        # failed after immutable filesystem publication. It is
                        # not authoritative until committed source metadata
                        # owns its exact digest.
                        checkpoint = None
                    elif (
                        checkpoint.encoding_info.content_sha256
                        != durable_checkpoint_digest
                    ):
                        raise CheckpointStoreError(
                            "Checkpoint content SHA-256 does not match durable "
                            "source-hour output ownership"
                        )

            if checkpoint is not None:
                durable_source_digest = source_repository.find_durable_content_sha256(
                    predecessor_spec
                )
                checkpoint_source_digest = checkpoint.checkpoint.source_content_sha256

                if durable_source_digest is None:
                    raise CheckpointStoreError(
                        "Checkpoint source hour has no durable content SHA-256"
                    )

                if checkpoint_source_digest is None:
                    raise CheckpointStoreError(
                        "Production checkpoint has no source content SHA-256"
                    )

                if checkpoint_source_digest != durable_source_digest:
                    raise CheckpointStoreError(
                        "Checkpoint source SHA-256 does not match durable "
                        "source-hour metadata"
                    )

                replay_sources = tuple(reversed(reverse_sources))

                return CheckpointSearchPlan(
                    target=target,
                    replay_sources=replay_sources,
                    inspected_checkpoint_hours=tuple(inspected_hours),
                    stop_reason=CheckpointSearchStopReason.CHECKPOINT_FOUND,
                    checkpoint=checkpoint,
                )

            predecessor = source_repository.find_replayable(predecessor_spec)

            if predecessor is None:
                return CheckpointSearchPlan(
                    target=target,
                    replay_sources=tuple(reversed(reverse_sources)),
                    inspected_checkpoint_hours=tuple(inspected_hours),
                    stop_reason=(CheckpointSearchStopReason.SOURCE_GAP),
                    missing_source_hour_utc=through_hour,
                )

            reverse_sources.append(predecessor)

        return CheckpointSearchPlan(
            target=target,
            replay_sources=tuple(reversed(reverse_sources)),
            inspected_checkpoint_hours=tuple(inspected_hours),
            stop_reason=(CheckpointSearchStopReason.SEARCH_BOUND_REACHED),
        )


__all__ = [
    "CheckpointArtifact",
    "CheckpointIdentity",
    "CheckpointSearchPlan",
    "CheckpointSearchStopReason",
    "CheckpointStore",
]

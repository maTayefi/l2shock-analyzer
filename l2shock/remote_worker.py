# l2shock/remote_worker.py
"""Scheduled/manual remote-hour processing orchestration.

This module composes existing boundaries without reimplementing them:

    shared release schedule
    -> pinned Hugging Face repository inspection
    -> predecessor checkpoint lookup
    -> PostgreSQL-free CryptoHFTData acquisition
    -> shared headless L2/price processing
    -> conflict-safe Hugging Face publication

It deliberately performs no:

- PostgreSQL access;
- NiceGUI work;
- alternative replay implementation;
- alternative downloader implementation;
- analytical resampling;
- raw-source persistence after the worker workspace closes.

The scheduled unit is one venue/instrument/completed UTC hour.

Independent venue/instrument chains may run concurrently. GitHub Actions owns
cross-run chain serialization. This module still verifies predecessor ownership
and immutable artifact identities rather than trusting workflow scheduling.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from pathlib import Path
from typing import Final

from pydantic import SecretStr

from l2shock.acquisition import (
    AcquisitionError,
    RemoteFileNotFoundError,
    SourceDataKind,
    SourceFileSpec,
    latest_release_eligible_hour,
)
from l2shock.config import CryptoHFTConfig
from l2shock.presets import (
    LiquidityDataPreset,
    build_binance_futures_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.processing import (
    ProcessingError,
    ProcessingSourceArchive,
)
from l2shock.remote import (
    HuggingFaceArtifactNotFoundError,
    HuggingFaceDatasetRepository,
    HuggingFacePublicationResult,
    HuggingFaceRepositoryError,
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    RemoteWorkerWorkspace,
    acquire_remote_worker_archives,
    process_l2_archive_headlessly,
    process_price_archives_headlessly,
    temporary_remote_worker_workspace,
)
from l2shock.timeutils import now_utc, require_utc_hour


class RemoteWorkerExitStatus(IntEnum):
    OK = 0
    UNEXPECTED_ERROR = 1
    INPUT_OR_CONTRACT_ERROR = 2
    SOURCE_UNAVAILABLE = 3
    CHECKPOINT_CHAIN_BLOCKED = 4
    HUGGING_FACE_ERROR = 5
    INTERRUPTED = 130


class RemoteWorkerError(RuntimeError):
    """One remote-hour worker operation could not complete safely."""


class RemoteWorkerCheckpointBlockedError(RemoteWorkerError):
    """The target L2 hour cannot safely advance its checkpoint chain."""


@dataclass(frozen=True, slots=True)
class RemoteWorkerResult:
    """JSON-safe summary of one remote-hour operation."""

    venue: str
    instrument: str
    hour_utc: datetime
    pinned_input_revision: str

    l2_created: bool
    l2_revision: str | None
    l2_reused: bool

    price_required: bool
    price_created: bool
    price_revision: str | None
    price_reused: bool

    source_downloaded_count: int
    source_reused_count: int

    def __post_init__(self) -> None:
        venue = str(self.venue or "").strip().lower()
        instrument = str(self.instrument or "").strip().upper()
        hour = require_utc_hour(
            "hour_utc",
            self.hour_utc,
        )

        if not venue or not instrument:
            raise RemoteWorkerError(
                "Remote worker result identity cannot contain blank fields"
            )

        revision = str(self.pinned_input_revision or "").strip().lower()

        if len(revision) not in {40, 64} or any(
            character not in "0123456789abcdef" for character in revision
        ):
            raise RemoteWorkerError("pinned_input_revision must be a full commit SHA")

        for field_name in (
            "l2_created",
            "l2_reused",
            "price_required",
            "price_created",
            "price_reused",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be bool")

        for field_name in (
            "source_downloaded_count",
            "source_reused_count",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RemoteWorkerError(f"{field_name} must be a non-negative integer")

        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "hour_utc", hour)
        object.__setattr__(
            self,
            "pinned_input_revision",
            revision,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "l2shock.remote_worker_result",
            "schema_version": 1,
            "venue": self.venue,
            "instrument": self.instrument,
            "hour_utc": self.hour_utc.isoformat().replace("+00:00", "Z"),
            "pinned_input_revision": self.pinned_input_revision,
            "l2": {
                "created": self.l2_created,
                "reused": self.l2_reused,
                "revision": self.l2_revision,
            },
            "price": {
                "required": self.price_required,
                "created": self.price_created,
                "reused": self.price_reused,
                "revision": self.price_revision,
            },
            "source_acquisition": {
                "downloaded_count": self.source_downloaded_count,
                "reused_count": self.source_reused_count,
            },
        }


@dataclass(frozen=True, slots=True)
class _ExistingRemoteState:
    pinned_revision: str
    l2_artifact: RemoteL2ProcessedArtifact | None
    price_artifact: RemotePriceProcessedArtifact | None


@dataclass(frozen=True, slots=True)
class _RemoteCatchUpObservation:
    """One pinned-revision hour inspected during chain-frontier discovery."""

    hour_utc: datetime
    l2_exists: bool
    output_checkpoint_exists: bool
    price_exists: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "hour_utc",
            require_utc_hour(
                "hour_utc",
                self.hour_utc,
            ),
        )

        for field_name in (
            "l2_exists",
            "output_checkpoint_exists",
            "price_exists",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be bool")

        if self.output_checkpoint_exists and not self.l2_exists:
            raise RemoteWorkerError(
                "A remote output checkpoint cannot exist without its L2 artifact"
            )


_SUPPORTED_CHAINS: Final[dict[tuple[str, str], bool]] = {
    ("binance_futures", "BTCUSDT"): True,
    ("binance_futures", "ETHUSDT"): True,
    ("okx_futures", "BTC-USDT-SWAP"): False,
    ("okx_futures", "ETH-USDT-SWAP"): False,
}


def _canonical_utc_hour(value: str) -> datetime:
    text = str(value or "").strip()

    if not text.endswith("Z"):
        raise argparse.ArgumentTypeError("hour must use canonical UTC text ending in Z")

    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "hour must use canonical ISO-8601 UTC text"
        ) from exc

    try:
        hour = require_utc_hour(
            "hour",
            parsed,
        )
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc

    canonical = hour.isoformat().replace("+00:00", "Z")

    if canonical != text:
        raise argparse.ArgumentTypeError("hour is valid but not canonically encoded")

    return hour


def _positive_integer(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc

    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")

    return result


def _depth_fraction(value: str) -> Decimal:
    text = str(value or "").strip()

    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "depth fraction must be an exact decimal"
        ) from exc

    if not result.is_finite():
        raise argparse.ArgumentTypeError("depth fraction must be finite")

    return result


def _normalized_chain(
    venue: object,
    instrument: object,
) -> tuple[str, str, bool]:
    normalized_venue = str(venue or "").strip().lower()
    normalized_instrument = str(instrument or "").strip().upper()

    try:
        price_required = _SUPPORTED_CHAINS[
            (
                normalized_venue,
                normalized_instrument,
            )
        ]
    except KeyError as exc:
        allowed = ", ".join(
            f"{item_venue}/{item_instrument}"
            for item_venue, item_instrument in sorted(_SUPPORTED_CHAINS)
        )
        raise RemoteWorkerError(
            "Unsupported remote processing chain. " f"Allowed chains: {allowed}"
        ) from exc

    return (
        normalized_venue,
        normalized_instrument,
        price_required,
    )


def _preset_for_chain(
    *,
    venue: str,
    instrument: str,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
) -> LiquidityDataPreset:
    source = SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=instrument,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=datetime(
            2000,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    )

    builders = {
        "binance_futures": build_binance_futures_data_preset,
        "okx_futures": build_okx_futures_data_preset,
    }

    preset = builders[venue](
        base=source.base,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
    )

    market = preset.eligible_markets[0]

    if (
        market.provider != source.provider
        or market.venue != source.venue
        or market.instrument != source.symbol
    ):
        raise RemoteWorkerError(
            "Generated component preset does not match the worker chain"
        )

    return preset


def _source_spec(
    *,
    venue: str,
    instrument: str,
    hour_utc: datetime,
    data_kind: SourceDataKind,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=instrument,
        data_kind=data_kind,
        hour_utc=hour_utc,
    )


def _l2_key(
    *,
    venue: str,
    instrument: str,
    hour_utc: datetime,
    preset: LiquidityDataPreset,
) -> RemoteArtifactKey:
    return RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
        hour_utc=hour_utc,
        preset_hash=preset.preset_hash,
    )


def _price_key(
    *,
    instrument: str,
    hour_utc: datetime,
) -> RemoteArtifactKey:
    return RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument=instrument,
        hour_utc=hour_utc,
    )


def _catch_up_target_from_observations(
    *,
    venue: str,
    latest_eligible_hour_utc: datetime,
    observations: tuple[_RemoteCatchUpObservation, ...],
    price_required: bool,
) -> datetime:
    """Select one safe remote target from newest-to-oldest observations.

    The first existing L2 artifact is the newest verified chain frontier inside
    the bounded search window.

    For Binance:

    - a missing price artifact at that frontier is repaired first;
    - otherwise only the immediately following hour may advance;
    - the frontier must own a usable output checkpoint;
    - absence of a frontier fails closed because update-only archives cannot
      initialize a fresh book.

    For OKX:

    - the same existing-frontier behavior is used when a frontier exists;
    - when no artifact exists in the bounded window, the oldest inspected hour
      is selected because the approved OKX source contract requires an opening
      complete snapshot in the target archive.

    Returning the latest already-complete hour is an idempotent no-work probe.
    ``process_remote_hour`` will verify and reuse its existing artifacts.
    """

    normalized_venue = str(venue or "").strip().lower()
    latest = require_utc_hour(
        "latest_eligible_hour_utc",
        latest_eligible_hour_utc,
    )
    values = tuple(observations)

    if not values:
        raise RemoteWorkerError(
            "Remote catch-up planning requires at least one inspected hour"
        )

    if not isinstance(price_required, bool):
        raise TypeError("price_required must be bool")

    expected_hour = latest

    for observation in values:
        if not isinstance(observation, _RemoteCatchUpObservation):
            raise TypeError(
                "observations must contain _RemoteCatchUpObservation objects"
            )

        if observation.hour_utc != expected_hour:
            raise RemoteWorkerError(
                "Remote catch-up observations must be contiguous and ordered "
                "newest to oldest"
            )

        expected_hour -= timedelta(hours=1)

    frontier = next(
        (observation for observation in values if observation.l2_exists),
        None,
    )

    if frontier is None:
        if normalized_venue == "okx_futures":
            return values[-1].hour_utc

        raise RemoteWorkerCheckpointBlockedError(
            "No verified Binance L2/checkpoint seed exists inside the bounded "
            "remote catch-up search window"
        )

    if not frontier.output_checkpoint_exists:
        raise RemoteWorkerCheckpointBlockedError(
            "The newest remote L2 frontier has no usable output checkpoint"
        )

    if price_required and not frontier.price_exists:
        return frontier.hour_utc

    next_hour = frontier.hour_utc + timedelta(hours=1)

    if next_hour <= latest:
        return next_hour

    return frontier.hour_utc


async def select_remote_catch_up_hour(
    *,
    repository: HuggingFaceDatasetRepository,
    venue: str,
    instrument: str,
    latest_eligible_hour_utc: datetime,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
    search_hours: int,
) -> datetime:
    """Find one safe processable hour from one pinned HF repository revision."""

    if not isinstance(
        repository,
        HuggingFaceDatasetRepository,
    ):
        raise TypeError("repository must be HuggingFaceDatasetRepository")

    if (
        isinstance(search_hours, bool)
        or not isinstance(search_hours, int)
        or search_hours <= 0
    ):
        raise RemoteWorkerError("search_hours must be a positive integer")

    latest = require_utc_hour(
        "latest_eligible_hour_utc",
        latest_eligible_hour_utc,
    )
    normalized_venue, normalized_instrument, price_required = _normalized_chain(
        venue,
        instrument,
    )
    preset = _preset_for_chain(
        venue=normalized_venue,
        instrument=normalized_instrument,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
    )

    pinned_revision = await asyncio.to_thread(
        repository.current_revision,
    )
    observations: list[_RemoteCatchUpObservation] = []

    for offset in range(search_hours):
        hour = latest - timedelta(hours=offset)
        l2_key = _l2_key(
            venue=normalized_venue,
            instrument=normalized_instrument,
            hour_utc=hour,
            preset=preset,
        )
        downloaded_l2 = await asyncio.to_thread(
            repository.download_artifact,
            l2_key,
            revision=pinned_revision,
        )

        if downloaded_l2 is None:
            observations.append(
                _RemoteCatchUpObservation(
                    hour_utc=hour,
                    l2_exists=False,
                    output_checkpoint_exists=False,
                    price_exists=False,
                )
            )
            continue

        if not isinstance(
            downloaded_l2.artifact,
            RemoteL2ProcessedArtifact,
        ):
            raise RemoteWorkerError(
                "Remote catch-up L2 key resolved to a non-L2 artifact"
            )

        price_exists = False

        if price_required:
            downloaded_price = await asyncio.to_thread(
                repository.download_artifact,
                _price_key(
                    instrument=normalized_instrument,
                    hour_utc=hour,
                ),
                revision=pinned_revision,
            )

            if downloaded_price is not None:
                if not isinstance(
                    downloaded_price.artifact,
                    RemotePriceProcessedArtifact,
                ):
                    raise RemoteWorkerError(
                        "Remote catch-up price key resolved to a non-price artifact"
                    )

                price_exists = True

        observations.append(
            _RemoteCatchUpObservation(
                hour_utc=hour,
                l2_exists=True,
                output_checkpoint_exists=(
                    downloaded_l2.artifact.output_checkpoint is not None
                ),
                price_exists=price_exists,
            )
        )

        # The newest existing L2 artifact is the only frontier needed for the
        # next safe chain transition. Older artifacts cannot supersede it.
        break

    return _catch_up_target_from_observations(
        venue=normalized_venue,
        latest_eligible_hour_utc=latest,
        observations=tuple(observations),
        price_required=price_required,
    )


async def _inspect_existing_state(
    repository: HuggingFaceDatasetRepository,
    *,
    l2_key: RemoteArtifactKey,
    price_key: RemoteArtifactKey | None,
) -> _ExistingRemoteState:
    revision = await asyncio.to_thread(
        repository.current_revision,
    )

    downloaded_l2 = await asyncio.to_thread(
        repository.download_artifact,
        l2_key,
        revision=revision,
    )

    if downloaded_l2 is None:
        l2_artifact = None
    else:
        if not isinstance(
            downloaded_l2.artifact,
            RemoteL2ProcessedArtifact,
        ):
            raise RemoteWorkerError("L2 key resolved to a non-L2 remote artifact")

        l2_artifact = downloaded_l2.artifact

    if price_key is None:
        price_artifact = None
    else:
        downloaded_price = await asyncio.to_thread(
            repository.download_artifact,
            price_key,
            revision=revision,
        )

        if downloaded_price is None:
            price_artifact = None
        else:
            if not isinstance(
                downloaded_price.artifact,
                RemotePriceProcessedArtifact,
            ):
                raise RemoteWorkerError(
                    "Price key resolved to a non-price remote artifact"
                )

            price_artifact = downloaded_price.artifact

    return _ExistingRemoteState(
        pinned_revision=revision,
        l2_artifact=l2_artifact,
        price_artifact=price_artifact,
    )


async def _predecessor_checkpoint(
    repository: HuggingFaceDatasetRepository,
    *,
    target_key: RemoteArtifactKey,
    pinned_revision: str,
    predecessor_required: bool,
) -> bytes | None:
    try:
        predecessor = await asyncio.to_thread(
            repository.download_l2_predecessor_checkpoint,
            target_key,
            revision=pinned_revision,
        )
    except HuggingFaceArtifactNotFoundError as exc:
        if predecessor_required:
            raise RemoteWorkerCheckpointBlockedError(
                "The required immediately preceding L2 artifact does not "
                "exist at the pinned Hugging Face revision"
            ) from exc

        return None

    checkpoint = predecessor.checkpoint_bytes

    if checkpoint is None and predecessor_required:
        raise RemoteWorkerCheckpointBlockedError(
            "The immediately preceding L2 artifact has no output checkpoint"
        )

    return checkpoint


def _archive_by_kind(
    archives: tuple[ProcessingSourceArchive, ...],
    data_kind: SourceDataKind,
) -> ProcessingSourceArchive:
    selected = tuple(
        archive for archive in archives if archive.spec.data_kind is data_kind
    )

    if len(selected) != 1:
        raise RemoteWorkerError(
            f"Expected exactly one {data_kind.value} source archive"
        )

    return selected[0]


async def process_remote_hour(
    *,
    repository: HuggingFaceDatasetRepository,
    cryptohft: CryptoHFTConfig,
    workspace: RemoteWorkerWorkspace,
    venue: str,
    instrument: str,
    hour_utc: datetime,
    latest_eligible_hour_utc: datetime,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
    producer_git_commit: str | None,
    use_api_key: bool = False,
    batch_size: int = 131_072,
) -> RemoteWorkerResult:
    """Acquire, process, and publish one completed remote source hour."""

    if not isinstance(
        repository,
        HuggingFaceDatasetRepository,
    ):
        raise TypeError("repository must be HuggingFaceDatasetRepository")

    if not isinstance(cryptohft, CryptoHFTConfig):
        raise TypeError("cryptohft must be CryptoHFTConfig")

    if not isinstance(workspace, RemoteWorkerWorkspace):
        raise TypeError("workspace must be RemoteWorkerWorkspace")

    target_hour = require_utc_hour(
        "hour_utc",
        hour_utc,
    )
    latest_eligible = require_utc_hour(
        "latest_eligible_hour_utc",
        latest_eligible_hour_utc,
    )

    if target_hour > latest_eligible:
        raise RemoteWorkerError(
            "Target hour is newer than the latest release-eligible hour"
        )

    normalized_venue, normalized_instrument, price_required = _normalized_chain(
        venue,
        instrument,
    )

    preset = _preset_for_chain(
        venue=normalized_venue,
        instrument=normalized_instrument,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
    )

    target_l2_key = _l2_key(
        venue=normalized_venue,
        instrument=normalized_instrument,
        hour_utc=target_hour,
        preset=preset,
    )
    target_price_key = (
        _price_key(
            instrument=normalized_instrument,
            hour_utc=target_hour,
        )
        if price_required
        else None
    )

    existing = await _inspect_existing_state(
        repository,
        l2_key=target_l2_key,
        price_key=target_price_key,
    )

    if (
        existing.l2_artifact is not None
        and existing.l2_artifact.output_checkpoint is None
    ):
        raise RemoteWorkerCheckpointBlockedError(
            "The existing target L2 artifact has no output checkpoint; "
            "the chain cannot advance"
        )

    l2_missing = existing.l2_artifact is None
    price_missing = bool(price_required and existing.price_artifact is None)

    if not l2_missing and not price_missing:
        return RemoteWorkerResult(
            venue=normalized_venue,
            instrument=normalized_instrument,
            hour_utc=target_hour,
            pinned_input_revision=existing.pinned_revision,
            l2_created=False,
            l2_revision=existing.pinned_revision,
            l2_reused=True,
            price_required=price_required,
            price_created=False,
            price_revision=(existing.pinned_revision if price_required else None),
            price_reused=price_required,
            source_downloaded_count=0,
            source_reused_count=0,
        )

    checkpoint_bytes: bytes | None = None

    if l2_missing:
        checkpoint_bytes = await _predecessor_checkpoint(
            repository,
            target_key=target_l2_key,
            pinned_revision=existing.pinned_revision,
            predecessor_required=(normalized_venue == "binance_futures"),
        )

    requested_specs: list[SourceFileSpec] = []

    if l2_missing:
        requested_specs.append(
            _source_spec(
                venue=normalized_venue,
                instrument=normalized_instrument,
                hour_utc=target_hour,
                data_kind=SourceDataKind.ORDERBOOK,
            )
        )

    if price_missing:
        requested_specs.append(
            _source_spec(
                venue="binance_futures",
                instrument=normalized_instrument,
                hour_utc=target_hour,
                data_kind=SourceDataKind.TRADES,
            )
        )

    acquisition = await acquire_remote_worker_archives(
        tuple(requested_specs),
        cryptohft=cryptohft,
        workspace=workspace,
        latest_eligible_hour_utc=latest_eligible,
        use_api_key=use_api_key,
    )

    l2_output = None
    price_output = None

    if l2_missing:
        orderbook_archive = _archive_by_kind(
            acquisition.processing_archives,
            SourceDataKind.ORDERBOOK,
        )

        l2_output = await asyncio.to_thread(
            process_l2_archive_headlessly,
            orderbook_archive,
            preset,
            input_checkpoint_bytes=checkpoint_bytes,
            producer_git_commit=producer_git_commit,
            batch_size=batch_size,
        )

        if l2_output.artifact.output_checkpoint is None:
            raise RemoteWorkerCheckpointBlockedError(
                "Target L2 processing did not produce a usable output "
                "checkpoint; no remote artifact was published"
            )

    if price_missing:
        trade_archive = _archive_by_kind(
            acquisition.processing_archives,
            SourceDataKind.TRADES,
        )

        price_output = await asyncio.to_thread(
            process_price_archives_headlessly,
            trade_archive.spec,
            (trade_archive,),
            producer_git_commit=producer_git_commit,
            batch_size=batch_size,
        )

    l2_publication: HuggingFacePublicationResult | None = None
    price_publication: HuggingFacePublicationResult | None = None

    if l2_output is not None:
        l2_publication = await asyncio.to_thread(
            repository.publish_artifact,
            l2_output.artifact,
        )

    if price_output is not None:
        price_publication = await asyncio.to_thread(
            repository.publish_artifact,
            price_output.artifact,
        )

    return RemoteWorkerResult(
        venue=normalized_venue,
        instrument=normalized_instrument,
        hour_utc=target_hour,
        pinned_input_revision=existing.pinned_revision,
        l2_created=(l2_publication.created if l2_publication is not None else False),
        l2_revision=(
            l2_publication.revision
            if l2_publication is not None
            else existing.pinned_revision
        ),
        l2_reused=(
            not l2_missing
            or (l2_publication is not None and not l2_publication.created)
        ),
        price_required=price_required,
        price_created=(
            price_publication.created if price_publication is not None else False
        ),
        price_revision=(
            price_publication.revision
            if price_publication is not None
            else (
                existing.pinned_revision
                if price_required and existing.price_artifact is not None
                else None
            )
        ),
        price_reused=bool(
            price_required
            and (
                existing.price_artifact is not None
                or (price_publication is not None and not price_publication.created)
            )
        ),
        source_downloaded_count=acquisition.downloaded_count,
        source_reused_count=acquisition.reused_count,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Acquire, process, and publish one completed CryptoHFTData "
            "hour to a private Hugging Face dataset."
        )
    )
    parser.add_argument(
        "--venue",
        required=True,
        choices=(
            "binance_futures",
            "okx_futures",
        ),
    )
    parser.add_argument(
        "--instrument",
        required=True,
    )
    parser.add_argument(
        "--hour",
        type=_canonical_utc_hour,
        default=None,
        help=(
            "Explicit completed UTC hour. When omitted, process the newest "
            "release-eligible hour."
        ),
    )
    parser.add_argument(
        "--release-delay-minutes",
        type=_positive_integer,
        default=15,
    )
    parser.add_argument(
        "--catch-up-hours",
        type=_positive_integer,
        default=72,
        help=(
            "Bounded newest-to-oldest HF frontier search used when --hour "
            "is omitted."
        ),
    )
    parser.add_argument(
        "--depth-lower",
        type=_depth_fraction,
        required=True,
    )
    parser.add_argument(
        "--depth-upper",
        type=_depth_fraction,
        required=True,
    )
    parser.add_argument(
        "--hf-repo-id",
        default=os.environ.get(
            "L2SHOCK_HF_REPO_ID",
            "",
        ),
    )
    parser.add_argument(
        "--hf-revision",
        default=os.environ.get(
            "L2SHOCK_HF_REVISION",
            "main",
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_integer,
        default=131_072,
    )
    parser.add_argument(
        "--use-api-key",
        action="store_true",
    )
    parser.add_argument(
        "--workspace-parent",
        type=Path,
        default=None,
    )

    return parser


def _required_environment_secret(name: str) -> SecretStr:
    value = str(os.environ.get(name, "") or "").strip()

    if not value:
        raise RemoteWorkerError(f"Required process secret {name} is not configured")

    return SecretStr(value)


async def _run_from_arguments(
    args: argparse.Namespace,
) -> RemoteWorkerResult:
    repo_id = str(args.hf_repo_id or "").strip()

    if not repo_id:
        raise RemoteWorkerError(
            "Hugging Face dataset repo ID is required through "
            "--hf-repo-id or L2SHOCK_HF_REPO_ID"
        )

    current = now_utc()
    latest_eligible = latest_release_eligible_hour(
        current,
        release_delay_minutes=args.release_delay_minutes,
    )

    hf_token = _required_environment_secret("HF_TOKEN")

    crypto_api_key = str(
        os.environ.get(
            "L2SHOCK__CRYPTOHFT__API_KEY",
            "",
        )
        or ""
    ).strip()

    cryptohft = CryptoHFTConfig(
        api_key=SecretStr(crypto_api_key),
        expected_release_delay_minutes=(args.release_delay_minutes),
    )

    repository = HuggingFaceDatasetRepository(
        repo_id=repo_id,
        revision=args.hf_revision,
        token=hf_token,
    )

    if args.hour is None:
        target_hour = await select_remote_catch_up_hour(
            repository=repository,
            venue=args.venue,
            instrument=args.instrument,
            latest_eligible_hour_utc=latest_eligible,
            lower_fraction=args.depth_lower,
            upper_fraction=args.depth_upper,
            search_hours=args.catch_up_hours,
        )
    else:
        target_hour = args.hour

    if target_hour > latest_eligible:
        raise RemoteWorkerError(
            "Requested target hour is newer than the release-eligible boundary"
        )

    with temporary_remote_worker_workspace(
        parent=args.workspace_parent,
    ) as workspace:
        return await process_remote_hour(
            repository=repository,
            cryptohft=cryptohft,
            workspace=workspace,
            venue=args.venue,
            instrument=args.instrument,
            hour_utc=target_hour,
            latest_eligible_hour_utc=latest_eligible,
            lower_fraction=args.depth_lower,
            upper_fraction=args.depth_upper,
            producer_git_commit=(
                str(os.environ.get("GITHUB_SHA", "") or "").strip() or None
            ),
            use_api_key=bool(args.use_api_key),
            batch_size=args.batch_size,
        )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        result = asyncio.run(_run_from_arguments(args))

    except KeyboardInterrupt:
        print(
            "Remote worker was interrupted.",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.INTERRUPTED)

    except RemoteWorkerCheckpointBlockedError as exc:
        print(
            f"Remote checkpoint chain is blocked: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.CHECKPOINT_CHAIN_BLOCKED)

    except RemoteFileNotFoundError as exc:
        print(
            f"Remote source is not currently available: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.SOURCE_UNAVAILABLE)

    except HuggingFaceRepositoryError as exc:
        print(
            f"Hugging Face operation failed: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.HUGGING_FACE_ERROR)

    except (
        AcquisitionError,
        ProcessingError,
        RemoteWorkerError,
        TypeError,
        ValueError,
    ) as exc:
        print(
            "Remote worker input or processing contract failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.INPUT_OR_CONTRACT_ERROR)

    except Exception as exc:
        print(
            "Unexpected remote-worker failure: " f"{type(exc).__name__}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.UNEXPECTED_ERROR)

    print(
        json.dumps(
            result.to_dict(),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return int(RemoteWorkerExitStatus.OK)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "RemoteWorkerCheckpointBlockedError",
    "RemoteWorkerError",
    "RemoteWorkerExitStatus",
    "RemoteWorkerResult",
    "build_parser",
    "main",
    "process_remote_hour",
    "select_remote_catch_up_hour",
]

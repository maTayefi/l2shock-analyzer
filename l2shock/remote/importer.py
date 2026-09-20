# l2shock/remote/importer.py
"""Verified local import of remote processed-hour artifacts.

This module bridges the private Hugging Face processed dataset to the existing
local PostgreSQL repositories.

It does not:

- download CryptoHFTData raw archives;
- reconstruct an order book;
- calculate liquidity;
- construct trade OHLC;
- create another compact codec;
- start NiceGUI;
- publish to Hugging Face;
- commit or roll back caller-owned sessions.

Import sequence:

    verified DownloadedHuggingFaceArtifact
        -> verify artifact/key/revision ownership
        -> reconstruct deterministic quality metadata from encoded channels
        -> reconstruct existing typed PostgreSQL provenance
        -> write through existing analytical repositories
        -> record verified remote source ownership without claiming local raw bytes

The remote artifact's existing encoded channels are persisted unchanged.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from sqlalchemy.orm import Session

from l2shock.acquisition import (
    AcquisitionRepository,
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
    acquire_source_hour_transaction_lock,
    sha256_file,
)
from l2shock.config import get_settings
from l2shock.db import (
    AnalyticalRepository,
    L2HourlyProvenance,
    PriceAnalyticalRepository,
    PriceHourlyProvenance,
    PriceSourceHourReference,
    SourceHourReference,
)
from l2shock.db.checkpoint_reference_locks import (
    acquire_checkpoint_reference_transaction_locks,
)
from l2shock.db.engine import session_scope
from l2shock.ingest import (
    BookSampleInvalidReason,
    BookSampleQuality,
    decode_checkpoint,
)
from l2shock.liquidity import decode_hourly_liquidity_blocks
from l2shock.processing import updated_l2_analytical_output_metadata
from l2shock.processing.checkpoint_store import CheckpointStore
from l2shock.price import (
    TradeSampleInvalidReason,
    TradeSampleQuality,
    decode_hourly_trade_ohlc_blocks,
)
from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
)
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteSourceHourReference,
)
from l2shock.remote.hf_repository import (
    DownloadedHuggingFaceArtifact,
    HuggingFaceDatasetRepository,
)
from l2shock.timeutils import now_utc

REMOTE_IMPORT_ORIGIN = "hugging_face_remote_import_v1"


class RemoteArtifactImportError(RuntimeError):
    """A verified remote artifact could not be imported safely."""


class RemoteImportSessionScopeFactory(Protocol):
    def __call__(self) -> AbstractContextManager[Session]: ...


@dataclass(frozen=True, slots=True)
class RemoteArtifactImportResult:
    """Primitive immutable result of one local remote-artifact import."""

    revision: str
    key: RemoteArtifactKey
    analytical_inserted: bool
    preset_inserted: bool | None
    source_metadata_updated: bool
    imported_at_utc: datetime

    def __post_init__(self) -> None:
        revision = str(self.revision or "").strip().lower()

        if len(revision) not in {40, 64} or any(
            character not in "0123456789abcdef" for character in revision
        ):
            raise RemoteArtifactImportError(
                "revision must be a full canonical commit SHA"
            )

        if not isinstance(self.key, RemoteArtifactKey):
            raise TypeError("key must be RemoteArtifactKey")

        if not isinstance(self.analytical_inserted, bool):
            raise TypeError("analytical_inserted must be bool")

        if self.preset_inserted is not None and not isinstance(
            self.preset_inserted,
            bool,
        ):
            raise TypeError("preset_inserted must be bool or null")

        if not isinstance(self.source_metadata_updated, bool):
            raise TypeError("source_metadata_updated must be bool")

        imported_at = self.imported_at_utc

        if (
            not isinstance(imported_at, datetime)
            or imported_at.tzinfo is None
            or imported_at.utcoffset() is None
        ):
            raise RemoteArtifactImportError("imported_at_utc must be timezone-aware")

        if imported_at.utcoffset().total_seconds() != 0:
            raise RemoteArtifactImportError(
                "imported_at_utc must have UTC offset +00:00"
            )

        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "imported_at_utc", imported_at)


def _l2_quality_summary(
    artifact: RemoteL2ProcessedArtifact,
) -> dict[str, object]:
    """Reconstruct the existing PostgreSQL L2 quality-summary contract."""

    decoded = decode_hourly_liquidity_blocks(artifact.encoded)

    valid_count = sum(quality is BookSampleQuality.VALID for quality in decoded.quality)
    degraded_count = sum(
        quality is BookSampleQuality.DEGRADED for quality in decoded.quality
    )
    invalid_count = sum(
        quality is BookSampleQuality.INVALID for quality in decoded.quality
    )

    invalid_reason_counts = {
        reason.value: sum(observed is reason for observed in decoded.invalid_reason)
        for reason in BookSampleInvalidReason
    }
    invalid_reason_counts = {
        reason: count for reason, count in invalid_reason_counts.items() if count > 0
    }

    zero_total_count = sum(
        quality is BookSampleQuality.VALID and bid == 0 and ask == 0
        for quality, bid, ask in zip(
            decoded.quality,
            decoded.bid_liquidity,
            decoded.ask_liquidity,
            strict=True,
        )
    )

    return {
        "schema": "l2shock.liquidity_quality_summary",
        "schema_version": 1,
        "observation_count": decoded.observation_count,
        "valid_count": valid_count,
        "degraded_count": degraded_count,
        "invalid_count": invalid_count,
        "zero_total_liquidity_count": zero_total_count,
        "invalid_reason_counts": invalid_reason_counts,
    }


def _price_quality_summary(
    artifact: RemotePriceProcessedArtifact,
) -> dict[str, object]:
    """Reconstruct the existing PostgreSQL price quality-summary contract."""

    decoded = decode_hourly_trade_ohlc_blocks(artifact.encoded)

    valid_count = sum(
        quality is TradeSampleQuality.VALID for quality in decoded.quality
    )
    invalid_count = sum(
        quality is TradeSampleQuality.INVALID for quality in decoded.quality
    )
    no_trade_count = sum(
        reason is TradeSampleInvalidReason.NO_TRADES
        for reason in decoded.invalid_reason
    )

    if no_trade_count != invalid_count:
        raise RemoteArtifactImportError(
            "Remote price artifact invalid reasons do not match "
            "the current no-trade contract"
        )

    return {
        "schema": "l2shock.trade_ohlc_quality_summary",
        "schema_version": 1,
        "observation_count": decoded.observation_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "total_trade_count": decoded.total_trade_count,
        "invalid_reason_counts": {
            TradeSampleInvalidReason.NO_TRADES.value: no_trade_count,
        },
    }


def _processing_quality_state(
    *,
    valid_count: int,
    degraded_count: int = 0,
) -> str:
    if valid_count == 3_600:
        return "VALID"

    if valid_count > 0 or degraded_count > 0:
        return "DEGRADED"

    return "INVALID"


def _current_source_reference(
    downloaded: DownloadedHuggingFaceArtifact,
) -> RemoteSourceHourReference:
    manifest = downloaded.artifact.manifest
    current = tuple(
        source
        for source in manifest.source_hours
        if source.hour_utc == manifest.key.hour_utc
    )

    if len(current) != 1:
        raise RemoteArtifactImportError(
            "Remote artifact must own exactly one current source-hour reference"
        )

    return current[0]


def _source_spec(
    reference: RemoteSourceHourReference,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider=reference.provider,
        venue=reference.venue,
        symbol=reference.instrument,
        data_kind=reference.data_kind,
        hour_utc=reference.hour_utc,
    )


def _source_quality_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}

    return {str(key): item for key, item in value.items() if isinstance(key, str)}


def _verify_existing_local_source_attachment(
    row,
    spec: SourceFileSpec,
    *,
    expected_content_sha256: str,
) -> None:
    """Fail closed when a source row claims unverified local raw bytes.

    A remote import may own a source without any local raw archive. However,
    when an existing row claims ``local_path``, that attachment remains subject
    to the normal canonical path, size, and SHA-256 contracts.
    """

    path_text = str(row.local_path or "").strip()

    if not path_text:
        return

    expected_size = row.file_size_bytes

    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size <= 0
    ):
        raise RemoteArtifactImportError(
            "Existing local raw attachment has no positive durable file size"
        )

    stored_path = Path(path_text).expanduser()

    try:
        if stored_path.is_symlink():
            raise RemoteArtifactImportError(
                "Existing local raw attachment cannot be a symbolic link"
            )

        resolved_path = stored_path.resolve()
        canonical_path = spec.local_path(
            get_settings().storage.raw_path,
        ).resolve()

        if resolved_path != canonical_path:
            raise RemoteArtifactImportError(
                "Existing local raw attachment is not at its canonical path"
            )

        if not resolved_path.is_file():
            raise RemoteArtifactImportError(
                "Existing local raw attachment is not a regular file"
            )

        actual_digest, actual_size = sha256_file(resolved_path)

    except RemoteArtifactImportError:
        raise
    except Exception as exc:
        raise RemoteArtifactImportError(
            "Existing local raw attachment could not be verified"
        ) from exc

    if actual_size != expected_size:
        raise RemoteArtifactImportError(
            "Existing local raw attachment size differs from durable metadata"
        )

    if actual_digest != expected_content_sha256:
        raise RemoteArtifactImportError(
            "Existing local raw attachment SHA-256 differs from "
            "the remote source identity"
        )


def _update_remote_source_metadata(
    session: Session,
    downloaded: DownloadedHuggingFaceArtifact,
    *,
    quality_summary: Mapping[str, object],
) -> None:
    """Record remote processing ownership without claiming local raw bytes."""

    artifact = downloaded.artifact
    manifest = artifact.manifest
    source = _current_source_reference(downloaded)
    spec = _source_spec(source)

    acquire_source_hour_transaction_lock(
        session,
        spec,
    )

    repository = AcquisitionRepository(session)
    row = repository.upsert_discovered(spec)

    try:
        status = SourceHourStatus(row.status)
    except (TypeError, ValueError) as exc:
        raise RemoteArtifactImportError(
            "Local source row has an unsupported status"
        ) from exc

    if status in {
        SourceHourStatus.DOWNLOADING,
        SourceHourStatus.PROCESSING,
        SourceHourStatus.INVALID,
        SourceHourStatus.QUARANTINED,
    }:
        raise RemoteArtifactImportError(
            "Remote import cannot replace a transient, invalid, or "
            "quarantined local source state"
        )

    existing_digest = str(row.content_sha256 or "").strip()

    if existing_digest and existing_digest != source.content_sha256:
        raise RemoteArtifactImportError(
            "Local source identity owns a different source archive SHA-256"
        )

    _verify_existing_local_source_attachment(
        row,
        spec,
        expected_content_sha256=source.content_sha256,
    )

    previous_quality = _source_quality_mapping(row.quality_json)
    updated_quality = dict(previous_quality)

    common_remote_metadata = {
        "processing_origin": REMOTE_IMPORT_ORIGIN,
        "remote_repository_revision": downloaded.revision,
        "remote_artifact_path": manifest.key.relative_path,
        "remote_manifest_sha256": manifest.manifest_sha256,
        "remote_analytical_content_sha256": manifest.content_sha256,
        "remote_imported_at_utc": now_utc().isoformat(),
    }

    if artifact.manifest.key.kind is RemoteArtifactKind.L2:
        preset = manifest.l2_preset

        if preset is None:
            raise RemoteArtifactImportError(
                "Remote L2 manifest lacks its canonical preset"
            )

        hashes, outputs = updated_l2_analytical_output_metadata(
            previous_quality,
            preset_hash=preset.preset_hash,
            content_sha256=manifest.content_sha256,
        )

        valid_count = int(quality_summary["valid_count"])
        degraded_count = int(quality_summary["degraded_count"])
        quality_state = _processing_quality_state(
            valid_count=valid_count,
            degraded_count=degraded_count,
        )

        updated_quality.update(
            {
                **common_remote_metadata,
                "schema": "l2shock.remote_imported_source_hour_quality",
                "schema_version": 1,
                "quality_state": quality_state,
                "liquidity_quality_summary": dict(quality_summary),
                "analytical_content_sha256": manifest.content_sha256,
                "analytical_content_sha256s": hashes,
                "analytical_outputs_by_preset": outputs,
                "input_checkpoint_content_sha256": (
                    manifest.input_checkpoint_content_sha256
                ),
                "output_checkpoint_content_sha256": (
                    manifest.output_checkpoint_content_sha256
                ),
                "remote_input_checkpoint_content_sha256": (
                    manifest.input_checkpoint_content_sha256
                ),
                "remote_output_checkpoint_content_sha256": (
                    manifest.output_checkpoint_content_sha256
                ),
            }
        )
    else:
        valid_count = int(quality_summary["valid_count"])
        quality_state = _processing_quality_state(
            valid_count=valid_count,
        )

        updated_quality.update(
            {
                **common_remote_metadata,
                "schema": "l2shock.remote_imported_source_hour_quality",
                "schema_version": 1,
                "quality_state": quality_state,
                "price_quality_summary": dict(quality_summary),
                "price_content_sha256": manifest.content_sha256,
            }
        )

    row.status = SourceHourStatus.PROCESSED.value
    row.remote_path = spec.remote_path
    row.content_sha256 = source.content_sha256
    row.quality_state = quality_state
    row.quality_json = updated_quality
    row.processed_at = now_utc()
    row.error_text = None

    # A remote import deliberately does not invent a local raw-file path,
    # source archive byte size, row count, event count, or snapshot count.
    # Existing valid local metadata is preserved when already present.
    session.flush()


def _import_l2(
    session: Session,
    downloaded: DownloadedHuggingFaceArtifact,
    artifact: RemoteL2ProcessedArtifact,
    *,
    checkpoint_store: CheckpointStore,
) -> RemoteArtifactImportResult:
    manifest = artifact.manifest
    preset = manifest.l2_preset

    if preset is None:
        raise RemoteArtifactImportError(
            "Remote L2 artifact lacks its canonical data preset"
        )

    if not isinstance(checkpoint_store, CheckpointStore):
        raise TypeError("checkpoint_store must be a CheckpointStore")

    current_source = _current_source_reference(downloaded)
    current_source_spec = _source_spec(current_source)

    acquire_source_hour_transaction_lock(
        session,
        current_source_spec,
    )
    acquire_checkpoint_reference_transaction_locks(
        session,
        (
            manifest.input_checkpoint_content_sha256,
            manifest.output_checkpoint_content_sha256,
        ),
    )

    if artifact.output_checkpoint is not None:
        try:
            decoded_checkpoint = decode_checkpoint(
                artifact.output_checkpoint,
            )
            published_checkpoint = checkpoint_store.publish(
                decoded_checkpoint,
            )
        except Exception as exc:
            raise RemoteArtifactImportError(
                "Could not install the verified remote output checkpoint"
            ) from exc

        if (
            published_checkpoint.encoding_info.content_sha256
            != manifest.output_checkpoint_content_sha256
        ):
            raise RemoteArtifactImportError(
                "Installed output checkpoint does not match the remote manifest"
            )
    elif manifest.output_checkpoint_content_sha256 is not None:
        raise RemoteArtifactImportError(
            "Remote manifest owns an output checkpoint without checkpoint bytes"
        )

    analytical_repository = AnalyticalRepository(session)

    preset_result = analytical_repository.ensure_preset(
        preset,
        enabled=True,
    )

    provenance = L2HourlyProvenance(
        source_hours=tuple(
            SourceHourReference(
                provider=source.provider,
                venue=source.venue,
                instrument=source.instrument,
                hour_utc=source.hour_utc,
                content_sha256=source.content_sha256,
            )
            for source in manifest.source_hours
        ),
        checkpoint_content_sha256=(manifest.input_checkpoint_content_sha256),
        replay_schema_version=1,
        liquidity_schema_version=1,
    )

    quality_summary = _l2_quality_summary(artifact)

    write_result = analytical_repository.write_l2_hour(
        preset=preset,
        hour_utc=manifest.key.hour_utc,
        encoded=artifact.encoded,
        quality_summary_json=quality_summary,
        provenance=provenance,
    )

    _update_remote_source_metadata(
        session,
        downloaded,
        quality_summary=quality_summary,
    )

    imported_at = now_utc()

    return RemoteArtifactImportResult(
        revision=downloaded.revision,
        key=manifest.key,
        analytical_inserted=write_result.inserted,
        preset_inserted=preset_result.inserted,
        source_metadata_updated=True,
        imported_at_utc=imported_at,
    )


def _import_price(
    session: Session,
    downloaded: DownloadedHuggingFaceArtifact,
    artifact: RemotePriceProcessedArtifact,
) -> RemoteArtifactImportResult:
    manifest = artifact.manifest
    key = manifest.key

    provenance = PriceHourlyProvenance(
        source_hours=tuple(
            PriceSourceHourReference(
                provider=source.provider,
                venue=source.venue,
                instrument=source.instrument,
                hour_utc=source.hour_utc,
                content_sha256=source.content_sha256,
            )
            for source in manifest.source_hours
        ),
        trade_reader_schema_version=1,
        trade_ohlc_schema_version=1,
    )

    quality_summary = _price_quality_summary(artifact)

    write_result = PriceAnalyticalRepository(session).write_price_hour(
        base=key.source_spec.base,
        hour_utc=key.hour_utc,
        encoded=artifact.encoded,
        quality_summary_json=quality_summary,
        provenance=provenance,
    )

    _update_remote_source_metadata(
        session,
        downloaded,
        quality_summary=quality_summary,
    )

    imported_at = now_utc()

    return RemoteArtifactImportResult(
        revision=downloaded.revision,
        key=key,
        analytical_inserted=write_result.inserted,
        preset_inserted=None,
        source_metadata_updated=True,
        imported_at_utc=imported_at,
    )


def import_downloaded_huggingface_artifact(
    session: Session,
    downloaded: DownloadedHuggingFaceArtifact,
    *,
    checkpoint_store: CheckpointStore | None = None,
) -> RemoteArtifactImportResult:
    """Import one already downloaded and verified HF artifact.

    Transaction ownership belongs to the caller.
    """

    if not isinstance(session, Session):
        raise TypeError(
            "import_downloaded_huggingface_artifact requires a SQLAlchemy Session"
        )

    if not isinstance(
        downloaded,
        DownloadedHuggingFaceArtifact,
    ):
        raise TypeError("downloaded must be DownloadedHuggingFaceArtifact")

    artifact = downloaded.artifact

    if artifact.manifest.key.kind is RemoteArtifactKind.L2:
        if not isinstance(artifact, RemoteL2ProcessedArtifact):
            raise RemoteArtifactImportError("L2 key does not own a remote L2 artifact")

        selected_checkpoint_store = (
            checkpoint_store
            if checkpoint_store is not None
            else CheckpointStore(get_settings().storage.cache_path)
        )

        if not isinstance(selected_checkpoint_store, CheckpointStore):
            raise TypeError("checkpoint_store must be a CheckpointStore or null")

        return _import_l2(
            session,
            downloaded,
            artifact,
            checkpoint_store=selected_checkpoint_store,
        )

    if not isinstance(artifact, RemotePriceProcessedArtifact):
        raise RemoteArtifactImportError(
            "Price key does not own a remote price artifact"
        )

    return _import_price(
        session,
        downloaded,
        artifact,
    )


def download_and_import_huggingface_artifact(
    repository: HuggingFaceDatasetRepository,
    key: RemoteArtifactKey,
    *,
    session_scope_factory: RemoteImportSessionScopeFactory = session_scope,
    checkpoint_store: CheckpointStore | None = None,
) -> RemoteArtifactImportResult:
    """Pin, download, verify, and transactionally import one remote artifact."""

    if not isinstance(repository, HuggingFaceDatasetRepository):
        raise TypeError("repository must be HuggingFaceDatasetRepository")

    if not isinstance(key, RemoteArtifactKey):
        raise TypeError("key must be RemoteArtifactKey")

    if not callable(session_scope_factory):
        raise TypeError("session_scope_factory must be callable")

    revision = repository.current_revision()
    downloaded = repository.require_artifact(
        key,
        revision=revision,
    )

    if downloaded.revision != revision:
        raise RemoteArtifactImportError(
            "Downloaded artifact does not belong to the pinned revision"
        )

    with session_scope_factory() as session:
        return import_downloaded_huggingface_artifact(
            session,
            downloaded,
            checkpoint_store=checkpoint_store,
        )


__all__ = [
    "REMOTE_IMPORT_ORIGIN",
    "RemoteArtifactImportError",
    "RemoteArtifactImportResult",
    "RemoteImportSessionScopeFactory",
    "download_and_import_huggingface_artifact",
    "import_downloaded_huggingface_artifact",
]

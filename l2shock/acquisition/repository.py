# l2shock/acquisition/repository.py
"""Transactional PostgreSQL persistence for acquisition state."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from l2shock.acquisition.errors import DownloadConflictError
from l2shock.acquisition.locks import (
    acquire_source_hour_transaction_lock,
)
from l2shock.acquisition.models import SourceFileSpec
from l2shock.acquisition.persistence import (
    FetchRunKind,
    FetchRunStatus,
    SourceHourStatus,
    bounded_diagnostic_text,
    nonnegative_counter,
    validate_source_hour_transition,
)
from l2shock.acquisition.results import DownloadArtifact
from l2shock.db.models import FetchRun, SourceHour
from l2shock.timeutils import now_utc, require_aware_utc

_SOURCE_IDENTITY_COLUMNS = (
    "provider",
    "venue",
    "data_kind",
    "instrument",
    "hour_utc",
)


def _operation_id_text(value: UUID | str) -> str:
    if isinstance(value, UUID):
        return str(value)

    raw = str(value or "").strip()

    try:
        parsed = UUID(raw)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("operation_id must be a valid UUID") from exc

    canonical = str(parsed)

    if raw != canonical:
        raise ValueError(
            "operation_id must use canonical lowercase " "36-character UUID form"
        )

    return canonical


def _source_identity_filter(spec: SourceFileSpec) -> tuple[Any, ...]:
    return (
        SourceHour.provider == spec.provider,
        SourceHour.venue == spec.venue,
        SourceHour.data_kind == spec.data_kind.value,
        SourceHour.instrument == spec.symbol,
        SourceHour.hour_utc == spec.hour_utc,
    )


def _validate_artifact_identity(
    spec: SourceFileSpec,
    artifact: DownloadArtifact,
) -> None:
    if artifact.spec.identity_tuple != spec.identity_tuple:
        raise ValueError("Download artifact identity does not match persistence target")

    if artifact.local_path.name != spec.filename:
        raise ValueError(
            "Download artifact filename does not match source identity: "
            f"expected {spec.filename!r}, got {artifact.local_path.name!r}"
        )


_PROCESSING_REFERENCE_KEYS = frozenset(
    {
        "input_checkpoint_content_sha256",
        "output_checkpoint_content_sha256",
        "analytical_content_sha256",
        "analytical_content_sha256s",
        "analytical_outputs_by_preset",
        "price_content_sha256",
    }
)


def _preserved_processing_references(
    quality_json: object,
) -> dict[str, Any]:
    """Retain durable artifact references while clearing stale processing facts."""

    if not isinstance(quality_json, dict):
        return {}

    return {
        key: value
        for key, value in quality_json.items()
        if key in _PROCESSING_REFERENCE_KEYS
    }


class AcquisitionRepository:
    """Repository bound to one caller-owned SQLAlchemy transaction."""

    def __init__(
        self,
        session: Session,
        *,
        diagnostic_secrets: Iterable[str] = (),
    ) -> None:
        if not isinstance(session, Session):
            raise TypeError("session must be a SQLAlchemy Session")

        self._session = session
        self._diagnostic_secrets = tuple(
            str(secret) for secret in diagnostic_secrets if str(secret or "").strip()
        )

    @property
    def session(self) -> Session:
        return self._session

    def get_fetch_run(
        self,
        operation_id: UUID | str,
    ) -> FetchRun | None:
        """Return one fetch run by its canonical operation UUID."""
        operation_text = _operation_id_text(operation_id)

        return self._session.scalar(
            select(FetchRun).where(FetchRun.operation_id == operation_text)
        )

    def get_source_hour(
        self,
        spec: SourceFileSpec,
    ) -> SourceHour | None:
        return self._session.scalar(
            select(SourceHour).where(*_source_identity_filter(spec))
        )

    def upsert_discovered(
        self,
        spec: SourceFileSpec,
    ) -> SourceHour:
        """Insert the source identity or refresh only its canonical path.

        Existing operational status, timestamps, hashes, and diagnostics are
        preserved. Discovery must never regress a downloaded or processed row.
        """
        statement = insert(SourceHour).values(
            provider=spec.provider,
            venue=spec.venue,
            data_kind=spec.data_kind.value,
            instrument=spec.symbol,
            hour_utc=spec.hour_utc,
            remote_path=spec.remote_path,
            status=SourceHourStatus.DISCOVERED.value,
            quality_state="INVALID",
            quality_json={},
        )

        statement = statement.on_conflict_do_update(
            index_elements=list(_SOURCE_IDENTITY_COLUMNS),
            set_={
                "remote_path": statement.excluded.remote_path,
            },
        ).returning(SourceHour)

        return self._session.scalars(statement).one()

    def transition_source_hour(
        self,
        spec: SourceFileSpec,
        target: SourceHourStatus | str,
        *,
        allow_processing_cancellation_reset: bool = False,
    ) -> SourceHour:
        """Handle state transitions for source hours with transaction locking.

        ``PROCESSING -> DOWNLOADED`` is reserved for cooperative cancellation
        before any terminal analytical mutation has been committed.
        """
        acquire_source_hour_transaction_lock(self._session, spec)
        row = self.upsert_discovered(spec)

        _current, target_status = validate_source_hour_transition(
            row.status,
            target,
            allow_processing_cancellation_reset=(allow_processing_cancellation_reset),
        )

        row.status = target_status.value
        self._session.flush()
        return row

    def mark_downloading(
        self,
        spec: SourceFileSpec,
    ) -> SourceHour:
        return self.transition_source_hour(
            spec,
            SourceHourStatus.DOWNLOADING,
        )

    def record_artifact(
        self,
        artifact: DownloadArtifact,
    ) -> SourceHour:
        """Persist successful downloaded or reused artifact facts.

        A logical hourly source identity is immutable once it owns a durable
        content SHA-256. Normal acquisition may reattach identical bytes, but
        it must never rebind that identity to different source content.

        Returning the row to ``downloaded`` clears processing-derived scalar
        metadata. Durable analytical and checkpoint references are retained so
        maintenance cannot orphan still-referenced immutable artifacts.
        """
        spec = artifact.spec
        _validate_artifact_identity(spec, artifact)

        acquire_source_hour_transaction_lock(self._session, spec)
        row = self.upsert_discovered(spec)

        existing_digest = str(row.content_sha256 or "").strip()

        if existing_digest and existing_digest != artifact.content_sha256:
            raise DownloadConflictError(
                "A source-hour identity already owns a different durable "
                "content SHA-256. Normal acquisition cannot replace immutable "
                f"source content for {spec.remote_path}."
            )

        validate_source_hour_transition(
            row.status,
            SourceHourStatus.DOWNLOADED,
        )

        preserved_references = _preserved_processing_references(
            row.quality_json,
        )

        row.status = SourceHourStatus.DOWNLOADED.value
        row.remote_path = spec.remote_path
        row.local_path = str(artifact.local_path)
        row.file_size_bytes = artifact.file_size_bytes
        row.content_sha256 = artifact.content_sha256
        row.row_count = artifact.validation.row_count
        row.downloaded_at = now_utc()
        row.processed_at = None
        row.error_text = None

        row.event_count = None
        row.snapshot_count = None
        row.continuity_mismatch_count = None

        # Structural Parquet validation is not reconstruction validity.
        row.quality_state = "INVALID"
        row.quality_json = preserved_references

        self._session.flush()
        return row

    def record_missing(
        self,
        spec: SourceFileSpec,
        *,
        message: object = "Remote hourly archive is not available",
    ) -> SourceHour:
        acquire_source_hour_transaction_lock(self._session, spec)
        row = self.upsert_discovered(spec)

        validate_source_hour_transition(
            row.status,
            SourceHourStatus.MISSING,
        )

        row.status = SourceHourStatus.MISSING.value
        row.error_text = bounded_diagnostic_text(
            message,
            secrets=self._diagnostic_secrets,
        )

        self._session.flush()
        return row

    def record_error(
        self,
        spec: SourceFileSpec,
        *,
        message: object,
    ) -> SourceHour:
        acquire_source_hour_transaction_lock(self._session, spec)
        row = self.upsert_discovered(spec)

        validate_source_hour_transition(
            row.status,
            SourceHourStatus.ERROR,
        )

        row.status = SourceHourStatus.ERROR.value
        row.error_text = bounded_diagnostic_text(
            message,
            secrets=self._diagnostic_secrets,
        )

        self._session.flush()
        return row

    def create_fetch_run(
        self,
        *,
        operation_id: UUID | str,
        kind: FetchRunKind | str,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        files_requested: int,
        details: dict[str, Any] | None = None,
    ) -> FetchRun:
        operation_text = _operation_id_text(operation_id)

        try:
            run_kind = FetchRunKind(kind)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in FetchRunKind)
            raise ValueError(f"Fetch-run kind must be one of: {allowed}") from exc

        start = require_aware_utc(
            "requested_start_utc",
            requested_start_utc,
        )
        end = require_aware_utc(
            "requested_end_utc",
            requested_end_utc,
        )

        if end < start:
            raise ValueError(
                "requested_end_utc must be at or after " "requested_start_utc"
            )

        requested_count = nonnegative_counter(
            "files_requested",
            files_requested,
        )

        details_json = dict(details or {})

        run = FetchRun(
            operation_id=operation_text,
            kind=run_kind.value,
            status=FetchRunStatus.RUNNING.value,
            requested_start_utc=start,
            requested_end_utc=end,
            files_requested=requested_count,
            files_downloaded=0,
            files_processed=0,
            files_failed=0,
            details_json=details_json,
            error_text=None,
        )

        self._session.add(run)
        self._session.flush()
        return run

    def complete_fetch_run(
        self,
        run: FetchRun,
        *,
        status: FetchRunStatus | str,
        files_downloaded: int,
        files_processed: int,
        files_failed: int,
        details: dict[str, Any] | None = None,
        error_text: object = None,
    ) -> FetchRun:
        try:
            final_status = FetchRunStatus(status)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in FetchRunStatus)
            raise ValueError(f"Fetch-run status must be one of: {allowed}") from exc

        if final_status is FetchRunStatus.RUNNING:
            raise ValueError("complete_fetch_run requires a terminal status")

        if run.status != FetchRunStatus.RUNNING.value:
            raise ValueError("Only a running fetch run can be completed")

        downloaded = nonnegative_counter(
            "files_downloaded",
            files_downloaded,
        )
        processed = nonnegative_counter(
            "files_processed",
            files_processed,
        )
        failed = nonnegative_counter(
            "files_failed",
            files_failed,
        )

        if downloaded + failed > run.files_requested:
            raise ValueError(
                "files_downloaded + files_failed cannot exceed " "files_requested"
            )

        if processed > downloaded:
            raise ValueError("files_processed cannot exceed files_downloaded")

        run.status = final_status.value
        run.files_downloaded = downloaded
        run.files_processed = processed
        run.files_failed = failed
        run.ended_at = now_utc()

        if details is not None:
            run.details_json = dict(details)

        run.error_text = bounded_diagnostic_text(
            error_text,
            secrets=self._diagnostic_secrets,
        )

        self._session.flush()
        return run


__all__ = ["AcquisitionRepository"]

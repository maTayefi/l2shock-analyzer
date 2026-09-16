from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session

from l2shock.acquisition import (
    AcquisitionRepository,
    DownloadArtifact,
    DownloadConflictError,
    DownloadDisposition,
    FetchRunStatus,
    ParquetValidationReport,
    SourceFileSpec,
    SourceHourLockUnavailableError,
    SourceHourStatus,
    acquire_source_hour_transaction_lock,
)
from l2shock.db.engine import get_engine
from l2shock.db.schema import verify_schema
from l2shock.processing import (
    SourceArchiveMetadataError,
)
from l2shock.processing.source_repository import (
    SQLAlchemyProcessingSourceRepository,
)

pytestmark = pytest.mark.postgresql


def _hour(
    day: int,
    hour: int,
) -> datetime:
    return datetime(
        2080,
        1,
        day,
        hour,
        tzinfo=timezone.utc,
    )


def _spec(
    *,
    symbol: str = "BTCUSDT",
    data_kind: str = "orderbook",
    day: int = 1,
    hour: int = 0,
) -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol=symbol,
        data_kind=data_kind,
        hour_utc=_hour(day, hour),
    )


def _validation_report(
    *,
    data_kind: str = "orderbook",
    row_count: int = 123,
) -> ParquetValidationReport:
    return ParquetValidationReport(
        data_kind=data_kind,
        row_count=row_count,
        row_group_count=2,
        column_names=(
            "received_time",
            "event_time",
            "symbol",
        ),
        created_by="integration-test",
        format_version="2.6",
    )


def _artifact(
    tmp_path: Path,
    spec: SourceFileSpec,
    *,
    disposition: DownloadDisposition = (DownloadDisposition.DOWNLOADED),
    digest_character: str = "a",
    row_count: int = 123,
) -> DownloadArtifact:
    attempts = 0 if disposition is DownloadDisposition.REUSED else 1

    return DownloadArtifact(
        spec=spec,
        local_path=tmp_path / spec.filename,
        disposition=disposition,
        file_size_bytes=4096,
        content_sha256=digest_character * 64,
        validation=_validation_report(
            data_kind=spec.data_kind.value,
            row_count=row_count,
        ),
        attempts=attempts,
    )


@pytest.fixture
def database_session() -> Iterator[Session]:
    """Provide a real PostgreSQL session isolated by an outer rollback.

    Repository methods may flush normally, but no integration-test rows are
    committed to the configured application database.
    """
    engine = get_engine()
    verify_schema(engine)

    with engine.connect() as connection:
        outer_transaction = connection.begin()

        session = Session(
            bind=connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )

        try:
            yield session
        finally:
            session.close()

            if outer_transaction.is_active:
                outer_transaction.rollback()


def test_discovered_upsert_is_deterministic(
    database_session: Session,
) -> None:
    repository = AcquisitionRepository(database_session)
    spec = _spec(day=1, hour=1)

    first = repository.upsert_discovered(spec)
    first_id = first.id

    second = repository.upsert_discovered(spec)

    assert first_id is not None
    assert second.id == first_id
    assert second.provider == spec.provider
    assert second.venue == spec.venue
    assert second.data_kind == spec.data_kind.value
    assert second.instrument == spec.symbol
    assert second.hour_utc == spec.hour_utc
    assert second.remote_path == spec.remote_path
    assert second.status == "discovered"
    assert second.quality_state == "INVALID"


def test_discovery_does_not_regress_downloaded_artifact(
    database_session: Session,
    tmp_path: Path,
) -> None:
    repository = AcquisitionRepository(database_session)
    spec = _spec(day=1, hour=2)
    artifact = _artifact(
        tmp_path,
        spec,
        digest_character="b",
        row_count=987,
    )

    downloaded = repository.record_artifact(artifact)
    downloaded_id = downloaded.id
    downloaded_at = downloaded.downloaded_at

    rediscovered = repository.upsert_discovered(spec)

    assert rediscovered.id == downloaded_id
    assert rediscovered.status == "downloaded"
    assert rediscovered.local_path == str(artifact.local_path)
    assert rediscovered.file_size_bytes == 4096
    assert rediscovered.content_sha256 == "b" * 64
    assert rediscovered.row_count == 987
    assert rediscovered.downloaded_at == downloaded_at
    assert rediscovered.quality_state == "INVALID"
    assert rediscovered.error_text is None


def test_downloaded_and_reused_artifacts_persist_idempotently(
    database_session: Session,
    tmp_path: Path,
) -> None:
    repository = AcquisitionRepository(database_session)
    spec = _spec(day=1, hour=3)

    downloaded_artifact = _artifact(
        tmp_path,
        spec,
        disposition=DownloadDisposition.DOWNLOADED,
        digest_character="c",
        row_count=456,
    )

    first = repository.record_artifact(downloaded_artifact)
    first_id = first.id

    reused_artifact = _artifact(
        tmp_path,
        spec,
        disposition=DownloadDisposition.REUSED,
        digest_character="c",
        row_count=456,
    )

    second = repository.record_artifact(reused_artifact)

    assert first_id is not None
    assert second.id == first_id
    assert second.status == "downloaded"
    assert second.remote_path == spec.remote_path
    assert second.local_path == str(reused_artifact.local_path)
    assert second.file_size_bytes == 4096
    assert second.content_sha256 == "c" * 64
    assert second.row_count == 456
    assert second.downloaded_at is not None
    assert second.downloaded_at.tzinfo is not None
    assert second.downloaded_at.utcoffset() is not None

    # Structural Parquet validity must not promote reconstruction validity.
    assert second.quality_state == "INVALID"


def test_missing_and_error_diagnostics_are_redacted(
    database_session: Session,
) -> None:
    configured_secret = "-".join(
        (
            "configured",
            "integration",
            "credential",
            "value",
        )
    )
    query_secret = "-".join(
        (
            "query",
            "integration",
            "credential",
        )
    )
    bearer_secret = "-".join(
        (
            "bearer",
            "integration",
            "credential",
        )
    )
    database_secret = "-".join(
        (
            "database",
            "integration",
            "credential",
        )
    )

    repository = AcquisitionRepository(
        database_session,
        diagnostic_secrets=(configured_secret,),
    )
    spec = _spec(day=1, hour=4)

    missing_message = (
        "Remote archive unavailable "
        f"https://example.test/download?api_key={query_secret} "
        f"configured={configured_secret}"
    )

    missing = repository.record_missing(
        spec,
        message=missing_message,
    )

    assert missing.status == "missing"
    assert missing.error_text is not None
    assert query_secret not in missing.error_text
    assert configured_secret not in missing.error_text
    assert "***" in missing.error_text

    error_message = (
        f"Authorization: Bearer {bearer_secret} "
        f"postgresql://user:{database_secret}@127.0.0.1/db "
        + ("diagnostic-data " * 300)
    )

    errored = repository.record_error(
        spec,
        message=error_message,
    )

    assert errored.id == missing.id
    assert errored.status == "error"
    assert errored.error_text is not None
    assert bearer_secret not in errored.error_text
    assert database_secret not in errored.error_text
    assert len(errored.error_text) <= 2000
    assert errored.error_text.endswith("...")


def test_fetch_run_lifecycle_persists_real_postgresql_values(
    database_session: Session,
) -> None:
    repository = AcquisitionRepository(database_session)
    operation_id = uuid4()

    run = repository.create_fetch_run(
        operation_id=operation_id,
        kind="manual",
        requested_start_utc=_hour(2, 0),
        requested_end_utc=_hour(2, 2),
        files_requested=8,
        details={
            "source": "postgresql-integration-test",
            "phase": "created",
        },
    )

    assert run.id is not None
    assert run.operation_id == str(operation_id)
    assert run.kind == "manual"
    assert run.status == "running"
    assert run.files_requested == 8
    assert run.files_downloaded == 0
    assert run.files_processed == 0
    assert run.files_failed == 0
    assert run.ended_at is None
    assert run.started_at is not None
    assert run.started_at.tzinfo is not None

    completed = repository.complete_fetch_run(
        run,
        status=FetchRunStatus.PARTIAL_OK,
        files_downloaded=6,
        files_processed=0,
        files_failed=2,
        details={
            "source": "postgresql-integration-test",
            "phase": "completed",
        },
        error_text="Two remote hourly archives were unavailable",
    )

    assert completed.id == run.id
    assert completed.status == "partial_ok"
    assert completed.files_requested == 8
    assert completed.files_downloaded == 6
    assert completed.files_processed == 0
    assert completed.files_failed == 2
    assert completed.ended_at is not None
    assert completed.ended_at.tzinfo is not None
    assert completed.ended_at >= completed.started_at
    assert completed.details_json == {
        "source": "postgresql-integration-test",
        "phase": "completed",
    }
    assert completed.error_text == ("Two remote hourly archives were unavailable")

    with pytest.raises(
        ValueError,
        match="Only a running fetch run",
    ):
        repository.complete_fetch_run(
            completed,
            status="ok",
            files_downloaded=6,
            files_processed=0,
            files_failed=2,
        )


def test_fetch_run_can_be_loaded_by_operation_id(
    database_session: Session,
) -> None:
    repository = AcquisitionRepository(database_session)
    operation_id = uuid4()

    created = repository.create_fetch_run(
        operation_id=operation_id,
        kind="manual",
        requested_start_utc=_hour(2, 7),
        requested_end_utc=_hour(2, 8),
        files_requested=4,
    )

    loaded_by_uuid = repository.get_fetch_run(operation_id)
    loaded_by_text = repository.get_fetch_run(str(operation_id))

    assert loaded_by_uuid is not None
    assert loaded_by_text is not None
    assert loaded_by_uuid.id == created.id
    assert loaded_by_text.id == created.id
    assert loaded_by_uuid.operation_id == str(operation_id)


def test_fetch_run_counter_validation_precedes_mutation(
    database_session: Session,
) -> None:
    repository = AcquisitionRepository(database_session)

    run = repository.create_fetch_run(
        operation_id=uuid4(),
        kind="validation",
        requested_start_utc=_hour(2, 3),
        requested_end_utc=_hour(2, 4),
        files_requested=4,
    )

    with pytest.raises(
        ValueError,
        match=r"files_downloaded \+ files_failed",
    ):
        repository.complete_fetch_run(
            run,
            status="error",
            files_downloaded=4,
            files_processed=0,
            files_failed=1,
        )

    assert run.status == "running"
    assert run.files_downloaded == 0
    assert run.files_processed == 0
    assert run.files_failed == 0
    assert run.ended_at is None


def test_fetch_run_requires_canonical_operation_uuid(
    database_session: Session,
) -> None:
    repository = AcquisitionRepository(database_session)
    operation_id = uuid4()

    with pytest.raises(
        ValueError,
        match="canonical lowercase 36-character UUID",
    ):
        repository.create_fetch_run(
            operation_id="{" + str(operation_id) + "}",
            kind="manual",
            requested_start_utc=_hour(2, 5),
            requested_end_utc=_hour(2, 6),
            files_requested=1,
        )

    accepted = repository.create_fetch_run(
        operation_id=UUID(str(operation_id)),
        kind="manual",
        requested_start_utc=_hour(2, 5),
        requested_end_utc=_hour(2, 6),
        files_requested=1,
    )

    assert accepted.operation_id == str(operation_id)


def test_transaction_advisory_lock_reports_contention_and_releases() -> None:
    engine = get_engine()
    verify_schema(engine)

    spec = _spec(
        symbol="ETHUSDT",
        data_kind="trades",
        day=3,
        hour=7,
    )

    first_connection = engine.connect()
    second_connection = engine.connect()
    first_session = Session(bind=first_connection)
    second_session = Session(bind=second_connection)

    try:
        first_keys = acquire_source_hour_transaction_lock(
            first_session,
            spec,
        )

        with pytest.raises(SourceHourLockUnavailableError):
            acquire_source_hour_transaction_lock(
                second_session,
                spec,
            )

        # Transaction-scoped locks are released by rollback.
        first_session.rollback()

        second_keys = acquire_source_hour_transaction_lock(
            second_session,
            spec,
        )

        assert second_keys == first_keys

    finally:
        first_session.rollback()
        second_session.rollback()
        first_session.close()
        second_session.close()
        first_connection.close()
        second_connection.close()


def test_processed_source_without_local_path_is_not_replayable(
    database_session: Session,
) -> None:
    spec = _spec()
    repository = AcquisitionRepository(database_session)
    row = repository.upsert_discovered(spec)

    row.status = SourceHourStatus.PROCESSED.value
    row.local_path = None
    row.file_size_bytes = 123
    row.content_sha256 = "a" * 64
    database_session.flush()

    result = SQLAlchemyProcessingSourceRepository(database_session).find_replayable(
        spec
    )

    assert result is None


def test_downloaded_source_without_local_path_is_metadata_error(
    database_session: Session,
) -> None:
    spec = _spec()
    repository = AcquisitionRepository(database_session)
    row = repository.upsert_discovered(spec)

    row.status = SourceHourStatus.DOWNLOADED.value
    row.local_path = None
    row.file_size_bytes = 123
    row.content_sha256 = "a" * 64
    database_session.flush()

    with pytest.raises(
        SourceArchiveMetadataError,
        match="no local path",
    ):
        SQLAlchemyProcessingSourceRepository(database_session).find_replayable(spec)


def test_existing_source_digest_cannot_be_rebound(
    database_session: Session,
    tmp_path: Path,
) -> None:
    repository = AcquisitionRepository(database_session)
    spec = _spec(day=4, hour=1)

    first = repository.record_artifact(
        _artifact(
            tmp_path,
            spec,
            digest_character="a",
        )
    )

    first.status = SourceHourStatus.ERROR.value
    database_session.flush()

    repository.mark_downloading(spec)

    with pytest.raises(
        DownloadConflictError,
        match="different durable content SHA-256",
    ):
        repository.record_artifact(
            _artifact(
                tmp_path,
                spec,
                digest_character="b",
            )
        )

    database_session.refresh(first)

    assert first.content_sha256 == "a" * 64
    assert first.file_size_bytes == 4096
    assert first.status == SourceHourStatus.DOWNLOADING.value


def test_redownload_clears_stale_processing_metadata_but_keeps_references(
    database_session: Session,
    tmp_path: Path,
) -> None:
    repository = AcquisitionRepository(database_session)
    spec = _spec(day=4, hour=2)

    row = repository.record_artifact(
        _artifact(
            tmp_path,
            spec,
            digest_character="c",
            row_count=500,
        )
    )

    row.status = SourceHourStatus.PROCESSED.value
    row.processed_at = _hour(4, 3)
    row.event_count = 100
    row.snapshot_count = 2
    row.continuity_mismatch_count = 1
    row.quality_state = "VALID"
    row.quality_json = {
        "schema": "old-processing-schema",
        "quality_state": "VALID",
        "event_diagnostic": "stale",
        "analytical_content_sha256": "d" * 64,
        "output_checkpoint_content_sha256": "e" * 64,
    }
    database_session.flush()

    repository.record_error(
        spec,
        message="retry requested",
    )
    repository.mark_downloading(spec)

    refreshed = repository.record_artifact(
        _artifact(
            tmp_path,
            spec,
            digest_character="c",
            row_count=600,
        )
    )

    assert refreshed.status == SourceHourStatus.DOWNLOADED.value
    assert refreshed.processed_at is None
    assert refreshed.event_count is None
    assert refreshed.snapshot_count is None
    assert refreshed.continuity_mismatch_count is None
    assert refreshed.row_count == 600
    assert refreshed.quality_state == "INVALID"
    assert refreshed.quality_json == {
        "analytical_content_sha256": "d" * 64,
        "output_checkpoint_content_sha256": "e" * 64,
    }

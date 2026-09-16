from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from l2shock.acquisition import (
    AcquisitionRepository,
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.db import get_engine
from l2shock.acquisition.retention import (
    RawRetentionError,
    plan_processed_raw_retention,
    raw_retention_cutoff_hour,
)
from l2shock.db.schema import verify_schema

pytestmark = pytest.mark.postgresql


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2087,
        1,
        1,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _spec(
    *,
    offset: int = 0,
    data_kind: SourceDataKind = SourceDataKind.ORDERBOOK,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=data_kind,
        hour_utc=_hour(offset),
    )


@pytest.fixture
def database_session() -> Session:
    engine = get_engine()
    verify_schema(engine)

    connection = engine.connect()
    transaction = connection.begin()

    session = Session(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    try:
        yield session
    finally:
        session.close()

        if transaction.is_active:
            transaction.rollback()

        connection.close()


def _register_processed(
    session: Session,
    raw_root: Path,
    *,
    offset: int,
    data_kind: SourceDataKind,
    content: bytes,
) -> SourceFileSpec:
    spec = _spec(
        offset=offset,
        data_kind=data_kind,
    )
    path = spec.local_path(raw_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

    repository = AcquisitionRepository(session)
    row = repository.upsert_discovered(spec)

    row.status = SourceHourStatus.PROCESSED.value
    row.local_path = str(path)
    row.file_size_bytes = len(content)
    row.content_sha256 = hashlib.sha256(content).hexdigest()
    row.processed_at = _hour(10)
    session.flush()

    return spec


def test_retention_cutoff_is_hour_aligned() -> None:
    cutoff = raw_retention_cutoff_hour(
        datetime(
            2087,
            1,
            10,
            12,
            34,
            56,
            tzinfo=timezone.utc,
        ),
        retention_hours=72,
    )

    assert cutoff == datetime(
        2087,
        1,
        7,
        12,
        tzinfo=timezone.utc,
    )


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        0,
        -1,
        1.5,
        "72",
    ],
)
def test_retention_hours_must_be_positive_integer(
    value: object,
) -> None:
    with pytest.raises(
        RawRetentionError,
        match="positive integer",
    ):
        raw_retention_cutoff_hour(
            _hour(),
            retention_hours=value,  # type: ignore[arg-type]
        )


def test_processed_old_archives_are_reported_without_deletion(
    database_session: Session,
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"

    first = _register_processed(
        database_session,
        raw_root,
        offset=0,
        data_kind=SourceDataKind.ORDERBOOK,
        content=b"old-orderbook",
    )
    second = _register_processed(
        database_session,
        raw_root,
        offset=1,
        data_kind=SourceDataKind.TRADES,
        content=b"old-trades",
    )

    plan = plan_processed_raw_retention(
        database_session,
        raw_root=raw_root,
        retention_hours=24,
        now=_hour(48),
        hour_utc_min=_hour(0),
    )

    assert plan.candidate_count == 2
    assert plan.blocked_count == 0
    assert plan.reclaimable_bytes == (len(b"old-orderbook") + len(b"old-trades"))

    assert {candidate.spec.identity_tuple for candidate in plan.candidates} == {
        first.identity_tuple,
        second.identity_tuple,
    }

    for candidate in plan.candidates:
        assert candidate.local_path.is_file()

    payload = plan.to_dict()

    assert payload["schema"] == "l2shock.raw_retention_plan"
    assert payload["schema_version"] == 1
    assert payload["candidate_count"] == 2


def test_downloaded_unprocessed_archive_is_not_candidate(
    database_session: Session,
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    spec = _spec()
    path = spec.local_path(raw_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"downloaded")

    repository = AcquisitionRepository(database_session)
    row = repository.upsert_discovered(spec)

    row.status = SourceHourStatus.DOWNLOADED.value
    row.local_path = str(path)
    row.file_size_bytes = path.stat().st_size
    row.content_sha256 = "c" * 64
    row.processed_at = None
    database_session.flush()

    plan = plan_processed_raw_retention(
        database_session,
        raw_root=raw_root,
        retention_hours=1,
        now=_hour(48),
        hour_utc_min=_hour(0),
    )

    assert plan.candidate_count == 0
    assert plan.blocked_count == 0
    assert path.is_file()


def test_noncanonical_processed_path_is_blocked(
    database_session: Session,
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    wrong_path = tmp_path / "outside" / "wrong.parquet"
    wrong_path.parent.mkdir(parents=True)
    wrong_path.write_bytes(b"wrong")

    spec = _spec()
    repository = AcquisitionRepository(database_session)
    row = repository.upsert_discovered(spec)

    row.status = SourceHourStatus.PROCESSED.value
    row.local_path = str(wrong_path)
    row.file_size_bytes = wrong_path.stat().st_size
    row.content_sha256 = "d" * 64
    row.processed_at = _hour(10)
    database_session.flush()

    plan = plan_processed_raw_retention(
        database_session,
        raw_root=raw_root,
        retention_hours=1,
        now=_hour(48),
        hour_utc_min=_hour(0),
    )

    assert plan.candidate_count == 0
    assert plan.blocked_count == 1
    assert "canonical source path" in plan.blocked[0].reason
    assert wrong_path.is_file()


def test_size_mismatch_is_blocked(
    database_session: Session,
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    spec = _spec()
    path = spec.local_path(raw_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"actual")

    repository = AcquisitionRepository(database_session)
    row = repository.upsert_discovered(spec)

    row.status = SourceHourStatus.PROCESSED.value
    row.local_path = str(path)
    row.file_size_bytes = len(b"actual") + 1
    row.content_sha256 = "e" * 64
    row.processed_at = _hour(10)
    database_session.flush()

    plan = plan_processed_raw_retention(
        database_session,
        raw_root=raw_root,
        retention_hours=1,
        now=_hour(48),
        hour_utc_min=_hour(0),
    )

    assert plan.candidate_count == 0
    assert plan.blocked_count == 1
    assert "size differs" in plan.blocked[0].reason
    assert path.is_file()


def test_same_size_content_mismatch_is_blocked(
    database_session: Session,
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    content = b"original-content"

    spec = _register_processed(
        database_session,
        raw_root,
        offset=0,
        data_kind=SourceDataKind.ORDERBOOK,
        content=content,
    )

    path = spec.local_path(raw_root)

    replacement = b"x" * len(content)
    assert len(replacement) == len(content)

    path.write_bytes(replacement)

    plan = plan_processed_raw_retention(
        database_session,
        raw_root=raw_root,
        retention_hours=1,
        now=_hour(48),
        hour_utc_min=_hour(0),
    )

    assert plan.candidate_count == 0
    assert plan.blocked_count == 1
    assert "SHA-256 differs" in plan.blocked[0].reason
    assert path.is_file()

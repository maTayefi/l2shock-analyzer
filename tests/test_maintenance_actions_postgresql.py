"""PostgreSQL integration tests for explicitly authorized maintenance actions.

These tests use the same rollback-isolated fixture pattern as the existing
repository integration tests. The outer transaction is always rolled back,
so no test data persists in the configured application database.

Because ``build_maintenance_preview`` and ``execute_maintenance_action``
internally call ``session_scope()``, we monkeypatch that symbol within the
``l2shock.maintenance_actions`` module to yield the test session without
committing. This preserves rollback isolation.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.orm import Session

from l2shock.acquisition import (
    AcquisitionRepository,
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.db import get_engine
from l2shock.db.models import SourceHour
from l2shock.db.schema import verify_schema
from l2shock.ingest.checkpoint_codec import (
    encode_checkpoint,
    write_checkpoint_file,
)
from l2shock.ingest.parquet_reader import BookSide
from l2shock.ingest.replay import (
    CheckpointLevel,
    OrderBookCheckpoint,
)
from l2shock.maintenance_actions import (
    MaintenanceActionKind,
    MaintenancePreview,
    MaintenancePreviewChangedError,
    MaintenancePreviewExpiredError,
    build_maintenance_preview,
    execute_maintenance_action,
)
from l2shock.processing.checkpoint_store import CheckpointStore

pytestmark = pytest.mark.postgresql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2088,
        1,
        1,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _spec(
    *,
    offset: int = 0,
    data_kind: SourceDataKind = SourceDataKind.ORDERBOOK,
    symbol: str = "BTCUSDT",
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol=symbol,
        data_kind=data_kind,
        hour_utc=_hour(offset),
    )


def _checkpoint(
    offset: int = 0,
    *,
    last_update_id: int = 100,
) -> OrderBookCheckpoint:
    return OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(offset),
        last_update_id=last_update_id,
        levels=(
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("59999"),
                quantity=Decimal("2"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("60001"),
                quantity=Decimal("1.5"),
                order_count=None,
            ),
        ),
        source_content_sha256="a" * 64,
    )


@pytest.fixture
def database_session() -> Iterator[Session]:
    """Provide a real PostgreSQL session isolated by an outer rollback."""
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


@pytest.fixture
def patched_session_scope(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> Session:
    """Monkeypatch session_scope inside maintenance_actions to use the test session."""

    @contextmanager
    def _fake_scope():
        yield database_session

    import l2shock.maintenance_actions as ma_module

    monkeypatch.setattr(ma_module, "session_scope", _fake_scope)
    return database_session


@pytest.fixture
def raw_root(tmp_path: Path) -> Path:
    return tmp_path / "raw"


@pytest.fixture
def cache_root(tmp_path: Path) -> Path:
    return tmp_path / "cache"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _register_stale_source(
    session: Session,
    *,
    status: SourceHourStatus,
    offset: int = 0,
    data_kind: SourceDataKind = SourceDataKind.ORDERBOOK,
    local_path: str | None = None,
    file_size_bytes: int | None = None,
    content_sha256: str | None = None,
    discovered_hours_ago: int = 10,
    downloaded_hours_ago: int | None = None,
) -> SourceHour:
    """Insert a source row in a transient status with an old timestamp."""
    spec = _spec(offset=offset, data_kind=data_kind)
    repository = AcquisitionRepository(session)
    row = repository.upsert_discovered(spec)
    row.status = status.value
    row.discovered_at = _hour() - timedelta(hours=discovered_hours_ago)

    if downloaded_hours_ago is not None:
        row.downloaded_at = _hour() - timedelta(hours=downloaded_hours_ago)

    if local_path is not None:
        row.local_path = local_path
    if file_size_bytes is not None:
        row.file_size_bytes = file_size_bytes
    if content_sha256 is not None:
        row.content_sha256 = content_sha256

    session.flush()
    return row


def _register_processed_source(
    session: Session,
    raw_root: Path,
    *,
    offset: int = 0,
    data_kind: SourceDataKind = SourceDataKind.ORDERBOOK,
    content: bytes = b"processed-raw-content",
) -> SourceFileSpec:
    """Register a processed source with a real local file."""
    spec = _spec(offset=offset, data_kind=data_kind)
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
    row.quality_state = "VALID"
    row.quality_json = {
        "schema": "l2shock.processed_source_hour_quality",
        "schema_version": 1,
        "quality_state": "VALID",
    }
    session.flush()
    return spec


def _publish_checkpoint(
    cache_root: Path,
    offset: int = 0,
    *,
    last_update_id: int = 100,
) -> Path:
    """Write a valid checkpoint file into the content-addressed store."""
    store = CheckpointStore(cache_root)
    checkpoint = _checkpoint(offset, last_update_id=last_update_id)
    encoded = encode_checkpoint(checkpoint)

    from l2shock.ingest.checkpoint_codec import checkpoint_encoding_info

    info = checkpoint_encoding_info(encoded)
    identity_path = store.path_for(
        __import__(
            "l2shock.processing.checkpoint_store",
            fromlist=["CheckpointIdentity"],
        ).CheckpointIdentity.from_checkpoint(checkpoint),
        info.content_sha256,
    )
    identity_path.parent.mkdir(parents=True, exist_ok=True)
    identity_path.write_bytes(encoded)
    return identity_path


def _reference_checkpoint_in_source(
    session: Session,
    *,
    offset: int = 0,
    content_sha256: str,
) -> None:
    """Write output_checkpoint_content_sha256 into a source quality_json."""
    spec = _spec(offset=offset)
    repository = AcquisitionRepository(session)
    row = repository.upsert_discovered(spec)
    row.status = SourceHourStatus.PROCESSED.value
    row.quality_json = {
        "output_checkpoint_content_sha256": content_sha256,
    }
    session.flush()


# ---------------------------------------------------------------------------
# 1. Stale processing with intact raw file returns to downloaded
# ---------------------------------------------------------------------------


def test_stale_processing_with_intact_raw_returns_to_downloaded(
    patched_session_scope: Session,
    tmp_path: Path,
) -> None:
    session = patched_session_scope
    raw_file = tmp_path / "intact.parquet"
    raw_file.write_bytes(b"valid-raw-data")

    _register_stale_source(
        session,
        status=SourceHourStatus.PROCESSING,
        offset=0,
        local_path=str(raw_file),
        file_size_bytes=raw_file.stat().st_size,
        content_sha256=hashlib.sha256(b"valid-raw-data").hexdigest(),
        downloaded_hours_ago=10,
    )

    preview = build_maintenance_preview(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_hour(),
    )

    assert preview.candidate_count == 1
    assert preview.items[0]["recovery_status"] == "downloaded"

    report = execute_maintenance_action(
        preview,
        executed_at_utc=None,
    )

    assert report.status == "ok"
    assert report.succeeded_count == 1
    assert report.failed_count == 0

    row = (
        session.query(SourceHour).filter_by(id=preview.items[0]["source_hour_id"]).one()
    )
    assert row.status == SourceHourStatus.DOWNLOADED.value
    assert row.processed_at is None


# ---------------------------------------------------------------------------
# 2. Stale processing without intact raw file becomes error
# ---------------------------------------------------------------------------


def test_stale_processing_without_raw_becomes_error(
    patched_session_scope: Session,
    tmp_path: Path,
) -> None:
    session = patched_session_scope
    missing_path = tmp_path / "does_not_exist.parquet"

    _register_stale_source(
        session,
        status=SourceHourStatus.PROCESSING,
        offset=1,
        local_path=str(missing_path),
        file_size_bytes=100,
        content_sha256="b" * 64,
        downloaded_hours_ago=10,
    )

    preview = build_maintenance_preview(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_hour(),
    )

    assert preview.candidate_count == 1
    assert preview.items[0]["recovery_status"] == "error"

    report = execute_maintenance_action(
        preview,
        executed_at_utc=None,
    )

    assert report.succeeded_count == 1
    row = (
        session.query(SourceHour).filter_by(id=preview.items[0]["source_hour_id"]).one()
    )
    assert row.status == SourceHourStatus.ERROR.value


# ---------------------------------------------------------------------------
# 3. Stale downloading becomes error
# ---------------------------------------------------------------------------


def test_stale_downloading_becomes_error(
    patched_session_scope: Session,
) -> None:
    session = patched_session_scope

    _register_stale_source(
        session,
        status=SourceHourStatus.DOWNLOADING,
        offset=2,
        discovered_hours_ago=10,
    )

    preview = build_maintenance_preview(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_hour(),
    )

    assert preview.candidate_count == 1
    assert preview.items[0]["recovery_status"] == "error"

    report = execute_maintenance_action(
        preview,
        executed_at_utc=None,
    )

    assert report.succeeded_count == 1
    row = (
        session.query(SourceHour).filter_by(id=preview.items[0]["source_hour_id"]).one()
    )
    assert row.status == SourceHourStatus.ERROR.value


# ---------------------------------------------------------------------------
# 4. Referenced checkpoint is never included in deletion preview
# ---------------------------------------------------------------------------


def test_referenced_checkpoint_not_in_deletion_preview(
    patched_session_scope: Session,
    cache_root: Path,
) -> None:
    session = patched_session_scope

    checkpoint_path = _publish_checkpoint(cache_root, offset=-50)

    from l2shock.ingest.checkpoint_codec import (
        checkpoint_encoding_info,
        load_checkpoint_file,
    )

    loaded = load_checkpoint_file(checkpoint_path)
    encoded = encode_checkpoint(loaded)
    info = checkpoint_encoding_info(encoded)

    _reference_checkpoint_in_source(
        session,
        offset=-50,
        content_sha256=info.content_sha256,
    )

    # Make the file old enough to pass the age check
    old_time = datetime.now(timezone.utc) - timedelta(hours=48)
    os.utime(checkpoint_path, (old_time.timestamp(), old_time.timestamp()))

    preview = build_maintenance_preview(
        MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS,
        generated_at_utc=datetime.now(timezone.utc),
        settings=_make_settings_with_cache(cache_root),
    )

    # The referenced checkpoint must NOT appear as a deletion candidate
    candidate_paths = [item["path"] for item in preview.items]
    assert str(checkpoint_path) not in candidate_paths


# ---------------------------------------------------------------------------
# 5. Unreferenced checkpoint younger than 24 hours is blocked
# ---------------------------------------------------------------------------


def test_young_unreferenced_checkpoint_is_blocked(
    patched_session_scope: Session,
    cache_root: Path,
) -> None:
    _publish_checkpoint(cache_root, offset=-1)

    preview = build_maintenance_preview(
        MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS,
        generated_at_utc=datetime.now(timezone.utc),
        settings=_make_settings_with_cache(cache_root),
    )

    # Young checkpoint should be blocked, not a candidate
    assert preview.candidate_count == 0
    assert preview.blocked_count >= 1


# ---------------------------------------------------------------------------
# 6. Old unreferenced canonical checkpoint is deleted
# ---------------------------------------------------------------------------


def test_old_unreferenced_checkpoint_is_deleted(
    patched_session_scope: Session,
    cache_root: Path,
) -> None:
    checkpoint_path = _publish_checkpoint(cache_root, offset=-100)

    # Make the file old
    old_time = datetime.now(timezone.utc) - timedelta(hours=48)
    os.utime(checkpoint_path, (old_time.timestamp(), old_time.timestamp()))

    preview = build_maintenance_preview(
        MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS,
        generated_at_utc=datetime.now(timezone.utc),
        settings=_make_settings_with_cache(cache_root),
    )

    assert preview.candidate_count == 1
    assert checkpoint_path.is_file()

    report = execute_maintenance_action(
        preview,
        executed_at_utc=datetime.now(timezone.utc),
        settings=_make_settings_with_cache(cache_root),
    )

    assert report.succeeded_count == 1
    assert report.failed_count == 0
    assert report.affected_bytes > 0
    assert not checkpoint_path.is_file()


# ---------------------------------------------------------------------------
# 7. Raw pruning clears only local_path
# ---------------------------------------------------------------------------


def test_raw_pruning_clears_only_local_path(
    patched_session_scope: Session,
    raw_root: Path,
) -> None:
    session = patched_session_scope
    spec = _register_processed_source(session, raw_root, offset=0)
    local_path = spec.local_path(raw_root)

    assert local_path.is_file()

    preview = build_maintenance_preview(
        MaintenanceActionKind.PRUNE_RAW_FILES,
        generated_at_utc=_hour(48),
        settings=_make_settings_with_raw(raw_root),
    )

    assert preview.candidate_count == 1

    report = execute_maintenance_action(
        preview,
        executed_at_utc=None,
        settings=_make_settings_with_raw(raw_root),
    )

    assert report.succeeded_count == 1
    assert not local_path.is_file()

    row = (
        session.query(SourceHour)
        .filter_by(
            provider=spec.provider,
            venue=spec.venue,
            data_kind=spec.data_kind.value,
            instrument=spec.symbol,
            hour_utc=spec.hour_utc,
        )
        .one()
    )
    assert row.local_path is None


# ---------------------------------------------------------------------------
# 8. Raw pruning preserves status, size, hash, quality, and analytical rows
# ---------------------------------------------------------------------------


def test_raw_pruning_preserves_durable_metadata(
    patched_session_scope: Session,
    raw_root: Path,
) -> None:
    session = patched_session_scope
    content = b"important-processed-data"
    spec = _register_processed_source(
        session,
        raw_root,
        offset=1,
        content=content,
    )

    expected_sha = hashlib.sha256(content).hexdigest()
    expected_size = len(content)

    preview = build_maintenance_preview(
        MaintenanceActionKind.PRUNE_RAW_FILES,
        generated_at_utc=_hour(48),
        settings=_make_settings_with_raw(raw_root),
    )

    execute_maintenance_action(
        preview,
        executed_at_utc=None,
        settings=_make_settings_with_raw(raw_root),
    )

    row = (
        session.query(SourceHour)
        .filter_by(
            provider=spec.provider,
            venue=spec.venue,
            data_kind=spec.data_kind.value,
            instrument=spec.symbol,
            hour_utc=spec.hour_utc,
        )
        .one()
    )

    # Status remains processed
    assert row.status == SourceHourStatus.PROCESSED.value
    # Durable size and hash preserved
    assert row.file_size_bytes == expected_size
    assert row.content_sha256 == expected_sha
    # Quality state preserved
    assert row.quality_state == "VALID"
    assert row.quality_json["quality_state"] == "VALID"
    # local_path cleared
    assert row.local_path is None


# ---------------------------------------------------------------------------
# 9. Changed preview token refuses execution
# ---------------------------------------------------------------------------


def test_changed_preview_token_refuses_execution(
    patched_session_scope: Session,
) -> None:
    _register_stale_source(
        patched_session_scope,
        status=SourceHourStatus.DOWNLOADING,
        offset=3,
        discovered_hours_ago=10,
    )

    preview = build_maintenance_preview(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_hour(),
    )

    # Tamper with the token
    tampered = MaintenancePreview(
        action=preview.action,
        generated_at_utc=preview.generated_at_utc,
        expires_at_utc=preview.expires_at_utc,
        token="f" * 64,  # wrong token
        candidate_count=preview.candidate_count,
        candidate_bytes=preview.candidate_bytes,
        blocked_count=preview.blocked_count,
        items=preview.items,
    )

    with pytest.raises(MaintenancePreviewChangedError):
        execute_maintenance_action(
            tampered,
            executed_at_utc=_hour(),
        )


# ---------------------------------------------------------------------------
# 10. Expired preview refuses execution
# ---------------------------------------------------------------------------


def test_expired_preview_refuses_execution(
    patched_session_scope: Session,
) -> None:
    _register_stale_source(
        patched_session_scope,
        status=SourceHourStatus.DOWNLOADING,
        offset=4,
        discovered_hours_ago=10,
    )

    preview = build_maintenance_preview(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_hour(),
    )

    # Execute well after expiry
    with pytest.raises(MaintenancePreviewExpiredError):
        execute_maintenance_action(
            preview,
            executed_at_utc=_hour() + timedelta(hours=1),
        )


# ---------------------------------------------------------------------------
# Settings helpers for filesystem-dependent tests
# ---------------------------------------------------------------------------


def _make_settings_with_cache(cache_root: Path):
    """Build a minimal settings-like object pointing at a temp cache root."""
    from unittest.mock import MagicMock

    settings = MagicMock()
    settings.storage.cache_path = cache_root
    settings.storage.raw_path = cache_root.parent / "raw"
    settings.storage.raw_retention_hours = 24
    return settings


def _make_settings_with_raw(raw_root: Path):
    """Build a minimal settings-like object pointing at a temp raw root."""
    from unittest.mock import MagicMock

    settings = MagicMock()
    settings.storage.raw_path = raw_root
    settings.storage.cache_path = raw_root.parent / "cache"
    settings.storage.raw_retention_hours = 24
    return settings


def test_stale_processing_with_same_size_modified_raw_becomes_error(
    patched_session_scope: Session,
    tmp_path: Path,
) -> None:
    session = patched_session_scope

    original = b"original-source-content"
    modified = b"x" * len(original)

    raw_file = tmp_path / "same-size-modified.parquet"
    raw_file.write_bytes(modified)

    _register_stale_source(
        session,
        status=SourceHourStatus.PROCESSING,
        offset=5,
        local_path=str(raw_file),
        file_size_bytes=len(original),
        content_sha256=hashlib.sha256(original).hexdigest(),
        downloaded_hours_ago=10,
    )

    preview = build_maintenance_preview(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_hour(),
    )

    selected = next(
        item
        for item in preview.items
        if int(item["source_hour_id"]) > 0
        and str(item["hour_utc"]) == _hour(5).isoformat()
    )

    assert selected["recovery_status"] == "error"

    report = execute_maintenance_action(
        preview,
        executed_at_utc=None,
    )

    assert report.failed_count == 0

    row = session.get(
        SourceHour,
        int(selected["source_hour_id"]),
    )

    assert row is not None
    assert row.status == SourceHourStatus.ERROR.value


def test_analytical_provenance_protects_input_checkpoint(
    database_session: Session,
    tmp_path: Path,
) -> None:
    from l2shock.timeutils import now_utc
    import l2shock.maintenance_actions as maintenance_actions
    from l2shock.db.models import DataPreset, L2HourlySeries

    checkpoint = _checkpoint()
    store = CheckpointStore(tmp_path / "cache")
    artifact = store.publish(checkpoint)
    digest = artifact.encoding_info.content_sha256

    # FIX: Insert the required DataPreset to satisfy the foreign key constraint
    database_session.add(
        DataPreset(
            preset_hash="a" * 64,
            schema_version=1,
            algorithm_version="test-algorithm",
            base="BTC",
            enabled=True,
            config_json={},
        )
    )

    # Write a minimal L2HourlySeries row whose provenance references
    # the checkpoint digest, proving the reference collector protects it.
    database_session.add(
        L2HourlySeries(
            base="BTC",
            hour_utc=datetime(2088, 1, 1, 13, tzinfo=timezone.utc),
            preset_hash="a" * 64,
            schema_version=1,
            sampling_interval_ms=1000,
            observation_count=3600,
            codec="arrow-ipc-zstd",
            bid_liquidity_block=b"\x01",
            ask_liquidity_block=b"\x01",
            validity_block=b"\x01",
            source_count_block=b"\x01",
            quality_summary_json={
                "schema": "l2shock.liquidity_quality_summary",
                "schema_version": 1,
                "observation_count": 3600,
                "valid_count": 3600,
                "degraded_count": 0,
                "invalid_count": 0,
                "zero_total_liquidity_count": 0,
                "invalid_reason_counts": {},
            },
            provenance_json={
                "schema": "l2shock.l2_hourly_series_provenance",
                "schema_version": 1,
                "replay_schema_version": 1,
                "liquidity_schema_version": 1,
                "checkpoint_content_sha256": digest,
                "source_hours": [
                    {
                        "provider": "cryptohftdata",
                        "venue": "binance_futures",
                        "instrument": "BTCUSDT",
                        "data_kind": "orderbook",
                        "hour_utc": "2088-01-01T12:00:00Z",
                        "content_sha256": "a" * 64,
                    }
                ],
            },
            content_sha256="b" * 64,
        )
    )
    database_session.flush()

    # Make the checkpoint file old enough to pass the age check.
    old_time = datetime.now(timezone.utc) - timedelta(hours=48)
    os.utime(
        artifact.path,
        (old_time.timestamp(), old_time.timestamp()),
    )

    generated = now_utc() + timedelta(days=2)
    items, _candidate_bytes, _blocked_count = (
        maintenance_actions._plan_orphan_checkpoints(
            database_session,
            cache_root=tmp_path / "cache",
            generated_at_utc=generated,
            minimum_age=timedelta(hours=24),
        )
    )
    # The checkpoint referenced by L2 provenance must NOT appear
    # as a deletion candidate.
    assert all(item["content_sha256"] != digest for item in items)


def test_stale_processing_revalidates_raw_under_execution_lock(
    patched_session_scope: Session,
    tmp_path: Path,
) -> None:
    session = patched_session_scope
    original = b"original-locked-source"
    changed = b"x" * len(original)

    raw_file = tmp_path / "changed-after-preview.parquet"
    raw_file.write_bytes(original)

    row = _register_stale_source(
        session,
        status=SourceHourStatus.PROCESSING,
        offset=6,
        local_path=str(raw_file),
        file_size_bytes=len(original),
        content_sha256=hashlib.sha256(original).hexdigest(),
        downloaded_hours_ago=10,
    )

    preview = build_maintenance_preview(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_hour(),
    )

    selected = next(
        item for item in preview.items if int(item["source_hour_id"]) == int(row.id)
    )

    assert selected["recovery_status"] == "downloaded"

    raw_file.write_bytes(changed)

    report = execute_maintenance_action(
        preview,
        executed_at_utc=_hour(),
    )

    session.refresh(row)

    assert report.succeeded_count == 0
    assert report.failed_count == 1
    assert row.status == SourceHourStatus.PROCESSING.value
    assert row.processed_at is None


def test_restart_reconciliation_restores_rolled_back_pruning_rename(
    database_session: Session,
    raw_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.acquisition.pruning_recovery as recovery_module

    spec = _register_processed_source(
        database_session,
        raw_root,
        offset=20,
        content=b"rolled-back-pruning-content",
    )
    canonical = spec.local_path(raw_root)
    pruning = canonical.with_name(f".{canonical.name}.{'a' * 32}.pruning")

    canonical.replace(pruning)

    row = AcquisitionRepository(
        database_session,
    ).get_source_hour(spec)

    assert row is not None
    assert row.status == SourceHourStatus.PROCESSED.value
    assert Path(str(row.local_path)).resolve() == canonical.resolve()
    assert pruning.is_file()
    assert not canonical.exists()

    @contextmanager
    def test_scope():
        yield database_session

    monkeypatch.setattr(
        recovery_module,
        "session_scope",
        test_scope,
    )

    report = recovery_module.reconcile_stranded_raw_pruning(
        raw_root,
    )

    assert report.examined_count == 1
    assert report.restored_count == 1
    assert report.deleted_count == 0
    assert report.failed_count == 0

    assert canonical.is_file()
    assert canonical.read_bytes() == b"rolled-back-pruning-content"
    assert not pruning.exists()


def test_restart_reconciliation_finishes_committed_pruning_deletion(
    database_session: Session,
    raw_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.acquisition.pruning_recovery as recovery_module

    spec = _register_processed_source(
        database_session,
        raw_root,
        offset=21,
        content=b"committed-pruning-content",
    )
    canonical = spec.local_path(raw_root)
    pruning = canonical.with_name(f".{canonical.name}.{'b' * 32}.pruning")

    canonical.replace(pruning)

    row = AcquisitionRepository(
        database_session,
    ).get_source_hour(spec)

    assert row is not None
    row.local_path = None
    database_session.flush()

    assert pruning.is_file()
    assert not canonical.exists()

    @contextmanager
    def test_scope():
        yield database_session

    monkeypatch.setattr(
        recovery_module,
        "session_scope",
        test_scope,
    )

    report = recovery_module.reconcile_stranded_raw_pruning(
        raw_root,
    )

    assert report.examined_count == 1
    assert report.restored_count == 0
    assert report.deleted_count == 1
    assert report.failed_count == 0

    assert not pruning.exists()
    assert not canonical.exists()
    assert row.local_path is None


def test_restart_reconciliation_retains_digest_conflict(
    database_session: Session,
    raw_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.acquisition.pruning_recovery as recovery_module

    original = b"immutable-pruning-content"

    spec = _register_processed_source(
        database_session,
        raw_root,
        offset=22,
        content=original,
    )
    canonical = spec.local_path(raw_root)
    pruning = canonical.with_name(f".{canonical.name}.{'c' * 32}.pruning")

    canonical.replace(pruning)
    pruning.write_bytes(b"x" * len(original))

    row = AcquisitionRepository(
        database_session,
    ).get_source_hour(spec)

    assert row is not None
    assert row.content_sha256 == hashlib.sha256(original).hexdigest()

    @contextmanager
    def test_scope():
        yield database_session

    monkeypatch.setattr(
        recovery_module,
        "session_scope",
        test_scope,
    )

    report = recovery_module.reconcile_stranded_raw_pruning(
        raw_root,
    )

    assert report.examined_count == 1
    assert report.restored_count == 0
    assert report.deleted_count == 0
    assert report.failed_count == 1

    assert pruning.is_file()
    assert not canonical.exists()


def test_stale_running_fetch_run_is_reported_read_only(
    database_session: Session,
) -> None:
    from uuid import uuid4

    from l2shock.maintenance_diagnostics import (
        stale_fetch_run_diagnostics,
    )

    repository = AcquisitionRepository(database_session)
    operation_id = uuid4()

    run = repository.create_fetch_run(
        operation_id=operation_id,
        kind="manual",
        requested_start_utc=_hour(30),
        requested_end_utc=_hour(31),
        files_requested=6,
        details={
            "phase": "started",
        },
    )
    run.started_at = _hour(20)
    database_session.flush()

    report = stale_fetch_run_diagnostics(
        database_session,
        generated_at_utc=_hour(24),
        stale_after=timedelta(hours=2),
    )

    assert report["running_fetch_run_count"] >= 1
    assert report["stale_running_fetch_run_count"] >= 1
    assert report["recovery_performed"] is False

    item = next(
        value
        for value in report["stale_running_fetch_runs"]
        if value["operation_id"] == str(operation_id)
    )

    assert item["status"] == "running"
    assert item["kind"] == "manual"
    assert item["age_seconds"] == 14_400.0

    database_session.refresh(run)

    assert run.status == "running"
    assert run.ended_at is None

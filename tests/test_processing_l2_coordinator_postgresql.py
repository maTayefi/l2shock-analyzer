from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy.orm import Session

from l2shock.acquisition import (
    AcquisitionRepository,
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
    sha256_file,
)
from l2shock.db import AnalyticalRepository, get_engine
from l2shock.db.schema import verify_schema
from l2shock.presets import build_binance_futures_data_preset
from l2shock.processing import (
    CheckpointStore,
    ProcessingCancelledError,
    ProcessingQualityState,
    ProcessingRequest,
    ProcessingSourceArchive,
    SingleMarketL2ProcessingCoordinator,
)
from l2shock.remote import process_l2_archive_headlessly

pytestmark = pytest.mark.postgresql


def _hour() -> datetime:
    return datetime(
        2084,
        1,
        1,
        12,
        tzinfo=timezone.utc,
    )


def _epoch_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )


def _write_snapshot(
    path: Path,
    *,
    ask_price: str = "101",
) -> None:
    received = _epoch_ns(_hour()) + 100_000_000
    event_time = received // 1_000_000

    table = pa.Table.from_pylist(
        [
            {
                "received_time": received,
                "event_time": event_time,
                "transaction_time": event_time,
                "symbol": "BTCUSDT",
                "event_type": "snapshot",
                "first_update_id": None,
                "final_update_id": None,
                "prev_final_update_id": None,
                "last_update_id": 100,
                "side": "bid",
                "price": "100",
                "quantity": "2",
                "order_count": None,
            },
            {
                "received_time": received,
                "event_time": event_time,
                "transaction_time": event_time,
                "symbol": "BTCUSDT",
                "event_type": "snapshot",
                "first_update_id": None,
                "final_update_id": None,
                "prev_final_update_id": None,
                "last_update_id": 100,
                "side": "ask",
                "price": ask_price,
                "quantity": "3",
                "order_count": None,
            },
        ]
    )

    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=1,
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


def _register_source(
    session: Session,
    path: Path,
) -> None:
    digest, size = sha256_file(path)
    repository = AcquisitionRepository(session)
    row = repository.upsert_discovered(_spec())

    row.status = SourceHourStatus.DOWNLOADED.value
    row.local_path = str(path.resolve())
    row.file_size_bytes = size
    row.content_sha256 = digest
    row.row_count = 2
    row.quality_state = "INVALID"
    session.flush()


def test_single_market_processing_persists_target_and_checkpoint(
    database_session: Session,
    tmp_path: Path,
) -> None:
    source = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_snapshot(source)
    _register_source(database_session, source)

    @contextmanager
    def scope():
        yield database_session

    coordinator = SingleMarketL2ProcessingCoordinator(
        checkpoint_store=CheckpointStore(tmp_path / "cache"),
        session_scope_factory=scope,
        batch_size=1,
    )

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )

    result = coordinator.run(
        ProcessingRequest(
            operation_id=uuid4(),
            target=_spec(),
            max_checkpoint_search_hours=24,
        ),
        preset,
    )

    assert result.quality_state is ProcessingQualityState.VALID
    assert result.valid_count == 3_600
    assert result.degraded_count == 0
    assert result.invalid_count == 0
    assert result.output_checkpoint_content_sha256 is not None
    assert result.analytical_inserted is True
    assert result.preset_inserted is True

    stored = AnalyticalRepository(database_session).get_l2_hour(
        base="BTC",
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )

    assert stored is not None
    assert stored.encoded.content_sha256 == result.analytical_content_sha256

    row = AcquisitionRepository(database_session).get_source_hour(_spec())

    assert row is not None
    assert row.status == SourceHourStatus.PROCESSED.value
    assert row.quality_state == "VALID"
    assert row.event_count == 1
    assert row.snapshot_count == 1
    assert row.continuity_mismatch_count == 0
    assert row.processed_at is not None


def test_processing_cancellation_resets_processing_to_downloaded(
    database_session: Session,
    tmp_path: Path,
) -> None:
    source = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_snapshot(source)
    _register_source(database_session, source)

    @contextmanager
    def scope():
        yield database_session

    coordinator = SingleMarketL2ProcessingCoordinator(
        checkpoint_store=CheckpointStore(tmp_path / "cache"),
        session_scope_factory=scope,
        batch_size=1,
    )

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )

    checks = 0

    def cancel() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 3

    with pytest.raises(
        ProcessingCancelledError,
        match="cancellation|cancelled",
    ):
        coordinator.run(
            ProcessingRequest(
                operation_id=uuid4(),
                target=_spec(),
                max_checkpoint_search_hours=24,
            ),
            preset,
            cancellation_probe=cancel,
        )

    row = AcquisitionRepository(database_session).get_source_hour(_spec())

    assert row is not None
    assert row.status == SourceHourStatus.DOWNLOADED.value

    assert (
        AnalyticalRepository(database_session).get_l2_hour(
            base="BTC",
            hour_utc=_hour(),
            preset_hash=preset.preset_hash,
        )
        is None
    )


def test_local_coordinator_and_headless_l2_outputs_are_identical(
    database_session: Session,
    tmp_path: Path,
) -> None:
    source = tmp_path / "BTCUSDT_equivalence_orderbook.parquet"
    _write_snapshot(source)

    digest, size = sha256_file(source)

    archive = ProcessingSourceArchive(
        spec=_spec(),
        local_path=source,
        content_sha256=digest,
        file_size_bytes=size,
        status=SourceHourStatus.DOWNLOADED,
    )

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )

    headless = process_l2_archive_headlessly(
        archive,
        preset,
        batch_size=1,
    )

    _register_source(
        database_session,
        source,
    )

    @contextmanager
    def scope():
        yield database_session

    coordinator = SingleMarketL2ProcessingCoordinator(
        checkpoint_store=CheckpointStore(tmp_path / "local-equivalence-cache"),
        session_scope_factory=scope,
        batch_size=1,
    )

    local_result = coordinator.run(
        ProcessingRequest(
            operation_id=uuid4(),
            target=_spec(),
            max_checkpoint_search_hours=24,
        ),
        preset,
    )

    stored = AnalyticalRepository(
        database_session,
    ).get_l2_hour(
        base="BTC",
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )

    assert stored is not None
    assert stored.encoded == headless.artifact.encoded
    assert (
        local_result.analytical_content_sha256
        == headless.artifact.manifest.content_sha256
    )
    assert (
        local_result.output_checkpoint_content_sha256
        == headless.artifact.manifest.output_checkpoint_content_sha256
    )
    assert headless.artifact.manifest.source_hours[0].content_sha256 == digest


def test_all_invalid_locked_hour_does_not_publish_checkpoint(
    database_session: Session,
    tmp_path: Path,
) -> None:
    source = tmp_path / "BTCUSDT_locked_orderbook.parquet"

    # Equal best bid and ask retain sequence-valid replay state while making
    # every sampled second analytically invalid because the book is locked.
    _write_snapshot(
        source,
        ask_price="100",
    )
    _register_source(
        database_session,
        source,
    )

    @contextmanager
    def scope():
        yield database_session

    cache_root = tmp_path / "locked-cache"
    coordinator = SingleMarketL2ProcessingCoordinator(
        checkpoint_store=CheckpointStore(cache_root),
        session_scope_factory=scope,
        batch_size=1,
    )

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )

    result = coordinator.run(
        ProcessingRequest(
            operation_id=uuid4(),
            target=_spec(),
            max_checkpoint_search_hours=24,
        ),
        preset,
    )

    assert result.quality_state is ProcessingQualityState.INVALID
    assert result.valid_count == 0
    assert result.invalid_count == 3_600
    assert result.output_checkpoint_content_sha256 is None

    row = AcquisitionRepository(
        database_session,
    ).get_source_hour(_spec())

    assert row is not None
    assert row.status == SourceHourStatus.PROCESSED.value
    assert row.quality_state == ProcessingQualityState.INVALID.value
    assert row.quality_json["output_checkpoint_content_sha256"] is None

    assert not tuple(cache_root.rglob("*.l2checkpoint"))

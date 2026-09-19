from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy.orm import Session

import l2shock.processing.price_coordinator as price_coordinator_module

from l2shock.acquisition import (
    AcquisitionRepository,
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
    sha256_file,
)
from l2shock.db import (
    PriceAnalyticalRepository,
    get_engine,
)
from l2shock.db.schema import verify_schema
from l2shock.processing import (
    PriceProcessingRequest,
    ProcessingCancelledError,
    ProcessingQualityState,
    ProcessingSourceArchive,
    SingleMarketPriceProcessingCoordinator,
)
from l2shock.remote import process_price_archives_headlessly

pytestmark = pytest.mark.postgresql


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2085,
        1,
        1,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _epoch_ms(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_hour(offset),
    )


def _write_trade_archive(
    path: Path,
    *,
    source_hour_offset: int,
    trade_times_and_prices: tuple[tuple[datetime, str], ...],
) -> None:
    rows: list[dict[str, object]] = []

    for index, (trade_time, price) in enumerate(
        trade_times_and_prices,
    ):
        trade_time_ms = _epoch_ms(trade_time)

        rows.append(
            {
                "received_time": trade_time_ms * 1_000_000,
                "event_time": trade_time_ms,
                "symbol": "BTCUSDT",
                "trade_id": (f"{source_hour_offset}:{index}:{trade_time_ms}"),
                "price": price,
                "quantity": "1",
                "trade_time": trade_time_ms,
                "is_buyer_maker": False,
                "order_type": "market",
            }
        )

    pq.write_table(
        pa.Table.from_pylist(rows),
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
    spec: SourceFileSpec,
    path: Path,
) -> None:
    digest, size = sha256_file(path)

    repository = AcquisitionRepository(session)
    row = repository.upsert_discovered(spec)

    row.status = SourceHourStatus.DOWNLOADED.value
    row.local_path = str(path.resolve())
    row.file_size_bytes = size
    row.content_sha256 = digest
    row.row_count = int(pq.ParquetFile(path).metadata.num_rows)
    row.quality_state = "INVALID"
    session.flush()


def test_price_processing_persists_real_trade_hour(
    database_session: Session,
    tmp_path: Path,
) -> None:
    source = tmp_path / "BTCUSDT_trades.parquet"

    _write_trade_archive(
        source,
        source_hour_offset=0,
        trade_times_and_prices=(
            (_hour() + timedelta(milliseconds=100), "100"),
            (_hour() + timedelta(milliseconds=500), "105"),
            (_hour() + timedelta(milliseconds=900), "102"),
            (_hour() + timedelta(seconds=2, milliseconds=100), "99"),
        ),
    )
    _register_source(
        database_session,
        _spec(),
        source,
    )

    @contextmanager
    def scope():
        yield database_session

    coordinator = SingleMarketPriceProcessingCoordinator(
        session_scope_factory=scope,
        batch_size=1,
        cancellation_check_interval_rows=1,
        cancellation_check_interval_records=1,
    )

    result = coordinator.run(
        PriceProcessingRequest(
            operation_id=uuid4(),
            target=_spec(),
        )
    )

    assert result.quality_state is ProcessingQualityState.DEGRADED
    assert result.valid_count == 2
    assert result.invalid_count == 3_598
    assert result.total_trade_count == 4
    assert result.source_archive_count == 1
    assert result.price_inserted is True

    stored = PriceAnalyticalRepository(database_session).get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )

    assert stored is not None
    assert stored.encoded.content_sha256 == result.price_content_sha256

    source_row = AcquisitionRepository(database_session).get_source_hour(_spec())

    assert source_row is not None
    assert source_row.status == SourceHourStatus.PROCESSED.value
    assert source_row.quality_state == "DEGRADED"
    assert source_row.processed_at is not None
    assert source_row.quality_json["source_selection_policy"] == (
        "current_source_only_v1"
    )
    assert source_row.quality_json["accepted_trade_count"] == 4


def test_explicit_adjacent_source_routing_is_recorded(
    database_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_path = tmp_path / "previous.parquet"
    current_path = tmp_path / "current.parquet"
    following_path = tmp_path / "following.parquet"

    # Every archive contains at least one row so it remains valid Parquet input.
    # The previous and following source archives deliberately contain trades
    # whose authoritative trade_time_ms belongs to the target UTC hour.
    _write_trade_archive(
        previous_path,
        source_hour_offset=-1,
        trade_times_and_prices=((_hour() + timedelta(milliseconds=100), "100"),),
    )
    _write_trade_archive(
        current_path,
        source_hour_offset=0,
        trade_times_and_prices=((_hour() + timedelta(milliseconds=200), "101"),),
    )
    _write_trade_archive(
        following_path,
        source_hour_offset=1,
        trade_times_and_prices=((_hour() + timedelta(milliseconds=300), "102"),),
    )

    for spec, path in (
        (_spec(-1), previous_path),
        (_spec(0), current_path),
        (_spec(1), following_path),
    ):
        _register_source(database_session, spec, path)

    @contextmanager
    def scope():
        yield database_session

    observed_lock_identities: list[tuple[object, ...]] = []
    production_lock = price_coordinator_module.acquire_source_hour_transaction_lock

    def recording_lock(
        session: Session,
        spec: SourceFileSpec,
    ):
        observed_lock_identities.append(spec.identity_tuple)
        return production_lock(
            session,
            spec,
        )

    monkeypatch.setattr(
        price_coordinator_module,
        "acquire_source_hour_transaction_lock",
        recording_lock,
    )

    coordinator = SingleMarketPriceProcessingCoordinator(
        session_scope_factory=scope,
        batch_size=1,
    )

    result = coordinator.run(
        PriceProcessingRequest(
            operation_id=uuid4(),
            target=_spec(),
            include_adjacent_sources=True,
        )
    )

    assert result.source_archive_count == 3
    assert result.valid_count == 1
    assert result.total_trade_count == 3

    expected_lock_identities = [
        _spec(-1).identity_tuple,
        _spec(0).identity_tuple,
        _spec(1).identity_tuple,
    ]

    assert observed_lock_identities == sorted(
        expected_lock_identities,
    )
    assert len(observed_lock_identities) == len(set(observed_lock_identities))

    stored = PriceAnalyticalRepository(database_session).get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )

    assert stored is not None

    source_hours = stored.provenance_json["source_hours"]

    assert [item["hour_utc"] for item in source_hours] == [
        _hour(-1).isoformat().replace("+00:00", "Z"),
        _hour(0).isoformat().replace("+00:00", "Z"),
        _hour(1).isoformat().replace("+00:00", "Z"),
    ]


def test_price_processing_cancellation_resets_to_downloaded(
    database_session: Session,
    tmp_path: Path,
) -> None:
    source = tmp_path / "BTCUSDT_trades.parquet"

    _write_trade_archive(
        source,
        source_hour_offset=0,
        trade_times_and_prices=((_hour() + timedelta(milliseconds=100), "100"),),
    )
    _register_source(
        database_session,
        _spec(),
        source,
    )

    @contextmanager
    def scope():
        yield database_session

    coordinator = SingleMarketPriceProcessingCoordinator(
        session_scope_factory=scope,
        batch_size=1,
        cancellation_check_interval_rows=1,
        cancellation_check_interval_records=1,
    )

    checks = 0

    def cancel() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 3

    with pytest.raises(
        ProcessingCancelledError,
        match="cancel",
    ):
        coordinator.run(
            PriceProcessingRequest(
                operation_id=uuid4(),
                target=_spec(),
            ),
            cancellation_probe=cancel,
        )

    source_row = AcquisitionRepository(database_session).get_source_hour(_spec())

    assert source_row is not None
    assert source_row.status == SourceHourStatus.DOWNLOADED.value

    assert (
        PriceAnalyticalRepository(database_session).get_price_hour(
            base="BTC",
            hour_utc=_hour(),
        )
        is None
    )


def test_local_coordinator_and_headless_price_outputs_are_identical(
    database_session: Session,
    tmp_path: Path,
) -> None:
    source = tmp_path / "BTCUSDT_equivalence_trades.parquet"

    _write_trade_archive(
        source,
        source_hour_offset=0,
        trade_times_and_prices=(
            (
                _hour() + timedelta(milliseconds=100),
                "100",
            ),
            (
                _hour() + timedelta(milliseconds=500),
                "105",
            ),
            (
                _hour() + timedelta(milliseconds=900),
                "102",
            ),
            (
                _hour()
                + timedelta(
                    seconds=2,
                    milliseconds=100,
                ),
                "99",
            ),
        ),
    )

    digest, size = sha256_file(source)

    archive = ProcessingSourceArchive(
        spec=_spec(),
        local_path=source,
        content_sha256=digest,
        file_size_bytes=size,
        status=SourceHourStatus.DOWNLOADED,
    )

    headless = process_price_archives_headlessly(
        _spec(),
        (archive,),
        batch_size=1,
    )

    _register_source(
        database_session,
        _spec(),
        source,
    )

    @contextmanager
    def scope():
        yield database_session

    coordinator = SingleMarketPriceProcessingCoordinator(
        session_scope_factory=scope,
        batch_size=1,
        cancellation_check_interval_rows=1,
        cancellation_check_interval_records=1,
    )

    local_result = coordinator.run(
        PriceProcessingRequest(
            operation_id=uuid4(),
            target=_spec(),
        )
    )

    stored = PriceAnalyticalRepository(
        database_session,
    ).get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )

    assert stored is not None
    assert stored.encoded == headless.artifact.encoded
    assert (
        local_result.price_content_sha256 == headless.artifact.manifest.content_sha256
    )
    assert headless.artifact.manifest.source_hours[0].content_sha256 == digest

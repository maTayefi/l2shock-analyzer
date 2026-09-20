from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import TradeRecord
from l2shock.price import (
    PRICE_OBSERVATIONS_PER_HOUR,
    OneSecondTradeOHLCAccumulator,
    TradeOHLCCancelledError,
    TradeOHLCError,
    TradeSampleInvalidReason,
    TradeSampleQuality,
    build_trade_ohlc_hour,
    stream_trade_ohlc_hour,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        2,
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


def _record(
    *,
    trade_id: str,
    milliseconds: int,
    price: str,
    symbol: str = "BTCUSDT",
    quantity: str = "1",
    row_number: int = 0,
) -> TradeRecord:
    trade_time_ms = _epoch_ms(_hour()) + milliseconds

    return TradeRecord(
        symbol=symbol,
        trade_id=trade_id,
        price=Decimal(price),
        quantity=Decimal(quantity),
        received_time_ns=trade_time_ms * 1_000_000,
        event_time_ms=trade_time_ms,
        trade_time_ms=trade_time_ms,
        is_buyer_maker=False,
        order_type="market",
        row_number=row_number,
    )


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_hour(offset),
    )


def _write_trade_archive(
    path: Path,
    rows: list[dict[str, object]],
) -> None:
    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
        row_group_size=1,
    )


def _row(
    *,
    trade_id: str,
    trade_time_ms: int,
    price: str,
) -> dict[str, object]:
    return {
        "received_time": trade_time_ms * 1_000_000,
        "event_time": trade_time_ms,
        "symbol": "BTCUSDT",
        "trade_id": trade_id,
        "price": price,
        "quantity": "1",
        "trade_time": trade_time_ms,
        "is_buyer_maker": False,
        "order_type": "market",
    }


def test_trade_hour_contains_exactly_3600_slots() -> None:
    block = build_trade_ohlc_hour(
        (
            _record(
                trade_id="1",
                milliseconds=100,
                price="100.25",
            ),
        ),
        base="BTC",
        hour_utc=_hour(),
    )

    assert len(block.observations) == PRICE_OBSERVATIONS_PER_HOUR
    assert block.quality_summary.observation_count == 3_600
    assert block.valid_count == 1
    assert block.invalid_count == 3_599

    first = block.observations[0]
    second = block.observations[1]

    assert first.quality is TradeSampleQuality.VALID
    assert first.open == Decimal("100.25")
    assert first.high == Decimal("100.25")
    assert first.low == Decimal("100.25")
    assert first.close == Decimal("100.25")
    assert first.trade_count == 1

    assert second.quality is TradeSampleQuality.INVALID
    assert second.invalid_reason is TradeSampleInvalidReason.NO_TRADES
    assert second.open is None
    assert second.close is None
    assert second.trade_count == 0


def test_ohlc_uses_trade_time_not_input_order() -> None:
    block = build_trade_ohlc_hour(
        (
            _record(
                trade_id="middle",
                milliseconds=500,
                price="105",
                row_number=0,
            ),
            _record(
                trade_id="first",
                milliseconds=100,
                price="100",
                row_number=1,
            ),
            _record(
                trade_id="last",
                milliseconds=900,
                price="103",
                row_number=2,
            ),
            _record(
                trade_id="high",
                milliseconds=700,
                price="110",
                row_number=3,
            ),
            _record(
                trade_id="low",
                milliseconds=300,
                price="95",
                row_number=4,
            ),
        ),
        base="BTC",
        hour_utc=_hour(),
    )

    first = block.observations[0]

    assert first.open == Decimal("100")
    assert first.high == Decimal("110")
    assert first.low == Decimal("95")
    assert first.close == Decimal("103")
    assert first.trade_count == 5
    assert first.first_trade_time_ms == _epoch_ms(_hour()) + 100
    assert first.last_trade_time_ms == _epoch_ms(_hour()) + 900


def test_equal_trade_time_uses_stable_input_order_for_open_close() -> None:
    block = build_trade_ohlc_hour(
        (
            _record(
                trade_id="a",
                milliseconds=100,
                price="100",
            ),
            _record(
                trade_id="b",
                milliseconds=100,
                price="101",
            ),
            _record(
                trade_id="c",
                milliseconds=100,
                price="99",
            ),
        ),
        base="BTC",
        hour_utc=_hour(),
    )

    first = block.observations[0]

    assert first.open == Decimal("100")
    assert first.close == Decimal("99")
    assert first.high == Decimal("101")
    assert first.low == Decimal("99")


def test_half_open_second_and_hour_boundaries() -> None:
    start_ms = _epoch_ms(_hour())

    records = (
        _record(
            trade_id="second-zero",
            milliseconds=0,
            price="100",
        ),
        _record(
            trade_id="second-one",
            milliseconds=1_000,
            price="101",
        ),
        TradeRecord(
            symbol="BTCUSDT",
            trade_id="next-hour",
            price=Decimal("102"),
            quantity=Decimal("1"),
            received_time_ns=(start_ms + 3_600_000) * 1_000_000,
            event_time_ms=start_ms + 3_600_000,
            trade_time_ms=start_ms + 3_600_000,
            is_buyer_maker=False,
            order_type="market",
            row_number=2,
        ),
    )

    block = build_trade_ohlc_hour(
        records,
        base="BTC",
        hour_utc=_hour(),
    )

    assert block.observations[0].open == Decimal("100")
    assert block.observations[1].open == Decimal("101")

    assert block.input_trade_count == 3
    assert block.accepted_trade_count == 2
    assert block.outside_target_hour_trade_count == 1


def test_outside_hour_trade_ids_do_not_participate_in_target_deduplication() -> None:
    start_ms = _epoch_ms(_hour())
    next_hour_ms = start_ms + 3_600_000

    records = (
        TradeRecord(
            symbol="BTCUSDT",
            trade_id="outside-duplicate",
            price=Decimal("100"),
            quantity=Decimal("1"),
            received_time_ns=next_hour_ms * 1_000_000,
            event_time_ms=next_hour_ms,
            trade_time_ms=next_hour_ms,
            is_buyer_maker=False,
            order_type="market",
            row_number=0,
        ),
        TradeRecord(
            symbol="BTCUSDT",
            trade_id="outside-duplicate",
            price=Decimal("101"),
            quantity=Decimal("2"),
            received_time_ns=(next_hour_ms + 1) * 1_000_000,
            event_time_ms=next_hour_ms + 1,
            trade_time_ms=next_hour_ms + 1,
            is_buyer_maker=True,
            order_type="market",
            row_number=1,
        ),
    )

    block = build_trade_ohlc_hour(
        records,
        base="BTC",
        hour_utc=_hour(),
    )

    assert block.input_trade_count == 2
    assert block.accepted_trade_count == 0
    assert block.exact_duplicate_trade_count == 0
    assert block.outside_target_hour_trade_count == 2
    assert block.valid_count == 0
    assert block.invalid_count == 3_600


def test_exact_duplicate_trade_is_counted_once() -> None:
    record = _record(
        trade_id="duplicate",
        milliseconds=100,
        price="100",
    )

    block = build_trade_ohlc_hour(
        (record, record),
        base="BTC",
        hour_utc=_hour(),
    )

    assert block.input_trade_count == 2
    assert block.accepted_trade_count == 1
    assert block.exact_duplicate_trade_count == 1
    assert block.observations[0].trade_count == 1


def test_conflicting_duplicate_trade_id_is_rejected() -> None:
    with pytest.raises(
        TradeOHLCError,
        match="conflicting normalized content",
    ):
        build_trade_ohlc_hour(
            (
                _record(
                    trade_id="same-id",
                    milliseconds=100,
                    price="100",
                ),
                _record(
                    trade_id="same-id",
                    milliseconds=100,
                    price="101",
                ),
            ),
            base="BTC",
            hour_utc=_hour(),
        )


def test_symbol_mismatch_is_rejected() -> None:
    with pytest.raises(
        TradeOHLCError,
        match="does not match target symbol",
    ):
        build_trade_ohlc_hour(
            (
                _record(
                    trade_id="1",
                    milliseconds=100,
                    price="100",
                    symbol="ETHUSDT",
                ),
            ),
            base="BTC",
            hour_utc=_hour(),
        )


def test_cancellation_is_checked_during_record_ingestion() -> None:
    checks = 0

    def cancel() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 3

    records = tuple(
        _record(
            trade_id=str(index),
            milliseconds=index,
            price="100",
            row_number=index,
        )
        for index in range(100)
    )

    with pytest.raises(
        TradeOHLCCancelledError,
        match="cancelled",
    ):
        build_trade_ohlc_hour(
            records,
            base="BTC",
            hour_utc=_hour(),
            cancellation_probe=cancel,
            cancellation_check_interval_records=2,
        )

    assert checks >= 3


def test_streamed_trade_archive_builds_real_ohlc(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"
    start_ms = _epoch_ms(_hour())

    _write_trade_archive(
        path,
        [
            _row(
                trade_id="1",
                trade_time_ms=start_ms + 100,
                price="100",
            ),
            _row(
                trade_id="2",
                trade_time_ms=start_ms + 200,
                price="105",
            ),
            _row(
                trade_id="3",
                trade_time_ms=start_ms + 900,
                price="102",
            ),
        ],
    )

    result = stream_trade_ohlc_hour(
        ((path, _spec()),),
        target_hour_utc=_hour(),
        batch_size=1,
    )

    assert len(result.reader_reports) == 1
    assert result.reader_reports[0].rows_read == 3

    first = result.block.observations[0]

    assert first.open == Decimal("100")
    assert first.high == Decimal("105")
    assert first.low == Decimal("100")
    assert first.close == Decimal("102")
    assert first.trade_count == 3

    assert result.block.accepted_trade_count == 3
    assert result.block.quality_summary.total_trade_count == 3


def test_trade_id_deduplication_uses_temporary_sqlite_storage() -> None:
    accumulator = OneSecondTradeOHLCAccumulator(
        base="BTC",
        hour_utc=_hour(),
    )

    try:
        for index in range(5_000):
            accumulator.consume(
                _record(
                    trade_id=f"disk-backed-{index}",
                    milliseconds=index,
                    price=str(100 + (index % 10)),
                    row_number=index,
                )
            )

        # Regression boundary: unique target-hour IDs must not be retained in
        # an unbounded Python dictionary.
        assert not hasattr(
            accumulator,
            "_trade_id_fingerprints",
        )

        store = accumulator._trade_id_store
        connection = store._connection

        assert connection is not None
        assert store.cache_size_kib == 2_048

        cache_size = connection.execute("PRAGMA cache_size").fetchone()

        database_list = connection.execute("PRAGMA database_list").fetchall()

        assert cache_size == (-2_048,)
        assert database_list
        assert database_list[0][2] == ""

        block = accumulator.finish()
    finally:
        accumulator.close()

    assert block.input_trade_count == 5_000
    assert block.accepted_trade_count == 5_000
    assert block.exact_duplicate_trade_count == 0
    assert block.quality_summary.total_trade_count == 5_000


def test_trade_id_sqlite_storage_preserves_exact_duplicate_semantics() -> None:
    record = _record(
        trade_id="sqlite-exact-duplicate",
        milliseconds=100,
        price="100.2500",
    )

    block = build_trade_ohlc_hour(
        (
            record,
            record,
        ),
        base="BTC",
        hour_utc=_hour(),
    )

    assert block.input_trade_count == 2
    assert block.accepted_trade_count == 1
    assert block.exact_duplicate_trade_count == 1
    assert block.observations[0].trade_count == 1


def test_trade_id_sqlite_storage_preserves_conflict_detection() -> None:
    with pytest.raises(
        TradeOHLCError,
        match="conflicting normalized content",
    ):
        build_trade_ohlc_hour(
            (
                _record(
                    trade_id="sqlite-conflict",
                    milliseconds=100,
                    price="100",
                ),
                _record(
                    trade_id="sqlite-conflict",
                    milliseconds=100,
                    price="101",
                ),
            ),
            base="BTC",
            hour_utc=_hour(),
        )

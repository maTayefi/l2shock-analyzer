from __future__ import annotations

import inspect
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import l2shock.ingest.parquet_reader as parquet_reader_module
from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    BookSide,
    OrderBookEvent,
    OrderBookEventType,
    StreamedParquetReadError,
    TradeRecord,
    read_orderbook_file,
    read_source_file,
    read_trade_file,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _orderbook_spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )


def _trade_spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_hour(),
    )


def _orderbook_row(
    *,
    received_time: int,
    event_time: int,
    transaction_time: int | None,
    first_update_id: int | None,
    final_update_id: int | None,
    prev_final_update_id: int | None,
    side: str,
    price: str,
    quantity: str,
    event_type: str = "update",
    symbol: str = "BTCUSDT",
    last_update_id: int | None = None,
    order_count: int | None = None,
) -> dict[str, object]:
    return {
        "received_time": received_time,
        "event_time": event_time,
        "transaction_time": transaction_time,
        "symbol": symbol,
        "event_type": event_type,
        "first_update_id": first_update_id,
        "final_update_id": final_update_id,
        "prev_final_update_id": prev_final_update_id,
        "last_update_id": last_update_id,
        "side": side,
        "price": price,
        "quantity": quantity,
        "order_count": order_count,
    }


def _trade_row(
    *,
    received_time: int,
    event_time: int,
    trade_time: int,
    trade_id: int | str,
    price: str,
    quantity: str,
    symbol: str = "BTCUSDT",
    is_buyer_maker: bool = False,
    order_type: str | None = "LIMIT",
) -> dict[str, object]:
    return {
        "received_time": received_time,
        "event_time": event_time,
        "symbol": symbol,
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "trade_time": trade_time,
        "is_buyer_maker": is_buyer_maker,
        "order_type": order_type,
    }


def _write_rows(
    path: Path,
    rows: list[dict[str, object]],
    *,
    row_group_size: int,
) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=row_group_size,
    )


def test_orderbook_event_spans_batches_and_row_groups(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"

    delegate = parquet_reader_module.orderbook_sequence_contract("binance_futures")
    resolver_call_count = 0
    validation_call_count = 0

    class CountingSequenceContract:
        transaction_time_required = delegate.transaction_time_required

        def validate_event(self, **values: object) -> None:
            nonlocal validation_call_count
            validation_call_count += 1
            delegate.validate_event(**values)

        def __getattr__(self, name: str):
            return getattr(delegate, name)

    counting_contract = CountingSequenceContract()

    def counted_contract_resolver(venue: str):
        nonlocal resolver_call_count
        resolver_call_count += 1
        assert venue == "binance_futures"
        return counting_contract

    monkeypatch.setattr(
        parquet_reader_module,
        "orderbook_sequence_contract",
        counted_contract_resolver,
    )

    rows = [
        _orderbook_row(
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_399_900,
            transaction_time=1_788_350_399_899,
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            side="ask",
            price="60001.25",
            quantity="1.125",
        ),
        _orderbook_row(
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_399_900,
            transaction_time=1_788_350_399_899,
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            side="bid",
            price="59999.75",
            quantity="2.250",
        ),
        _orderbook_row(
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_399_900,
            transaction_time=1_788_350_399_899,
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            side="bid",
            price="59998.50",
            quantity="0.000",
        ),
        _orderbook_row(
            received_time=1_788_350_400_101_000_000,
            event_time=1_788_350_400_000,
            transaction_time=1_788_350_399_999,
            first_update_id=104,
            final_update_id=106,
            prev_final_update_id=103,
            side="ask",
            price="60002.00",
            quantity="0.500",
        ),
    ]

    # The first event has three rows. With a row-group and batch size of two,
    # it must be retained correctly across both boundaries.
    _write_rows(path, rows, row_group_size=2)

    events: list[OrderBookEvent] = []

    report = read_orderbook_file(
        path,
        _orderbook_spec(),
        consumer=events.append,
        batch_size=2,
    )

    assert report.rows_read == 4
    assert report.events_read == 2
    assert report.update_event_count == 2
    assert report.snapshot_event_count == 0
    assert report.continuity_checks == 1
    assert report.continuity_mismatch_count == 0
    assert report.has_snapshot is False
    assert report.has_ordering_regression is False

    assert len(events) == 2

    first = events[0]
    assert first.event_type is OrderBookEventType.UPDATE
    assert first.first_row_number == 0
    assert first.last_row_number == 2
    assert first.row_count == 3
    assert first.first_update_id == 101
    assert first.final_update_id == 103
    assert first.prev_final_update_id == 100

    assert first.changes[0].side is BookSide.ASK
    assert first.changes[0].price == Decimal("60001.25")
    assert first.changes[0].quantity == Decimal("1.125")

    # Quantity zero remains an exact level-removal mutation. The reader does
    # not apply it because reconstruction belongs to a later batch.
    assert first.changes[2].quantity == Decimal("0.000")

    # Four physical rows form two logical exchange events. The venue adapter
    # is resolved once for the file and validates each logical event once.
    assert resolver_call_count == 1
    assert validation_call_count == 2


def test_orderbook_continuity_mismatch_is_reported_not_hidden(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"

    rows = [
        _orderbook_row(
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_399_900,
            transaction_time=1_788_350_399_899,
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            side="bid",
            price="59999.00",
            quantity="1.0",
        ),
        _orderbook_row(
            received_time=1_788_350_400_101_000_000,
            event_time=1_788_350_400_000,
            transaction_time=1_788_350_399_999,
            first_update_id=110,
            final_update_id=112,
            prev_final_update_id=109,
            side="ask",
            price="60001.00",
            quantity="1.5",
        ),
    ]

    _write_rows(path, rows, row_group_size=1)

    report = read_orderbook_file(
        path,
        _orderbook_spec(),
        batch_size=1,
    )

    assert report.continuity_checks == 1
    assert report.continuity_mismatch_count == 1
    assert report.has_continuity_mismatch is True
    assert report.retained_issues[0].kind == ("update_continuity_mismatch")
    assert report.retained_issues[0].previous_value == 103
    assert report.retained_issues[0].current_value == 109


def test_snapshot_resets_within_file_update_continuity_chain(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"

    rows = [
        _orderbook_row(
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_399_900,
            transaction_time=1_788_350_399_899,
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            side="bid",
            price="59999.00",
            quantity="1.0",
        ),
        _orderbook_row(
            received_time=1_788_350_400_101_000_000,
            event_time=1_788_350_400_000,
            transaction_time=1_788_350_399_999,
            first_update_id=None,
            final_update_id=None,
            prev_final_update_id=None,
            last_update_id=500,
            event_type="snapshot",
            side="bid",
            price="59998.00",
            quantity="2.0",
        ),
        _orderbook_row(
            received_time=1_788_350_400_201_000_000,
            event_time=1_788_350_400_100,
            transaction_time=1_788_350_400_099,
            first_update_id=501,
            final_update_id=503,
            prev_final_update_id=500,
            side="ask",
            price="60002.00",
            quantity="3.0",
        ),
    ]

    _write_rows(path, rows, row_group_size=1)

    report = read_orderbook_file(
        path,
        _orderbook_spec(),
        batch_size=1,
    )

    assert report.events_read == 3
    assert report.snapshot_event_count == 1
    assert report.update_event_count == 2

    # The update after a snapshot begins a new in-file update chain. Part 4B
    # will prove whether the snapshot/checkpoint actually initializes replay.
    assert report.continuity_checks == 0
    assert report.continuity_mismatch_count == 0


def test_orderbook_rejects_duplicate_side_price_inside_event(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"

    rows = [
        _orderbook_row(
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_399_900,
            transaction_time=1_788_350_399_899,
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            side="bid",
            price="59999.00",
            quantity="1.0",
        ),
        _orderbook_row(
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_399_900,
            transaction_time=1_788_350_399_899,
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            side="bid",
            price="59999.00",
            quantity="2.0",
        ),
    ]

    _write_rows(path, rows, row_group_size=1)

    with pytest.raises(
        StreamedParquetReadError,
        match="more than one mutation",
    ):
        read_orderbook_file(
            path,
            _orderbook_spec(),
            batch_size=1,
        )


@pytest.mark.parametrize(
    ("field_name", "bad_value", "message"),
    [
        ("symbol", "ETHUSDT", "does not match expected"),
        ("price", "NaN", "must be finite"),
        ("price", "0", "must be positive"),
        ("quantity", "-1", "must be non-negative"),
        ("side", "middle", "Unsupported order-book side"),
        ("event_type", "partial", "Unsupported order-book event_type"),
    ],
)
def test_orderbook_rejects_invalid_scalar_values(
    tmp_path: Path,
    field_name: str,
    bad_value: object,
    message: str,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"

    row = _orderbook_row(
        received_time=1_788_350_400_001_000_000,
        event_time=1_788_350_399_900,
        transaction_time=1_788_350_399_899,
        first_update_id=101,
        final_update_id=103,
        prev_final_update_id=100,
        side="bid",
        price="59999.00",
        quantity="1.0",
    )
    row[field_name] = bad_value

    _write_rows(path, [row], row_group_size=1)

    with pytest.raises(
        StreamedParquetReadError,
        match=message,
    ):
        read_orderbook_file(
            path,
            _orderbook_spec(),
            batch_size=1,
        )


def test_event_ordering_regressions_are_reported(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"

    rows = [
        _orderbook_row(
            received_time=1_788_350_400_200_000_000,
            event_time=1_788_350_400_100,
            transaction_time=1_788_350_400_099,
            first_update_id=200,
            final_update_id=205,
            prev_final_update_id=199,
            side="bid",
            price="59999.00",
            quantity="1.0",
        ),
        _orderbook_row(
            received_time=1_788_350_400_100_000_000,
            event_time=1_788_350_400_000,
            transaction_time=1_788_350_399_999,
            first_update_id=191,
            final_update_id=200,
            prev_final_update_id=190,
            side="ask",
            price="60001.00",
            quantity="1.0",
        ),
    ]

    _write_rows(path, rows, row_group_size=1)

    report = read_orderbook_file(
        path,
        _orderbook_spec(),
        batch_size=1,
    )

    assert report.received_time_regression_count == 1
    assert report.event_time_regression_count == 1
    assert report.update_id_regression_count == 1
    assert report.has_ordering_regression is True
    assert {issue.kind for issue in report.retained_issues} == {
        "received_time_regression",
        "event_time_regression",
        "update_continuity_mismatch",
        "final_update_id_regression",
    }


def test_trade_reader_streams_rows_and_reports_ordering(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"

    rows = [
        _trade_row(
            received_time=1_788_350_400_200_000_000,
            event_time=1_788_350_400_100,
            trade_time=1_788_350_400_099,
            trade_id="1001",
            price="60000.25",
            quantity="0.010",
            is_buyer_maker=True,
        ),
        _trade_row(
            received_time=1_788_350_400_100_000_000,
            event_time=1_788_350_400_000,
            trade_time=1_788_350_400_050,
            trade_id="1001",
            price="60001.50",
            quantity="0.020",
            is_buyer_maker=False,
        ),
        _trade_row(
            received_time=1_788_350_400_300_000_000,
            event_time=1_788_350_400_200,
            trade_time=1_788_350_400_199,
            trade_id="1002",
            price="60002.75",
            quantity="0.030",
            order_type=None,
        ),
    ]

    _write_rows(path, rows, row_group_size=1)

    records: list[TradeRecord] = []

    report = read_trade_file(
        path,
        _trade_spec(),
        consumer=records.append,
        batch_size=1,
    )

    assert report.rows_read == 3
    assert report.received_time_regression_count == 1
    assert report.trade_time_regression_count == 1
    assert report.adjacent_duplicate_trade_id_count == 1
    assert report.has_ordering_regression is True

    assert records[0].trade_id == "1001"
    assert records[0].price == Decimal("60000.25")
    assert records[0].quantity == Decimal("0.010")
    assert records[0].is_buyer_maker is True

    assert records[2].trade_id == "1002"
    assert records[2].order_type is None


def test_dispatcher_uses_source_data_kind(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"

    rows = [
        _trade_row(
            received_time=1_788_350_400_200_000_000,
            event_time=1_788_350_400_100,
            trade_time=1_788_350_400_099,
            trade_id=1001,
            price="60000.25",
            quantity="0.010",
        )
    ]

    _write_rows(path, rows, row_group_size=1)

    records: list[TradeRecord] = []

    report = read_source_file(
        path,
        _trade_spec(),
        trade_consumer=records.append,
        batch_size=1,
    )

    assert report.rows_read == 1
    assert len(records) == 1


def test_reader_does_not_require_whole_hour_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"

    rows: list[dict[str, object]] = []

    for index in range(250):
        rows.append(
            _orderbook_row(
                received_time=(1_788_350_400_000_000_000 + index * 1_000_000),
                event_time=1_788_350_399_900 + index,
                transaction_time=1_788_350_399_899 + index,
                first_update_id=1_000 + index,
                final_update_id=1_000 + index,
                prev_final_update_id=999 + index,
                side="bid" if index % 2 == 0 else "ask",
                price=str(60_000 + index),
                quantity="1.0",
            )
        )

    _write_rows(path, rows, row_group_size=7)

    # A whole-table read would defeat the production contract. The streamed
    # implementation only uses ParquetFile.iter_batches.
    def fail_read_table(*args, **kwargs):
        del args, kwargs
        raise AssertionError("pq.read_table must not be used")

    monkeypatch.setattr(pq, "read_table", fail_read_table)

    event_count = 0

    def consume(_event: OrderBookEvent) -> None:
        nonlocal event_count
        event_count += 1

    report = read_orderbook_file(
        path,
        _orderbook_spec(),
        consumer=consume,
        batch_size=3,
    )

    assert report.rows_read == 250
    assert report.events_read == 250
    assert event_count == 250
    assert report.continuity_mismatch_count == 0


def test_projected_row_iterator_avoids_per_row_dictionary_materialization() -> None:
    source = inspect.getsource(
        parquet_reader_module._iter_projected_rows,
    )

    assert ".to_pylist(" not in source
    assert ".to_pydict(" in source
    assert "zip(" in source


def test_binance_snapshot_null_transaction_time_uses_event_time(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_snapshot_null_transaction.parquet"
    received_time = 1_788_350_400_100_000_000
    event_time = 1_788_350_400_100

    rows = [
        _orderbook_row(
            received_time=received_time,
            event_time=event_time,
            transaction_time=None,
            first_update_id=None,
            final_update_id=None,
            prev_final_update_id=None,
            last_update_id=500,
            event_type="snapshot",
            side="bid",
            price="59999",
            quantity="2",
            order_count=None,
        ),
        _orderbook_row(
            received_time=received_time,
            event_time=event_time,
            transaction_time=None,
            first_update_id=None,
            final_update_id=None,
            prev_final_update_id=None,
            last_update_id=500,
            event_type="snapshot",
            side="ask",
            price="60001",
            quantity="3",
            order_count=None,
        ),
    ]

    _write_rows(
        path,
        rows,
        row_group_size=1,
    )

    events: list[OrderBookEvent] = []

    report = read_orderbook_file(
        path,
        _orderbook_spec(),
        consumer=events.append,
        batch_size=1,
    )

    assert report.rows_read == 2
    assert report.events_read == 1
    assert report.snapshot_event_count == 1

    event = events[0]

    assert event.event_type is OrderBookEventType.SNAPSHOT
    assert event.event_time_ms == event_time
    assert event.transaction_time_ms == event_time
    assert event.last_update_id == 500

    # Null order_count remains unknown; it is not fabricated as zero.
    assert all(change.order_count is None for change in event.changes)


def test_binance_update_still_rejects_null_transaction_time(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_update_null_transaction.parquet"

    rows = [
        _orderbook_row(
            received_time=1_788_350_400_100_000_000,
            event_time=1_788_350_400_100,
            transaction_time=None,
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
            last_update_id=None,
            event_type="update",
            side="bid",
            price="59999",
            quantity="2",
            order_count=None,
        ),
    ]

    _write_rows(
        path,
        rows,
        row_group_size=1,
    )

    with pytest.raises(
        StreamedParquetReadError,
        match="transaction_time cannot be null",
    ):
        read_orderbook_file(
            path,
            _orderbook_spec(),
            batch_size=1,
        )


def test_authoritative_integer_parser_rejects_integral_float(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        StreamedParquetReadError,
        match="not floating point",
    ):
        parquet_reader_module._integer(
            float(2**53),
            field_name="received_time",
            path=tmp_path / "float-backed.parquet",
            row_number=0,
        )


def test_nullable_integer_parser_retains_nan_null_compatibility(
    tmp_path: Path,
) -> None:
    observed = parquet_reader_module._integer(
        float("nan"),
        field_name="last_update_id",
        path=tmp_path / "nullable.parquet",
        row_number=0,
        nullable=True,
    )

    assert observed is None


def test_okx_snapshot_rejects_mixed_raw_reset_sentinel_representation(
    tmp_path: Path,
) -> None:
    from datetime import datetime, timezone

    import pyarrow as pa
    import pyarrow.parquet as pq
    import pytest

    from l2shock.acquisition import SourceDataKind, SourceFileSpec
    from l2shock.ingest.parquet_reader import (
        StreamedParquetReadError,
        read_orderbook_file,
    )

    hour = datetime(
        2026,
        9,
        14,
        12,
        tzinfo=timezone.utc,
    )
    received_time_ns = 1_789_372_800_100_000_000
    event_time_ms = received_time_ns // 1_000_000
    path = tmp_path / "mixed-okx-snapshot.parquet"

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "received_time": received_time_ns,
                    "event_time": event_time_ms,
                    "transaction_time": None,
                    "symbol": "BTC-USDT-SWAP",
                    "event_type": "snapshot",
                    "first_update_id": None,
                    "final_update_id": 500,
                    "prev_final_update_id": None,
                    "last_update_id": -1,
                    "side": "bid",
                    "price": "100",
                    "quantity": "2",
                    "order_count": None,
                },
                {
                    "received_time": received_time_ns,
                    "event_time": event_time_ms,
                    "transaction_time": None,
                    "symbol": "BTC-USDT-SWAP",
                    "event_type": "snapshot",
                    "first_update_id": None,
                    "final_update_id": 500,
                    "prev_final_update_id": None,
                    "last_update_id": 500,
                    "side": "ask",
                    "price": "101",
                    "quantity": "3",
                    "order_count": None,
                },
            ]
        ),
        path,
        compression="zstd",
        row_group_size=1,
    )

    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue="okx_futures",
        symbol="BTC-USDT-SWAP",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour,
    )

    with pytest.raises(
        StreamedParquetReadError,
        match="mixes raw last_update_id=-1",
    ):
        read_orderbook_file(
            path,
            spec,
            batch_size=1,
        )

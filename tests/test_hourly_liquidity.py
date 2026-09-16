from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    OBSERVATIONS_PER_HOUR,
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.liquidity import (
    DepthBand,
    DepthLiquidityCancelledError,
    HourlyLiquidityError,
    sample_liquidity_archives,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _epoch_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _received_ns(
    *,
    seconds: int = 0,
    milliseconds: int = 0,
    hour_offset: int = 0,
) -> int:
    return _epoch_ns(
        _hour(hour_offset)
        + timedelta(
            seconds=seconds,
            milliseconds=milliseconds,
        )
    )


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(offset),
    )


def _row(
    *,
    received_time: int,
    event_type: str,
    side: str,
    price: str,
    quantity: str,
    first_update_id: int | None = None,
    final_update_id: int | None = None,
    prev_final_update_id: int | None = None,
    last_update_id: int | None = None,
) -> dict[str, object]:
    event_time_ms = received_time // 1_000_000

    return {
        "received_time": received_time,
        "event_time": event_time_ms,
        "transaction_time": event_time_ms,
        "symbol": "BTCUSDT",
        "event_type": event_type,
        "first_update_id": first_update_id,
        "final_update_id": final_update_id,
        "prev_final_update_id": prev_final_update_id,
        "last_update_id": last_update_id,
        "side": side,
        "price": price,
        "quantity": quantity,
        "order_count": None,
    }


def _snapshot_rows(
    *,
    received_time: int,
    last_update_id: int,
    bid_price: str = "100",
    bid_quantity: str = "2",
    ask_price: str = "101",
    ask_quantity: str = "3",
) -> list[dict[str, object]]:
    return [
        _row(
            received_time=received_time,
            event_type="snapshot",
            side="bid",
            price=bid_price,
            quantity=bid_quantity,
            last_update_id=last_update_id,
        ),
        _row(
            received_time=received_time,
            event_type="snapshot",
            side="ask",
            price=ask_price,
            quantity=ask_quantity,
            last_update_id=last_update_id,
        ),
    ]


def _update_row(
    *,
    received_time: int,
    update_id: int,
    previous_update_id: int,
    side: str,
    price: str,
    quantity: str,
) -> dict[str, object]:
    return _row(
        received_time=received_time,
        event_type="update",
        side=side,
        price=price,
        quantity=quantity,
        first_update_id=update_id,
        final_update_id=update_id,
        prev_final_update_id=previous_update_id,
    )


def _write(
    path: Path,
    rows: list[dict[str, object]],
) -> None:
    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
        row_group_size=1,
    )


def _top_band() -> DepthBand:
    return DepthBand(
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )


def test_hour_has_exactly_3600_liquidity_observations(
    tmp_path: Path,
) -> None:
    path = tmp_path / "book.parquet"

    _write(
        path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=100,
        ),
    )

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    assert len(result.hours) == 1
    assert result.total_observation_count == OBSERVATIONS_PER_HOUR

    hour = result.hours[0]

    assert len(hour.observations) == OBSERVATIONS_PER_HOUR
    assert hour.valid_count == OBSERVATIONS_PER_HOUR
    assert hour.invalid_count == 0
    assert hour.fully_valid is True

    assert hour.quality_summary.valid_count == 3600
    assert hour.quality_summary.invalid_count == 0
    assert hour.quality_summary.invalid_reason_counts == ()


def test_valid_samples_contain_exact_liquidity_metrics(
    tmp_path: Path,
) -> None:
    path = tmp_path / "book.parquet"

    _write(
        path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=100,
            bid_price="100",
            bid_quantity="2",
            ask_price="101",
            ask_quantity="3",
        ),
    )

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    observation = result.hours[0].observations[0]

    assert observation.quality is BookSampleQuality.VALID
    assert observation.invalid_reason is None

    assert observation.bid_liquidity == Decimal("200")
    assert observation.ask_liquidity == Decimal("303")
    assert observation.total_liquidity == Decimal("503")
    assert observation.bid_ask_imbalance is not None
    assert observation.source_count == 1

    assert observation.bid_depth_level_count == 1
    assert observation.ask_depth_level_count == 1
    assert observation.levels_examined == 2


def test_pre_snapshot_buckets_have_no_fabricated_liquidity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "late_snapshot.parquet"

    _write(
        path,
        _snapshot_rows(
            received_time=_received_ns(
                seconds=1,
                milliseconds=500,
            ),
            last_update_id=100,
        ),
    )

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    first = result.hours[0].observations[0]
    second = result.hours[0].observations[1]

    assert first.quality is BookSampleQuality.INVALID
    assert first.invalid_reason is BookSampleInvalidReason.UNINITIALIZED
    assert first.bid_liquidity is None
    assert first.ask_liquidity is None
    assert first.total_liquidity is None
    assert first.bid_ask_imbalance is None
    assert first.levels_examined == 0
    assert first.source_count == 0

    assert second.quality is BookSampleQuality.VALID
    assert second.bid_liquidity == Decimal("200")
    assert second.ask_liquidity == Decimal("303")


def test_event_exactly_at_boundary_changes_that_bucket_liquidity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "boundary.parquet"

    rows = _snapshot_rows(
        received_time=_received_ns(milliseconds=100),
        last_update_id=100,
        bid_quantity="1",
        ask_quantity="1",
    )
    rows.append(
        _update_row(
            received_time=_received_ns(seconds=1),
            update_id=101,
            previous_update_id=100,
            side="bid",
            price="100",
            quantity="5",
        )
    )

    _write(path, rows)

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    first = result.hours[0].observations[0]

    assert first.bucket_end_utc == _hour() + timedelta(seconds=1)
    assert first.last_update_id == 101
    assert first.bid_liquidity == Decimal("500")
    assert first.ask_liquidity == Decimal("101")
    assert first.source_received_time_ns == _received_ns(seconds=1)


def test_same_timestamp_events_are_all_applied_before_bucket(
    tmp_path: Path,
) -> None:
    path = tmp_path / "same_timestamp.parquet"
    boundary = _received_ns(seconds=1)

    rows = _snapshot_rows(
        received_time=_received_ns(milliseconds=100),
        last_update_id=100,
        bid_quantity="1",
        ask_quantity="1",
    )
    rows.extend(
        [
            _update_row(
                received_time=boundary,
                update_id=101,
                previous_update_id=100,
                side="bid",
                price="100",
                quantity="5",
            ),
            _update_row(
                received_time=boundary,
                update_id=102,
                previous_update_id=101,
                side="ask",
                price="101",
                quantity="7",
            ),
        ]
    )

    _write(path, rows)

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    first = result.hours[0].observations[0]

    assert first.last_update_id == 102
    assert first.bid_liquidity == Decimal("500")
    assert first.ask_liquidity == Decimal("707")


def test_quiet_seconds_carry_current_liquidity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "quiet.parquet"

    _write(
        path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=100,
        ),
    )

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    observations = result.hours[0].observations

    for index in (0, 1, 60, 600, 3599):
        observation = observations[index]

        assert observation.quality is BookSampleQuality.VALID
        assert observation.bid_liquidity == Decimal("200")
        assert observation.ask_liquidity == Decimal("303")
        assert observation.source_count == 1


def test_continuity_failure_removes_future_liquidity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalidated.parquet"

    rows = _snapshot_rows(
        received_time=_received_ns(milliseconds=100),
        last_update_id=100,
    )
    rows.append(
        _update_row(
            received_time=_received_ns(seconds=2),
            update_id=112,
            previous_update_id=109,
            side="bid",
            price="100",
            quantity="9",
        )
    )

    _write(path, rows)

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    observations = result.hours[0].observations

    assert observations[0].quality is BookSampleQuality.VALID

    invalidated = observations[1]

    assert invalidated.quality is BookSampleQuality.INVALID
    assert invalidated.invalid_reason is (BookSampleInvalidReason.REPLAY_INVALIDATED)
    assert invalidated.bid_liquidity is None
    assert invalidated.ask_liquidity is None
    assert invalidated.total_liquidity is None
    assert invalidated.source_count == 0

    assert observations[-1].quality is BookSampleQuality.INVALID
    assert observations[-1].bid_liquidity is None

    summary = result.hours[0].quality_summary

    assert summary.valid_count == 1
    assert summary.invalid_count == 3599
    assert (
        summary.invalid_reason_map[BookSampleInvalidReason.REPLAY_INVALIDATED] == 3599
    )


@pytest.mark.parametrize(
    ("bid", "ask", "reason"),
    [
        ("100", "100", BookSampleInvalidReason.LOCKED),
        ("101", "100", BookSampleInvalidReason.CROSSED),
    ],
)
def test_locked_and_crossed_books_do_not_calculate_liquidity(
    tmp_path: Path,
    bid: str,
    ask: str,
    reason: BookSampleInvalidReason,
) -> None:
    path = tmp_path / f"{reason.value}.parquet"

    _write(
        path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=100,
            bid_price=bid,
            ask_price=ask,
        ),
    )

    result = sample_liquidity_archives(
        ((path, _spec()),),
        _top_band(),
        batch_size=1,
    )

    first = result.hours[0].observations[0]

    assert first.quality is BookSampleQuality.INVALID
    assert first.invalid_reason is reason
    assert first.bid_liquidity is None
    assert first.ask_liquidity is None
    assert first.total_liquidity is None
    assert first.levels_examined == 0


def test_zero_total_band_keeps_book_valid_but_imbalance_null(
    tmp_path: Path,
) -> None:
    path = tmp_path / "empty_band.parquet"

    _write(
        path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=100,
        ),
    )

    result = sample_liquidity_archives(
        ((path, _spec()),),
        DepthBand(
            lower_fraction=Decimal("0.10"),
            upper_fraction=Decimal("0.20"),
        ),
        batch_size=1,
    )

    first = result.hours[0].observations[0]

    assert first.quality is BookSampleQuality.VALID
    assert first.bid_liquidity == Decimal("0")
    assert first.ask_liquidity == Decimal("0")
    assert first.total_liquidity == Decimal("0")
    assert first.bid_ask_imbalance is None
    assert first.source_count == 1

    assert result.hours[0].quality_summary.zero_total_liquidity_count == 3600


def test_depth_cancellation_propagates_from_hourly_sampling(
    tmp_path: Path,
) -> None:
    path = tmp_path / "large.parquet"

    rows = _snapshot_rows(
        received_time=_received_ns(milliseconds=100),
        last_update_id=100,
        bid_price="100",
        ask_price="101",
    )

    # Add many levels to the same snapshot event.
    for index in range(1, 500):
        rows.append(
            _row(
                received_time=_received_ns(milliseconds=100),
                event_type="snapshot",
                side="bid",
                price=str(Decimal("100") - Decimal(index) / Decimal("1000")),
                quantity="1",
                last_update_id=100,
            )
        )

    _write(path, rows)

    checks = 0

    def cancel() -> bool:
        nonlocal checks
        checks += 1
        # The parquet reader and replay layer together consume exactly
        # 4 probe calls before any depth calculation begins (initial
        # replay check, initial reader check, row-0 check, end-of-read
        # check).  The threshold must exceed those so the probe fires
        # inside calculate_depth_liquidity, not inside read_orderbook_file.
        return checks >= 10

    with pytest.raises(DepthLiquidityCancelledError):
        sample_liquidity_archives(
            ((path, _spec()),),
            DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.50"),
            ),
            batch_size=32,
            cancellation_probe=cancel,
            cancellation_check_interval_levels=50,
        )

    assert checks >= 10


def test_received_time_regression_is_rejected_by_liquidity_sampler(
    tmp_path: Path,
) -> None:
    path = tmp_path / "received_time_regression.parquet"

    rows = _snapshot_rows(
        received_time=_received_ns(
            seconds=2,
            milliseconds=100,
        ),
        last_update_id=100,
    )
    rows.append(
        _update_row(
            received_time=_received_ns(
                seconds=1,
                milliseconds=900,
            ),
            update_id=101,
            previous_update_id=100,
            side="bid",
            price="100",
            quantity="5",
        )
    )

    _write(path, rows)

    from l2shock.ingest import StreamedParquetReadError

    with pytest.raises(StreamedParquetReadError) as exc_info:
        sample_liquidity_archives(
            ((path, _spec()),),
            _top_band(),
            batch_size=1,
        )

    cause = exc_info.value.__cause__
    assert isinstance(cause, HourlyLiquidityError)
    assert "received_time_ns regressed" in str(cause)


def test_liquidity_observation_total_validation_ignores_ambient_context() -> None:
    from decimal import Context, localcontext

    bid = Decimal("12345678901234567890.123456789")
    ask = Decimal("0.000000000987654321")
    total = Decimal("12345678901234567890.123456789987654321")

    with localcontext(Context(prec=6)):
        observation = __import__(
            "l2shock.liquidity",
            fromlist=["LiquidityObservation"],
        ).LiquidityObservation(
            bucket_index=0,
            bucket_start_utc=_hour(),
            bucket_end_utc=_hour() + timedelta(seconds=1),
            source_received_time_ns=_received_ns(milliseconds=100),
            quality=BookSampleQuality.VALID,
            invalid_reason=None,
            replay_valid=True,
            initialization_state="snapshot",
            book_structure="normal",
            last_update_id=100,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            bid_liquidity=bid,
            ask_liquidity=ask,
            total_liquidity=total,
            bid_ask_imbalance=Decimal("0.9999999999999999999999999998"),
            bid_depth_level_count=1,
            ask_depth_level_count=1,
            levels_examined=2,
            source_count=1,
        )

    assert observation.total_liquidity == total

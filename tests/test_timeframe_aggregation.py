from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext

import pytest

from l2shock.analysis import (
    AggregatedL2Bar,
    AnalysisBarQuality,
    L2Second,
    PriceSecond,
    TimeframeError,
    aggregate_l2_seconds,
    aggregate_price_seconds,
    get_timeframe,
    select_chart_timeframe,
    snap_closed_analysis_range,
)
from l2shock.ingest import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.price import (
    TradeSampleInvalidReason,
    TradeSampleQuality,
)


def _utc(
    hour: int,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> datetime:
    return datetime(
        2026,
        9,
        2,
        hour,
        minute,
        second,
        microsecond,
        tzinfo=timezone.utc,
    )


def _valid_l2(
    timestamp: datetime,
    *,
    bid: str,
    ask: str,
) -> L2Second:
    return L2Second(
        timestamp_utc=timestamp,
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(bid),
        ask_liquidity=Decimal(ask),
        source_count=1,
    )


def _invalid_l2(timestamp: datetime) -> L2Second:
    return L2Second(
        timestamp_utc=timestamp,
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.REPLAY_INVALIDATED,
        bid_liquidity=None,
        ask_liquidity=None,
        source_count=0,
    )


def _valid_price(
    timestamp: datetime,
    *,
    open_value: str,
    high: str,
    low: str,
    close: str,
    trades: int,
) -> PriceSecond:
    return PriceSecond(
        timestamp_utc=timestamp,
        quality=TradeSampleQuality.VALID,
        invalid_reason=None,
        open=Decimal(open_value),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        trade_count=trades,
    )


def _invalid_price(timestamp: datetime) -> PriceSecond:
    return PriceSecond(
        timestamp_utc=timestamp,
        quality=TradeSampleQuality.INVALID,
        invalid_reason=TradeSampleInvalidReason.NO_TRADES,
        open=None,
        high=None,
        low=None,
        close=None,
        trade_count=0,
    )


def test_closed_range_snapping_includes_both_partial_edge_bars() -> None:
    snapped = snap_closed_analysis_range(
        _utc(13, 3, 13, 500_000),
        _utc(14, 6, 1, 600_000),
        "5m",
    )

    assert snapped.start_utc == _utc(13, 0)
    assert snapped.end_utc == _utc(14, 10)
    assert snapped.bar_count == 14


def test_exact_end_boundary_still_includes_bar_beginning_at_end() -> None:
    snapped = snap_closed_analysis_range(
        _utc(12, 0),
        _utc(13, 0),
        "1h",
    )

    assert snapped.start_utc == _utc(12, 0)
    assert snapped.end_utc == _utc(14, 0)
    assert snapped.bar_count == 2


def test_automatic_chart_timeframe_selects_finest_fitting_candidate() -> None:
    selected = select_chart_timeframe(
        _utc(0),
        _utc(8),
        maximum_bars=400,
    )

    # Closed endpoint ownership gives 481 one-minute bars, so 1m does not fit.
    # Three-minute bars do fit.
    assert selected.label == "3m"


def test_calendar_month_timeframe_is_not_faked_as_fixed_seconds() -> None:
    with pytest.raises(TimeframeError, match="Unsupported"):
        get_timeframe("1M")


def test_l2_aggregation_uses_final_endpoint_state() -> None:
    start = _utc(12)
    observations = tuple(
        _valid_l2(
            start + timedelta(seconds=index),
            bid=str(100 + index),
            ask=str(200 + index),
        )
        for index in range(5)
    )

    bars = aggregate_l2_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(seconds=5),
        timeframe="5s",
    )

    assert len(bars) == 1

    bar = bars[0]

    assert bar.quality is AnalysisBarQuality.VALID
    assert bar.bid_liquidity == Decimal("104")
    assert bar.ask_liquidity == Decimal("204")
    assert bar.total_liquidity == Decimal("308")
    assert bar.valid_seconds == 5
    assert bar.invalid_seconds == 0
    assert bar.hard_discontinuity is False


def test_l2_valid_endpoint_with_earlier_invalid_second_is_degraded_boundary() -> None:
    start = _utc(12)
    observations = (
        _valid_l2(start, bid="100", ask="200"),
        _invalid_l2(start + timedelta(seconds=1)),
        _valid_l2(
            start + timedelta(seconds=2),
            bid="102",
            ask="202",
        ),
        _valid_l2(
            start + timedelta(seconds=3),
            bid="103",
            ask="203",
        ),
        _valid_l2(
            start + timedelta(seconds=4),
            bid="104",
            ask="204",
        ),
    )

    bar = aggregate_l2_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(seconds=5),
        timeframe="5s",
    )[0]

    assert bar.quality is AnalysisBarQuality.DEGRADED
    assert bar.bid_liquidity == Decimal("104")
    assert bar.ask_liquidity == Decimal("204")
    assert bar.invalid_seconds == 1
    assert bar.maximum_invalid_run_seconds == 1
    assert bar.hard_discontinuity is True


def test_l2_invalid_endpoint_produces_no_aggregate_liquidity() -> None:
    start = _utc(12)
    observations = (
        _valid_l2(start, bid="100", ask="200"),
        _valid_l2(
            start + timedelta(seconds=1),
            bid="101",
            ask="201",
        ),
        _valid_l2(
            start + timedelta(seconds=2),
            bid="102",
            ask="202",
        ),
        _valid_l2(
            start + timedelta(seconds=3),
            bid="103",
            ask="203",
        ),
        _invalid_l2(start + timedelta(seconds=4)),
    )

    bar = aggregate_l2_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(seconds=5),
        timeframe="5s",
    )[0]

    assert bar.quality is AnalysisBarQuality.INVALID
    assert bar.bid_liquidity is None
    assert bar.ask_liquidity is None
    assert bar.total_liquidity is None
    assert bar.hard_discontinuity is True
    assert bar.endpoint_invalid_reason is (BookSampleInvalidReason.REPLAY_INVALIDATED)


def test_missing_l2_second_is_an_explicit_hard_discontinuity() -> None:
    start = _utc(12)
    observations = tuple(
        _valid_l2(
            start + timedelta(seconds=index),
            bid=str(100 + index),
            ask=str(200 + index),
        )
        for index in (0, 1, 3, 4)
    )

    bar = aggregate_l2_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(seconds=5),
        timeframe="5s",
    )[0]

    assert bar.quality is AnalysisBarQuality.DEGRADED
    assert bar.missing_seconds == 1
    assert bar.invalid_seconds == 1
    assert bar.hard_discontinuity is True


def test_price_aggregation_uses_real_trade_ohlc_geometry() -> None:
    start = _utc(12)

    observations = (
        _valid_price(
            start,
            open_value="100",
            high="102",
            low="99",
            close="101",
            trades=3,
        ),
        _valid_price(
            start + timedelta(seconds=1),
            open_value="101",
            high="105",
            low="100",
            close="104",
            trades=4,
        ),
        _invalid_price(start + timedelta(seconds=2)),
        _valid_price(
            start + timedelta(seconds=3),
            open_value="103",
            high="106",
            low="98",
            close="99",
            trades=5,
        ),
        _valid_price(
            start + timedelta(seconds=4),
            open_value="99",
            high="101",
            low="97",
            close="100",
            trades=2,
        ),
    )

    bar = aggregate_price_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(seconds=5),
        timeframe="5s",
    )[0]

    assert bar.quality is AnalysisBarQuality.DEGRADED
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("106")
    assert bar.low == Decimal("97")
    assert bar.close == Decimal("100")
    assert bar.trade_count == 14
    assert bar.invalid_seconds == 1
    assert bar.hard_discontinuity is True


def test_price_bucket_with_no_real_trades_is_invalid() -> None:
    start = _utc(12)

    observations = tuple(
        _invalid_price(start + timedelta(seconds=index)) for index in range(5)
    )

    bar = aggregate_price_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(seconds=5),
        timeframe="5s",
    )[0]

    assert bar.quality is AnalysisBarQuality.INVALID
    assert bar.open is None
    assert bar.high is None
    assert bar.low is None
    assert bar.close is None
    assert bar.trade_count == 0
    assert bar.hard_discontinuity is True


def test_duplicate_one_second_timestamp_is_rejected() -> None:
    start = _utc(12)
    observation = _valid_l2(start, bid="100", ask="200")

    with pytest.raises(ValueError, match="Duplicate"):
        aggregate_l2_seconds(
            (observation, observation),
            start_utc=start,
            end_utc=start + timedelta(seconds=1),
            timeframe="1s",
        )


def test_numerically_usable_degraded_market_coverage_is_not_hard_gap() -> None:
    start = _utc(12)

    observations = tuple(
        L2Second(
            timestamp_utc=start + timedelta(seconds=index),
            quality=BookSampleQuality.VALID,
            invalid_reason=None,
            bid_liquidity=Decimal(str(100 + index)),
            ask_liquidity=Decimal(str(200 + index)),
            source_count=1,
            coverage_degraded=(index == 2),
        )
        for index in range(5)
    )

    bar = aggregate_l2_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(seconds=5),
        timeframe="5s",
    )[0]

    assert bar.quality is AnalysisBarQuality.DEGRADED
    assert bar.bid_liquidity == Decimal("104")
    assert bar.ask_liquidity == Decimal("204")
    assert bar.invalid_seconds == 0
    assert bar.hard_discontinuity is False


def test_sparse_one_day_l2_coverage_preserves_invalid_run_counts() -> None:
    start = _utc(0)

    observations = (
        _valid_l2(
            start,
            bid="100",
            ask="200",
        ),
        _valid_l2(
            start + timedelta(seconds=86_399),
            bid="110",
            ask="210",
        ),
    )

    bar = aggregate_l2_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(days=1),
        timeframe="1d",
    )[0]

    assert bar.expected_seconds == 86_400
    assert bar.observed_seconds == 2
    assert bar.valid_seconds == 2
    assert bar.invalid_seconds == 86_398
    assert bar.missing_seconds == 86_398
    assert bar.maximum_invalid_run_seconds == 86_398
    assert bar.quality is AnalysisBarQuality.DEGRADED
    assert bar.bid_liquidity == Decimal("110")
    assert bar.ask_liquidity == Decimal("210")
    assert bar.hard_discontinuity is True


def test_sparse_one_day_price_coverage_preserves_exact_ohlc() -> None:
    start = _utc(0)

    observations = (
        _valid_price(
            start + timedelta(seconds=10),
            open_value="100",
            high="105",
            low="99",
            close="101",
            trades=2,
        ),
        _valid_price(
            start + timedelta(seconds=86_399),
            open_value="102",
            high="110",
            low="98",
            close="109",
            trades=3,
        ),
    )

    bar = aggregate_price_seconds(
        observations,
        start_utc=start,
        end_utc=start + timedelta(days=1),
        timeframe="1d",
    )[0]

    assert bar.expected_seconds == 86_400
    assert bar.observed_seconds == 2
    assert bar.valid_seconds == 2
    assert bar.invalid_seconds == 86_398
    assert bar.missing_seconds == 86_398
    assert bar.maximum_invalid_run_seconds == 86_388

    assert bar.quality is AnalysisBarQuality.DEGRADED
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("110")
    assert bar.low == Decimal("98")
    assert bar.close == Decimal("109")
    assert bar.trade_count == 5
    assert bar.hard_discontinuity is True

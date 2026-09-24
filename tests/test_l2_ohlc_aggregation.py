from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext

from l2shock.analysis.aggregation import (
    AggregatedL2Bar,
    AnalysisBarQuality,
    L2OHLC,
    L2Second,
    aggregate_l2_seconds,
)
from l2shock.ingest import BookSampleInvalidReason, BookSampleQuality

_START = datetime(2026, 9, 9, 12, 0, 0, tzinfo=timezone.utc)


def _valid(
    second: int,
    *,
    bid: str,
    ask: str,
    coverage_degraded: bool = False,
) -> L2Second:
    return L2Second(
        timestamp_utc=_START + timedelta(seconds=second),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(bid),
        ask_liquidity=Decimal(ask),
        source_count=1,
        coverage_degraded=coverage_degraded,
    )


def _invalid(second: int) -> L2Second:
    return L2Second(
        timestamp_utc=_START + timedelta(seconds=second),
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.REPLAY_INVALIDATED,
        bid_liquidity=None,
        ask_liquidity=None,
        source_count=0,
    )


def _aggregate(
    observations: tuple[L2Second, ...],
    *,
    seconds: int = 5,
) -> AggregatedL2Bar:
    bars = aggregate_l2_seconds(
        observations,
        start_utc=_START,
        end_utc=_START + timedelta(seconds=seconds),
        timeframe=f"{seconds}s",
    )
    assert len(bars) == 1
    return bars[0]


def test_four_l2_candles_use_timestamp_order_and_per_second_metrics() -> None:
    # Deliberately pass observations out of order. Bid and Ask highs occur
    # on different seconds, so Total High cannot be "Bid High + Ask High".
    bar = _aggregate(
        (
            _valid(4, bid="4", ask="8"),
            _valid(1, bid="9", ask="1"),
            _valid(3, bid="2", ask="10"),
            _valid(0, bid="3", ask="6"),
            _valid(2, bid="5", ask="4"),
        )
    )

    assert bar.quality is AnalysisBarQuality.VALID
    assert bar.hard_discontinuity is False
    assert bar.bid_ohlc == L2OHLC(
        open=Decimal("3"),
        high=Decimal("9"),
        low=Decimal("2"),
        close=Decimal("4"),
    )
    assert bar.ask_ohlc == L2OHLC(
        open=Decimal("6"),
        high=Decimal("10"),
        low=Decimal("1"),
        close=Decimal("8"),
    )
    assert bar.total_ohlc == L2OHLC(
        open=Decimal("9"),
        high=Decimal("12"),
        low=Decimal("9"),
        close=Decimal("12"),
    )
    assert bar.delta_ohlc == L2OHLC(
        open=Decimal("-3"),
        high=Decimal("8"),
        low=Decimal("-8"),
        close=Decimal("-4"),
    )

    assert bar.bid_ohlc.close == bar.bid_liquidity
    assert bar.ask_ohlc.close == bar.ask_liquidity
    assert bar.total_ohlc.close == bar.total_liquidity
    assert bar.delta_ohlc.close == bar.bid_ask_delta()


def test_missing_second_is_not_filled_and_does_not_change_quality_policy() -> None:
    # The second at index 2 is absent. The endpoint at index 4 is valid.
    bar = _aggregate(
        (
            _valid(0, bid="1", ask="9"),
            _valid(1, bid="7", ask="2"),
            _valid(3, bid="3", ask="5"),
            _valid(4, bid="4", ask="6"),
        )
    )

    assert bar.quality is AnalysisBarQuality.DEGRADED
    assert bar.hard_discontinuity is True
    assert bar.valid_seconds == 4
    assert bar.missing_seconds == 1
    assert bar.bid_ohlc == L2OHLC(
        open=Decimal("1"),
        high=Decimal("7"),
        low=Decimal("1"),
        close=Decimal("4"),
    )
    assert bar.delta_ohlc == L2OHLC(
        open=Decimal("-8"),
        high=Decimal("5"),
        low=Decimal("-8"),
        close=Decimal("-2"),
    )


def test_invalid_second_is_excluded_even_when_it_is_the_endpoint() -> None:
    bar = _aggregate(
        (
            _valid(0, bid="0", ask="2"),
            _invalid(1),
            _valid(2, bid="3", ask="1"),
            _invalid(4),
        )
    )

    assert bar.quality is AnalysisBarQuality.INVALID
    assert bar.hard_discontinuity is True
    assert bar.bid_liquidity is None
    assert bar.ask_liquidity is None
    assert bar.bid_ohlc == L2OHLC(
        open=Decimal("0"),
        high=Decimal("3"),
        low=Decimal("0"),
        close=Decimal("3"),
    )
    assert bar.delta_ohlc == L2OHLC(
        open=Decimal("-2"),
        high=Decimal("2"),
        low=Decimal("-2"),
        close=Decimal("2"),
    )

    # Numerical OHLC on an invalid bucket must never itself make that
    # bucket eligible for the chart or LM analysis.


def test_bucket_without_valid_seconds_has_no_l2_candles() -> None:
    bar = _aggregate((_invalid(0), _invalid(4)))

    assert bar.quality is AnalysisBarQuality.INVALID
    assert bar.bid_ohlc is None
    assert bar.ask_ohlc is None
    assert bar.total_ohlc is None
    assert bar.delta_ohlc is None


def test_one_second_candles_match_existing_endpoint_metrics() -> None:
    bar = _aggregate(
        (_valid(0, bid="0", ask="2"),),
        seconds=1,
    )

    assert bar.quality is AnalysisBarQuality.VALID
    assert bar.bid_ohlc == L2OHLC(
        open=Decimal("0"),
        high=Decimal("0"),
        low=Decimal("0"),
        close=Decimal("0"),
    )
    assert bar.ask_ohlc.close == bar.ask_liquidity
    assert bar.total_ohlc.close == bar.total_liquidity
    assert bar.delta_ohlc.close == bar.bid_ask_delta()


def test_delta_candle_close_uses_existing_34_digit_policy() -> None:
    bid = "1.123456789012345678901234567890123456789"
    ask = "0.000000000000000000000000000000000000001"

    bar = _aggregate(
        (_valid(0, bid=bid, ask=ask),),
        seconds=1,
    )

    with localcontext(Context(prec=34, rounding=ROUND_HALF_EVEN)):
        expected_delta = Decimal(bid) - Decimal(ask)

    assert bar.delta_ohlc is not None
    assert bar.delta_ohlc.open == expected_delta
    assert bar.delta_ohlc.high == expected_delta
    assert bar.delta_ohlc.low == expected_delta
    assert bar.delta_ohlc.close == expected_delta
    assert bar.delta_ohlc.close == bar.bid_ask_delta()

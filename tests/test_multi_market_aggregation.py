# tests/test_multi_market_aggregation.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext

from l2shock.analysis import (
    AggregatedL2Bar,
    AnalysisBarQuality,
    get_timeframe,
)
from l2shock.analysis.multi_market import (
    AggregateMarketContribution,
    AggregateMarketKey,
    AggregatedL2Second,
    AggregateL2QualityState,
)


def _start() -> datetime:
    return datetime(
        2026,
        9,
        9,
        12,
        tzinfo=timezone.utc,
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


def _market(
    venue: str,
    instrument: str,
) -> AggregateMarketKey:
    return AggregateMarketKey(
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
    )


def test_aggregated_l2_total_is_independent_of_ambient_decimal_context() -> None:
    bar = AggregatedL2Bar(
        bucket_index=0,
        start_utc=_utc(12),
        end_utc=_utc(12) + timedelta(seconds=1),
        timeframe=get_timeframe("1s"),
        quality=AnalysisBarQuality.VALID,
        bid_liquidity=Decimal("12345678901234567890.123456789"),
        ask_liquidity=Decimal("0.000000000987654321"),
        source_count=1,
        expected_seconds=1,
        observed_seconds=1,
        valid_seconds=1,
        invalid_seconds=0,
        missing_seconds=0,
        maximum_invalid_run_seconds=0,
        hard_discontinuity=False,
        endpoint_invalid_reason=None,
    )

    with localcontext(Context(prec=8)):
        observed = bar.total_liquidity

    assert observed == Decimal("12345678901234567890.123456789987654321")


def test_aggregated_l2_imbalance_is_independent_of_ambient_decimal_context() -> None:
    market = _market("binance_futures", "BTCUSDT")
    bid = Decimal("12345678901234567890.123456789")
    ask = Decimal("0.000000000987654321")

    contribution = AggregateMarketContribution(
        market=market,
        component_preset_hash="a" * 64,
        l2_content_sha256="b" * 64,
        bid_liquidity=bid,
        ask_liquidity=ask,
        source_count=1,
    )

    second = AggregatedL2Second(
        timestamp_utc=_start(),
        quality_state=AggregateL2QualityState.VALID,
        expected_market_count=1,
        contributing_market_count=1,
        bid_liquidity=bid,
        ask_liquidity=ask,
        source_count=1,
        contributions=(contribution,),
        invalid_reason=None,
    )

    with localcontext(Context(prec=8)):
        low = second.bid_ask_imbalance(decimal_precision=34)

    with localcontext(Context(prec=50)):
        high = second.bid_ask_imbalance(decimal_precision=34)

    assert low == high

# l2shock/analysis/__init__.py
"""Verified one-second L2 analysis primitives shared by Shock-Start.

Batch 36 removed Liquidity Movement detection, LM ranking, price filtering,
LM execution, and the price-dependent aligned LM loader. Shock-Start modules
(``shock_dataset``, ``shock_start``, ``shock_evidence``, ``shock_review``,
``robust_stats``) are imported directly from their own modules.

This package initializer must never import a Shock-Start or UI module; it
only re-exports small, dependency-light primitives.
"""

from l2shock.analysis.aggregation import (
    AggregatedL2Bar,
    AggregatedPriceBar,
    AnalysisBarQuality,
    L2Second,
    PriceSecond,
    aggregate_l2_seconds,
    aggregate_price_seconds,
)
from l2shock.analysis.l2_seconds import decode_l2_hour_to_seconds
from l2shock.analysis.multi_market import (
    AGGREGATE_L2_ALGORITHM_VERSION,
    AGGREGATE_L2_SCHEMA,
    AGGREGATE_L2_SCHEMA_VERSION,
    AggregateL2Error,
    AggregateL2InvalidReason,
    AggregateL2QualityState,
    AggregateL2Series,
    AggregateMarketContribution,
    AggregateMarketKey,
    AggregateMarketSeries,
    AggregatedL2Second,
    aggregate_market_l2_seconds,
)
from l2shock.analysis.timeframes import (
    TIMEFRAMES,
    SnappedAnalysisRange,
    Timeframe,
    TimeframeError,
    floor_to_timeframe,
    get_timeframe,
    iter_timeframe_buckets,
    select_chart_timeframe,
    snap_closed_analysis_range,
)

__all__ = [
    "AGGREGATE_L2_ALGORITHM_VERSION",
    "AGGREGATE_L2_SCHEMA",
    "AGGREGATE_L2_SCHEMA_VERSION",
    "TIMEFRAMES",
    "AggregateL2Error",
    "AggregateL2InvalidReason",
    "AggregateL2QualityState",
    "AggregateL2Series",
    "AggregateMarketContribution",
    "AggregateMarketKey",
    "AggregateMarketSeries",
    "AggregatedL2Bar",
    "AggregatedL2Second",
    "AggregatedPriceBar",
    "AnalysisBarQuality",
    "L2Second",
    "PriceSecond",
    "SnappedAnalysisRange",
    "Timeframe",
    "TimeframeError",
    "aggregate_l2_seconds",
    "aggregate_market_l2_seconds",
    "aggregate_price_seconds",
    "decode_l2_hour_to_seconds",
    "floor_to_timeframe",
    "get_timeframe",
    "iter_timeframe_buckets",
    "select_chart_timeframe",
    "snap_closed_analysis_range",
]
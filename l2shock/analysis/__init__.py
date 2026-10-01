# l2shock/analysis/__init__.py
"""Dependency-light primitives for verified historical L2 analysis.

This package re-exports retained aggregation, exact-second decoding,
multi-market composition, and fixed-duration timeframe primitives.

Detector-free streaming and metric computation live in
``l2_view_stream`` and ``l2_view_metrics`` and are imported explicitly by
their callers.

This initializer must not import UI modules or retired detector modules.
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

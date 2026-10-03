# l2shock/analysis/l2_view_metrics.py
"""Selectable derived L2 metrics for Analysis Panels A and B.

Panels A/B are presentation choices, unrelated to the retired detector's
A/B/C points. Every metric is derived from verified Bid and Ask (already
aggregated across markets at the same UTC second). Nothing is persisted.

State metrics are candles built from each valid second. Change metrics
compare adjacent viewing-bar closes; the first bar and any bar following
an invalid bar is null. Undefined values (zero denominators) are null.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from fractions import Fraction
from types import MappingProxyType
from typing import Final, TypeAlias

from l2shock.analysis.l2_view_stream import L2ViewBar

FloatOhlc: TypeAlias = tuple[float, float, float, float]
TwoLineValue: TypeAlias = tuple[float | None, float | None]
MetricValue: TypeAlias = FloatOhlc | float | TwoLineValue | None


class L2ViewMetricError(ValueError):
    """Unknown metric or non-representable value."""


class L2ViewMetric(StrEnum):
    DELTA = "delta"
    TOTAL = "total"
    IMBALANCE_PCT = "imbalance_pct"
    BID_SHARE_PCT = "bid_share_pct"
    ASK_SHARE_PCT = "ask_share_pct"
    BID_ASK_SHARES_PCT = "bid_ask_shares_pct"
    TOTAL_CHANGE = "total_change"
    TOTAL_CHANGE_PCT = "total_change_pct"
    DELTA_CHANGE = "delta_change"
    IMBALANCE_CHANGE_PP = "imbalance_change_pp"
    RELATIVE_SIDE_CHANGE_PCT = "relative_side_change_pct"


class L2ViewMetricKind(StrEnum):
    CANDLE = "candle"
    LINE = "line"
    TWO_LINES = "two_lines"


@dataclass(frozen=True, slots=True)
class L2ViewMetricSpec:
    metric: L2ViewMetric
    label: str
    axis_name: str
    formula: str
    kind: L2ViewMetricKind
    colors: tuple[str, ...]
    series_names: tuple[str, ...]


_C = L2ViewMetricKind.CANDLE
_L = L2ViewMetricKind.LINE

_SPECS: Final[tuple[L2ViewMetricSpec, ...]] = (
    L2ViewMetricSpec(
        L2ViewMetric.DELTA,
        "Order-Book Delta",
        "Delta",
        "Bid - Ask",
        _C,
        ("#66bb6a",),
        ("Delta",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.TOTAL,
        "Total Liquidity",
        "Total",
        "Bid + Ask",
        _C,
        ("#ab47bc",),
        ("Total",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.IMBALANCE_PCT,
        "Signed Imbalance %",
        "Imb %",
        "100 (Bid - Ask) / (Bid + Ask)",
        _C,
        ("#42a5f5",),
        ("Signed Imbalance %",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.BID_SHARE_PCT,
        "Bid Share %",
        "Bid %",
        "100 Bid / (Bid + Ask)",
        _C,
        ("#42a5f5",),
        ("Bid Share %",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.ASK_SHARE_PCT,
        "Ask Share %",
        "Ask %",
        "100 Ask / (Bid + Ask)",
        _C,
        ("#ffb74d",),
        ("Ask Share %",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.BID_ASK_SHARES_PCT,
        "Bid/Ask Shares % (two lines)",
        "Shares %",
        "Bid Share % and Ask Share % at bar close",
        L2ViewMetricKind.TWO_LINES,
        ("#42a5f5", "#ffb74d"),
        ("Bid Share %", "Ask Share %"),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.TOTAL_CHANGE,
        "Total Change",
        "dTotal",
        "Total[n] - Total[n-1] (bar closes)",
        _L,
        ("#7e57c2",),
        ("Total Change",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.TOTAL_CHANGE_PCT,
        "Total Change %",
        "dTotal %",
        "100 (Total[n] - Total[n-1]) / Total[n-1]",
        _L,
        ("#7e57c2",),
        ("Total Change %",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.DELTA_CHANGE,
        "Delta Change",
        "dDelta",
        "Delta[n] - Delta[n-1] (bar closes)",
        _L,
        ("#9ccc65",),
        ("Delta Change",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.IMBALANCE_CHANGE_PP,
        "Imbalance Change (pp)",
        "dImb pp",
        "Imb%[n] - Imb%[n-1] (bar closes)",
        _L,
        ("#29b6f6",),
        ("Imbalance Change pp",),
    ),
    L2ViewMetricSpec(
        L2ViewMetric.RELATIVE_SIDE_CHANGE_PCT,
        "Relative Side Change %",
        "Rel chg %",
        "100 [(Bid[n]/Bid[n-1] - 1) - (Ask[n]/Ask[n-1] - 1)]",
        _L,
        ("#ffa726",),
        ("Relative Side Change %",),
    ),
)

L2_VIEW_METRIC_SPECS = MappingProxyType({spec.metric: spec for spec in _SPECS})
L2_VIEW_METRIC_LABELS: Final[dict[str, str]] = {
    spec.metric.value: spec.label for spec in _SPECS
}
DEFAULT_PANEL_A_METRIC: Final = L2ViewMetric.IMBALANCE_PCT
DEFAULT_PANEL_B_METRIC: Final = L2ViewMetric.DELTA


def l2_view_metric(value: object) -> L2ViewMetric:
    try:
        return L2ViewMetric(str(value))
    except ValueError as exc:
        raise L2ViewMetricError(f"Unknown L2 metric: {value!r}") from exc


def _f(value: object) -> float:
    number = float(value)  # type: ignore[arg-type]
    if not math.isfinite(number):
        raise L2ViewMetricError("L2 metric is not a finite chart coordinate")
    return number


def _ohlc(value: Sequence[object] | None) -> FloatOhlc | None:
    if value is None:
        return None
    return (_f(value[0]), _f(value[1]), _f(value[2]), _f(value[3]))


def _share_close(bar: L2ViewBar) -> Fraction | None:
    return None if bar.bid_share_pct is None else Fraction(bar.bid_share_pct[3])


def _close(bar: L2ViewBar, channel: str) -> Fraction | None:
    value = getattr(bar, channel)
    return None if value is None else Fraction(value[3])


def compute_l2_view_metric(
    bars: Sequence[L2ViewBar],
    metric: L2ViewMetric | str,
) -> tuple[MetricValue, ...]:
    """One value per viewing bar; None is a chart gap."""
    selected = l2_view_metric(metric)
    result: list[MetricValue] = []

    if selected is L2ViewMetric.DELTA:
        return tuple(_ohlc(bar.delta) for bar in bars)

    if selected is L2ViewMetric.TOTAL:
        return tuple(_ohlc(bar.total) for bar in bars)

    if selected is L2ViewMetric.BID_SHARE_PCT:
        return tuple(_ohlc(bar.bid_share_pct) for bar in bars)

    if selected is L2ViewMetric.IMBALANCE_PCT:
        # This increasing transformation preserves OHLC ordering.
        # Transform the exact ratios before converting coordinates.
        for bar in bars:
            share = bar.bid_share_pct
            result.append(
                None
                if share is None
                else _ohlc(tuple(2 * Fraction(value) - 100 for value in share))
            )
        return tuple(result)

    if selected is L2ViewMetric.ASK_SHARE_PCT:
        # This decreasing transformation swaps the high and low.
        for bar in bars:
            share = bar.bid_share_pct
            result.append(
                None
                if share is None
                else _ohlc(
                    (
                        100 - Fraction(share[0]),
                        100 - Fraction(share[2]),
                        100 - Fraction(share[1]),
                        100 - Fraction(share[3]),
                    )
                )
            )
        return tuple(result)

    if selected is L2ViewMetric.BID_ASK_SHARES_PCT:
        for bar in bars:
            close = _share_close(bar)
            result.append(None if close is None else (_f(close), _f(100 - close)))
        return tuple(result)

    previous: L2ViewBar | None = None

    for bar in bars:
        value: float | None = None

        if previous is not None and previous.valid_l2 and bar.valid_l2:
            if selected is L2ViewMetric.TOTAL_CHANGE:
                value = _f(_close(bar, "total") - _close(previous, "total"))  # type: ignore[operator]
            elif selected is L2ViewMetric.TOTAL_CHANGE_PCT:
                before = _close(previous, "total")
                if before:
                    value = _f(100 * (_close(bar, "total") - before) / before)  # type: ignore[operator]
            elif selected is L2ViewMetric.DELTA_CHANGE:
                value = _f(_close(bar, "delta") - _close(previous, "delta"))  # type: ignore[operator]
            elif selected is L2ViewMetric.IMBALANCE_CHANGE_PP:
                now, before_s = _share_close(bar), _share_close(previous)
                if now is not None and before_s is not None:
                    value = _f(2 * (now - before_s))
            elif selected is L2ViewMetric.RELATIVE_SIDE_CHANGE_PCT:
                b0, a0 = _close(previous, "bid"), _close(previous, "ask")
                if b0 and a0:
                    value = _f(
                        100 * ((_close(bar, "bid") / b0) - (_close(bar, "ask") / a0))  # type: ignore[operator]
                    )

        result.append(value)
        previous = bar

    return tuple(result)


__all__ = [
    "DEFAULT_PANEL_A_METRIC",
    "DEFAULT_PANEL_B_METRIC",
    "L2_VIEW_METRIC_LABELS",
    "L2_VIEW_METRIC_SPECS",
    "L2ViewMetric",
    "L2ViewMetricError",
    "L2ViewMetricKind",
    "L2ViewMetricSpec",
    "compute_l2_view_metric",
    "l2_view_metric",
]

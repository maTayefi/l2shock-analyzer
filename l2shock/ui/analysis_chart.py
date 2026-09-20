# l2shock/ui/analysis_chart.py
"""Pure ECharts option construction for the Analysis workspace.

The chart workspace uses one ECharts instance with five grids. This provides
native shared category-axis ownership, dataZoom synchronization, and linked
axis pointers without Python websocket round trips.

The chart displays only chart-timeframe core-eligible bars. Excluded or invalid
runs are represented as explicit vertical jump markers rather than silently
pretending the retained categories are contiguous in elapsed time.

All conversions from Decimal to float are presentation-only.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Final

from l2shock.analysis import (
    AnalysisDiscontinuityReason,
    LiquidityMetric,
    LiquidityMovementAnalysisResult,
    LiquidityMovementDirection,
    RankedLiquidityMovement,
    get_timeframe,
)
from l2shock.config import LMConfig
from l2shock.timeutils import utc_to_local

_PRICE_GRID_INDEX: Final[int] = 0
_BID_GRID_INDEX: Final[int] = 1
_ASK_GRID_INDEX: Final[int] = 2
_TOTAL_GRID_INDEX: Final[int] = 3
_DELTA_GRID_INDEX: Final[int] = 4

ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX: Final[str] = "l2shock-selected-focus-"

_GRID_BY_METRIC: Final[dict[LiquidityMetric, int]] = {
    LiquidityMetric.BID_LIQUIDITY: _BID_GRID_INDEX,
    LiquidityMetric.ASK_LIQUIDITY: _ASK_GRID_INDEX,
    LiquidityMetric.TOTAL_LIQUIDITY: _TOTAL_GRID_INDEX,
    LiquidityMetric.BID_ASK_DELTA: _DELTA_GRID_INDEX,
}


class AnalysisChartError(ValueError):
    """Chart input or presentation ownership is inconsistent."""


class ChartHighlightDestination(StrEnum):
    PRICE = "price"
    LIQUIDITY = "liquidity"


@dataclass(frozen=True, slots=True)
class AnalysisChartColors:
    background: str = "#0f172a"
    text: str = "#dbeafe"
    muted_text: str = "#94a3b8"
    grid_line: str = "#334155"

    price_up: str = "#22c55e"
    price_down: str = "#ef4444"

    bid_line: str = "#38bdf8"
    ask_line: str = "#f97316"
    total_line: str = "#a78bfa"

    delta_positive: str = "#22c55e"
    delta_negative: str = "#ef4444"
    delta_zero: str = "#94a3b8"

    upward_lm_rgb: tuple[int, int, int] = (37, 99, 235)
    downward_lm_rgb: tuple[int, int, int] = (220, 38, 38)

    discontinuity: str = "#f59e0b"
    invalid_warning: str = "#ef4444"
    discontinuity_fill: str = "rgba(245,158,11,0.16)"
    invalid_warning_fill: str = "rgba(239,68,68,0.24)"


ANALYSIS_CHART_COLORS: Final[AnalysisChartColors] = AnalysisChartColors()


@dataclass(frozen=True, slots=True)
class AnalysisChartVisibility:
    """Rendering-only visibility policy.

    These values must never influence detection, scoring, ranking, or cache
    identity.
    """

    show_price_highlights: bool = True
    show_liquidity_highlights: bool = True

    show_activity_candidates: bool = True
    show_chart_candidates: bool = True

    show_height_selected: bool = True
    show_sharpness_selected: bool = True

    highlighted_metrics: frozenset[LiquidityMetric] = frozenset(LiquidityMetric)

    def __post_init__(self) -> None:
        for field_name in (
            "show_price_highlights",
            "show_liquidity_highlights",
            "show_activity_candidates",
            "show_chart_candidates",
            "show_height_selected",
            "show_sharpness_selected",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise AnalysisChartError(f"{field_name} must be bool")

        metrics = frozenset(
            LiquidityMetric(metric) for metric in self.highlighted_metrics
        )
        object.__setattr__(
            self,
            "highlighted_metrics",
            metrics,
        )


@dataclass(frozen=True, slots=True)
class ChartNavigationWindow:
    """Category-index window suitable for table-to-chart navigation."""

    start_index: int
    end_index: int
    candidate_start_index: int
    candidate_end_index: int

    def __post_init__(self) -> None:
        for name in (
            "start_index",
            "end_index",
            "candidate_start_index",
            "candidate_end_index",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise AnalysisChartError(f"{name} must be a non-negative integer")

        if self.end_index < self.start_index:
            raise AnalysisChartError("Navigation end cannot precede start")

        if not (
            self.start_index
            <= self.candidate_start_index
            <= self.candidate_end_index
            <= self.end_index
        ):
            raise AnalysisChartError(
                "Candidate navigation extent must lie inside its window"
            )


@dataclass(frozen=True, slots=True)
class _VisibleChartBars:
    source_indices: tuple[int, ...]
    source_to_display: dict[int, int]

    def __post_init__(self) -> None:
        if len(set(self.source_indices)) != len(self.source_indices):
            raise AnalysisChartError("Visible chart source indices contain duplicates")

        if tuple(sorted(self.source_indices)) != self.source_indices:
            raise AnalysisChartError("Visible chart source indices are not ordered")


def _decimal_float(
    value: object,
) -> float | None:
    if value is None:
        return None

    try:
        result = float(value)
    except TypeError, ValueError, OverflowError:
        return None

    return result if math.isfinite(result) else None


def _visible_chart_bars(
    result: LiquidityMovementAnalysisResult,
) -> _VisibleChartBars:
    chart = result.dataset.chart

    indices = tuple(
        sorted(
            {
                index
                for segment in chart.segments
                for index in range(
                    segment.core_start_index,
                    segment.core_end_index + 1,
                )
            }
        )
    )

    return _VisibleChartBars(
        source_indices=indices,
        source_to_display={
            source_index: display_index
            for display_index, source_index in enumerate(indices)
        },
    )


def _local_label(
    value: datetime,
    timezone_name: str,
) -> str:
    return utc_to_local(
        value,
        timezone_name,
    ).isoformat(timespec="seconds")


def _candidate_is_visible(
    ranking: RankedLiquidityMovement,
    result: LiquidityMovementAnalysisResult,
    visibility: AnalysisChartVisibility,
) -> bool:
    candidate = ranking.candidate

    if candidate.metric not in visibility.highlighted_metrics:
        return False

    if (
        not visibility.show_height_selected
        and ranking.selected_by_height
        and not ranking.selected_by_sharpness
    ):
        return False

    if (
        not visibility.show_sharpness_selected
        and ranking.selected_by_sharpness
        and not ranking.selected_by_height
    ):
        return False

    if (
        ranking.selected_by_height
        and ranking.selected_by_sharpness
        and not (visibility.show_height_selected or visibility.show_sharpness_selected)
    ):
        return False

    activity_label = result.dataset.activity.timeframe.label
    chart_label = result.dataset.chart.timeframe.label
    label = candidate.timeframe_label

    is_activity = label == activity_label
    is_chart = label == chart_label

    if is_activity and is_chart:
        return bool(
            visibility.show_activity_candidates or visibility.show_chart_candidates
        )

    if is_activity:
        return visibility.show_activity_candidates

    if is_chart:
        return visibility.show_chart_candidates

    return False


def _individual_alpha(
    ranking: RankedLiquidityMovement,
    settings: LMConfig,
) -> float:
    score = _decimal_float(ranking.priority_weighted_evidence)

    if score is None:
        score = 0.0

    score = min(1.0, max(0.0, score))

    return float(settings.highlight_min_alpha) + score * (
        float(settings.highlight_max_alpha) - float(settings.highlight_min_alpha)
    )


def _accumulate_alpha(
    current: float,
    incoming: float,
) -> float:
    """Alpha-composite one same-direction highlight contribution."""
    return 1.0 - (1.0 - current) * (1.0 - incoming)


def _capped_opposite_direction_alphas(
    upward: float,
    downward: float,
    cap: float,
) -> tuple[float, float]:
    """Scale two alphas so their composed opacity does not exceed cap."""

    values = {
        "upward": float(upward),
        "downward": float(downward),
        "cap": float(cap),
    }

    for name, value in values.items():
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise AnalysisChartError(f"{name} alpha must be finite and inside [0, 1]")

    upward_value = values["upward"]
    downward_value = values["downward"]
    cap_value = values["cap"]

    combined = 1.0 - ((1.0 - upward_value) * (1.0 - downward_value))

    if combined <= cap_value or combined <= 0.0:
        return upward_value, downward_value

    total = upward_value + downward_value
    product = upward_value * downward_value

    if product <= 0.0:
        scale = cap_value / total
    else:
        discriminant = max(
            0.0,
            total * total - 4.0 * product * cap_value,
        )
        denominator = total + math.sqrt(discriminant)
        scale = 2.0 * cap_value / denominator if denominator > 0.0 else 0.0

    scale = min(
        1.0,
        max(
            0.0,
            scale,
        ),
    )

    return (
        upward_value * scale,
        downward_value * scale,
    )


def _candidate_display_indices(
    ranking: RankedLiquidityMovement,
    result: LiquidityMovementAnalysisResult,
    visible: _VisibleChartBars,
) -> tuple[int, ...]:
    candidate = ranking.candidate
    candidate_timeframe = get_timeframe(candidate.timeframe_label)

    interval_start = candidate.start_time_utc
    interval_end = candidate.end_time_utc + candidate_timeframe.duration

    chart_bars = result.dataset.chart.bars

    return tuple(
        visible.source_to_display[source_index]
        for source_index in visible.source_indices
        if (
            chart_bars[source_index].start_utc < interval_end
            and chart_bars[source_index].end_utc > interval_start
        )
    )


def _rgba_css(
    rgb: tuple[int, int, int],
    alpha: float,
) -> str:
    red, green, blue = rgb
    bounded_alpha = min(1.0, max(0.0, float(alpha)))

    return (
        f"rgba("
        f"{int(red)},"
        f"{int(green)},"
        f"{int(blue)},"
        f"{bounded_alpha:.6f}"
        f")"
    )


def _highlight_alpha_by_panel(
    result: LiquidityMovementAnalysisResult,
    visible: _VisibleChartBars,
    visibility: AnalysisChartVisibility,
    settings: LMConfig,
) -> dict[
    tuple[int, LiquidityMovementDirection],
    list[float],
]:
    count = len(visible.source_indices)
    values: dict[
        tuple[int, LiquidityMovementDirection],
        list[float],
    ] = {}

    for panel_index in range(5):
        for direction in LiquidityMovementDirection:
            values[(panel_index, direction)] = [0.0 for _index in range(count)]

    for ranking in result.selected:
        if not _candidate_is_visible(
            ranking,
            result,
            visibility,
        ):
            continue

        candidate = ranking.candidate
        display_indices = _candidate_display_indices(
            ranking,
            result,
            visible,
        )

        if not display_indices:
            continue

        destinations: list[int] = []

        if visibility.show_price_highlights:
            destinations.append(_PRICE_GRID_INDEX)

        if visibility.show_liquidity_highlights:
            destinations.append(_GRID_BY_METRIC[candidate.metric])

        alpha = _individual_alpha(
            ranking,
            settings,
        )

        for panel_index in destinations:
            target = values[
                (
                    panel_index,
                    candidate.direction,
                )
            ]

            for display_index in display_indices:
                target[display_index] = _accumulate_alpha(
                    target[display_index],
                    alpha,
                )

    cap = float(settings.highlight_accumulated_alpha_cap)

    # Keep the combined upward/downward effective opacity within the configured
    # cap. This changes rendering only; candidate evidence is untouched.
    for panel_index in range(5):
        upward = values[
            (
                panel_index,
                LiquidityMovementDirection.UP,
            )
        ]
        downward = values[
            (
                panel_index,
                LiquidityMovementDirection.DOWN,
            )
        ]

        for index in range(count):
            (
                upward[index],
                downward[index],
            ) = _capped_opposite_direction_alphas(
                upward[index],
                downward[index],
                cap,
            )

    return values


def _alpha_runs(
    values: list[float],
    *,
    rgb: tuple[int, int, int],
) -> list[dict[str, object]]:
    """Collapse equal rounded alpha values into contiguous rectangles."""
    result: list[dict[str, object]] = []
    start: int | None = None
    current_alpha = 0.0

    def finish(end_index: int) -> None:
        nonlocal start
        nonlocal current_alpha

        if start is None:
            return

        result.append(
            {
                "value": [
                    start,
                    end_index,
                ],
                "itemStyle": {
                    "color": _rgba_css(
                        rgb,
                        current_alpha,
                    ),
                },
            }
        )

        start = None
        current_alpha = 0.0

    for index, raw_alpha in enumerate([*values, 0.0]):
        alpha = round(float(raw_alpha), 4)

        if alpha <= 0.0:
            finish(index - 1)
            continue

        if start is None:
            start = index
            current_alpha = alpha
            continue

        if alpha != current_alpha:
            finish(index - 1)
            start = index
            current_alpha = alpha

    return result


def _slot_background_render_item_js() -> str:
    return r"""
    function(params, api) {
        const firstIndex = Number(api.value(0));
        const lastIndex = Number(api.value(1));

        if (
            !Number.isFinite(firstIndex)
            || !Number.isFinite(lastIndex)
            || !params.coordSys
        ) {
            return null;
        }

        const first = api.coord([firstIndex, 0]);
        const last = api.coord([lastIndex, 0]);

        if (
            !Array.isArray(first)
            || !Array.isArray(last)
            || !Number.isFinite(first[0])
            || !Number.isFinite(last[0])
        ) {
            return null;
        }

        let categoryWidth = 0;

        try {
            const size = api.size([1, 0]);
            categoryWidth = Array.isArray(size)
                ? Math.abs(Number(size[0]) || 0)
                : 0;
        } catch (error) {}

        if (!(categoryWidth > 0)) {
            categoryWidth = params.coordSys.width;
        }

        const rawLeft =
            Math.min(first[0], last[0]) - categoryWidth * 0.5;
        const rawRight =
            Math.max(first[0], last[0]) + categoryWidth * 0.5;

        const left = Math.max(
            params.coordSys.x,
            rawLeft
        );
        const right = Math.min(
            params.coordSys.x + params.coordSys.width,
            rawRight
        );

        if (!(right > left)) {
            return null;
        }

        return {
            type: 'rect',
            shape: {
                x: left,
                y: params.coordSys.y,
                width: right - left,
                height: params.coordSys.height,
            },
            style: api.style(),
        };
    }
    """


def _background_series(
    *,
    name: str,
    panel_index: int,
    data: list[dict[str, object]],
    z: int,
) -> dict[str, object] | None:
    if not data:
        return None

    return {
        "name": name,
        "type": "custom",
        "coordinateSystem": "cartesian2d",
        "xAxisIndex": panel_index,
        "yAxisIndex": panel_index,
        ":renderItem": _slot_background_render_item_js(),
        "dimensions": [
            "first_category",
            "last_category",
        ],
        "encode": {
            "x": [0, 1],
            "y": [],
            "tooltip": [],
        },
        "data": data,
        "silent": True,
        "tooltip": {"show": False},
        "legendHoverLink": True,
        "z": z,
    }


def _discontinuity_metadata(
    result: LiquidityMovementAnalysisResult,
    visible: _VisibleChartBars,
    *,
    l2_warning_seconds: int,
    price_warning_seconds: int,
) -> dict[int, dict[str, object]]:
    chart = result.dataset.chart
    metadata: dict[int, dict[str, object]] = {}

    for previous_source, current_source in zip(
        visible.source_indices,
        visible.source_indices[1:],
        strict=False,
    ):
        if current_source == previous_source + 1:
            continue

        previous_bar = chart.bars[previous_source]
        current_bar = chart.bars[current_source]
        skipped_seconds = int(
            (current_bar.start_utc - previous_bar.end_utc).total_seconds()
        )

        reasons: set[AnalysisDiscontinuityReason] = set()

        for discontinuity in chart.discontinuities:
            if (
                discontinuity.end_index < previous_source + 1
                or discontinuity.start_index > current_source - 1
            ):
                continue

            reasons.update(discontinuity.reasons)

        reason_values = [
            reason.value for reason in AnalysisDiscontinuityReason if reason in reasons
        ]

        l2_problem = bool(
            reasons
            & {
                AnalysisDiscontinuityReason.L2_INVALID,
                AnalysisDiscontinuityReason.L2_COVERAGE_GAP,
            }
        )
        price_problem = bool(
            reasons
            & {
                AnalysisDiscontinuityReason.PRICE_INVALID,
                AnalysisDiscontinuityReason.PRICE_COVERAGE_GAP,
            }
        )

        persistent_warning = bool(
            (l2_problem and skipped_seconds > l2_warning_seconds)
            or (price_problem and skipped_seconds > price_warning_seconds)
        )

        display_index = visible.source_to_display[current_source]

        metadata[display_index] = {
            "skipped_seconds": max(0, skipped_seconds),
            "reasons": reason_values,
            "persistent_warning": persistent_warning,
        }

    return metadata


def _discontinuity_series(
    metadata: dict[int, dict[str, object]],
    *,
    panel_index: int,
    colors: AnalysisChartColors,
) -> list[dict[str, object]]:
    ordinary: list[dict[str, object]] = []
    warning: list[dict[str, object]] = []

    for display_index, item in metadata.items():
        is_warning = bool(item["persistent_warning"])
        alpha = 0.24 if is_warning else 0.16
        rgb = (239, 68, 68) if is_warning else (245, 158, 11)

        target = warning if is_warning else ordinary
        target.append(
            {
                "value": [
                    display_index,
                    display_index,
                ],
                "itemStyle": {
                    "color": _rgba_css(
                        rgb,
                        alpha,
                    ),
                },
            }
        )

    result: list[dict[str, object]] = []

    for name, data, z in (
        ("Discontinuity", ordinary, 20),
        ("Persistent data warning", warning, 21),
    ):
        series = _background_series(
            name=f"{name} \u00b7 panel {panel_index}",
            panel_index=panel_index,
            data=data,
            z=z,
        )

        if series is not None:
            result.append(series)

    return result


def _tooltip_formatter_js(
    metadata_by_index: list[dict[str, object]],
) -> str:
    payload = json.dumps(
        metadata_by_index,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )

    template = r"""
    function(params) {
        const rows = __L2SHOCK_TOOLTIP_ROWS__;
        const ps = Array.isArray(params) ? params : [params];

        if (!ps.length) {
            return '';
        }

        const categoryParam = ps.find(
            item =>
                item
                && item.seriesType !== 'custom'
                && Number.isFinite(Number(item.dataIndex))
        );

        if (!categoryParam) {
            return '';
        }

        const index = Number(categoryParam.dataIndex);

        if (
            !Number.isInteger(index)
            || index < 0
            || index >= rows.length
        ) {
            return '';
        }

        const row = rows[index] || {};

        function escapeHtml(value) {
            return String(value ?? '')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#039;');
        }

        function formatNumber(value) {
            const numeric = Number(value);

            if (!Number.isFinite(numeric)) {
                return 'unavailable';
            }

            return numeric.toLocaleString(undefined, {
                maximumFractionDigits: 10,
            });
        }

        let output =
            '<div style="max-width:460px;white-space:normal;">'
            + '<b>' + escapeHtml(row.local_time || '') + '</b>'
            + '<br/><span style="color:#94a3b8;">UTC '
            + escapeHtml(row.utc_time || '') + '</span>';

        if (row.price) {
            output +=
                '<br/><b>Price</b>'
                + ' O ' + formatNumber(row.price.open)
                + ' H ' + formatNumber(row.price.high)
                + ' L ' + formatNumber(row.price.low)
                + ' C ' + formatNumber(row.price.close);
        }

        output +=
            '<br/><b>Bid Liquidity:</b> '
            + formatNumber(row.bid_liquidity)
            + '<br/><b>Ask Liquidity:</b> '
            + formatNumber(row.ask_liquidity)
            + '<br/><b>Total Liquidity:</b> '
            + formatNumber(row.total_liquidity)
            + '<br/><b>Order-Book Delta:</b> '
            + formatNumber(row.delta);

        if (row.discontinuity) {
            output +=
                '<div style="margin-top:6px;border-top:1px solid #475569;'
                + 'padding-top:5px;color:'
                + (row.discontinuity.persistent_warning
                    ? '#f87171'
                    : '#fbbf24')
                + ';">'
                + '<b>Timeline jump</b><br/>'
                + 'Skipped elapsed time: '
                + escapeHtml(row.discontinuity.skipped_seconds)
                + ' second(s)<br/>'
                + 'Reasons: '
                + escapeHtml(
                    (row.discontinuity.reasons || []).join(', ')
                )
                + '</div>';
        }

        output += '</div>';
        return output;
    }
    """

    return template.replace(
        "__L2SHOCK_TOOLTIP_ROWS__",
        payload,
    )


def _chart_metadata(
    result: LiquidityMovementAnalysisResult,
    visible: _VisibleChartBars,
    *,
    timezone_name: str,
    discontinuities: dict[int, dict[str, object]],
) -> list[dict[str, object]]:
    bars = result.dataset.chart.bars
    result_rows: list[dict[str, object]] = []

    for display_index, source_index in enumerate(visible.source_indices):
        bar = bars[source_index]
        display = bar.display_ohlc

        result_rows.append(
            {
                "utc_time": bar.start_utc.isoformat(),
                "local_time": _local_label(
                    bar.start_utc,
                    timezone_name,
                ),
                "price": (
                    {
                        "open": _decimal_float(display.open),
                        "high": _decimal_float(display.high),
                        "low": _decimal_float(display.low),
                        "close": _decimal_float(display.close),
                    }
                    if display is not None
                    else None
                ),
                "bid_liquidity": _decimal_float(bar.bid_liquidity),
                "ask_liquidity": _decimal_float(bar.ask_liquidity),
                "total_liquidity": _decimal_float(bar.total_liquidity),
                "delta": _decimal_float(bar.l2.bid_ask_delta()),
                "discontinuity": discontinuities.get(display_index),
            }
        )

    return result_rows


def _line_series(
    *,
    name: str,
    panel_index: int,
    data: list[float | None],
    color: str,
    area_color: str | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "name": name,
        "type": "line",
        "xAxisIndex": panel_index,
        "yAxisIndex": panel_index,
        "data": data,
        "showSymbol": False,
        "connectNulls": False,
        "lineStyle": {
            "width": 1.5,
            "color": color,
        },
        "sampling": "lttb",
        "progressive": 5_000,
        "z": 30,
    }

    if area_color is not None:
        result["areaStyle"] = {
            "color": area_color,
        }

    return result


def _selected_focus_series(
    panel_index: int,
) -> dict[str, object]:
    """Return one empty series reserved for browser-owned LM focus."""

    return {
        "id": (ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX + str(panel_index)),
        "name": f"Selected LM focus \u00b7 panel {panel_index}",
        "type": "line",
        "xAxisIndex": panel_index,
        "yAxisIndex": panel_index,
        "data": [],
        "showSymbol": False,
        "silent": True,
        "tooltip": {"show": False},
        "legendHoverLink": False,
        "lineStyle": {
            "opacity": 0,
        },
        "markArea": {
            "silent": True,
            "label": {"show": False},
            "itemStyle": {
                "color": "rgba(245,158,11,0.18)",
                "borderColor": "#f59e0b",
                "borderWidth": 2,
            },
            "data": [],
        },
        "z": 46,
    }


def empty_analysis_chart_option(
    title: str = "Run Analysis to render charts",
) -> dict[str, Any]:
    return {
        "animation": False,
        "backgroundColor": ANALYSIS_CHART_COLORS.background,
        "title": {
            "text": title,
            "left": "center",
            "top": "middle",
            "textStyle": {
                "color": ANALYSIS_CHART_COLORS.muted_text,
                "fontSize": 13,
            },
        },
        "tooltip": {"show": False},
        "legend": {"show": False},
        "xAxis": [
            {
                "type": "category",
                "data": [],
                "show": False,
            }
        ],
        "yAxis": [
            {
                "type": "value",
                "show": False,
            }
        ],
        "series": [],
    }


def build_analysis_chart_option(
    result: LiquidityMovementAnalysisResult,
    *,
    timezone_name: str,
    visibility: AnalysisChartVisibility | None = None,
    lm_settings: LMConfig | None = None,
    l2_warning_seconds: int = 60,
    price_warning_seconds: int = 180,
) -> dict[str, Any]:
    """Build one synchronized five-panel chart option."""
    if not isinstance(
        result,
        LiquidityMovementAnalysisResult,
    ):
        raise TypeError("result must be LiquidityMovementAnalysisResult")

    selected_visibility = (
        visibility if visibility is not None else AnalysisChartVisibility()
    )
    selected_settings = lm_settings if lm_settings is not None else LMConfig()

    if not isinstance(
        selected_visibility,
        AnalysisChartVisibility,
    ):
        raise TypeError("visibility must be AnalysisChartVisibility")

    if not isinstance(selected_settings, LMConfig):
        raise TypeError("lm_settings must be LMConfig")

    for field_name, value in (
        ("l2_warning_seconds", l2_warning_seconds),
        ("price_warning_seconds", price_warning_seconds),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise AnalysisChartError(f"{field_name} must be a positive integer")

    visible = _visible_chart_bars(result)

    if not visible.source_indices:
        return empty_analysis_chart_option("No continuous eligible chart bars")

    chart = result.dataset.chart
    bars = chart.bars
    colors = ANALYSIS_CHART_COLORS

    labels = [
        _local_label(
            bars[source_index].start_utc,
            timezone_name,
        )
        for source_index in visible.source_indices
    ]

    price_data: list[object] = []
    bid_data: list[float | None] = []
    ask_data: list[float | None] = []
    total_data: list[float | None] = []
    delta_positive: list[float | None] = []
    delta_negative: list[float | None] = []

    for source_index in visible.source_indices:
        bar = bars[source_index]
        display = bar.display_ohlc

        if display is None:
            price_data.append("-")
        else:
            price_data.append(
                [
                    _decimal_float(display.open),
                    _decimal_float(display.close),
                    _decimal_float(display.low),
                    _decimal_float(display.high),
                ]
            )

        bid_data.append(_decimal_float(bar.bid_liquidity))
        ask_data.append(_decimal_float(bar.ask_liquidity))
        total_data.append(_decimal_float(bar.total_liquidity))

        delta = _decimal_float(bar.l2.bid_ask_delta())

        delta_positive.append(delta if delta is not None and delta >= 0.0 else None)
        delta_negative.append(delta if delta is not None and delta < 0.0 else None)

    grids = [
        {
            "left": 82,
            "right": 42,
            "top": 42,
            "height": "29%",
        },
        {
            "left": 82,
            "right": 42,
            "top": "35%",
            "height": "12%",
        },
        {
            "left": 82,
            "right": 42,
            "top": "50%",
            "height": "12%",
        },
        {
            "left": 82,
            "right": 42,
            "top": "65%",
            "height": "12%",
        },
        {
            "left": 82,
            "right": 42,
            "top": "80%",
            "height": "12%",
        },
    ]

    x_axes = [
        {
            "type": "category",
            "data": labels,
            "gridIndex": index,
            "boundaryGap": True,
            "axisLabel": {
                "show": index == _DELTA_GRID_INDEX,
                "fontSize": 9,
                "color": colors.muted_text,
            },
            "axisLine": {
                "lineStyle": {
                    "color": colors.grid_line,
                }
            },
            "axisPointer": {
                "show": True,
                "lineStyle": {
                    "opacity": 0,
                },
                "label": {
                    "show": True,
                    "backgroundColor": "#111827",
                },
            },
        }
        for index in range(5)
    ]

    y_axis_names = (
        "Price (USDT)",
        "Bid Liquidity (USD eq.)",
        "Ask Liquidity (USD eq.)",
        "Total Liquidity (USD eq.)",
        "Order-Book Delta (USD eq.)",
    )

    y_axes = [
        {
            "type": "value",
            "scale": True,
            "gridIndex": index,
            "name": name,
            "nameTextStyle": {
                "fontSize": 10,
                "color": colors.text,
            },
            "axisLabel": {
                "fontSize": 9,
                "color": colors.muted_text,
            },
            "axisLine": {
                "lineStyle": {
                    "color": colors.grid_line,
                }
            },
            "splitLine": {
                "show": True,
                "lineStyle": {
                    "color": colors.grid_line,
                    "opacity": 0.55,
                },
            },
            "axisPointer": {
                "show": index == _PRICE_GRID_INDEX,
                "lineStyle": {
                    "opacity": 0,
                },
                "label": {
                    "show": index == _PRICE_GRID_INDEX,
                    "backgroundColor": "#111827",
                },
            },
        }
        for index, name in enumerate(y_axis_names)
    ]

    series: list[dict[str, object]] = [
        {
            "name": "Binance perpetual price",
            "type": "candlestick",
            "xAxisIndex": _PRICE_GRID_INDEX,
            "yAxisIndex": _PRICE_GRID_INDEX,
            "data": price_data,
            "itemStyle": {
                "color": colors.price_up,
                "color0": colors.price_down,
                "borderColor": colors.price_up,
                "borderColor0": colors.price_down,
            },
            "progressive": 5_000,
            "z": 30,
        },
        _line_series(
            name="Bid Liquidity",
            panel_index=_BID_GRID_INDEX,
            data=bid_data,
            color=colors.bid_line,
            area_color="rgba(56,189,248,0.07)",
        ),
        _line_series(
            name="Ask Liquidity",
            panel_index=_ASK_GRID_INDEX,
            data=ask_data,
            color=colors.ask_line,
            area_color="rgba(249,115,22,0.07)",
        ),
        _line_series(
            name="Total Liquidity",
            panel_index=_TOTAL_GRID_INDEX,
            data=total_data,
            color=colors.total_line,
            area_color="rgba(167,139,250,0.07)",
        ),
        _line_series(
            name="Positive Delta",
            panel_index=_DELTA_GRID_INDEX,
            data=delta_positive,
            color=colors.delta_positive,
            area_color="rgba(34,197,94,0.09)",
        ),
        _line_series(
            name="Negative Delta",
            panel_index=_DELTA_GRID_INDEX,
            data=delta_negative,
            color=colors.delta_negative,
            area_color="rgba(239,68,68,0.09)",
        ),
    ]

    series.extend(_selected_focus_series(panel_index) for panel_index in range(5))

    negative_delta_series = next(
        (item for item in series if item.get("name") == "Negative Delta"),
        None,
    )

    if negative_delta_series is None:
        raise AnalysisChartError("Negative Delta series is missing")

    negative_delta_series["markLine"] = {
        "silent": True,
        "symbol": "none",
        "data": [
            {
                "yAxis": 0,
                "lineStyle": {
                    "color": colors.delta_zero,
                    "type": "dashed",
                    "width": 1,
                },
                "label": {"show": False},
            }
        ],
    }

    alpha_values = _highlight_alpha_by_panel(
        result,
        visible,
        selected_visibility,
        selected_settings,
    )

    for panel_index in range(5):
        for direction, rgb, label in (
            (
                LiquidityMovementDirection.UP,
                colors.upward_lm_rgb,
                "Upward LM highlights",
            ),
            (
                LiquidityMovementDirection.DOWN,
                colors.downward_lm_rgb,
                "Downward LM highlights",
            ),
        ):
            data = _alpha_runs(
                alpha_values[
                    (
                        panel_index,
                        direction,
                    )
                ],
                rgb=rgb,
            )
            background = _background_series(
                name=f"{label} \u00b7 panel {panel_index}",
                panel_index=panel_index,
                data=data,
                z=2,
            )

            if background is not None:
                series.append(background)

    discontinuity_metadata = _discontinuity_metadata(
        result,
        visible,
        l2_warning_seconds=l2_warning_seconds,
        price_warning_seconds=price_warning_seconds,
    )

    for panel_index in range(5):
        series.extend(
            _discontinuity_series(
                discontinuity_metadata,
                panel_index=panel_index,
                colors=colors,
            )
        )

    tooltip_metadata = _chart_metadata(
        result,
        visible,
        timezone_name=timezone_name,
        discontinuities=discontinuity_metadata,
    )

    return {
        "animation": False,
        "backgroundColor": colors.background,
        "useUTC": False,
        "title": {
            "text": (
                f"{result.dataset.request.base} Liquidity Shock Analysis"
                f" \u00b7 chart {chart.timeframe.label}"
                f" \u00b7 activity {result.dataset.activity.timeframe.label}"
            ),
            "left": "center",
            "top": 4,
            "textStyle": {
                "fontSize": 13,
                "fontWeight": "bold",
                "color": colors.text,
            },
        },
        "legend": {
            "top": 22,
            "type": "scroll",
            "textStyle": {
                "fontSize": 10,
                "color": colors.text,
            },
            "data": [
                "Binance perpetual price",
                "Bid Liquidity",
                "Ask Liquidity",
                "Total Liquidity",
                "Positive Delta",
                "Negative Delta",
                "Upward LM highlights \u00b7 panel 0",
                "Downward LM highlights \u00b7 panel 0",
            ],
        },
        "tooltip": {
            "trigger": "axis",
            "renderMode": "html",
            "appendToBody": True,
            "enterable": True,
            "confine": False,
            "transitionDuration": 0,
            "axisPointer": {
                "type": "cross",
                "lineStyle": {
                    "opacity": 0,
                },
                "crossStyle": {
                    "opacity": 0,
                },
            },
            "extraCssText": (
                "max-width:min(480px,calc(100vw - 64px));"
                "max-height:min(70vh,700px);"
                "overflow:auto;"
                "white-space:normal;"
                "z-index:2147483647;"
            ),
            ":formatter": _tooltip_formatter_js(tooltip_metadata),
        },
        "axisPointer": {
            "show": True,
            "type": "cross",
            "link": [
                {
                    "xAxisIndex": "all",
                }
            ],
            "lineStyle": {
                "opacity": 0,
            },
            "crossStyle": {
                "opacity": 0,
            },
            "label": {
                "show": True,
                "backgroundColor": "#111827",
            },
        },
        "grid": grids,
        "xAxis": x_axes,
        "yAxis": y_axes,
        "dataZoom": [
            {
                "type": "inside",
                "xAxisIndex": [0, 1, 2, 3, 4],
                "filterMode": "none",
                "start": 0,
                "end": 100,
                "zoomOnMouseWheel": True,
                "moveOnMouseMove": False,
                "moveOnMouseWheel": False,
            },
            {
                "type": "slider",
                "xAxisIndex": [0, 1, 2, 3, 4],
                "filterMode": "none",
                "start": 0,
                "end": 100,
                "bottom": 2,
                "height": 18,
            },
        ],
        "series": series,
        "l2shockChartMetadata": {
            "analysis_id": result.analysis_id,
            "dataset_analysis_id": result.dataset.analysis_id,
            "chart_timeframe": chart.timeframe.label,
            "activity_timeframe": (result.dataset.activity.timeframe.label),
            "visible_bar_count": len(visible.source_indices),
            "source_bar_indices": list(visible.source_indices),
            "visible_start_times_utc": [
                chart.bars[source_index].start_utc.isoformat().replace("+00:00", "Z")
                for source_index in visible.source_indices
            ],
            "bar_duration_seconds": chart.timeframe.seconds,
            "discontinuities": discontinuity_metadata,
        },
    }


def chart_navigation_window(
    result: LiquidityMovementAnalysisResult,
    ranking: RankedLiquidityMovement,
    *,
    padding_bars: int = 5,
) -> ChartNavigationWindow | None:
    """Map one selected LM onto compressed chart categories."""
    if not isinstance(
        result,
        LiquidityMovementAnalysisResult,
    ):
        raise TypeError("result must be LiquidityMovementAnalysisResult")

    if not isinstance(
        ranking,
        RankedLiquidityMovement,
    ):
        raise TypeError("ranking must be RankedLiquidityMovement")

    if (
        isinstance(padding_bars, bool)
        or not isinstance(padding_bars, int)
        or padding_bars < 0
    ):
        raise AnalysisChartError("padding_bars must be a non-negative integer")

    visible = _visible_chart_bars(result)
    indices = _candidate_display_indices(
        ranking,
        result,
        visible,
    )

    if not indices:
        return None

    candidate_start = min(indices)
    candidate_end = max(indices)
    maximum_index = len(visible.source_indices) - 1

    return ChartNavigationWindow(
        start_index=max(
            0,
            candidate_start - padding_bars,
        ),
        end_index=min(
            maximum_index,
            candidate_end + padding_bars,
        ),
        candidate_start_index=candidate_start,
        candidate_end_index=candidate_end,
    )


__all__ = [
    "ANALYSIS_CHART_COLORS",
    "ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX",
    "AnalysisChartColors",
    "AnalysisChartError",
    "AnalysisChartVisibility",
    "ChartHighlightDestination",
    "ChartNavigationWindow",
    "build_analysis_chart_option",
    "chart_navigation_window",
    "empty_analysis_chart_option",
]

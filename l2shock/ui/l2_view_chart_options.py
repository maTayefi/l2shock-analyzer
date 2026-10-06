# l2shock/ui/l2_view_chart_options.py
"""Five-panel Analysis chart: Price, Bid, Ask, Panel A, Panel B.

Presentation only. X zoom (dataZoom index 0) is synchronized across all
panels and must stay first: restore_shock_time_viewport dispatches to it.
Each panel has an independent inside Y zoom on Shift+wheel.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

from l2shock.analysis.l2_ratio_extremeness import (
    RATIO_EXTREMENESS_ALGORITHM_VERSION,
    RatioExtremenessResult,
    RatioExtremenessSide,
    compute_ratio_extremeness,
)
from l2shock.analysis.l2_view_metrics import (
    L2_VIEW_METRIC_SPECS,
    L2ViewMetric,
    L2ViewMetricKind,
    L2ViewMetricSpec,
    compute_l2_view_metric,
    l2_view_metric,
)
from l2shock.analysis.l2_view_stream import L2ViewOutageRegion, L2ViewProjection
from l2shock.ui.l2_view_warning_style import WARNING_REGION_COLOR

L2_VIEW_COLORS = {
    "price_up": "#26a69a",
    "price_down": "#ef5350",
    "bid": "#42a5f5",
    "ask": "#ffb74d",
    "data_outage": WARNING_REGION_COLOR,
}

PRICE_SERIES_ID = "l2view-price"
_PANELS = 5

# Invisible helper series: carries the background markArea and tooltip data.
RATIO_EXTREMENESS_SERIES_SUFFIX = "-extremeness"
RATIO_EXTREMENESS_PANEL_METRICS = frozenset(
    {
        L2ViewMetric.IMBALANCE_PCT,
        L2ViewMetric.BID_SHARE_PCT,
        L2ViewMetric.ASK_SHARE_PCT,
    }
)
RATIO_EXTREMENESS_SIDE_CODES = {
    RatioExtremenessSide.BID_DOMINANT: 1,
    RatioExtremenessSide.ASK_DOMINANT: 2,
    RatioExtremenessSide.BOTH: 3,
}
_EXTREMENESS_RGB = {
    RatioExtremenessSide.BID_DOMINANT: "45, 212, 191",
    RatioExtremenessSide.ASK_DOMINANT: "244, 114, 182",
    RatioExtremenessSide.BOTH: "167, 139, 250",
}
_EXTREMENESS_MAX_OPACITY = 0.45
_EXTREMENESS_MIN_VISIBLE_OPACITY = 0.02


class L2ViewChartError(ValueError):
    """Invalid projection for chart construction."""


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def l2_view_bar_time_coordinates(
    projection: L2ViewProjection,
) -> tuple[tuple[datetime, datetime, datetime], ...]:
    """Return owned start, owned exclusive end, and plot centre per bar.

    The analytical bucket start remains bar.start_utc. Display coordinates
    belong to the requested portion of that bucket, including partial edges.
    """
    if not isinstance(projection, L2ViewProjection):
        raise TypeError("projection must be L2ViewProjection")

    duration = timedelta(seconds=projection.timeframe_seconds)
    coordinates: list[tuple[datetime, datetime, datetime]] = []

    for bar in projection.bars:
        owned_start = max(projection.start_utc, bar.start_utc)
        owned_end = min(
            projection.end_utc_exclusive,
            bar.start_utc + duration,
        )

        if owned_end <= owned_start:
            raise L2ViewChartError(
                "Viewing bar has no owned interval inside the projection"
            )

        centre = owned_start + (owned_end - owned_start) / 2
        coordinates.append((owned_start, owned_end, centre))

    return tuple(coordinates)


def _finite(value: object) -> float:
    number = float(value)  # type: ignore[arg-type]
    if not math.isfinite(number):
        raise L2ViewChartError("Value cannot be represented on the chart")
    return number


def _candle(ohlc: Any) -> list[float | None]:
    """ECharts candlestick order: open, close, low, high."""
    if ohlc is None:
        return [None, None, None, None]
    o, h, l, c = (_finite(v) for v in ohlc)
    return [o, c, l, h]


def _warning_area(regions: tuple[L2ViewOutageRegion, ...]) -> dict[str, Any]:
    return {
        "silent": True,
        "animation": False,
        "label": {"show": False},
        "data": [
            [
                {
                    "name": f"l2shock-warning:{r.channel}",
                    "xAxis": _iso(r.start_utc),
                    "itemStyle": {"color": WARNING_REGION_COLOR},
                },
                {"xAxis": _iso(r.end_utc_exclusive)},
            ]
            for r in regions
        ],
    }


def _candle_series(
    sid: str, name: str, panel: int, data: list, up: str, down: str
) -> dict:
    return {
        "id": sid,
        "name": name,
        "type": "candlestick",
        "xAxisIndex": panel,
        "yAxisIndex": panel,
        "data": data,
        "itemStyle": {
            "color": up,
            "color0": down,
            "borderColor": up,
            "borderColor0": down,
        },
    }


def _metric_series(
    projection: L2ViewProjection, metric: object, panel: int, slot: str
) -> list[dict]:
    spec = L2_VIEW_METRIC_SPECS[l2_view_metric(metric)]
    values = compute_l2_view_metric(projection.bars, spec.metric)
    times = [
        _iso(centre)
        for _start, _end, centre in l2_view_bar_time_coordinates(projection)
    ]

    if spec.kind is L2ViewMetricKind.CANDLE:
        data = [[t, *_candle(v)] for t, v in zip(times, values, strict=True)]
        color = spec.colors[0]
        return [
            _candle_series(
                f"l2view-panel-{slot}-0",
                spec.series_names[0],
                panel,
                data,
                color,
                color,
            )
        ]

    series = []
    lines = 2 if spec.kind is L2ViewMetricKind.TWO_LINES else 1

    for index in range(lines):
        data = []
        for t, v in zip(times, values, strict=True):
            item = v if lines == 1 else (None if v is None else v[index])  # type: ignore[index]
            data.append([t, None if item is None else _finite(item)])
        series.append(
            {
                "id": f"l2view-panel-{slot}-{index}",
                "name": spec.series_names[index],
                "type": "line",
                "xAxisIndex": panel,
                "yAxisIndex": panel,
                "showSymbol": False,
                "connectNulls": False,
                "lineStyle": {"color": spec.colors[index], "width": 1.5},
                "itemStyle": {"color": spec.colors[index]},
                "data": data,
            }
        )

    return series


def ratio_extremeness_opacity(score: float) -> float:
    """Fixed presentation curve: transparent near 0, strongest at 100."""
    if not math.isfinite(score) or score <= 0.0:
        return 0.0
    bounded = min(score, 100.0) / 100.0
    return round(_EXTREMENESS_MAX_OPACITY * bounded * bounded, 4)


def _ratio_extremeness_series(
    projection: L2ViewProjection,
    result: RatioExtremenessResult,
    spec: L2ViewMetricSpec,
    panel: int,
    slot: str,
    *,
    show_background: bool,
) -> list[dict]:
    if spec.metric not in RATIO_EXTREMENESS_PANEL_METRICS:
        return []

    coordinates = l2_view_bar_time_coordinates(projection)
    data: list[list[object]] = []
    areas: list[list[dict[str, object]]] = []

    for (owned_start, owned_end, centre), entry in zip(
        coordinates,
        result.bars,
        strict=True,
    ):
        timestamp = _iso(centre)

        if entry is None:
            data.append([timestamp, None, None, None])
            continue

        side_code = (
            None if entry.side is None else RATIO_EXTREMENESS_SIDE_CODES[entry.side]
        )
        data.append([timestamp, None, round(_finite(entry.score), 4), side_code])

        if not show_background or entry.side is None:
            continue

        alpha = ratio_extremeness_opacity(entry.score)

        if alpha < _EXTREMENESS_MIN_VISIBLE_OPACITY:
            continue

        # Background ownership matches the requested part of this bucket.
        # Partial edge bars must not shade time outside the loaded range.
        areas.append(
            [
                {
                    "name": f"l2shock-extremeness:{entry.side.value}",
                    "xAxis": _iso(owned_start),
                    "itemStyle": {
                        "color": f"rgba({_EXTREMENESS_RGB[entry.side]}, {alpha})"
                    },
                },
                {"xAxis": _iso(owned_end)},
            ]
        )

    series: dict[str, Any] = {
        "id": f"l2view-panel-{slot}{RATIO_EXTREMENESS_SERIES_SUFFIX}",
        "name": "L2 Ratio Extremeness",
        "type": "line",
        "xAxisIndex": panel,
        "yAxisIndex": panel,
        "z": 1,
        "showSymbol": False,
        "symbol": "none",
        "connectNulls": False,
        "lineStyle": {"opacity": 0, "width": 0},
        "itemStyle": {"opacity": 0},
        "data": data,
    }

    if areas:
        series["markArea"] = {
            "silent": True,
            "animation": False,
            "label": {"show": False},
            "data": areas,
        }

    return [series]


def build_l2_view_chart_options(
    projection: L2ViewProjection,
    *,
    panel_a_metric: object,
    panel_b_metric: object,
    show_warnings: bool = True,
    show_ratio_extremeness: bool = True,
) -> dict[str, Any]:
    if not isinstance(projection, L2ViewProjection) or not projection.bars:
        raise L2ViewChartError("Projection must contain at least one viewing bar")

    spec_a = L2_VIEW_METRIC_SPECS[l2_view_metric(panel_a_metric)]
    spec_b = L2_VIEW_METRIC_SPECS[l2_view_metric(panel_b_metric)]
    names = ("Price", "Bid", "Ask", spec_a.axis_name, spec_b.axis_name)
    # Population = every displayed viewing bar; never the browser zoom.
    extremeness = compute_ratio_extremeness(projection.bars)
    show_background = bool(show_ratio_extremeness)
    source_start = _iso(projection.start_utc)
    source_end = _iso(projection.end_utc_exclusive)

    grid, x_axes, y_axes = [], [], []

    for panel, name in enumerate(names):
        grid.append(
            {"top": f"{4 + panel * 19}%", "height": "15%", "left": "9%", "right": "3%"}
        )
        x_axes.append(
            {
                "type": "time",
                "gridIndex": panel,
                "min": source_start,
                "max": source_end,
                "axisLabel": {"show": panel == _PANELS - 1},
                "axisTick": {"show": panel == _PANELS - 1},
            }
        )
        y_axes.append(
            {
                "type": "value",
                "gridIndex": panel,
                "scale": True,
                "name": name,
                "nameLocation": "middle",
                "nameGap": 46,
                "splitLine": {"show": False},
            }
        )

    coordinates = l2_view_bar_time_coordinates(projection)
    times = [_iso(centre) for _start, _end, centre in coordinates]
    bucket_times = [_iso(bar.start_utc) for bar in projection.bars]
    owned_starts = [_iso(start) for start, _end, _centre in coordinates]
    owned_ends = [_iso(end) for _start, end, _centre in coordinates]

    price_name = (
        "Price (optional context)"
        if projection.price_status == "loaded"
        else f"Price (optional; {projection.price_status})"
    )
    series = [
        _candle_series(
            PRICE_SERIES_ID,
            price_name,
            0,
            [
                [t, *_candle(bar.price)]
                for t, bar in zip(times, projection.bars, strict=True)
            ],
            L2_VIEW_COLORS["price_up"],
            L2_VIEW_COLORS["price_down"],
        ),
        _candle_series(
            "l2view-bid",
            "Bid",
            1,
            [
                [t, *_candle(bar.bid)]
                for t, bar in zip(times, projection.bars, strict=True)
            ],
            L2_VIEW_COLORS["bid"],
            L2_VIEW_COLORS["bid"],
        ),
        _candle_series(
            "l2view-ask",
            "Ask",
            2,
            [
                [t, *_candle(bar.ask)]
                for t, bar in zip(times, projection.bars, strict=True)
            ],
            L2_VIEW_COLORS["ask"],
            L2_VIEW_COLORS["ask"],
        ),
        *_metric_series(projection, spec_a.metric, 3, "a"),
        *_ratio_extremeness_series(
            projection,
            extremeness,
            spec_a,
            3,
            "a",
            show_background=show_background,
        ),
        *_metric_series(projection, spec_b.metric, 4, "b"),
        *_ratio_extremeness_series(
            projection,
            extremeness,
            spec_b,
            4,
            "b",
            show_background=show_background,
        ),
    ]

    if show_warnings:
        for item in series:
            regions = projection.l2_regions
            if item["id"] == PRICE_SERIES_ID:
                regions = regions + projection.price_regions
            if regions and item["xAxisIndex"] in {0, 1, 2, 3, 4}:
                # Only the first series of each panel carries the overlay.
                if item["id"].endswith("-1"):
                    continue
                if item["id"].endswith(RATIO_EXTREMENESS_SERIES_SUFFIX):
                    continue
                item["markArea"] = _warning_area(regions)

    data_zoom: list[dict[str, Any]] = [
        {"type": "inside", "xAxisIndex": list(range(_PANELS)), "filterMode": "none"}
    ]
    data_zoom += [
        {
            "type": "inside",
            "yAxisIndex": [panel],
            "filterMode": "none",
            "zoomOnMouseWheel": "shift",
            "moveOnMouseWheel": False,
            "moveOnMouseMove": False,
        }
        for panel in range(_PANELS)
    ]

    return {
        "animation": False,
        "backgroundColor": "#0f172a",
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "axisPointer": {"link": [{"xAxisIndex": list(range(_PANELS))}]},
        "grid": grid,
        "xAxis": x_axes,
        "yAxis": y_axes,
        "dataZoom": data_zoom,
        "series": series,
        "l2shockChartMetadata": {
            "analysis_id": projection.input_id,
            "dataset_analysis_id": projection.input_id,
            "chart_timeframe": f"{projection.timeframe_seconds}s",
            "activity_timeframe": "1s",
            "visible_bar_count": len(projection.bars),
            "visible_start_times_utc": bucket_times,
            "visible_owned_start_times_utc": owned_starts,
            "visible_owned_end_times_utc_exclusive": owned_ends,
            "visible_plot_times_utc": times,
            "plot_timestamp_policy": "owned_interval_midpoint_v1",
            "bar_duration_seconds": projection.timeframe_seconds,
            "source_bar_indices": list(range(len(projection.bars))),
            "discontinuities": {},
            "panel_a_metric": spec_a.metric.value,
            "panel_b_metric": spec_b.metric.value,
            "ratio_extremeness_algorithm_version": (
                RATIO_EXTREMENESS_ALGORITHM_VERSION
            ),
            "ratio_extremeness_status": extremeness.status.value,
            "ratio_extremeness_eligible_bars": extremeness.eligible_bar_count,
        },
    }


__all__ = [
    "L2_VIEW_COLORS",
    "PRICE_SERIES_ID",
    "RATIO_EXTREMENESS_PANEL_METRICS",
    "RATIO_EXTREMENESS_SERIES_SUFFIX",
    "RATIO_EXTREMENESS_SIDE_CODES",
    "L2ViewChartError",
    "build_l2_view_chart_options",
    "ratio_extremeness_opacity",
]

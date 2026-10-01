# l2shock/ui/l2_view_chart_options.py
"""Five-panel Analysis chart: Price, Bid, Ask, Panel A, Panel B.

Presentation only. X zoom (dataZoom index 0) is synchronized across all
panels and must stay first: restore_shock_time_viewport dispatches to it.
Each panel has an independent inside Y zoom on Shift+wheel.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from l2shock.analysis.l2_view_metrics import (
    L2_VIEW_METRIC_SPECS,
    L2ViewMetricKind,
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


class L2ViewChartError(ValueError):
    """Invalid projection for chart construction."""


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


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
    times = [_iso(bar.start_utc) for bar in projection.bars]

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


def build_l2_view_chart_options(
    projection: L2ViewProjection,
    *,
    panel_a_metric: object,
    panel_b_metric: object,
    show_warnings: bool = True,
) -> dict[str, Any]:
    if not isinstance(projection, L2ViewProjection) or not projection.bars:
        raise L2ViewChartError("Projection must contain at least one viewing bar")

    spec_a = L2_VIEW_METRIC_SPECS[l2_view_metric(panel_a_metric)]
    spec_b = L2_VIEW_METRIC_SPECS[l2_view_metric(panel_b_metric)]
    names = ("Price", "Bid", "Ask", spec_a.axis_name, spec_b.axis_name)
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

    times = [_iso(bar.start_utc) for bar in projection.bars]
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
        *_metric_series(projection, spec_b.metric, 4, "b"),
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
            "visible_start_times_utc": times,
            "bar_duration_seconds": projection.timeframe_seconds,
            "source_bar_indices": list(range(len(projection.bars))),
            "discontinuities": {},
            "panel_a_metric": spec_a.metric.value,
            "panel_b_metric": spec_b.metric.value,
        },
    }


__all__ = [
    "L2_VIEW_COLORS",
    "PRICE_SERIES_ID",
    "L2ViewChartError",
    "build_l2_view_chart_options",
]

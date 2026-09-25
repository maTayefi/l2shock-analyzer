# l2shock/ui/shock_view_chart_options.py
"""Five-panel ECharts options for a bounded Shock L2 viewing projection.

The source projection contains viewing bars; its selected B interval and
representative anchors are specified in absolute one-second dataset
indices. Annotations use exact UTC source timestamps, never rounded
viewing-bar boundaries.

Price is optional visual context and is not loaded by this builder.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from l2shock.ui.shock_view_bars import (
    L2Ohlc,
    ShockViewProjection,
)

_ONE_SECOND = timedelta(seconds=1)

_COLORS = {
    "bid": "#42a5f5",
    "ask": "#ffb74d",
    "total": "#ab47bc",
    "delta": "#66bb6a",
    "b": "#fbc02d",
    "c": "#ec407a",
    "b_area": "rgba(251, 192, 45, 0.18)",
}


class ShockViewChartError(ValueError):
    """Invalid projection or selected-area chart coordinates."""


@dataclass(frozen=True, slots=True)
class ShockViewAnchorTimes:
    """Only anchors inside the requested dataset viewport are shown."""

    b_start_utc: datetime | None
    b_end_utc_exclusive: datetime | None
    representative_b_utc: datetime | None
    representative_c_utc: datetime | None


def _dataset_index(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShockViewChartError(f"{name} must be an integer dataset index")

    return value


def _validate_projection(
    projection: ShockViewProjection,
) -> tuple[int, int]:
    bars = projection.bars

    if not bars:
        raise ShockViewChartError("Viewing projection must contain at least one bar")

    first_index = bars[0].first_dataset_index
    last_exclusive = bars[-1].last_dataset_index + 1

    if (
        first_index < 0
        or last_exclusive <= first_index
        or projection.source_start_utc.tzinfo is None
        or projection.source_start_utc.utcoffset() != timedelta(0)
        or projection.source_end_utc_exclusive <= projection.source_start_utc
    ):
        raise ShockViewChartError("Viewing projection has invalid source bounds")

    expected = first_index

    for bar in bars:
        if (
            bar.first_dataset_index != expected
            or bar.last_dataset_index < bar.first_dataset_index
        ):
            raise ShockViewChartError(
                "Viewing bars must preserve contiguous " "dataset indices"
            )

        expected = bar.last_dataset_index + 1

    if (
        projection.source_start_utc + timedelta(seconds=last_exclusive - first_index)
        != projection.source_end_utc_exclusive
    ):
        raise ShockViewChartError(
            "Viewing projection source duration does not match " "its dataset indices"
        )

    return first_index, last_exclusive


def project_shock_view_anchor_times(
    projection: ShockViewProjection,
    *,
    b_first_dataset_index: int,
    b_last_dataset_index: int,
    representative_b_dataset_index: int,
    representative_c_dataset_index: int | None = None,
) -> ShockViewAnchorTimes:
    """Clip B to the actual source viewport without rounding to bars.

    B's source indices are inclusive. The visual interval is half-open
    so a one-second B area has one full second of width. A wholly
    offscreen B area produces no annotation.
    """
    first_index, last_exclusive = _validate_projection(projection)

    b_first = _dataset_index(
        b_first_dataset_index,
        "b_first_dataset_index",
    )
    b_last = _dataset_index(
        b_last_dataset_index,
        "b_last_dataset_index",
    )
    representative_b = _dataset_index(
        representative_b_dataset_index,
        "representative_b_dataset_index",
    )

    if b_first < 0 or b_last < b_first:
        raise ShockViewChartError(
            "B area must have ordered, nonnegative dataset indices"
        )

    if not b_first <= representative_b <= b_last:
        raise ShockViewChartError("Representative B must belong to the B area")

    if representative_c_dataset_index is not None:
        representative_c = _dataset_index(
            representative_c_dataset_index,
            "representative_c_dataset_index",
        )

        if representative_c < 0:
            raise ShockViewChartError("Representative C index cannot be negative")
    else:
        representative_c = None

    visible_b_first = max(b_first, first_index)
    visible_b_last_exclusive = min(
        b_last + 1,
        last_exclusive,
    )

    if visible_b_first >= visible_b_last_exclusive:
        return ShockViewAnchorTimes(
            b_start_utc=None,
            b_end_utc_exclusive=None,
            representative_b_utc=None,
            representative_c_utc=None,
        )

    def timestamp_at(index: int) -> datetime:
        return projection.source_start_utc + timedelta(seconds=index - first_index)

    visible_b = (
        timestamp_at(representative_b)
        if first_index <= representative_b < last_exclusive
        else None
    )
    visible_c = (
        timestamp_at(representative_c)
        if representative_c is not None
        and first_index <= representative_c < last_exclusive
        else None
    )

    return ShockViewAnchorTimes(
        b_start_utc=timestamp_at(visible_b_first),
        b_end_utc_exclusive=timestamp_at(visible_b_last_exclusive),
        representative_b_utc=visible_b,
        representative_c_utc=visible_c,
    )


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _candlestick_value(
    ohlc: L2Ohlc | None,
) -> list[float | None]:
    """ECharts order is open, close, low, high."""
    if ohlc is None:
        return [None, None, None, None]

    opened, high, low, closed = ohlc
    return [opened, closed, low, high]


def _b_band(
    anchors: ShockViewAnchorTimes,
) -> dict[str, Any]:
    data: list[list[dict[str, str]]] = []

    if anchors.b_start_utc is not None and anchors.b_end_utc_exclusive is not None:
        data.append(
            [
                {
                    "name": "L2 B candidate interval",
                    "xAxis": _iso(anchors.b_start_utc),
                },
                {
                    "xAxis": _iso(anchors.b_end_utc_exclusive),
                },
            ]
        )

    return {
        "silent": True,
        "animation": False,
        "label": {"show": False},
        "itemStyle": {"color": _COLORS["b_area"]},
        "data": data,
    }


def _anchor_lines(
    anchors: ShockViewAnchorTimes,
    *,
    include_c: bool,
) -> dict[str, Any]:
    data: list[dict[str, Any]] = []

    if anchors.representative_b_utc is not None:
        data.append(
            {
                "name": "Representative B",
                "xAxis": _iso(anchors.representative_b_utc),
                "lineStyle": {
                    "color": _COLORS["b"],
                    "type": "dashed",
                    "width": 2,
                },
            }
        )

    if include_c and anchors.representative_c_utc is not None:
        data.append(
            {
                "name": "Representative C",
                "xAxis": _iso(anchors.representative_c_utc),
                "lineStyle": {
                    "color": _COLORS["c"],
                    "type": "solid",
                    "width": 2,
                },
            }
        )

    return {
        "silent": True,
        "animation": False,
        "symbol": ["none", "none"],
        "label": {"show": False},
        "data": data,
    }


def build_shock_view_chart_options(
    projection: ShockViewProjection,
    *,
    b_first_dataset_index: int,
    b_last_dataset_index: int,
    representative_b_dataset_index: int,
    representative_c_dataset_index: int | None = None,
    price_candles: Sequence[Sequence[float] | None] | None = None,
) -> dict[str, Any]:
    """Render bounded viewing candles with exact one-second anchors.

    This is an option builder, not a data loader. It neither fetches
    price nor changes the one-second detector's B coordinates.
    """
    anchors = project_shock_view_anchor_times(
        projection,
        b_first_dataset_index=b_first_dataset_index,
        b_last_dataset_index=b_last_dataset_index,
        representative_b_dataset_index=(representative_b_dataset_index),
        representative_c_dataset_index=(representative_c_dataset_index),
    )

    source_start = _iso(projection.source_start_utc)
    source_end = _iso(projection.source_end_utc_exclusive)

    panel_names = (
        "Price",
        "Bid",
        "Ask",
        "Total",
        "Delta",
    )
    channel_names = (
        "bid",
        "ask",
        "total",
        "delta",
    )

    grid = []
    x_axes = []
    y_axes = []

    for panel_index, name in enumerate(panel_names):
        grid.append(
            {
                "top": f"{4 + panel_index * 19}%",
                "height": "15%",
                "left": "9%",
                "right": "3%",
            }
        )
        x_axes.append(
            {
                "type": "time",
                "gridIndex": panel_index,
                "min": source_start,
                "max": source_end,
                "axisLabel": {
                    "show": panel_index == 4,
                },
                "axisTick": {
                    "show": panel_index == 4,
                },
            }
        )
        y_axes.append(
            {
                "type": "value",
                "gridIndex": panel_index,
                "scale": True,
                "name": name,
                "nameLocation": "middle",
                "nameGap": 46,
                "splitLine": {"show": False},
            }
        )

    price_data: list[list[str | float | None]] = []

    if price_candles is not None:
        if len(price_candles) != len(projection.bars):
            raise ShockViewChartError(
                "Price candle count must match the viewing-bar count"
            )

        for bar, candle in zip(projection.bars, price_candles):
            timestamp = _iso(bar.start_utc)

            if candle is None:
                price_data.append([timestamp, None, None, None, None])
                continue

            if len(candle) != 4 or any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                for value in candle
            ):
                raise ShockViewChartError(
                    "Price candle must contain four finite ECharts OHLC values"
                )

            opened, closed, low, high = candle

            if low > min(opened, closed) or high < max(opened, closed):
                raise ShockViewChartError("Price candle OHLC bounds are inconsistent")

            price_data.append([timestamp, opened, closed, low, high])

    series: list[dict[str, Any]] = [
        {
            "id": "shock-view-price-context",
            "name": (
                "Price (optional context)"
                if price_candles is not None
                else "Price (optional; not loaded)"
            ),
            "type": "candlestick",
            "xAxisIndex": 0,
            "yAxisIndex": 0,
            "data": price_data,
            "itemStyle": {
                "color": "#26a69a",
                "color0": "#ef5350",
                "borderColor": "#26a69a",
                "borderColor0": "#ef5350",
            },
            "markArea": _b_band(anchors),
            "markLine": _anchor_lines(
                anchors,
                include_c=False,
            ),
        }
    ]

    for panel_index, channel in enumerate(
        channel_names,
        start=1,
    ):
        data = []

        for bar in projection.bars:
            candle = _candlestick_value(getattr(bar, channel))
            data.append(
                [
                    _iso(bar.start_utc),
                    *candle,
                ]
            )

        series.append(
            {
                "id": f"shock-view-{channel}",
                "name": channel.capitalize(),
                "type": "candlestick",
                "xAxisIndex": panel_index,
                "yAxisIndex": panel_index,
                "data": data,
                "itemStyle": {
                    "color": _COLORS[channel],
                    "color0": _COLORS[channel],
                    "borderColor": _COLORS[channel],
                    "borderColor0": _COLORS[channel],
                },
                "markArea": _b_band(anchors),
                "markLine": _anchor_lines(
                    anchors,
                    include_c=(channel == "total"),
                ),
            }
        )

    return {
        "animation": False,
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross"},
        },
        "axisPointer": {
            "link": [{"xAxisIndex": [0, 1, 2, 3, 4]}],
        },
        "grid": grid,
        "xAxis": x_axes,
        "yAxis": y_axes,
        "dataZoom": [
            {
                "type": "inside",
                "xAxisIndex": [0, 1, 2, 3, 4],
                "filterMode": "none",
            }
        ],
        "series": series,
    }

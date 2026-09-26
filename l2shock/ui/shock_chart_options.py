# l2shock/ui/shock_chart_options.py
"""Five-panel ECharts options for inspecting one Shock-Start B area.

This is a pure option builder. It does not publish to NiceGUI, alter the
existing Analysis chart controller, or decide whether an L2 event is valid.
The verified one-second ShockAreaWindow owns the shared UTC axis.

Price candles are optional visual context. They are never used to select,
filter, or shorten the L2 window.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import TypeAlias
from collections.abc import Mapping

from l2shock.analysis.shock_window import ShockAreaWindow

# Input is (open, high, low, close). ECharts candlestick data uses
# [open, close, low, high]; the conversion is made explicitly below.
PriceCandle: TypeAlias = tuple[float, float, float, float]
PriceBySecond: TypeAlias = Mapping[datetime, PriceCandle]

_PANEL_NAMES = ("Price", "Bid", "Ask", "Total", "Delta")
_L2_NAMES = ("Bid", "Ask", "Total", "Delta")

_COLORS = {
    "price_up": "#26a69a",
    "price_down": "#ef5350",
    "bid": "#42a5f5",
    "ask": "#ffb74d",
    "total": "#ab47bc",
    "delta": "#66bb6a",
    "b": "#fbc02d",
    "c": "#ec407a",
    "ab": "rgba(90, 120, 160, 0.08)",
    "bc": "rgba(171, 71, 188, 0.08)",
    "b_area": "rgba(251, 192, 45, 0.18)",
}

from types import MappingProxyType as _MappingProxyType  # noqa: E402

# Read-only public view: the Shock-Start legend derives its swatches from
# exactly the colors the chart uses.
SHOCK_CHART_COLORS = _MappingProxyType(_COLORS)


class ShockChartOptionsError(ValueError):
    """The supplied window cannot be represented as a shock review chart."""


def _finite_number(value: object, description: str) -> float:
    if isinstance(value, bool):
        raise ShockChartOptionsError(f"{description} must be finite numeric data")

    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ShockChartOptionsError(
            f"{description} must be finite numeric data"
        ) from exc

    if not math.isfinite(number):
        raise ShockChartOptionsError(f"{description} must be finite numeric data")

    return number


def _validate_window(window: ShockAreaWindow) -> None:
    if not isinstance(window, ShockAreaWindow):
        raise TypeError("window must be ShockAreaWindow")

    count = len(window.seconds)

    if count < 3:
        raise ShockChartOptionsError(
            "A shock inspection chart requires at least three seconds"
        )

    if window.last_dataset_index - window.first_dataset_index + 1 != count:
        raise ShockChartOptionsError("Shock window index span is inconsistent")

    positions = (
        window.representative_a_position,
        window.b_first_position,
        window.representative_b_position,
        window.b_last_position,
        window.representative_c_position,
    )

    if not all(0 <= position < count for position in positions):
        raise ShockChartOptionsError("A/B/C anchor is outside the window")

    if not (
        window.representative_a_position
        < window.representative_b_position
        < window.representative_c_position
    ):
        raise ShockChartOptionsError("Representative A/B/C order is invalid")

    if not (
        window.b_first_position
        <= window.representative_b_position
        <= window.b_last_position
        < window.representative_c_position
    ):
        raise ShockChartOptionsError("B-area interval is inconsistent")

    for position, second in enumerate(window.seconds):
        if second.dataset_index != window.first_dataset_index + position:
            raise ShockChartOptionsError(
                "Shock window does not have a contiguous dataset axis"
            )

        if second.timestamp_utc.tzinfo is None:
            raise ShockChartOptionsError("Shock window contains a naive UTC timestamp")

        values = (second.bid, second.ask, second.total, second.delta)

        if second.valid_l2:
            if any(value is None for value in values):
                raise ShockChartOptionsError(
                    "Valid L2 second is missing a plotted channel"
                )

            if (
                second.total != second.bid + second.ask
                or second.delta != second.bid - second.ask
            ):
                raise ShockChartOptionsError(
                    "Total or Delta disagrees with Bid and Ask"
                )
        elif any(value is not None for value in values):
            raise ShockChartOptionsError(
                "Invalid L2 second must be null in every L2 channel"
            )


def _price_data(
    window: ShockAreaWindow,
    price_by_second: PriceBySecond | None,
) -> list[list[float] | None]:
    prices = {} if price_by_second is None else price_by_second
    result: list[list[float] | None] = []

    for second in window.seconds:
        candle = prices.get(second.timestamp_utc)

        if candle is None:
            result.append(None)
            continue

        if len(candle) != 4:
            raise ShockChartOptionsError(
                "Price candle must be (open, high, low, close)"
            )

        opening, high, low, close = (
            _finite_number(value, "Price candle value") for value in candle
        )

        if low > min(opening, close) or high < max(opening, close):
            raise ShockChartOptionsError("Price candle OHLC bounds are inconsistent")

        result.append([opening, close, low, high])

    return result


def _b_band(
    axis: list[str],
    window: ShockAreaWindow,
) -> dict[str, object]:
    # The right-hand coordinate is *exclusive*. Using the following axis
    # second makes even a one-second B area visibly wide in ECharts.
    end_exclusive = window.b_last_position + 1

    if end_exclusive >= len(axis):
        raise ShockChartOptionsError(
            "B area needs a following chart second for its visible band"
        )

    return {
        "silent": True,
        "animation": False,
        "label": {"show": False},
        "itemStyle": {"color": _COLORS["b_area"]},
        "data": [
            [
                {"name": "B start area", "xAxis": axis[window.b_first_position]},
                {"xAxis": axis[end_exclusive]},
            ]
        ],
    }


def _b_line(axis: list[str], position: int) -> dict[str, object]:
    return {
        "silent": True,
        "animation": False,
        "symbol": ["none", "none"],
        "label": {"show": False},
        "lineStyle": {
            "color": _COLORS["b"],
            "type": "dashed",
            "width": 2,
        },
        "data": [
            {
                "name": "Representative B",
                "xAxis": axis[position],
            }
        ],
    }


def _c_line(axis: list[str], position: int) -> dict[str, object]:
    return {
        "silent": True,
        "animation": False,
        "symbol": ["none", "none"],
        "label": {"show": False},
        "lineStyle": {
            "color": _COLORS["c"],
            "type": "solid",
            "width": 2,
        },
        "data": [
            {
                "name": "Representative C",
                "xAxis": axis[position],
            }
        ],
    }


def build_shock_chart_options(
    window: ShockAreaWindow,
    *,
    price_by_second: PriceBySecond | None = None,
) -> dict[str, object]:
    """Build five aligned panels; Price is optional and never an L2 gate.

    The returned dict contains ordinary JSON-compatible ECharts options.
    Publication, viewport restoration, crosshair installation, and exports
    remain responsibilities of the existing UI chart controller.
    """
    _validate_window(window)

    axis = [
        second.timestamp_utc.astimezone(timezone.utc).isoformat()
        for second in window.seconds
    ]

    price = _price_data(window, price_by_second)

    # The axis is never compressed to the available price candles.
    x_axes: list[dict[str, object]] = []
    y_axes: list[dict[str, object]] = []
    grids: list[dict[str, object]] = []

    for panel_index, panel_name in enumerate(_PANEL_NAMES):
        grids.append(
            {
                "top": f"{5 + panel_index * 18}%",
                "height": "14%",
                "left": 92,
                "right": 24,
                "containLabel": False,
            }
        )
        x_axes.append(
            {
                "type": "category",
                "gridIndex": panel_index,
                "data": axis,
                "boundaryGap": True,
                "axisLabel": {"show": panel_index == 4},
                "axisTick": {"show": panel_index == 4},
            }
        )
        y_axes.append(
            {
                "type": "value",
                "gridIndex": panel_index,
                "scale": True,
                "name": panel_name,
                "nameLocation": "middle",
                "nameGap": 65,
                "splitLine": {"show": False},
            }
        )

    series: list[dict[str, object]] = [
        {
            "id": "shock-price-context",
            "name": "Price (optional context)",
            "type": "candlestick",
            "xAxisIndex": 0,
            "yAxisIndex": 0,
            "data": price,
            "itemStyle": {
                "color": _COLORS["price_up"],
                "color0": _COLORS["price_down"],
                "borderColor": _COLORS["price_up"],
                "borderColor0": _COLORS["price_down"],
            },
            # These coordinates come from the selected L2 B area, not from
            # price candles. Missing price never moves or removes the interval.
            "markArea": _b_band(axis, window),
            "markLine": _b_line(axis, window.representative_b_position),
        }
    ]

    for panel_index, name in enumerate(_L2_NAMES, start=1):
        values = [getattr(second, name.lower()) for second in window.seconds]

        series.append(
            {
                "id": f"shock-{name.lower()}",
                "name": name,
                "type": "line",
                "xAxisIndex": panel_index,
                "yAxisIndex": panel_index,
                # ECharts interprets null as a gap, not as zero liquidity.
                "data": [
                    (
                        _finite_number(value, f"{name} L2 value")
                        if value is not None
                        else None
                    )
                    for value in values
                ],
                "showSymbol": False,
                "connectNulls": False,
                "animation": False,
                "lineStyle": {
                    "color": _COLORS[name.lower()],
                    "width": 1.5,
                },
                "itemStyle": {"color": _COLORS[name.lower()]},
                "markArea": _b_band(axis, window),
                "markLine": (
                    _c_line(axis, window.representative_c_position)
                    if name == "Total"
                    else _b_line(axis, window.representative_b_position)
                ),
            }
        )

    # Total needs both B and C. ECharts takes one markLine definition per
    # series, so extend its data rather than replacing the C marker.
    total_series = series[3]

    b_marker = _b_line(
        axis,
        window.representative_b_position,
    )[
        "data"
    ][0]
    b_marker["lineStyle"] = {
        "color": _COLORS["b"],
        "type": "dashed",
        "width": 2,
    }

    c_marker = total_series["markLine"]["data"][0]
    c_marker["lineStyle"] = {
        "color": _COLORS["c"],
        "type": "solid",
        "width": 2,
    }

    total_series["markLine"]["data"].insert(0, b_marker)

    return {
        "animation": False,
        "backgroundColor": "#ffffff",
        "legend": {
            "top": 43,
            "data": [
                "Price (optional context)",
                "Bid",
                "Ask",
                "Total",
                "Delta",
            ],
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross"},
        },
        "axisPointer": {
            "link": [{"xAxisIndex": "all"}],
        },
        "grid": grids,
        "xAxis": x_axes,
        "yAxis": y_axes,
        "dataZoom": [
            {
                "type": "inside",
                "xAxisIndex": [0, 1, 2, 3, 4],
                "filterMode": "none",
            },
            {
                "type": "slider",
                "xAxisIndex": [0, 1, 2, 3, 4],
                "filterMode": "none",
                "bottom": "1%",
            },
        ],
        "series": series,
    }


__all__ = [
    "PriceBySecond",
    "PriceCandle",
    "ShockChartOptionsError",
    "build_shock_chart_options",
]

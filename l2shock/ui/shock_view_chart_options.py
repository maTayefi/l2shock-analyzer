# l2shock/ui/shock_view_chart_options.py
"""Five-panel ECharts options for a bounded Shock L2 viewing projection.

The source projection contains viewing bars; its selected B interval and
representative anchors are specified in absolute one-second dataset
indices. Annotations use exact UTC source timestamps, never rounded
viewing-bar boundaries.

Optional Top-N ranked areas are presentation only. They never change the
review, its inspection order, or detection coordinates. The selected area
is always drawn first (yellow) so existing consumers of markArea.data[0]
and markLine.data[0] keep their meaning.

Price is optional visual context and is not loaded by this builder.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from l2shock.ui.shock_chart_options import (
    MAX_SHOCK_CHART_TOP_N,
    SHOCK_CHART_COLORS,
    SHOCK_RANK_COLOR_KEYS,
)
from l2shock.ui.shock_view_bars import (
    L2Ohlc,
    ShockViewProjection,
)

_ONE_SECOND = timedelta(seconds=1)

# Single color source shared with the Shock-Start legend.
_COLORS = SHOCK_CHART_COLORS

_RANK_BAND_ALPHA = 0.12


class ShockViewChartError(ValueError):
    """Invalid projection or selected-area chart coordinates."""


@dataclass(frozen=True, slots=True)
class ShockViewAnchorTimes:
    """Only anchors inside the requested dataset viewport are shown."""

    b_start_utc: datetime | None
    b_end_utc_exclusive: datetime | None
    representative_b_utc: datetime | None
    representative_c_utc: datetime | None


@dataclass(frozen=True, slots=True)
class ShockViewRankedArea:
    """One non-selected Top-N B area, identified by inspection position."""

    rank: int
    b_first_dataset_index: int
    b_last_dataset_index: int
    representative_b_dataset_index: int
    representative_c_dataset_index: int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.rank, bool) or not isinstance(self.rank, int):
            raise ShockViewChartError("rank must be a positive integer")

        if self.rank < 1:
            raise ShockViewChartError("rank must be a positive integer")


def rank_color_key(rank: int) -> str:
    """Return the SHOCK_CHART_COLORS key for one inspection rank."""
    if isinstance(rank, bool) or not isinstance(rank, int) or rank < 1:
        raise ShockViewChartError("rank must be a positive integer")

    return SHOCK_RANK_COLOR_KEYS[(rank - 1) % len(SHOCK_RANK_COLOR_KEYS)]


def _rank_band_color(hex_color: str) -> str:
    text = str(hex_color).lstrip("#")

    if len(text) != 6:
        raise ShockViewChartError("Rank colors must be #rrggbb")

    try:
        red, green, blue = (int(text[i : i + 2], 16) for i in (0, 2, 4))
    except ValueError as exc:
        raise ShockViewChartError("Rank colors must be #rrggbb") from exc

    return f"rgba({red}, {green}, {blue}, {_RANK_BAND_ALPHA})"


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


# (rank, anchors, line color) for one visible non-selected Top-N area.
_RankedAnchors = tuple[int, ShockViewAnchorTimes, str]


def _band_item(
    anchors: ShockViewAnchorTimes,
    *,
    name: str,
    fill: str | None = None,
) -> list[dict[str, Any]] | None:
    if anchors.b_start_utc is None or anchors.b_end_utc_exclusive is None:
        return None

    first: dict[str, Any] = {
        "name": name,
        "xAxis": _iso(anchors.b_start_utc),
    }

    if fill is not None:
        first["itemStyle"] = {"color": fill}

    return [first, {"xAxis": _iso(anchors.b_end_utc_exclusive)}]


def _b_band(
    anchors: ShockViewAnchorTimes,
    ranked: Sequence[_RankedAnchors] = (),
) -> dict[str, Any]:
    data: list[list[dict[str, Any]]] = []

    selected = _band_item(anchors, name="L2 B candidate interval")

    if selected is not None:
        data.append(selected)

    for rank, rank_anchors, color in ranked:
        item = _band_item(
            rank_anchors,
            name=f"Rank #{rank} B area",
            fill=_rank_band_color(color),
        )

        if item is not None:
            data.append(item)

    return {
        "silent": True,
        "animation": False,
        "label": {"show": False},
        "itemStyle": {"color": _COLORS["b_area"]},
        "data": data,
    }


def _line_item(
    at: datetime,
    *,
    name: str,
    color: str,
    line_type: str,
    label: str | None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "name": name,
        "xAxis": _iso(at),
        "lineStyle": {
            "color": color,
            "type": line_type,
            "width": 2,
        },
    }

    if label:
        # A plain string formatter; it contains no "{...}" template.
        item["label"] = {
            "show": True,
            "formatter": label,
            "position": "end",
            "color": color,
            "fontSize": 11,
            "fontWeight": "bold",
        }

    return item


def _anchor_lines(
    anchors: ShockViewAnchorTimes,
    *,
    include_c: bool,
    selected_label: str | None = None,
    ranked: Sequence[_RankedAnchors] = (),
) -> dict[str, Any]:
    data: list[dict[str, Any]] = []

    # Selected area first: markLine.data[0] is its B, data[1] its C (Total).
    if anchors.representative_b_utc is not None:
        data.append(
            _line_item(
                anchors.representative_b_utc,
                name="Representative B",
                color=_COLORS["b"],
                line_type="dashed",
                label=selected_label,
            )
        )

    if include_c and anchors.representative_c_utc is not None:
        data.append(
            _line_item(
                anchors.representative_c_utc,
                name="Representative C",
                color=_COLORS["c"],
                line_type="solid",
                label=(f"{selected_label} C" if selected_label else None),
            )
        )

    for rank, rank_anchors, color in ranked:
        if rank_anchors.representative_b_utc is not None:
            data.append(
                _line_item(
                    rank_anchors.representative_b_utc,
                    name=f"Rank #{rank} B",
                    color=color,
                    line_type="dashed",
                    label=f"#{rank}",
                )
            )

        if include_c and rank_anchors.representative_c_utc is not None:
            data.append(
                _line_item(
                    rank_anchors.representative_c_utc,
                    name=f"Rank #{rank} C",
                    color=color,
                    line_type="solid",
                    label=f"#{rank} C",
                )
            )

    return {
        "silent": True,
        "animation": False,
        "symbol": ["none", "none"],
        "label": {"show": False},
        "data": data,
    }


def _visible_ranked_anchors(
    projection: ShockViewProjection,
    ranked_areas: Sequence[ShockViewRankedArea],
    *,
    selected_rank: int | None,
) -> list[_RankedAnchors]:
    areas = tuple(ranked_areas)

    if len(areas) > MAX_SHOCK_CHART_TOP_N:
        raise ShockViewChartError(
            f"At most {MAX_SHOCK_CHART_TOP_N} ranked areas can be drawn"
        )

    seen: set[int] = set()
    result: list[_RankedAnchors] = []

    for area in areas:
        if not isinstance(area, ShockViewRankedArea):
            raise ShockViewChartError(
                "ranked_areas must contain ShockViewRankedArea objects"
            )

        if area.rank in seen or area.rank == selected_rank:
            raise ShockViewChartError(
                "Ranked areas must have unique ranks distinct from the " "selected area"
            )

        seen.add(area.rank)

        anchors = project_shock_view_anchor_times(
            projection,
            b_first_dataset_index=area.b_first_dataset_index,
            b_last_dataset_index=area.b_last_dataset_index,
            representative_b_dataset_index=area.representative_b_dataset_index,
            representative_c_dataset_index=area.representative_c_dataset_index,
        )

        if anchors.b_start_utc is None:
            continue  # Wholly outside this viewport.

        result.append((area.rank, anchors, _COLORS[rank_color_key(area.rank)]))

    return result


def build_shock_view_chart_options(
    projection: ShockViewProjection,
    *,
    b_first_dataset_index: int,
    b_last_dataset_index: int,
    representative_b_dataset_index: int,
    representative_c_dataset_index: int | None = None,
    price_candles: Sequence[Sequence[float] | None] | None = None,
    ranked_areas: Sequence[ShockViewRankedArea] = (),
    selected_rank: int | None = None,
) -> dict[str, Any]:
    """Render bounded viewing candles with exact one-second anchors.

    This is an option builder, not a data loader. It neither fetches
    price nor changes the one-second detector's B coordinates.
    """
    if selected_rank is not None and (
        isinstance(selected_rank, bool)
        or not isinstance(selected_rank, int)
        or selected_rank < 1
    ):
        raise ShockViewChartError("selected_rank must be a positive integer or None")

    anchors = project_shock_view_anchor_times(
        projection,
        b_first_dataset_index=b_first_dataset_index,
        b_last_dataset_index=b_last_dataset_index,
        representative_b_dataset_index=(representative_b_dataset_index),
        representative_c_dataset_index=(representative_c_dataset_index),
    )
    ranked = _visible_ranked_anchors(
        projection,
        ranked_areas,
        selected_rank=selected_rank,
    )
    selected_label = f"#{selected_rank}" if selected_rank is not None else None

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
                "color": _COLORS["price_up"],
                "color0": _COLORS["price_down"],
                "borderColor": _COLORS["price_up"],
                "borderColor0": _COLORS["price_down"],
            },
            "markArea": _b_band(anchors, ranked),
            "markLine": _anchor_lines(
                anchors,
                include_c=False,
                selected_label=selected_label,
                ranked=ranked,
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
                "markArea": _b_band(anchors, ranked),
                "markLine": _anchor_lines(
                    anchors,
                    include_c=(channel == "total"),
                    selected_label=selected_label,
                    ranked=ranked,
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


__all__ = [
    "MAX_SHOCK_CHART_TOP_N",
    "ShockViewAnchorTimes",
    "ShockViewChartError",
    "ShockViewRankedArea",
    "build_shock_view_chart_options",
    "project_shock_view_anchor_times",
    "rank_color_key",
]

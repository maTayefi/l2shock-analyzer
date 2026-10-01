# l2shock/ui/l2_view_presentation.py
"""Presentation helpers for the detector-free Analysis tab."""

from __future__ import annotations

import copy
import csv
import io
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from typing import Any

from l2shock.analysis.l2_view_stream import (
    L2ViewError,
    L2ViewProjection,
    coarsen_l2_view,
    select_l2_view_timeframe,
)


def whole_number(
    value: object,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    """Parse a bounded integer without truncating browser floats."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a whole number")

    if isinstance(value, int):
        result = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{name} must be a whole number")
        result = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text or not text.isascii() or not text.isdecimal():
            raise ValueError(f"{name} must be a whole number")
        result = int(text)
    else:
        raise ValueError(f"{name} must be a whole number")

    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum:,} and {maximum:,}")
    return result


def fit_closed_handoff(
    start_utc: datetime,
    closed_end_utc: datetime,
    *,
    max_duration_seconds: int,
) -> tuple[datetime, datetime, bool]:
    """Keep the newest whole-second slots within the user's duration limit."""
    for value in (start_utc, closed_end_utc):
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError("Calendar endpoints must be timezone-aware")

    original_start = start_utc.astimezone(timezone.utc)
    original_end = closed_end_utc.astimezone(timezone.utc)
    if original_end < original_start:
        raise ValueError("Calendar end precedes its start")

    if (
        isinstance(max_duration_seconds, bool)
        or not isinstance(max_duration_seconds, int)
        or max_duration_seconds <= 0
    ):
        raise ValueError("Duration limit must be a positive integer")

    start = original_start.replace(microsecond=0)
    end = original_end.replace(microsecond=0)
    slots = int((end - start).total_seconds()) + 1
    if slots <= max_duration_seconds:
        return start, end, False

    return (
        end - timedelta(seconds=max_duration_seconds - 1),
        end,
        True,
    )


def cached_view(
    loaded: L2ViewProjection,
    *,
    timeframe_seconds: int | None,
    max_bars: int,
) -> tuple[int, L2ViewProjection | None]:
    """Return a cache-derived view, or None when a source reload is required."""
    if not isinstance(loaded, L2ViewProjection):
        raise TypeError("loaded must be L2ViewProjection")

    target = select_l2_view_timeframe(
        loaded.start_utc,
        loaded.end_utc_exclusive,
        timeframe_seconds=timeframe_seconds,
        max_bars=max_bars,
    )
    base = loaded.timeframe_seconds
    if target < base or target % base:
        return target, None

    # coarsen_l2_view returns its input for an unchanged timeframe.
    # Replace the budget first so that path does not retain an old budget.
    source = replace(loaded, max_bars=max_bars)
    return target, coarsen_l2_view(
        source,
        target,
        max_bars=max_bars,
    )


def with_wheel_policy(
    option: dict[str, Any],
    *,
    preserved_y: dict[int, tuple[float, float]] | None = None,
    reset_panels: frozenset[int] = frozenset(range(5)),
) -> dict[str, Any]:
    """Use native X wheel and explicitly routed Shift+wheel for Y."""
    result = copy.deepcopy(option)
    zooms = result.get("dataZoom")
    if not isinstance(zooms, list) or len(zooms) != 6:
        raise L2ViewError("Analysis chart must have one X and five Y zooms")

    zooms[0]["zoomOnMouseWheel"] = True

    for panel in range(5):
        zoom = zooms[panel + 1]
        # ECharts' native X zoom also accepts Shift+wheel. A modifier on
        # Y alone would therefore zoom both axes. Route Shift explicitly.
        zoom["zoomOnMouseWheel"] = False
        zoom["moveOnMouseWheel"] = False
        zoom["moveOnMouseMove"] = False
        zoom["start"] = 0.0
        zoom["end"] = 100.0

        if panel in reset_panels or not preserved_y:
            continue
        values = preserved_y.get(panel)
        if values is None:
            continue
        start, end = values
        if 0.0 <= start < end <= 100.0:
            zoom["start"] = start
            zoom["end"] = end

    return result


def _javascript_target(chart: Any) -> tuple[Any, int] | None:
    element_id = getattr(chart, "id", None)
    if (
        isinstance(element_id, bool)
        or not isinstance(element_id, int)
        or element_id < 0
    ):
        return None
    try:
        runner = getattr(chart.client, "run_javascript", None)
    except Exception:
        return None
    return (runner, element_id) if callable(runner) else None


_CAPTURE_Y_JS = r"""
(function () {
    const chartId = __CHART_ID__;
    const expectedToken = __TOKEN__;
    const prefix = "__l2shock_render_token__:";
    let instance = null;
    try {
        const component = getElement(chartId);
        instance = component && component.chart;
    } catch (error) {}
    if (!instance) return null;

    const option = instance.getOption();
    if (!(option.series || []).some(
        item => item && item.name === prefix + expectedToken
    )) return null;

    const zooms = option.dataZoom || [];
    return [1, 2, 3, 4, 5].map(function (index) {
        const item = zooms[index];
        if (!item) return null;
        return [item.start, item.end];
    });
})()
"""


async def capture_y_viewports(
    chart: Any,
    *,
    expected_render_token: str,
) -> dict[int, tuple[float, float]]:
    target = _javascript_target(chart)
    if target is None:
        return {}
    runner, element_id = target
    code = _CAPTURE_Y_JS.replace("__CHART_ID__", json.dumps(element_id)).replace(
        "__TOKEN__", json.dumps(expected_render_token)
    )
    try:
        raw = await runner(code, timeout=2.0)
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, list):
            return {}
        result = {}
        for panel, values in enumerate(raw[:5]):
            if not isinstance(values, list) or len(values) != 2:
                continue
            start, end = float(values[0]), float(values[1])
            if 0.0 <= start < end <= 100.0:
                result[panel] = (start, end)
        return result
    except Exception:
        return {}


_INSTALL_Y_JS = r"""
(function () {
    const chartId = __CHART_ID__;
    const expectedToken = __TOKEN__;
    const prefix = "__l2shock_render_token__:";
    const registry = (
        window.__l2shockYWheel
        || (window.__l2shockYWheel = {})
    );
    const key = String(chartId);

    let component = null;
    try {
        component = getElement(chartId);
    } catch (error) {}
    const instance = component && component.chart;
    const root = (
        component && component.$el
        || document.getElementById("c" + key)
    );
    if (!instance || !root || !root.addEventListener) return false;

    const previous = registry[key];
    if (previous) {
        previous.root.removeEventListener(
            "wheel", previous.handler, true
        );
    }

    function owns() {
        try {
            const option = instance.getOption();
            return (option.series || []).some(
                item => item && item.name === prefix + expectedToken
            );
        } catch (error) {
            return false;
        }
    }

    function handler(event) {
        if (!event.shiftKey || event.ctrlKey || event.altKey || event.metaKey) {
            return;
        }
        if (!owns()) return;

        const dom = instance.getDom();
        const bounds = dom.getBoundingClientRect();
        const width = dom.clientWidth;
        const height = dom.clientHeight;
        if (!(bounds.width > 0 && bounds.height > 0)) return;

        const x = (event.clientX - bounds.left) * width / bounds.width;
        const y = (event.clientY - bounds.top) * height / bounds.height;
        let panel = -1;
        let rectangle = null;

        for (let index = 0; index < 5; index += 1) {
            const grid = instance.getModel().getComponent("grid", index);
            const coordinates = grid && grid.coordinateSystem;
            const rect = coordinates && coordinates.getRect();
            if (
                rect
                && x >= rect.x && x <= rect.x + rect.width
                && y >= rect.y && y <= rect.y + rect.height
            ) {
                panel = index;
                rectangle = rect;
                break;
            }
        }
        if (panel < 0 || !rectangle) return;

        // Prevent the same Shift+wheel from reaching native X zoom.
        event.preventDefault();
        event.stopImmediatePropagation();

        const option = instance.getOption();
        const zoom = (option.dataZoom || [])[panel + 1] || {};
        const start = Number.isFinite(zoom.start) ? zoom.start : 0;
        const end = Number.isFinite(zoom.end) ? zoom.end : 100;
        const span = end - start;
        if (!(span > 0)) return;

        let delta = Number(event.deltaY);
        if (!Number.isFinite(delta) || delta === 0) return;
        if (event.deltaMode === 1) delta *= 16;
        if (event.deltaMode === 2) delta *= height;

        const factor = Math.exp(Math.max(-0.5, Math.min(0.5, delta * 0.002)));
        const nextSpan = Math.max(0.1, Math.min(100, span * factor));
        const fraction = Math.max(
            0, Math.min(1, 1 - (y - rectangle.y) / rectangle.height)
        );
        const anchor = start + span * fraction;
        const nextStart = Math.max(
            0, Math.min(100 - nextSpan, anchor - nextSpan * fraction)
        );

        instance.dispatchAction({
            type: "dataZoom",
            dataZoomIndex: panel + 1,
            start: nextStart,
            end: nextStart + nextSpan
        });
    }

    root.addEventListener("wheel", handler, {
        capture: true,
        passive: false
    });
    registry[key] = {root: root, handler: handler};
    return true;
})()
"""


async def install_y_wheel(
    chart: Any,
    *,
    expected_render_token: str,
) -> bool:
    target = _javascript_target(chart)
    if target is None:
        return False
    runner, element_id = target
    code = _INSTALL_Y_JS.replace("__CHART_ID__", json.dumps(element_id)).replace(
        "__TOKEN__", json.dumps(expected_render_token)
    )
    result = await runner(code, timeout=3.0)
    return result is True or result == "true"


def _exact_json(value: object) -> object:
    if isinstance(value, Fraction):
        return f"{value.numerator}/{value.denominator}"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (tuple, list)):
        return [_exact_json(item) for item in value]
    if isinstance(value, dict):
        return {key: _exact_json(item) for key, item in value.items()}
    return value


def displayed_json_bytes(
    projection: L2ViewProjection,
    option: dict[str, Any],
    *,
    panel_a_metric: str,
    panel_b_metric: str,
) -> bytes:
    """Export the full displayed-bar dataset, not merely the current X zoom."""
    payload = {
        "schema": "l2shock.displayed_analysis",
        "schema_version": 1,
        "analysis_id": projection.input_id,
        "base": projection.request.base,
        "preset_hash": projection.request.preset_hash,
        "requested_start_utc": projection.request.requested_start_utc,
        "requested_end_utc": projection.request.requested_end_utc,
        "start_utc": projection.start_utc,
        "end_utc_exclusive": projection.end_utc_exclusive,
        "timeframe_seconds": projection.timeframe_seconds,
        "panel_a_metric": panel_a_metric,
        "panel_b_metric": panel_b_metric,
        "price_status": projection.price_status,
        "usable_l2_seconds": projection.usable_l2_seconds,
        "unusable_l2_seconds": projection.unusable_l2_seconds,
        "partial_market_seconds": projection.partial_market_seconds,
        "warning_regions_truncated": {
            "l2": projection.l2_regions_truncated,
            "price": projection.price_regions_truncated,
        },
        "warning_regions": [
            {
                "channel": region.channel,
                "start_utc": region.start_utc,
                "end_utc_exclusive": region.end_utc_exclusive,
            }
            for region in (projection.l2_regions + projection.price_regions)
        ],
        "bars": [
            {
                "start_utc": bar.start_utc,
                "source_seconds": bar.source_seconds,
                "valid_l2": bar.valid_l2,
                "bid": bar.bid,
                "ask": bar.ask,
                "total": bar.total,
                "delta": bar.delta,
                "bid_share_pct": bar.bid_share_pct,
                "price": bar.price,
            }
            for bar in projection.bars
        ],
        "exact_ohlc_order": ["open", "high", "low", "close"],
        "exact_fraction_encoding": "numerator/denominator",
        "panels": [
            {
                "index": panel,
                "series": [
                    {
                        "id": series["id"],
                        "name": series["name"],
                        "type": series["type"],
                        "data": series["data"],
                    }
                    for series in option["series"]
                    if series["xAxisIndex"] == panel
                ],
            }
            for panel in range(5)
        ],
        "panel_candle_data_order": ["timestamp_utc", "open", "close", "low", "high"],
    }
    return json.dumps(
        _exact_json(payload),
        ensure_ascii=False,
        allow_nan=False,
        indent=2,
    ).encode("utf-8")


def displayed_csv_bytes(
    projection: L2ViewProjection,
    option: dict[str, Any],
) -> bytes:
    """Long-form plot-coordinate export; nulls are empty cells, not zero."""
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(
        [
            "analysis_id",
            "base",
            "preset_hash",
            "timeframe_seconds",
            "panel_index",
            "series_id",
            "series_name",
            "timestamp_utc",
            "kind",
            "value",
            "open",
            "high",
            "low",
            "close",
        ]
    )

    for series in option["series"]:
        for item in series["data"]:
            if series["type"] == "candlestick":
                timestamp, opened, closed, low, high = item
                values = ["candlestick", "", opened, high, low, closed]
            else:
                timestamp, value = item
                values = ["line", value, "", "", "", ""]
            writer.writerow(
                [
                    projection.input_id,
                    projection.request.base,
                    projection.request.preset_hash,
                    projection.timeframe_seconds,
                    series["xAxisIndex"],
                    series["id"],
                    series["name"],
                    timestamp,
                    *values,
                ]
            )

    return output.getvalue().encode("utf-8-sig")


__all__ = [
    "cached_view",
    "capture_y_viewports",
    "displayed_csv_bytes",
    "displayed_json_bytes",
    "fit_closed_handoff",
    "install_y_wheel",
    "whole_number",
    "with_wheel_policy",
]

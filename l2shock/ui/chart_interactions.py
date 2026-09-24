# l2shock/ui/chart_interactions.py
"""Browser interaction ownership for the synchronized Analysis chart.

This module owns presentation-only interactions:

- publication generation ordering;
- browser render acknowledgement;
- dataZoom viewport capture and restoration;
- deterministic table-row navigation;
- custom gapped crosshair graphics.

It does not alter datasets, candidates, rankings, analysis IDs, or persistence.
"""

from __future__ import annotations

import asyncio
import bisect
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from nicegui import ui

from l2shock.ui.analysis_chart import (
    ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX,
    ChartNavigationWindow,
)
from l2shock.ui.echarts import (
    EChartPublication,
    acknowledged_render_token,
    confirm_echart_render_identity,
    set_echart_options,
)

log = logging.getLogger(__name__)

_DEFAULT_CROSSHAIR_GAP_PX: Final[int] = 50
_CROSSHAIR_COLOR: Final[str] = "#fbbf24"


class AnalysisChartInteractionError(RuntimeError):
    """Analysis chart publication or interaction ownership failed."""


@dataclass(frozen=True, slots=True)
class AnalysisChartViewport:
    """One ECharts percentage-based x-axis viewport."""

    start_percent: float
    end_percent: float

    def __post_init__(self) -> None:
        start = float(self.start_percent)
        end = float(self.end_percent)

        if not 0.0 <= start <= 100.0:
            raise AnalysisChartInteractionError(
                "Viewport start must lie inside [0, 100]"
            )

        if not 0.0 <= end <= 100.0:
            raise AnalysisChartInteractionError("Viewport end must lie inside [0, 100]")

        if end < start:
            raise AnalysisChartInteractionError("Viewport end cannot precede start")

        object.__setattr__(self, "start_percent", start)
        object.__setattr__(self, "end_percent", end)


@dataclass(frozen=True, slots=True)
class AnalysisChartTemporalViewport:
    """Timestamp-owned viewport transferable between chart timeframes.

    ``left_edge_utc`` owns the old visible left edge.

    ``visible_duration_seconds`` owns the approximate real elapsed duration
    visible in the old chart. It includes elapsed time represented by explicit
    compressed-timeline jumps; it does not pretend those gaps were continuous
    analytical data.
    """

    left_edge_utc: datetime
    visible_duration_seconds: float
    source_timeframe_seconds: int

    def __post_init__(self) -> None:
        left = self.left_edge_utc

        if (
            not isinstance(left, datetime)
            or left.tzinfo is None
            or left.utcoffset() is None
        ):
            raise AnalysisChartInteractionError(
                "Temporal viewport left edge must be timezone-aware"
            )

        left = left.astimezone(timezone.utc)
        duration = float(self.visible_duration_seconds)

        if not math.isfinite(duration) or duration <= 0.0:
            raise AnalysisChartInteractionError(
                "Temporal viewport duration must be finite and positive"
            )

        source_seconds = self.source_timeframe_seconds

        if (
            isinstance(source_seconds, bool)
            or not isinstance(source_seconds, int)
            or source_seconds <= 0
        ):
            raise AnalysisChartInteractionError(
                "source_timeframe_seconds must be a positive integer"
            )

        object.__setattr__(self, "left_edge_utc", left)
        object.__setattr__(
            self,
            "visible_duration_seconds",
            duration,
        )


@dataclass(frozen=True, slots=True)
class AnalysisChartCommit:
    """One browser-acknowledged chart owner."""

    owner_id: str
    publication: EChartPublication
    visible_category_count: int

    def __post_init__(self) -> None:
        owner = str(self.owner_id or "").strip()

        if not owner:
            raise AnalysisChartInteractionError("Chart commit owner_id cannot be blank")

        if not isinstance(self.publication, EChartPublication):
            raise TypeError("publication must be EChartPublication")

        count = self.visible_category_count

        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise AnalysisChartInteractionError(
                "visible_category_count must be non-negative"
            )

        object.__setattr__(self, "owner_id", owner)


def _decoded_json(value: object) -> object:
    if not isinstance(value, str):
        return value

    try:
        return json.loads(value)
    except TypeError, ValueError, json.JSONDecodeError:
        return value


def _first_data_zoom(
    option: object,
) -> dict[str, object] | None:
    if not isinstance(option, dict):
        return None

    raw = option.get("dataZoom")
    values = raw if isinstance(raw, list) else [raw]

    for item in values:
        if not isinstance(item, dict):
            continue

        start = item.get("start")
        end = item.get("end")

        if isinstance(start, (int, float)) and isinstance(
            end,
            (int, float),
        ):
            return item

    return None


def _server_chart_option(
    chart: Any,
) -> dict[str, Any] | None:
    """Return the isolated complete option stored on the NiceGUI widget."""

    props = getattr(chart, "_props", None)

    if not isinstance(props, dict):
        return None

    option = props.get("options")

    return option if isinstance(option, dict) else None


def _metadata_utc_datetime(
    value: object,
) -> datetime | None:
    text = str(value or "").strip()

    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None

    return parsed.astimezone(timezone.utc)


def _chart_temporal_metadata(
    chart: Any,
) -> tuple[tuple[datetime, ...], int] | None:
    """Return compressed category timestamps and fixed bar duration."""

    option = _server_chart_option(chart)

    if option is None:
        return None

    metadata = option.get("l2shockChartMetadata")

    if not isinstance(metadata, dict):
        return None

    raw_times = metadata.get("visible_start_times_utc")

    if not isinstance(raw_times, list):
        return None

    timestamps: list[datetime] = []

    for value in raw_times:
        parsed = _metadata_utc_datetime(value)

        if parsed is None:
            return None

        timestamps.append(parsed)

    if not timestamps:
        return None

    if any(
        later <= earlier
        for earlier, later in zip(
            timestamps,
            timestamps[1:],
        )
    ):
        return None

    raw_duration = metadata.get("bar_duration_seconds")

    if (
        isinstance(raw_duration, bool)
        or not isinstance(raw_duration, int)
        or raw_duration <= 0
    ):
        return None

    visible_count = metadata.get("visible_bar_count")

    if (
        isinstance(visible_count, bool)
        or not isinstance(visible_count, int)
        or visible_count != len(timestamps)
    ):
        return None

    return tuple(timestamps), raw_duration


def _viewport_category_indices(
    viewport: AnalysisChartViewport,
    *,
    category_count: int,
) -> tuple[int, int]:
    if category_count <= 0:
        raise AnalysisChartInteractionError("category_count must be positive")

    if category_count == 1:
        return 0, 0

    last_index = category_count - 1

    start_position = viewport.start_percent / 100.0 * last_index
    end_position = viewport.end_percent / 100.0 * last_index

    start_index = max(
        0,
        min(last_index, int(math.floor(start_position))),
    )
    end_index = max(
        start_index,
        min(last_index, int(math.ceil(end_position))),
    )

    return start_index, end_index


def _nearest_timestamp_index(
    timestamps: tuple[datetime, ...],
    target: datetime,
) -> int:
    """Return the deterministic nearest compressed category index."""

    if not timestamps:
        raise AnalysisChartInteractionError(
            "Cannot map a timestamp into an empty chart"
        )

    position = bisect.bisect_left(
        timestamps,
        target,
    )

    if position <= 0:
        return 0

    if position >= len(timestamps):
        return len(timestamps) - 1

    before = timestamps[position - 1]
    after = timestamps[position]

    before_distance = abs((target - before).total_seconds())
    after_distance = abs((after - target).total_seconds())

    # Ties intentionally choose the earlier category so the restored view does
    # not move its left edge farther into the future.
    if before_distance <= after_distance:
        return position - 1

    return position


async def capture_analysis_chart_viewport(
    chart: Any,
) -> AnalysisChartViewport | None:
    """Capture the current browser dataZoom window when available."""
    if chart is None:
        return None

    runner = getattr(chart, "run_chart_method", None)

    if not callable(runner):
        return None

    try:
        option = _decoded_json(
            await runner(
                "getOption",
                timeout=1.5,
            )
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.debug(
            "Could not capture Analysis chart viewport.",
            exc_info=True,
        )
        return None

    data_zoom = _first_data_zoom(option)

    if data_zoom is None:
        return None

    try:
        return AnalysisChartViewport(
            start_percent=float(data_zoom["start"]),
            end_percent=float(data_zoom["end"]),
        )
    except (
        KeyError,
        TypeError,
        ValueError,
        AnalysisChartInteractionError,
    ):
        return None


async def capture_analysis_chart_temporal_viewport(
    chart: Any,
) -> AnalysisChartTemporalViewport | None:
    """Capture left-edge UTC ownership from one acknowledged chart."""

    token = acknowledged_render_token(chart)

    if not token:
        return None

    current_token = str(getattr(chart, "_l2shock_render_token", "") or "").strip()

    if current_token != token:
        return None

    metadata = _chart_temporal_metadata(chart)

    if metadata is None:
        return None

    timestamps, bar_duration_seconds = metadata

    viewport = await capture_analysis_chart_viewport(chart)

    if viewport is None:
        return None

    if acknowledged_render_token(chart) != token:
        return None

    start_index, end_index = _viewport_category_indices(
        viewport,
        category_count=len(timestamps),
    )

    left = timestamps[start_index]
    right_exclusive = timestamps[end_index] + timedelta(seconds=bar_duration_seconds)
    visible_duration = (right_exclusive - left).total_seconds()

    if not math.isfinite(visible_duration) or visible_duration <= 0:
        return None

    return AnalysisChartTemporalViewport(
        left_edge_utc=left,
        visible_duration_seconds=visible_duration,
        source_timeframe_seconds=bar_duration_seconds,
    )


async def _run_chart_method(
    chart: Any,
    method: str,
    *arguments: str,
) -> object:
    runner = getattr(chart, "run_chart_method", None)

    if not callable(runner):
        raise AnalysisChartInteractionError("ECharts widget has no run_chart_method()")

    result = runner(
        method,
        *arguments,
        timeout=2.0,
    )

    if hasattr(result, "__await__"):
        return await result

    return result


async def restore_analysis_chart_viewport(
    chart: Any,
    viewport: AnalysisChartViewport,
    *,
    expected_render_token: str,
) -> bool:
    """Restore a viewport only while one acknowledged generation still owns it."""
    if not isinstance(viewport, AnalysisChartViewport):
        raise TypeError("viewport must be AnalysisChartViewport")

    token = str(expected_render_token or "").strip()

    if not token or acknowledged_render_token(chart) != token:
        return False

    action = {
        "type": "dataZoom",
        "batch": [
            {
                "dataZoomIndex": 0,
                "start": viewport.start_percent,
                "end": viewport.end_percent,
            },
            {
                "dataZoomIndex": 1,
                "start": viewport.start_percent,
                "end": viewport.end_percent,
            },
        ],
    }

    try:
        await _run_chart_method(
            chart,
            ":dispatchAction",
            "("
            + json.dumps(
                action,
                allow_nan=False,
                separators=(",", ":"),
            )
            + ")",
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.debug(
            "Could not restore Analysis chart viewport.",
            exc_info=True,
        )
        return False

    return acknowledged_render_token(chart) == token


async def restore_analysis_chart_temporal_viewport(
    chart: Any,
    viewport: AnalysisChartTemporalViewport,
    *,
    expected_render_token: str,
) -> bool:
    """Restore a UTC left edge and approximately equal elapsed duration."""

    if not isinstance(
        viewport,
        AnalysisChartTemporalViewport,
    ):
        raise TypeError("viewport must be AnalysisChartTemporalViewport")

    token = str(expected_render_token or "").strip()

    if not token or acknowledged_render_token(chart) != token:
        return False

    metadata = _chart_temporal_metadata(chart)

    if metadata is None:
        return False

    timestamps, target_bar_seconds = metadata

    start_index = _nearest_timestamp_index(
        timestamps,
        viewport.left_edge_utc,
    )

    target_right_edge = (
        viewport.left_edge_utc
        + timedelta(seconds=viewport.visible_duration_seconds)
        - timedelta(seconds=target_bar_seconds)
    )

    end_index = _nearest_timestamp_index(
        timestamps,
        target_right_edge,
    )
    end_index = max(start_index, end_index)

    action = {
        "type": "dataZoom",
        "batch": [
            {
                "dataZoomIndex": 0,
                "startValue": start_index,
                "endValue": end_index,
            },
            {
                "dataZoomIndex": 1,
                "startValue": start_index,
                "endValue": end_index,
            },
        ],
    }

    try:
        await _run_chart_method(
            chart,
            ":dispatchAction",
            "("
            + json.dumps(
                action,
                allow_nan=False,
                separators=(",", ":"),
            )
            + ")",
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.debug(
            "Could not restore Analysis chart temporal viewport.",
            exc_info=True,
        )
        return False

    return acknowledged_render_token(chart) == token


def _chart_metadata(
    option: dict[str, Any],
) -> dict[str, object]:
    value = option.get("l2shockChartMetadata")

    if not isinstance(value, dict):
        raise AnalysisChartInteractionError(
            "Analysis chart option lacks l2shockChartMetadata"
        )

    return value


async def install_analysis_gapped_crosshair(
    chart: Any,
    *,
    expected_render_token: str,
    gap_px: int = _DEFAULT_CROSSHAIR_GAP_PX,
) -> bool:
    """Install the five-panel browser-side custom crosshair."""
    if chart is None:
        return False

    token = str(expected_render_token or "").strip()

    if not token or acknowledged_render_token(chart) != token:
        return False

    if isinstance(gap_px, bool) or not isinstance(gap_px, int) or gap_px < 0:
        raise ValueError("gap_px must be a non-negative integer")

    chart_id = getattr(chart, "id", None)

    if chart_id is None:
        return False

    js = f"""
    (function() {{
        const chartId = {json.dumps(chart_id)};
        const expectedToken = {json.dumps(token)};
        const gapPx = {json.dumps(gap_px)};
        const lineColor = {json.dumps(_CROSSHAIR_COLOR)};
        const tokenPrefix = "__l2shock_render_token__:";

        window.__l2shockAnalysisCrosshair =
            window.__l2shockAnalysisCrosshair || {{
                instances: {{}},
                owners: {{}},
                raf: {{}},
                generation: 0,
            }};

        const state = window.__l2shockAnalysisCrosshair;
        state.generation = Number(state.generation || 0) + 1;
        const generation = state.generation;
        const key = String(chartId);

        function unwrapDom(raw) {{
            if (!raw) return null;

            try {{
                if (raw instanceof HTMLElement) return raw;
            }} catch (error) {{}}

            try {{
                if (raw.$el instanceof HTMLElement) return raw.$el;
            }} catch (error) {{}}

            try {{
                if (raw.$el && raw.$el.nodeType === 1) {{
                    return raw.$el;
                }}
            }} catch (error) {{}}

            try {{
                if (raw.el instanceof HTMLElement) return raw.el;
            }} catch (error) {{}}

            try {{
                if (raw.nodeType === 1) return raw;
            }} catch (error) {{}}

            return null;
        }}

        function rootElement() {{
            try {{
                if (typeof getElement === "function") {{
                    const element = unwrapDom(getElement(chartId));
                    if (element) return element;
                }}
            }} catch (error) {{}}

            try {{
                return (
                    document.getElementById("c" + String(chartId))
                    || document.getElementById(String(chartId))
                );
            }} catch (error) {{
                return null;
            }}
        }}

        function chartInstance() {{
            if (typeof echarts === "undefined") return null;

            const root = rootElement();
            if (!root) return null;

            function tryNode(node) {{
                if (!node) return null;

                try {{
                    return echarts.getInstanceByDom(node) || null;
                }} catch (error) {{
                    return null;
                }}
            }}

            let found = tryNode(root);
            if (found) return found;

            const candidates = [];

            try {{
                candidates.push(
                    ...root.querySelectorAll("[_echarts_instance_]")
                );
            }} catch (error) {{}}

            try {{
                candidates.push(
                    ...root.querySelectorAll(
                        ".echarts, .q-echart, div, canvas, svg"
                    )
                );
            }} catch (error) {{}}

            let parent = root.parentElement;
            let depth = 0;

            while (parent && depth < 5) {{
                candidates.push(parent);
                parent = parent.parentElement;
                depth += 1;
            }}

            const seen = new Set();

            for (const node of candidates) {{
                if (!node || seen.has(node)) continue;
                seen.add(node);

                found = tryNode(node);
                if (found) return found;
            }}

            return null;
        }}

        function ownsToken(instance) {{
            if (!instance || !instance.getOption) return false;

            let option = null;

            try {{
                option = instance.getOption();
            }} catch (error) {{
                return false;
            }}

            const series = Array.isArray(option && option.series)
                ? option.series
                : [];

            return series.some(
                item =>
                    String((item && item.name) || "") ===
                    tokenPrefix + expectedToken
            );
        }}

        function gridRect(instance, index) {{
            try {{
                const model = instance.getModel();
                const grid = model.getComponent("grid", index);
                const coordinateSystem =
                    grid && grid.coordinateSystem;
                const rectangle =
                    coordinateSystem && coordinateSystem.getRect();

                if (!rectangle) return null;

                return {{
                    x: Number(rectangle.x),
                    y: Number(rectangle.y),
                    width: Number(rectangle.width),
                    height: Number(rectangle.height),
                }};
            }} catch (error) {{
                return null;
            }}
        }}

        function line(id, x1, y1, x2, y2) {{
            const visible =
                Number.isFinite(x1)
                && Number.isFinite(y1)
                && Number.isFinite(x2)
                && Number.isFinite(y2)
                && Math.abs(x2 - x1) + Math.abs(y2 - y1) > 1;

            return {{
                id: id,
                type: "line",
                silent: true,
                z: 50000,
                shape: visible
                    ? {{x1: x1, y1: y1, x2: x2, y2: y2}}
                    : {{x1: 0, y1: 0, x2: 0, y2: 0}},
                style: {{
                    stroke: lineColor,
                    lineWidth: 1.2,
                    opacity: visible ? 0.92 : 0,
                }},
            }};
        }}

        function removeGraphics(instance) {{
            if (!instance) return;

            const removals = [];

            for (let panel = 0; panel < 5; panel += 1) {{
                removals.push({{
                    id: "l2shock_crosshair_v_" + panel,
                    $action: "remove",
                }});
            }}

            removals.push(
                {{
                    id: "l2shock_crosshair_price_v_top",
                    $action: "remove",
                }},
                {{
                    id: "l2shock_crosshair_price_v_bottom",
                    $action: "remove",
                }},
                {{
                    id: "l2shock_crosshair_price_h_left",
                    $action: "remove",
                }},
                {{
                    id: "l2shock_crosshair_price_h_right",
                    $action: "remove",
                }}
            );

            try {{
                instance.setOption(
                    {{graphic: removals}},
                    {{notMerge: false, lazyUpdate: true}}
                );
            }} catch (error) {{}}
        }}

        function pointerPanel(rectangles, x, y) {{
            for (let index = 0; index < rectangles.length; index += 1) {{
                const rectangle = rectangles[index];

                if (!rectangle) continue;

                if (
                    x >= rectangle.x
                    && x <= rectangle.x + rectangle.width
                    && y >= rectangle.y
                    && y <= rectangle.y + rectangle.height
                ) {{
                    return index;
                }}
            }}

            return -1;
        }}

        function draw(instance, pointer) {{
            if (
                generation !== state.generation
                || state.owners[key] !== expectedToken
                || !ownsToken(instance)
            ) {{
                removeGraphics(instance);
                return;
            }}

            const x = Number(pointer && pointer.x);
            const y = Number(pointer && pointer.y);

            if (!Number.isFinite(x) || !Number.isFinite(y)) {{
                removeGraphics(instance);
                return;
            }}

            const rectangles = [];

            for (let index = 0; index < 5; index += 1) {{
                rectangles.push(gridRect(instance, index));
            }}

            const hoveredPanel = pointerPanel(rectangles, x, y);

            if (hoveredPanel < 0) {{
                removeGraphics(instance);
                return;
            }}

            const graphics = [];
            const price = rectangles[0];

            for (let index = 1; index < 5; index += 1) {{
                const rectangle = rectangles[index];

                if (!rectangle) continue;

                graphics.push(
                    line(
                        "l2shock_crosshair_v_" + index,
                        x,
                        rectangle.y,
                        x,
                        rectangle.y + rectangle.height
                    )
                );
            }}

            if (price) {{
                if (hoveredPanel === 0) {{
                    graphics.push(
                        line(
                            "l2shock_crosshair_price_v_top",
                            x,
                            price.y,
                            x,
                            Math.max(price.y, y - gapPx)
                        ),
                        line(
                            "l2shock_crosshair_price_v_bottom",
                            x,
                            Math.min(
                                price.y + price.height,
                                y + gapPx
                            ),
                            x,
                            price.y + price.height
                        ),
                        line(
                            "l2shock_crosshair_price_h_left",
                            price.x,
                            y,
                            Math.max(price.x, x - gapPx),
                            y
                        ),
                        line(
                            "l2shock_crosshair_price_h_right",
                            Math.min(
                                price.x + price.width,
                                x + gapPx
                            ),
                            y,
                            price.x + price.width,
                            y
                        )
                    );
                }} else {{
                    graphics.push(
                        line(
                            "l2shock_crosshair_v_0",
                            x,
                            price.y,
                            x,
                            price.y + price.height
                        ),
                        {{
                            id: "l2shock_crosshair_price_v_top",
                            $action: "remove",
                        }},
                        {{
                            id: "l2shock_crosshair_price_v_bottom",
                            $action: "remove",
                        }},
                        {{
                            id: "l2shock_crosshair_price_h_left",
                            $action: "remove",
                        }},
                        {{
                            id: "l2shock_crosshair_price_h_right",
                            $action: "remove",
                        }}
                    );
                }}
            }}

            try {{
                instance.setOption(
                    {{graphic: graphics}},
                    {{notMerge: false, lazyUpdate: true}}
                );
            }} catch (error) {{}}
        }}

        function schedule(instance, pointer) {{
            if (Number(state.raf[key] || 0)) return;

            state.raf[key] = requestAnimationFrame(function() {{
                state.raf[key] = 0;
                draw(instance, pointer);
            }});
        }}

        function install(attempt) {{
            if (
                generation !== state.generation
                || state.owners[key] !== expectedToken
            ) {{
                return;
            }}

            const instance = chartInstance();

            if (!instance || !ownsToken(instance)) {{
                if (attempt < 20) {{
                    setTimeout(
                        () => install(attempt + 1),
                        120
                    );
                }}
                return;
            }}

            state.instances[key] = instance;

            try {{
                const renderer = instance.getZr();
                const previous =
                    instance.__l2shockGappedCrosshairHandlers;

                if (previous && typeof previous === "object") {{
                    try {{
                        if (typeof previous.mousemove === "function") {{
                            renderer.off(
                                "mousemove",
                                previous.mousemove
                            );
                        }}

                        if (typeof previous.globalout === "function") {{
                            renderer.off(
                                "globalout",
                                previous.globalout
                            );
                        }}
                    }} catch (error) {{}}
                }}

                const mousemoveHandler = function(event) {{
                    const pointer = {{
                        x: Number(event && event.offsetX),
                        y: Number(event && event.offsetY),
                    }};

                    schedule(instance, pointer);
                }};

                const globaloutHandler = function() {{
                    const pending = Number(state.raf[key] || 0);

                    if (pending) {{
                        try {{
                            cancelAnimationFrame(pending);
                        }} catch (error) {{}}
                    }}

                    state.raf[key] = 0;
                    removeGraphics(instance);
                }};

                instance.__l2shockGappedCrosshairHandlers = {{
                    mousemove: mousemoveHandler,
                    globalout: globaloutHandler,
                    token: expectedToken,
                    generation: generation,
                }};
                instance.__l2shockGappedCrosshairInstalled =
                    expectedToken;

                renderer.on(
                    "mousemove",
                    mousemoveHandler
                );
                renderer.on(
                    "globalout",
                    globaloutHandler
                );
            }} catch (error) {{}}
        }}

        state.owners[key] = expectedToken;
        install(0);

        return {{
            ok: true,
            chartId: chartId,
            token: expectedToken,
        }};
    }})();
    """

    try:
        await ui.run_javascript(
            js,
            timeout=6.0,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.debug(
            "Could not install the Analysis gapped crosshair.",
            exc_info=True,
        )
        return False

    return acknowledged_render_token(chart) == token


async def emphasize_analysis_chart_window(
    chart: Any,
    window: ChartNavigationWindow,
    *,
    commit: AnalysisChartCommit,
) -> bool:
    """Emphasize one selected LM interval in all five chart panels."""

    if not isinstance(window, ChartNavigationWindow):
        raise TypeError("window must be ChartNavigationWindow")

    if not isinstance(commit, AnalysisChartCommit):
        raise TypeError("commit must be AnalysisChartCommit")

    token = commit.publication.render_token

    if acknowledged_render_token(chart) != token:
        return False

    patch = {
        "series": [
            {
                "id": (ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX + str(panel_index)),
                "type": "line",
                "xAxisIndex": panel_index,
                "yAxisIndex": panel_index,
                "markArea": {
                    "silent": True,
                    "label": {"show": False},
                    "itemStyle": {
                        "color": "rgba(245,158,11,0.18)",
                        "borderColor": "#f59e0b",
                        "borderWidth": 2,
                    },
                    "data": [
                        [
                            {"xAxis": (window.candidate_start_index)},
                            {"xAxis": (window.candidate_end_index)},
                        ]
                    ],
                },
            }
            for panel_index in range(5)
        ]
    }

    try:
        await _run_chart_method(
            chart,
            ":setOption",
            "("
            + json.dumps(
                patch,
                allow_nan=False,
                separators=(",", ":"),
            )
            + ")",
            "({notMerge:false,lazyUpdate:false})",
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.debug(
            "Could not emphasize the selected Analysis LM.",
            exc_info=True,
        )
        return False

    return acknowledged_render_token(chart) == token


def _safe_export_filename(
    value: object,
    *,
    extension: str,
) -> str:
    text = str(value or "").strip()

    if not text:
        raise AnalysisChartInteractionError("Export filename cannot be blank")

    cleaned = "".join(
        character if (character.isalnum() or character in {"-", "_", "."}) else "_"
        for character in text
    )
    cleaned = cleaned.strip("._-")

    if not cleaned:
        raise AnalysisChartInteractionError(
            "Export filename contains no safe characters"
        )

    suffix = "." + extension

    if cleaned.lower().endswith(suffix):
        stem = cleaned[: -len(suffix)]
    else:
        stem = cleaned

    maximum_stem_length = max(
        1,
        180 - len(suffix),
    )
    stem = stem[:maximum_stem_length].rstrip("._-")

    if not stem:
        stem = "chart"

    return stem + suffix


async def export_analysis_chart_image(
    chart: Any,
    *,
    commit: AnalysisChartCommit,
    image_format: str,
    filename: str,
) -> tuple[bool, str]:
    """Export one acknowledged chart generation as PNG or SVG."""

    if not isinstance(commit, AnalysisChartCommit):
        raise TypeError("commit must be AnalysisChartCommit")

    selected_format = str(image_format or "").strip().lower()

    if selected_format not in {"png", "svg"}:
        raise AnalysisChartInteractionError("image_format must be 'png' or 'svg'")

    safe_filename = _safe_export_filename(
        filename,
        extension=selected_format,
    )

    token = commit.publication.render_token

    if acknowledged_render_token(chart) != token:
        return (
            False,
            "The chart render generation changed before export.",
        )

    chart_id = getattr(chart, "id", None)

    if chart_id is None:
        return False, "The ECharts widget has no browser element ID."

    js = f"""
    (async function() {{
        const chartId = {json.dumps(chart_id)};
        const expectedToken = {json.dumps(token)};
        const filename = {json.dumps(safe_filename)};
        const exportFormat = {json.dumps(selected_format)};
        const tokenPrefix = "__l2shock_render_token__:";

        function unwrapDom(raw) {{
            if (!raw) return null;

            try {{
                if (raw instanceof HTMLElement) return raw;
            }} catch (error) {{}}

            try {{
                if (raw.$el instanceof HTMLElement) return raw.$el;
            }} catch (error) {{}}

            try {{
                if (raw.$el && raw.$el.nodeType === 1) {{
                    return raw.$el;
                }}
            }} catch (error) {{}}

            try {{
                if (raw.el instanceof HTMLElement) return raw.el;
            }} catch (error) {{}}

            try {{
                if (raw.nodeType === 1) return raw;
            }} catch (error) {{}}

            return null;
        }}

        function rootElement() {{
            try {{
                if (typeof getElement === "function") {{
                    const element = unwrapDom(getElement(chartId));
                    if (element) return element;
                }}
            }} catch (error) {{}}

            try {{
                return (
                    document.getElementById("c" + String(chartId))
                    || document.getElementById(String(chartId))
                );
            }} catch (error) {{
                return null;
            }}
        }}

        function chartInstance() {{
            if (typeof echarts === "undefined") return null;

            const root = rootElement();
            if (!root) return null;

            function tryNode(node) {{
                if (!node) return null;

                try {{
                    return echarts.getInstanceByDom(node) || null;
                }} catch (error) {{
                    return null;
                }}
            }}

            let found = tryNode(root);
            if (found) return found;

            const candidates = [];

            try {{
                candidates.push(
                    ...root.querySelectorAll(
                        "[_echarts_instance_]"
                    )
                );
            }} catch (error) {{}}

            try {{
                candidates.push(
                    ...root.querySelectorAll(
                        ".echarts, .q-echart, div, canvas, svg"
                    )
                );
            }} catch (error) {{}}

            let parent = root.parentElement;
            let depth = 0;

            while (parent && depth < 5) {{
                candidates.push(parent);
                parent = parent.parentElement;
                depth += 1;
            }}

            const seen = new Set();

            for (const node of candidates) {{
                if (!node || seen.has(node)) continue;
                seen.add(node);

                found = tryNode(node);
                if (found) return found;
            }}

            return null;
        }}

        function ownsToken(instance) {{
            if (!instance || !instance.getOption) return false;

            let option = null;

            try {{
                option = instance.getOption();
            }} catch (error) {{
                return false;
            }}

            const series = Array.isArray(option && option.series)
                ? option.series
                : [];

            return series.some(
                item =>
                    String((item && item.name) || "") ===
                    tokenPrefix + expectedToken
            );
        }}

        const instance = chartInstance();

        if (!instance) {{
            return {{
                ok: false,
                reason: "Could not locate the live ECharts instance.",
            }};
        }}

        if (!ownsToken(instance)) {{
            return {{
                ok: false,
                reason: (
                    "The browser chart no longer owns the "
                    + "acknowledged render token."
                ),
            }};
        }}

        const option = instance.getOption();

        // Browser-side crosshair graphics are transient interaction state.
        // Selected-LM markArea emphasis remains because it belongs to series.
        option.graphic = [];

        const width = Math.max(
            900,
            Number(instance.getWidth()) || 900
        );
        const height = Math.max(
            680,
            Number(instance.getHeight()) || 680
        );

        const container = document.createElement("div");
        container.style.position = "fixed";
        container.style.left = "-100000px";
        container.style.top = "-100000px";
        container.style.width = width + "px";
        container.style.height = height + "px";
        container.style.opacity = "0";
        container.style.pointerEvents = "none";
        document.body.appendChild(container);

        let clone = null;

        try {{
            clone = echarts.init(
                container,
                null,
                {{
                    renderer: (
                        exportFormat === "svg"
                            ? "svg"
                            : "canvas"
                    ),
                    width: width,
                    height: height,
                    devicePixelRatio: 1,
                }}
            );

            clone.setOption(
                option,
                {{
                    notMerge: true,
                    lazyUpdate: false,
                }}
            );
            clone.resize({{
                width: width,
                height: height,
            }});

            await new Promise(
                resolve => setTimeout(resolve, 80)
            );

            const dataUrl = clone.getDataURL({{
                type: exportFormat,
                pixelRatio: (
                    exportFormat === "png" ? 2 : 1
                ),
                backgroundColor: "#0f172a",
            }});

            if (
                typeof dataUrl !== "string"
                || !dataUrl.startsWith("data:")
            ) {{
                return {{
                    ok: false,
                    reason: "ECharts returned an invalid image payload.",
                }};
            }}

            const anchor = document.createElement("a");
            anchor.href = dataUrl;
            anchor.download = filename;
            anchor.style.display = "none";
            document.body.appendChild(anchor);
            anchor.click();
            document.body.removeChild(anchor);

            return {{
                ok: true,
                reason: "",
                filename: filename,
                format: exportFormat,
            }};
        }} catch (error) {{
            return {{
                ok: false,
                reason: String(
                    (error && error.message) || error
                ),
            }};
        }} finally {{
            try {{
                if (clone) clone.dispose();
            }} catch (error) {{}}

            try {{
                document.body.removeChild(container);
            }} catch (error) {{}}
        }}
    }})();
    """

    try:
        raw_result = await ui.run_javascript(
            js,
            timeout=30.0,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.exception("Analysis chart image export failed.")
        return (
            False,
            "Browser chart export failed: " f"{type(exc).__name__}",
        )

    result = _decoded_json(raw_result)

    if not isinstance(result, dict):
        return False, "Browser returned an invalid export result."

    if not bool(result.get("ok")):
        return (
            False,
            str(result.get("reason") or "Chart export failed."),
        )

    if acknowledged_render_token(chart) != token:
        return (
            False,
            "The chart generation changed during export.",
        )

    return True, ""


async def navigate_analysis_chart(
    chart: Any,
    window: ChartNavigationWindow,
    *,
    commit: AnalysisChartCommit,
) -> bool:
    """Navigate all five panels to one selected LM category window."""
    if not isinstance(window, ChartNavigationWindow):
        raise TypeError("window must be ChartNavigationWindow")

    if not isinstance(commit, AnalysisChartCommit):
        raise TypeError("commit must be AnalysisChartCommit")

    token = commit.publication.render_token

    if acknowledged_render_token(chart) != token:
        return False

    count = commit.visible_category_count

    if count <= 0:
        return False

    start_percent = 100.0 * window.start_index / count
    end_percent = 100.0 * (window.end_index + 1) / count

    start_percent = max(0.0, min(100.0, start_percent))
    end_percent = max(start_percent, min(100.0, end_percent))

    zoom_action = {
        "type": "dataZoom",
        "batch": [
            {
                "dataZoomIndex": 0,
                "start": start_percent,
                "end": end_percent,
            },
            {
                "dataZoomIndex": 1,
                "start": start_percent,
                "end": end_percent,
            },
        ],
    }

    tooltip_action = {
        "type": "showTip",
        "seriesIndex": 0,
        "dataIndex": window.candidate_start_index,
    }

    emphasized = await emphasize_analysis_chart_window(
        chart,
        window,
        commit=commit,
    )

    if not emphasized:
        return False

    try:
        await _run_chart_method(
            chart,
            ":dispatchAction",
            "("
            + json.dumps(
                zoom_action,
                allow_nan=False,
                separators=(",", ":"),
            )
            + ")",
        )

        await _run_chart_method(
            chart,
            ":dispatchAction",
            "("
            + json.dumps(
                tooltip_action,
                allow_nan=False,
                separators=(",", ":"),
            )
            + ")",
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.debug(
            "Could not navigate the Analysis chart.",
            exc_info=True,
        )
        return False

    return acknowledged_render_token(chart) == token


class AnalysisChartController:
    """Serialize and commit chart publication ownership."""

    def __init__(
        self,
        chart: Any,
        *,
        crosshair_gap_px: int = _DEFAULT_CROSSHAIR_GAP_PX,
    ) -> None:
        if chart is None:
            raise ValueError("chart cannot be null")

        if (
            isinstance(crosshair_gap_px, bool)
            or not isinstance(crosshair_gap_px, int)
            or crosshair_gap_px < 0
        ):
            raise ValueError("crosshair_gap_px must be a non-negative integer")

        self._chart = chart
        self._crosshair_gap_px = crosshair_gap_px
        self._request_generation = 0
        self._commit: AnalysisChartCommit | None = None
        self._emphasis_owner_id: str | None = None
        self._emphasis_window: ChartNavigationWindow | None = None

    @property
    def commit(self) -> AnalysisChartCommit | None:
        return self._commit

    def invalidate(self) -> None:
        """Invalidate pending/committed ownership without destroying the widget."""
        self._request_generation += 1
        self._commit = None
        self._emphasis_owner_id = None
        self._emphasis_window = None

    def clear_emphasis(self) -> None:
        """Clear selected-LM emphasis without invalidating the chart commit."""
        self._emphasis_owner_id = None
        self._emphasis_window = None

    async def publish(
        self,
        option: dict[str, Any],
        *,
        owner_id: str,
        preserve_viewport: bool,
        temporal_viewport: AnalysisChartTemporalViewport | None = None,
    ) -> AnalysisChartCommit | None:
        """Publish and commit one browser-acknowledged chart generation."""
        owner = str(owner_id or "").strip()

        if not owner:
            raise AnalysisChartInteractionError(
                "Chart publication owner_id cannot be blank"
            )

        if not isinstance(option, dict):
            raise TypeError("option must be a dictionary")

        self._request_generation += 1
        request_generation = self._request_generation

        # No chart generation owns export/navigation while a replacement is
        # still being acknowledged and post-processed.
        self._commit = None

        if preserve_viewport and temporal_viewport is not None:
            raise AnalysisChartInteractionError(
                "A publication cannot request percentage and temporal "
                "viewport restoration simultaneously"
            )

        viewport = (
            await capture_analysis_chart_viewport(self._chart)
            if preserve_viewport
            else None
        )

        if request_generation != self._request_generation:
            return None

        publication = set_echart_options(
            self._chart,
            option,
        )

        acknowledged, reason = await confirm_echart_render_identity(
            self._chart,
            publication,
        )

        if request_generation != self._request_generation:
            return None

        if not acknowledged:
            raise AnalysisChartInteractionError(
                "Browser did not acknowledge the chart publication: " + reason
            )

        metadata = _chart_metadata(option)
        raw_count = metadata.get("visible_bar_count")

        if (
            isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_count < 0
        ):
            raise AnalysisChartInteractionError(
                "Chart metadata has invalid visible_bar_count"
            )

        commit = AnalysisChartCommit(
            owner_id=owner,
            publication=publication,
            visible_category_count=raw_count,
        )

        if temporal_viewport is not None:
            await restore_analysis_chart_temporal_viewport(
                self._chart,
                temporal_viewport,
                expected_render_token=publication.render_token,
            )
        elif viewport is not None:
            await restore_analysis_chart_viewport(
                self._chart,
                viewport,
                expected_render_token=publication.render_token,
            )

        if request_generation != self._request_generation:
            return None

        await install_analysis_gapped_crosshair(
            self._chart,
            expected_render_token=publication.render_token,
            gap_px=self._crosshair_gap_px,
        )

        if request_generation != self._request_generation:
            return None

        if self._emphasis_owner_id == owner and self._emphasis_window is not None:
            await emphasize_analysis_chart_window(
                self._chart,
                self._emphasis_window,
                commit=commit,
            )

        if request_generation != self._request_generation:
            return None

        self._commit = commit
        return commit

    async def navigate(
        self,
        window: ChartNavigationWindow,
        *,
        owner_id: str,
    ) -> bool:
        """Navigate only if the requested analysis still owns the chart."""
        commit = self._commit

        if commit is None:
            return False

        normalized_owner = str(owner_id or "").strip()

        if commit.owner_id != normalized_owner:
            return False

        navigated = await navigate_analysis_chart(
            self._chart,
            window,
            commit=commit,
        )

        if navigated:
            self._emphasis_owner_id = normalized_owner
            self._emphasis_window = window

        return navigated

    async def export_image(
        self,
        *,
        owner_id: str,
        image_format: str,
        filename: str,
    ) -> tuple[bool, str]:
        """Export only the currently committed Analysis owner."""

        commit = self._commit

        if commit is None:
            return False, "No acknowledged chart is available."

        if commit.owner_id != str(owner_id or "").strip():
            return (
                False,
                "The requested Analysis result no longer owns the chart.",
            )

        return await export_analysis_chart_image(
            self._chart,
            commit=commit,
            image_format=image_format,
            filename=filename,
        )


__all__ = [
    "AnalysisChartCommit",
    "AnalysisChartController",
    "AnalysisChartInteractionError",
    "AnalysisChartTemporalViewport",
    "AnalysisChartViewport",
    "capture_analysis_chart_temporal_viewport",
    "capture_analysis_chart_viewport",
    "emphasize_analysis_chart_window",
    "export_analysis_chart_image",
    "install_analysis_gapped_crosshair",
    "navigate_analysis_chart",
    "restore_analysis_chart_temporal_viewport",
    "restore_analysis_chart_viewport",
]

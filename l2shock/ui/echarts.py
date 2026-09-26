# l2shock/ui/echarts.py
"""Safe complete-option publication for NiceGUI ECharts widgets.

Every complete chart publication owns two synchronized representations:

1. NiceGUI/Vue's stored ``options`` property;
2. the live browser ECharts instance through ``setOption(notMerge=True)``.

Every publication also carries one hidden render-token series. Ownership-
critical callers may await ``confirm_echart_render_identity`` before treating
the browser as committed to that generation.

The hidden series contains no data and does not affect chart semantics,
legends, tooltips, zoom, analysis identity, or export data.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import secrets
from dataclasses import dataclass
from typing import Any, Final

log = logging.getLogger(__name__)

ECHART_RENDER_TOKEN_SERIES_PREFIX: Final[str] = "__l2shock_render_token__:"


class EChartPublicationError(RuntimeError):
    """A complete ECharts option could not be safely published or verified."""


@dataclass(frozen=True, slots=True)
class EChartPublication:
    """Identity of one complete Python-to-browser option publication."""

    render_token: str
    expected_category_count: int | None

    def __post_init__(self) -> None:
        token = str(self.render_token or "").strip()

        if (
            len(token) != 32
            or token != token.lower()
            or any(character not in "0123456789abcdef" for character in token)
        ):
            raise EChartPublicationError(
                "render_token must be 32 canonical lowercase hexadecimal characters"
            )

        count = self.expected_category_count

        if count is not None:
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise EChartPublicationError(
                    "expected_category_count must be null or non-negative"
                )

        object.__setattr__(self, "render_token", token)


def empty_echart_option(
    title: str = "",
) -> dict[str, Any]:
    """Return a browser-safe empty ECharts option."""
    return {
        "animation": False,
        "backgroundColor": "#0f172a",
        "title": {
            "text": str(title or ""),
            "left": "center",
            "top": "middle",
            "textStyle": {
                "fontSize": 12,
                "color": "#94a3b8",
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


def coerce_echart_option(
    option: object,
) -> dict[str, Any]:
    """Return an isolated complete option shape accepted by ECharts."""
    if not isinstance(option, dict) or not option:
        return empty_echart_option()

    safe = copy.deepcopy(option)

    series = safe.get("series")

    if series is None:
        safe["series"] = []
    elif not isinstance(series, list):
        safe["series"] = [series]

    if "xAxis" not in safe:
        safe["xAxis"] = [
            {
                "type": "category",
                "data": [],
                "show": False,
            }
        ]

    if "yAxis" not in safe:
        safe["yAxis"] = [
            {
                "type": "value",
                "show": False,
            }
        ]

    return safe


def _component_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value

    if value is None:
        return []

    return [value]


def _category_count(
    option: dict[str, Any],
    *,
    x_axis_index: int = 0,
) -> int | None:
    axes = _component_list(option.get("xAxis"))

    if not axes or x_axis_index >= len(axes):
        return None

    axis = axes[x_axis_index]

    if not isinstance(axis, dict):
        return None

    data = axis.get("data")

    if not isinstance(data, list):
        return None

    return len(data)


def _attach_render_identity(
    option: dict[str, Any],
) -> EChartPublication:
    """Attach a hidden series identifying this exact option generation."""
    token = secrets.token_hex(16)
    prefix = ECHART_RENDER_TOKEN_SERIES_PREFIX

    series = [
        item
        for item in list(option.get("series") or [])
        if not (
            isinstance(item, dict) and str(item.get("name") or "").startswith(prefix)
        )
    ]

    series.append(
        {
            "name": prefix + token,
            "type": "scatter",
            "data": [],
            "silent": True,
            "tooltip": {"show": False},
            "legendHoverLink": False,
            "symbolSize": 0,
            "z": -1000,
        }
    )

    option["series"] = series

    metadata = option.get("l2shockPublication")

    if metadata is None:
        metadata = {}
        option["l2shockPublication"] = metadata

    if not isinstance(metadata, dict):
        raise EChartPublicationError(
            "l2shockPublication option metadata must be an object"
        )

    metadata["render_token"] = token

    return EChartPublication(
        render_token=token,
        expected_category_count=_category_count(option),
    )


def _javascript_option_expression(
    option: dict[str, Any],
) -> str:
    """Return JavaScript which revives NiceGUI ``:function`` keys."""
    try:
        payload = json.dumps(
            option,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise EChartPublicationError(
            f"ECharts option is not strict JSON serializable: {exc}"
        ) from exc

    return f"""
    (function() {{
        const root = {payload};

        function revive(value) {{
            if (!value || typeof value !== 'object') {{
                return value;
            }}

            if (Array.isArray(value)) {{
                for (const item of value) {{
                    revive(item);
                }}
                return value;
            }}

            const removeKeys = [];

            for (const key of Object.keys(value)) {{
                const item = value[key];

                if (
                    key.startsWith(':')
                    && typeof item === 'string'
                ) {{
                    const realKey = key.slice(1);
                    value[realKey] = (
                        new Function('return (' + item + ');')
                    )();
                    removeKeys.push(key);
                    continue;
                }}

                revive(item);
            }}

            for (const key of removeKeys) {{
                delete value[key];
            }}

            return value;
        }}

        return revive(root);
    }})()
    """


def set_echart_options(
    chart: Any,
    option: dict[str, Any],
) -> EChartPublication:
    """Publish one complete option generation.

    Successful return means that both publication requests were accepted by
    Python/NiceGUI. It does not yet prove that the browser has rendered the
    generation. Use ``confirm_echart_render_identity`` for that proof.
    """
    if chart is None:
        raise EChartPublicationError("ECharts widget is unavailable")
    safe = coerce_echart_option(option)
    publication = _attach_render_identity(safe)
    try:
        chart._props["options"] = safe
        chart.update()
    except Exception as exc:
        raise EChartPublicationError(
            "NiceGUI did not accept the complete ECharts option"
        ) from exc

    runner = getattr(chart, "run_chart_method", None)
    if not callable(runner):
        raise EChartPublicationError("ECharts widget has no run_chart_method()")

    setattr(chart, "_l2shock_render_token", publication.render_token)
    setattr(chart, "_l2shock_acknowledged_render_token", "")

    # Apply the complete generation to the live ECharts instance with
    # notMerge=true. NiceGUI's own update_chart() merges whenever the series
    # count is unchanged, which can retain stale series state.
    #
    # The ":" prefix is mandatory: only then does NiceGUI evaluate each
    # argument as a JavaScript expression. Without it, ECharts receives the
    # option *text* as a string and its instance is corrupted, after which
    # every getOption() throws in the browser and Python only sees timeouts.
    js_option = _javascript_option_expression(safe)
    # Real widgets: a browser script applies the option inside try/catch,
    # retries once after instance.clear(), and records its outcome so the
    # live-state probe can report the exact ECharts error.
    # Test doubles without id/client keep the ":setOption" chart-method path.
    apply_target = _live_state_javascript_runner(chart)
    if apply_target is not None:
        _disable_nicegui_merge_update(chart)
    try:
        if apply_target is not None:
            js_runner, element_id = apply_target
            js_runner(
                _apply_option_code(
                    element_id,
                    render_token=str(safe["l2shockPublication"]["render_token"]),
                    option_expression=js_option,
                ),
                timeout=5.0,
            )
        else:
            runner(
                ":setOption",
                js_option,
                "({notMerge:true,lazyUpdate:false})",
            )
    except Exception:
        log.debug(
            "Explicit setOption failed; relying on NiceGUI update.",
            exc_info=True,
        )

    try:
        runner("resize")
    except Exception:
        log.debug(
            "Immediate ECharts resize failed.",
            exc_info=True,
        )

    return publication


def _decoded_method_result(value: object) -> object:
    if not isinstance(value, str):
        return value

    try:
        return json.loads(value)
    except TypeError, ValueError, json.JSONDecodeError:
        return value


async def _confirm_via_get_option(
    chart: Any,
    publication: EChartPublication,
    *,
    x_axis_index: int = 0,
    attempts: int = 16,
    retry_delay_seconds: float = 0.125,
) -> tuple[bool, str]:
    """Legacy confirmation through a full getOption() round trip.

    Used only for widgets that cannot run the compact browser probe, such as
    unit-test doubles without ``id``/``client``. Real NiceGUI charts use
    ``confirm_echart_render_identity``'s small probe, because a full
    getOption() reply can exceed NiceGUI's ~1 MB browser-to-server limit.
    """
    if chart is None:
        return False, "ECharts widget is unavailable."

    if not isinstance(publication, EChartPublication):
        raise TypeError("publication must be EChartPublication")

    runner = getattr(chart, "run_chart_method", None)

    if not callable(runner):
        return False, "ECharts widget has no run_chart_method()."

    if (
        isinstance(x_axis_index, bool)
        or not isinstance(x_axis_index, int)
        or x_axis_index < 0
    ):
        raise ValueError("x_axis_index must be a non-negative integer")

    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts <= 0:
        raise ValueError("attempts must be a positive integer")

    retry_delay = max(0.0, float(retry_delay_seconds))
    expected_name = ECHART_RENDER_TOKEN_SERIES_PREFIX + publication.render_token
    last_reason = "Browser did not acknowledge the ECharts generation."

    for attempt in range(attempts):
        current_token = str(getattr(chart, "_l2shock_render_token", "") or "")

        if current_token != publication.render_token:
            return (
                False,
                "The publication was superseded before browser acknowledgement.",
            )

        try:
            raw_option = await runner(
                "getOption",
                timeout=1.0,
            )
            option = _decoded_method_result(raw_option)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_reason = (
                "Could not read the live browser ECharts option: "
                f"{type(exc).__name__}"
            )
            option = None

        if isinstance(option, dict):
            series = _component_list(option.get("series"))
            token_found = any(
                isinstance(item, dict) and str(item.get("name") or "") == expected_name
                for item in series
            )

            if not token_found:
                last_reason = "Browser chart does not own the expected render token."
            else:
                expected_count = publication.expected_category_count

                if expected_count is not None:
                    axes = _component_list(option.get("xAxis"))

                    if x_axis_index >= len(axes):
                        last_reason = "Browser chart lacks the expected x-axis."
                        token_found = False
                    else:
                        axis = axes[x_axis_index]
                        data = axis.get("data") if isinstance(axis, dict) else None
                        observed_count = len(data) if isinstance(data, list) else 0

                        if observed_count != expected_count:
                            last_reason = (
                                "Browser category count differs from the "
                                "published option: "
                                f"expected={expected_count}, "
                                f"observed={observed_count}."
                            )
                            token_found = False

                if token_found:
                    try:
                        width = float(
                            await runner(
                                "getWidth",
                                timeout=1.0,
                            )
                        )
                        height = float(
                            await runner(
                                "getHeight",
                                timeout=1.0,
                            )
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        last_reason = (
                            "Browser owns the option, but chart layout "
                            f"could not be verified: {type(exc).__name__}."
                        )
                    else:
                        if (
                            math.isfinite(width)
                            and math.isfinite(height)
                            and width > 20.0
                            and height > 20.0
                        ):
                            current_token = str(
                                getattr(
                                    chart,
                                    "_l2shock_render_token",
                                    "",
                                )
                                or ""
                            )

                            if current_token != publication.render_token:
                                return (
                                    False,
                                    "The publication was superseded during "
                                    "browser acknowledgement.",
                                )

                            setattr(
                                chart,
                                "_l2shock_acknowledged_render_token",
                                publication.render_token,
                            )
                            return True, ""

                        last_reason = (
                            "Browser chart has no usable rendered layout: "
                            f"width={width!r}, height={height!r}."
                        )

        if attempt + 1 < attempts:
            try:
                await runner(
                    "resize",
                    timeout=1.0,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

            await asyncio.sleep(retry_delay)

    return False, last_reason


# Instance tag only. Batch 28 wrapped instance.setOption here; that forced a
# full reset on every NiceGUI re-apply and broke dataZoom. NiceGUI's
# re-apply is now disabled at the source (see _disable_nicegui_merge_update),
# so this only labels the live instance so diagnostics can detect when the
# ECharts instance is re-created.
_MERGE_GUARD_JS: Final[str] = r"""
    function l2shockGuardInstance(instance, guardChartId) {
        if (!instance || instance.__l2shockInstanceUid) {
            return;
        }
        var counter = (
            window.__l2shockInstanceCounter
            = (window.__l2shockInstanceCounter || 0) + 1
        );
        instance.__l2shockInstanceUid = String(guardChartId) + "#" + String(counter);
    }
"""

_MERGE_GUARD_MARKER: Final[str] = "/*__L2SHOCK_MERGE_GUARD__*/"


# Browser-side probe. It returns only small scalar facts, never the complete
# option: a full getOption() reply for a bounded viewport can exceed
# NiceGUI's ~1 MB browser-to-server message limit and is then dropped.
# Placeholders are replaced explicitly; no f-string touches the JavaScript.
_LIVE_STATE_PROBE_JS: Final[str] = r"""
(function () {
    "use strict";
    var chartId = __L2SHOCK_CHART_ID__;
    var tokenPrefix = __L2SHOCK_TOKEN_PREFIX__;
    var axisIndex = __L2SHOCK_X_AXIS_INDEX__;
    /*__L2SHOCK_MERGE_GUARD__*/

    function asList(value) {
        if (Array.isArray(value)) {
            return value;
        }
        return value === null || value === undefined ? [] : [value];
    }

    function liveInstance() {
        try {
            if (typeof getElement === "function") {
                var component = getElement(chartId);
                if (component && component.chart) {
                    return component.chart;
                }
            }
        } catch (error) {}

        try {
            if (typeof echarts !== "undefined") {
                var node = document.getElementById("c" + String(chartId));
                if (node) {
                    return echarts.getInstanceByDom(node) || null;
                }
            }
        } catch (error) {}

        return null;
    }

    try {
        var instance = liveInstance();

        if (!instance) {
            return {ok: false, reason: "no live ECharts instance"};
        }

        if (typeof instance.isDisposed === "function" && instance.isDisposed()) {
            return {ok: false, reason: "ECharts instance is disposed"};
        }

        l2shockGuardInstance(instance, chartId);

        var option = instance.getOption() || {};
        var tokens = [];

        asList(option.series).forEach(function (item) {
            var name = (
                item && item.name !== null && item.name !== undefined
                    ? String(item.name)
                    : ""
            );
            if (name.indexOf(tokenPrefix) === 0) {
                tokens.push(name.slice(tokenPrefix.length));
            }
        });

        var axes = asList(option.xAxis);
        var axisPresent = axisIndex < axes.length;
        var categoryCount = null;

        if (
            axisPresent
            && axes[axisIndex]
            && Array.isArray(axes[axisIndex].data)
        ) {
            categoryCount = axes[axisIndex].data.length;
        }

        var dataZoom = null;

        asList(option.dataZoom).some(function (item) {
            if (
                item
                && typeof item.start === "number"
                && typeof item.end === "number"
            ) {
                dataZoom = {start: item.start, end: item.end};
                return true;
            }
            return false;
        });

        var applyRecord = null;

        try {
            var registry = window.__l2shockEchartApply;
            var entry = registry ? registry[String(chartId)] : null;
            if (entry) {
                applyRecord = {
                    token: String(entry.token || ""),
                    state: String(entry.state || ""),
                    error: entry.error ? String(entry.error) : null,
                    token_after: (
                        typeof entry.token_after === "boolean"
                            ? entry.token_after
                            : null
                    ),
                    series_after: (
                        typeof entry.series_after === "number"
                            ? entry.series_after
                            : null
                    ),
                    instance_uid: String(entry.instance_uid || ""),
                    after_error: entry.after_error ? String(entry.after_error) : null,
                    unwedged: (
                        Array.isArray(entry.unwedged) ? entry.unwedged.map(String) : []
                    ),
                    last_window_error: (
                        entry.last_window_error ? String(entry.last_window_error) : null
                    )
                };
            }

        var seriesList = asList(option.series);
        var seriesNames = seriesList.slice(0, 8).map(function (item) {
            return item && item.name !== null && item.name !== undefined
                ? String(item.name).slice(0, 48)
                : "";
        });
        } catch (error) {}

        return {
            ok: true,
            apply: applyRecord,
            series_count: seriesList.length,
            series_names: seriesNames,
            instance_uid: String(instance.__l2shockInstanceUid || ""),
            wedged_flags: (function () {
                var flags = [];
                for (var flagKey in instance) {
                    if (flagKey.indexOf("__flagIn") === 0 && instance[flagKey] === true) {
                        flags.push(flagKey);
                    }
                }
                return flags;
            })(),
            last_window_error: (
                window.__l2shockLastError ? String(window.__l2shockLastError) : null
            ),
            tokens: tokens,
            axis_present: axisPresent,
            category_count: categoryCount,
            width: instance.getWidth(),
            height: instance.getHeight(),
            data_zoom: dataZoom
        };
    } catch (error) {
        return {
            ok: false,
            reason: "probe failed: " + String(
                error && error.message ? error.message : error
            )
        };
    }
})()
"""


def _live_state_javascript_runner(
    chart: Any,
) -> tuple[Any, int] | None:
    """Return (client.run_javascript, element id), or None if unsupported."""
    element_id = getattr(chart, "id", None)

    if (
        isinstance(element_id, bool)
        or not isinstance(element_id, int)
        or element_id < 0
    ):
        return None

    try:
        # A deleted NiceGUI element raises instead of returning None.
        client = getattr(chart, "client", None)
    except Exception:
        return None

    runner = getattr(client, "run_javascript", None)

    if not callable(runner):
        return None

    return runner, element_id


def _live_state_probe_code(
    element_id: int,
    *,
    x_axis_index: int,
) -> str:
    return (
        _LIVE_STATE_PROBE_JS.replace(
            _MERGE_GUARD_MARKER,
            _MERGE_GUARD_JS,
        )
        .replace(
            "__L2SHOCK_CHART_ID__",
            json.dumps(element_id),
        )
        .replace(
            "__L2SHOCK_TOKEN_PREFIX__",
            json.dumps(ECHART_RENDER_TOKEN_SERIES_PREFIX),
        )
        .replace(
            "__L2SHOCK_X_AXIS_INDEX__",
            json.dumps(x_axis_index),
        )
    )


# Browser-side option application. The first-line marker identifies it.
# The option placeholder is replaced LAST, so option text is never scanned.
#
# run_javascript executes from a NiceGUI message handler, never inside an
# ECharts update cycle. An in-cycle flag that is already true here is stale:
# an earlier render threw, ECharts never reset it, and it now silently
# ignores every setOption and dispatchAction (including zoom). Such flags
# are reset before applying, and the result is verified, not assumed.
_APPLY_OPTION_JS: Final[str] = r"""/* l2shock:apply */
(function () {
    "use strict";
    var chartId = __L2SHOCK_CHART_ID__;
    var tokenSeriesName = __L2SHOCK_TOKEN_PREFIX__ + __L2SHOCK_RENDER_TOKEN__;
    /*__L2SHOCK_MERGE_GUARD__*/
    var registry = (
        window.__l2shockEchartApply
        || (window.__l2shockEchartApply = {})
    );
    var record = {
        token: __L2SHOCK_RENDER_TOKEN__,
        state: "pending",
        error: null,
        attempts: 0,
        unwedged: []
    };
    registry[String(chartId)] = record;

    function message(error) {
        return String(error && error.message ? error.message : error);
    }

    function installErrorCapture() {
        if (window.__l2shockErrorCapture === true) {
            return;
        }
        window.__l2shockErrorCapture = true;
        window.addEventListener("error", function (event) {
            try {
                window.__l2shockLastError = String(
                    event && event.message ? event.message : event
                ).slice(0, 300);
            } catch (error) {}
        });
        window.addEventListener("unhandledrejection", function (event) {
            try {
                window.__l2shockLastError = (
                    "unhandled rejection: " + message(event ? event.reason : null)
                ).slice(0, 300);
            } catch (error) {}
        });
    }

    function liveInstance() {
        try {
            if (typeof getElement === "function") {
                var component = getElement(chartId);
                if (component && component.chart) {
                    return component.chart;
                }
            }
        } catch (error) {}

        try {
            if (typeof echarts !== "undefined") {
                var node = document.getElementById("c" + String(chartId));
                if (node) {
                    return echarts.getInstanceByDom(node) || null;
                }
            }
        } catch (error) {}

        return null;
    }

    function unwedge(instance) {
        var cleared = [];
        for (var key in instance) {
            if (key.indexOf("__flagIn") === 0 && instance[key] === true) {
                instance[key] = false;
                cleared.push(key);
            }
        }
        if (cleared.length && instance.__pendingUpdate) {
            instance.__pendingUpdate = null;
            cleared.push("__pendingUpdate");
        }
        return cleared;
    }

    function ownsToken(instance) {
        var after = instance.getOption() || {};
        var names = (Array.isArray(after.series) ? after.series : []).map(
            function (item) {
                return item && item.name !== null && item.name !== undefined
                    ? String(item.name)
                    : "";
            }
        );
        record.series_after = names.length;
        record.token_after = names.indexOf(tokenSeriesName) >= 0;
        return record.token_after;
    }

    function buildOption() {
        return (__L2SHOCK_OPTION__);
    }

    installErrorCapture();

    var instance = liveInstance();

    if (
        !instance
        || (typeof instance.isDisposed === "function" && instance.isDisposed())
    ) {
        record.state = "no-instance";
        return record.state;
    }

    l2shockGuardInstance(instance, chartId);
    record.instance_uid = String(instance.__l2shockInstanceUid || "");
    record.unwedged = unwedge(instance);

    var errors = [];

    for (var attempt = 0; attempt < 2; attempt++) {
        record.attempts = attempt + 1;

        try {
            if (attempt > 0) {
                record.unwedged = record.unwedged.concat(unwedge(instance));
                instance.clear();
            }
            instance.setOption(buildOption(), {notMerge: true, lazyUpdate: false});
        } catch (error) {
            errors.push(message(error));
            continue;
        }

        try {
            if (ownsToken(instance)) {
                record.state = "applied";
                record.error = errors.length ? errors.join("; ") : null;
                return record.state;
            }
            errors.push("setOption returned but the token series is absent");
        } catch (error) {
            record.after_error = message(error);
            record.state = "applied";
            return record.state;
        }
    }

    record.state = "failed";
    record.error = errors.join("; ") || "unknown ECharts failure";
    record.last_window_error = window.__l2shockLastError || null;
    try {
        console.error("l2shock: ECharts did not apply the option:", record.error);
    } catch (error) {}
    return record.state;
})()
"""


def _apply_option_code(
    element_id: int,
    *,
    render_token: str,
    option_expression: str,
) -> str:
    return (
        _APPLY_OPTION_JS.replace(
            _MERGE_GUARD_MARKER,
            _MERGE_GUARD_JS,
        )
        .replace(
            "__L2SHOCK_CHART_ID__",
            json.dumps(element_id),
        )
        .replace(
            "__L2SHOCK_RENDER_TOKEN__",
            json.dumps(render_token),
        )
        .replace(
            "__L2SHOCK_TOKEN_PREFIX__",
            json.dumps(ECHART_RENDER_TOKEN_SERIES_PREFIX),
        )
        .replace(
            "__L2SHOCK_OPTION__",
            option_expression,
        )
    )


def _disable_nicegui_merge_update(chart: Any) -> None:
    """Make the apply script the only live ECharts writer for this widget.

    NiceGUI calls its update_chart() after every chart.update(), and that
    uses setOption(..., {notMerge: <series count changed>}). Its later write
    replaced our acknowledged generation. The stored options property is
    kept, so a re-mounted component still draws the current generation.
    """
    try:
        if getattr(chart, "_update_method", None) is not None:
            chart._update_method = None
    except Exception:
        log.debug("Could not disable NiceGUI update_chart.", exc_info=True)


def _probe_diagnostics(state: dict[str, Any]) -> str:
    """Summarise compact probe facts for an unacknowledged publication."""
    parts: list[str] = []

    count = state.get("series_count")
    if isinstance(count, int) and not isinstance(count, bool):
        parts.append(f"live series={count}")

    names = state.get("series_names")
    if isinstance(names, list) and names:
        parts.append("names=" + ",".join(str(name)[:32] for name in names[:6]))

    if state.get("instance_uid"):
        parts.append(f"instance={state['instance_uid']}")

    record = state.get("apply")
    if isinstance(record, dict):
        if record.get("token_after") is not None:
            parts.append(f"token right after apply={record['token_after']}")
        if record.get("series_after") is not None:
            parts.append(f"series after apply={record['series_after']}")
        if record.get("instance_uid"):
            parts.append(f"apply instance={record['instance_uid']}")
        if record.get("after_error"):
            parts.append(f"after-apply read error={record['after_error']}")
        unwedged = record.get("unwedged")
        if isinstance(unwedged, list) and unwedged:
            parts.append("reset stale ECharts flags=" + ",".join(map(str, unwedged)))

    wedged = state.get("wedged_flags")
    if isinstance(wedged, list) and wedged:
        parts.append("stuck ECharts flags=" + ",".join(map(str, wedged)))

    last_error = state.get("last_window_error")
    if not last_error and isinstance(record, dict):
        last_error = record.get("last_window_error")
    if last_error:
        parts.append(f"last browser error={str(last_error)[:200]}")

    return "; ".join(parts)


def _apply_outcome(
    state: dict[str, Any],
    token: str,
) -> tuple[str, str]:
    """Return (state, error) of the browser apply script for this token."""
    record = state.get("apply")

    if not isinstance(record, dict) or record.get("token") != token:
        return "not-run", ""

    return (
        str(record.get("state") or "unknown"),
        str(record.get("error") or ""),
    )


async def read_echart_live_state(
    chart: Any,
    *,
    x_axis_index: int = 0,
    timeout: float = 1.5,
) -> dict[str, Any] | None:
    """Read compact live browser chart facts.

    Returns ``None`` only when the widget cannot run the probe at all, so
    callers may fall back to legacy paths. Every browser or transport
    failure returns ``{"ok": False, "reason": ...}``.
    """
    target = _live_state_javascript_runner(chart)

    if target is None:
        return None

    if (
        isinstance(x_axis_index, bool)
        or not isinstance(x_axis_index, int)
        or x_axis_index < 0
    ):
        raise ValueError("x_axis_index must be a non-negative integer")

    runner, element_id = target
    code = _live_state_probe_code(
        element_id,
        x_axis_index=x_axis_index,
    )

    try:
        raw = await runner(
            code,
            timeout=max(0.1, float(timeout)),
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return {
            "ok": False,
            "reason": ("live-state probe did not answer: " f"{type(exc).__name__}"),
        }

    state = _decoded_method_result(raw)

    if not isinstance(state, dict):
        return {
            "ok": False,
            "reason": "live-state probe returned no state",
        }

    return state


def _current_render_token(chart: Any) -> str:
    return str(getattr(chart, "_l2shock_render_token", "") or "")


def _live_state_mismatch(
    state: dict[str, Any],
    publication: EChartPublication,
) -> str | None:
    """Return why a token-owning live state is not yet usable, or None."""
    expected_count = publication.expected_category_count

    if expected_count is not None:
        if state.get("axis_present") is not True:
            return "Browser chart lacks the expected x-axis."

        observed = state.get("category_count")

        if isinstance(observed, bool) or not isinstance(observed, int):
            observed = 0

        if observed != expected_count:
            return (
                "Browser category count differs from the published option: "
                f"expected={expected_count}, observed={observed}."
            )

    try:
        width = float(state.get("width"))
        height = float(state.get("height"))
    except TypeError, ValueError:
        return "Browser owns the option, but chart layout could not be verified."

    if not (
        math.isfinite(width)
        and math.isfinite(height)
        and width > 20.0
        and height > 20.0
    ):
        return (
            "Browser chart has no usable rendered layout: "
            f"width={width!r}, height={height!r}."
        )

    return None


async def confirm_echart_render_identity(
    chart: Any,
    publication: EChartPublication,
    *,
    x_axis_index: int = 0,
    attempts: int = 16,
    retry_delay_seconds: float = 0.125,
) -> tuple[bool, str]:
    """Confirm browser ownership of one exact publication generation.

    Real NiceGUI widgets are verified with a compact browser probe that
    returns token names, category count, and layout size only.
    """
    if chart is None:
        return False, "ECharts widget is unavailable."

    if not isinstance(publication, EChartPublication):
        raise TypeError("publication must be EChartPublication")

    if (
        isinstance(x_axis_index, bool)
        or not isinstance(x_axis_index, int)
        or x_axis_index < 0
    ):
        raise ValueError("x_axis_index must be a non-negative integer")

    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts <= 0:
        raise ValueError("attempts must be a positive integer")

    if _live_state_javascript_runner(chart) is None:
        return await _confirm_via_get_option(
            chart,
            publication,
            x_axis_index=x_axis_index,
            attempts=attempts,
            retry_delay_seconds=retry_delay_seconds,
        )

    resize = getattr(chart, "run_chart_method", None)
    retry_delay = max(0.0, float(retry_delay_seconds))
    token = publication.render_token
    last_reason = "Browser did not acknowledge the ECharts generation."

    for attempt in range(attempts):
        if _current_render_token(chart) != token:
            return (
                False,
                "The publication was superseded before browser acknowledgement.",
            )

        state = await read_echart_live_state(
            chart,
            x_axis_index=x_axis_index,
            timeout=1.5,
        )

        if not isinstance(state, dict) or state.get("ok") is not True:
            reason = state.get("reason") if isinstance(state, dict) else None
            last_reason = "Could not read the live browser ECharts state: " + str(
                reason or "unavailable"
            )
        else:
            tokens = state.get("tokens")

            if not isinstance(tokens, list) or token not in tokens:
                apply_state, apply_error = _apply_outcome(state, token)

                if apply_state == "failed":
                    # The apply script already reset stale flags and retried
                    # once after clear(); further polling cannot help.
                    diagnostics = _probe_diagnostics(state)
                    return (
                        False,
                        "Browser rejected the chart option: "
                        + (apply_error or "unknown ECharts error")
                        + (f" ({diagnostics})" if diagnostics else ""),
                    )

                diagnostics = _probe_diagnostics(state)
                last_reason = (
                    "Browser chart does not own the expected render token "
                    f"(apply: {apply_state}"
                    + (f"; {apply_error}" if apply_error else "")
                    + (f"; {diagnostics}" if diagnostics else "")
                    + ")."
                )
            else:
                mismatch = _live_state_mismatch(state, publication)

                if mismatch is not None:
                    last_reason = mismatch
                else:
                    if _current_render_token(chart) != token:
                        return (
                            False,
                            "The publication was superseded during "
                            "browser acknowledgement.",
                        )

                    setattr(
                        chart,
                        "_l2shock_acknowledged_render_token",
                        token,
                    )
                    return True, ""

        if attempt + 1 < attempts:
            if callable(resize):
                try:
                    await resize(
                        "resize",
                        timeout=1.0,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass

            await asyncio.sleep(retry_delay)

    return False, last_reason


def acknowledged_render_token(
    chart: Any,
) -> str:
    """Return the last browser-acknowledged token for one widget."""
    if chart is None:
        return ""

    return str(
        getattr(
            chart,
            "_l2shock_acknowledged_render_token",
            "",
        )
        or ""
    ).strip()


__all__ = [
    "ECHART_RENDER_TOKEN_SERIES_PREFIX",
    "EChartPublication",
    "EChartPublicationError",
    "acknowledged_render_token",
    "coerce_echart_option",
    "confirm_echart_render_identity",
    "empty_echart_option",
    "read_echart_live_state",
    "set_echart_options",
]

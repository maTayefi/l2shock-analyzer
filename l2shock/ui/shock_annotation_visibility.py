# l2shock/ui/shock_annotation_visibility.py
"""Display-only visibility of Shock-Start chart annotations.

A pure transform of an already-built ECharts option. It never touches the
L2 data, the price candles, the axes, or the chart metadata, and it never
changes which B area is selected. Hidden annotations are removed by
emptying ``markLine.data`` / ``markArea.data``; the input is not mutated.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def with_shock_annotation_visibility(
    option: dict[str, Any],
    *,
    show_lines_and_labels: bool,
    show_b_bands: bool,
) -> dict[str, Any]:
    """Return ``option`` with B/C lines, rank labels, and/or bands hidden.

    Both visible: the original object is returned unchanged (no copy).
    Otherwise a deep copy is returned; the caller's option stays intact so
    the annotations can be restored later without rebuilding the chart.
    """
    if not isinstance(option, dict):
        raise TypeError("option must be a dictionary")

    if show_lines_and_labels and show_b_bands:
        return option

    result = copy.deepcopy(option)
    series_list = result.get("series")

    if not isinstance(series_list, list):
        return result

    for series in series_list:
        if not isinstance(series, dict):
            continue

        mark_line = series.get("markLine")
        if not show_lines_and_labels and isinstance(mark_line, dict):
            # Lines and their "#N" / "#N C" labels live in the same items.
            mark_line["data"] = []

        mark_area = series.get("markArea")
        if not show_b_bands and isinstance(mark_area, dict):
            mark_area["data"] = []

    return result


class ShockDisplayTimezoneError(ValueError):
    """The configured display timezone cannot be used for chart labels."""


_TIMEZONE_NAME_RE = re.compile(r"^[A-Za-z0-9_+\-]+(?:/[A-Za-z0-9_+\-]+)*$")

# Shared JavaScript prelude. __TZ__ becomes a JSON string literal.
_TZ_PARTS_JS = r"""
    var zone = __TZ__;
    var cache = window.__l2shockTzFormats || (window.__l2shockTzFormats = {});
    if (!cache[zone]) {
        cache[zone] = new Intl.DateTimeFormat("en-GB", {
            timeZone: zone,
            hourCycle: "h23",
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit"
        });
    }
    function parts(raw) {
        var ms = typeof raw === "number" ? raw : Date.parse(raw);
        if (!isFinite(ms)) {
            return null;
        }
        var out = {};
        cache[zone].formatToParts(new Date(ms)).forEach(function (item) {
            out[item.type] = item.value;
        });
        return out;
    }
    function stamp(raw) {
        var p = parts(raw);
        if (!p) {
            return "";
        }
        return p.year + "-" + p.month + "-" + p.day + " "
            + p.hour + ":" + p.minute + ":" + p.second;
    }
"""

_AXIS_LABEL_JS = r"""function (value) {
__PARTS__
    var p = parts(value);
    if (!p) {
        return "";
    }
    return p.hour + ":" + p.minute + ":" + p.second
        + "\n" + p.year + "-" + p.month + "-" + p.day;
}"""

_AXIS_POINTER_JS = r"""function (params) {
__PARTS__
    return stamp(params && params.value);
}"""

_TOOLTIP_JS = r"""function (params) {
__PARTS__
    var list = Array.isArray(params) ? params : [params];
    if (!list.length || !list[0]) {
        return "";
    }
    function num(value) {
        return Number(value).toLocaleString("en-US", {
            maximumFractionDigits: 2
        });
    }
    var rows = [stamp(list[0].axisValue) + " (" + zone + ")"];
    list.forEach(function (item) {
        var value = item && item.value;
        if (!Array.isArray(value) || value.length !== 5) {
            return;
        }
        for (var i = 1; i < 5; i += 1) {
            if (value[i] === null || value[i] === undefined) {
                return;
            }
        }
        rows.push(
            item.marker + item.seriesName
            + "  O " + num(value[1]) + "  H " + num(value[4])
            + "  L " + num(value[3]) + "  C " + num(value[2])
        );
    });
    return rows.join("<br/>");
}"""


def _display_timezone(name: object) -> str:
    if not isinstance(name, str) or not _TIMEZONE_NAME_RE.fullmatch(name):
        raise ShockDisplayTimezoneError(
            "display timezone must be an IANA name such as Asia/Tehran"
        )

    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ShockDisplayTimezoneError(f"unknown display timezone: {name!r}") from exc

    return name


def shock_display_timezone_formatters(timezone_name: str) -> dict[str, str]:
    """JavaScript formatter sources for one IANA display timezone."""
    zone_literal = json.dumps(_display_timezone(timezone_name))

    def render(template: str) -> str:
        return template.replace("__PARTS__", _TZ_PARTS_JS).replace(
            "__TZ__", zone_literal
        )

    return {
        "axis_label": render(_AXIS_LABEL_JS),
        "axis_pointer": render(_AXIS_POINTER_JS),
        "tooltip": render(_TOOLTIP_JS),
    }


def with_shock_display_timezone(
    option: dict[str, Any],
    timezone_name: str,
) -> dict[str, Any]:
    """Show time-axis labels, crosshair label, and tooltip header locally.

    Only NiceGUI ``:formatter`` keys are added. Axis types, min/max, data,
    B bands, and metadata stay in UTC. ``option`` is never mutated.
    """
    if not isinstance(option, dict):
        raise TypeError("option must be a dictionary")

    formatters = shock_display_timezone_formatters(timezone_name)
    result = dict(option)

    raw_axes = option.get("xAxis")
    axes = (
        raw_axes
        if isinstance(raw_axes, list)
        else [raw_axes] if isinstance(raw_axes, dict) else []
    )
    new_axes: list[Any] = []

    for axis in axes:
        if not isinstance(axis, dict) or axis.get("type") != "time":
            new_axes.append(axis)
            continue

        copied = dict(axis)
        label = dict(copied.get("axisLabel") or {})
        label[":formatter"] = formatters["axis_label"]
        copied["axisLabel"] = label

        pointer = dict(copied.get("axisPointer") or {})
        pointer_label = dict(pointer.get("label") or {})
        pointer_label[":formatter"] = formatters["axis_pointer"]
        pointer["label"] = pointer_label
        copied["axisPointer"] = pointer

        new_axes.append(copied)

    if isinstance(raw_axes, list):
        result["xAxis"] = new_axes
    elif isinstance(raw_axes, dict):
        result["xAxis"] = new_axes[0]

    tooltip = option.get("tooltip")

    if isinstance(tooltip, dict):
        localized_tooltip = dict(tooltip)
        localized_tooltip[":formatter"] = formatters["tooltip"]
        result["tooltip"] = localized_tooltip

    return result


__all__ = [
    "ShockDisplayTimezoneError",
    "shock_display_timezone_formatters",
    "with_shock_annotation_visibility",
    "with_shock_display_timezone",
]

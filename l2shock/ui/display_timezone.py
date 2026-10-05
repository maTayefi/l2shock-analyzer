# l2shock/ui/display_timezone.py
"""Local-timezone axis labels, crosshair labels, and tooltips.

This module owns display-only timezone formatting for time-axis charts.

Axis types, UTC bounds, series coordinates, and analytical metadata are
unchanged. The transform adds NiceGUI ``:formatter`` keys without mutating
the input option.

It has no dependency on detection, rankings, candidate annotations, or
legacy Shock-Start modules.
"""

from __future__ import annotations

import json
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class DisplayTimezoneError(ValueError):
    """The configured display timezone cannot be used for chart labels."""


_TIMEZONE_NAME_RE = re.compile(r"^[A-Za-z0-9_+\-]+(?:/[A-Za-z0-9_+\-]+)*$")


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
        return Number(value).toLocaleString("en-US", {maximumFractionDigits: 4});
    }
    var rows = [stamp(list[0].axisValue) + " (" + zone + ")"];
    list.forEach(function (item) {
        var value = item && item.value;
        if (!Array.isArray(value)) {
            return;
        }
        if (/-extremeness$/.test(String((item && item.seriesId) || ""))) {
            if (value.length === 4 && value[2] !== null && value[2] !== undefined) {
                var side = {1: "bid-dominant", 2: "ask-dominant", 3: "both sides"}[value[3]] || "none";
                rows.push("&nbsp;&nbsp;Extremeness  " + Number(value[2]).toFixed(1)
                    + " / 100  (" + side + ")");
            }
            return;
        }
        if (value.length === 5) {
            for (var i = 1; i < 5; i += 1) {
                if (value[i] === null || value[i] === undefined) {
                    return;
                }
            }
            rows.push(item.marker + item.seriesName
                + "  O " + num(value[1]) + "  H " + num(value[4])
                + "  L " + num(value[3]) + "  C " + num(value[2]));
        } else if (value.length === 2 && value[1] !== null && value[1] !== undefined) {
            rows.push(item.marker + item.seriesName + "  " + num(value[1]));
        }
    });
    return rows.join("<br/>");
}"""


def _display_timezone(name: object) -> str:
    if not isinstance(name, str) or not _TIMEZONE_NAME_RE.fullmatch(name):
        raise DisplayTimezoneError(
            "display timezone must be an IANA name such as Asia/Tehran"
        )

    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise DisplayTimezoneError(f"unknown display timezone: {name!r}") from exc

    return name


def display_timezone_formatters(timezone_name: str) -> dict[str, str]:
    """Return validated JavaScript formatters for one display timezone."""
    zone = json.dumps(_display_timezone(timezone_name))

    def render(template: str) -> str:
        return template.replace("__PARTS__", _TZ_PARTS_JS).replace("__TZ__", zone)

    return {
        "axis_label": render(_AXIS_LABEL_JS),
        "axis_pointer": render(_AXIS_POINTER_JS),
        "tooltip": render(_TOOLTIP_JS),
    }


def with_display_timezone(
    option: dict[str, Any],
    timezone_name: str,
) -> dict[str, Any]:
    """Add local-time display formatters without changing UTC ownership."""
    if not isinstance(option, dict):
        raise TypeError("option must be a dictionary")

    formatters = display_timezone_formatters(timezone_name)
    result = dict(option)
    axes = option.get("xAxis")

    if isinstance(axes, list):
        new_axes = []

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

        result["xAxis"] = new_axes

    tooltip = option.get("tooltip")

    if isinstance(tooltip, dict):
        localized = dict(tooltip)
        localized[":formatter"] = formatters["tooltip"]
        result["tooltip"] = localized

    return result


__all__ = [
    "DisplayTimezoneError",
    "display_timezone_formatters",
    "with_display_timezone",
]

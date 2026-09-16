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
    expression = _javascript_option_expression(safe)

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

    try:
        runner(
            ":setOption",
            expression,
            "({notMerge: true, lazyUpdate: false})",
        )
    except Exception as exc:
        raise EChartPublicationError(
            "ECharts setOption(notMerge=True) publication failed"
        ) from exc

    setattr(
        chart,
        "_l2shock_render_token",
        publication.render_token,
    )
    setattr(
        chart,
        "_l2shock_acknowledged_render_token",
        "",
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


async def confirm_echart_render_identity(
    chart: Any,
    publication: EChartPublication,
    *,
    x_axis_index: int = 0,
    attempts: int = 16,
    retry_delay_seconds: float = 0.125,
) -> tuple[bool, str]:
    """Confirm browser ownership of one exact publication generation."""
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
    "set_echart_options",
]

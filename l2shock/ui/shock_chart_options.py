# l2shock/ui/shock_chart_options.py
"""Shared Shock-Start chart colors.

The former one-second category chart builder was retired in Batch 45A.
Every B-area row click now publishes through the bounded L2 viewport
(shock_view_chart_options). This module only owns the palette, so the
viewport chart, the color legend, the Top-N overlay, and the data-outage
warning regions all read one source of truth. Presentation only.
"""

from __future__ import annotations

from types import MappingProxyType

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
    # Persistent data-outage warning region (L2 or price). Presentation only.
    "data_outage": "rgba(220, 38, 38, 0.14)",
}

# Top-N rank hues for non-selected B areas on the bounded viewport. The
# selected area always keeps "b" / "b_area" (yellow). Bands use the same
# hue at low opacity; the legend reads these exact values.
_RANK_LINE_COLORS: tuple[str, ...] = (
    "#ff5252",
    "#18ffff",
    "#b2ff59",
    "#ff80ab",
    "#ffd180",
    "#8c9eff",
    "#a7ffeb",
    "#f4ff81",
)

for _rank, _rank_color in enumerate(_RANK_LINE_COLORS, start=1):
    _COLORS[f"rank_{_rank}"] = _rank_color

del _rank, _rank_color

SHOCK_RANK_COLOR_KEYS: tuple[str, ...] = tuple(
    f"rank_{rank}" for rank in range(1, len(_RANK_LINE_COLORS) + 1)
)
MAX_SHOCK_CHART_TOP_N: int = len(_RANK_LINE_COLORS)

# Read-only public view: the Shock-Start legend derives its swatches from
# exactly the colors the chart uses.
SHOCK_CHART_COLORS = MappingProxyType(_COLORS)


__all__ = [
    "MAX_SHOCK_CHART_TOP_N",
    "SHOCK_CHART_COLORS",
    "SHOCK_RANK_COLOR_KEYS",
]

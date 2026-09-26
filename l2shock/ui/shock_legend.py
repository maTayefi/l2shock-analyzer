# l2shock/ui/shock_legend.py
"""Color legend for the Shock-Start review chart.

Every swatch is read from ``SHOCK_CHART_COLORS``, the same mapping that
``build_shock_chart_options`` uses, so the legend cannot drift from the chart.
Presentation only: opening it never touches review or detection state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from nicegui import ui

from l2shock.ui.shock_chart_options import SHOCK_CHART_COLORS

SwatchKind = Literal["fill", "line", "dashed"]


@dataclass(frozen=True, slots=True)
class ShockLegendEntry:
    label: str
    color_key: str
    kind: SwatchKind
    meaning: str

    @property
    def color(self) -> str:
        return SHOCK_CHART_COLORS[self.color_key]


SHOCK_LEGEND_ENTRIES: tuple[ShockLegendEntry, ...] = (
    ShockLegendEntry(
        "Price candle up",
        "price_up",
        "fill",
        "Real one-second trade candle closed at or above its open. "
        "Optional context: missing price never moves the B area.",
    ),
    ShockLegendEntry(
        "Price candle down",
        "price_down",
        "fill",
        "Real one-second trade candle closed below its open.",
    ),
    ShockLegendEntry("Bid Liquidity", "bid", "line", "Per-second Bid L2 liquidity."),
    ShockLegendEntry("Ask Liquidity", "ask", "line", "Per-second Ask L2 liquidity."),
    ShockLegendEntry("Total Liquidity", "total", "line", "Per-second Bid plus Ask."),
    ShockLegendEntry("Order-Book Delta", "delta", "line", "Per-second Bid minus Ask."),
    ShockLegendEntry(
        "B start area",
        "b_area",
        "fill",
        "The detected B area, drawn on every panel. The band ends at the "
        "following second (exclusive), so a one-second B area stays visible.",
    ),
    ShockLegendEntry(
        "Representative B",
        "b",
        "dashed",
        "Representative B second, on every panel.",
    ),
    ShockLegendEntry(
        "Representative C",
        "c",
        "line",
        "Representative C second, on the Total panel.",
    ),
)

_NOTES: tuple[str, ...] = (
    "A break in an L2 line is a missing second, never zero liquidity.",
    "Highlights are presentation only; they never rerun or change the review.",
)


def shock_legend_rows() -> tuple[tuple[str, str, str], ...]:
    """Backward-compatible (label, color, meaning) rows."""
    return tuple(
        (entry.label, entry.color, entry.meaning) for entry in SHOCK_LEGEND_ENTRIES
    )


def _swatch(entry: ShockLegendEntry) -> None:
    if entry.kind == "fill":
        style = (
            "width:42px;height:16px;border:1px solid #64748b;"
            f"background:{entry.color};"
        )
    else:
        border = "dashed" if entry.kind == "dashed" else "solid"
        style = f"width:42px;height:0;border-top:3px {border} {entry.color};"

    ui.element("div").style(style)


def open_shock_color_legend() -> None:
    with ui.dialog() as dialog:
        with ui.card().classes("w-[56rem] max-w-full p-4"):
            with ui.row().classes("w-full items-center"):
                ui.label("Shock-Start - Color Legend").classes("text-xl font-bold")
                ui.space()
                ui.button("Close", on_click=dialog.close).props("flat")

            for entry in SHOCK_LEGEND_ENTRIES:
                with ui.row().classes("w-full items-center no-wrap gap-3"):
                    with ui.element("div").classes("w-12 flex justify-center"):
                        _swatch(entry)
                    ui.label(entry.label).classes("w-44 font-medium")
                    ui.label(entry.color).classes("w-48 font-mono text-xs")
                    ui.label(entry.meaning).classes("text-sm")

            for note in _NOTES:
                ui.label(note).classes("text-sm text-gray-500")

    dialog.open()


__all__ = [
    "SHOCK_LEGEND_ENTRIES",
    "ShockLegendEntry",
    "open_shock_color_legend",
    "shock_legend_rows",
]

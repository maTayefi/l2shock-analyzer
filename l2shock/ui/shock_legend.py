# l2shock/ui/shock_legend.py
"""Color legend for the Shock-Start inspection and viewport charts.

The colors are read from the chart builder itself, so the legend cannot
drift from what the chart actually draws.
"""

from __future__ import annotations

import html

from nicegui import ui

from l2shock.ui.shock_chart_options import _COLORS as _SHOCK_COLORS


def shock_legend_rows() -> tuple[tuple[str, str, str], ...]:
    """Return (item, color, meaning) rows. Pure; safe to unit-test."""
    colors = _SHOCK_COLORS

    return (
        (
            "Price candle up",
            colors["price_up"],
            "Real Binance Futures trade OHLC, close >= open. Optional "
            "context only; never used to detect or filter L2 shocks.",
        ),
        (
            "Price candle down",
            colors["price_down"],
            "Real Binance Futures trade OHLC, close < open. Seconds "
            "without trades stay empty rather than being filled.",
        ),
        (
            "Bid Liquidity",
            colors["bid"],
            "Verified one-second Bid depth-band liquidity.",
        ),
        (
            "Ask Liquidity",
            colors["ask"],
            "Verified one-second Ask depth-band liquidity.",
        ),
        (
            "Total Liquidity",
            colors["total"],
            "Bid + Ask at the same second. The master series for "
            "Shock-Start detection.",
        ),
        (
            "Order-Book Delta",
            colors["delta"],
            "Bid - Ask at the same second. Reported as derived "
            "evidence, not an independent vote.",
        ),
        (
            "B area (band)",
            colors["b_area"],
            "Candidate shock-start interval (one or a few seconds). "
            "Drawn on all five panels; it is an L2 result, not a "
            "price signal.",
        ),
        (
            "Representative B (dashed)",
            colors["b"],
            "Deterministic representative start second inside the B area.",
        ),
        (
            "Representative C (solid, Total panel)",
            colors["c"],
            "The Total-L2 extreme that B's leg leads to.",
        ),
    )


def _row_html(label: str, color: str, meaning: str) -> str:
    return (
        "<tr>"
        f"<td>{html.escape(label)}</td>"
        "<td>"
        '<span style="display:inline-block;width:42px;height:16px;'
        f'border:1px solid #64748b;background:{html.escape(color)};">'
        "</span>"
        "</td>"
        f"<td><code>{html.escape(color)}</code></td>"
        f"<td>{html.escape(meaning)}</td>"
        "</tr>"
    )


def open_shock_color_legend() -> None:
    table = (
        '<table style="width:100%;border-collapse:collapse;font-size:12px;">'
        "<thead><tr>"
        "<th>Item</th><th>Color</th><th>Code</th><th>Meaning</th>"
        "</tr></thead><tbody>"
        + "".join(_row_html(*row) for row in shock_legend_rows())
        + "</tbody></table>"
    )

    with ui.dialog() as dialog:
        with ui.card().classes("w-[56rem] max-w-full p-4"):
            with ui.row().classes("w-full items-center"):
                ui.label("Shock-Start \u2014 Color Legend").classes("text-xl font-bold")
                ui.space()
                ui.button("Close", on_click=dialog.close).props("flat")

            ui.label(
                "Gaps in any L2 panel are invalid or missing one-second L2 "
                "data; they are never drawn as zero liquidity. In the "
                "bounded viewport, a viewing bar containing any invalid "
                "second is empty in all four L2 panels."
            ).classes("text-sm text-gray-500")

            ui.html(table).classes("w-full")

    dialog.open()


__all__ = [
    "open_shock_color_legend",
    "shock_legend_rows",
]

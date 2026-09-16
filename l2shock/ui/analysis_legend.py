# l2shock/ui/analysis_legend.py
"""Color legend for the L2 Liquidity Shock Analysis workspace."""

from __future__ import annotations

import html

from nicegui import ui

from l2shock.ui.analysis_chart import ANALYSIS_CHART_COLORS


def _row(
    label: str,
    color: str,
    meaning: str,
) -> str:
    return (
        "<tr>"
        f"<td>{html.escape(label)}</td>"
        "<td>"
        f'<span style="display:inline-block;width:42px;height:16px;'
        f'border:1px solid #64748b;background:{html.escape(color)};">'
        "</span>"
        "</td>"
        f"<td><code>{html.escape(color)}</code></td>"
        f"<td>{html.escape(meaning)}</td>"
        "</tr>"
    )


def open_analysis_color_legend() -> None:
    colors = ANALYSIS_CHART_COLORS

    rows = [
        _row(
            "Price candle up",
            colors.price_up,
            "Real Binance Futures trade OHLC closed above/open.",
        ),
        _row(
            "Price candle down",
            colors.price_down,
            "Real Binance Futures trade OHLC closed below/open.",
        ),
        _row(
            "Bid Liquidity",
            colors.bid_line,
            "Exact reconstructed bid-side depth-band liquidity.",
        ),
        _row(
            "Ask Liquidity",
            colors.ask_line,
            "Exact reconstructed ask-side depth-band liquidity.",
        ),
        _row(
            "Total Liquidity",
            colors.total_line,
            "Bid Liquidity plus Ask Liquidity.",
        ),
        _row(
            "Positive imbalance",
            colors.imbalance_positive,
            "Bid Liquidity exceeds Ask Liquidity.",
        ),
        _row(
            "Negative imbalance",
            colors.imbalance_negative,
            "Ask Liquidity exceeds Bid Liquidity.",
        ),
        _row(
            "Upward LM",
            "rgba(37,99,235,0.18)",
            "Selected upward Liquidity Movement highlight.",
        ),
        _row(
            "Downward LM",
            "rgba(220,38,38,0.18)",
            "Selected downward Liquidity Movement highlight.",
        ),
        _row(
            "Timeline discontinuity",
            colors.discontinuity_fill,
            "Filtered or unavailable elapsed time was compressed.",
        ),
        _row(
            "Persistent data warning",
            colors.invalid_warning_fill,
            "Long invalid L2 or real-price interval.",
        ),
    ]

    table = (
        '<table style="width:100%;border-collapse:collapse;'
        'font-size:12px;">'
        "<thead><tr>"
        "<th>Item</th><th>Color</th><th>Code</th><th>Meaning</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )

    with ui.dialog().props("maximized") as dialog:
        with ui.card().classes("w-full h-full p-4 overflow-auto"):
            with ui.row().classes("w-full items-center"):
                ui.label("Liquidity Shock Analyzer — Color Legend").classes(
                    "text-xl font-bold"
                )
                ui.space()
                ui.button(
                    "Close",
                    on_click=dialog.close,
                ).props("flat")

            ui.label(
                "Highlight visibility is presentation-only. "
                "Turning a layer off never reruns or changes LM analysis."
            ).classes("text-sm text-gray-500")

            ui.html(table).classes("w-full")

    dialog.open()


__all__ = ["open_analysis_color_legend"]

# l2shock/analysis/shock_window.py
"""One-second L2 values handed to Shock-Start viewing bars.

ShockWindowSecond is the per-second input of build_shock_view_bars and
build_shock_dataset_view. This module owns neither chart-timeframe selection
nor NiceGUI/ECharts state. It never reads price. Invalid L2 seconds remain
explicit null values so a renderer cannot mistake missing coverage for zero
liquidity.

The padded one-second B-area window builder was retired in Batch 45B
together with the one-second category chart it fed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from fractions import Fraction


class ShockWindowError(ValueError):
    """An L2 value cannot be represented as a chart coordinate."""


def _exact_text(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def _chart_number(value: Fraction) -> float:
    """Convert only browser coordinates; retain exact values separately."""
    try:
        converted = float(value)
    except (OverflowError, ValueError) as exc:
        raise ShockWindowError(
            "L2 value cannot be represented as a chart coordinate"
        ) from exc

    if not math.isfinite(converted):
        raise ShockWindowError(
            "L2 value cannot be represented as a finite chart coordinate"
        )

    return converted


@dataclass(frozen=True, slots=True)
class ShockWindowSecond:
    """One dataset-owned second, including an explicit invalid-data slot."""

    dataset_index: int
    timestamp_utc: datetime
    valid_l2: bool
    bid: Fraction | None
    ask: Fraction | None
    total: Fraction | None
    delta: Fraction | None

    def to_dict(self) -> dict[str, object]:
        values = {
            "bid": self.bid,
            "ask": self.ask,
            "total": self.total,
            "delta": self.delta,
        }

        return {
            "dataset_index": self.dataset_index,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "valid_l2": self.valid_l2,
            # Floats are for ECharts coordinates only. An invalid observation
            # is null, never zero and never a carried-forward value.
            "plot": {
                name: _chart_number(value) if value is not None else None
                for name, value in values.items()
            },
            # Exact rational text is suitable for tooltips and exports.
            "exact": {
                name: _exact_text(value) if value is not None else None
                for name, value in values.items()
            },
        }


__all__ = [
    "ShockWindowError",
    "ShockWindowSecond",
]

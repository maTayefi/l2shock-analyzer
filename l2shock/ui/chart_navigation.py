# l2shock/ui/chart_navigation.py
"""LM-independent chart navigation primitives.

``ChartNavigationWindow`` and the selected-focus series prefix are used by
``chart_interactions.py``. They live here so the chart controller does not
import the LM chart builder (``analysis_chart.py``), which is being retired.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX: Final[str] = "l2shock-selected-focus-"


class ChartNavigationError(ValueError):
    """A chart navigation window is internally inconsistent."""


@dataclass(frozen=True, slots=True)
class ChartNavigationWindow:
    """Category-index window suitable for table-to-chart navigation."""

    start_index: int
    end_index: int
    candidate_start_index: int
    candidate_end_index: int

    def __post_init__(self) -> None:
        for name in (
            "start_index",
            "end_index",
            "candidate_start_index",
            "candidate_end_index",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ChartNavigationError(f"{name} must be a non-negative integer")

        if self.end_index < self.start_index:
            raise ChartNavigationError("Navigation end cannot precede start")

        if not (
            self.start_index
            <= self.candidate_start_index
            <= self.candidate_end_index
            <= self.end_index
        ):
            raise ChartNavigationError(
                "Candidate navigation extent must lie inside its window"
            )


__all__ = [
    "ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX",
    "ChartNavigationError",
    "ChartNavigationWindow",
]
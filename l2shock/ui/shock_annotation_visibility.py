# l2shock/ui/shock_annotation_visibility.py
"""Display-only visibility of Shock-Start chart annotations.

A pure transform of an already-built ECharts option. It never touches the
L2 data, the price candles, the axes, or the chart metadata, and it never
changes which B area is selected. Hidden annotations are removed by
emptying ``markLine.data`` / ``markArea.data``; the input is not mutated.
"""

from __future__ import annotations

import copy
from typing import Any


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


__all__ = ["with_shock_annotation_visibility"]

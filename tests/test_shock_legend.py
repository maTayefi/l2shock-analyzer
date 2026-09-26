# tests/test_shock_legend.py
from __future__ import annotations

import pytest

from l2shock.ui.shock_chart_options import SHOCK_CHART_COLORS
from l2shock.ui.shock_legend import SHOCK_LEGEND_ENTRIES, open_shock_color_legend


def test_legend_colors_come_from_chart_colors() -> None:
    for entry in SHOCK_LEGEND_ENTRIES:
        assert entry.color == SHOCK_CHART_COLORS[entry.color_key]


def test_legend_covers_every_drawn_chart_color() -> None:
    drawn = set(SHOCK_CHART_COLORS) - {"ab", "bc"}  # defined but not drawn
    assert {entry.color_key for entry in SHOCK_LEGEND_ENTRIES} == drawn


def test_legend_labels_are_unique() -> None:
    labels = [entry.label for entry in SHOCK_LEGEND_ENTRIES]
    assert len(labels) == len(set(labels))


def test_chart_colors_are_read_only() -> None:
    with pytest.raises(TypeError):
        SHOCK_CHART_COLORS["bid"] = "#000000"  # type: ignore[index]


def test_open_legend_is_callable() -> None:
    assert callable(open_shock_color_legend)

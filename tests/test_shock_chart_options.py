# tests/test_shock_chart_options.py
"""Shock-Start palette and bounded-viewport panel styling.

The one-second category chart builder was retired in Batch 45A; row clicks
publish through build_shock_view_chart_options only.
"""

from datetime import datetime, timedelta, timezone
from fractions import Fraction

import pytest

from l2shock.analysis.shock_window import ShockWindowSecond
from l2shock.ui.shock_chart_options import (
    MAX_SHOCK_CHART_TOP_N,
    SHOCK_CHART_COLORS,
    SHOCK_RANK_COLOR_KEYS,
)
from l2shock.ui.shock_view_bars import build_shock_view_bars
from l2shock.ui.shock_view_chart_options import build_shock_view_chart_options


def _viewport_projection(
    *,
    invalid_positions: frozenset[int] = frozenset(),
):
    origin = datetime(2026, 9, 24, 2, 6, 58, tzinfo=timezone.utc)
    seconds = []
    for position in range(8):
        valid = position not in invalid_positions
        bid = Fraction(10 + position) if valid else None
        ask = Fraction(30 - position) if valid else None
        seconds.append(
            ShockWindowSecond(
                dataset_index=100 + position,
                timestamp_utc=(origin + timedelta(seconds=position)),
                valid_l2=valid,
                bid=bid,
                ask=ask,
                total=bid + ask if valid else None,
                delta=bid - ask if valid else None,
            )
        )
    return build_shock_view_bars(
        tuple(seconds),
        timeframe_seconds=5,
        max_bars=3,
    )


def _viewport_options(projection):
    return build_shock_view_chart_options(
        projection,
        b_first_dataset_index=101,
        b_last_dataset_index=103,
        representative_b_dataset_index=102,
        representative_c_dataset_index=106,
    )


def test_viewport_panels_hide_horizontal_y_gridlines():
    options = _viewport_options(_viewport_projection())
    assert len(options["yAxis"]) == 5
    assert all(axis["splitLine"]["show"] is False for axis in options["yAxis"])


def test_palette_is_read_only_and_rank_keys_match_top_n():
    assert len(SHOCK_RANK_COLOR_KEYS) == MAX_SHOCK_CHART_TOP_N
    assert all(key in SHOCK_CHART_COLORS for key in SHOCK_RANK_COLOR_KEYS)

    for key in ("price_up", "price_down", "bid", "ask", "total", "delta"):
        assert key in SHOCK_CHART_COLORS

    for key in ("b", "c", "b_area", "data_outage"):
        assert key in SHOCK_CHART_COLORS

    with pytest.raises(TypeError):
        SHOCK_CHART_COLORS["b"] = "#000000"  # type: ignore[index]

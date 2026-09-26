# tests/test_shock_top_n_and_quality.py
from __future__ import annotations

import dataclasses
import inspect
from datetime import datetime, timedelta, timezone
from fractions import Fraction

import pytest

from l2shock.analysis.aggregation import AggregatedL2Bar
from l2shock.analysis.shock_review import (
    SHOCK_REVIEW_ORDER_VERSION,
    SHOCK_REVIEW_SCHEMA_VERSION,
    ReviewedShockArea,
    _bc_path_metrics,
    _bc_sharpness_squared,
    _endpoint_extremeness,
)
from l2shock.ui.shock_chart_options import (
    MAX_SHOCK_CHART_TOP_N,
    SHOCK_CHART_COLORS,
    SHOCK_RANK_COLOR_KEYS,
)
from l2shock.ui.shock_legend import SHOCK_LEGEND_ENTRIES
from l2shock.ui.shock_view_bars import ShockViewBar, ShockViewProjection
from l2shock.ui.shock_view_chart_options import (
    ShockViewChartError,
    ShockViewRankedArea,
    build_shock_view_chart_options,
    rank_color_key,
)
from l2shock.ui.shock_view_selection import build_shock_view_selection

_START = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)


def _projection(first_index: int = 100, seconds: int = 60) -> ShockViewProjection:
    flat = (1.0, 1.0, 1.0, 1.0)
    bars = tuple(
        ShockViewBar(
            start_utc=_START + timedelta(seconds=i),
            end_utc_exclusive=_START + timedelta(seconds=i + 1),
            first_source_position=i,
            last_source_position_exclusive=i + 1,
            first_dataset_index=first_index + i,
            last_dataset_index=first_index + i,
            valid_l2=True,
            bid=flat,
            ask=flat,
            total=(2.0, 2.0, 2.0, 2.0),
            delta=(0.0, 0.0, 0.0, 0.0),
        )
        for i in range(seconds)
    )
    return ShockViewProjection(
        timeframe_seconds=1,
        requested_max_bars=1200,
        source_start_utc=_START,
        source_end_utc_exclusive=_START + timedelta(seconds=seconds),
        bars=bars,
    )


def _options(**kwargs):
    return build_shock_view_chart_options(
        _projection(),
        b_first_dataset_index=110,
        b_last_dataset_index=112,
        representative_b_dataset_index=111,
        representative_c_dataset_index=120,
        **kwargs,
    )


def test_review_order_and_schema_are_version_two() -> None:
    assert SHOCK_REVIEW_ORDER_VERSION == "total_structure_first_v2"
    assert SHOCK_REVIEW_SCHEMA_VERSION == 2


def test_new_review_fields_have_safe_defaults() -> None:
    names = {field.name for field in dataclasses.fields(ReviewedShockArea)}
    assert {
        "total_bc_seconds",
        "total_bc_sharpness_squared",
        "total_bc_adverse_move_count",
        "total_bc_adverse_total_fraction",
        "total_bc_max_retracement_fraction",
        "total_b_extremeness",
        "total_c_extremeness",
    } <= names


def test_bc_path_metrics_upward_and_downward() -> None:
    up = tuple(Fraction(v) for v in (0, 5, 2, 1, 20))
    assert _bc_path_metrics(up) == (2, Fraction(1, 5), Fraction(1, 5))

    clean = tuple(Fraction(v) for v in (0, 4, 9, 20))
    assert _bc_path_metrics(clean) == (0, Fraction(0), Fraction(0))

    down = tuple(Fraction(v) for v in (20, 10, 12, 0))
    assert _bc_path_metrics(down) == (1, Fraction(1, 10), Fraction(1, 10))


def test_bc_path_metrics_rejects_flat_or_short_legs() -> None:
    with pytest.raises(ValueError):
        _bc_path_metrics((Fraction(1),))
    with pytest.raises(ValueError):
        _bc_path_metrics((Fraction(1), Fraction(3), Fraction(1)))


def test_sqrt_sharpness_is_exact_and_penalizes_duration() -> None:
    fast = _bc_sharpness_squared(Fraction(1, 5), 4)
    slow = _bc_sharpness_squared(Fraction(1, 5), 16)
    assert fast == Fraction(1, 100)
    assert fast == 4 * slow


def test_endpoint_extremeness_is_direction_oriented() -> None:
    low, high = Fraction(100), Fraction(200)
    assert _endpoint_extremeness(
        b_total=low, c_total=high, scan_min=low, scan_max=high
    ) == (Fraction(1), Fraction(1))
    assert _endpoint_extremeness(
        b_total=high, c_total=low, scan_min=low, scan_max=high
    ) == (Fraction(1), Fraction(1))
    assert _endpoint_extremeness(
        b_total=Fraction(150), c_total=Fraction(175), scan_min=low, scan_max=high
    ) == (Fraction(1, 2), Fraction(3, 4))


def test_rank_colors_are_in_chart_colors_and_legend() -> None:
    assert len(SHOCK_RANK_COLOR_KEYS) == MAX_SHOCK_CHART_TOP_N
    legend_keys = {entry.color_key for entry in SHOCK_LEGEND_ENTRIES}

    for key in SHOCK_RANK_COLOR_KEYS:
        assert SHOCK_CHART_COLORS[key].startswith("#")
        assert key in legend_keys

    assert rank_color_key(1) == "rank_1"
    assert rank_color_key(MAX_SHOCK_CHART_TOP_N + 1) == "rank_1"


def test_no_ranked_areas_keeps_previous_annotation_shape() -> None:
    options = _options()
    assert [len(s["markLine"]["data"]) for s in options["series"]] == [1, 1, 1, 2, 1]
    assert all(len(s["markArea"]["data"]) == 1 for s in options["series"])
    assert "label" not in options["series"][0]["markLine"]["data"][0]


def test_ranked_areas_get_colored_bands_and_rank_labels() -> None:
    options = _options(
        selected_rank=3,
        ranked_areas=(
            ShockViewRankedArea(1, 130, 131, 130, 140),
            ShockViewRankedArea(2, 500, 501, 500, 510),  # offscreen
        ),
    )
    price, total = options["series"][0], options["series"][3]

    assert len(price["markArea"]["data"]) == 2
    rank_band = price["markArea"]["data"][1][0]
    assert rank_band["itemStyle"]["color"].startswith("rgba(")
    assert rank_band["itemStyle"]["color"].endswith(", 0.12)")

    labels = [
        item["label"]["formatter"]
        for item in total["markLine"]["data"]
        if "label" in item
    ]
    assert labels == ["#3", "#3 C", "#1", "#1 C"]

    price_labels = [
        item["label"]["formatter"]
        for item in price["markLine"]["data"]
        if "label" in item
    ]
    assert price_labels == ["#3", "#1"]
    assert total["markLine"]["data"][2]["lineStyle"]["color"] == (
        SHOCK_CHART_COLORS["rank_1"]
    )


def test_ranked_area_validation() -> None:
    with pytest.raises(ShockViewChartError):
        ShockViewRankedArea(0, 130, 131, 130)

    with pytest.raises(ShockViewChartError):
        _options(
            selected_rank=1,
            ranked_areas=(ShockViewRankedArea(1, 130, 131, 130, 140),),
        )


def test_selection_top_n_defaults_to_no_overlay() -> None:
    parameter = inspect.signature(build_shock_view_selection).parameters["top_n"]
    assert parameter.default == 0


def test_aggregated_l2_bar_keeps_l2_ohlc_fields() -> None:
    names = {field.name for field in dataclasses.fields(AggregatedL2Bar)}
    assert {"bid_ohlc", "ask_ohlc", "total_ohlc", "delta_ohlc"} <= names

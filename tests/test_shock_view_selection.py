# tests/test_shock_view_selection.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from l2shock.ingest.sampling import BookSampleQuality
from l2shock.ui.shock_view_bars import ShockViewBarsError
from l2shock.ui.shock_view_selection import (
    build_shock_view_selection,
)

_ORIGIN = datetime(2026, 9, 24, 2, 6, 58, tzinfo=timezone.utc)


def _review(count: int = 20):
    seconds = tuple(
        SimpleNamespace(
            timestamp_utc=(_ORIGIN + timedelta(seconds=index)),
            quality=BookSampleQuality.VALID,
            coverage_degraded=False,
            bid_liquidity=10 + index,
            ask_liquidity=30 - index,
        )
        for index in range(count)
    )
    representative = SimpleNamespace(
        b_index=8,
        c_index=12,
    )
    area = SimpleNamespace(
        first_b_index=7,
        last_b_index=9,
        representative=representative,
    )
    entry = SimpleNamespace(
        inspection_position=1,
        area=area,
    )
    scan = SimpleNamespace(
        scan_id="scan-example",
        dataset=SimpleNamespace(seconds=seconds),
    )

    return SimpleNamespace(
        review_id="review-example",
        ordered_areas=(entry,),
        evidence_result=SimpleNamespace(candidate_scan=scan),
    )


def test_selection_publishes_exact_time_annotations_and_metadata():
    selection = build_shock_view_selection(
        _review(),
        1,
        source_seconds=12,
        seconds_before_b=5,
        timeframe_seconds=5,
        max_bars=4,
    )

    assert selection.owner_id == ("shock:review-example:1")
    assert selection.source_seconds == 12
    assert selection.timeframe_seconds == 5

    option = selection.option
    metadata = option["l2shockChartMetadata"]

    assert metadata["analysis_id"] == "review-example"
    assert metadata["dataset_analysis_id"] == ("scan-example")
    assert metadata["activity_timeframe"] == "1s"
    assert metadata["bar_duration_seconds"] == 5
    assert metadata["visible_bar_count"] == (selection.displayed_bars)
    assert len(metadata["source_bar_indices"]) == (selection.displayed_bars)

    expected_start = (_ORIGIN + timedelta(seconds=7)).isoformat()
    expected_end = (_ORIGIN + timedelta(seconds=10)).isoformat()

    for series in option["series"]:
        band = series["markArea"]["data"][0]
        assert band[0]["xAxis"] == expected_start
        assert band[1]["xAxis"] == expected_end

    assert option["series"][0]["data"] == []


def test_viewport_at_dataset_start_clamps_without_shifting_b():
    selection = build_shock_view_selection(
        _review(),
        1,
        source_seconds=10,
        seconds_before_b=100,
        timeframe_seconds=5,
        max_bars=3,
    )

    assert selection.source_seconds == 10
    assert (
        selection.option["series"][3]["markArea"]["data"][0][0]["xAxis"]
        == (_ORIGIN + timedelta(seconds=7)).isoformat()
    )


def test_requested_explicit_timeframe_must_fit_budget():
    with pytest.raises(
        ShockViewBarsError,
        match="exceeds max_bars",
    ):
        build_shock_view_selection(
            _review(),
            1,
            source_seconds=15,
            seconds_before_b=5,
            timeframe_seconds=1,
            max_bars=5,
        )


@pytest.mark.parametrize(
    "value",
    [0, -1, 86_401, True, 1.5],
)
def test_invalid_source_second_counts_are_rejected(
    value: object,
):
    with pytest.raises(
        ShockViewBarsError,
        match="source_seconds",
    ):
        build_shock_view_selection(
            _review(),
            1,
            source_seconds=value,
        )


def test_number_control_integral_floats_are_accepted():
    selection = build_shock_view_selection(
        _review(),
        1,
        source_seconds=10.0,
        seconds_before_b=5.0,
        timeframe_seconds=5,
        max_bars=3,
    )

    assert selection.source_seconds == 10


def test_auto_budget_changes_granularity_not_source_interval():
    review = _review(2002)

    tighter = build_shock_view_selection(
        review,
        1,
        source_seconds=2002,
        seconds_before_b=100,
        timeframe_seconds=None,
        max_bars=400,
    )
    looser = build_shock_view_selection(
        review,
        1,
        source_seconds=2002,
        seconds_before_b=100,
        timeframe_seconds=None,
        max_bars=401,
    )

    assert tighter.source_seconds == 2002
    assert looser.source_seconds == 2002
    assert tighter.timeframe_seconds == 15
    assert looser.timeframe_seconds == 5
    assert tighter.displayed_bars <= 400
    assert looser.displayed_bars <= 401

    for selection in (tighter, looser):
        axes = selection.option["xAxis"]
        assert all(axis["min"] == _ORIGIN.isoformat() for axis in axes)
        assert all(
            axis["max"] == (_ORIGIN + timedelta(seconds=2002)).isoformat()
            for axis in axes
        )

        band = selection.option["series"][3]["markArea"]["data"][0]
        assert band[0]["xAxis"] == (_ORIGIN + timedelta(seconds=7)).isoformat()
        assert band[1]["xAxis"] == (_ORIGIN + timedelta(seconds=10)).isoformat()


@pytest.mark.parametrize(
    "budget",
    [0, -1, 5001, True, 2.5],
)
def test_invalid_user_bar_budget_is_rejected(
    budget: object,
):
    with pytest.raises(
        ShockViewBarsError,
        match="max_bars",
    ):
        build_shock_view_selection(
            _review(),
            1,
            max_bars=budget,
        )

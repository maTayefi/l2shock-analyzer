# tests/test_shock_view_chart_options.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from fractions import Fraction

import pytest

from l2shock.analysis.shock_window import ShockWindowSecond
from l2shock.ui.shock_view_bars import (
    build_shock_view_bars,
)
from l2shock.ui.shock_view_chart_options import (
    ShockViewChartError,
    build_shock_view_chart_options,
    project_shock_view_anchor_times,
)

_ORIGIN = datetime(2026, 9, 24, 2, 6, 58, tzinfo=timezone.utc)


def _projection(
    *,
    invalid_positions: frozenset[int] = frozenset(),
):
    seconds = []

    for position in range(8):
        valid = position not in invalid_positions
        bid = Fraction(10 + position) if valid else None
        ask = Fraction(30 - position) if valid else None

        seconds.append(
            ShockWindowSecond(
                dataset_index=100 + position,
                timestamp_utc=(_ORIGIN + timedelta(seconds=position)),
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


def _options(projection):
    return build_shock_view_chart_options(
        projection,
        b_first_dataset_index=101,
        b_last_dataset_index=103,
        representative_b_dataset_index=102,
        representative_c_dataset_index=106,
    )


def test_b_band_keeps_exact_seconds_inside_five_second_bars():
    options = _options(_projection())
    expected_start = (_ORIGIN + timedelta(seconds=1)).isoformat()
    expected_end = (_ORIGIN + timedelta(seconds=4)).isoformat()
    expected_b = (_ORIGIN + timedelta(seconds=2)).isoformat()
    expected_c = (_ORIGIN + timedelta(seconds=6)).isoformat()

    assert len(options["xAxis"]) == 5
    assert all(axis["type"] == "time" for axis in options["xAxis"])
    assert len(options["series"]) == 5

    for series in options["series"]:
        band = series["markArea"]["data"][0]
        assert band[0]["xAxis"] == expected_start
        assert band[1]["xAxis"] == expected_end
        assert series["markLine"]["data"][0]["xAxis"] == (expected_b)

    assert options["series"][3]["markLine"]["data"][1]["xAxis"] == expected_c
    assert [len(series["markLine"]["data"]) for series in options["series"]] == [
        1,
        1,
        1,
        2,
        1,
    ]

    # Neither the B start nor its exclusive end is snapped to
    # the UTC-aligned five-second viewing-bar boundaries.
    assert expected_start not in {
        bar.start_utc.isoformat() for bar in _projection().bars
    }


def test_echarts_candle_order_is_explicit():
    options = _options(_projection())
    bid = options["series"][1]

    assert bid["data"][0] == [
        datetime(
            2026,
            9,
            24,
            2,
            6,
            55,
            tzinfo=timezone.utc,
        ).isoformat(),
        10.0,
        11.0,
        10.0,
        11.0,
    ]

    assert bid["data"][1][1:] == [
        12.0,
        16.0,
        12.0,
        16.0,
    ]


def test_invalid_source_second_nulls_all_l2_candles_in_its_bar():
    options = _options(_projection(invalid_positions=frozenset({1})))

    for series in options["series"][1:]:
        assert series["data"][0][1:] == [None, None, None, None]
        assert series["data"][1][1] is not None


def test_price_is_empty_without_changing_l2_or_b_coordinates():
    plain = _options(_projection())
    invalid = _options(_projection(invalid_positions=frozenset({1})))

    assert plain["series"][0]["data"] == []
    assert invalid["series"][0]["data"] == []

    for index in range(5):
        assert (
            plain["series"][index]["markArea"] == invalid["series"][index]["markArea"]
        )


def test_partly_visible_b_is_clipped_to_source_viewport_not_bar():
    projection = _projection()

    anchors = project_shock_view_anchor_times(
        projection,
        b_first_dataset_index=98,
        b_last_dataset_index=102,
        representative_b_dataset_index=99,
        representative_c_dataset_index=106,
    )

    assert anchors.b_start_utc == (projection.source_start_utc)
    assert anchors.b_end_utc_exclusive == (_ORIGIN + timedelta(seconds=3))
    assert anchors.representative_b_utc is None
    assert anchors.representative_c_utc == (_ORIGIN + timedelta(seconds=6))


def test_offscreen_b_does_not_produce_chart_annotations():
    projection = _projection()

    options = build_shock_view_chart_options(
        projection,
        b_first_dataset_index=10,
        b_last_dataset_index=12,
        representative_b_dataset_index=11,
        representative_c_dataset_index=106,
    )

    for series in options["series"]:
        assert series["markArea"]["data"] == []
        assert series["markLine"]["data"] == []


def test_b_ending_at_last_source_second_uses_exclusive_end():
    projection = _projection()

    anchors = project_shock_view_anchor_times(
        projection,
        b_first_dataset_index=107,
        b_last_dataset_index=107,
        representative_b_dataset_index=107,
    )

    assert anchors.b_start_utc == (_ORIGIN + timedelta(seconds=7))
    assert anchors.b_end_utc_exclusive == (projection.source_end_utc_exclusive)


@pytest.mark.parametrize(
    ("first", "last", "representative"),
    [
        (-1, 2, 1),
        (103, 102, 102),
        (101, 103, 104),
        (True, 103, 102),
    ],
)
def test_invalid_b_coordinates_are_rejected(
    first: object,
    last: object,
    representative: object,
):
    with pytest.raises(ShockViewChartError):
        project_shock_view_anchor_times(
            _projection(),
            b_first_dataset_index=first,
            b_last_dataset_index=last,
            representative_b_dataset_index=representative,
        )

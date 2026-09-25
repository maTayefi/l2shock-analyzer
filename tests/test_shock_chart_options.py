# tests/test_shock_chart_options.py
from datetime import datetime, timedelta, timezone
from fractions import Fraction
import pytest
from l2shock.analysis.shock_window import (
    ShockAreaWindow,
    ShockWindowSecond,
)
from l2shock.ui.shock_chart_options import (
    ShockChartOptionsError,
    build_shock_chart_options,
)
from l2shock.ui.shock_view_bars import build_shock_view_bars
from l2shock.ui.shock_view_chart_options import build_shock_view_chart_options


def _window() -> ShockAreaWindow:
    origin = datetime(2026, 9, 24, 2, 6, 15, tzinfo=timezone.utc)
    seconds = []

    for index in range(6):
        valid = index != 4
        bid = Fraction(10 + index) if valid else None
        ask = Fraction(3) if valid else None

        seconds.append(
            ShockWindowSecond(
                dataset_index=100 + index,
                timestamp_utc=origin + timedelta(seconds=index),
                valid_l2=valid,
                bid=bid,
                ask=ask,
                total=bid + ask if valid else None,
                delta=bid - ask if valid else None,
            )
        )

    return ShockAreaWindow(
        review_id="review-example",
        inspection_position=1,
        direction="up",
        first_dataset_index=100,
        last_dataset_index=105,
        b_first_position=2,
        b_last_position=2,
        representative_a_position=1,
        representative_b_position=2,
        representative_c_position=5,
        member_abc_positions=((1, 2, 5),),
        seconds=tuple(seconds),
    )


def test_five_panels_share_full_l2_axis_without_price():
    window = _window()
    options = build_shock_chart_options(window)

    assert len(options["grid"]) == 5
    assert len(options["xAxis"]) == 5
    assert len(options["series"]) == 5

    axis = [second.timestamp_utc.isoformat() for second in window.seconds]

    for x_axis in options["xAxis"]:
        assert x_axis["data"] == axis

    assert options["series"][0]["data"] == [None] * 6
    assert options["series"][1]["data"][4] is None
    assert options["series"][2]["data"][4] is None
    assert options["series"][3]["data"][4] is None
    assert options["series"][4]["data"][4] is None

    assert all(item["connectNulls"] is False for item in options["series"][1:])
    assert "optional context" in options["legend"]["data"][0]


def test_one_second_b_band_has_visible_end_and_total_has_b_and_c():
    window = _window()
    options = build_shock_chart_options(window)
    axis = options["xAxis"][0]["data"]

    for series in options["series"][1:]:
        band = series["markArea"]["data"][0]
        assert band[0]["xAxis"] == axis[2]
        assert band[1]["xAxis"] == axis[3]

    total = options["series"][3]
    markers = total["markLine"]["data"]

    assert [marker["name"] for marker in markers] == [
        "Representative B",
        "Representative C",
    ]
    assert [marker["xAxis"] for marker in markers] == [
        axis[2],
        axis[5],
    ]


def test_price_is_optional_context_not_an_axis_or_l2_filter():
    window = _window()
    timestamp = window.seconds[2].timestamp_utc

    options = build_shock_chart_options(
        window,
        price_by_second={
            timestamp: (100.0, 104.0, 99.0, 103.0),
        },
    )

    price = options["series"][0]["data"]

    assert len(price) == len(window.seconds)
    assert price[2] == [100.0, 103.0, 99.0, 104.0]
    assert sum(candle is not None for candle in price) == 1

    # No price for second 3; verified L2 at second 3 is still present.
    assert price[3] is None
    assert options["series"][3]["data"][3] == 16.0
    assert (
        options["series"][3]["markArea"]["data"][0][0]["xAxis"]
        == options["xAxis"][3]["data"][2]
    )


def test_inconsistent_price_or_l2_input_fails_explicitly():
    from dataclasses import replace

    window = _window()

    with pytest.raises(ShockChartOptionsError, match="OHLC bounds"):
        build_shock_chart_options(
            window,
            price_by_second={
                window.seconds[2].timestamp_utc: (100.0, 99.0, 98.0, 102.0),
            },
        )

    bad_second = replace(
        window.seconds[1],
        total=Fraction(999),
    )
    bad_window = replace(
        window,
        seconds=(
            window.seconds[0],
            bad_second,
            *window.seconds[2:],
        ),
    )

    with pytest.raises(
        ShockChartOptionsError,
        match="disagrees with Bid and Ask",
    ):
        build_shock_chart_options(bad_window)


def test_price_panel_uses_exact_selected_l2_b_coordinates_without_price():
    window = _window()
    options = build_shock_chart_options(window)
    axis = options["xAxis"][0]["data"]

    price_series = options["series"][0]

    assert price_series["data"] == [None] * len(window.seconds)

    for series in options["series"]:
        band = series["markArea"]["data"][0]
        assert band[0]["xAxis"] == axis[window.b_first_position]
        assert band[1]["xAxis"] == axis[window.b_last_position + 1]

    price_b = price_series["markLine"]["data"]
    assert len(price_b) == 1
    assert price_b[0]["name"] == "Representative B"
    assert price_b[0]["xAxis"] == axis[window.representative_b_position]

    total_markers = options["series"][3]["markLine"]["data"]
    assert [item["name"] for item in total_markers] == [
        "Representative B",
        "Representative C",
    ]


def test_partial_price_does_not_change_b_band_or_l2_series():
    window = _window()
    plain = build_shock_chart_options(window)

    with_price = build_shock_chart_options(
        window,
        price_by_second={
            window.seconds[2].timestamp_utc: (
                100.0,
                104.0,
                99.0,
                103.0,
            ),
        },
    )

    assert with_price["series"][0]["data"][2] == [
        100.0,
        103.0,
        99.0,
        104.0,
    ]
    assert with_price["series"][0]["data"][3] is None

    for panel_index in range(5):
        assert (
            with_price["series"][panel_index]["markArea"]
            == plain["series"][panel_index]["markArea"]
        )

    assert with_price["series"][0]["markLine"] == (plain["series"][0]["markLine"])

    for panel_index in range(1, 5):
        assert with_price["series"][panel_index]["data"] == (
            plain["series"][panel_index]["data"]
        )


def test_inspection_panels_hide_horizontal_y_gridlines():
    options = build_shock_chart_options(_window())

    assert len(options["yAxis"]) == 5
    assert all(axis["splitLine"]["show"] is False for axis in options["yAxis"])


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

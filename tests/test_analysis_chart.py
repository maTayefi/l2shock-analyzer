from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from l2shock.analysis import (
    AnalysisDatasetRequest,
    L2Second,
    LiquidityMovementAnalysisResult,
    PriceBounds,
    PriceSecond,
    build_aligned_analysis_dataset,
    execute_liquidity_movement_analysis,
)
from l2shock.ingest import BookSampleQuality
from l2shock.price import TradeSampleQuality
from l2shock.ui.analysis_chart import (
    ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX,
    AnalysisChartVisibility,
    build_analysis_chart_option,
    chart_navigation_window,
)


def _start() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _result(
    *,
    excluded: frozenset[int] = frozenset(),
) -> LiquidityMovementAnalysisResult:
    values = (
        "100",
        "120",
        "116",
        "110",
        "90",
        "94",
        "105",
        "101",
    )

    request = AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=(_start() + timedelta(seconds=len(values) - 1)),
        activity_timeframe="1s",
        maximum_chart_bars=400,
        price_bounds=PriceBounds(
            min_price=90,
            max_price=110,
        ),
        context_before=0,
        context_after=0,
    )

    l2 = tuple(
        L2Second(
            timestamp_utc=(_start() + timedelta(seconds=index)),
            quality=BookSampleQuality.VALID,
            invalid_reason=None,
            bid_liquidity=Decimal(value),
            ask_liquidity=Decimal("200"),
            source_count=1,
        )
        for index, value in enumerate(values)
    )

    price = tuple(
        PriceSecond(
            timestamp_utc=(_start() + timedelta(seconds=index)),
            quality=TradeSampleQuality.VALID,
            invalid_reason=None,
            open=(Decimal("130") if index in excluded else Decimal("100")),
            high=(Decimal("135") if index in excluded else Decimal("105")),
            low=(Decimal("125") if index in excluded else Decimal("95")),
            close=(Decimal("132") if index in excluded else Decimal("101")),
            trade_count=1,
        )
        for index in range(len(values))
    )

    dataset = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
    )

    return execute_liquidity_movement_analysis(dataset)


def _series_by_name(
    option: dict,
) -> dict[str, dict]:
    return {
        str(item.get("name")): item
        for item in option["series"]
        if isinstance(item, dict)
    }


def test_chart_has_five_synchronized_panels() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )

    assert len(option["grid"]) == 5
    assert len(option["xAxis"]) == 5
    assert len(option["yAxis"]) == 5

    assert option["dataZoom"][0]["xAxisIndex"] == [
        0,
        1,
        2,
        3,
        4,
    ]
    assert option["dataZoom"][1]["xAxisIndex"] == [
        0,
        1,
        2,
        3,
        4,
    ]

    assert option["axisPointer"]["link"] == [
        {
            "xAxisIndex": "all",
        }
    ]


def test_chart_contains_price_and_four_liquidity_metrics() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )
    series = _series_by_name(option)

    assert series["Binance perpetual price"]["type"] == "candlestick"
    assert series["Bid Liquidity"]["type"] == "line"
    assert series["Ask Liquidity"]["type"] == "line"
    assert series["Total Liquidity"]["type"] == "line"
    assert series["Positive Delta"]["type"] == "line"
    assert series["Negative Delta"]["type"] == "line"


def test_chart_uses_local_timezone_labels() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )

    labels = option["xAxis"][0]["data"]

    assert labels
    assert "+03:30" in labels[0]


def test_price_excluded_bars_are_compressed_not_concatenated_silently() -> None:
    result = _result(
        excluded=frozenset({2, 3}),
    )

    option = build_analysis_chart_option(
        result,
        timezone_name="Asia/Tehran",
    )

    metadata = option["l2shockChartMetadata"]

    assert metadata["visible_bar_count"] == 6
    assert metadata["source_bar_indices"] == [
        0,
        1,
        4,
        5,
        6,
        7,
    ]

    assert metadata["discontinuities"]
    first = next(iter(metadata["discontinuities"].values()))

    assert first["skipped_seconds"] == 2
    assert "price_excluded" in first["reasons"]


def test_selected_candidates_create_highlight_layers() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )

    names = {
        str(item.get("name")) for item in option["series"] if isinstance(item, dict)
    }

    assert any(name.startswith("Upward LM highlights") for name in names)
    assert any(name.startswith("Downward LM highlights") for name in names)


def test_render_visibility_does_not_change_analysis_result() -> None:
    result = _result()

    hidden = build_analysis_chart_option(
        result,
        timezone_name="Asia/Tehran",
        visibility=AnalysisChartVisibility(
            show_price_highlights=False,
            show_liquidity_highlights=False,
        ),
    )
    visible = build_analysis_chart_option(
        result,
        timezone_name="Asia/Tehran",
    )

    assert result.analysis_id == result.analysis_id
    assert result.selected
    assert len(hidden["series"]) < len(visible["series"])


def test_candidate_navigation_maps_to_chart_categories() -> None:
    result = _result()
    ranking = result.selected[0]

    window = chart_navigation_window(
        result,
        ranking,
        padding_bars=2,
    )

    assert window is not None
    assert (
        window.start_index
        <= window.candidate_start_index
        <= window.candidate_end_index
        <= window.end_index
    )


def test_chart_reserves_selected_focus_series_for_all_panels() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )

    focus = [
        item
        for item in option["series"]
        if isinstance(item, dict)
        and str(item.get("id") or "").startswith(ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX)
    ]

    assert len(focus) == 5
    assert {int(item["xAxisIndex"]) for item in focus} == {0, 1, 2, 3, 4}
    assert all(item["markArea"]["data"] == [] for item in focus)


def test_chart_accepts_configured_warning_thresholds() -> None:
    option = build_analysis_chart_option(
        _result(excluded=frozenset({2, 3})),
        timezone_name="Asia/Tehran",
        l2_warning_seconds=10,
        price_warning_seconds=20,
    )

    assert option["l2shockChartMetadata"]["discontinuities"]


def test_chart_metadata_contains_temporal_viewport_ownership() -> None:
    result = _result()

    option = build_analysis_chart_option(
        result,
        timezone_name="Asia/Tehran",
    )

    metadata = option["l2shockChartMetadata"]
    timestamps = metadata["visible_start_times_utc"]

    assert len(timestamps) == metadata["visible_bar_count"]
    assert metadata["bar_duration_seconds"] == (result.dataset.chart.timeframe.seconds)
    assert timestamps
    assert all(str(value).endswith("Z") for value in timestamps)


def test_delta_zero_reference_belongs_to_delta_series() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )
    series = _series_by_name(option)

    negative = series["Negative Delta"]

    assert "markLine" in negative
    assert negative["markLine"]["data"][0]["yAxis"] == 0

    focus = [
        item
        for item in option["series"]
        if isinstance(item, dict)
        and str(item.get("id") or "").startswith(ANALYSIS_SELECTED_FOCUS_SERIES_PREFIX)
    ]

    assert focus
    assert all("markLine" not in item for item in focus)


def test_principal_series_are_bound_to_independent_panel_axes() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )
    series = _series_by_name(option)

    expected_axes = {
        "Binance perpetual price": 0,
        "Bid Liquidity": 1,
        "Ask Liquidity": 2,
        "Total Liquidity": 3,
        "Positive Delta": 4,
        "Negative Delta": 4,
    }

    for name, expected_index in expected_axes.items():
        assert series[name]["xAxisIndex"] == expected_index
        assert series[name]["yAxisIndex"] == expected_index


def test_each_value_axis_belongs_to_its_matching_grid() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )

    assert [axis["gridIndex"] for axis in option["yAxis"]] == [
        0,
        1,
        2,
        3,
        4,
    ]

    assert [axis["name"] for axis in option["yAxis"]] == [
        "Price (USDT)",
        "Bid Liquidity (USD eq.)",
        "Ask Liquidity (USD eq.)",
        "Total Liquidity (USD eq.)",
        "Order-Book Delta (USD eq.)",
    ]

    assert all(axis["scale"] is True for axis in option["yAxis"])
    assert all("min" not in axis for axis in option["yAxis"])
    assert all("max" not in axis for axis in option["yAxis"])


def test_delta_series_contains_bid_minus_ask_values() -> None:
    option = build_analysis_chart_option(
        _result(),
        timezone_name="Asia/Tehran",
    )
    series = _series_by_name(option)

    positive = series["Positive Delta"]["data"]
    negative = series["Negative Delta"]["data"]

    combined = [
        positive_value if positive_value is not None else negative_value
        for positive_value, negative_value in zip(
            positive,
            negative,
            strict=True,
        )
    ]

    result = _result()
    expected = []

    for bar in result.dataset.chart.bars:
        if not bar.core_eligible:
            continue

        delta = bar.l2.bid_ask_delta()
        expected.append(None if delta is None else float(delta))

    assert combined == expected

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext

import pytest

from l2shock.analysis import (
    AnalysisDatasetRequest,
    L2Second,
    LiquidityMetric,
    PriceSecond,
    build_aligned_analysis_dataset,
    execute_liquidity_movement_analysis,
)
from l2shock.config import LMConfig
from l2shock.ingest import BookSampleQuality
from l2shock.price import TradeSampleQuality
from l2shock.ui.analysis_controls import (
    AnalysisControlError,
    analysis_result_summary,
    build_analysis_request_and_config,
    build_analysis_table_rows,
    parse_confirmation_fraction,
    parse_nonnegative_integer,
    parse_optional_price_bound,
    parse_positive_integer,
)
from l2shock.ui.analysis_export import (
    ANALYSIS_EXPORT_SCHEMA,
    analysis_export_filename,
    analysis_export_json_bytes,
    build_analysis_export_payload,
)
import json


def _start() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def test_positive_integer_accepts_integral_nicegui_float() -> None:
    assert (
        parse_positive_integer(
            400.0,
            field_name="Maximum chart bars",
        )
        == 400
    )


@pytest.mark.parametrize(
    "value",
    [
        True,
        0,
        -1,
        1.5,
        "not-an-integer",
    ],
)
def test_positive_integer_rejects_invalid_values(
    value: object,
) -> None:
    with pytest.raises(
        AnalysisControlError,
        match="positive integer|positive",
    ):
        parse_positive_integer(
            value,
            field_name="Count",
        )


def test_optional_price_bound_is_null_when_disabled() -> None:
    assert (
        parse_optional_price_bound(
            enabled=False,
            value="not-used",
            field_name="Minimum price",
        )
        is None
    )


@pytest.mark.parametrize(
    "value",
    [
        "",
        "0",
        "-1",
        "NaN",
        "Infinity",
        "not-a-price",
    ],
)
def test_enabled_price_bound_rejects_invalid_values(
    value: str,
) -> None:
    with pytest.raises(AnalysisControlError):
        parse_optional_price_bound(
            enabled=True,
            value=value,
            field_name="Minimum price",
        )


def test_confirmation_fraction_uses_exact_decimal() -> None:
    assert parse_confirmation_fraction("0.20") == Decimal("0.20")

    with pytest.raises(AnalysisControlError):
        parse_confirmation_fraction("1")

    with pytest.raises(AnalysisControlError):
        parse_confirmation_fraction("0")


def test_build_request_and_config_preserves_semantic_inputs() -> None:
    request, config = build_analysis_request_and_config(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(hours=1),
        activity_timeframe="5s",
        maximum_chart_bars=400.0,
        chart_timeframe_override="15m",
        minimum_price_enabled=True,
        minimum_price_value="90",
        maximum_price_enabled=True,
        maximum_price_value="110",
        context_before=3.0,
        context_after=4.0,
        confirmation_fraction="0.25",
        top_n_height=7.0,
        top_n_sharpness=9.0,
        lm_settings=LMConfig(),
    )

    assert request.base == "BTC"
    assert request.activity_timeframe.label == "5s"
    assert request.maximum_chart_bars == 400
    assert request.chart_timeframe_override is not None
    assert request.chart_timeframe_override.label == "15m"
    assert request.price_bounds.min_price == Decimal("90")
    assert request.price_bounds.max_price == Decimal("110")
    assert request.context_before == 3
    assert request.context_after == 4

    assert config.detection.confirmation_retracement_fraction == Decimal("0.25")
    assert config.ranking.top_n_height == 7
    assert config.ranking.top_n_sharpness == 9
    assert config.metrics == tuple(LiquidityMetric)


def _analysis_result():
    bid_values = (
        "100",
        "120",
        "116",
        "90",
        "94",
    )

    request = AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(seconds=4),
        activity_timeframe="1s",
        maximum_chart_bars=400,
        context_before=0,
        context_after=0,
    )

    l2 = tuple(
        L2Second(
            timestamp_utc=_start() + timedelta(seconds=index),
            quality=BookSampleQuality.VALID,
            invalid_reason=None,
            bid_liquidity=Decimal(value),
            ask_liquidity=Decimal("200"),
            source_count=1,
        )
        for index, value in enumerate(bid_values)
    )
    price = tuple(
        PriceSecond(
            timestamp_utc=_start() + timedelta(seconds=index),
            quality=TradeSampleQuality.VALID,
            invalid_reason=None,
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("95"),
            close=Decimal("101"),
            trade_count=1,
        )
        for index in range(len(bid_values))
    )

    dataset = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
    )

    return execute_liquidity_movement_analysis(dataset)


def test_table_rows_are_selected_and_sortable_numeric_values() -> None:
    result = _analysis_result()
    rows = build_analysis_table_rows(
        result,
        timezone_name="Asia/Tehran",
    )

    assert len(rows) == len(result.selected)
    assert rows

    first = rows[0]
    first_ranking = result.selected[0]

    assert isinstance(first["absolute_height"], float)
    assert isinstance(first["primary_evidence"], float)
    assert isinstance(first["population_rank"], int)
    assert first["metric"] in {metric.value for metric in LiquidityMetric}
    assert "+03:30" in str(first["start_time"])

    assert first["start_extremeness"] == float(first_ranking.start_extremeness)
    assert first["end_extremeness"] == float(first_ranking.end_extremeness)
    assert first["boundary_extremeness"] == float(first_ranking.boundary_extremeness)
    assert "endpoint_extremeness" not in first


def test_analysis_summary_reports_coverage_and_candidates() -> None:
    result = _analysis_result()
    summary = analysis_result_summary(result)

    assert result.analysis_id[:12] in summary
    assert f"candidates={len(result.candidates)}" in summary
    assert f"selected={len(result.selected)}" in summary


def test_analysis_json_export_is_deterministic_and_exact() -> None:
    result = _analysis_result()

    first = analysis_export_json_bytes(
        result,
        timezone_name="Asia/Tehran",
    )
    second = analysis_export_json_bytes(
        result,
        timezone_name="Asia/Tehran",
    )

    assert first == second

    payload = json.loads(first.decode("utf-8"))

    assert payload["schema"] == ANALYSIS_EXPORT_SCHEMA
    assert payload["analysis_id"] == result.analysis_id
    assert payload["dataset_analysis_id"] == (result.dataset.analysis_id)
    assert payload["display_timezone"] == "Asia/Tehran"
    assert payload["summary"]["candidate_count"] == (len(result.candidates))
    assert payload["summary"]["selected_count"] == (len(result.selected))

    assert payload["candidates"]

    candidate = payload["candidates"][0]

    assert isinstance(candidate["absolute_height"], str)
    assert isinstance(candidate["relative_height"], str)
    assert isinstance(candidate["sharpness"], str)
    assert len(candidate["candidate_id"]) == 64

    assert payload["schema_version"] == 2
    assert payload["rankings"]

    ranking = payload["rankings"][0]
    boundary = ranking["boundary_extremeness"]

    assert boundary["start_weight_fraction"] == "0.5"
    assert boundary["end_weight_fraction"] == "0.5"

    with localcontext(Context(prec=100)):
        expected_mean = (
            Decimal(boundary["start"]) + Decimal(boundary["end"])
        ) / Decimal(2)

    assert Decimal(boundary["equal_weight_mean"]) == expected_mean
    assert ranking["schema_version"] == 2
    assert ranking["algorithm_version"] == (
        "population-primary-recall-boundary-extremeness-v2"
    )


def test_analysis_export_payload_has_no_chart_ui_state() -> None:
    result = _analysis_result()

    payload = build_analysis_export_payload(
        result,
        timezone_name="Asia/Tehran",
    )
    encoded = json.dumps(
        payload,
        sort_keys=True,
    )

    for forbidden in (
        "render_token",
        "crosshair",
        "selected_table_row",
        "zoom_state",
        "visible_panels",
    ):
        assert forbidden not in encoded


def test_analysis_export_filename_is_stable() -> None:
    result = _analysis_result()

    filename = analysis_export_filename(result)

    assert filename.startswith("l2shock_BTC_")
    assert result.analysis_id[:16] in filename
    assert filename.endswith("_analysis.json")


def test_empty_chart_timeframe_override_keeps_automatic_policy() -> None:
    request, _config = build_analysis_request_and_config(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(hours=1),
        activity_timeframe="5s",
        maximum_chart_bars=400.0,
        chart_timeframe_override="",
        minimum_price_enabled=False,
        minimum_price_value="",
        maximum_price_enabled=False,
        maximum_price_value="",
        context_before=3.0,
        context_after=4.0,
        confirmation_fraction="0.25",
        top_n_height=7.0,
        top_n_sharpness=9.0,
        lm_settings=LMConfig(),
    )

    assert request.chart_timeframe_override is None


def test_optional_price_bound_preserves_exact_decimal_text() -> None:
    value = parse_optional_price_bound(
        enabled=True,
        value="100.0000000000000000001",
        field_name="Minimum price",
    )

    assert value == Decimal("100.0000000000000000001")
    assert isinstance(value, Decimal)


@pytest.mark.parametrize(
    "value",
    [
        0,
        0.0,
        "0",
        "+0",
    ],
)
def test_nonnegative_integer_accepts_zero_forms(
    value: object,
) -> None:
    assert (
        parse_nonnegative_integer(
            value,
            field_name="Context bars",
            maximum=10_000,
        )
        == 0
    )


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        -1,
        -1.0,
        1.5,
        "1.5",
        "",
        None,
    ],
)
def test_nonnegative_integer_rejects_invalid_values(
    value: object,
) -> None:
    with pytest.raises(
        AnalysisControlError,
        match="non-negative|non-negative integer",
    ):
        parse_nonnegative_integer(
            value,
            field_name="Context bars",
            maximum=10_000,
        )


def test_build_request_accepts_numeric_zero_context_counts() -> None:
    request, _config = build_analysis_request_and_config(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(hours=1),
        activity_timeframe="5s",
        maximum_chart_bars=400.0,
        chart_timeframe_override="",
        minimum_price_enabled=False,
        minimum_price_value="",
        maximum_price_enabled=False,
        maximum_price_value="",
        context_before=0.0,
        context_after=0,
        confirmation_fraction="0.20",
        top_n_height=10.0,
        top_n_sharpness=10.0,
        lm_settings=LMConfig(),
    )

    assert request.context_before == 0
    assert request.context_after == 0

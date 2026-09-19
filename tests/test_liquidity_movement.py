from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext

import pytest

from l2shock.analysis import (
    AnalysisDatasetRequest,
    LiquidityMetric,
    LiquidityMovementDetectionConfig,
    LiquidityMovementDirection,
    LiquidityMovementError,
    PriceBounds,
    build_aligned_analysis_dataset,
    detect_liquidity_movements,
    liquidity_metric_value,
)
from l2shock.analysis import L2Second, PriceSecond
from l2shock.ingest import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.price import (
    TradeSampleQuality,
)


def _start() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _l2(
    index: int,
    bid: str | None,
    *,
    ask: str = "100",
) -> L2Second:
    if bid is None:
        return L2Second(
            timestamp_utc=_start() + timedelta(seconds=index),
            quality=BookSampleQuality.INVALID,
            invalid_reason=BookSampleInvalidReason.REPLAY_INVALIDATED,
            bid_liquidity=None,
            ask_liquidity=None,
            source_count=0,
        )

    return L2Second(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(bid),
        ask_liquidity=Decimal(ask),
        source_count=1,
    )


def _price(
    index: int,
    *,
    outside: bool = False,
) -> PriceSecond:
    if outside:
        open_value = Decimal("130")
        high = Decimal("135")
        low = Decimal("125")
        close = Decimal("132")
    else:
        open_value = Decimal("100")
        high = Decimal("105")
        low = Decimal("95")
        close = Decimal("101")

    return PriceSecond(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=TradeSampleQuality.VALID,
        invalid_reason=None,
        open=open_value,
        high=high,
        low=low,
        close=close,
        trade_count=1,
    )


def _series(
    bid_values: tuple[str | None, ...],
    *,
    outside_price_indices: frozenset[int] = frozenset(),
    context_before: int = 0,
    context_after: int = 0,
):
    l2 = tuple(_l2(index, value) for index, value in enumerate(bid_values))
    price = tuple(
        _price(
            index,
            outside=index in outside_price_indices,
        )
        for index in range(len(bid_values))
    )

    request = AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=(_start() + timedelta(seconds=len(bid_values) - 1)),
        activity_timeframe="1s",
        maximum_chart_bars=400,
        price_bounds=PriceBounds(
            min_price=Decimal("90"),
            max_price=Decimal("110"),
        ),
        context_before=context_before,
        context_after=context_after,
    )

    return build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
    ).activity


def test_upward_lm_is_confirmed_at_exact_twenty_percent() -> None:
    series = _series(("100", "120", "116"))

    candidates = detect_liquidity_movements(
        series,
        metric=LiquidityMetric.BID_LIQUIDITY,
    )

    upward = candidates[0]

    assert upward.direction is LiquidityMovementDirection.UP
    assert upward.start_index == 0
    assert upward.end_index == 1
    assert upward.confirmation_index == 2
    assert upward.start_value == Decimal("100")
    assert upward.end_value == Decimal("120")
    assert upward.absolute_height == Decimal("20")
    assert upward.confirmation_retracement == Decimal("4")
    assert upward.confirmation_retracement_fraction == Decimal("0.2")
    assert upward.terminal_offline is False
    assert upward.confirmed is True


def test_endpoint_is_extremum_not_confirmation_bar() -> None:
    series = _series(("100", "110", "120", "116"))

    candidate = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert candidate.end_index == 2
    assert candidate.end_time_utc == _start() + timedelta(seconds=2)
    assert candidate.confirmation_index == 3
    assert candidate.confirmation_time_utc == (_start() + timedelta(seconds=3))


def test_retracement_below_threshold_does_not_confirm() -> None:
    series = _series(("100", "120", "116.01"))

    candidate = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert candidate.direction is LiquidityMovementDirection.UP
    assert candidate.end_index == 1
    assert candidate.confirmation_index is None
    assert candidate.terminal_offline is True


def test_right_edge_extremum_is_retained_as_offline_terminal() -> None:
    series = _series(("100", "105", "110", "120"))

    candidate = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert candidate.start_index == 0
    assert candidate.end_index == 3
    assert candidate.terminal_offline is True
    assert candidate.confirmation_index is None
    assert candidate.confirmation_time_utc is None


def test_downward_lm_uses_opposite_upward_confirmation() -> None:
    series = _series(("120", "100", "104"))

    candidate = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert candidate.direction is LiquidityMovementDirection.DOWN
    assert candidate.start_index == 0
    assert candidate.end_index == 1
    assert candidate.confirmation_index == 2
    assert candidate.confirmation_retracement == Decimal("4")
    assert candidate.confirmation_retracement_fraction == Decimal("0.2")


def test_equal_extremum_tie_keeps_earliest_endpoint() -> None:
    series = _series(("100", "120", "120", "116"))

    candidate = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert candidate.end_index == 1
    assert candidate.confirmation_index == 3


def test_adverse_moves_inside_candidate_are_measured() -> None:
    # 100→110→109→120→116
    # The dip 110→109 is 1/10 = 10% < 20%, so it does NOT confirm early.
    # The detector continues to the larger extremum at 120.
    # The retracement 120→116 = 4/20 = 20% >= 20% confirms at index 4.
    series = _series(("100", "110", "109", "120", "116"))

    candidate = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert candidate.direction is LiquidityMovementDirection.UP
    assert candidate.start_index == 0
    assert candidate.end_index == 3
    assert candidate.confirmation_index == 4
    assert candidate.terminal_offline is False
    assert candidate.start_value == Decimal("100")
    assert candidate.end_value == Decimal("120")
    assert candidate.absolute_height == Decimal("20")
    assert candidate.adverse_move_count == 1
    assert candidate.adverse_move_total == Decimal("1")
    assert candidate.adverse_move_maximum == Decimal("1")
    assert candidate.adverse_move_total_fraction == Decimal("0.05")
    assert candidate.adverse_move_maximum_fraction == Decimal("0.05")


def test_relative_height_uses_metric_population_range() -> None:
    series = _series(("100", "120", "116", "80"))

    first = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert first.absolute_height == Decimal("20")
    assert first.relative_height == Decimal("0.5")
    assert first.bars == 2

    # Production uses prec=34; match it here so the Decimal digits agree.
    with localcontext(Context(prec=34)):
        expected = Decimal("0.5") / Decimal(2).sqrt()
    assert first.sharpness == expected


def test_invalid_l2_bar_splits_candidate_population() -> None:
    series = _series(
        (
            "100",
            "120",
            None,
            "80",
            "100",
            "96",
        )
    )

    candidates = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )

    assert candidates
    assert all(
        not (candidate.start_index < 2 < candidate.end_index)
        for candidate in candidates
    )

    assert {candidate.segment_index for candidate in candidates} == {0, 1}


def test_price_excluded_run_cannot_be_bridged() -> None:
    series = _series(
        ("100", "120", "80", "70", "100", "94"),
        outside_price_indices=frozenset({2, 3}),
        context_before=1,
        context_after=1,
    )

    assert [
        (
            segment.core_start_index,
            segment.core_end_index,
        )
        for segment in series.segments
    ] == [
        (0, 1),
        (4, 5),
    ]

    candidates = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )

    assert all(
        not (candidate.start_index <= 1 and candidate.end_index >= 4)
        for candidate in candidates
    )


def test_full_and_in_range_extent_are_distinct_for_context() -> None:
    series = _series(
        ("90", "100", "120", "116"),
        outside_price_indices=frozenset({0}),
        context_before=1,
    )

    candidate = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )[0]

    assert candidate.start_index == 0
    assert candidate.in_range_start_index == 1
    assert candidate.end_index == 2
    assert candidate.in_range_end_index == 2


def test_metric_selection_supports_all_four_metrics() -> None:
    series = _series(("100", "120", "116"))

    for metric in LiquidityMetric:
        result = detect_liquidity_movements(
            series,
            metric=metric,
        )

        assert isinstance(result, tuple)


def test_constant_series_has_no_candidate() -> None:
    series = _series(("100", "100", "100"))

    assert (
        detect_liquidity_movements(
            series,
            metric="bid_liquidity",
        )
        == ()
    )


def test_single_bar_series_has_no_candidate() -> None:
    series = _series(("100",))

    assert (
        detect_liquidity_movements(
            series,
            metric="bid_liquidity",
        )
        == ()
    )


def test_detection_is_deterministic() -> None:
    series = _series(("100", "110", "108", "120", "116"))

    first = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )
    second = detect_liquidity_movements(
        series,
        metric="bid_liquidity",
    )

    assert first == second


@pytest.mark.parametrize(
    "fraction",
    [
        Decimal("0"),
        Decimal("-0.1"),
        Decimal("1"),
        Decimal("1.1"),
        Decimal("NaN"),
    ],
)
def test_invalid_confirmation_fraction_is_rejected(
    fraction: Decimal,
) -> None:
    with pytest.raises(LiquidityMovementError):
        LiquidityMovementDetectionConfig(
            confirmation_retracement_fraction=fraction,
        )


def test_config_requires_exact_decimal_fraction() -> None:
    with pytest.raises(LiquidityMovementError, match="Decimal"):
        LiquidityMovementDetectionConfig(
            confirmation_retracement_fraction=0.2,  # type: ignore[arg-type]
        )


def test_relative_height_uses_values_outside_segment_context() -> None:
    series = _series(
        (
            "0",
            "100",
            "120",
            "116",
            "1000",
        ),
        outside_price_indices=frozenset(
            {
                0,
                4,
            }
        ),
        context_before=0,
        context_after=0,
    )

    candidate = detect_liquidity_movements(
        series,
        metric=LiquidityMetric.BID_LIQUIDITY,
    )[0]

    assert candidate.absolute_height == Decimal("20")
    assert candidate.relative_height == Decimal("0.02")


def test_detection_is_independent_of_ambient_decimal_context() -> None:
    series = _series(
        (
            "123456789.123456789",
            "123456799.987654321",
            "123456797.814814815",
            "123456810.111111111",
            "123456805.913580247",
        )
    )
    config = LiquidityMovementDetectionConfig(
        confirmation_retracement_fraction=Decimal("0.20"),
        decimal_precision=34,
    )

    expected = detect_liquidity_movements(
        series,
        metric=LiquidityMetric.BID_LIQUIDITY,
        config=config,
    )

    with localcontext(Context(prec=6)):
        observed = detect_liquidity_movements(
            series,
            metric=LiquidityMetric.BID_LIQUIDITY,
            config=config,
        )

    assert observed == expected


def test_candidate_validation_uses_detector_precision_above_34() -> None:
    series = _series(
        (
            "1",
            "3",
            "2.3333333333333333333333333333333333333333333333333",
        )
    )
    config = LiquidityMovementDetectionConfig(
        confirmation_retracement_fraction=Decimal("0.20"),
        decimal_precision=50,
    )

    candidates = detect_liquidity_movements(
        series,
        metric=LiquidityMetric.BID_LIQUIDITY,
        config=config,
    )

    assert candidates

    candidate = candidates[0]

    with localcontext(
        Context(
            prec=50,
        )
    ):
        expected = candidate.confirmation_retracement / candidate.absolute_height

    assert candidate.confirmation_retracement_fraction == expected


def test_delta_metric_uses_requested_decimal_precision() -> None:
    series = _series(
        (
            "1",
            "2",
            "1.5",
        )
    )
    bar = series.bars[0]

    observed = liquidity_metric_value(
        bar,
        LiquidityMetric.BID_ASK_DELTA,
        decimal_precision=50,
    )

    assert bar.l2.bid_liquidity is not None
    assert bar.l2.ask_liquidity is not None

    with localcontext(
        Context(
            prec=50,
        )
    ):
        expected = bar.l2.bid_liquidity - bar.l2.ask_liquidity

    assert observed == expected

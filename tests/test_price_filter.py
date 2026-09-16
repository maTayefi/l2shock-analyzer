from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.analysis.price_filter import (
    OHLC,
    PriceBarState,
    PriceBounds,
    build_filtered_segments,
    classify_price_bar,
    clip_ohlc_for_display,
)


def _time(minute: int) -> datetime:
    return datetime(2026, 9, 2, 12, minute, tzinfo=timezone.utc)


def test_close_outside_but_wick_intersects_is_eligible() -> None:
    candle = OHLC(
        open=220.0,
        high=230.0,
        low=195.0,
        close=225.0,
    )
    bounds = PriceBounds(min_price=100.0, max_price=200.0)

    assert classify_price_bar(candle, bounds) is PriceBarState.ELIGIBLE


def test_completely_above_range_is_excluded() -> None:
    candle = OHLC(
        open=220.0,
        high=230.0,
        low=210.0,
        close=225.0,
    )
    bounds = PriceBounds(min_price=100.0, max_price=200.0)

    assert classify_price_bar(candle, bounds) is PriceBarState.EXCLUDED


def test_display_clipping_preserves_valid_candle_geometry() -> None:
    candle = OHLC(
        open=220.0,
        high=230.0,
        low=195.0,
        close=225.0,
    )
    bounds = PriceBounds(min_price=100.0, max_price=200.0)

    clipped = clip_ohlc_for_display(candle, bounds)

    assert clipped == OHLC(
        open=200.0,
        high=200.0,
        low=195.0,
        close=200.0,
    )

    # The immutable source object is unchanged.
    assert candle.high == 230.0
    assert candle.close == 225.0


def test_invalid_ohlc_is_not_made_eligible() -> None:
    candle = OHLC(
        open=100.0,
        high=90.0,
        low=95.0,
        close=98.0,
    )

    assert classify_price_bar(candle, PriceBounds()) is PriceBarState.INVALID


def test_excluded_run_splits_core_segments() -> None:
    timestamps = [_time(index) for index in range(6)]
    candles = [
        OHLC(100.0, 105.0, 99.0, 104.0),
        OHLC(104.0, 106.0, 101.0, 102.0),
        OHLC(130.0, 135.0, 125.0, 132.0),
        OHLC(131.0, 136.0, 126.0, 133.0),
        OHLC(102.0, 104.0, 98.0, 100.0),
        OHLC(100.0, 103.0, 97.0, 101.0),
    ]

    segments, states = build_filtered_segments(
        timestamps,
        candles,
        PriceBounds(min_price=95.0, max_price=110.0),
        expected_step=timedelta(minutes=1),
        context_before=0,
        context_after=0,
    )

    assert states == (
        PriceBarState.ELIGIBLE,
        PriceBarState.ELIGIBLE,
        PriceBarState.EXCLUDED,
        PriceBarState.EXCLUDED,
        PriceBarState.ELIGIBLE,
        PriceBarState.ELIGIBLE,
    )

    assert [(segment.core_start, segment.core_end) for segment in segments] == [
        (0, 1),
        (4, 5),
    ]


def test_context_does_not_merge_separated_segments() -> None:
    timestamps = [_time(index) for index in range(6)]
    candles = [
        OHLC(100.0, 105.0, 99.0, 104.0),
        OHLC(104.0, 106.0, 101.0, 102.0),
        OHLC(130.0, 135.0, 125.0, 132.0),
        OHLC(131.0, 136.0, 126.0, 133.0),
        OHLC(102.0, 104.0, 98.0, 100.0),
        OHLC(100.0, 103.0, 97.0, 101.0),
    ]

    segments, _states = build_filtered_segments(
        timestamps,
        candles,
        PriceBounds(min_price=95.0, max_price=110.0),
        expected_step=timedelta(minutes=1),
        context_before=1,
        context_after=1,
    )

    assert len(segments) == 2

    assert (
        segments[0].core_start,
        segments[0].core_end,
        segments[0].context_start,
        segments[0].context_end,
    ) == (0, 1, 0, 2)

    assert (
        segments[1].core_start,
        segments[1].core_end,
        segments[1].context_start,
        segments[1].context_end,
    ) == (4, 5, 3, 5)


def test_timestamp_gap_splits_eligible_segments() -> None:
    timestamps = [
        _time(0),
        _time(1),
        _time(4),
        _time(5),
    ]
    candles = [
        OHLC(100.0, 105.0, 99.0, 104.0),
        OHLC(104.0, 106.0, 101.0, 102.0),
        OHLC(102.0, 104.0, 98.0, 100.0),
        OHLC(100.0, 103.0, 97.0, 101.0),
    ]

    segments, _states = build_filtered_segments(
        timestamps,
        candles,
        PriceBounds(min_price=95.0, max_price=110.0),
        expected_step=timedelta(minutes=1),
    )

    assert [(segment.core_start, segment.core_end) for segment in segments] == [
        (0, 1),
        (2, 3),
    ]


def test_invalid_bar_stops_context_extension() -> None:
    timestamps = [_time(index) for index in range(4)]
    candles = [
        OHLC(130.0, 135.0, 125.0, 132.0),
        OHLC(100.0, 90.0, 95.0, 98.0),  # Invalid geometry
        OHLC(100.0, 104.0, 98.0, 102.0),
        OHLC(102.0, 105.0, 99.0, 101.0),
    ]

    segments, states = build_filtered_segments(
        timestamps,
        candles,
        PriceBounds(min_price=95.0, max_price=110.0),
        expected_step=timedelta(minutes=1),
        context_before=3,
        context_after=3,
    )

    assert states[1] is PriceBarState.INVALID
    assert len(segments) == 1
    assert segments[0].core_start == 2
    assert segments[0].context_start == 2


def test_reversed_bounds_are_rejected() -> None:
    with pytest.raises(ValueError, match="min_price must be <= max_price"):
        PriceBounds(min_price=200.0, max_price=100.0)


def test_context_never_includes_another_eligible_core_segment() -> None:
    timestamps = [_time(index) for index in range(5)]
    candles = [
        OHLC(100.0, 105.0, 99.0, 104.0),
        OHLC(104.0, 106.0, 101.0, 102.0),
        OHLC(130.0, 135.0, 125.0, 132.0),
        OHLC(102.0, 104.0, 98.0, 100.0),
        OHLC(100.0, 103.0, 97.0, 101.0),
    ]

    segments, states = build_filtered_segments(
        timestamps,
        candles,
        PriceBounds(
            min_price=95.0,
            max_price=110.0,
        ),
        expected_step=timedelta(minutes=1),
        context_before=3,
        context_after=3,
    )

    assert states == (
        PriceBarState.ELIGIBLE,
        PriceBarState.ELIGIBLE,
        PriceBarState.EXCLUDED,
        PriceBarState.ELIGIBLE,
        PriceBarState.ELIGIBLE,
    )

    assert len(segments) == 2

    first, second = segments

    assert (
        first.core_start,
        first.core_end,
        first.context_start,
        first.context_end,
    ) == (
        0,
        1,
        0,
        2,
    )

    assert (
        second.core_start,
        second.core_end,
        second.context_start,
        second.context_end,
    ) == (
        3,
        4,
        2,
        4,
    )

    # Neither context may absorb bars from the other eligible core.
    assert first.context_end < second.core_start
    assert second.context_start > first.core_end


def test_exact_decimal_boundary_intersection_is_not_float_rounded() -> None:
    candle = OHLC(
        open=Decimal("100.0000000000000000001"),
        high=Decimal("100.0000000000000000001"),
        low=Decimal("100.0000000000000000001"),
        close=Decimal("100.0000000000000000001"),
    )
    bounds = PriceBounds(
        max_price=Decimal("100.0000000000000000000"),
    )

    assert (
        classify_price_bar(
            candle,
            bounds,
        )
        is PriceBarState.EXCLUDED
    )


def test_exact_decimal_boundary_touch_is_eligible() -> None:
    candle = OHLC(
        open=Decimal("101"),
        high=Decimal("102"),
        low=Decimal("100.0000000000000000001"),
        close=Decimal("101"),
    )
    bounds = PriceBounds(
        max_price=Decimal("100.0000000000000000001"),
    )

    assert (
        classify_price_bar(
            candle,
            bounds,
        )
        is PriceBarState.ELIGIBLE
    )


def test_display_clipping_retains_exact_decimal_value() -> None:
    candle = OHLC(
        open=Decimal("101"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("101.5"),
    )
    bounds = PriceBounds(
        min_price=Decimal("99.1234567890123456789"),
        max_price=Decimal("101.1234567890123456789"),
    )

    clipped = clip_ohlc_for_display(
        candle,
        bounds,
    )

    assert clipped.open == Decimal("101")
    assert clipped.high == Decimal("101.1234567890123456789")
    assert clipped.low == Decimal("99.1234567890123456789")
    assert clipped.close == Decimal("101.1234567890123456789")

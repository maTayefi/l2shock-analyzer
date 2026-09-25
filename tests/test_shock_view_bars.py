# tests/test_shock_view_bars.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from fractions import Fraction

import pytest

from l2shock.analysis.shock_window import ShockWindowSecond
from l2shock.ui.shock_view_bars import (
    ShockViewBarsError,
    build_shock_view_bars,
)

_ORIGIN = datetime(2026, 9, 24, 2, 6, 58, tzinfo=timezone.utc)


def _seconds(
    count: int,
    *,
    invalid_positions: frozenset[int] = frozenset(),
) -> tuple[ShockWindowSecond, ...]:
    result = []

    for position in range(count):
        valid = position not in invalid_positions
        bid = Fraction(10 + position) if valid else None
        ask = Fraction(30 - position) if valid else None

        result.append(
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

    return tuple(result)


def test_utc_alignment_and_partial_boundary_bars() -> None:
    projection = build_shock_view_bars(
        _seconds(5),
        timeframe_seconds=5,
        max_bars=2,
    )

    assert projection.timeframe_seconds == 5
    assert projection.source_start_utc == _ORIGIN
    assert projection.source_end_utc_exclusive == (_ORIGIN + timedelta(seconds=5))
    assert len(projection.bars) == 2

    first, second = projection.bars

    assert first.start_utc == datetime(2026, 9, 24, 2, 6, 55, tzinfo=timezone.utc)
    assert first.end_utc_exclusive == datetime(
        2026, 9, 24, 2, 7, 0, tzinfo=timezone.utc
    )
    assert (first.first_source_position, first.last_source_position_exclusive) == (0, 2)

    assert second.start_utc == datetime(2026, 9, 24, 2, 7, 0, tzinfo=timezone.utc)
    assert second.end_utc_exclusive == datetime(
        2026, 9, 24, 2, 7, 5, tzinfo=timezone.utc
    )
    assert (second.first_source_position, second.last_source_position_exclusive) == (
        2,
        5,
    )

    assert first.bid == (10.0, 11.0, 10.0, 11.0)
    assert second.bid == (12.0, 14.0, 12.0, 14.0)
    assert first.total == (40.0, 40.0, 40.0, 40.0)
    assert first.delta == (-20.0, -18.0, -20.0, -18.0)


def test_any_invalid_second_nulls_the_entire_l2_viewing_bar() -> None:
    projection = build_shock_view_bars(
        _seconds(5, invalid_positions=frozenset({1})),
        timeframe_seconds=5,
        max_bars=2,
    )

    first, second = projection.bars

    assert first.valid_l2 is False
    assert (
        first.bid,
        first.ask,
        first.total,
        first.delta,
    ) == (None, None, None, None)

    assert second.valid_l2 is True
    assert second.bid is not None


def test_auto_timeframe_chooses_finest_one_that_fits() -> None:
    projection = build_shock_view_bars(
        _seconds(11),
        max_bars=3,
    )

    assert projection.timeframe_seconds == 5
    assert len(projection.bars) == 3


def test_explicit_timeframe_never_silently_coarsens() -> None:
    with pytest.raises(
        ShockViewBarsError,
        match="exceeds max_bars",
    ):
        build_shock_view_bars(
            _seconds(11),
            timeframe_seconds=1,
            max_bars=3,
        )


def test_one_second_projection_keeps_original_values_and_gaps() -> None:
    projection = build_shock_view_bars(
        _seconds(5, invalid_positions=frozenset({3})),
        timeframe_seconds=1,
        max_bars=5,
    )

    assert len(projection.bars) == 5
    assert projection.bars[0].bid == (10.0, 10.0, 10.0, 10.0)
    assert projection.bars[3].bid is None
    assert projection.bars[4].bid == (14.0, 14.0, 14.0, 14.0)
    assert projection.bars[4].first_dataset_index == 104


@pytest.mark.parametrize(
    "budget",
    [0, -1, 5_001, True, 2.5, "1200"],
)
def test_invalid_bar_budgets_are_rejected(
    budget: object,
) -> None:
    with pytest.raises(ShockViewBarsError, match="max_bars"):
        build_shock_view_bars(
            _seconds(1),
            max_bars=budget,
        )


@pytest.mark.parametrize(
    "timeframe",
    [0, 2, 30, True, 5.0, "5"],
)
def test_unsupported_timeframes_are_rejected(
    timeframe: object,
) -> None:
    with pytest.raises(
        ShockViewBarsError,
        match="timeframe",
    ):
        build_shock_view_bars(
            _seconds(1),
            timeframe_seconds=timeframe,
        )


def test_internal_timestamp_gap_is_not_hidden_by_aggregation() -> None:
    original = _seconds(4)
    broken = (
        original[0],
        original[1],
        original[3],
    )

    with pytest.raises(
        ShockViewBarsError,
        match="contiguous UTC seconds",
    ):
        build_shock_view_bars(
            broken,
            timeframe_seconds=5,
        )


def test_dataset_index_gap_is_rejected() -> None:
    original = _seconds(4)
    broken = (
        original[0],
        original[2],
        original[3],
    )

    with pytest.raises(
        ShockViewBarsError,
        match="contiguous UTC seconds",
    ):
        build_shock_view_bars(
            broken,
            timeframe_seconds=5,
        )


def test_bar_limit_does_not_truncate_one_second_source() -> None:
    source = _seconds(11)
    projection = build_shock_view_bars(
        source,
        max_bars=3,
    )

    assert projection.bars[0].first_dataset_index == 100
    assert projection.bars[-1].last_dataset_index == 110
    assert sum(
        bar.last_source_position_exclusive - bar.first_source_position
        for bar in projection.bars
    ) == len(source)

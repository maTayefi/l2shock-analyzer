from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction

import pytest

from l2shock.analysis.l2_view_stream import (
    L2_VIEW_TIMEFRAMES_SECONDS,
    L2ViewError,
    L2ViewLoadOptions,
    select_l2_view_timeframe,
)


def _start() -> datetime:
    return datetime(2094, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("timeframe", L2_VIEW_TIMEFRAMES_SECONDS)
def test_options_normalize_supported_integral_float_timeframe(
    timeframe: int,
) -> None:
    options = L2ViewLoadOptions(timeframe_seconds=float(timeframe))

    assert options.timeframe_seconds == timeframe
    assert type(options.timeframe_seconds) is int


@pytest.mark.parametrize("timeframe", L2_VIEW_TIMEFRAMES_SECONDS)
def test_direct_selection_returns_canonical_integer_timeframe(
    timeframe: int,
) -> None:
    selected = select_l2_view_timeframe(
        _start(),
        _start() + timedelta(seconds=1),
        timeframe_seconds=float(timeframe),
        max_bars=1,
    )

    assert selected == timeframe
    assert type(selected) is int


@pytest.mark.parametrize(
    "value",
    (
        True,
        False,
        0,
        -1,
        2,
        60.5,
        float("nan"),
        float("inf"),
        float("-inf"),
        "60",
        Decimal("60"),
        Fraction(60, 1),
        object(),
    ),
)
def test_invalid_timeframe_is_rejected_at_both_boundaries(
    value: object,
) -> None:
    with pytest.raises(L2ViewError):
        L2ViewLoadOptions(timeframe_seconds=value)

    with pytest.raises(L2ViewError):
        select_l2_view_timeframe(
            _start(),
            _start() + timedelta(seconds=1),
            timeframe_seconds=value,
            max_bars=1,
        )


def test_auto_remains_none_and_selects_finest_fitting_integer() -> None:
    options = L2ViewLoadOptions(timeframe_seconds=None, max_bars=1)

    assert options.timeframe_seconds is None

    selected = select_l2_view_timeframe(
        _start(),
        _start() + timedelta(seconds=60),
        timeframe_seconds=None,
        max_bars=1,
    )

    assert selected == 60
    assert type(selected) is int


def test_integral_float_does_not_bypass_explicit_bar_budget() -> None:
    with pytest.raises(L2ViewError, match="maximum"):
        select_l2_view_timeframe(
            _start(),
            _start() + timedelta(seconds=120),
            timeframe_seconds=60.0,
            max_bars=1,
        )
# l2shock/ui/shock_view_bars.py
"""Bounded, UTC-aligned viewing bars for verified one-second Shock L2.

This module does not detect shocks. It does not change the dataset's
one-second timestamps, B-area coordinates, or review rankings.

A viewing bar containing even one invalid L2 second is null in all four
channels. This avoids visually concealing a one-second data gap inside
an otherwise plausible OHLC candle.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from typing import TypeAlias
from collections.abc import Sequence

from l2shock.analysis.shock_window import ShockWindowSecond

L2Ohlc: TypeAlias = tuple[float, float, float, float]
"""Open, high, low, close. Convert explicitly for ECharts when needed."""

_VIEW_TIMEFRAMES_SECONDS = (1, 5, 15, 60, 300, 900, 3_600)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_SECOND = timedelta(seconds=1)


class ShockViewBarsError(ValueError):
    """Invalid viewing input or a projection exceeding its bar budget."""


@dataclass(frozen=True, slots=True)
class ShockViewBar:
    """One half-open UTC interval; indices refer to the source sequence."""

    start_utc: datetime
    end_utc_exclusive: datetime
    first_source_position: int
    last_source_position_exclusive: int
    first_dataset_index: int
    last_dataset_index: int
    valid_l2: bool
    bid: L2Ohlc | None
    ask: L2Ohlc | None
    total: L2Ohlc | None
    delta: L2Ohlc | None


@dataclass(frozen=True, slots=True)
class ShockViewProjection:
    """A display-only projection. It does not own detection coordinates."""

    timeframe_seconds: int
    requested_max_bars: int
    source_start_utc: datetime
    source_end_utc_exclusive: datetime
    bars: tuple[ShockViewBar, ...]


def _positive_bar_budget(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShockViewBarsError("max_bars must be a positive integer")

    if not 1 <= value <= 5_000:
        raise ShockViewBarsError("max_bars must be between 1 and 5,000")

    return value


def _epoch_second(value: datetime) -> int:
    if (
        value.tzinfo is None
        or value.utcoffset() != timedelta(0)
        or value.microsecond != 0
    ):
        raise ShockViewBarsError("Source timestamps must be exact UTC seconds")

    return (value - _EPOCH) // _ONE_SECOND


def _bucket_id(timestamp_utc: datetime, timeframe_seconds: int) -> int:
    return _epoch_second(timestamp_utc) // timeframe_seconds


def _bucket_start(bucket_id: int, timeframe_seconds: int) -> datetime:
    return _EPOCH + timedelta(seconds=bucket_id * timeframe_seconds)


def _expected_bar_count(
    first: ShockWindowSecond,
    last: ShockWindowSecond,
    timeframe_seconds: int,
) -> int:
    return (
        _bucket_id(last.timestamp_utc, timeframe_seconds)
        - _bucket_id(first.timestamp_utc, timeframe_seconds)
        + 1
    )


def _select_timeframe(
    first: ShockWindowSecond,
    last: ShockWindowSecond,
    *,
    timeframe_seconds: int | None,
    max_bars: int,
) -> int:
    if timeframe_seconds is not None:
        if (
            isinstance(timeframe_seconds, bool)
            or not isinstance(timeframe_seconds, int)
            or timeframe_seconds not in _VIEW_TIMEFRAMES_SECONDS
        ):
            raise ShockViewBarsError(
                "Unsupported viewing timeframe; choose 1, 5, 15, "
                "60, 300, 900, or 3600 seconds"
            )

        if _expected_bar_count(first, last, timeframe_seconds) > max_bars:
            raise ShockViewBarsError(
                "Viewing timeframe exceeds max_bars; choose a "
                "coarser timeframe or a shorter viewport"
            )

        return timeframe_seconds

    for candidate in _VIEW_TIMEFRAMES_SECONDS:
        if _expected_bar_count(first, last, candidate) <= max_bars:
            return candidate

    raise ShockViewBarsError(
        "Even one-hour viewing bars exceed max_bars; choose a "
        "shorter viewport or increase max_bars"
    )


def _ohlc(values: list[Fraction]) -> L2Ohlc:
    if not values:
        raise ShockViewBarsError("Cannot construct an OHLC bar without valid seconds")

    try:
        result = (
            float(values[0]),
            float(max(values)),
            float(min(values)),
            float(values[-1]),
        )
    except (OverflowError, ValueError) as exc:
        raise ShockViewBarsError("L2 value cannot be represented on the chart") from exc

    if not all(math.isfinite(number) for number in result):
        raise ShockViewBarsError("L2 value cannot be represented on the chart")

    return result


def _make_bar(
    seconds: Sequence[ShockWindowSecond],
    first_position: int,
    last_position_exclusive: int,
    *,
    bucket_id: int,
    timeframe_seconds: int,
) -> ShockViewBar:
    group = seconds[first_position:last_position_exclusive]
    if not group:
        raise ShockViewBarsError("Empty viewing bucket")

    valid = all(second.valid_l2 for second in group)

    if valid:
        for second in group:
            if any(
                value is None
                for value in (
                    second.bid,
                    second.ask,
                    second.total,
                    second.delta,
                )
            ):
                raise ShockViewBarsError("Valid L2 second has a missing channel")

        bid = _ohlc([second.bid for second in group])
        ask = _ohlc([second.ask for second in group])
        total = _ohlc([second.total for second in group])
        delta = _ohlc([second.delta for second in group])
    else:
        bid = ask = total = delta = None

    start = _bucket_start(bucket_id, timeframe_seconds)

    return ShockViewBar(
        start_utc=start,
        end_utc_exclusive=start + timedelta(seconds=timeframe_seconds),
        first_source_position=first_position,
        last_source_position_exclusive=last_position_exclusive,
        first_dataset_index=group[0].dataset_index,
        last_dataset_index=group[-1].dataset_index,
        valid_l2=valid,
        bid=bid,
        ask=ask,
        total=total,
        delta=delta,
    )


def build_shock_view_bars(
    seconds: Sequence[ShockWindowSecond],
    *,
    timeframe_seconds: int | None = None,
    max_bars: int = 1_200,
) -> ShockViewProjection:
    """Project contiguous one-second L2 into bounded UTC viewing bars.

    ``None`` chooses the finest supported timeframe that fits the
    budget. An explicit timeframe that does not fit raises rather than
    silently dropping seconds or changing the requested timeframe.
    """
    budget = _positive_bar_budget(max_bars)

    if not seconds:
        raise ShockViewBarsError("Cannot build viewing bars from an empty source")

    first = seconds[0]
    last = seconds[-1]

    # Check the entire source, not only the first and last timestamps.
    previous = None
    for second in seconds:
        _epoch_second(second.timestamp_utc)

        if previous is not None:
            if second.timestamp_utc != (previous.timestamp_utc + _ONE_SECOND):
                raise ShockViewBarsError("Source must contain contiguous UTC seconds")

            if second.dataset_index != previous.dataset_index + 1:
                raise ShockViewBarsError("Source dataset indices must be contiguous")

        previous = second

    selected_timeframe = _select_timeframe(
        first,
        last,
        timeframe_seconds=timeframe_seconds,
        max_bars=budget,
    )

    bars: list[ShockViewBar] = []
    group_first = 0
    current_bucket = _bucket_id(
        first.timestamp_utc,
        selected_timeframe,
    )

    for position in range(1, len(seconds)):
        bucket = _bucket_id(
            seconds[position].timestamp_utc,
            selected_timeframe,
        )

        if bucket != current_bucket:
            bars.append(
                _make_bar(
                    seconds,
                    group_first,
                    position,
                    bucket_id=current_bucket,
                    timeframe_seconds=selected_timeframe,
                )
            )
            group_first = position
            current_bucket = bucket

    bars.append(
        _make_bar(
            seconds,
            group_first,
            len(seconds),
            bucket_id=current_bucket,
            timeframe_seconds=selected_timeframe,
        )
    )

    if len(bars) > budget:
        raise ShockViewBarsError("Viewing projection exceeded max_bars")

    return ShockViewProjection(
        timeframe_seconds=selected_timeframe,
        requested_max_bars=budget,
        source_start_utc=first.timestamp_utc,
        source_end_utc_exclusive=(last.timestamp_utc + _ONE_SECOND),
        bars=tuple(bars),
    )

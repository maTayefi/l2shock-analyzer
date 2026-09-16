# l2shock/analysis/price_filter.py
"""Locked price-filter and segmented-timeline semantics.

The user price filter controls price/time analysis eligibility and display. It
does not remove reconstructed order-book levels and does not change liquidity
depth calculation.

Core rules:

- eligibility uses true exact-Decimal OHLC intersection, not close-only
  inclusion;
- display OHLC may be clipped, but source OHLC is immutable;
- excluded and invalid bars create hard timeline boundaries;
- context may extend segment edges but cannot join separated core segments.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum


class PriceBarState(StrEnum):
    ELIGIBLE = "eligible"
    EXCLUDED = "excluded"
    INVALID = "invalid"


def _positive_decimal(
    field_name: str,
    value: object,
) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric, not boolean")

    if isinstance(value, Decimal):
        result = value
    else:
        text = str(value or "").strip()

        if not text:
            raise ValueError(f"{field_name} cannot be blank")

        try:
            result = Decimal(text)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{field_name} must be a valid decimal value") from exc

    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field_name} must be finite and > 0")

    return result


@dataclass(frozen=True, slots=True)
class PriceBounds:
    min_price: Decimal | None = None
    max_price: Decimal | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "min_price",
            "max_price",
        ):
            value = getattr(self, field_name)

            if value is None:
                continue

            object.__setattr__(
                self,
                field_name,
                _positive_decimal(
                    field_name,
                    value,
                ),
            )

        if (
            self.min_price is not None
            and self.max_price is not None
            and self.min_price > self.max_price
        ):
            raise ValueError("min_price must be <= max_price")


@dataclass(frozen=True, slots=True)
class OHLC:
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    def __post_init__(self) -> None:
        for field_name in (
            "open",
            "high",
            "low",
            "close",
        ):
            object.__setattr__(
                self,
                field_name,
                _positive_decimal(
                    field_name,
                    getattr(self, field_name),
                ),
            )

    @property
    def valid(self) -> bool:
        return bool(
            self.high >= self.low
            and self.high >= self.open
            and self.high >= self.close
            and self.low <= self.open
            and self.low <= self.close
        )


@dataclass(frozen=True, slots=True)
class FilteredPriceSegment:
    """One independently analyzable price-filter segment.

    ``core_*`` identifies consecutive price-filter-eligible observations.

    ``context_*`` may extend the segment over immediately adjacent valid bars,
    but never crosses an invalid observation or timestamp discontinuity.

    Different segment objects remain independent even if their context windows
    overlap.
    """

    core_start: int
    core_end: int
    context_start: int
    context_end: int

    def __post_init__(self) -> None:
        if self.core_start < 0:
            raise ValueError("core_start must be non-negative")
        if self.core_end < self.core_start:
            raise ValueError("core_end must be >= core_start")
        if self.context_start < 0:
            raise ValueError("context_start must be non-negative")
        if self.context_start > self.core_start:
            raise ValueError("context_start must be <= core_start")
        if self.context_end < self.core_end:
            raise ValueError("context_end must be >= core_end")

    @property
    def core_bars(self) -> int:
        return self.core_end - self.core_start + 1

    @property
    def context_bars(self) -> int:
        return self.context_end - self.context_start + 1


def ohlc_intersects_bounds(
    ohlc: OHLC,
    bounds: PriceBounds,
) -> bool:
    """Return whether the exact candle range intersects active bounds."""
    if not ohlc.valid:
        return False

    if bounds.min_price is not None and ohlc.high < bounds.min_price:
        return False

    if bounds.max_price is not None and ohlc.low > bounds.max_price:
        return False

    return True


def classify_price_bar(
    ohlc: OHLC,
    bounds: PriceBounds,
) -> PriceBarState:
    if not ohlc.valid:
        return PriceBarState.INVALID

    if ohlc_intersects_bounds(ohlc, bounds):
        return PriceBarState.ELIGIBLE

    return PriceBarState.EXCLUDED


def clip_ohlc_for_display(
    ohlc: OHLC,
    bounds: PriceBounds,
) -> OHLC:
    """Return exact display-only OHLC clipped to the active bounds.

    This function must never overwrite authoritative Binance trade OHLC in
    storage or provenance.
    """
    state = classify_price_bar(
        ohlc,
        bounds,
    )

    if state is PriceBarState.INVALID:
        raise ValueError("Cannot display-clip an invalid OHLC candle")

    if state is PriceBarState.EXCLUDED:
        raise ValueError("A completely excluded candle has no filtered display")

    def _clip(value: Decimal) -> Decimal:
        result = value

        if bounds.min_price is not None:
            result = max(
                result,
                bounds.min_price,
            )

        if bounds.max_price is not None:
            result = min(
                result,
                bounds.max_price,
            )

        return result

    result = OHLC(
        open=_clip(ohlc.open),
        high=_clip(ohlc.high),
        low=_clip(ohlc.low),
        close=_clip(ohlc.close),
    )

    if not result.valid:
        raise RuntimeError(
            "Display clipping produced invalid OHLC; this indicates an "
            "internal price-filter implementation error"
        )

    return result


def _timestamps_are_adjacent(
    first: datetime,
    second: datetime,
    expected_step: timedelta,
) -> bool:
    if first.tzinfo is None or first.utcoffset() is None:
        raise ValueError("Price timestamps must be timezone-aware")

    if second.tzinfo is None or second.utcoffset() is None:
        raise ValueError("Price timestamps must be timezone-aware")

    return second - first == expected_step


def build_filtered_segments(
    timestamps: Sequence[datetime],
    candles: Sequence[OHLC],
    bounds: PriceBounds,
    *,
    expected_step: timedelta,
    context_before: int = 3,
    context_after: int = 3,
) -> tuple[
    tuple[FilteredPriceSegment, ...],
    tuple[PriceBarState, ...],
]:
    """Build independent eligible segments and bounded edge context.

    Eligible core bars must be consecutive in both array position and real
    time. Excluded bars, invalid bars, or timestamp gaps close the core
    segment.

    Context may include adjacent excluded-but-valid observations, but it stops
    at invalid observations and timestamp discontinuities. Context never
    merges two core segments into one result.
    """
    if len(timestamps) != len(candles):
        raise ValueError("timestamps and candles must have equal length")

    if expected_step <= timedelta(0):
        raise ValueError("expected_step must be positive")

    if isinstance(context_before, bool) or isinstance(context_after, bool):
        raise ValueError("context bar counts must be integers, not booleans")

    context_before = int(context_before)
    context_after = int(context_after)

    if context_before < 0 or context_after < 0:
        raise ValueError("context bar counts must be non-negative")

    states = tuple(
        classify_price_bar(
            candle,
            bounds,
        )
        for candle in candles
    )

    core_ranges: list[tuple[int, int]] = []
    core_start: int | None = None

    for index, state in enumerate(states):
        adjacent_to_previous = bool(
            index > 0
            and _timestamps_are_adjacent(
                timestamps[index - 1],
                timestamps[index],
                expected_step,
            )
        )

        eligible_continuation = bool(
            state is PriceBarState.ELIGIBLE and (index == 0 or adjacent_to_previous)
        )

        if eligible_continuation:
            if core_start is None:
                core_start = index
            continue

        if core_start is not None:
            core_ranges.append(
                (
                    core_start,
                    index - 1,
                )
            )
            core_start = None

        if state is PriceBarState.ELIGIBLE:
            core_start = index

    if core_start is not None:
        core_ranges.append(
            (
                core_start,
                len(states) - 1,
            )
        )

    segments: list[FilteredPriceSegment] = []

    for range_start, range_end in core_ranges:
        context_start = range_start
        context_end = range_end

        remaining_before = context_before

        while context_start > 0 and remaining_before > 0:
            candidate = context_start - 1

            if states[candidate] is not PriceBarState.EXCLUDED:
                break

            if not _timestamps_are_adjacent(
                timestamps[candidate],
                timestamps[context_start],
                expected_step,
            ):
                break

            context_start = candidate
            remaining_before -= 1

        remaining_after = context_after

        while context_end + 1 < len(states) and remaining_after > 0:
            candidate = context_end + 1

            if states[candidate] is not PriceBarState.EXCLUDED:
                break

            if not _timestamps_are_adjacent(
                timestamps[context_end],
                timestamps[candidate],
                expected_step,
            ):
                break

            context_end = candidate
            remaining_after -= 1

        segments.append(
            FilteredPriceSegment(
                core_start=range_start,
                core_end=range_end,
                context_start=context_start,
                context_end=context_end,
            )
        )

    return tuple(segments), states


__all__ = [
    "FilteredPriceSegment",
    "OHLC",
    "PriceBarState",
    "PriceBounds",
    "build_filtered_segments",
    "classify_price_bar",
    "clip_ohlc_for_display",
    "ohlc_intersects_bounds",
]

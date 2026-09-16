# l2shock/analysis/timeframes.py
"""Canonical fixed-duration analytical timeframe ownership.

This module owns:

- fixed-duration timeframe identities;
- UTC bucket alignment;
- permissive closed-endpoint analysis-range snapping;
- automatic chart-timeframe selection;
- aligned bucket iteration.

Calendar-month timeframes are intentionally excluded. A month is not a fixed
number of seconds and requires a separate calendar-aware contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final
from collections.abc import Iterator

from l2shock.timeutils import require_aware_utc

_EPOCH_UTC: Final[datetime] = datetime(
    1970,
    1,
    1,
    tzinfo=timezone.utc,
)

# ISO-style Monday anchor for weekly bars.
_WEEK_ANCHOR_UTC: Final[datetime] = datetime(
    1970,
    1,
    5,
    tzinfo=timezone.utc,
)


class TimeframeError(ValueError):
    """A timeframe identity or aligned range is invalid."""


@dataclass(frozen=True, slots=True)
class Timeframe:
    """One canonical fixed-duration analytical timeframe."""

    label: str
    seconds: int
    anchor_utc: datetime = _EPOCH_UTC

    def __post_init__(self) -> None:
        label = str(self.label or "").strip()

        if not label:
            raise TimeframeError("Timeframe label cannot be blank")

        if (
            isinstance(self.seconds, bool)
            or not isinstance(self.seconds, int)
            or self.seconds <= 0
        ):
            raise TimeframeError("Timeframe seconds must be a positive integer")

        anchor = require_aware_utc(
            "anchor_utc",
            self.anchor_utc,
        )

        object.__setattr__(self, "label", label)
        object.__setattr__(self, "anchor_utc", anchor)

    @property
    def duration(self) -> timedelta:
        return timedelta(seconds=self.seconds)


_FIXED_TIMEFRAMES: Final[tuple[Timeframe, ...]] = (
    Timeframe("1s", 1),
    Timeframe("5s", 5),
    Timeframe("10s", 10),
    Timeframe("15s", 15),
    Timeframe("30s", 30),
    Timeframe("1m", 60),
    Timeframe("3m", 3 * 60),
    Timeframe("5m", 5 * 60),
    Timeframe("10m", 10 * 60),
    Timeframe("15m", 15 * 60),
    Timeframe("30m", 30 * 60),
    Timeframe("45m", 45 * 60),
    Timeframe("1h", 60 * 60),
    Timeframe("2h", 2 * 60 * 60),
    Timeframe("4h", 4 * 60 * 60),
    Timeframe("8h", 8 * 60 * 60),
    Timeframe("12h", 12 * 60 * 60),
    Timeframe("1d", 24 * 60 * 60),
    Timeframe("3d", 3 * 24 * 60 * 60),
    Timeframe(
        "1w",
        7 * 24 * 60 * 60,
        anchor_utc=_WEEK_ANCHOR_UTC,
    ),
)

TIMEFRAMES: Final[tuple[Timeframe, ...]] = _FIXED_TIMEFRAMES
TIMEFRAME_BY_LABEL: Final[dict[str, Timeframe]] = {
    timeframe.label: timeframe for timeframe in TIMEFRAMES
}


@dataclass(frozen=True, slots=True)
class SnappedAnalysisRange:
    """One bar-aligned half-open range derived from closed user endpoints."""

    requested_start_utc: datetime
    requested_end_utc: datetime
    start_utc: datetime
    end_utc: datetime
    timeframe: Timeframe

    def __post_init__(self) -> None:
        requested_start = require_aware_utc(
            "requested_start_utc",
            self.requested_start_utc,
        )
        requested_end = require_aware_utc(
            "requested_end_utc",
            self.requested_end_utc,
        )
        start = require_aware_utc(
            "start_utc",
            self.start_utc,
        )
        end = require_aware_utc(
            "end_utc",
            self.end_utc,
        )

        if requested_end < requested_start:
            raise TimeframeError("requested_end_utc cannot precede requested_start_utc")

        if end <= start:
            raise TimeframeError("Snapped analysis range must be non-empty")

        if floor_to_timeframe(start, self.timeframe) != start:
            raise TimeframeError("Snapped start is not timeframe-aligned")

        if floor_to_timeframe(end, self.timeframe) != end:
            raise TimeframeError("Snapped end is not timeframe-aligned")

        if requested_start < start or requested_start >= end:
            raise TimeframeError(
                "Snapped analysis range does not contain requested start"
            )

        if requested_end < start or requested_end >= end:
            raise TimeframeError(
                "Snapped analysis range does not contain requested end"
            )

        object.__setattr__(self, "requested_start_utc", requested_start)
        object.__setattr__(self, "requested_end_utc", requested_end)
        object.__setattr__(self, "start_utc", start)
        object.__setattr__(self, "end_utc", end)

    @property
    def bar_count(self) -> int:
        seconds = int((self.end_utc - self.start_utc).total_seconds())
        return seconds // self.timeframe.seconds


def get_timeframe(value: Timeframe | str) -> Timeframe:
    """Return one registered fixed-duration timeframe."""
    if isinstance(value, Timeframe):
        registered = TIMEFRAME_BY_LABEL.get(value.label)

        if registered != value:
            raise TimeframeError(
                "Custom timeframe objects are not supported by this registry"
            )

        return registered

    label = str(value or "").strip()

    try:
        return TIMEFRAME_BY_LABEL[label]
    except KeyError as exc:
        allowed = ", ".join(item.label for item in TIMEFRAMES)
        raise TimeframeError(
            f"Unsupported fixed-duration timeframe {label!r}; "
            f"allowed values: {allowed}"
        ) from exc


def floor_to_timeframe(
    value_utc: datetime,
    timeframe: Timeframe | str,
) -> datetime:
    """Floor one exact UTC instant to its containing timeframe bucket."""
    value = require_aware_utc(
        "value_utc",
        value_utc,
    )
    spec = get_timeframe(timeframe)

    elapsed = value - spec.anchor_utc
    elapsed_microseconds = (
        elapsed.days * 86_400_000_000
        + elapsed.seconds * 1_000_000
        + elapsed.microseconds
    )
    duration_microseconds = spec.seconds * 1_000_000

    bucket_number = elapsed_microseconds // duration_microseconds

    return spec.anchor_utc + timedelta(
        microseconds=bucket_number * duration_microseconds
    )


def snap_closed_analysis_range(
    requested_start_utc: datetime,
    requested_end_utc: datetime,
    timeframe: Timeframe | str,
) -> SnappedAnalysisRange:
    """Snap closed user endpoints into an inclusive-edge bar range.

    User intent is interpreted as the closed continuous-time range ``[m, n]``.
    The returned storage/iteration range is half-open:

        [floor(m, T), floor(n, T) + T)

    Therefore both bars containing the selected endpoints are included.
    """
    start = require_aware_utc(
        "requested_start_utc",
        requested_start_utc,
    )
    end = require_aware_utc(
        "requested_end_utc",
        requested_end_utc,
    )
    spec = get_timeframe(timeframe)

    if end < start:
        raise TimeframeError("requested_end_utc cannot precede requested_start_utc")

    snapped_start = floor_to_timeframe(start, spec)
    snapped_end = floor_to_timeframe(end, spec) + spec.duration

    return SnappedAnalysisRange(
        requested_start_utc=start,
        requested_end_utc=end,
        start_utc=snapped_start,
        end_utc=snapped_end,
        timeframe=spec,
    )


def iter_timeframe_buckets(
    start_utc: datetime,
    end_utc: datetime,
    timeframe: Timeframe | str,
) -> Iterator[tuple[int, datetime, datetime]]:
    """Yield aligned half-open buckets covering ``[start_utc, end_utc)``."""
    start = require_aware_utc("start_utc", start_utc)
    end = require_aware_utc("end_utc", end_utc)
    spec = get_timeframe(timeframe)

    if end <= start:
        raise TimeframeError("end_utc must be after start_utc")

    if floor_to_timeframe(start, spec) != start:
        raise TimeframeError("start_utc must be timeframe-aligned")

    if floor_to_timeframe(end, spec) != end:
        raise TimeframeError("end_utc must be timeframe-aligned")

    current = start
    index = 0

    while current < end:
        bucket_end = current + spec.duration

        if bucket_end > end:
            raise TimeframeError(
                "Requested range contains a partial final timeframe bucket"
            )

        yield index, current, bucket_end

        current = bucket_end
        index += 1


def select_chart_timeframe(
    requested_start_utc: datetime,
    requested_end_utc: datetime,
    *,
    maximum_bars: int,
    candidates: tuple[Timeframe, ...] = TIMEFRAMES,
) -> Timeframe:
    """Choose the finest registered timeframe fitting the chart bar budget."""
    if (
        isinstance(maximum_bars, bool)
        or not isinstance(maximum_bars, int)
        or maximum_bars <= 0
    ):
        raise TimeframeError("maximum_bars must be a positive integer")

    if not candidates:
        raise TimeframeError("At least one chart timeframe candidate is required")

    normalized_candidates = tuple(get_timeframe(item) for item in candidates)

    if tuple(sorted(normalized_candidates, key=lambda item: item.seconds)) != (
        normalized_candidates
    ):
        raise TimeframeError(
            "Chart timeframe candidates must be ordered finest to coarsest"
        )

    for timeframe in normalized_candidates:
        snapped = snap_closed_analysis_range(
            requested_start_utc,
            requested_end_utc,
            timeframe,
        )

        if snapped.bar_count <= maximum_bars:
            return timeframe

    # The coarsest registered timeframe is returned when no candidate can fit
    # the requested budget. The caller may warn that the bar target was
    # exceeded, but the selection remains deterministic.
    return normalized_candidates[-1]


__all__ = [
    "TIMEFRAMES",
    "TIMEFRAME_BY_LABEL",
    "SnappedAnalysisRange",
    "Timeframe",
    "TimeframeError",
    "floor_to_timeframe",
    "get_timeframe",
    "iter_timeframe_buckets",
    "select_chart_timeframe",
    "snap_closed_analysis_range",
]

# l2shock/ui/shock_warning_regions.py
"""Persistent data-outage warning regions for Shock-Start charts.

Presentation only. Regions never change detection, review identity, B-area
ordering, scale thresholds, or stored data.

A region marks a run of consecutive whole UTC seconds in which a channel is
invalid or unavailable. Invalid and missing seconds are equivalent here and
contiguous runs of either merge. A region is reported only when its run is
strictly longer than the configured threshold.

Run length is judged on the complete loaded window before viewport clipping,
so a long outage that is only partly visible is still highlighted.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

__all__ = [
    "WARNING_REGION_COLOR",
    "ShockWarningChannel",
    "ShockWarningRegion",
    "ShockWarningRegionError",
    "clip_warning_regions",
    "find_long_outage_regions",
    "l2_warning_observations",
    "warning_mark_area_items",
    "warning_threshold_seconds_from_minutes",
    "ShockWarningOverlay",
    "dataset_l2_warning_regions",
    "l2_dataset_warning_observations",
    "with_shock_warning_regions",
]

_ONE_SECOND = timedelta(seconds=1)

WARNING_REGION_COLOR = "rgba(220, 38, 38, 0.14)"


class ShockWarningRegionError(ValueError):
    """Raised when warning-region input violates the one-second contract."""


class ShockWarningChannel(StrEnum):
    L2 = "l2"
    PRICE = "price"


@dataclass(frozen=True, slots=True)
class ShockWarningRegion:
    channel: ShockWarningChannel
    start_utc: datetime
    end_utc_exclusive: datetime

    @property
    def duration_seconds(self) -> int:
        return (self.end_utc_exclusive - self.start_utc) // _ONE_SECOND


def _utc_second(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ShockWarningRegionError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ShockWarningRegionError(f"{name} must be timezone-aware UTC")
    if value.microsecond != 0:
        raise ShockWarningRegionError(f"{name} must be a whole UTC second")
    return value.astimezone(UTC)


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ShockWarningRegionError(f"{name} must be a positive integer")
    return value


def warning_threshold_seconds_from_minutes(minutes: object) -> int:
    return _positive_int(minutes, "warning threshold minutes") * 60


def find_long_outage_regions(
    observations: Iterable[tuple[datetime, bool]],
    *,
    channel: ShockWarningChannel | str,
    window_start_utc: datetime,
    window_end_utc_exclusive: datetime,
    minimum_exclusive_seconds: int,
) -> tuple[ShockWarningRegion, ...]:
    try:
        resolved_channel = ShockWarningChannel(channel)
    except ValueError as error:
        raise ShockWarningRegionError(f"unknown channel: {channel!r}") from error

    start = _utc_second(window_start_utc, "window_start_utc")
    end = _utc_second(window_end_utc_exclusive, "window_end_utc_exclusive")
    if end < start:
        raise ShockWarningRegionError("window end precedes window start")
    threshold = _positive_int(minimum_exclusive_seconds, "minimum_exclusive_seconds")

    regions: list[ShockWarningRegion] = []

    def close(run_start: datetime | None, run_end: datetime) -> None:
        if run_start is not None and (run_end - run_start) // _ONE_SECOND > threshold:
            regions.append(ShockWarningRegion(resolved_channel, run_start, run_end))

    run_start: datetime | None = None
    cursor = start

    for raw_timestamp, valid in observations:
        timestamp = _utc_second(raw_timestamp, "observation timestamp")
        if timestamp < cursor:
            raise ShockWarningRegionError(
                "observations must be strictly increasing and inside the window"
            )
        if timestamp >= end:
            raise ShockWarningRegionError("observation lies outside the window")
        if type(valid) is not bool:
            raise ShockWarningRegionError("observation validity must be a bool")

        if timestamp > cursor and run_start is None:
            run_start = cursor  # unavailable seconds [cursor, timestamp)

        if valid:
            close(run_start, timestamp)
            run_start = None
        elif run_start is None:
            run_start = timestamp

        cursor = timestamp + _ONE_SECOND

    if cursor < end and run_start is None:
        run_start = cursor
    close(run_start, end)

    return tuple(regions)


def l2_warning_observations(
    seconds: Sequence[Any],
) -> tuple[tuple[datetime, bool], ...]:
    """Project ShockWindowSecond-like rows to (timestamp_utc, valid_l2)."""
    return tuple((second.timestamp_utc, second.valid_l2) for second in seconds)


def clip_warning_regions(
    regions: Iterable[ShockWarningRegion],
    *,
    view_start_utc: datetime,
    view_end_utc_exclusive: datetime,
) -> tuple[ShockWarningRegion, ...]:
    view_start = _utc_second(view_start_utc, "view_start_utc")
    view_end = _utc_second(view_end_utc_exclusive, "view_end_utc_exclusive")
    clipped: list[ShockWarningRegion] = []

    for region in regions:
        start = max(region.start_utc, view_start)
        end = min(region.end_utc_exclusive, view_end)
        if start < end:
            clipped.append(ShockWarningRegion(region.channel, start, end))

    return tuple(clipped)


def warning_mark_area_items(
    regions: Iterable[ShockWarningRegion],
) -> list[list[dict[str, Any]]]:
    """ECharts markArea items; append after existing B-band items."""
    return [
        [
            {
                "name": f"l2shock-warning:{region.channel.value}",
                "xAxis": region.start_utc.isoformat(),
                "itemStyle": {"color": WARNING_REGION_COLOR},
            },
            {"xAxis": region.end_utc_exclusive.isoformat()},
        ]
        for region in regions
    ]


_PRICE_SERIES_ID = "shock-view-price-context"
_L2_SERIES_IDS = frozenset(
    {
        "shock-view-bid",
        "shock-view-ask",
        "shock-view-total",
        "shock-view-delta",
    }
)


@dataclass(frozen=True, slots=True)
class ShockWarningOverlay:
    """Already-clipped regions for one bounded viewport (presentation only)."""

    l2_regions: tuple[ShockWarningRegion, ...] = ()
    price_regions: tuple[ShockWarningRegion, ...] = ()

    def __post_init__(self) -> None:
        l2_regions = tuple(self.l2_regions)
        price_regions = tuple(self.price_regions)

        for regions, channel in (
            (l2_regions, ShockWarningChannel.L2),
            (price_regions, ShockWarningChannel.PRICE),
        ):
            for region in regions:
                if (
                    not isinstance(region, ShockWarningRegion)
                    or region.channel is not channel
                ):
                    raise ShockWarningRegionError(
                        f"{channel.value} overlay accepts only "
                        f"{channel.value} warning regions"
                    )

        object.__setattr__(self, "l2_regions", l2_regions)
        object.__setattr__(self, "price_regions", price_regions)

    @property
    def is_empty(self) -> bool:
        return not self.l2_regions and not self.price_regions


def l2_dataset_warning_observations(
    seconds: Sequence[Any],
) -> tuple[tuple[datetime, bool], ...]:
    """Project verified L2Second rows to (timestamp_utc, usable).

    A second is usable only when its quality is VALID and it is not
    partial-market coverage. Everything else counts as an outage.
    """
    return tuple(
        (
            second.timestamp_utc,
            str(getattr(second.quality, "value", second.quality)) == "VALID"
            and not bool(getattr(second, "coverage_degraded", False)),
        )
        for second in seconds
    )


def dataset_l2_warning_regions(
    seconds: Sequence[Any],
    *,
    window_start_utc: datetime,
    window_end_utc_exclusive: datetime,
    minimum_exclusive_seconds: int,
) -> tuple[ShockWarningRegion, ...]:
    """Long L2 outages over the complete loaded scan (clip afterwards)."""
    return find_long_outage_regions(
        l2_dataset_warning_observations(seconds),
        channel=ShockWarningChannel.L2,
        window_start_utc=window_start_utc,
        window_end_utc_exclusive=window_end_utc_exclusive,
        minimum_exclusive_seconds=minimum_exclusive_seconds,
    )


def with_shock_warning_regions(
    option: dict[str, Any],
    *,
    overlay: ShockWarningOverlay,
) -> dict[str, Any]:
    """Append red outage markArea items; never mutates ``option``.

    L2 regions go to all five panels, price regions to the Price panel
    only. Apply this after the annotation-visibility transform so hiding
    B-area bands can never remove a warning region (and vice versa).
    """
    if not isinstance(option, dict):
        raise TypeError("option must be a dictionary")

    if not isinstance(overlay, ShockWarningOverlay):
        raise TypeError("overlay must be ShockWarningOverlay")

    series_list = option.get("series")

    if overlay.is_empty or not isinstance(series_list, list):
        return option

    result = dict(option)
    new_series: list[Any] = []

    for series in series_list:
        if not isinstance(series, dict):
            new_series.append(series)
            continue

        series_id = series.get("id")

        if series_id == _PRICE_SERIES_ID:
            regions = overlay.l2_regions + overlay.price_regions
        elif series_id in _L2_SERIES_IDS:
            regions = overlay.l2_regions
        else:
            regions = ()

        if not regions:
            new_series.append(series)
            continue

        raw_area = series.get("markArea")
        area = (
            dict(raw_area)
            if isinstance(raw_area, dict)
            else {"silent": True, "animation": False, "label": {"show": False}}
        )
        existing = area.get("data")
        area["data"] = [
            *(existing if isinstance(existing, list) else []),
            *warning_mark_area_items(regions),
        ]

        copied = dict(series)
        copied["markArea"] = area
        new_series.append(copied)

    result["series"] = new_series
    return result

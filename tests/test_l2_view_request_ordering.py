# tests/test_l2_view_request_ordering.py
"""Original endpoint ordering precedes one-second slot assignment."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from l2shock.analysis.l2_view_stream import (
    L2ViewError,
    L2ViewRequest,
    closed_request_slots,
)

ORIGIN = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)


def test_reversed_instants_inside_same_second_are_rejected() -> None:
    start = ORIGIN + timedelta(seconds=7, microseconds=900_000)
    end = ORIGIN + timedelta(seconds=7, microseconds=100_000)

    with pytest.raises(L2ViewError, match="End must not precede Start"):
        closed_request_slots(start, end)


def test_request_rejects_reversed_instants_inside_same_second() -> None:
    with pytest.raises(L2ViewError, match="End must not precede Start"):
        L2ViewRequest(
            base="BTC",
            preset_hash="a" * 64,
            requested_start_utc=(ORIGIN + timedelta(seconds=7, microseconds=900_000)),
            requested_end_utc=(ORIGIN + timedelta(seconds=7, microseconds=100_000)),
        )


def test_ordered_instants_inside_same_second_own_one_slot() -> None:
    start = ORIGIN + timedelta(seconds=7, microseconds=100_000)
    end = ORIGIN + timedelta(seconds=7, microseconds=900_000)

    assert closed_request_slots(start, end) == (
        ORIGIN + timedelta(seconds=7),
        ORIGIN + timedelta(seconds=8),
    )


def test_equal_fractional_endpoints_own_one_slot() -> None:
    instant = ORIGIN + timedelta(seconds=7, microseconds=500_000)

    assert closed_request_slots(instant, instant) == (
        ORIGIN + timedelta(seconds=7),
        ORIGIN + timedelta(seconds=8),
    )


def test_documented_fractional_range_keeps_containing_seconds() -> None:
    start = ORIGIN + timedelta(seconds=7, microseconds=500_000)
    end = ORIGIN + timedelta(seconds=9, microseconds=100_000)

    assert closed_request_slots(start, end) == (
        ORIGIN + timedelta(seconds=7),
        ORIGIN + timedelta(seconds=10),
    )


def test_endpoint_ordering_uses_utc_instants_not_local_clock_text() -> None:
    local_zone = timezone(timedelta(hours=3, minutes=30))
    start = ORIGIN + timedelta(seconds=7, microseconds=100_000)
    end = (ORIGIN + timedelta(seconds=7, microseconds=900_000)).astimezone(local_zone)

    assert closed_request_slots(start, end) == (
        ORIGIN + timedelta(seconds=7),
        ORIGIN + timedelta(seconds=8),
    )


def test_reversed_cross_timezone_instants_are_rejected() -> None:
    local_zone = timezone(timedelta(hours=3, minutes=30))
    start = (ORIGIN + timedelta(seconds=7, microseconds=900_000)).astimezone(local_zone)
    end = ORIGIN + timedelta(seconds=7, microseconds=100_000)

    with pytest.raises(L2ViewError, match="End must not precede Start"):
        closed_request_slots(start, end)

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from l2shock.timeutils import (
    floor_to_hour,
    from_unix_timestamp,
    local_to_utc,
    require_aware_utc,
    require_utc_hour,
)


def test_tehran_local_time_converts_to_utc() -> None:
    local = datetime(2026, 9, 2, 15, 30)

    result = local_to_utc(
        local,
        "Asia/Tehran",
        original_text="2026-09-02 15:30",
    )

    assert result.tzinfo is timezone.utc
    assert result == datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


def test_require_aware_utc_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        require_aware_utc("value", datetime(2026, 9, 2, 12, 0))


def test_require_utc_hour_rejects_minute_offset() -> None:
    with pytest.raises(ValueError, match="exact UTC hour"):
        require_utc_hour(
            "hour",
            datetime(2026, 9, 2, 12, 1, tzinfo=timezone.utc),
        )


def test_floor_to_hour_preserves_timezone() -> None:
    value = datetime(
        2026,
        9,
        2,
        12,
        34,
        56,
        123456,
        tzinfo=timezone.utc,
    )

    assert floor_to_hour(value) == datetime(
        2026,
        9,
        2,
        12,
        0,
        tzinfo=timezone.utc,
    )


def test_timestamp_unit_detection_matches_real_sample() -> None:
    received_ns = 1788350400007361359
    event_ms = 1788350399886

    received = from_unix_timestamp(received_ns)
    event = from_unix_timestamp(event_ms)

    assert received.year == 2026
    assert event.year == 2026
    assert abs((received - event).total_seconds()) < 1.0

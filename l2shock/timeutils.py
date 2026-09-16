# l2shock/timeutils.py
"""Timezone and timestamp utilities.

Project invariant:

- persisted timestamps are timezone-aware UTC;
- user input is interpreted in the configured IANA timezone;
- ambiguous/nonexistent local times are rejected;
- derived hourly identities use exact UTC hour boundaries.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def _localize_strict(
    value: datetime,
    tz_name: str,
    *,
    original_text: str,
) -> datetime:
    timezone_local = ZoneInfo(tz_name)
    candidates: dict[datetime, datetime] = {}

    for fold in (0, 1):
        aware = value.replace(tzinfo=timezone_local, fold=fold)
        round_trip = (
            aware.astimezone(timezone.utc)
            .astimezone(timezone_local)
            .replace(tzinfo=None, fold=0)
        )

        if round_trip == value.replace(fold=0):
            candidates[aware.astimezone(timezone.utc)] = aware

    if not candidates:
        raise ValueError(
            f"Local time {original_text!r} does not exist in timezone "
            f"{tz_name!r} because of a DST transition."
        )

    if len(candidates) > 1:
        raise ValueError(
            f"Local time {original_text!r} is ambiguous in timezone "
            f"{tz_name!r} because of a DST transition."
        )

    return next(iter(candidates.values()))


def local_to_utc(
    value: datetime,
    tz_name: str,
    *,
    original_text: str | None = None,
) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"local_to_utc expects datetime, got {type(value).__name__}")

    if value.tzinfo is None or value.utcoffset() is None:
        value = _localize_strict(
            value,
            tz_name,
            original_text=original_text or value.isoformat(sep=" "),
        )

    return value.astimezone(timezone.utc)


def utc_to_local(value: datetime, tz_name: str) -> datetime:
    return require_aware_utc("value", value).astimezone(ZoneInfo(tz_name))


def require_aware_utc(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")

    result = value.astimezone(timezone.utc)

    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must have UTC offset +00:00")

    return result


def require_utc_hour(name: str, value: datetime) -> datetime:
    result = require_aware_utc(name, value)

    if result.minute != 0 or result.second != 0 or result.microsecond != 0:
        raise ValueError(f"{name} must be aligned to an exact UTC hour")

    return result


def floor_to_second(value: datetime) -> datetime:
    return value.replace(microsecond=0)


def floor_to_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def floor_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def from_unix_timestamp(value: int | float) -> datetime:
    """Convert common epoch units to timezone-aware UTC.

    Supported magnitude families:

    - seconds;
    - milliseconds;
    - microseconds;
    - nanoseconds.
    """
    if isinstance(value, bool):
        raise ValueError("UNIX timestamp must be numeric, not boolean")

    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid UNIX timestamp: {value!r}") from exc

    if not math.isfinite(numeric):
        raise ValueError(f"Invalid non-finite UNIX timestamp: {value!r}")

    magnitude = abs(numeric)

    if magnitude >= 1e17:
        numeric /= 1_000_000_000.0
    elif magnitude >= 1e14:
        numeric /= 1_000_000.0
    elif magnitude >= 1e11:
        numeric /= 1_000.0

    try:
        return datetime.fromtimestamp(numeric, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError(
            f"Timestamp {value!r} normalized to {numeric!r} seconds "
            "is outside the supported datetime range"
        ) from exc


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "floor_to_hour",
    "floor_to_minute",
    "floor_to_second",
    "from_unix_timestamp",
    "local_to_utc",
    "now_utc",
    "require_aware_utc",
    "require_utc_hour",
    "utc_to_local",
]

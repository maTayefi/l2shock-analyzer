# l2shock/acquisition/release_schedule.py
"""Shared CryptoHFTData completed-hour release scheduling.

This module owns only deterministic source-hour eligibility.

Both the local Automatic Fetch runtime and the future headless remote worker
must use this same function. Scheduling environments may run late or retry, but
they must not reinterpret which UTC source hour is eligible.

CryptoHFTData is expected to publish an hourly archive after the configured
release delay. For example, with a 15-minute delay:

    source hour:
        12:00:00 through 12:59:59 UTC

    first expected eligibility:
        13:15 UTC
"""

from __future__ import annotations

from datetime import datetime, timedelta

from l2shock.timeutils import (
    floor_to_hour,
    require_aware_utc,
    require_utc_hour,
)


def latest_release_eligible_hour(
    now: datetime,
    *,
    release_delay_minutes: int,
) -> datetime:
    """Return the newest completed source hour whose delay has elapsed.

    At exactly ``13:15 UTC`` with a 15-minute delay, the newest eligible source
    hour is ``12:00 UTC``.

    At ``13:14:59 UTC``, that source hour is not yet eligible, so the result is
    ``11:00 UTC``.
    """

    if (
        isinstance(release_delay_minutes, bool)
        or not isinstance(release_delay_minutes, int)
        or release_delay_minutes <= 0
    ):
        raise ValueError("release_delay_minutes must be a positive integer")

    current = require_aware_utc(
        "now",
        now,
    )
    delayed = current - timedelta(
        minutes=release_delay_minutes,
    )

    return require_utc_hour(
        "eligible_hour",
        floor_to_hour(delayed) - timedelta(hours=1),
    )


__all__ = ["latest_release_eligible_hour"]

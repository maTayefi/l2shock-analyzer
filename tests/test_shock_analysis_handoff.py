# tests/test_shock_analysis_handoff.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from l2shock.ui.tab_shock_review import _shock_handoff_range, build_shock_request

_UTC = timezone.utc
_HASH = "b" * 64


def test_short_window_is_unchanged() -> None:
    start = datetime(2026, 9, 23, 17, 0, tzinfo=_UTC)
    end = datetime(2026, 9, 24, 2, 59, 59, tzinfo=_UTC)

    assert _shock_handoff_range(start, end) == (start, end, False)


def test_exact_24_hour_closed_window_is_unchanged() -> None:
    start = datetime(2026, 9, 23, 0, 0, tzinfo=_UTC)
    end = start + timedelta(hours=24) - timedelta(seconds=1)

    assert _shock_handoff_range(start, end) == (start, end, False)


def test_long_window_keeps_newest_24_hours_and_is_a_valid_request() -> None:
    start = datetime(2026, 9, 20, 0, 0, tzinfo=_UTC)
    end = datetime(2026, 9, 24, 11, 59, 59, tzinfo=_UTC)

    clipped_start, clipped_end, clipped = _shock_handoff_range(start, end)

    assert clipped is True
    assert clipped_end == end
    assert clipped_start == datetime(2026, 9, 23, 12, 0, tzinfo=_UTC)

    request, _config = build_shock_request(
        base="BTC",
        preset_hash=_HASH,
        start_utc=clipped_start.isoformat(),
        end_utc=clipped_end.isoformat(),
        major_fraction=0.20,
        medium_fraction=0.10,
        minor_fraction=0.05,
    )
    assert request.requested_start_utc == clipped_start


def test_naive_or_reversed_windows_are_rejected() -> None:
    naive = datetime(2026, 9, 23, 0, 0)
    aware = datetime(2026, 9, 23, 0, 0, tzinfo=_UTC)

    with pytest.raises(ValueError):
        _shock_handoff_range(naive, aware)

    with pytest.raises(ValueError):
        _shock_handoff_range(aware, aware - timedelta(seconds=1))

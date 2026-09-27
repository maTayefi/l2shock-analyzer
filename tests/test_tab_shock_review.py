# tests/test_tab_shock_review.py
from datetime import datetime, timezone

import pytest

from l2shock.ui.tab_shock_review import build_shock_request


def test_shock_ui_request_is_l2_only_and_exposes_three_scale_knobs():
    request, config = build_shock_request(
        base="BTC",
        preset_hash="a" * 64,
        start_utc="2026-09-24T00:00:00Z",
        end_utc="2026-09-24T00:10:00+00:00",
        major_fraction="0.20",
        medium_fraction="0.10",
        minor_fraction="0.05",
    )

    assert request.base == "BTC"
    assert request.requested_start_utc == datetime(2026, 9, 24, tzinfo=timezone.utc)
    assert [str(scale.minimum_leg_fraction) for scale in config.scales] == [
        "0.20",
        "0.10",
        "0.05",
    ]
    assert [scale.pivot_radius_seconds for scale in config.scales] == [
        60,
        30,
        10,
    ]
    assert not hasattr(request, "minimum_price")
    assert not hasattr(request, "price_coverage")


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-09-24T00:00:00", "2026-09-24T01:00:00Z"),
        ("2026-09-24T01:00:00Z", "2026-09-24T00:00:00Z"),
        ("2026-09-24T00:00:00Z", "2026-09-26T00:00:00Z"),
    ],
)
def test_shock_ui_rejects_invalid_scan_window(start, end):
    with pytest.raises(ValueError):
        build_shock_request(
            base="BTC",
            preset_hash="a" * 64,
            start_utc=start,
            end_utc=end,
            major_fraction="0.20",
            medium_fraction="0.10",
            minor_fraction="0.05",
        )


def test_shock_ui_rejects_inverted_scale_thresholds():
    with pytest.raises(ValueError):
        build_shock_request(
            base="BTC",
            preset_hash="a" * 64,
            start_utc="2026-09-24T00:00:00Z",
            end_utc="2026-09-24T00:01:00Z",
            major_fraction="0.05",
            medium_fraction="0.10",
            minor_fraction="0.20",
        )


_COMMON_SCALES = dict(
    base="BTC",
    preset_hash="a" * 64,
    major_fraction="0.20",
    medium_fraction="0.10",
    minor_fraction="0.05",
)


def test_shock_ui_scan_owns_at_most_86400_one_second_slots():
    request, _ = build_shock_request(
        start_utc="2026-09-24T00:00:00Z",
        end_utc="2026-09-24T23:59:59Z",
        **_COMMON_SCALES,
    )
    assert (
        request.requested_end_utc - request.requested_start_utc
    ).total_seconds() == 86_399

    # Closed endpoints exactly 24 h apart would own 86,401 slots.
    with pytest.raises(ValueError, match="24 hours"):
        build_shock_request(
            start_utc="2026-09-24T00:00:00Z",
            end_utc="2026-09-25T00:00:00Z",
            **_COMMON_SCALES,
        )


def test_calendar_handoff_clip_is_accepted_by_the_request_limit():
    from datetime import timedelta

    from l2shock.ui.tab_shock_review import _shock_handoff_range

    start = datetime(2026, 9, 24, tzinfo=timezone.utc)
    clipped_start, end, clipped = _shock_handoff_range(
        start,
        start + timedelta(hours=48) - timedelta(seconds=1),
    )

    assert clipped
    assert (end - clipped_start).total_seconds() == 86_399

    build_shock_request(
        start_utc=clipped_start.isoformat(),
        end_utc=end.isoformat(),
        **_COMMON_SCALES,
    )

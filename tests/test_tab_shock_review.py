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
    assert request.requested_start_utc == datetime(
        2026, 9, 24, tzinfo=timezone.utc
    )
    assert [str(scale.minimum_leg_fraction) for scale in config.scales] == [
        "0.20",
        "0.10",
        "0.05",
    ]
    assert [scale.pivot_radius_seconds for scale in config.scales] == [
        60, 30, 10,
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
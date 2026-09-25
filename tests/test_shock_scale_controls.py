# tests/test_shock_scale_controls.py
from __future__ import annotations

from decimal import Decimal

import pytest

from l2shock.ui.tab_shock_review import build_shock_request

_INPUTS = {
    "base": "BTC",
    "preset_hash": "a" * 64,
    "start_utc": "2026-09-24T00:00:00Z",
    "end_utc": "2026-09-24T00:01:00Z",
    "major_fraction": "0.20",
    "medium_fraction": "0.10",
    "minor_fraction": "0.05",
}


def _build(**overrides: object):
    values = {**_INPUTS, **overrides}
    return build_shock_request(**values)


def test_default_controls_preserve_original_three_scale_semantics() -> None:
    request, config = _build()

    assert request.base == "BTC"
    assert [
        (scale.name, scale.minimum_leg_fraction, scale.pivot_radius_seconds)
        for scale in config.scales
    ] == [
        ("major", Decimal("0.20"), 60),
        ("medium", Decimal("0.10"), 30),
        ("minor", Decimal("0.05"), 10),
    ]


def test_two_scales_exclude_minor_and_do_not_parse_its_inputs() -> None:
    _, config = _build(
        scale_count=2,
        minor_fraction="inactive",
        minor_radius_seconds="inactive",
    )

    assert [scale.name for scale in config.scales] == [
        "major",
        "medium",
    ]


def test_three_scale_thresholds_and_radii_reach_detector_config() -> None:
    _, config = _build(
        scale_count=3,
        major_fraction="0.25",
        medium_fraction="0.12",
        minor_fraction="0.06",
        major_radius_seconds=90,
        medium_radius_seconds=45,
        minor_radius_seconds=15,
    )

    assert [
        (
            scale.name,
            scale.minimum_leg_fraction,
            scale.pivot_radius_seconds,
        )
        for scale in config.scales
    ] == [
        ("major", Decimal("0.25"), 90),
        ("medium", Decimal("0.12"), 45),
        ("minor", Decimal("0.06"), 15),
    ]


@pytest.mark.parametrize(
    "value",
    [0, 1, 4, True, False, 2.5, "2.5", None],
)
def test_scale_count_rejects_unsupported_values(value: object) -> None:
    with pytest.raises(ValueError, match="Scale count"):
        _build(scale_count=value)


@pytest.mark.parametrize(
    "value",
    [0, -1, 3601, True, 2.5, "2.5", None],
)
def test_pivot_radius_does_not_truncate_or_accept_invalid_values(
    value: object,
) -> None:
    with pytest.raises(ValueError, match="Major pivot radius"):
        _build(major_radius_seconds=value)


def test_inactive_minor_does_not_change_two_scale_config() -> None:
    _, first = _build(
        scale_count=2,
        minor_fraction="0.05",
        minor_radius_seconds=10,
    )
    _, second = _build(
        scale_count=2,
        minor_fraction="0.001",
        minor_radius_seconds=300,
    )

    assert first == second


def test_invalid_active_scale_order_is_rejected() -> None:
    with pytest.raises(ValueError, match="higher to a lower"):
        _build(
            scale_count=3,
            medium_fraction="0.20",
        )


def test_request_does_not_require_price_or_chart_inputs() -> None:
    request, config = _build(scale_count=2)

    assert request.base == "BTC"
    assert len(config.scales) == 2
    assert not hasattr(request, "price_bounds")
    assert not hasattr(request, "chart_timeframe")

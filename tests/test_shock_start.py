# tests/test_shock_start.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction

import pytest

from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.shock_start import (
    ShockStartConfig,
    ShockStartDirection,
    ShockStartError,
    ShockStartKind,
    ShockStructuralScale,
    propose_shock_starts,
)
from l2shock.ingest.sampling import (
    BookSampleInvalidReason,
    BookSampleQuality,
)

_START = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)


def _valid(index: int, total: int) -> L2Second:
    return L2Second(
        timestamp_utc=_START + timedelta(seconds=index),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(total),
        ask_liquidity=Decimal(0),
        source_count=1,
    )


def _invalid(index: int) -> L2Second:
    return L2Second(
        timestamp_utc=_START + timedelta(seconds=index),
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
        bid_liquidity=None,
        ask_liquidity=None,
        source_count=0,
    )


def _config() -> ShockStartConfig:
    # Small test radii keep the fixture readable. Production defaults
    # remain major/medium/minor 20/10/5% with their own radii.
    return ShockStartConfig(
        scales=(
            ShockStructuralScale("major", Decimal("0.20"), 3),
            ShockStructuralScale("medium", Decimal("0.10"), 2),
            ShockStructuralScale("minor", Decimal("0.05"), 1),
        )
    )


def test_defaults_lock_three_fractal_height_thresholds() -> None:
    config = ShockStartConfig()

    assert tuple(
        (scale.name, scale.minimum_leg_fraction) for scale in config.scales
    ) == (
        ("major", Decimal("0.20")),
        ("medium", Decimal("0.10")),
        ("minor", Decimal("0.05")),
    )


def test_turning_low_can_lead_to_local_high_not_scan_global_high() -> None:
    totals = (
        50,
        45,
        40,
        30,
        20,
        10,
        20,
        35,
        50,
        65,
        75,
        65,
        55,
        45,
        40,
        50,
        60,
        80,
        100,
        90,
        80,
    )
    candidates = propose_shock_starts(
        (_valid(i, value) for i, value in enumerate(totals)),
        config=_config(),
    )

    starts = tuple(
        item
        for item in candidates
        if (
            item.b_index == 5
            and item.direction is ShockStartDirection.UP
            and item.kind is ShockStartKind.TURNING
        )
    )

    assert starts
    assert {item.scale_name for item in starts} == {
        "major",
        "medium",
        "minor",
    }

    # The first local C can be 75 even though the full scan later reaches 100.
    assert any(item.c_index == 10 and item.c_total == 75 for item in starts)
    assert all(item.scan_range == Fraction(90) for item in starts)


def test_acceleration_start_does_not_require_a_turning_pivot() -> None:
    totals = (
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        18,
        22,
        30,
        42,
        55,
        65,
        70,
        70,
        70,
    )
    candidates = propose_shock_starts(
        (_valid(i, value) for i, value in enumerate(totals)),
        config=_config(),
    )

    assert any(
        item.kind is ShockStartKind.ACCELERATION
        and item.direction is ShockStartDirection.UP
        and 5 <= item.b_index <= 9
        for item in candidates
    )


def test_invalid_second_prevents_a_b_c_from_crossing_gap() -> None:
    values = (
        _valid(0, 50),
        _valid(1, 40),
        _valid(2, 30),
        _valid(3, 20),
        _invalid(4),
        _valid(5, 25),
        _valid(6, 45),
        _valid(7, 70),
    )

    candidates = propose_shock_starts(values, config=_config())

    assert all(item.c_index < 4 or item.a_index > 4 for item in candidates)


def test_missing_timestamp_prevents_cross_gap_hypothesis() -> None:
    values = (
        _valid(0, 40),
        _valid(1, 20),
        _valid(3, 30),
        _valid(4, 80),
    )

    assert propose_shock_starts(values, config=_config()) == ()


def test_no_price_or_chart_input_is_required() -> None:
    # This entire detector boundary accepts L2Second alone. Sparse or absent
    # Binance trades cannot disqualify a valid L2 observation here.
    totals = (40, 30, 20, 10, 20, 30, 50, 70, 60, 50)

    assert propose_shock_starts(
        (_valid(i, value) for i, value in enumerate(totals)),
        config=_config(),
    )


def test_invalid_scale_order_is_rejected() -> None:
    with pytest.raises(ShockStartError, match="higher to a lower"):
        ShockStartConfig(
            scales=(
                ShockStructuralScale("minor", Decimal("0.05"), 1),
                ShockStructuralScale("major", Decimal("0.20"), 3),
            )
        )


def test_duplicate_second_is_rejected() -> None:
    first = _valid(0, 10)

    with pytest.raises(ShockStartError, match="unique, increasing"):
        propose_shock_starts((first, first), config=_config())

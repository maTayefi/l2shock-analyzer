# tests/test_shock_performance_equivalence.py
"""Batch 35 optimizations must be exactly behaviour-neutral."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from statistics import median

from l2shock.analysis.shock_evidence import (
    ChannelEvidence,
    EvidenceChannel,
    EvidenceOrientation,
    ShockEvidenceConfig,
    _channel_evidence,
    _channel_series,
    _series_evidence,
)
from l2shock.analysis.shock_start import (
    ShockStartConfig,
    ShockStartDirection,
    ShockStartHypothesis,
    ShockStartKind,
    ShockStructuralScale,
    _is_turn,
    _run_hypotheses,
)

_T0 = datetime(2026, 9, 24, tzinfo=timezone.utc)
_CONFIG = ShockStartConfig(
    scales=(
        ShockStructuralScale("major", Decimal("0.20"), 12),
        ShockStructuralScale("medium", Decimal("0.10"), 6),
        ShockStructuralScale("minor", Decimal("0.05"), 3),
    )
)


def _walk(seed: int, count: int = 900) -> tuple[Fraction, ...]:
    rng = random.Random(seed)
    value = Fraction(1000)
    out: list[Fraction] = []

    for index in range(count):
        if rng.random() >= 0.15:  # otherwise: exact plateau repeat
            value += Fraction(rng.randint(-60, 60), rng.choice((1, 8, 10, 100, 1000)))
        if index % 150 == 75:
            value += Fraction(rng.choice((-1, 1)) * rng.randint(300, 600))
        out.append(value)

    return tuple(out)


# ---- frozen pre-Batch-35 reference implementations -------------------------


def _reference_run_hypotheses(values, *, offset, times, scan_range, config):
    results = []
    for scale in config.scales:
        radius = scale.pivot_radius_seconds
        horizon = radius * scale.forward_radius_multiplier
        threshold = Fraction(scale.minimum_leg_fraction) * scan_range
        if len(values) < 2 * radius + 2:
            continue
        for b in range(radius, len(values) - 1):
            a = b - radius
            c_limit = min(len(values), b + horizon + 1)
            if c_limit <= b + 1:
                continue
            future = values[b + 1 : c_limit]
            for direction in ShockStartDirection:
                c_value = (
                    max(future) if direction is ShockStartDirection.UP else min(future)
                )
                c = b + 1 + future.index(c_value)
                if direction is ShockStartDirection.UP:
                    height = c_value - values[b]
                    before = values[b] - values[a]
                    after = values[min(b + radius, len(values) - 1)] - values[b]
                else:
                    height = values[b] - c_value
                    before = values[a] - values[b]
                    after = values[b] - values[min(b + radius, len(values) - 1)]
                if height < threshold:
                    continue
                turning = _is_turn(values, index=b, radius=radius, direction=direction)
                accelerating = (
                    before >= 0
                    and after > 0
                    and after >= config.acceleration_ratio * before
                )
                if turning:
                    kind = ShockStartKind.TURNING
                elif accelerating:
                    kind = ShockStartKind.ACCELERATION
                else:
                    continue
                results.append(
                    ShockStartHypothesis(
                        scale_name=scale.name,
                        kind=kind,
                        direction=direction,
                        a_index=offset + a,
                        b_index=offset + b,
                        c_index=offset + c,
                        a_utc=times[offset + a],
                        b_utc=times[offset + b],
                        c_utc=times[offset + c],
                        a_total=values[a],
                        b_total=values[b],
                        c_total=c_value,
                        scan_range=scan_range,
                        bc_height=height,
                        bc_fraction_of_scan_range=height / scan_range,
                    )
                )
    return results


def _reference_channel_evidence(values, candidate, config, channel):
    available = tuple(value for value in values if value is not None)
    if len(available) < 2:
        return None
    channel_range = max(available) - min(available)
    if channel_range <= 0:
        return None
    threshold = Fraction(config.minimum_channel_leg_fraction) * channel_range
    lookback = candidate.b_index - candidate.a_index
    tolerance = config.maximum_offset_seconds
    matches = []
    for b in range(candidate.b_index - tolerance, candidate.b_index + tolerance + 1):
        a = b - lookback
        if a < 0 or b >= len(values):
            continue
        for c in range(
            max(b + 1, candidate.c_index - tolerance),
            min(len(values) - 1, candidate.c_index + tolerance) + 1,
        ):
            if any(value is None for value in values[a : c + 1]):
                continue
            a_value, b_value, c_value = values[a], values[b], values[c]
            bc = c_value - b_value
            if abs(bc) < threshold:
                continue
            ab = b_value - a_value
            matches.append(
                ChannelEvidence(
                    channel=channel,
                    derived_from_bid_ask=channel is EvidenceChannel.DELTA,
                    orientation=(
                        EvidenceOrientation.UP if bc > 0 else EvidenceOrientation.DOWN
                    ),
                    a_index=a,
                    b_index=b,
                    c_index=c,
                    b_offset_seconds=b - candidate.b_index,
                    c_offset_seconds=c - candidate.c_index,
                    a_value=a_value,
                    b_value=b_value,
                    c_value=c_value,
                    pre_b_median=Fraction(median(values[a:b])),
                    ab_signed_change=ab,
                    bc_signed_change=bc,
                    bc_fraction_of_channel_range=abs(bc) / channel_range,
                    ab_abs_change_per_second=abs(ab) / (b - a),
                    bc_abs_change_per_second=abs(bc) / (c - b),
                )
            )
    if not matches:
        return None
    return min(
        matches,
        key=lambda item: (
            -item.bc_fraction_of_channel_range,
            abs(item.b_offset_seconds),
            abs(item.c_offset_seconds),
            item.b_index,
            item.c_index,
        ),
    )


# ---- equivalence tests ------------------------------------------------------


def _hypotheses(seed: int, offset: int = 0):
    values = _walk(seed)
    times = tuple(_T0 + timedelta(seconds=i) for i in range(len(values) + offset))
    scan_range = max(values) - min(values)
    kwargs = dict(offset=offset, times=times, scan_range=scan_range, config=_CONFIG)
    return _reference_run_hypotheses(values, **kwargs), _run_hypotheses(
        values, **kwargs
    )


def test_integer_detector_matches_fraction_reference_exactly() -> None:
    produced = 0

    for seed in range(6):
        for offset in (0, 7):
            expected, actual = _hypotheses(seed, offset)
            assert actual == expected
            assert all(type(item.bc_height) is Fraction for item in actual)
            produced += len(actual)

    assert produced > 0, "fixture must exercise real hypotheses"


def test_series_evidence_matches_reference_exactly() -> None:
    config = ShockEvidenceConfig(
        maximum_offset_seconds=3,
        minimum_channel_leg_fraction=Decimal("0.02"),
    )
    compared = matched = 0

    for seed in range(4):
        _, hypotheses = _hypotheses(seed)
        length = len(_walk(seed))
        rng = random.Random(500 + seed)

        for index, channel in enumerate(EvidenceChannel):
            values = tuple(
                None if rng.random() < 0.02 else value
                for value in _walk(100 + seed * 3 + index, length)
            )
            series = _channel_series(values)
            step = max(1, len(hypotheses) // 60)

            for candidate in hypotheses[::step]:
                expected = _reference_channel_evidence(
                    values, candidate, config, channel
                )
                assert _series_evidence(series, candidate, config, channel) == expected
                compared += 1
                matched += expected is not None

            if hypotheses:
                candidate = hypotheses[0]
                assert _channel_evidence(values, candidate, config, channel) == (
                    _reference_channel_evidence(values, candidate, config, channel)
                )

    assert compared > 0 and matched > 0


def test_flat_or_sparse_channel_has_no_evidence() -> None:
    _, hypotheses = _hypotheses(0)
    candidate = hypotheses[0]
    config = ShockEvidenceConfig()
    length = len(_walk(0))

    for values in (
        tuple(Fraction(5) for _ in range(length)),
        tuple(None for _ in range(length)),
    ):
        assert (
            _series_evidence(
                _channel_series(values), candidate, config, EvidenceChannel.BID
            )
            is None
        )


# ---- Batch 37: integer evidence/review paths ---------------------------------


def _reference_bc_path_metrics(leg):
    """Frozen pre-Batch-37 Fraction implementation."""
    values = tuple(leg)
    height = abs(values[-1] - values[0])
    sign = 1 if values[-1] > values[0] else -1
    count = 0
    adverse_total = Fraction(0)
    extreme = values[0]
    maximum_retracement = Fraction(0)

    for previous, current in zip(values, values[1:]):
        step = (current - previous) * sign
        if step < 0:
            count += 1
            adverse_total -= step
        if (current - extreme) * sign > 0:
            extreme = current
        retracement = (extreme - current) * sign
        if retracement > maximum_retracement:
            maximum_retracement = retracement

    return count, adverse_total / height, maximum_retracement / height


def test_integer_bc_path_metrics_match_fraction_reference() -> None:
    from l2shock.analysis.shock_review import _bc_path_metrics

    rng = random.Random(3737)
    compared = 0

    for _ in range(400):
        length = rng.randint(2, 60)
        leg = [
            Fraction(rng.randint(-5000, 5000), rng.choice((1, 3, 8, 10, 1000)))
            for _ in range(length)
        ]
        if leg[-1] == leg[0]:
            leg[-1] += Fraction(1, 7)
        assert _bc_path_metrics(leg) == _reference_bc_path_metrics(leg)
        compared += 1

    assert compared == 400


def test_integer_median_and_mad_match_statistics_reference() -> None:
    import math

    from l2shock.analysis.shock_review import _median_and_mad_scaled

    rng = random.Random(4242)

    for _ in range(400):
        values = [
            Fraction(rng.randint(0, 9000), rng.choice((1, 4, 10, 100)))
            for _ in range(rng.randint(1, 40))
        ]
        denominator = math.lcm(*(value.denominator for value in values))
        scaled = [
            value.numerator * (denominator // value.denominator) for value in values
        ]
        expected_median = Fraction(median(values))
        expected_mad = Fraction(median([abs(v - expected_median) for v in values]))

        assert _median_and_mad_scaled(scaled, denominator) == (
            expected_median,
            expected_mad,
        )


def test_scaled_totals_match_exact_bid_plus_ask() -> None:
    from l2shock.analysis.aggregation import L2Second
    from l2shock.analysis.shock_review import (
        _scaled_totals,
        _scaled_total_bounds,
    )

    def _reference_valid_total_bounds(values):
        """Pre-Batch-37 exact min/max Total over VALID seconds (reference)."""
        totals = [
            Fraction(item.bid_liquidity) + Fraction(item.ask_liquidity)
            for item in values
            if item.quality is BookSampleQuality.VALID
        ]
        if not totals:
            return None
        return min(totals), max(totals)

    from l2shock.ingest.sampling import BookSampleInvalidReason, BookSampleQuality

    rng = random.Random(99)
    seconds = []

    for index in range(300):
        timestamp = _T0 + timedelta(seconds=index)
        if rng.random() < 0.05:
            seconds.append(
                L2Second(
                    timestamp_utc=timestamp,
                    quality=BookSampleQuality.INVALID,
                    invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
                    bid_liquidity=None,
                    ask_liquidity=None,
                    source_count=0,
                )
            )
            continue
        seconds.append(
            L2Second(
                timestamp_utc=timestamp,
                quality=BookSampleQuality.VALID,
                invalid_reason=None,
                bid_liquidity=Decimal(rng.randint(0, 10**9))
                / Decimal(10 ** rng.randint(0, 6)),
                ask_liquidity=Decimal(rng.randint(0, 10**9))
                / Decimal(10 ** rng.randint(0, 6)),
                source_count=1,
            )
        )

    scaled = _scaled_totals(seconds)

    for index, second in enumerate(seconds):
        value = scaled.values[index]
        if second.quality is BookSampleQuality.VALID:
            assert Fraction(value, scaled.denominator) == (
                Fraction(second.bid_liquidity) + Fraction(second.ask_liquidity)
            )
        else:
            assert value is None
        assert scaled.invalid_prefix[index + 1] - scaled.invalid_prefix[index] == (
            second.quality is not BookSampleQuality.VALID
        )

    assert _scaled_total_bounds(scaled) == _reference_valid_total_bounds(seconds)


def test_channel_series_scaling_is_exact_and_cache_is_neutral() -> None:
    _, hypotheses = _hypotheses(1)
    config = ShockEvidenceConfig(
        maximum_offset_seconds=3,
        minimum_channel_leg_fraction=Decimal("0.02"),
    )
    rng = random.Random(7)
    values = tuple(
        None if rng.random() < 0.02 else value for value in _walk(55, len(_walk(1)))
    )
    series = _channel_series(values)

    for index, value in enumerate(values):
        if value is None:
            assert series.scaled[index] is None
        else:
            assert Fraction(series.scaled[index], series.denominator) == value

    first = [
        _series_evidence(series, h, config, EvidenceChannel.ASK)
        for h in hypotheses[:80]
    ]
    second = [
        _series_evidence(series, h, config, EvidenceChannel.ASK)
        for h in hypotheses[:80]
    ]
    fresh = [
        _series_evidence(_channel_series(values), h, config, EvidenceChannel.ASK)
        for h in hypotheses[:80]
    ]

    assert first == second == fresh


def test_legacy_review_helpers_are_removed() -> None:
    import l2shock.analysis.shock_review as review_module

    for name in ("_total_at", "_valid_total_bounds", "_statistics_median"):
        assert not hasattr(review_module, name), name

    # The integer fast paths that replaced them remain available.
    for name in ("_scaled_totals", "_scaled_total_bounds", "_median_and_mad_scaled"):
        assert callable(getattr(review_module, name)), name

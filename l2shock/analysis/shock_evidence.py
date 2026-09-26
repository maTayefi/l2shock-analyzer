# l2shock/analysis/shock_evidence.py
"""Bounded B areas and descriptive L2-channel evidence.

No price input, causal attribution, confidence score, or UI ranking.
Delta = Bid - Ask at the same second and is explicitly derived evidence.

Performance (Batch 37, behaviour-neutral): each channel is scaled ONCE per
scan to exact integers over one common denominator. Matching, thresholds,
pre-B medians, and the deterministic best-match choice run on integers;
exact Fractions are rebuilt only for the single stored winning match.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction

from l2shock.analysis.shock_execution import ShockCandidateScan
from l2shock.analysis.shock_start import ShockStartHypothesis
from l2shock.ingest.sampling import BookSampleQuality


class ShockEvidenceError(ValueError):
    """Evidence configuration or candidate ownership is inconsistent."""


class EvidenceChannel(StrEnum):
    BID = "bid"
    ASK = "ask"
    DELTA = "delta"


class EvidenceOrientation(StrEnum):
    UP = "up"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class ShockEvidenceConfig:
    """Timing tolerance and minimum channel movement, not chart settings."""

    maximum_offset_seconds: int = 3
    minimum_channel_leg_fraction: Decimal = Decimal("0.05")

    def __post_init__(self) -> None:
        if (
            isinstance(self.maximum_offset_seconds, bool)
            or not isinstance(self.maximum_offset_seconds, int)
            or self.maximum_offset_seconds < 0
        ):
            raise ShockEvidenceError(
                "maximum_offset_seconds must be a non-negative integer"
            )
        fraction = self.minimum_channel_leg_fraction
        if (
            not isinstance(fraction, Decimal)
            or not fraction.is_finite()
            or not Decimal(0) < fraction < Decimal(1)
        ):
            raise ShockEvidenceError(
                "minimum_channel_leg_fraction must be a finite Decimal "
                "strictly between zero and one"
            )


@dataclass(frozen=True, slots=True)
class ChannelEvidence:
    channel: EvidenceChannel
    derived_from_bid_ask: bool
    orientation: EvidenceOrientation
    a_index: int
    b_index: int
    c_index: int
    b_offset_seconds: int
    c_offset_seconds: int
    a_value: Fraction
    b_value: Fraction
    c_value: Fraction
    pre_b_median: Fraction
    ab_signed_change: Fraction
    bc_signed_change: Fraction
    bc_fraction_of_channel_range: Fraction
    ab_abs_change_per_second: Fraction
    bc_abs_change_per_second: Fraction


@dataclass(frozen=True, slots=True)
class ShockBArea:
    """Nearby B hypotheses in ONE direction, without transitive chaining.

    The representative is deterministic, not a confidence winner.
    All original hypotheses remain available as members.
    """

    direction: str
    first_b_index: int
    last_b_index: int
    representative: ShockStartHypothesis
    members: tuple[ShockStartHypothesis, ...]
    evidence: tuple[ChannelEvidence, ...]

    @property
    def scale_names(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(member.scale_name for member in self.members))

    @property
    def independent_channel_count(self) -> int:
        """Bid and Ask only. Total and derived Delta are not extra votes."""
        return sum(
            item.channel in {EvidenceChannel.BID, EvidenceChannel.ASK}
            for item in self.evidence
        )


@dataclass(frozen=True, slots=True)
class ShockEvidenceResult:
    candidate_scan: ShockCandidateScan
    config: ShockEvidenceConfig
    areas: tuple[ShockBArea, ...]

    @property
    def hypothesis_count(self) -> int:
        return len(self.candidate_scan.hypotheses)


def _channels(scan: ShockCandidateScan) -> dict[EvidenceChannel, tuple]:
    values: dict[EvidenceChannel, list[Fraction | None]] = {
        channel: [] for channel in EvidenceChannel
    }
    for second in scan.dataset.seconds:
        if second.quality is not BookSampleQuality.VALID:
            for channel in EvidenceChannel:
                values[channel].append(None)
            continue
        bid = Fraction(second.bid_liquidity)
        ask = Fraction(second.ask_liquidity)
        values[EvidenceChannel.BID].append(bid)
        values[EvidenceChannel.ASK].append(ask)
        values[EvidenceChannel.DELTA].append(bid - ask)
    return {key: tuple(items) for key, items in values.items()}


def _representative(
    members: tuple[ShockStartHypothesis, ...],
    scale_order: dict[str, int],
) -> ShockStartHypothesis:
    # Structural tier first; then strongest B-C fraction; then stable time
    # ordering. This chooses an evidence anchor, NOT a ranked shock.
    return min(
        members,
        key=lambda item: (
            scale_order[item.scale_name],
            -item.bc_fraction_of_scan_range,
            item.b_index,
            item.c_index,
            item.kind.value,
        ),
    )


def _partition(
    scan: ShockCandidateScan,
) -> tuple[tuple[ShockStartHypothesis, ...], ...]:
    radii = {scale.name: scale.pivot_radius_seconds for scale in scan.config.scales}
    groups: list[tuple[ShockStartHypothesis, ...]] = []

    for direction in sorted({h.direction.value for h in scan.hypotheses}):
        ordered = sorted(
            (h for h in scan.hypotheses if h.direction.value == direction),
            key=lambda h: (h.b_index, h.scale_name, h.c_index),
        )
        current: list[ShockStartHypothesis] = []
        anchor = 0
        maximum_distance = 0

        for item in ordered:
            if item.scale_name not in radii:
                raise ShockEvidenceError("Hypothesis refers to an unconfigured scale")

            if not current:
                current = [item]
                anchor = item.b_index
                maximum_distance = radii[item.scale_name]
                continue

            # Compare to the FIRST B, never to the previous member:
            # a long chain of neighboring candidates cannot grow one area
            # across an arbitrarily long portion of the scan.
            allowed = min(maximum_distance, radii[item.scale_name])

            if item.b_index - anchor <= allowed:
                current.append(item)
                maximum_distance = allowed
            else:
                groups.append(tuple(current))
                current = [item]
                anchor = item.b_index
                maximum_distance = radii[item.scale_name]

        if current:
            groups.append(tuple(current))

    return tuple(
        sorted(groups, key=lambda group: (group[0].b_index, group[0].direction.value))
    )


@dataclass(frozen=True, slots=True)
class _ChannelSeries:
    """Per-channel facts that depend only on the scan, never on a candidate.

    ``invalid_prefix[i]`` is the number of invalid (None) seconds in
    ``values[:i]``; an interval ``values[a : c + 1]`` is fully verified
    exactly when ``invalid_prefix[c + 1] == invalid_prefix[a]``.

    ``scaled[i] == values[i] * denominator`` exactly (an int, or None).
    ``range_scaled == channel_range * denominator`` (0 when no range).
    ``median_cache`` memoizes exact pre-B medians keyed by ``(a, b)``;
    it is a pure cache and never participates in equality.
    """

    values: tuple[Fraction | None, ...]
    channel_range: Fraction | None
    invalid_prefix: tuple[int, ...]
    scaled: tuple[int | None, ...] = ()
    denominator: int = 1
    range_scaled: int = 0
    median_cache: dict[tuple[int, int], Fraction] = field(
        default_factory=dict,
        compare=False,
        repr=False,
    )


def _channel_series(values: tuple[Fraction | None, ...]) -> _ChannelSeries:
    values = tuple(values)

    denominators = {value.denominator for value in values if value is not None}
    denominator = math.lcm(*denominators) if denominators else 1

    scaled = tuple(
        (
            None
            if value is None
            else value.numerator * (denominator // value.denominator)
        )
        for value in values
    )

    available = [value for value in scaled if value is not None]
    channel_range: Fraction | None = None
    range_scaled = 0

    if len(available) >= 2:
        spread = max(available) - min(available)

        if spread > 0:
            range_scaled = spread
            channel_range = Fraction(spread, denominator)

    prefix = [0]

    for value in values:
        prefix.append(prefix[-1] + (value is None))

    return _ChannelSeries(
        values=values,
        channel_range=channel_range,
        invalid_prefix=tuple(prefix),
        scaled=scaled,
        denominator=denominator,
        range_scaled=range_scaled,
    )


def _median_of_scaled(window: tuple[int | None, ...]) -> Fraction:
    """Exact median in scaled units; identical to statistics.median.

    Odd count: the middle element. Even count: the exact mean of the two
    middle elements (kept as a Fraction, never a float).
    """
    ordered = sorted(window)  # type: ignore[type-var]
    count = len(ordered)

    if count == 0:
        raise ShockEvidenceError("A pre-B median requires at least one second")

    middle = count // 2

    if count % 2:
        return Fraction(ordered[middle])

    return Fraction(ordered[middle - 1] + ordered[middle], 2)


def _pre_b_median(series: _ChannelSeries, a: int, b: int) -> Fraction:
    key = (a, b)
    cached = series.median_cache.get(key)

    if cached is None:
        cached = _median_of_scaled(series.scaled[a:b]) / series.denominator
        series.median_cache[key] = cached

    return cached


def _series_evidence(
    series: _ChannelSeries,
    candidate: ShockStartHypothesis,
    config: ShockEvidenceConfig,
    channel: EvidenceChannel,
) -> ChannelEvidence | None:
    """Best descriptive channel match for one candidate.

    Semantics are identical to the original per-candidate implementation:
    the whole-scan channel range, the shifted A-B-C search, the
    uninterrupted-coverage rule, the threshold, the pre-B median, and the
    deterministic tie-break.

    Exactness of the integer path: inside one channel the range is a
    constant, so ordering by ``-abs(bc) / range`` equals ordering by
    ``-abs(bc_scaled)``, and ``abs(bc) < fraction * range`` is equivalent to
    ``abs(bc_scaled) * q < p * range_scaled`` for ``fraction = p / q``.
    """
    channel_range = series.channel_range

    if channel_range is None:
        return None

    scaled = series.scaled

    if len(scaled) != len(series.values):
        # A manually constructed legacy series without scaled values.
        series = _channel_series(series.values)
        scaled = series.scaled

    fraction = Fraction(config.minimum_channel_leg_fraction)
    threshold_numerator = fraction.numerator * series.range_scaled
    threshold_denominator = fraction.denominator

    lookback = candidate.b_index - candidate.a_index
    tolerance = config.maximum_offset_seconds
    prefix = series.invalid_prefix
    count = len(scaled)
    candidate_b = candidate.b_index
    candidate_c = candidate.c_index

    best_key: tuple[int, int, int, int, int] | None = None
    best: tuple[int, int, int] | None = None

    for b in range(candidate_b - tolerance, candidate_b + tolerance + 1):
        a = b - lookback

        if a < 0 or b >= count:
            continue

        b_value = scaled[b]

        for c in range(
            max(b + 1, candidate_c - tolerance),
            min(count - 1, candidate_c + tolerance) + 1,
        ):
            # The shifted A-B-C AND baseline must have uninterrupted
            # verified coverage. Do not match across an invalid second.
            if prefix[c + 1] != prefix[a]:
                continue

            bc = scaled[c] - b_value  # type: ignore[operator]
            magnitude = bc if bc >= 0 else -bc

            if magnitude * threshold_denominator < threshold_numerator:
                continue

            key = (
                -magnitude,
                abs(b - candidate_b),
                abs(c - candidate_c),
                b,
                c,
            )

            if best_key is None or key < best_key:
                best_key = key
                best = (a, b, c)

    if best is None:
        return None

    # Rebuild exact stored fields for the single winning match only.
    a, b, c = best
    values = series.values
    a_value = values[a]
    b_value_exact = values[b]
    c_value = values[c]
    assert a_value is not None
    assert b_value_exact is not None
    assert c_value is not None

    ab = b_value_exact - a_value
    bc_exact = c_value - b_value_exact

    return ChannelEvidence(
        channel=channel,
        derived_from_bid_ask=channel is EvidenceChannel.DELTA,
        orientation=(
            EvidenceOrientation.UP if bc_exact > 0 else EvidenceOrientation.DOWN
        ),
        a_index=a,
        b_index=b,
        c_index=c,
        b_offset_seconds=b - candidate_b,
        c_offset_seconds=c - candidate_c,
        a_value=a_value,
        b_value=b_value_exact,
        c_value=c_value,
        pre_b_median=_pre_b_median(series, a, b),
        ab_signed_change=ab,
        bc_signed_change=bc_exact,
        bc_fraction_of_channel_range=abs(bc_exact) / channel_range,
        ab_abs_change_per_second=abs(ab) / (b - a),
        bc_abs_change_per_second=abs(bc_exact) / (c - b),
    )


def _channel_evidence(
    values: tuple[Fraction | None, ...],
    candidate: ShockStartHypothesis,
    config: ShockEvidenceConfig,
    channel: EvidenceChannel,
) -> ChannelEvidence | None:
    """Compatibility wrapper for existing callers; O(scan) per call."""
    return _series_evidence(_channel_series(values), candidate, config, channel)


def describe_shock_evidence(
    scan: ShockCandidateScan,
    *,
    config: ShockEvidenceConfig | None = None,
) -> ShockEvidenceResult:
    if not isinstance(scan, ShockCandidateScan):
        raise TypeError("scan must be ShockCandidateScan")
    selected = config if config is not None else ShockEvidenceConfig()
    if not isinstance(selected, ShockEvidenceConfig):
        raise TypeError("config must be ShockEvidenceConfig")

    scale_order = {scale.name: index for index, scale in enumerate(scan.config.scales)}
    # Range, coverage prefix, and integer scaling are scan constants:
    # compute them ONCE per channel, never once per B area.
    channel_series = {
        channel: _channel_series(values) for channel, values in _channels(scan).items()
    }
    areas: list[ShockBArea] = []

    for members in _partition(scan):
        representative = _representative(members, scale_order)
        evidence = tuple(
            match
            for channel in EvidenceChannel
            if (
                match := _series_evidence(
                    channel_series[channel],
                    representative,
                    selected,
                    channel,
                )
            )
            is not None
        )
        areas.append(
            ShockBArea(
                direction=representative.direction.value,
                first_b_index=min(item.b_index for item in members),
                last_b_index=max(item.b_index for item in members),
                representative=representative,
                members=members,
                evidence=evidence,
            )
        )

    return ShockEvidenceResult(
        candidate_scan=scan,
        config=selected,
        areas=tuple(areas),
    )


__all__ = [
    "ChannelEvidence",
    "EvidenceChannel",
    "EvidenceOrientation",
    "ShockBArea",
    "ShockEvidenceConfig",
    "ShockEvidenceError",
    "ShockEvidenceResult",
    "describe_shock_evidence",
]

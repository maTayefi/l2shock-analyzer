# l2shock/analysis/shock_evidence.py
"""Bounded B areas and descriptive L2-channel evidence.

No price input, causal attribution, confidence score, or UI ranking.
Delta = Bid - Ask at the same second and is explicitly derived evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction
from statistics import median

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


def _channel_evidence(
    values: tuple[Fraction | None, ...],
    candidate: ShockStartHypothesis,
    config: ShockEvidenceConfig,
    channel: EvidenceChannel,
) -> ChannelEvidence | None:
    available = tuple(value for value in values if value is not None)
    if len(available) < 2:
        return None

    channel_range = max(available) - min(available)
    if channel_range <= 0:
        return None

    threshold = Fraction(config.minimum_channel_leg_fraction) * channel_range
    lookback = candidate.b_index - candidate.a_index
    tolerance = config.maximum_offset_seconds
    matches: list[ChannelEvidence] = []

    for b in range(
        candidate.b_index - tolerance,
        candidate.b_index + tolerance + 1,
    ):
        a = b - lookback

        if a < 0 or b >= len(values):
            continue

        for c in range(
            max(b + 1, candidate.c_index - tolerance),
            min(len(values) - 1, candidate.c_index + tolerance) + 1,
        ):
            # The shifted A-B-C AND baseline must have uninterrupted
            # verified coverage. Do not match across an invalid second.
            interval = values[a : c + 1]
            if any(value is None for value in interval):
                continue

            a_value = values[a]
            b_value = values[b]
            c_value = values[c]
            assert a_value is not None
            assert b_value is not None
            assert c_value is not None

            bc = c_value - b_value
            if abs(bc) < threshold:
                continue

            baseline = values[a:b]
            assert baseline and all(value is not None for value in baseline)
            pre_b_median = Fraction(median(baseline))

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
                    pre_b_median=pre_b_median,
                    ab_signed_change=ab,
                    bc_signed_change=bc,
                    bc_fraction_of_channel_range=abs(bc) / channel_range,
                    ab_abs_change_per_second=abs(ab) / (b - a),
                    bc_abs_change_per_second=abs(bc) / (c - b),
                )
            )

    if not matches:
        return None

    # Deterministic best *descriptive match* per channel. Preference for a
    # large channel-relative B-C movement is not an event-confidence score.
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
    channel_values = _channels(scan)
    areas: list[ShockBArea] = []

    for members in _partition(scan):
        representative = _representative(members, scale_order)
        evidence = tuple(
            match
            for channel in EvidenceChannel
            if (
                match := _channel_evidence(
                    channel_values[channel],
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

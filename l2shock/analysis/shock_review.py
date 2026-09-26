# l2shock/analysis/shock_review.py
"""Deterministic, inspectable ordering of Shock-Start B areas.

Inspection order is not a probability, calibrated confidence, or claim that
the representative B is the visually best second in its area.

Order version 2 adds exact Total B->C path diagnostics adapted from the
retired LM detector:

- square-root-normalised sharpness: (B->C height / scan range) / sqrt(s),
  kept as an exact squared rational so ordering never depends on floats;
- adverse moves inside B->C (count, total / height, max retracement);
- B and C endpoint extremeness inside the whole-scan Total range.

These are diagnostics. Only sharpness and adverse-total fraction join the
lexicographic order, after structural tier and B->C height fraction.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass, replace
from fractions import Fraction
from statistics import median

from l2shock.analysis.shock_evidence import (
    EvidenceChannel,
    ShockBArea,
    ShockEvidenceResult,
)
from l2shock.ingest.sampling import BookSampleQuality

SHOCK_REVIEW_SCHEMA = "l2shock.shock_b_area_review"
SHOCK_REVIEW_SCHEMA_VERSION = 2
SHOCK_REVIEW_ORDER_VERSION = "total_structure_first_v2"


class ShockReviewError(ValueError):
    """A B area does not belong to its verified one-second input."""


@dataclass(frozen=True, slots=True)
class ReviewedShockArea:
    inspection_position: int
    area: ShockBArea

    # These are diagnostics, not components of a weighted confidence score.
    highest_scale_fraction: Fraction
    total_bc_fraction_of_scan_range: Fraction
    total_bc_change_per_second: Fraction
    total_ab_signed_change: Fraction
    total_ab_signed_change_per_second: Fraction
    total_pre_b_median: Fraction
    total_pre_b_median_absolute_deviation: Fraction
    total_pre_b_deviation_fraction_of_scan_range: Fraction
    independent_channel_count: int
    maximum_channel_timing_offset_seconds: int | None

    # Order-version-2 B->C path and endpoint diagnostics.
    total_bc_seconds: int = 0
    total_bc_sharpness_squared: Fraction = Fraction(0)
    total_bc_adverse_move_count: int = 0
    total_bc_adverse_total_fraction: Fraction = Fraction(0)
    total_bc_max_retracement_fraction: Fraction = Fraction(0)
    total_b_extremeness: Fraction = Fraction(0)
    total_c_extremeness: Fraction = Fraction(0)

    @property
    def total_bc_sharpness(self) -> float:
        """Display value of (B->C height / scan range) / sqrt(seconds)."""
        return math.sqrt(self.total_bc_sharpness_squared)


@dataclass(frozen=True, slots=True)
class ShockReview:
    evidence_result: ShockEvidenceResult
    ordered_areas: tuple[ReviewedShockArea, ...]
    review_id: str

    def review_rows(self) -> tuple[dict, ...]:
        """JSON-serializable rows; no hypothesis is discarded.

        Exact rational measurements are emitted as numerator/denominator
        strings. This avoids changing threshold comparisons through float
        conversion. The rows are suitable for collecting labeled examples.
        ``total_bc_sharpness`` is a display float; its exact ordering value
        is ``total_bc_sharpness_squared``.
        """
        rows = []

        for entry in self.ordered_areas:
            area = entry.area
            representative = area.representative

            rows.append(
                {
                    "inspection_position": entry.inspection_position,
                    "review_id": self.review_id,
                    "scan_id": self.evidence_result.candidate_scan.scan_id,
                    "direction": area.direction,
                    "first_b_utc": (
                        self.evidence_result.candidate_scan.dataset.seconds[
                            area.first_b_index
                        ].timestamp_utc.isoformat()
                    ),
                    "last_b_utc": (
                        self.evidence_result.candidate_scan.dataset.seconds[
                            area.last_b_index
                        ].timestamp_utc.isoformat()
                    ),
                    "representative_a_utc": representative.a_utc.isoformat(),
                    "representative_b_utc": representative.b_utc.isoformat(),
                    "representative_c_utc": representative.c_utc.isoformat(),
                    "representative_kind": representative.kind.value,
                    "scale_names": list(area.scale_names),
                    "member_count": len(area.members),
                    "highest_scale_fraction": _rational(entry.highest_scale_fraction),
                    "total_bc_fraction_of_scan_range": _rational(
                        entry.total_bc_fraction_of_scan_range
                    ),
                    "total_bc_change_per_second": _rational(
                        entry.total_bc_change_per_second
                    ),
                    "total_bc_seconds": entry.total_bc_seconds,
                    "total_bc_sharpness_squared": _rational(
                        entry.total_bc_sharpness_squared
                    ),
                    "total_bc_sharpness": entry.total_bc_sharpness,
                    "total_bc_adverse_move_count": (entry.total_bc_adverse_move_count),
                    "total_bc_adverse_total_fraction": _rational(
                        entry.total_bc_adverse_total_fraction
                    ),
                    "total_bc_max_retracement_fraction": _rational(
                        entry.total_bc_max_retracement_fraction
                    ),
                    "total_b_extremeness": _rational(entry.total_b_extremeness),
                    "total_c_extremeness": _rational(entry.total_c_extremeness),
                    "total_ab_signed_change": _rational(entry.total_ab_signed_change),
                    "total_ab_signed_change_per_second": _rational(
                        entry.total_ab_signed_change_per_second
                    ),
                    "total_pre_b_median": _rational(entry.total_pre_b_median),
                    "total_pre_b_median_absolute_deviation": _rational(
                        entry.total_pre_b_median_absolute_deviation
                    ),
                    "total_pre_b_deviation_fraction_of_scan_range": (
                        _rational(entry.total_pre_b_deviation_fraction_of_scan_range)
                    ),
                    "independent_channel_count": (entry.independent_channel_count),
                    "maximum_channel_timing_offset_seconds": (
                        entry.maximum_channel_timing_offset_seconds
                    ),
                    "channels": [
                        {
                            "channel": item.channel.value,
                            "derived_from_bid_ask": (item.derived_from_bid_ask),
                            "orientation": item.orientation.value,
                            "b_offset_seconds": item.b_offset_seconds,
                            "c_offset_seconds": item.c_offset_seconds,
                            "pre_b_median": _rational(item.pre_b_median),
                            "ab_signed_change": _rational(item.ab_signed_change),
                            "bc_signed_change": _rational(item.bc_signed_change),
                            "bc_fraction_of_channel_range": _rational(
                                item.bc_fraction_of_channel_range
                            ),
                            "ab_abs_change_per_second": _rational(
                                item.ab_abs_change_per_second
                            ),
                            "bc_abs_change_per_second": _rational(
                                item.bc_abs_change_per_second
                            ),
                        }
                        for item in area.evidence
                    ],
                }
            )

        return tuple(rows)


def _rational(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def _total_at(seconds, index: int) -> Fraction:
    if not 0 <= index < len(seconds):
        raise ShockReviewError("B-area index lies outside its dataset")

    second = seconds[index]
    if second.quality is not BookSampleQuality.VALID:
        raise ShockReviewError("B-area Total diagnostic touches invalid L2 coverage")

    return Fraction(second.bid_liquidity) + Fraction(second.ask_liquidity)


def _valid_total_bounds(seconds) -> tuple[Fraction, Fraction] | None:
    """Exact min/max Total over every VALID second of the scan."""
    low: Fraction | None = None
    high: Fraction | None = None

    for second in seconds:
        if second.quality is not BookSampleQuality.VALID:
            continue

        total = Fraction(second.bid_liquidity) + Fraction(second.ask_liquidity)

        if low is None or total < low:
            low = total
        if high is None or total > high:
            high = total

    if low is None or high is None:
        return None

    return low, high


def _bc_sharpness_squared(height_fraction: Fraction, seconds: int) -> Fraction:
    """Exact square of (height / scan range) / sqrt(seconds)."""
    if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds <= 0:
        raise ShockReviewError("B->C duration must be a positive number of seconds")

    if height_fraction < 0:
        raise ShockReviewError("B->C height fraction cannot be negative")

    return height_fraction * height_fraction / seconds


def _bc_path_metrics(
    leg: Sequence[Fraction],
) -> tuple[int, Fraction, Fraction]:
    """Adverse-step count, adverse total / height, max retracement / height.

    ``leg`` is the exact Total path from B through C inclusive. An adverse
    step moves against the B->C direction. The maximum retracement is the
    largest pullback from the running B->C extreme.
    """
    values = tuple(leg)

    if len(values) < 2:
        raise ShockReviewError("A B->C leg needs at least two seconds")

    height = abs(values[-1] - values[0])

    if height == 0:
        raise ShockReviewError("A B->C leg must have positive height")

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


def _endpoint_extremeness(
    *,
    b_total: Fraction,
    c_total: Fraction,
    scan_min: Fraction,
    scan_max: Fraction,
) -> tuple[Fraction, Fraction]:
    """Direction-oriented B and C position inside the scan Total range.

    Upward leg: B=1 at the scan's lowest Total, C=1 at its highest.
    Downward leg: B=1 at the scan's highest Total, C=1 at its lowest.
    """
    span = scan_max - scan_min

    if span <= 0:
        raise ShockReviewError("Scan Total range must be positive")

    if c_total > b_total:
        b_value = (scan_max - b_total) / span
        c_value = (c_total - scan_min) / span
    else:
        b_value = (b_total - scan_min) / span
        c_value = (scan_max - c_total) / span

    def clamp(value: Fraction) -> Fraction:
        return min(Fraction(1), max(Fraction(0), value))

    return clamp(b_value), clamp(c_value)


def _diagnostics(
    area: ShockBArea,
    evidence_result: ShockEvidenceResult,
    scale_fractions: dict[str, Fraction],
    scan_bounds: tuple[Fraction, Fraction],
) -> ReviewedShockArea:
    scan = evidence_result.candidate_scan
    seconds = scan.dataset.seconds
    candidate = area.representative

    if candidate.scale_name not in scale_fractions:
        raise ShockReviewError("Representative has an unknown scale")

    a, b, c = (
        candidate.a_index,
        candidate.b_index,
        candidate.c_index,
    )
    if not 0 <= a < b < c < len(seconds):
        raise ShockReviewError("Representative A-B-C indices are invalid")

    owned = tuple(_total_at(seconds, index) for index in range(a, c + 1))
    a_total = owned[0]
    b_total = owned[b - a]
    c_total = owned[-1]

    if (
        seconds[a].timestamp_utc != candidate.a_utc
        or seconds[b].timestamp_utc != candidate.b_utc
        or seconds[c].timestamp_utc != candidate.c_utc
        or (a_total, b_total, c_total)
        != (candidate.a_total, candidate.b_total, candidate.c_total)
    ):
        raise ShockReviewError(
            "Representative values or timestamps do not match L2 input"
        )

    scan_range = candidate.scan_range
    if scan_range <= 0:
        raise ShockReviewError("Representative has no positive scan range")

    before_b = owned[: b - a]
    pre_median = Fraction(median(before_b))
    pre_mad = Fraction(median(tuple(abs(value - pre_median) for value in before_b)))

    # Retain the direction of A->B; an upward Total turn normally has
    # negative AB movement. BC is the absolute leg rate.
    ab_signed = b_total - a_total
    bc_seconds = c - b
    bc_speed = abs(c_total - b_total) / bc_seconds
    bc_fraction = abs(c_total - b_total) / scan_range

    adverse_count, adverse_fraction, retracement_fraction = _bc_path_metrics(
        owned[b - a :]
    )
    b_extremeness, c_extremeness = _endpoint_extremeness(
        b_total=b_total,
        c_total=c_total,
        scan_min=scan_bounds[0],
        scan_max=scan_bounds[1],
    )

    independent = tuple(
        item
        for item in area.evidence
        if item.channel in {EvidenceChannel.BID, EvidenceChannel.ASK}
    )
    offsets = tuple(
        max(
            abs(item.b_offset_seconds),
            abs(item.c_offset_seconds),
        )
        for item in independent
    )

    return ReviewedShockArea(
        inspection_position=0,  # Assigned after sorting.
        area=area,
        highest_scale_fraction=max(
            scale_fractions[member.scale_name] for member in area.members
        ),
        total_bc_fraction_of_scan_range=bc_fraction,
        total_bc_change_per_second=bc_speed,
        total_ab_signed_change=ab_signed,
        total_ab_signed_change_per_second=ab_signed / (b - a),
        total_pre_b_median=pre_median,
        total_pre_b_median_absolute_deviation=pre_mad,
        total_pre_b_deviation_fraction_of_scan_range=(pre_mad / scan_range),
        independent_channel_count=len(independent),
        maximum_channel_timing_offset_seconds=(max(offsets) if offsets else None),
        total_bc_seconds=bc_seconds,
        total_bc_sharpness_squared=_bc_sharpness_squared(bc_fraction, bc_seconds),
        total_bc_adverse_move_count=adverse_count,
        total_bc_adverse_total_fraction=adverse_fraction,
        total_bc_max_retracement_fraction=retracement_fraction,
        total_b_extremeness=b_extremeness,
        total_c_extremeness=c_extremeness,
    )


def _review_id(result: ShockEvidenceResult) -> str:
    config = result.config
    identity = {
        "schema": SHOCK_REVIEW_SCHEMA,
        "schema_version": SHOCK_REVIEW_SCHEMA_VERSION,
        "order_version": SHOCK_REVIEW_ORDER_VERSION,
        "candidate_scan_id": result.candidate_scan.scan_id,
        "evidence": {
            "maximum_offset_seconds": config.maximum_offset_seconds,
            "minimum_channel_leg_fraction": str(
                Fraction(config.minimum_channel_leg_fraction)
            ),
        },
    }
    canonical = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def review_shock_areas(result: ShockEvidenceResult) -> ShockReview:
    if not isinstance(result, ShockEvidenceResult):
        raise TypeError("result must be ShockEvidenceResult")

    scale_fractions = {
        item.name: Fraction(item.minimum_leg_fraction)
        for item in result.candidate_scan.config.scales
    }

    if result.areas:
        bounds = _valid_total_bounds(result.candidate_scan.dataset.seconds)

        if bounds is None:
            raise ShockReviewError("B areas exist but the scan has no valid Total")
    else:
        bounds = (Fraction(0), Fraction(1))  # Unused: nothing to measure.

    measured = tuple(
        _diagnostics(area, result, scale_fractions, bounds) for area in result.areas
    )

    # Lexicographic INSPECTION order (version 2), not a weighted score:
    #
    # 1. Largest qualifying structural tier present in the area.
    # 2. Representative Total B-C height / whole-scan Total range.
    # 3. sqrt-normalised B-C sharpness (exact squared rational).
    # 4. Lower B-C adverse-move total / height (cleaner leg first).
    # 5. Bid/Ask support count (Delta is NOT an independent vote).
    # 6. Less pre-B Total variability, then stable time ordering.
    #
    # Key 2 is a continuous exact fraction, so keys 3-6 act only on exact
    # ties. A future weighted/percentile score must be a new order version.
    measured = tuple(
        sorted(
            measured,
            key=lambda item: (
                -item.highest_scale_fraction,
                -item.total_bc_fraction_of_scan_range,
                -item.total_bc_sharpness_squared,
                item.total_bc_adverse_total_fraction,
                -item.independent_channel_count,
                item.total_pre_b_deviation_fraction_of_scan_range,
                item.area.first_b_index,
                item.area.direction,
            ),
        )
    )

    ordered = tuple(
        replace(item, inspection_position=position)
        for position, item in enumerate(measured, start=1)
    )

    return ShockReview(
        evidence_result=result,
        ordered_areas=ordered,
        review_id=_review_id(result),
    )


__all__ = [
    "SHOCK_REVIEW_ORDER_VERSION",
    "SHOCK_REVIEW_SCHEMA",
    "SHOCK_REVIEW_SCHEMA_VERSION",
    "ReviewedShockArea",
    "ShockReview",
    "ShockReviewError",
    "review_shock_areas",
]

# l2shock/analysis/shock_window.py
"""Exact one-second plotting data around one reviewed Shock-Start B area.

This module owns neither chart-timeframe selection nor NiceGUI/ECharts state.
It never reads price. Invalid L2 seconds remain explicit null points so a
renderer cannot mistake missing coverage for zero liquidity.

The representative A/B/C points are inspection anchors. They are not proof
that one particular second within the B area is the true market-event start.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from fractions import Fraction
from typing import Final

from l2shock.analysis.shock_review import ShockReview
from l2shock.ingest.sampling import BookSampleQuality

_ONE_SECOND: Final[timedelta] = timedelta(seconds=1)


class ShockWindowError(ValueError):
    """A requested plotting window violates its dataset/area contract."""


def _nonnegative_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ShockWindowError(f"{name} must be a non-negative integer")
    return value


def _positive_int(name: str, value: object) -> int:
    result = _nonnegative_int(name, value)
    if result == 0:
        raise ShockWindowError(f"{name} must be positive")
    return result


def _exact_text(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def _chart_number(value: Fraction) -> float:
    """Convert only browser coordinates; retain exact values separately."""
    try:
        converted = float(value)
    except (OverflowError, ValueError) as exc:
        raise ShockWindowError(
            "L2 value cannot be represented as a chart coordinate"
        ) from exc

    if not math.isfinite(converted):
        raise ShockWindowError(
            "L2 value cannot be represented as a finite chart coordinate"
        )

    return converted


@dataclass(frozen=True, slots=True)
class ShockWindowSecond:
    """One dataset-owned second, including an explicit invalid-data slot."""

    dataset_index: int
    timestamp_utc: datetime
    valid_l2: bool
    bid: Fraction | None
    ask: Fraction | None
    total: Fraction | None
    delta: Fraction | None

    def to_dict(self) -> dict[str, object]:
        values = {
            "bid": self.bid,
            "ask": self.ask,
            "total": self.total,
            "delta": self.delta,
        }

        return {
            "dataset_index": self.dataset_index,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "valid_l2": self.valid_l2,
            # Floats are for ECharts coordinates only. An invalid observation
            # is null, never zero and never a carried-forward value.
            "plot": {
                name: _chart_number(value) if value is not None else None
                for name, value in values.items()
            },
            # Exact rational text is suitable for tooltips and exports.
            "exact": {
                name: _exact_text(value) if value is not None else None
                for name, value in values.items()
            },
        }


@dataclass(frozen=True, slots=True)
class ShockAreaWindow:
    """A bounded, contiguous UTC axis around one reviewed B area."""

    review_id: str
    inspection_position: int
    direction: str
    first_dataset_index: int
    last_dataset_index: int

    # Inclusive positions on the window's one-second axis.
    b_first_position: int
    b_last_position: int
    representative_a_position: int
    representative_b_position: int
    representative_c_position: int

    # Every member is retained; chart integration may choose which of its
    # alternative A/B/C anchors to reveal on selection.
    member_abc_positions: tuple[tuple[int, int, int], ...]
    seconds: tuple[ShockWindowSecond, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "review_id": self.review_id,
            "inspection_position": self.inspection_position,
            "direction": self.direction,
            "first_dataset_index": self.first_dataset_index,
            "last_dataset_index": self.last_dataset_index,
            "b_first_position": self.b_first_position,
            "b_last_position": self.b_last_position,
            "representative_a_position": self.representative_a_position,
            "representative_b_position": self.representative_b_position,
            "representative_c_position": self.representative_c_position,
            "member_abc_positions": [
                {"a": a, "b": b, "c": c} for a, b, c in self.member_abc_positions
            ],
            "seconds": [second.to_dict() for second in self.seconds],
        }


def build_shock_area_window(
    review: ShockReview,
    inspection_position: int,
    *,
    padding_seconds: int = 30,
    maximum_seconds: int = 3_600,
) -> ShockAreaWindow:
    """Build one exact one-second, four-channel window for chart inspection.

    ``inspection_position`` is the one-based position in ``review.ordered_areas``.
    A window exceeding ``maximum_seconds`` fails explicitly: silently
    downsampling it here could move or hide a proposed B second. A future
    chart may show a separate overview at lower visual resolution.
    """
    if not isinstance(review, ShockReview):
        raise TypeError("review must be ShockReview")

    position = _positive_int("inspection_position", inspection_position)
    padding = _nonnegative_int("padding_seconds", padding_seconds)
    limit = _positive_int("maximum_seconds", maximum_seconds)

    if position > len(review.ordered_areas):
        raise ShockWindowError(
            "inspection_position does not identify a reviewed B area"
        )

    entry = review.ordered_areas[position - 1]

    if entry.inspection_position != position:
        raise ShockWindowError(
            "Review inspection positions do not match their ordering"
        )

    area = entry.area
    members = area.members

    if not members or area.representative not in members:
        raise ShockWindowError("B area requires its representative among its members")

    dataset = review.evidence_result.candidate_scan.dataset
    observations = dataset.seconds

    if not observations:
        raise ShockWindowError("Reviewed B area has no L2 observations")

    member_start = min(member.a_index for member in members)
    member_end = max(member.c_index for member in members)

    if not 0 <= member_start <= area.first_b_index:
        raise ShockWindowError("B-area start lies outside member A/B geometry")

    if not area.first_b_index <= area.last_b_index <= member_end:
        raise ShockWindowError("B-area end lies outside member B/C geometry")

    if member_end >= len(observations):
        raise ShockWindowError("B-area member extends beyond the L2 dataset")

    for member in members:
        if not (
            0 <= member.a_index < member.b_index < member.c_index < len(observations)
        ):
            raise ShockWindowError("B-area member has invalid A/B/C indices")

        if not (area.first_b_index <= member.b_index <= area.last_b_index):
            raise ShockWindowError("B-area member B is outside the B interval")

        if (
            observations[member.a_index].timestamp_utc != member.a_utc
            or observations[member.b_index].timestamp_utc != member.b_utc
            or observations[member.c_index].timestamp_utc != member.c_utc
        ):
            raise ShockWindowError(
                "B-area member timestamps do not match the L2 dataset"
            )

    first = max(0, member_start - padding)
    last = min(len(observations) - 1, member_end + padding)

    if last - first + 1 > limit:
        raise ShockWindowError(
            "Selected B-area window exceeds maximum_seconds; "
            "reduce padding or inspect a narrower event"
        )

    first_timestamp = observations[first].timestamp_utc
    plotted: list[ShockWindowSecond] = []

    for index in range(first, last + 1):
        observation = observations[index]

        if observation.timestamp_utc != (
            first_timestamp + (index - first) * _ONE_SECOND
        ):
            raise ShockWindowError(
                "Shock plotting input must own every one-second UTC slot"
            )

        if observation.coverage_degraded:
            raise ShockWindowError(
                "Partial-market L2 coverage cannot be plotted as complete"
            )

        if observation.quality is BookSampleQuality.VALID:
            if observation.bid_liquidity is None or observation.ask_liquidity is None:
                raise ShockWindowError("VALID L2 second has no Bid or Ask liquidity")

            bid = Fraction(observation.bid_liquidity)
            ask = Fraction(observation.ask_liquidity)
            total = bid + ask
            delta = bid - ask
            valid_l2 = True
        else:
            bid = ask = total = delta = None
            valid_l2 = False

        plotted.append(
            ShockWindowSecond(
                dataset_index=index,
                timestamp_utc=observation.timestamp_utc,
                valid_l2=valid_l2,
                bid=bid,
                ask=ask,
                total=total,
                delta=delta,
            )
        )

    representative = area.representative

    return ShockAreaWindow(
        review_id=review.review_id,
        inspection_position=position,
        direction=area.direction,
        first_dataset_index=first,
        last_dataset_index=last,
        b_first_position=area.first_b_index - first,
        b_last_position=area.last_b_index - first,
        representative_a_position=representative.a_index - first,
        representative_b_position=representative.b_index - first,
        representative_c_position=representative.c_index - first,
        member_abc_positions=tuple(
            (
                member.a_index - first,
                member.b_index - first,
                member.c_index - first,
            )
            for member in members
        ),
        seconds=tuple(plotted),
    )


__all__ = [
    "ShockAreaWindow",
    "ShockWindowError",
    "ShockWindowSecond",
    "build_shock_area_window",
]

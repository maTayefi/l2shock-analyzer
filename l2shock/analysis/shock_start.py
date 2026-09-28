# l2shock/analysis/shock_start.py
"""Price-independent, one-second Total-L2 shock-start hypotheses.

This is the first, deliberately narrow stage of Shock-Start Analysis.

It accepts verified/decoded one-second L2 observations. It does not load
PostgreSQL rows, inspect Binance price, rank cross-channel evidence, or
publish results to the UI.

A structural scale's minimum leg height is a fraction of the Total-L2 range
over the complete supplied scan. Its pivot radius and forward horizon are
separate parameters. A missing or invalid second breaks the candidate run.

The output contains retrospective hypotheses, not causal claims.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
import math
from collections import deque
from fractions import Fraction
from typing import Final
from collections.abc import Iterable

from l2shock.analysis.aggregation import L2Second
from l2shock.ingest.sampling import BookSampleQuality

_SECONDS: Final[timedelta] = timedelta(seconds=1)


class ShockStartError(ValueError):
    """Shock-start input or structural configuration is invalid."""


class ShockStartKind(StrEnum):
    TURNING = "turning"
    ACCELERATION = "acceleration"


class ShockStartDirection(StrEnum):
    UP = "up"
    DOWN = "down"


def _positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ShockStartError(f"{name} must be a positive integer")
    return value


def _fraction(name: str, value: object) -> Fraction:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ShockStartError(f"{name} must be a finite Decimal")

    result = Fraction(value)

    if not 0 < result < 1:
        raise ShockStartError(f"{name} must be strictly between 0 and 1")

    return result


@dataclass(frozen=True, slots=True)
class ShockStructuralScale:
    """One pivot-structure tier; not an aggregation/chart timeframe."""

    name: str
    minimum_leg_fraction: Decimal
    pivot_radius_seconds: int
    forward_radius_multiplier: int = 4

    def __post_init__(self) -> None:
        name = str(self.name or "").strip().lower()

        if not name or not name.replace("_", "").isalnum():
            raise ShockStartError("scale name must be a simple nonblank identity")

        _fraction("minimum_leg_fraction", self.minimum_leg_fraction)
        _positive_int("pivot_radius_seconds", self.pivot_radius_seconds)
        _positive_int("forward_radius_multiplier", self.forward_radius_multiplier)

        object.__setattr__(self, "name", name)


DEFAULT_SHOCK_SCALES: Final[tuple[ShockStructuralScale, ...]] = (
    ShockStructuralScale("major", Decimal("0.20"), 60),
    ShockStructuralScale("medium", Decimal("0.10"), 30),
    ShockStructuralScale("minor", Decimal("0.05"), 10),
)


@dataclass(frozen=True, slots=True)
class ShockStartConfig:
    """Version-1 structural input; expose these scale values in a later UI batch."""

    scales: tuple[ShockStructuralScale, ...] = DEFAULT_SHOCK_SCALES
    acceleration_ratio: int = 2

    def __post_init__(self) -> None:
        scales = tuple(self.scales)

        if not scales or any(
            not isinstance(scale, ShockStructuralScale) for scale in scales
        ):
            raise ShockStartError("scales must contain structural scales")

        names = tuple(scale.name for scale in scales)

        if len(set(names)) != len(names):
            raise ShockStartError("structural scale names must be unique")

        # A smaller-scale tier may accept shorter legs, not demand taller ones.
        thresholds = tuple(Fraction(scale.minimum_leg_fraction) for scale in scales)

        if any(later >= earlier for earlier, later in zip(thresholds, thresholds[1:])):
            raise ShockStartError(
                "scales must be ordered from a higher to a lower "
                "minimum leg fraction"
            )

        _positive_int("acceleration_ratio", self.acceleration_ratio)

        if self.acceleration_ratio < 2:
            raise ShockStartError("acceleration_ratio must be at least 2")

        object.__setattr__(self, "scales", scales)


@dataclass(frozen=True, slots=True)
class ShockStartHypothesis:
    """One scale-specific A-B-C hypothesis in the input's second index."""

    scale_name: str
    kind: ShockStartKind
    direction: ShockStartDirection

    a_index: int
    b_index: int
    c_index: int

    a_utc: datetime
    b_utc: datetime
    c_utc: datetime

    a_total: Fraction
    b_total: Fraction
    c_total: Fraction

    scan_range: Fraction
    bc_height: Fraction
    bc_fraction_of_scan_range: Fraction

    def __post_init__(self) -> None:
        if not 0 <= self.a_index < self.b_index < self.c_index:
            raise ShockStartError("A, B and C must occur in that order")

        if not (
            self.a_utc < self.b_utc < self.c_utc
            and self.bc_height > 0
            and self.scan_range > 0
            and self.bc_fraction_of_scan_range == self.bc_height / self.scan_range
        ):
            raise ShockStartError("A-B-C geometry or leg height is inconsistent")


def _total(observation: L2Second) -> Fraction | None:
    if observation.quality is not BookSampleQuality.VALID:
        return None

    bid = observation.bid_liquidity
    ask = observation.ask_liquidity

    if bid is None or ask is None:
        raise ShockStartError("VALID L2 second has no Bid or Ask value")

    # Fraction(Decimal) preserves the exact represented decimal value and
    # makes range/threshold comparisons independent of ambient precision.
    return Fraction(bid) + Fraction(ask)


def _is_turn(
    values: tuple[Fraction, ...],
    *,
    index: int,
    radius: int,
    direction: ShockStartDirection,
) -> bool:
    start = index - radius
    end = index + radius + 1

    if start < 0 or end > len(values):
        return False

    neighborhood = values[start:end]
    value = values[index]

    if direction is ShockStartDirection.UP:
        extreme = min(neighborhood)
    else:
        extreme = max(neighborhood)

    # Earliest ownership of a flat extremum; no duplicate B on a plateau.
    return value == extreme and neighborhood.index(extreme) == radius


def _first_extremes(
    scaled: tuple[int, ...],
    windows: Iterable[tuple[int, int]],
    *,
    maximum: bool,
) -> list[int]:
    """Index of the FIRST-occurring extreme of each half-open window.

    Window starts and ends must both be non-decreasing and every window must
    be nonempty. An equal value never evicts an earlier index, so the deque
    front is always the earliest occurrence of the window extreme. This is
    exactly ``start + window.index(max_or_min(window))`` in O(n) total.
    """
    result: list[int] = []
    candidates: deque[int] = deque()
    pushed = 0

    for start, end in windows:
        while pushed < end:
            value = scaled[pushed]

            if maximum:
                while candidates and scaled[candidates[-1]] < value:
                    candidates.pop()
            else:
                while candidates and scaled[candidates[-1]] > value:
                    candidates.pop()

            candidates.append(pushed)
            pushed += 1

        while candidates[0] < start:
            candidates.popleft()

        result.append(candidates[0])

    return result


def _run_hypotheses(
    values: tuple[Fraction, ...],
    *,
    offset: int,
    times: tuple[datetime, ...],
    scan_range: Fraction,
    config: ShockStartConfig,
) -> list[ShockStartHypothesis]:
    """Propose hypotheses inside one uninterrupted VALID run.

    Every comparison runs on an exact integer image of the run:
    ``scaled[i] == values[i] * common``, where ``common`` is the LCM of all
    denominators. Multiplying by a positive constant is strictly
    order-preserving and injective, so min/max, equality, first-index
    plateau ownership, and every threshold test answer exactly as Fraction
    comparisons would. Emitted fields remain the original exact Fractions.

    Forward C extremes and turn neighbourhoods use monotonic sliding windows
    (O(n) per scale) with first-occurrence ownership identical to the former
    slice-based ``max(future)`` / ``future.index`` / ``_is_turn`` path.
    """
    results: list[ShockStartHypothesis] = []
    count = len(values)

    if count == 0:
        return results

    common = math.lcm(*{value.denominator for value in values})
    scaled = tuple(value.numerator * (common // value.denominator) for value in values)

    for scale in config.scales:
        radius = scale.pivot_radius_seconds
        horizon = radius * scale.forward_radius_multiplier
        threshold = Fraction(scale.minimum_leg_fraction) * scan_range

        # For an integer h:  h < T  <=>  h < ceil(T).  Exact and integer-only.
        scaled_threshold = threshold * common
        minimum_height = -(
            (-scaled_threshold.numerator) // scaled_threshold.denominator
        )

        if count < 2 * radius + 2:
            continue

        # With no forward horizon every former slice was empty (skipped).
        if horizon < 1:
            continue

        b_values = range(radius, count - 1)

        forward = [(b + 1, min(count, b + horizon + 1)) for b in b_values]
        forward_max = _first_extremes(scaled, forward, maximum=True)
        forward_min = _first_extremes(scaled, forward, maximum=False)

        if radius > 0:
            left = [(b - radius, b) for b in b_values]
            right = [(b + 1, min(count, b + radius + 1)) for b in b_values]
            left_max = _first_extremes(scaled, left, maximum=True)
            left_min = _first_extremes(scaled, left, maximum=False)
            right_max = _first_extremes(scaled, right, maximum=True)
            right_min = _first_extremes(scaled, right, maximum=False)

        for position, b in enumerate(b_values):
            a = b - radius
            a_scaled = scaled[a]
            b_scaled = scaled[b]
            after_scaled = scaled[min(b + radius, count - 1)]

            # _is_turn requires the complete [b - r, b + r] neighbourhood.
            turn_window_complete = b + radius + 1 <= count

            for direction in ShockStartDirection:
                if direction is ShockStartDirection.UP:
                    c = forward_max[position]
                    c_scaled = scaled[c]
                    height_scaled = c_scaled - b_scaled
                    before = b_scaled - a_scaled
                    after = after_scaled - b_scaled
                else:
                    c = forward_min[position]
                    c_scaled = scaled[c]
                    height_scaled = b_scaled - c_scaled
                    before = a_scaled - b_scaled
                    after = b_scaled - after_scaled

                if height_scaled < minimum_height:
                    continue

                # Equivalent to _is_turn: B is the neighbourhood extreme and
                # no EARLIER neighbour ties it (first-index plateau ownership),
                # i.e. left side strictly beyond B, right side not beyond B.
                if radius == 0:
                    turning = True
                elif not turn_window_complete:
                    turning = False
                elif direction is ShockStartDirection.UP:
                    turning = (
                        scaled[left_min[position]] > b_scaled
                        and scaled[right_min[position]] >= b_scaled
                    )
                else:
                    turning = (
                        scaled[left_max[position]] < b_scaled
                        and scaled[right_max[position]] <= b_scaled
                    )

                # An acceleration continues in the same direction:
                # compare rates over equal-length before/after windows.
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

                height = Fraction(height_scaled, common)

                absolute_a = offset + a
                absolute_b = offset + b
                absolute_c = offset + c

                results.append(
                    ShockStartHypothesis(
                        scale_name=scale.name,
                        kind=kind,
                        direction=direction,
                        a_index=absolute_a,
                        b_index=absolute_b,
                        c_index=absolute_c,
                        a_utc=times[absolute_a],
                        b_utc=times[absolute_b],
                        c_utc=times[absolute_c],
                        a_total=values[a],
                        b_total=values[b],
                        c_total=values[c],
                        scan_range=scan_range,
                        bc_height=height,
                        bc_fraction_of_scan_range=height / scan_range,
                    )
                )

    return results


def propose_shock_starts(
    observations: Iterable[L2Second],
    *,
    config: ShockStartConfig | None = None,
) -> tuple[ShockStartHypothesis, ...]:
    """Propose Total-L2 A-B-C starts without consulting price or chart bars.

    Inputs must be strictly increasing, exact one-second UTC observations.
    Missing timestamps and non-VALID L2 observations break runs. Thresholds
    use the Total range of all VALID seconds in the supplied scan, including
    valid seconds on opposite sides of a gap; candidates never cross that gap.
    """
    selected = config if config is not None else ShockStartConfig()

    if not isinstance(selected, ShockStartConfig):
        raise TypeError("config must be ShockStartConfig")

    seconds = tuple(observations)

    if any(not isinstance(item, L2Second) for item in seconds):
        raise TypeError("observations must contain L2Second objects")

    times = tuple(item.timestamp_utc for item in seconds)

    for earlier, later in zip(times, times[1:]):
        if later <= earlier:
            raise ShockStartError(
                "L2 observations must have unique, increasing UTC timestamps"
            )

    totals = tuple(_total(item) for item in seconds)
    available = tuple(value for value in totals if value is not None)

    if len(available) < 2:
        return ()

    scan_range = max(available) - min(available)

    if scan_range == 0:
        return ()

    results: list[ShockStartHypothesis] = []
    start = 0

    while start < len(seconds):
        if totals[start] is None:
            start += 1
            continue

        end = start + 1

        while (
            end < len(seconds)
            and totals[end] is not None
            and times[end] - times[end - 1] == _SECONDS
        ):
            end += 1

        run = totals[start:end]
        assert all(value is not None for value in run)

        results.extend(
            _run_hypotheses(
                tuple(value for value in run if value is not None),
                offset=start,
                times=times,
                scan_range=scan_range,
                config=selected,
            )
        )

        start = end

    return tuple(
        sorted(
            results,
            key=lambda item: (
                item.b_index,
                item.scale_name,
                item.direction.value,
                item.kind.value,
            ),
        )
    )


__all__ = [
    "DEFAULT_SHOCK_SCALES",
    "ShockStartConfig",
    "ShockStartDirection",
    "ShockStartError",
    "ShockStartHypothesis",
    "ShockStartKind",
    "ShockStructuralScale",
    "propose_shock_starts",
]

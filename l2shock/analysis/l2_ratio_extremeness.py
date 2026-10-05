# l2shock/analysis/l2_ratio_extremeness.py
"""Post-scan L2 ratio extremeness score over completed viewing bars.

This is a deterministic, offline, descriptive statistic. It is not a
causal or live signal: every bar is compared with the complete eligible
viewing-bar population of the same displayed scan range, including bars
that occur later in time. Browser zoom never changes the population.

Input is the Bid Share % viewing-bar high and low already owned by
``L2ViewBar.bid_share_pct`` (exact Fractions reduced from valid one-second
ratios). Signed Imbalance % and Ask Share % are exact affine transforms of
Bid Share %, so the score is identical for all three ratio panels.

For each eligible bar i, with N eligible bars:

    H = Bid Share % bar high          L = Bid Share % bar low

    P_high = (count(H < H_i) + (count(H == H_i) - 1) / 2) / (N - 1)
    P_low  = (count(L > L_i) + (count(L == L_i) - 1) / 2) / (N - 1)

    z_high = max(0, 0.6744897501960817 * (H_i - median(H)) / MAD(H))
    z_low  = max(0, 0.6744897501960817 * (median(L) - L_i) / MAD(L))

    If MAD is zero, the mean absolute deviation around the median is used:
        z = distance / (1.2533141373155003 * MeanAD)
    If MeanAD is also zero, z = 0.

    strength(z) = z / (z + 3.5)
    side_score  = 100 * sqrt(P * strength(z))
    score       = max(high_score, low_score)

score[i] belongs to bar[i]; there is no temporal shift. Medians, deviations
and tail counts use exact Fractions; ties are exact equality (no epsilon).
Bars without a ratio are excluded from the population and receive None.
"""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from fractions import Fraction
from typing import Final

from l2shock.analysis.l2_view_stream import L2ViewBar

RATIO_EXTREMENESS_ALGORITHM_VERSION: Final[int] = 1
RATIO_EXTREMENESS_MIN_POPULATION: Final[int] = 20
RATIO_EXTREMENESS_Z_HALF_STRENGTH: Final[float] = 3.5
RATIO_EXTREMENESS_FORMULA: Final[str] = (
    "score = max(side scores); side = 100*sqrt(P*z/(z+3.5)); "
    "P = midpoint-tie tail fraction over (N-1); "
    "z = 0.6744897501960817*distance/MAD "
    "(MAD=0: distance/(1.2533141373155003*MeanAD); both 0: z=0)"
)

_MAD_CONSISTENCY: Final[float] = 0.6744897501960817
_MEAN_AD_CONSISTENCY: Final[float] = 1.2533141373155003


class RatioExtremenessError(ValueError):
    """Viewing-bar ratio input is internally inconsistent."""


class RatioExtremenessSide(StrEnum):
    """Direction-independent side metadata for one scored bar."""

    BID_DOMINANT = "BID_DOMINANT"
    ASK_DOMINANT = "ASK_DOMINANT"
    BOTH = "BOTH"


class RatioExtremenessStatus(StrEnum):
    SCORED = "scored"
    INSUFFICIENT_POPULATION = "insufficient_population"


class RatioScaleMethod(StrEnum):
    MAD = "mad"
    MEAN_ABSOLUTE_DEVIATION = "mean_absolute_deviation"
    ZERO = "zero"


@dataclass(frozen=True, slots=True)
class RatioDistributionSummary:
    """Exact robust centre and scale of one side's reference population."""

    median: Fraction
    scale: Fraction
    scale_method: RatioScaleMethod


@dataclass(frozen=True, slots=True)
class RatioExtremenessBar:
    """Score of one eligible viewing bar; all components are auditable."""

    score: float
    side: RatioExtremenessSide | None
    high_score: float
    low_score: float
    high_percentile: float
    low_percentile: float
    high_robust_z: float
    low_robust_z: float


@dataclass(frozen=True, slots=True)
class RatioExtremenessResult:
    """Per-bar scores aligned one-to-one with the input viewing bars."""

    algorithm_version: int
    status: RatioExtremenessStatus
    eligible_bar_count: int
    bars: tuple[RatioExtremenessBar | None, ...]
    high_distribution: RatioDistributionSummary | None
    low_distribution: RatioDistributionSummary | None


def _median(sorted_values: Sequence[Fraction]) -> Fraction:
    count = len(sorted_values)
    middle = count // 2

    if count % 2:
        return sorted_values[middle]

    return (sorted_values[middle - 1] + sorted_values[middle]) / 2


def _distribution_summary(
    sorted_values: Sequence[Fraction],
) -> RatioDistributionSummary:
    median = _median(sorted_values)
    deviations = sorted(abs(value - median) for value in sorted_values)
    mad = _median(deviations)

    if mad > 0:
        return RatioDistributionSummary(median, mad, RatioScaleMethod.MAD)

    mean_ad = sum(deviations, Fraction(0)) / len(deviations)

    if mean_ad > 0:
        return RatioDistributionSummary(
            median,
            mean_ad,
            RatioScaleMethod.MEAN_ABSOLUTE_DEVIATION,
        )

    return RatioDistributionSummary(median, Fraction(0), RatioScaleMethod.ZERO)


def _directional_robust_z(
    distance: Fraction,
    summary: RatioDistributionSummary,
) -> float:
    """Distance is signed positive toward the scored tail."""
    if distance <= 0 or summary.scale_method is RatioScaleMethod.ZERO:
        return 0.0

    ratio = float(distance / summary.scale)

    if summary.scale_method is RatioScaleMethod.MAD:
        value = _MAD_CONSISTENCY * ratio
    else:
        value = ratio / _MEAN_AD_CONSISTENCY

    if not math.isfinite(value) or value < 0.0:
        raise RatioExtremenessError("Robust distance is not finite")

    return value


def _upper_tail_fraction(sorted_values: Sequence[Fraction], value: Fraction) -> float:
    count = len(sorted_values)
    below = bisect_left(sorted_values, value)
    equal = bisect_right(sorted_values, value) - below
    return float(Fraction(2 * below + equal - 1, 2 * (count - 1)))


def _lower_tail_fraction(sorted_values: Sequence[Fraction], value: Fraction) -> float:
    count = len(sorted_values)
    upper = bisect_right(sorted_values, value)
    equal = upper - bisect_left(sorted_values, value)
    above = count - upper
    return float(Fraction(2 * above + equal - 1, 2 * (count - 1)))


def _combined_score(tail_fraction: float, robust_z: float) -> float:
    if tail_fraction <= 0.0 or robust_z <= 0.0:
        return 0.0

    strength = robust_z / (robust_z + RATIO_EXTREMENESS_Z_HALF_STRENGTH)
    score = 100.0 * math.sqrt(tail_fraction * strength)
    return min(100.0, max(0.0, score))


def _side(high_score: float, low_score: float) -> RatioExtremenessSide | None:
    if max(high_score, low_score) <= 0.0:
        return None
    if high_score > low_score:
        return RatioExtremenessSide.BID_DOMINANT
    if low_score > high_score:
        return RatioExtremenessSide.ASK_DOMINANT
    return RatioExtremenessSide.BOTH


def compute_ratio_extremeness(
    bars: Sequence[L2ViewBar],
) -> RatioExtremenessResult:
    """Score every eligible viewing bar against the complete population."""
    eligible: list[tuple[int, Fraction, Fraction]] = []

    for index, bar in enumerate(bars):
        share = bar.bid_share_pct

        if share is None:
            continue

        high = Fraction(share[1])
        low = Fraction(share[2])

        if low > high:
            raise RatioExtremenessError(
                f"Viewing bar {index} has a ratio low above its high"
            )

        eligible.append((index, high, low))

    count = len(eligible)
    empty: tuple[RatioExtremenessBar | None, ...] = (None,) * len(bars)

    if count < RATIO_EXTREMENESS_MIN_POPULATION:
        return RatioExtremenessResult(
            algorithm_version=RATIO_EXTREMENESS_ALGORITHM_VERSION,
            status=RatioExtremenessStatus.INSUFFICIENT_POPULATION,
            eligible_bar_count=count,
            bars=empty,
            high_distribution=None,
            low_distribution=None,
        )

    highs = sorted(high for _index, high, _low in eligible)
    lows = sorted(low for _index, _high, low in eligible)
    high_summary = _distribution_summary(highs)
    low_summary = _distribution_summary(lows)
    entries: list[RatioExtremenessBar | None] = list(empty)

    for index, high, low in eligible:
        high_fraction = _upper_tail_fraction(highs, high)
        low_fraction = _lower_tail_fraction(lows, low)
        high_z = _directional_robust_z(high - high_summary.median, high_summary)
        low_z = _directional_robust_z(low_summary.median - low, low_summary)
        high_score = _combined_score(high_fraction, high_z)
        low_score = _combined_score(low_fraction, low_z)

        entries[index] = RatioExtremenessBar(
            score=max(high_score, low_score),
            side=_side(high_score, low_score),
            high_score=high_score,
            low_score=low_score,
            high_percentile=high_fraction,
            low_percentile=low_fraction,
            high_robust_z=high_z,
            low_robust_z=low_z,
        )

    return RatioExtremenessResult(
        algorithm_version=RATIO_EXTREMENESS_ALGORITHM_VERSION,
        status=RatioExtremenessStatus.SCORED,
        eligible_bar_count=count,
        bars=tuple(entries),
        high_distribution=high_summary,
        low_distribution=low_summary,
    )


__all__ = [
    "RATIO_EXTREMENESS_ALGORITHM_VERSION",
    "RATIO_EXTREMENESS_FORMULA",
    "RATIO_EXTREMENESS_MIN_POPULATION",
    "RATIO_EXTREMENESS_Z_HALF_STRENGTH",
    "RatioDistributionSummary",
    "RatioExtremenessBar",
    "RatioExtremenessError",
    "RatioExtremenessResult",
    "RatioExtremenessSide",
    "RatioExtremenessStatus",
    "RatioScaleMethod",
    "compute_ratio_extremeness",
]

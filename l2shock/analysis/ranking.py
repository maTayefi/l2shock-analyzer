# l2shock/analysis/ranking.py
"""Population-scoped statistical ranking for Liquidity Movements.

Ranking populations are isolated by:

    liquidity metric x analytical timeframe x direction

The ranking process deliberately has two stages:

1. primary evidence from relative height and sharpness;
2. secondary ordering from equal-weight start/end boundary extremeness and
   movement cleanliness.

The primary recall pool is the union of Top-N by height evidence and Top-N by
sharpness evidence. Secondary evidence cannot remove a candidate from that
pool.

Final selection ordering is lexicographic by primary evidence, then secondary
evidence, rather than by one unreviewed flat weighted sum.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from typing import Final
from collections.abc import Iterable, Mapping, Sequence

from l2shock.analysis.liquidity_movement import (
    LiquidityMetric,
    LiquidityMovementCandidate,
    LiquidityMovementDirection,
)

LM_RANKING_SCHEMA: Final[str] = "l2shock.liquidity_movement_ranking"
LM_RANKING_SCHEMA_VERSION: Final[int] = 2
LM_RANKING_ALGORITHM_VERSION: Final[str] = (
    "population-primary-recall-boundary-extremeness-v2"
)

_DEFAULT_DECIMAL_PRECISION: Final[int] = 34
_MODIFIED_Z_SCALE: Final[Decimal] = Decimal("0.6744897501960817")
_ZERO: Final[Decimal] = Decimal(0)
_ONE: Final[Decimal] = Decimal(1)
_TWO: Final[Decimal] = Decimal(2)


class LiquidityMovementRankingError(ValueError):
    """LM ranking input or output violates its analytical contract."""


def _finite_decimal(
    field_name: str,
    value: object,
    *,
    nonnegative: bool = False,
    positive: bool = False,
) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise LiquidityMovementRankingError(f"{field_name} must be a finite Decimal")

    if nonnegative and value < 0:
        raise LiquidityMovementRankingError(f"{field_name} must be non-negative")

    if positive and value <= 0:
        raise LiquidityMovementRankingError(f"{field_name} must be positive")

    return value


def _unit_interval(
    field_name: str,
    value: object,
) -> Decimal:
    result = _finite_decimal(field_name, value)

    if result < 0 or result > 1:
        raise LiquidityMovementRankingError(f"{field_name} must lie inside [0, 1]")

    return result


def _positive_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LiquidityMovementRankingError(f"{field_name} must be a positive integer")

    return value


def _exact_equal_weight_mean(
    left: Decimal,
    right: Decimal,
) -> Decimal:
    """Return ``(left + right) / 2`` without ambient-context rounding.

    Both inputs are finite unit-interval values. Constructing the result from
    integer coefficients avoids coupling the ranking invariant to either the
    ambient Decimal context or one hard-coded ranking precision.
    """

    left_value = _unit_interval("left boundary extremeness", left)
    right_value = _unit_interval("right boundary extremeness", right)

    left_parts = left_value.as_tuple()
    right_parts = right_value.as_tuple()
    common_exponent = min(
        int(left_parts.exponent),
        int(right_parts.exponent),
    )

    def _coefficient(value: Decimal) -> int:
        parts = value.as_tuple()
        coefficient = 0

        for digit in parts.digits:
            coefficient = coefficient * 10 + digit

        if parts.sign:
            coefficient = -coefficient

        return coefficient * (10 ** (int(parts.exponent) - common_exponent))

    total_coefficient = _coefficient(left_value) + _coefficient(right_value)

    if total_coefficient % 2 == 0:
        mean_coefficient = total_coefficient // 2
        mean_exponent = common_exponent
    else:
        # Dividing an integer coefficient by two is exactly equivalent to
        # multiplying it by five and decreasing the decimal exponent by one.
        mean_coefficient = total_coefficient * 5
        mean_exponent = common_exponent - 1

    if mean_coefficient == 0:
        return Decimal(0)

    return Decimal(
        (
            int(mean_coefficient < 0),
            tuple(int(character) for character in str(abs(mean_coefficient))),
            mean_exponent,
        )
    )


@dataclass(frozen=True, slots=True)
class LiquidityMovementRankingConfig:
    """Semantic configuration for population ranking."""

    top_n_height: int = 10
    top_n_sharpness: int = 10

    priority_height: Decimal = Decimal("0.35")
    priority_sharpness: Decimal = Decimal("0.30")
    priority_endpoint_extremeness: Decimal = Decimal("0.20")
    priority_retracement_magnitude: Decimal = Decimal("0.10")
    priority_retracement_count: Decimal = Decimal("0.05")

    decimal_precision: int = _DEFAULT_DECIMAL_PRECISION

    def __post_init__(self) -> None:
        _positive_integer("top_n_height", self.top_n_height)
        _positive_integer("top_n_sharpness", self.top_n_sharpness)

        if (
            isinstance(self.decimal_precision, bool)
            or not isinstance(self.decimal_precision, int)
            or self.decimal_precision < 16
        ):
            raise LiquidityMovementRankingError(
                "decimal_precision must be an integer of at least 16"
            )

        priority_names = (
            "priority_height",
            "priority_sharpness",
            "priority_endpoint_extremeness",
            "priority_retracement_magnitude",
            "priority_retracement_count",
        )
        priorities: list[Decimal] = []

        for field_name in priority_names:
            priority = _finite_decimal(
                field_name,
                getattr(self, field_name),
                nonnegative=True,
            )
            object.__setattr__(self, field_name, priority)
            priorities.append(priority)

        if sum(priorities, _ZERO) != _ONE:
            raise LiquidityMovementRankingError(
                "LM ranking priorities must sum exactly to 1"
            )

        if self.priority_height + self.priority_sharpness <= 0:
            raise LiquidityMovementRankingError(
                "At least one primary ranking priority must be positive"
            )

        if (
            self.priority_endpoint_extremeness
            + self.priority_retracement_magnitude
            + self.priority_retracement_count
            <= 0
        ):
            raise LiquidityMovementRankingError(
                "At least one secondary ranking priority must be positive"
            )


@dataclass(frozen=True, slots=True, order=True)
class LiquidityMovementPopulationKey:
    """Identity of one statistically independent LM population."""

    metric: LiquidityMetric
    timeframe_label: str
    direction: LiquidityMovementDirection

    def __post_init__(self) -> None:
        metric = LiquidityMetric(self.metric)
        direction = LiquidityMovementDirection(self.direction)
        timeframe = str(self.timeframe_label or "").strip()

        if not timeframe:
            raise LiquidityMovementRankingError("timeframe_label cannot be blank")

        object.__setattr__(self, "metric", metric)
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "timeframe_label", timeframe)


@dataclass(frozen=True, slots=True)
class LiquidityMovementScanBounds:
    """Full scan-level metric range used for LM boundary extremeness."""

    metric: LiquidityMetric
    timeframe_label: str
    minimum: Decimal
    maximum: Decimal

    def __post_init__(self) -> None:
        metric = LiquidityMetric(self.metric)
        timeframe = str(self.timeframe_label or "").strip()

        if not timeframe:
            raise LiquidityMovementRankingError("timeframe_label cannot be blank")

        minimum = _finite_decimal("minimum", self.minimum)
        maximum = _finite_decimal("maximum", self.maximum)

        if maximum <= minimum:
            raise LiquidityMovementRankingError(
                "Scan maximum must be greater than scan minimum"
            )

        object.__setattr__(self, "metric", metric)
        object.__setattr__(self, "timeframe_label", timeframe)
        object.__setattr__(self, "minimum", minimum)
        object.__setattr__(self, "maximum", maximum)

    @property
    def range(self) -> Decimal:
        return self.maximum - self.minimum

    @property
    def identity_tuple(self) -> tuple[LiquidityMetric, str]:
        return self.metric, self.timeframe_label


@dataclass(frozen=True, slots=True)
class LiquidityMovementStatisticalEvidence:
    """Percentile and positive-tail modified-Z evidence for one value."""

    percentile: Decimal
    modified_z: Decimal
    normalized_positive_tail_modified_z: Decimal
    combined: Decimal

    def __post_init__(self) -> None:
        percentile = _unit_interval("percentile", self.percentile)
        modified_z = _finite_decimal(
            "modified_z",
            self.modified_z,
            nonnegative=True,
        )
        normalized_z = _unit_interval(
            "normalized_positive_tail_modified_z",
            self.normalized_positive_tail_modified_z,
        )
        combined = _unit_interval("combined", self.combined)

        expected = (percentile + normalized_z) / _TWO

        if combined != expected:
            raise LiquidityMovementRankingError(
                "combined statistical evidence is inconsistent"
            )

        object.__setattr__(self, "percentile", percentile)
        object.__setattr__(self, "modified_z", modified_z)
        object.__setattr__(
            self,
            "normalized_positive_tail_modified_z",
            normalized_z,
        )
        object.__setattr__(self, "combined", combined)


@dataclass(frozen=True, slots=True)
class RankedLiquidityMovement:
    """One candidate enriched with population ranking evidence."""

    candidate: LiquidityMovementCandidate
    population_key: LiquidityMovementPopulationKey

    height: LiquidityMovementStatisticalEvidence
    sharpness: LiquidityMovementStatisticalEvidence

    start_extremeness: Decimal
    end_extremeness: Decimal
    boundary_extremeness: Decimal

    retracement_magnitude_quality: Decimal
    retracement_count_quality: Decimal

    primary_evidence: Decimal
    secondary_evidence: Decimal
    priority_weighted_evidence: Decimal

    height_rank: int
    sharpness_rank: int
    population_rank: int

    selected_by_height: bool
    selected_by_sharpness: bool
    selected: bool
    final_rank: int | None

    schema: str = LM_RANKING_SCHEMA
    schema_version: int = LM_RANKING_SCHEMA_VERSION
    algorithm_version: str = LM_RANKING_ALGORITHM_VERSION

    def __post_init__(self) -> None:
        if not isinstance(
            self.candidate,
            LiquidityMovementCandidate,
        ):
            raise TypeError("candidate must be a LiquidityMovementCandidate")

        if not isinstance(
            self.population_key,
            LiquidityMovementPopulationKey,
        ):
            raise TypeError("population_key must be LiquidityMovementPopulationKey")

        expected_key = LiquidityMovementPopulationKey(
            metric=self.candidate.metric,
            timeframe_label=self.candidate.timeframe_label,
            direction=self.candidate.direction,
        )
        if self.population_key != expected_key:
            raise LiquidityMovementRankingError(
                "population_key does not match candidate identity"
            )

        if self.schema != LM_RANKING_SCHEMA:
            raise LiquidityMovementRankingError("Unsupported LM ranking schema")

        if self.schema_version != LM_RANKING_SCHEMA_VERSION:
            raise LiquidityMovementRankingError("Unsupported LM ranking schema version")

        if self.algorithm_version != LM_RANKING_ALGORITHM_VERSION:
            raise LiquidityMovementRankingError(
                "Unsupported LM ranking algorithm version"
            )

        for field_name in (
            "start_extremeness",
            "end_extremeness",
            "boundary_extremeness",
            "retracement_magnitude_quality",
            "retracement_count_quality",
            "primary_evidence",
            "secondary_evidence",
            "priority_weighted_evidence",
        ):
            _unit_interval(field_name, getattr(self, field_name))

        expected_boundary_extremeness = _exact_equal_weight_mean(
            self.start_extremeness,
            self.end_extremeness,
        )

        if self.boundary_extremeness != expected_boundary_extremeness:
            raise LiquidityMovementRankingError(
                "boundary_extremeness must be the equal-weight mean of "
                "start_extremeness and end_extremeness"
            )

        for field_name in (
            "height_rank",
            "sharpness_rank",
            "population_rank",
        ):
            _positive_integer(field_name, getattr(self, field_name))

        if not isinstance(self.selected_by_height, bool):
            raise LiquidityMovementRankingError("selected_by_height must be bool")

        if not isinstance(self.selected_by_sharpness, bool):
            raise LiquidityMovementRankingError("selected_by_sharpness must be bool")

        if not isinstance(self.selected, bool):
            raise LiquidityMovementRankingError("selected must be bool")

        if self.selected != (self.selected_by_height or self.selected_by_sharpness):
            raise LiquidityMovementRankingError(
                "selected does not match recall-guard flags"
            )

        if self.selected:
            if self.final_rank is None:
                raise LiquidityMovementRankingError(
                    "A selected candidate requires final_rank"
                )
            _positive_integer("final_rank", self.final_rank)
        elif self.final_rank is not None:
            raise LiquidityMovementRankingError(
                "An unselected candidate cannot own final_rank"
            )


@dataclass(frozen=True, slots=True)
class LiquidityMovementRankingBatch:
    """Complete ranked populations and their selected recall pools."""

    rankings: tuple[RankedLiquidityMovement, ...]

    schema: str = LM_RANKING_SCHEMA
    schema_version: int = LM_RANKING_SCHEMA_VERSION
    algorithm_version: str = LM_RANKING_ALGORITHM_VERSION

    def __post_init__(self) -> None:
        rankings = tuple(self.rankings)

        for ranking in rankings:
            if not isinstance(ranking, RankedLiquidityMovement):
                raise TypeError("rankings must contain RankedLiquidityMovement objects")

        identities = [_candidate_identity(ranking.candidate) for ranking in rankings]
        if len(set(identities)) != len(identities):
            raise LiquidityMovementRankingError(
                "Ranking batch contains duplicate candidate identities"
            )

        if self.schema != LM_RANKING_SCHEMA:
            raise LiquidityMovementRankingError("Unsupported LM ranking-batch schema")

        if self.schema_version != LM_RANKING_SCHEMA_VERSION:
            raise LiquidityMovementRankingError(
                "Unsupported LM ranking-batch schema version"
            )

        if self.algorithm_version != LM_RANKING_ALGORITHM_VERSION:
            raise LiquidityMovementRankingError(
                "Unsupported LM ranking-batch algorithm version"
            )

        object.__setattr__(self, "rankings", rankings)

    @property
    def selected(self) -> tuple[RankedLiquidityMovement, ...]:
        return tuple(ranking for ranking in self.rankings if ranking.selected)

    def population(
        self,
        key: LiquidityMovementPopulationKey,
    ) -> tuple[RankedLiquidityMovement, ...]:
        return tuple(
            ranking for ranking in self.rankings if ranking.population_key == key
        )


@dataclass(frozen=True, slots=True)
class _EvidenceRow:
    candidate: LiquidityMovementCandidate
    population_key: LiquidityMovementPopulationKey
    height: LiquidityMovementStatisticalEvidence
    sharpness: LiquidityMovementStatisticalEvidence

    start_extremeness: Decimal
    end_extremeness: Decimal
    boundary_extremeness: Decimal

    retracement_magnitude_quality: Decimal
    retracement_count_quality: Decimal
    primary_evidence: Decimal
    secondary_evidence: Decimal
    priority_weighted_evidence: Decimal


def _median(
    values: Sequence[Decimal],
) -> Decimal:
    if not values:
        raise LiquidityMovementRankingError("Median requires at least one value")

    ordered = sorted(values)
    middle = len(ordered) // 2

    if len(ordered) % 2:
        return ordered[middle]

    return (ordered[middle - 1] + ordered[middle]) / _TWO


def percentile_ranks(
    values: Sequence[Decimal],
) -> tuple[Decimal, ...]:
    """Return deterministic midpoint-tie percentiles in input order.

    For a non-singleton population:

        percentile =
            (count_less + (count_equal - 1) / 2)
            / (population_size - 1)

    A singleton population receives percentile 1.

    Values are sorted once and each distinct value's midpoint percentile is
    calculated once. This preserves the original semantics without performing
    a complete population scan for every input value.
    """
    normalized = tuple(
        _finite_decimal(
            f"values[{index}]",
            value,
        )
        for index, value in enumerate(values)
    )

    if not normalized:
        return ()

    if len(normalized) == 1:
        return (_ONE,)

    ordered = sorted(normalized)
    denominator = Decimal(len(normalized) - 1)
    percentile_by_value: dict[Decimal, Decimal] = {}

    start = 0

    with localcontext(Context(prec=_DEFAULT_DECIMAL_PRECISION)):
        while start < len(ordered):
            value = ordered[start]
            stop = start + 1

            while stop < len(ordered) and ordered[stop] == value:
                stop += 1

            count_less = start
            count_equal = stop - start
            midpoint_rank = Decimal(count_less) + Decimal(count_equal - 1) / _TWO

            percentile_by_value[value] = midpoint_rank / denominator
            start = stop

    return tuple(percentile_by_value[value] for value in normalized)


def positive_tail_modified_z_scores(
    values: Sequence[Decimal],
    *,
    decimal_precision: int = _DEFAULT_DECIMAL_PRECISION,
) -> tuple[tuple[Decimal, Decimal], ...]:
    """Return ``(positive_z, normalized_positive_z)`` in input order.

    Normalization is:

        normalized_positive_z = positive_z / (1 + positive_z)

    If MAD is zero, values above the median receive normalized evidence 1,
    while values at or below the median receive zero. The raw positive Z-score
    remains zero because a finite modified Z-score is undefined in that case.
    """
    normalized = tuple(
        _finite_decimal(
            f"values[{index}]",
            value,
        )
        for index, value in enumerate(values)
    )

    if not normalized:
        return ()

    if (
        isinstance(decimal_precision, bool)
        or not isinstance(decimal_precision, int)
        or decimal_precision < 16
    ):
        raise LiquidityMovementRankingError(
            "decimal_precision must be an integer of at least 16"
        )

    median = _median(normalized)
    deviations = tuple(abs(value - median) for value in normalized)
    mad = _median(deviations)

    if mad == 0:
        return tuple(
            (
                _ZERO,
                _ONE if value > median else _ZERO,
            )
            for value in normalized
        )

    result: list[tuple[Decimal, Decimal]] = []

    with localcontext(Context(prec=decimal_precision)):
        for value in normalized:
            modified_z = _MODIFIED_Z_SCALE * (value - median) / mad
            positive_z = max(_ZERO, modified_z)
            normalized_z = positive_z / (_ONE + positive_z)
            result.append((positive_z, normalized_z))

    return tuple(result)


def _statistical_evidence(
    values: Sequence[Decimal],
    *,
    decimal_precision: int,
) -> tuple[LiquidityMovementStatisticalEvidence, ...]:
    percentiles = percentile_ranks(values)
    modified = positive_tail_modified_z_scores(
        values,
        decimal_precision=decimal_precision,
    )

    return tuple(
        LiquidityMovementStatisticalEvidence(
            percentile=percentile,
            modified_z=modified_z,
            normalized_positive_tail_modified_z=normalized_z,
            combined=(percentile + normalized_z) / _TWO,
        )
        for percentile, (modified_z, normalized_z) in zip(
            percentiles,
            modified,
            strict=True,
        )
    )


def _candidate_identity(
    candidate: LiquidityMovementCandidate,
) -> tuple[object, ...]:
    return (
        candidate.metric.value,
        candidate.timeframe_label,
        candidate.direction.value,
        candidate.segment_index,
        candidate.start_index,
        candidate.end_index,
        (
            candidate.confirmation_index
            if candidate.confirmation_index is not None
            else -1
        ),
        candidate.start_time_utc,
        candidate.end_time_utc,
        candidate.start_value,
        candidate.end_value,
        candidate.terminal_offline,
    )


def _population_key(
    candidate: LiquidityMovementCandidate,
) -> LiquidityMovementPopulationKey:
    return LiquidityMovementPopulationKey(
        metric=candidate.metric,
        timeframe_label=candidate.timeframe_label,
        direction=candidate.direction,
    )


def _boundary_extremeness(
    candidate: LiquidityMovementCandidate,
    bounds: LiquidityMovementScanBounds,
    *,
    decimal_precision: int,
) -> tuple[Decimal, Decimal, Decimal]:
    """Return equally weighted start, end, and combined boundary evidence.

    Directional ownership:

    - UP candidates are strongest when they start near the scan minimum and
      end near the scan maximum.
    - DOWN candidates are strongest when they start near the scan maximum and
      end near the scan minimum.

    ``boundary_extremeness`` is the arithmetic mean of the two directional
    boundary scores. Therefore neither the starting pivot nor the terminal
    extremum receives more importance.
    """

    if candidate.metric is not bounds.metric:
        raise LiquidityMovementRankingError(
            "Scan bounds metric does not match candidate metric"
        )

    if candidate.timeframe_label != bounds.timeframe_label:
        raise LiquidityMovementRankingError(
            "Scan bounds timeframe does not match candidate timeframe"
        )

    if (
        candidate.start_value < bounds.minimum
        or candidate.start_value > bounds.maximum
        or candidate.end_value < bounds.minimum
        or candidate.end_value > bounds.maximum
    ):
        raise LiquidityMovementRankingError(
            "Candidate boundary values lie outside supplied scan bounds"
        )

    if (
        isinstance(decimal_precision, bool)
        or not isinstance(decimal_precision, int)
        or decimal_precision <= 0
    ):
        raise LiquidityMovementRankingError(
            "decimal_precision must be a positive integer"
        )

    with localcontext(
        Context(
            prec=decimal_precision,
            rounding=ROUND_HALF_EVEN,
        )
    ):
        if candidate.direction is LiquidityMovementDirection.UP:
            start = (bounds.maximum - candidate.start_value) / bounds.range
            end = (candidate.end_value - bounds.minimum) / bounds.range
        else:
            start = (candidate.start_value - bounds.minimum) / bounds.range
            end = (bounds.maximum - candidate.end_value) / bounds.range

        start = min(_ONE, max(_ZERO, start))
        end = min(_ONE, max(_ZERO, end))

    combined = _exact_equal_weight_mean(
        start,
        end,
    )

    return start, end, combined


def _retracement_magnitude_quality(
    candidate: LiquidityMovementCandidate,
) -> Decimal:
    fraction = _finite_decimal(
        "adverse_move_total_fraction",
        candidate.adverse_move_total_fraction,
        nonnegative=True,
    )
    with localcontext(
        Context(
            prec=_DEFAULT_DECIMAL_PRECISION,
            rounding=ROUND_HALF_EVEN,
        )
    ):
        return _ONE / (_ONE + fraction)


def _retracement_count_quality(
    candidate: LiquidityMovementCandidate,
) -> Decimal:
    transition_count = candidate.bars - 1
    if transition_count <= 0:
        raise LiquidityMovementRankingError(
            "An LM candidate must contain at least one transition"
        )
    if candidate.adverse_move_count > transition_count:
        raise LiquidityMovementRankingError(
            "adverse_move_count exceeds candidate transitions"
        )
    with localcontext(
        Context(
            prec=_DEFAULT_DECIMAL_PRECISION,
            rounding=ROUND_HALF_EVEN,
        )
    ):
        return _ONE - Decimal(candidate.adverse_move_count) / Decimal(transition_count)


def _bounds_map(
    scan_bounds: Iterable[LiquidityMovementScanBounds],
) -> dict[tuple[LiquidityMetric, str], LiquidityMovementScanBounds]:
    result: dict[
        tuple[LiquidityMetric, str],
        LiquidityMovementScanBounds,
    ] = {}

    for bounds in scan_bounds:
        if not isinstance(bounds, LiquidityMovementScanBounds):
            raise TypeError("scan_bounds must contain LiquidityMovementScanBounds")

        key = bounds.identity_tuple

        if key in result:
            raise LiquidityMovementRankingError(
                "Duplicate scan bounds for one metric/timeframe"
            )

        result[key] = bounds

    return result


def _evidence_rows(
    candidates: Sequence[LiquidityMovementCandidate],
    *,
    bounds: LiquidityMovementScanBounds,
    config: LiquidityMovementRankingConfig,
) -> list[_EvidenceRow]:
    heights = tuple(candidate.relative_height for candidate in candidates)
    sharpness_values = tuple(candidate.sharpness for candidate in candidates)

    height_evidence = _statistical_evidence(
        heights,
        decimal_precision=config.decimal_precision,
    )
    sharpness_evidence = _statistical_evidence(
        sharpness_values,
        decimal_precision=config.decimal_precision,
    )

    primary_weight = config.priority_height + config.priority_sharpness
    secondary_weight = (
        config.priority_endpoint_extremeness
        + config.priority_retracement_magnitude
        + config.priority_retracement_count
    )

    rows: list[_EvidenceRow] = []

    for candidate, height, sharpness in zip(
        candidates,
        height_evidence,
        sharpness_evidence,
        strict=True,
    ):
        (
            start_extremeness,
            end_extremeness,
            boundary_extremeness,
        ) = _boundary_extremeness(
            candidate,
            bounds,
            decimal_precision=config.decimal_precision,
        )

        magnitude = _retracement_magnitude_quality(candidate)
        count = _retracement_count_quality(candidate)

        with localcontext(
            Context(
                prec=config.decimal_precision,
                rounding=ROUND_HALF_EVEN,
            )
        ):
            primary = (
                config.priority_height * height.combined
                + config.priority_sharpness * sharpness.combined
            ) / primary_weight

            secondary = (
                config.priority_endpoint_extremeness * boundary_extremeness
                + config.priority_retracement_magnitude * magnitude
                + config.priority_retracement_count * count
            ) / secondary_weight

            priority_weighted = (
                config.priority_height * height.combined
                + config.priority_sharpness * sharpness.combined
                + config.priority_endpoint_extremeness * boundary_extremeness
                + config.priority_retracement_magnitude * magnitude
                + config.priority_retracement_count * count
            )

        rows.append(
            _EvidenceRow(
                candidate=candidate,
                population_key=_population_key(candidate),
                height=height,
                sharpness=sharpness,
                start_extremeness=start_extremeness,
                end_extremeness=end_extremeness,
                boundary_extremeness=boundary_extremeness,
                retracement_magnitude_quality=magnitude,
                retracement_count_quality=count,
                primary_evidence=primary,
                secondary_evidence=secondary,
                priority_weighted_evidence=priority_weighted,
            )
        )

    return rows


def _height_sort_key(
    row: _EvidenceRow,
) -> tuple[object, ...]:
    return (
        -row.height.combined,
        -row.height.percentile,
        -row.height.normalized_positive_tail_modified_z,
        -row.candidate.relative_height,
        _candidate_identity(row.candidate),
    )


def _sharpness_sort_key(
    row: _EvidenceRow,
) -> tuple[object, ...]:
    return (
        -row.sharpness.combined,
        -row.sharpness.percentile,
        -row.sharpness.normalized_positive_tail_modified_z,
        -row.candidate.sharpness,
        _candidate_identity(row.candidate),
    )


def _final_sort_key(
    row: _EvidenceRow,
) -> tuple[object, ...]:
    return (
        -row.primary_evidence,
        -row.secondary_evidence,
        -row.height.combined,
        -row.sharpness.combined,
        -row.boundary_extremeness,
        -row.start_extremeness,
        -row.end_extremeness,
        -row.retracement_magnitude_quality,
        -row.retracement_count_quality,
        _candidate_identity(row.candidate),
    )


def _rank_population(
    candidates: Sequence[LiquidityMovementCandidate],
    *,
    bounds: LiquidityMovementScanBounds,
    config: LiquidityMovementRankingConfig,
) -> tuple[RankedLiquidityMovement, ...]:
    if not candidates:
        return ()

    key = _population_key(candidates[0])

    for candidate in candidates:
        if _population_key(candidate) != key:
            raise LiquidityMovementRankingError(
                "One ranking population contains mixed identities"
            )

    ordered_candidates = tuple(sorted(candidates, key=_candidate_identity))
    rows = _evidence_rows(
        ordered_candidates,
        bounds=bounds,
        config=config,
    )

    height_order = sorted(rows, key=_height_sort_key)
    sharpness_order = sorted(rows, key=_sharpness_sort_key)
    population_order = sorted(rows, key=_final_sort_key)

    height_ranks = {
        _candidate_identity(row.candidate): rank
        for rank, row in enumerate(height_order, start=1)
    }
    sharpness_ranks = {
        _candidate_identity(row.candidate): rank
        for rank, row in enumerate(sharpness_order, start=1)
    }
    population_ranks = {
        _candidate_identity(row.candidate): rank
        for rank, row in enumerate(population_order, start=1)
    }

    height_selected = {
        _candidate_identity(row.candidate)
        for row in height_order[: config.top_n_height]
    }
    sharpness_selected = {
        _candidate_identity(row.candidate)
        for row in sharpness_order[: config.top_n_sharpness]
    }
    selected_identities = height_selected | sharpness_selected

    selected_order = [
        row
        for row in population_order
        if _candidate_identity(row.candidate) in selected_identities
    ]
    final_ranks = {
        _candidate_identity(row.candidate): rank
        for rank, row in enumerate(selected_order, start=1)
    }

    ranked: list[RankedLiquidityMovement] = []

    for row in population_order:
        identity = _candidate_identity(row.candidate)
        by_height = identity in height_selected
        by_sharpness = identity in sharpness_selected
        selected = by_height or by_sharpness

        ranked.append(
            RankedLiquidityMovement(
                candidate=row.candidate,
                population_key=row.population_key,
                height=row.height,
                sharpness=row.sharpness,
                start_extremeness=row.start_extremeness,
                end_extremeness=row.end_extremeness,
                boundary_extremeness=row.boundary_extremeness,
                retracement_magnitude_quality=(row.retracement_magnitude_quality),
                retracement_count_quality=(row.retracement_count_quality),
                primary_evidence=row.primary_evidence,
                secondary_evidence=row.secondary_evidence,
                priority_weighted_evidence=(row.priority_weighted_evidence),
                height_rank=height_ranks[identity],
                sharpness_rank=sharpness_ranks[identity],
                population_rank=population_ranks[identity],
                selected_by_height=by_height,
                selected_by_sharpness=by_sharpness,
                selected=selected,
                final_rank=(final_ranks[identity] if selected else None),
            )
        )

    return tuple(ranked)


def rank_liquidity_movements(
    candidates: Sequence[LiquidityMovementCandidate],
    *,
    scan_bounds: Iterable[LiquidityMovementScanBounds],
    config: LiquidityMovementRankingConfig | None = None,
) -> LiquidityMovementRankingBatch:
    """Rank candidates independently by metric, timeframe, and direction."""
    selected_config = config or LiquidityMovementRankingConfig()

    if not isinstance(
        selected_config,
        LiquidityMovementRankingConfig,
    ):
        raise TypeError("config must be LiquidityMovementRankingConfig")

    normalized_candidates = tuple(candidates)

    for candidate in normalized_candidates:
        if not isinstance(candidate, LiquidityMovementCandidate):
            raise TypeError(
                "candidates must contain LiquidityMovementCandidate objects"
            )

    identities = [_candidate_identity(candidate) for candidate in normalized_candidates]
    if len(set(identities)) != len(identities):
        raise LiquidityMovementRankingError(
            "candidates contains a duplicate candidate identity"
        )

    bounds_by_identity = _bounds_map(scan_bounds)

    populations: dict[
        LiquidityMovementPopulationKey,
        list[LiquidityMovementCandidate],
    ] = {}

    for candidate in normalized_candidates:
        key = _population_key(candidate)
        populations.setdefault(key, []).append(candidate)

    rankings: list[RankedLiquidityMovement] = []

    for key in sorted(populations):
        bounds_key = (key.metric, key.timeframe_label)

        try:
            bounds = bounds_by_identity[bounds_key]
        except KeyError as exc:
            raise LiquidityMovementRankingError(
                "Missing scan bounds for LM population "
                f"{key.metric.value}/{key.timeframe_label}"
            ) from exc

        rankings.extend(
            _rank_population(
                populations[key],
                bounds=bounds,
                config=selected_config,
            )
        )

    return LiquidityMovementRankingBatch(
        rankings=tuple(
            sorted(
                rankings,
                key=lambda ranking: (
                    ranking.population_key,
                    ranking.population_rank,
                ),
            )
        )
    )


def ranking_config_from_lm_config(
    value: object,
) -> LiquidityMovementRankingConfig:
    """Build exact ranking configuration from the validated app LM config."""
    required_fields = (
        "top_n_height",
        "top_n_sharpness",
        "priority_height",
        "priority_sharpness",
        "priority_endpoint_extremeness",
        "priority_retracement_magnitude",
        "priority_retracement_count",
    )

    if any(not hasattr(value, field_name) for field_name in required_fields):
        raise TypeError("value does not expose the required LM configuration fields")

    return LiquidityMovementRankingConfig(
        top_n_height=getattr(value, "top_n_height"),
        top_n_sharpness=getattr(value, "top_n_sharpness"),
        priority_height=Decimal(str(getattr(value, "priority_height"))),
        priority_sharpness=Decimal(str(getattr(value, "priority_sharpness"))),
        priority_endpoint_extremeness=Decimal(
            str(getattr(value, "priority_endpoint_extremeness"))
        ),
        priority_retracement_magnitude=Decimal(
            str(getattr(value, "priority_retracement_magnitude"))
        ),
        priority_retracement_count=Decimal(
            str(getattr(value, "priority_retracement_count"))
        ),
    )


__all__ = [
    "LM_RANKING_ALGORITHM_VERSION",
    "LM_RANKING_SCHEMA",
    "LM_RANKING_SCHEMA_VERSION",
    "LiquidityMovementPopulationKey",
    "LiquidityMovementRankingBatch",
    "LiquidityMovementRankingConfig",
    "LiquidityMovementRankingError",
    "LiquidityMovementScanBounds",
    "LiquidityMovementStatisticalEvidence",
    "RankedLiquidityMovement",
    "percentile_ranks",
    "positive_tail_modified_z_scores",
    "rank_liquidity_movements",
    "ranking_config_from_lm_config",
]

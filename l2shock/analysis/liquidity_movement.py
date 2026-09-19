# l2shock/analysis/liquidity_movement.py
"""Segment-scoped Liquidity Movement candidate detection.

Detection semantics:

- candidates belong to one metric and one analytical timeframe;
- candidates are detected independently inside each AnalysisSegment;
- a candidate cannot cross a hard discontinuity or unavailable metric value;
- an upward movement is confirmed after a downward retracement of at least the
  configured fraction of its current height;
- a downward movement is confirmed after an upward retracement of at least the
  configured fraction of its current height;
- the candidate endpoint is the movement extremum, not its confirmation bar;
- an unresolved movement at the right edge of an analysis segment may be
  retained as terminal_offline;
- ranking, percentiles, modified Z-scores, Top-N selection, and visualization
  are deliberately outside this module.
"""

from __future__ import annotations

from dataclasses import InitVar, dataclass
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from enum import StrEnum
from typing import Final

from l2shock.analysis.aggregation import AnalysisBarQuality
from l2shock.analysis.dataset import (
    AlignedAnalysisBar,
    AnalysisSegment,
    TimeframeAnalysisSeries,
)
from l2shock.timeutils import require_aware_utc

LM_DETECTOR_SCHEMA: Final[str] = "l2shock.liquidity_movement_candidate"
LM_DETECTOR_SCHEMA_VERSION: Final[int] = 1
LM_DETECTOR_ALGORITHM_VERSION: Final[str] = "scan-range-segment-extremum-retrace-v2"

_DEFAULT_DECIMAL_PRECISION: Final[int] = 34


class LiquidityMovementError(ValueError):
    """Liquidity Movement detection input or output violates its contract."""


class LiquidityMetric(StrEnum):
    """Authoritative liquidity metrics accepted by the LM detector."""

    BID_LIQUIDITY = "bid_liquidity"
    ASK_LIQUIDITY = "ask_liquidity"
    TOTAL_LIQUIDITY = "total_liquidity"
    BID_ASK_IMBALANCE = "bid_ask_imbalance"


class LiquidityMovementDirection(StrEnum):
    """Direction from candidate start pivot to terminal extremum."""

    UP = "up"
    DOWN = "down"


def _finite_decimal(
    field_name: str,
    value: object,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise LiquidityMovementError(f"{field_name} must be a finite Decimal")

    if positive and value <= 0:
        raise LiquidityMovementError(f"{field_name} must be positive")

    if nonnegative and value < 0:
        raise LiquidityMovementError(f"{field_name} must be non-negative")

    return value


def _nonnegative_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiquidityMovementError(f"{field_name} must be an integer")

    if value < 0:
        raise LiquidityMovementError(f"{field_name} must be non-negative")

    return value


def _validated_decimal_precision(
    value: object,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 16
    ):
        raise LiquidityMovementError(
            "decimal_precision must be an integer of at least 16"
        )

    return value


@dataclass(frozen=True, slots=True)
class LiquidityMovementDetectionConfig:
    """Semantic configuration for candidate detection."""

    confirmation_retracement_fraction: Decimal = Decimal("0.20")
    decimal_precision: int = _DEFAULT_DECIMAL_PRECISION

    def __post_init__(self) -> None:
        fraction = _finite_decimal(
            "confirmation_retracement_fraction",
            self.confirmation_retracement_fraction,
            positive=True,
        )

        if fraction >= 1:
            raise LiquidityMovementError(
                "confirmation_retracement_fraction must be less than 1"
            )

        precision = _validated_decimal_precision(
            self.decimal_precision,
        )

        object.__setattr__(
            self,
            "confirmation_retracement_fraction",
            fraction,
        )
        object.__setattr__(
            self,
            "decimal_precision",
            precision,
        )


@dataclass(frozen=True, slots=True)
class LiquidityMovementCandidate:
    """One measured segment-scoped Liquidity Movement candidate."""

    metric: LiquidityMetric
    timeframe_label: str
    direction: LiquidityMovementDirection
    segment_index: int

    start_index: int
    end_index: int
    confirmation_index: int | None

    in_range_start_index: int
    in_range_end_index: int

    start_time_utc: datetime
    end_time_utc: datetime
    confirmation_time_utc: datetime | None

    in_range_start_time_utc: datetime
    in_range_end_time_utc: datetime

    start_value: Decimal
    end_value: Decimal

    absolute_height: Decimal
    relative_height: Decimal
    bars: int
    sharpness: Decimal

    adverse_move_count: int
    adverse_move_total: Decimal
    adverse_move_maximum: Decimal
    adverse_move_total_fraction: Decimal
    adverse_move_maximum_fraction: Decimal

    confirmation_retracement: Decimal | None
    confirmation_retracement_fraction: Decimal | None

    degraded_bar_count: int
    terminal_offline: bool

    decimal_precision: InitVar[int] = _DEFAULT_DECIMAL_PRECISION

    schema: str = LM_DETECTOR_SCHEMA
    schema_version: int = LM_DETECTOR_SCHEMA_VERSION
    algorithm_version: str = LM_DETECTOR_ALGORITHM_VERSION

    def __post_init__(
        self,
        decimal_precision: int,
    ) -> None:
        metric = LiquidityMetric(self.metric)
        direction = LiquidityMovementDirection(self.direction)
        validated_precision = _validated_decimal_precision(
            decimal_precision,
        )

        if self.schema != LM_DETECTOR_SCHEMA:
            raise LiquidityMovementError("Unsupported LM candidate schema")

        if self.schema_version != LM_DETECTOR_SCHEMA_VERSION:
            raise LiquidityMovementError("Unsupported LM candidate schema version")

        if self.algorithm_version != LM_DETECTOR_ALGORITHM_VERSION:
            raise LiquidityMovementError("Unsupported LM detector algorithm version")

        timeframe_label = str(self.timeframe_label or "").strip()
        if not timeframe_label:
            raise LiquidityMovementError("timeframe_label cannot be blank")

        for field_name in (
            "segment_index",
            "start_index",
            "end_index",
            "in_range_start_index",
            "in_range_end_index",
            "adverse_move_count",
            "degraded_bar_count",
        ):
            _nonnegative_integer(
                field_name,
                getattr(self, field_name),
            )

        if self.end_index <= self.start_index:
            raise LiquidityMovementError(
                "An LM candidate must span at least two distinct bars"
            )

        if not (
            self.start_index
            <= self.in_range_start_index
            <= self.in_range_end_index
            <= self.end_index
        ):
            raise LiquidityMovementError(
                "In-range LM extent must lie inside the full extent"
            )

        expected_bars = self.end_index - self.start_index + 1
        if self.bars != expected_bars:
            raise LiquidityMovementError(
                "bars does not match inclusive candidate extent"
            )

        start_time = require_aware_utc(
            "start_time_utc",
            self.start_time_utc,
        )
        end_time = require_aware_utc(
            "end_time_utc",
            self.end_time_utc,
        )
        in_range_start_time = require_aware_utc(
            "in_range_start_time_utc",
            self.in_range_start_time_utc,
        )
        in_range_end_time = require_aware_utc(
            "in_range_end_time_utc",
            self.in_range_end_time_utc,
        )

        if end_time <= start_time:
            raise LiquidityMovementError("end_time_utc must follow start_time_utc")

        if not (start_time <= in_range_start_time <= in_range_end_time <= end_time):
            raise LiquidityMovementError(
                "In-range LM times must lie inside the full extent"
            )

        start_value = _finite_decimal(
            "start_value",
            self.start_value,
        )
        end_value = _finite_decimal(
            "end_value",
            self.end_value,
        )
        absolute_height = _finite_decimal(
            "absolute_height",
            self.absolute_height,
            positive=True,
        )
        relative_height = _finite_decimal(
            "relative_height",
            self.relative_height,
            positive=True,
        )
        sharpness = _finite_decimal(
            "sharpness",
            self.sharpness,
            positive=True,
        )

        # Validate endpoint geometry using the same precision used during
        # candidate construction to avoid ambient-context rounding mismatches.
        with localcontext(
    Context(
        prec=validated_precision,
        rounding=ROUND_HALF_EVEN,
    )
):
            if absolute_height != abs(end_value - start_value):
                raise LiquidityMovementError(
                    "absolute_height does not match endpoint values"
                )

        if direction is LiquidityMovementDirection.UP:
            if end_value <= start_value:
                raise LiquidityMovementError("An upward LM must end above its start")
        elif end_value >= start_value:
            raise LiquidityMovementError("A downward LM must end below its start")

        adverse_total = _finite_decimal(
            "adverse_move_total",
            self.adverse_move_total,
            nonnegative=True,
        )
        adverse_maximum = _finite_decimal(
            "adverse_move_maximum",
            self.adverse_move_maximum,
            nonnegative=True,
        )
        adverse_total_fraction = _finite_decimal(
            "adverse_move_total_fraction",
            self.adverse_move_total_fraction,
            nonnegative=True,
        )
        adverse_maximum_fraction = _finite_decimal(
            "adverse_move_maximum_fraction",
            self.adverse_move_maximum_fraction,
            nonnegative=True,
        )

        if adverse_maximum > adverse_total:
            raise LiquidityMovementError(
                "adverse_move_maximum cannot exceed adverse_move_total"
            )

        with localcontext(
    Context(
        prec=validated_precision,
        rounding=ROUND_HALF_EVEN,
    )
):
            if adverse_total_fraction != adverse_total / absolute_height:
                raise LiquidityMovementError(
                    "adverse_move_total_fraction is inconsistent"
                )

            if adverse_maximum_fraction != adverse_maximum / absolute_height:
                raise LiquidityMovementError(
                    "adverse_move_maximum_fraction is inconsistent"
                )

        if self.terminal_offline:
            if (
                self.confirmation_index is not None
                or self.confirmation_time_utc is not None
                or self.confirmation_retracement is not None
                or self.confirmation_retracement_fraction is not None
            ):
                raise LiquidityMovementError(
                    "An offline-terminal LM cannot own confirmation data"
                )
        else:
            if self.confirmation_index is None:
                raise LiquidityMovementError(
                    "A confirmed LM requires confirmation_index"
                )

            _nonnegative_integer(
                "confirmation_index",
                self.confirmation_index,
            )

            if self.confirmation_index <= self.end_index:
                raise LiquidityMovementError(
                    "Confirmation must occur after the LM extremum"
                )

            if self.confirmation_time_utc is None:
                raise LiquidityMovementError(
                    "A confirmed LM requires confirmation_time_utc"
                )

            confirmation_time = require_aware_utc(
                "confirmation_time_utc",
                self.confirmation_time_utc,
            )

            if confirmation_time <= end_time:
                raise LiquidityMovementError(
                    "Confirmation time must follow the extremum time"
                )

            retracement = _finite_decimal(
                "confirmation_retracement",
                self.confirmation_retracement,
                positive=True,
            )
            retracement_fraction = _finite_decimal(
                "confirmation_retracement_fraction",
                self.confirmation_retracement_fraction,
                positive=True,
            )

            with localcontext(
    Context(
        prec=validated_precision,
        rounding=ROUND_HALF_EVEN,
    )
):
                if retracement_fraction != retracement / absolute_height:
                    raise LiquidityMovementError(
                        "Confirmation retracement fraction is inconsistent"
                    )

        object.__setattr__(self, "metric", metric)
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "timeframe_label", timeframe_label)
        object.__setattr__(self, "start_time_utc", start_time)
        object.__setattr__(self, "end_time_utc", end_time)
        object.__setattr__(
            self,
            "in_range_start_time_utc",
            in_range_start_time,
        )
        object.__setattr__(
            self,
            "in_range_end_time_utc",
            in_range_end_time,
        )

    @property
    def confirmed(self) -> bool:
        return not self.terminal_offline


def _metric_value(
    bar: AlignedAnalysisBar,
    metric: LiquidityMetric,
    *,
    decimal_precision: int,
) -> Decimal | None:
    if bar.hard_discontinuity:
        return None

    if metric is LiquidityMetric.BID_LIQUIDITY:
        return bar.l2.bid_liquidity

    if metric is LiquidityMetric.ASK_LIQUIDITY:
        return bar.l2.ask_liquidity

    if metric is LiquidityMetric.TOTAL_LIQUIDITY:
        return bar.l2.total_liquidity

    return bar.l2.bid_ask_imbalance(
        decimal_precision=decimal_precision,
    )


def liquidity_metric_value(
    bar: AlignedAnalysisBar,
    metric: LiquidityMetric | str,
    *,
    decimal_precision: int = _DEFAULT_DECIMAL_PRECISION,
) -> Decimal | None:
    """Return one selected metric under the detector's availability policy.

    This is the public read-only metric boundary used by analysis orchestration
    to derive scan bounds. It deliberately shares the detector's rule that a
    hard-discontinuity bar does not expose an LM metric value.
    """
    if not isinstance(bar, AlignedAnalysisBar):
        raise TypeError("bar must be an AlignedAnalysisBar")

    precision = _validated_decimal_precision(
        decimal_precision,
    )

    return _metric_value(
        bar,
        LiquidityMetric(metric),
        decimal_precision=precision,
    )


def _adverse_diagnostics(
    bars: tuple[AlignedAnalysisBar, ...],
    *,
    metric: LiquidityMetric,
    direction: LiquidityMovementDirection,
    start_index: int,
    end_index: int,
    decimal_precision: int,
) -> tuple[int, Decimal, Decimal]:
    count = 0
    total = Decimal(0)
    maximum = Decimal(0)

    previous = _metric_value(
        bars[start_index],
        metric,
        decimal_precision=decimal_precision,
    )
    if previous is None:
        raise LiquidityMovementError(
            "LM diagnostic extent contains unavailable metric data"
        )

    for index in range(start_index + 1, end_index + 1):
        current = _metric_value(
            bars[index],
            metric,
            decimal_precision=decimal_precision,
        )
        if current is None:
            raise LiquidityMovementError(
                "LM diagnostic extent contains unavailable metric data"
            )

        delta = current - previous

        adverse = -delta if direction is LiquidityMovementDirection.UP else delta

        if adverse > 0:
            count += 1
            total += adverse
            maximum = max(maximum, adverse)

        previous = current

    return count, total, maximum


def _candidate(
    *,
    series: TimeframeAnalysisSeries,
    segment: AnalysisSegment,
    segment_index: int,
    metric: LiquidityMetric,
    direction: LiquidityMovementDirection,
    start_index: int,
    end_index: int,
    confirmation_index: int | None,
    population_range: Decimal,
    config: LiquidityMovementDetectionConfig,
) -> LiquidityMovementCandidate | None:
    in_range_start = max(start_index, segment.core_start_index)
    in_range_end = min(end_index, segment.core_end_index)

    if in_range_start > in_range_end:
        return None

    bars = series.bars
    start_value = _metric_value(
        bars[start_index],
        metric,
        decimal_precision=config.decimal_precision,
    )
    end_value = _metric_value(
        bars[end_index],
        metric,
        decimal_precision=config.decimal_precision,
    )

    if start_value is None or end_value is None:
        raise LiquidityMovementError(
            "LM endpoints must contain available metric values"
        )

    bar_count = end_index - start_index + 1

    adverse_count, adverse_total, adverse_maximum = _adverse_diagnostics(
        bars,
        metric=metric,
        direction=direction,
        start_index=start_index,
        end_index=end_index,
        decimal_precision=config.decimal_precision,
    )

    terminal_offline = confirmation_index is None

    with localcontext(Context(prec=config.decimal_precision)):
        absolute_height = abs(end_value - start_value)

        if absolute_height == 0:
            return None

        relative_height = absolute_height / population_range
        sharpness = relative_height / Decimal(bar_count).sqrt()

        adverse_total_fraction = adverse_total / absolute_height
        adverse_maximum_fraction = adverse_maximum / absolute_height

        if terminal_offline:
            confirmation_time = None
            confirmation_retracement = None
            confirmation_fraction = None
        else:
            confirmation_value = _metric_value(
                bars[confirmation_index],
                metric,
                decimal_precision=config.decimal_precision,
            )

            if confirmation_value is None:
                raise LiquidityMovementError(
                    "Confirmation bar lacks the selected metric"
                )

            confirmation_time = bars[confirmation_index].start_utc

            if direction is LiquidityMovementDirection.UP:
                confirmation_retracement = end_value - confirmation_value
            else:
                confirmation_retracement = confirmation_value - end_value

            confirmation_fraction = confirmation_retracement / absolute_height

    degraded_count = sum(
        bars[index].l2.quality is AnalysisBarQuality.DEGRADED
        for index in range(start_index, end_index + 1)
    )

    return LiquidityMovementCandidate(
        metric=metric,
        timeframe_label=series.snapped_range.timeframe.label,
        direction=direction,
        segment_index=segment_index,
        start_index=start_index,
        end_index=end_index,
        confirmation_index=confirmation_index,
        in_range_start_index=in_range_start,
        in_range_end_index=in_range_end,
        start_time_utc=bars[start_index].start_utc,
        end_time_utc=bars[end_index].start_utc,
        confirmation_time_utc=confirmation_time,
        in_range_start_time_utc=bars[in_range_start].start_utc,
        in_range_end_time_utc=bars[in_range_end].start_utc,
        start_value=start_value,
        end_value=end_value,
        absolute_height=absolute_height,
        relative_height=relative_height,
        bars=bar_count,
        sharpness=sharpness,
        adverse_move_count=adverse_count,
        adverse_move_total=adverse_total,
        adverse_move_maximum=adverse_maximum,
        adverse_move_total_fraction=adverse_total_fraction,
        adverse_move_maximum_fraction=adverse_maximum_fraction,
        confirmation_retracement=confirmation_retracement,
        confirmation_retracement_fraction=confirmation_fraction,
        degraded_bar_count=degraded_count,
        terminal_offline=terminal_offline,
        decimal_precision=config.decimal_precision,
    )


def _detect_run(
    *,
    series: TimeframeAnalysisSeries,
    segment: AnalysisSegment,
    segment_index: int,
    metric: LiquidityMetric,
    run_start: int,
    run_end: int,
    population_range: Decimal,
    config: LiquidityMovementDetectionConfig,
    retain_terminal: bool,
) -> list[LiquidityMovementCandidate]:
    bars = series.bars
    result: list[LiquidityMovementCandidate] = []

    pivot_index = run_start
    pivot_value = _metric_value(
        bars[pivot_index],
        metric,
        decimal_precision=config.decimal_precision,
    )
    direction: LiquidityMovementDirection | None = None
    extremum_index = run_start
    extremum_value = pivot_value

    if pivot_value is None:
        return result

    index = run_start + 1

    while index <= run_end:
        value = _metric_value(
            bars[index],
            metric,
            decimal_precision=config.decimal_precision,
        )
        if value is None:
            raise LiquidityMovementError(
                "LM run unexpectedly contains unavailable data"
            )

        if direction is None:
            if value > pivot_value:
                direction = LiquidityMovementDirection.UP
                extremum_index = index
                extremum_value = value
            elif value < pivot_value:
                direction = LiquidityMovementDirection.DOWN
                extremum_index = index
                extremum_value = value

            index += 1
            continue

        assert extremum_value is not None

        if direction is LiquidityMovementDirection.UP:
            if value > extremum_value:
                extremum_index = index
                extremum_value = value
                index += 1
                continue

            movement_height = extremum_value - pivot_value
            retracement = extremum_value - value
        else:
            if value < extremum_value:
                extremum_index = index
                extremum_value = value
                index += 1
                continue

            movement_height = pivot_value - extremum_value
            retracement = value - extremum_value

        with localcontext(Context(prec=config.decimal_precision)):
            confirmed = bool(
                movement_height > 0
                and retracement / movement_height
                >= config.confirmation_retracement_fraction
            )

        if confirmed:
            candidate = _candidate(
                series=series,
                segment=segment,
                segment_index=segment_index,
                metric=metric,
                direction=direction,
                start_index=pivot_index,
                end_index=extremum_index,
                confirmation_index=index,
                population_range=population_range,
                config=config,
            )
            if candidate is not None:
                result.append(candidate)

            pivot_index = extremum_index
            pivot_value = extremum_value
            direction = (
                LiquidityMovementDirection.DOWN
                if direction is LiquidityMovementDirection.UP
                else LiquidityMovementDirection.UP
            )
            extremum_index = index
            extremum_value = value

        index += 1

    if direction is not None and retain_terminal:
        candidate = _candidate(
            series=series,
            segment=segment,
            segment_index=segment_index,
            metric=metric,
            direction=direction,
            start_index=pivot_index,
            end_index=extremum_index,
            confirmation_index=None,
            population_range=population_range,
            config=config,
        )
        if candidate is not None:
            result.append(candidate)

    return result


def detect_liquidity_movements(
    series: TimeframeAnalysisSeries,
    *,
    metric: LiquidityMetric | str,
    config: LiquidityMovementDetectionConfig | None = None,
) -> tuple[LiquidityMovementCandidate, ...]:
    """Detect LM candidates independently inside each analysis segment."""
    if not isinstance(series, TimeframeAnalysisSeries):
        raise TypeError("series must be a TimeframeAnalysisSeries")

    selected_metric = LiquidityMetric(metric)
    selected_config = config or LiquidityMovementDetectionConfig()

    if not isinstance(
        selected_config,
        LiquidityMovementDetectionConfig,
    ):
        raise TypeError("config must be LiquidityMovementDetectionConfig")

    with localcontext(
        Context(
            prec=selected_config.decimal_precision,
            rounding=ROUND_HALF_EVEN,
        )
    ):
        available_values = [
            value
            for bar in series.bars
            if (
                value := _metric_value(
                    bar,
                    selected_metric,
                    decimal_precision=selected_config.decimal_precision,
                )
            )
            is not None
        ]

        if len(available_values) < 2:
            return ()

        population_range = max(available_values) - min(available_values)

        if population_range <= 0:
            return ()

        candidates: list[LiquidityMovementCandidate] = []

        for segment_index, segment in enumerate(series.segments):
            run_start: int | None = None

            for index in range(
                segment.context_start_index,
                segment.context_end_index + 2,
            ):
                in_bounds = index <= segment.context_end_index
                value = (
                    _metric_value(
                        series.bars[index],
                        selected_metric,
                        decimal_precision=selected_config.decimal_precision,
                    )
                    if in_bounds
                    else None
                )

                if value is not None:
                    if run_start is None:
                        run_start = index
                    continue

                if run_start is None:
                    continue

                run_end = index - 1

                if run_end > run_start:
                    candidates.extend(
                        _detect_run(
                            series=series,
                            segment=segment,
                            segment_index=segment_index,
                            metric=selected_metric,
                            run_start=run_start,
                            run_end=run_end,
                            population_range=population_range,
                            config=selected_config,
                            retain_terminal=(run_end == segment.context_end_index),
                        )
                    )

                run_start = None

        return tuple(
            sorted(
                candidates,
                key=lambda candidate: (
                    candidate.segment_index,
                    candidate.start_index,
                    candidate.end_index,
                    candidate.direction.value,
                    (
                        candidate.confirmation_index
                        if candidate.confirmation_index is not None
                        else len(series.bars)
                    ),
                ),
            )
        )


__all__ = [
    "LM_DETECTOR_ALGORITHM_VERSION",
    "LM_DETECTOR_SCHEMA",
    "LM_DETECTOR_SCHEMA_VERSION",
    "LiquidityMetric",
    "LiquidityMovementCandidate",
    "LiquidityMovementDetectionConfig",
    "LiquidityMovementDirection",
    "LiquidityMovementError",
    "detect_liquidity_movements",
    "liquidity_metric_value",
]

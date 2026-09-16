# l2shock/analysis/execution.py
"""Deterministic execution of Liquidity Movement detection and ranking.

This module composes the verified aligned dataset, segment-scoped detector, and
population-scoped ranking engine.

Ownership rules:

- activity and chart series are executed independently when their timeframe
  labels differ;
- the same timeframe is executed only once when activity and chart resolutions
  coincide;
- metric/timeframe/direction populations remain statistically independent;
- scan bounds come from the complete usable selected-metric series, not merely
  from retained LM endpoints;
- result identity depends only on data-affecting analysis inputs and algorithm
  versions;
- cache state, progress callbacks, UI visibility, colors, and chart navigation
  do not affect analysis identity;
- cancellation never publishes a partial result to the cache.

This module performs no database writes and owns no NiceGUI state.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Final, TypeAlias

from l2shock.analysis.dataset import (
    AlignedAnalysisDataset,
    TimeframeAnalysisSeries,
)
from l2shock.analysis.liquidity_movement import (
    LM_DETECTOR_ALGORITHM_VERSION,
    LM_DETECTOR_SCHEMA_VERSION,
    LiquidityMetric,
    LiquidityMovementCandidate,
    LiquidityMovementDetectionConfig,
    detect_liquidity_movements,
    liquidity_metric_value,
)
from l2shock.analysis.ranking import (
    LM_RANKING_ALGORITHM_VERSION,
    LM_RANKING_SCHEMA_VERSION,
    LiquidityMovementPopulationKey,
    LiquidityMovementRankingBatch,
    LiquidityMovementRankingConfig,
    LiquidityMovementScanBounds,
    RankedLiquidityMovement,
    rank_liquidity_movements,
    ranking_config_from_lm_config,
)

log = logging.getLogger(__name__)

LM_ANALYSIS_EXECUTION_SCHEMA: Final[str] = (
    "l2shock.liquidity_movement_analysis_execution"
)
LM_ANALYSIS_EXECUTION_SCHEMA_VERSION: Final[int] = 1
LM_ANALYSIS_EXECUTION_ALGORITHM_VERSION: Final[str] = "aligned-detect-rank-cache-v1"


class LiquidityMovementAnalysisExecutionError(ValueError):
    """LM execution input or output violates its deterministic contract."""


class LiquidityMovementAnalysisCancelledError(RuntimeError):
    """Cooperative cancellation interrupted LM analysis execution."""


class LiquidityMovementAnalysisProgressPhase(StrEnum):
    """Stable phases emitted by the synchronous LM analysis executor."""

    PREPARING = "preparing"
    DETECTING = "detecting"
    RANKING = "ranking"
    CACHE_HIT = "cache_hit"
    COMPLETED = "completed"


AnalysisCancellationProbe: TypeAlias = Callable[[], bool]
AnalysisProgressSink: TypeAlias = Callable[
    ["LiquidityMovementAnalysisProgress"],
    object,
]


def _canonical_decimal_text(
    value: Decimal,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise LiquidityMovementAnalysisExecutionError(
            f"{field_name} must be a finite Decimal"
        )

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if text in {"", "-0", "+0"}:
        text = "0"

    if "e" in text.lower():
        raise LiquidityMovementAnalysisExecutionError(
            f"{field_name} must not use exponent notation"
        )

    return text


def _positive_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LiquidityMovementAnalysisExecutionError(
            f"{field_name} must be a positive integer"
        )

    return value


def _nonnegative_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiquidityMovementAnalysisExecutionError(
            f"{field_name} must be an integer"
        )

    if value < 0:
        raise LiquidityMovementAnalysisExecutionError(
            f"{field_name} must be non-negative"
        )

    return value


def _canonical_sha256(
    field_name: str,
    value: object,
) -> str:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise LiquidityMovementAnalysisExecutionError(
            f"{field_name} must be a canonical lowercase SHA-256"
        )

    return digest


def _check_cancellation(
    cancellation_probe: AnalysisCancellationProbe | None,
) -> None:
    if cancellation_probe is None:
        return

    try:
        cancelled = cancellation_probe()
    except LiquidityMovementAnalysisCancelledError:
        raise
    except Exception as exc:
        raise LiquidityMovementAnalysisExecutionError(
            "Analysis cancellation probe failed"
        ) from exc

    if not isinstance(cancelled, bool):
        raise LiquidityMovementAnalysisExecutionError(
            "Analysis cancellation probe must return bool"
        )

    if cancelled:
        raise LiquidityMovementAnalysisCancelledError(
            "Liquidity Movement analysis was cancelled"
        )


def _emit_progress(
    sink: AnalysisProgressSink | None,
    event: LiquidityMovementAnalysisProgress,
) -> None:
    if sink is None:
        return

    try:
        result = sink(event)
    except Exception:
        # Progress rendering is not analytical truth. A disconnected UI client
        # must not change candidate detection or ranking output.
        log.exception(
            "Liquidity Movement analysis progress sink failed for %s.",
            event.analysis_id,
        )
        return

    if hasattr(result, "__await__"):
        raise LiquidityMovementAnalysisExecutionError(
            "Synchronous analysis progress sinks must not return an awaitable"
        )


@dataclass(frozen=True, slots=True)
class LiquidityMovementAnalysisConfig:
    """Complete semantic configuration for LM execution."""

    detection: LiquidityMovementDetectionConfig = LiquidityMovementDetectionConfig()
    ranking: LiquidityMovementRankingConfig = LiquidityMovementRankingConfig()
    metrics: tuple[LiquidityMetric, ...] = tuple(LiquidityMetric)

    def __post_init__(self) -> None:
        if not isinstance(
            self.detection,
            LiquidityMovementDetectionConfig,
        ):
            raise TypeError("detection must be LiquidityMovementDetectionConfig")

        if not isinstance(
            self.ranking,
            LiquidityMovementRankingConfig,
        ):
            raise TypeError("ranking must be LiquidityMovementRankingConfig")

        normalized_metrics: set[LiquidityMetric] = set()

        for raw_metric in tuple(self.metrics):
            normalized_metrics.add(LiquidityMetric(raw_metric))

        if not normalized_metrics:
            raise LiquidityMovementAnalysisExecutionError(
                "At least one liquidity metric must be selected"
            )

        canonical_metrics = tuple(
            metric for metric in LiquidityMetric if metric in normalized_metrics
        )

        object.__setattr__(
            self,
            "metrics",
            canonical_metrics,
        )

    def to_canonical_dict(self) -> dict[str, object]:
        detection = self.detection
        ranking = self.ranking

        return {
            "metrics": [metric.value for metric in self.metrics],
            "detection": {
                "confirmation_retracement_fraction": (
                    _canonical_decimal_text(
                        detection.confirmation_retracement_fraction,
                        field_name=("confirmation_retracement_fraction"),
                    )
                ),
                "decimal_precision": detection.decimal_precision,
            },
            "ranking": {
                "top_n_height": ranking.top_n_height,
                "top_n_sharpness": ranking.top_n_sharpness,
                "priority_height": _canonical_decimal_text(
                    ranking.priority_height,
                    field_name="priority_height",
                ),
                "priority_sharpness": _canonical_decimal_text(
                    ranking.priority_sharpness,
                    field_name="priority_sharpness",
                ),
                "priority_endpoint_extremeness": (
                    _canonical_decimal_text(
                        ranking.priority_endpoint_extremeness,
                        field_name=("priority_endpoint_extremeness"),
                    )
                ),
                "priority_retracement_magnitude": (
                    _canonical_decimal_text(
                        ranking.priority_retracement_magnitude,
                        field_name=("priority_retracement_magnitude"),
                    )
                ),
                "priority_retracement_count": (
                    _canonical_decimal_text(
                        ranking.priority_retracement_count,
                        field_name=("priority_retracement_count"),
                    )
                ),
                "decimal_precision": ranking.decimal_precision,
            },
        }


@dataclass(frozen=True, slots=True, order=True)
class LiquidityMovementAnalysisSliceKey:
    """Identity of one metric/timeframe execution slice."""

    timeframe_label: str
    metric: LiquidityMetric

    def __post_init__(self) -> None:
        timeframe = str(self.timeframe_label or "").strip()

        if not timeframe:
            raise LiquidityMovementAnalysisExecutionError(
                "timeframe_label cannot be blank"
            )

        object.__setattr__(
            self,
            "timeframe_label",
            timeframe,
        )
        object.__setattr__(
            self,
            "metric",
            LiquidityMetric(self.metric),
        )


@dataclass(frozen=True, slots=True)
class LiquidityMovementAnalysisSlice:
    """Detection and ranking output for one metric/timeframe pair."""

    key: LiquidityMovementAnalysisSliceKey
    scan_bounds: LiquidityMovementScanBounds | None
    candidates: tuple[LiquidityMovementCandidate, ...]
    rankings: tuple[RankedLiquidityMovement, ...]

    def __post_init__(self) -> None:
        if not isinstance(
            self.key,
            LiquidityMovementAnalysisSliceKey,
        ):
            raise TypeError("key must be LiquidityMovementAnalysisSliceKey")

        candidates = tuple(self.candidates)
        rankings = tuple(self.rankings)

        for candidate in candidates:
            if not isinstance(
                candidate,
                LiquidityMovementCandidate,
            ):
                raise TypeError(
                    "candidates must contain " "LiquidityMovementCandidate objects"
                )

            if (
                candidate.metric is not self.key.metric
                or candidate.timeframe_label != self.key.timeframe_label
            ):
                raise LiquidityMovementAnalysisExecutionError(
                    "Candidate does not belong to its analysis slice"
                )

        for ranking in rankings:
            if not isinstance(
                ranking,
                RankedLiquidityMovement,
            ):
                raise TypeError("rankings must contain RankedLiquidityMovement")

            if (
                ranking.candidate.metric is not self.key.metric
                or ranking.candidate.timeframe_label != self.key.timeframe_label
            ):
                raise LiquidityMovementAnalysisExecutionError(
                    "Ranking does not belong to its analysis slice"
                )

        if candidates:
            if self.scan_bounds is None:
                raise LiquidityMovementAnalysisExecutionError(
                    "A non-empty candidate slice requires scan bounds"
                )

            if (
                self.scan_bounds.metric is not self.key.metric
                or self.scan_bounds.timeframe_label != self.key.timeframe_label
            ):
                raise LiquidityMovementAnalysisExecutionError(
                    "scan_bounds does not belong to its analysis slice"
                )

        ranked_candidates = tuple(ranking.candidate for ranking in rankings)

        if len(rankings) != len(candidates) or set(ranked_candidates) != set(
            candidates
        ):
            raise LiquidityMovementAnalysisExecutionError(
                "Slice rankings do not cover exactly its candidates"
            )

        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "rankings", rankings)

    @property
    def selected(
        self,
    ) -> tuple[RankedLiquidityMovement, ...]:
        return tuple(ranking for ranking in self.rankings if ranking.selected)


@dataclass(frozen=True, slots=True)
class LiquidityMovementAnalysisProgress:
    """One immutable synchronous analysis-progress event."""

    analysis_id: str
    phase: LiquidityMovementAnalysisProgressPhase
    message: str
    completed_units: int
    total_units: int
    current_slice: LiquidityMovementAnalysisSliceKey | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "analysis_id",
            _canonical_sha256(
                "analysis_id",
                self.analysis_id,
            ),
        )
        object.__setattr__(
            self,
            "phase",
            LiquidityMovementAnalysisProgressPhase(self.phase),
        )

        message = str(self.message or "").strip()

        if not message:
            raise LiquidityMovementAnalysisExecutionError(
                "Analysis progress message cannot be blank"
            )

        completed = _nonnegative_integer(
            "completed_units",
            self.completed_units,
        )
        total = _positive_integer(
            "total_units",
            self.total_units,
        )

        if completed > total:
            raise LiquidityMovementAnalysisExecutionError(
                "completed_units cannot exceed total_units"
            )

        if self.current_slice is not None and not isinstance(
            self.current_slice,
            LiquidityMovementAnalysisSliceKey,
        ):
            raise TypeError(
                "current_slice must be null or " "LiquidityMovementAnalysisSliceKey"
            )

        object.__setattr__(self, "message", message)
        object.__setattr__(self, "completed_units", completed)
        object.__setattr__(self, "total_units", total)


@dataclass(frozen=True, slots=True)
class LiquidityMovementAnalysisResult:
    """Complete deterministic LM analysis output for one aligned dataset."""

    dataset: AlignedAnalysisDataset
    config: LiquidityMovementAnalysisConfig
    analysis_id: str
    timeframe_labels: tuple[str, ...]
    slices: tuple[LiquidityMovementAnalysisSlice, ...]
    ranking_batch: LiquidityMovementRankingBatch

    schema: str = LM_ANALYSIS_EXECUTION_SCHEMA
    schema_version: int = LM_ANALYSIS_EXECUTION_SCHEMA_VERSION
    algorithm_version: str = LM_ANALYSIS_EXECUTION_ALGORITHM_VERSION

    def __post_init__(self) -> None:
        if not isinstance(
            self.dataset,
            AlignedAnalysisDataset,
        ):
            raise TypeError("dataset must be AlignedAnalysisDataset")

        if not isinstance(
            self.config,
            LiquidityMovementAnalysisConfig,
        ):
            raise TypeError("config must be LiquidityMovementAnalysisConfig")

        if self.schema != LM_ANALYSIS_EXECUTION_SCHEMA:
            raise LiquidityMovementAnalysisExecutionError(
                "Unsupported LM analysis execution schema"
            )

        if self.schema_version != LM_ANALYSIS_EXECUTION_SCHEMA_VERSION:
            raise LiquidityMovementAnalysisExecutionError(
                "Unsupported LM analysis execution schema version"
            )

        if self.algorithm_version != LM_ANALYSIS_EXECUTION_ALGORITHM_VERSION:
            raise LiquidityMovementAnalysisExecutionError(
                "Unsupported LM analysis execution algorithm version"
            )

        analysis_id = _canonical_sha256(
            "analysis_id",
            self.analysis_id,
        )
        expected_id = liquidity_movement_analysis_id(
            self.dataset,
            self.config,
        )

        if analysis_id != expected_id:
            raise LiquidityMovementAnalysisExecutionError(
                "analysis_id does not match dataset and configuration"
            )

        expected_timeframes = _unique_series_labels(
            self.dataset,
        )
        timeframe_labels = tuple(
            str(label or "").strip() for label in self.timeframe_labels
        )

        if timeframe_labels != expected_timeframes:
            raise LiquidityMovementAnalysisExecutionError(
                "timeframe_labels do not match dataset execution ownership"
            )

        slices = tuple(self.slices)
        expected_slice_keys = tuple(
            LiquidityMovementAnalysisSliceKey(
                timeframe_label=timeframe_label,
                metric=metric,
            )
            for timeframe_label in timeframe_labels
            for metric in self.config.metrics
        )

        if tuple(item.key for item in slices) != expected_slice_keys:
            raise LiquidityMovementAnalysisExecutionError(
                "Analysis slices are incomplete or incorrectly ordered"
            )

        if not isinstance(
            self.ranking_batch,
            LiquidityMovementRankingBatch,
        ):
            raise TypeError("ranking_batch must be LiquidityMovementRankingBatch")

        slice_rankings = tuple(ranking for item in slices for ranking in item.rankings)

        if set(slice_rankings) != set(self.ranking_batch.rankings):
            raise LiquidityMovementAnalysisExecutionError(
                "Analysis slices and ranking batch disagree"
            )

        object.__setattr__(self, "analysis_id", analysis_id)
        object.__setattr__(
            self,
            "timeframe_labels",
            timeframe_labels,
        )
        object.__setattr__(self, "slices", slices)

    @property
    def selected(
        self,
    ) -> tuple[RankedLiquidityMovement, ...]:
        return self.ranking_batch.selected

    @property
    def candidates(
        self,
    ) -> tuple[LiquidityMovementCandidate, ...]:
        return tuple(candidate for item in self.slices for candidate in item.candidates)

    def slice(
        self,
        *,
        timeframe_label: str,
        metric: LiquidityMetric | str,
    ) -> LiquidityMovementAnalysisSlice:
        key = LiquidityMovementAnalysisSliceKey(
            timeframe_label=timeframe_label,
            metric=LiquidityMetric(metric),
        )

        for item in self.slices:
            if item.key == key:
                return item

        raise KeyError(
            f"Unknown analysis slice " f"{key.timeframe_label}/{key.metric.value}"
        )


def _unique_series(
    dataset: AlignedAnalysisDataset,
) -> tuple[TimeframeAnalysisSeries, ...]:
    result: list[TimeframeAnalysisSeries] = []
    seen: set[str] = set()

    for series in (
        dataset.activity,
        dataset.chart,
    ):
        label = series.timeframe.label

        if label in seen:
            continue

        seen.add(label)
        result.append(series)

    return tuple(result)


def _unique_series_labels(
    dataset: AlignedAnalysisDataset,
) -> tuple[str, ...]:
    return tuple(series.timeframe.label for series in _unique_series(dataset))


def _execution_identity_payload(
    dataset: AlignedAnalysisDataset,
    config: LiquidityMovementAnalysisConfig,
) -> dict[str, object]:
    return {
        "schema": LM_ANALYSIS_EXECUTION_SCHEMA,
        "schema_version": LM_ANALYSIS_EXECUTION_SCHEMA_VERSION,
        "algorithm_version": (LM_ANALYSIS_EXECUTION_ALGORITHM_VERSION),
        "dataset_analysis_id": dataset.analysis_id,
        "detector": {
            "schema_version": LM_DETECTOR_SCHEMA_VERSION,
            "algorithm_version": LM_DETECTOR_ALGORITHM_VERSION,
        },
        "ranking": {
            "schema_version": LM_RANKING_SCHEMA_VERSION,
            "algorithm_version": LM_RANKING_ALGORITHM_VERSION,
        },
        "timeframe_labels": list(_unique_series_labels(dataset)),
        "config": config.to_canonical_dict(),
    }


def liquidity_movement_analysis_id(
    dataset: AlignedAnalysisDataset,
    config: LiquidityMovementAnalysisConfig | None = None,
) -> str:
    """Return deterministic identity for one dataset/config execution."""
    if not isinstance(
        dataset,
        AlignedAnalysisDataset,
    ):
        raise TypeError("dataset must be AlignedAnalysisDataset")

    selected_config = (
        config if config is not None else LiquidityMovementAnalysisConfig()
    )

    if not isinstance(
        selected_config,
        LiquidityMovementAnalysisConfig,
    ):
        raise TypeError("config must be LiquidityMovementAnalysisConfig")

    payload = _execution_identity_payload(
        dataset,
        selected_config,
    )
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest()


def _scan_bounds(
    series: TimeframeAnalysisSeries,
    metric: LiquidityMetric,
) -> LiquidityMovementScanBounds | None:
    values = tuple(
        value
        for bar in series.bars
        if (
            value := liquidity_metric_value(
                bar,
                metric,
            )
        )
        is not None
    )

    if len(values) < 2:
        return None

    minimum = min(values)
    maximum = max(values)

    if maximum <= minimum:
        return None

    return LiquidityMovementScanBounds(
        metric=metric,
        timeframe_label=series.timeframe.label,
        minimum=minimum,
        maximum=maximum,
    )


def execute_liquidity_movement_analysis(
    dataset: AlignedAnalysisDataset,
    *,
    config: LiquidityMovementAnalysisConfig | None = None,
    cancellation_probe: AnalysisCancellationProbe | None = None,
    progress_sink: AnalysisProgressSink | None = None,
    cache: LiquidityMovementAnalysisCache | None = None,
) -> LiquidityMovementAnalysisResult:
    """Detect and rank LMs for every configured metric and unique timeframe."""
    if not isinstance(
        dataset,
        AlignedAnalysisDataset,
    ):
        raise TypeError("dataset must be AlignedAnalysisDataset")

    selected_config = (
        config if config is not None else LiquidityMovementAnalysisConfig()
    )

    if not isinstance(
        selected_config,
        LiquidityMovementAnalysisConfig,
    ):
        raise TypeError("config must be LiquidityMovementAnalysisConfig")

    analysis_id = liquidity_movement_analysis_id(
        dataset,
        selected_config,
    )

    # A caller that has already requested cancellation must not receive a
    # successful cached result. Otherwise a stop/result race can publish an
    # analysis after the operation was cancelled.
    _check_cancellation(cancellation_probe)

    if cache is not None:
        if not isinstance(
            cache,
            LiquidityMovementAnalysisCache,
        ):
            raise TypeError("cache must be LiquidityMovementAnalysisCache")

        cached = cache.get(analysis_id)

        if cached is not None:
            _emit_progress(
                progress_sink,
                LiquidityMovementAnalysisProgress(
                    analysis_id=analysis_id,
                    phase=(LiquidityMovementAnalysisProgressPhase.CACHE_HIT),
                    message="Reused cached Liquidity Movement analysis.",
                    completed_units=1,
                    total_units=1,
                ),
            )
            return cached

    # Re-check after cache access so cancellation requested during the lookup
    # is observed before any detection work begins.
    _check_cancellation(cancellation_probe)

    series_values = _unique_series(dataset)
    detection_units = len(series_values) * len(selected_config.metrics)
    total_units = detection_units + 1

    _emit_progress(
        progress_sink,
        LiquidityMovementAnalysisProgress(
            analysis_id=analysis_id,
            phase=(LiquidityMovementAnalysisProgressPhase.PREPARING),
            message="Preparing Liquidity Movement analysis.",
            completed_units=0,
            total_units=total_units,
        ),
    )

    candidates_by_key: dict[
        LiquidityMovementAnalysisSliceKey,
        tuple[LiquidityMovementCandidate, ...],
    ] = {}
    bounds_by_key: dict[
        LiquidityMovementAnalysisSliceKey,
        LiquidityMovementScanBounds | None,
    ] = {}

    all_candidates: list[LiquidityMovementCandidate] = []
    ranking_bounds: list[LiquidityMovementScanBounds] = []
    completed_units = 0

    for series in series_values:
        for metric in selected_config.metrics:
            _check_cancellation(cancellation_probe)

            key = LiquidityMovementAnalysisSliceKey(
                timeframe_label=series.timeframe.label,
                metric=metric,
            )
            bounds = _scan_bounds(
                series,
                metric,
            )

            if bounds is None:
                candidates: tuple[LiquidityMovementCandidate, ...] = ()
            else:
                candidates = detect_liquidity_movements(
                    series,
                    metric=metric,
                    config=selected_config.detection,
                )

                if candidates:
                    ranking_bounds.append(bounds)
                    all_candidates.extend(candidates)

            candidates_by_key[key] = candidates
            bounds_by_key[key] = bounds

            completed_units += 1

            _emit_progress(
                progress_sink,
                LiquidityMovementAnalysisProgress(
                    analysis_id=analysis_id,
                    phase=(LiquidityMovementAnalysisProgressPhase.DETECTING),
                    message=(
                        "Detected Liquidity Movements for "
                        f"{metric.value}/{series.timeframe.label}."
                    ),
                    completed_units=completed_units,
                    total_units=total_units,
                    current_slice=key,
                ),
            )

    _check_cancellation(cancellation_probe)

    _emit_progress(
        progress_sink,
        LiquidityMovementAnalysisProgress(
            analysis_id=analysis_id,
            phase=(LiquidityMovementAnalysisProgressPhase.RANKING),
            message="Ranking Liquidity Movement populations.",
            completed_units=completed_units,
            total_units=total_units,
        ),
    )

    ranking_batch = rank_liquidity_movements(
        tuple(all_candidates),
        scan_bounds=tuple(ranking_bounds),
        config=selected_config.ranking,
    )

    _check_cancellation(cancellation_probe)

    rankings_by_key: dict[
        LiquidityMovementAnalysisSliceKey,
        list[RankedLiquidityMovement],
    ] = {key: [] for key in candidates_by_key}

    for ranking in ranking_batch.rankings:
        key = LiquidityMovementAnalysisSliceKey(
            timeframe_label=(ranking.population_key.timeframe_label),
            metric=ranking.population_key.metric,
        )
        rankings_by_key[key].append(ranking)

    slices = tuple(
        LiquidityMovementAnalysisSlice(
            key=key,
            scan_bounds=bounds_by_key[key],
            candidates=candidates_by_key[key],
            rankings=tuple(rankings_by_key[key]),
        )
        for series in series_values
        for metric in selected_config.metrics
        for key in (
            LiquidityMovementAnalysisSliceKey(
                timeframe_label=series.timeframe.label,
                metric=metric,
            ),
        )
    )

    result = LiquidityMovementAnalysisResult(
        dataset=dataset,
        config=selected_config,
        analysis_id=analysis_id,
        timeframe_labels=tuple(series.timeframe.label for series in series_values),
        slices=slices,
        ranking_batch=ranking_batch,
    )

    if cache is not None:
        cache.put(result)

    _emit_progress(
        progress_sink,
        LiquidityMovementAnalysisProgress(
            analysis_id=analysis_id,
            phase=(LiquidityMovementAnalysisProgressPhase.COMPLETED),
            message="Liquidity Movement analysis completed.",
            completed_units=total_units,
            total_units=total_units,
        ),
    )

    return result


class LiquidityMovementAnalysisCache:
    """Small process-local thread-safe LRU cache of immutable LM results."""

    def __init__(
        self,
        *,
        maximum_entries: int = 8,
    ) -> None:
        self._maximum_entries = _positive_integer(
            "maximum_entries",
            maximum_entries,
        )
        self._entries: OrderedDict[
            str,
            LiquidityMovementAnalysisResult,
        ] = OrderedDict()
        self._lock = threading.Lock()

    @property
    def maximum_entries(self) -> int:
        return self._maximum_entries

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def get(
        self,
        analysis_id: str,
    ) -> LiquidityMovementAnalysisResult | None:
        digest = _canonical_sha256(
            "analysis_id",
            analysis_id,
        )

        with self._lock:
            result = self._entries.get(digest)

            if result is None:
                return None

            self._entries.move_to_end(digest)
            return result

    def put(
        self,
        result: LiquidityMovementAnalysisResult,
    ) -> None:
        if not isinstance(
            result,
            LiquidityMovementAnalysisResult,
        ):
            raise TypeError("result must be LiquidityMovementAnalysisResult")

        with self._lock:
            self._entries[result.analysis_id] = result
            self._entries.move_to_end(result.analysis_id)

            while len(self._entries) > self._maximum_entries:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    @property
    def analysis_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._entries)


def analysis_config_from_lm_config(
    value: object,
) -> LiquidityMovementAnalysisConfig:
    """Build exact detector/ranking configuration from app LM settings."""
    if not hasattr(
        value,
        "confirmation_retracement_fraction",
    ):
        raise TypeError("value does not expose confirmation_retracement_fraction")

    detection = LiquidityMovementDetectionConfig(
        confirmation_retracement_fraction=Decimal(
            str(
                getattr(
                    value,
                    "confirmation_retracement_fraction",
                )
            )
        )
    )

    return LiquidityMovementAnalysisConfig(
        detection=detection,
        ranking=ranking_config_from_lm_config(value),
    )


__all__ = [
    "LM_ANALYSIS_EXECUTION_ALGORITHM_VERSION",
    "LM_ANALYSIS_EXECUTION_SCHEMA",
    "LM_ANALYSIS_EXECUTION_SCHEMA_VERSION",
    "AnalysisCancellationProbe",
    "AnalysisProgressSink",
    "LiquidityMovementAnalysisCache",
    "LiquidityMovementAnalysisCancelledError",
    "LiquidityMovementAnalysisConfig",
    "LiquidityMovementAnalysisExecutionError",
    "LiquidityMovementAnalysisProgress",
    "LiquidityMovementAnalysisProgressPhase",
    "LiquidityMovementAnalysisResult",
    "LiquidityMovementAnalysisSlice",
    "LiquidityMovementAnalysisSliceKey",
    "analysis_config_from_lm_config",
    "execute_liquidity_movement_analysis",
    "liquidity_movement_analysis_id",
]

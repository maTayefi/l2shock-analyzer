# l2shock/ui/analysis_export.py
"""Deterministic JSON export for completed Liquidity Movement analysis.

The export contains exact analytical values as canonical decimal strings.
Presentation floats, chart render tokens, zoom state, crosshair state, and
temporary table selection are deliberately excluded.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Final

from l2shock.analysis import (
    LiquidityMovementAnalysisResult,
    LiquidityMovementCandidate,
    RankedLiquidityMovement,
)
from l2shock.timeutils import require_aware_utc

ANALYSIS_EXPORT_SCHEMA: Final[str] = "l2shock.liquidity_movement_analysis_export"
ANALYSIS_EXPORT_SCHEMA_VERSION: Final[int] = 2


class AnalysisExportError(ValueError):
    """A completed analysis cannot be exported canonically."""


def _decimal_text(
    value: Decimal | None,
) -> str | None:
    if value is None:
        return None

    if not isinstance(value, Decimal) or not value.is_finite():
        raise AnalysisExportError("Exported analytical decimals must be finite")

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if text in {"", "-0", "+0"}:
        text = "0"

    if "e" in text.lower():
        raise AnalysisExportError("Exported decimals must not use exponent notation")

    return text


def _utc_text(
    value: datetime | None,
) -> str | None:
    if value is None:
        return None

    return (
        require_aware_utc("export datetime", value).isoformat().replace("+00:00", "Z")
    )


def _enum_text(value: Enum | object) -> str:
    if isinstance(value, Enum):
        return str(value.value)

    return str(value)


def _candidate_payload(
    candidate: LiquidityMovementCandidate,
) -> dict[str, object]:
    return {
        "metric": candidate.metric.value,
        "timeframe": candidate.timeframe_label,
        "direction": candidate.direction.value,
        "segment_index": candidate.segment_index,
        "start_index": candidate.start_index,
        "end_index": candidate.end_index,
        "confirmation_index": candidate.confirmation_index,
        "in_range_start_index": candidate.in_range_start_index,
        "in_range_end_index": candidate.in_range_end_index,
        "start_time_utc": _utc_text(candidate.start_time_utc),
        "end_time_utc": _utc_text(candidate.end_time_utc),
        "confirmation_time_utc": _utc_text(candidate.confirmation_time_utc),
        "in_range_start_time_utc": _utc_text(candidate.in_range_start_time_utc),
        "in_range_end_time_utc": _utc_text(candidate.in_range_end_time_utc),
        "start_value": _decimal_text(candidate.start_value),
        "end_value": _decimal_text(candidate.end_value),
        "absolute_height": _decimal_text(candidate.absolute_height),
        "relative_height": _decimal_text(candidate.relative_height),
        "bars": candidate.bars,
        "sharpness": _decimal_text(candidate.sharpness),
        "adverse_move_count": candidate.adverse_move_count,
        "adverse_move_total": _decimal_text(candidate.adverse_move_total),
        "adverse_move_maximum": _decimal_text(candidate.adverse_move_maximum),
        "adverse_move_total_fraction": _decimal_text(
            candidate.adverse_move_total_fraction
        ),
        "adverse_move_maximum_fraction": _decimal_text(
            candidate.adverse_move_maximum_fraction
        ),
        "confirmation_retracement": _decimal_text(candidate.confirmation_retracement),
        "confirmation_retracement_fraction": _decimal_text(
            candidate.confirmation_retracement_fraction
        ),
        "degraded_bar_count": candidate.degraded_bar_count,
        "terminal_offline": candidate.terminal_offline,
        "schema": candidate.schema,
        "schema_version": candidate.schema_version,
        "algorithm_version": candidate.algorithm_version,
    }


def _candidate_identity_payload(
    candidate: LiquidityMovementCandidate,
) -> tuple[str, dict[str, object]]:
    payload = _candidate_payload(candidate)
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest(), payload


def _ranking_payload(
    ranking: RankedLiquidityMovement,
    *,
    candidate_id: str,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "population": {
            "metric": ranking.population_key.metric.value,
            "timeframe": (ranking.population_key.timeframe_label),
            "direction": ranking.population_key.direction.value,
        },
        "height": {
            "percentile": _decimal_text(ranking.height.percentile),
            "modified_z": _decimal_text(ranking.height.modified_z),
            "normalized_positive_tail_modified_z": (
                _decimal_text(ranking.height.normalized_positive_tail_modified_z)
            ),
            "combined": _decimal_text(ranking.height.combined),
        },
        "sharpness": {
            "percentile": _decimal_text(ranking.sharpness.percentile),
            "modified_z": _decimal_text(ranking.sharpness.modified_z),
            "normalized_positive_tail_modified_z": (
                _decimal_text(ranking.sharpness.normalized_positive_tail_modified_z)
            ),
            "combined": _decimal_text(ranking.sharpness.combined),
        },
        "boundary_extremeness": {
            "start": _decimal_text(ranking.start_extremeness),
            "end": _decimal_text(ranking.end_extremeness),
            "equal_weight_mean": _decimal_text(ranking.boundary_extremeness),
            "start_weight_fraction": "0.5",
            "end_weight_fraction": "0.5",
        },
        "retracement_magnitude_quality": _decimal_text(
            ranking.retracement_magnitude_quality
        ),
        "retracement_count_quality": _decimal_text(ranking.retracement_count_quality),
        "primary_evidence": _decimal_text(ranking.primary_evidence),
        "secondary_evidence": _decimal_text(ranking.secondary_evidence),
        "priority_weighted_evidence": _decimal_text(ranking.priority_weighted_evidence),
        "height_rank": ranking.height_rank,
        "sharpness_rank": ranking.sharpness_rank,
        "population_rank": ranking.population_rank,
        "selected_by_height": ranking.selected_by_height,
        "selected_by_sharpness": (ranking.selected_by_sharpness),
        "selected": ranking.selected,
        "final_rank": ranking.final_rank,
        "schema": ranking.schema,
        "schema_version": ranking.schema_version,
        "algorithm_version": ranking.algorithm_version,
    }


def build_analysis_export_payload(
    result: LiquidityMovementAnalysisResult,
    *,
    timezone_name: str,
) -> dict[str, object]:
    """Return one strict JSON-safe deterministic Analysis export."""

    if not isinstance(
        result,
        LiquidityMovementAnalysisResult,
    ):
        raise TypeError("result must be LiquidityMovementAnalysisResult")

    timezone_text = str(timezone_name or "").strip()

    if not timezone_text:
        raise AnalysisExportError("timezone_name cannot be blank")

    candidate_payloads: list[dict[str, object]] = []
    candidate_id_by_object: dict[int, str] = {}

    for candidate in result.candidates:
        candidate_id, payload = _candidate_identity_payload(candidate)
        candidate_id_by_object[id(candidate)] = candidate_id
        candidate_payloads.append(
            {
                "candidate_id": candidate_id,
                **payload,
            }
        )

    ranking_payloads = [
        _ranking_payload(
            ranking,
            candidate_id=(candidate_id_by_object[id(ranking.candidate)]),
        )
        for ranking in result.ranking_batch.rankings
    ]

    slices: list[dict[str, object]] = []

    for item in result.slices:
        bounds = item.scan_bounds

        slices.append(
            {
                "timeframe": item.key.timeframe_label,
                "metric": item.key.metric.value,
                "scan_bounds": (
                    {
                        "minimum": _decimal_text(bounds.minimum),
                        "maximum": _decimal_text(bounds.maximum),
                    }
                    if bounds is not None
                    else None
                ),
                "candidate_ids": [
                    candidate_id_by_object[id(candidate)]
                    for candidate in item.candidates
                ],
                "selected_candidate_ids": [
                    candidate_id_by_object[id(ranking.candidate)]
                    for ranking in item.selected
                ],
            }
        )

    return {
        "schema": ANALYSIS_EXPORT_SCHEMA,
        "schema_version": ANALYSIS_EXPORT_SCHEMA_VERSION,
        "analysis_id": result.analysis_id,
        "dataset_analysis_id": result.dataset.analysis_id,
        "display_timezone": timezone_text,
        "dataset_provenance": (result.dataset.provenance.to_canonical_dict()),
        "execution": {
            "schema": result.schema,
            "schema_version": result.schema_version,
            "algorithm_version": result.algorithm_version,
            "timeframes": list(result.timeframe_labels),
            "config": result.config.to_canonical_dict(),
        },
        "summary": {
            "candidate_count": len(result.candidates),
            "ranking_count": len(result.ranking_batch.rankings),
            "selected_count": len(result.selected),
            "slice_count": len(result.slices),
        },
        "slices": slices,
        "candidates": candidate_payloads,
        "rankings": ranking_payloads,
        "selected_candidate_ids": [
            candidate_id_by_object[id(ranking.candidate)] for ranking in result.selected
        ],
    }


def analysis_export_json_bytes(
    result: LiquidityMovementAnalysisResult,
    *,
    timezone_name: str,
) -> bytes:
    payload = build_analysis_export_payload(
        result,
        timezone_name=timezone_name,
    )

    return (
        json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def analysis_export_filename(
    result: LiquidityMovementAnalysisResult,
) -> str:
    if not isinstance(
        result,
        LiquidityMovementAnalysisResult,
    ):
        raise TypeError("result must be LiquidityMovementAnalysisResult")

    return (
        "l2shock_"
        + result.dataset.request.base
        + "_"
        + result.analysis_id[:16]
        + "_analysis.json"
    )


__all__ = [
    "ANALYSIS_EXPORT_SCHEMA",
    "ANALYSIS_EXPORT_SCHEMA_VERSION",
    "AnalysisExportError",
    "analysis_export_filename",
    "analysis_export_json_bytes",
    "build_analysis_export_payload",
]

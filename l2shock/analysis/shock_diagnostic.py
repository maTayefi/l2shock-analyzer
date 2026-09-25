# l2shock/analysis/shock_diagnostic.py
"""Exact, JSON-safe diagnostic export of a verified shock review.

This is a research/inspection artifact, not the LM export format or a
confidence-ranked set of detected market events.
"""

from __future__ import annotations

import json
from fractions import Fraction

from l2shock.analysis.shock_evidence import (
    ShockEvidenceConfig,
    describe_shock_evidence,
)
from l2shock.analysis.shock_execution import (
    ShockCandidateScan,
    run_verified_shock_scan,
)
from l2shock.analysis.shock_review import (
    ShockReview,
    review_shock_areas,
)
from l2shock.analysis.shock_start import ShockStartConfig
from l2shock.analysis.shock_dataset import ShockDatasetRequest

SHOCK_DIAGNOSTIC_SCHEMA = "l2shock.shock_start_diagnostic"
SHOCK_DIAGNOSTIC_SCHEMA_VERSION = 1


def _fraction(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def _hypothesis_payload(item) -> dict:
    return {
        "scale_name": item.scale_name,
        "kind": item.kind.value,
        "direction": item.direction.value,
        "a_index": item.a_index,
        "b_index": item.b_index,
        "c_index": item.c_index,
        "a_utc": item.a_utc.isoformat(),
        "b_utc": item.b_utc.isoformat(),
        "c_utc": item.c_utc.isoformat(),
        "a_total": _fraction(item.a_total),
        "b_total": _fraction(item.b_total),
        "c_total": _fraction(item.c_total),
        "scan_range": _fraction(item.scan_range),
        "bc_height": _fraction(item.bc_height),
        "bc_fraction_of_scan_range": _fraction(item.bc_fraction_of_scan_range),
    }


def build_shock_diagnostic_payload(review: ShockReview) -> dict:
    if not isinstance(review, ShockReview):
        raise TypeError("review must be ShockReview")

    scan = review.evidence_result.candidate_scan
    dataset = scan.dataset
    request = dataset.request
    evidence_config = review.evidence_result.config
    rows = review.review_rows()

    areas = []

    for entry, row in zip(
        review.ordered_areas,
        rows,
        strict=True,
    ):
        areas.append(
            {
                **row,
                # review_rows() has descriptive measurements; retain the
                # *entire* area so alternative B choices can be inspected.
                "members": [
                    _hypothesis_payload(member) for member in entry.area.members
                ],
            }
        )

    return {
        "schema": SHOCK_DIAGNOSTIC_SCHEMA,
        "schema_version": SHOCK_DIAGNOSTIC_SCHEMA_VERSION,
        "diagnostic_only": True,
        "price_used_for_eligibility": False,
        "clock": "verified_l2_one_second_utc",
        "l2_input_id": dataset.input_id,
        "candidate_scan_id": scan.scan_id,
        "review_id": review.review_id,
        "request": {
            "base": request.base,
            "preset_hash": request.preset_hash,
            "requested_start_utc": (request.requested_start_utc.isoformat()),
            "requested_end_utc": request.requested_end_utc.isoformat(),
            "effective_start_utc": dataset.start_utc.isoformat(),
            "effective_end_exclusive_utc": dataset.end_utc.isoformat(),
        },
        "coverage": {
            "second_count": len(dataset.seconds),
            "valid_second_count": dataset.valid_second_count,
            "invalid_second_count": dataset.invalid_second_count,
            "component_hours": [
                {
                    "provider": item.provider,
                    "venue": item.venue,
                    "instrument": item.instrument,
                    "component_preset_hash": (item.component_preset_hash),
                    "hour_utc": item.hour_utc.isoformat(),
                    "content_sha256": item.content_sha256,
                }
                for item in dataset.component_hours
            ],
        },
        "candidate_config": {
            "acceleration_ratio": scan.config.acceleration_ratio,
            "scales": [
                {
                    "name": scale.name,
                    "minimum_leg_fraction": str(Fraction(scale.minimum_leg_fraction)),
                    "pivot_radius_seconds": (scale.pivot_radius_seconds),
                    "forward_radius_multiplier": (scale.forward_radius_multiplier),
                }
                for scale in scan.config.scales
            ],
        },
        "evidence_config": {
            "maximum_offset_seconds": (evidence_config.maximum_offset_seconds),
            "minimum_channel_leg_fraction": str(
                Fraction(evidence_config.minimum_channel_leg_fraction)
            ),
        },
        "summary": {
            "total_range": (
                _fraction(scan.total_range) if scan.total_range is not None else None
            ),
            "hypothesis_count": len(scan.hypotheses),
            "b_area_count": len(review.ordered_areas),
            "active_scale_names": list(scan.active_scale_names),
        },
        "areas_in_inspection_order": areas,
    }


def shock_diagnostic_json_bytes(review: ShockReview) -> bytes:
    return (
        json.dumps(
            build_shock_diagnostic_payload(review),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("utf-8")


def build_shock_diagnostic(
    scan: ShockCandidateScan,
    *,
    evidence_config: ShockEvidenceConfig | None = None,
) -> ShockReview:
    """Pure post-processing; never loads data or consults price."""
    return review_shock_areas(
        describe_shock_evidence(
            scan,
            config=evidence_config,
        )
    )


def run_verified_shock_diagnostic(
    session,
    request: ShockDatasetRequest,
    *,
    candidate_config: ShockStartConfig | None = None,
    evidence_config: ShockEvidenceConfig | None = None,
) -> ShockReview:
    """Production read path; the caller owns the database session."""
    scan = run_verified_shock_scan(
        session,
        request,
        config=candidate_config,
    )
    return build_shock_diagnostic(
        scan,
        evidence_config=evidence_config,
    )


__all__ = [
    "SHOCK_DIAGNOSTIC_SCHEMA",
    "SHOCK_DIAGNOSTIC_SCHEMA_VERSION",
    "build_shock_diagnostic",
    "build_shock_diagnostic_payload",
    "run_verified_shock_diagnostic",
    "shock_diagnostic_json_bytes",
]

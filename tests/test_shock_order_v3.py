# tests/test_shock_order_v3.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction
from types import SimpleNamespace

import pytest

from l2shock.analysis.shock_review import (
    SHOCK_REVIEW_ORDER_VERSION,
    SHOCK_REVIEW_ORDER_VERSION_V3,
    SHOCK_REVIEW_SCHEMA,
    SHOCK_REVIEW_SCHEMA_VERSION,
    ShockReview,
    ShockReviewError,
    _order_measured,
    reorder_shock_review,
)
from l2shock.ui.shock_runtime import ManualShockRuntime


@dataclass(frozen=True)
class _Area:
    first_b_index: int
    direction: str = "up"


@dataclass(frozen=True)
class _Item:
    area: _Area
    highest_scale_fraction: Fraction
    total_bc_fraction_of_scan_range: Fraction
    total_bc_sharpness_squared: Fraction
    total_bc_adverse_total_fraction: Fraction
    independent_channel_count: int
    total_c_extremeness: Fraction
    total_pre_b_deviation_fraction_of_scan_range: Fraction = Fraction(0)
    inspection_position: int = 0
    within_tier_percentile_score: Fraction | None = None


# Higher tier, weak everywhere: must stay first under v2 and v3.
_C = _Item(
    _Area(300),
    Fraction(1, 5),
    Fraction(1, 5),
    Fraction(1, 100),
    Fraction(1),
    0,
    Fraction(0),
)
# Same lower tier: A is tallest but slow/dirty; B is slightly shorter but
# sharp, clean, extreme, and Bid/Ask supported.
_A = _Item(
    _Area(100),
    Fraction(1, 10),
    Fraction(9, 10),
    Fraction(1, 100),
    Fraction(1),
    0,
    Fraction(1, 10),
)
_B = _Item(
    _Area(200),
    Fraction(1, 10),
    Fraction(8, 10),
    Fraction(1),
    Fraction(0),
    2,
    Fraction(1),
)


def _positions(items) -> list[int]:
    return [item.area.first_b_index for item in items]


def test_v2_is_structure_then_height() -> None:
    ordered = _order_measured((_A, _B, _C), SHOCK_REVIEW_ORDER_VERSION)

    assert _positions(ordered) == [300, 100, 200]
    assert [item.inspection_position for item in ordered] == [1, 2, 3]
    assert all(item.within_tier_percentile_score is None for item in ordered)


def test_v3_keeps_tier_first_and_rewards_confluence() -> None:
    ordered = _order_measured((_A, _B, _C), SHOCK_REVIEW_ORDER_VERSION_V3)

    assert _positions(ordered) == [300, 200, 100]
    scores = {
        item.area.first_b_index: item.within_tier_percentile_score for item in ordered
    }
    assert scores[300] == Fraction(1)  # singleton tier
    assert scores[200] == Fraction(4, 5)
    assert scores[100] == Fraction(1, 5)


def test_unknown_order_is_rejected() -> None:
    with pytest.raises(ShockReviewError):
        _order_measured((_A,), "weighted_magic_v9")


def _fake_review() -> ShockReview:
    result = SimpleNamespace(
        config=SimpleNamespace(
            maximum_offset_seconds=3,
            minimum_channel_leg_fraction=Fraction(1, 20),
        ),
        candidate_scan=SimpleNamespace(scan_id="s" * 64),
    )
    ordered = _order_measured((_A, _B, _C), SHOCK_REVIEW_ORDER_VERSION)
    return ShockReview(
        evidence_result=result, ordered_areas=ordered, review_id="legacy"
    )


def _legacy_v2_review_id(result) -> str:
    # Frozen copy of the pre-Batch-34 identity; v2 must never change it.
    identity = {
        "schema": SHOCK_REVIEW_SCHEMA,
        "schema_version": SHOCK_REVIEW_SCHEMA_VERSION,
        "order_version": "total_structure_first_v2",
        "candidate_scan_id": result.candidate_scan.scan_id,
        "evidence": {
            "maximum_offset_seconds": result.config.maximum_offset_seconds,
            "minimum_channel_leg_fraction": str(
                Fraction(result.config.minimum_channel_leg_fraction)
            ),
        },
    }
    canonical = json.dumps(
        identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def test_v2_identity_is_preserved_and_v3_is_distinct() -> None:
    review = _fake_review()
    v2 = reorder_shock_review(review, order_version=SHOCK_REVIEW_ORDER_VERSION)
    v3 = reorder_shock_review(review, order_version=SHOCK_REVIEW_ORDER_VERSION_V3)

    assert v2.review_id == _legacy_v2_review_id(review.evidence_result)
    assert v3.review_id != v2.review_id
    assert v3.order_version == SHOCK_REVIEW_ORDER_VERSION_V3


def test_round_trip_v3_back_to_v2_is_identical() -> None:
    review = _fake_review()
    v2 = reorder_shock_review(review, order_version=SHOCK_REVIEW_ORDER_VERSION)
    back = reorder_shock_review(
        reorder_shock_review(v2, order_version=SHOCK_REVIEW_ORDER_VERSION_V3),
        order_version=SHOCK_REVIEW_ORDER_VERSION,
    )

    assert back.ordered_areas == v2.ordered_areas
    assert back.review_id == v2.review_id


def test_runtime_reorders_last_review() -> None:
    runtime = ManualShockRuntime()

    with pytest.raises(RuntimeError):
        runtime.reorder_last_review(SHOCK_REVIEW_ORDER_VERSION_V3)

    runtime._last_review = _fake_review()
    reordered = runtime.reorder_last_review(SHOCK_REVIEW_ORDER_VERSION_V3)

    assert runtime.snapshot().last_review is reordered
    assert reordered.order_version == SHOCK_REVIEW_ORDER_VERSION_V3

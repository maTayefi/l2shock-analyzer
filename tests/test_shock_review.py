# tests/test_shock_review.py
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction

import pytest

from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.shock_dataset import (
    ShockDatasetRequest,
    VerifiedShockDataset,
)
from l2shock.analysis.shock_evidence import (
    ShockEvidenceConfig,
    describe_shock_evidence,
)
from l2shock.analysis.shock_execution import ShockCandidateScan
from l2shock.analysis.shock_review import (
    ShockReviewError,
    review_shock_areas,
)
from l2shock.analysis.shock_start import (
    ShockStartConfig,
    ShockStartDirection,
    ShockStartHypothesis,
    ShockStartKind,
    ShockStructuralScale,
)
from l2shock.ingest.sampling import BookSampleQuality

_START = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)

_TOTALS = (
    100,
    95,
    90,
    80,
    70,
    60,
    70,
    80,
    100,
    120,
    150,
    140,
    130,
    120,
    110,
    120,
    130,
    140,
    150,
    160,
)


def _seconds():
    return tuple(
        L2Second(
            timestamp_utc=_START + timedelta(seconds=index),
            quality=BookSampleQuality.VALID,
            invalid_reason=None,
            bid_liquidity=Decimal(total),
            ask_liquidity=Decimal(0),
            source_count=1,
        )
        for index, total in enumerate(_TOTALS)
    )


def _hypothesis(
    a: int,
    b: int,
    c: int,
    *,
    scale: str,
    scan_range: int,
) -> ShockStartHypothesis:
    height = _TOTALS[c] - _TOTALS[b]
    return ShockStartHypothesis(
        scale_name=scale,
        kind=ShockStartKind.TURNING,
        direction=ShockStartDirection.UP,
        a_index=a,
        b_index=b,
        c_index=c,
        a_utc=_START + timedelta(seconds=a),
        b_utc=_START + timedelta(seconds=b),
        c_utc=_START + timedelta(seconds=c),
        a_total=Fraction(_TOTALS[a]),
        b_total=Fraction(_TOTALS[b]),
        c_total=Fraction(_TOTALS[c]),
        scan_range=Fraction(scan_range),
        bc_height=Fraction(height),
        bc_fraction_of_scan_range=Fraction(height, scan_range),
    )


def _candidate_scan(hypotheses):
    seconds = _seconds()
    request = ShockDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=seconds[0].timestamp_utc,
        requested_end_utc=seconds[-1].timestamp_utc,
    )
    return ShockCandidateScan(
        dataset=VerifiedShockDataset(
            request=request,
            start_utc=seconds[0].timestamp_utc,
            end_utc=seconds[-1].timestamp_utc + timedelta(seconds=1),
            seconds=seconds,
            component_hours=(),
            input_id="b" * 64,
        ),
        config=ShockStartConfig(
            scales=(
                ShockStructuralScale("major", Decimal("0.20"), 4),
                ShockStructuralScale("minor", Decimal("0.05"), 2),
            )
        ),
        hypotheses=tuple(hypotheses),
        scan_id="c" * 64,
    )


def _review(hypotheses, *, offset=1):
    evidence = describe_shock_evidence(
        _candidate_scan(hypotheses),
        config=ShockEvidenceConfig(maximum_offset_seconds=offset),
    )
    return review_shock_areas(evidence)


def test_all_areas_survive_and_total_structure_orders_inspection():
    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review(
        (
            _hypothesis(
                2,
                5,
                10,
                scale="major",
                scan_range=scan_range,
            ),
            _hypothesis(
                12,
                14,
                19,
                scale="minor",
                scan_range=scan_range,
            ),
        )
    )

    assert len(review.ordered_areas) == 2
    assert [entry.inspection_position for entry in review.ordered_areas] == [1, 2]
    assert [entry.area.representative.b_index for entry in review.ordered_areas] == [
        5,
        14,
    ]
    assert review.ordered_areas[0].highest_scale_fraction == Fraction(1, 5)
    assert review.ordered_areas[0].total_bc_fraction_of_scan_range == (Fraction(9, 10))
    assert review.ordered_areas[0].total_ab_signed_change == -30


def test_review_rows_are_exact_json_and_delta_is_not_an_extra_vote():
    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review(
        (
            _hypothesis(
                2,
                5,
                10,
                scale="major",
                scan_range=scan_range,
            ),
        )
    )

    rows = review.review_rows()
    encoded = json.dumps(rows)

    assert len(rows) == 1
    assert rows[0]["total_bc_fraction_of_scan_range"] == "9/10"
    assert rows[0]["review_id"] == review.review_id
    assert encoded
    # Ask is flat. Bid may support the move; Delta is derived from Bid.
    assert review.ordered_areas[0].independent_channel_count <= 1


def test_evidence_tolerance_changes_review_identity_not_candidate_identity():
    scan_range = max(_TOTALS) - min(_TOTALS)
    candidates = (_hypothesis(2, 5, 10, scale="major", scan_range=scan_range),)

    first = _review(candidates, offset=1)
    second = _review(candidates, offset=2)

    assert first.review_id != second.review_id
    assert (
        first.evidence_result.candidate_scan.scan_id
        == second.evidence_result.candidate_scan.scan_id
    )


def test_review_refuses_candidate_values_from_another_dataset():
    scan_range = max(_TOTALS) - min(_TOTALS)
    original = _hypothesis(
        2,
        5,
        10,
        scale="major",
        scan_range=scan_range,
    )
    stale = ShockStartHypothesis(
        scale_name=original.scale_name,
        kind=original.kind,
        direction=original.direction,
        a_index=original.a_index,
        b_index=original.b_index,
        c_index=original.c_index,
        a_utc=original.a_utc,
        b_utc=original.b_utc,
        c_utc=original.c_utc,
        a_total=original.a_total,
        b_total=original.b_total + 1,
        c_total=original.c_total,
        scan_range=original.scan_range,
        bc_height=original.bc_height,
        bc_fraction_of_scan_range=original.bc_fraction_of_scan_range,
    )

    evidence = describe_shock_evidence(_candidate_scan((stale,)))
    with pytest.raises(ShockReviewError, match="do not match L2 input"):
        review_shock_areas(evidence)


def test_diagnostic_export_keeps_every_member_and_is_deterministic():
    from l2shock.analysis.shock_diagnostic import (
        SHOCK_DIAGNOSTIC_SCHEMA,
        shock_diagnostic_json_bytes,
    )

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review(
        (
            _hypothesis(
                2,
                5,
                10,
                scale="major",
                scan_range=scan_range,
            ),
            _hypothesis(
                2,
                5,
                8,
                scale="minor",
                scan_range=scan_range,
            ),
        )
    )

    first = shock_diagnostic_json_bytes(review)
    second = shock_diagnostic_json_bytes(review)
    payload = json.loads(first)

    assert first == second
    assert payload["schema"] == SHOCK_DIAGNOSTIC_SCHEMA
    assert payload["diagnostic_only"] is True
    assert payload["price_used_for_eligibility"] is False
    assert payload["summary"]["hypothesis_count"] == 2
    assert payload["summary"]["b_area_count"] == 1
    assert len(payload["areas_in_inspection_order"][0]["members"]) == 2
    assert payload["areas_in_inspection_order"][0]["members"][0]["b_utc"]
    assert payload["coverage"]["second_count"] == len(_TOTALS)


def test_shock_inspection_page_is_bounded_and_keeps_review_order():
    from l2shock.ui.shock_inspection import (
        ShockInspectionError,
        ShockInspectionModel,
    )

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review(
        (
            _hypothesis(2, 5, 10, scale="major", scan_range=scan_range),
            _hypothesis(3, 5, 8, scale="minor", scan_range=scan_range),
        )
    )
    model = ShockInspectionModel(review)

    page = model.page(1, page_size=1)

    assert page.review_id == review.review_id
    assert page.page == 1
    assert page.total_areas == len(review.ordered_areas)
    assert len(page.rows) == 1
    assert page.rows[0]["inspection_position"] == 1
    assert page.rows[0]["id"] == f"{review.review_id}:1"
    assert page.rows[0]["member_count"] == 2

    with pytest.raises(ShockInspectionError, match="page_size"):
        model.page(1, page_size=201)


def test_ui_default_order_is_v3_while_backend_default_stays_v2():
    import inspect
    from pathlib import Path

    import l2shock.ui.tab_shock_review as tab_module
    from l2shock.analysis.shock_review import (
        SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION,
        SHOCK_REVIEW_ORDER_LABELS,
        SHOCK_REVIEW_ORDER_VERSION,
        SHOCK_REVIEW_ORDER_VERSION_V3,
    )

    assert SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION == SHOCK_REVIEW_ORDER_VERSION_V3
    assert "default" in SHOCK_REVIEW_ORDER_LABELS[SHOCK_REVIEW_ORDER_VERSION_V3]
    assert (
        inspect.signature(review_shock_areas).parameters["order_version"].default
        == SHOCK_REVIEW_ORDER_VERSION
    )

    source = Path(tab_module.__file__).read_text(encoding="utf-8")
    assert "value=SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION" in source
    assert "SHOCK_REVIEW_ORDER_VERSION)" not in source
    assert (
        source.count("order_select.value or SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION") == 2
    )


def test_diagnostic_honours_requested_order_version():
    from l2shock.analysis.shock_diagnostic import build_shock_diagnostic
    from l2shock.analysis.shock_review import (
        SHOCK_REVIEW_ORDER_VERSION,
        SHOCK_REVIEW_ORDER_VERSION_V3,
    )

    scan_range = max(_TOTALS) - min(_TOTALS)
    scan = _candidate_scan(
        (_hypothesis(2, 5, 10, scale="major", scan_range=scan_range),)
    )

    backend_default = build_shock_diagnostic(scan)
    ui_default = build_shock_diagnostic(
        scan,
        order_version=SHOCK_REVIEW_ORDER_VERSION_V3,
    )

    assert backend_default.order_version == SHOCK_REVIEW_ORDER_VERSION
    assert ui_default.order_version == SHOCK_REVIEW_ORDER_VERSION_V3
    assert backend_default.review_id != ui_default.review_id
    assert (
        backend_default.evidence_result.candidate_scan.scan_id
        == ui_default.evidence_result.candidate_scan.scan_id
    )

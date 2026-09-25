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


def test_plot_window_retains_exact_one_second_b_area_and_all_members():
    from l2shock.analysis.shock_window import build_shock_area_window

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review(
        (
            _hypothesis(2, 5, 10, scale="major", scan_range=scan_range),
            _hypothesis(3, 5, 8, scale="minor", scan_range=scan_range),
        )
    )

    window = build_shock_area_window(
        review,
        1,
        padding_seconds=1,
    )
    payload = window.to_dict()

    assert window.first_dataset_index == 1
    assert window.last_dataset_index == 11
    assert len(window.seconds) == 11
    assert window.b_first_position == window.b_last_position == 4
    assert window.representative_b_position == 4
    assert len(window.member_abc_positions) == 2
    assert {item[2] for item in window.member_abc_positions} == {7, 9}

    for second in payload["seconds"]:
        assert second["valid_l2"] is True
        assert second["plot"]["total"] == second["plot"]["bid"]
        assert second["plot"]["ask"] == 0.0
        assert second["exact"]["delta"] == second["exact"]["bid"]

    # There is one axis slot per verified second, not one per chart bar.
    assert [item["dataset_index"] for item in payload["seconds"]] == list(range(1, 12))


def test_plot_window_preserves_invalid_l2_as_null_in_every_channel():
    from dataclasses import replace

    from l2shock.analysis.shock_window import build_shock_area_window
    from l2shock.ingest.sampling import (
        BookSampleInvalidReason,
        BookSampleQuality,
    )

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review((_hypothesis(2, 5, 10, scale="major", scan_range=scan_range),))

    # The reviewed A-B-C interval remains valid; only extra plot padding
    # becomes invalid. Construct the same dataset type with its existing
    # explicit identity fields rather than relying on an implicit loader.
    original = review.evidence_result.candidate_scan.dataset
    seconds = list(original.seconds)
    seconds[11] = L2Second(
        timestamp_utc=seconds[11].timestamp_utc,
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
        bid_liquidity=None,
        ask_liquidity=None,
        source_count=0,
    )

    altered_dataset = replace(original, seconds=tuple(seconds))
    altered_scan = replace(
        review.evidence_result.candidate_scan,
        dataset=altered_dataset,
    )
    altered_review = replace(
        review,
        evidence_result=replace(
            review.evidence_result,
            candidate_scan=altered_scan,
        ),
    )

    window = build_shock_area_window(
        altered_review,
        1,
        padding_seconds=1,
    )
    invalid = window.to_dict()["seconds"][-1]

    assert invalid["dataset_index"] == 11
    assert invalid["valid_l2"] is False
    assert invalid["plot"] == {
        "bid": None,
        "ask": None,
        "total": None,
        "delta": None,
    }
    assert all(value is None for value in invalid["exact"].values())


def test_plot_window_fails_instead_of_silently_downsampling_b():
    from l2shock.analysis.shock_window import (
        ShockWindowError,
        build_shock_area_window,
    )

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review((_hypothesis(2, 5, 10, scale="major", scan_range=scan_range),))

    with pytest.raises(ShockWindowError, match="exceeds maximum_seconds"):
        build_shock_area_window(
            review,
            1,
            padding_seconds=30,
            maximum_seconds=5,
        )

    with pytest.raises(ShockWindowError, match="inspection_position"):
        build_shock_area_window(review, 2)


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

    with pytest.raises(
        ShockInspectionError,
        match="inspection_position",
    ):
        model.select(len(review.ordered_areas) + 1)


def test_shock_inspection_selection_has_controller_temporal_metadata():
    import json

    from l2shock.ui.shock_inspection import ShockInspectionModel

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review((_hypothesis(2, 5, 10, scale="major", scan_range=scan_range),))

    selection = ShockInspectionModel(review).select(
        1,
        padding_seconds=1,
    )

    assert selection.owner_id == f"shock:{review.review_id}:1"

    metadata = selection.option["l2shockChartMetadata"]
    axis = selection.option["xAxis"][0]["data"]

    assert metadata["visible_bar_count"] == len(axis)
    assert metadata["bar_duration_seconds"] == 1
    assert metadata["visible_start_times_utc"] == axis
    assert metadata["source_bar_indices"] == list(
        range(
            selection.window.first_dataset_index,
            selection.window.last_dataset_index + 1,
        )
    )
    assert metadata["shock_review_id"] == review.review_id
    assert metadata["shock_inspection_position"] == 1
    assert selection.option["series"][0]["data"] == [None] * len(axis)

    # The existing publisher requires strictly JSON-serializable options.
    json.dumps(selection.option, allow_nan=False)


def test_total_b_and_c_have_distinct_marker_styles():
    from l2shock.ui.shock_inspection import ShockInspectionModel

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review((_hypothesis(2, 5, 10, scale="major", scan_range=scan_range),))

    selection = ShockInspectionModel(review).select(
        1,
        padding_seconds=1,
    )
    total = selection.option["series"][3]
    b_marker, c_marker = total["markLine"]["data"]

    assert b_marker["name"] == "Representative B"
    assert c_marker["name"] == "Representative C"
    assert b_marker["lineStyle"]["color"] != (c_marker["lineStyle"]["color"])

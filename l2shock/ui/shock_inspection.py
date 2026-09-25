# l2shock/ui/shock_inspection.py
"""Paged Shock-Start inspection and existing-controller publication bridge.

A completed ShockReview owns the rows. The selected B area determines which
bounded one-second L2 window to build. Nothing here runs a scan, reads price,
publishes a partial review, or ranks areas again.

The returned chart option carries the temporal metadata required by the
existing AnalysisChartController. The controller still owns render identity,
browser acknowledgement, crosshair installation, and image export.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from l2shock.analysis.shock_review import ShockReview
from l2shock.analysis.shock_window import (
    ShockAreaWindow,
    build_shock_area_window,
)
from l2shock.ui.chart_interactions import (
    AnalysisChartCommit,
    AnalysisChartController,
)
from l2shock.ui.shock_chart_options import (
    PriceBySecond,
    build_shock_chart_options,
)


class ShockInspectionError(ValueError):
    """Invalid inspection position, page, or review ownership."""


def _positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ShockInspectionError(f"{name} must be a positive integer")
    return value


def _fraction_text(value: Any) -> str:
    return f"{value.numerator}/{value.denominator}"


@dataclass(frozen=True, slots=True)
class ShockInspectionPage:
    """One bounded table page; positions remain global and one-based."""

    review_id: str
    page: int
    page_size: int
    total_areas: int
    rows: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class ShockInspectionSelection:
    """One controller-ready chart generation for one selected B area."""

    review_id: str
    inspection_position: int
    owner_id: str
    window: ShockAreaWindow
    option: dict[str, object]


class ShockInspectionModel:
    """Read-only UI projection of a completed ShockReview."""

    def __init__(self, review: ShockReview) -> None:
        if not isinstance(review, ShockReview):
            raise TypeError("review must be ShockReview")

        if not review.review_id:
            raise ShockInspectionError("Review has no identity")

        self.review = review

    def page(
        self,
        page: int,
        *,
        page_size: int = 30,
    ) -> ShockInspectionPage:
        number = _positive_int("page", page)
        size = _positive_int("page_size", page_size)

        if size > 200:
            raise ShockInspectionError("page_size cannot exceed 200")

        entries = self.review.ordered_areas
        start = (number - 1) * size
        end = min(start + size, len(entries))
        rows: list[dict[str, object]] = []

        # Do not serialize or create plotting windows for all 3,782 areas
        # merely because the user opened the first results-table page.
        for index in range(start, end):
            entry = entries[index]
            position = index + 1

            if entry.inspection_position != position:
                raise ShockInspectionError(
                    "Review inspection positions do not match their ordering"
                )

            area = entry.area
            representative = area.representative

            rows.append({
                # Suitable for the existing QTable row_key="id".
                "id": f"{self.review.review_id}:{position}",
                "inspection_position": position,
                "direction": area.direction,
                "first_b_utc": min(
                    member.b_utc for member in area.members
                ).isoformat(),
                "b_first_index": area.first_b_index,
                "b_last_index": area.last_b_index,
                "representative_kind": str(representative.kind),
                "representative_a_utc": representative.a_utc.isoformat(),
                "representative_b_utc": representative.b_utc.isoformat(),
                "representative_c_utc": representative.c_utc.isoformat(),
                "member_count": len(area.members),
                "scale_names": ", ".join(area.scale_names),
                "independent_channel_count": (
                    area.independent_channel_count
                ),
                "highest_scale_fraction": _fraction_text(
                    entry.highest_scale_fraction
                ),
                "total_bc_fraction_of_scan_range": _fraction_text(
                    entry.total_bc_fraction_of_scan_range
                ),
            })

        return ShockInspectionPage(
            review_id=self.review.review_id,
            page=number,
            page_size=size,
            total_areas=len(entries),
            rows=tuple(rows),
        )

    def select(
        self,
        inspection_position: int,
        *,
        padding_seconds: int = 30,
        maximum_seconds: int = 3_600,
        price_by_second: PriceBySecond | None = None,
    ) -> ShockInspectionSelection:
        position = _positive_int(
            "inspection_position",
            inspection_position,
        )

        if position > len(self.review.ordered_areas):
            raise ShockInspectionError(
                "inspection_position does not identify a reviewed B area"
            )

        window = build_shock_area_window(
            self.review,
            position,
            padding_seconds=padding_seconds,
            maximum_seconds=maximum_seconds,
        )
        option = build_shock_chart_options(
            window,
            price_by_second=price_by_second,
        )

        timestamps = [
            second.timestamp_utc.isoformat()
            for second in window.seconds
        ]
        count = len(timestamps)

        # These are the existing chart-controller temporal/ownership fields.
        # Invalid L2 or missing price seconds still occupy their own slots.
        option["l2shockChartMetadata"] = {
            "analysis_id": self.review.review_id,
            "dataset_analysis_id": (
                self.review.evidence_result.candidate_scan.scan_id
            ),
            "chart_timeframe": "1s",
            "activity_timeframe": "1s",
            "visible_bar_count": count,
            "visible_start_times_utc": timestamps,
            "bar_duration_seconds": 1,
            "source_bar_indices": list(
                range(
                    window.first_dataset_index,
                    window.last_dataset_index + 1,
                )
            ),
            "discontinuities": {},
            "shock_review_id": self.review.review_id,
            "shock_inspection_position": position,
        }

        # An old selected area's export/navigation must not own the newly
        # selected area, even when both came from the same completed review.
        owner_id = f"shock:{self.review.review_id}:{position}"

        return ShockInspectionSelection(
            review_id=self.review.review_id,
            inspection_position=position,
            owner_id=owner_id,
            window=window,
            option=option,
        )


async def publish_shock_selection(
    controller: AnalysisChartController,
    selection: ShockInspectionSelection,
) -> AnalysisChartCommit | None:
    """Publish through the existing token-acknowledged chart controller.

    The caller must still guard against a *newer UI selection* superseding
    this one, just as the existing Analysis tab guards runtime-result
    ownership after browser acknowledgement.
    """
    if not isinstance(controller, AnalysisChartController):
        raise TypeError("controller must be AnalysisChartController")

    if not isinstance(selection, ShockInspectionSelection):
        raise TypeError("selection must be ShockInspectionSelection")

    return await controller.publish(
        selection.option,
        owner_id=selection.owner_id,
        preserve_viewport=False,
    )


__all__ = [
    "ShockInspectionError",
    "ShockInspectionModel",
    "ShockInspectionPage",
    "ShockInspectionSelection",
    "publish_shock_selection",
]
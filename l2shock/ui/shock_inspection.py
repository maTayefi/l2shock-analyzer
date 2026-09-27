# l2shock/ui/shock_inspection.py
"""Paged Shock-Start inspection rows for the review results table.

A completed ShockReview owns the rows. Nothing here runs a scan, reads
price, publishes a chart, or ranks areas again. A row click opens its B area
through the bounded L2 viewport (shock_view_selection); the former
one-second selection path (ShockInspectionModel.select) was retired in
Batch 45A.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from l2shock.analysis.shock_review import ShockReview


class ShockInspectionError(ValueError):
    """Invalid inspection page or review ownership."""


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


class ShockInspectionModel:
    """Read-only table projection of a completed ShockReview."""

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

        # Serialize only the requested page, never every reviewed area.
        for index in range(start, end):
            entry = entries[index]
            position = index + 1

            if entry.inspection_position != position:
                raise ShockInspectionError(
                    "Review inspection positions do not match their ordering"
                )

            area = entry.area
            representative = area.representative

            rows.append(
                {
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
                    "independent_channel_count": (area.independent_channel_count),
                    "highest_scale_fraction": _fraction_text(
                        entry.highest_scale_fraction
                    ),
                    "total_bc_fraction_of_scan_range": _fraction_text(
                        entry.total_bc_fraction_of_scan_range
                    ),
                    "total_bc_sharpness": f"{entry.total_bc_sharpness:.4g}",
                    "total_bc_adverse_total_fraction": _fraction_text(
                        entry.total_bc_adverse_total_fraction
                    ),
                    "total_c_extremeness": _fraction_text(entry.total_c_extremeness),
                }
            )

        return ShockInspectionPage(
            review_id=self.review.review_id,
            page=number,
            page_size=size,
            total_areas=len(entries),
            rows=tuple(rows),
        )


__all__ = [
    "ShockInspectionError",
    "ShockInspectionModel",
    "ShockInspectionPage",
]

# l2shock/ui/shock_view_selection.py
"""Prepare one owner-checked Shock chart viewport around a selected B area.

This is presentation plumbing. Detection still uses the completed
review's original verified one-second dataset.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from l2shock.analysis.shock_review import ShockReview
from l2shock.ui.shock_dataset_view import (
    build_shock_dataset_view,
)
from l2shock.ui.shock_view_bars import ShockViewBarsError
from l2shock.ui.shock_view_chart_options import (
    build_shock_view_chart_options,
)


@dataclass(frozen=True, slots=True)
class ShockViewportSelection:
    review_id: str
    inspection_position: int
    owner_id: str
    source_seconds: int
    displayed_bars: int
    timeframe_seconds: int
    option: dict[str, Any]
    source_start_utc: datetime
    source_end_utc_exclusive: datetime
    bar_starts_utc: tuple[datetime, ...]


def _whole_number(
    value: object,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool):
        raise ShockViewBarsError(
            f"{name} must be a whole number between " f"{minimum} and {maximum}"
        )

    if isinstance(value, int):
        number = value
    elif isinstance(value, float) and value.is_integer():
        # NiceGUI number controls may deliver integral values as floats.
        number = int(value)
    else:
        raise ShockViewBarsError(
            f"{name} must be a whole number between " f"{minimum} and {maximum}"
        )

    if not minimum <= number <= maximum:
        raise ShockViewBarsError(f"{name} must be between {minimum} and {maximum}")

    return number


def build_shock_view_selection(
    review: ShockReview,
    inspection_position: int,
    *,
    source_seconds: int = 3_600,
    seconds_before_b: int = 1_800,
    timeframe_seconds: int | None = None,
    max_bars: int = 1_200,
    price_candles: Sequence[Sequence[float] | None] | None = None,
) -> ShockViewportSelection:
    """Build a bounded viewport anchored to an existing reviewed B area."""
    position = _whole_number(
        inspection_position,
        "inspection_position",
        minimum=1,
        maximum=len(review.ordered_areas),
    )
    requested_seconds = _whole_number(
        source_seconds,
        "source_seconds",
        minimum=1,
        maximum=86_400,
    )
    before_b = _whole_number(
        seconds_before_b,
        "seconds_before_b",
        minimum=0,
        maximum=86_399,
    )
    budget = _whole_number(
        max_bars,
        "max_bars",
        minimum=1,
        maximum=5_000,
    )

    entry = review.ordered_areas[position - 1]

    if entry.inspection_position != position:
        raise ShockViewBarsError(
            "Review inspection positions do not match their ordering"
        )

    area = entry.area
    dataset = review.evidence_result.candidate_scan.dataset
    dataset_size = len(dataset.seconds)

    if not 0 <= area.first_b_index < dataset_size:
        raise ShockViewBarsError("Selected B area begins outside its dataset")

    first = max(
        0,
        area.first_b_index - before_b,
    )
    last_exclusive = min(
        dataset_size,
        first + requested_seconds,
    )

    projection = build_shock_dataset_view(
        dataset,
        first_dataset_index=first,
        last_dataset_index_exclusive=last_exclusive,
        timeframe_seconds=timeframe_seconds,
        max_bars=budget,
        max_source_seconds=86_400,
    )

    option = build_shock_view_chart_options(
        projection,
        b_first_dataset_index=area.first_b_index,
        b_last_dataset_index=area.last_b_index,
        representative_b_dataset_index=(area.representative.b_index),
        representative_c_dataset_index=(area.representative.c_index),
        price_candles=price_candles,
    )

    # Keep the chart's established publication/ownership contract.
    # Do not use this category-oriented metadata to restore a time-axis
    # viewport until the controller has explicit time-axis support.
    option["l2shockChartMetadata"] = {
        "analysis_id": review.review_id,
        "dataset_analysis_id": (review.evidence_result.candidate_scan.scan_id),
        "chart_timeframe": (f"{projection.timeframe_seconds}s"),
        "activity_timeframe": "1s",
        "visible_bar_count": len(projection.bars),
        "visible_start_times_utc": [
            bar.start_utc.isoformat() for bar in projection.bars
        ],
        "bar_duration_seconds": (projection.timeframe_seconds),
        "source_bar_indices": [bar.first_dataset_index for bar in projection.bars],
        "discontinuities": {},
        "shock_review_id": review.review_id,
        "shock_inspection_position": position,
    }

    return ShockViewportSelection(
        review_id=review.review_id,
        inspection_position=position,
        owner_id=(f"shock:{review.review_id}:{position}"),
        source_seconds=(last_exclusive - first),
        displayed_bars=len(projection.bars),
        timeframe_seconds=(projection.timeframe_seconds),
        option=option,
        source_start_utc=projection.source_start_utc,
        source_end_utc_exclusive=projection.source_end_utc_exclusive,
        bar_starts_utc=tuple(bar.start_utc for bar in projection.bars),
    )

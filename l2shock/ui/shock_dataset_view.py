# l2shock/ui/shock_dataset_view.py
"""Project an explicit verified-L2 dataset viewport into viewing bars.

The source interval uses absolute, zero-based dataset indices and is
half-open: [first_dataset_index, last_dataset_index_exclusive).

This adapter does not build a ShockAreaWindow, change detection
coordinates, fetch price, or silently shorten the requested interval.
"""

from __future__ import annotations

from datetime import timedelta
from fractions import Fraction
from typing import TYPE_CHECKING

from l2shock.analysis.shock_window import ShockWindowSecond
from l2shock.ingest.sampling import BookSampleQuality
from l2shock.ui.shock_view_bars import (
    ShockViewBarsError,
    ShockViewProjection,
    build_shock_view_bars,
)

if TYPE_CHECKING:
    from l2shock.analysis.shock_dataset import (
        VerifiedShockDataset,
    )


def _source_limit(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShockViewBarsError("max_source_seconds must be a positive integer")

    if not 1 <= value <= 604_800:
        raise ShockViewBarsError("max_source_seconds must be between 1 and 604,800")

    return value


def _dataset_position(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShockViewBarsError(f"{name} must be an integer dataset index")

    return value


def build_shock_dataset_view(
    dataset: VerifiedShockDataset,
    *,
    first_dataset_index: int,
    last_dataset_index_exclusive: int,
    timeframe_seconds: int | None = None,
    max_bars: int = 1_200,
    max_source_seconds: int = 86_400,
) -> ShockViewProjection:
    """Build bounded viewing bars from exactly the requested L2 seconds.

    The verified dataset owns the timestamps and absolute indices.
    A valid slot needs both liquidity channels; an invalid slot remains
    null in all four channels. Degraded market coverage is not plotted
    as complete L2, matching the existing inspection-window policy.
    """
    first = _dataset_position(
        first_dataset_index,
        "first_dataset_index",
    )
    last_exclusive = _dataset_position(
        last_dataset_index_exclusive,
        "last_dataset_index_exclusive",
    )
    source_limit = _source_limit(max_source_seconds)

    observations = dataset.seconds

    if not 0 <= first < last_exclusive <= len(observations):
        raise ShockViewBarsError(
            "Requested dataset viewport must be a nonempty "
            "in-bounds half-open interval"
        )

    count = last_exclusive - first

    if count > source_limit:
        raise ShockViewBarsError(
            "Requested dataset viewport exceeds max_source_seconds; "
            "choose a shorter viewport"
        )

    origin = observations[first].timestamp_utc
    plotted: list[ShockWindowSecond] = []

    for index in range(first, last_exclusive):
        observation = observations[index]

        if observation.timestamp_utc != (origin + timedelta(seconds=index - first)):
            raise ShockViewBarsError(
                "Dataset viewport must own every one-second UTC slot"
            )

        if observation.coverage_degraded:
            raise ShockViewBarsError(
                "Partial-market L2 coverage cannot be plotted " "as complete"
            )

        if observation.quality is BookSampleQuality.VALID:
            if observation.bid_liquidity is None or observation.ask_liquidity is None:
                raise ShockViewBarsError("VALID L2 second has no Bid or Ask liquidity")

            bid = Fraction(observation.bid_liquidity)
            ask = Fraction(observation.ask_liquidity)
            total = bid + ask
            delta = bid - ask
            valid_l2 = True
        else:
            bid = ask = total = delta = None
            valid_l2 = False

        plotted.append(
            ShockWindowSecond(
                dataset_index=index,
                timestamp_utc=observation.timestamp_utc,
                valid_l2=valid_l2,
                bid=bid,
                ask=ask,
                total=total,
                delta=delta,
            )
        )

    return build_shock_view_bars(
        plotted,
        timeframe_seconds=timeframe_seconds,
        max_bars=max_bars,
    )

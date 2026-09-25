# tests/test_shock_dataset_view.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from l2shock.ingest.sampling import BookSampleQuality
from l2shock.ui.shock_dataset_view import (
    build_shock_dataset_view,
)
from l2shock.ui.shock_view_bars import ShockViewBarsError

_ORIGIN = datetime(2026, 9, 24, 2, 6, 58, tzinfo=timezone.utc)


def _dataset(
    count: int,
    *,
    invalid: frozenset[int] = frozenset(),
    degraded: frozenset[int] = frozenset(),
):
    seconds = []

    for index in range(count):
        is_valid = index not in invalid

        seconds.append(
            SimpleNamespace(
                timestamp_utc=(_ORIGIN + timedelta(seconds=index)),
                quality=(
                    BookSampleQuality.VALID if is_valid else BookSampleQuality.INVALID
                ),
                coverage_degraded=index in degraded,
                bid_liquidity=(10 + index if is_valid else None),
                ask_liquidity=(30 - index if is_valid else None),
            )
        )

    return SimpleNamespace(seconds=tuple(seconds))


def test_slice_preserves_absolute_dataset_indices() -> None:
    projection = build_shock_dataset_view(
        _dataset(12),
        first_dataset_index=3,
        last_dataset_index_exclusive=10,
        timeframe_seconds=5,
        max_bars=3,
    )

    assert projection.source_start_utc == (_ORIGIN + timedelta(seconds=3))
    assert projection.source_end_utc_exclusive == (_ORIGIN + timedelta(seconds=10))

    assert projection.bars[0].first_dataset_index == 3
    assert projection.bars[-1].last_dataset_index == 9
    assert (
        sum(
            bar.last_dataset_index - bar.first_dataset_index + 1
            for bar in projection.bars
        )
        == 7
    )


def test_invalid_second_stays_a_gap_after_dataset_conversion() -> None:
    projection = build_shock_dataset_view(
        _dataset(5, invalid=frozenset({1})),
        first_dataset_index=0,
        last_dataset_index_exclusive=5,
        timeframe_seconds=5,
        max_bars=2,
    )

    first, second = projection.bars

    assert first.valid_l2 is False
    assert (
        first.bid,
        first.ask,
        first.total,
        first.delta,
    ) == (None, None, None, None)

    assert second.valid_l2 is True
    assert second.bid is not None


def test_one_second_view_keeps_bid_ask_total_and_delta() -> None:
    projection = build_shock_dataset_view(
        _dataset(5),
        first_dataset_index=2,
        last_dataset_index_exclusive=4,
        timeframe_seconds=1,
        max_bars=2,
    )

    first = projection.bars[0]

    assert first.first_dataset_index == 2
    assert first.bid == (12.0, 12.0, 12.0, 12.0)
    assert first.ask == (28.0, 28.0, 28.0, 28.0)
    assert first.total == (40.0, 40.0, 40.0, 40.0)
    assert first.delta == (-16.0, -16.0, -16.0, -16.0)


def test_degraded_coverage_fails_even_if_liquidity_is_present() -> None:
    with pytest.raises(
        ShockViewBarsError,
        match="Partial-market L2 coverage",
    ):
        build_shock_dataset_view(
            _dataset(5, degraded=frozenset({2})),
            first_dataset_index=0,
            last_dataset_index_exclusive=5,
        )


def test_source_limit_rejects_instead_of_truncating() -> None:
    with pytest.raises(
        ShockViewBarsError,
        match="exceeds max_source_seconds",
    ):
        build_shock_dataset_view(
            _dataset(12),
            first_dataset_index=0,
            last_dataset_index_exclusive=12,
            max_source_seconds=10,
        )


@pytest.mark.parametrize(
    ("first", "last"),
    [
        (-1, 2),
        (0, 0),
        (3, 2),
        (0, 6),
        (True, 2),
        (0, 2.5),
    ],
)
def test_invalid_viewports_are_rejected(
    first: object,
    last: object,
) -> None:
    with pytest.raises(
        ShockViewBarsError,
        match="dataset",
    ):
        build_shock_dataset_view(
            _dataset(5),
            first_dataset_index=first,
            last_dataset_index_exclusive=last,
        )


def test_timestamp_gap_fails_before_projection() -> None:
    dataset = _dataset(5)
    seconds = list(dataset.seconds)
    seconds[3].timestamp_utc += timedelta(seconds=1)

    with pytest.raises(
        ShockViewBarsError,
        match="every one-second UTC slot",
    ):
        build_shock_dataset_view(
            dataset,
            first_dataset_index=0,
            last_dataset_index_exclusive=5,
        )


def test_requested_viewport_can_span_more_than_inspection_window() -> None:
    dataset = _dataset(3_601)

    projection = build_shock_dataset_view(
        dataset,
        first_dataset_index=0,
        last_dataset_index_exclusive=3_601,
        timeframe_seconds=60,
        max_bars=62,
    )

    assert projection.bars[0].first_dataset_index == 0
    assert projection.bars[-1].last_dataset_index == 3_600
    assert len(projection.bars) <= 62

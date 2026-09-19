from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext

import pytest

from l2shock.analysis import (
    LiquidityMetric,
    LiquidityMovementCandidate,
    LiquidityMovementDirection,
    LiquidityMovementPopulationKey,
    LiquidityMovementRankingConfig,
    LiquidityMovementRankingError,
    LiquidityMovementScanBounds,
    percentile_ranks,
    positive_tail_modified_z_scores,
    rank_liquidity_movements,
)


def _time(index: int) -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(seconds=index)


def _candidate(
    *,
    start_index: int,
    end_index: int,
    start_value: str,
    end_value: str,
    direction: LiquidityMovementDirection = LiquidityMovementDirection.UP,
    metric: LiquidityMetric = LiquidityMetric.ASK_LIQUIDITY,
    timeframe_label: str = "1s",
    relative_height: str = "0.5",
    sharpness: str = "0.25",
    adverse_count: int = 0,
    adverse_total: str = "0",
    terminal_offline: bool = True,
) -> LiquidityMovementCandidate:
    start = Decimal(start_value)
    end = Decimal(end_value)
    height = abs(end - start)
    adverse = Decimal(adverse_total)

    return LiquidityMovementCandidate(
        metric=metric,
        timeframe_label=timeframe_label,
        direction=direction,
        segment_index=0,
        start_index=start_index,
        end_index=end_index,
        confirmation_index=None if terminal_offline else end_index + 1,
        in_range_start_index=start_index,
        in_range_end_index=end_index,
        start_time_utc=_time(start_index),
        end_time_utc=_time(end_index),
        confirmation_time_utc=(None if terminal_offline else _time(end_index + 1)),
        in_range_start_time_utc=_time(start_index),
        in_range_end_time_utc=_time(end_index),
        start_value=start,
        end_value=end,
        absolute_height=height,
        relative_height=Decimal(relative_height),
        bars=end_index - start_index + 1,
        sharpness=Decimal(sharpness),
        adverse_move_count=adverse_count,
        adverse_move_total=adverse,
        adverse_move_maximum=adverse,
        adverse_move_total_fraction=adverse / height,
        adverse_move_maximum_fraction=adverse / height,
        confirmation_retracement=(
            None if terminal_offline else height * Decimal("0.20")
        ),
        confirmation_retracement_fraction=(
            None if terminal_offline else Decimal("0.20")
        ),
        degraded_bar_count=0,
        terminal_offline=terminal_offline,
    )


def _bounds(
    *,
    metric: LiquidityMetric = LiquidityMetric.ASK_LIQUIDITY,
    timeframe_label: str = "1s",
) -> LiquidityMovementScanBounds:
    return LiquidityMovementScanBounds(
        metric=metric,
        timeframe_label=timeframe_label,
        minimum=Decimal("0"),
        maximum=Decimal("100"),
    )


def test_percentile_ranks_use_midpoint_ties() -> None:
    observed = percentile_ranks(
        (
            Decimal("10"),
            Decimal("20"),
            Decimal("20"),
            Decimal("40"),
        )
    )

    assert observed == (
        Decimal("0"),
        Decimal("0.5"),
        Decimal("0.5"),
        Decimal("1"),
    )


def test_singleton_percentile_is_one() -> None:
    assert percentile_ranks((Decimal("10"),)) == (Decimal("1"),)


def test_percentile_ranks_preserve_input_order_with_repeated_values() -> None:
    observed = percentile_ranks(
        (
            Decimal("40"),
            Decimal("10"),
            Decimal("20"),
            Decimal("20"),
        )
    )

    assert observed == (
        Decimal("1"),
        Decimal("0"),
        Decimal("0.5"),
        Decimal("0.5"),
    )


def test_large_percentile_population_has_exact_endpoints() -> None:
    values = tuple(Decimal(index) for index in range(10_000))

    observed = percentile_ranks(values)

    assert len(observed) == len(values)
    assert observed[0] == Decimal("0")
    assert observed[-1] == Decimal("1")
    assert all(
        earlier < later
        for earlier, later in zip(
            observed,
            observed[1:],
            strict=False,
        )
    )


def test_positive_tail_modified_z_ignores_negative_tail() -> None:
    observed = positive_tail_modified_z_scores(
        (
            Decimal("1"),
            Decimal("2"),
            Decimal("3"),
        )
    )

    assert observed[0] == (Decimal("0"), Decimal("0"))
    assert observed[1] == (Decimal("0"), Decimal("0"))
    assert observed[2][0] > 0
    assert Decimal("0") < observed[2][1] < Decimal("1")


def test_zero_mad_outlier_receives_full_normalized_tail_evidence() -> None:
    observed = positive_tail_modified_z_scores(
        (
            Decimal("1"),
            Decimal("1"),
            Decimal("1"),
            Decimal("10"),
        )
    )

    assert observed[:3] == (
        (Decimal("0"), Decimal("0")),
        (Decimal("0"), Decimal("0")),
        (Decimal("0"), Decimal("0")),
    )
    assert observed[3] == (Decimal("0"), Decimal("1"))


def test_population_identity_includes_direction() -> None:
    upward = _candidate(
        start_index=0,
        end_index=1,
        start_value="10",
        end_value="20",
    )
    downward = _candidate(
        start_index=2,
        end_index=3,
        start_value="20",
        end_value="10",
        direction=LiquidityMovementDirection.DOWN,
    )

    batch = rank_liquidity_movements(
        (upward, downward),
        scan_bounds=(_bounds(),),
    )

    assert {ranking.population_key for ranking in batch.rankings} == {
        LiquidityMovementPopulationKey(
            metric=LiquidityMetric.ASK_LIQUIDITY,
            timeframe_label="1s",
            direction=LiquidityMovementDirection.UP,
        ),
        LiquidityMovementPopulationKey(
            metric=LiquidityMetric.ASK_LIQUIDITY,
            timeframe_label="1s",
            direction=LiquidityMovementDirection.DOWN,
        ),
    }


def test_start_and_end_extremeness_are_directional_and_equally_weighted() -> None:
    upward = _candidate(
        start_index=0,
        end_index=1,
        start_value="10",
        end_value="90",
        relative_height="0.8",
        sharpness="0.6",
    )
    downward = _candidate(
        start_index=2,
        end_index=3,
        start_value="90",
        end_value="10",
        direction=LiquidityMovementDirection.DOWN,
        relative_height="0.8",
        sharpness="0.6",
    )

    batch = rank_liquidity_movements(
        (upward, downward),
        scan_bounds=(_bounds(),),
    )

    assert len(batch.rankings) == 2

    for ranking in batch.rankings:
        assert ranking.start_extremeness == Decimal("0.9")
        assert ranking.end_extremeness == Decimal("0.9")
        assert ranking.boundary_extremeness == Decimal("0.9")


def test_recall_guard_is_union_of_height_and_sharpness() -> None:
    tallest = _candidate(
        start_index=0,
        end_index=4,
        start_value="10",
        end_value="90",
        relative_height="0.8",
        sharpness="0.2",
    )
    sharpest = _candidate(
        start_index=10,
        end_index=11,
        start_value="40",
        end_value="60",
        relative_height="0.2",
        sharpness="0.9",
    )
    middle = _candidate(
        start_index=20,
        end_index=22,
        start_value="30",
        end_value="70",
        relative_height="0.4",
        sharpness="0.4",
    )

    batch = rank_liquidity_movements(
        (tallest, sharpest, middle),
        scan_bounds=(_bounds(),),
        config=LiquidityMovementRankingConfig(
            top_n_height=1,
            top_n_sharpness=1,
        ),
    )

    selected = batch.selected

    assert len(selected) == 2
    assert {ranking.candidate.start_index for ranking in selected} == {0, 10}

    by_start = {ranking.candidate.start_index: ranking for ranking in batch.rankings}

    assert by_start[0].selected_by_height is True
    assert by_start[10].selected_by_sharpness is True
    assert by_start[20].selected is False
    assert by_start[20].final_rank is None


def test_cleaner_candidate_wins_secondary_tie() -> None:
    clean = _candidate(
        start_index=0,
        end_index=4,
        start_value="10",
        end_value="60",
        relative_height="0.5",
        sharpness="0.25",
    )
    noisy = _candidate(
        start_index=10,
        end_index=14,
        start_value="10",
        end_value="60",
        relative_height="0.5",
        sharpness="0.25",
        adverse_count=2,
        adverse_total="10",
    )

    batch = rank_liquidity_movements(
        (noisy, clean),
        scan_bounds=(_bounds(),),
    )

    assert batch.rankings[0].candidate == clean
    assert batch.rankings[0].primary_evidence == batch.rankings[1].primary_evidence
    assert batch.rankings[0].secondary_evidence > batch.rankings[1].secondary_evidence


def test_rankings_are_independent_by_metric() -> None:
    ask = _candidate(
        start_index=0,
        end_index=1,
        start_value="10",
        end_value="20",
        metric=LiquidityMetric.ASK_LIQUIDITY,
    )
    bid = _candidate(
        start_index=2,
        end_index=3,
        start_value="20",
        end_value="30",
        metric=LiquidityMetric.BID_LIQUIDITY,
    )

    batch = rank_liquidity_movements(
        (ask, bid),
        scan_bounds=(
            _bounds(metric=LiquidityMetric.ASK_LIQUIDITY),
            _bounds(metric=LiquidityMetric.BID_LIQUIDITY),
        ),
    )

    assert len(batch.rankings) == 2
    assert all(ranking.population_rank == 1 for ranking in batch.rankings)


def test_missing_scan_bounds_are_rejected() -> None:
    candidate = _candidate(
        start_index=0,
        end_index=1,
        start_value="10",
        end_value="20",
    )

    with pytest.raises(
        LiquidityMovementRankingError,
        match="Missing scan bounds",
    ):
        rank_liquidity_movements(
            (candidate,),
            scan_bounds=(),
        )


def test_candidate_outside_scan_bounds_is_rejected() -> None:
    candidate = _candidate(
        start_index=0,
        end_index=1,
        start_value="90",
        end_value="110",
    )

    with pytest.raises(
        LiquidityMovementRankingError,
        match="outside supplied scan bounds",
    ):
        rank_liquidity_movements(
            (candidate,),
            scan_bounds=(_bounds(),),
        )


def test_duplicate_candidate_identity_is_rejected() -> None:
    candidate = _candidate(
        start_index=0,
        end_index=1,
        start_value="10",
        end_value="20",
    )

    with pytest.raises(
        LiquidityMovementRankingError,
        match="duplicate candidate",
    ):
        rank_liquidity_movements(
            (candidate, replace(candidate)),
            scan_bounds=(_bounds(),),
        )


def test_priorities_must_sum_exactly_to_one() -> None:
    with pytest.raises(
        LiquidityMovementRankingError,
        match="sum exactly to 1",
    ):
        LiquidityMovementRankingConfig(
            priority_height=Decimal("0.50"),
        )


def test_boundary_extremeness_is_equal_weight_mean() -> None:
    candidate = _candidate(
        start_index=0,
        end_index=1,
        start_value="40",
        end_value="90",
        relative_height="0.5",
        sharpness="0.4",
    )

    batch = rank_liquidity_movements(
        (candidate,),
        scan_bounds=(_bounds(),),
    )

    ranking = batch.rankings[0]

    assert ranking.start_extremeness == Decimal("0.6")
    assert ranking.end_extremeness == Decimal("0.9")
    assert ranking.boundary_extremeness == Decimal("0.75")


def test_start_extremeness_affects_secondary_ranking_equally_with_end() -> None:
    better_start = _candidate(
        start_index=0,
        end_index=1,
        start_value="10",
        end_value="80",
        relative_height="0.7",
        sharpness="0.5",
    )
    better_end = _candidate(
        start_index=2,
        end_index=3,
        start_value="20",
        end_value="90",
        relative_height="0.7",
        sharpness="0.5",
    )

    batch = rank_liquidity_movements(
        (
            better_start,
            better_end,
        ),
        scan_bounds=(_bounds(),),
    )

    by_start_index = {
        ranking.candidate.start_index: ranking for ranking in batch.rankings
    }

    first = by_start_index[0]
    second = by_start_index[2]

    assert first.start_extremeness == Decimal("0.9")
    assert first.end_extremeness == Decimal("0.8")
    assert first.boundary_extremeness == Decimal("0.85")

    assert second.start_extremeness == Decimal("0.8")
    assert second.end_extremeness == Decimal("0.9")
    assert second.boundary_extremeness == Decimal("0.85")

    assert first.secondary_evidence == second.secondary_evidence
    assert first.priority_weighted_evidence == second.priority_weighted_evidence


def test_boundary_extremeness_is_independent_of_ambient_decimal_context() -> None:
    candidate = _candidate(
        start_index=0,
        end_index=1,
        start_value="33.333333333333333333",
        end_value="88.888888888888888888",
        relative_height="0.55555555555555555555",
        sharpness="0.4",
    )

    config = LiquidityMovementRankingConfig(
        decimal_precision=50,
    )

    expected = rank_liquidity_movements(
        (candidate,),
        scan_bounds=(_bounds(),),
        config=config,
    )

    with localcontext(Context(prec=6)):
        observed = rank_liquidity_movements(
            (candidate,),
            scan_bounds=(_bounds(),),
            config=config,
        )

    assert observed == expected
    ranking = observed.rankings[0]
    assert ranking.boundary_extremeness * 2 == (
        ranking.start_extremeness + ranking.end_extremeness
    )


def test_modified_z_median_and_mad_ignore_ambient_context() -> None:
    values = (
        Decimal("1.0000000000000000000000000000000000000000000000001"),
        Decimal("2.0000000000000000000000000000000000000000000000002"),
        Decimal("3.0000000000000000000000000000000000000000000000003"),
        Decimal("9.0000000000000000000000000000000000000000000000009"),
    )

    expected = positive_tail_modified_z_scores(
        values,
        decimal_precision=50,
    )

    with localcontext(
        Context(
            prec=6,
        )
    ):
        observed = positive_tail_modified_z_scores(
            values,
            decimal_precision=50,
        )

    assert observed == expected


def test_percentile_ranks_use_requested_precision() -> None:
    values = tuple(
        Decimal(index)
        for index in range(4)
    )

    observed = percentile_ranks(
        values,
        decimal_precision=50,
    )

    with localcontext(
        Context(
            prec=50,
        )
    ):
        expected_one_third = Decimal(1) / Decimal(3)
        expected_two_thirds = Decimal(2) / Decimal(3)

    assert observed == (
        Decimal(0),
        expected_one_third,
        expected_two_thirds,
        Decimal(1),
    )
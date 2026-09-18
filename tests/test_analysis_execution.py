from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.analysis import (
    AnalysisDatasetRequest,
    LiquidityMetric,
    LiquidityMovementAnalysisCache,
    LiquidityMovementAnalysisCancelledError,
    LiquidityMovementAnalysisConfig,
    LiquidityMovementAnalysisProgressPhase,
    LiquidityMovementDetectionConfig,
    LiquidityMovementRankingConfig,
    PriceBounds,
    analysis_config_from_lm_config,
    build_aligned_analysis_dataset,
    execute_liquidity_movement_analysis,
    liquidity_movement_analysis_id,
)
from l2shock.analysis import L2Second, PriceSecond
from l2shock.config import LMConfig
from l2shock.ingest import BookSampleQuality
from l2shock.price import TradeSampleQuality


def _start() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _l2(
    index: int,
    bid: str,
    ask: str,
) -> L2Second:
    return L2Second(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(bid),
        ask_liquidity=Decimal(ask),
        source_count=1,
    )


def _price(index: int) -> PriceSecond:
    return PriceSecond(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=TradeSampleQuality.VALID,
        invalid_reason=None,
        open=Decimal("100"),
        high=Decimal("105"),
        low=Decimal("95"),
        close=Decimal("101"),
        trade_count=1,
    )


def _dataset(
    bid_values: tuple[str, ...],
    *,
    ask_values: tuple[str, ...] | None = None,
    maximum_chart_bars: int = 400,
):
    asks = ask_values or tuple(str(200 + index) for index in range(len(bid_values)))

    request = AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=(_start() + timedelta(seconds=len(bid_values) - 1)),
        activity_timeframe="1s",
        maximum_chart_bars=maximum_chart_bars,
        price_bounds=PriceBounds(
            min_price=90,
            max_price=110,
        ),
        context_before=0,
        context_after=0,
    )

    return build_aligned_analysis_dataset(
        request,
        l2_seconds=tuple(
            _l2(
                index,
                bid,
                asks[index],
            )
            for index, bid in enumerate(bid_values)
        ),
        price_seconds=tuple(_price(index) for index in range(len(bid_values))),
    )


def _bid_only_config() -> LiquidityMovementAnalysisConfig:
    return LiquidityMovementAnalysisConfig(
        detection=LiquidityMovementDetectionConfig(
            confirmation_retracement_fraction=Decimal("0.20"),
        ),
        ranking=LiquidityMovementRankingConfig(
            top_n_height=10,
            top_n_sharpness=10,
        ),
        metrics=(LiquidityMetric.BID_LIQUIDITY,),
    )


def test_execution_detects_and_ranks_one_unique_timeframe() -> None:
    dataset = _dataset(
        (
            "100",
            "120",
            "116",
            "110",
            "90",
            "94",
        )
    )

    result = execute_liquidity_movement_analysis(
        dataset,
        config=_bid_only_config(),
    )

    assert result.timeframe_labels == ("1s",)
    assert len(result.slices) == 1
    assert result.slices[0].key.metric is (LiquidityMetric.BID_LIQUIDITY)
    assert result.slices[0].key.timeframe_label == "1s"

    assert result.candidates
    assert result.ranking_batch.rankings
    assert result.selected

    assert all(
        ranking.candidate.metric is LiquidityMetric.BID_LIQUIDITY
        for ranking in result.ranking_batch.rankings
    )


def test_execution_runs_activity_and_distinct_chart_timeframes() -> None:
    values = tuple(
        str(100 + (index if index < 15 else 30 - index)) for index in range(25)
    )

    dataset = _dataset(
        values,
        maximum_chart_bars=5,
    )

    result = execute_liquidity_movement_analysis(
        dataset,
        config=_bid_only_config(),
    )

    assert result.timeframe_labels == (
        "1s",
        "5s",
    )
    assert [item.key.timeframe_label for item in result.slices] == [
        "1s",
        "5s",
    ]


def test_same_timeframe_is_not_executed_twice() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        ),
        maximum_chart_bars=400,
    )

    assert dataset.activity.timeframe.label == "1s"
    assert dataset.chart.timeframe.label == "1s"

    result = execute_liquidity_movement_analysis(
        dataset,
        config=_bid_only_config(),
    )

    assert result.timeframe_labels == ("1s",)
    assert len(result.slices) == 1


def test_execution_id_is_deterministic() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )
    config = _bid_only_config()

    first_id = liquidity_movement_analysis_id(
        dataset,
        config,
    )
    second_id = liquidity_movement_analysis_id(
        dataset,
        config,
    )

    first = execute_liquidity_movement_analysis(
        dataset,
        config=config,
    )
    second = execute_liquidity_movement_analysis(
        dataset,
        config=config,
    )

    assert first_id == second_id
    assert first.analysis_id == first_id
    assert second.analysis_id == first_id
    assert first == second


def test_execution_id_changes_with_semantic_configuration() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )

    first = _bid_only_config()
    second = LiquidityMovementAnalysisConfig(
        detection=LiquidityMovementDetectionConfig(
            confirmation_retracement_fraction=Decimal("0.25"),
        ),
        ranking=first.ranking,
        metrics=first.metrics,
    )

    assert liquidity_movement_analysis_id(
        dataset,
        first,
    ) != liquidity_movement_analysis_id(
        dataset,
        second,
    )


def test_cache_reuses_immutable_result() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )
    config = _bid_only_config()
    cache = LiquidityMovementAnalysisCache(
        maximum_entries=2,
    )
    phases: list[LiquidityMovementAnalysisProgressPhase] = []

    first = execute_liquidity_movement_analysis(
        dataset,
        config=config,
        cache=cache,
    )
    second = execute_liquidity_movement_analysis(
        dataset,
        config=config,
        cache=cache,
        progress_sink=lambda event: phases.append(event.phase),
    )

    assert second is first
    assert len(cache) == 1
    assert cache.analysis_ids == (first.analysis_id,)
    assert phases == [
        LiquidityMovementAnalysisProgressPhase.CACHE_HIT,
    ]


def test_cache_evicts_least_recently_used_result() -> None:
    cache = LiquidityMovementAnalysisCache(
        maximum_entries=1,
    )
    config = _bid_only_config()

    first = execute_liquidity_movement_analysis(
        _dataset(("100", "120", "116")),
        config=config,
        cache=cache,
    )
    second = execute_liquidity_movement_analysis(
        _dataset(("100", "130", "124", "120")),
        config=config,
        cache=cache,
    )

    assert len(cache) == 1
    assert cache.get(first.analysis_id) is None
    assert cache.get(second.analysis_id) is second


def test_cancellation_does_not_publish_partial_cache_result() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )
    cache = LiquidityMovementAnalysisCache()

    with pytest.raises(
        LiquidityMovementAnalysisCancelledError,
        match="cancelled",
    ):
        execute_liquidity_movement_analysis(
            dataset,
            config=_bid_only_config(),
            cache=cache,
            cancellation_probe=lambda: True,
        )

    assert len(cache) == 0


def test_constant_metric_creates_empty_slice() -> None:
    dataset = _dataset(
        (
            "100",
            "100",
            "100",
            "100",
        )
    )

    result = execute_liquidity_movement_analysis(
        dataset,
        config=_bid_only_config(),
    )

    item = result.slices[0]

    assert item.scan_bounds is None
    assert item.candidates == ()
    assert item.rankings == ()
    assert result.selected == ()


def test_scan_bounds_come_from_complete_usable_series() -> None:
    dataset = _dataset(
        (
            "80",
            "100",
            "120",
            "116",
        )
    )

    result = execute_liquidity_movement_analysis(
        dataset,
        config=_bid_only_config(),
    )
    bounds = result.slices[0].scan_bounds

    assert bounds is not None
    assert bounds.minimum == Decimal("80")
    assert bounds.maximum == Decimal("120")


def test_progress_reaches_completed_phase() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )
    events = []

    result = execute_liquidity_movement_analysis(
        dataset,
        config=_bid_only_config(),
        progress_sink=events.append,
    )

    assert events[0].phase is (LiquidityMovementAnalysisProgressPhase.PREPARING)
    assert events[-1].phase is (LiquidityMovementAnalysisProgressPhase.COMPLETED)
    assert events[-1].analysis_id == result.analysis_id
    assert events[-1].completed_units == events[-1].total_units


def test_app_lm_config_converts_to_exact_execution_config() -> None:
    app_config = LMConfig()

    config = analysis_config_from_lm_config(
        app_config,
    )

    assert config.detection.confirmation_retracement_fraction == Decimal("0.2")
    assert config.ranking.priority_height == Decimal("0.35")
    assert config.ranking.priority_retracement_count == Decimal("0.05")
    assert config.ranking.top_n_height == app_config.top_n_height


def test_cancelled_execution_does_not_return_cached_result() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )
    config = _bid_only_config()
    cache = LiquidityMovementAnalysisCache()

    expected = execute_liquidity_movement_analysis(
        dataset,
        config=config,
        cache=cache,
    )

    assert cache.get(expected.analysis_id) is expected

    with pytest.raises(LiquidityMovementAnalysisCancelledError):
        execute_liquidity_movement_analysis(
            dataset,
            config=config,
            cache=cache,
            cancellation_probe=lambda: True,
        )


def test_analysis_identity_owns_boundary_extremeness_ranking_version() -> None:
    from l2shock.analysis.execution import _execution_identity_payload
    from l2shock.analysis.dataset import AlignedAnalysisDataset

    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )
    config = LiquidityMovementAnalysisConfig()
    payload = _execution_identity_payload(dataset, config)

    assert payload["ranking"] == {
        "schema_version": 2,
        "algorithm_version": ("population-primary-recall-boundary-extremeness-v2"),
    }


def test_cancellation_during_completed_publication_removes_cached_result() -> None:
    dataset = _dataset(
        (
            "100",
            "110",
            "108",
            "120",
            "116",
        )
    )
    config = _bid_only_config()
    cache = LiquidityMovementAnalysisCache()
    cancelled = False

    def progress_sink(event) -> None:
        nonlocal cancelled

        if event.phase is LiquidityMovementAnalysisProgressPhase.COMPLETED:
            cancelled = True

    with pytest.raises(
        LiquidityMovementAnalysisCancelledError,
        match="cancelled",
    ):
        execute_liquidity_movement_analysis(
            dataset,
            config=config,
            cache=cache,
            cancellation_probe=lambda: cancelled,
            progress_sink=progress_sink,
        )

    assert len(cache) == 0
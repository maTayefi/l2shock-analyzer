from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.analysis import (
    AnalysisDatasetRequest,
    LiquidityMetric,
    LiquidityMovementAnalysisCancelledError,
    LiquidityMovementAnalysisConfig,
    LiquidityMovementDetectionConfig,
    LiquidityMovementRankingConfig,
    PriceBounds,
    build_aligned_analysis_dataset,
    execute_liquidity_movement_analysis,
)
from l2shock.analysis import L2Second, PriceSecond
from l2shock.ingest import BookSampleQuality
from l2shock.price import TradeSampleQuality
from l2shock.ui.analysis_runtime import (
    AnalysisRuntimeBusyError,
    ManualAnalysisRuntime,
)
from l2shock.ui.state import get_state, reset_state_for_tests


def _start() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _request() -> AnalysisDatasetRequest:
    return AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(seconds=4),
        activity_timeframe="1s",
        maximum_chart_bars=400,
        price_bounds=PriceBounds(
            min_price=90,
            max_price=110,
        ),
        context_before=0,
        context_after=0,
    )


def _dataset():
    request = _request()
    bid_values = (
        "100",
        "120",
        "116",
        "90",
        "94",
    )

    l2 = tuple(
        L2Second(
            timestamp_utc=_start() + timedelta(seconds=index),
            quality=BookSampleQuality.VALID,
            invalid_reason=None,
            bid_liquidity=Decimal(bid),
            ask_liquidity=Decimal("200"),
            source_count=1,
        )
        for index, bid in enumerate(bid_values)
    )

    price = tuple(
        PriceSecond(
            timestamp_utc=_start() + timedelta(seconds=index),
            quality=TradeSampleQuality.VALID,
            invalid_reason=None,
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("95"),
            close=Decimal("101"),
            trade_count=1,
        )
        for index in range(len(bid_values))
    )

    return build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
    )


def _config() -> LiquidityMovementAnalysisConfig:
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


@pytest.mark.asyncio
async def test_runtime_loads_executes_and_retains_result() -> None:
    reset_state_for_tests()
    dataset = _dataset()
    loader_thread_ids: list[int] = []

    def loader(request: AnalysisDatasetRequest):
        loader_thread_ids.append(threading.get_ident())
        assert request == dataset.request
        return dataset

    runtime = ManualAnalysisRuntime(
        dataset_loader=loader,
    )

    task = runtime.start(
        request=dataset.request,
        config=_config(),
    )
    result = await task

    assert result.status.value == "ok"
    assert result.stopped is False
    assert result.analysis is not None
    assert result.analysis_id == result.analysis.analysis_id
    assert result.candidate_count > 0
    assert result.selected_count > 0

    assert loader_thread_ids
    assert loader_thread_ids[0] != threading.get_ident()

    assert runtime.is_running is False
    assert get_state().active_operation_name == ""
    assert task not in get_state().tracked_tasks

    snapshot = runtime.snapshot()

    assert snapshot.last_result is result
    assert snapshot.last_error is None
    assert snapshot.completion_sequence == 1


@pytest.mark.asyncio
async def test_runtime_reuses_exact_cached_analysis() -> None:
    reset_state_for_tests()
    dataset = _dataset()
    load_count = 0

    def loader(_request: AnalysisDatasetRequest):
        nonlocal load_count
        load_count += 1
        return dataset

    runtime = ManualAnalysisRuntime(
        dataset_loader=loader,
    )

    first = await runtime.start(
        request=dataset.request,
        config=_config(),
    )
    second = await runtime.start(
        request=dataset.request,
        config=_config(),
    )

    assert first.analysis is not None
    assert second.analysis is first.analysis
    assert load_count == 2
    assert len(runtime.cache) == 1


@pytest.mark.asyncio
async def test_runtime_rejects_start_when_fetch_is_active() -> None:
    reset_state_for_tests()
    state = get_state()
    state.active_operation_name = "manual_fetch"
    state.active_operation_started_at = _start()

    runtime = ManualAnalysisRuntime(
        dataset_loader=lambda _request: _dataset(),
    )

    with pytest.raises(
        AnalysisRuntimeBusyError,
        match="manual_fetch",
    ):
        runtime.start(
            request=_request(),
            config=_config(),
        )

    assert runtime.is_running is False
    assert state.tracked_tasks == set()


@pytest.mark.asyncio
async def test_runtime_rejects_start_when_processing_is_active() -> None:
    reset_state_for_tests()
    state = get_state()
    state.active_operation_name = "manual_processing"
    state.active_operation_started_at = _start()

    runtime = ManualAnalysisRuntime(
        dataset_loader=lambda _request: _dataset(),
    )

    with pytest.raises(
        AnalysisRuntimeBusyError,
        match="manual_processing",
    ):
        runtime.start(
            request=_request(),
            config=_config(),
        )


@pytest.mark.asyncio
async def test_runtime_rejects_new_work_after_shutdown() -> None:
    reset_state_for_tests()
    get_state().shutdown_started = True

    runtime = ManualAnalysisRuntime(
        dataset_loader=lambda _request: _dataset(),
    )

    with pytest.raises(
        RuntimeError,
        match="shutdown has started",
    ):
        runtime.start(
            request=_request(),
            config=_config(),
        )


@pytest.mark.asyncio
async def test_runtime_rejects_second_analysis_start() -> None:
    reset_state_for_tests()
    entered = threading.Event()
    release = threading.Event()

    def loader(_request: AnalysisDatasetRequest):
        entered.set()
        release.wait(timeout=2.0)
        return _dataset()

    runtime = ManualAnalysisRuntime(
        dataset_loader=loader,
    )

    first_task = runtime.start(
        request=_request(),
        config=_config(),
    )

    observed = await asyncio.to_thread(
        entered.wait,
        1.0,
    )
    assert observed is True

    with pytest.raises(
        AnalysisRuntimeBusyError,
        match="already active",
    ):
        runtime.start(
            request=_request(),
            config=_config(),
        )

    release.set()
    await first_task


@pytest.mark.asyncio
async def test_runtime_cooperative_stop_signals_executor() -> None:
    reset_state_for_tests()
    dataset = _dataset()
    entered = threading.Event()
    observed_cancellation = threading.Event()

    def executor(
        _dataset_value,
        *,
        config,
        cancellation_probe,
        progress_sink,
        cache,
    ):
        del config, progress_sink, cache
        entered.set()

        while True:
            if cancellation_probe is not None and cancellation_probe():
                observed_cancellation.set()
                raise LiquidityMovementAnalysisCancelledError(
                    "cooperative stop observed"
                )

            time.sleep(0.005)

    runtime = ManualAnalysisRuntime(
        dataset_loader=lambda _request: dataset,
        executor=executor,
    )

    task = runtime.start(
        request=dataset.request,
        config=_config(),
    )

    observed = await asyncio.to_thread(
        entered.wait,
        1.0,
    )
    assert observed is True
    assert runtime.is_running is True
    assert get_state().active_operation_name == "manual_analysis"

    stopped = await runtime.stop_and_wait(
        grace_seconds=1.0,
    )
    result = await task

    assert stopped is True
    assert observed_cancellation.is_set()
    assert result.status.value == "stopped"
    assert result.stopped is True
    assert result.analysis is None
    assert len(runtime.cache) == 0
    assert runtime.is_running is False
    assert get_state().active_operation_name == ""


@pytest.mark.asyncio
async def test_native_task_cancellation_signals_worker() -> None:
    reset_state_for_tests()
    dataset = _dataset()
    entered = threading.Event()
    observed_cancellation = threading.Event()

    def executor(
        _dataset_value,
        *,
        config,
        cancellation_probe,
        progress_sink,
        cache,
    ):
        del config, progress_sink, cache
        entered.set()

        while True:
            if cancellation_probe is not None and cancellation_probe():
                observed_cancellation.set()
                raise LiquidityMovementAnalysisCancelledError(
                    "task cancellation observed"
                )

            time.sleep(0.005)

    runtime = ManualAnalysisRuntime(
        dataset_loader=lambda _request: dataset,
        executor=executor,
    )

    task = runtime.start(
        request=dataset.request,
        config=_config(),
    )

    observed = await asyncio.to_thread(
        entered.wait,
        1.0,
    )
    assert observed is True

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert observed_cancellation.is_set()
    assert runtime.is_running is False
    assert get_state().active_operation_name == ""


@pytest.mark.asyncio
async def test_default_executor_matches_direct_execution() -> None:
    reset_state_for_tests()
    dataset = _dataset()
    config = _config()

    expected = execute_liquidity_movement_analysis(
        dataset,
        config=config,
    )

    runtime = ManualAnalysisRuntime(
        dataset_loader=lambda _request: dataset,
    )

    observed = await runtime.start(
        request=dataset.request,
        config=config,
    )

    assert observed.analysis == expected

from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.processing import (
    PriceProcessingResult,
    ProcessingCancelledError,
    ProcessingQualityState,
    ProcessingResult,
)
from l2shock.ui.processing_runtime import (
    ManualProcessingRuntime,
    ProcessingRuntimeBusyError,
)
from l2shock.ui.state import get_state, reset_state_for_tests


def _hour(value: int = 12) -> datetime:
    return datetime(
        2026,
        9,
        2,
        value,
        tzinfo=timezone.utc,
    )


def _spec(
    kind: SourceDataKind,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=kind,
        hour_utc=_hour(),
    )


class FakeL2Coordinator:
    def __init__(self) -> None:
        self.calls = 0

    def run(
        self,
        request,
        preset,
        *,
        cancellation_probe=None,
    ) -> ProcessingResult:
        del preset
        self.calls += 1

        if cancellation_probe is not None and cancellation_probe():
            raise AssertionError("unexpected cancellation")

        return ProcessingResult(
            operation_id=request.operation_id,
            target=request.target,
            preset_hash="a" * 64,
            quality_state=ProcessingQualityState.INVALID,
            valid_count=0,
            degraded_count=0,
            invalid_count=3_600,
            replay_source_count=1,
            analytical_inserted=True,
            preset_inserted=True,
            analytical_content_sha256="b" * 64,
            input_checkpoint_content_sha256=None,
            output_checkpoint_content_sha256=None,
            completed_at=_hour(13),
        )


class FakePriceCoordinator:
    def __init__(self) -> None:
        self.calls = 0

    def run(
        self,
        request,
        *,
        cancellation_probe=None,
    ) -> PriceProcessingResult:
        self.calls += 1

        if cancellation_probe is not None and cancellation_probe():
            raise AssertionError("unexpected cancellation")

        return PriceProcessingResult(
            operation_id=request.operation_id,
            target=request.target,
            quality_state=ProcessingQualityState.DEGRADED,
            valid_count=1,
            invalid_count=3_599,
            total_trade_count=2,
            source_archive_count=1,
            price_inserted=True,
            price_content_sha256="c" * 64,
            completed_at=_hour(13),
        )


@pytest.mark.asyncio
async def test_runtime_processes_l2_and_price_targets() -> None:
    reset_state_for_tests()

    l2 = FakeL2Coordinator()
    price = FakePriceCoordinator()

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return (
            _spec(SourceDataKind.ORDERBOOK),
            _spec(SourceDataKind.TRADES),
        )

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: l2,
        price_coordinator_factory=lambda _sink: price,
    )

    task = runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    result = await task

    assert result.status == "ok"
    assert result.targets_selected == 2
    assert result.completed_count == 2
    assert result.failed_count == 0
    assert l2.calls == 1
    assert price.calls == 1

    assert runtime.is_running is False
    assert get_state().active_operation_name == ""
    assert task not in get_state().tracked_tasks

    snapshot = runtime.snapshot()

    assert snapshot.last_result is result
    assert snapshot.last_error is None
    assert snapshot.completion_sequence == 1


@pytest.mark.asyncio
async def test_runtime_reports_no_downloaded_work() -> None:
    reset_state_for_tests()

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return ()

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: FakeL2Coordinator(),
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    result = await runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert result.status == "no_work"
    assert result.targets_selected == 0
    assert result.items == ()


@pytest.mark.asyncio
async def test_runtime_rejects_operation_when_fetch_is_active() -> None:
    reset_state_for_tests()
    state = get_state()
    state.active_operation_name = "manual_fetch"
    state.active_operation_started_at = _hour()

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return ()

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: FakeL2Coordinator(),
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    with pytest.raises(
        ProcessingRuntimeBusyError,
        match="manual_fetch",
    ):
        runtime.start(
            requested_start_utc=_hour(12),
            requested_end_utc=_hour(13),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )


@pytest.mark.asyncio
async def test_runtime_rejects_new_work_after_shutdown() -> None:
    reset_state_for_tests()
    get_state().shutdown_started = True

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return ()

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: FakeL2Coordinator(),
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    with pytest.raises(
        RuntimeError,
        match="shutdown has started",
    ):
        runtime.start(
            requested_start_utc=_hour(12),
            requested_end_utc=_hour(13),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )


@pytest.mark.asyncio
async def test_processing_result_operation_id_is_shared() -> None:
    reset_state_for_tests()
    observed_ids = []

    class RecordingL2(FakeL2Coordinator):
        def run(
            self,
            request,
            preset,
            *,
            cancellation_probe=None,
        ) -> ProcessingResult:
            observed_ids.append(request.operation_id)
            return super().run(
                request,
                preset,
                cancellation_probe=cancellation_probe,
            )

    class RecordingPrice(FakePriceCoordinator):
        def run(
            self,
            request,
            *,
            cancellation_probe=None,
        ) -> PriceProcessingResult:
            observed_ids.append(request.operation_id)
            return super().run(
                request,
                cancellation_probe=cancellation_probe,
            )

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return (
            _spec(SourceDataKind.ORDERBOOK),
            _spec(SourceDataKind.TRADES),
        )

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: RecordingL2(),
        price_coordinator_factory=lambda _sink: RecordingPrice(),
    )

    result = await runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert len(observed_ids) == 2
    assert observed_ids == [
        result.operation_id,
        result.operation_id,
    ]


@pytest.mark.asyncio
async def test_runtime_cooperative_stop_signals_worker_thread() -> None:
    reset_state_for_tests()

    class BlockingL2Coordinator:
        def __init__(self) -> None:
            self.entered = threading.Event()
            self.observed_cancellation = threading.Event()

        def run(
            self,
            request,
            preset,
            *,
            cancellation_probe=None,
        ) -> ProcessingResult:
            del request, preset
            self.entered.set()

            while True:
                if cancellation_probe is not None and cancellation_probe():
                    self.observed_cancellation.set()
                    raise ProcessingCancelledError("cooperative stop observed")

                time.sleep(0.005)

    coordinator = BlockingL2Coordinator()

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return (_spec(SourceDataKind.ORDERBOOK),)

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: coordinator,
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    task = runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    entered = await asyncio.to_thread(
        coordinator.entered.wait,
        1.0,
    )
    assert entered is True
    assert runtime.is_running is True
    assert get_state().active_operation_name == "manual_processing"

    stopped = await runtime.stop_and_wait(grace_seconds=1.0)
    result = await task

    assert stopped is True
    assert coordinator.observed_cancellation.is_set()
    assert result.status == "stopped"
    assert result.stopped is True
    assert result.targets_selected == 1
    assert result.completed_count == 0
    assert result.failed_count == 0
    assert runtime.is_running is False
    assert get_state().active_operation_name == ""
    assert task not in get_state().tracked_tasks


@pytest.mark.asyncio
async def test_runtime_rejects_second_processing_start() -> None:
    reset_state_for_tests()

    release_loader = asyncio.Event()
    loader_entered = asyncio.Event()

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        loader_entered.set()
        await release_loader.wait()
        return ()

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: FakeL2Coordinator(),
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    first_task = runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    await loader_entered.wait()

    with pytest.raises(
        ProcessingRuntimeBusyError,
        match="already active",
    ):
        runtime.start(
            requested_start_utc=_hour(12),
            requested_end_utc=_hour(13),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )

    release_loader.set()
    await first_task


@pytest.mark.asyncio
async def test_processing_runtime_rejects_unrelated_active_operation() -> None:
    reset_state_for_tests()
    state = get_state()
    state.active_operation_name = "settings_maintenance"
    state.active_operation_started_at = _hour()

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return ()

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: FakeL2Coordinator(),
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    with pytest.raises(
        ProcessingRuntimeBusyError,
        match="settings_maintenance",
    ):
        runtime.start(
            requested_start_utc=_hour(12),
            requested_end_utc=_hour(13),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )


@pytest.mark.asyncio
async def test_runtime_builds_okx_preset_for_okx_orderbook() -> None:
    reset_state_for_tests()
    observed_markets: list[tuple[str, str]] = []

    class RecordingL2(FakeL2Coordinator):
        def run(
            self,
            request,
            preset,
            *,
            cancellation_probe=None,
        ) -> ProcessingResult:
            market = preset.eligible_markets[0]
            observed_markets.append(
                (
                    market.venue,
                    market.instrument,
                )
            )

            return super().run(
                request,
                preset,
                cancellation_probe=cancellation_probe,
            )

    target = SourceFileSpec(
        provider="cryptohftdata",
        venue="okx_futures",
        symbol="BTC-USDT-SWAP",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        return (target,)

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: RecordingL2(),
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    result = await runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert result.status == "ok"
    assert observed_markets == [
        (
            "okx_futures",
            "BTC-USDT-SWAP",
        )
    ]


@pytest.mark.asyncio
async def test_processing_completion_does_not_clear_newer_operation_owner() -> None:
    reset_state_for_tests()

    release_loader = asyncio.Event()
    loader_entered = asyncio.Event()

    async def loader(
        _start: datetime,
        _end: datetime,
        _lower_depth_fraction: Decimal,
        _upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        loader_entered.set()
        await release_loader.wait()
        return ()

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=lambda _sink: FakeL2Coordinator(),
        price_coordinator_factory=lambda _sink: FakePriceCoordinator(),
    )

    task = runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    await loader_entered.wait()

    state = get_state()
    state.active_operation_name = "settings_maintenance"
    state.active_operation_started_at = _hour(12)

    release_loader.set()
    await task

    assert state.active_operation_name == "settings_maintenance"
    assert state.active_operation_started_at == _hour(12)


@pytest.mark.asyncio
async def test_runtime_forwards_depth_to_target_loader() -> None:
    reset_state_for_tests()

    observed: list[tuple[Decimal, Decimal]] = []

    async def loader(
        _start: datetime,
        _end: datetime,
        lower_depth_fraction: Decimal,
        upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]:
        observed.append(
            (
                lower_depth_fraction,
                upper_depth_fraction,
            )
        )
        return ()

    runtime = ManualProcessingRuntime(
        target_loader=loader,
        l2_coordinator_factory=(lambda _sink: FakeL2Coordinator()),
        price_coordinator_factory=(lambda _sink: FakePriceCoordinator()),
    )

    result = await runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        lower_depth_fraction=Decimal("0.001"),
        upper_depth_fraction=Decimal("0.025"),
    )

    assert result.status == "no_work"
    assert observed == [
        (
            Decimal("0.001"),
            Decimal("0.025"),
        )
    ]

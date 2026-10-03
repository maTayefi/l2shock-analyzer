from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from l2shock.acquisition import (
    FetchOperationBusyError,
    ManualFetchResult,
)
from l2shock.ui.fetch_runtime import ManualFetchRuntime
from l2shock.ui.state import get_state, reset_state_for_tests


def _utc(hour: int) -> datetime:
    return datetime(
        2026,
        9,
        2,
        hour,
        tzinfo=timezone.utc,
    )


def _result() -> ManualFetchResult:
    return ManualFetchResult(
        operation_id=uuid4(),
        started_at=_utc(12),
        ended_at=_utc(12),
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
        status="ok",
        items=(),
        files_requested=4,
        files_downloaded=0,
        files_reused=0,
        files_missing=0,
        files_failed=0,
        stopped=False,
        details={},
    )


class FakeCoordinator:
    def __init__(self) -> None:
        self.active_operation_id = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.stop_requested = False

    async def run(
        self,
        *,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
    ) -> ManualFetchResult:
        del requested_start_utc, requested_end_utc

        self.active_operation_id = uuid4()
        self.entered.set()

        await self.release.wait()
        self.active_operation_id = None
        return _result()

    def request_stop(self) -> bool:
        if self.stop_requested:
            return False

        self.stop_requested = True
        self.release.set()
        return True


@pytest.mark.asyncio
async def test_runtime_owns_task_and_clears_active_state() -> None:
    reset_state_for_tests()
    runtime = ManualFetchRuntime()
    coordinator = FakeCoordinator()
    runtime.attach_coordinator(coordinator)  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    await coordinator.entered.wait()

    assert runtime.is_running is True
    assert get_state().active_operation_name == "manual_fetch"
    assert task in get_state().tracked_tasks

    coordinator.release.set()
    result = await task

    assert result.status == "ok"
    assert runtime.is_running is False
    assert get_state().active_operation_name == ""
    assert task not in get_state().tracked_tasks

    snapshot = runtime.snapshot()
    assert snapshot.last_result is result
    assert snapshot.last_error is None
    assert snapshot.completion_sequence == 1


@pytest.mark.asyncio
async def test_runtime_rejects_new_work_after_shutdown_barrier() -> None:
    reset_state_for_tests()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(FakeCoordinator())  # type: ignore[arg-type]

    get_state().shutdown_started = True

    with pytest.raises(RuntimeError, match="shutdown has started"):
        runtime.start(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )


@pytest.mark.asyncio
async def test_runtime_cooperative_stop_waits_for_completion() -> None:
    reset_state_for_tests()
    runtime = ManualFetchRuntime()
    coordinator = FakeCoordinator()
    runtime.attach_coordinator(coordinator)  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    await coordinator.entered.wait()

    stopped = await runtime.stop_and_wait(grace_seconds=1.0)

    assert stopped is True
    assert coordinator.stop_requested is True
    assert task.done() is True
    assert runtime.is_running is False


@pytest.mark.asyncio
async def test_runtime_validates_range_before_task_creation() -> None:
    reset_state_for_tests()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(FakeCoordinator())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="must be after"):
        runtime.start(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(12),
        )

    assert get_state().tracked_tasks == set()


@pytest.mark.asyncio
async def test_fetch_runtime_rejects_start_when_processing_is_active() -> None:
    reset_state_for_tests()
    state = get_state()
    state.active_operation_name = "manual_processing"
    state.active_operation_started_at = _utc(12)

    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(FakeCoordinator())  # type: ignore[arg-type]

    with pytest.raises(
        FetchOperationBusyError,
        match="manual_processing",
    ):
        runtime.start(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )

    assert runtime.is_running is False
    assert get_state().tracked_tasks == set()


@pytest.mark.asyncio
async def test_fetch_completion_does_not_clear_newer_operation_owner() -> None:
    reset_state_for_tests()

    runtime = ManualFetchRuntime()
    coordinator = FakeCoordinator()
    runtime.attach_coordinator(coordinator)  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    await coordinator.entered.wait()

    state = get_state()

    # Defensive ownership test: if another subsystem has replaced the marker,
    # this runtime must not erase that newer ownership during finalization.
    state.active_operation_name = "settings_maintenance"
    state.active_operation_started_at = _utc(12)

    coordinator.release.set()
    await task

    assert state.active_operation_name == "settings_maintenance"
    assert state.active_operation_started_at == _utc(12)


def test_fetch_start_without_event_loop_rolls_back_admission() -> None:
    reset_state_for_tests()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(FakeCoordinator())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError):
        runtime.start(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )

    assert get_state().active_operation_name == ""
    assert get_state().active_operation_started_at is None
    assert runtime.is_running is False


@pytest.mark.asyncio
async def test_force_cancel_propagates_caller_cancellation() -> None:
    reset_state_for_tests()

    class StubbornCoordinator(FakeCoordinator):
        async def run(
            self,
            *,
            requested_start_utc: datetime,
            requested_end_utc: datetime,
        ) -> ManualFetchResult:
            del requested_start_utc, requested_end_utc
            self.active_operation_id = uuid4()
            self.entered.set()

            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    continue

            self.active_operation_id = None
            return _result()

    runtime = ManualFetchRuntime()
    coordinator = StubbornCoordinator()
    runtime.attach_coordinator(coordinator)  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )
    await coordinator.entered.wait()

    waiter = asyncio.create_task(runtime.force_cancel_and_wait(timeout_seconds=5.0))
    await asyncio.sleep(0.05)
    waiter.cancel()

    try:
        with pytest.raises(asyncio.CancelledError):
            await waiter
    finally:
        coordinator.release.set()
        await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_fetch_prestart_cancel_releases_admission_exactly_once() -> None:
    reset_state_for_tests()
    state = get_state()
    coordinator = FakeCoordinator()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(coordinator)  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert state.active_operation_name == "manual_fetch"
    assert task in state.tracked_tasks

    # No await occurs between task creation and cancellation.
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0)

    snapshot = runtime.snapshot()

    assert not coordinator.entered.is_set()
    assert runtime.is_running is False
    assert runtime._task is None
    assert snapshot.started_at is None
    assert snapshot.stop_requested is False
    assert snapshot.completion_sequence == 1
    assert snapshot.last_error == "Manual fetch was cancelled before it started"
    assert state.active_operation_name == ""
    assert state.active_operation_started_at is None
    assert task not in state.tracked_tasks

    # The finalized runtime can admit and finish another operation.
    replacement = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    await coordinator.entered.wait()
    coordinator.release.set()
    await replacement
    await asyncio.sleep(0)

    assert runtime.snapshot().completion_sequence == 2
    assert state.active_operation_name == ""
    assert replacement not in state.tracked_tasks


@pytest.mark.asyncio
async def test_fetch_immediate_force_cancel_releases_admission() -> None:
    reset_state_for_tests()
    state = get_state()
    coordinator = FakeCoordinator()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(coordinator)  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    # Exercise the public cancellation path without first letting the
    # fetch coroutine enter the coordinator.
    stopped = await runtime.force_cancel_and_wait(timeout_seconds=1.0)
    await asyncio.sleep(0)

    assert stopped is True
    assert task.cancelled()
    assert not coordinator.entered.is_set()
    assert runtime._task is None
    assert runtime.snapshot().completion_sequence == 1
    assert state.active_operation_name == ""
    assert state.active_operation_started_at is None
    assert task not in state.tracked_tasks


@pytest.mark.asyncio
async def test_fetch_prestart_cancel_preserves_newer_same_name_owner() -> None:
    reset_state_for_tests()
    state = get_state()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(FakeCoordinator())  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )
    old_started_at = state.active_operation_started_at
    newer_started_at = _utc(13)

    assert old_started_at != newer_started_at

    state.active_operation_name = "manual_fetch"
    state.active_operation_started_at = newer_started_at
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0)

    assert runtime._task is None
    assert runtime.snapshot().completion_sequence == 1
    assert task not in state.tracked_tasks
    assert state.active_operation_name == "manual_fetch"
    assert state.active_operation_started_at == newer_started_at


@pytest.mark.asyncio
async def test_fetch_normal_completion_preserves_newer_same_name_owner() -> None:
    reset_state_for_tests()
    state = get_state()
    coordinator = FakeCoordinator()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(coordinator)  # type: ignore[arg-type]

    task = runtime.start(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    await coordinator.entered.wait()

    newer_started_at = _utc(13)
    assert state.active_operation_started_at != newer_started_at

    state.active_operation_name = "manual_fetch"
    state.active_operation_started_at = newer_started_at

    coordinator.release.set()
    await task
    await asyncio.sleep(0)

    assert runtime._task is None
    assert runtime.snapshot().completion_sequence == 1
    assert task not in state.tracked_tasks
    assert state.active_operation_name == "manual_fetch"
    assert state.active_operation_started_at == newer_started_at


def test_fetch_start_without_loop_preserves_existing_snapshot() -> None:
    reset_state_for_tests()
    state = get_state()
    runtime = ManualFetchRuntime()
    runtime.attach_coordinator(FakeCoordinator())  # type: ignore[arg-type]

    runtime._started_at = _utc(12)
    runtime._stop_requested = True
    runtime._last_error = "Previous diagnostic"
    before = runtime.snapshot()

    with pytest.raises(RuntimeError):
        runtime.start(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )

    assert runtime.snapshot() == before
    assert runtime._task is None
    assert state.active_operation_name == ""
    assert state.active_operation_started_at is None
    assert state.tracked_tasks == set()

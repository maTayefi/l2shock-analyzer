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

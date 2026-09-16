from __future__ import annotations

import pytest

from l2shock.ui import shutdown as shutdown_module
from l2shock.ui.state import get_state, reset_state_for_tests


@pytest.fixture(autouse=True)
def _isolate_automatic_fetch_runtime(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        shutdown_module,
        "peek_automatic_fetch_runtime",
        lambda: None,
    )
    yield


class FakeRuntime:
    def __init__(
        self,
        events: list[str],
        *,
        name: str,
        cooperative_result: bool = True,
        forced_result: bool = True,
    ) -> None:
        self._events = events
        self._name = name
        self._cooperative_result = cooperative_result
        self._forced_result = forced_result
        self.is_running = True

    async def stop_and_wait(
        self,
        *,
        grace_seconds: float,
    ) -> bool:
        self._events.append(f"{self._name}:cooperative:{grace_seconds}")

        if self._cooperative_result:
            self.is_running = False

        return self._cooperative_result

    async def force_cancel_and_wait(
        self,
        *,
        timeout_seconds: float,
    ) -> bool:
        self._events.append(f"{self._name}:forced:{timeout_seconds}")

        if self._forced_result:
            self.is_running = False

        return self._forced_result


@pytest.mark.asyncio
async def test_shutdown_orders_fetch_processing_tasks_engine_and_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    fetch_runtime = FakeRuntime(
        events,
        name="fetch",
    )
    processing_runtime = FakeRuntime(
        events,
        name="processing",
    )

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: fetch_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: processing_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    async def stop_server() -> None:
        events.append("server")

    monkeypatch.setattr(
        shutdown_module,
        "_request_nicegui_shutdown",
        stop_server,
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=True,
        fetch_grace_seconds=2.0,
        fetch_force_cancel_seconds=3.0,
        processing_grace_seconds=5.0,
        processing_force_cancel_seconds=6.0,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "fetch:cooperative:2.0",
        "processing:cooperative:5.0",
        "tasks:4.0",
        "engine",
        "server",
    ]

    state = get_state()
    assert state.shutdown_started is True
    assert state.shutdown_complete is True


@pytest.mark.asyncio
async def test_shutdown_forces_fetch_after_grace_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    fetch_runtime = FakeRuntime(
        events,
        name="fetch",
        cooperative_result=False,
    )

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: fetch_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        fetch_grace_seconds=2.0,
        fetch_force_cancel_seconds=3.0,
        processing_grace_seconds=5.0,
        processing_force_cancel_seconds=6.0,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "fetch:cooperative:2.0",
        "fetch:forced:3.0",
        "tasks:4.0",
        "engine",
    ]


@pytest.mark.asyncio
async def test_shutdown_forces_processing_after_grace_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    processing_runtime = FakeRuntime(
        events,
        name="processing",
        cooperative_result=False,
    )

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: processing_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        fetch_grace_seconds=2.0,
        fetch_force_cancel_seconds=3.0,
        processing_grace_seconds=5.0,
        processing_force_cancel_seconds=6.0,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "processing:cooperative:5.0",
        "processing:forced:6.0",
        "tasks:4.0",
        "engine",
    ]


@pytest.mark.asyncio
async def test_shutdown_raises_admission_barrier_before_runtime_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    class BarrierCheckingRuntime:
        is_running = True

        async def stop_and_wait(
            self,
            *,
            grace_seconds: float,
        ) -> bool:
            assert grace_seconds == 2.0
            assert get_state().shutdown_started is True
            events.append("barrier-observed")
            self.is_running = False
            return True

        async def force_cancel_and_wait(
            self,
            *,
            timeout_seconds: float,
        ) -> bool:
            del timeout_seconds
            raise AssertionError("forced cancellation was not expected")

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: BarrierCheckingRuntime(),
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        del timeout_seconds
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: None,
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        fetch_grace_seconds=2.0,
    )

    assert events == ["barrier-observed"]


@pytest.mark.asyncio
async def test_shutdown_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    engine_disposals = 0

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        del timeout_seconds
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )

    def dispose_engine() -> None:
        nonlocal engine_disposals
        engine_disposals += 1

    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        dispose_engine,
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
    )
    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
    )

    assert engine_disposals == 1


@pytest.mark.asyncio
async def test_shutdown_skips_idle_runtimes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    fetch_runtime = FakeRuntime(events, name="fetch")
    processing_runtime = FakeRuntime(events, name="processing")
    fetch_runtime.is_running = False
    processing_runtime.is_running = False

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: fetch_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: processing_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "tasks:4.0",
        "engine",
    ]


@pytest.mark.asyncio
async def test_shutdown_orders_fetch_processing_analysis_tasks_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    fetch_runtime = FakeRuntime(
        events,
        name="fetch",
    )
    processing_runtime = FakeRuntime(
        events,
        name="processing",
    )
    analysis_runtime = FakeRuntime(
        events,
        name="analysis",
    )

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: fetch_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: processing_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: analysis_runtime,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        fetch_grace_seconds=2.0,
        fetch_force_cancel_seconds=3.0,
        processing_grace_seconds=5.0,
        processing_force_cancel_seconds=6.0,
        analysis_grace_seconds=7.0,
        analysis_force_cancel_seconds=8.0,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "fetch:cooperative:2.0",
        "processing:cooperative:5.0",
        "analysis:cooperative:7.0",
        "tasks:4.0",
        "engine",
    ]


@pytest.mark.asyncio
async def test_shutdown_forces_analysis_after_grace_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    analysis_runtime = FakeRuntime(
        events,
        name="analysis",
        cooperative_result=False,
    )

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: analysis_runtime,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        analysis_grace_seconds=7.0,
        analysis_force_cancel_seconds=8.0,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "analysis:cooperative:7.0",
        "analysis:forced:8.0",
        "tasks:4.0",
        "engine",
    ]


@pytest.mark.asyncio
async def test_shutdown_does_not_dispose_engine_when_worker_remains_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    processing_runtime = FakeRuntime(
        events,
        name="processing",
        cooperative_result=False,
        forced_result=False,
    )

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: processing_runtime,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    with pytest.raises(
        RuntimeError,
        match="background work remained active",
    ):
        await shutdown_module.shutdown_runtime(
            request_server_stop=False,
            processing_grace_seconds=5.0,
            processing_force_cancel_seconds=6.0,
            other_task_timeout_seconds=4.0,
        )

    assert events == [
        "processing:cooperative:5.0",
        "processing:forced:6.0",
        "tasks:4.0",
    ]

    state = get_state()

    assert state.shutdown_started is True
    assert state.shutdown_complete is False
    assert "engine" not in events


@pytest.mark.asyncio
async def test_shutdown_does_not_dispose_engine_when_tracked_tasks_remain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_analysis_runtime",
        lambda: None,
    )

    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return 2

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )

    with pytest.raises(
        RuntimeError,
        match="background work remained active",
    ):
        await shutdown_module.shutdown_runtime(
            request_server_stop=False,
            other_task_timeout_seconds=4.0,
        )

    assert events == ["tasks:4.0"]

    state = get_state()

    assert state.shutdown_started is True
    assert state.shutdown_complete is False

from __future__ import annotations

from types import SimpleNamespace

import pytest

from l2shock.ui import shutdown as shutdown_module
from l2shock.ui.state import get_state, reset_state_for_tests


@pytest.fixture(autouse=True)
def _isolate_background_runtimes(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        shutdown_module,
        "peek_automatic_fetch_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_remote_import_runtime",
        lambda: None,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_shock_runtime",
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


class FakeShockRuntime:
    """Mirrors ManualShockRuntime: snapshot() plus stop_and_wait only."""

    def __init__(
        self,
        events: list[str],
        *,
        cooperative_result: bool = True,
        running: bool = True,
    ) -> None:
        self._events = events
        self._cooperative_result = cooperative_result
        self._running = running

    def snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(is_running=self._running)

    async def stop_and_wait(
        self,
        *,
        grace_seconds: float,
    ) -> bool:
        self._events.append(f"shock:cooperative:{grace_seconds}")

        if self._cooperative_result:
            self._running = False

        return self._cooperative_result


def _patch_tail(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
    *,
    pending: int = 0,
) -> None:
    async def cancel_tasks(
        *,
        timeout_seconds: float,
    ) -> int:
        events.append(f"tasks:{timeout_seconds}")
        return pending

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


def _patch_owners(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fetch=None,
    processing=None,
    shock=None,
) -> None:
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_fetch_runtime",
        lambda: fetch,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_processing_runtime",
        lambda: processing,
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_manual_shock_runtime",
        lambda: shock,
    )


def test_lm_analysis_runtime_is_not_referenced_by_shutdown() -> None:
    assert not hasattr(shutdown_module, "peek_manual_analysis_runtime")


@pytest.mark.asyncio
async def test_shutdown_orders_fetch_processing_tasks_engine_and_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    _patch_owners(
        monkeypatch,
        fetch=FakeRuntime(events, name="fetch"),
        processing=FakeRuntime(events, name="processing"),
    )
    _patch_tail(monkeypatch, events)

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

    _patch_owners(
        monkeypatch,
        fetch=FakeRuntime(events, name="fetch", cooperative_result=False),
    )
    _patch_tail(monkeypatch, events)

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        fetch_grace_seconds=2.0,
        fetch_force_cancel_seconds=3.0,
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

    _patch_owners(
        monkeypatch,
        processing=FakeRuntime(
            events,
            name="processing",
            cooperative_result=False,
        ),
    )
    _patch_tail(monkeypatch, events)

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

    _patch_owners(monkeypatch, fetch=BarrierCheckingRuntime())
    _patch_tail(monkeypatch, [])

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
    events: list[str] = []

    _patch_owners(monkeypatch)
    _patch_tail(monkeypatch, events)

    await shutdown_module.shutdown_runtime(request_server_stop=False)
    await shutdown_module.shutdown_runtime(request_server_stop=False)

    assert events.count("engine") == 1


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

    _patch_owners(
        monkeypatch,
        fetch=fetch_runtime,
        processing=processing_runtime,
        shock=FakeShockRuntime(events, running=False),
    )
    _patch_tail(monkeypatch, events)

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "tasks:4.0",
        "engine",
    ]


@pytest.mark.asyncio
async def test_shutdown_orders_fetch_processing_shock_tasks_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    _patch_owners(
        monkeypatch,
        fetch=FakeRuntime(events, name="fetch"),
        processing=FakeRuntime(events, name="processing"),
        shock=FakeShockRuntime(events),
    )
    _patch_tail(monkeypatch, events)

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        fetch_grace_seconds=2.0,
        processing_grace_seconds=5.0,
        analysis_grace_seconds=7.0,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "fetch:cooperative:2.0",
        "processing:cooperative:5.0",
        "shock:cooperative:7.0",
        "tasks:4.0",
        "engine",
    ]


@pytest.mark.asyncio
async def test_shock_worker_outliving_grace_blocks_engine_disposal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    _patch_owners(
        monkeypatch,
        shock=FakeShockRuntime(events, cooperative_result=False),
    )
    _patch_tail(monkeypatch, events)

    with pytest.raises(
        RuntimeError,
        match="background work remained active",
    ):
        await shutdown_module.shutdown_runtime(
            request_server_stop=False,
            analysis_grace_seconds=7.0,
            other_task_timeout_seconds=4.0,
        )

    # There is no forced step for the non-interruptible shock detector.
    assert events == [
        "shock:cooperative:7.0",
        "tasks:4.0",
    ]
    assert get_state().shutdown_complete is False


@pytest.mark.asyncio
async def test_shutdown_does_not_dispose_engine_when_worker_remains_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    _patch_owners(
        monkeypatch,
        processing=FakeRuntime(
            events,
            name="processing",
            cooperative_result=False,
            forced_result=False,
        ),
    )
    _patch_tail(monkeypatch, events)

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

    _patch_owners(monkeypatch)
    _patch_tail(monkeypatch, events, pending=2)

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


@pytest.mark.asyncio
async def test_shutdown_stops_remote_import_before_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events: list[str] = []

    remote_import_runtime = FakeRuntime(events, name="remote_import")

    _patch_owners(
        monkeypatch,
        processing=FakeRuntime(events, name="processing"),
    )
    monkeypatch.setattr(
        shutdown_module,
        "peek_remote_import_runtime",
        lambda: remote_import_runtime,
    )
    _patch_tail(monkeypatch, events)

    await shutdown_module.shutdown_runtime(
        request_server_stop=False,
        remote_import_grace_seconds=7.0,
        remote_import_force_cancel_seconds=8.0,
        processing_grace_seconds=5.0,
        processing_force_cancel_seconds=6.0,
        other_task_timeout_seconds=4.0,
    )

    assert events == [
        "remote_import:cooperative:7.0",
        "processing:cooperative:5.0",
        "tasks:4.0",
        "engine",
    ]

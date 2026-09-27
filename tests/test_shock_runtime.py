# tests/test_shock_runtime.py
import asyncio
import threading
from types import SimpleNamespace

import pytest

from l2shock.analysis.shock_dataset import ShockDatasetRequest
from l2shock.analysis.shock_evidence import ShockEvidenceConfig
from l2shock.analysis.shock_start import ShockStartConfig
from l2shock.ui import shock_runtime
from l2shock.ui.shock_runtime import (
    ManualShockRuntime,
    ShockRuntimeBusyError,
    ShockRuntimePhase,
)


def _state():
    # Created inside each test's event loop.
    return SimpleNamespace(
        shutdown_started=False,
        active_operation_name="",
        active_operation_started_at=None,
        operation_lock=asyncio.Lock(),
        tracked_tasks=set(),
    )


def _request():
    from datetime import datetime, timedelta, timezone

    start = datetime(2026, 9, 24, tzinfo=timezone.utc)

    return ShockDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=start,
        requested_end_utc=start + timedelta(seconds=12),
    )


def _start(runtime):
    return runtime.start(
        request=_request(),
        config=ShockStartConfig(),
        evidence_config=ShockEvidenceConfig(),
    )


def test_runtime_shared_admission_and_success(monkeypatch):
    from fractions import Fraction

    from tests.test_shock_review import (
        _TOTALS,
        _hypothesis,
        _review,
    )

    scan_range = max(_TOTALS) - min(_TOTALS)
    review = _review(
        (
            _hypothesis(
                2,
                5,
                10,
                scale="major",
                scan_range=scan_range,
            ),
        )
    )

    async def scenario():
        state = _state()
        monkeypatch.setattr(shock_runtime, "get_state", lambda: state)

        runtime = ManualShockRuntime(
            runner=lambda request, config, evidence, stop: review
        )

        task = _start(runtime)

        assert state.active_operation_name == "manual_shock_review"

        with pytest.raises(ShockRuntimeBusyError):
            _start(runtime)

        assert await task is review

        snapshot = runtime.snapshot()
        assert snapshot.phase is ShockRuntimePhase.COMPLETED
        assert snapshot.last_review is review
        assert snapshot.completion_sequence == 1
        assert snapshot.is_running is False

        assert state.active_operation_name == ""
        assert state.active_operation_started_at is None
        assert state.tracked_tasks == set()
        assert state.operation_lock.locked() is False

        # Other application operations must block admission before the
        # worker begins, not merely wait behind the shared lock.
        state.active_operation_name = "manual_processing"

        with pytest.raises(ShockRuntimeBusyError):
            _start(runtime)

    asyncio.run(scenario())


def test_stop_keeps_admission_until_worker_exits(monkeypatch):
    async def scenario():
        state = _state()
        monkeypatch.setattr(shock_runtime, "get_state", lambda: state)

        worker_entered = threading.Event()
        worker_release = threading.Event()

        def slow_runner(request, config, evidence, stop):
            worker_entered.set()
            worker_release.wait(timeout=3)
            return None  # Stop must discard this; it is not a review.

        runtime = ManualShockRuntime(runner=slow_runner)
        task = _start(runtime)

        entered = await asyncio.to_thread(worker_entered.wait, 3)
        assert entered is True

        assert runtime.request_stop() is True
        assert runtime.snapshot().phase is ShockRuntimePhase.STOPPING

        # A timed-out Stop cannot release the global operation slot while
        # the synchronous worker still owns it.
        assert await runtime.stop_and_wait(grace_seconds=0.001) is False
        assert state.active_operation_name == "manual_shock_review"
        assert state.operation_lock.locked() is True

        worker_release.set()

        assert await task is None
        assert runtime.snapshot().phase is ShockRuntimePhase.STOPPED
        assert runtime.snapshot().last_review is None
        assert state.active_operation_name == ""
        assert state.operation_lock.locked() is False

    asyncio.run(scenario())


def test_failure_does_not_publish_partial_review_or_sql_details(monkeypatch):
    async def scenario():
        state = _state()
        monkeypatch.setattr(shock_runtime, "get_state", lambda: state)

        def failing_runner(request, config, evidence, stop):
            raise RuntimeError("password=do-not-show")

        runtime = ManualShockRuntime(runner=failing_runner)

        with pytest.raises(
            RuntimeError,
            match="password=do-not-show",
        ):
            await _start(runtime)

        snapshot = runtime.snapshot()

        assert snapshot.phase is ShockRuntimePhase.FAILED
        assert snapshot.last_review is None
        assert snapshot.last_error == "Unexpected RuntimeError"
        assert "password" not in snapshot.last_error
        assert state.active_operation_name == ""
        assert state.operation_lock.locked() is False

    asyncio.run(scenario())


async def test_repeated_cancellation_keeps_shock_lock_until_worker_exits() -> None:
    import asyncio
    import threading

    import pytest

    from l2shock.ui.shock_runtime import ManualShockRuntime
    from l2shock.ui.state import get_state, reset_state_for_tests

    reset_state_for_tests()
    entered = threading.Event()
    release = threading.Event()

    def blocking_runner(request, config, evidence_config, stop_event):
        entered.set()
        if not release.wait(timeout=5.0):
            raise TimeoutError("test did not release the worker")
        return object()

    runtime = ManualShockRuntime()
    runtime._runner = blocking_runner
    task = asyncio.create_task(
        runtime._run(
            request=None,
            config=None,
            evidence_config=None,
            stop_event=threading.Event(),
        )
    )

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert get_state().operation_lock.locked()
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert not get_state().operation_lock.locked()


def test_shock_start_without_event_loop_rolls_back_admission() -> None:
    import warnings

    import pytest

    from l2shock.analysis.shock_dataset import ShockDatasetRequest
    from l2shock.ui.shock_runtime import ManualShockRuntime
    from l2shock.ui.state import get_state, reset_state_for_tests
    from l2shock.ui.tab_shock_review import build_shock_request

    reset_state_for_tests()
    request, config = build_shock_request(
        base="BTC",
        preset_hash="a" * 64,
        start_utc="2026-09-24T00:00:00Z",
        end_utc="2026-09-24T00:10:00Z",
        major_fraction="0.20",
        medium_fraction="0.10",
        minor_fraction="0.05",
    )
    assert isinstance(request, ShockDatasetRequest)

    from l2shock.analysis.shock_evidence import ShockEvidenceConfig

    runtime = ManualShockRuntime()

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)

        with pytest.raises(RuntimeError):
            runtime.start(
                request=request,
                config=config,
                evidence_config=ShockEvidenceConfig(),
            )

    assert get_state().active_operation_name == ""
    assert get_state().active_operation_started_at is None

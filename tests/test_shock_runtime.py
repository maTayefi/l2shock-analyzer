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

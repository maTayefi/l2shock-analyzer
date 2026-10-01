# l2shock/ui/l2_view_runtime.py
"""Application-owned runtime for one Analysis load (no detection).

Shares operation admission and the process lock with other operations.
Stop is cooperative at every hour boundary. A stopped load publishes
nothing; the previous projection stays the last result (D12).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from enum import StrEnum
from uuid import uuid4

from l2shock.analysis.l2_view_stream import (
    L2ViewCancelledError,
    L2ViewError,
    L2ViewLoadOptions,
    L2ViewProjection,
    L2ViewRequest,
    stream_l2_view,
)
from l2shock.timeutils import now_utc
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)
_OPERATION_NAME = "manual_analysis_load"


class L2ViewRuntimeBusyError(RuntimeError):
    """Another operation owns admission."""


class L2ViewRuntimePhase(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class L2ViewRuntimeSnapshot:
    phase: L2ViewRuntimePhase
    is_running: bool
    completion_sequence: int
    hours_done: int
    hours_total: int
    last_projection: L2ViewProjection | None
    last_error: str | None
    operation_id: str | None = None
    stop_requested: bool = False


class L2ViewRuntime:
    def __init__(self, *, loader=stream_l2_view) -> None:
        self._loader = loader
        self._task: asyncio.Task | None = None
        self._stop: threading.Event | None = None
        self._phase = L2ViewRuntimePhase.IDLE
        self._sequence = 0
        self._progress = (0, 0)
        self._last: L2ViewProjection | None = None
        self._error: str | None = None
        self._operation_id: str | None = None

    def snapshot(self) -> L2ViewRuntimeSnapshot:
        task = self._task
        stop = self._stop
        return L2ViewRuntimeSnapshot(
            phase=self._phase,
            is_running=task is not None and not task.done(),
            completion_sequence=self._sequence,
            hours_done=self._progress[0],
            hours_total=self._progress[1],
            last_projection=self._last,
            last_error=self._error,
            operation_id=self._operation_id,
            stop_requested=stop is not None and stop.is_set(),
        )

    def start(
        self,
        request: L2ViewRequest,
        options: L2ViewLoadOptions,
    ) -> asyncio.Task:
        """Admit one load without leaving an unowned operation reservation."""
        if not isinstance(request, L2ViewRequest) or not isinstance(
            options,
            L2ViewLoadOptions,
        ):
            raise TypeError("request/options have the wrong type")

        state = get_state()

        if state.shutdown_started:
            raise L2ViewRuntimeBusyError("Application shutdown has started")

        # A cancelled-but-not-finalized task still owns its reservation.
        # Its done callback must finish cleanup before another start.
        if self._task is not None:
            raise L2ViewRuntimeBusyError("An Analysis load is already active")

        if state.active_operation_name:
            raise L2ViewRuntimeBusyError(
                f"Another operation is active: {state.active_operation_name}"
            )

        if state.operation_lock.locked():
            raise L2ViewRuntimeBusyError("Another operation owns the process lock")

        # Obtain the loop before changing any runtime or admission state.
        # Calling start() without a running loop leaves no reservation.
        loop = asyncio.get_running_loop()

        previous_phase = self._phase
        previous_error = self._error
        previous_progress = self._progress
        previous_operation_id = self._operation_id

        stop = threading.Event()
        operation_id = uuid4().hex
        entered = False

        async def run_owned() -> L2ViewProjection | None:
            nonlocal entered
            entered = True
            return await self._run(request, options, stop)

        self._stop = stop
        self._operation_id = operation_id
        self._phase = L2ViewRuntimePhase.RUNNING
        self._error = None
        self._progress = (0, 0)
        state.active_operation_name = _OPERATION_NAME
        state.active_operation_started_at = now_utc()

        coroutine = run_owned()

        try:
            task = loop.create_task(
                coroutine,
                name="l2shock-analysis-load",
            )
        except BaseException:
            coroutine.close()

            if state.active_operation_name == _OPERATION_NAME:
                state.active_operation_name = ""
                state.active_operation_started_at = None

            self._stop = None
            self._operation_id = previous_operation_id
            self._phase = previous_phase
            self._error = previous_error
            self._progress = previous_progress
            raise

        self._task = task
        state.tracked_tasks.add(task)

        def finalize(done: asyncio.Task) -> None:
            state.tracked_tasks.discard(done)

            if (
                not entered
                and self._task is done
                and self._operation_id == operation_id
            ):
                # A task cancelled before its first execution never enters
                # _run(), so its try/finally cannot release admission.
                stop.set()
                self._phase = L2ViewRuntimePhase.STOPPED
                self._sequence += 1
                self._stop = None

                if state.active_operation_name == _OPERATION_NAME:
                    state.active_operation_name = ""
                    state.active_operation_started_at = None

            if self._task is done:
                self._task = None

            # Retrieve failures even when a caller does not await the task.
            # Awaiting callers still receive the same exception.
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finalize)
        return task

    def request_stop(self) -> bool:
        if self._task is None or self._task.done() or self._stop is None:
            return False
        self._stop.set()
        self._phase = L2ViewRuntimePhase.STOPPING
        return True

    async def stop_and_wait(self, *, grace_seconds: float) -> bool:
        task = self._task
        if task is None or task.done():
            return True
        self.request_stop()
        try:
            await asyncio.wait_for(
                asyncio.shield(task), timeout=max(0.0, float(grace_seconds))
            )
        except TimeoutError:
            return False
        except asyncio.CancelledError:
            if task.cancelled():
                return True
            raise
        except Exception:
            return True
        return True

    def _set_progress(self, done: int, total: int) -> None:
        self._progress = (done, total)

    async def _run(self, request, options, stop: threading.Event):
        state = get_state()
        current = asyncio.current_task()
        lock = state.operation_lock
        acquired = False
        worker: asyncio.Task | None = None

        try:
            if stop.is_set():
                self._phase = L2ViewRuntimePhase.STOPPED
                return None

            if state.shutdown_started:
                stop.set()
                self._phase = L2ViewRuntimePhase.STOPPED
                return None

            if lock.locked():
                raise L2ViewRuntimeBusyError("Another operation owns the process lock")

            await lock.acquire()
            acquired = True

            # Keep this check at the worker boundary as well. A Stop must
            # not be interpreted merely as "discard the eventual result"
            # when no synchronous loading work has started yet.
            if stop.is_set() or state.shutdown_started:
                stop.set()
                self._phase = L2ViewRuntimePhase.STOPPED
                return None

            worker = asyncio.create_task(
                asyncio.to_thread(
                    self._loader,
                    request,
                    options,
                    cancellation_probe=stop.is_set,
                    progress_sink=self._set_progress,
                ),
                name="l2shock-analysis-load-worker",
            )
            projection = await asyncio.shield(worker)

            if stop.is_set():
                self._phase = L2ViewRuntimePhase.STOPPED
                return None

            if not isinstance(projection, L2ViewProjection):
                raise TypeError("Analysis loader must return L2ViewProjection")

            self._last = projection
            self._phase = L2ViewRuntimePhase.COMPLETED
            return projection

        except L2ViewCancelledError:
            self._phase = L2ViewRuntimePhase.STOPPED
            return None

        except asyncio.CancelledError:
            stop.set()
            if worker is not None:
                while not worker.done():
                    try:
                        await asyncio.shield(worker)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                if not worker.cancelled():
                    try:
                        worker.result()
                    except Exception:
                        pass
            self._phase = L2ViewRuntimePhase.STOPPED
            raise

        except Exception as exc:
            self._phase = L2ViewRuntimePhase.FAILED
            self._error = (
                str(exc)
                if isinstance(exc, (L2ViewError, L2ViewRuntimeBusyError))
                else f"Unexpected {type(exc).__name__}"
            )
            log.exception("Analysis load failed.")
            raise

        finally:
            if acquired:
                lock.release()
            self._sequence += 1
            self._stop = None
            if state.active_operation_name == _OPERATION_NAME:
                state.active_operation_name = ""
                state.active_operation_started_at = None
            if current is not None:
                state.tracked_tasks.discard(current)
            if self._task is current:
                self._task = None


_runtime: L2ViewRuntime | None = None
_runtime_loop: asyncio.AbstractEventLoop | None = None


def get_l2_view_runtime() -> L2ViewRuntime:
    global _runtime, _runtime_loop
    loop = asyncio.get_running_loop()
    if _runtime is None:
        _runtime, _runtime_loop = L2ViewRuntime(), loop
    elif _runtime_loop is not loop:
        raise RuntimeError("Analysis runtime belongs to another event loop")
    return _runtime


def peek_l2_view_runtime() -> L2ViewRuntime | None:
    return _runtime


def reset_l2_view_runtime_for_tests() -> None:
    global _runtime, _runtime_loop
    _runtime = _runtime_loop = None


__all__ = [
    "L2ViewRuntime",
    "L2ViewRuntimeBusyError",
    "L2ViewRuntimePhase",
    "L2ViewRuntimeSnapshot",
    "get_l2_view_runtime",
    "peek_l2_view_runtime",
    "reset_l2_view_runtime_for_tests",
]

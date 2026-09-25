# l2shock/ui/shock_runtime.py
"""Application-owned runtime for a completed Shock-Start review.

The existing application state owns operation admission and the shared
asyncio operation lock. PostgreSQL sessions are created inside the worker
thread. No partial scan, evidence result, or stopped review is published.

The synchronous scan API currently has no cancellation hook. Stop is
cooperative at stage boundaries and after the worker returns; it cannot
interrupt an in-progress detector call.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from uuid import uuid4

from l2shock.analysis.shock_dataset import ShockDatasetRequest
from l2shock.analysis.shock_evidence import (
    ShockEvidenceConfig,
    describe_shock_evidence,
)
from l2shock.analysis.shock_execution import run_verified_shock_scan
from l2shock.analysis.shock_review import ShockReview, review_shock_areas
from l2shock.analysis.shock_start import ShockStartConfig
from l2shock.db.engine import session_scope
from l2shock.timeutils import now_utc
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)

_OPERATION_NAME = "manual_shock_review"


class ShockRuntimeBusyError(RuntimeError):
    """The shared application operation slot is occupied."""


class ShockRuntimeStopped(RuntimeError):
    """A worker reached a cooperative stop boundary."""


class ShockRuntimePhase(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ShockRuntimeSnapshot:
    operation_id: str | None
    phase: ShockRuntimePhase
    completion_sequence: int
    is_running: bool
    stop_requested: bool
    last_review: ShockReview | None
    last_error: str | None


ShockRunner = Callable[
    [ShockDatasetRequest, ShockStartConfig, ShockEvidenceConfig, threading.Event],
    ShockReview,
]


def _checkpoint(stop_event: threading.Event) -> None:
    if stop_event.is_set():
        raise ShockRuntimeStopped("Shock review was stopped")


def _production_runner(
    request: ShockDatasetRequest,
    config: ShockStartConfig,
    evidence_config: ShockEvidenceConfig,
    stop_event: threading.Event,
) -> ShockReview:
    """Run all synchronous stages in the same worker thread."""
    _checkpoint(stop_event)

    with session_scope() as session:
        scan = run_verified_shock_scan(
            session,
            request,
            config=config,
        )

    # In particular, do not publish a scan if Stop arrived while the
    # non-interruptible verified loader or detector was executing.
    _checkpoint(stop_event)

    evidence = describe_shock_evidence(
        scan,
        config=evidence_config,
    )
    _checkpoint(stop_event)

    review = review_shock_areas(evidence)
    _checkpoint(stop_event)

    return review


class ManualShockRuntime:
    """One process-local Shock-Start worker and its completed review."""

    def __init__(
        self,
        *,
        runner: ShockRunner = _production_runner,
    ) -> None:
        if not callable(runner):
            raise TypeError("runner must be callable")

        self._runner = runner
        self._task: asyncio.Task[ShockReview | None] | None = None
        self._stop_event: threading.Event | None = None

        self._operation_id: str | None = None
        self._phase = ShockRuntimePhase.IDLE
        self._completion_sequence = 0
        self._last_review: ShockReview | None = None
        self._last_error: str | None = None

    def snapshot(self) -> ShockRuntimeSnapshot:
        task = self._task

        return ShockRuntimeSnapshot(
            operation_id=self._operation_id,
            phase=self._phase,
            completion_sequence=self._completion_sequence,
            is_running=task is not None and not task.done(),
            stop_requested=(
                self._stop_event.is_set()
                if self._stop_event is not None else False
            ),
            last_review=self._last_review,
            last_error=self._last_error,
        )

    def start(
        self,
        *,
        request: ShockDatasetRequest,
        config: ShockStartConfig,
        evidence_config: ShockEvidenceConfig,
    ) -> asyncio.Task[ShockReview | None]:
        if not isinstance(request, ShockDatasetRequest):
            raise TypeError("request must be ShockDatasetRequest")

        if not isinstance(config, ShockStartConfig):
            raise TypeError("config must be ShockStartConfig")

        if not isinstance(evidence_config, ShockEvidenceConfig):
            raise TypeError("evidence_config must be ShockEvidenceConfig")

        state = get_state()

        if state.shutdown_started:
            raise ShockRuntimeBusyError(
                "Application shutdown has started"
            )

        if self._task is not None and not self._task.done():
            raise ShockRuntimeBusyError(
                "A Shock-Start review is already active"
            )

        if state.active_operation_name:
            raise ShockRuntimeBusyError(
                f"Another operation is active: "
                f"{state.active_operation_name}"
            )

        # Admission and task creation occur on the NiceGUI event loop,
        # without an intervening await.
        operation_id = uuid4().hex
        started_at = now_utc()
        stop_event = threading.Event()

        self._operation_id = operation_id
        self._stop_event = stop_event
        self._phase = ShockRuntimePhase.RUNNING
        self._last_review = None
        self._last_error = None

        state.active_operation_name = _OPERATION_NAME
        state.active_operation_started_at = started_at

        try:
            task = asyncio.create_task(
                self._run(
                    request=request,
                    config=config,
                    evidence_config=evidence_config,
                    stop_event=stop_event,
                ),
                name="l2shock-manual-shock-review",
            )
        except BaseException:
            state.active_operation_name = ""
            state.active_operation_started_at = None
            self._stop_event = None
            self._operation_id = None
            self._phase = ShockRuntimePhase.IDLE
            raise

        self._task = task
        state.tracked_tasks.add(task)
        return task

    def request_stop(self) -> bool:
        task = self._task

        if task is None or task.done() or self._stop_event is None:
            return False

        self._stop_event.set()
        self._phase = ShockRuntimePhase.STOPPING
        return True

    async def stop_and_wait(
        self,
        *,
        grace_seconds: float,
    ) -> bool:
        """Ask for Stop; return False if the worker outlives the grace period.

        On timeout the operation remains active. Its worker and shared lock
        must NOT be detached or falsely reported as stopped.
        """
        task = self._task

        if task is None or task.done():
            return True

        self.request_stop()

        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=max(0.0, float(grace_seconds)),
            )
        except TimeoutError:
            return False
        except asyncio.CancelledError:
            if task.cancelled():
                return True
            raise
        except Exception:
            # The worker has terminated; snapshot() carries the safe error.
            return True

        return True

    async def _run(
        self,
        *,
        request: ShockDatasetRequest,
        config: ShockStartConfig,
        evidence_config: ShockEvidenceConfig,
        stop_event: threading.Event,
    ) -> ShockReview | None:
        state = get_state()
        current_task = asyncio.current_task()
        operation_lock = state.operation_lock
        acquired = False
        worker: asyncio.Task[ShockReview] | None = None

        try:
            if operation_lock.locked():
                raise ShockRuntimeBusyError(
                    "Another operation owns the process lock"
                )

            await operation_lock.acquire()
            acquired = True

            if stop_event.is_set():
                self._phase = ShockRuntimePhase.STOPPED
                return None

            # shield prevents native cancellation of the asyncio wrapper
            # from orphaning a still-running synchronous database worker.
            worker = asyncio.create_task(
                asyncio.to_thread(
                    self._runner,
                    request,
                    config,
                    evidence_config,
                    stop_event,
                ),
                name="l2shock-shock-review-worker",
            )

            review = await asyncio.shield(worker)

            if stop_event.is_set():
                self._phase = ShockRuntimePhase.STOPPED
                return None

            if not isinstance(review, ShockReview):
                raise TypeError("Shock runner did not return ShockReview")

            self._last_review = review
            self._phase = ShockRuntimePhase.COMPLETED
            return review

        except ShockRuntimeStopped:
            self._phase = ShockRuntimePhase.STOPPED
            return None

        except asyncio.CancelledError:
            stop_event.set()

            if worker is not None:
                # Do not release the process lock or clear admission while
                # the synchronous worker still uses its database session.
                try:
                    await asyncio.shield(worker)
                except Exception:
                    log.exception(
                        "Shock worker failed during task cancellation."
                    )

            self._last_review = None
            self._phase = ShockRuntimePhase.STOPPED
            raise

        except Exception as exc:
            self._last_review = None
            self._phase = ShockRuntimePhase.FAILED

            if isinstance(
                exc,
                (ShockRuntimeBusyError, ShockRuntimeStopped),
            ):
                self._last_error = str(exc)
            else:
                # Do not disclose SQL connection details in UI polling.
                self._last_error = (
                    f"Unexpected {type(exc).__name__}"
                )

            log.exception("Shock-Start review failed.")
            raise

        finally:
            if acquired:
                operation_lock.release()

            self._completion_sequence += 1
            self._stop_event = None
            self._operation_id = None

            if state.active_operation_name == _OPERATION_NAME:
                state.active_operation_name = ""
                state.active_operation_started_at = None

            if current_task is not None:
                state.tracked_tasks.discard(current_task)

            if self._task is current_task:
                self._task = None


# One runtime per application event loop, not one runtime per browser client.
_manual_shock_runtime: ManualShockRuntime | None = None
_manual_shock_loop: asyncio.AbstractEventLoop | None = None


def get_manual_shock_runtime() -> ManualShockRuntime:
    global _manual_shock_runtime, _manual_shock_loop

    loop = asyncio.get_running_loop()

    if _manual_shock_runtime is None:
        _manual_shock_runtime = ManualShockRuntime()
        _manual_shock_loop = loop
    elif _manual_shock_loop is not loop:
        raise RuntimeError(
            "Manual Shock-Start runtime belongs to another event loop"
        )

    return _manual_shock_runtime


def peek_manual_shock_runtime() -> ManualShockRuntime | None:
    """Return the existing runtime without constructing one at shutdown."""
    return _manual_shock_runtime

__all__ = [
    "ManualShockRuntime",
    "ShockRuntimeBusyError",
    "ShockRuntimePhase",
    "ShockRuntimeSnapshot",
    "ShockRuntimeStopped",
    "get_manual_shock_runtime",
    "peek_manual_shock_runtime",
]
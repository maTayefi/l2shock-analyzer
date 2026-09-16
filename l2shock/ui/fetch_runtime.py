# l2shock/ui/fetch_runtime.py
"""Application-owned runtime for one manual acquisition coordinator.

Durable fetch truth remains in PostgreSQL. This module owns only the current
process's coordinator, active task, latest progress event, and latest result
needed by the UI and shutdown path.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from l2shock.acquisition import (
    AcquisitionError,
    FetchOperationBusyError,
    FetchProgress,
    ManualFetchCoordinator,
    ManualFetchResult,
    create_production_manual_fetch_coordinator,
)
from l2shock.timeutils import now_utc, require_aware_utc
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ManualFetchRuntimeSnapshot:
    """Immutable process-local state suitable for UI polling."""

    is_running: bool
    operation_id: str | None
    started_at: datetime | None
    stop_requested: bool
    latest_progress: FetchProgress | None
    last_result: ManualFetchResult | None
    last_error: str | None
    completion_sequence: int


class ManualFetchRuntime:
    """Own one coordinator and its currently active application task."""

    def __init__(self) -> None:
        self._coordinator: ManualFetchCoordinator | None = None
        self._task: asyncio.Task[ManualFetchResult] | None = None

        self._started_at: datetime | None = None
        self._stop_requested = False
        self._latest_progress: FetchProgress | None = None
        self._last_result: ManualFetchResult | None = None
        self._last_error: str | None = None
        self._completion_sequence = 0

    def attach_coordinator(
        self,
        coordinator: ManualFetchCoordinator,
    ) -> None:
        if self._coordinator is not None:
            raise RuntimeError("Manual fetch coordinator is already attached")

        self._coordinator = coordinator

    @property
    def coordinator(self) -> ManualFetchCoordinator:
        coordinator = self._coordinator

        if coordinator is None:
            raise RuntimeError("Manual fetch coordinator has not been attached")

        return coordinator

    @property
    def task(self) -> asyncio.Task[ManualFetchResult] | None:
        return self._task

    @property
    def is_running(self) -> bool:
        task = self._task
        return task is not None and not task.done()

    async def accept_progress(
        self,
        event: FetchProgress,
    ) -> None:
        """Receive progress without performing client-specific UI work."""
        self._latest_progress = event

    def snapshot(self) -> ManualFetchRuntimeSnapshot:
        coordinator = self._coordinator
        operation_id = (
            coordinator.active_operation_id if coordinator is not None else None
        )

        return ManualFetchRuntimeSnapshot(
            is_running=self.is_running,
            operation_id=(str(operation_id) if operation_id is not None else None),
            started_at=self._started_at,
            stop_requested=self._stop_requested,
            latest_progress=self._latest_progress,
            last_result=self._last_result,
            last_error=self._last_error,
            completion_sequence=self._completion_sequence,
        )

    def start(
        self,
        *,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
    ) -> asyncio.Task[ManualFetchResult]:
        """Start one application-owned manual fetch without queueing."""
        state = get_state()

        if state.shutdown_started:
            raise RuntimeError(
                "Application shutdown has started; new operations are blocked"
            )

        if self.is_running:
            raise FetchOperationBusyError("A manual fetch operation is already active")

        if state.active_operation_name:
            raise FetchOperationBusyError(
                f"Another operation is active: {state.active_operation_name}"
            )

        start = require_aware_utc(
            "requested_start_utc",
            requested_start_utc,
        )
        end = require_aware_utc(
            "requested_end_utc",
            requested_end_utc,
        )

        if end <= start:
            raise ValueError("requested_end_utc must be after requested_start_utc")

        self._started_at = now_utc()
        self._stop_requested = False
        self._latest_progress = None
        self._last_result = None
        self._last_error = None

        state.active_operation_name = "manual_fetch"
        state.active_operation_started_at = self._started_at

        task = asyncio.create_task(
            self._run(
                requested_start_utc=start,
                requested_end_utc=end,
            ),
            name="l2shock-manual-fetch",
        )

        self._task = task
        state.tracked_tasks.add(task)

        return task

    async def _run(
        self,
        *,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
    ) -> ManualFetchResult:
        state = get_state()
        current_task = asyncio.current_task()

        try:
            result = await self.coordinator.run(
                requested_start_utc=requested_start_utc,
                requested_end_utc=requested_end_utc,
            )

            self._last_result = result
            self._last_error = None
            return result

        except asyncio.CancelledError:
            self._last_error = (
                "Manual fetch task was cancelled during application shutdown"
            )
            raise

        except AcquisitionError as exc:
            self._last_error = str(exc) or type(exc).__name__
            log.exception("Manual fetch failed.")
            raise

        except Exception as exc:
            # Arbitrary exception arguments may include request or database
            # details. Keep the process-local UI boundary type-only.
            self._last_error = f"Unexpected {type(exc).__name__}"
            log.exception("Unexpected manual-fetch runtime failure.")
            raise

        finally:
            self._completion_sequence += 1
            self._stop_requested = False

            # Clear only operation ownership still belonging to this runtime.
            # Never erase a newer or independently owned operation marker.
            if state.active_operation_name == "manual_fetch":
                state.active_operation_name = ""
                state.active_operation_started_at = None

            if current_task is not None:
                state.tracked_tasks.discard(current_task)

            if self._task is current_task:
                self._task = None

    def request_stop(self) -> bool:
        """Request cooperative cancellation of the active coordinator."""
        if not self.is_running:
            return False

        requested = self.coordinator.request_stop()

        if requested:
            self._stop_requested = True

        return requested

    async def stop_and_wait(
        self,
        *,
        grace_seconds: float,
    ) -> bool:
        """Request cooperative stop and wait for bounded completion.

        Returns True when no fetch task remains active after the grace period.
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
            # The task has terminated and its error is already retained/logged.
            return True

        return True

    async def force_cancel_and_wait(
        self,
        *,
        timeout_seconds: float,
    ) -> bool:
        """Cancel an active task and wait for bounded cancellation."""
        task = self._task

        if task is None or task.done():
            return True

        task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=max(0.0, float(timeout_seconds)),
            )
        except TimeoutError:
            return False
        except asyncio.CancelledError:
            return True
        except Exception:
            return True

        return True


_runtime: ManualFetchRuntime | None = None
_runtime_loop: asyncio.AbstractEventLoop | None = None


def get_manual_fetch_runtime() -> ManualFetchRuntime:
    """Return the singleton runtime bound to the current application loop."""
    global _runtime, _runtime_loop

    loop = asyncio.get_running_loop()

    if _runtime is None:
        runtime = ManualFetchRuntime()
        coordinator = create_production_manual_fetch_coordinator(
            operation_lock=get_state().operation_lock,
            progress_sink=runtime.accept_progress,
        )
        runtime.attach_coordinator(coordinator)

        _runtime = runtime
        _runtime_loop = loop
        return runtime

    if _runtime_loop is not loop:
        raise RuntimeError("Manual fetch runtime belongs to another asyncio event loop")

    return _runtime


def peek_manual_fetch_runtime() -> ManualFetchRuntime | None:
    """Return the existing runtime without constructing production services."""
    return _runtime


def reset_manual_fetch_runtime_for_tests() -> None:
    global _runtime, _runtime_loop
    _runtime = None
    _runtime_loop = None


__all__ = [
    "ManualFetchRuntime",
    "ManualFetchRuntimeSnapshot",
    "get_manual_fetch_runtime",
    "peek_manual_fetch_runtime",
    "reset_manual_fetch_runtime_for_tests",
]

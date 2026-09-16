# l2shock/ui/analysis_runtime.py
"""Application-owned runtime for verified Liquidity Movement analysis.

Verified PostgreSQL loading, compact block decoding, timeframe aggregation,
candidate detection, and population ranking are synchronous operations.

This runtime executes that work in a worker thread while the NiceGUI event loop
remains responsive.

Ownership rules:

- analysis shares the process-wide operation lock with Fetch and Processing;
- a thread-safe Event provides cooperative cancellation;
- database sessions are created and closed inside the worker thread;
- immutable completed results are retained process-locally for the future
  Analysis table and chart UI;
- durable source and analytical truth remains in PostgreSQL;
- cancellation never publishes a partial result into the analysis cache;
- native asyncio cancellation signals the worker's cooperative cancellation
  event and waits for the worker boundary rather than assuming that task
  cancellation terminates a Python thread.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID, uuid4

from l2shock.analysis import (
    AlignedAnalysisDataset,
    AnalysisDatasetRequest,
    LiquidityMovementAnalysisCache,
    LiquidityMovementAnalysisCancelledError,
    LiquidityMovementAnalysisConfig,
    LiquidityMovementAnalysisProgress,
    LiquidityMovementAnalysisResult,
    execute_liquidity_movement_analysis,
    load_verified_analysis_dataset,
)
from l2shock.db.engine import session_scope
from l2shock.timeutils import now_utc, require_aware_utc
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)


class AnalysisRuntimeBusyError(RuntimeError):
    """Another process-local operation currently owns operation admission."""


class AnalysisRuntimeError(RuntimeError):
    """The application-owned analysis runtime could not complete safely."""


class ManualAnalysisStatus(StrEnum):
    """Terminal status of one application-owned analysis operation."""

    OK = "ok"
    STOPPED = "stopped"


class AnalysisRuntimePhase(StrEnum):
    """High-level phases owned by the asynchronous UI runtime."""

    LOADING = "loading"
    ANALYZING = "analyzing"
    CACHE_HIT = "cache_hit"
    COMPLETED = "completed"
    STOPPING = "stopping"


@dataclass(frozen=True, slots=True)
class ManualAnalysisResult:
    """Terminal immutable result of one analysis operation."""

    operation_id: UUID
    request: AnalysisDatasetRequest
    started_at: datetime
    ended_at: datetime
    status: ManualAnalysisStatus
    stopped: bool
    analysis: LiquidityMovementAnalysisResult | None

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            try:
                operation_id = UUID(str(operation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise AnalysisRuntimeError("operation_id must be a valid UUID") from exc

            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        if not isinstance(
            self.request,
            AnalysisDatasetRequest,
        ):
            raise TypeError("request must be AnalysisDatasetRequest")

        started = require_aware_utc(
            "started_at",
            self.started_at,
        )
        ended = require_aware_utc(
            "ended_at",
            self.ended_at,
        )

        if ended < started:
            raise AnalysisRuntimeError("ended_at cannot precede started_at")

        try:
            status = ManualAnalysisStatus(self.status)
        except (TypeError, ValueError) as exc:
            raise AnalysisRuntimeError("Unsupported manual analysis status") from exc

        if not isinstance(self.stopped, bool):
            raise TypeError("stopped must be bool")

        if status is ManualAnalysisStatus.OK:
            if self.stopped:
                raise AnalysisRuntimeError(
                    "A successful analysis result cannot be stopped"
                )

            if not isinstance(
                self.analysis,
                LiquidityMovementAnalysisResult,
            ):
                raise AnalysisRuntimeError(
                    "A successful analysis requires a complete result"
                )

            if self.analysis.dataset.request != self.request:
                raise AnalysisRuntimeError(
                    "Analysis result request does not match runtime request"
                )

        else:
            if not self.stopped:
                raise AnalysisRuntimeError("A stopped result must set stopped=True")

            if self.analysis is not None:
                raise AnalysisRuntimeError(
                    "A stopped operation cannot publish a partial analysis"
                )

        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "ended_at", ended)
        object.__setattr__(self, "status", status)

    @property
    def analysis_id(self) -> str | None:
        return self.analysis.analysis_id if self.analysis is not None else None

    @property
    def candidate_count(self) -> int:
        return len(self.analysis.candidates) if self.analysis is not None else 0

    @property
    def selected_count(self) -> int:
        return len(self.analysis.selected) if self.analysis is not None else 0


@dataclass(frozen=True, slots=True)
class AnalysisRuntimeProgress:
    """Process-local progress suitable for NiceGUI polling."""

    operation_id: UUID
    phase: AnalysisRuntimePhase
    message: str
    completed_units: int
    total_units: int
    analysis_id: str | None = None
    current_timeframe_label: str | None = None
    current_metric: str | None = None

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            try:
                operation_id = UUID(str(operation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise AnalysisRuntimeError("operation_id must be a valid UUID") from exc

            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        try:
            phase = AnalysisRuntimePhase(self.phase)
        except (TypeError, ValueError) as exc:
            raise AnalysisRuntimeError("Unsupported analysis runtime phase") from exc

        message = str(self.message or "").strip()

        if not message:
            raise AnalysisRuntimeError(
                "Analysis runtime progress message cannot be blank"
            )

        for field_name in (
            "completed_units",
            "total_units",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise AnalysisRuntimeError(
                    f"{field_name} must be a non-negative integer"
                )

        if self.total_units <= 0:
            raise AnalysisRuntimeError("total_units must be positive")

        if self.completed_units > self.total_units:
            raise AnalysisRuntimeError("completed_units cannot exceed total_units")

        if self.analysis_id is not None:
            digest = str(self.analysis_id).strip()

            if (
                digest != digest.lower()
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise AnalysisRuntimeError(
                    "analysis_id must be null or a canonical lowercase SHA-256"
                )

            object.__setattr__(
                self,
                "analysis_id",
                digest,
            )

        timeframe = str(self.current_timeframe_label or "").strip()
        metric = str(self.current_metric or "").strip()

        object.__setattr__(
            self,
            "phase",
            phase,
        )
        object.__setattr__(
            self,
            "message",
            message,
        )
        object.__setattr__(
            self,
            "current_timeframe_label",
            timeframe or None,
        )
        object.__setattr__(
            self,
            "current_metric",
            metric or None,
        )

    @property
    def fraction_complete(self) -> float:
        return self.completed_units / self.total_units


@dataclass(frozen=True, slots=True)
class AnalysisRuntimeSnapshot:
    """Immutable process-local state returned to UI and health polling."""

    is_running: bool
    operation_id: str | None
    started_at: datetime | None
    stop_requested: bool
    latest_progress: AnalysisRuntimeProgress | None
    last_result: ManualAnalysisResult | None
    last_error: str | None
    completion_sequence: int


class AnalysisDatasetLoader(Protocol):
    """Synchronous verified-dataset loading boundary."""

    def __call__(
        self,
        request: AnalysisDatasetRequest,
    ) -> AlignedAnalysisDataset: ...


class AnalysisExecutor(Protocol):
    """Synchronous LM execution boundary used by the runtime."""

    def __call__(
        self,
        dataset: AlignedAnalysisDataset,
        *,
        config: LiquidityMovementAnalysisConfig,
        cancellation_probe: Callable[[], bool] | None,
        progress_sink: (
            Callable[
                [LiquidityMovementAnalysisProgress],
                object,
            ]
            | None
        ),
        cache: LiquidityMovementAnalysisCache | None,
    ) -> LiquidityMovementAnalysisResult: ...


def _production_dataset_loader(
    request: AnalysisDatasetRequest,
) -> AlignedAnalysisDataset:
    """Load and verify the dataset inside the current worker thread."""
    with session_scope() as session:
        return load_verified_analysis_dataset(
            session,
            request,
        )


def _production_executor(
    dataset: AlignedAnalysisDataset,
    *,
    config: LiquidityMovementAnalysisConfig,
    cancellation_probe: Callable[[], bool] | None,
    progress_sink: (
        Callable[
            [LiquidityMovementAnalysisProgress],
            object,
        ]
        | None
    ),
    cache: LiquidityMovementAnalysisCache | None,
) -> LiquidityMovementAnalysisResult:
    return execute_liquidity_movement_analysis(
        dataset,
        config=config,
        cancellation_probe=cancellation_probe,
        progress_sink=progress_sink,
        cache=cache,
    )


class ManualAnalysisRuntime:
    """Own one verified analysis operation and its immutable latest result."""

    def __init__(
        self,
        *,
        dataset_loader: AnalysisDatasetLoader = (_production_dataset_loader),
        executor: AnalysisExecutor = _production_executor,
        cache: LiquidityMovementAnalysisCache | None = None,
    ) -> None:
        if not callable(dataset_loader):
            raise TypeError("dataset_loader must be callable")

        if not callable(executor):
            raise TypeError("executor must be callable")

        if cache is not None and not isinstance(
            cache,
            LiquidityMovementAnalysisCache,
        ):
            raise TypeError("cache must be LiquidityMovementAnalysisCache or null")

        self._dataset_loader = dataset_loader
        self._executor = executor
        self._cache = (
            cache
            if cache is not None
            else LiquidityMovementAnalysisCache(
                maximum_entries=2,
            )
        )

        self._task: asyncio.Task[ManualAnalysisResult] | None = None
        self._cancellation_event: threading.Event | None = None

        self._operation_id: UUID | None = None
        self._started_at: datetime | None = None
        self._stop_requested = False
        self._latest_progress: AnalysisRuntimeProgress | None = None
        self._last_result: ManualAnalysisResult | None = None
        self._last_error: str | None = None
        self._completion_sequence = 0

        self._state_lock = threading.Lock()

    @property
    def task(self) -> asyncio.Task[ManualAnalysisResult] | None:
        return self._task

    @property
    def is_running(self) -> bool:
        task = self._task
        return task is not None and not task.done()

    @property
    def cache(self) -> LiquidityMovementAnalysisCache:
        return self._cache

    def snapshot(self) -> AnalysisRuntimeSnapshot:
        with self._state_lock:
            return AnalysisRuntimeSnapshot(
                is_running=self.is_running,
                operation_id=(
                    str(self._operation_id) if self._operation_id is not None else None
                ),
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
        request: AnalysisDatasetRequest,
        config: LiquidityMovementAnalysisConfig,
    ) -> asyncio.Task[ManualAnalysisResult]:
        """Start one application-owned analysis without queueing."""
        if not isinstance(request, AnalysisDatasetRequest):
            raise TypeError("request must be AnalysisDatasetRequest")

        if not isinstance(
            config,
            LiquidityMovementAnalysisConfig,
        ):
            raise TypeError("config must be LiquidityMovementAnalysisConfig")

        state = get_state()

        if state.shutdown_started:
            raise RuntimeError(
                "Application shutdown has started; new operations are blocked"
            )

        if self.is_running:
            raise AnalysisRuntimeBusyError(
                "A manual analysis operation is already active"
            )

        if state.active_operation_name:
            raise AnalysisRuntimeBusyError(
                f"Another operation is active: " f"{state.active_operation_name}"
            )

        operation_id = uuid4()
        started_at = now_utc()
        cancellation_event = threading.Event()

        with self._state_lock:
            self._operation_id = operation_id
            self._started_at = started_at
            self._stop_requested = False
            self._latest_progress = AnalysisRuntimeProgress(
                operation_id=operation_id,
                phase=AnalysisRuntimePhase.LOADING,
                message="Loading verified analytical data.",
                completed_units=0,
                total_units=1,
            )
            self._last_result = None
            self._last_error = None
            self._cancellation_event = cancellation_event

        state.active_operation_name = "manual_analysis"
        state.active_operation_started_at = started_at

        task = asyncio.create_task(
            self._run(
                operation_id=operation_id,
                started_at=started_at,
                request=request,
                config=config,
                cancellation_event=cancellation_event,
            ),
            name="l2shock-manual-analysis",
        )

        self._task = task
        state.tracked_tasks.add(task)

        return task

    def request_stop(self) -> bool:
        """Signal cooperative cancellation to the active worker."""
        with self._state_lock:
            cancellation_event = self._cancellation_event
            operation_id = self._operation_id

            if (
                not self.is_running
                or cancellation_event is None
                or cancellation_event.is_set()
                or operation_id is None
            ):
                return False

            cancellation_event.set()
            self._stop_requested = True

            previous = self._latest_progress

            self._latest_progress = AnalysisRuntimeProgress(
                operation_id=operation_id,
                phase=AnalysisRuntimePhase.STOPPING,
                message="Stopping analysis cooperatively.",
                completed_units=(
                    previous.completed_units if previous is not None else 0
                ),
                total_units=(previous.total_units if previous is not None else 1),
                analysis_id=(previous.analysis_id if previous is not None else None),
                current_timeframe_label=(
                    previous.current_timeframe_label if previous is not None else None
                ),
                current_metric=(
                    previous.current_metric if previous is not None else None
                ),
            )
            return True

    async def stop_and_wait(
        self,
        *,
        grace_seconds: float,
    ) -> bool:
        """Request cooperative cancellation and await bounded completion."""
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
            return True

        return True

    async def force_cancel_and_wait(
        self,
        *,
        timeout_seconds: float,
    ) -> bool:
        """Cancel the owner task after signalling the worker event."""
        task = self._task

        if task is None or task.done():
            return True

        self.request_stop()
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

    def _analysis_progress(
        self,
        event: LiquidityMovementAnalysisProgress,
    ) -> None:
        """Receive synchronous worker-thread progress safely."""
        if not isinstance(
            event,
            LiquidityMovementAnalysisProgress,
        ):
            raise TypeError("event must be LiquidityMovementAnalysisProgress")

        if event.phase.value == "cache_hit":
            phase = AnalysisRuntimePhase.CACHE_HIT
        elif event.phase.value == "completed":
            phase = AnalysisRuntimePhase.COMPLETED
        else:
            phase = AnalysisRuntimePhase.ANALYZING

        current_slice = event.current_slice

        with self._state_lock:
            operation_id = self._operation_id

            if operation_id is None:
                return

            self._latest_progress = AnalysisRuntimeProgress(
                operation_id=operation_id,
                phase=phase,
                message=event.message,
                completed_units=event.completed_units,
                total_units=event.total_units,
                analysis_id=event.analysis_id,
                current_timeframe_label=(
                    current_slice.timeframe_label if current_slice is not None else None
                ),
                current_metric=(
                    current_slice.metric.value if current_slice is not None else None
                ),
            )

    def _run_worker(
        self,
        *,
        request: AnalysisDatasetRequest,
        config: LiquidityMovementAnalysisConfig,
        cancellation_event: threading.Event,
    ) -> LiquidityMovementAnalysisResult:
        dataset = self._dataset_loader(request)

        if cancellation_event.is_set():
            raise LiquidityMovementAnalysisCancelledError(
                "Liquidity Movement analysis was cancelled after loading"
            )

        return self._executor(
            dataset,
            config=config,
            cancellation_probe=cancellation_event.is_set,
            progress_sink=self._analysis_progress,
            cache=self._cache,
        )

    async def _await_worker(
        self,
        function: Callable[[], LiquidityMovementAnalysisResult],
        *,
        cancellation_event: threading.Event,
    ) -> LiquidityMovementAnalysisResult:
        worker = asyncio.create_task(
            asyncio.to_thread(function),
            name="l2shock-analysis-worker",
        )

        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancellation_event.set()

            try:
                await asyncio.shield(worker)
            except LiquidityMovementAnalysisCancelledError:
                pass
            except Exception:
                log.exception(
                    "Analysis worker failed while responding to "
                    "owner-task cancellation."
                )

            raise

    async def _run(
        self,
        *,
        operation_id: UUID,
        started_at: datetime,
        request: AnalysisDatasetRequest,
        config: LiquidityMovementAnalysisConfig,
        cancellation_event: threading.Event,
    ) -> ManualAnalysisResult:
        state = get_state()
        current_task = asyncio.current_task()
        operation_lock = state.operation_lock
        acquired = False

        try:
            if operation_lock.locked():
                raise AnalysisRuntimeBusyError(
                    "Another application operation owns the process lock"
                )

            await operation_lock.acquire()
            acquired = True

            try:
                analysis = await self._await_worker(
                    lambda: self._run_worker(
                        request=request,
                        config=config,
                        cancellation_event=cancellation_event,
                    ),
                    cancellation_event=cancellation_event,
                )
            except LiquidityMovementAnalysisCancelledError:
                result = ManualAnalysisResult(
                    operation_id=operation_id,
                    request=request,
                    started_at=started_at,
                    ended_at=now_utc(),
                    status=ManualAnalysisStatus.STOPPED,
                    stopped=True,
                    analysis=None,
                )

                with self._state_lock:
                    self._last_result = result
                    self._last_error = None

                return result

            result = ManualAnalysisResult(
                operation_id=operation_id,
                request=request,
                started_at=started_at,
                ended_at=now_utc(),
                status=ManualAnalysisStatus.OK,
                stopped=False,
                analysis=analysis,
            )

            with self._state_lock:
                self._last_result = result
                self._last_error = None

            return result

        except asyncio.CancelledError:
            cancellation_event.set()

            with self._state_lock:
                self._last_error = (
                    "Manual analysis was cancelled during " "application shutdown"
                )

            raise

        except Exception as exc:
            with self._state_lock:
                if isinstance(
                    exc,
                    (
                        AnalysisRuntimeBusyError,
                        AnalysisRuntimeError,
                        LiquidityMovementAnalysisCancelledError,
                    ),
                ):
                    self._last_error = str(exc).strip() or type(exc).__name__
                else:
                    # Database/filesystem exceptions may contain sensitive
                    # implementation details. Keep UI state type-only.
                    self._last_error = f"Unexpected {type(exc).__name__}"

            log.exception("Manual analysis runtime failed.")
            raise

        finally:
            if acquired:
                operation_lock.release()

            with self._state_lock:
                self._completion_sequence += 1
                self._stop_requested = False
                self._cancellation_event = None
                self._operation_id = None

            if state.active_operation_name == "manual_analysis":
                state.active_operation_name = ""
                state.active_operation_started_at = None

            if current_task is not None:
                state.tracked_tasks.discard(current_task)

            if self._task is current_task:
                self._task = None


_runtime: ManualAnalysisRuntime | None = None
_runtime_loop: asyncio.AbstractEventLoop | None = None


def get_manual_analysis_runtime() -> ManualAnalysisRuntime:
    """Return the analysis runtime bound to the current event loop."""
    global _runtime, _runtime_loop

    loop = asyncio.get_running_loop()

    if _runtime is None:
        _runtime = ManualAnalysisRuntime()
        _runtime_loop = loop
        return _runtime

    if _runtime_loop is not loop:
        raise RuntimeError(
            "Manual analysis runtime belongs to another asyncio event loop"
        )

    return _runtime


def peek_manual_analysis_runtime() -> ManualAnalysisRuntime | None:
    """Return the existing runtime without constructing database services."""
    return _runtime


def reset_manual_analysis_runtime_for_tests() -> None:
    global _runtime, _runtime_loop
    _runtime = None
    _runtime_loop = None


__all__ = [
    "AnalysisRuntimeBusyError",
    "AnalysisRuntimeError",
    "AnalysisRuntimePhase",
    "AnalysisRuntimeProgress",
    "AnalysisRuntimeSnapshot",
    "ManualAnalysisResult",
    "ManualAnalysisRuntime",
    "ManualAnalysisStatus",
    "get_manual_analysis_runtime",
    "peek_manual_analysis_runtime",
    "reset_manual_analysis_runtime_for_tests",
]

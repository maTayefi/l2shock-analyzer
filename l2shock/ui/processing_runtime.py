# l2shock/ui/processing_runtime.py
"""Application-owned runtime for manual analytical source processing.

The processing coordinators are synchronous because PyArrow streaming,
order-book replay, exact Decimal arithmetic, and SQLAlchemy sessions are
synchronous boundaries.

This runtime executes each coordinator inside a worker thread while the NiceGUI
event loop remains responsive.

Ownership:

- the shared asyncio operation lock prevents concurrent fetch/processing work;
- a thread-safe Event provides cooperative cancellation to worker code;
- synchronous coordinator progress is transferred back to process-local state;
- durable analytical truth remains in PostgreSQL and immutable checkpoints;
- this module owns only current-process task/progress/result state.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol
from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.config import get_settings
from l2shock.presets import (
    build_binance_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.processing import (
    PriceProcessingRequest,
    PriceProcessingResult,
    ProcessingCancelledError,
    ProcessingError,
    ProcessingProgress,
    ProcessingQualityState,
    ProcessingRequest,
    ProcessingResult,
    create_production_l2_processing_coordinator,
    create_production_price_processing_coordinator,
)
from l2shock.processing.target_planning import (
    load_materialization_processing_targets,
)
from l2shock.timeutils import now_utc, require_aware_utc
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)


class ProcessingRuntimeBusyError(RuntimeError):
    """Another process-local operation currently owns operation admission."""


class ProcessingItemDisposition(StrEnum):
    COMPLETED = "completed"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class ProcessingItemResult:
    """Terminal result for one downloaded source archive."""

    target: SourceFileSpec
    disposition: ProcessingItemDisposition
    quality_state: ProcessingQualityState | None
    content_sha256: str | None
    inserted: bool | None
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.target, SourceFileSpec):
            raise TypeError("target must be a SourceFileSpec")

        object.__setattr__(
            self,
            "disposition",
            ProcessingItemDisposition(self.disposition),
        )

        if self.quality_state is not None:
            object.__setattr__(
                self,
                "quality_state",
                ProcessingQualityState(self.quality_state),
            )

        if self.content_sha256 is not None:
            digest = str(self.content_sha256).strip()

            if (
                digest != digest.lower()
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError(
                    "content_sha256 must be null or a canonical lowercase SHA-256"
                )

            object.__setattr__(self, "content_sha256", digest)

        if self.inserted is not None and not isinstance(self.inserted, bool):
            raise TypeError("inserted must be bool or null")

        diagnostic = str(self.diagnostic or "").strip()
        object.__setattr__(
            self,
            "diagnostic",
            diagnostic or None,
        )


@dataclass(frozen=True, slots=True)
class ManualProcessingResult:
    """Complete result of one manual processing-range operation."""

    operation_id: UUID
    requested_start_utc: datetime
    requested_end_utc: datetime
    started_at: datetime
    ended_at: datetime

    status: str
    items: tuple[ProcessingItemResult, ...]
    targets_selected: int
    completed_count: int
    failed_count: int
    stopped: bool

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            operation_id = UUID(str(operation_id))
            object.__setattr__(self, "operation_id", operation_id)

        start = require_aware_utc(
            "requested_start_utc",
            self.requested_start_utc,
        )
        end = require_aware_utc(
            "requested_end_utc",
            self.requested_end_utc,
        )
        started = require_aware_utc("started_at", self.started_at)
        ended = require_aware_utc("ended_at", self.ended_at)

        if end <= start:
            raise ValueError("requested_end_utc must be after requested_start_utc")

        if ended < started:
            raise ValueError("ended_at cannot precede started_at")

        object.__setattr__(self, "requested_start_utc", start)
        object.__setattr__(self, "requested_end_utc", end)
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "ended_at", ended)

        if self.status not in {
            "ok",
            "partial_ok",
            "error",
            "stopped",
            "no_work",
        }:
            raise ValueError("Unsupported manual processing status")

        for name in (
            "targets_selected",
            "completed_count",
            "failed_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

        if self.completed_count + self.failed_count != len(self.items):
            raise ValueError(
                "completed_count + failed_count must equal processed item count"
            )

        if len(self.items) > self.targets_selected:
            raise ValueError("Processed item count cannot exceed selected targets")

        if not isinstance(self.stopped, bool):
            raise TypeError("stopped must be bool")


@dataclass(frozen=True, slots=True)
class ProcessingRuntimeProgress:
    """Process-local progress suitable for UI polling."""

    operation_id: UUID
    message: str
    targets_selected: int
    targets_completed: int
    completed_count: int
    failed_count: int
    current_target: SourceFileSpec | None
    coordinator_phase: str | None

    @property
    def fraction_complete(self) -> float:
        if self.targets_selected <= 0:
            return 0.0

        return self.targets_completed / self.targets_selected


@dataclass(frozen=True, slots=True)
class ProcessingRuntimeSnapshot:
    is_running: bool
    operation_id: str | None
    started_at: datetime | None
    stop_requested: bool
    latest_progress: ProcessingRuntimeProgress | None
    last_result: ManualProcessingResult | None
    last_error: str | None
    completion_sequence: int


class DownloadedTargetLoader(Protocol):
    async def __call__(
        self,
        start_utc: datetime,
        end_utc: datetime,
        lower_depth_fraction: Decimal,
        upper_depth_fraction: Decimal,
    ) -> tuple[SourceFileSpec, ...]: ...


class L2CoordinatorProtocol(Protocol):
    def run(
        self,
        request: ProcessingRequest,
        preset,
        *,
        cancellation_probe: Callable[[], bool] | None = None,
    ) -> ProcessingResult: ...


class PriceCoordinatorProtocol(Protocol):
    def run(
        self,
        request: PriceProcessingRequest,
        *,
        cancellation_probe: Callable[[], bool] | None = None,
    ) -> PriceProcessingResult: ...


L2CoordinatorFactory = Callable[
    [Callable[[ProcessingProgress], object]],
    L2CoordinatorProtocol,
]
PriceCoordinatorFactory = Callable[
    [Callable[[ProcessingProgress], object]],
    PriceCoordinatorProtocol,
]


async def _production_target_loader(
    start_utc: datetime,
    end_utc: datetime,
    lower_depth_fraction: Decimal,
    upper_depth_fraction: Decimal,
) -> tuple[SourceFileSpec, ...]:
    return await asyncio.to_thread(
        load_materialization_processing_targets,
        start_utc,
        end_utc,
        lower_depth_fraction,
        upper_depth_fraction,
    )


def _production_l2_factory(
    progress_sink: Callable[[ProcessingProgress], object],
) -> L2CoordinatorProtocol:
    return create_production_l2_processing_coordinator(
        progress_sink=progress_sink,
    )


def _production_price_factory(
    progress_sink: Callable[[ProcessingProgress], object],
) -> PriceCoordinatorProtocol:
    return create_production_price_processing_coordinator(
        progress_sink=progress_sink,
    )


class ManualProcessingRuntime:
    """Own one manual source-processing operation."""

    def __init__(
        self,
        *,
        target_loader: DownloadedTargetLoader = _production_target_loader,
        l2_coordinator_factory: L2CoordinatorFactory = _production_l2_factory,
        price_coordinator_factory: PriceCoordinatorFactory = (
            _production_price_factory
        ),
    ) -> None:
        self._target_loader = target_loader
        self._l2_coordinator_factory = l2_coordinator_factory
        self._price_coordinator_factory = price_coordinator_factory

        self._task: asyncio.Task[ManualProcessingResult] | None = None
        self._cancellation_event: threading.Event | None = None

        self._operation_id: UUID | None = None
        self._started_at: datetime | None = None
        self._stop_requested = False
        self._latest_progress: ProcessingRuntimeProgress | None = None
        self._last_result: ManualProcessingResult | None = None
        self._last_error: str | None = None
        self._completion_sequence = 0

        self._state_lock = threading.Lock()

    @property
    def task(self) -> asyncio.Task[ManualProcessingResult] | None:
        return self._task

    @property
    def is_running(self) -> bool:
        task = self._task
        return task is not None and not task.done()

    def snapshot(self) -> ProcessingRuntimeSnapshot:
        with self._state_lock:
            return ProcessingRuntimeSnapshot(
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
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        lower_depth_fraction: Decimal,
        upper_depth_fraction: Decimal,
    ) -> asyncio.Task[ManualProcessingResult]:
        state = get_state()

        if state.shutdown_started:
            raise RuntimeError(
                "Application shutdown has started; new operations are blocked"
            )

        if self.is_running:
            raise ProcessingRuntimeBusyError(
                "A manual processing operation is already active"
            )

        if state.active_operation_name:
            raise ProcessingRuntimeBusyError(
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

        if not isinstance(lower_depth_fraction, Decimal):
            raise TypeError("lower_depth_fraction must be Decimal")

        if not isinstance(upper_depth_fraction, Decimal):
            raise TypeError("upper_depth_fraction must be Decimal")

        operation_id = uuid4()
        started_at = now_utc()
        cancellation_event = threading.Event()

        with self._state_lock:
            self._operation_id = operation_id
            self._started_at = started_at
            self._stop_requested = False
            self._latest_progress = None
            self._last_result = None
            self._last_error = None
            self._cancellation_event = cancellation_event

        state.active_operation_name = "manual_processing"
        state.active_operation_started_at = started_at

        task = asyncio.create_task(
            self._run(
                operation_id=operation_id,
                started_at=started_at,
                requested_start_utc=start,
                requested_end_utc=end,
                lower_depth_fraction=lower_depth_fraction,
                upper_depth_fraction=upper_depth_fraction,
                cancellation_event=cancellation_event,
            ),
            name="l2shock-manual-processing",
        )

        self._task = task
        state.tracked_tasks.add(task)
        return task

    def request_stop(self) -> bool:
        with self._state_lock:
            event = self._cancellation_event

            if not self.is_running or event is None or event.is_set():
                return False

            event.set()
            self._stop_requested = True
            return True

    async def stop_and_wait(
        self,
        *,
        grace_seconds: float,
    ) -> bool:
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

    def _coordinator_progress(
        self,
        event: ProcessingProgress,
    ) -> None:
        with self._state_lock:
            previous = self._latest_progress

            self._latest_progress = ProcessingRuntimeProgress(
                operation_id=event.operation_id,
                message=event.message,
                targets_selected=(
                    previous.targets_selected if previous is not None else 0
                ),
                targets_completed=(
                    previous.targets_completed if previous is not None else 0
                ),
                completed_count=(
                    previous.completed_count if previous is not None else 0
                ),
                failed_count=(previous.failed_count if previous is not None else 0),
                current_target=event.target,
                coordinator_phase=event.phase.value,
            )

    def _set_progress(
        self,
        *,
        operation_id: UUID,
        message: str,
        targets_selected: int,
        targets_completed: int,
        completed_count: int,
        failed_count: int,
        current_target: SourceFileSpec | None,
        coordinator_phase: str | None,
    ) -> None:
        with self._state_lock:
            self._latest_progress = ProcessingRuntimeProgress(
                operation_id=operation_id,
                message=message,
                targets_selected=targets_selected,
                targets_completed=targets_completed,
                completed_count=completed_count,
                failed_count=failed_count,
                current_target=current_target,
                coordinator_phase=coordinator_phase,
            )

    async def _run_sync_worker(
        self,
        function: Callable[[], ProcessingResult | PriceProcessingResult],
        *,
        cancellation_event: threading.Event,
    ) -> ProcessingResult | PriceProcessingResult:
        worker = asyncio.create_task(
            asyncio.to_thread(function),
            name="l2shock-processing-worker",
        )

        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancellation_event.set()

            try:
                await asyncio.shield(worker)
            except ProcessingCancelledError:
                pass
            except Exception:
                log.exception(
                    "Processing worker failed while responding to task cancellation."
                )

            raise

    async def _run(
        self,
        *,
        operation_id: UUID,
        started_at: datetime,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        lower_depth_fraction: Decimal,
        upper_depth_fraction: Decimal,
        cancellation_event: threading.Event,
    ) -> ManualProcessingResult:
        state = get_state()
        current_task = asyncio.current_task()
        items: list[ProcessingItemResult] = []
        stopped = False
        acquired_operation_lock = False

        try:
            operation_lock = state.operation_lock

            if operation_lock.locked():
                raise ProcessingRuntimeBusyError(
                    "Another application operation owns the process lock"
                )

            await operation_lock.acquire()
            acquired_operation_lock = True

            try:
                targets = await self._target_loader(
                    requested_start_utc,
                    requested_end_utc,
                    lower_depth_fraction,
                    upper_depth_fraction,
                )

                self._set_progress(
                    operation_id=operation_id,
                    message=(f"Selected {len(targets)} downloaded source archive(s)."),
                    targets_selected=len(targets),
                    targets_completed=0,
                    completed_count=0,
                    failed_count=0,
                    current_target=None,
                    coordinator_phase="planning",
                )

                settings = get_settings()

                l2_coordinator = self._l2_coordinator_factory(
                    self._coordinator_progress
                )
                price_coordinator = self._price_coordinator_factory(
                    self._coordinator_progress
                )

                failed_l2_chain_hours: dict[
                    tuple[str, str, str],
                    datetime,
                ] = {}

                for target in targets:
                    if cancellation_event.is_set():
                        stopped = True
                        break

                    l2_chain = (
                        (
                            target.provider,
                            target.venue,
                            target.symbol,
                        )
                        if target.data_kind is SourceDataKind.ORDERBOOK
                        else None
                    )

                    try:
                        if l2_chain is not None:
                            failed_hour = failed_l2_chain_hours.get(l2_chain)

                            if (
                                failed_hour is not None
                                and target.hour_utc > failed_hour
                            ):
                                raise ProcessingError(
                                    "A previous L2 hour in this exact "
                                    "venue/instrument chain failed; later "
                                    "checkpoint-dependent hours are blocked"
                                )

                        if target.data_kind is SourceDataKind.ORDERBOOK:
                            preset_builder = {
                                "binance_futures": (build_binance_futures_data_preset),
                                "bybit": build_bybit_data_preset,
                                "okx_futures": build_okx_futures_data_preset,
                            }.get(target.venue)

                            if preset_builder is None:
                                raise ProcessingError(
                                    "No production liquidity preset builder "
                                    f"exists for venue {target.venue!r}"
                                )

                            preset = preset_builder(
                                base=target.base,
                                lower_fraction=lower_depth_fraction,
                                upper_fraction=upper_depth_fraction,
                            )

                            result = await self._run_sync_worker(
                                lambda target=target, preset=preset: (
                                    l2_coordinator.run(
                                        ProcessingRequest(
                                            operation_id=operation_id,
                                            target=target,
                                            max_checkpoint_search_hours=(
                                                settings.processing.checkpoint_search_max_hours
                                            ),
                                        ),
                                        preset,
                                        cancellation_probe=(cancellation_event.is_set),
                                    )
                                ),
                                cancellation_event=cancellation_event,
                            )

                            assert isinstance(result, ProcessingResult)

                            item = ProcessingItemResult(
                                target=target,
                                disposition=(ProcessingItemDisposition.COMPLETED),
                                quality_state=result.quality_state,
                                content_sha256=(result.analytical_content_sha256),
                                inserted=result.analytical_inserted,
                            )
                        else:
                            result = await self._run_sync_worker(
                                lambda target=target: (
                                    price_coordinator.run(
                                        PriceProcessingRequest(
                                            operation_id=operation_id,
                                            target=target,
                                        ),
                                        cancellation_probe=(cancellation_event.is_set),
                                    )
                                ),
                                cancellation_event=cancellation_event,
                            )

                            assert isinstance(result, PriceProcessingResult)

                            item = ProcessingItemResult(
                                target=target,
                                disposition=(ProcessingItemDisposition.COMPLETED),
                                quality_state=result.quality_state,
                                content_sha256=(result.price_content_sha256),
                                inserted=result.price_inserted,
                            )

                        items.append(item)

                    except ProcessingCancelledError:
                        cancellation_event.set()
                        stopped = True
                        break

                    except ProcessingError as exc:
                        diagnostic = str(exc).strip() or type(exc).__name__

                        if l2_chain is not None:
                            previous_failed_hour = failed_l2_chain_hours.get(l2_chain)

                            if (
                                previous_failed_hour is None
                                or target.hour_utc < previous_failed_hour
                            ):
                                failed_l2_chain_hours[l2_chain] = target.hour_utc

                        items.append(
                            ProcessingItemResult(
                                target=target,
                                disposition=(ProcessingItemDisposition.ERROR),
                                quality_state=None,
                                content_sha256=None,
                                inserted=None,
                                diagnostic=diagnostic,
                            )
                        )
                        log.exception(
                            "Processing failed for %s.",
                            target.remote_path,
                        )

                    except Exception as exc:
                        diagnostic = f"Unexpected {type(exc).__name__}"

                        if l2_chain is not None:
                            previous_failed_hour = failed_l2_chain_hours.get(l2_chain)

                            if (
                                previous_failed_hour is None
                                or target.hour_utc < previous_failed_hour
                            ):
                                failed_l2_chain_hours[l2_chain] = target.hour_utc

                        items.append(
                            ProcessingItemResult(
                                target=target,
                                disposition=(ProcessingItemDisposition.ERROR),
                                quality_state=None,
                                content_sha256=None,
                                inserted=None,
                                diagnostic=diagnostic,
                            )
                        )
                        log.exception(
                            "Unexpected processing failure for %s.",
                            target.remote_path,
                        )

                    completed_count = sum(
                        item.disposition is ProcessingItemDisposition.COMPLETED
                        for item in items
                    )
                    failed_count = sum(
                        item.disposition is ProcessingItemDisposition.ERROR
                        for item in items
                    )

                    self._set_progress(
                        operation_id=operation_id,
                        message=(
                            f"Completed {len(items)} of {len(targets)} "
                            "selected source archives."
                        ),
                        targets_selected=len(targets),
                        targets_completed=len(items),
                        completed_count=completed_count,
                        failed_count=failed_count,
                        current_target=target,
                        coordinator_phase="item_complete",
                    )

                completed_count = sum(
                    item.disposition is ProcessingItemDisposition.COMPLETED
                    for item in items
                )
                failed_count = sum(
                    item.disposition is ProcessingItemDisposition.ERROR
                    for item in items
                )

                if stopped:
                    status = "stopped"
                elif not targets:
                    status = "no_work"
                elif failed_count == 0:
                    status = "ok"
                elif completed_count > 0:
                    status = "partial_ok"
                else:
                    status = "error"

                result = ManualProcessingResult(
                    operation_id=operation_id,
                    requested_start_utc=requested_start_utc,
                    requested_end_utc=requested_end_utc,
                    started_at=started_at,
                    ended_at=now_utc(),
                    status=status,
                    items=tuple(items),
                    targets_selected=len(targets),
                    completed_count=completed_count,
                    failed_count=failed_count,
                    stopped=stopped,
                )

                with self._state_lock:
                    self._last_result = result
                    self._last_error = None

                return result

            finally:
                if acquired_operation_lock:
                    operation_lock.release()
                    acquired_operation_lock = False

        except asyncio.CancelledError:
            cancellation_event.set()

            with self._state_lock:
                self._last_error = (
                    "Manual processing was cancelled during application shutdown"
                )

            raise

        except Exception as exc:
            with self._state_lock:
                if isinstance(exc, ProcessingError):
                    self._last_error = str(exc).strip() or type(exc).__name__
                else:
                    self._last_error = f"Unexpected {type(exc).__name__}"

            log.exception("Manual processing runtime failed.")
            raise

        finally:
            with self._state_lock:
                self._completion_sequence += 1
                self._stop_requested = False
                self._cancellation_event = None
                self._operation_id = None

            # Clear only operation ownership still belonging to this runtime.
            if state.active_operation_name == "manual_processing":
                state.active_operation_name = ""
                state.active_operation_started_at = None

            if current_task is not None:
                state.tracked_tasks.discard(current_task)

            if self._task is current_task:
                self._task = None


_runtime: ManualProcessingRuntime | None = None
_runtime_loop: asyncio.AbstractEventLoop | None = None


def get_manual_processing_runtime() -> ManualProcessingRuntime:
    global _runtime, _runtime_loop

    loop = asyncio.get_running_loop()

    if _runtime is None:
        _runtime = ManualProcessingRuntime()
        _runtime_loop = loop
        return _runtime

    if _runtime_loop is not loop:
        raise RuntimeError(
            "Manual processing runtime belongs to another asyncio event loop"
        )

    return _runtime


def peek_manual_processing_runtime() -> ManualProcessingRuntime | None:
    return _runtime


def reset_manual_processing_runtime_for_tests() -> None:
    global _runtime, _runtime_loop
    _runtime = None
    _runtime_loop = None


__all__ = [
    "ManualProcessingResult",
    "ManualProcessingRuntime",
    "ProcessingItemDisposition",
    "ProcessingItemResult",
    "ProcessingRuntimeBusyError",
    "ProcessingRuntimeProgress",
    "ProcessingRuntimeSnapshot",
    "get_manual_processing_runtime",
    "peek_manual_processing_runtime",
    "reset_manual_processing_runtime_for_tests",
]

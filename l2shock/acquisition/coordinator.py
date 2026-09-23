# l2shock/acquisition/coordinator.py
"""Manual CryptoHFTData fetch coordination.

The coordinator owns operational sequencing only:

    plan source identities
    -> create fetch run
    -> persist downloading state
    -> invoke downloader
    -> persist terminal source outcome
    -> complete fetch run

It does not read Parquet events, reconstruct books, calculate liquidity, or
promote analytical quality.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import (
    Awaitable,
    Callable,
)
from contextlib import (
    AbstractAsyncContextManager as AsyncContextManager,
    asynccontextmanager,
)
from datetime import datetime
from typing import Protocol
from uuid import UUID, uuid4

from l2shock.acquisition.downloader import CryptoHFTDownloader
from l2shock.acquisition.errors import (
    AcquisitionCancelledError,
    AcquisitionError,
    RemoteFileNotFoundError,
)
from l2shock.acquisition.fetch_models import (
    FetchItemDisposition,
    FetchItemResult,
    FetchProgress,
    FetchProgressPhase,
    ManualFetchResult,
)
from l2shock.acquisition.fetch_persistence import (
    FetchPersistenceProtocol,
    SQLAlchemyFetchPersistence,
)
from l2shock.acquisition.models import SourceFileSpec
from l2shock.acquisition.persistence import (
    FetchRunKind,
    FetchRunStatus,
    bounded_diagnostic_text,
)
from l2shock.acquisition.planning import (
    plan_binance_futures_files,
    plan_production_source_files,
)
from l2shock.acquisition.results import (
    DownloadArtifact,
    DownloadDisposition,
)
from l2shock.config import get_settings
from l2shock.timeutils import now_utc, require_aware_utc

log = logging.getLogger(__name__)


class FetchOperationBusyError(RuntimeError):
    """Another process-local operation currently owns the operation lock."""


class DownloaderProtocol(Protocol):
    async def download(
        self,
        spec: SourceFileSpec,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> DownloadArtifact: ...


DownloaderFactory = Callable[
    [],
    AsyncContextManager[DownloaderProtocol],
]

ProgressSink = Callable[
    [FetchProgress],
    Awaitable[None] | None,
]

SourcePlanner = Callable[
    [datetime, datetime],
    tuple[SourceFileSpec, ...],
]


def _safe_unexpected_error(exc: BaseException) -> str:
    """Describe an unexpected failure without exposing exception arguments.

    AcquisitionError instances are already required to be secret-safe. Other
    exception messages may contain a URL, database DSN, or request object, so
    only their type is exposed here.
    """
    if isinstance(exc, AcquisitionError):
        return bounded_diagnostic_text(exc) or type(exc).__name__

    return f"Unexpected {type(exc).__name__}"


def _terminal_fetch_status(
    *,
    stopped: bool,
    successful: int,
    missing: int,
    failed: int,
) -> FetchRunStatus:
    if stopped:
        return FetchRunStatus.STOPPED

    unsuccessful = missing + failed

    if unsuccessful == 0:
        return FetchRunStatus.OK

    if successful > 0:
        return FetchRunStatus.PARTIAL_OK

    return FetchRunStatus.ERROR


class ManualFetchCoordinator:
    """Run one globally planned manual fetch at a time.

    The supplied operation lock must be the shared process lock from
    RuntimeState.operation_lock in production. Tests may provide an isolated
    asyncio.Lock.
    """

    def __init__(
        self,
        *,
        operation_lock: asyncio.Lock,
        persistence: FetchPersistenceProtocol,
        downloader_factory: DownloaderFactory,
        progress_sink: ProgressSink | None = None,
        source_planner: SourcePlanner = plan_binance_futures_files,
    ) -> None:
        if not isinstance(operation_lock, asyncio.Lock):
            raise TypeError("operation_lock must be an asyncio.Lock")

        if not callable(downloader_factory):
            raise TypeError("downloader_factory must be callable")

        if not callable(source_planner):
            raise TypeError("source_planner must be callable")

        self._operation_lock = operation_lock
        self._persistence = persistence
        self._downloader_factory = downloader_factory
        self._progress_sink = progress_sink
        self._source_planner = source_planner

        self._active_cancel_event: asyncio.Event | None = None
        self._active_operation_id: UUID | None = None

    @property
    def active_operation_id(self) -> UUID | None:
        return self._active_operation_id

    @property
    def is_running(self) -> bool:
        return self._active_operation_id is not None

    def request_stop(self) -> bool:
        """Request cooperative cancellation of the active fetch operation."""
        cancel_event = self._active_cancel_event

        if cancel_event is None or cancel_event.is_set():
            return False

        cancel_event.set()
        return True

    async def _emit(
        self,
        event: FetchProgress,
    ) -> None:
        sink = self._progress_sink

        if sink is None:
            return

        try:
            result = sink(event)

            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            # UI/progress rendering must not corrupt acquisition truth.
            log.exception(
                "Fetch progress sink failed for operation %s.",
                event.operation_id,
            )

    async def _record_interrupted_source(
        self,
        spec: SourceFileSpec,
    ) -> None:
        """Best-effort closure of a source left in downloading state.

        The current schema has no dedicated cancelled source-hour status.
        Recording a bounded operational error is preferable to leaving the row
        indefinitely marked downloading. Failure here must not prevent the
        enclosing fetch run from being finalized as stopped.
        """
        task = asyncio.create_task(
            self._persistence.record_error(
                spec,
                message="Fetch operation stopped before source completion",
            ),
            name=f"persist-interrupted-source-{spec.symbol}",
        )

        cancellation_count = 0

        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancellation_count += 1

                # The persistence task is still alive because shield prevents
                # cancellation from propagating into it. Continue joining it
                # so fetch ownership cannot end while the source-state write
                # remains outstanding.
                continue
            except Exception:
                log.exception(
                    "Could not persist interrupted source state for %s.",
                    spec.remote_path,
                )
                return

        if cancellation_count > 1:
            log.warning(
                "Repeated cancellation was deferred until source-state "
                "cleanup completed for %s.",
                spec.remote_path,
            )

        if task.cancelled():
            log.warning(
                "Interrupted source-state cleanup task was cancelled for %s.",
                spec.remote_path,
            )
            return

        try:
            task.result()
        except Exception:
            log.exception(
                "Could not persist interrupted source state for %s.",
                spec.remote_path,
            )

    async def run(
        self,
        *,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        run_kind: FetchRunKind | str = FetchRunKind.MANUAL,
    ) -> ManualFetchResult:
        """Run one global BTC/ETH Binance Futures fetch.

        ``run_kind`` changes operational fetch-run provenance only. Manual and
        automatic fetches use the same planner, downloader, validation,
        persistence, cancellation, and source-hour state transitions.

        The requested range uses half-open interval semantics:

            [requested_start_utc, requested_end_utc)
        """
        try:
            normalized_run_kind = FetchRunKind(run_kind)
        except (TypeError, ValueError) as exc:
            raise ValueError("run_kind is unsupported") from exc
        start_utc = require_aware_utc(
            "requested_start_utc",
            requested_start_utc,
        )
        end_utc = require_aware_utc(
            "requested_end_utc",
            requested_end_utc,
        )

        if end_utc <= start_utc:
            raise ValueError("requested_end_utc must be after requested_start_utc")

        plan = self._source_planner(
            start_utc,
            end_utc,
        )

        if not plan:
            raise ValueError("The requested range produced no source files")

        # This check and acquire contain no intervening await. In one event
        # loop, another coroutine cannot acquire the lock between them.
        if self._operation_lock.locked():
            raise FetchOperationBusyError(
                "Another application operation is already active"
            )

        await self._operation_lock.acquire()

        operation_id = uuid4()
        cancel_event = asyncio.Event()
        started_at = now_utc()

        self._active_operation_id = operation_id
        self._active_cancel_event = cancel_event

        try:
            return await self._run_locked(
                operation_id=operation_id,
                started_at=started_at,
                start_utc=start_utc,
                end_utc=end_utc,
                plan=plan,
                cancel_event=cancel_event,
                run_kind=normalized_run_kind,
            )
        finally:
            self._active_cancel_event = None
            self._active_operation_id = None
            self._operation_lock.release()

    async def _run_locked(
        self,
        *,
        operation_id: UUID,
        started_at: datetime,
        start_utc: datetime,
        end_utc: datetime,
        plan: tuple[SourceFileSpec, ...],
        cancel_event: asyncio.Event,
        run_kind: FetchRunKind,
    ) -> ManualFetchResult:
        files_requested = len(plan)
        planned_venues = sorted({spec.venue for spec in plan})
        planned_markets = sorted({f"{spec.venue}/{spec.symbol}" for spec in plan})
        planned_data_kinds = sorted({spec.data_kind.value for spec in plan})
        downloaded = 0
        reused = 0
        missing = 0
        failed = 0
        stopped = False
        fatal_diagnostic: str | None = None
        items: list[FetchItemResult] = []

        await self._emit(
            self._progress(
                operation_id=operation_id,
                phase=FetchProgressPhase.PLANNING,
                message=(f"Planned {files_requested} hourly source files."),
                files_requested=files_requested,
                items=items,
            )
        )

        native_cancellation: asyncio.CancelledError | None = None

        creation_task = asyncio.create_task(
            self._persistence.create_fetch_run(
                operation_id=operation_id,
                kind=run_kind,
                requested_start_utc=start_utc,
                requested_end_utc=end_utc,
                files_requested=files_requested,
                details={
                    "provider": "cryptohftdata",
                    "venues": planned_venues,
                    "markets": planned_markets,
                    "bases": ["BTC", "ETH"],
                    "data_kinds": planned_data_kinds,
                    "range_semantics": "[start_utc, end_utc)",
                    "phase": "started",
                },
            ),
            name=f"persist-fetch-creation-{operation_id}",
        )

        # The production coroutine awaits a worker-thread transaction.
        # Cancellation of this waiter must not release operation ownership
        # before we know whether a running fetch-run row was committed.
        while not creation_task.done():
            try:
                await asyncio.shield(creation_task)
            except asyncio.CancelledError as exc:
                if native_cancellation is None:
                    native_cancellation = exc
                cancel_event.set()

        # If creation failed, there is no confirmed run to finalize. Propagate
        # that failure rather than issuing a completion for an unknown row.
        creation_task.result()

        try:
            if native_cancellation is not None:
                raise native_cancellation

            await self._emit(
                self._progress(
                    operation_id=operation_id,
                    phase=FetchProgressPhase.STARTED,
                    message=f"{run_kind.value.capitalize()} fetch started.",
                    files_requested=files_requested,
                    items=items,
                )
            )

            async with self._downloader_factory() as downloader:
                for spec in plan:
                    if cancel_event.is_set():
                        stopped = True
                        break

                    admission_task = asyncio.create_task(
                        self._persistence.mark_downloading(spec),
                        name=f"persist-fetch-admission-{spec.symbol}",
                    )

                    # Shielding alone is insufficient: a second cancellation
                    # can interrupt the waiter while its DB thread continues.
                    while not admission_task.done():
                        try:
                            await asyncio.shield(admission_task)
                        except asyncio.CancelledError as exc:
                            if native_cancellation is None:
                                native_cancellation = exc
                            cancel_event.set()
                            stopped = True

                    already_processed = bool(admission_task.result())

                    if native_cancellation is not None:
                        if not already_processed:
                            await self._record_interrupted_source(spec)
                        break

                    try:
                        await self._emit(
                            self._progress(
                                operation_id=operation_id,
                                phase=FetchProgressPhase.DOWNLOADING,
                                message=f"Fetching {spec.remote_path}",
                                files_requested=files_requested,
                                items=items,
                                current_spec=spec,
                            )
                        )
                    except asyncio.CancelledError as exc:
                        cancel_event.set()
                        stopped = True
                        native_cancellation = exc
                        if not already_processed:
                            await self._record_interrupted_source(spec)
                        break

                    try:
                        artifact = await downloader.download(
                            spec,
                            cancel_event=cancel_event,
                        )
                    except AcquisitionCancelledError:
                        stopped = True
                        cancel_event.set()
                        if not already_processed:
                            await self._record_interrupted_source(spec)
                        break
                    except RemoteFileNotFoundError as exc:
                        diagnostic = (
                            bounded_diagnostic_text(exc)
                            or "Remote hourly archive is unavailable"
                        )

                        if not already_processed:
                            await self._persistence.record_missing(
                                spec,
                                message=diagnostic,
                            )

                        item = FetchItemResult(
                            spec=spec,
                            disposition=FetchItemDisposition.MISSING,
                            diagnostic=diagnostic,
                        )
                        items.append(item)
                        missing += 1

                        await self._emit(
                            self._progress(
                                operation_id=operation_id,
                                phase=FetchProgressPhase.FILE_MISSING,
                                message=(
                                    f"Remote file is unavailable: "
                                    f"{spec.remote_path}"
                                ),
                                files_requested=files_requested,
                                items=items,
                                current_spec=spec,
                            )
                        )
                    except AcquisitionError as exc:
                        diagnostic = _safe_unexpected_error(exc)

                        if not already_processed:
                            await self._persistence.record_error(
                                spec,
                                message=diagnostic,
                            )

                        item = FetchItemResult(
                            spec=spec,
                            disposition=FetchItemDisposition.ERROR,
                            diagnostic=diagnostic,
                        )
                        items.append(item)
                        failed += 1

                        await self._emit(
                            self._progress(
                                operation_id=operation_id,
                                phase=FetchProgressPhase.FILE_ERROR,
                                message=(
                                    f"Fetch failed for {spec.remote_path}: "
                                    f"{diagnostic}"
                                ),
                                files_requested=files_requested,
                                items=items,
                                current_spec=spec,
                            )
                        )
                    except asyncio.CancelledError as exc:
                        cancel_event.set()
                        stopped = True
                        native_cancellation = exc
                        if not already_processed:
                            await self._record_interrupted_source(spec)
                        break
                    except Exception as exc:
                        # Do not expose arbitrary exception text. An httpx
                        # request or DB exception may include credentials.
                        diagnostic = _safe_unexpected_error(exc)

                        if not already_processed:
                            await self._persistence.record_error(
                                spec,
                                message=diagnostic,
                            )

                        item = FetchItemResult(
                            spec=spec,
                            disposition=FetchItemDisposition.ERROR,
                            diagnostic=diagnostic,
                        )
                        items.append(item)
                        failed += 1

                        log.exception(
                            "Unexpected source fetch failure for %s.",
                            spec.remote_path,
                        )

                        await self._emit(
                            self._progress(
                                operation_id=operation_id,
                                phase=FetchProgressPhase.FILE_ERROR,
                                message=(
                                    f"Fetch failed for {spec.remote_path}: "
                                    f"{diagnostic}"
                                ),
                                files_requested=files_requested,
                                items=items,
                                current_spec=spec,
                            )
                        )
                    else:
                        await self._persistence.record_artifact(artifact)

                        if artifact.disposition is DownloadDisposition.DOWNLOADED:
                            disposition = FetchItemDisposition.DOWNLOADED
                            downloaded += 1
                        else:
                            disposition = FetchItemDisposition.REUSED
                            reused += 1

                        items.append(
                            FetchItemResult(
                                spec=spec,
                                disposition=disposition,
                                local_path=artifact.local_path,
                                file_size_bytes=(artifact.file_size_bytes),
                                content_sha256=(artifact.content_sha256),
                                attempts=artifact.attempts,
                            )
                        )

                        await self._emit(
                            self._progress(
                                operation_id=operation_id,
                                phase=(FetchProgressPhase.FILE_COMPLETE),
                                message=(
                                    f"Source file available: " f"{spec.remote_path}"
                                ),
                                files_requested=files_requested,
                                items=items,
                                current_spec=spec,
                            )
                        )
        except asyncio.CancelledError as exc:
            cancel_event.set()
            stopped = True
            native_cancellation = exc
        except Exception as exc:
            # Per-file acquisition failures are handled inside the loop.
            # Reaching this boundary means orchestration, persistence, or
            # downloader-context setup failed. Arbitrary exception arguments
            # remain excluded because they may contain a DSN or request data.
            fatal_diagnostic = _safe_unexpected_error(exc)

            log.exception(
                "Manual fetch coordinator failed for operation %s.",
                operation_id,
            )

        if stopped and native_cancellation is None:
            try:
                await self._emit(
                    self._progress(
                        operation_id=operation_id,
                        phase=FetchProgressPhase.STOPPING,
                        message="Manual fetch is stopping safely.",
                        files_requested=files_requested,
                        items=items,
                    )
                )
            except asyncio.CancelledError as exc:
                # Progress is not durable truth. Continue to the existing
                # shielded fetch-run completion before propagating cancellation.
                cancel_event.set()
                native_cancellation = exc

        successful = downloaded + reused

        if fatal_diagnostic is not None:
            terminal_status = FetchRunStatus.ERROR
        else:
            terminal_status = _terminal_fetch_status(
                stopped=stopped,
                successful=successful,
                missing=missing,
                failed=failed,
            )

        details = {
            "provider": "cryptohftdata",
            "venues": planned_venues,
            "markets": planned_markets,
            "bases": ["BTC", "ETH"],
            "data_kinds": planned_data_kinds,
            "phase": "completed",
            "files_requested": files_requested,
            "files_attempted": len(items),
            "files_downloaded": downloaded,
            "files_reused": reused,
            "files_available": successful,
            "files_missing": missing,
            "files_failed": failed,
            "files_unattempted": files_requested - len(items),
            "stopped": stopped,
            "fatal_error": fatal_diagnostic,
        }

        error_text: str | None = None

        if stopped:
            error_text = "Fetch operation was stopped"
        elif fatal_diagnostic is not None:
            error_text = fatal_diagnostic
        elif missing or failed:
            error_text = (
                f"{missing} source files were unavailable and "
                f"{failed} source files failed"
            )

        completion_task = asyncio.create_task(
            self._persistence.complete_fetch_run(
                operation_id=operation_id,
                status=terminal_status,
                # The database's existing files_downloaded field means
                # successful local availability at this orchestration stage.
                # The downloaded/reused split is retained in details_json.
                files_downloaded=successful,
                files_processed=0,
                files_failed=missing + failed,
                details=details,
                error_text=error_text,
            ),
            name=f"persist-fetch-completion-{operation_id}",
        )

        # Shield the final DB transaction from task cancellation. A cancelled
        # waiter cannot cancel the shielded persistence task, so retain fetch
        # ownership through every repeated cancellation until it finishes.
        while not completion_task.done():
            try:
                await asyncio.shield(completion_task)
            except asyncio.CancelledError as exc:
                if native_cancellation is None:
                    native_cancellation = exc
                continue

        # Do not report completion or release the operation lock if durable
        # fetch-run finalization failed.
        completion_task.result()

        ended_at = now_utc()

        result = ManualFetchResult(
            operation_id=operation_id,
            started_at=started_at,
            ended_at=ended_at,
            requested_start_utc=start_utc,
            requested_end_utc=end_utc,
            status=terminal_status.value,
            items=tuple(items),
            files_requested=files_requested,
            files_downloaded=downloaded,
            files_reused=reused,
            files_missing=missing,
            files_failed=failed,
            stopped=stopped,
            details=details,
        )

        await self._emit(
            self._progress(
                operation_id=operation_id,
                phase=FetchProgressPhase.COMPLETED,
                message=(
                    "Manual fetch stopped."
                    if stopped
                    else (
                        "Manual fetch completed with status "
                        f"{terminal_status.value}."
                    )
                ),
                files_requested=files_requested,
                items=items,
            )
        )

        if native_cancellation is not None:
            raise native_cancellation

        return result

    @staticmethod
    def _progress(
        *,
        operation_id: UUID,
        phase: FetchProgressPhase,
        message: str,
        files_requested: int,
        items: list[FetchItemResult],
        current_spec: SourceFileSpec | None = None,
    ) -> FetchProgress:
        return FetchProgress(
            operation_id=operation_id,
            phase=phase,
            message=message,
            files_requested=files_requested,
            files_completed=len(items),
            files_downloaded=sum(
                item.disposition is FetchItemDisposition.DOWNLOADED for item in items
            ),
            files_reused=sum(
                item.disposition is FetchItemDisposition.REUSED for item in items
            ),
            files_missing=sum(
                item.disposition is FetchItemDisposition.MISSING for item in items
            ),
            files_failed=sum(
                item.disposition is FetchItemDisposition.ERROR for item in items
            ),
            current_spec=current_spec,
        )


def create_production_manual_fetch_coordinator(
    *,
    operation_lock: asyncio.Lock,
    progress_sink: ProgressSink | None = None,
    use_api_key: bool = False,
) -> ManualFetchCoordinator:
    """Build the production coordinator from current application settings."""
    settings = get_settings()
    api_key = settings.cryptohft.api_key.get_secret_value()

    persistence = SQLAlchemyFetchPersistence(
        diagnostic_secrets=(api_key,),
    )

    @asynccontextmanager
    async def _downloader_factory():
        async with CryptoHFTDownloader(
            cryptohft=settings.cryptohft,
            storage=settings.storage,
            use_api_key=use_api_key,
        ) as downloader:
            yield downloader

    return ManualFetchCoordinator(
        operation_lock=operation_lock,
        persistence=persistence,
        downloader_factory=_downloader_factory,
        progress_sink=progress_sink,
        source_planner=plan_production_source_files,
    )


__all__ = [
    "DownloaderFactory",
    "DownloaderProtocol",
    "FetchOperationBusyError",
    "ManualFetchCoordinator",
    "ProgressSink",
    "SourcePlanner",
    "create_production_manual_fetch_coordinator",
]

# l2shock/ui/remote_import_runtime.py
"""Application-owned runtime for pinned Hugging Face range imports.

One operation:

1. validates an exact UTC range and selected BTC/ETH bases;
2. constructs the exact component L2 and Binance price artifact keys;
3. resolves one immutable Hugging Face commit SHA;
4. downloads every artifact and manifest from that same revision;
5. delegates PostgreSQL writes to the existing verified importer;
6. exposes immutable progress and terminal snapshots.

The runtime does not replay order books, calculate liquidity, reconstruct price
OHLC, publish to Hugging Face, or implement another analytical codec.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Protocol
from uuid import UUID, uuid4

from l2shock.db.engine import session_scope
from l2shock.presets import (
    build_binance_futures_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
)
from l2shock.remote.hf_repository import (
    DownloadedHuggingFaceArtifact,
    HuggingFaceDatasetRepository,
)
from l2shock.remote.importer import (
    RemoteArtifactImportResult,
    import_downloaded_huggingface_artifact,
)
from l2shock.timeutils import (
    now_utc,
    require_utc_hour,
)
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)

_SUPPORTED_BASES = ("BTC", "ETH")


class RemoteImportRuntimeError(RuntimeError):
    """A remote range-import operation could not be completed safely."""


class RemoteImportRuntimeBusyError(RemoteImportRuntimeError):
    """Another operation currently owns process-local admission."""


class RemoteImportItemDisposition(StrEnum):
    IMPORTED = "imported"
    REUSED = "reused"
    MISSING = "missing"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class RemoteImportItemResult:
    """Terminal result for one planned remote artifact."""

    key: RemoteArtifactKey
    disposition: RemoteImportItemDisposition
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, RemoteArtifactKey):
            raise TypeError("key must be a RemoteArtifactKey")

        object.__setattr__(
            self,
            "disposition",
            RemoteImportItemDisposition(self.disposition),
        )

        diagnostic = str(self.diagnostic or "").strip()
        object.__setattr__(
            self,
            "diagnostic",
            diagnostic or None,
        )


@dataclass(frozen=True, slots=True)
class RemoteImportRangeResult:
    """Terminal result of one pinned-revision range import."""

    operation_id: UUID
    requested_start_utc: datetime
    requested_end_utc: datetime
    selected_bases: tuple[str, ...]
    pinned_revision: str
    started_at: datetime
    ended_at: datetime
    status: str
    items: tuple[RemoteImportItemResult, ...]
    artifacts_selected: int
    imported_count: int
    reused_count: int
    missing_count: int
    failed_count: int
    stopped: bool

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            operation_id = UUID(str(operation_id))
            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        start = require_utc_hour(
            "requested_start_utc",
            self.requested_start_utc,
        )
        end = require_utc_hour(
            "requested_end_utc",
            self.requested_end_utc,
        )

        if end <= start:
            raise ValueError("requested_end_utc must be after requested_start_utc")

        started_at = _require_aware_utc(
            "started_at",
            self.started_at,
        )
        ended_at = _require_aware_utc(
            "ended_at",
            self.ended_at,
        )

        if ended_at < started_at:
            raise ValueError("ended_at cannot precede started_at")

        bases = _normalized_bases(self.selected_bases)
        revision = _full_commit_sha(self.pinned_revision)

        if self.status not in {
            "ok",
            "partial_ok",
            "error",
            "stopped",
            "no_work",
        }:
            raise ValueError("Unsupported remote import status")

        for name in (
            "artifacts_selected",
            "imported_count",
            "reused_count",
            "missing_count",
            "failed_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

        terminal_count = (
            self.imported_count
            + self.reused_count
            + self.missing_count
            + self.failed_count
        )

        if terminal_count != len(self.items):
            raise ValueError("Remote import result counts must equal item count")

        if len(self.items) > self.artifacts_selected:
            raise ValueError("Processed item count cannot exceed selected artifacts")

        if not isinstance(self.stopped, bool):
            raise TypeError("stopped must be bool")

        object.__setattr__(
            self,
            "requested_start_utc",
            start,
        )
        object.__setattr__(
            self,
            "requested_end_utc",
            end,
        )
        object.__setattr__(
            self,
            "selected_bases",
            bases,
        )
        object.__setattr__(
            self,
            "pinned_revision",
            revision,
        )
        object.__setattr__(
            self,
            "started_at",
            started_at,
        )
        object.__setattr__(
            self,
            "ended_at",
            ended_at,
        )


@dataclass(frozen=True, slots=True)
class RemoteImportRuntimeProgress:
    """Process-local progress suitable for UI polling."""

    operation_id: UUID
    message: str
    pinned_revision: str
    artifacts_selected: int
    artifacts_completed: int
    imported_count: int
    reused_count: int
    missing_count: int
    failed_count: int
    current_key: RemoteArtifactKey | None

    @property
    def fraction_complete(self) -> float:
        if self.artifacts_selected <= 0:
            return 0.0

        return self.artifacts_completed / self.artifacts_selected


@dataclass(frozen=True, slots=True)
class RemoteImportRuntimeSnapshot:
    """Immutable snapshot of the current process-local runtime."""

    is_running: bool
    operation_id: str | None
    started_at: datetime | None
    stop_requested: bool
    pinned_revision: str | None
    latest_progress: RemoteImportRuntimeProgress | None
    last_result: RemoteImportRangeResult | None
    last_error: str | None
    completion_sequence: int


class RemoteImportRepositoryProtocol(Protocol):
    def current_revision(self) -> str: ...

    def download_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> DownloadedHuggingFaceArtifact | None: ...


RemoteArtifactImporter = Callable[
    [DownloadedHuggingFaceArtifact],
    RemoteArtifactImportResult,
]


def _require_aware_utc(
    field_name: str,
    value: datetime,
) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware")

    if value.utcoffset().total_seconds() != 0:
        raise ValueError(f"{field_name} must have UTC offset +00:00")

    return value


def _full_commit_sha(value: object) -> str:
    revision = str(value or "").strip().lower()

    if len(revision) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in revision
    ):
        raise RemoteImportRuntimeError(
            "Pinned revision must be a full canonical commit SHA"
        )

    return revision


def _normalized_bases(
    bases: Iterable[str],
) -> tuple[str, ...]:
    if isinstance(bases, str):
        raw_bases = (bases,)
    else:
        raw_bases = tuple(bases)

    normalized = {str(base or "").strip().upper() for base in raw_bases}

    if not normalized:
        raise ValueError("At least one base must be selected")

    unsupported = normalized.difference(_SUPPORTED_BASES)

    if unsupported:
        raise ValueError("Remote import supports only BTC and ETH bases")

    return tuple(base for base in _SUPPORTED_BASES if base in normalized)


def plan_remote_import_keys(
    *,
    requested_start_utc: datetime,
    requested_end_utc: datetime,
    bases: Iterable[str],
    lower_depth_fraction: Decimal,
    upper_depth_fraction: Decimal,
) -> tuple[RemoteArtifactKey, ...]:
    """Construct the exact component-artifact universe for a UTC range."""

    start = require_utc_hour(
        "requested_start_utc",
        requested_start_utc,
    )
    end = require_utc_hour(
        "requested_end_utc",
        requested_end_utc,
    )

    if end <= start:
        raise ValueError("requested_end_utc must be after requested_start_utc")

    if not isinstance(lower_depth_fraction, Decimal):
        raise TypeError("lower_depth_fraction must be Decimal")

    if not isinstance(upper_depth_fraction, Decimal):
        raise TypeError("upper_depth_fraction must be Decimal")

    selected_bases = _normalized_bases(bases)
    keys: list[RemoteArtifactKey] = []
    hour = start

    while hour < end:
        for base in selected_bases:
            binance_instrument = {
                "BTC": "BTCUSDT",
                "ETH": "ETHUSDT",
            }[base]
            okx_instrument = {
                "BTC": "BTC-USDT-SWAP",
                "ETH": "ETH-USDT-SWAP",
            }[base]

            binance_preset = build_binance_futures_data_preset(
                base=base,
                lower_fraction=lower_depth_fraction,
                upper_fraction=upper_depth_fraction,
            )
            okx_preset = build_okx_futures_data_preset(
                base=base,
                lower_fraction=lower_depth_fraction,
                upper_fraction=upper_depth_fraction,
            )

            keys.extend(
                (
                    RemoteArtifactKey(
                        kind=RemoteArtifactKind.L2,
                        provider="cryptohftdata",
                        venue="binance_futures",
                        instrument=binance_instrument,
                        hour_utc=hour,
                        preset_hash=binance_preset.preset_hash,
                    ),
                    RemoteArtifactKey(
                        kind=RemoteArtifactKind.L2,
                        provider="cryptohftdata",
                        venue="okx_futures",
                        instrument=okx_instrument,
                        hour_utc=hour,
                        preset_hash=okx_preset.preset_hash,
                    ),
                    RemoteArtifactKey(
                        kind=RemoteArtifactKind.PRICE,
                        provider="cryptohftdata",
                        venue="binance_futures",
                        instrument=binance_instrument,
                        hour_utc=hour,
                    ),
                )
            )

        hour += timedelta(hours=1)

    return tuple(keys)


def _production_artifact_importer(
    downloaded: DownloadedHuggingFaceArtifact,
) -> RemoteArtifactImportResult:
    with session_scope() as session:
        return import_downloaded_huggingface_artifact(
            session,
            downloaded,
        )


class RemoteImportRuntime:
    """Own one pinned-revision remote range-import operation."""

    def __init__(
        self,
        *,
        repository: RemoteImportRepositoryProtocol,
        artifact_importer: RemoteArtifactImporter = (_production_artifact_importer),
    ) -> None:
        if not callable(getattr(repository, "current_revision", None)):
            raise TypeError("repository must provide current_revision()")

        if not callable(getattr(repository, "download_artifact", None)):
            raise TypeError("repository must provide download_artifact()")

        if not callable(artifact_importer):
            raise TypeError("artifact_importer must be callable")

        self._repository = repository
        self._artifact_importer = artifact_importer

        self._task: asyncio.Task[RemoteImportRangeResult] | None = None
        self._cancellation_event: threading.Event | None = None

        self._operation_id: UUID | None = None
        self._started_at: datetime | None = None
        self._stop_requested = False
        self._pinned_revision: str | None = None
        self._latest_progress: RemoteImportRuntimeProgress | None = None
        self._last_result: RemoteImportRangeResult | None = None
        self._last_error: str | None = None
        self._completion_sequence = 0

        self._state_lock = threading.Lock()

    @property
    def task(
        self,
    ) -> asyncio.Task[RemoteImportRangeResult] | None:
        return self._task

    @property
    def is_running(self) -> bool:
        task = self._task
        return task is not None and not task.done()

    def snapshot(self) -> RemoteImportRuntimeSnapshot:
        with self._state_lock:
            return RemoteImportRuntimeSnapshot(
                is_running=self.is_running,
                operation_id=(
                    str(self._operation_id) if self._operation_id is not None else None
                ),
                started_at=self._started_at,
                stop_requested=self._stop_requested,
                pinned_revision=self._pinned_revision,
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
        bases: Iterable[str],
        lower_depth_fraction: Decimal,
        upper_depth_fraction: Decimal,
    ) -> asyncio.Task[RemoteImportRangeResult]:
        state = get_state()

        if state.shutdown_started:
            raise RuntimeError(
                "Application shutdown has started; " "new remote imports are blocked"
            )

        if self.is_running:
            raise RemoteImportRuntimeBusyError(
                "A remote import operation is already active"
            )

        if state.active_operation_name:
            raise RemoteImportRuntimeBusyError(
                "Another operation is active: " f"{state.active_operation_name}"
            )

        start = require_utc_hour(
            "requested_start_utc",
            requested_start_utc,
        )
        end = require_utc_hour(
            "requested_end_utc",
            requested_end_utc,
        )
        selected_bases = _normalized_bases(bases)

        keys = plan_remote_import_keys(
            requested_start_utc=start,
            requested_end_utc=end,
            bases=selected_bases,
            lower_depth_fraction=lower_depth_fraction,
            upper_depth_fraction=upper_depth_fraction,
        )

        operation_id = uuid4()
        started_at = now_utc()
        cancellation_event = threading.Event()

        with self._state_lock:
            self._operation_id = operation_id
            self._started_at = started_at
            self._stop_requested = False
            self._pinned_revision = None
            self._latest_progress = None
            self._last_result = None
            self._last_error = None
            self._cancellation_event = cancellation_event

        state.active_operation_name = "remote_hf_import"
        state.active_operation_started_at = started_at

        task = asyncio.create_task(
            self._run(
                operation_id=operation_id,
                started_at=started_at,
                requested_start_utc=start,
                requested_end_utc=end,
                selected_bases=selected_bases,
                keys=keys,
                cancellation_event=cancellation_event,
            ),
            name="l2shock-remote-hf-import",
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

    def _set_progress(
        self,
        *,
        operation_id: UUID,
        message: str,
        pinned_revision: str,
        artifacts_selected: int,
        items: list[RemoteImportItemResult],
        current_key: RemoteArtifactKey | None,
    ) -> None:
        with self._state_lock:
            self._latest_progress = RemoteImportRuntimeProgress(
                operation_id=operation_id,
                message=message,
                pinned_revision=pinned_revision,
                artifacts_selected=artifacts_selected,
                artifacts_completed=len(items),
                imported_count=sum(
                    item.disposition is RemoteImportItemDisposition.IMPORTED
                    for item in items
                ),
                reused_count=sum(
                    item.disposition is RemoteImportItemDisposition.REUSED
                    for item in items
                ),
                missing_count=sum(
                    item.disposition is RemoteImportItemDisposition.MISSING
                    for item in items
                ),
                failed_count=sum(
                    item.disposition is RemoteImportItemDisposition.ERROR
                    for item in items
                ),
                current_key=current_key,
            )

    async def _run_thread_boundary(
        self,
        function: Callable[[], object],
        *,
        task_name: str,
        cancellation_event: threading.Event,
    ) -> object:
        worker = asyncio.create_task(
            asyncio.to_thread(function),
            name=task_name,
        )

        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancellation_event.set()

            try:
                await asyncio.shield(worker)
            except Exception:
                log.exception(
                    "Remote import worker failed while responding "
                    "to task cancellation."
                )

            raise

    async def _run(
        self,
        *,
        operation_id: UUID,
        started_at: datetime,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        selected_bases: tuple[str, ...],
        keys: tuple[RemoteArtifactKey, ...],
        cancellation_event: threading.Event,
    ) -> RemoteImportRangeResult:
        state = get_state()
        current_task = asyncio.current_task()
        items: list[RemoteImportItemResult] = []
        pinned_revision = ""
        stopped = False
        acquired_operation_lock = False

        try:
            operation_lock = state.operation_lock

            if operation_lock.locked():
                raise RemoteImportRuntimeBusyError(
                    "Another application operation owns " "the process lock"
                )

            await operation_lock.acquire()
            acquired_operation_lock = True

            pinned_revision = _full_commit_sha(
                await self._run_thread_boundary(
                    self._repository.current_revision,
                    task_name="l2shock-hf-pin-revision",
                    cancellation_event=cancellation_event,
                )
            )

            with self._state_lock:
                self._pinned_revision = pinned_revision

            self._set_progress(
                operation_id=operation_id,
                message=(f"Pinned revision and selected " f"{len(keys)} artifact(s)."),
                pinned_revision=pinned_revision,
                artifacts_selected=len(keys),
                items=items,
                current_key=None,
            )

            for key in keys:
                if cancellation_event.is_set():
                    stopped = True
                    break

                self._set_progress(
                    operation_id=operation_id,
                    message=("Downloading and verifying " f"{key.relative_path}."),
                    pinned_revision=pinned_revision,
                    artifacts_selected=len(keys),
                    items=items,
                    current_key=key,
                )

                try:
                    downloaded = await self._run_thread_boundary(
                        lambda key=key: (
                            self._repository.download_artifact(
                                key,
                                revision=pinned_revision,
                            )
                        ),
                        task_name="l2shock-hf-artifact-download",
                        cancellation_event=cancellation_event,
                    )

                    if downloaded is None:
                        item = RemoteImportItemResult(
                            key=key,
                            disposition=(RemoteImportItemDisposition.MISSING),
                            diagnostic=(
                                "Artifact and manifest are absent "
                                "from the pinned revision"
                            ),
                        )
                    else:
                        if not isinstance(
                            downloaded,
                            DownloadedHuggingFaceArtifact,
                        ):
                            raise RemoteImportRuntimeError(
                                "Repository returned an unsupported " "download result"
                            )

                        if downloaded.revision != pinned_revision:
                            raise RemoteImportRuntimeError(
                                "Downloaded artifact does not belong "
                                "to the operation's pinned revision"
                            )

                        if downloaded.artifact.manifest.key != key:
                            raise RemoteImportRuntimeError(
                                "Downloaded artifact key does not "
                                "match the planned key"
                            )

                        imported = await self._run_thread_boundary(
                            lambda downloaded=downloaded: (
                                self._artifact_importer(downloaded)
                            ),
                            task_name="l2shock-hf-artifact-import",
                            cancellation_event=cancellation_event,
                        )

                        if not isinstance(
                            imported,
                            RemoteArtifactImportResult,
                        ):
                            raise RemoteImportRuntimeError(
                                "Artifact importer returned an " "unsupported result"
                            )

                        if imported.revision != pinned_revision:
                            raise RemoteImportRuntimeError(
                                "Imported artifact revision does not "
                                "match the pinned revision"
                            )

                        if imported.key != key:
                            raise RemoteImportRuntimeError(
                                "Imported artifact key does not "
                                "match the planned key"
                            )

                        disposition = (
                            RemoteImportItemDisposition.IMPORTED
                            if imported.analytical_inserted
                            else RemoteImportItemDisposition.REUSED
                        )

                        item = RemoteImportItemResult(
                            key=key,
                            disposition=disposition,
                        )

                except asyncio.CancelledError:
                    cancellation_event.set()
                    raise

                except Exception as exc:
                    item = RemoteImportItemResult(
                        key=key,
                        disposition=RemoteImportItemDisposition.ERROR,
                        diagnostic=(f"Unexpected {type(exc).__name__}"),
                    )
                    log.exception(
                        "Remote import failed for %s.",
                        key.relative_path,
                    )

                items.append(item)

                self._set_progress(
                    operation_id=operation_id,
                    message=(
                        f"Completed {len(items)} of "
                        f"{len(keys)} selected artifact(s)."
                    ),
                    pinned_revision=pinned_revision,
                    artifacts_selected=len(keys),
                    items=items,
                    current_key=key,
                )

            imported_count = sum(
                item.disposition is RemoteImportItemDisposition.IMPORTED
                for item in items
            )
            reused_count = sum(
                item.disposition is RemoteImportItemDisposition.REUSED for item in items
            )
            missing_count = sum(
                item.disposition is RemoteImportItemDisposition.MISSING
                for item in items
            )
            failed_count = sum(
                item.disposition is RemoteImportItemDisposition.ERROR for item in items
            )

            successful_count = imported_count + reused_count

            if stopped:
                status = "stopped"
            elif not keys:
                status = "no_work"
            elif missing_count == 0 and failed_count == 0:
                status = "ok"
            elif successful_count > 0:
                status = "partial_ok"
            else:
                status = "error"

            result = RemoteImportRangeResult(
                operation_id=operation_id,
                requested_start_utc=requested_start_utc,
                requested_end_utc=requested_end_utc,
                selected_bases=selected_bases,
                pinned_revision=pinned_revision,
                started_at=started_at,
                ended_at=now_utc(),
                status=status,
                items=tuple(items),
                artifacts_selected=len(keys),
                imported_count=imported_count,
                reused_count=reused_count,
                missing_count=missing_count,
                failed_count=failed_count,
                stopped=stopped,
            )

            with self._state_lock:
                self._last_result = result
                self._last_error = None

            return result

        except asyncio.CancelledError:
            cancellation_event.set()

            with self._state_lock:
                self._last_error = (
                    "Remote import was cancelled during " "application shutdown"
                )

            raise

        except Exception as exc:
            with self._state_lock:
                if isinstance(exc, RemoteImportRuntimeError):
                    self._last_error = str(exc).strip() or type(exc).__name__
                else:
                    self._last_error = f"Unexpected {type(exc).__name__}"

            log.exception("Remote import runtime failed.")
            raise

        finally:
            if acquired_operation_lock:
                operation_lock.release()

            with self._state_lock:
                self._completion_sequence += 1
                self._stop_requested = False
                self._cancellation_event = None
                self._operation_id = None

            if state.active_operation_name == "remote_hf_import":
                state.active_operation_name = ""
                state.active_operation_started_at = None

            if current_task is not None:
                state.tracked_tasks.discard(current_task)

            if self._task is current_task:
                self._task = None


_runtime: RemoteImportRuntime | None = None
_runtime_loop: asyncio.AbstractEventLoop | None = None


def get_remote_import_runtime(
    *,
    repository: HuggingFaceDatasetRepository | None = None,
) -> RemoteImportRuntime:
    """Return the singleton runtime bound to the current event loop."""

    global _runtime, _runtime_loop

    loop = asyncio.get_running_loop()

    if _runtime is None:
        if repository is None:
            raise RuntimeError(
                "repository is required when creating " "the remote import runtime"
            )

        _runtime = RemoteImportRuntime(
            repository=repository,
        )
        _runtime_loop = loop
        return _runtime

    if _runtime_loop is not loop:
        raise RuntimeError(
            "Remote import runtime belongs to another " "asyncio event loop"
        )

    return _runtime


def peek_remote_import_runtime() -> RemoteImportRuntime | None:
    return _runtime


def reset_remote_import_runtime_for_tests() -> None:
    global _runtime, _runtime_loop

    _runtime = None
    _runtime_loop = None


__all__ = [
    "RemoteImportItemDisposition",
    "RemoteImportItemResult",
    "RemoteImportRangeResult",
    "RemoteImportRuntime",
    "RemoteImportRuntimeBusyError",
    "RemoteImportRuntimeError",
    "RemoteImportRuntimeProgress",
    "RemoteImportRuntimeSnapshot",
    "get_remote_import_runtime",
    "peek_remote_import_runtime",
    "plan_remote_import_keys",
    "reset_remote_import_runtime_for_tests",
]

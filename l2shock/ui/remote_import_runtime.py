# l2shock/ui/remote_import_runtime.py
"""Application-owned runtime for verified HF and staged B2 range imports.

One operation:

1. validates an exact UTC range and selected BTC/ETH bases;
2. constructs the exact component L2 and Binance price artifact keys;
3. pins one HF commit or pins each B2 publication independently;
4. downloads and verifies the selected immutable artifact ownership;
5. delegates PostgreSQL writes to the existing verified importer;
6. exposes immutable progress and terminal snapshots.

B2 does not provide a repository-wide range snapshot or synthetic revision.
The existing Fetch UI singleton remains on HF until its integration batch.

The runtime does not replay order books, calculate liquidity, reconstruct price
OHLC, publish artifacts, or implement another analytical codec.
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

from l2shock.config import B2Config, get_settings
from l2shock.db.engine import session_scope
from l2shock.presets import (
    build_binance_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
)
from l2shock.db.analytical_repository import AnalyticalRepositoryError
from l2shock.remote.hf_repository import (
    DownloadedHuggingFaceArtifact,
    HuggingFaceDatasetRepository,
    HuggingFaceRepositoryError,
)
from l2shock.remote.importer import (
    RemoteArtifactImportError,
    RemoteArtifactImportResult,
    import_downloaded_huggingface_artifact,
    import_verified_remote_artifact,
)
from l2shock.remote.b2_repository import (
    B2ProcessedArtifactRepository,
    B2RepositoryError,
)
from l2shock.remote.b2_transport import (
    B2ObjectStore,
    B2TransportError,
)
from l2shock.remote.import_contracts import (
    B2_STORAGE_BACKEND,
    HF_STORAGE_BACKEND,
    RemoteImportStorageIdentity,
    VerifiedRemoteImportArtifact,
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
    storage_identity: RemoteImportStorageIdentity | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, RemoteArtifactKey):
            raise TypeError("key must be a RemoteArtifactKey")

        identity = self.storage_identity

        if identity is not None:
            if not isinstance(identity, RemoteImportStorageIdentity):
                raise TypeError(
                    "storage_identity must be RemoteImportStorageIdentity or null"
                )

            if identity.backend == B2_STORAGE_BACKEND:
                reference = identity.publication_reference
                assert reference is not None

                if reference.publication.key != self.key:
                    raise RemoteImportRuntimeError(
                        "Item storage ownership belongs to a different artifact"
                    )

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
    pinned_revision: str | None
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
    storage_backend: str = HF_STORAGE_BACKEND

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

        if self.storage_backend == B2_STORAGE_BACKEND:
            if self.pinned_revision is not None:
                raise RemoteImportRuntimeError(
                    "B2 range results must not fabricate a repository revision"
                )

            revision = None

        elif self.storage_backend == HF_STORAGE_BACKEND:
            if self.pinned_revision is None and self.stopped is True and not self.items:
                # A stop before the first worker boundary performs no HF
                # revision lookup and therefore owns no pinned revision.
                revision = None
            else:
                revision = _full_commit_sha(self.pinned_revision)

        else:
            raise RemoteImportRuntimeError("Unsupported remote range storage backend")

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

        for item in self.items:
            if not isinstance(item, RemoteImportItemResult):
                raise TypeError("items must contain RemoteImportItemResult objects")

            identity = item.storage_identity

            if identity is not None and identity.backend != self.storage_backend:
                raise RemoteImportRuntimeError(
                    "Item storage backend disagrees with the range backend"
                )

            if (
                self.storage_backend == B2_STORAGE_BACKEND
                and item.disposition
                in {
                    RemoteImportItemDisposition.IMPORTED,
                    RemoteImportItemDisposition.REUSED,
                }
                and identity is None
            ):
                raise RemoteImportRuntimeError(
                    "Successful B2 items require exact storage ownership"
                )

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
    pinned_revision: str | None
    artifacts_selected: int
    artifacts_completed: int
    imported_count: int
    reused_count: int
    missing_count: int
    failed_count: int
    current_key: RemoteArtifactKey | None
    storage_backend: str = HF_STORAGE_BACKEND

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
    storage_backend: str = HF_STORAGE_BACKEND


class RemoteImportRepositoryProtocol(Protocol):
    def current_revision(self) -> str | None: ...

    def download_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> DownloadedHuggingFaceArtifact | VerifiedRemoteImportArtifact | None: ...


RemoteArtifactImporter = Callable[
    [DownloadedHuggingFaceArtifact | VerifiedRemoteImportArtifact],
    RemoteArtifactImportResult,
]


class B2RangeImportRepository:
    """Read-only range adapter with per-artifact publication pinning.

    B2 has no repository-wide commit revision. Each download resolves one
    completion descriptor and consumes exactly that reference.

    The store is created and closed inside the synchronous download boundary.
    No SDK client survives a completed or failed artifact download.
    """

    storage_backend = B2_STORAGE_BACKEND

    def __init__(
        self,
        settings: B2Config,
        *,
        store_factory: Callable[[B2Config], B2ObjectStore] = B2ObjectStore,
    ) -> None:
        if not isinstance(settings, B2Config):
            raise TypeError("settings must be B2Config")

        if not settings.configured:
            raise RemoteImportRuntimeError(
                "Remote B2 Import requires endpoint, bucket, and credentials"
            )

        if not callable(store_factory):
            raise TypeError("store_factory must be callable")

        # Take a private settings copy. Later mutation of the caller's
        # configuration must not change this adapter's storage location.
        self._settings = settings.model_copy(deep=True)
        self._store_factory = store_factory

    def __repr__(self) -> str:
        return "B2RangeImportRepository()"

    @property
    def endpoint_url(self) -> str:
        return self._settings.endpoint_url

    @property
    def bucket(self) -> str:
        return self._settings.bucket

    def current_revision(self) -> None:
        """Return no revision without contacting B2."""
        return None

    def download_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> VerifiedRemoteImportArtifact | None:
        if not isinstance(key, RemoteArtifactKey):
            raise TypeError("key must be RemoteArtifactKey")

        if revision is not None:
            raise RemoteImportRuntimeError(
                "B2 range downloads do not accept an HF revision"
            )

        with self._store_factory(self._settings) as store:
            repository = B2ProcessedArtifactRepository(store)

            if (
                repository.endpoint_url != self.endpoint_url
                or repository.bucket != self.bucket
            ):
                raise RemoteImportRuntimeError(
                    "B2 transport location disagrees with configured ownership"
                )

            reference = repository.resolve_publication(key)

            if reference is None:
                return None

            downloaded = repository.require_artifact(
                key,
                reference=reference,
            )

            if downloaded.reference != reference:
                raise RemoteImportRuntimeError(
                    "B2 download changed the pinned publication reference"
                )

            return VerifiedRemoteImportArtifact.from_b2(
                downloaded,
                endpoint_url=repository.endpoint_url,
                bucket=repository.bucket,
            )


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
            bybit_instrument = {
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
            bybit_preset = build_bybit_data_preset(
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
                        venue="bybit",
                        instrument=bybit_instrument,
                        hour_utc=hour,
                        preset_hash=bybit_preset.preset_hash,
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


_MAX_ITEM_DIAGNOSTIC_CHARS = 300

# Project-owned error types. Their messages are fixed, secret-free text that
# tells the user what to fix (conflicting checkpoint, partial HF pair,
# provenance conflict, unverifiable local raw attachment, ...). Any other
# exception may carry request/database details and stays type-only.
_SURFACED_ITEM_ERRORS: tuple[type[BaseException], ...] = (
    RemoteImportRuntimeError,
    RemoteArtifactImportError,
    HuggingFaceRepositoryError,
    B2RepositoryError,
    B2TransportError,
    AnalyticalRepositoryError,
)


def _item_diagnostic(exc: BaseException) -> str:
    """Return a bounded, secret-safe per-artifact failure diagnostic."""
    if isinstance(exc, _SURFACED_ITEM_ERRORS):
        text = " ".join(str(exc).split())

        if text:
            if len(text) > _MAX_ITEM_DIAGNOSTIC_CHARS:
                text = text[: _MAX_ITEM_DIAGNOSTIC_CHARS - 3] + "..."

            return f"{type(exc).__name__}: {text}"

    return f"Unexpected {type(exc).__name__}"


def _production_artifact_importer(
    downloaded: DownloadedHuggingFaceArtifact | VerifiedRemoteImportArtifact,
) -> RemoteArtifactImportResult:
    # Downloads and transport cleanup have already completed before this
    # function opens the PostgreSQL session.
    with session_scope() as session:
        if isinstance(downloaded, VerifiedRemoteImportArtifact):
            return import_verified_remote_artifact(
                session,
                downloaded,
            )

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

        storage_backend = getattr(
            repository,
            "storage_backend",
            HF_STORAGE_BACKEND,
        )

        if storage_backend not in {
            HF_STORAGE_BACKEND,
            B2_STORAGE_BACKEND,
        }:
            raise RemoteImportRuntimeError(
                "Repository declares an unsupported storage backend"
            )

        if storage_backend == B2_STORAGE_BACKEND and not isinstance(
            repository,
            B2RangeImportRepository,
        ):
            raise TypeError("B2 runtime requires B2RangeImportRepository")

        self._repository = repository
        self._artifact_importer = artifact_importer
        self._storage_backend = storage_backend
        self._operation_name = (
            "remote_b2_import"
            if storage_backend == B2_STORAGE_BACKEND
            else "remote_hf_import"
        )

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
                storage_backend=self._storage_backend,
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

        if self._task is not None:
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

        # A call outside a running event loop must leave both the runtime
        # snapshot and process admission state untouched.
        loop = asyncio.get_running_loop()

        operation_id = uuid4()
        started_at = now_utc()
        cancellation_event = threading.Event()

        with self._state_lock:
            previous_runtime_state = (
                self._operation_id,
                self._started_at,
                self._stop_requested,
                self._pinned_revision,
                self._latest_progress,
                self._last_result,
                self._last_error,
                self._cancellation_event,
            )

            self._operation_id = operation_id
            self._started_at = started_at
            self._stop_requested = False
            self._pinned_revision = None
            self._latest_progress = None
            self._last_result = None
            self._last_error = None
            self._cancellation_event = cancellation_event

        state.active_operation_name = self._operation_name
        state.active_operation_started_at = started_at

        async def run_owned() -> RemoteImportRangeResult:
            # An eager task factory must not enter repository work before
            # start() has installed task ownership and returned to its caller.
            # Cancellation at this barrier is handled by the existing
            # reservation-finalization callback.
            await asyncio.sleep(0)

            return await self._run(
                operation_id=operation_id,
                started_at=started_at,
                requested_start_utc=start,
                requested_end_utc=end,
                selected_bases=selected_bases,
                keys=keys,
                cancellation_event=cancellation_event,
            )

        coroutine = run_owned()

        try:
            task = loop.create_task(
                coroutine,
                name=f"l2shock-{self._operation_name}",
            )
        except BaseException:
            coroutine.close()

            with self._state_lock:
                (
                    self._operation_id,
                    self._started_at,
                    self._stop_requested,
                    self._pinned_revision,
                    self._latest_progress,
                    self._last_result,
                    self._last_error,
                    self._cancellation_event,
                ) = previous_runtime_state

            if (
                state.active_operation_name == self._operation_name
                and state.active_operation_started_at == started_at
            ):
                state.active_operation_name = ""
                state.active_operation_started_at = None

            raise

        self._task = task
        state.tracked_tasks.add(task)

        def finalize(done: asyncio.Task[RemoteImportRangeResult]) -> None:
            state.tracked_tasks.discard(done)

            with self._state_lock:
                owns_unfinalized_reservation = (
                    self._task is done and self._operation_id == operation_id
                )

                if owns_unfinalized_reservation:
                    # Cancellation before the coroutine's first execution
                    # bypasses _run() and therefore bypasses its finally.
                    cancellation_event.set()
                    self._completion_sequence += 1
                    self._stop_requested = False
                    self._cancellation_event = None
                    self._operation_id = None
                    self._started_at = None
                    self._pinned_revision = None

                    if done.cancelled():
                        self._last_error = (
                            "Remote import was cancelled before it started"
                        )

                if self._task is done:
                    self._task = None

            if (
                owns_unfinalized_reservation
                and state.active_operation_name == self._operation_name
                and state.active_operation_started_at == started_at
            ):
                state.active_operation_name = ""
                state.active_operation_started_at = None

            # Observe failures even when the caller does not await this task.
            # Awaiting callers still receive the task's original exception.
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finalize)
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
            # Our own task.cancel() surfaces here once the task finishes. Any
            # other CancelledError belongs to the caller and must propagate
            # instead of reporting a false "stopped".
            if task.cancelled():
                return True
            raise
        except Exception:
            return True

        return True

    def _set_progress(
        self,
        *,
        operation_id: UUID,
        message: str,
        pinned_revision: str | None,
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
                storage_backend=self._storage_backend,
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
        loop = asyncio.get_running_loop()

        def _mark_worker_exception_retrieved(done_worker):
            # Python 3.14's asyncio.shield() can eagerly report an inner
            # task exception through the loop exception handler, even when
            # this boundary retrieves the exception immediately afterwards.
            # Retrieving it here keeps asyncio's default handler from
            # rendering arbitrary worker exception text.
            if done_worker.cancelled():
                return
            done_worker.exception()

        worker.add_done_callback(_mark_worker_exception_retrieved)

        waiter = loop.create_future()

        def _release_initial_waiter(done_worker):
            del done_worker
            if not waiter.done():
                waiter.set_result(None)

        worker.add_done_callback(_release_initial_waiter)

        try:
            await waiter
        except asyncio.CancelledError:
            cancellation_event.set()

            # Cancelling this boundary task must not cancel the worker
            # thread. Retain operation ownership until that thread has
            # actually exited, even if shutdown issues additional
            # cancellation requests while this join is in progress.
            while not worker.done():
                join_waiter = loop.create_future()

                def _release_join_waiter(done_worker, *, _waiter=join_waiter):
                    del done_worker
                    if not _waiter.done():
                        _waiter.set_result(None)

                worker.add_done_callback(_release_join_waiter)
                try:
                    await join_waiter
                except asyncio.CancelledError:
                    cancellation_event.set()
                    continue
                finally:
                    worker.remove_done_callback(_release_join_waiter)

            if worker.done() and not worker.cancelled():
                try:
                    worker.result()
                except Exception as exc:
                    log.error(
                        "Remote import worker failed while responding "
                        "to task cancellation; error_type=%s.",
                        type(exc).__name__,
                    )

            raise
        finally:
            worker.remove_done_callback(_release_initial_waiter)

        return worker.result()

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
        pinned_revision: str | None = None
        stopped = False
        acquired_operation_lock = False

        try:
            operation_lock = state.operation_lock

            if state.shutdown_started:
                cancellation_event.set()

            if not cancellation_event.is_set():
                if operation_lock.locked():
                    raise RemoteImportRuntimeBusyError(
                        "Another application operation owns the process lock"
                    )

                await operation_lock.acquire()
                acquired_operation_lock = True

                # Recheck immediately before admitting synchronous work.
                if state.shutdown_started:
                    cancellation_event.set()

                if not cancellation_event.is_set():
                    resolved_revision = await self._run_thread_boundary(
                        self._repository.current_revision,
                        task_name=f"l2shock-{self._operation_name}-pin",
                        cancellation_event=cancellation_event,
                    )

                    if self._storage_backend == HF_STORAGE_BACKEND:
                        pinned_revision = _full_commit_sha(resolved_revision)
                    elif resolved_revision is not None:
                        raise RemoteImportRuntimeError(
                            "B2 repository must not return a synthetic revision"
                        )

            with self._state_lock:
                self._pinned_revision = pinned_revision

            self._set_progress(
                operation_id=operation_id,
                message=(
                    f"Selected {len(keys)} artifact(s); "
                    + (
                        "B2 publications are pinned per artifact."
                        if self._storage_backend == B2_STORAGE_BACKEND
                        else "HF revision pinning completed."
                    )
                    if not cancellation_event.is_set()
                    else "Stopped before further remote work."
                ),
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

                downloaded = None

                try:
                    downloaded = await self._run_thread_boundary(
                        lambda key=key: (
                            self._repository.download_artifact(
                                key,
                                revision=pinned_revision,
                            )
                        ),
                        task_name=f"l2shock-{self._operation_name}-download",
                        cancellation_event=cancellation_event,
                    )

                    if downloaded is None:
                        item = RemoteImportItemResult(
                            key=key,
                            disposition=(RemoteImportItemDisposition.MISSING),
                            diagnostic=(
                                "No completed B2 publication exists"
                                if self._storage_backend == B2_STORAGE_BACKEND
                                else (
                                    "Artifact and manifest are absent "
                                    "from the pinned revision"
                                )
                            ),
                        )
                    else:
                        if self._storage_backend == B2_STORAGE_BACKEND:
                            if not isinstance(
                                downloaded,
                                VerifiedRemoteImportArtifact,
                            ):
                                raise RemoteImportRuntimeError(
                                    "B2 repository returned an unsupported download"
                                )

                            identity = downloaded.storage_identity

                            if (
                                identity.backend != B2_STORAGE_BACKEND
                                or downloaded.revision is not None
                                or identity.endpoint_url
                                != self._repository.endpoint_url
                                or identity.bucket != self._repository.bucket
                            ):
                                raise RemoteImportRuntimeError(
                                    "B2 download has incorrect storage ownership"
                                )

                        else:
                            if not isinstance(
                                downloaded,
                                DownloadedHuggingFaceArtifact,
                            ):
                                raise RemoteImportRuntimeError(
                                    "HF repository returned an unsupported download"
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
                            task_name=f"l2shock-{self._operation_name}-import",
                            cancellation_event=cancellation_event,
                        )

                        if not isinstance(
                            imported,
                            RemoteArtifactImportResult,
                        ):
                            raise RemoteImportRuntimeError(
                                "Artifact importer returned an " "unsupported result"
                            )

                        if self._storage_backend == B2_STORAGE_BACKEND:
                            if (
                                imported.revision is not None
                                or imported.storage_identity
                                != downloaded.storage_identity
                            ):
                                raise RemoteImportRuntimeError(
                                    "Imported B2 storage ownership changed"
                                )

                        elif imported.revision != pinned_revision:
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
                            storage_identity=(
                                downloaded.storage_identity
                                if isinstance(
                                    downloaded,
                                    VerifiedRemoteImportArtifact,
                                )
                                else None
                            ),
                        )

                except asyncio.CancelledError:
                    cancellation_event.set()
                    raise

                except Exception as exc:
                    item = RemoteImportItemResult(
                        key=key,
                        disposition=RemoteImportItemDisposition.ERROR,
                        diagnostic=_item_diagnostic(exc),
                        storage_identity=(
                            downloaded.storage_identity
                            if (
                                isinstance(
                                    downloaded,
                                    VerifiedRemoteImportArtifact,
                                )
                                and downloaded.artifact.manifest.key == key
                                and downloaded.storage_identity.backend
                                == self._storage_backend
                            )
                            else None
                        ),
                    )
                    log.error(
                        "Remote import failed for %s; error_type=%s.",
                        key.relative_path,
                        type(exc).__name__,
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

                # A stop request cannot interrupt the current atomic
                # download/import boundary. Observe it immediately afterward,
                # including when this was the final selected artifact.
                if cancellation_event.is_set():
                    stopped = True
                    break

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
                storage_backend=self._storage_backend,
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

            log.error(
                "Remote import runtime failed; error_type=%s.",
                type(exc).__name__,
            )
            raise

        finally:
            if acquired_operation_lock:
                operation_lock.release()

            with self._state_lock:
                self._completion_sequence += 1
                self._stop_requested = False
                self._cancellation_event = None
                self._operation_id = None
                self._started_at = None

            if (
                state.active_operation_name == self._operation_name
                and state.active_operation_started_at == started_at
            ):
                state.active_operation_name = ""
                state.active_operation_started_at = None

            if current_task is not None:
                state.tracked_tasks.discard(current_task)

            if self._task is current_task:
                self._task = None


_runtime: RemoteImportRuntime | None = None
_runtime_loop: asyncio.AbstractEventLoop | None = None


def create_production_remote_import_repository() -> HuggingFaceDatasetRepository:
    """Build the local read-only private-dataset transport.

    Repository permissions are enforced by the configured token on Hugging
    Face. The local application requires only read access.
    """

    settings = get_settings()
    config = settings.remote

    if not config.hf_repo_id:
        raise RuntimeError(
            "Remote HF Import is not configured: "
            "set remote.hf_repo_id in config.yaml"
        )

    token = config.hf_token.get_secret_value().strip()

    if not token:
        raise RuntimeError(
            "Remote HF Import is not configured: "
            "set L2SHOCK__REMOTE__HF_TOKEN in .env"
        )

    return HuggingFaceDatasetRepository(
        repo_id=config.hf_repo_id,
        revision=config.hf_revision,
        token=config.hf_token,
    )


def create_production_b2_remote_import_runtime() -> RemoteImportRuntime:
    """Build an explicit B2 runtime without switching the HF UI singleton.

    This staged factory is suitable for controlled range-import verification.
    Fetch UI selection and application singleton integration follow in the
    next migration batch.
    """
    return RemoteImportRuntime(
        repository=B2RangeImportRepository(
            get_settings().remote.b2,
        ),
    )


def get_remote_import_runtime(
    *,
    repository: RemoteImportRepositoryProtocol | None = None,
    storage_backend: str | None = None,
) -> RemoteImportRuntime:
    """Return the application-owned remote runtime for an explicit backend.

    With no arguments, an existing runtime is returned unchanged. Initial
    construction uses the configured default remote workflow.

    An explicit backend change is allowed only while the previous runtime
    and shared application admission are idle. Construction must succeed
    before the previous singleton is replaced.

    This getter does not perform artifact downloads or database imports.
    """

    global _runtime, _runtime_loop

    loop = asyncio.get_running_loop()
    current = _runtime

    if current is not None and _runtime_loop is not loop:
        raise RuntimeError(
            "Remote import runtime belongs to another asyncio event loop"
        )

    if current is not None and repository is None and storage_backend is None:
        return current

    if storage_backend is not None and storage_backend not in {
        HF_STORAGE_BACKEND,
        B2_STORAGE_BACKEND,
    }:
        raise RemoteImportRuntimeError("Unsupported remote import storage backend")

    if repository is not None:
        repository_backend = getattr(
            repository,
            "storage_backend",
            HF_STORAGE_BACKEND,
        )

        if repository_backend not in {
            HF_STORAGE_BACKEND,
            B2_STORAGE_BACKEND,
        }:
            raise RemoteImportRuntimeError(
                "Repository declares an unsupported storage backend"
            )

        if storage_backend is not None and storage_backend != repository_backend:
            raise RemoteImportRuntimeError(
                "Requested backend disagrees with the supplied repository"
            )

        selected_backend = repository_backend

    elif storage_backend is not None:
        selected_backend = storage_backend

    else:
        selected_backend = (
            B2_STORAGE_BACKEND
            if get_settings().remote.default_workflow == "remote_b2_import"
            else HF_STORAGE_BACKEND
        )

    if current is not None and current.snapshot().storage_backend == selected_backend:
        if repository is not None and repository is not current._repository:
            raise RemoteImportRuntimeError(
                "The existing remote runtime owns a different repository; "
                "restart before changing its storage location"
            )

        return current

    state = get_state()

    if state.shutdown_started:
        raise RemoteImportRuntimeBusyError(
            "Application shutdown has started; remote runtime construction "
            "and backend changes are blocked"
        )

    # A done task can still have a pending reservation-finalization callback.
    # task is not None is therefore deliberately stricter than is_running.
    if current is not None and current.task is not None:
        raise RemoteImportRuntimeBusyError(
            "The current remote runtime is still active or finalizing"
        )

    if state.active_operation_name:
        raise RemoteImportRuntimeBusyError(
            f"Another operation is active: {state.active_operation_name}"
        )

    if state.operation_lock.locked():
        raise RemoteImportRuntimeBusyError(
            "Another application operation owns the process lock"
        )

    if repository is not None:
        selected_repository = repository
    elif selected_backend == B2_STORAGE_BACKEND:
        selected_repository = B2RangeImportRepository(
            get_settings().remote.b2,
        )
    else:
        selected_repository = create_production_remote_import_repository()

    # Keep the previous singleton intact if configuration/construction fails.
    replacement = RemoteImportRuntime(
        repository=selected_repository,
    )

    if replacement.snapshot().storage_backend != selected_backend:
        raise RemoteImportRuntimeError(
            "Constructed runtime disagrees with the requested storage backend"
        )

    if current is not None:
        # Polling clients use this process-level sequence to detect completion.
        # Do not reset it merely because an idle backend was changed.
        replacement._completion_sequence = current.snapshot().completion_sequence

    _runtime = replacement
    _runtime_loop = loop
    return replacement


def peek_remote_import_runtime() -> RemoteImportRuntime | None:
    return _runtime


def reset_remote_import_runtime_for_tests() -> None:
    global _runtime, _runtime_loop

    _runtime = None
    _runtime_loop = None


__all__ = [
    "B2RangeImportRepository",
    "RemoteImportItemDisposition",
    "RemoteImportItemResult",
    "RemoteImportRangeResult",
    "RemoteImportRuntime",
    "RemoteImportRuntimeBusyError",
    "RemoteImportRuntimeError",
    "RemoteImportRuntimeProgress",
    "RemoteImportRuntimeSnapshot",
    "create_production_b2_remote_import_runtime",
    "create_production_remote_import_repository",
    "get_remote_import_runtime",
    "peek_remote_import_runtime",
    "plan_remote_import_keys",
    "reset_remote_import_runtime_for_tests",
]

# l2shock/ui/automatic_fetch_runtime.py
"""Application-owned automatic fetch scheduler.

The scheduler reuses the production acquisition coordinator. It does not
implement a second download workflow.

The newest eligible source hour is derived from the configured provider release
delay. Missing files remain retryable and are polled at the configured release
poll interval.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy import select

from l2shock.acquisition import (
    FetchOperationBusyError,
    FetchRunKind,
    ManualFetchResult,
    SourceDataKind,
    SourceFileSpec,
    create_production_manual_fetch_coordinator,
)
from l2shock.acquisition.release_schedule import (
    latest_release_eligible_hour,
)
from l2shock.config import get_settings
from l2shock.db.engine import session_scope
from l2shock.db.models import AppSetting, SourceHour
from l2shock.timeutils import (
    now_utc,
    require_utc_hour,
)
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)

_AUTOMATIC_FETCH_SETTING_KEY = "automatic_fetch"
_AUTOMATIC_FETCH_RETRY_CURSOR_SETTING_KEY = "automatic_fetch_retry_cursor"

_REQUIRED_SOURCE_IDENTITIES = frozenset(
    {
        (
            "binance_futures",
            "BTCUSDT",
            "orderbook",
        ),
        (
            "binance_futures",
            "BTCUSDT",
            "trades",
        ),
        (
            "binance_futures",
            "ETHUSDT",
            "orderbook",
        ),
        (
            "binance_futures",
            "ETHUSDT",
            "trades",
        ),
        (
            "bybit",
            "BTCUSDT",
            "orderbook",
        ),
        (
            "bybit",
            "ETHUSDT",
            "orderbook",
        ),
        (
            "okx_futures",
            "BTC-USDT-SWAP",
            "orderbook",
        ),
        (
            "okx_futures",
            "ETH-USDT-SWAP",
            "orderbook",
        ),
    }
)

_REQUIRED_FILES_PER_HOUR = len(_REQUIRED_SOURCE_IDENTITIES)


@dataclass(frozen=True, slots=True)
class AutomaticFetchRuntimeSnapshot:
    """Immutable automatic-fetch state for UI and health reporting."""

    enabled: bool
    is_running: bool
    stop_requested: bool
    current_operation_id: str | None
    current_target_hour_utc: datetime | None
    last_poll_at: datetime | None
    next_poll_at: datetime | None
    last_result: ManualFetchResult | None
    last_error: str | None
    completion_sequence: int


def select_automatic_fetch_target_hour(
    now: datetime,
    *,
    release_delay_minutes: int,
    catch_up_hours: int,
    complete_hours: Iterable[datetime],
    after_hour_utc: datetime | None = None,
) -> datetime | None:
    """Return one incomplete release-eligible source hour.

    Candidate hours are ordered newest to oldest inside the bounded catch-up
    window.

    With no retry cursor, the newest incomplete hour is selected. After an
    attempted hour is supplied through ``after_hour_utc``, selection continues
    with the next older incomplete hour and wraps to the newest incomplete hour
    after reaching the end of the window.

    This rotation prevents one persistently unavailable recent archive from
    starving every older gap. A newly release-eligible hour still receives
    immediate priority because the persisted cursor is scoped to the exact
    newest eligible hour.

    Returning ``None`` means every hour in the inspected catch-up window is
    already complete.

    This function owns planning only. It does not download, process, or mutate
    source rows.
    """

    if (
        isinstance(catch_up_hours, bool)
        or not isinstance(catch_up_hours, int)
        or catch_up_hours <= 0
    ):
        raise ValueError("catch_up_hours must be a positive integer")

    newest = latest_release_eligible_hour(
        now,
        release_delay_minutes=release_delay_minutes,
    )

    normalized_complete = {
        require_utc_hour(
            "complete source hour",
            hour,
        )
        for hour in complete_hours
    }

    candidates = tuple(
        newest - timedelta(hours=offset) for offset in range(catch_up_hours)
    )
    incomplete = tuple(
        candidate for candidate in candidates if candidate not in normalized_complete
    )

    if not incomplete:
        return None

    if after_hour_utc is None:
        return incomplete[0]

    cursor = require_utc_hour(
        "after_hour_utc",
        after_hour_utc,
    )

    try:
        cursor_index = candidates.index(cursor)
    except ValueError:
        # A cursor from a previous release window, or malformed/stale durable
        # state, must not delay the newest currently eligible gap.
        return incomplete[0]

    rotated_candidates = candidates[cursor_index + 1 :] + candidates[: cursor_index + 1]

    for candidate in rotated_candidates:
        if candidate not in normalized_complete:
            return candidate

    # ``incomplete`` proved that at least one candidate exists. This fallback is
    # defensive against an internal ordering error.
    return incomplete[0]


def _read_enabled_setting_sync() -> bool:
    with session_scope() as session:
        value = session.scalar(
            select(AppSetting.value_json).where(
                AppSetting.key == _AUTOMATIC_FETCH_SETTING_KEY
            )
        )

    if not isinstance(value, dict):
        return False

    return value.get("enabled") is True


def _write_enabled_setting_sync(enabled: bool) -> None:
    payload = {
        "enabled": bool(enabled),
        "schema": "l2shock.automatic_fetch_setting",
        "schema_version": 1,
        "updated_at_utc": now_utc().isoformat(),
    }

    with session_scope() as session:
        statement = postgresql_insert(AppSetting).values(
            key=_AUTOMATIC_FETCH_SETTING_KEY,
            value_json=payload,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={
                "value_json": statement.excluded.value_json,
                "updated_at": now_utc(),
            },
        )
        session.execute(statement)


def _canonical_hour_text(value: datetime) -> str:
    return (
        require_utc_hour(
            "automatic-fetch cursor hour",
            value,
        )
        .isoformat()
        .replace("+00:00", "Z")
    )


def _parse_canonical_hour_text(
    value: object,
) -> datetime | None:
    text_value = str(value or "").strip()

    if not text_value or not text_value.endswith("Z"):
        return None

    try:
        parsed = datetime.fromisoformat(
            text_value[:-1] + "+00:00",
        )
        hour = require_utc_hour(
            "automatic-fetch cursor hour",
            parsed,
        )
    except TypeError, ValueError:
        return None

    if _canonical_hour_text(hour) != text_value:
        return None

    return hour


def _read_retry_cursor_sync(
    newest_eligible_hour_utc: datetime,
) -> datetime | None:
    """Read a cursor only when it belongs to the current release window."""

    newest = require_utc_hour(
        "newest_eligible_hour_utc",
        newest_eligible_hour_utc,
    )

    with session_scope() as session:
        value = session.scalar(
            select(AppSetting.value_json).where(
                AppSetting.key == _AUTOMATIC_FETCH_RETRY_CURSOR_SETTING_KEY
            )
        )

    if not isinstance(value, dict):
        return None

    if value.get("schema") != "l2shock.automatic_fetch_retry_cursor":
        return None

    if value.get("schema_version") != 1:
        return None

    stored_newest = _parse_canonical_hour_text(
        value.get("newest_eligible_hour_utc"),
    )
    attempted = _parse_canonical_hour_text(
        value.get("last_attempted_hour_utc"),
    )

    if stored_newest != newest:
        # A new release-eligible hour resets fairness rotation so the newest
        # hour is always protected first.
        return None

    return attempted


def _write_retry_cursor_sync(
    *,
    newest_eligible_hour_utc: datetime,
    attempted_hour_utc: datetime,
) -> None:
    newest = require_utc_hour(
        "newest_eligible_hour_utc",
        newest_eligible_hour_utc,
    )
    attempted = require_utc_hour(
        "attempted_hour_utc",
        attempted_hour_utc,
    )

    payload = {
        "schema": "l2shock.automatic_fetch_retry_cursor",
        "schema_version": 1,
        "newest_eligible_hour_utc": _canonical_hour_text(newest),
        "last_attempted_hour_utc": _canonical_hour_text(attempted),
        "updated_at_utc": now_utc().isoformat(),
    }

    with session_scope() as session:
        statement = postgresql_insert(AppSetting).values(
            key=_AUTOMATIC_FETCH_RETRY_CURSOR_SETTING_KEY,
            value_json=payload,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={
                "value_json": statement.excluded.value_json,
                "updated_at": now_utc(),
            },
        )
        session.execute(statement)


async def _write_retry_cursor_durably(
    *,
    newest_eligible_hour_utc: datetime,
    attempted_hour_utc: datetime,
) -> None:
    """Persist retry rotation before propagating owner-task cancellation."""

    task = asyncio.create_task(
        asyncio.to_thread(
            _write_retry_cursor_sync,
            newest_eligible_hour_utc=newest_eligible_hour_utc,
            attempted_hour_utc=attempted_hour_utc,
        ),
        name="l2shock-automatic-fetch-retry-cursor",
    )

    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        # Native cancellation cannot terminate the synchronous DB worker.
        # Explicitly await its durable boundary before preserving cancellation.
        await asyncio.shield(task)
        raise


def _clear_retry_cursor_sync() -> None:
    """Remove stale rotation state when the full bounded window is complete."""

    with session_scope() as session:
        row = session.get(
            AppSetting,
            _AUTOMATIC_FETCH_RETRY_CURSOR_SETTING_KEY,
        )

        if row is not None:
            session.delete(row)


def _canonical_sha256_or_none(
    value: object,
) -> str | None:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        return None

    return digest


def _source_row_counts_as_complete(
    *,
    venue: object,
    instrument: object,
    data_kind: object,
    hour_utc: datetime,
    status: object,
    local_path: object,
    file_size_bytes: object,
    content_sha256: object,
    raw_root: Path,
    quality_json: object = None,
) -> bool:
    """Return whether one durable source row satisfies acquisition completeness.

    Downloaded and processing rows require an intact canonical local file.

    A local processed row with ``local_path=None`` counts as complete after
    explicit pruning only when durable size and SHA-256 metadata remain.

    A verified HF-imported processed row counts as complete from its canonical
    source SHA and remote-import ownership marker without pretending that raw
    bytes exist locally.
    """

    digest = _canonical_sha256_or_none(content_sha256)

    if digest is None:
        return False

    try:
        spec = SourceFileSpec(
            provider="cryptohftdata",
            venue=str(venue),
            symbol=str(instrument),
            data_kind=SourceDataKind(str(data_kind)),
            hour_utc=hour_utc,
        )
    except TypeError, ValueError:
        return False

    status_text = str(status or "").strip().lower()

    if status_text not in {
        "downloaded",
        "processing",
        "processed",
    }:
        return False

    quality = quality_json if isinstance(quality_json, Mapping) else {}
    remote_imported = (
        quality.get("processing_origin") == "hugging_face_remote_import_v1"
    )

    path_text = str(local_path or "").strip()

    if status_text == "processed" and not path_text:
        if remote_imported:
            return True

        return bool(
            not isinstance(file_size_bytes, bool)
            and isinstance(file_size_bytes, int)
            and file_size_bytes > 0
        )

    if (
        isinstance(file_size_bytes, bool)
        or not isinstance(file_size_bytes, int)
        or file_size_bytes <= 0
    ):
        return False

    if not path_text:
        return False

    stored_path = Path(path_text).expanduser()

    try:
        if stored_path.is_symlink():
            return False

        resolved_path = stored_path.resolve()
        canonical_path = spec.local_path(raw_root)

        if resolved_path != canonical_path:
            return False

        if not resolved_path.is_file():
            return False

        return resolved_path.stat().st_size == file_size_bytes

    except OSError:
        return False


def _complete_source_hours_sync(
    start_utc: datetime,
    end_utc: datetime,
) -> frozenset[datetime]:
    """Return acquisition-complete source hours in ``[start, end)``.

    Processed rows remain complete after explicit raw pruning. Downloaded and
    processing rows require an intact canonical local archive.
    """

    start = require_utc_hour(
        "start_utc",
        start_utc,
    )
    end = require_utc_hour(
        "end_utc",
        end_utc,
    )

    if end <= start:
        raise ValueError("end_utc must be after start_utc")

    raw_root = get_settings().storage.raw_path

    with session_scope() as session:
        rows = session.execute(
            select(
                SourceHour.hour_utc,
                SourceHour.venue,
                SourceHour.instrument,
                SourceHour.data_kind,
                SourceHour.status,
                SourceHour.local_path,
                SourceHour.file_size_bytes,
                SourceHour.content_sha256,
                SourceHour.quality_json,
            )
            .where(SourceHour.provider == "cryptohftdata")
            .where(
                SourceHour.venue.in_(
                    (
                        "binance_futures",
                        "bybit",
                        "okx_futures",
                    )
                )
            )
            .where(SourceHour.hour_utc >= start)
            .where(SourceHour.hour_utc < end)
            .where(
                SourceHour.instrument.in_(
                    (
                        "BTCUSDT",
                        "ETHUSDT",
                        "BTC-USDT-SWAP",
                        "ETH-USDT-SWAP",
                    )
                )
            )
            .where(SourceHour.data_kind.in_(("orderbook", "trades")))
        ).all()

    available_by_hour: dict[
        datetime,
        set[tuple[str, str, str]],
    ] = {}

    for (
        hour_utc,
        venue,
        instrument,
        data_kind,
        status,
        local_path,
        file_size_bytes,
        digest,
        quality_json,
    ) in rows:
        hour = require_utc_hour(
            "stored source hour",
            hour_utc,
        )

        if not _source_row_counts_as_complete(
            venue=venue,
            instrument=instrument,
            data_kind=data_kind,
            hour_utc=hour,
            status=status,
            local_path=local_path,
            file_size_bytes=file_size_bytes,
            content_sha256=digest,
            raw_root=raw_root,
            quality_json=quality_json,
        ):
            continue

        available_by_hour.setdefault(
            hour,
            set(),
        ).add(
            (
                str(venue),
                str(instrument),
                str(data_kind),
            )
        )

    return frozenset(
        hour
        for hour, identities in available_by_hour.items()
        if _REQUIRED_SOURCE_IDENTITIES.issubset(identities)
    )


class AutomaticFetchRuntime:
    """Own one process-local automatic-fetch polling loop."""

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._coordinator = None

        self._enabled = False
        self._stop_requested = False
        self._current_target_hour_utc: datetime | None = None
        self._last_poll_at: datetime | None = None
        self._next_poll_at: datetime | None = None
        self._last_result: ManualFetchResult | None = None
        self._last_error: str | None = None
        self._completion_sequence = 0

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    @property
    def is_running(self) -> bool:
        task = self._task
        return task is not None and not task.done()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def snapshot(self) -> AutomaticFetchRuntimeSnapshot:
        operation_id: UUID | None = None

        if self._coordinator is not None:
            operation_id = self._coordinator.active_operation_id

        return AutomaticFetchRuntimeSnapshot(
            enabled=self._enabled,
            is_running=self.is_running,
            stop_requested=self._stop_requested,
            current_operation_id=(
                str(operation_id) if operation_id is not None else None
            ),
            current_target_hour_utc=self._current_target_hour_utc,
            last_poll_at=self._last_poll_at,
            next_poll_at=self._next_poll_at,
            last_result=self._last_result,
            last_error=self._last_error,
            completion_sequence=self._completion_sequence,
        )

    async def restore_persisted_state(self) -> bool:
        """Start the loop when persisted configuration says enabled."""

        enabled = await asyncio.to_thread(_read_enabled_setting_sync)

        if not enabled:
            self._enabled = False
            return False

        self.start(persist=False)
        return True

    def start(
        self,
        *,
        persist: bool = True,
    ) -> asyncio.Task[None]:
        state = get_state()

        if state.shutdown_started:
            raise RuntimeError(
                "Application shutdown has started; automatic fetch is blocked"
            )

        if self.is_running:
            assert self._task is not None
            return self._task

        if state.active_operation_name:
            raise FetchOperationBusyError(
                "Automatic Fetch cannot start while another operation is "
                f"active: {state.active_operation_name}"
            )

        self._enabled = True
        self._stop_requested = False
        self._stop_event = asyncio.Event()
        self._last_error = None

        task = asyncio.create_task(
            self._run(
                persist_enabled=persist,
            ),
            name="l2shock-automatic-fetch",
        )

        self._task = task
        state.tracked_tasks.add(task)

        return task

    def request_stop(self) -> bool:
        stop_event = self._stop_event

        if stop_event is None or stop_event.is_set():
            return False

        self._stop_requested = True
        stop_event.set()

        coordinator = self._coordinator
        if coordinator is not None:
            coordinator.request_stop()

        return True

    async def stop_and_wait(
        self,
        *,
        grace_seconds: float,
        persist: bool = True,
    ) -> bool:
        task = self._task

        self._enabled = False

        if persist:
            try:
                await asyncio.to_thread(
                    _write_enabled_setting_sync,
                    False,
                )
            except Exception:
                log.exception("Could not persist automatic-fetch disabled state.")

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

    async def _wait_until_next_poll(
        self,
        stop_event: asyncio.Event,
        seconds: float,
    ) -> bool:
        timeout = max(0.0, float(seconds))
        self._next_poll_at = now_utc() + timedelta(seconds=timeout)

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=timeout,
            )
        except TimeoutError:
            return False

        return True

    async def _run(
        self,
        *,
        persist_enabled: bool,
    ) -> None:
        state = get_state()
        current_task = asyncio.current_task()
        stop_event = self._stop_event

        if stop_event is None:
            raise RuntimeError("Automatic fetch stop event was not initialized")

        settings = get_settings()
        poll_seconds = settings.cryptohft.release_poll_interval_minutes * 60

        try:
            if persist_enabled:
                await asyncio.to_thread(
                    _write_enabled_setting_sync,
                    True,
                )

            while not stop_event.is_set():
                self._last_poll_at = now_utc()

                newest_eligible_hour = latest_release_eligible_hour(
                    self._last_poll_at,
                    release_delay_minutes=(
                        settings.cryptohft.expected_release_delay_minutes
                    ),
                )
                catch_up_hours = settings.cryptohft.automatic_fetch_catch_up_hours
                catch_up_start = newest_eligible_hour - timedelta(
                    hours=catch_up_hours - 1
                )

                try:
                    complete_hours = await asyncio.to_thread(
                        _complete_source_hours_sync,
                        catch_up_start,
                        newest_eligible_hour + timedelta(hours=1),
                    )

                    retry_cursor = await asyncio.to_thread(
                        _read_retry_cursor_sync,
                        newest_eligible_hour,
                    )

                    target_hour = select_automatic_fetch_target_hour(
                        self._last_poll_at,
                        release_delay_minutes=(
                            settings.cryptohft.expected_release_delay_minutes
                        ),
                        catch_up_hours=catch_up_hours,
                        complete_hours=complete_hours,
                        after_hour_utc=retry_cursor,
                    )
                    self._current_target_hour_utc = target_hour

                    if target_hour is None:
                        self._last_error = None

                        if retry_cursor is not None:
                            await asyncio.to_thread(
                                _clear_retry_cursor_sync,
                            )

                    elif state.active_operation_name:
                        self._last_error = (
                            "Automatic fetch deferred because another "
                            f"operation is active: {state.active_operation_name}"
                        )

                    else:
                        state.active_operation_name = "automatic_fetch"
                        state.active_operation_started_at = now_utc()

                        try:
                            coordinator = create_production_manual_fetch_coordinator(
                                operation_lock=state.operation_lock,
                            )
                            self._coordinator = coordinator

                            try:
                                result = await coordinator.run(
                                    requested_start_utc=target_hour,
                                    requested_end_utc=(
                                        target_hour + timedelta(hours=1)
                                    ),
                                    run_kind=FetchRunKind.AUTOMATIC,
                                )
                            finally:
                                # Advance fairness after every real coordinator
                                # attempt, including a normal missing/error
                                # result. On the next poll, the selector tries
                                # the next older incomplete hour. When a newer
                                # source hour becomes release-eligible, cursor
                                # ownership resets automatically.
                                await _write_retry_cursor_durably(
                                    newest_eligible_hour_utc=newest_eligible_hour,
                                    attempted_hour_utc=target_hour,
                                )

                            self._last_result = result
                            self._last_error = None

                        finally:
                            self._coordinator = None

                            if state.active_operation_name == "automatic_fetch":
                                state.active_operation_name = ""
                                state.active_operation_started_at = None

                except FetchOperationBusyError:
                    self._last_error = (
                        "Automatic fetch deferred because the global "
                        "operation lock is busy"
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._last_error = f"Unexpected {type(exc).__name__}"
                    log.exception("Automatic fetch polling iteration failed.")
                finally:
                    self._completion_sequence += 1

                stopped = await self._wait_until_next_poll(
                    stop_event,
                    poll_seconds,
                )

                if stopped:
                    break

        finally:
            self._enabled = False
            self._stop_requested = False
            self._coordinator = None
            self._current_target_hour_utc = None
            self._next_poll_at = None
            self._stop_event = None

            if state.active_operation_name == "automatic_fetch":
                state.active_operation_name = ""
                state.active_operation_started_at = None

            if current_task is not None:
                state.tracked_tasks.discard(current_task)

            if self._task is current_task:
                self._task = None


_runtime: AutomaticFetchRuntime | None = None
_runtime_loop: asyncio.AbstractEventLoop | None = None


def get_automatic_fetch_runtime() -> AutomaticFetchRuntime:
    """Return the runtime bound to the current event loop."""

    global _runtime, _runtime_loop

    loop = asyncio.get_running_loop()

    if _runtime is None:
        _runtime = AutomaticFetchRuntime()
        _runtime_loop = loop
        return _runtime

    if _runtime_loop is not loop:
        raise RuntimeError(
            "Automatic fetch runtime belongs to another asyncio event loop"
        )

    return _runtime


def peek_automatic_fetch_runtime() -> AutomaticFetchRuntime | None:
    return _runtime


def reset_automatic_fetch_runtime_for_tests() -> None:
    global _runtime, _runtime_loop

    _runtime = None
    _runtime_loop = None


__all__ = [
    "AutomaticFetchRuntime",
    "AutomaticFetchRuntimeSnapshot",
    "get_automatic_fetch_runtime",
    "latest_release_eligible_hour",
    "peek_automatic_fetch_runtime",
    "reset_automatic_fetch_runtime_for_tests",
    "select_automatic_fetch_target_hour",
]

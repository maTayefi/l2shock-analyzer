# l2shock/ui/shutdown.py
"""Coordinated application shutdown.

Shutdown sequence:

1. raise the process admission barrier;
2. stop automatic fetch (cooperative, then bounded force-cancel);
3. request cooperative manual-fetch cancellation;
4. force-cancel manual fetch only if its grace period expires;
5. request cooperative remote-import cancellation;
6. force-cancel remote import only if its grace period expires;
7. request cooperative processing cancellation;
8. force-cancel processing only if its grace period expires;
9. request cooperative Shock-Start review stop and wait a bounded time;
   the synchronous detector has no cancellation hook, so there is no
   forced step: a worker outliving the grace period blocks engine disposal;
10. cancel and await remaining unrelated tracked tasks;
11. dispose SQLAlchemy connection pools;
12. optionally request NiceGUI server shutdown.

Cancelling an owning asyncio task never assumes that asyncio can terminate a
worker thread. If any owner cannot prove its worker stopped, the engine is not
disposed and shutdown is not published as complete.
"""

from __future__ import annotations

import inspect
import logging
from typing import Final

from nicegui import app as nicegui_app

from l2shock.db.engine import reset_engine
from l2shock.ui.automatic_fetch_runtime import (
    peek_automatic_fetch_runtime,
)
from l2shock.ui.components import cancel_and_wait_for_tracked_tasks
from l2shock.ui.fetch_runtime import peek_manual_fetch_runtime
from l2shock.ui.processing_runtime import peek_manual_processing_runtime
from l2shock.ui.remote_import_runtime import peek_remote_import_runtime
from l2shock.ui.shock_runtime import peek_manual_shock_runtime
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)

_FETCH_GRACE_SECONDS: Final[float] = 15.0
_FETCH_FORCE_CANCEL_SECONDS: Final[float] = 5.0
_REMOTE_IMPORT_GRACE_SECONDS: Final[float] = 30.0
_REMOTE_IMPORT_FORCE_CANCEL_SECONDS: Final[float] = 15.0
_PROCESSING_GRACE_SECONDS: Final[float] = 30.0
_PROCESSING_FORCE_CANCEL_SECONDS: Final[float] = 15.0
_ANALYSIS_GRACE_SECONDS: Final[float] = 30.0
_OTHER_TASK_TIMEOUT_SECONDS: Final[float] = 10.0


_OPERATION_LOCK_WAIT_SECONDS: float = 30.0


async def _wait_for_operation_lock_release(
    *,
    timeout_seconds: float,
) -> bool:
    """Wait (bounded) for an untracked operation-lock owner to finish.

    Settings maintenance and preset mutations own the process operation lock
    through a shielded worker thread without a runtime handle. Shutdown must
    not dispose the engine while such a worker may still use a session.
    """
    import asyncio

    lock = get_state().operation_lock
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(timeout_seconds))

    while lock.locked():
        if loop.time() >= deadline:
            return False

        await asyncio.sleep(0.05)

    return True


async def shutdown_runtime(
    *,
    request_server_stop: bool,
    fetch_grace_seconds: float = _FETCH_GRACE_SECONDS,
    fetch_force_cancel_seconds: float = _FETCH_FORCE_CANCEL_SECONDS,
    remote_import_grace_seconds: float = _REMOTE_IMPORT_GRACE_SECONDS,
    remote_import_force_cancel_seconds: float = (_REMOTE_IMPORT_FORCE_CANCEL_SECONDS),
    processing_grace_seconds: float = _PROCESSING_GRACE_SECONDS,
    processing_force_cancel_seconds: float = (_PROCESSING_FORCE_CANCEL_SECONDS),
    analysis_grace_seconds: float = _ANALYSIS_GRACE_SECONDS,
    other_task_timeout_seconds: float = _OTHER_TASK_TIMEOUT_SECONDS,
    operation_lock_wait_seconds: float = _OPERATION_LOCK_WAIT_SECONDS,
) -> None:
    """Perform idempotent bounded application shutdown."""
    state = get_state()

    async with state.shutdown_lock:
        if state.shutdown_complete:
            if request_server_stop:
                await _request_nicegui_shutdown()
            return

        # This admission barrier must be raised before waiting for active work.
        # Otherwise another client could start a new operation during shutdown.
        state.shutdown_started = True
        log.info("Graceful application shutdown started.")

        all_operation_owners_stopped = True

        automatic_fetch_runtime = peek_automatic_fetch_runtime()

        if automatic_fetch_runtime is not None:
            stopped = await automatic_fetch_runtime.stop_and_wait(
                grace_seconds=fetch_grace_seconds,
                persist=False,
            )

            if not stopped:
                stopped = await automatic_fetch_runtime.force_cancel_and_wait(
                    timeout_seconds=fetch_force_cancel_seconds,
                )

            if not stopped:
                all_operation_owners_stopped = False
                log.error(
                    "Automatic-fetch task remained pending after forced "
                    "cancellation timeout."
                )

        fetch_runtime = peek_manual_fetch_runtime()

        if fetch_runtime is not None and fetch_runtime.is_running:
            log.info("Requesting cooperative manual-fetch stop.")

            fetch_stopped = await fetch_runtime.stop_and_wait(
                grace_seconds=fetch_grace_seconds,
            )

            if not fetch_stopped:
                log.warning(
                    "Manual fetch did not stop within %.3f seconds; "
                    "requesting native task cancellation.",
                    fetch_grace_seconds,
                )

                fetch_stopped = await fetch_runtime.force_cancel_and_wait(
                    timeout_seconds=fetch_force_cancel_seconds,
                )

                if not fetch_stopped:
                    all_operation_owners_stopped = False
                    log.error(
                        "Manual fetch task remained pending after forced "
                        "cancellation timeout."
                    )

        remote_import_runtime = peek_remote_import_runtime()

        if remote_import_runtime is not None and remote_import_runtime.is_running:
            log.info("Requesting cooperative remote-import stop.")

            remote_import_stopped = await remote_import_runtime.stop_and_wait(
                grace_seconds=remote_import_grace_seconds,
            )

            if not remote_import_stopped:
                log.warning(
                    "Remote import did not stop within %.3f seconds; "
                    "requesting bounded task cancellation.",
                    remote_import_grace_seconds,
                )

                remote_import_stopped = (
                    await remote_import_runtime.force_cancel_and_wait(
                        timeout_seconds=(remote_import_force_cancel_seconds),
                    )
                )

                if not remote_import_stopped:
                    all_operation_owners_stopped = False
                    log.error(
                        "Remote-import task remained pending after "
                        "forced cancellation timeout."
                    )

        processing_runtime = peek_manual_processing_runtime()

        if processing_runtime is not None and processing_runtime.is_running:
            log.info("Requesting cooperative manual-processing stop.")

            processing_stopped = await processing_runtime.stop_and_wait(
                grace_seconds=processing_grace_seconds,
            )

            if not processing_stopped:
                log.warning(
                    "Manual processing did not stop within %.3f seconds; "
                    "requesting bounded task cancellation while preserving "
                    "the worker's cooperative cancellation event.",
                    processing_grace_seconds,
                )

                processing_stopped = await processing_runtime.force_cancel_and_wait(
                    timeout_seconds=processing_force_cancel_seconds,
                )

                if not processing_stopped:
                    all_operation_owners_stopped = False
                    log.error(
                        "Manual processing task remained pending after forced "
                        "cancellation timeout."
                    )

        shock_runtime = peek_manual_shock_runtime()

        if shock_runtime is not None and shock_runtime.snapshot().is_running:
            log.info("Requesting cooperative Shock-Start review stop.")

            shock_stopped = await shock_runtime.stop_and_wait(
                grace_seconds=analysis_grace_seconds,
            )

            if not shock_stopped:
                # The synchronous detector has no cancellation callback.
                # In particular, do not mark this owner stopped or dispose
                # the SQLAlchemy engine while its worker may still run.
                all_operation_owners_stopped = False
                log.error(
                    "Shock-Start worker did not exit within %.3f seconds; "
                    "database engine disposal is blocked.",
                    analysis_grace_seconds,
                )

        if all_operation_owners_stopped and state.operation_lock.locked():
            owner = state.active_operation_name or "unknown"
            log.info(
                "Waiting for operation-lock owner %r to finish before "
                "engine disposal.",
                owner,
            )

            lock_released = await _wait_for_operation_lock_release(
                timeout_seconds=operation_lock_wait_seconds,
            )

            if not lock_released:
                all_operation_owners_stopped = False
                log.error(
                    "Operation-lock owner %r did not finish within %.3f "
                    "seconds; database engine disposal is blocked.",
                    owner,
                    operation_lock_wait_seconds,
                )

        pending = await cancel_and_wait_for_tracked_tasks(
            timeout_seconds=other_task_timeout_seconds,
        )

        if pending:
            log.error(
                "%d tracked task(s) remained pending at shutdown.",
                pending,
            )

        if not all_operation_owners_stopped or pending:
            log.critical(
                "Shutdown could not prove that every worker stopped. "
                "The admission barrier remains active, but the SQLAlchemy "
                "engine will not be disposed and shutdown will not be "
                "published as complete."
            )
            raise RuntimeError(
                "Application shutdown timed out while background work "
                "remained active"
            )

        reset_engine()

        state.active_operation_name = ""
        state.active_operation_started_at = None
        state.shutdown_complete = True

        log.info("Graceful application shutdown cleanup completed.")

    if request_server_stop:
        await _request_nicegui_shutdown()


async def _request_nicegui_shutdown() -> None:
    """Request framework shutdown while tolerating sync/async API forms."""
    try:
        result = nicegui_app.shutdown()

        if inspect.isawaitable(result):
            await result
    except Exception:
        log.exception("NiceGUI shutdown request failed.")
        raise


__all__ = ["shutdown_runtime"]

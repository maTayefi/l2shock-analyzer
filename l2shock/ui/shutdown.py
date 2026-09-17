# l2shock/ui/shutdown.py
"""Coordinated application shutdown.

Shutdown sequence:

1. raise the process admission barrier;
2. request cooperative fetch cancellation;
3. wait for bounded fetch finalization;
4. force-cancel the fetch only if its grace period expires;
5. request cooperative remote-import cancellation;
6. wait for bounded remote-import finalization;
7. force-cancel remote import only if its grace period expires;
8. request cooperative processing cancellation;
9. wait for bounded processing finalization;
10. force-cancel processing only if its grace period expires;
11. request cooperative analysis cancellation;
12. wait for bounded analysis finalization;
13. force-cancel analysis only if its grace period expires;
14. cancel and await remaining unrelated tracked tasks;
15. dispose SQLAlchemy connection pools;
16. optionally request NiceGUI server shutdown.

Processing cancellation remains cooperative at the synchronous worker boundary.
Cancelling the owning asyncio task does not assume that asyncio can directly
terminate a worker thread.
"""

from __future__ import annotations

import inspect
import logging
from typing import Final

from nicegui import app as nicegui_app

from l2shock.db.engine import reset_engine
from l2shock.ui.analysis_runtime import peek_manual_analysis_runtime
from l2shock.ui.components import cancel_and_wait_for_tracked_tasks
from l2shock.ui.fetch_runtime import peek_manual_fetch_runtime
from l2shock.ui.automatic_fetch_runtime import (
    peek_automatic_fetch_runtime,
)
from l2shock.ui.processing_runtime import peek_manual_processing_runtime
from l2shock.ui.remote_import_runtime import peek_remote_import_runtime
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)

_FETCH_GRACE_SECONDS: Final[float] = 15.0
_FETCH_FORCE_CANCEL_SECONDS: Final[float] = 5.0
_REMOTE_IMPORT_GRACE_SECONDS: Final[float] = 30.0
_REMOTE_IMPORT_FORCE_CANCEL_SECONDS: Final[float] = 15.0
_PROCESSING_GRACE_SECONDS: Final[float] = 30.0
_PROCESSING_FORCE_CANCEL_SECONDS: Final[float] = 15.0
_ANALYSIS_GRACE_SECONDS: Final[float] = 30.0
_ANALYSIS_FORCE_CANCEL_SECONDS: Final[float] = 15.0
_OTHER_TASK_TIMEOUT_SECONDS: Final[float] = 10.0


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
    analysis_force_cancel_seconds: float = (_ANALYSIS_FORCE_CANCEL_SECONDS),
    other_task_timeout_seconds: float = _OTHER_TASK_TIMEOUT_SECONDS,
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

        analysis_runtime = peek_manual_analysis_runtime()

        if analysis_runtime is not None and analysis_runtime.is_running:
            log.info("Requesting cooperative manual-analysis stop.")

            analysis_stopped = await analysis_runtime.stop_and_wait(
                grace_seconds=analysis_grace_seconds,
            )

            if not analysis_stopped:
                log.warning(
                    "Manual analysis did not stop within %.3f seconds; "
                    "requesting bounded task cancellation while preserving "
                    "the worker's cooperative cancellation event.",
                    analysis_grace_seconds,
                )

                analysis_stopped = await analysis_runtime.force_cancel_and_wait(
                    timeout_seconds=analysis_force_cancel_seconds,
                )

                if not analysis_stopped:
                    all_operation_owners_stopped = False
                    log.error(
                        "Manual analysis task remained pending after forced "
                        "cancellation timeout."
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

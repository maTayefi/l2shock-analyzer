# l2shock/ui/components.py
"""Reusable NiceGUI and background-task helpers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from nicegui import context as nicegui_context
from nicegui import ui

from l2shock.ui.state import get_state

log = logging.getLogger(__name__)


def persistent_notify(
    message: str,
    *,
    title: str = "Important",
    notification_type: str = "warning",
) -> None:
    """Show a persistent user-dismissible warning.

    Use this for errors or reliability warnings which the user must explicitly
    see. Ordinary informational messages should use normal finite-duration
    notifications.
    """
    text = str(message or "").strip()
    if not text:
        return

    rendered = f"{title}: {text}"

    try:
        ui.notify(
            rendered,
            type=notification_type,
            timeout=0,
            close_button=True,
            multi_line=True,
        )
        return
    except TypeError:
        # Compatibility fallback if a future/alternate NiceGUI build changes
        # close_button handling.
        try:
            ui.notify(
                rendered,
                type=notification_type,
                timeout=0,
                multi_line=True,
            )
            return
        except Exception:
            pass
    except Exception:
        pass

    log.error("%s", rendered)


def create_tracked_task(
    coroutine: Coroutine[Any, Any, Any],
    *,
    name: str | None = None,
) -> asyncio.Task[Any] | None:
    """Create a strongly referenced task tied to current client context."""
    state = get_state()

    if state.shutdown_started:
        try:
            coroutine.close()
        except Exception:
            pass
        log.debug("Skipped task creation because shutdown has started.")
        return None

    try:
        client = nicegui_context.client
    except Exception:
        client = None

    inner_started = False

    async def _runner() -> Any:
        nonlocal inner_started

        if client is None:
            inner_started = True
            return await coroutine

        with client:
            inner_started = True
            return await coroutine

    runner = _runner()

    try:
        task = asyncio.create_task(runner, name=name)
    except RuntimeError:
        runner.close()
        if not inner_started:
            try:
                coroutine.close()
            except Exception:
                pass
        log.debug("Could not create tracked task: no running event loop.")
        return None

    state.tracked_tasks.add(task)

    def _task_done(done_task: asyncio.Task[Any]) -> None:
        state.tracked_tasks.discard(done_task)

        if not inner_started:
            try:
                coroutine.close()
            except Exception:
                log.debug(
                    "Could not close unstarted tracked coroutine.",
                    exc_info=True,
                )

        try:
            done_task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Tracked UI task failed.")

    task.add_done_callback(_task_done)
    return task


async def cancel_and_wait_for_tracked_tasks(
    *,
    timeout_seconds: float = 10.0,
) -> int:
    """Cancel tracked tasks and wait for a bounded interval.

    Returns the number of tasks which remained pending after the timeout.
    The current shutdown task is never cancelled by this helper.
    """
    state = get_state()
    state.shutdown_started = True

    timeout = max(0.0, float(timeout_seconds))
    current = asyncio.current_task()

    tasks = {
        task
        for task in list(state.tracked_tasks)
        if task is not current and not task.done()
    }

    for task in tasks:
        task.cancel()

    if not tasks:
        return 0

    done, pending = await asyncio.wait(
        tasks,
        timeout=timeout,
        return_when=asyncio.ALL_COMPLETED,
    )

    for task in done:
        state.tracked_tasks.discard(task)

        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception(
                "Tracked task failed during shutdown.",
            )

    for task in pending:
        log.error(
            "Tracked task did not stop within %.3f seconds: %s",
            timeout,
            task.get_name(),
        )

    return len(pending)


def section_header(title: str) -> Any:
    return ui.label(title).classes(
        "text-base font-semibold text-slate-800 dark:text-slate-100"
    )


__all__ = [
    "cancel_and_wait_for_tracked_tasks",
    "create_tracked_task",
    "persistent_notify",
    "section_header",
]

# l2shock/ui/state.py
"""Process-local runtime state.

Durable analytical and fetch truth belongs in PostgreSQL. This module owns only
the current process's task, shutdown, and operation-coordination state.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from l2shock.timeutils import now_utc


@dataclass
class RuntimeState:
    process_started_at: datetime = field(default_factory=now_utc)

    # shutdown_started is the admission barrier. Once true, no new fetch,
    # processing, analysis, or destructive Settings operation may begin.
    shutdown_started: bool = False
    shutdown_complete: bool = False

    active_operation_name: str = ""
    active_operation_started_at: datetime | None = None

    tracked_tasks: set[asyncio.Task[Any]] = field(
        default_factory=set,
        repr=False,
    )

    _operation_lock: asyncio.Lock | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _operation_lock_loop: asyncio.AbstractEventLoop | None = field(
        default=None,
        init=False,
        repr=False,
    )

    _shutdown_lock: asyncio.Lock | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _shutdown_lock_loop: asyncio.AbstractEventLoop | None = field(
        default=None,
        init=False,
        repr=False,
    )

    @property
    def operation_lock(self) -> asyncio.Lock:
        """Return the global process operation lock for the active event loop.

        Manual fetch, manual source processing, automatic fetch, and
        destructive Settings operations share this lock. PostgreSQL advisory
        locks remain the cross-process correctness mechanism.
        """
        loop = asyncio.get_running_loop()

        if self._operation_lock is None:
            self._operation_lock = asyncio.Lock()
            self._operation_lock_loop = loop
            return self._operation_lock

        if self._operation_lock_loop is not loop:
            raise RuntimeError(
                "RuntimeState.operation_lock belongs to another asyncio event "
                "loop. Restart the process instead of reusing runtime state "
                "across event-loop generations."
            )

        return self._operation_lock

    @property
    def shutdown_lock(self) -> asyncio.Lock:
        """Serialize duplicate UI, signal, and framework shutdown callbacks."""
        loop = asyncio.get_running_loop()

        if self._shutdown_lock is None:
            self._shutdown_lock = asyncio.Lock()
            self._shutdown_lock_loop = loop
            return self._shutdown_lock

        if self._shutdown_lock_loop is not loop:
            raise RuntimeError(
                "RuntimeState.shutdown_lock belongs to another asyncio event " "loop."
            )

        return self._shutdown_lock


_state: RuntimeState | None = None


def get_state() -> RuntimeState:
    global _state

    if _state is None:
        _state = RuntimeState()

    return _state


def reset_state_for_tests() -> None:
    """Reset process-local state for isolated tests only."""
    global _state
    _state = None


__all__ = [
    "RuntimeState",
    "get_state",
    "reset_state_for_tests",
]

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone

import pytest

import l2shock.ui.automatic_fetch_runtime as module


@pytest.mark.asyncio
async def test_retry_cursor_write_waits_through_repeated_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()

    def blocking_write(**_kwargs: object) -> None:
        entered.set()
        try:
            if not release.wait(timeout=5.0):
                raise TimeoutError("Retry-cursor test did not release its worker")
        finally:
            exited.set()

    monkeypatch.setattr(
        module,
        "_write_retry_cursor_sync",
        blocking_write,
    )

    hour = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)
    task = asyncio.create_task(
        module._write_retry_cursor_durably(
            newest_eligible_hour_utc=hour,
            attempted_hour_utc=hour,
        )
    )

    assert await asyncio.to_thread(entered.wait, 1.0)

    try:
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not exited.is_set()
        assert not task.done()
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert exited.is_set()

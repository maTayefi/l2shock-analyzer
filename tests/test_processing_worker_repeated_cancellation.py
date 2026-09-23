from __future__ import annotations

import asyncio
import threading

import pytest

from l2shock.ui.processing_runtime import ManualProcessingRuntime


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_processing_thread() -> None:
    runtime = object.__new__(ManualProcessingRuntime)
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    cancellation_event = threading.Event()

    def blocking_worker() -> object:
        entered.set()
        try:
            if not release.wait(timeout=5.0):
                raise TimeoutError("Test did not release the worker")
            return object()
        finally:
            finished.set()

    task = asyncio.create_task(
        runtime._run_sync_worker(
            blocking_worker,
            cancellation_event=cancellation_event,
        )
    )

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert cancellation_event.is_set()
        assert not finished.is_set()
        assert not task.done()
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert finished.is_set()

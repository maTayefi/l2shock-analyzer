from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import l2shock.ui.processing_runtime as processing_module


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_target_loader_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocking_loader(
        start_utc: datetime,
        end_utc: datetime,
        lower_fraction: Decimal,
        upper_fraction: Decimal,
    ) -> tuple[()]:
        assert end_utc > start_utc
        assert lower_fraction == Decimal("0")
        assert upper_fraction == Decimal("0.01")
        entered.set()

        try:
            if not release.wait(timeout=5.0):
                raise TimeoutError("Test did not release the target loader")
            return ()
        finally:
            finished.set()

    monkeypatch.setattr(
        processing_module,
        "load_materialization_processing_targets",
        blocking_loader,
    )

    start = datetime(2080, 1, 1, 12, tzinfo=timezone.utc)
    end = datetime(2080, 1, 1, 13, tzinfo=timezone.utc)

    task = asyncio.create_task(
        processing_module._production_target_loader(
            start,
            end,
            Decimal("0"),
            Decimal("0.01"),
        )
    )

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert not finished.is_set()
        assert not task.done()
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert finished.is_set()

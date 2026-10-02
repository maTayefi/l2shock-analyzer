from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.ui.remote_import_runtime import RemoteImportRuntime
from l2shock.ui.state import get_state, reset_state_for_tests


class _MissingArtifactRepository:
    def __init__(self) -> None:
        self.revision_calls = 0
        self.download_calls = 0

    def current_revision(self) -> str:
        self.revision_calls += 1
        return "a" * 40

    def download_artifact(self, key, *, revision=None):
        del key, revision
        self.download_calls += 1
        return None


def _start(runtime: RemoteImportRuntime):
    start = datetime(2026, 10, 1, 10, tzinfo=timezone.utc)

    return runtime.start(
        requested_start_utc=start,
        requested_end_utc=start + timedelta(hours=1),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.25"),
    )


def test_remote_import_without_loop_leaves_no_reservation() -> None:
    reset_state_for_tests()
    state = get_state()
    repository = _MissingArtifactRepository()
    runtime = RemoteImportRuntime(repository=repository)

    before = runtime.snapshot()

    with pytest.raises(RuntimeError, match="running event loop"):
        _start(runtime)

    assert runtime.snapshot() == before
    assert runtime.task is None
    assert state.active_operation_name == ""
    assert state.active_operation_started_at is None
    assert state.tracked_tasks == set()
    assert repository.revision_calls == 0
    assert repository.download_calls == 0


@pytest.mark.asyncio
async def test_remote_import_prestart_cancel_releases_admission() -> None:
    reset_state_for_tests()
    state = get_state()
    repository = _MissingArtifactRepository()
    runtime = RemoteImportRuntime(repository=repository)

    task = _start(runtime)

    assert state.active_operation_name == "remote_hf_import"
    assert task in state.tracked_tasks
    assert runtime.snapshot().operation_id is not None

    # No await between admission and cancellation.
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0)

    snapshot = runtime.snapshot()

    assert repository.revision_calls == 0
    assert repository.download_calls == 0
    assert runtime.task is None
    assert snapshot.is_running is False
    assert snapshot.operation_id is None
    assert snapshot.started_at is None
    assert snapshot.stop_requested is False
    assert snapshot.completion_sequence == 1
    assert snapshot.last_error == "Remote import was cancelled before it started"
    assert state.active_operation_name == ""
    assert state.active_operation_started_at is None
    assert state.tracked_tasks == set()
    assert state.operation_lock.locked() is False

    # The old reservation must not block another operation.
    result = await _start(runtime)
    await asyncio.sleep(0)

    assert result.missing_count == 4
    assert result.failed_count == 0
    assert runtime.snapshot().completion_sequence == 2
    assert runtime.task is None
    assert state.active_operation_name == ""
    assert state.tracked_tasks == set()


@pytest.mark.asyncio
async def test_remote_prestart_cancel_preserves_newer_same_name_marker() -> None:
    reset_state_for_tests()
    state = get_state()
    repository = _MissingArtifactRepository()
    runtime = RemoteImportRuntime(repository=repository)

    task = _start(runtime)

    original_started_at = state.active_operation_started_at
    assert original_started_at is not None

    replacement_started_at = original_started_at + timedelta(seconds=1)
    state.active_operation_name = "remote_hf_import"
    state.active_operation_started_at = replacement_started_at

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0)

    assert runtime.task is None
    assert runtime.snapshot().operation_id is None
    assert runtime.snapshot().completion_sequence == 1
    assert task not in state.tracked_tasks
    assert state.active_operation_name == "remote_hf_import"
    assert state.active_operation_started_at == replacement_started_at

    reset_state_for_tests()


@pytest.mark.asyncio
async def test_remote_normal_completion_preserves_newer_same_name_marker() -> None:
    reset_state_for_tests()
    state = get_state()

    class _ReplacingRepository(_MissingArtifactRepository):
        def __init__(self) -> None:
            super().__init__()
            self.loop = asyncio.get_running_loop()
            self.replacement_started_at = None

        def current_revision(self) -> str:
            self.revision_calls += 1
            return "a" * 40

        def download_artifact(self, key, *, revision=None):
            del key, revision
            self.download_calls += 1

            if self.download_calls == 1:
                # The repository runs in a worker thread. Schedule state
                # replacement on the application event loop.
                self.loop.call_soon_threadsafe(self._replace_marker)

            return None

        def _replace_marker(self) -> None:
            original = state.active_operation_started_at
            assert original is not None
            self.replacement_started_at = original + timedelta(seconds=1)
            state.active_operation_name = "remote_hf_import"
            state.active_operation_started_at = self.replacement_started_at

    repository = _ReplacingRepository()
    runtime = RemoteImportRuntime(repository=repository)

    result = await _start(runtime)
    await asyncio.sleep(0)

    assert result.missing_count == 4
    assert repository.replacement_started_at is not None
    assert runtime.task is None
    assert runtime.snapshot().started_at is None
    assert state.active_operation_name == "remote_hf_import"
    assert state.active_operation_started_at == repository.replacement_started_at
    assert state.tracked_tasks == set()

    reset_state_for_tests()

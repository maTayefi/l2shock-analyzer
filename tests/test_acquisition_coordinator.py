from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from l2shock.acquisition import (
    AcquisitionCancelledError,
    DownloadArtifact,
    DownloadDisposition,
    FetchItemDisposition,
    FetchOperationBusyError,
    FetchProgress,
    FetchProgressPhase,
    FetchRunKind,
    FetchRunStatus,
    ManualFetchCoordinator,
    ParquetValidationReport,
    PersistedFetchRun,
    RemoteFileNotFoundError,
    RemoteRequestError,
    SourceFileSpec,
)


def _utc(
    hour: int,
    minute: int = 0,
) -> datetime:
    return datetime(
        2026,
        9,
        2,
        hour,
        minute,
        tzinfo=timezone.utc,
    )


def _artifact(
    tmp_path: Path,
    spec: SourceFileSpec,
    *,
    disposition: DownloadDisposition,
) -> DownloadArtifact:
    attempts = 0 if disposition is DownloadDisposition.REUSED else 1

    return DownloadArtifact(
        spec=spec,
        local_path=tmp_path / spec.filename,
        disposition=disposition,
        file_size_bytes=4096,
        content_sha256="a" * 64,
        validation=ParquetValidationReport(
            data_kind=spec.data_kind.value,
            row_count=123,
            row_group_count=1,
            column_names=(
                "received_time",
                "event_time",
                "symbol",
            ),
            created_by="coordinator-test",
            format_version="2.6",
        ),
        attempts=attempts,
    )


class FakePersistence:
    def __init__(self) -> None:
        self.created_runs: list[dict[str, Any]] = []
        self.downloading: list[SourceFileSpec] = []
        self.artifacts: list[DownloadArtifact] = []
        self.missing: list[tuple[SourceFileSpec, str]] = []
        self.errors: list[tuple[SourceFileSpec, str]] = []
        self.completions: list[dict[str, Any]] = []

    async def create_fetch_run(
        self,
        *,
        operation_id: UUID,
        kind: FetchRunKind,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        files_requested: int,
        details: dict[str, Any],
    ) -> PersistedFetchRun:
        self.created_runs.append(
            {
                "operation_id": operation_id,
                "kind": kind,
                "requested_start_utc": requested_start_utc,
                "requested_end_utc": requested_end_utc,
                "files_requested": files_requested,
                "details": details,
            }
        )

        return PersistedFetchRun(
            id=1,
            operation_id=operation_id,
        )

    async def mark_downloading(
        self,
        spec: SourceFileSpec,
    ) -> None:
        self.downloading.append(spec)

    async def record_artifact(
        self,
        artifact: DownloadArtifact,
    ) -> None:
        self.artifacts.append(artifact)

    async def record_missing(
        self,
        spec: SourceFileSpec,
        *,
        message: object,
    ) -> None:
        self.missing.append((spec, str(message)))

    async def record_error(
        self,
        spec: SourceFileSpec,
        *,
        message: object,
    ) -> None:
        self.errors.append((spec, str(message)))

    async def complete_fetch_run(
        self,
        *,
        operation_id: UUID,
        status: FetchRunStatus,
        files_downloaded: int,
        files_processed: int,
        files_failed: int,
        details: dict[str, Any],
        error_text: object = None,
    ) -> None:
        self.completions.append(
            {
                "operation_id": operation_id,
                "status": status,
                "files_downloaded": files_downloaded,
                "files_processed": files_processed,
                "files_failed": files_failed,
                "details": details,
                "error_text": error_text,
            }
        )


class FakeDownloader:
    def __init__(
        self,
        handler,
    ) -> None:
        self._handler = handler
        self.calls: list[SourceFileSpec] = []

    async def download(
        self,
        spec: SourceFileSpec,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> DownloadArtifact:
        self.calls.append(spec)
        return await self._handler(spec, cancel_event)


def _factory(downloader: FakeDownloader):
    @asynccontextmanager
    async def factory():
        yield downloader

    return factory


@pytest.mark.asyncio
async def test_manual_fetch_downloads_global_four_file_plan(
    tmp_path: Path,
) -> None:
    persistence = FakePersistence()
    progress: list[FetchProgress] = []

    async def handler(
        spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        disposition = (
            DownloadDisposition.DOWNLOADED
            if spec.symbol == "BTCUSDT"
            else DownloadDisposition.REUSED
        )
        return _artifact(
            tmp_path,
            spec,
            disposition=disposition,
        )

    downloader = FakeDownloader(handler)

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(downloader),
        progress_sink=progress.append,
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert result.status == "ok"
    assert result.files_requested == 4
    assert result.files_downloaded == 2
    assert result.files_reused == 2
    assert result.files_available == 4
    assert result.files_missing == 0
    assert result.files_failed == 0
    assert result.stopped is False

    assert [(spec.symbol, spec.data_kind.value) for spec in downloader.calls] == [
        ("BTCUSDT", "orderbook"),
        ("BTCUSDT", "trades"),
        ("ETHUSDT", "orderbook"),
        ("ETHUSDT", "trades"),
    ]

    assert len(persistence.created_runs) == 1
    assert persistence.created_runs[0]["kind"] is FetchRunKind.MANUAL
    assert persistence.created_runs[0]["files_requested"] == 4

    assert len(persistence.downloading) == 4
    assert len(persistence.artifacts) == 4
    assert persistence.missing == []
    assert persistence.errors == []

    completion = persistence.completions[0]
    assert completion["status"] is FetchRunStatus.OK

    # Existing DB field means successfully available local files here.
    assert completion["files_downloaded"] == 4
    assert completion["files_processed"] == 0
    assert completion["files_failed"] == 0
    assert completion["details"]["files_downloaded"] == 2
    assert completion["details"]["files_reused"] == 2

    phases = [event.phase for event in progress]
    assert phases[0] is FetchProgressPhase.PLANNING
    assert FetchProgressPhase.STARTED in phases
    assert phases.count(FetchProgressPhase.DOWNLOADING) == 4
    assert phases.count(FetchProgressPhase.FILE_COMPLETE) == 4
    assert phases[-1] is FetchProgressPhase.COMPLETED

    assert progress[-1].files_completed == 4
    assert progress[-1].percent_complete == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_missing_and_failed_files_produce_partial_result(
    tmp_path: Path,
) -> None:
    persistence = FakePersistence()

    async def handler(
        spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        identity = (spec.symbol, spec.data_kind.value)

        if identity == ("BTCUSDT", "trades"):
            raise RemoteFileNotFoundError("Remote hourly archive is unavailable")

        if identity == ("ETHUSDT", "orderbook"):
            raise RemoteRequestError("Remote request failed after retries")

        return _artifact(
            tmp_path,
            spec,
            disposition=DownloadDisposition.DOWNLOADED,
        )

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert result.status == "partial_ok"
    assert result.files_downloaded == 2
    assert result.files_reused == 0
    assert result.files_missing == 1
    assert result.files_failed == 1

    dispositions = [item.disposition for item in result.items]

    assert dispositions == [
        FetchItemDisposition.DOWNLOADED,
        FetchItemDisposition.MISSING,
        FetchItemDisposition.ERROR,
        FetchItemDisposition.DOWNLOADED,
    ]

    assert len(persistence.missing) == 1
    assert len(persistence.errors) == 1

    completion = persistence.completions[0]
    assert completion["status"] is FetchRunStatus.PARTIAL_OK
    assert completion["files_downloaded"] == 2
    assert completion["files_failed"] == 2


@pytest.mark.asyncio
async def test_all_unsuccessful_files_produce_error_status() -> None:
    persistence = FakePersistence()

    async def handler(
        spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        raise RemoteFileNotFoundError(f"Unavailable {spec.filename}")

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert result.status == "error"
    assert result.files_available == 0
    assert result.files_missing == 4
    assert result.files_failed == 0

    completion = persistence.completions[0]
    assert completion["status"] is FetchRunStatus.ERROR
    assert completion["files_downloaded"] == 0
    assert completion["files_failed"] == 4


@pytest.mark.asyncio
async def test_request_stop_is_cooperative_and_persists_stopped(
    tmp_path: Path,
) -> None:
    persistence = FakePersistence()
    entered_download = asyncio.Event()
    release_download = asyncio.Event()

    async def handler(
        spec: SourceFileSpec,
        cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        assert cancel_event is not None
        entered_download.set()

        while not release_download.is_set():
            if cancel_event.is_set():
                raise AcquisitionCancelledError("Acquisition operation was cancelled")
            await asyncio.sleep(0)

        return _artifact(
            tmp_path,
            spec,
            disposition=DownloadDisposition.DOWNLOADED,
        )

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    task = asyncio.create_task(
        coordinator.run(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )
    )

    await entered_download.wait()

    assert coordinator.is_running is True
    assert coordinator.active_operation_id is not None
    assert coordinator.request_stop() is True
    assert coordinator.request_stop() is False

    result = await task

    assert result.status == "stopped"
    assert result.stopped is True
    assert result.files_completed == 0
    assert result.files_requested == 4

    assert persistence.completions[0]["status"] is FetchRunStatus.STOPPED
    assert persistence.completions[0]["files_downloaded"] == 0
    assert persistence.completions[0]["files_failed"] == 0
    assert coordinator.is_running is False


@pytest.mark.asyncio
async def test_native_task_cancellation_finalizes_fetch_run(
    tmp_path: Path,
) -> None:
    persistence = FakePersistence()
    entered_download = asyncio.Event()

    async def handler(
        _spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        entered_download.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    task = asyncio.create_task(
        coordinator.run(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )
    )

    await entered_download.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(persistence.completions) == 1
    assert persistence.completions[0]["status"] is FetchRunStatus.STOPPED
    assert coordinator.is_running is False


@pytest.mark.asyncio
async def test_process_operation_lock_rejects_concurrent_operation(
    tmp_path: Path,
) -> None:
    persistence = FakePersistence()
    operation_lock = asyncio.Lock()
    entered_download = asyncio.Event()
    finish_download = asyncio.Event()

    async def handler(
        spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        entered_download.set()
        await finish_download.wait()

        return _artifact(
            tmp_path,
            spec,
            disposition=DownloadDisposition.DOWNLOADED,
        )

    coordinator = ManualFetchCoordinator(
        operation_lock=operation_lock,
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    first_task = asyncio.create_task(
        coordinator.run(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )
    )

    await entered_download.wait()

    with pytest.raises(
        FetchOperationBusyError,
        match="already active",
    ):
        await coordinator.run(
            requested_start_utc=_utc(13),
            requested_end_utc=_utc(14),
        )

    finish_download.set()
    await first_task

    assert operation_lock.locked() is False


@pytest.mark.asyncio
async def test_progress_sink_failure_does_not_abort_fetch(
    tmp_path: Path,
) -> None:
    persistence = FakePersistence()
    call_count = 0

    def failing_sink(_event: FetchProgress) -> None:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("UI client disconnected")

    async def handler(
        spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        return _artifact(
            tmp_path,
            spec,
            disposition=DownloadDisposition.DOWNLOADED,
        )

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
        progress_sink=failing_sink,
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert call_count > 0
    assert result.status == "ok"
    assert len(persistence.completions) == 1


@pytest.mark.asyncio
async def test_unexpected_exception_text_is_not_persisted() -> None:
    persistence = FakePersistence()
    secret = "-".join(
        (
            "unexpected",
            "private",
            "credential",
            "value",
        )
    )

    async def handler(
        _spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        raise RuntimeError(f"request failed with api_key={secret}")

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert result.status == "error"
    assert len(persistence.errors) == 4

    for _spec, diagnostic in persistence.errors:
        assert secret not in diagnostic
        assert diagnostic == "Unexpected RuntimeError"


@pytest.mark.asyncio
async def test_invalid_range_fails_before_database_or_downloader() -> None:
    persistence = FakePersistence()
    downloader_called = False

    async def handler(
        _spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        nonlocal downloader_called
        downloader_called = True
        raise AssertionError("must not be called")

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    with pytest.raises(
        ValueError,
        match="must be after",
    ):
        await coordinator.run(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(12),
        )

    assert persistence.created_runs == []
    assert downloader_called is False


@pytest.mark.asyncio
async def test_cooperative_cancellation_closes_downloading_source_state(
    tmp_path: Path,
) -> None:
    persistence = FakePersistence()
    entered_download = asyncio.Event()

    async def handler(
        _spec: SourceFileSpec,
        cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        assert cancel_event is not None
        entered_download.set()

        while not cancel_event.is_set():
            await asyncio.sleep(0)

        raise AcquisitionCancelledError("Acquisition operation was cancelled")

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(handler)),
    )

    task = asyncio.create_task(
        coordinator.run(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )
    )

    await entered_download.wait()
    assert coordinator.request_stop() is True

    result = await task

    assert result.status == "stopped"
    assert len(persistence.errors) == 1
    assert persistence.errors[0][1] == (
        "Fetch operation stopped before source completion"
    )


@pytest.mark.asyncio
async def test_downloader_context_failure_finalizes_fetch_run() -> None:
    persistence = FakePersistence()

    @asynccontextmanager
    async def failing_factory():
        raise RuntimeError("credential-bearing implementation detail")
        yield  # pragma: no cover

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=failing_factory,
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert result.status == "error"
    assert result.files_completed == 0
    assert result.details["fatal_error"] == ("Unexpected RuntimeError")

    assert len(persistence.completions) == 1
    completion = persistence.completions[0]

    assert completion["status"] is FetchRunStatus.ERROR
    assert completion["error_text"] == "Unexpected RuntimeError"
    assert "credential-bearing implementation detail" not in str(completion)


@pytest.mark.asyncio
async def test_production_source_planner_can_fetch_eight_file_universe(
    tmp_path: Path,
) -> None:
    from l2shock.acquisition import plan_production_source_files

    persistence = FakePersistence()

    async def handler(
        spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        return _artifact(
            tmp_path,
            spec,
            disposition=DownloadDisposition.DOWNLOADED,
        )

    downloader = FakeDownloader(handler)

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(downloader),
        source_planner=plan_production_source_files,
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert result.files_requested == 8
    assert result.files_downloaded == 8
    assert result.files_failed == 0

    assert {
        (
            spec.venue,
            spec.symbol,
            spec.data_kind.value,
        )
        for spec in downloader.calls
    } == {
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
            "okx_futures",
            "BTC-USDT-SWAP",
            "orderbook",
        ),
        (
            "okx_futures",
            "ETH-USDT-SWAP",
            "orderbook",
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
    }


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_interrupted_source_cleanup() -> None:
    from datetime import datetime, timezone

    from l2shock.acquisition import SourceDataKind, SourceFileSpec
    from l2shock.acquisition.coordinator import ManualFetchCoordinator

    class BlockingPersistence:
        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.completed = False

        async def record_error(
            self,
            spec: SourceFileSpec,
            *,
            message: object,
        ) -> None:
            assert spec.symbol == "BTCUSDT"
            assert "stopped" in str(message).lower()

            self.entered.set()
            await self.release.wait()
            self.completed = True

    persistence = BlockingPersistence()
    coordinator = object.__new__(ManualFetchCoordinator)
    coordinator._persistence = persistence

    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=datetime(
            2026,
            9,
            14,
            12,
            tzinfo=timezone.utc,
        ),
    )

    task = asyncio.create_task(coordinator._record_interrupted_source(spec))

    await asyncio.wait_for(
        persistence.entered.wait(),
        timeout=1.0,
    )

    task.cancel()
    await asyncio.sleep(0)

    task.cancel()
    await asyncio.sleep(0)

    assert not persistence.completed
    assert not task.done()

    persistence.release.set()
    await asyncio.wait_for(
        task,
        timeout=1.0,
    )

    assert persistence.completed


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_fetch_run_completion(
    tmp_path: Path,
) -> None:
    class BlockingCompletionPersistence(FakePersistence):
        def __init__(self) -> None:
            super().__init__()
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def complete_fetch_run(self, **kwargs) -> None:
            self.entered.set()
            await self.release.wait()
            await super().complete_fetch_run(**kwargs)

    persistence = BlockingCompletionPersistence()
    operation_lock = asyncio.Lock()

    async def download_immediately(
        spec,
        _cancel_event,
    ):
        return _artifact(
            tmp_path,
            spec,
            disposition=DownloadDisposition.DOWNLOADED,
        )

    coordinator = ManualFetchCoordinator(
        operation_lock=operation_lock,
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(download_immediately)),
    )

    task = asyncio.create_task(
        coordinator.run(
            requested_start_utc=_utc(12),
            requested_end_utc=_utc(13),
        )
    )

    await asyncio.wait_for(persistence.entered.wait(), timeout=2.0)

    try:
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert not task.done()
        assert operation_lock.locked()
        assert persistence.completions == []
    finally:
        persistence.release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert len(persistence.completions) == 1
    assert operation_lock.locked() is False


@pytest.mark.asyncio
async def test_processed_repeat_fetch_reports_reuse_without_rewriting_source(
    tmp_path: Path,
) -> None:
    class ProcessedPersistence(FakePersistence):
        async def mark_downloading(
            self,
            spec: SourceFileSpec,
        ) -> bool:
            self.downloading.append(spec)
            return True

        async def record_missing(
            self,
            spec: SourceFileSpec,
            *,
            message: object,
        ) -> None:
            raise AssertionError("Processed source must not become missing")

        async def record_error(
            self,
            spec: SourceFileSpec,
            *,
            message: object,
        ) -> None:
            raise AssertionError("Processed source must not become error")

    persistence = ProcessedPersistence()

    async def reuse(
        spec: SourceFileSpec,
        _cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        return _artifact(
            tmp_path,
            spec,
            disposition=DownloadDisposition.REUSED,
        )

    coordinator = ManualFetchCoordinator(
        operation_lock=asyncio.Lock(),
        persistence=persistence,
        downloader_factory=_factory(FakeDownloader(reuse)),
    )

    result = await coordinator.run(
        requested_start_utc=_utc(12),
        requested_end_utc=_utc(13),
    )

    assert result.status == "ok"
    assert result.files_reused == 4
    assert len(persistence.artifacts) == 4
    assert persistence.missing == []
    assert persistence.errors == []
    assert persistence.completions[0]["status"] is FetchRunStatus.OK

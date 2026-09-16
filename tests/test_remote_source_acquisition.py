# tests/test_remote_source_acquisition.py
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from l2shock.acquisition import (
    DownloadDisposition,
    RemoteFileNotFoundError,
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.config import CryptoHFTConfig
from l2shock.processing import verify_processing_source_archive
from l2shock.remote import (
    RemoteWorkerAcquisitionError,
    RemoteWorkerSourceNotEligibleError,
    acquire_remote_worker_archives,
    build_remote_worker_workspace,
    temporary_remote_worker_workspace,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        14,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _spec(
    data_kind: SourceDataKind = SourceDataKind.ORDERBOOK,
    *,
    offset: int = 0,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=data_kind,
        hour_utc=_hour(offset),
    )


def _cryptohft() -> CryptoHFTConfig:
    return CryptoHFTConfig(
        base_url="https://api.cryptohftdata.com/v1",
        request_timeout_seconds=5,
        download_rate_limit_per_minute=60,
        retry_max_attempts=1,
        retry_initial_backoff_seconds=1,
        expected_release_delay_minutes=15,
        release_poll_interval_minutes=5,
        automatic_fetch_catch_up_hours=72,
    )


def _orderbook_bytes(tmp_path: Path) -> bytes:
    path = tmp_path / "source-orderbook.parquet"

    table = pa.Table.from_pylist(
        [
            {
                "received_time": 1_789_372_800_100_000_000,
                "event_time": 1_789_372_800_100,
                "transaction_time": 1_789_372_800_100,
                "symbol": "BTCUSDT",
                "event_type": "snapshot",
                "first_update_id": None,
                "final_update_id": None,
                "prev_final_update_id": None,
                "last_update_id": 100,
                "side": "bid",
                "price": "100",
                "quantity": "2",
                "order_count": None,
            },
            {
                "received_time": 1_789_372_800_100_000_000,
                "event_time": 1_789_372_800_100,
                "transaction_time": 1_789_372_800_100,
                "symbol": "BTCUSDT",
                "event_type": "snapshot",
                "first_update_id": None,
                "final_update_id": None,
                "prev_final_update_id": None,
                "last_update_id": 100,
                "side": "ask",
                "price": "101",
                "quantity": "3",
                "order_count": None,
            },
        ]
    )

    pq.write_table(
        table,
        path,
        compression="zstd",
    )

    return path.read_bytes()


@pytest.mark.asyncio
async def test_remote_worker_acquisition_downloads_validated_processing_archive(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    workspace = build_remote_worker_workspace(
        tmp_path / "worker",
    )
    observed_queries: list[dict[str, list[str]]] = []

    async def handler(
        request: httpx.Request,
    ) -> httpx.Response:
        observed_queries.append(parse_qs(request.url.query.decode("ascii")))

        return httpx.Response(
            200,
            headers={
                "Content-Length": str(len(content)),
            },
            content=content,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await acquire_remote_worker_archives(
            (_spec(),),
            cryptohft=_cryptohft(),
            workspace=workspace,
            latest_eligible_hour_utc=_hour(),
            client=client,
        )

    assert result.source_count == 1
    assert result.downloaded_count == 1
    assert result.reused_count == 0

    artifact = result.download_artifacts[0]
    archive = result.processing_archives[0]

    assert artifact.disposition is DownloadDisposition.DOWNLOADED
    assert artifact.spec == _spec()
    assert archive.spec == _spec()
    assert archive.local_path == artifact.local_path
    assert archive.content_sha256 == artifact.content_sha256
    assert archive.file_size_bytes == artifact.file_size_bytes

    assert archive.local_path.is_file()
    assert archive.local_path.read_bytes() == content

    verify_processing_source_archive(archive)

    assert observed_queries == [
        {"file": ["binance_futures/2026-09-14/12/" "BTCUSDT_orderbook.parquet"]}
    ]


@pytest.mark.asyncio
async def test_remote_worker_acquisition_reuses_existing_valid_archive(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    workspace = build_remote_worker_workspace(
        tmp_path / "worker",
    )
    spec = _spec()
    destination = spec.local_path(workspace.storage.raw_path)
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    destination.write_bytes(content)

    request_count = 0

    async def handler(
        _request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        return httpx.Response(
            500,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await acquire_remote_worker_archives(
            (spec,),
            cryptohft=_cryptohft(),
            workspace=workspace,
            latest_eligible_hour_utc=_hour(),
            client=client,
        )

    assert request_count == 0
    assert result.source_count == 1
    assert result.downloaded_count == 0
    assert result.reused_count == 1

    artifact = result.download_artifacts[0]

    assert artifact.disposition is DownloadDisposition.REUSED
    assert artifact.attempts == 0


@pytest.mark.asyncio
async def test_remote_worker_acquisition_rejects_duplicate_sources_before_http(
    tmp_path: Path,
) -> None:
    workspace = build_remote_worker_workspace(
        tmp_path / "worker",
    )
    request_count = 0

    async def handler(
        _request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        return httpx.Response(
            500,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(
            RemoteWorkerAcquisitionError,
            match="duplicate source identity",
        ):
            await acquire_remote_worker_archives(
                (
                    _spec(),
                    _spec(),
                ),
                cryptohft=_cryptohft(),
                workspace=workspace,
                latest_eligible_hour_utc=_hour(),
                client=client,
            )

    assert request_count == 0


@pytest.mark.asyncio
async def test_remote_worker_acquisition_rejects_unreleased_hour_before_http(
    tmp_path: Path,
) -> None:
    workspace = build_remote_worker_workspace(
        tmp_path / "worker",
    )
    request_count = 0

    async def handler(
        _request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        return httpx.Response(
            200,
            content=b"must-not-be-used",
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(
            RemoteWorkerSourceNotEligibleError,
            match="newer than the admitted release boundary",
        ):
            await acquire_remote_worker_archives(
                (
                    _spec(
                        offset=1,
                    ),
                ),
                cryptohft=_cryptohft(),
                workspace=workspace,
                latest_eligible_hour_utc=_hour(),
                client=client,
            )

    assert request_count == 0


@pytest.mark.asyncio
async def test_remote_worker_missing_source_preserves_downloader_exception(
    tmp_path: Path,
) -> None:
    workspace = build_remote_worker_workspace(
        tmp_path / "worker",
    )

    async def handler(
        _request: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            404,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(
            RemoteFileNotFoundError,
            match="not currently available",
        ):
            await acquire_remote_worker_archives(
                (_spec(),),
                cryptohft=_cryptohft(),
                workspace=workspace,
                latest_eligible_hour_utc=_hour(),
                client=client,
            )

    assert not _spec().local_path(workspace.storage.raw_path).exists()


@pytest.mark.asyncio
async def test_remote_worker_acquisition_honors_preexisting_cancellation(
    tmp_path: Path,
) -> None:
    from l2shock.acquisition import AcquisitionCancelledError

    workspace = build_remote_worker_workspace(
        tmp_path / "worker",
    )
    cancel_event = asyncio.Event()
    cancel_event.set()
    request_count = 0

    async def handler(
        _request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        return httpx.Response(
            200,
            content=b"must-not-be-used",
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(
            AcquisitionCancelledError,
            match="cancelled",
        ):
            await acquire_remote_worker_archives(
                (_spec(),),
                cryptohft=_cryptohft(),
                workspace=workspace,
                latest_eligible_hour_utc=_hour(),
                cancel_event=cancel_event,
                client=client,
            )

    assert request_count == 0


def test_temporary_remote_worker_workspace_is_removed(
    tmp_path: Path,
) -> None:
    with temporary_remote_worker_workspace(
        parent=tmp_path,
    ) as workspace:
        root = workspace.root

        assert root.is_dir()
        assert workspace.storage.raw_path.is_dir()
        assert workspace.storage.cache_path.is_dir()
        assert workspace.storage.quarantine_path.is_dir()
        assert workspace.storage.export_path.is_dir()
        assert workspace.storage.log_path.is_dir()
        assert workspace.storage.backup_path.is_dir()

    assert not root.exists()


def test_remote_source_acquisition_has_no_database_or_ui_dependency() -> None:
    import ast
    import l2shock.remote.source_acquisition as module

    path = Path(module.__file__)
    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
        ),
        filename=str(path),
    )

    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)

        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    assert not any(
        name == "l2shock.db" or name.startswith("l2shock.db.")
        for name in imported_modules
    )
    assert not any(
        name == "l2shock.ui" or name.startswith("l2shock.ui.")
        for name in imported_modules
    )
    assert "nicegui" not in imported_modules

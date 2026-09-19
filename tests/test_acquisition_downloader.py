from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import SecretStr

from l2shock.acquisition import (
    AcquisitionCancelledError,
    CryptoHFTDownloader,
    DownloadConflictError,
    DownloadDisposition,
    InsufficientDiskSpaceError,
    ParquetValidationError,
    ParquetValidationReport,
    RemoteFileNotFoundError,
    SourceFileSpec,
)
from l2shock.config import CryptoHFTConfig, StorageConfig


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="orderbook",
        hour_utc=datetime(
            2026,
            9,
            2,
            12,
            tzinfo=timezone.utc,
        ),
    )


def _cryptohft(
    *,
    attempts: int = 3,
    api_key: str = "",
) -> CryptoHFTConfig:
    return CryptoHFTConfig(
        base_url="https://api.cryptohftdata.com/v1",
        api_key=SecretStr(api_key),
        request_timeout_seconds=5,
        download_rate_limit_per_minute=60,
        retry_max_attempts=attempts,
        retry_initial_backoff_seconds=1,
        expected_release_delay_minutes=15,
        release_poll_interval_minutes=5,
    )


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        raw_dir=str(tmp_path / "raw"),
        cache_dir=str(tmp_path / "cache"),
        quarantine_dir=str(tmp_path / "quarantine"),
        export_dir=str(tmp_path / "exports"),
        log_dir=str(tmp_path / "logs"),
        backup_dir=str(tmp_path / "backups"),
        raw_retention_hours=72,
        minimum_free_disk_gib=0.0,
    )


def _orderbook_bytes(tmp_path: Path) -> bytes:
    path = tmp_path / "source.parquet"

    table = pa.table(
        {
            "received_time": pa.array(
                [1788350400007361359],
                type=pa.int64(),
            ),
            "event_time": pa.array(
                [1788350399886],
                type=pa.int64(),
            ),
            "transaction_time": pa.array(
                [1788350399885],
                type=pa.int64(),
            ),
            "symbol": ["BTCUSDT"],
            "event_type": ["update"],
            "first_update_id": pa.array([100], type=pa.int64()),
            "final_update_id": pa.array([110], type=pa.int64()),
            "prev_final_update_id": pa.array([99], type=pa.int64()),
            "last_update_id": pa.array([None], type=pa.int64()),
            "side": ["bid"],
            "price": ["75000.00"],
            "quantity": ["1.250"],
            "order_count": pa.array([None], type=pa.int64()),
        }
    )

    pq.write_table(table, path, compression="zstd")
    return path.read_bytes()


@pytest.mark.asyncio
async def test_anonymous_download_is_validated_and_published(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    observed_queries: list[dict[str, list[str]]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        observed_queries.append(parse_qs(request.url.query.decode("ascii")))
        return httpx.Response(
            200,
            headers={"Content-Length": str(len(content))},
            content=content,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
        follow_redirects=True,
    ) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(),
            storage=_storage(tmp_path),
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            artifact = await downloader.download(_spec())

    assert artifact.disposition is DownloadDisposition.DOWNLOADED
    assert artifact.attempts == 1
    assert artifact.local_path.exists()
    assert artifact.local_path.read_bytes() == content
    assert artifact.validation.row_count == 1

    assert observed_queries == [
        {"file": ["binance_futures/2026-09-02/12/" "BTCUSDT_orderbook.parquet"]}
    ]

    assert not list(artifact.local_path.parent.glob("*.part"))
    assert not list(artifact.local_path.parent.glob(".*.part"))


@pytest.mark.asyncio
async def test_valid_existing_file_is_reused_without_http(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    spec = _spec()
    storage = _storage(tmp_path)
    destination = spec.local_path(storage.raw_path)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(content)

    request_count = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(),
            storage=storage,
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            artifact = await downloader.download(spec)

    assert artifact.disposition is DownloadDisposition.REUSED
    assert artifact.attempts == 0
    assert request_count == 0


@pytest.mark.asyncio
async def test_retryable_status_retries_then_succeeds(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    attempts = 0
    sleeps: list[float] = []

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            return httpx.Response(
                503,
                headers={"Retry-After": "2"},
            )

        return httpx.Response(200, content=content)

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(attempts=3),
            storage=_storage(tmp_path),
            client=client,
            sleeper=fake_sleep,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            artifact = await downloader.download(_spec())

    assert attempts == 2
    assert sleeps == [pytest.approx(2.0)]
    assert artifact.attempts == 2


@pytest.mark.asyncio
async def test_404_is_reported_without_retry(
    tmp_path: Path,
) -> None:
    attempts = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(attempts=3),
            storage=_storage(tmp_path),
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            with pytest.raises(RemoteFileNotFoundError):
                await downloader.download(_spec())

    assert attempts == 1


@pytest.mark.asyncio
async def test_invalid_download_is_quarantined(
    tmp_path: Path,
) -> None:
    invalid_content = b"<html>not parquet</html>"

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=invalid_content)

    storage = _storage(tmp_path)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(attempts=1),
            storage=storage,
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            with pytest.raises(ParquetValidationError):
                await downloader.download(_spec())

    assert not _spec().local_path(storage.raw_path).exists()

    quarantined = list(storage.quarantine_path.rglob("*.parquet"))
    sidecars = list(storage.quarantine_path.rglob("*.quarantine.json"))

    assert len(quarantined) == 1
    assert len(sidecars) == 1
    assert quarantined[0].read_bytes() == invalid_content


@pytest.mark.asyncio
async def test_invalid_existing_file_is_quarantined_then_replaced(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    spec = _spec()
    storage = _storage(tmp_path)
    destination = spec.local_path(storage.raw_path)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"invalid existing content")

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(),
            storage=storage,
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            artifact = await downloader.download(spec)

    assert artifact.disposition is DownloadDisposition.DOWNLOADED
    assert destination.read_bytes() == content
    assert list(storage.quarantine_path.rglob("*.parquet"))


@pytest.mark.asyncio
async def test_disk_floor_failure_occurs_before_http(
    tmp_path: Path,
) -> None:
    request_count = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, content=b"unused")

    storage = _storage(tmp_path)
    storage.minimum_free_disk_gib = 1.0

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(),
            storage=storage,
            client=client,
            free_bytes_provider=lambda _path: 100,
        ) as downloader:
            with pytest.raises(InsufficientDiskSpaceError):
                await downloader.download(_spec())

    assert request_count == 0


@pytest.mark.asyncio
async def test_pre_set_cooperative_cancellation_avoids_http(
    tmp_path: Path,
) -> None:
    request_count = 0
    cancel_event = asyncio.Event()
    cancel_event.set()

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, content=b"unused")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(),
            storage=_storage(tmp_path),
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            with pytest.raises(AcquisitionCancelledError):
                await downloader.download(
                    _spec(),
                    cancel_event=cancel_event,
                )

    assert request_count == 0


@pytest.mark.asyncio
async def test_direct_api_key_is_used_only_when_explicit(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    observed_query: dict[str, list[str]] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal observed_query
        observed_query = parse_qs(request.url.query.decode("ascii"))
        return httpx.Response(200, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(api_key="example-private-key"),
            storage=_storage(tmp_path),
            client=client,
            use_api_key=True,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            await downloader.download(_spec())

    assert observed_query["api_key"] == ["example-private-key"]


def test_published_archive_survives_temporary_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _spec()
    storage = _storage(tmp_path)
    destination = spec.local_path(storage.raw_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary = destination.with_name(f".{destination.name}.cleanup-test.part")
    temporary.write_bytes(b"validated-source-content")

    downloader = CryptoHFTDownloader(
        cryptohft=_cryptohft(),
        storage=storage,
        free_bytes_provider=lambda _path: 10 * 1024**3,
    )

    original_unlink = Path.unlink
    failed_once = False

    def fail_first_temporary_unlink(
        path: Path,
        *args,
        **kwargs,
    ):
        nonlocal failed_once

        if path == temporary and not failed_once:
            failed_once = True
            raise OSError("simulated temporary cleanup failure")

        return original_unlink(
            path,
            *args,
            **kwargs,
        )

    monkeypatch.setattr(
        Path,
        "unlink",
        fail_first_temporary_unlink,
    )

    artifact = downloader._publish_validated_file(
        spec,
        destination=destination,
        temporary=temporary,
        validation=ParquetValidationReport(
            data_kind="orderbook",
            row_count=1,
            row_group_count=1,
            column_names=(
                "received_time",
                "event_time",
                "symbol",
            ),
            created_by="test",
            format_version="2.6",
        ),
        file_size=temporary.stat().st_size,
        content_sha256="a" * 64,
        attempt=1,
    )

    assert failed_once is True
    assert artifact.local_path == destination
    assert destination.is_file()
    assert destination.read_bytes() == b"validated-source-content"

    original_unlink(
        temporary,
        missing_ok=True,
    )


@pytest.mark.asyncio
async def test_existing_symlink_destination_is_rejected_before_reuse(
    tmp_path: Path,
) -> None:
    content = _orderbook_bytes(tmp_path)
    spec = _spec()
    storage = _storage(tmp_path)
    destination = spec.local_path(storage.raw_path)
    target = tmp_path / "external-source.parquet"

    destination.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)

    try:
        destination.symlink_to(target)
    except OSError:
        pytest.skip("File symlinks are unavailable on this platform")

    request_count = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(),
            storage=storage,
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            with pytest.raises(
                DownloadConflictError,
                match="symbolic link",
            ):
                await downloader.download(spec)

    assert request_count == 0
    assert destination.is_symlink()
    assert target.read_bytes() == content


@pytest.mark.asyncio
async def test_intermediate_symlink_destination_is_rejected_before_http(
    tmp_path: Path,
) -> None:
    spec = _spec()
    storage = _storage(tmp_path)
    destination = spec.local_path(storage.raw_path)
    redirected_parent = destination.parent
    external_parent = tmp_path / "external-raw-parent"

    redirected_parent.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    external_parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        redirected_parent.symlink_to(
            external_parent,
            target_is_directory=True,
        )
    except OSError:
        pytest.skip("Directory symbolic links are unavailable on this platform")

    request_count = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(
            500,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        async with CryptoHFTDownloader(
            cryptohft=_cryptohft(),
            storage=storage,
            client=client,
            free_bytes_provider=lambda _path: 10 * 1024**3,
        ) as downloader:
            with pytest.raises(
                DownloadConflictError,
                match="application-owned storage|symbolic link|junction",
            ):
                await downloader.download(spec)

    assert request_count == 0
    assert not (external_parent / destination.name).exists()

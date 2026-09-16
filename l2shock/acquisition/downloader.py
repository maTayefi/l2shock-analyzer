# l2shock/acquisition/downloader.py
"""Atomic asynchronous CryptoHFTData hourly-file downloader."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import shutil
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Final

import httpx

from l2shock.acquisition.errors import (
    AcquisitionCancelledError,
    DownloadConflictError,
    DownloadIntegrityError,
    InsufficientDiskSpaceError,
    ParquetValidationError,
    RemoteFileNotFoundError,
    RemoteRequestError,
)
from l2shock.acquisition.http import (
    download_endpoint,
    download_query_parameters,
    is_retryable_http_status,
    parse_retry_after_seconds,
)
from l2shock.acquisition.models import SourceFileSpec
from l2shock.acquisition.rate_limit import (
    AsyncRollingWindowRateLimiter,
)
from l2shock.acquisition.results import (
    DownloadArtifact,
    DownloadDisposition,
)
from l2shock.acquisition.validation import (
    quarantine_file,
    sha256_file,
    validate_parquet_file,
)
from l2shock.config import CryptoHFTConfig, StorageConfig

log = logging.getLogger(__name__)

Sleeper = Callable[[float], Awaitable[None]]
FreeBytesProvider = Callable[[Path], int]

_STREAM_CHUNK_SIZE: Final[int] = 1024 * 1024
_DISK_RECHECK_BYTES: Final[int] = 8 * 1024 * 1024
_MAX_BACKOFF_SECONDS: Final[float] = 300.0

_RETRYABLE_NETWORK_ERRORS: Final[tuple[type[BaseException], ...]] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.WriteError,
    httpx.WriteTimeout,
)


def _default_free_bytes(path: Path) -> int:
    return int(shutil.disk_usage(path).free)


def _part_path(destination: Path) -> Path:
    return destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")


def _content_length(response: httpx.Response) -> int | None:
    """Return enforceable response length when representation is unencoded."""
    content_encoding = str(response.headers.get("Content-Encoding", "")).strip().lower()

    if content_encoding not in {"", "identity"}:
        return None

    raw = str(response.headers.get("Content-Length", "")).strip()
    if not raw:
        return None

    try:
        result = int(raw)
    except ValueError:
        return None

    if result < 0:
        raise DownloadIntegrityError(
            "Remote response declared a negative Content-Length"
        )

    return result


class CryptoHFTDownloader:
    """Download and atomically publish validated hourly Parquet files.

    Anonymous REST is the default. Direct API-key mode is explicit and opt-in.

    The API key may temporarily exist in an in-memory request parameter
    mapping, but no exception or log emitted by this class includes the request
    URL, query string, headers, or key.
    """

    def __init__(
        self,
        *,
        cryptohft: CryptoHFTConfig,
        storage: StorageConfig,
        client: httpx.AsyncClient | None = None,
        rate_limiter: AsyncRollingWindowRateLimiter | None = None,
        sleeper: Sleeper = asyncio.sleep,
        free_bytes_provider: FreeBytesProvider = _default_free_bytes,
        use_api_key: bool = False,
    ) -> None:
        self._cryptohft = cryptohft
        self._storage = storage
        self._endpoint = download_endpoint(cryptohft.base_url)
        self._sleeper = sleeper
        self._free_bytes_provider = free_bytes_provider
        self._use_api_key = bool(use_api_key)

        self._rate_limiter = (
            rate_limiter
            if rate_limiter is not None
            else AsyncRollingWindowRateLimiter(
                cryptohft.download_rate_limit_per_minute,
            )
        )

        self._client = client
        self._owns_client = client is None
        self._closed = False

    async def __aenter__(self) -> CryptoHFTDownloader:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        _exception_type,
        _exception,
        _traceback,
    ) -> None:
        await self.aclose()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("CryptoHFTDownloader is closed")

    def _http_client(self) -> httpx.AsyncClient:
        self._ensure_open()

        if self._client is None:
            timeout = httpx.Timeout(float(self._cryptohft.request_timeout_seconds))
            self._client = httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                trust_env=True,
            )

        return self._client

    async def aclose(self) -> None:
        if self._closed:
            return

        self._closed = True

        if self._owns_client and self._client is not None:
            await self._client.aclose()

    def _minimum_free_bytes(self) -> int:
        return int(float(self._storage.minimum_free_disk_gib) * 1024**3)

    def _check_cancelled(
        self,
        cancel_event: asyncio.Event | None,
    ) -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise AcquisitionCancelledError("Acquisition was cancelled")

    def _check_disk_space(
        self,
        directory: Path,
        *,
        additional_required_bytes: int = 0,
    ) -> None:
        minimum = self._minimum_free_bytes()
        required = minimum + max(0, int(additional_required_bytes))
        free = int(self._free_bytes_provider(directory))

        if free < required:
            raise InsufficientDiskSpaceError(
                "Insufficient free disk space for acquisition: "
                f"free={free} bytes, required={required} bytes"
            )

    async def _await_or_cancel(
        self,
        awaitable: Awaitable[object],
        *,
        cancel_event: asyncio.Event | None,
    ) -> object:
        if cancel_event is None:
            return await awaitable

        self._check_cancelled(cancel_event)

        operation_task = asyncio.ensure_future(awaitable)
        cancellation_task = asyncio.create_task(cancel_event.wait())

        try:
            done, _pending = await asyncio.wait(
                {operation_task, cancellation_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if cancellation_task in done and cancel_event.is_set():
                operation_task.cancel()
                await asyncio.gather(
                    operation_task,
                    return_exceptions=True,
                )
                raise AcquisitionCancelledError("Acquisition was cancelled")

            cancellation_task.cancel()
            await asyncio.gather(
                cancellation_task,
                return_exceptions=True,
            )
            return await operation_task

        except BaseException:
            for task in (operation_task, cancellation_task):
                if not task.done():
                    task.cancel()

            await asyncio.gather(
                operation_task,
                cancellation_task,
                return_exceptions=True,
            )
            raise

    async def _acquire_rate_slot(
        self,
        cancel_event: asyncio.Event | None,
    ) -> None:
        await self._await_or_cancel(
            self._rate_limiter.acquire(),
            cancel_event=cancel_event,
        )

    async def _wait_before_retry(
        self,
        seconds: float,
        cancel_event: asyncio.Event | None,
    ) -> None:
        delay = max(0.0, min(float(seconds), _MAX_BACKOFF_SECONDS))
        if delay <= 0.0:
            self._check_cancelled(cancel_event)
            return

        await self._await_or_cancel(
            self._sleeper(delay),
            cancel_event=cancel_event,
        )

    def _backoff_seconds(self, attempt: int) -> float:
        initial = float(self._cryptohft.retry_initial_backoff_seconds)
        exponent = max(0, int(attempt) - 1)

        return min(
            initial * (2**exponent),
            _MAX_BACKOFF_SECONDS,
        )

    def _request_parameters(
        self,
        spec: SourceFileSpec,
    ) -> dict[str, str]:
        return download_query_parameters(
            spec,
            use_api_key=self._use_api_key,
            api_key=(self._cryptohft.api_key.get_secret_value()),
        )

    def _existing_artifact(
        self,
        spec: SourceFileSpec,
        destination: Path,
    ) -> DownloadArtifact | None:
        if not destination.exists():
            return None

        if not destination.is_file():
            raise DownloadConflictError(
                "Raw archive destination exists but is not a regular file: "
                f"{destination.name!r}"
            )

        try:
            validation = validate_parquet_file(destination, spec)
            digest, file_size = sha256_file(destination)
        except (ParquetValidationError, DownloadIntegrityError):
            quarantined = quarantine_file(
                destination,
                self._storage.quarantine_path,
                spec=spec,
                reason="invalid_existing_archive",
            )
            log.warning(
                "Moved invalid existing source archive to quarantine: %s",
                quarantined.name,
            )
            return None

        return DownloadArtifact(
            spec=spec,
            local_path=destination,
            disposition=DownloadDisposition.REUSED,
            file_size_bytes=file_size,
            content_sha256=digest,
            validation=validation,
            attempts=0,
        )

    async def download(
        self,
        spec: SourceFileSpec,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> DownloadArtifact:
        """Make one validated hourly source file available locally."""
        self._ensure_open()
        self._check_cancelled(cancel_event)

        destination = spec.local_path(self._storage.raw_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        existing = self._existing_artifact(spec, destination)
        if existing is not None:
            return existing

        self._check_disk_space(destination.parent)

        maximum_attempts = int(self._cryptohft.retry_max_attempts)
        last_remote_error: RemoteRequestError | None = None

        for attempt in range(1, maximum_attempts + 1):
            self._check_cancelled(cancel_event)
            await self._acquire_rate_slot(cancel_event)

            temporary = _part_path(destination)

            try:
                artifact = await self._download_attempt(
                    spec,
                    destination=destination,
                    temporary=temporary,
                    attempt=attempt,
                    cancel_event=cancel_event,
                )
                return artifact

            except RemoteFileNotFoundError:
                raise

            except AcquisitionCancelledError:
                raise

            except asyncio.CancelledError:
                raise

            except RemoteRequestError as exc:
                last_remote_error = exc

                if attempt >= maximum_attempts:
                    raise

                retry_delay = getattr(
                    exc,
                    "retry_delay_override",
                    None,
                )
                if retry_delay is None:
                    retry_delay = self._backoff_seconds(attempt)

                await self._wait_before_retry(
                    float(retry_delay),
                    cancel_event,
                )

            finally:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    log.warning(
                        "Could not remove temporary download file %s",
                        temporary.name,
                        exc_info=True,
                    )

        if last_remote_error is not None:
            raise last_remote_error

        raise RemoteRequestError("Remote acquisition exhausted its retry budget")

    async def _download_attempt(
        self,
        spec: SourceFileSpec,
        *,
        destination: Path,
        temporary: Path,
        attempt: int,
        cancel_event: asyncio.Event | None,
    ) -> DownloadArtifact:
        client = self._http_client()
        parameters = self._request_parameters(spec)

        try:
            async with client.stream(
                "GET",
                self._endpoint,
                params=parameters,
            ) as response:
                status = int(response.status_code)

                if status == 404:
                    raise RemoteFileNotFoundError(
                        "The requested hourly archive is not currently "
                        f"available: {spec.remote_path}"
                    )

                if is_retryable_http_status(status):
                    retry_after = parse_retry_after_seconds(
                        response.headers.get("Retry-After")
                    )

                    error = RemoteRequestError(
                        "CryptoHFTData returned a retryable HTTP status "
                        f"{status} for {spec.remote_path}"
                    )
                    setattr(error, "retry_after_seconds", retry_after)
                    raise error

                if status < 200 or status >= 300:
                    raise RemoteRequestError(
                        "CryptoHFTData returned HTTP status "
                        f"{status} for {spec.remote_path}"
                    )

                expected_length = _content_length(response)
                if expected_length == 0:
                    raise DownloadIntegrityError("Remote archive response was empty")

                if expected_length is not None:
                    self._check_disk_space(
                        destination.parent,
                        additional_required_bytes=expected_length,
                    )

                digest = hashlib.sha256()
                byte_count = 0
                bytes_at_last_disk_check = 0

                try:
                    with temporary.open("xb") as handle:
                        async for chunk in response.aiter_bytes(_STREAM_CHUNK_SIZE):
                            self._check_cancelled(cancel_event)

                            if not chunk:
                                continue

                            handle.write(chunk)
                            digest.update(chunk)
                            byte_count += len(chunk)

                            if (
                                byte_count - bytes_at_last_disk_check
                                >= _DISK_RECHECK_BYTES
                            ):
                                self._check_disk_space(destination.parent)
                                bytes_at_last_disk_check = byte_count

                        handle.flush()
                        os.fsync(handle.fileno())

                except FileExistsError as exc:
                    raise DownloadIntegrityError(
                        "A supposedly unique temporary download path " "already exists"
                    ) from exc
                except OSError as exc:
                    raise DownloadIntegrityError(
                        "Could not write the temporary hourly archive"
                    ) from exc

        except RemoteFileNotFoundError:
            raise
        except AcquisitionCancelledError:
            raise
        except DownloadIntegrityError:
            raise
        except RemoteRequestError as exc:
            retry_after = getattr(
                exc,
                "retry_after_seconds",
                None,
            )
            if retry_after is not None:
                setattr(exc, "retry_delay_override", retry_after)
            raise
        except asyncio.CancelledError:
            raise
        except _RETRYABLE_NETWORK_ERRORS as exc:
            raise RemoteRequestError(
                "A retryable network error occurred while downloading "
                f"{spec.remote_path}"
            ) from exc
        except httpx.HTTPError as exc:
            raise RemoteRequestError(
                "An HTTP transport error occurred while downloading "
                f"{spec.remote_path}"
            ) from exc

        if byte_count <= 0:
            raise DownloadIntegrityError("Downloaded archive contains zero bytes")

        if expected_length is not None and byte_count != expected_length:
            raise DownloadIntegrityError(
                "Downloaded byte count does not match Content-Length: "
                f"received={byte_count}, expected={expected_length}"
            )

        content_sha256 = digest.hexdigest()

        try:
            validation = validate_parquet_file(temporary, spec)
        except ParquetValidationError:
            quarantined = quarantine_file(
                temporary,
                self._storage.quarantine_path,
                spec=spec,
                reason="invalid_downloaded_archive",
            )
            log.warning(
                "Quarantined structurally invalid downloaded archive: %s",
                quarantined.name,
            )
            raise

        return self._publish_validated_file(
            spec,
            destination=destination,
            temporary=temporary,
            validation=validation,
            file_size=byte_count,
            content_sha256=content_sha256,
            attempt=attempt,
        )

    def _publish_validated_file(
        self,
        spec: SourceFileSpec,
        *,
        destination: Path,
        temporary: Path,
        validation,
        file_size: int,
        content_sha256: str,
        attempt: int,
    ) -> DownloadArtifact:
        """Publish without overwriting a concurrently created destination.

        The temporary file resides in the destination directory. Creating a
        hard link gives us an atomic create-if-absent operation on normal NTFS
        and POSIX filesystems. It prevents one process from silently replacing
        another process's already published immutable source file.
        """
        try:
            os.link(temporary, destination)
        except FileExistsError:
            existing = self._existing_artifact(spec, destination)

            if existing is None:
                raise DownloadConflictError(
                    "A competing writer created an unusable source archive"
                )

            if (
                existing.file_size_bytes == file_size
                and existing.content_sha256 == content_sha256
            ):
                return existing

            quarantined = quarantine_file(
                temporary,
                self._storage.quarantine_path,
                spec=spec,
                reason="conflicting_downloaded_archive",
            )

            raise DownloadConflictError(
                "Different valid content was downloaded for an existing "
                "immutable source identity. The newly downloaded file was "
                f"quarantined as {quarantined.name!r}"
            )

        except OSError as exc:
            raise DownloadIntegrityError(
                "Could not atomically publish the validated archive. "
                "The raw destination filesystem must support same-filesystem "
                "hard-link creation."
            ) from exc

        try:
            temporary.unlink()
        except OSError:
            # The canonical destination is already a complete hard link to the
            # validated temporary file. Preserve the valid publication. The
            # enclosing download attempt's finally block retries removal of the
            # redundant temporary directory entry.
            log.warning(
                "Archive publication succeeded, but temporary-file cleanup "
                "will be retried for %s",
                temporary.name,
                exc_info=True,
            )

        return DownloadArtifact(
            spec=spec,
            local_path=destination,
            disposition=DownloadDisposition.DOWNLOADED,
            file_size_bytes=file_size,
            content_sha256=content_sha256,
            validation=validation,
            attempts=attempt,
        )


__all__ = ["CryptoHFTDownloader"]

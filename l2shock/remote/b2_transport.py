# l2shock/remote/b2_transport.py
"""Bounded, version-aware B2 S3 object transport.

This module deliberately does not implement processed-artifact publication
atomicity or immutable-key conflict resolution. Those belong to the verified
repository adapter built above this transport.

The caller owns local temporary-file paths and guarantees that a source file
is not mutated while it is being uploaded.
"""

from __future__ import annotations

import email.utils
import hashlib
import os
import random
import re
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from l2shock.config import B2Config
from l2shock.filesystem import (
    OwnedPathError,
    prepare_owned_file_path,
)

MAX_B2_TRANSPORT_BYTES = 512 * 1024 * 1024
_READ_CHUNK_BYTES = 1024 * 1024
_SHA256_METADATA_KEY = "l2shock-transport-sha256"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class B2TransportError(RuntimeError):
    """A B2 operation failed without exposing SDK request details."""


class B2IntegrityError(B2TransportError):
    """Object identity, size, or transport checksum failed verification."""


class B2RateLimitError(B2TransportError):
    """A server-directed retry delay exceeds the configured wait budget."""


@dataclass(frozen=True, slots=True)
class B2ObjectInfo:
    key: str
    size_bytes: int
    version_id: str
    transport_sha256: str | None


@dataclass(frozen=True, slots=True)
class B2DownloadedObject:
    info: B2ObjectInfo
    data: bytes


def _key_text(value: str, *, prefix: bool = False) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("B2 object keys and prefixes must be nonempty strings")

    candidate = value[:-1] if prefix and value.endswith("/") else value

    if (
        not candidate
        or candidate.startswith("/")
        or "\\" in candidate
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or any(component in {"", ".", ".."} for component in candidate.split("/"))
        or len(value.encode("utf-8")) > 1024
    ):
        raise ValueError("B2 object key or prefix is not canonical")

    return value


def _version_text(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise B2IntegrityError("B2 did not provide a usable object version ID")

    return value


def _sha256_text(value: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError("Transport SHA-256 must be 64 lowercase hexadecimal digits")

    return value


def _positive_byte_limit(value: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > MAX_B2_TRANSPORT_BYTES
    ):
        raise ValueError(
            "Byte limit must be a positive integer no greater than "
            "MAX_B2_TRANSPORT_BYTES"
        )

    return value


def _retry_after_seconds(headers: Any) -> float | None:
    if not hasattr(headers, "items"):
        return None

    raw_value = next(
        (
            value
            for name, value in headers.items()
            if str(name).lower() == "retry-after"
        ),
        None,
    )

    if raw_value is None:
        return None

    text = str(raw_value).strip()

    if text.isascii() and text.isdigit():
        # Avoid parsing an arbitrarily large server-provided integer.
        if len(text) > 10:
            raise B2RateLimitError(
                "B2 requested a retry delay beyond the supported wait budget"
            )
        return float(int(text))

    try:
        target = email.utils.parsedate_to_datetime(text)
    except TypeError, ValueError, OverflowError:
        return None

    if target is None:
        return None

    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)

    return max(
        0.0,
        (target.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds(),
    )


def _remove_b2_expect_header(
    *,
    request: Any,
    **kwargs: Any,
) -> None:
    """Avoid botocore's separate Expect/100-continue response-reading path.

    This handler belongs only to the dedicated B2 client. It runs before
    signing, including when the SDK creates a fresh request for a retry.

    TLS verification, request signing, request-body streaming, and ordinary
    SDK retry handling remain enabled.
    """
    headers = request.headers

    for name in tuple(headers):
        if name.lower() == "expect":
            del headers[name]


class _B2RetryHandler:
    """Supplement standard SDK retries with B2-specific rate-limit handling."""

    def __init__(self, settings: B2Config) -> None:
        self._total_max_attempts = settings.total_max_attempts
        self._maximum_retry_after_seconds = settings.maximum_retry_after_seconds

    def __call__(
        self,
        *,
        response: Any = None,
        attempts: int = 1,
        **kwargs: Any,
    ) -> float | None:
        if response is None or attempts >= self._total_max_attempts:
            return None

        http_response, parsed_response = response
        status = getattr(http_response, "status_code", None)
        code = parsed_response.get("Error", {}).get("Code", "")

        retryable = status in {429, 500, 502, 503, 504} or code in {
            "SlowDown",
            "TooManyRequestsException",
        }

        if not retryable:
            return None

        retry_after = _retry_after_seconds(
            getattr(http_response, "headers", {}),
        )

        if retry_after is not None and retry_after > self._maximum_retry_after_seconds:
            raise B2RateLimitError(
                "B2 Retry-After exceeds the configured wait budget; "
                "resume the operation later"
            )

        jitter = random.uniform(
            0.0,
            min(20.0, 2.0 ** min(attempts - 1, 10)),
        )

        if retry_after is None:
            return jitter

        # Never retry earlier than the server-directed delay.
        return max(retry_after, jitter)


def build_b2_s3_client(settings: B2Config) -> Any:
    if not isinstance(settings, B2Config):
        raise TypeError("settings must be B2Config")

    if not settings.configured:
        raise B2TransportError(
            "B2 requires an endpoint, bucket, application key ID, "
            "and application key"
        )

    sdk_config = Config(
        signature_version="s3v4",
        connect_timeout=settings.connect_timeout_seconds,
        read_timeout=settings.read_timeout_seconds,
        retries={
            "mode": "standard",
            "total_max_attempts": settings.total_max_attempts,
        },
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )

    try:
        client = boto3.session.Session().client(
            "s3",
            endpoint_url=settings.endpoint_url,
            region_name=settings.region,
            aws_access_key_id=settings.key_id.get_secret_value(),
            aws_secret_access_key=settings.application_key.get_secret_value(),
            config=sdk_config,
        )
    except BotoCoreError, ValueError:
        raise B2TransportError("Could not initialize the B2 S3 client") from None

    client.meta.events.register_first(
        "before-sign.s3",
        _remove_b2_expect_header,
        unique_id="l2shock-b2-remove-expect",
    )

    client.meta.events.register_first(
        "needs-retry.s3",
        _B2RetryHandler(settings),
        unique_id="l2shock-b2-retry-after",
    )

    return client


def _object_info(key: str, response: dict[str, Any]) -> B2ObjectInfo:
    size = response.get("ContentLength")

    if (
        isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
        or size > MAX_B2_TRANSPORT_BYTES
    ):
        raise B2IntegrityError(
            "B2 object size is missing or exceeds the transport limit"
        )

    version_id = _version_text(response.get("VersionId"))
    metadata = response.get("Metadata", {})

    if not isinstance(metadata, dict):
        raise B2IntegrityError("B2 object metadata is malformed")

    checksum = metadata.get(_SHA256_METADATA_KEY)

    if checksum is not None:
        try:
            checksum = _sha256_text(checksum)
        except ValueError:
            raise B2IntegrityError(
                "B2 transport checksum metadata is malformed"
            ) from None

    return B2ObjectInfo(
        key=key,
        size_bytes=size,
        version_id=version_id,
        transport_sha256=checksum,
    )


class B2ObjectStore:
    """Small B2 transport with bounded reads and version-aware verification."""

    def __init__(
        self,
        settings: B2Config,
        *,
        client: Any | None = None,
    ) -> None:
        if not isinstance(settings, B2Config):
            raise TypeError("settings must be B2Config")

        if not settings.configured:
            raise B2TransportError("B2 transport configuration is incomplete")

        self._endpoint_url = settings.endpoint_url
        self._bucket = settings.bucket
        self._client = client if client is not None else build_b2_s3_client(settings)
        self._closed = False

    def __repr__(self) -> str:
        return "B2ObjectStore()"

    @property
    def endpoint_url(self) -> str:
        """Configured non-secret regional endpoint."""
        return self._endpoint_url

    @property
    def bucket(self) -> str:
        """Configured non-secret bucket identity."""
        return self._bucket

    def __enter__(self) -> B2ObjectStore:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._client.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise B2TransportError("B2 transport is closed")

    def _request(
        self,
        operation: str,
        *,
        allow_missing: bool = False,
        **parameters: Any,
    ) -> dict[str, Any] | None:
        self._ensure_open()

        try:
            return getattr(self._client, operation)(
                Bucket=self._bucket,
                **parameters,
            )
        except B2RateLimitError:
            raise
        except ClientError as exc:
            response = exc.response
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = response.get("Error", {}).get("Code", "")
            # Missing buckets and permission failures must not be treated as
            # evidence that a particular artifact is absent.
            if (
                allow_missing
                and status == 404
                and code in {"404", "NotFound", "NoSuchKey", "NoSuchVersion"}
            ):
                return None
            # A 403 may hide an absent key when listing permission is missing,
            # but it may also deny access to an existing object. It is not
            # evidence of absence. Fail closed through the sanitized error
            # boundary below rather than reporting missing content.
            safe_status = (
                str(status)
                if isinstance(status, int) and not isinstance(status, bool)
                else "unknown"
            )

            # Expose only fixed, application-owned classifications.
            # Never include the server's Message, request URL, credentials,
            # arbitrary error code, or SDK exception arguments.
            safe_codes = {
                "AccessDenied",
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "RequestTimeTooSkewed",
                "ExpiredToken",
                "InvalidToken",
                "NoSuchBucket",
                "NoSuchKey",
                "NoSuchVersion",
                "SlowDown",
                "TooManyRequestsException",
                "RequestTimeout",
                "InternalError",
                "ServiceUnavailable",
            }
            safe_code = (
                code if isinstance(code, str) and code in safe_codes else "Unclassified"
            )
            safe_status = f"{safe_status}; error category {safe_code}"

            # Server error messages and SDK exception arguments may contain
            # sensitive request details. Keep the public boundary secret-safe.
            raise B2TransportError(
                f"B2 {operation} failed with HTTP status {safe_status}"
            ) from None
        except (BotoCoreError, OSError) as exc:
            raise B2TransportError(
                f"B2 {operation} failed before/during request: " f"{type(exc).__name__}"
            ) from None

    def head(
        self,
        key: str,
        *,
        version_id: str | None = None,
    ) -> B2ObjectInfo | None:
        key = _key_text(key)
        parameters: dict[str, Any] = {"Key": key}

        if version_id is not None:
            parameters["VersionId"] = _version_text(version_id)

        response = self._request(
            "head_object",
            allow_missing=True,
            **parameters,
        )

        if response is None:
            return None

        info = _object_info(key, response)

        if version_id is not None and info.version_id != version_id:
            raise B2IntegrityError("B2 HEAD returned a different object version")

        return info

    def _put(
        self,
        key: str,
        body: bytes | BinaryIO,
        *,
        size_bytes: int,
        transport_sha256: str,
        content_type: str,
    ) -> B2ObjectInfo:
        response = self._request(
            "put_object",
            Key=key,
            Body=body,
            ContentLength=size_bytes,
            ContentType=content_type,
            Metadata={_SHA256_METADATA_KEY: transport_sha256},
        )

        assert response is not None
        version_id = _version_text(response.get("VersionId"))
        info = self.head(key, version_id=version_id)

        if (
            info is None
            or info.size_bytes != size_bytes
            or info.transport_sha256 != transport_sha256
        ):
            raise B2IntegrityError("B2 upload metadata verification failed")

        return info

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> B2ObjectInfo:
        key = _key_text(key)

        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        if len(data) > MAX_B2_TRANSPORT_BYTES:
            raise ValueError("Upload exceeds the B2 transport size limit")

        return self._put(
            key,
            data,
            size_bytes=len(data),
            transport_sha256=hashlib.sha256(data).hexdigest(),
            content_type=content_type,
        )

    def put_file(
        self,
        key: str,
        source_path: Path | str,
        *,
        content_type: str = "application/octet-stream",
    ) -> B2ObjectInfo:
        key = _key_text(key)
        source = Path(source_path)

        if source.is_symlink() or not source.is_file():
            raise B2TransportError("Upload source must be a regular non-symlink file")

        try:
            with source.open("rb") as handle:
                before = os.fstat(handle.fileno())

                if before.st_size > MAX_B2_TRANSPORT_BYTES:
                    raise ValueError("Upload exceeds the B2 transport size limit")

                digest = hashlib.sha256()
                measured_size = 0

                while chunk := handle.read(_READ_CHUNK_BYTES):
                    measured_size += len(chunk)

                    if measured_size > MAX_B2_TRANSPORT_BYTES:
                        raise ValueError("Upload exceeds the B2 transport size limit")

                    digest.update(chunk)

                if measured_size != before.st_size:
                    raise B2IntegrityError("Upload source changed during hashing")

                handle.seek(0)

                info = self._put(
                    key,
                    handle,
                    size_bytes=measured_size,
                    transport_sha256=digest.hexdigest(),
                    content_type=content_type,
                )

                after = os.fstat(handle.fileno())

                if (
                    after.st_size != before.st_size
                    or after.st_mtime_ns != before.st_mtime_ns
                ):
                    raise B2IntegrityError("Upload source changed during publication")

                return info
        except OSError:
            raise B2TransportError("Could not read the upload source") from None

    def get_bytes(
        self,
        key: str,
        *,
        maximum_bytes: int,
        version_id: str | None = None,
        expected_sha256: str | None = None,
    ) -> B2DownloadedObject | None:
        key = _key_text(key)
        maximum_bytes = _positive_byte_limit(maximum_bytes)

        if expected_sha256 is not None:
            expected_sha256 = _sha256_text(expected_sha256)

        parameters: dict[str, Any] = {"Key": key}

        if version_id is not None:
            parameters["VersionId"] = _version_text(version_id)

        response = self._request(
            "get_object",
            allow_missing=True,
            **parameters,
        )

        if response is None:
            return None

        body = response.get("Body")

        if body is None or not callable(getattr(body, "read", None)):
            raise B2IntegrityError("B2 GET did not provide a readable response body")

        try:
            info = _object_info(key, response)

            if version_id is not None and info.version_id != version_id:
                raise B2IntegrityError("B2 GET returned a different object version")

            if info.size_bytes > maximum_bytes:
                raise B2IntegrityError("B2 object exceeds the requested download limit")

            if (
                expected_sha256 is not None
                and info.transport_sha256 is not None
                and info.transport_sha256 != expected_sha256
            ):
                raise B2IntegrityError(
                    "B2 transport metadata disagrees with expected hash"
                )

            data = bytearray()
            digest = hashlib.sha256()

            while True:
                chunk = body.read(min(_READ_CHUNK_BYTES, maximum_bytes - len(data) + 1))

                if not chunk:
                    break

                if not isinstance(chunk, bytes):
                    raise B2IntegrityError("B2 response body returned non-byte content")

                if len(data) + len(chunk) > maximum_bytes:
                    raise B2IntegrityError(
                        "B2 response exceeded the download byte limit"
                    )

                data.extend(chunk)
                digest.update(chunk)

            if len(data) != info.size_bytes:
                raise B2IntegrityError(
                    "B2 response length does not match object metadata"
                )

            actual_sha256 = digest.hexdigest()

            if expected_sha256 is not None and actual_sha256 != expected_sha256:
                raise B2IntegrityError(
                    "Downloaded B2 object failed SHA-256 verification"
                )

            if (
                info.transport_sha256 is not None
                and actual_sha256 != info.transport_sha256
            ):
                raise B2IntegrityError(
                    "Downloaded B2 object disagrees with its checksum"
                )

            return B2DownloadedObject(
                info=info,
                data=bytes(data),
            )
        except BotoCoreError, OSError:
            raise B2TransportError("Could not read the B2 response body") from None
        finally:
            try:
                body.close()
            except Exception:
                # Cleanup must not replace a verification/transport failure.
                pass

    def get_file(
        self,
        key: str,
        destination_path: Path | str,
        *,
        owned_root: Path | str,
        maximum_bytes: int,
        version_id: str | None = None,
        expected_sha256: str | None = None,
    ) -> B2ObjectInfo | None:
        """Stream, verify, and publish an object to a new owned local file.

        The destination must not already exist. Publication uses a hard link
        from a verified sibling temporary file, so an existing destination is
        never overwritten and readers never observe a partially written file.

        The caller owns the storage root and must not concurrently replace its
        directories. On filesystems without hard-link support, publication
        fails closed rather than falling back to an overwriting rename.
        """
        self._ensure_open()
        key = _key_text(key)
        maximum_bytes = _positive_byte_limit(maximum_bytes)

        if expected_sha256 is not None:
            expected_sha256 = _sha256_text(expected_sha256)

        parameters: dict[str, Any] = {"Key": key}

        if version_id is not None:
            parameters["VersionId"] = _version_text(version_id)

        try:
            destination = prepare_owned_file_path(
                owned_root,
                destination_path,
                create_parents=True,
            )

            if os.path.lexists(destination):
                raise B2TransportError("B2 download destination must not already exist")

            temporary = destination.with_name(
                "." + destination.name + "." + uuid.uuid4().hex + ".part"
            )
            temporary = prepare_owned_file_path(
                owned_root,
                temporary,
            )
        except OwnedPathError, OSError:
            raise B2TransportError(
                "B2 download destination is not an owned file path"
            ) from None

        response = self._request(
            "get_object",
            allow_missing=True,
            **parameters,
        )

        if response is None:
            return None

        body = response.get("Body")
        temporary_created = False

        try:
            if body is None or not callable(getattr(body, "read", None)):
                raise B2IntegrityError(
                    "B2 GET did not provide a readable response body"
                )

            info = _object_info(key, response)

            if version_id is not None and info.version_id != version_id:
                raise B2IntegrityError("B2 GET returned a different object version")

            if info.size_bytes > maximum_bytes:
                raise B2IntegrityError("B2 object exceeds the requested download limit")

            if (
                expected_sha256 is not None
                and info.transport_sha256 is not None
                and info.transport_sha256 != expected_sha256
            ):
                raise B2IntegrityError(
                    "B2 transport metadata disagrees with expected hash"
                )

            digest = hashlib.sha256()
            measured_size = 0

            with temporary.open("xb") as handle:
                temporary_created = True

                while True:
                    chunk = body.read(
                        min(
                            _READ_CHUNK_BYTES,
                            maximum_bytes - measured_size + 1,
                        )
                    )

                    if not chunk:
                        break

                    if not isinstance(chunk, bytes):
                        raise B2IntegrityError(
                            "B2 response body returned non-byte content"
                        )

                    if measured_size + len(chunk) > maximum_bytes:
                        raise B2IntegrityError(
                            "B2 response exceeded the download byte limit"
                        )

                    handle.write(chunk)
                    digest.update(chunk)
                    measured_size += len(chunk)

                if measured_size != info.size_bytes:
                    raise B2IntegrityError(
                        "B2 response length does not match object metadata"
                    )

                actual_sha256 = digest.hexdigest()

                if expected_sha256 is not None and actual_sha256 != expected_sha256:
                    raise B2IntegrityError(
                        "Downloaded B2 object failed SHA-256 verification"
                    )

                if (
                    info.transport_sha256 is not None
                    and actual_sha256 != info.transport_sha256
                ):
                    raise B2IntegrityError(
                        "Downloaded B2 object disagrees with its checksum"
                    )

                handle.flush()
                os.fsync(handle.fileno())

            # Revalidate ownership immediately before local publication.
            prepare_owned_file_path(owned_root, temporary)
            prepare_owned_file_path(owned_root, destination)

            try:
                os.link(temporary, destination)
            except FileExistsError:
                raise B2TransportError(
                    "B2 download destination appeared during publication"
                ) from None

            return info
        except OwnedPathError, BotoCoreError, OSError:
            raise B2TransportError(
                "Could not stream or publish the B2 download"
            ) from None
        finally:
            if body is not None:
                close_body = getattr(body, "close", None)

                if callable(close_body):
                    try:
                        close_body()
                    except Exception:
                        # Cleanup must not replace the operation's result.
                        pass

            if temporary_created:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    # A successfully published destination remains valid.
                    # A failed download is never published at destination.
                    pass

    def iter_keys(self, prefix: str) -> Iterator[str]:
        prefix = _key_text(prefix, prefix=True)
        token: str | None = None
        seen_tokens: set[str] = set()

        while True:
            parameters: dict[str, Any] = {
                "Prefix": prefix,
                "MaxKeys": 1000,
            }

            if token is not None:
                parameters["ContinuationToken"] = token

            response = self._request(
                "list_objects_v2",
                **parameters,
            )

            assert response is not None
            entries = response.get("Contents", [])

            if not isinstance(entries, list):
                raise B2IntegrityError("B2 object listing is malformed")

            for entry in entries:
                if not isinstance(entry, dict):
                    raise B2IntegrityError(
                        "B2 object listing contains a malformed entry"
                    )

                try:
                    key = _key_text(entry.get("Key"))
                except TypeError, ValueError:
                    raise B2IntegrityError(
                        "B2 listing returned an invalid key"
                    ) from None

                if not key.startswith(prefix):
                    raise B2IntegrityError(
                        "B2 listing returned a key outside its prefix"
                    )

                yield key

            truncated = response.get("IsTruncated", False)

            if not isinstance(truncated, bool):
                raise B2IntegrityError("B2 listing has an invalid pagination flag")

            if not truncated:
                return

            next_token = response.get("NextContinuationToken")

            if (
                not isinstance(next_token, str)
                or not next_token
                or next_token in seen_tokens
            ):
                raise B2IntegrityError("B2 listing has an invalid continuation token")

            seen_tokens.add(next_token)
            token = next_token

from __future__ import annotations

import hashlib
import io
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from l2shock.config import B2Config
from l2shock.remote import b2_transport
from l2shock.remote.b2_transport import (
    B2IntegrityError,
    B2ObjectStore,
    B2RateLimitError,
    B2TransportError,
    _B2RetryHandler,
)

_CHECKSUM_KEY = "l2shock-transport-sha256"


def _settings(**overrides) -> B2Config:
    values = {
        "endpoint_url": "https://s3.us-west-004.backblazeb2.com",
        "bucket": "l2shock-test-storage",
        "key_id": "test-key-id",
        "application_key": "test-application-key",
    }
    values.update(overrides)
    return B2Config(**values)


def _info(data: bytes, *, version: str = "version-1") -> dict:
    return {
        "ContentLength": len(data),
        "VersionId": version,
        "Metadata": {
            _CHECKSUM_KEY: hashlib.sha256(data).hexdigest(),
        },
    }


def _client_error(code: str, status: int, operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": code,
                "Message": "private-server-message-test-application-key",
            },
            "ResponseMetadata": {
                "HTTPStatusCode": status,
            },
        },
        operation,
    )


class _Events:
    def __init__(self) -> None:
        self.registered = []

    def register_first(self, event, handler, unique_id=None) -> None:
        self.registered.append((event, handler, unique_id))


class _FakeClient:
    def __init__(self) -> None:
        self.responses = {}
        self.calls = []
        self.closed = False
        self.meta = SimpleNamespace(events=_Events())

    def queue(self, operation: str, *responses) -> None:
        self.responses.setdefault(operation, []).extend(responses)

    def _call(self, operation: str, parameters: dict):
        recorded = dict(parameters)

        if operation == "put_object":
            body = recorded["Body"]
            recorded["BodyBytes"] = body if isinstance(body, bytes) else body.read()

        self.calls.append((operation, recorded))

        queue = self.responses.get(operation, [])

        if not queue:
            raise AssertionError(f"Unexpected fake call: {operation}")

        response = queue.pop(0)

        if isinstance(response, BaseException):
            raise response

        return response

    def head_object(self, **parameters):
        return self._call("head_object", parameters)

    def get_object(self, **parameters):
        return self._call("get_object", parameters)

    def put_object(self, **parameters):
        return self._call("put_object", parameters)

    def list_objects_v2(self, **parameters):
        return self._call("list_objects_v2", parameters)

    def close(self) -> None:
        self.closed = True


def test_b2_client_uses_explicit_credentials_endpoint_and_bounded_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    captured = {}

    class FakeSession:
        def client(self, service, **parameters):
            captured["service"] = service
            captured.update(parameters)
            return client

    monkeypatch.setattr(
        b2_transport.boto3.session,
        "Session",
        FakeSession,
    )

    result = b2_transport.build_b2_s3_client(_settings())

    assert result is client
    assert captured["service"] == "s3"
    assert captured["endpoint_url"] == ("https://s3.us-west-004.backblazeb2.com")
    assert captured["region_name"] == "us-west-004"
    assert captured["aws_access_key_id"] == "test-key-id"
    assert captured["aws_secret_access_key"] == "test-application-key"

    sdk_config = captured["config"]

    assert sdk_config.signature_version == "s3v4"
    assert sdk_config.retries == {
        "mode": "standard",
        "total_max_attempts": 6,
    }
    assert sdk_config.s3["addressing_style"] == "path"
    assert sdk_config.request_checksum_calculation == "when_required"
    assert sdk_config.response_checksum_validation == "when_required"

    registered_events = [
        (event, uid) for event, _handler, uid in client.meta.events.registered
    ]

    assert ("needs-retry.s3", "l2shock-b2-retry-after") in registered_events
    assert ("before-sign.s3", "l2shock-b2-remove-expect") in registered_events
    assert not any(
        uid == "l2shock-b2-connection-close" for _event, uid in registered_events
    )


def test_b2_client_refuses_incomplete_configuration() -> None:
    with pytest.raises(B2TransportError, match="requires"):
        b2_transport.build_b2_s3_client(B2Config())


def test_put_bytes_verifies_the_uploaded_version() -> None:
    data = b"processed artifact"
    client = _FakeClient()
    client.queue("put_object", {"VersionId": "version-1"})
    client.queue("head_object", _info(data))

    store = B2ObjectStore(_settings(), client=client)
    result = store.put_bytes("processed/test/artifact.parquet", data)

    assert result.version_id == "version-1"
    assert result.transport_sha256 == hashlib.sha256(data).hexdigest()

    put_call = client.calls[0][1]
    head_call = client.calls[1][1]

    assert put_call["Bucket"] == "l2shock-test-storage"
    assert put_call["BodyBytes"] == data
    assert put_call["ContentLength"] == len(data)
    assert head_call["VersionId"] == "version-1"


def test_put_file_streams_a_regular_file(tmp_path: Path) -> None:
    data = b"local transport artifact"
    path = tmp_path / "artifact.parquet"
    path.write_bytes(data)

    client = _FakeClient()
    client.queue("put_object", {"VersionId": "version-1"})
    client.queue("head_object", _info(data))

    store = B2ObjectStore(_settings(), client=client)
    result = store.put_file("processed/test/artifact.parquet", path)

    assert result.size_bytes == len(data)
    assert client.calls[0][1]["BodyBytes"] == data


def test_put_refuses_missing_version_id() -> None:
    client = _FakeClient()
    client.queue("put_object", {})

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="version ID"):
        store.put_bytes("processed/test/object", b"data")


def test_put_detects_wrong_uploaded_metadata() -> None:
    client = _FakeClient()
    client.queue("put_object", {"VersionId": "version-1"})
    client.queue("head_object", _info(b"different-data"))

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="upload metadata"):
        store.put_bytes("processed/test/object", b"data")


@pytest.mark.parametrize("code", ("404", "NotFound", "NoSuchKey"))
def test_head_returns_none_only_for_object_absence(code: str) -> None:
    client = _FakeClient()
    client.queue("head_object", _client_error(code, 404, "HeadObject"))

    store = B2ObjectStore(_settings(), client=client)

    assert store.head("processed/test/missing") is None


@pytest.mark.parametrize(
    ("code", "status"),
    (
        ("AccessDenied", 403),
        ("NoSuchBucket", 404),
        ("SlowDown", 503),
    ),
)
def test_head_does_not_misclassify_bucket_permission_or_transport_errors(
    code: str,
    status: int,
) -> None:
    client = _FakeClient()
    client.queue("head_object", _client_error(code, status, "HeadObject"))

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2TransportError) as captured:
        store.head("processed/test/object")

    message = str(captured.value)

    assert "private-server-message" not in message
    assert "test-application-key" not in message
    assert captured.value.__suppress_context__ is True


def test_get_verifies_hash_version_and_closes_body() -> None:
    data = b"verified data"
    body = io.BytesIO(data)
    client = _FakeClient()
    client.queue(
        "get_object",
        {
            **_info(data),
            "Body": body,
        },
    )

    store = B2ObjectStore(_settings(), client=client)
    result = store.get_bytes(
        "processed/test/object",
        maximum_bytes=1024,
        version_id="version-1",
        expected_sha256=hashlib.sha256(data).hexdigest(),
    )

    assert result is not None
    assert result.data == data
    assert body.closed is True
    assert client.calls[0][1]["VersionId"] == "version-1"


def test_get_detects_corrupt_content_even_with_matching_metadata() -> None:
    expected = b"expected"
    body = io.BytesIO(b"corrupt!")
    client = _FakeClient()
    client.queue(
        "get_object",
        {
            **_info(expected),
            "Body": body,
        },
    )

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="SHA-256"):
        store.get_bytes(
            "processed/test/object",
            maximum_bytes=1024,
            expected_sha256=hashlib.sha256(expected).hexdigest(),
        )

    assert body.closed is True


def test_get_rejects_response_larger_than_declared_length() -> None:
    body = io.BytesIO(b"too-large")
    client = _FakeClient()
    client.queue(
        "get_object",
        {
            "ContentLength": 1,
            "VersionId": "version-1",
            "Metadata": {},
            "Body": body,
        },
    )

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="byte limit"):
        store.get_bytes(
            "processed/test/object",
            maximum_bytes=4,
        )

    assert body.closed is True


def test_get_rejects_declared_oversize_before_reading() -> None:
    body = io.BytesIO(b"large")
    client = _FakeClient()
    client.queue(
        "get_object",
        {
            **_info(b"large"),
            "Body": body,
        },
    )

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="download limit"):
        store.get_bytes(
            "processed/test/object",
            maximum_bytes=2,
        )

    assert body.closed is True


def test_get_rejects_a_different_version() -> None:
    body = io.BytesIO(b"data")
    client = _FakeClient()
    client.queue(
        "get_object",
        {
            **_info(b"data", version="version-2"),
            "Body": body,
        },
    )

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="different object version"):
        store.get_bytes(
            "processed/test/object",
            maximum_bytes=100,
            version_id="version-1",
        )

    assert body.closed is True


def test_get_accepts_legacy_object_without_checksum_metadata() -> None:
    data = b"legacy verified bytes"
    client = _FakeClient()
    client.queue(
        "get_object",
        {
            "ContentLength": len(data),
            "VersionId": "version-1",
            "Metadata": {},
            "Body": io.BytesIO(data),
        },
    )

    store = B2ObjectStore(_settings(), client=client)
    result = store.get_bytes(
        "processed/test/object",
        maximum_bytes=1024,
        expected_sha256=hashlib.sha256(data).hexdigest(),
    )

    assert result is not None
    assert result.data == data


def test_listing_follows_continuation_tokens() -> None:
    client = _FakeClient()
    client.queue(
        "list_objects_v2",
        {
            "Contents": [{"Key": "processed/test/00.manifest.json"}],
            "IsTruncated": True,
            "NextContinuationToken": "next-page",
        },
        {
            "Contents": [{"Key": "processed/test/01.manifest.json"}],
            "IsTruncated": False,
        },
    )

    store = B2ObjectStore(_settings(), client=client)

    assert list(store.iter_keys("processed/test/")) == [
        "processed/test/00.manifest.json",
        "processed/test/01.manifest.json",
    ]
    assert client.calls[1][1]["ContinuationToken"] == "next-page"


def test_listing_rejects_repeated_continuation_token() -> None:
    client = _FakeClient()
    page = {
        "Contents": [],
        "IsTruncated": True,
        "NextContinuationToken": "repeated",
    }
    client.queue("list_objects_v2", page, page)

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="continuation token"):
        list(store.iter_keys("processed/test/"))


def test_listing_rejects_keys_outside_requested_prefix() -> None:
    client = _FakeClient()
    client.queue(
        "list_objects_v2",
        {
            "Contents": [{"Key": "processed/another-chain/object"}],
            "IsTruncated": False,
        },
    )

    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2IntegrityError, match="outside its prefix"):
        list(store.iter_keys("processed/test/"))


@pytest.mark.parametrize(
    "key",
    (
        "",
        "/absolute",
        "processed/../escape",
        "processed//double",
        "processed\\windows",
        "processed/control\ncharacter",
    ),
)
def test_invalid_keys_are_rejected_before_network_calls(key: str) -> None:
    client = _FakeClient()
    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(ValueError):
        store.head(key)

    assert client.calls == []


def test_rate_limit_handler_honors_retry_after() -> None:
    handler = _B2RetryHandler(_settings())
    response = (
        SimpleNamespace(
            status_code=503,
            headers={"Retry-After": "30"},
        ),
        {"Error": {"Code": "SlowDown"}},
    )

    delay = handler(response=response, attempts=1)

    assert delay is not None
    assert delay >= 30


def test_rate_limit_handler_handles_unmodeled_http_429() -> None:
    handler = _B2RetryHandler(_settings())
    response = (
        SimpleNamespace(
            status_code=429,
            headers={},
        ),
        {"Error": {"Code": "429"}},
    )

    delay = handler(response=response, attempts=1)

    assert delay is not None
    assert 0 <= delay <= 1


def test_rate_limit_handler_does_not_retry_after_attempt_budget() -> None:
    handler = _B2RetryHandler(_settings(total_max_attempts=2))
    response = (
        SimpleNamespace(
            status_code=503,
            headers={"Retry-After": "30"},
        ),
        {"Error": {"Code": "SlowDown"}},
    )

    assert handler(response=response, attempts=2) is None


def test_rate_limit_handler_does_not_shorten_long_server_wait() -> None:
    handler = _B2RetryHandler(_settings(maximum_retry_after_seconds=30))
    response = (
        SimpleNamespace(
            status_code=503,
            headers={"Retry-After": "120"},
        ),
        {"Error": {"Code": "SlowDown"}},
    )

    with pytest.raises(B2RateLimitError, match="wait budget"):
        handler(response=response, attempts=1)


def test_rate_limit_handler_does_not_retry_permission_failure() -> None:
    handler = _B2RetryHandler(_settings())
    response = (
        SimpleNamespace(
            status_code=403,
            headers={"Retry-After": "30"},
        ),
        {"Error": {"Code": "AccessDenied"}},
    )

    assert handler(response=response, attempts=1) is None


def test_store_context_closes_client_and_rejects_further_operations() -> None:
    client = _FakeClient()

    with B2ObjectStore(_settings(), client=client) as store:
        assert "test-application-key" not in repr(store)

    assert client.closed is True

    with pytest.raises(B2TransportError, match="closed"):
        store.head("processed/test/object")


@pytest.mark.parametrize(
    "code",
    (
        "AccessDenied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "RequestTimeTooSkewed",
        "NoSuchBucket",
    ),
)
def test_transport_surfaces_only_allowlisted_error_categories(
    code: str,
) -> None:
    client = _FakeClient()
    client.queue(
        "head_object",
        _client_error(code, 403, "HeadObject"),
    )
    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2TransportError) as captured:
        store.head("processed/test/object")

    message = str(captured.value)

    assert f"error category {code}" in message
    assert "private-server-message" not in message
    assert "test-application-key" not in message
    assert captured.value.__cause__ is None
    assert captured.value.__suppress_context__ is True


def test_transport_does_not_echo_arbitrary_server_error_codes() -> None:
    private_code = "private-error-code-test-application-key"
    client = _FakeClient()
    client.queue(
        "head_object",
        _client_error(private_code, 403, "HeadObject"),
    )
    store = B2ObjectStore(_settings(), client=client)

    with pytest.raises(B2TransportError) as captured:
        store.head("processed/test/object")

    message = str(captured.value)

    assert "error category Unclassified" in message
    assert private_code not in message
    assert "private-server-message" not in message


@pytest.mark.parametrize("error_code", ("403", "AccessDenied"))
@pytest.mark.parametrize("method", ("head", "get_bytes", "get_file"))
def test_b2_permission_denial_never_becomes_object_absence(
    tmp_path: Path,
    error_code: str,
    method: str,
) -> None:
    client = _FakeClient()
    sdk_operation = "head_object" if method == "head" else "get_object"
    sdk_name = "HeadObject" if method == "head" else "GetObject"

    client.queue(
        sdk_operation,
        _client_error(error_code, 403, sdk_name),
    )

    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2TransportError) as captured:
            if method == "head":
                store.head(
                    "processed/test/object",
                    version_id="pinned-version",
                )
            elif method == "get_bytes":
                store.get_bytes(
                    "processed/test/object",
                    maximum_bytes=1024,
                    version_id="pinned-version",
                )
            else:
                store.get_file(
                    "processed/test/object",
                    destination,
                    owned_root=root,
                    maximum_bytes=1024,
                    version_id="pinned-version",
                )

    assert "403" in str(captured.value)
    assert "private-server-message" not in str(captured.value)
    assert "test-application-key" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__suppress_context__ is True
    assert not destination.exists()
    assert not list(root.rglob("*.part"))
    assert len(client.calls) == 1
    assert client.closed is True

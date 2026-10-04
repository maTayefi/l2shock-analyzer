"""B2 SDK request construction and retry-loop regression tests.

The real SDK constructs and signs requests. Only the final HTTP send method
is replaced, so no connection to Backblaze or AWS is made.
"""

from __future__ import annotations

import io
import traceback
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.awsrequest import AWSRequest, AWSResponse
from botocore.exceptions import ClientError, ConnectionClosedError

import botocore.endpoint as endpoint_module

from l2shock.config import B2Config
from l2shock.remote.b2_transport import (
    B2ObjectStore,
    B2RateLimitError,
    B2TransportError,
    _remove_b2_expect_header,
    build_b2_s3_client,
)


def _settings(**overrides) -> B2Config:
    values = {
        "endpoint_url": "https://s3.us-west-004.backblazeb2.com",
        "bucket": "l2shock-test-storage",
        "key_id": "test-key-id",
        "application_key": "test-application-key",
        "total_max_attempts": 3,
    }
    values.update(overrides)
    return B2Config(**values)


class _RawResponse(io.BytesIO):
    def stream(self, amt=None, decode_content=False):
        del decode_content
        chunk_size = amt or 1024 * 1024

        while chunk := self.read(chunk_size):
            yield chunk


def _response(
    request,
    *,
    status: int = 200,
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> AWSResponse:
    response_headers = {
        "content-length": str(len(body)),
    }

    if headers is not None:
        response_headers.update(headers)

    return AWSResponse(
        request.url,
        status,
        response_headers,
        _RawResponse(body),
    )


def _headers(request) -> dict[str, str]:
    result = {}

    for name, value in request.headers.items():
        if isinstance(value, bytes):
            text = value.decode("ascii")
        else:
            text = str(value)

        result[name.lower()] = text

    return result


def _body_bytes(request) -> bytes:
    body = request.body

    if body is None:
        return b""

    if isinstance(body, bytes):
        return body

    return body.read()


@pytest.fixture
def sdk_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    # Isolate client creation from any real AWS profile configuration.
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv(
        "AWS_CONFIG_FILE",
        str(tmp_path / "absent-aws-config"),
    )
    monkeypatch.setenv(
        "AWS_SHARED_CREDENTIALS_FILE",
        str(tmp_path / "absent-aws-credentials"),
    )

    client = build_b2_s3_client(_settings())

    try:
        yield client
    finally:
        client.close()


@pytest.mark.parametrize("name", ("Expect", "expect", "EXPECT"))
def test_expect_removal_preserves_other_request_headers(name: str) -> None:
    request = SimpleNamespace(
        headers={
            name: "100-continue",
            "Content-Length": "4",
            "Content-Type": "application/octet-stream",
        }
    )

    _remove_b2_expect_header(request=request)

    assert request.headers == {
        "Content-Length": "4",
        "Content-Type": "application/octet-stream",
    }


def test_expect_removal_accepts_real_sdk_header_container() -> None:
    request = AWSRequest(
        method="PUT",
        url="https://example.invalid/object",
        data=b"data",
        headers={
            "Expect": "100-continue",
            "Content-Length": "4",
        },
    )

    _remove_b2_expect_header(request=request)

    assert "Expect" not in request.headers
    assert request.headers["Content-Length"] == "4"


@pytest.mark.parametrize("stream_body", (False, True))
def test_real_sdk_sends_signed_plain_body_without_expect(
    sdk_client,
    monkeypatch: pytest.MonkeyPatch,
    stream_body: bool,
) -> None:
    payload = b"complete upload body"
    observed = []

    def send(request):
        headers = _headers(request)
        observed.append((headers, _body_bytes(request)))

        return _response(
            request,
            headers={"x-amz-version-id": "version-1"},
        )

    monkeypatch.setattr(sdk_client._endpoint.http_session, "send", send)

    body = io.BytesIO(payload) if stream_body else payload

    result = sdk_client.put_object(
        Bucket="l2shock-test-storage",
        Key="processed/test/object",
        Body=body,
        ContentLength=len(payload),
    )

    assert result["VersionId"] == "version-1"
    assert len(observed) == 1

    headers, sent_body = observed[0]

    assert "expect" not in headers
    assert headers.get("connection", "").lower() != "close"
    assert headers["authorization"].startswith("AWS4-HMAC-SHA256 ")
    assert headers["content-length"] == str(len(payload))
    assert "aws-chunked" not in headers.get("content-encoding", "").lower()
    assert sent_body == payload

    signed_headers = (
        headers["authorization"]
        .split(
            "SignedHeaders=",
            1,
        )[1]
        .split(",", 1)[0]
    )

    assert "expect" not in signed_headers.split(";")


def test_sdk_rewinds_stream_after_connection_failure(
    sdk_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"must be resent from byte zero"
    observed_bodies = []
    observed_headers = []
    sleeps = []

    def send(request):
        observed_headers.append(_headers(request))
        observed_bodies.append(_body_bytes(request))

        if len(observed_bodies) == 1:
            raise ConnectionClosedError(
                endpoint_url=request.url,
                error=OSError("simulated interrupted response"),
            )

        return _response(
            request,
            headers={"x-amz-version-id": "version-2"},
        )

    monkeypatch.setattr(sdk_client._endpoint.http_session, "send", send)
    monkeypatch.setattr(endpoint_module.time, "sleep", sleeps.append)

    result = sdk_client.put_object(
        Bucket="l2shock-test-storage",
        Key="processed/test/object",
        Body=io.BytesIO(payload),
        ContentLength=len(payload),
    )

    assert result["VersionId"] == "version-2"
    assert observed_bodies == [payload, payload]
    assert len(sleeps) == 1
    assert all("expect" not in headers for headers in observed_headers)


def test_connection_failure_does_not_multiply_sdk_attempt_budget(
    sdk_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    sleeps = []

    def send(request):
        calls.append(request.method)

        raise ConnectionClosedError(
            endpoint_url=request.url,
            error=OSError("simulated persistent failure"),
        )

    monkeypatch.setattr(sdk_client._endpoint.http_session, "send", send)
    monkeypatch.setattr(endpoint_module.time, "sleep", sleeps.append)

    with B2ObjectStore(_settings(), client=sdk_client) as store:
        with pytest.raises(
            B2TransportError,
            match="ConnectionClosedError",
        ):
            store.head("processed/test/object")

    assert calls == ["HEAD", "HEAD", "HEAD"]
    assert len(sleeps) == 2


@pytest.mark.parametrize(
    ("status", "code"),
    (
        (429, "RateLimit"),
        (503, "SlowDown"),
    ),
)
def test_real_sdk_retry_loop_honors_retry_after(
    sdk_client,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    code: str,
) -> None:
    calls = []
    sleeps = []

    def send(request):
        calls.append(request.method)

        if len(calls) == 1:
            error_body = (
                "<Error><Code>" + code + "</Code><Message>retry later</Message></Error>"
            ).encode("ascii")

            return _response(
                request,
                status=status,
                body=error_body,
                headers={
                    "content-type": "application/xml",
                    "retry-after": "7",
                },
            )

        return _response(
            request,
            headers={"x-amz-version-id": "version-1"},
        )

    monkeypatch.setattr(sdk_client._endpoint.http_session, "send", send)
    monkeypatch.setattr(endpoint_module.time, "sleep", sleeps.append)

    result = sdk_client.put_object(
        Bucket="l2shock-test-storage",
        Key="processed/test/object",
        Body=b"data",
        ContentLength=4,
    )

    assert result["VersionId"] == "version-1"
    assert calls == ["PUT", "PUT"]
    assert len(sleeps) == 1
    assert sleeps[0] >= 7


def test_excessive_retry_after_aborts_without_another_request(
    sdk_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    sleeps = []

    def send(request):
        calls.append(request.method)

        return _response(
            request,
            status=503,
            body=(
                b"<Error><Code>SlowDown</Code>"
                b"<Message>retry later</Message></Error>"
            ),
            headers={
                "content-type": "application/xml",
                "retry-after": "301",
            },
        )

    monkeypatch.setattr(sdk_client._endpoint.http_session, "send", send)
    monkeypatch.setattr(endpoint_module.time, "sleep", sleeps.append)

    with pytest.raises(B2RateLimitError, match="wait budget"):
        sdk_client.put_object(
            Bucket="l2shock-test-storage",
            Key="processed/test/object",
            Body=b"data",
            ContentLength=4,
        )

    assert calls == ["PUT"]
    assert sleeps == []


class _FailureClient:
    def __init__(self, failure: BaseException) -> None:
        self.failure = failure
        self.calls = 0
        self.closed = False

    def head_object(self, **parameters):
        del parameters
        self.calls += 1
        raise self.failure

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("failure_kind", ("client", "connection"))
def test_terminal_errors_and_tracebacks_do_not_expose_sdk_details(
    failure_kind: str,
) -> None:
    private_marker = "private-request-detail-must-not-escape"

    if failure_kind == "client":
        failure = ClientError(
            {
                "Error": {
                    "Code": private_marker,
                    "Message": private_marker,
                },
                "ResponseMetadata": {
                    "HTTPStatusCode": 403,
                },
            },
            "HeadObject",
        )
    else:
        failure = ConnectionClosedError(
            endpoint_url="https://example.invalid/" + private_marker,
            error=OSError(private_marker),
        )

    client = _FailureClient(failure)

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2TransportError) as captured:
            store.head("processed/test/object")

    rendered = "".join(
        traceback.format_exception(
            type(captured.value),
            captured.value,
            captured.value.__traceback__,
        )
    )

    assert private_marker not in str(captured.value)
    assert private_marker not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__suppress_context__ is True
    assert client.calls == 1
    assert client.closed is True

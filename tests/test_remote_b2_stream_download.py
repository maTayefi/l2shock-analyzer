"""Bounded B2 file-download and no-overwrite publication tests."""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from l2shock.config import B2Config
from l2shock.remote import b2_transport
from l2shock.remote.b2_transport import (
    B2IntegrityError,
    B2ObjectStore,
    B2TransportError,
)

_CHECKSUM_KEY = "l2shock-transport-sha256"


def _settings() -> B2Config:
    return B2Config(
        endpoint_url="https://s3.us-west-004.backblazeb2.com",
        bucket="l2shock-test-storage",
        key_id="test-key-id",
        application_key="test-application-key",
    )


class _Body(io.BytesIO):
    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)


class _Client:
    def __init__(self, response) -> None:
        self.response = response
        self.calls = []
        self.closed = False

    def get_object(self, **parameters):
        self.calls.append(parameters)

        if isinstance(self.response, BaseException):
            raise self.response

        return self.response

    def close(self) -> None:
        self.closed = True


def _response(
    data: bytes,
    *,
    version: str = "version-1",
    declared_size: int | None = None,
    metadata_sha256: str | None = None,
):
    body = _Body(data)
    digest = metadata_sha256 or hashlib.sha256(data).hexdigest()

    return {
        "ContentLength": len(data) if declared_size is None else declared_size,
        "VersionId": version,
        "Metadata": {_CHECKSUM_KEY: digest},
        "Body": body,
    }


def _parts(root: Path) -> tuple[Path, ...]:
    return tuple(root.rglob("*.part"))


def test_file_download_streams_verifies_and_closes_body(tmp_path: Path) -> None:
    data = b"verified transport payload" * 100_000
    response = _response(data)
    body = response["Body"]
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "downloads" / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        info = store.get_file(
            "processed/test/artifact.parquet",
            destination,
            owned_root=root,
            maximum_bytes=len(data),
            version_id="version-1",
            expected_sha256=hashlib.sha256(data).hexdigest(),
        )

    assert info is not None
    assert info.size_bytes == len(data)
    assert info.version_id == "version-1"
    assert destination.read_bytes() == data
    assert body.closed is True
    assert client.closed is True
    assert client.calls[0]["VersionId"] == "version-1"
    assert body.read_sizes
    assert all(0 < size <= 1024 * 1024 for size in body.read_sizes)
    assert _parts(root) == ()


def test_existing_destination_is_not_overwritten_or_downloaded(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    destination = root / "artifact.parquet"
    destination.write_bytes(b"existing")
    client = _Client(_response(b"replacement"))

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2TransportError, match="already exist"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=1024,
            )

    assert destination.read_bytes() == b"existing"
    assert client.calls == []
    assert _parts(root) == ()


def test_destination_outside_owned_root_fails_before_get(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    destination = tmp_path / "outside.parquet"
    client = _Client(_response(b"data"))

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2TransportError, match="owned file path"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=1024,
            )

    assert client.calls == []
    assert not destination.exists()


def test_file_download_missing_object_returns_none(tmp_path: Path) -> None:
    client = _Client(
        ClientError(
            {
                "Error": {"Code": "NoSuchKey", "Message": "absent"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            "GetObject",
        )
    )
    root = tmp_path / "cache"
    destination = root / "missing.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        result = store.get_file(
            "processed/test/missing.parquet",
            destination,
            owned_root=root,
            maximum_bytes=1024,
        )

    assert result is None
    assert not destination.exists()
    assert _parts(root) == ()


def test_wrong_version_is_not_published(tmp_path: Path) -> None:
    response = _response(b"data", version="version-2")
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2IntegrityError, match="different object version"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=1024,
                version_id="version-1",
            )

    assert not destination.exists()
    assert response["Body"].closed is True
    assert response["Body"].read_sizes == []
    assert _parts(root) == ()


def test_declared_oversize_is_rejected_before_body_read(tmp_path: Path) -> None:
    response = _response(b"data", declared_size=100)
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2IntegrityError, match="download limit"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=10,
            )

    assert not destination.exists()
    assert response["Body"].read_sizes == []
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_stream_exceeding_limit_is_not_published(tmp_path: Path) -> None:
    response = _response(b"too much content", declared_size=4)
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2IntegrityError, match="byte limit"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=4,
            )

    assert not destination.exists()
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_truncated_stream_is_not_published(tmp_path: Path) -> None:
    response = _response(b"short", declared_size=10)
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2IntegrityError, match="response length"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=100,
            )

    assert not destination.exists()
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_corrupt_stream_with_expected_hash_is_not_published(
    tmp_path: Path,
) -> None:
    expected = b"expected"
    expected_hash = hashlib.sha256(expected).hexdigest()
    response = _response(
        b"corrupt!",
        metadata_sha256=expected_hash,
    )
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2IntegrityError, match="SHA-256"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=100,
                expected_sha256=expected_hash,
            )

    assert not destination.exists()
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_corrupt_stream_disagreeing_with_metadata_is_not_published(
    tmp_path: Path,
) -> None:
    response = _response(
        b"corrupt!",
        metadata_sha256=hashlib.sha256(b"expected").hexdigest(),
    )
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2IntegrityError, match="checksum"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=100,
            )

    assert not destination.exists()
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_legacy_object_is_verified_using_caller_hash(tmp_path: Path) -> None:
    data = b"legacy object"
    response = _response(data)
    response["Metadata"] = {}
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    with B2ObjectStore(_settings(), client=client) as store:
        info = store.get_file(
            "processed/test/artifact.parquet",
            destination,
            owned_root=root,
            maximum_bytes=100,
            expected_sha256=hashlib.sha256(data).hexdigest(),
        )

    assert info is not None
    assert info.transport_sha256 is None
    assert destination.read_bytes() == data
    assert _parts(root) == ()


def test_empty_object_can_be_downloaded_and_verified(tmp_path: Path) -> None:
    response = _response(b"")
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "empty.bin"

    with B2ObjectStore(_settings(), client=client) as store:
        info = store.get_file(
            "processed/test/empty.bin",
            destination,
            owned_root=root,
            maximum_bytes=1,
            expected_sha256=hashlib.sha256(b"").hexdigest(),
        )

    assert info is not None
    assert info.size_bytes == 0
    assert destination.read_bytes() == b""
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_publication_race_does_not_overwrite_competing_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _response(b"downloaded")
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    def competing_link(source, target):
        del source
        Path(target).write_bytes(b"competing file")
        raise FileExistsError("simulated competing publication")

    monkeypatch.setattr(b2_transport.os, "link", competing_link)

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2TransportError, match="appeared"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=100,
            )

    assert destination.read_bytes() == b"competing file"
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_failed_hard_link_does_not_publish_or_leave_partial_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _response(b"data")
    client = _Client(response)
    root = tmp_path / "cache"
    destination = root / "artifact.parquet"

    def unsupported_link(source, target):
        del source, target
        raise OSError("simulated filesystem without hard-link support")

    monkeypatch.setattr(b2_transport.os, "link", unsupported_link)

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2TransportError, match="stream or publish"):
            store.get_file(
                "processed/test/artifact.parquet",
                destination,
                owned_root=root,
                maximum_bytes=100,
            )

    assert not destination.exists()
    assert response["Body"].closed is True
    assert _parts(root) == ()


def test_symlinked_parent_is_rejected_before_get(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    redirected = root / "redirected"

    try:
        redirected.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Directory symlinks are unavailable on this platform")

    client = _Client(_response(b"data"))

    with B2ObjectStore(_settings(), client=client) as store:
        with pytest.raises(B2TransportError, match="owned file path"):
            store.get_file(
                "processed/test/artifact.parquet",
                redirected / "artifact.parquet",
                owned_root=root,
                maximum_bytes=100,
            )

    assert client.calls == []
    assert not (outside / "artifact.parquet").exists()


def test_closed_store_refuses_download_without_creating_directories(
    tmp_path: Path,
) -> None:
    client = _Client(_response(b"data"))
    store = B2ObjectStore(_settings(), client=client)
    store.close()
    root = tmp_path / "cache"

    with pytest.raises(B2TransportError, match="closed"):
        store.get_file(
            "processed/test/artifact.parquet",
            root / "artifact.parquet",
            owned_root=root,
            maximum_bytes=100,
        )

    assert client.calls == []
    assert not root.exists()

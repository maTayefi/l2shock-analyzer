from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.price import (
    build_trade_ohlc_hour,
    encode_hourly_trade_ohlc_block,
)
from l2shock.remote.artifact_codec import (
    RemotePriceProcessedArtifact,
    write_remote_artifact_file,
)
from l2shock.remote.b2_publication import (
    B2Publication,
    B2PublicationError,
    publication_relative_path,
)
from l2shock.remote.b2_repository import (
    B2ArtifactConflictError,
    B2IncompletePublicationError,
    B2ProcessedArtifactRepository,
    B2RepositoryError,
)
from l2shock.remote.b2_transport import (
    B2DownloadedObject,
    B2IntegrityError,
    B2ObjectInfo,
    B2ObjectStore,
    B2TransportError,
)
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteSourceHourReference,
)
from l2shock.acquisition import SourceDataKind


class MemoryStore(B2ObjectStore):
    """Version-retaining repository test double; no SDK/client initialization."""

    def __init__(self) -> None:
        self.versions = {}
        self.current = {}
        self.writes = []
        self.reads = []
        self.next_version = 0
        self.fail_next_descriptor = False

    def put_bytes(
        self,
        key,
        data,
        *,
        content_type="application/octet-stream",
    ):
        del content_type

        if key.endswith(".publication.json") and self.fail_next_descriptor:
            self.fail_next_descriptor = False
            raise B2TransportError("Simulated completion write failure")

        self.next_version += 1
        version = f"version-{self.next_version}"
        info = B2ObjectInfo(
            key,
            len(data),
            version,
            hashlib.sha256(data).hexdigest(),
        )
        self.versions[(key, version)] = (info, data)
        self.current[key] = version
        self.writes.append(key)
        return info

    def put_file(
        self,
        key,
        source_path,
        *,
        content_type="application/octet-stream",
    ):
        return self.put_bytes(
            key,
            Path(source_path).read_bytes(),
            content_type=content_type,
        )

    def head(self, key, *, version_id=None):
        version = version_id if version_id is not None else self.current.get(key)
        entry = self.versions.get((key, version))
        return None if entry is None else entry[0]

    def get_bytes(
        self,
        key,
        *,
        maximum_bytes,
        version_id=None,
        expected_sha256=None,
    ):
        version = version_id if version_id is not None else self.current.get(key)
        self.reads.append((key, version_id))
        entry = self.versions.get((key, version))
        if entry is None:
            return None

        info, data = entry
        actual = hashlib.sha256(data).hexdigest()

        if (
            len(data) > maximum_bytes
            or len(data) != info.size_bytes
            or (expected_sha256 is not None and actual != expected_sha256)
            or (info.transport_sha256 is not None and actual != info.transport_sha256)
        ):
            raise B2IntegrityError("Simulated object integrity failure")

        return B2DownloadedObject(info, data)

    def get_file(
        self,
        key,
        destination_path,
        *,
        owned_root,
        maximum_bytes,
        version_id=None,
        expected_sha256=None,
    ):
        destination = Path(destination_path)
        destination.relative_to(Path(owned_root))

        downloaded = self.get_bytes(
            key,
            maximum_bytes=maximum_bytes,
            version_id=version_id,
            expected_sha256=expected_sha256,
        )
        if downloaded is None:
            return None

        with destination.open("xb") as handle:
            handle.write(downloaded.data)

        return downloaded.info


def _artifact(*, source_digest="a" * 64):
    hour = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=hour,
    )
    block = build_trade_ohlc_hour(
        (),
        base="BTC",
        hour_utc=hour,
    )
    encoded = encode_hourly_trade_ohlc_block(block)
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(
            RemoteSourceHourReference(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument="BTCUSDT",
                data_kind=SourceDataKind.TRADES,
                hour_utc=hour,
                content_sha256=source_digest,
            ),
        ),
        content_sha256=encoded.content_sha256,
        producer_git_commit="b" * 40,
    )
    return RemotePriceProcessedArtifact(manifest, encoded)


def _publish(repository, artifact):
    return repository.publish_artifact(
        artifact,
        single_writer_confirmed=True,
    )


def test_publication_is_last_and_download_is_version_pinned():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()

    result = _publish(repository, artifact)

    assert result.created is True
    assert store.writes == [
        artifact.manifest.key.relative_path,
        artifact.manifest.key.manifest_relative_path,
        publication_relative_path(artifact.manifest.key),
    ]

    store.reads.clear()
    downloaded = repository.require_artifact(
        artifact.manifest.key,
        reference=result.reference,
    )

    assert downloaded.artifact == artifact
    assert all(version is not None for _key, version in store.reads)


def test_identical_publication_performs_no_additional_writes():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()

    first = _publish(repository, artifact)
    before = list(store.writes)
    second = _publish(repository, artifact)

    assert second.created is False
    assert second.reference == first.reference
    assert store.writes == before


def test_same_analytical_channels_with_different_provenance_conflict():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    first = _artifact()
    conflicting = _artifact(source_digest="c" * 64)

    assert first.encoded == conflicting.encoded
    _publish(repository, first)
    before = list(store.writes)

    with pytest.raises(B2ArtifactConflictError):
        _publish(repository, conflicting)

    assert store.writes == before


def test_publication_requires_explicit_single_writer_contract():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)

    with pytest.raises(B2RepositoryError, match="single-writer"):
        repository.publish_artifact(_artifact())

    assert store.writes == []


def test_failed_completion_write_is_not_importable_and_is_resumable():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()
    store.fail_next_descriptor = True

    with pytest.raises(B2TransportError):
        _publish(repository, artifact)

    assert repository.download_artifact(artifact.manifest.key) is None
    assert len(store.writes) == 2

    result = _publish(repository, artifact)

    assert result.created is True
    assert len(store.writes) == 3
    assert repository.require_artifact(artifact.manifest.key).artifact == artifact


@pytest.mark.parametrize("lone", ("artifact", "manifest"))
def test_compatible_lone_object_is_repaired_without_reupload(
    tmp_path,
    lone,
):
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()
    key = artifact.manifest.key

    if lone == "manifest":
        store.put_bytes(
            key.manifest_relative_path,
            artifact.manifest.canonical_json_bytes,
        )
        retained_key = key.manifest_relative_path
    else:
        path = tmp_path / "artifact.parquet"
        write_remote_artifact_file(path, artifact)
        store.put_file(key.relative_path, path)
        retained_key = key.relative_path

    retained_version = store.current[retained_key]
    result = _publish(repository, artifact)

    assert result.created is True
    assert store.current[retained_key] == retained_version
    assert len(store.writes) == 3


def test_conflicting_lone_manifest_is_not_overwritten():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()
    other = _artifact(source_digest="c" * 64)

    store.put_bytes(
        artifact.manifest.key.manifest_relative_path,
        other.manifest.canonical_json_bytes,
    )
    before = list(store.writes)

    with pytest.raises(B2ArtifactConflictError):
        _publish(repository, artifact)

    assert store.writes == before


def test_current_object_replacement_does_not_change_pinned_read():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()
    result = _publish(repository, artifact)

    store.put_bytes(artifact.manifest.key.relative_path, b"new current bytes")
    store.put_bytes(
        artifact.manifest.key.manifest_relative_path,
        b"new current manifest",
    )

    downloaded = repository.require_artifact(
        artifact.manifest.key,
        reference=result.reference,
    )
    assert downloaded.artifact == artifact


def test_deleted_pinned_version_does_not_fall_back_to_current():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()
    result = _publish(repository, artifact)
    info = result.reference.publication.artifact

    store.put_bytes(info.key, b"replacement")
    del store.versions[(info.key, info.version_id)]

    with pytest.raises(B2IncompletePublicationError):
        repository.require_artifact(
            artifact.manifest.key,
            reference=result.reference,
        )


def test_corrupt_pinned_bytes_fail_verification():
    store = MemoryStore()
    repository = B2ProcessedArtifactRepository(store)
    artifact = _artifact()
    result = _publish(repository, artifact)
    info = result.reference.publication.artifact
    _original_info, data = store.versions[(info.key, info.version_id)]

    changed = bytes([data[0] ^ 1]) + data[1:]
    store.versions[(info.key, info.version_id)] = (info, changed)

    with pytest.raises(B2IntegrityError):
        repository.require_artifact(
            artifact.manifest.key,
            reference=result.reference,
        )


def test_publication_round_trip_and_noncanonical_json_rejection():
    store = MemoryStore()
    result = _publish(
        B2ProcessedArtifactRepository(store),
        _artifact(),
    )
    publication = result.reference.publication

    assert (
        B2Publication.from_canonical_json_bytes(publication.canonical_json_bytes)
        == publication
    )

    noncanonical = json.dumps(
        publication.to_canonical_dict(),
        indent=2,
    ).encode("utf-8")

    with pytest.raises(B2PublicationError, match="canonical"):
        B2Publication.from_canonical_json_bytes(noncanonical)


@pytest.mark.parametrize("bad_size", (True, False, 0, -1, "10", 1.5))
def test_publication_rejects_invalid_object_sizes(bad_size):
    store = MemoryStore()
    result = _publish(
        B2ProcessedArtifactRepository(store),
        _artifact(),
    )
    payload = result.reference.publication.to_canonical_dict()
    payload["artifact"]["size_bytes"] = bad_size

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    with pytest.raises(B2PublicationError):
        B2Publication.from_canonical_json_bytes(encoded)


def test_publication_rejects_boolean_schema_version():
    store = MemoryStore()
    result = _publish(
        B2ProcessedArtifactRepository(store),
        _artifact(),
    )
    payload = result.reference.publication.to_canonical_dict()
    payload["schema_version"] = True

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    with pytest.raises(B2PublicationError, match="schema"):
        B2Publication.from_canonical_json_bytes(encoded)


def test_publication_rejects_duplicate_json_keys():
    with pytest.raises(B2PublicationError, match="duplicate"):
        B2Publication.from_canonical_json_bytes(b'{"schema":1,"schema":2}')


def test_repository_has_no_database_or_ui_import():
    import ast
    import l2shock.remote.b2_repository as module

    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)

    assert not any(
        name == "l2shock.db"
        or name.startswith("l2shock.db.")
        or name == "l2shock.ui"
        or name.startswith("l2shock.ui.")
        or name == "nicegui"
        for name in imports
    )


@pytest.mark.parametrize(
    "bad_size_kind",
    ("float", "string", "boolean"),
)
def test_completion_reference_rejects_noninteger_descriptor_size(
    bad_size_kind: str,
) -> None:
    from dataclasses import replace

    from l2shock.remote.b2_publication import B2PublicationReference

    store = MemoryStore()
    result = _publish(
        B2ProcessedArtifactRepository(store),
        _artifact(),
    )
    reference = result.reference
    expected_size = reference.descriptor.size_bytes

    if bad_size_kind == "float":
        bad_size = float(expected_size)
    elif bad_size_kind == "string":
        bad_size = str(expected_size)
    else:
        bad_size = True

    malformed = replace(
        reference.descriptor,
        size_bytes=bad_size,
    )

    with pytest.raises(B2PublicationError, match="size"):
        B2PublicationReference(
            publication=reference.publication,
            descriptor=malformed,
        )


def test_completion_reference_rejects_wrong_descriptor_size() -> None:
    from dataclasses import replace

    from l2shock.remote.b2_publication import B2PublicationReference

    result = _publish(
        B2ProcessedArtifactRepository(MemoryStore()),
        _artifact(),
    )
    malformed = replace(
        result.reference.descriptor,
        size_bytes=result.reference.descriptor.size_bytes + 1,
    )

    with pytest.raises(B2PublicationError, match="descriptor"):
        B2PublicationReference(
            publication=result.reference.publication,
            descriptor=malformed,
        )

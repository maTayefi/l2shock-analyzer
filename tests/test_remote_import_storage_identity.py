from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from l2shock.acquisition import SourceDataKind
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
    B2PublicationReference,
    publication_relative_path,
)
from l2shock.remote.b2_repository import (
    B2ArtifactNotFoundError,
    B2ProcessedArtifactRepository,
    DownloadedB2Artifact,
)
from l2shock.remote.b2_transport import B2ObjectInfo
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteSourceHourReference,
)
from l2shock.remote.hf_repository import DownloadedHuggingFaceArtifact
from l2shock.remote.import_contracts import (
    B2_STORAGE_BACKEND,
    HF_STORAGE_BACKEND,
    RemoteImportIdentityError,
    RemoteImportStorageIdentity,
    VerifiedRemoteImportArtifact,
)
from l2shock.remote.importer import (
    RemoteArtifactImportError,
    RemoteArtifactImportResult,
    download_and_import_b2_artifact,
)

_ENDPOINT = "https://s3.us-east-005.backblazeb2.com"
_BUCKET = "l2shock-test-storage"
_HOUR = datetime(2092, 1, 1, 12, tzinfo=timezone.utc)


def _artifact() -> RemotePriceProcessedArtifact:
    encoded = encode_hourly_trade_ohlc_block(
        build_trade_ohlc_hour(
            (),
            base="BTC",
            hour_utc=_HOUR,
        )
    )
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_HOUR,
    )
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(
            RemoteSourceHourReference(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument="BTCUSDT",
                data_kind=SourceDataKind.TRADES,
                hour_utc=_HOUR,
                content_sha256="a" * 64,
            ),
        ),
        content_sha256=encoded.content_sha256,
        producer_git_commit="b" * 40,
    )
    return RemotePriceProcessedArtifact(manifest, encoded)


def _downloaded_b2(tmp_path: Path) -> DownloadedB2Artifact:
    artifact = _artifact()
    path = tmp_path / "artifact.parquet"
    file_info = write_remote_artifact_file(path, artifact)
    key = artifact.manifest.key
    manifest_bytes = artifact.manifest.canonical_json_bytes

    publication = B2Publication(
        key=key,
        artifact=B2ObjectInfo(
            key=key.relative_path,
            size_bytes=file_info.file_size_bytes,
            version_id="artifact-version-1",
            transport_sha256=file_info.transport_sha256,
        ),
        manifest=B2ObjectInfo(
            key=key.manifest_relative_path,
            size_bytes=len(manifest_bytes),
            version_id="manifest-version-1",
            transport_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        ),
    )
    descriptor = B2ObjectInfo(
        key=publication_relative_path(key),
        size_bytes=len(publication.canonical_json_bytes),
        version_id="descriptor-version-1",
        transport_sha256=publication.publication_sha256,
    )
    return DownloadedB2Artifact(
        B2PublicationReference(publication, descriptor),
        artifact,
    )


def _identity(downloaded: DownloadedB2Artifact) -> RemoteImportStorageIdentity:
    return RemoteImportStorageIdentity(
        backend=B2_STORAGE_BACKEND,
        endpoint_url=_ENDPOINT,
        bucket=_BUCKET,
        publication_reference=downloaded.reference,
    )


def test_b2_identity_records_exact_versions_and_no_fake_revision(
    tmp_path: Path,
) -> None:
    downloaded = _downloaded_b2(tmp_path)
    verified = VerifiedRemoteImportArtifact.from_b2(
        downloaded,
        endpoint_url=_ENDPOINT,
        bucket=_BUCKET,
    )
    payload = verified.storage_identity.to_canonical_dict()

    assert verified.revision is None
    assert verified.artifact is downloaded.artifact
    assert payload["backend"] == B2_STORAGE_BACKEND
    assert payload["endpoint_url"] == _ENDPOINT
    assert payload["bucket"] == _BUCKET
    assert "revision" not in payload
    assert payload["descriptor"]["version_id"] == "descriptor-version-1"
    assert payload["publication"]["artifact"]["version_id"] == "artifact-version-1"
    assert payload["publication"]["manifest"]["version_id"] == "manifest-version-1"
    assert "application_key" not in payload
    assert "key_id" not in payload


def test_b2_identity_rejects_fabricated_hf_revision(tmp_path: Path) -> None:
    downloaded = _downloaded_b2(tmp_path)

    with pytest.raises(RemoteImportIdentityError, match="fabricate"):
        RemoteImportStorageIdentity(
            backend=B2_STORAGE_BACKEND,
            revision="c" * 40,
            endpoint_url=_ENDPOINT,
            bucket=_BUCKET,
            publication_reference=downloaded.reference,
        )


def test_hf_identity_rejects_b2_fields() -> None:
    with pytest.raises(RemoteImportIdentityError, match="B2"):
        RemoteImportStorageIdentity(
            backend=HF_STORAGE_BACKEND,
            revision="c" * 40,
            endpoint_url=_ENDPOINT,
        )


def test_unknown_backend_is_rejected() -> None:
    with pytest.raises(RemoteImportIdentityError, match="Unsupported"):
        RemoteImportStorageIdentity(backend="unknown")


def test_b2_result_requires_matching_key_and_structured_identity(
    tmp_path: Path,
) -> None:
    downloaded = _downloaded_b2(tmp_path)
    result = RemoteArtifactImportResult(
        revision=None,
        key=downloaded.artifact.manifest.key,
        analytical_inserted=True,
        preset_inserted=None,
        source_metadata_updated=True,
        imported_at_utc=_HOUR,
        storage_identity=_identity(downloaded),
    )

    assert result.revision is None

    with pytest.raises(RemoteArtifactImportError, match="fabricate"):
        replace(result, revision="c" * 40)

    other_key = replace(result.key, hour_utc=_HOUR + timedelta(hours=1))

    with pytest.raises(RemoteArtifactImportError, match="different artifact"):
        replace(result, key=other_key)

    with pytest.raises(RemoteArtifactImportError, match="commit SHA"):
        replace(result, storage_identity=None)


def test_existing_hf_result_constructor_remains_supported() -> None:
    result = RemoteArtifactImportResult(
        revision="c" * 40,
        key=_artifact().manifest.key,
        analytical_inserted=True,
        preset_inserted=None,
        source_metadata_updated=True,
        imported_at_utc=_HOUR,
    )

    assert result.revision == "c" * 40
    assert result.storage_identity is None


def test_hf_download_is_wrapped_without_changing_content(
    tmp_path: Path,
) -> None:
    artifact = _artifact()
    artifact_path = tmp_path / "hf-artifact.parquet"
    manifest_path = tmp_path / "hf-manifest.json"
    write_remote_artifact_file(artifact_path, artifact)
    manifest_path.write_bytes(artifact.manifest.canonical_json_bytes)

    downloaded = DownloadedHuggingFaceArtifact(
        revision="c" * 40,
        artifact_path=artifact_path,
        manifest_path=manifest_path,
        artifact=artifact,
    )
    verified = VerifiedRemoteImportArtifact.from_huggingface(downloaded)

    assert verified.artifact is artifact
    assert verified.revision == "c" * 40
    assert verified.storage_identity.backend == HF_STORAGE_BACKEND


class _Repository(B2ProcessedArtifactRepository):
    """No transport construction; records importer boundary calls."""

    def __init__(
        self,
        downloaded: DownloadedB2Artifact,
        events: list[str],
        *,
        missing: bool = False,
        fail_download: bool = False,
    ) -> None:
        self.downloaded = downloaded
        self.events = events
        self.missing = missing
        self.fail_download = fail_download

    @property
    def endpoint_url(self) -> str:
        return _ENDPOINT

    @property
    def bucket(self) -> str:
        return _BUCKET

    def resolve_publication(self, key, *, descriptor_version_id=None):
        assert key == self.downloaded.artifact.manifest.key
        assert descriptor_version_id is None
        self.events.append("resolve")
        return None if self.missing else self.downloaded.reference

    def require_artifact(self, key, *, reference=None):
        assert key == self.downloaded.artifact.manifest.key
        assert reference == self.downloaded.reference
        self.events.append("download")
        if self.fail_download:
            raise RemoteArtifactImportError("simulated verification failure")
        return self.downloaded


@pytest.mark.parametrize("explicit_reference", (False, True))
def test_b2_download_and_verification_precede_database_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    explicit_reference: bool,
) -> None:
    import l2shock.remote.importer as importer

    downloaded = _downloaded_b2(tmp_path)
    events: list[str] = []
    repository = _Repository(downloaded, events)
    session_marker = object()
    result_marker = object()

    @contextmanager
    def open_session():
        events.append("session")
        yield session_marker
        events.append("commit")

    def import_verified(session, verified, *, checkpoint_store=None):
        assert session is session_marker
        assert checkpoint_store is None
        assert isinstance(verified, VerifiedRemoteImportArtifact)
        assert verified.revision is None
        assert verified.storage_identity.publication_reference == downloaded.reference
        assert verified.storage_identity.endpoint_url == _ENDPOINT
        assert verified.storage_identity.bucket == _BUCKET
        events.append("import")
        return result_marker

    monkeypatch.setattr(
        importer,
        "import_verified_remote_artifact",
        import_verified,
    )

    result = download_and_import_b2_artifact(
        repository,
        downloaded.artifact.manifest.key,
        reference=downloaded.reference if explicit_reference else None,
        session_scope_factory=open_session,
    )

    assert result is result_marker
    expected = ["download", "session", "import", "commit"]
    if not explicit_reference:
        expected.insert(0, "resolve")
    assert events == expected


@pytest.mark.parametrize("failure", ("missing", "verification"))
def test_b2_failure_does_not_open_database_session(
    tmp_path: Path,
    failure: str,
) -> None:
    downloaded = _downloaded_b2(tmp_path)
    events: list[str] = []
    repository = _Repository(
        downloaded,
        events,
        missing=failure == "missing",
        fail_download=failure == "verification",
    )

    @contextmanager
    def forbidden_session():
        raise AssertionError("database session opened before verification")
        yield

    expected_error = (
        B2ArtifactNotFoundError if failure == "missing" else RemoteArtifactImportError
    )

    with pytest.raises(expected_error):
        download_and_import_b2_artifact(
            repository,
            downloaded.artifact.manifest.key,
            session_scope_factory=forbidden_session,
        )

    assert "session" not in events

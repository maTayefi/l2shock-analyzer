from __future__ import annotations

import asyncio
import hashlib
import threading
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import l2shock.ui.remote_import_runtime as runtime_module
from l2shock.config import B2Config
from l2shock.price import (
    build_trade_ohlc_hour,
    encode_hourly_trade_ohlc_block,
)
from l2shock.remote.artifact_codec import RemotePriceProcessedArtifact
from l2shock.remote.b2_publication import (
    B2Publication,
    B2PublicationReference,
    publication_relative_path,
)
from l2shock.remote.b2_repository import (
    B2IncompletePublicationError,
    DownloadedB2Artifact,
)
from l2shock.remote.b2_transport import B2ObjectInfo
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteSourceHourReference,
)
from l2shock.remote.import_contracts import (
    B2_STORAGE_BACKEND,
    RemoteImportStorageIdentity,
    VerifiedRemoteImportArtifact,
)
from l2shock.remote.importer import RemoteArtifactImportResult
from l2shock.acquisition import SourceDataKind
from l2shock.timeutils import now_utc
from l2shock.ui.remote_import_runtime import (
    B2RangeImportRepository,
    RemoteImportItemDisposition,
    RemoteImportRuntime,
)
from l2shock.ui.state import get_state, reset_state_for_tests


def _hour(value=12):
    return datetime(2026, 10, 1, value, tzinfo=timezone.utc)


def _settings():
    return B2Config(
        endpoint_url="https://s3.us-west-004.backblazeb2.com",
        bucket="l2shock-test-storage",
        key_id="test-key-id",
        application_key="test-application-key",
    )


def _verified_price():
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
    )
    encoded = encode_hourly_trade_ohlc_block(
        build_trade_ohlc_hour((), base="BTC", hour_utc=_hour())
    )
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(
            RemoteSourceHourReference(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument="BTCUSDT",
                data_kind=SourceDataKind.TRADES,
                hour_utc=_hour(),
                content_sha256="a" * 64,
            ),
        ),
        content_sha256=encoded.content_sha256,
        producer_git_commit="b" * 40,
    )
    artifact = RemotePriceProcessedArtifact(manifest, encoded)

    publication = B2Publication(
        key=key,
        artifact=B2ObjectInfo(
            key.relative_path,
            10,
            "artifact-version",
            "c" * 64,
        ),
        manifest=B2ObjectInfo(
            key.manifest_relative_path,
            len(manifest.canonical_json_bytes),
            "manifest-version",
            hashlib.sha256(manifest.canonical_json_bytes).hexdigest(),
        ),
    )
    descriptor_bytes = publication.canonical_json_bytes
    reference = B2PublicationReference(
        publication=publication,
        descriptor=B2ObjectInfo(
            publication_relative_path(key),
            len(descriptor_bytes),
            "descriptor-version",
            hashlib.sha256(descriptor_bytes).hexdigest(),
        ),
    )
    downloaded = DownloadedB2Artifact(reference, artifact)

    return VerifiedRemoteImportArtifact.from_b2(
        downloaded,
        endpoint_url=_settings().endpoint_url,
        bucket=_settings().bucket,
    )


def _start(runtime):
    return runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )


def test_b2_adapter_has_no_synthetic_revision_or_secret_repr():
    adapter = B2RangeImportRepository(_settings())

    assert adapter.current_revision() is None
    assert "test-key-id" not in repr(adapter)
    assert "test-application-key" not in repr(adapter)

    with pytest.raises(
        runtime_module.RemoteImportRuntimeError,
        match="HF revision",
    ):
        adapter.download_artifact(
            _verified_price().artifact.manifest.key,
            revision="a" * 40,
        )


@pytest.mark.parametrize("outcome", ("present", "missing", "broken"))
def test_adapter_pins_reference_and_closes_transport(
    monkeypatch,
    outcome,
):
    verified = _verified_price()
    reference = verified.storage_identity.publication_reference
    events = []

    class Store:
        def __enter__(self):
            events.append("open")
            return self

        def __exit__(self, *_args):
            events.append("close")

    class Repository:
        def __init__(self, store):
            assert isinstance(store, Store)
            self.endpoint_url = _settings().endpoint_url
            self.bucket = _settings().bucket

        def resolve_publication(self, key):
            assert key == verified.artifact.manifest.key
            events.append("resolve")
            return None if outcome == "missing" else reference

        def require_artifact(self, key, *, reference):
            assert key == verified.artifact.manifest.key
            assert reference is verified.storage_identity.publication_reference
            events.append("download")

            if outcome == "broken":
                raise B2IncompletePublicationError(
                    "Pinned artifact version is unavailable"
                )

            return DownloadedB2Artifact(reference, verified.artifact)

    monkeypatch.setattr(
        runtime_module,
        "B2ProcessedArtifactRepository",
        Repository,
    )
    adapter = B2RangeImportRepository(
        _settings(),
        store_factory=lambda _settings: Store(),
    )

    if outcome == "broken":
        with pytest.raises(B2IncompletePublicationError):
            adapter.download_artifact(verified.artifact.manifest.key)
    else:
        result = adapter.download_artifact(verified.artifact.manifest.key)

        if outcome == "missing":
            assert result is None
        else:
            assert result == verified

    assert events[0] == "open"
    assert events[-1] == "close"
    assert events.count("resolve") == 1


@pytest.mark.asyncio
async def test_b2_range_retains_exact_identity_and_reports_missing(
    monkeypatch,
):
    reset_state_for_tests()
    verified = _verified_price()
    imported_downloads = []
    adapter = B2RangeImportRepository(_settings())

    def download(key, *, revision=None):
        assert revision is None

        if key.kind is RemoteArtifactKind.PRICE:
            return verified

        return None

    def importer(downloaded):
        imported_downloads.append(downloaded)
        return RemoteArtifactImportResult(
            revision=None,
            key=downloaded.artifact.manifest.key,
            analytical_inserted=True,
            preset_inserted=None,
            source_metadata_updated=True,
            imported_at_utc=now_utc(),
            storage_identity=downloaded.storage_identity,
        )

    monkeypatch.setattr(adapter, "download_artifact", download)
    runtime = RemoteImportRuntime(
        repository=adapter,
        artifact_importer=importer,
    )

    task = _start(runtime)
    assert get_state().active_operation_name == "remote_b2_import"

    result = await task
    await asyncio.sleep(0)

    assert result.storage_backend == B2_STORAGE_BACKEND
    assert result.pinned_revision is None
    assert result.status == "partial_ok"
    assert result.artifacts_selected == 4
    assert result.imported_count == 1
    assert result.missing_count == 3
    assert result.failed_count == 0
    assert imported_downloads == [verified]

    item = result.items[-1]
    assert item.disposition is RemoteImportItemDisposition.IMPORTED
    assert item.storage_identity == verified.storage_identity
    assert item.storage_identity.revision is None
    assert (
        item.storage_identity.publication_reference.descriptor.version_id
        == "descriptor-version"
    )

    snapshot = runtime.snapshot()
    assert snapshot.storage_backend == B2_STORAGE_BACKEND
    assert snapshot.latest_progress.storage_backend == B2_STORAGE_BACKEND
    assert snapshot.pinned_revision is None
    assert get_state().active_operation_name == ""
    assert get_state().tracked_tasks == set()
    assert get_state().operation_lock.locked() is False


@pytest.mark.asyncio
async def test_b2_download_failure_is_error_not_missing(monkeypatch):
    reset_state_for_tests()
    adapter = B2RangeImportRepository(_settings())
    importer_calls = []

    def download(key, *, revision=None):
        del key, revision
        raise B2IncompletePublicationError("Pinned artifact version is unavailable")

    monkeypatch.setattr(adapter, "download_artifact", download)
    runtime = RemoteImportRuntime(
        repository=adapter,
        artifact_importer=lambda item: importer_calls.append(item),
    )

    result = await _start(runtime)

    assert result.failed_count == 4
    assert result.missing_count == 0
    assert importer_calls == []
    assert all(
        "Pinned artifact version is unavailable" in item.diagnostic
        for item in result.items
    )


@pytest.mark.asyncio
async def test_b2_import_result_cannot_change_storage_location(monkeypatch):
    reset_state_for_tests()
    verified = _verified_price()
    adapter = B2RangeImportRepository(_settings())

    monkeypatch.setattr(
        adapter,
        "download_artifact",
        lambda key, *, revision=None: (
            verified if key.kind is RemoteArtifactKind.PRICE else None
        ),
    )

    def importer(downloaded):
        identity = downloaded.storage_identity
        wrong_identity = RemoteImportStorageIdentity(
            backend=B2_STORAGE_BACKEND,
            endpoint_url=identity.endpoint_url,
            bucket="another-test-bucket",
            publication_reference=identity.publication_reference,
        )
        return RemoteArtifactImportResult(
            revision=None,
            key=downloaded.artifact.manifest.key,
            analytical_inserted=True,
            preset_inserted=None,
            source_metadata_updated=True,
            imported_at_utc=now_utc(),
            storage_identity=wrong_identity,
        )

    runtime = RemoteImportRuntime(
        repository=adapter,
        artifact_importer=importer,
    )
    result = await _start(runtime)

    assert result.imported_count == 0
    assert result.failed_count == 1
    assert result.missing_count == 3
    assert "storage ownership changed" in result.items[-1].diagnostic
    assert result.items[-1].storage_identity == verified.storage_identity


@pytest.mark.asyncio
async def test_b2_repeated_cancellation_holds_lock_until_download_exits(
    monkeypatch,
):
    reset_state_for_tests()
    state = get_state()
    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    importer_calls = []
    adapter = B2RangeImportRepository(_settings())

    def download(key, *, revision=None):
        del key, revision
        entered.set()

        try:
            if not release.wait(timeout=5.0):
                raise TimeoutError("Test did not release the download")
            return None
        finally:
            exited.set()

    monkeypatch.setattr(adapter, "download_artifact", download)
    runtime = RemoteImportRuntime(
        repository=adapter,
        artifact_importer=lambda item: importer_calls.append(item),
    )
    task = _start(runtime)

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert not task.done()
        assert not exited.is_set()
        assert state.active_operation_name == "remote_b2_import"
        assert state.operation_lock.locked() is True
        assert task in state.tracked_tasks
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(task), timeout=3.0)

    await asyncio.sleep(0)

    assert exited.is_set()
    assert importer_calls == []
    assert state.operation_lock.locked() is False
    assert state.active_operation_name == ""
    assert state.tracked_tasks == set()


@pytest.mark.asyncio
async def test_b2_prestart_cancellation_releases_admission():
    reset_state_for_tests()
    runtime = RemoteImportRuntime(
        repository=B2RangeImportRepository(_settings()),
    )
    task = _start(runtime)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0)

    assert runtime.task is None
    assert runtime.snapshot().completion_sequence == 1
    assert get_state().active_operation_name == ""
    assert get_state().tracked_tasks == set()

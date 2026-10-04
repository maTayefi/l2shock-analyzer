"""B2 worker pinning, predecessor, frontier, ownership, and lifecycle tests."""

from __future__ import annotations

import asyncio
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import l2shock.remote_worker as worker
from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
    sha256_file,
)
from l2shock.config import CryptoHFTConfig
from l2shock.presets import build_okx_futures_data_preset
from l2shock.processing import ProcessingSourceArchive
from l2shock.remote.b2_publication import publication_relative_path
from l2shock.remote.b2_repository import (
    B2IncompletePublicationError,
    B2RepositoryError,
)
from l2shock.remote.b2_transport import (
    B2DownloadedObject,
    B2IntegrityError,
    B2ObjectInfo,
    B2ObjectStore,
)
from l2shock.remote.b2_worker_repository import B2WorkerRepository
from l2shock.remote.contracts import RemoteArtifactKey, RemoteArtifactKind
from l2shock.remote.headless_processing import process_l2_archive_headlessly
from l2shock.remote.import_contracts import B2_STORAGE_BACKEND
from l2shock.remote.source_acquisition import build_remote_worker_workspace


def _hour(offset: int = 0) -> datetime:
    return datetime(2026, 10, 1, 12, tzinfo=timezone.utc) + timedelta(hours=offset)


def _preset():
    return build_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )


class MemoryStore(B2ObjectStore):
    """Version-retaining transport double; no SDK initialization."""

    def __init__(self) -> None:
        self._endpoint_url = "https://s3.us-east-005.backblazeb2.com"
        self._bucket = "l2shock-test-storage"
        self._closed = False
        self.versions = {}
        self.current = {}
        self.writes = []
        self.reads = []
        self.next_version = 0

    def close(self) -> None:
        self._closed = True

    def put_bytes(
        self,
        key,
        data,
        *,
        content_type="application/octet-stream",
    ):
        del content_type
        self._ensure_open()
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
        self._ensure_open()
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
        self._ensure_open()
        self.reads.append((key, version_id))
        version = version_id if version_id is not None else self.current.get(key)
        entry = self.versions.get((key, version))

        if entry is None:
            return None

        info, data = entry
        actual = hashlib.sha256(data).hexdigest()

        if (
            len(data) > maximum_bytes
            or len(data) != info.size_bytes
            or actual != info.transport_sha256
            or (expected_sha256 is not None and actual != expected_sha256)
        ):
            raise B2IntegrityError("Memory object failed integrity verification")

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


def _artifact(tmp_path: Path, *, offset: int = 0, locked: bool = False):
    hour = _hour(offset)
    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue="okx_futures",
        symbol="BTC-USDT-SWAP",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour,
    )
    path = spec.local_path(tmp_path / "raw")
    path.parent.mkdir(parents=True, exist_ok=True)

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    received = int((hour - epoch).total_seconds()) * 1_000_000_000 + 100_000_000

    common = {
        "received_time": received,
        "event_time": received // 1_000_000,
        "transaction_time": None,
        "symbol": spec.symbol,
        "event_type": "snapshot",
        "first_update_id": None,
        "final_update_id": 100,
        "prev_final_update_id": None,
        "last_update_id": 100,
        "quantity": "2",
        "order_count": None,
    }

    rows = [
        {**common, "side": "bid", "price": "100"},
        {**common, "side": "ask", "price": "100" if locked else "101"},
    ]
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")

    digest, size = sha256_file(path)
    archive = ProcessingSourceArchive(
        spec=spec,
        local_path=path,
        content_sha256=digest,
        file_size_bytes=size,
        status=SourceHourStatus.DOWNLOADED,
    )
    output = process_l2_archive_headlessly(
        archive,
        _preset(),
        producer_git_commit="a" * 40,
        batch_size=1,
    )
    return worker._l2_artifact_for_publication(output)


def _repository(store, *, writer: bool = True):
    return B2WorkerRepository(
        store,
        preset=_preset(),
        single_writer_confirmed=writer,
    )


def _target_key(offset: int) -> RemoteArtifactKey:
    return RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        hour_utc=_hour(offset),
        preset_hash=_preset().preset_hash,
    )


def test_worker_repository_has_no_revision_or_construction_network():
    store = MemoryStore()
    repository = _repository(store)

    assert repository.current_revision() is None
    assert store.reads == []
    assert store.writes == []
    assert repository.observed_storage_identities() == ()


def test_missing_descriptor_is_pinned_until_next_inspection(tmp_path):
    store = MemoryStore()
    reader = _repository(store, writer=False)
    publisher = _repository(store)
    artifact = _artifact(tmp_path)

    reader.current_revision()
    assert reader.download_artifact(artifact.manifest.key) is None
    publisher.publish_artifact(artifact)

    # Absence remains the observation for this generation.
    assert reader.download_artifact(artifact.manifest.key) is None

    reader.current_revision()
    assert reader.download_artifact(artifact.manifest.key).artifact == artifact


def test_current_replacement_does_not_change_worker_pinned_read(tmp_path):
    store = MemoryStore()
    repository = _repository(store)
    artifact = _artifact(tmp_path)
    publication = repository.publish_artifact(artifact)

    repository.current_revision()
    first = repository.download_artifact(artifact.manifest.key)
    reference = first.storage_identity.publication_reference

    store.put_bytes(artifact.manifest.key.relative_path, b"new current artifact")
    store.put_bytes(
        artifact.manifest.key.manifest_relative_path,
        b"new current manifest",
    )
    store.put_bytes(
        publication_relative_path(artifact.manifest.key),
        b"new invalid current descriptor",
    )

    again = repository.download_artifact(artifact.manifest.key)

    assert again.artifact == artifact
    assert again.storage_identity.publication_reference == reference
    assert reference == publication.reference

    repository.current_revision()

    with pytest.raises(B2RepositoryError):
        repository.download_artifact(artifact.manifest.key)


def test_deleted_pinned_version_is_error_not_current_fallback(tmp_path):
    store = MemoryStore()
    repository = _repository(store)
    artifact = _artifact(tmp_path)
    repository.publish_artifact(artifact)

    repository.current_revision()
    downloaded = repository.download_artifact(artifact.manifest.key)
    reference = downloaded.storage_identity.publication_reference
    info = reference.publication.artifact

    store.put_bytes(info.key, b"replacement current bytes")
    del store.versions[(info.key, info.version_id)]

    with pytest.raises(B2IncompletePublicationError):
        repository.download_artifact(artifact.manifest.key)


def test_predecessor_is_exact_immediately_preceding_publication(tmp_path):
    store = MemoryStore()
    repository = _repository(store)
    artifact = _artifact(tmp_path)
    published = repository.publish_artifact(artifact)

    repository.current_revision()
    predecessor = repository.download_l2_predecessor_checkpoint(_target_key(1))

    assert predecessor.checkpoint_bytes == artifact.output_checkpoint
    assert predecessor.predecessor.storage_identity.revision is None
    assert (
        predecessor.predecessor.storage_identity.publication_reference
        == published.reference
    )


def test_wrong_chain_preset_and_revision_fail_before_network():
    store = MemoryStore()
    repository = _repository(store)

    wrong_preset = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        hour_utc=_hour(),
        preset_hash="b" * 64,
    )
    wrong_chain = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="bybit",
        instrument="BTCUSDT",
        hour_utc=_hour(),
        preset_hash="b" * 64,
    )

    for key in (wrong_preset, wrong_chain):
        with pytest.raises(B2RepositoryError):
            repository.download_artifact(key)

    with pytest.raises(B2RepositoryError, match="HF revision"):
        repository.download_artifact(_target_key(0), revision="a" * 40)

    assert store.reads == []


def test_publication_requires_external_writer_acknowledgement(tmp_path):
    store = MemoryStore()
    repository = _repository(store, writer=False)

    with pytest.raises(B2RepositoryError, match="single-writer"):
        repository.publish_artifact(_artifact(tmp_path))

    assert store.writes == []


@pytest.mark.asyncio
async def test_shared_frontier_uses_b2_and_skips_checkpointless_marker(tmp_path):
    store = MemoryStore()
    repository = _repository(store)
    seed = _artifact(tmp_path, offset=0)
    blocked = _artifact(tmp_path, offset=1, locked=True)

    assert seed.output_checkpoint is not None
    assert blocked.output_checkpoint is None

    repository.publish_artifact(seed)
    repository.publish_artifact(blocked)

    selected = await worker.select_remote_catch_up_hour(
        repository=repository,
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        latest_eligible_hour_utc=_hour(2),
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
        search_hours=3,
    )

    assert selected == _hour(2)


@pytest.mark.asyncio
async def test_shared_worker_reuses_b2_without_source_acquisition(
    tmp_path,
    monkeypatch,
):
    store = MemoryStore()
    repository = _repository(store)
    artifact = _artifact(tmp_path)
    published = repository.publish_artifact(artifact)

    async def forbidden_acquisition(*_args, **_kwargs):
        raise AssertionError("Existing verified target must not acquire raw sources")

    monkeypatch.setattr(
        worker,
        "acquire_remote_worker_archives",
        forbidden_acquisition,
    )

    result = await worker.process_remote_hour(
        repository=repository,
        cryptohft=CryptoHFTConfig(),
        workspace=build_remote_worker_workspace(tmp_path / "workspace"),
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        hour_utc=_hour(),
        latest_eligible_hour_utc=_hour(),
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
        producer_git_commit="a" * 40,
    )

    assert result.storage_backend == B2_STORAGE_BACKEND
    assert result.pinned_input_revision is None
    assert result.l2_revision is None
    assert result.price_revision is None
    assert result.l2_reused is True
    assert result.source_downloaded_count == 0
    assert len(result.b2_publications) == 1
    assert result.b2_publications[0].publication_reference == published.reference

    payload = result.to_dict()
    assert payload["schema_version"] == 2
    assert payload["storage_backend"] == B2_STORAGE_BACKEND
    assert payload["b2_publications"][0]["bucket"] == store.bucket


@pytest.mark.asyncio
async def test_broken_b2_publication_never_reaches_source_acquisition(
    tmp_path,
    monkeypatch,
):
    store = MemoryStore()
    repository = _repository(store)
    artifact = _artifact(tmp_path)
    published = repository.publish_artifact(artifact)
    info = published.reference.publication.artifact
    del store.versions[(info.key, info.version_id)]

    async def forbidden_acquisition(*_args, **_kwargs):
        raise AssertionError("Broken publication must not become missing source work")

    monkeypatch.setattr(
        worker,
        "acquire_remote_worker_archives",
        forbidden_acquisition,
    )

    with pytest.raises(B2IncompletePublicationError):
        await worker.process_remote_hour(
            repository=repository,
            cryptohft=CryptoHFTConfig(),
            workspace=build_remote_worker_workspace(tmp_path / "workspace"),
            venue="okx_futures",
            instrument="BTC-USDT-SWAP",
            hour_utc=_hour(),
            latest_eligible_hour_utc=_hour(),
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
            producer_git_commit=None,
        )


def test_parser_keeps_hf_default_and_accepts_explicit_b2():
    common = [
        "--venue",
        "okx_futures",
        "--instrument",
        "BTC-USDT-SWAP",
        "--depth-lower",
        "0",
        "--depth-upper",
        "0.01",
    ]

    assert worker.build_parser().parse_args(common).storage_backend == "hugging_face"

    args = worker.build_parser().parse_args(
        common
        + [
            "--storage-backend",
            "backblaze_b2",
            "--b2-endpoint-url",
            "https://s3.us-east-005.backblazeb2.com",
            "--b2-bucket",
            "l2shock-test-storage",
            "--b2-single-writer-confirmed",
        ]
    )

    assert args.storage_backend == B2_STORAGE_BACKEND
    assert args.b2_single_writer_confirmed is True


@pytest.mark.asyncio
async def test_b2_cli_closes_store_without_hf_credentials(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setenv("L2SHOCK__REMOTE__B2__KEY_ID", "test-key-id")
    monkeypatch.setenv("L2SHOCK__REMOTE__B2__APPLICATION_KEY", "test-key")
    store = MemoryStore()
    sentinel = object()

    monkeypatch.setattr(worker, "B2ObjectStore", lambda _settings: store)

    async def processing(**arguments):
        assert isinstance(arguments["repository"], B2WorkerRepository)
        assert store._closed is False
        assert arguments["workspace"].root.is_dir()
        return sentinel

    monkeypatch.setattr(worker, "process_remote_hour", processing)

    args = worker.build_parser().parse_args(
        [
            "--venue",
            "okx_futures",
            "--instrument",
            "BTC-USDT-SWAP",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
            "--hour",
            "2026-10-01T12:00:00Z",
            "--storage-backend",
            "backblaze_b2",
            "--b2-endpoint-url",
            store.endpoint_url,
            "--b2-bucket",
            store.bucket,
            "--b2-single-writer-confirmed",
            "--workspace-parent",
            str(tmp_path / "workspaces"),
        ]
    )

    assert await worker._run_from_arguments(args) is sentinel
    assert store._closed is True


@pytest.mark.asyncio
async def test_b2_cli_refuses_unacknowledged_writer_before_client(
    monkeypatch,
):
    def forbidden_store(_settings):
        raise AssertionError("Client must not be constructed")

    monkeypatch.setattr(worker, "B2ObjectStore", forbidden_store)

    args = worker.build_parser().parse_args(
        [
            "--venue",
            "okx_futures",
            "--instrument",
            "BTC-USDT-SWAP",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
            "--storage-backend",
            "backblaze_b2",
        ]
    )

    with pytest.raises(worker.RemoteWorkerError, match="single-writer"):
        await worker._run_from_arguments(args)


@pytest.mark.asyncio
async def test_joined_b2_work_outlives_repeated_owner_cancellation():
    store = MemoryStore()
    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()

    def blocking_operation():
        entered.set()
        try:
            if not release.wait(timeout=5.0):
                raise TimeoutError("Test did not release worker")
            assert store._closed is False
        finally:
            exited.set()

    async def owner():
        with store:
            await worker._to_thread_joined(blocking_operation)

    task = asyncio.create_task(owner())

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert not task.done()
        assert not exited.is_set()
        assert store._closed is False
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(task), timeout=3.0)

    assert exited.is_set()
    assert store._closed is True


# Batch 5B-A: exercise newly acquired hours through the real headless engine.


def _gate_chain_preset(venue: str, instrument: str):
    from l2shock.presets import (
        build_binance_futures_data_preset,
        build_bybit_data_preset,
        build_okx_futures_data_preset,
    )

    base = "BTC" if instrument.startswith("BTC") else "ETH"
    builders = {
        "binance_futures": build_binance_futures_data_preset,
        "bybit": build_bybit_data_preset,
        "okx_futures": build_okx_futures_data_preset,
    }
    return builders[venue](
        base=base,
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )


def _gate_source_archive(
    spec: SourceFileSpec,
    raw_root: Path,
    *,
    book_mode: str = "snapshot",
    locked: bool = False,
) -> ProcessingSourceArchive:
    """Create small real archives, not fabricated headless output."""
    path = spec.local_path(raw_root)
    path.parent.mkdir(parents=True, exist_ok=True)

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    received = (
        (spec.hour_utc - epoch) // timedelta(seconds=1)
    ) * 1_000_000_000 + 100_000_000
    milliseconds = received // 1_000_000

    if spec.data_kind is SourceDataKind.TRADES:
        rows = [
            {
                "received_time": received,
                "event_time": milliseconds,
                "symbol": spec.symbol,
                "trade_id": "batch-5b-trade",
                "price": "100.25",
                "quantity": "1",
                "trade_time": milliseconds,
                "is_buyer_maker": False,
                "order_type": "market",
            }
        ]
    else:
        common = {
            "received_time": received,
            "event_time": milliseconds,
            "transaction_time": milliseconds,
            "symbol": spec.symbol,
            "first_update_id": None,
            "prev_final_update_id": None,
            "quantity": "2",
            "order_count": None,
        }

        if book_mode in {"snapshot", "boundary"}:
            snapshot = {
                **common,
                "event_type": "snapshot",
                "final_update_id": (None if book_mode == "boundary" else 100),
                "last_update_id": 100,
            }
            rows = [
                {**snapshot, "side": "bid", "price": "100"},
                {
                    **snapshot,
                    "side": "ask",
                    "price": "100" if locked else "101",
                },
            ]
        elif book_mode == "update":
            rows = []
        else:
            raise AssertionError(f"Unsupported test book mode: {book_mode}")

        if book_mode in {"update", "boundary"}:
            update_received = received + (100_000_000 if book_mode == "boundary" else 0)
            update = {
                **common,
                "received_time": update_received,
                "event_time": update_received // 1_000_000,
                "transaction_time": update_received // 1_000_000,
                "event_type": "update",
                "final_update_id": 101,
                "last_update_id": (100 if spec.venue == "okx_futures" else 101),
                "side": "bid",
                "price": "100",
                "quantity": "3",
            }

            if spec.venue == "binance_futures":
                update["first_update_id"] = 101
                update["prev_final_update_id"] = 100
                update["last_update_id"] = None

            rows.append(update)

    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
    )
    digest, size = sha256_file(path)

    return ProcessingSourceArchive(
        spec=spec,
        local_path=path,
        content_sha256=digest,
        file_size_bytes=size,
        status=SourceHourStatus.DOWNLOADED,
    )


def _gate_install_acquisition(
    monkeypatch: pytest.MonkeyPatch,
    *,
    book_mode: str = "snapshot",
    locked: bool = False,
):
    from types import SimpleNamespace

    observed: list[tuple[SourceFileSpec, ...]] = []

    async def acquire(specs, *, workspace, **kwargs):
        assert kwargs["latest_eligible_hour_utc"] >= specs[0].hour_utc
        selected = tuple(specs)
        observed.append(selected)

        archives = tuple(
            _gate_source_archive(
                spec,
                workspace.storage.raw_path,
                book_mode=book_mode,
                locked=locked,
            )
            for spec in selected
        )

        # The downloader boundary is substituted; the consumed archives are
        # real typed archives and headless processing is not mocked.
        return SimpleNamespace(
            source_count=len(archives),
            downloaded_count=len(archives),
            reused_count=0,
            processing_archives=archives,
        )

    monkeypatch.setattr(
        worker,
        "acquire_remote_worker_archives",
        acquire,
    )
    return observed


async def _gate_process(
    repository,
    tmp_path: Path,
    *,
    venue: str,
    instrument: str,
    offset: int = 0,
):
    return await worker.process_remote_hour(
        repository=repository,
        cryptohft=CryptoHFTConfig(),
        workspace=build_remote_worker_workspace(tmp_path / "workspace"),
        venue=venue,
        instrument=instrument,
        hour_utc=_hour(offset),
        latest_eligible_hour_utc=_hour(offset),
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
        producer_git_commit="a" * 40,
        batch_size=1,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("venue", "instrument"),
    (
        ("binance_futures", "BTCUSDT"),
        ("binance_futures", "ETHUSDT"),
        ("bybit", "BTCUSDT"),
        ("bybit", "ETHUSDT"),
        ("okx_futures", "BTC-USDT-SWAP"),
        ("okx_futures", "ETH-USDT-SWAP"),
    ),
)
async def test_b2_new_hour_processes_and_publishes_real_channels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    venue: str,
    instrument: str,
) -> None:
    from l2shock.ingest import decode_checkpoint

    store = MemoryStore()
    preset = _gate_chain_preset(venue, instrument)
    repository = B2WorkerRepository(
        store,
        preset=preset,
        single_writer_confirmed=True,
    )
    requests = _gate_install_acquisition(monkeypatch)

    result = await _gate_process(
        repository,
        tmp_path,
        venue=venue,
        instrument=instrument,
    )

    expected_sources = 2 if venue == "binance_futures" else 1
    assert len(requests) == 1
    assert len(requests[0]) == expected_sources
    assert result.source_downloaded_count == expected_sources
    assert result.source_reused_count == 0
    assert result.l2_created is True
    assert result.l2_reused is False
    assert result.storage_backend == B2_STORAGE_BACKEND
    assert result.pinned_input_revision is None
    assert result.l2_revision is None
    assert result.price_revision is None

    l2_key = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )
    downloaded = repository.download_artifact(l2_key)
    assert downloaded is not None
    assert downloaded.artifact.output_checkpoint is not None

    checkpoint = decode_checkpoint(downloaded.artifact.output_checkpoint)
    assert checkpoint.venue == venue
    assert checkpoint.symbol == instrument
    assert checkpoint.through_hour_utc == _hour()
    assert checkpoint.last_update_id == 100

    assert store.writes[:3] == [
        l2_key.relative_path,
        l2_key.manifest_relative_path,
        publication_relative_path(l2_key),
    ]

    if venue == "binance_futures":
        assert result.price_required is True
        assert result.price_created is True
        assert len(store.writes) == 6
        assert len(result.b2_publications) == 2
    else:
        assert result.price_required is False
        assert result.price_created is False
        assert len(store.writes) == 3
        assert len(result.b2_publications) == 1

    for identity in result.b2_publications:
        assert identity.endpoint_url == store.endpoint_url
        assert identity.bucket == store.bucket
        assert identity.revision is None
        assert identity.publication_reference is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("venue", "instrument", "mode"),
    (
        ("binance_futures", "BTCUSDT", "update"),
        ("bybit", "BTCUSDT", "boundary"),
        ("okx_futures", "BTC-USDT-SWAP", "update"),
    ),
)
async def test_b2_new_hour_carries_exact_predecessor_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    venue: str,
    instrument: str,
    mode: str,
) -> None:
    from l2shock.ingest import decode_checkpoint

    store = MemoryStore()
    preset = _gate_chain_preset(venue, instrument)
    repository = B2WorkerRepository(
        store,
        preset=preset,
        single_writer_confirmed=True,
    )

    predecessor_spec = SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=instrument,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )
    predecessor_archive = _gate_source_archive(
        predecessor_spec,
        tmp_path / "seed-raw",
    )
    predecessor_output = process_l2_archive_headlessly(
        predecessor_archive,
        preset,
        producer_git_commit="a" * 40,
        batch_size=1,
    )
    predecessor = worker._l2_artifact_for_publication(predecessor_output)
    seed_publication = repository.publish_artifact(predecessor)

    _gate_install_acquisition(monkeypatch, book_mode=mode)
    result = await _gate_process(
        repository,
        tmp_path,
        venue=venue,
        instrument=instrument,
        offset=1,
    )

    target_key = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
        hour_utc=_hour(1),
        preset_hash=preset.preset_hash,
    )
    downloaded = repository.download_artifact(target_key)
    assert downloaded is not None

    artifact = downloaded.artifact
    assert artifact.output_checkpoint is not None
    assert (
        artifact.manifest.input_checkpoint_content_sha256
        == predecessor.manifest.output_checkpoint_content_sha256
    )
    checkpoint = decode_checkpoint(artifact.output_checkpoint)
    assert checkpoint.through_hour_utc == _hour(1)
    assert checkpoint.last_update_id == 101

    assert result.l2_created is True
    assert any(
        identity.publication_reference == seed_publication.reference
        for identity in result.b2_publications
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("venue", "instrument", "mode"),
    (
        ("binance_futures", "BTCUSDT", "update"),
        ("bybit", "BTCUSDT", "boundary"),
    ),
)
async def test_b2_uninitialized_new_hour_does_not_occupy_artifact_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    venue: str,
    instrument: str,
    mode: str,
) -> None:
    store = MemoryStore()
    repository = B2WorkerRepository(
        store,
        preset=_gate_chain_preset(venue, instrument),
        single_writer_confirmed=True,
    )
    _gate_install_acquisition(monkeypatch, book_mode=mode)

    with pytest.raises(worker.RemoteWorkerCheckpointBlockedError):
        await _gate_process(
            repository,
            tmp_path,
            venue=venue,
            instrument=instrument,
        )

    assert store.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("venue", "instrument"),
    (
        ("binance_futures", "BTCUSDT"),
        ("bybit", "BTCUSDT"),
        ("okx_futures", "BTC-USDT-SWAP"),
    ),
)
async def test_b2_all_invalid_new_hour_publishes_no_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    venue: str,
    instrument: str,
) -> None:
    store = MemoryStore()
    preset = _gate_chain_preset(venue, instrument)
    repository = B2WorkerRepository(
        store,
        preset=preset,
        single_writer_confirmed=True,
    )
    _gate_install_acquisition(monkeypatch, locked=True)

    result = await _gate_process(
        repository,
        tmp_path,
        venue=venue,
        instrument=instrument,
    )
    assert result.l2_created is True

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )
    downloaded = repository.download_artifact(key)
    assert downloaded is not None
    assert downloaded.artifact.output_checkpoint is None
    assert downloaded.artifact.manifest.output_checkpoint_content_sha256 is None

    # Once the explicit blocked marker exists, ordinary reuse must not
    # incorrectly report that the chain can continue.
    with pytest.raises(worker.RemoteWorkerCheckpointBlockedError):
        await _gate_process(
            repository,
            tmp_path,
            venue=venue,
            instrument=instrument,
        )


@pytest.mark.asyncio
async def test_b2_binance_price_repair_does_not_replay_existing_l2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venue = "binance_futures"
    instrument = "BTCUSDT"
    store = MemoryStore()
    preset = _gate_chain_preset(venue, instrument)
    repository = B2WorkerRepository(
        store,
        preset=preset,
        single_writer_confirmed=True,
    )

    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=instrument,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )
    archive = _gate_source_archive(spec, tmp_path / "seed-raw")
    output = process_l2_archive_headlessly(
        archive,
        preset,
        producer_git_commit="a" * 40,
        batch_size=1,
    )
    artifact = worker._l2_artifact_for_publication(output)
    original = repository.publish_artifact(artifact)
    writes_before = tuple(store.writes)

    requests = _gate_install_acquisition(monkeypatch)

    def forbidden_l2(*_args, **_kwargs):
        raise AssertionError("Price repair must not replay existing L2")

    monkeypatch.setattr(worker, "process_l2_archive_headlessly", forbidden_l2)

    result = await _gate_process(
        repository,
        tmp_path,
        venue=venue,
        instrument=instrument,
    )

    assert len(requests) == 1
    assert len(requests[0]) == 1
    assert requests[0][0].data_kind is SourceDataKind.TRADES
    assert result.l2_reused is True
    assert result.l2_created is False
    assert result.price_created is True
    assert tuple(store.writes[: len(writes_before)]) == writes_before
    assert len(store.writes) == len(writes_before) + 3

    reread = repository.download_artifact(artifact.manifest.key)
    assert reread is not None
    assert reread.storage_identity.publication_reference == original.reference


@pytest.mark.asyncio
async def test_b2_new_hour_recovers_interrupted_completion_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from l2shock.remote.b2_transport import B2TransportError

    venue = "okx_futures"
    instrument = "BTC-USDT-SWAP"
    store = MemoryStore()
    preset = _gate_chain_preset(venue, instrument)
    repository = B2WorkerRepository(
        store,
        preset=preset,
        single_writer_confirmed=True,
    )
    _gate_install_acquisition(monkeypatch)

    original_put = store.put_bytes
    fail_descriptor = True

    def interrupted_put(key, data, *, content_type="application/octet-stream"):
        nonlocal fail_descriptor

        if key.endswith(".publication.json") and fail_descriptor:
            fail_descriptor = False
            raise B2TransportError("Simulated completion-publication interruption")

        return original_put(key, data, content_type=content_type)

    monkeypatch.setattr(store, "put_bytes", interrupted_put)

    with pytest.raises(B2TransportError):
        await _gate_process(
            repository,
            tmp_path,
            venue=venue,
            instrument=instrument,
        )

    assert len(store.writes) == 2
    retained_versions = dict(store.current)

    result = await _gate_process(
        repository,
        tmp_path,
        venue=venue,
        instrument=instrument,
    )

    assert result.l2_created is True
    assert len(store.writes) == 3

    for key, version in retained_versions.items():
        assert store.current[key] == version


# Batch 5B-B: verified source pinning, migration identity, and audit gates.


def _migration_hf_source(tmp_path: Path, artifact, *, wrong_revision=False):
    from l2shock.remote.artifact_codec import write_remote_artifact_file
    from l2shock.remote.b2_migration import PinnedHuggingFaceMigrationSource
    from l2shock.remote.hf_repository import DownloadedHuggingFaceArtifact

    artifact_path = tmp_path / "hf-artifact.parquet"
    manifest_path = tmp_path / "hf-manifest.json"
    write_remote_artifact_file(artifact_path, artifact)
    manifest_path.write_bytes(artifact.manifest.canonical_json_bytes)

    class Repository:
        repo_id = "test-owner/test-dataset"

        def __init__(self):
            self.pins = 0
            self.read_revisions = []
            self.head = "b" * 40

        def current_revision(self):
            self.pins += 1
            return self.head

        def download_artifact(self, key, *, revision):
            self.read_revisions.append(revision)
            assert key == artifact.manifest.key

            return DownloadedHuggingFaceArtifact(
                revision=("c" * 40 if wrong_revision else revision),
                artifact_path=artifact_path,
                manifest_path=manifest_path,
                artifact=artifact,
            )

    repository = Repository()
    source = PinnedHuggingFaceMigrationSource(repository)
    return source, repository


def test_migration_plan_has_exact_eight_artifacts_per_hour():
    from l2shock.remote.b2_migration import (
        CHAIN_PROFILES,
        build_migration_keys,
        terminal_l2_keys,
    )

    keys = build_migration_keys(
        chains=tuple(CHAIN_PROFILES),
        start_utc=_hour(),
        end_utc=_hour(2),
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert len(keys) == 16
    assert len(set(keys)) == 16
    assert {key.hour_utc for key in keys} == {_hour(), _hour(1)}
    assert sum(key.kind is RemoteArtifactKind.PRICE for key in keys) == 4
    assert all(
        key.venue == "binance_futures" and key.preset_hash is None
        for key in keys
        if key.kind is RemoteArtifactKind.PRICE
    )

    terminals = terminal_l2_keys(keys)
    assert len(terminals) == 6
    assert all(key.hour_utc == _hour(1) for key in terminals)


def test_migration_pins_hf_once_and_preserves_full_artifact(
    tmp_path: Path,
):
    from l2shock.remote.b2_migration import migrate_artifacts
    from l2shock.remote.b2_repository import B2ProcessedArtifactRepository

    artifact = _artifact(tmp_path / "input")
    source_root = tmp_path / "source"
    source_root.mkdir()
    source, hf = _migration_hf_source(source_root, artifact)
    hf.head = "d" * 40

    store = MemoryStore()
    destination = B2ProcessedArtifactRepository(store)
    events = []

    first = migrate_artifacts(
        source,
        destination,
        (artifact.manifest.key,),
        single_writer_confirmed=True,
        record=lambda payload: events.append(dict(payload)),
    )

    assert first.complete is True
    assert first.created == 1
    assert hf.pins == 1
    assert hf.read_revisions == ["b" * 40, "b" * 40]
    assert events[0]["source"]["revision"] == "b" * 40

    copied = next(event for event in events if event["event"] == "copied")
    storage = copied["storage_identity"]
    assert storage["backend"] == B2_STORAGE_BACKEND
    assert storage["endpoint_url"] == store.endpoint_url
    assert storage["bucket"] == store.bucket
    assert "revision" not in storage

    downloaded = destination.require_artifact(artifact.manifest.key)
    assert downloaded.artifact == artifact
    assert downloaded.artifact.encoded == artifact.encoded
    assert downloaded.artifact.output_checkpoint == artifact.output_checkpoint

    writes = tuple(store.writes)
    second = migrate_artifacts(
        source,
        destination,
        (artifact.manifest.key,),
        single_writer_confirmed=True,
        record=lambda _payload: None,
    )

    assert second.complete is True
    assert second.reused == 1
    assert second.created == 0
    assert tuple(store.writes) == writes
    assert hf.pins == 1


def test_migration_rejects_changed_hf_revision_before_b2_write(
    tmp_path: Path,
):
    from l2shock.remote.b2_migration import B2MigrationError, migrate_artifacts
    from l2shock.remote.b2_repository import B2ProcessedArtifactRepository

    artifact = _artifact(tmp_path / "input")
    source_root = tmp_path / "source"
    source_root.mkdir()
    source, _hf = _migration_hf_source(
        source_root,
        artifact,
        wrong_revision=True,
    )
    store = MemoryStore()
    events = []

    with pytest.raises(B2MigrationError, match="pinned revision"):
        migrate_artifacts(
            source,
            B2ProcessedArtifactRepository(store),
            (artifact.manifest.key,),
            single_writer_confirmed=True,
            record=lambda payload: events.append(dict(payload)),
        )

    assert store.writes == []
    assert events[-1]["event"] == "failed"
    assert events[-1]["stage"] == "terminal_preflight"


def test_migration_terminal_checkpoint_gate_runs_before_writes(
    tmp_path: Path,
):
    from l2shock.remote.b2_migration import B2MigrationError, migrate_artifacts
    from l2shock.remote.b2_repository import B2ProcessedArtifactRepository

    blocked = _artifact(tmp_path, locked=True)

    class Source:
        identity = {"backend": "test"}

        def read(self, key):
            assert key == blocked.manifest.key
            return blocked

    store = MemoryStore()

    with pytest.raises(B2MigrationError, match="Terminal seed"):
        migrate_artifacts(
            Source(),
            B2ProcessedArtifactRepository(store),
            (blocked.manifest.key,),
            single_writer_confirmed=True,
            record=lambda _payload: None,
        )

    assert store.writes == []


def test_migration_missing_history_is_not_reported_complete():
    from l2shock.remote.b2_migration import migrate_artifacts
    from l2shock.remote.b2_repository import B2ProcessedArtifactRepository

    class Source:
        identity = {"backend": "test"}

        def read(self, _key):
            return None

    store = MemoryStore()
    events = []

    counts = migrate_artifacts(
        Source(),
        B2ProcessedArtifactRepository(store),
        (_target_key(0),),
        single_writer_confirmed=True,
        record=lambda payload: events.append(dict(payload)),
        require_terminal_checkpoints=False,
    )

    assert counts.complete is False
    assert counts.missing == 1
    assert counts.created == counts.reused == 0
    assert store.writes == []
    assert events[-1]["event"] == "summary"
    assert events[-1]["complete"] is False


def test_migration_requires_external_writer_confirmation(
    tmp_path: Path,
):
    from l2shock.remote.b2_migration import B2MigrationError, migrate_artifacts
    from l2shock.remote.b2_repository import B2ProcessedArtifactRepository

    class Source:
        identity = {"backend": "test"}

        def read(self, _key):
            raise AssertionError("Unadmitted migration must not read sources")

    store = MemoryStore()

    with pytest.raises(B2MigrationError, match="single-writer"):
        migrate_artifacts(
            Source(),
            B2ProcessedArtifactRepository(store),
            (_target_key(0),),
            single_writer_confirmed=False,
            record=lambda _payload: None,
        )

    assert store.writes == []


def test_local_migration_requires_matching_external_manifest(
    tmp_path: Path,
):
    from l2shock.remote.artifact_codec import write_remote_artifact_file
    from l2shock.remote.b2_migration import B2MigrationError, LocalMigrationSource

    artifact = _artifact(tmp_path / "input")
    root = tmp_path / "local-output"
    key = artifact.manifest.key
    artifact_path = root.joinpath(*key.relative_path.split("/"))
    manifest_path = root.joinpath(*key.manifest_relative_path.split("/"))
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    write_remote_artifact_file(artifact_path, artifact)

    source = LocalMigrationSource(root)

    with pytest.raises(B2MigrationError, match="incomplete"):
        source.read(key)

    manifest_path.write_bytes(artifact.manifest.canonical_json_bytes)
    assert source.read(key) == artifact

    from l2shock.remote.contracts import RemoteContractError

    manifest_path.write_bytes(b"{}")
    with pytest.raises(RemoteContractError):
        source.read(key)


def test_migration_conflict_never_rewrites_destination(
    tmp_path: Path,
):
    from dataclasses import replace

    from l2shock.remote.artifact_codec import RemoteL2ProcessedArtifact
    from l2shock.remote.b2_migration import migrate_artifacts
    from l2shock.remote.b2_repository import (
        B2ArtifactConflictError,
        B2ProcessedArtifactRepository,
    )

    artifact = _artifact(tmp_path)
    conflicting = RemoteL2ProcessedArtifact(
        manifest=replace(
            artifact.manifest,
            producer_git_commit="e" * 40,
        ),
        encoded=artifact.encoded,
        output_checkpoint=artifact.output_checkpoint,
    )

    class Source:
        identity = {"backend": "test"}

        def read(self, _key):
            return conflicting

    store = MemoryStore()
    destination = B2ProcessedArtifactRepository(store)
    destination.publish_artifact(artifact, single_writer_confirmed=True)
    before = tuple(store.writes)
    events = []

    with pytest.raises(B2ArtifactConflictError):
        migrate_artifacts(
            Source(),
            destination,
            (artifact.manifest.key,),
            single_writer_confirmed=True,
            record=lambda payload: events.append(dict(payload)),
        )

    assert tuple(store.writes) == before
    assert not any(event["event"] == "copied" for event in events)
    assert events[-1]["event"] == "failed"


def test_migration_journal_refuses_overwrite_and_writes_valid_jsonl(
    tmp_path: Path,
):
    import json

    from l2shock.remote.b2_migration import MigrationJournal

    path = tmp_path / "receipts" / "run.jsonl"

    with MigrationJournal(path) as journal:
        journal.write({"event": "start"})
        journal.write({"event": "summary", "complete": True})

    original = path.read_bytes()
    records = [json.loads(line) for line in original.decode("utf-8").splitlines()]
    assert records == [
        {"event": "start"},
        {"event": "summary", "complete": True},
    ]

    with pytest.raises(FileExistsError):
        with MigrationJournal(path):
            pass

    assert path.read_bytes() == original


def test_migration_success_receipt_follows_pinned_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from l2shock.remote.b2_migration import migrate_artifacts
    from l2shock.remote.b2_repository import B2ProcessedArtifactRepository
    from l2shock.remote.b2_transport import B2IntegrityError

    artifact = _artifact(tmp_path)

    class Source:
        identity = {"backend": "test"}

        def read(self, _key):
            return artifact

    store = MemoryStore()
    destination = B2ProcessedArtifactRepository(store)
    original_publish = destination.publish_artifact
    original_require = destination.require_artifact
    publication_returned = False
    events = []

    def publish(*args, **kwargs):
        nonlocal publication_returned
        result = original_publish(*args, **kwargs)
        publication_returned = True
        return result

    def require(*args, **kwargs):
        if publication_returned:
            raise B2IntegrityError("Simulated final pinned read-back failure")
        return original_require(*args, **kwargs)

    monkeypatch.setattr(destination, "publish_artifact", publish)
    monkeypatch.setattr(destination, "require_artifact", require)

    with pytest.raises(B2IntegrityError):
        migrate_artifacts(
            Source(),
            destination,
            (artifact.manifest.key,),
            single_writer_confirmed=True,
            record=lambda payload: events.append(dict(payload)),
        )

    assert any(event["event"] == "publication_intent" for event in events)
    assert not any(event["event"] == "copied" for event in events)
    assert events[-1]["stage"] == "pinned_readback"

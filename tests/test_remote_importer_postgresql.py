# tests/test_remote_importer_postgresql.py
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Session

from l2shock.acquisition import AcquisitionRepository, SourceDataKind
from l2shock.db import (
    AnalyticalRepository,
    PriceAnalyticalRepository,
    get_engine,
)
from l2shock.db.schema import verify_schema
from l2shock.ingest import TradeRecord
from l2shock.presets import build_binance_futures_data_preset
from l2shock.price import (
    build_trade_ohlc_hour,
    encode_hourly_trade_ohlc_block,
)
from l2shock.remote import (
    DownloadedHuggingFaceArtifact,
    HuggingFaceDatasetRepository,
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemotePriceProcessedArtifact,
    RemoteSourceHourReference,
    download_and_import_huggingface_artifact,
    import_downloaded_huggingface_artifact,
    write_remote_artifact_file,
)

pytestmark = pytest.mark.postgresql


def _hour() -> datetime:
    return datetime(
        2091,
        1,
        1,
        12,
        tzinfo=timezone.utc,
    )


def _epoch_ms(value: datetime) -> int:
    epoch = datetime(
        1970,
        1,
        1,
        tzinfo=timezone.utc,
    )
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


@pytest.fixture
def database_session() -> Session:
    engine = get_engine()
    verify_schema(engine)

    connection = engine.connect()
    transaction = connection.begin()

    session = Session(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    try:
        yield session
    finally:
        session.close()

        if transaction.is_active:
            transaction.rollback()

        connection.close()


def _price_artifact() -> RemotePriceProcessedArtifact:
    trade_time_ms = _epoch_ms(_hour() + timedelta(milliseconds=100))

    block = build_trade_ohlc_hour(
        (
            TradeRecord(
                symbol="BTCUSDT",
                trade_id="remote-importer-test",
                price=Decimal("100.25"),
                quantity=Decimal("1"),
                received_time_ns=trade_time_ms * 1_000_000,
                event_time_ms=trade_time_ms,
                trade_time_ms=trade_time_ms,
                is_buyer_maker=False,
                order_type="market",
                row_number=0,
            ),
        ),
        base="BTC",
        hour_utc=_hour(),
    )
    encoded = encode_hourly_trade_ohlc_block(block)

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
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

    return RemotePriceProcessedArtifact(
        manifest=manifest,
        encoded=encoded,
    )


def _downloaded_price(
    tmp_path: Path,
) -> DownloadedHuggingFaceArtifact:
    artifact = _price_artifact()
    artifact_path = tmp_path / "price.parquet"
    manifest_path = tmp_path / "price.manifest.json"

    write_remote_artifact_file(
        artifact_path,
        artifact,
    )
    manifest_path.write_bytes(artifact.manifest.canonical_json_bytes)

    return DownloadedHuggingFaceArtifact(
        revision="c" * 40,
        artifact_path=artifact_path,
        manifest_path=manifest_path,
        artifact=artifact,
    )


def test_remote_price_import_uses_existing_repository_and_is_idempotent(
    database_session: Session,
    tmp_path: Path,
) -> None:
    downloaded = _downloaded_price(tmp_path)

    first = import_downloaded_huggingface_artifact(
        database_session,
        downloaded,
    )
    second = import_downloaded_huggingface_artifact(
        database_session,
        downloaded,
    )

    assert first.key.kind is RemoteArtifactKind.PRICE
    assert first.analytical_inserted is True
    assert first.preset_inserted is None
    assert first.source_metadata_updated is True

    assert second.analytical_inserted is False
    assert second.preset_inserted is None

    stored = PriceAnalyticalRepository(database_session).get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )

    assert stored is not None
    assert stored.encoded.content_sha256 == downloaded.artifact.encoded.content_sha256

    source = downloaded.artifact.manifest.key.source_spec
    source_row = AcquisitionRepository(database_session).get_source_hour(source)

    assert source_row is not None
    assert source_row.status == "processed"
    assert source_row.local_path is None
    assert source_row.content_sha256 == "a" * 64
    assert source_row.quality_json["processing_origin"] == (
        "hugging_face_remote_import_v1"
    )
    assert source_row.quality_json["remote_repository_revision"] == ("c" * 40)
    assert source_row.quality_json["price_content_sha256"] == (
        downloaded.artifact.encoded.content_sha256
    )


def test_remote_import_rejects_local_source_digest_conflict(
    database_session: Session,
    tmp_path: Path,
) -> None:
    downloaded = _downloaded_price(tmp_path)
    source = downloaded.artifact.manifest.key.source_spec

    row = AcquisitionRepository(database_session).upsert_discovered(source)
    row.content_sha256 = "f" * 64
    database_session.flush()

    with pytest.raises(
        Exception,
        match="different source archive SHA-256",
    ):
        import_downloaded_huggingface_artifact(
            database_session,
            downloaded,
        )

    # The low-level importer follows the existing repository convention:
    # transaction ownership remains with the caller. This test therefore
    # verifies the conflict itself; production atomic rollback is owned by
    # download_and_import_huggingface_artifact() and session_scope().
    assert row.content_sha256 == "f" * 64


def test_session_owning_remote_import_rolls_back_when_local_attachment_is_invalid(
    database_session: Session,
    tmp_path: Path,
) -> None:
    downloaded = _downloaded_price(tmp_path)
    source = downloaded.artifact.manifest.key.source_spec

    source_row = AcquisitionRepository(
        database_session,
    ).upsert_discovered(source)

    source_row.status = "error"
    source_row.local_path = str(tmp_path / "noncanonical-missing-source.parquet")
    source_row.file_size_bytes = 123
    source_row.content_sha256 = "a" * 64
    database_session.flush()

    class FakeApi:
        def repo_info(self, **_kwargs):
            return SimpleNamespace(
                sha=downloaded.revision,
            )

        def create_commit(self, **_kwargs):
            raise AssertionError("Importer test must not publish to Hugging Face")

    def fake_download(**kwargs) -> str:
        filename = str(kwargs["filename"])

        if filename == downloaded.artifact.manifest.key.relative_path:
            return str(downloaded.artifact_path)

        if filename == downloaded.artifact.manifest.key.manifest_relative_path:
            return str(downloaded.manifest_path)

        raise AssertionError(f"Unexpected Hugging Face path: {filename}")

    repository = HuggingFaceDatasetRepository(
        repo_id="example/private-l2shock",
        revision="main",
        token="hf_test_read_token",
        api=FakeApi(),
        download_function=fake_download,
    )

    @contextmanager
    def rollback_owned_scope():
        savepoint = database_session.begin_nested()

        try:
            yield database_session
            savepoint.commit()
        except BaseException:
            savepoint.rollback()
            database_session.expire_all()
            raise

    with pytest.raises(
        Exception,
        match="local raw attachment",
    ):
        download_and_import_huggingface_artifact(
            repository,
            downloaded.artifact.manifest.key,
            session_scope_factory=rollback_owned_scope,
        )

    stored = PriceAnalyticalRepository(
        database_session,
    ).get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )

    assert stored is None

    refreshed_source = AcquisitionRepository(
        database_session,
    ).get_source_hour(source)

    assert refreshed_source is not None
    assert refreshed_source.status == "error"
    assert refreshed_source.content_sha256 == "a" * 64
    assert refreshed_source.local_path == str(
        tmp_path / "noncanonical-missing-source.parquet"
    )


def _as_b2_download_for_import_test(
    artifact,
    tmp_path: Path,
):
    import hashlib

    from l2shock.remote.b2_publication import (
        B2Publication,
        B2PublicationReference,
        publication_relative_path,
    )
    from l2shock.remote.b2_repository import DownloadedB2Artifact
    from l2shock.remote.b2_transport import B2ObjectInfo

    path = tmp_path / "b2-import-artifact.parquet"
    local = write_remote_artifact_file(path, artifact)
    key = artifact.manifest.key
    manifest_bytes = artifact.manifest.canonical_json_bytes

    publication = B2Publication(
        key=key,
        artifact=B2ObjectInfo(
            key=key.relative_path,
            size_bytes=local.file_size_bytes,
            version_id="artifact-version-test",
            transport_sha256=local.transport_sha256,
        ),
        manifest=B2ObjectInfo(
            key=key.manifest_relative_path,
            size_bytes=len(manifest_bytes),
            version_id="manifest-version-test",
            transport_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        ),
    )
    descriptor = B2ObjectInfo(
        key=publication_relative_path(key),
        size_bytes=len(publication.canonical_json_bytes),
        version_id="descriptor-version-test",
        transport_sha256=publication.publication_sha256,
    )

    return DownloadedB2Artifact(
        reference=B2PublicationReference(publication, descriptor),
        artifact=artifact,
    )


def test_b2_price_import_is_idempotent_and_records_real_storage_ownership(
    database_session: Session,
    tmp_path: Path,
) -> None:
    from l2shock.remote.importer import import_downloaded_b2_artifact

    downloaded = _as_b2_download_for_import_test(_price_artifact(), tmp_path)

    arguments = {
        "endpoint_url": "https://s3.us-east-005.backblazeb2.com",
        "bucket": "l2shock-test-storage",
    }
    first = import_downloaded_b2_artifact(
        database_session,
        downloaded,
        **arguments,
    )
    second = import_downloaded_b2_artifact(
        database_session,
        downloaded,
        **arguments,
    )

    assert first.revision is None
    assert first.analytical_inserted is True
    assert second.analytical_inserted is False
    assert first.storage_identity == second.storage_identity

    stored = PriceAnalyticalRepository(database_session).get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )
    assert stored is not None
    assert stored.encoded == downloaded.artifact.encoded

    source_row = AcquisitionRepository(database_session).get_source_hour(
        downloaded.artifact.manifest.key.source_spec
    )
    assert source_row is not None
    assert source_row.status == "processed"
    assert source_row.local_path is None
    assert source_row.file_size_bytes is None
    assert source_row.content_sha256 == "a" * 64

    quality = source_row.quality_json
    assert quality["processing_origin"] == "backblaze_b2_remote_import_v1"
    assert quality["remote_repository_revision"] is None
    assert quality["remote_storage_identity"] == (
        first.storage_identity.to_canonical_dict()
    )
    assert quality["price_content_sha256"] == (
        downloaded.artifact.encoded.content_sha256
    )


def test_b2_import_preserves_previous_hf_revision_as_explicit_legacy_metadata(
    database_session: Session,
    tmp_path: Path,
) -> None:
    from l2shock.remote.importer import import_downloaded_b2_artifact

    artifact = _price_artifact()
    source_repository = AcquisitionRepository(database_session)
    row = source_repository.upsert_discovered(artifact.manifest.key.source_spec)
    row.quality_json = {
        "remote_repository_revision": "c" * 40,
        "unrelated_existing_metadata": {"keep": True},
    }
    database_session.flush()

    downloaded = _as_b2_download_for_import_test(artifact, tmp_path)
    result = import_downloaded_b2_artifact(
        database_session,
        downloaded,
        endpoint_url="https://s3.us-east-005.backblazeb2.com",
        bucket="l2shock-test-storage",
    )

    assert result.revision is None
    assert row.quality_json["remote_repository_revision"] is None
    assert row.quality_json["remote_hf_repository_revision"] == "c" * 40
    assert row.quality_json["unrelated_existing_metadata"] == {"keep": True}


def test_b2_price_import_failure_rolls_back_analytical_insert(
    database_session: Session,
    tmp_path: Path,
) -> None:
    from l2shock.remote.importer import (
        RemoteArtifactImportError,
        import_downloaded_b2_artifact,
    )

    artifact = _price_artifact()
    row = AcquisitionRepository(database_session).upsert_discovered(
        artifact.manifest.key.source_spec
    )
    row.status = "downloading"
    database_session.flush()

    downloaded = _as_b2_download_for_import_test(artifact, tmp_path)

    with pytest.raises(RemoteArtifactImportError, match="transient"):
        with database_session.begin_nested():
            import_downloaded_b2_artifact(
                database_session,
                downloaded,
                endpoint_url="https://s3.us-east-005.backblazeb2.com",
                bucket="l2shock-test-storage",
            )

    stored = PriceAnalyticalRepository(database_session).get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )
    assert stored is None
    assert row.status == "downloading"


def test_b2_l2_import_installs_existing_checkpoint_and_preserves_encoded_channels(
    database_session: Session,
    tmp_path: Path,
) -> None:
    import hashlib

    import pyarrow as pa
    import pyarrow.parquet as pq

    from l2shock.acquisition import SourceFileSpec, SourceHourStatus
    from l2shock.processing import ProcessingSourceArchive
    from l2shock.processing.checkpoint_store import (
        CheckpointIdentity,
        CheckpointStore,
    )
    from l2shock.remote.headless_processing import process_l2_archive_headlessly
    from l2shock.remote.importer import import_downloaded_b2_artifact

    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    received_ns = ((_hour() - epoch) // timedelta(microseconds=1)) * 1000
    received_ns += 100_000_000
    event_ms = received_ns // 1_000_000

    rows = [
        {
            "received_time": received_ns,
            "event_time": event_ms,
            "transaction_time": event_ms,
            "symbol": "BTCUSDT",
            "event_type": "snapshot",
            "first_update_id": None,
            "final_update_id": None,
            "prev_final_update_id": None,
            "last_update_id": 100,
            "side": side,
            "price": price,
            "quantity": quantity,
            "order_count": None,
        }
        for side, price, quantity in (
            ("bid", "100", "2"),
            ("ask", "101", "3"),
        )
    ]

    source_path = tmp_path / "raw-l2.parquet"
    pq.write_table(
        pa.Table.from_pylist(rows),
        source_path,
        compression="zstd",
    )
    source_bytes = source_path.read_bytes()
    archive = ProcessingSourceArchive(
        spec=spec,
        local_path=source_path,
        content_sha256=hashlib.sha256(source_bytes).hexdigest(),
        file_size_bytes=len(source_bytes),
        status=SourceHourStatus.DOWNLOADED,
    )
    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    output = process_l2_archive_headlessly(
        archive,
        preset,
        producer_git_commit="b" * 40,
        batch_size=1,
    )
    assert output.artifact.output_checkpoint is not None

    downloaded = _as_b2_download_for_import_test(output.artifact, tmp_path)
    checkpoint_store = CheckpointStore(tmp_path / "checkpoint-cache")

    first = import_downloaded_b2_artifact(
        database_session,
        downloaded,
        endpoint_url="https://s3.us-east-005.backblazeb2.com",
        bucket="l2shock-test-storage",
        checkpoint_store=checkpoint_store,
    )
    second = import_downloaded_b2_artifact(
        database_session,
        downloaded,
        endpoint_url="https://s3.us-east-005.backblazeb2.com",
        bucket="l2shock-test-storage",
        checkpoint_store=checkpoint_store,
    )

    assert first.analytical_inserted is True
    assert second.analytical_inserted is False
    assert first.revision is None
    assert first.storage_identity is not None

    installed = checkpoint_store.find_exact(
        CheckpointIdentity(
            provider=spec.provider,
            venue=spec.venue,
            instrument=spec.symbol,
            through_hour_utc=spec.hour_utc,
        )
    )
    assert installed is not None
    assert installed.encoding_info.content_sha256 == (
        output.artifact.manifest.output_checkpoint_content_sha256
    )

    stored = AnalyticalRepository(database_session).list_l2_hours(
        base="BTC",
        preset_hash=preset.preset_hash,
        start_utc=_hour(),
        end_utc=_hour() + timedelta(hours=1),
        verify_codec=True,
    )
    assert len(stored) == 1
    assert stored[0].encoded == output.artifact.encoded

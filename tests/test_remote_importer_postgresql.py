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

"""Remote price imports must acquire source admission before persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Session

import l2shock.remote.importer as importer
from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.price import (
    build_trade_ohlc_hour,
    encode_hourly_trade_ohlc_block,
)
from l2shock.remote.artifact_codec import RemotePriceProcessedArtifact
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteSourceHourReference,
)
from l2shock.remote.import_contracts import (
    HF_STORAGE_BACKEND,
    RemoteImportStorageIdentity,
    VerifiedRemoteImportArtifact,
)


def _lock_order_verified_price() -> VerifiedRemoteImportArtifact:
    hour = datetime(
        2026,
        10,
        1,
        12,
        tzinfo=timezone.utc,
    )

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=hour,
    )

    encoded = encode_hourly_trade_ohlc_block(
        build_trade_ohlc_hour(
            (),
            base="BTC",
            hour_utc=hour,
        )
    )

    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(
            RemoteSourceHourReference(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument="BTCUSDT",
                data_kind=SourceDataKind.TRADES,
                hour_utc=hour,
                content_sha256="a" * 64,
            ),
        ),
        content_sha256=encoded.content_sha256,
        producer_git_commit="b" * 40,
    )

    return VerifiedRemoteImportArtifact(
        storage_identity=RemoteImportStorageIdentity(
            backend=HF_STORAGE_BACKEND,
            revision="c" * 40,
        ),
        artifact=RemotePriceProcessedArtifact(
            manifest=manifest,
            encoded=encoded,
        ),
    )


@pytest.mark.parametrize("lock_available", (True, False))
def test_remote_price_source_lock_precedes_decode_and_persistence(
    monkeypatch: pytest.MonkeyPatch,
    lock_available: bool,
) -> None:
    downloaded = _lock_order_verified_price()
    artifact = downloaded.artifact
    events: list[str] = []

    expected_spec = SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=artifact.manifest.key.hour_utc,
    )

    class SimulatedSourceBusyError(RuntimeError):
        pass

    original_summary = importer._price_quality_summary

    with Session() as session:

        def acquire_lock(actual_session, actual_spec):
            assert actual_session is session
            assert actual_spec == expected_spec
            events.append("source_lock")

            if not lock_available:
                raise SimulatedSourceBusyError("source admission refused")

            return (1, 2)

        def quality_summary(actual_artifact):
            assert actual_artifact is artifact
            events.append("quality")
            return original_summary(actual_artifact)

        class PriceRepository:
            def __init__(self, actual_session):
                assert actual_session is session
                events.append("repository")

            def write_price_hour(self, **arguments):
                events.append("write")
                assert arguments["base"] == "BTC"
                assert arguments["hour_utc"] == expected_spec.hour_utc
                assert arguments["encoded"] is artifact.encoded
                return SimpleNamespace(inserted=True)

        def update_source_metadata(
            actual_session,
            actual_downloaded,
            *,
            quality_summary,
        ):
            assert actual_session is session
            assert actual_downloaded is downloaded
            assert quality_summary["observation_count"] == 3_600
            events.append("metadata")

        monkeypatch.setattr(
            importer,
            "acquire_source_hour_transaction_lock",
            acquire_lock,
        )
        monkeypatch.setattr(
            importer,
            "_price_quality_summary",
            quality_summary,
        )
        monkeypatch.setattr(
            importer,
            "PriceAnalyticalRepository",
            PriceRepository,
        )
        monkeypatch.setattr(
            importer,
            "_update_remote_source_metadata",
            update_source_metadata,
        )

        if not lock_available:
            with pytest.raises(
                SimulatedSourceBusyError,
                match="source admission refused",
            ):
                importer.import_verified_remote_artifact(
                    session,
                    downloaded,
                )

            assert events == ["source_lock"]
            return

        result = importer.import_verified_remote_artifact(
            session,
            downloaded,
        )

    assert events == [
        "source_lock",
        "quality",
        "repository",
        "write",
        "metadata",
    ]
    assert result.key == artifact.manifest.key
    assert result.revision == downloaded.revision
    assert result.storage_identity == downloaded.storage_identity
    assert result.analytical_inserted is True
    assert result.preset_inserted is None
    assert result.source_metadata_updated is True

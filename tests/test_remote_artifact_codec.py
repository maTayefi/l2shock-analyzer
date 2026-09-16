from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.ingest import (
    OBSERVATIONS_PER_HOUR,
    BookInitializationState,
    BookSampleInvalidReason,
    BookSampleQuality,
    ReplayBookStructure,
    TradeRecord,
)
from l2shock.liquidity import (
    DepthBand,
    HourlyLiquidityBlock,
    LiquidityObservation,
    build_liquidity_quality_summary,
    encode_hourly_liquidity_block,
)
from l2shock.presets import build_binance_futures_data_preset
from l2shock.price import (
    build_trade_ohlc_hour,
    encode_hourly_trade_ohlc_block,
)
from l2shock.remote import (
    RemoteArtifactCorruptionError,
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteContractError,
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    RemoteSourceHourReference,
    read_remote_artifact_file,
    write_remote_artifact_file,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        14,
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


def _l2_spec() -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )


def _invalid_l2_observation(
    index: int,
) -> LiquidityObservation:
    bucket_start = _hour() + timedelta(seconds=index)

    return LiquidityObservation(
        bucket_index=index,
        bucket_start_utc=bucket_start,
        bucket_end_utc=bucket_start + timedelta(seconds=1),
        source_received_time_ns=None,
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
        replay_valid=False,
        initialization_state=BookInitializationState.UNINITIALIZED,
        book_structure=ReplayBookStructure.EMPTY_BOTH,
        last_update_id=None,
        best_bid=None,
        best_ask=None,
        bid_liquidity=None,
        ask_liquidity=None,
        total_liquidity=None,
        bid_ask_imbalance=None,
        bid_depth_level_count=0,
        ask_depth_level_count=0,
        levels_examined=0,
        source_count=0,
    )


def _encoded_l2():
    observations = tuple(
        _invalid_l2_observation(index) for index in range(OBSERVATIONS_PER_HOUR)
    )

    block = HourlyLiquidityBlock(
        spec=_l2_spec(),
        band=DepthBand(
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
        ),
        imbalance_decimal_precision=34,
        observations=observations,
        quality_summary=build_liquidity_quality_summary(observations),
    )

    return encode_hourly_liquidity_block(block)


def _trade() -> TradeRecord:
    trade_time = _hour() + timedelta(milliseconds=100)
    trade_time_ms = _epoch_ms(trade_time)

    return TradeRecord(
        symbol="BTCUSDT",
        trade_id="remote-codec-test",
        price=Decimal("100.25"),
        quantity=Decimal("1"),
        received_time_ns=trade_time_ms * 1_000_000,
        event_time_ms=trade_time_ms,
        trade_time_ms=trade_time_ms,
        is_buyer_maker=False,
        order_type="market",
        row_number=0,
    )


def _encoded_price():
    block = build_trade_ohlc_hour(
        (_trade(),),
        base="BTC",
        hour_utc=_hour(),
    )

    return encode_hourly_trade_ohlc_block(block)


def _source(
    data_kind: SourceDataKind,
) -> RemoteSourceHourReference:
    return RemoteSourceHourReference(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        data_kind=data_kind,
        hour_utc=_hour(),
        content_sha256="a" * 64,
    )


def _l2_preset():
    return build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )


def _l2_artifact() -> RemoteL2ProcessedArtifact:
    encoded = _encoded_l2()
    preset = _l2_preset()

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(_source(SourceDataKind.ORDERBOOK),),
        content_sha256=encoded.content_sha256,
        l2_preset=preset,
        producer_git_commit="c" * 40,
    )

    return RemoteL2ProcessedArtifact(
        manifest=manifest,
        encoded=encoded,
        output_checkpoint=None,
    )


def _price_artifact() -> RemotePriceProcessedArtifact:
    encoded = _encoded_price()
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
    )
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(_source(SourceDataKind.TRADES),),
        content_sha256=encoded.content_sha256,
        producer_git_commit="d" * 40,
    )

    return RemotePriceProcessedArtifact(
        manifest=manifest,
        encoded=encoded,
    )


def test_manifest_canonical_json_round_trip() -> None:
    manifest = _l2_artifact().manifest

    decoded = RemoteArtifactManifest.from_canonical_json_bytes(
        manifest.canonical_json_bytes
    )

    assert decoded == manifest
    assert decoded.canonical_json_bytes == (manifest.canonical_json_bytes)
    assert decoded.manifest_sha256 == manifest.manifest_sha256


def test_manifest_rejects_noncanonical_json() -> None:
    manifest = _l2_artifact().manifest
    pretty = json.dumps(
        manifest.to_canonical_dict(),
        indent=2,
        sort_keys=True,
    ).encode("utf-8")

    assert pretty != manifest.canonical_json_bytes

    with pytest.raises(
        RemoteContractError,
        match="not canonically encoded",
    ):
        RemoteArtifactManifest.from_canonical_json_bytes(pretty)


def test_manifest_rejects_duplicate_json_keys() -> None:
    malformed = (
        b'{"schema":"l2shock.remote_hour_artifact",'
        b'"schema":"l2shock.remote_hour_artifact"}'
    )

    with pytest.raises(
        RemoteContractError,
        match="duplicate key",
    ):
        RemoteArtifactManifest.from_canonical_json_bytes(malformed)


def test_l2_transport_round_trip(
    tmp_path: Path,
) -> None:
    artifact = _l2_artifact()
    path = tmp_path / "l2.parquet"

    info = write_remote_artifact_file(
        path,
        artifact,
    )
    decoded = read_remote_artifact_file(
        path,
        expected_key=artifact.manifest.key,
        expected_manifest_sha256=(artifact.manifest.manifest_sha256),
        external_manifest_bytes=(artifact.manifest.canonical_json_bytes),
    )

    assert decoded == artifact
    assert info.path == path.resolve()
    assert info.file_size_bytes == path.stat().st_size
    assert len(info.transport_sha256) == 64
    assert info.manifest_sha256 == (artifact.manifest.manifest_sha256)


def test_price_transport_round_trip(
    tmp_path: Path,
) -> None:
    artifact = _price_artifact()
    path = tmp_path / "price.parquet"

    write_remote_artifact_file(
        path,
        artifact,
    )
    decoded = read_remote_artifact_file(
        path,
        expected_key=artifact.manifest.key,
        external_manifest_bytes=(artifact.manifest.canonical_json_bytes),
    )

    assert decoded == artifact


def test_existing_transport_is_not_overwritten_by_default(
    tmp_path: Path,
) -> None:
    artifact = _price_artifact()
    path = tmp_path / "price.parquet"

    write_remote_artifact_file(
        path,
        artifact,
    )

    with pytest.raises(FileExistsError):
        write_remote_artifact_file(
            path,
            artifact,
        )


def test_external_manifest_mismatch_is_rejected(
    tmp_path: Path,
) -> None:
    l2_artifact = _l2_artifact()
    price_artifact = _price_artifact()
    path = tmp_path / "l2.parquet"

    write_remote_artifact_file(
        path,
        l2_artifact,
    )

    with pytest.raises(
        RemoteArtifactCorruptionError,
        match="manifests disagree",
    ):
        read_remote_artifact_file(
            path,
            external_manifest_bytes=(price_artifact.manifest.canonical_json_bytes),
        )


def test_requested_key_mismatch_is_rejected(
    tmp_path: Path,
) -> None:
    artifact = _price_artifact()
    path = tmp_path / "price.parquet"

    write_remote_artifact_file(
        path,
        artifact,
    )

    wrong_key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour() + timedelta(hours=1),
    )

    with pytest.raises(
        RemoteArtifactCorruptionError,
        match="requested identity",
    ):
        read_remote_artifact_file(
            path,
            expected_key=wrong_key,
        )


def test_truncated_transport_is_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "truncated.parquet"
    path.write_bytes(b"PAR1")

    with pytest.raises(
        RemoteArtifactCorruptionError,
        match="not readable Parquet",
    ):
        read_remote_artifact_file(path)


def test_l2_transport_round_trip_preserves_canonical_preset(
    tmp_path: Path,
) -> None:
    artifact = _l2_artifact()
    path = tmp_path / "l2-with-preset.parquet"

    write_remote_artifact_file(
        path,
        artifact,
    )
    decoded = read_remote_artifact_file(
        path,
        expected_key=artifact.manifest.key,
    )

    assert isinstance(decoded, RemoteL2ProcessedArtifact)
    assert decoded.manifest.l2_preset == _l2_preset()
    assert decoded.manifest.l2_preset is not None
    assert decoded.manifest.l2_preset.preset_hash == decoded.manifest.key.preset_hash


def test_explicit_overwrite_replaces_complete_transport(
    tmp_path: Path,
) -> None:
    first = _l2_artifact()
    second = _price_artifact()
    path = tmp_path / "replace.parquet"

    write_remote_artifact_file(
        path,
        first,
    )
    write_remote_artifact_file(
        path,
        second,
        overwrite=True,
    )

    decoded = read_remote_artifact_file(
        path,
        expected_key=second.manifest.key,
    )

    assert decoded == second

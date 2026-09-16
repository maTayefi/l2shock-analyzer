# tests/test_remote_contracts.py
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    latest_release_eligible_hour,
)
from l2shock.presets import build_binance_futures_data_preset
from l2shock.remote import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteContractError,
    RemoteSourceHourReference,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        14,
        12,
        tzinfo=timezone.utc,
    )


def _l2_preset():
    return build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )


def _l2_key() -> RemoteArtifactKey:
    preset = _l2_preset()

    return RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )


def _orderbook_source(
    *,
    hour: datetime | None = None,
    digest: str = "b" * 64,
) -> RemoteSourceHourReference:
    return RemoteSourceHourReference(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour or _hour(),
        content_sha256=digest,
    )


def test_release_schedule_owns_fifteen_minute_delay() -> None:
    assert (
        latest_release_eligible_hour(
            datetime(
                2026,
                9,
                14,
                13,
                15,
                tzinfo=timezone.utc,
            ),
            release_delay_minutes=15,
        )
        == _hour()
    )

    assert latest_release_eligible_hour(
        datetime(
            2026,
            9,
            14,
            13,
            14,
            59,
            tzinfo=timezone.utc,
        ),
        release_delay_minutes=15,
    ) == datetime(
        2026,
        9,
        14,
        11,
        tzinfo=timezone.utc,
    )


def test_remote_l2_artifact_path_is_deterministic() -> None:
    key = _l2_key()
    preset_hash = _l2_preset().preset_hash

    assert key.relative_path == (
        "processed/v1/l2/cryptohftdata/binance_futures/"
        "BTCUSDT/" + preset_hash + "/2026-09-14/12.parquet"
    )
    assert key.manifest_relative_path == (
        "processed/v1/l2/cryptohftdata/binance_futures/"
        "BTCUSDT/" + preset_hash + "/2026-09-14/12.manifest.json"
    )


def test_remote_price_artifact_has_no_preset_identity() -> None:
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="ETHUSDT",
        hour_utc=_hour(),
    )

    assert key.preset_hash is None
    assert key.relative_path == (
        "processed/v1/price/cryptohftdata/binance_futures/"
        "ETHUSDT/2026-09-14/12.parquet"
    )


def test_remote_manifest_is_source_order_independent() -> None:
    previous = _orderbook_source(
        hour=datetime(
            2026,
            9,
            14,
            11,
            tzinfo=timezone.utc,
        ),
        digest="c" * 64,
    )
    current = _orderbook_source()

    first = RemoteArtifactManifest(
        key=_l2_key(),
        source_hours=(
            current,
            previous,
        ),
        content_sha256="d" * 64,
        l2_preset=_l2_preset(),
        input_checkpoint_content_sha256="e" * 64,
        output_checkpoint_content_sha256="f" * 64,
        producer_git_commit="1" * 40,
    )
    second = RemoteArtifactManifest(
        key=_l2_key(),
        source_hours=(
            previous,
            current,
        ),
        content_sha256="d" * 64,
        l2_preset=_l2_preset(),
        input_checkpoint_content_sha256="e" * 64,
        output_checkpoint_content_sha256="f" * 64,
        producer_git_commit="1" * 40,
    )

    assert first.canonical_json_bytes == second.canonical_json_bytes
    assert first.manifest_sha256 == second.manifest_sha256


def test_remote_manifest_requires_current_target_source() -> None:
    with pytest.raises(
        RemoteContractError,
        match="current target source hour",
    ):
        RemoteArtifactManifest(
            key=_l2_key(),
            source_hours=(
                _orderbook_source(
                    hour=datetime(
                        2026,
                        9,
                        14,
                        11,
                        tzinfo=timezone.utc,
                    ),
                ),
            ),
            content_sha256="d" * 64,
            l2_preset=_l2_preset(),
        )


def test_price_manifest_cannot_claim_checkpoint_ownership() -> None:
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
    )
    source = RemoteSourceHourReference(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_hour(),
        content_sha256="a" * 64,
    )

    with pytest.raises(
        RemoteContractError,
        match="cannot own order-book checkpoints",
    ):
        RemoteArtifactManifest(
            key=key,
            source_hours=(source,),
            content_sha256="b" * 64,
            output_checkpoint_content_sha256="c" * 64,
        )


def test_remote_l2_manifest_owns_complete_canonical_preset() -> None:
    preset = _l2_preset()

    manifest = RemoteArtifactManifest(
        key=_l2_key(),
        source_hours=(_orderbook_source(),),
        content_sha256="d" * 64,
        l2_preset=preset,
    )

    decoded = RemoteArtifactManifest.from_canonical_json_bytes(
        manifest.canonical_json_bytes
    )

    assert decoded.l2_preset == preset
    assert decoded.l2_preset is not None
    assert decoded.l2_preset.preset_hash == manifest.key.preset_hash
    assert decoded.canonical_json_bytes == manifest.canonical_json_bytes


def test_remote_l2_manifest_rejects_missing_preset_content() -> None:
    with pytest.raises(
        RemoteContractError,
        match="canonical LiquidityDataPreset",
    ):
        RemoteArtifactManifest(
            key=_l2_key(),
            source_hours=(_orderbook_source(),),
            content_sha256="d" * 64,
        )

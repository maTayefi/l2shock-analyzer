# tests/test_remote_cli.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.presets import build_binance_futures_data_preset
from l2shock.remote import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    read_remote_artifact_file,
)
from l2shock.remote_cli import main


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        14,
        12,
        tzinfo=timezone.utc,
    )


def _epoch_ns(value: datetime) -> int:
    epoch = datetime(
        1970,
        1,
        1,
        tzinfo=timezone.utc,
    )
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
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


def _spec(
    data_kind: SourceDataKind,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=data_kind,
        hour_utc=_hour(),
    )


def _write_snapshot_source(
    raw_root: Path,
) -> Path:
    spec = _spec(SourceDataKind.ORDERBOOK)
    path = spec.local_path(raw_root)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    received = _epoch_ns(_hour()) + 100_000_000
    event_time = received // 1_000_000

    table = pa.Table.from_pylist(
        [
            {
                "received_time": received,
                "event_time": event_time,
                "transaction_time": event_time,
                "symbol": "BTCUSDT",
                "event_type": "snapshot",
                "first_update_id": None,
                "final_update_id": None,
                "prev_final_update_id": None,
                "last_update_id": 100,
                "side": "bid",
                "price": "100",
                "quantity": "2",
                "order_count": None,
            },
            {
                "received_time": received,
                "event_time": event_time,
                "transaction_time": event_time,
                "symbol": "BTCUSDT",
                "event_type": "snapshot",
                "first_update_id": None,
                "final_update_id": None,
                "prev_final_update_id": None,
                "last_update_id": 100,
                "side": "ask",
                "price": "101",
                "quantity": "3",
                "order_count": None,
            },
        ]
    )

    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=1,
    )

    return path


def _write_trade_source(
    raw_root: Path,
) -> Path:
    spec = _spec(SourceDataKind.TRADES)
    path = spec.local_path(raw_root)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    trade_time = _hour() + timedelta(milliseconds=100)
    trade_time_ms = _epoch_ms(trade_time)

    table = pa.Table.from_pylist(
        [
            {
                "received_time": trade_time_ms * 1_000_000,
                "event_time": trade_time_ms,
                "symbol": "BTCUSDT",
                "trade_id": "remote-cli-price-test",
                "price": "100.25",
                "quantity": "1",
                "trade_time": trade_time_ms,
                "is_buyer_maker": False,
                "order_type": "market",
            }
        ]
    )

    pq.write_table(
        table,
        path,
        compression="zstd",
    )

    return path


def test_l2_cli_writes_deterministic_artifact_and_manifest(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "input"
    output_root = tmp_path / "output"

    _write_snapshot_source(raw_root)

    arguments = [
        "l2",
        "--venue",
        "binance_futures",
        "--instrument",
        "BTCUSDT",
        "--hour",
        "2026-09-14T12:00:00Z",
        "--depth-lower",
        "0",
        "--depth-upper",
        "0.01",
        "--input-dir",
        str(raw_root),
        "--output-dir",
        str(output_root),
        "--producer-git-commit",
        "a" * 40,
        "--batch-size",
        "1",
    ]

    assert main(arguments) == 0

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )

    artifact_path = output_root.joinpath(*key.relative_path.split("/"))
    manifest_path = output_root.joinpath(*key.manifest_relative_path.split("/"))

    assert artifact_path.is_file()
    assert manifest_path.is_file()

    decoded = read_remote_artifact_file(
        artifact_path,
        expected_key=key,
        external_manifest_bytes=manifest_path.read_bytes(),
    )

    assert isinstance(decoded, RemoteL2ProcessedArtifact)
    assert decoded.manifest.l2_preset == preset
    assert decoded.output_checkpoint is not None

    artifact_bytes = artifact_path.read_bytes()
    manifest_bytes = manifest_path.read_bytes()

    # Identical reruns are idempotent success, not conflicts.
    assert main(arguments) == 0
    assert artifact_path.read_bytes() == artifact_bytes
    assert manifest_path.read_bytes() == manifest_bytes


def test_price_cli_writes_current_source_only_artifact(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "input"
    output_root = tmp_path / "output"

    _write_trade_source(raw_root)

    status = main(
        [
            "price",
            "--venue",
            "binance_futures",
            "--instrument",
            "BTCUSDT",
            "--hour",
            "2026-09-14T12:00:00Z",
            "--input-dir",
            str(raw_root),
            "--output-dir",
            str(output_root),
            "--producer-git-commit",
            "b" * 40,
            "--batch-size",
            "1",
        ]
    )

    assert status == 0

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
    )
    artifact_path = output_root.joinpath(*key.relative_path.split("/"))
    manifest_path = output_root.joinpath(*key.manifest_relative_path.split("/"))

    decoded = read_remote_artifact_file(
        artifact_path,
        expected_key=key,
        external_manifest_bytes=manifest_path.read_bytes(),
    )

    assert isinstance(decoded, RemotePriceProcessedArtifact)
    assert len(decoded.manifest.source_hours) == 1
    assert decoded.manifest.source_hours[0].hour_utc == _hour()


def test_cli_reports_missing_source_with_exit_status_three(
    tmp_path: Path,
) -> None:
    status = main(
        [
            "price",
            "--venue",
            "binance_futures",
            "--instrument",
            "BTCUSDT",
            "--hour",
            "2026-09-14T12:00:00Z",
            "--input-dir",
            str(tmp_path / "missing-input"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )

    assert status == 3


def test_cli_refuses_conflicting_external_manifest(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "input"
    output_root = tmp_path / "output"

    _write_trade_source(raw_root)

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
    )
    manifest_path = output_root.joinpath(*key.manifest_relative_path.split("/"))
    manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    manifest_path.write_bytes(b"conflicting manifest")

    status = main(
        [
            "price",
            "--venue",
            "binance_futures",
            "--instrument",
            "BTCUSDT",
            "--hour",
            "2026-09-14T12:00:00Z",
            "--input-dir",
            str(raw_root),
            "--output-dir",
            str(output_root),
            "--batch-size",
            "1",
        ]
    )

    assert status == 4

    artifact_path = output_root.joinpath(*key.relative_path.split("/"))

    # Manifest conflict is detected before writing the artifact.
    assert not artifact_path.exists()

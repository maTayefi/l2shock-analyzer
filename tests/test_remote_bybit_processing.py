from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.liquidity import decode_hourly_liquidity_blocks
from l2shock.presets import build_bybit_data_preset
from l2shock.processing import ProcessingSourceArchive
from l2shock.remote import process_l2_archive_headlessly


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        4,
        6,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


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


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="bybit",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(offset),
    )


def _preset():
    return build_bybit_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )


def _row(
    *,
    hour_offset: int,
    milliseconds: int,
    event_type: str,
    side: str,
    price: str,
    quantity: str,
    final_update_id: int | None,
    last_update_id: int,
    transaction_time_present: bool = True,
) -> dict[str, object]:
    received_time = _epoch_ns(_hour(hour_offset)) + milliseconds * 1_000_000
    event_time = received_time // 1_000_000

    return {
        "received_time": received_time,
        "event_time": event_time,
        "transaction_time": (event_time if transaction_time_present else None),
        "symbol": "BTCUSDT",
        "event_type": event_type,
        "first_update_id": None,
        "final_update_id": final_update_id,
        "prev_final_update_id": None,
        "last_update_id": last_update_id,
        "side": side,
        "price": price,
        "quantity": quantity,
        "order_count": None,
    }


def _native_snapshot_rows() -> list[dict[str, object]]:
    return [
        _row(
            hour_offset=0,
            milliseconds=100,
            event_type="snapshot",
            side="bid",
            price="100",
            quantity="2",
            final_update_id=500,
            last_update_id=9_000,
        ),
        _row(
            hour_offset=0,
            milliseconds=100,
            event_type="snapshot",
            side="ask",
            price="101",
            quantity="3",
            final_update_id=500,
            last_update_id=9_000,
        ),
        _row(
            hour_offset=0,
            milliseconds=200,
            event_type="update",
            side="bid",
            price="100",
            quantity="4",
            final_update_id=501,
            last_update_id=9_100,
        ),
    ]


def _boundary_snapshot_rows() -> list[dict[str, object]]:
    return [
        _row(
            hour_offset=1,
            milliseconds=0,
            event_type="snapshot",
            side="bid",
            price="99",
            quantity="5",
            final_update_id=None,
            last_update_id=9_100,
            transaction_time_present=False,
        ),
        _row(
            hour_offset=1,
            milliseconds=0,
            event_type="snapshot",
            side="ask",
            price="102",
            quantity="6",
            final_update_id=None,
            last_update_id=9_100,
            transaction_time_present=False,
        ),
        _row(
            hour_offset=1,
            milliseconds=100,
            event_type="update",
            side="bid",
            price="99.5",
            quantity="7",
            final_update_id=502,
            last_update_id=9_200,
        ),
    ]


def _write(
    path: Path,
    rows: list[dict[str, object]],
) -> ProcessingSourceArchive:
    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
        row_group_size=1,
    )

    content = path.read_bytes()

    offset = 1 if rows[0]["transaction_time"] is None else 0

    return ProcessingSourceArchive(
        spec=_spec(offset),
        local_path=path,
        content_sha256=hashlib.sha256(content).hexdigest(),
        file_size_bytes=len(content),
        status=SourceHourStatus.DOWNLOADED,
    )


def test_remote_bybit_native_snapshot_produces_checkpoint(
    tmp_path: Path,
) -> None:
    archive = _write(
        tmp_path / "bybit-native.parquet",
        _native_snapshot_rows(),
    )

    output = process_l2_archive_headlessly(
        archive,
        _preset(),
        producer_git_commit="a" * 40,
        batch_size=1,
    )

    manifest = output.artifact.manifest

    assert manifest.key.venue == "bybit"
    assert manifest.key.instrument == "BTCUSDT"
    assert manifest.key.hour_utc == _hour()
    assert manifest.l2_preset == _preset()

    assert manifest.input_checkpoint_content_sha256 is None
    assert manifest.output_checkpoint_content_sha256 is not None
    assert output.artifact.output_checkpoint is not None

    assert output.replay_snapshot_count == 1
    assert output.replay_continuity_mismatch_count == 0

    decoded = decode_hourly_liquidity_blocks(output.artifact.encoded)

    assert decoded.valid_count == 3_600
    assert decoded.invalid_count == 0
    assert decoded.bid_liquidity[0] == Decimal("400")
    assert decoded.ask_liquidity[0] == Decimal("303")


def test_remote_bybit_boundary_snapshot_uses_predecessor_checkpoint(
    tmp_path: Path,
) -> None:
    first_archive = _write(
        tmp_path / "bybit-hour-06.parquet",
        _native_snapshot_rows(),
    )
    second_archive = _write(
        tmp_path / "bybit-hour-07.parquet",
        _boundary_snapshot_rows(),
    )

    first = process_l2_archive_headlessly(
        first_archive,
        _preset(),
        batch_size=1,
    )

    assert first.artifact.output_checkpoint is not None

    second = process_l2_archive_headlessly(
        second_archive,
        _preset(),
        input_checkpoint_bytes=first.artifact.output_checkpoint,
        batch_size=1,
    )

    assert second.artifact.output_checkpoint is not None

    assert (
        second.artifact.manifest.input_checkpoint_content_sha256
        == first.artifact.manifest.output_checkpoint_content_sha256
    )
    assert second.artifact.manifest.output_checkpoint_content_sha256 is not None

    assert second.replay_snapshot_count == 1
    assert second.replay_continuity_mismatch_count == 0

    decoded = decode_hourly_liquidity_blocks(second.artifact.encoded)

    assert decoded.valid_count == 3_600
    assert decoded.invalid_count == 0
    assert decoded.bid_liquidity[0] == Decimal("696.5")
    assert decoded.ask_liquidity[0] == Decimal("612")

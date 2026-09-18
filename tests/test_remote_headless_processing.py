# tests/test_remote_headless_processing.py
from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.ingest import (
    BookSampleQuality,
    checkpoint_encoding_info,
    decode_checkpoint,
    encode_checkpoint,
)
from l2shock.liquidity import decode_hourly_liquidity_blocks
from l2shock.presets import build_binance_futures_data_preset
from l2shock.price import decode_hourly_trade_ohlc_blocks
from l2shock.processing import (
    ProcessingContractError,
    ProcessingSourceArchive,
)
from l2shock.remote import (
    RemoteArtifactCorruptionError,
    RemoteArtifactKind,
    RemoteL2ProcessedArtifact,
    process_l2_archive_headlessly,
    process_price_archives_headlessly,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        14,
        12,
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


def _archive(
    path: Path,
    spec: SourceFileSpec,
) -> ProcessingSourceArchive:
    content = path.read_bytes()

    return ProcessingSourceArchive(
        spec=spec,
        local_path=path,
        content_sha256=hashlib.sha256(content).hexdigest(),
        file_size_bytes=len(content),
        status=SourceHourStatus.DOWNLOADED,
    )


def _l2_spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(offset),
    )


def _price_spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_hour(offset),
    )


def _preset():
    return build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )


def _write_snapshot_archive(
    path: Path,
    *,
    hour: datetime,
) -> None:
    received = _epoch_ns(hour) + 100_000_000
    event_time = received // 1_000_000

    table = pa.Table.from_pylist(
        [
            {
                "received_time": received,
                "event_time": event_time,
                "transaction_time": None,
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
                "transaction_time": None,
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


def _write_update_archive(
    path: Path,
    *,
    hour: datetime,
) -> None:
    received = _epoch_ns(hour) + 100_000_000
    event_time = received // 1_000_000

    table = pa.Table.from_pylist(
        [
            {
                "received_time": received,
                "event_time": event_time,
                "transaction_time": event_time,
                "symbol": "BTCUSDT",
                "event_type": "update",
                "first_update_id": 101,
                "final_update_id": 101,
                "prev_final_update_id": 100,
                "last_update_id": None,
                "side": "bid",
                "price": "100",
                "quantity": "5",
                "order_count": None,
            }
        ]
    )

    pq.write_table(
        table,
        path,
        compression="zstd",
    )


def _write_trade_archive(
    path: Path,
    *,
    source_hour: datetime,
    target_trade_time: datetime,
) -> None:
    trade_time_ms = _epoch_ms(target_trade_time)

    table = pa.Table.from_pylist(
        [
            {
                "received_time": trade_time_ms * 1_000_000,
                "event_time": trade_time_ms,
                "symbol": "BTCUSDT",
                "trade_id": (source_hour.isoformat() + ":" + str(trade_time_ms)),
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


def test_headless_l2_processing_builds_verified_remote_artifact(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_snapshot_archive(
        path,
        hour=_hour(),
    )

    output = process_l2_archive_headlessly(
        _archive(
            path,
            _l2_spec(),
        ),
        _preset(),
        producer_git_commit="a" * 40,
        batch_size=1,
    )

    artifact = output.artifact
    manifest = artifact.manifest

    assert manifest.key.kind is RemoteArtifactKind.L2
    assert manifest.key.hour_utc == _hour()
    assert manifest.key.preset_hash == _preset().preset_hash
    assert manifest.l2_preset == _preset()
    assert manifest.input_checkpoint_content_sha256 is None
    assert manifest.output_checkpoint_content_sha256 is not None

    assert artifact.output_checkpoint is not None

    checkpoint = decode_checkpoint(artifact.output_checkpoint)

    assert checkpoint.through_hour_utc == _hour()
    assert checkpoint.symbol == "BTCUSDT"
    assert checkpoint.source_content_sha256 == (
        artifact.manifest.source_hours[0].content_sha256
    )
    assert all(level.order_count is None for level in checkpoint.levels)

    decoded = decode_hourly_liquidity_blocks(artifact.encoded)

    assert decoded.valid_count == 3_600
    assert decoded.invalid_count == 0
    assert decoded.bid_liquidity[0] == Decimal("200")
    assert decoded.ask_liquidity[0] == Decimal("303")

    assert output.replay_event_count == 1
    assert output.replay_snapshot_count == 1
    assert output.replay_continuity_mismatch_count == 0


def test_headless_l2_checkpoint_initializes_next_update_only_hour(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    _write_snapshot_archive(
        first_path,
        hour=_hour(),
    )
    _write_update_archive(
        second_path,
        hour=_hour(1),
    )

    first = process_l2_archive_headlessly(
        _archive(
            first_path,
            _l2_spec(),
        ),
        _preset(),
        batch_size=1,
    )

    assert first.artifact.output_checkpoint is not None

    second = process_l2_archive_headlessly(
        _archive(
            second_path,
            _l2_spec(1),
        ),
        _preset(),
        input_checkpoint_bytes=(first.artifact.output_checkpoint),
        batch_size=1,
    )

    assert (
        second.artifact.manifest.input_checkpoint_content_sha256
        == first.artifact.manifest.output_checkpoint_content_sha256
    )

    decoded = decode_hourly_liquidity_blocks(second.artifact.encoded)

    assert decoded.quality[0] is BookSampleQuality.VALID
    assert decoded.bid_liquidity[0] == Decimal("500")
    assert decoded.ask_liquidity[0] == Decimal("303")


def test_headless_l2_rejects_wrong_preset_market(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_snapshot_archive(
        path,
        hour=_hour(),
    )

    wrong_preset = build_binance_futures_data_preset(
        base="ETH",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0"),
    )

    with pytest.raises(
        ProcessingContractError,
        match="base|market identity",
    ):
        process_l2_archive_headlessly(
            _archive(
                path,
                _l2_spec(),
            ),
            wrong_preset,
        )


def test_headless_price_processing_builds_verified_remote_artifact(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"

    _write_trade_archive(
        path,
        source_hour=_hour(),
        target_trade_time=_hour() + timedelta(milliseconds=100),
    )

    archive = _archive(
        path,
        _price_spec(),
    )

    output = process_price_archives_headlessly(
        _price_spec(),
        (archive,),
        producer_git_commit="b" * 40,
        batch_size=1,
    )

    artifact = output.artifact
    manifest = artifact.manifest

    assert manifest.key.kind is RemoteArtifactKind.PRICE
    assert manifest.key.hour_utc == _hour()
    assert manifest.key.preset_hash is None
    assert manifest.l2_preset is None
    assert manifest.input_checkpoint_content_sha256 is None
    assert manifest.output_checkpoint_content_sha256 is None

    decoded = decode_hourly_trade_ohlc_blocks(artifact.encoded)

    assert decoded.valid_count == 1
    assert decoded.invalid_count == 3_599
    assert decoded.open[0] == Decimal("100.25")
    assert decoded.close[0] == Decimal("100.25")
    assert decoded.trade_count[0] == 1

    assert output.input_trade_count == 1
    assert output.accepted_trade_count == 1
    assert output.exact_duplicate_trade_count == 0
    assert output.outside_target_hour_trade_count == 0


def test_headless_price_processing_requires_current_archive(
    tmp_path: Path,
) -> None:
    previous_path = tmp_path / "previous.parquet"

    _write_trade_archive(
        previous_path,
        source_hour=_hour(-1),
        target_trade_time=_hour() + timedelta(milliseconds=100),
    )

    with pytest.raises(
        ProcessingContractError,
        match="exactly one current target archive",
    ):
        process_price_archives_headlessly(
            _price_spec(),
            (
                _archive(
                    previous_path,
                    _price_spec(-1),
                ),
            ),
        )


def test_remote_l2_artifact_rejects_checkpoint_source_hash_mismatch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_snapshot_archive(
        path,
        hour=_hour(),
    )

    output = process_l2_archive_headlessly(
        _archive(
            path,
            _l2_spec(),
        ),
        _preset(),
        batch_size=1,
    )

    original_bytes = output.artifact.output_checkpoint

    assert original_bytes is not None

    changed_checkpoint = replace(
        decode_checkpoint(original_bytes),
        source_content_sha256="f" * 64,
    )
    changed_bytes = encode_checkpoint(changed_checkpoint)
    changed_hash = checkpoint_encoding_info(changed_bytes).content_sha256

    changed_manifest = replace(
        output.artifact.manifest,
        output_checkpoint_content_sha256=changed_hash,
    )

    with pytest.raises(
        RemoteArtifactCorruptionError,
        match="source SHA-256",
    ):
        RemoteL2ProcessedArtifact(
            manifest=changed_manifest,
            encoded=output.artifact.encoded,
            output_checkpoint=changed_bytes,
        )


def test_headless_l2_normalizes_processing_cancellation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_snapshot_archive(
        path,
        hour=_hour(),
    )

    checks = 0

    def cancel() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 5

    from l2shock.processing import ProcessingCancelledError

    with pytest.raises(
        ProcessingCancelledError,
        match="cancelled",
    ):
        process_l2_archive_headlessly(
            _archive(
                path,
                _l2_spec(),
            ),
            _preset(),
            cancellation_probe=cancel,
            batch_size=1,
            cancellation_check_interval_rows=1,
            cancellation_check_interval_levels=1,
        )


def test_headless_price_normalizes_processing_cancellation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"

    _write_trade_archive(
        path,
        source_hour=_hour(),
        target_trade_time=_hour() + timedelta(milliseconds=100),
    )

    checks = 0

    def cancel() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 5

    from l2shock.processing import ProcessingCancelledError

    with pytest.raises(
        ProcessingCancelledError,
        match="cancelled",
    ):
        process_price_archives_headlessly(
            _price_spec(),
            (
                _archive(
                    path,
                    _price_spec(),
                ),
            ),
            cancellation_probe=cancel,
            batch_size=1,
            cancellation_check_interval_rows=1,
            cancellation_check_interval_records=1,
        )


def test_headless_price_skips_exact_binance_zero_price_trade_rows(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"

    first_trade_time_ms = _epoch_ms(_hour() + timedelta(milliseconds=50))
    valid_trade_time_ms = _epoch_ms(_hour() + timedelta(milliseconds=100))

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "received_time": first_trade_time_ms * 1_000_000,
                    "event_time": first_trade_time_ms,
                    "symbol": "BTCUSDT",
                    "trade_id": "zero-price-sentinel",
                    "price": "0",
                    "quantity": "1",
                    "trade_time": first_trade_time_ms,
                    "is_buyer_maker": False,
                    "order_type": "market",
                },
                {
                    "received_time": valid_trade_time_ms * 1_000_000,
                    "event_time": valid_trade_time_ms,
                    "symbol": "BTCUSDT",
                    "trade_id": "valid-trade",
                    "price": "100.25",
                    "quantity": "2",
                    "trade_time": valid_trade_time_ms,
                    "is_buyer_maker": True,
                    "order_type": "market",
                },
            ]
        ),
        path,
        compression="zstd",
        row_group_size=1,
    )

    caplog.set_level(
        "WARNING",
        logger="l2shock.ingest.parquet_reader",
    )

    output = process_price_archives_headlessly(
        _price_spec(),
        (
            _archive(
                path,
                _price_spec(),
            ),
        ),
        batch_size=1,
    )

    decoded = decode_hourly_trade_ohlc_blocks(output.artifact.encoded)

    assert output.input_trade_count == 1
    assert output.accepted_trade_count == 1
    assert decoded.valid_count == 1
    assert decoded.invalid_count == 3_599
    assert decoded.open[0] == Decimal("100.25")
    assert decoded.high[0] == Decimal("100.25")
    assert decoded.low[0] == Decimal("100.25")
    assert decoded.close[0] == Decimal("100.25")
    assert decoded.trade_count[0] == 1

    assert "BINANCE ZERO-PRICE TRADE ROW SKIPPED" in caplog.text
    assert "skipped_zero_price_rows=1" in caplog.text

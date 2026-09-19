from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import l2shock.remote.headless_processing as headless_module
from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.presets import build_binance_futures_data_preset
from l2shock.processing import ProcessingSourceArchive
from l2shock.processing.errors import SourceArchiveIntegrityError


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
    return _epoch_ns(value) // 1_000_000


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


def _replace_with_same_size_different_content(
    path: Path,
) -> None:
    size = path.stat().st_size
    path.write_bytes(b"x" * size)


def _write_orderbook_snapshot(
    path: Path,
) -> None:
    received_time_ns = _epoch_ns(
        _hour() + timedelta(milliseconds=100),
    )
    event_time_ms = received_time_ns // 1_000_000

    rows = [
        {
            "received_time": received_time_ns,
            "event_time": event_time_ms,
            "transaction_time": event_time_ms,
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
            "received_time": received_time_ns,
            "event_time": event_time_ms,
            "transaction_time": event_time_ms,
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

    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
    )


def _write_trade_archive(
    path: Path,
) -> None:
    trade_time = _hour() + timedelta(milliseconds=100)
    trade_time_ms = _epoch_ms(trade_time)

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "received_time": trade_time_ms * 1_000_000,
                    "event_time": trade_time_ms,
                    "symbol": "BTCUSDT",
                    "trade_id": "post-stream-integrity-trade",
                    "price": "100.25",
                    "quantity": "1",
                    "trade_time": trade_time_ms,
                    "is_buyer_maker": False,
                    "order_type": "market",
                }
            ]
        ),
        path,
        compression="zstd",
    )


def test_headless_l2_rejects_source_changed_after_streaming(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_orderbook_snapshot(path)

    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )
    archive = _archive(path, spec)
    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    original_sample = headless_module.sample_liquidity_archives

    def sample_then_replace(*args, **kwargs):
        result = original_sample(*args, **kwargs)
        _replace_with_same_size_different_content(path)
        return result

    monkeypatch.setattr(
        headless_module,
        "sample_liquidity_archives",
        sample_then_replace,
    )

    with pytest.raises(
        SourceArchiveIntegrityError,
        match="SHA-256 differs",
    ):
        headless_module.process_l2_archive_headlessly(
            archive,
            preset,
            batch_size=1,
        )


def test_headless_price_rejects_source_changed_after_streaming(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"
    _write_trade_archive(path)

    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_hour(),
    )
    archive = _archive(path, spec)

    original_stream = headless_module.stream_trade_ohlc_hour

    def stream_then_replace(*args, **kwargs):
        result = original_stream(*args, **kwargs)
        _replace_with_same_size_different_content(path)
        return result

    monkeypatch.setattr(
        headless_module,
        "stream_trade_ohlc_hour",
        stream_then_replace,
    )

    with pytest.raises(
        SourceArchiveIntegrityError,
        match="SHA-256 differs",
    ):
        headless_module.process_price_archives_headlessly(
            spec,
            (archive,),
            batch_size=1,
        )

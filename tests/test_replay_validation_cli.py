from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.ingest.replay_validation import (
    main,
    parse_utc_hour,
)


def _write_update_only(path: Path) -> None:
    table = pa.Table.from_pylist(
        [
            {
                "received_time": 1_788_350_400_001_000_000,
                "event_time": 1_788_350_400_001,
                "transaction_time": 1_788_350_400_000,
                "symbol": "BTCUSDT",
                "event_type": "update",
                "first_update_id": 101,
                "final_update_id": 103,
                "prev_final_update_id": 100,
                "last_update_id": None,
                "side": "bid",
                "price": "60000",
                "quantity": "1",
                "order_count": None,
            }
        ]
    )

    pq.write_table(
        table,
        path,
        compression="zstd",
    )


def test_parse_utc_hour_accepts_canonical_zulu() -> None:
    parsed = parse_utc_hour("2026-09-02T12:00:00Z")

    assert parsed.isoformat() == ("2026-09-02T12:00:00+00:00")


def test_cli_writes_report_for_update_only_archive(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "BTCUSDT_orderbook.parquet"
    report_path = tmp_path / "report.json"
    checkpoint_path = tmp_path / "checkpoint.l2checkpoint"

    _write_update_only(archive)

    status = main(
        [
            "--symbol",
            "BTCUSDT",
            "--archive",
            "2026-09-02T12:00:00Z",
            str(archive),
            "--json-out",
            str(report_path),
            "--checkpoint-out",
            str(checkpoint_path),
            "--batch-size",
            "1",
        ]
    )

    # Validation completed, but an update-only archive cannot initialize.
    assert status == 3
    assert report_path.is_file()
    assert not checkpoint_path.exists()

    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["finally_valid"] is False
    assert payload["final_checkpoint"] is None
    assert payload["checkpoint_output"]["written"] is False
    assert payload["archives"][0]["snapshots_applied"] == 0
    assert payload["archives"][0]["updates_skipped_uninitialized"] == 1


def test_cli_accepts_okx_symbol_and_rejects_wrong_venue_symbol(
    tmp_path: Path,
) -> None:
    import pytest

    from l2shock.ingest.replay_validation import (
        _build_sources,
        build_parser,
    )

    archive = tmp_path / "BTC-USDT-SWAP_orderbook.parquet"
    archive.write_bytes(b"parser-only fixture")

    parser = build_parser()
    args = parser.parse_args(
        [
            "--venue",
            "okx_futures",
            "--symbol",
            "BTC-USDT-SWAP",
            "--archive",
            "2026-09-02T12:00:00Z",
            str(archive),
        ]
    )

    sources = _build_sources(args)

    assert len(sources) == 1
    assert sources[0][1].venue == "okx_futures"
    assert sources[0][1].symbol == "BTC-USDT-SWAP"

    wrong_venue_args = parser.parse_args(
        [
            "--venue",
            "binance_futures",
            "--symbol",
            "BTC-USDT-SWAP",
            "--archive",
            "2026-09-02T12:00:00Z",
            str(archive),
        ]
    )

    with pytest.raises(ValueError):
        _build_sources(wrong_venue_args)

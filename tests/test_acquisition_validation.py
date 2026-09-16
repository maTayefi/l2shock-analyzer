from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from l2shock.acquisition import (
    ParquetValidationError,
    SourceFileSpec,
    quarantine_file,
    sha256_file,
    validate_parquet_file,
)


def _hour() -> datetime:
    return datetime(2026, 9, 2, 12, tzinfo=timezone.utc)


def _orderbook_spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_hour(),
    )


def _trades_spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="trades",
        hour_utc=_hour(),
    )


def _write_orderbook_parquet(path: Path) -> None:
    table = pa.table(
        {
            "received_time": pa.array(
                [1788350400007361359],
                type=pa.int64(),
            ),
            "event_time": pa.array(
                [1788350399886],
                type=pa.int64(),
            ),
            "transaction_time": pa.array(
                [1788350399885],
                type=pa.int64(),
            ),
            "symbol": ["BTCUSDT"],
            "event_type": ["update"],
            "first_update_id": pa.array([100], type=pa.int64()),
            "final_update_id": pa.array([110], type=pa.int64()),
            "prev_final_update_id": pa.array([99], type=pa.int64()),
            "last_update_id": pa.array([None], type=pa.int64()),
            "side": ["bid"],
            "price": ["75000.00"],
            "quantity": ["1.250"],
            "order_count": pa.array([None], type=pa.int64()),
        }
    )

    pq.write_table(table, path, compression="zstd")


def _write_trades_parquet(path: Path) -> None:
    table = pa.table(
        {
            "received_time": pa.array(
                [1788350400007361359],
                type=pa.int64(),
            ),
            "event_time": pa.array(
                [1788350399886],
                type=pa.int64(),
            ),
            "symbol": ["BTCUSDT"],
            "trade_id": ["trade-1"],
            "price": ["75000.00"],
            "quantity": ["0.100"],
            "trade_time": pa.array(
                [1788350399885],
                type=pa.int64(),
            ),
            "is_buyer_maker": [False],
            "order_type": ["market"],
        }
    )

    pq.write_table(table, path, compression="zstd")


def test_valid_orderbook_archive_passes_structural_validation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_orderbook_parquet(path)

    report = validate_parquet_file(path, _orderbook_spec())

    assert report.data_kind == "orderbook"
    assert report.row_count == 1
    assert report.row_group_count == 1
    assert "prev_final_update_id" in report.column_names


def test_valid_trades_archive_passes_structural_validation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_trades.parquet"
    _write_trades_parquet(path)

    report = validate_parquet_file(path, _trades_spec())

    assert report.data_kind == "trades"
    assert report.row_count == 1
    assert "trade_time" in report.column_names


def test_missing_required_columns_are_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalid.parquet"
    pq.write_table(
        pa.table(
            {
                "symbol": ["BTCUSDT"],
                "price": ["75000.00"],
            }
        ),
        path,
    )

    with pytest.raises(
        ParquetValidationError,
        match="missing required",
    ):
        validate_parquet_file(path, _orderbook_spec())


def test_non_parquet_content_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "not_parquet.parquet"
    path.write_bytes(b"<html>not parquet</html>")

    with pytest.raises(
        ParquetValidationError,
        match="Parquet",
    ):
        validate_parquet_file(path, _orderbook_spec())


def test_sha256_reports_exact_bytes(tmp_path: Path) -> None:
    path = tmp_path / "content.bin"
    path.write_bytes(b"abc")

    digest, size = sha256_file(path)

    assert size == 3
    assert digest == (
        "ba7816bf8f01cfea414140de5dae2223" "b00361a396177a9cb410ff61f20015ad"
    )


def test_quarantine_moves_file_and_writes_sidecar(
    tmp_path: Path,
) -> None:
    source = tmp_path / "raw" / "BTCUSDT_orderbook.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"invalid parquet bytes")

    destination = quarantine_file(
        source,
        tmp_path / "quarantine",
        spec=_orderbook_spec(),
        reason="invalid schema",
    )

    assert not source.exists()
    assert destination.exists()
    assert destination.read_bytes() == b"invalid parquet bytes"

    sidecar = destination.with_suffix(destination.suffix + ".quarantine.json")
    assert sidecar.exists()

    sidecar_text = sidecar.read_text(encoding="utf-8")
    assert '"reason": "invalid_schema"' in sidecar_text
    assert '"symbol": "BTCUSDT"' in sidecar_text
    assert "api_key" not in sidecar_text


def test_quarantined_temporary_download_uses_parquet_extension(
    tmp_path: Path,
) -> None:
    source = tmp_path / "raw" / ".BTCUSDT_orderbook.parquet.example-download.part"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"invalid downloaded parquet bytes")

    destination = quarantine_file(
        source,
        tmp_path / "quarantine",
        spec=_orderbook_spec(),
        reason="invalid_downloaded_archive",
    )

    assert not source.exists()
    assert destination.exists()
    assert destination.suffix == ".parquet"
    assert destination.name.startswith("BTCUSDT_orderbook.invalid_downloaded_archive.")
    assert destination.read_bytes() == b"invalid downloaded parquet bytes"

    sidecar = destination.with_suffix(destination.suffix + ".quarantine.json")
    assert sidecar.exists()

    sidecar_text = sidecar.read_text(encoding="utf-8")
    assert (
        '"original_filename": '
        '".BTCUSDT_orderbook.parquet.example-download.part"' in sidecar_text
    )
    assert f'"quarantined_filename": "{destination.name}"' in sidecar_text


def test_download_artifact_attempt_rules(
    tmp_path: Path,
) -> None:
    from l2shock.acquisition import (
        DownloadArtifact,
        DownloadDisposition,
        ParquetValidationReport,
    )

    report = ParquetValidationReport(
        data_kind="orderbook",
        row_count=1,
        row_group_count=1,
        column_names=("symbol",),
        created_by=None,
        format_version=None,
    )

    common = {
        "spec": _orderbook_spec(),
        "local_path": tmp_path / "archive.parquet",
        "file_size_bytes": 1,
        "content_sha256": "a" * 64,
        "validation": report,
    }

    reused = DownloadArtifact(
        **common,
        disposition=DownloadDisposition.REUSED,
        attempts=0,
    )
    assert reused.attempts == 0

    downloaded = DownloadArtifact(
        **common,
        disposition=DownloadDisposition.DOWNLOADED,
        attempts=1,
    )
    assert downloaded.attempts == 1

    with pytest.raises(ValueError, match="reused artifact"):
        DownloadArtifact(
            **common,
            disposition=DownloadDisposition.REUSED,
            attempts=1,
        )

    with pytest.raises(ValueError, match="at least one"):
        DownloadArtifact(
            **common,
            disposition=DownloadDisposition.DOWNLOADED,
            attempts=0,
        )

# l2shock/acquisition/validation.py
"""Local archive hashing, Parquet validation, and quarantine handling."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import pyarrow.parquet as pq

from l2shock.acquisition.errors import (
    DownloadIntegrityError,
    ParquetValidationError,
    QuarantineError,
)
from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.acquisition.results import ParquetValidationReport

_PARQUET_MAGIC: Final[bytes] = b"PAR1"
_HASH_CHUNK_SIZE: Final[int] = 1024 * 1024

_ORDERBOOK_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "received_time",
        "event_time",
        "transaction_time",
        "symbol",
        "event_type",
        "first_update_id",
        "final_update_id",
        "prev_final_update_id",
        "last_update_id",
        "side",
        "price",
        "quantity",
        "order_count",
    }
)

_TRADES_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "received_time",
        "event_time",
        "symbol",
        "trade_id",
        "price",
        "quantity",
        "trade_time",
        "is_buyer_maker",
        "order_type",
    }
)

_SAFE_REASON_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def sha256_file(
    path: Path,
    *,
    chunk_size: int = _HASH_CHUNK_SIZE,
) -> tuple[str, int]:
    """Return lowercase SHA-256 and bytes read for a regular file."""
    source = Path(path)

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    if not source.is_file():
        raise DownloadIntegrityError(f"Expected a regular file: {source}")

    digest = hashlib.sha256()
    byte_count = 0

    try:
        with source.open("rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break

                digest.update(chunk)
                byte_count += len(chunk)

    except OSError as exc:
        raise DownloadIntegrityError(
            f"Could not hash local archive {source.name!r}"
        ) from exc

    if byte_count <= 0:
        raise DownloadIntegrityError(f"Local archive {source.name!r} is empty")

    return digest.hexdigest(), byte_count


def _required_columns(
    data_kind: SourceDataKind,
) -> frozenset[str]:
    if data_kind is SourceDataKind.ORDERBOOK:
        return _ORDERBOOK_REQUIRED_COLUMNS

    if data_kind is SourceDataKind.TRADES:
        return _TRADES_REQUIRED_COLUMNS

    raise RuntimeError(f"Unsupported source data kind: {data_kind!r}")


def _validate_parquet_magic(path: Path) -> None:
    try:
        file_size = path.stat().st_size
    except OSError as exc:
        raise ParquetValidationError(
            f"Could not inspect local archive {path.name!r}"
        ) from exc

    if file_size < 12:
        raise ParquetValidationError(
            f"Archive {path.name!r} is too small to be Parquet"
        )

    try:
        with path.open("rb") as handle:
            prefix = handle.read(4)
            handle.seek(-4, os.SEEK_END)
            suffix = handle.read(4)
    except OSError as exc:
        raise ParquetValidationError(
            f"Could not read Parquet framing for {path.name!r}"
        ) from exc

    if prefix != _PARQUET_MAGIC or suffix != _PARQUET_MAGIC:
        raise ParquetValidationError(
            f"Archive {path.name!r} does not have valid Parquet framing"
        )


def validate_parquet_file(
    path: Path,
    spec: SourceFileSpec,
) -> ParquetValidationReport:
    """Validate footer readability and the minimum normalized schema.

    This is an acquisition-boundary structural check. It does not prove:

    - event ordering;
    - sequence continuity;
    - snapshot availability;
    - symbol consistency in every row;
    - numerical price/quantity validity;
    - reconstructability.

    Those checks belong to the streamed event reader and reconstruction
    validator.
    """
    source = Path(path)

    if not source.is_file():
        raise ParquetValidationError(
            f"Archive does not exist as a regular file: {source.name!r}"
        )

    _validate_parquet_magic(source)

    try:
        parquet_file = pq.ParquetFile(source)
        metadata = parquet_file.metadata
        schema = parquet_file.schema_arrow
    except Exception as exc:
        raise ParquetValidationError(
            f"Could not read Parquet metadata for {source.name!r}"
        ) from exc

    if metadata is None:
        raise ParquetValidationError(f"Archive {source.name!r} has no Parquet metadata")

    row_count = int(metadata.num_rows)
    row_group_count = int(metadata.num_row_groups)

    if row_count <= 0:
        raise ParquetValidationError(f"Archive {source.name!r} contains no rows")

    if row_group_count <= 0:
        raise ParquetValidationError(f"Archive {source.name!r} contains no row groups")

    column_names = tuple(str(name) for name in schema.names)
    actual_columns = set(column_names)
    required_columns = _required_columns(spec.data_kind)
    missing_columns = sorted(required_columns - actual_columns)

    if missing_columns:
        raise ParquetValidationError(
            f"Archive {source.name!r} is missing required "
            f"{spec.data_kind.value} columns: {missing_columns}"
        )

    created_by = str(metadata.created_by) if metadata.created_by is not None else None
    format_version = (
        str(metadata.format_version) if metadata.format_version is not None else None
    )

    return ParquetValidationReport(
        data_kind=spec.data_kind.value,
        row_count=row_count,
        row_group_count=row_group_count,
        column_names=column_names,
        created_by=created_by,
        format_version=format_version,
    )


def _safe_quarantine_reason(reason: str) -> str:
    normalized = _SAFE_REASON_RE.sub(
        "_",
        str(reason or "").strip().lower(),
    ).strip("._-")

    return normalized[:64] or "invalid"


def quarantine_file(
    path: Path,
    quarantine_root: Path,
    *,
    spec: SourceFileSpec,
    reason: str,
) -> Path:
    """Move an unusable local file to deterministic quarantine storage.

    A JSON sidecar is written beside the quarantined file. It contains source
    identity and local diagnostic facts but never credentials.
    """
    source = Path(path).resolve()
    quarantine_base = Path(quarantine_root).expanduser().resolve()

    if not source.is_file():
        raise QuarantineError(f"Cannot quarantine missing file {source.name!r}")

    timestamp = datetime.now(timezone.utc)
    timestamp_text = timestamp.strftime("%Y%m%dT%H%M%S.%fZ")
    safe_reason = _safe_quarantine_reason(reason)
    unique_suffix = uuid.uuid4().hex[:12]

    destination_directory = (
        quarantine_base
        / spec.provider
        / spec.venue
        / spec.hour_utc.strftime("%Y-%m-%d")
        / spec.hour_utc.strftime("%H")
    ).resolve()

    try:
        destination_directory.relative_to(quarantine_base)
    except ValueError as exc:
        raise QuarantineError(
            "Generated quarantine path escaped the configured root"
        ) from exc

    destination_directory.mkdir(parents=True, exist_ok=True)

    # A downloaded temporary file ends in ".part", but quarantine stores the
    # object as the canonical source archive identity. This preserves the
    # expected ".parquet" extension for inspection tools and retention rules.
    # The actual temporary/original filename remains recorded in the sidecar.
    canonical_archive = Path(spec.filename)

    destination = destination_directory / (
        f"{canonical_archive.stem}."
        f"{safe_reason}."
        f"{timestamp_text}."
        f"{unique_suffix}"
        f"{canonical_archive.suffix}"
    )

    try:
        try:
            os.replace(source, destination)
        except OSError:
            # This fallback supports configurations where raw and quarantine
            # directories are on different filesystems.
            shutil.move(str(source), str(destination))
    except Exception as exc:
        raise QuarantineError(
            f"Could not quarantine local archive {source.name!r}"
        ) from exc

    metadata = {
        "quarantined_at_utc": timestamp.isoformat(),
        "reason": safe_reason,
        "provider": spec.provider,
        "venue": spec.venue,
        "symbol": spec.symbol,
        "data_kind": spec.data_kind.value,
        "hour_utc": spec.hour_utc.isoformat(),
        "remote_path": spec.remote_path,
        "original_filename": source.name,
        "quarantined_filename": destination.name,
        "file_size_bytes": destination.stat().st_size,
    }

    sidecar = destination.with_suffix(destination.suffix + ".quarantine.json")
    temporary_sidecar = sidecar.with_suffix(sidecar.suffix + f".{uuid.uuid4().hex}.tmp")

    try:
        temporary_sidecar.write_text(
            json.dumps(
                metadata,
                indent=2,
                ensure_ascii=False,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_sidecar, sidecar)
    except Exception as exc:
        try:
            temporary_sidecar.unlink(missing_ok=True)
        except OSError:
            pass

        raise QuarantineError(
            "File was quarantined, but its diagnostic sidecar "
            f"could not be written for {destination.name!r}"
        ) from exc

    return destination


__all__ = [
    "quarantine_file",
    "sha256_file",
    "validate_parquet_file",
]

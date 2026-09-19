from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.processing import (
    ProcessingCancelledError,
    ProcessingSourceArchive,
    SourceArchiveIntegrityError,
    SourceArchiveMetadataError,
    verify_processing_source_archive,
)


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=datetime(
            2026,
            9,
            2,
            12,
            tzinfo=timezone.utc,
        ),
    )


def _archive(path: Path) -> ProcessingSourceArchive:
    content = path.read_bytes()

    return ProcessingSourceArchive(
        spec=_spec(),
        local_path=path,
        content_sha256=hashlib.sha256(content).hexdigest(),
        file_size_bytes=len(content),
        status=SourceHourStatus.DOWNLOADED,
    )


def test_processing_integrity_accepts_exact_file(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.parquet"
    path.write_bytes(b"immutable-source-content")

    verify_processing_source_archive(_archive(path))


def test_processing_integrity_rejects_modified_bytes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.parquet"
    path.write_bytes(b"original")

    archive = _archive(path)
    path.write_bytes(b"modified")

    with pytest.raises(
        SourceArchiveIntegrityError,
        match="size|SHA-256",
    ):
        verify_processing_source_archive(archive)


def test_processing_integrity_honors_cancellation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.parquet"
    path.write_bytes(b"x" * (2 * 1024 * 1024))

    archive = _archive(path)

    with pytest.raises(
        ProcessingCancelledError,
        match="cancellation",
    ):
        verify_processing_source_archive(
            archive,
            cancellation_probe=lambda: True,
        )


def test_processing_source_archive_rejects_symbolic_link(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.parquet"
    link = tmp_path / "source.parquet"

    target.write_bytes(b"immutable-source-content")

    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("File symlinks are unavailable on this platform")

    with pytest.raises(
        SourceArchiveMetadataError,
        match="symbolic link",
    ):
        ProcessingSourceArchive(
            spec=_spec(),
            local_path=link,
            content_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
            file_size_bytes=target.stat().st_size,
            status=SourceHourStatus.DOWNLOADED,
        )

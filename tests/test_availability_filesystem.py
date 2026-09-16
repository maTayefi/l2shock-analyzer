from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.availability import (
    _local_source_archive_is_available,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        12,
        12,
        tzinfo=timezone.utc,
    )


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )


def _available(
    raw_root: Path,
    *,
    local_path: Path,
    file_size_bytes: object,
    content_sha256: object = "a" * 64,
    status: object = "downloaded",
) -> bool:
    spec = _spec()

    return _local_source_archive_is_available(
        raw_root=raw_root,
        provider=spec.provider,
        venue=spec.venue,
        data_kind=spec.data_kind.value,
        instrument=spec.symbol,
        hour_utc=spec.hour_utc,
        status=status,
        local_path=local_path,
        file_size_bytes=file_size_bytes,
        content_sha256=content_sha256,
    )


def test_canonical_regular_source_file_is_available(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    path = _spec().local_path(raw_root)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"canonical-source")

    assert (
        _available(
            raw_root,
            local_path=path,
            file_size_bytes=path.stat().st_size,
        )
        is True
    )


def test_missing_source_file_is_not_available(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    path = _spec().local_path(raw_root)

    assert (
        _available(
            raw_root,
            local_path=path,
            file_size_bytes=123,
        )
        is False
    )


def test_noncanonical_source_path_is_not_available(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    wrong_path = tmp_path / "outside" / _spec().filename
    wrong_path.parent.mkdir(parents=True)
    wrong_path.write_bytes(b"source")

    assert (
        _available(
            raw_root,
            local_path=wrong_path,
            file_size_bytes=wrong_path.stat().st_size,
        )
        is False
    )


def test_source_size_mismatch_is_not_available(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    path = _spec().local_path(raw_root)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"source")

    assert (
        _available(
            raw_root,
            local_path=path,
            file_size_bytes=path.stat().st_size + 1,
        )
        is False
    )


@pytest.mark.parametrize(
    "digest",
    [
        "",
        "A" * 64,
        "not-a-sha256",
        "a" * 63,
    ],
)
def test_invalid_digest_metadata_is_not_local_evidence(
    tmp_path: Path,
    digest: str,
) -> None:
    raw_root = tmp_path / "raw"
    path = _spec().local_path(raw_root)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"source")

    assert (
        _available(
            raw_root,
            local_path=path,
            file_size_bytes=path.stat().st_size,
            content_sha256=digest,
        )
        is False
    )


def test_unavailable_status_is_not_local_evidence(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    path = _spec().local_path(raw_root)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"source")

    assert (
        _available(
            raw_root,
            local_path=path,
            file_size_bytes=path.stat().st_size,
            status="error",
        )
        is False
    )


def test_symlink_is_not_local_evidence(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    canonical = _spec().local_path(raw_root)
    target = tmp_path / "target.parquet"

    canonical.parent.mkdir(parents=True)
    target.write_bytes(b"source")

    try:
        canonical.symlink_to(target)
    except OSError:
        pytest.skip("File symlinks are unavailable on this platform")

    assert (
        _available(
            raw_root,
            local_path=canonical,
            file_size_bytes=target.stat().st_size,
        )
        is False
    )

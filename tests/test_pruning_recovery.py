from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.pruning_recovery import (
    RawPruningRecoveryError,
    _pruning_candidate,
)


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=datetime(
            2026,
            9,
            12,
            12,
            tzinfo=timezone.utc,
        ),
    )


def test_application_pruning_filename_recovers_exact_source_identity(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    canonical = _spec().local_path(raw_root)
    canonical.parent.mkdir(parents=True)

    pruning = canonical.with_name(f".{canonical.name}.{'a' * 32}.pruning")
    pruning.write_bytes(b"source")

    candidate = _pruning_candidate(
        raw_root,
        pruning,
    )

    assert candidate.spec == _spec()
    assert candidate.path == pruning.resolve()
    assert candidate.canonical_path == canonical.resolve()


@pytest.mark.parametrize(
    "filename",
    [
        ".BTCUSDT_orderbook.parquet.short.pruning",
        ".BTCUSDT_orderbook.parquet." + ("A" * 32) + ".pruning",
        ".BTCUSDT_orderbook.parquet." + ("a" * 31) + ".pruning",
        ".BTCUSDT_orderbook.parquet." + ("a" * 33) + ".pruning",
        "BTCUSDT_orderbook.parquet." + ("a" * 32) + ".pruning",
        ".BTCUSDT_unknown.parquet." + ("a" * 32) + ".pruning",
    ],
)
def test_noncanonical_pruning_filename_is_rejected(
    tmp_path: Path,
    filename: str,
) -> None:
    raw_root = tmp_path / "raw"
    directory = raw_root / "cryptohftdata" / "binance_futures" / "2026-09-12" / "12"
    directory.mkdir(parents=True)
    path = directory / filename
    path.write_bytes(b"source")

    with pytest.raises(RawPruningRecoveryError):
        _pruning_candidate(
            raw_root,
            path,
        )

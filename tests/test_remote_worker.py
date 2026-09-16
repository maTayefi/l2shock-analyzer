# tests/test_remote_worker.py
from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.remote_worker import (
    RemoteWorkerCheckpointBlockedError,
    _canonical_utc_hour,
    _normalized_chain,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        14,
        12,
        tzinfo=timezone.utc,
    )


def test_remote_worker_parses_canonical_utc_hour() -> None:
    assert _canonical_utc_hour("2026-09-14T12:00:00Z") == _hour()


@pytest.mark.parametrize(
    "value",
    (
        "2026-09-14T12:00:00+00:00",
        "2026-09-14T12:30:00Z",
        "2026-09-14",
        "",
    ),
)
def test_remote_worker_rejects_noncanonical_hour(
    value: str,
) -> None:
    with pytest.raises(
        Exception,
    ):
        _canonical_utc_hour(value)


@pytest.mark.parametrize(
    ("venue", "instrument", "price_required"),
    (
        (
            "binance_futures",
            "BTCUSDT",
            True,
        ),
        (
            "binance_futures",
            "ETHUSDT",
            True,
        ),
        (
            "okx_futures",
            "BTC-USDT-SWAP",
            False,
        ),
        (
            "okx_futures",
            "ETH-USDT-SWAP",
            False,
        ),
    ),
)
def test_remote_worker_accepts_only_approved_chains(
    venue: str,
    instrument: str,
    price_required: bool,
) -> None:
    assert _normalized_chain(
        venue,
        instrument,
    ) == (
        venue,
        instrument,
        price_required,
    )


def test_remote_worker_rejects_unproven_venue() -> None:
    with pytest.raises(
        Exception,
        match="Unsupported remote processing chain",
    ):
        _normalized_chain(
            "bybit",
            "BTCUSDT",
        )


def test_checkpoint_blocked_error_is_distinct() -> None:
    assert issubclass(
        RemoteWorkerCheckpointBlockedError,
        RuntimeError,
    )


def test_remote_worker_has_no_database_or_ui_dependency() -> None:
    import l2shock.remote_worker as module

    path = Path(module.__file__)
    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
        ),
        filename=str(path),
    )

    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)

        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    assert not any(
        name == "l2shock.db" or name.startswith("l2shock.db.")
        for name in imported_modules
    )
    assert not any(
        name == "l2shock.ui" or name.startswith("l2shock.ui.")
        for name in imported_modules
    )
    assert "nicegui" not in imported_modules

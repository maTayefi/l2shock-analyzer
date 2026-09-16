# tests/test_remote_worker.py
from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from l2shock.remote_worker import (
    RemoteWorkerCheckpointBlockedError,
    RemoteWorkerError,
    _catch_up_target_from_observations,
    _canonical_utc_hour,
    _normalized_chain,
    _RemoteCatchUpObservation,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        14,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _observation(
    offset: int,
    *,
    l2_exists: bool,
    checkpoint_exists: bool = False,
    price_exists: bool = False,
) -> _RemoteCatchUpObservation:
    return _RemoteCatchUpObservation(
        hour_utc=_hour(offset),
        l2_exists=l2_exists,
        output_checkpoint_exists=checkpoint_exists,
        price_exists=price_exists,
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


def test_binance_catch_up_advances_only_one_hour_after_frontier() -> None:
    selected = _catch_up_target_from_observations(
        venue="binance_futures",
        latest_eligible_hour_utc=_hour(3),
        observations=(
            _observation(
                3,
                l2_exists=False,
            ),
            _observation(
                2,
                l2_exists=False,
            ),
            _observation(
                1,
                l2_exists=True,
                checkpoint_exists=True,
                price_exists=True,
            ),
        ),
        price_required=True,
    )
    assert selected == _hour(2)


def test_binance_repairs_missing_frontier_price_before_advancing() -> None:
    selected = _catch_up_target_from_observations(
        venue="binance_futures",
        latest_eligible_hour_utc=_hour(3),
        observations=(
            _observation(
                3,
                l2_exists=False,
            ),
            _observation(
                2,
                l2_exists=True,
                checkpoint_exists=True,
                price_exists=False,
            ),
        ),
        price_required=True,
    )
    assert selected == _hour(2)


def test_binance_without_bounded_seed_fails_closed() -> None:
    with pytest.raises(
        RemoteWorkerCheckpointBlockedError,
        match="No verified Binance",
    ):
        _catch_up_target_from_observations(
            venue="binance_futures",
            latest_eligible_hour_utc=_hour(2),
            observations=(
                _observation(
                    2,
                    l2_exists=False,
                ),
                _observation(
                    1,
                    l2_exists=False,
                ),
                _observation(
                    0,
                    l2_exists=False,
                ),
            ),
            price_required=True,
        )


def test_okx_without_existing_frontier_selects_oldest_bounded_hour() -> None:
    selected = _catch_up_target_from_observations(
        venue="okx_futures",
        latest_eligible_hour_utc=_hour(2),
        observations=(
            _observation(
                2,
                l2_exists=False,
            ),
            _observation(
                1,
                l2_exists=False,
            ),
            _observation(
                0,
                l2_exists=False,
            ),
        ),
        price_required=False,
    )
    assert selected == _hour(0)


def test_existing_frontier_without_checkpoint_fails_closed() -> None:
    with pytest.raises(
        RemoteWorkerCheckpointBlockedError,
        match="no usable output checkpoint",
    ):
        _catch_up_target_from_observations(
            venue="binance_futures",
            latest_eligible_hour_utc=_hour(1),
            observations=(
                _observation(
                    1,
                    l2_exists=True,
                    checkpoint_exists=False,
                    price_exists=True,
                ),
            ),
            price_required=True,
        )


def test_complete_latest_hour_becomes_idempotent_no_work_probe() -> None:
    selected = _catch_up_target_from_observations(
        venue="binance_futures",
        latest_eligible_hour_utc=_hour(1),
        observations=(
            _observation(
                1,
                l2_exists=True,
                checkpoint_exists=True,
                price_exists=True,
            ),
        ),
        price_required=True,
    )
    assert selected == _hour(1)


def test_catch_up_observations_must_be_contiguous_newest_to_oldest() -> None:
    with pytest.raises(
        RemoteWorkerError,
        match="contiguous",
    ):
        _catch_up_target_from_observations(
            venue="binance_futures",
            latest_eligible_hour_utc=_hour(3),
            observations=(
                _observation(
                    3,
                    l2_exists=False,
                ),
                _observation(
                    1,
                    l2_exists=True,
                    checkpoint_exists=True,
                    price_exists=True,
                ),
            ),
            price_required=True,
        )


def test_checkpoint_cannot_exist_without_l2_artifact() -> None:
    with pytest.raises(
        RemoteWorkerError,
        match="cannot exist without",
    ):
        _observation(
            0,
            l2_exists=False,
            checkpoint_exists=True,
        )

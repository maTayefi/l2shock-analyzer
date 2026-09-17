# tests/test_remote_worker.py
from __future__ import annotations

import ast
import asyncio
from decimal import Decimal

import l2shock.remote_worker as remote_worker_module
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from l2shock.remote_worker import (
    RemoteCatchUpRunResult,
    RemoteWorkerCheckpointBlockedError,
    RemoteWorkerError,
    RemoteWorkerResult,
    _RemoteCatchUpObservation,
    build_parser,
    process_remote_catch_up,
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


def _worker_result(
    hour_utc: datetime,
    *,
    venue: str = "okx_futures",
    instrument: str = "BTC-USDT-SWAP",
) -> RemoteWorkerResult:
    return RemoteWorkerResult(
        venue=venue,
        instrument=instrument,
        hour_utc=hour_utc,
        pinned_input_revision="a" * 40,
        l2_created=True,
        l2_revision="b" * 40,
        l2_reused=False,
        price_required=False,
        price_created=False,
        price_revision=None,
        price_reused=False,
        source_downloaded_count=1,
        source_reused_count=0,
    )


class _Clock:
    def __init__(self, *values: float) -> None:
        self._values = iter(values)
        self._last = 0.0

    def __call__(self) -> float:
        try:
            self._last = next(self._values)
        except StopIteration:
            pass

        return self._last


def test_bounded_catch_up_processes_contiguous_hours_until_caught_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = _hour(1)
    latest = _hour(3)
    observed: list[datetime] = []

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return selected

    async def fake_process(**kwargs: object) -> RemoteWorkerResult:
        hour = kwargs["hour_utc"]
        assert isinstance(hour, datetime)
        observed.append(hour)
        return _worker_result(hour)

    monkeypatch.setattr(
        remote_worker_module,
        "select_remote_catch_up_hour",
        fake_select,
    )
    monkeypatch.setattr(
        remote_worker_module,
        "process_remote_hour",
        fake_process,
    )

    result = asyncio.run(
        process_remote_catch_up(
            repository=object(),  # type: ignore[arg-type]
            cryptohft=object(),  # type: ignore[arg-type]
            workspace=object(),  # type: ignore[arg-type]
            venue="okx_futures",
            instrument="BTC-USDT-SWAP",
            latest_eligible_hour_utc=latest,
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
            search_hours=72,
            max_hours_per_run=4,
            max_runtime_minutes=240,
            producer_git_commit=None,
            _monotonic=_Clock(0.0),
        )
    )

    assert observed == [
        _hour(1),
        _hour(2),
        _hour(3),
    ]
    assert isinstance(result, RemoteCatchUpRunResult)
    assert result.stop_reason == "caught_up"
    assert result.completed_hour_count == 3
    assert result.first_hour_utc == _hour(1)
    assert result.last_hour_utc == _hour(3)


def test_bounded_catch_up_stops_at_max_hours_per_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[datetime] = []

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return _hour(0)

    async def fake_process(**kwargs: object) -> RemoteWorkerResult:
        hour = kwargs["hour_utc"]
        assert isinstance(hour, datetime)
        observed.append(hour)
        return _worker_result(hour)

    monkeypatch.setattr(
        remote_worker_module,
        "select_remote_catch_up_hour",
        fake_select,
    )
    monkeypatch.setattr(
        remote_worker_module,
        "process_remote_hour",
        fake_process,
    )

    result = asyncio.run(
        process_remote_catch_up(
            repository=object(),  # type: ignore[arg-type]
            cryptohft=object(),  # type: ignore[arg-type]
            workspace=object(),  # type: ignore[arg-type]
            venue="okx_futures",
            instrument="BTC-USDT-SWAP",
            latest_eligible_hour_utc=_hour(10),
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
            search_hours=72,
            max_hours_per_run=4,
            max_runtime_minutes=240,
            producer_git_commit=None,
            _monotonic=_Clock(0.0),
        )
    )

    assert observed == [
        _hour(0),
        _hour(1),
        _hour(2),
        _hour(3),
    ]
    assert result.stop_reason == "max_hours_per_run"
    assert result.completed_hour_count == 4


def test_bounded_catch_up_stops_before_admitting_after_runtime_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[datetime] = []

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return _hour(0)

    async def fake_process(**kwargs: object) -> RemoteWorkerResult:
        hour = kwargs["hour_utc"]
        assert isinstance(hour, datetime)
        observed.append(hour)
        return _worker_result(hour)

    monkeypatch.setattr(
        remote_worker_module,
        "select_remote_catch_up_hour",
        fake_select,
    )
    monkeypatch.setattr(
        remote_worker_module,
        "process_remote_hour",
        fake_process,
    )

    # Start at zero. After the first completed hour, the clock reports that
    # more than the one-minute budget has elapsed.
    result = asyncio.run(
        process_remote_catch_up(
            repository=object(),  # type: ignore[arg-type]
            cryptohft=object(),  # type: ignore[arg-type]
            workspace=object(),  # type: ignore[arg-type]
            venue="okx_futures",
            instrument="BTC-USDT-SWAP",
            latest_eligible_hour_utc=_hour(5),
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
            search_hours=72,
            max_hours_per_run=4,
            max_runtime_minutes=1,
            producer_git_commit=None,
            _monotonic=_Clock(0.0, 61.0),
        )
    )

    assert observed == [_hour(0)]
    assert result.stop_reason == "runtime_budget"
    assert result.completed_hour_count == 1


def test_bounded_catch_up_never_skips_failed_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[datetime] = []

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return _hour(0)

    async def fake_process(**kwargs: object) -> RemoteWorkerResult:
        hour = kwargs["hour_utc"]
        assert isinstance(hour, datetime)
        observed.append(hour)

        if hour == _hour(1):
            raise RemoteWorkerCheckpointBlockedError("simulated predecessor failure")

        return _worker_result(hour)

    monkeypatch.setattr(
        remote_worker_module,
        "select_remote_catch_up_hour",
        fake_select,
    )
    monkeypatch.setattr(
        remote_worker_module,
        "process_remote_hour",
        fake_process,
    )

    with pytest.raises(
        RemoteWorkerCheckpointBlockedError,
        match="simulated predecessor failure",
    ):
        asyncio.run(
            process_remote_catch_up(
                repository=object(),  # type: ignore[arg-type]
                cryptohft=object(),  # type: ignore[arg-type]
                workspace=object(),  # type: ignore[arg-type]
                venue="okx_futures",
                instrument="BTC-USDT-SWAP",
                latest_eligible_hour_utc=_hour(5),
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.01"),
                search_hours=72,
                max_hours_per_run=4,
                max_runtime_minutes=240,
                producer_git_commit=None,
                _monotonic=_Clock(0.0),
            )
        )

    assert observed == [
        _hour(0),
        _hour(1),
    ]


def test_remote_worker_parser_accepts_bounded_run_arguments() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "--venue",
            "okx_futures",
            "--instrument",
            "BTC-USDT-SWAP",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
            "--catch-up-hours",
            "168",
            "--max-hours-per-run",
            "6",
            "--max-runtime-minutes",
            "180",
        ]
    )

    assert args.catch_up_hours == 168
    assert args.max_hours_per_run == 6
    assert args.max_runtime_minutes == 180


@pytest.mark.parametrize(
    ("argument", "value"),
    (
        ("--max-hours-per-run", "0"),
        ("--max-hours-per-run", "-1"),
        ("--max-runtime-minutes", "0"),
        ("--max-runtime-minutes", "-1"),
    ),
)
def test_remote_worker_parser_rejects_invalid_bounded_run_arguments(
    argument: str,
    value: str,
) -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--venue",
                "okx_futures",
                "--instrument",
                "BTC-USDT-SWAP",
                "--depth-lower",
                "0",
                "--depth-upper",
                "0.01",
                argument,
                value,
            ]
        )


def test_catch_up_result_serializes_all_completed_hours() -> None:
    result = RemoteCatchUpRunResult(
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        latest_eligible_hour_utc=_hour(2),
        results=(
            _worker_result(_hour(0)),
            _worker_result(_hour(1)),
        ),
        max_hours_per_run=2,
        max_runtime_minutes=240,
        stop_reason="max_hours_per_run",
    )

    payload = result.to_dict()

    assert payload["schema"] == "l2shock.remote_catch_up_run_result"
    assert payload["schema_version"] == 1
    assert payload["completed_hour_count"] == 2
    assert payload["first_hour_utc"] == "2026-09-14T12:00:00Z"
    assert payload["last_hour_utc"] == "2026-09-14T13:00:00Z"
    assert len(payload["hours"]) == 2


def test_caught_up_result_requires_latest_completed_hour() -> None:
    with pytest.raises(
        RemoteWorkerError,
        match="caught_up requires",
    ):
        RemoteCatchUpRunResult(
            venue="okx_futures",
            instrument="BTC-USDT-SWAP",
            latest_eligible_hour_utc=_hour(2),
            results=(_worker_result(_hour(0)),),
            max_hours_per_run=4,
            max_runtime_minutes=240,
            stop_reason="caught_up",
        )

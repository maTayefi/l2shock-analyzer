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
    _canonical_utc_hour,
    _catch_up_target_from_observations,
    _normalized_chain,
    build_parser,
    process_remote_catch_up,
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
            "bybit",
            "BTCUSDT",
            False,
        ),
        (
            "bybit",
            "ETHUSDT",
            False,
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
            "bitget_futures",
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


def test_binance_without_bounded_seed_falls_back_to_oldest_for_self_init() -> None:
    selected = _catch_up_target_from_observations(
        venue="binance_futures",
        latest_eligible_hour_utc=_hour(2),
        observations=(
            _observation(
                2,
                l2_exists=False,
                checkpoint_exists=False,
                price_exists=False,
            ),
            _observation(
                1,
                l2_exists=False,
                checkpoint_exists=False,
                price_exists=False,
            ),
            _observation(
                0,
                l2_exists=False,
                checkpoint_exists=False,
                price_exists=False,
            ),
        ),
        price_required=True,
    )
    assert selected == _hour(0)
    assert selected == _hour(0)


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


def test_bybit_without_existing_frontier_selects_oldest_for_strict_attempt() -> None:
    selected = _catch_up_target_from_observations(
        venue="bybit",
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


def test_existing_artifact_without_any_usable_frontier_is_blocked() -> None:
    with pytest.raises(
        RemoteWorkerCheckpointBlockedError,
        match="no older usable checkpoint frontier",
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


def test_blocked_artifact_is_skipped_after_usable_frontier() -> None:
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
                checkpoint_exists=False,
                price_exists=True,
            ),
            _observation(
                0,
                l2_exists=True,
                checkpoint_exists=True,
                price_exists=True,
            ),
        ),
        price_required=True,
    )
    assert selected == _hour(2)


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


def test_bounded_catch_up_replans_each_hour_until_caught_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _hour(3)
    observed: list[datetime] = []

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return _hour(len(observed) + 1)

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


def test_catch_up_reports_source_unavailable_after_completed_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _hour(3)
    observed: list[datetime] = []

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return _hour(len(observed) + 1)

    async def fake_process(**kwargs: object) -> RemoteWorkerResult:
        hour = kwargs["hour_utc"]
        assert isinstance(hour, datetime)
        observed.append(hour)

        if hour == latest:
            raise remote_worker_module.RemoteFileNotFoundError(
                "simulated unpublished source archive"
            )

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
    assert result.stop_reason == "source_unavailable"
    assert result.completed_hour_count == 2
    assert result.first_hour_utc == _hour(1)
    assert result.last_hour_utc == _hour(2)


def test_catch_up_reports_no_work_when_first_source_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _hour(3)

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return target

    async def fake_process(**kwargs: object) -> RemoteWorkerResult:
        assert kwargs["hour_utc"] == target
        raise remote_worker_module.RemoteFileNotFoundError(
            "simulated unpublished source archive"
        )

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
            latest_eligible_hour_utc=target,
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
            search_hours=72,
            max_hours_per_run=4,
            max_runtime_minutes=240,
            producer_git_commit=None,
            _monotonic=_Clock(0.0),
        )
    )

    assert result.stop_reason == "no_work"
    assert result.completed_hour_count == 0
    assert result.first_hour_utc is None
    assert result.last_hour_utc is None
    assert result.to_dict()["hours"] == []


def test_bounded_catch_up_stops_at_max_hours_per_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[datetime] = []

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return _hour(len(observed))

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
    select_call_count = [0]

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        call_index = select_call_count[0]
        select_call_count[0] += 1
        if call_index == 0:
            return _hour(0)
        return _hour(1)

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
    with pytest.raises(RemoteWorkerError, match="caught_up requires"):
        RemoteCatchUpRunResult(
            venue="okx_futures",
            instrument="BTC-USDT-SWAP",
            latest_eligible_hour_utc=_hour(2),
            results=(_worker_result(_hour(0)),),
            max_hours_per_run=4,
            max_runtime_minutes=240,
            stop_reason="caught_up",
        )


def test_source_unavailable_preserves_completed_hours_below_latest() -> None:
    result = RemoteCatchUpRunResult(
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        latest_eligible_hour_utc=_hour(2),
        results=(
            _worker_result(_hour(0)),
            _worker_result(_hour(1)),
        ),
        max_hours_per_run=4,
        max_runtime_minutes=240,
        stop_reason="source_unavailable",
    )

    payload = result.to_dict()

    assert result.completed_hour_count == 2
    assert result.first_hour_utc == _hour(0)
    assert result.last_hour_utc == _hour(1)
    assert payload["stop_reason"] == "source_unavailable"
    assert payload["first_hour_utc"] == "2026-09-14T12:00:00Z"
    assert payload["last_hour_utc"] == "2026-09-14T13:00:00Z"


def test_no_work_allows_empty_result_and_serializes_null_boundaries() -> None:
    result = RemoteCatchUpRunResult(
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        latest_eligible_hour_utc=_hour(2),
        results=(),
        max_hours_per_run=4,
        max_runtime_minutes=240,
        stop_reason="no_work",
    )

    payload = result.to_dict()

    assert result.completed_hour_count == 0
    assert result.first_hour_utc is None
    assert result.last_hour_utc is None
    assert payload["first_hour_utc"] is None
    assert payload["last_hour_utc"] is None
    assert payload["hours"] == []


def test_catch_up_replanning_skips_immutable_blocked_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[datetime] = []
    selected_hours = iter(
        (
            _hour(0),
            _hour(2),
        )
    )

    async def fake_select(**kwargs: object) -> datetime:
        del kwargs
        return next(selected_hours)

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
            latest_eligible_hour_utc=_hour(2),
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
        _hour(2),
    ]
    assert result.stop_reason == "caught_up"
    assert result.completed_hour_count == 2
    assert result.first_hour_utc == _hour(0)
    assert result.last_hour_utc == _hour(2)


def test_remote_worker_default_search_reaches_historical_seeds() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "--venue",
            "bybit",
            "--instrument",
            "BTCUSDT",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
        ]
    )

    assert args.catch_up_hours == 720
    assert args.max_hours_per_run == 4
    assert args.max_runtime_minutes == 240


def test_remote_worker_routes_l2_through_blocked_marker_policy() -> None:
    source = Path(remote_worker_module.__file__).read_text(
        encoding="utf-8",
    )

    assert "_l2_artifact_for_publication(" in source
    assert "REMOTE L2 BLOCKED-HOUR MARKER" in source
    assert "checkpoint_published=false" in source


def test_blocked_artifact_without_frontier_allows_later_self_init_attempt() -> None:
    selected = _catch_up_target_from_observations(
        venue="okx_futures",
        latest_eligible_hour_utc=_hour(1),
        observations=(
            _observation(
                1,
                l2_exists=False,
                checkpoint_exists=False,
                price_exists=False,
            ),
            _observation(
                0,
                l2_exists=True,
                checkpoint_exists=False,
                price_exists=False,
            ),
        ),
        price_required=False,
    )

    assert selected == _hour(1)


async def test_predecessor_artifact_exists_distinguishes_absent_predecessor() -> None:
    import inspect

    import l2shock.remote_worker as worker_module
    from l2shock.remote.hf_repository import HuggingFaceArtifactNotFoundError

    class _Absent:
        def download_l2_predecessor_checkpoint(self, key, *, revision):
            raise HuggingFaceArtifactNotFoundError("absent")

    class _Present:
        def download_l2_predecessor_checkpoint(self, key, *, revision):
            return object()

    kwargs = {"target_key": object(), "pinned_revision": "a" * 40}

    assert (
        await worker_module._predecessor_artifact_exists(_Absent(), **kwargs) is False
    )
    assert (
        await worker_module._predecessor_artifact_exists(_Present(), **kwargs) is True
    )

    source = inspect.getsource(worker_module.process_remote_hour)
    assert "predecessor_artifact_present" in source
    assert "refusing" in source


def test_planner_repairs_price_on_blocked_hour_after_frontier() -> None:
    from datetime import datetime, timedelta, timezone

    from l2shock.remote_worker import (
        _catch_up_target_from_observations,
        _RemoteCatchUpObservation,
    )

    latest = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)
    observations = (
        _RemoteCatchUpObservation(
            hour_utc=latest,
            l2_exists=False,
            output_checkpoint_exists=False,
            price_exists=False,
        ),
        _RemoteCatchUpObservation(
            hour_utc=latest - timedelta(hours=1),
            l2_exists=True,
            output_checkpoint_exists=False,
            price_exists=False,
        ),
        _RemoteCatchUpObservation(
            hour_utc=latest - timedelta(hours=2),
            l2_exists=True,
            output_checkpoint_exists=True,
            price_exists=True,
        ),
    )

    assert _catch_up_target_from_observations(
        venue="binance_futures",
        latest_eligible_hour_utc=latest,
        observations=observations,
        price_required=True,
    ) == latest - timedelta(hours=1)


def test_planner_repairs_price_on_blocked_hour_without_frontier() -> None:
    from datetime import datetime, timedelta, timezone

    from l2shock.remote_worker import (
        _catch_up_target_from_observations,
        _RemoteCatchUpObservation,
    )

    latest = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)
    observations = (
        _RemoteCatchUpObservation(
            hour_utc=latest,
            l2_exists=True,
            output_checkpoint_exists=False,
            price_exists=True,
        ),
        _RemoteCatchUpObservation(
            hour_utc=latest - timedelta(hours=1),
            l2_exists=True,
            output_checkpoint_exists=False,
            price_exists=False,
        ),
    )

    assert _catch_up_target_from_observations(
        venue="binance_futures",
        latest_eligible_hour_utc=latest,
        observations=observations,
        price_required=True,
    ) == latest - timedelta(hours=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("worker_fails", (False, True))
async def test_joined_remote_worker_preserves_normal_outcome_and_context(
    worker_fails: bool,
) -> None:
    from contextvars import ContextVar

    context = ContextVar("remote-worker-test-context", default="missing")
    token = context.set("owner-context")

    def work(value: int, *, increment: int) -> int:
        assert context.get() == "owner-context"

        if worker_fails:
            raise ValueError("simulated worker failure")

        return value + increment

    try:
        if worker_fails:
            with pytest.raises(ValueError, match="simulated worker failure"):
                await remote_worker_module._to_thread_joined(
                    work,
                    40,
                    increment=2,
                )
        else:
            assert (
                await remote_worker_module._to_thread_joined(
                    work,
                    40,
                    increment=2,
                )
                == 42
            )
    finally:
        context.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("worker_fails", (False, True))
async def test_remote_worker_cancellation_retains_workspace_until_thread_exit(
    tmp_path: Path,
    worker_fails: bool,
) -> None:
    import threading
    from tempfile import TemporaryDirectory

    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    workspace_closed = threading.Event()
    paths: list[Path] = []

    def work(root: Path) -> None:
        entered.set()

        try:
            if not release.wait(timeout=5.0):
                raise TimeoutError("Test did not release the remote worker")

            # The owner's workspace must still exist when the worker resumes.
            (root / "completed.txt").write_text("completed", encoding="utf-8")

            if worker_fails:
                raise ValueError("simulated worker failure")
        finally:
            exited.set()

    async def owner() -> None:
        try:
            with TemporaryDirectory(dir=tmp_path) as directory:
                root = Path(directory)
                paths.append(root)
                await remote_worker_module._to_thread_joined(work, root)
        finally:
            workspace_closed.set()

    task = asyncio.create_task(owner())

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert not task.done()
        assert not exited.is_set()
        assert not workspace_closed.is_set()
        assert paths[0].is_dir()
    finally:
        release.set()

        if not task.done():
            task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3.0)

    assert exited.is_set()
    assert workspace_closed.is_set()
    assert not paths[0].exists()


def test_remote_worker_routes_thread_calls_through_joining_helper() -> None:
    source = Path(remote_worker_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    helper = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_to_thread_joined"
    )
    direct_thread_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
        and node.func.attr == "to_thread"
    ]
    helper_node_ids = {id(node) for node in ast.walk(helper)}

    assert len(direct_thread_calls) == 1
    assert all(id(node) in helper_node_ids for node in direct_thread_calls)


@pytest.mark.asyncio
async def test_catch_up_failure_log_does_not_render_exception_details(
    monkeypatch,
    caplog,
) -> None:
    import logging

    private_message = "private-worker-detail-must-not-escape"
    private_cause = "private-worker-sdk-cause-must-not-escape"

    failure = RuntimeError(private_message)
    failure.__cause__ = ValueError(private_cause)

    target = _hour()

    async def select_target(**_kwargs):
        return target

    async def fail_processing(**_kwargs):
        raise failure

    monkeypatch.setattr(
        remote_worker_module,
        "select_remote_catch_up_hour",
        select_target,
    )
    monkeypatch.setattr(
        remote_worker_module,
        "process_remote_hour",
        fail_processing,
    )

    with caplog.at_level(
        logging.ERROR,
        logger=remote_worker_module.__name__,
    ):
        with pytest.raises(RuntimeError) as caught:
            await process_remote_catch_up(
                repository=object(),
                cryptohft=object(),
                workspace=object(),
                venue="okx_futures",
                instrument="BTC-USDT-SWAP",
                latest_eligible_hour_utc=target,
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.01"),
                search_hours=72,
                max_hours_per_run=1,
                max_runtime_minutes=1,
                producer_git_commit=None,
            )

    assert caught.value is failure

    records = [
        record
        for record in caplog.records
        if "PROCESSING HOUR FAILED" in record.getMessage()
    ]

    assert len(records) == 1
    assert "RuntimeError" in records[0].getMessage()
    assert "okx_futures" in records[0].getMessage()
    assert "BTC-USDT-SWAP" in records[0].getMessage()
    assert records[0].exc_info is None

    assert private_message not in caplog.text
    assert private_cause not in caplog.text


@pytest.mark.parametrize(
    "error_name",
    (
        "RemoteRequestError",
        "DownloadIntegrityError",
        "InsufficientDiskSpaceError",
    ),
)
def test_audit_worker_operational_acquisition_has_distinct_failed_exit(
    monkeypatch,
    capsys,
    error_name,
):
    import l2shock.remote_worker as worker

    error_type = getattr(worker, error_name)
    private_message = "audit-private-acquisition-message"
    private_cause = "audit-private-acquisition-cause"
    failure = error_type(private_message)
    failure.__cause__ = RuntimeError(private_cause)

    async def fail_run(_arguments):
        raise failure

    monkeypatch.setattr(worker, "_run_from_arguments", fail_run)

    status = worker.main(
        [
            "--venue",
            "okx_futures",
            "--instrument",
            "BTC-USDT-SWAP",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
            "--hour",
            "2026-10-01T12:00:00Z",
        ]
    )

    captured = capsys.readouterr()
    rendered = captured.out + captured.err

    assert status == int(worker.RemoteWorkerExitStatus.ACQUISITION_OPERATIONAL_ERROR)
    assert status == 7
    assert status != int(worker.RemoteWorkerExitStatus.OK)
    assert error_name in captured.err
    assert "Remote source acquisition failed" in captured.err
    assert private_message not in rendered
    assert private_cause not in rendered


def test_audit_worker_source_absence_keeps_existing_exit_status(
    monkeypatch,
    capsys,
):
    import l2shock.remote_worker as worker

    async def fail_run(_arguments):
        raise worker.RemoteFileNotFoundError("Source is unavailable")

    monkeypatch.setattr(worker, "_run_from_arguments", fail_run)

    status = worker.main(
        [
            "--venue",
            "okx_futures",
            "--instrument",
            "BTC-USDT-SWAP",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
            "--hour",
            "2026-10-01T12:00:00Z",
        ]
    )

    assert status == int(worker.RemoteWorkerExitStatus.SOURCE_UNAVAILABLE)
    assert status == 3
    assert "not currently available" in capsys.readouterr().err


def test_audit_worker_contract_failure_keeps_existing_exit_status(
    monkeypatch,
    capsys,
):
    import l2shock.remote_worker as worker

    async def fail_run(_arguments):
        raise worker.RemoteWorkerError("Simulated contract failure")

    monkeypatch.setattr(worker, "_run_from_arguments", fail_run)

    status = worker.main(
        [
            "--venue",
            "okx_futures",
            "--instrument",
            "BTC-USDT-SWAP",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
            "--hour",
            "2026-10-01T12:00:00Z",
        ]
    )

    assert status == int(worker.RemoteWorkerExitStatus.INPUT_OR_CONTRACT_ERROR)
    assert status == 2
    assert "contract failed" in capsys.readouterr().err


def _audit_4b_catch_up_arguments():
    from decimal import Decimal

    return {
        "repository": object(),
        "cryptohft": object(),
        "workspace": object(),
        "venue": "okx_futures",
        "instrument": "BTC-USDT-SWAP",
        "latest_eligible_hour_utc": _hour(2),
        "lower_fraction": Decimal("0"),
        "upper_fraction": Decimal("0.01"),
        "search_hours": 72,
        "max_hours_per_run": 3,
        "max_runtime_minutes": 240,
        "producer_git_commit": None,
        "_monotonic": lambda: 0.0,
    }


def _audit_4b_failure_payloads(text):
    import json

    payloads = []

    for line in text.splitlines():
        if not line.lstrip().startswith("{"):
            continue

        try:
            value = json.loads(line)
        except ValueError:
            continue

        if (
            isinstance(value, dict)
            and value.get("schema") == "l2shock.remote_catch_up_failure"
        ):
            payloads.append(value)

    return payloads


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scenario", "expected_stage", "expected_completed"),
    (
        ("selection", "select_target", 0),
        ("first_processing", "process_hour", 0),
        ("later_processing", "process_hour", 1),
        ("replanning", "replan", 1),
        ("nonadvancing", "replan", 1),
        ("future_target", "replan", 1),
    ),
)
async def test_audit_4b_failed_catch_up_reports_completed_operations(
    monkeypatch,
    capsys,
    caplog,
    scenario,
    expected_stage,
    expected_completed,
):
    import logging

    import l2shock.remote_worker as worker

    private_message = "audit-4b-private-exception-message"
    private_cause = "audit-4b-private-exception-cause"
    failure = RuntimeError(private_message)
    failure.__cause__ = ValueError(private_cause)

    selection_calls = 0
    attempted = []

    async def select_target(**_arguments):
        nonlocal selection_calls
        selection_calls += 1

        if scenario == "selection":
            raise failure

        if selection_calls == 1:
            return _hour(0)

        if scenario == "replanning":
            raise failure

        if scenario == "nonadvancing":
            return _hour(0)

        if scenario == "future_target":
            return _hour(3)

        return _hour(1)

    async def process_hour(**arguments):
        hour = arguments["hour_utc"]
        attempted.append(hour)

        if scenario == "first_processing":
            raise failure

        if scenario == "later_processing" and hour == _hour(1):
            raise failure

        return _worker_result(hour)

    monkeypatch.setattr(worker, "select_remote_catch_up_hour", select_target)
    monkeypatch.setattr(worker, "process_remote_hour", process_hour)

    expected_type = (
        worker.RemoteWorkerError
        if scenario in {"nonadvancing", "future_target"}
        else RuntimeError
    )

    with caplog.at_level(logging.WARNING, logger=worker.__name__):
        with pytest.raises(expected_type) as captured:
            await worker.process_remote_catch_up(**_audit_4b_catch_up_arguments())

    if scenario not in {"nonadvancing", "future_target"}:
        assert captured.value is failure

    output = capsys.readouterr()
    payloads = _audit_4b_failure_payloads(output.out)
    assert len(payloads) == 1

    payload = payloads[0]
    assert payload["schema_version"] == 1
    assert payload["status"] == "failed"
    assert payload["venue"] == "okx_futures"
    assert payload["instrument"] == "BTC-USDT-SWAP"
    assert payload["completed_hour_count"] == expected_completed
    assert payload["failure"]["stage"] == expected_stage
    assert payload["failure"]["error_type"] == expected_type.__name__
    assert payload["completion_policy"] == "returned_hour_operations_only"

    assert payload["hours"] == (
        [_worker_result(_hour(0)).to_dict()] if expected_completed else []
    )

    if expected_completed:
        expected_last = _hour(0).isoformat().replace("+00:00", "Z")
        assert payload["first_hour_utc"] == expected_last
        assert payload["last_hour_utc"] == expected_last
        assert payload["failure"]["after_completed_hour_utc"] == expected_last
    else:
        assert payload["first_hour_utc"] is None
        assert payload["last_hour_utc"] is None
        assert payload["failure"]["after_completed_hour_utc"] is None

    if expected_stage == "process_hour":
        failed_hour = _hour(1) if expected_completed else _hour(0)
        assert payload["failure"]["failed_hour_utc"] == (
            failed_hour.isoformat().replace("+00:00", "Z")
        )
        assert payload["failed_hour_publication_state"] == "not_determined"
    else:
        assert payload["failure"]["failed_hour_utc"] is None
        assert payload["failed_hour_publication_state"] is None

    if scenario == "selection":
        assert attempted == []
    elif scenario == "later_processing":
        assert attempted == [_hour(0), _hour(1)]
    else:
        assert attempted == [_hour(0)]

    rendered = output.out + output.err + caplog.text
    assert private_message not in rendered
    assert private_cause not in rendered

    records = [record for record in caplog.records if record.name == worker.__name__]
    assert all(record.exc_info is None for record in records)

    assert "timing" in payload
    assert payload["timing"]["total_seconds"] >= 0


@pytest.mark.parametrize(
    ("error_name", "expected_status"),
    (
        ("RemoteRequestError", 7),
        ("DownloadIntegrityError", 7),
        ("InsufficientDiskSpaceError", 7),
        ("RemoteWorkerCheckpointBlockedError", 4),
        ("HuggingFaceRepositoryError", 5),
        ("B2TransportError", 6),
        ("RuntimeError", 1),
    ),
)
def test_audit_4b_cli_preserves_original_failure_classification(
    monkeypatch,
    capsys,
    error_name,
    expected_status,
):
    import l2shock.remote_worker as worker

    error_type = (
        RuntimeError
        if error_name == "RuntimeError"
        else getattr(
            worker,
            error_name,
        )
    )
    failure = error_type("Simulated catch-up failure")
    attempted = []
    propagated = []

    async def select_target(**_arguments):
        return _hour(len(attempted))

    async def process_hour(**arguments):
        hour = arguments["hour_utc"]
        attempted.append(hour)

        if hour == _hour(1):
            raise failure

        return _worker_result(hour)

    async def run_arguments(_arguments):
        try:
            return await worker.process_remote_catch_up(
                **_audit_4b_catch_up_arguments()
            )
        except Exception as exc:
            propagated.append(exc)
            raise

    monkeypatch.setattr(worker, "select_remote_catch_up_hour", select_target)
    monkeypatch.setattr(worker, "process_remote_hour", process_hour)
    monkeypatch.setattr(worker, "_run_from_arguments", run_arguments)

    status = worker.main(
        [
            "--venue",
            "okx_futures",
            "--instrument",
            "BTC-USDT-SWAP",
            "--depth-lower",
            "0",
            "--depth-upper",
            "0.01",
        ]
    )

    output = capsys.readouterr()
    payloads = _audit_4b_failure_payloads(output.out)

    assert status == expected_status
    assert status != int(worker.RemoteWorkerExitStatus.OK)
    assert propagated == [failure]
    assert propagated[0] is failure
    assert attempted == [_hour(0), _hour(1)]

    assert len(payloads) == 1
    assert payloads[0]["status"] == "failed"
    assert payloads[0]["completed_hour_count"] == 1
    assert payloads[0]["failure"]["error_type"] == error_name
    assert payloads[0]["hours"] == [_worker_result(_hour(0)).to_dict()]

    assert output.err


@pytest.mark.asyncio
async def test_audit_4b_reporting_failure_preserves_original_exception(
    monkeypatch,
    caplog,
):
    import logging

    import l2shock.remote_worker as worker

    failure = worker.RemoteRequestError("audit-4b-private-original-request-detail")

    async def select_target(**_arguments):
        return _hour(0)

    async def process_hour(**_arguments):
        raise failure

    def broken_print(*_arguments, **_keywords):
        raise OSError("audit-4b-private-output-stream-detail")

    monkeypatch.setattr(worker, "select_remote_catch_up_hour", select_target)
    monkeypatch.setattr(worker, "process_remote_hour", process_hour)
    monkeypatch.setattr(worker, "print", broken_print, raising=False)

    with caplog.at_level(logging.WARNING, logger=worker.__name__):
        with pytest.raises(worker.RemoteRequestError) as captured:
            await worker.process_remote_catch_up(**_audit_4b_catch_up_arguments())

    assert captured.value is failure
    assert "Could not emit remote catch-up failure summary" in caplog.text
    assert "error_type=OSError" in caplog.text
    assert "audit-4b-private-original-request-detail" not in caplog.text
    assert "audit-4b-private-output-stream-detail" not in caplog.text

    records = [record for record in caplog.records if record.name == worker.__name__]
    assert all(record.exc_info is None for record in records)


@pytest.mark.asyncio
async def test_audit_4b_successful_catch_up_emits_no_failure_record(
    monkeypatch,
    capsys,
):
    import l2shock.remote_worker as worker

    attempted = []

    async def select_target(**_arguments):
        return _hour(len(attempted))

    async def process_hour(**arguments):
        hour = arguments["hour_utc"]
        attempted.append(hour)
        return _worker_result(hour)

    monkeypatch.setattr(worker, "select_remote_catch_up_hour", select_target)
    monkeypatch.setattr(worker, "process_remote_hour", process_hour)

    result = await worker.process_remote_catch_up(**_audit_4b_catch_up_arguments())

    assert result.stop_reason == "caught_up"
    assert result.completed_hour_count == 3
    assert attempted == [_hour(0), _hour(1), _hour(2)]
    assert _audit_4b_failure_payloads(capsys.readouterr().out) == []

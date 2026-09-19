# tests/test_remote_import_runtime.py
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from l2shock.presets import (
    build_binance_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.remote import (
    RemoteArtifactImportResult,
    RemoteArtifactKey,
    RemoteArtifactKind,
)
from l2shock.timeutils import now_utc
from l2shock.ui.remote_import_runtime import (
    RemoteImportRuntime,
    RemoteImportRuntimeBusyError,
    plan_remote_import_keys,
)
from l2shock.ui.state import (
    get_state,
    reset_state_for_tests,
)


def _hour(value: int = 12) -> datetime:
    return datetime(
        2026,
        9,
        17,
        value,
        tzinfo=timezone.utc,
    )


def test_range_planner_uses_all_component_preset_hashes() -> None:
    lower = Decimal("0")
    upper = Decimal("0.01")

    keys = plan_remote_import_keys(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=lower,
        upper_depth_fraction=upper,
    )

    binance_preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=lower,
        upper_fraction=upper,
    )
    bybit_preset = build_bybit_data_preset(
        base="BTC",
        lower_fraction=lower,
        upper_fraction=upper,
    )
    okx_preset = build_okx_futures_data_preset(
        base="BTC",
        lower_fraction=lower,
        upper_fraction=upper,
    )

    assert len(keys) == 4

    assert keys[0] == RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(12),
        preset_hash=binance_preset.preset_hash,
    )
    assert keys[1] == RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="bybit",
        instrument="BTCUSDT",
        hour_utc=_hour(12),
        preset_hash=bybit_preset.preset_hash,
    )
    assert keys[2] == RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue="okx_futures",
        instrument="BTC-USDT-SWAP",
        hour_utc=_hour(12),
        preset_hash=okx_preset.preset_hash,
    )
    assert keys[3] == RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(12),
    )


def test_range_planner_builds_eight_artifacts_per_hour_for_both_bases() -> None:
    keys = plan_remote_import_keys(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(14),
        bases=("eth", "BTC", "BTC"),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert len(keys) == 16
    assert {key.hour_utc for key in keys} == {
        _hour(12),
        _hour(13),
    }

    l2_keys = tuple(key for key in keys if key.kind is RemoteArtifactKind.L2)
    price_keys = tuple(key for key in keys if key.kind is RemoteArtifactKind.PRICE)

    assert len(l2_keys) == 12
    assert len(price_keys) == 4

    assert {key.venue for key in l2_keys} == {
        "binance_futures",
        "bybit",
        "okx_futures",
    }
    assert {key.instrument for key in price_keys} == {
        "BTCUSDT",
        "ETHUSDT",
    }
    assert {key.venue for key in price_keys} == {
        "binance_futures",
    }


def test_range_planner_rejects_non_hour_boundaries() -> None:
    with pytest.raises(
        ValueError,
        match="exact UTC hour",
    ):
        plan_remote_import_keys(
            requested_start_utc=_hour(12).replace(minute=30),
            requested_end_utc=_hour(13),
            bases=("BTC",),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )


@pytest.mark.asyncio
async def test_runtime_rejects_unrelated_active_operation() -> None:
    reset_state_for_tests()
    state = get_state()
    state.active_operation_name = "manual_fetch"
    state.active_operation_started_at = _hour(12)

    class Repository:
        def current_revision(self) -> str:
            return "a" * 40

        def download_artifact(
            self,
            key,
            *,
            revision=None,
        ):
            del key, revision
            return None

    runtime = RemoteImportRuntime(
        repository=Repository(),
    )

    with pytest.raises(
        RemoteImportRuntimeBusyError,
        match="manual_fetch",
    ):
        runtime.start(
            requested_start_utc=_hour(12),
            requested_end_utc=_hour(13),
            bases=("BTC",),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )


@pytest.mark.asyncio
async def test_runtime_pins_revision_once_and_reports_missing() -> None:
    reset_state_for_tests()
    observed_revisions: list[str | None] = []

    class Repository:
        def __init__(self) -> None:
            self.revision_calls = 0

        def current_revision(self) -> str:
            self.revision_calls += 1
            return "a" * 40

        def download_artifact(
            self,
            key,
            *,
            revision=None,
        ):
            del key
            observed_revisions.append(revision)
            return None

    repository = Repository()
    runtime = RemoteImportRuntime(
        repository=repository,
    )

    result = await runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert repository.revision_calls == 1
    assert observed_revisions == [
        "a" * 40,
        "a" * 40,
        "a" * 40,
        "a" * 40,
    ]

    assert result.status == "error"
    assert result.pinned_revision == "a" * 40
    assert result.artifacts_selected == 4
    assert result.imported_count == 0
    assert result.reused_count == 0
    assert result.missing_count == 4
    assert result.failed_count == 0
    assert result.stopped is False

    snapshot = runtime.snapshot()

    assert snapshot.is_running is False
    assert snapshot.last_result is result
    assert snapshot.pinned_revision == "a" * 40
    assert snapshot.completion_sequence == 1
    assert get_state().active_operation_name == ""


@pytest.mark.asyncio
async def test_runtime_stops_between_artifacts() -> None:
    reset_state_for_tests()
    observed_keys: list[RemoteArtifactKey] = []

    class Repository:
        def current_revision(self) -> str:
            return "a" * 40

        def download_artifact(
            self,
            key,
            *,
            revision=None,
        ):
            assert revision == "a" * 40
            observed_keys.append(key)
            runtime.request_stop()
            return None

    runtime = RemoteImportRuntime(
        repository=Repository(),
    )

    result = await runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert len(observed_keys) == 1
    assert result.status == "stopped"
    assert result.artifacts_selected == 4
    assert result.missing_count == 1
    assert result.stopped is True
    assert len(result.items) == 1


@pytest.mark.asyncio
async def test_runtime_rejects_new_import_after_shutdown() -> None:
    reset_state_for_tests()
    get_state().shutdown_started = True

    class Repository:
        def current_revision(self) -> str:
            return "a" * 40

        def download_artifact(
            self,
            key,
            *,
            revision=None,
        ):
            del key, revision
            return None

    runtime = RemoteImportRuntime(
        repository=Repository(),
    )

    with pytest.raises(
        RuntimeError,
        match="shutdown has started",
    ):
        runtime.start(
            requested_start_utc=_hour(12),
            requested_end_utc=_hour(13),
            bases=("BTC",),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )


def test_remote_import_result_fixture_contract() -> None:
    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(12),
    )

    result = RemoteArtifactImportResult(
        revision="a" * 40,
        key=key,
        analytical_inserted=True,
        preset_inserted=None,
        source_metadata_updated=True,
        imported_at_utc=now_utc(),
    )

    assert result.key is key
    assert result.analytical_inserted is True


@pytest.mark.asyncio
async def test_runtime_reports_stop_requested_during_final_artifact() -> None:
    reset_state_for_tests()
    observed_keys: list[RemoteArtifactKey] = []

    class Repository:
        def current_revision(self) -> str:
            return "a" * 40

        def download_artifact(
            self,
            key,
            *,
            revision=None,
        ):
            assert revision == "a" * 40
            observed_keys.append(key)

            if len(observed_keys) == 4:
                assert runtime.request_stop() is True

            return None

    runtime = RemoteImportRuntime(
        repository=Repository(),
    )

    result = await runtime.start(
        requested_start_utc=_hour(12),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert len(observed_keys) == 4
    assert result.artifacts_selected == 4
    assert result.missing_count == 4
    assert result.status == "stopped"
    assert result.stopped is True

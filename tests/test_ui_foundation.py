from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from l2shock.config import AppConfig
from l2shock.ui import app as ui_app
from l2shock.ui import tab_settings as ui_tab_settings
from l2shock.ui.tab_fetch import _parse_depth_band
from l2shock.ui.state import get_state, reset_state_for_tests


def test_app_config_rejects_non_loopback_host() -> None:
    with pytest.raises(ValidationError, match="loopback"):
        AppConfig(host="0.0.0.0")


def test_runtime_state_uses_aware_utc_start_time() -> None:
    reset_state_for_tests()
    state = get_state()

    assert isinstance(state.process_started_at, datetime)
    assert state.process_started_at.tzinfo is timezone.utc


@pytest.mark.asyncio
async def test_health_snapshot_reports_foundation_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()

    monkeypatch.setattr(
        ui_app,
        "_database_health",
        lambda: {
            "ok": True,
            "reachable": True,
            "current_heads": ["0001_initial"],
            "expected_head": "0001_initial",
            "error": None,
        },
    )
    monkeypatch.setattr(
        ui_app,
        "_disk_health",
        lambda: {
            "ok": True,
            "free_gib": 100.0,
            "minimum_free_gib": 5.0,
            "raw_path": r"C:\data\raw",
        },
    )

    snapshot = await ui_app.build_health_snapshot()

    assert snapshot["ok"] is True
    assert snapshot["ready"] is True
    assert snapshot["database"]["ok"] is True
    assert snapshot["disk"]["ok"] is True

    assert snapshot["shutdown_started"] is False
    assert snapshot["shutdown_complete"] is False
    assert snapshot["manual_fetch_running"] is False
    assert snapshot["manual_fetch_stop_requested"] is False
    assert snapshot["manual_processing_running"] is False
    assert snapshot["manual_processing_stop_requested"] is False
    assert snapshot["active_processing_operation_id"] is None
    assert snapshot["remote_import_running"] is False
    assert snapshot["remote_import_stop_requested"] is False
    assert snapshot["active_remote_import_operation_id"] is None
    assert snapshot["remote_import_pinned_revision"] is None
    assert snapshot["manual_analysis_running"] is False
    assert snapshot["manual_analysis_stop_requested"] is False
    assert snapshot["active_analysis_operation_id"] is None

    implemented = snapshot["implemented_subsystems"]
    assert implemented["database_foundation"] is True
    assert implemented["downloader"] is True
    assert implemented["fetch_orchestration"] is True
    assert implemented["manual_fetch_ui"] is True
    assert implemented["safe_application_shutdown"] is True
    assert implemented["automatic_fetch"] is True
    assert implemented["availability_calendar"] is True
    assert implemented["parquet_event_reader"] is True
    assert implemented["replay_engine"] is True
    assert implemented["binance_sequence_adapter"] is True
    assert implemented["bybit_sequence_adapter"] is True
    assert implemented["okx_sequence_adapter"] is True
    assert implemented["bybit_orderbook_acquisition"] is True
    assert implemented["bybit_l2_processing"] is True
    assert implemented["bybit_remote_worker"] is True
    assert implemented["bybit_hf_publication"] is True
    assert implemented["bybit_hf_range_import"] is True
    assert implemented["okx_orderbook_acquisition"] is True
    assert implemented["okx_l2_processing"] is True
    assert implemented["checkpoint_codec"] is True
    assert implemented["checkpoint_store"] is True
    assert implemented["one_second_book_sampling"] is True
    assert implemented["single_market_liquidity"] is True
    assert implemented["single_market_l2_processing_coordinator"] is True
    assert implemented["trade_ohlc_foundation"] is True
    assert implemented["trade_ohlc_codec"] is True
    assert implemented["trade_ohlc_persistence"] is True
    assert implemented["trade_price_processing_coordinator"] is True
    assert implemented["processing_runtime_ui"] is True
    assert implemented["production_processing_pipeline"] is True
    assert implemented["remote_hf_artifact_importer"] is True
    assert implemented["remote_hf_range_import_runtime"] is True
    assert implemented["timeframe_aggregation"] is True
    assert implemented["verified_l2_only_loading"] is True
    assert implemented["streaming_analysis_loading"] is True
    assert implemented["selectable_l2_panels"] is True
    assert implemented["independent_panel_y_zoom"] is True
    assert implemented["analysis_runtime"] is True
    assert implemented["analysis_ui"] is True
    assert implemented["analysis_chart_workspace"] is True
    assert implemented["chart_render_acknowledgement"] is True
    assert implemented["chart_viewport_preservation"] is True
    assert implemented["custom_gapped_crosshair"] is True
    assert implemented["chart_export"] is True
    assert implemented["displayed_bar_json_export"] is True
    assert implemented["displayed_bar_csv_export"] is True

    for retired in (
        "price_filter_policy",
        "verified_analysis_loading",
        "segmented_price_filtering",
        "liquidity_movement_analysis",
        "analysis_execution_orchestration",
        "analysis_result_cache",
        "selected_lm_emphasis",
        "chart_timeframe_switching",
        "analysis_json_export",
    ):
        assert retired not in implemented
    assert implemented["production_diagnostics"] is True
    assert implemented["preset_crud_ui"] is True
    assert implemented["aggregate_preset_crud"] is True
    assert implemented["multi_market_aggregation"] is True
    assert implemented["component_aware_availability"] is True
    assert implemented["partial_market_coverage_warning"] is True
    assert implemented["maintenance_diagnostics"] is True
    assert implemented["maintenance_actions"] is True
    assert implemented["stale_source_recovery"] is True
    assert implemented["orphan_checkpoint_cleanup"] is True
    assert implemented["raw_retention_enforcement"] is True
    assert implemented["automatic_maintenance"] is False
    assert implemented["checkpoint_inventory"] is True
    assert implemented["stale_source_diagnostics"] is True
    assert implemented["analytical_consistency_diagnostics"] is True
    assert implemented["processing_performance_summaries"] is True
    assert implemented["raw_retention_dry_run"] is True
    assert implemented["raw_retention_deletion"] is True
    assert implemented["checkpoint_cleanup"] is False
    assert implemented["analytical_repair"] is False
    assert implemented["charts"] is True
    assert snapshot["automatic_fetch_enabled"] is False
    assert snapshot["automatic_fetch_running"] is False
    assert snapshot["automatic_fetch_stop_requested"] is False
    assert snapshot["automatic_fetch_target_hour_utc"] is None
    assert snapshot["automatic_fetch_last_poll_at"] is None
    assert snapshot["automatic_fetch_next_poll_at"] is None


@pytest.mark.asyncio
async def test_health_snapshot_ok_is_false_when_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()

    monkeypatch.setattr(
        ui_app,
        "_database_health",
        lambda: {
            "ok": False,
            "reachable": False,
            "current_heads": [],
            "expected_head": "0001_initial",
            "error": "database unavailable",
        },
    )
    monkeypatch.setattr(
        ui_app,
        "_disk_health",
        lambda: {
            "ok": True,
            "free_gib": 100.0,
            "minimum_free_gib": 5.0,
            "raw_path": r"C:\data\raw",
        },
    )

    snapshot = await ui_app.build_health_snapshot()

    assert snapshot["ready"] is False
    assert snapshot["ok"] is False


def test_processing_depth_band_defaults_are_accepted() -> None:
    lower, upper = _parse_depth_band("0", "0.01")

    assert str(lower) == "0"
    assert str(upper) == "0.01"


@pytest.mark.parametrize(
    ("lower", "upper", "message"),
    [
        ("-0.01", "0.01", "greater than or equal to 0"),
        ("0.02", "0.01", "less than or equal"),
        ("0", "1", "less than 1"),
        ("0", "NaN", "finite"),
        ("Infinity", "0.01", "finite"),
        ("not-a-number", "0.01", "valid decimal"),
        ("", "0.01", "required"),
    ],
)
def test_processing_depth_band_rejects_invalid_values(
    lower: str,
    upper: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _parse_depth_band(lower, upper)


def test_disk_health_returns_not_ready_on_filesystem_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_disk_usage(_path):
        raise OSError("sensitive local path detail")

    monkeypatch.setattr(
        ui_app.shutil,
        "disk_usage",
        fail_disk_usage,
    )

    result = ui_app._disk_health()

    assert result["ok"] is False
    assert result["free_gib"] is None
    assert result["error"] == "Unexpected OSError"
    assert "sensitive local path detail" not in str(result)


def test_prepare_runtime_handles_unmeasurable_disk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ui_app,
        "_ensure_runtime_directories",
        lambda: None,
    )
    monkeypatch.setattr(
        ui_app,
        "_disk_health",
        lambda: {
            "ok": False,
            "free_gib": None,
            "minimum_free_gib": 5.0,
            "raw_path": r"C:\data\raw",
            "error": "Unexpected OSError",
        },
    )
    monkeypatch.setattr(
        ui_app,
        "verify_schema",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        ui_app,
        "get_engine",
        lambda: object(),
    )

    ui_app.prepare_runtime()


def test_settings_selects_do_not_use_literal_tuple_options() -> None:
    source_path = Path(ui_tab_settings.__file__)
    tree = ast.parse(
        source_path.read_text(encoding="utf-8"),
        filename=str(source_path),
    )

    tuple_option_lines: list[int] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        if not isinstance(node.func, ast.Attribute):
            continue

        if node.func.attr != "select":
            continue

        for keyword in node.keywords:
            if keyword.arg == "options" and isinstance(keyword.value, ast.Tuple):
                tuple_option_lines.append(keyword.value.lineno)

    assert tuple_option_lines == [], (
        "NiceGUI ui.select options must be a list or mapping; "
        f"literal tuple options found at lines {tuple_option_lines}"
    )


def test_fetch_tab_exposes_remote_default_and_local_fallback_profiles() -> None:
    from l2shock.ui import tab_fetch

    source = Path(tab_fetch.__file__).read_text(
        encoding="utf-8",
    )

    assert "Remote HF Import (default)" in source
    assert "Local CryptoHFTData Fetch + Processing" in source
    assert "Start Remote Import" in source
    assert "Stop Remote Import" in source
    assert "get_remote_import_runtime" in source
    assert "get_manual_fetch_runtime" in source
    assert "get_manual_processing_runtime" in source


def test_settings_market_composition_includes_bybit_profiles() -> None:
    source = Path(ui_tab_settings.__file__).read_text(
        encoding="utf-8",
    )

    assert "PresetMarketProfile.BYBIT.value" in source
    assert "PresetMarketProfile.BYBIT.label" in source

    assert "PresetMarketProfile.BINANCE_BYBIT_OKX_FUTURES.value" in source
    assert "PresetMarketProfile.BINANCE_BYBIT_OKX_FUTURES.label" in source


def test_remote_import_ui_names_all_l2_component_venues() -> None:
    from l2shock.ui import tab_fetch

    source = Path(tab_fetch.__file__).read_text(
        encoding="utf-8",
    )

    assert "Binance, Bybit, and OKX component L2" in source
    assert "Binance real-trade price artifacts" in source


def _audit_isolate_health_runtimes(monkeypatch):
    import l2shock.ui.app as module

    for name in (
        "peek_manual_fetch_runtime",
        "peek_automatic_fetch_runtime",
        "peek_manual_processing_runtime",
        "peek_remote_import_runtime",
        "peek_l2_view_runtime",
    ):
        monkeypatch.setattr(module, name, lambda: None)

    monkeypatch.setattr(
        module,
        "expected_alembic_head",
        lambda: "0001_initial",
    )
    monkeypatch.setattr(
        module,
        "_disk_health",
        lambda: {
            "ok": True,
            "free_gib": 100.0,
            "minimum_free_gib": 5.0,
            "raw_path": "audit-raw",
            "error": None,
        },
    )
    return module


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown_complete", (False, True))
async def test_audit_health_during_shutdown_does_not_open_database(
    monkeypatch,
    shutdown_complete,
):
    from l2shock.ui.state import get_state, reset_state_for_tests

    reset_state_for_tests()
    try:
        module = _audit_isolate_health_runtimes(monkeypatch)
        state = get_state()
        state.shutdown_started = True
        state.shutdown_complete = shutdown_complete

        def forbidden():
            raise AssertionError("Shutdown health probe attempted database access")

        monkeypatch.setattr(module, "_database_health", forbidden)
        monkeypatch.setattr(module, "get_engine", forbidden)

        snapshot = await module.health_endpoint()

        assert snapshot["ready"] is False
        assert snapshot["ok"] is False
        assert snapshot["shutdown_started"] is True
        assert snapshot["shutdown_complete"] is shutdown_complete
        assert snapshot["database"]["probe_skipped"] is True
        assert snapshot["database"]["reachable"] is None
        assert state.untracked_db_workers == set()
    finally:
        reset_state_for_tests()


def test_audit_database_health_defensively_skips_after_shutdown(monkeypatch):
    from l2shock.ui.state import get_state, reset_state_for_tests

    reset_state_for_tests()
    try:
        module = _audit_isolate_health_runtimes(monkeypatch)
        get_state().shutdown_started = True

        def forbidden():
            raise AssertionError("Skipped probe recreated the database engine")

        monkeypatch.setattr(module, "get_engine", forbidden)

        result = module._database_health()

        assert result["ok"] is False
        assert result["reachable"] is None
        assert result["probe_skipped"] is True
    finally:
        reset_state_for_tests()


@pytest.mark.asyncio
async def test_audit_cancelled_health_request_keeps_db_worker_shutdown_owned(
    monkeypatch,
):
    import asyncio
    import threading

    from l2shock.ui import shutdown as shutdown_module
    from l2shock.ui.components import wait_for_untracked_db_workers
    from l2shock.ui.state import get_state, reset_state_for_tests

    reset_state_for_tests()
    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    disposed = []
    task = None

    try:
        module = _audit_isolate_health_runtimes(monkeypatch)
        state = get_state()

        def database_probe():
            entered.set()
            try:
                if not release.wait(timeout=5.0):
                    raise TimeoutError("Test did not release the database probe")
                return {
                    "ok": True,
                    "reachable": True,
                    "current_heads": ["0001_initial"],
                    "expected_head": "0001_initial",
                    "error": None,
                }
            finally:
                exited.set()

        monkeypatch.setattr(module, "_database_health", database_probe)

        for name in (
            "peek_automatic_fetch_runtime",
            "peek_manual_fetch_runtime",
            "peek_remote_import_runtime",
            "peek_manual_processing_runtime",
            "peek_l2_view_runtime",
        ):
            monkeypatch.setattr(shutdown_module, name, lambda: None)

        monkeypatch.setattr(
            shutdown_module,
            "reset_engine",
            lambda: disposed.append(True),
        )

        task = asyncio.create_task(module.health_endpoint())
        assert await asyncio.to_thread(entered.wait, 2.0)
        assert state.untracked_db_workers

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert not exited.is_set()
        assert any(not worker.done() for worker in state.untracked_db_workers)

        with pytest.raises(RuntimeError, match="background work"):
            await shutdown_module.shutdown_runtime(
                request_server_stop=False,
                other_task_timeout_seconds=0.0,
            )

        assert state.shutdown_started is True
        assert state.shutdown_complete is False
        assert disposed == []

        release.set()
        assert await wait_for_untracked_db_workers(timeout_seconds=2.0) == 0
        await asyncio.sleep(0)

        await shutdown_module.shutdown_runtime(
            request_server_stop=False,
            other_task_timeout_seconds=2.0,
        )

        assert exited.is_set()
        assert state.shutdown_complete is True
        assert disposed == [True]
    finally:
        release.set()
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await wait_for_untracked_db_workers(timeout_seconds=2.0)
        reset_state_for_tests()


@pytest.mark.asyncio
async def test_audit_health_admitted_before_shutdown_cannot_report_ready(
    monkeypatch,
):
    import asyncio
    import threading

    from l2shock.ui.components import wait_for_untracked_db_workers
    from l2shock.ui.state import get_state, reset_state_for_tests

    reset_state_for_tests()
    entered = threading.Event()
    release = threading.Event()
    task = None

    try:
        module = _audit_isolate_health_runtimes(monkeypatch)
        state = get_state()

        def database_probe():
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("Test did not release the database probe")
            return {
                "ok": True,
                "reachable": True,
                "current_heads": ["0001_initial"],
                "expected_head": "0001_initial",
                "error": None,
            }

        monkeypatch.setattr(module, "_database_health", database_probe)

        task = asyncio.create_task(module.health_endpoint())
        assert await asyncio.to_thread(entered.wait, 2.0)

        state.shutdown_started = True
        release.set()
        snapshot = await asyncio.wait_for(task, timeout=2.0)

        assert snapshot["database"]["ok"] is True
        assert snapshot["shutdown_started"] is True
        assert snapshot["ready"] is False
        assert snapshot["ok"] is False
    finally:
        release.set()
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await wait_for_untracked_db_workers(timeout_seconds=2.0)
        reset_state_for_tests()


def test_audit_database_health_logs_only_failure_type(monkeypatch, caplog):
    import logging

    from l2shock.ui.state import reset_state_for_tests

    reset_state_for_tests()
    try:
        module = _audit_isolate_health_runtimes(monkeypatch)
        private_message = "audit-private-health-message"
        private_cause = "audit-private-health-cause"

        def fail_engine():
            try:
                raise ValueError(private_cause)
            except ValueError as cause:
                raise RuntimeError(private_message) from cause

        monkeypatch.setattr(module, "get_engine", fail_engine)

        with caplog.at_level(logging.ERROR, logger=module.__name__):
            result = module._database_health()

        assert result["ok"] is False
        assert result["error"] == "Unexpected RuntimeError"
        assert private_message not in caplog.text
        assert private_cause not in caplog.text

        records = [
            record for record in caplog.records if record.name == module.__name__
        ]
        assert records
        assert all(record.exc_info is None for record in records)
    finally:
        reset_state_for_tests()

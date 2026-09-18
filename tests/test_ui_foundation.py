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


def test_health_snapshot_reports_foundation_status(
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

    snapshot = ui_app.build_health_snapshot()

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
    assert implemented["price_filter_policy"] is True
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
    assert implemented["verified_analysis_loading"] is True
    assert implemented["segmented_price_filtering"] is True
    assert implemented["liquidity_movement_analysis"] is True
    assert implemented["analysis_execution_orchestration"] is True
    assert implemented["analysis_result_cache"] is True
    assert implemented["analysis_runtime"] is True
    assert implemented["analysis_ui"] is True
    assert implemented["analysis_chart_workspace"] is True
    assert implemented["chart_render_acknowledgement"] is True
    assert implemented["chart_viewport_preservation"] is True
    assert implemented["custom_gapped_crosshair"] is True
    assert implemented["table_to_chart_navigation"] is True
    assert implemented["selected_lm_emphasis"] is True
    assert implemented["chart_timeframe_switching"] is True
    assert implemented["analysis_json_export"] is True
    assert implemented["chart_export"] is True
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


def test_health_snapshot_ok_is_false_when_not_ready(
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

    snapshot = ui_app.build_health_snapshot()

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

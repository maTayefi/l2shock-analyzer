# l2shock/ui/app.py
"""NiceGUI application shell and readiness endpoint."""

from __future__ import annotations

import asyncio
import logging
import shutil
from datetime import datetime
from typing import Any

from alembic.runtime.migration import MigrationContext
from nicegui import app, ui
from sqlalchemy import text

from l2shock import __version__
from l2shock.acquisition.pruning_recovery import (
    reconcile_stranded_raw_pruning,
)
from l2shock.config import get_settings
from l2shock.db.engine import get_engine
from l2shock.db.schema import expected_alembic_head, verify_schema
from l2shock.timeutils import now_utc
from l2shock.ui.analysis_runtime import peek_manual_analysis_runtime
from l2shock.ui.fetch_runtime import peek_manual_fetch_runtime
from l2shock.ui.automatic_fetch_runtime import (
    get_automatic_fetch_runtime,
    peek_automatic_fetch_runtime,
)
from l2shock.ui.processing_runtime import peek_manual_processing_runtime
from l2shock.ui.remote_import_runtime import peek_remote_import_runtime
from l2shock.ui.shutdown import shutdown_runtime
from l2shock.ui.state import get_state
from l2shock.ui.tab_analysis import build_analysis_tab
from l2shock.ui.tab_fetch import build_fetch_tab
from l2shock.ui.tab_settings import build_settings_tab

log = logging.getLogger(__name__)


def _ensure_runtime_directories() -> None:
    settings = get_settings()

    for directory in (
        settings.storage.raw_path,
        settings.storage.cache_path,
        settings.storage.quarantine_path,
        settings.storage.export_path,
        settings.storage.log_path,
        settings.storage.backup_path,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def _disk_health() -> dict[str, Any]:
    settings = get_settings()
    raw_path = settings.storage.raw_path
    minimum_gib = settings.storage.minimum_free_disk_gib

    try:
        usage = shutil.disk_usage(raw_path)
        free_gib = usage.free / (1024**3)

        return {
            "ok": free_gib >= minimum_gib,
            "free_gib": round(free_gib, 3),
            "minimum_free_gib": float(minimum_gib),
            "raw_path": str(raw_path),
            "error": None,
        }

    except OSError as exc:
        log.exception("Disk health probe failed.")

        return {
            "ok": False,
            "free_gib": None,
            "minimum_free_gib": float(minimum_gib),
            "raw_path": str(raw_path),
            "error": f"Unexpected {type(exc).__name__}",
        }


def _database_health() -> dict[str, Any]:
    expected_head = expected_alembic_head()

    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1"))

            current_heads = sorted(
                MigrationContext.configure(connection).get_current_heads()
            )

        return {
            "ok": current_heads == [expected_head],
            "reachable": True,
            "current_heads": current_heads,
            "expected_head": expected_head,
            "error": None,
        }

    except Exception as exc:
        log.exception("Database health probe failed.")
        return {
            "ok": False,
            "reachable": False,
            "current_heads": [],
            "expected_head": expected_head,
            "error": f"Unexpected {type(exc).__name__}",
        }


def build_health_snapshot() -> dict[str, Any]:
    """Return process, database, and local-storage readiness state."""
    state = get_state()
    database = _database_health()
    disk = _disk_health()
    now = now_utc()

    started_at = state.process_started_at
    uptime_seconds = max(
        0.0,
        (now - started_at).total_seconds(),
    )

    fetch_runtime = peek_manual_fetch_runtime()
    fetch_snapshot = fetch_runtime.snapshot() if fetch_runtime is not None else None

    automatic_fetch_runtime = peek_automatic_fetch_runtime()
    automatic_fetch_snapshot = (
        automatic_fetch_runtime.snapshot()
        if automatic_fetch_runtime is not None
        else None
    )

    processing_runtime = peek_manual_processing_runtime()
    processing_snapshot = (
        processing_runtime.snapshot() if processing_runtime is not None else None
    )

    remote_import_runtime = peek_remote_import_runtime()
    remote_import_snapshot = (
        remote_import_runtime.snapshot() if remote_import_runtime is not None else None
    )

    analysis_runtime = peek_manual_analysis_runtime()
    analysis_snapshot = (
        analysis_runtime.snapshot() if analysis_runtime is not None else None
    )

    ready = bool(database["ok"] and disk["ok"] and not state.shutdown_started)

    return {
        "ok": ready,
        "ready": ready,
        "version": __version__,
        "now_utc": now.isoformat(),
        "process_started_at": started_at.isoformat(),
        "uptime_seconds": round(uptime_seconds, 3),
        "shutdown_started": bool(state.shutdown_started),
        "shutdown_complete": bool(state.shutdown_complete),
        "active_operation": state.active_operation_name or None,
        "active_operation_started_at": (
            state.active_operation_started_at.isoformat()
            if isinstance(state.active_operation_started_at, datetime)
            else None
        ),
        "active_fetch_operation_id": (
            fetch_snapshot.operation_id if fetch_snapshot is not None else None
        ),
        "manual_fetch_running": bool(
            fetch_snapshot is not None and fetch_snapshot.is_running
        ),
        "manual_fetch_stop_requested": bool(
            fetch_snapshot is not None and fetch_snapshot.stop_requested
        ),
        "automatic_fetch_enabled": bool(
            automatic_fetch_snapshot is not None and automatic_fetch_snapshot.enabled
        ),
        "automatic_fetch_running": bool(
            automatic_fetch_snapshot is not None and automatic_fetch_snapshot.is_running
        ),
        "automatic_fetch_stop_requested": bool(
            automatic_fetch_snapshot is not None
            and automatic_fetch_snapshot.stop_requested
        ),
        "automatic_fetch_target_hour_utc": (
            automatic_fetch_snapshot.current_target_hour_utc.isoformat()
            if (
                automatic_fetch_snapshot is not None
                and automatic_fetch_snapshot.current_target_hour_utc is not None
            )
            else None
        ),
        "automatic_fetch_last_poll_at": (
            automatic_fetch_snapshot.last_poll_at.isoformat()
            if (
                automatic_fetch_snapshot is not None
                and automatic_fetch_snapshot.last_poll_at is not None
            )
            else None
        ),
        "automatic_fetch_next_poll_at": (
            automatic_fetch_snapshot.next_poll_at.isoformat()
            if (
                automatic_fetch_snapshot is not None
                and automatic_fetch_snapshot.next_poll_at is not None
            )
            else None
        ),
        "active_processing_operation_id": (
            processing_snapshot.operation_id
            if processing_snapshot is not None
            else None
        ),
        "manual_processing_running": bool(
            processing_snapshot is not None and processing_snapshot.is_running
        ),
        "manual_processing_stop_requested": bool(
            processing_snapshot is not None and processing_snapshot.stop_requested
        ),
        "active_remote_import_operation_id": (
            remote_import_snapshot.operation_id
            if remote_import_snapshot is not None
            else None
        ),
        "remote_import_running": bool(
            remote_import_snapshot is not None and remote_import_snapshot.is_running
        ),
        "remote_import_stop_requested": bool(
            remote_import_snapshot is not None and remote_import_snapshot.stop_requested
        ),
        "remote_import_pinned_revision": (
            remote_import_snapshot.pinned_revision
            if remote_import_snapshot is not None
            else None
        ),
        "active_analysis_operation_id": (
            analysis_snapshot.operation_id if analysis_snapshot is not None else None
        ),
        "manual_analysis_running": bool(
            analysis_snapshot is not None and analysis_snapshot.is_running
        ),
        "manual_analysis_stop_requested": bool(
            analysis_snapshot is not None and analysis_snapshot.stop_requested
        ),
        "tracked_task_count": sum(1 for task in state.tracked_tasks if not task.done()),
        "database": database,
        "disk": disk,
        "implemented_subsystems": {
            "configuration": True,
            "timezone": True,
            "database_foundation": True,
            "price_filter_policy": True,
            "downloader": True,
            "fetch_orchestration": True,
            "manual_fetch_ui": True,
            "safe_application_shutdown": True,
            "automatic_fetch": True,
            "availability_calendar": True,
            "parquet_event_reader": True,
            "replay_engine": True,
            "binance_sequence_adapter": True,
            "bybit_sequence_adapter": True,
            "okx_sequence_adapter": True,
            "bybit_orderbook_acquisition": True,
            "bybit_l2_processing": True,
            "okx_orderbook_acquisition": True,
            "okx_l2_processing": True,
            "checkpoint_codec": True,
            "checkpoint_store": True,
            "one_second_book_sampling": True,
            "single_market_liquidity": True,
            "single_market_l2_processing_coordinator": True,
            "trade_ohlc_foundation": True,
            "trade_ohlc_codec": True,
            "trade_ohlc_persistence": True,
            "trade_price_processing_coordinator": True,
            "processing_runtime_ui": True,
            "production_processing_pipeline": True,
            "remote_hf_artifact_importer": True,
            "remote_hf_range_import_runtime": True,
            "timeframe_aggregation": True,
            "verified_analysis_loading": True,
            "segmented_price_filtering": True,
            "liquidity_movement_analysis": True,
            "analysis_execution_orchestration": True,
            "analysis_result_cache": True,
            "analysis_runtime": True,
            "analysis_ui": True,
            "analysis_chart_workspace": True,
            "chart_render_acknowledgement": True,
            "chart_viewport_preservation": True,
            "custom_gapped_crosshair": True,
            "table_to_chart_navigation": True,
            "selected_lm_emphasis": True,
            "chart_timeframe_switching": True,
            "analysis_json_export": True,
            "chart_export": True,
            "production_diagnostics": True,
            "preset_crud_ui": True,
            "aggregate_preset_crud": True,
            "multi_market_aggregation": True,
            "component_aware_availability": True,
            "partial_market_coverage_warning": True,
            "maintenance_diagnostics": True,
            "maintenance_actions": True,
            "stale_source_recovery": True,
            "orphan_checkpoint_cleanup": True,
            "raw_retention_enforcement": True,
            "automatic_maintenance": False,
            "checkpoint_inventory": True,
            "stale_source_diagnostics": True,
            "analytical_consistency_diagnostics": True,
            "processing_performance_summaries": True,
            "raw_retention_dry_run": True,
            "raw_retention_deletion": True,
            # General deletion of referenced checkpoints is intentionally
            # unavailable. Only old, canonical, unreferenced orphan
            # checkpoints are eligible through orphan_checkpoint_cleanup.
            "checkpoint_cleanup": False,
            "analytical_repair": False,
            "charts": True,
        },
    }


@app.get("/health")
def health_endpoint() -> dict[str, Any]:
    return build_health_snapshot()


@ui.page("/")
def index_page() -> None:
    settings = get_settings()

    dark_mode = ui.dark_mode()
    dark_mode.enable()

    ui.add_css("""
        body.body--dark {
            background: #0b1020;
        }

        .l2shock-page {
            width: 100%;
            max-width: 1920px;
            margin: 0 auto;
        }

        .l2shock-status-chip {
            border-radius: 9999px;
            padding: 3px 10px;
            font-weight: 700;
            background: #f59e0b;
            color: #111827;
        }
        """)

    with ui.header().classes("bg-slate-900 text-white items-center"):
        ui.label(settings.app.title).classes("text-xl font-bold text-cyan-300")
        ui.space()
        ui.label(f"TZ: {settings.app.timezone}").classes("text-sm opacity-80")
        ui.label(f"LOCAL RESEARCH v{__version__}").classes("l2shock-status-chip")

    with ui.column().classes("l2shock-page"):
        with ui.tabs().classes("w-full") as tabs:
            fetch_tab = ui.tab("Fetch", icon="cloud_download")
            analysis_tab = ui.tab("Analysis", icon="insights")
            settings_tab = ui.tab("Settings", icon="settings")

        with ui.tab_panels(tabs, value=fetch_tab).classes("w-full"):
            # Build Analysis first so its handoff callback exists when the
            # Fetch calendar is constructed. Visual tab order remains owned by
            # the ui.tabs declarations above.
            with ui.tab_panel(analysis_tab):
                analysis_handoff = build_analysis_tab()

            async def _apply_availability_handoff(
                handoff,
            ) -> bool:
                applied = await analysis_handoff(handoff)

                if applied:
                    tabs.set_value(analysis_tab)

                return applied

            with ui.tab_panel(fetch_tab):
                build_fetch_tab(
                    on_analysis_handoff=(_apply_availability_handoff),
                )

            with ui.tab_panel(settings_tab):
                build_settings_tab()


async def _shutdown_runtime() -> None:
    # Framework/terminal shutdown performs cleanup but must not recursively
    # request another NiceGUI shutdown.
    await shutdown_runtime(request_server_stop=False)


async def _startup_runtime() -> None:
    """Reconcile restart state, then restore background services."""

    settings = get_settings()

    try:
        pruning_report = await asyncio.to_thread(
            reconcile_stranded_raw_pruning,
            settings.storage.raw_path,
        )

        if pruning_report.examined_count:
            log.info(
                "Raw-pruning restart reconciliation completed: "
                "examined=%d restored=%d deleted=%d failed=%d.",
                pruning_report.examined_count,
                pruning_report.restored_count,
                pruning_report.deleted_count,
                pruning_report.failed_count,
            )

        if pruning_report.failed_count:
            log.error(
                "Raw-pruning restart reconciliation retained %d "
                "unresolved artifact(s): %s",
                pruning_report.failed_count,
                pruning_report.failures,
            )

    except Exception:
        log.exception("Could not complete raw-pruning restart reconciliation.")

    try:
        runtime = get_automatic_fetch_runtime()
        await runtime.restore_persisted_state()
    except Exception:
        log.exception("Could not restore persisted automatic-fetch state.")


app.on_startup(_startup_runtime)


app.on_shutdown(_shutdown_runtime)


def prepare_runtime() -> None:
    """Validate filesystem, database, and schema before serving the UI."""
    _ensure_runtime_directories()

    disk = _disk_health()

    if not disk["ok"]:
        free_gib = disk.get("free_gib")
        minimum_free_gib = float(disk["minimum_free_gib"])

        if isinstance(free_gib, (int, float)):
            log.warning(
                "Free disk space %.3f GiB is below configured minimum "
                "%.3f GiB. The application shell may start, but future "
                "downloads must fail closed until sufficient disk space "
                "is available.",
                float(free_gib),
                minimum_free_gib,
            )
        else:
            log.warning(
                "Free disk space could not be measured. The application "
                "shell may start, but future downloads must fail closed "
                "until the configured raw storage is available. "
                "Configured minimum: %.3f GiB.",
                minimum_free_gib,
            )

    verify_schema(get_engine())
    log.info("Database schema verification passed.")


def run_app() -> None:
    settings = get_settings()

    prepare_runtime()

    ui.run(
        host=settings.app.host,
        port=settings.app.port,
        title=settings.app.title,
        show=True,
        reload=False,
        dark=True,
        reconnect_timeout=24 * 60 * 60,
    )


__all__ = [
    "build_health_snapshot",
    "prepare_runtime",
    "run_app",
]

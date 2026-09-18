# l2shock/ui/tab_settings.py
"""Settings, production diagnostics, and safe application shutdown."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from nicegui import ui

from l2shock.config import get_settings
from l2shock.diagnostics import (
    production_diagnostics_report,
    report_json_bytes,
)
from l2shock.maintenance_actions import (
    MaintenanceActionKind,
    MaintenanceActionReport,
    MaintenancePreview,
    build_maintenance_preview,
    execute_maintenance_action,
)
from l2shock.presets import (
    ManagedPreset,
    ManagedPresetWriteResult,
    PresetManagementError,
    PresetMarketProfile,
    create_edited_preset,
    create_managed_preset,
    delete_managed_preset,
    list_managed_presets,
    set_managed_preset_enabled,
)
from l2shock.ui.components import (
    create_tracked_task,
    persistent_notify,
    section_header,
)
from l2shock.ui.shutdown import shutdown_runtime
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)


def _diagnostics_export_filename(
    report: dict[str, object],
) -> str:
    generated = str(report.get("generated_at_utc") or "").strip()

    compact = (
        generated.replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
        .replace(".", "")
    )

    compact = "".join(
        character
        for character in compact
        if character.isalnum() or character in {"_", "-"}
    )

    return f"l2shock-production-diagnostics-{compact or 'report'}.json"


def build_settings_tab() -> None:
    settings = get_settings()
    state = get_state()

    latest_report: dict[str, object] | None = None
    latest_maintenance_audit: MaintenanceActionReport | None = None
    pending_maintenance_preview: MaintenancePreview | None = None

    managed_presets: tuple[ManagedPreset, ...] = ()
    preset_refresh_running = False
    preset_mutation_running = False
    maintenance_running = False

    with ui.column().classes("w-full gap-3 p-4"):
        section_header("Settings")

        with ui.card().classes("w-full"):
            ui.label("Application").classes("font-semibold")

            with ui.grid(columns=2).classes("gap-x-6 gap-y-2 text-sm"):
                ui.label("UI timezone")
                ui.label(settings.app.timezone).classes("font-mono")

                ui.label("Host")
                ui.label(settings.app.host).classes("font-mono")

                ui.label("Port")
                ui.label(str(settings.app.port)).classes("font-mono")

                ui.label("Base sampling")
                ui.label(f"{settings.analysis.base_sampling_interval_ms} ms").classes(
                    "font-mono"
                )

        with ui.card().classes("w-full"):
            ui.label("Database").classes("font-semibold")

            safe_connection = (
                f"postgresql://{settings.database.user}:***@"
                f"{settings.database.host}:{settings.database.port}/"
                f"{settings.database.database}"
            )

            ui.label(safe_connection).classes("font-mono text-sm break-all")

            ui.label(
                "The password is environment-backed and is never rendered."
            ).classes("text-xs text-gray-500")

        with ui.card().classes("w-full"):
            ui.label("Storage paths").classes("font-semibold")

            for label, path in (
                ("Raw Parquet", settings.storage.raw_path),
                ("Cache", settings.storage.cache_path),
                ("Quarantine", settings.storage.quarantine_path),
                ("Exports", settings.storage.export_path),
                ("Logs", settings.storage.log_path),
                ("Backups", settings.storage.backup_path),
            ):
                with ui.row().classes("w-full gap-3"):
                    ui.label(label).classes("w-32 text-sm font-semibold")
                    ui.label(str(path)).classes("font-mono text-xs break-all")

        with ui.card().classes("w-full"):
            ui.label("Data presets").classes("text-lg font-semibold")

            ui.label(
                "Semantic preset content is immutable. Editing depth settings "
                "creates a new preset hash; it never rewrites historical data."
            ).classes("text-sm text-gray-600")

            ui.label(
                "The editor supports independently materialized Binance "
                "Futures, Bybit, and OKX Futures single-market presets, plus "
                "the existing analysis-time Binance + OKX aggregate preset. "
                "A three-market aggregate is not enabled yet. Aggregate "
                "presets do not combine raw events or create aggregate "
                "PostgreSQL L2 rows."
            ).classes("text-xs text-blue-700")

            with ui.row().classes("w-full gap-3 flex-wrap items-end mt-3"):
                preset_base = ui.select(
                    options=["BTC", "ETH"],
                    value="BTC",
                    label="Base asset",
                ).classes("w-40")

                preset_market_profile = ui.select(
                    options={
                        PresetMarketProfile.BINANCE_FUTURES.value: (
                            PresetMarketProfile.BINANCE_FUTURES.label
                        ),
                        PresetMarketProfile.OKX_FUTURES.value: (
                            PresetMarketProfile.OKX_FUTURES.label
                        ),
                        PresetMarketProfile.BINANCE_OKX_FUTURES.value: (
                            PresetMarketProfile.BINANCE_OKX_FUTURES.label
                        ),
                    },
                    value=PresetMarketProfile.BINANCE_FUTURES.value,
                    label="Market composition",
                ).classes("w-72")

                preset_lower = (
                    ui.input(
                        label="Depth lower fraction",
                        value="0",
                    )
                    .props("type=number min=0 max=0.999999 step=0.0001")
                    .classes("w-56")
                )

                preset_upper = (
                    ui.input(
                        label="Depth upper fraction",
                        value="0.01",
                    )
                    .props("type=number min=0 max=0.999999 step=0.0001")
                    .classes("w-56")
                )

                preset_enabled_on_create = ui.checkbox(
                    "Enable new preset",
                    value=True,
                )

            with ui.row().classes("w-full gap-2 flex-wrap mt-2"):
                add_preset_button = ui.button(
                    "Add preset",
                    icon="add",
                ).props("color=primary")

                save_edited_preset_button = ui.button(
                    "Save edited values as new preset",
                    icon="content_copy",
                ).props("outline color=primary")

                refresh_presets_button = ui.button(
                    "Refresh presets",
                    icon="refresh",
                ).props("outline")

            ui.separator().classes("my-3")

            preset_select = ui.select(
                options={},
                value=None,
                label="Persisted data preset",
            ).classes("w-full")

            preset_status = ui.label("Persisted presets have not been loaded.").classes(
                "text-sm text-gray-600"
            )

            with ui.row().classes("w-full gap-2 flex-wrap mt-2"):
                load_preset_button = ui.button(
                    "Load selected into editor",
                    icon="edit",
                ).props("outline")

                enable_preset_button = ui.button(
                    "Enable",
                    icon="toggle_on",
                ).props("outline color=positive")

                disable_preset_button = ui.button(
                    "Disable",
                    icon="toggle_off",
                ).props("outline color=warning")

                delete_preset_button = ui.button(
                    "Delete unused preset",
                    icon="delete",
                ).props("outline color=negative")

            ui.label(
                "Delete is fail-closed: the preset must be disabled and must "
                "not be referenced by any historical L2 hourly row. "
                "Analytical rows are never cascade-deleted."
            ).classes("text-xs text-gray-500")

        with ui.card().classes("w-full"):
            ui.label("Production diagnostics").classes("font-semibold")

            ui.label(
                "Build a read-only inventory of configured storage, "
                "PostgreSQL rows, source states, disk capacity, and raw "
                "archives eligible for future retention enforcement."
            ).classes("text-sm text-gray-600")

            ui.label(
                "The raw-retention report is a dry run. It does not delete "
                "files or mutate source metadata."
            ).classes("text-xs text-orange-700")

            with ui.row().classes("gap-2 flex-wrap"):
                run_diagnostics_button = ui.button(
                    "Run diagnostics",
                    icon="health_and_safety",
                ).props("color=primary")

                export_diagnostics_button = ui.button(
                    "Export diagnostics JSON",
                    icon="download",
                ).props("outline")

            export_diagnostics_button.disable()

            diagnostics_status = ui.label(
                "No production diagnostics report has been generated."
            ).classes("text-sm")

            with ui.grid(columns=2).classes("gap-x-6 gap-y-2 text-sm"):
                ui.label("Storage files")
                diagnostics_files = ui.label("-").classes("font-mono")

                ui.label("Storage bytes")
                diagnostics_bytes = ui.label("-").classes("font-mono")

                ui.label("Retention candidates")
                diagnostics_candidates = ui.label("-").classes("font-mono")

                ui.label("Reclaimable bytes")
                diagnostics_reclaimable = ui.label("-").classes("font-mono")

                ui.label("Blocked retention rows")
                diagnostics_blocked = ui.label("-").classes("font-mono")

                ui.label("Orphan checkpoints")
                diagnostics_orphan_checkpoints = ui.label("-").classes("font-mono")

                ui.label("Missing checkpoints")
                diagnostics_missing_checkpoints = ui.label("-").classes("font-mono")

                ui.label("Stale source rows")
                diagnostics_stale_sources = ui.label("-").classes("font-mono")

                ui.label("Stale running fetch runs")
                diagnostics_stale_fetch_runs = ui.label("-").classes("font-mono")

                ui.label("Missing analytical outputs")
                diagnostics_missing_outputs = ui.label("-").classes("font-mono")

                ui.label("L2 / price mismatches")
                diagnostics_series_mismatches = ui.label("-").classes("font-mono")

                ui.label("Large artifacts")
                diagnostics_large_artifacts = ui.label("-").classes("font-mono")

                ui.label("Report status")
                diagnostics_ok = ui.label("-").classes("font-mono")

        with ui.card().classes("w-full border border-orange-700"):
            ui.label("Explicit maintenance actions").classes(
                "text-lg font-semibold text-orange-700"
            )

            ui.label(
                "Every destructive action first builds a fresh preview. "
                "Execution requires explicit confirmation and revalidates the "
                "complete candidate set before mutation."
            ).classes("text-sm text-gray-600")

            ui.label(
                "These actions never delete or repair analytical rows. "
                "Automatic maintenance remains disabled."
            ).classes("text-xs text-orange-700")

            with ui.row().classes("gap-2 flex-wrap mt-2"):
                preview_stale_sources_button = ui.button(
                    "Preview stale-source recovery",
                    icon="restart_alt",
                ).props("outline color=warning")

                preview_orphan_checkpoints_button = ui.button(
                    "Preview orphan checkpoint deletion",
                    icon="delete_sweep",
                ).props("outline color=warning")

                preview_raw_pruning_button = ui.button(
                    "Preview raw-file pruning",
                    icon="folder_delete",
                ).props("outline color=negative")

            maintenance_status = ui.label(
                "No maintenance action has been previewed."
            ).classes("text-sm")

            maintenance_audit_status = ui.label(
                "No destructive maintenance action has run in this process."
            ).classes("text-xs text-gray-500")

            export_maintenance_audit_button = (
                ui.button(
                    "Export latest maintenance audit",
                    icon="download",
                )
                .props("outline")
                .disable()
            )

        with ui.card().classes("w-full border border-red-800"):
            ui.label("Application lifecycle").classes("font-semibold text-red-500")
            ui.label(
                "Application Shutdown blocks new operations, requests "
                "cooperative Fetch, Processing, and Analysis cancellation, "
                "waits for durable operation boundaries, cancels remaining "
                "tracked tasks, disposes the database engine, and then stops "
                "the local NiceGUI server."
            ).classes("text-sm text-gray-600")

            shutdown_button = ui.button(
                "Application Shutdown",
                icon="power_settings_new",
            ).props("color=negative")

    with ui.dialog() as preset_delete_dialog:
        with ui.card().classes("w-[34rem] max-w-full"):
            ui.label("Confirm preset deletion").classes(
                "text-lg font-bold text-red-500"
            )

            ui.label(
                "Only a disabled preset with no historical L2 rows can be "
                "deleted. This operation never deletes analytical rows."
            ).classes("text-sm")

            preset_delete_identity = ui.label("").classes("text-xs font-mono break-all")

            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button(
                    "Cancel",
                    on_click=preset_delete_dialog.close,
                ).props("flat")

                confirm_preset_delete_button = ui.button(
                    "Delete preset",
                    icon="delete_forever",
                ).props("color=negative")

    with ui.dialog() as maintenance_dialog:
        with ui.card().classes("w-[42rem] max-w-full"):
            ui.label("Confirm destructive maintenance").classes(
                "text-lg font-bold text-red-500"
            )

            maintenance_dialog_summary = ui.label("").classes(
                "text-sm whitespace-pre-wrap"
            )

            maintenance_dialog_token = ui.label("").classes(
                "text-xs font-mono break-all text-gray-500"
            )

            ui.label(
                "The preview is short-lived. If candidates change before "
                "execution, the action fails and a new preview is required."
            ).classes("text-xs text-orange-700")

            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button(
                    "Cancel",
                    on_click=maintenance_dialog.close,
                ).props("flat")

                confirm_maintenance_button = ui.button(
                    "Execute confirmed action",
                    icon="warning",
                ).props("color=negative")

    with ui.dialog() as shutdown_dialog:
        with ui.card().classes("w-[34rem] max-w-full"):
            ui.label("Confirm Application Shutdown").classes(
                "text-lg font-bold text-red-500"
            )
            ui.label(
                "The browser connection will close after active work is "
                "stopped safely. You will need to run run_app.bat again to "
                "restart the application."
            ).classes("text-sm")

            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button(
                    "Cancel",
                    on_click=shutdown_dialog.close,
                ).props("flat")

                confirm_button = ui.button(
                    "Stop application safely",
                    icon="power_settings_new",
                ).props("color=negative")

    def _selected_managed_preset() -> ManagedPreset | None:
        selected_hash = str(preset_select.value or "").strip()

        if not selected_hash:
            return None

        return next(
            (
                preset
                for preset in managed_presets
                if preset.preset_hash == selected_hash
            ),
            None,
        )

    def _set_preset_controls_enabled(enabled: bool) -> None:
        controls = (
            preset_base,
            preset_market_profile,
            preset_lower,
            preset_upper,
            preset_enabled_on_create,
            preset_select,
            add_preset_button,
            save_edited_preset_button,
            refresh_presets_button,
            load_preset_button,
            enable_preset_button,
            disable_preset_button,
            delete_preset_button,
        )

        for control in controls:
            if enabled:
                control.enable()
            else:
                control.disable()

        if not managed_presets:
            preset_select.disable()
            load_preset_button.disable()
            enable_preset_button.disable()
            disable_preset_button.disable()
            delete_preset_button.disable()
            save_edited_preset_button.disable()

    async def _refresh_managed_presets() -> None:
        nonlocal managed_presets
        nonlocal preset_refresh_running

        if preset_refresh_running or preset_mutation_running:
            return

        preset_refresh_running = True
        refresh_presets_button.disable()
        preset_status.set_text("Loading persisted data presets...")

        try:
            loaded = await asyncio.to_thread(
                list_managed_presets,
            )
            managed_presets = loaded

            previous = str(preset_select.value or "").strip()
            options = {preset.preset_hash: preset.label for preset in loaded}

            preset_select.options = options

            if previous in options:
                preset_select.value = previous
            elif loaded:
                preset_select.value = loaded[0].preset_hash
            else:
                preset_select.value = None

            preset_select.update()

            enabled_count = sum(preset.enabled for preset in loaded)
            referenced_editor_count = sum(
                preset.current_editor_supported for preset in loaded
            )

            preset_status.set_text(
                f"{len(loaded)} preset(s); "
                f"enabled={enabled_count}; "
                f"current-editor-compatible={referenced_editor_count}."
            )

        except Exception as exc:
            log.exception("Could not load managed data presets.")
            preset_status.set_text(f"Preset refresh failed with {type(exc).__name__}.")
            persistent_notify(
                f"Could not load data presets: {type(exc).__name__}",
                title="Data presets",
                notification_type="negative",
            )

        finally:
            preset_refresh_running = False
            _set_preset_controls_enabled(
                not state.shutdown_started and not preset_mutation_running
            )

    async def _run_preset_mutation(
        action: Callable[[], object],
        *,
        success_message: str,
    ) -> None:
        nonlocal preset_mutation_running

        if state.shutdown_started:
            persistent_notify(
                "Application shutdown has started; preset changes are blocked.",
                title="Data presets",
                notification_type="negative",
            )
            return

        if preset_mutation_running:
            return

        if state.active_operation_name:
            persistent_notify(
                f"Another operation is active: {state.active_operation_name}",
                title="Data presets",
                notification_type="negative",
            )
            return

        operation_lock = state.operation_lock

        if operation_lock.locked():
            persistent_notify(
                "Another application operation currently owns the operation lock.",
                title="Data presets",
                notification_type="negative",
            )
            return

        await operation_lock.acquire()
        preset_mutation_running = True
        state.active_operation_name = "preset_management"
        state.active_operation_started_at = None
        _set_preset_controls_enabled(False)

        try:
            result = await asyncio.to_thread(action)

            ui.notify(
                success_message,
                type="positive",
                timeout=5000,
            )

            preset_mutation_running = False
            await _refresh_managed_presets()
            preset_mutation_running = True

            selected_result: ManagedPreset | None

            if isinstance(result, ManagedPreset):
                selected_result = result
            elif isinstance(result, ManagedPresetWriteResult):
                selected_result = result.preset
            else:
                selected_result = None

            if selected_result is not None:
                preset_select.value = selected_result.preset_hash
                preset_select.update()

        except PresetManagementError as exc:
            persistent_notify(
                str(exc),
                title="Data presets",
                notification_type="negative",
            )

        except Exception as exc:
            log.exception("Preset mutation failed.")
            persistent_notify(
                f"Preset mutation failed with {type(exc).__name__}.",
                title="Data presets",
                notification_type="negative",
            )

        finally:
            if state.active_operation_name == "preset_management":
                state.active_operation_name = ""
                state.active_operation_started_at = None

            preset_mutation_running = False

            if operation_lock.locked():
                operation_lock.release()

            _set_preset_controls_enabled(not state.shutdown_started)

    async def _add_preset() -> None:
        await _run_preset_mutation(
            lambda: create_managed_preset(
                base=preset_base.value,
                lower_fraction=preset_lower.value,
                upper_fraction=preset_upper.value,
                market_profile=preset_market_profile.value,
                enabled=bool(preset_enabled_on_create.value),
            ),
            success_message="Data preset creation completed.",
        )

    async def _save_edited_preset() -> None:
        selected = _selected_managed_preset()

        if selected is None:
            persistent_notify(
                "Select a persisted preset before saving an edited version.",
                title="Data presets",
                notification_type="negative",
            )
            return

        await _run_preset_mutation(
            lambda: create_edited_preset(
                source_preset_hash=selected.preset_hash,
                lower_fraction=preset_lower.value,
                upper_fraction=preset_upper.value,
                market_profile=preset_market_profile.value,
                enabled=bool(preset_enabled_on_create.value),
            ),
            success_message=(
                "Edited semantic values were saved as a new preset identity."
            ),
        )

    def _load_selected_preset() -> None:
        selected = _selected_managed_preset()

        if selected is None:
            persistent_notify(
                "Select a persisted data preset first.",
                title="Data presets",
                notification_type="negative",
            )
            return

        if not selected.current_editor_supported:
            persistent_notify(
                "The selected market configuration is not supported by the "
                "current editor.",
                title="Data presets",
                notification_type="warning",
            )
            return

        if selected.market_profile is None:
            persistent_notify(
                "The selected preset has no recognized editor market profile.",
                title="Data presets",
                notification_type="warning",
            )
            return

        preset_base.value = selected.base
        preset_market_profile.value = selected.market_profile.value
        preset_lower.value = str(selected.lower_fraction)
        preset_upper.value = str(selected.upper_fraction)
        preset_enabled_on_create.value = selected.enabled

        preset_base.update()
        preset_market_profile.update()
        preset_lower.update()
        preset_upper.update()
        preset_enabled_on_create.update()

        preset_status.set_text(
            "Selected preset loaded into the editor. Saving semantic changes "
            "will create a new hash."
        )

    async def _set_selected_enabled(enabled: bool) -> None:
        selected = _selected_managed_preset()

        if selected is None:
            persistent_notify(
                "Select a persisted data preset first.",
                title="Data presets",
                notification_type="negative",
            )
            return

        await _run_preset_mutation(
            lambda: set_managed_preset_enabled(
                selected.preset_hash,
                enabled=enabled,
            ),
            success_message=(
                "Data preset enabled." if enabled else "Data preset disabled."
            ),
        )

    def _open_preset_delete_dialog() -> None:
        selected = _selected_managed_preset()

        if selected is None:
            persistent_notify(
                "Select a persisted data preset first.",
                title="Data presets",
                notification_type="negative",
            )
            return

        preset_delete_identity.set_text(f"{selected.base} | {selected.preset_hash}")
        preset_delete_dialog.open()

    async def _confirmed_preset_delete() -> None:
        selected = _selected_managed_preset()

        if selected is None:
            preset_delete_dialog.close()
            return

        preset_delete_dialog.close()

        await _run_preset_mutation(
            lambda: delete_managed_preset(
                selected.preset_hash,
            ),
            success_message="Unused data preset deleted.",
        )

    def _set_maintenance_controls_enabled(
        enabled: bool,
    ) -> None:
        controls = (
            preview_stale_sources_button,
            preview_orphan_checkpoints_button,
            preview_raw_pruning_button,
        )

        for control in controls:
            if enabled:
                control.enable()
            else:
                control.disable()

        if latest_maintenance_audit is None:
            export_maintenance_audit_button.disable()
        elif enabled:
            export_maintenance_audit_button.enable()

    async def _preview_maintenance(
        action: MaintenanceActionKind,
    ) -> None:
        nonlocal pending_maintenance_preview

        if state.shutdown_started:
            persistent_notify(
                "Application shutdown has started; maintenance is blocked.",
                title="Maintenance",
                notification_type="negative",
            )
            return

        if maintenance_running:
            return

        if state.active_operation_name:
            persistent_notify(
                f"Another operation is active: {state.active_operation_name}",
                title="Maintenance",
                notification_type="negative",
            )
            return

        maintenance_status.set_text(f"Building {action.value} preview...")
        _set_maintenance_controls_enabled(False)

        try:
            preview = await asyncio.to_thread(
                build_maintenance_preview,
                action,
            )
            pending_maintenance_preview = preview

            maintenance_status.set_text(
                f"Preview ready: {preview.candidate_count} candidate(s), "
                f"{preview.candidate_bytes:,} byte(s), "
                f"{preview.blocked_count} blocked item(s)."
            )

            maintenance_dialog_summary.set_text(
                f"Action: {preview.action.value}\n"
                f"Candidates: {preview.candidate_count:,}\n"
                f"Candidate bytes: {preview.candidate_bytes:,}\n"
                f"Blocked: {preview.blocked_count:,}\n"
                f"Preview expires: {preview.expires_at_utc.isoformat()}"
            )
            maintenance_dialog_token.set_text(f"Preview token: {preview.token}")

            if preview.candidate_count == 0:
                ui.notify(
                    "The preview contains no eligible maintenance candidates.",
                    type="info",
                )
                return

            maintenance_dialog.open()

        except Exception as exc:
            log.exception("Maintenance preview failed.")
            pending_maintenance_preview = None
            maintenance_status.set_text(
                f"Maintenance preview failed with {type(exc).__name__}."
            )
            persistent_notify(
                f"Maintenance preview failed with {type(exc).__name__}.",
                title="Maintenance",
                notification_type="negative",
            )

        finally:
            _set_maintenance_controls_enabled(
                not state.shutdown_started and not maintenance_running
            )

    async def _confirmed_maintenance() -> None:
        nonlocal latest_maintenance_audit
        nonlocal maintenance_running
        nonlocal pending_maintenance_preview

        preview = pending_maintenance_preview

        if preview is None:
            maintenance_dialog.close()
            persistent_notify(
                "No current maintenance preview is available.",
                title="Maintenance",
                notification_type="negative",
            )
            return

        if state.shutdown_started:
            maintenance_dialog.close()
            persistent_notify(
                "Application shutdown has started; maintenance is blocked.",
                title="Maintenance",
                notification_type="negative",
            )
            return

        if state.active_operation_name:
            maintenance_dialog.close()
            persistent_notify(
                f"Another operation is active: {state.active_operation_name}",
                title="Maintenance",
                notification_type="negative",
            )
            return

        operation_lock = state.operation_lock

        if operation_lock.locked():
            maintenance_dialog.close()
            persistent_notify(
                "Another application operation owns the operation lock.",
                title="Maintenance",
                notification_type="negative",
            )
            return

        maintenance_dialog.close()
        await operation_lock.acquire()

        maintenance_running = True
        state.active_operation_name = "settings_maintenance"
        state.active_operation_started_at = None
        _set_maintenance_controls_enabled(False)
        confirm_maintenance_button.disable()

        try:
            audit = await asyncio.to_thread(
                execute_maintenance_action,
                preview,
            )
            latest_maintenance_audit = audit
            pending_maintenance_preview = None

            maintenance_audit_status.set_text(
                f"Last action: {audit.action.value}; "
                f"status={audit.status}; "
                f"succeeded={audit.succeeded_count}; "
                f"failed={audit.failed_count}; "
                f"affected bytes={audit.affected_bytes:,}."
            )
            export_maintenance_audit_button.enable()

            notification_type = (
                "positive"
                if audit.status == "ok"
                else ("warning" if audit.status == "partial_ok" else "negative")
            )

            persistent_notify(
                maintenance_audit_status.text,
                title="Maintenance completed",
                notification_type=notification_type,
            )

        except Exception as exc:
            log.exception("Maintenance execution failed.")
            maintenance_audit_status.set_text(
                f"Maintenance failed with {type(exc).__name__}."
            )
            persistent_notify(
                (
                    "Maintenance execution failed with "
                    f"{type(exc).__name__}. Build and review a new preview."
                ),
                title="Maintenance",
                notification_type="negative",
            )

        finally:
            if state.active_operation_name == "settings_maintenance":
                state.active_operation_name = ""
                state.active_operation_started_at = None

            maintenance_running = False
            confirm_maintenance_button.enable()

            if operation_lock.locked():
                operation_lock.release()

            _set_maintenance_controls_enabled(not state.shutdown_started)

    def _export_maintenance_audit() -> None:
        audit = latest_maintenance_audit

        if audit is None:
            ui.notify(
                "No maintenance audit is available.",
                type="warning",
            )
            return

        timestamp = audit.completed_at_utc.strftime("%Y%m%dT%H%M%SZ")
        filename = f"l2shock-maintenance-{audit.action.value}-" f"{timestamp}.json"

        ui.download(
            report_json_bytes(audit.to_dict()),
            filename=filename,
            media_type="application/json",
        )

    def _update_diagnostics_view(
        report: dict[str, object],
    ) -> None:
        storage = report.get("storage")
        retention = report.get("raw_retention_dry_run")

        storage_mapping: dict[str, Any] = storage if isinstance(storage, dict) else {}
        totals = storage_mapping.get("totals")
        totals_mapping: dict[str, Any] = totals if isinstance(totals, dict) else {}

        retention_mapping: dict[str, Any] = (
            retention if isinstance(retention, dict) else {}
        )

        maintenance = report.get("maintenance_diagnostics")
        maintenance_mapping: dict[str, Any] = (
            maintenance if isinstance(maintenance, dict) else {}
        )

        checkpoint = maintenance_mapping.get("checkpoint_storage")
        checkpoint_mapping: dict[str, Any] = (
            checkpoint if isinstance(checkpoint, dict) else {}
        )

        stale = maintenance_mapping.get("stale_sources")
        stale_mapping: dict[str, Any] = stale if isinstance(stale, dict) else {}

        stale_fetch_runs = maintenance_mapping.get("stale_fetch_runs")
        stale_fetch_runs_mapping: dict[str, Any] = (
            stale_fetch_runs if isinstance(stale_fetch_runs, dict) else {}
        )

        consistency = maintenance_mapping.get("analytical_consistency")
        consistency_mapping: dict[str, Any] = (
            consistency if isinstance(consistency, dict) else {}
        )

        diagnostics_files.set_text(f"{int(totals_mapping.get('file_count') or 0):,}")
        diagnostics_bytes.set_text(f"{int(totals_mapping.get('total_bytes') or 0):,}")
        diagnostics_candidates.set_text(
            f"{int(retention_mapping.get('candidate_count') or 0):,}"
        )
        diagnostics_reclaimable.set_text(
            f"{int(retention_mapping.get('reclaimable_bytes') or 0):,}"
        )
        diagnostics_blocked.set_text(
            f"{int(retention_mapping.get('blocked_count') or 0):,}"
        )

        diagnostics_orphan_checkpoints.set_text(
            f"{int(checkpoint_mapping.get('orphan_artifact_count') or 0):,}"
        )
        diagnostics_missing_checkpoints.set_text(
            f"{int(checkpoint_mapping.get('missing_expected_count') or 0):,}"
        )
        diagnostics_stale_sources.set_text(f"{(
                int(stale_mapping.get('stale_downloading_count') or 0)
                + int(stale_mapping.get('stale_processing_count') or 0)
            ):,}")
        diagnostics_stale_fetch_runs.set_text(f"{int(
                stale_fetch_runs_mapping.get(
                    'stale_running_fetch_run_count'
                )
                or 0
            ):,}")
        diagnostics_missing_outputs.set_text(f"{int(
                consistency_mapping.get(
                    'missing_processed_output_count'
                )
                or 0
            ):,}")
        diagnostics_series_mismatches.set_text(f"{(
                int(consistency_mapping.get('l2_without_price_count') or 0)
                + int(consistency_mapping.get('price_without_l2_count') or 0)
            ):,}")
        diagnostics_large_artifacts.set_text(f"{(
                int(checkpoint_mapping.get('large_artifact_count') or 0)
                + int(consistency_mapping.get('large_l2_row_count') or 0)
                + int(consistency_mapping.get('large_price_row_count') or 0)
            ):,}")

        report_ok = report.get("ok") is True
        diagnostics_ok.set_text("OK" if report_ok else "ATTENTION")

        generated = str(report.get("generated_at_utc") or "")
        diagnostics_status.set_text(
            "Latest report generated at " f"{generated or 'an unknown time'}."
        )

    async def _run_diagnostics() -> None:
        nonlocal latest_report

        if run_diagnostics_button.enabled is False:
            return

        run_diagnostics_button.disable()
        export_diagnostics_button.disable()
        diagnostics_status.set_text("Building production diagnostics report...")

        try:
            report = await asyncio.to_thread(
                production_diagnostics_report,
            )
            latest_report = report
            _update_diagnostics_view(report)
            export_diagnostics_button.enable()

            warnings = report.get("warnings")
            warning_count = len(warnings) if isinstance(warnings, list) else 0

            if report.get("ok") is True:
                ui.notify(
                    "Production diagnostics completed.",
                    type="positive",
                )
            else:
                persistent_notify(
                    "Production diagnostics completed with "
                    f"{warning_count} warning(s). Export the report for "
                    "details.",
                    title="Production Diagnostics",
                    notification_type="warning",
                )

        except asyncio.CancelledError:
            diagnostics_status.set_text("Production diagnostics were cancelled.")
            raise

        except Exception as exc:
            diagnostics_status.set_text(
                f"Diagnostics failed with {type(exc).__name__}."
            )

            persistent_notify(
                f"Production diagnostics failed with " f"{type(exc).__name__}.",
                title="Production Diagnostics",
                notification_type="negative",
            )

        finally:
            if not state.shutdown_started:
                run_diagnostics_button.enable()

    def _export_diagnostics() -> None:
        report = latest_report

        if report is None:
            ui.notify(
                "Run production diagnostics before exporting.",
                type="warning",
            )
            return

        ui.download(
            report_json_bytes(report),
            filename=_diagnostics_export_filename(report),
            media_type="application/json",
        )

    def _open_shutdown_dialog() -> None:
        if state.shutdown_started:
            ui.notify(
                "Application shutdown is already in progress.",
                type="warning",
            )
            return

        shutdown_dialog.open()

    async def _confirmed_shutdown() -> None:
        if state.shutdown_started:
            return

        shutdown_button.disable()
        confirm_button.disable()
        run_diagnostics_button.disable()
        export_diagnostics_button.disable()
        shutdown_dialog.close()

        persistent_notify(
            "Safe application shutdown has started. Active operations are "
            "being stopped and finalized.",
            title="Application Shutdown",
            notification_type="warning",
        )

        task = create_tracked_task(
            shutdown_runtime(request_server_stop=True),
            name="l2shock-application-shutdown",
        )

        if task is None:
            persistent_notify(
                "Could not create the application shutdown task.",
                title="Application Shutdown",
                notification_type="negative",
            )

    add_preset_button.on_click(_add_preset)
    save_edited_preset_button.on_click(_save_edited_preset)
    refresh_presets_button.on_click(_refresh_managed_presets)
    load_preset_button.on_click(_load_selected_preset)
    enable_preset_button.on_click(lambda: _set_selected_enabled(True))
    disable_preset_button.on_click(lambda: _set_selected_enabled(False))
    delete_preset_button.on_click(_open_preset_delete_dialog)
    confirm_preset_delete_button.on_click(_confirmed_preset_delete)
    preview_stale_sources_button.on_click(
        lambda: _preview_maintenance(MaintenanceActionKind.RECOVER_STALE_SOURCES)
    )
    preview_orphan_checkpoints_button.on_click(
        lambda: _preview_maintenance(MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS)
    )
    preview_raw_pruning_button.on_click(
        lambda: _preview_maintenance(MaintenanceActionKind.PRUNE_RAW_FILES)
    )
    confirm_maintenance_button.on_click(_confirmed_maintenance)
    export_maintenance_audit_button.on_click(_export_maintenance_audit)

    run_diagnostics_button.on_click(_run_diagnostics)
    export_diagnostics_button.on_click(_export_diagnostics)
    shutdown_button.on_click(_open_shutdown_dialog)
    confirm_button.on_click(_confirmed_shutdown)

    ui.timer(
        interval=0.1,
        callback=_refresh_managed_presets,
        once=True,
    )


__all__ = [
    "_diagnostics_export_filename",
    "build_settings_tab",
]

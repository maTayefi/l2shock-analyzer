# l2shock/ui/tab_fetch.py
"""Functional manual-fetch and downloaded-source processing controls."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from nicegui import ui

from l2shock.acquisition import FetchOperationBusyError
from l2shock.config import get_settings
from l2shock.timeutils import (
    floor_to_hour,
    local_to_utc,
    now_utc,
    utc_to_local,
)
from l2shock.ui.availability_calendar import (
    AnalysisRangeHandoff,
    AvailabilityCalendarBaseSnapshot,
    AvailabilityCalendarSnapshot,
    availability_hour_tooltip,
    availability_state_color,
    availability_state_label,
    format_contiguous_window_local,
    load_availability_calendar_snapshot,
    parse_local_calendar_date,
)
from l2shock.ui.components import (
    create_tracked_task,
    persistent_notify,
    section_header,
)
from l2shock.ui.fetch_runtime import get_manual_fetch_runtime
from l2shock.ui.automatic_fetch_runtime import (
    get_automatic_fetch_runtime,
)
from l2shock.ui.processing_runtime import (
    ProcessingRuntimeBusyError,
    get_manual_processing_runtime,
)
from l2shock.ui.remote_import_runtime import (
    RemoteImportRuntime,
    RemoteImportRuntimeBusyError,
    get_remote_import_runtime,
)
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)


def _local_input_values(value_utc: datetime) -> tuple[str, str]:
    settings = get_settings()
    local = utc_to_local(value_utc, settings.app.timezone)

    return (
        local.strftime("%Y-%m-%d"),
        local.strftime("%H:%M"),
    )


def _parse_local_datetime(
    date_text: str,
    time_text: str,
    *,
    field_name: str,
) -> datetime:
    settings = get_settings()

    date_value = str(date_text or "").strip()
    time_value = str(time_text or "").strip()

    if not date_value or not time_value:
        raise ValueError(f"{field_name} date and time are required")

    try:
        local_naive = datetime.fromisoformat(f"{date_value}T{time_value}")
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must contain a valid local date and time"
        ) from exc

    return local_to_utc(
        local_naive,
        settings.app.timezone,
        original_text=f"{date_value} {time_value}",
    )


def _parse_depth_fraction(
    raw_value: object,
    *,
    field_name: str,
) -> Decimal:
    """Parse one finite Decimal depth fraction from a UI value."""
    text = str(raw_value or "").strip()

    if not text:
        raise ValueError(f"{field_name} is required")

    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid decimal fraction") from exc

    if not value.is_finite():
        raise ValueError(f"{field_name} must be a finite decimal fraction")

    return value


def _parse_depth_band(
    lower_value: object,
    upper_value: object,
) -> tuple[Decimal, Decimal]:
    """Parse and validate the DepthBand-compatible UI fractions."""
    lower = _parse_depth_fraction(
        lower_value,
        field_name="Depth lower fraction",
    )
    upper = _parse_depth_fraction(
        upper_value,
        field_name="Depth upper fraction",
    )

    if lower < Decimal("0"):
        raise ValueError("Depth lower fraction must be greater than or equal to 0")

    if upper >= Decimal("1"):
        raise ValueError("Depth upper fraction must be less than 1")

    if lower > upper:
        raise ValueError(
            "Depth lower fraction must be less than or equal to "
            "the depth upper fraction"
        )

    return lower, upper


def build_fetch_tab(
    *,
    on_analysis_handoff: Callable[[AnalysisRangeHandoff], object] | None = None,
) -> None:
    settings = get_settings()
    state = get_state()
    fetch_runtime = get_manual_fetch_runtime()
    automatic_runtime = get_automatic_fetch_runtime()
    processing_runtime = get_manual_processing_runtime()

    remote_runtime: RemoteImportRuntime | None = None
    remote_runtime_configuration_error: str | None = None

    try:
        remote_runtime = get_remote_import_runtime()
    except Exception as exc:
        # Configuration failures must not remove the existing local fallback.
        remote_runtime_configuration_error = f"Unexpected {type(exc).__name__}"
        log.warning(
            "Remote HF Import is unavailable: %s.",
            type(exc).__name__,
        )

    default_end_utc = floor_to_hour(now_utc())
    default_start_utc = default_end_utc - timedelta(hours=1)

    start_date_value, start_time_value = _local_input_values(default_start_utc)
    end_date_value, end_time_value = _local_input_values(default_end_utc)

    observed_fetch_completion_sequence = fetch_runtime.snapshot().completion_sequence
    observed_processing_completion_sequence = (
        processing_runtime.snapshot().completion_sequence
    )
    observed_automatic_completion_sequence = (
        automatic_runtime.snapshot().completion_sequence
    )
    observed_remote_completion_sequence = (
        remote_runtime.snapshot().completion_sequence
        if remote_runtime is not None
        else 0
    )

    calendar_refresh_running = False
    calendar_refresh_pending = False
    latest_calendar_snapshot: AvailabilityCalendarSnapshot | None = None

    local_today = utc_to_local(
        now_utc(),
        settings.app.timezone,
    ).date()

    with ui.column().classes("w-full gap-3 p-4"):
        section_header("Fetch \u2014 CryptoHFTData hourly files")

        with ui.card().classes("w-full"):
            ui.label("Data workflow").classes("text-lg font-semibold")

            workflow_profile = ui.select(
                options={
                    "remote_hf_import": "Remote HF Import (default)",
                    "local_fetch_processing": (
                        "Local CryptoHFTData Fetch + Processing"
                    ),
                },
                value=settings.remote.default_workflow,
                label="Workflow profile",
            ).classes("w-[32rem] max-w-full")

            ui.label(
                "Remote HF Import downloads verified compact processed "
                "artifacts into local PostgreSQL. The existing local Fetch "
                "and Processing workflow remains available as a fallback."
            ).classes("text-sm text-gray-600")

        with ui.card().classes("w-full") as remote_import_card:
            ui.label("Remote Hugging Face Import").classes("text-lg font-semibold")

            ui.label(
                "Import verified Binance, Bybit, and OKX component L2 "
                "artifacts plus Binance real-trade price artifacts from one "
                "pinned private Hugging Face dataset revision."
            ).classes("text-sm text-gray-600")

            if remote_runtime is None:
                ui.label(
                    "Remote HF Import is not configured. Set "
                    "remote.hf_repo_id in config.yaml and "
                    "L2SHOCK__REMOTE__HF_TOKEN in .env, then restart."
                ).classes("text-sm text-red-600")

                if remote_runtime_configuration_error is not None:
                    ui.label(
                        "Configuration state: " + remote_runtime_configuration_error
                    ).classes("text-xs font-mono text-gray-500")

            with ui.row().classes("w-full gap-3 flex-wrap mt-3"):
                remote_start_date = (
                    ui.input(
                        label="Remote import start date",
                        value=start_date_value,
                    )
                    .props("type=date")
                    .classes("w-48")
                )
                remote_start_time = (
                    ui.input(
                        label="Remote import start time (24-hour HH:MM)",
                        value=start_time_value,
                    )
                    .props('type=text inputmode=numeric placeholder="HH:MM"')
                    .classes("w-44")
                )
                remote_end_date = (
                    ui.input(
                        label="Remote import end date",
                        value=end_date_value,
                    )
                    .props("type=date")
                    .classes("w-48")
                )
                remote_end_time = (
                    ui.input(
                        label="Remote import end time (24-hour HH:MM)",
                        value=end_time_value,
                    )
                    .props('type=text inputmode=numeric placeholder="HH:MM"')
                    .classes("w-44")
                )

            ui.label(
                "Remote ranges must convert to exact UTC-hour boundaries and "
                "use half-open [start, end) semantics."
            ).classes("text-xs text-gray-500")

            with ui.row().classes("w-full gap-4 flex-wrap mt-3"):
                remote_btc = ui.checkbox(
                    "BTC",
                    value=True,
                )
                remote_eth = ui.checkbox(
                    "ETH",
                    value=False,
                )

                remote_depth_lower = (
                    ui.input(
                        label="Remote depth lower fraction",
                        value="0",
                    )
                    .props("type=number min=0 max=0.999999 step=0.0001")
                    .classes("w-60")
                )
                remote_depth_upper = (
                    ui.input(
                        label="Remote depth upper fraction",
                        value="0.01",
                    )
                    .props("type=number min=0 max=0.999999 step=0.0001")
                    .classes("w-60")
                )

            with ui.row().classes("gap-2 mt-3"):
                start_remote_import_button = ui.button(
                    "Start Remote Import",
                    icon="cloud_download",
                ).props("color=primary")

                stop_remote_import_button = (
                    ui.button(
                        "Stop Remote Import",
                        icon="stop_circle",
                    )
                    .props("outline color=negative")
                    .disable()
                )

            remote_progress_bar = ui.linear_progress(
                value=0.0,
                show_value=False,
            ).classes("w-full mt-3")

            remote_status_label = ui.label("Idle.").classes("text-sm font-semibold")
            remote_revision_label = ui.label("Pinned revision: none").classes(
                "text-xs font-mono break-all"
            )
            remote_current_artifact_label = ui.label("Current artifact: none").classes(
                "text-xs font-mono break-all"
            )
            remote_counter_label = ui.label(
                "Completed 0 / 0 | imported=0 | reused=0 | " "missing=0 | failed=0"
            ).classes("text-xs font-mono")
            remote_result_label = ui.label(
                "No remote HF import has completed in this process."
            ).classes("text-sm text-gray-600")

        with ui.card().classes("w-full") as local_fetch_card:
            ui.label(
                "Manual Fetch downloads BTC and ETH Binance Futures "
                "order-book/trade archives plus empirically supported Bybit "
                "and OKX Futures order-book archives."
            ).classes("font-semibold")

            ui.label(
                f"All input times use {settings.app.timezone}. "
                "The requested interval is converted strictly to UTC and uses "
                "half-open [start, end) semantics."
            ).classes("text-sm text-gray-600")

            with ui.row().classes("w-full gap-3 flex-wrap mt-3"):
                start_date = (
                    ui.input(
                        label="Start date",
                        value=start_date_value,
                    )
                    .props("type=date")
                    .classes("w-44")
                )
                start_time = (
                    ui.input(
                        label="Start time (24-hour HH:MM)",
                        value=start_time_value,
                    )
                    .props('type=text inputmode=numeric placeholder="HH:MM"')
                    .classes("w-40")
                )
                end_date = (
                    ui.input(
                        label="End date",
                        value=end_date_value,
                    )
                    .props("type=date")
                    .classes("w-44")
                )
                end_time = (
                    ui.input(
                        label="End time (24-hour HH:MM)",
                        value=end_time_value,
                    )
                    .props('type=text inputmode=numeric placeholder="HH:MM"')
                    .classes("w-40")
                )

            with ui.row().classes("gap-2 mt-3"):
                manual_button = ui.button(
                    "Manual Fetch",
                    icon="cloud_download",
                ).props("color=primary")

                automatic_start_button = ui.button(
                    "Start Automatic Fetch",
                    icon="schedule",
                ).props("color=secondary")

                automatic_stop_button = ui.button(
                    "Stop Automatic Fetch",
                    icon="stop_circle",
                ).props("outline color=secondary")

                stop_button = (
                    ui.button(
                        "Stop Fetch",
                        icon="stop_circle",
                    )
                    .props("color=negative outline")
                    .disable()
                )

            automatic_status_label = ui.label("Automatic Fetch is inactive.").classes(
                "text-xs text-gray-500"
            )

        with ui.card().classes("w-full"):
            ui.label("Hourly data availability").classes("text-lg font-semibold")

            ui.label(
                "Each cell is one exact CryptoHFTData UTC source hour. "
                f"Cell labels and contiguous-window times are displayed in "
                f"{settings.app.timezone}. Availability is evaluated against "
                "the newest enabled data preset for each base."
            ).classes("text-sm text-gray-600")

            with ui.row().classes("w-full gap-2 items-end flex-wrap mt-3"):
                previous_calendar_date_button = ui.button(
                    "Previous day",
                    icon="chevron_left",
                ).props("outline dense")

                calendar_date_input = (
                    ui.input(
                        label=(f"Local calendar date " f"({settings.app.timezone})"),
                        value=local_today.isoformat(),
                    )
                    .props("type=date")
                    .classes("w-56")
                )

                next_calendar_date_button = ui.button(
                    "Next day",
                    icon="chevron_right",
                ).props("outline dense")

                refresh_calendar_button = ui.button(
                    "Refresh availability",
                    icon="refresh",
                ).props("outline color=secondary")

            with ui.row().classes("w-full gap-3 flex-wrap items-center mt-3"):
                for state_value in (
                    "unknown",
                    "missing",
                    "partial",
                    "downloaded",
                    "materialized",
                    "analyzable",
                ):
                    from l2shock.acquisition import (
                        HourAvailabilityState,
                    )

                    availability_state = HourAvailabilityState(state_value)

                    ui.badge(
                        availability_state_label(availability_state),
                        color=availability_state_color(availability_state),
                    )

            calendar_status_label = ui.label(
                "Availability has not been loaded."
            ).classes("text-xs text-gray-500")

            calendar_grid = ui.column().classes("w-full gap-3 mt-2")

        with ui.card().classes("w-full") as local_processing_card:
            ui.label("Processing").classes("text-lg font-semibold")

            ui.label(
                "Process downloaded Binance, Bybit, and OKX Futures order "
                "books into independent single-market compact L2 series, and "
                "Binance Futures trades into the fixed real-price series."
            ).classes("text-sm text-gray-600")

            ui.label(
                f"Processing times use {settings.app.timezone} and the same "
                "half-open [start, end) range semantics as Manual Fetch."
            ).classes("text-sm text-gray-600")

            with ui.row().classes("w-full gap-3 flex-wrap mt-3"):
                processing_start_date = (
                    ui.input(
                        label="Processing start date",
                        value=start_date_value,
                    )
                    .props("type=date")
                    .classes("w-44")
                )
                processing_start_time = (
                    ui.input(
                        label="Processing start time",
                        value=start_time_value,
                    )
                    .props("type=time step=60")
                    .classes("w-40")
                )
                processing_end_date = (
                    ui.input(
                        label="Processing end date",
                        value=end_date_value,
                    )
                    .props("type=date")
                    .classes("w-44")
                )
                processing_end_time = (
                    ui.input(
                        label="Processing end time",
                        value=end_time_value,
                    )
                    .props("type=time step=60")
                    .classes("w-40")
                )

            with ui.row().classes("w-full gap-3 flex-wrap mt-3"):
                depth_lower_fraction = (
                    ui.input(
                        label="Depth lower fraction",
                        value="0",
                    )
                    .props("type=number min=0 max=0.999999 step=0.0001")
                    .classes("w-56")
                )
                depth_upper_fraction = (
                    ui.input(
                        label="Depth upper fraction",
                        value="0.01",
                    )
                    .props("type=number min=0 max=0.999999 step=0.0001")
                    .classes("w-56")
                )

            ui.label(
                "Depth fractions use exact Decimal values and must satisfy "
                "0 \u2264 lower \u2264 upper < 1."
            ).classes("text-xs text-gray-500")

            with ui.row().classes("gap-2 mt-3"):
                process_button = ui.button(
                    "Process Downloaded Data",
                    icon="precision_manufacturing",
                ).props("color=primary")

                stop_processing_button = (
                    ui.button(
                        "Stop Processing",
                        icon="stop_circle",
                    )
                    .props("color=negative outline")
                    .disable()
                )

            processing_progress_bar = ui.linear_progress(
                value=0.0,
                show_value=False,
            ).classes("w-full mt-3")

            processing_status_label = ui.label("Idle.").classes("text-sm font-semibold")
            processing_target_label = ui.label("Current source target: none").classes(
                "text-xs font-mono break-all"
            )
            processing_counter_label = ui.label("Completed 0 / 0 | failed=0").classes(
                "text-xs font-mono"
            )
            processing_result_label = ui.label(
                "No manual processing operation has completed in this process."
            ).classes("text-sm text-gray-600")

        with ui.card().classes("w-full"):
            ui.label("Live fetch status").classes("font-semibold")

            progress_bar = ui.linear_progress(
                value=0.0,
                show_value=False,
            ).classes("w-full")

            status_label = ui.label("Idle.").classes("text-sm font-semibold")
            current_file_label = ui.label("Current file: none").classes(
                "text-xs font-mono break-all"
            )
            counter_label = ui.label("Completed 0 / 0").classes("text-xs font-mono")
            result_label = ui.label(
                "No manual fetch has completed in this process."
            ).classes("text-sm text-gray-600")

        with ui.card().classes("w-full") as local_transport_card:
            ui.label("Transport and retention").classes("font-semibold")

            with ui.row().classes("gap-3 flex-wrap"):
                ui.label(
                    "REST admission: "
                    f"{settings.cryptohft.download_rate_limit_per_minute}/min"
                ).classes("font-mono text-sm")

                ui.label(
                    "Expected release delay: "
                    f"{settings.cryptohft.expected_release_delay_minutes} min"
                ).classes("font-mono text-sm")

                ui.label(
                    f"Raw retention: " f"{settings.storage.raw_retention_hours} hours"
                ).classes("font-mono text-sm")

            ui.label(
                "Downloaded archives remain analytically INVALID until the "
                "implemented reconstruction and trade-price processing "
                "coordinators prove initialization, continuity, and source "
                "quality. Invalid observations are never forward-filled."
            ).classes("text-xs text-orange-700")

    def _sync_workflow_profile() -> None:
        remote_selected = (
            str(workflow_profile.value or "").strip() == "remote_hf_import"
        )

        remote_import_card.set_visibility(remote_selected)
        local_fetch_card.set_visibility(not remote_selected)
        local_processing_card.set_visibility(not remote_selected)
        local_transport_card.set_visibility(not remote_selected)

    async def _start_remote_import() -> None:
        runtime = remote_runtime

        if runtime is None:
            persistent_notify(
                "Remote HF Import is not configured. Set "
                "remote.hf_repo_id in config.yaml and "
                "L2SHOCK__REMOTE__HF_TOKEN in .env, then restart.",
                title="Remote HF Import",
                notification_type="negative",
            )
            return

        if state.shutdown_started:
            persistent_notify(
                "Application shutdown has started; new operations are blocked.",
                title="Remote HF Import",
                notification_type="negative",
            )
            return

        selected_bases = tuple(
            base
            for base, selected in (
                ("BTC", bool(remote_btc.value)),
                ("ETH", bool(remote_eth.value)),
            )
            if selected
        )

        if not selected_bases:
            persistent_notify(
                "Select at least one base: BTC or ETH.",
                title="Remote HF Import",
                notification_type="negative",
            )
            return

        try:
            requested_start_utc = _parse_local_datetime(
                remote_start_date.value,
                remote_start_time.value,
                field_name="Remote import start",
            )
            requested_end_utc = _parse_local_datetime(
                remote_end_date.value,
                remote_end_time.value,
                field_name="Remote import end",
            )
            lower_fraction, upper_fraction = _parse_depth_band(
                remote_depth_lower.value,
                remote_depth_upper.value,
            )

            runtime.start(
                requested_start_utc=requested_start_utc,
                requested_end_utc=requested_end_utc,
                bases=selected_bases,
                lower_depth_fraction=lower_fraction,
                upper_depth_fraction=upper_fraction,
            )

        except (
            RemoteImportRuntimeBusyError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            persistent_notify(
                str(exc),
                title="Remote HF Import",
                notification_type="negative",
            )

    def _stop_remote_import() -> None:
        runtime = remote_runtime

        if runtime is not None and runtime.request_stop():
            ui.notify(
                "Remote import stop requested. The current download or "
                "database transaction will finish before stopping.",
                type="warning",
                timeout=5000,
            )
        else:
            ui.notify(
                "No stoppable remote import is active.",
                type="info",
                timeout=3000,
            )

    async def _start_manual_fetch() -> None:
        if state.shutdown_started:
            persistent_notify(
                "Application shutdown has started; new operations are blocked.",
                title="Fetch not started",
                notification_type="negative",
            )
            return

        if (
            processing_runtime.is_running
            or state.active_operation_name == "manual_processing"
        ):
            persistent_notify(
                "Manual processing is active. Stop or finish processing before "
                "starting a fetch.",
                title="Fetch not started",
                notification_type="negative",
            )
            return

        try:
            requested_start_utc = _parse_local_datetime(
                start_date.value,
                start_time.value,
                field_name="Start",
            )
            requested_end_utc = _parse_local_datetime(
                end_date.value,
                end_time.value,
                field_name="End",
            )

            fetch_runtime.start(
                requested_start_utc=requested_start_utc,
                requested_end_utc=requested_end_utc,
            )

        except (ValueError, RuntimeError, FetchOperationBusyError) as exc:
            persistent_notify(
                str(exc),
                title="Manual Fetch",
                notification_type="negative",
            )

    def _stop_manual_fetch() -> None:
        if fetch_runtime.request_stop():
            ui.notify(
                "Stop requested. The current download will clean up safely.",
                type="warning",
                timeout=5000,
            )
        else:
            ui.notify(
                "No stoppable manual fetch is active.",
                type="info",
                timeout=3000,
            )

    def _calendar_base_by_name(
        snapshot: AvailabilityCalendarSnapshot,
        base: str,
    ) -> AvailabilityCalendarBaseSnapshot | None:
        return next(
            (item for item in snapshot.bases if item.base == base),
            None,
        )

    async def _handoff_calendar_window(
        base_snapshot: AvailabilityCalendarBaseSnapshot,
    ) -> None:
        handoff = base_snapshot.handoff

        if handoff is None:
            ui.notify(
                f"{base_snapshot.base} has no contiguous analyzable window.",
                type="warning",
                timeout=5000,
            )
            return

        if on_analysis_handoff is None:
            persistent_notify(
                "The Analysis page did not register a calendar handoff " "handler.",
                title="Analysis handoff",
                notification_type="negative",
            )
            return

        try:
            result = on_analysis_handoff(handoff)

            if inspect.isawaitable(result):
                result = await result

            if result is False:
                return

        except Exception as exc:
            log.exception("Availability-calendar Analysis handoff failed.")
            persistent_notify(
                f"Could not apply Analysis handoff: " f"{type(exc).__name__}",
                title="Analysis handoff",
                notification_type="negative",
            )

    def _show_hour_details(item) -> None:
        persistent_notify(
            availability_hour_tooltip(
                item,
                timezone_name=settings.app.timezone,
            ),
            title="Hourly availability",
            notification_type="info",
        )

    def _render_calendar(
        snapshot: AvailabilityCalendarSnapshot,
    ) -> None:
        calendar_grid.clear()

        with calendar_grid:
            for base in settings.analysis.supported_bases:
                base_snapshot = _calendar_base_by_name(
                    snapshot,
                    base,
                )

                if base_snapshot is None:
                    continue

                with ui.card().classes("w-full"):
                    with ui.row().classes("w-full items-center gap-3 flex-wrap"):
                        ui.label(base_snapshot.base).classes("text-lg font-bold")

                        if base_snapshot.preset_hash is None:
                            ui.badge(
                                "No enabled preset",
                                color="negative",
                            )
                        else:
                            ui.label(
                                "Preset " + base_snapshot.preset_hash[:12] + "..."
                            ).classes("text-xs font-mono text-gray-500")

                        ui.space()

                        handoff_button = ui.button(
                            "Use latest window in Analysis",
                            icon="insights",
                            on_click=(
                                lambda _event=None, selected=base_snapshot: create_tracked_task(
                                    _handoff_calendar_window(selected),
                                    name=(
                                        "l2shock-availability-handoff-"
                                        + selected.base.lower()
                                    ),
                                )
                            ),
                        ).props("outline color=primary")

                        if base_snapshot.handoff is None:
                            handoff_button.disable()

                    ui.label(
                        format_contiguous_window_local(
                            base_snapshot.contiguous_window,
                            timezone_name=settings.app.timezone,
                        )
                    ).classes("text-sm text-gray-600")

                    if base_snapshot.preset_hash is None:
                        ui.label(
                            "Create or enable a data preset before "
                            "analytical availability can be evaluated."
                        ).classes("text-sm text-orange-700")
                        continue

                    if not base_snapshot.hours:
                        ui.label(
                            "No exact UTC source hours start on this "
                            "local calendar date."
                        ).classes("text-sm text-gray-500")
                        continue

                    with ui.row().classes("w-full gap-1 flex-wrap mt-2"):
                        for item in base_snapshot.hours:
                            local_start = utc_to_local(
                                item.hour_utc,
                                settings.app.timezone,
                            )

                            button = (
                                ui.button(
                                    local_start.strftime("%H:%M"),
                                    on_click=(
                                        lambda _event=None, selected=item: _show_hour_details(
                                            selected
                                        )
                                    ),
                                )
                                .props(
                                    "dense unelevated "
                                    f"color={availability_state_color(item.state)}"
                                )
                                .classes("min-w-[4.5rem] font-mono")
                            )

                            button.tooltip(
                                availability_hour_tooltip(
                                    item,
                                    timezone_name=(settings.app.timezone),
                                )
                            )

    async def _refresh_availability_calendar() -> None:
        nonlocal calendar_refresh_running
        nonlocal calendar_refresh_pending
        nonlocal latest_calendar_snapshot

        if calendar_refresh_running:
            calendar_refresh_pending = True
            return

        calendar_refresh_running = True
        refresh_calendar_button.disable()
        calendar_status_label.text = "Loading hourly availability..."

        try:
            while True:
                calendar_refresh_pending = False
                selected_date = parse_local_calendar_date(calendar_date_input.value)

                snapshot = await asyncio.to_thread(
                    load_availability_calendar_snapshot,
                    selected_local_date=selected_date,
                    timezone_name=settings.app.timezone,
                    bases=tuple(settings.analysis.supported_bases),
                )

                current_date = parse_local_calendar_date(calendar_date_input.value)

                if current_date != selected_date:
                    # The user changed the date while the worker was loading.
                    # Never publish the obsolete snapshot.
                    calendar_refresh_pending = True
                    continue

                latest_calendar_snapshot = snapshot
                _render_calendar(snapshot)

                calendar_status_label.text = (
                    f"Showing exact source hours whose local start is on "
                    f"{selected_date.isoformat()} "
                    f"({settings.app.timezone})."
                )

                if not calendar_refresh_pending:
                    break

        except Exception as exc:
            log.exception("Could not refresh hourly availability calendar.")
            calendar_status_label.text = "Availability refresh failed."
            persistent_notify(
                f"Could not refresh availability: {type(exc).__name__}",
                title="Availability calendar",
                notification_type="negative",
            )

        finally:
            calendar_refresh_running = False

            if not state.shutdown_started:
                refresh_calendar_button.enable()
            else:
                refresh_calendar_button.disable()

    def _move_calendar_date(days: int) -> None:
        try:
            selected = parse_local_calendar_date(calendar_date_input.value)
        except ValueError as exc:
            persistent_notify(
                str(exc),
                title="Availability calendar",
                notification_type="negative",
            )
            return

        calendar_date_input.value = (selected + timedelta(days=days)).isoformat()
        calendar_date_input.update()

        create_tracked_task(
            _refresh_availability_calendar(),
            name="l2shock-availability-calendar-navigation",
        )

    def _start_automatic_fetch() -> None:
        try:
            automatic_runtime.start()
        except Exception as exc:
            persistent_notify(
                str(exc),
                title="Automatic Fetch",
                notification_type="negative",
            )
            return

        ui.notify(
            "Automatic Fetch enabled. The newest release-eligible UTC hour "
            "will be checked immediately.",
            type="positive",
        )

    async def _stop_automatic_fetch() -> None:
        stopped = await automatic_runtime.stop_and_wait(
            grace_seconds=10.0,
            persist=True,
        )

        if not stopped:
            persistent_notify(
                "Automatic Fetch did not stop within its cooperative grace "
                "period. Application Shutdown can force cancellation.",
                title="Automatic Fetch",
                notification_type="warning",
            )
            return

        ui.notify(
            "Automatic Fetch disabled.",
            type="positive",
        )

    automatic_start_button.on(
        "click",
        _start_automatic_fetch,
    )
    automatic_stop_button.on(
        "click",
        _stop_automatic_fetch,
    )
    previous_calendar_date_button.on_click(lambda: _move_calendar_date(-1))
    next_calendar_date_button.on_click(lambda: _move_calendar_date(1))
    refresh_calendar_button.on_click(_refresh_availability_calendar)
    calendar_date_input.on_value_change(_refresh_availability_calendar)

    async def _start_manual_processing() -> None:
        if state.shutdown_started:
            persistent_notify(
                "Application shutdown has started; new operations are blocked.",
                title="Processing not started",
                notification_type="negative",
            )
            return

        if fetch_runtime.is_running or state.active_operation_name == "manual_fetch":
            persistent_notify(
                "Manual Fetch is active. Stop or finish the fetch before "
                "starting processing.",
                title="Processing not started",
                notification_type="negative",
            )
            return

        try:
            requested_start_utc = _parse_local_datetime(
                processing_start_date.value,
                processing_start_time.value,
                field_name="Processing start",
            )
            requested_end_utc = _parse_local_datetime(
                processing_end_date.value,
                processing_end_time.value,
                field_name="Processing end",
            )
            lower_fraction, upper_fraction = _parse_depth_band(
                depth_lower_fraction.value,
                depth_upper_fraction.value,
            )

            processing_runtime.start(
                requested_start_utc=requested_start_utc,
                requested_end_utc=requested_end_utc,
                lower_depth_fraction=lower_fraction,
                upper_depth_fraction=upper_fraction,
            )

        except (
            InvalidOperation,
            ProcessingRuntimeBusyError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            persistent_notify(
                str(exc),
                title="Manual Processing",
                notification_type="negative",
            )

    def _stop_manual_processing() -> None:
        if processing_runtime.request_stop():
            ui.notify(
                "Stop requested. The current processing coordinator will "
                "finish or roll back safely.",
                type="warning",
                timeout=5000,
            )
        else:
            ui.notify(
                "No stoppable manual processing operation is active.",
                type="info",
                timeout=3000,
            )

    workflow_profile.on_value_change(lambda _event: _sync_workflow_profile())
    start_remote_import_button.on_click(_start_remote_import)
    stop_remote_import_button.on_click(_stop_remote_import)

    manual_button.on_click(_start_manual_fetch)
    stop_button.on_click(_stop_manual_fetch)
    process_button.on_click(_start_manual_processing)
    stop_processing_button.on_click(_stop_manual_processing)

    _sync_workflow_profile()

    def _refresh_runtime_view() -> None:
        nonlocal observed_fetch_completion_sequence
        nonlocal observed_processing_completion_sequence
        nonlocal observed_automatic_completion_sequence
        nonlocal observed_remote_completion_sequence

        fetch_snapshot = fetch_runtime.snapshot()
        processing_snapshot = processing_runtime.snapshot()
        automatic_snapshot = automatic_runtime.snapshot()
        remote_snapshot = (
            remote_runtime.snapshot() if remote_runtime is not None else None
        )

        profile_is_remote = (
            str(workflow_profile.value or "").strip() == "remote_hf_import"
        )

        if not state.shutdown_started and not state.active_operation_name:
            workflow_profile.enable()
        else:
            workflow_profile.disable()

        remote_inputs = (
            remote_start_date,
            remote_start_time,
            remote_end_date,
            remote_end_time,
            remote_btc,
            remote_eth,
            remote_depth_lower,
            remote_depth_upper,
        )

        remote_start_allowed = bool(
            profile_is_remote
            and remote_runtime is not None
            and remote_snapshot is not None
            and not remote_snapshot.is_running
            and not state.active_operation_name
            and not state.shutdown_started
        )

        for element in remote_inputs:
            if remote_start_allowed:
                element.enable()
            else:
                element.disable()

        if remote_start_allowed:
            start_remote_import_button.enable()
        else:
            start_remote_import_button.disable()

        if (
            remote_snapshot is not None
            and remote_snapshot.is_running
            and not remote_snapshot.stop_requested
        ):
            stop_remote_import_button.enable()
        else:
            stop_remote_import_button.disable()

        if remote_snapshot is not None:
            remote_progress = remote_snapshot.latest_progress

            if remote_progress is not None:
                remote_progress_bar.value = remote_progress.fraction_complete
                remote_status_label.text = remote_progress.message
                remote_revision_label.text = (
                    "Pinned revision: " + remote_progress.pinned_revision
                )
                remote_counter_label.text = (
                    f"Completed {remote_progress.artifacts_completed} / "
                    f"{remote_progress.artifacts_selected} | "
                    f"imported={remote_progress.imported_count} | "
                    f"reused={remote_progress.reused_count} | "
                    f"missing={remote_progress.missing_count} | "
                    f"failed={remote_progress.failed_count}"
                )
                remote_current_artifact_label.text = "Current artifact: " + (
                    remote_progress.current_key.relative_path
                    if remote_progress.current_key is not None
                    else "none"
                )
            elif remote_snapshot.is_running:
                remote_progress_bar.value = 0.0
                remote_status_label.text = (
                    "Resolving one immutable Hugging Face revision..."
                )
                remote_current_artifact_label.text = "Current artifact: preparing"
            else:
                remote_status_label.text = "Idle."
                remote_current_artifact_label.text = "Current artifact: none"

            if (
                remote_snapshot.completion_sequence
                != observed_remote_completion_sequence
            ):
                observed_remote_completion_sequence = (
                    remote_snapshot.completion_sequence
                )

                create_tracked_task(
                    _refresh_availability_calendar(),
                    name=("l2shock-availability-refresh-after-remote-import"),
                )

                if remote_snapshot.last_result is not None:
                    result = remote_snapshot.last_result

                    remote_progress_bar.value = (
                        1.0
                        if result.artifacts_selected == 0
                        else len(result.items) / result.artifacts_selected
                    )
                    remote_revision_label.text = (
                        "Pinned revision: " + result.pinned_revision
                    )
                    remote_result_label.text = (
                        f"Last result: {result.status}; "
                        f"selected={result.artifacts_selected}; "
                        f"imported={result.imported_count}; "
                        f"reused={result.reused_count}; "
                        f"missing={result.missing_count}; "
                        f"failed={result.failed_count}; "
                        f"stopped={result.stopped}."
                    )

                    notification_type = (
                        "positive"
                        if result.status == "ok"
                        else (
                            "warning"
                            if result.status in {"partial_ok", "stopped", "no_work"}
                            else "negative"
                        )
                    )

                    persistent_notify(
                        remote_result_label.text,
                        title="Remote HF Import completed",
                        notification_type=notification_type,
                    )

                elif remote_snapshot.last_error:
                    remote_result_label.text = (
                        "Last remote import error: " + remote_snapshot.last_error
                    )
                    persistent_notify(
                        remote_snapshot.last_error,
                        title="Remote HF Import failed",
                        notification_type="negative",
                    )

        if automatic_snapshot.is_running:
            automatic_start_button.disable()
            automatic_stop_button.enable()

            target = automatic_snapshot.current_target_hour_utc

            if target is None:
                automatic_status_label.text = (
                    "Automatic Fetch is active; the configured catch-up "
                    "window is complete."
                )
            else:
                local_target = utc_to_local(
                    target,
                    settings.app.timezone,
                )
                automatic_status_label.text = (
                    "Automatic Fetch is active; next incomplete hour: "
                    f"{local_target.isoformat()} "
                    f"({settings.app.timezone}; "
                    f"source UTC {target.isoformat()})."
                )

            automatic_status_label.classes(replace="text-xs text-green-700")
        else:
            automatic_start_allowed = bool(
                not state.shutdown_started and not state.active_operation_name
            )

            if automatic_start_allowed:
                automatic_start_button.enable()
            else:
                automatic_start_button.disable()

            automatic_stop_button.disable()
            automatic_status_label.text = "Automatic Fetch is inactive."
            automatic_status_label.classes(replace="text-xs text-gray-500")

        if (
            automatic_snapshot.completion_sequence
            != observed_automatic_completion_sequence
        ):
            observed_automatic_completion_sequence = (
                automatic_snapshot.completion_sequence
            )
            create_tracked_task(
                _refresh_availability_calendar(),
                name=("l2shock-availability-refresh-after-" "automatic-fetch"),
            )

        operation_idle = not bool(state.active_operation_name)
        new_work_allowed = operation_idle and not state.shutdown_started

        fetch_inputs_enabled = (
            new_work_allowed
            and not fetch_snapshot.is_running
            and not processing_snapshot.is_running
        )

        if fetch_inputs_enabled:
            start_date.enable()
            start_time.enable()
            end_date.enable()
            end_time.enable()
            manual_button.enable()
        else:
            start_date.disable()
            start_time.disable()
            end_date.disable()
            end_time.disable()
            manual_button.disable()

        processing_inputs_enabled = (
            new_work_allowed
            and not fetch_snapshot.is_running
            and not processing_snapshot.is_running
        )

        processing_inputs = (
            processing_start_date,
            processing_start_time,
            processing_end_date,
            processing_end_time,
            depth_lower_fraction,
            depth_upper_fraction,
        )

        if processing_inputs_enabled:
            for element in processing_inputs:
                element.enable()
            process_button.enable()
        else:
            for element in processing_inputs:
                element.disable()
            process_button.disable()

        if fetch_snapshot.is_running and not fetch_snapshot.stop_requested:
            stop_button.enable()
        else:
            stop_button.disable()

        if processing_snapshot.is_running and not processing_snapshot.stop_requested:
            stop_processing_button.enable()
        else:
            stop_processing_button.disable()

        fetch_progress = fetch_snapshot.latest_progress

        if fetch_progress is not None:
            progress_bar.value = fetch_progress.fraction_complete
            status_label.text = fetch_progress.message
            counter_label.text = (
                f"Completed {fetch_progress.files_completed} / "
                f"{fetch_progress.files_requested} | "
                f"downloaded={fetch_progress.files_downloaded} | "
                f"reused={fetch_progress.files_reused} | "
                f"missing={fetch_progress.files_missing} | "
                f"failed={fetch_progress.files_failed}"
            )
            current_file_label.text = "Current file: " + (
                fetch_progress.current_spec.remote_path
                if fetch_progress.current_spec is not None
                else "none"
            )
        elif fetch_snapshot.is_running:
            progress_bar.value = 0.0
            status_label.text = "Starting manual fetch..."
            current_file_label.text = "Current file: preparing"
        else:
            status_label.text = "Idle."
            current_file_label.text = "Current file: none"

        if fetch_snapshot.completion_sequence != observed_fetch_completion_sequence:
            observed_fetch_completion_sequence = fetch_snapshot.completion_sequence
            create_tracked_task(
                _refresh_availability_calendar(),
                name=("l2shock-availability-refresh-after-" "manual-fetch"),
            )
            if fetch_snapshot.last_result is not None:
                result = fetch_snapshot.last_result
                result_label.text = (
                    f"Last result: {result.status}; "
                    f"available={result.files_available}; "
                    f"missing={result.files_missing}; "
                    f"failed={result.files_failed}; "
                    f"completed={result.files_completed}/"
                    f"{result.files_requested}."
                )

                notification_type = (
                    "positive"
                    if result.status == "ok"
                    else (
                        "warning"
                        if result.status in {"partial_ok", "stopped"}
                        else "negative"
                    )
                )

                persistent_notify(
                    result_label.text,
                    title="Manual Fetch completed",
                    notification_type=notification_type,
                )

            elif fetch_snapshot.last_error:
                result_label.text = (
                    f"Last manual fetch error: " f"{fetch_snapshot.last_error}"
                )
                persistent_notify(
                    fetch_snapshot.last_error,
                    title="Manual Fetch failed",
                    notification_type="negative",
                )

        processing_progress = processing_snapshot.latest_progress

        if processing_progress is not None:
            processing_progress_bar.value = processing_progress.fraction_complete

            phase_suffix = (
                f" [{processing_progress.coordinator_phase}]"
                if processing_progress.coordinator_phase
                else ""
            )
            processing_status_label.text = (
                f"{processing_progress.message}{phase_suffix}"
            )
            processing_counter_label.text = (
                f"Completed "
                f"{processing_progress.targets_completed} / "
                f"{processing_progress.targets_selected} | "
                f"succeeded={processing_progress.completed_count} | "
                f"failed={processing_progress.failed_count}"
            )
            processing_target_label.text = "Current source target: " + (
                processing_progress.current_target.remote_path
                if processing_progress.current_target is not None
                else "none"
            )
        elif processing_snapshot.is_running:
            processing_progress_bar.value = 0.0
            processing_status_label.text = "Starting manual processing..."
            processing_target_label.text = "Current source target: planning"
        else:
            processing_status_label.text = "Idle."
            processing_target_label.text = "Current source target: none"

        if (
            processing_snapshot.completion_sequence
            != observed_processing_completion_sequence
        ):
            observed_processing_completion_sequence = (
                processing_snapshot.completion_sequence
            )
            create_tracked_task(
                _refresh_availability_calendar(),
                name=("l2shock-availability-refresh-after-" "processing"),
            )
            if processing_snapshot.last_result is not None:
                result = processing_snapshot.last_result

                processing_progress_bar.value = (
                    1.0
                    if result.targets_selected == 0
                    else len(result.items) / result.targets_selected
                )
                processing_counter_label.text = (
                    f"Completed {len(result.items)} / "
                    f"{result.targets_selected} | "
                    f"succeeded={result.completed_count} | "
                    f"failed={result.failed_count}"
                )
                processing_result_label.text = (
                    f"Last result: {result.status}; "
                    f"selected={result.targets_selected}; "
                    f"succeeded={result.completed_count}; "
                    f"failed={result.failed_count}; "
                    f"stopped={result.stopped}."
                )

                notification_type = (
                    "positive"
                    if result.status == "ok"
                    else (
                        "warning"
                        if result.status in {"partial_ok", "stopped", "no_work"}
                        else "negative"
                    )
                )

                persistent_notify(
                    processing_result_label.text,
                    title="Manual Processing completed",
                    notification_type=notification_type,
                )

            elif processing_snapshot.last_error:
                processing_result_label.text = (
                    "Last manual processing error: " f"{processing_snapshot.last_error}"
                )
                persistent_notify(
                    processing_snapshot.last_error,
                    title="Manual Processing failed",
                    notification_type="negative",
                )

    ui.timer(
        interval=0.1,
        callback=_refresh_availability_calendar,
        once=True,
    )
    ui.timer(
        interval=0.25,
        callback=_refresh_runtime_view,
        active=True,
    )


__all__ = [
    "_parse_depth_band",
    "_parse_depth_fraction",
    "_parse_local_datetime",
    "build_fetch_tab",
]

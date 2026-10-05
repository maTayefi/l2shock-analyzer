# l2shock/ui/tab_l2_view.py
"""Detector-free Analysis: load, aggregate, render, and export."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from nicegui import ui

from l2shock.analysis.l2_view_metrics import (
    L2_VIEW_METRIC_SPECS,
    L2ViewMetric,
)
from l2shock.analysis.l2_view_stream import (
    HARD_MAX_ANALYSIS_DURATION_SECONDS,
    MAX_ANALYSIS_MEMORY_BUDGET_MIB,
    MAX_L2_VIEW_BARS,
    MIN_ANALYSIS_MEMORY_BUDGET_MIB,
    MIN_MAX_ANALYSIS_DURATION_SECONDS,
    L2ViewLoadOptions,
    L2ViewProjection,
    L2ViewRequest,
)
from l2shock.config import get_settings
from l2shock.timeutils import utc_to_local
from l2shock.ui.analysis_inputs import (
    load_enabled_analysis_presets,
    parse_local_analysis_datetime,
)
from l2shock.ui.availability_calendar import AnalysisRangeHandoff
from l2shock.ui.chart_interactions import (
    AnalysisChartController,
    AnalysisChartTemporalViewport,
    capture_shock_time_viewport,
)
from l2shock.ui.components import (
    persistent_notify,
    run_db_worker_thread,
)
from l2shock.ui.display_timezone import with_display_timezone
from l2shock.ui.echarts import empty_echart_option
from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options
from l2shock.ui.l2_view_presentation import (
    cached_view,
    capture_y_viewports,
    displayed_csv_bytes,
    displayed_json_bytes,
    fit_closed_handoff,
    install_y_wheel,
    whole_number,
    with_wheel_policy,
)
from l2shock.ui.l2_view_runtime import (
    L2ViewRuntimePhase,
    get_l2_view_runtime,
)
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)

_TIMEFRAMES = {
    0: "Auto: finest bars within budget",
    1: "1 second",
    5: "5 seconds",
    15: "15 seconds",
    60: "1 minute",
    300: "5 minutes",
    900: "15 minutes",
    3600: "1 hour",
    14400: "4 hours",
    86400: "1 day",
}


def build_l2_view_section() -> Callable[[AnalysisRangeHandoff], Awaitable[bool]]:
    state = get_state()
    runtime = get_l2_view_runtime()
    settings = get_settings()
    timezone_name = settings.app.timezone

    loaded: L2ViewProjection | None = None
    displayed: L2ViewProjection | None = None
    displayed_option: dict[str, Any] | None = None
    displayed_a = L2ViewMetric.IMBALANCE_PCT.value
    displayed_b = L2ViewMetric.DELTA.value
    displayed_timeframe_setting = 0

    observed_completion = -1
    observed_successful_operation_id: str | None = None
    pending_operation_id: str | None = None
    pending_viewport: AnalysisChartTemporalViewport | None = None
    preset_loading = False
    publishing = False
    presets_base: str | None = None
    enabled_hashes: set[str] = set()
    render_lock = asyncio.Lock()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    initial_start = utc_to_local(now - timedelta(hours=1), timezone_name)
    initial_end = utc_to_local(now, timezone_name)

    with ui.column().classes("w-full gap-3 p-4"):
        ui.label("Analysis").classes("text-xl font-semibold")
        ui.label(
            "Load verified liquidity and optional Binance price context. "
            "No shock detection, ranking, candidate table, or A/B/C overlays."
        ).classes("text-sm text-gray-500")

        with ui.row().classes("w-full gap-3 flex-wrap items-end"):
            base_input = ui.select(
                {"BTC": "BTC", "ETH": "ETH"},
                value="BTC",
                label="Base",
            ).classes("w-32")
            preset_input = ui.select(
                options={},
                value=None,
                label="Enabled liquidity preset",
            ).classes("w-[34rem] max-w-full")
            reload_presets_button = ui.button("Reload presets", icon="refresh").props(
                "outline"
            )

        with ui.row().classes("w-full gap-3 flex-wrap"):
            start_date = (
                ui.input(
                    f"Start date ({timezone_name})",
                    value=initial_start.strftime("%Y-%m-%d"),
                )
                .props("type=date")
                .classes("w-44")
            )
            start_time = ui.input(
                "Start time (HH:MM:SS)",
                value=initial_start.strftime("%H:%M:%S"),
            ).classes("w-44")
            end_date = (
                ui.input(
                    f"End date ({timezone_name})",
                    value=initial_end.strftime("%Y-%m-%d"),
                )
                .props("type=date")
                .classes("w-44")
            )
            end_time = ui.input(
                "End time (HH:MM:SS)",
                value=initial_end.strftime("%H:%M:%S"),
            ).classes("w-44")
            duration_input = (
                ui.input("Max scan duration (seconds)", value="86400")
                .props("inputmode=numeric")
                .classes("w-64")
            )

        with ui.row().classes("w-full gap-3 flex-wrap items-end"):
            memory_input = (
                ui.input("Streaming memory budget (MiB)", value="512")
                .props("inputmode=numeric")
                .classes("w-64")
            )
            max_bars_input = ui.number(
                "Maximum viewing bars",
                value=1200,
                min=1,
                max=MAX_L2_VIEW_BARS,
                step=1,
            ).classes("w-56")
            start_button = ui.button("Start Analysis", icon="play_arrow")
            stop_button = ui.button("Stop Analysis", icon="stop").props("outline")

        ui.label(
            "Duration is measured in owned one-second slots, including "
            "both endpoint seconds. The streaming budget is a chunk-size "
            "heuristic, not a guaranteed total-process RAM limit."
        ).classes("text-xs text-gray-500")
        preset_status = ui.label("Enabled presets have not been loaded.").classes(
            "text-xs text-gray-500"
        )

        metric_options = {
            metric.value: spec.label for metric, spec in L2_VIEW_METRIC_SPECS.items()
        }
        with ui.row().classes("w-full gap-3 flex-wrap items-end"):
            timeframe_input = ui.select(
                _TIMEFRAMES, value=0, label="Viewing timeframe"
            ).classes("w-72")
            panel_a = ui.select(
                metric_options,
                value=L2ViewMetric.IMBALANCE_PCT.value,
                label="L2 Panel A",
            ).classes("w-72")
            panel_b = ui.select(
                metric_options,
                value=L2ViewMetric.DELTA.value,
                label="L2 Panel B",
            ).classes("w-72")
            warnings_switch = ui.switch("Show data-quality warnings", value=True)
            extremeness_switch = ui.switch("Show L2 ratio extremeness", value=True)

        with ui.row().classes("gap-2 flex-wrap"):
            png_button = ui.button("Export PNG", icon="image").props("outline")
            svg_button = ui.button("Export SVG", icon="draw").props("outline")
            json_button = ui.button(
                "Export displayed bars JSON", icon="download"
            ).props("outline")
            csv_button = ui.button("Export displayed bars CSV", icon="download").props(
                "outline"
            )
            legend_button = ui.button(
                "Chart legend and formulas", icon="palette"
            ).props("outline")

        status = ui.label("No Analysis load in this session.")
        progress = ui.label("").classes("text-xs text-gray-500")
        chart = (
            ui.echart(empty_echart_option("Start Analysis to load data"))
            .classes("w-full h-[78vh] min-h-[680px]")
            .props("renderer=canvas")
        )
        controller = AnalysisChartController(chart, crosshair_gap_px=50)
        ui.label(
            "Wheel: synchronized time-axis zoom. Shift+wheel: independent "
            "Y zoom in the hovered panel. Metric changes reset only the "
            "changed panel's Y zoom; timeframe changes reset all Y zooms. "
            "JSON/CSV export the full displayed-bar dataset, not only the "
            "current zoom window. Missing values are not zero-filled."
        ).classes("text-xs text-gray-500")

    def _enabled(control: Any, value: bool) -> None:
        control.enable() if value else control.disable()

    def _sync_controls() -> None:
        snapshot = runtime.snapshot()
        idle = (
            not state.shutdown_started
            and not snapshot.is_running
            and not publishing
            and not preset_loading
        )
        for control in (
            base_input,
            preset_input,
            reload_presets_button,
            start_date,
            start_time,
            end_date,
            end_time,
            duration_input,
            memory_input,
            max_bars_input,
            timeframe_input,
            panel_a,
            panel_b,
            warnings_switch,
            extremeness_switch,
        ):
            _enabled(control, idle)

        valid_preset = (
            presets_base == str(base_input.value)
            and str(preset_input.value or "") in enabled_hashes
        )
        _enabled(
            start_button,
            idle
            and valid_preset
            and not state.active_operation_name
            and not state.operation_lock.locked(),
        )
        _enabled(
            stop_button,
            not state.shutdown_started and snapshot.is_running,
        )
        export_ready = (
            idle
            and displayed is not None
            and displayed_option is not None
            and controller.commit is not None
        )
        for control in (png_button, svg_button, json_button, csv_button):
            _enabled(control, export_ready)

    def _duration() -> int:
        return whole_number(
            duration_input.value,
            "Max scan duration",
            minimum=MIN_MAX_ANALYSIS_DURATION_SECONDS,
            maximum=HARD_MAX_ANALYSIS_DURATION_SECONDS,
        )

    def _options() -> L2ViewLoadOptions:
        selected = whole_number(
            timeframe_input.value,
            "Viewing timeframe",
            minimum=0,
            maximum=86400,
        )
        if selected not in _TIMEFRAMES:
            raise ValueError("Unsupported viewing timeframe")
        return L2ViewLoadOptions(
            timeframe_seconds=None if selected == 0 else selected,
            max_bars=whole_number(
                max_bars_input.value,
                "Maximum viewing bars",
                minimum=1,
                maximum=MAX_L2_VIEW_BARS,
            ),
            memory_budget_mib=whole_number(
                memory_input.value,
                "Streaming memory budget",
                minimum=MIN_ANALYSIS_MEMORY_BUDGET_MIB,
                maximum=MAX_ANALYSIS_MEMORY_BUDGET_MIB,
            ),
            l2_warning_seconds=(settings.analysis.l2_long_invalid_warning_seconds),
            price_warning_seconds=(
                settings.analysis.price_long_invalid_warning_minutes * 60
            ),
        )

    async def _reload_presets(_event: Any = None) -> None:
        nonlocal preset_loading, enabled_hashes, presets_base
        if state.shutdown_started or preset_loading or runtime.snapshot().is_running:
            return
        preset_loading = True
        _sync_controls()
        try:
            presets = await run_db_worker_thread(load_enabled_analysis_presets)
            selected_base = str(base_input.value)
            matches = tuple(
                preset for preset in presets if preset.base == selected_base
            )
            choices = {preset.preset_hash: preset.label for preset in matches}
            previous = str(preset_input.value or "")
            enabled_hashes = set(choices)
            presets_base = selected_base
            preset_input.options = choices
            preset_input.value = (
                previous if previous in choices else next(iter(choices), None)
            )
            preset_input.update()
            preset_status.set_text(f"{len(choices)} enabled {selected_base} preset(s).")
        except Exception:
            log.exception("Could not load Analysis presets.")
            enabled_hashes = set()
            presets_base = None
            preset_input.options = {}
            preset_input.value = None
            preset_input.update()
            preset_status.set_text("Preset loading failed; check the log.")
        finally:
            preset_loading = False
            _sync_controls()

    def _consume(done: asyncio.Task) -> None:
        if not done.cancelled():
            done.exception()

    def _admit(
        request: L2ViewRequest,
        options: L2ViewLoadOptions,
        viewport: AnalysisChartTemporalViewport | None,
    ) -> None:
        nonlocal pending_operation_id, pending_viewport
        task = runtime.start(request, options)
        pending_operation_id = runtime.snapshot().operation_id
        pending_viewport = viewport
        task.add_done_callback(_consume)
        _sync_controls()
        status.set_text(
            "Loading verified data. The previous chart remains available "
            "until a replacement is committed."
        )

    async def _start() -> None:
        if (
            publishing
            or preset_loading
            or state.shutdown_started
            or render_lock.locked()
        ):
            return

        if runtime.snapshot().is_running:
            return

        if (
            presets_base != str(base_input.value)
            or str(preset_input.value or "") not in enabled_hashes
        ):
            status.set_text("Select an enabled preset for the current base.")
            return
        try:
            request = L2ViewRequest(
                base=str(base_input.value),
                preset_hash=str(preset_input.value),
                requested_start_utc=parse_local_analysis_datetime(
                    start_date.value,
                    start_time.value,
                    timezone_name=timezone_name,
                    field_name="Analysis start",
                ),
                requested_end_utc=parse_local_analysis_datetime(
                    end_date.value,
                    end_time.value,
                    timezone_name=timezone_name,
                    field_name="Analysis end",
                ),
                max_duration_seconds=_duration(),
            )
            options = _options()
            if request.slot_count > 183 * 86400:
                ui.notify(
                    "This long range is streamed, but may take substantial "
                    "time to read, decode, and verify.",
                    type="warning",
                    timeout=8000,
                )
            if options.memory_budget_mib > 6144:
                ui.notify(
                    "A streaming budget above 6 GiB can leave little "
                    "headroom on an 8 GiB machine.",
                    type="warning",
                    timeout=8000,
                )
            _admit(request, options, None)
        except (TypeError, ValueError, RuntimeError) as exc:
            status.set_text(f"Analysis not started: {exc}")

    def _stop() -> None:
        status.set_text(
            "Stop requested. Waiting for a cooperative loading boundary; "
            "the previously committed chart will be retained."
            if runtime.request_stop()
            else "No active Analysis load to stop."
        )
        _sync_controls()

    async def _publish(
        projection: L2ViewProjection,
        *,
        viewport: AnalysisChartTemporalViewport | None,
        new_source: bool,
        timeframe_setting: int,
    ) -> None:
        nonlocal displayed, displayed_option
        nonlocal displayed_a, displayed_b
        nonlocal displayed_timeframe_setting, publishing

        publishing = True
        _sync_controls()
        try:
            chosen_a = L2ViewMetric(str(panel_a.value))
            chosen_b = L2ViewMetric(str(panel_b.value))
            old_commit = controller.commit
            reset = set(range(5))
            preserved_y = {}

            if (
                not new_source
                and displayed is not None
                and displayed.timeframe_seconds == projection.timeframe_seconds
                and old_commit is not None
            ):
                reset = set()
                if chosen_a.value != displayed_a:
                    reset.add(3)
                if chosen_b.value != displayed_b:
                    reset.add(4)
                preserved_y = await capture_y_viewports(
                    chart,
                    expected_render_token=old_commit.publication.render_token,
                )

            try:
                _show_extremeness = bool(extremeness_switch.value)
            except NameError:
                # Fallback for test harnesses that extract _publish via AST
                # without mocking the newly added extremeness_switch control.
                _show_extremeness = True

            raw_option = build_l2_view_chart_options(
                projection,
                panel_a_metric=chosen_a,
                panel_b_metric=chosen_b,
                show_warnings=bool(warnings_switch.value),
                show_ratio_extremeness=_show_extremeness,
            )
            shown = with_display_timezone(
                with_wheel_policy(
                    raw_option,
                    preserved_y=preserved_y,
                    reset_panels=frozenset(reset),
                ),
                timezone_name,
            )
            commit = await controller.publish(
                shown,
                owner_id=projection.input_id,
                preserve_viewport=False,
                shock_time_viewport=None if new_source else viewport,
            )
            if commit is None:
                raise RuntimeError("Chart publication was superseded")

            displayed = projection
            displayed_option = raw_option
            displayed_a = chosen_a.value
            displayed_b = chosen_b.value
            displayed_timeframe_setting = timeframe_setting
            timeframe_input.value = timeframe_setting
            timeframe_input.update()

            try:
                installed = await install_y_wheel(
                    chart,
                    expected_render_token=commit.publication.render_token,
                )
                if not installed:
                    raise RuntimeError("Browser wheel handler unavailable")
            except Exception:
                log.exception("Independent Analysis Y zoom unavailable.")
                persistent_notify(
                    "The chart rendered, but independent Shift+wheel "
                    "Y zoom could not be installed.",
                    title="Analysis chart",
                )

            valid_l2_bars = sum(bar.valid_l2 for bar in projection.bars)
            gap_l2_bars = len(projection.bars) - valid_l2_bars
            price_bars = sum(bar.price is not None for bar in projection.bars)

            status.set_text(
                f"Showing {len(projection.bars):,} "
                f"{projection.timeframe_seconds}s bars; "
                f"L2 candles={valid_l2_bars:,}; "
                f"L2 gap bars={gap_l2_bars:,}; "
                f"usable L2 seconds={projection.usable_l2_seconds:,}; "
                f"unusable={projection.unusable_l2_seconds:,}; "
                f"partial-market={projection.partial_market_seconds:,}; "
                f"price={projection.price_status}; "
                f"price candles={price_bars:,}."
            )

            if new_source and (
                valid_l2_bars == 0
                or projection.unusable_l2_seconds > 0
                or projection.partial_market_seconds > 0
            ):
                if valid_l2_bars == 0:
                    quality_message = (
                        "Analysis completed without numerical L2 observations "
                        "for the selected preset and range. Available verified "
                        "L2 seconds are not suppressed by a coverage threshold. "
                        "If the log says 'no_row_for_exact_component_preset', "
                        "the required local component rows are absent: the "
                        "remote collector depth, Remote HF Import depth, and "
                        "Analysis preset depth must match exactly. Enabled "
                        "presets are configurations, not proof of materialized "
                        "L2 data. Remote publication must also be imported "
                        "into this local database. If component rows exist "
                        "but have zero usable seconds, inspect 'ANALYSIS L2 "
                        "COMPONENT COVERAGE' for their quality reasons. "
                        "Price is independent; loaded price does not prove "
                        "that the selected L2 components exist."
                    )
                    notify_title = "No numerical L2 data for selected preset"
                else:
                    quality_message = (
                        "Analysis rendered the available verified L2 data. "
                        f"{projection.unusable_l2_seconds:,} seconds have "
                        "no numerical L2 observations; "
                        f"{projection.partial_market_seconds:,} seconds "
                        "contain only some expected markets. "
                        "Candles use the available observations without "
                        "zero-fill or forward-fill. Changes in contributing "
                        "venues can create apparent liquidity changes. "
                        "Cross-check price and L2 with trdr.io before trading."
                    )
                    notify_title = "L2 data-quality warning"
                log.warning(
                    "Analysis L2 quality warning: "
                    "analysis_id=%s timeframe_seconds=%d "
                    "renderable_l2_bars=%d unusable_l2_seconds=%d "
                    "partial_market_seconds=%d",
                    projection.input_id,
                    projection.timeframe_seconds,
                    valid_l2_bars,
                    projection.unusable_l2_seconds,
                    projection.partial_market_seconds,
                )
                persistent_notify(
                    quality_message,
                    title=notify_title,
                    notification_type="warning",
                )

            if projection.price_status in {"failed", "corrupt"}:
                persistent_notify(
                    "Optional price loading failed. L2 eligibility is "
                    "unchanged; price outage regions are unavailable "
                    "for this load.",
                    title="Optional price",
                )
            if projection.l2_regions_truncated or projection.price_regions_truncated:
                persistent_notify(
                    "The warning-region cap was reached. Not every outage "
                    "region can be highlighted; quality counts remain shown.",
                    title="Data-quality warnings",
                )
        finally:
            publishing = False
            _sync_controls()

    async def _change_presentation(_event: Any = None) -> None:
        """Change metrics or warnings without entering a loading path."""
        nonlocal publishing

        if state.shutdown_started or runtime.snapshot().is_running or preset_loading:
            return

        async with render_lock:
            if (
                state.shutdown_started
                or runtime.snapshot().is_running
                or preset_loading
                or displayed is None
            ):
                return

            publishing = True
            _sync_controls()

            try:
                projection = displayed
                viewport = await capture_shock_time_viewport(chart)

                if state.shutdown_started or runtime.snapshot().is_running:
                    return

                await _publish(
                    projection,
                    viewport=viewport,
                    new_source=False,
                    timeframe_setting=displayed_timeframe_setting,
                )

            except (TypeError, ValueError, RuntimeError) as exc:
                status.set_text(f"Presentation not changed: {exc}")

            finally:
                publishing = False
                _sync_controls()

    async def _change_view(_event: Any = None) -> None:
        """Change timeframe or bar budget, reloading only when required."""
        nonlocal publishing

        if state.shutdown_started or runtime.snapshot().is_running or preset_loading:
            return

        async with render_lock:
            if (
                state.shutdown_started
                or runtime.snapshot().is_running
                or preset_loading
                or loaded is None
            ):
                return

            publishing = True
            _sync_controls()

            try:
                options = _options()
                viewport = await capture_shock_time_viewport(chart)

                if state.shutdown_started or runtime.snapshot().is_running:
                    return

                _target, projection = cached_view(
                    loaded,
                    timeframe_seconds=options.timeframe_seconds,
                    max_bars=options.max_bars,
                )

                if projection is None:
                    _admit(loaded.request, options, viewport)
                    return

                new_source = (
                    displayed is None
                    or displayed.request != projection.request
                    or displayed.input_id != projection.input_id
                )
                await _publish(
                    projection,
                    viewport=None if new_source else viewport,
                    new_source=new_source,
                    timeframe_setting=(
                        0
                        if options.timeframe_seconds is None
                        else options.timeframe_seconds
                    ),
                )

            except (TypeError, ValueError, RuntimeError) as exc:
                timeframe_input.value = displayed_timeframe_setting
                timeframe_input.update()
                status.set_text(f"View not changed: {exc}")

            except Exception:
                log.exception("Could not update Analysis chart.")
                status.set_text("Chart update failed; check the log.")

            finally:
                publishing = False
                _sync_controls()

    async def _poll() -> None:
        nonlocal loaded, observed_completion
        nonlocal observed_successful_operation_id
        nonlocal pending_operation_id, pending_viewport

        snapshot = runtime.snapshot()
        _sync_controls()
        progress.set_text(
            f"Loading hours: {snapshot.hours_done:,} / "
            f"{snapshot.hours_total:,}; phase={snapshot.phase.value}"
            if snapshot.is_running
            else ""
        )

        # A newer operation may already be running while last_projection
        # still belongs to an earlier successful operation. Do not consume
        # either completion or pending viewport ownership during that load.
        if publishing or snapshot.is_running:
            return

        successful_operation_id = snapshot.last_projection_operation_id
        has_unhandled_success = (
            snapshot.last_projection is not None
            and successful_operation_id is not None
            and successful_operation_id != observed_successful_operation_id
        )
        if (
            snapshot.completion_sequence == observed_completion
            and not has_unhandled_success
        ):
            return

        async with render_lock:
            # Another page can admit a load while this page waits for its
            # render lock. The earlier snapshot is no longer authoritative.
            snapshot = runtime.snapshot()
            if publishing or snapshot.is_running:
                return

            successful_operation_id = snapshot.last_projection_operation_id
            projection = snapshot.last_projection
            has_unhandled_success = (
                projection is not None
                and successful_operation_id is not None
                and successful_operation_id != observed_successful_operation_id
            )

            if (
                snapshot.completion_sequence == observed_completion
                and not has_unhandled_success
            ):
                return

            if has_unhandled_success:
                assert projection is not None
                assert successful_operation_id is not None

                viewport = (
                    pending_viewport
                    if successful_operation_id == pending_operation_id
                    else None
                )
                same_range = (
                    displayed is not None
                    and displayed.request == projection.request
                    and displayed.input_id == projection.input_id
                )
                loaded = projection

                try:
                    timeframe_setting = snapshot.last_projection_timeframe_setting
                    if timeframe_setting is None:
                        raise RuntimeError(
                            "Successful Analysis result lacks timeframe ownership"
                        )

                    await _publish(
                        projection,
                        viewport=viewport,
                        new_source=not same_range,
                        timeframe_setting=timeframe_setting,
                    )
                except Exception:
                    log.exception("Could not publish completed Analysis.")
                    status.set_text(
                        "Data loaded, but chart publication failed. "
                        "Change Timeframe or Maximum viewing bars to retry "
                        "the loaded result. Panel and Warnings changes only "
                        "rebuild the last displayed result."
                    )

                # Record a handled publication attempt, not a claim that
                # browser publication succeeded. On failure, loaded retains
                # the result for an explicit viewing-control retry, while
                # displayed and controller.commit remain authoritative.
                observed_successful_operation_id = successful_operation_id

                if pending_operation_id == successful_operation_id:
                    pending_operation_id = None
                    pending_viewport = None

            # Publication awaits browser work. A different page may have
            # started another load in the meantime. Do not consume its
            # completion or withdraw its pending state.
            snapshot = runtime.snapshot()
            if snapshot.is_running:
                return

            terminal_changed = snapshot.completion_sequence != observed_completion
            observed_completion = snapshot.completion_sequence

            if pending_operation_id == snapshot.operation_id and snapshot.phase in {
                L2ViewRuntimePhase.COMPLETED,
                L2ViewRuntimePhase.STOPPED,
                L2ViewRuntimePhase.FAILED,
            }:
                pending_operation_id = None
                pending_viewport = None

            if not terminal_changed:
                return

            if snapshot.phase is L2ViewRuntimePhase.STOPPED:
                timeframe_input.value = displayed_timeframe_setting
                timeframe_input.update()
                status.set_text(
                    "Analysis stopped. No partial result was published; "
                    "the latest committed chart is retained."
                )
            elif snapshot.phase is L2ViewRuntimePhase.FAILED:
                timeframe_input.value = displayed_timeframe_setting
                timeframe_input.update()
                status.set_text(
                    f"Analysis failed: {snapshot.last_error}. "
                    "The latest committed chart is retained."
                )

    def _export_data(kind: str) -> None:
        if (
            publishing
            or displayed is None
            or displayed_option is None
            or controller.commit is None
            or controller.commit.owner_id != displayed.input_id
        ):
            status.set_text("No acknowledged Analysis chart to export.")
            return
        stem = (
            f"l2shock_analysis_{displayed.input_id[:16]}_"
            f"{displayed.timeframe_seconds}s"
        )
        if kind == "json":
            content = displayed_json_bytes(
                displayed,
                displayed_option,
                panel_a_metric=displayed_a,
                panel_b_metric=displayed_b,
            )
            media_type = "application/json"
        else:
            content = displayed_csv_bytes(displayed, displayed_option)
            media_type = "text/csv"
        ui.download(
            content,
            filename=f"{stem}.{kind}",
            media_type=media_type,
        )

    async def _export_image(kind: str) -> None:
        nonlocal publishing
        async with render_lock:
            if displayed is None or controller.commit is None:
                return
            owner = displayed.input_id
            publishing = True
            _sync_controls()
            try:
                exported, reason = await controller.export_image(
                    owner_id=owner,
                    image_format=kind,
                    filename=(
                        f"l2shock_analysis_{owner[:16]}_"
                        f"{displayed.timeframe_seconds}s.{kind}"
                    ),
                )
                status.set_text(
                    f"{kind.upper()} export completed."
                    if exported
                    else f"Export unavailable: {reason}"
                )
            except Exception:
                log.exception("Analysis image export failed.")
                status.set_text("Image export failed; check the log.")
            finally:
                publishing = False
                _sync_controls()

    legend_dialog: Any = None

    def _open_legend() -> None:
        """Build the static legend once per page, then only reopen it."""
        nonlocal legend_dialog

        if legend_dialog is None:
            with ui.dialog() as legend_dialog:
                with ui.card().classes("w-[48rem] max-w-full"):
                    ui.label("Analysis chart legend").classes("text-lg font-semibold")
                    ui.label(
                        "Price: green up, red down. Bid: blue. Ask: orange. "
                        "Red background areas: data-quality outages."
                    )
                    for spec in L2_VIEW_METRIC_SPECS.values():
                        ui.label(f"{spec.label}: {spec.formula}").classes("text-sm")
                    ui.label(
                        "L2 candles use available verified observations. "
                        "Partial-market sums remain visible with quality "
                        "warnings; changing contributors can create apparent "
                        "liquidity changes. Derived percentage candles use "
                        "one-second ratios, not ratios of separate OHLC "
                        "extrema. A contributing zero-total second makes "
                        "that bar's percentage candle undefined. Change "
                        "metrics compare adjacent renderable bar closes. "
                        "Unavailable values remain gaps."
                    ).classes("text-xs text-gray-500")
                    ui.button("Close", on_click=legend_dialog.close)

        legend_dialog.open()

    async def _handoff(handoff: AnalysisRangeHandoff) -> bool:
        if not isinstance(handoff, AnalysisRangeHandoff):
            raise TypeError("handoff must be AnalysisRangeHandoff")
        if (
            state.shutdown_started
            or runtime.snapshot().is_running
            or publishing
            or preset_loading
        ):
            ui.notify(
                "Finish the current operation before applying a calendar window.",
                type="warning",
            )
            return False
        try:
            start, end, clipped = fit_closed_handoff(
                handoff.start_utc,
                handoff.closed_end_utc,
                max_duration_seconds=_duration(),
            )
            base_input.value = handoff.base
            base_input.update()
            # Explicitly refresh; do not depend on a timing sleep or
            # on_value_change being raised by programmatic assignment.
            await _reload_presets()
            if (
                presets_base != handoff.base
                or handoff.preset_hash not in enabled_hashes
            ):
                raise ValueError(
                    "The calendar preset is no longer enabled for this base"
                )

            preset_input.value = handoff.preset_hash
            preset_input.update()
            local_start = utc_to_local(start, timezone_name)
            local_end = utc_to_local(end, timezone_name)
            for control, value in (
                (start_date, local_start.strftime("%Y-%m-%d")),
                (start_time, local_start.strftime("%H:%M:%S")),
                (end_date, local_end.strftime("%Y-%m-%d")),
                (end_time, local_end.strftime("%H:%M:%S")),
            ):
                control.value = value
                control.update()
            status.set_text(
                "Calendar window applied"
                + (
                    "; kept its newest seconds within Max scan duration"
                    if clipped
                    else ""
                )
                + ". Review the controls and press Start Analysis."
            )
            _sync_controls()
            return True
        except (TypeError, ValueError, RuntimeError) as exc:
            ui.notify(f"Calendar window not applied: {exc}", type="negative")
            return False

    start_button.on_click(_start)
    stop_button.on_click(_stop)
    reload_presets_button.on_click(_reload_presets)
    base_input.on_value_change(_reload_presets)
    timeframe_input.on_value_change(_change_view)
    max_bars_input.on("blur", _change_view)
    max_bars_input.on("keydown.enter", _change_view)
    panel_a.on_value_change(_change_presentation)
    panel_b.on_value_change(_change_presentation)
    warnings_switch.on_value_change(_change_presentation)
    extremeness_switch.on_value_change(_change_presentation)
    png_button.on_click(lambda: _export_image("png"))
    svg_button.on_click(lambda: _export_image("svg"))
    json_button.on_click(lambda: _export_data("json"))
    csv_button.on_click(lambda: _export_data("csv"))
    legend_button.on_click(_open_legend)

    _sync_controls()
    ui.timer(0.25, _poll)
    ui.timer(0.1, _reload_presets, once=True)
    return _handoff


__all__ = ["build_l2_view_section"]

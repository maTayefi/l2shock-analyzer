# l2shock/ui/tab_analysis.py
"""Functional Analysis controls and sortable selected-LM table."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from nicegui import ui

from l2shock.config import get_settings
from l2shock.timeutils import (
    floor_to_hour,
    now_utc,
    utc_to_local,
)
from l2shock.analysis import TIMEFRAMES
from l2shock.analysis.timeframes import snap_closed_analysis_range
from l2shock.ui.analysis_chart import (
    AnalysisChartVisibility,
    build_analysis_chart_option,
    chart_navigation_window,
    empty_analysis_chart_option,
)
from l2shock.ui.chart_interactions import (
    AnalysisChartController,
    AnalysisChartInteractionError,
    AnalysisChartTemporalViewport,
    capture_analysis_chart_temporal_viewport,
)
from l2shock.ui.analysis_export import (
    analysis_export_filename,
    analysis_export_json_bytes,
)
from l2shock.ui.analysis_controls import (
    AnalysisControlError,
    AnalysisPresetOption,
    analysis_result_summary,
    build_analysis_request_and_config,
    build_analysis_table_rows,
    load_enabled_analysis_presets,
    parse_local_analysis_datetime,
)
from l2shock.ui.analysis_legend import (
    open_analysis_color_legend,
)
from l2shock.ui.analysis_runtime import (
    AnalysisRuntimeBusyError,
    get_manual_analysis_runtime,
)
from l2shock.ui.availability_calendar import (
    AnalysisRangeHandoff,
)
from l2shock.ui.components import (
    create_tracked_task,
    persistent_notify,
    section_header,
)
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)


def _local_input_values(
    value_utc,
    *,
    timezone_name: str,
) -> tuple[str, str]:
    local = utc_to_local(
        value_utc,
        timezone_name,
    )

    return (
        local.strftime("%Y-%m-%d"),
        local.strftime("%H:%M:%S"),
    )


def _table_columns() -> list[dict[str, Any]]:
    """Return sortable QTable column ownership."""
    numeric = {
        "align": "right",
        "sortable": True,
    }
    text = {
        "align": "left",
        "sortable": True,
    }

    return [
        {
            "name": "metric",
            "label": "Metric",
            "field": "metric",
            **text,
        },
        {
            "name": "timeframe",
            "label": "TF",
            "field": "timeframe",
            **text,
        },
        {
            "name": "direction",
            "label": "Direction",
            "field": "direction",
            **text,
        },
        {
            "name": "final_rank",
            "label": "Selected Rank",
            "field": "final_rank",
            **numeric,
        },
        {
            "name": "population_rank",
            "label": "Population Rank",
            "field": "population_rank",
            **numeric,
        },
        {
            "name": "start_time",
            "label": "Start",
            "field": "start_time",
            **text,
        },
        {
            "name": "end_time",
            "label": "Extremum",
            "field": "end_time",
            **text,
        },
        {
            "name": "confirmation_time",
            "label": "Confirmation",
            "field": "confirmation_time",
            **text,
        },
        {
            "name": "terminal_offline",
            "label": "Offline Terminal",
            "field": "terminal_offline",
            **text,
        },
        {
            "name": "absolute_height",
            "label": "Absolute Height",
            "field": "absolute_height",
            **numeric,
        },
        {
            "name": "relative_height",
            "label": "Relative Height",
            "field": "relative_height",
            **numeric,
        },
        {
            "name": "bars",
            "label": "Bars",
            "field": "bars",
            **numeric,
        },
        {
            "name": "sharpness",
            "label": "Sharpness",
            "field": "sharpness",
            **numeric,
        },
        {
            "name": "primary_evidence",
            "label": "Primary",
            "field": "primary_evidence",
            **numeric,
        },
        {
            "name": "secondary_evidence",
            "label": "Secondary",
            "field": "secondary_evidence",
            **numeric,
        },
        {
            "name": "height_percentile",
            "label": "Height %",
            "field": "height_percentile",
            **numeric,
        },
        {
            "name": "height_normalized_z",
            "label": "Height Z",
            "field": "height_normalized_z",
            **numeric,
        },
        {
            "name": "sharpness_percentile",
            "label": "Sharpness %",
            "field": "sharpness_percentile",
            **numeric,
        },
        {
            "name": "sharpness_normalized_z",
            "label": "Sharpness Z",
            "field": "sharpness_normalized_z",
            **numeric,
        },
        {
            "name": "start_extremeness",
            "label": "Start Extreme",
            "field": "start_extremeness",
            **numeric,
        },
        {
            "name": "end_extremeness",
            "label": "End Extreme",
            "field": "end_extremeness",
            **numeric,
        },
        {
            "name": "boundary_extremeness",
            "label": "Boundary Mean",
            "field": "boundary_extremeness",
            **numeric,
        },
        {
            "name": "retracement_magnitude_quality",
            "label": "Retrace Magnitude",
            "field": "retracement_magnitude_quality",
            **numeric,
        },
        {
            "name": "retracement_count_quality",
            "label": "Retrace Count",
            "field": "retracement_count_quality",
            **numeric,
        },
        {
            "name": "adverse_move_count",
            "label": "Adverse Moves",
            "field": "adverse_move_count",
            **numeric,
        },
        {
            "name": "degraded_bars",
            "label": "Degraded Bars",
            "field": "degraded_bars",
            **numeric,
        },
        {
            "name": "selected_height",
            "label": "Top Height",
            "field": "selected_height",
            **text,
        },
        {
            "name": "selected_sharpness",
            "label": "Top Sharpness",
            "field": "selected_sharpness",
            **text,
        },
    ]


AnalysisHandoffHandler = Callable[
    [AnalysisRangeHandoff],
    Awaitable[bool],
]


def build_analysis_tab() -> AnalysisHandoffHandler:
    settings = get_settings()
    state = get_state()
    runtime = get_manual_analysis_runtime()

    completed_hour = floor_to_hour(now_utc())
    default_start_utc = completed_hour - timedelta(hours=1)
    default_end_utc = completed_hour - timedelta(seconds=1)

    start_date_value, start_time_value = _local_input_values(
        default_start_utc,
        timezone_name=settings.app.timezone,
    )
    end_date_value, end_time_value = _local_input_values(
        default_end_utc,
        timezone_name=settings.app.timezone,
    )

    observed_completion_sequence = runtime.snapshot().completion_sequence
    preset_options: tuple[AnalysisPresetOption, ...] = ()
    preset_loading = False

    pending_temporal_viewport: AnalysisChartTemporalViewport | None = None
    pending_temporal_request = None

    with ui.column().classes("w-full gap-3 p-4"):
        section_header("Analysis \u2014 Price and L2 Liquidity Movements")

        with ui.card().classes("w-full"):
            ui.label(
                "Run verified historical Liquidity Movement analysis "
                "from compact PostgreSQL L2 and real-trade price data."
            ).classes("font-semibold text-green-700")

            ui.label(
                f"Inputs use {settings.app.timezone}. Selected endpoints "
                "are closed and are snapped permissively to the active "
                "timeframe. Invalid and price-excluded runs remain hard "
                "discontinuities."
            ).classes("text-sm text-gray-600")

            with ui.row().classes("w-full gap-3 flex-wrap mt-3"):
                base_select = ui.select(
                    options=settings.analysis.supported_bases,
                    value=settings.analysis.supported_bases[0],
                    label="Base asset",
                ).classes("w-40")

                preset_select = ui.select(
                    options={},
                    value=None,
                    label="Enabled data preset",
                ).classes("w-[34rem] max-w-full")

                reload_presets_button = ui.button(
                    "Refresh presets",
                    icon="refresh",
                ).props("outline")

            preset_status_label = ui.label("Loading enabled data presets...").classes(
                "text-xs text-gray-500"
            )

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
                        label="Start time (24-hour HH:MM:SS)",
                        value=start_time_value,
                    )
                    .props('type=text inputmode=numeric placeholder="HH:MM:SS"')
                    .classes("w-44")
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
                        label="End time (24-hour HH:MM:SS)",
                        value=end_time_value,
                    )
                    .props('type=text inputmode=numeric placeholder="HH:MM:SS"')
                    .classes("w-44")
                )

            with ui.row().classes("w-full gap-3 flex-wrap mt-3"):
                activity_timeframe = ui.select(
                    options=settings.analysis.activity_timeframes,
                    value=settings.analysis.default_activity_timeframe,
                    label="Activity timeframe",
                ).classes("w-48")

                chart_timeframe_override = ui.select(
                    options={
                        "": "Automatic",
                        **{
                            timeframe.label: timeframe.label for timeframe in TIMEFRAMES
                        },
                    },
                    value="",
                    label="Chart timeframe",
                ).classes("w-48")

                maximum_chart_bars = ui.number(
                    label="Maximum automatic chart bars",
                    value=settings.analysis.default_chart_max_bars,
                    min=1,
                    step=1,
                ).classes("w-56")

                context_before = ui.number(
                    label="Context bars before",
                    value=settings.analysis.price_context_bars_before,
                    min=0,
                    step=1,
                ).classes("w-48")

                context_after = ui.number(
                    label="Context bars after",
                    value=settings.analysis.price_context_bars_after,
                    min=0,
                    step=1,
                ).classes("w-48")

            ui.label(
                "Automatic chooses the finest fixed timeframe fitting the "
                "maximum chart-bar budget. Selecting an explicit chart "
                "timeframe reruns verified loading and LM analysis for that "
                "resolution; it is not browser-side resampling."
            ).classes("text-xs text-gray-500")

            with ui.row().classes("w-full gap-3 flex-wrap mt-3 items-center"):
                minimum_price_enabled = ui.checkbox(
                    "Enable minimum price",
                    value=False,
                )
                minimum_price = (
                    ui.input(
                        label="Minimum price",
                        value="",
                    )
                    .props("type=number min=0 step=any")
                    .classes("w-48")
                    .disable()
                )

                maximum_price_enabled = ui.checkbox(
                    "Enable maximum price",
                    value=False,
                )
                maximum_price = (
                    ui.input(
                        label="Maximum price",
                        value="",
                    )
                    .props("type=number min=0 step=any")
                    .classes("w-48")
                    .disable()
                )

            ui.label(
                "Price bounds use true OHLC intersection. Display clipping "
                "never changes raw Binance OHLC or L2 depth arithmetic."
            ).classes("text-xs text-gray-500")

            with ui.row().classes("w-full gap-3 flex-wrap mt-3"):
                confirmation_fraction = (
                    ui.input(
                        label="LM confirmation fraction",
                        value=str(
                            settings.analysis.lm.confirmation_retracement_fraction
                        ),
                    )
                    .props("type=number min=0.000001 max=0.999999 step=0.01")
                    .classes("w-56")
                )

                top_n_height = ui.number(
                    label="Top N by height",
                    value=settings.analysis.lm.top_n_height,
                    min=1,
                    max=1000,
                    step=1,
                ).classes("w-48")

                top_n_sharpness = ui.number(
                    label="Top N by sharpness",
                    value=settings.analysis.lm.top_n_sharpness,
                    min=1,
                    max=1000,
                    step=1,
                ).classes("w-48")

            with ui.row().classes("gap-2 mt-3"):
                run_button = ui.button(
                    "Run Analysis",
                    icon="play_arrow",
                ).props("color=primary")

                stop_button = (
                    ui.button(
                        "Stop Analysis",
                        icon="stop_circle",
                    )
                    .props("color=negative outline")
                    .disable()
                )

        with ui.card().classes("w-full"):
            ui.label("Analysis progress").classes("font-semibold")

            progress_bar = ui.linear_progress(
                value=0.0,
                show_value=False,
            ).classes("w-full")

            progress_status_label = ui.label("Idle.").classes("text-sm font-semibold")
            progress_slice_label = ui.label("Current slice: none").classes(
                "text-xs font-mono"
            )
            result_summary_label = ui.label(
                "No analysis has completed in this process."
            ).classes("text-sm text-gray-600")

        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center gap-3 flex-wrap"):
                ui.label("Synchronized Price and Liquidity Workspace").classes(
                    "text-lg font-semibold"
                )

                ui.space()

                export_json_button = (
                    ui.button(
                        "Export JSON",
                        icon="data_object",
                    )
                    .props("outline")
                    .disable()
                )

                export_png_button = (
                    ui.button(
                        "Export PNG",
                        icon="image",
                    )
                    .props("outline")
                    .disable()
                )

                export_svg_button = (
                    ui.button(
                        "Export SVG",
                        icon="draw",
                    )
                    .props("outline")
                    .disable()
                )

                ui.button(
                    "Color Legend",
                    icon="palette",
                    on_click=open_analysis_color_legend,
                ).props("outline")

            ui.label(
                "The five panels share one compressed chart-timeframe axis, "
                "linked zoom, and linked vertical pointer. Timeline jumps "
                "mark filtered or invalid elapsed time."
            ).classes("text-sm text-gray-600")

            ui.label(
                "The highlight toggles control LM backgrounds only. Amber "
                "timeline jumps and red persistent-data warnings remain "
                "visible to show skipped or invalid data. Clicking an LM "
                "table row can also add a separate amber selected-row focus."
            ).classes("text-xs text-amber-700")

            with ui.row().classes("w-full gap-4 flex-wrap mt-2"):
                show_price_highlights = ui.checkbox(
                    "LM highlights on Price",
                    value=True,
                )
                show_liquidity_highlights = ui.checkbox(
                    "LM highlights on liquidity panels",
                    value=True,
                )
                show_activity_highlights = ui.checkbox(
                    "Activity-timeframe LMs",
                    value=True,
                )
                show_chart_highlights = ui.checkbox(
                    "Chart-timeframe LMs",
                    value=True,
                )
                show_height_highlights = ui.checkbox(
                    "Top Height",
                    value=True,
                )
                show_sharpness_highlights = ui.checkbox(
                    "Top Sharpness",
                    value=True,
                )

            with ui.row().classes("w-full gap-4 flex-wrap"):
                show_bid_highlights = ui.checkbox(
                    "Bid LM",
                    value=True,
                )
                show_ask_highlights = ui.checkbox(
                    "Ask LM",
                    value=True,
                )
                show_total_highlights = ui.checkbox(
                    "Total LM",
                    value=True,
                )
                show_imbalance_highlights = ui.checkbox(
                    "Imbalance LM",
                    value=True,
                )

            analysis_chart = (
                ui.echart(empty_analysis_chart_option())
                .classes("w-full h-[78vh] min-h-[680px]")
                .props("renderer=canvas")
            )

        with ui.card().classes("w-full"):
            ui.label("Selected Liquidity Movements").classes("text-lg font-semibold")

            ui.label(
                "Rows are the union of Top-N height and Top-N sharpness "
                "within independent metric \u00d7 timeframe \u00d7 direction "
                "populations. Click a column heading to sort."
            ).classes("text-sm text-gray-600")

            result_table = (
                ui.table(
                    columns=_table_columns(),
                    rows=[],
                    row_key="id",
                    pagination=25,
                    selection="single",
                )
                .classes("w-full")
                .props("dense flat bordered virtual-scroll")
            )

            table_navigation_status = ui.label(
                "Click a selected-LM row to zoom all five panels to that movement."
            ).classes("text-xs text-gray-500")

        with ui.card().classes("w-full"):
            ui.label("Locked analysis principles").classes("font-semibold")
            ui.markdown("""
- Price: Binance BTCUSDT/ETHUSDT USD-M perpetual trades only.
- L2 metrics: Bid Liquidity, Ask Liquidity, Total Liquidity, Bid-Ask Imbalance.
- Ranking populations: metric \u00d7 timeframe \u00d7 direction.
- Event location: actual extremum, never the later confirmation bar.
- Invalid or filtered discontinuities cannot be crossed by an LM.
- Analysis remains historical, offline, and post-scan.
            """).classes("text-sm")

            ui.label(
                "Fixed-duration chart timeframe switching and availability-"
                "calendar handoff are available. Calendar-month timeframes "
                "remain deferred."
            ).classes("text-xs text-gray-600")

    chart_controller = AnalysisChartController(
        analysis_chart,
        crosshair_gap_px=50,
    )
    controlled_inputs = (
        base_select,
        preset_select,
        reload_presets_button,
        start_date,
        start_time,
        end_date,
        end_time,
        activity_timeframe,
        chart_timeframe_override,
        maximum_chart_bars,
        context_before,
        context_after,
        minimum_price_enabled,
        maximum_price_enabled,
        confirmation_fraction,
        top_n_height,
        top_n_sharpness,
    )

    chart_visibility_inputs = (
        show_price_highlights,
        show_liquidity_highlights,
        show_activity_highlights,
        show_chart_highlights,
        show_height_highlights,
        show_sharpness_highlights,
        show_bid_highlights,
        show_ask_highlights,
        show_total_highlights,
        show_imbalance_highlights,
    )

    def _chart_visibility() -> AnalysisChartVisibility:
        from l2shock.analysis import LiquidityMetric

        metrics = set()

        if show_bid_highlights.value:
            metrics.add(LiquidityMetric.BID_LIQUIDITY)

        if show_ask_highlights.value:
            metrics.add(LiquidityMetric.ASK_LIQUIDITY)

        if show_total_highlights.value:
            metrics.add(LiquidityMetric.TOTAL_LIQUIDITY)

        if show_imbalance_highlights.value:
            metrics.add(LiquidityMetric.BID_ASK_DELTA)

        return AnalysisChartVisibility(
            show_price_highlights=bool(show_price_highlights.value),
            show_liquidity_highlights=bool(show_liquidity_highlights.value),
            show_activity_candidates=bool(show_activity_highlights.value),
            show_chart_candidates=bool(show_chart_highlights.value),
            show_height_selected=bool(show_height_highlights.value),
            show_sharpness_selected=bool(show_sharpness_highlights.value),
            highlighted_metrics=frozenset(metrics),
        )

    async def _render_latest_chart(
        *,
        preserve_viewport: bool = True,
        temporal_viewport: AnalysisChartTemporalViewport | None = None,
        expected_runtime_result=None,
    ) -> None:
        initial_snapshot = runtime.snapshot()

        if initial_snapshot.is_running:
            return

        runtime_result = (
            expected_runtime_result
            if expected_runtime_result is not None
            else initial_snapshot.last_result
        )

        if runtime_result is None or runtime_result.analysis is None:
            return

        if (
            expected_runtime_result is not None
            and initial_snapshot.last_result is not expected_runtime_result
        ):
            return

        analysis = runtime_result.analysis

        option = build_analysis_chart_option(
            analysis,
            timezone_name=settings.app.timezone,
            visibility=_chart_visibility(),
            lm_settings=settings.analysis.lm,
            l2_warning_seconds=(settings.analysis.l2_long_invalid_warning_seconds),
            price_warning_seconds=(
                settings.analysis.price_long_invalid_warning_minutes * 60
            ),
        )

        try:
            committed = await chart_controller.publish(
                option,
                owner_id=analysis.analysis_id,
                preserve_viewport=preserve_viewport,
                temporal_viewport=temporal_viewport,
            )
        except AnalysisChartInteractionError as exc:
            log.exception("Analysis chart publication failed.")
            persistent_notify(
                str(exc),
                title="Analysis chart",
                notification_type="negative",
            )
            return

        if committed is None:
            log.debug("Analysis chart publication was superseded before commit.")
            return

        latest_snapshot = runtime.snapshot()

        if (
            latest_snapshot.is_running
            or latest_snapshot.last_result is not runtime_result
        ):
            chart_controller.invalidate()
            log.debug(
                "Analysis chart publication lost runtime-result ownership "
                "after browser acknowledgement."
            )

    def _schedule_latest_chart_render(
        *,
        preserve_viewport: bool = True,
        temporal_viewport: AnalysisChartTemporalViewport | None = None,
        expected_runtime_result=None,
    ) -> None:
        create_tracked_task(
            _render_latest_chart(
                preserve_viewport=preserve_viewport,
                temporal_viewport=temporal_viewport,
                expected_runtime_result=expected_runtime_result,
            ),
            name="l2shock-analysis-chart-render",
        )

    def _completed_analysis():
        runtime_result = runtime.snapshot().last_result

        if runtime_result is None or runtime_result.analysis is None:
            return None

        return runtime_result.analysis

    def _export_analysis_json() -> None:
        analysis = _completed_analysis()

        if analysis is None:
            persistent_notify(
                "No completed Analysis result is available.",
                title="Analysis JSON export",
                notification_type="negative",
            )
            return

        try:
            content = analysis_export_json_bytes(
                analysis,
                timezone_name=settings.app.timezone,
            )
            filename = analysis_export_filename(analysis)

            # NiceGUI accepts raw byte content for direct browser download.
            ui.download(
                content,
                filename=filename,
            )
        except Exception as exc:
            log.exception("Analysis JSON export failed.")
            persistent_notify(
                f"Could not export Analysis JSON: " f"{type(exc).__name__}",
                title="Analysis JSON export",
                notification_type="negative",
            )

    async def _export_analysis_image(
        image_format: str,
    ) -> None:
        analysis = _completed_analysis()

        if analysis is None:
            persistent_notify(
                "No completed Analysis result is available.",
                title="Analysis chart export",
                notification_type="negative",
            )
            return

        filename = (
            "l2shock_"
            + analysis.dataset.request.base
            + "_"
            + analysis.analysis_id[:16]
            + "_chart."
            + image_format
        )

        exported, reason = await chart_controller.export_image(
            owner_id=analysis.analysis_id,
            image_format=image_format,
            filename=filename,
        )

        if not exported:
            persistent_notify(
                reason,
                title="Analysis chart export",
                notification_type="negative",
            )
            return

        ui.notify(
            f"{image_format.upper()} chart export started.",
            type="positive",
            timeout=4000,
        )

    async def _export_analysis_png() -> None:
        await _export_analysis_image("png")

    async def _export_analysis_svg() -> None:
        await _export_analysis_image("svg")

    def _row_from_event(event: Any) -> dict[str, Any] | None:
        args = getattr(event, "args", None)

        if isinstance(args, dict):
            if isinstance(args.get("row"), dict):
                return dict(args["row"])

            if "metric" in args and "population_rank" in args:
                return dict(args)

        if isinstance(args, (list, tuple)):
            for item in args:
                if not isinstance(item, dict):
                    continue

                if isinstance(item.get("row"), dict):
                    return dict(item["row"])

                if "metric" in item and "population_rank" in item:
                    return dict(item)

        return None

    async def _navigate_to_table_row(event: Any) -> None:
        row = _row_from_event(event)

        if row is None:
            table_navigation_status.text = "Could not identify the clicked LM row."
            return

        runtime_result = runtime.snapshot().last_result

        if runtime_result is None or runtime_result.analysis is None:
            table_navigation_status.text = (
                "No completed Analysis result currently owns the chart."
            )
            return

        analysis = runtime_result.analysis

        try:
            metric = str(row.get("metric") or "")
            timeframe = str(row.get("timeframe") or "")
            direction = str(row.get("direction") or "")
            population_rank = int(row.get("population_rank"))
        except TypeError, ValueError:
            table_navigation_status.text = (
                "The selected row has incomplete ranking identity."
            )
            return

        ranking = next(
            (
                candidate
                for candidate in analysis.selected
                if (
                    candidate.candidate.metric.value == metric
                    and candidate.candidate.timeframe_label == timeframe
                    and candidate.candidate.direction.value == direction
                    and candidate.population_rank == population_rank
                )
            ),
            None,
        )

        if ranking is None:
            table_navigation_status.text = (
                "The selected table row no longer belongs to the "
                "current Analysis result."
            )
            return

        window = chart_navigation_window(
            analysis,
            ranking,
            padding_bars=5,
        )

        if window is None:
            table_navigation_status.text = (
                "This LM does not overlap the visible chart-timeframe core."
            )
            return

        navigated = await chart_controller.navigate(
            window,
            owner_id=analysis.analysis_id,
        )

        if not navigated:
            table_navigation_status.text = (
                "Chart navigation was rejected because the browser render "
                "generation changed. Re-rendering the latest chart."
            )
            _schedule_latest_chart_render(
                preserve_viewport=False,
            )
            return

        result_table.selected = [row]
        result_table.update()

        table_navigation_status.text = (
            f"Focused {metric} / {timeframe} / {direction} "
            f"population rank {population_rank}; "
            f"chart categories {window.candidate_start_index}"
            f"\u2013{window.candidate_end_index}."
        )

    def _sync_price_inputs() -> None:
        snapshot = runtime.snapshot()
        operation_idle = (
            not snapshot.is_running
            and not bool(state.active_operation_name)
            and not state.shutdown_started
        )

        if operation_idle and minimum_price_enabled.value:
            minimum_price.enable()
        else:
            minimum_price.disable()

        if operation_idle and maximum_price_enabled.value:
            maximum_price.enable()
        else:
            maximum_price.disable()

    async def _reload_presets(_event=None) -> None:
        nonlocal preset_options
        nonlocal preset_loading

        if preset_loading or runtime.is_running:
            return

        preset_loading = True
        preset_status_label.text = "Loading enabled data presets..."
        reload_presets_button.disable()

        try:
            loaded = await asyncio.to_thread(load_enabled_analysis_presets)
            preset_options = loaded

            selected_base = str(base_select.value or "").strip().upper()

            matching = tuple(
                option for option in loaded if option.base == selected_base
            )
            options = {option.preset_hash: option.label for option in matching}

            previous_value = preset_select.value
            preset_select.options = options

            if previous_value in options:
                preset_select.value = previous_value
            elif matching:
                preset_select.value = matching[0].preset_hash
            else:
                preset_select.value = None

            preset_select.update()

            preset_status_label.text = (
                f"{len(matching)} enabled {selected_base} preset(s); "
                f"{len(loaded)} enabled preset(s) total."
            )

        except Exception as exc:
            preset_options = ()
            preset_select.options = {}
            preset_select.value = None
            preset_select.update()
            preset_status_label.text = "Could not load enabled presets."

            log.exception("Could not load Analysis preset choices.")
            persistent_notify(
                f"Could not load analysis presets: " f"{type(exc).__name__}",
                title="Analysis presets",
                notification_type="negative",
            )
        finally:
            preset_loading = False

    async def _apply_analysis_handoff(
        handoff: AnalysisRangeHandoff,
    ) -> bool:
        """Apply a verified contiguous availability window to Analysis."""

        if not isinstance(handoff, AnalysisRangeHandoff):
            raise TypeError("handoff must be AnalysisRangeHandoff")

        if runtime.is_running:
            persistent_notify(
                "Stop or finish the current Analysis before applying "
                "an availability-calendar window.",
                title="Analysis handoff",
                notification_type="warning",
            )
            return False

        while preset_loading:
            await asyncio.sleep(0.05)

        base_select.value = handoff.base
        base_select.update()

        await _reload_presets()

        preset_values = set(
            str(value)
            for value in (
                preset_select.options.keys()
                if isinstance(
                    preset_select.options,
                    dict,
                )
                else preset_select.options
            )
        )

        if handoff.preset_hash not in preset_values:
            persistent_notify(
                (
                    f"The calendar preset {handoff.preset_hash} is no longer "
                    f"enabled for {handoff.base}. Refresh the calendar and "
                    "try again."
                ),
                title="Analysis handoff",
                notification_type="negative",
            )
            return False

        preset_select.value = handoff.preset_hash
        preset_select.update()

        start_date_value, start_time_value = _local_input_values(
            handoff.start_utc,
            timezone_name=settings.app.timezone,
        )
        end_date_value, end_time_value = _local_input_values(
            handoff.closed_end_utc,
            timezone_name=settings.app.timezone,
        )

        start_date.value = start_date_value
        start_time.value = start_time_value
        end_date.value = end_date_value
        end_time.value = end_time_value

        start_date.update()
        start_time.update()
        end_date.update()
        end_time.update()

        result_summary_label.text = (
            f"Availability handoff applied: {handoff.base}, "
            f"{handoff.hour_count} contiguous hour(s). "
            "Review controls and press Run Analysis."
        )

        ui.notify(
            "Contiguous analyzable window applied to Analysis controls.",
            type="positive",
            timeout=5000,
        )
        return True

    async def _start_analysis() -> None:
        nonlocal pending_temporal_request
        nonlocal pending_temporal_viewport
        if state.shutdown_started:
            persistent_notify(
                "Application shutdown has started; new operations " "are blocked.",
                title="Analysis not started",
                notification_type="negative",
            )
            return

        if state.active_operation_name:
            persistent_notify(
                f"Another operation is active: " f"{state.active_operation_name}",
                title="Analysis not started",
                notification_type="negative",
            )
            return

        preset_hash = str(preset_select.value or "").strip()

        if not preset_hash:
            persistent_notify(
                "Select an enabled data preset. If none is available, "
                "process downloaded L2 data first.",
                title="Analysis not started",
                notification_type="negative",
            )
            return

        try:
            requested_start_utc = parse_local_analysis_datetime(
                start_date.value,
                start_time.value,
                timezone_name=settings.app.timezone,
                field_name="Analysis start",
            )
            requested_end_utc = parse_local_analysis_datetime(
                end_date.value,
                end_time.value,
                timezone_name=settings.app.timezone,
                field_name="Analysis end",
            )

            request, config = build_analysis_request_and_config(
                base=str(base_select.value or ""),
                preset_hash=preset_hash,
                requested_start_utc=requested_start_utc,
                requested_end_utc=requested_end_utc,
                activity_timeframe=str(activity_timeframe.value or ""),
                maximum_chart_bars=maximum_chart_bars.value,
                chart_timeframe_override=(
                    str(chart_timeframe_override.value or "").strip() or None
                ),
                minimum_price_enabled=bool(minimum_price_enabled.value),
                minimum_price_value=minimum_price.value,
                maximum_price_enabled=bool(maximum_price_enabled.value),
                maximum_price_value=maximum_price.value,
                context_before=context_before.value,
                context_after=context_after.value,
                confirmation_fraction=(confirmation_fraction.value),
                top_n_height=top_n_height.value,
                top_n_sharpness=top_n_sharpness.value,
                lm_settings=settings.analysis.lm,
            )

            if request.chart_timeframe_override is not None:
                snapped = snap_closed_analysis_range(
                    request.requested_start_utc,
                    request.requested_end_utc,
                    request.chart_timeframe_override,
                )

                if snapped.bar_count > request.maximum_chart_bars:
                    raise AnalysisControlError(
                        "The explicit chart timeframe produces "
                        f"{snapped.bar_count} bars, exceeding "
                        f"maximum_chart_bars={request.maximum_chart_bars}. "
                        "Select a coarser chart timeframe or increase "
                        "the chart-bar limit."
                    )

            previous_result = runtime.snapshot().last_result
            previous_analysis = (
                previous_result.analysis if previous_result is not None else None
            )
            chart_commit = chart_controller.commit

            same_chart_window = bool(
                previous_analysis is not None
                and chart_commit is not None
                and chart_commit.owner_id == previous_analysis.analysis_id
                and previous_result.request.base == request.base
                and previous_result.request.preset_hash == request.preset_hash
                and (
                    previous_result.request.requested_start_utc
                    == request.requested_start_utc
                )
                and (
                    previous_result.request.requested_end_utc
                    == request.requested_end_utc
                )
            )

            temporal_viewport = (
                await capture_analysis_chart_temporal_viewport(analysis_chart)
                if same_chart_window
                else None
            )

            runtime.start(
                request=request,
                config=config,
            )

            # Preserve temporal navigation only for the same rendered
            # base/preset/time window. A new window starts fully zoomed out.
            # Bind any captured viewport to this exact immutable request.
            pending_temporal_request = request
            pending_temporal_viewport = temporal_viewport

        except (
            AnalysisControlError,
            AnalysisRuntimeBusyError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            persistent_notify(
                str(exc),
                title="Run Analysis",
                notification_type="negative",
            )

    def _stop_analysis() -> None:
        if runtime.request_stop():
            ui.notify(
                "Stop requested. Analysis will stop at its next "
                "cooperative boundary.",
                type="warning",
                timeout=5000,
            )
        else:
            ui.notify(
                "No stoppable analysis operation is active.",
                type="info",
                timeout=3000,
            )

    minimum_price_enabled.on_value_change(lambda _event: _sync_price_inputs())
    maximum_price_enabled.on_value_change(lambda _event: _sync_price_inputs())

    for visibility_input in chart_visibility_inputs:
        visibility_input.on_value_change(
            lambda _event: _schedule_latest_chart_render(
                preserve_viewport=True,
            )
        )

    result_table.on(
        "rowClick",
        _navigate_to_table_row,
    )

    export_json_button.on_click(_export_analysis_json)
    export_png_button.on_click(_export_analysis_png)
    export_svg_button.on_click(_export_analysis_svg)

    base_select.on_value_change(_reload_presets)
    reload_presets_button.on_click(_reload_presets)
    run_button.on_click(_start_analysis)
    stop_button.on_click(_stop_analysis)

    def _refresh_runtime_view() -> None:
        nonlocal observed_completion_sequence
        nonlocal pending_temporal_request
        nonlocal pending_temporal_viewport

        snapshot = runtime.snapshot()
        operation_idle = not bool(state.active_operation_name)
        new_work_allowed = (
            operation_idle and not state.shutdown_started and not snapshot.is_running
        )

        if new_work_allowed:
            for element in controlled_inputs:
                element.enable()

            if preset_loading:
                reload_presets_button.disable()

            if not preset_select.options:
                preset_select.disable()
                run_button.disable()
            else:
                run_button.enable()
        else:
            for element in controlled_inputs:
                element.disable()
            run_button.disable()

        _sync_price_inputs()

        completed_analysis = (
            snapshot.last_result.analysis
            if (
                snapshot.last_result is not None
                and snapshot.last_result.analysis is not None
            )
            else None
        )

        export_allowed = bool(
            completed_analysis is not None
            and not snapshot.is_running
            and not state.shutdown_started
        )

        if export_allowed:
            export_json_button.enable()
        else:
            export_json_button.disable()

        committed = chart_controller.commit
        image_export_allowed = bool(
            export_allowed
            and committed is not None
            and completed_analysis is not None
            and committed.owner_id == completed_analysis.analysis_id
        )

        if image_export_allowed:
            export_png_button.enable()
            export_svg_button.enable()
        else:
            export_png_button.disable()
            export_svg_button.disable()

        if snapshot.is_running and not snapshot.stop_requested:
            stop_button.enable()
        else:
            stop_button.disable()

        progress = snapshot.latest_progress

        if progress is not None:
            progress_bar.value = progress.fraction_complete
            progress_status_label.text = f"{progress.message} [{progress.phase.value}]"
            progress_slice_label.text = (
                "Current slice: "
                f"{progress.current_metric or 'none'} / "
                f"{progress.current_timeframe_label or 'none'}"
            )
        elif snapshot.is_running:
            progress_bar.value = 0.0
            progress_status_label.text = "Starting verified analysis..."
            progress_slice_label.text = "Current slice: loading"
        else:
            progress_status_label.text = "Idle."
            progress_slice_label.text = "Current slice: none"

        if snapshot.completion_sequence != observed_completion_sequence:
            observed_completion_sequence = snapshot.completion_sequence

            if snapshot.last_result is not None:
                result = snapshot.last_result

                if result.analysis is not None:
                    progress_bar.value = 1.0
                    rows = build_analysis_table_rows(
                        result.analysis,
                        timezone_name=settings.app.timezone,
                    )
                    result_table.rows = rows
                    result_table.update()

                    result_table.selected = []
                    result_table.update()

                    table_navigation_status.text = (
                        "Click a selected-LM row to zoom all five panels "
                        "to that movement."
                    )

                    temporal_viewport = (
                        pending_temporal_viewport
                        if result.request == pending_temporal_request
                        else None
                    )

                    pending_temporal_request = None
                    pending_temporal_viewport = None

                    _schedule_latest_chart_render(
                        preserve_viewport=False,
                        temporal_viewport=temporal_viewport,
                        expected_runtime_result=result,
                    )

                    result_summary_label.text = analysis_result_summary(result.analysis)

                    partial_market_hours = sum(
                        coverage.l2_market_coverage_degraded
                        for coverage in result.analysis.dataset.coverage
                    )

                    if partial_market_hours > 0:
                        persistent_notify(
                            (
                                "Analysis continued with partial L2 market "
                                f"coverage in {partial_market_hours} hour(s). "
                                "At least one expected market was unavailable "
                                "or lacked a compact L2 row. Available valid "
                                "markets were used; missing markets were not "
                                "interpreted as zero liquidity."
                            ),
                            title="Partial L2 market coverage",
                            notification_type="warning",
                        )

                    persistent_notify(
                        (
                            f"Analysis completed: "
                            f"{result.candidate_count} candidate(s), "
                            f"{result.selected_count} selected."
                        ),
                        title="Analysis completed",
                        notification_type="positive",
                    )
                else:
                    result_table.rows = []
                    result_table.update()
                    pending_temporal_request = None
                    pending_temporal_viewport = None
                    chart_controller.invalidate()

                    stopped_option = empty_analysis_chart_option(
                        "Analysis stopped; no partial result was published"
                    )
                    stopped_option["l2shockChartMetadata"] = {
                        "analysis_id": "stopped",
                        "dataset_analysis_id": "stopped",
                        "chart_timeframe": "",
                        "activity_timeframe": "",
                        "visible_bar_count": 0,
                        "source_bar_indices": [],
                        "discontinuities": {},
                    }

                    create_tracked_task(
                        chart_controller.publish(
                            stopped_option,
                            owner_id="stopped",
                            preserve_viewport=False,
                        ),
                        name="l2shock-analysis-chart-clear",
                    )

                    table_navigation_status.text = (
                        "No completed Analysis result owns the chart."
                    )

                    result_summary_label.text = (
                        "Analysis stopped. No partial result " "was published."
                    )
                    persistent_notify(
                        result_summary_label.text,
                        title="Analysis stopped",
                        notification_type="warning",
                    )

            elif snapshot.last_error:
                result_table.rows = []
                result_table.selected = []
                result_table.update()

                pending_temporal_request = None
                pending_temporal_viewport = None
                chart_controller.invalidate()

                failed_option = empty_analysis_chart_option(
                    "Analysis failed; the previous result was cleared"
                )
                failed_option["l2shockChartMetadata"] = {
                    "analysis_id": "failed",
                    "dataset_analysis_id": "failed",
                    "chart_timeframe": "",
                    "activity_timeframe": "",
                    "visible_bar_count": 0,
                    "visible_start_times_utc": [],
                    "bar_duration_seconds": 1,
                    "source_bar_indices": [],
                    "discontinuities": {},
                }

                create_tracked_task(
                    chart_controller.publish(
                        failed_option,
                        owner_id="failed",
                        preserve_viewport=False,
                    ),
                    name="l2shock-analysis-chart-clear-after-failure",
                )

                table_navigation_status.text = (
                    "No completed Analysis result owns the chart."
                )
                result_summary_label.text = (
                    "Last analysis error: " f"{snapshot.last_error}"
                )

                persistent_notify(
                    snapshot.last_error,
                    title="Analysis failed",
                    notification_type="negative",
                )

    ui.timer(
        interval=0.25,
        callback=_refresh_runtime_view,
        active=True,
    )
    ui.timer(
        interval=0.1,
        callback=_reload_presets,
        once=True,
    )
    return _apply_analysis_handoff


__all__ = [
    "build_analysis_tab",
    "AnalysisHandoffHandler",
    "AnalysisRangeHandoff",
]

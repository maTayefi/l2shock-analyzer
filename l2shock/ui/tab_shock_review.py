# l2shock/ui/tab_shock_review.py
"""Shock-Start section of the Analysis tab.

This is intentionally distinct from the legacy LM controls during migration.
Price is optional visual context, never a condition for running a review.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from nicegui import ui

from l2shock.analysis.shock_dataset import ShockDatasetRequest
from l2shock.config import get_settings
from l2shock.timeutils import utc_to_local
from l2shock.analysis.shock_evidence import ShockEvidenceConfig
from l2shock.analysis.shock_start import (
    ShockStartConfig,
    ShockStructuralScale,
)
from l2shock.ui.analysis_controls import (
    load_enabled_analysis_presets,
    parse_local_analysis_datetime,
)
from l2shock.ui.shock_legend import open_shock_color_legend
from l2shock.ui.chart_interactions import (
    AnalysisChartController,
    capture_shock_time_viewport,
)
from l2shock.ui.components import create_tracked_task
from l2shock.ui.shock_inspection import (
    ShockInspectionModel,
    publish_shock_selection,
)
from l2shock.ui.shock_view_selection import (
    build_shock_view_selection,
)
from l2shock.ui.shock_price_context import (
    load_shock_price_context,
)
from l2shock.ui.shock_runtime import (
    ShockRuntimeBusyError,
    ShockRuntimePhase,
    get_manual_shock_runtime,
)
from l2shock.ui.state import get_state

log = logging.getLogger(__name__)

_HASH_RE = re.compile(r"[0-9a-fA-F]{64}\Z")


def _utc_input(value: object, name: str) -> datetime:
    text = str(value or "").strip()

    if not text:
        raise ValueError(f"{name} is required")

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 UTC timestamp") from exc

    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must explicitly specify UTC")

    return parsed.astimezone(timezone.utc)


def _scale_fraction(value: object, name: str) -> Decimal:
    try:
        fraction = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a decimal fraction") from exc

    if not fraction.is_finite() or not Decimal(0) < fraction < Decimal(1):
        raise ValueError(f"{name} must be between 0 and 1")

    return fraction


def _pivot_radius(value: object, name: str) -> int:
    """Parse a positive, whole-second structural radius without truncation."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive whole number of seconds")

    if isinstance(value, int):
        radius = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{name} must be a positive whole number of seconds")
        radius = int(value)
    else:
        text = str(value or "").strip()

        if not text or not text.isascii() or not text.isdecimal():
            raise ValueError(f"{name} must be a positive whole number of seconds")

        radius = int(text)

    if not 1 <= radius <= 3_600:
        raise ValueError(f"{name} must be between 1 and 3,600 seconds")

    return radius


def _scale_count(value: object) -> int:
    """Accept only the two explicitly supported structural-scale counts."""
    if isinstance(value, bool):
        raise ValueError("Scale count must be 2 or 3")

    if isinstance(value, int):
        count = value
    elif isinstance(value, float) and value.is_integer():
        count = int(value)
    elif isinstance(value, str) and value.strip() in {"2", "3"}:
        count = int(value.strip())
    else:
        raise ValueError("Scale count must be 2 or 3")

    if count not in {2, 3}:
        raise ValueError("Scale count must be 2 or 3")

    return count


def build_shock_request(
    *,
    base: object,
    preset_hash: object,
    start_utc: object,
    end_utc: object,
    major_fraction: object,
    medium_fraction: object,
    minor_fraction: object,
    scale_count: object = 3,
    major_radius_seconds: object = 60,
    medium_radius_seconds: object = 30,
    minor_radius_seconds: object = 10,
) -> tuple[ShockDatasetRequest, ShockStartConfig]:
    """Validate one-second structural inputs before operation admission.

    Scale thresholds are fractions of the verified whole-scan Total range.
    Pivot radii are structural neighborhoods, not chart timeframes.
    Selecting two scales deliberately excludes the Minor tier.
    """
    selected_base = str(base or "").strip().upper()
    selected_hash = str(preset_hash or "").strip().lower()

    if selected_base not in {"BTC", "ETH"}:
        raise ValueError("Base must be BTC or ETH")

    if not _HASH_RE.fullmatch(selected_hash):
        raise ValueError("Preset hash must contain 64 hexadecimal digits")

    start = _utc_input(start_utc, "Start UTC")
    end = _utc_input(end_utc, "End UTC")

    if end < start:
        raise ValueError("End UTC must not precede Start UTC")

    if (end - start).total_seconds() > 86_400:
        raise ValueError("Shock-Start scan cannot exceed 24 hours")

    count = _scale_count(scale_count)

    # The ordering of these tiers is analytical input. Do not silently sort
    # thresholds or turn a chart timeframe into a pivot radius.
    major = ShockStructuralScale(
        "major",
        _scale_fraction(major_fraction, "Major threshold"),
        _pivot_radius(major_radius_seconds, "Major pivot radius"),
    )
    medium = ShockStructuralScale(
        "medium",
        _scale_fraction(medium_fraction, "Medium threshold"),
        _pivot_radius(medium_radius_seconds, "Medium pivot radius"),
    )

    scales = (major, medium)

    if count == 3:
        minor = ShockStructuralScale(
            "minor",
            _scale_fraction(minor_fraction, "Minor threshold"),
            _pivot_radius(minor_radius_seconds, "Minor pivot radius"),
        )
        scales += (minor,)

    config = ShockStartConfig(scales=scales)

    request = ShockDatasetRequest(
        base=selected_base,
        preset_hash=selected_hash,
        requested_start_utc=start,
        requested_end_utc=end,
    )

    return request, config


def _event_row(event: Any) -> dict[str, Any] | None:
    """Extract the clicked row from supported NiceGUI rowClick shapes.

    Quasar's rowClick sends [browser_event, row, row_index].
    Some event adapters instead send {"row": row}. Do not treat
    the browser event dictionary itself as a table row.
    """
    args = getattr(event, "args", None)

    if isinstance(args, dict):
        row = args.get("row")
        return row if isinstance(row, dict) else None

    if isinstance(args, (list, tuple)):
        if (
            len(args) >= 2
            and isinstance(args[1], dict)
            and "inspection_position" in args[1]
            and "id" in args[1]
        ):
            return args[1]

        for item in args:
            if not isinstance(item, dict):
                continue

            row = item.get("row")

            if isinstance(row, dict):
                return row

    return None


_LOCAL_TIME_FIELDS = (
    "first_b_utc",
    "representative_b_utc",
    "representative_c_utc",
)


def _localized_row(
    row: dict[str, object],
    timezone_name: str,
) -> dict[str, object]:
    """Return a display copy with review timestamps in the local timezone.

    Field names keep their UTC-owned identity. Only the displayed text
    changes. "id" and "inspection_position" are never modified, so row
    clicks and review-ownership checks behave exactly as before.
    """
    result = dict(row)

    for field in _LOCAL_TIME_FIELDS:
        value = result.get(field)

        if not isinstance(value, str) or not value:
            continue

        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        result[field] = utc_to_local(parsed, timezone_name).isoformat(
            timespec="seconds"
        )

    return result


def _columns(timezone_name: str = "UTC") -> list[dict[str, object]]:
    fields = (
        ("inspection_position", "Inspect #"),
        ("direction", "Direction"),
        ("first_b_utc", f"B-area first ({timezone_name})"),
        ("representative_b_utc", f"Representative B ({timezone_name})"),
        ("representative_c_utc", f"C ({timezone_name})"),
        ("representative_kind", "B kind"),
        ("scale_names", "Scales"),
        ("member_count", "Members"),
        ("independent_channel_count", "Bid/Ask support"),
        ("total_bc_fraction_of_scan_range", "B→C / Total range"),
    )

    return [
        {
            "name": field,
            "label": label,
            "field": field,
            "align": "left",
        }
        for field, label in fields
    ]


def build_shock_review_section() -> None:
    """Build the Shock-Start section inside the existing Analysis tab."""
    state = get_state()
    runtime = get_manual_shock_runtime()
    timezone_name = get_settings().app.timezone

    initial_snapshot = runtime.snapshot()
    observed_completion = initial_snapshot.completion_sequence
    model: ShockInspectionModel | None = None
    selection_generation = 0
    selected_owner_id: str | None = None
    selected_position: int | None = None
    bounded_owner_id: str | None = None
    preset_loading = False
    enabled_preset_hashes: set[str] = set()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start_local = utc_to_local(now - timedelta(hours=1), timezone_name)
    end_local = utc_to_local(now, timezone_name)

    with ui.card().classes("w-full"):
        ui.label("Shock-Start — verified Total L2").classes("text-xl font-semibold")
        ui.label(
            "Separate from legacy LM during migration. "
            "Detection uses verified one-second L2. Verified Binance "
            "trade-price candles are optional chart context, loaded "
            "only when a chart is opened."
        ).classes("text-sm text-gray-500")

        with ui.row().classes("w-full gap-4 flex-wrap"):
            base_input = ui.select(
                {"BTC": "BTC", "ETH": "ETH"},
                value="BTC",
                label="Base",
            )
            preset_input = ui.select(
                options={},
                value=None,
                label="Enabled L2 component preset",
            ).classes("w-[34rem] max-w-full")
            start_date_input = (
                ui.input(
                    f"Start date ({timezone_name})",
                    value=start_local.strftime("%Y-%m-%d"),
                )
                .props("type=date")
                .classes("w-44")
            )
            start_time_input = (
                ui.input(
                    "Start time (24-hour HH:MM:SS)",
                    value=start_local.strftime("%H:%M:%S"),
                )
                .props('type=text inputmode=numeric placeholder="HH:MM:SS"')
                .classes("w-44")
            )
            end_date_input = (
                ui.input(
                    f"End date ({timezone_name})",
                    value=end_local.strftime("%Y-%m-%d"),
                )
                .props("type=date")
                .classes("w-44")
            )
            end_time_input = (
                ui.input(
                    "End time (24-hour HH:MM:SS)",
                    value=end_local.strftime("%H:%M:%S"),
                )
                .props('type=text inputmode=numeric placeholder="HH:MM:SS"')
                .classes("w-44")
            )

        with ui.row().classes("w-full gap-4 flex-wrap"):
            scale_count_input = ui.select(
                {2: "2 scales: Major + Medium", 3: "3 scales: Major + Medium + Minor"},
                value=3,
                label="Structural scales",
            ).classes("w-72")

        with ui.row().classes("w-full gap-4 flex-wrap"):
            major_input = ui.number(
                "Major minimum B\u2192C / scan range",
                value=0.20,
                min=0.001,
                max=0.999,
                step=0.01,
            )
            major_radius_input = ui.number(
                "Major pivot radius (seconds)",
                value=60,
                min=1,
                max=3600,
                step=1,
            )

            medium_input = ui.number(
                "Medium minimum B\u2192C / scan range",
                value=0.10,
                min=0.001,
                max=0.999,
                step=0.01,
            )
            medium_radius_input = ui.number(
                "Medium pivot radius (seconds)",
                value=30,
                min=1,
                max=3600,
                step=1,
            )

        with ui.row().classes("w-full gap-4 flex-wrap"):
            minor_input = ui.number(
                "Minor minimum B\u2192C / scan range",
                value=0.05,
                min=0.001,
                max=0.999,
                step=0.01,
            )
            minor_radius_input = ui.number(
                "Minor pivot radius (seconds)",
                value=10,
                min=1,
                max=3600,
                step=1,
            )

        preset_status = ui.label("Loading enabled L2 presets\u2026").classes(
            "text-xs text-gray-500"
        )
        ui.label(
            "Detection always uses verified one-second L2. Thresholds "
            "are fractions of the whole-scan Total-L2 range; pivot "
            "radii define structural neighborhoods, not viewing bars. "
            "Two-scale mode excludes Minor."
        ).classes("text-xs text-gray-500")

        def _sync_minor_controls() -> None:
            enabled = _scale_count(scale_count_input.value) == 3
            for control in (minor_input, minor_radius_input):
                if enabled:
                    control.enable()
                else:
                    control.disable()

        scale_count_input.on_value_change(lambda _event: _sync_minor_controls())
        _sync_minor_controls()

        with ui.row().classes("gap-3"):
            run_button = ui.button(
                "Run Shock-Start",
                icon="play_arrow",
            )
            stop_button = ui.button(
                "Request Stop",
                icon="stop",
            )
            reload_presets_button = ui.button(
                "Reload presets",
                icon="refresh",
            )

        with ui.row().classes("gap-3"):
            export_png_button = (
                ui.button("Export Shock PNG", icon="image").props("outline").disable()
            )
            export_svg_button = (
                ui.button("Export Shock SVG", icon="draw").props("outline").disable()
            )
            ui.button(
                "Shock Color Legend",
                icon="palette",
                on_click=open_shock_color_legend,
            ).props("outline")

        status = ui.label("No Shock-Start review run in this session.")

        with ui.row().classes("gap-3 items-center"):
            page_input = ui.number(
                "Inspection page (30 areas)",
                value=1,
                min=1,
                step=1,
            )
            page_button = ui.button("Show page")

        count_label = ui.label(
            "Rows are inspection order, not calibrated probabilities."
        )

        table = (
            ui.table(
                columns=_columns(timezone_name),
                rows=[],
                row_key="id",
                pagination=30,
                selection="single",
            )
            .classes("w-full")
            .props("dense flat bordered")
        )

        chart = (
            ui.echart(
                {
                    "title": {"text": "Select a completed Shock-Start B area"},
                    "series": [],
                }
            )
            .classes("w-full h-[78vh] min-h-[680px]")
            .props("renderer=canvas")
        )
        controller = AnalysisChartController(
            chart,
            crosshair_gap_px=50,
        )

        ui.label(
            "Yellow band on all five panels: the selected L2 B candidate "
            "interval, not a price signal. Dashed yellow: representative B. "
            "Pink on Total: representative C. Bid blue, Ask orange, "
            "Total purple, Delta green. Null L2 slots remain gaps. "
            "Price is optional context; an empty Price panel does not "
            "invalidate the L2 result."
        ).classes("text-xs text-gray-500")

        ui.label(
            "Selected-area inspection stays at one-second resolution. "
            "Optionally switch this same chart to a bounded L2 viewing "
            "viewport around the selected B area. Viewing bars do not "
            "change detection. Switching this B area's bounded viewing "
            "timeframe preserves its visible UTC interval where possible."
        ).classes("text-xs text-gray-500")

        with ui.row().classes("w-full gap-3 flex-wrap items-end"):
            view_area_input = ui.number(
                "B area # for bounded viewport",
                value=1,
                min=1,
                step=1,
            )
            view_source_seconds_input = ui.number(
                "Viewport source seconds",
                value=3600,
                min=1,
                max=86400,
                step=1,
            )
            view_before_b_input = ui.number(
                "Seconds before B",
                value=1800,
                min=0,
                max=86399,
                step=1,
            )
            view_max_bars_input = ui.number(
                "Maximum viewing bars",
                value=1200,
                min=1,
                max=5000,
                step=1,
            )
            view_timeframe_input = ui.select(
                {
                    0: "Auto: finest bars within budget",
                    1: "1 second",
                    5: "5 seconds",
                    15: "15 seconds",
                    60: "1 minute",
                    300: "5 minutes",
                    900: "15 minutes",
                    3600: "1 hour",
                },
                value=0,
                label="L2 viewing bars",
            ).classes("w-72")
            view_button = (
                ui.button(
                    "Show bounded L2 viewport",
                    icon="candlestick_chart",
                )
                .props("outline")
                .disable()
            )

        ui.label(
            "Auto changes only viewing-bar width: it selects the finest "
            "timeframe fitting Maximum viewing bars. It never trims the "
            "requested source interval to meet that budget. An explicit "
            "timeframe that does not fit produces an error."
        ).classes("text-xs text-gray-500")

    async def _reload_presets(_event: Any = None) -> None:
        nonlocal preset_loading, enabled_preset_hashes

        if preset_loading or runtime.snapshot().is_running:
            return

        preset_loading = True
        reload_presets_button.disable()
        preset_status.text = "Loading enabled L2 presets…"

        try:
            loaded = await asyncio.to_thread(load_enabled_analysis_presets)

            # Read the base *after* the load: the user may have changed it
            # while the database query was running.
            selected_base = str(base_input.value or "").strip().upper()
            matching = tuple(
                option for option in loaded if option.base == selected_base
            )
            options = {option.preset_hash: option.label for option in matching}
            enabled_preset_hashes = set(options)

            previous = preset_input.value
            preset_input.options = options

            if previous in options:
                preset_input.value = previous
            elif matching:
                preset_input.value = matching[0].preset_hash
            else:
                preset_input.value = None

            preset_input.update()
            preset_status.text = f"{len(matching)} enabled {selected_base} preset(s)."

        except Exception:
            log.exception("Could not load Shock-Start enabled presets.")
            enabled_preset_hashes = set()
            preset_input.options = {}
            preset_input.value = None
            preset_input.update()
            preset_status.text = (
                "Could not load enabled presets; check the " "application log."
            )

        finally:
            preset_loading = False
            reload_presets_button.enable()

    def _clear_export_owner() -> None:
        nonlocal selected_owner_id, selected_position

        selected_owner_id = None
        selected_position = None
        export_png_button.disable()
        export_svg_button.disable()
        view_button.disable()

    def _show_page() -> None:
        if model is None:
            table.rows = []
            table.update()
            count_label.text = "Run a Shock-Start review first."
            return

        try:
            page_number = int(page_input.value)
            page = model.page(page_number)
        except (TypeError, ValueError) as exc:
            status.text = f"Cannot show page: {exc}"
            return

        table.rows = [_localized_row(row, timezone_name) for row in page.rows]
        table.update()
        count_label.text = (
            f"{page.total_areas} B areas; page {page.page}, "
            f"up to {page.page_size} rows. Click a row to inspect it."
        )

    def _clear_display() -> None:
        nonlocal model, selection_generation, bounded_owner_id
        model = None
        bounded_owner_id = None
        selection_generation += 1
        _clear_export_owner()
        controller.invalidate()
        table.selected.clear()
        table.rows = []
        table.update()
        create_tracked_task(
            controller.publish(
                {
                    "title": {"text": "Shock-Start review in progress"},
                    "series": [],
                },
                owner_id="shock-cleared",
                preserve_viewport=False,
            ),
            name="l2shock-shock-chart-clear",
        )
        count_label.text = "No completed review currently owns this chart."

    async def _run() -> None:
        nonlocal observed_completion

        if preset_loading:
            status.text = "Wait for enabled presets to finish loading."
            return

        selected_hash = str(preset_input.value or "").strip().lower()

        if selected_hash not in enabled_preset_hashes:
            status.text = "Select an enabled preset for the chosen base."
            return

        try:
            # User-facing endpoints are local wall-clock times. The request,
            # the detector, and the review identity remain strictly UTC.
            requested_start_utc = parse_local_analysis_datetime(
                start_date_input.value,
                start_time_input.value,
                timezone_name=timezone_name,
                field_name="Shock-Start start",
            )
            requested_end_utc = parse_local_analysis_datetime(
                end_date_input.value,
                end_time_input.value,
                timezone_name=timezone_name,
                field_name="Shock-Start end",
            )

            request, config = build_shock_request(
                base=base_input.value,
                preset_hash=preset_input.value,
                start_utc=requested_start_utc.isoformat(),
                end_utc=requested_end_utc.isoformat(),
                major_fraction=major_input.value,
                medium_fraction=medium_input.value,
                minor_fraction=minor_input.value,
                scale_count=scale_count_input.value,
                major_radius_seconds=major_radius_input.value,
                medium_radius_seconds=medium_radius_input.value,
                minor_radius_seconds=minor_radius_input.value,
            )
            task = runtime.start(
                request=request,
                config=config,
                evidence_config=ShockEvidenceConfig(),
            )
        except (ValueError, TypeError, RuntimeError) as exc:
            status.text = f"Shock-Start not started: {exc}"
            return

        # Observe exactly this new completion, not an earlier cached run.
        observed_completion = runtime.snapshot().completion_sequence
        _clear_display()
        status.text = (
            "Running verified one-second L2 scan. Stop is checked "
            "between synchronous stages, not inside the detector."
        )

        # Runtime tracks its own task. Retrieve an exception so the UI
        # timer can display snapshot.last_error without an unhandled-task
        # warning; the runtime has already logged its traceback.
        def _consume(done: asyncio.Task[object]) -> None:
            if not done.cancelled():
                done.exception()

        task.add_done_callback(_consume)

    def _stop() -> None:
        status.text = (
            "Stop requested; waiting for the current synchronous " "stage to return."
            if runtime.request_stop()
            else "No active Shock-Start review to stop."
        )

    async def _select_row(event: Any) -> None:
        nonlocal selection_generation
        nonlocal selected_owner_id, selected_position, bounded_owner_id

        row = _event_row(event)
        current_model = model

        if row is None or current_model is None:
            status.text = "No completed B-area row was selected."
            return

        try:
            position = int(row["inspection_position"])
        except KeyError, TypeError, ValueError:
            status.text = "Selected row has no inspection position."
            return

        if row.get("id") != (f"{current_model.review.review_id}:{position}"):
            status.text = "Selected row belongs to another review."
            return

        selection_generation += 1
        my_generation = selection_generation
        _clear_export_owner()

        try:
            selection = current_model.select(position)
            window = selection.window
            price_candles = None

            try:
                price_candles = await asyncio.to_thread(
                    load_shock_price_context,
                    base=(
                        current_model.review.evidence_result.candidate_scan.dataset.request.base
                    ),
                    source_start_utc=window.seconds[0].timestamp_utc,
                    source_end_utc_exclusive=(
                        window.seconds[-1].timestamp_utc + timedelta(seconds=1)
                    ),
                    bar_starts_utc=tuple(
                        second.timestamp_utc for second in window.seconds
                    ),
                    timeframe_seconds=1,
                )
            except Exception:
                # Missing or corrupt price, an unavailable DB, or an
                # unsupported price hour must never suppress verified L2.
                log.exception(
                    "Optional price context unavailable for B area #%s.",
                    position,
                )

            # The thread may finish after another selection or review.
            if (
                my_generation != selection_generation
                or model is not current_model
                or runtime.snapshot().last_review is not current_model.review
            ):
                return

            if price_candles is not None:
                try:
                    # The loader returns ECharts [open, close, low, high].
                    # The one-second builder accepts (open, high, low, close).
                    price_by_second = {
                        second.timestamp_utc: (
                            candle[0],
                            candle[3],
                            candle[2],
                            candle[1],
                        )
                        for second, candle in zip(window.seconds, price_candles)
                        if candle is not None
                    }
                    priced_selection = current_model.select(
                        position,
                        price_by_second=price_by_second,
                    )
                except Exception:
                    log.exception(
                        "Optional price candles could not be plotted "
                        "for B area #%s inspection.",
                        position,
                    )
                else:
                    selection = priced_selection

            status.text = f"Publishing B area #{position}"

            commit = await publish_shock_selection(
                controller,
                selection,
            )
        except Exception:
            log.exception("Could not publish selected Shock-Start area.")
            status.text = (
                f"Could not display B area #{position}. " "Check the application log."
            )
            return

        if (
            commit is None
            or my_generation != selection_generation
            or model is not current_model
            or runtime.snapshot().last_review is not current_model.review
            or commit.owner_id != selection.owner_id
        ):
            return

        bounded_owner_id = None
        selected_owner_id = selection.owner_id
        selected_position = position
        view_area_input.value = position
        table.selected[:] = [row]
        table.update()
        export_png_button.enable()
        export_svg_button.enable()
        view_button.enable()

        status.text = (
            f"Showing B area #{position}; "
            f"{len(selection.window.seconds)} one-second slots. "
            "Price is not required."
        )

    async def _show_bounded_view() -> None:
        nonlocal selection_generation
        nonlocal selected_owner_id, selected_position, bounded_owner_id

        current_model = model

        if (
            current_model is None
            or runtime.snapshot().last_review is not current_model.review
        ):
            status.text = "Complete a Shock-Start review before opening a viewport."
            return

        # Row selection writes its position into this control. If the user
        # subsequently enters another number, that explicit request wins.
        # build_shock_view_selection validates the resulting position before
        # any chart is published.
        position = view_area_input.value

        selection_generation += 1
        my_generation = selection_generation

        expected_owner = f"shock:{current_model.review.review_id}:{position}"
        previous_commit = controller.commit
        same_bounded_owner = (
            bounded_owner_id == expected_owner
            and previous_commit is not None
            and previous_commit.owner_id == expected_owner
        )

        _clear_export_owner()

        old_time_viewport = (
            await capture_shock_time_viewport(chart) if same_bounded_owner else None
        )

        if (
            my_generation != selection_generation
            or model is not current_model
            or runtime.snapshot().last_review is not current_model.review
        ):
            return

        # A pending replacement has no committed bounded-view owner.
        bounded_owner_id = None

        try:
            selected_timeframe = view_timeframe_input.value

            if selected_timeframe == 0:
                selected_timeframe = None

            requested_source_seconds = view_source_seconds_input.value
            requested_before_b = view_before_b_input.value
            requested_max_bars = view_max_bars_input.value

            viewport = build_shock_view_selection(
                current_model.review,
                position,
                source_seconds=requested_source_seconds,
                seconds_before_b=requested_before_b,
                timeframe_seconds=selected_timeframe,
                max_bars=requested_max_bars,
            )

            try:
                price_candles = await asyncio.to_thread(
                    load_shock_price_context,
                    base=(
                        current_model.review.evidence_result.candidate_scan.dataset.request.base
                    ),
                    source_start_utc=viewport.source_start_utc,
                    source_end_utc_exclusive=viewport.source_end_utc_exclusive,
                    bar_starts_utc=viewport.bar_starts_utc,
                    timeframe_seconds=viewport.timeframe_seconds,
                )
            except Exception:
                log.exception(
                    "Optional price context unavailable for B area #%s viewport.",
                    viewport.inspection_position,
                )
            else:
                try:
                    # Use the captured controls from the original L2 view.
                    # Price must not change its area, bounds, or timeframe.
                    priced_viewport = build_shock_view_selection(
                        current_model.review,
                        position,
                        source_seconds=requested_source_seconds,
                        seconds_before_b=requested_before_b,
                        timeframe_seconds=selected_timeframe,
                        max_bars=requested_max_bars,
                        price_candles=price_candles,
                    )
                except Exception:
                    log.exception(
                        "Optional price candles could not be plotted "
                        "for B area #%s viewport.",
                        viewport.inspection_position,
                    )
                else:
                    viewport = priced_viewport

            if (
                my_generation != selection_generation
                or model is not current_model
                or runtime.snapshot().last_review is not current_model.review
            ):
                return

            status.text = (
                f"Publishing B area #{viewport.inspection_position} "
                "L2 viewing viewport"
            )

            commit = await controller.publish(
                viewport.option,
                owner_id=viewport.owner_id,
                preserve_viewport=False,
                shock_time_viewport=old_time_viewport,
            )
        except Exception as exc:
            log.exception("Could not publish bounded Shock-Start viewport.")
            if (
                my_generation == selection_generation
                and model is current_model
                and runtime.snapshot().last_review is current_model.review
            ):
                view_button.enable()
                status.text = f"Could not display L2 viewport: {exc}"
            return

        if (
            my_generation != selection_generation
            or model is not current_model
            or runtime.snapshot().last_review is not current_model.review
        ):
            return

        if commit is None or commit.owner_id != viewport.owner_id:
            view_button.enable()
            status.text = (
                "The browser did not acknowledge the requested "
                "viewport. Try again or check the application log."
            )
            return

        bounded_owner_id = viewport.owner_id
        selected_owner_id = viewport.owner_id
        selected_position = viewport.inspection_position
        view_area_input.value = viewport.inspection_position

        visible_row = next(
            (
                row
                for row in table.rows
                if row.get("id")
                == (f"{viewport.review_id}:" f"{viewport.inspection_position}")
            ),
            None,
        )

        table.selected[:] = [visible_row] if visible_row is not None else []
        table.update()

        export_png_button.enable()
        export_svg_button.enable()
        view_button.enable()

        status.text = (
            f"Showing B area #{viewport.inspection_position}: "
            f"all {viewport.source_seconds} requested one-second "
            f"slots as {viewport.displayed_bars} "
            f"{viewport.timeframe_seconds}s viewing bars "
            f"(maximum {requested_max_bars}). "
            "Previous bounded-view UTC zoom was requested when available; "
            "Price remains optional."
        )

    def _poll() -> None:
        nonlocal observed_completion, model

        snapshot = runtime.snapshot()

        if snapshot.completion_sequence == observed_completion:
            return

        observed_completion = snapshot.completion_sequence

        if snapshot.phase is ShockRuntimePhase.COMPLETED:
            review = snapshot.last_review

            if review is None:
                status.text = "Review completed without a result."
                return

            _clear_display()
            model = ShockInspectionModel(review)
            page_input.value = 1
            view_area_input.value = 1
            _show_page()

            area_count = len(review.ordered_areas)

            if area_count:
                view_button.enable()
                status.text = (
                    f"Review complete: {area_count} B areas. "
                    "Opening the bounded L2 viewport for area #1…"
                )

                # _poll runs in the UI event loop. Publishing is async
                # because browser acknowledgement must be awaited.
                # _show_bounded_view uses the same generation/review
                # guards as a manually requested viewport.
                asyncio.create_task(_show_bounded_view())
            else:
                view_button.disable()
                status.text = "Review complete: no B areas to inspect."
        elif snapshot.phase is ShockRuntimePhase.STOPPED:
            _clear_display()
            status.text = "Shock-Start review stopped; no partial result."
        elif snapshot.phase is ShockRuntimePhase.FAILED:
            _clear_display()
            status.text = f"Shock-Start review failed: {snapshot.last_error}"

    async def _export_image(image_format: str) -> None:
        current_model = model
        owner = selected_owner_id
        position = selected_position

        if (
            current_model is None
            or owner is None
            or position is None
            or runtime.snapshot().last_review is not current_model.review
        ):
            status.text = "Select an acknowledged B-area chart before exporting."
            return

        expected_owner = f"shock:{current_model.review.review_id}:{position}"
        commit = controller.commit

        if (
            owner != expected_owner
            or commit is None
            or commit.owner_id != expected_owner
        ):
            status.text = "The selected B area no longer owns the chart."
            _clear_export_owner()
            return

        filename = (
            f"l2shock_shock_"
            f"{current_model.review.review_id[:16]}_"
            f"area_{position:05d}.{image_format}"
        )

        try:
            exported, reason = await controller.export_image(
                owner_id=owner,
                image_format=image_format,
                filename=filename,
            )
        except Exception:
            log.exception(
                "Shock-Start %s chart export failed.",
                image_format,
            )
            status.text = "Chart export failed; check the application log."
            return

        # Selection may have changed while browser-side export ran.
        if (
            owner != selected_owner_id
            or position != selected_position
            or model is not current_model
        ):
            return

        status.text = (
            f"Exported {filename}"
            if exported
            else f"Chart export unavailable: {reason}"
        )

    async def _export_png() -> None:
        await _export_image("png")

    async def _export_svg() -> None:
        await _export_image("svg")

    run_button.on_click(_run)
    stop_button.on_click(_stop)
    reload_presets_button.on_click(_reload_presets)
    base_input.on_value_change(_reload_presets)
    page_button.on_click(_show_page)
    export_png_button.on_click(_export_png)
    export_svg_button.on_click(_export_svg)
    view_button.on_click(_show_bounded_view)
    table.on("rowClick", _select_row)

    ui.timer(
        interval=0.25,
        callback=_poll,
        active=True,
    )
    ui.timer(
        interval=0.1,
        callback=_reload_presets,
        once=True,
    )


__all__ = [
    "build_shock_request",
    "build_shock_review_section",
]

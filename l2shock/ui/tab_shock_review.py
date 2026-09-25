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
from l2shock.analysis.shock_evidence import ShockEvidenceConfig
from l2shock.analysis.shock_start import (
    ShockStartConfig,
    ShockStructuralScale,
)
from l2shock.ui.chart_interactions import AnalysisChartController
from l2shock.ui.components import create_tracked_task
from l2shock.ui.shock_inspection import (
    ShockInspectionModel,
    publish_shock_selection,
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
        raise ValueError(
            f"{name} must be an ISO-8601 UTC timestamp"
        ) from exc

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


def build_shock_request(
    *,
    base: object,
    preset_hash: object,
    start_utc: object,
    end_utc: object,
    major_fraction: object,
    medium_fraction: object,
    minor_fraction: object,
) -> tuple[ShockDatasetRequest, ShockStartConfig]:
    """Validate UI input before acquiring the shared operation slot."""
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

    major = _scale_fraction(major_fraction, "Major threshold")
    medium = _scale_fraction(medium_fraction, "Medium threshold")
    minor = _scale_fraction(minor_fraction, "Minor threshold")

    config = ShockStartConfig(
        scales=(
            ShockStructuralScale("major", major, 60),
            ShockStructuralScale("medium", medium, 30),
            ShockStructuralScale("minor", minor, 10),
        )
    )

    request = ShockDatasetRequest(
        base=selected_base,
        preset_hash=selected_hash,
        requested_start_utc=start,
        requested_end_utc=end,
    )

    return request, config


def _event_row(event: Any) -> dict[str, Any] | None:
    args = getattr(event, "args", None)

    if isinstance(args, dict):
        row = args.get("row")
        return row if isinstance(row, dict) else None

    if isinstance(args, (list, tuple)):
        for item in args:
            if isinstance(item, dict):
                row = item.get("row")
                if isinstance(row, dict):
                    return row

    return None


def _columns() -> list[dict[str, object]]:
    fields = (
        ("inspection_position", "Inspect #"),
        ("direction", "Direction"),
        ("first_b_utc", "B-area first UTC"),
        ("representative_b_utc", "Representative B UTC"),
        ("representative_c_utc", "C UTC"),
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

    initial_snapshot = runtime.snapshot()
    observed_completion = initial_snapshot.completion_sequence
    model: ShockInspectionModel | None = None
    selection_generation = 0

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start_default = (now - timedelta(hours=1)).isoformat()
    end_default = now.isoformat()

    with ui.card().classes("w-full"):
        ui.label("Shock-Start — verified Total L2").classes(
            "text-xl font-semibold"
        )
        ui.label(
            "Separate from legacy LM during migration. "
            "Detection uses verified one-second L2; price is optional "
            "chart context and is not loaded by this section."
        ).classes("text-sm text-gray-500")

        with ui.row().classes("w-full gap-4 flex-wrap"):
            base_input = ui.select(
                {"BTC": "BTC", "ETH": "ETH"},
                value="BTC",
                label="Base",
            )
            preset_input = ui.input(
                "Enabled L2 component preset hash (64 hex characters)"
            ).classes("min-w-[420px]")
            start_input = ui.input(
                "Start UTC (ISO-8601)",
                value=start_default,
            )
            end_input = ui.input(
                "End UTC (ISO-8601)",
                value=end_default,
            )

        with ui.row().classes("w-full gap-4 flex-wrap"):
            major_input = ui.number(
                "Major leg fraction (60s pivot)",
                value=0.20,
                min=0.001,
                max=0.999,
                step=0.01,
            )
            medium_input = ui.number(
                "Medium leg fraction (30s pivot)",
                value=0.10,
                min=0.001,
                max=0.999,
                step=0.01,
            )
            minor_input = ui.number(
                "Minor leg fraction (10s pivot)",
                value=0.05,
                min=0.001,
                max=0.999,
                step=0.01,
            )

        ui.label(
            "Thresholds are fractions of the scanned Total-L2 range "
            "(20% / 10% / 5% by default), not chart timeframes."
        ).classes("text-xs text-gray-500")

        with ui.row().classes("gap-3"):
            run_button = ui.button(
                "Run Shock-Start",
                icon="play_arrow",
            )
            stop_button = ui.button(
                "Request Stop",
                icon="stop",
            )

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

        table = ui.table(
            columns=_columns(),
            rows=[],
            row_key="id",
            pagination=30,
            selection="single",
        ).classes("w-full").props("dense flat bordered")

        chart = (
            ui.echart({
                "title": {"text": "Select a completed Shock-Start B area"},
                "series": [],
            })
            .classes("w-full h-[78vh] min-h-[680px]")
            .props("renderer=canvas")
        )
        controller = AnalysisChartController(
            chart,
            crosshair_gap_px=50,
        )

        ui.label(
            "Yellow band: B candidate area. Dashed yellow: representative B. "
            "Pink on Total: representative C. Bid blue, Ask orange, "
            "Total purple, Delta green. Null L2 slots remain gaps. "
            "The Price panel is optional context and is empty here."
        ).classes("text-xs text-gray-500")

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

        table.rows = list(page.rows)
        table.update()
        count_label.text = (
            f"{page.total_areas} B areas; page {page.page}, "
            f"up to {page.page_size} rows. Click a row to inspect it."
        )

    def _clear_display() -> None:
        nonlocal model, selection_generation
        model = None
        selection_generation += 1
        controller.invalidate()
        table.rows = []
        table.update()
        chart.options = {
            "title": {"text": "Shock-Start review in progress"},
            "series": [],
        }
        chart.update()
        count_label.text = "No completed review currently owns this chart."

    async def _run() -> None:
        nonlocal observed_completion

        try:
            request, config = build_shock_request(
                base=base_input.value,
                preset_hash=preset_input.value,
                start_utc=start_input.value,
                end_utc=end_input.value,
                major_fraction=major_input.value,
                medium_fraction=medium_input.value,
                minor_fraction=minor_input.value,
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
            "Stop requested; waiting for the current synchronous "
            "stage to return."
            if runtime.request_stop()
            else "No active Shock-Start review to stop."
        )

    async def _select_row(event: Any) -> None:
        nonlocal selection_generation

        row = _event_row(event)
        current_model = model

        if row is None or current_model is None:
            status.text = "No completed B-area row was selected."
            return

        try:
            position = int(row["inspection_position"])
        except (KeyError, TypeError, ValueError):
            status.text = "Selected row has no inspection position."
            return

        if row.get("id") != (
            f"{current_model.review.review_id}:{position}"
        ):
            status.text = "Selected row belongs to another review."
            return

        selection_generation += 1
        my_generation = selection_generation

        try:
            selection = current_model.select(position)
            status.text = f"Publishing B area #{position}…"

            commit = await publish_shock_selection(
                controller,
                selection,
            )
        except Exception:
            log.exception("Could not publish selected Shock-Start area.")
            status.text = (
                f"Could not display B area #{position}. "
                "Check the application log."
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

        status.text = (
            f"Showing B area #{position}; "
            f"{len(selection.window.seconds)} one-second slots. "
            "Price is not required."
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
            _show_page()
            status.text = (
                f"Review complete: {len(review.ordered_areas)} B areas. "
                "Choose a table row for a one-second chart."
            )
        elif snapshot.phase is ShockRuntimePhase.STOPPED:
            _clear_display()
            status.text = "Shock-Start review stopped; no partial result."
        elif snapshot.phase is ShockRuntimePhase.FAILED:
            _clear_display()
            status.text = (
                f"Shock-Start review failed: {snapshot.last_error}"
            )

    run_button.on_click(_run)
    stop_button.on_click(_stop)
    page_button.on_click(_show_page)
    table.on("rowClick", _select_row)
    ui.timer(interval=0.25, callback=_poll, active=True)


__all__ = [
    "build_shock_request",
    "build_shock_review_section",
]
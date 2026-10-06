from __future__ import annotations

import asyncio
import csv
import io
import json
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from types import SimpleNamespace

import pytest

from l2shock.analysis.l2_view_metrics import (
    L2_VIEW_METRIC_SPECS,
    L2ViewMetric,
)
from l2shock.analysis.l2_view_stream import (
    L2ViewBar,
    L2ViewCancelledError,
    L2ViewLoadOptions,
    L2ViewProjection,
    L2ViewRequest,
)
from l2shock.ui import shutdown as shutdown_module
from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options
from l2shock.ui.l2_view_presentation import (
    cached_view,
    displayed_csv_bytes,
    displayed_json_bytes,
    fit_closed_handoff,
    whole_number,
    with_wheel_policy,
)
from l2shock.ui.l2_view_runtime import (
    L2ViewRuntime,
    L2ViewRuntimePhase,
)
from l2shock.ui.state import get_state, reset_state_for_tests

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _request(seconds: int = 3) -> L2ViewRequest:
    return L2ViewRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_T0,
        requested_end_utc=_T0 + timedelta(seconds=seconds - 1),
    )


def _projection() -> L2ViewProjection:
    def candle(value: Fraction):
        return (value, value, value, value)

    bars = tuple(
        L2ViewBar(
            start_utc=_T0 + timedelta(seconds=index),
            source_seconds=1,
            valid_l2=True,
            bid=candle(Fraction(2 + index)),
            ask=candle(Fraction(1)),
            total=candle(Fraction(3 + index)),
            delta=candle(Fraction(1 + index)),
            bid_share_pct=candle(Fraction(100 * (2 + index), 3 + index)),
            price=None,
        )
        for index in range(3)
    )
    return L2ViewProjection(
        request=_request(),
        start_utc=_T0,
        end_utc_exclusive=_T0 + timedelta(seconds=3),
        timeframe_seconds=1,
        max_bars=1200,
        bars=bars,
        l2_regions=(),
        price_regions=(),
        l2_regions_truncated=False,
        price_regions_truncated=False,
        price_status="missing",
        usable_l2_seconds=3,
        unusable_l2_seconds=0,
        partial_market_seconds=0,
        input_id="b" * 64,
    )


def _option(projection: L2ViewProjection):
    return build_l2_view_chart_options(
        projection,
        panel_a_metric=L2ViewMetric.IMBALANCE_PCT,
        panel_b_metric=L2ViewMetric.DELTA,
    )


@pytest.mark.parametrize("value", [True, False, 1.5, "1.5", "", None])
def test_whole_number_rejects_non_integer_inputs(value) -> None:
    with pytest.raises(ValueError):
        whole_number(value, "Budget", minimum=1, maximum=5000)


@pytest.mark.parametrize("value", [1200, 1200.0, "1200", " 1200 "])
def test_whole_number_accepts_browser_integer_values(value) -> None:
    assert whole_number(value, "Budget", minimum=1, maximum=5000) == 1200


def test_duration_owns_closed_endpoint_seconds() -> None:
    assert _request(86400).slot_count == 86400
    with pytest.raises(ValueError):
        _request(86401)


def test_handoff_clipping_retains_newest_owned_seconds() -> None:
    end = _T0 + timedelta(seconds=86400)
    start, closed_end, clipped = fit_closed_handoff(
        _T0, end, max_duration_seconds=86400
    )
    assert clipped is True
    assert start == _T0 + timedelta(seconds=1)
    assert closed_end == end
    assert int((closed_end - start).total_seconds()) + 1 == 86400


def test_handoff_does_not_clip_exact_limit() -> None:
    end = _T0 + timedelta(seconds=86399)
    assert fit_closed_handoff(_T0, end, max_duration_seconds=86400) == (_T0, end, False)


def test_cache_coarsens_without_mutating_finest_projection() -> None:
    projection = _projection()
    target, coarse = cached_view(projection, timeframe_seconds=5, max_bars=100)
    assert target == 5
    assert coarse is not None
    assert len(coarse.bars) == 1
    assert coarse.max_bars == 100
    assert projection.timeframe_seconds == 1
    assert len(projection.bars) == 3

    target, fine = cached_view(coarse, timeframe_seconds=1, max_bars=100)
    assert target == 1
    assert fine is None


def test_unchanged_timeframe_updates_bar_budget() -> None:
    projection = _projection()
    _, same = cached_view(projection, timeframe_seconds=1, max_bars=10)
    assert same is not None
    assert same.max_bars == 10
    assert projection.max_bars == 1200


def test_percentage_coarsening_does_not_skip_undefined_subbar() -> None:
    projection = _projection()
    bars = list(projection.bars)
    bars[1] = replace(bars[1], bid_share_pct=None)
    _, coarse = cached_view(
        replace(projection, bars=tuple(bars)),
        timeframe_seconds=5,
        max_bars=100,
    )

    assert coarse is not None
    assert coarse.bars[0].valid_l2 is True
    assert coarse.bars[0].bid is not None
    assert coarse.bars[0].ask is not None

    # Undefined percentages do not suppress the numerical liquidity
    # channels, but cached coarsening cannot fabricate a defined ratio.
    assert coarse.bars[0].bid_share_pct is None


def test_five_panels_and_all_registry_metrics_build() -> None:
    projection = _projection()
    assert len(L2_VIEW_METRIC_SPECS) == 11
    for metric in L2_VIEW_METRIC_SPECS:
        option = build_l2_view_chart_options(
            projection,
            panel_a_metric=metric,
            panel_b_metric=L2ViewMetric.DELTA,
        )
        assert len(option["grid"]) == 5
        assert len(option["xAxis"]) == 5
        assert len(option["yAxis"]) == 5
        assert len(option["dataZoom"]) == 6
        assert option["l2shockChartMetadata"]["panel_a_metric"] == metric.value


def test_wheel_policy_is_isolated_and_resets_only_changed_panel() -> None:
    option = _option(_projection())
    original = json.dumps(option, sort_keys=True)
    preserved = {panel: (10.0, 90.0) for panel in range(5)}
    shown = with_wheel_policy(
        option,
        preserved_y=preserved,
        reset_panels=frozenset({3}),
    )
    assert shown["dataZoom"][0]["zoomOnMouseWheel"] is True
    for panel in range(5):
        zoom = shown["dataZoom"][panel + 1]
        assert zoom["zoomOnMouseWheel"] is False
        expected = (0.0, 100.0) if panel == 3 else (10.0, 90.0)
        assert (zoom["start"], zoom["end"]) == expected
    assert json.dumps(option, sort_keys=True) == original


def test_json_export_has_exact_values_and_five_panels() -> None:
    projection = _projection()
    payload = json.loads(
        displayed_json_bytes(
            projection,
            _option(projection),
            panel_a_metric=L2ViewMetric.IMBALANCE_PCT.value,
            panel_b_metric=L2ViewMetric.DELTA.value,
        )
    )
    assert payload["analysis_id"] == projection.input_id
    assert len(payload["panels"]) == 5
    assert payload["bars"][0]["bid"] == ["2/1"] * 4
    assert payload["bars"][0]["price"] is None
    assert payload["exact_ohlc_order"] == ["open", "high", "low", "close"]
    for retired in ("hypotheses", "b_areas", "review_id", "rankings"):
        assert retired not in payload


def test_csv_preserves_null_price_and_candle_order() -> None:
    projection = _projection()
    option = _option(projection)
    option["series"][1]["data"][0] = [_T0.isoformat(), 2.0, 3.0, 1.0, 4.0]
    rows = list(
        csv.DictReader(
            io.StringIO(displayed_csv_bytes(projection, option).decode("utf-8-sig"))
        )
    )
    price = next(row for row in rows if row["series_id"] == "l2view-price")
    assert price["open"] == ""
    assert price["close"] == ""
    bid = next(row for row in rows if row["series_id"] == "l2view-bid")
    assert (bid["open"], bid["high"], bid["low"], bid["close"]) == (
        "2.0",
        "4.0",
        "1.0",
        "3.0",
    )


@pytest.mark.asyncio
async def test_runtime_completion_has_operation_identity() -> None:
    reset_state_for_tests()
    projection = _projection()

    def loader(*_args, **_kwargs):
        return projection

    runtime = L2ViewRuntime(loader=loader)
    task = runtime.start(_request(), L2ViewLoadOptions())
    before = runtime.snapshot()
    assert before.is_running is True
    assert before.operation_id
    assert before.stop_requested is False
    assert await task is projection

    after = runtime.snapshot()
    assert after.phase is L2ViewRuntimePhase.COMPLETED
    assert after.operation_id == before.operation_id
    assert after.last_projection is projection
    assert after.is_running is False
    assert get_state().active_operation_name == ""
    assert not get_state().operation_lock.locked()


@pytest.mark.asyncio
async def test_stop_keeps_last_successful_projection() -> None:
    reset_state_for_tests()
    projection = _projection()
    entered = threading.Event()

    def loader(_request, _options, *, cancellation_probe, progress_sink):
        entered.set()
        while not cancellation_probe():
            threading.Event().wait(0.005)
        raise L2ViewCancelledError("Stopped by test")

    runtime = L2ViewRuntime(loader=loader)
    runtime._last = projection
    task = runtime.start(_request(), L2ViewLoadOptions())
    try:
        assert await asyncio.to_thread(entered.wait, 2.0)
        assert runtime.request_stop() is True
        assert runtime.snapshot().stop_requested is True
        assert await asyncio.wait_for(task, 3.0) is None
    finally:
        runtime.request_stop()
        if not task.done():
            await asyncio.wait_for(task, 3.0)

    snapshot = runtime.snapshot()
    assert snapshot.phase is L2ViewRuntimePhase.STOPPED
    assert snapshot.last_projection is projection
    assert not get_state().operation_lock.locked()


@pytest.mark.asyncio
async def test_analysis_worker_timeout_blocks_engine_disposal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_state_for_tests()
    events = []

    for name in (
        "peek_automatic_fetch_runtime",
        "peek_manual_fetch_runtime",
        "peek_remote_import_runtime",
        "peek_manual_processing_runtime",
    ):
        monkeypatch.setattr(shutdown_module, name, lambda: None)

    class Runtime:
        def snapshot(self):
            return SimpleNamespace(is_running=True)

        async def stop_and_wait(self, *, grace_seconds):
            events.append(("analysis_stop", grace_seconds))
            return False

    async def cancel_tasks(*, timeout_seconds):
        return 0

    async def join_readers(*, timeout_seconds):
        return 0

    monkeypatch.setattr(shutdown_module, "peek_l2_view_runtime", lambda: Runtime())
    monkeypatch.setattr(
        shutdown_module, "cancel_and_wait_for_tracked_tasks", cancel_tasks
    )
    monkeypatch.setattr(shutdown_module, "wait_for_untracked_db_workers", join_readers)
    monkeypatch.setattr(
        shutdown_module, "reset_engine", lambda: events.append("disposed")
    )

    with pytest.raises(RuntimeError, match="background work"):
        await shutdown_module.shutdown_runtime(
            request_server_stop=False,
            analysis_grace_seconds=0.01,
        )

    assert events == [("analysis_stop", 0.01)]
    assert get_state().shutdown_started is True
    assert get_state().shutdown_complete is False


def test_analysis_publication_explains_empty_l2_viewing_bars():
    import ast
    import inspect

    from l2shock.analysis import l2_view_stream
    from l2shock.ui import tab_l2_view

    source = inspect.getsource(tab_l2_view)
    tree = ast.parse(source)

    # Keep the publication counters and warning admission conditions
    # covered by this retained source-contract test.
    assert "valid_l2_bars = sum(" in source
    assert "renderable_l2_bars=%d" in source
    assert "unusable_l2_seconds=%d" in source
    assert "partial_market_seconds=%d" in source
    assert "if new_source and (" in source
    assert "valid_l2_bars == 0" in source
    assert "projection.unusable_l2_seconds > 0" in source
    assert "projection.partial_market_seconds > 0" in source

    # The UI selects a title first, then passes title=notify_title.
    # Do not require a literal title keyword that the implementation
    # deliberately does not use.
    assigned_titles = {
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "notify_title"
            for target in node.targets
        )
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    }

    assert "No numerical L2 data for selected preset" in assigned_titles
    assert "L2 data-quality warning" in assigned_titles
    assert "L2 data-quality warning" in assigned_titles

    title_variable_is_used = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "persistent_notify"
        and any(
            keyword.arg == "title"
            and isinstance(keyword.value, ast.Name)
            and keyword.value.id == "notify_title"
            for keyword in node.keywords
        )
        for node in ast.walk(tree)
    )
    assert title_variable_is_used

    # Python joins adjacent ordinary string literals in the AST.
    # These checks therefore survive harmless source-line wrapping.
    string_values = tuple(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    )

    ui_required_messages = (
        "Analysis rendered the available verified L2 data.",
        "Cross-check price and L2 with trdr.io before trading.",
    )
    for message in ui_required_messages:
        assert any(message in value for value in string_values), message
    retired_message = "Optional price loading failed. L2 remains displayed;"
    assert not any(retired_message in value for value in string_values)

    # Load-summary diagnostics belong to the loader, not the UI module.
    loader_tree = ast.parse(inspect.getsource(l2_view_stream))
    loader_string_values = tuple(
        node.value
        for node in ast.walk(loader_tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    )
    loader_required_messages = (
        "ANALYSIS L2 COMPONENT MISSING",
        "ANALYSIS L2 COMPONENT COVERAGE",
        "ANALYSIS L2 LOAD SUMMARY",
    )
    for message in loader_required_messages:
        assert any(message in value for value in loader_string_values), message


# Analysis successful-result ownership and timeframe regressions.

import ast as _completion_ast
import copy as _completion_copy
import logging as _completion_logging
from pathlib import Path as _CompletionPath

from l2shock.ui.l2_view_runtime import (
    L2ViewRuntimeSnapshot as _CompletionSnapshot,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    ("failed", "stopped", "prestart_cancelled"),
)
async def test_analysis_success_metadata_survives_later_unsuccessful_load(
    outcome: str,
) -> None:
    reset_state_for_tests()
    projection = _projection()
    calls = 0

    def loader(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return projection
        if outcome == "stopped":
            raise L2ViewCancelledError("Stopped by ownership regression")
        raise ValueError("Failed by ownership regression")

    runtime = L2ViewRuntime(loader=loader)
    first = runtime.start(
        _request(),
        L2ViewLoadOptions(timeframe_seconds=None),
    )
    first_operation_id = runtime.snapshot().operation_id

    assert await first is projection
    await asyncio.sleep(0)

    successful = runtime.snapshot()
    assert successful.last_projection_operation_id == first_operation_id
    assert successful.last_projection_timeframe_setting == 0

    second = runtime.start(
        _request(),
        L2ViewLoadOptions(timeframe_seconds=5),
    )
    running = runtime.snapshot()

    assert running.is_running
    assert running.operation_id != first_operation_id
    assert running.last_projection is projection
    assert running.last_projection_operation_id == first_operation_id
    assert running.last_projection_timeframe_setting == 0

    if outcome == "prestart_cancelled":
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
    elif outcome == "stopped":
        assert await second is None
    else:
        with pytest.raises(ValueError, match="ownership regression"):
            await second

    await asyncio.sleep(0)
    after = runtime.snapshot()

    assert after.last_projection is projection
    assert after.last_projection_operation_id == first_operation_id
    assert after.last_projection_timeframe_setting == 0
    assert after.completion_sequence == 2
    assert not after.is_running
    assert get_state().active_operation_name == ""
    assert not get_state().operation_lock.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeframe_setting", (None, 1, 5))
async def test_analysis_success_records_requested_not_effective_timeframe(
    timeframe_setting: int | None,
) -> None:
    reset_state_for_tests()
    projection = _projection()

    def loader(*_args, **_kwargs):
        return projection

    runtime = L2ViewRuntime(loader=loader)
    task = runtime.start(
        _request(),
        L2ViewLoadOptions(timeframe_seconds=timeframe_setting),
    )
    operation_id = runtime.snapshot().operation_id

    assert await task is projection
    await asyncio.sleep(0)

    snapshot = runtime.snapshot()
    assert snapshot.last_projection_operation_id == operation_id
    assert snapshot.last_projection_timeframe_setting == (
        0 if timeframe_setting is None else timeframe_setting
    )


class _CompletionControl:
    def __init__(self, value=None) -> None:
        self.value = value
        self.updates = 0
        self.text = ""
        self.messages: list[str] = []

    def update(self) -> None:
        self.updates += 1

    def set_text(self, value: str) -> None:
        self.text = value
        self.messages.append(value)


class _CompletionRuntimeDouble:
    def __init__(self, snapshot: _CompletionSnapshot) -> None:
        self.current = snapshot

    def snapshot(self) -> _CompletionSnapshot:
        return self.current


class _CompletionControllerDouble:
    def __init__(self) -> None:
        self.commit = SimpleNamespace(
            publication=SimpleNamespace(render_token="a" * 32),
        )
        self.calls: list[dict] = []
        self.fail = False

    async def publish(self, _option, **kwargs):
        self.calls.append(kwargs)

        if self.fail:
            self.commit = None
            raise RuntimeError("Simulated publication failure")

        self.commit = SimpleNamespace(
            owner_id=kwargs["owner_id"],
            publication=SimpleNamespace(render_token="b" * 32),
        )
        return self.commit


def _completion_snapshot(
    *,
    projection,
    operation_id: str,
    successful_operation_id: str | None,
    timeframe_setting: int | None,
    phase=L2ViewRuntimePhase.COMPLETED,
    running: bool = False,
    sequence: int = 1,
) -> _CompletionSnapshot:
    return _CompletionSnapshot(
        phase=phase,
        is_running=running,
        completion_sequence=sequence,
        hours_done=1,
        hours_total=1,
        last_projection=projection,
        last_error=(
            "Simulated load failure" if phase is L2ViewRuntimePhase.FAILED else None
        ),
        operation_id=operation_id,
        stop_requested=False,
        last_projection_operation_id=successful_operation_id,
        last_projection_timeframe_setting=timeframe_setting,
    )


def _completion_ui_harness(
    snapshot: _CompletionSnapshot,
    *,
    initial_projection=None,
    initial_setting: int = 0,
    initial_successful_operation_id: str | None = None,
):
    """Execute exact production handlers without building a browser page."""
    source_path = (
        _CompletionPath(__file__).resolve().parents[1]
        / "l2shock"
        / "ui"
        / "tab_l2_view.py"
    )
    tree = _completion_ast.parse(
        source_path.read_text(encoding="utf-8"),
        filename=str(source_path),
    )
    builders = [
        node
        for node in tree.body
        if isinstance(node, _completion_ast.FunctionDef)
        and node.name == "build_l2_view_section"
    ]
    assert len(builders) == 1

    wanted = {
        "_publish",
        "_poll",
        "_change_view",
        "_change_presentation",
    }
    selected = [
        _completion_copy.deepcopy(node)
        for node in builders[0].body
        if isinstance(
            node,
            (_completion_ast.FunctionDef, _completion_ast.AsyncFunctionDef),
        )
        and node.name in wanted
    ]
    assert {node.name for node in selected} == wanted

    wrapper = _completion_ast.parse(
        "def _completion_factory("
        "initial_projection, initial_setting, initial_successful_operation_id"
        "):\n"
        "    loaded = initial_projection\n"
        "    displayed = initial_projection\n"
        "    displayed_option = {}\n"
        "    displayed_a = 'imbalance_pct'\n"
        "    displayed_b = 'delta'\n"
        "    displayed_timeframe_setting = initial_setting\n"
        "    observed_completion = 0\n"
        "    observed_successful_operation_id = "
        "initial_successful_operation_id\n"
        "    pending_operation_id = None\n"
        "    pending_viewport = None\n"
        "    preset_loading = False\n"
        "    publishing = False\n"
        "    render_lock = asyncio.Lock()\n"
        "    def read_state():\n"
        "        return {\n"
        "            'loaded': loaded,\n"
        "            'displayed': displayed,\n"
        "            'displayed_timeframe_setting': "
        "displayed_timeframe_setting,\n"
        "            'observed_completion': observed_completion,\n"
        "            'observed_successful_operation_id': "
        "observed_successful_operation_id,\n"
        "            'pending_operation_id': pending_operation_id,\n"
        "            'pending_viewport': pending_viewport,\n"
        "            'render_lock': render_lock,\n"
        "        }\n"
        "    def set_pending(operation_id, viewport):\n"
        "        nonlocal pending_operation_id, pending_viewport\n"
        "        pending_operation_id = operation_id\n"
        "        pending_viewport = viewport\n"
    )
    factory = wrapper.body[0]
    assert isinstance(factory, _completion_ast.FunctionDef)
    factory.body.extend(selected)
    factory.body.extend(
        _completion_ast.parse(
            "return {\n"
            "    'poll': _poll,\n"
            "    'publish': _publish,\n"
            "    'change_view': _change_view,\n"
            "    'change_presentation': _change_presentation,\n"
            "    'read_state': read_state,\n"
            "    'set_pending': set_pending,\n"
            "}\n"
        ).body
    )
    _completion_ast.fix_missing_locations(wrapper)

    runtime = _CompletionRuntimeDouble(snapshot)
    controller = _CompletionControllerDouble()
    timeframe = _CompletionControl(initial_setting)
    status = _CompletionControl()
    progress = _CompletionControl()

    async def capture_x(_chart):
        return None

    async def capture_y(_chart, **_kwargs):
        return {}

    async def install_y(_chart, **_kwargs):
        return True

    def options():
        return L2ViewLoadOptions(
            timeframe_seconds=(None if timeframe.value == 0 else timeframe.value),
        )

    def unexpected_admission(*_args, **_kwargs):
        raise AssertionError("This test did not expect a reload admission")

    namespace = {
        "asyncio": asyncio,
        "Any": object,
        "L2ViewProjection": L2ViewProjection,
        "AnalysisChartTemporalViewport": object,
        "L2ViewRuntimePhase": L2ViewRuntimePhase,
        "L2ViewMetric": L2ViewMetric,
        "runtime": runtime,
        "state": SimpleNamespace(shutdown_started=False),
        "controller": controller,
        "chart": object(),
        "panel_a": _CompletionControl("imbalance_pct"),
        "panel_b": _CompletionControl("delta"),
        "warnings_switch": _CompletionControl(True),
        "timeframe_input": timeframe,
        "status": status,
        "progress": progress,
        "timezone_name": "UTC",
        "_sync_controls": lambda: None,
        "_options": options,
        "_admit": unexpected_admission,
        "cached_view": cached_view,
        "capture_shock_time_viewport": capture_x,
        "capture_y_viewports": capture_y,
        "install_y_wheel": install_y,
        "build_l2_view_chart_options": lambda *_args, **_kwargs: {},
        "with_wheel_policy": lambda option, **_kwargs: option,
        "with_display_timezone": lambda option, _timezone: option,
        "persistent_notify": lambda *_args, **_kwargs: None,
        "log": _completion_logging.getLogger(__name__),
    }
    exec(
        compile(wrapper, str(source_path), "exec"),
        namespace,
    )
    handlers = namespace["_completion_factory"](
        initial_projection,
        initial_setting,
        initial_successful_operation_id,
    )
    return SimpleNamespace(
        handlers=handlers,
        runtime=runtime,
        controller=controller,
        timeframe=timeframe,
        status=status,
        namespace=namespace,
    )


@pytest.mark.asyncio
async def test_analysis_poll_defers_old_success_until_newer_load_finishes() -> None:
    previous = _projection()
    successful = replace(previous, input_id="c" * 64)
    snapshot = _completion_snapshot(
        projection=successful,
        operation_id="operation-b",
        successful_operation_id="operation-a",
        timeframe_setting=0,
        phase=L2ViewRuntimePhase.RUNNING,
        running=True,
    )
    harness = _completion_ui_harness(
        snapshot,
        initial_projection=previous,
    )
    pending_viewport = object()
    harness.handlers["set_pending"]("operation-b", pending_viewport)

    await harness.handlers["poll"]()

    deferred = harness.handlers["read_state"]()
    assert deferred["observed_completion"] == 0
    assert deferred["observed_successful_operation_id"] is None
    assert deferred["pending_operation_id"] == "operation-b"
    assert deferred["pending_viewport"] is pending_viewport
    assert not harness.controller.calls

    harness.runtime.current = replace(
        snapshot,
        phase=L2ViewRuntimePhase.FAILED,
        is_running=False,
        completion_sequence=2,
        last_error="Simulated later load failure",
    )
    await harness.handlers["poll"]()

    completed = harness.handlers["read_state"]()
    assert completed["displayed"] is successful
    assert completed["observed_successful_operation_id"] == "operation-a"
    assert completed["observed_completion"] == 2
    assert completed["pending_operation_id"] is None
    assert completed["pending_viewport"] is None
    assert len(harness.controller.calls) == 1
    assert harness.controller.calls[0]["shock_time_viewport"] is None
    assert harness.timeframe.value == 0


@pytest.mark.asyncio
async def test_analysis_poll_uses_viewport_owned_by_successful_operation() -> None:
    projection = _projection()
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=projection,
            operation_id="operation-a",
            successful_operation_id="operation-a",
            timeframe_setting=0,
        ),
        # This is a reload of an already displayed dataset. Only that case
        # is allowed to preserve the pending operation's viewport.
        initial_projection=projection,
        initial_setting=0,
        initial_successful_operation_id="operation-before-a",
    )
    viewport = object()
    harness.handlers["set_pending"]("operation-a", viewport)

    await harness.handlers["poll"]()

    assert len(harness.controller.calls) == 1
    assert harness.controller.calls[0]["shock_time_viewport"] is viewport
    assert harness.handlers["read_state"]()["pending_operation_id"] is None


@pytest.mark.asyncio
async def test_analysis_poll_discards_pending_viewport_for_first_dataset() -> None:
    projection = _projection()
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=projection,
            operation_id="operation-first",
            successful_operation_id="operation-first",
            timeframe_setting=0,
        )
    )
    viewport = object()
    harness.handlers["set_pending"]("operation-first", viewport)

    await harness.handlers["poll"]()

    assert len(harness.controller.calls) == 1
    assert harness.controller.calls[0]["shock_time_viewport"] is None
    assert harness.handlers["read_state"]()["pending_operation_id"] is None


@pytest.mark.asyncio
async def test_analysis_poll_rechecks_runtime_after_waiting_for_render_lock() -> None:
    projection = _projection()
    snapshot = _completion_snapshot(
        projection=projection,
        operation_id="operation-a",
        successful_operation_id="operation-a",
        timeframe_setting=0,
    )
    harness = _completion_ui_harness(snapshot)
    lock = harness.handlers["read_state"]()["render_lock"]
    await lock.acquire()

    task = asyncio.create_task(harness.handlers["poll"]())
    try:
        await asyncio.sleep(0)
        assert not task.done()

        harness.runtime.current = replace(
            snapshot,
            phase=L2ViewRuntimePhase.RUNNING,
            is_running=True,
            operation_id="operation-b",
        )
        viewport = object()
        harness.handlers["set_pending"]("operation-b", viewport)
    finally:
        lock.release()

    await asyncio.wait_for(task, timeout=2.0)

    observed = harness.handlers["read_state"]()
    assert observed["observed_completion"] == 0
    assert observed["observed_successful_operation_id"] is None
    assert observed["pending_operation_id"] == "operation-b"
    assert observed["pending_viewport"] is viewport
    assert not harness.controller.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    (L2ViewRuntimePhase.STOPPED, L2ViewRuntimePhase.FAILED),
)
async def test_unsuccessful_reload_restores_auto_before_presentation_change(
    phase,
) -> None:
    projection = _projection()
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=projection,
            operation_id="operation-b",
            successful_operation_id="operation-a",
            timeframe_setting=0,
            phase=phase,
            sequence=2,
        ),
        initial_projection=projection,
        initial_setting=0,
        initial_successful_operation_id="operation-a",
    )
    harness.timeframe.value = 1
    harness.handlers["set_pending"]("operation-b", object())

    await harness.handlers["poll"]()

    assert harness.timeframe.value == 0
    assert harness.handlers["read_state"]()["displayed_timeframe_setting"] == 0
    assert not harness.controller.calls

    # Even a later unrelated selector value must not become the setting
    # committed by a presentation-only publication.
    harness.timeframe.value = 15
    await harness.handlers["change_presentation"]()

    assert harness.timeframe.value == 0
    assert harness.handlers["read_state"]()["displayed_timeframe_setting"] == 0
    assert len(harness.controller.calls) == 1


@pytest.mark.asyncio
async def test_identical_projection_new_success_is_handled_once_per_operation() -> None:
    projection = _projection()
    snapshot = _completion_snapshot(
        projection=projection,
        operation_id="operation-a",
        successful_operation_id="operation-a",
        timeframe_setting=0,
    )
    harness = _completion_ui_harness(snapshot)

    await asyncio.gather(
        harness.handlers["poll"](),
        harness.handlers["poll"](),
    )
    assert len(harness.controller.calls) == 1

    harness.runtime.current = replace(
        snapshot,
        operation_id="operation-b",
        last_projection_operation_id="operation-b",
        completion_sequence=2,
    )
    await harness.handlers["poll"]()
    await harness.handlers["poll"]()

    assert len(harness.controller.calls) == 2
    assert (
        harness.handlers["read_state"]()["observed_successful_operation_id"]
        == "operation-b"
    )


@pytest.mark.asyncio
async def test_failed_publication_does_not_commit_requested_timeframe() -> None:
    previous = _projection()
    successful = replace(previous, input_id="d" * 64)
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=successful,
            operation_id="operation-a",
            successful_operation_id="operation-a",
            timeframe_setting=5,
        ),
        initial_projection=previous,
        initial_setting=0,
    )
    harness.controller.fail = True

    await harness.handlers["poll"]()

    observed = harness.handlers["read_state"]()
    assert observed["loaded"] is successful
    assert observed["displayed"] is previous
    assert observed["displayed_timeframe_setting"] == 0
    assert observed["observed_successful_operation_id"] == "operation-a"
    assert harness.controller.commit is None
    assert "chart publication failed" in harness.status.text

    # Do not repeatedly publish the same failed browser generation on
    # every timer poll. An explicit view change remains the retry path.
    await harness.handlers["poll"]()
    assert len(harness.controller.calls) == 1


@pytest.mark.asyncio
async def test_cached_view_commits_captured_auto_setting_not_live_selector() -> None:
    projection = _projection()
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=projection,
            operation_id="operation-a",
            successful_operation_id="operation-a",
            timeframe_setting=0,
        ),
        initial_projection=projection,
        initial_setting=0,
        initial_successful_operation_id="operation-a",
    )

    async def capture_and_change_selector(_chart):
        harness.timeframe.value = 15
        return None

    harness.namespace["capture_shock_time_viewport"] = capture_and_change_selector
    harness.namespace["cached_view"] = lambda *_args, **_kwargs: (
        projection.timeframe_seconds,
        projection,
    )

    await harness.handlers["change_view"]()

    observed = harness.handlers["read_state"]()
    assert observed["displayed"] is projection
    assert observed["displayed_timeframe_setting"] == 0
    assert harness.timeframe.value == 0
    assert len(harness.controller.calls) == 1


@pytest.mark.asyncio
async def test_rejected_cached_view_restores_last_committed_setting() -> None:
    projection = _projection()
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=projection,
            operation_id="operation-a",
            successful_operation_id="operation-a",
            timeframe_setting=0,
        ),
        initial_projection=projection,
        initial_setting=0,
        initial_successful_operation_id="operation-a",
    )

    def rejected_options():
        raise ValueError("Simulated invalid viewing controls")

    harness.namespace["_options"] = rejected_options
    harness.timeframe.value = 15

    await harness.handlers["change_view"]()

    assert harness.timeframe.value == 0
    assert harness.handlers["read_state"]()["displayed_timeframe_setting"] == 0
    assert not harness.controller.calls
    assert "View not changed" in harness.status.text


def _audit_owned_interval_projection():
    from datetime import datetime, timedelta, timezone
    from fractions import Fraction

    from l2shock.analysis.l2_view_stream import (
        L2ViewBar,
        L2ViewOutageRegion,
        L2ViewProjection,
        L2ViewRequest,
    )

    origin = datetime(2026, 1, 1, tzinfo=timezone.utc)
    start = origin + timedelta(seconds=17)
    end = origin + timedelta(minutes=24, seconds=43)

    def constant(value):
        number = Fraction(value)
        return number, number, number, number

    bars = []

    for index in range(25):
        bucket_start = origin + timedelta(minutes=index)
        owned_start = max(start, bucket_start)
        owned_end = min(end, bucket_start + timedelta(minutes=1))

        high = Fraction(90 if index == 12 else 50)
        share = Fraction(50), high, Fraction(50), Fraction(50)

        bars.append(
            L2ViewBar(
                start_utc=bucket_start,
                source_seconds=int((owned_end - owned_start).total_seconds()),
                valid_l2=True,
                bid=constant(2),
                ask=constant(1),
                total=constant(3),
                delta=constant(1),
                bid_share_pct=share,
                price=constant(100),
            )
        )

    region = L2ViewOutageRegion(
        channel="l2",
        start_utc=origin + timedelta(minutes=12),
        end_utc_exclusive=origin + timedelta(minutes=13),
    )
    request = L2ViewRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=start,
        requested_end_utc=end - timedelta(seconds=1),
    )

    return L2ViewProjection(
        request=request,
        start_utc=start,
        end_utc_exclusive=end,
        timeframe_seconds=60,
        max_bars=1200,
        bars=tuple(bars),
        l2_regions=(region,),
        price_regions=(),
        l2_regions_truncated=False,
        price_regions_truncated=False,
        price_status="loaded",
        usable_l2_seconds=int((end - start).total_seconds()),
        unusable_l2_seconds=0,
        partial_market_seconds=0,
        input_id="b" * 64,
    )


def test_audit_all_chart_series_use_owned_interval_centres():
    from datetime import timedelta

    from l2shock.ui.l2_view_chart_options import (
        build_l2_view_chart_options,
        l2_view_bar_time_coordinates,
    )

    projection = _audit_owned_interval_projection()
    coordinates = l2_view_bar_time_coordinates(projection)

    assert coordinates[0][0] == projection.start_utc
    assert coordinates[-1][1] == projection.end_utc_exclusive
    assert coordinates[0][2] == (
        projection.start_utc + timedelta(seconds=21, microseconds=500_000)
    )

    expected = [centre.isoformat() for _start, _end, centre in coordinates]

    # Exercise candle, single-line, and two-line metric configurations.
    for metric in (
        "imbalance_pct",
        "total_change",
        "bid_ask_shares_pct",
    ):
        option = build_l2_view_chart_options(
            projection,
            panel_a_metric=metric,
            panel_b_metric="delta",
        )

        for series in option["series"]:
            assert [item[0] for item in series["data"]] == expected

        metadata = option["l2shockChartMetadata"]
        assert metadata["visible_plot_times_utc"] == expected
        assert metadata["visible_start_times_utc"] == [
            bar.start_utc.isoformat() for bar in projection.bars
        ]
        assert metadata["plot_timestamp_policy"] == ("owned_interval_midpoint_v1")

        for owned_start, owned_end, centre in coordinates:
            assert projection.start_utc <= owned_start < centre < owned_end
            assert owned_end <= projection.end_utc_exclusive

        for axis in option["xAxis"]:
            assert axis["min"] == projection.start_utc.isoformat()
            assert axis["max"] == projection.end_utc_exclusive.isoformat()


def test_audit_extremeness_and_warning_intervals_share_utc_ownership():
    from l2shock.ui.l2_view_chart_options import (
        RATIO_EXTREMENESS_SERIES_SUFFIX,
        build_l2_view_chart_options,
        l2_view_bar_time_coordinates,
    )

    projection = _audit_owned_interval_projection()
    option = build_l2_view_chart_options(
        projection,
        panel_a_metric="imbalance_pct",
        panel_b_metric="delta",
    )

    helper = next(
        series
        for series in option["series"]
        if series["id"].endswith(RATIO_EXTREMENESS_SERIES_SUFFIX)
    )
    owned_start, owned_end, centre = l2_view_bar_time_coordinates(projection)[12]

    assert helper["data"][12][0] == centre.isoformat()
    assert helper["data"][12][2] > 0

    bands = helper["markArea"]["data"]
    assert any(
        band[0]["xAxis"] == owned_start.isoformat()
        and band[1]["xAxis"] == owned_end.isoformat()
        for band in bands
    )

    bid = next(series for series in option["series"] if series["id"] == "l2view-bid")
    warning = bid["markArea"]["data"][0]

    assert warning[0]["xAxis"] == owned_start.isoformat()
    assert warning[1]["xAxis"] == owned_end.isoformat()


def test_audit_single_partial_bar_is_inside_time_axis():
    from dataclasses import replace
    from datetime import timedelta

    from l2shock.analysis.l2_view_stream import L2ViewRequest
    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

    source = _audit_owned_interval_projection()
    start = source.start_utc
    end = start + timedelta(seconds=10)

    projection = replace(
        source,
        request=L2ViewRequest(
            base="BTC",
            preset_hash="a" * 64,
            requested_start_utc=start,
            requested_end_utc=end - timedelta(seconds=1),
        ),
        end_utc_exclusive=end,
        bars=(replace(source.bars[0], source_seconds=10),),
        l2_regions=(),
        usable_l2_seconds=10,
    )
    option = build_l2_view_chart_options(
        projection,
        panel_a_metric="imbalance_pct",
        panel_b_metric="delta",
    )
    expected = (start + timedelta(seconds=5)).isoformat()

    for series in option["series"]:
        assert series["data"][0][0] == expected

    assert option["xAxis"][0]["min"] < expected
    assert expected < option["xAxis"][0]["max"]


def test_audit_exports_distinguish_bucket_interval_and_plot_time():
    import csv
    import io
    import json

    from l2shock.ui.l2_view_chart_options import (
        build_l2_view_chart_options,
        l2_view_bar_time_coordinates,
    )
    from l2shock.ui.l2_view_presentation import (
        displayed_csv_bytes,
        displayed_json_bytes,
    )

    projection = _audit_owned_interval_projection()
    option = build_l2_view_chart_options(
        projection,
        panel_a_metric="imbalance_pct",
        panel_b_metric="delta",
    )
    owned_start, owned_end, centre = l2_view_bar_time_coordinates(projection)[0]

    payload = json.loads(
        displayed_json_bytes(
            projection,
            option,
            panel_a_metric="imbalance_pct",
            panel_b_metric="delta",
        )
    )

    assert payload["schema_version"] == 2
    assert payload["analysis_id"] == projection.input_id
    assert payload["plot_timestamp_policy"] == "owned_interval_midpoint_v1"

    first = payload["bars"][0]
    assert first["start_utc"] == projection.bars[0].start_utc.isoformat()
    assert first["owned_start_utc"] == owned_start.isoformat()
    assert first["owned_end_utc_exclusive"] == owned_end.isoformat()
    assert first["plot_timestamp_utc"] == centre.isoformat()

    raw = displayed_csv_bytes(projection, option).decode("utf-8-sig")
    physical_rows = list(csv.reader(io.StringIO(raw)))
    assert all(len(row) == len(physical_rows[0]) for row in physical_rows)

    rows = list(csv.DictReader(io.StringIO(raw)))
    bid = next(row for row in rows if row["series_id"] == "l2view-bid")
    score = next(row for row in rows if row["series_id"] == "ratio_extremeness")

    for row in (bid, score):
        assert row["timestamp_utc"] == centre.isoformat()
        assert row["bar_start_utc"] == (projection.bars[0].start_utc.isoformat())
        assert row["owned_start_utc"] == owned_start.isoformat()
        assert row["owned_end_utc_exclusive"] == owned_end.isoformat()
        assert row["plot_timestamp_policy"] == "owned_interval_midpoint_v1"


def test_audit_timezone_tooltip_contains_owned_interval_mapping():
    import copy

    from l2shock.ui.display_timezone import (
        display_timezone_formatters,
        with_display_timezone,
    )
    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

    projection = _audit_owned_interval_projection()
    option = build_l2_view_chart_options(
        projection,
        panel_a_metric="imbalance_pct",
        panel_b_metric="delta",
    )
    original = copy.deepcopy(option)

    localized = with_display_timezone(option, "UTC")
    javascript = localized["tooltip"][":formatter"]

    assert option == original
    assert localized["series"] == option["series"]
    assert "__BAR_INTERVALS__" not in javascript
    assert projection.start_utc.isoformat() in javascript
    assert "var interval = intervals[String(plotMilliseconds)];" in javascript
    assert 'stamp(interval[1]) + ")"' in javascript

    for formatter in display_timezone_formatters("UTC").values():
        assert "__BAR_INTERVALS__" not in formatter
        assert "__PARTS__" not in formatter
        assert "__TZ__" not in formatter


def test_audit_generated_tooltip_renders_owned_interval():
    import json
    import shutil
    import subprocess

    import pytest

    from l2shock.ui.display_timezone import with_display_timezone
    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

    node = shutil.which("node")
    if node is None:
        pytest.skip("Optional JavaScript runtime is not installed")

    projection = _audit_owned_interval_projection()
    option = build_l2_view_chart_options(
        projection,
        panel_a_metric="imbalance_pct",
        panel_b_metric="delta",
    )
    localized = with_display_timezone(option, "UTC")
    formatter = localized["tooltip"][":formatter"]
    bid = next(series for series in option["series"] if series["id"] == "l2view-bid")

    harness = r"""
"use strict";
const fs = require("node:fs");
const vm = require("node:vm");
const payload = JSON.parse(fs.readFileSync(0, "utf8"));

const context = vm.createContext({window: {}});
const formatter = vm.runInContext(
    "(" + payload.formatter + ")",
    context,
    {timeout: 2000}
);
const rendered = formatter([{
    axisValue: payload.value[0],
    value: payload.value,
    marker: "",
    seriesName: "Bid",
    seriesId: "l2view-bid"
}]);
process.stdout.write(JSON.stringify({rendered}));
"""

    completed = subprocess.run(
        [node, "-e", harness],
        input=json.dumps(
            {
                "formatter": formatter,
                "value": bid["data"][0],
            }
        ),
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    rendered = json.loads(completed.stdout)["rendered"]

    assert "[2026-01-01 00:00:17, 2026-01-01 00:01:00) (UTC)" in rendered
    assert "Bid" in rendered
    assert " O 2" in rendered

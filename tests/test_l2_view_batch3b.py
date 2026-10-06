# tests/test_l2_view_batch3b.py
"""Analysis admission, cancellation, and presentation-only regressions."""

from __future__ import annotations

import ast
import asyncio
import inspect
import logging
import threading
import warnings
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


from l2shock.analysis.l2_view_metrics import L2ViewMetric
from l2shock.analysis.l2_view_stream import (
    L2ViewBar,
    L2ViewLoadOptions,
    L2ViewProjection,
    L2ViewRequest,
)
from l2shock.ui.l2_view_runtime import (
    L2ViewRuntime,
    L2ViewRuntimeBusyError,
    L2ViewRuntimePhase,
)
from l2shock.ui.state import get_state, reset_state_for_tests

_ROOT = Path(__file__).resolve().parents[1]
_TAB_PATH = _ROOT / "l2shock/ui/tab_l2_view.py"
_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _request() -> L2ViewRequest:
    return L2ViewRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_T0,
        requested_end_utc=_T0 + timedelta(seconds=2),
    )


def _projection() -> L2ViewProjection:
    def candle(value: Decimal) -> tuple[Decimal, ...]:
        return value, value, value, value

    bars = tuple(
        L2ViewBar(
            start_utc=_T0 + timedelta(seconds=index),
            source_seconds=1,
            valid_l2=True,
            bid=candle(Decimal(2 + index)),
            ask=candle(Decimal(1)),
            total=candle(Decimal(3 + index)),
            delta=candle(Decimal(1 + index)),
            bid_share_pct=None,
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


async def _settle_callbacks() -> None:
    await asyncio.sleep(0)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_locked_operation_is_rejected_before_admission() -> None:
    reset_state_for_tests()
    state = get_state()
    runtime = L2ViewRuntime()
    await state.operation_lock.acquire()

    try:
        before = runtime.snapshot()

        with pytest.raises(
            L2ViewRuntimeBusyError,
            match="process lock",
        ):
            runtime.start(_request(), L2ViewLoadOptions())

        after = runtime.snapshot()
        assert after.phase == before.phase
        assert after.operation_id == before.operation_id
        assert after.completion_sequence == before.completion_sequence
        assert after.is_running is False
        assert state.active_operation_name == ""
        assert state.active_operation_started_at is None
        assert state.tracked_tasks == set()

    finally:
        state.operation_lock.release()


def test_start_without_running_loop_does_not_change_runtime() -> None:
    reset_state_for_tests()
    state = get_state()
    runtime = L2ViewRuntime()
    projection = _projection()
    runtime._last = projection
    before = runtime.snapshot()

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)

        with pytest.raises(RuntimeError):
            runtime.start(_request(), L2ViewLoadOptions())

    after = runtime.snapshot()
    assert after == before
    assert after.last_projection is projection
    assert state.active_operation_name == ""
    assert state.active_operation_started_at is None
    assert state.tracked_tasks == set()
    try:
        assert state.operation_lock.locked() is False
    except RuntimeError:
        # Accessing the operation lock requires a running event loop,
        # which intentionally does not exist in this synchronous test.
        pass


@pytest.mark.asyncio
async def test_task_cancelled_before_execution_releases_admission() -> None:
    reset_state_for_tests()
    state = get_state()
    calls: list[str] = []
    projection = _projection()

    def loader(*_args: Any, **_kwargs: Any) -> L2ViewProjection:
        calls.append("loader")
        return projection

    runtime = L2ViewRuntime(loader=loader)
    runtime._last = projection

    task = runtime.start(_request(), L2ViewLoadOptions())
    operation_id = runtime.snapshot().operation_id

    # No await occurred between start() and cancel(), so the coroutine has
    # not begun execution under the default asyncio task factory.
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await _settle_callbacks()

    snapshot = runtime.snapshot()
    assert calls == []
    assert snapshot.phase is L2ViewRuntimePhase.STOPPED
    assert snapshot.operation_id == operation_id
    assert snapshot.completion_sequence == 1
    assert snapshot.last_projection is projection
    assert snapshot.is_running is False
    assert snapshot.stop_requested is False
    assert state.active_operation_name == ""
    assert state.active_operation_started_at is None
    assert state.tracked_tasks == set()
    assert state.operation_lock.locked() is False

    # Admission must be reusable after the cancelled operation is finalized.
    replacement = runtime.start(_request(), L2ViewLoadOptions())
    assert await replacement is projection
    await _settle_callbacks()

    assert calls == ["loader"]
    assert runtime.snapshot().completion_sequence == 2
    assert runtime.snapshot().phase is L2ViewRuntimePhase.COMPLETED


@pytest.mark.asyncio
async def test_stop_before_execution_does_not_invoke_loader() -> None:
    reset_state_for_tests()
    state = get_state()
    calls: list[str] = []
    projection = _projection()

    def loader(*_args: Any, **_kwargs: Any) -> L2ViewProjection:
        calls.append("loader")
        return projection

    runtime = L2ViewRuntime(loader=loader)
    runtime._last = projection
    task = runtime.start(_request(), L2ViewLoadOptions())

    assert runtime.request_stop() is True
    assert await task is None
    await _settle_callbacks()

    snapshot = runtime.snapshot()
    assert calls == []
    assert snapshot.phase is L2ViewRuntimePhase.STOPPED
    assert snapshot.last_projection is projection
    assert snapshot.completion_sequence == 1
    assert state.active_operation_name == ""
    assert state.tracked_tasks == set()
    assert state.operation_lock.locked() is False


@pytest.mark.asyncio
async def test_shutdown_before_execution_does_not_invoke_loader() -> None:
    reset_state_for_tests()
    state = get_state()
    calls: list[str] = []
    projection = _projection()

    def loader(*_args: Any, **_kwargs: Any) -> L2ViewProjection:
        calls.append("loader")
        return projection

    runtime = L2ViewRuntime(loader=loader)
    runtime._last = projection
    task = runtime.start(_request(), L2ViewLoadOptions())
    state.shutdown_started = True

    try:
        assert await task is None
        await _settle_callbacks()

        snapshot = runtime.snapshot()
        assert calls == []
        assert snapshot.phase is L2ViewRuntimePhase.STOPPED
        assert snapshot.last_projection is projection
        assert state.active_operation_name == ""
        assert state.tracked_tasks == set()
        assert state.operation_lock.locked() is False

    finally:
        state.shutdown_started = False


@pytest.mark.asyncio
async def test_invalid_loader_result_preserves_last_projection() -> None:
    reset_state_for_tests()
    state = get_state()
    projection = _projection()

    def loader(*_args: Any, **_kwargs: Any) -> object:
        return object()

    runtime = L2ViewRuntime(loader=loader)
    runtime._last = projection
    task = runtime.start(_request(), L2ViewLoadOptions())

    with pytest.raises(
        TypeError,
        match="must return L2ViewProjection",
    ):
        await task

    await _settle_callbacks()

    snapshot = runtime.snapshot()
    assert snapshot.phase is L2ViewRuntimePhase.FAILED
    assert snapshot.last_projection is projection
    assert snapshot.last_error == "Unexpected TypeError"
    assert state.active_operation_name == ""
    assert state.tracked_tasks == set()
    assert state.operation_lock.locked() is False


@pytest.mark.asyncio
async def test_repeated_cancellation_keeps_lock_until_worker_exit() -> None:
    reset_state_for_tests()
    state = get_state()
    projection = _projection()
    entered = threading.Event()
    release = threading.Event()

    def loader(
        _request: L2ViewRequest,
        _options: L2ViewLoadOptions,
        *,
        cancellation_probe: Any,
        progress_sink: Any,
    ) -> L2ViewProjection:
        entered.set()

        if not release.wait(timeout=5.0):
            raise TimeoutError("test did not release the worker")

        return projection

    runtime = L2ViewRuntime(loader=loader)
    runtime._last = projection
    task = runtime.start(_request(), L2ViewLoadOptions())

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert task.done() is False
        assert state.operation_lock.locked() is True
        assert state.active_operation_name == "manual_analysis_load"
        assert runtime.snapshot().last_projection is projection

        release.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=3.0)

        await _settle_callbacks()

        assert runtime.snapshot().phase is L2ViewRuntimePhase.STOPPED
        assert runtime.snapshot().last_projection is projection
        assert runtime.snapshot().completion_sequence == 1
        assert state.active_operation_name == ""
        assert state.tracked_tasks == set()
        assert state.operation_lock.locked() is False

    finally:
        release.set()

        if not task.done():
            task.cancel()

            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=3.0)
            except asyncio.CancelledError:
                pass

        await _settle_callbacks()


def _handler_node(name: str) -> ast.AsyncFunctionDef:
    source = _TAB_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(_TAB_PATH))

    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name
    ]

    assert len(matches) == 1, f"Expected one nested handler named {name}"
    return matches[0]


class _StandaloneHandler(ast.NodeTransformer):
    """Redirect closure assignments to the isolated test namespace."""

    def visit_Nonlocal(self, node: ast.Nonlocal) -> ast.Global:
        return ast.copy_location(
            ast.Global(names=list(node.names)),
            node,
        )


def _load_handler(name: str, namespace: dict[str, Any]) -> Any:
    node = _handler_node(name)
    node = _StandaloneHandler().visit(node)
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)

    exec(
        compile(module, str(_TAB_PATH), "exec"),
        namespace,
    )

    return namespace[name]


def _presentation_namespace() -> tuple[dict[str, Any], list[Any]]:
    events: list[Any] = []
    projection = _projection()
    viewport = object()
    chart = object()

    namespace: dict[str, Any] = {
        "Any": Any,
        "state": SimpleNamespace(shutdown_started=False),
        "runtime": SimpleNamespace(
            snapshot=lambda: SimpleNamespace(is_running=False),
        ),
        "preset_loading": False,
        "publishing": False,
        "render_lock": asyncio.Lock(),
        "displayed": projection,
        "loaded": object(),
        "chart": chart,
        "displayed_timeframe_setting": 0,
        "status": SimpleNamespace(
            set_text=lambda value: events.append(("status", value)),
        ),
        "log": logging.getLogger(__name__),
    }

    def sync_controls() -> None:
        events.append(("sync", namespace["publishing"]))

    async def capture(selected_chart: Any) -> object:
        assert selected_chart is chart
        assert namespace["publishing"] is True
        assert namespace["render_lock"].locked() is True
        events.append("capture")
        return viewport

    async def publish(
        selected: L2ViewProjection,
        *,
        viewport: Any,
        new_source: bool,
        timeframe_setting: int,
    ) -> None:
        assert selected is projection
        assert new_source is False
        events.append(("publish", selected, viewport, timeframe_setting))

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("Presentation entered a loading-capable path")

    namespace.update(
        {
            "_sync_controls": sync_controls,
            "capture_shock_time_viewport": capture,
            "_publish": publish,
            "_options": forbidden,
            "cached_view": forbidden,
            "_admit": forbidden,
        }
    )

    return namespace, events


@pytest.mark.asyncio
async def test_presentation_uses_displayed_projection_without_loading() -> None:
    namespace, events = _presentation_namespace()
    handler = _load_handler("_change_presentation", namespace)

    await handler()

    publications = [
        event for event in events if isinstance(event, tuple) and event[0] == "publish"
    ]

    assert len(publications) == 1
    assert publications[0][1] is namespace["displayed"]
    assert events[0] == ("sync", True)
    assert events[-1] == ("sync", False)
    assert namespace["publishing"] is False
    assert namespace["render_lock"].locked() is False


@pytest.mark.asyncio
async def test_presentation_stops_after_shutdown_during_capture() -> None:
    namespace, events = _presentation_namespace()

    async def capture(_chart: Any) -> object:
        assert namespace["publishing"] is True
        namespace["state"].shutdown_started = True
        return object()

    namespace["capture_shock_time_viewport"] = capture
    handler = _load_handler("_change_presentation", namespace)

    await handler()

    assert not any(
        isinstance(event, tuple) and event[0] == "publish" for event in events
    )
    assert namespace["publishing"] is False
    assert namespace["render_lock"].locked() is False
    assert events[-1] == ("sync", False)


@pytest.mark.asyncio
async def test_presentation_failure_restores_transaction_state() -> None:
    namespace, events = _presentation_namespace()

    async def publish(
        _projection: L2ViewProjection,
        *,
        viewport: Any,
        new_source: bool,
        timeframe_setting: int,
    ) -> None:
        raise RuntimeError("test publication failure")

    namespace["_publish"] = publish
    handler = _load_handler("_change_presentation", namespace)

    await handler()

    assert namespace["publishing"] is False
    assert namespace["render_lock"].locked() is False
    assert (
        "status",
        "Presentation not changed: test publication failure",
    ) in events
    assert events[-1] == ("sync", False)


def test_presentation_handler_has_no_loading_calls() -> None:
    node = _handler_node("_change_presentation")
    called_names = {
        call.func.id
        for call in ast.walk(node)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }

    assert {
        "_options",
        "cached_view",
        "_admit",
        "stream_l2_view",
    }.isdisjoint(called_names)


def test_controls_use_the_correct_handlers() -> None:
    source = _TAB_PATH.read_text(encoding="utf-8")

    for control in ("panel_a", "panel_b", "warnings_switch"):
        assert f"{control}.on_value_change(_change_presentation)" in source
        assert f"{control}.on_value_change(_change_view)" not in source

    assert "timeframe_input.on_value_change(_timeframe_changed)" in source
    assert "timeframe_input.on_value_change(_change_view)" not in source
    assert 'max_bars_input.on("blur", _change_view)' in source
    assert 'max_bars_input.on("keydown.enter", _change_view)' in source
    assert "max_bars_input.on_value_change(_change_view)" not in source


def test_start_checks_render_transaction_before_admission() -> None:
    source = _TAB_PATH.read_text(encoding="utf-8")
    node = _handler_node("_start")
    handler_source = ast.get_source_segment(source, node) or ""

    assert "render_lock.locked()" in handler_source
    assert handler_source.index("render_lock.locked()") < (
        handler_source.index("_admit(")
    )


def _completion_snapshot(
    *,
    projection,
    operation_id: str,
    successful_operation_id: str | None,
    timeframe_setting: int | None,
    sequence: int = 1,
) -> SimpleNamespace:
    return SimpleNamespace(
        phase=L2ViewRuntimePhase.COMPLETED,
        is_running=False,
        completion_sequence=sequence,
        hours_done=1,
        hours_total=1,
        last_projection=projection,
        last_error=None,
        operation_id=operation_id,
        stop_requested=False,
        last_projection_operation_id=successful_operation_id,
        last_projection_timeframe_setting=timeframe_setting,
    )


def _completion_ui_harness(
    snapshot,
    *,
    initial_projection=None,
    initial_setting=0,
    initial_successful_operation_id=None,
):
    loaded = initial_projection
    displayed = initial_projection
    displayed_timeframe_setting = initial_setting
    observed_successful_operation_id = initial_successful_operation_id
    observed_completion = 0
    pending_operation_id = None
    pending_viewport = None
    publishing = False

    class _Controller:
        def __init__(self):
            self.fail = False
            self.calls: list[dict] = []
            self.commit = SimpleNamespace(
                owner_id="initial",
                publication=SimpleNamespace(render_token="t" * 32),
            )

        async def publish(
            self,
            option,
            *,
            owner_id,
            preserve_viewport,
            shock_time_viewport=None,
            temporal_viewport=None,
        ):
            self.calls.append(
                {
                    "owner_id": owner_id,
                    "shock_time_viewport": shock_time_viewport,
                    "preserve_viewport": preserve_viewport,
                }
            )
            if self.fail:
                raise RuntimeError("Mock publish failure")
            self.commit = SimpleNamespace(
                owner_id=owner_id,
                publication=SimpleNamespace(render_token="t" * 32),
            )
            return self.commit

    controller = _Controller()

    class _Status:
        def __init__(self):
            self.text = ""

        def set_text(self, value):
            self.text = value

    status = _Status()

    namespace: dict[str, Any] = {
        "log": logging.getLogger("l2shock.ui.tab_l2_view"),
        "persistent_notify": lambda msg, **kwargs: None,
        "capture_shock_time_viewport": lambda chart: None,
        "capture_y_viewports": lambda chart, **kwargs: {},
        "panel_a": SimpleNamespace(value="imbalance_pct"),
        "panel_b": SimpleNamespace(value="delta"),
        "warnings_switch": SimpleNamespace(value=True),
    }

    class _Runtime:
        def __init__(self):
            self._snap = snapshot

        def snapshot(self):
            return self._snap

    runtime = _Runtime()

    async def _publish_impl(projection, *, viewport, new_source, timeframe_setting):
        nonlocal displayed, displayed_timeframe_setting, publishing
        publishing = True
        try:
            commit = await controller.publish(
                {},
                owner_id=projection.input_id,
                preserve_viewport=False,
                shock_time_viewport=viewport,
            )
            if commit is None:
                raise RuntimeError("Chart publication was superseded")
            displayed = projection
            displayed_timeframe_setting = timeframe_setting
            valid_l2_bars = sum(bar.valid_l2 for bar in projection.bars)
            if new_source and (
                valid_l2_bars == 0
                or projection.unusable_l2_seconds > 0
                or projection.partial_market_seconds > 0
            ):
                notify_fn = namespace.get("persistent_notify")
                if notify_fn is not None:
                    notify_fn(
                        "quality warning",
                        title="L2 data-quality warning",
                        notification_type="warning",
                    )
        finally:
            publishing = False

    async def poll():
        nonlocal loaded, displayed, displayed_timeframe_setting
        nonlocal observed_successful_operation_id, observed_completion
        nonlocal pending_operation_id, pending_viewport, publishing

        snap = runtime.snapshot()
        if publishing or snap.is_running:
            return
        successful_operation_id = snap.last_projection_operation_id
        has_unhandled_success = (
            snap.last_projection is not None
            and successful_operation_id is not None
            and successful_operation_id != observed_successful_operation_id
        )
        if (
            snap.completion_sequence == observed_completion
            and not has_unhandled_success
        ):
            return
        if has_unhandled_success:
            projection = snap.last_projection
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
                tf_setting = snap.last_projection_timeframe_setting
                if tf_setting is None:
                    raise RuntimeError(
                        "Successful Analysis result lacks timeframe ownership"
                    )
                await _publish_impl(
                    projection,
                    viewport=viewport,
                    new_source=not same_range,
                    timeframe_setting=tf_setting,
                )
            except Exception:
                status.set_text(
                    "Data loaded, but chart publication failed. "
                    "Change Timeframe or Maximum viewing bars to retry "
                    "the loaded result. Panel and Warnings changes only "
                    "rebuild the last displayed result."
                )
            observed_successful_operation_id = successful_operation_id
        if pending_operation_id == successful_operation_id:
            pending_operation_id = None
            pending_viewport = None
        snap = runtime.snapshot()
        if snap.is_running:
            return
        observed_completion = snap.completion_sequence

    async def change_view():
        nonlocal publishing
        snap = runtime.snapshot()
        if snap.is_running or loaded is None:
            return
        capture_fn = namespace.get("capture_shock_time_viewport")
        if inspect.iscoroutinefunction(capture_fn):
            viewport = await capture_fn(None)
        elif callable(capture_fn):
            viewport = capture_fn(None)
        else:
            viewport = None
        new_source = (
            displayed is None
            or displayed.request != loaded.request
            or displayed.input_id != loaded.input_id
        )
        await _publish_impl(
            loaded,
            viewport=None if new_source else viewport,
            new_source=new_source,
            timeframe_setting=displayed_timeframe_setting,
        )

    async def change_presentation():
        nonlocal publishing
        if displayed is None:
            return
        capture_fn = namespace.get("capture_shock_time_viewport")
        if inspect.iscoroutinefunction(capture_fn):
            viewport = await capture_fn(None)
        elif callable(capture_fn):
            viewport = capture_fn(None)
        else:
            viewport = None
        await _publish_impl(
            displayed,
            viewport=viewport,
            new_source=False,
            timeframe_setting=displayed_timeframe_setting,
        )

    def read_state():
        return {"loaded": loaded, "displayed": displayed}

    return SimpleNamespace(
        namespace=namespace,
        controller=controller,
        status=status,
        runtime=runtime,
        handlers={
            "poll": poll,
            "change_view": change_view,
            "change_presentation": change_presentation,
            "read_state": read_state,
        },
    )


@pytest.mark.asyncio
async def test_failed_publication_cached_retry_owns_new_dataset_warning() -> None:
    previous = _projection()
    successful = replace(
        previous,
        input_id="d" * 64,
        usable_l2_seconds=2,
        unusable_l2_seconds=1,
    )
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=successful,
            operation_id="operation-new",
            successful_operation_id="operation-new",
            timeframe_setting=0,
        ),
        initial_projection=previous,
        initial_setting=0,
    )
    notifications = []
    y_captures = []
    old_viewport = object()

    def notify(message, **kwargs):
        notifications.append((message, kwargs))

    async def capture_x(_chart):
        return old_viewport

    async def capture_y(_chart, **kwargs):
        y_captures.append(kwargs)
        return {}

    harness.namespace["persistent_notify"] = notify
    harness.namespace["capture_shock_time_viewport"] = capture_x
    harness.namespace["capture_y_viewports"] = capture_y

    harness.controller.fail = True
    await harness.handlers["poll"]()

    assert harness.handlers["read_state"]()["loaded"] is successful
    assert harness.handlers["read_state"]()["displayed"] is previous
    assert "Timeframe or Maximum viewing bars" in harness.status.text
    assert notifications == []

    harness.controller.fail = False
    await harness.handlers["change_view"]()

    assert harness.handlers["read_state"]()["displayed"] is successful
    assert harness.controller.calls[-1]["owner_id"] == successful.input_id
    assert harness.controller.calls[-1]["shock_time_viewport"] is None
    assert y_captures == []
    assert any(
        kwargs.get("title") == "L2 data-quality warning"
        for _message, kwargs in notifications
    )


@pytest.mark.asyncio
async def test_same_dataset_cached_view_preserves_existing_viewport() -> None:
    projection = _projection()
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=projection,
            operation_id="operation-existing",
            successful_operation_id="operation-existing",
            timeframe_setting=0,
        ),
        initial_projection=projection,
        initial_setting=0,
        initial_successful_operation_id="operation-existing",
    )
    viewport = object()
    notifications = []

    async def capture_x(_chart):
        return viewport

    def notify(message, **kwargs):
        notifications.append((message, kwargs))

    harness.namespace["capture_shock_time_viewport"] = capture_x
    harness.namespace["persistent_notify"] = notify

    await harness.handlers["change_view"]()

    assert harness.controller.calls[-1]["shock_time_viewport"] is viewport
    assert harness.handlers["read_state"]()["displayed"].input_id == (
        projection.input_id
    )
    assert not any(
        kwargs.get("title") == "L2 data-quality warning"
        for _message, kwargs in notifications
    )


@pytest.mark.asyncio
async def test_panel_change_after_failed_publication_keeps_displayed_source() -> None:
    previous = _projection()
    successful = replace(previous, input_id="e" * 64)
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=successful,
            operation_id="operation-new",
            successful_operation_id="operation-new",
            timeframe_setting=0,
        ),
        initial_projection=previous,
        initial_setting=0,
    )

    harness.controller.fail = True
    await harness.handlers["poll"]()

    harness.controller.fail = False
    harness.namespace["panel_a"].value = "total"

    await harness.handlers["change_presentation"]()

    observed = harness.handlers["read_state"]()
    assert observed["loaded"] is successful
    assert observed["displayed"] is previous
    assert harness.controller.calls[-1]["owner_id"] == previous.input_id


@pytest.mark.asyncio
async def test_success_after_failed_publication_is_compared_to_displayed_source() -> (
    None
):
    previous = _projection()
    first_success = replace(
        previous,
        input_id="f" * 64,
        usable_l2_seconds=2,
        unusable_l2_seconds=1,
    )
    harness = _completion_ui_harness(
        _completion_snapshot(
            projection=first_success,
            operation_id="operation-first",
            successful_operation_id="operation-first",
            timeframe_setting=0,
        ),
        initial_projection=previous,
        initial_setting=0,
    )
    notifications = []

    def notify(message, **kwargs):
        notifications.append((message, kwargs))

    harness.namespace["persistent_notify"] = notify
    harness.controller.fail = True
    await harness.handlers["poll"]()

    # The next successful operation has the same request and input identity
    # as the failed publication, but that source was never displayed.
    harness.controller.fail = False

    # Use the runtime double's snapshot method directly so this test does
    # not depend on the name of its internal storage attribute.
    harness.runtime.snapshot = lambda: _completion_snapshot(
        projection=first_success,
        operation_id="operation-second",
        successful_operation_id="operation-second",
        timeframe_setting=0,
        sequence=2,
    )

    await harness.handlers["poll"]()

    assert harness.handlers["read_state"]()["displayed"] is first_success
    assert any(
        kwargs.get("title") == "L2 data-quality warning"
        for _message, kwargs in notifications
    )


def _audit_timeframe_gate(*, displayed_setting, change_view):
    import ast
    import copy
    from pathlib import Path

    import l2shock.ui.tab_l2_view as module

    path = Path(module.__file__)
    tree = ast.parse(
        path.read_text(encoding="utf-8"),
        filename=str(path),
    )
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_timeframe_changed"
    ]
    assert len(matches) == 1

    extracted = ast.Module(
        body=[copy.deepcopy(matches[0])],
        type_ignores=[],
    )
    ast.fix_missing_locations(extracted)

    namespace = {
        "Any": object,
        "displayed_timeframe_setting": displayed_setting,
        "_change_view": change_view,
    }
    exec(compile(extracted, str(path), "exec"), namespace)
    return namespace["_timeframe_changed"], namespace


def test_audit_timeframe_restoration_creates_no_queued_callback():
    from types import SimpleNamespace

    def forbidden(_event):
        raise AssertionError("Restoration created a viewing coroutine")

    gate, namespace = _audit_timeframe_gate(
        displayed_setting=0,
        change_view=forbidden,
    )

    assert gate(SimpleNamespace(value=0)) is None

    # Nothing was scheduled that could start after publication state changes.
    namespace["displayed_timeframe_setting"] = 15


@pytest.mark.asyncio
async def test_audit_new_timeframe_choice_returns_existing_async_handler():
    import inspect
    from types import SimpleNamespace

    calls = []

    async def change_view(event):
        calls.append(event)

    gate, _namespace = _audit_timeframe_gate(
        displayed_setting=0,
        change_view=change_view,
    )
    event = SimpleNamespace(value=15)
    pending = gate(event)

    assert inspect.isawaitable(pending)
    assert calls == []

    await pending
    assert calls == [event]


def test_audit_timeframe_gate_is_synchronous():
    import ast
    from pathlib import Path

    import l2shock.ui.tab_l2_view as module

    path = Path(module.__file__)
    tree = ast.parse(
        path.read_text(encoding="utf-8"),
        filename=str(path),
    )
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_timeframe_changed"
    ]

    assert len(matches) == 1
    assert isinstance(matches[0], ast.FunctionDef)
    assert not any(isinstance(node, ast.Await) for node in ast.walk(matches[0]))

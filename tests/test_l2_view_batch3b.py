# tests/test_l2_view_batch3b.py
"""Analysis admission, cancellation, and presentation-only regressions."""

from __future__ import annotations

import ast
import asyncio
import logging
import threading
import warnings
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

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
    ) -> None:
        assert selected is projection
        assert new_source is False
        events.append(("publish", selected, viewport))

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

    assert "timeframe_input.on_value_change(_change_view)" in source
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

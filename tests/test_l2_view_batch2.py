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

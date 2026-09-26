# tests/test_echarts_live_state.py
from __future__ import annotations

import json
import shutil
import subprocess
import pytest

from l2shock.ui.chart_interactions import (
    AnalysisChartInteractionError,
    _chart_metadata,
    capture_analysis_chart_viewport,
)
from l2shock.ui.echarts import (
    ECHART_RENDER_TOKEN_SERIES_PREFIX,
    _apply_option_code,
    confirm_echart_render_identity,
    read_echart_live_state,
    set_echart_options,
)

_APPLY_MARKER = "/* l2shock:apply */"


class _Answer:
    """NiceGUI-like response: fires when not awaited, answers when awaited."""

    def __init__(self, value: object = None) -> None:
        self._value = value

    def __await__(self):
        if isinstance(self._value, BaseException):
            raise self._value
        return self._value
        yield  # pragma: no cover - makes this a generator


class _ProbeClient:
    def __init__(self) -> None:
        self.states: list[object] = []
        self.codes: list[str] = []
        self.applied: list[str] = []

    def run_javascript(self, code: str, *, timeout: float = 1.0) -> _Answer:
        if code.startswith(_APPLY_MARKER):
            self.applied.append(code)
            return _Answer(None)

        self.codes.append(code)
        return _Answer(self.states.pop(0) if self.states else None)


class _ProbeChart:
    def __init__(self, *, element_id: int = 7) -> None:
        self.id = element_id
        self.client: object = _ProbeClient()
        self._props: dict[str, object] = {"options": {}}
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.updates = 0

    def update(self) -> None:
        self.updates += 1

    def run_chart_method(self, method: str, *args: object, **_kwargs: object):
        self.calls.append((method, args))
        return _Answer(None)


def _option() -> dict[str, object]:
    return {
        "xAxis": [{"type": "category", "data": ["a", "b", "c"]}],
        "yAxis": [{"type": "value"}],
        "series": [{"name": "v", "type": "line", "data": [1, 2, 3]}],
        "l2shockChartMetadata": {"visible_bar_count": 3},
    }


def _good_state(token: str, *, count: int = 3) -> dict[str, object]:
    return {
        "ok": True,
        "apply": {"token": token, "state": "applied", "error": None},
        "tokens": [token],
        "axis_present": True,
        "category_count": count,
        "width": 900,
        "height": 700,
        "data_zoom": {"start": 10.0, "end": 90.0},
    }


def test_set_echart_options_uses_browser_apply_script() -> None:
    chart = _ProbeChart()

    publication = set_echart_options(chart, _option())

    assert chart.updates == 1
    assert len(chart.client.applied) == 1
    assert not any(method.endswith("setOption") for method, _args in chart.calls)

    code = chart.client.applied[0]
    assert "__L2SHOCK_" not in code
    assert "var chartId = 7;" in code
    assert json.dumps(publication.render_token) in code
    assert (ECHART_RENDER_TOKEN_SERIES_PREFIX + publication.render_token) in code
    assert "notMerge: true" in code
    assert "instance.clear()" in code
    assert publication.expected_category_count == 3


def test_set_echart_options_falls_back_without_client() -> None:
    chart = _ProbeChart()
    chart.client = None

    publication = set_echart_options(chart, _option())

    assert chart.calls[0][0] == ":setOption"
    expression, merge_options = chart.calls[0][1]
    assert merge_options == "({notMerge:true,lazyUpdate:false})"
    assert publication.render_token in str(expression)


def test_apply_code_has_no_unreplaced_placeholders() -> None:
    code = _apply_option_code(9, render_token="ab", option_expression="({})")

    assert code.startswith(_APPLY_MARKER)
    assert "__L2SHOCK_" not in code
    assert "var chartId = 9;" in code
    assert 'token: "ab",' in code
    assert "return (({}));" in code


@pytest.mark.asyncio
async def test_confirm_uses_compact_probe_not_get_option() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    chart.client.states = [_good_state(publication.render_token)]

    acknowledged, reason = await confirm_echart_render_identity(chart, publication)

    assert (acknowledged, reason) == (True, "")
    assert chart._l2shock_acknowledged_render_token == publication.render_token
    assert not any(method == "getOption" for method, _args in chart.calls)
    assert len(chart.client.codes) == 1


@pytest.mark.asyncio
async def test_confirm_fails_fast_when_browser_rejects_option() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    rejected = _good_state(publication.render_token)
    rejected["tokens"] = []
    rejected["apply"] = {
        "token": publication.render_token,
        "state": "failed",
        "error": "boom",
    }
    chart.client.states = [rejected]

    acknowledged, reason = await confirm_echart_render_identity(chart, publication)

    assert acknowledged is False
    assert "rejected" in reason and "boom" in reason
    assert len(chart.client.codes) == 1


@pytest.mark.asyncio
async def test_confirm_rejects_missing_token_and_names_apply_state() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    stale = _good_state("0" * 32)
    chart.client.states = [stale, stale]

    acknowledged, reason = await confirm_echart_render_identity(
        chart,
        publication,
        attempts=2,
        retry_delay_seconds=0,
    )

    assert acknowledged is False
    assert "render token" in reason
    assert "apply: not-run" in reason
    assert chart._l2shock_acknowledged_render_token == ""


@pytest.mark.asyncio
async def test_confirm_rejects_category_count_mismatch() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    chart.client.states = [_good_state(publication.render_token, count=2)]

    acknowledged, reason = await confirm_echart_render_identity(
        chart,
        publication,
        attempts=1,
    )

    assert acknowledged is False
    assert "expected=3, observed=2" in reason


@pytest.mark.asyncio
async def test_confirm_reports_unanswered_probe() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    chart.client.states = [TimeoutError()]

    acknowledged, reason = await confirm_echart_render_identity(
        chart,
        publication,
        attempts=1,
    )

    assert acknowledged is False
    assert "TimeoutError" in reason


@pytest.mark.asyncio
async def test_confirm_rejects_superseded_generation_without_probe() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    chart._l2shock_render_token = "f" * 32

    acknowledged, reason = await confirm_echart_render_identity(chart, publication)

    assert acknowledged is False
    assert "superseded" in reason
    assert chart.client.codes == []


@pytest.mark.asyncio
async def test_probe_code_has_no_unreplaced_placeholders() -> None:
    chart = _ProbeChart(element_id=42)
    chart.client.states = [{"ok": False, "reason": "no live ECharts instance"}]

    state = await read_echart_live_state(chart, x_axis_index=1)

    assert state == {"ok": False, "reason": "no live ECharts instance"}
    code = chart.client.codes[0]
    assert "__L2SHOCK_" not in code
    assert "var chartId = 42;" in code
    assert "var axisIndex = 1;" in code
    assert "apply: applyRecord," in code
    assert json.dumps(ECHART_RENDER_TOKEN_SERIES_PREFIX) in code


@pytest.mark.asyncio
async def test_probe_is_unavailable_for_widgets_without_client() -> None:
    class _Bare:
        pass

    assert await read_echart_live_state(_Bare()) is None


@pytest.mark.asyncio
async def test_viewport_capture_reads_probe_zoom() -> None:
    chart = _ProbeChart()
    chart.client.states = [_good_state("a" * 32)]

    viewport = await capture_analysis_chart_viewport(chart)

    assert viewport is not None
    assert (viewport.start_percent, viewport.end_percent) == (10.0, 90.0)
    assert not any(method == "getOption" for method, _args in chart.calls)


def test_chart_metadata_contract_is_strict_again() -> None:
    with pytest.raises(AnalysisChartInteractionError, match="l2shockChartMetadata"):
        _chart_metadata({"series": []})


def test_browser_scripts_no_longer_wrap_set_option() -> None:
    from l2shock.ui.echarts import _live_state_probe_code

    apply = _apply_option_code(3, render_token="t", option_expression="({})")
    probe = _live_state_probe_code(3, x_axis_index=0)

    for code in (apply, probe):
        assert "__L2SHOCK_" not in code
        assert "instance.setOption = function" not in code
        assert "__l2shockInstanceUid" in code

    assert "token_after" in apply
    assert json.dumps(ECHART_RENDER_TOKEN_SERIES_PREFIX) in apply
    assert "series_count: seriesList.length" in probe


def test_real_widget_publication_disables_nicegui_merge_update() -> None:
    chart = _ProbeChart()
    chart._update_method = "update_chart"

    set_echart_options(chart, _option())

    assert chart._update_method is None


@pytest.mark.asyncio
async def test_missing_token_reason_includes_browser_diagnostics() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    state = _good_state("0" * 32)
    state["series_count"] = 6
    state["series_names"] = ["Price", "Bid"]
    state["instance_uid"] = "7#2"
    state["apply"] = {
        "token": publication.render_token,
        "state": "applied",
        "error": None,
        "token_after": True,
        "series_after": 7,
        "instance_uid": "7#1",
    }
    chart.client.states = [state]

    acknowledged, reason = await confirm_echart_render_identity(
        chart,
        publication,
        attempts=1,
    )

    assert acknowledged is False
    assert "live series=6" in reason
    assert "instance=7#2" in reason
    assert "apply instance=7#1" in reason
    assert "token right after apply=True" in reason


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_browser_scripts_are_valid_javascript(tmp_path) -> None:
    from l2shock.ui.echarts import _live_state_probe_code

    scripts = {
        "apply.js": _apply_option_code(3, render_token="t", option_expression="({})"),
        "probe.js": _live_state_probe_code(3, x_axis_index=0),
    }

    for name, code in scripts.items():
        path = tmp_path / name
        path.write_text(code, encoding="utf-8")
        result = subprocess.run(
            ["node", "--check", str(path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, name + ": " + result.stderr


def test_apply_script_resets_stale_cycle_flags_and_verifies_token() -> None:
    code = _apply_option_code(4, render_token="tok", option_expression="({})")

    assert 'key.indexOf("__flagIn") === 0' in code
    assert "record.unwedged = unwedge(instance);" in code
    assert "if (ownsToken(instance))" in code
    assert "setOption returned but the token series is absent" in code
    assert 'window.addEventListener("error"' in code
    assert "instance.setOption = function" not in code


def test_probe_reports_stuck_flags_and_last_error() -> None:
    from l2shock.ui.echarts import _live_state_probe_code

    probe = _live_state_probe_code(4, x_axis_index=0)

    assert "wedged_flags:" in probe
    assert "last_window_error:" in probe
    assert "instance[flagKey] = false" not in probe  # probe stays read-only


@pytest.mark.asyncio
async def test_failed_apply_fails_fast_with_browser_error() -> None:
    chart = _ProbeChart()
    publication = set_echart_options(chart, _option())
    state = _good_state("0" * 32)
    state["wedged_flags"] = []
    state["last_window_error"] = "TypeError: x is undefined"
    state["apply"] = {
        "token": publication.render_token,
        "state": "failed",
        "error": "setOption returned but the token series is absent",
        "unwedged": ["__flagInMainProcess"],
    }
    chart.client.states = [state]

    acknowledged, reason = await confirm_echart_render_identity(chart, publication)

    assert acknowledged is False
    assert "token series is absent" in reason
    assert "reset stale ECharts flags=__flagInMainProcess" in reason
    assert "last browser error=TypeError: x is undefined" in reason
    assert len(chart.client.codes) == 1

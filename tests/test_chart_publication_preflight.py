# tests/test_chart_publication_preflight.py
"""Chart publication validation before mutation and final ownership checks."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

import l2shock.ui.chart_interactions as interactions
import l2shock.ui.echarts as echarts

_OLD_TOKEN = "a" * 32
_NEW_TOKEN = "b" * 32


class _Chart:
    """Widget-shaped double without a real NiceGUI client."""

    def __init__(self) -> None:
        self.previous_option = {
            "xAxis": [{"type": "category", "data": ["old"]}],
            "yAxis": [{"type": "value"}],
            "series": [],
        }
        self._props = {"options": self.previous_option}
        self._l2shock_render_token = _OLD_TOKEN
        self._l2shock_acknowledged_render_token = _OLD_TOKEN
        self.updates = 0
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def update(self) -> None:
        self.updates += 1

    def run_chart_method(
        self,
        method: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.calls.append((method, args, kwargs))


def _option(count: int = 1) -> dict[str, Any]:
    return {
        "animation": False,
        "xAxis": [{"type": "category", "data": ["new"]}],
        "yAxis": [{"type": "value"}],
        "series": [],
        "l2shockChartMetadata": {
            "visible_bar_count": count,
        },
    }


def _assert_old_widget_untouched(chart: _Chart) -> None:
    assert chart._props["options"] is chart.previous_option
    assert chart._l2shock_render_token == _OLD_TOKEN
    assert chart._l2shock_acknowledged_render_token == _OLD_TOKEN
    assert chart.updates == 0
    assert chart.calls == []


@pytest.mark.parametrize(
    "bad_value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        object(),
    ],
    ids=["nan", "positive-infinity", "negative-infinity", "object"],
)
def test_invalid_json_leaves_previous_widget_untouched(
    bad_value: object,
) -> None:
    chart = _Chart()
    option = _option()
    option["unsupported_value"] = bad_value

    with pytest.raises(
        echarts.EChartPublicationError,
        match="strict JSON",
    ):
        echarts.set_echart_options(chart, option)

    _assert_old_widget_untouched(chart)

    # Identity attachment belongs to the isolated copy, not the caller.
    assert "l2shockPublication" not in option
    assert option["series"] == []


def test_missing_chart_method_leaves_previous_widget_untouched() -> None:
    chart = _Chart()
    chart.run_chart_method = None

    with pytest.raises(
        echarts.EChartPublicationError,
        match="run_chart_method",
    ):
        echarts.set_echart_options(chart, _option())

    _assert_old_widget_untouched(chart)


def test_valid_publication_keeps_existing_publication_contract() -> None:
    chart = _Chart()
    option = _option()
    original = copy.deepcopy(option)

    publication = echarts.set_echart_options(chart, option)

    assert isinstance(publication, echarts.EChartPublication)
    assert publication.expected_category_count == 1
    assert chart.updates == 1
    assert chart._props["options"] is not option
    assert option == original
    assert chart._l2shock_render_token == publication.render_token
    assert chart._l2shock_acknowledged_render_token == ""

    assert [call[0] for call in chart.calls] == [
        ":setOption",
        "resize",
    ]
    assert chart.calls[0][1][1] == ("({notMerge:true,lazyUpdate:false})")

    stored = chart._props["options"]
    assert stored["l2shockPublication"]["render_token"] == (publication.render_token)
    assert any(
        item.get("name")
        == (echarts.ECHART_RENDER_TOKEN_SERIES_PREFIX + publication.render_token)
        for item in stored["series"]
    )


def _controller(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    interactions.AnalysisChartController,
    _Chart,
    interactions.AnalysisChartCommit,
    list[str],
]:
    chart = _Chart()
    controller = interactions.AnalysisChartController(chart)

    previous = interactions.AnalysisChartCommit(
        owner_id="previous-owner",
        publication=echarts.EChartPublication(
            render_token=_OLD_TOKEN,
            expected_category_count=1,
        ),
        visible_category_count=1,
    )
    controller._commit = previous

    events: list[str] = []

    def publish(
        selected_chart: _Chart,
        option: dict[str, Any],
    ) -> echarts.EChartPublication:
        assert selected_chart is chart
        events.append("publish")
        selected_chart._props["options"] = option
        selected_chart._l2shock_render_token = _NEW_TOKEN
        selected_chart._l2shock_acknowledged_render_token = ""
        return echarts.EChartPublication(
            render_token=_NEW_TOKEN,
            expected_category_count=1,
        )

    async def confirm(
        selected_chart: _Chart,
        publication: echarts.EChartPublication,
    ) -> tuple[bool, str]:
        assert selected_chart is chart
        events.append("confirm")
        selected_chart._l2shock_acknowledged_render_token = publication.render_token
        return True, ""

    async def install(
        selected_chart: _Chart,
        *,
        expected_render_token: str,
        gap_px: int,
    ) -> bool:
        assert selected_chart is chart
        assert expected_render_token == _NEW_TOKEN
        assert gap_px == 50
        events.append("install")
        return True

    monkeypatch.setattr(
        interactions,
        "set_echart_options",
        publish,
    )
    monkeypatch.setattr(
        interactions,
        "confirm_echart_render_identity",
        confirm,
    )
    monkeypatch.setattr(
        interactions,
        "install_analysis_gapped_crosshair",
        install,
    )
    monkeypatch.setattr(
        interactions,
        "acknowledged_render_token",
        lambda selected: (selected._l2shock_acknowledged_render_token),
    )

    return controller, chart, previous, events


@pytest.mark.asyncio
async def test_conflicting_restore_modes_preserve_previous_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, chart, previous, events = _controller(monkeypatch)

    viewport = interactions.AnalysisChartTemporalViewport(
        left_edge_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        visible_duration_seconds=60,
        source_timeframe_seconds=1,
    )
    generation = controller._request_generation

    with pytest.raises(
        interactions.AnalysisChartInteractionError,
        match="only one viewport",
    ):
        await controller.publish(
            _option(),
            owner_id="replacement-owner",
            preserve_viewport=True,
            shock_time_viewport=viewport,
        )

    assert controller.commit is previous
    assert controller._request_generation == generation
    assert events == []
    _assert_old_widget_untouched(chart)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["temporal_viewport", "shock_time_viewport"],
)
async def test_wrong_viewport_type_preserves_previous_commit(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    controller, chart, previous, events = _controller(monkeypatch)
    generation = controller._request_generation

    with pytest.raises(TypeError, match=field):
        await controller.publish(
            _option(),
            owner_id="replacement-owner",
            preserve_viewport=False,
            **{field: object()},
        )

    assert controller.commit is previous
    assert controller._request_generation == generation
    assert events == []
    _assert_old_widget_untouched(chart)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {},
        {"visible_bar_count": True},
        {"visible_bar_count": -1},
        {"visible_bar_count": 1.5},
        {"visible_bar_count": "1"},
    ],
    ids=[
        "missing-metadata",
        "missing-count",
        "boolean-count",
        "negative-count",
        "fractional-count",
        "text-count",
    ],
)
async def test_bad_metadata_is_rejected_before_publication(
    monkeypatch: pytest.MonkeyPatch,
    metadata: object,
) -> None:
    controller, chart, previous, events = _controller(monkeypatch)
    option = _option()
    option["l2shockChartMetadata"] = metadata
    generation = controller._request_generation

    with pytest.raises(interactions.AnalysisChartInteractionError):
        await controller.publish(
            option,
            owner_id="replacement-owner",
            preserve_viewport=False,
        )

    assert controller.commit is previous
    assert controller._request_generation == generation
    assert events == []
    _assert_old_widget_untouched(chart)


@pytest.mark.asyncio
async def test_valid_controller_publication_commits_new_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, _chart, previous, events = _controller(monkeypatch)

    result = await controller.publish(
        _option(),
        owner_id="replacement-owner",
        preserve_viewport=False,
    )

    assert result is not None
    assert result is controller.commit
    assert result is not previous
    assert result.owner_id == "replacement-owner"
    assert result.publication.render_token == _NEW_TOKEN
    assert result.visible_category_count == 1
    assert events == ["publish", "confirm", "install"]


@pytest.mark.asyncio
async def test_lost_acknowledgement_during_postprocessing_is_not_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, chart, _previous, events = _controller(monkeypatch)

    async def supersede(
        selected_chart: _Chart,
        *,
        expected_render_token: str,
        gap_px: int,
    ) -> bool:
        assert selected_chart is chart
        assert expected_render_token == _NEW_TOKEN
        assert gap_px == 50
        events.append("supersede")
        selected_chart._l2shock_render_token = "c" * 32
        selected_chart._l2shock_acknowledged_render_token = ""
        return False

    monkeypatch.setattr(
        interactions,
        "install_analysis_gapped_crosshair",
        supersede,
    )

    result = await controller.publish(
        _option(),
        owner_id="replacement-owner",
        preserve_viewport=False,
    )

    assert result is None
    assert controller.commit is None
    assert events == ["publish", "confirm", "supersede"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        object(),
    ],
    ids=["nan", "positive-infinity", "negative-infinity", "object"],
)
async def test_controller_invalid_json_preserves_previous_commit_and_widget(
    monkeypatch: pytest.MonkeyPatch,
    bad_value: object,
) -> None:
    controller, chart, previous, events = _controller(monkeypatch)
    option = _option()
    option["unsupported_value"] = bad_value
    generation = controller._request_generation

    with pytest.raises(
        echarts.EChartPublicationError,
        match="strict JSON",
    ):
        await controller.publish(
            option,
            owner_id="replacement-owner",
            preserve_viewport=False,
        )

    assert controller.commit is previous
    assert controller._request_generation == generation
    assert events == []
    _assert_old_widget_untouched(chart)
    assert "l2shockPublication" not in option
    assert option["series"] == []


@pytest.mark.asyncio
async def test_controller_invalid_publication_metadata_preserves_previous_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, chart, previous, events = _controller(monkeypatch)
    option = _option()
    option["l2shockPublication"] = "not-an-object"
    generation = controller._request_generation

    with pytest.raises(
        echarts.EChartPublicationError,
        match="metadata must be an object",
    ):
        await controller.publish(
            option,
            owner_id="replacement-owner",
            preserve_viewport=False,
        )

    assert controller.commit is previous
    assert controller._request_generation == generation
    assert events == []
    _assert_old_widget_untouched(chart)


@pytest.mark.asyncio
async def test_controller_missing_chart_method_preserves_previous_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, chart, previous, events = _controller(monkeypatch)
    chart.run_chart_method = None
    generation = controller._request_generation

    with pytest.raises(
        echarts.EChartPublicationError,
        match="run_chart_method",
    ):
        await controller.publish(
            _option(),
            owner_id="replacement-owner",
            preserve_viewport=False,
        )

    assert controller.commit is previous
    assert controller._request_generation == generation
    assert events == []
    _assert_old_widget_untouched(chart)


def test_option_preflight_does_not_mutate_caller_or_attach_identity() -> None:
    option = _option()
    original = copy.deepcopy(option)

    assert echarts.validate_echart_option(option) is None
    assert option == original
    assert "l2shockPublication" not in option
    assert option["series"] == []

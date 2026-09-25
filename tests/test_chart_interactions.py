from __future__ import annotations
from datetime import datetime, timezone
import json

from l2shock.ui.chart_interactions import (
    capture_shock_time_viewport,
    restore_shock_time_viewport,
)
from l2shock.ui.analysis_chart import ChartNavigationWindow
from l2shock.ui.chart_interactions import (
    AnalysisChartCommit,
    AnalysisChartInteractionError,
    AnalysisChartTemporalViewport,
    AnalysisChartViewport,
    capture_analysis_chart_temporal_viewport,
    emphasize_analysis_chart_window,
    restore_analysis_chart_temporal_viewport,
)
from l2shock.ui.echarts import (
    EChartPublication,
    EChartPublicationError,
    coerce_echart_option,
)


def test_viewport_accepts_ordered_percentages() -> None:
    viewport = AnalysisChartViewport(
        start_percent=12.5,
        end_percent=87.5,
    )

    assert viewport.start_percent == 12.5
    assert viewport.end_percent == 87.5


def test_viewport_rejects_reversed_percentages() -> None:
    try:
        AnalysisChartViewport(
            start_percent=80,
            end_percent=20,
        )
    except AnalysisChartInteractionError:
        pass
    else:
        raise AssertionError("Reversed viewport was accepted")


def test_publication_requires_canonical_render_token() -> None:
    publication = EChartPublication(
        render_token="a" * 32,
        expected_category_count=400,
    )

    assert publication.render_token == "a" * 32
    assert publication.expected_category_count == 400


def test_publication_rejects_uppercase_token() -> None:
    try:
        EChartPublication(
            render_token="A" * 32,
            expected_category_count=1,
        )
    except EChartPublicationError:
        pass
    else:
        raise AssertionError("Uppercase render token was accepted")


def test_chart_commit_owns_analysis_and_category_count() -> None:
    publication = EChartPublication(
        render_token="b" * 32,
        expected_category_count=100,
    )

    commit = AnalysisChartCommit(
        owner_id="analysis-id",
        publication=publication,
        visible_category_count=100,
    )

    assert commit.owner_id == "analysis-id"
    assert commit.publication is publication
    assert commit.visible_category_count == 100


def test_navigation_window_requires_candidate_inside_zoom_window() -> None:
    window = ChartNavigationWindow(
        start_index=5,
        end_index=20,
        candidate_start_index=10,
        candidate_end_index=15,
    )

    assert window.start_index == 5
    assert window.end_index == 20

    try:
        ChartNavigationWindow(
            start_index=5,
            end_index=20,
            candidate_start_index=2,
            candidate_end_index=15,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("Candidate outside navigation window was accepted")


def test_option_coercion_is_isolated_from_caller_mutation() -> None:
    source = {
        "xAxis": {
            "type": "category",
            "data": ["a", "b"],
        },
        "yAxis": {
            "type": "value",
        },
        "series": {
            "name": "value",
            "type": "line",
            "data": [1, 2],
        },
    }

    safe = coerce_echart_option(source)

    assert isinstance(safe["series"], list)
    assert safe["series"][0]["data"] == [1, 2]

    source["xAxis"]["data"].append("c")
    source["series"]["data"].append(3)

    assert safe["xAxis"]["data"] == ["a", "b"]
    assert safe["series"][0]["data"] == [1, 2]


class _FakeAcknowledgedChart:
    def __init__(self, token: str) -> None:
        self._l2shock_acknowledged_render_token = token
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def run_chart_method(
        self,
        method: str,
        *args: object,
        **_kwargs: object,
    ) -> None:
        self.calls.append((method, args))


async def test_selected_window_emphasis_updates_all_panels() -> None:
    publication = EChartPublication(
        render_token="c" * 32,
        expected_category_count=100,
    )
    commit = AnalysisChartCommit(
        owner_id="analysis-id",
        publication=publication,
        visible_category_count=100,
    )
    chart = _FakeAcknowledgedChart(publication.render_token)
    window = ChartNavigationWindow(
        start_index=5,
        end_index=20,
        candidate_start_index=10,
        candidate_end_index=15,
    )

    emphasized = await emphasize_analysis_chart_window(
        chart,
        window,
        commit=commit,
    )

    assert emphasized is True
    assert chart.calls
    assert chart.calls[0][0] == ":setOption"

    expression = str(chart.calls[0][1][0])

    for panel_index in range(5):
        assert f"l2shock-selected-focus-{panel_index}" in expression


class _TemporalChart:
    def __init__(
        self,
        *,
        token: str,
        timestamps: list[str],
        bar_duration_seconds: int,
        start_percent: float = 0.0,
        end_percent: float = 100.0,
    ) -> None:
        self._l2shock_render_token = token
        self._l2shock_acknowledged_render_token = token
        self._props = {
            "options": {
                "dataZoom": [
                    {
                        "start": start_percent,
                        "end": end_percent,
                    }
                ],
                "l2shockChartMetadata": {
                    "visible_bar_count": len(timestamps),
                    "visible_start_times_utc": timestamps,
                    "bar_duration_seconds": bar_duration_seconds,
                },
            }
        }
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def run_chart_method(
        self,
        method: str,
        *args: object,
        **_kwargs: object,
    ):
        self.calls.append((method, args))

        if method == "getOption":
            return {
                "dataZoom": [
                    {
                        "start": self._props["options"]["dataZoom"][0]["start"],
                        "end": self._props["options"]["dataZoom"][0]["end"],
                    }
                ]
            }

        return None


async def test_temporal_viewport_capture_owns_utc_left_edge() -> None:
    token = "d" * 32
    chart = _TemporalChart(
        token=token,
        timestamps=[
            "2026-09-02T12:00:00Z",
            "2026-09-02T12:05:00Z",
            "2026-09-02T12:10:00Z",
            "2026-09-02T12:15:00Z",
            "2026-09-02T12:20:00Z",
        ],
        bar_duration_seconds=300,
        start_percent=25.0,
        end_percent=75.0,
    )

    viewport = await capture_analysis_chart_temporal_viewport(chart)

    assert viewport is not None
    assert viewport.left_edge_utc == datetime(
        2026,
        9,
        2,
        12,
        5,
        tzinfo=timezone.utc,
    )
    assert viewport.visible_duration_seconds == 900.0
    assert viewport.source_timeframe_seconds == 300


async def test_temporal_restore_maps_timestamp_into_new_categories() -> None:
    token = "e" * 32
    chart = _TemporalChart(
        token=token,
        timestamps=[
            "2026-09-02T12:00:00Z",
            "2026-09-02T12:01:00Z",
            "2026-09-02T12:02:00Z",
            "2026-09-02T12:03:00Z",
            "2026-09-02T12:04:00Z",
            "2026-09-02T12:05:00Z",
            "2026-09-02T12:06:00Z",
            "2026-09-02T12:07:00Z",
            "2026-09-02T12:08:00Z",
            "2026-09-02T12:09:00Z",
            "2026-09-02T12:10:00Z",
        ],
        bar_duration_seconds=60,
    )

    restored = await restore_analysis_chart_temporal_viewport(
        chart,
        AnalysisChartTemporalViewport(
            left_edge_utc=datetime(
                2026,
                9,
                2,
                12,
                3,
                tzinfo=timezone.utc,
            ),
            visible_duration_seconds=300.0,
            source_timeframe_seconds=300,
        ),
        expected_render_token=token,
    )

    assert restored is True

    dispatch_calls = [call for call in chart.calls if call[0] == ":dispatchAction"]

    assert dispatch_calls

    expression = str(dispatch_calls[-1][1][0])

    assert '"startValue":3' in expression
    assert '"endValue":7' in expression


async def test_temporal_restore_rejects_superseded_generation() -> None:
    chart = _TemporalChart(
        token="f" * 32,
        timestamps=["2026-09-02T12:00:00Z"],
        bar_duration_seconds=60,
    )
    chart._l2shock_acknowledged_render_token = "0" * 32

    restored = await restore_analysis_chart_temporal_viewport(
        chart,
        AnalysisChartTemporalViewport(
            left_edge_utc=datetime(
                2026,
                9,
                2,
                12,
                tzinfo=timezone.utc,
            ),
            visible_duration_seconds=60.0,
            source_timeframe_seconds=60,
        ),
        expected_render_token="f" * 32,
    )

    assert restored is False
    assert not any(method == ":dispatchAction" for method, _args in chart.calls)


def _time_axis_chart(
    *,
    token: str,
    start_percent: float = 0.0,
    end_percent: float = 100.0,
) -> _TemporalChart:
    chart = _TemporalChart(
        token=token,
        timestamps=["2026-09-02T12:00:00Z"],
        bar_duration_seconds=60,
        start_percent=start_percent,
        end_percent=end_percent,
    )
    chart._props["options"]["xAxis"] = [
        {
            "type": "time",
            "min": "2026-09-02T12:00:00Z",
            "max": "2026-09-02T12:20:00Z",
        }
        for _ in range(5)
    ]
    return chart


async def test_shock_time_capture_uses_live_browser_percentages() -> None:
    chart = _time_axis_chart(
        token="1" * 32,
        start_percent=25,
        end_percent=75,
    )

    captured = await capture_shock_time_viewport(chart)

    assert captured is not None
    assert captured.left_edge_utc == datetime(2026, 9, 2, 12, 5, tzinfo=timezone.utc)
    assert captured.visible_duration_seconds == 600
    assert captured.source_timeframe_seconds == 60


async def test_shock_time_restore_dispatches_utc_milliseconds() -> None:
    token = "2" * 32
    chart = _time_axis_chart(token=token)
    viewport = AnalysisChartTemporalViewport(
        left_edge_utc=datetime(2026, 9, 2, 12, 5, tzinfo=timezone.utc),
        visible_duration_seconds=600,
        source_timeframe_seconds=300,
    )

    assert await restore_shock_time_viewport(
        chart,
        viewport,
        expected_render_token=token,
    )

    dispatches = [args for method, args in chart.calls if method == ":dispatchAction"]
    assert len(dispatches) == 1

    action = json.loads(str(dispatches[0][0])[1:-1])
    zoom = action["batch"][0]

    assert zoom["dataZoomIndex"] == 0
    assert zoom["startValue"] == int(
        datetime(2026, 9, 2, 12, 5, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert zoom["endValue"] == int(
        datetime(2026, 9, 2, 12, 15, tzinfo=timezone.utc).timestamp() * 1000
    )


async def test_shock_time_restore_rejects_superseded_browser_token() -> None:
    token = "3" * 32
    chart = _time_axis_chart(token=token)
    chart._l2shock_acknowledged_render_token = "4" * 32

    restored = await restore_shock_time_viewport(
        chart,
        AnalysisChartTemporalViewport(
            left_edge_utc=datetime(2026, 9, 2, 12, 5, tzinfo=timezone.utc),
            visible_duration_seconds=600,
            source_timeframe_seconds=60,
        ),
        expected_render_token=token,
    )

    assert restored is False
    assert not any(method == ":dispatchAction" for method, _args in chart.calls)


async def test_shock_time_capture_rejects_category_axis() -> None:
    chart = _time_axis_chart(token="5" * 32)
    chart._props["options"]["xAxis"][0]["type"] = "category"

    assert await capture_shock_time_viewport(chart) is None


async def test_shock_time_restore_clips_left_edge_to_new_bounds() -> None:
    token = "6" * 32
    chart = _time_axis_chart(token=token)

    restored = await restore_shock_time_viewport(
        chart,
        AnalysisChartTemporalViewport(
            left_edge_utc=datetime(2026, 9, 2, 12, 17, tzinfo=timezone.utc),
            visible_duration_seconds=600,
            source_timeframe_seconds=5,
        ),
        expected_render_token=token,
    )

    assert restored is True

    dispatches = [args for method, args in chart.calls if method == ":dispatchAction"]
    assert len(dispatches) == 1

    action = json.loads(str(dispatches[0][0])[1:-1])
    zoom = action["batch"][0]

    # The original 10-minute span fits, but 12:17–12:27 does not.
    # Preserve its duration and move its left edge to 12:10.
    assert zoom["startValue"] == int(
        datetime(2026, 9, 2, 12, 10, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert zoom["endValue"] == int(
        datetime(2026, 9, 2, 12, 20, tzinfo=timezone.utc).timestamp() * 1000
    )


async def test_shock_time_restore_clips_span_to_shorter_new_source() -> None:
    token = "7" * 32
    chart = _time_axis_chart(token=token)

    for axis in chart._props["options"]["xAxis"]:
        axis["min"] = "2026-09-02T12:07:00Z"
        axis["max"] = "2026-09-02T12:12:00Z"

    restored = await restore_shock_time_viewport(
        chart,
        AnalysisChartTemporalViewport(
            left_edge_utc=datetime(2026, 9, 2, 12, 5, tzinfo=timezone.utc),
            visible_duration_seconds=600,
            source_timeframe_seconds=60,
        ),
        expected_render_token=token,
    )

    assert restored is True

    dispatches = [args for method, args in chart.calls if method == ":dispatchAction"]
    assert len(dispatches) == 1

    action = json.loads(str(dispatches[0][0])[1:-1])
    zoom = action["batch"][0]

    assert zoom["startValue"] == int(
        datetime(2026, 9, 2, 12, 7, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert zoom["endValue"] == int(
        datetime(2026, 9, 2, 12, 12, tzinfo=timezone.utc).timestamp() * 1000
    )

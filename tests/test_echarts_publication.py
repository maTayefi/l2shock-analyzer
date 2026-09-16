from __future__ import annotations

from typing import Any

from l2shock.ui.echarts import (
    ECHART_RENDER_TOKEN_SERIES_PREFIX,
    confirm_echart_render_identity,
    set_echart_options,
)


class _FakeChart:
    def __init__(
        self,
        *,
        option: dict[str, Any] | None = None,
        width: float = 1200.0,
        height: float = 800.0,
    ) -> None:
        self._props: dict[str, Any] = {}
        if option is not None:
            self._props["options"] = option

        self._width = width
        self._height = height
        self.updated = 0
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def update(self) -> None:
        self.updated += 1

    def run_chart_method(
        self,
        method: str,
        *args: object,
        **_kwargs: object,
    ) -> object:
        self.calls.append((method, args))

        if method == "getOption":
            value: object = self._props.get("options", {})
        elif method == "getWidth":
            value = self._width
        elif method == "getHeight":
            value = self._height
        else:
            return None

        async def _awaitable() -> object:
            return value

        return _awaitable()


def test_complete_publication_adds_hidden_render_identity() -> None:
    chart = _FakeChart()

    publication = set_echart_options(
        chart,
        {
            "xAxis": [
                {
                    "type": "category",
                    "data": ["a", "b", "c"],
                }
            ],
            "yAxis": [
                {
                    "type": "value",
                }
            ],
            "series": [
                {
                    "name": "real",
                    "type": "line",
                    "data": [1, 2, 3],
                }
            ],
        },
    )

    assert chart.updated == 1
    assert publication.expected_category_count == 3

    option = chart._props["options"]
    names = {
        str(item.get("name")) for item in option["series"] if isinstance(item, dict)
    }

    assert "real" in names
    assert (ECHART_RENDER_TOKEN_SERIES_PREFIX + publication.render_token) in names

    assert chart._l2shock_render_token == publication.render_token
    assert chart._l2shock_acknowledged_render_token == ""

    assert any(method == ":setOption" for method, _args in chart.calls)


def test_republication_replaces_old_hidden_identity() -> None:
    chart = _FakeChart()

    first = set_echart_options(
        chart,
        {
            "xAxis": [{"type": "category", "data": ["a"]}],
            "yAxis": [{"type": "value"}],
            "series": [],
        },
    )

    second = set_echart_options(
        chart,
        chart._props["options"],
    )

    assert first.render_token != second.render_token

    names = [
        str(item.get("name"))
        for item in chart._props["options"]["series"]
        if isinstance(item, dict)
        and str(item.get("name") or "").startswith(ECHART_RENDER_TOKEN_SERIES_PREFIX)
    ]

    assert names == [ECHART_RENDER_TOKEN_SERIES_PREFIX + second.render_token]


async def test_render_acknowledgement_rejects_infinite_layout() -> None:
    option = {
        "xAxis": [
            {
                "type": "category",
                "data": ["one"],
            }
        ],
        "yAxis": [{"type": "value"}],
        "series": [],
    }

    chart = _FakeChart(
        option=option,
        width=float("inf"),
        height=700.0,
    )

    publication = set_echart_options(
        chart,
        option,
    )

    acknowledged, reason = await confirm_echart_render_identity(
        chart,
        publication,
        attempts=1,
        retry_delay_seconds=0.0,
    )

    assert acknowledged is False
    assert "layout" in reason.lower()

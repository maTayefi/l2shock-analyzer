# tests/test_l2_view_batch3a.py
"""Regression coverage for Analysis dependency extraction and coarsening."""

from __future__ import annotations

import ast
import copy
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path

import pytest

from l2shock.analysis.l2_view_stream import (
    MAX_L2_VIEW_BARS,
    L2ViewBar,
    L2ViewError,
    L2ViewProjection,
    L2ViewRequest,
    coarsen_l2_view,
)
from l2shock.ui.display_timezone import (
    DisplayTimezoneError,
    display_timezone_formatters,
    with_display_timezone,
)
from l2shock.ui.l2_view_warning_style import WARNING_REGION_COLOR

_ROOT = Path(__file__).resolve().parents[1]
_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _projection() -> L2ViewProjection:
    def candle(value: Fraction) -> tuple[Fraction, ...]:
        return value, value, value, value

    request = L2ViewRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_T0,
        requested_end_utc=_T0 + timedelta(seconds=2),
    )

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
        request=request,
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


def _option() -> dict:
    return {
        "xAxis": [
            {
                "type": "time",
                "min": _T0.isoformat(),
                "max": (_T0 + timedelta(seconds=3)).isoformat(),
                "axisLabel": {"show": True},
                "axisPointer": {
                    "label": {"show": True},
                },
            }
            for _ in range(5)
        ],
        "tooltip": {
            "trigger": "axis",
        },
        "series": [
            {
                "id": "example",
                "data": [
                    [_T0.isoformat(), 2.0, 2.0, 2.0, 2.0],
                ],
            }
        ],
        "l2shockChartMetadata": {
            "bar_duration_seconds": 1,
            "visible_bar_count": 3,
        },
    }


def _absolute_imports(path: Path) -> set[str]:
    tree = ast.parse(
        path.read_text(encoding="utf-8"),
        filename=str(path),
    )
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)

    return names


def test_same_timeframe_applies_new_budget_without_mutation() -> None:
    source = _projection()

    result = coarsen_l2_view(
        source,
        1,
        max_bars=10,
    )

    assert result.max_bars == 10
    assert source.max_bars == 1200
    assert result.bars == source.bars
    assert result.input_id == source.input_id
    assert result.request == source.request


def test_same_timeframe_rejects_budget_smaller_than_bar_count() -> None:
    with pytest.raises(
        L2ViewError,
        match="Maximum viewing bars",
    ):
        coarsen_l2_view(
            _projection(),
            1,
            max_bars=2,
        )


def test_same_timeframe_and_budget_can_reuse_projection() -> None:
    source = _projection()

    assert coarsen_l2_view(source, 1) is source


@pytest.mark.parametrize(
    "budget",
    [
        True,
        False,
        0,
        -1,
        MAX_L2_VIEW_BARS + 1,
        1.5,
        "10",
    ],
)
def test_direct_coarsening_rejects_invalid_budget(budget: object) -> None:
    with pytest.raises(L2ViewError):
        coarsen_l2_view(
            _projection(),
            5,
            max_bars=budget,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "timeframe",
    [
        True,
        False,
        0,
        -1,
        2,
        1.5,
        "5",
        None,
    ],
)
def test_direct_coarsening_rejects_invalid_timeframe(
    timeframe: object,
) -> None:
    with pytest.raises(L2ViewError):
        coarsen_l2_view(
            _projection(),
            timeframe,  # type: ignore[arg-type]
            max_bars=10,
        )


def test_coarsening_preserves_source_identity_and_quality_counts() -> None:
    source = _projection()

    result = coarsen_l2_view(
        source,
        5,
        max_bars=10,
    )

    assert result.timeframe_seconds == 5
    assert len(result.bars) == 1
    assert result.max_bars == 10
    assert result.input_id == source.input_id
    assert result.request == source.request
    assert result.start_utc == source.start_utc
    assert result.end_utc_exclusive == source.end_utc_exclusive
    assert result.usable_l2_seconds == source.usable_l2_seconds
    assert result.unusable_l2_seconds == source.unusable_l2_seconds
    assert result.partial_market_seconds == source.partial_market_seconds
    assert len(source.bars) == 3


def test_display_timezone_keeps_utc_coordinates_and_input_unchanged() -> None:
    source = _option()
    original = copy.deepcopy(source)

    result = with_display_timezone(
        source,
        "Asia/Tehran",
    )

    assert source == original
    assert result["series"] == original["series"]
    assert result["l2shockChartMetadata"] == original["l2shockChartMetadata"]

    for before, after in zip(
        original["xAxis"],
        result["xAxis"],
        strict=True,
    ):
        assert after["type"] == before["type"]
        assert after["min"] == before["min"]
        assert after["max"] == before["max"]
        assert after["axisLabel"]["show"] is True
        assert after["axisPointer"]["label"]["show"] is True
        assert '"Asia/Tehran"' in after["axisLabel"][":formatter"]
        assert '"Asia/Tehran"' in after["axisPointer"]["label"][":formatter"]

    assert result["tooltip"]["trigger"] == "axis"
    assert '"Asia/Tehran"' in result["tooltip"][":formatter"]


def test_display_timezone_templates_have_no_unresolved_placeholders() -> None:
    formatters = display_timezone_formatters(
        "Asia/Tehran",
    )

    assert set(formatters) == {
        "axis_label",
        "axis_pointer",
        "tooltip",
    }

    for javascript in formatters.values():
        assert javascript.startswith("function")
        assert "__TZ__" not in javascript
        assert "__PARTS__" not in javascript


@pytest.mark.parametrize(
    "name",
    [
        "",
        None,
        "Mars/Olympus_Mons",
        "Asia/Tehran'); alert(1); //",
    ],
)
def test_display_timezone_rejects_unknown_or_unsafe_names(
    name: object,
) -> None:
    with pytest.raises(DisplayTimezoneError):
        with_display_timezone(
            _option(),
            name,  # type: ignore[arg-type]
        )


def test_display_timezone_leaves_non_time_axes_unchanged() -> None:
    source = {
        "xAxis": [
            {
                "type": "category",
                "data": ["first", "second"],
            }
        ],
    }
    original = copy.deepcopy(source)

    result = with_display_timezone(
        source,
        "Asia/Tehran",
    )

    assert source == original
    assert result == original


def test_timezone_module_does_not_import_legacy_shock_module() -> None:
    imports = _absolute_imports(_ROOT / "l2shock/ui/display_timezone.py")

    assert "l2shock.ui.shock_annotation_visibility" not in imports


def test_l2_chart_builder_uses_independent_warning_style() -> None:
    imports = _absolute_imports(_ROOT / "l2shock/ui/l2_view_chart_options.py")

    assert "l2shock.ui.l2_view_warning_style" in imports
    assert "l2shock.ui.shock_warning_regions" not in imports


def test_warning_style_retains_existing_color() -> None:
    assert WARNING_REGION_COLOR == "rgba(220, 38, 38, 0.14)"

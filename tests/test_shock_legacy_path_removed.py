# tests/test_shock_legacy_path_removed.py
from __future__ import annotations

from pathlib import Path

import l2shock.ui.shock_inspection as shock_inspection

ROOT = Path(__file__).resolve().parents[1]


def test_one_second_legacy_publication_path_is_gone() -> None:
    source = (ROOT / "l2shock/ui/tab_shock_review.py").read_text(encoding="utf-8")

    assert "_select_row_one_second_legacy" not in source
    assert "publish_shock_selection" not in source
    assert not hasattr(shock_inspection, "publish_shock_selection")
    assert "publish_shock_selection" not in shock_inspection.__all__


def test_row_selection_still_uses_bounded_viewport() -> None:
    source = (ROOT / "l2shock/ui/tab_shock_review.py").read_text(encoding="utf-8")

    assert 'table.on("rowClick", _select_row)' in source
    assert "await _show_bounded_view()" in source


def test_one_second_category_chart_path_is_retired() -> None:
    import l2shock.ui.shock_chart_options as chart_options
    from l2shock.ui.shock_inspection import ShockInspectionModel

    assert not hasattr(ShockInspectionModel, "select")
    assert not hasattr(shock_inspection, "ShockInspectionSelection")
    assert not hasattr(chart_options, "build_shock_chart_options")
    assert not hasattr(chart_options, "ShockChartOptionsError")
    assert not hasattr(chart_options, "PriceBySecond")

    stale = sorted(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "l2shock").rglob("*.py")
        if "__pycache__" not in path.parts
        and (
            "build_shock_chart_options" in path.read_text(encoding="utf-8")
            or "ShockInspectionSelection" in path.read_text(encoding="utf-8")
        )
    )
    assert stale == []


def test_one_second_area_window_builder_is_retired() -> None:
    from datetime import datetime, timezone

    import l2shock.analysis.shock_window as shock_window

    assert not hasattr(shock_window, "build_shock_area_window")
    assert not hasattr(shock_window, "ShockAreaWindow")
    assert shock_window.__all__ == ["ShockWindowError", "ShockWindowSecond"]

    invalid = shock_window.ShockWindowSecond(
        dataset_index=7,
        timestamp_utc=datetime(2026, 9, 24, 2, 7, tzinfo=timezone.utc),
        valid_l2=False,
        bid=None,
        ask=None,
        total=None,
        delta=None,
    ).to_dict()

    assert invalid["valid_l2"] is False
    assert set(invalid["plot"].values()) == {None}
    assert set(invalid["exact"].values()) == {None}

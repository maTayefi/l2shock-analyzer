from __future__ import annotations

import ast
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from l2shock.ui.shock_annotation_visibility import with_shock_annotation_visibility
from l2shock.ui.shock_view_bars import ShockViewBar, ShockViewProjection
from l2shock.ui.shock_view_chart_options import (
    ShockViewRankedArea,
    build_shock_view_chart_options,
)

ROOT = Path(__file__).resolve().parents[1]
_START = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)


def _options():
    flat = (1.0, 1.0, 1.0, 1.0)
    bars = tuple(
        ShockViewBar(
            start_utc=_START + timedelta(seconds=i),
            end_utc_exclusive=_START + timedelta(seconds=i + 1),
            first_source_position=i,
            last_source_position_exclusive=i + 1,
            first_dataset_index=100 + i,
            last_dataset_index=100 + i,
            valid_l2=True,
            bid=flat,
            ask=flat,
            total=(2.0, 2.0, 2.0, 2.0),
            delta=(0.0, 0.0, 0.0, 0.0),
        )
        for i in range(60)
    )
    projection = ShockViewProjection(
        timeframe_seconds=1,
        requested_max_bars=1200,
        source_start_utc=_START,
        source_end_utc_exclusive=_START + timedelta(seconds=60),
        bars=bars,
    )
    return build_shock_view_chart_options(
        projection,
        b_first_dataset_index=110,
        b_last_dataset_index=112,
        representative_b_dataset_index=111,
        representative_c_dataset_index=120,
        selected_rank=3,
        ranked_areas=(ShockViewRankedArea(1, 130, 131, 130, 140),),
    )


def _strip(option, *, lines: bool, bands: bool):
    result = copy.deepcopy(option)
    for series in result["series"]:
        if lines:
            series["markLine"].pop("data")
        if bands:
            series["markArea"].pop("data")
    return result


def test_all_visible_returns_the_same_object() -> None:
    option = _options()
    assert (
        with_shock_annotation_visibility(
            option, show_lines_and_labels=True, show_b_bands=True
        )
        is option
    )


def test_hiding_lines_removes_lines_and_labels_only() -> None:
    option = _options()
    before = copy.deepcopy(option)

    hidden = with_shock_annotation_visibility(
        option, show_lines_and_labels=False, show_b_bands=True
    )

    assert option == before  # input is never mutated
    assert all(s["markLine"]["data"] == [] for s in hidden["series"])
    assert [s["markArea"]["data"] for s in hidden["series"]] == [
        s["markArea"]["data"] for s in option["series"]
    ]
    assert _strip(hidden, lines=True, bands=False) == _strip(
        option, lines=True, bands=False
    )


def test_hiding_bands_keeps_lines_and_labels() -> None:
    option = _options()
    hidden = with_shock_annotation_visibility(
        option, show_lines_and_labels=True, show_b_bands=False
    )

    assert all(s["markArea"]["data"] == [] for s in hidden["series"])
    assert [s["markLine"]["data"] for s in hidden["series"]] == [
        s["markLine"]["data"] for s in option["series"]
    ]


def test_hiding_both_keeps_every_data_series_and_axis() -> None:
    option = _options()
    hidden = with_shock_annotation_visibility(
        option, show_lines_and_labels=False, show_b_bands=False
    )
    assert _strip(hidden, lines=True, bands=True) == _strip(
        option, lines=True, bands=True
    )
    assert [s["data"] for s in hidden["series"]] == [
        s["data"] for s in option["series"]
    ]


def test_non_dict_option_is_rejected() -> None:
    with pytest.raises(TypeError):
        with_shock_annotation_visibility(
            [], show_lines_and_labels=False, show_b_bands=True
        )


def test_tab_wires_switches_without_rerun() -> None:
    source = (ROOT / "l2shock/ui/tab_shock_review.py").read_text(encoding="utf-8")

    assert source.count("with_shock_annotation_visibility(") == 1
    assert source.count("_presented_option(") >= 3
    assert (
        "annotation_lines_switch.on_value_change(_apply_annotation_visibility)"
        in source
    )
    assert (
        "annotation_bands_switch.on_value_change(_apply_annotation_visibility)"
        in source
    )
    assert "bounded_base_option = viewport.option" in source

    tree = ast.parse(source)
    handler = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_apply_annotation_visibility"
    )
    handler_source = ast.get_source_segment(source, handler) or ""
    # Display-only: no data loading, no review run.
    for forbidden in (
        "build_shock_view_selection",
        "load_shock_price_context",
        "runtime.start",
        "review_shock_areas",
    ):
        assert forbidden not in handler_source


def test_shutdown_is_in_global_header_not_settings() -> None:
    app_source = (ROOT / "l2shock/ui/app.py").read_text(encoding="utf-8")
    settings_source = (ROOT / "l2shock/ui/tab_settings.py").read_text(encoding="utf-8")
    control_source = (ROOT / "l2shock/ui/shutdown_control.py").read_text(
        encoding="utf-8"
    )

    header = app_source.index("with ui.header()")
    call = app_source.index("build_shutdown_header_button()")
    tabs = app_source.index("with ui.tabs()")
    assert header < call < tabs

    assert "shutdown_runtime" not in settings_source
    assert "Application Shutdown" not in settings_source
    assert "shutdown_button" not in settings_source
    assert "create_tracked_task" not in settings_source

    assert "shutdown_runtime(request_server_stop=True)" in control_source
    assert "Confirm Application Shutdown" in control_source
    assert "if state.shutdown_started:" in control_source


def test_display_timezone_adds_formatters_without_mutation() -> None:
    from l2shock.ui.shock_annotation_visibility import with_shock_display_timezone

    option = _options()
    before = copy.deepcopy(option)

    shown = with_shock_display_timezone(option, "Asia/Tehran")

    assert option == before
    assert shown["series"] == option["series"]
    assert [a["min"] for a in shown["xAxis"]] == [a["min"] for a in option["xAxis"]]
    assert [a["max"] for a in shown["xAxis"]] == [a["max"] for a in option["xAxis"]]

    for original, axis in zip(option["xAxis"], shown["xAxis"], strict=True):
        label_js = axis["axisLabel"][":formatter"]
        assert label_js.startswith("function")
        assert '"Asia/Tehran"' in label_js
        assert axis["axisLabel"].get("show") == original.get("axisLabel", {}).get(
            "show"
        )
        assert '"Asia/Tehran"' in axis["axisPointer"]["label"][":formatter"]

    tooltip_js = shown["tooltip"][":formatter"]
    assert tooltip_js.startswith("function")
    assert shown["tooltip"]["trigger"] == "axis"
    for source in (tooltip_js, shown["xAxis"][0]["axisLabel"][":formatter"]):
        assert "__TZ__" not in source
        assert "__PARTS__" not in source


def test_display_timezone_rejects_unknown_or_unsafe_names() -> None:
    from l2shock.ui.shock_annotation_visibility import (
        ShockDisplayTimezoneError,
        with_shock_display_timezone,
    )

    for bad in ("Mars/Olympus_Mons", "Asia/Tehran'); alert(1); //", "", None):
        with pytest.raises(ShockDisplayTimezoneError):
            with_shock_display_timezone(_options(), bad)  # type: ignore[arg-type]


def test_tab_wires_data_quality_switch_as_display_only() -> None:
    source = (ROOT / "l2shock/ui/tab_shock_review.py").read_text(encoding="utf-8")

    assert '"Show data-quality warnings"' in source
    assert (
        "warning_regions_switch.on_value_change(_apply_annotation_visibility)" in source
    )
    assert "bounded_warnings = used_warnings" in source

    tree = ast.parse(source)
    handler = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_apply_annotation_visibility"
    )
    handler_source = ast.get_source_segment(source, handler) or ""
    # Flipping any switch never reloads regions, price, or L2.
    for forbidden in (
        "_load_warning_overlay",
        "load_shock_price_warning_regions",
        "dataset_l2_warning_regions",
    ):
        assert forbidden not in handler_source

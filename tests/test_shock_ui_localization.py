# tests/test_shock_ui_localization.py
from __future__ import annotations

from datetime import datetime, timezone

from l2shock.ui.analysis_controls import parse_local_analysis_datetime
from l2shock.ui.shock_chart_options import _COLORS
from l2shock.ui.shock_legend import shock_legend_rows
from l2shock.ui.tab_shock_review import (
    _columns,
    _localized_row,
    build_shock_request,
)

_HASH = "a" * 64


def test_localized_row_converts_only_display_timestamps() -> None:
    row = {
        "id": "review:1",
        "inspection_position": 1,
        "first_b_utc": "2026-09-23T17:04:00+00:00",
        "representative_b_utc": "2026-09-23T17:04:05+00:00",
        "representative_c_utc": "2026-09-23T17:10:00+00:00",
    }

    localized = _localized_row(row, "Asia/Tehran")

    assert localized["first_b_utc"] == "2026-09-23T20:34:00+03:30"
    assert localized["representative_b_utc"] == "2026-09-23T20:34:05+03:30"
    assert localized["representative_c_utc"] == "2026-09-23T20:40:00+03:30"

    # Identity fields are untouched, and the source row is not mutated.
    assert localized["id"] == "review:1"
    assert localized["inspection_position"] == 1
    assert row["first_b_utc"] == "2026-09-23T17:04:00+00:00"


def test_columns_label_the_display_timezone() -> None:
    labels = {column["field"]: column["label"] for column in _columns("Asia/Tehran")}

    assert labels["first_b_utc"] == "B-area first (Asia/Tehran)"
    assert labels["representative_c_utc"] == "C (Asia/Tehran)"


def test_local_tehran_input_becomes_exact_utc_request() -> None:
    start = parse_local_analysis_datetime(
        "2026-09-23",
        "20:34:00",
        timezone_name="Asia/Tehran",
        field_name="start",
    )
    end = parse_local_analysis_datetime(
        "2026-09-24",
        "05:47:00",
        timezone_name="Asia/Tehran",
        field_name="end",
    )

    assert start == datetime(2026, 9, 23, 17, 4, tzinfo=timezone.utc)
    assert end == datetime(2026, 9, 24, 2, 17, tzinfo=timezone.utc)

    request, _config = build_shock_request(
        base="BTC",
        preset_hash=_HASH,
        start_utc=start.isoformat(),
        end_utc=end.isoformat(),
        major_fraction=0.20,
        medium_fraction=0.10,
        minor_fraction=0.05,
    )

    assert request.requested_start_utc == start
    assert request.requested_end_utc == end


def test_shock_legend_colors_match_chart_builder() -> None:
    colors = {color for _label, color, _meaning in shock_legend_rows()}

    for key in (
        "price_up",
        "price_down",
        "bid",
        "ask",
        "total",
        "delta",
        "b",
        "c",
        "b_area",
    ):
        assert _COLORS[key] in colors

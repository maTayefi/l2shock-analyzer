# tests/test_ui_localization.py
"""Retained local-time presentation and diagnostic filename contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from l2shock.analysis.l2_view_stream import L2ViewRequest
from l2shock.ui.analysis_inputs import parse_local_analysis_datetime
from l2shock.ui.tab_settings import (
    _diagnostics_export_filename,
    _local_timestamp_text,
)

_ROOT = Path(__file__).resolve().parents[1]


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

    request = L2ViewRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=start,
        requested_end_utc=end,
    )

    assert request.requested_start_utc == start
    assert request.requested_end_utc == end


def test_settings_timestamps_are_shown_in_local_timezone() -> None:
    expected = "2026-09-23T20:34:00+03:30 (Asia/Tehran)"
    instant = datetime(2026, 9, 23, 17, 4, tzinfo=timezone.utc)

    assert _local_timestamp_text(instant, "Asia/Tehran") == expected
    assert (
        _local_timestamp_text(
            "2026-09-23T17:04:00+00:00",
            "Asia/Tehran",
        )
        == expected
    )
    assert (
        _local_timestamp_text(
            "2026-09-23T17:04:00Z",
            "Asia/Tehran",
        )
        == expected
    )
    assert _local_timestamp_text("", "Asia/Tehran") == ""
    assert _local_timestamp_text(None, "Asia/Tehran") == ""
    assert _local_timestamp_text("not-a-time", "Asia/Tehran") == "not-a-time"


def test_ui_status_texts_do_not_publish_unlocalized_status_times() -> None:
    fetch_source = (_ROOT / "l2shock/ui/tab_fetch.py").read_text(encoding="utf-8")
    settings_source = (_ROOT / "l2shock/ui/tab_settings.py").read_text(encoding="utf-8")
    analysis_source = (_ROOT / "l2shock/ui/tab_l2_view.py").read_text(encoding="utf-8")

    assert "source UTC" not in fetch_source
    assert "expires_at_utc.isoformat()" not in settings_source
    assert "UTC zoom" not in analysis_source
    assert "visible UTC interval" not in analysis_source


def test_diagnostics_export_filename_keeps_utc_z_suffix() -> None:
    name = _diagnostics_export_filename(
        {"generated_at_utc": "2089-01-01T12:00:00+00:00"}
    )

    assert name == ("l2shock-production-diagnostics-20890101T120000Z.json")

# tests/test_header_shutdown_and_timezone.py
"""Global shutdown controls and detector-free timezone ownership."""

from __future__ import annotations

import copy
from pathlib import Path

from l2shock.ui.display_timezone import with_display_timezone

_ROOT = Path(__file__).resolve().parents[1]


def test_shutdown_is_in_global_header_not_settings() -> None:
    app_source = (_ROOT / "l2shock/ui/app.py").read_text(encoding="utf-8")
    settings_source = (_ROOT / "l2shock/ui/tab_settings.py").read_text(encoding="utf-8")
    control_source = (_ROOT / "l2shock/ui/shutdown_control.py").read_text(
        encoding="utf-8"
    )

    header = app_source.index("with ui.header()")
    call = app_source.index("build_shutdown_header_button()")
    tabs = app_source.index("with ui.tabs()")

    assert header < call < tabs

    for forbidden in (
        "shutdown_runtime",
        "Application Shutdown",
        "shutdown_button",
        "create_tracked_task",
    ):
        assert forbidden not in settings_source

    assert "shutdown_runtime(request_server_stop=True)" in control_source
    assert "Confirm Application Shutdown" in control_source
    assert "if state.shutdown_started:" not in control_source
    assert "shutdown_in_progress" in control_source
    assert "nonlocal shutdown_in_progress" in control_source
    assert "state.shutdown_complete" in control_source
    assert "if shutdown_in_progress or state.shutdown_complete:" in control_source
    assert "async def _run_shutdown" in control_source
    assert "Shutdown again to retry" in control_source


def test_header_shutdown_reports_failure_and_allows_retry() -> None:
    source = (_ROOT / "l2shock/ui/shutdown_control.py").read_text(encoding="utf-8")

    assert "async def _run_shutdown" in source
    assert "press " in source
    assert "Shutdown again to retry" in source
    assert "shutdown_in_progress" in source


def test_display_timezone_preserves_utc_chart_ownership() -> None:
    option = {
        "xAxis": [
            {
                "type": "time",
                "min": "2026-09-23T17:04:00+00:00",
                "max": "2026-09-23T17:05:00+00:00",
                "axisLabel": {"show": True},
                "axisPointer": {"label": {"show": True}},
            }
            for _ in range(5)
        ],
        "tooltip": {"trigger": "axis"},
        "series": [
            {
                "id": "retained-example",
                "data": [
                    [
                        "2026-09-23T17:04:00+00:00",
                        1.0,
                        2.0,
                        0.5,
                        3.0,
                    ]
                ],
            }
        ],
    }
    before = copy.deepcopy(option)

    shown = with_display_timezone(option, "Asia/Tehran")

    assert option == before
    assert shown["series"] == before["series"]

    for original, axis in zip(
        before["xAxis"],
        shown["xAxis"],
        strict=True,
    ):
        assert axis["min"] == original["min"]
        assert axis["max"] == original["max"]
        assert axis["type"] == "time"
        assert axis["axisLabel"]["show"] is True
        assert axis["axisPointer"]["label"]["show"] is True

        for javascript in (
            axis["axisLabel"][":formatter"],
            axis["axisPointer"]["label"][":formatter"],
        ):
            assert javascript.startswith("function")
            assert '"Asia/Tehran"' in javascript
            assert "__TZ__" not in javascript
            assert "__PARTS__" not in javascript

    assert shown["tooltip"]["trigger"] == "axis"
    assert '"Asia/Tehran"' in shown["tooltip"][":formatter"]

# tests/test_project_cores_contract.py
"""Core-document contracts for detector-free Analysis and retained workflows."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORES = ROOT / "project_cores.md"

PATH_LINE = re.compile(r"(?m)^((?:l2shock|tests)/[A-Za-z0-9_./-]+\.py)$")

CORE4_REQUIRED = (
    "l2shock/analysis/l2_seconds.py",
    "l2shock/analysis/l2_view_stream.py",
    "l2shock/analysis/l2_view_metrics.py",
    "l2shock/analysis/multi_market.py",
    "l2shock/analysis/aggregation.py",
    "l2shock/analysis/timeframes.py",
)

CORE5_REQUIRED = (
    "l2shock/ui/tab_l2_view.py",
    "l2shock/ui/l2_view_chart_options.py",
    "l2shock/ui/l2_view_presentation.py",
    "l2shock/ui/l2_view_warning_style.py",
    "l2shock/ui/display_timezone.py",
    "l2shock/ui/chart_interactions.py",
    "l2shock/ui/echarts.py",
    "l2shock/ui/shutdown_control.py",
)

CORE6_REQUIRED = (
    "l2shock/ui/l2_view_runtime.py",
    "l2shock/ui/shutdown.py",
    "l2shock/ui/shutdown_control.py",
    "l2shock/ui/components.py",
    "l2shock/ui/state.py",
)


def _text() -> str:
    return CORES.read_text(encoding="utf-8").replace("\r\n", "\n")


def _core(number: int) -> str:
    text = _text()
    start = re.search(rf"^# Core {number} .*$", text, flags=re.MULTILINE)
    assert start is not None

    following = re.search(
        rf"^# Core {number + 1} .*$",
        text[start.end() :],
        flags=re.MULTILINE,
    )
    assert following is not None

    return text[start.start() : start.end() + following.start()]


def test_every_documented_python_path_exists() -> None:
    missing = sorted(
        {
            relative
            for relative in PATH_LINE.findall(_text())
            if not (ROOT / relative).is_file()
        }
    )
    assert missing == []


def test_core4_documents_retained_analysis_modules() -> None:
    section = _core(4)

    assert "detector-free Analysis" in section

    for relative in CORE4_REQUIRED:
        assert relative in section, relative

    assert "partial-market" in section
    assert "same-second" in section
    assert "undefined" in section
    assert "Shock-Start" not in section


def test_core5_documents_current_ui_and_exports() -> None:
    section = _core(5)

    for relative in CORE5_REQUIRED:
        assert relative in section, relative

    assert "Shift+wheel" in section
    assert "PNG/SVG" in section
    assert "JSON/CSV" in section
    assert "render-token" in section
    assert "Selected review-row emphasis" not in section


def test_core6_documents_analysis_runtime_and_shutdown_ownership() -> None:
    section = _core(6)

    for relative in CORE6_REQUIRED:
        assert relative in section, relative

    assert "worker" in section
    assert "cancellation" in section
    assert "engine" in section
    assert "l2shock/ui/shock_runtime.py" not in section


def test_core_document_does_not_describe_retired_detector_workflow() -> None:
    text = _text()

    assert "Shock-Start" not in text
    assert re.search(r"\bLM\b", text) is None
    assert re.search(r"Liquidity[ -]Movement", text, re.IGNORECASE) is None

    for marker in (
        "B-area review",
        "within-tier percentiles",
        "deterministic v2/v3 ordering",
        "SHOCK_REVIEW",
        "tab_shock_review.py",
    ):
        assert marker not in text, marker


def test_historical_batch_sections_removed_and_fences_balanced() -> None:
    text = _text()

    assert "## Important inventory note" not in text
    assert "## Batch 1 validation" not in text
    assert text.count("```") % 2 == 0


def test_contract_tests_are_inventoried() -> None:
    text = _text()
    begin = text.index("# Global test inventory")
    end = text.index("# Workflow,", begin)

    inventory = text[begin:end]

    for relative in (
        "tests/test_project_cores_contract.py",
        "tests/test_project_cores_inventory.py",
        "tests/test_detector_backend_deleted.py",
    ):
        assert relative in inventory, relative

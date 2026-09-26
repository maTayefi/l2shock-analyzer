"""Batch 40B guards for project_cores.md: Core 5, Core 6, rankings, and cleanup."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORES = ROOT / "project_cores.md"

PATH_LINE = re.compile(r"(?m)^((?:l2shock|tests)/[A-Za-z0-9_./-]+\.py)$")

RETIRED_PATHS = (
    "l2shock/ui/analysis_chart.py",
    "l2shock/ui/analysis_controls.py",
    "l2shock/ui/analysis_export.py",
    "l2shock/ui/analysis_legend.py",
    "l2shock/ui/analysis_runtime.py",
    "l2shock/ui/tab_analysis.py",
    "tests/test_analysis_chart.py",
    "tests/test_ui_analysis_controls.py",
    "tests/test_ui_analysis_runtime.py",
    "tests/test_analysis_dataset_aggregate_regression.py",
    "tests/test_analysis_execution.py",
)

CORE5_REQUIRED = (
    "l2shock/ui/tab_shock_review.py",
    "l2shock/ui/shutdown_control.py",
    "l2shock/ui/shock_annotation_visibility.py",
    "l2shock/ui/shock_view_chart_options.py",
    "l2shock/ui/shock_view_selection.py",
    "l2shock/ui/chart_navigation.py",
    "tests/test_shock_annotation_visibility_and_header_shutdown.py",
    "tests/test_tab_shock_review.py",
)


def _text() -> str:
    return CORES.read_text(encoding="utf-8").replace("\r\n", "\n")


def _section(text: str, start: str, end: str) -> str:
    begin = text.index(start)
    return text[begin : text.index(end, begin + len(start))]


def test_every_documented_python_path_exists() -> None:
    missing = sorted(
        {value for value in PATH_LINE.findall(_text()) if not (ROOT / value).is_file()}
    )
    assert missing == []


def test_retired_paths_absent_from_document_and_repository() -> None:
    text = _text()
    for value in RETIRED_PATHS:
        assert value not in text, value
        assert not (ROOT / value).exists(), value


def test_core5_lists_current_shock_ui_modules() -> None:
    core5 = _section(_text(), "# Core 5 \u2014", "# Core 6 \u2014")
    for value in CORE5_REQUIRED:
        assert value in core5, value
    assert "Selected review-row emphasis" in core5


def test_core6_uses_shock_runtime() -> None:
    core6 = _section(_text(), "# Core 6 \u2014", "# Core 7 \u2014")
    assert "l2shock/ui/shock_runtime.py" in core6
    assert "l2shock/ui/shutdown_control.py" in core6
    assert "tests/test_shock_runtime.py" in core6


def test_no_liquidity_movement_wording_remains() -> None:
    text = _text()
    assert re.search(r"\bLM\b", text) is None
    assert re.search(r"Liquidity[ -]Movement", text, re.IGNORECASE) is None
    assert "3. Shock-Start analysis must not fail" in text


def test_historical_batch_sections_removed_and_fences_balanced() -> None:
    text = _text()
    assert "## Important inventory note" not in text
    assert "## Batch 1 validation" not in text
    assert "test_ui_analysis_controls.py` exists" not in text
    assert text.count("```") % 2 == 0


def test_new_contract_test_is_inventoried() -> None:
    inventory = _section(_text(), "# Global test inventory", "# Workflow,")
    assert "tests/test_project_cores_contract.py" in inventory

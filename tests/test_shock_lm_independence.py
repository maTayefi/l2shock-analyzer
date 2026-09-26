# tests/test_shock_lm_independence.py
from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.ui.analysis_inputs import (
    AnalysisInputError,
    AnalysisPresetOption,
    parse_local_analysis_datetime,
)
from l2shock.ui.chart_navigation import ChartNavigationError, ChartNavigationWindow

ROOT = Path(__file__).resolve().parents[1]

_FORBIDDEN = {
    "l2shock.analysis.liquidity_movement",
    "l2shock.analysis.ranking",
    "l2shock.analysis.execution",
    "l2shock.analysis.price_filter",
    "l2shock.ui.analysis_chart",
    "l2shock.ui.analysis_controls",
    "l2shock.ui.analysis_runtime",
    "l2shock.ui.analysis_export",
    "l2shock.ui.analysis_legend",
    "l2shock.ui.tab_analysis",
}

_CUT_FILES = (
    "l2shock/ui/tab_shock_review.py",
    "l2shock/ui/chart_interactions.py",
    "l2shock/ui/chart_navigation.py",
    "l2shock/ui/analysis_inputs.py",
    "l2shock/ui/shock_runtime.py",
    "l2shock/analysis/shock_review.py",
    "l2shock/analysis/robust_stats.py",
)


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
        elif isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)

    return names


@pytest.mark.parametrize("relative", _CUT_FILES)
def test_cut_file_does_not_import_lm(relative: str) -> None:
    assert not (_imported_modules(ROOT / relative) & _FORBIDDEN)


def test_local_input_converts_tehran_to_utc() -> None:
    assert parse_local_analysis_datetime(
        "2026-09-02", "15:30:00", timezone_name="Asia/Tehran", field_name="Start"
    ) == datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


def test_local_input_errors_are_value_errors() -> None:
    with pytest.raises(AnalysisInputError):
        parse_local_analysis_datetime("", "", timezone_name="Asia/Tehran", field_name="Start")

    assert issubclass(AnalysisInputError, ValueError)


def test_preset_option_label() -> None:
    option = AnalysisPresetOption(
        preset_hash="a" * 64,
        base="btc",
        algorithm_version="v1",
        created_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        config_json={"depth_band": {"lower_fraction": "0", "upper_fraction": "0.01"}},
    )
    assert option.label == "BTC | depth 0..0.01 | v1 | aaaaaaaaaaaa"

def test_navigation_window_validation_and_reexport() -> None:
    with pytest.raises(ChartNavigationError):
        ChartNavigationWindow(start_index=5, end_index=20, candidate_start_index=2, candidate_end_index=15)
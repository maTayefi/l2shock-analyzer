# tests/test_l2_view_batch3e.py
"""Batch 3E: legend dialog reuse and config/loader threshold agreement."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from pydantic import ValidationError

from l2shock.analysis.l2_view_stream import L2ViewLoadOptions
from l2shock.config import AnalysisConfig

_ROOT = Path(__file__).resolve().parents[1]
_TAB_PATH = _ROOT / "l2shock/ui/tab_l2_view.py"


def test_default_thresholds_are_accepted_by_loader() -> None:
    config = AnalysisConfig()
    L2ViewLoadOptions(
        l2_warning_seconds=config.l2_long_invalid_warning_seconds,
        price_warning_seconds=config.price_long_invalid_warning_minutes * 60,
    )


def test_maximum_config_thresholds_are_accepted_by_loader() -> None:
    config = AnalysisConfig(
        l2_long_invalid_warning_seconds=86_400,
        price_long_invalid_warning_minutes=1_440,
    )
    L2ViewLoadOptions(
        l2_warning_seconds=config.l2_long_invalid_warning_seconds,
        price_warning_seconds=config.price_long_invalid_warning_minutes * 60,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("l2_long_invalid_warning_seconds", 86_401),
        ("price_long_invalid_warning_minutes", 1_441),
        ("l2_long_invalid_warning_seconds", 0),
        ("price_long_invalid_warning_minutes", True),
    ],
)
def test_out_of_range_thresholds_fail_at_configuration(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        AnalysisConfig(**{field: value})


def _function(name: str) -> ast.FunctionDef:
    tree = ast.parse(_TAB_PATH.read_text(encoding="utf-8"))
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    assert len(matches) == 1
    return matches[0]


def test_legend_dialog_is_built_once_and_reopened() -> None:
    source = _TAB_PATH.read_text(encoding="utf-8")
    handler = ast.get_source_segment(source, _function("_open_legend")) or ""

    assert "nonlocal legend_dialog" in handler
    assert "if legend_dialog is None:" in handler
    assert handler.count("ui.dialog()") == 1
    assert handler.rstrip().endswith("legend_dialog.open()")
    assert "legend_button.on_click(_open_legend)" in source

# tests/test_lm_backend_deleted.py
"""Batch 36 guard: the LM backend and UI stay deleted.

Scans every Python file under l2shock/, tools/, and tests/ with the AST so a
later change cannot silently reintroduce an LM import.
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

import l2shock.analysis as analysis_package
import l2shock.analysis.shock_dataset as shock_dataset
import l2shock.config as config_module
from l2shock.analysis.l2_seconds import decode_l2_hour_to_seconds

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_DELETED_MODULES = (
    "l2shock.analysis.dataset",
    "l2shock.analysis.execution",
    "l2shock.analysis.liquidity_movement",
    "l2shock.analysis.price_filter",
    "l2shock.analysis.ranking",
    "l2shock.ui.analysis_chart",
    "l2shock.ui.analysis_controls",
    "l2shock.ui.analysis_export",
    "l2shock.ui.analysis_legend",
    "l2shock.ui.analysis_runtime",
    "l2shock.ui.tab_analysis",
)


def _python_files() -> tuple[Path, ...]:
    files: list[Path] = []

    for root_name in ("l2shock", "tools", "tests"):
        root = PROJECT_ROOT / root_name

        if not root.exists():
            continue

        files.extend(
            path for path in root.rglob("*.py") if "__pycache__" not in path.parts
        )

    return tuple(sorted(files))


def _imported_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
            names.update(f"{node.module}.{alias.name}" for alias in node.names)

    return names


@pytest.mark.parametrize("module_name", _DELETED_MODULES)
def test_lm_module_is_deleted(module_name: str) -> None:
    assert importlib.util.find_spec(module_name) is None


def test_no_python_file_imports_a_deleted_lm_module() -> None:
    offenders: list[str] = []

    for path in _python_files():
        tree = ast.parse(
            path.read_text(encoding="utf-8"),
            filename=str(path),
        )
        hits = _imported_names(tree) & set(_DELETED_MODULES)

        if hits:
            offenders.append(f"{path.relative_to(PROJECT_ROOT)}: {sorted(hits)}")

    assert offenders == []


def test_shock_loader_uses_the_moved_l2_decoder() -> None:
    assert shock_dataset.decode_l2_hour_to_seconds is decode_l2_hour_to_seconds
    assert analysis_package.decode_l2_hour_to_seconds is decode_l2_hour_to_seconds


def test_lm_configuration_class_is_gone() -> None:
    assert not hasattr(config_module, "LMConfig")
    assert "LMConfig" not in config_module.__all__


def test_package_exports_no_lm_names() -> None:
    lm_markers = ("LiquidityMovement", "LM_", "PriceBounds", "rank_liquidity")

    for name in analysis_package.__all__:
        assert not any(marker in name for marker in lm_markers), name

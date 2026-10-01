# tests/test_detector_backend_deleted.py
"""The retired detector backend must remain physically and logically absent."""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]

RETIRED_PRODUCTION_PATHS = (
    "l2shock/analysis/robust_stats.py",
    "l2shock/analysis/shock_dataset.py",
    "l2shock/analysis/shock_diagnostic.py",
    "l2shock/analysis/shock_diagnostic_cli.py",
    "l2shock/analysis/shock_evidence.py",
    "l2shock/analysis/shock_execution.py",
    "l2shock/analysis/shock_review.py",
    "l2shock/analysis/shock_start.py",
    "l2shock/analysis/shock_window.py",
    "l2shock/ui/shock_annotation_visibility.py",
    "l2shock/ui/shock_chart_options.py",
    "l2shock/ui/shock_dataset_view.py",
    "l2shock/ui/shock_inspection.py",
    "l2shock/ui/shock_legend.py",
    "l2shock/ui/shock_price_context.py",
    "l2shock/ui/shock_runtime.py",
    "l2shock/ui/shock_view_bars.py",
    "l2shock/ui/shock_view_chart_options.py",
    "l2shock/ui/shock_view_selection.py",
    "l2shock/ui/shock_warning_regions.py",
    "l2shock/ui/tab_shock_review.py",
)

_RETIRED_MODULES = frozenset(
    path.removesuffix(".py").replace("/", ".") for path in RETIRED_PRODUCTION_PATHS
)


def _python_files() -> tuple[Path, ...]:
    files: list[Path] = []

    for root_name in ("l2shock", "tools", "tests"):
        root = _ROOT / root_name

        if root.is_dir():
            files.extend(
                path for path in root.rglob("*.py") if "__pycache__" not in path.parts
            )

    return tuple(sorted(files))


def _package_name(path: Path) -> str:
    relative = path.relative_to(_ROOT).with_suffix("")
    parts = list(relative.parts)

    # Both ordinary modules and __init__.py resolve relative imports
    # against the containing package.
    parts.pop()

    return ".".join(parts)


def _imported_names(
    tree: ast.AST,
    *,
    package: str,
) -> set[str]:
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
            continue

        if isinstance(node, ast.ImportFrom):
            if node.level:
                relative = "." * node.level + (node.module or "")

                try:
                    module = importlib.util.resolve_name(
                        relative,
                        package,
                    )
                except ImportError, ValueError:
                    continue
            else:
                module = node.module or ""

            if module:
                names.add(module)
                names.update(
                    f"{module}.{alias.name}"
                    for alias in node.names
                    if alias.name != "*"
                )

            continue

        if not isinstance(node, ast.Call) or not node.args:
            continue

        first = node.args[0]

        if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
            continue

        function = node.func
        dynamic_import = (
            isinstance(function, ast.Name)
            and function.id in {"__import__", "import_module"}
        ) or (isinstance(function, ast.Attribute) and function.attr == "import_module")

        if dynamic_import and not first.value.startswith("."):
            names.add(first.value)

    return names


def _retired_hits(names: set[str]) -> set[str]:
    return {
        name
        for name in names
        if any(
            name == retired or name.startswith(retired + ".")
            for retired in _RETIRED_MODULES
        )
    }


@pytest.mark.parametrize("relative", RETIRED_PRODUCTION_PATHS)
def test_retired_production_file_is_absent(relative: str) -> None:
    assert not (_ROOT / relative).exists(), relative


@pytest.mark.parametrize(
    "module_name",
    sorted(_RETIRED_MODULES),
)
def test_retired_module_is_not_importable(module_name: str) -> None:
    assert importlib.util.find_spec(module_name) is None


def test_no_retained_python_file_imports_a_retired_detector() -> None:
    offenders: list[str] = []

    for path in _python_files():
        tree = ast.parse(
            path.read_text(encoding="utf-8-sig"),
            filename=str(path),
        )
        names = _imported_names(
            tree,
            package=_package_name(path),
        )
        hits = _retired_hits(names)

        if hits:
            offenders.append(
                f"{path.relative_to(_ROOT).as_posix()}: " f"{sorted(hits)}"
            )

    assert offenders == []


def test_application_shell_uses_retained_analysis_runtime_and_ui() -> None:
    source = (_ROOT / "l2shock/ui/app.py").read_text(encoding="utf-8")

    assert "build_l2_view_section" in source
    assert "peek_l2_view_runtime" in source
    assert "build_shock_review_section" not in source
    assert "peek_manual_shock_runtime" not in source


def test_shutdown_has_no_retired_runtime_owner() -> None:
    source = (_ROOT / "l2shock/ui/shutdown.py").read_text(encoding="utf-8")

    assert "peek_l2_view_runtime" in source
    assert "peek_manual_shock_runtime" not in source
    assert "Requesting cooperative Shock-Start review stop." not in source


def test_generic_retained_chart_and_data_modules_remain() -> None:
    for relative in (
        "l2shock/analysis/aggregation.py",
        "l2shock/analysis/l2_seconds.py",
        "l2shock/analysis/l2_view_metrics.py",
        "l2shock/analysis/l2_view_stream.py",
        "l2shock/analysis/multi_market.py",
        "l2shock/analysis/timeframes.py",
        "l2shock/ui/analysis_inputs.py",
        "l2shock/ui/chart_interactions.py",
        "l2shock/ui/chart_navigation.py",
        "l2shock/ui/display_timezone.py",
        "l2shock/ui/echarts.py",
        "l2shock/ui/l2_view_chart_options.py",
        "l2shock/ui/l2_view_presentation.py",
        "l2shock/ui/l2_view_runtime.py",
        "l2shock/ui/l2_view_warning_style.py",
        "l2shock/ui/tab_l2_view.py",
    ):
        assert (_ROOT / relative).is_file(), relative

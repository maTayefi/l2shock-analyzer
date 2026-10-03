from __future__ import annotations

import ast
from pathlib import Path

import pytest

import l2shock.ui.tab_l2_view as view_module


@pytest.mark.parametrize("new_source", (False, True))
def test_publication_boundary_discards_viewport_only_for_new_source(
    new_source: bool,
) -> None:
    """Inspect the actual argument expression at the production boundary.

    Existing handler tests cover publication/retry behavior. This test
    specifically prevents a caller-only guard from replacing the required
    guard inside _publish.
    """
    path = Path(view_module.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    publishers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_publish"
    ]
    assert len(publishers) == 1

    calls = [
        node
        for node in ast.walk(publishers[0])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "controller"
        and node.func.attr == "publish"
    ]
    assert len(calls) == 1

    arguments = [
        keyword.value
        for keyword in calls[0].keywords
        if keyword.arg == "shock_time_viewport"
    ]
    assert len(arguments) == 1

    expression = ast.Expression(body=arguments[0])
    ast.fix_missing_locations(expression)

    viewport = object()
    actual = eval(
        compile(expression, str(path), "eval"),
        {"__builtins__": {}},
        {"new_source": new_source, "viewport": viewport},
    )

    assert actual is (None if new_source else viewport)
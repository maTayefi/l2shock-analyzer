from __future__ import annotations

import ast
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TAB_SETTINGS_PATH = PROJECT_ROOT / "l2shock" / "ui" / "tab_settings.py"


def _module_tree() -> tuple[str, ast.Module]:
    source = TAB_SETTINGS_PATH.read_text(encoding="utf-8")
    tree = ast.parse(
        source,
        filename=str(TAB_SETTINGS_PATH),
    )
    return source, tree


def _async_function(
    tree: ast.Module,
    name: str,
) -> ast.AsyncFunctionDef:
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name
    ]

    assert len(matches) == 1, (
        f"Expected exactly one async function named {name!r}; " f"found {len(matches)}"
    )

    return matches[0]


def _is_operation_lock_acquire(statement: ast.stmt) -> bool:
    if not isinstance(statement, ast.Expr):
        return False

    awaited = statement.value

    if not isinstance(awaited, ast.Await):
        return False

    call = awaited.value

    return bool(
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "acquire"
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "operation_lock"
    )


def _contains_operation_lock_release(statements: list[ast.stmt]) -> bool:
    return any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "release"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "operation_lock"
        for statement in statements
        for node in ast.walk(statement)
    )


def test_settings_handlers_enter_try_immediately_after_lock_acquisition() -> None:
    _source, tree = _module_tree()

    for function_name in (
        "_run_preset_mutation",
        "_confirmed_maintenance",
    ):
        function = _async_function(
            tree,
            function_name,
        )

        acquire_indices = [
            index
            for index, statement in enumerate(function.body)
            if _is_operation_lock_acquire(statement)
        ]

        assert len(acquire_indices) == 1

        acquire_index = acquire_indices[0]

        assert acquire_index + 1 < len(function.body)
        protected_statement = function.body[acquire_index + 1]

        assert isinstance(protected_statement, ast.Try), (
            f"{function_name} must enter try/finally immediately after "
            "operation_lock.acquire()"
        )
        assert protected_statement.finalbody
        assert _contains_operation_lock_release(
            protected_statement.finalbody
        ), f"{function_name} finally block must release operation_lock"


def test_preset_mutation_does_not_publish_an_idle_flag_for_refresh() -> None:
    source, tree = _module_tree()
    function = _async_function(
        tree,
        "_run_preset_mutation",
    )
    function_source = ast.get_source_segment(
        source,
        function,
    )

    assert function_source is not None

    assert (
        "await _refresh_managed_presets(\n"
        "                allow_during_mutation=True,\n"
        "            )"
    ) in function_source

    refresh_position = function_source.index("await _refresh_managed_presets(")
    prefix = function_source[:refresh_position]

    assert "preset_mutation_running = False" not in prefix


def test_refresh_has_explicit_internal_mutation_override() -> None:
    source, tree = _module_tree()
    function = _async_function(
        tree,
        "_refresh_managed_presets",
    )
    function_source = ast.get_source_segment(
        source,
        function,
    )

    assert function_source is not None
    assert "allow_during_mutation: bool = False" in function_source
    assert (
        "if preset_mutation_running and not allow_during_mutation:" in function_source
    )


@pytest.mark.asyncio
async def test_cancelled_preset_mutation_keeps_lock_until_thread_finishes() -> None:
    import asyncio
    import logging
    import threading
    from collections.abc import Callable
    from types import SimpleNamespace

    import pytest

    _source, tree = _module_tree()
    mutation = _async_function(tree, "_run_preset_mutation")

    # The production function is nested and declares nonlocal
    # preset_mutation_running. Put that exact function AST into a minimal
    # enclosing function so the test exercises its real cancellation body.
    wrapper = ast.parse(
        "def _make_test_mutator():\n"
        "    preset_mutation_running = False\n"
        "    return _run_preset_mutation\n"
    )
    factory = wrapper.body[0]
    assert isinstance(factory, ast.FunctionDef)
    factory.body.insert(1, mutation)
    ast.fix_missing_locations(wrapper)

    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    operation_lock = asyncio.Lock()

    state = SimpleNamespace(
        shutdown_started=False,
        active_operation_name="",
        active_operation_started_at=None,
        operation_lock=operation_lock,
    )

    async def unused_refresh(*, allow_during_mutation: bool = False) -> None:
        assert allow_during_mutation

    namespace: dict[str, object] = {
        "asyncio": asyncio,
        "Callable": Callable,
        "state": state,
        "preset_refresh_running": False,
        "persistent_notify": lambda *args, **kwargs: None,
        "ui": SimpleNamespace(notify=lambda *args, **kwargs: None),
        "PresetManagementError": type(
            "PresetManagementError",
            (Exception,),
            {},
        ),
        "log": logging.getLogger(__name__),
        "_set_preset_controls_enabled": lambda enabled: None,
        "_refresh_managed_presets": unused_refresh,
    }
    exec(
        compile(wrapper, str(TAB_SETTINGS_PATH), "exec"),
        namespace,
    )
    make_mutator = namespace["_make_test_mutator"]
    assert callable(make_mutator)
    run_mutation = make_mutator()

    def blocking_action() -> object:
        entered.set()
        try:
            if not release.wait(timeout=10.0):
                raise TimeoutError("Test did not release the preset action")
            return object()
        finally:
            finished.set()

    task = asyncio.create_task(
        run_mutation(
            blocking_action,
            success_message="Preset mutation completed.",
        )
    )

    try:
        assert await asyncio.to_thread(entered.wait, 2.0)
        assert operation_lock.locked()

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        assert not finished.is_set()
        assert not task.done()
        assert operation_lock.locked()
        assert state.active_operation_name == "preset_management"
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5.0)

    assert finished.is_set()
    assert not operation_lock.locked()
    assert state.active_operation_name == ""

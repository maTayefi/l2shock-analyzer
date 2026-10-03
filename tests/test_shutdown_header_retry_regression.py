from __future__ import annotations

import ast
import asyncio
import copy
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from l2shock.ui import shutdown as shutdown_module
from l2shock.ui import shutdown_control
from l2shock.ui.state import get_state, reset_state_for_tests


class _Button:
    def __init__(self) -> None:
        self.enabled = True

    def enable(self) -> None:
        self.enabled = True

    def disable(self) -> None:
        self.enabled = False


class _Dialog:
    def __init__(self) -> None:
        self.open_count = 0
        self.close_count = 0

    def open(self) -> None:
        self.open_count += 1

    def close(self) -> None:
        self.close_count += 1


def _make_header_handlers(
    *,
    state,
    tasks: list[asyncio.Task],
    notifications: list[str],
):
    source_path = Path(shutdown_control.__file__)
    tree = ast.parse(
        source_path.read_text(encoding="utf-8"),
        filename=str(source_path),
    )

    builders = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "build_shutdown_header_button"
    ]
    assert len(builders) == 1

    names = (
        "_restore_shutdown_controls",
        "_run_shutdown",
        "_open_shutdown_dialog",
        "_confirmed_shutdown",
    )
    selected = {}

    for node in ast.walk(builders[0]):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in names:
                assert node.name not in selected
                selected[node.name] = copy.deepcopy(node)

    assert set(selected) == set(names)

    wrapper = ast.parse(
        "def _make_test_header():\n" "    shutdown_in_progress = False\n"
    )
    factory = wrapper.body[0]
    assert isinstance(factory, ast.FunctionDef)

    factory.body.extend(selected[name] for name in names)
    factory.body.append(
        ast.Return(
            value=ast.Tuple(
                elts=[
                    ast.Name(id="_open_shutdown_dialog", ctx=ast.Load()),
                    ast.Name(id="_confirmed_shutdown", ctx=ast.Load()),
                ],
                ctx=ast.Load(),
            )
        )
    )
    ast.fix_missing_locations(wrapper)

    shutdown_button = _Button()
    confirm_button = _Button()
    dialog = _Dialog()

    def start_task(coroutine):
        task = asyncio.create_task(coroutine)
        tasks.append(task)
        return task

    def notify(message, **_kwargs) -> None:
        notifications.append(str(message))

    namespace = {
        "state": state,
        "shutdown_runtime": shutdown_module.shutdown_runtime,
        "_start_shutdown_task": start_task,
        "shutdown_button": shutdown_button,
        "confirm_button": confirm_button,
        "shutdown_dialog": dialog,
        "persistent_notify": notify,
        "ui": SimpleNamespace(notify=notify),
        "log": logging.getLogger(__name__),
    }

    exec(
        compile(wrapper, str(source_path), "exec"),
        namespace,
    )

    open_dialog, confirm = namespace["_make_test_header"]()

    return (
        open_dialog,
        confirm,
        dialog,
        shutdown_button,
        confirm_button,
    )


def _patch_idle_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[str],
) -> None:
    for name in (
        "peek_automatic_fetch_runtime",
        "peek_manual_fetch_runtime",
        "peek_remote_import_runtime",
        "peek_manual_processing_runtime",
        "peek_l2_view_runtime",
    ):
        monkeypatch.setattr(
            shutdown_module,
            name,
            lambda: None,
        )

    async def no_pending(*, timeout_seconds: float) -> int:
        del timeout_seconds
        return 0

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        no_pending,
    )
    monkeypatch.setattr(
        shutdown_module,
        "wait_for_untracked_db_workers",
        no_pending,
    )
    monkeypatch.setattr(
        shutdown_module,
        "reset_engine",
        lambda: events.append("engine"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ("cleanup", "server_stop"))
async def test_header_retries_failed_shutdown_without_repeating_completed_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    reset_state_for_tests()
    state = get_state()
    events: list[str] = []
    tasks: list[asyncio.Task] = []
    notifications: list[str] = []

    _patch_idle_shutdown(monkeypatch, events=events)

    cleanup_attempts = 0

    async def cancel_tasks(*, timeout_seconds: float) -> int:
        nonlocal cleanup_attempts
        del timeout_seconds
        cleanup_attempts += 1

        if failure_stage == "cleanup" and cleanup_attempts == 1:
            raise RuntimeError("Simulated cleanup failure")

        return 0

    server_attempts = 0

    async def request_server_stop() -> None:
        nonlocal server_attempts
        server_attempts += 1
        events.append("server_stop")

        if failure_stage == "server_stop" and server_attempts == 1:
            raise RuntimeError("Simulated server-stop failure")

    monkeypatch.setattr(
        shutdown_module,
        "cancel_and_wait_for_tracked_tasks",
        cancel_tasks,
    )
    monkeypatch.setattr(
        shutdown_module,
        "_request_nicegui_shutdown",
        request_server_stop,
    )

    (
        open_dialog,
        confirm,
        dialog,
        shutdown_button,
        confirm_button,
    ) = _make_header_handlers(
        state=state,
        tasks=tasks,
        notifications=notifications,
    )

    open_dialog()
    assert dialog.open_count == 1

    await confirm()
    assert len(tasks) == 1
    await asyncio.wait_for(tasks[0], timeout=2.0)

    assert state.shutdown_started is True
    assert shutdown_button.enabled is True
    assert confirm_button.enabled is True

    if failure_stage == "server_stop":
        assert state.shutdown_complete is True
        assert events.count("engine") == 1
        assert cleanup_attempts == 1
        assert server_attempts == 1
        assert any(
            "Resource cleanup completed, but the local server-stop" in message
            for message in notifications
        )
    else:
        assert state.shutdown_complete is False
        assert events.count("engine") == 0
        assert cleanup_attempts == 1
        assert server_attempts == 0
        assert any(
            "Shutdown cleanup did not complete" in message for message in notifications
        )

    # The actual header handlers must admit a retry after either failure.
    open_dialog()
    assert dialog.open_count == 2

    await confirm()
    assert len(tasks) == 2
    await asyncio.wait_for(tasks[1], timeout=2.0)

    assert state.shutdown_started is True
    assert state.shutdown_complete is True
    assert events.count("engine") == 1

    if failure_stage == "server_stop":
        # Cleanup is not repeated after its completed-state boundary.
        assert cleanup_attempts == 1
        assert server_attempts == 2
    else:
        assert cleanup_attempts == 2
        assert server_attempts == 1


@pytest.mark.asyncio
async def test_header_rejects_duplicate_confirmation_while_shutdown_runs() -> None:
    reset_state_for_tests()
    state = get_state()
    tasks: list[asyncio.Task] = []
    notifications: list[str] = []

    open_dialog, confirm, dialog, _shutdown_button, _confirm_button = (
        _make_header_handlers(
            state=state,
            tasks=tasks,
            notifications=notifications,
        )
    )

    # The flag is raised synchronously before the background task runs.
    await confirm()
    await confirm()
    open_dialog()

    assert len(tasks) == 1
    assert dialog.open_count == 0
    assert any("already in progress" in message for message in notifications)

    # Do not allow this test to execute the real shutdown backend.
    tasks[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await tasks[0]

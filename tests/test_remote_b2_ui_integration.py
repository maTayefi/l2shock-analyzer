"""Explicit B2/HF Fetch selection and application runtime ownership."""

from __future__ import annotations

import ast
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import l2shock.ui.remote_import_runtime as runtime_module
import l2shock.ui.tab_fetch as fetch_module
from l2shock.config import B2Config, RemoteConfig
from l2shock.remote.import_contracts import (
    B2_STORAGE_BACKEND,
    HF_STORAGE_BACKEND,
)
from l2shock.ui.remote_import_runtime import (
    B2RangeImportRepository,
    RemoteImportRuntime,
    RemoteImportRuntimeBusyError,
    RemoteImportRuntimeError,
)
from l2shock.ui.state import get_state, reset_state_for_tests


def _hour(value: int = 12) -> datetime:
    return datetime(2026, 10, 1, value, tzinfo=timezone.utc)


def _b2_settings() -> B2Config:
    return B2Config(
        endpoint_url="https://s3.us-west-004.backblazeb2.com",
        bucket="l2shock-test-storage",
        key_id="test-key-id",
        application_key="test-application-key",
    )


def _remote_settings(*, b2_ready: bool = True) -> RemoteConfig:
    return RemoteConfig(
        hf_repo_id="test-owner/test-dataset",
        hf_token="test-hf-token",
        b2=_b2_settings() if b2_ready else B2Config(),
    )


class EmptyHFRepository:
    def current_revision(self) -> str:
        return "a" * 40

    def download_artifact(self, key, *, revision=None):
        del key
        assert revision == "a" * 40
        return None


@pytest.fixture(autouse=True)
def _isolated_runtime():
    reset_state_for_tests()
    runtime_module.reset_remote_import_runtime_for_tests()
    yield
    runtime_module.reset_remote_import_runtime_for_tests()
    reset_state_for_tests()


@pytest.mark.parametrize(
    "workflow",
    (
        "remote_hf_import",
        "remote_b2_import",
        "local_fetch_processing",
    ),
)
def test_remote_config_accepts_explicit_supported_workflows(workflow: str) -> None:
    config = RemoteConfig(default_workflow=workflow)
    assert config.default_workflow == workflow


def test_remote_config_rejects_unknown_workflow() -> None:
    with pytest.raises(ValidationError, match="default_workflow"):
        RemoteConfig(default_workflow="automatic_backend_fallback")


def test_b2_configuration_does_not_claim_hf_readiness() -> None:
    config = RemoteConfig(
        default_workflow="remote_b2_import",
        b2=_b2_settings(),
    )

    assert config.b2.configured is True
    assert config.configured is False
    assert fetch_module._remote_configuration_message(config, "remote_b2_import") == ""
    assert "HF" in fetch_module._remote_configuration_message(
        config, "remote_hf_import"
    )


@pytest.mark.parametrize(
    ("profile", "expected"),
    (
        ("remote_hf_import", HF_STORAGE_BACKEND),
        ("remote_b2_import", B2_STORAGE_BACKEND),
        ("local_fetch_processing", None),
    ),
)
def test_profile_maps_to_only_the_explicit_backend(profile, expected) -> None:
    assert fetch_module._remote_backend_for_profile(profile) == expected


def test_unknown_profile_does_not_fall_back_to_hf() -> None:
    with pytest.raises(ValueError, match="workflow"):
        fetch_module._remote_backend_for_profile("unknown")


def test_readiness_message_does_not_expose_credentials() -> None:
    config = _remote_settings(b2_ready=False)
    message = fetch_module._remote_configuration_message(config, "remote_b2_import")

    assert "remote.b2.endpoint_url" in message
    assert "L2SHOCK__REMOTE__B2__APPLICATION_KEY" in message
    assert "test-hf-token" not in message
    assert "test-key-id" not in message
    assert "test-application-key" not in message


@pytest.mark.parametrize("revision", (None, "a" * 40))
def test_hf_storage_label_accepts_unpinned_stopped_result(revision) -> None:
    assert fetch_module._remote_storage_label(
        HF_STORAGE_BACKEND,
        revision,
    ) == "Pinned revision: " + (revision or "none")


def test_b2_storage_label_has_no_synthetic_revision() -> None:
    label = fetch_module._remote_storage_label(B2_STORAGE_BACKEND, None)
    assert "per artifact" in label
    assert "Pinned revision:" not in label

    with pytest.raises(ValueError, match="synthetic"):
        fetch_module._remote_storage_label(B2_STORAGE_BACKEND, "a" * 40)


@pytest.mark.asyncio
async def test_singleton_switches_idle_backend_and_preserves_sequence() -> None:
    hf_repository = EmptyHFRepository()
    hf_runtime = runtime_module.get_remote_import_runtime(
        repository=hf_repository,
    )

    result = await hf_runtime.start(
        requested_start_utc=_hour(),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )
    await asyncio.sleep(0)

    assert result.missing_count == 4
    assert hf_runtime.task is None
    assert hf_runtime.snapshot().completion_sequence == 1

    def forbidden_store(_settings):
        raise AssertionError("Singleton construction opened a B2 client")

    b2_repository = B2RangeImportRepository(
        _b2_settings(),
        store_factory=forbidden_store,
    )
    b2_runtime = runtime_module.get_remote_import_runtime(
        repository=b2_repository,
        storage_backend=B2_STORAGE_BACKEND,
    )

    assert b2_runtime is not hf_runtime
    assert runtime_module.peek_remote_import_runtime() is b2_runtime
    assert runtime_module.get_remote_import_runtime() is b2_runtime
    assert b2_runtime.snapshot().storage_backend == B2_STORAGE_BACKEND
    assert b2_runtime.snapshot().completion_sequence == 1
    assert b2_runtime.snapshot().last_result is None
    assert b2_runtime.snapshot().pinned_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize("barrier", ("operation", "lock", "shutdown"))
async def test_singleton_backend_switch_respects_shared_admission(barrier) -> None:
    hf_runtime = runtime_module.get_remote_import_runtime(
        repository=EmptyHFRepository(),
    )
    state = get_state()

    if barrier == "operation":
        state.active_operation_name = "settings_maintenance"
    elif barrier == "lock":
        await state.operation_lock.acquire()
    else:
        state.shutdown_started = True

    try:
        with pytest.raises(RemoteImportRuntimeBusyError):
            runtime_module.get_remote_import_runtime(
                repository=B2RangeImportRepository(_b2_settings()),
                storage_backend=B2_STORAGE_BACKEND,
            )

        assert runtime_module.peek_remote_import_runtime() is hf_runtime
    finally:
        if barrier == "lock":
            state.operation_lock.release()


@pytest.mark.asyncio
async def test_cancelled_unfinalized_runtime_cannot_be_replaced() -> None:
    hf_runtime = runtime_module.get_remote_import_runtime(
        repository=EmptyHFRepository(),
    )
    task = hf_runtime.start(
        requested_start_utc=_hour(),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )
    task.cancel()

    # No await has allowed the task's finalization callback to run yet.
    with pytest.raises(RemoteImportRuntimeBusyError):
        runtime_module.get_remote_import_runtime(
            repository=B2RangeImportRepository(_b2_settings()),
            storage_backend=B2_STORAGE_BACKEND,
        )

    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0)
    assert hf_runtime.task is None


@pytest.mark.asyncio
async def test_failed_b2_construction_preserves_existing_hf_runtime(
    monkeypatch,
) -> None:
    hf_runtime = runtime_module.get_remote_import_runtime(
        repository=EmptyHFRepository(),
    )
    before = hf_runtime.snapshot()

    monkeypatch.setattr(
        runtime_module,
        "get_settings",
        lambda: SimpleNamespace(remote=RemoteConfig()),
    )

    with pytest.raises(RemoteImportRuntimeError, match="B2"):
        runtime_module.get_remote_import_runtime(
            storage_backend=B2_STORAGE_BACKEND,
        )

    assert runtime_module.peek_remote_import_runtime() is hf_runtime
    assert hf_runtime.snapshot() == before


@pytest.mark.asyncio
async def test_requested_backend_cannot_disagree_with_repository() -> None:
    with pytest.raises(RemoteImportRuntimeError, match="disagrees"):
        runtime_module.get_remote_import_runtime(
            repository=EmptyHFRepository(),
            storage_backend=B2_STORAGE_BACKEND,
        )

    assert runtime_module.peek_remote_import_runtime() is None


@pytest.mark.asyncio
async def test_configured_initial_b2_default_does_not_construct_hf(
    monkeypatch,
) -> None:
    config = RemoteConfig(
        default_workflow="remote_b2_import",
        b2=_b2_settings(),
    )
    monkeypatch.setattr(
        runtime_module,
        "get_settings",
        lambda: SimpleNamespace(remote=config),
    )

    def forbidden_hf_factory():
        raise AssertionError("Explicit B2 default constructed HF")

    monkeypatch.setattr(
        runtime_module,
        "create_production_remote_import_repository",
        forbidden_hf_factory,
    )

    runtime = runtime_module.get_remote_import_runtime()

    assert runtime.snapshot().storage_backend == B2_STORAGE_BACKEND
    assert isinstance(runtime._repository, B2RangeImportRepository)


class _StandaloneHandler(ast.NodeTransformer):
    def visit_Nonlocal(self, node: ast.Nonlocal) -> ast.Global:
        return ast.copy_location(
            ast.Global(names=list(node.names)),
            node,
        )


def _load_start_handler(namespace):
    source_path = Path(fetch_module.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_start_remote_import"
    ]
    assert len(matches) == 1

    node = _StandaloneHandler().visit(matches[0])
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)

    exec(compile(module, str(source_path), "exec"), namespace)
    return namespace["_start_remote_import"]


def _start_handler_namespace(
    *,
    profile="remote_b2_import",
    b2_ready=True,
    shutdown=False,
):
    events = []
    runtime = SimpleNamespace(
        start=lambda **kwargs: events.append(("start", kwargs)),
    )

    def get_runtime(*, storage_backend):
        events.append(("backend", storage_backend))
        return runtime

    namespace = {
        "remote_runtime": None,
        "workflow_profile": SimpleNamespace(value=profile),
        "state": SimpleNamespace(shutdown_started=shutdown),
        "settings": SimpleNamespace(
            remote=_remote_settings(b2_ready=b2_ready),
        ),
        "remote_btc": SimpleNamespace(value=True),
        "remote_eth": SimpleNamespace(value=False),
        "remote_start_date": SimpleNamespace(value="2026-10-01"),
        "remote_start_time": SimpleNamespace(value="12:00"),
        "remote_end_date": SimpleNamespace(value="2026-10-01"),
        "remote_end_time": SimpleNamespace(value="13:00"),
        "remote_depth_lower": SimpleNamespace(value="0"),
        "remote_depth_upper": SimpleNamespace(value="0.01"),
        "remote_result_label": SimpleNamespace(text=""),
        "remote_revision_label": SimpleNamespace(text=""),
        "remote_current_artifact_label": SimpleNamespace(text=""),
        "_remote_backend_for_profile": fetch_module._remote_backend_for_profile,
        "_remote_backend_title": fetch_module._remote_backend_title,
        "_remote_configuration_message": (fetch_module._remote_configuration_message),
        "_remote_storage_label": fetch_module._remote_storage_label,
        "_parse_local_datetime": (
            lambda _date, value, *, field_name: (
                _hour() if value == "12:00" else _hour(13)
            )
        ),
        "_parse_depth_band": (lambda _lower, _upper: (Decimal("0"), Decimal("0.01"))),
        "get_remote_import_runtime": get_runtime,
        "persistent_notify": (
            lambda message, **kwargs: events.append(("notify", message, kwargs))
        ),
        "RemoteImportRuntimeBusyError": RemoteImportRuntimeBusyError,
    }
    return namespace, events, runtime


@pytest.mark.asyncio
async def test_actual_start_handler_requests_b2_explicitly() -> None:
    namespace, events, runtime = _start_handler_namespace()
    await _load_start_handler(namespace)()

    assert events[0] == ("backend", B2_STORAGE_BACKEND)
    assert events[1][0] == "start"
    assert events[1][1]["bases"] == ("BTC",)
    assert namespace["remote_runtime"] is runtime
    assert "per artifact" in namespace["remote_revision_label"].text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parameters",
    (
        {"b2_ready": False},
        {"shutdown": True},
        {"profile": "local_fetch_processing"},
        {"profile": "unknown"},
    ),
)
async def test_actual_start_handler_refuses_without_backend_fallback(
    parameters,
) -> None:
    namespace, events, _runtime = _start_handler_namespace(**parameters)
    await _load_start_handler(namespace)()

    assert not any(event[0] in {"backend", "start"} for event in events)
    assert any(event[0] == "notify" for event in events)


def test_stop_handler_uses_current_application_runtime() -> None:
    path = Path(fetch_module.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_stop_remote_import"
    ]
    assert len(matches) == 1

    calls = [
        node
        for node in ast.walk(matches[0])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "peek_remote_import_runtime"
    ]
    assert len(calls) == 1


def test_ui_has_no_nullable_revision_concatenation() -> None:
    source = Path(fetch_module.__file__).read_text(encoding="utf-8")

    assert '"Pinned revision: " + result.pinned_revision' not in source
    assert '"Pinned revision: " + remote_progress.pinned_revision' not in source
    assert '"remote_b2_import": "Remote Backblaze B2 Import"' in source


@pytest.mark.asyncio
async def test_remote_task_creation_failure_preserves_previous_result(
    monkeypatch,
) -> None:
    runtime = RemoteImportRuntime(repository=EmptyHFRepository())

    previous_result = await runtime.start(
        requested_start_utc=_hour(),
        requested_end_utc=_hour(13),
        bases=("BTC",),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )
    await asyncio.sleep(0)

    before = runtime.snapshot()
    assert before.last_result is previous_result
    assert before.latest_progress is not None
    assert before.pinned_revision == "a" * 40
    assert runtime.task is None

    loop = asyncio.get_running_loop()

    def reject_task(_coroutine, **_kwargs):
        raise RuntimeError("simulated remote task creation failure")

    # Restore the loop method before pytest runs its own asynchronous cleanup.
    with monkeypatch.context() as patch:
        patch.setattr(loop, "create_task", reject_task)

        with pytest.raises(RuntimeError, match="task creation failure"):
            runtime.start(
                requested_start_utc=_hour(),
                requested_end_utc=_hour(13),
                bases=("BTC",),
                lower_depth_fraction=Decimal("0"),
                upper_depth_fraction=Decimal("0.01"),
            )

    assert runtime.snapshot() == before
    assert runtime.task is None
    assert get_state().active_operation_name == ""
    assert get_state().active_operation_started_at is None
    assert get_state().tracked_tasks == set()
    assert get_state().operation_lock.locked() is False


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_mode", ("stop", "shutdown", "cancel"))
async def test_remote_eager_task_factory_preserves_preworker_stop(
    monkeypatch,
    stop_mode,
) -> None:
    state = get_state()
    calls = []
    adapter = B2RangeImportRepository(_b2_settings())

    def forbidden_pin():
        calls.append("pin")
        raise AssertionError("Repository work started before admission settled")

    def forbidden_download(_key, *, revision=None):
        del revision
        calls.append("download")
        raise AssertionError("Stopped operation started a download")

    monkeypatch.setattr(adapter, "current_revision", forbidden_pin)
    monkeypatch.setattr(adapter, "download_artifact", forbidden_download)

    runtime = RemoteImportRuntime(repository=adapter)
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()

    try:
        loop.set_task_factory(asyncio.eager_task_factory)

        task = runtime.start(
            requested_start_utc=_hour(),
            requested_end_utc=_hour(13),
            bases=("BTC",),
            lower_depth_fraction=Decimal("0"),
            upper_depth_fraction=Decimal("0.01"),
        )

        assert runtime.task is task
        assert task in state.tracked_tasks
        assert state.active_operation_name == "remote_b2_import"

        if stop_mode == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            if stop_mode == "stop":
                assert runtime.request_stop() is True
            else:
                state.shutdown_started = True

            result = await task
            assert result.status == "stopped"
            assert result.stopped is True
            assert result.items == ()
            assert result.pinned_revision is None

        await asyncio.sleep(0)

        assert calls == []
        assert runtime.task is None
        assert runtime.snapshot().completion_sequence == 1
        assert state.active_operation_name == ""
        assert state.active_operation_started_at is None
        assert state.operation_lock.locked() is False
        assert state.tracked_tasks == set()

    finally:
        loop.set_task_factory(previous_factory)

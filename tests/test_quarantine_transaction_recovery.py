from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import l2shock.acquisition.validation as validation_module
import l2shock.ui.automatic_fetch_runtime as runtime_module
from l2shock.acquisition.errors import QuarantineError
from l2shock.acquisition.models import SourceDataKind, SourceFileSpec


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=datetime(2088, 1, 1, 12, tzinfo=timezone.utc),
    )


def _fail_sidecar_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_write_text = Path.write_text

    def write_text(path: Path, *args, **kwargs):
        if ".quarantine.json." in path.name:
            raise OSError("simulated sidecar failure")
        return original_write_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", write_text)


def test_quarantine_error_retains_completed_move_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"corrupt-source")
    quarantine_root = tmp_path / "quarantine"
    _fail_sidecar_writes(monkeypatch)

    with pytest.raises(QuarantineError) as caught:
        validation_module.quarantine_file(
            source,
            quarantine_root,
            spec=_spec(),
            reason="test_sidecar_failure",
        )

    moved = caught.value.quarantined_path

    assert isinstance(moved, Path)
    assert not source.exists()
    assert moved.is_file()
    assert moved.read_bytes() == b"corrupt-source"
    assert moved.is_relative_to(quarantine_root)
    assert not list(quarantine_root.rglob("*.tmp"))
    assert not moved.with_suffix(moved.suffix + ".quarantine.json").exists()


def test_quarantine_post_move_stat_failure_retains_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"corrupt-source")
    quarantine_root = tmp_path / "quarantine"
    original_stat = Path.stat
    failed = False

    def stat(path: Path, *args, **kwargs):
        nonlocal failed
        if (
            not failed
            and path.suffix == ".parquet"
            and path.is_relative_to(quarantine_root)
        ):
            failed = True
            raise OSError("simulated post-move stat failure")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", stat)

    with pytest.raises(QuarantineError) as caught:
        validation_module.quarantine_file(
            source,
            quarantine_root,
            spec=_spec(),
            reason="test_stat_failure",
        )

    moved = caught.value.quarantined_path

    assert failed
    assert isinstance(moved, Path)
    assert not source.exists()
    assert moved.read_bytes() == b"corrupt-source"


class _CommitInterrupted(BaseException):
    pass


def _install_fake_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    status: str,
    commit_failure: type[BaseException] | None,
):
    spec = _spec()
    raw_root = tmp_path / "raw"
    quarantine_root = tmp_path / "quarantine"
    canonical = spec.local_path(raw_root)
    canonical.parent.mkdir(parents=True)
    corrupt_content = b"corrupt-local-bytes"
    canonical.write_bytes(corrupt_content)
    expected_content = b"durable-original-bytes"

    row = SimpleNamespace(
        provider=spec.provider,
        venue=spec.venue,
        instrument=spec.symbol,
        data_kind=spec.data_kind.value,
        hour_utc=spec.hour_utc,
        local_path=str(canonical),
        file_size_bytes=len(expected_content),
        content_sha256=hashlib.sha256(expected_content).hexdigest(),
        status=status,
    )
    counters = SimpleNamespace(commits=0, rollbacks=0)

    class SessionDouble:
        def execute(self, _statement):
            return SimpleNamespace(
                all=lambda: [
                    (
                        row.provider,
                        row.venue,
                        row.instrument,
                        row.data_kind,
                        row.local_path,
                        row.file_size_bytes,
                        row.content_sha256,
                    )
                ]
            )

        def flush(self):
            return None

        def commit(self):
            counters.commits += 1
            if commit_failure is not None:
                raise commit_failure("simulated commit failure")

        def rollback(self):
            counters.rollbacks += 1

    @contextmanager
    def scope():
        before = (row.status, row.local_path)
        session = SessionDouble()
        try:
            yield session
        except BaseException:
            row.status, row.local_path = before
            session.rollback()
            raise

    class RepositoryDouble:
        def __init__(self, _session):
            pass

        def get_source_hour(self, requested):
            assert requested == spec
            return row

        def record_error(self, requested, *, message):
            assert requested == spec
            assert "immutable SHA-256" in message
            row.status = "error"

    monkeypatch.setattr(
        runtime_module,
        "get_settings",
        lambda: SimpleNamespace(
            storage=SimpleNamespace(
                raw_path=raw_root,
                quarantine_path=quarantine_root,
            )
        ),
    )
    monkeypatch.setattr(runtime_module, "session_scope", scope)
    monkeypatch.setattr(
        runtime_module,
        "AcquisitionRepository",
        RepositoryDouble,
    )
    monkeypatch.setattr(
        runtime_module,
        "acquire_source_hour_transaction_lock",
        lambda _session, _spec: None,
    )

    return SimpleNamespace(
        spec=spec,
        row=row,
        canonical=canonical,
        quarantine_root=quarantine_root,
        content=corrupt_content,
        counters=counters,
    )


@pytest.mark.parametrize("status", ("downloaded", "processed"))
@pytest.mark.parametrize(
    "commit_failure",
    (RuntimeError, _CommitInterrupted),
)
def test_sidecar_failure_then_commit_failure_restores_raw_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    commit_failure: type[BaseException],
) -> None:
    setup = _install_fake_transaction(
        tmp_path,
        monkeypatch,
        status=status,
        commit_failure=commit_failure,
    )
    _fail_sidecar_writes(monkeypatch)

    with pytest.raises(commit_failure, match="simulated commit failure"):
        runtime_module._prepare_corrupt_local_sources_for_reacquisition_sync(
            setup.spec.hour_utc,
        )

    assert setup.canonical.read_bytes() == setup.content
    assert setup.row.status == status
    assert setup.row.local_path == str(setup.canonical)
    assert setup.counters.commits == 1
    assert setup.counters.rollbacks == 1
    assert not list(setup.quarantine_root.rglob("*.parquet"))
    assert not list(setup.quarantine_root.rglob("*.tmp"))


@pytest.mark.parametrize("status", ("downloaded", "processed"))
def test_sidecar_failure_with_successful_commit_keeps_matching_row_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    setup = _install_fake_transaction(
        tmp_path,
        monkeypatch,
        status=status,
        commit_failure=None,
    )
    _fail_sidecar_writes(monkeypatch)

    repaired = runtime_module._prepare_corrupt_local_sources_for_reacquisition_sync(
        setup.spec.hour_utc,
    )

    assert repaired == 1
    assert not setup.canonical.exists()
    assert setup.counters.commits == 1
    assert setup.counters.rollbacks == 0

    quarantined = list(setup.quarantine_root.rglob("*.parquet"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == setup.content

    if status == "processed":
        assert setup.row.status == "processed"
        assert setup.row.local_path is None
    else:
        assert setup.row.status == "error"


def test_move_failure_rolls_back_and_reports_source_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = _install_fake_transaction(
        tmp_path,
        monkeypatch,
        status="downloaded",
        commit_failure=None,
    )

    def fail_move(*_args, **_kwargs):
        raise QuarantineError("simulated move failure")

    monkeypatch.setattr(runtime_module, "quarantine_file", fail_move)
    failures: list[str] = []

    repaired = runtime_module._prepare_corrupt_local_sources_for_reacquisition_sync(
        setup.spec.hour_utc,
        failed_sources=failures,
    )

    assert repaired == 0
    assert setup.canonical.read_bytes() == setup.content
    assert setup.counters.commits == 0
    assert setup.counters.rollbacks == 1
    assert failures == [setup.spec.remote_path]

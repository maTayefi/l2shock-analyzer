from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from l2shock.maintenance_actions import (
    MaintenanceActionError,
    MaintenanceActionKind,
    MaintenanceActionReport,
    MaintenancePreview,
    MaintenancePreviewExpiredError,
    _preview_candidate_identities,
    _preview_token,
)


def _time(
    minute: int = 0,
) -> datetime:
    return datetime(
        2026,
        9,
        11,
        12,
        minute,
        tzinfo=timezone.utc,
    )


def _items() -> tuple[dict[str, object], ...]:
    return (
        {
            "source_hour_id": 1,
            "provider": "cryptohftdata",
            "venue": "binance_futures",
            "data_kind": "orderbook",
            "instrument": "BTCUSDT",
            "hour_utc": "2026-09-10T12:00:00+00:00",
            "current_status": "processing",
            "recovery_status": "downloaded",
        },
    )


def _preview() -> MaintenancePreview:
    items = _items()
    token = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
    )

    return MaintenancePreview(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        expires_at_utc=_time(15),
        token=token,
        candidate_count=1,
        candidate_bytes=0,
        blocked_count=0,
        items=items,
    )


def test_preview_token_is_deterministic() -> None:
    items = _items()

    first = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
    )
    second = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
    )

    assert first == second
    assert len(first) == 64


def test_preview_requires_complete_item_count() -> None:
    items = _items()
    token = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
    )

    with pytest.raises(
        MaintenanceActionError,
        match="candidate_count",
    ):
        MaintenancePreview(
            action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
            generated_at_utc=_time(),
            expires_at_utc=_time(15),
            token=token,
            candidate_count=2,
            candidate_bytes=0,
            blocked_count=0,
            items=items,
        )


def test_preview_is_json_safe() -> None:
    payload = _preview().to_dict()

    assert payload["schema"] == "l2shock.maintenance_action"
    assert payload["kind"] == "preview"
    assert payload["action"] == "recover_stale_sources"
    assert payload["candidate_count"] == 1
    assert payload["destructive"] is True


def test_audit_never_claims_analytical_mutation() -> None:
    report = MaintenanceActionReport(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        preview_token="a" * 64,
        started_at_utc=_time(),
        completed_at_utc=_time(1),
        attempted_count=1,
        succeeded_count=1,
        failed_count=0,
        affected_bytes=0,
        succeeded=({"source_hour_id": 1},),
        failed=(),
    )

    payload = report.to_dict()

    assert payload["status"] == "ok"
    assert payload["analytical_row_deletion_performed"] is False
    assert payload["analytical_row_repair_performed"] is False


def test_expired_preview_error_is_specific() -> None:
    assert issubclass(
        MaintenancePreviewExpiredError,
        MaintenanceActionError,
    )


def test_preview_validity_must_be_positive() -> None:
    items = _items()
    token = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
    )

    with pytest.raises(
        MaintenanceActionError,
        match="expiration",
    ):
        MaintenancePreview(
            action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
            generated_at_utc=_time(),
            expires_at_utc=_time(),
            token=token,
            candidate_count=1,
            candidate_bytes=0,
            blocked_count=0,
            items=items,
        )


def test_preview_policy_seconds_must_be_positive() -> None:
    items = _items()
    token = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
    )

    with pytest.raises(
        MaintenanceActionError,
        match="stale_processing_after_seconds",
    ):
        MaintenancePreview(
            action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
            generated_at_utc=_time(),
            expires_at_utc=_time(15),
            token=token,
            candidate_count=1,
            candidate_bytes=0,
            blocked_count=0,
            items=items,
            stale_processing_after_seconds=0,
        )


def test_preview_token_includes_maintenance_policy() -> None:
    items = _items()

    first = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
        stale_processing_after_seconds=21_600,
    )
    second = _preview_token(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        generated_at_utc=_time(),
        items=items,
        stale_processing_after_seconds=21_601,
    )

    assert first != second


def test_maintenance_audit_reports_truncated_items() -> None:
    report = MaintenanceActionReport(
        action=MaintenanceActionKind.RECOVER_STALE_SOURCES,
        preview_token="a" * 64,
        started_at_utc=_time(),
        completed_at_utc=_time(1),
        attempted_count=3,
        succeeded_count=2,
        failed_count=1,
        affected_bytes=0,
        succeeded=({"source_hour_id": 1},),
        failed=(),
    )

    payload = report.to_dict()

    assert payload["succeeded_omitted_count"] == 1
    assert payload["failed_omitted_count"] == 1
    assert payload["audit_items_truncated"] is True


def test_preview_token_includes_raw_retention_policy() -> None:
    items = _items()

    first = _preview_token(
        action=MaintenanceActionKind.PRUNE_RAW_FILES,
        generated_at_utc=_time(),
        items=items,
        raw_retention_hours=72,
    )
    second = _preview_token(
        action=MaintenanceActionKind.PRUNE_RAW_FILES,
        generated_at_utc=_time(),
        items=items,
        raw_retention_hours=73,
    )

    assert first != second


def test_preview_raw_retention_hours_must_be_positive() -> None:
    items = _items()
    token = _preview_token(
        action=MaintenanceActionKind.PRUNE_RAW_FILES,
        generated_at_utc=_time(),
        items=items,
    )

    with pytest.raises(
        MaintenanceActionError,
        match="raw_retention_hours",
    ):
        MaintenancePreview(
            action=MaintenanceActionKind.PRUNE_RAW_FILES,
            generated_at_utc=_time(),
            expires_at_utc=_time(15),
            token=token,
            candidate_count=1,
            candidate_bytes=0,
            blocked_count=0,
            items=items,
            raw_retention_hours=0,
        )


def test_stale_preview_identity_owns_source_row_not_recovery_classification() -> None:
    original = (
        {
            "source_hour_id": 17,
            "provider": "cryptohftdata",
            "venue": "binance_futures",
            "data_kind": "orderbook",
            "instrument": "BTCUSDT",
            "hour_utc": "2026-09-10T12:00:00+00:00",
            "current_status": "processing",
            "recovery_status": "downloaded",
        },
    )
    reclassified = (
        {
            **original[0],
            "recovery_status": "error",
        },
    )

    assert _preview_candidate_identities(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        original,
    ) == _preview_candidate_identities(
        MaintenanceActionKind.RECOVER_STALE_SOURCES,
        reclassified,
    )


def test_raw_pruning_preview_identity_changes_with_candidate_path() -> None:
    first = (
        {
            "provider": "cryptohftdata",
            "venue": "binance_futures",
            "data_kind": "orderbook",
            "instrument": "BTCUSDT",
            "hour_utc": "2026-09-10T12:00:00+00:00",
            "local_path": r"C:\data\raw\first.parquet",
            "file_size_bytes": 100,
            "content_sha256": "a" * 64,
            "processed_at": "2026-09-11T12:00:00+00:00",
        },
    )
    second = (
        {
            **first[0],
            "local_path": r"C:\data\raw\second.parquet",
        },
    )

    assert _preview_candidate_identities(
        MaintenanceActionKind.PRUNE_RAW_FILES,
        first,
    ) != _preview_candidate_identities(
        MaintenanceActionKind.PRUNE_RAW_FILES,
        second,
    )


def test_checkpoint_preview_identity_changes_even_when_size_is_unchanged() -> None:
    first = (
        {
            "path": r"C:\cache\checkpoint-a.l2checkpoint",
            "provider": "cryptohftdata",
            "venue": "binance_futures",
            "instrument": "BTCUSDT",
            "through_hour_utc": "2026-09-10T12:00:00+00:00",
            "content_sha256": "a" * 64,
            "size_bytes": 4096,
            "age_seconds": 100_000.0,
        },
    )
    second = (
        {
            **first[0],
            "path": r"C:\cache\checkpoint-b.l2checkpoint",
            "content_sha256": "b" * 64,
        },
    )

    assert _preview_candidate_identities(
        MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS,
        first,
    ) != _preview_candidate_identities(
        MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS,
        second,
    )


def test_audit_raw_pruning_database_failure_restores_earlier_file(
    tmp_path,
    monkeypatch,
):
    import hashlib
    from contextlib import contextmanager
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace

    import pytest
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.orm import Session

    import l2shock.maintenance_actions as module
    from l2shock.acquisition import SourceFileSpec

    raw_root = tmp_path / "raw"
    raw_root.mkdir()

    start = datetime(2095, 1, 1, 12, tzinfo=timezone.utc)
    items = []
    specs = {}
    rows = {}
    original_contents = {}

    for offset in range(2):
        spec = SourceFileSpec(
            provider="cryptohftdata",
            venue="binance_futures",
            symbol="BTCUSDT",
            data_kind="orderbook",
            hour_utc=start + timedelta(hours=offset),
        )
        path = spec.local_path(raw_root)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = f"audit-pruning-source-{offset}".encode("ascii")
        path.write_bytes(content)
        digest = hashlib.sha256(content).hexdigest()

        item = {
            "local_path": str(path),
            "file_size_bytes": len(content),
            "content_sha256": digest,
        }
        items.append(item)
        specs[str(path)] = spec
        rows[spec.identity_tuple] = SimpleNamespace(
            status="processed",
            local_path=str(path),
            file_size_bytes=len(content),
            content_sha256=digest,
        )
        original_contents[path] = content

    reached_successful_exit = []

    @contextmanager
    def scope():
        with Session() as session:
            try:
                yield session
            except BaseException:
                session.rollback()
                raise
            else:
                reached_successful_exit.append(True)

    lock_calls = []

    def acquire_lock(_session, spec):
        lock_calls.append(spec)
        if len(lock_calls) == 2:
            raise DBAPIError(
                "audit simulated source-lock statement",
                {},
                RuntimeError("audit simulated database failure"),
            )

    monkeypatch.setattr(module, "session_scope", scope)
    monkeypatch.setattr(
        module,
        "_source_spec_from_item",
        lambda item: specs[str(item["local_path"])],
    )
    monkeypatch.setattr(
        module,
        "_source_row_for_spec",
        lambda _session, spec: rows[spec.identity_tuple],
    )
    monkeypatch.setattr(
        module,
        "acquire_source_hour_transaction_lock",
        acquire_lock,
    )

    settings = SimpleNamespace(
        storage=SimpleNamespace(raw_path=raw_root),
    )
    preview = SimpleNamespace(items=tuple(items))

    with pytest.raises(DBAPIError):
        module._execute_raw_pruning(preview, settings=settings)

    assert len(lock_calls) == 2
    assert reached_successful_exit == []

    for path, content in original_contents.items():
        assert path.is_file()
        assert path.read_bytes() == content

    assert list(raw_root.rglob("*.pruning")) == []

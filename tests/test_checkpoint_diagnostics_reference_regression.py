from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

import l2shock.maintenance_diagnostics as diagnostics
from l2shock.ingest import BookSide, CheckpointLevel, OrderBookCheckpoint
from l2shock.processing.checkpoint_references import (
    CheckpointReferenceError,
    collect_referenced_checkpoint_sha256s,
)
from l2shock.processing.checkpoint_store import CheckpointStore


def _hour() -> datetime:
    return datetime(2094, 1, 1, 12, tzinfo=timezone.utc)


def _checkpoint() -> OrderBookCheckpoint:
    return OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(),
        last_update_id=100,
        levels=(
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("100"),
                quantity=Decimal("2"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("101"),
                quantity=Decimal("3"),
                order_count=None,
            ),
        ),
        source_content_sha256="a" * 64,
    )


class _Rows:
    def __init__(self, values) -> None:
        self.values = list(values)

    def all(self):
        return list(self.values)


def _session(
    monkeypatch: pytest.MonkeyPatch,
    *,
    qualities: tuple[object, ...],
    provenances: tuple[object, ...] = (),
) -> Session:
    session = Session()
    source_rows = [
        (
            "cryptohftdata",
            "binance_futures",
            "BTCUSDT",
            _hour(),
            "processed",
            quality,
        )
        for quality in qualities
    ]

    monkeypatch.setattr(
        session,
        "execute",
        lambda *_args, **_kwargs: _Rows(source_rows),
    )

    scalar_batches = iter((qualities, provenances))
    monkeypatch.setattr(
        session,
        "scalars",
        lambda *_args, **_kwargs: _Rows(next(scalar_batches)),
    )
    return session


@pytest.mark.parametrize(
    "quality",
    (
        {"output_checkpoint_content_sha256": "A" * 64},
        {"output_checkpoint_content_sha256": "abc"},
        {"input_checkpoint_content_sha256": 0},
        {"remote_input_checkpoint_content_sha256": "bad"},
        {"remote_output_checkpoint_content_sha256": "bad"},
        [],
        "not-an-object",
    ),
)
def test_malformed_source_reference_does_not_abort_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    quality: object,
) -> None:
    cache_root = tmp_path / "cache"
    artifact = CheckpointStore(cache_root).publish(_checkpoint())
    before = artifact.path.read_bytes()

    with _session(monkeypatch, qualities=(quality,)) as session:
        report = diagnostics.checkpoint_storage_diagnostics(
            session,
            cache_root=cache_root,
        )

    assert report["valid_artifact_count"] == 1
    assert report["malformed_source_reference_count"] == 1
    assert report["reference_graph_complete"] is False
    assert report["reference_graph_error"] == "CheckpointReferenceError"
    assert report["reference_graph_error_count"] == 1
    assert report["orphan_classification_available"] is False
    assert report["orphan_artifact_count"] == 0
    assert report["orphan_artifacts"] == []
    assert artifact.path.read_bytes() == before

    json.dumps(report, allow_nan=False)


@pytest.mark.parametrize(
    "provenance",
    (
        [],
        {"checkpoint_content_sha256": "bad"},
    ),
)
def test_malformed_l2_provenance_disables_orphan_classification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provenance: object,
) -> None:
    cache_root = tmp_path / "cache"
    artifact = CheckpointStore(cache_root).publish(_checkpoint())
    digest = artifact.encoding_info.content_sha256

    with _session(
        monkeypatch,
        qualities=({"output_checkpoint_content_sha256": digest},),
        provenances=(provenance,),
    ) as session:
        report = diagnostics.checkpoint_storage_diagnostics(
            session,
            cache_root=cache_root,
        )

    assert report["valid_artifact_count"] == 1
    assert report["missing_expected_count"] == 0
    assert report["malformed_source_reference_count"] == 0
    assert report["reference_graph_complete"] is False
    assert report["reference_graph_error_count"] == 1
    assert report["orphan_classification_available"] is False
    assert report["orphan_artifacts"] == []


def test_known_missing_output_is_still_reported_with_incomplete_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _session(
        monkeypatch,
        qualities=(
            {
                "output_checkpoint_content_sha256": "b" * 64,
                "input_checkpoint_content_sha256": "bad",
            },
        ),
    ) as session:
        report = diagnostics.checkpoint_storage_diagnostics(
            session,
            cache_root=tmp_path / "absent-cache",
        )

    assert report["reference_graph_complete"] is False
    assert report["missing_expected_count"] == 1
    assert report["missing_expected"][0]["content_sha256"] == "b" * 64
    assert report["orphan_artifacts"] == []
    assert not (tmp_path / "absent-cache").exists()


@pytest.mark.parametrize("referenced", (False, True))
def test_complete_graph_preserves_existing_orphan_classification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    referenced: bool,
) -> None:
    cache_root = tmp_path / "cache"
    artifact = CheckpointStore(cache_root).publish(_checkpoint())
    digest = artifact.encoding_info.content_sha256
    quality = (
        {"output_checkpoint_content_sha256": digest}
        if referenced
        else {}
    )

    with _session(monkeypatch, qualities=(quality,)) as session:
        report = diagnostics.checkpoint_storage_diagnostics(
            session,
            cache_root=cache_root,
        )

    assert report["reference_graph_complete"] is True
    assert report["reference_graph_error"] is None
    assert report["reference_graph_error_count"] == 0
    assert report["orphan_classification_available"] is True
    assert report["orphan_artifact_count"] == (0 if referenced else 1)
    assert report["missing_expected_count"] == 0
    assert artifact.path.is_file()


def test_strict_reference_collector_still_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _session(
        monkeypatch,
        qualities=({"output_checkpoint_content_sha256": "bad"},),
    ) as session:
        with pytest.raises(CheckpointReferenceError):
            collect_referenced_checkpoint_sha256s(session)


def test_complete_report_requires_attention_for_malformed_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def empty_section(*_args, **_kwargs):
        return defaultdict(int)

    for name in (
        "stale_source_diagnostics",
        "stale_fetch_run_diagnostics",
        "analytical_consistency_diagnostics",
        "performance_diagnostics",
    ):
        monkeypatch.setattr(diagnostics, name, empty_section)

    with _session(
        monkeypatch,
        qualities=(),
        provenances=({"checkpoint_content_sha256": "bad"},),
    ) as session:
        report = diagnostics.maintenance_diagnostics_report(
            session,
            cache_root=tmp_path / "cache",
            generated_at_utc=_hour(),
        )

    assert report["ok"] is False
    assert report["attention_count"] == 1
    assert report["checkpoint_storage"]["reference_graph_complete"] is False


def test_unrelated_reference_query_failure_is_not_hidden(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(_session):
        raise RuntimeError("simulated database query failure")

    monkeypatch.setattr(
        diagnostics,
        "collect_referenced_checkpoint_sha256s",
        unavailable,
    )

    with _session(monkeypatch, qualities=()) as session:
        with pytest.raises(RuntimeError, match="simulated database"):
            diagnostics.checkpoint_storage_diagnostics(
                session,
                cache_root=tmp_path / "cache",
            )
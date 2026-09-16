# l2shock/ingest/reporting.py
"""JSON-safe reporting for streamed source reads and replay validation."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from l2shock.ingest.parquet_reader import (
    OrderBookReadReport,
    StreamOrderingIssue,
)
from l2shock.ingest.replay import (
    OrderBookCheckpoint,
    ReplayArchiveReport,
    ReplayChainReport,
    ReplayIssue,
)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("JSON report datetime must be timezone-aware")

    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _ordering_issue_to_dict(
    issue: StreamOrderingIssue,
) -> dict[str, Any]:
    return {
        "kind": issue.kind,
        "previous_row_number": issue.previous_row_number,
        "current_row_number": issue.current_row_number,
        "previous_value": issue.previous_value,
        "current_value": issue.current_value,
    }


def _replay_issue_to_dict(
    issue: ReplayIssue,
) -> dict[str, Any]:
    return {
        "kind": issue.kind,
        "hour_utc": _utc_text(issue.hour_utc),
        "row_number": issue.row_number,
        "previous_update_id": issue.previous_update_id,
        "current_update_id": issue.current_update_id,
        "message": issue.message,
    }


def orderbook_read_report_to_dict(
    report: OrderBookReadReport,
) -> dict[str, Any]:
    """Return one JSON-safe streamed-reader report."""
    return {
        "path": str(report.path),
        "symbol": report.symbol,
        "rows_read": report.rows_read,
        "events_read": report.events_read,
        "update_event_count": report.update_event_count,
        "snapshot_event_count": report.snapshot_event_count,
        "has_snapshot": report.has_snapshot,
        "first_received_time_ns": report.first_received_time_ns,
        "last_received_time_ns": report.last_received_time_ns,
        "first_event_time_ms": report.first_event_time_ms,
        "last_event_time_ms": report.last_event_time_ms,
        "continuity_checks": report.continuity_checks,
        "continuity_mismatch_count": (report.continuity_mismatch_count),
        "received_time_regression_count": (report.received_time_regression_count),
        "event_time_regression_count": (report.event_time_regression_count),
        "update_id_regression_count": (report.update_id_regression_count),
        "has_continuity_mismatch": (report.has_continuity_mismatch),
        "has_ordering_regression": (report.has_ordering_regression),
        "retained_issue_limit": report.retained_issue_limit,
        "retained_issues": [
            _ordering_issue_to_dict(issue) for issue in report.retained_issues
        ],
        "batch_size": report.batch_size,
    }


def _checkpoint_summary(
    checkpoint: OrderBookCheckpoint | None,
) -> dict[str, Any] | None:
    if checkpoint is None:
        return None

    return {
        "provider": checkpoint.provider,
        "venue": checkpoint.venue,
        "symbol": checkpoint.symbol,
        "through_hour_utc": _utc_text(checkpoint.through_hour_utc),
        "next_hour_utc": _utc_text(checkpoint.next_hour_utc),
        "last_update_id": checkpoint.last_update_id,
        "level_count": len(checkpoint.levels),
        "bid_level_count": checkpoint.bid_level_count,
        "ask_level_count": checkpoint.ask_level_count,
        "source_content_sha256": (checkpoint.source_content_sha256),
    }


def replay_archive_report_to_dict(
    report: ReplayArchiveReport,
) -> dict[str, Any]:
    """Return one JSON-safe archive replay report."""
    return {
        "path": str(report.path),
        "source": {
            "provider": report.spec.provider,
            "venue": report.spec.venue,
            "symbol": report.spec.symbol,
            "data_kind": report.spec.data_kind.value,
            "hour_utc": _utc_text(report.spec.hour_utc),
            "remote_path": report.spec.remote_path,
        },
        "reader_report": orderbook_read_report_to_dict(report.reader_report),
        "initial_state": report.initial_state.value,
        "final_state": report.final_state.value,
        "initially_valid": report.initially_valid,
        "finally_valid": report.finally_valid,
        "independently_initialized": (report.independently_initialized),
        "required_carried_state": (report.required_carried_state),
        "events_seen": report.events_seen,
        "snapshots_applied": report.snapshots_applied,
        "updates_applied": report.updates_applied,
        "updates_skipped_uninitialized": (report.updates_skipped_uninitialized),
        "duplicate_updates_skipped": (report.duplicate_updates_skipped),
        "invalidation_count": report.invalidation_count,
        "recovery_count": report.recovery_count,
        "zero_quantity_change_count": (report.zero_quantity_change_count),
        "levels_removed_count": report.levels_removed_count,
        "initial_update_id": report.initial_update_id,
        "final_update_id": report.final_update_id,
        "final_bid_level_count": (report.final_bid_level_count),
        "final_ask_level_count": (report.final_ask_level_count),
        "structure_counts": {
            structure.value: count for structure, count in report.structure_counts
        },
        "structural_anomaly_count": (report.structural_anomaly_count),
        "retained_issue_limit": report.retained_issue_limit,
        "retained_issues": [
            _replay_issue_to_dict(issue) for issue in report.retained_issues
        ],
    }


def replay_chain_report_to_dict(
    report: ReplayChainReport,
) -> dict[str, Any]:
    """Return a complete JSON-safe replay-chain report."""
    return {
        "schema": "l2shock.replay_validation_report",
        "schema_version": 1,
        "generated_at_utc": _utc_text(datetime.now(timezone.utc)),
        "provider": report.provider,
        "venue": report.venue,
        "symbol": report.symbol,
        "archive_count": len(report.archives),
        "finally_valid": report.finally_valid,
        "total_events_seen": report.total_events_seen,
        "total_invalidations": report.total_invalidations,
        "total_snapshots_applied": (report.total_snapshots_applied),
        "final_checkpoint": _checkpoint_summary(report.final_checkpoint),
        "archives": [
            replay_archive_report_to_dict(archive) for archive in report.archives
        ],
    }


def write_json_report_atomic(
    path: Path,
    report: ReplayChainReport | dict[str, Any],
    *,
    overwrite: bool = False,
) -> Path:
    """Atomically write one deterministic, human-readable JSON report."""
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and not overwrite:
        raise FileExistsError(f"JSON report already exists: {destination}")

    payload = (
        replay_chain_report_to_dict(report)
        if isinstance(report, ReplayChainReport)
        else dict(report)
    )

    encoded = (
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")

    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

        if overwrite:
            os.replace(temporary, destination)
        else:
            try:
                os.link(temporary, destination)
            except FileExistsError as exc:
                raise FileExistsError(
                    f"JSON report already exists: {destination}"
                ) from exc
            else:
                temporary.unlink()

    finally:
        temporary.unlink(missing_ok=True)

    return destination


__all__ = [
    "orderbook_read_report_to_dict",
    "replay_archive_report_to_dict",
    "replay_chain_report_to_dict",
    "write_json_report_atomic",
]

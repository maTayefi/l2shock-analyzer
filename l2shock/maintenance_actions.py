# l2shock/maintenance_actions.py
"""Explicitly authorized destructive maintenance actions.

This module is intentionally separate from ``maintenance_diagnostics``.
Diagnostics remain read-only; every action here requires:

1. a fresh preview;
2. a matching preview token;
3. explicit caller confirmation;
4. process-wide operation admission owned by the caller;
5. a final fail-closed revalidation immediately before mutation.

This module never deletes or repairs analytical rows.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Final
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from l2shock.acquisition.locks import (
    acquire_source_hour_transaction_lock,
)
from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.persistence import (
    SourceHourStatus,
    validate_source_hour_transition,
)
from l2shock.acquisition.retention import (
    RawRetentionPlan,
    plan_processed_raw_retention,
)
from l2shock.acquisition.validation import sha256_file
from l2shock.config import Settings, get_settings
from l2shock.db.checkpoint_reference_locks import (
    acquire_checkpoint_reference_transaction_lock,
)
from l2shock.db.engine import session_scope
from l2shock.db.models import SourceHour
from l2shock.ingest.checkpoint_codec import (
    checkpoint_encoding_info,
    encode_checkpoint,
    load_checkpoint_file,
)
from l2shock.processing.checkpoint_references import (
    collect_referenced_checkpoint_sha256s,
)
from l2shock.processing.checkpoint_store import (
    CheckpointIdentity,
    CheckpointStore,
)
from l2shock.timeutils import now_utc, require_aware_utc

MAINTENANCE_ACTION_SCHEMA: Final[str] = "l2shock.maintenance_action"
MAINTENANCE_ACTION_SCHEMA_VERSION: Final[int] = 1

DEFAULT_PREVIEW_VALIDITY: Final[timedelta] = timedelta(minutes=15)
DEFAULT_STALE_DOWNLOADING_AFTER: Final[timedelta] = timedelta(hours=2)
DEFAULT_STALE_PROCESSING_AFTER: Final[timedelta] = timedelta(hours=6)
DEFAULT_ORPHAN_CHECKPOINT_MINIMUM_AGE: Final[timedelta] = timedelta(hours=24)

_MAX_AUDIT_ITEMS: Final[int] = 500


class MaintenanceActionError(RuntimeError):
    """An explicitly requested maintenance action could not run safely."""


class MaintenancePreviewExpiredError(MaintenanceActionError):
    """The destructive preview is no longer fresh enough to execute."""


class MaintenancePreviewChangedError(MaintenanceActionError):
    """The eligible destructive set changed after preview generation."""


class MaintenanceActionKind(StrEnum):
    """Supported explicitly authorized maintenance operations."""

    RECOVER_STALE_SOURCES = "recover_stale_sources"
    DELETE_ORPHAN_CHECKPOINTS = "delete_orphan_checkpoints"
    PRUNE_RAW_FILES = "prune_raw_files"


def _positive_timedelta(
    field_name: str,
    value: object,
) -> timedelta:
    if not isinstance(value, timedelta) or value <= timedelta(0):
        raise MaintenanceActionError(f"{field_name} must be a positive timedelta")

    return value


def _utc_text(value: datetime) -> str:
    return require_aware_utc(
        "maintenance datetime",
        value,
    ).isoformat()


def _canonical_sha256_or_none(
    value: object,
) -> str | None:
    if value is None:
        return None

    digest = str(value).strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        return None

    return digest


def _canonical_payload_bytes(
    value: Mapping[str, object],
) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _preview_token(
    *,
    action: MaintenanceActionKind,
    generated_at_utc: datetime,
    items: Sequence[Mapping[str, object]],
    stale_downloading_after_seconds: int = 7_200,
    stale_processing_after_seconds: int = 21_600,
    orphan_checkpoint_minimum_age_seconds: int = 86_400,
    raw_retention_hours: int = 72,
) -> str:
    policy: dict[str, int] = {}

    for field_name, value in (
        (
            "stale_downloading_after_seconds",
            stale_downloading_after_seconds,
        ),
        (
            "stale_processing_after_seconds",
            stale_processing_after_seconds,
        ),
        (
            "orphan_checkpoint_minimum_age_seconds",
            orphan_checkpoint_minimum_age_seconds,
        ),
        (
            "raw_retention_hours",
            raw_retention_hours,
        ),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise MaintenanceActionError(f"{field_name} must be a positive integer")

        policy[field_name] = value

    payload = {
        "schema": MAINTENANCE_ACTION_SCHEMA,
        "schema_version": MAINTENANCE_ACTION_SCHEMA_VERSION,
        "action": action.value,
        "generated_at_utc": _utc_text(generated_at_utc),
        "policy": policy,
        "items": [dict(item) for item in items],
    }

    return hashlib.sha256(_canonical_payload_bytes(payload)).hexdigest()


def _safe_exception_name(
    exc: BaseException,
) -> str:
    return f"Unexpected {type(exc).__name__}"


def _bounded_items(
    values: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    return tuple(dict(value) for value in values[:_MAX_AUDIT_ITEMS])


def _preview_candidate_identities(
    action: MaintenanceActionKind,
    items: Sequence[Mapping[str, object]],
) -> tuple[str, ...]:
    """Return deterministic authorization identities for preview candidates.

    Stale-source execution deliberately owns the exact source-row identity
    while revalidating mutable filesystem-derived recovery facts under the
    source lock.

    Checkpoint deletion and raw pruning own the complete preview item because
    the user authorized those exact destructive filesystem candidates.
    """

    selected_action = MaintenanceActionKind(action)
    identities: list[str] = []

    for index, raw_item in enumerate(items):
        if not isinstance(raw_item, Mapping):
            raise MaintenanceActionError(
                f"Maintenance candidate {index} must be an object mapping"
            )

        item = dict(raw_item)

        if selected_action is MaintenanceActionKind.RECOVER_STALE_SOURCES:
            source_hour_id = item.get("source_hour_id")

            if (
                isinstance(source_hour_id, bool)
                or not isinstance(source_hour_id, int)
                or source_hour_id <= 0
            ):
                raise MaintenanceActionError(
                    "Stale-source candidate requires a positive source_hour_id"
                )

            identity = f"source_hour:{source_hour_id}"
        else:
            item_digest = hashlib.sha256(_canonical_payload_bytes(item)).hexdigest()
            identity = f"candidate:{item_digest}"

        identities.append(identity)

    if len(set(identities)) != len(identities):
        raise MaintenanceActionError(
            "Maintenance preview contains duplicate candidate identities"
        )

    return tuple(sorted(identities))


@dataclass(frozen=True, slots=True)
class MaintenancePreview:
    """Immutable confirmation material for one destructive action."""

    action: MaintenanceActionKind
    generated_at_utc: datetime
    expires_at_utc: datetime
    token: str

    candidate_count: int
    candidate_bytes: int
    blocked_count: int

    items: tuple[dict[str, object], ...]

    stale_downloading_after_seconds: int = 7_200
    stale_processing_after_seconds: int = 21_600
    orphan_checkpoint_minimum_age_seconds: int = 86_400
    raw_retention_hours: int = 72

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "action",
            MaintenanceActionKind(self.action),
        )

        generated = require_aware_utc(
            "generated_at_utc",
            self.generated_at_utc,
        )
        expires = require_aware_utc(
            "expires_at_utc",
            self.expires_at_utc,
        )

        if expires <= generated:
            raise MaintenanceActionError(
                "Maintenance preview expiration must follow generation"
            )

        token = str(self.token or "").strip()

        if (
            token != token.lower()
            or len(token) != 64
            or any(character not in "0123456789abcdef" for character in token)
        ):
            raise MaintenanceActionError(
                "Maintenance preview token must be a canonical lowercase SHA-256"
            )

        for field_name in (
            "candidate_count",
            "candidate_bytes",
            "blocked_count",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise MaintenanceActionError(
                    f"{field_name} must be a non-negative integer"
                )

        for field_name in (
            "stale_downloading_after_seconds",
            "stale_processing_after_seconds",
            "orphan_checkpoint_minimum_age_seconds",
            "raw_retention_hours",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise MaintenanceActionError(f"{field_name} must be a positive integer")

        items = tuple(dict(item) for item in self.items)

        if self.candidate_count != len(items):
            raise MaintenanceActionError(
                "candidate_count must equal the complete preview item count"
            )

        object.__setattr__(self, "generated_at_utc", generated)
        object.__setattr__(self, "expires_at_utc", expires)
        object.__setattr__(self, "token", token)
        object.__setattr__(self, "items", items)

    @property
    def destructive(self) -> bool:
        return self.candidate_count > 0

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": MAINTENANCE_ACTION_SCHEMA,
            "schema_version": MAINTENANCE_ACTION_SCHEMA_VERSION,
            "kind": "preview",
            "action": self.action.value,
            "generated_at_utc": self.generated_at_utc.isoformat(),
            "expires_at_utc": self.expires_at_utc.isoformat(),
            "token": self.token,
            "candidate_count": self.candidate_count,
            "candidate_bytes": self.candidate_bytes,
            "blocked_count": self.blocked_count,
            "destructive": self.destructive,
            "policy": {
                "stale_downloading_after_seconds": (
                    self.stale_downloading_after_seconds
                ),
                "stale_processing_after_seconds": (self.stale_processing_after_seconds),
                "orphan_checkpoint_minimum_age_seconds": (
                    self.orphan_checkpoint_minimum_age_seconds
                ),
                "raw_retention_hours": self.raw_retention_hours,
            },
            "items": [dict(item) for item in self.items],
        }


@dataclass(frozen=True, slots=True)
class MaintenanceActionReport:
    """JSON-safe audit result for one authorized maintenance execution."""

    action: MaintenanceActionKind
    preview_token: str
    started_at_utc: datetime
    completed_at_utc: datetime

    attempted_count: int
    succeeded_count: int
    failed_count: int
    affected_bytes: int

    succeeded: tuple[dict[str, object], ...]
    failed: tuple[dict[str, object], ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "action",
            MaintenanceActionKind(self.action),
        )

        token = str(self.preview_token or "").strip()

        if (
            token != token.lower()
            or len(token) != 64
            or any(character not in "0123456789abcdef" for character in token)
        ):
            raise MaintenanceActionError(
                "preview_token must be a canonical lowercase SHA-256"
            )

        started = require_aware_utc(
            "started_at_utc",
            self.started_at_utc,
        )
        completed = require_aware_utc(
            "completed_at_utc",
            self.completed_at_utc,
        )

        if completed < started:
            raise MaintenanceActionError(
                "Maintenance completion cannot precede its start"
            )

        for field_name in (
            "attempted_count",
            "succeeded_count",
            "failed_count",
            "affected_bytes",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise MaintenanceActionError(
                    f"{field_name} must be a non-negative integer"
                )

        if self.succeeded_count + self.failed_count != self.attempted_count:
            raise MaintenanceActionError(
                "Maintenance action accounting is inconsistent"
            )

        object.__setattr__(self, "started_at_utc", started)
        object.__setattr__(self, "completed_at_utc", completed)
        object.__setattr__(
            self,
            "succeeded",
            tuple(dict(item) for item in self.succeeded),
        )
        object.__setattr__(
            self,
            "failed",
            tuple(dict(item) for item in self.failed),
        )

    @property
    def status(self) -> str:
        if self.failed_count == 0:
            return "ok"

        if self.succeeded_count > 0:
            return "partial_ok"

        return "error"

    @property
    def succeeded_omitted_count(self) -> int:
        return max(
            0,
            self.succeeded_count - len(self.succeeded),
        )

    @property
    def failed_omitted_count(self) -> int:
        return max(
            0,
            self.failed_count - len(self.failed),
        )

    @property
    def audit_items_truncated(self) -> bool:
        return bool(self.succeeded_omitted_count or self.failed_omitted_count)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": MAINTENANCE_ACTION_SCHEMA,
            "schema_version": MAINTENANCE_ACTION_SCHEMA_VERSION,
            "kind": "audit",
            "action": self.action.value,
            "preview_token": self.preview_token,
            "started_at_utc": self.started_at_utc.isoformat(),
            "completed_at_utc": self.completed_at_utc.isoformat(),
            "status": self.status,
            "attempted_count": self.attempted_count,
            "succeeded_count": self.succeeded_count,
            "failed_count": self.failed_count,
            "affected_bytes": self.affected_bytes,
            "succeeded": [dict(item) for item in self.succeeded],
            "failed": [dict(item) for item in self.failed],
            "succeeded_omitted_count": (self.succeeded_omitted_count),
            "failed_omitted_count": (self.failed_omitted_count),
            "audit_items_truncated": self.audit_items_truncated,
            "analytical_row_deletion_performed": False,
            "analytical_row_repair_performed": False,
        }


def _checkpoint_files(
    root: Path,
) -> tuple[Path, ...]:
    if not root.exists():
        return ()

    if not root.is_dir():
        raise MaintenanceActionError("Configured checkpoint root is not a directory")

    values: list[Path] = []

    for current, directory_names, file_names in os.walk(
        root,
        topdown=True,
        followlinks=False,
    ):
        current_path = Path(current)

        directory_names[:] = [
            name for name in directory_names if not (current_path / name).is_symlink()
        ]

        for name in file_names:
            candidate = current_path / name

            if candidate.is_symlink():
                continue

            if candidate.is_file() and name.endswith(".l2checkpoint"):
                values.append(candidate.resolve())

    return tuple(sorted(values, key=lambda path: str(path).lower()))


def _checkpoint_reference_set(
    session: Session,
) -> frozenset[str]:
    """Return every checkpoint digest protected by committed metadata."""
    return collect_referenced_checkpoint_sha256s(session)


def _source_row_has_intact_raw_file(
    row: SourceHour,
    *,
    raw_root: Path,
) -> bool:
    """Verify canonical transient-source bytes against durable metadata."""

    raw_path = str(row.local_path or "").strip()
    expected_digest = _canonical_sha256_or_none(row.content_sha256)

    if not raw_path or row.file_size_bytes is None or expected_digest is None:
        return False

    if (
        isinstance(row.file_size_bytes, bool)
        or not isinstance(row.file_size_bytes, int)
        or row.file_size_bytes <= 0
    ):
        return False

    try:
        spec = SourceFileSpec(
            provider=row.provider,
            venue=row.venue,
            symbol=row.instrument,
            data_kind=SourceDataKind(row.data_kind),
            hour_utc=row.hour_utc,
        )

        stored_path = Path(raw_path).expanduser()

        # Inspect the claimed directory entry before any operation which
        # follows symbolic links.
        if stored_path.is_symlink():
            return False

        stored_path = stored_path.absolute()
        canonical_path = spec.local_path(raw_root)

        if stored_path != canonical_path:
            return False

        if not stored_path.is_file():
            return False

        if stored_path.stat().st_size != row.file_size_bytes:
            return False

        actual_digest, actual_size = sha256_file(stored_path)

    except Exception:
        return False

    return bool(actual_size == row.file_size_bytes and actual_digest == expected_digest)


def _plan_stale_sources(
    session: Session,
    *,
    raw_root: Path,
    generated_at_utc: datetime,
    stale_downloading_after: timedelta,
    stale_processing_after: timedelta,
) -> tuple[tuple[dict[str, object], ...], int]:
    generated = require_aware_utc(
        "generated_at_utc",
        generated_at_utc,
    )
    downloading_after = _positive_timedelta(
        "stale_downloading_after",
        stale_downloading_after,
    )
    processing_after = _positive_timedelta(
        "stale_processing_after",
        stale_processing_after,
    )

    rows = session.scalars(
        select(SourceHour)
        .where(
            SourceHour.status.in_(
                (
                    SourceHourStatus.DOWNLOADING.value,
                    SourceHourStatus.PROCESSING.value,
                )
            )
        )
        .order_by(
            SourceHour.hour_utc,
            SourceHour.instrument,
            SourceHour.data_kind,
        )
    ).all()

    items: list[dict[str, object]] = []

    for row in rows:
        status = SourceHourStatus(row.status)

        if status is SourceHourStatus.DOWNLOADING:
            anchor = require_aware_utc(
                "discovered_at",
                row.discovered_at,
            )
            threshold = downloading_after
            recovery_status = SourceHourStatus.ERROR
        else:
            anchor = require_aware_utc(
                "processing age anchor",
                row.downloaded_at or row.discovered_at,
            )
            threshold = processing_after

            has_replayable_raw = _source_row_has_intact_raw_file(
                row,
                raw_root=raw_root,
            )

            recovery_status = (
                SourceHourStatus.DOWNLOADED
                if has_replayable_raw
                else SourceHourStatus.ERROR
            )

        age_seconds = max(
            0.0,
            (generated - anchor).total_seconds(),
        )

        if age_seconds <= threshold.total_seconds():
            continue

        items.append(
            {
                "source_hour_id": int(row.id),
                "provider": str(row.provider),
                "venue": str(row.venue),
                "data_kind": str(row.data_kind),
                "instrument": str(row.instrument),
                "hour_utc": require_aware_utc(
                    "hour_utc",
                    row.hour_utc,
                ).isoformat(),
                "current_status": status.value,
                "recovery_status": recovery_status.value,
                "age_seconds": round(age_seconds, 3),
            }
        )

    return tuple(items), 0


def _plan_orphan_checkpoints(
    session: Session,
    *,
    cache_root: Path,
    generated_at_utc: datetime,
    minimum_age: timedelta,
) -> tuple[tuple[dict[str, object], ...], int, int]:
    generated = require_aware_utc(
        "generated_at_utc",
        generated_at_utc,
    )
    age = _positive_timedelta(
        "minimum_age",
        minimum_age,
    )

    store = CheckpointStore(cache_root)
    references = _checkpoint_reference_set(session)

    items: list[dict[str, object]] = []
    blocked_count = 0
    total_bytes = 0

    for path in _checkpoint_files(store.root):
        try:
            stat = path.stat()
            modified_at = datetime.fromtimestamp(
                stat.st_mtime,
                tz=generated.tzinfo,
            )
            age_seconds = max(
                0.0,
                (generated - modified_at).total_seconds(),
            )

            checkpoint = load_checkpoint_file(path)
            encoded = encode_checkpoint(checkpoint)
            info = checkpoint_encoding_info(encoded)
            identity = CheckpointIdentity.from_checkpoint(checkpoint)

            canonical = store.path_for(
                identity,
                info.content_sha256,
            ).resolve()

            if path != canonical:
                blocked_count += 1
                continue

            if info.content_sha256 in references:
                continue

            if age_seconds < age.total_seconds():
                blocked_count += 1
                continue

            total_bytes += int(stat.st_size)
            items.append(
                {
                    "path": str(path),
                    "provider": identity.provider,
                    "venue": identity.venue,
                    "instrument": identity.instrument,
                    "through_hour_utc": (identity.through_hour_utc.isoformat()),
                    "content_sha256": info.content_sha256,
                    "size_bytes": int(stat.st_size),
                    "age_seconds": round(age_seconds, 3),
                }
            )

        except Exception:
            blocked_count += 1

    return tuple(items), total_bytes, blocked_count


def _raw_retention_items(
    plan: RawRetentionPlan,
) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "provider": candidate.spec.provider,
            "venue": candidate.spec.venue,
            "data_kind": candidate.spec.data_kind.value,
            "instrument": candidate.spec.symbol,
            "hour_utc": candidate.spec.hour_utc.isoformat(),
            "local_path": str(candidate.local_path),
            "file_size_bytes": candidate.file_size_bytes,
            "content_sha256": candidate.content_sha256,
            "processed_at": candidate.processed_at.isoformat(),
        }
        for candidate in plan.candidates
    )


def build_maintenance_preview(
    action: MaintenanceActionKind | str,
    *,
    generated_at_utc: datetime | None = None,
    settings: Settings | None = None,
    preview_validity: timedelta = DEFAULT_PREVIEW_VALIDITY,
    stale_downloading_after: timedelta = DEFAULT_STALE_DOWNLOADING_AFTER,
    stale_processing_after: timedelta = DEFAULT_STALE_PROCESSING_AFTER,
    orphan_checkpoint_minimum_age: timedelta = (DEFAULT_ORPHAN_CHECKPOINT_MINIMUM_AGE),
    raw_retention_hours: int | None = None,
) -> MaintenancePreview:
    """Build a fresh read-only preview for one maintenance action."""

    selected_action = MaintenanceActionKind(action)
    generated = require_aware_utc(
        "generated_at_utc",
        generated_at_utc or now_utc(),
    )
    validity = _positive_timedelta(
        "preview_validity",
        preview_validity,
    )
    selected_settings = settings or get_settings()

    selected_raw_retention_hours = (
        selected_settings.storage.raw_retention_hours
        if raw_retention_hours is None
        else raw_retention_hours
    )

    if (
        isinstance(selected_raw_retention_hours, bool)
        or not isinstance(selected_raw_retention_hours, int)
        or selected_raw_retention_hours <= 0
    ):
        raise MaintenanceActionError("raw_retention_hours must be a positive integer")

    selected_stale_downloading_after = _positive_timedelta(
        "stale_downloading_after",
        stale_downloading_after,
    )
    selected_stale_processing_after = _positive_timedelta(
        "stale_processing_after",
        stale_processing_after,
    )
    selected_orphan_checkpoint_minimum_age = _positive_timedelta(
        "orphan_checkpoint_minimum_age",
        orphan_checkpoint_minimum_age,
    )

    with session_scope() as session:
        if selected_action is MaintenanceActionKind.RECOVER_STALE_SOURCES:
            items, blocked_count = _plan_stale_sources(
                session,
                raw_root=selected_settings.storage.raw_path,
                generated_at_utc=generated,
                stale_downloading_after=(selected_stale_downloading_after),
                stale_processing_after=(selected_stale_processing_after),
            )
            candidate_bytes = 0

        elif selected_action is MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS:
            items, candidate_bytes, blocked_count = _plan_orphan_checkpoints(
                session,
                cache_root=selected_settings.storage.cache_path,
                generated_at_utc=generated,
                minimum_age=(selected_orphan_checkpoint_minimum_age),
            )

        else:
            plan = plan_processed_raw_retention(
                session,
                raw_root=selected_settings.storage.raw_path,
                retention_hours=selected_raw_retention_hours,
                now=generated,
            )
            items = _raw_retention_items(plan)
            candidate_bytes = plan.reclaimable_bytes
            blocked_count = plan.blocked_count

    stale_downloading_after_seconds = int(
        selected_stale_downloading_after.total_seconds()
    )
    stale_processing_after_seconds = int(
        selected_stale_processing_after.total_seconds()
    )
    orphan_checkpoint_minimum_age_seconds = int(
        selected_orphan_checkpoint_minimum_age.total_seconds()
    )

    token = _preview_token(
        action=selected_action,
        generated_at_utc=generated,
        items=items,
        stale_downloading_after_seconds=(stale_downloading_after_seconds),
        stale_processing_after_seconds=(stale_processing_after_seconds),
        orphan_checkpoint_minimum_age_seconds=(orphan_checkpoint_minimum_age_seconds),
        raw_retention_hours=selected_raw_retention_hours,
    )

    return MaintenancePreview(
        action=selected_action,
        generated_at_utc=generated,
        expires_at_utc=generated + validity,
        token=token,
        candidate_count=len(items),
        candidate_bytes=candidate_bytes,
        blocked_count=blocked_count,
        items=items,
        stale_downloading_after_seconds=(stale_downloading_after_seconds),
        stale_processing_after_seconds=(stale_processing_after_seconds),
        orphan_checkpoint_minimum_age_seconds=(orphan_checkpoint_minimum_age_seconds),
        raw_retention_hours=selected_raw_retention_hours,
    )


def _rebuild_preview_for_execution(
    preview: MaintenancePreview,
    *,
    settings: Settings,
) -> MaintenancePreview:
    return build_maintenance_preview(
        preview.action,
        generated_at_utc=preview.generated_at_utc,
        settings=settings,
        preview_validity=(preview.expires_at_utc - preview.generated_at_utc),
        stale_downloading_after=timedelta(
            seconds=preview.stale_downloading_after_seconds
        ),
        stale_processing_after=timedelta(
            seconds=preview.stale_processing_after_seconds
        ),
        orphan_checkpoint_minimum_age=timedelta(
            seconds=(preview.orphan_checkpoint_minimum_age_seconds)
        ),
        raw_retention_hours=preview.raw_retention_hours,
    )


def _verify_preview(
    preview: MaintenancePreview,
    *,
    executed_at_utc: datetime,
    settings: Settings,
) -> None:
    executed = require_aware_utc(
        "executed_at_utc",
        executed_at_utc,
    )
    if executed > preview.expires_at_utc:
        raise MaintenancePreviewExpiredError(
            "Maintenance preview expired; create and review a new preview"
        )

    # 1. Self-integrity: verify the submitted token matches the submitted
    #    preview's own content.  This catches token tampering without
    #    rejecting filesystem-driven item-level changes that execution-time
    #    advisory-lock revalidation is designed to handle.
    expected_token = _preview_token(
        action=preview.action,
        generated_at_utc=preview.generated_at_utc,
        items=preview.items,
        stale_downloading_after_seconds=(preview.stale_downloading_after_seconds),
        stale_processing_after_seconds=(preview.stale_processing_after_seconds),
        orphan_checkpoint_minimum_age_seconds=(
            preview.orphan_checkpoint_minimum_age_seconds
        ),
        raw_retention_hours=preview.raw_retention_hours,
    )
    if expected_token != preview.token:
        raise MaintenancePreviewChangedError(
            "Maintenance candidates changed after preview; " "review a new preview"
        )

    # 2. Structural identity: rebuild from current state and compare the
    #    action-specific authorization identities.
    #
    # Stale-source recovery owns source-row identity and performs mutable raw
    # validation again under the source lock during execution. Checkpoint
    # deletion and raw pruning own the complete candidate item reviewed by the
    # user and therefore require exact candidate equivalence.
    current = _rebuild_preview_for_execution(
        preview,
        settings=settings,
    )
    preview_identities = _preview_candidate_identities(
        preview.action,
        preview.items,
    )
    current_identities = _preview_candidate_identities(
        current.action,
        current.items,
    )

    if (
        preview_identities != current_identities
        or current.candidate_count != preview.candidate_count
        or current.candidate_bytes != preview.candidate_bytes
        or current.blocked_count != preview.blocked_count
    ):
        raise MaintenancePreviewChangedError(
            "Maintenance candidates changed after preview; review a new preview"
        )


def _source_spec_from_item(
    item: Mapping[str, object],
) -> SourceFileSpec:
    return SourceFileSpec(
        provider=str(item["provider"]),
        venue=str(item["venue"]),
        symbol=str(item["instrument"]),
        data_kind=SourceDataKind(str(item["data_kind"])),
        hour_utc=datetime.fromisoformat(str(item["hour_utc"])),
    )


def _execute_stale_source_recovery(
    preview: MaintenancePreview,
    *,
    settings: Settings,
) -> tuple[list[dict[str, object]], list[dict[str, object]], int]:
    succeeded: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    with session_scope() as session:
        for item in preview.items:
            success_item: dict[str, object] | None = None

            try:
                # One candidate-level savepoint prevents a failed flush from
                # leaving the complete maintenance transaction unusable.
                with session.begin_nested():
                    source_id = int(item["source_hour_id"])
                    preview_spec = _source_spec_from_item(item)

                    # Lock the exact identity authorized by the preview before
                    # loading mutable ORM state.
                    acquire_source_hour_transaction_lock(
                        session,
                        preview_spec,
                    )

                    row = session.get(
                        SourceHour,
                        source_id,
                        populate_existing=True,
                    )

                    if row is None:
                        raise MaintenanceActionError(
                            "Source row disappeared after preview"
                        )

                    # Force current database values into the identity-mapped
                    # instance after lock acquisition.
                    session.refresh(row)

                    current_spec = SourceFileSpec(
                        provider=row.provider,
                        venue=row.venue,
                        symbol=row.instrument,
                        data_kind=SourceDataKind(row.data_kind),
                        hour_utc=row.hour_utc,
                    )

                    if current_spec.identity_tuple != preview_spec.identity_tuple:
                        raise MaintenanceActionError(
                            "Source identity changed after preview"
                        )

                    expected_status = str(item["current_status"])

                    if row.status != expected_status:
                        raise MaintenanceActionError(
                            "Source status changed after preview"
                        )

                    current_status = SourceHourStatus(row.status)

                    # The preview authorizes recovery of this exact stale
                    # source row. Mutable filesystem facts are recomputed under
                    # the source advisory lock immediately before mutation.
                    if current_status is SourceHourStatus.PROCESSING:
                        target = (
                            SourceHourStatus.DOWNLOADED
                            if _source_row_has_intact_raw_file(
                                row,
                                raw_root=settings.storage.raw_path,
                            )
                            else SourceHourStatus.ERROR
                        )
                    elif current_status is SourceHourStatus.DOWNLOADING:
                        target = SourceHourStatus.ERROR
                    else:
                        raise MaintenanceActionError(
                            "Source is no longer in a recoverable transient status"
                        )

                    validate_source_hour_transition(
                        row.status,
                        target,
                        allow_processing_cancellation_reset=(
                            current_status is SourceHourStatus.PROCESSING
                            and target is SourceHourStatus.DOWNLOADED
                        ),
                    )

                    row.status = target.value
                    row.error_text = (
                        "Recovered by explicitly authorized stale-source "
                        "maintenance."
                    )

                    if target is SourceHourStatus.DOWNLOADED:
                        row.processed_at = None

                    session.flush()

                    success_item = {
                        **dict(item),
                        "final_status": target.value,
                    }

            except Exception as exc:
                failed.append(
                    {
                        **dict(item),
                        "error": _safe_exception_name(exc),
                    }
                )
            else:
                if success_item is None:
                    raise RuntimeError(
                        "Stale-source recovery completed without an audit item"
                    )

                succeeded.append(success_item)

    return succeeded, failed, 0


def _remove_empty_checkpoint_parents(
    path: Path,
    *,
    checkpoint_root: Path,
) -> None:
    current = path.parent.resolve()
    root = checkpoint_root.resolve()

    while current != root:
        try:
            current.relative_to(root)
        except ValueError:
            return

        try:
            current.rmdir()
        except OSError:
            return

        current = current.parent.resolve()


def _execute_orphan_checkpoint_deletion(
    preview: MaintenancePreview,
    *,
    settings: Settings,
) -> tuple[list[dict[str, object]], list[dict[str, object]], int]:
    succeeded: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    affected_bytes = 0

    store = CheckpointStore(settings.storage.cache_path)

    for item in preview.items:
        path = Path(str(item["path"])).expanduser().resolve()

        try:
            path.relative_to(store.root)

            if path.is_symlink() or not path.is_file():
                raise MaintenanceActionError(
                    "Checkpoint candidate is not a regular file"
                )

            checkpoint = load_checkpoint_file(path)
            info = checkpoint_encoding_info(encode_checkpoint(checkpoint))
            identity = CheckpointIdentity.from_checkpoint(checkpoint)
            canonical = store.path_for(
                identity,
                info.content_sha256,
            ).resolve()

            if path != canonical:
                raise MaintenanceActionError(
                    "Checkpoint candidate path is not canonical"
                )

            if info.content_sha256 != str(item["content_sha256"]):
                raise MaintenanceActionError("Checkpoint content changed after preview")

            checkpoint_source = SourceFileSpec(
                provider=identity.provider,
                venue=identity.venue,
                symbol=identity.instrument,
                data_kind=SourceDataKind.ORDERBOOK,
                hour_utc=identity.through_hour_utc,
            )

            with session_scope() as session:
                acquire_source_hour_transaction_lock(
                    session,
                    checkpoint_source,
                )
                acquire_checkpoint_reference_transaction_lock(
                    session,
                    info.content_sha256,
                )

                current_references = _checkpoint_reference_set(session)

                if info.content_sha256 in current_references:
                    raise MaintenanceActionError(
                        "Checkpoint became durably referenced after preview"
                    )

                if path.is_symlink() or not path.is_file():
                    raise MaintenanceActionError(
                        "Checkpoint candidate disappeared during revalidation"
                    )

                size = path.stat().st_size

                if size != int(item["size_bytes"]):
                    raise MaintenanceActionError(
                        "Checkpoint size changed after preview"
                    )

                path.unlink()

            affected_bytes += size
            _remove_empty_checkpoint_parents(
                path,
                checkpoint_root=store.root,
            )

            succeeded.append(dict(item))

        except Exception as exc:
            failed.append(
                {
                    **dict(item),
                    "error": _safe_exception_name(exc),
                }
            )

    return succeeded, failed, affected_bytes


def _source_row_for_spec(
    session: Session,
    spec: SourceFileSpec,
) -> SourceHour | None:
    return session.scalar(
        select(SourceHour).where(
            SourceHour.provider == spec.provider,
            SourceHour.venue == spec.venue,
            SourceHour.data_kind == spec.data_kind.value,
            SourceHour.instrument == spec.symbol,
            SourceHour.hour_utc == spec.hour_utc,
        )
    )


def _execute_raw_pruning(
    preview: MaintenancePreview,
    *,
    settings: Settings,
) -> tuple[list[dict[str, object]], list[dict[str, object]], int]:
    succeeded: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    affected_bytes = 0

    raw_root = settings.storage.raw_path.resolve()
    renamed: list[
        tuple[
            Path,
            Path,
            dict[str, object],
            SourceFileSpec,
        ]
    ] = []

    try:
        with session_scope() as session:
            for item in preview.items:
                original = Path(str(item["local_path"])).expanduser().resolve()

                try:
                    original.relative_to(raw_root)

                    spec = _source_spec_from_item(item)
                    canonical = spec.local_path(raw_root).resolve()

                    if original != canonical:
                        raise MaintenanceActionError(
                            "Raw candidate path is not canonical"
                        )

                    acquire_source_hour_transaction_lock(
                        session,
                        spec,
                    )
                    row = _source_row_for_spec(session, spec)

                    if row is None:
                        raise MaintenanceActionError(
                            "Raw source row disappeared after preview"
                        )

                    if row.status != SourceHourStatus.PROCESSED.value:
                        raise MaintenanceActionError(
                            "Raw source is no longer processed"
                        )

                    if str(row.local_path or "").strip() != str(original):
                        raise MaintenanceActionError(
                            "Raw source path changed after preview"
                        )

                    if row.file_size_bytes != int(item["file_size_bytes"]):
                        raise MaintenanceActionError(
                            "Raw source size metadata changed after preview"
                        )

                    if row.content_sha256 != str(item["content_sha256"]):
                        raise MaintenanceActionError(
                            "Raw source digest changed after preview"
                        )

                    if original.is_symlink() or not original.is_file():
                        raise MaintenanceActionError(
                            "Raw candidate is not a regular file"
                        )

                    if original.stat().st_size != row.file_size_bytes:
                        raise MaintenanceActionError(
                            "Raw file size changed after preview"
                        )

                    actual_digest, actual_size = sha256_file(original)

                    if actual_size != row.file_size_bytes:
                        raise MaintenanceActionError(
                            "Raw file changed while it was being verified"
                        )

                    if actual_digest != row.content_sha256:
                        raise MaintenanceActionError(
                            "Raw file SHA-256 changed after preview"
                        )

                    temporary = original.with_name(
                        f".{original.name}.{uuid4().hex}.pruning"
                    )
                    original.replace(temporary)

                    try:
                        row.local_path = None
                        session.flush()
                    except BaseException:
                        try:
                            if temporary.exists() and not original.exists():
                                temporary.replace(original)
                        except OSError:
                            pass
                        raise

                    renamed.append(
                        (
                            original,
                            temporary,
                            dict(item),
                            spec,
                        )
                    )

                except Exception as exc:
                    failed.append(
                        {
                            **dict(item),
                            "error": _safe_exception_name(exc),
                        }
                    )

    except Exception:
        for original, temporary, _item, _spec in reversed(renamed):
            try:
                if temporary.exists() and not original.exists():
                    temporary.replace(original)
            except OSError:
                pass

        raise

    for original, temporary, item, _spec in renamed:
        try:
            size = temporary.stat().st_size
            temporary.unlink()
            affected_bytes += size
            succeeded.append(
                {
                    **item,
                    "local_path_cleared": True,
                }
            )

        except Exception as exc:
            failed.append(
                {
                    **item,
                    "temporary_path": str(temporary),
                    "local_path_cleared": True,
                    "error": _safe_exception_name(exc),
                }
            )

    return succeeded, failed, affected_bytes


def execute_maintenance_action(
    preview: MaintenancePreview,
    *,
    executed_at_utc: datetime | None = None,
    settings: Settings | None = None,
) -> MaintenanceActionReport:
    """Execute one fresh, explicitly confirmed maintenance preview."""

    if not isinstance(preview, MaintenancePreview):
        raise TypeError("preview must be MaintenancePreview")

    selected_settings = settings or get_settings()
    started = require_aware_utc(
        "executed_at_utc",
        executed_at_utc or now_utc(),
    )

    _verify_preview(
        preview,
        executed_at_utc=started,
        settings=selected_settings,
    )

    if preview.action is MaintenanceActionKind.RECOVER_STALE_SOURCES:
        succeeded, failed, affected_bytes = _execute_stale_source_recovery(
            preview,
            settings=selected_settings,
        )

    elif preview.action is MaintenanceActionKind.DELETE_ORPHAN_CHECKPOINTS:
        succeeded, failed, affected_bytes = _execute_orphan_checkpoint_deletion(
            preview,
            settings=selected_settings,
        )

    else:
        succeeded, failed, affected_bytes = _execute_raw_pruning(
            preview,
            settings=selected_settings,
        )

    completed = now_utc()
    # Prevent completion from preceding start due to test time injection or clock skew
    if completed < started:
        completed = started

    return MaintenanceActionReport(
        action=preview.action,
        preview_token=preview.token,
        started_at_utc=started,
        completed_at_utc=completed,
        attempted_count=preview.candidate_count,
        succeeded_count=len(succeeded),
        failed_count=len(failed),
        affected_bytes=affected_bytes,
        succeeded=_bounded_items(succeeded),
        failed=_bounded_items(failed),
    )


__all__ = [
    "DEFAULT_ORPHAN_CHECKPOINT_MINIMUM_AGE",
    "DEFAULT_PREVIEW_VALIDITY",
    "DEFAULT_STALE_DOWNLOADING_AFTER",
    "DEFAULT_STALE_PROCESSING_AFTER",
    "MAINTENANCE_ACTION_SCHEMA",
    "MAINTENANCE_ACTION_SCHEMA_VERSION",
    "MaintenanceActionError",
    "MaintenanceActionKind",
    "MaintenanceActionReport",
    "MaintenancePreview",
    "MaintenancePreviewChangedError",
    "MaintenancePreviewExpiredError",
    "build_maintenance_preview",
    "execute_maintenance_action",
]

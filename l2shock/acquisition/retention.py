# l2shock/acquisition/retention.py
"""Read-only planning for processed raw-archive retention.

This module does not delete files or mutate source-hour rows.

It identifies processed raw archives which are old enough to be considered for
future retention enforcement and verifies that each candidate:

- belongs to a supported immutable source identity;
- is stored at its canonical path under the configured raw root;
- is a regular file;
- matches its durable file-size metadata;
- has canonical content-hash metadata.

Actual deletion remains deferred until the project defines durable raw-pruned
state and reacquisition/reprocessing semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any
from collections.abc import Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.persistence import SourceHourStatus
from l2shock.acquisition.validation import sha256_file
from l2shock.db.models import SourceHour
from l2shock.timeutils import (
    floor_to_hour,
    require_aware_utc,
    require_utc_hour,
)


class RawRetentionError(ValueError):
    """Raw-retention planning input or durable metadata is invalid."""


def _positive_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RawRetentionError(f"{field_name} must be a positive integer")

    return value


def _canonical_sha256(
    field_name: str,
    value: object,
) -> str:
    text_value = str(value or "").strip()

    if (
        text_value != text_value.lower()
        or len(text_value) != 64
        or any(character not in "0123456789abcdef" for character in text_value)
    ):
        raise RawRetentionError(f"{field_name} must be a canonical lowercase SHA-256")

    return text_value


@dataclass(frozen=True, slots=True)
class RawRetentionCandidate:
    """One canonically verified processed raw archive."""

    spec: SourceFileSpec
    local_path: Path
    file_size_bytes: int
    content_sha256: str
    processed_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.spec, SourceFileSpec):
            raise RawRetentionError("RawRetentionCandidate.spec must be SourceFileSpec")

        path = Path(self.local_path).expanduser().resolve()

        if not path.is_file():
            raise RawRetentionError(
                f"Retention candidate is not a regular file: {path}"
            )

        if (
            isinstance(self.file_size_bytes, bool)
            or not isinstance(self.file_size_bytes, int)
            or self.file_size_bytes <= 0
        ):
            raise RawRetentionError("file_size_bytes must be a positive integer")

        try:
            actual_size = path.stat().st_size
        except OSError as exc:
            raise RawRetentionError(
                f"Could not inspect retention candidate: {path}"
            ) from exc

        if actual_size != self.file_size_bytes:
            raise RawRetentionError(
                "Retention candidate size differs from durable metadata: "
                f"path={path}, actual={actual_size}, "
                f"expected={self.file_size_bytes}"
            )

        expected_digest = _canonical_sha256(
            "content_sha256",
            self.content_sha256,
        )

        try:
            actual_digest, hashed_size = sha256_file(path)
        except Exception as exc:
            raise RawRetentionError(
                f"Could not verify retention candidate content: {path}"
            ) from exc

        if hashed_size != self.file_size_bytes:
            raise RawRetentionError(
                "Retention candidate changed while it was being verified: "
                f"path={path}, hashed={hashed_size}, "
                f"expected={self.file_size_bytes}"
            )

        if actual_digest != expected_digest:
            raise RawRetentionError(
                "Retention candidate SHA-256 differs from durable metadata: "
                f"path={path}"
            )

        object.__setattr__(self, "local_path", path)
        object.__setattr__(
            self,
            "content_sha256",
            expected_digest,
        )
        object.__setattr__(
            self,
            "processed_at",
            require_aware_utc(
                "processed_at",
                self.processed_at,
            ),
        )


@dataclass(frozen=True, slots=True)
class RawRetentionBlockedItem:
    """One old processed row which could not be accepted safely."""

    source_hour_id: int
    reason: str
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if (
            isinstance(self.source_hour_id, bool)
            or not isinstance(self.source_hour_id, int)
            or self.source_hour_id <= 0
        ):
            raise RawRetentionError("source_hour_id must be a positive integer")

        reason = str(self.reason or "").strip()

        if not reason:
            raise RawRetentionError("Blocked retention item requires a reason")

        metadata = dict(self.metadata)

        if any(not isinstance(key, str) for key in metadata):
            raise RawRetentionError("Blocked retention metadata keys must be strings")

        object.__setattr__(self, "reason", reason)
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(metadata),
        )


@dataclass(frozen=True, slots=True)
class RawRetentionPlan:
    """Read-only raw-retention report for one cutoff."""

    generated_at_utc: datetime
    retention_hours: int
    cutoff_hour_utc: datetime

    candidates: tuple[RawRetentionCandidate, ...]
    blocked: tuple[RawRetentionBlockedItem, ...]

    def __post_init__(self) -> None:
        generated = require_aware_utc(
            "generated_at_utc",
            self.generated_at_utc,
        )
        cutoff = require_utc_hour(
            "cutoff_hour_utc",
            self.cutoff_hour_utc,
        )
        retention_hours = _positive_integer(
            "retention_hours",
            self.retention_hours,
        )

        candidates = tuple(self.candidates)
        blocked = tuple(self.blocked)

        object.__setattr__(self, "generated_at_utc", generated)
        object.__setattr__(self, "cutoff_hour_utc", cutoff)
        object.__setattr__(self, "retention_hours", retention_hours)
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "blocked", blocked)

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def blocked_count(self) -> int:
        return len(self.blocked)

    @property
    def reclaimable_bytes(self) -> int:
        return sum(candidate.file_size_bytes for candidate in self.candidates)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "l2shock.raw_retention_plan",
            "schema_version": 1,
            "generated_at_utc": self.generated_at_utc.isoformat(),
            "retention_hours": self.retention_hours,
            "cutoff_hour_utc": self.cutoff_hour_utc.isoformat(),
            "candidate_count": self.candidate_count,
            "blocked_count": self.blocked_count,
            "reclaimable_bytes": self.reclaimable_bytes,
            "candidates": [
                {
                    "source": {
                        "provider": item.spec.provider,
                        "venue": item.spec.venue,
                        "symbol": item.spec.symbol,
                        "data_kind": item.spec.data_kind.value,
                        "hour_utc": item.spec.hour_utc.isoformat(),
                        "remote_path": item.spec.remote_path,
                    },
                    "local_path": str(item.local_path),
                    "file_size_bytes": item.file_size_bytes,
                    "content_sha256": item.content_sha256,
                    "processed_at": item.processed_at.isoformat(),
                }
                for item in self.candidates
            ],
            "blocked": [
                {
                    "source_hour_id": item.source_hour_id,
                    "reason": item.reason,
                    "metadata": dict(item.metadata),
                }
                for item in self.blocked
            ],
        }


def raw_retention_cutoff_hour(
    now: datetime,
    *,
    retention_hours: int,
) -> datetime:
    current = require_aware_utc("now", now)
    hours = _positive_integer(
        "retention_hours",
        retention_hours,
    )

    return require_utc_hour(
        "retention cutoff",
        floor_to_hour(current - timedelta(hours=hours)),
    )


def plan_processed_raw_retention(
    session: Session,
    *,
    raw_root: Path,
    retention_hours: int,
    now: datetime,
    hour_utc_min: datetime | None = None,
    hour_utc_end: datetime | None = None,
) -> RawRetentionPlan:
    """Build a fail-closed dry-run plan without deleting or mutating anything."""
    if not isinstance(session, Session):
        raise TypeError("plan_processed_raw_retention requires a SQLAlchemy Session")
    root = Path(raw_root).expanduser().resolve()
    cutoff = raw_retention_cutoff_hour(
        now,
        retention_hours=retention_hours,
    )
    generated = require_aware_utc("now", now)
    query = select(SourceHour).where(
        SourceHour.status == SourceHourStatus.PROCESSED.value,
        SourceHour.hour_utc < cutoff,
        SourceHour.local_path.is_not(None),
    )
    if hour_utc_min is not None:
        query = query.where(SourceHour.hour_utc >= hour_utc_min)
    if hour_utc_end is not None:
        query = query.where(SourceHour.hour_utc < hour_utc_end)
    models: Sequence[SourceHour] = (
        session.scalars(
            query.order_by(
                SourceHour.hour_utc,
                SourceHour.instrument,
                SourceHour.data_kind,
            )
        )
        .unique()
        .all()
    )

    candidates: list[RawRetentionCandidate] = []
    blocked: list[RawRetentionBlockedItem] = []

    for model in models:
        metadata = {
            "provider": str(model.provider or ""),
            "venue": str(model.venue or ""),
            "instrument": str(model.instrument or ""),
            "data_kind": str(model.data_kind or ""),
            "hour_utc": (
                model.hour_utc.isoformat()
                if isinstance(model.hour_utc, datetime)
                else str(model.hour_utc)
            ),
            "local_path": str(model.local_path or ""),
            "file_size_bytes": model.file_size_bytes,
            "content_sha256": str(model.content_sha256 or ""),
            "processed_at": (
                model.processed_at.isoformat()
                if isinstance(model.processed_at, datetime)
                else str(model.processed_at)
            ),
        }

        try:
            spec = SourceFileSpec(
                provider=model.provider,
                venue=model.venue,
                symbol=model.instrument,
                data_kind=SourceDataKind(model.data_kind),
                hour_utc=model.hour_utc,
            )

            stored_path = Path(str(model.local_path or "")).expanduser().resolve()
            canonical_path = spec.local_path(root)

            if stored_path != canonical_path:
                raise RawRetentionError(
                    "Stored local path does not match canonical source path: "
                    f"stored={stored_path}, canonical={canonical_path}"
                )

            if model.file_size_bytes is None:
                raise RawRetentionError("Processed source row has no durable file size")

            if model.content_sha256 is None:
                raise RawRetentionError(
                    "Processed source row has no durable content SHA-256"
                )

            if model.processed_at is None:
                raise RawRetentionError(
                    "Processed source row has no processed_at timestamp"
                )

            candidates.append(
                RawRetentionCandidate(
                    spec=spec,
                    local_path=stored_path,
                    file_size_bytes=int(model.file_size_bytes),
                    content_sha256=str(model.content_sha256),
                    processed_at=model.processed_at,
                )
            )

        except Exception as exc:
            blocked.append(
                RawRetentionBlockedItem(
                    source_hour_id=int(model.id),
                    reason=(str(exc).strip() or type(exc).__name__),
                    metadata=metadata,
                )
            )

    return RawRetentionPlan(
        generated_at_utc=generated,
        retention_hours=retention_hours,
        cutoff_hour_utc=cutoff,
        candidates=tuple(candidates),
        blocked=tuple(blocked),
    )


__all__ = [
    "RawRetentionBlockedItem",
    "RawRetentionCandidate",
    "RawRetentionError",
    "RawRetentionPlan",
    "plan_processed_raw_retention",
    "raw_retention_cutoff_hour",
]

# l2shock/acquisition/fetch_models.py
"""Typed progress and result models for acquisition coordination.

These objects describe operational acquisition outcomes only. They do not
claim that a downloaded source hour can initialize or reconstruct a valid
order book.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from collections.abc import Mapping
from uuid import UUID

from l2shock.acquisition.models import SourceFileSpec
from l2shock.timeutils import require_aware_utc


class FetchProgressPhase(StrEnum):
    """Phases emitted by a fetch coordinator."""

    PLANNING = "planning"
    STARTED = "started"
    DOWNLOADING = "downloading"
    FILE_COMPLETE = "file_complete"
    FILE_MISSING = "file_missing"
    FILE_ERROR = "file_error"
    STOPPING = "stopping"
    COMPLETED = "completed"


class FetchItemDisposition(StrEnum):
    """Terminal outcome for one attempted source file."""

    DOWNLOADED = "downloaded"
    REUSED = "reused"
    MISSING = "missing"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class FetchProgress:
    """One immutable progress event emitted during a fetch run."""

    operation_id: UUID
    phase: FetchProgressPhase
    message: str
    files_requested: int
    files_completed: int
    files_downloaded: int
    files_reused: int
    files_missing: int
    files_failed: int
    current_spec: SourceFileSpec | None = None

    def __post_init__(self) -> None:
        try:
            operation_id = UUID(str(self.operation_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("FetchProgress.operation_id must be a UUID") from exc

        object.__setattr__(self, "operation_id", operation_id)

        try:
            phase = FetchProgressPhase(self.phase)
        except (TypeError, ValueError) as exc:
            raise ValueError("FetchProgress.phase is invalid") from exc

        object.__setattr__(self, "phase", phase)

        message = str(self.message or "").strip()
        if not message:
            raise ValueError("FetchProgress.message must be non-empty")

        object.__setattr__(self, "message", message)

        counter_names = (
            "files_requested",
            "files_completed",
            "files_downloaded",
            "files_reused",
            "files_missing",
            "files_failed",
        )

        for name in counter_names:
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer")

            if value < 0:
                raise ValueError(f"{name} must be non-negative")

        if self.files_completed > self.files_requested:
            raise ValueError("files_completed cannot exceed files_requested")

        terminal_count = (
            self.files_downloaded
            + self.files_reused
            + self.files_missing
            + self.files_failed
        )

        if terminal_count != self.files_completed:
            raise ValueError(
                "files_completed must equal downloaded + reused + " "missing + failed"
            )

    @property
    def fraction_complete(self) -> float:
        if self.files_requested == 0:
            return 0.0

        return self.files_completed / self.files_requested

    @property
    def percent_complete(self) -> float:
        return self.fraction_complete * 100.0


@dataclass(frozen=True, slots=True)
class FetchItemResult:
    """Terminal acquisition outcome for one attempted source file."""

    spec: SourceFileSpec
    disposition: FetchItemDisposition
    local_path: Path | None = None
    file_size_bytes: int | None = None
    content_sha256: str | None = None
    attempts: int = 0
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        try:
            disposition = FetchItemDisposition(self.disposition)
        except (TypeError, ValueError) as exc:
            raise ValueError("FetchItemResult.disposition is invalid") from exc

        object.__setattr__(self, "disposition", disposition)

        if isinstance(self.attempts, bool) or not isinstance(
            self.attempts,
            int,
        ):
            raise ValueError("attempts must be an integer")

        if self.attempts < 0:
            raise ValueError("attempts must be non-negative")

        successful = disposition in {
            FetchItemDisposition.DOWNLOADED,
            FetchItemDisposition.REUSED,
        }

        if successful:
            if self.local_path is None:
                raise ValueError("Successful fetch item requires local_path")

            path = Path(self.local_path).expanduser().resolve()
            object.__setattr__(self, "local_path", path)

            if (
                isinstance(self.file_size_bytes, bool)
                or not isinstance(self.file_size_bytes, int)
                or self.file_size_bytes <= 0
            ):
                raise ValueError(
                    "Successful fetch item requires positive " "file_size_bytes"
                )

            digest = str(self.content_sha256 or "").strip().lower()

            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError("Successful fetch item requires a valid SHA-256")

            object.__setattr__(self, "content_sha256", digest)

        else:
            if self.local_path is not None:
                object.__setattr__(
                    self,
                    "local_path",
                    Path(self.local_path).expanduser().resolve(),
                )

        diagnostic = str(self.diagnostic or "").strip()
        object.__setattr__(
            self,
            "diagnostic",
            diagnostic or None,
        )


@dataclass(frozen=True, slots=True)
class ManualFetchResult:
    """Complete immutable result of one manual fetch operation."""

    operation_id: UUID
    started_at: datetime
    ended_at: datetime
    requested_start_utc: datetime
    requested_end_utc: datetime
    status: str
    items: tuple[FetchItemResult, ...]
    files_requested: int
    files_downloaded: int
    files_reused: int
    files_missing: int
    files_failed: int
    stopped: bool
    details: Mapping[str, Any]

    def __post_init__(self) -> None:
        try:
            operation_id = UUID(str(self.operation_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("ManualFetchResult.operation_id must be a UUID") from exc

        object.__setattr__(self, "operation_id", operation_id)

        started_at = require_aware_utc(
            "started_at",
            self.started_at,
        )
        ended_at = require_aware_utc(
            "ended_at",
            self.ended_at,
        )
        requested_start = require_aware_utc(
            "requested_start_utc",
            self.requested_start_utc,
        )
        requested_end = require_aware_utc(
            "requested_end_utc",
            self.requested_end_utc,
        )

        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "ended_at", ended_at)
        object.__setattr__(
            self,
            "requested_start_utc",
            requested_start,
        )
        object.__setattr__(
            self,
            "requested_end_utc",
            requested_end,
        )

        if ended_at < started_at:
            raise ValueError("ended_at cannot precede started_at")

        if requested_end <= requested_start:
            raise ValueError("requested_end_utc must be after requested_start_utc")

        status = str(self.status or "").strip()

        if status not in {
            "ok",
            "partial_ok",
            "error",
            "stopped",
        }:
            raise ValueError(
                "ManualFetchResult.status must be ok, partial_ok, " "error, or stopped"
            )

        object.__setattr__(self, "status", status)

        counters = (
            self.files_requested,
            self.files_downloaded,
            self.files_reused,
            self.files_missing,
            self.files_failed,
        )

        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counters
        ):
            raise ValueError("ManualFetchResult counters must be non-negative integers")

        if len(self.items) > self.files_requested:
            raise ValueError("Attempted item count cannot exceed files_requested")

        if (
            self.files_downloaded
            + self.files_reused
            + self.files_missing
            + self.files_failed
            != len(self.items)
        ):
            raise ValueError("ManualFetchResult counters must equal item count")

        object.__setattr__(self, "details", dict(self.details))

    @property
    def files_available(self) -> int:
        """Return downloaded plus valid locally reused source files."""
        return self.files_downloaded + self.files_reused

    @property
    def files_completed(self) -> int:
        return len(self.items)

    @property
    def duration_seconds(self) -> float:
        value = (self.ended_at - self.started_at).total_seconds()

        if not math.isfinite(value):
            raise RuntimeError("Fetch duration is non-finite")

        return max(0.0, value)


__all__ = [
    "FetchItemDisposition",
    "FetchItemResult",
    "FetchProgress",
    "FetchProgressPhase",
    "ManualFetchResult",
]

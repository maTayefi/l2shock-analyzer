# l2shock/acquisition/results.py
"""Immutable acquisition result and validation objects."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from l2shock.acquisition.models import SourceFileSpec


class DownloadDisposition(StrEnum):
    """How a successful local source file became available."""

    DOWNLOADED = "downloaded"
    REUSED = "reused"


@dataclass(frozen=True, slots=True)
class ParquetValidationReport:
    """Structural facts proven from a local Parquet archive."""

    data_kind: str
    row_count: int
    row_group_count: int
    column_names: tuple[str, ...]
    created_by: str | None
    format_version: str | None

    def __post_init__(self) -> None:
        if self.data_kind not in {"orderbook", "trades"}:
            raise ValueError(
                "ParquetValidationReport.data_kind must be " "'orderbook' or 'trades'"
            )

        if self.row_count <= 0:
            raise ValueError("ParquetValidationReport.row_count must be positive")

        if self.row_group_count <= 0:
            raise ValueError("ParquetValidationReport.row_group_count must be positive")

        if not self.column_names:
            raise ValueError("ParquetValidationReport.column_names cannot be empty")


@dataclass(frozen=True, slots=True)
class DownloadArtifact:
    """One valid local hourly archive produced or reused by acquisition."""

    spec: SourceFileSpec
    local_path: Path
    disposition: DownloadDisposition
    file_size_bytes: int
    content_sha256: str
    validation: ParquetValidationReport
    attempts: int

    def __post_init__(self) -> None:
        local_path = Path(self.local_path).resolve()
        object.__setattr__(self, "local_path", local_path)

        try:
            disposition = DownloadDisposition(self.disposition)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid download disposition") from exc

        object.__setattr__(self, "disposition", disposition)

        if self.file_size_bytes <= 0:
            raise ValueError("file_size_bytes must be positive")

        digest = str(self.content_sha256 or "").strip().lower()
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise ValueError("content_sha256 must be a lowercase 64-character SHA-256")

        object.__setattr__(self, "content_sha256", digest)

        if isinstance(self.attempts, bool) or not isinstance(
            self.attempts,
            int,
        ):
            raise ValueError("attempts must be an integer")

        if self.attempts < 0:
            raise ValueError("attempts must be non-negative")

        if disposition is DownloadDisposition.REUSED and self.attempts != 0:
            raise ValueError("A reused artifact must report zero network attempts")

        if disposition is DownloadDisposition.DOWNLOADED and self.attempts <= 0:
            raise ValueError("A downloaded artifact must report at least one attempt")


__all__ = [
    "DownloadArtifact",
    "DownloadDisposition",
    "ParquetValidationReport",
]

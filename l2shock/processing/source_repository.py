# l2shock/processing/source_repository.py
"""Read-only source-archive lookup for processing discovery.

This repository intentionally performs no source-hour status mutation. Batch
8A-2 will own transaction locking and operational state transitions.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final, Protocol, runtime_checkable

from sqlalchemy import select
from sqlalchemy.orm import Session

from l2shock.acquisition.models import SourceFileSpec
from l2shock.acquisition.persistence import SourceHourStatus
from l2shock.db.models import SourceHour
from l2shock.processing.errors import SourceArchiveMetadataError

_REPLAYABLE_STATUSES: Final[frozenset[SourceHourStatus]] = frozenset(
    {
        SourceHourStatus.DOWNLOADED,
        SourceHourStatus.PROCESSING,
        SourceHourStatus.PROCESSED,
    }
)


def _validated_sha256(name: str, value: object) -> str:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise SourceArchiveMetadataError(
            f"{name} must be a canonical lowercase SHA-256"
        )

    return digest


@dataclass(frozen=True, slots=True)
class ProcessingSourceArchive:
    """Verified metadata for one locally available raw source archive."""

    spec: SourceFileSpec
    local_path: Path
    content_sha256: str
    file_size_bytes: int
    status: SourceHourStatus

    def __post_init__(self) -> None:
        if not isinstance(self.spec, SourceFileSpec):
            raise SourceArchiveMetadataError("spec must be a SourceFileSpec")

        raw_path = Path(self.local_path).expanduser()

        # Check the supplied directory entry before resolution. Resolving first
        # would turn a symbolic link into its target and lose the evidence
        # needed to reject externally mutable source ownership.
        if raw_path.is_symlink():
            raise SourceArchiveMetadataError(
                "Processing source archive cannot be a symbolic link"
            )

        path = raw_path.absolute()
        object.__setattr__(self, "local_path", path)

        object.__setattr__(
            self,
            "content_sha256",
            _validated_sha256(
                "content_sha256",
                self.content_sha256,
            ),
        )

        if (
            isinstance(self.file_size_bytes, bool)
            or not isinstance(self.file_size_bytes, int)
            or self.file_size_bytes < 0
        ):
            raise SourceArchiveMetadataError(
                "file_size_bytes must be a non-negative integer"
            )

        try:
            status = SourceHourStatus(self.status)
        except (TypeError, ValueError) as exc:
            raise SourceArchiveMetadataError(
                "Source archive has an unsupported status"
            ) from exc

        if status not in _REPLAYABLE_STATUSES:
            raise SourceArchiveMetadataError(
                "ProcessingSourceArchive status must be downloaded, "
                "processing, or processed"
            )

        object.__setattr__(self, "status", status)


@runtime_checkable
class ProcessingSourceRepository(Protocol):
    """Read-only source lookup boundary used by discovery planning."""

    def find_durable_content_sha256(
        self,
        spec: SourceFileSpec,
    ) -> str | None:
        """Return durable source content identity without requiring raw bytes."""
        ...

    def find_replayable(
        self,
        spec: SourceFileSpec,
    ) -> ProcessingSourceArchive | None:
        """Return one usable local source archive, or none if unavailable."""
        ...


class SQLAlchemyProcessingSourceRepository:
    """Read-only PostgreSQL-backed processing source lookup.

    ``raw_root`` should always be supplied by production local-processing
    coordinators. ``None`` is retained only for isolated tests and generic
    repository consumers that do not claim configured local-storage
    ownership.
    """

    def __init__(
        self,
        session: Session,
        *,
        raw_root: Path | None = None,
    ) -> None:
        if not isinstance(session, Session):
            raise TypeError("session must be a SQLAlchemy Session")

        self._session = session
        self._raw_root = (
            Path(raw_root).expanduser().resolve() if raw_root is not None else None
        )

    @property
    def session(self) -> Session:
        return self._session

    def find_durable_content_sha256(
        self,
        spec: SourceFileSpec,
    ) -> str | None:
        """Return the durable digest even when processed raw bytes were pruned."""
        if not isinstance(spec, SourceFileSpec):
            raise TypeError("spec must be a SourceFileSpec")

        row = self._session.scalar(
            select(SourceHour).where(
                SourceHour.provider == spec.provider,
                SourceHour.venue == spec.venue,
                SourceHour.data_kind == spec.data_kind.value,
                SourceHour.instrument == spec.symbol,
                SourceHour.hour_utc == spec.hour_utc,
            )
        )

        if row is None:
            return None

        if str(row.remote_path or "").strip() != spec.remote_path:
            raise SourceArchiveMetadataError(
                "Source-hour remote path does not match its canonical identity"
            )

        if row.content_sha256 is None:
            return None

        return _validated_sha256(
            "content_sha256",
            row.content_sha256,
        )

    def find_replayable(
        self,
        spec: SourceFileSpec,
    ) -> ProcessingSourceArchive | None:
        if not isinstance(spec, SourceFileSpec):
            raise TypeError("spec must be a SourceFileSpec")

        row = self._session.scalar(
            select(SourceHour).where(
                SourceHour.provider == spec.provider,
                SourceHour.venue == spec.venue,
                SourceHour.data_kind == spec.data_kind.value,
                SourceHour.instrument == spec.symbol,
                SourceHour.hour_utc == spec.hour_utc,
            )
        )

        if row is None:
            return None

        try:
            status = SourceHourStatus(row.status)
        except (TypeError, ValueError) as exc:
            raise SourceArchiveMetadataError(
                "Source-hour row has an unsupported status"
            ) from exc

        if status not in _REPLAYABLE_STATUSES:
            return None

        if str(row.remote_path or "").strip() != spec.remote_path:
            raise SourceArchiveMetadataError(
                "Source-hour remote path does not match its canonical identity"
            )

        if not str(row.local_path or "").strip():
            # Explicit raw pruning preserves the durable PROCESSED row while
            # clearing only local_path. Such a row is valid historical
            # metadata, but it is no longer locally replayable.
            if status is SourceHourStatus.PROCESSED:
                return None

            raise SourceArchiveMetadataError(
                "Replayable source-hour row has no local path"
            )

        if row.file_size_bytes is None:
            raise SourceArchiveMetadataError(
                "Replayable source-hour row has no file size"
            )

        if row.content_sha256 is None:
            raise SourceArchiveMetadataError(
                "Replayable source-hour row has no content SHA-256"
            )

        stored_path = Path(row.local_path).expanduser()

        # Inspect the stored directory entry before any operation that follows
        # symbolic links.
        if stored_path.is_symlink():
            raise SourceArchiveMetadataError(
                "Replayable source-hour local path cannot be a symbolic link"
            )

        local_path = stored_path.absolute()
        raw_root = self._raw_root

        if raw_root is not None:
            canonical_path = spec.local_path(raw_root)

            if local_path != canonical_path:
                raise SourceArchiveMetadataError(
                    "Replayable source-hour local path does not match its "
                    "canonical configured raw-storage identity"
                )

            # Reject a symbolic link in any component below the configured
            # root. Checking only the final filename would permit a canonical
            # textual path whose parent directory redirects outside raw
            # storage.
            try:
                relative_parts = local_path.relative_to(raw_root).parts
            except ValueError as exc:
                raise SourceArchiveMetadataError(
                    "Replayable source-hour local path lies outside the "
                    "configured raw root"
                ) from exc

            current = raw_root

            for part in relative_parts:
                current = current / part

                if current.is_symlink():
                    raise SourceArchiveMetadataError(
                        "Replayable source-hour local path contains a "
                        "symbolic-link component"
                    )

        if not local_path.is_file():
            raise SourceArchiveMetadataError(
                "Replayable source-hour local path is not a regular file: "
                f"{local_path}"
            )

        try:
            actual_size = local_path.stat().st_size
        except OSError as exc:
            raise SourceArchiveMetadataError(
                "Could not inspect replayable source-hour local file"
            ) from exc

        if actual_size != row.file_size_bytes:
            raise SourceArchiveMetadataError(
                "Replayable source-hour file size does not match durable "
                f"metadata: actual={actual_size}, "
                f"expected={row.file_size_bytes}"
            )

        return ProcessingSourceArchive(
            spec=spec,
            local_path=local_path,
            content_sha256=row.content_sha256,
            file_size_bytes=row.file_size_bytes,
            status=status,
        )


__all__ = [
    "ProcessingSourceArchive",
    "ProcessingSourceRepository",
    "SQLAlchemyProcessingSourceRepository",
]

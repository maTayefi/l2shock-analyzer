# l2shock/acquisition/fetch_persistence.py
"""Async-safe persistence boundary for acquisition coordination.

SQLAlchemy sessions are synchronous and thread-affine. This adapter creates and
closes every session inside the same worker thread that uses it. ORM instances
never cross the async/thread boundary.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from collections.abc import Iterable
from uuid import UUID

from l2shock.acquisition.models import SourceFileSpec
from l2shock.acquisition.persistence import (
    FetchRunKind,
    FetchRunStatus,
)
from l2shock.acquisition.repository import AcquisitionRepository
from l2shock.acquisition.results import DownloadArtifact
from l2shock.db.engine import session_scope


@dataclass(frozen=True, slots=True)
class PersistedFetchRun:
    """Primitive identity returned after creating a fetch run."""

    id: int
    operation_id: UUID


class FetchPersistenceProtocol(Protocol):
    """Persistence behavior required by ManualFetchCoordinator."""

    async def create_fetch_run(
        self,
        *,
        operation_id: UUID,
        kind: FetchRunKind,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        files_requested: int,
        details: dict[str, Any],
    ) -> PersistedFetchRun: ...

    async def mark_downloading(
        self,
        spec: SourceFileSpec,
    ) -> bool:
        """Return whether the source was already processed at admission."""
        ...

    async def record_artifact(
        self,
        artifact: DownloadArtifact,
    ) -> None: ...

    async def record_missing(
        self,
        spec: SourceFileSpec,
        *,
        message: object,
    ) -> None: ...

    async def record_error(
        self,
        spec: SourceFileSpec,
        *,
        message: object,
    ) -> None: ...

    async def complete_fetch_run(
        self,
        *,
        operation_id: UUID,
        status: FetchRunStatus,
        files_downloaded: int,
        files_processed: int,
        files_failed: int,
        details: dict[str, Any],
        error_text: object = None,
    ) -> None: ...


class SQLAlchemyFetchPersistence:
    """Production persistence adapter using short worker-thread transactions."""

    def __init__(
        self,
        *,
        diagnostic_secrets: Iterable[str] = (),
    ) -> None:
        self._diagnostic_secrets = tuple(
            str(secret) for secret in diagnostic_secrets if str(secret or "").strip()
        )

    def _repository(
        self,
        session,
    ) -> AcquisitionRepository:
        return AcquisitionRepository(
            session,
            diagnostic_secrets=self._diagnostic_secrets,
        )

    async def create_fetch_run(
        self,
        *,
        operation_id: UUID,
        kind: FetchRunKind,
        requested_start_utc: datetime,
        requested_end_utc: datetime,
        files_requested: int,
        details: dict[str, Any],
    ) -> PersistedFetchRun:
        def _write() -> PersistedFetchRun:
            with session_scope() as session:
                run = self._repository(session).create_fetch_run(
                    operation_id=operation_id,
                    kind=kind,
                    requested_start_utc=requested_start_utc,
                    requested_end_utc=requested_end_utc,
                    files_requested=files_requested,
                    details=details,
                )

                if run.id is None:
                    raise RuntimeError("Fetch run did not receive a database identity")

                return PersistedFetchRun(
                    id=int(run.id),
                    operation_id=UUID(run.operation_id),
                )

        return await asyncio.to_thread(_write)

    async def mark_downloading(
        self,
        spec: SourceFileSpec,
    ) -> bool:
        def _write() -> bool:
            with session_scope() as session:
                row = self._repository(session).mark_downloading(spec)
                return row.status == "processed"

        return await asyncio.to_thread(_write)

    async def record_artifact(
        self,
        artifact: DownloadArtifact,
    ) -> None:
        def _write() -> None:
            with session_scope() as session:
                self._repository(session).record_artifact(artifact)

        await asyncio.to_thread(_write)

    async def record_missing(
        self,
        spec: SourceFileSpec,
        *,
        message: object,
    ) -> None:
        def _write() -> None:
            with session_scope() as session:
                self._repository(session).record_missing(
                    spec,
                    message=message,
                )

        await asyncio.to_thread(_write)

    async def record_error(
        self,
        spec: SourceFileSpec,
        *,
        message: object,
    ) -> None:
        def _write() -> None:
            with session_scope() as session:
                self._repository(session).record_error(
                    spec,
                    message=message,
                )

        await asyncio.to_thread(_write)

    async def complete_fetch_run(
        self,
        *,
        operation_id: UUID,
        status: FetchRunStatus,
        files_downloaded: int,
        files_processed: int,
        files_failed: int,
        details: dict[str, Any],
        error_text: object = None,
    ) -> None:
        def _write() -> None:
            with session_scope() as session:
                repository = self._repository(session)
                run = repository.get_fetch_run(operation_id)

                if run is None:
                    raise RuntimeError(
                        "Fetch run no longer exists for operation " f"{operation_id}"
                    )

                repository.complete_fetch_run(
                    run,
                    status=status,
                    files_downloaded=files_downloaded,
                    files_processed=files_processed,
                    files_failed=files_failed,
                    details=details,
                    error_text=error_text,
                )

        await asyncio.to_thread(_write)


__all__ = [
    "FetchPersistenceProtocol",
    "PersistedFetchRun",
    "SQLAlchemyFetchPersistence",
]

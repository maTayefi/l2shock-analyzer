# l2shock/processing/errors.py
"""Processing-boundary exceptions.

Batch 8A-1 defines contracts and read-only discovery. It deliberately does not
own replay, analytical persistence, or source-hour state mutation.
"""

from __future__ import annotations


class ProcessingError(RuntimeError):
    """Base class for operational processing failures."""


class ProcessingContractError(ProcessingError, ValueError):
    """A processing request or result violated a deterministic contract."""


class ProcessingCancelledError(ProcessingError):
    """Cooperative processing cancellation was requested."""


class CheckpointStoreError(ProcessingError):
    """A checkpoint store operation could not complete safely."""


class CheckpointConflictError(CheckpointStoreError):
    """More than one checkpoint content identity exists for one source hour."""


class SourceArchiveError(ProcessingError):
    """A source archive record or local file is unusable."""


class SourceArchiveMetadataError(SourceArchiveError):
    """Durable source metadata is missing or internally inconsistent."""


class SourceArchiveIntegrityError(SourceArchiveError):
    """A local source file differs from its durable size or SHA-256."""


class ProcessingPersistenceError(ProcessingError):
    """A completed in-memory result could not be persisted safely."""


__all__ = [
    "CheckpointConflictError",
    "CheckpointStoreError",
    "ProcessingCancelledError",
    "ProcessingContractError",
    "ProcessingError",
    "ProcessingPersistenceError",
    "SourceArchiveError",
    "SourceArchiveIntegrityError",
    "SourceArchiveMetadataError",
]

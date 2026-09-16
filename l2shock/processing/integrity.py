# l2shock/processing/integrity.py
"""Exact local source-archive integrity verification for processing.

Acquisition validates and hashes an archive when it is downloaded or reused.
Processing verifies it again immediately before source interpretation.

This protects the analytical boundary against a local file being replaced,
truncated, or modified after acquisition metadata was persisted.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Final

from l2shock.processing.errors import (
    ProcessingCancelledError,
    SourceArchiveIntegrityError,
)
from l2shock.processing.models import (
    ProcessingCancellationProbe,
    raise_if_processing_cancelled,
)
from l2shock.processing.source_repository import ProcessingSourceArchive

_HASH_CHUNK_SIZE: Final[int] = 1024 * 1024


def verify_processing_source_archive(
    archive: ProcessingSourceArchive,
    *,
    cancellation_probe: ProcessingCancellationProbe | None = None,
) -> None:
    """Verify exact size and SHA-256 immediately before processing."""
    if not isinstance(archive, ProcessingSourceArchive):
        raise TypeError("archive must be a ProcessingSourceArchive")

    path = Path(archive.local_path).expanduser().resolve()

    if not path.is_file():
        raise SourceArchiveIntegrityError(
            f"Processing source is not a regular file: {path}"
        )

    try:
        expected_size = int(archive.file_size_bytes)
        actual_size = path.stat().st_size
    except OSError as exc:
        raise SourceArchiveIntegrityError(
            "Could not inspect processing source file"
        ) from exc

    if actual_size != expected_size:
        raise SourceArchiveIntegrityError(
            "Processing source size differs from durable metadata: "
            f"actual={actual_size}, expected={expected_size}"
        )

    digest = hashlib.sha256()
    bytes_read = 0

    try:
        with path.open("rb") as handle:
            while True:
                raise_if_processing_cancelled(cancellation_probe)

                chunk = handle.read(_HASH_CHUNK_SIZE)

                if not chunk:
                    break

                digest.update(chunk)
                bytes_read += len(chunk)

    except ProcessingCancelledError:
        raise
    except OSError as exc:
        raise SourceArchiveIntegrityError(
            "Could not hash processing source file"
        ) from exc

    if bytes_read != expected_size:
        raise SourceArchiveIntegrityError(
            "Processing source changed while it was being verified"
        )

    if digest.hexdigest() != archive.content_sha256:
        raise SourceArchiveIntegrityError(
            "Processing source SHA-256 differs from durable metadata"
        )


__all__ = ["verify_processing_source_archive"]

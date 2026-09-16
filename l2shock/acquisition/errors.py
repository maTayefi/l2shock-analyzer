# l2shock/acquisition/errors.py
"""Secret-safe acquisition exception hierarchy.

Exceptions in this module must not contain API keys, authorization headers, or
temporary credentials. Network exceptions are translated at the transport
boundary instead of being exposed directly to UI or persistent fetch history.
"""

from __future__ import annotations


class AcquisitionError(RuntimeError):
    """Base class for acquisition failures safe to expose diagnostically."""


class AcquisitionCancelledError(AcquisitionError):
    """The acquisition operation was cooperatively cancelled."""


class RemoteFileNotFoundError(AcquisitionError):
    """The requested hourly archive does not currently exist remotely."""


class RemoteRequestError(AcquisitionError):
    """A remote request failed without exposing its credential-bearing URL."""


class DownloadIntegrityError(AcquisitionError):
    """Downloaded bytes failed size, digest, or filesystem integrity checks."""


class DownloadConflictError(AcquisitionError):
    """Two different valid files claimed the same immutable source identity."""


class InsufficientDiskSpaceError(AcquisitionError):
    """The configured post-download free-space floor cannot be preserved."""


class ParquetValidationError(AcquisitionError):
    """A local file is not a structurally valid supported Parquet archive."""


class QuarantineError(AcquisitionError):
    """A corrupt or conflicting file could not be moved to quarantine."""


__all__ = [
    "AcquisitionCancelledError",
    "AcquisitionError",
    "DownloadConflictError",
    "DownloadIntegrityError",
    "InsufficientDiskSpaceError",
    "ParquetValidationError",
    "QuarantineError",
    "RemoteFileNotFoundError",
    "RemoteRequestError",
]

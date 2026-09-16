# l2shock/acquisition/__init__.py
"""CryptoHFTData acquisition identities and transport services."""

from l2shock.acquisition.availability import (
    AvailabilityError,
    ContiguousAnalysisWindow,
    HourAvailability,
    HourAvailabilityState,
    enabled_preset_hashes,
    load_hourly_availability,
    most_recent_contiguous_analyzable_window,
    preferred_enabled_preset_hash,
)
from l2shock.acquisition.coordinator import (
    DownloaderFactory,
    DownloaderProtocol,
    FetchOperationBusyError,
    ManualFetchCoordinator,
    ProgressSink,
    create_production_manual_fetch_coordinator,
)
from l2shock.acquisition.downloader import CryptoHFTDownloader
from l2shock.acquisition.errors import (
    AcquisitionCancelledError,
    AcquisitionError,
    DownloadConflictError,
    DownloadIntegrityError,
    InsufficientDiskSpaceError,
    ParquetValidationError,
    QuarantineError,
    RemoteFileNotFoundError,
    RemoteRequestError,
)
from l2shock.acquisition.fetch_models import (
    FetchItemDisposition,
    FetchItemResult,
    FetchProgress,
    FetchProgressPhase,
    ManualFetchResult,
)
from l2shock.acquisition.fetch_persistence import (
    FetchPersistenceProtocol,
    PersistedFetchRun,
    SQLAlchemyFetchPersistence,
)
from l2shock.acquisition.http import (
    download_endpoint,
    download_query_parameters,
    is_retryable_http_status,
    parse_retry_after_seconds,
)
from l2shock.acquisition.locks import (
    acquire_source_hour_transaction_lock,
    source_hour_lock_keys,
)
from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.acquisition.persistence import (
    FetchRunKind,
    FetchRunStatus,
    InvalidSourceHourTransitionError,
    SourceHourLockUnavailableError,
    SourceHourStatus,
    bounded_diagnostic_text,
    nonnegative_counter,
    normalize_source_hour_status,
    validate_source_hour_transition,
)
from l2shock.acquisition.planning import (
    intersecting_utc_hours,
    plan_binance_futures_files,
    plan_okx_futures_files,
    plan_production_source_files,
)
from l2shock.acquisition.rate_limit import (
    AsyncRollingWindowRateLimiter,
)
from l2shock.acquisition.release_schedule import (
    latest_release_eligible_hour,
)
from l2shock.acquisition.retention import (
    RawRetentionBlockedItem,
    RawRetentionCandidate,
    RawRetentionError,
    RawRetentionPlan,
    plan_processed_raw_retention,
    raw_retention_cutoff_hour,
)
from l2shock.acquisition.repository import AcquisitionRepository
from l2shock.acquisition.results import (
    DownloadArtifact,
    DownloadDisposition,
    ParquetValidationReport,
)
from l2shock.acquisition.validation import (
    quarantine_file,
    sha256_file,
    validate_parquet_file,
)

__all__ = [
    "AcquisitionCancelledError",
    "AcquisitionError",
    "AcquisitionRepository",
    "AsyncRollingWindowRateLimiter",
    "CryptoHFTDownloader",
    "DownloadArtifact",
    "DownloadConflictError",
    "DownloadDisposition",
    "DownloadIntegrityError",
    "DownloaderFactory",
    "DownloaderProtocol",
    "FetchItemDisposition",
    "FetchItemResult",
    "FetchOperationBusyError",
    "FetchPersistenceProtocol",
    "FetchProgress",
    "FetchProgressPhase",
    "FetchRunKind",
    "FetchRunStatus",
    "InsufficientDiskSpaceError",
    "InvalidSourceHourTransitionError",
    "ManualFetchCoordinator",
    "ManualFetchResult",
    "ParquetValidationError",
    "ParquetValidationReport",
    "PersistedFetchRun",
    "ProgressSink",
    "QuarantineError",
    "RawRetentionBlockedItem",
    "RawRetentionCandidate",
    "RawRetentionError",
    "RawRetentionPlan",
    "RemoteFileNotFoundError",
    "RemoteRequestError",
    "SQLAlchemyFetchPersistence",
    "SourceDataKind",
    "SourceFileSpec",
    "SourceHourLockUnavailableError",
    "SourceHourStatus",
    "acquire_source_hour_transaction_lock",
    "bounded_diagnostic_text",
    "create_production_manual_fetch_coordinator",
    "download_endpoint",
    "download_query_parameters",
    "intersecting_utc_hours",
    "is_retryable_http_status",
    "latest_release_eligible_hour",
    "nonnegative_counter",
    "normalize_source_hour_status",
    "parse_retry_after_seconds",
    "plan_binance_futures_files",
    "plan_okx_futures_files",
    "plan_production_source_files",
    "quarantine_file",
    "sha256_file",
    "source_hour_lock_keys",
    "validate_parquet_file",
    "validate_source_hour_transition",
    "AvailabilityError",
    "ContiguousAnalysisWindow",
    "HourAvailability",
    "HourAvailabilityState",
    "enabled_preset_hashes",
    "load_hourly_availability",
    "most_recent_contiguous_analyzable_window",
    "preferred_enabled_preset_hash",
    "plan_processed_raw_retention",
    "raw_retention_cutoff_hour",
]

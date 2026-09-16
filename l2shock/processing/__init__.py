# l2shock/processing/__init__.py
"""Production processing contracts and checkpoint discovery foundation."""

from l2shock.processing.checkpoint_store import (
    CheckpointArtifact,
    CheckpointIdentity,
    CheckpointSearchPlan,
    CheckpointSearchStopReason,
    CheckpointStore,
)
from l2shock.processing.l2_coordinator import (
    ProcessingProgressSink,
    SessionScopeFactory,
    SingleMarketL2ProcessingCoordinator,
    create_production_l2_processing_coordinator,
    updated_l2_analytical_output_metadata,
)
from l2shock.processing.price_coordinator import (
    PriceProcessingProgressSink,
    PriceSessionScopeFactory,
    SingleMarketPriceProcessingCoordinator,
    create_production_price_processing_coordinator,
)
from l2shock.processing.errors import (
    CheckpointConflictError,
    CheckpointStoreError,
    ProcessingCancelledError,
    ProcessingContractError,
    ProcessingError,
    ProcessingPersistenceError,
    SourceArchiveError,
    SourceArchiveIntegrityError,
    SourceArchiveMetadataError,
)
from l2shock.processing.integrity import (
    verify_processing_source_archive,
)
from l2shock.processing.models import (
    PriceProcessingRequest,
    PriceProcessingResult,
    ProcessingCancellationProbe,
    ProcessingProgress,
    ProcessingProgressPhase,
    ProcessingQualityState,
    ProcessingRequest,
    ProcessingResult,
    raise_if_processing_cancelled,
)
from l2shock.processing.source_repository import (
    ProcessingSourceArchive,
    ProcessingSourceRepository,
    SQLAlchemyProcessingSourceRepository,
)
from l2shock.processing.target_planning import (
    load_downloaded_processing_targets,
    load_materialization_processing_targets,
)

__all__ = [
    "CheckpointArtifact",
    "CheckpointConflictError",
    "CheckpointIdentity",
    "CheckpointSearchPlan",
    "CheckpointSearchStopReason",
    "CheckpointStore",
    "CheckpointStoreError",
    "PriceProcessingProgressSink",
    "PriceProcessingRequest",
    "PriceProcessingResult",
    "PriceSessionScopeFactory",
    "ProcessingCancellationProbe",
    "ProcessingCancelledError",
    "ProcessingContractError",
    "ProcessingError",
    "ProcessingPersistenceError",
    "ProcessingProgress",
    "ProcessingProgressPhase",
    "ProcessingProgressSink",
    "ProcessingQualityState",
    "ProcessingRequest",
    "ProcessingResult",
    "ProcessingSourceArchive",
    "ProcessingSourceRepository",
    "SQLAlchemyProcessingSourceRepository",
    "SessionScopeFactory",
    "SingleMarketL2ProcessingCoordinator",
    "SingleMarketPriceProcessingCoordinator",
    "SourceArchiveError",
    "SourceArchiveIntegrityError",
    "SourceArchiveMetadataError",
    "create_production_l2_processing_coordinator",
    "create_production_price_processing_coordinator",
    "load_downloaded_processing_targets",
    "load_materialization_processing_targets",
    "raise_if_processing_cancelled",
    "updated_l2_analytical_output_metadata",
    "verify_processing_source_archive",
]

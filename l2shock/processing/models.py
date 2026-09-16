# l2shock/processing/models.py
"""Immutable processing contracts.

These DTOs describe operation identity, progress, and cooperative
cancellation. They do not execute replay or mutate durable state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TypeAlias
from collections.abc import Callable
from uuid import UUID

from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.processing.errors import (
    ProcessingCancelledError,
    ProcessingContractError,
)
from l2shock.timeutils import require_aware_utc

ProcessingCancellationProbe: TypeAlias = Callable[[], bool]


class ProcessingProgressPhase(StrEnum):
    """Stable progress phases shared by processing runtimes and UI adapters."""

    PLANNING = "planning"
    DISCOVERING_CHECKPOINT = "discovering_checkpoint"
    VERIFYING_SOURCES = "verifying_sources"
    REPLAYING_PREDECESSORS = "replaying_predecessors"
    SAMPLING_TARGET = "sampling_target"
    BUILDING_PRICE = "building_price"
    ENCODING = "encoding"
    PUBLISHING_CHECKPOINT = "publishing_checkpoint"
    PERSISTING = "persisting"
    FINALIZING = "finalizing"
    READY = "ready"
    STOPPING = "stopping"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    ERROR = "error"


class ProcessingQualityState(StrEnum):
    """Operational quality summary for one processed source hour."""

    VALID = "VALID"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    """Immutable result of one completed single-market L2 operation."""

    operation_id: UUID
    target: SourceFileSpec
    preset_hash: str

    quality_state: ProcessingQualityState
    valid_count: int
    degraded_count: int
    invalid_count: int

    replay_source_count: int
    analytical_inserted: bool
    preset_inserted: bool

    analytical_content_sha256: str
    input_checkpoint_content_sha256: str | None
    output_checkpoint_content_sha256: str | None

    completed_at: datetime

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            try:
                operation_id = UUID(str(operation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ProcessingContractError(
                    "operation_id must be a valid UUID"
                ) from exc

            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        if not isinstance(self.target, SourceFileSpec):
            raise ProcessingContractError("target must be a SourceFileSpec")

        if self.target.data_kind is not SourceDataKind.ORDERBOOK:
            raise ProcessingContractError(
                "ProcessingResult requires an orderbook target"
            )

        digest_fields = (
            "preset_hash",
            "analytical_content_sha256",
        )

        for name in digest_fields:
            value = str(getattr(self, name) or "").strip()

            if (
                value != value.lower()
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ProcessingContractError(
                    f"{name} must be a canonical lowercase SHA-256"
                )

            object.__setattr__(self, name, value)

        for name in (
            "input_checkpoint_content_sha256",
            "output_checkpoint_content_sha256",
        ):
            value = getattr(self, name)

            if value is None:
                continue

            digest = str(value).strip()

            if (
                digest != digest.lower()
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ProcessingContractError(
                    f"{name} must be null or a canonical lowercase SHA-256"
                )

            object.__setattr__(self, name, digest)

        try:
            quality = ProcessingQualityState(self.quality_state)
        except (TypeError, ValueError) as exc:
            raise ProcessingContractError(
                "Unsupported processing quality state"
            ) from exc

        object.__setattr__(self, "quality_state", quality)

        for name in (
            "valid_count",
            "degraded_count",
            "invalid_count",
            "replay_source_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProcessingContractError(f"{name} must be a non-negative integer")

        if self.valid_count + self.degraded_count + self.invalid_count != 3_600:
            raise ProcessingContractError("Processing quality counts must total 3,600")

        expected_quality = (
            ProcessingQualityState.VALID
            if self.valid_count == 3_600
            else (
                ProcessingQualityState.DEGRADED
                if self.valid_count > 0 or self.degraded_count > 0
                else ProcessingQualityState.INVALID
            )
        )

        if quality is not expected_quality:
            raise ProcessingContractError(
                "ProcessingResult quality_state does not match its "
                "valid/degraded/invalid counts"
            )

        if self.replay_source_count <= 0:
            raise ProcessingContractError("replay_source_count must be positive")

        if not isinstance(self.analytical_inserted, bool):
            raise ProcessingContractError("analytical_inserted must be bool")

        if not isinstance(self.preset_inserted, bool):
            raise ProcessingContractError("preset_inserted must be bool")

        object.__setattr__(
            self,
            "completed_at",
            require_aware_utc(
                "completed_at",
                self.completed_at,
            ),
        )


@dataclass(frozen=True, slots=True)
class PriceProcessingRequest:
    """One deterministic Binance Futures trade-price processing request.

    Version 1 uses the current source archive by default.

    Adjacent source archives are included only when explicitly requested. This
    avoids silently changing an already-persisted price-hour provenance identity
    merely because neighboring archives become available later.
    """

    operation_id: UUID
    target: SourceFileSpec
    include_adjacent_sources: bool = False

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            try:
                operation_id = UUID(str(operation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ProcessingContractError(
                    "operation_id must be a valid UUID"
                ) from exc

            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        if not isinstance(self.target, SourceFileSpec):
            raise ProcessingContractError("target must be a SourceFileSpec")

        if self.target.data_kind is not SourceDataKind.TRADES:
            raise ProcessingContractError(
                "PriceProcessingRequest requires a trades source"
            )

        if not isinstance(self.include_adjacent_sources, bool):
            raise ProcessingContractError("include_adjacent_sources must be bool")


@dataclass(frozen=True, slots=True)
class PriceProcessingResult:
    """Immutable result of one completed trade-price processing operation."""

    operation_id: UUID
    target: SourceFileSpec

    quality_state: ProcessingQualityState
    valid_count: int
    invalid_count: int
    total_trade_count: int

    source_archive_count: int
    price_inserted: bool

    price_content_sha256: str
    completed_at: datetime

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            try:
                operation_id = UUID(str(operation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ProcessingContractError(
                    "operation_id must be a valid UUID"
                ) from exc

            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        if not isinstance(self.target, SourceFileSpec):
            raise ProcessingContractError("target must be a SourceFileSpec")

        if self.target.data_kind is not SourceDataKind.TRADES:
            raise ProcessingContractError(
                "PriceProcessingResult requires a trades target"
            )

        try:
            quality = ProcessingQualityState(self.quality_state)
        except (TypeError, ValueError) as exc:
            raise ProcessingContractError(
                "Unsupported price processing quality state"
            ) from exc

        object.__setattr__(
            self,
            "quality_state",
            quality,
        )

        for name in (
            "valid_count",
            "invalid_count",
            "total_trade_count",
            "source_archive_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProcessingContractError(f"{name} must be a non-negative integer")

        if self.valid_count + self.invalid_count != 3_600:
            raise ProcessingContractError(
                "Price processing quality counts must total 3,600"
            )

        if self.source_archive_count <= 0:
            raise ProcessingContractError("source_archive_count must be positive")

        if not isinstance(self.price_inserted, bool):
            raise ProcessingContractError("price_inserted must be bool")

        expected_quality = (
            ProcessingQualityState.VALID
            if self.valid_count == 3_600
            else (
                ProcessingQualityState.DEGRADED
                if self.valid_count > 0
                else ProcessingQualityState.INVALID
            )
        )

        if quality is not expected_quality:
            raise ProcessingContractError(
                "Price processing quality_state does not match its "
                "valid/invalid counts"
            )

        digest = str(self.price_content_sha256 or "").strip()

        if (
            digest != digest.lower()
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ProcessingContractError(
                "price_content_sha256 must be a canonical lowercase SHA-256"
            )

        object.__setattr__(
            self,
            "price_content_sha256",
            digest,
        )

        object.__setattr__(
            self,
            "completed_at",
            require_aware_utc(
                "completed_at",
                self.completed_at,
            ),
        )


@dataclass(frozen=True, slots=True)
class ProcessingRequest:
    """Identity and bounded-search policy for one order-book source hour."""

    operation_id: UUID
    target: SourceFileSpec
    max_checkpoint_search_hours: int

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            try:
                operation_id = UUID(str(operation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ProcessingContractError(
                    "operation_id must be a valid UUID"
                ) from exc

            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        if not isinstance(self.target, SourceFileSpec):
            raise ProcessingContractError("target must be a SourceFileSpec")

        if self.target.data_kind is not SourceDataKind.ORDERBOOK:
            raise ProcessingContractError(
                "Batch 8A processing requests require an orderbook source"
            )

        if (
            isinstance(self.max_checkpoint_search_hours, bool)
            or not isinstance(self.max_checkpoint_search_hours, int)
            or self.max_checkpoint_search_hours <= 0
        ):
            raise ProcessingContractError(
                "max_checkpoint_search_hours must be a positive integer"
            )


@dataclass(frozen=True, slots=True)
class ProcessingProgress:
    """One immutable progress event for a future processing coordinator."""

    operation_id: UUID
    target: SourceFileSpec
    phase: ProcessingProgressPhase
    message: str
    completed_units: int = 0
    total_units: int | None = None

    def __post_init__(self) -> None:
        operation_id = self.operation_id

        if not isinstance(operation_id, UUID):
            try:
                operation_id = UUID(str(operation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ProcessingContractError(
                    "operation_id must be a valid UUID"
                ) from exc

            object.__setattr__(
                self,
                "operation_id",
                operation_id,
            )

        if not isinstance(self.target, SourceFileSpec):
            raise ProcessingContractError("target must be a SourceFileSpec")

        try:
            phase = ProcessingProgressPhase(self.phase)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in ProcessingProgressPhase)
            raise ProcessingContractError(f"phase must be one of: {allowed}") from exc

        object.__setattr__(self, "phase", phase)

        message = str(self.message or "").strip()

        if not message:
            raise ProcessingContractError("Processing progress message cannot be blank")

        object.__setattr__(self, "message", message)

        if (
            isinstance(self.completed_units, bool)
            or not isinstance(self.completed_units, int)
            or self.completed_units < 0
        ):
            raise ProcessingContractError(
                "completed_units must be a non-negative integer"
            )

        if self.total_units is not None:
            if (
                isinstance(self.total_units, bool)
                or not isinstance(self.total_units, int)
                or self.total_units < 0
            ):
                raise ProcessingContractError(
                    "total_units must be null or a non-negative integer"
                )

            if self.completed_units > self.total_units:
                raise ProcessingContractError(
                    "completed_units cannot exceed total_units"
                )


def raise_if_processing_cancelled(
    cancellation_probe: ProcessingCancellationProbe | None,
) -> None:
    """Raise the stable processing cancellation exception when requested."""
    if cancellation_probe is None:
        return

    try:
        cancelled = cancellation_probe()
    except ProcessingCancelledError:
        raise
    except Exception as exc:
        raise ProcessingContractError("Processing cancellation probe failed") from exc

    if not isinstance(cancelled, bool):
        raise ProcessingContractError("Processing cancellation probe must return bool")

    if cancelled:
        raise ProcessingCancelledError("Processing cancellation was requested")


__all__ = [
    "PriceProcessingRequest",
    "PriceProcessingResult",
    "ProcessingCancellationProbe",
    "ProcessingProgress",
    "ProcessingProgressPhase",
    "ProcessingQualityState",
    "ProcessingRequest",
    "ProcessingResult",
    "raise_if_processing_cancelled",
]

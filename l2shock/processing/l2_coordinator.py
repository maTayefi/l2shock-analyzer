# l2shock/processing/l2_coordinator.py
"""Production single-market L2 source-chain processing.

This coordinator composes existing, independently tested boundaries:

    read-only checkpoint/source discovery
    -> exact source-file integrity verification
    -> predecessor-only replay when initialization requires it
    -> target-hour one-second depth liquidity
    -> compact block encoding
    -> immutable checkpoint publication
    -> idempotent PostgreSQL analytical persistence
    -> source-hour terminal operational metadata

Important ownership rules:

- raw source rows remain in Parquet;
- only the target hour is sampled for liquidity;
- predecessor hours are replayed only to establish target initialization;
- a checkpoint is published before the database transaction commits;
- a database failure may therefore leave an immutable orphan checkpoint;
- retry verifies and reuses identical checkpoint content;
- cancellation is honored before checkpoint publication;
- after checkpoint publication begins, the coordinator finishes the short
  persistence/finalization path rather than falsely reporting a safe reset;
- no cross-market aggregation occurs here.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol, TypeAlias

from sqlalchemy.orm import Session

from l2shock.acquisition.locks import (
    acquire_source_hour_transaction_lock,
)
from l2shock.acquisition.models import SourceFileSpec
from l2shock.acquisition.persistence import (
    SourceHourStatus,
    validate_source_hour_transition,
)
from l2shock.acquisition.repository import AcquisitionRepository
from l2shock.db.analytical_repository import (
    AnalyticalRepository,
    L2HourlyProvenance,
    SourceHourReference,
)
from l2shock.db.engine import session_scope
from l2shock.ingest.replay import (
    OrderBookCheckpoint,
    ReplayCancelledError,
    replay_orderbook_archives,
)
from l2shock.liquidity import (
    DepthLiquidityCancelledError,
    encode_hourly_liquidity_block,
    liquidity_quality_summary_to_dict,
    sample_liquidity_archives,
)
from l2shock.presets import LiquidityDataPreset
from l2shock.processing.checkpoint_store import (
    CheckpointArtifact,
    CheckpointSearchPlan,
    CheckpointSearchStopReason,
    CheckpointStore,
)
from l2shock.processing.errors import (
    ProcessingCancelledError,
    ProcessingContractError,
    ProcessingPersistenceError,
)
from l2shock.processing.integrity import (
    verify_processing_source_archive,
)
from l2shock.processing.models import (
    ProcessingCancellationProbe,
    ProcessingProgress,
    ProcessingProgressPhase,
    ProcessingQualityState,
    ProcessingRequest,
    ProcessingResult,
    raise_if_processing_cancelled,
)
from l2shock.processing.source_repository import (
    ProcessingSourceRepository,
    SQLAlchemyProcessingSourceRepository,
)
from l2shock.timeutils import now_utc

log = logging.getLogger(__name__)


ProcessingProgressSink: TypeAlias = Callable[
    [ProcessingProgress],
    object,
]


class SessionScopeFactory(Protocol):
    def __call__(self) -> AbstractContextManager[Session]: ...


def _quality_state(
    *,
    valid_count: int,
    degraded_count: int,
    invalid_count: int,
) -> ProcessingQualityState:
    if valid_count == 3_600:
        return ProcessingQualityState.VALID

    if valid_count > 0 or degraded_count > 0:
        return ProcessingQualityState.DEGRADED

    if invalid_count == 3_600:
        return ProcessingQualityState.INVALID

    raise ProcessingContractError(
        "Hourly quality counts do not describe 3,600 observations"
    )


def _canonical_sha256_or_none(
    value: object,
) -> str | None:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        return None

    return digest


def updated_l2_analytical_output_metadata(
    previous_quality: object,
    *,
    preset_hash: str,
    content_sha256: str,
) -> tuple[list[str], dict[str, str]]:
    """Preserve every known L2 analytical output for one source hour."""

    normalized_preset_hash = _canonical_sha256_or_none(preset_hash)
    normalized_content_hash = _canonical_sha256_or_none(content_sha256)

    if normalized_preset_hash is None:
        raise ProcessingContractError(
            "preset_hash must be a canonical lowercase SHA-256"
        )

    if normalized_content_hash is None:
        raise ProcessingContractError(
            "content_sha256 must be a canonical lowercase SHA-256"
        )

    quality = dict(previous_quality) if isinstance(previous_quality, dict) else {}

    known_hashes: set[str] = set()

    legacy_hash = _canonical_sha256_or_none(quality.get("analytical_content_sha256"))

    if legacy_hash is not None:
        known_hashes.add(legacy_hash)

    raw_hashes = quality.get(
        "analytical_content_sha256s",
        (),
    )

    if isinstance(raw_hashes, (list, tuple)):
        for value in raw_hashes:
            digest = _canonical_sha256_or_none(value)

            if digest is not None:
                known_hashes.add(digest)

    outputs_by_preset: dict[str, str] = {}

    raw_outputs = quality.get(
        "analytical_outputs_by_preset",
        {},
    )

    if isinstance(raw_outputs, dict):
        for raw_preset, raw_content in raw_outputs.items():
            output_preset = _canonical_sha256_or_none(raw_preset)
            output_content = _canonical_sha256_or_none(raw_content)

            if output_preset is None or output_content is None:
                continue

            outputs_by_preset[output_preset] = output_content
            known_hashes.add(output_content)

    outputs_by_preset[normalized_preset_hash] = normalized_content_hash
    known_hashes.add(normalized_content_hash)

    return (
        sorted(known_hashes),
        dict(sorted(outputs_by_preset.items())),
    )


# Compatibility alias retained for existing focused tests and internal callers.
# New infrastructure should use the public descriptive name above.
_updated_analytical_output_metadata = updated_l2_analytical_output_metadata


def _single_market_contract(
    request: ProcessingRequest,
    preset: LiquidityDataPreset,
) -> None:
    if not isinstance(preset, LiquidityDataPreset):
        raise TypeError("preset must be a LiquidityDataPreset")

    if preset.base != request.target.base:
        raise ProcessingContractError(
            "Preset base does not match the processing target"
        )

    if len(preset.eligible_markets) != 1:
        raise ProcessingContractError(
            "The current coordinator supports exactly one eligible market"
        )

    market = preset.eligible_markets[0]

    if (
        market.provider != request.target.provider
        or market.venue != request.target.venue
        or market.instrument != request.target.symbol
    ):
        raise ProcessingContractError(
            "Preset eligible-market identity does not match the target source"
        )


class SingleMarketL2ProcessingCoordinator:
    """Process one empirically supported single-market order-book target hour."""

    def __init__(
        self,
        *,
        checkpoint_store: CheckpointStore,
        progress_sink: ProcessingProgressSink | None = None,
        session_scope_factory: SessionScopeFactory = session_scope,
        raw_root: Path | None = None,
        batch_size: int = 131_072,
        cancellation_check_interval_rows: int = 4_096,
        cancellation_check_interval_levels: int = 1_024,
        imbalance_decimal_precision: int = 34,
    ) -> None:
        if not isinstance(checkpoint_store, CheckpointStore):
            raise TypeError("checkpoint_store must be a CheckpointStore")

        for name, value in (
            ("batch_size", batch_size),
            (
                "cancellation_check_interval_rows",
                cancellation_check_interval_rows,
            ),
            (
                "cancellation_check_interval_levels",
                cancellation_check_interval_levels,
            ),
            (
                "imbalance_decimal_precision",
                imbalance_decimal_precision,
            ),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ProcessingContractError(f"{name} must be a positive integer")

        if not callable(session_scope_factory):
            raise TypeError("session_scope_factory must be callable")

        self._checkpoint_store = checkpoint_store
        self._progress_sink = progress_sink
        self._session_scope_factory = session_scope_factory
        self._raw_root = (
            Path(raw_root).expanduser().resolve() if raw_root is not None else None
        )
        self._batch_size = batch_size
        self._cancellation_check_interval_rows = cancellation_check_interval_rows
        self._cancellation_check_interval_levels = cancellation_check_interval_levels
        self._imbalance_decimal_precision = imbalance_decimal_precision

        # Exact terminal replay state owned by this coordinator instance.
        #
        # The UTC hour is part of the key because a checkpoint through hour N
        # may initialize only hour N+1. A provider/venue/symbol-only cache
        # would violate immediate-hour checkpoint ownership.
        #
        # None is meaningful: the exact completed hour finished without a
        # usable checkpoint, so the immediately following target must begin
        # uninitialized unless its own archive contains a snapshot.
        self._terminal_state_cache: dict[
            tuple[str, str, str, datetime],
            CheckpointArtifact | None,
        ] = {}

    @staticmethod
    def _terminal_state_key(
        spec: SourceFileSpec,
    ) -> tuple[str, str, str, datetime]:
        return (
            spec.provider,
            spec.venue,
            spec.symbol,
            spec.hour_utc,
        )

    def _cached_search_plan(
        self,
        target: SourceFileSpec,
        source_repository: ProcessingSourceRepository,
    ) -> CheckpointSearchPlan | None:
        """Return a target-only plan from the exact predecessor terminal state.

        This lookup occurs before ordinary backward discovery. Therefore a
        sequential processing operation does not rediscover, lock, hash, and
        replay an expanding predecessor chain for every later target.
        """

        predecessor_hour = target.hour_utc - timedelta(hours=1)
        predecessor_key = (
            target.provider,
            target.venue,
            target.symbol,
            predecessor_hour,
        )

        if predecessor_key not in self._terminal_state_cache:
            return None

        target_archive = source_repository.find_replayable(target)

        if target_archive is None:
            raise ProcessingContractError(
                "Target source archive is not locally replayable"
            )

        cached_checkpoint = self._terminal_state_cache[predecessor_key]

        if cached_checkpoint is not None:
            return CheckpointSearchPlan(
                target=target,
                replay_sources=(target_archive,),
                inspected_checkpoint_hours=(predecessor_hour,),
                stop_reason=CheckpointSearchStopReason.CHECKPOINT_FOUND,
                checkpoint=cached_checkpoint,
            )

        return CheckpointSearchPlan(
            target=target,
            replay_sources=(target_archive,),
            inspected_checkpoint_hours=(predecessor_hour,),
            stop_reason=(CheckpointSearchStopReason.PREDECESSOR_UNINITIALIZED),
        )

    def _emit(
        self,
        request: ProcessingRequest,
        phase: ProcessingProgressPhase,
        message: str,
        *,
        completed_units: int = 0,
        total_units: int | None = None,
    ) -> None:
        sink = self._progress_sink

        if sink is None:
            return

        event = ProcessingProgress(
            operation_id=request.operation_id,
            target=request.target,
            phase=phase,
            message=message,
            completed_units=completed_units,
            total_units=total_units,
        )

        try:
            result = sink(event)

            if inspect.isawaitable(result):
                raise ProcessingContractError(
                    "Synchronous processing progress sinks must not return "
                    "an awaitable"
                )
        except ProcessingContractError:
            raise
        except Exception:
            # Progress/UI failure must not corrupt processing truth.
            log.exception(
                "Processing progress sink failed for operation %s.",
                request.operation_id,
            )

    def run(
        self,
        request: ProcessingRequest,
        preset: LiquidityDataPreset,
        *,
        cancellation_probe: ProcessingCancellationProbe | None = None,
    ) -> ProcessingResult:
        """Run one recoverable single-market L2 processing operation."""
        if not isinstance(request, ProcessingRequest):
            raise TypeError("request must be a ProcessingRequest")

        _single_market_contract(request, preset)

        self._emit(
            request,
            ProcessingProgressPhase.PLANNING,
            "Planning single-market L2 processing.",
        )

        raise_if_processing_cancelled(cancellation_probe)

        cancelled: ProcessingCancelledError | None = None
        result: ProcessingResult | None = None

        try:
            with self._session_scope_factory() as session:
                try:
                    result = self._run_transaction(
                        session,
                        request,
                        preset,
                        cancellation_probe=cancellation_probe,
                    )
                except ProcessingCancelledError as exc:
                    self._reset_cancelled_source(
                        session,
                        request,
                    )
                    cancelled = exc

        except Exception:
            self._emit(
                request,
                ProcessingProgressPhase.ERROR,
                "L2 processing failed.",
            )
            raise

        if cancelled is not None:
            self._emit(
                request,
                ProcessingProgressPhase.CANCELLED,
                "L2 processing was cancelled before terminal publication.",
            )
            raise cancelled

        if result is None:
            raise ProcessingPersistenceError(
                "Processing transaction completed without a result"
            )

        self._emit(
            request,
            ProcessingProgressPhase.COMPLETED,
            "Single-market L2 processing completed.",
        )

        return result

    def _run_transaction(
        self,
        session: Session,
        request: ProcessingRequest,
        preset: LiquidityDataPreset,
        *,
        cancellation_probe: ProcessingCancellationProbe | None,
    ) -> ProcessingResult:
        target = request.target

        acquire_source_hour_transaction_lock(
            session,
            target,
        )

        acquisition_repository = AcquisitionRepository(session)
        target_row = acquisition_repository.get_source_hour(target)

        if target_row is None:
            raise ProcessingContractError("Target source-hour row does not exist")

        try:
            current_status = SourceHourStatus(target_row.status)
        except (TypeError, ValueError) as exc:
            raise ProcessingContractError(
                "Target source has an unsupported durable status"
            ) from exc

        if current_status not in {
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.PROCESSING,
            SourceHourStatus.PROCESSED,
        }:
            raise ProcessingContractError(
                "Target source is not available for processing"
            )

        if current_status is SourceHourStatus.DOWNLOADED:
            validate_source_hour_transition(
                current_status,
                SourceHourStatus.PROCESSING,
            )
            target_row.status = SourceHourStatus.PROCESSING.value
            session.flush()

        raise_if_processing_cancelled(cancellation_probe)

        source_repository = SQLAlchemyProcessingSourceRepository(
            session,
            raw_root=self._raw_root,
        )

        self._emit(
            request,
            ProcessingProgressPhase.DISCOVERING_CHECKPOINT,
            "Discovering checkpoint and contiguous predecessor sources.",
        )

        plan = self._cached_search_plan(
            target,
            source_repository,
        )
        used_cached_terminal_state = plan is not None

        if plan is None:
            plan = self._checkpoint_store.build_search_plan(
                target,
                source_repository,
                max_checkpoint_search_hours=(request.max_checkpoint_search_hours),
                cancellation_probe=cancellation_probe,
            )
        else:
            self._emit(
                request,
                ProcessingProgressPhase.DISCOVERING_CHECKPOINT,
                (
                    "Reused the exact immediately preceding terminal replay "
                    "state; backward predecessor discovery is unnecessary."
                ),
            )

        for archive in sorted(
            plan.replay_sources,
            key=lambda item: item.spec.identity_tuple,
        ):
            acquire_source_hour_transaction_lock(
                session,
                archive.spec,
            )

        if used_cached_terminal_state:
            locked_plan = self._cached_search_plan(
                target,
                source_repository,
            )

            if locked_plan is None:
                raise ProcessingContractError(
                    "Cached predecessor terminal state disappeared while "
                    "source locks were being acquired"
                )
        else:
            locked_plan = self._checkpoint_store.build_search_plan(
                target,
                source_repository,
                max_checkpoint_search_hours=(request.max_checkpoint_search_hours),
                cancellation_probe=cancellation_probe,
            )

        if locked_plan != plan:
            raise ProcessingContractError(
                "Replay source or checkpoint discovery changed while "
                "source locks were being acquired"
            )

        plan = locked_plan
        total_sources = len(plan.replay_sources)

        for index, archive in enumerate(
            plan.replay_sources,
            start=1,
        ):
            self._emit(
                request,
                ProcessingProgressPhase.VERIFYING_SOURCES,
                f"Verifying source archive {archive.spec.remote_path}",
                completed_units=index - 1,
                total_units=total_sources,
            )

            verify_processing_source_archive(
                archive,
                cancellation_probe=cancellation_probe,
            )

        self._emit(
            request,
            ProcessingProgressPhase.VERIFYING_SOURCES,
            "All replay source archives passed SHA-256 verification.",
            completed_units=total_sources,
            total_units=total_sources,
        )

        raise_if_processing_cancelled(cancellation_probe)

        target_archive = plan.replay_sources[-1]
        target_initial_checkpoint = self._target_initial_checkpoint(
            request,
            plan,
            cancellation_probe=cancellation_probe,
        )

        self._emit(
            request,
            ProcessingProgressPhase.SAMPLING_TARGET,
            "Replaying and sampling the target liquidity hour.",
        )

        try:
            sampled = sample_liquidity_archives(
                (
                    (
                        target_archive.local_path,
                        target_archive.spec,
                    ),
                ),
                preset.band,
                initial_checkpoint=target_initial_checkpoint,
                batch_size=self._batch_size,
                final_source_content_sha256=(target_archive.content_sha256),
                cancellation_probe=cancellation_probe,
                cancellation_check_interval_rows=(
                    self._cancellation_check_interval_rows
                ),
                cancellation_check_interval_levels=(
                    self._cancellation_check_interval_levels
                ),
                imbalance_decimal_precision=(self._imbalance_decimal_precision),
            )
        except (
            ReplayCancelledError,
            DepthLiquidityCancelledError,
        ) as exc:
            raise ProcessingCancelledError(
                "L2 processing was cancelled during replay or liquidity " "sampling"
            ) from exc

        # Every replay source was initially verified before interpretation.
        # Reverify the complete source set after all predecessor replay and
        # target sampling have finished, but before encoding or checkpoint
        # publication. A source changed during interpretation must fail closed.
        for archive in plan.replay_sources:
            verify_processing_source_archive(
                archive,
                cancellation_probe=cancellation_probe,
            )

        if len(sampled.hours) != 1:
            raise ProcessingContractError(
                "Target-only sampling did not produce exactly one hour"
            )

        target_block = sampled.hours[0]

        if target_block.spec != target:
            raise ProcessingContractError(
                "Sampled liquidity block does not belong to the target source"
            )

        raise_if_processing_cancelled(cancellation_probe)

        self._emit(
            request,
            ProcessingProgressPhase.ENCODING,
            "Encoding compact target-hour liquidity channels.",
        )

        encoded = encode_hourly_liquidity_block(target_block)
        quality_summary_json = liquidity_quality_summary_to_dict(
            target_block.quality_summary
        )

        # This is the final safe cancellation point. No analytical row or
        # output checkpoint has yet been published.
        raise_if_processing_cancelled(cancellation_probe)

        output_checkpoint: CheckpointArtifact | None = None
        final_checkpoint = sampled.replay_report.final_checkpoint

        if final_checkpoint is not None:
            self._emit(
                request,
                ProcessingProgressPhase.PUBLISHING_CHECKPOINT,
                "Publishing immutable target-hour checkpoint.",
            )
            output_checkpoint = self._checkpoint_store.publish(final_checkpoint)

        # Cache the exact target-hour terminal state for only the immediately
        # following target processed by this coordinator.
        #
        # A published artifact is retained rather than only its decoded
        # checkpoint so the next target's provenance can own the exact input
        # checkpoint content SHA-256.
        #
        # None is also retained intentionally: it proves that this exact target
        # finished without usable carried state.
        self._terminal_state_cache[self._terminal_state_key(target)] = output_checkpoint

        # Do not consult the cancellation probe after checkpoint publication.
        # The remaining operations are short, idempotent persistence and source
        # finalization. Reporting cancellation here would incorrectly claim a
        # safe PROCESSING -> DOWNLOADED reset after terminal publication.

        self._emit(
            request,
            ProcessingProgressPhase.PERSISTING,
            "Persisting preset and compact target-hour liquidity.",
        )

        analytical_repository = AnalyticalRepository(session)
        preset_result = analytical_repository.ensure_preset(
            preset,
            enabled=True,
        )

        provenance = L2HourlyProvenance(
            source_hours=tuple(
                SourceHourReference(
                    provider=archive.spec.provider,
                    venue=archive.spec.venue,
                    instrument=archive.spec.symbol,
                    hour_utc=archive.spec.hour_utc,
                    content_sha256=archive.content_sha256,
                )
                for archive in plan.replay_sources
            ),
            checkpoint_content_sha256=(
                plan.checkpoint.encoding_info.content_sha256
                if plan.checkpoint is not None
                else None
            ),
            replay_schema_version=1,
            liquidity_schema_version=1,
        )

        try:
            write_result = analytical_repository.write_l2_hour(
                preset=preset,
                hour_utc=target.hour_utc,
                encoded=encoded,
                quality_summary_json=quality_summary_json,
                provenance=provenance,
            )
        except Exception as exc:
            raise ProcessingPersistenceError(
                "Could not persist compact target-hour liquidity"
            ) from exc

        summary = target_block.quality_summary
        quality_state = _quality_state(
            valid_count=summary.valid_count,
            degraded_count=summary.degraded_count,
            invalid_count=summary.invalid_count,
        )

        self._emit(
            request,
            ProcessingProgressPhase.FINALIZING,
            "Finalizing target source-hour processing metadata.",
        )

        target_report = sampled.replay_report.archives[-1]

        target_row.event_count = target_report.events_seen
        target_row.row_count = target_report.reader_report.rows_read
        target_row.snapshot_count = target_report.reader_report.snapshot_event_count
        target_row.continuity_mismatch_count = (
            target_report.reader_report.continuity_mismatch_count
        )
        previous_quality = (
            dict(target_row.quality_json)
            if isinstance(target_row.quality_json, dict)
            else {}
        )

        (
            analytical_content_sha256s,
            analytical_outputs_by_preset,
        ) = updated_l2_analytical_output_metadata(
            previous_quality,
            preset_hash=preset.preset_hash,
            content_sha256=encoded.content_sha256,
        )

        previous_output_checkpoint = _canonical_sha256_or_none(
            previous_quality.get("output_checkpoint_content_sha256")
        )
        output_checkpoint_content_sha256 = (
            output_checkpoint.encoding_info.content_sha256
            if output_checkpoint is not None
            else previous_output_checkpoint
        )

        updated_quality = dict(previous_quality)
        updated_quality.update(
            {
                "schema": "l2shock.processed_source_hour_quality",
                "schema_version": 1,
                "quality_state": quality_state.value,
                "liquidity_quality_summary": quality_summary_json,
                "search_stop_reason": plan.stop_reason.value,
                "replay_source_count": len(plan.replay_sources),
                "input_checkpoint_content_sha256": (
                    plan.checkpoint.encoding_info.content_sha256
                    if plan.checkpoint is not None
                    else None
                ),
                "output_checkpoint_content_sha256": (output_checkpoint_content_sha256),
                # The singular field remains the latest materialization for
                # backward-compatible operational displays. The complete list
                # and preset map preserve every immutable analytical output
                # owned by this raw source hour.
                "analytical_content_sha256": encoded.content_sha256,
                "analytical_content_sha256s": (analytical_content_sha256s),
                "analytical_outputs_by_preset": (analytical_outputs_by_preset),
            }
        )

        target_row.quality_state = quality_state.value
        target_row.quality_json = updated_quality

        if current_status is not SourceHourStatus.PROCESSED:
            validate_source_hour_transition(
                SourceHourStatus.PROCESSING,
                SourceHourStatus.PROCESSED,
            )
            target_row.status = SourceHourStatus.PROCESSED.value

        target_row.processed_at = now_utc()
        target_row.error_text = None
        session.flush()

        return ProcessingResult(
            operation_id=request.operation_id,
            target=target,
            preset_hash=preset.preset_hash,
            quality_state=quality_state,
            valid_count=summary.valid_count,
            degraded_count=summary.degraded_count,
            invalid_count=summary.invalid_count,
            replay_source_count=len(plan.replay_sources),
            analytical_inserted=write_result.inserted,
            preset_inserted=preset_result.inserted,
            analytical_content_sha256=(encoded.content_sha256),
            input_checkpoint_content_sha256=(
                plan.checkpoint.encoding_info.content_sha256
                if plan.checkpoint is not None
                else None
            ),
            output_checkpoint_content_sha256=(
                output_checkpoint.encoding_info.content_sha256
                if output_checkpoint is not None
                else None
            ),
            completed_at=now_utc(),
        )

    def _target_initial_checkpoint(
        self,
        request: ProcessingRequest,
        plan: CheckpointSearchPlan,
        *,
        cancellation_probe: ProcessingCancellationProbe | None,
    ) -> OrderBookCheckpoint | None:
        sources = plan.replay_sources
        initial_checkpoint = (
            plan.checkpoint.checkpoint if plan.checkpoint is not None else None
        )

        # A target-only plan means either:
        #
        # 1. the exact immediately preceding checkpoint initializes the target;
        # 2. the exact predecessor is known to have finished uninitialized;
        # 3. ordinary discovery found no predecessor source.
        if len(sources) == 1:
            return initial_checkpoint

        # A checkpoint found several hours behind the target initializes the
        # oldest replay source, not the target. Every intermediate predecessor
        # must be replayed before target sampling.
        predecessors = sources[:-1]

        self._emit(
            request,
            ProcessingProgressPhase.REPLAYING_PREDECESSORS,
            "Replaying predecessor archives to establish target initialization.",
            completed_units=0,
            total_units=len(predecessors),
        )

        try:
            report = replay_orderbook_archives(
                tuple(
                    (
                        archive.local_path,
                        archive.spec,
                    )
                    for archive in predecessors
                ),
                initial_checkpoint=initial_checkpoint,
                batch_size=self._batch_size,
                final_source_content_sha256=(predecessors[-1].content_sha256),
                cancellation_probe=cancellation_probe,
                cancellation_check_interval_rows=(
                    self._cancellation_check_interval_rows
                ),
            )
        except ReplayCancelledError as exc:
            raise ProcessingCancelledError(
                "L2 processing was cancelled during predecessor replay"
            ) from exc

        raise_if_processing_cancelled(cancellation_probe)

        self._emit(
            request,
            ProcessingProgressPhase.REPLAYING_PREDECESSORS,
            (
                "Predecessor replay produced target initialization."
                if report.final_checkpoint is not None
                else (
                    "Predecessor replay did not finish with a usable "
                    "target checkpoint; the target must initialize from "
                    "its own snapshot or remain invalid."
                )
            ),
            completed_units=len(predecessors),
            total_units=len(predecessors),
        )

        return report.final_checkpoint

    @staticmethod
    def _reset_cancelled_source(
        session: Session,
        request: ProcessingRequest,
    ) -> None:
        repository = AcquisitionRepository(session)
        row = repository.get_source_hour(request.target)

        if row is None:
            return

        try:
            current = SourceHourStatus(row.status)
        except TypeError, ValueError:
            return

        if current is not SourceHourStatus.PROCESSING:
            return

        validate_source_hour_transition(
            current,
            SourceHourStatus.DOWNLOADED,
            allow_processing_cancellation_reset=True,
        )

        row.status = SourceHourStatus.DOWNLOADED.value
        row.error_text = None
        session.flush()


def create_production_l2_processing_coordinator(
    *,
    progress_sink: ProcessingProgressSink | None = None,
) -> SingleMarketL2ProcessingCoordinator:
    """Build the synchronous production coordinator from current settings."""
    from l2shock.config import get_settings

    settings = get_settings()

    return SingleMarketL2ProcessingCoordinator(
        checkpoint_store=CheckpointStore(settings.storage.cache_path),
        progress_sink=progress_sink,
        raw_root=settings.storage.raw_path,
    )


__all__ = [
    "ProcessingProgressSink",
    "SessionScopeFactory",
    "SingleMarketL2ProcessingCoordinator",
    "create_production_l2_processing_coordinator",
    "updated_l2_analytical_output_metadata",
]

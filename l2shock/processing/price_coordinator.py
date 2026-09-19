# l2shock/processing/price_coordinator.py
"""Production Binance Futures trade-price processing.

This coordinator composes existing verified boundaries:

    exact source lookup
    -> source-file SHA-256 verification
    -> streamed trade_time_ms OHLC construction
    -> compact price block encoding
    -> typed price provenance
    -> idempotent PostgreSQL persistence
    -> target trade-source operational finalization

Version-1 default processing uses only the current source archive.

Adjacent archives may be included only when explicitly requested by the
PriceProcessingRequest. Their availability must not silently alter the durable
identity of an already-persisted price hour.

No order-book midpoint, spot price, or alternative exchange price is used.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import timedelta
from pathlib import Path
from typing import Protocol, TypeAlias

from sqlalchemy.orm import Session

from l2shock.acquisition.locks import (
    acquire_source_hour_transaction_lock,
)
from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.persistence import (
    SourceHourStatus,
    validate_source_hour_transition,
)
from l2shock.acquisition.repository import AcquisitionRepository
from l2shock.db.engine import session_scope
from l2shock.db.price_repository import (
    PriceAnalyticalRepository,
    PriceHourlyProvenance,
    PriceSourceHourReference,
)
from l2shock.ingest.parquet_reader import (
    StreamedParquetReadCancelled,
    TradeReadReport,
)
from l2shock.price import (
    TradeOHLCCancelledError,
    encode_hourly_trade_ohlc_block,
    stream_trade_ohlc_hour,
    trade_ohlc_quality_summary_to_dict,
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
    PriceProcessingRequest,
    PriceProcessingResult,
    ProcessingCancellationProbe,
    ProcessingProgress,
    ProcessingProgressPhase,
    ProcessingQualityState,
    raise_if_processing_cancelled,
)
from l2shock.processing.source_repository import (
    ProcessingSourceArchive,
    SQLAlchemyProcessingSourceRepository,
)
from l2shock.timeutils import now_utc

log = logging.getLogger(__name__)

PriceProcessingProgressSink: TypeAlias = Callable[
    [ProcessingProgress],
    object,
]


class PriceSessionScopeFactory(Protocol):
    def __call__(self) -> AbstractContextManager[Session]: ...


def _price_quality_state(
    *,
    valid_count: int,
    invalid_count: int,
) -> ProcessingQualityState:
    if valid_count + invalid_count != 3_600:
        raise ProcessingContractError(
            "Price quality counts do not describe 3,600 observations"
        )

    if valid_count == 3_600:
        return ProcessingQualityState.VALID

    if valid_count > 0:
        return ProcessingQualityState.DEGRADED

    return ProcessingQualityState.INVALID


def _adjacent_trade_spec(
    target: SourceFileSpec,
    *,
    offset_hours: int,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider=target.provider,
        venue=target.venue,
        symbol=target.symbol,
        data_kind=SourceDataKind.TRADES,
        hour_utc=target.hour_utc + timedelta(hours=offset_hours),
    )


class SingleMarketPriceProcessingCoordinator:
    """Process one BTCUSDT or ETHUSDT Binance Futures trade hour."""

    def __init__(
        self,
        *,
        progress_sink: PriceProcessingProgressSink | None = None,
        session_scope_factory: PriceSessionScopeFactory = session_scope,
        raw_root: Path | None = None,
        batch_size: int = 131_072,
        cancellation_check_interval_rows: int = 4_096,
        cancellation_check_interval_records: int = 4_096,
    ) -> None:
        if not callable(session_scope_factory):
            raise TypeError("session_scope_factory must be callable")

        for name, value in (
            ("batch_size", batch_size),
            (
                "cancellation_check_interval_rows",
                cancellation_check_interval_rows,
            ),
            (
                "cancellation_check_interval_records",
                cancellation_check_interval_records,
            ),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ProcessingContractError(f"{name} must be a positive integer")

        self._progress_sink = progress_sink
        self._session_scope_factory = session_scope_factory
        self._raw_root = (
            Path(raw_root).expanduser().resolve() if raw_root is not None else None
        )
        self._batch_size = batch_size
        self._cancellation_check_interval_rows = cancellation_check_interval_rows
        self._cancellation_check_interval_records = cancellation_check_interval_records

    def _emit(
        self,
        request: PriceProcessingRequest,
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
            # UI or progress failures must not alter processing truth.
            log.exception(
                "Price processing progress sink failed for operation %s.",
                request.operation_id,
            )

    def run(
        self,
        request: PriceProcessingRequest,
        *,
        cancellation_probe: ProcessingCancellationProbe | None = None,
    ) -> PriceProcessingResult:
        """Process and persist one trade-time-owned UTC price hour."""
        if not isinstance(request, PriceProcessingRequest):
            raise TypeError("request must be a PriceProcessingRequest")

        self._emit(
            request,
            ProcessingProgressPhase.PLANNING,
            "Planning Binance Futures trade-price processing.",
        )

        raise_if_processing_cancelled(cancellation_probe)

        cancelled: ProcessingCancelledError | None = None
        result: PriceProcessingResult | None = None

        try:
            with self._session_scope_factory() as session:
                try:
                    result = self._run_transaction(
                        session,
                        request,
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
                "Trade-price processing failed.",
            )
            raise

        if cancelled is not None:
            self._emit(
                request,
                ProcessingProgressPhase.CANCELLED,
                "Trade-price processing was cancelled before persistence.",
            )
            raise cancelled

        if result is None:
            raise ProcessingPersistenceError(
                "Price processing transaction completed without a result"
            )

        self._emit(
            request,
            ProcessingProgressPhase.COMPLETED,
            "Trade-price processing completed.",
        )

        return result

    def _run_transaction(
        self,
        session: Session,
        request: PriceProcessingRequest,
        *,
        cancellation_probe: ProcessingCancellationProbe | None,
    ) -> PriceProcessingResult:
        target = request.target

        acquire_source_hour_transaction_lock(
            session,
            target,
        )

        acquisition_repository = AcquisitionRepository(session)
        target_row = acquisition_repository.get_source_hour(target)

        if target_row is None:
            raise ProcessingContractError("Target trade source-hour row does not exist")

        try:
            current_status = SourceHourStatus(target_row.status)
        except (TypeError, ValueError) as exc:
            raise ProcessingContractError(
                "Target trade source has an unsupported durable status"
            ) from exc

        if current_status not in {
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.PROCESSING,
            SourceHourStatus.PROCESSED,
        }:
            raise ProcessingContractError(
                "Target trade source is not available for processing"
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
        sources = self._select_sources(
            request,
            source_repository,
        )

        for archive in sorted(
            sources,
            key=lambda item: item.spec.identity_tuple,
        ):
            acquire_source_hour_transaction_lock(
                session,
                archive.spec,
            )

        locked_sources = self._select_sources(
            request,
            source_repository,
        )

        if locked_sources != sources:
            raise ProcessingContractError(
                "Trade source selection changed while source locks "
                "were being acquired"
            )

        sources = locked_sources

        self._emit(
            request,
            ProcessingProgressPhase.VERIFYING_SOURCES,
            f"Verifying {len(sources)} trade source archive(s).",
            completed_units=0,
            total_units=len(sources),
        )

        for index, archive in enumerate(
            sources,
            start=1,
        ):
            verify_processing_source_archive(
                archive,
                cancellation_probe=cancellation_probe,
            )

            self._emit(
                request,
                ProcessingProgressPhase.VERIFYING_SOURCES,
                f"Verified {archive.spec.remote_path}",
                completed_units=index,
                total_units=len(sources),
            )

        raise_if_processing_cancelled(cancellation_probe)

        self._emit(
            request,
            ProcessingProgressPhase.BUILDING_PRICE,
            "Streaming real Binance Futures trades into one-second OHLC.",
        )

        try:
            streamed = stream_trade_ohlc_hour(
                tuple(
                    (
                        archive.local_path,
                        archive.spec,
                    )
                    for archive in sources
                ),
                target_hour_utc=target.hour_utc,
                batch_size=self._batch_size,
                cancellation_probe=cancellation_probe,
                cancellation_check_interval_rows=(
                    self._cancellation_check_interval_rows
                ),
                cancellation_check_interval_records=(
                    self._cancellation_check_interval_records
                ),
            )
        except (
            TradeOHLCCancelledError,
            StreamedParquetReadCancelled,
        ) as exc:
            raise ProcessingCancelledError(
                "Trade-price processing was cancelled during source streaming"
            ) from exc

        block = streamed.block

        if block.base != target.base:
            raise ProcessingContractError(
                "Constructed price block base does not match target"
            )

        if block.symbol != target.symbol:
            raise ProcessingContractError(
                "Constructed price block symbol does not match target"
            )

        if block.hour_utc != target.hour_utc:
            raise ProcessingContractError(
                "Constructed price block hour does not match target"
            )

        raise_if_processing_cancelled(cancellation_probe)

        self._emit(
            request,
            ProcessingProgressPhase.ENCODING,
            "Encoding compact trade-price channels.",
        )

        encoded = encode_hourly_trade_ohlc_block(block)
        quality_summary_json = trade_ohlc_quality_summary_to_dict(block.quality_summary)

        # This is the final safe cancellation point. No price-hour row or
        # terminal source mutation has yet been written.
        raise_if_processing_cancelled(cancellation_probe)

        provenance = PriceHourlyProvenance(
            source_hours=tuple(
                PriceSourceHourReference(
                    provider=archive.spec.provider,
                    venue=archive.spec.venue,
                    instrument=archive.spec.symbol,
                    hour_utc=archive.spec.hour_utc,
                    content_sha256=archive.content_sha256,
                )
                for archive in sources
            ),
            trade_reader_schema_version=1,
            trade_ohlc_schema_version=1,
        )

        self._emit(
            request,
            ProcessingProgressPhase.PERSISTING,
            "Persisting compact real-trade price hour.",
        )

        try:
            write_result = PriceAnalyticalRepository(session).write_price_hour(
                base=target.base,
                hour_utc=target.hour_utc,
                encoded=encoded,
                quality_summary_json=quality_summary_json,
                provenance=provenance,
            )
        except Exception as exc:
            raise ProcessingPersistenceError(
                "Could not persist compact trade-price hour"
            ) from exc

        # Do not consult cancellation after persistence starts. The remaining
        # source finalization is short and belongs to the same transaction.
        quality_state = _price_quality_state(
            valid_count=block.valid_count,
            invalid_count=block.invalid_count,
        )

        self._emit(
            request,
            ProcessingProgressPhase.FINALIZING,
            "Finalizing target trade source metadata.",
        )

        target_report = self._target_reader_report(
            sources,
            streamed.reader_reports,
            target,
        )

        target_row.row_count = target_report.rows_read
        target_row.event_count = None
        target_row.snapshot_count = None
        target_row.continuity_mismatch_count = None
        target_row.quality_state = quality_state.value
        target_row.quality_json = {
            "schema": "l2shock.processed_trade_source_hour_quality",
            "schema_version": 1,
            "quality_state": quality_state.value,
            "price_quality_summary": quality_summary_json,
            "source_selection_policy": (
                "current_plus_available_adjacent_v1"
                if request.include_adjacent_sources
                else "current_source_only_v1"
            ),
            "source_archive_count": len(sources),
            "input_trade_count": block.input_trade_count,
            "accepted_trade_count": block.accepted_trade_count,
            "exact_duplicate_trade_count": (block.exact_duplicate_trade_count),
            "outside_target_hour_trade_count": (block.outside_target_hour_trade_count),
            "price_content_sha256": encoded.content_sha256,
            "reader_received_time_regression_count": (
                target_report.received_time_regression_count
            ),
            "reader_trade_time_regression_count": (
                target_report.trade_time_regression_count
            ),
            "reader_adjacent_duplicate_trade_id_count": (
                target_report.adjacent_duplicate_trade_id_count
            ),
        }

        if current_status is not SourceHourStatus.PROCESSED:
            validate_source_hour_transition(
                SourceHourStatus.PROCESSING,
                SourceHourStatus.PROCESSED,
            )
            target_row.status = SourceHourStatus.PROCESSED.value

        target_row.processed_at = now_utc()
        target_row.error_text = None
        session.flush()

        return PriceProcessingResult(
            operation_id=request.operation_id,
            target=target,
            quality_state=quality_state,
            valid_count=block.valid_count,
            invalid_count=block.invalid_count,
            total_trade_count=(block.quality_summary.total_trade_count),
            source_archive_count=len(sources),
            price_inserted=write_result.inserted,
            price_content_sha256=encoded.content_sha256,
            completed_at=now_utc(),
        )

    @staticmethod
    def _select_sources(
        request: PriceProcessingRequest,
        repository: SQLAlchemyProcessingSourceRepository,
    ) -> tuple[ProcessingSourceArchive, ...]:
        target = request.target

        current = repository.find_replayable(target)

        if current is None:
            raise ProcessingContractError(
                "Target trade source archive is not locally replayable"
            )

        selected = [current]

        if request.include_adjacent_sources:
            for offset in (-1, 1):
                candidate = repository.find_replayable(
                    _adjacent_trade_spec(
                        target,
                        offset_hours=offset,
                    )
                )

                if candidate is not None:
                    selected.append(candidate)

        selected.sort(key=lambda archive: archive.spec.hour_utc)

        return tuple(selected)

    @staticmethod
    def _target_reader_report(
        sources: tuple[ProcessingSourceArchive, ...],
        reports: tuple[TradeReadReport, ...],
        target: SourceFileSpec,
    ) -> TradeReadReport:
        if len(sources) != len(reports):
            raise ProcessingContractError(
                "Trade source and reader-report counts disagree"
            )

        for archive, report in zip(
            sources,
            reports,
            strict=True,
        ):
            if archive.spec.identity_tuple == target.identity_tuple:
                return report

        raise ProcessingContractError(
            "Trade processing result lacks the target reader report"
        )

    @staticmethod
    def _reset_cancelled_source(
        session: Session,
        request: PriceProcessingRequest,
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


def create_production_price_processing_coordinator(
    *,
    progress_sink: PriceProcessingProgressSink | None = None,
) -> SingleMarketPriceProcessingCoordinator:
    """Build the synchronous production price coordinator."""
    from l2shock.config import get_settings

    settings = get_settings()

    return SingleMarketPriceProcessingCoordinator(
        progress_sink=progress_sink,
        raw_root=settings.storage.raw_path,
    )


__all__ = [
    "PriceProcessingProgressSink",
    "PriceSessionScopeFactory",
    "SingleMarketPriceProcessingCoordinator",
    "create_production_price_processing_coordinator",
]

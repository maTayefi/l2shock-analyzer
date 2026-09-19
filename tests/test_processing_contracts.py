from __future__ import annotations

import inspect
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.persistence import (
    InvalidSourceHourTransitionError,
    SourceHourStatus,
    validate_source_hour_transition,
)
from l2shock.config import ProcessingConfig
from l2shock.processing import (
    PriceProcessingRequest,
    PriceProcessingResult,
    ProcessingContractError,
    ProcessingProgress,
    ProcessingProgressPhase,
    ProcessingQualityState,
    ProcessingRequest,
    ProcessingResult,
    ProcessingSourceArchive,
    SingleMarketL2ProcessingCoordinator,
    SingleMarketPriceProcessingCoordinator,
)


def _spec(
    data_kind: SourceDataKind = SourceDataKind.ORDERBOOK,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=data_kind,
        hour_utc=datetime(
            2026,
            9,
            2,
            12,
            tzinfo=timezone.utc,
        ),
    )


def test_processing_request_accepts_orderbook_target() -> None:
    operation_id = uuid4()

    request = ProcessingRequest(
        operation_id=operation_id,
        target=_spec(),
        max_checkpoint_search_hours=168,
    )

    assert request.operation_id == operation_id
    assert request.max_checkpoint_search_hours == 168


def test_processing_request_rejects_trade_target() -> None:
    with pytest.raises(
        ProcessingContractError,
        match="orderbook",
    ):
        ProcessingRequest(
            operation_id=uuid4(),
            target=_spec(SourceDataKind.TRADES),
            max_checkpoint_search_hours=168,
        )


@pytest.mark.parametrize(
    "value",
    [True, False, 0, -1, "168"],
)
def test_processing_request_requires_positive_integer_bound(
    value: object,
) -> None:
    with pytest.raises(
        ProcessingContractError,
        match="positive integer",
    ):
        ProcessingRequest(
            operation_id=uuid4(),
            target=_spec(),
            max_checkpoint_search_hours=value,  # type: ignore[arg-type]
        )


def test_processing_progress_is_immutable_and_bounded() -> None:
    progress = ProcessingProgress(
        operation_id=uuid4(),
        target=_spec(),
        phase=ProcessingProgressPhase.DISCOVERING_CHECKPOINT,
        message="Inspecting predecessor hour.",
        completed_units=1,
        total_units=168,
    )

    assert progress.phase is ProcessingProgressPhase.DISCOVERING_CHECKPOINT
    assert progress.completed_units == 1
    assert progress.total_units == 168


def test_processing_progress_rejects_completed_above_total() -> None:
    with pytest.raises(
        ProcessingContractError,
        match="cannot exceed",
    ):
        ProcessingProgress(
            operation_id=uuid4(),
            target=_spec(),
            phase=ProcessingProgressPhase.PLANNING,
            message="Planning.",
            completed_units=2,
            total_units=1,
        )


def test_processing_source_archive_requires_replayable_status(
    tmp_path: Path,
) -> None:
    from l2shock.processing import SourceArchiveMetadataError

    path = tmp_path / "source.parquet"
    path.write_bytes(b"source")

    with pytest.raises(
        SourceArchiveMetadataError,
        match="downloaded, processing, or processed",
    ):
        ProcessingSourceArchive(
            spec=_spec(),
            local_path=path,
            content_sha256="a" * 64,
            file_size_bytes=path.stat().st_size,
            status=SourceHourStatus.DISCOVERED,
        )


def test_processing_config_defaults_to_one_week() -> None:
    config = ProcessingConfig()

    assert config.checkpoint_search_max_hours == 168


@pytest.mark.parametrize(
    "value",
    [True, False, 0, -1, 8761],
)
def test_processing_config_rejects_unsafe_search_bound(
    value: object,
) -> None:
    with pytest.raises(ValueError):
        ProcessingConfig(
            checkpoint_search_max_hours=value,  # type: ignore[arg-type]
        )


def test_processing_cancellation_reset_requires_explicit_permission() -> None:
    with pytest.raises(
        InvalidSourceHourTransitionError,
        match="cooperative cancellation",
    ):
        validate_source_hour_transition(
            SourceHourStatus.PROCESSING,
            SourceHourStatus.DOWNLOADED,
        )


def test_processing_cancellation_reset_is_allowed_when_explicitly_safe() -> None:
    current, target = validate_source_hour_transition(
        SourceHourStatus.PROCESSING,
        SourceHourStatus.DOWNLOADED,
        allow_processing_cancellation_reset=True,
    )

    assert current is SourceHourStatus.PROCESSING
    assert target is SourceHourStatus.DOWNLOADED


def test_processing_cancellation_probe_must_return_bool() -> None:
    from l2shock.processing import raise_if_processing_cancelled

    with pytest.raises(
        ProcessingContractError,
        match="must return bool",
    ):
        raise_if_processing_cancelled(
            lambda: 1,  # type: ignore[return-value]
        )


def test_processing_cancellation_probe_failure_is_wrapped() -> None:
    from l2shock.processing import raise_if_processing_cancelled

    def failing_probe() -> bool:
        raise RuntimeError("implementation detail")

    with pytest.raises(
        ProcessingContractError,
        match="probe failed",
    ):
        raise_if_processing_cancelled(failing_probe)


def test_processing_result_requires_3600_quality_slots() -> None:
    with pytest.raises(
        ProcessingContractError,
        match="total 3,600",
    ):
        ProcessingResult(
            operation_id=uuid4(),
            target=_spec(),
            preset_hash="a" * 64,
            quality_state=ProcessingQualityState.DEGRADED,
            valid_count=100,
            degraded_count=0,
            invalid_count=100,
            replay_source_count=1,
            analytical_inserted=True,
            preset_inserted=True,
            analytical_content_sha256="b" * 64,
            input_checkpoint_content_sha256=None,
            output_checkpoint_content_sha256=None,
            completed_at=datetime(
                2026,
                9,
                2,
                13,
                tzinfo=timezone.utc,
            ),
        )


def test_processing_result_accepts_all_invalid_hour() -> None:
    result = ProcessingResult(
        operation_id=uuid4(),
        target=_spec(),
        preset_hash="a" * 64,
        quality_state=ProcessingQualityState.INVALID,
        valid_count=0,
        degraded_count=0,
        invalid_count=3_600,
        replay_source_count=1,
        analytical_inserted=False,
        preset_inserted=False,
        analytical_content_sha256="b" * 64,
        input_checkpoint_content_sha256=None,
        output_checkpoint_content_sha256=None,
        completed_at=datetime(
            2026,
            9,
            2,
            13,
            tzinfo=timezone.utc,
        ),
    )

    assert result.invalid_count == 3_600
    assert result.quality_state is ProcessingQualityState.INVALID


def test_price_processing_request_requires_trade_target() -> None:
    request = PriceProcessingRequest(
        operation_id=uuid4(),
        target=_spec(SourceDataKind.TRADES),
    )

    assert request.target.data_kind is SourceDataKind.TRADES
    assert request.include_adjacent_sources is False

    with pytest.raises(
        ProcessingContractError,
        match="trades source",
    ):
        PriceProcessingRequest(
            operation_id=uuid4(),
            target=_spec(SourceDataKind.ORDERBOOK),
        )


def test_price_processing_request_requires_boolean_adjacent_policy() -> None:
    with pytest.raises(
        ProcessingContractError,
        match="must be bool",
    ):
        PriceProcessingRequest(
            operation_id=uuid4(),
            target=_spec(SourceDataKind.TRADES),
            include_adjacent_sources=1,  # type: ignore[arg-type]
        )


def test_price_processing_result_requires_consistent_quality() -> None:
    with pytest.raises(
        ProcessingContractError,
        match="does not match",
    ):
        PriceProcessingResult(
            operation_id=uuid4(),
            target=_spec(SourceDataKind.TRADES),
            quality_state=ProcessingQualityState.VALID,
            valid_count=1,
            invalid_count=3_599,
            total_trade_count=1,
            source_archive_count=1,
            price_inserted=True,
            price_content_sha256="a" * 64,
            completed_at=datetime(
                2026,
                9,
                2,
                13,
                tzinfo=timezone.utc,
            ),
        )


def test_price_processing_result_accepts_degraded_hour() -> None:
    result = PriceProcessingResult(
        operation_id=uuid4(),
        target=_spec(SourceDataKind.TRADES),
        quality_state=ProcessingQualityState.DEGRADED,
        valid_count=1,
        invalid_count=3_599,
        total_trade_count=2,
        source_archive_count=1,
        price_inserted=True,
        price_content_sha256="a" * 64,
        completed_at=datetime(
            2026,
            9,
            2,
            13,
            tzinfo=timezone.utc,
        ),
    )

    assert result.quality_state is ProcessingQualityState.DEGRADED
    assert result.valid_count == 1
    assert result.invalid_count == 3_599


def test_processing_result_requires_consistent_quality_state() -> None:
    with pytest.raises(
        ProcessingContractError,
        match="does not match",
    ):
        ProcessingResult(
            operation_id=uuid4(),
            target=_spec(),
            preset_hash="a" * 64,
            quality_state=ProcessingQualityState.VALID,
            valid_count=1,
            degraded_count=0,
            invalid_count=3_599,
            replay_source_count=1,
            analytical_inserted=True,
            preset_inserted=True,
            analytical_content_sha256="b" * 64,
            input_checkpoint_content_sha256=None,
            output_checkpoint_content_sha256=None,
            completed_at=datetime(
                2026,
                9,
                2,
                13,
                tzinfo=timezone.utc,
            ),
        )


def test_production_processing_coordinator_batch_defaults() -> None:
    l2_parameters = inspect.signature(
        SingleMarketL2ProcessingCoordinator.__init__,
    ).parameters
    price_parameters = inspect.signature(
        SingleMarketPriceProcessingCoordinator.__init__,
    ).parameters

    assert l2_parameters["batch_size"].default == 131_072
    assert price_parameters["batch_size"].default == 131_072


def test_price_processing_request_rejects_non_binance_trades() -> None:
    from uuid import uuid4

    from l2shock.acquisition import SourceDataKind, SourceFileSpec
    from l2shock.processing import (
        PriceProcessingRequest,
        ProcessingContractError,
    )

    target = SourceFileSpec(
        provider="cryptohftdata",
        venue="bybit",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=datetime(
            2026,
            9,
            2,
            12,
            tzinfo=timezone.utc,
        ),
    )

    with pytest.raises(
        ProcessingContractError,
        match="Binance Futures",
    ):
        PriceProcessingRequest(
            operation_id=uuid4(),
            target=target,
        )

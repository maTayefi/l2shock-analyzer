# l2shock/liquidity/hourly.py
"""One-second depth-liquidity emission over streamed replay state.

This module connects deterministic order-book replay to the approved
depth-band calculation.

Locked semantics:

- received_time_ns is the sampling clock;
- every completed UTC source hour has exactly 3,600 slots;
- an event exactly at a bucket endpoint is included in that bucket;
- all events sharing one received_time_ns are applied before that timestamp's
  bucket can be emitted;
- quiet seconds carry the current replay state;
- only sequence-valid NORMAL replay state produces a VALID liquidity sample;
- invalid samples contain no fabricated liquidity values;
- a zero depth-band denominator leaves imbalance null without invalidating an
  otherwise valid reconstructed-book sample;
- user price filters do not affect liquidity arithmetic.

The mutable replay state passed to callback methods is borrowed synchronously.
It is never retained by this sampler.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Final

from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.ingest.parquet_reader import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS,
    OrderBookEvent,
)
from l2shock.ingest.replay import (
    BookInitializationState,
    OrderBookCheckpoint,
    OrderBookReplayState,
    ReplayBookStructure,
    ReplayCancellationProbe,
    ReplayChainReport,
    ReplaySource,
    replay_orderbook_archives,
)
from l2shock.ingest.sampling import (
    NANOSECONDS_PER_SECOND,
    OBSERVATIONS_PER_HOUR,
    BookSampleInvalidReason,
    BookSampleQuality,
    classify_book_observation,
)
from l2shock.liquidity.depth import (
    DEFAULT_CANCELLATION_CHECK_INTERVAL_LEVELS,
    DEFAULT_IMBALANCE_DECIMAL_PRECISION,
    DepthBand,
    DepthCancellationProbe,
    DepthLiquidityResult,
    _exact_add,
    calculate_depth_liquidity,
)
from l2shock.timeutils import require_utc_hour

_EPOCH_UTC: Final[datetime] = datetime(
    1970,
    1,
    1,
    tzinfo=timezone.utc,
)


class HourlyLiquidityError(ValueError):
    """Replay callbacks violated the hourly-liquidity contract."""


@dataclass(frozen=True, slots=True)
class LiquidityObservation:
    """One immutable analytical observation at a one-second bucket endpoint."""

    bucket_index: int
    bucket_start_utc: datetime
    bucket_end_utc: datetime
    source_received_time_ns: int | None

    quality: BookSampleQuality
    invalid_reason: BookSampleInvalidReason | None

    replay_valid: bool
    initialization_state: BookInitializationState
    book_structure: ReplayBookStructure
    last_update_id: int | None

    best_bid: Decimal | None
    best_ask: Decimal | None

    bid_liquidity: Decimal | None
    ask_liquidity: Decimal | None
    total_liquidity: Decimal | None
    bid_ask_imbalance: Decimal | None

    bid_depth_level_count: int
    ask_depth_level_count: int
    levels_examined: int
    source_count: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.bucket_index, bool)
            or not isinstance(self.bucket_index, int)
            or not 0 <= self.bucket_index < OBSERVATIONS_PER_HOUR
        ):
            raise HourlyLiquidityError("bucket_index must be inside [0, 3599]")

        for name in ("bucket_start_utc", "bucket_end_utc"):
            value = getattr(self, name)

            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() != timedelta(0)
            ):
                raise HourlyLiquidityError(f"{name} must be timezone-aware UTC")

        if self.bucket_end_utc - self.bucket_start_utc != timedelta(seconds=1):
            raise HourlyLiquidityError("Liquidity bucket must span exactly one second")

        if self.source_received_time_ns is not None:
            if (
                isinstance(self.source_received_time_ns, bool)
                or not isinstance(self.source_received_time_ns, int)
                or self.source_received_time_ns < 0
            ):
                raise HourlyLiquidityError(
                    "source_received_time_ns must be non-negative or null"
                )

        object.__setattr__(
            self,
            "quality",
            BookSampleQuality(self.quality),
        )
        object.__setattr__(
            self,
            "initialization_state",
            BookInitializationState(self.initialization_state),
        )
        object.__setattr__(
            self,
            "book_structure",
            ReplayBookStructure(self.book_structure),
        )

        if self.invalid_reason is not None:
            object.__setattr__(
                self,
                "invalid_reason",
                BookSampleInvalidReason(self.invalid_reason),
            )

        for name in (
            "bid_depth_level_count",
            "ask_depth_level_count",
            "levels_examined",
            "source_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise HourlyLiquidityError(f"{name} must be a non-negative integer")

        if self.levels_examined != (
            self.bid_depth_level_count + self.ask_depth_level_count
        ):
            raise HourlyLiquidityError("levels_examined does not match side counts")

        liquidity_values = (
            self.bid_liquidity,
            self.ask_liquidity,
            self.total_liquidity,
        )

        if self.quality is BookSampleQuality.VALID:
            if self.invalid_reason is not None:
                raise HourlyLiquidityError(
                    "A VALID liquidity observation cannot have invalid_reason"
                )

            if not self.replay_valid:
                raise HourlyLiquidityError(
                    "A VALID liquidity observation requires valid replay"
                )

            if self.book_structure is not ReplayBookStructure.NORMAL:
                raise HourlyLiquidityError(
                    "A VALID liquidity observation requires NORMAL structure"
                )

            if self.best_bid is None or self.best_ask is None:
                raise HourlyLiquidityError(
                    "A VALID liquidity observation requires best prices"
                )

            if self.best_bid >= self.best_ask:
                raise HourlyLiquidityError(
                    "A VALID liquidity observation requires positive spread"
                )

            if any(
                not isinstance(value, Decimal) or not value.is_finite() or value < 0
                for value in liquidity_values
            ):
                raise HourlyLiquidityError(
                    "VALID liquidity values must be finite non-negative Decimals"
                )

            assert self.bid_liquidity is not None
            assert self.ask_liquidity is not None
            assert self.total_liquidity is not None

            if self.total_liquidity != _exact_add(
                self.bid_liquidity,
                self.ask_liquidity,
            ):
                raise HourlyLiquidityError(
                    "Total Liquidity must equal Bid plus Ask Liquidity"
                )

            if self.total_liquidity == 0:
                if self.bid_ask_imbalance is not None:
                    raise HourlyLiquidityError(
                        "Zero total liquidity requires null imbalance"
                    )
            else:
                imbalance = self.bid_ask_imbalance

                if (
                    not isinstance(imbalance, Decimal)
                    or not imbalance.is_finite()
                    or imbalance < Decimal("-1")
                    or imbalance > Decimal("1")
                ):
                    raise HourlyLiquidityError(
                        "Nonzero total requires finite imbalance in [-1, 1]"
                    )

            if self.source_count != 1:
                raise HourlyLiquidityError(
                    "A valid single-market observation requires source_count=1"
                )

        else:
            if self.invalid_reason is None:
                raise HourlyLiquidityError(
                    "An invalid/degraded observation requires invalid_reason"
                )

            if any(value is not None for value in liquidity_values):
                raise HourlyLiquidityError(
                    "Invalid observations cannot contain liquidity values"
                )

            if self.bid_ask_imbalance is not None:
                raise HourlyLiquidityError(
                    "Invalid observations cannot contain imbalance"
                )

            if self.bid_depth_level_count != 0:
                raise HourlyLiquidityError(
                    "Invalid observations cannot report scanned bid levels"
                )

            if self.ask_depth_level_count != 0:
                raise HourlyLiquidityError(
                    "Invalid observations cannot report scanned ask levels"
                )

            if self.levels_examined != 0:
                raise HourlyLiquidityError(
                    "Invalid observations cannot report examined levels"
                )

            if self.source_count != 0:
                raise HourlyLiquidityError(
                    "Invalid observations require source_count=0"
                )

    @property
    def valid(self) -> bool:
        return self.quality is BookSampleQuality.VALID


@dataclass(frozen=True, slots=True)
class LiquidityQualitySummary:
    """Deterministic quality counts for one hourly liquidity block."""

    valid_count: int
    degraded_count: int
    invalid_count: int
    invalid_reason_counts: tuple[tuple[BookSampleInvalidReason, int], ...]
    zero_total_liquidity_count: int

    def __post_init__(self) -> None:
        for name in (
            "valid_count",
            "degraded_count",
            "invalid_count",
            "zero_total_liquidity_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise HourlyLiquidityError(f"{name} must be a non-negative integer")

        if (
            self.valid_count + self.degraded_count + self.invalid_count
            != OBSERVATIONS_PER_HOUR
        ):
            raise HourlyLiquidityError("Quality counts must total 3,600")

        normalized: list[tuple[BookSampleInvalidReason, int]] = []
        seen: set[BookSampleInvalidReason] = set()

        for raw_reason, count in self.invalid_reason_counts:
            reason = BookSampleInvalidReason(raw_reason)

            if reason in seen:
                raise HourlyLiquidityError(
                    "invalid_reason_counts contains duplicate reasons"
                )

            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise HourlyLiquidityError("Invalid-reason count must be non-negative")

            seen.add(reason)
            normalized.append((reason, count))

        expected_order = [
            reason for reason in BookSampleInvalidReason if reason in seen
        ]

        if [reason for reason, _count in normalized] != expected_order:
            raise HourlyLiquidityError(
                "invalid_reason_counts must use enum declaration order"
            )

        reason_total = sum(count for _reason, count in normalized)

        if reason_total != self.invalid_count + self.degraded_count:
            raise HourlyLiquidityError(
                "Invalid-reason counts do not match non-valid observations"
            )

        if self.zero_total_liquidity_count > self.valid_count:
            raise HourlyLiquidityError(
                "zero_total_liquidity_count cannot exceed valid_count"
            )

        object.__setattr__(
            self,
            "invalid_reason_counts",
            tuple(normalized),
        )

    @property
    def invalid_reason_map(
        self,
    ) -> dict[BookSampleInvalidReason, int]:
        return dict(self.invalid_reason_counts)


@dataclass(frozen=True, slots=True)
class HourlyLiquidityBlock:
    """Exactly 3,600 single-market liquidity observations."""

    spec: SourceFileSpec
    band: DepthBand
    imbalance_decimal_precision: int
    observations: tuple[LiquidityObservation, ...]
    quality_summary: LiquidityQualitySummary

    def __post_init__(self) -> None:
        if self.spec.data_kind is not SourceDataKind.ORDERBOOK:
            raise HourlyLiquidityError(
                "HourlyLiquidityBlock requires an orderbook source"
            )

        if not isinstance(self.band, DepthBand):
            raise HourlyLiquidityError("HourlyLiquidityBlock.band must be DepthBand")

        if (
            isinstance(self.imbalance_decimal_precision, bool)
            or not isinstance(self.imbalance_decimal_precision, int)
            or self.imbalance_decimal_precision <= 0
        ):
            raise HourlyLiquidityError("imbalance_decimal_precision must be positive")

        if len(self.observations) != OBSERVATIONS_PER_HOUR:
            raise HourlyLiquidityError(
                "HourlyLiquidityBlock must contain exactly 3,600 observations"
            )

        for expected_index, observation in enumerate(self.observations):
            if observation.bucket_index != expected_index:
                raise HourlyLiquidityError(
                    "Liquidity observations are not in bucket order"
                )

            expected_start = self.spec.hour_utc + timedelta(seconds=expected_index)
            expected_end = expected_start + timedelta(seconds=1)

            if observation.bucket_start_utc != expected_start:
                raise HourlyLiquidityError(
                    "Liquidity bucket start violates source-hour ownership"
                )

            if observation.bucket_end_utc != expected_end:
                raise HourlyLiquidityError(
                    "Liquidity bucket end violates source-hour ownership"
                )

        calculated_summary = build_liquidity_quality_summary(self.observations)

        if calculated_summary != self.quality_summary:
            raise HourlyLiquidityError(
                "Hourly quality summary does not match observations"
            )

    @property
    def valid_count(self) -> int:
        return self.quality_summary.valid_count

    @property
    def invalid_count(self) -> int:
        return self.quality_summary.invalid_count

    @property
    def fully_valid(self) -> bool:
        return self.valid_count == OBSERVATIONS_PER_HOUR


@dataclass(frozen=True, slots=True)
class SampledLiquidityChain:
    """Replay report and liquidity blocks produced by one streamed execution."""

    replay_report: ReplayChainReport
    hours: tuple[HourlyLiquidityBlock, ...]

    def __post_init__(self) -> None:
        if len(self.hours) != len(self.replay_report.archives):
            raise HourlyLiquidityError(
                "Liquidity hour count must match replay archive count"
            )

        for hour, archive in zip(
            self.hours,
            self.replay_report.archives,
        ):
            if hour.spec.identity_tuple != archive.spec.identity_tuple:
                raise HourlyLiquidityError(
                    "Liquidity-hour identity does not match replay archive"
                )

    @property
    def total_observation_count(self) -> int:
        return sum(len(hour.observations) for hour in self.hours)

    @property
    def total_valid_count(self) -> int:
        return sum(hour.valid_count for hour in self.hours)

    @property
    def total_invalid_count(self) -> int:
        return sum(hour.invalid_count for hour in self.hours)


def _datetime_to_epoch_ns(value: datetime) -> int:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise HourlyLiquidityError("Epoch conversion requires timezone-aware UTC")

    delta = value - _EPOCH_UTC

    return (
        delta.days * 86_400 * NANOSECONDS_PER_SECOND
        + delta.seconds * NANOSECONDS_PER_SECOND
        + delta.microseconds * 1_000
    )


def build_liquidity_quality_summary(
    observations: tuple[LiquidityObservation, ...],
) -> LiquidityQualitySummary:
    """Build deterministic counts from one complete hourly observation tuple."""
    if len(observations) != OBSERVATIONS_PER_HOUR:
        raise HourlyLiquidityError(
            "Quality summary requires exactly 3,600 observations"
        )

    valid_count = 0
    degraded_count = 0
    invalid_count = 0
    zero_total_count = 0

    reason_counts = {reason: 0 for reason in BookSampleInvalidReason}

    for observation in observations:
        if observation.quality is BookSampleQuality.VALID:
            valid_count += 1

            if observation.total_liquidity == 0:
                zero_total_count += 1

        elif observation.quality is BookSampleQuality.DEGRADED:
            degraded_count += 1

            if observation.invalid_reason is None:
                raise HourlyLiquidityError("DEGRADED observation lacks a reason")

            reason_counts[observation.invalid_reason] += 1

        else:
            invalid_count += 1

            if observation.invalid_reason is None:
                raise HourlyLiquidityError("INVALID observation lacks a reason")

            reason_counts[observation.invalid_reason] += 1

    return LiquidityQualitySummary(
        valid_count=valid_count,
        degraded_count=degraded_count,
        invalid_count=invalid_count,
        invalid_reason_counts=tuple(
            (reason, reason_counts[reason])
            for reason in BookSampleInvalidReason
            if reason_counts[reason] > 0
        ),
        zero_total_liquidity_count=zero_total_count,
    )


class OneSecondLiquiditySampler:
    """Emit one-second liquidity from borrowed replay-state callbacks."""

    def __init__(
        self,
        band: DepthBand,
        *,
        cancellation_probe: DepthCancellationProbe | None = None,
        cancellation_check_interval_levels: int = (
            DEFAULT_CANCELLATION_CHECK_INTERVAL_LEVELS
        ),
        imbalance_decimal_precision: int = (DEFAULT_IMBALANCE_DECIMAL_PRECISION),
    ) -> None:
        if not isinstance(band, DepthBand):
            raise TypeError("band must be a DepthBand")

        if (
            isinstance(cancellation_check_interval_levels, bool)
            or not isinstance(cancellation_check_interval_levels, int)
            or cancellation_check_interval_levels <= 0
        ):
            raise HourlyLiquidityError(
                "cancellation_check_interval_levels must be positive"
            )

        if (
            isinstance(imbalance_decimal_precision, bool)
            or not isinstance(imbalance_decimal_precision, int)
            or imbalance_decimal_precision <= 0
        ):
            raise HourlyLiquidityError("imbalance_decimal_precision must be positive")

        self._band = band
        self._cancellation_probe = cancellation_probe
        self._cancellation_check_interval_levels = cancellation_check_interval_levels
        self._imbalance_decimal_precision = imbalance_decimal_precision

        self._active_spec: SourceFileSpec | None = None
        self._next_bucket_index = 0
        self._last_event_received_time_ns: int | None = None
        self._active_observations: list[LiquidityObservation] = []
        self._completed_hours: list[HourlyLiquidityBlock] = []
        self._finished = False

    @property
    def completed_hours(self) -> tuple[HourlyLiquidityBlock, ...]:
        return tuple(self._completed_hours)

    def start_archive(
        self,
        spec: SourceFileSpec,
        state: OrderBookReplayState,
    ) -> None:
        """Begin one source hour from its pre-first-event replay state."""
        if self._finished:
            raise HourlyLiquidityError(
                "Cannot start an archive after sampler finalization"
            )

        if self._active_spec is not None:
            raise HourlyLiquidityError("Previous liquidity hour was not finalized")

        if spec.data_kind is not SourceDataKind.ORDERBOOK:
            raise HourlyLiquidityError("Liquidity sampling requires orderbook sources")

        require_utc_hour("spec.hour_utc", spec.hour_utc)
        self._validate_state_identity(spec, state)

        if self._completed_hours:
            previous = self._completed_hours[-1].spec
            expected_hour = previous.hour_utc + timedelta(hours=1)

            if spec.hour_utc != expected_hour:
                raise HourlyLiquidityError("Liquidity source hours must be adjacent")

            if (
                spec.provider != previous.provider
                or spec.venue != previous.venue
                or spec.symbol != previous.symbol
            ):
                raise HourlyLiquidityError(
                    "Liquidity source identity changed between hours"
                )

        self._active_spec = spec
        self._next_bucket_index = 0
        self._last_event_received_time_ns = None
        self._active_observations = []

    def before_event(
        self,
        spec: SourceFileSpec,
        event: OrderBookEvent,
        state: OrderBookReplayState,
    ) -> None:
        """Emit buckets ending strictly before the current event.

        The current event has not yet been applied. Therefore the borrowed state
        is exactly the last reconstructed state available before that event.
        """
        self._require_active_spec(spec)
        self._validate_state_identity(spec, state)

        received_time_ns = event.received_time_ns
        hour_start_ns = _datetime_to_epoch_ns(spec.hour_utc)
        hour_end_ns = hour_start_ns + OBSERVATIONS_PER_HOUR * NANOSECONDS_PER_SECOND

        if not hour_start_ns <= received_time_ns < hour_end_ns:
            raise HourlyLiquidityError(
                "Event received_time_ns lies outside its source UTC hour"
            )

        previous_received_time_ns = self._last_event_received_time_ns

        if (
            previous_received_time_ns is not None
            and received_time_ns < previous_received_time_ns
        ):
            raise HourlyLiquidityError(
                "received_time_ns regressed during liquidity sampling"
            )

        self._last_event_received_time_ns = received_time_ns

        while (
            self._next_bucket_index < OBSERVATIONS_PER_HOUR
            and self._bucket_end_ns(self._next_bucket_index) < received_time_ns
        ):
            self._append_from_state(state)

    def finish_archive(
        self,
        spec: SourceFileSpec,
        state: OrderBookReplayState,
    ) -> None:
        """Emit all remaining buckets from the final replay state."""
        self._require_active_spec(spec)
        self._validate_state_identity(spec, state)

        while self._next_bucket_index < OBSERVATIONS_PER_HOUR:
            self._append_from_state(state)

        observations = tuple(self._active_observations)
        summary = build_liquidity_quality_summary(observations)

        self._completed_hours.append(
            HourlyLiquidityBlock(
                spec=spec,
                band=self._band,
                imbalance_decimal_precision=(self._imbalance_decimal_precision),
                observations=observations,
                quality_summary=summary,
            )
        )

        self._active_spec = None
        self._next_bucket_index = 0
        self._active_observations = []

    def finish(self) -> tuple[HourlyLiquidityBlock, ...]:
        if self._active_spec is not None:
            raise HourlyLiquidityError(
                "Cannot finalize while a source hour remains active"
            )

        self._finished = True
        return tuple(self._completed_hours)

    def _require_active_spec(
        self,
        spec: SourceFileSpec,
    ) -> None:
        active = self._active_spec

        if active is None:
            raise HourlyLiquidityError("No liquidity source hour is active")

        if spec.identity_tuple != active.identity_tuple:
            raise HourlyLiquidityError(
                "Replay callback source does not match active liquidity hour"
            )

    @staticmethod
    def _validate_state_identity(
        spec: SourceFileSpec,
        state: OrderBookReplayState,
    ) -> None:
        if not isinstance(state, OrderBookReplayState):
            raise TypeError("state must be an OrderBookReplayState")

        if (
            state.provider != spec.provider
            or state.venue != spec.venue
            or state.symbol != spec.symbol
        ):
            raise HourlyLiquidityError(
                "Replay-state identity does not match liquidity source"
            )

    def _bucket_end_ns(self, bucket_index: int) -> int:
        active = self._active_spec

        if active is None:
            raise HourlyLiquidityError("No liquidity source hour is active")

        return (
            _datetime_to_epoch_ns(active.hour_utc)
            + (bucket_index + 1) * NANOSECONDS_PER_SECOND
        )

    def _append_from_state(
        self,
        state: OrderBookReplayState,
    ) -> None:
        active = self._active_spec

        if active is None:
            raise HourlyLiquidityError("Cannot emit liquidity without an active hour")

        observation = state.observation()
        quality, invalid_reason = classify_book_observation(observation)

        depth: DepthLiquidityResult | None = None

        if quality is BookSampleQuality.VALID:
            depth = calculate_depth_liquidity(
                state,
                self._band,
                cancellation_probe=self._cancellation_probe,
                cancellation_check_interval_levels=(
                    self._cancellation_check_interval_levels
                ),
                imbalance_decimal_precision=(self._imbalance_decimal_precision),
            )

        bucket_index = self._next_bucket_index
        bucket_start = active.hour_utc + timedelta(seconds=bucket_index)
        bucket_end = bucket_start + timedelta(seconds=1)

        self._active_observations.append(
            LiquidityObservation(
                bucket_index=bucket_index,
                bucket_start_utc=bucket_start,
                bucket_end_utc=bucket_end,
                source_received_time_ns=(observation.last_event_received_time_ns),
                quality=quality,
                invalid_reason=invalid_reason,
                replay_valid=observation.valid,
                initialization_state=(observation.initialization_state),
                book_structure=observation.book_structure,
                last_update_id=observation.last_update_id,
                best_bid=observation.best_bid,
                best_ask=observation.best_ask,
                bid_liquidity=(depth.bid_liquidity if depth is not None else None),
                ask_liquidity=(depth.ask_liquidity if depth is not None else None),
                total_liquidity=(depth.total_liquidity if depth is not None else None),
                bid_ask_imbalance=(
                    depth.bid_ask_imbalance if depth is not None else None
                ),
                bid_depth_level_count=(
                    depth.bid_level_count if depth is not None else 0
                ),
                ask_depth_level_count=(
                    depth.ask_level_count if depth is not None else 0
                ),
                levels_examined=(depth.levels_examined if depth is not None else 0),
                source_count=1 if depth is not None else 0,
            )
        )

        self._next_bucket_index += 1


def sample_liquidity_archives(
    sources: tuple[ReplaySource, ...],
    band: DepthBand,
    *,
    initial_checkpoint: OrderBookCheckpoint | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    final_source_content_sha256: str | None = None,
    cancellation_probe: ReplayCancellationProbe | None = None,
    cancellation_check_interval_rows: int = (DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS),
    cancellation_check_interval_levels: int = (
        DEFAULT_CANCELLATION_CHECK_INTERVAL_LEVELS
    ),
    imbalance_decimal_precision: int = (DEFAULT_IMBALANCE_DECIMAL_PRECISION),
) -> SampledLiquidityChain:
    """Replay and emit exact one-second liquidity in one streamed pass."""
    sampler = OneSecondLiquiditySampler(
        band,
        cancellation_probe=cancellation_probe,
        cancellation_check_interval_levels=(cancellation_check_interval_levels),
        imbalance_decimal_precision=imbalance_decimal_precision,
    )

    replay_report = replay_orderbook_archives(
        sources,
        initial_checkpoint=initial_checkpoint,
        batch_size=batch_size,
        final_source_content_sha256=final_source_content_sha256,
        cancellation_probe=cancellation_probe,
        cancellation_check_interval_rows=(cancellation_check_interval_rows),
        archive_start_state_sink=sampler.start_archive,
        before_event_state_sink=sampler.before_event,
        archive_end_state_sink=sampler.finish_archive,
    )

    return SampledLiquidityChain(
        replay_report=replay_report,
        hours=sampler.finish(),
    )


__all__ = [
    "HourlyLiquidityBlock",
    "HourlyLiquidityError",
    "LiquidityObservation",
    "LiquidityQualitySummary",
    "OneSecondLiquiditySampler",
    "SampledLiquidityChain",
    "build_liquidity_quality_summary",
    "sample_liquidity_archives",
]

# l2shock/ingest/sampling.py
"""Exact one-second sampling of deterministic replay state.

Sampling semantics:

- ``received_time_ns`` is the authoritative sampling clock;
- every completed UTC source hour produces exactly 3,600 samples;
- bucket endpoints are second boundaries from ``HH:00:01`` through the next
  exact UTC hour;
- an event observed exactly at a bucket endpoint belongs to that bucket;
- when several events share one ``received_time_ns``, all of them are applied
  before a bucket at that timestamp is emitted;
- quiet seconds carry the latest replay state;
- uninitialized, invalidated, locked, crossed, and empty-side states produce
  explicit INVALID observations;
- only a sequence-valid NORMAL book produces a VALID observation.

This module records scalar state only. It does not copy complete order-book
levels and does not calculate depth-band liquidity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Final

from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.ingest.parquet_reader import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS,
    OrderBookEvent,
)
from l2shock.ingest.replay import (
    BookInitializationState,
    OrderBookCheckpoint,
    ReplayBookObservation,
    ReplayBookStructure,
    ReplayCancellationProbe,
    ReplayChainReport,
    ReplayEventOutcome,
    ReplaySource,
    replay_orderbook_archives,
)
from l2shock.timeutils import require_utc_hour

OBSERVATIONS_PER_HOUR: Final[int] = 3_600
NANOSECONDS_PER_SECOND: Final[int] = 1_000_000_000

_EPOCH_UTC: Final[datetime] = datetime(
    1970,
    1,
    1,
    tzinfo=timezone.utc,
)


class BookSampleQuality(StrEnum):
    """Analytical usability of one sampled reconstructed-book state."""

    VALID = "VALID"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


class BookSampleInvalidReason(StrEnum):
    """Why one sampled book state is analytically unusable."""

    UNINITIALIZED = "uninitialized"
    REPLAY_INVALIDATED = "replay_invalidated"
    LOCKED = "locked"
    CROSSED = "crossed"
    EMPTY_BID = "empty_bid"
    EMPTY_ASK = "empty_ask"
    EMPTY_BOTH = "empty_both"


class BookSamplingError(ValueError):
    """Replay observations violate the one-second sampling contract."""


@dataclass(frozen=True, slots=True)
class SampledBookObservation:
    """One immutable reconstructed-state observation at a bucket endpoint."""

    bucket_index: int
    bucket_start_utc: datetime
    bucket_end_utc: datetime

    source_received_time_ns: int | None

    quality: BookSampleQuality
    invalid_reason: BookSampleInvalidReason | None

    replay_valid: bool
    initialization_state: BookInitializationState
    last_update_id: int | None
    book_structure: ReplayBookStructure

    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_level_count: int
    ask_level_count: int
    checkpoint_eligible: bool

    def __post_init__(self) -> None:
        if (
            isinstance(self.bucket_index, bool)
            or not isinstance(self.bucket_index, int)
            or not 0 <= self.bucket_index < OBSERVATIONS_PER_HOUR
        ):
            raise ValueError("bucket_index must be inside [0, 3599]")

        for name in ("bucket_start_utc", "bucket_end_utc"):
            value = getattr(self, name)

            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() != timedelta(0)
            ):
                raise ValueError(f"{name} must be timezone-aware UTC")

        if self.bucket_end_utc - self.bucket_start_utc != timedelta(seconds=1):
            raise ValueError("Sample bucket must span exactly one second")

        if self.source_received_time_ns is not None:
            if (
                isinstance(self.source_received_time_ns, bool)
                or not isinstance(self.source_received_time_ns, int)
                or self.source_received_time_ns < 0
            ):
                raise ValueError("source_received_time_ns must be non-negative or null")

        quality = BookSampleQuality(self.quality)
        object.__setattr__(self, "quality", quality)

        if self.invalid_reason is not None:
            object.__setattr__(
                self,
                "invalid_reason",
                BookSampleInvalidReason(self.invalid_reason),
            )

        if quality is BookSampleQuality.VALID:
            if self.invalid_reason is not None:
                raise ValueError("A VALID book sample cannot have an invalid reason")

            if not self.replay_valid:
                raise ValueError("A VALID book sample requires valid replay state")

            if self.book_structure is not ReplayBookStructure.NORMAL:
                raise ValueError("Only a NORMAL book can produce a VALID sample")

            if self.best_bid is None or self.best_ask is None:
                raise ValueError("A VALID book sample requires best bid and ask")

            if self.best_bid >= self.best_ask:
                raise ValueError("A VALID book sample requires best_bid < best_ask")

        elif self.invalid_reason is None:
            raise ValueError("An INVALID or DEGRADED sample requires a reason")

    @property
    def valid(self) -> bool:
        return self.quality is BookSampleQuality.VALID


@dataclass(frozen=True, slots=True)
class SampledBookHour:
    """Exactly 3,600 one-second observations for one source UTC hour."""

    spec: SourceFileSpec
    observations: tuple[SampledBookObservation, ...]

    def __post_init__(self) -> None:
        if self.spec.data_kind is not SourceDataKind.ORDERBOOK:
            raise ValueError("SampledBookHour requires an orderbook source")

        if len(self.observations) != OBSERVATIONS_PER_HOUR:
            raise ValueError("SampledBookHour must contain exactly 3,600 observations")

        for expected_index, observation in enumerate(self.observations):
            if observation.bucket_index != expected_index:
                raise ValueError("Sampled observations are not in bucket-index order")

            expected_start = self.spec.hour_utc + timedelta(seconds=expected_index)
            expected_end = expected_start + timedelta(seconds=1)

            if observation.bucket_start_utc != expected_start:
                raise ValueError(
                    "Sample bucket start does not match source-hour ownership"
                )

            if observation.bucket_end_utc != expected_end:
                raise ValueError(
                    "Sample bucket end does not match source-hour ownership"
                )

    @property
    def valid_count(self) -> int:
        return sum(observation.valid for observation in self.observations)

    @property
    def degraded_count(self) -> int:
        return sum(
            observation.quality is BookSampleQuality.DEGRADED
            for observation in self.observations
        )

    @property
    def invalid_count(self) -> int:
        return sum(
            observation.quality is BookSampleQuality.INVALID
            for observation in self.observations
        )

    @property
    def fully_valid(self) -> bool:
        return self.valid_count == OBSERVATIONS_PER_HOUR


@dataclass(frozen=True, slots=True)
class SampledReplayChain:
    """Replay report and one-second sampled hours from the same execution."""

    replay_report: ReplayChainReport
    hours: tuple[SampledBookHour, ...]

    def __post_init__(self) -> None:
        if len(self.hours) != len(self.replay_report.archives):
            raise ValueError("Sampled hour count must match replay archive count")

        for sampled, archive in zip(
            self.hours,
            self.replay_report.archives,
        ):
            if sampled.spec.identity_tuple != archive.spec.identity_tuple:
                raise ValueError("Sampled-hour identity does not match replay archive")

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
        raise BookSamplingError("Epoch conversion requires timezone-aware UTC")

    delta = value - _EPOCH_UTC

    return (
        delta.days * 86_400 * NANOSECONDS_PER_SECOND
        + delta.seconds * NANOSECONDS_PER_SECOND
        + delta.microseconds * 1_000
    )


def classify_book_observation(
    observation: ReplayBookObservation,
) -> tuple[
    BookSampleQuality,
    BookSampleInvalidReason | None,
]:
    """Apply the locked analytical-quality policy to replay state."""
    if not observation.valid:
        if observation.initialization_state is BookInitializationState.INVALIDATED:
            reason = BookSampleInvalidReason.REPLAY_INVALIDATED
        else:
            reason = BookSampleInvalidReason.UNINITIALIZED

        return BookSampleQuality.INVALID, reason

    structure = observation.book_structure

    if structure is ReplayBookStructure.NORMAL:
        return BookSampleQuality.VALID, None

    reason_by_structure = {
        ReplayBookStructure.LOCKED: (BookSampleInvalidReason.LOCKED),
        ReplayBookStructure.CROSSED: (BookSampleInvalidReason.CROSSED),
        ReplayBookStructure.EMPTY_BID: (BookSampleInvalidReason.EMPTY_BID),
        ReplayBookStructure.EMPTY_ASK: (BookSampleInvalidReason.EMPTY_ASK),
        ReplayBookStructure.EMPTY_BOTH: (BookSampleInvalidReason.EMPTY_BOTH),
    }

    try:
        reason = reason_by_structure[structure]
    except KeyError as exc:
        raise BookSamplingError(
            f"Unsupported replay book structure: {structure!r}"
        ) from exc

    return BookSampleQuality.INVALID, reason


class OneSecondBookSampler:
    """Consume replay callbacks and produce fixed one-second source hours.

    Complete order-book levels are never retained by this object. It holds only
    the latest immutable scalar replay observation and the fixed output samples.
    """

    def __init__(self) -> None:
        self._active_spec: SourceFileSpec | None = None
        self._current_observation: ReplayBookObservation | None = None

        self._pending_received_time_ns: int | None = None
        self._pending_observation: ReplayBookObservation | None = None

        self._next_bucket_index = 0
        self._active_samples: list[SampledBookObservation] = []
        self._completed_hours: list[SampledBookHour] = []
        self._finished = False

    @property
    def completed_hours(self) -> tuple[SampledBookHour, ...]:
        return tuple(self._completed_hours)

    def start_archive(
        self,
        spec: SourceFileSpec,
        initial_observation: ReplayBookObservation,
    ) -> None:
        """Begin one source hour from its pre-first-event replay state."""
        if self._finished:
            raise BookSamplingError(
                "Cannot start an archive after sampler finalization"
            )

        if spec.data_kind is not SourceDataKind.ORDERBOOK:
            raise BookSamplingError(
                "One-second book sampling requires orderbook sources"
            )

        require_utc_hour("spec.hour_utc", spec.hour_utc)

        if self._active_spec is not None:
            self._finish_active_hour()

            previous = self._completed_hours[-1].spec
            expected = previous.hour_utc + timedelta(hours=1)

            if spec.hour_utc != expected:
                raise BookSamplingError(
                    "Sampled source archives must be adjacent UTC hours"
                )

            if (
                spec.provider != previous.provider
                or spec.venue != previous.venue
                or spec.symbol != previous.symbol
            ):
                raise BookSamplingError(
                    "Sampled source identity changed between archives"
                )

        self._active_spec = spec
        self._current_observation = initial_observation
        self._pending_received_time_ns = None
        self._pending_observation = None
        self._next_bucket_index = 0
        self._active_samples = []

    def observe_event(
        self,
        spec: SourceFileSpec,
        event: OrderBookEvent,
        _outcome: ReplayEventOutcome,
        observation: ReplayBookObservation,
    ) -> None:
        """Consume the post-event replay state.

        Events sharing one receive timestamp are collapsed to their final
        post-event state before a bucket at that exact timestamp is emitted.
        """
        active = self._active_spec

        if active is None:
            raise BookSamplingError(
                "Replay event arrived before archive sampling started"
            )

        if spec.identity_tuple != active.identity_tuple:
            raise BookSamplingError(
                "Replay event source does not match the active sampled hour"
            )

        received_time_ns = event.received_time_ns

        if observation.last_event_received_time_ns != received_time_ns:
            raise BookSamplingError(
                "Replay observation timestamp does not match its event"
            )

        hour_start_ns = _datetime_to_epoch_ns(active.hour_utc)
        hour_end_ns = hour_start_ns + OBSERVATIONS_PER_HOUR * NANOSECONDS_PER_SECOND

        if not hour_start_ns <= received_time_ns < hour_end_ns:
            raise BookSamplingError(
                "Event received_time_ns lies outside its source UTC hour"
            )

        pending_time = self._pending_received_time_ns

        if pending_time is None:
            self._emit_bucket_ends_before(received_time_ns)
            self._pending_received_time_ns = received_time_ns
            self._pending_observation = observation
            return

        if received_time_ns < pending_time:
            raise BookSamplingError(
                "received_time_ns regressed during one-second sampling"
            )

        if received_time_ns == pending_time:
            # Retain the final replay state after all events with this exact
            # receive timestamp.
            self._pending_observation = observation
            return

        self._commit_pending_timestamp()
        self._emit_bucket_ends_before(received_time_ns)

        self._pending_received_time_ns = received_time_ns
        self._pending_observation = observation

    def finish(self) -> tuple[SampledBookHour, ...]:
        """Finalize the active source hour and return all sampled hours."""
        if not self._finished:
            if self._active_spec is not None:
                self._finish_active_hour()

            self._finished = True

        return tuple(self._completed_hours)

    def _bucket_end_ns(self, bucket_index: int) -> int:
        active = self._active_spec

        if active is None:
            raise BookSamplingError("No source hour is active")

        return (
            _datetime_to_epoch_ns(active.hour_utc)
            + (bucket_index + 1) * NANOSECONDS_PER_SECOND
        )

    def _emit_bucket_ends_before(
        self,
        received_time_ns: int,
    ) -> None:
        while (
            self._next_bucket_index < OBSERVATIONS_PER_HOUR
            and self._bucket_end_ns(self._next_bucket_index) < received_time_ns
        ):
            self._append_current_sample()

    def _emit_bucket_ends_at_or_before(
        self,
        received_time_ns: int,
    ) -> None:
        while (
            self._next_bucket_index < OBSERVATIONS_PER_HOUR
            and self._bucket_end_ns(self._next_bucket_index) <= received_time_ns
        ):
            self._append_current_sample()

    def _commit_pending_timestamp(self) -> None:
        if self._pending_received_time_ns is None:
            return

        if self._pending_observation is None:
            raise BookSamplingError(
                "Pending sampling timestamp has no replay observation"
            )

        self._current_observation = self._pending_observation

        self._emit_bucket_ends_at_or_before(self._pending_received_time_ns)

        self._pending_received_time_ns = None
        self._pending_observation = None

    def _append_current_sample(self) -> None:
        active = self._active_spec
        observation = self._current_observation

        if active is None or observation is None:
            raise BookSamplingError("Cannot emit a sample without active replay state")

        bucket_index = self._next_bucket_index
        bucket_start = active.hour_utc + timedelta(seconds=bucket_index)
        bucket_end = bucket_start + timedelta(seconds=1)

        quality, invalid_reason = classify_book_observation(observation)

        sampled = SampledBookObservation(
            bucket_index=bucket_index,
            bucket_start_utc=bucket_start,
            bucket_end_utc=bucket_end,
            source_received_time_ns=(observation.last_event_received_time_ns),
            quality=quality,
            invalid_reason=invalid_reason,
            replay_valid=observation.valid,
            initialization_state=(observation.initialization_state),
            last_update_id=observation.last_update_id,
            book_structure=observation.book_structure,
            best_bid=observation.best_bid,
            best_ask=observation.best_ask,
            bid_level_count=observation.bid_level_count,
            ask_level_count=observation.ask_level_count,
            checkpoint_eligible=observation.checkpoint_eligible,
        )

        self._active_samples.append(sampled)
        self._next_bucket_index += 1

    def _finish_active_hour(self) -> None:
        active = self._active_spec

        if active is None:
            return

        self._commit_pending_timestamp()

        while self._next_bucket_index < OBSERVATIONS_PER_HOUR:
            self._append_current_sample()

        hour = SampledBookHour(
            spec=active,
            observations=tuple(self._active_samples),
        )
        self._completed_hours.append(hour)

        self._active_spec = None
        self._current_observation = None
        self._pending_received_time_ns = None
        self._pending_observation = None
        self._next_bucket_index = 0
        self._active_samples = []


def sample_orderbook_archives(
    sources: tuple[ReplaySource, ...],
    *,
    initial_checkpoint: OrderBookCheckpoint | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    final_source_content_sha256: str | None = None,
    cancellation_probe: ReplayCancellationProbe | None = None,
    cancellation_check_interval_rows: int = (DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS),
) -> SampledReplayChain:
    """Replay and sample adjacent order-book archives in one streamed pass."""
    sampler = OneSecondBookSampler()

    replay_report = replay_orderbook_archives(
        sources,
        initial_checkpoint=initial_checkpoint,
        batch_size=batch_size,
        final_source_content_sha256=final_source_content_sha256,
        cancellation_probe=cancellation_probe,
        cancellation_check_interval_rows=(cancellation_check_interval_rows),
        archive_start_sink=sampler.start_archive,
        event_observation_sink=sampler.observe_event,
    )

    hours = sampler.finish()

    return SampledReplayChain(
        replay_report=replay_report,
        hours=hours,
    )


__all__ = [
    "BookSampleInvalidReason",
    "BookSampleQuality",
    "BookSamplingError",
    "NANOSECONDS_PER_SECOND",
    "OBSERVATIONS_PER_HOUR",
    "OneSecondBookSampler",
    "SampledBookHour",
    "SampledBookObservation",
    "SampledReplayChain",
    "classify_book_observation",
    "sample_orderbook_archives",
]

# l2shock/ingest/replay.py
"""Deterministic order-book replay and cross-hour checkpoint contracts.

This module consumes normalized ``OrderBookEvent`` objects produced by the
streamed Parquet reader.

It establishes whether a complete order-book state is currently available,
applies valid snapshots and updates, invalidates unverifiable state, and creates
immutable checkpoints for the immediately following UTC hour.

Important boundaries:

- an update-only stream cannot initialize a book;
- a checkpoint is accepted only for the immediately following source hour;
- quantity zero removes one price level;
- continuity failure discards the complete local state;
- unapplied updates never mutate an uninitialized or invalidated book;
- this module does not sample one-second buckets;
- this module does not calculate liquidity;
- this module does not promote PostgreSQL analytical quality.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from heapq import heapify, heappop, heappush
from pathlib import Path
from typing import Final, TypeAlias

from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.ingest.parquet_reader import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS,
    BookSide,
    OrderBookEvent,
    OrderBookEventType,
    OrderBookReadReport,
    StreamedParquetReadCancelled,
    read_orderbook_file,
)
from l2shock.ingest.venue_adapter import (
    VenueSequenceContractError,
    orderbook_sequence_contract,
)
from l2shock.timeutils import require_utc_hour

MAX_RETAINED_REPLAY_ISSUES: Final[int] = 100


class BookInitializationState(StrEnum):
    """How the current replay state should be interpreted."""

    UNINITIALIZED = "uninitialized"
    SNAPSHOT = "snapshot"
    CARRIED = "carried"
    INVALIDATED = "invalidated"


class ReplayBookStructure(StrEnum):
    """Observed shape of the currently retained price-level book."""

    NORMAL = "normal"
    LOCKED = "locked"
    CROSSED = "crossed"
    EMPTY_BID = "empty_bid"
    EMPTY_ASK = "empty_ask"
    EMPTY_BOTH = "empty_both"


class ReplayEventAction(StrEnum):
    """Result of applying or refusing one normalized event."""

    SNAPSHOT_APPLIED = "snapshot_applied"
    UPDATE_APPLIED = "update_applied"
    SKIPPED_UNINITIALIZED = "skipped_uninitialized"
    DUPLICATE_SKIPPED = "duplicate_skipped"
    INVALIDATED = "invalidated"


class ReplayContractError(ValueError):
    """Replay inputs or checkpoint identity violate the replay contract."""


class ReplayCancelledError(RuntimeError):
    """Replay was cooperatively cancelled before completion."""


ReplayCancellationProbe: TypeAlias = Callable[[], bool]


@dataclass(frozen=True, slots=True)
class CheckpointLevel:
    """One positive price level retained in an immutable checkpoint."""

    side: BookSide
    price: Decimal
    quantity: Decimal
    order_count: int | None

    def __post_init__(self) -> None:
        try:
            side = BookSide(self.side)
        except (TypeError, ValueError) as exc:
            raise ReplayContractError(
                "Checkpoint level has an unsupported side"
            ) from exc

        object.__setattr__(self, "side", side)

        if not isinstance(self.price, Decimal):
            raise ReplayContractError("Checkpoint price must be an exact Decimal")

        if not self.price.is_finite() or self.price <= 0:
            raise ReplayContractError("Checkpoint price must be finite and positive")

        if not isinstance(self.quantity, Decimal):
            raise ReplayContractError("Checkpoint quantity must be an exact Decimal")

        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ReplayContractError("Checkpoint quantity must be finite and positive")

        if self.order_count is not None:
            if isinstance(self.order_count, bool) or not isinstance(
                self.order_count, int
            ):
                raise ReplayContractError(
                    "Checkpoint order_count must be an integer or null"
                )

            if self.order_count < 0:
                raise ReplayContractError("Checkpoint order_count cannot be negative")


@dataclass(frozen=True, slots=True)
class OrderBookCheckpoint:
    """Complete immutable state carried after one source UTC hour.

    ``through_hour_utc`` identifies the source hour whose replay has completed.
    This checkpoint may initialize only the immediately following UTC hour for
    the same provider, venue, and symbol.
    """

    provider: str
    venue: str
    symbol: str
    through_hour_utc: datetime
    last_update_id: int
    levels: tuple[CheckpointLevel, ...]
    source_content_sha256: str | None = None

    def __post_init__(self) -> None:
        provider = str(self.provider or "").strip().lower()
        venue = str(self.venue or "").strip().lower()
        symbol = str(self.symbol or "").strip().upper()

        if not provider or not venue or not symbol:
            raise ReplayContractError(
                "Checkpoint source identity cannot contain blank fields"
            )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(
            self,
            "through_hour_utc",
            require_utc_hour(
                "through_hour_utc",
                self.through_hour_utc,
            ),
        )

        if (
            isinstance(self.last_update_id, bool)
            or not isinstance(self.last_update_id, int)
            or self.last_update_id < 0
        ):
            raise ReplayContractError(
                "Checkpoint last_update_id must be a non-negative integer"
            )

        levels = tuple(self.levels)
        object.__setattr__(self, "levels", levels)

        if not levels:
            raise ReplayContractError(
                "A usable checkpoint must contain order-book levels"
            )

        identities: set[tuple[BookSide, Decimal]] = set()
        sides: set[BookSide] = set()

        for level in levels:
            if not isinstance(level, CheckpointLevel):
                raise ReplayContractError(
                    "Checkpoint levels must be CheckpointLevel objects"
                )

            identity = (level.side, level.price)

            if identity in identities:
                raise ReplayContractError(
                    "Checkpoint contains a duplicate side/price level"
                )

            identities.add(identity)
            sides.add(level.side)

        if sides != {BookSide.BID, BookSide.ASK}:
            raise ReplayContractError(
                "A usable checkpoint must contain both bid and ask levels"
            )

        digest = str(self.source_content_sha256 or "").strip().lower()

        if digest:
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ReplayContractError(
                    "source_content_sha256 must be a lowercase SHA-256"
                )

            object.__setattr__(
                self,
                "source_content_sha256",
                digest,
            )
        else:
            object.__setattr__(
                self,
                "source_content_sha256",
                None,
            )

    @property
    def next_hour_utc(self) -> datetime:
        return self.through_hour_utc + timedelta(hours=1)

    @property
    def bid_level_count(self) -> int:
        return sum(level.side is BookSide.BID for level in self.levels)

    @property
    def ask_level_count(self) -> int:
        return sum(level.side is BookSide.ASK for level in self.levels)

    def validate_for_source(self, spec: SourceFileSpec) -> None:
        """Require exact identity and immediate-hour checkpoint ownership."""
        if spec.data_kind is not SourceDataKind.ORDERBOOK:
            raise ReplayContractError(
                "An order-book checkpoint requires an orderbook source"
            )

        if (
            self.provider != spec.provider
            or self.venue != spec.venue
            or self.symbol != spec.symbol
        ):
            raise ReplayContractError(
                "Checkpoint source identity does not match the target source"
            )

        if self.next_hour_utc != spec.hour_utc:
            raise ReplayContractError(
                "Checkpoint may initialize only the immediately following "
                "UTC source hour"
            )


@dataclass(frozen=True, slots=True)
class ReplayIssue:
    """One bounded deterministic replay diagnostic."""

    kind: str
    hour_utc: datetime
    row_number: int
    previous_update_id: int | None
    current_update_id: int | None
    message: str


@dataclass(frozen=True, slots=True)
class ReplayEventOutcome:
    """Outcome of applying or refusing one normalized event."""

    action: ReplayEventAction
    valid_after: bool
    initialization_state: BookInitializationState
    last_update_id: int | None
    book_structure_after: ReplayBookStructure | None = None
    zero_quantity_change_count: int = 0
    levels_removed_count: int = 0
    issue: ReplayIssue | None = None


@dataclass(frozen=True, slots=True)
class ReplayBookObservation:
    """Immutable scalar observation of the current replay state.

    This object intentionally contains no mutable level dictionaries and does
    not copy the complete book. It captures the exact top-of-book and replay
    metadata at the time ``OrderBookReplayState.observation`` is called.

    ``last_event_received_time_ns`` is the authoritative sampling-clock marker
    for the most recently processed source event. It is null only when no
    source event has yet been processed, such as immediately after checkpoint
    restoration.
    """

    last_event_received_time_ns: int | None
    valid: bool
    initialization_state: BookInitializationState
    last_update_id: int | None
    book_structure: ReplayBookStructure
    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_level_count: int
    ask_level_count: int
    checkpoint_eligible: bool


@dataclass(frozen=True, slots=True)
class ReplayBookLevel:
    """Immutable exact view of one currently live order-book level."""

    side: BookSide
    price: Decimal
    quantity: Decimal
    order_count: int | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", BookSide(self.side))

        if (
            not isinstance(self.price, Decimal)
            or not self.price.is_finite()
            or self.price <= 0
        ):
            raise ReplayContractError(
                "ReplayBookLevel.price must be a positive finite Decimal"
            )

        if (
            not isinstance(self.quantity, Decimal)
            or not self.quantity.is_finite()
            or self.quantity <= 0
        ):
            raise ReplayContractError(
                "ReplayBookLevel.quantity must be a positive finite Decimal"
            )


ReplayArchiveStartSink: TypeAlias = Callable[
    [SourceFileSpec, ReplayBookObservation],
    None,
]

ReplayEventObservationSink: TypeAlias = Callable[
    [
        SourceFileSpec,
        OrderBookEvent,
        ReplayEventOutcome,
        ReplayBookObservation,
    ],
    None,
]

ReplayArchiveStateSink: TypeAlias = Callable[
    [SourceFileSpec, "OrderBookReplayState"],
    None,
]

ReplayBeforeEventStateSink: TypeAlias = Callable[
    [SourceFileSpec, OrderBookEvent, "OrderBookReplayState"],
    None,
]


@dataclass(frozen=True, slots=True)
class ReplayArchiveReport:
    """Replay facts for one streamed hourly order-book archive."""

    path: Path
    spec: SourceFileSpec
    reader_report: OrderBookReadReport

    initial_state: BookInitializationState
    final_state: BookInitializationState
    initially_valid: bool
    finally_valid: bool

    events_seen: int
    snapshots_applied: int
    updates_applied: int
    updates_skipped_uninitialized: int
    duplicate_updates_skipped: int
    invalidation_count: int
    recovery_count: int

    zero_quantity_change_count: int
    levels_removed_count: int

    initial_update_id: int | None
    final_update_id: int | None
    final_bid_level_count: int
    final_ask_level_count: int

    structure_counts: tuple[tuple[ReplayBookStructure, int], ...]

    retained_issues: tuple[ReplayIssue, ...]
    retained_issue_limit: int

    @property
    def independently_initialized(self) -> bool:
        """Return whether this archive established state without carry input.

        Applying a snapshot does not imply independent initialization when the
        archive began from a checkpoint or carried replay state. In particular,
        a Bybit frontier-less archive-boundary snapshot may be applied only
        because the immediately preceding replay frontier already exists.
        """

        return bool(
            self.snapshots_applied > 0
            and self.initial_state is not BookInitializationState.CARRIED
        )

    @property
    def required_carried_state(self) -> bool:
        """Return whether the archive began from carried/checkpoint state.

        A later snapshot does not erase the fact that the archive began with
        carried state. This is especially important for Bybit archive-boundary
        snapshots whose native replay frontier is null: those snapshots can
        replace levels only because the carried frontier already exists.
        """

        return self.initial_state is BookInitializationState.CARRIED

    @property
    def structure_count_map(
        self,
    ) -> dict[ReplayBookStructure, int]:
        return dict(self.structure_counts)

    @property
    def structural_anomaly_count(self) -> int:
        return sum(
            count
            for structure, count in self.structure_counts
            if structure is not ReplayBookStructure.NORMAL
        )


@dataclass(frozen=True, slots=True)
class ReplayChainReport:
    """Result of replaying one ordered sequence of adjacent source hours."""

    provider: str
    venue: str
    symbol: str
    archives: tuple[ReplayArchiveReport, ...]
    final_checkpoint: OrderBookCheckpoint | None

    @property
    def finally_valid(self) -> bool:
        return self.final_checkpoint is not None

    @property
    def total_events_seen(self) -> int:
        return sum(item.events_seen for item in self.archives)

    @property
    def total_invalidations(self) -> int:
        return sum(item.invalidation_count for item in self.archives)

    @property
    def total_snapshots_applied(self) -> int:
        return sum(item.snapshots_applied for item in self.archives)


ReplaySource: TypeAlias = tuple[Path, SourceFileSpec]
_LevelPayload: TypeAlias = tuple[Decimal, int | None]
_EventSignature: TypeAlias = tuple[object, ...]


def _event_signature(event: OrderBookEvent) -> _EventSignature:
    return (
        event.symbol,
        event.event_type,
        event.received_time_ns,
        event.event_time_ms,
        event.transaction_time_ms,
        event.first_update_id,
        event.final_update_id,
        event.prev_final_update_id,
        event.last_update_id,
        tuple(
            (
                change.side,
                change.price,
                change.quantity,
                change.order_count,
            )
            for change in event.changes
        ),
    )


class OrderBookReplayState:
    """Mutable replay state for exactly one provider/venue/symbol identity."""

    def __init__(
        self,
        *,
        provider: str,
        venue: str,
        symbol: str,
    ) -> None:
        self._provider = str(provider or "").strip().lower()
        self._venue = str(venue or "").strip().lower()
        self._symbol = str(symbol or "").strip().upper()

        if not self._provider or not self._venue or not self._symbol:
            raise ReplayContractError(
                "Replay source identity cannot contain blank fields"
            )

        self._bids: dict[Decimal, _LevelPayload] = {}
        self._asks: dict[Decimal, _LevelPayload] = {}

        # Exact lazy top-of-book indexes.
        #
        # Bids are stored as negative Decimal prices so heap position zero is
        # the greatest live bid. Asks use positive prices so position zero is
        # the least live ask.
        #
        # The dictionaries remain authoritative. Heap entries whose prices
        # are no longer present in the corresponding dictionary are stale and
        # are removed lazily.
        self._bid_price_heap: list[Decimal] = []
        self._ask_price_heap: list[Decimal] = []

        # Exact ascending live-price indexes used for bounded depth queries.
        #
        # Unlike the lazy heaps, these lists contain only currently live
        # prices. Bisect locates inclusive interval boundaries without
        # scanning the complete authoritative dictionaries. Only prices inside
        # the requested depth interval are subsequently visited.
        self._bid_prices_sorted: list[Decimal] = []
        self._ask_prices_sorted: list[Decimal] = []

        self._valid = False
        self._sequence_contract = orderbook_sequence_contract(self._venue)
        self._initialization_state = BookInitializationState.UNINITIALIZED
        self._last_update_id: int | None = None
        self._last_update_signature: _EventSignature | None = None
        self._last_event_received_time_ns: int | None = None

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: OrderBookCheckpoint,
        target_spec: SourceFileSpec,
    ) -> OrderBookReplayState:
        checkpoint.validate_for_source(target_spec)

        state = cls(
            provider=checkpoint.provider,
            venue=checkpoint.venue,
            symbol=checkpoint.symbol,
        )

        for level in checkpoint.levels:
            target = state._bids if level.side is BookSide.BID else state._asks
            target[level.price] = (
                level.quantity,
                level.order_count,
            )

        state._rebuild_price_indexes()
        state._valid = True
        state._initialization_state = BookInitializationState.CARRIED
        state._last_update_id = checkpoint.last_update_id
        state._last_update_signature = None
        state._last_event_received_time_ns = None
        return state

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def venue(self) -> str:
        return self._venue

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def valid(self) -> bool:
        return self._valid

    @property
    def initialization_state(self) -> BookInitializationState:
        return self._initialization_state

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    @property
    def bid_level_count(self) -> int:
        return len(self._bids)

    @property
    def ask_level_count(self) -> int:
        return len(self._asks)

    def _rebuild_price_indexes(self) -> None:
        """Rebuild exact top-of-book and range-query indexes."""
        self._bid_price_heap = [-price for price in self._bids]
        self._ask_price_heap = list(self._asks)
        heapify(self._bid_price_heap)
        heapify(self._ask_price_heap)

        self._bid_prices_sorted = sorted(self._bids)
        self._ask_prices_sorted = sorted(self._asks)

    def _clear_book(self) -> None:
        """Clear authoritative levels and all derived price indexes."""
        self._bids.clear()
        self._asks.clear()
        self._bid_price_heap.clear()
        self._ask_price_heap.clear()
        self._bid_prices_sorted.clear()
        self._ask_prices_sorted.clear()

    def _maybe_compact_price_index(self, side: BookSide) -> None:
        """Bound stale lazy-heap entries without scanning on every read."""
        if side is BookSide.BID:
            source = self._bids
            heap = self._bid_price_heap
        else:
            source = self._asks
            heap = self._ask_price_heap

        # Decide whether compaction is needed before constructing a complete
        # replacement. Building that replacement is itself O(live levels).
        if len(heap) <= max(64, len(source) * 2):
            return

        if side is BookSide.BID:
            replacement = [-price for price in source]
        else:
            replacement = list(source)

        heap[:] = replacement
        heapify(heap)

    def _upsert_live_level(
        self,
        *,
        side: BookSide,
        price: Decimal,
        payload: _LevelPayload,
    ) -> None:
        """Insert or replace one live level while maintaining exact indexes."""
        if side is BookSide.BID:
            target = self._bids
            heap = self._bid_price_heap
            sorted_prices = self._bid_prices_sorted
            heap_price = -price
        else:
            target = self._asks
            heap = self._ask_price_heap
            sorted_prices = self._ask_prices_sorted
            heap_price = price

        is_new_price = price not in target
        target[price] = payload

        if is_new_price:
            heappush(heap, heap_price)
            insort(sorted_prices, price)

    def _remove_live_level(
        self,
        *,
        side: BookSide,
        price: Decimal,
    ) -> bool:
        """Remove one authoritative level and its exact range-index entry."""
        if side is BookSide.BID:
            target = self._bids
            sorted_prices = self._bid_prices_sorted
        else:
            target = self._asks
            sorted_prices = self._ask_prices_sorted

        if target.pop(price, None) is None:
            return False

        position = bisect_left(sorted_prices, price)

        if position >= len(sorted_prices) or sorted_prices[position] != price:
            raise ReplayContractError(
                "Live-level dictionary and sorted price index diverged"
            )

        sorted_prices.pop(position)
        self._maybe_compact_price_index(side)
        return True

    @property
    def best_bid(self) -> Decimal | None:
        """Return the greatest live bid without scanning all bid levels."""
        while self._bid_price_heap:
            price = -self._bid_price_heap[0]

            if price in self._bids:
                return price

            heappop(self._bid_price_heap)

        return None

    @property
    def best_ask(self) -> Decimal | None:
        """Return the least live ask without scanning all ask levels."""
        while self._ask_price_heap:
            price = self._ask_price_heap[0]

            if price in self._asks:
                return price

            heappop(self._ask_price_heap)

        return None

    @staticmethod
    def _classify_book_structure(
        *,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> ReplayBookStructure:
        if best_bid is None and best_ask is None:
            return ReplayBookStructure.EMPTY_BOTH

        if best_bid is None:
            return ReplayBookStructure.EMPTY_BID

        if best_ask is None:
            return ReplayBookStructure.EMPTY_ASK

        if best_bid > best_ask:
            return ReplayBookStructure.CROSSED

        if best_bid == best_ask:
            return ReplayBookStructure.LOCKED

        return ReplayBookStructure.NORMAL

    @property
    def book_structure(self) -> ReplayBookStructure:
        """Return book geometry without changing replay validity."""
        return self._classify_book_structure(
            best_bid=self.best_bid,
            best_ask=self.best_ask,
        )

    @property
    def checkpoint_eligible(self) -> bool:
        """Return whether the state can satisfy the checkpoint contract."""
        return bool(
            self._valid
            and self._last_update_id is not None
            and self._bids
            and self._asks
        )

    def observation(self) -> ReplayBookObservation:
        """Return an immutable scalar observation without copying the book."""
        best_bid = self.best_bid
        best_ask = self.best_ask

        return ReplayBookObservation(
            last_event_received_time_ns=(self._last_event_received_time_ns),
            valid=self._valid,
            initialization_state=self._initialization_state,
            last_update_id=self._last_update_id,
            book_structure=self._classify_book_structure(
                best_bid=best_bid,
                best_ask=best_ask,
            ),
            best_bid=best_bid,
            best_ask=best_ask,
            bid_level_count=len(self._bids),
            ask_level_count=len(self._asks),
            checkpoint_eligible=self.checkpoint_eligible,
        )

    def quantity_at(
        self,
        side: BookSide | str,
        price: Decimal,
    ) -> Decimal | None:
        normalized_side = BookSide(side)
        source = self._bids if normalized_side is BookSide.BID else self._asks

        payload = source.get(price)
        return payload[0] if payload is not None else None

    def iter_levels(
        self,
        side: BookSide | str,
        *,
        lower_price: Decimal,
        upper_price: Decimal,
    ) -> Iterator[ReplayBookLevel]:
        """Iterate live levels inside one inclusive exact price interval.

        Interval discovery uses the exact ascending side index and ``bisect``.
        The complete book dictionary is not scanned or copied.

        The yielded levels are ordered by ascending price for both sides.
        Callers which need a different presentation order may reverse their
        own bounded result, but liquidity summation is order-independent.
        """
        normalized_side = BookSide(side)

        for name, value in (
            ("lower_price", lower_price),
            ("upper_price", upper_price),
        ):
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ReplayContractError(f"{name} must be a positive finite Decimal")

        if lower_price > upper_price:
            raise ReplayContractError("lower_price cannot exceed upper_price")

        if normalized_side is BookSide.BID:
            source = self._bids
            sorted_prices = self._bid_prices_sorted
        else:
            source = self._asks
            sorted_prices = self._ask_prices_sorted

        start = bisect_left(sorted_prices, lower_price)
        stop = bisect_right(sorted_prices, upper_price)

        # Copy only the bounded price slice. This prevents index mutation from
        # changing traversal positions if a future consumer accidentally
        # retains the iterator while replay continues. It never copies the
        # complete level dictionaries or payloads.
        bounded_prices = tuple(sorted_prices[start:stop])

        for price in bounded_prices:
            payload = source.get(price)

            if payload is None:
                raise ReplayContractError(
                    "Sorted price index references a missing live level"
                )

            quantity, order_count = payload

            yield ReplayBookLevel(
                side=normalized_side,
                price=price,
                quantity=quantity,
                order_count=order_count,
            )

    def mark_carried(self) -> None:
        """Mark still-valid in-memory state as carried into another hour."""
        if self._valid:
            self._initialization_state = BookInitializationState.CARRIED

    def _invalidate(
        self,
        *,
        event: OrderBookEvent,
        hour_utc: datetime,
        kind: str,
        message: str,
    ) -> ReplayEventOutcome:
        previous_update_id = self._last_update_id

        self._clear_book()
        self._valid = False
        self._initialization_state = BookInitializationState.INVALIDATED
        self._last_update_id = None
        self._last_update_signature = None

        issue = ReplayIssue(
            kind=kind,
            hour_utc=hour_utc,
            row_number=event.first_row_number,
            previous_update_id=previous_update_id,
            current_update_id=event.final_update_id,
            message=message,
        )

        return ReplayEventOutcome(
            action=ReplayEventAction.INVALIDATED,
            valid_after=False,
            initialization_state=self._initialization_state,
            last_update_id=None,
            book_structure_after=self.book_structure,
            issue=issue,
        )

    @staticmethod
    def _apply_changes(
        event: OrderBookEvent,
        *,
        bids: dict[Decimal, _LevelPayload],
        asks: dict[Decimal, _LevelPayload],
    ) -> tuple[int, int]:
        zero_changes = 0
        removed_levels = 0

        for change in event.changes:
            target = bids if change.side is BookSide.BID else asks

            if change.quantity == 0:
                zero_changes += 1

                if target.pop(change.price, None) is not None:
                    removed_levels += 1

                continue

            target[change.price] = (
                change.quantity,
                change.order_count,
            )

        return zero_changes, removed_levels

    def _apply_live_changes(
        self,
        event: OrderBookEvent,
    ) -> tuple[int, int]:
        """Apply changes to live dictionaries and exact price indexes."""
        zero_changes = 0
        removed_levels = 0

        for change in event.changes:
            if change.quantity == 0:
                zero_changes += 1

                if self._remove_live_level(
                    side=change.side,
                    price=change.price,
                ):
                    removed_levels += 1

                continue

            self._upsert_live_level(
                side=change.side,
                price=change.price,
                payload=(
                    change.quantity,
                    change.order_count,
                ),
            )

        return zero_changes, removed_levels

    def _apply_snapshot(
        self,
        event: OrderBookEvent,
        *,
        hour_utc: datetime,
    ) -> ReplayEventOutcome:
        try:
            self._sequence_contract.validate_event(
                event_type=event.event_type.value,
                transaction_time_ms=event.transaction_time_ms,
                first_update_id=event.first_update_id,
                final_update_id=event.final_update_id,
                prev_final_update_id=event.prev_final_update_id,
                last_update_id=event.last_update_id,
            )
            snapshot_frontier = self._sequence_contract.snapshot_frontier(
                final_update_id=event.final_update_id,
                last_update_id=event.last_update_id,
            )
        except VenueSequenceContractError as exc:
            return self._invalidate(
                event=event,
                hour_utc=hour_utc,
                kind="snapshot_sequence_contract_violation",
                message=str(exc),
            )

        if snapshot_frontier is None:
            if not self._valid or self._last_update_id is None:
                return self._invalidate(
                    event=event,
                    hour_utc=hour_utc,
                    kind="snapshot_missing_replay_frontier",
                    message=(
                        "A snapshot without its venue replay frontier may "
                        "replace only an already valid carried/checkpoint "
                        "state. The replay frontier must not be inferred "
                        "from a later update."
                    ),
                )

            snapshot_frontier = self._last_update_id

        new_bids: dict[Decimal, _LevelPayload] = {}
        new_asks: dict[Decimal, _LevelPayload] = {}

        zero_changes, _removed = self._apply_changes(
            event,
            bids=new_bids,
            asks=new_asks,
        )

        if not new_bids or not new_asks:
            return self._invalidate(
                event=event,
                hour_utc=hour_utc,
                kind="snapshot_missing_book_side",
                message=(
                    "A complete initialization snapshot must contain at least "
                    "one positive bid level and one positive ask level"
                ),
            )

        self._bids = new_bids
        self._asks = new_asks
        self._rebuild_price_indexes()
        self._valid = True
        self._initialization_state = BookInitializationState.SNAPSHOT
        self._last_update_id = snapshot_frontier
        self._last_update_signature = None

        return ReplayEventOutcome(
            action=ReplayEventAction.SNAPSHOT_APPLIED,
            valid_after=True,
            initialization_state=self._initialization_state,
            last_update_id=self._last_update_id,
            book_structure_after=self.book_structure,
            zero_quantity_change_count=zero_changes,
            levels_removed_count=0,
        )

    def _apply_update(
        self,
        event: OrderBookEvent,
        *,
        hour_utc: datetime,
    ) -> ReplayEventOutcome:
        if not self._valid:
            return ReplayEventOutcome(
                action=ReplayEventAction.SKIPPED_UNINITIALIZED,
                valid_after=False,
                initialization_state=self._initialization_state,
                last_update_id=None,
                book_structure_after=self.book_structure,
            )

        try:
            self._sequence_contract.validate_event(
                event_type=event.event_type.value,
                transaction_time_ms=event.transaction_time_ms,
                first_update_id=event.first_update_id,
                final_update_id=event.final_update_id,
                prev_final_update_id=event.prev_final_update_id,
                last_update_id=event.last_update_id,
            )
            event_predecessor = self._sequence_contract.update_predecessor_frontier(
                first_update_id=event.first_update_id,
                final_update_id=event.final_update_id,
                prev_final_update_id=event.prev_final_update_id,
                last_update_id=event.last_update_id,
            )
            event_frontier = self._sequence_contract.update_resulting_frontier(
                first_update_id=event.first_update_id,
                final_update_id=event.final_update_id,
                prev_final_update_id=event.prev_final_update_id,
                last_update_id=event.last_update_id,
            )
        except VenueSequenceContractError as exc:
            return self._invalidate(
                event=event,
                hour_utc=hour_utc,
                kind="update_sequence_contract_violation",
                message=str(exc),
            )

        previous_update_id = self._last_update_id

        if previous_update_id is None:
            return self._invalidate(
                event=event,
                hour_utc=hour_utc,
                kind="valid_state_missing_update_id",
                message=("Valid replay state unexpectedly lacks a final update ID"),
            )

        signature = _event_signature(event)

        if event_frontier == previous_update_id:
            if (
                self._last_update_signature is not None
                and signature == self._last_update_signature
            ):
                return ReplayEventOutcome(
                    action=ReplayEventAction.DUPLICATE_SKIPPED,
                    valid_after=True,
                    initialization_state=self._initialization_state,
                    last_update_id=self._last_update_id,
                    book_structure_after=self.book_structure,
                )

            return self._invalidate(
                event=event,
                hour_utc=hour_utc,
                kind="conflicting_replayed_update",
                message=(
                    "An update reused the current venue replay frontier "
                    "without matching the immediately preceding applied event"
                ),
            )

        if event_frontier < previous_update_id:
            return self._invalidate(
                event=event,
                hour_utc=hour_utc,
                kind="update_frontier_regression",
                message=(
                    "An update resulting frontier regressed behind the "
                    "current valid venue replay frontier"
                ),
            )

        if event_predecessor != previous_update_id:
            return self._invalidate(
                event=event,
                hour_utc=hour_utc,
                kind="update_continuity_mismatch",
                message=(
                    "The update's venue-specific predecessor frontier does "
                    "not match the current valid replay frontier"
                ),
            )

        zero_changes, removed_levels = self._apply_live_changes(event)

        self._last_update_id = event_frontier
        self._last_update_signature = signature

        return ReplayEventOutcome(
            action=ReplayEventAction.UPDATE_APPLIED,
            valid_after=True,
            initialization_state=self._initialization_state,
            last_update_id=self._last_update_id,
            book_structure_after=self.book_structure,
            zero_quantity_change_count=zero_changes,
            levels_removed_count=removed_levels,
        )

    def apply(
        self,
        event: OrderBookEvent,
        *,
        hour_utc: datetime,
    ) -> ReplayEventOutcome:
        """Apply or explicitly refuse one normalized order-book event."""
        source_hour = require_utc_hour("hour_utc", hour_utc)

        # received_time_ns is the locked authoritative sampling clock.
        # Record it before dispatch so invalidating and skipped events also
        # produce observations at the correct source-event time.
        self._last_event_received_time_ns = event.received_time_ns

        if event.symbol != self._symbol:
            return self._invalidate(
                event=event,
                hour_utc=source_hour,
                kind="event_symbol_mismatch",
                message=("Event symbol does not match the replay-state identity"),
            )

        if event.event_type is OrderBookEventType.SNAPSHOT:
            return self._apply_snapshot(
                event,
                hour_utc=source_hour,
            )

        if event.event_type is OrderBookEventType.UPDATE:
            return self._apply_update(
                event,
                hour_utc=source_hour,
            )

        return self._invalidate(
            event=event,
            hour_utc=source_hour,
            kind="unsupported_event_type",
            message="Replay received an unsupported event type",
        )

    def checkpoint(
        self,
        *,
        through_hour_utc: datetime,
        source_content_sha256: str | None = None,
    ) -> OrderBookCheckpoint:
        """Freeze the current complete state after one source hour."""
        if not self._valid or self._last_update_id is None:
            raise ReplayContractError(
                "Cannot create a checkpoint from invalid or uninitialized state"
            )

        if not self._bids or not self._asks:
            raise ReplayContractError(
                "Cannot create a checkpoint without both book sides"
            )

        levels: list[CheckpointLevel] = []

        for price in sorted(self._bids, reverse=True):
            quantity, order_count = self._bids[price]
            levels.append(
                CheckpointLevel(
                    side=BookSide.BID,
                    price=price,
                    quantity=quantity,
                    order_count=order_count,
                )
            )

        for price in sorted(self._asks):
            quantity, order_count = self._asks[price]
            levels.append(
                CheckpointLevel(
                    side=BookSide.ASK,
                    price=price,
                    quantity=quantity,
                    order_count=order_count,
                )
            )

        return OrderBookCheckpoint(
            provider=self._provider,
            venue=self._venue,
            symbol=self._symbol,
            through_hour_utc=through_hour_utc,
            last_update_id=self._last_update_id,
            levels=tuple(levels),
            source_content_sha256=source_content_sha256,
        )


def _validated_replay_sources(
    sources: tuple[ReplaySource, ...],
) -> tuple[ReplaySource, ...]:
    if not sources:
        raise ReplayContractError("At least one order-book source archive is required")

    normalized: list[ReplaySource] = []

    previous_spec: SourceFileSpec | None = None

    for raw_path, spec in sources:
        path = Path(raw_path).expanduser().resolve()

        if spec.data_kind is not SourceDataKind.ORDERBOOK:
            raise ReplayContractError(
                "Replay chains may contain only order-book sources"
            )

        if not path.is_file():
            raise ReplayContractError(f"Replay source is not a regular file: {path}")

        if previous_spec is not None:
            if (
                spec.provider != previous_spec.provider
                or spec.venue != previous_spec.venue
                or spec.symbol != previous_spec.symbol
            ):
                raise ReplayContractError(
                    "All replay-chain archives must share provider, venue, "
                    "and symbol identity"
                )

            expected_hour = previous_spec.hour_utc + timedelta(hours=1)

            if spec.hour_utc != expected_hour:
                raise ReplayContractError(
                    "Replay-chain archives must be adjacent UTC hours in "
                    "strict chronological order"
                )

        normalized.append((path, spec))
        previous_spec = spec

    return tuple(normalized)


def replay_orderbook_archives(
    sources: tuple[ReplaySource, ...],
    *,
    initial_checkpoint: OrderBookCheckpoint | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    final_source_content_sha256: str | None = None,
    cancellation_probe: ReplayCancellationProbe | None = None,
    cancellation_check_interval_rows: int = (DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS),
    archive_start_sink: ReplayArchiveStartSink | None = None,
    event_observation_sink: ReplayEventObservationSink | None = None,
    archive_start_state_sink: ReplayArchiveStateSink | None = None,
    before_event_state_sink: ReplayBeforeEventStateSink | None = None,
    archive_end_state_sink: ReplayArchiveStateSink | None = None,
) -> ReplayChainReport:
    """Replay one ordered, adjacent sequence of order-book archives.

    The function remains row-streamed through ``read_orderbook_file``. It does
    not retain a complete-hour event list.

    If no snapshot or valid initial checkpoint exists, update events are
    counted but deliberately not applied. The resulting chain has no final
    checkpoint.
    """
    validated_sources = _validated_replay_sources(sources)
    first_spec = validated_sources[0][1]

    if (
        isinstance(cancellation_check_interval_rows, bool)
        or not isinstance(cancellation_check_interval_rows, int)
        or cancellation_check_interval_rows <= 0
    ):
        raise ValueError("cancellation_check_interval_rows must be a positive integer")

    if cancellation_probe is not None:
        try:
            initially_cancelled = cancellation_probe()
        except Exception as exc:
            raise ReplayContractError("Replay cancellation probe failed") from exc

        if not isinstance(initially_cancelled, bool):
            raise ReplayContractError("Replay cancellation probe must return bool")

        if initially_cancelled:
            raise ReplayCancelledError(
                "Order-book replay was cancelled before it started"
            )

    if initial_checkpoint is None:
        state = OrderBookReplayState(
            provider=first_spec.provider,
            venue=first_spec.venue,
            symbol=first_spec.symbol,
        )
    else:
        state = OrderBookReplayState.from_checkpoint(
            initial_checkpoint,
            first_spec,
        )

    archive_reports: list[ReplayArchiveReport] = []

    for archive_index, (path, spec) in enumerate(validated_sources):
        if archive_index > 0:
            state.mark_carried()

        initial_state = state.initialization_state
        initially_valid = state.valid
        initial_update_id = state.last_update_id

        if archive_start_sink is not None:
            archive_start_sink(
                spec,
                state.observation(),
            )

        if archive_start_state_sink is not None:
            archive_start_state_sink(
                spec,
                state,
            )

        events_seen = 0
        snapshots_applied = 0
        updates_applied = 0
        skipped_uninitialized = 0
        duplicates_skipped = 0
        invalidations = 0
        recoveries = 0
        zero_changes = 0
        levels_removed = 0
        issues: list[ReplayIssue] = []

        structure_counts = {structure: 0 for structure in ReplayBookStructure}

        def consume(event: OrderBookEvent) -> None:
            nonlocal events_seen
            nonlocal snapshots_applied
            nonlocal updates_applied
            nonlocal skipped_uninitialized
            nonlocal duplicates_skipped
            nonlocal invalidations
            nonlocal recoveries
            nonlocal zero_changes
            nonlocal levels_removed

            if before_event_state_sink is not None:
                before_event_state_sink(
                    spec,
                    event,
                    state,
                )

            was_valid = state.valid
            events_seen += 1

            outcome = state.apply(
                event,
                hour_utc=spec.hour_utc,
            )

            zero_changes += outcome.zero_quantity_change_count
            levels_removed += outcome.levels_removed_count

            if outcome.action is ReplayEventAction.SNAPSHOT_APPLIED:
                snapshots_applied += 1

                if not was_valid:
                    recoveries += 1

            elif outcome.action is ReplayEventAction.UPDATE_APPLIED:
                updates_applied += 1

            elif outcome.action is ReplayEventAction.SKIPPED_UNINITIALIZED:
                skipped_uninitialized += 1

            elif outcome.action is ReplayEventAction.DUPLICATE_SKIPPED:
                duplicates_skipped += 1

            elif outcome.action is ReplayEventAction.INVALIDATED:
                invalidations += 1

            if outcome.action in {
                ReplayEventAction.SNAPSHOT_APPLIED,
                ReplayEventAction.UPDATE_APPLIED,
            }:
                structure = outcome.book_structure_after

                if structure is None:
                    raise ReplayContractError(
                        "Applied replay event did not expose book structure"
                    )

                structure_counts[structure] += 1

            if event_observation_sink is not None:
                event_observation_sink(
                    spec,
                    event,
                    outcome,
                    state.observation(),
                )

            if outcome.issue is not None and len(issues) < MAX_RETAINED_REPLAY_ISSUES:
                issues.append(outcome.issue)

        try:
            reader_report = read_orderbook_file(
                path,
                spec,
                consumer=consume,
                batch_size=batch_size,
                cancellation_probe=cancellation_probe,
                cancellation_check_interval_rows=(cancellation_check_interval_rows),
            )
        except StreamedParquetReadCancelled as exc:
            raise ReplayCancelledError(
                "Order-book replay was cancelled while reading " f"{spec.remote_path}"
            ) from exc

        if archive_end_state_sink is not None:
            archive_end_state_sink(
                spec,
                state,
            )

        archive_reports.append(
            ReplayArchiveReport(
                path=path,
                spec=spec,
                reader_report=reader_report,
                initial_state=initial_state,
                final_state=state.initialization_state,
                initially_valid=initially_valid,
                finally_valid=state.valid,
                events_seen=events_seen,
                snapshots_applied=snapshots_applied,
                updates_applied=updates_applied,
                updates_skipped_uninitialized=skipped_uninitialized,
                duplicate_updates_skipped=duplicates_skipped,
                invalidation_count=invalidations,
                recovery_count=recoveries,
                zero_quantity_change_count=zero_changes,
                levels_removed_count=levels_removed,
                initial_update_id=initial_update_id,
                final_update_id=state.last_update_id,
                final_bid_level_count=state.bid_level_count,
                final_ask_level_count=state.ask_level_count,
                structure_counts=tuple(
                    (
                        structure,
                        structure_counts[structure],
                    )
                    for structure in ReplayBookStructure
                ),
                retained_issues=tuple(issues),
                retained_issue_limit=MAX_RETAINED_REPLAY_ISSUES,
            )
        )

    final_spec = validated_sources[-1][1]

    final_checkpoint = (
        state.checkpoint(
            through_hour_utc=final_spec.hour_utc,
            source_content_sha256=final_source_content_sha256,
        )
        if state.checkpoint_eligible
        else None
    )

    return ReplayChainReport(
        provider=first_spec.provider,
        venue=first_spec.venue,
        symbol=first_spec.symbol,
        archives=tuple(archive_reports),
        final_checkpoint=final_checkpoint,
    )


__all__ = [
    "BookInitializationState",
    "CheckpointLevel",
    "MAX_RETAINED_REPLAY_ISSUES",
    "OrderBookCheckpoint",
    "OrderBookReplayState",
    "ReplayArchiveReport",
    "ReplayArchiveStartSink",
    "ReplayArchiveStateSink",
    "ReplayBeforeEventStateSink",
    "ReplayBookLevel",
    "ReplayBookObservation",
    "ReplayBookStructure",
    "ReplayCancellationProbe",
    "ReplayCancelledError",
    "ReplayChainReport",
    "ReplayContractError",
    "ReplayEventAction",
    "ReplayEventObservationSink",
    "ReplayEventOutcome",
    "ReplayIssue",
    "ReplaySource",
    "replay_orderbook_archives",
]

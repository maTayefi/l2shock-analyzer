from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    BookInitializationState,
    BookSide,
    CheckpointLevel,
    OrderBookCheckpoint,
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevelChange,
    OrderBookReplayState,
    ReplayBookStructure,
    ReplayContractError,
    ReplayEventAction,
    replay_orderbook_archives,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(offset),
    )


def _change(
    side: BookSide | str,
    price: str,
    quantity: str,
) -> OrderBookLevelChange:
    return OrderBookLevelChange(
        side=BookSide(side),
        price=Decimal(price),
        quantity=Decimal(quantity),
        order_count=None,
    )


def _snapshot(
    *,
    last_update_id: int,
    changes: tuple[OrderBookLevelChange, ...],
    row_number: int = 0,
) -> OrderBookEvent:
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=OrderBookEventType.SNAPSHOT,
        received_time_ns=1_788_350_400_000_000_000 + row_number,
        event_time_ms=1_788_350_400_000 + row_number,
        transaction_time_ms=1_788_350_400_000 + row_number,
        first_update_id=None,
        final_update_id=None,
        prev_final_update_id=None,
        last_update_id=last_update_id,
        changes=changes,
        first_row_number=row_number,
        last_row_number=row_number + len(changes) - 1,
    )


def _update(
    *,
    first_update_id: int,
    final_update_id: int,
    prev_final_update_id: int,
    changes: tuple[OrderBookLevelChange, ...],
    row_number: int = 0,
) -> OrderBookEvent:
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=OrderBookEventType.UPDATE,
        received_time_ns=1_788_350_400_100_000_000 + row_number,
        event_time_ms=1_788_350_400_100 + row_number,
        transaction_time_ms=1_788_350_400_099 + row_number,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        prev_final_update_id=prev_final_update_id,
        last_update_id=None,
        changes=changes,
        first_row_number=row_number,
        last_row_number=row_number + len(changes) - 1,
    )


def _state() -> OrderBookReplayState:
    return OrderBookReplayState(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
    )


def _row(
    *,
    event_type: str,
    received_time: int,
    event_time: int,
    transaction_time: int,
    side: str,
    price: str,
    quantity: str,
    first_update_id: int | None,
    final_update_id: int | None,
    prev_final_update_id: int | None,
    last_update_id: int | None,
) -> dict[str, object]:
    return {
        "received_time": received_time,
        "event_time": event_time,
        "transaction_time": transaction_time,
        "symbol": "BTCUSDT",
        "event_type": event_type,
        "first_update_id": first_update_id,
        "final_update_id": final_update_id,
        "prev_final_update_id": prev_final_update_id,
        "last_update_id": last_update_id,
        "side": side,
        "price": price,
        "quantity": quantity,
        "order_count": None,
    }


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
        row_group_size=1,
    )


def test_update_only_stream_cannot_initialize_book() -> None:
    state = _state()

    outcome = state.apply(
        _update(
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    assert outcome.action is ReplayEventAction.SKIPPED_UNINITIALIZED
    assert state.valid is False
    assert state.last_update_id is None
    assert state.bid_level_count == 0
    assert state.ask_level_count == 0

    with pytest.raises(
        ReplayContractError,
        match="invalid or uninitialized",
    ):
        state.checkpoint(through_hour_utc=_hour())


def test_complete_snapshot_initializes_and_update_mutates_book() -> None:
    state = _state()

    snapshot_result = state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    assert snapshot_result.action is ReplayEventAction.SNAPSHOT_APPLIED
    assert state.valid is True
    assert state.initialization_state is BookInitializationState.SNAPSHOT
    assert state.last_update_id == 100
    assert state.best_bid == Decimal("59999.00")
    assert state.best_ask == Decimal("60001.00")

    update_result = state.apply(
        _update(
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            changes=(
                _change("bid", "60000.00", "3.0"),
                _change("ask", "60001.00", "2.0"),
            ),
            row_number=2,
        ),
        hour_utc=_hour(),
    )

    assert update_result.action is ReplayEventAction.UPDATE_APPLIED
    assert state.last_update_id == 103
    assert state.best_bid == Decimal("60000.00")
    assert state.quantity_at(
        BookSide.ASK,
        Decimal("60001.00"),
    ) == Decimal("2.0")


def test_quantity_zero_removes_existing_level() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("bid", "59998.00", "4.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    result = state.apply(
        _update(
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
            changes=(_change("bid", "59999.00", "0.000"),),
            row_number=3,
        ),
        hour_utc=_hour(),
    )

    assert result.action is ReplayEventAction.UPDATE_APPLIED
    assert result.zero_quantity_change_count == 1
    assert result.levels_removed_count == 1
    assert (
        state.quantity_at(
            BookSide.BID,
            Decimal("59999.00"),
        )
        is None
    )
    assert state.best_bid == Decimal("59998.00")


def test_continuity_failure_discards_complete_state() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    result = state.apply(
        _update(
            first_update_id=110,
            final_update_id=112,
            prev_final_update_id=109,
            changes=(_change("bid", "60000.00", "3.0"),),
            row_number=2,
        ),
        hour_utc=_hour(),
    )

    assert result.action is ReplayEventAction.INVALIDATED
    assert result.issue is not None
    assert result.issue.kind == "update_continuity_mismatch"
    assert result.issue.previous_update_id == 100
    assert result.issue.current_update_id == 112

    assert state.valid is False
    assert state.initialization_state is BookInitializationState.INVALIDATED
    assert state.last_update_id is None
    assert state.bid_level_count == 0
    assert state.ask_level_count == 0


def test_updates_after_invalidation_remain_unapplied_until_snapshot() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    state.apply(
        _update(
            first_update_id=110,
            final_update_id=112,
            prev_final_update_id=109,
            changes=(_change("bid", "60000.00", "3.0"),),
        ),
        hour_utc=_hour(),
    )

    skipped = state.apply(
        _update(
            first_update_id=113,
            final_update_id=115,
            prev_final_update_id=112,
            changes=(_change("ask", "60002.00", "4.0"),),
        ),
        hour_utc=_hour(),
    )

    assert skipped.action is ReplayEventAction.SKIPPED_UNINITIALIZED
    assert state.valid is False

    recovered = state.apply(
        _snapshot(
            last_update_id=500,
            changes=(
                _change("bid", "59000.00", "5.0"),
                _change("ask", "61000.00", "6.0"),
            ),
            row_number=5,
        ),
        hour_utc=_hour(),
    )

    assert recovered.action is ReplayEventAction.SNAPSHOT_APPLIED
    assert state.valid is True
    assert state.last_update_id == 500
    assert state.best_bid == Decimal("59000.00")
    assert state.best_ask == Decimal("61000.00")


def test_exact_adjacent_duplicate_update_is_skipped() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    event = _update(
        first_update_id=101,
        final_update_id=103,
        prev_final_update_id=100,
        changes=(_change("bid", "60000.00", "3.0"),),
        row_number=2,
    )

    first = state.apply(event, hour_utc=_hour())
    duplicate = state.apply(event, hour_utc=_hour())

    assert first.action is ReplayEventAction.UPDATE_APPLIED
    assert duplicate.action is ReplayEventAction.DUPLICATE_SKIPPED
    assert state.valid is True
    assert state.last_update_id == 103


def test_conflicting_same_final_update_id_invalidates() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    state.apply(
        _update(
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            changes=(_change("bid", "60000.00", "3.0"),),
            row_number=2,
        ),
        hour_utc=_hour(),
    )

    conflict = state.apply(
        _update(
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            changes=(_change("bid", "60000.00", "4.0"),),
            row_number=3,
        ),
        hour_utc=_hour(),
    )

    assert conflict.action is ReplayEventAction.INVALIDATED
    assert conflict.issue is not None
    assert conflict.issue.kind == "conflicting_replayed_update"
    assert state.valid is False


def test_checkpoint_requires_immediately_following_hour() -> None:
    checkpoint = OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(),
        last_update_id=100,
        levels=(
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("59999.00"),
                quantity=Decimal("2.0"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("60001.00"),
                quantity=Decimal("1.5"),
                order_count=None,
            ),
        ),
    )

    checkpoint.validate_for_source(_spec(1))

    with pytest.raises(
        ReplayContractError,
        match="immediately following",
    ):
        checkpoint.validate_for_source(_spec(2))


def test_cross_hour_chain_carries_state_and_checks_boundary(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "hour_12.parquet"
    second_path = tmp_path / "hour_13.parquet"

    first_rows = [
        _row(
            event_type="snapshot",
            received_time=1_788_350_400_000_000_000,
            event_time=1_788_350_400_000,
            transaction_time=1_788_350_400_000,
            side="bid",
            price="59999.00",
            quantity="2.0",
            first_update_id=None,
            final_update_id=None,
            prev_final_update_id=None,
            last_update_id=100,
        ),
        _row(
            event_type="snapshot",
            received_time=1_788_350_400_000_000_000,
            event_time=1_788_350_400_000,
            transaction_time=1_788_350_400_000,
            side="ask",
            price="60001.00",
            quantity="1.5",
            first_update_id=None,
            final_update_id=None,
            prev_final_update_id=None,
            last_update_id=100,
        ),
        _row(
            event_type="update",
            received_time=1_788_350_400_100_000_000,
            event_time=1_788_350_400_100,
            transaction_time=1_788_350_400_099,
            side="bid",
            price="60000.00",
            quantity="3.0",
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            last_update_id=None,
        ),
    ]

    second_rows = [
        _row(
            event_type="update",
            received_time=1_788_354_000_001_000_000,
            event_time=1_788_354_000_001,
            transaction_time=1_788_354_000_000,
            side="ask",
            price="60002.00",
            quantity="4.0",
            first_update_id=104,
            final_update_id=106,
            prev_final_update_id=103,
            last_update_id=None,
        )
    ]

    _write(first_path, first_rows)
    _write(second_path, second_rows)

    report = replay_orderbook_archives(
        (
            (first_path, _spec(0)),
            (second_path, _spec(1)),
        ),
        batch_size=1,
    )

    assert report.finally_valid is True
    assert report.final_checkpoint is not None
    assert report.final_checkpoint.last_update_id == 106
    assert report.final_checkpoint.through_hour_utc == _hour(1)

    first_report, second_report = report.archives

    assert first_report.independently_initialized is True
    assert first_report.finally_valid is True

    assert second_report.initial_state is BookInitializationState.CARRIED
    assert second_report.required_carried_state is True
    assert second_report.updates_applied == 1
    assert second_report.finally_valid is True


def test_cross_hour_boundary_mismatch_invalidates_carried_state(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "hour_12.parquet"
    second_path = tmp_path / "hour_13.parquet"

    _write(
        first_path,
        [
            _row(
                event_type="snapshot",
                received_time=1_788_350_400_000_000_000,
                event_time=1_788_350_400_000,
                transaction_time=1_788_350_400_000,
                side="bid",
                price="59999.00",
                quantity="2.0",
                first_update_id=None,
                final_update_id=None,
                prev_final_update_id=None,
                last_update_id=100,
            ),
            _row(
                event_type="snapshot",
                received_time=1_788_350_400_000_000_000,
                event_time=1_788_350_400_000,
                transaction_time=1_788_350_400_000,
                side="ask",
                price="60001.00",
                quantity="1.5",
                first_update_id=None,
                final_update_id=None,
                prev_final_update_id=None,
                last_update_id=100,
            ),
        ],
    )

    _write(
        second_path,
        [
            _row(
                event_type="update",
                received_time=1_788_354_000_001_000_000,
                event_time=1_788_354_000_001,
                transaction_time=1_788_354_000_000,
                side="bid",
                price="60000.00",
                quantity="3.0",
                first_update_id=120,
                final_update_id=122,
                prev_final_update_id=119,
                last_update_id=None,
            )
        ],
    )

    report = replay_orderbook_archives(
        (
            (first_path, _spec(0)),
            (second_path, _spec(1)),
        ),
        batch_size=1,
    )

    assert report.finally_valid is False
    assert report.final_checkpoint is None

    second_report = report.archives[1]

    assert second_report.initially_valid is True
    assert second_report.finally_valid is False
    assert second_report.invalidation_count == 1
    assert second_report.retained_issues[0].kind == ("update_continuity_mismatch")


def test_update_only_archive_report_explicitly_refuses_initialization(
    tmp_path: Path,
) -> None:
    path = tmp_path / "update_only.parquet"

    _write(
        path,
        [
            _row(
                event_type="update",
                received_time=1_788_350_400_001_000_000,
                event_time=1_788_350_400_001,
                transaction_time=1_788_350_400_000,
                side="bid",
                price="59999.00",
                quantity="2.0",
                first_update_id=101,
                final_update_id=103,
                prev_final_update_id=100,
                last_update_id=None,
            )
        ],
    )

    report = replay_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    archive = report.archives[0]

    assert archive.events_seen == 1
    assert archive.snapshots_applied == 0
    assert archive.updates_applied == 0
    assert archive.updates_skipped_uninitialized == 1
    assert archive.finally_valid is False
    assert archive.final_update_id is None
    assert report.final_checkpoint is None


def test_nonadjacent_archive_chain_is_rejected(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "first.parquet"
    third_path = tmp_path / "third.parquet"

    rows = [
        _row(
            event_type="update",
            received_time=1_788_350_400_001_000_000,
            event_time=1_788_350_400_001,
            transaction_time=1_788_350_400_000,
            side="bid",
            price="59999.00",
            quantity="2.0",
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            last_update_id=None,
        )
    ]

    _write(first_path, rows)
    _write(third_path, rows)

    with pytest.raises(
        ReplayContractError,
        match="adjacent UTC hours",
    ):
        replay_orderbook_archives(
            (
                (first_path, _spec(0)),
                (third_path, _spec(2)),
            )
        )


def test_best_price_indexes_track_insert_delete_and_reinsert() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("bid", "59998.00", "3.0"),
                _change("ask", "60001.00", "1.5"),
                _change("ask", "60002.00", "2.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    assert state.best_bid == Decimal("59999.00")
    assert state.best_ask == Decimal("60001.00")
    assert state.book_structure is ReplayBookStructure.NORMAL

    inserted = state.apply(
        _update(
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
            changes=(
                _change("bid", "60000.00", "4.0"),
                _change("ask", "60000.50", "5.0"),
            ),
            row_number=2,
        ),
        hour_utc=_hour(),
    )

    assert inserted.action is ReplayEventAction.UPDATE_APPLIED
    assert state.best_bid == Decimal("60000.00")
    assert state.best_ask == Decimal("60000.50")
    assert state.book_structure is ReplayBookStructure.NORMAL

    removed = state.apply(
        _update(
            first_update_id=102,
            final_update_id=102,
            prev_final_update_id=101,
            changes=(
                _change("bid", "60000.00", "0"),
                _change("ask", "60000.50", "0"),
            ),
            row_number=3,
        ),
        hour_utc=_hour(),
    )

    assert removed.action is ReplayEventAction.UPDATE_APPLIED
    assert removed.zero_quantity_change_count == 2
    assert removed.levels_removed_count == 2
    assert state.best_bid == Decimal("59999.00")
    assert state.best_ask == Decimal("60001.00")

    reinserted = state.apply(
        _update(
            first_update_id=103,
            final_update_id=103,
            prev_final_update_id=102,
            changes=(
                _change("bid", "60000.00", "6.0"),
                _change("ask", "60000.50", "7.0"),
            ),
            row_number=4,
        ),
        hour_utc=_hour(),
    )

    assert reinserted.action is ReplayEventAction.UPDATE_APPLIED
    assert state.best_bid == Decimal("60000.00")
    assert state.best_ask == Decimal("60000.50")
    assert state.quantity_at(
        BookSide.BID,
        Decimal("60000.00"),
    ) == Decimal("6.0")
    assert state.quantity_at(
        BookSide.ASK,
        Decimal("60000.50"),
    ) == Decimal("7.0")


def test_snapshot_replacement_rebuilds_price_indexes() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "70000.00", "2.0"),
                _change("bid", "69999.00", "3.0"),
                _change("ask", "70001.00", "1.5"),
                _change("ask", "70002.00", "2.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    assert state.best_bid == Decimal("70000.00")
    assert state.best_ask == Decimal("70001.00")

    replacement = state.apply(
        _snapshot(
            last_update_id=500,
            changes=(
                _change("bid", "59000.00", "5.0"),
                _change("bid", "58999.00", "6.0"),
                _change("ask", "61000.00", "7.0"),
                _change("ask", "61001.00", "8.0"),
            ),
            row_number=5,
        ),
        hour_utc=_hour(),
    )

    assert replacement.action is ReplayEventAction.SNAPSHOT_APPLIED
    assert state.last_update_id == 500
    assert state.best_bid == Decimal("59000.00")
    assert state.best_ask == Decimal("61000.00")
    assert (
        state.quantity_at(
            BookSide.BID,
            Decimal("70000.00"),
        )
        is None
    )
    assert (
        state.quantity_at(
            BookSide.ASK,
            Decimal("70001.00"),
        )
        is None
    )
    assert state.bid_level_count == 2
    assert state.ask_level_count == 2


def test_checkpoint_restoration_rebuilds_best_price_indexes() -> None:
    checkpoint = OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(),
        last_update_id=100,
        levels=(
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("59998.00"),
                quantity=Decimal("3.0"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("59999.00"),
                quantity=Decimal("2.0"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("60002.00"),
                quantity=Decimal("2.5"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("60001.00"),
                quantity=Decimal("1.5"),
                order_count=None,
            ),
        ),
    )

    state = OrderBookReplayState.from_checkpoint(
        checkpoint,
        _spec(1),
    )

    assert state.valid is True
    assert state.initialization_state is BookInitializationState.CARRIED
    assert state.last_update_id == 100
    assert state.best_bid == Decimal("59999.00")
    assert state.best_ask == Decimal("60001.00")
    assert state.book_structure is ReplayBookStructure.NORMAL

    observation = state.observation()

    assert observation.last_event_received_time_ns is None
    assert observation.valid is True
    assert observation.best_bid == Decimal("59999.00")
    assert observation.best_ask == Decimal("60001.00")
    assert observation.bid_level_count == 2
    assert observation.ask_level_count == 2
    assert observation.checkpoint_eligible is True


def test_invalidation_clears_levels_and_price_indexes() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "59999.00", "2.0"),
                _change("ask", "60001.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    invalidating_event = _update(
        first_update_id=110,
        final_update_id=112,
        prev_final_update_id=109,
        changes=(_change("bid", "60000.00", "3.0"),),
        row_number=2,
    )

    result = state.apply(
        invalidating_event,
        hour_utc=_hour(),
    )

    assert result.action is ReplayEventAction.INVALIDATED
    assert state.valid is False
    assert state.best_bid is None
    assert state.best_ask is None
    assert state.book_structure is ReplayBookStructure.EMPTY_BOTH

    observation = state.observation()

    assert observation.last_event_received_time_ns == (
        invalidating_event.received_time_ns
    )
    assert observation.valid is False
    assert observation.initialization_state is (BookInitializationState.INVALIDATED)
    assert observation.last_update_id is None
    assert observation.book_structure is ReplayBookStructure.EMPTY_BOTH
    assert observation.best_bid is None
    assert observation.best_ask is None
    assert observation.bid_level_count == 0
    assert observation.ask_level_count == 0
    assert observation.checkpoint_eligible is False


def test_observation_is_immutable_and_uses_received_time_ns() -> None:
    from dataclasses import FrozenInstanceError

    state = _state()
    event = _snapshot(
        last_update_id=100,
        changes=(
            _change("bid", "59999.00", "2.0"),
            _change("ask", "60001.00", "1.5"),
        ),
    )

    state.apply(event, hour_utc=_hour())
    observation = state.observation()

    assert observation.last_event_received_time_ns == event.received_time_ns
    assert observation.valid is True
    assert observation.initialization_state is (BookInitializationState.SNAPSHOT)
    assert observation.last_update_id == 100
    assert observation.book_structure is ReplayBookStructure.NORMAL
    assert observation.best_bid == Decimal("59999.00")
    assert observation.best_ask == Decimal("60001.00")
    assert observation.bid_level_count == 1
    assert observation.ask_level_count == 1
    assert observation.checkpoint_eligible is True

    with pytest.raises(FrozenInstanceError):
        setattr(observation, "valid", False)


def test_observation_reports_locked_and_crossed_structure() -> None:
    locked_state = _state()

    locked_state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "60000.00", "2.0"),
                _change("ask", "60000.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    locked = locked_state.observation()

    assert locked.valid is True
    assert locked.book_structure is ReplayBookStructure.LOCKED
    assert locked.best_bid == Decimal("60000.00")
    assert locked.best_ask == Decimal("60000.00")

    crossed_state = _state()

    crossed_state.apply(
        _snapshot(
            last_update_id=200,
            changes=(
                _change("bid", "60001.00", "2.0"),
                _change("ask", "60000.00", "1.5"),
            ),
        ),
        hour_utc=_hour(),
    )

    crossed = crossed_state.observation()

    assert crossed.valid is True
    assert crossed.book_structure is ReplayBookStructure.CROSSED
    assert crossed.best_bid == Decimal("60001.00")
    assert crossed.best_ask == Decimal("60000.00")


def test_top_of_book_reads_do_not_scan_level_dictionaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.ingest.replay as replay_module

    state = _state()

    bid_changes = tuple(
        _change(
            "bid",
            str(Decimal("50000.00") - Decimal(index)),
            "1.0",
        )
        for index in range(4096)
    )
    ask_changes = tuple(
        _change(
            "ask",
            str(Decimal("60001.00") + Decimal(index)),
            "1.0",
        )
        for index in range(4096)
    )

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=bid_changes + ask_changes,
        ),
        hour_utc=_hour(),
    )

    assert state.bid_level_count == 4096
    assert state.ask_level_count == 4096
    assert state.best_bid == Decimal("50000.00")
    assert state.best_ask == Decimal("60001.00")

    def fail_if_full_scan_is_attempted(
        *args: object,
        **kwargs: object,
    ) -> object:
        raise AssertionError("Top-of-book observation attempted a full max/min scan")

    # The former implementation resolved max/min from this module's global
    # namespace through builtins. Installing module globals makes any retained
    # max(self._bids) or min(self._asks) call fail deterministically.
    monkeypatch.setattr(
        replay_module,
        "max",
        fail_if_full_scan_is_attempted,
        raising=False,
    )
    monkeypatch.setattr(
        replay_module,
        "min",
        fail_if_full_scan_is_attempted,
        raising=False,
    )

    for _ in range(10_000):
        assert state.best_bid == Decimal("50000.00")
        assert state.best_ask == Decimal("60001.00")

        observation = state.observation()

        assert observation.best_bid == Decimal("50000.00")
        assert observation.best_ask == Decimal("60001.00")
        assert observation.book_structure is ReplayBookStructure.NORMAL


def test_level_range_iteration_is_inclusive_and_bounded() -> None:
    state = _state()

    state.apply(
        _snapshot(
            last_update_id=100,
            changes=(
                _change("bid", "100", "1"),
                _change("bid", "99", "2"),
                _change("bid", "98", "3"),
                _change("bid", "97", "4"),
                _change("ask", "101", "5"),
                _change("ask", "102", "6"),
                _change("ask", "103", "7"),
                _change("ask", "104", "8"),
            ),
        ),
        hour_utc=_hour(),
    )

    bids = tuple(
        state.iter_levels(
            BookSide.BID,
            lower_price=Decimal("98"),
            upper_price=Decimal("99"),
        )
    )
    asks = tuple(
        state.iter_levels(
            BookSide.ASK,
            lower_price=Decimal("102"),
            upper_price=Decimal("103"),
        )
    )

    assert [(level.price, level.quantity) for level in bids] == [
        (Decimal("98"), Decimal("3")),
        (Decimal("99"), Decimal("2")),
    ]

    assert [(level.price, level.quantity) for level in asks] == [
        (Decimal("102"), Decimal("6")),
        (Decimal("103"), Decimal("7")),
    ]


def test_state_query_callbacks_observe_safe_replay_boundaries(
    tmp_path: Path,
) -> None:
    path = tmp_path / "callback_boundaries.parquet"

    rows = [
        _row(
            event_type="snapshot",
            received_time=1_788_350_400_100_000_000,
            event_time=1_788_350_400_100,
            transaction_time=1_788_350_400_099,
            side="bid",
            price="100",
            quantity="1",
            first_update_id=None,
            final_update_id=None,
            prev_final_update_id=None,
            last_update_id=100,
        ),
        _row(
            event_type="snapshot",
            received_time=1_788_350_400_100_000_000,
            event_time=1_788_350_400_100,
            transaction_time=1_788_350_400_099,
            side="ask",
            price="101",
            quantity="1",
            first_update_id=None,
            final_update_id=None,
            prev_final_update_id=None,
            last_update_id=100,
        ),
        _row(
            event_type="update",
            received_time=1_788_350_401_000_000_000,
            event_time=1_788_350_401_000,
            transaction_time=1_788_350_400_999,
            side="bid",
            price="100",
            quantity="5",
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
            last_update_id=None,
        ),
    ]

    _write(path, rows)

    observed: list[tuple[str, int | None, Decimal | None]] = []

    def start_state(
        _spec_value: SourceFileSpec,
        state: OrderBookReplayState,
    ) -> None:
        observed.append(("start", state.last_update_id, state.best_bid))

    def before_event(
        _spec_value: SourceFileSpec,
        event: OrderBookEvent,
        state: OrderBookReplayState,
    ) -> None:
        observed.append(
            (
                f"before:{event.event_type.value}",
                state.last_update_id,
                state.quantity_at(
                    BookSide.BID,
                    Decimal("100"),
                ),
            )
        )

    def end_state(
        _spec_value: SourceFileSpec,
        state: OrderBookReplayState,
    ) -> None:
        observed.append(
            (
                "end",
                state.last_update_id,
                state.quantity_at(
                    BookSide.BID,
                    Decimal("100"),
                ),
            )
        )

    replay_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
        archive_start_state_sink=start_state,
        before_event_state_sink=before_event,
        archive_end_state_sink=end_state,
    )

    assert observed == [
        ("start", None, None),
        ("before:snapshot", None, None),
        ("before:update", 100, Decimal("1")),
        ("end", 101, Decimal("5")),
    ]

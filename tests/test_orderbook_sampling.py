from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.ingest import (
    OBSERVATIONS_PER_HOUR,
    BookInitializationState,
    BookSampleInvalidReason,
    BookSampleQuality,
    BookSide,
    CheckpointLevel,
    OrderBookCheckpoint,
    ReplayBookStructure,
    sample_orderbook_archives,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _epoch_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _received_ns(
    *,
    seconds: int = 0,
    milliseconds: int = 0,
    hour_offset: int = 0,
) -> int:
    return _epoch_ns(
        _hour(hour_offset)
        + timedelta(
            seconds=seconds,
            milliseconds=milliseconds,
        )
    )


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(offset),
    )


def _row(
    *,
    received_time: int,
    event_type: str,
    side: str,
    price: str,
    quantity: str,
    first_update_id: int | None = None,
    final_update_id: int | None = None,
    prev_final_update_id: int | None = None,
    last_update_id: int | None = None,
) -> dict[str, object]:
    event_time_ms = received_time // 1_000_000

    return {
        "received_time": received_time,
        "event_time": event_time_ms,
        "transaction_time": event_time_ms,
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


def _snapshot_rows(
    *,
    received_time: int,
    last_update_id: int,
    bid: str = "100",
    ask: str = "101",
) -> list[dict[str, object]]:
    return [
        _row(
            received_time=received_time,
            event_type="snapshot",
            side="bid",
            price=bid,
            quantity="1",
            last_update_id=last_update_id,
        ),
        _row(
            received_time=received_time,
            event_type="snapshot",
            side="ask",
            price=ask,
            quantity="1",
            last_update_id=last_update_id,
        ),
    ]


def _update_row(
    *,
    received_time: int,
    first_update_id: int,
    final_update_id: int,
    prev_final_update_id: int,
    side: str,
    price: str,
    quantity: str,
) -> dict[str, object]:
    return _row(
        received_time=received_time,
        event_type="update",
        side=side,
        price=price,
        quantity=quantity,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        prev_final_update_id=prev_final_update_id,
    )


def _write(
    path: Path,
    rows: list[dict[str, object]],
) -> None:
    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
        row_group_size=1,
    )


def test_sampling_produces_exactly_3600_observations(
    tmp_path: Path,
) -> None:
    path = tmp_path / "snapshot.parquet"

    _write(
        path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=100,
        ),
    )

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    assert len(result.hours) == 1

    sampled = result.hours[0]

    assert len(sampled.observations) == OBSERVATIONS_PER_HOUR
    assert sampled.valid_count == OBSERVATIONS_PER_HOUR
    assert sampled.invalid_count == 0
    assert sampled.fully_valid is True

    first = sampled.observations[0]
    last = sampled.observations[-1]

    assert first.bucket_start_utc == _hour()
    assert first.bucket_end_utc == (_hour() + timedelta(seconds=1))

    assert last.bucket_start_utc == (_hour() + timedelta(seconds=3599))
    assert last.bucket_end_utc == (_hour() + timedelta(hours=1))


def test_pre_snapshot_seconds_are_invalid_then_state_becomes_valid(
    tmp_path: Path,
) -> None:
    path = tmp_path / "late_snapshot.parquet"

    snapshot_time = _received_ns(
        seconds=1,
        milliseconds=500,
    )

    _write(
        path,
        _snapshot_rows(
            received_time=snapshot_time,
            last_update_id=100,
        ),
    )

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    observations = result.hours[0].observations
    first = observations[0]
    second = observations[1]

    # Bucket [12:00:00, 12:00:01] ends before the snapshot at 1.5 s.
    assert first.quality is BookSampleQuality.INVALID
    assert first.invalid_reason is (BookSampleInvalidReason.UNINITIALIZED)
    assert first.best_bid is None
    assert first.best_ask is None

    # Bucket ending at 12:00:02 includes the snapshot.
    assert second.quality is BookSampleQuality.VALID
    assert second.invalid_reason is None
    assert second.source_received_time_ns == snapshot_time
    assert second.best_bid == Decimal("100")
    assert second.best_ask == Decimal("101")


def test_event_exactly_at_bucket_end_is_included(
    tmp_path: Path,
) -> None:
    path = tmp_path / "boundary.parquet"

    snapshot_time = _received_ns(milliseconds=100)
    update_time = _received_ns(seconds=1)

    rows = _snapshot_rows(
        received_time=snapshot_time,
        last_update_id=100,
    )
    rows.append(
        _update_row(
            received_time=update_time,
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
            side="bid",
            price="100.5",
            quantity="2",
        )
    )

    _write(path, rows)

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    first = result.hours[0].observations[0]

    assert first.bucket_end_utc == (_hour() + timedelta(seconds=1))
    assert first.source_received_time_ns == update_time
    assert first.last_update_id == 101
    assert first.best_bid == Decimal("100.5")
    assert first.best_ask == Decimal("101")
    assert first.quality is BookSampleQuality.VALID


def test_all_events_with_same_receive_time_precede_boundary_sample(
    tmp_path: Path,
) -> None:
    path = tmp_path / "same_timestamp.parquet"

    snapshot_time = _received_ns(milliseconds=100)
    boundary_time = _received_ns(seconds=1)

    rows = _snapshot_rows(
        received_time=snapshot_time,
        last_update_id=100,
    )
    rows.extend(
        [
            _update_row(
                received_time=boundary_time,
                first_update_id=101,
                final_update_id=101,
                prev_final_update_id=100,
                side="bid",
                price="100.25",
                quantity="2",
            ),
            _update_row(
                received_time=boundary_time,
                first_update_id=102,
                final_update_id=102,
                prev_final_update_id=101,
                side="bid",
                price="100.75",
                quantity="3",
            ),
        ]
    )

    _write(path, rows)

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    first = result.hours[0].observations[0]

    assert first.source_received_time_ns == boundary_time
    assert first.last_update_id == 102
    assert first.best_bid == Decimal("100.75")
    assert first.quality is BookSampleQuality.VALID


def test_quiet_seconds_carry_latest_valid_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "quiet.parquet"

    snapshot_time = _received_ns(milliseconds=100)

    _write(
        path,
        _snapshot_rows(
            received_time=snapshot_time,
            last_update_id=100,
            bid="99",
            ask="101",
        ),
    )

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    observations = result.hours[0].observations

    for index in (0, 1, 59, 600, 3599):
        observation = observations[index]

        assert observation.quality is BookSampleQuality.VALID
        assert observation.source_received_time_ns == snapshot_time
        assert observation.best_bid == Decimal("99")
        assert observation.best_ask == Decimal("101")
        assert observation.last_update_id == 100


def test_invalidation_applies_at_its_bucket_boundary_and_persists(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalidated.parquet"

    snapshot_time = _received_ns(milliseconds=100)
    invalidation_time = _received_ns(seconds=2)

    rows = _snapshot_rows(
        received_time=snapshot_time,
        last_update_id=100,
    )
    rows.append(
        _update_row(
            received_time=invalidation_time,
            first_update_id=110,
            final_update_id=112,
            prev_final_update_id=109,
            side="bid",
            price="100.5",
            quantity="2",
        )
    )

    _write(path, rows)

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    observations = result.hours[0].observations

    # Bucket ending at one second still uses the valid snapshot.
    assert observations[0].quality is BookSampleQuality.VALID

    # Event at exactly two seconds invalidates the bucket ending at two.
    assert observations[1].quality is BookSampleQuality.INVALID
    assert observations[1].invalid_reason is (
        BookSampleInvalidReason.REPLAY_INVALIDATED
    )
    assert observations[1].source_received_time_ns == invalidation_time
    assert observations[1].best_bid is None
    assert observations[1].best_ask is None

    # Quiet time cannot repair invalidated replay state.
    assert observations[-1].quality is BookSampleQuality.INVALID
    assert observations[-1].invalid_reason is (
        BookSampleInvalidReason.REPLAY_INVALIDATED
    )


def test_later_complete_snapshot_recovers_sampling_validity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "recovered.parquet"

    rows = _snapshot_rows(
        received_time=_received_ns(milliseconds=100),
        last_update_id=100,
    )
    rows.append(
        _update_row(
            received_time=_received_ns(seconds=2),
            first_update_id=110,
            final_update_id=112,
            prev_final_update_id=109,
            side="bid",
            price="100.5",
            quantity="2",
        )
    )
    rows.extend(
        _snapshot_rows(
            received_time=_received_ns(
                seconds=3,
                milliseconds=500,
            ),
            last_update_id=500,
            bid="90",
            ask="110",
        )
    )

    _write(path, rows)

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    observations = result.hours[0].observations

    assert observations[0].quality is BookSampleQuality.VALID
    assert observations[1].quality is BookSampleQuality.INVALID
    assert observations[2].quality is BookSampleQuality.INVALID

    # Snapshot at 3.5 seconds is visible in the bucket ending at 4 seconds.
    assert observations[3].quality is BookSampleQuality.VALID
    assert observations[3].last_update_id == 500
    assert observations[3].best_bid == Decimal("90")
    assert observations[3].best_ask == Decimal("110")


def test_locked_and_crossed_books_are_invalid_samples(
    tmp_path: Path,
) -> None:
    locked_path = tmp_path / "locked.parquet"
    crossed_path = tmp_path / "crossed.parquet"

    _write(
        locked_path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=100,
            bid="100",
            ask="100",
        ),
    )
    _write(
        crossed_path,
        _snapshot_rows(
            received_time=_received_ns(milliseconds=100),
            last_update_id=200,
            bid="101",
            ask="100",
        ),
    )

    locked_result = sample_orderbook_archives(
        ((locked_path, _spec()),),
        batch_size=1,
    )
    crossed_result = sample_orderbook_archives(
        ((crossed_path, _spec()),),
        batch_size=1,
    )

    locked = locked_result.hours[0].observations[0]
    crossed = crossed_result.hours[0].observations[0]

    assert locked.replay_valid is True
    assert locked.book_structure is ReplayBookStructure.LOCKED
    assert locked.quality is BookSampleQuality.INVALID
    assert locked.invalid_reason is BookSampleInvalidReason.LOCKED

    assert crossed.replay_valid is True
    assert crossed.book_structure is ReplayBookStructure.CROSSED
    assert crossed.quality is BookSampleQuality.INVALID
    assert crossed.invalid_reason is BookSampleInvalidReason.CROSSED


def test_empty_side_remains_sequence_valid_but_sample_invalid(
    tmp_path: Path,
) -> None:
    path = tmp_path / "empty_ask.parquet"

    rows = _snapshot_rows(
        received_time=_received_ns(milliseconds=100),
        last_update_id=100,
    )
    rows.append(
        _update_row(
            received_time=_received_ns(seconds=1),
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
            side="ask",
            price="101",
            quantity="0",
        )
    )

    _write(path, rows)

    result = sample_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    first = result.hours[0].observations[0]

    assert first.replay_valid is True
    assert first.book_structure is ReplayBookStructure.EMPTY_ASK
    assert first.quality is BookSampleQuality.INVALID
    assert first.invalid_reason is BookSampleInvalidReason.EMPTY_ASK
    assert first.checkpoint_eligible is False


def test_checkpoint_state_is_valid_from_first_bucket(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carried.parquet"

    # The archive requires at least one row. Its first update arrives later,
    # allowing early buckets to prove checkpoint carry behavior.
    _write(
        path,
        [
            _update_row(
                received_time=_received_ns(seconds=10),
                first_update_id=101,
                final_update_id=101,
                prev_final_update_id=100,
                side="bid",
                price="100.5",
                quantity="2",
            )
        ],
    )

    checkpoint = OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(-1),
        last_update_id=100,
        levels=(
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("100"),
                quantity=Decimal("1"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("101"),
                quantity=Decimal("1"),
                order_count=None,
            ),
        ),
    )

    result = sample_orderbook_archives(
        ((path, _spec()),),
        initial_checkpoint=checkpoint,
        batch_size=1,
    )

    observations = result.hours[0].observations

    first = observations[0]
    boundary = observations[9]

    assert first.quality is BookSampleQuality.VALID
    assert first.initialization_state is BookInitializationState.CARRIED
    assert first.source_received_time_ns is None
    assert first.last_update_id == 100
    assert first.best_bid == Decimal("100")
    assert first.best_ask == Decimal("101")

    # Update at exactly 10 seconds belongs to the tenth bucket.
    assert boundary.quality is BookSampleQuality.VALID
    assert boundary.source_received_time_ns == _received_ns(seconds=10)
    assert boundary.last_update_id == 101
    assert boundary.best_bid == Decimal("100.5")


def test_adjacent_hour_carries_last_reconstructed_state(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "hour12.parquet"
    second_path = tmp_path / "hour13.parquet"

    first_rows = _snapshot_rows(
        received_time=_received_ns(milliseconds=100),
        last_update_id=100,
    )
    first_rows.append(
        _update_row(
            received_time=_received_ns(seconds=3599),
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
            side="bid",
            price="100.5",
            quantity="2",
        )
    )

    second_rows = [
        _update_row(
            received_time=_received_ns(
                seconds=5,
                hour_offset=1,
            ),
            first_update_id=102,
            final_update_id=102,
            prev_final_update_id=101,
            side="ask",
            price="102",
            quantity="2",
        )
    ]

    _write(first_path, first_rows)
    _write(second_path, second_rows)

    result = sample_orderbook_archives(
        (
            (first_path, _spec(0)),
            (second_path, _spec(1)),
        ),
        batch_size=1,
    )

    assert len(result.hours) == 2
    assert result.total_observation_count == 7_200

    second_first = result.hours[1].observations[0]

    assert second_first.quality is BookSampleQuality.VALID
    assert second_first.initialization_state is (BookInitializationState.CARRIED)
    assert second_first.last_update_id == 101
    assert second_first.best_bid == Decimal("100.5")
    assert second_first.best_ask == Decimal("101")

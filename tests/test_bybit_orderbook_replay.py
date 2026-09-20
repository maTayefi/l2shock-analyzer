from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    BookInitializationState,
    BookSide,
    read_orderbook_file,
    replay_orderbook_archives,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        4,
        6,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _epoch_ns(value: datetime) -> int:
    epoch = datetime(
        1970,
        1,
        1,
        tzinfo=timezone.utc,
    )
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="bybit",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(offset),
    )


def _row(
    *,
    hour_offset: int,
    milliseconds: int,
    event_type: str,
    side: str,
    price: str,
    quantity: str,
    final_update_id: int | None,
    last_update_id: int,
    transaction_time_present: bool = True,
) -> dict[str, object]:
    received_time = _epoch_ns(_hour(hour_offset)) + milliseconds * 1_000_000
    event_time = received_time // 1_000_000

    return {
        "received_time": received_time,
        "event_time": event_time,
        "transaction_time": (event_time if transaction_time_present else None),
        "symbol": "BTCUSDT",
        "event_type": event_type,
        "first_update_id": None,
        "final_update_id": final_update_id,
        "prev_final_update_id": None,
        "last_update_id": last_update_id,
        "side": side,
        "price": price,
        "quantity": quantity,
        "order_count": None,
    }


def _native_snapshot_rows(
    *,
    hour_offset: int,
    milliseconds: int,
    final_update_id: int,
    last_update_id: int,
) -> list[dict[str, object]]:
    return [
        _row(
            hour_offset=hour_offset,
            milliseconds=milliseconds,
            event_type="snapshot",
            side="bid",
            price="100",
            quantity="2",
            final_update_id=final_update_id,
            last_update_id=last_update_id,
        ),
        _row(
            hour_offset=hour_offset,
            milliseconds=milliseconds,
            event_type="snapshot",
            side="ask",
            price="101",
            quantity="3",
            final_update_id=final_update_id,
            last_update_id=last_update_id,
        ),
    ]


def _boundary_snapshot_rows(
    *,
    hour_offset: int,
    last_update_id: int,
) -> list[dict[str, object]]:
    return [
        _row(
            hour_offset=hour_offset,
            milliseconds=0,
            event_type="snapshot",
            side="bid",
            price="99",
            quantity="5",
            final_update_id=None,
            last_update_id=last_update_id,
            transaction_time_present=False,
        ),
        _row(
            hour_offset=hour_offset,
            milliseconds=0,
            event_type="snapshot",
            side="ask",
            price="102",
            quantity="6",
            final_update_id=None,
            last_update_id=last_update_id,
            transaction_time_present=False,
        ),
    ]


def _update_row(
    *,
    hour_offset: int,
    milliseconds: int,
    final_update_id: int,
    last_update_id: int,
    side: str,
    price: str,
    quantity: str,
) -> dict[str, object]:
    return _row(
        hour_offset=hour_offset,
        milliseconds=milliseconds,
        event_type="update",
        side=side,
        price=price,
        quantity=quantity,
        final_update_id=final_update_id,
        last_update_id=last_update_id,
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


def test_bybit_native_snapshot_initializes_final_id_frontier(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bybit-native-snapshot.parquet"

    rows = _native_snapshot_rows(
        hour_offset=0,
        milliseconds=100,
        final_update_id=500,
        last_update_id=9_000,
    )
    rows.append(
        _update_row(
            hour_offset=0,
            milliseconds=200,
            final_update_id=501,
            last_update_id=9_050,
            side="bid",
            price="100.5",
            quantity="4",
        )
    )

    _write(path, rows)

    report = replay_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    assert report.finally_valid is True
    assert report.final_checkpoint is not None
    assert report.final_checkpoint.last_update_id == 501

    archive = report.archives[0]

    assert archive.initial_state is BookInitializationState.UNINITIALIZED
    assert archive.independently_initialized is True
    assert archive.required_carried_state is False
    assert archive.snapshots_applied == 1
    assert archive.updates_applied == 1
    assert archive.invalidation_count == 0
    assert archive.final_update_id == 501

    levels = {
        (
            level.side,
            level.price,
        ): level.quantity
        for level in report.final_checkpoint.levels
    }

    assert levels[
        (
            BookSide.BID,
            Decimal("100.5"),
        )
    ] == Decimal("4")


def test_bybit_reader_reports_zero_update_continuity_mismatches(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bybit-updates.parquet"

    rows = _native_snapshot_rows(
        hour_offset=0,
        milliseconds=100,
        final_update_id=500,
        last_update_id=9_000,
    )
    rows.extend(
        [
            _update_row(
                hour_offset=0,
                milliseconds=200,
                final_update_id=501,
                last_update_id=9_050,
                side="bid",
                price="100",
                quantity="4",
            ),
            _update_row(
                hour_offset=0,
                milliseconds=300,
                final_update_id=502,
                last_update_id=9_200,
                side="ask",
                price="101",
                quantity="5",
            ),
        ]
    )

    _write(path, rows)

    report = read_orderbook_file(
        path,
        _spec(),
        batch_size=1,
    )

    assert report.rows_read == 4
    assert report.events_read == 3
    assert report.snapshot_event_count == 1
    assert report.update_event_count == 2
    assert report.continuity_checks == 1
    assert report.continuity_mismatch_count == 0
    assert report.update_id_regression_count == 0


def test_bybit_boundary_snapshot_replaces_carried_book_and_preserves_frontier(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "hour-06.parquet"
    second_path = tmp_path / "hour-07.parquet"

    first_rows = _native_snapshot_rows(
        hour_offset=0,
        milliseconds=100,
        final_update_id=500,
        last_update_id=9_000,
    )
    first_rows.append(
        _update_row(
            hour_offset=0,
            milliseconds=3_599_000,
            final_update_id=501,
            last_update_id=9_500,
            side="bid",
            price="100",
            quantity="4",
        )
    )

    second_rows = _boundary_snapshot_rows(
        hour_offset=1,
        last_update_id=9_500,
    )
    second_rows.append(
        _update_row(
            hour_offset=1,
            milliseconds=100,
            final_update_id=502,
            last_update_id=9_700,
            side="bid",
            price="99.5",
            quantity="7",
        )
    )

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
    assert report.final_checkpoint.last_update_id == 502
    assert report.final_checkpoint.through_hour_utc == _hour(1)

    first, second = report.archives

    assert first.final_update_id == 501

    assert second.initial_state is BookInitializationState.CARRIED
    assert second.required_carried_state is True
    assert second.independently_initialized is False
    assert second.snapshots_applied == 1
    assert second.updates_applied == 1
    assert second.invalidation_count == 0
    assert second.final_update_id == 502

    levels = {
        (
            level.side,
            level.price,
        ): level.quantity
        for level in report.final_checkpoint.levels
    }

    assert (
        BookSide.BID,
        Decimal("100"),
    ) not in levels

    assert levels[
        (
            BookSide.BID,
            Decimal("99"),
        )
    ] == Decimal("5")

    assert levels[
        (
            BookSide.BID,
            Decimal("99.5"),
        )
    ] == Decimal("7")


def test_bybit_boundary_snapshot_cannot_initialize_without_carried_frontier(
    tmp_path: Path,
) -> None:
    path = tmp_path / "boundary-without-checkpoint.parquet"

    rows = _boundary_snapshot_rows(
        hour_offset=0,
        last_update_id=9_500,
    )
    rows.append(
        _update_row(
            hour_offset=0,
            milliseconds=100,
            final_update_id=502,
            last_update_id=9_700,
            side="bid",
            price="99.5",
            quantity="7",
        )
    )

    _write(path, rows)

    report = replay_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    assert report.finally_valid is False
    assert report.final_checkpoint is None

    archive = report.archives[0]

    assert archive.independently_initialized is False
    assert archive.required_carried_state is False
    assert archive.snapshots_applied == 0
    assert archive.updates_applied == 0
    assert archive.updates_skipped_uninitialized == 1
    assert archive.invalidation_count == 1
    assert archive.retained_issues[0].kind == ("snapshot_missing_replay_frontier")


def test_bybit_skipped_final_update_id_invalidates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bybit-gap.parquet"

    rows = _native_snapshot_rows(
        hour_offset=0,
        milliseconds=100,
        final_update_id=500,
        last_update_id=9_000,
    )
    rows.append(
        _update_row(
            hour_offset=0,
            milliseconds=200,
            final_update_id=502,
            last_update_id=9_100,
            side="bid",
            price="100",
            quantity="4",
        )
    )

    _write(path, rows)

    report = replay_orderbook_archives(
        ((path, _spec()),),
        batch_size=1,
    )

    assert report.finally_valid is False
    assert report.final_checkpoint is None

    archive = report.archives[0]

    assert archive.invalidation_count == 1
    assert archive.retained_issues[0].kind == ("update_continuity_mismatch")


def test_bybit_frontierless_snapshot_cannot_inherit_in_archive_snapshot_frontier(
    tmp_path: Path,
) -> None:
    path = tmp_path / "same-hour-frontierless-snapshot.parquet"

    rows = _native_snapshot_rows(
        hour_offset=0,
        milliseconds=100,
        final_update_id=500,
        last_update_id=9_000,
    )
    rows.extend(
        _boundary_snapshot_rows(
            hour_offset=0,
            last_update_id=9_500,
        )
    )

    _write(
        path,
        rows,
    )

    report = replay_orderbook_archives(
        (
            (
                path,
                _spec(0),
            ),
        ),
        batch_size=1,
    )

    assert report.finally_valid is False
    assert report.final_checkpoint is None

    archive = report.archives[0]

    assert archive.snapshots_applied == 1
    assert archive.invalidation_count == 1
    assert any(
        issue.kind == "snapshot_missing_replay_frontier"
        for issue in archive.retained_issues
    )

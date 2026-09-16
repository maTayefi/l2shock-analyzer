from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    BookInitializationState,
    BookSide,
    ReplayBookStructure,
    read_orderbook_file,
    replay_orderbook_archives,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        9,
        12,
        tzinfo=timezone.utc,
    )


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="okx_futures",
        symbol="BTC-USDT-SWAP",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )


def _row(
    *,
    received_time: int,
    event_time: int,
    event_type: str,
    side: str,
    price: str,
    quantity: str,
    final_update_id: int,
    last_update_id: int,
) -> dict[str, object]:
    return {
        "received_time": received_time,
        "event_time": event_time,
        "transaction_time": None,
        "symbol": "BTC-USDT-SWAP",
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


def _write(path: Path) -> None:
    hour_ns = 1_788_955_200_000_000_000
    hour_ms = 1_788_955_200_000

    rows = [
        _row(
            received_time=hour_ns,
            event_time=hour_ms,
            event_type="snapshot",
            side="bid",
            price="100",
            quantity="2",
            final_update_id=100,
            last_update_id=100,
        ),
        _row(
            received_time=hour_ns,
            event_time=hour_ms,
            event_type="snapshot",
            side="ask",
            price="101",
            quantity="3",
            final_update_id=100,
            last_update_id=100,
        ),
        _row(
            received_time=hour_ns + 100_000_000,
            event_time=hour_ms + 5,
            event_type="update",
            side="bid",
            price="100.5",
            quantity="4",
            final_update_id=110,
            last_update_id=100,
        ),
        _row(
            received_time=hour_ns + 200_000_000,
            event_time=hour_ms + 105,
            event_type="update",
            side="ask",
            price="101",
            quantity="0",
            final_update_id=120,
            last_update_id=110,
        ),
        _row(
            received_time=hour_ns + 200_000_000,
            event_time=hour_ms + 105,
            event_type="update",
            side="ask",
            price="102",
            quantity="5",
            final_update_id=120,
            last_update_id=110,
        ),
    ]

    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
        row_group_size=1,
    )


def test_okx_reader_accepts_nullable_venue_fields(
    tmp_path: Path,
) -> None:
    path = tmp_path / "okx.parquet"
    _write(path)

    report = read_orderbook_file(
        path,
        _spec(),
        batch_size=1,
    )

    assert report.rows_read == 5
    assert report.events_read == 3
    assert report.snapshot_event_count == 1
    assert report.update_event_count == 2
    assert report.continuity_checks == 1
    assert report.continuity_mismatch_count == 0


def test_okx_snapshot_and_updates_reconstruct_book(
    tmp_path: Path,
) -> None:
    path = tmp_path / "okx.parquet"
    _write(path)

    report = replay_orderbook_archives(
        (
            (
                path,
                _spec(),
            ),
        ),
        batch_size=1,
    )

    assert report.finally_valid is True
    assert report.final_checkpoint is not None

    archive = report.archives[0]

    assert archive.snapshots_applied == 1
    assert archive.updates_applied == 2
    assert archive.final_state is BookInitializationState.SNAPSHOT
    assert archive.final_update_id == 120
    assert archive.reader_report.continuity_mismatch_count == 0

    checkpoint = report.final_checkpoint

    assert checkpoint.venue == "okx_futures"
    assert checkpoint.symbol == "BTC-USDT-SWAP"
    assert checkpoint.last_update_id == 120

    levels = {
        (
            level.side,
            level.price,
        ): level.quantity
        for level in checkpoint.levels
    }

    assert levels[
        (
            BookSide.BID,
            Decimal("100.5"),
        )
    ] == Decimal("4")

    assert (
        BookSide.ASK,
        Decimal("101"),
    ) not in levels

    assert levels[
        (
            BookSide.ASK,
            Decimal("102"),
        )
    ] == Decimal("5")


def test_okx_continuity_failure_invalidates_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bad-okx.parquet"

    hour_ns = 1_788_955_200_000_000_000
    hour_ms = 1_788_955_200_000

    rows = [
        _row(
            received_time=hour_ns,
            event_time=hour_ms,
            event_type="snapshot",
            side="bid",
            price="100",
            quantity="1",
            final_update_id=100,
            last_update_id=100,
        ),
        _row(
            received_time=hour_ns,
            event_time=hour_ms,
            event_type="snapshot",
            side="ask",
            price="101",
            quantity="1",
            final_update_id=100,
            last_update_id=100,
        ),
        _row(
            received_time=hour_ns + 100_000_000,
            event_time=hour_ms + 5,
            event_type="update",
            side="bid",
            price="100.5",
            quantity="2",
            final_update_id=120,
            last_update_id=119,
        ),
    ]

    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression="zstd",
    )

    report = replay_orderbook_archives(
        (
            (
                path,
                _spec(),
            ),
        ),
    )

    archive = report.archives[0]

    assert report.finally_valid is False
    assert report.final_checkpoint is None
    assert archive.invalidation_count == 1
    assert archive.finally_valid is False
    assert archive.final_update_id is None
    assert archive.final_bid_level_count == 0
    assert archive.final_ask_level_count == 0
    assert report.archives[0].invalidation_count == 1
    assert (
        report.archives[0].final_book_structure is (ReplayBookStructure.EMPTY_BOTH)
        if hasattr(report.archives[0], "final_book_structure")
        else True
    )

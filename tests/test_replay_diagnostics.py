from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from l2shock.acquisition import SourceFileSpec
from l2shock.ingest import (
    BookSide,
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevelChange,
    OrderBookReplayState,
    ReplayBookStructure,
    ReplayCancelledError,
    replay_chain_report_to_dict,
    replay_orderbook_archives,
    write_json_report_atomic,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_hour(),
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


def _event(
    *,
    event_type: OrderBookEventType,
    changes: tuple[OrderBookLevelChange, ...],
    first_update_id: int | None = None,
    final_update_id: int | None = None,
    prev_final_update_id: int | None = None,
    last_update_id: int | None = None,
    row_number: int = 0,
) -> OrderBookEvent:
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=event_type,
        received_time_ns=1_788_350_400_000_000_000 + row_number,
        event_time_ms=1_788_350_400_000 + row_number,
        transaction_time_ms=1_788_350_399_999 + row_number,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        prev_final_update_id=prev_final_update_id,
        last_update_id=last_update_id,
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


def _initialize(state: OrderBookReplayState) -> None:
    state.apply(
        _event(
            event_type=OrderBookEventType.SNAPSHOT,
            last_update_id=100,
            changes=(
                _change("bid", "100", "1"),
                _change("ask", "101", "1"),
            ),
        ),
        hour_utc=_hour(),
    )


def _update(
    state: OrderBookReplayState,
    *,
    final_update_id: int,
    changes: tuple[OrderBookLevelChange, ...],
) -> None:
    previous = state.last_update_id
    assert previous is not None

    state.apply(
        _event(
            event_type=OrderBookEventType.UPDATE,
            first_update_id=final_update_id,
            final_update_id=final_update_id,
            prev_final_update_id=previous,
            changes=changes,
            row_number=final_update_id,
        ),
        hour_utc=_hour(),
    )


def _write_update_only_archive(path: Path, rows: int = 20) -> None:
    values: list[dict[str, object]] = []

    previous = 100

    for index in range(rows):
        final_id = previous + 1

        values.append(
            {
                "received_time": (1_788_350_400_000_000_000 + index),
                "event_time": 1_788_350_400_000 + index,
                "transaction_time": (1_788_350_399_999 + index),
                "symbol": "BTCUSDT",
                "event_type": "update",
                "first_update_id": final_id,
                "final_update_id": final_id,
                "prev_final_update_id": previous,
                "last_update_id": None,
                "side": "bid",
                "price": str(60_000 + index),
                "quantity": "1",
                "order_count": None,
            }
        )

        previous = final_id

    pq.write_table(
        pa.Table.from_pylist(values),
        path,
        compression="zstd",
        row_group_size=2,
    )


def test_book_structure_reports_normal_locked_and_crossed() -> None:
    state = _state()
    _initialize(state)

    assert state.book_structure is ReplayBookStructure.NORMAL
    assert state.checkpoint_eligible is True

    _update(
        state,
        final_update_id=101,
        changes=(_change("bid", "101", "2"),),
    )

    assert state.book_structure is ReplayBookStructure.LOCKED

    _update(
        state,
        final_update_id=102,
        changes=(_change("bid", "102", "2"),),
    )

    assert state.book_structure is ReplayBookStructure.CROSSED
    assert state.checkpoint_eligible is True


def test_empty_side_is_diagnostic_and_not_checkpoint_eligible() -> None:
    state = _state()
    _initialize(state)

    _update(
        state,
        final_update_id=101,
        changes=(_change("ask", "101", "0"),),
    )

    assert state.valid is True
    assert state.book_structure is ReplayBookStructure.EMPTY_ASK
    assert state.checkpoint_eligible is False


def test_replay_cancellation_is_checked_during_streaming(
    tmp_path: Path,
) -> None:
    path = tmp_path / "update_only.parquet"
    _write_update_only_archive(path, rows=50)

    checks = 0

    def cancellation_probe() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 4

    with pytest.raises(
        ReplayCancelledError,
        match="cancelled",
    ):
        replay_orderbook_archives(
            ((path, _spec()),),
            batch_size=2,
            cancellation_probe=cancellation_probe,
            cancellation_check_interval_rows=2,
        )

    assert checks >= 4


def test_json_report_is_safe_for_update_only_archive(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "update_only.parquet"
    output = tmp_path / "reports" / "replay.json"

    _write_update_only_archive(archive, rows=3)

    report = replay_orderbook_archives(
        ((archive, _spec()),),
        batch_size=1,
    )

    payload = replay_chain_report_to_dict(report)

    assert payload["schema"] == ("l2shock.replay_validation_report")
    assert payload["schema_version"] == 1
    assert payload["finally_valid"] is False
    assert payload["archive_count"] == 1
    assert payload["final_checkpoint"] is None

    archive_payload = payload["archives"][0]

    assert archive_payload["events_seen"] == 3
    assert archive_payload["updates_skipped_uninitialized"] == 3
    assert archive_payload["structure_counts"] == {
        "normal": 0,
        "locked": 0,
        "crossed": 0,
        "empty_bid": 0,
        "empty_ask": 0,
        "empty_both": 0,
    }

    published = write_json_report_atomic(output, report)

    assert published == output.resolve()
    decoded = json.loads(output.read_text(encoding="utf-8"))

    assert decoded["finally_valid"] is False
    assert decoded["symbol"] == "BTCUSDT"

    with pytest.raises(FileExistsError):
        write_json_report_atomic(output, report)

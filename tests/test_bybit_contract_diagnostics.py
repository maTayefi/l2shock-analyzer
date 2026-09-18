from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "tools" / "diagnose_bybit_contract.py"

_SPEC = importlib.util.spec_from_file_location(
    "diagnose_bybit_contract",
    MODULE_PATH,
)

if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError("Could not load tools/diagnose_bybit_contract.py")

diagnostic = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(diagnostic)


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


def _row(
    *,
    hour_offset: int,
    milliseconds: int,
    event_type: str,
    side: str,
    price: str,
    quantity: str,
    final_update_id: int,
    last_update_id: int,
) -> dict[str, object]:
    received_time = _epoch_ns(_hour(hour_offset)) + milliseconds * 1_000_000
    event_time = received_time // 1_000_000

    return {
        "received_time": received_time,
        "event_time": event_time,
        "transaction_time": event_time,
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


def test_snapshot_rows_are_grouped_into_one_complete_event(
    tmp_path: Path,
) -> None:
    path = tmp_path / "snapshot.parquet"

    rows = [
        _row(
            hour_offset=0,
            milliseconds=100,
            event_type="snapshot",
            side="bid",
            price="100",
            quantity="2",
            final_update_id=500,
            last_update_id=9_000,
        ),
        _row(
            hour_offset=0,
            milliseconds=100,
            event_type="snapshot",
            side="ask",
            price="101",
            quantity="3",
            final_update_id=500,
            last_update_id=9_000,
        ),
        _row(
            hour_offset=0,
            milliseconds=200,
            event_type="update",
            side="bid",
            price="100",
            quantity="4",
            final_update_id=501,
            last_update_id=9_001,
        ),
    ]

    _write(path, rows)

    report = diagnostic.inspect_archive(
        path,
        symbol="BTCUSDT",
        hour_utc=_hour(),
    )

    assert report["row_count"] == 3
    assert report["event_count"] == 2

    assert report["event_type_row_counts"] == {
        "snapshot": 2,
        "update": 1,
    }
    assert report["event_type_event_counts"] == {
        "snapshot": 1,
        "update": 1,
    }

    assert report["snapshot_event_count"] == 1
    assert report["complete_snapshot_candidate_count"] == 1

    snapshot = report["snapshot_events"][0]

    assert snapshot["level_row_count"] == 2
    assert snapshot["positive_bid_row_count"] == 1
    assert snapshot["positive_ask_row_count"] == 1
    assert snapshot["snapshot_complete_candidate"] is True

    assert report["snapshot_to_next_update_relations"][
        "update_final_vs_snapshot_final"
    ] == {
        "increment_by_one": 1,
    }
    assert report["snapshot_to_next_update_relations"][
        "update_last_vs_snapshot_last"
    ] == {
        "increment_by_one": 1,
    }


def test_update_relationships_are_counted_by_logical_event(
    tmp_path: Path,
) -> None:
    path = tmp_path / "updates.parquet"

    rows = [
        _row(
            hour_offset=0,
            milliseconds=100,
            event_type="update",
            side="bid",
            price="100",
            quantity="2",
            final_update_id=500,
            last_update_id=9_000,
        ),
        _row(
            hour_offset=0,
            milliseconds=100,
            event_type="update",
            side="ask",
            price="101",
            quantity="3",
            final_update_id=500,
            last_update_id=9_000,
        ),
        _row(
            hour_offset=0,
            milliseconds=200,
            event_type="update",
            side="bid",
            price="100",
            quantity="4",
            final_update_id=501,
            last_update_id=9_001,
        ),
        _row(
            hour_offset=0,
            milliseconds=300,
            event_type="update",
            side="ask",
            price="101",
            quantity="5",
            final_update_id=502,
            last_update_id=9_005,
        ),
    ]

    _write(path, rows)

    report = diagnostic.inspect_archive(
        path,
        symbol="BTCUSDT",
        hour_utc=_hour(),
    )

    relations = report["adjacent_update_relations"]

    assert report["row_count"] == 4
    assert report["event_count"] == 3
    assert report["update_event_count"] == 3

    assert relations["final_vs_previous_final"] == {
        "increment_by_one": 2,
    }
    assert relations["last_vs_previous_last"] == {
        "forward_gap": 1,
        "increment_by_one": 1,
    }


def test_cross_hour_report_keeps_sequence_hypotheses_explicit(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    _write(
        first_path,
        [
            _row(
                hour_offset=0,
                milliseconds=100,
                event_type="update",
                side="bid",
                price="100",
                quantity="2",
                final_update_id=500,
                last_update_id=9_000,
            )
        ],
    )
    _write(
        second_path,
        [
            _row(
                hour_offset=1,
                milliseconds=100,
                event_type="update",
                side="ask",
                price="101",
                quantity="3",
                final_update_id=501,
                last_update_id=9_010,
            )
        ],
    )

    first = diagnostic.inspect_archive(
        first_path,
        symbol="BTCUSDT",
        hour_utc=_hour(),
    )
    second = diagnostic.inspect_archive(
        second_path,
        symbol="BTCUSDT",
        hour_utc=_hour(1),
    )

    report = diagnostic.build_report(
        [
            first,
            second,
        ]
    )
    boundary = report["cross_hour_boundaries"][0]

    assert (
        boundary["relations"]["current_final_vs_previous_final"] == "increment_by_one"
    )

    assert boundary["relations"]["current_last_vs_previous_last"] == "forward_gap"

    assert report["warning"] == (
        "Diagnostic hypotheses are not a production sequence contract."
    )

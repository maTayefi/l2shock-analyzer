#!/usr/bin/env python3
"""Inspect normalized venue event identity and sequence behavior.

This tool is intentionally permissive. It records nullable venue-specific
fields without applying the production Binance sequence contract.

It streams Parquet batches and does not materialize a complete archive.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

_COLUMNS = (
    "received_time",
    "event_time",
    "transaction_time",
    "symbol",
    "event_type",
    "first_update_id",
    "final_update_id",
    "prev_final_update_id",
    "last_update_id",
    "side",
    "price",
    "quantity",
    "order_count",
)


@dataclass(frozen=True)
class ArchiveArgument:
    venue: str
    symbol: str
    hour_utc: str
    path: Path


def _event_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("symbol"),
        row.get("event_type"),
        row.get("received_time"),
        row.get("event_time"),
        row.get("transaction_time"),
        row.get("first_update_id"),
        row.get("final_update_id"),
        row.get("prev_final_update_id"),
        row.get("last_update_id"),
    )


def _summary(
    key: tuple[Any, ...],
    *,
    first_row: int,
    last_row: int,
    level_rows: int,
) -> dict[str, Any]:
    (
        symbol,
        event_type,
        received_time,
        event_time,
        transaction_time,
        first_update_id,
        final_update_id,
        prev_final_update_id,
        last_update_id,
    ) = key

    return {
        "symbol": symbol,
        "event_type": event_type,
        "received_time": received_time,
        "event_time": event_time,
        "transaction_time": transaction_time,
        "first_update_id": first_update_id,
        "final_update_id": final_update_id,
        "prev_final_update_id": prev_final_update_id,
        "last_update_id": last_update_id,
        "first_row": first_row,
        "last_row": last_row,
        "level_row_count": level_rows,
    }


def inspect_archive(argument: ArchiveArgument) -> dict[str, Any]:
    path = argument.path.expanduser().resolve()

    if not path.is_file():
        raise FileNotFoundError(f"Archive file does not exist: {path}")

    parquet = pq.ParquetFile(path)
    missing = sorted(set(_COLUMNS) - set(parquet.schema_arrow.names))

    if missing:
        raise ValueError(f"{path.name} is missing columns: {missing}")

    row_count = 0
    event_count = 0
    event_type_rows: Counter[str] = Counter()
    event_type_events: Counter[str] = Counter()
    field_patterns: Counter[tuple[Any, ...]] = Counter()

    transaction_null_rows = 0
    first_update_null_rows = 0
    previous_update_null_rows = 0
    last_update_null_rows = 0

    adjacent_update_pairs = 0
    prev_matches_previous_final = 0
    final_is_previous_final_plus_one = 0
    uncheckable_prev_pairs = 0

    first_events: list[dict[str, Any]] = []
    last_events: deque[dict[str, Any]] = deque(maxlen=5)
    first_updates: list[dict[str, Any]] = []
    first_snapshots: list[dict[str, Any]] = []

    current_key: tuple[Any, ...] | None = None
    current_first_row = 0
    current_level_rows = 0
    previous_update: dict[str, Any] | None = None

    def finish_event(last_row: int) -> None:
        nonlocal current_key
        nonlocal current_level_rows
        nonlocal event_count
        nonlocal previous_update
        nonlocal adjacent_update_pairs
        nonlocal prev_matches_previous_final
        nonlocal final_is_previous_final_plus_one
        nonlocal uncheckable_prev_pairs

        if current_key is None:
            return

        item = _summary(
            current_key,
            first_row=current_first_row,
            last_row=last_row,
            level_rows=current_level_rows,
        )
        event_count += 1

        event_type = str(item["event_type"])
        event_type_events[event_type] += 1

        pattern = (
            event_type,
            item["transaction_time"] is None,
            item["first_update_id"] is None,
            item["final_update_id"] is None,
            item["prev_final_update_id"] is None,
            item["last_update_id"] is None,
        )
        field_patterns[pattern] += 1

        if len(first_events) < 5:
            first_events.append(item)

        last_events.append(item)

        if event_type == "snapshot" and len(first_snapshots) < 5:
            first_snapshots.append(item)

        if event_type == "update":
            if len(first_updates) < 5:
                first_updates.append(item)

            if previous_update is not None:
                adjacent_update_pairs += 1

                previous_final = previous_update["final_update_id"]
                current_previous = item["prev_final_update_id"]
                current_final = item["final_update_id"]

                if previous_final is None or current_previous is None:
                    uncheckable_prev_pairs += 1
                elif current_previous == previous_final:
                    prev_matches_previous_final += 1

                if (
                    previous_final is not None
                    and current_final is not None
                    and current_final == previous_final + 1
                ):
                    final_is_previous_final_plus_one += 1

            previous_update = item
        else:
            previous_update = None

        current_key = None
        current_level_rows = 0

    for batch in parquet.iter_batches(
        batch_size=65_536,
        columns=list(_COLUMNS),
        use_threads=True,
    ):
        for row in batch.to_pylist():
            key = _event_key(row)
            event_type_rows[str(row.get("event_type"))] += 1

            transaction_null_rows += row.get("transaction_time") is None
            first_update_null_rows += row.get("first_update_id") is None
            previous_update_null_rows += row.get("prev_final_update_id") is None
            last_update_null_rows += row.get("last_update_id") is None

            if current_key is None:
                current_key = key
                current_first_row = row_count
                current_level_rows = 1
            elif key == current_key:
                current_level_rows += 1
            else:
                finish_event(row_count - 1)
                current_key = key
                current_first_row = row_count
                current_level_rows = 1

            row_count += 1

    finish_event(row_count - 1)

    return {
        "venue": argument.venue,
        "symbol": argument.symbol,
        "hour_utc": argument.hour_utc,
        "path": str(path),
        "file_size_bytes": path.stat().st_size,
        "metadata_row_count": int(parquet.metadata.num_rows),
        "streamed_row_count": row_count,
        "row_group_count": int(parquet.metadata.num_row_groups),
        "event_count": event_count,
        "event_type_row_counts": dict(sorted(event_type_rows.items())),
        "event_type_event_counts": dict(sorted(event_type_events.items())),
        "nullable_row_counts": {
            "transaction_time": transaction_null_rows,
            "first_update_id": first_update_null_rows,
            "prev_final_update_id": previous_update_null_rows,
            "last_update_id": last_update_null_rows,
        },
        "field_presence_patterns": [
            {
                "event_type": pattern[0],
                "transaction_time_null": pattern[1],
                "first_update_id_null": pattern[2],
                "final_update_id_null": pattern[3],
                "prev_final_update_id_null": pattern[4],
                "last_update_id_null": pattern[5],
                "event_count": count,
            }
            for pattern, count in sorted(
                field_patterns.items(),
                key=lambda item: repr(item[0]),
            )
        ],
        "adjacent_update_relations": {
            "pair_count": adjacent_update_pairs,
            "prev_matches_previous_final": prev_matches_previous_final,
            "uncheckable_prev_pairs": uncheckable_prev_pairs,
            "final_is_previous_final_plus_one": (final_is_previous_final_plus_one),
        },
        "first_events": first_events,
        "last_events": list(last_events),
        "first_snapshots": first_snapshots,
        "first_updates": first_updates,
    }


def _archive_argument(values: list[str]) -> ArchiveArgument:
    venue, symbol, hour_utc, raw_path = values

    parsed = datetime.fromisoformat(hour_utc.replace("Z", "+00:00"))

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("archive hour must be timezone-aware")

    return ArchiveArgument(
        venue=venue.strip().lower(),
        symbol=symbol.strip().upper(),
        hour_utc=parsed.isoformat().replace("+00:00", "Z"),
        path=Path(raw_path),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect venue-specific normalized event contracts."
    )
    parser.add_argument(
        "--archive",
        action="append",
        nargs=4,
        required=True,
        metavar=("VENUE", "SYMBOL", "UTC_HOUR", "PATH"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("venue_sequence_contract.json"),
    )

    args = parser.parse_args()

    archive_arguments = [_archive_argument(values) for values in args.archive]

    missing_paths = [
        argument.path.expanduser().resolve()
        for argument in archive_arguments
        if not argument.path.expanduser().is_file()
    ]

    if missing_paths:
        details = "\n".join(f"  - {path}" for path in missing_paths)
        raise FileNotFoundError(
            "One or more archive files do not exist:\n"
            f"{details}\n\n"
            "Verify the supplied --archive PATH values before rerunning."
        )

    results = [inspect_archive(argument) for argument in archive_arguments]

    boundaries: list[dict[str, Any]] = []

    for item in results:
        hour = datetime.fromisoformat(str(item["hour_utc"]).replace("Z", "+00:00"))
        next_hour = hour.timestamp() + 3600

        following = next(
            (
                candidate
                for candidate in results
                if candidate["venue"] == item["venue"]
                and candidate["symbol"] == item["symbol"]
                and datetime.fromisoformat(
                    str(candidate["hour_utc"]).replace("Z", "+00:00")
                ).timestamp()
                == next_hour
            ),
            None,
        )

        if following is None:
            continue

        previous_last = next(
            (
                event
                for event in reversed(item["last_events"])
                if event["event_type"] == "update"
            ),
            None,
        )
        current_first = next(
            (
                event
                for event in following["first_updates"]
                if event["event_type"] == "update"
            ),
            None,
        )

        boundaries.append(
            {
                "venue": item["venue"],
                "symbol": item["symbol"],
                "previous_hour": item["hour_utc"],
                "current_hour": following["hour_utc"],
                "current_starts_with_snapshot": bool(
                    following["first_events"]
                    and following["first_events"][0]["event_type"] == "snapshot"
                ),
                "previous_last_update": previous_last,
                "current_first_update": current_first,
                "relations": {
                    "current_prev_matches_previous_final": (
                        previous_last is not None
                        and current_first is not None
                        and current_first["prev_final_update_id"] is not None
                        and current_first["prev_final_update_id"]
                        == previous_last["final_update_id"]
                    ),
                    "current_final_is_previous_final_plus_one": (
                        previous_last is not None
                        and current_first is not None
                        and previous_last["final_update_id"] is not None
                        and current_first["final_update_id"] is not None
                        and current_first["final_update_id"]
                        == previous_last["final_update_id"] + 1
                    ),
                },
            }
        )

    payload = {
        "schema": "l2shock.venue_sequence_contract_inspection",
        "schema_version": 1,
        "archives": results,
        "cross_hour_boundaries": boundaries,
    }

    destination = args.output.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

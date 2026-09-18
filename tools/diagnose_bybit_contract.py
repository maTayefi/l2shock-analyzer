#!/usr/bin/env python3
"""Download and inspect bounded Bybit order-book contract samples.

This is a diagnostic tool, not a production venue adapter.

It deliberately does not:

- enable Bybit in SourceFileSpec;
- enable Bybit in production acquisition planning;
- declare a sequence frontier;
- replay or calculate liquidity;
- publish Hugging Face artifacts;
- access PostgreSQL or NiceGUI.

The tool groups physical price-level rows into logical events using every
normalized event-identity field. It reports:

- row and event counts separately;
- snapshot completeness candidates;
- nullable-field patterns;
- adjacent-update sequence hypotheses;
- cross-hour sequence hypotheses;
- received-time ordering;
- bounded first/last/snapshot event samples.

No sequence hypothesis is promoted to a production contract by this tool.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

import httpx
import pyarrow.parquet as pq

BASE_URL: Final[str] = "https://api.cryptohftdata.com/v1/download"
DEFAULT_REPORT: Final[Path] = Path("bybit-contract-diagnostics.json")
DEFAULT_CACHE_ROOT: Final[Path] = Path("bybit-contract-samples")

_COLUMNS: Final[tuple[str, ...]] = (
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

log = logging.getLogger("l2shock.bybit_contract_diagnostics")


def _canonical_utc_hour(value: str) -> datetime:
    text = str(value or "").strip()

    if not text.endswith("Z"):
        raise argparse.ArgumentTypeError(
            "hour must use canonical UTC text ending in Z"
        )

    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "hour must use canonical ISO-8601 UTC text"
        ) from exc

    if (
        parsed.utcoffset() != timedelta(0)
        or parsed.minute
        or parsed.second
        or parsed.microsecond
    ):
        raise argparse.ArgumentTypeError(
            "hour must identify an exact UTC hour"
        )

    canonical = parsed.isoformat().replace("+00:00", "Z")

    if canonical != text:
        raise argparse.ArgumentTypeError(
            "hour is valid but not canonically encoded"
        )

    return parsed


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _remote_path(
    *,
    symbol: str,
    hour_utc: datetime,
) -> str:
    return (
        f"bybit/{hour_utc:%Y-%m-%d}/{hour_utc:%H}/"
        f"{symbol}_orderbook.parquet"
    )


def _download(
    client: httpx.Client,
    *,
    symbol: str,
    hour_utc: datetime,
    cache_root: Path,
) -> Path:
    remote_path = _remote_path(
        symbol=symbol,
        hour_utc=hour_utc,
    )
    destination = (
        cache_root
        / symbol
        / hour_utc.strftime("%Y-%m-%d")
        / hour_utc.strftime("%H")
        / f"{symbol}_orderbook.parquet"
    ).resolve()

    if destination.is_file() and destination.stat().st_size > 0:
        log.info(
            "Reusing cached diagnostic source: remote_path=%s bytes=%d",
            remote_path,
            destination.stat().st_size,
        )
        return destination

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    temporary = destination.with_suffix(".parquet.part")

    log.info(
        "Downloading diagnostic source: remote_path=%s",
        remote_path,
    )

    try:
        with client.stream(
            "GET",
            BASE_URL,
            params={"file": remote_path},
            timeout=180.0,
            follow_redirects=True,
        ) as response:
            if response.status_code == 404:
                raise FileNotFoundError(
                    f"Bybit archive is unavailable: {remote_path}"
                )

            if response.status_code < 200 or response.status_code >= 300:
                raise RuntimeError(
                    "CryptoHFTData diagnostic download returned "
                    f"HTTP {response.status_code} for {remote_path}"
                )

            with temporary.open("wb") as handle:
                for chunk in response.iter_bytes(
                    chunk_size=1024 * 1024,
                ):
                    if chunk:
                        handle.write(chunk)

        if not temporary.is_file() or temporary.stat().st_size <= 0:
            raise RuntimeError(
                f"Downloaded diagnostic archive is empty: {remote_path}"
            )

        temporary.replace(destination)

    finally:
        temporary.unlink(missing_ok=True)

    log.info(
        "Downloaded diagnostic source: remote_path=%s bytes=%d",
        remote_path,
        destination.stat().st_size,
    )

    return destination


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


def _optional_int(value: object) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool) or not isinstance(value, int):
        return None

    return value


def _decimal_or_none(value: object) -> Decimal | None:
    if not isinstance(value, str):
        return None

    try:
        parsed = Decimal(value.strip())
    except InvalidOperation:
        return None

    return parsed if parsed.is_finite() else None


def _relation(
    current: int | None,
    previous: int | None,
) -> str:
    if current is None or previous is None:
        return "uncheckable"

    difference = current - previous

    if difference < 0:
        return "regression"

    if difference == 0:
        return "equal"

    if difference == 1:
        return "increment_by_one"

    return "forward_gap"


def _event_summary(
    key: tuple[Any, ...],
    *,
    first_row: int,
    last_row: int,
    bid_rows: int,
    ask_rows: int,
    positive_bid_rows: int,
    positive_ask_rows: int,
    zero_quantity_rows: int,
    malformed_quantity_rows: int,
    duplicate_side_price_rows: int,
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

    snapshot_complete_candidate = bool(
        event_type == "snapshot"
        and positive_bid_rows > 0
        and positive_ask_rows > 0
        and malformed_quantity_rows == 0
        and duplicate_side_price_rows == 0
    )

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
        "level_row_count": last_row - first_row + 1,
        "bid_row_count": bid_rows,
        "ask_row_count": ask_rows,
        "positive_bid_row_count": positive_bid_rows,
        "positive_ask_row_count": positive_ask_rows,
        "zero_quantity_row_count": zero_quantity_rows,
        "malformed_quantity_row_count": malformed_quantity_rows,
        "duplicate_side_price_row_count": duplicate_side_price_rows,
        "snapshot_complete_candidate": snapshot_complete_candidate,
    }


def inspect_archive(
    path: Path,
    *,
    symbol: str,
    hour_utc: datetime,
) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()

    if not source.is_file():
        raise FileNotFoundError(source)

    parquet = pq.ParquetFile(source)
    schema = parquet.schema_arrow
    missing = sorted(set(_COLUMNS) - set(schema.names))

    if missing:
        raise ValueError(
            f"{source.name} is missing required columns: {missing}"
        )

    schema_types = {
        field.name: str(field.type)
        for field in schema
    }

    row_count = 0
    event_count = 0
    snapshot_event_count = 0
    complete_snapshot_candidate_count = 0
    update_event_count = 0
    received_time_regression_count = 0

    event_type_row_counts: Counter[str] = Counter()
    event_type_event_counts: Counter[str] = Counter()
    field_null_counts: Counter[str] = Counter()
    field_presence_patterns: Counter[tuple[Any, ...]] = Counter()

    adjacent_update_relations: dict[str, Counter[str]] = {
        "final_vs_previous_final": Counter(),
        "last_vs_previous_last": Counter(),
        "current_last_vs_previous_final": Counter(),
        "current_final_vs_previous_last": Counter(),
    }

    first_events: list[dict[str, Any]] = []
    first_updates: list[dict[str, Any]] = []
    snapshot_events: list[dict[str, Any]] = []
    last_events: deque[dict[str, Any]] = deque(maxlen=5)

    current_key: tuple[Any, ...] | None = None
    current_first_row = 0
    current_bid_rows = 0
    current_ask_rows = 0
    current_positive_bid_rows = 0
    current_positive_ask_rows = 0
    current_zero_quantity_rows = 0
    current_malformed_quantity_rows = 0
    current_duplicate_side_price_rows = 0
    current_side_prices: set[tuple[str, str]] = set()

    previous_event: dict[str, Any] | None = None
    previous_update: dict[str, Any] | None = None

    def finish_event(last_row: int) -> None:
        nonlocal current_key
        nonlocal current_bid_rows
        nonlocal current_ask_rows
        nonlocal current_positive_bid_rows
        nonlocal current_positive_ask_rows
        nonlocal current_zero_quantity_rows
        nonlocal current_malformed_quantity_rows
        nonlocal current_duplicate_side_price_rows
        nonlocal current_side_prices
        nonlocal event_count
        nonlocal snapshot_event_count
        nonlocal complete_snapshot_candidate_count
        nonlocal update_event_count
        nonlocal received_time_regression_count
        nonlocal previous_event
        nonlocal previous_update

        if current_key is None:
            return

        event = _event_summary(
            current_key,
            first_row=current_first_row,
            last_row=last_row,
            bid_rows=current_bid_rows,
            ask_rows=current_ask_rows,
            positive_bid_rows=current_positive_bid_rows,
            positive_ask_rows=current_positive_ask_rows,
            zero_quantity_rows=current_zero_quantity_rows,
            malformed_quantity_rows=current_malformed_quantity_rows,
            duplicate_side_price_rows=current_duplicate_side_price_rows,
        )

        event_count += 1
        event_type = str(event["event_type"])
        event_type_event_counts[event_type] += 1

        pattern = (
            event_type,
            event["transaction_time"] is None,
            event["first_update_id"] is None,
            event["final_update_id"] is None,
            event["prev_final_update_id"] is None,
            event["last_update_id"] is None,
        )
        field_presence_patterns[pattern] += 1

        if len(first_events) < 5:
            first_events.append(event)

        last_events.append(event)

        if previous_event is not None:
            previous_received = _optional_int(
                previous_event["received_time"]
            )
            current_received = _optional_int(
                event["received_time"]
            )

            if (
                previous_received is not None
                and current_received is not None
                and current_received < previous_received
            ):
                received_time_regression_count += 1

        if event_type == "snapshot":
            snapshot_event_count += 1

            if event["snapshot_complete_candidate"]:
                complete_snapshot_candidate_count += 1

            if len(snapshot_events) < 10:
                snapshot_events.append(event)

            previous_update = None

        elif event_type == "update":
            update_event_count += 1

            if len(first_updates) < 5:
                first_updates.append(event)

            if previous_update is not None:
                previous_final = _optional_int(
                    previous_update["final_update_id"]
                )
                previous_last = _optional_int(
                    previous_update["last_update_id"]
                )
                current_final = _optional_int(
                    event["final_update_id"]
                )
                current_last = _optional_int(
                    event["last_update_id"]
                )

                adjacent_update_relations[
                    "final_vs_previous_final"
                ][_relation(current_final, previous_final)] += 1

                adjacent_update_relations[
                    "last_vs_previous_last"
                ][_relation(current_last, previous_last)] += 1

                adjacent_update_relations[
                    "current_last_vs_previous_final"
                ][_relation(current_last, previous_final)] += 1

                adjacent_update_relations[
                    "current_final_vs_previous_last"
                ][_relation(current_final, previous_last)] += 1

            previous_update = event

        previous_event = event
        current_key = None
        current_bid_rows = 0
        current_ask_rows = 0
        current_positive_bid_rows = 0
        current_positive_ask_rows = 0
        current_zero_quantity_rows = 0
        current_malformed_quantity_rows = 0
        current_duplicate_side_price_rows = 0
        current_side_prices = set()

    for batch in parquet.iter_batches(
        batch_size=65_536,
        columns=list(_COLUMNS),
        use_threads=True,
    ):
        projected = batch.to_pydict()
        ordered = tuple(projected[name] for name in _COLUMNS)

        for values in zip(*ordered, strict=True):
            row = dict(zip(_COLUMNS, values, strict=True))
            key = _event_key(row)

            if current_key is None or key != current_key:
                finish_event(row_count - 1)
                current_key = key
                current_first_row = row_count

            event_type_row_counts[str(row.get("event_type"))] += 1

            for field_name in _COLUMNS:
                if row.get(field_name) is None:
                    field_null_counts[field_name] += 1

            side = str(row.get("side") or "").strip().lower()
            price_text = str(row.get("price") or "").strip()
            quantity = _decimal_or_none(row.get("quantity"))

            identity = (side, price_text)

            if identity in current_side_prices:
                current_duplicate_side_price_rows += 1
            else:
                current_side_prices.add(identity)

            if side == "bid":
                current_bid_rows += 1
            elif side == "ask":
                current_ask_rows += 1

            if quantity is None:
                current_malformed_quantity_rows += 1
            elif quantity == 0:
                current_zero_quantity_rows += 1
            elif quantity > 0:
                if side == "bid":
                    current_positive_bid_rows += 1
                elif side == "ask":
                    current_positive_ask_rows += 1

            row_count += 1

    finish_event(row_count - 1)

    metadata_rows = int(parquet.metadata.num_rows)

    if row_count != metadata_rows:
        raise RuntimeError(
            "Streamed row count does not match Parquet metadata: "
            f"streamed={row_count}, metadata={metadata_rows}"
        )

    return {
        "venue": "bybit",
        "symbol": symbol,
        "hour_utc": _utc_text(hour_utc),
        "remote_path": _remote_path(
            symbol=symbol,
            hour_utc=hour_utc,
        ),
        "local_path": str(source),
        "file_size_bytes": source.stat().st_size,
        "row_group_count": int(parquet.metadata.num_row_groups),
        "row_count": row_count,
        "event_count": event_count,
        "schema_types": schema_types,
        "event_type_row_counts": dict(
            sorted(event_type_row_counts.items())
        ),
        "event_type_event_counts": dict(
            sorted(event_type_event_counts.items())
        ),
        "snapshot_event_count": snapshot_event_count,
        "complete_snapshot_candidate_count": (
            complete_snapshot_candidate_count
        ),
        "update_event_count": update_event_count,
        "received_time_regression_count": (
            received_time_regression_count
        ),
        "field_null_counts": {
            field_name: int(field_null_counts[field_name])
            for field_name in _COLUMNS
        },
        "field_null_percentages": {
            field_name: round(
                100.0 * field_null_counts[field_name] / row_count,
                6,
            )
            for field_name in _COLUMNS
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
                field_presence_patterns.items(),
                key=lambda item: repr(item[0]),
            )
        ],
        "adjacent_update_relations": {
            name: dict(sorted(counter.items()))
            for name, counter in adjacent_update_relations.items()
        },
        "first_events": first_events,
        "first_updates": first_updates,
        "last_events": list(last_events),
        "snapshot_events": snapshot_events,
    }


def _cross_hour_boundary(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> dict[str, Any]:
    previous_last_event = (
        previous["last_events"][-1]
        if previous["last_events"]
        else None
    )
    current_first_event = (
        current["first_events"][0]
        if current["first_events"]
        else None
    )
    current_first_update = (
        current["first_updates"][0]
        if current["first_updates"]
        else None
    )

    def field(
        event: dict[str, Any] | None,
        name: str,
    ) -> int | None:
        if event is None:
            return None

        return _optional_int(event.get(name))

    previous_final = field(
        previous_last_event,
        "final_update_id",
    )
    previous_last = field(
        previous_last_event,
        "last_update_id",
    )
    current_final = field(
        current_first_update,
        "final_update_id",
    )
    current_last = field(
        current_first_update,
        "last_update_id",
    )

    return {
        "venue": "bybit",
        "symbol": previous["symbol"],
        "previous_hour_utc": previous["hour_utc"],
        "current_hour_utc": current["hour_utc"],
        "previous_last_event": previous_last_event,
        "current_first_event": current_first_event,
        "current_first_update": current_first_update,
        "current_starts_with_snapshot": bool(
            current_first_event is not None
            and current_first_event["event_type"] == "snapshot"
        ),
        "relations": {
            "current_final_vs_previous_final": _relation(
                current_final,
                previous_final,
            ),
            "current_last_vs_previous_last": _relation(
                current_last,
                previous_last,
            ),
            "current_last_vs_previous_final": _relation(
                current_last,
                previous_final,
            ),
            "current_final_vs_previous_last": _relation(
                current_final,
                previous_last,
            ),
        },
    }


def build_report(
    archives: list[dict[str, Any]],
) -> dict[str, Any]:
    ordered = sorted(
        archives,
        key=lambda item: item["hour_utc"],
    )

    boundaries = [
        _cross_hour_boundary(previous, current)
        for previous, current in zip(
            ordered,
            ordered[1:],
            strict=False,
        )
    ]

    snapshot_event_count = sum(
        int(item["snapshot_event_count"])
        for item in ordered
    )
    complete_snapshot_count = sum(
        int(item["complete_snapshot_candidate_count"])
        for item in ordered
    )

    return {
        "schema": "l2shock.bybit_contract_diagnostics",
        "schema_version": 1,
        "warning": (
            "Diagnostic hypotheses are not a production sequence contract."
        ),
        "archive_count": len(ordered),
        "snapshot_event_count": snapshot_event_count,
        "complete_snapshot_candidate_count": complete_snapshot_count,
        "all_received_times_monotonic": all(
            int(item["received_time_regression_count"]) == 0
            for item in ordered
        ),
        "archives": ordered,
        "cross_hour_boundaries": boundaries,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download and inspect two adjacent Bybit order-book hours."
        )
    )
    parser.add_argument(
        "--profile",
        choices=("bybit_btc", "bybit_eth"),
        default="bybit_btc",
    )
    parser.add_argument(
        "--first-hour",
        type=_canonical_utc_hour,
        default=None,
        help=(
            "Optional first exact UTC hour. Defaults to a known "
            "snapshot-containing diagnostic anchor."
        ),
    )
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=DEFAULT_CACHE_ROOT,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_REPORT,
    )

    args = parser.parse_args(argv)

    if args.profile == "bybit_btc":
        symbol = "BTCUSDT"
        default_first = _canonical_utc_hour(
            "2026-09-04T06:00:00Z"
        )
    else:
        symbol = "ETHUSDT"
        default_first = _canonical_utc_hour(
            "2026-09-04T05:00:00Z"
        )

    first_hour = args.first_hour or default_first
    hours = (
        first_hour,
        first_hour + timedelta(hours=1),
    )

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)-8s | "
            "%(name)s | %(message)s"
        ),
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    cache_root = args.cache_root.expanduser().resolve()
    cache_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    archive_reports: list[dict[str, Any]] = []

    with httpx.Client() as client:
        for hour in hours:
            path = _download(
                client,
                symbol=symbol,
                hour_utc=hour,
                cache_root=cache_root,
            )
            report = inspect_archive(
                path,
                symbol=symbol,
                hour_utc=hour,
            )
            archive_reports.append(report)

            log.info(
                "BYBIT ARCHIVE SUMMARY: symbol=%s hour=%s "
                "rows=%d events=%d snapshot_rows=%d "
                "snapshot_events=%d complete_snapshot_candidates=%d "
                "updates=%d received_time_regressions=%d",
                symbol,
                report["hour_utc"],
                report["row_count"],
                report["event_count"],
                report["event_type_row_counts"].get("snapshot", 0),
                report["snapshot_event_count"],
                report["complete_snapshot_candidate_count"],
                report["update_event_count"],
                report["received_time_regression_count"],
            )

            log.info(
                "BYBIT FIELD PATTERNS: symbol=%s hour=%s patterns=%s",
                symbol,
                report["hour_utc"],
                json.dumps(
                    report["field_presence_patterns"],
                    ensure_ascii=True,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )

            log.info(
                "BYBIT UPDATE RELATIONS: symbol=%s hour=%s relations=%s",
                symbol,
                report["hour_utc"],
                json.dumps(
                    report["adjacent_update_relations"],
                    ensure_ascii=True,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )

            log.info(
                "BYBIT SNAPSHOT EVENTS: symbol=%s hour=%s snapshots=%s",
                symbol,
                report["hour_utc"],
                json.dumps(
                    report["snapshot_events"],
                    ensure_ascii=True,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )

    payload = build_report(archive_reports)

    destination = args.output.expanduser().resolve()
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    destination.write_text(
        json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    log.info(
        "BYBIT CROSS-HOUR BOUNDARIES: %s",
        json.dumps(
            payload["cross_hour_boundaries"],
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    log.info(
        "Bybit diagnostic report written: path=%s bytes=%d",
        destination,
        destination.stat().st_size,
    )

    print(
        json.dumps(
            {
                "profile": args.profile,
                "symbol": symbol,
                "archive_count": payload["archive_count"],
                "snapshot_event_count": (
                    payload["snapshot_event_count"]
                ),
                "complete_snapshot_candidate_count": (
                    payload["complete_snapshot_candidate_count"]
                ),
                "all_received_times_monotonic": (
                    payload["all_received_times_monotonic"]
                ),
                "output": str(destination),
            },
            indent=2,
            sort_keys=True,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
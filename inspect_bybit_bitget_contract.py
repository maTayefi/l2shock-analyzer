#!/usr/bin/env python3
"""
Deep venue contract inspector for Bybit and Bitget.

Gathers the evidence required to build strict sequence adapters:
1. Downloads N consecutive hourly archives (default 24) for a venue/symbol.
2. Searches backward day-by-day for a snapshot event (up to 7 days).
3. Reports per-archive: event types, field nullability, ID patterns.
4. Reports cross-hour: final_update_id continuity between adjacent hours.
5. For Bybit: checks whether final_update_id increments by exactly 1.
6. Reports received_time monotonicity violations.
7. Samples quantity values to help determine unit conventions.

Usage:
    py -3.14 inspect_bybit_bitget_contract.py --venue bybit --symbol BTCUSDT --hours 24
    py -3.14 inspect_bybit_bitget_contract.py --venue bitget_futures --symbol BTCUSDT --hours 24
    py -3.14 inspect_bybit_bitget_contract.py --venue bybit --symbol BTCUSDT --hours 24 --date 2026-09-08
    py -3.14 inspect_bybit_bitget_contract.py --venue bybit --symbol ETHUSDT --hours 12 --search-snapshot-days 7
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pyarrow.parquet as pq

BASE_URL = "https://api.cryptohftdata.com/v1/download"
CACHE_DIR = Path("cache_venue_inspection")
OUTPUT_REPORT = "venue_contract_deep_inspection.json"

# Maximum rows to sample from each archive for detailed inspection
MAX_SAMPLE_ROWS = 50


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _download_file(client: httpx.Client, remote_path: str, dest: Path) -> bool:
    if dest.exists() and dest.stat().st_size > 0:
        return True
    url = f"{BASE_URL}?file={remote_path}"
    try:
        with client.stream("GET", url, timeout=120.0, follow_redirects=True) as resp:
            if resp.status_code != 200:
                return False
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1 << 16):
                    f.write(chunk)
        return True
    except Exception:
        return False


def _head_check(client: httpx.Client, remote_path: str) -> int:
    url = f"{BASE_URL}?file={remote_path}"
    try:
        resp = client.head(url, timeout=15.0, follow_redirects=True)
        return resp.status_code
    except Exception:
        return -1


def inspect_archive_deep(
    path: Path,
    *,
    max_sample_rows: int = MAX_SAMPLE_ROWS,
) -> dict:
    """Stream one archive and extract contract evidence."""
    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        return {"error": str(e), "path": str(path)}

    schema_names = list(pf.schema_arrow.names)
    total_rows = int(pf.metadata.num_rows)

    # Counters
    event_type_rows: Counter[str] = Counter()
    event_type_events: Counter[str] = Counter()
    field_null_counts: dict[str, int] = {name: 0 for name in schema_names}

    # ID tracking
    first_final_update_id = None
    last_final_update_id = None
    first_last_update_id = None
    last_last_update_id = None
    first_first_update_id = None
    last_first_update_id = None
    first_prev_final_update_id = None
    last_prev_final_update_id = None

    # Bybit-specific: check if final_update_id increments by 1
    final_id_increments_by_one = True
    prev_final_id_seen = None
    final_id_non_one_increment_count = 0

    # received_time monotonicity
    received_time_regressions = 0
    prev_received_time = None

    # Snapshot tracking
    has_snapshot = False
    snapshot_row_count = 0
    snapshot_first_row = None
    snapshot_last_row = None
    snapshot_sample_rows: list[dict] = []

    # Update tracking
    update_count = 0
    first_update_sample_rows: list[dict] = []
    last_update_sample_rows: deque[dict] = deque(maxlen=5)

    # Quantity samples
    quantity_samples: list[str] = []
    price_samples: list[str] = []

    # Stream all rows
    current_event_key = None
    current_event_type = None
    current_event_rows = 0
    row_number = 0

    for batch in pf.iter_batches(batch_size=65536, use_threads=True):
        rows = batch.to_pylist()
        for row in rows:
            row_number += 1

            # Event type counting
            et = str(row.get("event_type", ""))
            event_type_rows[et] += 1

            # Null tracking
            for name in schema_names:
                if row.get(name) is None:
                    field_null_counts[name] += 1

            # received_time monotonicity
            rt = row.get("received_time")
            if rt is not None and prev_received_time is not None:
                if rt < prev_received_time:
                    received_time_regressions += 1
            prev_received_time = rt

            # Track IDs
            fuid = row.get("final_update_id")
            luid = row.get("last_update_id")
            fiuid = row.get("first_update_id")
            pfuid = row.get("prev_final_update_id")

            if first_final_update_id is None and fuid is not None:
                first_final_update_id = fuid
            if fuid is not None:
                last_final_update_id = fuid

            if first_last_update_id is None and luid is not None:
                first_last_update_id = luid
            if luid is not None:
                last_last_update_id = luid

            if first_first_update_id is None and fiuid is not None:
                first_first_update_id = fiuid
            if fiuid is not None:
                last_first_update_id = fiuid

            if first_prev_final_update_id is None and pfuid is not None:
                first_prev_final_update_id = pfuid
            if pfuid is not None:
                last_prev_final_update_id = pfuid

            # Bybit increment check (per-row, within same event_type=update)
            if et == "update" and fuid is not None:
                if prev_final_id_seen is not None:
                    if fuid != prev_final_id_seen + 1:
                        final_id_increments_by_one = False
                        final_id_non_one_increment_count += 1
                prev_final_id_seen = fuid

            # Event grouping
            event_key = (
                row.get("symbol"),
                et,
                row.get("received_time"),
                row.get("event_time"),
                row.get("transaction_time"),
                row.get("first_update_id"),
                row.get("final_update_id"),
                row.get("prev_final_update_id"),
                row.get("last_update_id"),
            )

            if event_key != current_event_key:
                # Finish previous event
                if current_event_type is not None:
                    event_type_events[current_event_type] += 1

                current_event_key = event_key
                current_event_type = et
                current_event_rows = 1

                if et == "snapshot":
                    has_snapshot = True
                    snapshot_row_count += 1
                    if snapshot_first_row is None:
                        snapshot_first_row = row_number
                    snapshot_last_row = row_number
                    if len(snapshot_sample_rows) < max_sample_rows:
                        snapshot_sample_rows.append(dict(row))

                elif et == "update":
                    update_count += 1
                    if len(first_update_sample_rows) < 5:
                        first_update_sample_rows.append(dict(row))
                    last_update_sample_rows.append(dict(row))
            else:
                current_event_rows += 1
                if et == "snapshot":
                    snapshot_row_count += 1
                    snapshot_last_row = row_number

            # Quantity/price samples
            q = row.get("quantity")
            p = row.get("price")
            if q is not None and len(quantity_samples) < 20:
                quantity_samples.append(str(q))
            if p is not None and len(price_samples) < 20:
                price_samples.append(str(p))

    # Finish last event
    if current_event_type is not None:
        event_type_events[current_event_type] += 1

    return {
        "path": str(path),
        "file_size_bytes": path.stat().st_size,
        "total_rows": total_rows,
        "row_group_count": int(pf.metadata.num_row_groups),
        "schema_columns": schema_names,
        "event_type_row_counts": dict(sorted(event_type_rows.items())),
        "event_type_event_counts": dict(sorted(event_type_events.items())),
        "field_null_counts": {
            k: v for k, v in sorted(field_null_counts.items()) if v > 0
        },
        "field_null_percentages": {
            k: round(v / total_rows * 100, 2)
            for k, v in sorted(field_null_counts.items())
            if v > 0
        },
        "has_snapshot": has_snapshot,
        "snapshot_row_count": snapshot_row_count,
        "snapshot_first_row": snapshot_first_row,
        "snapshot_last_row": snapshot_last_row,
        "update_event_count": update_count,
        "id_ranges": {
            "first_final_update_id": first_final_update_id,
            "last_final_update_id": last_final_update_id,
            "first_last_update_id": first_last_update_id,
            "last_last_update_id": last_last_update_id,
            "first_first_update_id": first_first_update_id,
            "last_first_update_id": last_first_update_id,
            "first_prev_final_update_id": first_prev_final_update_id,
            "last_prev_final_update_id": last_prev_final_update_id,
        },
        "final_update_id_increments_by_one": final_id_increments_by_one,
        "final_id_non_one_increment_count": final_id_non_one_increment_count,
        "received_time_regressions": received_time_regressions,
        "snapshot_sample_rows": snapshot_sample_rows[:10],
        "first_update_sample_rows": first_update_sample_rows[:5],
        "last_update_sample_rows": list(last_update_sample_rows)[:5],
        "quantity_samples": quantity_samples[:20],
        "price_samples": price_samples[:20],
    }


def run_deep_inspection(
    venue: str,
    symbol: str,
    *,
    hours: int = 24,
    start_date: str | None = None,
    search_snapshot_days: int = 7,
    output_path: str = OUTPUT_REPORT,
) -> None:
    CACHE_DIR.mkdir(exist_ok=True)

    if start_date:
        base_date = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    else:
        # Use yesterday to ensure data is available
        base_date = (_utc_now() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    print(f"=== Deep Venue Contract Inspection ===")
    print(f"Venue: {venue}")
    print(f"Symbol: {symbol}")
    print(f"Hours: {hours}")
    print(f"Base date: {base_date.strftime('%Y-%m-%d')}")
    print(f"Snapshot search days: {search_snapshot_days}")
    print()

    client = httpx.Client()
    report: dict = {
        "generated_at": _utc_now().isoformat(),
        "venue": venue,
        "symbol": symbol,
        "hours_requested": hours,
        "base_date": base_date.strftime("%Y-%m-%d"),
        "search_snapshot_days": search_snapshot_days,
    }

    # Phase 1: Check which hours are available
    print("--- Phase 1: Checking availability ---")
    available_hours: list[tuple[datetime, str]] = []

    for h in range(hours):
        dt = base_date + timedelta(hours=h)
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")
        remote_path = f"{venue}/{date_str}/{hour_str}/{symbol}_orderbook.parquet"
        status = _head_check(client, remote_path)
        if status == 200:
            available_hours.append((dt, remote_path))
            print(f"  [OK] {date_str}/{hour_str}")
        else:
            print(f"  [MISSING] {date_str}/{hour_str} (status={status})")
        time.sleep(0.3)

    if not available_hours:
        print("\nERROR: No archives available. Try a different date.")
        sys.exit(1)

    print(f"\nAvailable: {len(available_hours)} / {hours} hours")

    # Phase 2: Download and inspect each archive
    print("\n--- Phase 2: Downloading and inspecting ---")
    archive_reports: list[dict] = []

    for i, (dt, remote_path) in enumerate(available_hours):
        local_name = f"{venue}_{dt.strftime('%Y-%m-%d_%H')}_{symbol}_orderbook.parquet"
        local_path = CACHE_DIR / local_name

        print(f"  [{i+1}/{len(available_hours)}] {remote_path}")
        ok = _download_file(client, remote_path, local_path)
        if not ok:
            print(f"    DOWNLOAD FAILED")
            continue

        result = inspect_archive_deep(local_path)
        result["hour_utc"] = dt.isoformat()
        result["remote_path"] = remote_path
        archive_reports.append(result)

        # Brief status
        has_snap = result.get("has_snapshot", False)
        inc_by_one = result.get("final_update_id_increments_by_one", None)
        print(
            f"    rows={result.get('total_rows', '?')}, "
            f"snapshot={'YES' if has_snap else 'no'}, "
            f"final_id+1={'YES' if inc_by_one else 'NO'}"
        )
        time.sleep(0.5)

    # Phase 3: Search backward for snapshot if none found
    snapshot_found = any(r.get("has_snapshot", False) for r in archive_reports)

    if not snapshot_found and search_snapshot_days > 0:
        print("\n--- Phase 3: Searching backward for snapshot ---")
        search_results: list[dict] = []

        for day_offset in range(1, search_snapshot_days + 1):
            search_date = base_date - timedelta(days=day_offset)
            print(f"\n  Searching {search_date.strftime('%Y-%m-%d')}...")

            # Check a few hours in this day
            for h in [0, 6, 12, 18]:
                dt = search_date + timedelta(hours=h)
                date_str = dt.strftime("%Y-%m-%d")
                hour_str = dt.strftime("%H")
                remote_path = (
                    f"{venue}/{date_str}/{hour_str}/{symbol}_orderbook.parquet"
                )

                status = _head_check(client, remote_path)
                if status != 200:
                    continue

                local_name = f"{venue}_{date_str}_{hour_str}_{symbol}_orderbook.parquet"
                local_path = CACHE_DIR / local_name

                ok = _download_file(client, remote_path, local_path)
                if not ok:
                    continue

                result = inspect_archive_deep(local_path, max_sample_rows=20)
                result["hour_utc"] = dt.isoformat()
                result["remote_path"] = remote_path
                result["search_day_offset"] = day_offset
                search_results.append(result)

                has_snap = result.get("has_snapshot", False)
                print(
                    f"    {date_str}/{hour_str}: "
                    f"snapshot={'YES' if has_snap else 'no'}, "
                    f"rows={result.get('total_rows', '?')}"
                )

                if has_snap:
                    snapshot_found = True
                    print(f"\n  *** SNAPSHOT FOUND at {dt.isoformat()} ***")
                    break

                time.sleep(0.5)

            if snapshot_found:
                break

        report["snapshot_search_results"] = search_results
    else:
        report["snapshot_search_results"] = []

    # Phase 4: Cross-hour continuity analysis
    print("\n--- Phase 4: Cross-hour continuity ---")
    cross_hour_analysis: list[dict] = []

    for i in range(len(archive_reports) - 1):
        curr = archive_reports[i]
        nxt = archive_reports[i + 1]

        curr_last_final = curr.get("id_ranges", {}).get("last_final_update_id")
        curr_last_last = curr.get("id_ranges", {}).get("last_last_update_id")
        nxt_first_final = nxt.get("id_ranges", {}).get("first_final_update_id")
        nxt_first_last = nxt.get("id_ranges", {}).get("first_last_update_id")
        nxt_first_prev = nxt.get("id_ranges", {}).get("first_prev_final_update_id")

        analysis = {
            "hour_n": curr.get("hour_utc"),
            "hour_n_plus_1": nxt.get("hour_utc"),
            "hour_n_last_final_update_id": curr_last_final,
            "hour_n_last_last_update_id": curr_last_last,
            "hour_n1_first_final_update_id": nxt_first_final,
            "hour_n1_first_last_update_id": nxt_first_last,
            "hour_n1_first_prev_final_update_id": nxt_first_prev,
            "n1_starts_with_snapshot": nxt.get("has_snapshot", False),
        }

        # Check various continuity hypotheses
        if curr_last_final is not None and nxt_first_final is not None:
            analysis["final_id_gap"] = nxt_first_final - curr_last_final
            analysis["final_id_continuous"] = nxt_first_final == curr_last_final + 1

        if curr_last_final is not None and nxt_first_prev is not None:
            analysis["prev_matches_previous_final"] = nxt_first_prev == curr_last_final

        if curr_last_last is not None and nxt_first_last is not None:
            analysis["last_id_gap"] = nxt_first_last - curr_last_last

        cross_hour_analysis.append(analysis)

        gap = analysis.get("final_id_gap", "?")
        cont = analysis.get("final_id_continuous", "?")
        print(
            f"  {curr.get('hour_utc', '?')} -> {nxt.get('hour_utc', '?')}: "
            f"final_id_gap={gap}, continuous={cont}, "
            f"next_snapshot={nxt.get('has_snapshot', False)}"
        )

    report["cross_hour_analysis"] = cross_hour_analysis

    # Phase 5: Summary and conclusions
    print("\n--- Phase 5: Summary ---")

    summary = {
        "total_archives_inspected": len(archive_reports),
        "snapshot_found": snapshot_found,
        "all_final_id_increment_by_one": all(
            r.get("final_update_id_increments_by_one", False) for r in archive_reports
        ),
        "any_received_time_regressions": any(
            r.get("received_time_regressions", 0) > 0 for r in archive_reports
        ),
        "total_received_time_regressions": sum(
            r.get("received_time_regressions", 0) for r in archive_reports
        ),
        "field_nullability_summary": {},
    }

    # Aggregate nullability across all archives
    all_fields: set[str] = set()
    for r in archive_reports:
        all_fields.update(r.get("field_null_counts", {}).keys())

    for field in sorted(all_fields):
        null_count = sum(
            r.get("field_null_counts", {}).get(field, 0) for r in archive_reports
        )
        total = sum(r.get("total_rows", 0) for r in archive_reports)
        if total > 0:
            summary["field_nullability_summary"][field] = {
                "null_rows": null_count,
                "total_rows": total,
                "null_percent": round(null_count / total * 100, 2),
            }

    report["summary"] = summary
    report["archives"] = archive_reports

    # Print conclusions
    print(f"\n  Snapshot found: {snapshot_found}")
    print(
        f"  All final_update_id increment by 1: "
        f"{summary['all_final_id_increment_by_one']}"
    )
    print(
        f"  received_time regressions: " f"{summary['total_received_time_regressions']}"
    )
    print(f"\n  Field nullability:")
    for field, info in summary["field_nullability_summary"].items():
        if info["null_percent"] > 0:
            print(f"    {field}: {info['null_percent']}% null")

    # Write report
    output = Path(output_path)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nReport written to: {output} ({output.stat().st_size:,} bytes)")
    print("\nDone.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Deep venue contract inspection for Bybit/Bitget. "
            "Gathers evidence needed to build strict sequence adapters."
        )
    )
    parser.add_argument(
        "--venue",
        required=True,
        choices=["bybit", "bitget_futures", "okx_futures", "binance_futures"],
        help="Venue directory name in CryptoHFTData.",
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="Symbol, e.g. BTCUSDT or ETHUSDT or BTC-USDT-SWAP.",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Number of consecutive hours to inspect (default: 24).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Start date YYYY-MM-DD. Default: yesterday.",
    )
    parser.add_argument(
        "--search-snapshot-days",
        type=int,
        default=7,
        help=(
            "If no snapshot found in initial hours, search backward "
            "this many days (default: 7). Set 0 to disable."
        ),
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_REPORT,
        help=f"Output JSON report path (default: {OUTPUT_REPORT}).",
    )

    args = parser.parse_args()

    run_deep_inspection(
        venue=args.venue,
        symbol=args.symbol,
        hours=args.hours,
        start_date=args.date,
        search_snapshot_days=args.search_snapshot_days,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()

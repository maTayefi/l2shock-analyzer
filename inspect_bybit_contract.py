#!/usr/bin/env python3
"""
Deep inspection of Bybit orderbook Parquet files from CryptoHFTData.
Downloads sample files and analyzes:
  - Event types (snapshot vs update)
  - Sequence ID fields (which are null/non-null)
  - Continuity patterns between events
  - Cross-hour boundary behavior

Usage:
    py -3.14 inspect_bybit_contract.py --venue-path bybit --symbol BTCUSDT --date 2026-01-15 --hours 3
    py -3.14 inspect_bybit_contract.py --venue-path bybit --symbol ETHUSDT --date 2026-01-15 --hours 2 --output-dir cache_bybit
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import httpx
    import pyarrow.parquet as pq
except ImportError as e:
    print(f"ERROR: Missing dependency: {e}. Run: pip install httpx pyarrow")
    sys.exit(1)

BASE_URL = "https://api.cryptohftdata.com/v1/download"


def download_file(client: httpx.Client, remote_path: str, dest: Path) -> bool:
    """Download a file from CryptoHFTData."""
    if dest.exists() and dest.stat().st_size > 0:
        print(f"    [CACHED] {dest.name}")
        return True

    url = f"{BASE_URL}?file={remote_path}"
    try:
        with client.stream("GET", url, timeout=120.0, follow_redirects=True) as resp:
            if resp.status_code != 200:
                print(f"    [HTTP {resp.status_code}] {remote_path}")
                return False
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1 << 16):
                    f.write(chunk)
            print(f"    [OK] {dest.name} ({dest.stat().st_size:,} bytes)")
            return True
    except Exception as e:
        print(f"    [ERROR] {remote_path}: {e}")
        return False


def inspect_parquet_deep(path: Path) -> dict:
    """Deep inspection of one Parquet file."""
    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        return {"error": str(e), "path": str(path)}

    schema_names = list(pf.schema_arrow.names)
    total_rows = int(pf.metadata.num_rows)

    # Counters
    event_type_counts = Counter()
    field_null_counts = defaultdict(int)
    field_non_null_counts = defaultdict(int)

    # Sequence tracking
    first_update_ids = []
    final_update_ids = []
    prev_final_update_ids = []
    last_update_ids = []
    transaction_times = []
    received_times = []

    # Snapshot tracking
    snapshot_count = 0
    snapshot_rows_sample = []
    first_rows = []
    last_rows = []

    # Event grouping
    events = []
    current_event_key = None
    current_event_rows = []
    current_event_type = None

    # Continuity analysis
    final_id_gaps = []
    received_time_regressions = 0
    prev_received_time = None

    row_number = 0

    for batch in pf.iter_batches(batch_size=65536, use_threads=True):
        rows = batch.to_pylist()
        for row in rows:
            row_number += 1

            # Null tracking
            for name in schema_names:
                if row.get(name) is None:
                    field_null_counts[name] += 1
                else:
                    field_non_null_counts[name] += 1

            # Event type
            et = str(row.get("event_type", ""))
            event_type_counts[et] += 1

            # Track IDs
            fuid = row.get("final_update_id")
            luid = row.get("last_update_id")
            fiuid = row.get("first_update_id")
            pfuid = row.get("prev_final_update_id")
            tt = row.get("transaction_time")
            rt = row.get("received_time")

            if fuid is not None:
                final_update_ids.append(fuid)
            if luid is not None:
                last_update_ids.append(luid)
            if fiuid is not None:
                first_update_ids.append(fiuid)
            if pfuid is not None:
                prev_final_update_ids.append(pfuid)
            if tt is not None:
                transaction_times.append(tt)
            if rt is not None:
                received_times.append(rt)

            # received_time monotonicity
            if rt is not None and prev_received_time is not None:
                if rt < prev_received_time:
                    received_time_regressions += 1
            if rt is not None:
                prev_received_time = rt

            # Snapshot tracking
            if et == "snapshot":
                snapshot_count += 1
                if len(snapshot_rows_sample) < 5:
                    snapshot_rows_sample.append(dict(row))

            # Track first/last rows
            if len(first_rows) < 5:
                first_rows.append(dict(row))
            if row_number > total_rows - 5:
                last_rows.append(dict(row))

            # Event grouping
            event_key = (
                row.get("symbol"),
                et,
                rt,
                row.get("event_time"),
                tt,
                fiuid,
                fuid,
                pfuid,
                luid,
            )
            if event_key != current_event_key:
                if current_event_rows:
                    events.append({
                        "type": current_event_type,
                        "row_count": len(current_event_rows),
                        "first_update_id": current_event_rows[0].get("first_update_id"),
                        "final_update_id": current_event_rows[0].get("final_update_id"),
                        "prev_final_update_id": current_event_rows[0].get("prev_final_update_id"),
                        "last_update_id": current_event_rows[0].get("last_update_id"),
                    })
                current_event_key = event_key
                current_event_type = et
                current_event_rows = [dict(row)]
            else:
                current_event_rows.append(dict(row))

    # Final event
    if current_event_rows:
        events.append({
            "type": current_event_type,
            "row_count": len(current_event_rows),
            "first_update_id": current_event_rows[0].get("first_update_id"),
            "final_update_id": current_event_rows[0].get("final_update_id"),
            "prev_final_update_id": current_event_rows[0].get("prev_final_update_id"),
            "last_update_id": current_event_rows[0].get("last_update_id"),
        })

    # Final ID continuity analysis
    final_id_increment_patterns = Counter()
    for i in range(1, min(len(final_update_ids), 50000)):
        diff = final_update_ids[i] - final_update_ids[i - 1]
        if diff == 0:
            final_id_increment_patterns["same_event"] += 1
        elif diff == 1:
            final_id_increment_patterns["increment_by_1"] += 1
        elif diff > 1:
            final_id_increment_patterns[f"increment_by_{min(diff, 100)}"] += 1
            if len(final_id_gaps) < 10:
                final_id_gaps.append({
                    "index": i,
                    "prev": final_update_ids[i - 1],
                    "curr": final_update_ids[i],
                    "gap": diff,
                })
        else:
            final_id_increment_patterns["negative/regression"] += 1

    # prev_final_update_id continuity check
    prev_final_matches = 0
    prev_final_mismatches = 0
    for i in range(1, min(len(prev_final_update_ids), len(final_update_ids), 50000)):
        if prev_final_update_ids[i] == final_update_ids[i - 1]:
            prev_final_matches += 1
        else:
            prev_final_mismatches += 1

    # last_update_id continuity check
    last_id_matches = 0
    last_id_mismatches = 0
    for i in range(1, min(len(last_update_ids), len(final_update_ids), 50000)):
        if last_update_ids[i] == final_update_ids[i - 1]:
            last_id_matches += 1
        elif last_update_ids[i] == last_update_ids[i - 1]:
            pass  # same event
        else:
            last_id_mismatches += 1

    # Field nullability summary
    field_summary = {}
    for name in schema_names:
        null_count = field_null_counts.get(name, 0)
        non_null = field_non_null_counts.get(name, 0)
        total = null_count + non_null
        if total > 0:
            field_summary[name] = {
                "null_count": null_count,
                "non_null_count": non_null,
                "null_percent": round(null_count / total * 100, 2),
                "always_null": null_count == total,
                "never_null": null_count == 0,
            }

    return {
        "path": str(path),
        "file_size_bytes": path.stat().st_size,
        "total_rows": total_rows,
        "row_group_count": int(pf.metadata.num_row_groups),
        "schema_columns": schema_names,
        "event_type_counts": dict(sorted(event_type_counts.items())),
        "event_count": len(events),
        "snapshot_count": snapshot_count,
        "has_snapshot": snapshot_count > 0,
        "snapshot_rows_sample": snapshot_rows_sample[:3],
        "first_rows_sample": first_rows[:3],
        "last_rows_sample": last_rows[:3],
        "field_summary": field_summary,
        "sequence_analysis": {
            "first_update_id_count": len(first_update_ids),
            "final_update_id_count": len(final_update_ids),
            "prev_final_update_id_count": len(prev_final_update_ids),
            "last_update_id_count": len(last_update_ids),
            "transaction_time_count": len(transaction_times),
            "final_update_id_first": final_update_ids[0] if final_update_ids else None,
            "final_update_id_last": final_update_ids[-1] if final_update_ids else None,
            "last_update_id_first": last_update_ids[0] if last_update_ids else None,
            "last_update_id_last": last_update_ids[-1] if last_update_ids else None,
            "final_id_increment_patterns": dict(final_id_increment_patterns),
            "final_id_gaps_sample": final_id_gaps[:5],
            "prev_final_matches_final": prev_final_matches,
            "prev_final_mismatches": prev_final_mismatches,
            "last_id_matches_final": last_id_matches,
            "last_id_mismatches": last_id_mismatches,
            "received_time_regressions": received_time_regressions,
        },
        "events_sample_first_5": events[:5],
        "events_sample_last_5": events[-5:],
        "events_type_distribution": dict(Counter(e["type"] for e in events)),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Deep inspection of Bybit Parquet files from CryptoHFTData."
    )
    parser.add_argument(
        "--venue-path",
        required=True,
        help="Venue path on CryptoHFTData (e.g., 'bybit', 'bybit_futures')",
    )
    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Symbol to inspect (default: BTCUSDT)",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to inspect (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=3,
        help="Number of consecutive hours to inspect (default: 3)",
    )
    parser.add_argument(
        "--start-hour",
        type=int,
        default=12,
        help="Starting hour (default: 12)",
    )
    parser.add_argument(
        "--output-dir",
        default="cache_bybit_inspection",
        help="Local cache directory for downloaded files",
    )
    parser.add_argument(
        "--output",
        default="bybit_contract_report.json",
        help="Output JSON report path",
    )
    args = parser.parse_args()

    venue_path = args.venue_path
    symbol = args.symbol
    date_str = args.date
    start_hour = args.start_hour
    hours = args.hours
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"=== Deep Bybit Contract Inspection ===")
    print(f"Venue path: {venue_path}")
    print(f"Symbol: {symbol}")
    print(f"Date: {date_str}")
    print(f"Hours: {start_hour:02d} to {start_hour + hours - 1:02d}")
    print(f"Cache dir: {output_dir}")
    print()

    client = httpx.Client()
    report = {
        "venue_path": venue_path,
        "symbol": symbol,
        "date": date_str,
        "hours_inspected": hours,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": [],
    }

    for h in range(hours):
        hour = start_hour + h
        remote_path = f"{venue_path}/{date_str}/{hour:02d}/{symbol}_orderbook.parquet"
        local_path = output_dir / f"{venue_path}_{date_str}_{hour:02d}_{symbol}_orderbook.parquet"

        print(f"  Hour {hour:02d}: {remote_path}")

        if not download_file(client, remote_path, local_path):
            report["files"].append({"hour": hour, "error": "download_failed"})
            continue

        print(f"    Inspecting...")
        result = inspect_parquet_deep(local_path)
        result["hour"] = hour
        result["remote_path"] = remote_path
        report["files"].append(result)

        # Print quick summary
        if "error" not in result:
            print(f"    Rows: {result['total_rows']:,}")
            print(f"    Events: {result['event_count']:,}")
            print(f"    Snapshots: {result['snapshot_count']}")
            print(f"    Event types: {result['event_type_counts']}")

            seq = result["sequence_analysis"]
            print(f"    first_update_id present: {seq['first_update_id_count']}")
            print(f"    final_update_id present: {seq['final_update_id_count']}")
            print(f"    prev_final_update_id present: {seq['prev_final_update_id_count']}")
            print(f"    last_update_id present: {seq['last_update_id_count']}")
            print(f"    transaction_time present: {seq['transaction_time_count']}")
            print(f"    ID patterns: {seq['final_id_increment_patterns']}")
        print()

        time.sleep(0.5)

    client.close()

    # Cross-hour analysis
    print("=" * 60)
    print("CROSS-HOUR ANALYSIS")
    print("=" * 60)

    valid_files = [f for f in report["files"] if "error" not in f]
    if len(valid_files) >= 2:
        for i in range(len(valid_files) - 1):
            curr = valid_files[i]
            nxt = valid_files[i + 1]
            curr_seq = curr.get("sequence_analysis", {})
            nxt_seq = nxt.get("sequence_analysis", {})

            curr_last_final = curr_seq.get("final_update_id_last")
            nxt_first_final = nxt_seq.get("final_update_id_first")
            curr_last_luid = curr_seq.get("last_update_id_last")
            nxt_first_luid = nxt_seq.get("last_update_id_first")

            print(f"  Hour {curr['hour']:02d} -> {nxt['hour']:02d}:")
            print(f"    final_update_id: {curr_last_final} -> {nxt_first_final}")
            if curr_last_final and nxt_first_final:
                gap = nxt_first_final - curr_last_final
                print(f"    Gap: {gap}")
                print(f"    Continuous (gap=1): {gap == 1}")
            print(f"    last_update_id: {curr_last_luid} -> {nxt_first_luid}")
            print(f"    Next hour has snapshot: {nxt.get('has_snapshot', False)}")
            print()

    # Key findings summary
    print("=" * 60)
    print("KEY FINDINGS FOR BYBIT ADAPTER")
    print("=" * 60)

    if valid_files:
        first_file = valid_files[0]
        seq = first_file.get("sequence_analysis", {})

        print(f"  Has snapshots: {first_file.get('has_snapshot', False)}")
        print(f"  Snapshot count: {first_file.get('snapshot_count', 0)}")
        print()
        print("  Field nullability:")
        for field, info in first_file.get("field_summary", {}).items():
            status = "ALWAYS NULL" if info["always_null"] else (
                "NEVER NULL" if info["never_null"] else f"{info['null_percent']}% null"
            )
            print(f"    {field}: {status}")
        print()
        print("  Sequence ID patterns:")
        patterns = seq.get("final_id_increment_patterns", {})
        for pattern, count in sorted(patterns.items(), key=lambda x: -x[1]):
            print(f"    {pattern}: {count}")
        print()
        print("  Continuity checks:")
        print(f"    prev_final matches previous final: {seq.get('prev_final_matches_final', 0)}")
        print(f"    prev_final mismatches: {seq.get('prev_final_mismatches', 0)}")
        print(f"    last_update_id matches previous final: {seq.get('last_id_matches_final', 0)}")
        print(f"    last_update_id mismatches: {seq.get('last_id_mismatches', 0)}")
        print(f"    received_time regressions: {seq.get('received_time_regressions', 0)}")

        print()
        if first_file.get("has_snapshot"):
            print("  >>> BYBIT HAS SNAPSHOTS - Similar to OKX (easier path)")
            print("  >>> Estimated: 6-8 GPT messages for full Bybit support")
        else:
            print("  >>> BYBIT IS UPDATE-ONLY - Similar to Binance (harder path)")
            print("  >>> Needs checkpoint bootstrapping")
            print("  >>> Estimated: 8-10 GPT messages for full Bybit support")
    else:
        print("  NO VALID FILES FOUND")
        print("  Cannot determine Bybit sequence contract")

    # Write report
    output_path = Path(args.output)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull report written to: {output_path}")


if __name__ == "__main__":
    main()
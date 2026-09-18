# inspect_bybit_latest.py
#!/usr/bin/env python3
"""
Probe the LAST ~72 hours of CryptoHFTData for Bybit (files are deleted after
~72h), download the NEWEST available orderbook hour, and dump its schema,
keys, event types, field nullability, and any snapshot rows.

Usage:
    py -3.14 inspect_bybit_latest.py
    py -3.14 inspect_bybit_latest.py --venue bybit --symbol BTCUSDT --hours-back 72
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pyarrow.parquet as pq

BASE_URL = "https://api.cryptohftdata.com/v1/download"
CACHE_DIR = Path("cache_bybit_latest")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def head_check(client: httpx.Client, remote_path: str) -> int:
    url = f"{BASE_URL}?file={remote_path}"
    try:
        resp = client.head(url, timeout=15.0, follow_redirects=True)
        return resp.status_code
    except Exception:
        return -1


def download_file(client: httpx.Client, remote_path: str, dest: Path) -> bool:
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


def find_latest_hour(client, venue, symbol, hours_back):
    """Newest-first scan of the last `hours_back` hours."""
    now = utc_now()
    # Start at current hour, go backward. Newest available wins.
    for hours_ago in range(0, hours_back + 1):
        probe = now - timedelta(hours=hours_ago)
        date_str = probe.strftime("%Y-%m-%d")
        hour_str = probe.strftime("%H")
        path = f"{venue}/{date_str}/{hour_str}/{symbol}_orderbook.parquet"
        status = head_check(client, path)
        print(f"  [{hours_ago:>3}h ago] {date_str}/{hour_str} -> HTTP {status}")
        if status == 200:
            print(f"\n  >>> FOUND newest available hour: {date_str}/{hour_str}")
            return date_str, hour_str, path
    return None, None, None


def inspect_parquet(path: Path) -> dict:
    """Full structural + snapshot inspection of one orderbook parquet."""
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    schema_names = list(schema.names)
    schema_types = {
        schema.field(i).name: str(schema.field(i).type)
        for i in range(len(schema_names))
    }
    total_rows = pf.metadata.num_rows

    event_types = Counter()
    null_counts = {n: 0 for n in schema_names}
    snapshot_rows = []
    first_rows = []
    last_rows = []
    rows_read = 0

    # Sequence ID ranges
    id_fields = [
        "first_update_id",
        "final_update_id",
        "prev_final_update_id",
        "last_update_id",
    ]
    id_ranges = {f: {"min": None, "max": None, "count": 0} for f in id_fields}

    for batch in pf.iter_batches(batch_size=65536, use_threads=True):
        for row in batch.to_pylist():
            rows_read += 1
            et = row.get("event_type")
            if et:
                event_types[et] += 1

            for n in schema_names:
                if row.get(n) is None:
                    null_counts[n] += 1

            # Snapshot detection
            if et == "snapshot":
                if len(snapshot_rows) < 10:
                    snapshot_rows.append(dict(row))

            # ID ranges
            for f in id_fields:
                v = row.get(f)
                if v is not None:
                    id_ranges[f]["count"] += 1
                    if id_ranges[f]["min"] is None or v < id_ranges[f]["min"]:
                        id_ranges[f]["min"] = v
                    if id_ranges[f]["max"] is None or v > id_ranges[f]["max"]:
                        id_ranges[f]["max"] = v

            if len(first_rows) < 3:
                first_rows.append(dict(row))
            if rows_read > total_rows - 3:
                last_rows.append(dict(row))

    null_pct = {
        n: round(null_counts[n] / total_rows * 100, 2)
        for n in schema_names
        if null_counts[n] > 0
    }

    return {
        "total_rows": total_rows,
        "row_groups": pf.metadata.num_row_groups,
        "schema": schema_types,
        "event_types": dict(sorted(event_types.items())),
        "has_snapshot": event_types.get("snapshot", 0) > 0,
        "snapshot_row_count": event_types.get("snapshot", 0),
        "snapshot_rows_sample": snapshot_rows[:3],
        "null_percentages": null_pct,
        "id_ranges": id_ranges,
        "first_rows": first_rows,
        "last_rows": last_rows,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--venue", default="bybit")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument(
        "--hours-back",
        type=int,
        default=72,
        help="How many hours back to scan (default 72 = retention window).",
    )
    args = ap.parse_args()

    CACHE_DIR.mkdir(exist_ok=True)
    print(
        f"=== Probing LAST {args.hours_back} hours for {args.venue}/{args.symbol} ===\n"
    )

    client = httpx.Client()
    date_str, hour_str, remote_path = find_latest_hour(
        client, args.venue, args.symbol, args.hours_back
    )

    if remote_path is None:
        print("\nNO Bybit data found in the last " f"{args.hours_back} hours.")
        print("Possible reasons:")
        print("  1. CryptoHFTData does not currently host Bybit data.")
        print("  2. The venue path name is different.")
        print("  3. The retention window is shorter than expected.")
        print("\nTry: --venue bybit_futures  or check cryptohftdata.com directly.")
        return

    # Download the newest available hour
    local = (
        CACHE_DIR
        / f"{args.venue}_{date_str}_{hour_str}_{args.symbol}_orderbook.parquet"
    )
    print(f"\nDownloading: {remote_path}")
    ok = download_file(client, remote_path, local)
    if not ok:
        print("  DOWNLOAD FAILED.")
        return
    print(f"  Saved to: {local} ({local.stat().st_size:,} bytes)")

    # Inspect
    print("\nInspecting...\n")
    info = inspect_parquet(local)

    print(f"  Total rows:      {info['total_rows']:,}")
    print(f"  Row groups:      {info['row_groups']}")
    print(f"  Event types:     {info['event_types']}")
    print(f"  HAS SNAPSHOT:    {info['has_snapshot']}")
    print(f"  Snapshot rows:   {info['snapshot_row_count']}")

    print("\n  Schema (name -> type):")
    for name, typ in info["schema"].items():
        print(f"    {name:<28} {typ}")

    print("\n  Field nullability (only fields with nulls):")
    if info["null_percentages"]:
        for name, pct in info["null_percentages"].items():
            print(f"    {name:<28} {pct}% null")
    else:
        print("    (no nulls anywhere)")

    print("\n  Sequence ID ranges:")
    for f, r in info["id_ranges"].items():
        print(f"    {f:<24} count={r['count']:,}  " f"min={r['min']}  max={r['max']}")

    if info["has_snapshot"]:
        print("\n  SNAPSHOT ROW SAMPLE:")
        print(json.dumps(info["snapshot_rows_sample"], indent=2, default=str))

    print("\n  FIRST ROW SAMPLE:")
    print(json.dumps(info["first_rows"], indent=2, default=str))

    # Save full report
    report_path = CACHE_DIR / "bybit_latest_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, default=str)
    print(f"\nFull report: {report_path}")


if __name__ == "__main__":
    main()

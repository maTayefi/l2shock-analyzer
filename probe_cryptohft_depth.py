#!/usr/bin/env python3
"""
Probe CryptoHFTData historical depth and search for Bybit/Bitget snapshots.

Phase 1: Find the oldest available date for each venue.
Phase 2: Search every hour of the oldest N days for snapshot events.

Usage:
    py -3.14 probe_cryptohft_depth.py
    py -3.14 probe_cryptohft_depth.py --search-days 14 --venues bybit bitget_futures
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pyarrow.parquet as pq

BASE_URL = "https://api.cryptohftdata.com/v1/download"
CACHE_DIR = Path("cache_depth_probe")
REPORT_FILE = "cryptohft_depth_and_snapshot_report.json"

VENUE_CONFIGS = {
    "bybit": {
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "suffix": "orderbook",
    },
    "bitget_futures": {
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "suffix": "orderbook",
    },
    "okx_futures": {
        "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
        "suffix": "orderbook",
    },
    "binance_futures": {
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "suffix": "orderbook",
    },
}


def check_exists(
    client: httpx.Client,
    venue: str,
    date_str: str,
    hour: int,
    symbol: str,
    suffix: str,
) -> int:
    path = f"{venue}/{date_str}/{hour:02d}/{symbol}_{suffix}.parquet"
    url = f"{BASE_URL}?file={path}"
    try:
        # The API does not support HEAD requests (returns 404).
        # Use a GET request with a Range header to check existence efficiently.
        resp = client.get(
            url,
            headers={"Range": "bytes=0-1023"},
            timeout=10.0,
            follow_redirects=True
        )
        # 200 (OK) or 206 (Partial Content) means the file exists
        if resp.status_code in (200, 206):
            return 200
        return resp.status_code
    except Exception:
        return -1


def download_file(
    client: httpx.Client,
    venue: str,
    date_str: str,
    hour: int,
    symbol: str,
    suffix: str,
) -> Path | None:
    path = f"{venue}/{date_str}/{hour:02d}/{symbol}_{suffix}.parquet"
    url = f"{BASE_URL}?file={path}"
    local = CACHE_DIR / f"{venue}_{date_str}_{hour:02d}_{symbol}_{suffix}.parquet"
    if local.exists() and local.stat().st_size > 0:
        return local
    try:
        with client.stream("GET", url, timeout=60.0, follow_redirects=True) as resp:
            if resp.status_code != 200:
                return None
            local.parent.mkdir(parents=True, exist_ok=True)
            with open(local, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1 << 16):
                    f.write(chunk)
        return local
    except Exception:
        return None


def scan_for_snapshots(parquet_path: Path) -> dict:
    """Scan one parquet file for snapshot events."""
    try:
        pf = pq.ParquetFile(parquet_path)
        total_rows = pf.metadata.num_rows
        snapshot_count = 0
        snapshot_events = []
        update_count = 0
        other_events = {}

        for batch in pf.iter_batches(
            batch_size=65536,
            columns=["event_type"],
            use_threads=True,
        ):
            for row in batch.to_pylist():
                et = row.get("event_type", "")
                if et == "snapshot":
                    snapshot_count += 1
                    if len(snapshot_events) < 5:
                        snapshot_events.append({"row_event_type": et})
                elif et == "update":
                    update_count += 1
                else:
                    other_events[et] = other_events.get(et, 0) + 1

        return {
            "total_rows": total_rows,
            "snapshot_count": snapshot_count,
            "update_count": update_count,
            "other_events": other_events,
            "has_snapshot": snapshot_count > 0,
        }
    except Exception as e:
        return {"error": str(e), "has_snapshot": False}


def main():
    parser = argparse.ArgumentParser(
        description="Probe CryptoHFTData historical depth and snapshot availability."
    )
    parser.add_argument(
        "--search-days",
        type=int,
        default=14,
        help="How many days back to search for snapshots (default: 14).",
    )
    parser.add_argument(
        "--venues",
        nargs="*",
        default=["bybit", "bitget_futures"],
        choices=list(VENUE_CONFIGS.keys()),
        help="Which venues to probe (default: bybit bitget_futures).",
    )
    parser.add_argument(
        "--scan-snapshots",
        action="store_true",
        default=True,
        help="Download and scan files for snapshot events.",
    )
    parser.add_argument(
        "--hours-per-day",
        type=int,
        default=24,
        help="Hours per day to check (default: 24 = every hour).",
    )
    args = parser.parse_args()

    CACHE_DIR.mkdir(exist_ok=True)
    now = datetime.now(timezone.utc)
    # Start from 2 days ago to avoid processing delay
    probe_start = (now - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    results = {
        "generated_at": now.isoformat(),
        "search_days": args.search_days,
        "venues": {},
    }

    client = httpx.Client()

    for venue_name in args.venues:
        config = VENUE_CONFIGS[venue_name]
        venue_result = {
            "oldest_available_date": None,
            "newest_available_date": None,
            "total_available_hours": 0,
            "snapshot_scan": {},
        }

        print(f"\n{'='*60}")
        print(f"Venue: {venue_name}")
        print(f"{'='*60}")

        # Phase 1: Find oldest and newest available dates
        print("\n--- Phase 1: Finding available date range ---")
        oldest_found = None
        newest_found = None
        total_hours = 0

        # Probe forward from search_days ago to 2 days ago
        for day_offset in range(args.search_days, 1, -1):
            probe_date = probe_start - timedelta(days=day_offset - 2)
            date_str = probe_date.strftime("%Y-%m-%d")
            symbol = config["symbols"][0]

            # Quick check: just hour 12
            status = check_exists(
                client, venue_name, date_str, 12, symbol, config["suffix"]
            )
            if status == 200:
                if oldest_found is None:
                    oldest_found = date_str
                newest_found = date_str
                print(f"  [FOUND] {date_str}")
            else:
                if oldest_found is not None:
                    # We found data, then hit a gap - stop searching further back
                    print(f"  [GAP]   {date_str} (status={status})")
                    break
                else:
                    print(f"  [MISS]  {date_str} (status={status})")
            time.sleep(0.5)

        # Count total available hours in the found range
        if oldest_found and newest_found:
            oldest_dt = datetime.strptime(oldest_found, "%Y-%m-%d")
            newest_dt = datetime.strptime(newest_found, "%Y-%m-%d")
            current = oldest_dt
            while current <= newest_dt:
                date_str = current.strftime("%Y-%m-%d")
                for hour in range(0, 24, args.hours_per_day):
                    symbol = config["symbols"][0]
                    status = check_exists(
                        client, venue_name, date_str, hour, symbol, config["suffix"]
                    )
                    if status == 200:
                        total_hours += 1
                    time.sleep(0.3)
                current += timedelta(days=1)

        venue_result["oldest_available_date"] = oldest_found
        venue_result["newest_available_date"] = newest_found
        venue_result["total_available_hours"] = total_hours

        print(f"\n  Date range: {oldest_found} to {newest_found}")
        print(f"  Total available hours: {total_hours}")

        # Phase 2: Scan for snapshots
        if args.scan_snapshots and oldest_found and newest_found:
            print("\n--- Phase 2: Scanning for snapshot events ---")
            snapshot_scan = {}
            snapshot_found_any = False

            oldest_dt = datetime.strptime(oldest_found, "%Y-%m-%d")
            newest_dt = datetime.strptime(newest_found, "%Y-%m-%d")
            current = oldest_dt

            while current <= newest_dt:
                date_str = current.strftime("%Y-%m-%d")
                for hour in range(0, 24):
                    for symbol in config["symbols"]:
                        key = f"{date_str}/{hour:02d}/{symbol}"
                        local = download_file(
                            client, venue_name, date_str, hour, symbol, config["suffix"]
                        )
                        if local is None:
                            snapshot_scan[key] = {
                                "status": "not_available",
                                "has_snapshot": False,
                            }
                            continue

                        scan = scan_for_snapshots(local)
                        snapshot_scan[key] = scan

                        if scan.get("has_snapshot"):
                            snapshot_found_any = True
                            print(
                                f"  *** SNAPSHOT FOUND: {key} "
                                f"({scan['snapshot_count']} snapshot rows)"
                            )
                        elif scan.get("snapshot_count", 0) == 0:
                            pass  # No snapshot, don't spam output

                        time.sleep(0.5)

                current += timedelta(days=1)

            venue_result["snapshot_scan"] = snapshot_scan
            venue_result["snapshot_found_any"] = snapshot_found_any

            if not snapshot_found_any:
                print(
                    f"\n  NO SNAPSHOTS FOUND in {args.search_days} days "
                    f"for {venue_name}"
                )
            else:
                print(f"\n  Snapshots were found for {venue_name}")

        results["venues"][venue_name] = venue_result

    # Write report
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nReport written to {REPORT_FILE}")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for venue_name, vdata in results["venues"].items():
        print(f"\n{venue_name}:")
        print(
            f"  Date range: {vdata['oldest_available_date']} "
            f"to {vdata['newest_available_date']}"
        )
        print(f"  Total hours: {vdata['total_available_hours']}")
        if vdata.get("snapshot_found_any") is not None:
            if vdata["snapshot_found_any"]:
                print(f"  SNAPSHOTS: FOUND")
            else:
                print(f"  SNAPSHOTS: NOT FOUND")

        # Gap tolerance conclusion
        if venue_name == "bybit":
            if vdata.get("snapshot_found_any"):
                print(f"  Gap tolerance: Depends on snapshot frequency")
            else:
                print(f"  Gap tolerance: N=0 (cannot miss any hour)")
                print(f"  WARNING: Bybit requires every hour + external bootstrap")
        elif venue_name == "bitget_futures":
            print(f"  Gap tolerance: N=0 (no proven continuity at all)")
            print(f"  WARNING: Bitget is not viable regardless of snapshots")


if __name__ == "__main__":
    main()

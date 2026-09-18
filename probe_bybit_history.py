#!/usr/bin/env python3
"""
Probe CryptoHFTData for Bybit data availability going back in history.
Checks HEAD requests across dates and hours to find the oldest available data.

Usage:
    py -3.14 probe_bybit_history.py --days-back 60 --venue bybit
    py -3.14 probe_bybit_history.py --days-back 90 --venue bybit --symbol BTCUSDT
    py -3.14 probe_bybit_history.py --days-back 30 --venue bybit --hours-per-day 4
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import httpx
except ImportError:
    print("ERROR: httpx is required. Run: pip install httpx")
    sys.exit(1)

BASE_URL = "https://api.cryptohftdata.com/v1/download"

# Known venue path variants to try
VENUE_PATH_VARIANTS = {
    "bybit": ["bybit", "bybit_futures", "bybitfutures", "bybit-futures"],
    "bitget_futures": ["bitget_futures", "bitget", "bitgetfutures"],
}

SYMBOLS = {
    "bybit": ["BTCUSDT", "ETHUSDT"],
    "bitget_futures": ["BTCUSDT", "ETHUSDT"],
}


def head_check(client: httpx.Client, path: str) -> int:
    """Return HTTP status code for a HEAD request."""
    url = f"{BASE_URL}?file={path}"
    try:
        resp = client.head(url, timeout=15.0, follow_redirects=True)
        return resp.status_code
    except Exception:
        return -1


def probe_date(
    client: httpx.Client,
    venue_paths: list[str],
    symbol: str,
    date_str: str,
    hours_to_check: list[int],
) -> dict:
    """Check all venue path variants for a given date."""
    results = {}
    for venue_path in venue_paths:
        found_hours = []
        for hour in hours_to_check:
            path = f"{venue_path}/{date_str}/{hour:02d}/{symbol}_orderbook.parquet"
            status = head_check(client, path)
            if status == 200:
                found_hours.append(hour)
            time.sleep(0.15)  # Rate limit
        if found_hours:
            results[venue_path] = found_hours
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Probe CryptoHFTData for Bybit/Bitget data availability."
    )
    parser.add_argument(
        "--venue",
        default="bybit",
        choices=list(VENUE_PATH_VARIANTS.keys()),
        help="Venue to probe (default: bybit)",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=60,
        help="How many days back to search (default: 60)",
    )
    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Symbol to use as canary (default: BTCUSDT)",
    )
    parser.add_argument(
        "--hours-per-day",
        type=int,
        default=6,
        help="Hours to check per day (default: 6, spread across day)",
    )
    parser.add_argument(
        "--output",
        default="bybit_availability_report.json",
        help="Output JSON report path",
    )
    args = parser.parse_args()

    venue = args.venue
    venue_paths = VENUE_PATH_VARIANTS[venue]
    symbol = args.symbol
    days_back = args.days_back
    hours_per_day = min(args.hours_per_day, 24)

    # Spread hours evenly across the day
    hours_to_check = sorted(
        set(int(i * 24 / hours_per_day) for i in range(hours_per_day))
    )

    now = datetime.now(timezone.utc)
    # Start from 3 days ago to avoid publication delay
    start_date = (now - timedelta(days=3)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    print(f"=== Probing {venue} availability ===")
    print(f"Venue path variants: {venue_paths}")
    print(f"Symbol: {symbol}")
    print(f"Days back: {days_back}")
    print(f"Hours checked per day: {hours_to_check}")
    print()

    client = httpx.Client()
    report = {
        "venue": venue,
        "symbol": symbol,
        "days_searched": days_back,
        "hours_per_day_checked": hours_per_day,
        "generated_at": now.isoformat(),
        "results": [],
    }

    found_data = False
    oldest_found = None
    newest_found = None
    working_venue_path = None

    for day_offset in range(days_back):
        current_date = start_date - timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")

        day_result = probe_date(client, venue_paths, symbol, date_str, hours_to_check)

        if day_result:
            found_data = True
            if working_venue_path is None:
                working_venue_path = list(day_result.keys())[0]
                print(f"\n*** FOUND DATA! Venue path: {working_venue_path}")

            hours_found = day_result.get(working_venue_path, [])
            if hours_found:
                if oldest_found is None:
                    oldest_found = date_str
                newest_found = date_str

            print(f"  [FOUND] {date_str}: hours {hours_found}")
            report["results"].append(
                {
                    "date": date_str,
                    "venue_path": working_venue_path,
                    "hours_found": hours_found,
                }
            )
        else:
            if found_data:
                # Found data before, now hitting gap — keep going a bit more
                print(f"  [GAP]   {date_str}: no data (continuing search)")
                report["results"].append(
                    {
                        "date": date_str,
                        "venue_path": None,
                        "hours_found": [],
                    }
                )
            else:
                print(f"  [MISS]  {date_str}: no data")

        time.sleep(0.3)

    client.close()

    # Summary
    report["found_data"] = found_data
    report["working_venue_path"] = working_venue_path
    report["oldest_found_date"] = oldest_found
    report["newest_found_date"] = newest_found

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if found_data:
        print(f"  Venue path: {working_venue_path}")
        print(f"  Oldest data: {oldest_found}")
        print(f"  Newest data: {newest_found}")
        print(f"  Symbol: {symbol}")
        print()
        print("NEXT STEP: Run inspect_bybit_contract.py to check snapshots")
    else:
        print(f"  NO DATA FOUND for {venue} in {days_back} days")
        print()
        print("  Possible reasons:")
        print("  1. Bybit data doesn't exist on CryptoHFTData")
        print("  2. Venue path name is different from variants tried")
        print("  3. Data is too old (beyond search window)")
        print()
        print("  Try increasing --days-back or checking cryptohftdata.com directly")

    # Write report
    output_path = Path(args.output)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport written to: {output_path}")


if __name__ == "__main__":
    main()

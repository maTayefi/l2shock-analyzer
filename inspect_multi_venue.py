#!/usr/bin/env python3
"""
Multi-venue CryptoHFTData inspection with automatic date discovery.

Probes backwards from today to find dates with actual data, then downloads
and inspects orderbook + trades files for Bybit, OKX, and Bitget.
"""

import httpx
import pyarrow.parquet as pq
import json
import os
import sys
import time
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter

BASE_URL = "https://api.cryptohftdata.com/v1/download"
OUTPUT_REPORT = "multi_venue_inspection_report.json"
CACHE_DIR = Path("cache_multi_venue")
CACHE_DIR.mkdir(exist_ok=True)

# How far back to search for valid data (in days)
MAX_LOOKBACK_DAYS = 14
# Hours to try within a valid day
HOURS_TO_TRY = [12, 13, 6, 0]

VENUES = [
    {
        "name": "bybit",
        "aliases": ["bybit", "bybit_futures"],
        "symbol_btc": "BTCUSDT",
        "symbol_eth": "ETHUSDT",
    },
    {
        "name": "okx",
        "aliases": ["okx", "okx_futures"],
        "symbol_btc": "BTC-USDT-SWAP",
        "symbol_eth": "ETH-USDT-SWAP",
    },
    {
        "name": "bitget",
        "aliases": ["bitget", "bitget_futures"],
        "symbol_btc": "BTCUSDT",
        "symbol_eth": "ETHUSDT",
    },
    {
        "name": "binance_futures",
        "aliases": ["binance_futures"],
        "symbol_btc": "BTCUSDT",
        "symbol_eth": "ETHUSDT",
    },
]


def head_check(client: httpx.Client, path: str) -> int:
    """Return HTTP status code for a HEAD request (fast, no body download)."""
    url = f"{BASE_URL}?file={path}"
    try:
        resp = client.head(url, timeout=15.0, follow_redirects=True)
        return resp.status_code
    except Exception:
        return -1


def discover_valid_dates(client: httpx.Client) -> dict:
    """Probe backwards from today to find dates+hours+venues with data."""
    now = datetime.now(timezone.utc)
    results = {}  # venue_alias -> list of (date_str, hour) with data

    for venue_cfg in VENUES:
        for alias in venue_cfg["aliases"]:
            key = alias
            if key in results:
                continue
            results[key] = []

            for day_offset in range(MAX_LOOKBACK_DAYS):
                probe_date = now - timedelta(days=day_offset)
                date_str = probe_date.strftime("%Y-%m-%d")

                for hour in HOURS_TO_TRY:
                    # Use BTC orderbook as the canary file
                    path = f"{alias}/{date_str}/{hour:02d}/{venue_cfg['symbol_btc']}_orderbook.parquet"
                    status = head_check(client, path)
                    if status == 200:
                        results[key].append((date_str, hour))
                        print(f"  [FOUND] {alias} {date_str} hour={hour:02d} -> 200")
                        break  # Found one valid hour for this day, move to next day
                    time.sleep(0.2)
                else:
                    continue
                break  # Found at least one valid slot for this venue alias
            else:
                print(
                    f"  [NONE] {alias}: no data found in last {MAX_LOOKBACK_DAYS} days"
                )

    return results


def inspect_parquet(path: Path) -> dict:
    """Extract structural info from a Parquet file without loading all rows."""
    info = {
        "path": str(path),
        "file_size_bytes": path.stat().st_size,
        "num_rows": 0,
        "num_row_groups": 0,
        "schema": [],
        "metadata": {},
        "event_type_counts": {},
        "first_rows": [],
        "last_rows": [],
        "error": None,
    }
    try:
        pf = pq.ParquetFile(path)
        info["num_rows"] = pf.metadata.num_rows
        info["num_row_groups"] = pf.metadata.num_row_groups

        for i in range(len(pf.schema_arrow)):
            field = pf.schema_arrow.field(i)
            info["schema"].append({"name": field.name, "type": str(field.type)})

        if pf.metadata.metadata:
            for k, v in pf.metadata.metadata.items():
                key = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                info["metadata"][key] = val[:500]

        # Read first row group for samples
        if pf.metadata.num_row_groups > 0:
            rg = pf.read_row_group(0)
            cols = rg.column_names
            rows = rg.to_pylist()

            if "event_type" in cols:
                ev_types = [r.get("event_type") for r in rows]
                info["event_type_counts"] = dict(Counter(ev_types))

            info["first_rows"] = rows[:3]
            if len(rows) > 6:
                info["last_rows"] = rows[-3:]

    except Exception as e:
        info["error"] = str(e)

    return info


def download_file(client: httpx.Client, remote_path: str, dest: Path) -> bool:
    if dest.exists() and dest.stat().st_size > 0:
        return True
    url = f"{BASE_URL}?file={remote_path}"
    try:
        with client.stream("GET", url, timeout=120.0, follow_redirects=True) as resp:
            if resp.status_code != 200:
                print(f"  [{resp.status_code}] {remote_path}")
                return False
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1 << 16):
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"  [ERR] {remote_path}: {e}")
        return False


def main():
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "discovery": {},
        "venues": {},
    }

    client = httpx.Client()

    print("=== Phase 1: Discovering valid dates ===")
    discovery = discover_valid_dates(client)
    report["discovery"] = {k: v for k, v in discovery.items()}

    # Pick the best date/hour for each venue (prefer the same date across venues)
    # Find the most common valid date
    all_valid_dates = Counter()
    for alias, slots in discovery.items():
        for date_str, hour in slots:
            all_valid_dates[date_str] += 1

    if not all_valid_dates:
        print("\nERROR: No valid data found for any venue in the lookback window.")
        print("This likely means CryptoHFTData has no data for these venues yet,")
        print("or the data hasn't been published for recent dates.")
        print(
            "Try increasing MAX_LOOKBACK_DAYS or checking cryptohftdata.com directly."
        )
        sys.exit(1)

    best_date = all_valid_dates.most_common(1)[0][0]
    print(
        f"\nUsing date: {best_date} (found data for {all_valid_dates[best_date]} venue aliases)"
    )

    # Determine hours for this date
    hour_n = None
    hour_n_minus_1 = None
    for alias, slots in discovery.items():
        for d, h in slots:
            if d == best_date:
                if hour_n is None or h > hour_n:
                    hour_n_minus_1 = hour_n
                    hour_n = h
                elif hour_n_minus_1 is None or h < hour_n:
                    if hour_n_minus_1 is None or h < hour_n_minus_1:
                        hour_n_minus_1 = h

    if hour_n is None:
        hour_n = 12
    if hour_n_minus_1 is None:
        hour_n_minus_1 = max(0, hour_n - 1)

    print(f"Using hours: N-1={hour_n_minus_1:02d}, N={hour_n:02d}")
    report["target_date"] = best_date
    report["hour_n_minus_1"] = hour_n_minus_1
    report["hour_n"] = hour_n

    # === Phase 2: Download and inspect ===
    print("\n=== Phase 2: Downloading and inspecting ===")

    for venue_cfg in VENUES:
        venue_name = venue_cfg["name"]
        # Pick the alias that had data
        valid_alias = None
        for alias in venue_cfg["aliases"]:
            if alias in discovery and discovery[alias]:
                valid_alias = alias
                break

        if valid_alias is None:
            report["venues"][venue_name] = {
                "status": "no_data_found",
                "aliases_tried": venue_cfg["aliases"],
            }
            print(
                f"\n[{venue_name}] No data found for any alias: {venue_cfg['aliases']}"
            )
            continue

        print(f"\n[{venue_name}] Using alias: {valid_alias}")
        venue_report = {
            "resolved_alias": valid_alias,
            "symbols": {
                "BTC": {"orderbook": {}, "trades": {}},
                "ETH": {"orderbook": {}, "trades": {}},
            },
        }

        for base, symbol_key in [("BTC", "symbol_btc"), ("ETH", "symbol_eth")]:
            symbol = venue_cfg[symbol_key]

            # Orderbook: hour N-1 and N
            for hour, label in [(hour_n_minus_1, "ob_n_minus_1"), (hour_n, "ob_n")]:
                remote = (
                    f"{valid_alias}/{best_date}/{hour:02d}/{symbol}_orderbook.parquet"
                )
                local = CACHE_DIR / f"{valid_alias}_{base}_ob_h{hour:02d}.parquet"
                print(f"  [{base}] OB h={hour:02d}: {remote}")
                ok = download_file(client, remote, local)
                time.sleep(0.5)
                if ok:
                    info = inspect_parquet(local)
                    venue_report["symbols"][base]["orderbook"][label] = info
                    print(
                        f"    -> {info['num_rows']} rows, {info['num_row_groups']} row groups"
                    )
                else:
                    venue_report["symbols"][base]["orderbook"][label] = {
                        "error": "download failed"
                    }

            # Trades: hour N only
            remote = f"{valid_alias}/{best_date}/{hour_n:02d}/{symbol}_trades.parquet"
            local = CACHE_DIR / f"{valid_alias}_{base}_trades_h{hour_n:02d}.parquet"
            print(f"  [{base}] Trades h={hour_n:02d}: {remote}")
            ok = download_file(client, remote, local)
            time.sleep(0.5)
            if ok:
                info = inspect_parquet(local)
                venue_report["symbols"][base]["trades"]["trades_n"] = info
                print(
                    f"    -> {info['num_rows']} rows, {info['num_row_groups']} row groups"
                )
            else:
                venue_report["symbols"][base]["trades"]["trades_n"] = {
                    "error": "download failed"
                }

        report["venues"][venue_name] = venue_report

    # Write report
    with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    size = os.path.getsize(OUTPUT_REPORT)
    print(f"\nReport written to {OUTPUT_REPORT} ({size:,} bytes)")


if __name__ == "__main__":
    main()

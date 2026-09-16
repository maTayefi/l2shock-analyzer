#!/usr/bin/env python3
"""
Verify that 404s are genuine "venue not found" and not URL typos.

Strategy:
1. Probe a KNOWN-GOOD path (binance_futures BTCUSDT) to confirm the API works.
2. Probe the 404'd venue with MULTIPLE different hours/dates to confirm
   it is consistently 404 (not a timing/availability issue).
3. Probe a KNOWN-BAD symbol on the WORKING venue to confirm 404 behavior
   for missing symbols (distinguishes venue-missing from symbol-missing).
4. Probe a KNOWN-BAD date far in the future on the working venue.
"""

import httpx
import time

BASE = "https://api.cryptohftdata.com/v1/download"

PROBES = [
    # --- Control: known-good path (should succeed) ---
    (
        "CONTROL: known-good binance_futures BTCUSDT",
        "binance_futures/2026-09-09/12/BTCUSDT_orderbook.parquet",
        "expect_200",
    ),
    # --- Venue existence probes: try the 404'd venues at multiple hours ---
    (
        "PROBE: okx venue, hour 12",
        "okx/2026-09-09/12/BTC-USDT-SWAP_orderbook.parquet",
        "expect_404",
    ),
    (
        "PROBE: okx venue, hour 06",
        "okx/2026-09-09/06/BTC-USDT-SWAP_orderbook.parquet",
        "expect_404",
    ),
    (
        "PROBE: okx venue, different date",
        "okx/2026-09-08/12/BTC-USDT-SWAP_orderbook.parquet",
        "expect_404",
    ),
    (
        "PROBE: bitget venue, hour 12",
        "bitget/2026-09-09/12/BTCUSDT_orderbook.parquet",
        "expect_404",
    ),
    (
        "PROBE: bitget venue, hour 06",
        "bitget/2026-09-09/06/BTCUSDT_orderbook.parquet",
        "expect_404",
    ),
    (
        "PROBE: bitget venue, different date",
        "bitget/2026-09-08/12/BTCUSDT_orderbook.parquet",
        "expect_404",
    ),
    # --- Confirm the CORRECT venue works at same hours ---
    (
        "CONTROL: okx_futures hour 12",
        "okx_futures/2026-09-09/12/BTC-USDT-SWAP_orderbook.parquet",
        "expect_200",
    ),
    (
        "CONTROL: okx_futures hour 06",
        "okx_futures/2026-09-09/06/BTC-USDT-SWAP_orderbook.parquet",
        "expect_200",
    ),
    (
        "CONTROL: bitget_futures hour 12",
        "bitget_futures/2026-09-09/12/BTCUSDT_orderbook.parquet",
        "expect_200",
    ),
    (
        "CONTROL: bitget_futures hour 06",
        "bitget_futures/2026-09-09/06/BTCUSDT_orderbook.parquet",
        "expect_200",
    ),
    # --- Symbol-missing probe on a KNOWN-GOOD venue ---
    (
        "PROBE: okx_futures with FAKE symbol",
        "okx_futures/2026-09-09/12/FAKECOIN_orderbook.parquet",
        "expect_404",
    ),
    (
        "PROBE: bitget_futures with FAKE symbol",
        "bitget_futures/2026-09-09/12/FAKECOIN_orderbook.parquet",
        "expect_404",
    ),
    # --- Future date probe on known-good venue ---
    (
        "PROBE: okx_futures far-future date",
        "okx_futures/2027-01-01/12/BTC-USDT-SWAP_orderbook.parquet",
        "expect_404",
    ),
]


def probe(client: httpx.Client, label: str, path: str, expected: str) -> dict:
    url = f"{BASE}?file={path}"
    try:
        resp = client.head(url, timeout=10.0, follow_redirects=True)
        status = resp.status_code
    except Exception as exc:
        status = f"ERROR: {type(exc).__name__}"

    if expected == "expect_200":
        ok = status == 200
    else:
        ok = status == 404

    return {
        "label": label,
        "path": path,
        "expected": expected,
        "status": status,
        "verdict": "PASS" if ok else "UNEXPECTED",
    }


def main():
    print(f"{'Label':<55} {'Status':<8} {'Expected':<14} {'Verdict'}")
    print("-" * 100)

    results = []
    with httpx.Client() as client:
        for label, path, expected in PROBES:
            r = probe(client, label, path, expected)
            results.append(r)
            print(
                f"{r['label']:<55} {r['status']:<8} {r['expected']:<14} {r['verdict']}"
            )
            time.sleep(1.2)  # stay under 60 req/min

    print()
    passed = sum(1 for r in results if r["verdict"] == "PASS")
    total = len(results)
    print(f"Result: {passed}/{total} probes passed.")

    if passed == total:
        print()
        print("CONCLUSION: All 404s are genuine 'venue not found'.")
        print("  - 'okx' and 'bitget' are NOT valid venue identifiers.")
        print("  - 'okx_futures' and 'bitget_futures' ARE the correct identifiers.")
        print("  - URL format (date/hour/symbol/suffix) is confirmed correct.")
        print("  - 404 on fake symbols confirms symbol-level 404 works too.")
    else:
        print()
        print("WARNING: Some probes returned unexpected results. Review above.")


if __name__ == "__main__":
    main()

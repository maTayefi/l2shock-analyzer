import httpx
import sys

BASE_URL = "https://api.cryptohftdata.com/v1"
TIMEOUT = 30.0


def print_header(title):
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")


def test_symbols_discovery(client):
    print_header("TEST 1: Symbols Discovery (API Metadata)")
    url = f"{BASE_URL}/symbols"
    params = {"exchange": "bybit", "data_type": "orderbook"}

    try:
        resp = client.get(url, params=params, timeout=TIMEOUT, follow_redirects=True)
        print(f"URL: {resp.url}")
        print(f"STATUS: {resp.status_code}")
        if resp.status_code == 200:
            if "BTCUSDT" in resp.text:
                print(
                    "✅ SUCCESS: 'BTCUSDT' is confirmed available for Bybit orderbooks."
                )
            else:
                print(
                    "⚠️ WARNING: 'BTCUSDT' NOT found in response. Check if it's named 'BTCUSD' or similar."
                )
                print(f"Response snippet: {resp.text[:300]}...")
        else:
            print(f"❌ FAILED: Non-200 status code. Body: {resp.text[:500]}")
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {e}")


def test_file_access(client, label, file_path):
    print_header(f"TEST: {label}")
    print(f"Target File: {file_path}")
    url = f"{BASE_URL}/download"
    params = {"file": file_path}

    # 1. HEAD request (What your current probe is doing)
    print("\n--- 1. HEAD Request (Current Probe Method) ---")
    try:
        h = client.head(url, params=params, timeout=TIMEOUT, follow_redirects=True)
        print(f"STATUS: {h.status_code}")
        print(f"Content-Type: {h.headers.get('content-type', 'N/A')}")
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {e}")

    # 2. GET with Range request (Safe existence check, downloads max 1KB)
    print("\n--- 2. GET with Range (bytes=0-1023) ---")
    try:
        headers = {"Range": "bytes=0-1023"}
        with client.stream(
            "GET",
            url,
            params=params,
            headers=headers,
            timeout=TIMEOUT,
            follow_redirects=True,
        ) as r:
            print(f"STATUS: {r.status_code}")
            print(f"Final URL (after redirects): {r.url}")
            print(f"Content-Range: {r.headers.get('content-range', 'N/A')}")

            chunk = b""
            for data in r.iter_bytes(1024):
                chunk = data
                break  # Stop immediately to prevent full file download

            if r.status_code in (200, 206):
                print(f"First 16 bytes (hex): {chunk[:16].hex()}")
                if chunk[:4] == b"PAR1":
                    print("✅ SUCCESS: Valid Parquet magic bytes ('PAR1') detected!")
                else:
                    print(
                        "⚠️ WARNING: Missing Parquet magic bytes. Might be an error page or XML."
                    )
            else:
                print(
                    f"❌ FAILED: Body snippet: {chunk.decode('utf-8', errors='ignore')[:200]}"
                )
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {e}")


def main():
    print("CryptoHFTData Bybit Diagnostic Script")
    print("Testing HEAD vs GET, Range requests, and Symbol discovery.\n")

    with httpx.Client() as client:
        # Test 1: Symbols API
        test_symbols_discovery(client)

        # Test 2: Known Historical File (from their docs/website sample)
        test_file_access(
            client,
            "Historical Sample (2026-09-02)",
            "bybit/2026-09-02/12/BTCUSDT_orderbook.parquet",
        )

        # Test 3: Recent File (from your 404 probe)
        test_file_access(
            client,
            "Recent Hour (2026-09-17)",
            "bybit/2026-09-17/12/BTCUSDT_orderbook.parquet",
        )

    print_header("DIAGNOSTIC COMPLETE")
    print("Review the outputs above to determine the next step.")


if __name__ == "__main__":
    main()

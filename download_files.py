import httpx
from pathlib import Path

BASE = "https://api.cryptohftdata.com/v1/download"
FILES = [
    "binance_futures/2026-09-17/12/BTCUSDT_orderbook.parquet",
    "binance_futures/2026-09-17/12/ETHUSDT_orderbook.parquet",
]

out = Path("downloads")
out.mkdir(exist_ok=True)

with httpx.Client(timeout=120.0, follow_redirects=True) as c:
    for f in FILES:
        dest = out / Path(f).name
        print(f"Downloading {f} ...")
        with c.stream("GET", BASE, params={"file": f}) as r:
            if r.status_code != 200:
                print(f"  FAILED (HTTP {r.status_code})")
                continue
            with open(dest, "wb") as fh:
                for chunk in r.iter_bytes(1024 * 1024):
                    fh.write(chunk)
        print(f"  Saved: {dest} ({dest.stat().st_size:,} bytes)")
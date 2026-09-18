"""One-time Binance bootstrap: fetch live book, create a synthetic checkpoint."""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from httpx import Client

from l2shock.ingest.checkpoint_codec import encode_checkpoint, checkpoint_encoding_info
from l2shock.ingest.replay import CheckpointLevel, OrderBookCheckpoint
from l2shock.ingest import BookSide


def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap a synthetic Binance checkpoint from the live API."
    )
    parser.add_argument(
        "--symbol",
        required=True,
        choices=["BTCUSDT", "ETHUSDT"],
        help="The trading pair symbol.",
    )
    parser.add_argument(
        "--hour-offset",
        type=int,
        default=-1,
        help="Hour offset from the current UTC hour (e.g., -1 for the previous completed hour).",
    )
    args = parser.parse_args()

    symbol = args.symbol
    hour_offset = args.hour_offset

    # Calculate the target THROUGH_HOUR dynamically based on current UTC time
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    through_hour = current_hour + timedelta(hours=hour_offset)

    output_dir = Path("data/cache/checkpoints/binance_futures") / symbol

    # --- Fetch live book from Binance ---
    client = Client(timeout=30.0)
    try:
        resp = client.get(
            "https://fapi.binance.com/fapi/v1/depth",
            params={"symbol": symbol, "limit": 1000},
        )
        resp.raise_for_status()
        book = resp.json()
    except Exception as e:
        print(f"ERROR: Failed to fetch book for {symbol}: {e}")
        sys.exit(1)

    bids = [(Decimal(p), Decimal(q)) for p, q in book["bids"] if Decimal(q) > 0]
    asks = [(Decimal(p), Decimal(q)) for p, q in book["asks"] if Decimal(q) > 0]

    if not bids or not asks:
        print("ERROR: Empty book side")
        sys.exit(1)

    print(f"Fetched {len(bids)} bids, {len(asks)} asks for {symbol}")
    print(f"Best bid: {bids[0][0]}, Best ask: {asks[0][0]}")

    # --- Build checkpoint level dicts ---
    levels = []
    for price, qty in bids:
        levels.append(
            CheckpointLevel(
                side=BookSide.BID, price=price, quantity=qty, order_count=None
            )
        )
    for price, qty in asks:
        levels.append(
            CheckpointLevel(
                side=BookSide.ASK, price=price, quantity=qty, order_count=None
            )
        )

    checkpoint = OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol=symbol,
        through_hour_utc=through_hour,
        last_update_id=int(book.get("lastUpdateId", 0)),
        levels=tuple(levels),
        source_content_sha256=None,  # synthetic — no source archive
    )

    encoded = encode_checkpoint(checkpoint)
    info = checkpoint_encoding_info(encoded)

    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = through_hour.strftime("%Y-%m-%d")
    hour_str = through_hour.strftime("%H")
    out_dir = output_dir / date_str / hour_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"checkpoint-v1-{info.content_sha256}.l2checkpoint"

    if out_path.exists():
        print(f"\nCheckpoint already exists: {out_path}")
    else:
        out_path.write_bytes(encoded)
        print(f"\nCheckpoint written: {out_path}")

    print(f"SHA-256: {info.content_sha256}")
    print(
        f"Levels: {info.level_count} (bid={info.bid_level_count}, ask={info.ask_level_count})"
    )
    print(f"\nThis checkpoint is for hour {through_hour.isoformat()}")
    print(
        f"It can initialize processing of hour {(through_hour + timedelta(hours=1)).isoformat()}"
    )


if __name__ == "__main__":
    main()

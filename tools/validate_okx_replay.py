#!/usr/bin/env python3
"""Validate real OKX CryptoHFTData order-book archives.

This tool is opt-in and intentionally separate from pytest because real sample
archives are not part of the repository.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    replay_chain_report_to_dict,
    replay_orderbook_archives,
)
from l2shock.ingest.checkpoint_codec import checkpoint_encoding_info, encode_checkpoint


def _utc_hour(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or parsed.minute
        or parsed.second
        or parsed.microsecond
    ):
        raise argparse.ArgumentTypeError(
            "hour must be an exact timezone-aware UTC hour"
        )

    if parsed.utcoffset().total_seconds() != 0:
        raise argparse.ArgumentTypeError("hour must have UTC offset +00:00")

    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate one or more adjacent OKX order-book archives."
    )
    parser.add_argument(
        "--symbol",
        required=True,
        choices=("BTC-USDT-SWAP", "ETH-USDT-SWAP"),
    )
    parser.add_argument(
        "--archive",
        action="append",
        nargs=2,
        required=True,
        metavar=("UTC_HOUR", "PATH"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=65_536,
    )

    args = parser.parse_args()

    sources = []

    for raw_hour, raw_path in args.archive:
        hour = _utc_hour(raw_hour)
        path = Path(raw_path).expanduser().resolve()

        if not path.is_file():
            raise FileNotFoundError(path)

        sources.append(
            (
                path,
                SourceFileSpec(
                    venue="okx_futures",
                    symbol=args.symbol,
                    data_kind=SourceDataKind.ORDERBOOK,
                    hour_utc=hour,
                ),
            )
        )

    report = replay_orderbook_archives(
        tuple(sources),
        batch_size=args.batch_size,
    )

    payload = replay_chain_report_to_dict(report)

    if report.final_checkpoint is not None:
        encoded = encode_checkpoint(report.final_checkpoint)
        info = checkpoint_encoding_info(encoded)

        payload["checkpoint_encoding"] = {
            "format_version": info.format_version,
            "level_count": info.level_count,
            "bid_level_count": info.bid_level_count,
            "ask_level_count": info.ask_level_count,
            "payload_size_bytes": info.payload_size_bytes,
            "encoded_size_bytes": info.encoded_size_bytes,
            "content_sha256": info.content_sha256,
        }
    else:
        payload["checkpoint_encoding"] = None

    destination = args.output.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "finally_valid": report.finally_valid,
                "archive_count": len(report.archives),
                "total_events_seen": report.total_events_seen,
                "total_invalidations": report.total_invalidations,
                "final_checkpoint": (payload["checkpoint_encoding"]),
                "output": str(destination),
            },
            indent=2,
            sort_keys=True,
        )
    )

    return 0 if report.finally_valid else 3


if __name__ == "__main__":
    raise SystemExit(main())

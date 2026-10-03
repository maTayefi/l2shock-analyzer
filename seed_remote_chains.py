#!/usr/bin/env python3
"""Seed remote processing chains with real CryptoHFTData snapshots at 25% depth.

Downloads real hourly archives (containing genuine exchange snapshots),
processes them through the headless L2 pipeline at depth 0→0.25, verifies
the output checkpoint is usable, and publishes the artifact to Hugging Face.

If --date and --hour are omitted, the script automatically searches backwards
(up to --lookback hours) to find an archive that contains a valid snapshot.

Supported chains:
  binance_futures / BTCUSDT
  binance_futures / ETHUSDT
  bybit           / BTCUSDT
  bybit           / ETHUSDT

Usage:
  # Auto-discover and seed all 4 chains
  python seed_remote_chains.py --all

  # Auto-discover a specific chain
  python seed_remote_chains.py --venue bybit --symbol BTCUSDT

  # Manual specific date/hour
  python seed_remote_chains.py --venue bybit --symbol BTCUSDT --date 2026-09-04 --hour 06

Environment:
  HF_TOKEN              - Hugging Face write token (required)
  L2SHOCK_HF_REPO_ID   - HF dataset repo ID (default: maTayefi/l2shock-processed)
  L2SHOCK__CRYPTOHFT__API_KEY - Optional CryptoHFTData API key
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
from pydantic import SecretStr

# ---------------------------------------------------------------------------
# Depth configuration: 0 → 0.25 (25%)
# ---------------------------------------------------------------------------
DEPTH_LOWER = Decimal("0")
DEPTH_UPPER = Decimal("0.25")

# ---------------------------------------------------------------------------
# Supported seed chains
# ---------------------------------------------------------------------------
SEED_CHAINS = {
    ("binance_futures", "BTCUSDT"),
    ("binance_futures", "ETHUSDT"),
    ("bybit", "BTCUSDT"),
    ("bybit", "ETHUSDT"),
}


def _parse_hour(raw_date: str, raw_hour: str) -> datetime:
    """Build an exact UTC hour from date + hour strings."""
    try:
        dt = datetime.strptime(f"{raw_date} {int(raw_hour):02d}", "%Y-%m-%d %H")
    except ValueError as exc:
        raise SystemExit(f"Invalid date/hour: {raw_date} {raw_hour}") from exc
    return dt.replace(tzinfo=timezone.utc)


def _build_preset(venue: str, symbol: str):
    """Build the single-market L2 preset at 25% depth."""
    from l2shock.presets import (
        build_binance_futures_data_preset,
        build_bybit_data_preset,
    )

    base = "BTC" if "BTC" in symbol else "ETH"

    if venue == "binance_futures":
        return build_binance_futures_data_preset(
            base=base,
            lower_fraction=DEPTH_LOWER,
            upper_fraction=DEPTH_UPPER,
        )
    elif venue == "bybit":
        return build_bybit_data_preset(
            base=base,
            lower_fraction=DEPTH_LOWER,
            upper_fraction=DEPTH_UPPER,
        )
    else:
        raise SystemExit(f"Unsupported venue for seeding: {venue}")


def _check_archive_exists(venue: str, symbol: str, hour_utc: datetime) -> bool:
    """Quick Range check to see if the archive exists on CryptoHFTData."""
    date_str = hour_utc.strftime("%Y-%m-%d")
    hour_str = hour_utc.strftime("%H")
    path = f"{venue}/{date_str}/{hour_str}/{symbol}_orderbook.parquet"
    url = f"https://api.cryptohftdata.com/v1/download?file={path}"

    api_key = os.environ.get("L2SHOCK__CRYPTOHFT__API_KEY", "")
    params = {}
    if api_key:
        params["api_key"] = api_key

    try:
        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            # Use Range to avoid downloading the whole file and bypassing HEAD issues
            resp = client.get(url, params=params, headers={"Range": "bytes=0-1023"})
            return resp.status_code in (200, 206)
    except Exception:
        return False


def _download_archive(
    venue: str, symbol: str, hour_utc: datetime, workspace: Path
) -> Path:
    """Download one real CryptoHFTData archive into the workspace."""
    from l2shock.acquisition import SourceDataKind, SourceFileSpec
    from l2shock.acquisition.downloader import CryptoHFTDownloader
    from l2shock.config import CryptoHFTConfig, StorageConfig

    api_key = os.environ.get("L2SHOCK__CRYPTOHFT__API_KEY", "")

    cryptohft = CryptoHFTConfig(
        api_key=SecretStr(api_key),
        request_timeout_seconds=120,
        download_rate_limit_per_minute=55,
        retry_max_attempts=5,
        retry_initial_backoff_seconds=2,
    )

    storage = StorageConfig(
        raw_dir=str(workspace / "raw"),
        cache_dir=str(workspace / "cache"),
        quarantine_dir=str(workspace / "quarantine"),
        export_dir=str(workspace / "exports"),
        log_dir=str(workspace / "logs"),
        backup_dir=str(workspace / "backups"),
        raw_retention_hours=72,
        minimum_free_disk_gib=1.0,
    )

    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=symbol,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour_utc,
    )

    async def _download():
        async with CryptoHFTDownloader(
            cryptohft=cryptohft,
            storage=storage,
            use_api_key=bool(api_key),
        ) as downloader:
            artifact = await downloader.download(spec)
            if artifact is None or not artifact.local_path.exists():
                raise RuntimeError(
                    f"Failed to download {venue}/{symbol} for {hour_utc.isoformat()}"
                )
            return artifact.local_path

    path = asyncio.run(_download())
    print(f"  Downloaded: {path} ({path.stat().st_size:,} bytes)")
    return path


def _process_archive(archive_path: Path, venue: str, symbol: str, hour_utc: datetime):
    """Process one archive through the headless L2 pipeline at 25% depth."""
    from l2shock.acquisition import SourceDataKind, SourceFileSpec, SourceHourStatus
    from l2shock.processing import ProcessingSourceArchive
    from l2shock.remote.headless_processing import process_l2_archive_headlessly

    preset = _build_preset(venue, symbol)

    content = archive_path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()

    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=symbol,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour_utc,
    )

    source_archive = ProcessingSourceArchive(
        spec=spec,
        local_path=archive_path,
        content_sha256=digest,
        file_size_bytes=len(content),
        status=SourceHourStatus.DOWNLOADED,
    )

    print(f"  Processing at depth {DEPTH_LOWER}→{DEPTH_UPPER} ...")
    print(f"  Preset hash: {preset.preset_hash}")

    output = process_l2_archive_headlessly(
        source_archive,
        preset,
        producer_git_commit=os.environ.get("GITHUB_SHA", ""),
        batch_size=131_072,
    )

    # Check if we got a usable output checkpoint (i.e., real snapshot initialized)
    if output.artifact.output_checkpoint is None:
        print("  ✗ No usable output checkpoint — archive lacks a real snapshot.")
        return None

    print(
        f"  ✓ Output checkpoint produced (content_sha256={output.artifact.manifest.output_checkpoint_content_sha256})"
    )
    print(f"  Valid seconds: {output.quality_summary.get('valid_count', 0)}")
    print(f"  Invalid seconds: {output.quality_summary.get('invalid_count', 0)}")

    return output


def _publish_to_hf(output) -> bool:
    """Publish the processed artifact to Hugging Face."""
    from l2shock.remote.hf_repository import HuggingFaceDatasetRepository

    token = os.environ.get("HF_TOKEN", "")
    if not token:
        print("  ✗ HF_TOKEN environment variable is not set. Cannot publish.")
        return False

    repo_id = os.environ.get("L2SHOCK_HF_REPO_ID", "maTayefi/l2shock-processed")

    repo = HuggingFaceDatasetRepository(
        repo_id=repo_id,
        revision="main",
        token=SecretStr(token),
    )

    print(f"  Publishing to {repo_id} ...")
    result = repo.publish_artifact(output.artifact)

    if result.created:
        print(f"  ✓ Published new artifact at revision {result.revision}")
    else:
        print(f"  ✓ Artifact already exists (idempotent) at revision {result.revision}")

    return True


def seed_one_chain_manual(
    venue: str, symbol: str, date_str: str, hour_str: str
) -> bool:
    """Full pipeline: download → process → publish for one specific hour."""
    hour_utc = _parse_hour(date_str, hour_str)
    preset = _build_preset(venue, symbol)

    print(f"\n{'='*60}")
    print(f"  SEED (Manual): {venue} / {symbol}")
    print(f"  Hour: {hour_utc.isoformat()}")
    print(f"  Depth: {DEPTH_LOWER} → {DEPTH_UPPER}")
    print(f"  Preset: {preset.preset_hash}")
    print(f"{'='*60}")

    with tempfile.TemporaryDirectory(prefix="l2shock-seed-") as tmp:
        workspace = Path(tmp)

        print("\n[1/3] Downloading real CryptoHFTData archive...")
        try:
            archive_path = _download_archive(venue, symbol, hour_utc, workspace)
        except Exception as exc:
            print(f"  ✗ Download failed: {exc}")
            return False

        print("\n[2/3] Processing through headless L2 pipeline...")
        try:
            output = _process_archive(archive_path, venue, symbol, hour_utc)
        except Exception as exc:
            print(f"  ✗ Processing failed: {exc}")
            traceback.print_exc()
            return False

        if output is None:
            return False

        print("\n[3/3] Publishing to Hugging Face...")
        try:
            success = _publish_to_hf(output)
        except Exception as exc:
            print(f"  ✗ Publication failed: {exc}")
            traceback.print_exc()
            return False

        if success:
            print(f"\n  ✓✓✓ {venue}/{symbol} seeded successfully at 25% depth ✓✓✓")
        return success


def seed_one_chain_auto(venue: str, symbol: str, max_lookback_hours: int = 96) -> bool:
    """Auto-discover a valid snapshot hour and seed the chain."""
    preset = _build_preset(venue, symbol)

    print(f"\n{'='*60}")
    print(f"  SEED (Auto): {venue} / {symbol}")
    print(f"  Depth: {DEPTH_LOWER} → {DEPTH_UPPER}")
    print(f"  Preset: {preset.preset_hash}")
    print(
        f"  Searching backwards up to {max_lookback_hours} hours for a valid snapshot..."
    )
    print(f"{'='*60}")

    now = datetime.now(timezone.utc)
    # Start probing from 2 hours ago to account for CryptoHFTData publication delays
    current_probe = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)

    for i in range(max_lookback_hours):
        hour_utc = current_probe - timedelta(hours=i)
        date_str = hour_utc.strftime("%Y-%m-%d")
        hour_str = hour_utc.strftime("%H")

        print(
            f"\n[{i+1}/{max_lookback_hours}] Probing {date_str} {hour_str}:00 UTC ..."
        )

        if not _check_archive_exists(venue, symbol, hour_utc):
            print("  ⤷ Archive not available yet or missing.")
            continue

        print("  ✓ Archive found! Downloading...")
        with tempfile.TemporaryDirectory(prefix="l2shock-seed-") as tmp:
            workspace = Path(tmp)
            try:
                archive_path = _download_archive(venue, symbol, hour_utc, workspace)
            except Exception as exc:
                print(f"  ✗ Download failed: {exc}")
                continue

            print("  Processing through headless L2 pipeline...")
            try:
                output = _process_archive(archive_path, venue, symbol, hour_utc)
            except Exception as exc:
                print(f"  ✗ Processing failed: {exc}")
                traceback.print_exc()
                continue

            if output is None:
                print("  ⤷ No usable checkpoint (no snapshot). Trying older hour...")
                continue

            print("  Publishing to Hugging Face...")
            try:
                success = _publish_to_hf(output)
                if success:
                    print(
                        f"\n  ✓✓✓ {venue}/{symbol} seeded successfully at {hour_utc.isoformat()} ✓✓✓"
                    )
                    return True
            except Exception as exc:
                print(f"  ✗ Publication failed: {exc}")
                traceback.print_exc()
                continue

    print(
        f"\n✗ Failed to find a valid snapshot for {venue}/{symbol} in the last {max_lookback_hours} hours."
    )
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Seed remote chains with real CryptoHFTData snapshots at 25% depth."
    )
    parser.add_argument(
        "--venue", choices=["binance_futures", "bybit"], help="Venue to seed"
    )
    parser.add_argument("--symbol", help="e.g. BTCUSDT or ETHUSDT")
    parser.add_argument(
        "--date", help="YYYY-MM-DD (optional, auto-discovers if omitted)"
    )
    parser.add_argument("--hour", help="00-23 (optional, auto-discovers if omitted)")
    parser.add_argument(
        "--all", action="store_true", help="Auto-seed all 4 supported chains"
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=96,
        help="Max hours to look back for auto-discovery (default: 96)",
    )

    args = parser.parse_args()

    if args.all:
        chains = list(SEED_CHAINS)
        results = {}
        for venue, symbol in chains:
            success = seed_one_chain_auto(
                venue, symbol, max_lookback_hours=args.lookback
            )
            results[f"{venue}/{symbol}"] = success

        print("\n" + "=" * 60)
        print("AUTO-SEED SUMMARY")
        print("=" * 60)
        for chain, success in results.items():
            status = "✓ SUCCESS" if success else "✗ FAILED"
            print(f"  {chain:<25} {status}")
        return 0 if all(results.values()) else 1

    if not args.venue or not args.symbol:
        parser.error("You must provide --venue and --symbol, or use --all.")

    chain = (args.venue, args.symbol.upper())
    if chain not in SEED_CHAINS:
        print(f"Unsupported chain: {args.venue}/{args.symbol}")
        print(f"Supported: {sorted(SEED_CHAINS)}")
        return 1

    if args.date and args.hour:
        success = seed_one_chain_manual(
            args.venue, args.symbol.upper(), args.date, args.hour
        )
    else:
        success = seed_one_chain_auto(
            args.venue, args.symbol.upper(), max_lookback_hours=args.lookback
        )

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())

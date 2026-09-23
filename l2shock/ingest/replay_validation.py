# l2shock/ingest/replay_validation.py
"""Command-line validation of adjacent local order-book archives.

Example:

    py -3.14 -m l2shock.ingest.replay_validation ^
      --symbol BTCUSDT ^
      --archive 2026-09-02T12:00:00Z path/hour12.parquet ^
      --archive 2026-09-02T13:00:00Z path/hour13.parquet ^
      --json-out replay_report.json ^
      --checkpoint-out BTCUSDT_13.l2checkpoint

Exit statuses:

    0:
        replay completed and produced a valid final checkpoint

    2:
        command-line, source, decoding, or replay-contract failure

    3:
        replay completed, but no valid final checkpoint was available

    130:
        interrupted or cooperatively cancelled
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    sha256_file,
)
from l2shock.ingest.checkpoint_codec import (
    CheckpointCodecError,
    load_checkpoint_file,
    write_checkpoint_file,
)
from l2shock.ingest.reporting import (
    replay_chain_report_to_dict,
    write_json_report_atomic,
)
from l2shock.ingest.replay import (
    ReplayCancelledError,
    ReplayContractError,
    replay_orderbook_archives,
)
from l2shock.timeutils import require_utc_hour


def parse_utc_hour(value: str) -> datetime:
    """Parse a canonical or ISO-compatible UTC source hour."""
    raw = str(value or "").strip()

    if not raw:
        raise argparse.ArgumentTypeError("archive hour cannot be blank")

    normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "archive hour must be ISO-8601, for example " "2026-09-02T12:00:00Z"
        ) from exc

    try:
        return require_utc_hour("archive hour", parsed)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")

    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate replay across one or more adjacent local "
            "CryptoHFTData order-book archives."
        )
    )

    parser.add_argument(
        "--provider",
        default="cryptohftdata",
    )
    parser.add_argument(
        "--venue",
        default="binance_futures",
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="Instrument supported by the selected venue.",
    )
    parser.add_argument(
        "--archive",
        action="append",
        nargs=2,
        required=True,
        metavar=("UTC_HOUR", "PARQUET_PATH"),
        help=(
            "Adjacent source hour and local Parquet path. "
            "Repeat in strict chronological order."
        ),
    )
    parser.add_argument(
        "--checkpoint-in",
        type=Path,
        default=None,
        help=("Optional checkpoint from the immediately preceding UTC hour."),
    )
    parser.add_argument(
        "--checkpoint-out",
        type=Path,
        default=None,
        help=("Write the final checkpoint when replay finishes valid."),
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Write the replay report as JSON.",
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_int,
        default=65_536,
    )
    parser.add_argument(
        "--cancellation-check-rows",
        type=_positive_int,
        default=4_096,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=("Allow replacing checkpoint and JSON output destinations."),
    )
    parser.add_argument(
        "--hash-final-source",
        action="store_true",
        help=(
            "Hash the final source archive and embed its SHA-256 "
            "in the produced checkpoint."
        ),
    )

    return parser


def _build_sources(
    args: argparse.Namespace,
) -> tuple[tuple[Path, SourceFileSpec], ...]:
    result: list[tuple[Path, SourceFileSpec]] = []

    for raw_hour, raw_path in args.archive:
        hour = parse_utc_hour(raw_hour)
        path = Path(raw_path).expanduser().resolve()

        if not path.is_file():
            raise ReplayContractError(f"Archive is not a regular file: {path}")

        spec = SourceFileSpec(
            provider=args.provider,
            venue=args.venue,
            symbol=args.symbol,
            data_kind=SourceDataKind.ORDERBOOK,
            hour_utc=hour,
        )

        result.append((path, spec))

    return tuple(result)


def _print_summary(
    payload: dict[str, object],
) -> None:
    print(
        json.dumps(
            {
                "schema": payload["schema"],
                "schema_version": payload["schema_version"],
                "provider": payload["provider"],
                "venue": payload["venue"],
                "symbol": payload["symbol"],
                "archive_count": payload["archive_count"],
                "finally_valid": payload["finally_valid"],
                "total_events_seen": payload["total_events_seen"],
                "total_invalidations": payload["total_invalidations"],
                "total_snapshots_applied": (payload["total_snapshots_applied"]),
                "final_checkpoint": payload["final_checkpoint"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        sources = _build_sources(args)

        initial_checkpoint = (
            load_checkpoint_file(args.checkpoint_in)
            if args.checkpoint_in is not None
            else None
        )

        final_source_sha256: str | None = None

        if args.hash_final_source:
            final_source_sha256, _size = sha256_file(sources[-1][0])

        report = replay_orderbook_archives(
            sources,
            initial_checkpoint=initial_checkpoint,
            batch_size=args.batch_size,
            final_source_content_sha256=(final_source_sha256),
            cancellation_check_interval_rows=(args.cancellation_check_rows),
        )

        payload = replay_chain_report_to_dict(report)

        checkpoint_artifact: dict[str, object] | None = None

        if args.checkpoint_out is not None:
            if report.final_checkpoint is None:
                payload["checkpoint_output"] = {
                    "written": False,
                    "path": str(args.checkpoint_out.expanduser().resolve()),
                    "reason": (
                        "Replay did not finish with a complete " "valid checkpoint"
                    ),
                }
            else:
                info = write_checkpoint_file(
                    args.checkpoint_out,
                    report.final_checkpoint,
                    overwrite=args.overwrite,
                )

                checkpoint_artifact = {
                    "written": True,
                    "path": str(args.checkpoint_out.expanduser().resolve()),
                    "format_version": info.format_version,
                    "payload_size_bytes": (info.payload_size_bytes),
                    "encoded_size_bytes": (info.encoded_size_bytes),
                    "payload_sha256": info.payload_sha256,
                    "content_sha256": info.content_sha256,
                    "level_count": info.level_count,
                    "bid_level_count": info.bid_level_count,
                    "ask_level_count": info.ask_level_count,
                }
                payload["checkpoint_output"] = checkpoint_artifact

        if args.json_out is not None:
            write_json_report_atomic(
                args.json_out,
                payload,
                overwrite=args.overwrite,
            )

        _print_summary(payload)

        if checkpoint_artifact is not None:
            print()
            print("Checkpoint written: " f"{checkpoint_artifact['path']}")

        if args.json_out is not None:
            print()
            print("JSON report written: " f"{args.json_out.expanduser().resolve()}")

        return 0 if report.finally_valid else 3

    except ReplayCancelledError as exc:
        print(
            f"Replay cancelled: {exc}",
            file=sys.stderr,
        )
        return 130

    except KeyboardInterrupt:
        print(
            "Replay interrupted by user.",
            file=sys.stderr,
        )
        return 130

    except (
        ReplayContractError,
        CheckpointCodecError,
        FileExistsError,
        OSError,
        ValueError,
    ) as exc:
        print(
            f"Replay validation failed: " f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_parser",
    "main",
    "parse_utc_hour",
]

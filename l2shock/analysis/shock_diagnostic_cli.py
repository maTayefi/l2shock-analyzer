# l2shock/analysis/shock_diagnostic_cli.py
"""Standalone, read-only shock diagnostic export.

Run when no writer is replacing the selected analytical hours. This command
does not participate in the NiceGUI application's operation lock.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from l2shock.analysis.shock_dataset import ShockDatasetRequest
from l2shock.analysis.shock_diagnostic import (
    run_verified_shock_diagnostic,
    shock_diagnostic_json_bytes,
)
from l2shock.analysis.shock_evidence import ShockEvidenceConfig
from l2shock.analysis.shock_start import (
    DEFAULT_SHOCK_SCALES,
    ShockStartConfig,
    ShockStructuralScale,
)
from l2shock.db.engine import session_scope

_MAX_SCAN_SECONDS = 86_400


def _datetime_utc(text: str) -> datetime:
    try:
        result = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Use an ISO-8601 UTC timestamp, e.g. 2026-09-20T12:00:00Z"
        ) from exc
    if (
        result.tzinfo is None
        or result.utcoffset() is None
        or result.utcoffset().total_seconds() != 0
    ):
        raise argparse.ArgumentTypeError("Timestamp must explicitly specify UTC")
    return result


def _scale(text: str) -> ShockStructuralScale:
    # Example: major:0.20:60:4
    parts = text.split(":")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "Scale must be NAME:FRACTION:PIVOT_SECONDS:HORIZON_MULTIPLIER"
        )
    try:
        return ShockStructuralScale(
            name=parts[0],
            minimum_leg_fraction=Decimal(parts[1]),
            pivot_radius_seconds=int(parts[2]),
            forward_radius_multiplier=int(parts[3]),
        )
    except (ValueError, InvalidOperation) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export verified one-second L2 shock-start hypotheses "
            "for human review; no price gate, score, or LM changes."
        )
    )
    parser.add_argument("--base", required=True, choices=("BTC", "ETH"))
    parser.add_argument("--preset-hash", required=True)
    parser.add_argument(
        "--start-utc",
        required=True,
        type=_datetime_utc,
    )
    parser.add_argument(
        "--end-utc",
        required=True,
        type=_datetime_utc,
    )
    parser.add_argument(
        "--scale",
        action="append",
        type=_scale,
        help=(
            "Repeat in descending threshold order; omit for "
            "major/medium/minor defaults. Example: major:0.20:60:4"
        ),
    )
    parser.add_argument(
        "--maximum-offset-seconds",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--minimum-channel-leg-fraction",
        type=Decimal,
        default=Decimal("0.05"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New .json file; existing files are never overwritten",
    )
    return parser


def _write_new_file(path: Path, contents: bytes) -> None:
    """Publish a complete JSON file, never replacing an existing file."""
    target = path.expanduser().resolve()
    if target.suffix.lower() != ".json":
        raise ValueError("--output must end in .json")
    if not target.parent.is_dir():
        raise ValueError("--output parent directory does not exist")

    staged = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            staged = Path(handle.name)
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())

        # Same-directory hard link: publish only once the staging file is
        # complete; fail if the destination already exists.
        os.link(staged, target)
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)

    try:
        request = ShockDatasetRequest(
            base=args.base,
            preset_hash=args.preset_hash,
            requested_start_utc=args.start_utc,
            requested_end_utc=args.end_utc,
        )
        seconds = (
            request.requested_end_utc - request.requested_start_utc
        ).total_seconds()
        if seconds < 0 or seconds >= _MAX_SCAN_SECONDS:
            raise ValueError(
                "Diagnostic scan must be shorter than 24 hours; "
                "use separate exports for longer investigations"
            )
        if args.output.expanduser().exists():
            raise FileExistsError(f"Refusing to overwrite {args.output}")

        candidate_config = ShockStartConfig(
            scales=(
                tuple(args.scale) if args.scale is not None else DEFAULT_SHOCK_SCALES
            )
        )
        evidence_config = ShockEvidenceConfig(
            maximum_offset_seconds=args.maximum_offset_seconds,
            minimum_channel_leg_fraction=(args.minimum_channel_leg_fraction),
        )

        with session_scope() as session:
            review = run_verified_shock_diagnostic(
                session,
                request,
                candidate_config=candidate_config,
                evidence_config=evidence_config,
            )

        contents = shock_diagnostic_json_bytes(review)
        _write_new_file(args.output, contents)
        print(
            f"Wrote {len(review.ordered_areas)} B areas; "
            f"{review.evidence_result.hypothesis_count} hypotheses; "
            f"review_id={review.review_id}",
            file=sys.stderr,
        )
        return 0
    except (ValueError, OSError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())

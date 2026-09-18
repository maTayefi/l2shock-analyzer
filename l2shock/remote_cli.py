# l2shock/remote_cli.py
"""Local filesystem CLI for deterministic remote-hour artifact construction.

This is the command-line entry point used before adding network adapters.

It deliberately performs no:

- PostgreSQL access;
- NiceGUI work;
- Hugging Face access;
- CryptoHFTData download;
- GitHub Actions scheduling.

Input source archives must already exist under the canonical raw-root layout:

    <input-dir>/
        cryptohftdata/
            <venue>/
                YYYY-MM-DD/
                    HH/
                        <instrument>_<kind>.parquet

Output artifacts use ``RemoteArtifactKey.relative_path`` beneath
``--output-dir``. The matching canonical external manifest is written beside
the transport artifact.

Identical repeated publication is idempotent. Conflicting existing output is
rejected unless ``--overwrite`` is explicitly supplied.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath

from l2shock.acquisition import (
    AcquisitionError,
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
    sha256_file,
    validate_parquet_file,
)
from l2shock.ingest import MAX_CHECKPOINT_PAYLOAD_BYTES
from l2shock.presets import (
    build_binance_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.processing import (
    ProcessingError,
    ProcessingSourceArchive,
)
from l2shock.remote.artifact_codec import (
    RemoteProcessedArtifact,
    read_remote_artifact_file,
    write_remote_artifact_file,
)
from l2shock.remote.contracts import RemoteContractError
from l2shock.remote.headless_processing import (
    process_l2_archive_headlessly,
    process_price_archives_headlessly,
)


class LocalRemoteCLIError(RuntimeError):
    """The local remote-processing CLI could not complete safely."""


class LocalArtifactConflictError(LocalRemoteCLIError):
    """An existing local publication owns different immutable content."""


def _positive_integer(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc

    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")

    return result


def _canonical_utc_hour(value: str) -> datetime:
    text = str(value or "").strip()

    if not text.endswith("Z"):
        raise argparse.ArgumentTypeError("hour must use canonical UTC text ending in Z")

    try:
        result = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "hour must be ISO-8601 UTC text such as " "2026-09-14T12:00:00Z"
        ) from exc

    if result.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("hour must have UTC offset +00:00")

    if result.minute or result.second or result.microsecond:
        raise argparse.ArgumentTypeError("hour must identify an exact UTC hour")

    canonical = result.isoformat().replace("+00:00", "Z")

    if text != canonical:
        raise argparse.ArgumentTypeError("hour is valid but not canonically encoded")

    return result


def _depth_fraction(value: str) -> Decimal:
    text = str(value or "").strip()

    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "depth fraction must be an exact decimal"
        ) from exc

    if not result.is_finite():
        raise argparse.ArgumentTypeError("depth fraction must be finite")

    return result


def _canonical_output_path(
    output_root: Path,
    relative_path: str,
) -> Path:
    root = Path(output_root).expanduser().resolve()
    pure = PurePosixPath(relative_path)

    if pure.is_absolute() or ".." in pure.parts:
        raise LocalRemoteCLIError("Remote output path is not a safe relative path")

    result = root.joinpath(*pure.parts).resolve()

    try:
        result.relative_to(root)
    except ValueError as exc:
        raise LocalRemoteCLIError(
            "Remote output path escaped the output directory"
        ) from exc

    return result


def _processing_archive(
    input_root: Path,
    spec: SourceFileSpec,
) -> ProcessingSourceArchive:
    path = spec.local_path(input_root)

    if path.is_symlink():
        raise LocalRemoteCLIError("CLI source archive cannot be a symbolic link")

    if not path.is_file():
        raise FileNotFoundError(f"Required source archive does not exist: {path}")

    validate_parquet_file(
        path,
        spec,
    )
    digest, size = sha256_file(path)

    return ProcessingSourceArchive(
        spec=spec,
        local_path=path,
        content_sha256=digest,
        file_size_bytes=size,
        status=SourceHourStatus.DOWNLOADED,
    )


def _read_checkpoint_bytes(
    path: Path | None,
) -> bytes | None:
    if path is None:
        return None

    source = Path(path).expanduser()

    if source.is_symlink():
        raise LocalRemoteCLIError("Input checkpoint cannot be a symbolic link")

    source = source.resolve()

    if not source.is_file():
        raise FileNotFoundError(f"Input checkpoint does not exist: {source}")

    size = int(source.stat().st_size)
    maximum = MAX_CHECKPOINT_PAYLOAD_BYTES + 1024

    if size <= 0:
        raise LocalRemoteCLIError("Input checkpoint is empty")

    if size > maximum:
        raise LocalRemoteCLIError("Input checkpoint exceeds the supported size limit")

    return source.read_bytes()


def _preset_for_l2_target(
    target: SourceFileSpec,
    *,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
):
    builders = {
        "binance_futures": build_binance_futures_data_preset,
        "bybit": build_bybit_data_preset,
        "okx_futures": build_okx_futures_data_preset,
    }

    try:
        builder = builders[target.venue]
    except KeyError as exc:
        raise LocalRemoteCLIError(
            "No approved L2 preset builder exists for " f"venue {target.venue!r}"
        ) from exc

    return builder(
        base=target.base,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
    )


def _sha256_regular_file(path: Path) -> tuple[str, int]:
    source = Path(path)

    if source.is_symlink() or not source.is_file():
        raise LocalRemoteCLIError("Published artifact is not a regular file")

    digest = hashlib.sha256()
    size = 0

    with source.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)
            size += len(chunk)

    if size <= 0:
        raise LocalRemoteCLIError("Published artifact is empty")

    return digest.hexdigest(), size


def _write_bytes_atomic(
    path: Path,
    data: bytes,
    *,
    overwrite: bool,
) -> bool:
    """Write exact bytes using atomic replace or create-if-absent.

    Returns ``True`` when this call created or replaced the destination.
    An identical pre-existing destination is an idempotent success and returns
    ``False``.
    """

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if destination.exists():
        if destination.is_symlink() or not destination.is_file():
            raise LocalArtifactConflictError(
                "Manifest destination exists but is not a regular file"
            )

        if not overwrite:
            if destination.read_bytes() == data:
                return False

            raise LocalArtifactConflictError(
                "Existing external manifest owns different content"
            )

    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")

    try:
        with temporary.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

        if overwrite:
            os.replace(
                temporary,
                destination,
            )
            return True

        try:
            os.link(
                temporary,
                destination,
            )
        except FileExistsError:
            if (
                destination.is_file()
                and not destination.is_symlink()
                and destination.read_bytes() == data
            ):
                return False

            raise LocalArtifactConflictError(
                "A racing writer published a conflicting external manifest"
            )
        except OSError as exc:
            raise LocalRemoteCLIError(
                "Could not atomically publish the external manifest. "
                "The output filesystem must support same-filesystem "
                "hard-link creation."
            ) from exc
        else:
            temporary.unlink()
            return True

    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _verify_existing_artifact(
    path: Path,
    artifact: RemoteProcessedArtifact,
) -> None:
    try:
        existing = read_remote_artifact_file(
            path,
            expected_key=artifact.manifest.key,
        )
    except Exception as exc:
        raise LocalArtifactConflictError(
            "Existing transport artifact failed verification"
        ) from exc

    if existing != artifact:
        raise LocalArtifactConflictError(
            "Existing transport artifact owns different immutable content"
        )


def _publish_artifact_pair(
    output_root: Path,
    artifact: RemoteProcessedArtifact,
    *,
    overwrite: bool,
) -> dict[str, object]:
    key = artifact.manifest.key
    artifact_path = _canonical_output_path(
        output_root,
        key.relative_path,
    )
    manifest_path = _canonical_output_path(
        output_root,
        key.manifest_relative_path,
    )
    manifest_bytes = artifact.manifest.canonical_json_bytes

    if manifest_path.exists() and not overwrite:
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise LocalArtifactConflictError(
                "Existing manifest path is not a regular file"
            )

        if manifest_path.read_bytes() != manifest_bytes:
            raise LocalArtifactConflictError(
                "Existing external manifest owns different content"
            )

    artifact_created = False

    if artifact_path.exists() and not overwrite:
        _verify_existing_artifact(
            artifact_path,
            artifact,
        )
    else:
        try:
            write_remote_artifact_file(
                artifact_path,
                artifact,
                overwrite=overwrite,
            )
            artifact_created = True
        except FileExistsError:
            _verify_existing_artifact(
                artifact_path,
                artifact,
            )

    manifest_created = _write_bytes_atomic(
        manifest_path,
        manifest_bytes,
        overwrite=overwrite,
    )

    verified = read_remote_artifact_file(
        artifact_path,
        expected_key=key,
        expected_manifest_sha256=(artifact.manifest.manifest_sha256),
        external_manifest_bytes=manifest_bytes,
    )

    if verified != artifact:
        raise LocalArtifactConflictError(
            "Published artifact pair differs from the requested result"
        )

    transport_sha256, file_size = _sha256_regular_file(artifact_path)

    return {
        "schema": "l2shock.remote_cli_publication",
        "schema_version": 1,
        "artifact_kind": key.kind.value,
        "artifact_path": str(artifact_path),
        "manifest_path": str(manifest_path),
        "relative_path": key.relative_path,
        "manifest_relative_path": key.manifest_relative_path,
        "artifact_created": artifact_created,
        "manifest_created": manifest_created,
        "file_size_bytes": file_size,
        "transport_sha256": transport_sha256,
        "manifest_sha256": artifact.manifest.manifest_sha256,
        "analytical_content_sha256": (artifact.manifest.content_sha256),
    }


def _common_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        "--provider",
        default="cryptohftdata",
    )
    parser.add_argument(
        "--venue",
        required=True,
    )
    parser.add_argument(
        "--instrument",
        required=True,
    )
    parser.add_argument(
        "--hour",
        required=True,
        type=_canonical_utc_hour,
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        type=Path,
        help="Canonical raw-source root directory.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Root directory for processed artifacts and manifests.",
    )
    parser.add_argument(
        "--producer-git-commit",
        default=os.environ.get("GITHUB_SHA", ""),
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_integer,
        default=131_072,
    )
    parser.add_argument(
        "--cancellation-check-rows",
        type=_positive_integer,
        default=4_096,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly replace a complete existing local publication.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build deterministic l2shock processed-hour artifacts from "
            "already-downloaded canonical source archives."
        )
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    l2_parser = subparsers.add_parser(
        "l2",
        help="Process one order-book source hour.",
    )
    _common_arguments(l2_parser)
    l2_parser.add_argument(
        "--depth-lower",
        required=True,
        type=_depth_fraction,
    )
    l2_parser.add_argument(
        "--depth-upper",
        required=True,
        type=_depth_fraction,
    )
    l2_parser.add_argument(
        "--checkpoint-in",
        type=Path,
        default=None,
    )
    l2_parser.add_argument(
        "--cancellation-check-levels",
        type=_positive_integer,
        default=1_024,
    )
    l2_parser.add_argument(
        "--imbalance-decimal-precision",
        type=_positive_integer,
        default=34,
    )

    price_parser = subparsers.add_parser(
        "price",
        help="Process one Binance Futures trade-time hour.",
    )
    _common_arguments(price_parser)
    price_parser.add_argument(
        "--source-offset",
        type=int,
        action="append",
        choices=(-1, 0, 1),
        default=None,
        help=(
            "Explicit source-hour offset. Repeat as needed. "
            "Default is current source offset 0 only."
        ),
    )
    price_parser.add_argument(
        "--cancellation-check-records",
        type=_positive_integer,
        default=4_096,
    )

    return parser


def _run_l2(args: argparse.Namespace) -> dict[str, object]:
    target = SourceFileSpec(
        provider=args.provider,
        venue=args.venue,
        symbol=args.instrument,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=args.hour,
    )
    archive = _processing_archive(
        args.input_dir,
        target,
    )
    preset = _preset_for_l2_target(
        target,
        lower_fraction=args.depth_lower,
        upper_fraction=args.depth_upper,
    )
    checkpoint_bytes = _read_checkpoint_bytes(args.checkpoint_in)

    output = process_l2_archive_headlessly(
        archive,
        preset,
        input_checkpoint_bytes=checkpoint_bytes,
        producer_git_commit=(str(args.producer_git_commit or "").strip() or None),
        batch_size=args.batch_size,
        cancellation_check_interval_rows=(args.cancellation_check_rows),
        cancellation_check_interval_levels=(args.cancellation_check_levels),
        imbalance_decimal_precision=(args.imbalance_decimal_precision),
    )

    return _publish_artifact_pair(
        args.output_dir,
        output.artifact,
        overwrite=args.overwrite,
    )


def _run_price(args: argparse.Namespace) -> dict[str, object]:
    target = SourceFileSpec(
        provider=args.provider,
        venue=args.venue,
        symbol=args.instrument,
        data_kind=SourceDataKind.TRADES,
        hour_utc=args.hour,
    )

    raw_offsets = tuple(args.source_offset) if args.source_offset is not None else (0,)
    offsets = tuple(sorted(set(raw_offsets)))

    if len(offsets) != len(raw_offsets):
        raise LocalRemoteCLIError("Price source offsets contain a duplicate")

    if 0 not in offsets:
        raise LocalRemoteCLIError(
            "Price source offsets must include the current offset 0"
        )

    archives = tuple(
        _processing_archive(
            args.input_dir,
            SourceFileSpec(
                provider=target.provider,
                venue=target.venue,
                symbol=target.symbol,
                data_kind=SourceDataKind.TRADES,
                hour_utc=target.hour_utc + timedelta(hours=offset),
            ),
        )
        for offset in offsets
    )

    output = process_price_archives_headlessly(
        target,
        archives,
        producer_git_commit=(str(args.producer_git_commit or "").strip() or None),
        batch_size=args.batch_size,
        cancellation_check_interval_rows=(args.cancellation_check_rows),
        cancellation_check_interval_records=(args.cancellation_check_records),
    )

    return _publish_artifact_pair(
        args.output_dir,
        output.artifact,
        overwrite=args.overwrite,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "l2":
            result = _run_l2(args)
        elif args.command == "price":
            result = _run_price(args)
        else:
            raise LocalRemoteCLIError("Unsupported remote CLI command")

    except KeyboardInterrupt:
        print(
            "Remote processing was interrupted.",
            file=sys.stderr,
        )
        return 130

    except LocalArtifactConflictError as exc:
        print(
            f"Remote artifact conflict: {exc}",
            file=sys.stderr,
        )
        return 4

    except FileNotFoundError as exc:
        print(
            f"Required input is unavailable: {exc}",
            file=sys.stderr,
        )
        return 3

    except (
        AcquisitionError,
        ProcessingError,
        RemoteContractError,
        LocalRemoteCLIError,
        TypeError,
        ValueError,
    ) as exc:
        print(
            f"Remote processing input or contract error: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2

    except Exception as exc:
        print(
            f"Unexpected remote processing failure: " f"{type(exc).__name__}",
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            result,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "LocalArtifactConflictError",
    "LocalRemoteCLIError",
    "build_parser",
    "main",
]

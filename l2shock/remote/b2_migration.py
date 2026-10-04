# l2shock/remote/b2_migration.py
"""Verified, bounded HF/local processed-artifact migration to B2.

No raw acquisition, replay, PostgreSQL access, or workflow cutover occurs here.

The destination requires externally enforced single-writer ownership.
Acknowledgement flags are not distributed locks.

HF reads are pinned to one immutable commit for the entire migration.
Local reads require canonical artifact/manifest pairs and existing filesystem
ownership checks.

Every copied artifact is published through the existing B2 repository and
read back from the exact returned publication reference.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Protocol

from dotenv import load_dotenv
from pydantic import SecretStr

from l2shock.config import B2Config
from l2shock.filesystem import (
    absolute_path_without_resolution,
    path_entry_exists,
    prepare_owned_file_path,
    require_owned_regular_file,
)
from l2shock.ingest import BookSampleQuality
from l2shock.liquidity import decode_hourly_liquidity_blocks
from l2shock.presets import (
    build_binance_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    RemoteProcessedArtifact,
    read_remote_artifact_file,
)
from l2shock.remote.b2_repository import B2ProcessedArtifactRepository
from l2shock.remote.b2_transport import B2ObjectStore
from l2shock.remote.contracts import RemoteArtifactKey, RemoteArtifactKind
from l2shock.remote.hf_repository import (
    DownloadedHuggingFaceArtifact,
    HuggingFaceDatasetRepository,
)
from l2shock.remote.import_contracts import (
    B2_STORAGE_BACKEND,
    RemoteImportStorageIdentity,
)
from l2shock.timeutils import require_utc_hour

MAX_MIGRATION_HOURS = 744
MAX_MIGRATION_KEYS = MAX_MIGRATION_HOURS * 8
MAX_LOCAL_MANIFEST_BYTES = 4 * 1024 * 1024

CHAIN_PROFILES = {
    "binance_btc": ("binance_futures", "BTCUSDT", "BTC"),
    "binance_eth": ("binance_futures", "ETHUSDT", "ETH"),
    "bybit_btc": ("bybit", "BTCUSDT", "BTC"),
    "bybit_eth": ("bybit", "ETHUSDT", "ETH"),
    "okx_btc": ("okx_futures", "BTC-USDT-SWAP", "BTC"),
    "okx_eth": ("okx_futures", "ETH-USDT-SWAP", "ETH"),
}

RecordSink = Callable[[Mapping[str, object]], None]


class B2MigrationError(RuntimeError):
    """Migration cannot safely continue."""


class MigrationSource(Protocol):
    @property
    def identity(self) -> Mapping[str, object]: ...

    def read(self, key: RemoteArtifactKey) -> RemoteProcessedArtifact | None: ...


def _commit_sha(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise B2MigrationError("Migration requires a full canonical HF commit SHA")
    return value


class PinnedHuggingFaceMigrationSource:
    """Resolve once; never follow the branch again during this run."""

    def __init__(self, repository: HuggingFaceDatasetRepository) -> None:
        # Repository-protocol doubles are accepted for offline tests.
        if not callable(getattr(repository, "current_revision", None)):
            raise TypeError("repository must provide current_revision")
        if not callable(getattr(repository, "download_artifact", None)):
            raise TypeError("repository must provide download_artifact")

        self._repository = repository
        self._revision = _commit_sha(repository.current_revision())
        self._repo_id = str(repository.repo_id)

    @property
    def identity(self) -> Mapping[str, object]:
        return {
            "backend": "hugging_face",
            "repo_id": self._repo_id,
            "revision": self._revision,
        }

    def read(self, key: RemoteArtifactKey) -> RemoteProcessedArtifact | None:
        downloaded = self._repository.download_artifact(
            key,
            revision=self._revision,
        )
        if downloaded is None:
            return None
        if not isinstance(downloaded, DownloadedHuggingFaceArtifact):
            raise B2MigrationError("HF returned an unsupported verified download")
        if downloaded.revision != self._revision:
            raise B2MigrationError("HF migration download changed pinned revision")
        if downloaded.artifact.manifest.key != key:
            raise B2MigrationError("HF migration download changed artifact identity")
        return downloaded.artifact


class LocalMigrationSource:
    """Read canonical processed transport/manifest pairs beneath one root."""

    def __init__(self, root: Path) -> None:
        self._root = absolute_path_without_resolution(root)

    @property
    def identity(self) -> Mapping[str, object]:
        return {
            "backend": "verified_local_files",
            "root": str(self._root),
        }

    def read(self, key: RemoteArtifactKey) -> RemoteProcessedArtifact | None:
        artifact_path = self._root.joinpath(*PurePosixPath(key.relative_path).parts)
        manifest_path = self._root.joinpath(
            *PurePosixPath(key.manifest_relative_path).parts
        )

        artifact_exists = path_entry_exists(artifact_path)
        manifest_exists = path_entry_exists(manifest_path)

        if not artifact_exists and not manifest_exists:
            return None
        if artifact_exists != manifest_exists:
            raise B2MigrationError("Local migration pair is incomplete")

        artifact_path = require_owned_regular_file(self._root, artifact_path)
        manifest_path = require_owned_regular_file(self._root, manifest_path)

        with manifest_path.open("rb") as handle:
            manifest_bytes = handle.read(MAX_LOCAL_MANIFEST_BYTES + 1)

        if len(manifest_bytes) > MAX_LOCAL_MANIFEST_BYTES:
            raise B2MigrationError("Local external manifest exceeds migration limit")

        artifact = read_remote_artifact_file(
            artifact_path,
            expected_key=key,
            external_manifest_bytes=manifest_bytes,
        )

        # Recheck observable ownership after reading.
        require_owned_regular_file(self._root, artifact_path)
        require_owned_regular_file(self._root, manifest_path)
        return artifact


def build_migration_keys(
    *,
    chains: Sequence[str],
    start_utc: datetime,
    end_utc: datetime,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
) -> tuple[RemoteArtifactKey, ...]:
    """Plan exact processed artifacts over half-open [start, end)."""
    start = require_utc_hour("start_utc", start_utc)
    end = require_utc_hour("end_utc", end_utc)
    hours = (end - start) // timedelta(hours=1)

    if not 1 <= hours <= MAX_MIGRATION_HOURS:
        raise B2MigrationError(
            f"Migration range must contain 1-{MAX_MIGRATION_HOURS} UTC hours"
        )

    selected = tuple(chains)
    if (
        not selected
        or len(set(selected)) != len(selected)
        or any(chain not in CHAIN_PROFILES for chain in selected)
    ):
        raise B2MigrationError("Migration chains must be unique approved profiles")

    builders = {
        "binance_futures": build_binance_futures_data_preset,
        "bybit": build_bybit_data_preset,
        "okx_futures": build_okx_futures_data_preset,
    }
    keys: list[RemoteArtifactKey] = []

    for chain in selected:
        venue, instrument, base = CHAIN_PROFILES[chain]
        preset = builders[venue](
            base=base,
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
        )

        for offset in range(hours):
            hour = start + timedelta(hours=offset)
            keys.append(
                RemoteArtifactKey(
                    kind=RemoteArtifactKind.L2,
                    provider="cryptohftdata",
                    venue=venue,
                    instrument=instrument,
                    hour_utc=hour,
                    preset_hash=preset.preset_hash,
                )
            )

            if venue == "binance_futures":
                keys.append(
                    RemoteArtifactKey(
                        kind=RemoteArtifactKind.PRICE,
                        provider="cryptohftdata",
                        venue=venue,
                        instrument=instrument,
                        hour_utc=hour,
                    )
                )

    return tuple(sorted(keys, key=lambda key: (key.hour_utc, key.relative_path)))


def terminal_l2_keys(
    keys: Sequence[RemoteArtifactKey],
) -> tuple[RemoteArtifactKey, ...]:
    newest: dict[tuple[str, str, str, str | None], RemoteArtifactKey] = {}

    for key in keys:
        if key.kind is not RemoteArtifactKind.L2:
            continue
        chain = (key.provider, key.venue, key.instrument, key.preset_hash)
        previous = newest.get(chain)
        if previous is None or key.hour_utc > previous.hour_utc:
            newest[chain] = key

    return tuple(sorted(newest.values(), key=lambda key: key.relative_path))


def _check_artifact(
    key: RemoteArtifactKey,
    artifact: RemoteProcessedArtifact,
    *,
    require_checkpoint: bool = False,
) -> None:
    if not isinstance(
        artifact,
        (RemoteL2ProcessedArtifact, RemotePriceProcessedArtifact),
    ):
        raise B2MigrationError("Source returned an unsupported processed artifact")
    if artifact.manifest.key != key:
        raise B2MigrationError("Source artifact does not own the requested key")

    if key.kind is RemoteArtifactKind.L2:
        if not isinstance(artifact, RemoteL2ProcessedArtifact):
            raise B2MigrationError("L2 key does not contain an L2 artifact")

        if require_checkpoint and artifact.output_checkpoint is None:
            raise B2MigrationError("Terminal seed has no usable output checkpoint")

        if artifact.output_checkpoint is not None:
            decoded = decode_hourly_liquidity_blocks(artifact.encoded)
            if not any(
                quality is BookSampleQuality.VALID for quality in decoded.quality
            ):
                # Do not silently rewrite historical identity to repair it.
                raise B2MigrationError(
                    "Source carries a checkpoint for an all-invalid L2 hour"
                )
    elif not isinstance(artifact, RemotePriceProcessedArtifact):
        raise B2MigrationError("Price key does not contain a price artifact")


@dataclass(frozen=True, slots=True)
class MigrationCounts:
    planned: int
    created: int
    reused: int
    missing: int

    @property
    def complete(self) -> bool:
        return self.missing == 0 and self.created + self.reused == self.planned


def migrate_artifacts(
    source: MigrationSource,
    destination: B2ProcessedArtifactRepository,
    keys: Sequence[RemoteArtifactKey],
    *,
    single_writer_confirmed: bool,
    record: RecordSink,
    require_terminal_checkpoints: bool = True,
) -> MigrationCounts:
    """Copy verified identities; a journal success never precedes read-back."""
    if single_writer_confirmed is not True:
        raise B2MigrationError("External B2 single-writer ownership is required")
    if not isinstance(require_terminal_checkpoints, bool):
        raise TypeError("require_terminal_checkpoints must be bool")
    if not isinstance(destination, B2ProcessedArtifactRepository):
        raise TypeError("destination must be B2ProcessedArtifactRepository")
    if not callable(record):
        raise TypeError("record must be callable")

    planned = tuple(keys)
    if (
        not planned
        or len(planned) > MAX_MIGRATION_KEYS
        or any(not isinstance(key, RemoteArtifactKey) for key in planned)
        or len(set(planned)) != len(planned)
    ):
        raise B2MigrationError("Migration requires a bounded unique typed key plan")

    terminals = terminal_l2_keys(planned)
    if require_terminal_checkpoints and not terminals:
        raise B2MigrationError("Seed migration requires at least one terminal L2 key")

    planned = tuple(sorted(planned, key=lambda key: (key.hour_utc, key.relative_path)))
    terminal_set = set(terminals)

    created = reused = missing = 0
    current_key: str | None = None
    stage = "start"

    try:
        record(
            {
                "event": "start",
                "schema": "l2shock.b2_migration_journal",
                "schema_version": 1,
                "source": dict(source.identity),
                "destination": {
                    "backend": B2_STORAGE_BACKEND,
                    "endpoint_url": destination.endpoint_url,
                    "bucket": destination.bucket,
                },
                "planned_count": len(planned),
                "require_terminal_checkpoints": require_terminal_checkpoints,
                "terminal_l2_keys": [key.relative_path for key in terminals],
            }
        )

        if require_terminal_checkpoints:
            # At most one artifact is retained at a time. This preflight
            # does not cache a potentially large multi-chain artifact set.
            for key in terminals:
                stage = "terminal_preflight"
                current_key = key.relative_path
                artifact = source.read(key)
                if artifact is None:
                    raise B2MigrationError("Required terminal seed is missing")
                _check_artifact(key, artifact, require_checkpoint=True)
                record(
                    {
                        "event": "terminal_verified",
                        "key": key.relative_path,
                        "output_checkpoint_content_sha256": (
                            artifact.manifest.output_checkpoint_content_sha256
                        ),
                    }
                )
                del artifact

        for key in planned:
            current_key = key.relative_path
            stage = "source_read"
            artifact = source.read(key)

            if artifact is None:
                missing += 1
                record({"event": "missing", "key": current_key})
                continue

            _check_artifact(
                key,
                artifact,
                require_checkpoint=(
                    require_terminal_checkpoints and key in terminal_set
                ),
            )

            stage = "publication_intent"
            record(
                {
                    "event": "publication_intent",
                    "key": current_key,
                    "manifest_sha256": artifact.manifest.manifest_sha256,
                    "analytical_content_sha256": artifact.manifest.content_sha256,
                    "input_checkpoint_content_sha256": (
                        artifact.manifest.input_checkpoint_content_sha256
                    ),
                    "output_checkpoint_content_sha256": (
                        artifact.manifest.output_checkpoint_content_sha256
                    ),
                }
            )

            stage = "publish"
            published = destination.publish_artifact(
                artifact,
                single_writer_confirmed=True,
            )

            stage = "pinned_readback"
            downloaded = destination.require_artifact(
                key,
                reference=published.reference,
            )
            if (
                downloaded.reference != published.reference
                or downloaded.artifact != artifact
            ):
                raise B2MigrationError("B2 read-back changed migrated identity")

            identity = RemoteImportStorageIdentity(
                backend=B2_STORAGE_BACKEND,
                endpoint_url=destination.endpoint_url,
                bucket=destination.bucket,
                publication_reference=published.reference,
            )

            stage = "success_receipt"
            record(
                {
                    "event": "copied",
                    "key": current_key,
                    "created": published.created,
                    "manifest_sha256": artifact.manifest.manifest_sha256,
                    "analytical_content_sha256": artifact.manifest.content_sha256,
                    "storage_identity": identity.to_canonical_dict(),
                }
            )

            if published.created:
                created += 1
            else:
                reused += 1

        counts = MigrationCounts(len(planned), created, reused, missing)
        stage = "summary"
        record(
            {
                "event": "summary",
                "complete": counts.complete,
                "planned": counts.planned,
                "created": counts.created,
                "reused": counts.reused,
                "missing": counts.missing,
            }
        )
        return counts

    except BaseException as exc:
        # Never emit arbitrary remote exception messages or arguments.
        # Preserve the original exception if the journal is itself failing.
        try:
            record(
                {
                    "event": "failed",
                    "stage": stage,
                    "key": current_key,
                    "error_type": type(exc).__name__,
                    "created": created,
                    "reused": reused,
                    "missing": missing,
                }
            )
        except Exception:
            pass
        raise


class MigrationJournal:
    """Exclusive, fsynced JSONL receipt; never overwrites an earlier run."""

    def __init__(self, path: Path) -> None:
        self.path = absolute_path_without_resolution(path)
        self._handle = None

    def __enter__(self):
        root = self.path.parent
        destination = prepare_owned_file_path(
            root,
            self.path,
            create_parents=True,
        )
        self._handle = destination.open("xb")
        return self

    def __exit__(self, *_args) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def write(self, payload: Mapping[str, object]) -> None:
        if self._handle is None:
            raise B2MigrationError("Migration journal is not open")
        encoded = (
            json.dumps(
                dict(payload),
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        self._handle.write(encoded)
        self._handle.flush()
        os.fsync(self._handle.fileno())


def _canonical_hour(value: str) -> datetime:
    try:
        if not value.endswith("Z"):
            raise ValueError
        hour = require_utc_hour(
            "migration hour",
            datetime.fromisoformat(value[:-1] + "+00:00"),
        )
        if hour.isoformat().replace("+00:00", "Z") != value:
            raise ValueError
        return hour
    except TypeError, ValueError:
        raise argparse.ArgumentTypeError(
            "Use an exact canonical UTC hour such as 2026-10-01T12:00:00Z"
        ) from None


def _fraction(value: str) -> Decimal:
    try:
        result = Decimal(value)
    except InvalidOperation, ValueError:
        raise argparse.ArgumentTypeError("Depth must be an exact Decimal") from None
    if not result.is_finite():
        raise argparse.ArgumentTypeError("Depth must be finite")
    return result


def _secret(*names: str) -> SecretStr:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return SecretStr(value)
    raise B2MigrationError("A required migration process secret is not configured")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate verified processed HF/local artifact pairs to B2."
    )
    parser.add_argument("--source", choices=("hf", "local"), default="hf")
    parser.add_argument(
        "--chain",
        choices=("all", *CHAIN_PROFILES),
        default="all",
    )
    parser.add_argument("--from-hour", required=True, type=_canonical_hour)
    parser.add_argument(
        "--until-hour",
        required=True,
        type=_canonical_hour,
        help="Exclusive end UTC hour.",
    )
    parser.add_argument("--depth-lower", required=True, type=_fraction)
    parser.add_argument("--depth-upper", required=True, type=_fraction)
    parser.add_argument("--local-root", type=Path)
    parser.add_argument(
        "--hf-repo-id",
        default=os.environ.get(
            "L2SHOCK_HF_REPO_ID",
            os.environ.get("L2SHOCK__REMOTE__HF_REPO_ID", ""),
        ),
    )
    parser.add_argument("--hf-revision", default="main")
    parser.add_argument(
        "--b2-endpoint-url",
        default=os.environ.get("L2SHOCK__REMOTE__B2__ENDPOINT_URL", ""),
    )
    parser.add_argument(
        "--b2-bucket",
        default=os.environ.get("L2SHOCK__REMOTE__B2__BUCKET", ""),
    )
    parser.add_argument(
        "--b2-single-writer-confirmed",
        action="store_true",
        help="Acknowledges external destination writer exclusion; not a lock.",
    )
    parser.add_argument(
        "--history-only",
        action="store_true",
        help="Do not require terminal seeds; this does not establish chain readiness.",
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=Path("data/migration-reports"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    # Load only process secrets/defaults; do not initialize application/DB settings.
    load_dotenv(Path.cwd() / ".env", override=False)
    args = build_parser().parse_args(argv)

    try:
        if args.b2_single_writer_confirmed is not True:
            raise B2MigrationError(
                "External B2 single-writer ownership must be confirmed"
            )

        chains = tuple(CHAIN_PROFILES) if args.chain == "all" else (args.chain,)
        keys = build_migration_keys(
            chains=chains,
            start_utc=args.from_hour,
            end_utc=args.until_hour,
            lower_fraction=args.depth_lower,
            upper_fraction=args.depth_upper,
        )

        if args.source == "local":
            if args.local_root is None:
                raise B2MigrationError("Local source requires --local-root")
            source = LocalMigrationSource(args.local_root)
        else:
            if args.local_root is not None:
                raise B2MigrationError("--local-root is only valid with --source local")
            if not args.hf_repo_id:
                raise B2MigrationError("HF source requires --hf-repo-id")
            source = PinnedHuggingFaceMigrationSource(
                HuggingFaceDatasetRepository(
                    repo_id=args.hf_repo_id,
                    revision=args.hf_revision,
                    token=_secret("HF_TOKEN", "L2SHOCK__REMOTE__HF_TOKEN"),
                )
            )

        settings = B2Config(
            endpoint_url=args.b2_endpoint_url,
            bucket=args.b2_bucket,
            key_id=_secret("L2SHOCK__REMOTE__B2__KEY_ID"),
            application_key=_secret("L2SHOCK__REMOTE__B2__APPLICATION_KEY"),
        )
        if not settings.configured:
            raise B2MigrationError("B2 migration configuration is incomplete")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        report_path = args.report_root / (
            f"b2-migration-{stamp}-{uuid.uuid4().hex}.jsonl"
        )

        with MigrationJournal(report_path) as journal:
            with B2ObjectStore(settings) as store:
                counts = migrate_artifacts(
                    source,
                    B2ProcessedArtifactRepository(store),
                    keys,
                    single_writer_confirmed=True,
                    record=journal.write,
                    require_terminal_checkpoints=not args.history_only,
                )

        print(f"Migration receipt: {report_path}")
        print(
            f"planned={counts.planned} created={counts.created} "
            f"reused={counts.reused} missing={counts.missing}"
        )
        return 0 if counts.complete else 3

    except KeyboardInterrupt:
        print("Migration interrupted; rerun the same pinned plan.", file=sys.stderr)
        return 130
    except Exception as exc:
        # HF SDK exceptions may contain private request details in their chains.
        print(
            f"Migration failed: {type(exc).__name__}. "
            "No cutover readiness is established.",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

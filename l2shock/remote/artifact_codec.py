# l2shock/remote/artifact_codec.py
"""Strict Parquet transport for immutable processed-hour artifacts.

This module performs no acquisition, replay, sampling, liquidity calculation,
PostgreSQL persistence, Hugging Face networking, or GitHub Actions scheduling.

It transports the already encoded authoritative analytical blocks produced by
the existing local processing engine.

The transport Parquet contains one row. The canonical manifest is embedded in
that row so the artifact can be validated independently. A future Hugging Face
adapter may additionally publish the same canonical bytes at the manifest path
owned by ``RemoteArtifactKey.manifest_relative_path``.

The manifest's ``content_sha256`` remains the analytical content identity.
The SHA-256 of the complete transport file is operational transport metadata,
not analytical identity.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypeAlias

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.ingest.checkpoint_codec import (
    checkpoint_encoding_info,
    decode_checkpoint,
)
from l2shock.liquidity import (
    HOURLY_BLOCK_CODEC,
    HOURLY_BLOCK_FORMAT_VERSION,
    EncodedHourlyLiquidityBlocks,
    verify_hourly_liquidity_blocks,
)
from l2shock.price import (
    PRICE_BLOCK_CODEC,
    PRICE_BLOCK_FORMAT_VERSION,
    EncodedHourlyTradeOHLCBlocks,
    verify_hourly_trade_ohlc_blocks,
)
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteContractError,
)

REMOTE_ARTIFACT_PARQUET_SCHEMA: Final[str] = "l2shock.remote_processed_artifact_parquet"
REMOTE_ARTIFACT_PARQUET_SCHEMA_VERSION: Final[int] = 1
REMOTE_ARTIFACT_PARQUET_CODEC: Final[str] = "parquet-zstd"

_MAX_TRANSPORT_FILE_BYTES: Final[int] = 512 * 1024 * 1024
_MAX_BINARY_COLUMN_BYTES: Final[int] = 256 * 1024 * 1024
_MAX_TOTAL_UNCOMPRESSED_BYTES: Final[int] = 512 * 1024 * 1024

_SCHEMA_METADATA: Final[dict[bytes, bytes]] = {
    b"l2shock.schema": REMOTE_ARTIFACT_PARQUET_SCHEMA.encode("ascii"),
    b"l2shock.schema_version": str(REMOTE_ARTIFACT_PARQUET_SCHEMA_VERSION).encode(
        "ascii"
    ),
    b"l2shock.codec": REMOTE_ARTIFACT_PARQUET_CODEC.encode("ascii"),
    b"l2shock.row_count": b"1",
}

_COLUMN_NAMES: Final[tuple[str, ...]] = (
    "artifact_kind",
    "manifest_json",
    "l2_bid_liquidity_block",
    "l2_ask_liquidity_block",
    "l2_validity_block",
    "l2_source_count_block",
    "price_ohlc_block",
    "price_validity_block",
    "price_trade_count_block",
    "output_checkpoint",
)


class RemoteArtifactCodecError(RemoteContractError):
    """A processed-hour transport artifact violates its codec contract."""


class RemoteArtifactCorruptionError(RemoteArtifactCodecError):
    """A transport artifact is truncated, modified, or inconsistent."""


class RemoteArtifactLimitError(RemoteArtifactCodecError):
    """A transport artifact exceeds a resource-safety boundary."""


def _transport_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field(
                "artifact_kind",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "manifest_json",
                pa.binary(),
                nullable=False,
            ),
            pa.field(
                "l2_bid_liquidity_block",
                pa.binary(),
                nullable=True,
            ),
            pa.field(
                "l2_ask_liquidity_block",
                pa.binary(),
                nullable=True,
            ),
            pa.field(
                "l2_validity_block",
                pa.binary(),
                nullable=True,
            ),
            pa.field(
                "l2_source_count_block",
                pa.binary(),
                nullable=True,
            ),
            pa.field(
                "price_ohlc_block",
                pa.binary(),
                nullable=True,
            ),
            pa.field(
                "price_validity_block",
                pa.binary(),
                nullable=True,
            ),
            pa.field(
                "price_trade_count_block",
                pa.binary(),
                nullable=True,
            ),
            pa.field(
                "output_checkpoint",
                pa.binary(),
                nullable=True,
            ),
        ],
        metadata=_SCHEMA_METADATA,
    )


def _canonical_sha256(
    field_name: str,
    value: object,
) -> str:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise RemoteArtifactCodecError(
            f"{field_name} must be a canonical lowercase SHA-256"
        )

    return digest


def _sha256_file(
    path: Path,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0

    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)

                if not chunk:
                    break

                digest.update(chunk)
                byte_count += len(chunk)
    except OSError as exc:
        raise RemoteArtifactCodecError(
            "Could not hash remote transport artifact"
        ) from exc

    if byte_count <= 0:
        raise RemoteArtifactCorruptionError("Remote transport artifact is empty")

    return digest.hexdigest(), byte_count


def _checked_optional_bytes(
    field_name: str,
    value: object,
) -> bytes | None:
    if value is None:
        return None

    if not isinstance(value, bytes):
        raise RemoteArtifactCodecError(f"{field_name} must be bytes or null")

    if not value:
        raise RemoteArtifactCodecError(f"{field_name} cannot be empty")

    if len(value) > _MAX_BINARY_COLUMN_BYTES:
        raise RemoteArtifactLimitError(f"{field_name} exceeds the binary-column limit")

    return value


def _preflight_parquet_resource_limits(
    parquet: pq.ParquetFile,
) -> None:
    """Reject excessive declared decompressed payload before table decoding."""

    metadata = parquet.metadata

    if metadata.num_row_groups != 1:
        raise RemoteArtifactCorruptionError(
            "Remote transport artifact must contain exactly one row group"
        )

    row_group = metadata.row_group(0)
    total_uncompressed = 0

    if row_group.num_columns != len(_COLUMN_NAMES):
        raise RemoteArtifactCodecError(
            "Remote transport Parquet column metadata is unsupported"
        )

    for index, column_name in enumerate(_COLUMN_NAMES):
        column = row_group.column(index)
        uncompressed = int(column.total_uncompressed_size)

        if uncompressed < 0:
            raise RemoteArtifactCorruptionError(
                "Remote transport Parquet contains a negative "
                "uncompressed-size declaration"
            )

        total_uncompressed += uncompressed

        if uncompressed > _MAX_BINARY_COLUMN_BYTES:
            raise RemoteArtifactLimitError(
                f"{column_name} exceeds the declared uncompressed " "column-size limit"
            )

    if total_uncompressed > _MAX_TOTAL_UNCOMPRESSED_BYTES:
        raise RemoteArtifactLimitError(
            "Remote transport artifact exceeds the total declared "
            "uncompressed-size limit"
        )


@dataclass(frozen=True, slots=True)
class RemoteL2ProcessedArtifact:
    """One verified L2 analytical block set and optional output checkpoint."""

    manifest: RemoteArtifactManifest
    encoded: EncodedHourlyLiquidityBlocks
    output_checkpoint: bytes | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, RemoteArtifactManifest):
            raise TypeError("manifest must be RemoteArtifactManifest")

        if self.manifest.key.kind is not RemoteArtifactKind.L2:
            raise RemoteArtifactCodecError(
                "RemoteL2ProcessedArtifact requires an L2 manifest"
            )

        if not isinstance(
            self.encoded,
            EncodedHourlyLiquidityBlocks,
        ):
            raise TypeError("encoded must be EncodedHourlyLiquidityBlocks")

        try:
            verify_hourly_liquidity_blocks(self.encoded)
        except Exception as exc:
            raise RemoteArtifactCorruptionError(
                "L2 analytical blocks failed verification"
            ) from exc

        if self.encoded.content_sha256 != self.manifest.content_sha256:
            raise RemoteArtifactCorruptionError(
                "L2 analytical content SHA-256 does not match the manifest"
            )

        checkpoint_bytes = _checked_optional_bytes(
            "output_checkpoint",
            self.output_checkpoint,
        )
        expected_checkpoint_hash = self.manifest.output_checkpoint_content_sha256

        if expected_checkpoint_hash is None:
            if checkpoint_bytes is not None:
                raise RemoteArtifactCodecError(
                    "An L2 artifact cannot contain output checkpoint bytes "
                    "when its manifest has no output checkpoint identity"
                )
        else:
            if checkpoint_bytes is None:
                raise RemoteArtifactCorruptionError(
                    "The L2 manifest requires an output checkpoint, but "
                    "the transport artifact does not contain it"
                )

            try:
                checkpoint = decode_checkpoint(checkpoint_bytes)
                checkpoint_info = checkpoint_encoding_info(checkpoint_bytes)
            except Exception as exc:
                raise RemoteArtifactCorruptionError(
                    "Output checkpoint failed strict decoding"
                ) from exc

            if checkpoint_info.content_sha256 != expected_checkpoint_hash:
                raise RemoteArtifactCorruptionError(
                    "Output checkpoint SHA-256 does not match the manifest"
                )

            key = self.manifest.key

            if (
                checkpoint.provider != key.provider
                or checkpoint.venue != key.venue
                or checkpoint.symbol != key.instrument
                or checkpoint.through_hour_utc != key.hour_utc
            ):
                raise RemoteArtifactCorruptionError(
                    "Output checkpoint identity does not match " "the processed L2 hour"
                )

            current_source = next(
                (
                    source
                    for source in self.manifest.source_hours
                    if source.hour_utc == key.hour_utc
                ),
                None,
            )

            if current_source is None:
                raise RemoteArtifactCorruptionError(
                    "L2 manifest lacks its current source-hour reference"
                )

            if checkpoint.source_content_sha256 != current_source.content_sha256:
                raise RemoteArtifactCorruptionError(
                    "Output checkpoint source SHA-256 does not match "
                    "the current manifest source archive"
                )

        object.__setattr__(
            self,
            "output_checkpoint",
            checkpoint_bytes,
        )


@dataclass(frozen=True, slots=True)
class RemotePriceProcessedArtifact:
    """One verified real-trade OHLC analytical block set."""

    manifest: RemoteArtifactManifest
    encoded: EncodedHourlyTradeOHLCBlocks

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, RemoteArtifactManifest):
            raise TypeError("manifest must be RemoteArtifactManifest")

        if self.manifest.key.kind is not RemoteArtifactKind.PRICE:
            raise RemoteArtifactCodecError(
                "RemotePriceProcessedArtifact requires a price manifest"
            )

        if not isinstance(
            self.encoded,
            EncodedHourlyTradeOHLCBlocks,
        ):
            raise TypeError("encoded must be EncodedHourlyTradeOHLCBlocks")

        try:
            verify_hourly_trade_ohlc_blocks(self.encoded)
        except Exception as exc:
            raise RemoteArtifactCorruptionError(
                "Price analytical blocks failed verification"
            ) from exc

        if self.encoded.content_sha256 != self.manifest.content_sha256:
            raise RemoteArtifactCorruptionError(
                "Price analytical content SHA-256 does not match " "the manifest"
            )


RemoteProcessedArtifact: TypeAlias = (
    RemoteL2ProcessedArtifact | RemotePriceProcessedArtifact
)


@dataclass(frozen=True, slots=True)
class RemoteArtifactFileInfo:
    """Deterministic facts about one transport Parquet file."""

    path: Path
    file_size_bytes: int
    transport_sha256: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        path = Path(self.path).expanduser().resolve()

        if (
            isinstance(self.file_size_bytes, bool)
            or not isinstance(self.file_size_bytes, int)
            or self.file_size_bytes <= 0
        ):
            raise RemoteArtifactCodecError("file_size_bytes must be a positive integer")

        object.__setattr__(
            self,
            "path",
            path,
        )
        object.__setattr__(
            self,
            "transport_sha256",
            _canonical_sha256(
                "transport_sha256",
                self.transport_sha256,
            ),
        )
        object.__setattr__(
            self,
            "manifest_sha256",
            _canonical_sha256(
                "manifest_sha256",
                self.manifest_sha256,
            ),
        )


def _artifact_row(
    artifact: RemoteProcessedArtifact,
) -> dict[str, object]:
    if isinstance(artifact, RemoteL2ProcessedArtifact):
        return {
            "artifact_kind": RemoteArtifactKind.L2.value,
            "manifest_json": artifact.manifest.canonical_json_bytes,
            "l2_bid_liquidity_block": (artifact.encoded.bid_liquidity_block),
            "l2_ask_liquidity_block": (artifact.encoded.ask_liquidity_block),
            "l2_validity_block": artifact.encoded.validity_block,
            "l2_source_count_block": (artifact.encoded.source_count_block),
            "price_ohlc_block": None,
            "price_validity_block": None,
            "price_trade_count_block": None,
            "output_checkpoint": artifact.output_checkpoint,
        }

    if isinstance(artifact, RemotePriceProcessedArtifact):
        return {
            "artifact_kind": RemoteArtifactKind.PRICE.value,
            "manifest_json": artifact.manifest.canonical_json_bytes,
            "l2_bid_liquidity_block": None,
            "l2_ask_liquidity_block": None,
            "l2_validity_block": None,
            "l2_source_count_block": None,
            "price_ohlc_block": artifact.encoded.ohlc_block,
            "price_validity_block": artifact.encoded.validity_block,
            "price_trade_count_block": (artifact.encoded.trade_count_block),
            "output_checkpoint": None,
        }

    raise TypeError(
        "artifact must be RemoteL2ProcessedArtifact or " "RemotePriceProcessedArtifact"
    )


def _artifact_table(
    artifact: RemoteProcessedArtifact,
) -> pa.Table:
    row = _artifact_row(artifact)
    schema = _transport_schema()

    arrays = [
        pa.array(
            [row[field.name]],
            type=field.type,
        )
        for field in schema
    ]

    return pa.Table.from_arrays(
        arrays,
        schema=schema,
    )


def write_remote_artifact_file(
    path: Path,
    artifact: RemoteProcessedArtifact,
    *,
    overwrite: bool = False,
) -> RemoteArtifactFileInfo:
    """Atomically write one verified processed-hour transport Parquet."""

    if not isinstance(overwrite, bool):
        raise TypeError("overwrite must be bool")

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if destination.exists() and not overwrite:
        raise FileExistsError(f"Remote artifact already exists: {destination}")

    table = _artifact_table(artifact)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")

    try:
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=False,
            write_statistics=False,
            row_group_size=1,
            version="2.6",
        )

        size = int(temporary.stat().st_size)

        if size <= 0:
            raise RemoteArtifactCorruptionError(
                "Written remote transport artifact is empty"
            )

        if size > _MAX_TRANSPORT_FILE_BYTES:
            raise RemoteArtifactLimitError(
                "Written remote transport artifact exceeds " "the file-size limit"
            )

        # On Windows, os.fsync requires the file handle to have write access.
        # Opening in "r+b" mode ensures the underlying FlushFileBuffers succeeds.
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())

        if overwrite:
            os.replace(
                temporary,
                destination,
            )
        else:
            try:
                os.link(
                    temporary,
                    destination,
                )
            except FileExistsError as exc:
                raise FileExistsError(
                    f"Remote artifact already exists: {destination}"
                ) from exc
            except OSError as exc:
                raise RemoteArtifactCodecError(
                    "Could not atomically publish the remote artifact. "
                    "The destination filesystem must support same-filesystem "
                    "hard-link creation."
                ) from exc
            else:
                temporary.unlink()

    except BaseException:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass

        raise

    transport_sha256, file_size = _sha256_file(destination)

    return RemoteArtifactFileInfo(
        path=destination,
        file_size_bytes=file_size,
        transport_sha256=transport_sha256,
        manifest_sha256=artifact.manifest.manifest_sha256,
    )


def _scalar(
    table: pa.Table,
    column_name: str,
) -> object:
    column = table.column(column_name)

    if len(column) != 1:
        raise RemoteArtifactCorruptionError(
            "Remote transport column does not contain exactly one value"
        )

    return column[0].as_py()


def _required_binary(
    table: pa.Table,
    column_name: str,
) -> bytes:
    value = _checked_optional_bytes(
        column_name,
        _scalar(table, column_name),
    )

    if value is None:
        raise RemoteArtifactCorruptionError(
            f"{column_name} cannot be null for this artifact kind"
        )

    return value


def _forbid_present_columns(
    table: pa.Table,
    column_names: tuple[str, ...],
) -> None:
    for column_name in column_names:
        if _scalar(table, column_name) is not None:
            raise RemoteArtifactCorruptionError(
                f"{column_name} must be null for this artifact kind"
            )


def read_remote_artifact_file(
    path: Path,
    *,
    expected_key: RemoteArtifactKey | None = None,
    expected_manifest_sha256: str | None = None,
    external_manifest_bytes: bytes | None = None,
) -> RemoteProcessedArtifact:
    """Read and strictly verify one processed-hour transport Parquet."""

    source = Path(path).expanduser()

    if source.is_symlink():
        raise RemoteArtifactCodecError(
            "Remote transport artifact cannot be a symbolic link"
        )

    source = source.resolve()

    if not source.is_file():
        raise FileNotFoundError(f"Remote transport artifact does not exist: {source}")

    size = int(source.stat().st_size)

    if size <= 0:
        raise RemoteArtifactCorruptionError("Remote transport artifact is empty")

    if size > _MAX_TRANSPORT_FILE_BYTES:
        raise RemoteArtifactLimitError(
            "Remote transport artifact exceeds the file-size limit"
        )

    try:
        parquet = pq.ParquetFile(source)
    except Exception as exc:
        raise RemoteArtifactCorruptionError(
            "Remote transport artifact is not readable Parquet"
        ) from exc

    if parquet.metadata.num_rows != 1:
        raise RemoteArtifactCorruptionError(
            "Remote transport artifact must contain exactly one row"
        )

    if parquet.metadata.num_row_groups != 1:
        raise RemoteArtifactCorruptionError(
            "Remote transport artifact must contain exactly one row group"
        )

    _preflight_parquet_resource_limits(parquet)

    expected_schema = _transport_schema()

    if not parquet.schema_arrow.equals(
        expected_schema,
        check_metadata=True,
    ):
        raise RemoteArtifactCodecError(
            "Remote transport Parquet schema or metadata is unsupported"
        )

    try:
        table = parquet.read(
            columns=list(_COLUMN_NAMES),
            use_threads=False,
        )
    except Exception as exc:
        raise RemoteArtifactCorruptionError(
            "Could not read remote transport Parquet payload"
        ) from exc

    if table.num_rows != 1:
        raise RemoteArtifactCorruptionError(
            "Decoded remote transport artifact must contain one row"
        )

    raw_manifest = _required_binary(
        table,
        "manifest_json",
    )

    try:
        manifest = RemoteArtifactManifest.from_canonical_json_bytes(raw_manifest)
    except Exception as exc:
        raise RemoteArtifactCorruptionError(
            "Embedded remote artifact manifest failed verification"
        ) from exc

    if expected_key is not None:
        if not isinstance(expected_key, RemoteArtifactKey):
            raise TypeError("expected_key must be RemoteArtifactKey or null")

        if manifest.key != expected_key:
            raise RemoteArtifactCorruptionError(
                "Remote artifact key does not match the requested identity"
            )

    if expected_manifest_sha256 is not None:
        expected_digest = _canonical_sha256(
            "expected_manifest_sha256",
            expected_manifest_sha256,
        )

        if manifest.manifest_sha256 != expected_digest:
            raise RemoteArtifactCorruptionError(
                "Remote artifact manifest SHA-256 does not match "
                "the expected identity"
            )

    if external_manifest_bytes is not None:
        try:
            external_manifest = RemoteArtifactManifest.from_canonical_json_bytes(
                external_manifest_bytes
            )
        except Exception as exc:
            raise RemoteArtifactCorruptionError(
                "External remote manifest failed verification"
            ) from exc

        if external_manifest != manifest:
            raise RemoteArtifactCorruptionError(
                "Embedded and external remote manifests disagree"
            )

    raw_kind = _scalar(
        table,
        "artifact_kind",
    )

    if raw_kind != manifest.key.kind.value:
        raise RemoteArtifactCorruptionError(
            "Transport artifact kind does not match its manifest"
        )

    if manifest.key.kind is RemoteArtifactKind.L2:
        _forbid_present_columns(
            table,
            (
                "price_ohlc_block",
                "price_validity_block",
                "price_trade_count_block",
            ),
        )

        encoded = EncodedHourlyLiquidityBlocks(
            format_version=HOURLY_BLOCK_FORMAT_VERSION,
            codec=HOURLY_BLOCK_CODEC,
            observation_count=3_600,
            bid_liquidity_block=_required_binary(
                table,
                "l2_bid_liquidity_block",
            ),
            ask_liquidity_block=_required_binary(
                table,
                "l2_ask_liquidity_block",
            ),
            validity_block=_required_binary(
                table,
                "l2_validity_block",
            ),
            source_count_block=_required_binary(
                table,
                "l2_source_count_block",
            ),
            content_sha256=manifest.content_sha256,
        )

        output_checkpoint = _checked_optional_bytes(
            "output_checkpoint",
            _scalar(
                table,
                "output_checkpoint",
            ),
        )

        return RemoteL2ProcessedArtifact(
            manifest=manifest,
            encoded=encoded,
            output_checkpoint=output_checkpoint,
        )

    _forbid_present_columns(
        table,
        (
            "l2_bid_liquidity_block",
            "l2_ask_liquidity_block",
            "l2_validity_block",
            "l2_source_count_block",
            "output_checkpoint",
        ),
    )

    encoded_price = EncodedHourlyTradeOHLCBlocks(
        format_version=PRICE_BLOCK_FORMAT_VERSION,
        codec=PRICE_BLOCK_CODEC,
        observation_count=3_600,
        ohlc_block=_required_binary(
            table,
            "price_ohlc_block",
        ),
        validity_block=_required_binary(
            table,
            "price_validity_block",
        ),
        trade_count_block=_required_binary(
            table,
            "price_trade_count_block",
        ),
        content_sha256=manifest.content_sha256,
    )

    return RemotePriceProcessedArtifact(
        manifest=manifest,
        encoded=encoded_price,
    )


__all__ = [
    "REMOTE_ARTIFACT_PARQUET_CODEC",
    "REMOTE_ARTIFACT_PARQUET_SCHEMA",
    "REMOTE_ARTIFACT_PARQUET_SCHEMA_VERSION",
    "RemoteArtifactCodecError",
    "RemoteArtifactCorruptionError",
    "RemoteArtifactFileInfo",
    "RemoteArtifactLimitError",
    "RemoteL2ProcessedArtifact",
    "RemotePriceProcessedArtifact",
    "RemoteProcessedArtifact",
    "read_remote_artifact_file",
    "write_remote_artifact_file",
]

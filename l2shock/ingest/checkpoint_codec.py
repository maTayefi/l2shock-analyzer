# l2shock/ingest/checkpoint_codec.py
"""Deterministic binary encoding for immutable order-book checkpoints.

The encoded checkpoint format is intentionally small and auditable:

    fixed binary envelope
        magic
        format version
        canonical payload length
        canonical payload SHA-256

    canonical UTF-8 JSON payload
        checkpoint source identity
        completed source hour
        replay frontier
        exact decimal book levels
        optional source archive SHA-256

The payload uses JSON only as a deterministic internal representation. The
checkpoint file itself is a versioned binary artifact and must be read through
this module rather than treated as a general-purpose JSON document.

Important boundaries:

- checkpoint serialization does not prove that a checkpoint was produced from
  trustworthy source data;
- the replay layer remains responsible for checkpoint source/hour ownership;
- decimals are encoded as canonical non-exponent decimal strings;
- decoding is bounded before JSON parsing;
- payload hashes detect corruption but are not signatures or authentication;
- serialized checkpoints must not contain API keys, passwords, or unrelated
  application configuration.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

from l2shock.ingest.parquet_reader import BookSide
from l2shock.ingest.replay import (
    CheckpointLevel,
    OrderBookCheckpoint,
    ReplayContractError,
)

CHECKPOINT_FORMAT_VERSION: Final[int] = 1

# Exactly eight bytes. The explicit trailing NUL distinguishes this binary
# artifact from ordinary text which merely begins with "L2CKPT1".
CHECKPOINT_MAGIC: Final[bytes] = b"L2CKPT1\x00"

# magic, unsigned format version, unsigned payload length, payload SHA-256
_CHECKPOINT_HEADER: Final[struct.Struct] = struct.Struct(">8sHQ32s")

MAX_CHECKPOINT_PAYLOAD_BYTES: Final[int] = 256 * 1024 * 1024
MAX_CHECKPOINT_LEVELS: Final[int] = 2_000_000
MAX_DECIMAL_TEXT_LENGTH: Final[int] = 128
MAX_IDENTITY_TEXT_LENGTH: Final[int] = 128
MAX_UPDATE_ID: Final[int] = (2**63) - 1
MAX_ORDER_COUNT: Final[int] = (2**63) - 1

_PAYLOAD_SCHEMA: Final[str] = "l2shock.orderbook_checkpoint"

_PAYLOAD_KEYS: Final[frozenset[str]] = frozenset(
    {
        "schema",
        "version",
        "provider",
        "venue",
        "symbol",
        "through_hour_utc",
        "last_update_id",
        "source_content_sha256",
        "level_count",
        "levels",
    }
)


class CheckpointCodecError(ValueError):
    """A checkpoint cannot be encoded or decoded safely."""


class CheckpointCorruptionError(CheckpointCodecError):
    """Encoded bytes are truncated, modified, or internally inconsistent."""


class CheckpointLimitError(CheckpointCodecError):
    """A checkpoint exceeds a configured resource-safety bound."""


@dataclass(frozen=True, slots=True)
class CheckpointEncodingInfo:
    """Deterministic facts about one encoded checkpoint artifact."""

    format_version: int
    payload_size_bytes: int
    encoded_size_bytes: int
    payload_sha256: str
    content_sha256: str
    level_count: int
    bid_level_count: int
    ask_level_count: int

    def __post_init__(self) -> None:
        for name in (
            "format_version",
            "payload_size_bytes",
            "encoded_size_bytes",
            "level_count",
            "bid_level_count",
            "ask_level_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(
                    f"CheckpointEncodingInfo.{name} must be a " "non-negative integer"
                )

        for name in ("payload_sha256", "content_sha256"):
            value = str(getattr(self, name) or "").strip().lower()

            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValueError(f"CheckpointEncodingInfo.{name} must be a SHA-256")

            object.__setattr__(self, name, value)

        if self.bid_level_count + self.ask_level_count != self.level_count:
            raise ValueError("Checkpoint level counts are internally inconsistent")


def _bounded_identity(name: str, value: object) -> str:
    text = str(value or "").strip()

    if not text:
        raise CheckpointCodecError(f"Checkpoint {name} cannot be blank")

    if len(text.encode("utf-8")) > MAX_IDENTITY_TEXT_LENGTH:
        raise CheckpointLimitError(
            f"Checkpoint {name} exceeds " f"{MAX_IDENTITY_TEXT_LENGTH} UTF-8 bytes"
        )

    return text


def _canonical_decimal_text(
    value: Decimal,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, Decimal):
        raise CheckpointCodecError(f"{field_name} must be an exact Decimal")

    if not value.is_finite() or value <= 0:
        raise CheckpointCodecError(f"{field_name} must be finite and positive")

    try:
        text = format(value, "f")
    except (ValueError, OverflowError) as exc:
        raise CheckpointCodecError(
            f"{field_name} cannot be represented canonically"
        ) from exc

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if text in {"", "-0", "+0"}:
        text = "0"

    if len(text) > MAX_DECIMAL_TEXT_LENGTH:
        raise CheckpointLimitError(
            f"{field_name} exceeds the maximum canonical decimal "
            f"length of {MAX_DECIMAL_TEXT_LENGTH}"
        )

    if "e" in text.lower():
        raise CheckpointCodecError(
            f"{field_name} canonical text must not use exponent notation"
        )

    return text


def _parse_canonical_decimal(
    value: object,
    *,
    field_name: str,
) -> Decimal:
    if not isinstance(value, str):
        raise CheckpointCodecError(f"{field_name} must be encoded as a decimal string")

    if not value or len(value) > MAX_DECIMAL_TEXT_LENGTH:
        raise CheckpointLimitError(f"{field_name} has an invalid encoded length")

    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise CheckpointCodecError(
            f"{field_name} is not a valid decimal string"
        ) from exc

    canonical = _canonical_decimal_text(
        parsed,
        field_name=field_name,
    )

    if value != canonical:
        raise CheckpointCodecError(f"{field_name} is not in canonical decimal form")

    return parsed


def _canonical_hour_text(value: datetime) -> str:
    if not isinstance(value, datetime):
        raise CheckpointCodecError("through_hour_utc must be a datetime")

    if value.tzinfo is None or value.utcoffset() is None:
        raise CheckpointCodecError("through_hour_utc must be timezone-aware UTC")

    if value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
        raise CheckpointCodecError("through_hour_utc must have UTC offset +00:00")

    if value.minute or value.second or value.microsecond:
        raise CheckpointCodecError(
            "through_hour_utc must be aligned to an exact UTC hour"
        )

    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_hour_text(value: object) -> datetime:
    if not isinstance(value, str):
        raise CheckpointCodecError("through_hour_utc must be encoded as text")

    try:
        parsed = datetime.strptime(
            value,
            "%Y-%m-%dT%H:%M:%SZ",
        ).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise CheckpointCodecError(
            "through_hour_utc must use canonical " "YYYY-MM-DDTHH:00:00Z form"
        ) from exc

    if value != _canonical_hour_text(parsed):
        raise CheckpointCodecError("through_hour_utc is not canonically encoded")

    return parsed


def _bounded_nonnegative_integer(
    name: str,
    value: object,
    *,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CheckpointCodecError(f"{name} must be an integer")

    if value < 0 or value > maximum:
        raise CheckpointLimitError(f"{name} must be inside [0, {maximum}]")

    return value


def _validated_source_digest(value: object) -> str | None:
    if value is None:
        return None

    if not isinstance(value, str):
        raise CheckpointCodecError("source_content_sha256 must be null or a string")

    digest = value.strip()

    if len(digest) != 64 or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise CheckpointCodecError("source_content_sha256 must be a lowercase SHA-256")

    return digest


def _canonical_levels(
    checkpoint: OrderBookCheckpoint,
) -> tuple[CheckpointLevel, ...]:
    levels = tuple(checkpoint.levels)

    if len(levels) > MAX_CHECKPOINT_LEVELS:
        raise CheckpointLimitError(
            "Checkpoint contains more than " f"{MAX_CHECKPOINT_LEVELS} levels"
        )

    bids = sorted(
        (level for level in levels if level.side is BookSide.BID),
        key=lambda level: level.price,
        reverse=True,
    )
    asks = sorted(
        (level for level in levels if level.side is BookSide.ASK),
        key=lambda level: level.price,
    )

    if len(bids) + len(asks) != len(levels):
        raise CheckpointCodecError("Checkpoint contains an unsupported level side")

    return tuple((*bids, *asks))


def _payload_object(
    checkpoint: OrderBookCheckpoint,
) -> dict[str, Any]:
    if not isinstance(checkpoint, OrderBookCheckpoint):
        raise TypeError("checkpoint must be an OrderBookCheckpoint")

    provider = _bounded_identity(
        "provider",
        checkpoint.provider,
    ).lower()
    venue = _bounded_identity(
        "venue",
        checkpoint.venue,
    ).lower()
    symbol = _bounded_identity(
        "symbol",
        checkpoint.symbol,
    ).upper()

    last_update_id = _bounded_nonnegative_integer(
        "last_update_id",
        checkpoint.last_update_id,
        maximum=MAX_UPDATE_ID,
    )

    levels = _canonical_levels(checkpoint)

    encoded_levels: list[list[object]] = []

    for index, level in enumerate(levels):
        order_count = level.order_count

        if order_count is not None:
            order_count = _bounded_nonnegative_integer(
                f"levels[{index}].order_count",
                order_count,
                maximum=MAX_ORDER_COUNT,
            )

        encoded_levels.append(
            [
                level.side.value,
                _canonical_decimal_text(
                    level.price,
                    field_name=f"levels[{index}].price",
                ),
                _canonical_decimal_text(
                    level.quantity,
                    field_name=f"levels[{index}].quantity",
                ),
                order_count,
            ]
        )

    return {
        "schema": _PAYLOAD_SCHEMA,
        "version": CHECKPOINT_FORMAT_VERSION,
        "provider": provider,
        "venue": venue,
        "symbol": symbol,
        "through_hour_utc": _canonical_hour_text(checkpoint.through_hour_utc),
        "last_update_id": last_update_id,
        "source_content_sha256": _validated_source_digest(
            checkpoint.source_content_sha256
        ),
        "level_count": len(encoded_levels),
        "levels": encoded_levels,
    }


def _canonical_payload_bytes(
    checkpoint: OrderBookCheckpoint,
) -> bytes:
    payload = json.dumps(
        _payload_object(checkpoint),
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    if len(payload) > MAX_CHECKPOINT_PAYLOAD_BYTES:
        raise CheckpointLimitError(
            "Encoded checkpoint payload exceeds "
            f"{MAX_CHECKPOINT_PAYLOAD_BYTES} bytes"
        )

    return payload


def _reject_float(_value: str) -> None:
    raise CheckpointCodecError(
        "Checkpoint JSON must not contain floating-point numbers"
    )


def _reject_nonfinite_constant(value: str) -> None:
    raise CheckpointCodecError(
        f"Checkpoint JSON contains unsupported constant {value!r}"
    )


def _unique_json_object(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key, value in pairs:
        if key in result:
            raise CheckpointCodecError(
                f"Checkpoint JSON contains duplicate key {key!r}"
            )

        result[key] = value

    return result


def _decode_payload_object(
    payload_bytes: bytes,
) -> dict[str, Any]:
    try:
        payload_text = payload_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CheckpointCorruptionError(
            "Checkpoint payload is not valid UTF-8"
        ) from exc

    try:
        decoded = json.loads(
            payload_text,
            parse_float=_reject_float,
            parse_constant=_reject_nonfinite_constant,
            object_pairs_hook=_unique_json_object,
        )
    except CheckpointCodecError:
        raise
    except (json.JSONDecodeError, UnicodeError, RecursionError) as exc:
        raise CheckpointCorruptionError(
            "Checkpoint payload is not valid bounded JSON"
        ) from exc

    if not isinstance(decoded, dict):
        raise CheckpointCodecError("Checkpoint payload root must be an object")

    actual_keys = frozenset(decoded)

    if actual_keys != _PAYLOAD_KEYS:
        missing = sorted(_PAYLOAD_KEYS - actual_keys)
        unexpected = sorted(actual_keys - _PAYLOAD_KEYS)

        raise CheckpointCodecError(
            "Checkpoint payload fields do not match the format contract: "
            f"missing={missing}, unexpected={unexpected}"
        )

    return decoded


def _checkpoint_from_payload(
    payload: dict[str, Any],
) -> OrderBookCheckpoint:
    if payload["schema"] != _PAYLOAD_SCHEMA:
        raise CheckpointCodecError("Checkpoint payload schema identity is unsupported")

    version = _bounded_nonnegative_integer(
        "version",
        payload["version"],
        maximum=65_535,
    )

    if version != CHECKPOINT_FORMAT_VERSION:
        raise CheckpointCodecError(
            "Checkpoint payload version does not match the envelope"
        )

    provider = _bounded_identity(
        "provider",
        payload["provider"],
    )

    if provider != provider.lower():
        raise CheckpointCodecError("Checkpoint provider must be lowercase")

    venue = _bounded_identity(
        "venue",
        payload["venue"],
    )

    if venue != venue.lower():
        raise CheckpointCodecError("Checkpoint venue must be lowercase")

    symbol = _bounded_identity(
        "symbol",
        payload["symbol"],
    )

    if symbol != symbol.upper():
        raise CheckpointCodecError("Checkpoint symbol must be uppercase")

    last_update_id = _bounded_nonnegative_integer(
        "last_update_id",
        payload["last_update_id"],
        maximum=MAX_UPDATE_ID,
    )

    level_count = _bounded_nonnegative_integer(
        "level_count",
        payload["level_count"],
        maximum=MAX_CHECKPOINT_LEVELS,
    )

    raw_levels = payload["levels"]

    if not isinstance(raw_levels, list):
        raise CheckpointCodecError("Checkpoint levels must be an array")

    if len(raw_levels) != level_count:
        raise CheckpointCorruptionError(
            "Checkpoint level_count does not match the encoded level array"
        )

    levels: list[CheckpointLevel] = []

    for index, raw_level in enumerate(raw_levels):
        if not isinstance(raw_level, list) or len(raw_level) != 4:
            raise CheckpointCodecError(f"levels[{index}] must be a four-item array")

        raw_side, raw_price, raw_quantity, raw_order_count = raw_level

        if raw_side not in {"bid", "ask"}:
            raise CheckpointCodecError(f"levels[{index}].side is unsupported")

        order_count: int | None

        if raw_order_count is None:
            order_count = None
        else:
            order_count = _bounded_nonnegative_integer(
                f"levels[{index}].order_count",
                raw_order_count,
                maximum=MAX_ORDER_COUNT,
            )

        levels.append(
            CheckpointLevel(
                side=BookSide(raw_side),
                price=_parse_canonical_decimal(
                    raw_price,
                    field_name=f"levels[{index}].price",
                ),
                quantity=_parse_canonical_decimal(
                    raw_quantity,
                    field_name=f"levels[{index}].quantity",
                ),
                order_count=order_count,
            )
        )

    try:
        checkpoint = OrderBookCheckpoint(
            provider=provider,
            venue=venue,
            symbol=symbol,
            through_hour_utc=_parse_hour_text(payload["through_hour_utc"]),
            last_update_id=last_update_id,
            levels=tuple(levels),
            source_content_sha256=_validated_source_digest(
                payload["source_content_sha256"]
            ),
        )
    except ReplayContractError as exc:
        raise CheckpointCodecError(
            f"Decoded checkpoint violates replay contracts: {exc}"
        ) from exc

    return checkpoint


def encode_checkpoint(
    checkpoint: OrderBookCheckpoint,
) -> bytes:
    """Encode one checkpoint into the deterministic versioned binary format."""
    payload = _canonical_payload_bytes(checkpoint)
    payload_digest = hashlib.sha256(payload).digest()

    header = _CHECKPOINT_HEADER.pack(
        CHECKPOINT_MAGIC,
        CHECKPOINT_FORMAT_VERSION,
        len(payload),
        payload_digest,
    )

    return header + payload


def decode_checkpoint(
    encoded: bytes | bytearray | memoryview,
) -> OrderBookCheckpoint:
    """Decode and strictly validate one binary checkpoint artifact."""
    if not isinstance(encoded, (bytes, bytearray, memoryview)):
        raise TypeError("encoded checkpoint must be bytes-like")

    data = bytes(encoded)

    if len(data) < _CHECKPOINT_HEADER.size:
        raise CheckpointCorruptionError(
            "Checkpoint is shorter than its fixed binary header"
        )

    magic, version, payload_length, expected_payload_digest = (
        _CHECKPOINT_HEADER.unpack_from(data)
    )

    if magic != CHECKPOINT_MAGIC:
        raise CheckpointCorruptionError("Checkpoint binary magic is invalid")

    if version != CHECKPOINT_FORMAT_VERSION:
        raise CheckpointCodecError(f"Unsupported checkpoint envelope version {version}")

    if payload_length > MAX_CHECKPOINT_PAYLOAD_BYTES:
        raise CheckpointLimitError(
            "Checkpoint declares a payload larger than "
            f"{MAX_CHECKPOINT_PAYLOAD_BYTES} bytes"
        )

    expected_total = _CHECKPOINT_HEADER.size + payload_length

    if len(data) != expected_total:
        raise CheckpointCorruptionError(
            "Checkpoint encoded length does not match its envelope: "
            f"actual={len(data)}, expected={expected_total}"
        )

    payload = data[_CHECKPOINT_HEADER.size :]

    actual_payload_digest = hashlib.sha256(payload).digest()

    if actual_payload_digest != expected_payload_digest:
        raise CheckpointCorruptionError(
            "Checkpoint payload SHA-256 does not match its envelope"
        )

    payload_object = _decode_payload_object(payload)
    checkpoint = _checkpoint_from_payload(payload_object)

    # The payload must be not merely valid, but canonical. This rejects
    # alternate key ordering, insignificant whitespace, noncanonical decimal
    # forms, unsorted levels, and other multiple encodings of the same state.
    canonical_payload = _canonical_payload_bytes(checkpoint)

    if payload != canonical_payload:
        raise CheckpointCodecError(
            "Checkpoint payload is valid but not canonically encoded"
        )

    return checkpoint


def checkpoint_content_sha256(
    encoded: bytes | bytearray | memoryview,
) -> str:
    """Return SHA-256 for the complete encoded checkpoint artifact."""
    if not isinstance(encoded, (bytes, bytearray, memoryview)):
        raise TypeError("encoded checkpoint must be bytes-like")

    return hashlib.sha256(bytes(encoded)).hexdigest()


def checkpoint_encoding_info(
    encoded: bytes | bytearray | memoryview,
) -> CheckpointEncodingInfo:
    """Validate encoded bytes and return deterministic artifact facts."""
    data = bytes(encoded)
    checkpoint = decode_checkpoint(data)

    _magic, version, payload_length, payload_digest = _CHECKPOINT_HEADER.unpack_from(
        data
    )

    return CheckpointEncodingInfo(
        format_version=version,
        payload_size_bytes=payload_length,
        encoded_size_bytes=len(data),
        payload_sha256=payload_digest.hex(),
        content_sha256=checkpoint_content_sha256(data),
        level_count=len(checkpoint.levels),
        bid_level_count=checkpoint.bid_level_count,
        ask_level_count=checkpoint.ask_level_count,
    )


def load_checkpoint_file(
    path: Path,
) -> OrderBookCheckpoint:
    """Read and decode one bounded checkpoint file."""
    source = Path(path).expanduser().resolve()

    if not source.is_file():
        raise CheckpointCodecError(f"Checkpoint path is not a regular file: {source}")

    try:
        file_size = source.stat().st_size
    except OSError as exc:
        raise CheckpointCodecError(
            f"Could not inspect checkpoint file {source.name!r}"
        ) from exc

    maximum_file_size = _CHECKPOINT_HEADER.size + MAX_CHECKPOINT_PAYLOAD_BYTES

    if file_size > maximum_file_size:
        raise CheckpointLimitError(
            "Checkpoint file exceeds the maximum supported size of "
            f"{maximum_file_size} bytes"
        )

    try:
        encoded = source.read_bytes()
    except OSError as exc:
        raise CheckpointCodecError(
            f"Could not read checkpoint file {source.name!r}"
        ) from exc

    return decode_checkpoint(encoded)


def write_checkpoint_file(
    path: Path,
    checkpoint: OrderBookCheckpoint,
    *,
    overwrite: bool = False,
) -> CheckpointEncodingInfo:
    """Atomically publish one deterministic checkpoint file.

    By default an existing destination is never replaced. When ``overwrite`` is
    true, ``os.replace`` atomically installs the new complete artifact.

    The temporary file is created in the destination directory so publication
    remains on one filesystem.
    """
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and not destination.is_file():
        raise CheckpointCodecError(
            "Checkpoint destination exists but is not a regular file"
        )

    encoded = encode_checkpoint(checkpoint)
    info = checkpoint_encoding_info(encoded)

    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")

    try:
        try:
            with temporary.open("xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as exc:
            raise CheckpointCodecError(
                "A supposedly unique checkpoint temporary file already exists"
            ) from exc
        except OSError as exc:
            raise CheckpointCodecError(
                "Could not write checkpoint temporary file"
            ) from exc

        if overwrite:
            try:
                os.replace(temporary, destination)
            except OSError as exc:
                raise CheckpointCodecError(
                    "Could not atomically replace checkpoint destination"
                ) from exc
        else:
            try:
                os.link(temporary, destination)
            except FileExistsError as exc:
                raise FileExistsError(
                    f"Checkpoint destination already exists: {destination}"
                ) from exc
            except OSError as exc:
                raise CheckpointCodecError(
                    "Could not atomically publish checkpoint. The destination "
                    "filesystem must support same-filesystem hard links."
                ) from exc
            else:
                try:
                    temporary.unlink()
                except OSError:
                    # The canonical destination is already a complete hard link
                    # to the fsynced temporary file. Preserve that valid
                    # publication. The outer finally block retries removal of
                    # the redundant temporary directory entry.
                    pass

    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass

    # Read through the same public decoder after publication. This catches a
    # filesystem or implementation failure before the caller treats the file as
    # durable replay state.
    published = load_checkpoint_file(destination)
    published_encoded = encode_checkpoint(published)

    if checkpoint_content_sha256(published_encoded) != info.content_sha256:
        raise CheckpointCorruptionError(
            "Published checkpoint does not match the encoded source state"
        )

    return info


__all__ = [
    "CHECKPOINT_FORMAT_VERSION",
    "CHECKPOINT_MAGIC",
    "MAX_CHECKPOINT_LEVELS",
    "MAX_CHECKPOINT_PAYLOAD_BYTES",
    "CheckpointCodecError",
    "CheckpointCorruptionError",
    "CheckpointEncodingInfo",
    "CheckpointLimitError",
    "checkpoint_content_sha256",
    "checkpoint_encoding_info",
    "decode_checkpoint",
    "encode_checkpoint",
    "load_checkpoint_file",
    "write_checkpoint_file",
]

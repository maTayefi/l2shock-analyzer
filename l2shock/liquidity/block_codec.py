# l2shock/liquidity/block_codec.py
"""Versioned compact encoding for hourly liquidity channels.

The PostgreSQL schema stores four independent authoritative blocks:

    bid_liquidity_block
    ask_liquidity_block
    validity_block
    source_count_block

Bid and Ask Liquidity remain exact Decimal values. They are encoded as
canonical non-exponent decimal strings inside Zstandard-compressed Arrow IPC
streams. This avoids silently imposing a fixed Arrow decimal scale on values
whose exact source-derived scale can vary.

Total Liquidity and Bid-Ask Imbalance are deliberately not stored as separate
channels. They remain derived from Bid and Ask Liquidity.

Each Arrow IPC stream is wrapped in a small binary envelope containing:

    magic
    block format version
    channel identity
    observation count
    Arrow payload length
    Arrow payload SHA-256

The complete four-channel artifact also receives a deterministic content
SHA-256. Hashes detect accidental corruption; they are not signatures.
"""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from typing import Final

import pyarrow as pa
import pyarrow.ipc as ipc

from l2shock.ingest.sampling import (
    OBSERVATIONS_PER_HOUR,
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.liquidity.hourly import (
    HourlyLiquidityBlock,
    LiquidityQualitySummary,
)

HOURLY_BLOCK_FORMAT_VERSION: Final[int] = 1
HOURLY_BLOCK_CODEC: Final[str] = "arrow-ipc-zstd"
HOURLY_BLOCK_SCHEMA_NAME: Final[str] = "l2shock.hourly_liquidity_channel"

# Exactly eight bytes.
HOURLY_BLOCK_MAGIC: Final[bytes] = b"L2HBLK1\x00"

# magic, version, channel, flags, observation_count, payload_length, SHA-256
_BLOCK_HEADER: Final[struct.Struct] = struct.Struct(">8sHBBIQ32s")

_MAX_BLOCK_PAYLOAD_BYTES: Final[int] = 64 * 1024 * 1024
_MAX_DECIMAL_TEXT_LENGTH: Final[int] = 256

_CONTENT_HASH_DOMAIN: Final[bytes] = b"l2shock/hourly-liquidity-blocks/v1\x00"
_LENGTH_PREFIX: Final[struct.Struct] = struct.Struct(">Q")


class HourlyBlockCodecError(ValueError):
    """An hourly analytical block cannot be encoded or decoded safely."""


class HourlyBlockCorruptionError(HourlyBlockCodecError):
    """An encoded block is truncated, modified, or internally inconsistent."""


class HourlyBlockLimitError(HourlyBlockCodecError):
    """An encoded block exceeds a resource-safety limit."""


class HourlyLiquidityChannel(IntEnum):
    """Stable binary identities for persisted hourly channels."""

    BID_LIQUIDITY = 1
    ASK_LIQUIDITY = 2
    VALIDITY = 3
    SOURCE_COUNT = 4


class HourlyQualityCode(IntEnum):
    """Stable format-version-1 analytical-quality codes."""

    VALID = 1
    DEGRADED = 2
    INVALID = 3


class HourlyInvalidReasonCode(IntEnum):
    """Stable format-version-1 invalid-reason codes.

    Zero is reserved for the absence of an invalid reason and is therefore not
    represented by an enum member.
    """

    UNINITIALIZED = 1
    REPLAY_INVALIDATED = 2
    LOCKED = 3
    CROSSED = 4
    EMPTY_BID = 5
    EMPTY_ASK = 6
    EMPTY_BOTH = 7


_QUALITY_TO_CODE: Final[dict[BookSampleQuality, int]] = {
    BookSampleQuality.VALID: int(HourlyQualityCode.VALID),
    BookSampleQuality.DEGRADED: int(HourlyQualityCode.DEGRADED),
    BookSampleQuality.INVALID: int(HourlyQualityCode.INVALID),
}
_CODE_TO_QUALITY: Final[dict[int, BookSampleQuality]] = {
    code: quality for quality, code in _QUALITY_TO_CODE.items()
}

_REASON_TO_CODE: Final[dict[BookSampleInvalidReason, int]] = {
    BookSampleInvalidReason.UNINITIALIZED: int(HourlyInvalidReasonCode.UNINITIALIZED),
    BookSampleInvalidReason.REPLAY_INVALIDATED: int(
        HourlyInvalidReasonCode.REPLAY_INVALIDATED
    ),
    BookSampleInvalidReason.LOCKED: int(HourlyInvalidReasonCode.LOCKED),
    BookSampleInvalidReason.CROSSED: int(HourlyInvalidReasonCode.CROSSED),
    BookSampleInvalidReason.EMPTY_BID: int(HourlyInvalidReasonCode.EMPTY_BID),
    BookSampleInvalidReason.EMPTY_ASK: int(HourlyInvalidReasonCode.EMPTY_ASK),
    BookSampleInvalidReason.EMPTY_BOTH: int(HourlyInvalidReasonCode.EMPTY_BOTH),
}
_CODE_TO_REASON: Final[dict[int, BookSampleInvalidReason]] = {
    code: reason for reason, code in _REASON_TO_CODE.items()
}

if frozenset(_QUALITY_TO_CODE) != frozenset(BookSampleQuality):
    raise RuntimeError("Hourly format-version-1 quality-code registry is incomplete")

if frozenset(_REASON_TO_CODE) != frozenset(BookSampleInvalidReason):
    raise RuntimeError("Hourly format-version-1 invalid-reason registry is incomplete")

if len(set(_QUALITY_TO_CODE.values())) != len(_QUALITY_TO_CODE):
    raise RuntimeError("Hourly format-version-1 quality codes are not unique")

if len(set(_REASON_TO_CODE.values())) != len(_REASON_TO_CODE):
    raise RuntimeError("Hourly format-version-1 invalid-reason codes are not unique")


def _sha256_text(value: str) -> str:
    normalized = str(value or "").strip()

    if (
        normalized != normalized.lower()
        or len(normalized) != 64
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise HourlyBlockCodecError(
            "SHA-256 text must contain exactly 64 canonical lowercase "
            "hexadecimal characters"
        )

    return normalized


def _canonical_nonnegative_decimal_text(
    value: Decimal,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, Decimal):
        raise HourlyBlockCodecError(f"{field_name} must be an exact Decimal")

    if not value.is_finite() or value < 0:
        raise HourlyBlockCodecError(
            f"{field_name} must be a finite non-negative Decimal"
        )

    try:
        text = format(value, "f")
    except (ValueError, OverflowError) as exc:
        raise HourlyBlockCodecError(
            f"{field_name} cannot be represented canonically"
        ) from exc

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if text in {"", "-0", "+0"}:
        text = "0"

    if "e" in text.lower():
        raise HourlyBlockCodecError(
            f"{field_name} canonical text must not use exponent notation"
        )

    if len(text) > _MAX_DECIMAL_TEXT_LENGTH:
        raise HourlyBlockLimitError(
            f"{field_name} exceeds the maximum encoded decimal length"
        )

    return text


def _parse_canonical_nonnegative_decimal(
    value: object,
    *,
    field_name: str,
) -> Decimal:
    if not isinstance(value, str):
        raise HourlyBlockCodecError(f"{field_name} must be encoded as text")

    if not value or len(value) > _MAX_DECIMAL_TEXT_LENGTH:
        raise HourlyBlockLimitError(f"{field_name} has an invalid encoded length")

    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise HourlyBlockCodecError(f"{field_name} is not a valid decimal") from exc

    canonical = _canonical_nonnegative_decimal_text(
        parsed,
        field_name=field_name,
    )

    if value != canonical:
        raise HourlyBlockCodecError(f"{field_name} is not canonically encoded")

    return parsed


def _schema_metadata(
    channel: HourlyLiquidityChannel,
) -> dict[bytes, bytes]:
    return {
        b"l2shock.schema": HOURLY_BLOCK_SCHEMA_NAME.encode("ascii"),
        b"l2shock.schema_version": str(HOURLY_BLOCK_FORMAT_VERSION).encode("ascii"),
        b"l2shock.codec": HOURLY_BLOCK_CODEC.encode("ascii"),
        b"l2shock.channel": channel.name.lower().encode("ascii"),
        b"l2shock.observation_count": str(OBSERVATIONS_PER_HOUR).encode("ascii"),
        b"l2shock.sampling_interval_ms": b"1000",
    }


def _schema_for_channel(
    channel: HourlyLiquidityChannel,
) -> pa.Schema:
    metadata = _schema_metadata(channel)

    if channel is HourlyLiquidityChannel.BID_LIQUIDITY:
        return pa.schema(
            [
                pa.field(
                    "bid_liquidity",
                    pa.string(),
                    nullable=True,
                )
            ],
            metadata=metadata,
        )

    if channel is HourlyLiquidityChannel.ASK_LIQUIDITY:
        return pa.schema(
            [
                pa.field(
                    "ask_liquidity",
                    pa.string(),
                    nullable=True,
                )
            ],
            metadata=metadata,
        )

    if channel is HourlyLiquidityChannel.VALIDITY:
        return pa.schema(
            [
                pa.field(
                    "quality_code",
                    pa.uint8(),
                    nullable=False,
                ),
                pa.field(
                    "invalid_reason_code",
                    pa.uint8(),
                    nullable=False,
                ),
            ],
            metadata=metadata,
        )

    if channel is HourlyLiquidityChannel.SOURCE_COUNT:
        return pa.schema(
            [
                pa.field(
                    "source_count",
                    pa.uint16(),
                    nullable=False,
                )
            ],
            metadata=metadata,
        )

    raise HourlyBlockCodecError(f"Unsupported hourly-liquidity channel {channel!r}")


def _write_arrow_payload(
    table: pa.Table,
    *,
    channel: HourlyLiquidityChannel,
) -> bytes:
    expected_schema = _schema_for_channel(channel)

    if not table.schema.equals(
        expected_schema,
        check_metadata=True,
    ):
        raise HourlyBlockCodecError(f"Arrow table schema does not match {channel.name}")

    if table.num_rows != OBSERVATIONS_PER_HOUR:
        raise HourlyBlockCodecError(
            "Hourly Arrow table must contain exactly " f"{OBSERVATIONS_PER_HOUR} rows"
        )

    options = ipc.IpcWriteOptions(
        compression="zstd",
        use_legacy_format=False,
    )
    sink = pa.BufferOutputStream()

    try:
        with ipc.new_stream(
            sink,
            expected_schema,
            options=options,
        ) as writer:
            writer.write_table(table)

        payload = sink.getvalue().to_pybytes()
    except Exception as exc:
        raise HourlyBlockCodecError(
            f"Could not encode {channel.name} Arrow IPC payload"
        ) from exc

    if not payload:
        raise HourlyBlockCodecError(f"{channel.name} Arrow IPC payload is empty")

    if len(payload) > _MAX_BLOCK_PAYLOAD_BYTES:
        raise HourlyBlockLimitError(
            f"{channel.name} Arrow IPC payload exceeds "
            f"{_MAX_BLOCK_PAYLOAD_BYTES} bytes"
        )

    return payload


def _wrap_payload(
    payload: bytes,
    *,
    channel: HourlyLiquidityChannel,
) -> bytes:
    if len(payload) > _MAX_BLOCK_PAYLOAD_BYTES:
        raise HourlyBlockLimitError("Arrow payload exceeds the hourly block limit")

    digest = hashlib.sha256(payload).digest()

    header = _BLOCK_HEADER.pack(
        HOURLY_BLOCK_MAGIC,
        HOURLY_BLOCK_FORMAT_VERSION,
        int(channel),
        0,
        OBSERVATIONS_PER_HOUR,
        len(payload),
        digest,
    )

    return header + payload


def _unwrap_payload(
    encoded: bytes | bytearray | memoryview,
    *,
    expected_channel: HourlyLiquidityChannel,
) -> bytes:
    if not isinstance(encoded, (bytes, bytearray, memoryview)):
        raise TypeError("encoded block must be bytes-like")

    data = bytes(encoded)

    if len(data) < _BLOCK_HEADER.size:
        raise HourlyBlockCorruptionError(
            "Hourly block is shorter than its fixed header"
        )

    (
        magic,
        version,
        raw_channel,
        flags,
        observation_count,
        payload_length,
        expected_digest,
    ) = _BLOCK_HEADER.unpack_from(data)

    if magic != HOURLY_BLOCK_MAGIC:
        raise HourlyBlockCorruptionError("Hourly block magic is invalid")

    if version != HOURLY_BLOCK_FORMAT_VERSION:
        raise HourlyBlockCodecError(f"Unsupported hourly block version {version}")

    try:
        channel = HourlyLiquidityChannel(raw_channel)
    except ValueError as exc:
        raise HourlyBlockCodecError(
            f"Unknown hourly block channel {raw_channel}"
        ) from exc

    if channel is not expected_channel:
        raise HourlyBlockCodecError(
            "Hourly block channel does not match its storage destination: "
            f"encoded={channel.name}, expected={expected_channel.name}"
        )

    if flags != 0:
        raise HourlyBlockCodecError(f"Unsupported hourly block flags value {flags}")

    if observation_count != OBSERVATIONS_PER_HOUR:
        raise HourlyBlockCorruptionError("Hourly block observation count is not 3,600")

    if payload_length > _MAX_BLOCK_PAYLOAD_BYTES:
        raise HourlyBlockLimitError("Hourly block declares an oversized Arrow payload")

    expected_total = _BLOCK_HEADER.size + payload_length

    if len(data) != expected_total:
        raise HourlyBlockCorruptionError(
            "Hourly block encoded length does not match its header: "
            f"actual={len(data)}, expected={expected_total}"
        )

    payload = data[_BLOCK_HEADER.size :]
    actual_digest = hashlib.sha256(payload).digest()

    if actual_digest != expected_digest:
        raise HourlyBlockCorruptionError(
            "Hourly block Arrow payload SHA-256 does not match"
        )

    return payload


def _read_arrow_table(
    payload: bytes,
    *,
    channel: HourlyLiquidityChannel,
) -> pa.Table:
    expected_schema = _schema_for_channel(channel)
    source = pa.BufferReader(payload)

    try:
        reader = ipc.open_stream(source)

        if not reader.schema.equals(
            expected_schema,
            check_metadata=True,
        ):
            raise HourlyBlockCodecError(
                f"Decoded {channel.name} schema or metadata is unsupported"
            )

        batches: list[pa.RecordBatch] = []
        row_count = 0

        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break

            row_count += batch.num_rows

            if row_count > OBSERVATIONS_PER_HOUR:
                raise HourlyBlockCorruptionError(
                    f"Decoded {channel.name} row count is not 3,600"
                )

            batches.append(batch)

        if row_count != OBSERVATIONS_PER_HOUR:
            raise HourlyBlockCorruptionError(
                f"Decoded {channel.name} row count is not 3,600"
            )

        table = pa.Table.from_batches(
            batches,
            schema=expected_schema,
        )
    except HourlyBlockCodecError, HourlyBlockCorruptionError:
        raise
    except Exception as exc:
        raise HourlyBlockCorruptionError(
            f"Could not decode {channel.name} Arrow IPC payload"
        ) from exc

    if source.tell() != len(payload):
        raise HourlyBlockCorruptionError(
            f"{channel.name} Arrow IPC payload contains trailing bytes"
        )

    return table


def _encoded_content_sha256(
    *,
    bid_liquidity_block: bytes,
    ask_liquidity_block: bytes,
    validity_block: bytes,
    source_count_block: bytes,
) -> str:
    digest = hashlib.sha256()
    digest.update(_CONTENT_HASH_DOMAIN)

    for block in (
        bid_liquidity_block,
        ask_liquidity_block,
        validity_block,
        source_count_block,
    ):
        digest.update(_LENGTH_PREFIX.pack(len(block)))
        digest.update(block)

    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class EncodedHourlyLiquidityBlocks:
    """Four encoded PostgreSQL channels for one hourly liquidity result."""

    format_version: int
    codec: str
    observation_count: int

    bid_liquidity_block: bytes
    ask_liquidity_block: bytes
    validity_block: bytes
    source_count_block: bytes

    content_sha256: str

    def __post_init__(self) -> None:
        if self.format_version != HOURLY_BLOCK_FORMAT_VERSION:
            raise HourlyBlockCodecError("Encoded block format version is unsupported")

        if self.codec != HOURLY_BLOCK_CODEC:
            raise HourlyBlockCodecError("Encoded block codec identity is unsupported")

        if self.observation_count != OBSERVATIONS_PER_HOUR:
            raise HourlyBlockCodecError(
                "Encoded hourly blocks must contain 3,600 observations"
            )

        for name in (
            "bid_liquidity_block",
            "ask_liquidity_block",
            "validity_block",
            "source_count_block",
        ):
            value = getattr(self, name)

            if not isinstance(value, bytes) or not value:
                raise HourlyBlockCodecError(f"{name} must be non-empty bytes")

        object.__setattr__(
            self,
            "content_sha256",
            _sha256_text(self.content_sha256),
        )

        calculated = _encoded_content_sha256(
            bid_liquidity_block=self.bid_liquidity_block,
            ask_liquidity_block=self.ask_liquidity_block,
            validity_block=self.validity_block,
            source_count_block=self.source_count_block,
        )

        if calculated != self.content_sha256:
            raise HourlyBlockCorruptionError(
                "Encoded hourly-block content SHA-256 does not match"
            )

    @property
    def encoded_size_bytes(self) -> int:
        return sum(
            len(value)
            for value in (
                self.bid_liquidity_block,
                self.ask_liquidity_block,
                self.validity_block,
                self.source_count_block,
            )
        )

    @property
    def bid_block_size_bytes(self) -> int:
        return len(self.bid_liquidity_block)

    @property
    def ask_block_size_bytes(self) -> int:
        return len(self.ask_liquidity_block)

    @property
    def validity_block_size_bytes(self) -> int:
        return len(self.validity_block)

    @property
    def source_count_block_size_bytes(self) -> int:
        return len(self.source_count_block)


@dataclass(frozen=True, slots=True)
class DecodedHourlyLiquidityChannels:
    """Strictly decoded authoritative hourly liquidity channels."""

    bid_liquidity: tuple[Decimal | None, ...]
    ask_liquidity: tuple[Decimal | None, ...]
    quality: tuple[BookSampleQuality, ...]
    invalid_reason: tuple[BookSampleInvalidReason | None, ...]
    source_count: tuple[int, ...]

    def __post_init__(self) -> None:
        channels = (
            self.bid_liquidity,
            self.ask_liquidity,
            self.quality,
            self.invalid_reason,
            self.source_count,
        )

        if any(len(channel) != OBSERVATIONS_PER_HOUR for channel in channels):
            raise HourlyBlockCodecError(
                "Every decoded hourly channel must contain 3,600 values"
            )

        for index in range(OBSERVATIONS_PER_HOUR):
            bid = self.bid_liquidity[index]
            ask = self.ask_liquidity[index]
            quality = BookSampleQuality(self.quality[index])
            reason = self.invalid_reason[index]
            count = self.source_count[index]

            if (
                isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
                or count > 65_535
            ):
                raise HourlyBlockCodecError(
                    f"source_count[{index}] is outside uint16 range"
                )

            if quality is BookSampleQuality.VALID:
                if reason is not None:
                    raise HourlyBlockCodecError(
                        f"VALID observation {index} has an invalid reason"
                    )

                if bid is None or ask is None:
                    raise HourlyBlockCodecError(
                        f"VALID observation {index} lacks liquidity"
                    )

                if count <= 0:
                    raise HourlyBlockCodecError(
                        f"VALID observation {index} has no source"
                    )

            else:
                if reason is None:
                    raise HourlyBlockCodecError(
                        f"Non-valid observation {index} lacks a reason"
                    )

                if bid is not None or ask is not None:
                    raise HourlyBlockCodecError(
                        f"Non-valid observation {index} contains liquidity"
                    )

                if count != 0:
                    raise HourlyBlockCodecError(
                        f"Non-valid observation {index} has source_count != 0"
                    )

    @property
    def observation_count(self) -> int:
        return len(self.quality)

    @property
    def valid_count(self) -> int:
        return sum(quality is BookSampleQuality.VALID for quality in self.quality)

    @property
    def invalid_count(self) -> int:
        return sum(quality is BookSampleQuality.INVALID for quality in self.quality)

    @property
    def degraded_count(self) -> int:
        return sum(quality is BookSampleQuality.DEGRADED for quality in self.quality)


def encode_hourly_liquidity_block(
    block: HourlyLiquidityBlock,
) -> EncodedHourlyLiquidityBlocks:
    """Encode one immutable 3,600-slot hourly liquidity result."""
    if not isinstance(block, HourlyLiquidityBlock):
        raise TypeError("block must be an HourlyLiquidityBlock")

    bid_values: list[str | None] = []
    ask_values: list[str | None] = []
    quality_codes: list[int] = []
    reason_codes: list[int] = []
    source_counts: list[int] = []

    for index, observation in enumerate(block.observations):
        quality = BookSampleQuality(observation.quality)

        try:
            quality_code = _QUALITY_TO_CODE[quality]
        except KeyError as exc:
            raise HourlyBlockCodecError(
                f"Unsupported quality at observation {index}"
            ) from exc

        if observation.invalid_reason is None:
            reason_code = 0
        else:
            try:
                reason_code = _REASON_TO_CODE[
                    BookSampleInvalidReason(observation.invalid_reason)
                ]
            except (KeyError, ValueError) as exc:
                raise HourlyBlockCodecError(
                    f"Unsupported invalid reason at observation {index}"
                ) from exc

        if quality is BookSampleQuality.VALID:
            if observation.bid_liquidity is None or observation.ask_liquidity is None:
                raise HourlyBlockCodecError(
                    f"VALID observation {index} lacks Bid/Ask Liquidity"
                )

            bid_text = _canonical_nonnegative_decimal_text(
                observation.bid_liquidity,
                field_name=f"bid_liquidity[{index}]",
            )
            ask_text = _canonical_nonnegative_decimal_text(
                observation.ask_liquidity,
                field_name=f"ask_liquidity[{index}]",
            )
        else:
            if (
                observation.bid_liquidity is not None
                or observation.ask_liquidity is not None
            ):
                raise HourlyBlockCodecError(
                    f"Non-valid observation {index} contains liquidity"
                )

            bid_text = None
            ask_text = None

        source_count = observation.source_count

        if (
            isinstance(source_count, bool)
            or not isinstance(source_count, int)
            or source_count < 0
            or source_count > 65_535
        ):
            raise HourlyBlockCodecError(
                f"source_count[{index}] is outside uint16 range"
            )

        bid_values.append(bid_text)
        ask_values.append(ask_text)
        quality_codes.append(quality_code)
        reason_codes.append(reason_code)
        source_counts.append(source_count)

    bid_schema = _schema_for_channel(HourlyLiquidityChannel.BID_LIQUIDITY)
    ask_schema = _schema_for_channel(HourlyLiquidityChannel.ASK_LIQUIDITY)
    validity_schema = _schema_for_channel(HourlyLiquidityChannel.VALIDITY)
    source_schema = _schema_for_channel(HourlyLiquidityChannel.SOURCE_COUNT)

    bid_table = pa.Table.from_arrays(
        [
            pa.array(
                bid_values,
                type=pa.string(),
            )
        ],
        schema=bid_schema,
    )
    ask_table = pa.Table.from_arrays(
        [
            pa.array(
                ask_values,
                type=pa.string(),
            )
        ],
        schema=ask_schema,
    )
    validity_table = pa.Table.from_arrays(
        [
            pa.array(
                quality_codes,
                type=pa.uint8(),
            ),
            pa.array(
                reason_codes,
                type=pa.uint8(),
            ),
        ],
        schema=validity_schema,
    )
    source_table = pa.Table.from_arrays(
        [
            pa.array(
                source_counts,
                type=pa.uint16(),
            )
        ],
        schema=source_schema,
    )

    bid_block = _wrap_payload(
        _write_arrow_payload(
            bid_table,
            channel=HourlyLiquidityChannel.BID_LIQUIDITY,
        ),
        channel=HourlyLiquidityChannel.BID_LIQUIDITY,
    )
    ask_block = _wrap_payload(
        _write_arrow_payload(
            ask_table,
            channel=HourlyLiquidityChannel.ASK_LIQUIDITY,
        ),
        channel=HourlyLiquidityChannel.ASK_LIQUIDITY,
    )
    validity_block = _wrap_payload(
        _write_arrow_payload(
            validity_table,
            channel=HourlyLiquidityChannel.VALIDITY,
        ),
        channel=HourlyLiquidityChannel.VALIDITY,
    )
    source_count_block = _wrap_payload(
        _write_arrow_payload(
            source_table,
            channel=HourlyLiquidityChannel.SOURCE_COUNT,
        ),
        channel=HourlyLiquidityChannel.SOURCE_COUNT,
    )

    content_sha256 = _encoded_content_sha256(
        bid_liquidity_block=bid_block,
        ask_liquidity_block=ask_block,
        validity_block=validity_block,
        source_count_block=source_count_block,
    )

    return EncodedHourlyLiquidityBlocks(
        format_version=HOURLY_BLOCK_FORMAT_VERSION,
        codec=HOURLY_BLOCK_CODEC,
        observation_count=OBSERVATIONS_PER_HOUR,
        bid_liquidity_block=bid_block,
        ask_liquidity_block=ask_block,
        validity_block=validity_block,
        source_count_block=source_count_block,
        content_sha256=content_sha256,
    )


def _decode_decimal_channel(
    encoded: bytes,
    *,
    channel: HourlyLiquidityChannel,
    column_name: str,
) -> tuple[Decimal | None, ...]:
    payload = _unwrap_payload(
        encoded,
        expected_channel=channel,
    )
    table = _read_arrow_table(
        payload,
        channel=channel,
    )

    values = table.column(column_name).to_pylist()
    decoded: list[Decimal | None] = []

    for index, value in enumerate(values):
        if value is None:
            decoded.append(None)
            continue

        decoded.append(
            _parse_canonical_nonnegative_decimal(
                value,
                field_name=f"{column_name}[{index}]",
            )
        )

    return tuple(decoded)


def _decode_validity_channel(
    encoded: bytes,
) -> tuple[
    tuple[BookSampleQuality, ...],
    tuple[BookSampleInvalidReason | None, ...],
]:
    channel = HourlyLiquidityChannel.VALIDITY
    payload = _unwrap_payload(
        encoded,
        expected_channel=channel,
    )
    table = _read_arrow_table(
        payload,
        channel=channel,
    )

    quality_column = table.column("quality_code")
    reason_column = table.column("invalid_reason_code")

    if quality_column.null_count or reason_column.null_count:
        raise HourlyBlockCorruptionError(
            "Validity channel contains unexpected null values"
        )

    quality_values = quality_column.to_pylist()
    reason_values = reason_column.to_pylist()

    qualities: list[BookSampleQuality] = []
    reasons: list[BookSampleInvalidReason | None] = []

    for index, raw_quality in enumerate(quality_values):
        try:
            quality = _CODE_TO_QUALITY[int(raw_quality)]
        except (KeyError, TypeError, ValueError) as exc:
            raise HourlyBlockCodecError(
                f"quality_code[{index}] is unsupported"
            ) from exc

        raw_reason = int(reason_values[index])

        if raw_reason == 0:
            reason = None
        else:
            try:
                reason = _CODE_TO_REASON[raw_reason]
            except KeyError as exc:
                raise HourlyBlockCodecError(
                    f"invalid_reason_code[{index}] is unsupported"
                ) from exc

        if quality is BookSampleQuality.VALID and reason is not None:
            raise HourlyBlockCodecError(
                f"VALID observation {index} has an invalid reason code"
            )

        if quality is not BookSampleQuality.VALID and reason is None:
            raise HourlyBlockCodecError(
                f"Non-valid observation {index} lacks an invalid reason code"
            )

        qualities.append(quality)
        reasons.append(reason)

    return tuple(qualities), tuple(reasons)


def _decode_source_count_channel(
    encoded: bytes,
) -> tuple[int, ...]:
    channel = HourlyLiquidityChannel.SOURCE_COUNT
    payload = _unwrap_payload(
        encoded,
        expected_channel=channel,
    )
    table = _read_arrow_table(
        payload,
        channel=channel,
    )

    column = table.column("source_count")

    if column.null_count:
        raise HourlyBlockCorruptionError(
            "Source-count channel contains unexpected null values"
        )

    return tuple(int(value) for value in column.to_pylist())


def decode_hourly_liquidity_blocks(
    encoded: EncodedHourlyLiquidityBlocks,
) -> DecodedHourlyLiquidityChannels:
    """Decode and cross-validate all four authoritative channels."""
    if not isinstance(encoded, EncodedHourlyLiquidityBlocks):
        raise TypeError("encoded must be EncodedHourlyLiquidityBlocks")

    expected_content_sha256 = _encoded_content_sha256(
        bid_liquidity_block=encoded.bid_liquidity_block,
        ask_liquidity_block=encoded.ask_liquidity_block,
        validity_block=encoded.validity_block,
        source_count_block=encoded.source_count_block,
    )

    if expected_content_sha256 != encoded.content_sha256:
        raise HourlyBlockCorruptionError(
            "Complete hourly-block content SHA-256 does not match"
        )

    bid = _decode_decimal_channel(
        encoded.bid_liquidity_block,
        channel=HourlyLiquidityChannel.BID_LIQUIDITY,
        column_name="bid_liquidity",
    )
    ask = _decode_decimal_channel(
        encoded.ask_liquidity_block,
        channel=HourlyLiquidityChannel.ASK_LIQUIDITY,
        column_name="ask_liquidity",
    )
    quality, invalid_reason = _decode_validity_channel(encoded.validity_block)
    source_count = _decode_source_count_channel(encoded.source_count_block)

    return DecodedHourlyLiquidityChannels(
        bid_liquidity=bid,
        ask_liquidity=ask,
        quality=quality,
        invalid_reason=invalid_reason,
        source_count=source_count,
    )


def verify_hourly_liquidity_blocks(
    encoded: EncodedHourlyLiquidityBlocks,
) -> None:
    """Decode and validate an encoded hourly block set."""
    decode_hourly_liquidity_blocks(encoded)


def liquidity_quality_summary_to_dict(
    summary: LiquidityQualitySummary,
) -> dict[str, object]:
    """Return the stable JSON-safe representation stored beside block bytes."""
    if not isinstance(summary, LiquidityQualitySummary):
        raise TypeError("summary must be a LiquidityQualitySummary")

    return {
        "schema": "l2shock.liquidity_quality_summary",
        "schema_version": 1,
        "observation_count": OBSERVATIONS_PER_HOUR,
        "valid_count": summary.valid_count,
        "degraded_count": summary.degraded_count,
        "invalid_count": summary.invalid_count,
        "zero_total_liquidity_count": (summary.zero_total_liquidity_count),
        "invalid_reason_counts": {
            reason.value: count for reason, count in summary.invalid_reason_counts
        },
    }


__all__ = [
    "HOURLY_BLOCK_CODEC",
    "HOURLY_BLOCK_FORMAT_VERSION",
    "HOURLY_BLOCK_MAGIC",
    "DecodedHourlyLiquidityChannels",
    "EncodedHourlyLiquidityBlocks",
    "HourlyBlockCodecError",
    "HourlyBlockCorruptionError",
    "HourlyBlockLimitError",
    "HourlyInvalidReasonCode",
    "HourlyLiquidityChannel",
    "HourlyQualityCode",
    "decode_hourly_liquidity_blocks",
    "encode_hourly_liquidity_block",
    "liquidity_quality_summary_to_dict",
    "verify_hourly_liquidity_blocks",
]

# l2shock/price/block_codec.py
"""Versioned compact encoding for hourly real-trade OHLC channels.

The PostgreSQL price_hourly_series schema owns three binary channels:

    ohlc_block
    validity_block
    trade_count_block

OHLC values remain exact Decimals. They are encoded as canonical non-exponent
decimal strings inside Zstandard-compressed Arrow IPC streams.

The validity channel uses explicit fixed numeric format identities. Existing
format-version-1 codes must never depend on Python enum declaration order.

Each channel has a binary envelope containing:

    magic
    format version
    channel identity
    flags
    observation count
    Arrow payload length
    Arrow payload SHA-256

The complete three-channel artifact has one deterministic combined SHA-256.

Hashes detect corruption; they do not authenticate the producer. The database
row, not the channel payload, owns base, UTC hour, source venue, source symbol,
quality summary, and provenance.
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

from l2shock.price.hourly import (
    PRICE_OBSERVATIONS_PER_HOUR,
    HourlyTradeOHLCBlock,
    TradeOHLCQualitySummary,
    TradeSampleInvalidReason,
    TradeSampleQuality,
)

PRICE_BLOCK_FORMAT_VERSION: Final[int] = 1
PRICE_BLOCK_CODEC: Final[str] = "arrow-ipc-zstd"
PRICE_BLOCK_SCHEMA_NAME: Final[str] = "l2shock.hourly_trade_ohlc_channel"

# Exactly eight bytes.
PRICE_BLOCK_MAGIC: Final[bytes] = b"L2PBLK1\x00"

# magic, version, channel, flags, observation_count, payload_length, SHA-256
_BLOCK_HEADER: Final[struct.Struct] = struct.Struct(">8sHBBIQ32s")

_MAX_BLOCK_PAYLOAD_BYTES: Final[int] = 64 * 1024 * 1024
_MAX_DECIMAL_TEXT_LENGTH: Final[int] = 256
_MAX_TRADE_COUNT: Final[int] = (2**32) - 1

_CONTENT_HASH_DOMAIN: Final[bytes] = b"l2shock/hourly-trade-ohlc-blocks/v1\x00"
_LENGTH_PREFIX: Final[struct.Struct] = struct.Struct(">Q")


class PriceBlockCodecError(ValueError):
    """An hourly trade-price block cannot be encoded or decoded safely."""


class PriceBlockCorruptionError(PriceBlockCodecError):
    """An encoded channel is truncated, modified, or inconsistent."""


class PriceBlockLimitError(PriceBlockCodecError):
    """An encoded channel exceeds a resource-safety limit."""


class PriceBlockChannel(IntEnum):
    """Stable binary identities for price-hour storage channels."""

    OHLC = 1
    VALIDITY = 2
    TRADE_COUNT = 3


class PriceQualityCode(IntEnum):
    """Stable format-version-1 trade-price quality codes.

    DEGRADED is reserved so the general project quality identity remains:

        1 = VALID
        2 = DEGRADED
        3 = INVALID

    The current one-second trade OHLC path emits only VALID and INVALID.
    """

    VALID = 1
    DEGRADED = 2
    INVALID = 3


class PriceInvalidReasonCode(IntEnum):
    """Stable format-version-1 trade-price invalid-reason codes.

    Zero is reserved for the absence of an invalid reason.
    """

    NO_TRADES = 1


_QUALITY_TO_CODE: Final[dict[TradeSampleQuality, int]] = {
    TradeSampleQuality.VALID: int(PriceQualityCode.VALID),
    TradeSampleQuality.INVALID: int(PriceQualityCode.INVALID),
}
_CODE_TO_QUALITY: Final[dict[int, TradeSampleQuality]] = {
    code: quality for quality, code in _QUALITY_TO_CODE.items()
}

_REASON_TO_CODE: Final[dict[TradeSampleInvalidReason, int]] = {
    TradeSampleInvalidReason.NO_TRADES: int(PriceInvalidReasonCode.NO_TRADES),
}
_CODE_TO_REASON: Final[dict[int, TradeSampleInvalidReason]] = {
    code: reason for reason, code in _REASON_TO_CODE.items()
}

if frozenset(_QUALITY_TO_CODE) != frozenset(TradeSampleQuality):
    raise RuntimeError("Price format-version-1 quality-code registry is incomplete")

if frozenset(_REASON_TO_CODE) != frozenset(TradeSampleInvalidReason):
    raise RuntimeError("Price format-version-1 invalid-reason registry is incomplete")

if len(set(_QUALITY_TO_CODE.values())) != len(_QUALITY_TO_CODE):
    raise RuntimeError("Price format-version-1 quality codes are not unique")

if len(set(_REASON_TO_CODE.values())) != len(_REASON_TO_CODE):
    raise RuntimeError("Price format-version-1 invalid-reason codes are not unique")


def _sha256_text(value: object) -> str:
    text = str(value or "").strip()

    if (
        text != text.lower()
        or len(text) != 64
        or any(character not in "0123456789abcdef" for character in text)
    ):
        raise PriceBlockCodecError(
            "SHA-256 text must contain exactly 64 canonical lowercase "
            "hexadecimal characters"
        )

    return text


def _canonical_positive_decimal_text(
    value: Decimal,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, Decimal):
        raise PriceBlockCodecError(f"{field_name} must be an exact Decimal")

    if not value.is_finite() or value <= 0:
        raise PriceBlockCodecError(f"{field_name} must be a finite positive Decimal")

    try:
        text = format(value, "f")
    except (ValueError, OverflowError) as exc:
        raise PriceBlockCodecError(
            f"{field_name} cannot be represented canonically"
        ) from exc

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if not text or text in {"0", "-0", "+0"}:
        raise PriceBlockCodecError(f"{field_name} must be positive")

    if "e" in text.lower():
        raise PriceBlockCodecError(
            f"{field_name} canonical text must not use exponent notation"
        )

    if len(text) > _MAX_DECIMAL_TEXT_LENGTH:
        raise PriceBlockLimitError(
            f"{field_name} exceeds the maximum encoded decimal length"
        )

    return text


def _parse_canonical_positive_decimal(
    value: object,
    *,
    field_name: str,
) -> Decimal:
    if not isinstance(value, str):
        raise PriceBlockCodecError(f"{field_name} must be encoded as text")

    if not value or len(value) > _MAX_DECIMAL_TEXT_LENGTH:
        raise PriceBlockLimitError(f"{field_name} has an invalid encoded length")

    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise PriceBlockCodecError(f"{field_name} is not a valid decimal") from exc

    canonical = _canonical_positive_decimal_text(
        parsed,
        field_name=field_name,
    )

    if value != canonical:
        raise PriceBlockCodecError(f"{field_name} is not canonically encoded")

    return parsed


def _schema_metadata(
    channel: PriceBlockChannel,
) -> dict[bytes, bytes]:
    return {
        b"l2shock.schema": PRICE_BLOCK_SCHEMA_NAME.encode("ascii"),
        b"l2shock.schema_version": str(PRICE_BLOCK_FORMAT_VERSION).encode("ascii"),
        b"l2shock.codec": PRICE_BLOCK_CODEC.encode("ascii"),
        b"l2shock.channel": channel.name.lower().encode("ascii"),
        b"l2shock.observation_count": str(PRICE_OBSERVATIONS_PER_HOUR).encode("ascii"),
        b"l2shock.sampling_interval_ms": b"1000",
        b"l2shock.price_clock": b"trade_time_ms",
        b"l2shock.bucket_policy": b"half_open_start_inclusive",
    }


def _schema_for_channel(
    channel: PriceBlockChannel,
) -> pa.Schema:
    metadata = _schema_metadata(channel)

    if channel is PriceBlockChannel.OHLC:
        return pa.schema(
            [
                pa.field("open", pa.string(), nullable=True),
                pa.field("high", pa.string(), nullable=True),
                pa.field("low", pa.string(), nullable=True),
                pa.field("close", pa.string(), nullable=True),
            ],
            metadata=metadata,
        )

    if channel is PriceBlockChannel.VALIDITY:
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

    if channel is PriceBlockChannel.TRADE_COUNT:
        return pa.schema(
            [
                pa.field(
                    "trade_count",
                    pa.uint32(),
                    nullable=False,
                ),
            ],
            metadata=metadata,
        )

    raise PriceBlockCodecError(f"Unsupported price block channel {channel!r}")


def _write_arrow_payload(
    table: pa.Table,
    *,
    channel: PriceBlockChannel,
) -> bytes:
    expected_schema = _schema_for_channel(channel)

    if not table.schema.equals(
        expected_schema,
        check_metadata=True,
    ):
        raise PriceBlockCodecError(f"Arrow table schema does not match {channel.name}")

    if table.num_rows != PRICE_OBSERVATIONS_PER_HOUR:
        raise PriceBlockCodecError(
            "Hourly price Arrow table must contain exactly "
            f"{PRICE_OBSERVATIONS_PER_HOUR} rows"
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
        raise PriceBlockCodecError(
            f"Could not encode {channel.name} Arrow IPC payload"
        ) from exc

    if not payload:
        raise PriceBlockCodecError(f"{channel.name} Arrow IPC payload is empty")

    if len(payload) > _MAX_BLOCK_PAYLOAD_BYTES:
        raise PriceBlockLimitError(
            f"{channel.name} Arrow IPC payload exceeds "
            f"{_MAX_BLOCK_PAYLOAD_BYTES} bytes"
        )

    return payload


def _wrap_payload(
    payload: bytes,
    *,
    channel: PriceBlockChannel,
) -> bytes:
    if len(payload) > _MAX_BLOCK_PAYLOAD_BYTES:
        raise PriceBlockLimitError("Arrow payload exceeds the price block limit")

    payload_digest = hashlib.sha256(payload).digest()

    header = _BLOCK_HEADER.pack(
        PRICE_BLOCK_MAGIC,
        PRICE_BLOCK_FORMAT_VERSION,
        int(channel),
        0,
        PRICE_OBSERVATIONS_PER_HOUR,
        len(payload),
        payload_digest,
    )

    return header + payload


def _unwrap_payload(
    encoded: bytes | bytearray | memoryview,
    *,
    expected_channel: PriceBlockChannel,
) -> bytes:
    if not isinstance(encoded, (bytes, bytearray, memoryview)):
        raise TypeError("encoded price channel must be bytes-like")

    data = bytes(encoded)

    if len(data) < _BLOCK_HEADER.size:
        raise PriceBlockCorruptionError("Price block is shorter than its fixed header")

    (
        magic,
        version,
        raw_channel,
        flags,
        observation_count,
        payload_length,
        expected_payload_digest,
    ) = _BLOCK_HEADER.unpack_from(data)

    if magic != PRICE_BLOCK_MAGIC:
        raise PriceBlockCorruptionError("Price block magic is invalid")

    if version != PRICE_BLOCK_FORMAT_VERSION:
        raise PriceBlockCodecError(f"Unsupported price block version {version}")

    try:
        channel = PriceBlockChannel(raw_channel)
    except ValueError as exc:
        raise PriceBlockCodecError(
            f"Unknown price block channel {raw_channel}"
        ) from exc

    if channel is not expected_channel:
        raise PriceBlockCodecError(
            "Price block channel does not match its storage destination: "
            f"encoded={channel.name}, expected={expected_channel.name}"
        )

    if flags != 0:
        raise PriceBlockCodecError(f"Unsupported price block flags value {flags}")

    if observation_count != PRICE_OBSERVATIONS_PER_HOUR:
        raise PriceBlockCorruptionError("Price block observation count is not 3,600")

    if payload_length > _MAX_BLOCK_PAYLOAD_BYTES:
        raise PriceBlockLimitError("Price block declares an oversized Arrow payload")

    expected_total = _BLOCK_HEADER.size + payload_length

    if len(data) != expected_total:
        raise PriceBlockCorruptionError(
            "Price block encoded length does not match its header: "
            f"actual={len(data)}, expected={expected_total}"
        )

    payload = data[_BLOCK_HEADER.size :]
    actual_payload_digest = hashlib.sha256(payload).digest()

    if actual_payload_digest != expected_payload_digest:
        raise PriceBlockCorruptionError(
            "Price block Arrow payload SHA-256 does not match"
        )

    return payload


def _read_arrow_table(
    payload: bytes,
    *,
    channel: PriceBlockChannel,
) -> pa.Table:
    expected_schema = _schema_for_channel(channel)
    source = pa.BufferReader(payload)

    try:
        reader = ipc.open_stream(source)

        if not reader.schema.equals(
            expected_schema,
            check_metadata=True,
        ):
            raise PriceBlockCodecError(
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

            if row_count > PRICE_OBSERVATIONS_PER_HOUR:
                raise PriceBlockCorruptionError(
                    f"Decoded {channel.name} row count is not 3,600"
                )

            batches.append(batch)

        if row_count != PRICE_OBSERVATIONS_PER_HOUR:
            raise PriceBlockCorruptionError(
                f"Decoded {channel.name} row count is not 3,600"
            )

        table = pa.Table.from_batches(
            batches,
            schema=expected_schema,
        )
    except (PriceBlockCodecError, PriceBlockCorruptionError):
        raise
    except Exception as exc:
        raise PriceBlockCorruptionError(
            f"Could not decode {channel.name} Arrow IPC payload"
        ) from exc

    if source.tell() != len(payload):
        raise PriceBlockCorruptionError(
            f"{channel.name} Arrow IPC payload contains trailing bytes"
        )

    return table


def _encoded_content_sha256(
    *,
    ohlc_block: bytes,
    validity_block: bytes,
    trade_count_block: bytes,
) -> str:
    digest = hashlib.sha256()
    digest.update(_CONTENT_HASH_DOMAIN)

    for block in (
        ohlc_block,
        validity_block,
        trade_count_block,
    ):
        digest.update(_LENGTH_PREFIX.pack(len(block)))
        digest.update(block)

    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class EncodedHourlyTradeOHLCBlocks:
    """Three encoded PostgreSQL channels for one real-trade price hour."""

    format_version: int
    codec: str
    observation_count: int

    ohlc_block: bytes
    validity_block: bytes
    trade_count_block: bytes

    content_sha256: str

    def __post_init__(self) -> None:
        if self.format_version != PRICE_BLOCK_FORMAT_VERSION:
            raise PriceBlockCodecError(
                "Encoded price block format version is unsupported"
            )

        if self.codec != PRICE_BLOCK_CODEC:
            raise PriceBlockCodecError(
                "Encoded price block codec identity is unsupported"
            )

        if self.observation_count != PRICE_OBSERVATIONS_PER_HOUR:
            raise PriceBlockCodecError(
                "Encoded hourly price blocks must contain 3,600 observations"
            )

        for name in (
            "ohlc_block",
            "validity_block",
            "trade_count_block",
        ):
            value = getattr(self, name)

            if not isinstance(value, bytes) or not value:
                raise PriceBlockCodecError(f"{name} must be non-empty bytes")

        object.__setattr__(
            self,
            "content_sha256",
            _sha256_text(self.content_sha256),
        )

        calculated = _encoded_content_sha256(
            ohlc_block=self.ohlc_block,
            validity_block=self.validity_block,
            trade_count_block=self.trade_count_block,
        )

        if calculated != self.content_sha256:
            raise PriceBlockCorruptionError(
                "Encoded price-block content SHA-256 does not match"
            )

    @property
    def encoded_size_bytes(self) -> int:
        return (
            len(self.ohlc_block)
            + len(self.validity_block)
            + len(self.trade_count_block)
        )

    @property
    def ohlc_block_size_bytes(self) -> int:
        return len(self.ohlc_block)

    @property
    def validity_block_size_bytes(self) -> int:
        return len(self.validity_block)

    @property
    def trade_count_block_size_bytes(self) -> int:
        return len(self.trade_count_block)


@dataclass(frozen=True, slots=True)
class DecodedHourlyTradeOHLCChannels:
    """Strictly decoded authoritative one-second price channels."""

    open: tuple[Decimal | None, ...]
    high: tuple[Decimal | None, ...]
    low: tuple[Decimal | None, ...]
    close: tuple[Decimal | None, ...]

    quality: tuple[TradeSampleQuality, ...]
    invalid_reason: tuple[
        TradeSampleInvalidReason | None,
        ...,
    ]
    trade_count: tuple[int, ...]

    def __post_init__(self) -> None:
        channels = (
            self.open,
            self.high,
            self.low,
            self.close,
            self.quality,
            self.invalid_reason,
            self.trade_count,
        )

        if any(len(channel) != PRICE_OBSERVATIONS_PER_HOUR for channel in channels):
            raise PriceBlockCodecError(
                "Every decoded price channel must contain 3,600 values"
            )

        for index in range(PRICE_OBSERVATIONS_PER_HOUR):
            quality = TradeSampleQuality(self.quality[index])
            reason = self.invalid_reason[index]
            count = self.trade_count[index]
            values = (
                self.open[index],
                self.high[index],
                self.low[index],
                self.close[index],
            )

            if (
                isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
                or count > _MAX_TRADE_COUNT
            ):
                raise PriceBlockCodecError(
                    f"trade_count[{index}] is outside uint32 range"
                )

            if quality is TradeSampleQuality.VALID:
                if reason is not None:
                    raise PriceBlockCodecError(
                        f"VALID price observation {index} has an invalid reason"
                    )

                if count <= 0:
                    raise PriceBlockCodecError(
                        f"VALID price observation {index} has no trades"
                    )

                if any(
                    not isinstance(value, Decimal)
                    or not value.is_finite()
                    or value <= 0
                    for value in values
                ):
                    raise PriceBlockCodecError(
                        f"VALID price observation {index} has invalid OHLC"
                    )

                open_value = self.open[index]
                high_value = self.high[index]
                low_value = self.low[index]
                close_value = self.close[index]

                assert open_value is not None
                assert high_value is not None
                assert low_value is not None
                assert close_value is not None

                if high_value < low_value:
                    raise PriceBlockCodecError(
                        f"Price observation {index} has high below low"
                    )

                if not (
                    low_value <= open_value <= high_value
                    and low_value <= close_value <= high_value
                ):
                    raise PriceBlockCodecError(
                        f"Price observation {index} has invalid OHLC geometry"
                    )

            else:
                if reason is None:
                    raise PriceBlockCodecError(
                        f"INVALID price observation {index} lacks a reason"
                    )

                if any(value is not None for value in values):
                    raise PriceBlockCodecError(
                        f"INVALID price observation {index} contains OHLC"
                    )

                if count != 0:
                    raise PriceBlockCodecError(
                        f"INVALID price observation {index} has trades"
                    )

    @property
    def observation_count(self) -> int:
        return len(self.quality)

    @property
    def valid_count(self) -> int:
        return sum(quality is TradeSampleQuality.VALID for quality in self.quality)

    @property
    def invalid_count(self) -> int:
        return sum(quality is TradeSampleQuality.INVALID for quality in self.quality)

    @property
    def total_trade_count(self) -> int:
        return sum(self.trade_count)


def encode_hourly_trade_ohlc_block(
    block: HourlyTradeOHLCBlock,
) -> EncodedHourlyTradeOHLCBlocks:
    """Encode one immutable 3,600-slot real-trade OHLC result."""
    if not isinstance(block, HourlyTradeOHLCBlock):
        raise TypeError("block must be an HourlyTradeOHLCBlock")

    open_values: list[str | None] = []
    high_values: list[str | None] = []
    low_values: list[str | None] = []
    close_values: list[str | None] = []

    quality_codes: list[int] = []
    reason_codes: list[int] = []
    trade_counts: list[int] = []

    for index, observation in enumerate(block.observations):
        quality = TradeSampleQuality(observation.quality)

        try:
            quality_code = _QUALITY_TO_CODE[quality]
        except KeyError as exc:
            raise PriceBlockCodecError(
                f"Unsupported price quality at observation {index}"
            ) from exc

        if observation.invalid_reason is None:
            reason_code = 0
        else:
            try:
                reason_code = _REASON_TO_CODE[
                    TradeSampleInvalidReason(observation.invalid_reason)
                ]
            except (KeyError, ValueError) as exc:
                raise PriceBlockCodecError(
                    f"Unsupported price invalid reason at observation {index}"
                ) from exc

        trade_count = observation.trade_count

        if (
            isinstance(trade_count, bool)
            or not isinstance(trade_count, int)
            or trade_count < 0
            or trade_count > _MAX_TRADE_COUNT
        ):
            raise PriceBlockCodecError(f"trade_count[{index}] is outside uint32 range")

        if quality is TradeSampleQuality.VALID:
            values = (
                observation.open,
                observation.high,
                observation.low,
                observation.close,
            )

            if any(value is None for value in values):
                raise PriceBlockCodecError(
                    f"VALID price observation {index} lacks OHLC"
                )

            assert observation.open is not None
            assert observation.high is not None
            assert observation.low is not None
            assert observation.close is not None

            open_text = _canonical_positive_decimal_text(
                observation.open,
                field_name=f"open[{index}]",
            )
            high_text = _canonical_positive_decimal_text(
                observation.high,
                field_name=f"high[{index}]",
            )
            low_text = _canonical_positive_decimal_text(
                observation.low,
                field_name=f"low[{index}]",
            )
            close_text = _canonical_positive_decimal_text(
                observation.close,
                field_name=f"close[{index}]",
            )
        else:
            if any(
                value is not None
                for value in (
                    observation.open,
                    observation.high,
                    observation.low,
                    observation.close,
                )
            ):
                raise PriceBlockCodecError(
                    f"INVALID price observation {index} contains OHLC"
                )

            open_text = None
            high_text = None
            low_text = None
            close_text = None

        open_values.append(open_text)
        high_values.append(high_text)
        low_values.append(low_text)
        close_values.append(close_text)
        quality_codes.append(quality_code)
        reason_codes.append(reason_code)
        trade_counts.append(trade_count)

    ohlc_schema = _schema_for_channel(PriceBlockChannel.OHLC)
    validity_schema = _schema_for_channel(PriceBlockChannel.VALIDITY)
    trade_count_schema = _schema_for_channel(PriceBlockChannel.TRADE_COUNT)

    ohlc_table = pa.Table.from_arrays(
        [
            pa.array(open_values, type=pa.string()),
            pa.array(high_values, type=pa.string()),
            pa.array(low_values, type=pa.string()),
            pa.array(close_values, type=pa.string()),
        ],
        schema=ohlc_schema,
    )
    validity_table = pa.Table.from_arrays(
        [
            pa.array(quality_codes, type=pa.uint8()),
            pa.array(reason_codes, type=pa.uint8()),
        ],
        schema=validity_schema,
    )
    trade_count_table = pa.Table.from_arrays(
        [
            pa.array(trade_counts, type=pa.uint32()),
        ],
        schema=trade_count_schema,
    )

    ohlc_block = _wrap_payload(
        _write_arrow_payload(
            ohlc_table,
            channel=PriceBlockChannel.OHLC,
        ),
        channel=PriceBlockChannel.OHLC,
    )
    validity_block = _wrap_payload(
        _write_arrow_payload(
            validity_table,
            channel=PriceBlockChannel.VALIDITY,
        ),
        channel=PriceBlockChannel.VALIDITY,
    )
    trade_count_block = _wrap_payload(
        _write_arrow_payload(
            trade_count_table,
            channel=PriceBlockChannel.TRADE_COUNT,
        ),
        channel=PriceBlockChannel.TRADE_COUNT,
    )

    content_sha256 = _encoded_content_sha256(
        ohlc_block=ohlc_block,
        validity_block=validity_block,
        trade_count_block=trade_count_block,
    )

    return EncodedHourlyTradeOHLCBlocks(
        format_version=PRICE_BLOCK_FORMAT_VERSION,
        codec=PRICE_BLOCK_CODEC,
        observation_count=PRICE_OBSERVATIONS_PER_HOUR,
        ohlc_block=ohlc_block,
        validity_block=validity_block,
        trade_count_block=trade_count_block,
        content_sha256=content_sha256,
    )


def _decode_ohlc_channel(
    encoded: bytes,
) -> tuple[
    tuple[Decimal | None, ...],
    tuple[Decimal | None, ...],
    tuple[Decimal | None, ...],
    tuple[Decimal | None, ...],
]:
    payload = _unwrap_payload(
        encoded,
        expected_channel=PriceBlockChannel.OHLC,
    )
    table = _read_arrow_table(
        payload,
        channel=PriceBlockChannel.OHLC,
    )

    decoded_columns: list[tuple[Decimal | None, ...]] = []

    for column_name in ("open", "high", "low", "close"):
        raw_values = table.column(column_name).to_pylist()
        values: list[Decimal | None] = []

        for index, value in enumerate(raw_values):
            if value is None:
                values.append(None)
                continue

            values.append(
                _parse_canonical_positive_decimal(
                    value,
                    field_name=f"{column_name}[{index}]",
                )
            )

        decoded_columns.append(tuple(values))

    return (
        decoded_columns[0],
        decoded_columns[1],
        decoded_columns[2],
        decoded_columns[3],
    )


def _decode_validity_channel(
    encoded: bytes,
) -> tuple[
    tuple[TradeSampleQuality, ...],
    tuple[TradeSampleInvalidReason | None, ...],
]:
    payload = _unwrap_payload(
        encoded,
        expected_channel=PriceBlockChannel.VALIDITY,
    )
    table = _read_arrow_table(
        payload,
        channel=PriceBlockChannel.VALIDITY,
    )

    quality_column = table.column("quality_code")
    reason_column = table.column("invalid_reason_code")

    if quality_column.null_count or reason_column.null_count:
        raise PriceBlockCorruptionError(
            "Price validity channel contains unexpected null values"
        )

    raw_qualities = quality_column.to_pylist()
    raw_reasons = reason_column.to_pylist()

    qualities: list[TradeSampleQuality] = []
    reasons: list[TradeSampleInvalidReason | None] = []

    for index, raw_quality in enumerate(raw_qualities):
        try:
            quality = _CODE_TO_QUALITY[int(raw_quality)]
        except (KeyError, TypeError, ValueError) as exc:
            raise PriceBlockCodecError(f"quality_code[{index}] is unsupported") from exc

        raw_reason = int(raw_reasons[index])

        if raw_reason == 0:
            reason = None
        else:
            try:
                reason = _CODE_TO_REASON[raw_reason]
            except KeyError as exc:
                raise PriceBlockCodecError(
                    f"invalid_reason_code[{index}] is unsupported"
                ) from exc

        if quality is TradeSampleQuality.VALID and reason is not None:
            raise PriceBlockCodecError(
                f"VALID price observation {index} has an invalid reason code"
            )

        if quality is TradeSampleQuality.INVALID and reason is None:
            raise PriceBlockCodecError(
                f"INVALID price observation {index} lacks a reason code"
            )

        qualities.append(quality)
        reasons.append(reason)

    return tuple(qualities), tuple(reasons)


def _decode_trade_count_channel(
    encoded: bytes,
) -> tuple[int, ...]:
    payload = _unwrap_payload(
        encoded,
        expected_channel=PriceBlockChannel.TRADE_COUNT,
    )
    table = _read_arrow_table(
        payload,
        channel=PriceBlockChannel.TRADE_COUNT,
    )

    column = table.column("trade_count")

    if column.null_count:
        raise PriceBlockCorruptionError(
            "Trade-count channel contains unexpected null values"
        )

    return tuple(int(value) for value in column.to_pylist())


def decode_hourly_trade_ohlc_blocks(
    encoded: EncodedHourlyTradeOHLCBlocks,
) -> DecodedHourlyTradeOHLCChannels:
    """Decode and cross-validate all three authoritative price channels."""
    if not isinstance(encoded, EncodedHourlyTradeOHLCBlocks):
        raise TypeError("encoded must be EncodedHourlyTradeOHLCBlocks")

    expected_content_sha256 = _encoded_content_sha256(
        ohlc_block=encoded.ohlc_block,
        validity_block=encoded.validity_block,
        trade_count_block=encoded.trade_count_block,
    )

    if expected_content_sha256 != encoded.content_sha256:
        raise PriceBlockCorruptionError(
            "Complete price-block content SHA-256 does not match"
        )

    open_values, high_values, low_values, close_values = _decode_ohlc_channel(
        encoded.ohlc_block
    )
    quality, invalid_reason = _decode_validity_channel(encoded.validity_block)
    trade_count = _decode_trade_count_channel(encoded.trade_count_block)

    return DecodedHourlyTradeOHLCChannels(
        open=open_values,
        high=high_values,
        low=low_values,
        close=close_values,
        quality=quality,
        invalid_reason=invalid_reason,
        trade_count=trade_count,
    )


def verify_hourly_trade_ohlc_blocks(
    encoded: EncodedHourlyTradeOHLCBlocks,
) -> None:
    """Decode and validate a complete encoded hourly price artifact."""
    decode_hourly_trade_ohlc_blocks(encoded)


def trade_ohlc_quality_summary_to_dict(
    summary: TradeOHLCQualitySummary,
) -> dict[str, object]:
    """Return stable JSON-safe quality metadata for PostgreSQL."""
    if not isinstance(summary, TradeOHLCQualitySummary):
        raise TypeError("summary must be a TradeOHLCQualitySummary")

    return {
        "schema": "l2shock.trade_ohlc_quality_summary",
        "schema_version": 1,
        "observation_count": summary.observation_count,
        "valid_count": summary.valid_count,
        "invalid_count": summary.invalid_count,
        "total_trade_count": summary.total_trade_count,
        "invalid_reason_counts": {
            TradeSampleInvalidReason.NO_TRADES.value: (summary.no_trade_count),
        },
    }


__all__ = [
    "PRICE_BLOCK_CODEC",
    "PRICE_BLOCK_FORMAT_VERSION",
    "PRICE_BLOCK_MAGIC",
    "DecodedHourlyTradeOHLCChannels",
    "EncodedHourlyTradeOHLCBlocks",
    "PriceBlockChannel",
    "PriceBlockCodecError",
    "PriceBlockCorruptionError",
    "PriceBlockLimitError",
    "PriceInvalidReasonCode",
    "PriceQualityCode",
    "decode_hourly_trade_ohlc_blocks",
    "encode_hourly_trade_ohlc_block",
    "trade_ohlc_quality_summary_to_dict",
    "verify_hourly_trade_ohlc_blocks",
]

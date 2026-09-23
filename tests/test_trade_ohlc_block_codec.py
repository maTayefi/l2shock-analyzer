from __future__ import annotations

import hashlib
import struct
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.ingest import TradeRecord
from l2shock.price import (
    PRICE_BLOCK_CODEC,
    PRICE_BLOCK_FORMAT_VERSION,
    PRICE_OBSERVATIONS_PER_HOUR,
    EncodedHourlyTradeOHLCBlocks,
    PriceBlockChannel,
    PriceBlockCodecError,
    PriceBlockCorruptionError,
    PriceInvalidReasonCode,
    PriceQualityCode,
    TradeSampleInvalidReason,
    TradeSampleQuality,
    build_trade_ohlc_hour,
    decode_hourly_trade_ohlc_blocks,
    encode_hourly_trade_ohlc_block,
    trade_ohlc_quality_summary_to_dict,
    verify_hourly_trade_ohlc_blocks,
)

_CONTENT_HASH_DOMAIN = b"l2shock/hourly-trade-ohlc-blocks/v1\x00"
_LENGTH_PREFIX = struct.Struct(">Q")


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _epoch_ms(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


def _record(
    *,
    trade_id: str,
    milliseconds: int,
    price: str,
    quantity: str = "1",
    row_number: int = 0,
) -> TradeRecord:
    trade_time_ms = _epoch_ms(_hour()) + milliseconds

    return TradeRecord(
        symbol="BTCUSDT",
        trade_id=trade_id,
        price=Decimal(price),
        quantity=Decimal(quantity),
        received_time_ns=trade_time_ms * 1_000_000,
        event_time_ms=trade_time_ms,
        trade_time_ms=trade_time_ms,
        is_buyer_maker=False,
        order_type="market",
        row_number=row_number,
    )


def _block(
    *,
    first_price: str = "100.2500",
):
    return build_trade_ohlc_hour(
        (
            _record(
                trade_id="first",
                milliseconds=100,
                price=first_price,
                quantity="0.5",
                row_number=0,
            ),
            _record(
                trade_id="high",
                milliseconds=200,
                price="105.5000",
                quantity="0.1",
                row_number=1,
            ),
            _record(
                trade_id="close",
                milliseconds=900,
                price="102.7500",
                quantity="0.2",
                row_number=2,
            ),
            _record(
                trade_id="second-candle",
                milliseconds=2_100,
                price="99.000",
                quantity="1",
                row_number=3,
            ),
        ),
        base="BTC",
        hour_utc=_hour(),
    )


def _combined_content_sha256(
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


def _replace_with_recalculated_hash(
    encoded: EncodedHourlyTradeOHLCBlocks,
    **changes: bytes,
) -> EncodedHourlyTradeOHLCBlocks:
    ohlc = changes.get(
        "ohlc_block",
        encoded.ohlc_block,
    )
    validity = changes.get(
        "validity_block",
        encoded.validity_block,
    )
    trade_count = changes.get(
        "trade_count_block",
        encoded.trade_count_block,
    )

    return EncodedHourlyTradeOHLCBlocks(
        format_version=encoded.format_version,
        codec=encoded.codec,
        observation_count=encoded.observation_count,
        ohlc_block=ohlc,
        validity_block=validity,
        trade_count_block=trade_count,
        content_sha256=_combined_content_sha256(
            ohlc_block=ohlc,
            validity_block=validity,
            trade_count_block=trade_count,
        ),
    )


def test_price_block_encoding_is_deterministic() -> None:
    block = _block()

    first = encode_hourly_trade_ohlc_block(block)
    second = encode_hourly_trade_ohlc_block(block)

    assert first == second
    assert first.format_version == PRICE_BLOCK_FORMAT_VERSION
    assert first.codec == PRICE_BLOCK_CODEC
    assert first.observation_count == PRICE_OBSERVATIONS_PER_HOUR
    assert len(first.content_sha256) == 64

    assert first.ohlc_block
    assert first.validity_block
    assert first.trade_count_block


def test_round_trip_preserves_authoritative_price_channels() -> None:
    block = _block()
    encoded = encode_hourly_trade_ohlc_block(block)
    decoded = decode_hourly_trade_ohlc_blocks(encoded)

    assert decoded.observation_count == 3_600
    assert decoded.valid_count == 2
    assert decoded.invalid_count == 3_598
    assert decoded.total_trade_count == 4

    for index, observation in enumerate(block.observations):
        assert decoded.open[index] == observation.open
        assert decoded.high[index] == observation.high
        assert decoded.low[index] == observation.low
        assert decoded.close[index] == observation.close
        assert decoded.quality[index] is observation.quality
        assert decoded.invalid_reason[index] is (observation.invalid_reason)
        assert decoded.trade_count[index] == (observation.trade_count)


def test_decimal_scale_is_canonicalized() -> None:
    first = encode_hourly_trade_ohlc_block(_block(first_price="100.2500"))
    second = encode_hourly_trade_ohlc_block(_block(first_price="100.25"))

    assert first == second

    decoded = decode_hourly_trade_ohlc_blocks(first)

    assert decoded.open[0] == Decimal("100.25")
    assert decoded.high[0] == Decimal("105.5")
    assert decoded.close[0] == Decimal("102.75")


def test_no_trade_slots_round_trip_as_explicit_invalid() -> None:
    encoded = encode_hourly_trade_ohlc_block(_block())
    decoded = decode_hourly_trade_ohlc_blocks(encoded)

    empty = 1

    assert decoded.quality[empty] is TradeSampleQuality.INVALID
    assert decoded.invalid_reason[empty] is (TradeSampleInvalidReason.NO_TRADES)
    assert decoded.open[empty] is None
    assert decoded.high[empty] is None
    assert decoded.low[empty] is None
    assert decoded.close[empty] is None
    assert decoded.trade_count[empty] == 0


def test_complete_hash_detects_changed_channel() -> None:
    encoded = encode_hourly_trade_ohlc_block(_block())

    changed = bytearray(encoded.ohlc_block)
    changed[-1] ^= 0x01

    with pytest.raises(
        PriceBlockCorruptionError,
        match="content SHA-256",
    ):
        replace(
            encoded,
            ohlc_block=bytes(changed),
        )


def test_payload_hash_is_checked_after_combined_hash() -> None:
    encoded = encode_hourly_trade_ohlc_block(_block())

    changed = bytearray(encoded.ohlc_block)
    changed[-1] ^= 0x01

    forged = _replace_with_recalculated_hash(
        encoded,
        ohlc_block=bytes(changed),
    )

    with pytest.raises(
        PriceBlockCorruptionError,
        match="payload SHA-256",
    ):
        decode_hourly_trade_ohlc_blocks(forged)


def test_channel_swap_is_checked_after_combined_hash() -> None:
    encoded = encode_hourly_trade_ohlc_block(_block())

    forged = _replace_with_recalculated_hash(
        encoded,
        validity_block=encoded.trade_count_block,
        trade_count_block=encoded.validity_block,
    )

    with pytest.raises(
        PriceBlockCodecError,
        match="channel does not match",
    ):
        decode_hourly_trade_ohlc_blocks(forged)


def test_noncanonical_uppercase_content_hash_is_rejected() -> None:
    encoded = encode_hourly_trade_ohlc_block(_block())
    uppercase = encoded.content_sha256.upper()

    assert uppercase != encoded.content_sha256

    with pytest.raises(
        PriceBlockCodecError,
        match="canonical lowercase",
    ):
        replace(
            encoded,
            content_sha256=uppercase,
        )


def test_verify_decodes_every_price_channel() -> None:
    encoded = encode_hourly_trade_ohlc_block(_block())

    verify_hourly_trade_ohlc_blocks(encoded)


def test_quality_summary_json_is_stable() -> None:
    block = _block()

    payload = trade_ohlc_quality_summary_to_dict(block.quality_summary)

    assert payload == {
        "schema": "l2shock.trade_ohlc_quality_summary",
        "schema_version": 1,
        "observation_count": 3_600,
        "valid_count": 2,
        "invalid_count": 3_598,
        "total_trade_count": 4,
        "invalid_reason_counts": {
            "no_trades": 3_598,
        },
    }


def test_price_channel_numeric_identities_are_stable() -> None:
    assert {item.name: int(item) for item in PriceBlockChannel} == {
        "OHLC": 1,
        "VALIDITY": 2,
        "TRADE_COUNT": 3,
    }


def test_price_quality_numeric_identities_are_stable() -> None:
    assert {item.name: int(item) for item in PriceQualityCode} == {
        "VALID": 1,
        "DEGRADED": 2,
        "INVALID": 3,
    }


def test_price_invalid_reason_numeric_identities_are_stable() -> None:
    assert {item.name: int(item) for item in PriceInvalidReasonCode} == {
        "NO_TRADES": 1,
    }


def test_reason_registry_covers_current_trade_reasons() -> None:
    assert {item.name for item in PriceInvalidReasonCode} == {
        reason.name for reason in TradeSampleInvalidReason
    }


def test_encoded_size_is_reasonable_for_sparse_trade_hour() -> None:
    encoded = encode_hourly_trade_ohlc_block(_block())

    component_total = (
        encoded.ohlc_block_size_bytes
        + encoded.validity_block_size_bytes
        + encoded.trade_count_block_size_bytes
    )

    assert encoded.encoded_size_bytes == component_total

    # This is intentionally a broad regression ceiling. It catches accidental
    # uncompressed object/JSON storage without promising a real-market size.
    assert encoded.encoded_size_bytes < 512 * 1024


def test_trade_times_must_belong_to_observation_bucket() -> None:
    from l2shock.price import (
        TradeOHLCError,
        TradeOHLCObservation,
    )

    start = _hour()
    start_ms = _epoch_ms(start)

    with pytest.raises(
        TradeOHLCError,
        match="half-open UTC bucket",
    ):
        TradeOHLCObservation(
            bucket_index=0,
            bucket_start_utc=start,
            bucket_end_utc=start + timedelta(seconds=1),
            quality=TradeSampleQuality.VALID,
            invalid_reason=None,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            close=Decimal("100"),
            trade_count=1,
            first_trade_time_ms=start_ms + 1_000,
            last_trade_time_ms=start_ms + 1_000,
        )


def test_price_arrow_reader_accepts_split_hour_but_rejects_extra_batch() -> None:
    import pyarrow as pa
    import pyarrow.ipc as ipc

    import l2shock.price.block_codec as codec_module

    channel = PriceBlockChannel.TRADE_COUNT
    schema = codec_module._schema_for_channel(channel)

    def payload_for_batch_sizes(*sizes: int) -> bytes:
        sink = pa.BufferOutputStream()
        options = ipc.IpcWriteOptions(
            compression="zstd",
            use_legacy_format=False,
        )

        with ipc.new_stream(sink, schema, options=options) as writer:
            for size in sizes:
                batch = pa.RecordBatch.from_arrays(
                    [pa.array([0] * size, type=pa.uint32())],
                    schema=schema,
                )
                writer.write_batch(batch)

        return sink.getvalue().to_pybytes()

    split_hour = codec_module._read_arrow_table(
        payload_for_batch_sizes(1_800, 1_800),
        channel=channel,
    )
    assert split_hour.num_rows == PRICE_OBSERVATIONS_PER_HOUR

    with pytest.raises(
        PriceBlockCorruptionError,
        match="row count is not 3,600",
    ):
        codec_module._read_arrow_table(
            payload_for_batch_sizes(3_600, 1),
            channel=channel,
        )

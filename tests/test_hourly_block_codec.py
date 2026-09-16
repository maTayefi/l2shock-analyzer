from __future__ import annotations

import hashlib
import struct
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.ingest import (
    OBSERVATIONS_PER_HOUR,
    BookInitializationState,
    BookSampleInvalidReason,
    BookSampleQuality,
    ReplayBookStructure,
)
from l2shock.liquidity import (
    HOURLY_BLOCK_CODEC,
    HOURLY_BLOCK_FORMAT_VERSION,
    DepthBand,
    EncodedHourlyLiquidityBlocks,
    HourlyBlockCodecError,
    HourlyBlockCorruptionError,
    HourlyInvalidReasonCode,
    HourlyLiquidityBlock,
    HourlyLiquidityChannel,
    HourlyQualityCode,
    LiquidityObservation,
    build_liquidity_quality_summary,
    decode_hourly_liquidity_blocks,
    encode_hourly_liquidity_block,
    liquidity_quality_summary_to_dict,
    verify_hourly_liquidity_blocks,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )


def _valid_observation(
    index: int,
    *,
    bid: str = "123456789.123400",
    ask: str = "987654321.567800",
    source_count: int = 1,
) -> LiquidityObservation:
    bucket_start = _hour() + timedelta(seconds=index)

    bid_value = Decimal(bid)
    ask_value = Decimal(ask)
    total = bid_value + ask_value

    return LiquidityObservation(
        bucket_index=index,
        bucket_start_utc=bucket_start,
        bucket_end_utc=bucket_start + timedelta(seconds=1),
        source_received_time_ns=(1_788_350_400_100_000_000 + index),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        replay_valid=True,
        initialization_state=BookInitializationState.SNAPSHOT,
        book_structure=ReplayBookStructure.NORMAL,
        last_update_id=100 + index,
        best_bid=Decimal("100"),
        best_ask=Decimal("101"),
        bid_liquidity=bid_value,
        ask_liquidity=ask_value,
        total_liquidity=total,
        bid_ask_imbalance=((bid_value - ask_value) / total if total != 0 else None),
        bid_depth_level_count=10,
        ask_depth_level_count=11,
        levels_examined=21,
        source_count=source_count,
    )


def _invalid_observation(
    index: int,
    *,
    reason: BookSampleInvalidReason = (BookSampleInvalidReason.REPLAY_INVALIDATED),
) -> LiquidityObservation:
    bucket_start = _hour() + timedelta(seconds=index)

    return LiquidityObservation(
        bucket_index=index,
        bucket_start_utc=bucket_start,
        bucket_end_utc=bucket_start + timedelta(seconds=1),
        source_received_time_ns=None,
        quality=BookSampleQuality.INVALID,
        invalid_reason=reason,
        replay_valid=False,
        initialization_state=BookInitializationState.INVALIDATED,
        book_structure=ReplayBookStructure.EMPTY_BOTH,
        last_update_id=None,
        best_bid=None,
        best_ask=None,
        bid_liquidity=None,
        ask_liquidity=None,
        total_liquidity=None,
        bid_ask_imbalance=None,
        bid_depth_level_count=0,
        ask_depth_level_count=0,
        levels_examined=0,
        source_count=0,
    )


def _block(
    *,
    invalid_from: int | None = None,
) -> HourlyLiquidityBlock:
    observations: list[LiquidityObservation] = []

    for index in range(OBSERVATIONS_PER_HOUR):
        if invalid_from is not None and index >= invalid_from:
            observations.append(_invalid_observation(index))
        else:
            observations.append(
                _valid_observation(
                    index,
                    bid=("123456789.123400" if index % 3 else "0.0300"),
                    ask=("987654321.567800" if index % 5 else "0.0800"),
                )
            )

    values = tuple(observations)

    return HourlyLiquidityBlock(
        spec=_spec(),
        band=DepthBand(
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
        ),
        imbalance_decimal_precision=34,
        observations=values,
        quality_summary=build_liquidity_quality_summary(values),
    )


def _replace_encoded(
    encoded: EncodedHourlyLiquidityBlocks,
    **changes: object,
) -> EncodedHourlyLiquidityBlocks:
    values = {
        "format_version": encoded.format_version,
        "codec": encoded.codec,
        "observation_count": encoded.observation_count,
        "bid_liquidity_block": encoded.bid_liquidity_block,
        "ask_liquidity_block": encoded.ask_liquidity_block,
        "validity_block": encoded.validity_block,
        "source_count_block": encoded.source_count_block,
        "content_sha256": encoded.content_sha256,
    }
    values.update(changes)

    return EncodedHourlyLiquidityBlocks(**values)


def _combined_content_sha256(
    *,
    bid_liquidity_block: bytes,
    ask_liquidity_block: bytes,
    validity_block: bytes,
    source_count_block: bytes,
) -> str:
    """Reproduce the documented format-version-1 combined digest in tests."""
    digest = hashlib.sha256()
    digest.update(b"l2shock/hourly-liquidity-blocks/v1\x00")

    for block in (
        bid_liquidity_block,
        ask_liquidity_block,
        validity_block,
        source_count_block,
    ):
        digest.update(struct.pack(">Q", len(block)))
        digest.update(block)

    return digest.hexdigest()


def _replace_with_recalculated_content_hash(
    encoded: EncodedHourlyLiquidityBlocks,
    **block_changes: bytes,
) -> EncodedHourlyLiquidityBlocks:
    bid = block_changes.get(
        "bid_liquidity_block",
        encoded.bid_liquidity_block,
    )
    ask = block_changes.get(
        "ask_liquidity_block",
        encoded.ask_liquidity_block,
    )
    validity = block_changes.get(
        "validity_block",
        encoded.validity_block,
    )
    source_count = block_changes.get(
        "source_count_block",
        encoded.source_count_block,
    )

    return EncodedHourlyLiquidityBlocks(
        format_version=encoded.format_version,
        codec=encoded.codec,
        observation_count=encoded.observation_count,
        bid_liquidity_block=bid,
        ask_liquidity_block=ask,
        validity_block=validity,
        source_count_block=source_count,
        content_sha256=_combined_content_sha256(
            bid_liquidity_block=bid,
            ask_liquidity_block=ask,
            validity_block=validity,
            source_count_block=source_count,
        ),
    )


def test_encoding_is_deterministic() -> None:
    block = _block(invalid_from=3_000)

    first = encode_hourly_liquidity_block(block)
    second = encode_hourly_liquidity_block(block)

    assert first == second
    assert first.format_version == HOURLY_BLOCK_FORMAT_VERSION
    assert first.codec == HOURLY_BLOCK_CODEC
    assert first.observation_count == 3_600
    assert len(first.content_sha256) == 64

    assert first.bid_liquidity_block
    assert first.ask_liquidity_block
    assert first.validity_block
    assert first.source_count_block


def test_round_trip_preserves_authoritative_channels() -> None:
    block = _block(invalid_from=2_500)
    encoded = encode_hourly_liquidity_block(block)
    decoded = decode_hourly_liquidity_blocks(encoded)

    assert decoded.observation_count == OBSERVATIONS_PER_HOUR
    assert decoded.valid_count == 2_500
    assert decoded.invalid_count == 1_100
    assert decoded.degraded_count == 0

    for index, observation in enumerate(block.observations):
        assert decoded.bid_liquidity[index] == (observation.bid_liquidity)
        assert decoded.ask_liquidity[index] == (observation.ask_liquidity)
        assert decoded.quality[index] is observation.quality
        assert decoded.invalid_reason[index] is (observation.invalid_reason)
        assert decoded.source_count[index] == (observation.source_count)


def test_decimal_scale_is_canonicalized_without_value_loss() -> None:
    block = _block()

    encoded = encode_hourly_liquidity_block(block)
    decoded = decode_hourly_liquidity_blocks(encoded)

    assert decoded.bid_liquidity[0] == Decimal("0.03")
    assert decoded.ask_liquidity[0] == Decimal("0.08")

    assert decoded.bid_liquidity[1] == Decimal("123456789.1234")
    assert decoded.ask_liquidity[1] == Decimal("987654321.5678")


def test_complete_content_hash_detects_changed_channel() -> None:
    encoded = encode_hourly_liquidity_block(_block(invalid_from=3_500))

    changed_bid = bytearray(encoded.bid_liquidity_block)
    changed_bid[-1] ^= 0x01

    with pytest.raises(
        HourlyBlockCorruptionError,
        match="content SHA-256",
    ):
        _replace_encoded(
            encoded,
            bid_liquidity_block=bytes(changed_bid),
        )


def test_individual_payload_corruption_is_detected() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    changed = bytearray(encoded.ask_liquidity_block)
    changed[-1] ^= 0x01

    corrupted_bytes = bytes(changed)

    # Recalculate only the complete four-block hash by constructing a
    # temporary object is intentionally impossible through the public API.
    # Instead decode the raw channel in a forged object whose top-level hash
    # uses the original value; construction must reject it immediately.
    with pytest.raises(HourlyBlockCorruptionError):
        EncodedHourlyLiquidityBlocks(
            format_version=encoded.format_version,
            codec=encoded.codec,
            observation_count=encoded.observation_count,
            bid_liquidity_block=encoded.bid_liquidity_block,
            ask_liquidity_block=corrupted_bytes,
            validity_block=encoded.validity_block,
            source_count_block=encoded.source_count_block,
            content_sha256=encoded.content_sha256,
        )


def test_channel_swap_is_rejected() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    # Rebuilding with swapped channels is rejected first by the complete
    # content hash. Channel identities provide a second independent boundary
    # during decoding.
    with pytest.raises(HourlyBlockCorruptionError):
        EncodedHourlyLiquidityBlocks(
            format_version=encoded.format_version,
            codec=encoded.codec,
            observation_count=encoded.observation_count,
            bid_liquidity_block=encoded.ask_liquidity_block,
            ask_liquidity_block=encoded.bid_liquidity_block,
            validity_block=encoded.validity_block,
            source_count_block=encoded.source_count_block,
            content_sha256=encoded.content_sha256,
        )


def test_verify_decodes_all_channels() -> None:
    encoded = encode_hourly_liquidity_block(_block(invalid_from=1_800))

    verify_hourly_liquidity_blocks(encoded)


def test_quality_summary_json_is_stable_and_complete() -> None:
    block = _block(invalid_from=3_000)

    payload = liquidity_quality_summary_to_dict(block.quality_summary)

    assert payload == {
        "schema": "l2shock.liquidity_quality_summary",
        "schema_version": 1,
        "observation_count": 3_600,
        "valid_count": 3_000,
        "degraded_count": 0,
        "invalid_count": 600,
        "zero_total_liquidity_count": 0,
        "invalid_reason_counts": {
            "replay_invalidated": 600,
        },
    }


def test_encoded_size_is_reasonable_for_repeated_hourly_values() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    # This is deliberately a broad regression ceiling, not a promise that all
    # real hours will compress this well. It primarily catches accidental
    # uncompressed JSON/pickle-like storage or duplicated complete objects.
    assert encoded.encoded_size_bytes < 512 * 1024

    assert encoded.bid_block_size_bytes > 0
    assert encoded.ask_block_size_bytes > 0
    assert encoded.validity_block_size_bytes > 0
    assert encoded.source_count_block_size_bytes > 0


def test_constructor_rejects_invalid_content_hash() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    with pytest.raises(
        HourlyBlockCorruptionError,
        match="content SHA-256",
    ):
        replace(
            encoded,
            content_sha256="0" * 64,
        )


def test_channel_enum_values_are_stable() -> None:
    assert {channel.name: int(channel) for channel in HourlyLiquidityChannel} == {
        "BID_LIQUIDITY": 1,
        "ASK_LIQUIDITY": 2,
        "VALIDITY": 3,
        "SOURCE_COUNT": 4,
    }


def test_encoded_size_report_is_internally_consistent() -> None:
    encoded = encode_hourly_liquidity_block(_block(invalid_from=3_000))

    component_total = (
        encoded.bid_block_size_bytes
        + encoded.ask_block_size_bytes
        + encoded.validity_block_size_bytes
        + encoded.source_count_block_size_bytes
    )

    assert encoded.encoded_size_bytes == component_total


def test_per_channel_payload_hash_is_checked_after_combined_hash() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    changed = bytearray(encoded.ask_liquidity_block)
    changed[-1] ^= 0x01

    forged = _replace_with_recalculated_content_hash(
        encoded,
        ask_liquidity_block=bytes(changed),
    )

    with pytest.raises(
        HourlyBlockCorruptionError,
        match="payload SHA-256",
    ):
        decode_hourly_liquidity_blocks(forged)


def test_channel_identity_is_checked_after_combined_hash() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    forged = _replace_with_recalculated_content_hash(
        encoded,
        bid_liquidity_block=encoded.ask_liquidity_block,
        ask_liquidity_block=encoded.bid_liquidity_block,
    )

    with pytest.raises(
        HourlyBlockCodecError,
        match="channel does not match",
    ):
        decode_hourly_liquidity_blocks(forged)


def test_content_sha256_rejects_noncanonical_uppercase() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    uppercase = encoded.content_sha256.upper()
    assert uppercase != encoded.content_sha256

    with pytest.raises(
        HourlyBlockCodecError,
        match="canonical lowercase",
    ):
        replace(
            encoded,
            content_sha256=uppercase,
        )


def test_quality_code_values_are_stable() -> None:
    assert {item.name: int(item) for item in HourlyQualityCode} == {
        "VALID": 1,
        "DEGRADED": 2,
        "INVALID": 3,
    }


def test_invalid_reason_code_values_are_stable() -> None:
    assert {item.name: int(item) for item in HourlyInvalidReasonCode} == {
        "UNINITIALIZED": 1,
        "REPLAY_INVALIDATED": 2,
        "LOCKED": 3,
        "CROSSED": 4,
        "EMPTY_BID": 5,
        "EMPTY_ASK": 6,
        "EMPTY_BOTH": 7,
    }


def test_invalid_reason_code_registry_covers_current_reasons() -> None:
    assert {item.name for item in HourlyInvalidReasonCode} == {
        reason.name for reason in BookSampleInvalidReason
    }


def test_arrow_payload_trailing_bytes_are_rejected() -> None:
    encoded = encode_hourly_liquidity_block(_block())

    header = struct.Struct(">8sHBBIQ32s")
    original = encoded.bid_liquidity_block

    (
        magic,
        version,
        channel,
        flags,
        observation_count,
        payload_length,
        _payload_digest,
    ) = header.unpack_from(original)

    payload = original[header.size :]

    assert len(payload) == payload_length

    changed_payload = payload + b"\x00"
    changed_channel = (
        header.pack(
            magic,
            version,
            channel,
            flags,
            observation_count,
            len(changed_payload),
            hashlib.sha256(changed_payload).digest(),
        )
        + changed_payload
    )

    forged = _replace_with_recalculated_content_hash(
        encoded,
        bid_liquidity_block=changed_channel,
    )

    with pytest.raises(
        HourlyBlockCorruptionError,
        match="trailing bytes",
    ):
        decode_hourly_liquidity_blocks(forged)

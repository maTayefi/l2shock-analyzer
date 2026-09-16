from __future__ import annotations

import hashlib
import json
import struct
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from l2shock.ingest import (
    CHECKPOINT_FORMAT_VERSION,
    CHECKPOINT_MAGIC,
    MAX_CHECKPOINT_PAYLOAD_BYTES,
    BookSide,
    CheckpointCodecError,
    CheckpointCorruptionError,
    CheckpointLevel,
    CheckpointLimitError,
    OrderBookCheckpoint,
    checkpoint_content_sha256,
    checkpoint_encoding_info,
    decode_checkpoint,
    encode_checkpoint,
    load_checkpoint_file,
    write_checkpoint_file,
)

_HEADER = struct.Struct(">8sHQ32s")


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _level(
    side: BookSide | str,
    price: str,
    quantity: str,
    order_count: int | None = None,
) -> CheckpointLevel:
    return CheckpointLevel(
        side=BookSide(side),
        price=Decimal(price),
        quantity=Decimal(quantity),
        order_count=order_count,
    )


def _checkpoint(
    *,
    levels: tuple[CheckpointLevel, ...] | None = None,
) -> OrderBookCheckpoint:
    return OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(),
        last_update_id=123_456,
        levels=levels
        or (
            _level(
                BookSide.BID,
                "59999.00",
                "2.5000",
                3,
            ),
            _level(
                BookSide.BID,
                "59998.50",
                "4.000",
            ),
            _level(
                BookSide.ASK,
                "60001.00",
                "1.2500",
                2,
            ),
            _level(
                BookSide.ASK,
                "60002.50",
                "3.00",
            ),
        ),
        source_content_sha256="a" * 64,
    )


def test_encoding_is_deterministic() -> None:
    checkpoint = _checkpoint()

    first = encode_checkpoint(checkpoint)
    second = encode_checkpoint(checkpoint)

    assert first == second
    assert first.startswith(CHECKPOINT_MAGIC)

    info = checkpoint_encoding_info(first)

    assert info.format_version == CHECKPOINT_FORMAT_VERSION
    assert info.encoded_size_bytes == len(first)
    assert info.level_count == 4
    assert info.bid_level_count == 2
    assert info.ask_level_count == 2

    assert info.content_sha256 == hashlib.sha256(first).hexdigest()
    assert checkpoint_content_sha256(first) == info.content_sha256


def test_input_level_order_does_not_change_encoding() -> None:
    ordered = _checkpoint()

    reversed_input = _checkpoint(
        levels=tuple(reversed(ordered.levels)),
    )

    assert encode_checkpoint(ordered) == encode_checkpoint(reversed_input)


def test_decimal_scale_does_not_create_multiple_encodings() -> None:
    first = _checkpoint(
        levels=(
            _level("bid", "59999.00", "2.5000"),
            _level("ask", "60001.00", "1.2500"),
        )
    )
    second = _checkpoint(
        levels=(
            _level("ask", "60001", "1.25"),
            _level("bid", "59999", "2.5"),
        )
    )

    assert encode_checkpoint(first) == encode_checkpoint(second)


def test_round_trip_preserves_checkpoint_identity_and_levels() -> None:
    encoded = encode_checkpoint(_checkpoint())
    decoded = decode_checkpoint(encoded)

    assert decoded.provider == "cryptohftdata"
    assert decoded.venue == "binance_futures"
    assert decoded.symbol == "BTCUSDT"
    assert decoded.through_hour_utc == _hour()
    assert decoded.last_update_id == 123_456
    assert decoded.source_content_sha256 == "a" * 64

    assert [
        (level.side, level.price, level.quantity, level.order_count)
        for level in decoded.levels
    ] == [
        (
            BookSide.BID,
            Decimal("59999"),
            Decimal("2.5"),
            3,
        ),
        (
            BookSide.BID,
            Decimal("59998.5"),
            Decimal("4"),
            None,
        ),
        (
            BookSide.ASK,
            Decimal("60001"),
            Decimal("1.25"),
            2,
        ),
        (
            BookSide.ASK,
            Decimal("60002.5"),
            Decimal("3"),
            None,
        ),
    ]


def test_payload_corruption_is_detected() -> None:
    encoded = bytearray(encode_checkpoint(_checkpoint()))
    encoded[-1] ^= 0x01

    with pytest.raises(
        CheckpointCorruptionError,
        match="SHA-256",
    ):
        decode_checkpoint(encoded)


@pytest.mark.parametrize(
    "modified",
    [
        lambda data: data[:-1],
        lambda data: data + b"\x00",
    ],
)
def test_truncation_and_trailing_bytes_are_rejected(
    modified,
) -> None:
    encoded = encode_checkpoint(_checkpoint())

    with pytest.raises(
        CheckpointCorruptionError,
        match="length",
    ):
        decode_checkpoint(modified(encoded))


def test_invalid_magic_is_rejected() -> None:
    encoded = bytearray(encode_checkpoint(_checkpoint()))
    encoded[0:8] = b"NOTCKPT!"

    with pytest.raises(
        CheckpointCorruptionError,
        match="magic",
    ):
        decode_checkpoint(encoded)


def test_unknown_envelope_version_is_rejected() -> None:
    encoded = encode_checkpoint(_checkpoint())
    _magic, _version, length, digest = _HEADER.unpack_from(encoded)
    payload = encoded[_HEADER.size :]

    changed = (
        _HEADER.pack(
            CHECKPOINT_MAGIC,
            CHECKPOINT_FORMAT_VERSION + 1,
            length,
            digest,
        )
        + payload
    )

    with pytest.raises(
        CheckpointCodecError,
        match="Unsupported.*version",
    ):
        decode_checkpoint(changed)


def test_oversized_declared_payload_is_rejected_before_json() -> None:
    encoded = _HEADER.pack(
        CHECKPOINT_MAGIC,
        CHECKPOINT_FORMAT_VERSION,
        MAX_CHECKPOINT_PAYLOAD_BYTES + 1,
        b"\x00" * 32,
    )

    with pytest.raises(
        CheckpointLimitError,
        match="larger",
    ):
        decode_checkpoint(encoded)


def test_duplicate_json_keys_are_rejected() -> None:
    payload = (
        b'{"schema":"l2shock.orderbook_checkpoint",'
        b'"schema":"l2shock.orderbook_checkpoint"}'
    )

    encoded = (
        _HEADER.pack(
            CHECKPOINT_MAGIC,
            CHECKPOINT_FORMAT_VERSION,
            len(payload),
            hashlib.sha256(payload).digest(),
        )
        + payload
    )

    with pytest.raises(
        CheckpointCodecError,
        match="duplicate key",
    ):
        decode_checkpoint(encoded)


def test_noncanonical_payload_whitespace_is_rejected() -> None:
    canonical = encode_checkpoint(_checkpoint())
    payload = canonical[_HEADER.size :]

    decoded_json = json.loads(payload.decode("utf-8"))
    noncanonical_payload = json.dumps(
        decoded_json,
        ensure_ascii=True,
        sort_keys=True,
        indent=2,
    ).encode("utf-8")

    encoded = (
        _HEADER.pack(
            CHECKPOINT_MAGIC,
            CHECKPOINT_FORMAT_VERSION,
            len(noncanonical_payload),
            hashlib.sha256(noncanonical_payload).digest(),
        )
        + noncanonical_payload
    )

    with pytest.raises(
        CheckpointCodecError,
        match="not canonically encoded",
    ):
        decode_checkpoint(encoded)


def test_noncanonical_decimal_text_is_rejected() -> None:
    canonical = encode_checkpoint(_checkpoint())
    payload = json.loads(canonical[_HEADER.size :].decode("utf-8"))

    payload["levels"][0][1] = "59999.00"

    changed_payload = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    encoded = (
        _HEADER.pack(
            CHECKPOINT_MAGIC,
            CHECKPOINT_FORMAT_VERSION,
            len(changed_payload),
            hashlib.sha256(changed_payload).digest(),
        )
        + changed_payload
    )

    with pytest.raises(
        CheckpointCodecError,
        match="canonical decimal",
    ):
        decode_checkpoint(encoded)


def test_noncanonical_level_order_is_rejected() -> None:
    canonical = encode_checkpoint(_checkpoint())
    payload = json.loads(canonical[_HEADER.size :].decode("utf-8"))

    payload["levels"][0], payload["levels"][1] = (
        payload["levels"][1],
        payload["levels"][0],
    )

    changed_payload = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    encoded = (
        _HEADER.pack(
            CHECKPOINT_MAGIC,
            CHECKPOINT_FORMAT_VERSION,
            len(changed_payload),
            hashlib.sha256(changed_payload).digest(),
        )
        + changed_payload
    )

    with pytest.raises(
        CheckpointCodecError,
        match="not canonically encoded",
    ):
        decode_checkpoint(encoded)


def test_checkpoint_file_is_written_and_loaded_atomically(
    tmp_path: Path,
) -> None:
    path = tmp_path / "checkpoints" / "BTCUSDT_12.l2checkpoint"

    info = write_checkpoint_file(
        path,
        _checkpoint(),
    )

    assert path.is_file()
    assert info.encoded_size_bytes == path.stat().st_size
    assert info.content_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()

    loaded = load_checkpoint_file(path)

    assert loaded.symbol == "BTCUSDT"
    assert loaded.last_update_id == 123_456
    assert loaded.bid_level_count == 2
    assert loaded.ask_level_count == 2

    assert not list(path.parent.glob("*.tmp"))
    assert not list(path.parent.glob(".*.tmp"))


def test_existing_checkpoint_is_not_overwritten_by_default(
    tmp_path: Path,
) -> None:
    path = tmp_path / "checkpoint.l2checkpoint"

    write_checkpoint_file(path, _checkpoint())
    original = path.read_bytes()

    replacement = OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(),
        last_update_id=999_999,
        levels=(
            _level("bid", "59000", "1"),
            _level("ask", "61000", "1"),
        ),
    )

    with pytest.raises(
        FileExistsError,
        match="already exists",
    ):
        write_checkpoint_file(path, replacement)

    assert path.read_bytes() == original


def test_explicit_overwrite_replaces_complete_checkpoint(
    tmp_path: Path,
) -> None:
    path = tmp_path / "checkpoint.l2checkpoint"

    write_checkpoint_file(path, _checkpoint())

    replacement = OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(),
        last_update_id=999_999,
        levels=(
            _level("bid", "59000", "1"),
            _level("ask", "61000", "1"),
        ),
    )

    write_checkpoint_file(
        path,
        replacement,
        overwrite=True,
    )

    loaded = load_checkpoint_file(path)

    assert loaded.last_update_id == 999_999
    assert loaded.bid_level_count == 1
    assert loaded.ask_level_count == 1


def test_decoded_checkpoint_keeps_immediate_hour_ownership() -> None:
    from datetime import timedelta

    from l2shock.acquisition import (
        SourceDataKind,
        SourceFileSpec,
    )
    from l2shock.ingest import ReplayContractError

    decoded = decode_checkpoint(encode_checkpoint(_checkpoint()))

    immediate = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour() + timedelta(hours=1),
    )

    decoded.validate_for_source(immediate)

    nonadjacent = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour() + timedelta(hours=2),
    )

    with pytest.raises(
        ReplayContractError,
        match="immediately following",
    ):
        decoded.validate_for_source(nonadjacent)

from __future__ import annotations

import struct

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

import l2shock.arrow_ipc_limits as limits_module
import l2shock.liquidity.block_codec as liquidity_codec
import l2shock.price.block_codec as price_codec


def _payload(
    schema: pa.Schema,
    *,
    sizes: tuple[int, ...] = (3_600,),
    string_value: str = "1",
    compression: str | None = "zstd",
) -> bytes:
    sink = pa.BufferOutputStream()
    options = ipc.IpcWriteOptions(
        compression=compression,
        use_legacy_format=False,
        metadata_version=ipc.MetadataVersion.V5,
    )

    with ipc.new_stream(sink, schema, options=options) as writer:
        for size in sizes:
            columns = [
                pa.array(
                    (
                        [string_value] * size
                        if pa.types.is_string(field.type)
                        else [1] * size
                    ),
                    type=field.type,
                )
                for field in schema
            ]
            writer.write_batch(
                pa.RecordBatch.from_arrays(columns, schema=schema)
            )

    return sink.getvalue().to_pybytes()


def _decode(
    family: str,
    payload: bytes,
    channel,
):
    if family == "liquidity":
        return liquidity_codec._read_arrow_table(payload, channel=channel)
    return price_codec._read_arrow_table(payload, channel=channel)


def _string_channel(family: str):
    if family == "liquidity":
        channel = liquidity_codec.HourlyLiquidityChannel.BID_LIQUIDITY
        return (
            liquidity_codec,
            channel,
            liquidity_codec.HourlyBlockLimitError,
        )

    channel = price_codec.PriceBlockChannel.OHLC
    return price_codec, channel, price_codec.PriceBlockLimitError


@pytest.mark.parametrize("compression", (None, "zstd", "lz4"))
@pytest.mark.parametrize("split", (False, True))
def test_all_current_hourly_channels_remain_readable(
    compression: str | None,
    split: bool,
) -> None:
    sizes = (1_800, 1_800) if split else (3_600,)

    for family, module, channels in (
        (
            "liquidity",
            liquidity_codec,
            tuple(liquidity_codec.HourlyLiquidityChannel),
        ),
        (
            "price",
            price_codec,
            tuple(price_codec.PriceBlockChannel),
        ),
    ):
        for channel in channels:
            schema = module._schema_for_channel(channel)
            payload = _payload(
                schema,
                sizes=sizes,
                compression=compression,
            )
            table = _decode(family, payload, channel)
            assert table.num_rows == 3_600
            assert table.schema.equals(schema, check_metadata=True)


@pytest.mark.parametrize("family", ("liquidity", "price"))
def test_oversized_compressed_strings_are_rejected_before_open_stream(
    family: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, channel, limit_error = _string_channel(family)
    schema = module._schema_for_channel(channel)

    # A few MiB of repeated text, not an unbounded test allocation.
    payload = _payload(schema, string_value="1" * 1_024)

    def forbidden_open(*_args, **_kwargs):
        raise AssertionError("Arrow decoding was admitted before preflight")

    monkeypatch.setattr(module.ipc, "open_stream", forbidden_open)

    with pytest.raises(limit_error, match="decoded buffer"):
        _decode(family, payload, channel)


@pytest.mark.parametrize("family", ("liquidity", "price"))
def test_forged_expansion_prefix_is_rejected_before_open_stream(
    family: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, channel, limit_error = _string_channel(family)
    schema = module._schema_for_channel(channel)
    payload = _payload(schema)

    first = limits_module._frame(memoryview(payload), 0)
    assert first is not None
    second = limits_module._frame(memoryview(payload), first[2])
    assert second is not None

    metadata, body, body_end = second
    flat = limits_module._Flatbuffer(metadata)
    header_field = flat.field(flat.root, 2, 4)
    assert header_field is not None
    batch = flat.indirect(header_field)

    buffer_count = sum(
        3 if pa.types.is_string(field.type) else 2
        for field in schema
    )
    buffers = flat.vector(
        batch,
        2,
        16,
        expected_count=buffer_count,
    )

    # String columns use validity, offsets, then data. Inflate only the
    # declared output size of the first data buffer; do not allocate it.
    descriptor = buffers + 2 * 16
    buffer_offset = struct.unpack_from("<q", metadata, descriptor)[0]
    stored_length = struct.unpack_from("<q", metadata, descriptor + 8)[0]
    assert stored_length >= 8

    changed = bytearray(payload)
    body_start = body_end - len(body)
    struct.pack_into(
        "<q",
        changed,
        body_start + buffer_offset,
        1 << 30,
    )

    def forbidden_open(*_args, **_kwargs):
        raise AssertionError("Forged expansion reached Arrow decoding")

    monkeypatch.setattr(module.ipc, "open_stream", forbidden_open)

    with pytest.raises(limit_error, match="decoded buffer"):
        _decode(family, bytes(changed), channel)


@pytest.mark.parametrize("family", ("liquidity", "price"))
def test_individual_string_length_is_checked_before_python_materialization(
    family: str,
) -> None:
    module, channel, limit_error = _string_channel(family)
    schema = module._schema_for_channel(channel)
    sink = pa.BufferOutputStream()

    columns = [
        pa.array(
            ["1" * 257] + ["1"] * 3_599,
            type=field.type,
        )
        for field in schema
    ]

    with ipc.new_stream(
        sink,
        schema,
        options=ipc.IpcWriteOptions(compression="zstd"),
    ) as writer:
        writer.write_batch(
            pa.RecordBatch.from_arrays(columns, schema=schema)
        )

    with pytest.raises(limit_error, match="string exceeds"):
        _decode(family, sink.getvalue().to_pybytes(), channel)


@pytest.mark.parametrize("family", ("liquidity", "price"))
def test_exact_decimal_length_limit_remains_accepted(family: str) -> None:
    module, channel, _limit_error = _string_channel(family)
    schema = module._schema_for_channel(channel)
    payload = _payload(schema, string_value="1" * 256)

    assert _decode(family, payload, channel).num_rows == 3_600


@pytest.mark.parametrize("family", ("liquidity", "price"))
def test_extra_batch_is_rejected_before_open_stream(
    family: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, channel, _limit_error = _string_channel(family)
    schema = module._schema_for_channel(channel)
    payload = _payload(schema, sizes=(3_600, 1))

    def forbidden_open(*_args, **_kwargs):
        raise AssertionError("Extra rows reached Arrow decoding")

    monkeypatch.setattr(module.ipc, "open_stream", forbidden_open)

    with pytest.raises(
        (
            liquidity_codec.HourlyBlockCorruptionError
            if family == "liquidity"
            else price_codec.PriceBlockCorruptionError
        ),
        match="row count is not 3,600",
    ):
        _decode(family, payload, channel)


@pytest.mark.parametrize("family", ("liquidity", "price"))
def test_trailing_bytes_are_still_rejected(family: str) -> None:
    module, channel, _limit_error = _string_channel(family)
    schema = module._schema_for_channel(channel)
    payload = _payload(schema) + b"\x00"

    with pytest.raises(
        (
            liquidity_codec.HourlyBlockCorruptionError
            if family == "liquidity"
            else price_codec.PriceBlockCorruptionError
        )
    ):
        _decode(family, payload, channel)
"""Allocation preflight for the project's flat hourly Arrow IPC channels.

Inspect immutable framing, FlatBuffer metadata, and compressed-buffer size
prefixes before Arrow record-batch decompression. This is not a general-purpose
IPC parser: only the flat string and unsigned-integer hourly layouts are
supported.

Decoded string lengths are checked from offsets before to_pylist() is used.
"""

from __future__ import annotations

import struct

import pyarrow as pa


_MAX_METADATA_BYTES = 64 * 1024
_MAX_BATCHES = 4_096
_MAX_DECODED_BYTES = 8 * 1024 * 1024
_MAX_PAYLOAD_BYTES = 64 * 1024 * 1024


class IPCLayoutError(ValueError):
    """Hourly IPC framing or layout is unsupported or malformed."""


class IPCAllocationLimitError(IPCLayoutError):
    """Hourly IPC declares or contains an excessive decoded representation."""


def _read(
    data: memoryview,
    offset: int,
    format_string: str,
) -> int:
    size = struct.calcsize(format_string)
    if offset < 0 or offset > len(data) - size:
        raise IPCLayoutError("Hourly IPC metadata is truncated")
    return int(struct.unpack_from(format_string, data, offset)[0])


class _Flatbuffer:
    """Bounds-checked access to the few tables used by hourly IPC."""

    def __init__(self, data: memoryview) -> None:
        self.data = data
        self.root = _read(data, 0, "<I")
        self._table(self.root)

    def _table(self, table: int) -> tuple[int, int, int]:
        displacement = _read(self.data, table, "<i")
        vtable = table - displacement
        vtable_size = _read(self.data, vtable, "<H")
        object_size = _read(self.data, vtable + 2, "<H")

        if (
            vtable_size < 4
            or vtable_size % 2
            or vtable < 0
            or vtable + vtable_size > len(self.data)
            or object_size < 4
            or table < 0
            or table + object_size > len(self.data)
        ):
            raise IPCLayoutError("Hourly IPC has an invalid FlatBuffer table")

        return vtable, vtable_size, object_size

    def field(
        self,
        table: int,
        index: int,
        width: int,
    ) -> int | None:
        vtable, vtable_size, object_size = self._table(table)
        entry = 4 + 2 * index

        if entry + 2 > vtable_size:
            return None

        relative = _read(self.data, vtable + entry, "<H")
        if relative == 0:
            return None

        if relative < 4 or relative + width > object_size:
            raise IPCLayoutError("Hourly IPC has an invalid field offset")

        return table + relative

    def scalar(
        self,
        table: int,
        index: int,
        format_string: str,
        default: int = 0,
    ) -> int:
        position = self.field(table, index, struct.calcsize(format_string))
        return (
            default
            if position is None
            else _read(self.data, position, format_string)
        )

    def indirect(self, position: int) -> int:
        distance = _read(self.data, position, "<I")
        target = position + distance

        if distance == 0 or target < 0 or target >= len(self.data):
            raise IPCLayoutError("Hourly IPC has an invalid indirect offset")

        return target

    def vector(
        self,
        table: int,
        index: int,
        item_size: int,
        *,
        expected_count: int,
    ) -> int:
        position = self.field(table, index, 4)
        if position is None:
            raise IPCLayoutError("Hourly IPC is missing a required vector")

        vector = self.indirect(position)
        count = _read(self.data, vector, "<I")

        if count != expected_count:
            raise IPCLayoutError("Hourly IPC has an unexpected vector count")

        start = vector + 4
        if start + count * item_size > len(self.data):
            raise IPCLayoutError("Hourly IPC vector is truncated")

        return start


def _frame(
    payload: memoryview,
    position: int,
) -> tuple[memoryview, memoryview, int] | None:
    """Return one metadata/body pair without decoding its body."""

    if position == len(payload):
        return None

    length = _read(payload, position, "<I")
    position += 4

    if length == 0xFFFFFFFF:
        length = _read(payload, position, "<I")
        position += 4

    if length == 0:
        if position != len(payload):
            raise IPCLayoutError("Hourly Arrow IPC payload contains trailing bytes")
        return None

    if length > _MAX_METADATA_BYTES:
        raise IPCAllocationLimitError("Hourly IPC metadata exceeds its size limit")

    metadata_end = position + length
    if metadata_end > len(payload):
        raise IPCLayoutError("Hourly IPC metadata is truncated")

    metadata = payload[position:metadata_end]
    flat = _Flatbuffer(metadata)
    body_length = flat.scalar(flat.root, 3, "<q")

    if body_length < 0 or body_length > len(payload) - metadata_end:
        raise IPCLayoutError("Hourly IPC body length is invalid")

    body_end = metadata_end + body_length
    return metadata, payload[metadata_end:body_end], body_end


def _buffer_limits(
    schema: pa.Schema,
    rows: int,
    max_decimal_bytes: int,
) -> list[int]:
    limits: list[int] = []
    bitmap_bytes = (rows + 7) // 8

    for field in schema:
        # Permit ordinary alignment padding without permitting arbitrary
        # oversized backing buffers.
        limits.append(bitmap_bytes + 64)

        if pa.types.is_string(field.type):
            limits.extend(
                (
                    4 * (rows + 1) + 64,
                    rows * max_decimal_bytes + 64,
                )
            )
        elif pa.types.is_unsigned_integer(field.type):
            limits.append(rows * (field.type.bit_width // 8) + 64)
        else:
            raise IPCLayoutError("Unsupported hourly IPC field type")

    return limits


def preflight_hourly_ipc(
    payload: bytes,
    *,
    schema: pa.Schema,
    expected_rows: int,
    max_decimal_bytes: int,
) -> None:
    """Reject excessive expansion before record-batch decoding begins."""

    if not isinstance(payload, bytes):
        raise TypeError("Hourly IPC payload must be bytes")

    if len(payload) > _MAX_PAYLOAD_BYTES:
        raise IPCAllocationLimitError("Hourly IPC payload exceeds its size limit")

    if (
        isinstance(expected_rows, bool)
        or not isinstance(expected_rows, int)
        or expected_rows <= 0
        or isinstance(max_decimal_bytes, bool)
        or not isinstance(max_decimal_bytes, int)
        or max_decimal_bytes <= 0
    ):
        raise ValueError("Hourly IPC limits must be positive integers")

    data = memoryview(payload)
    position = 0
    saw_schema = False
    batch_count = 0
    row_count = 0
    decoded_bytes = 0

    while True:
        framed = _frame(data, position)
        if framed is None:
            break

        metadata, body, position = framed
        flat = _Flatbuffer(metadata)
        message = flat.root

        # FlatBuffers enum values: MetadataVersion V5=4; MessageHeader
        # Schema=1; DictionaryBatch=2; RecordBatch=3.
        version = flat.scalar(message, 0, "<h")
        kind = flat.scalar(message, 1, "<B")

        if version != 4:
            raise IPCLayoutError("Hourly IPC requires V5 metadata")

        if not saw_schema:
            if kind != 1 or len(body) != 0:
                raise IPCLayoutError("Hourly IPC must begin with a schema message")
            saw_schema = True
            continue

        if kind != 3:
            raise IPCLayoutError("Unsupported message in hourly IPC stream")

        batch_count += 1
        if batch_count > _MAX_BATCHES:
            raise IPCAllocationLimitError("Hourly IPC contains too many batches")

        header_field = flat.field(message, 2, 4)
        if header_field is None:
            raise IPCLayoutError("Hourly IPC record batch has no header")

        batch = flat.indirect(header_field)
        rows = flat.scalar(batch, 0, "<q")

        if rows < 0 or rows > expected_rows - row_count:
            raise IPCLayoutError("Decoded hourly IPC row count is not 3,600")

        row_count += rows

        nodes = flat.vector(
            batch,
            1,
            16,
            expected_count=len(schema),
        )

        for index, field in enumerate(schema):
            node = nodes + 16 * index
            length = _read(metadata, node, "<q")
            null_count = _read(metadata, node + 8, "<q")

            if (
                length != rows
                or null_count < 0
                or null_count > rows
                or (not field.nullable and null_count != 0)
            ):
                raise IPCLayoutError("Hourly IPC field-node ownership is invalid")

        limits = _buffer_limits(schema, rows, max_decimal_bytes)
        buffers = flat.vector(
            batch,
            2,
            16,
            expected_count=len(limits),
        )

        compression_field = flat.field(batch, 3, 4)
        compressed = compression_field is not None

        if compressed:
            compression = flat.indirect(compression_field)
            codec = flat.scalar(compression, 0, "<B")
            method = flat.scalar(compression, 1, "<B")
            if codec not in (0, 1) or method != 0:
                raise IPCLayoutError("Unsupported hourly IPC compression")

        previous_end = 0

        for index, maximum in enumerate(limits):
            descriptor = buffers + 16 * index
            offset = _read(metadata, descriptor, "<q")
            stored_length = _read(metadata, descriptor + 8, "<q")

            if (
                offset < 0
                or offset % 8
                or stored_length < 0
                or offset > len(body)
                or stored_length > len(body) - offset
            ):
                raise IPCLayoutError("Hourly IPC buffer bounds are invalid")

            if stored_length == 0:
                decoded_length = 0
            else:
                if offset < previous_end:
                    raise IPCLayoutError("Hourly IPC buffers overlap")

                previous_end = offset + stored_length

                if compressed:
                    if stored_length < 8:
                        raise IPCLayoutError("Hourly IPC compression prefix is truncated")

                    declared_length = _read(body, offset, "<q")

                    if declared_length == -1:
                        decoded_length = stored_length - 8
                    elif declared_length < 0:
                        raise IPCLayoutError("Hourly IPC decoded length is negative")
                    else:
                        decoded_length = declared_length
                else:
                    decoded_length = stored_length

            if decoded_length > maximum:
                raise IPCAllocationLimitError(
                    "Hourly IPC decoded buffer exceeds its representation limit"
                )

            decoded_bytes += decoded_length

            if decoded_bytes > _MAX_DECODED_BYTES:
                raise IPCAllocationLimitError(
                    "Hourly IPC cumulative decoded buffers exceed their limit"
                )

    if not saw_schema or row_count != expected_rows:
        raise IPCLayoutError("Decoded hourly IPC row count is not 3,600")


def validate_decimal_string_offsets(
    batch: pa.RecordBatch,
    *,
    max_decimal_bytes: int,
) -> None:
    """Bound each string slot before conversion into Python strings."""

    # Full validation checks buffer/offset ownership and UTF-8 validity.
    # Expansion has already been bounded by preflight_hourly_ipc.
    batch.validate(full=True)

    for field, column in zip(batch.schema, batch.columns, strict=True):
        if not pa.types.is_string(field.type):
            continue

        offsets_buffer = column.buffers()[1]
        if offsets_buffer is None:
            raise IPCLayoutError("Hourly decimal string offsets are missing")

        offsets = memoryview(offsets_buffer)
        start = column.offset

        for index in range(len(column)):
            left = _read(offsets, 4 * (start + index), "<i")
            right = _read(offsets, 4 * (start + index + 1), "<i")

            if left < 0 or right < left:
                raise IPCLayoutError("Hourly decimal string offsets are invalid")

            if right - left > max_decimal_bytes:
                raise IPCAllocationLimitError(
                    "Hourly decimal string exceeds its encoded length limit"
                )
# l2shock/ingest/parquet_reader.py
"""Streamed readers for normalized CryptoHFTData Parquet archives.

This module interprets structurally valid hourly source files without loading
an entire hour into pandas or constructing an entire in-memory event list.

Important boundaries:

- order-book rows are grouped into complete exchange events;
- one event may span PyArrow record-batch or Parquet row-group boundaries;
- timestamp units are explicit for the observed Binance Futures contract;
- decimal source values remain exact ``Decimal`` objects;
- this module reports sequence and ordering facts but does not reconstruct a
  book and does not declare analytical liquidity validity.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, TypeAlias

import pyarrow as pa
import pyarrow.parquet as pq

from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.ingest.venue_adapter import (
    VenueSequenceContractError,
    orderbook_sequence_contract,
)

DEFAULT_BATCH_SIZE: Final[int] = 65_536
DEFAULT_MAX_EVENT_ROWS: Final[int] = 1_000_000
DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS: Final[int] = 4_096
MAX_RETAINED_DIAGNOSTIC_ISSUES: Final[int] = 100

_ORDERBOOK_COLUMNS: Final[tuple[str, ...]] = (
    "received_time",
    "event_time",
    "transaction_time",
    "symbol",
    "event_type",
    "first_update_id",
    "final_update_id",
    "prev_final_update_id",
    "last_update_id",
    "side",
    "price",
    "quantity",
    "order_count",
)

_TRADE_COLUMNS: Final[tuple[str, ...]] = (
    "received_time",
    "event_time",
    "symbol",
    "trade_id",
    "price",
    "quantity",
    "trade_time",
    "is_buyer_maker",
    "order_type",
)

# Required-column sets used by _validate_projected_columns.
# _ORDERBOOK_REQUIRED_COLUMNS was referenced but never defined.
_ORDERBOOK_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(_ORDERBOOK_COLUMNS)
_TRADES_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(_TRADE_COLUMNS)


class OrderBookEventType(StrEnum):
    """Normalized event types currently accepted from order-book archives."""

    SNAPSHOT = "snapshot"
    UPDATE = "update"


class BookSide(StrEnum):
    BID = "bid"
    ASK = "ask"


class StreamedParquetReadError(ValueError):
    """A source row or event violates the streamed-reader contract."""

    def __init__(
        self,
        message: str,
        *,
        path: Path | None = None,
        row_number: int | None = None,
    ) -> None:
        self.path = Path(path) if path is not None else None
        self.row_number = row_number

        context: list[str] = []

        if self.path is not None:
            context.append(f"file={self.path.name!r}")

        if row_number is not None:
            context.append(f"row={row_number}")

        suffix = f" ({', '.join(context)})" if context else ""
        super().__init__(f"{message}{suffix}")


class StreamedParquetReadCancelled(StreamedParquetReadError):
    """A synchronous streamed read was cooperatively cancelled."""


CancellationProbe: TypeAlias = Callable[[], bool]


@dataclass(frozen=True, slots=True)
class OrderBookLevelChange:
    """One exact price-level mutation belonging to an exchange event."""

    side: BookSide
    price: Decimal
    quantity: Decimal
    order_count: int | None


@dataclass(frozen=True, slots=True)
class OrderBookEvent:
    """One normalized order-book event assembled from one or more rows."""

    symbol: str
    event_type: OrderBookEventType

    # Explicit provider-unit fields. Do not infer these by magnitude here.
    received_time_ns: int
    event_time_ms: int
    transaction_time_ms: int | None

    first_update_id: int | None
    final_update_id: int | None
    prev_final_update_id: int | None
    last_update_id: int | None

    changes: tuple[OrderBookLevelChange, ...]

    first_row_number: int
    last_row_number: int

    @property
    def row_count(self) -> int:
        return self.last_row_number - self.first_row_number + 1

    @property
    def received_at_utc(self) -> datetime:
        return _epoch_ns_to_utc(self.received_time_ns)

    @property
    def event_at_utc(self) -> datetime:
        return _epoch_ms_to_utc(self.event_time_ms)

    @property
    def transaction_at_utc(self) -> datetime | None:
        if self.transaction_time_ms is None:
            return None

        return _epoch_ms_to_utc(self.transaction_time_ms)


@dataclass(frozen=True, slots=True)
class TradeRecord:
    """One normalized Binance Futures trade row."""

    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    received_time_ns: int
    event_time_ms: int
    trade_time_ms: int
    is_buyer_maker: bool
    order_type: str | None
    row_number: int

    @property
    def received_at_utc(self) -> datetime:
        return _epoch_ns_to_utc(self.received_time_ns)

    @property
    def event_at_utc(self) -> datetime:
        return _epoch_ms_to_utc(self.event_time_ms)

    @property
    def traded_at_utc(self) -> datetime:
        return _epoch_ms_to_utc(self.trade_time_ms)


@dataclass(frozen=True, slots=True)
class StreamOrderingIssue:
    """One bounded ordering or continuity diagnostic."""

    kind: str
    previous_row_number: int
    current_row_number: int
    previous_value: int | str | None
    current_value: int | str | None


@dataclass(frozen=True, slots=True)
class OrderBookReadReport:
    """Facts discovered while streaming one order-book archive."""

    path: Path
    symbol: str
    rows_read: int
    events_read: int
    update_event_count: int
    snapshot_event_count: int

    first_received_time_ns: int | None
    last_received_time_ns: int | None
    first_event_time_ms: int | None
    last_event_time_ms: int | None

    continuity_checks: int
    continuity_mismatch_count: int
    received_time_regression_count: int
    event_time_regression_count: int
    update_id_regression_count: int

    retained_issues: tuple[StreamOrderingIssue, ...]
    retained_issue_limit: int
    batch_size: int

    @property
    def has_snapshot(self) -> bool:
        return self.snapshot_event_count > 0

    @property
    def has_continuity_mismatch(self) -> bool:
        return self.continuity_mismatch_count > 0

    @property
    def has_ordering_regression(self) -> bool:
        return any(
            (
                self.received_time_regression_count,
                self.event_time_regression_count,
                self.update_id_regression_count,
            )
        )


@dataclass(frozen=True, slots=True)
class TradeReadReport:
    """Facts discovered while streaming one trade archive."""

    path: Path
    symbol: str
    rows_read: int

    first_received_time_ns: int | None
    last_received_time_ns: int | None
    first_trade_time_ms: int | None
    last_trade_time_ms: int | None

    received_time_regression_count: int
    trade_time_regression_count: int
    adjacent_duplicate_trade_id_count: int

    retained_issues: tuple[StreamOrderingIssue, ...]
    retained_issue_limit: int
    batch_size: int

    @property
    def has_ordering_regression(self) -> bool:
        return any(
            (
                self.received_time_regression_count,
                self.trade_time_regression_count,
            )
        )


OrderBookConsumer: TypeAlias = Callable[[OrderBookEvent], None]
TradeConsumer: TypeAlias = Callable[[TradeRecord], None]
SourceReadReport: TypeAlias = OrderBookReadReport | TradeReadReport


@dataclass(slots=True)
class _OrderBookEventBuilder:
    key: tuple[Any, ...]
    symbol: str
    event_type: OrderBookEventType
    received_time_ns: int
    event_time_ms: int
    transaction_time_ms: int | None
    first_update_id: int | None
    final_update_id: int | None
    prev_final_update_id: int | None
    last_update_id: int | None
    changes: list[OrderBookLevelChange]
    level_identities: set[tuple[BookSide, Decimal]]
    first_row_number: int
    last_row_number: int

    def append(
        self,
        change: OrderBookLevelChange,
        *,
        row_number: int,
        max_event_rows: int,
        path: Path,
    ) -> None:
        identity = (change.side, change.price)

        if identity in self.level_identities:
            raise StreamedParquetReadError(
                "One order-book event contains more than one mutation for "
                f"{change.side.value} price {change.price}",
                path=path,
                row_number=row_number,
            )

        self.level_identities.add(identity)
        self.changes.append(change)
        self.last_row_number = row_number

        if len(self.changes) > max_event_rows:
            raise StreamedParquetReadError(
                "One order-book event exceeded the configured row limit "
                f"of {max_event_rows}",
                path=path,
                row_number=row_number,
            )

    def freeze(self) -> OrderBookEvent:
        return OrderBookEvent(
            symbol=self.symbol,
            event_type=self.event_type,
            received_time_ns=self.received_time_ns,
            event_time_ms=self.event_time_ms,
            transaction_time_ms=self.transaction_time_ms,
            first_update_id=self.first_update_id,
            final_update_id=self.final_update_id,
            prev_final_update_id=self.prev_final_update_id,
            last_update_id=self.last_update_id,
            changes=tuple(self.changes),
            first_row_number=self.first_row_number,
            last_row_number=self.last_row_number,
        )


def _epoch_ns_to_utc(value: int) -> datetime:
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    result = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return result.replace(microsecond=nanoseconds // 1_000)


def _epoch_ms_to_utc(value: int) -> datetime:
    seconds, milliseconds = divmod(value, 1_000)
    result = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return result.replace(microsecond=milliseconds * 1_000)


def _positive_configuration_integer(
    name: str,
    value: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")

    if value <= 0:
        raise ValueError(f"{name} must be positive")

    return value


def _check_cancellation(
    cancellation_probe: CancellationProbe | None,
    *,
    path: Path,
    row_number: int | None,
) -> None:
    """Raise when a synchronous caller requests cooperative cancellation."""
    if cancellation_probe is None:
        return

    try:
        requested = cancellation_probe()
    except StreamedParquetReadCancelled:
        raise
    except Exception as exc:
        raise StreamedParquetReadError(
            "Cancellation probe failed",
            path=path,
            row_number=row_number,
        ) from exc

    if not isinstance(requested, bool):
        raise StreamedParquetReadError(
            "Cancellation probe must return bool",
            path=path,
            row_number=row_number,
        )

    if requested:
        raise StreamedParquetReadCancelled(
            "Streamed Parquet reading was cancelled",
            path=path,
            row_number=row_number,
        )


def _required_text(
    value: object,
    *,
    field_name: str,
    path: Path,
    row_number: int,
) -> str:
    if not isinstance(value, str):
        raise StreamedParquetReadError(
            f"{field_name} must be a string",
            path=path,
            row_number=row_number,
        )

    normalized = value.strip()

    if not normalized:
        raise StreamedParquetReadError(
            f"{field_name} cannot be blank",
            path=path,
            row_number=row_number,
        )

    return normalized


def _nullable_text(
    value: object,
    *,
    field_name: str,
    path: Path,
    row_number: int,
) -> str | None:
    if value is None:
        return None

    normalized = _required_text(
        value,
        field_name=field_name,
        path=path,
        row_number=row_number,
    )
    return normalized


import logging as _logging  # add at top of file if not already imported

_integer_log = _logging.getLogger(__name__)


def _integer(
    value: object,
    *,
    field_name: str,
    path: Path,
    row_number: int,
    nullable: bool = False,
    nonnegative: bool = True,
) -> int | None:
    if value is None:
        if nullable:
            return None
        raise StreamedParquetReadError(
            f"{field_name} cannot be null",
            path=path,
            row_number=row_number,
        )
    if isinstance(value, bool):
        raise StreamedParquetReadError(
            f"{field_name} must be an integer, not boolean",
            path=path,
            row_number=row_number,
        )
    parsed: int
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float):
        if math.isnan(value) and nullable:
            return None
        if not math.isfinite(value) or not value.is_integer():
            raise StreamedParquetReadError(
                f"{field_name} must be an exact integer",
                path=path,
                row_number=row_number,
            )
        parsed = int(value)
    else:
        raise StreamedParquetReadError(
            f"{field_name} must be stored as an integer",
            path=path,
            row_number=row_number,
        )
    if nonnegative and parsed < 0:
        # --- DIAGNOSTIC LOGGING: capture overflow details for CI ---
        _integer_log.error(
            "NEGATIVE INTEGER DETECTED: field=%s value=%r type=%s "
            "row=%d path=%s — this likely indicates int32 overflow "
            "in the source Parquet file (OKX sequence IDs exceed 2^31)",
            field_name,
            value,
            type(value).__name__,
            row_number,
            path.name,
        )
        raise StreamedParquetReadError(
            f"{field_name} cannot be negative "
            f"(raw_value={value!r}, row={row_number}, "
            f"file={path.name})",
            path=path,
            row_number=row_number,
        )
    return parsed


def _decimal_string(
    value: object,
    *,
    field_name: str,
    path: Path,
    row_number: int,
    allow_zero: bool,
) -> Decimal:
    if not isinstance(value, str):
        raise StreamedParquetReadError(
            f"{field_name} must be stored as a decimal string",
            path=path,
            row_number=row_number,
        )

    text = value.strip()

    if not text:
        raise StreamedParquetReadError(
            f"{field_name} cannot be blank",
            path=path,
            row_number=row_number,
        )

    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise StreamedParquetReadError(
            f"{field_name} is not a valid decimal string",
            path=path,
            row_number=row_number,
        ) from exc

    if not parsed.is_finite():
        raise StreamedParquetReadError(
            f"{field_name} must be finite",
            path=path,
            row_number=row_number,
        )

    if parsed < 0 or (not allow_zero and parsed == 0):
        relation = "non-negative" if allow_zero else "positive"
        raise StreamedParquetReadError(
            f"{field_name} must be {relation}",
            path=path,
            row_number=row_number,
        )

    return parsed


def _boolean(
    value: object,
    *,
    field_name: str,
    path: Path,
    row_number: int,
) -> bool:
    if not isinstance(value, bool):
        raise StreamedParquetReadError(
            f"{field_name} must be boolean",
            path=path,
            row_number=row_number,
        )

    return value


def _normalized_symbol(
    value: object,
    *,
    spec: SourceFileSpec,
    path: Path,
    row_number: int,
) -> str:
    symbol = _required_text(
        value,
        field_name="symbol",
        path=path,
        row_number=row_number,
    ).upper()

    if symbol != spec.symbol:
        raise StreamedParquetReadError(
            f"Row symbol {symbol!r} does not match expected " f"symbol {spec.symbol!r}",
            path=path,
            row_number=row_number,
        )

    return symbol


def _event_type(
    value: object,
    *,
    path: Path,
    row_number: int,
) -> OrderBookEventType:
    text = _required_text(
        value,
        field_name="event_type",
        path=path,
        row_number=row_number,
    ).lower()

    try:
        return OrderBookEventType(text)
    except ValueError as exc:
        raise StreamedParquetReadError(
            f"Unsupported order-book event_type {text!r}",
            path=path,
            row_number=row_number,
        ) from exc


def _book_side(
    value: object,
    *,
    path: Path,
    row_number: int,
) -> BookSide:
    text = _required_text(
        value,
        field_name="side",
        path=path,
        row_number=row_number,
    ).lower()

    try:
        return BookSide(text)
    except ValueError as exc:
        raise StreamedParquetReadError(
            f"Unsupported order-book side {text!r}",
            path=path,
            row_number=row_number,
        ) from exc


def _validate_projected_columns(
    parquet_file: pq.ParquetFile,
    *,
    required_columns: tuple[str, ...],
    path: Path,
) -> None:
    actual = set(parquet_file.schema_arrow.names)
    missing = sorted(set(required_columns) - actual)

    if missing:
        raise StreamedParquetReadError(
            f"Parquet archive is missing required columns: {missing}",
            path=path,
        )


def _iter_projected_rows(
    parquet_file: pq.ParquetFile,
    *,
    columns: tuple[str, ...],
    batch_size: int,
) -> Iterator[tuple[object, ...]]:
    """Yield projected rows as positional tuples in ``columns`` order.

    The per-row dictionary materialization method on RecordBatch creates one
    Python dictionary per physical Parquet row. Large order-book archives
    contain millions of rows, so those short-lived dictionaries create
    substantial allocation and garbage-collection overhead.

    Converting once to column-owned Python lists via ``to_pydict`` and zipping
    those lists keeps the same PyArrow scalar conversion while allocating only
    one positional tuple per row.
    """
    for batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=list(columns),
        use_threads=True,
    ):
        projected = batch.to_pydict()
        try:
            ordered_columns = tuple(projected[name] for name in columns)
        except KeyError as exc:
            raise RuntimeError(
                "Projected Parquet batch does not contain every requested column"
            ) from exc
        yield from zip(
            *ordered_columns,
            strict=True,
        )


def _append_issue(
    issues: list[StreamOrderingIssue],
    issue: StreamOrderingIssue,
) -> None:
    if len(issues) < MAX_RETAINED_DIAGNOSTIC_ISSUES:
        issues.append(issue)


def _build_event_row(
    row: tuple[object, ...],
    *,
    spec: SourceFileSpec,
    sequence_contract: Any,
    path: Path,
    row_number: int,
) -> tuple[tuple[Any, ...], dict[str, Any], OrderBookLevelChange]:
    if len(row) != len(_ORDERBOOK_COLUMNS):
        raise StreamedParquetReadError(
            "Projected order-book row has an unexpected column count",
            path=path,
            row_number=row_number,
        )

    (
        raw_received_time,
        raw_event_time,
        raw_transaction_time,
        raw_symbol,
        raw_event_type,
        raw_first_update_id,
        raw_final_update_id,
        raw_prev_final_update_id,
        raw_last_update_id,
        raw_side,
        raw_price,
        raw_quantity,
        raw_order_count,
    ) = row

    symbol = _normalized_symbol(
        raw_symbol,
        spec=spec,
        path=path,
        row_number=row_number,
    )
    event_type = _event_type(
        raw_event_type,
        path=path,
        row_number=row_number,
    )

    received_time_ns = _integer(
        raw_received_time,
        field_name="received_time",
        path=path,
        row_number=row_number,
    )
    event_time_ms = _integer(
        raw_event_time,
        field_name="event_time",
        path=path,
        row_number=row_number,
    )

    normalized_transaction_time = raw_transaction_time

    # Empirically observed CryptoHFTData Binance opening snapshots may omit
    # transaction_time even though Binance updates require it. For snapshot
    # rows only, event_time owns the deterministic compatibility fallback.
    #
    # This normalization is performed in memory. It does not rewrite the
    # original source archive or alter update-event strictness.
    if (
        spec.venue == "binance_futures"
        and event_type is OrderBookEventType.SNAPSHOT
        and normalized_transaction_time is None
    ):
        normalized_transaction_time = event_time_ms

    transaction_time_ms = _integer(
        normalized_transaction_time,
        field_name="transaction_time",
        path=path,
        row_number=row_number,
        nullable=not sequence_contract.transaction_time_required,
    )

    first_update_id = _integer(
        raw_first_update_id,
        field_name="first_update_id",
        path=path,
        row_number=row_number,
        nullable=True,
    )
    final_update_id = _integer(
        raw_final_update_id,
        field_name="final_update_id",
        path=path,
        row_number=row_number,
        nullable=True,
    )
    prev_final_update_id = _integer(
        raw_prev_final_update_id,
        field_name="prev_final_update_id",
        path=path,
        row_number=row_number,
        nullable=True,
    )
    last_update_id = _integer(
        raw_last_update_id,
        field_name="last_update_id",
        path=path,
        row_number=row_number,
        nullable=True,
    )

    side = _book_side(
        raw_side,
        path=path,
        row_number=row_number,
    )
    price = _decimal_string(
        raw_price,
        field_name="price",
        path=path,
        row_number=row_number,
        allow_zero=False,
    )
    quantity = _decimal_string(
        raw_quantity,
        field_name="quantity",
        path=path,
        row_number=row_number,
        allow_zero=True,
    )
    order_count = _integer(
        raw_order_count,
        field_name="order_count",
        path=path,
        row_number=row_number,
        nullable=True,
    )

    event_values = {
        "symbol": symbol,
        "event_type": event_type,
        "received_time_ns": received_time_ns,
        "event_time_ms": event_time_ms,
        "transaction_time_ms": transaction_time_ms,
        "first_update_id": first_update_id,
        "final_update_id": final_update_id,
        "prev_final_update_id": prev_final_update_id,
        "last_update_id": last_update_id,
    }

    key = (
        symbol,
        event_type,
        received_time_ns,
        event_time_ms,
        transaction_time_ms,
        first_update_id,
        final_update_id,
        prev_final_update_id,
        last_update_id,
    )

    change = OrderBookLevelChange(
        side=side,
        price=price,
        quantity=quantity,
        order_count=order_count,
    )

    return key, event_values, change


def _validate_orderbook_event_values(
    event_values: Mapping[str, Any],
    *,
    sequence_contract: Any,
    path: Path,
    row_number: int,
) -> None:
    """Validate event-owned sequence fields once per logical event."""

    event_type = event_values["event_type"]

    if not isinstance(event_type, OrderBookEventType):
        raise StreamedParquetReadError(
            "Internal order-book event type is invalid",
            path=path,
            row_number=row_number,
        )

    try:
        sequence_contract.validate_event(
            event_type=event_type.value,
            transaction_time_ms=event_values["transaction_time_ms"],
            first_update_id=event_values["first_update_id"],
            final_update_id=event_values["final_update_id"],
            prev_final_update_id=event_values["prev_final_update_id"],
            last_update_id=event_values["last_update_id"],
        )
    except VenueSequenceContractError as exc:
        raise StreamedParquetReadError(
            str(exc),
            path=path,
            row_number=row_number,
        ) from exc


def read_orderbook_file(
    path: Path,
    spec: SourceFileSpec,
    *,
    consumer: OrderBookConsumer | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_event_rows: int = DEFAULT_MAX_EVENT_ROWS,
    cancellation_probe: CancellationProbe | None = None,
    cancellation_check_interval_rows: int = (DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS),
) -> OrderBookReadReport:
    """Stream and validate one order-book archive.

    Complete events are delivered to ``consumer`` as soon as their final row is
    known. No complete-hour event list is retained.

    Continuity mismatches are reported rather than raised because the reader's
    job is to expose source facts. The later replay validator determines
    whether and where reconstructed state becomes invalid.
    """
    source = Path(path).expanduser().resolve()
    batch_size = _positive_configuration_integer(
        "batch_size",
        batch_size,
    )
    max_event_rows = _positive_configuration_integer(
        "max_event_rows",
        max_event_rows,
    )
    cancellation_check_interval_rows = _positive_configuration_integer(
        "cancellation_check_interval_rows",
        cancellation_check_interval_rows,
    )

    _check_cancellation(
        cancellation_probe,
        path=source,
        row_number=None,
    )

    if spec.data_kind is not SourceDataKind.ORDERBOOK:
        raise ValueError("read_orderbook_file requires an orderbook SourceFileSpec")

    if not source.is_file():
        raise StreamedParquetReadError(
            "Order-book archive is not a regular file",
            path=source,
        )

    try:
        parquet_file = pq.ParquetFile(source)
    except Exception as exc:
        raise StreamedParquetReadError(
            "Could not open order-book Parquet archive",
            path=source,
        ) from exc

    _validate_projected_columns(
        parquet_file,
        required_columns=_ORDERBOOK_REQUIRED_COLUMNS,
        path=source,
    )

    # --- DIAGNOSTIC: log Parquet schema types for CI debugging ---
    try:
        schema_names = parquet_file.schema_arrow.names
        schema_types = {
            parquet_file.schema_arrow.field(i).name: str(
                parquet_file.schema_arrow.field(i).type
            )
            for i in range(len(schema_names))
        }
        _integer_log.info(
            "PARQUET SCHEMA for %s: venue=%s symbol=%s hour=%s "
            "last_update_id_type=%s first_update_id_type=%s "
            "final_update_id_type=%s prev_final_update_id_type=%s",
            source.name,
            spec.venue,
            spec.symbol,
            spec.hour_utc.isoformat(),
            schema_types.get("last_update_id", "MISSING"),
            schema_types.get("first_update_id", "MISSING"),
            schema_types.get("final_update_id", "MISSING"),
            schema_types.get("prev_final_update_id", "MISSING"),
        )
    except Exception:
        _integer_log.warning("Could not log Parquet schema for %s", source.name)

    sequence_contract = orderbook_sequence_contract(spec.venue)

    rows_read = 0
    events_read = 0
    update_count = 0
    snapshot_count = 0

    first_received: int | None = None
    last_received: int | None = None
    first_event_time: int | None = None
    last_event_time: int | None = None

    continuity_checks = 0
    continuity_mismatches = 0
    received_regressions = 0
    event_time_regressions = 0
    update_id_regressions = 0
    issues: list[StreamOrderingIssue] = []

    previous_event: OrderBookEvent | None = None
    previous_update: OrderBookEvent | None = None
    builder: _OrderBookEventBuilder | None = None

    def finish_event() -> None:
        nonlocal events_read
        nonlocal update_count
        nonlocal snapshot_count
        nonlocal first_received
        nonlocal last_received
        nonlocal first_event_time
        nonlocal last_event_time
        nonlocal continuity_checks
        nonlocal continuity_mismatches
        nonlocal received_regressions
        nonlocal event_time_regressions
        nonlocal update_id_regressions
        nonlocal previous_event
        nonlocal previous_update
        nonlocal builder

        if builder is None:
            return

        event = builder.freeze()
        builder = None
        events_read += 1

        if first_received is None:
            first_received = event.received_time_ns
            first_event_time = event.event_time_ms

        last_received = event.received_time_ns
        last_event_time = event.event_time_ms

        if previous_event is not None:
            if event.received_time_ns < previous_event.received_time_ns:
                received_regressions += 1
                _append_issue(
                    issues,
                    StreamOrderingIssue(
                        kind="received_time_regression",
                        previous_row_number=previous_event.first_row_number,
                        current_row_number=event.first_row_number,
                        previous_value=previous_event.received_time_ns,
                        current_value=event.received_time_ns,
                    ),
                )

            if event.event_time_ms < previous_event.event_time_ms:
                event_time_regressions += 1
                _append_issue(
                    issues,
                    StreamOrderingIssue(
                        kind="event_time_regression",
                        previous_row_number=previous_event.first_row_number,
                        current_row_number=event.first_row_number,
                        previous_value=previous_event.event_time_ms,
                        current_value=event.event_time_ms,
                    ),
                )

        if event.event_type is OrderBookEventType.SNAPSHOT:
            snapshot_count += 1

            # A snapshot is a replay boundary. Part 4B will determine whether
            # its exact update-ID representation can initialize a valid book.
            previous_update = None
        else:
            update_count += 1

            current_predecessor = sequence_contract.update_predecessor_frontier(
                first_update_id=event.first_update_id,
                final_update_id=event.final_update_id,
                prev_final_update_id=event.prev_final_update_id,
                last_update_id=event.last_update_id,
            )
            current_frontier = sequence_contract.update_resulting_frontier(
                first_update_id=event.first_update_id,
                final_update_id=event.final_update_id,
                prev_final_update_id=event.prev_final_update_id,
                last_update_id=event.last_update_id,
            )

            if previous_update is not None:
                previous_frontier = sequence_contract.update_resulting_frontier(
                    first_update_id=previous_update.first_update_id,
                    final_update_id=previous_update.final_update_id,
                    prev_final_update_id=(previous_update.prev_final_update_id),
                    last_update_id=previous_update.last_update_id,
                )
                continuity_checks += 1

                if current_predecessor != previous_frontier:
                    continuity_mismatches += 1
                    _append_issue(
                        issues,
                        StreamOrderingIssue(
                            kind="update_continuity_mismatch",
                            previous_row_number=(previous_update.first_row_number),
                            current_row_number=event.first_row_number,
                            previous_value=previous_frontier,
                            current_value=current_predecessor,
                        ),
                    )

                if current_frontier < previous_frontier:
                    update_id_regressions += 1
                    _append_issue(
                        issues,
                        StreamOrderingIssue(
                            kind="final_update_id_regression",
                            previous_row_number=(previous_update.first_row_number),
                            current_row_number=event.first_row_number,
                            previous_value=previous_frontier,
                            current_value=current_frontier,
                        ),
                    )

            previous_update = event

        previous_event = event

        if consumer is not None:
            consumer(event)

    try:
        for row_number, row in enumerate(
            _iter_projected_rows(
                parquet_file,
                columns=_ORDERBOOK_COLUMNS,
                batch_size=batch_size,
            )
        ):
            if row_number % cancellation_check_interval_rows == 0:
                _check_cancellation(
                    cancellation_probe,
                    path=source,
                    row_number=row_number,
                )

            key, event_values, change = _build_event_row(
                row,
                spec=spec,
                sequence_contract=sequence_contract,
                path=source,
                row_number=row_number,
            )
            rows_read += 1

            if builder is None or key != builder.key:
                # Validate the newly encountered event before publishing the
                # preceding event. This preserves the existing fail-fast
                # publication order if the new event is malformed.
                _validate_orderbook_event_values(
                    event_values,
                    sequence_contract=sequence_contract,
                    path=source,
                    row_number=row_number,
                )

                finish_event()

                builder = _OrderBookEventBuilder(
                    key=key,
                    symbol=event_values["symbol"],
                    event_type=event_values["event_type"],
                    received_time_ns=event_values["received_time_ns"],
                    event_time_ms=event_values["event_time_ms"],
                    transaction_time_ms=(event_values["transaction_time_ms"]),
                    first_update_id=event_values["first_update_id"],
                    final_update_id=event_values["final_update_id"],
                    prev_final_update_id=(event_values["prev_final_update_id"]),
                    last_update_id=event_values["last_update_id"],
                    changes=[],
                    level_identities=set(),
                    first_row_number=row_number,
                    last_row_number=row_number,
                )

            builder.append(
                change,
                row_number=row_number,
                max_event_rows=max_event_rows,
                path=source,
            )

        finish_event()

        _check_cancellation(
            cancellation_probe,
            path=source,
            row_number=rows_read,
        )

    except StreamedParquetReadError:
        raise
    except Exception as exc:
        raise StreamedParquetReadError(
            "Unexpected failure while streaming order-book rows",
            path=source,
            row_number=rows_read,
        ) from exc

    metadata_rows = int(parquet_file.metadata.num_rows)

    if rows_read != metadata_rows:
        raise StreamedParquetReadError(
            f"Streamed row count {rows_read} does not match Parquet metadata "
            f"row count {metadata_rows}",
            path=source,
        )

    return OrderBookReadReport(
        path=source,
        symbol=spec.symbol,
        rows_read=rows_read,
        events_read=events_read,
        update_event_count=update_count,
        snapshot_event_count=snapshot_count,
        first_received_time_ns=first_received,
        last_received_time_ns=last_received,
        first_event_time_ms=first_event_time,
        last_event_time_ms=last_event_time,
        continuity_checks=continuity_checks,
        continuity_mismatch_count=continuity_mismatches,
        received_time_regression_count=received_regressions,
        event_time_regression_count=event_time_regressions,
        update_id_regression_count=update_id_regressions,
        retained_issues=tuple(issues),
        retained_issue_limit=MAX_RETAINED_DIAGNOSTIC_ISSUES,
        batch_size=batch_size,
    )


def _trade_id(
    value: object,
    *,
    path: Path,
    row_number: int,
) -> str:
    if isinstance(value, bool) or value is None:
        raise StreamedParquetReadError(
            "trade_id must be a non-null string or integer",
            path=path,
            row_number=row_number,
        )

    if isinstance(value, int):
        if value < 0:
            raise StreamedParquetReadError(
                "trade_id cannot be negative",
                path=path,
                row_number=row_number,
            )
        return str(value)

    if isinstance(value, str):
        normalized = value.strip()
        if normalized:
            return normalized

    raise StreamedParquetReadError(
        "trade_id must be a nonblank string or non-negative integer",
        path=path,
        row_number=row_number,
    )


def read_trade_file(
    path: Path,
    spec: SourceFileSpec,
    *,
    consumer: TradeConsumer | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancellation_probe: CancellationProbe | None = None,
    cancellation_check_interval_rows: int = (DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS),
) -> TradeReadReport:
    """Stream one normalized Binance Futures trade archive."""
    source = Path(path).expanduser().resolve()
    batch_size = _positive_configuration_integer(
        "batch_size",
        batch_size,
    )
    cancellation_check_interval_rows = _positive_configuration_integer(
        "cancellation_check_interval_rows",
        cancellation_check_interval_rows,
    )

    _check_cancellation(
        cancellation_probe,
        path=source,
        row_number=None,
    )

    if spec.data_kind is not SourceDataKind.TRADES:
        raise ValueError("read_trade_file requires a trades SourceFileSpec")

    if not source.is_file():
        raise StreamedParquetReadError(
            "Trade archive is not a regular file",
            path=source,
        )

    try:
        parquet_file = pq.ParquetFile(source)
    except Exception as exc:
        raise StreamedParquetReadError(
            "Could not open trade Parquet archive",
            path=source,
        ) from exc

    _validate_projected_columns(
        parquet_file,
        required_columns=_TRADE_COLUMNS,
        path=source,
    )

    rows_read = 0
    first_received: int | None = None
    last_received: int | None = None
    first_trade_time: int | None = None
    last_trade_time: int | None = None

    received_regressions = 0
    trade_time_regressions = 0
    adjacent_duplicate_ids = 0
    issues: list[StreamOrderingIssue] = []

    previous: TradeRecord | None = None

    try:
        for row_number, row in enumerate(
            _iter_projected_rows(
                parquet_file,
                columns=_TRADE_COLUMNS,
                batch_size=batch_size,
            )
        ):
            if row_number % cancellation_check_interval_rows == 0:
                _check_cancellation(
                    cancellation_probe,
                    path=source,
                    row_number=row_number,
                )

            if len(row) != len(_TRADE_COLUMNS):
                raise StreamedParquetReadError(
                    "Projected trade row has an unexpected column count",
                    path=source,
                    row_number=row_number,
                )

            (
                raw_received_time,
                raw_event_time,
                raw_symbol,
                raw_trade_id,
                raw_price,
                raw_quantity,
                raw_trade_time,
                raw_is_buyer_maker,
                raw_order_type,
            ) = row

            symbol = _normalized_symbol(
                raw_symbol,
                spec=spec,
                path=source,
                row_number=row_number,
            )
            received_time_ns = _integer(
                raw_received_time,
                field_name="received_time",
                path=source,
                row_number=row_number,
            )
            event_time_ms = _integer(
                raw_event_time,
                field_name="event_time",
                path=source,
                row_number=row_number,
            )
            trade_time_ms = _integer(
                raw_trade_time,
                field_name="trade_time",
                path=source,
                row_number=row_number,
            )

            record = TradeRecord(
                symbol=symbol,
                trade_id=_trade_id(
                    raw_trade_id,
                    path=source,
                    row_number=row_number,
                ),
                price=_decimal_string(
                    raw_price,
                    field_name="price",
                    path=source,
                    row_number=row_number,
                    allow_zero=False,
                ),
                quantity=_decimal_string(
                    raw_quantity,
                    field_name="quantity",
                    path=source,
                    row_number=row_number,
                    allow_zero=False,
                ),
                received_time_ns=received_time_ns,
                event_time_ms=event_time_ms,
                trade_time_ms=trade_time_ms,
                is_buyer_maker=_boolean(
                    raw_is_buyer_maker,
                    field_name="is_buyer_maker",
                    path=source,
                    row_number=row_number,
                ),
                order_type=_nullable_text(
                    raw_order_type,
                    field_name="order_type",
                    path=source,
                    row_number=row_number,
                ),
                row_number=row_number,
            )

            if first_received is None:
                first_received = record.received_time_ns
                first_trade_time = record.trade_time_ms

            last_received = record.received_time_ns
            last_trade_time = record.trade_time_ms

            if previous is not None:
                if record.received_time_ns < previous.received_time_ns:
                    received_regressions += 1
                    _append_issue(
                        issues,
                        StreamOrderingIssue(
                            kind="received_time_regression",
                            previous_row_number=previous.row_number,
                            current_row_number=record.row_number,
                            previous_value=previous.received_time_ns,
                            current_value=record.received_time_ns,
                        ),
                    )

                if record.trade_time_ms < previous.trade_time_ms:
                    trade_time_regressions += 1
                    _append_issue(
                        issues,
                        StreamOrderingIssue(
                            kind="trade_time_regression",
                            previous_row_number=previous.row_number,
                            current_row_number=record.row_number,
                            previous_value=previous.trade_time_ms,
                            current_value=record.trade_time_ms,
                        ),
                    )

                if record.trade_id == previous.trade_id:
                    adjacent_duplicate_ids += 1
                    _append_issue(
                        issues,
                        StreamOrderingIssue(
                            kind="adjacent_duplicate_trade_id",
                            previous_row_number=previous.row_number,
                            current_row_number=record.row_number,
                            previous_value=previous.trade_id,
                            current_value=record.trade_id,
                        ),
                    )

            rows_read += 1
            previous = record

            if consumer is not None:
                consumer(record)

        _check_cancellation(
            cancellation_probe,
            path=source,
            row_number=rows_read,
        )

    except StreamedParquetReadError:
        raise
    except Exception as exc:
        raise StreamedParquetReadError(
            "Unexpected failure while streaming trade rows",
            path=source,
            row_number=rows_read,
        ) from exc

    metadata_rows = int(parquet_file.metadata.num_rows)

    if rows_read != metadata_rows:
        raise StreamedParquetReadError(
            f"Streamed row count {rows_read} does not match Parquet metadata "
            f"row count {metadata_rows}",
            path=source,
        )

    return TradeReadReport(
        path=source,
        symbol=spec.symbol,
        rows_read=rows_read,
        first_received_time_ns=first_received,
        last_received_time_ns=last_received,
        first_trade_time_ms=first_trade_time,
        last_trade_time_ms=last_trade_time,
        received_time_regression_count=received_regressions,
        trade_time_regression_count=trade_time_regressions,
        adjacent_duplicate_trade_id_count=adjacent_duplicate_ids,
        retained_issues=tuple(issues),
        retained_issue_limit=MAX_RETAINED_DIAGNOSTIC_ISSUES,
        batch_size=batch_size,
    )


def read_source_file(
    path: Path,
    spec: SourceFileSpec,
    *,
    orderbook_consumer: OrderBookConsumer | None = None,
    trade_consumer: TradeConsumer | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancellation_probe: CancellationProbe | None = None,
    cancellation_check_interval_rows: int = (DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS),
) -> SourceReadReport:
    """Dispatch one source archive to its strict streamed reader."""
    if spec.data_kind is SourceDataKind.ORDERBOOK:
        return read_orderbook_file(
            path,
            spec,
            consumer=orderbook_consumer,
            batch_size=batch_size,
            cancellation_probe=cancellation_probe,
            cancellation_check_interval_rows=(cancellation_check_interval_rows),
        )

    if spec.data_kind is SourceDataKind.TRADES:
        return read_trade_file(
            path,
            spec,
            consumer=trade_consumer,
            batch_size=batch_size,
            cancellation_probe=cancellation_probe,
            cancellation_check_interval_rows=(cancellation_check_interval_rows),
        )

    raise RuntimeError(f"Unsupported SourceDataKind: {spec.data_kind!r}")


__all__ = [
    "BookSide",
    "CancellationProbe",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS",
    "DEFAULT_MAX_EVENT_ROWS",
    "MAX_RETAINED_DIAGNOSTIC_ISSUES",
    "OrderBookConsumer",
    "OrderBookEvent",
    "OrderBookEventType",
    "OrderBookLevelChange",
    "OrderBookReadReport",
    "SourceReadReport",
    "StreamOrderingIssue",
    "StreamedParquetReadCancelled",
    "StreamedParquetReadError",
    "TradeConsumer",
    "TradeReadReport",
    "TradeRecord",
    "read_orderbook_file",
    "read_source_file",
    "read_trade_file",
]

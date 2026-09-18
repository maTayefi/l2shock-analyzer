# l2shock/price/hourly.py
"""Exact one-second OHLC construction from real Binance Futures trades.

Locked price semantics:

- BTC price comes from CryptoHFTData Binance Futures BTCUSDT trades;
- ETH price comes from CryptoHFTData Binance Futures ETHUSDT trades;
- trade_time_ms is the authoritative candle-assignment clock;
- received_time_ns and event_time_ms remain source diagnostics;
- one-second candle intervals are half-open [start, end);
- a trade exactly at a second boundary belongs to the new second;
- no-trade seconds remain explicitly INVALID;
- missing candles are never forward-filled;
- order-book midpoint must never replace real traded OHLC.

The accumulator accepts records from one or more streamed source archives.
This allows a later processing coordinator to supply adjacent archives when
provider file-hour ownership and trade-time hour ownership differ near a UTC
boundary.

No PostgreSQL persistence or binary block encoding occurs in this module.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Final, TypeAlias

from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS,
    CancellationProbe,
    TradeReadReport,
    TradeRecord,
    read_trade_file,
)
from l2shock.timeutils import require_utc_hour

PRICE_OBSERVATIONS_PER_HOUR: Final[int] = 3_600
MILLISECONDS_PER_SECOND: Final[int] = 1_000
MILLISECONDS_PER_HOUR: Final[int] = (
    PRICE_OBSERVATIONS_PER_HOUR * MILLISECONDS_PER_SECOND
)

_EPOCH_UTC: Final[datetime] = datetime(
    1970,
    1,
    1,
    tzinfo=timezone.utc,
)

TradeFingerprint: TypeAlias = tuple[
    str,
    Decimal,
    Decimal,
    int,
    int,
    int,
    bool,
    str | None,
]


class TradeOHLCError(ValueError):
    """Trade records cannot form a trustworthy one-second price block."""


class TradeOHLCCancelledError(RuntimeError):
    """One-second trade OHLC construction was cooperatively cancelled."""


class TradeSampleQuality(StrEnum):
    """Price-data usability for one one-second candle slot."""

    VALID = "VALID"
    INVALID = "INVALID"


class TradeSampleInvalidReason(StrEnum):
    """Why one one-second traded-price slot is unavailable."""

    NO_TRADES = "no_trades"


@dataclass(frozen=True, slots=True)
class TradeOHLCObservation:
    """One immutable one-second real-trade OHLC observation."""

    bucket_index: int
    bucket_start_utc: datetime
    bucket_end_utc: datetime

    quality: TradeSampleQuality
    invalid_reason: TradeSampleInvalidReason | None

    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None

    trade_count: int
    first_trade_time_ms: int | None
    last_trade_time_ms: int | None

    def __post_init__(self) -> None:
        if (
            isinstance(self.bucket_index, bool)
            or not isinstance(self.bucket_index, int)
            or not 0 <= self.bucket_index < PRICE_OBSERVATIONS_PER_HOUR
        ):
            raise TradeOHLCError("bucket_index must be inside [0, 3599]")

        for name in ("bucket_start_utc", "bucket_end_utc"):
            value = getattr(self, name)

            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() != timedelta(0)
            ):
                raise TradeOHLCError(f"{name} must be timezone-aware UTC")

        if self.bucket_end_utc - self.bucket_start_utc != timedelta(seconds=1):
            raise TradeOHLCError("A price bucket must span exactly one second")

        quality = TradeSampleQuality(self.quality)
        object.__setattr__(self, "quality", quality)

        if self.invalid_reason is not None:
            object.__setattr__(
                self,
                "invalid_reason",
                TradeSampleInvalidReason(self.invalid_reason),
            )

        if (
            isinstance(self.trade_count, bool)
            or not isinstance(self.trade_count, int)
            or self.trade_count < 0
        ):
            raise TradeOHLCError("trade_count must be a non-negative integer")

        values = (
            self.open,
            self.high,
            self.low,
            self.close,
        )

        if quality is TradeSampleQuality.VALID:
            if self.invalid_reason is not None:
                raise TradeOHLCError(
                    "A VALID trade candle cannot have an invalid reason"
                )

            if self.trade_count <= 0:
                raise TradeOHLCError("A VALID trade candle requires at least one trade")

            if any(
                not isinstance(value, Decimal) or not value.is_finite() or value <= 0
                for value in values
            ):
                raise TradeOHLCError(
                    "VALID OHLC values must be positive finite Decimals"
                )

            assert self.open is not None
            assert self.high is not None
            assert self.low is not None
            assert self.close is not None

            if self.high < self.low:
                raise TradeOHLCError("OHLC high cannot be below low")

            if not (
                self.low <= self.open <= self.high
                and self.low <= self.close <= self.high
            ):
                raise TradeOHLCError("OHLC open and close must lie inside [low, high]")

            if self.first_trade_time_ms is None or self.last_trade_time_ms is None:
                raise TradeOHLCError(
                    "A VALID candle requires first and last trade times"
                )

            if self.first_trade_time_ms > self.last_trade_time_ms:
                raise TradeOHLCError(
                    "first_trade_time_ms cannot exceed last_trade_time_ms"
                )

            bucket_start_ms = _datetime_to_epoch_milliseconds(self.bucket_start_utc)
            bucket_end_ms = _datetime_to_epoch_milliseconds(self.bucket_end_utc)

            if not (bucket_start_ms <= self.first_trade_time_ms < bucket_end_ms):
                raise TradeOHLCError(
                    "first_trade_time_ms must belong to the candle's "
                    "half-open UTC bucket"
                )

            if not (bucket_start_ms <= self.last_trade_time_ms < bucket_end_ms):
                raise TradeOHLCError(
                    "last_trade_time_ms must belong to the candle's "
                    "half-open UTC bucket"
                )

        else:
            if self.invalid_reason is None:
                raise TradeOHLCError(
                    "An INVALID trade candle requires an invalid reason"
                )

            if self.trade_count != 0:
                raise TradeOHLCError(
                    "An INVALID no-trade candle requires trade_count=0"
                )

            if any(value is not None for value in values):
                raise TradeOHLCError(
                    "An INVALID trade candle cannot contain OHLC values"
                )

            if (
                self.first_trade_time_ms is not None
                or self.last_trade_time_ms is not None
            ):
                raise TradeOHLCError(
                    "An INVALID trade candle cannot contain trade times"
                )

    @property
    def valid(self) -> bool:
        return self.quality is TradeSampleQuality.VALID


@dataclass(frozen=True, slots=True)
class TradeOHLCQualitySummary:
    """Deterministic quality counts for one price hour."""

    observation_count: int
    valid_count: int
    invalid_count: int
    total_trade_count: int
    no_trade_count: int

    def __post_init__(self) -> None:
        values = (
            self.observation_count,
            self.valid_count,
            self.invalid_count,
            self.total_trade_count,
            self.no_trade_count,
        )

        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in values
        ):
            raise TradeOHLCError(
                "Trade OHLC quality counts must be non-negative integers"
            )

        if self.observation_count != PRICE_OBSERVATIONS_PER_HOUR:
            raise TradeOHLCError(
                "Trade OHLC quality summary must own 3,600 observations"
            )

        if self.valid_count + self.invalid_count != self.observation_count:
            raise TradeOHLCError("Trade OHLC quality counts do not total 3,600")

        if self.no_trade_count != self.invalid_count:
            raise TradeOHLCError(
                "Every current INVALID price slot must be a no-trade slot"
            )


@dataclass(frozen=True, slots=True)
class HourlyTradeOHLCBlock:
    """Exactly 3,600 one-second real Binance trade candles."""

    base: str
    symbol: str
    source_venue: str
    hour_utc: datetime

    observations: tuple[TradeOHLCObservation, ...]
    quality_summary: TradeOHLCQualitySummary

    input_trade_count: int
    accepted_trade_count: int
    exact_duplicate_trade_count: int
    outside_target_hour_trade_count: int

    def __post_init__(self) -> None:
        base = str(self.base or "").strip().upper()
        symbol = str(self.symbol or "").strip().upper()
        venue = str(self.source_venue or "").strip().lower()
        hour = require_utc_hour("hour_utc", self.hour_utc)

        if base not in {"BTC", "ETH"}:
            raise TradeOHLCError("base must be BTC or ETH")

        expected_symbol = {
            "BTC": "BTCUSDT",
            "ETH": "ETHUSDT",
        }[base]

        if symbol != expected_symbol:
            raise TradeOHLCError(
                f"base {base} requires source symbol {expected_symbol}"
            )

        if venue != "binance_futures":
            raise TradeOHLCError(
                "V1 price OHLC requires source_venue='binance_futures'"
            )

        object.__setattr__(self, "base", base)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source_venue", venue)
        object.__setattr__(self, "hour_utc", hour)

        observations = tuple(self.observations)
        object.__setattr__(self, "observations", observations)

        if len(observations) != PRICE_OBSERVATIONS_PER_HOUR:
            raise TradeOHLCError(
                "HourlyTradeOHLCBlock must contain exactly 3,600 observations"
            )

        for expected_index, observation in enumerate(observations):
            if observation.bucket_index != expected_index:
                raise TradeOHLCError("Trade OHLC observations are not in bucket order")

            expected_start = hour + timedelta(seconds=expected_index)
            expected_end = expected_start + timedelta(seconds=1)

            if observation.bucket_start_utc != expected_start:
                raise TradeOHLCError(
                    "Price bucket start violates target-hour ownership"
                )

            if observation.bucket_end_utc != expected_end:
                raise TradeOHLCError("Price bucket end violates target-hour ownership")

        for name in (
            "input_trade_count",
            "accepted_trade_count",
            "exact_duplicate_trade_count",
            "outside_target_hour_trade_count",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise TradeOHLCError(f"{name} must be a non-negative integer")

        if (
            self.accepted_trade_count
            + self.exact_duplicate_trade_count
            + self.outside_target_hour_trade_count
            != self.input_trade_count
        ):
            raise TradeOHLCError("Trade input accounting is inconsistent")

        calculated_summary = build_trade_ohlc_quality_summary(observations)

        if calculated_summary != self.quality_summary:
            raise TradeOHLCError(
                "Trade OHLC quality summary does not match observations"
            )

        if calculated_summary.total_trade_count != self.accepted_trade_count:
            raise TradeOHLCError(
                "Accepted trade count does not match candle trade counts"
            )

    @property
    def valid_count(self) -> int:
        return self.quality_summary.valid_count

    @property
    def invalid_count(self) -> int:
        return self.quality_summary.invalid_count


@dataclass(frozen=True, slots=True)
class StreamedTradeOHLCResult:
    """Price block and source-reader reports from one streamed execution."""

    block: HourlyTradeOHLCBlock
    reader_reports: tuple[TradeReadReport, ...]

    def __post_init__(self) -> None:
        if not self.reader_reports:
            raise TradeOHLCError("A streamed trade OHLC result requires reader reports")
        rows_read = sum(report.rows_read for report in self.reader_reports)
        skipped_zero_price = sum(
            report.skipped_zero_price_row_count for report in self.reader_reports
        )
        if rows_read - skipped_zero_price != self.block.input_trade_count:
            raise TradeOHLCError(
                "Reader row counts do not match OHLC input trade count"
            )


@dataclass(slots=True)
class _MutableCandle:
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    first_key: tuple[int, int]
    last_key: tuple[int, int]

    first_trade_time_ms: int
    last_trade_time_ms: int
    trade_count: int

    def add(
        self,
        *,
        price: Decimal,
        trade_time_ms: int,
        ordinal: int,
    ) -> None:
        key = (trade_time_ms, ordinal)

        if price > self.high:
            self.high = price

        if price < self.low:
            self.low = price

        if key < self.first_key:
            self.first_key = key
            self.first_trade_time_ms = trade_time_ms
            self.open = price

        if key > self.last_key:
            self.last_key = key
            self.last_trade_time_ms = trade_time_ms
            self.close = price

        self.trade_count += 1


def _datetime_to_epoch_milliseconds(value: datetime) -> int:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise TradeOHLCError("Epoch-millisecond conversion requires timezone-aware UTC")

    delta = value - _EPOCH_UTC

    return (
        delta.days * 86_400 * MILLISECONDS_PER_SECOND
        + delta.seconds * MILLISECONDS_PER_SECOND
        + delta.microseconds // 1_000
    )


def _datetime_to_epoch_ms(value: datetime) -> int:
    value = require_utc_hour("hour_utc", value)
    return _datetime_to_epoch_milliseconds(value)


def _validate_positive_configuration_integer(
    name: str,
    value: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TradeOHLCError(f"{name} must be an integer")

    if value <= 0:
        raise TradeOHLCError(f"{name} must be positive")

    return value


def _check_cancellation(
    cancellation_probe: CancellationProbe | None,
) -> None:
    if cancellation_probe is None:
        return

    try:
        cancelled = cancellation_probe()
    except TradeOHLCCancelledError:
        raise
    except Exception as exc:
        raise TradeOHLCError("Trade OHLC cancellation probe failed") from exc

    if not isinstance(cancelled, bool):
        raise TradeOHLCError("Trade OHLC cancellation probe must return bool")

    if cancelled:
        raise TradeOHLCCancelledError("Trade OHLC construction was cancelled")


def _trade_fingerprint(record: TradeRecord) -> TradeFingerprint:
    return (
        record.symbol,
        record.price,
        record.quantity,
        record.received_time_ns,
        record.event_time_ms,
        record.trade_time_ms,
        record.is_buyer_maker,
        record.order_type,
    )


class OneSecondTradeOHLCAccumulator:
    """Build one target UTC price hour without retaining complete trade rows."""

    def __init__(
        self,
        *,
        base: str,
        hour_utc: datetime,
        cancellation_probe: CancellationProbe | None = None,
        cancellation_check_interval_records: int = 4_096,
    ) -> None:
        normalized_base = str(base or "").strip().upper()

        if normalized_base not in {"BTC", "ETH"}:
            raise TradeOHLCError("base must be BTC or ETH")

        self._base = normalized_base
        self._symbol = {
            "BTC": "BTCUSDT",
            "ETH": "ETHUSDT",
        }[normalized_base]
        self._hour_utc = require_utc_hour(
            "hour_utc",
            hour_utc,
        )
        self._hour_start_ms = _datetime_to_epoch_ms(self._hour_utc)
        self._hour_end_ms = self._hour_start_ms + MILLISECONDS_PER_HOUR

        self._cancellation_probe = cancellation_probe
        self._cancellation_interval = _validate_positive_configuration_integer(
            "cancellation_check_interval_records",
            cancellation_check_interval_records,
        )

        self._candles: list[_MutableCandle | None] = [
            None for _index in range(PRICE_OBSERVATIONS_PER_HOUR)
        ]
        self._trade_id_fingerprints: dict[
            str,
            TradeFingerprint,
        ] = {}

        self._input_trade_count = 0
        self._accepted_trade_count = 0
        self._exact_duplicate_trade_count = 0
        self._outside_target_hour_trade_count = 0
        self._finalized = False

    def consume(self, record: TradeRecord) -> None:
        if self._finalized:
            raise TradeOHLCError("Cannot consume trades after accumulator finalization")

        if not isinstance(record, TradeRecord):
            raise TypeError("record must be a TradeRecord")

        if self._input_trade_count % self._cancellation_interval == 0:
            _check_cancellation(self._cancellation_probe)

        self._input_trade_count += 1

        if record.symbol != self._symbol:
            raise TradeOHLCError(
                f"Trade symbol {record.symbol!r} does not match "
                f"target symbol {self._symbol!r}"
            )

        # Adjacent source archives may contain very large numbers of trades
        # that do not belong to this target hour. They cannot affect target
        # OHLC and therefore must not consume target-hour deduplication memory.
        if not (self._hour_start_ms <= record.trade_time_ms < self._hour_end_ms):
            self._outside_target_hour_trade_count += 1
            return

        fingerprint = _trade_fingerprint(record)
        previous = self._trade_id_fingerprints.get(record.trade_id)

        if previous is not None:
            if previous != fingerprint:
                raise TradeOHLCError(
                    "One trade_id appears with conflicting normalized content: "
                    f"{record.trade_id!r}"
                )

            self._exact_duplicate_trade_count += 1
            return

        self._trade_id_fingerprints[record.trade_id] = fingerprint

        bucket_index = (
            record.trade_time_ms - self._hour_start_ms
        ) // MILLISECONDS_PER_SECOND

        if not 0 <= bucket_index < PRICE_OBSERVATIONS_PER_HOUR:
            raise TradeOHLCError(
                "Internal trade-time bucket calculation escaped [0, 3599]"
            )

        ordinal = self._input_trade_count - 1
        candle = self._candles[bucket_index]

        if candle is None:
            key = (record.trade_time_ms, ordinal)

            candle = _MutableCandle(
                open=record.price,
                high=record.price,
                low=record.price,
                close=record.price,
                first_key=key,
                last_key=key,
                first_trade_time_ms=record.trade_time_ms,
                last_trade_time_ms=record.trade_time_ms,
                trade_count=1,
            )
            self._candles[bucket_index] = candle
        else:
            candle.add(
                price=record.price,
                trade_time_ms=record.trade_time_ms,
                ordinal=ordinal,
            )

        self._accepted_trade_count += 1

    def finish(self) -> HourlyTradeOHLCBlock:
        if self._finalized:
            raise TradeOHLCError("Trade OHLC accumulator was already finalized")

        _check_cancellation(self._cancellation_probe)
        self._finalized = True

        observations: list[TradeOHLCObservation] = []

        for bucket_index, candle in enumerate(self._candles):
            bucket_start = self._hour_utc + timedelta(seconds=bucket_index)
            bucket_end = bucket_start + timedelta(seconds=1)

            if candle is None:
                observations.append(
                    TradeOHLCObservation(
                        bucket_index=bucket_index,
                        bucket_start_utc=bucket_start,
                        bucket_end_utc=bucket_end,
                        quality=TradeSampleQuality.INVALID,
                        invalid_reason=(TradeSampleInvalidReason.NO_TRADES),
                        open=None,
                        high=None,
                        low=None,
                        close=None,
                        trade_count=0,
                        first_trade_time_ms=None,
                        last_trade_time_ms=None,
                    )
                )
                continue

            observations.append(
                TradeOHLCObservation(
                    bucket_index=bucket_index,
                    bucket_start_utc=bucket_start,
                    bucket_end_utc=bucket_end,
                    quality=TradeSampleQuality.VALID,
                    invalid_reason=None,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    trade_count=candle.trade_count,
                    first_trade_time_ms=(candle.first_trade_time_ms),
                    last_trade_time_ms=(candle.last_trade_time_ms),
                )
            )

        observation_tuple = tuple(observations)

        return HourlyTradeOHLCBlock(
            base=self._base,
            symbol=self._symbol,
            source_venue="binance_futures",
            hour_utc=self._hour_utc,
            observations=observation_tuple,
            quality_summary=build_trade_ohlc_quality_summary(observation_tuple),
            input_trade_count=self._input_trade_count,
            accepted_trade_count=self._accepted_trade_count,
            exact_duplicate_trade_count=(self._exact_duplicate_trade_count),
            outside_target_hour_trade_count=(self._outside_target_hour_trade_count),
        )


def build_trade_ohlc_quality_summary(
    observations: tuple[TradeOHLCObservation, ...],
) -> TradeOHLCQualitySummary:
    if len(observations) != PRICE_OBSERVATIONS_PER_HOUR:
        raise TradeOHLCError("Trade OHLC quality summary requires 3,600 observations")

    valid_count = sum(
        observation.quality is TradeSampleQuality.VALID for observation in observations
    )
    invalid_count = sum(
        observation.quality is TradeSampleQuality.INVALID
        for observation in observations
    )
    total_trade_count = sum(observation.trade_count for observation in observations)
    no_trade_count = sum(
        observation.invalid_reason is TradeSampleInvalidReason.NO_TRADES
        for observation in observations
    )

    return TradeOHLCQualitySummary(
        observation_count=PRICE_OBSERVATIONS_PER_HOUR,
        valid_count=valid_count,
        invalid_count=invalid_count,
        total_trade_count=total_trade_count,
        no_trade_count=no_trade_count,
    )


def build_trade_ohlc_hour(
    records: Iterable[TradeRecord],
    *,
    base: str,
    hour_utc: datetime,
    cancellation_probe: CancellationProbe | None = None,
    cancellation_check_interval_records: int = 4_096,
) -> HourlyTradeOHLCBlock:
    """Build one target price hour from an arbitrary streamed record iterable."""
    accumulator = OneSecondTradeOHLCAccumulator(
        base=base,
        hour_utc=hour_utc,
        cancellation_probe=cancellation_probe,
        cancellation_check_interval_records=(cancellation_check_interval_records),
    )

    for record in records:
        accumulator.consume(record)

    return accumulator.finish()


def stream_trade_ohlc_hour(
    sources: tuple[tuple[Path, SourceFileSpec], ...],
    *,
    target_hour_utc: datetime,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancellation_probe: CancellationProbe | None = None,
    cancellation_check_interval_rows: int = (DEFAULT_CANCELLATION_CHECK_INTERVAL_ROWS),
    cancellation_check_interval_records: int = 4_096,
) -> StreamedTradeOHLCResult:
    """Stream one or more source archives into one trade-time-owned hour.

    The source list may include adjacent archives. Trades outside the requested
    target hour are counted and omitted from that target block. This is explicit
    routing, not silent data loss.

    Source archives must be supplied in strictly increasing source-hour order.
    """
    if not sources:
        raise TradeOHLCError("At least one trade source archive is required")

    target_hour = require_utc_hour(
        "target_hour_utc",
        target_hour_utc,
    )

    normalized_sources: list[tuple[Path, SourceFileSpec]] = []
    expected_symbol: str | None = None
    previous_hour: datetime | None = None
    identities: set[tuple[object, ...]] = set()

    for raw_path, spec in sources:
        if spec.data_kind is not SourceDataKind.TRADES:
            raise TradeOHLCError("Trade OHLC sources must use data_kind='trades'")

        path = Path(raw_path).expanduser().resolve()

        if not path.is_file():
            raise TradeOHLCError(f"Trade source is not a regular file: {path}")

        if expected_symbol is None:
            expected_symbol = spec.symbol
        elif spec.symbol != expected_symbol:
            raise TradeOHLCError("All trade OHLC sources must use the same symbol")

        if previous_hour is not None and spec.hour_utc <= previous_hour:
            raise TradeOHLCError(
                "Trade source archives must be in strictly increasing "
                "source-hour order"
            )

        if spec.identity_tuple in identities:
            raise TradeOHLCError(
                "Trade source list contains a duplicate source identity"
            )

        identities.add(spec.identity_tuple)
        previous_hour = spec.hour_utc
        normalized_sources.append((path, spec))

    assert expected_symbol is not None

    base = {
        "BTCUSDT": "BTC",
        "ETHUSDT": "ETH",
    }.get(expected_symbol)

    if base is None:
        raise TradeOHLCError("V1 price construction supports only BTCUSDT and ETHUSDT")

    accumulator = OneSecondTradeOHLCAccumulator(
        base=base,
        hour_utc=target_hour,
        cancellation_probe=cancellation_probe,
        cancellation_check_interval_records=(cancellation_check_interval_records),
    )

    reports: list[TradeReadReport] = []

    for path, spec in normalized_sources:
        report = read_trade_file(
            path,
            spec,
            consumer=accumulator.consume,
            batch_size=batch_size,
            cancellation_probe=cancellation_probe,
            cancellation_check_interval_rows=(cancellation_check_interval_rows),
        )
        reports.append(report)

    return StreamedTradeOHLCResult(
        block=accumulator.finish(),
        reader_reports=tuple(reports),
    )


__all__ = [
    "MILLISECONDS_PER_HOUR",
    "MILLISECONDS_PER_SECOND",
    "PRICE_OBSERVATIONS_PER_HOUR",
    "HourlyTradeOHLCBlock",
    "OneSecondTradeOHLCAccumulator",
    "StreamedTradeOHLCResult",
    "TradeOHLCCancelledError",
    "TradeOHLCError",
    "TradeOHLCObservation",
    "TradeOHLCQualitySummary",
    "TradeSampleInvalidReason",
    "TradeSampleQuality",
    "build_trade_ohlc_hour",
    "build_trade_ohlc_quality_summary",
    "stream_trade_ohlc_hour",
]

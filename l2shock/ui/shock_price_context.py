# l2shock/ui/shock_price_context.py
"""Optional verified Binance trade-price context for Shock charts.

Price is presentation data. Missing hours and no-trade seconds stay
missing; they never gate a Shock-Start review or change its identity.

The source repository verifies compact price codec, quality summary,
and provenance before any candle is used here.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Protocol

from l2shock.db.engine import session_scope
from l2shock.db.price_repository import PriceAnalyticalRepository
from l2shock.price import TradeSampleQuality, decode_hourly_trade_ohlc_blocks

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_SECOND = timedelta(seconds=1)
_ONE_HOUR = timedelta(hours=1)

# ECharts candlestick order: open, close, low, high.
PriceCandle = list[float]
PriceChartValue = PriceCandle | None


class PriceHourReader(Protocol):
    def list_price_hours(
        self,
        *,
        base: str,
        start_utc: datetime,
        end_utc: datetime,
        verify_codec: bool = True,
    ) -> tuple[object, ...]: ...


def _utc_second(value: datetime, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
        or value.microsecond != 0
    ):
        raise ValueError(f"{name} must be an exact timezone-aware UTC second")
    return value


def _hour_floor(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _display_number(value: Decimal) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Verified price cannot be displayed as a finite float")
    return number


def price_candles_from_repository(
    repository: PriceHourReader,
    *,
    base: str,
    source_start_utc: datetime,
    source_end_utc_exclusive: datetime,
    bar_starts_utc: tuple[datetime, ...],
    timeframe_seconds: int,
) -> tuple[PriceChartValue, ...]:
    """Return one optional candle for each requested UTC viewing bar.

    Only source seconds in [source_start_utc, source_end_utc_exclusive)
    contribute. Consequently a partial first/last viewing bucket cannot
    pull in trades from outside the selected L2 viewport.
    """
    if base not in {"BTC", "ETH"}:
        raise ValueError("Price context base must be BTC or ETH")

    start = _utc_second(source_start_utc, "source_start_utc")
    end = _utc_second(source_end_utc_exclusive, "source_end_utc_exclusive")

    if end <= start:
        raise ValueError("Price context source interval must be nonempty")

    if (
        isinstance(timeframe_seconds, bool)
        or not isinstance(timeframe_seconds, int)
        or timeframe_seconds <= 0
    ):
        raise ValueError("timeframe_seconds must be a positive integer")

    starts = tuple(
        _utc_second(value, "bar_starts_utc entry") for value in bar_starts_utc
    )

    if not starts or any(left >= right for left, right in zip(starts, starts[1:])):
        raise ValueError("Price viewing bars must be nonempty and ordered")

    duration = timedelta(seconds=timeframe_seconds)

    if (
        starts[0] > start
        or starts[0] + duration <= start
        or starts[-1] >= end
        or starts[-1] + duration < end
        or any(right != left + duration for left, right in zip(starts, starts[1:]))
    ):
        raise ValueError("Price viewing bars must cover the exact source viewport")

    first_hour = _hour_floor(start)
    final_hour_exclusive = _hour_floor(end - _ONE_SECOND) + _ONE_HOUR

    hours = repository.list_price_hours(
        base=base,
        start_utc=first_hour,
        end_utc=final_hour_exclusive,
        verify_codec=True,
    )

    # Decode verified repository results, never unverified ORM rows.
    decoded_by_hour = {}

    for hour in hours:
        hour_utc = _utc_second(hour.hour_utc, "price hour_utc")
        if hour_utc != _hour_floor(hour_utc):
            raise ValueError("Price repository returned a non-hourly row")
        if not first_hour <= hour_utc < final_hour_exclusive:
            raise ValueError("Price repository returned an out-of-range hour")
        if hour_utc in decoded_by_hour:
            raise ValueError("Price repository returned a duplicate hour")
        if hour.base != base:
            raise ValueError("Price repository returned another base")
        if hour.source_venue != "binance_futures":
            raise ValueError("Price repository returned a non-Binance source")

        decoded_by_hour[hour_utc] = decode_hourly_trade_ohlc_blocks(hour.encoded)

    candles: list[PriceChartValue] = []

    for bar_start in starts:
        included_start = max(bar_start, start)
        included_end = min(bar_start + duration, end)

        first_open: Decimal | None = None
        last_close: Decimal | None = None
        highest: Decimal | None = None
        lowest: Decimal | None = None

        cursor = included_start

        while cursor < included_end:
            hour_utc = _hour_floor(cursor)
            decoded = decoded_by_hour.get(hour_utc)

            if decoded is not None:
                offset = int((cursor - hour_utc).total_seconds())

                if decoded.quality[offset] is TradeSampleQuality.VALID:
                    opened = decoded.open[offset]
                    high = decoded.high[offset]
                    low = decoded.low[offset]
                    closed = decoded.close[offset]

                    # A verified codec should already enforce this; fail
                    # closed if a repository/test double violates it.
                    if any(
                        not isinstance(value, Decimal) or not value.is_finite()
                        for value in (opened, high, low, closed)
                    ):
                        raise ValueError("VALID verified price slot lacks finite OHLC")

                    if first_open is None:
                        first_open = opened
                    last_close = closed
                    highest = high if highest is None else max(highest, high)
                    lowest = low if lowest is None else min(lowest, low)

            cursor += _ONE_SECOND

        if first_open is None:
            candles.append(None)
        else:
            assert last_close is not None
            assert highest is not None
            assert lowest is not None
            candles.append(
                [
                    _display_number(first_open),
                    _display_number(last_close),
                    _display_number(lowest),
                    _display_number(highest),
                ]
            )

    return tuple(candles)


def load_shock_price_context(
    *,
    base: str,
    source_start_utc: datetime,
    source_end_utc_exclusive: datetime,
    bar_starts_utc: tuple[datetime, ...],
    timeframe_seconds: int,
) -> tuple[PriceChartValue, ...]:
    """Create and close the database session in the calling worker thread."""
    with session_scope() as session:
        return price_candles_from_repository(
            PriceAnalyticalRepository(session),
            base=base,
            source_start_utc=source_start_utc,
            source_end_utc_exclusive=source_end_utc_exclusive,
            bar_starts_utc=bar_starts_utc,
            timeframe_seconds=timeframe_seconds,
        )

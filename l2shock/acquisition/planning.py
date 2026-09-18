# l2shock/acquisition/planning.py
"""Deterministic CryptoHFTData hourly source-file planning."""

from __future__ import annotations

from datetime import datetime, timedelta
from collections.abc import Iterable, Sequence

from l2shock.acquisition.models import SourceDataKind, SourceFileSpec
from l2shock.timeutils import floor_to_hour, require_aware_utc

_SYMBOLS_BY_VENUE = {
    "binance_futures": {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
    },
    "bybit": {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
    },
    "okx_futures": {
        "BTC": "BTC-USDT-SWAP",
        "ETH": "ETH-USDT-SWAP",
    },
}

_BINANCE_FUTURES_SYMBOLS = _SYMBOLS_BY_VENUE["binance_futures"]


def _normalized_bases(bases: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()

    for raw in bases:
        base = str(raw or "").strip().upper()

        if base not in {"BTC", "ETH"}:
            raise ValueError("Acquisition bases must be BTC or ETH; " f"got {base!r}")

        if base not in seen:
            seen.add(base)
            result.append(base)

    if not result:
        raise ValueError("At least one acquisition base is required")

    return tuple(result)


def _normalized_kinds(
    kinds: Iterable[SourceDataKind | str],
) -> tuple[SourceDataKind, ...]:
    result: list[SourceDataKind] = []
    seen: set[SourceDataKind] = set()

    for raw in kinds:
        try:
            kind = SourceDataKind(raw)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in SourceDataKind)
            raise ValueError(f"Acquisition kind must be one of: {allowed}") from exc

        if kind not in seen:
            seen.add(kind)
            result.append(kind)

    if not result:
        raise ValueError("At least one acquisition data kind is required")

    return tuple(result)


def intersecting_utc_hours(
    start_utc: datetime,
    end_utc: datetime,
) -> tuple[datetime, ...]:
    """Return UTC hours intersecting the half-open range `[start, end)`.

    Examples:

        [12:00, 13:00) -> 12:00
        [12:15, 13:00) -> 12:00
        [12:15, 13:01) -> 12:00, 13:00
    """
    start = require_aware_utc("start_utc", start_utc)
    end = require_aware_utc("end_utc", end_utc)

    if end <= start:
        raise ValueError("end_utc must be after start_utc")

    first_hour = floor_to_hour(start)
    final_instant = end - timedelta(microseconds=1)
    last_hour = floor_to_hour(final_instant)

    hours: list[datetime] = []
    current = first_hour

    while current <= last_hour:
        hours.append(current)
        current += timedelta(hours=1)

    return tuple(hours)


def _plan_venue_files(
    venue: str,
    start_utc: datetime,
    end_utc: datetime,
    *,
    bases: Sequence[str],
    data_kinds: Sequence[SourceDataKind | str],
) -> tuple[SourceFileSpec, ...]:
    normalized_venue = str(venue or "").strip().lower()

    try:
        symbols = _SYMBOLS_BY_VENUE[normalized_venue]
    except KeyError as exc:
        raise ValueError(
            f"No proven source planner exists for venue {normalized_venue!r}"
        ) from exc

    normalized_bases = _normalized_bases(bases)
    normalized_kinds = _normalized_kinds(data_kinds)
    hours = intersecting_utc_hours(start_utc, end_utc)

    result: list[SourceFileSpec] = []

    for hour_utc in hours:
        for base in normalized_bases:
            symbol = symbols[base]

            for data_kind in normalized_kinds:
                result.append(
                    SourceFileSpec(
                        venue=normalized_venue,
                        symbol=symbol,
                        data_kind=data_kind,
                        hour_utc=hour_utc,
                    )
                )

    return tuple(result)


def plan_binance_futures_files(
    start_utc: datetime,
    end_utc: datetime,
    *,
    bases: Sequence[str] = ("BTC", "ETH"),
    data_kinds: Sequence[SourceDataKind | str] = (
        SourceDataKind.ORDERBOOK,
        SourceDataKind.TRADES,
    ),
) -> tuple[SourceFileSpec, ...]:
    """Plan Binance Futures hourly files in deterministic order.

    Ordering is:

        hour
        -> base in caller order
        -> data kind in caller order

    Duplicate bases and kinds are removed while preserving their first
    occurrence.
    """
    return _plan_venue_files(
        "binance_futures",
        start_utc,
        end_utc,
        bases=bases,
        data_kinds=data_kinds,
    )


def plan_bybit_files(
    start_utc: datetime,
    end_utc: datetime,
    *,
    bases: Sequence[str] = ("BTC", "ETH"),
    data_kinds: Sequence[SourceDataKind | str] = (SourceDataKind.ORDERBOOK,),
) -> tuple[SourceFileSpec, ...]:
    """Plan empirically supported Bybit linear USDT perpetual archives.

    Bybit contributes independently reconstructed L2 order-book liquidity.
    Binance Futures trades remain the sole production price source.
    """

    return _plan_venue_files(
        "bybit",
        start_utc,
        end_utc,
        bases=bases,
        data_kinds=data_kinds,
    )


def plan_okx_futures_files(
    start_utc: datetime,
    end_utc: datetime,
    *,
    bases: Sequence[str] = ("BTC", "ETH"),
    data_kinds: Sequence[SourceDataKind | str] = (
        SourceDataKind.ORDERBOOK,
        SourceDataKind.TRADES,
    ),
) -> tuple[SourceFileSpec, ...]:
    """Plan empirically confirmed OKX linear-perpetual archives."""

    return _plan_venue_files(
        "okx_futures",
        start_utc,
        end_utc,
        bases=bases,
        data_kinds=data_kinds,
    )


def plan_production_source_files(
    start_utc: datetime,
    end_utc: datetime,
) -> tuple[SourceFileSpec, ...]:
    """Plan the complete locally supported production source universe.

    Per exact UTC hour, version 1 requests:

        Binance Futures:
            BTCUSDT orderbook
            BTCUSDT trades
            ETHUSDT orderbook
            ETHUSDT trades

        OKX Futures:
            BTC-USDT-SWAP orderbook
            ETH-USDT-SWAP orderbook

        Bybit:
            BTCUSDT orderbook
            ETHUSDT orderbook

    Binance Futures trades remain the sole persisted price source. OKX and
    Bybit trade archives are not included merely because source identities or
    physical schemas may exist.
    """

    result: list[SourceFileSpec] = []

    for hour_utc in intersecting_utc_hours(
        start_utc,
        end_utc,
    ):
        next_hour = hour_utc + timedelta(hours=1)

        result.extend(
            plan_binance_futures_files(
                hour_utc,
                next_hour,
            )
        )
        result.extend(
            plan_okx_futures_files(
                hour_utc,
                next_hour,
                data_kinds=(SourceDataKind.ORDERBOOK,),
            )
        )
        result.extend(
            plan_bybit_files(
                hour_utc,
                next_hour,
            )
        )

    return tuple(result)


__all__ = [
    "intersecting_utc_hours",
    "plan_binance_futures_files",
    "plan_bybit_files",
    "plan_okx_futures_files",
    "plan_production_source_files",
]

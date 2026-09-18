from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    intersecting_utc_hours,
    plan_binance_futures_files,
    plan_bybit_files,
    plan_production_source_files,
)


def _utc(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int = 0,
) -> datetime:
    return datetime(
        year,
        month,
        day,
        hour,
        minute,
        tzinfo=timezone.utc,
    )


def test_confirmed_binance_orderbook_remote_path() -> None:
    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_utc(2026, 9, 2, 12),
    )

    assert spec.remote_path == (
        "binance_futures/2026-09-02/12/" "BTCUSDT_orderbook.parquet"
    )


def test_confirmed_binance_trades_remote_path() -> None:
    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_utc(2026, 9, 2, 12),
    )

    assert spec.remote_path == (
        "binance_futures/2026-09-02/12/" "BTCUSDT_trades.parquet"
    )


def test_local_archive_path_is_deterministic(tmp_path: Path) -> None:
    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="ETHUSDT",
        data_kind="orderbook",
        hour_utc=_utc(2026, 9, 2, 12),
    )

    expected = (
        tmp_path
        / "cryptohftdata"
        / "binance_futures"
        / "2026-09-02"
        / "12"
        / "ETHUSDT_orderbook.parquet"
    ).resolve()

    assert spec.local_path(tmp_path) == expected


def test_one_exact_half_open_hour_plans_one_hour() -> None:
    hours = intersecting_utc_hours(
        _utc(2026, 9, 2, 12),
        _utc(2026, 9, 2, 13),
    )

    assert hours == (_utc(2026, 9, 2, 12),)


def test_partial_range_plans_every_intersecting_hour() -> None:
    hours = intersecting_utc_hours(
        _utc(2026, 9, 2, 12, 15),
        _utc(2026, 9, 2, 14, 1),
    )

    assert hours == (
        _utc(2026, 9, 2, 12),
        _utc(2026, 9, 2, 13),
        _utc(2026, 9, 2, 14),
    )


def test_end_on_hour_boundary_does_not_include_next_hour() -> None:
    hours = intersecting_utc_hours(
        _utc(2026, 9, 2, 12, 15),
        _utc(2026, 9, 2, 14),
    )

    assert hours == (
        _utc(2026, 9, 2, 12),
        _utc(2026, 9, 2, 13),
    )


def test_default_global_plan_contains_four_files_per_hour() -> None:
    plan = plan_binance_futures_files(
        _utc(2026, 9, 2, 12),
        _utc(2026, 9, 2, 13),
    )

    assert len(plan) == 4

    assert [(item.symbol, item.data_kind.value) for item in plan] == [
        ("BTCUSDT", "orderbook"),
        ("BTCUSDT", "trades"),
        ("ETHUSDT", "orderbook"),
        ("ETHUSDT", "trades"),
    ]


def test_duplicate_base_and_kind_inputs_are_deduplicated() -> None:
    plan = plan_binance_futures_files(
        _utc(2026, 9, 2, 12),
        _utc(2026, 9, 2, 13),
        bases=("BTC", "btc", "ETH"),
        data_kinds=("orderbook", "orderbook"),
    )

    assert [(item.symbol, item.data_kind.value) for item in plan] == [
        ("BTCUSDT", "orderbook"),
        ("ETHUSDT", "orderbook"),
    ]


def test_naive_planning_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        intersecting_utc_hours(
            datetime(2026, 9, 2, 12),
            _utc(2026, 9, 2, 13),
        )


def test_non_utc_planning_timestamp_is_rejected() -> None:
    tehran = timezone.utc  # Keep construction explicit below.

    start = datetime.fromisoformat("2026-09-02T15:30:00+03:30")
    assert start.tzinfo is not tehran

    with pytest.raises(ValueError, match="UTC offset"):
        intersecting_utc_hours(
            start,
            _utc(2026, 9, 2, 13),
        )


def test_unaligned_source_hour_is_rejected() -> None:
    with pytest.raises(ValueError, match="exact UTC hour"):
        SourceFileSpec(
            venue="binance_futures",
            symbol="BTCUSDT",
            data_kind="orderbook",
            hour_utc=_utc(2026, 9, 2, 12, 1),
        )


def test_unsupported_symbol_is_rejected() -> None:
    with pytest.raises(ValueError, match="currently supports only"):
        SourceFileSpec(
            venue="binance_futures",
            symbol="SOLUSDT",
            data_kind="orderbook",
            hour_utc=_utc(2026, 9, 2, 12),
        )


def test_reversed_or_empty_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="must be after"):
        intersecting_utc_hours(
            _utc(2026, 9, 2, 12),
            _utc(2026, 9, 2, 12),
        )


def test_okx_futures_remote_paths_are_deterministic() -> None:
    from l2shock.acquisition import plan_okx_futures_files

    plan = plan_okx_futures_files(
        _utc(2026, 9, 9, 12),
        _utc(2026, 9, 9, 13),
    )

    assert [
        (
            item.venue,
            item.symbol,
            item.data_kind.value,
            item.remote_path,
        )
        for item in plan
    ] == [
        (
            "okx_futures",
            "BTC-USDT-SWAP",
            "orderbook",
            ("okx_futures/2026-09-09/12/" "BTC-USDT-SWAP_orderbook.parquet"),
        ),
        (
            "okx_futures",
            "BTC-USDT-SWAP",
            "trades",
            ("okx_futures/2026-09-09/12/" "BTC-USDT-SWAP_trades.parquet"),
        ),
        (
            "okx_futures",
            "ETH-USDT-SWAP",
            "orderbook",
            ("okx_futures/2026-09-09/12/" "ETH-USDT-SWAP_orderbook.parquet"),
        ),
        (
            "okx_futures",
            "ETH-USDT-SWAP",
            "trades",
            ("okx_futures/2026-09-09/12/" "ETH-USDT-SWAP_trades.parquet"),
        ),
    ]


def test_wrong_symbol_for_supported_venue_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="venue/symbol",
    ):
        SourceFileSpec(
            venue="okx_futures",
            symbol="BTCUSDT",
            data_kind="orderbook",
            hour_utc=_utc(2026, 9, 9, 12),
        )


def test_production_plan_contains_eight_files_per_hour() -> None:
    plan = plan_production_source_files(
        _utc(2026, 9, 9, 12),
        _utc(2026, 9, 9, 13),
    )

    assert [
        (
            item.venue,
            item.symbol,
            item.data_kind.value,
        )
        for item in plan
    ] == [
        (
            "binance_futures",
            "BTCUSDT",
            "orderbook",
        ),
        (
            "binance_futures",
            "BTCUSDT",
            "trades",
        ),
        (
            "binance_futures",
            "ETHUSDT",
            "orderbook",
        ),
        (
            "binance_futures",
            "ETHUSDT",
            "trades",
        ),
        (
            "okx_futures",
            "BTC-USDT-SWAP",
            "orderbook",
        ),
        (
            "okx_futures",
            "ETH-USDT-SWAP",
            "orderbook",
        ),
        (
            "bybit",
            "BTCUSDT",
            "orderbook",
        ),
        (
            "bybit",
            "ETHUSDT",
            "orderbook",
        ),
    ]


def test_production_plan_does_not_use_okx_trades_for_price() -> None:
    plan = plan_production_source_files(
        _utc(2026, 9, 9, 12),
        _utc(2026, 9, 9, 13),
    )

    assert not any(
        item.venue == "okx_futures" and item.data_kind is SourceDataKind.TRADES
        for item in plan
    )


def test_bybit_source_identity_and_remote_path_are_supported() -> None:
    spec = SourceFileSpec(
        venue="bybit",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_utc(2026, 9, 4, 6),
    )

    assert spec.base == "BTC"
    assert spec.remote_path == ("bybit/2026-09-04/06/BTCUSDT_orderbook.parquet")


def test_bybit_orderbooks_are_in_production_acquisition_plan() -> None:
    plan = plan_production_source_files(
        _utc(2026, 9, 4, 6),
        _utc(2026, 9, 4, 7),
    )

    bybit = tuple(item for item in plan if item.venue == "bybit")

    assert [
        (
            item.symbol,
            item.data_kind.value,
            item.remote_path,
        )
        for item in bybit
    ] == [
        (
            "BTCUSDT",
            "orderbook",
            "bybit/2026-09-04/06/BTCUSDT_orderbook.parquet",
        ),
        (
            "ETHUSDT",
            "orderbook",
            "bybit/2026-09-04/06/ETHUSDT_orderbook.parquet",
        ),
    ]

    assert not any(
        item.venue == "bybit" and item.data_kind is SourceDataKind.TRADES
        for item in plan
    )
    assert len(plan) == 8


def test_bybit_planner_defaults_to_orderbooks_only() -> None:
    plan = plan_bybit_files(
        _utc(2026, 9, 4, 6),
        _utc(2026, 9, 4, 7),
    )

    assert [
        (
            item.venue,
            item.symbol,
            item.data_kind,
        )
        for item in plan
    ] == [
        (
            "bybit",
            "BTCUSDT",
            SourceDataKind.ORDERBOOK,
        ),
        (
            "bybit",
            "ETHUSDT",
            SourceDataKind.ORDERBOOK,
        ),
    ]

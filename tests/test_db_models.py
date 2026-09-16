from __future__ import annotations

from sqlalchemy import DateTime

from l2shock.db.models import Base

EXPECTED_TABLES = {
    "app_settings",
    "data_presets",
    "source_hours",
    "l2_hourly_series",
    "price_hourly_series",
    "fetch_runs",
}


def test_expected_compact_storage_tables_exist() -> None:
    assert set(Base.metadata.tables) == EXPECTED_TABLES


def test_raw_l2_event_table_does_not_exist() -> None:
    forbidden_fragments = {
        "raw_l2",
        "orderbook_row",
        "orderbook_event",
        "price_level_update",
    }

    table_names = {name.lower() for name in Base.metadata.tables}

    for fragment in forbidden_fragments:
        assert not any(fragment in table_name for table_name in table_names)


def test_all_datetime_columns_are_timezone_aware() -> None:
    datetime_columns: list[str] = []

    for table in Base.metadata.sorted_tables:
        for column in table.columns:
            if isinstance(column.type, DateTime):
                datetime_columns.append(f"{table.name}.{column.name}")
                assert column.type.timezone is True

    assert datetime_columns


def test_l2_storage_contains_only_authoritative_liquidity_channels() -> None:
    table = Base.metadata.tables["l2_hourly_series"]
    columns = set(table.columns.keys())

    assert "bid_liquidity_block" in columns
    assert "ask_liquidity_block" in columns
    assert "validity_block" in columns
    assert "source_count_block" in columns

    assert "total_liquidity_block" not in columns
    assert "imbalance_block" not in columns


def test_price_storage_is_real_trade_ohlc_storage() -> None:
    table = Base.metadata.tables["price_hourly_series"]
    columns = set(table.columns.keys())

    assert "ohlc_block" in columns
    assert "source_venue" in columns
    assert "source_symbol" in columns
    assert "midpoint_block" not in columns

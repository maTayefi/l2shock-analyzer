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


def test_engine_timezone_listener_uses_temporary_autocommit(
    monkeypatch,
) -> None:
    from types import SimpleNamespace

    import pytest

    import l2shock.db.engine as engine_module

    listeners = []
    fake_engine = object()

    settings = SimpleNamespace(
        database=SimpleNamespace(
            sqlalchemy_url="postgresql+psycopg://unused",
            pool_size=1,
            max_overflow=0,
            pool_timeout_seconds=1,
        )
    )

    def register_listener(target, event_name):
        assert target is fake_engine
        assert event_name == "connect"

        def register(function):
            listeners.append(function)
            return function

        return register

    monkeypatch.setattr(engine_module, "_engine", None)
    monkeypatch.setattr(engine_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        engine_module,
        "create_engine",
        lambda *_args, **_kwargs: fake_engine,
    )
    monkeypatch.setattr(
        engine_module.event,
        "listens_for",
        register_listener,
    )

    assert engine_module.get_engine() is fake_engine
    assert len(listeners) == 1

    class Connection:
        def __init__(self, initial_autocommit, fail):
            self.autocommit = initial_autocommit
            self.fail = fail
            self.statements = []

        def cursor(self):
            return Cursor(self)

    class Cursor:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, statement):
            assert self.connection.autocommit is True
            self.connection.statements.append(statement)

            if self.connection.fail:
                raise RuntimeError("simulated timezone initialization failure")

    for initial_autocommit in (False, True):
        for fail in (False, True):
            connection = Connection(initial_autocommit, fail)

            if fail:
                with pytest.raises(RuntimeError, match="timezone initialization"):
                    listeners[0](connection, None)
            else:
                listeners[0](connection, None)

            assert connection.autocommit is initial_autocommit
            assert connection.statements == ["SET TIME ZONE 'UTC'"]

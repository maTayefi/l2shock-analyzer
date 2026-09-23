from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy.exc import ProgrammingError

import l2shock.db.bootstrap as bootstrap


class FakeConnection:
    def __init__(self, *, duplicate_on_create: bool) -> None:
        self.duplicate_on_create = duplicate_on_create
        self.lookups = 0
        self.created = False
        self.timezone_set = False
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        return False

    def rollback(self) -> None:
        self.rollbacks += 1

    def execute(self, statement, parameters=None):
        sql = str(statement)

        if "SELECT 1 FROM pg_database" in sql:
            self.lookups += 1
            return SimpleNamespace(
                scalar=lambda: (None if self.lookups == 1 else int(self.created))
            )

        if sql.startswith("CREATE DATABASE"):
            if self.duplicate_on_create:
                self.created = True
                original = RuntimeError("database already exists")
                original.sqlstate = "42P04"
                raise ProgrammingError(sql, {}, original)

            self.created = True
            return SimpleNamespace()

        if sql.startswith("ALTER DATABASE"):
            self.timezone_set = True
            return SimpleNamespace()

        raise AssertionError(f"Unexpected SQL statement: {sql}")


class FakeEngine:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.disposed = False

    def connect(self) -> FakeConnection:
        return self.connection

    def dispose(self) -> None:
        self.disposed = True


def test_bootstrap_accepts_verified_competing_database_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection(duplicate_on_create=True)
    engine = FakeEngine(connection)

    monkeypatch.setattr(
        bootstrap,
        "get_settings",
        lambda: SimpleNamespace(
            database=SimpleNamespace(
                admin_sqlalchemy_url="postgresql+psycopg://unused/unused",
                database="l2shock_test",
            )
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "create_engine",
        lambda *_args, **_kwargs: engine,
    )

    assert bootstrap._ensure_database_exists() is False
    assert connection.lookups == 2
    assert connection.rollbacks == 1
    assert connection.timezone_set is True
    assert engine.disposed is True


def test_bootstrap_still_creates_database_without_a_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection(duplicate_on_create=False)
    engine = FakeEngine(connection)

    monkeypatch.setattr(
        bootstrap,
        "get_settings",
        lambda: SimpleNamespace(
            database=SimpleNamespace(
                admin_sqlalchemy_url="postgresql+psycopg://unused/unused",
                database="l2shock_test",
            )
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "create_engine",
        lambda *_args, **_kwargs: engine,
    )

    assert bootstrap._ensure_database_exists() is True
    assert connection.lookups == 1
    assert connection.timezone_set is True
    assert engine.disposed is True

# l2shock/db/schema.py
"""Alembic-head and ORM schema verification."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from l2shock.config import PROJECT_ROOT, get_settings
from l2shock.db.models import Base


def configparser_safe_url(url: str) -> str:
    return str(url).replace("%", "%%")


def alembic_config() -> Config:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option(
        "sqlalchemy.url",
        configparser_safe_url(get_settings().database.sqlalchemy_url),
    )
    return config


def expected_alembic_head() -> str:
    script = ScriptDirectory.from_config(alembic_config())
    heads = list(script.get_heads())

    if not heads:
        raise RuntimeError("Alembic migration graph has no head.")

    if len(heads) != 1:
        raise RuntimeError(
            f"Alembic migration graph must have exactly one head; got {heads}"
        )

    head = str(heads[0])
    if len(head) > 32:
        raise RuntimeError("Alembic revision IDs must fit alembic_version VARCHAR(32).")

    return head


def verify_schema(engine: Engine) -> None:
    """Verify migration head, tables, and all ORM-declared columns."""
    expected_head = expected_alembic_head()

    with engine.connect() as connection:
        current_heads = set(MigrationContext.configure(connection).get_current_heads())

    if current_heads != {expected_head}:
        raise RuntimeError(
            "Database is not at the expected Alembic head. "
            f"Current={sorted(current_heads)}; expected={[expected_head]}"
        )

    inspector = inspect(engine)
    actual_tables = set(inspector.get_table_names(schema="public"))
    expected_tables = {table.name for table in Base.metadata.sorted_tables}

    missing_tables = sorted(expected_tables - actual_tables)
    if missing_tables:
        raise RuntimeError(f"Database is missing application tables: {missing_tables}")

    missing_columns: list[str] = []

    for table in Base.metadata.sorted_tables:
        actual_columns = {
            column["name"]
            for column in inspector.get_columns(table.name, schema="public")
        }

        for column in table.columns:
            if column.name not in actual_columns:
                missing_columns.append(f"{table.name}.{column.name}")

    if missing_columns:
        raise RuntimeError(
            f"Database is missing ORM-declared columns: {missing_columns}"
        )


__all__ = [
    "alembic_config",
    "configparser_safe_url",
    "expected_alembic_head",
    "verify_schema",
]

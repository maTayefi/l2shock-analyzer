# l2shock/db/bootstrap.py
"""Guarded PostgreSQL database creation and Alembic bootstrap.

Usage:

    python -m l2shock.db.bootstrap
"""

from __future__ import annotations

import logging
import os
import sys

from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from l2shock.config import get_settings, load_settings
from l2shock.db.engine import get_engine, reset_engine
from l2shock.db.schema import alembic_config, verify_schema
from l2shock.logging_setup import setup_logging

log = logging.getLogger(__name__)

_SCHEMA_LOCK_KEY_1 = 0x4C325348  # "L2SH"
_SCHEMA_LOCK_KEY_2 = 0x4F434B01  # "OCK" namespace suffix


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _allow_active_sessions() -> bool:
    value = (
        str(
            os.environ.get(
                "L2SHOCK_ALLOW_BOOTSTRAP_ACTIVE_SESSIONS",
                "",
            )
        )
        .strip()
        .lower()
    )

    return value in {"1", "true", "yes", "y", "on"}


def _ensure_database_exists() -> bool:
    settings = get_settings()
    database = settings.database

    admin_engine = create_engine(
        database.admin_sqlalchemy_url,
        isolation_level="AUTOCOMMIT",
        connect_args={"application_name": "l2shock-bootstrap-admin"},
    )

    created = False

    try:
        with admin_engine.connect() as connection:
            exists = connection.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": database.database},
            ).scalar()

            if exists:
                log.info("Database %r already exists.", database.database)
            else:
                log.info("Creating database %r.", database.database)

                try:
                    connection.execute(
                        text("CREATE DATABASE " + _quote_identifier(database.database))
                    )
                except DBAPIError as exc:
                    # Another bootstrap may have created the database after
                    # our existence check. Accept only PostgreSQL's exact
                    # duplicate_database error, and verify the resulting
                    # database rather than masking another creation failure.
                    if getattr(exc.orig, "sqlstate", None) != "42P04":
                        raise

                    connection.rollback()
                    created_by_other = connection.execute(
                        text("SELECT 1 FROM pg_database WHERE datname = :name"),
                        {"name": database.database},
                    ).scalar()

                    if not created_by_other:
                        raise

                    log.info(
                        "Database %r was created concurrently.",
                        database.database,
                    )
                else:
                    created = True

            connection.execute(
                text(
                    "ALTER DATABASE "
                    + _quote_identifier(database.database)
                    + " SET timezone TO 'UTC'"
                )
            )

    finally:
        admin_engine.dispose()

    return created


def _active_client_sessions() -> int:
    engine = get_engine()

    with engine.connect() as connection:
        return int(connection.execute(text("""
                    SELECT count(*)
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND backend_type = 'client backend'
                    """)).scalar() or 0)


def _refuse_unsafe_existing_sessions(*, database_created: bool) -> None:
    if database_created:
        return

    active = _active_client_sessions()
    if active <= 0:
        return

    if _allow_active_sessions():
        log.warning(
            "Continuing despite %d other active target-database session(s) "
            "because L2SHOCK_ALLOW_BOOTSTRAP_ACTIVE_SESSIONS is enabled.",
            active,
        )
        return

    raise RuntimeError(
        f"Target database has {active} other active client session(s). "
        "Close other application instances and PostgreSQL tools before running "
        "database migration. To override deliberately, set "
        "L2SHOCK_ALLOW_BOOTSTRAP_ACTIVE_SESSIONS=1."
    )


def _upgrade_with_cross_process_lock() -> None:
    """Serialize Alembic operations with a PostgreSQL advisory lock."""
    engine = get_engine()

    with engine.connect() as connection:
        acquired = bool(
            connection.execute(
                text(
                    "SELECT pg_try_advisory_lock("
                    "CAST(:key1 AS integer), CAST(:key2 AS integer)"
                    ")"
                ),
                {
                    "key1": _SCHEMA_LOCK_KEY_1,
                    "key2": _SCHEMA_LOCK_KEY_2,
                },
            ).scalar()
        )
        connection.commit()

        if not acquired:
            raise RuntimeError(
                "Another L2 Liquidity Shock Analyzer schema migration or "
                "bootstrap operation currently owns the database lock."
            )

        try:
            config = alembic_config()
            command.upgrade(config, "head")
        finally:
            try:
                connection.execute(
                    text(
                        "SELECT pg_advisory_unlock("
                        "CAST(:key1 AS integer), CAST(:key2 AS integer)"
                        ")"
                    ),
                    {
                        "key1": _SCHEMA_LOCK_KEY_1,
                        "key2": _SCHEMA_LOCK_KEY_2,
                    },
                )
                connection.commit()
            except Exception:
                log.exception(
                    "Could not explicitly release schema advisory lock. "
                    "Closing the connection will release it."
                )


def main() -> int:
    settings = load_settings()
    setup_logging(
        settings.app.log_level,
        log_dir=settings.storage.log_path,
    )

    log.info(
        "Database bootstrap target: %s@%s:%d/%s",
        settings.database.user,
        settings.database.host,
        settings.database.port,
        settings.database.database,
    )

    try:
        database_created = _ensure_database_exists()

        reset_engine()
        _refuse_unsafe_existing_sessions(
            database_created=database_created,
        )

        _upgrade_with_cross_process_lock()
        verify_schema(get_engine())

    except Exception as exc:
        log.exception("Database bootstrap failed.")
        print()
        print("=" * 70)
        print("DATABASE BOOTSTRAP FAILED")
        print("=" * 70)
        print(f"{type(exc).__name__}: {exc}")
        print()
        raise

    log.info("Database bootstrap and schema verification completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

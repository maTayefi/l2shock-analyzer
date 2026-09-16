# l2shock/db/engine.py
"""SQLAlchemy engine and transactional session factory."""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from collections.abc import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from l2shock.config import get_settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None

_ENGINE_APPLICATION_NAME = f"l2shock-app-{os.getpid()}-{uuid.uuid4().hex[:12]}"


def get_engine_application_name() -> str:
    return _ENGINE_APPLICATION_NAME


def reset_engine() -> None:
    """Dispose the cached engine and session factory."""
    global _engine, _session_factory

    if _engine is not None:
        _engine.dispose()

    _engine = None
    _session_factory = None


def get_engine() -> Engine:
    global _engine

    if _engine is None:
        settings = get_settings()

        _engine = create_engine(
            settings.database.sqlalchemy_url,
            pool_size=settings.database.pool_size,
            max_overflow=settings.database.max_overflow,
            pool_timeout=settings.database.pool_timeout_seconds,
            pool_pre_ping=True,
            connect_args={
                "application_name": _ENGINE_APPLICATION_NAME,
            },
        )

        @event.listens_for(_engine, "connect")
        def _set_connection_timezone_utc(
            dbapi_connection,
            _connection_record,
        ) -> None:
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")

    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory

    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            expire_on_commit=False,
        )

    return _session_factory


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transaction which commits on success and rolls back otherwise."""
    session = get_session_factory()()

    try:
        yield session
        session.commit()
    except BaseException:
        session.rollback()
        raise
    finally:
        session.close()


__all__ = [
    "get_engine",
    "get_engine_application_name",
    "get_session_factory",
    "reset_engine",
    "session_scope",
]

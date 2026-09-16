# l2shock/acquisition/locks.py
"""PostgreSQL advisory locking for immutable source-hour identities."""

from __future__ import annotations

import hashlib
from typing import Final

from sqlalchemy import text
from sqlalchemy.orm import Session

from l2shock.acquisition.models import SourceFileSpec
from l2shock.acquisition.persistence import (
    SourceHourLockUnavailableError,
)

_LOCK_NAMESPACE: Final[bytes] = b"l2shock/source-hour/v1\0"


def _signed_int32(value: int) -> int:
    normalized = int(value) & 0xFFFFFFFF
    if normalized >= 0x80000000:
        return normalized - 0x100000000
    return normalized


def source_hour_lock_keys(
    spec: SourceFileSpec,
) -> tuple[int, int]:
    """Return stable signed PostgreSQL two-key advisory-lock values."""
    identity = "\0".join(
        (
            spec.provider,
            spec.venue,
            spec.data_kind.value,
            spec.symbol,
            spec.hour_utc.isoformat(),
        )
    ).encode("utf-8")

    digest = hashlib.blake2b(
        _LOCK_NAMESPACE + identity,
        digest_size=8,
    ).digest()

    first = int.from_bytes(digest[:4], byteorder="big", signed=False)
    second = int.from_bytes(digest[4:], byteorder="big", signed=False)

    return _signed_int32(first), _signed_int32(second)


def acquire_source_hour_transaction_lock(
    session: Session,
    spec: SourceFileSpec,
) -> tuple[int, int]:
    """Acquire a transaction-scoped lock without indefinite waiting.

    The lock is released automatically by PostgreSQL when the current
    transaction commits or rolls back.
    """
    key1, key2 = source_hour_lock_keys(spec)

    acquired = bool(
        session.execute(
            text(
                "SELECT pg_try_advisory_xact_lock("
                "CAST(:key1 AS integer), CAST(:key2 AS integer)"
                ")"
            ),
            {
                "key1": key1,
                "key2": key2,
            },
        ).scalar_one()
    )

    if not acquired:
        raise SourceHourLockUnavailableError(
            "Another acquisition transaction currently owns the lock for "
            f"{spec.remote_path}"
        )

    return key1, key2


__all__ = [
    "acquire_source_hour_transaction_lock",
    "source_hour_lock_keys",
]

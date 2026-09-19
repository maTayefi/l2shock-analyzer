# l2shock/db/checkpoint_reference_locks.py
"""PostgreSQL advisory locks for durable checkpoint-reference ownership.

Every transaction which creates or deletes a durable reference to a checkpoint
digest must use this namespace. Orphan-checkpoint deletion uses the same lock
before its final reference query and filesystem unlink.

Locks are transaction-scoped and therefore release automatically when the
owning PostgreSQL transaction commits or rolls back.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Final

from sqlalchemy import text
from sqlalchemy.orm import Session

_LOCK_NAMESPACE: Final[bytes] = b"l2shock/checkpoint-reference/v1\0"


class CheckpointReferenceLockUnavailableError(RuntimeError):
    """Another transaction currently owns a checkpoint-reference lock."""


def _canonical_sha256(
    field_name: str,
    value: object,
) -> str:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ValueError(f"{field_name} must be a canonical lowercase SHA-256")

    return digest


def _signed_int32(value: int) -> int:
    normalized = int(value) & 0xFFFFFFFF

    if normalized >= 0x80000000:
        return normalized - 0x100000000

    return normalized


def checkpoint_reference_lock_keys(
    content_sha256: str,
) -> tuple[int, int]:
    """Return stable signed PostgreSQL keys for one checkpoint digest."""
    digest_text = _canonical_sha256(
        "content_sha256",
        content_sha256,
    )
    digest = hashlib.blake2b(
        _LOCK_NAMESPACE + digest_text.encode("ascii"),
        digest_size=8,
    ).digest()

    first = int.from_bytes(
        digest[:4],
        byteorder="big",
        signed=False,
    )
    second = int.from_bytes(
        digest[4:],
        byteorder="big",
        signed=False,
    )

    return _signed_int32(first), _signed_int32(second)


def acquire_checkpoint_reference_transaction_lock(
    session: Session,
    content_sha256: str,
) -> tuple[int, int]:
    """Acquire one transaction-scoped checkpoint-digest lock."""
    if not isinstance(session, Session):
        raise TypeError(
            "acquire_checkpoint_reference_transaction_lock requires "
            "a SQLAlchemy Session"
        )

    digest = _canonical_sha256(
        "content_sha256",
        content_sha256,
    )
    key1, key2 = checkpoint_reference_lock_keys(digest)

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
        raise CheckpointReferenceLockUnavailableError(
            "Another transaction currently owns checkpoint-reference " f"lock {digest}"
        )

    return key1, key2


def acquire_checkpoint_reference_transaction_locks(
    session: Session,
    content_sha256s: Iterable[str | None],
) -> tuple[tuple[str, tuple[int, int]], ...]:
    """Acquire distinct checkpoint locks in canonical digest order.

    Sorting is mandatory when a transaction owns more than one checkpoint
    digest. It prevents two reference-creating transactions from acquiring the
    same set of locks in opposite orders.
    """
    if not isinstance(session, Session):
        raise TypeError(
            "acquire_checkpoint_reference_transaction_locks requires "
            "a SQLAlchemy Session"
        )

    normalized = tuple(
        sorted(
            {
                _canonical_sha256(
                    "content_sha256",
                    value,
                )
                for value in content_sha256s
                if value is not None
            }
        )
    )

    return tuple(
        (
            digest,
            acquire_checkpoint_reference_transaction_lock(
                session,
                digest,
            ),
        )
        for digest in normalized
    )


__all__ = [
    "CheckpointReferenceLockUnavailableError",
    "acquire_checkpoint_reference_transaction_lock",
    "acquire_checkpoint_reference_transaction_locks",
    "checkpoint_reference_lock_keys",
]

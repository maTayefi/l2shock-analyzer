from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from l2shock.db import get_engine
from l2shock.db.checkpoint_reference_locks import (
    CheckpointReferenceLockUnavailableError,
    acquire_checkpoint_reference_transaction_lock,
    acquire_checkpoint_reference_transaction_locks,
    checkpoint_reference_lock_keys,
)
from l2shock.db.schema import verify_schema

pytestmark = pytest.mark.postgresql


def test_checkpoint_reference_lock_is_digest_stable() -> None:
    digest = "a" * 64

    assert checkpoint_reference_lock_keys(digest) == (
        checkpoint_reference_lock_keys(digest)
    )
    assert checkpoint_reference_lock_keys(digest) != (
        checkpoint_reference_lock_keys("b" * 64)
    )


def test_checkpoint_reference_lock_rejects_noncanonical_digest() -> None:
    with pytest.raises(
        ValueError,
        match="canonical lowercase SHA-256",
    ):
        checkpoint_reference_lock_keys("A" * 64)


def test_checkpoint_reference_transaction_lock_serializes_one_digest() -> None:
    engine = get_engine()
    verify_schema(engine)

    first_connection = engine.connect()
    second_connection = engine.connect()
    first_session = Session(bind=first_connection)
    second_session = Session(bind=second_connection)

    digest = "c" * 64

    try:
        first_keys = acquire_checkpoint_reference_transaction_lock(
            first_session,
            digest,
        )

        with pytest.raises(
            CheckpointReferenceLockUnavailableError,
            match=digest,
        ):
            acquire_checkpoint_reference_transaction_lock(
                second_session,
                digest,
            )

        first_session.rollback()

        second_keys = acquire_checkpoint_reference_transaction_lock(
            second_session,
            digest,
        )

        assert second_keys == first_keys

    finally:
        first_session.rollback()
        second_session.rollback()
        first_session.close()
        second_session.close()
        first_connection.close()
        second_connection.close()


def test_multiple_checkpoint_locks_are_deduplicated_and_sorted() -> None:
    engine = get_engine()
    verify_schema(engine)

    with Session(bind=engine) as session:
        acquired = acquire_checkpoint_reference_transaction_locks(
            session,
            (
                "f" * 64,
                None,
                "a" * 64,
                "f" * 64,
            ),
        )

        assert [digest for digest, _keys in acquired] == [
            "a" * 64,
            "f" * 64,
        ]

        session.rollback()

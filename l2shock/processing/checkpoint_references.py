# l2shock/processing/checkpoint_references.py
"""Durable checkpoint-reference collection.

Checkpoint deletion and diagnostics must use the same complete durable
reference graph. A malformed non-null reference fails closed rather than
allowing a potentially referenced checkpoint to be deleted.
"""

from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from l2shock.db.models import L2HourlySeries, SourceHour


class CheckpointReferenceError(ValueError):
    """Durable checkpoint reference metadata is malformed."""


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
        raise CheckpointReferenceError(
            f"{field_name} must be a canonical lowercase SHA-256"
        )

    return digest


def collect_referenced_checkpoint_sha256s(
    session: Session,
) -> frozenset[str]:
    """Return every checkpoint digest named by committed durable metadata."""
    if not isinstance(session, Session):
        raise TypeError(
            "collect_referenced_checkpoint_sha256s requires " "a SQLAlchemy Session"
        )

    references: set[str] = set()

    source_quality_rows = session.scalars(
        select(SourceHour.quality_json).where(
            SourceHour.data_kind == "orderbook",
        )
    ).all()

    for raw_quality in source_quality_rows:
        if raw_quality is None:
            continue

        if not isinstance(raw_quality, Mapping):
            raise CheckpointReferenceError(
                "Order-book source quality_json must be an object"
            )

        for key in (
            "input_checkpoint_content_sha256",
            "output_checkpoint_content_sha256",
        ):
            value = raw_quality.get(key)

            if value is None:
                continue

            references.add(
                _canonical_sha256(
                    f"source_hours.quality_json.{key}",
                    value,
                )
            )

    provenance_rows = session.scalars(select(L2HourlySeries.provenance_json)).all()

    for raw_provenance in provenance_rows:
        if not isinstance(raw_provenance, Mapping):
            raise CheckpointReferenceError("L2 provenance_json must be an object")

        value = raw_provenance.get("checkpoint_content_sha256")

        if value is None:
            continue

        references.add(
            _canonical_sha256(
                "l2_hourly_series.provenance_json." "checkpoint_content_sha256",
                value,
            )
        )

    return frozenset(references)


__all__ = [
    "CheckpointReferenceError",
    "collect_referenced_checkpoint_sha256s",
]

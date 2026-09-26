# l2shock/analysis/l2_seconds.py
"""Decode one verified compact L2 hour into timestamp-owned seconds.

Moved unchanged from the retired aligned LM loader (``dataset.py``) in
Batch 36. It performs no price read, no aggregation, and no database access;
the caller supplies a row already verified by ``AnalyticalRepository``.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from l2shock.analysis.aggregation import L2Second
from l2shock.liquidity import decode_hourly_liquidity_blocks

if TYPE_CHECKING:
    from l2shock.db.analytical_repository import PersistedL2HourlySeries


def decode_l2_hour_to_seconds(
    hour: PersistedL2HourlySeries,
) -> tuple[L2Second, ...]:
    """Decode one verified compact L2 hour into timestamp-owned seconds."""
    decoded = decode_hourly_liquidity_blocks(hour.encoded)

    return tuple(
        L2Second(
            timestamp_utc=(hour.hour_utc + timedelta(seconds=index)),
            quality=decoded.quality[index],
            invalid_reason=decoded.invalid_reason[index],
            bid_liquidity=decoded.bid_liquidity[index],
            ask_liquidity=decoded.ask_liquidity[index],
            source_count=decoded.source_count[index],
        )
        for index in range(decoded.observation_count)
    )


__all__ = ["decode_l2_hour_to_seconds"]

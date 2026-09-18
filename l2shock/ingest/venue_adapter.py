# l2shock/ingest/venue_adapter.py
"""Venue-specific normalized order-book sequence contracts.

CryptoHFTData exposes a normalized physical Parquet schema, but field presence
and sequence ownership are not uniform across venues.

This module converts venue-specific nullable sequence fields into two exact
concepts used by replay:

- predecessor frontier:
    the frontier which must equal the current valid replay frontier before an
    update can be applied;

- resulting frontier:
    the authoritative frontier after the event is applied.

No adapter may be enabled merely because a venue shares the same Parquet column
names. Each adapter requires empirical evidence and contract tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


class VenueSequenceContractError(ValueError):
    """A normalized event violates its venue-specific sequence contract."""


def _required_nonnegative_integer(
    field_name: str,
    value: int | None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise VenueSequenceContractError(f"{field_name} must be a non-negative integer")

    return value


@dataclass(frozen=True, slots=True)
class OrderBookSequenceContract:
    """Strict normalized sequence ownership for one supported venue.

    ``update_predecessor_adjustment`` supports contracts where the current
    event carries only its resulting frontier and continuity is expressed as a
    fixed step from the preceding replay frontier.

    For example, Bybit normalized updates empirically satisfy:

        current.final_update_id == previous_frontier + 1

    Therefore their derived predecessor is:

        current.final_update_id - 1
    """

    venue: str
    transaction_time_required: bool

    snapshot_requires_final_update_id: bool
    snapshot_requires_last_update_id: bool
    snapshot_final_must_equal_last: bool
    snapshot_frontier_field: str

    update_requires_first_update_id: bool
    update_requires_prev_final_update_id: bool
    update_requires_last_update_id: bool

    update_predecessor_field: str
    update_resulting_frontier_field: str

    absent_update_fields: tuple[str, ...] = ()
    update_predecessor_adjustment: int = 0
    snapshot_allows_null_transaction_time: bool = False
    snapshot_allows_missing_frontier_when_carried: bool = False

    def __post_init__(self) -> None:
        venue = str(self.venue or "").strip().lower()

        if not venue:
            raise VenueSequenceContractError("venue cannot be blank")

        supported_fields = {
            "first_update_id",
            "final_update_id",
            "prev_final_update_id",
            "last_update_id",
        }

        for field_name, value in (
            (
                "snapshot_frontier_field",
                self.snapshot_frontier_field,
            ),
            (
                "update_predecessor_field",
                self.update_predecessor_field,
            ),
            (
                "update_resulting_frontier_field",
                self.update_resulting_frontier_field,
            ),
        ):
            if value not in supported_fields:
                raise VenueSequenceContractError(f"{field_name} is unsupported")

        if any(field not in supported_fields for field in self.absent_update_fields):
            raise VenueSequenceContractError(
                "absent_update_fields contains an unsupported field"
            )

        if isinstance(self.update_predecessor_adjustment, bool) or not isinstance(
            self.update_predecessor_adjustment, int
        ):
            raise VenueSequenceContractError(
                "update_predecessor_adjustment must be an integer"
            )

        for field_name in (
            "transaction_time_required",
            "snapshot_requires_final_update_id",
            "snapshot_requires_last_update_id",
            "snapshot_final_must_equal_last",
            "update_requires_first_update_id",
            "update_requires_prev_final_update_id",
            "update_requires_last_update_id",
            "snapshot_allows_null_transaction_time",
            "snapshot_allows_missing_frontier_when_carried",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise VenueSequenceContractError(f"{field_name} must be bool")

        object.__setattr__(self, "venue", venue)

    def transaction_time_is_required(
        self,
        event_type: str,
    ) -> bool:
        """Return event-specific transaction-time ownership."""

        normalized_type = str(event_type or "").strip().lower()

        if normalized_type not in {"snapshot", "update"}:
            raise VenueSequenceContractError(
                f"Unsupported order-book event type {normalized_type!r}"
            )

        if normalized_type == "snapshot" and self.snapshot_allows_null_transaction_time:
            return False

        return self.transaction_time_required

    def validate_event(
        self,
        *,
        event_type: str,
        transaction_time_ms: int | None,
        first_update_id: int | None,
        final_update_id: int | None,
        prev_final_update_id: int | None,
        last_update_id: int | None,
    ) -> None:
        """Validate one normalized event against this exact venue contract."""

        normalized_type = str(event_type or "").strip().lower()

        if normalized_type not in {"snapshot", "update"}:
            raise VenueSequenceContractError(
                f"Unsupported order-book event type {normalized_type!r}"
            )

        if self.transaction_time_is_required(normalized_type):
            _required_nonnegative_integer(
                "transaction_time_ms",
                transaction_time_ms,
            )
        elif transaction_time_ms is not None:
            _required_nonnegative_integer(
                "transaction_time_ms",
                transaction_time_ms,
            )

        fields = {
            "first_update_id": first_update_id,
            "final_update_id": final_update_id,
            "prev_final_update_id": prev_final_update_id,
            "last_update_id": last_update_id,
        }

        for field_name, value in fields.items():
            if value is not None:
                _required_nonnegative_integer(field_name, value)

        if normalized_type == "snapshot":
            if self.snapshot_requires_final_update_id:
                _required_nonnegative_integer(
                    "snapshot.final_update_id",
                    final_update_id,
                )

            if self.snapshot_requires_last_update_id:
                _required_nonnegative_integer(
                    "snapshot.last_update_id",
                    last_update_id,
                )

            if self.snapshot_final_must_equal_last:
                if final_update_id != last_update_id:
                    raise VenueSequenceContractError(
                        "Snapshot final_update_id must equal last_update_id"
                    )

            if (
                fields[self.snapshot_frontier_field] is None
                and not self.snapshot_allows_missing_frontier_when_carried
            ):
                raise VenueSequenceContractError(
                    "Snapshot replay frontier cannot be null"
                )

            return

        _required_nonnegative_integer(
            "update.final_update_id",
            final_update_id,
        )

        if self.update_requires_first_update_id:
            first = _required_nonnegative_integer(
                "update.first_update_id",
                first_update_id,
            )
            final = _required_nonnegative_integer(
                "update.final_update_id",
                final_update_id,
            )

            if final < first:
                raise VenueSequenceContractError(
                    "update.final_update_id cannot precede first_update_id"
                )

        if self.update_requires_prev_final_update_id:
            _required_nonnegative_integer(
                "update.prev_final_update_id",
                prev_final_update_id,
            )

        if self.update_requires_last_update_id:
            _required_nonnegative_integer(
                "update.last_update_id",
                last_update_id,
            )

        for field_name in self.absent_update_fields:
            if fields[field_name] is not None:
                raise VenueSequenceContractError(
                    f"{self.venue} update requires {field_name}=null"
                )

        predecessor = self.update_predecessor_frontier(
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            prev_final_update_id=prev_final_update_id,
            last_update_id=last_update_id,
        )
        resulting = self.update_resulting_frontier(
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            prev_final_update_id=prev_final_update_id,
            last_update_id=last_update_id,
        )

        if predecessor < 0:
            raise VenueSequenceContractError(
                "Update predecessor frontier cannot be negative"
            )

        if resulting <= predecessor:
            raise VenueSequenceContractError(
                "Update resulting frontier must be greater than its "
                "predecessor frontier"
            )

    @staticmethod
    def _field_value(
        field_name: str,
        *,
        first_update_id: int | None,
        final_update_id: int | None,
        prev_final_update_id: int | None,
        last_update_id: int | None,
    ) -> int:
        values = {
            "first_update_id": first_update_id,
            "final_update_id": final_update_id,
            "prev_final_update_id": prev_final_update_id,
            "last_update_id": last_update_id,
        }

        return _required_nonnegative_integer(
            field_name,
            values[field_name],
        )

    def snapshot_frontier(
        self,
        *,
        final_update_id: int | None,
        last_update_id: int | None,
    ) -> int | None:
        values = {
            "final_update_id": final_update_id,
            "last_update_id": last_update_id,
        }
        value = values[self.snapshot_frontier_field]

        if value is None:
            if self.snapshot_allows_missing_frontier_when_carried:
                return None

            raise VenueSequenceContractError("Snapshot replay frontier cannot be null")

        return _required_nonnegative_integer(
            f"snapshot.{self.snapshot_frontier_field}",
            value,
        )

    def update_predecessor_frontier(
        self,
        *,
        first_update_id: int | None,
        final_update_id: int | None,
        prev_final_update_id: int | None,
        last_update_id: int | None,
    ) -> int:
        raw_value = self._field_value(
            self.update_predecessor_field,
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            prev_final_update_id=prev_final_update_id,
            last_update_id=last_update_id,
        )

        return raw_value + self.update_predecessor_adjustment

    def update_resulting_frontier(
        self,
        *,
        first_update_id: int | None,
        final_update_id: int | None,
        prev_final_update_id: int | None,
        last_update_id: int | None,
    ) -> int:
        return self._field_value(
            self.update_resulting_frontier_field,
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            prev_final_update_id=prev_final_update_id,
            last_update_id=last_update_id,
        )


BINANCE_FUTURES_SEQUENCE_CONTRACT: Final[OrderBookSequenceContract] = (
    OrderBookSequenceContract(
        venue="binance_futures",
        transaction_time_required=True,
        snapshot_requires_final_update_id=False,
        snapshot_requires_last_update_id=True,
        snapshot_final_must_equal_last=False,
        snapshot_frontier_field="last_update_id",
        update_requires_first_update_id=True,
        update_requires_prev_final_update_id=True,
        update_requires_last_update_id=False,
        update_predecessor_field="prev_final_update_id",
        update_resulting_frontier_field="final_update_id",
    )
)

BYBIT_SEQUENCE_CONTRACT: Final[OrderBookSequenceContract] = OrderBookSequenceContract(
    venue="bybit",
    transaction_time_required=True,
    snapshot_requires_final_update_id=False,
    snapshot_requires_last_update_id=True,
    snapshot_final_must_equal_last=False,
    snapshot_frontier_field="final_update_id",
    update_requires_first_update_id=False,
    update_requires_prev_final_update_id=False,
    update_requires_last_update_id=True,
    update_predecessor_field="final_update_id",
    update_resulting_frontier_field="final_update_id",
    absent_update_fields=(
        "first_update_id",
        "prev_final_update_id",
    ),
    update_predecessor_adjustment=-1,
    snapshot_allows_null_transaction_time=True,
    snapshot_allows_missing_frontier_when_carried=True,
)

OKX_FUTURES_SEQUENCE_CONTRACT: Final[OrderBookSequenceContract] = (
    OrderBookSequenceContract(
        venue="okx_futures",
        transaction_time_required=False,
        snapshot_requires_final_update_id=True,
        snapshot_requires_last_update_id=True,
        snapshot_final_must_equal_last=True,
        snapshot_frontier_field="final_update_id",
        update_requires_first_update_id=False,
        update_requires_prev_final_update_id=False,
        update_requires_last_update_id=True,
        update_predecessor_field="last_update_id",
        update_resulting_frontier_field="final_update_id",
        absent_update_fields=(
            "first_update_id",
            "prev_final_update_id",
        ),
    )
)

_ORDERBOOK_SEQUENCE_CONTRACTS: Final[dict[str, OrderBookSequenceContract]] = {
    BINANCE_FUTURES_SEQUENCE_CONTRACT.venue: (BINANCE_FUTURES_SEQUENCE_CONTRACT),
    BYBIT_SEQUENCE_CONTRACT.venue: BYBIT_SEQUENCE_CONTRACT,
    OKX_FUTURES_SEQUENCE_CONTRACT.venue: (OKX_FUTURES_SEQUENCE_CONTRACT),
}


def orderbook_sequence_contract(
    venue: str,
) -> OrderBookSequenceContract:
    """Return the strict adapter for one empirically supported venue."""

    normalized = str(venue or "").strip().lower()

    try:
        return _ORDERBOOK_SEQUENCE_CONTRACTS[normalized]
    except KeyError as exc:
        raise VenueSequenceContractError(
            f"No proven order-book sequence contract exists for {normalized!r}"
        ) from exc


def supported_orderbook_sequence_venues() -> tuple[str, ...]:
    return tuple(sorted(_ORDERBOOK_SEQUENCE_CONTRACTS))


__all__ = [
    "BINANCE_FUTURES_SEQUENCE_CONTRACT",
    "BYBIT_SEQUENCE_CONTRACT",
    "OKX_FUTURES_SEQUENCE_CONTRACT",
    "OrderBookSequenceContract",
    "VenueSequenceContractError",
    "orderbook_sequence_contract",
    "supported_orderbook_sequence_venues",
]

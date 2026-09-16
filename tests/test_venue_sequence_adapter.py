from __future__ import annotations

import pytest

from l2shock.ingest import (
    VenueSequenceContractError,
    orderbook_sequence_contract,
)


def test_binance_sequence_contract_uses_prev_final_frontier() -> None:
    contract = orderbook_sequence_contract("binance_futures")

    contract.validate_event(
        event_type="update",
        transaction_time_ms=1_000,
        first_update_id=101,
        final_update_id=103,
        prev_final_update_id=100,
        last_update_id=None,
    )

    assert (
        contract.update_predecessor_frontier(
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            last_update_id=None,
        )
        == 100
    )

    assert (
        contract.update_resulting_frontier(
            first_update_id=101,
            final_update_id=103,
            prev_final_update_id=100,
            last_update_id=None,
        )
        == 103
    )


def test_okx_snapshot_owns_equal_final_and_last_frontier() -> None:
    contract = orderbook_sequence_contract("okx_futures")

    contract.validate_event(
        event_type="snapshot",
        transaction_time_ms=None,
        first_update_id=None,
        final_update_id=338_695_735_803,
        prev_final_update_id=None,
        last_update_id=338_695_735_803,
    )

    assert (
        contract.snapshot_frontier(
            final_update_id=338_695_735_803,
            last_update_id=338_695_735_803,
        )
        == 338_695_735_803
    )


def test_okx_update_uses_last_as_predecessor_and_final_as_result() -> None:
    contract = orderbook_sequence_contract("okx_futures")

    contract.validate_event(
        event_type="update",
        transaction_time_ms=None,
        first_update_id=None,
        final_update_id=338_695_735_858,
        prev_final_update_id=None,
        last_update_id=338_695_735_803,
    )

    assert (
        contract.update_predecessor_frontier(
            first_update_id=None,
            final_update_id=338_695_735_858,
            prev_final_update_id=None,
            last_update_id=338_695_735_803,
        )
        == 338_695_735_803
    )

    assert (
        contract.update_resulting_frontier(
            first_update_id=None,
            final_update_id=338_695_735_858,
            prev_final_update_id=None,
            last_update_id=338_695_735_803,
        )
        == 338_695_735_858
    )


def test_okx_update_rejects_unexpected_binance_fields() -> None:
    contract = orderbook_sequence_contract("okx_futures")

    with pytest.raises(
        VenueSequenceContractError,
        match="first_update_id=null",
    ):
        contract.validate_event(
            event_type="update",
            transaction_time_ms=None,
            first_update_id=1,
            final_update_id=3,
            prev_final_update_id=None,
            last_update_id=2,
        )


def test_okx_snapshot_requires_matching_frontiers() -> None:
    contract = orderbook_sequence_contract("okx_futures")

    with pytest.raises(
        VenueSequenceContractError,
        match="must equal",
    ):
        contract.validate_event(
            event_type="snapshot",
            transaction_time_ms=None,
            first_update_id=None,
            final_update_id=101,
            prev_final_update_id=None,
            last_update_id=100,
        )


@pytest.mark.parametrize(
    "venue",
    [
        "bybit",
        "bitget_futures",
    ],
)
def test_unproven_venues_remain_disabled(
    venue: str,
) -> None:
    with pytest.raises(
        VenueSequenceContractError,
        match="No proven",
    ):
        orderbook_sequence_contract(venue)

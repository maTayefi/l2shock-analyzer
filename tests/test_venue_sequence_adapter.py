from __future__ import annotations

import pytest

from l2shock.ingest import (
    BYBIT_SEQUENCE_CONTRACT,
    VenueSequenceContractError,
    orderbook_sequence_contract,
    supported_orderbook_sequence_venues,
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


def test_bybit_update_derives_predecessor_from_final_minus_one() -> None:
    contract = orderbook_sequence_contract("bybit")

    assert contract is BYBIT_SEQUENCE_CONTRACT

    contract.validate_event(
        event_type="update",
        transaction_time_ms=1_788_501_600_023,
        first_update_id=None,
        final_update_id=119_249_208,
        prev_final_update_id=None,
        last_update_id=805_027_801_132,
    )

    assert (
        contract.update_predecessor_frontier(
            first_update_id=None,
            final_update_id=119_249_208,
            prev_final_update_id=None,
            last_update_id=805_027_801_132,
        )
        == 119_249_207
    )

    assert (
        contract.update_resulting_frontier(
            first_update_id=None,
            final_update_id=119_249_208,
            prev_final_update_id=None,
            last_update_id=805_027_801_132,
        )
        == 119_249_208
    )


def test_bybit_native_snapshot_uses_final_update_id_frontier() -> None:
    contract = orderbook_sequence_contract("bybit")

    contract.validate_event(
        event_type="snapshot",
        transaction_time_ms=1_788_502_964_127,
        first_update_id=None,
        final_update_id=119_185_195,
        prev_final_update_id=None,
        last_update_id=805_013_072_824,
    )

    assert (
        contract.snapshot_frontier(
            final_update_id=119_185_195,
            last_update_id=805_013_072_824,
        )
        == 119_185_195
    )


def test_bybit_boundary_snapshot_may_omit_native_frontier() -> None:
    contract = orderbook_sequence_contract("bybit")

    contract.validate_event(
        event_type="snapshot",
        transaction_time_ms=None,
        first_update_id=None,
        final_update_id=None,
        prev_final_update_id=None,
        last_update_id=805_027_801_001,
    )

    assert (
        contract.snapshot_frontier(
            final_update_id=None,
            last_update_id=805_027_801_001,
        )
        is None
    )


def test_bybit_update_rejects_unexpected_binance_fields() -> None:
    contract = orderbook_sequence_contract("bybit")

    with pytest.raises(
        VenueSequenceContractError,
        match="first_update_id=null",
    ):
        contract.validate_event(
            event_type="update",
            transaction_time_ms=1_000,
            first_update_id=100,
            final_update_id=101,
            prev_final_update_id=None,
            last_update_id=9_000,
        )


def test_supported_sequence_venues_include_bybit() -> None:
    assert supported_orderbook_sequence_venues() == (
        "binance_futures",
        "bybit",
        "okx_futures",
    )


def test_bitget_remains_disabled() -> None:
    with pytest.raises(
        VenueSequenceContractError,
        match="No proven",
    ):
        orderbook_sequence_contract("bitget_futures")

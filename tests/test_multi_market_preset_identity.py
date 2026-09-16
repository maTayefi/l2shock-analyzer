from __future__ import annotations

from decimal import Decimal

from l2shock.presets import (
    build_binance_futures_data_preset,
    build_binance_okx_futures_data_preset,
    build_okx_futures_data_preset,
    component_data_presets,
)


def test_combined_preset_contains_binance_and_okx() -> None:
    preset = build_binance_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert [
        (
            market.venue,
            market.instrument,
        )
        for market in preset.eligible_markets
    ] == [
        (
            "binance_futures",
            "BTCUSDT",
        ),
        (
            "okx_futures",
            "BTC-USDT-SWAP",
        ),
    ]


def test_combined_preset_identity_is_deterministic() -> None:
    first = build_binance_okx_futures_data_preset(
        base="ETH",
        lower_fraction=Decimal("0.0000"),
        upper_fraction=Decimal("0.0100"),
    )
    second = build_binance_okx_futures_data_preset(
        base="ETH",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert first == second
    assert first.preset_hash == second.preset_hash
    assert len(first.preset_hash) == 64


def test_component_hashes_match_existing_single_market_builders() -> None:
    aggregate = build_binance_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    components = component_data_presets(aggregate)

    expected_binance = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    expected_okx = build_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert tuple(component.preset_hash for component in components) == (
        expected_binance.preset_hash,
        expected_okx.preset_hash,
    )


def test_aggregate_hash_differs_from_component_hashes() -> None:
    aggregate = build_binance_okx_futures_data_preset(
        base="ETH",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    components = component_data_presets(aggregate)

    assert aggregate.preset_hash not in {
        component.preset_hash for component in components
    }


def test_component_order_is_canonical() -> None:
    aggregate = build_binance_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert [
        component.eligible_markets[0].venue
        for component in component_data_presets(aggregate)
    ] == [
        "binance_futures",
        "okx_futures",
    ]


def test_combined_preset_round_trips_through_canonical_decoder() -> None:
    from l2shock.presets import (
        liquidity_data_preset_from_canonical_dict,
    )

    preset = build_binance_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    decoded = liquidity_data_preset_from_canonical_dict(preset.to_canonical_dict())

    assert decoded == preset
    assert decoded.preset_hash == preset.preset_hash


def test_canonical_decoder_rejects_noncanonical_market_order() -> None:
    import copy

    import pytest

    from l2shock.presets import (
        DataPresetError,
        liquidity_data_preset_from_canonical_dict,
    )

    preset = build_binance_okx_futures_data_preset(
        base="ETH",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    payload = copy.deepcopy(preset.to_canonical_dict())
    payload["eligible_markets"] = list(reversed(payload["eligible_markets"]))

    with pytest.raises(
        DataPresetError,
        match="not canonically encoded",
    ):
        liquidity_data_preset_from_canonical_dict(payload)

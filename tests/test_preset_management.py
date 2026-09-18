from __future__ import annotations

from decimal import Decimal
import pytest

from l2shock.presets import (
    PresetMarketProfile,
    build_binance_futures_data_preset,
    build_binance_okx_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.presets.management import (
    PresetManagementError,
    _fraction,
    _initial_market_supported,
    _market_profile_from_config,
    _preset,
)


def test_fraction_parser_keeps_exact_decimal() -> None:
    assert _fraction(
        "Depth",
        "0.0100",
    ) == Decimal("0.0100")


@pytest.mark.parametrize(
    "value",
    (
        "",
        "not-a-number",
        "NaN",
        "Infinity",
    ),
)
def test_fraction_parser_rejects_invalid_values(
    value: str,
) -> None:
    with pytest.raises(PresetManagementError):
        _fraction(
            "Depth",
            value,
        )


@pytest.mark.parametrize(
    ("profile", "builder"),
    (
        (
            PresetMarketProfile.BINANCE_FUTURES,
            build_binance_futures_data_preset,
        ),
        (
            PresetMarketProfile.BYBIT,
            build_bybit_data_preset,
        ),
        (
            PresetMarketProfile.OKX_FUTURES,
            build_okx_futures_data_preset,
        ),
        (
            PresetMarketProfile.BINANCE_OKX_FUTURES,
            build_binance_okx_futures_data_preset,
        ),
    ),
)
def test_editor_recognizes_every_approved_market_profile(
    profile,
    builder,
) -> None:
    preset = builder(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    payload = preset.to_canonical_dict()

    assert _initial_market_supported(
        payload,
        base="BTC",
    )
    assert (
        _market_profile_from_config(
            payload,
            base="BTC",
        )
        is profile
    )


@pytest.mark.parametrize(
    "profile",
    tuple(PresetMarketProfile),
)
def test_profile_builder_matches_authoritative_identity(
    profile: PresetMarketProfile,
) -> None:
    built = _preset(
        base="ETH",
        lower_fraction="0",
        upper_fraction="0.0100",
        market_profile=profile,
    )

    expected_builder = {
        PresetMarketProfile.BINANCE_FUTURES: (build_binance_futures_data_preset),
        PresetMarketProfile.BYBIT: build_bybit_data_preset,
        PresetMarketProfile.OKX_FUTURES: build_okx_futures_data_preset,
        PresetMarketProfile.BINANCE_OKX_FUTURES: (
            build_binance_okx_futures_data_preset
        ),
    }[profile]

    expected = expected_builder(
        base="ETH",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.0100"),
    )

    assert built == expected
    assert built.preset_hash == expected.preset_hash


def test_default_profile_remains_binance_for_compatibility() -> None:
    observed = _preset(
        base="BTC",
        lower_fraction="0",
        upper_fraction="0.01",
    )
    expected = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert observed == expected


def test_unknown_market_profile_is_rejected() -> None:
    with pytest.raises(
        PresetManagementError,
        match="market_profile",
    ):
        _preset(
            base="BTC",
            lower_fraction="0",
            upper_fraction="0.01",
            market_profile="all-exchanges",
        )


def test_noncanonical_or_unknown_composition_is_not_reinterpreted() -> None:
    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    payload = preset.to_canonical_dict()

    markets = list(payload["eligible_markets"])
    markets.append(dict(markets[0]))
    payload["eligible_markets"] = markets

    assert (
        _market_profile_from_config(
            payload,
            base="BTC",
        )
        is None
    )
    assert (
        _initial_market_supported(
            payload,
            base="BTC",
        )
        is False
    )


def test_wrong_base_is_not_recognized_as_editor_profile() -> None:
    preset = build_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert (
        _market_profile_from_config(
            preset.to_canonical_dict(),
            base="ETH",
        )
        is None
    )

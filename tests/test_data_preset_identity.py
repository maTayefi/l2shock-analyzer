from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from decimal import Decimal

import pytest

from l2shock.liquidity import DepthBand
from l2shock.presets import (
    DATA_PRESET_ALGORITHM_VERSION,
    DATA_PRESET_SCHEMA,
    DATA_PRESET_SCHEMA_VERSION,
    DataPresetError,
    EligibleMarket,
    LiquidityDataPreset,
    MarketType,
    SettlementType,
    build_binance_futures_data_preset,
)


def _btc_market() -> EligibleMarket:
    return EligibleMarket(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        market_type=MarketType.PERPETUAL,
        settlement_type=SettlementType.QUOTE,
    )


def _second_btc_market() -> EligibleMarket:
    return EligibleMarket(
        provider="cryptohftdata",
        venue="example_linear_venue",
        instrument="BTC-USDT-PERP",
        base_asset="BTC",
        quote_asset="USDT",
        market_type=MarketType.PERPETUAL,
        settlement_type=SettlementType.QUOTE,
    )


def _preset(
    *,
    markets: tuple[EligibleMarket, ...] | None = None,
    lower: str = "0",
    upper: str = "0.01",
) -> LiquidityDataPreset:
    return LiquidityDataPreset(
        base="BTC",
        band=DepthBand(
            lower_fraction=Decimal(lower),
            upper_fraction=Decimal(upper),
        ),
        eligible_markets=markets or (_btc_market(),),
    )


def test_initial_binance_futures_preset_is_complete() -> None:
    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert preset.base == "BTC"
    assert preset.schema_version == DATA_PRESET_SCHEMA_VERSION
    assert preset.algorithm_version == DATA_PRESET_ALGORITHM_VERSION
    assert len(preset.preset_hash) == 64

    market = preset.eligible_markets[0]

    assert market.provider == "cryptohftdata"
    assert market.venue == "binance_futures"
    assert market.instrument == "BTCUSDT"
    assert market.market_type is MarketType.PERPETUAL
    assert market.settlement_type is SettlementType.QUOTE


def test_hash_is_sha256_of_canonical_json_bytes() -> None:
    preset = _preset()

    assert preset.preset_hash == hashlib.sha256(preset.canonical_json_bytes).hexdigest()

    payload = json.loads(preset.canonical_json_text)

    assert payload["schema"] == DATA_PRESET_SCHEMA
    assert payload["schema_version"] == DATA_PRESET_SCHEMA_VERSION
    assert payload["base"] == "BTC"
    assert payload["sampling"]["clock"] == "received_time_ns"
    assert payload["sampling"]["interval_ms"] == 1_000


def test_encoding_is_canonical_and_has_no_insignificant_whitespace() -> None:
    preset = _preset()

    assert b"\n" not in preset.canonical_json_bytes
    assert b": " not in preset.canonical_json_bytes
    assert b", " not in preset.canonical_json_bytes

    decoded = json.loads(preset.canonical_json_bytes)

    assert decoded == preset.to_canonical_dict()


def test_decimal_scale_does_not_change_preset_hash() -> None:
    first = _preset(
        lower="0.0000",
        upper="0.0100",
    )
    second = _preset(
        lower="0",
        upper="0.01",
    )

    assert first.canonical_json_bytes == second.canonical_json_bytes
    assert first.preset_hash == second.preset_hash


def test_eligible_market_input_order_does_not_change_hash() -> None:
    first_market = _btc_market()
    second_market = _second_btc_market()

    first = _preset(
        markets=(first_market, second_market),
    )
    second = _preset(
        markets=(second_market, first_market),
    )

    assert first.eligible_markets == second.eligible_markets
    assert first.canonical_json_bytes == second.canonical_json_bytes
    assert first.preset_hash == second.preset_hash


def test_semantic_depth_change_changes_hash() -> None:
    first = _preset(upper="0.01")
    second = _preset(upper="0.02")

    assert first.preset_hash != second.preset_hash


def test_semantic_market_change_changes_hash() -> None:
    first = _preset()
    second = _preset(
        markets=(
            _btc_market(),
            _second_btc_market(),
        )
    )

    assert first.preset_hash != second.preset_hash


def test_duplicate_eligible_market_is_rejected() -> None:
    market = _btc_market()

    with pytest.raises(
        DataPresetError,
        match="duplicate",
    ):
        _preset(markets=(market, market))


def test_market_base_must_match_preset_base() -> None:
    eth_market = EligibleMarket(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="ETHUSDT",
        base_asset="ETH",
        quote_asset="USDT",
        market_type=MarketType.PERPETUAL,
        settlement_type=SettlementType.QUOTE,
    )

    with pytest.raises(
        DataPresetError,
        match="match the preset base",
    ):
        LiquidityDataPreset(
            base="BTC",
            band=DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.01"),
            ),
            eligible_markets=(eth_market,),
        )


def test_coin_settled_market_is_not_supported_by_v1_identity() -> None:
    with pytest.raises(
        DataPresetError,
        match="settlement_type='quote'",
    ):
        EligibleMarket(
            provider="cryptohftdata",
            venue="binance_futures",
            instrument="BTCUSD_PERP",
            base_asset="BTC",
            quote_asset="USD",
            market_type=MarketType.PERPETUAL,
            settlement_type=SettlementType.NONE,
        )


def test_non_usd_equivalent_quote_is_rejected() -> None:
    with pytest.raises(
        DataPresetError,
        match="USD-equivalent",
    ):
        EligibleMarket(
            provider="cryptohftdata",
            venue="example_venue",
            instrument="BTCEUR",
            base_asset="BTC",
            quote_asset="EUR",
            market_type=MarketType.SPOT,
            settlement_type=SettlementType.NONE,
        )


def test_ui_only_settings_are_not_part_of_preset_model() -> None:
    field_names = {field.name for field in fields(LiquidityDataPreset)}

    assert field_names.isdisjoint(
        {
            "timezone",
            "chart_timeframe",
            "activity_timeframe",
            "chart_colors",
            "highlight_alpha",
            "top_n",
            "price_min",
            "price_max",
            "visible_panels",
        }
    )


def test_preset_rejects_unapproved_sampling_clock() -> None:
    with pytest.raises(
        DataPresetError,
        match="received_time_ns",
    ):
        LiquidityDataPreset(
            base="BTC",
            band=DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.01"),
            ),
            eligible_markets=(_btc_market(),),
            sampling_clock="event_time_ms",
        )


def test_okx_linear_perpetual_preset_identity() -> None:
    from l2shock.presets import build_okx_futures_data_preset

    preset = build_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert preset.base == "BTC"
    assert len(preset.eligible_markets) == 1

    market = preset.eligible_markets[0]

    assert market.provider == "cryptohftdata"
    assert market.venue == "okx_futures"
    assert market.instrument == "BTC-USDT-SWAP"
    assert market.quote_asset == "USDT"
    assert market.market_type is MarketType.PERPETUAL
    assert market.settlement_type is SettlementType.QUOTE


def test_okx_and_binance_presets_have_distinct_hashes() -> None:
    from l2shock.presets import build_okx_futures_data_preset

    binance = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    okx = build_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    assert okx.preset_hash != binance.preset_hash

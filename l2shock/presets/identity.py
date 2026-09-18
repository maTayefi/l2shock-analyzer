# l2shock/presets/identity.py
"""Immutable semantic data-preset identity and canonical hashing.

A data preset contains only settings that can change persisted one-second
liquidity values or their source ownership.

UI state, chart colors, visible panels, timezone presentation, selected chart
timeframe, LM settings, and temporary controls are deliberately excluded from
this identity.

The canonical JSON object includes its own schema and algorithm versions.
SHA-256 is calculated directly over the canonical UTF-8 JSON bytes.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Final

from l2shock.liquidity.depth import DepthBand

DATA_PRESET_SCHEMA: Final[str] = "l2shock.liquidity_data_preset"
DATA_PRESET_SCHEMA_VERSION: Final[int] = 1
DATA_PRESET_ALGORITHM_VERSION: Final[str] = "l2-liquidity-v1"

DATA_PRESET_SAMPLING_CLOCK: Final[str] = "received_time_ns"
DATA_PRESET_SAMPLING_INTERVAL_MS: Final[int] = 1_000

DATA_PRESET_NOTIONAL_CONVENTION: Final[str] = (
    "linear_usd_equivalent_price_times_quantity"
)
DATA_PRESET_AGGREGATION_POLICY: Final[str] = "sum_valid_market_liquidity_per_side"
DATA_PRESET_QUALITY_POLICY: Final[str] = "normal_valid_non_normal_invalid_v1"

_APPROVED_BASE_ASSETS: Final[frozenset[str]] = frozenset({"BTC", "ETH"})
_APPROVED_USD_EQUIVALENT_QUOTES: Final[frozenset[str]] = frozenset(
    {"USD", "USDT", "USDC"}
)

_IDENTITY_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_ALGORITHM_VERSION_RE: Final[re.Pattern[str]] = re.compile(
    r"^[a-z0-9][a-z0-9._-]{0,31}$"
)


class DataPresetError(ValueError):
    """A semantic data preset is incomplete, unsupported, or inconsistent."""


class MarketType(StrEnum):
    """Market classes represented by an eligible market snapshot."""

    SPOT = "spot"
    FUTURES = "futures"
    PERPETUAL = "perpetual"


class SettlementType(StrEnum):
    """Supported settlement ownership for V1 USD-equivalent markets."""

    NONE = "none"
    QUOTE = "quote"


def _identity_text(
    field_name: str,
    value: object,
    *,
    lowercase: bool = False,
    uppercase: bool = False,
) -> str:
    text = str(value or "").strip()

    if lowercase:
        text = text.lower()

    if uppercase:
        text = text.upper()

    if not text:
        raise DataPresetError(f"{field_name} cannot be blank")

    if len(text) > 128 or not _IDENTITY_RE.fullmatch(text):
        raise DataPresetError(f"{field_name} contains unsupported identity characters")

    return text


def _canonical_fraction_text(
    value: Decimal,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, Decimal):
        raise DataPresetError(f"{field_name} must be an exact Decimal")

    if not value.is_finite() or value < 0:
        raise DataPresetError(f"{field_name} must be finite and non-negative")

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if text in {"", "-0", "+0"}:
        text = "0"

    if "e" in text.lower():
        raise DataPresetError(f"{field_name} must not use exponent notation")

    return text


@dataclass(frozen=True, slots=True)
class EligibleMarket:
    """One exact market included in a materialized liquidity preset."""

    provider: str
    venue: str
    instrument: str
    base_asset: str
    quote_asset: str
    market_type: MarketType
    settlement_type: SettlementType

    def __post_init__(self) -> None:
        provider = _identity_text(
            "provider",
            self.provider,
            lowercase=True,
        )
        venue = _identity_text(
            "venue",
            self.venue,
            lowercase=True,
        )
        instrument = _identity_text(
            "instrument",
            self.instrument,
            uppercase=True,
        )
        base_asset = _identity_text(
            "base_asset",
            self.base_asset,
            uppercase=True,
        )
        quote_asset = _identity_text(
            "quote_asset",
            self.quote_asset,
            uppercase=True,
        )

        try:
            market_type = MarketType(self.market_type)
        except (TypeError, ValueError) as exc:
            raise DataPresetError(
                "Eligible market has an unsupported market type"
            ) from exc

        try:
            settlement_type = SettlementType(self.settlement_type)
        except (TypeError, ValueError) as exc:
            raise DataPresetError(
                "Eligible market has an unsupported settlement type"
            ) from exc

        if base_asset not in _APPROVED_BASE_ASSETS:
            raise DataPresetError(
                "V1 eligible markets support only BTC or ETH base assets"
            )

        if quote_asset not in _APPROVED_USD_EQUIVALENT_QUOTES:
            raise DataPresetError(
                "V1 eligible markets require an explicitly approved "
                "USD-equivalent quote asset"
            )

        if market_type is MarketType.SPOT:
            if settlement_type is not SettlementType.NONE:
                raise DataPresetError("Spot markets must use settlement_type='none'")
        elif settlement_type is not SettlementType.QUOTE:
            raise DataPresetError(
                "V1 linear futures/perpetual markets must use "
                "settlement_type='quote'"
            )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "base_asset", base_asset)
        object.__setattr__(self, "quote_asset", quote_asset)
        object.__setattr__(self, "market_type", market_type)
        object.__setattr__(self, "settlement_type", settlement_type)

    @property
    def identity_tuple(
        self,
    ) -> tuple[str, str, str, str, str, str, str]:
        return (
            self.provider,
            self.venue,
            self.instrument,
            self.base_asset,
            self.quote_asset,
            self.market_type.value,
            self.settlement_type.value,
        )

    def to_canonical_dict(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "instrument": self.instrument,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "market_type": self.market_type.value,
            "settlement_type": self.settlement_type.value,
        }


@dataclass(frozen=True, slots=True)
class LiquidityDataPreset:
    """Immutable semantic identity for materialized liquidity observations."""

    base: str
    band: DepthBand
    eligible_markets: tuple[EligibleMarket, ...]

    schema_version: int = DATA_PRESET_SCHEMA_VERSION
    algorithm_version: str = DATA_PRESET_ALGORITHM_VERSION

    sampling_clock: str = DATA_PRESET_SAMPLING_CLOCK
    sampling_interval_ms: int = DATA_PRESET_SAMPLING_INTERVAL_MS

    notional_convention: str = DATA_PRESET_NOTIONAL_CONVENTION
    aggregation_policy: str = DATA_PRESET_AGGREGATION_POLICY
    quality_policy: str = DATA_PRESET_QUALITY_POLICY

    def __post_init__(self) -> None:
        base = _identity_text(
            "base",
            self.base,
            uppercase=True,
        )

        if base not in _APPROVED_BASE_ASSETS:
            raise DataPresetError("LiquidityDataPreset.base must be BTC or ETH")

        if not isinstance(self.band, DepthBand):
            raise DataPresetError("LiquidityDataPreset.band must be a DepthBand")

        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != DATA_PRESET_SCHEMA_VERSION
        ):
            raise DataPresetError("Unsupported data-preset schema version")

        algorithm_version = str(self.algorithm_version or "").strip().lower()

        if not _ALGORITHM_VERSION_RE.fullmatch(algorithm_version):
            raise DataPresetError(
                "algorithm_version must be a lowercase version identity "
                "of at most 32 characters"
            )

        if self.sampling_clock != DATA_PRESET_SAMPLING_CLOCK:
            raise DataPresetError(
                "V1 data presets require sampling_clock='received_time_ns'"
            )

        if (
            isinstance(self.sampling_interval_ms, bool)
            or not isinstance(self.sampling_interval_ms, int)
            or self.sampling_interval_ms != DATA_PRESET_SAMPLING_INTERVAL_MS
        ):
            raise DataPresetError("V1 data presets require sampling_interval_ms=1000")

        fixed_text_fields = {
            "notional_convention": DATA_PRESET_NOTIONAL_CONVENTION,
            "aggregation_policy": DATA_PRESET_AGGREGATION_POLICY,
            "quality_policy": DATA_PRESET_QUALITY_POLICY,
        }

        for field_name, expected in fixed_text_fields.items():
            if getattr(self, field_name) != expected:
                raise DataPresetError(
                    f"V1 data presets require {field_name}={expected!r}"
                )

        markets = tuple(self.eligible_markets)

        if not markets:
            raise DataPresetError("A data preset requires at least one eligible market")

        for market in markets:
            if not isinstance(market, EligibleMarket):
                raise DataPresetError(
                    "eligible_markets must contain EligibleMarket objects"
                )

            if market.base_asset != base:
                raise DataPresetError(
                    "Every eligible market must match the preset base"
                )

        sorted_markets = tuple(
            sorted(
                markets,
                key=lambda item: item.identity_tuple,
            )
        )

        identities = [market.identity_tuple for market in sorted_markets]

        if len(set(identities)) != len(identities):
            raise DataPresetError(
                "eligible_markets contains a duplicate market identity"
            )

        object.__setattr__(self, "base", base)
        object.__setattr__(
            self,
            "algorithm_version",
            algorithm_version,
        )
        object.__setattr__(
            self,
            "eligible_markets",
            sorted_markets,
        )

    def to_canonical_dict(self) -> dict[str, object]:
        """Return the complete semantic JSON object used for hashing."""
        return {
            "schema": DATA_PRESET_SCHEMA,
            "schema_version": self.schema_version,
            "algorithm_version": self.algorithm_version,
            "base": self.base,
            "sampling": {
                "clock": self.sampling_clock,
                "interval_ms": self.sampling_interval_ms,
                "bucket_policy": ("last_reconstructed_state_at_or_before_bucket_end"),
            },
            "depth_band": {
                "lower_fraction": _canonical_fraction_text(
                    self.band.lower_fraction,
                    field_name="depth_band.lower_fraction",
                ),
                "upper_fraction": _canonical_fraction_text(
                    self.band.upper_fraction,
                    field_name="depth_band.upper_fraction",
                ),
                "boundary_policy": "inclusive",
                "anchor_policy": "independent_best_bid_and_best_ask",
            },
            "notional_convention": self.notional_convention,
            "aggregation_policy": self.aggregation_policy,
            "quality_policy": self.quality_policy,
            "eligible_markets": [
                market.to_canonical_dict() for market in self.eligible_markets
            ],
        }

    @property
    def canonical_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_canonical_dict(),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @property
    def canonical_json_text(self) -> str:
        return self.canonical_json_bytes.decode("utf-8")

    @property
    def preset_hash(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes).hexdigest()


def build_binance_futures_data_preset(
    *,
    base: str,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
) -> LiquidityDataPreset:
    """Build the initial single-market Binance Futures materialized preset."""
    normalized_base = _identity_text(
        "base",
        base,
        uppercase=True,
    )

    symbol_by_base = {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
    }

    try:
        instrument = symbol_by_base[normalized_base]
    except KeyError as exc:
        raise DataPresetError(
            "Initial Binance Futures presets support only BTC or ETH"
        ) from exc

    return LiquidityDataPreset(
        base=normalized_base,
        band=DepthBand(
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
        ),
        eligible_markets=(
            EligibleMarket(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument=instrument,
                base_asset=normalized_base,
                quote_asset="USDT",
                market_type=MarketType.PERPETUAL,
                settlement_type=SettlementType.QUOTE,
            ),
        ),
    )


def build_bybit_data_preset(
    *,
    base: str,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
) -> LiquidityDataPreset:
    """Build one empirically supported Bybit linear USDT perpetual preset."""

    normalized_base = _identity_text(
        "base",
        base,
        uppercase=True,
    )

    symbol_by_base = {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
    }

    try:
        instrument = symbol_by_base[normalized_base]
    except KeyError as exc:
        raise DataPresetError("Bybit presets support only BTC or ETH") from exc

    return LiquidityDataPreset(
        base=normalized_base,
        band=DepthBand(
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
        ),
        eligible_markets=(
            EligibleMarket(
                provider="cryptohftdata",
                venue="bybit",
                instrument=instrument,
                base_asset=normalized_base,
                quote_asset="USDT",
                market_type=MarketType.PERPETUAL,
                settlement_type=SettlementType.QUOTE,
            ),
        ),
    )


def build_okx_futures_data_preset(
    *,
    base: str,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
) -> LiquidityDataPreset:
    """Build one empirically supported OKX linear-perpetual preset."""

    normalized_base = _identity_text(
        "base",
        base,
        uppercase=True,
    )

    symbol_by_base = {
        "BTC": "BTC-USDT-SWAP",
        "ETH": "ETH-USDT-SWAP",
    }

    try:
        instrument = symbol_by_base[normalized_base]
    except KeyError as exc:
        raise DataPresetError("OKX Futures presets support only BTC or ETH") from exc

    return LiquidityDataPreset(
        base=normalized_base,
        band=DepthBand(
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
        ),
        eligible_markets=(
            EligibleMarket(
                provider="cryptohftdata",
                venue="okx_futures",
                instrument=instrument,
                base_asset=normalized_base,
                quote_asset="USDT",
                market_type=MarketType.PERPETUAL,
                settlement_type=SettlementType.QUOTE,
            ),
        ),
    )


def build_binance_okx_futures_data_preset(
    *,
    base: str,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
) -> LiquidityDataPreset:
    """Build the approved Binance+OKX aggregate-liquidity preset.

    The preset owns two independently reconstructed linear USDT perpetual
    markets. It does not imply raw-event aggregation or aggregate persistence.
    Aggregate liquidity is constructed from verified component rows at
    analysis load time.
    """
    normalized_base = _identity_text(
        "base",
        base,
        uppercase=True,
    )

    instrument_by_venue_and_base = {
        ("binance_futures", "BTC"): "BTCUSDT",
        ("binance_futures", "ETH"): "ETHUSDT",
        ("okx_futures", "BTC"): "BTC-USDT-SWAP",
        ("okx_futures", "ETH"): "ETH-USDT-SWAP",
    }

    if normalized_base not in _APPROVED_BASE_ASSETS:
        raise DataPresetError("Binance+OKX Futures presets support only BTC or ETH")

    markets = tuple(
        EligibleMarket(
            provider="cryptohftdata",
            venue=venue,
            instrument=instrument_by_venue_and_base[
                (
                    venue,
                    normalized_base,
                )
            ],
            base_asset=normalized_base,
            quote_asset="USDT",
            market_type=MarketType.PERPETUAL,
            settlement_type=SettlementType.QUOTE,
        )
        for venue in (
            "binance_futures",
            "okx_futures",
        )
    )

    return LiquidityDataPreset(
        base=normalized_base,
        band=DepthBand(
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
        ),
        eligible_markets=markets,
    )


def component_data_presets(
    preset: LiquidityDataPreset,
) -> tuple[LiquidityDataPreset, ...]:
    """Derive immutable one-market materialization presets.

    Existing ``l2_hourly_series`` rows remain owned by single-market preset
    hashes. A multi-market preset is an analysis identity whose component
    hashes are derived deterministically from its eligible-market snapshot.
    """
    if not isinstance(preset, LiquidityDataPreset):
        raise TypeError("preset must be a LiquidityDataPreset")

    return tuple(
        LiquidityDataPreset(
            base=preset.base,
            band=preset.band,
            eligible_markets=(market,),
            schema_version=preset.schema_version,
            algorithm_version=preset.algorithm_version,
            sampling_clock=preset.sampling_clock,
            sampling_interval_ms=preset.sampling_interval_ms,
            notional_convention=preset.notional_convention,
            aggregation_policy=preset.aggregation_policy,
            quality_policy=preset.quality_policy,
        )
        for market in preset.eligible_markets
    )


def liquidity_data_preset_from_canonical_dict(
    value: Mapping[str, object],
) -> LiquidityDataPreset:
    """Decode and verify one canonical persisted preset configuration.

    This is intentionally strict. It accepts only the complete current
    canonical representation and reconstructs the same semantic preset hash.

    It does not reinterpret incomplete, legacy, or UI-shaped dictionaries.
    """

    if not isinstance(value, Mapping):
        raise DataPresetError("Preset configuration must be a mapping")

    payload = dict(value)

    expected_keys = {
        "schema",
        "schema_version",
        "algorithm_version",
        "base",
        "sampling",
        "depth_band",
        "notional_convention",
        "aggregation_policy",
        "quality_policy",
        "eligible_markets",
    }

    if set(payload) != expected_keys:
        raise DataPresetError(
            "Preset configuration fields do not match the canonical contract"
        )

    if payload["schema"] != DATA_PRESET_SCHEMA:
        raise DataPresetError("Unsupported data-preset schema identity")

    schema_version = payload["schema_version"]

    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != DATA_PRESET_SCHEMA_VERSION
    ):
        raise DataPresetError("Unsupported data-preset schema version")

    raw_sampling = payload["sampling"]

    if not isinstance(raw_sampling, Mapping):
        raise DataPresetError("Preset sampling configuration must be an object")

    sampling = dict(raw_sampling)

    if set(sampling) != {
        "clock",
        "interval_ms",
        "bucket_policy",
    }:
        raise DataPresetError(
            "Preset sampling fields do not match the canonical contract"
        )

    if sampling["bucket_policy"] != (
        "last_reconstructed_state_at_or_before_bucket_end"
    ):
        raise DataPresetError("Unsupported preset bucket sampling policy")

    raw_depth = payload["depth_band"]

    if not isinstance(raw_depth, Mapping):
        raise DataPresetError("Preset depth_band must be an object")

    depth = dict(raw_depth)

    if set(depth) != {
        "lower_fraction",
        "upper_fraction",
        "boundary_policy",
        "anchor_policy",
    }:
        raise DataPresetError(
            "Preset depth-band fields do not match the canonical contract"
        )

    if depth["boundary_policy"] != "inclusive":
        raise DataPresetError("Unsupported depth-band boundary policy")

    if depth["anchor_policy"] != ("independent_best_bid_and_best_ask"):
        raise DataPresetError("Unsupported depth-band anchor policy")

    try:
        lower_fraction = Decimal(str(depth["lower_fraction"]))
        upper_fraction = Decimal(str(depth["upper_fraction"]))
    except (InvalidOperation, ValueError) as exc:
        raise DataPresetError("Preset depth fractions are invalid") from exc

    raw_markets = payload["eligible_markets"]

    if not isinstance(raw_markets, list) or not raw_markets:
        raise DataPresetError("Preset eligible_markets must be a non-empty array")

    market_keys = {
        "provider",
        "venue",
        "instrument",
        "base_asset",
        "quote_asset",
        "market_type",
        "settlement_type",
    }
    markets: list[EligibleMarket] = []

    for index, raw_market in enumerate(raw_markets):
        if not isinstance(raw_market, Mapping):
            raise DataPresetError(f"eligible_markets[{index}] must be an object")

        market = dict(raw_market)

        if set(market) != market_keys:
            raise DataPresetError(
                f"eligible_markets[{index}] fields do not match "
                "the canonical contract"
            )

        try:
            markets.append(
                EligibleMarket(
                    provider=market["provider"],
                    venue=market["venue"],
                    instrument=market["instrument"],
                    base_asset=market["base_asset"],
                    quote_asset=market["quote_asset"],
                    market_type=MarketType(market["market_type"]),
                    settlement_type=SettlementType(market["settlement_type"]),
                )
            )
        except (TypeError, ValueError) as exc:
            raise DataPresetError(f"eligible_markets[{index}] is invalid") from exc

    preset = LiquidityDataPreset(
        base=payload["base"],
        band=DepthBand(
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
        ),
        eligible_markets=tuple(markets),
        schema_version=schema_version,
        algorithm_version=str(payload["algorithm_version"]),
        sampling_clock=str(sampling["clock"]),
        sampling_interval_ms=sampling["interval_ms"],
        notional_convention=str(payload["notional_convention"]),
        aggregation_policy=str(payload["aggregation_policy"]),
        quality_policy=str(payload["quality_policy"]),
    )

    if preset.to_canonical_dict() != payload:
        raise DataPresetError(
            "Preset configuration is valid but not canonically encoded"
        )

    return preset


__all__ = [
    "DATA_PRESET_AGGREGATION_POLICY",
    "DATA_PRESET_ALGORITHM_VERSION",
    "DATA_PRESET_NOTIONAL_CONVENTION",
    "DATA_PRESET_QUALITY_POLICY",
    "DATA_PRESET_SAMPLING_CLOCK",
    "DATA_PRESET_SAMPLING_INTERVAL_MS",
    "DATA_PRESET_SCHEMA",
    "DATA_PRESET_SCHEMA_VERSION",
    "DataPresetError",
    "EligibleMarket",
    "LiquidityDataPreset",
    "MarketType",
    "SettlementType",
    "build_binance_futures_data_preset",
    "build_bybit_data_preset",
    "build_okx_futures_data_preset",
    "build_binance_okx_futures_data_preset",
    "component_data_presets",
    "liquidity_data_preset_from_canonical_dict",
]

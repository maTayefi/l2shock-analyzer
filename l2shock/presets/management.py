# l2shock/presets/management.py
"""Application-facing management of immutable liquidity data presets.

Semantic preset content is immutable:

- creating new semantic content creates a new preset hash;
- editing means publishing a new preset identity;
- enable/disable is operational state;
- deletion is allowed only for disabled, unreferenced presets;
- deleting a preset never deletes analytical hourly rows.

All functions in this module own their short PostgreSQL transaction and return
primitive immutable DTOs suitable for transfer to the NiceGUI event loop.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from l2shock.db.analytical_repository import (
    AnalyticalRepository,
    AnalyticalRepositoryError,
    AnalyticalRowNotFoundError,
    PresetDeletionBlockedError,
)
from l2shock.db.engine import session_scope
from l2shock.presets.identity import (
    LiquidityDataPreset,
    build_binance_futures_data_preset,
    build_binance_okx_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
    liquidity_data_preset_from_canonical_dict,
)


class PresetManagementError(ValueError):
    """A preset-management request violates the current V1 contract."""


class PresetMarketProfile(StrEnum):
    """Market compositions supported by the current Settings editor."""

    BINANCE_FUTURES = "binance_futures"
    BYBIT = "bybit"
    OKX_FUTURES = "okx_futures"
    BINANCE_OKX_FUTURES = "binance_okx_futures"

    @property
    def label(self) -> str:
        return {
            PresetMarketProfile.BINANCE_FUTURES: "Binance Futures",
            PresetMarketProfile.BYBIT: "Bybit",
            PresetMarketProfile.OKX_FUTURES: "OKX Futures",
            PresetMarketProfile.BINANCE_OKX_FUTURES: (
                "Binance + OKX Futures aggregate"
            ),
        }[self]


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
        raise PresetManagementError(
            f"{field_name} must be a canonical lowercase SHA-256"
        )

    return digest


def _normalized_base(value: object) -> str:
    base = str(value or "").strip().upper()

    if base not in {"BTC", "ETH"}:
        raise PresetManagementError("base must be BTC or ETH")

    return base


def _normalized_market_profile(
    value: object,
) -> PresetMarketProfile:
    try:
        return PresetMarketProfile(str(value or "").strip().lower())
    except ValueError as exc:
        raise PresetManagementError(
            "market_profile must be binance_futures, bybit, "
            "okx_futures, or binance_okx_futures"
        ) from exc


def _market_signature(
    preset: LiquidityDataPreset,
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (
            market.provider,
            market.venue,
            market.instrument,
        )
        for market in preset.eligible_markets
    )


def _market_profile_from_config(
    config_json: Mapping[str, object],
    *,
    base: str,
) -> PresetMarketProfile | None:
    """Recognize only canonical market compositions owned by this editor."""

    normalized_base = _normalized_base(base)

    try:
        preset = liquidity_data_preset_from_canonical_dict(config_json)
    except TypeError, ValueError:
        return None

    if preset.base != normalized_base:
        return None

    lower = preset.band.lower_fraction
    upper = preset.band.upper_fraction

    expected = {
        PresetMarketProfile.BINANCE_FUTURES: (
            build_binance_futures_data_preset(
                base=normalized_base,
                lower_fraction=lower,
                upper_fraction=upper,
            )
        ),
        PresetMarketProfile.BYBIT: (
            build_bybit_data_preset(
                base=normalized_base,
                lower_fraction=lower,
                upper_fraction=upper,
            )
        ),
        PresetMarketProfile.OKX_FUTURES: (
            build_okx_futures_data_preset(
                base=normalized_base,
                lower_fraction=lower,
                upper_fraction=upper,
            )
        ),
        PresetMarketProfile.BINANCE_OKX_FUTURES: (
            build_binance_okx_futures_data_preset(
                base=normalized_base,
                lower_fraction=lower,
                upper_fraction=upper,
            )
        ),
    }

    actual_signature = _market_signature(preset)

    for profile, expected_preset in expected.items():
        if actual_signature == _market_signature(expected_preset):
            return profile

    return None


def _fraction(
    field_name: str,
    value: object,
) -> Decimal:
    text = str(value or "").strip()

    if not text:
        raise PresetManagementError(f"{field_name} is required")

    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise PresetManagementError(
            f"{field_name} must be a valid decimal fraction"
        ) from exc

    if not result.is_finite():
        raise PresetManagementError(f"{field_name} must be finite")

    return result


def _depth_values(
    config_json: Mapping[str, object],
) -> tuple[Decimal, Decimal]:
    raw_depth = config_json.get("depth_band")

    if not isinstance(raw_depth, Mapping):
        raise PresetManagementError(
            "Stored preset does not contain a valid depth_band object"
        )

    lower = _fraction(
        "stored lower_fraction",
        raw_depth.get("lower_fraction"),
    )
    upper = _fraction(
        "stored upper_fraction",
        raw_depth.get("upper_fraction"),
    )

    return lower, upper


def _initial_market_supported(
    config_json: Mapping[str, object],
    *,
    base: str,
) -> bool:
    """Return whether the current editor can reproduce the preset exactly."""

    return (
        _market_profile_from_config(
            config_json,
            base=base,
        )
        is not None
    )


@dataclass(frozen=True, slots=True)
class ManagedPreset:
    """Small immutable preset summary safe for UI/event-loop ownership."""

    preset_hash: str
    base: str
    algorithm_version: str
    enabled: bool
    lower_fraction: Decimal
    upper_fraction: Decimal
    current_editor_supported: bool
    created_at: datetime
    market_profile: PresetMarketProfile | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "preset_hash",
            _canonical_sha256(
                "preset_hash",
                self.preset_hash,
            ),
        )
        object.__setattr__(
            self,
            "base",
            _normalized_base(self.base),
        )

        algorithm = str(self.algorithm_version or "").strip()

        if not algorithm:
            raise PresetManagementError("algorithm_version cannot be blank")

        if not isinstance(self.enabled, bool):
            raise PresetManagementError("enabled must be bool")

        if not isinstance(self.current_editor_supported, bool):
            raise PresetManagementError("current_editor_supported must be bool")

        profile = self.market_profile

        if profile is not None:
            try:
                profile = PresetMarketProfile(profile)
            except (TypeError, ValueError) as exc:
                raise PresetManagementError("market_profile is unsupported") from exc

            object.__setattr__(
                self,
                "market_profile",
                profile,
            )

        if self.current_editor_supported != (profile is not None):
            raise PresetManagementError(
                "current_editor_supported disagrees with market_profile"
            )

        for field_name in (
            "lower_fraction",
            "upper_fraction",
        ):
            value = getattr(self, field_name)

            if not isinstance(value, Decimal) or not value.is_finite():
                raise PresetManagementError(f"{field_name} must be a finite Decimal")

        if self.lower_fraction < 0:
            raise PresetManagementError("lower_fraction must be non-negative")

        if self.lower_fraction > self.upper_fraction:
            raise PresetManagementError("lower_fraction cannot exceed upper_fraction")

        if self.upper_fraction >= Decimal("1"):
            raise PresetManagementError("upper_fraction must be less than 1")

        if not isinstance(self.created_at, datetime):
            raise PresetManagementError("created_at must be a datetime")

        object.__setattr__(
            self,
            "algorithm_version",
            algorithm,
        )

    @property
    def label(self) -> str:
        enabled_text = "enabled" if self.enabled else "disabled"
        profile_text = (
            self.market_profile.label
            if self.market_profile is not None
            else "Unsupported market composition"
        )

        return (
            f"{self.base} | {profile_text} | depth "
            f"{self.lower_fraction}..{self.upper_fraction} | "
            f"{enabled_text} | {self.preset_hash[:12]}"
        )


@dataclass(frozen=True, slots=True)
class ManagedPresetWriteResult:
    """Result of creating or finding one immutable semantic preset."""

    preset: ManagedPreset
    inserted: bool

    def __post_init__(self) -> None:
        if not isinstance(self.preset, ManagedPreset):
            raise PresetManagementError("preset must be ManagedPreset")

        if not isinstance(self.inserted, bool):
            raise PresetManagementError("inserted must be bool")


def _managed_from_persisted(value) -> ManagedPreset:
    config = dict(value.config_json)
    lower, upper = _depth_values(config)
    profile = _market_profile_from_config(
        config,
        base=value.base,
    )

    return ManagedPreset(
        preset_hash=value.preset_hash,
        base=value.base,
        algorithm_version=value.algorithm_version,
        enabled=value.enabled,
        lower_fraction=lower,
        upper_fraction=upper,
        current_editor_supported=profile is not None,
        created_at=value.created_at,
        market_profile=profile,
    )


def _preset(
    *,
    base: object,
    lower_fraction: object,
    upper_fraction: object,
    market_profile: object = PresetMarketProfile.BINANCE_FUTURES,
) -> LiquidityDataPreset:
    normalized_base = _normalized_base(base)
    profile = _normalized_market_profile(market_profile)
    lower = _fraction(
        "Depth lower fraction",
        lower_fraction,
    )
    upper = _fraction(
        "Depth upper fraction",
        upper_fraction,
    )

    builder = {
        PresetMarketProfile.BINANCE_FUTURES: (build_binance_futures_data_preset),
        PresetMarketProfile.BYBIT: build_bybit_data_preset,
        PresetMarketProfile.OKX_FUTURES: build_okx_futures_data_preset,
        PresetMarketProfile.BINANCE_OKX_FUTURES: (
            build_binance_okx_futures_data_preset
        ),
    }[profile]

    try:
        return builder(
            base=normalized_base,
            lower_fraction=lower,
            upper_fraction=upper,
        )
    except ValueError as exc:
        raise PresetManagementError(str(exc)) from exc


def list_managed_presets(
    *,
    base: str | None = None,
) -> tuple[ManagedPreset, ...]:
    """Return all persisted presets in deterministic repository order."""

    normalized_base = _normalized_base(base) if base is not None else None

    with session_scope() as session:
        values = AnalyticalRepository(session).list_presets(
            base=normalized_base,
            enabled_only=False,
        )

        return tuple(_managed_from_persisted(value) for value in values)


def create_managed_preset(
    *,
    base: object,
    lower_fraction: object,
    upper_fraction: object,
    market_profile: object = PresetMarketProfile.BINANCE_FUTURES,
    enabled: bool = True,
) -> ManagedPresetWriteResult:
    """Create the current validated single-market preset identity."""

    if not isinstance(enabled, bool):
        raise PresetManagementError("enabled must be bool")

    preset = _preset(
        base=base,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
        market_profile=market_profile,
    )

    with session_scope() as session:
        result = AnalyticalRepository(session).ensure_preset(
            preset,
            enabled=enabled,
        )

        return ManagedPresetWriteResult(
            preset=_managed_from_persisted(result.preset),
            inserted=result.inserted,
        )


def create_edited_preset(
    *,
    source_preset_hash: object,
    lower_fraction: object,
    upper_fraction: object,
    market_profile: object | None = None,
    enabled: bool = True,
) -> ManagedPresetWriteResult:
    """Create a new immutable semantic version from an editor-supported preset.

    The source preset is never overwritten. Only the currently validated
    Binance Futures single-market editor is supported in V1.
    """

    if not isinstance(enabled, bool):
        raise PresetManagementError("enabled must be bool")

    digest = _canonical_sha256(
        "source_preset_hash",
        source_preset_hash,
    )

    with session_scope() as session:
        repository = AnalyticalRepository(session)
        source = repository.get_preset(digest)

        if source is None:
            raise PresetManagementError("Source data preset does not exist")

        managed_source = _managed_from_persisted(source)

        if (
            not managed_source.current_editor_supported
            or managed_source.market_profile is None
        ):
            raise PresetManagementError(
                "The selected preset uses a market configuration that the "
                "current editor cannot safely reinterpret"
            )

        selected_profile = (
            managed_source.market_profile
            if market_profile is None
            else _normalized_market_profile(market_profile)
        )

        preset = _preset(
            base=managed_source.base,
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
            market_profile=selected_profile,
        )
        result = repository.ensure_preset(
            preset,
            enabled=enabled,
        )

        return ManagedPresetWriteResult(
            preset=_managed_from_persisted(result.preset),
            inserted=result.inserted,
        )


def set_managed_preset_enabled(
    preset_hash: object,
    *,
    enabled: bool,
) -> ManagedPreset:
    """Change operational materialization eligibility only."""

    if not isinstance(enabled, bool):
        raise PresetManagementError("enabled must be bool")

    digest = _canonical_sha256(
        "preset_hash",
        preset_hash,
    )

    with session_scope() as session:
        result = AnalyticalRepository(session).set_preset_enabled(
            digest,
            enabled=enabled,
        )

        return _managed_from_persisted(result)


def delete_managed_preset(
    preset_hash: object,
) -> ManagedPreset:
    """Delete one disabled preset only when no L2 row references it."""

    digest = _canonical_sha256(
        "preset_hash",
        preset_hash,
    )

    with session_scope() as session:
        try:
            removed = AnalyticalRepository(session).delete_preset_if_unreferenced(
                digest
            )
        except (
            AnalyticalRepositoryError,
            AnalyticalRowNotFoundError,
            PresetDeletionBlockedError,
        ) as exc:
            raise PresetManagementError(str(exc)) from exc

        return _managed_from_persisted(removed)


__all__ = [
    "ManagedPreset",
    "ManagedPresetWriteResult",
    "PresetManagementError",
    "PresetMarketProfile",
    "create_edited_preset",
    "create_managed_preset",
    "delete_managed_preset",
    "list_managed_presets",
    "set_managed_preset_enabled",
]

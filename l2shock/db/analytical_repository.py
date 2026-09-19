# l2shock/db/analytical_repository.py
"""PostgreSQL persistence for immutable liquidity presets and hourly blocks.

This repository owns compact analytical persistence only.

It must never persist raw L2 events. Raw source rows remain in local Parquet
archives.

Transaction ownership belongs to the caller. Repository methods may execute
statements and flush ORM state, but they do not commit or roll back sessions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Final
from collections.abc import Mapping, Sequence

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.orm import Session

from l2shock.db.checkpoint_reference_locks import (
    acquire_checkpoint_reference_transaction_locks,
)
from l2shock.db.models import DataPreset, L2HourlySeries
from l2shock.timeutils import require_utc_hour

L2_PROVENANCE_SCHEMA: Final[str] = "l2shock.l2_hourly_series_provenance"
L2_PROVENANCE_SCHEMA_VERSION: Final[int] = 1

L2_QUALITY_SUMMARY_SCHEMA: Final[str] = "l2shock.liquidity_quality_summary"
L2_QUALITY_SUMMARY_SCHEMA_VERSION: Final[int] = 1

_SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


class AnalyticalRepositoryError(RuntimeError):
    """Base error for compact analytical persistence."""


class PresetIdentityConflictError(AnalyticalRepositoryError):
    """An existing preset hash owns different canonical semantic content."""


class HourlySeriesConflictError(AnalyticalRepositoryError):
    """An hourly identity already owns different analytical content."""


class AnalyticalRowCorruptionError(AnalyticalRepositoryError):
    """A stored analytical row violates its persistence contract."""


class AnalyticalRowNotFoundError(AnalyticalRepositoryError):
    """A requested analytical identity does not exist."""


class PresetDeletionBlockedError(AnalyticalRepositoryError):
    """A data preset cannot be deleted without violating durable ownership."""


def _sha256_text(
    field_name: str,
    value: object,
    *,
    nullable: bool = False,
) -> str | None:
    text = str(value or "").strip()

    if not text and nullable:
        return None

    if not _SHA256_RE.fullmatch(text):
        raise ValueError(
            f"{field_name} must contain exactly 64 canonical lowercase "
            "hexadecimal characters"
        )

    return text


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
        raise ValueError(f"{field_name} cannot be blank")

    if len(text) > 128 or not _IDENTITY_RE.fullmatch(text):
        raise ValueError(f"{field_name} contains unsupported identity characters")

    return text


def _positive_int(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")

    if value <= 0:
        raise ValueError(f"{field_name} must be positive")

    return value


def _nonnegative_int(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")

    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")

    return value


def _json_object(
    field_name: str,
    value: Mapping[str, Any] | dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object mapping")

    result = dict(value)

    for key in result:
        if not isinstance(key, str):
            raise ValueError(f"{field_name} keys must be strings")

    return result


def _canonical_utc_hour_from_json(
    field_name: str,
    value: object,
) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a canonical UTC string")

    text = value.strip()

    if not text.endswith("Z"):
        raise ValueError(f"{field_name} must end with canonical UTC suffix 'Z'")

    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field_name} is not valid ISO-8601 UTC text") from exc

    hour = require_utc_hour(field_name, parsed)
    canonical = hour.isoformat().replace("+00:00", "Z")

    if text != canonical:
        raise ValueError(f"{field_name} is not canonically encoded")

    return hour


@dataclass(frozen=True, slots=True)
class SourceHourReference:
    """Exact source-archive identity contributing to an analytical hour.

    Local paths are deliberately excluded. A local path is an operational
    location, not durable analytical identity.
    """

    provider: str
    venue: str
    instrument: str
    hour_utc: datetime
    content_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider",
            _identity_text(
                "provider",
                self.provider,
                lowercase=True,
            ),
        )
        object.__setattr__(
            self,
            "venue",
            _identity_text(
                "venue",
                self.venue,
                lowercase=True,
            ),
        )
        object.__setattr__(
            self,
            "instrument",
            _identity_text(
                "instrument",
                self.instrument,
                uppercase=True,
            ),
        )
        object.__setattr__(
            self,
            "hour_utc",
            require_utc_hour(
                "hour_utc",
                self.hour_utc,
            ),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256_text(
                "content_sha256",
                self.content_sha256,
            ),
        )

    @property
    def archive_identity_tuple(
        self,
    ) -> tuple[str, str, str, datetime]:
        """Logical archive identity excluding its claimed content hash."""
        return (
            self.provider,
            self.venue,
            self.instrument,
            self.hour_utc,
        )

    @property
    def identity_tuple(
        self,
    ) -> tuple[str, str, str, datetime, str]:
        return (
            *self.archive_identity_tuple,
            self.content_sha256,
        )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> SourceHourReference:
        payload = _json_object(
            "source hour reference",
            value,
        )

        expected_keys = {
            "provider",
            "venue",
            "instrument",
            "data_kind",
            "hour_utc",
            "content_sha256",
        }

        if set(payload) != expected_keys:
            raise ValueError(
                "L2 source-hour provenance fields do not match "
                "the version-1 contract"
            )

        if payload["data_kind"] != "orderbook":
            raise ValueError("L2 source-hour provenance requires data_kind='orderbook'")

        result = cls(
            provider=payload["provider"],
            venue=payload["venue"],
            instrument=payload["instrument"],
            hour_utc=_canonical_utc_hour_from_json(
                "source hour_utc",
                payload["hour_utc"],
            ),
            content_sha256=payload["content_sha256"],
        )

        if result.to_dict() != payload:
            raise ValueError("L2 source-hour provenance is not canonical")

        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "instrument": self.instrument,
            "data_kind": "orderbook",
            "hour_utc": self.hour_utc.isoformat().replace(
                "+00:00",
                "Z",
            ),
            "content_sha256": self.content_sha256,
        }


@dataclass(frozen=True, slots=True)
class L2HourlyProvenance:
    """Typed provenance for one compact hourly liquidity result.

    ``source_hours`` identifies current-hour order-book archives whose replay
    contributed to this output.

    ``checkpoint_content_sha256`` identifies an optional serialized checkpoint
    used to initialize replay. The checkpoint remains subject to its own source
    identity and immediately-following-hour ownership contract.
    """

    source_hours: tuple[SourceHourReference, ...]
    checkpoint_content_sha256: str | None = None
    replay_schema_version: int = 1
    liquidity_schema_version: int = 1

    schema: str = L2_PROVENANCE_SCHEMA
    schema_version: int = L2_PROVENANCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema != L2_PROVENANCE_SCHEMA:
            raise ValueError("Unsupported L2 provenance schema identity")

        if self.schema_version != L2_PROVENANCE_SCHEMA_VERSION:
            raise ValueError("Unsupported L2 provenance schema version")

        _positive_int(
            "replay_schema_version",
            self.replay_schema_version,
        )
        _positive_int(
            "liquidity_schema_version",
            self.liquidity_schema_version,
        )

        sources = tuple(self.source_hours)

        if not sources:
            raise ValueError("L2 hourly provenance requires at least one source hour")

        for source in sources:
            if not isinstance(source, SourceHourReference):
                raise ValueError(
                    "source_hours must contain SourceHourReference objects"
                )

        sorted_sources = tuple(
            sorted(
                sources,
                key=lambda item: item.identity_tuple,
            )
        )

        identities = [source.identity_tuple for source in sorted_sources]

        if len(set(identities)) != len(identities):
            raise ValueError("source_hours contains a duplicate source identity")

        archive_identities = [
            source.archive_identity_tuple for source in sorted_sources
        ]

        if len(set(archive_identities)) != len(archive_identities):
            raise ValueError(
                "source_hours contains one logical archive with "
                "conflicting content hashes"
            )

        checkpoint_hash = _sha256_text(
            "checkpoint_content_sha256",
            self.checkpoint_content_sha256,
            nullable=True,
        )

        object.__setattr__(
            self,
            "source_hours",
            sorted_sources,
        )
        object.__setattr__(
            self,
            "checkpoint_content_sha256",
            checkpoint_hash,
        )

    def validate_for_hour(
        self,
        hour_utc: datetime,
    ) -> None:
        expected_hour = require_utc_hour(
            "hour_utc",
            hour_utc,
        )
        future_sources = tuple(
            source for source in self.source_hours if source.hour_utc > expected_hour
        )
        if future_sources:
            raise ValueError(
                "L2 provenance cannot reference a source hour after the "
                "persisted analytical hour"
            )
        current_hour_sources = tuple(
            source for source in self.source_hours if source.hour_utc == expected_hour
        )
        if not current_hour_sources:
            raise ValueError(
                "L2 provenance must contain at least one order-book "
                "source for the persisted analytical hour"
            )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> L2HourlyProvenance:
        payload = _json_object(
            "provenance_json",
            value,
        )

        expected_keys = {
            "schema",
            "schema_version",
            "replay_schema_version",
            "liquidity_schema_version",
            "checkpoint_content_sha256",
            "source_hours",
        }

        if set(payload) != expected_keys:
            raise ValueError("L2 provenance fields do not match the version-1 contract")

        if payload["schema"] != L2_PROVENANCE_SCHEMA:
            raise ValueError("Unsupported L2 provenance schema identity")

        if (
            isinstance(payload["schema_version"], bool)
            or payload["schema_version"] != L2_PROVENANCE_SCHEMA_VERSION
        ):
            raise ValueError("Unsupported L2 provenance schema version")

        replay_version = _positive_int(
            "replay_schema_version",
            payload["replay_schema_version"],
        )
        liquidity_version = _positive_int(
            "liquidity_schema_version",
            payload["liquidity_schema_version"],
        )

        raw_sources = payload["source_hours"]

        if not isinstance(raw_sources, list):
            raise ValueError("L2 provenance source_hours must be a JSON array")

        result = cls(
            source_hours=tuple(
                SourceHourReference.from_dict(source) for source in raw_sources
            ),
            checkpoint_content_sha256=(payload["checkpoint_content_sha256"]),
            replay_schema_version=replay_version,
            liquidity_schema_version=liquidity_version,
            schema=payload["schema"],
            schema_version=payload["schema_version"],
        )

        if result.to_dict() != payload:
            raise ValueError("L2 provenance JSON is valid but not canonical")

        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "replay_schema_version": self.replay_schema_version,
            "liquidity_schema_version": (self.liquidity_schema_version),
            "checkpoint_content_sha256": (self.checkpoint_content_sha256),
            "source_hours": [source.to_dict() for source in self.source_hours],
        }


def _verified_l2_quality_summary(
    value: Mapping[str, Any],
    *,
    encoded: EncodedHourlyLiquidityBlocks,
) -> dict[str, Any]:
    from l2shock.ingest.sampling import BookSampleInvalidReason
    from l2shock.liquidity import decode_hourly_liquidity_blocks

    payload = _json_object(
        "quality_summary_json",
        value,
    )

    expected_keys = {
        "schema",
        "schema_version",
        "observation_count",
        "valid_count",
        "degraded_count",
        "invalid_count",
        "zero_total_liquidity_count",
        "invalid_reason_counts",
    }

    if set(payload) != expected_keys:
        raise ValueError(
            "L2 quality-summary fields do not match the " "format-version-1 contract"
        )

    if payload["schema"] != L2_QUALITY_SUMMARY_SCHEMA:
        raise ValueError("Unsupported L2 quality-summary schema")

    if (
        isinstance(payload["schema_version"], bool)
        or payload["schema_version"] != L2_QUALITY_SUMMARY_SCHEMA_VERSION
    ):
        raise ValueError("Unsupported L2 quality-summary version")

    observation_count = _nonnegative_int(
        "observation_count",
        payload["observation_count"],
    )
    valid_count = _nonnegative_int(
        "valid_count",
        payload["valid_count"],
    )
    degraded_count = _nonnegative_int(
        "degraded_count",
        payload["degraded_count"],
    )
    invalid_count = _nonnegative_int(
        "invalid_count",
        payload["invalid_count"],
    )
    zero_total_count = _nonnegative_int(
        "zero_total_liquidity_count",
        payload["zero_total_liquidity_count"],
    )

    if observation_count != 3_600:
        raise ValueError("L2 quality summary must own exactly 3,600 observations")

    if observation_count != encoded.observation_count:
        raise ValueError(
            "L2 quality summary and encoded channels disagree "
            "about observation count"
        )

    if valid_count + degraded_count + invalid_count != observation_count:
        raise ValueError("L2 quality counts must total 3,600")

    raw_reasons = payload["invalid_reason_counts"]

    if not isinstance(raw_reasons, Mapping):
        raise ValueError("L2 invalid_reason_counts must be a JSON object")

    reasons = dict(raw_reasons)

    if any(not isinstance(key, str) for key in reasons):
        raise ValueError("L2 invalid_reason_counts keys must be strings")

    normalized_reasons: dict[str, int] = {}

    for reason in BookSampleInvalidReason:
        if reason.value not in reasons:
            continue

        normalized_reasons[reason.value] = _nonnegative_int(
            f"invalid_reason_counts.{reason.value}",
            reasons[reason.value],
        )

    if set(normalized_reasons) != set(reasons):
        raise ValueError("L2 quality summary contains an unsupported invalid reason")

    decoded = decode_hourly_liquidity_blocks(encoded)

    if decoded.valid_count != valid_count:
        raise ValueError("Decoded L2 valid count does not match quality summary")

    if decoded.degraded_count != degraded_count:
        raise ValueError("Decoded L2 degraded count does not match quality summary")

    if decoded.invalid_count != invalid_count:
        raise ValueError("Decoded L2 invalid count does not match quality summary")

    decoded_reason_counts = {
        reason.value: sum(observed is reason for observed in decoded.invalid_reason)
        for reason in BookSampleInvalidReason
    }
    decoded_reason_counts = {
        reason: count for reason, count in decoded_reason_counts.items() if count > 0
    }

    if decoded_reason_counts != normalized_reasons:
        raise ValueError("Decoded L2 invalid reasons do not match quality summary")

    decoded_zero_total_count = sum(
        quality.value == "VALID"
        and bid is not None
        and ask is not None
        and bid + ask == 0
        for quality, bid, ask in zip(
            decoded.quality,
            decoded.bid_liquidity,
            decoded.ask_liquidity,
        )
    )

    if decoded_zero_total_count != zero_total_count:
        raise ValueError(
            "Decoded zero-total liquidity count does not match " "quality summary"
        )

    return {
        "schema": L2_QUALITY_SUMMARY_SCHEMA,
        "schema_version": L2_QUALITY_SUMMARY_SCHEMA_VERSION,
        "observation_count": observation_count,
        "valid_count": valid_count,
        "degraded_count": degraded_count,
        "invalid_count": invalid_count,
        "zero_total_liquidity_count": zero_total_count,
        "invalid_reason_counts": normalized_reasons,
    }


@dataclass(frozen=True, slots=True)
class PersistedDataPreset:
    """Primitive immutable view of one stored preset row."""

    id: int
    preset_hash: str
    schema_version: int
    algorithm_version: str
    base: str
    enabled: bool
    config_json: Mapping[str, Any]
    created_at: datetime

    @classmethod
    def from_model(
        cls,
        model: DataPreset,
    ) -> PersistedDataPreset:
        return cls(
            id=int(model.id),
            preset_hash=str(model.preset_hash),
            schema_version=int(model.schema_version),
            algorithm_version=str(model.algorithm_version),
            base=str(model.base),
            enabled=bool(model.enabled),
            config_json=MappingProxyType(dict(model.config_json)),
            created_at=model.created_at,
        )


@dataclass(frozen=True, slots=True)
class PresetWriteResult:
    """Result of an idempotent preset insertion."""

    preset: PersistedDataPreset
    inserted: bool


@dataclass(frozen=True, slots=True)
class PersistedL2HourlySeries:
    """Verified primitive view of one stored hourly liquidity row."""

    base: str
    hour_utc: datetime
    preset_hash: str
    schema_version: int
    sampling_interval_ms: int
    observation_count: int
    quality_summary_json: Mapping[str, Any]
    provenance_json: Mapping[str, Any]
    encoded: EncodedHourlyLiquidityBlocks
    created_at: datetime

    @classmethod
    def from_model(
        cls,
        model: L2HourlySeries,
        *,
        verify_codec: bool = True,
    ) -> PersistedL2HourlySeries:
        from l2shock.liquidity import EncodedHourlyLiquidityBlocks

        try:
            encoded = EncodedHourlyLiquidityBlocks(
                format_version=int(model.schema_version),
                codec=str(model.codec),
                observation_count=int(model.observation_count),
                bid_liquidity_block=bytes(model.bid_liquidity_block),
                ask_liquidity_block=bytes(model.ask_liquidity_block),
                validity_block=bytes(model.validity_block),
                source_count_block=bytes(model.source_count_block),
                content_sha256=str(model.content_sha256),
            )
        except Exception as exc:
            raise AnalyticalRowCorruptionError(
                "Stored L2 hourly row contains an invalid encoded artifact"
            ) from exc

        if int(model.sampling_interval_ms) != 1_000:
            raise AnalyticalRowCorruptionError(
                "Stored L2 hourly row has an unsupported sampling interval"
            )

        if int(model.observation_count) != 3_600:
            raise AnalyticalRowCorruptionError(
                "Stored L2 hourly row has an unsupported observation count"
            )

        try:
            hour_utc = require_utc_hour(
                "stored hour_utc",
                model.hour_utc,
            )
        except Exception as exc:
            raise AnalyticalRowCorruptionError(
                "Stored L2 hourly row has an invalid UTC-hour identity"
            ) from exc

        if verify_codec:
            try:
                quality = _verified_l2_quality_summary(
                    dict(model.quality_summary_json),
                    encoded=encoded,
                )
                provenance = L2HourlyProvenance.from_dict(dict(model.provenance_json))
                provenance.validate_for_hour(hour_utc)
            except Exception as exc:
                raise AnalyticalRowCorruptionError(
                    "Stored L2 hourly row failed codec, quality-summary, "
                    "or provenance verification"
                ) from exc
        else:
            quality = dict(model.quality_summary_json)
            provenance_json = dict(model.provenance_json)

        if verify_codec:
            provenance_json = provenance.to_dict()

        return cls(
            base=str(model.base),
            hour_utc=hour_utc,
            preset_hash=str(model.preset_hash),
            schema_version=int(model.schema_version),
            sampling_interval_ms=int(model.sampling_interval_ms),
            observation_count=int(model.observation_count),
            quality_summary_json=MappingProxyType(quality),
            provenance_json=MappingProxyType(provenance_json),
            encoded=encoded,
            created_at=model.created_at,
        )


@dataclass(frozen=True, slots=True)
class HourlySeriesWriteResult:
    """Result of an idempotent hourly-series insertion."""

    series: PersistedL2HourlySeries
    inserted: bool


def _preset_values(
    preset: LiquidityDataPreset,
    *,
    enabled: bool,
) -> dict[str, object]:
    from l2shock.presets import LiquidityDataPreset

    if not isinstance(preset, LiquidityDataPreset):
        raise TypeError("preset must be a LiquidityDataPreset")

    if not isinstance(enabled, bool):
        raise TypeError("enabled must be bool")

    return {
        "preset_hash": preset.preset_hash,
        "schema_version": preset.schema_version,
        "algorithm_version": preset.algorithm_version,
        "base": preset.base,
        "enabled": enabled,
        "config_json": preset.to_canonical_dict(),
    }


def _assert_preset_identity_matches(
    model: DataPreset,
    preset: LiquidityDataPreset,
) -> None:
    expected = _preset_values(
        preset,
        enabled=bool(model.enabled),
    )

    actual_identity = {
        "preset_hash": str(model.preset_hash),
        "schema_version": int(model.schema_version),
        "algorithm_version": str(model.algorithm_version),
        "base": str(model.base),
        "config_json": dict(model.config_json),
    }
    expected_identity = {
        key: value for key, value in expected.items() if key != "enabled"
    }

    if actual_identity != expected_identity:
        raise PresetIdentityConflictError(
            "Existing preset hash owns different immutable semantic content"
        )


def _hourly_identity_statement(
    *,
    base: str,
    hour_utc: datetime,
    preset_hash: str,
) -> Select[tuple[L2HourlySeries]]:
    return select(L2HourlySeries).where(
        L2HourlySeries.base == base,
        L2HourlySeries.hour_utc == hour_utc,
        L2HourlySeries.preset_hash == preset_hash,
    )


def _assert_hourly_content_matches(
    model: L2HourlySeries,
    *,
    base: str,
    hour_utc: datetime,
    preset_hash: str,
    encoded: EncodedHourlyLiquidityBlocks,
    quality_summary_json: dict[str, Any],
    provenance_json: dict[str, Any],
) -> None:
    existing = {
        "base": str(model.base),
        "hour_utc": require_utc_hour(
            "existing hour_utc",
            model.hour_utc,
        ),
        "preset_hash": str(model.preset_hash),
        "schema_version": int(model.schema_version),
        "sampling_interval_ms": int(model.sampling_interval_ms),
        "observation_count": int(model.observation_count),
        "codec": str(model.codec),
        "bid_liquidity_block": bytes(model.bid_liquidity_block),
        "ask_liquidity_block": bytes(model.ask_liquidity_block),
        "validity_block": bytes(model.validity_block),
        "source_count_block": bytes(model.source_count_block),
        "quality_summary_json": dict(model.quality_summary_json),
        "provenance_json": dict(model.provenance_json),
        "content_sha256": str(model.content_sha256),
    }

    requested = {
        "base": base,
        "hour_utc": hour_utc,
        "preset_hash": preset_hash,
        "schema_version": encoded.format_version,
        "sampling_interval_ms": 1_000,
        "observation_count": encoded.observation_count,
        "codec": encoded.codec,
        "bid_liquidity_block": encoded.bid_liquidity_block,
        "ask_liquidity_block": encoded.ask_liquidity_block,
        "validity_block": encoded.validity_block,
        "source_count_block": encoded.source_count_block,
        "quality_summary_json": quality_summary_json,
        "provenance_json": provenance_json,
        "content_sha256": encoded.content_sha256,
    }

    if existing != requested:
        raise HourlySeriesConflictError(
            "Existing L2 hourly identity owns different content, "
            "quality metadata, or provenance"
        )


class AnalyticalRepository:
    """Persistence operations for presets and compact L2 hourly series."""

    def __init__(self, session: Session) -> None:
        if not isinstance(session, Session):
            raise TypeError("AnalyticalRepository requires a SQLAlchemy Session")

        self._session = session

    def get_preset(
        self,
        preset_hash: str,
    ) -> PersistedDataPreset | None:
        normalized_hash = _sha256_text(
            "preset_hash",
            preset_hash,
        )

        model = self._session.scalar(
            select(DataPreset).where(DataPreset.preset_hash == normalized_hash)
        )

        if model is None:
            return None

        return PersistedDataPreset.from_model(model)

    def list_presets(
        self,
        *,
        base: str | None = None,
        enabled_only: bool = False,
    ) -> tuple[PersistedDataPreset, ...]:
        """Return presets in deterministic base/creation/hash order.

        This is a read-only selection boundary for UI and analysis planning.
        It does not enable, disable, insert, or otherwise mutate a preset.
        """
        if not isinstance(enabled_only, bool):
            raise TypeError("enabled_only must be bool")

        statement = select(DataPreset)

        if base is not None:
            normalized_base = _identity_text(
                "base",
                base,
                uppercase=True,
            )

            if normalized_base not in {"BTC", "ETH"}:
                raise ValueError("base must be BTC or ETH")

            statement = statement.where(
                DataPreset.base == normalized_base,
            )

        if enabled_only:
            statement = statement.where(
                DataPreset.enabled.is_(True),
            )

        models: Sequence[DataPreset] = (
            self._session.scalars(
                statement.order_by(
                    DataPreset.base,
                    DataPreset.created_at,
                    DataPreset.preset_hash,
                )
            )
            .unique()
            .all()
        )

        return tuple(PersistedDataPreset.from_model(model) for model in models)

    def ensure_preset(
        self,
        preset: LiquidityDataPreset,
        *,
        enabled: bool = True,
    ) -> PresetWriteResult:
        values = _preset_values(
            preset,
            enabled=enabled,
        )

        statement = (
            postgresql_insert(DataPreset)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[DataPreset.preset_hash])
            .returning(DataPreset.id)
        )

        inserted_id = self._session.execute(statement).scalar_one_or_none()

        model = self._session.scalar(
            select(DataPreset).where(DataPreset.preset_hash == preset.preset_hash)
        )

        if model is None:
            raise AnalyticalRepositoryError(
                "Preset insertion completed without a readable row"
            )

        _assert_preset_identity_matches(model, preset)

        return PresetWriteResult(
            preset=PersistedDataPreset.from_model(model),
            inserted=inserted_id is not None,
        )

    def set_preset_enabled(
        self,
        preset_hash: str,
        *,
        enabled: bool,
    ) -> PersistedDataPreset:
        normalized_hash = _sha256_text(
            "preset_hash",
            preset_hash,
        )

        if not isinstance(enabled, bool):
            raise TypeError("enabled must be bool")

        model = self._session.scalar(
            select(DataPreset)
            .where(DataPreset.preset_hash == normalized_hash)
            .with_for_update()
        )

        if model is None:
            raise AnalyticalRowNotFoundError("Data preset does not exist")

        model.enabled = enabled
        self._session.flush()

        return PersistedDataPreset.from_model(model)

    def delete_preset_if_unreferenced(
        self,
        preset_hash: str,
    ) -> PersistedDataPreset:
        """Delete one disabled preset only when no analytical row references it.

        Semantic preset deletion never cascades into L2 hourly-series deletion.
        A preset which owns historical analytical rows remains durable.
        """
        normalized_hash = _sha256_text(
            "preset_hash",
            preset_hash,
        )

        model = self._session.scalar(
            select(DataPreset)
            .where(DataPreset.preset_hash == normalized_hash)
            .with_for_update()
        )

        if model is None:
            raise AnalyticalRowNotFoundError("Data preset does not exist")

        if bool(model.enabled):
            raise PresetDeletionBlockedError(
                "Disable the data preset before deleting it"
            )

        referenced = self._session.scalar(
            select(L2HourlySeries.preset_hash)
            .where(
                L2HourlySeries.preset_hash == normalized_hash,
            )
            .limit(1)
        )

        if referenced is not None:
            raise PresetDeletionBlockedError(
                "The data preset owns historical L2 hourly rows and cannot "
                "be deleted without analytical-data deletion"
            )

        removed = PersistedDataPreset.from_model(model)

        self._session.delete(model)
        self._session.flush()

        return removed

    def write_l2_hour(
        self,
        *,
        preset: LiquidityDataPreset,
        hour_utc: datetime,
        encoded: EncodedHourlyLiquidityBlocks,
        quality_summary_json: Mapping[str, Any],
        provenance: L2HourlyProvenance,
    ) -> HourlySeriesWriteResult:
        from l2shock.liquidity import (
            HOURLY_BLOCK_CODEC,
            EncodedHourlyLiquidityBlocks,
            verify_hourly_liquidity_blocks,
        )
        from l2shock.presets import LiquidityDataPreset

        if not isinstance(preset, LiquidityDataPreset):
            raise TypeError("preset must be a LiquidityDataPreset")

        if not isinstance(
            encoded,
            EncodedHourlyLiquidityBlocks,
        ):
            raise TypeError("encoded must be EncodedHourlyLiquidityBlocks")

        if not isinstance(provenance, L2HourlyProvenance):
            raise TypeError("provenance must be L2HourlyProvenance")

        normalized_hour = require_utc_hour(
            "hour_utc",
            hour_utc,
        )
        provenance.validate_for_hour(normalized_hour)

        acquire_checkpoint_reference_transaction_locks(
            self._session,
            (provenance.checkpoint_content_sha256,),
        )

        if encoded.codec != HOURLY_BLOCK_CODEC:
            raise ValueError("Encoded hourly block uses an unsupported codec")

        if encoded.observation_count != 3_600:
            raise ValueError(
                "Encoded hourly block must contain exactly 3,600 observations"
            )

        try:
            verify_hourly_liquidity_blocks(encoded)
        except Exception as exc:
            raise AnalyticalRepositoryError(
                "Refusing to persist an invalid encoded hourly block"
            ) from exc

        quality_json = _verified_l2_quality_summary(
            quality_summary_json,
            encoded=encoded,
        )
        provenance_json = provenance.to_dict()

        stored_preset = self._session.scalar(
            select(DataPreset).where(DataPreset.preset_hash == preset.preset_hash)
        )

        if stored_preset is None:
            raise AnalyticalRowNotFoundError(
                "The data preset must be persisted before its hourly series"
            )

        _assert_preset_identity_matches(
            stored_preset,
            preset,
        )

        if str(stored_preset.base) != preset.base:
            raise PresetIdentityConflictError(
                "Stored preset base does not match the requested series base"
            )

        values = {
            "base": preset.base,
            "hour_utc": normalized_hour,
            "preset_hash": preset.preset_hash,
            "schema_version": encoded.format_version,
            "sampling_interval_ms": 1_000,
            "observation_count": encoded.observation_count,
            "codec": encoded.codec,
            "bid_liquidity_block": (encoded.bid_liquidity_block),
            "ask_liquidity_block": (encoded.ask_liquidity_block),
            "validity_block": encoded.validity_block,
            "source_count_block": (encoded.source_count_block),
            "quality_summary_json": quality_json,
            "provenance_json": provenance_json,
            "content_sha256": encoded.content_sha256,
        }

        statement = (
            postgresql_insert(L2HourlySeries)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=[
                    L2HourlySeries.base,
                    L2HourlySeries.hour_utc,
                    L2HourlySeries.preset_hash,
                ]
            )
            .returning(L2HourlySeries.content_sha256)
        )

        inserted_hash = self._session.execute(statement).scalar_one_or_none()

        model = self._session.scalar(
            _hourly_identity_statement(
                base=preset.base,
                hour_utc=normalized_hour,
                preset_hash=preset.preset_hash,
            )
        )

        if model is None:
            raise AnalyticalRepositoryError(
                "Hourly insertion completed without a readable row"
            )

        _assert_hourly_content_matches(
            model,
            base=preset.base,
            hour_utc=normalized_hour,
            preset_hash=preset.preset_hash,
            encoded=encoded,
            quality_summary_json=quality_json,
            provenance_json=provenance_json,
        )

        return HourlySeriesWriteResult(
            series=PersistedL2HourlySeries.from_model(
                model,
                verify_codec=True,
            ),
            inserted=inserted_hash is not None,
        )

    def get_l2_hour(
        self,
        *,
        base: str,
        hour_utc: datetime,
        preset_hash: str,
        verify_codec: bool = True,
    ) -> PersistedL2HourlySeries | None:
        normalized_base = _identity_text(
            "base",
            base,
            uppercase=True,
        )

        if normalized_base not in {"BTC", "ETH"}:
            raise ValueError("base must be BTC or ETH")

        normalized_hour = require_utc_hour(
            "hour_utc",
            hour_utc,
        )
        normalized_hash = _sha256_text(
            "preset_hash",
            preset_hash,
        )

        model = self._session.scalar(
            _hourly_identity_statement(
                base=normalized_base,
                hour_utc=normalized_hour,
                preset_hash=normalized_hash,
            )
        )

        if model is None:
            return None

        return PersistedL2HourlySeries.from_model(
            model,
            verify_codec=verify_codec,
        )

    def list_l2_hours(
        self,
        *,
        base: str,
        preset_hash: str,
        start_utc: datetime,
        end_utc: datetime,
        verify_codec: bool = True,
    ) -> tuple[PersistedL2HourlySeries, ...]:
        """Return rows in half-open ``[start_utc, end_utc)`` order."""
        normalized_base = _identity_text(
            "base",
            base,
            uppercase=True,
        )

        if normalized_base not in {"BTC", "ETH"}:
            raise ValueError("base must be BTC or ETH")

        normalized_hash = _sha256_text(
            "preset_hash",
            preset_hash,
        )
        normalized_start = require_utc_hour(
            "start_utc",
            start_utc,
        )
        normalized_end = require_utc_hour(
            "end_utc",
            end_utc,
        )

        if normalized_end <= normalized_start:
            raise ValueError("end_utc must be after start_utc")

        models: Sequence[L2HourlySeries] = (
            self._session.scalars(
                select(L2HourlySeries)
                .where(
                    L2HourlySeries.base == normalized_base,
                    L2HourlySeries.preset_hash == normalized_hash,
                    L2HourlySeries.hour_utc >= normalized_start,
                    L2HourlySeries.hour_utc < normalized_end,
                )
                .order_by(L2HourlySeries.hour_utc)
            )
            .unique()
            .all()
        )

        return tuple(
            PersistedL2HourlySeries.from_model(
                model,
                verify_codec=verify_codec,
            )
            for model in models
        )


__all__ = [
    "L2_PROVENANCE_SCHEMA",
    "L2_PROVENANCE_SCHEMA_VERSION",
    "AnalyticalRepository",
    "AnalyticalRepositoryError",
    "AnalyticalRowCorruptionError",
    "AnalyticalRowNotFoundError",
    "PresetDeletionBlockedError",
    "HourlySeriesConflictError",
    "HourlySeriesWriteResult",
    "L2HourlyProvenance",
    "PersistedDataPreset",
    "PersistedL2HourlySeries",
    "PresetIdentityConflictError",
    "PresetWriteResult",
    "SourceHourReference",
]

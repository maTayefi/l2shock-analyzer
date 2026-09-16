# l2shock/db/price_repository.py
"""PostgreSQL persistence for real Binance Futures trade-price hours.

The repository stores compact, immutable one-second OHLC artifacts produced
from CryptoHFTData Binance Futures trade archives.

It does not:

- construct OHLC candles;
- download source archives;
- commit or roll back caller transactions;
- promote source-hour processing status;
- substitute midpoint or spot price data;
- persist raw trade rows.

Transaction ownership belongs to the caller.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, Final
from collections.abc import Mapping, Sequence

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.orm import Session

from l2shock.db.analytical_repository import (
    AnalyticalRepositoryError,
    AnalyticalRowCorruptionError,
    AnalyticalRowNotFoundError,
)
from l2shock.db.models import PriceHourlySeries
from l2shock.price import (
    PRICE_BLOCK_CODEC,
    PRICE_BLOCK_FORMAT_VERSION,
    EncodedHourlyTradeOHLCBlocks,
    decode_hourly_trade_ohlc_blocks,
)
from l2shock.timeutils import require_utc_hour

PRICE_PROVENANCE_SCHEMA: Final[str] = "l2shock.price_hourly_series_provenance"
PRICE_PROVENANCE_SCHEMA_VERSION: Final[int] = 1

PRICE_QUALITY_SUMMARY_SCHEMA: Final[str] = "l2shock.trade_ohlc_quality_summary"
PRICE_QUALITY_SUMMARY_SCHEMA_VERSION: Final[int] = 1

_SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")

_SYMBOL_BY_BASE: Final[dict[str, str]] = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
}


class PriceHourlySeriesConflictError(AnalyticalRepositoryError):
    """A price-hour identity already owns different durable content."""


def _sha256_text(
    field_name: str,
    value: object,
) -> str:
    text = str(value or "").strip()

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
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object mapping")

    result = dict(value)

    if any(not isinstance(key, str) for key in result):
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


def _normalized_base(value: object) -> str:
    base = _identity_text(
        "base",
        value,
        uppercase=True,
    )

    if base not in _SYMBOL_BY_BASE:
        raise ValueError("base must be BTC or ETH")

    return base


@dataclass(frozen=True, slots=True)
class PriceSourceHourReference:
    """One exact CryptoHFTData trade archive used for a price hour."""

    provider: str
    venue: str
    instrument: str
    hour_utc: datetime
    content_sha256: str
    data_kind: str = "trades"

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
        data_kind = _identity_text(
            "data_kind",
            self.data_kind,
            lowercase=True,
        )

        if provider != "cryptohftdata":
            raise ValueError("V1 price provenance requires provider='cryptohftdata'")

        if venue != "binance_futures":
            raise ValueError("V1 price provenance requires venue='binance_futures'")

        if instrument not in {"BTCUSDT", "ETHUSDT"}:
            raise ValueError("V1 price provenance requires BTCUSDT or ETHUSDT")

        if data_kind != "trades":
            raise ValueError("Price provenance source data_kind must be 'trades'")

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "data_kind", data_kind)
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
    ) -> tuple[str, str, str, str, datetime]:
        """Logical trade archive identity excluding its content hash."""
        return (
            self.provider,
            self.venue,
            self.data_kind,
            self.instrument,
            self.hour_utc,
        )

    @property
    def identity_tuple(
        self,
    ) -> tuple[str, str, str, str, datetime, str]:
        return (
            *self.archive_identity_tuple,
            self.content_sha256,
        )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> PriceSourceHourReference:
        payload = _json_object(
            "price source-hour reference",
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
                "Price source-hour provenance fields do not match "
                "the version-1 contract"
            )

        result = cls(
            provider=payload["provider"],
            venue=payload["venue"],
            instrument=payload["instrument"],
            data_kind=payload["data_kind"],
            hour_utc=_canonical_utc_hour_from_json(
                "price source hour_utc",
                payload["hour_utc"],
            ),
            content_sha256=payload["content_sha256"],
        )

        if result.to_dict() != payload:
            raise ValueError("Price source-hour provenance is not canonical")

        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "instrument": self.instrument,
            "data_kind": self.data_kind,
            "hour_utc": self.hour_utc.isoformat().replace(
                "+00:00",
                "Z",
            ),
            "content_sha256": self.content_sha256,
        }


@dataclass(frozen=True, slots=True)
class PriceHourlyProvenance:
    """Typed source ownership for one trade-time-owned price hour.

    The current source archive must be included. Immediately adjacent source
    archives may also be included because trade records can later be routed
    according to their authoritative trade_time_ms rather than source-file
    hour alone.
    """

    source_hours: tuple[PriceSourceHourReference, ...]
    trade_reader_schema_version: int = 1
    trade_ohlc_schema_version: int = 1

    schema: str = PRICE_PROVENANCE_SCHEMA
    schema_version: int = PRICE_PROVENANCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema != PRICE_PROVENANCE_SCHEMA:
            raise ValueError("Unsupported price provenance schema identity")

        if self.schema_version != PRICE_PROVENANCE_SCHEMA_VERSION:
            raise ValueError("Unsupported price provenance schema version")

        for name in (
            "trade_reader_schema_version",
            "trade_ohlc_schema_version",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

        sources = tuple(self.source_hours)

        if not sources:
            raise ValueError(
                "Price hourly provenance requires at least one source hour"
            )

        for source in sources:
            if not isinstance(
                source,
                PriceSourceHourReference,
            ):
                raise ValueError(
                    "source_hours must contain " "PriceSourceHourReference objects"
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
                "source_hours contains one logical trade archive with "
                "conflicting content hashes"
            )

        object.__setattr__(
            self,
            "source_hours",
            sorted_sources,
        )

    def validate_for_hour(
        self,
        *,
        base: str,
        hour_utc: datetime,
    ) -> None:
        normalized_base = _normalized_base(base)
        target_hour = require_utc_hour(
            "hour_utc",
            hour_utc,
        )
        expected_symbol = _SYMBOL_BY_BASE[normalized_base]

        for source in self.source_hours:
            if source.instrument != expected_symbol:
                raise ValueError(
                    "Price provenance source instrument does not match "
                    "the persisted base"
                )

            distance = source.hour_utc - target_hour

            if distance not in {
                -timedelta(hours=1),
                timedelta(0),
                timedelta(hours=1),
            }:
                raise ValueError(
                    "Price provenance may reference only the immediately "
                    "previous, current, or immediately following source hour"
                )

        current_sources = [
            source for source in self.source_hours if source.hour_utc == target_hour
        ]

        if not current_sources:
            raise ValueError("Price provenance must include the current source hour")

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> PriceHourlyProvenance:
        payload = _json_object(
            "provenance_json",
            value,
        )

        expected_keys = {
            "schema",
            "schema_version",
            "trade_reader_schema_version",
            "trade_ohlc_schema_version",
            "source_hours",
        }

        if set(payload) != expected_keys:
            raise ValueError(
                "Price provenance fields do not match the version-1 contract"
            )

        if payload["schema"] != PRICE_PROVENANCE_SCHEMA:
            raise ValueError("Unsupported price provenance schema identity")

        if (
            isinstance(payload["schema_version"], bool)
            or payload["schema_version"] != PRICE_PROVENANCE_SCHEMA_VERSION
        ):
            raise ValueError("Unsupported price provenance schema version")

        reader_version = _nonnegative_int(
            "trade_reader_schema_version",
            payload["trade_reader_schema_version"],
        )
        ohlc_version = _nonnegative_int(
            "trade_ohlc_schema_version",
            payload["trade_ohlc_schema_version"],
        )

        if reader_version <= 0 or ohlc_version <= 0:
            raise ValueError("Price provenance component versions must be positive")

        raw_sources = payload["source_hours"]

        if not isinstance(raw_sources, list):
            raise ValueError("Price provenance source_hours must be a JSON array")

        result = cls(
            source_hours=tuple(
                PriceSourceHourReference.from_dict(source) for source in raw_sources
            ),
            trade_reader_schema_version=reader_version,
            trade_ohlc_schema_version=ohlc_version,
            schema=payload["schema"],
            schema_version=payload["schema_version"],
        )

        if result.to_dict() != payload:
            raise ValueError("Price provenance JSON is valid but not canonical")

        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "trade_reader_schema_version": (self.trade_reader_schema_version),
            "trade_ohlc_schema_version": (self.trade_ohlc_schema_version),
            "source_hours": [source.to_dict() for source in self.source_hours],
        }


def _verified_quality_summary(
    value: Mapping[str, Any],
    *,
    encoded: EncodedHourlyTradeOHLCBlocks,
) -> dict[str, Any]:
    payload = _json_object(
        "quality_summary_json",
        value,
    )

    expected_keys = {
        "schema",
        "schema_version",
        "observation_count",
        "valid_count",
        "invalid_count",
        "total_trade_count",
        "invalid_reason_counts",
    }

    if set(payload) != expected_keys:
        raise ValueError(
            "Trade OHLC quality summary fields do not match the "
            "format-version-1 contract"
        )

    if payload["schema"] != PRICE_QUALITY_SUMMARY_SCHEMA:
        raise ValueError("Unsupported trade OHLC quality-summary schema")

    if payload["schema_version"] != PRICE_QUALITY_SUMMARY_SCHEMA_VERSION:
        raise ValueError("Unsupported trade OHLC quality-summary version")

    observation_count = _nonnegative_int(
        "observation_count",
        payload["observation_count"],
    )
    valid_count = _nonnegative_int(
        "valid_count",
        payload["valid_count"],
    )
    invalid_count = _nonnegative_int(
        "invalid_count",
        payload["invalid_count"],
    )
    total_trade_count = _nonnegative_int(
        "total_trade_count",
        payload["total_trade_count"],
    )

    if observation_count != 3_600:
        raise ValueError("Trade OHLC quality summary must own 3,600 observations")

    if observation_count != encoded.observation_count:
        raise ValueError(
            "Trade OHLC quality summary and encoded block disagree "
            "about observation count"
        )

    if valid_count + invalid_count != observation_count:
        raise ValueError("Trade OHLC valid and invalid counts must total 3,600")

    reasons = payload["invalid_reason_counts"]

    if not isinstance(reasons, Mapping):
        raise ValueError("invalid_reason_counts must be a JSON object")

    normalized_reasons = dict(reasons)

    if normalized_reasons != {
        "no_trades": invalid_count,
    }:
        raise ValueError(
            "Trade OHLC invalid-reason counts do not match "
            "format-version-1 no-trade semantics"
        )

    decoded = decode_hourly_trade_ohlc_blocks(encoded)

    if decoded.valid_count != valid_count:
        raise ValueError("Decoded price valid count does not match quality summary")

    if decoded.invalid_count != invalid_count:
        raise ValueError("Decoded price invalid count does not match quality summary")

    if decoded.total_trade_count != total_trade_count:
        raise ValueError("Decoded total trade count does not match quality summary")

    return {
        "schema": PRICE_QUALITY_SUMMARY_SCHEMA,
        "schema_version": (PRICE_QUALITY_SUMMARY_SCHEMA_VERSION),
        "observation_count": observation_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "total_trade_count": total_trade_count,
        "invalid_reason_counts": normalized_reasons,
    }


@dataclass(frozen=True, slots=True)
class PersistedPriceHourlySeries:
    """Verified immutable view of one stored trade-price hour."""

    base: str
    hour_utc: datetime
    source_venue: str
    source_symbol: str

    schema_version: int
    sampling_interval_ms: int
    observation_count: int

    quality_summary_json: Mapping[str, Any]
    provenance_json: Mapping[str, Any]
    encoded: EncodedHourlyTradeOHLCBlocks
    created_at: datetime

    @classmethod
    def from_model(
        cls,
        model: PriceHourlySeries,
        *,
        verify_codec: bool = True,
    ) -> PersistedPriceHourlySeries:
        base = _normalized_base(model.base)
        expected_symbol = _SYMBOL_BY_BASE[base]

        venue = _identity_text(
            "source_venue",
            model.source_venue,
            lowercase=True,
        )
        symbol = _identity_text(
            "source_symbol",
            model.source_symbol,
            uppercase=True,
        )

        if venue != "binance_futures":
            raise AnalyticalRowCorruptionError(
                "Stored price row has an unsupported source venue"
            )

        if symbol != expected_symbol:
            raise AnalyticalRowCorruptionError(
                "Stored price row source symbol does not match its base"
            )

        if int(model.sampling_interval_ms) != 1_000:
            raise AnalyticalRowCorruptionError(
                "Stored price row has an unsupported sampling interval"
            )

        if int(model.observation_count) != 3_600:
            raise AnalyticalRowCorruptionError(
                "Stored price row has an unsupported observation count"
            )

        try:
            encoded = EncodedHourlyTradeOHLCBlocks(
                format_version=int(model.schema_version),
                codec=str(model.codec),
                observation_count=int(model.observation_count),
                ohlc_block=bytes(model.ohlc_block),
                validity_block=bytes(model.validity_block),
                trade_count_block=bytes(model.trade_count_block),
                content_sha256=str(model.content_sha256),
            )
        except Exception as exc:
            raise AnalyticalRowCorruptionError(
                "Stored price row contains an invalid encoded artifact"
            ) from exc

        try:
            stored_hour = require_utc_hour(
                "stored hour_utc",
                model.hour_utc,
            )
        except Exception as exc:
            raise AnalyticalRowCorruptionError(
                "Stored price row has an invalid UTC-hour identity"
            ) from exc

        if verify_codec:
            try:
                quality = _verified_quality_summary(
                    dict(model.quality_summary_json),
                    encoded=encoded,
                )
                provenance = PriceHourlyProvenance.from_dict(
                    dict(model.provenance_json)
                )
                provenance.validate_for_hour(
                    base=base,
                    hour_utc=stored_hour,
                )
                provenance_json = provenance.to_dict()
            except Exception as exc:
                raise AnalyticalRowCorruptionError(
                    "Stored price row failed codec, quality-summary, "
                    "or provenance verification"
                ) from exc
        else:
            quality = dict(model.quality_summary_json)
            provenance_json = dict(model.provenance_json)

        return cls(
            base=base,
            hour_utc=stored_hour,
            source_venue=venue,
            source_symbol=symbol,
            schema_version=int(model.schema_version),
            sampling_interval_ms=int(model.sampling_interval_ms),
            observation_count=int(model.observation_count),
            quality_summary_json=MappingProxyType(quality),
            provenance_json=MappingProxyType(provenance_json),
            encoded=encoded,
            created_at=model.created_at,
        )


@dataclass(frozen=True, slots=True)
class PriceHourlyWriteResult:
    """Result of an idempotent price-hour insertion."""

    series: PersistedPriceHourlySeries
    inserted: bool


def _price_identity_statement(
    *,
    base: str,
    hour_utc: datetime,
) -> Select[tuple[PriceHourlySeries]]:
    return select(PriceHourlySeries).where(
        PriceHourlySeries.base == base,
        PriceHourlySeries.hour_utc == hour_utc,
    )


def _assert_price_content_matches(
    model: PriceHourlySeries,
    *,
    base: str,
    hour_utc: datetime,
    source_symbol: str,
    encoded: EncodedHourlyTradeOHLCBlocks,
    quality_summary_json: dict[str, Any],
    provenance_json: dict[str, Any],
) -> None:
    existing = {
        "base": str(model.base),
        "hour_utc": require_utc_hour(
            "existing hour_utc",
            model.hour_utc,
        ),
        "source_venue": str(model.source_venue),
        "source_symbol": str(model.source_symbol),
        "schema_version": int(model.schema_version),
        "sampling_interval_ms": int(model.sampling_interval_ms),
        "observation_count": int(model.observation_count),
        "codec": str(model.codec),
        "ohlc_block": bytes(model.ohlc_block),
        "validity_block": bytes(model.validity_block),
        "trade_count_block": bytes(model.trade_count_block),
        "quality_summary_json": dict(model.quality_summary_json),
        "provenance_json": dict(model.provenance_json),
        "content_sha256": str(model.content_sha256),
    }

    requested = {
        "base": base,
        "hour_utc": hour_utc,
        "source_venue": "binance_futures",
        "source_symbol": source_symbol,
        "schema_version": encoded.format_version,
        "sampling_interval_ms": 1_000,
        "observation_count": encoded.observation_count,
        "codec": encoded.codec,
        "ohlc_block": encoded.ohlc_block,
        "validity_block": encoded.validity_block,
        "trade_count_block": encoded.trade_count_block,
        "quality_summary_json": quality_summary_json,
        "provenance_json": provenance_json,
        "content_sha256": encoded.content_sha256,
    }

    if existing != requested:
        raise PriceHourlySeriesConflictError(
            "Existing price-hour identity owns different content, "
            "quality metadata, or provenance"
        )


class PriceAnalyticalRepository:
    """Persistence operations for compact real-trade price hours."""

    def __init__(self, session: Session) -> None:
        if not isinstance(session, Session):
            raise TypeError("PriceAnalyticalRepository requires a SQLAlchemy Session")

        self._session = session

    def write_price_hour(
        self,
        *,
        base: str,
        hour_utc: datetime,
        encoded: EncodedHourlyTradeOHLCBlocks,
        quality_summary_json: Mapping[str, Any],
        provenance: PriceHourlyProvenance,
    ) -> PriceHourlyWriteResult:
        normalized_base = _normalized_base(base)
        normalized_hour = require_utc_hour(
            "hour_utc",
            hour_utc,
        )
        expected_symbol = _SYMBOL_BY_BASE[normalized_base]

        if not isinstance(
            encoded,
            EncodedHourlyTradeOHLCBlocks,
        ):
            raise TypeError("encoded must be EncodedHourlyTradeOHLCBlocks")

        if not isinstance(
            provenance,
            PriceHourlyProvenance,
        ):
            raise TypeError("provenance must be PriceHourlyProvenance")

        if encoded.format_version != PRICE_BLOCK_FORMAT_VERSION:
            raise ValueError("Encoded price block uses an unsupported format version")

        if encoded.codec != PRICE_BLOCK_CODEC:
            raise ValueError("Encoded price block uses an unsupported codec")

        if encoded.observation_count != 3_600:
            raise ValueError(
                "Encoded price block must contain exactly 3,600 observations"
            )

        quality_json = _verified_quality_summary(
            quality_summary_json,
            encoded=encoded,
        )

        provenance.validate_for_hour(
            base=normalized_base,
            hour_utc=normalized_hour,
        )
        provenance_json = provenance.to_dict()

        values = {
            "base": normalized_base,
            "hour_utc": normalized_hour,
            "source_venue": "binance_futures",
            "source_symbol": expected_symbol,
            "schema_version": encoded.format_version,
            "sampling_interval_ms": 1_000,
            "observation_count": encoded.observation_count,
            "codec": encoded.codec,
            "ohlc_block": encoded.ohlc_block,
            "validity_block": encoded.validity_block,
            "trade_count_block": encoded.trade_count_block,
            "quality_summary_json": quality_json,
            "provenance_json": provenance_json,
            "content_sha256": encoded.content_sha256,
        }

        statement = (
            postgresql_insert(PriceHourlySeries)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=[
                    PriceHourlySeries.base,
                    PriceHourlySeries.hour_utc,
                ]
            )
            .returning(PriceHourlySeries.content_sha256)
        )

        inserted_hash = self._session.execute(statement).scalar_one_or_none()

        model = self._session.scalar(
            _price_identity_statement(
                base=normalized_base,
                hour_utc=normalized_hour,
            )
        )

        if model is None:
            raise AnalyticalRepositoryError(
                "Price-hour insertion completed without a readable row"
            )

        _assert_price_content_matches(
            model,
            base=normalized_base,
            hour_utc=normalized_hour,
            source_symbol=expected_symbol,
            encoded=encoded,
            quality_summary_json=quality_json,
            provenance_json=provenance_json,
        )

        return PriceHourlyWriteResult(
            series=PersistedPriceHourlySeries.from_model(
                model,
                verify_codec=True,
            ),
            inserted=inserted_hash is not None,
        )

    def get_price_hour(
        self,
        *,
        base: str,
        hour_utc: datetime,
        verify_codec: bool = True,
    ) -> PersistedPriceHourlySeries | None:
        normalized_base = _normalized_base(base)
        normalized_hour = require_utc_hour(
            "hour_utc",
            hour_utc,
        )

        model = self._session.scalar(
            _price_identity_statement(
                base=normalized_base,
                hour_utc=normalized_hour,
            )
        )

        if model is None:
            return None

        return PersistedPriceHourlySeries.from_model(
            model,
            verify_codec=verify_codec,
        )

    def list_price_hours(
        self,
        *,
        base: str,
        start_utc: datetime,
        end_utc: datetime,
        verify_codec: bool = True,
    ) -> tuple[PersistedPriceHourlySeries, ...]:
        """Return price hours in half-open [start_utc, end_utc) order."""
        normalized_base = _normalized_base(base)
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

        models: Sequence[PriceHourlySeries] = (
            self._session.scalars(
                select(PriceHourlySeries)
                .where(
                    PriceHourlySeries.base == normalized_base,
                    PriceHourlySeries.hour_utc >= normalized_start,
                    PriceHourlySeries.hour_utc < normalized_end,
                )
                .order_by(PriceHourlySeries.hour_utc)
            )
            .unique()
            .all()
        )

        return tuple(
            PersistedPriceHourlySeries.from_model(
                model,
                verify_codec=verify_codec,
            )
            for model in models
        )


__all__ = [
    "PRICE_PROVENANCE_SCHEMA",
    "PRICE_PROVENANCE_SCHEMA_VERSION",
    "PersistedPriceHourlySeries",
    "PriceAnalyticalRepository",
    "PriceHourlyProvenance",
    "PriceHourlySeriesConflictError",
    "PriceHourlyWriteResult",
    "PriceSourceHourReference",
]

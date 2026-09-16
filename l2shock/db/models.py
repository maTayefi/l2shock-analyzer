# l2shock/db/models.py
"""Authoritative PostgreSQL ORM models.

Storage policy:

- raw CryptoHFTData order-book and trade rows remain in local Parquet files;
- PostgreSQL stores compact derived hourly blocks and operational metadata;
- all persisted timestamps are timezone-aware UTC;
- hourly analytical identities are aligned to exact UTC hour boundaries;
- Total Liquidity and Bid-Ask Imbalance are derived on demand.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from l2shock.timeutils import now_utc


class Base(DeclarativeBase):
    """Declarative model base."""


class AppSetting(Base):
    """Runtime-changeable application settings.

    Secrets must not be stored here merely for convenience. Database passwords,
    API keys, and temporary S3 credentials belong in environment-backed secret
    configuration.
    """

    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=now_utc,
        onupdate=now_utc,
        server_default=text("now()"),
    )

    __table_args__ = (
        CheckConstraint(
            "BTRIM(key) <> ''",
            name="ck_app_settings_key_nonblank",
        ),
        CheckConstraint(
            "jsonb_typeof(value_json) = 'object'",
            name="ck_app_settings_value_object",
        ),
    )


class DataPreset(Base):
    """One immutable data-affecting preset identity."""

    __tablename__ = "data_presets"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        primary_key=True,
    )
    preset_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        unique=True,
    )
    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default="1",
    )
    algorithm_version: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
    )
    base: Mapped[str] = mapped_column(
        String(8),
        nullable=False,
    )
    enabled: Mapped[bool] = mapped_column(
        nullable=False,
        default=True,
        server_default=text("true"),
    )
    config_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=now_utc,
        server_default=text("now()"),
    )

    __table_args__ = (
        CheckConstraint(
            "preset_hash ~ '^[0-9a-f]{64}$'",
            name="ck_data_presets_hash",
        ),
        CheckConstraint(
            "schema_version > 0",
            name="ck_data_presets_schema_version",
        ),
        CheckConstraint(
            "base IN ('BTC', 'ETH')",
            name="ck_data_presets_base",
        ),
        CheckConstraint(
            "BTRIM(algorithm_version) <> ''",
            name="ck_data_presets_algorithm_nonblank",
        ),
        CheckConstraint(
            "jsonb_typeof(config_json) = 'object'",
            name="ck_data_presets_config_object",
        ),
    )


class SourceHour(Base):
    """Identity and processing state for one local raw hourly source file."""

    __tablename__ = "source_hours"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        primary_key=True,
    )
    provider: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="cryptohftdata",
        server_default=text("'cryptohftdata'"),
    )
    venue: Mapped[str] = mapped_column(String(64), nullable=False)
    data_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    instrument: Mapped[str] = mapped_column(String(64), nullable=False)
    hour_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    remote_path: Mapped[str] = mapped_column(Text, nullable=False)
    local_path: Mapped[str | None] = mapped_column(Text, nullable=True)

    status: Mapped[str] = mapped_column(
        String(24),
        nullable=False,
        default="discovered",
        server_default=text("'discovered'"),
    )
    file_size_bytes: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    content_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )

    event_count: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    row_count: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    snapshot_count: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    continuity_mismatch_count: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )

    quality_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="INVALID",
        server_default=text("'INVALID'"),
    )
    quality_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=now_utc,
        server_default=text("now()"),
    )
    downloaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "provider",
            "venue",
            "data_kind",
            "instrument",
            "hour_utc",
            name="uq_source_hours_identity",
        ),
        CheckConstraint(
            "BTRIM(provider) <> '' "
            "AND BTRIM(venue) <> '' "
            "AND BTRIM(instrument) <> '' "
            "AND BTRIM(remote_path) <> ''",
            name="ck_source_hours_identity_nonblank",
        ),
        CheckConstraint(
            "data_kind IN ('orderbook', 'trades')",
            name="ck_source_hours_data_kind",
        ),
        CheckConstraint(
            "status IN ("
            "'discovered', 'downloading', 'downloaded', 'processing', "
            "'processed', 'missing', 'invalid', 'quarantined', 'error'"
            ")",
            name="ck_source_hours_status",
        ),
        CheckConstraint(
            "quality_state IN ('VALID', 'DEGRADED', 'INVALID')",
            name="ck_source_hours_quality_state",
        ),
        CheckConstraint(
            "hour_utc = date_trunc('hour', hour_utc)",
            name="ck_source_hours_hour_utc",
        ),
        CheckConstraint(
            "file_size_bytes IS NULL OR file_size_bytes >= 0",
            name="ck_source_hours_file_size",
        ),
        CheckConstraint(
            "content_sha256 IS NULL " "OR content_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_source_hours_sha256",
        ),
        CheckConstraint(
            "event_count IS NULL OR event_count >= 0",
            name="ck_source_hours_event_count",
        ),
        CheckConstraint(
            "row_count IS NULL OR row_count >= 0",
            name="ck_source_hours_row_count",
        ),
        CheckConstraint(
            "snapshot_count IS NULL OR snapshot_count >= 0",
            name="ck_source_hours_snapshot_count",
        ),
        CheckConstraint(
            "continuity_mismatch_count IS NULL " "OR continuity_mismatch_count >= 0",
            name="ck_source_hours_continuity_count",
        ),
        CheckConstraint(
            "jsonb_typeof(quality_json) = 'object'",
            name="ck_source_hours_quality_object",
        ),
        Index(
            "ix_source_hours_hour_kind",
            "hour_utc",
            "data_kind",
        ),
        Index(
            "ix_source_hours_status_hour",
            "status",
            "hour_utc",
        ),
    )


class L2HourlySeries(Base):
    """Compressed one-second Bid/Ask Liquidity observations for one UTC hour."""

    __tablename__ = "l2_hourly_series"

    base: Mapped[str] = mapped_column(String(8), primary_key=True)
    hour_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    preset_hash: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("data_presets.preset_hash"),
        primary_key=True,
    )

    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    sampling_interval_ms: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1000,
        server_default="1000",
    )
    observation_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3600,
        server_default="3600",
    )

    codec: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="arrow-ipc-zstd",
        server_default=text("'arrow-ipc-zstd'"),
    )
    bid_liquidity_block: Mapped[bytes] = mapped_column(
        LargeBinary,
        nullable=False,
    )
    ask_liquidity_block: Mapped[bytes] = mapped_column(
        LargeBinary,
        nullable=False,
    )
    validity_block: Mapped[bytes] = mapped_column(
        LargeBinary,
        nullable=False,
    )
    source_count_block: Mapped[bytes] = mapped_column(
        LargeBinary,
        nullable=False,
    )

    quality_summary_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    provenance_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    content_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=now_utc,
        server_default=text("now()"),
    )

    __table_args__ = (
        CheckConstraint(
            "base IN ('BTC', 'ETH')",
            name="ck_l2_hourly_series_base",
        ),
        CheckConstraint(
            "hour_utc = date_trunc('hour', hour_utc)",
            name="ck_l2_hourly_series_hour_utc",
        ),
        CheckConstraint(
            "schema_version > 0",
            name="ck_l2_hourly_series_schema_version",
        ),
        CheckConstraint(
            "sampling_interval_ms = 1000",
            name="ck_l2_hourly_series_sampling",
        ),
        CheckConstraint(
            "observation_count = 3600",
            name="ck_l2_hourly_series_observations",
        ),
        CheckConstraint(
            "BTRIM(codec) <> ''",
            name="ck_l2_hourly_series_codec",
        ),
        CheckConstraint(
            "octet_length(bid_liquidity_block) > 0 "
            "AND octet_length(ask_liquidity_block) > 0 "
            "AND octet_length(validity_block) > 0 "
            "AND octet_length(source_count_block) > 0",
            name="ck_l2_hourly_series_blocks_nonempty",
        ),
        CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_l2_hourly_series_sha256",
        ),
        CheckConstraint(
            "jsonb_typeof(quality_summary_json) = 'object'",
            name="ck_l2_hourly_series_quality_object",
        ),
        CheckConstraint(
            "jsonb_typeof(provenance_json) = 'object'",
            name="ck_l2_hourly_series_provenance_object",
        ),
        Index(
            "ix_l2_hourly_series_base_hour",
            "base",
            "hour_utc",
        ),
    )


class PriceHourlySeries(Base):
    """Compressed one-second real Binance perpetual trade OHLC observations."""

    __tablename__ = "price_hourly_series"

    base: Mapped[str] = mapped_column(String(8), primary_key=True)
    hour_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )

    source_venue: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="binance_futures",
        server_default=text("'binance_futures'"),
    )
    source_symbol: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
    )

    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    sampling_interval_ms: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1000,
        server_default="1000",
    )
    observation_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3600,
        server_default="3600",
    )
    codec: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="arrow-ipc-zstd",
        server_default=text("'arrow-ipc-zstd'"),
    )

    ohlc_block: Mapped[bytes] = mapped_column(
        LargeBinary,
        nullable=False,
    )
    validity_block: Mapped[bytes] = mapped_column(
        LargeBinary,
        nullable=False,
    )
    trade_count_block: Mapped[bytes] = mapped_column(
        LargeBinary,
        nullable=False,
    )

    quality_summary_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    provenance_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    content_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=now_utc,
        server_default=text("now()"),
    )

    __table_args__ = (
        CheckConstraint(
            "base IN ('BTC', 'ETH')",
            name="ck_price_hourly_series_base",
        ),
        CheckConstraint(
            "(base = 'BTC' AND source_symbol = 'BTCUSDT') "
            "OR (base = 'ETH' AND source_symbol = 'ETHUSDT')",
            name="ck_price_hourly_series_symbol",
        ),
        CheckConstraint(
            "source_venue = 'binance_futures'",
            name="ck_price_hourly_series_venue",
        ),
        CheckConstraint(
            "hour_utc = date_trunc('hour', hour_utc)",
            name="ck_price_hourly_series_hour_utc",
        ),
        CheckConstraint(
            "schema_version > 0",
            name="ck_price_hourly_series_schema_version",
        ),
        CheckConstraint(
            "sampling_interval_ms = 1000",
            name="ck_price_hourly_series_sampling",
        ),
        CheckConstraint(
            "observation_count = 3600",
            name="ck_price_hourly_series_observations",
        ),
        CheckConstraint(
            "octet_length(ohlc_block) > 0 "
            "AND octet_length(validity_block) > 0 "
            "AND octet_length(trade_count_block) > 0",
            name="ck_price_hourly_series_blocks_nonempty",
        ),
        CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_price_hourly_series_sha256",
        ),
        CheckConstraint(
            "jsonb_typeof(quality_summary_json) = 'object'",
            name="ck_price_hourly_series_quality_object",
        ),
        CheckConstraint(
            "jsonb_typeof(provenance_json) = 'object'",
            name="ck_price_hourly_series_provenance_object",
        ),
        Index(
            "ix_price_hourly_series_base_hour",
            "base",
            "hour_utc",
        ),
    )


class FetchRun(Base):
    """Operational history for manual and automatic acquisition runs."""

    __tablename__ = "fetch_runs"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        primary_key=True,
    )
    operation_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        unique=True,
    )
    kind: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="running",
        server_default=text("'running'"),
    )

    requested_start_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    requested_end_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=now_utc,
        server_default=text("now()"),
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    files_requested: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    files_downloaded: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    files_processed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    files_failed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    details_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "BTRIM(operation_id) <> ''",
            name="ck_fetch_runs_operation_id",
        ),
        CheckConstraint(
            "kind IN ('manual', 'automatic', 'backfill', 'validation')",
            name="ck_fetch_runs_kind",
        ),
        CheckConstraint(
            "status IN ('running', 'ok', 'partial_ok', 'error', 'stopped')",
            name="ck_fetch_runs_status",
        ),
        CheckConstraint(
            "requested_end_utc >= requested_start_utc",
            name="ck_fetch_runs_requested_range",
        ),
        CheckConstraint(
            "ended_at IS NULL OR ended_at >= started_at",
            name="ck_fetch_runs_chronology",
        ),
        CheckConstraint(
            "files_requested >= 0 "
            "AND files_downloaded >= 0 "
            "AND files_processed >= 0 "
            "AND files_failed >= 0",
            name="ck_fetch_runs_counters",
        ),
        CheckConstraint(
            "jsonb_typeof(details_json) = 'object'",
            name="ck_fetch_runs_details_object",
        ),
        Index(
            "ix_fetch_runs_started_at",
            "started_at",
        ),
    )


__all__ = [
    "AppSetting",
    "Base",
    "DataPreset",
    "FetchRun",
    "L2HourlySeries",
    "PriceHourlySeries",
    "SourceHour",
]

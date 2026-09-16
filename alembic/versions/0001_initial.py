"""Initial compact analytical storage schema.

Revision ID: 0001_initial
Revises:

This revision is intentionally self-contained. It must never import live ORM
metadata because later ORM changes must not alter the schema produced by a
historical migration.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable, DropTable

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

_frozen_metadata = sa.MetaData()


app_settings = sa.Table(
    "app_settings",
    _frozen_metadata,
    sa.Column(
        "key",
        sa.String(length=128),
        nullable=False,
    ),
    sa.Column(
        "value_json",
        postgresql.JSONB(astext_type=sa.Text()),
        server_default=sa.text("'{}'::jsonb"),
        nullable=False,
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    ),
    sa.CheckConstraint(
        "BTRIM(key) <> ''",
        name="ck_app_settings_key_nonblank",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(value_json) = 'object'",
        name="ck_app_settings_value_object",
    ),
    sa.PrimaryKeyConstraint("key"),
)


data_presets = sa.Table(
    "data_presets",
    _frozen_metadata,
    sa.Column(
        "id",
        sa.BigInteger(),
        sa.Identity(),
        nullable=False,
    ),
    sa.Column(
        "preset_hash",
        sa.String(length=64),
        nullable=False,
    ),
    sa.Column(
        "schema_version",
        sa.Integer(),
        server_default="1",
        nullable=False,
    ),
    sa.Column(
        "algorithm_version",
        sa.String(length=32),
        nullable=False,
    ),
    sa.Column(
        "base",
        sa.String(length=8),
        nullable=False,
    ),
    sa.Column(
        "enabled",
        sa.Boolean(),
        server_default=sa.text("true"),
        nullable=False,
    ),
    sa.Column(
        "config_json",
        postgresql.JSONB(astext_type=sa.Text()),
        nullable=False,
    ),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    ),
    sa.CheckConstraint(
        "preset_hash ~ '^[0-9a-f]{64}$'",
        name="ck_data_presets_hash",
    ),
    sa.CheckConstraint(
        "schema_version > 0",
        name="ck_data_presets_schema_version",
    ),
    sa.CheckConstraint(
        "base IN ('BTC', 'ETH')",
        name="ck_data_presets_base",
    ),
    sa.CheckConstraint(
        "BTRIM(algorithm_version) <> ''",
        name="ck_data_presets_algorithm_nonblank",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(config_json) = 'object'",
        name="ck_data_presets_config_object",
    ),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("preset_hash"),
)


source_hours = sa.Table(
    "source_hours",
    _frozen_metadata,
    sa.Column(
        "id",
        sa.BigInteger(),
        sa.Identity(),
        nullable=False,
    ),
    sa.Column(
        "provider",
        sa.String(length=32),
        server_default=sa.text("'cryptohftdata'"),
        nullable=False,
    ),
    sa.Column(
        "venue",
        sa.String(length=64),
        nullable=False,
    ),
    sa.Column(
        "data_kind",
        sa.String(length=16),
        nullable=False,
    ),
    sa.Column(
        "instrument",
        sa.String(length=64),
        nullable=False,
    ),
    sa.Column(
        "hour_utc",
        sa.DateTime(timezone=True),
        nullable=False,
    ),
    sa.Column(
        "remote_path",
        sa.Text(),
        nullable=False,
    ),
    sa.Column(
        "local_path",
        sa.Text(),
        nullable=True,
    ),
    sa.Column(
        "status",
        sa.String(length=24),
        server_default=sa.text("'discovered'"),
        nullable=False,
    ),
    sa.Column(
        "file_size_bytes",
        sa.BigInteger(),
        nullable=True,
    ),
    sa.Column(
        "content_sha256",
        sa.String(length=64),
        nullable=True,
    ),
    sa.Column(
        "event_count",
        sa.BigInteger(),
        nullable=True,
    ),
    sa.Column(
        "row_count",
        sa.BigInteger(),
        nullable=True,
    ),
    sa.Column(
        "snapshot_count",
        sa.BigInteger(),
        nullable=True,
    ),
    sa.Column(
        "continuity_mismatch_count",
        sa.BigInteger(),
        nullable=True,
    ),
    sa.Column(
        "quality_state",
        sa.String(length=16),
        server_default=sa.text("'INVALID'"),
        nullable=False,
    ),
    sa.Column(
        "quality_json",
        postgresql.JSONB(astext_type=sa.Text()),
        server_default=sa.text("'{}'::jsonb"),
        nullable=False,
    ),
    sa.Column(
        "discovered_at",
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    ),
    sa.Column(
        "downloaded_at",
        sa.DateTime(timezone=True),
        nullable=True,
    ),
    sa.Column(
        "processed_at",
        sa.DateTime(timezone=True),
        nullable=True,
    ),
    sa.Column(
        "error_text",
        sa.Text(),
        nullable=True,
    ),
    sa.CheckConstraint(
        "BTRIM(provider) <> '' "
        "AND BTRIM(venue) <> '' "
        "AND BTRIM(instrument) <> '' "
        "AND BTRIM(remote_path) <> ''",
        name="ck_source_hours_identity_nonblank",
    ),
    sa.CheckConstraint(
        "data_kind IN ('orderbook', 'trades')",
        name="ck_source_hours_data_kind",
    ),
    sa.CheckConstraint(
        "status IN ("
        "'discovered', 'downloading', 'downloaded', 'processing', "
        "'processed', 'missing', 'invalid', 'quarantined', 'error'"
        ")",
        name="ck_source_hours_status",
    ),
    sa.CheckConstraint(
        "quality_state IN ('VALID', 'DEGRADED', 'INVALID')",
        name="ck_source_hours_quality_state",
    ),
    sa.CheckConstraint(
        "hour_utc = date_trunc('hour', hour_utc)",
        name="ck_source_hours_hour_utc",
    ),
    sa.CheckConstraint(
        "file_size_bytes IS NULL OR file_size_bytes >= 0",
        name="ck_source_hours_file_size",
    ),
    sa.CheckConstraint(
        "content_sha256 IS NULL " "OR content_sha256 ~ '^[0-9a-f]{64}$'",
        name="ck_source_hours_sha256",
    ),
    sa.CheckConstraint(
        "event_count IS NULL OR event_count >= 0",
        name="ck_source_hours_event_count",
    ),
    sa.CheckConstraint(
        "row_count IS NULL OR row_count >= 0",
        name="ck_source_hours_row_count",
    ),
    sa.CheckConstraint(
        "snapshot_count IS NULL OR snapshot_count >= 0",
        name="ck_source_hours_snapshot_count",
    ),
    sa.CheckConstraint(
        "continuity_mismatch_count IS NULL " "OR continuity_mismatch_count >= 0",
        name="ck_source_hours_continuity_count",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(quality_json) = 'object'",
        name="ck_source_hours_quality_object",
    ),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint(
        "provider",
        "venue",
        "data_kind",
        "instrument",
        "hour_utc",
        name="uq_source_hours_identity",
    ),
)

sa.Index(
    "ix_source_hours_hour_kind",
    source_hours.c.hour_utc,
    source_hours.c.data_kind,
)
sa.Index(
    "ix_source_hours_status_hour",
    source_hours.c.status,
    source_hours.c.hour_utc,
)


l2_hourly_series = sa.Table(
    "l2_hourly_series",
    _frozen_metadata,
    sa.Column(
        "base",
        sa.String(length=8),
        nullable=False,
    ),
    sa.Column(
        "hour_utc",
        sa.DateTime(timezone=True),
        nullable=False,
    ),
    sa.Column(
        "preset_hash",
        sa.String(length=64),
        nullable=False,
    ),
    sa.Column(
        "schema_version",
        sa.Integer(),
        nullable=False,
    ),
    sa.Column(
        "sampling_interval_ms",
        sa.Integer(),
        server_default="1000",
        nullable=False,
    ),
    sa.Column(
        "observation_count",
        sa.Integer(),
        server_default="3600",
        nullable=False,
    ),
    sa.Column(
        "codec",
        sa.String(length=32),
        server_default=sa.text("'arrow-ipc-zstd'"),
        nullable=False,
    ),
    sa.Column(
        "bid_liquidity_block",
        sa.LargeBinary(),
        nullable=False,
    ),
    sa.Column(
        "ask_liquidity_block",
        sa.LargeBinary(),
        nullable=False,
    ),
    sa.Column(
        "validity_block",
        sa.LargeBinary(),
        nullable=False,
    ),
    sa.Column(
        "source_count_block",
        sa.LargeBinary(),
        nullable=False,
    ),
    sa.Column(
        "quality_summary_json",
        postgresql.JSONB(astext_type=sa.Text()),
        server_default=sa.text("'{}'::jsonb"),
        nullable=False,
    ),
    sa.Column(
        "provenance_json",
        postgresql.JSONB(astext_type=sa.Text()),
        nullable=False,
    ),
    sa.Column(
        "content_sha256",
        sa.String(length=64),
        nullable=False,
    ),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    ),
    sa.CheckConstraint(
        "base IN ('BTC', 'ETH')",
        name="ck_l2_hourly_series_base",
    ),
    sa.CheckConstraint(
        "hour_utc = date_trunc('hour', hour_utc)",
        name="ck_l2_hourly_series_hour_utc",
    ),
    sa.CheckConstraint(
        "schema_version > 0",
        name="ck_l2_hourly_series_schema_version",
    ),
    sa.CheckConstraint(
        "sampling_interval_ms = 1000",
        name="ck_l2_hourly_series_sampling",
    ),
    sa.CheckConstraint(
        "observation_count = 3600",
        name="ck_l2_hourly_series_observations",
    ),
    sa.CheckConstraint(
        "BTRIM(codec) <> ''",
        name="ck_l2_hourly_series_codec",
    ),
    sa.CheckConstraint(
        "octet_length(bid_liquidity_block) > 0 "
        "AND octet_length(ask_liquidity_block) > 0 "
        "AND octet_length(validity_block) > 0 "
        "AND octet_length(source_count_block) > 0",
        name="ck_l2_hourly_series_blocks_nonempty",
    ),
    sa.CheckConstraint(
        "content_sha256 ~ '^[0-9a-f]{64}$'",
        name="ck_l2_hourly_series_sha256",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(quality_summary_json) = 'object'",
        name="ck_l2_hourly_series_quality_object",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(provenance_json) = 'object'",
        name="ck_l2_hourly_series_provenance_object",
    ),
    sa.ForeignKeyConstraint(
        ["preset_hash"],
        ["data_presets.preset_hash"],
    ),
    sa.PrimaryKeyConstraint(
        "base",
        "hour_utc",
        "preset_hash",
    ),
)

sa.Index(
    "ix_l2_hourly_series_base_hour",
    l2_hourly_series.c.base,
    l2_hourly_series.c.hour_utc,
)


price_hourly_series = sa.Table(
    "price_hourly_series",
    _frozen_metadata,
    sa.Column(
        "base",
        sa.String(length=8),
        nullable=False,
    ),
    sa.Column(
        "hour_utc",
        sa.DateTime(timezone=True),
        nullable=False,
    ),
    sa.Column(
        "source_venue",
        sa.String(length=32),
        server_default=sa.text("'binance_futures'"),
        nullable=False,
    ),
    sa.Column(
        "source_symbol",
        sa.String(length=32),
        nullable=False,
    ),
    sa.Column(
        "schema_version",
        sa.Integer(),
        nullable=False,
    ),
    sa.Column(
        "sampling_interval_ms",
        sa.Integer(),
        server_default="1000",
        nullable=False,
    ),
    sa.Column(
        "observation_count",
        sa.Integer(),
        server_default="3600",
        nullable=False,
    ),
    sa.Column(
        "codec",
        sa.String(length=32),
        server_default=sa.text("'arrow-ipc-zstd'"),
        nullable=False,
    ),
    sa.Column(
        "ohlc_block",
        sa.LargeBinary(),
        nullable=False,
    ),
    sa.Column(
        "validity_block",
        sa.LargeBinary(),
        nullable=False,
    ),
    sa.Column(
        "trade_count_block",
        sa.LargeBinary(),
        nullable=False,
    ),
    sa.Column(
        "quality_summary_json",
        postgresql.JSONB(astext_type=sa.Text()),
        server_default=sa.text("'{}'::jsonb"),
        nullable=False,
    ),
    sa.Column(
        "provenance_json",
        postgresql.JSONB(astext_type=sa.Text()),
        nullable=False,
    ),
    sa.Column(
        "content_sha256",
        sa.String(length=64),
        nullable=False,
    ),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    ),
    sa.CheckConstraint(
        "base IN ('BTC', 'ETH')",
        name="ck_price_hourly_series_base",
    ),
    sa.CheckConstraint(
        "(base = 'BTC' AND source_symbol = 'BTCUSDT') "
        "OR (base = 'ETH' AND source_symbol = 'ETHUSDT')",
        name="ck_price_hourly_series_symbol",
    ),
    sa.CheckConstraint(
        "source_venue = 'binance_futures'",
        name="ck_price_hourly_series_venue",
    ),
    sa.CheckConstraint(
        "hour_utc = date_trunc('hour', hour_utc)",
        name="ck_price_hourly_series_hour_utc",
    ),
    sa.CheckConstraint(
        "schema_version > 0",
        name="ck_price_hourly_series_schema_version",
    ),
    sa.CheckConstraint(
        "sampling_interval_ms = 1000",
        name="ck_price_hourly_series_sampling",
    ),
    sa.CheckConstraint(
        "observation_count = 3600",
        name="ck_price_hourly_series_observations",
    ),
    sa.CheckConstraint(
        "octet_length(ohlc_block) > 0 "
        "AND octet_length(validity_block) > 0 "
        "AND octet_length(trade_count_block) > 0",
        name="ck_price_hourly_series_blocks_nonempty",
    ),
    sa.CheckConstraint(
        "content_sha256 ~ '^[0-9a-f]{64}$'",
        name="ck_price_hourly_series_sha256",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(quality_summary_json) = 'object'",
        name="ck_price_hourly_series_quality_object",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(provenance_json) = 'object'",
        name="ck_price_hourly_series_provenance_object",
    ),
    sa.PrimaryKeyConstraint(
        "base",
        "hour_utc",
    ),
)

sa.Index(
    "ix_price_hourly_series_base_hour",
    price_hourly_series.c.base,
    price_hourly_series.c.hour_utc,
)


fetch_runs = sa.Table(
    "fetch_runs",
    _frozen_metadata,
    sa.Column(
        "id",
        sa.BigInteger(),
        sa.Identity(),
        nullable=False,
    ),
    sa.Column(
        "operation_id",
        sa.String(length=36),
        nullable=False,
    ),
    sa.Column(
        "kind",
        sa.String(length=16),
        nullable=False,
    ),
    sa.Column(
        "status",
        sa.String(length=16),
        server_default=sa.text("'running'"),
        nullable=False,
    ),
    sa.Column(
        "requested_start_utc",
        sa.DateTime(timezone=True),
        nullable=False,
    ),
    sa.Column(
        "requested_end_utc",
        sa.DateTime(timezone=True),
        nullable=False,
    ),
    sa.Column(
        "started_at",
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    ),
    sa.Column(
        "ended_at",
        sa.DateTime(timezone=True),
        nullable=True,
    ),
    sa.Column(
        "files_requested",
        sa.Integer(),
        server_default="0",
        nullable=False,
    ),
    sa.Column(
        "files_downloaded",
        sa.Integer(),
        server_default="0",
        nullable=False,
    ),
    sa.Column(
        "files_processed",
        sa.Integer(),
        server_default="0",
        nullable=False,
    ),
    sa.Column(
        "files_failed",
        sa.Integer(),
        server_default="0",
        nullable=False,
    ),
    sa.Column(
        "details_json",
        postgresql.JSONB(astext_type=sa.Text()),
        server_default=sa.text("'{}'::jsonb"),
        nullable=False,
    ),
    sa.Column(
        "error_text",
        sa.Text(),
        nullable=True,
    ),
    sa.CheckConstraint(
        "BTRIM(operation_id) <> ''",
        name="ck_fetch_runs_operation_id",
    ),
    sa.CheckConstraint(
        "kind IN ('manual', 'automatic', 'backfill', 'validation')",
        name="ck_fetch_runs_kind",
    ),
    sa.CheckConstraint(
        "status IN ('running', 'ok', 'partial_ok', 'error', 'stopped')",
        name="ck_fetch_runs_status",
    ),
    sa.CheckConstraint(
        "requested_end_utc >= requested_start_utc",
        name="ck_fetch_runs_requested_range",
    ),
    sa.CheckConstraint(
        "ended_at IS NULL OR ended_at >= started_at",
        name="ck_fetch_runs_chronology",
    ),
    sa.CheckConstraint(
        "files_requested >= 0 "
        "AND files_downloaded >= 0 "
        "AND files_processed >= 0 "
        "AND files_failed >= 0",
        name="ck_fetch_runs_counters",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(details_json) = 'object'",
        name="ck_fetch_runs_details_object",
    ),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("operation_id"),
)

sa.Index(
    "ix_fetch_runs_started_at",
    fetch_runs.c.started_at,
)


def upgrade() -> None:
    """Create the initial schema from frozen revision-local metadata."""
    bind = op.get_bind()

    for table in _frozen_metadata.sorted_tables:
        bind.execute(CreateTable(table, if_not_exists=False))

    for table in _frozen_metadata.sorted_tables:
        for index in sorted(table.indexes, key=lambda item: item.name or ""):
            bind.execute(CreateIndex(index, if_not_exists=False))


def downgrade() -> None:
    """Drop all initial application tables in dependency-safe order."""
    bind = op.get_bind()

    for table in reversed(_frozen_metadata.sorted_tables):
        bind.execute(DropTable(table, if_exists=True))

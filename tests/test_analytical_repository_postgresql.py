from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.db import (
    AnalyticalRepository,
    AnalyticalRowNotFoundError,
    HourlySeriesConflictError,
    L2HourlyProvenance,
    PresetIdentityConflictError,
    SourceHourReference,
    get_engine,
)
from l2shock.db import PresetDeletionBlockedError
from l2shock.db.models import DataPreset
from l2shock.db.schema import verify_schema
from l2shock.ingest.replay import (
    BookInitializationState,
    ReplayBookStructure,
)
from l2shock.ingest.sampling import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.liquidity import (
    DepthBand,
    HourlyLiquidityBlock,
    LiquidityObservation,
    build_liquidity_quality_summary,
    encode_hourly_liquidity_block,
    liquidity_quality_summary_to_dict,
)
from l2shock.presets import (
    LiquidityDataPreset,
    build_binance_futures_data_preset,
)

pytestmark = pytest.mark.postgresql


@pytest.fixture
def database_session() -> Session:
    engine = get_engine()
    verify_schema(engine)

    connection = engine.connect()
    transaction = connection.begin()

    session = Session(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2081,
        1,
        1,
        offset,
        tzinfo=timezone.utc,
    )


def _preset(
    *,
    upper: str = "0.01",
) -> LiquidityDataPreset:
    return build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal(upper),
    )


def _spec(
    *,
    hour: datetime | None = None,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour or _hour(),
    )


def _valid_observation(
    *,
    spec: SourceFileSpec,
    bucket_index: int,
    bid: str = "1000",
    ask: str = "1200",
) -> LiquidityObservation:
    from datetime import timedelta

    if bucket_index >= 60:
        bucket_start = spec.hour_utc + timedelta(seconds=bucket_index)
    else:
        bucket_start = spec.hour_utc.replace(
            second=bucket_index,
        )
    bucket_end = bucket_start + timedelta(seconds=1)

    bid_value = Decimal(bid)
    ask_value = Decimal(ask)
    total = bid_value + ask_value

    return LiquidityObservation(
        bucket_index=bucket_index,
        bucket_start_utc=bucket_start,
        bucket_end_utc=bucket_end,
        source_received_time_ns=(int(bucket_end.timestamp() * 1_000_000_000) - 1),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        replay_valid=True,
        initialization_state=(BookInitializationState.SNAPSHOT),
        book_structure=ReplayBookStructure.NORMAL,
        last_update_id=100 + bucket_index,
        best_bid=Decimal("70000"),
        best_ask=Decimal("70001"),
        bid_liquidity=bid_value,
        ask_liquidity=ask_value,
        total_liquidity=total,
        bid_ask_imbalance=((bid_value - ask_value) / total),
        bid_depth_level_count=2,
        ask_depth_level_count=2,
        levels_examined=4,
        source_count=1,
    )


def _invalid_observation(
    *,
    spec: SourceFileSpec,
    bucket_index: int,
) -> LiquidityObservation:
    from datetime import timedelta

    bucket_start = spec.hour_utc + timedelta(seconds=bucket_index)
    bucket_end = bucket_start + timedelta(seconds=1)

    return LiquidityObservation(
        bucket_index=bucket_index,
        bucket_start_utc=bucket_start,
        bucket_end_utc=bucket_end,
        source_received_time_ns=None,
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
        replay_valid=False,
        initialization_state=(BookInitializationState.UNINITIALIZED),
        book_structure=ReplayBookStructure.EMPTY_BOTH,
        last_update_id=None,
        best_bid=None,
        best_ask=None,
        bid_liquidity=None,
        ask_liquidity=None,
        total_liquidity=None,
        bid_ask_imbalance=None,
        bid_depth_level_count=0,
        ask_depth_level_count=0,
        levels_examined=0,
        source_count=0,
    )


def _block(
    *,
    hour: datetime | None = None,
    first_bid: str = "1000",
) -> HourlyLiquidityBlock:
    spec = _spec(hour=hour)

    observations = [
        _valid_observation(
            spec=spec,
            bucket_index=index,
        )
        for index in range(3_600)
    ]

    observations[0] = _valid_observation(
        spec=spec,
        bucket_index=0,
        bid=first_bid,
    )
    observations[10] = _invalid_observation(
        spec=spec,
        bucket_index=10,
    )

    observation_tuple = tuple(observations)

    return HourlyLiquidityBlock(
        spec=spec,
        band=DepthBand(
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0.01"),
        ),
        imbalance_decimal_precision=18,
        observations=observation_tuple,
        quality_summary=build_liquidity_quality_summary(observation_tuple),
    )


def _provenance(
    *,
    hour: datetime | None = None,
    source_hash: str = "a" * 64,
) -> L2HourlyProvenance:
    return L2HourlyProvenance(
        source_hours=(
            SourceHourReference(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument="BTCUSDT",
                hour_utc=hour or _hour(),
                content_sha256=source_hash,
            ),
        ),
        checkpoint_content_sha256="b" * 64,
        replay_schema_version=1,
        liquidity_schema_version=1,
    )


def _encoded_inputs(
    *,
    hour: datetime | None = None,
    first_bid: str = "1000",
):
    block = _block(
        hour=hour,
        first_bid=first_bid,
    )
    encoded = encode_hourly_liquidity_block(block)
    summary = liquidity_quality_summary_to_dict(block.quality_summary)

    return block, encoded, summary


def test_preset_insert_is_idempotent(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset(upper="0.041")

    first = repository.ensure_preset(
        preset,
        enabled=True,
    )
    second = repository.ensure_preset(
        preset,
        enabled=True,
    )

    assert first.inserted is True
    assert second.inserted is False
    assert first.preset.id == second.preset.id
    assert first.preset.preset_hash == preset.preset_hash
    assert first.preset.enabled is True
    assert dict(first.preset.config_json) == (preset.to_canonical_dict())


def test_preset_listing_filters_base_and_enabled_state(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)

    btc_enabled = _preset(
        upper="0.071",
    )
    btc_disabled = _preset(
        upper="0.072",
    )

    eth_enabled = build_binance_futures_data_preset(
        base="ETH",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.073"),
    )

    repository.ensure_preset(
        btc_enabled,
        enabled=True,
    )
    repository.set_preset_enabled(
        btc_enabled.preset_hash,
        enabled=True,
    )

    repository.ensure_preset(
        btc_disabled,
        enabled=False,
    )
    repository.set_preset_enabled(
        btc_disabled.preset_hash,
        enabled=False,
    )

    repository.ensure_preset(
        eth_enabled,
        enabled=True,
    )
    repository.set_preset_enabled(
        eth_enabled.preset_hash,
        enabled=True,
    )

    btc_rows = repository.list_presets(
        base="BTC",
        enabled_only=True,
    )
    all_enabled = repository.list_presets(
        enabled_only=True,
    )

    btc_hashes = {item.preset_hash for item in btc_rows}
    all_enabled_hashes = {item.preset_hash for item in all_enabled}

    assert all(item.base == "BTC" for item in btc_rows)
    assert all(item.enabled is True for item in btc_rows)
    assert btc_enabled.preset_hash in btc_hashes
    assert btc_disabled.preset_hash not in btc_hashes
    assert eth_enabled.preset_hash not in btc_hashes

    assert all(item.enabled is True for item in all_enabled)
    assert btc_enabled.preset_hash in all_enabled_hashes
    assert eth_enabled.preset_hash in all_enabled_hashes
    assert btc_disabled.preset_hash not in all_enabled_hashes


def test_operational_enabled_state_does_not_change_hash(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()

    created = repository.ensure_preset(
        preset,
        enabled=True,
    )
    disabled = repository.set_preset_enabled(
        preset.preset_hash,
        enabled=False,
    )
    enabled = repository.set_preset_enabled(
        preset.preset_hash,
        enabled=True,
    )

    assert created.preset.preset_hash == preset.preset_hash
    assert disabled.preset_hash == preset.preset_hash
    assert enabled.preset_hash == preset.preset_hash

    assert disabled.enabled is False
    assert enabled.enabled is True


def test_ensure_preset_does_not_overwrite_enabled_state(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset(upper="0.042")

    repository.ensure_preset(
        preset,
        enabled=False,
    )

    repeated = repository.ensure_preset(
        preset,
        enabled=True,
    )

    assert repeated.inserted is False
    assert repeated.preset.enabled is False


def test_preset_hash_conflicting_content_is_rejected(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()

    repository.ensure_preset(preset)

    model = (
        database_session.query(DataPreset)
        .filter_by(preset_hash=preset.preset_hash)
        .one()
    )

    model.algorithm_version = "conflicting-v1"
    database_session.flush()

    with pytest.raises(
        PresetIdentityConflictError,
        match="different immutable",
    ):
        repository.ensure_preset(preset)


def test_missing_preset_enable_update_is_rejected(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)

    with pytest.raises(
        AnalyticalRowNotFoundError,
        match="does not exist",
    ):
        repository.set_preset_enabled(
            "f" * 64,
            enabled=False,
        )


def test_hourly_write_requires_persisted_preset(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset(upper="0.043")
    _block_value, encoded, summary = _encoded_inputs()

    with pytest.raises(
        AnalyticalRowNotFoundError,
        match="must be persisted",
    ):
        repository.write_l2_hour(
            preset=preset,
            hour_utc=_hour(),
            encoded=encoded,
            quality_summary_json=summary,
            provenance=_provenance(),
        )


def test_hourly_write_and_read_round_trip(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    block, encoded, summary = _encoded_inputs()

    result = repository.write_l2_hour(
        preset=preset,
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=_provenance(),
    )

    assert result.inserted is True

    stored = repository.get_l2_hour(
        base="BTC",
        hour_utc=_hour(),
        preset_hash=preset.preset_hash,
    )

    assert stored is not None
    assert stored.base == "BTC"
    assert stored.hour_utc == _hour()
    assert stored.preset_hash == preset.preset_hash
    assert stored.sampling_interval_ms == 1_000
    assert stored.observation_count == 3_600

    assert stored.encoded == encoded
    assert dict(stored.quality_summary_json) == summary

    provenance = dict(stored.provenance_json)

    assert provenance["schema"] == ("l2shock.l2_hourly_series_provenance")
    assert provenance["source_hours"][0]["instrument"] == ("BTCUSDT")

    assert block.spec.hour_utc == stored.hour_utc


def test_identical_hourly_write_is_idempotent(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    _block_value, encoded, summary = _encoded_inputs()
    provenance = _provenance()

    first = repository.write_l2_hour(
        preset=preset,
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=provenance,
    )
    second = repository.write_l2_hour(
        preset=preset,
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=provenance,
    )

    assert first.inserted is True
    assert second.inserted is False
    assert first.series.encoded == second.series.encoded


def test_conflicting_hourly_content_is_rejected(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    _first_block, first_encoded, first_summary = _encoded_inputs(first_bid="1000")
    _second_block, second_encoded, second_summary = _encoded_inputs(first_bid="1001")

    repository.write_l2_hour(
        preset=preset,
        hour_utc=_hour(),
        encoded=first_encoded,
        quality_summary_json=first_summary,
        provenance=_provenance(),
    )

    with pytest.raises(
        HourlySeriesConflictError,
        match="different content",
    ):
        repository.write_l2_hour(
            preset=preset,
            hour_utc=_hour(),
            encoded=second_encoded,
            quality_summary_json=second_summary,
            provenance=_provenance(),
        )


def test_conflicting_provenance_is_rejected(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    _block_value, encoded, summary = _encoded_inputs()

    repository.write_l2_hour(
        preset=preset,
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=_provenance(
            source_hash="a" * 64,
        ),
    )

    with pytest.raises(
        HourlySeriesConflictError,
        match="provenance",
    ):
        repository.write_l2_hour(
            preset=preset,
            hour_utc=_hour(),
            encoded=encoded,
            quality_summary_json=summary,
            provenance=_provenance(
                source_hash="c" * 64,
            ),
        )


def test_provenance_requires_current_analytical_hour(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    _block_value, encoded, summary = _encoded_inputs()

    with pytest.raises(
        ValueError,
        match="persisted analytical hour",
    ):
        repository.write_l2_hour(
            preset=preset,
            hour_utc=_hour(1),
            encoded=encoded,
            quality_summary_json=summary,
            provenance=_provenance(
                hour=_hour(0),
            ),
        )


def test_range_read_is_half_open_and_ordered(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    for hour_offset in (0, 1, 2):
        current_hour = _hour(hour_offset)
        _block_value, encoded, summary = _encoded_inputs(
            hour=current_hour,
        )

        repository.write_l2_hour(
            preset=preset,
            hour_utc=current_hour,
            encoded=encoded,
            quality_summary_json=summary,
            provenance=_provenance(
                hour=current_hour,
            ),
        )

    observed = repository.list_l2_hours(
        base="BTC",
        preset_hash=preset.preset_hash,
        start_utc=_hour(0),
        end_utc=_hour(2),
    )

    assert [item.hour_utc for item in observed] == [
        _hour(0),
        _hour(1),
    ]


def test_missing_hour_returns_none(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    assert (
        repository.get_l2_hour(
            base="BTC",
            hour_utc=_hour(),
            preset_hash=preset.preset_hash,
        )
        is None
    )


def test_uppercase_sha256_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="canonical lowercase",
    ):
        SourceHourReference(
            provider="cryptohftdata",
            venue="binance_futures",
            instrument="BTCUSDT",
            hour_utc=_hour(),
            content_sha256=("A" * 64),
        )


def test_duplicate_source_provenance_is_rejected() -> None:
    source = SourceHourReference(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
        content_sha256="a" * 64,
    )

    with pytest.raises(
        ValueError,
        match="duplicate",
    ):
        L2HourlyProvenance(
            source_hours=(source, source),
        )


def test_l2_quality_summary_must_match_decoded_channels(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset()
    repository.ensure_preset(preset)

    _block_value, encoded, summary = _encoded_inputs()

    changed = dict(summary)
    changed["valid_count"] = int(summary["valid_count"]) - 1
    changed["invalid_count"] = int(summary["invalid_count"]) + 1

    with pytest.raises(
        ValueError,
        match="Decoded L2 valid count",
    ):
        repository.write_l2_hour(
            preset=preset,
            hour_utc=_hour(),
            encoded=encoded,
            quality_summary_json=changed,
            provenance=_provenance(),
        )


def test_disabled_unreferenced_preset_can_be_deleted(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset(
        upper="0.031",
    )

    created = repository.ensure_preset(
        preset,
        enabled=False,
    )

    removed = repository.delete_preset_if_unreferenced(
        preset.preset_hash,
    )

    assert removed.preset_hash == created.preset.preset_hash
    assert removed.enabled is False

    assert repository.get_preset(preset.preset_hash) is None


def test_enabled_preset_cannot_be_deleted(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset(
        upper="0.032",
    )

    repository.ensure_preset(
        preset,
        enabled=True,
    )

    with pytest.raises(
        PresetDeletionBlockedError,
        match="Disable",
    ):
        repository.delete_preset_if_unreferenced(
            preset.preset_hash,
        )

    assert repository.get_preset(preset.preset_hash) is not None


def test_referenced_preset_cannot_be_deleted(
    database_session: Session,
) -> None:
    repository = AnalyticalRepository(database_session)
    preset = _preset(
        upper="0.033",
    )
    repository.ensure_preset(
        preset,
        enabled=False,
    )

    _block_value, encoded, summary = _encoded_inputs()

    repository.write_l2_hour(
        preset=preset,
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=_provenance(),
    )

    with pytest.raises(
        PresetDeletionBlockedError,
        match="historical L2 hourly rows",
    ):
        repository.delete_preset_if_unreferenced(
            preset.preset_hash,
        )

    assert repository.get_preset(preset.preset_hash) is not None

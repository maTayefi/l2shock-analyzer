from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from l2shock.db import (
    PriceAnalyticalRepository,
    PriceHourlyProvenance,
    PriceHourlySeriesConflictError,
    PriceSourceHourReference,
    get_engine,
)
from l2shock.db.schema import verify_schema
from l2shock.ingest import TradeRecord
from l2shock.price import (
    build_trade_ohlc_hour,
    encode_hourly_trade_ohlc_block,
    trade_ohlc_quality_summary_to_dict,
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

        if transaction.is_active:
            transaction.rollback()

        connection.close()


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2082,
        1,
        1,
        offset,
        tzinfo=timezone.utc,
    )


def _epoch_ms(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


def _record(
    *,
    hour: datetime,
    trade_id: str,
    milliseconds: int,
    price: str,
) -> TradeRecord:
    trade_time_ms = _epoch_ms(hour) + milliseconds

    return TradeRecord(
        symbol="BTCUSDT",
        trade_id=trade_id,
        price=Decimal(price),
        quantity=Decimal("1"),
        received_time_ns=trade_time_ms * 1_000_000,
        event_time_ms=trade_time_ms,
        trade_time_ms=trade_time_ms,
        is_buyer_maker=False,
        order_type="market",
        row_number=milliseconds,
    )


def _encoded_inputs(
    *,
    hour: datetime | None = None,
    first_price: str = "100",
):
    target = hour or _hour()

    block = build_trade_ohlc_hour(
        (
            _record(
                hour=target,
                trade_id="first",
                milliseconds=100,
                price=first_price,
            ),
            _record(
                hour=target,
                trade_id="second",
                milliseconds=900,
                price="105",
            ),
            _record(
                hour=target,
                trade_id="third",
                milliseconds=2_100,
                price="99",
            ),
        ),
        base="BTC",
        hour_utc=target,
    )

    encoded = encode_hourly_trade_ohlc_block(block)
    summary = trade_ohlc_quality_summary_to_dict(block.quality_summary)

    return block, encoded, summary


def _provenance(
    *,
    hour: datetime | None = None,
    digest: str = "a" * 64,
) -> PriceHourlyProvenance:
    target = hour or _hour()

    return PriceHourlyProvenance(
        source_hours=(
            PriceSourceHourReference(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument="BTCUSDT",
                hour_utc=target,
                content_sha256=digest,
            ),
        ),
    )


def test_price_hour_write_and_read_round_trip(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)
    block, encoded, summary = _encoded_inputs()

    result = repository.write_price_hour(
        base="BTC",
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=_provenance(),
    )

    assert result.inserted is True

    stored = repository.get_price_hour(
        base="BTC",
        hour_utc=_hour(),
    )

    assert stored is not None
    assert stored.base == "BTC"
    assert stored.hour_utc == _hour()
    assert stored.source_venue == "binance_futures"
    assert stored.source_symbol == "BTCUSDT"
    assert stored.sampling_interval_ms == 1_000
    assert stored.observation_count == 3_600
    assert stored.encoded == encoded
    assert dict(stored.quality_summary_json) == summary

    provenance = dict(stored.provenance_json)

    assert provenance["schema"] == ("l2shock.price_hourly_series_provenance")
    assert provenance["source_hours"][0]["data_kind"] == "trades"
    assert provenance["source_hours"][0]["instrument"] == "BTCUSDT"

    assert block.hour_utc == stored.hour_utc


def test_identical_price_hour_write_is_idempotent(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)
    _block, encoded, summary = _encoded_inputs()
    provenance = _provenance()

    first = repository.write_price_hour(
        base="BTC",
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=provenance,
    )
    second = repository.write_price_hour(
        base="BTC",
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=provenance,
    )

    assert first.inserted is True
    assert second.inserted is False
    assert first.series.encoded == second.series.encoded


def test_conflicting_price_content_is_rejected(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)

    _first_block, first_encoded, first_summary = _encoded_inputs(first_price="100")
    _second_block, second_encoded, second_summary = _encoded_inputs(first_price="101")

    repository.write_price_hour(
        base="BTC",
        hour_utc=_hour(),
        encoded=first_encoded,
        quality_summary_json=first_summary,
        provenance=_provenance(),
    )

    with pytest.raises(
        PriceHourlySeriesConflictError,
        match="different content",
    ):
        repository.write_price_hour(
            base="BTC",
            hour_utc=_hour(),
            encoded=second_encoded,
            quality_summary_json=second_summary,
            provenance=_provenance(),
        )


def test_conflicting_price_provenance_is_rejected(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)
    _block, encoded, summary = _encoded_inputs()

    repository.write_price_hour(
        base="BTC",
        hour_utc=_hour(),
        encoded=encoded,
        quality_summary_json=summary,
        provenance=_provenance(
            digest="a" * 64,
        ),
    )

    with pytest.raises(
        PriceHourlySeriesConflictError,
        match="provenance",
    ):
        repository.write_price_hour(
            base="BTC",
            hour_utc=_hour(),
            encoded=encoded,
            quality_summary_json=summary,
            provenance=_provenance(
                digest="b" * 64,
            ),
        )


def test_quality_summary_must_match_decoded_channels(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)
    _block, encoded, summary = _encoded_inputs()

    changed = dict(summary)
    changed["total_trade_count"] = int(summary["total_trade_count"]) + 1

    with pytest.raises(
        ValueError,
        match="total trade count",
    ):
        repository.write_price_hour(
            base="BTC",
            hour_utc=_hour(),
            encoded=encoded,
            quality_summary_json=changed,
            provenance=_provenance(),
        )


def test_price_range_read_is_half_open_and_ordered(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)

    for offset in (0, 1, 2):
        current = _hour(offset)
        _block, encoded, summary = _encoded_inputs(
            hour=current,
        )

        repository.write_price_hour(
            base="BTC",
            hour_utc=current,
            encoded=encoded,
            quality_summary_json=summary,
            provenance=_provenance(
                hour=current,
                digest=chr(ord("a") + offset) * 64,
            ),
        )

    observed = repository.list_price_hours(
        base="BTC",
        start_utc=_hour(0),
        end_utc=_hour(2),
    )

    assert [item.hour_utc for item in observed] == [
        _hour(0),
        _hour(1),
    ]


def test_missing_price_hour_returns_none(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)

    assert (
        repository.get_price_hour(
            base="BTC",
            hour_utc=_hour(),
        )
        is None
    )


def test_invalid_price_range_is_rejected(
    database_session: Session,
) -> None:
    repository = PriceAnalyticalRepository(database_session)

    with pytest.raises(
        ValueError,
        match="must be after",
    ):
        repository.list_price_hours(
            base="BTC",
            start_utc=_hour(),
            end_utc=_hour(),
        )

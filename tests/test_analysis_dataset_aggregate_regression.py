from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.dataset import (
    AnalysisDatasetError,
    _aggregate_series_to_l2_seconds,
)
from l2shock.analysis.multi_market import (
    AggregateMarketKey,
    AggregateMarketSeries,
    aggregate_market_l2_seconds,
)
from l2shock.ingest import (
    BookSampleInvalidReason,
    BookSampleQuality,
)


def _start() -> datetime:
    return datetime(
        2026,
        9,
        9,
        12,
        tzinfo=timezone.utc,
    )


def _market(
    venue: str,
    instrument: str,
) -> AggregateMarketKey:
    return AggregateMarketKey(
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
    )


def _valid(
    index: int,
    *,
    bid: str,
    ask: str,
) -> L2Second:
    return L2Second(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(bid),
        ask_liquidity=Decimal(ask),
        source_count=1,
    )


def test_partial_aggregate_is_rejected_before_analysis() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    aggregate = aggregate_market_l2_seconds(
        expected_markets=(binance, okx),
        market_series=(
            AggregateMarketSeries(
                market=binance,
                component_preset_hash="a" * 64,
                l2_content_sha256_by_hour={
                    _start(): "b" * 64,
                },
                observations=(
                    _valid(
                        0,
                        bid="100",
                        ask="120",
                    ),
                ),
            ),
            AggregateMarketSeries(
                market=okx,
                component_preset_hash="c" * 64,
                l2_content_sha256_by_hour={},
                observations=(),
            ),
        ),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )

    with pytest.raises(
        AnalysisDatasetError,
        match="every expected market",
    ):
        _aggregate_series_to_l2_seconds(aggregate)


def test_empty_aggregate_converts_to_explicit_invalid_second() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    aggregate = aggregate_market_l2_seconds(
        expected_markets=(binance, okx),
        market_series=(
            AggregateMarketSeries(
                market=binance,
                component_preset_hash="a" * 64,
                l2_content_sha256_by_hour={},
                observations=(),
            ),
            AggregateMarketSeries(
                market=okx,
                component_preset_hash="b" * 64,
                l2_content_sha256_by_hour={},
                observations=(),
            ),
        ),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )

    converted = _aggregate_series_to_l2_seconds(aggregate)

    assert len(converted) == 1
    assert converted[0].quality is BookSampleQuality.INVALID
    assert converted[0].invalid_reason is (BookSampleInvalidReason.UNINITIALIZED)
    assert converted[0].bid_liquidity is None
    assert converted[0].ask_liquidity is None
    assert converted[0].source_count == 0
    assert converted[0].coverage_degraded is False

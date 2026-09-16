from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext
import pytest
from l2shock.analysis import (
    AggregateL2Error,
    AggregateL2QualityState,
    AggregateMarketContribution,
    AggregateMarketKey,
    AggregateMarketSeries,
    L2Second,
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


def _invalid(index: int) -> L2Second:
    return L2Second(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=BookSampleQuality.INVALID,
        invalid_reason=(BookSampleInvalidReason.REPLAY_INVALIDATED),
        bid_liquidity=None,
        ask_liquidity=None,
        source_count=0,
    )


def _series(
    market: AggregateMarketKey,
    observations: tuple[L2Second, ...],
    *,
    preset_character: str,
    content_character: str,
) -> AggregateMarketSeries:
    return AggregateMarketSeries(
        market=market,
        component_preset_hash=preset_character * 64,
        l2_content_sha256_by_hour={
            _start(): content_character * 64,
        },
        observations=observations,
    )


def test_all_expected_markets_produce_valid_sum() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    result = aggregate_market_l2_seconds(
        expected_markets=(okx, binance),
        market_series=(
            _series(
                binance,
                (_valid(0, bid="100", ask="120"),),
                preset_character="a",
                content_character="b",
            ),
            _series(
                okx,
                (_valid(0, bid="30", ask="40"),),
                preset_character="c",
                content_character="d",
            ),
        ),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )

    observation = result.observations[0]

    assert observation.quality_state is (AggregateL2QualityState.VALID)
    assert observation.expected_market_count == 2
    assert observation.contributing_market_count == 2
    assert observation.source_count == 2
    assert observation.bid_liquidity == Decimal("130")
    assert observation.ask_liquidity == Decimal("160")
    assert observation.total_liquidity == Decimal("290")
    with localcontext(Context(prec=34)):
        expected_imbalance = Decimal("-30") / Decimal("290")
    assert observation.bid_ask_imbalance() == expected_imbalance


def test_one_valid_market_produces_degraded_partial_sum() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    result = aggregate_market_l2_seconds(
        expected_markets=(binance, okx),
        market_series=(
            _series(
                binance,
                (_valid(0, bid="100", ask="120"),),
                preset_character="a",
                content_character="b",
            ),
            _series(
                okx,
                (_invalid(0),),
                preset_character="c",
                content_character="d",
            ),
        ),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )

    observation = result.observations[0]

    assert observation.quality_state is (AggregateL2QualityState.DEGRADED)
    assert observation.expected_market_count == 2
    assert observation.contributing_market_count == 1
    assert observation.bid_liquidity == Decimal("100")
    assert observation.ask_liquidity == Decimal("120")
    assert len(observation.contributions) == 1
    assert observation.contributions[0].market == binance


def test_no_valid_market_produces_invalid_observation() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    result = aggregate_market_l2_seconds(
        expected_markets=(binance, okx),
        market_series=(
            _series(
                binance,
                (_invalid(0),),
                preset_character="a",
                content_character="b",
            ),
            _series(
                okx,
                (),
                preset_character="c",
                content_character="d",
            ),
        ),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )

    observation = result.observations[0]

    assert observation.quality_state is (AggregateL2QualityState.INVALID)
    assert observation.contributing_market_count == 0
    assert observation.bid_liquidity is None
    assert observation.ask_liquidity is None
    assert observation.total_liquidity is None
    assert observation.bid_ask_imbalance() is None


def test_missing_component_timestamp_is_degraded_not_forward_filled() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    result = aggregate_market_l2_seconds(
        expected_markets=(binance, okx),
        market_series=(
            _series(
                binance,
                (
                    _valid(0, bid="100", ask="120"),
                    _valid(1, bid="101", ask="121"),
                ),
                preset_character="a",
                content_character="b",
            ),
            _series(
                okx,
                (_valid(0, bid="30", ask="40"),),
                preset_character="c",
                content_character="d",
            ),
        ),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=2),
    )

    assert result.observations[0].quality_state is (AggregateL2QualityState.VALID)
    assert result.observations[1].quality_state is (AggregateL2QualityState.DEGRADED)
    assert result.observations[1].bid_liquidity == Decimal("101")
    assert result.observations[1].ask_liquidity == Decimal("121")


def test_component_series_order_does_not_change_identity() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    binance_series = _series(
        binance,
        (_valid(0, bid="100", ask="120"),),
        preset_character="a",
        content_character="b",
    )
    okx_series = _series(
        okx,
        (_valid(0, bid="30", ask="40"),),
        preset_character="c",
        content_character="d",
    )

    first = aggregate_market_l2_seconds(
        expected_markets=(binance, okx),
        market_series=(binance_series, okx_series),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )
    second = aggregate_market_l2_seconds(
        expected_markets=(okx, binance),
        market_series=(okx_series, binance_series),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )

    assert first.expected_markets == second.expected_markets
    assert first.observations == second.observations
    assert first.aggregate_content_sha256 == (second.aggregate_content_sha256)


def test_component_series_must_cover_exactly_expected_markets() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    with pytest.raises(
        AggregateL2Error,
        match="cover exactly",
    ):
        aggregate_market_l2_seconds(
            expected_markets=(binance, okx),
            market_series=(
                _series(
                    binance,
                    (_valid(0, bid="100", ask="120"),),
                    preset_character="a",
                    content_character="b",
                ),
            ),
            start_utc=_start(),
            end_utc=_start() + timedelta(seconds=1),
        )


def test_zero_total_liquidity_has_invalid_imbalance_only() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )

    result = aggregate_market_l2_seconds(
        expected_markets=(binance,),
        market_series=(
            _series(
                binance,
                (_valid(0, bid="0", ask="0"),),
                preset_character="a",
                content_character="b",
            ),
        ),
        start_utc=_start(),
        end_utc=_start() + timedelta(seconds=1),
    )

    observation = result.observations[0]

    assert observation.quality_state is (AggregateL2QualityState.VALID)
    assert observation.total_liquidity == Decimal("0")
    assert observation.bid_ask_imbalance() is None


def test_aggregate_decimal_sum_is_independent_of_ambient_context() -> None:
    binance = _market(
        "binance_futures",
        "BTCUSDT",
    )
    okx = _market(
        "okx_futures",
        "BTC-USDT-SWAP",
    )

    with localcontext(Context(prec=6)):
        result = aggregate_market_l2_seconds(
            expected_markets=(binance, okx),
            market_series=(
                _series(
                    binance,
                    (
                        _valid(
                            0,
                            bid="123456789.123456789",
                            ask="987654321.987654321",
                        ),
                    ),
                    preset_character="a",
                    content_character="b",
                ),
                _series(
                    okx,
                    (
                        _valid(
                            0,
                            bid="0.000000001",
                            ask="0.000000009",
                        ),
                    ),
                    preset_character="c",
                    content_character="d",
                ),
            ),
            start_utc=_start(),
            end_utc=_start() + timedelta(seconds=1),
        )

    observation = result.observations[0]

    assert observation.bid_liquidity == Decimal("123456789.123456790")
    assert observation.ask_liquidity == Decimal("987654321.987654330")
    assert observation.total_liquidity == Decimal("1111111111.111111120")

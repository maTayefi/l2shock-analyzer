from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.analysis import (
    AnalysisBarQuality,
    AnalysisDatasetError,
    AnalysisDatasetRequest,
    AnalysisDiscontinuityReason,
    AnalysisHourCoverage,
    AnalysisMarketCoverage,
    PriceBounds,
    build_aligned_analysis_dataset,
)
from l2shock.ingest import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.price import (
    TradeSampleInvalidReason,
    TradeSampleQuality,
)
from l2shock.analysis import L2Second, PriceSecond


def _start() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _valid_l2(
    index: int,
    *,
    bid: str = "100",
    ask: str = "200",
) -> L2Second:
    return L2Second(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(bid),
        ask_liquidity=Decimal(ask),
        source_count=1,
    )


def _invalid_l2(index: int) -> L2Second:
    return L2Second(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.REPLAY_INVALIDATED,
        bid_liquidity=None,
        ask_liquidity=None,
        source_count=0,
    )


def _valid_price(
    index: int,
    *,
    open_value: str = "100",
    high: str = "105",
    low: str = "95",
    close: str = "101",
) -> PriceSecond:
    return PriceSecond(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=TradeSampleQuality.VALID,
        invalid_reason=None,
        open=Decimal(open_value),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        trade_count=1,
    )


def _invalid_price(index: int) -> PriceSecond:
    return PriceSecond(
        timestamp_utc=_start() + timedelta(seconds=index),
        quality=TradeSampleQuality.INVALID,
        invalid_reason=TradeSampleInvalidReason.NO_TRADES,
        open=None,
        high=None,
        low=None,
        close=None,
        trade_count=0,
    )


def _request(
    *,
    end_second: int,
    activity_timeframe: str = "1s",
    maximum_chart_bars: int = 400,
    chart_timeframe_override: str | None = None,
    bounds: PriceBounds | None = None,
    context_before: int = 3,
    context_after: int = 3,
) -> AnalysisDatasetRequest:
    return AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=(_start() + timedelta(seconds=end_second)),
        activity_timeframe=activity_timeframe,
        maximum_chart_bars=maximum_chart_bars,
        chart_timeframe_override=chart_timeframe_override,
        price_bounds=bounds or PriceBounds(),
        context_before=context_before,
        context_after=context_after,
    )


def test_dataset_builds_independent_activity_and_chart_series() -> None:
    l2 = tuple(
        _valid_l2(
            index,
            bid=str(100 + index),
            ask=str(200 + index),
        )
        for index in range(25)
    )
    price = tuple(
        _valid_price(
            index,
            open_value=str(100 + index),
            high=str(102 + index),
            low=str(99 + index),
            close=str(101 + index),
        )
        for index in range(25)
    )

    dataset = build_aligned_analysis_dataset(
        _request(
            end_second=20,
            activity_timeframe="1s",
            maximum_chart_bars=5,
        ),
        l2_seconds=l2,
        price_seconds=price,
    )

    assert dataset.activity.timeframe.label == "1s"
    assert len(dataset.activity.bars) == 21

    assert dataset.chart.timeframe.label == "5s"
    assert len(dataset.chart.bars) == 5

    assert dataset.chart.bars[-1].start_utc == (_start() + timedelta(seconds=20))
    assert dataset.chart.bars[-1].end_utc == (_start() + timedelta(seconds=25))


def test_price_excluded_run_creates_two_independent_segments() -> None:
    l2 = tuple(_valid_l2(index) for index in range(6))

    price = (
        _valid_price(0),
        _valid_price(1),
        _valid_price(
            2,
            open_value="130",
            high="135",
            low="125",
            close="132",
        ),
        _valid_price(
            3,
            open_value="131",
            high="136",
            low="126",
            close="133",
        ),
        _valid_price(4),
        _valid_price(5),
    )

    dataset = build_aligned_analysis_dataset(
        _request(
            end_second=5,
            bounds=PriceBounds(
                min_price=90,
                max_price=110,
            ),
            context_before=1,
            context_after=1,
        ),
        l2_seconds=l2,
        price_seconds=price,
    )

    series = dataset.activity

    assert [
        (
            segment.core_start_index,
            segment.core_end_index,
        )
        for segment in series.segments
    ] == [
        (0, 1),
        (4, 5),
    ]

    assert (
        series.segments[0].context_start_index,
        series.segments[0].context_end_index,
    ) == (0, 2)

    assert (
        series.segments[1].context_start_index,
        series.segments[1].context_end_index,
    ) == (3, 5)

    assert len(series.discontinuities) == 1

    discontinuity = series.discontinuities[0]

    assert discontinuity.start_index == 2
    assert discontinuity.end_index == 3
    assert discontinuity.skipped_seconds == 2
    assert discontinuity.reasons == (AnalysisDiscontinuityReason.PRICE_EXCLUDED,)


def test_hard_l2_boundary_cannot_be_used_as_context() -> None:
    l2 = (
        _valid_l2(0),
        _valid_l2(1),
        _invalid_l2(2),
        _valid_l2(3),
        _valid_l2(4),
    )
    price = tuple(_valid_price(index) for index in range(5))

    dataset = build_aligned_analysis_dataset(
        _request(
            end_second=4,
            context_before=3,
            context_after=3,
        ),
        l2_seconds=l2,
        price_seconds=price,
    )

    series = dataset.activity

    assert [
        (
            segment.core_start_index,
            segment.core_end_index,
        )
        for segment in series.segments
    ] == [
        (0, 1),
        (3, 4),
    ]

    assert series.segments[0].context_end_index == 1
    assert series.segments[1].context_start_index == 3

    boundary = series.discontinuities[0]

    assert AnalysisDiscontinuityReason.L2_INVALID in boundary.reasons
    assert AnalysisDiscontinuityReason.L2_COVERAGE_GAP in boundary.reasons


def test_invalid_price_bar_is_a_hard_discontinuity() -> None:
    l2 = tuple(_valid_l2(index) for index in range(4))
    price = (
        _valid_price(0),
        _invalid_price(1),
        _valid_price(2),
        _valid_price(3),
    )

    dataset = build_aligned_analysis_dataset(
        _request(end_second=3),
        l2_seconds=l2,
        price_seconds=price,
    )

    assert [
        (
            segment.core_start_index,
            segment.core_end_index,
        )
        for segment in dataset.activity.segments
    ] == [
        (0, 0),
        (2, 3),
    ]

    reasons = dataset.activity.discontinuities[0].reasons

    assert AnalysisDiscontinuityReason.PRICE_INVALID in reasons
    assert AnalysisDiscontinuityReason.PRICE_COVERAGE_GAP in reasons


def test_missing_second_degrades_aggregate_and_blocks_core() -> None:
    l2 = tuple(_valid_l2(index) for index in (0, 1, 3, 4))
    price = tuple(_valid_price(index) for index in range(5))

    dataset = build_aligned_analysis_dataset(
        _request(
            end_second=4,
            activity_timeframe="5s",
        ),
        l2_seconds=l2,
        price_seconds=price,
    )

    bar = dataset.activity.bars[0]

    assert bar.l2.quality is AnalysisBarQuality.DEGRADED
    assert bar.l2.missing_seconds == 1
    assert bar.hard_discontinuity is True
    assert bar.core_eligible is False

    assert dataset.activity.segments == ()
    assert len(dataset.activity.discontinuities) == 1


def test_display_clipping_does_not_change_exact_price_bar() -> None:
    l2 = (_valid_l2(0),)
    price = (
        _valid_price(
            0,
            open_value="220",
            high="230",
            low="195",
            close="225",
        ),
    )

    dataset = build_aligned_analysis_dataset(
        _request(
            end_second=0,
            bounds=PriceBounds(
                min_price=100,
                max_price=200,
            ),
        ),
        l2_seconds=l2,
        price_seconds=price,
    )

    bar = dataset.activity.bars[0]

    assert bar.core_eligible is True

    assert bar.price.open == Decimal("220")
    assert bar.price.high == Decimal("230")
    assert bar.price.low == Decimal("195")
    assert bar.price.close == Decimal("225")

    assert bar.display_ohlc is not None
    assert bar.display_ohlc.open == 200.0
    assert bar.display_ohlc.high == 200.0
    assert bar.display_ohlc.low == 195.0
    assert bar.display_ohlc.close == 200.0


def test_analysis_identity_is_deterministic_and_coverage_order_independent() -> None:
    request = _request(end_second=0)
    l2 = (_valid_l2(0),)
    price = (_valid_price(0),)

    first_coverage = (
        AnalysisHourCoverage(
            hour_utc=_start(),
            l2_content_sha256="b" * 64,
            price_content_sha256="c" * 64,
        ),
        AnalysisHourCoverage(
            hour_utc=_start() + timedelta(hours=1),
            l2_content_sha256=None,
            price_content_sha256=None,
        ),
    )

    first = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
        coverage=first_coverage,
    )
    second = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
        coverage=tuple(reversed(first_coverage)),
    )

    assert first.analysis_id == second.analysis_id
    assert len(first.analysis_id) == 64
    assert first.provenance.to_canonical_dict() == (
        second.provenance.to_canonical_dict()
    )


def test_analysis_identity_changes_when_persisted_content_changes() -> None:
    request = _request(end_second=0)
    l2 = (_valid_l2(0),)
    price = (_valid_price(0),)

    first = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
        coverage=(
            AnalysisHourCoverage(
                hour_utc=_start(),
                l2_content_sha256="a" * 64,
                price_content_sha256="b" * 64,
            ),
        ),
    )
    second = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
        coverage=(
            AnalysisHourCoverage(
                hour_utc=_start(),
                l2_content_sha256="c" * 64,
                price_content_sha256="b" * 64,
            ),
        ),
    )

    assert first.analysis_id != second.analysis_id


def test_explicit_chart_timeframe_override_is_used() -> None:
    l2 = tuple(_valid_l2(index) for index in range(25))
    price = tuple(_valid_price(index) for index in range(25))

    request = AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(seconds=24),
        activity_timeframe="1s",
        maximum_chart_bars=400,
        chart_timeframe_override="10s",
        context_before=0,
        context_after=0,
    )

    dataset = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
    )

    assert dataset.activity.timeframe.label == "1s"
    assert dataset.chart.timeframe.label == "10s"
    assert dataset.request.chart_timeframe_override is not None
    assert dataset.request.chart_timeframe_override.label == "10s"


def test_explicit_and_automatic_chart_policy_have_distinct_identity() -> None:
    l2 = tuple(_valid_l2(index) for index in range(25))
    price = tuple(_valid_price(index) for index in range(25))

    automatic_request = AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(seconds=24),
        activity_timeframe="1s",
        maximum_chart_bars=5,
        context_before=0,
        context_after=0,
    )
    explicit_request = AnalysisDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_start(),
        requested_end_utc=_start() + timedelta(seconds=24),
        activity_timeframe="1s",
        maximum_chart_bars=5,
        chart_timeframe_override="5s",
        context_before=0,
        context_after=0,
    )

    automatic = build_aligned_analysis_dataset(
        automatic_request,
        l2_seconds=l2,
        price_seconds=price,
    )
    explicit = build_aligned_analysis_dataset(
        explicit_request,
        l2_seconds=l2,
        price_seconds=price,
    )

    assert automatic.chart.timeframe.label == "5s"
    assert explicit.chart.timeframe.label == "5s"

    assert automatic.analysis_id != explicit.analysis_id

    automatic_chart_identity = automatic.provenance.to_canonical_dict()["chart"]
    explicit_chart_identity = explicit.provenance.to_canonical_dict()["chart"]

    assert automatic_chart_identity["selection_policy"] == (
        "automatic_finest_within_maximum_bars"
    )
    assert automatic_chart_identity["requested_timeframe"] is None

    assert explicit_chart_identity["selection_policy"] == "explicit"
    assert explicit_chart_identity["requested_timeframe"] == "5s"


def test_partial_market_coverage_is_rejected_by_dataset_builder() -> None:
    request = _request(
        end_second=0,
    )

    with pytest.raises(
        AnalysisDatasetError,
        match="Partial multi-market L2 observations",
    ):
        build_aligned_analysis_dataset(
            request,
            l2_seconds=(
                L2Second(
                    timestamp_utc=_start(),
                    quality=BookSampleQuality.VALID,
                    invalid_reason=None,
                    bid_liquidity=Decimal("100"),
                    ask_liquidity=Decimal("200"),
                    source_count=1,
                    coverage_degraded=True,
                ),
            ),
            price_seconds=(_valid_price(0),),
            coverage=(
                AnalysisHourCoverage(
                    hour_utc=_start(),
                    l2_content_sha256="d" * 64,
                    price_content_sha256="e" * 64,
                    l2_market_coverage=(
                        AnalysisMarketCoverage(
                            provider="cryptohftdata",
                            venue="binance_futures",
                            instrument="BTCUSDT",
                            component_preset_hash="a" * 64,
                            content_sha256="b" * 64,
                        ),
                        AnalysisMarketCoverage(
                            provider="cryptohftdata",
                            venue="okx_futures",
                            instrument="BTC-USDT-SWAP",
                            component_preset_hash="c" * 64,
                            content_sha256=None,
                        ),
                    ),
                ),
            ),
        )


def test_market_coverage_is_part_of_analysis_identity() -> None:
    request = _request(end_second=0)
    l2 = (_valid_l2(0),)
    price = (_valid_price(0),)

    first = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
        coverage=(
            AnalysisHourCoverage(
                hour_utc=_start(),
                l2_content_sha256="d" * 64,
                price_content_sha256="e" * 64,
                l2_market_coverage=(
                    AnalysisMarketCoverage(
                        provider="cryptohftdata",
                        venue="binance_futures",
                        instrument="BTCUSDT",
                        component_preset_hash="a" * 64,
                        content_sha256="b" * 64,
                    ),
                    AnalysisMarketCoverage(
                        provider="cryptohftdata",
                        venue="okx_futures",
                        instrument="BTC-USDT-SWAP",
                        component_preset_hash="c" * 64,
                        content_sha256=None,
                    ),
                ),
            ),
        ),
    )
    second = build_aligned_analysis_dataset(
        request,
        l2_seconds=l2,
        price_seconds=price,
        coverage=(
            AnalysisHourCoverage(
                hour_utc=_start(),
                l2_content_sha256="f" * 64,
                price_content_sha256="e" * 64,
                l2_market_coverage=(
                    AnalysisMarketCoverage(
                        provider="cryptohftdata",
                        venue="binance_futures",
                        instrument="BTCUSDT",
                        component_preset_hash="a" * 64,
                        content_sha256="b" * 64,
                    ),
                    AnalysisMarketCoverage(
                        provider="cryptohftdata",
                        venue="okx_futures",
                        instrument="BTC-USDT-SWAP",
                        component_preset_hash="c" * 64,
                        content_sha256="9" * 64,
                    ),
                ),
            ),
        ),
    )

    assert first.analysis_id != second.analysis_id


def test_in_memory_observations_are_part_of_dataset_identity() -> None:
    request = _request(end_second=0)

    first = build_aligned_analysis_dataset(
        request,
        l2_seconds=(
            _valid_l2(
                0,
                bid="100",
                ask="200",
            ),
        ),
        price_seconds=(_valid_price(0),),
    )

    second = build_aligned_analysis_dataset(
        request,
        l2_seconds=(
            _valid_l2(
                0,
                bid="100.0000000000000000001",
                ask="200",
            ),
        ),
        price_seconds=(_valid_price(0),),
    )

    assert first.coverage == ()
    assert second.coverage == ()
    assert first.analysis_id != second.analysis_id

    assert (
        first.provenance.observation_content_sha256
        != second.provenance.observation_content_sha256
    )


def test_price_bounds_use_canonical_decimal_identity() -> None:
    request = _request(
        end_second=0,
        bounds=PriceBounds(
            min_price=Decimal("90.0000"),
            max_price=Decimal("110.1234567890123456789"),
        ),
    )

    dataset = build_aligned_analysis_dataset(
        request,
        l2_seconds=(_valid_l2(0),),
        price_seconds=(_valid_price(0),),
    )

    price_filter = dataset.provenance.to_canonical_dict()["price_filter"]

    assert price_filter["min_price"] == "90"
    assert price_filter["max_price"] == "110.1234567890123456789"
    assert price_filter["encoding"] == "canonical_decimal_string"


def test_partial_market_coverage_outside_effective_ranges_is_ignored() -> None:
    request = _request(
        end_second=0,
        context_before=0,
        context_after=0,
    )

    dataset = build_aligned_analysis_dataset(
        request,
        l2_seconds=(
            _valid_l2(0),
            L2Second(
                timestamp_utc=_start() + timedelta(seconds=30),
                quality=BookSampleQuality.VALID,
                invalid_reason=None,
                bid_liquidity=Decimal("100"),
                ask_liquidity=Decimal("200"),
                source_count=1,
                coverage_degraded=True,
            ),
        ),
        price_seconds=(_valid_price(0),),
    )

    assert dataset.activity.bars[0].core_eligible is True
    assert dataset.chart.bars[0].core_eligible is True


def test_explicit_chart_timeframe_must_fit_chart_bar_budget() -> None:
    request = _request(
        end_second=20,
        maximum_chart_bars=5,
        chart_timeframe_override="1s",
    )

    with pytest.raises(
        AnalysisDatasetError,
        match="explicit chart timeframe produces 21 bars",
    ):
        build_aligned_analysis_dataset(
            request,
            l2_seconds=tuple(_valid_l2(index) for index in range(21)),
            price_seconds=tuple(_valid_price(index) for index in range(21)),
        )


def test_explicit_chart_timeframe_is_retained_when_it_fits_budget() -> None:
    request = _request(
        end_second=20,
        maximum_chart_bars=5,
        chart_timeframe_override="5s",
    )

    dataset = build_aligned_analysis_dataset(
        request,
        l2_seconds=tuple(_valid_l2(index) for index in range(25)),
        price_seconds=tuple(_valid_price(index) for index in range(25)),
    )

    assert dataset.chart.timeframe.label == "5s"
    assert len(dataset.chart.bars) == 5

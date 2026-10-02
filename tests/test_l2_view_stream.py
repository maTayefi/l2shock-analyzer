# tests/test_l2_view_stream.py
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import l2shock.analysis.l2_view_stream as module
from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.l2_view_metrics import L2ViewMetric, compute_l2_view_metric
from l2shock.analysis.l2_view_stream import (
    L2ViewError,
    L2ViewLoadOptions,
    L2ViewRequest,
    coarsen_l2_view,
    stream_l2_view,
)
from l2shock.ingest.sampling import BookSampleInvalidReason, BookSampleQuality
from l2shock.presets import build_binance_futures_data_preset
from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

_HOUR = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
_INVALID = frozenset(range(100, 200))


def _preset():
    return build_binance_futures_data_preset(
        base="BTC", lower_fraction=Decimal("0"), upper_fraction=Decimal("0.01")
    )


def _decode(row):
    out = []
    for i in range(3_600):
        ts = row.hour_utc + timedelta(seconds=i)
        if i in _INVALID:
            out.append(
                L2Second(
                    ts,
                    BookSampleQuality.INVALID,
                    BookSampleInvalidReason.UNINITIALIZED,
                    None,
                    None,
                    0,
                )
            )
        else:
            out.append(
                L2Second(
                    ts,
                    BookSampleQuality.VALID,
                    None,
                    Decimal(10 + i % 7),
                    Decimal(5),
                    1,
                )
            )
    return tuple(out)


class _Repo:
    def __init__(self, preset):
        self.preset = preset

    def get_preset(self, preset_hash):
        return SimpleNamespace(base="BTC", config_json=self.preset.to_canonical_dict())

    def list_l2_hours(
        self, *, base, preset_hash, start_utc, end_utc, verify_codec=True
    ):
        assert verify_codec
        hour = start_utc
        rows = []
        while hour < end_utc:
            if hour == _HOUR:
                rows.append(
                    SimpleNamespace(
                        base=base,
                        preset_hash=preset_hash,
                        hour_utc=hour,
                        encoded=SimpleNamespace(content_sha256="a" * 64),
                    )
                )
            hour += timedelta(hours=1)
        return tuple(rows)


class _NoPrice:
    def list_price_hours(self, **_kwargs):
        return ()


def _opener(preset):
    @contextmanager
    def open_repositories():
        yield _Repo(preset), _NoPrice()

    return open_repositories


def _run(monkeypatch, end_offset=3_599, timeframe=60):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _preset()
    request = L2ViewRequest(
        "BTC", preset.preset_hash, _HOUR, _HOUR + timedelta(seconds=end_offset)
    )
    return stream_l2_view(
        request,
        L2ViewLoadOptions(timeframe_seconds=timeframe),
        open_repositories=_opener(preset),
    )


def test_bars_cover_range_and_invalid_bars_are_null(monkeypatch):
    p = _run(monkeypatch)
    assert len(p.bars) == 60
    assert p.usable_l2_seconds == 3_500 and p.unusable_l2_seconds == 100

    # Under the relaxed gap policy, bars containing at least one valid second
    # remain renderable. Seconds 100..199 are unusable, but bars 1 and 3
    # still contain valid seconds from the surrounding ranges.
    assert p.bars[1].bid is not None
    assert p.bars[2].bid is None  # completely unusable
    assert p.bars[3].bid is not None
    assert p.bars[0].bid is not None
    assert p.price_status == "missing"
    assert len(p.l2_regions) == 1
    assert p.l2_regions[0].start_utc == _HOUR + timedelta(seconds=100)


def test_coarsen_equals_direct_stream(monkeypatch):
    fine = _run(monkeypatch, timeframe=60)
    direct = _run(monkeypatch, timeframe=900)
    assert coarsen_l2_view(fine, 900).bars == direct.bars


def test_missing_next_hour_is_an_outage_not_zero(monkeypatch):
    p = _run(monkeypatch, end_offset=3_600 + 120, timeframe=60)
    assert p.bars[-1].bid is None
    assert p.l2_regions[-1].end_utc_exclusive == p.end_utc_exclusive


def test_duration_limit_uses_slot_count():
    preset = _preset()
    L2ViewRequest("BTC", preset.preset_hash, _HOUR, _HOUR + timedelta(seconds=86_399))
    with pytest.raises(L2ViewError, match="max analysis duration"):
        L2ViewRequest(
            "BTC", preset.preset_hash, _HOUR, _HOUR + timedelta(seconds=86_400)
        )


def test_change_metrics_null_first_and_after_invalid(monkeypatch):
    p = _run(monkeypatch)
    values = compute_l2_view_metric(p.bars, L2ViewMetric.TOTAL_CHANGE)

    # values[0] is None (first bar has no predecessor)
    # values[2] is None (Bar 2 is completely invalid)
    # values[3] is None (Bar 3 - Bar 2; Bar 2 is invalid)
    # Under the relaxed policy, Bar 4 is valid, so values[4] is computed.
    assert values[0] is None
    assert values[2] is None
    assert values[3] is None
    assert values[4] is not None
    assert values[5] is not None


def test_chart_has_five_panels_and_y_zoom(monkeypatch):
    p = _run(monkeypatch)
    option = build_l2_view_chart_options(
        p, panel_a_metric="imbalance_pct", panel_b_metric="delta"
    )
    assert len(option["grid"]) == 5
    assert option["dataZoom"][0]["xAxisIndex"] == [0, 1, 2, 3, 4]
    assert [z["yAxisIndex"] for z in option["dataZoom"][1:]] == [
        [0],
        [1],
        [2],
        [3],
        [4],
    ]
    assert option["l2shockChartMetadata"]["bar_duration_seconds"] == 60


def _diagnostic_aggregate_projection(
    monkeypatch,
    *,
    component_valid_offsets,
    timeframe=60,
    seconds=120,
):
    """Exercise exact three-market composition with controlled coverage."""
    from contextlib import contextmanager
    from decimal import Decimal
    from types import SimpleNamespace

    from l2shock.presets import (
        build_binance_bybit_okx_futures_data_preset,
        component_data_presets,
    )
    from l2shock.price import TradeSampleQuality

    preset = build_binance_bybit_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.009"),
    )

    components = component_data_presets(preset)
    components_by_hash = {component.preset_hash: component for component in components}

    contribution_values = {
        "binance_futures": (Decimal("10"), Decimal("4")),
        "bybit": (Decimal("20"), Decimal("5")),
        "okx_futures": (Decimal("30"), Decimal("6")),
    }

    component_digests = {
        "binance_futures": "a" * 64,
        "bybit": "b" * 64,
        "okx_futures": "c" * 64,
    }

    class DiagnosticL2Repository:
        def get_preset(self, preset_hash):
            assert preset_hash == preset.preset_hash
            return SimpleNamespace(
                base="BTC",
                config_json=preset.to_canonical_dict(),
            )

        def list_l2_hours(
            self,
            *,
            base,
            preset_hash,
            start_utc,
            end_utc,
            verify_codec=True,
        ):
            assert base == "BTC"
            assert verify_codec is True

            component = components_by_hash[preset_hash]
            venue = component.eligible_markets[0].venue
            valid_offsets = component_valid_offsets[venue]

            if valid_offsets is None:
                return ()

            if not start_utc <= _HOUR < end_utc:
                return ()

            return (
                SimpleNamespace(
                    base=base,
                    preset_hash=preset_hash,
                    hour_utc=_HOUR,
                    diagnostic_venue=venue,
                    encoded=SimpleNamespace(
                        content_sha256=component_digests[venue],
                    ),
                ),
            )

    class DiagnosticPriceRepository:
        def list_price_hours(
            self,
            *,
            base,
            start_utc,
            end_utc,
            verify_codec=True,
        ):
            assert base == "BTC"
            assert verify_codec is True

            if not start_utc <= _HOUR < end_utc:
                return ()

            return (
                SimpleNamespace(
                    base="BTC",
                    hour_utc=_HOUR,
                    source_venue="binance_futures",
                    source_symbol="BTCUSDT",
                    encoded=SimpleNamespace(
                        content_sha256="d" * 64,
                    ),
                ),
            )

    def decode_component(row):
        venue = row.diagnostic_venue
        valid_offsets = component_valid_offsets[venue]
        assert valid_offsets is not None

        bid, ask = contribution_values[venue]
        observations = []

        for offset in range(3_600):
            timestamp = row.hour_utc + timedelta(seconds=offset)

            if offset in valid_offsets:
                observations.append(
                    L2Second(
                        timestamp_utc=timestamp,
                        quality=BookSampleQuality.VALID,
                        invalid_reason=None,
                        bid_liquidity=bid,
                        ask_liquidity=ask,
                        source_count=1,
                    )
                )
            else:
                observations.append(
                    L2Second(
                        timestamp_utc=timestamp,
                        quality=BookSampleQuality.INVALID,
                        invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
                        bid_liquidity=None,
                        ask_liquidity=None,
                        source_count=0,
                    )
                )

        return tuple(observations)

    def decode_price(_encoded):
        value = Decimal("60000")

        return SimpleNamespace(
            quality=(TradeSampleQuality.VALID,) * 3_600,
            open=(value,) * 3_600,
            high=(value,) * 3_600,
            low=(value,) * 3_600,
            close=(value,) * 3_600,
        )

    monkeypatch.setattr(
        module,
        "decode_l2_hour_to_seconds",
        decode_component,
    )
    monkeypatch.setattr(
        module,
        "decode_hourly_trade_ohlc_blocks",
        decode_price,
    )

    @contextmanager
    def open_repositories():
        yield DiagnosticL2Repository(), DiagnosticPriceRepository()

    return stream_l2_view(
        L2ViewRequest(
            base="BTC",
            preset_hash=preset.preset_hash,
            requested_start_utc=_HOUR,
            requested_end_utc=(_HOUR + timedelta(seconds=seconds - 1)),
        ),
        L2ViewLoadOptions(
            timeframe_seconds=timeframe,
            max_bars=1_200,
            l2_warning_seconds=1,
            price_warning_seconds=1,
        ),
        open_repositories=open_repositories,
    )


def test_missing_bybit_is_logged_and_price_remains_available(
    monkeypatch,
    caplog,
):
    import logging

    caplog.set_level(logging.INFO, logger=module.__name__)

    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": None,
            "okx_futures": frozenset(range(120)),
        },
    )

    assert projection.usable_l2_seconds == 120
    assert projection.unusable_l2_seconds == 0
    assert projection.partial_market_seconds == 120
    assert projection.price_status == "loaded"

    assert len(projection.bars) == 2

    for bar in projection.bars:
        assert bar.valid_l2 is True
        assert bar.bid == (Decimal("40"),) * 4
        assert bar.ask == (Decimal("10"),) * 4
        assert bar.total == (Decimal("50"),) * 4
        assert bar.delta == (Decimal("30"),) * 4
        assert bar.bid_share_pct == (80.0,) * 4
        assert bar.price is not None

    # Partial numerical sums remain visible, but their quality warning
    # still covers the requested interval.
    assert len(projection.l2_regions) == 1
    assert projection.l2_regions[0].start_utc == _HOUR
    assert projection.l2_regions[0].end_utc_exclusive == (
        _HOUR + timedelta(seconds=120)
    )

    missing_messages = [
        record.getMessage()
        for record in caplog.records
        if "ANALYSIS L2 COMPONENT MISSING:" in record.getMessage()
    ]

    assert len(missing_messages) == 1
    assert "venue=bybit" in missing_messages[0]
    assert "component_preset_hash=" in missing_messages[0]
    assert "expected_seconds=120" in missing_messages[0]
    assert "reason=no_row_for_exact_component_preset" in missing_messages[0]

    summary_messages = [
        record.getMessage()
        for record in caplog.records
        if "ANALYSIS L2 LOAD SUMMARY:" in record.getMessage()
    ]

    assert len(summary_messages) == 1
    assert "valid_l2_bars=2" in summary_messages[0]
    assert "price_bars=2" in summary_messages[0]
    assert "reason=partial_market_coverage_rendered" in summary_messages[0]


def test_invalid_component_logs_reason_and_owned_interval(
    monkeypatch,
    caplog,
):
    import logging

    caplog.set_level(logging.INFO, logger=module.__name__)

    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(),
            "okx_futures": frozenset(range(120)),
        },
    )

    assert projection.usable_l2_seconds == 120
    assert projection.unusable_l2_seconds == 0
    assert projection.partial_market_seconds == 120
    assert all(bar.valid_l2 for bar in projection.bars)
    assert all(bar.bid == (Decimal("40"),) * 4 for bar in projection.bars)

    bybit_messages = [
        record.getMessage()
        for record in caplog.records
        if (
            "ANALYSIS L2 COMPONENT COVERAGE:" in record.getMessage()
            and "venue=bybit" in record.getMessage()
        )
    ]

    assert len(bybit_messages) == 1
    assert "expected_seconds=120" in bybit_messages[0]
    assert "valid_seconds=0" in bybit_messages[0]
    assert "unusable_seconds=120" in bybit_messages[0]
    assert '"uninitialized":120' in bybit_messages[0]

    # The other 3,480 seconds in this hour are outside the request.
    assert '"uninitialized":3600' not in bybit_messages[0]


def test_all_components_valid_produce_exact_aggregate_candles(
    monkeypatch,
    caplog,
):
    import logging

    caplog.set_level(logging.INFO, logger=module.__name__)

    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(range(120)),
            "okx_futures": frozenset(range(120)),
        },
    )

    assert projection.usable_l2_seconds == 120
    assert projection.unusable_l2_seconds == 0
    assert projection.partial_market_seconds == 0

    for bar in projection.bars:
        assert bar.valid_l2 is True
        assert bar.bid == (Decimal("60"),) * 4
        assert bar.ask == (Decimal("15"),) * 4
        assert bar.total == (Decimal("75"),) * 4
        assert bar.delta == (Decimal("45"),) * 4
        assert bar.bid_share_pct == (80.0,) * 4
        assert bar.price is not None

    assert "valid_l2_bars=2" in caplog.text
    assert "reason=complete_l2_viewing_coverage" in caplog.text
    assert "ANALYSIS L2 COMPONENT MISSING:" not in caplog.text

    option = build_l2_view_chart_options(
        projection,
        panel_a_metric="imbalance_pct",
        panel_b_metric="delta",
    )

    bid_series = next(
        series for series in option["series"] if series["id"] == "l2view-bid"
    )
    ask_series = next(
        series for series in option["series"] if series["id"] == "l2view-ask"
    )

    assert bid_series["data"][0][1:] == [60.0] * 4
    assert ask_series["data"][0][1:] == [15.0] * 4


def test_disjoint_component_validity_does_not_fabricate_overlap(
    monkeypatch,
    caplog,
):
    import logging

    caplog.set_level(logging.INFO, logger=module.__name__)

    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(range(60)),
            "okx_futures": frozenset(range(60, 120)),
        },
    )

    # There is no fabricated full-market overlap. Each second renders
    # only the exact sum of the markets valid at that particular second.
    assert projection.usable_l2_seconds == 120
    assert projection.unusable_l2_seconds == 0
    assert projection.partial_market_seconds == 120
    assert all(bar.valid_l2 for bar in projection.bars)
    assert all(bar.price is not None for bar in projection.bars)

    assert projection.bars[0].bid == (Decimal("30"),) * 4
    assert projection.bars[0].ask == (Decimal("9"),) * 4
    assert projection.bars[1].bid == (Decimal("40"),) * 4
    assert projection.bars[1].ask == (Decimal("10"),) * 4

    assert "reason=partial_market_coverage_rendered" in caplog.text


def test_sparse_gaps_keep_coarse_candles_using_valid_seconds(
    monkeypatch,
    caplog,
):
    import logging

    caplog.set_level(logging.INFO, logger=module.__name__)

    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(
                offset for offset in range(120) if offset not in {0, 60}
            ),
            "okx_futures": frozenset(range(120)),
        },
    )

    assert projection.usable_l2_seconds == 120
    assert projection.unusable_l2_seconds == 0
    assert projection.partial_market_seconds == 2
    assert len(projection.bars) == 2

    # With the relaxed gap policy, bars containing at least one valid second
    # remain renderable using the OHLC of their valid seconds.
    assert all(bar.valid_l2 for bar in projection.bars)

    assert "valid_l2_bars=2" in caplog.text
    assert "reason=partial_market_coverage_rendered" in caplog.text


def test_finer_timeframe_exposes_valid_seconds_without_filling_gaps(
    monkeypatch,
):
    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(
                offset for offset in range(120) if offset not in {0, 60}
            ),
            "okx_futures": frozenset(range(120)),
        },
        timeframe=1,
    )

    assert len(projection.bars) == 120
    assert sum(bar.valid_l2 for bar in projection.bars) == 120
    assert projection.usable_l2_seconds == 120
    assert projection.unusable_l2_seconds == 0
    assert projection.partial_market_seconds == 2

    # Bybit contributes nothing at offsets 0 and 60. The values are the
    # actual Binance+OKX sums, not forward-filled three-market sums.
    assert projection.bars[0].bid == (Decimal("40"),) * 4
    assert projection.bars[60].bid == (Decimal("40"),) * 4
    assert projection.bars[1].bid == (Decimal("60"),) * 4
    assert projection.bars[61].bid == (Decimal("60"),) * 4

    assert all(bar.price is not None for bar in projection.bars)


def test_preset_log_records_fraction_and_exact_component_hashes(
    monkeypatch,
    caplog,
):
    import json
    import logging

    from l2shock.presets import (
        build_binance_bybit_okx_futures_data_preset,
        component_data_presets,
    )

    caplog.set_level(logging.INFO, logger=module.__name__)

    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(range(120)),
            "okx_futures": frozenset(range(120)),
        },
    )

    preset = build_binance_bybit_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.009"),
    )

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("ANALYSIS L2 PRESET:")
    ]

    assert len(messages) == 1
    message = messages[0]

    assert f"preset_hash={projection.request.preset_hash}" in message
    assert "lower_depth_fraction=0" in message
    assert "upper_depth_fraction=0.009" in message
    assert "expected_markets=3" in message

    component_payload = json.loads(message.split("components=", 1)[1])

    assert {item["component_preset_hash"] for item in component_payload} == {
        component.preset_hash for component in component_data_presets(preset)
    }

    assert {item["venue"] for item in component_payload} == {
        "binance_futures",
        "bybit",
        "okx_futures",
    }


def test_missing_aggregate_keeps_price_candles_and_l2_outage_overlay(
    monkeypatch,
):
    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": None,
            "bybit": None,
            "okx_futures": None,
        },
    )

    option = build_l2_view_chart_options(
        projection,
        panel_a_metric="imbalance_pct",
        panel_b_metric="delta",
    )

    price_series = next(
        series for series in option["series"] if series["id"] == "l2view-price"
    )
    bid_series = next(
        series for series in option["series"] if series["id"] == "l2view-bid"
    )

    assert all(item[1:] == [60000.0] * 4 for item in price_series["data"])
    assert all(item[1:] == [None] * 4 for item in bid_series["data"])

    # Red behind price is an L2 warning, not evidence that price failed.
    assert price_series["markArea"]["data"]
    assert all(
        region[0]["name"] == "l2shock-warning:l2"
        for region in price_series["markArea"]["data"]
    )


def test_viewing_total_and_delta_are_exact_beyond_80_digits():
    from decimal import Context, localcontext

    bid = Decimal("1e100")
    ask = Decimal("1")

    expected_total = Decimal("1" + "0" * 99 + "1")
    expected_delta = Decimal("9" * 100)

    start_epoch = module._epoch(_HOUR)
    builder = module._BarBuilder(
        timeframe_seconds=1,
        start_epoch=start_epoch,
        end_epoch=start_epoch + 1,
    )

    with localcontext(Context(prec=80)):
        builder.add(start_epoch, bid, ask, None)
        bars = builder.finish()

    assert len(bars) == 1
    bar = bars[0]

    assert bar.valid_l2 is True
    assert bar.bid == (bid,) * 4
    assert bar.ask == (ask,) * 4
    assert bar.total == (expected_total,) * 4
    assert bar.delta == (expected_delta,) * 4


@pytest.mark.parametrize(
    ("bid_text", "ask_text"),
    [
        ("1e308", "1e308"),
        ("1e1000", "1e1000"),
        ("1e-1000", "1e-1000"),
    ],
)
def test_percentage_ratio_avoids_intermediate_float_failure(
    bid_text,
    ask_text,
):
    import math

    start_epoch = module._epoch(_HOUR)
    builder = module._BarBuilder(
        timeframe_seconds=1,
        start_epoch=start_epoch,
        end_epoch=start_epoch + 1,
    )

    builder.add(
        start_epoch,
        Decimal(bid_text),
        Decimal(ask_text),
        None,
    )
    bar = builder.finish()[0]

    assert bar.valid_l2 is True
    assert bar.bid_share_pct == (50.0,) * 4
    assert all(math.isfinite(value) for value in bar.bid_share_pct)


def test_exact_pair_arithmetic_ignores_low_ambient_precision():
    from decimal import Context, localcontext

    with localcontext(Context(prec=3)):
        total, delta = module._exact_l2_total_delta(
            Decimal("123456789.123456789"),
            Decimal("123456788.987654321"),
        )

    assert total == Decimal("246913578.111111110")
    assert delta == Decimal("0.135802468")


def test_zero_total_keeps_liquidity_valid_and_percentage_undefined():
    start_epoch = module._epoch(_HOUR)
    builder = module._BarBuilder(
        timeframe_seconds=1,
        start_epoch=start_epoch,
        end_epoch=start_epoch + 1,
    )

    builder.add(
        start_epoch,
        Decimal("0"),
        Decimal("0"),
        None,
    )
    bar = builder.finish()[0]

    assert bar.valid_l2 is True
    assert bar.bid == (Decimal("0"),) * 4
    assert bar.ask == (Decimal("0"),) * 4
    assert bar.total == (Decimal("0"),) * 4
    assert bar.delta == (Decimal("0"),) * 4
    assert bar.bid_share_pct is None


def test_undefined_percentage_second_still_invalidates_coarse_percentage():
    start_epoch = module._epoch(_HOUR)
    builder = module._BarBuilder(
        timeframe_seconds=5,
        start_epoch=start_epoch,
        end_epoch=start_epoch + 5,
    )

    for offset in range(5):
        bid, ask = (
            (Decimal("0"), Decimal("0"))
            if offset == 2
            else (Decimal("2"), Decimal("1"))
        )
        builder.add(start_epoch + offset, bid, ask, None)

    bar = builder.finish()[0]

    assert bar.valid_l2 is True
    assert bar.bid is not None
    assert bar.ask is not None
    assert bar.total is not None
    assert bar.delta is not None
    assert bar.bid_share_pct is None


@pytest.mark.parametrize("timeframe", (1, 5, 15, 60, 300, 900, 3600))
def test_one_contributing_second_remains_renderable_at_every_timeframe(
    monkeypatch,
    timeframe,
):
    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset({17}),
            "bybit": frozenset(),
            "okx_futures": None,
        },
        timeframe=timeframe,
    )

    assert projection.source_seconds == 120
    assert projection.usable_l2_seconds == 1
    assert projection.unusable_l2_seconds == 119
    assert projection.partial_market_seconds == 1

    renderable = [bar for bar in projection.bars if bar.valid_l2]
    assert len(renderable) == 1

    bar = renderable[0]
    assert bar.bid == (Decimal("10"),) * 4
    assert bar.ask == (Decimal("4"),) * 4
    assert bar.total == (Decimal("14"),) * 4
    assert bar.delta == (Decimal("6"),) * 4

    # The quality run is continuous: the one numerical second is still
    # partial-market coverage, so it does not erase the warning.
    assert len(projection.l2_regions) == 1
    assert projection.l2_regions[0].start_utc == _HOUR
    assert projection.l2_regions[0].end_utc_exclusive == (
        _HOUR + timedelta(seconds=120)
    )


def test_all_invalid_verified_components_complete_without_fabricated_l2(
    monkeypatch,
):
    projection = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(),
            "bybit": frozenset(),
            "okx_futures": frozenset(),
        },
    )

    assert projection.usable_l2_seconds == 0
    assert projection.unusable_l2_seconds == 120
    assert projection.partial_market_seconds == 0
    assert projection.price_status == "loaded"

    assert all(not bar.valid_l2 for bar in projection.bars)
    assert all(bar.bid is None for bar in projection.bars)
    assert all(bar.ask is None for bar in projection.bars)
    assert all(bar.price is not None for bar in projection.bars)


def test_partial_market_cached_coarsening_matches_direct_loading(
    monkeypatch,
):
    coverage = {
        "binance_futures": frozenset(range(120)),
        "bybit": frozenset(range(30, 90)),
        "okx_futures": frozenset(range(60, 120)),
    }

    fine = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets=coverage,
        timeframe=1,
    )
    direct = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets=coverage,
        timeframe=60,
    )
    cached = coarsen_l2_view(fine, 60)

    assert cached.bars == direct.bars
    assert cached.l2_regions == direct.l2_regions
    assert cached.usable_l2_seconds == direct.usable_l2_seconds
    assert cached.unusable_l2_seconds == direct.unusable_l2_seconds
    assert cached.partial_market_seconds == direct.partial_market_seconds
    assert cached.input_id == direct.input_id


@pytest.mark.parametrize(
    "unavailable_offsets",
    (
        frozenset(),
        frozenset({1, 7}),
    ),
)
def test_cached_percentage_matches_direct_builder_with_zero_total(
    monkeypatch,
    unavailable_offsets,
):
    from dataclasses import replace

    source = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(range(120)),
            "okx_futures": frozenset(range(120)),
        },
        timeframe=1,
        seconds=15,
    )

    start_epoch = module._epoch(_HOUR)
    fine_builder = module._BarBuilder(1, start_epoch, start_epoch + 15)
    direct_builder = module._BarBuilder(15, start_epoch, start_epoch + 15)

    for offset in range(15):
        if offset in unavailable_offsets:
            bid = ask = None
        elif offset == 3:
            bid = ask = Decimal("0")
        else:
            bid, ask = Decimal("2"), Decimal("1")

        fine_builder.add(start_epoch + offset, bid, ask, None)
        direct_builder.add(start_epoch + offset, bid, ask, None)

    fine_projection = replace(
        source,
        bars=fine_builder.finish(),
        timeframe_seconds=1,
    )
    cached = coarsen_l2_view(fine_projection, 15)
    direct = direct_builder.finish()

    assert cached.bars == direct
    assert cached.bars[0].valid_l2 is True
    assert cached.bars[0].bid is not None
    assert cached.bars[0].bid_share_pct is None


def test_cached_percentage_ignores_only_numerically_unavailable_subbars(
    monkeypatch,
):
    from dataclasses import replace

    source = _diagnostic_aggregate_projection(
        monkeypatch,
        component_valid_offsets={
            "binance_futures": frozenset(range(120)),
            "bybit": frozenset(range(120)),
            "okx_futures": frozenset(range(120)),
        },
        timeframe=1,
        seconds=15,
    )

    start_epoch = module._epoch(_HOUR)
    fine_builder = module._BarBuilder(1, start_epoch, start_epoch + 15)
    direct_builder = module._BarBuilder(15, start_epoch, start_epoch + 15)

    for offset in range(15):
        bid, ask = (None, None) if offset in {1, 7} else (Decimal("2"), Decimal("1"))
        fine_builder.add(start_epoch + offset, bid, ask, None)
        direct_builder.add(start_epoch + offset, bid, ask, None)

    fine_projection = replace(
        source,
        bars=fine_builder.finish(),
        timeframe_seconds=1,
    )
    cached = coarsen_l2_view(fine_projection, 15)

    assert cached.bars == direct_builder.finish()
    assert cached.bars[0].valid_l2 is True
    assert cached.bars[0].bid_share_pct is not None

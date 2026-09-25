"""Regression tests for optional Shock chart price context.

These use verified-repository-shaped test doubles; no database or
browser is required. Price must never determine L2 chart membership.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from l2shock.price import TradeSampleQuality
from l2shock.ui.shock_view_chart_options import (
    ShockViewChartError,
    build_shock_view_chart_options,
)

price_context = importlib.import_module("l2shock.ui.shock_price_context")

UTC = timezone.utc


def _utc(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2025, 1, 1, hour, minute, second, tzinfo=UTC)


def _decoded(
    valid: dict[int, tuple[str, str, str, str]],
) -> SimpleNamespace:
    """Make one codec-decoded hour with invalid, no-trade default slots."""
    quality = [TradeSampleQuality.INVALID] * 3600
    opened = [None] * 3600
    high = [None] * 3600
    low = [None] * 3600
    closed = [None] * 3600

    for offset, (o, h, l, c) in valid.items():
        quality[offset] = TradeSampleQuality.VALID
        opened[offset] = Decimal(o)
        high[offset] = Decimal(h)
        low[offset] = Decimal(l)
        closed[offset] = Decimal(c)

    return SimpleNamespace(
        quality=tuple(quality),
        open=tuple(opened),
        high=tuple(high),
        low=tuple(low),
        close=tuple(closed),
    )


class _Reader:
    def __init__(self, hours: tuple[SimpleNamespace, ...]) -> None:
        self.hours = hours
        self.calls: list[dict[str, object]] = []

    def list_price_hours(self, **kwargs: object) -> tuple[SimpleNamespace, ...]:
        self.calls.append(kwargs)
        return self.hours


def _hour(
    hour_utc: datetime,
    encoded: str,
    *,
    base: str = "BTC",
    venue: str = "binance_futures",
) -> SimpleNamespace:
    return SimpleNamespace(
        hour_utc=hour_utc,
        encoded=encoded,
        base=base,
        source_venue=venue,
    )


def test_price_aggregates_valid_seconds_across_hours_without_filling_gaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = {
        "first": _decoded(
            {
                3598: ("100", "103", "99", "102"),
                # Second 3599 has no trades.
            }
        ),
        "second": _decoded(
            {
                0: ("104", "105", "101", "102"),
                1: ("102", "110", "100", "109"),
            }
        ),
    }
    monkeypatch.setattr(
        price_context,
        "decode_hourly_trade_ohlc_blocks",
        lambda value: encoded[value],
    )

    reader = _Reader(
        (
            _hour(_utc(0, 0), "first"),
            _hour(_utc(1, 0), "second"),
        )
    )

    candles = price_context.price_candles_from_repository(
        reader,
        base="BTC",
        source_start_utc=_utc(0, 59, 58),
        source_end_utc_exclusive=_utc(1, 0, 2),
        bar_starts_utc=(_utc(0, 59, 58), _utc(1, 0)),
        timeframe_seconds=2,
    )

    # ECharts order is [open, close, low, high].
    assert candles == (
        [100.0, 102.0, 99.0, 103.0],
        [104.0, 109.0, 100.0, 110.0],
    )
    assert reader.calls == [
        {
            "base": "BTC",
            "start_utc": _utc(0, 0),
            "end_utc": _utc(2, 0),
            "verify_codec": True,
        }
    ]


def test_partial_first_bar_excludes_price_before_l2_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = {
        "first": _decoded(
            {
                3598: ("10", "11", "9", "10"),
                3599: ("20", "22", "19", "21"),
            }
        ),
        "second": _decoded({}),
    }
    monkeypatch.setattr(
        price_context,
        "decode_hourly_trade_ohlc_blocks",
        lambda value: encoded[value],
    )

    reader = _Reader(
        (
            _hour(_utc(0, 0), "first"),
            _hour(_utc(1, 0), "second"),
        )
    )

    candles = price_context.price_candles_from_repository(
        reader,
        base="BTC",
        source_start_utc=_utc(0, 59, 59),
        source_end_utc_exclusive=_utc(1, 0, 2),
        bar_starts_utc=(_utc(0, 59, 58), _utc(1, 0)),
        timeframe_seconds=2,
    )

    assert candles == ([20.0, 21.0, 19.0, 22.0], None)


def test_missing_price_hour_is_a_null_bar_not_a_shorter_axis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        price_context,
        "decode_hourly_trade_ohlc_blocks",
        lambda _encoded: _decoded({3598: ("100", "102", "99", "101")}),
    )
    reader = _Reader((_hour(_utc(0, 0), "first"),))

    candles = price_context.price_candles_from_repository(
        reader,
        base="BTC",
        source_start_utc=_utc(0, 59, 58),
        source_end_utc_exclusive=_utc(1, 0, 2),
        bar_starts_utc=(_utc(0, 59, 58), _utc(1, 0)),
        timeframe_seconds=2,
    )

    assert candles == ([100.0, 101.0, 99.0, 102.0], None)
    assert len(candles) == 2


@pytest.mark.parametrize(
    ("hours", "message"),
    [
        (
            (_hour(_utc(0, 0), "x", base="ETH"),),
            "another base",
        ),
        (
            (_hour(_utc(0, 0), "x", venue="other"),),
            "non-Binance",
        ),
        (
            (_hour(_utc(0, 0), "x"), _hour(_utc(0, 0), "x")),
            "duplicate hour",
        ),
        (
            (_hour(_utc(2, 0), "x"),),
            "out-of-range hour",
        ),
    ],
)
def test_repository_identity_or_range_error_is_not_plotted(
    hours: tuple[SimpleNamespace, ...],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Mock decode so the "duplicate hour" case can reach the
    # duplicate-check before the first hour's decode fails.
    # The other cases (ETH base, non-Binance venue, out-of-range)
    # all raise before reaching decode, so this mock is harmless.
    monkeypatch.setattr(
        price_context,
        "decode_hourly_trade_ohlc_blocks",
        lambda _encoded: SimpleNamespace(),
    )
    with pytest.raises(ValueError, match=message):
        price_context.price_candles_from_repository(
            _Reader(hours),
            base="BTC",
            source_start_utc=_utc(0, 0),
            source_end_utc_exclusive=_utc(0, 0, 1),
            bar_starts_utc=(_utc(0, 0),),
            timeframe_seconds=1,
        )


def _projection() -> SimpleNamespace:
    """Two five-second L2 bars, independent of price availability."""
    first = SimpleNamespace(
        first_dataset_index=0,
        last_dataset_index=4,
        start_utc=_utc(0, 0),
        bid=(1.0, 2.0, 1.0, 2.0),
        ask=(3.0, 4.0, 3.0, 4.0),
        total=(4.0, 6.0, 4.0, 6.0),
        delta=(-2.0, -2.0, -2.0, -2.0),
    )
    second = SimpleNamespace(
        first_dataset_index=5,
        last_dataset_index=9,
        start_utc=_utc(0, 0, 5),
        bid=None,
        ask=None,
        total=None,
        delta=None,
    )
    return SimpleNamespace(
        bars=(first, second),
        source_start_utc=_utc(0, 0),
        source_end_utc_exclusive=_utc(0, 0, 10),
        timeframe_seconds=5,
    )


def _view_options(
    *,
    price_candles: tuple[list[float] | None, ...] | None = None,
) -> dict:
    return build_shock_view_chart_options(
        _projection(),
        b_first_dataset_index=0,
        b_last_dataset_index=1,
        representative_b_dataset_index=0,
        representative_c_dataset_index=5,
        price_candles=price_candles,
    )


def test_view_price_null_keeps_timestamp_l2_b_band_and_source_bounds() -> None:
    no_price = _view_options()
    with_price = _view_options(price_candles=([100.0, 102.0, 99.0, 103.0], None))

    assert no_price["series"][0]["data"] == []
    assert with_price["series"][0]["data"] == [
        [_utc(0, 0).isoformat(), 100.0, 102.0, 99.0, 103.0],
        [_utc(0, 0, 5).isoformat(), None, None, None, None],
    ]

    # Price only changes the first series. The remaining four L2
    # series, B interval, and time-axis limits must remain unchanged.
    assert with_price["series"][1:] == no_price["series"][1:]
    assert with_price["series"][0]["markArea"] == no_price["series"][0]["markArea"]
    assert with_price["xAxis"] == no_price["xAxis"]


@pytest.mark.parametrize(
    "candles",
    [
        (),  # A missing bar must be represented by None, not removed.
        ([100.0, 101.0, 99.0, 102.0],),
        ([100.0, 101.0, 99.0, 102.0], [1.0, 2.0, 3.0]),
        ([100.0, 101.0, 99.0, 102.0], [1.0, 2.0, 3.0, float("nan")]),
    ],
)
def test_view_rejects_price_that_cannot_align_with_l2(
    candles: tuple,
) -> None:
    with pytest.raises(ShockViewChartError):
        _view_options(price_candles=candles)

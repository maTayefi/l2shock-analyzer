from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

import pytest

from l2shock.analysis.l2_view_metrics import (
    L2ViewMetric,
    compute_l2_view_metric,
)
from l2shock.analysis.l2_view_stream import (
    _BarBuilder,
    _merge_bars,
)


def _bars(
    observations: tuple[tuple[Decimal | None, Decimal | None], ...],
    *,
    timeframe: int = 1,
):
    builder = _BarBuilder(timeframe, 0, len(observations))

    for epoch, (bid, ask) in enumerate(observations):
        builder.add(epoch, bid, ask, None)

    return builder.finish()


def test_near_balance_preserves_representable_signed_imbalance() -> None:
    bid = Decimal("10000000000000001")
    ask = Decimal("10000000000000000")
    bars = _bars(((bid, ask),))

    expected = float(
        100 * (Fraction(bid) - Fraction(ask)) / (Fraction(bid) + Fraction(ask))
    )
    actual = compute_l2_view_metric(
        bars,
        L2ViewMetric.IMBALANCE_PCT,
    )[0]

    assert expected > 0
    assert actual is not None

    for coordinate in actual:
        assert coordinate > 0
        assert coordinate == pytest.approx(
            expected,
            rel=1e-15,
            abs=0.0,
        )


def test_small_positive_ask_share_survives_complement_calculation() -> None:
    bid = Decimal("100000000000000000000")
    ask = Decimal("1")
    bars = _bars(((bid, ask),))

    expected = float(100 * Fraction(ask) / (Fraction(bid) + Fraction(ask)))
    candle = compute_l2_view_metric(
        bars,
        L2ViewMetric.ASK_SHARE_PCT,
    )[0]
    lines = compute_l2_view_metric(
        bars,
        L2ViewMetric.BID_ASK_SHARES_PCT,
    )[0]

    assert expected > 0
    assert candle is not None
    assert lines is not None

    for coordinate in candle:
        assert coordinate > 0
        assert coordinate == pytest.approx(
            expected,
            rel=1e-15,
            abs=0.0,
        )

    assert lines[1] > 0
    assert lines[1] == pytest.approx(
        expected,
        rel=1e-15,
        abs=0.0,
    )


def test_imbalance_change_is_computed_before_float_rounding() -> None:
    observations = (
        (
            Decimal("10000000000000000"),
            Decimal("10000000000000000"),
        ),
        (
            Decimal("10000000000000001"),
            Decimal("10000000000000000"),
        ),
    )
    bars = _bars(observations)
    values = compute_l2_view_metric(
        bars,
        L2ViewMetric.IMBALANCE_CHANGE_PP,
    )

    expected = float(Fraction(100, 20000000000000001))

    assert values[0] is None
    assert values[1] is not None
    assert values[1] > 0
    assert values[1] == pytest.approx(
        expected,
        rel=1e-15,
        abs=0.0,
    )


def test_exact_percentage_ohlc_matches_cached_bar_merging() -> None:
    observations = (
        (Decimal("10000000000000001"), Decimal("10000000000000000")),
        (Decimal("3"), Decimal("7")),
        (Decimal("9"), Decimal("1")),
        (Decimal("1"), Decimal("100000000000000000000")),
        (Decimal("2"), Decimal("3")),
    )
    fine = _bars(observations)
    direct = _bars(observations, timeframe=5)[0]
    merged = _merge_bars(0, fine)

    assert merged == direct
    assert direct.bid_share_pct is not None
    assert all(isinstance(value, Fraction) for value in direct.bid_share_pct)

    for metric in (
        L2ViewMetric.BID_SHARE_PCT,
        L2ViewMetric.ASK_SHARE_PCT,
        L2ViewMetric.IMBALANCE_PCT,
        L2ViewMetric.BID_ASK_SHARES_PCT,
    ):
        assert compute_l2_view_metric((merged,), metric) == (
            compute_l2_view_metric((direct,), metric)
        )


def test_zero_total_still_invalidates_percentage_for_whole_bar() -> None:
    observations = (
        (Decimal("2"), Decimal("1")),
        (Decimal("0"), Decimal("0")),
        (Decimal("3"), Decimal("1")),
        (None, None),
        (Decimal("4"), Decimal("2")),
    )
    fine = _bars(observations)
    direct = _bars(observations, timeframe=5)[0]
    merged = _merge_bars(0, fine)

    assert direct.valid_l2 is True
    assert direct.bid_share_pct is None
    assert merged.bid_share_pct is None

    for metric in (
        L2ViewMetric.BID_SHARE_PCT,
        L2ViewMetric.ASK_SHARE_PCT,
        L2ViewMetric.IMBALANCE_PCT,
        L2ViewMetric.BID_ASK_SHARES_PCT,
    ):
        assert compute_l2_view_metric((direct,), metric) == (None,)
        assert compute_l2_view_metric((merged,), metric) == (None,)


def test_missing_second_does_not_invent_or_erase_percentage() -> None:
    bars = _bars(
        (
            (Decimal("2"), Decimal("1")),
            (None, None),
            (Decimal("3"), Decimal("1")),
            (None, None),
            (Decimal("4"), Decimal("2")),
        ),
        timeframe=5,
    )

    assert bars[0].valid_l2 is True
    assert bars[0].bid_share_pct is not None
    assert (
        compute_l2_view_metric(
            bars,
            L2ViewMetric.IMBALANCE_PCT,
        )[0]
        is not None
    )

# tests/test_l2_ratio_extremeness.py
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from fractions import Fraction

from l2shock.analysis.l2_ratio_extremeness import (
    RATIO_EXTREMENESS_ALGORITHM_VERSION,
    RATIO_EXTREMENESS_MIN_POPULATION,
    RatioExtremenessSide,
    RatioExtremenessStatus,
    RatioScaleMethod,
    compute_ratio_extremeness,
)
from l2shock.analysis.l2_view_stream import L2ViewBar

_T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)


def _bar(index: int, high: object, low: object) -> L2ViewBar:
    if high is None:
        share = None
    else:
        h = Fraction(high)
        lo = Fraction(low)
        middle = (h + lo) / 2
        share = (middle, h, lo, middle)

    return L2ViewBar(
        start_utc=_T0 + timedelta(minutes=index),
        source_seconds=60,
        valid_l2=share is not None,
        bid=None,
        ask=None,
        total=None,
        delta=None,
        bid_share_pct=share,
        price=None,
    )


def _bars(pairs) -> tuple[L2ViewBar, ...]:
    return tuple(_bar(index, high, low) for index, (high, low) in enumerate(pairs))


def _baseline(count: int = 40) -> list[tuple[Fraction, Fraction]]:
    return [
        (Fraction(50) + Fraction(i % 7, 10), Fraction(50) - Fraction(i % 5, 10))
        for i in range(count)
    ]


def _symmetric(count: int = 40) -> list[tuple[Fraction, Fraction]]:
    return [
        (Fraction(50) + Fraction(i % 7, 10), Fraction(50) - Fraction(i % 7, 10))
        for i in range(count)
    ]


def _scores(result) -> list[float | None]:
    return [None if entry is None else entry.score for entry in result.bars]


def test_highest_high_is_bid_dominant_with_full_tail_fraction() -> None:
    result = compute_ratio_extremeness(
        _bars(_baseline() + [(Fraction(60), Fraction(50))])
    )
    entry = result.bars[-1]

    assert result.status is RatioExtremenessStatus.SCORED
    assert result.algorithm_version == RATIO_EXTREMENESS_ALGORITHM_VERSION
    assert entry is not None
    assert entry.high_percentile == 1.0
    assert entry.side is RatioExtremenessSide.BID_DOMINANT
    assert entry.low_score == 0.0
    assert entry.score == max(_s for _s in _scores(result) if _s is not None)


def test_lowest_low_is_ask_dominant() -> None:
    result = compute_ratio_extremeness(
        _bars(_baseline() + [(Fraction(50), Fraction(40))])
    )
    entry = result.bars[-1]

    assert entry is not None
    assert entry.low_percentile == 1.0
    assert entry.side is RatioExtremenessSide.ASK_DOMINANT
    assert entry.score == entry.low_score


def test_high_side_depends_only_on_high_distribution() -> None:
    first = _baseline()
    second = [(high, low - Fraction(i % 3)) for i, (high, low) in enumerate(first)]

    a = compute_ratio_extremeness(_bars(first))
    b = compute_ratio_extremeness(_bars(second))

    assert [e.high_score for e in a.bars] == [e.high_score for e in b.bars]
    assert [e.high_percentile for e in a.bars] == [e.high_percentile for e in b.bars]


def test_ties_share_midpoint_tail_fraction() -> None:
    pairs = [(Fraction(50), Fraction(49))] * 25
    result = compute_ratio_extremeness(_bars(pairs))

    assert all(e.high_percentile == 0.5 for e in result.bars)
    assert all(e.low_percentile == 0.5 for e in result.bars)


def test_constant_distribution_has_no_false_extremes() -> None:
    result = compute_ratio_extremeness(_bars([(Fraction(50), Fraction(50))] * 30))

    assert result.high_distribution.scale_method is RatioScaleMethod.ZERO
    assert all(e.score == 0.0 and e.side is None for e in result.bars)


def test_zero_mad_uses_mean_absolute_deviation_fallback() -> None:
    pairs = [(Fraction(50), Fraction(49))] * 30
    pairs += [(Fraction(51), Fraction(49)), (Fraction(52), Fraction(49))]
    result = compute_ratio_extremeness(_bars(pairs))

    assert result.high_distribution.scale_method is (
        RatioScaleMethod.MEAN_ABSOLUTE_DEVIATION
    )
    s51, s52 = result.bars[-2].score, result.bars[-1].score
    assert 0.0 < s51 < s52 < 100.0
    assert all(e.score == 0.0 for e in result.bars[:30])


def test_null_bars_are_excluded_and_unscored() -> None:
    pairs = _baseline()
    reference = _scores(compute_ratio_extremeness(_bars(pairs)))

    mixed = []
    for pair in pairs:
        mixed.append(pair)
        mixed.append((None, None))

    result = compute_ratio_extremeness(_bars(mixed))

    assert result.eligible_bar_count == len(pairs)
    assert _scores(result)[1::2] == [None] * len(pairs)
    assert _scores(result)[0::2] == reference


def test_insufficient_population_scores_nothing() -> None:
    count = RATIO_EXTREMENESS_MIN_POPULATION - 1
    result = compute_ratio_extremeness(_bars(_baseline(count)))

    assert result.status is RatioExtremenessStatus.INSUFFICIENT_POPULATION
    assert result.eligible_bar_count == count
    assert result.bars == (None,) * count


def test_scores_are_bounded_and_final_is_max() -> None:
    rng = random.Random(20261005)
    pairs = []
    for _ in range(500):
        high = Fraction(rng.randint(0, 10_000), 100)
        low = max(Fraction(0), high - Fraction(rng.randint(0, 800), 100))
        pairs.append((high, low))

    for entry in compute_ratio_extremeness(_bars(pairs)).bars:
        assert 0.0 <= entry.score <= 100.0
        assert 0.0 <= entry.high_score <= 100.0
        assert 0.0 <= entry.low_score <= 100.0
        assert entry.score == max(entry.high_score, entry.low_score)
        assert (entry.side is None) == (entry.score == 0.0)


def test_exact_symmetric_tie_is_both() -> None:
    pairs = _symmetric() + [(Fraction(55), Fraction(45))]
    entry = compute_ratio_extremeness(_bars(pairs)).bars[-1]

    assert entry.high_score == entry.low_score > 0.0
    assert entry.side is RatioExtremenessSide.BOTH


def test_score_is_assigned_to_same_bar_without_shift() -> None:
    for position in (0, 7, 20, 40):
        pairs = _baseline()
        pairs.insert(position, (Fraction(70), Fraction(50)))
        scores = _scores(compute_ratio_extremeness(_bars(pairs)))

        assert scores.index(max(scores)) == position


def test_bar_order_permutation_does_not_change_value_scores() -> None:
    pairs = _baseline() + [(Fraction(65), Fraction(41))]
    shuffled = list(pairs)
    random.Random(7).shuffle(shuffled)

    def keyed(sequence):
        result = compute_ratio_extremeness(_bars(sequence))
        return sorted(
            (high, low, entry.score)
            for (high, low), entry in zip(sequence, result.bars, strict=True)
        )

    assert keyed(pairs) == keyed(shuffled)


def test_tail_resolution_is_not_saturated() -> None:
    ordinary = _scores(compute_ratio_extremeness(_bars(_baseline())))
    moderate = compute_ratio_extremeness(
        _bars(_baseline() + [(Fraction(60), Fraction(50))])
    ).bars[-1]
    severe = compute_ratio_extremeness(
        _bars(_baseline() + [(Fraction(90), Fraction(50))])
    ).bars[-1]

    assert max(ordinary) < 60.0
    assert moderate.score < severe.score < 100.0


def test_adding_extreme_bar_changes_reference_population() -> None:
    base = compute_ratio_extremeness(_bars(_baseline()))
    extended = compute_ratio_extremeness(
        _bars(_baseline() + [(Fraction(80), Fraction(50))])
    )

    assert extended.eligible_bar_count == base.eligible_bar_count + 1
    assert extended.bars[0].high_percentile != base.bars[0].high_percentile


def _projection(bars):
    from types import SimpleNamespace

    from l2shock.analysis.l2_view_stream import L2ViewProjection

    end = bars[-1].start_utc + timedelta(minutes=1)
    return L2ViewProjection(
        request=SimpleNamespace(
            base="BTC",
            preset_hash="a" * 64,
            requested_start_utc=bars[0].start_utc,
            requested_end_utc=end,
        ),
        start_utc=bars[0].start_utc,
        end_utc_exclusive=end,
        timeframe_seconds=60,
        max_bars=5_000,
        bars=bars,
        l2_regions=(),
        price_regions=(),
        l2_regions_truncated=False,
        price_regions_truncated=False,
        price_status="missing",
        usable_l2_seconds=60 * len(bars),
        unusable_l2_seconds=0,
        partial_market_seconds=0,
        input_id="b" * 64,
    )


def _extreme_projection(position: int = 12):
    pairs = _baseline()
    pairs.insert(position, (Fraction(75), Fraction(50)))
    return _projection(_bars(pairs))


def _helper_series(option):
    from l2shock.ui.l2_view_chart_options import RATIO_EXTREMENESS_SERIES_SUFFIX

    return [
        s for s in option["series"] if s["id"].endswith(RATIO_EXTREMENESS_SERIES_SUFFIX)
    ]


def test_chart_background_and_tooltip_data_on_ratio_panel_only() -> None:
    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

    projection = _extreme_projection(12)
    option = build_l2_view_chart_options(
        projection, panel_a_metric="imbalance_pct", panel_b_metric="delta"
    )
    helpers = _helper_series(option)

    assert [s["xAxisIndex"] for s in helpers] == [3]
    helper = helpers[0]
    assert len(helper["data"]) == len(projection.bars)
    scores = [item[2] for item in helper["data"]]
    assert scores.index(max(scores)) == 12
    assert helper["data"][12][3] == 1
    assert all(item[1] is None for item in helper["data"])
    assert helper["markArea"]["data"]
    assert option["l2shockChartMetadata"]["ratio_extremeness_algorithm_version"] == 1


def test_chart_toggle_hides_background_but_keeps_scores() -> None:
    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

    option = build_l2_view_chart_options(
        _extreme_projection(),
        panel_a_metric="ask_share_pct",
        panel_b_metric="bid_share_pct",
        show_ratio_extremeness=False,
    )
    helpers = _helper_series(option)

    assert [s["xAxisIndex"] for s in helpers] == [3, 4]
    assert all("markArea" not in s for s in helpers)
    assert helpers[0]["data"] == helpers[1]["data"]


def test_non_ratio_panels_have_no_extremeness_series() -> None:
    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

    option = build_l2_view_chart_options(
        _extreme_projection(),
        panel_a_metric="bid_ask_shares_pct",
        panel_b_metric="total",
    )

    assert _helper_series(option) == []


def test_tooltip_formatter_renders_extremeness_row() -> None:
    from l2shock.ui.display_timezone import display_timezone_formatters

    tooltip = display_timezone_formatters("UTC")["tooltip"]

    assert "-extremeness$" in tooltip
    assert "Extremeness" in tooltip


def test_json_export_contains_scores_and_excludes_helper_series() -> None:
    import json

    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options
    from l2shock.ui.l2_view_presentation import displayed_json_bytes

    projection = _extreme_projection(12)
    option = build_l2_view_chart_options(
        projection, panel_a_metric="imbalance_pct", panel_b_metric="delta"
    )
    payload = json.loads(
        displayed_json_bytes(
            projection,
            option,
            panel_a_metric="imbalance_pct",
            panel_b_metric="delta",
        )
    )

    assert payload["ratio_extremeness_algorithm_version"] == 1
    assert payload["ratio_extremeness"]["status"] == "scored"
    assert payload["bars"][12]["ratio_extremeness_side"] == "BID_DOMINANT"
    scores = [bar["ratio_extremeness_score"] for bar in payload["bars"]]
    assert scores.index(max(scores)) == 12
    for panel in payload["panels"]:
        assert not any(s["id"].endswith("-extremeness") for s in panel["series"])


def test_csv_export_has_one_extremeness_row_per_bar() -> None:
    import csv
    import io

    from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options
    from l2shock.ui.l2_view_presentation import displayed_csv_bytes

    projection = _extreme_projection(5)
    option = build_l2_view_chart_options(
        projection, panel_a_metric="bid_share_pct", panel_b_metric="delta"
    )
    rows = list(
        csv.reader(
            io.StringIO(displayed_csv_bytes(projection, option).decode("utf-8-sig"))
        )
    )
    header, body = rows[0], rows[1:]

    assert header[-1] == "extremeness_algorithm_version"
    assert all(len(row) == len(header) for row in body)
    extreme_rows = [row for row in body if row[8] == "ratio_extremeness"]
    assert len(extreme_rows) == len(projection.bars)
    assert extreme_rows[5][header.index("extremeness_side")] == "BID_DOMINANT"
    assert not any(row[5].endswith("-extremeness") for row in body)

# tests/test_robust_stats.py
from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

import pytest

from l2shock.analysis.robust_stats import (
    RobustStatsError,
    midpoint_percentile_ranks,
    percentile_ranks,
    positive_tail_modified_z_scores,
)


def test_decimal_midpoint_percentiles() -> None:
    values = [Decimal(1), Decimal(2), Decimal(2), Decimal(3)]
    assert percentile_ranks(values) == (
        Decimal(0),
        Decimal("0.5"),
        Decimal("0.5"),
        Decimal(1),
    )


def test_singleton_and_empty() -> None:
    assert percentile_ranks([Decimal(7)]) == (Decimal(1),)
    assert percentile_ranks([]) == ()
    assert midpoint_percentile_ranks([5]) == (Fraction(1),)
    assert midpoint_percentile_ranks([]) == ()


def test_non_finite_is_rejected() -> None:
    with pytest.raises(RobustStatsError):
        percentile_ranks([Decimal("NaN")])


def test_modified_z_positive_tail() -> None:
    values = [Decimal(v) for v in (1, 2, 3, 4, 100)]
    scores = positive_tail_modified_z_scores(values)

    assert scores[0] == (Decimal(0), Decimal(0))
    assert scores[-1][0] > Decimal(60)
    assert Decimal(0) < scores[-1][1] < Decimal(1)


def test_modified_z_mad_zero() -> None:
    values = [Decimal(v) for v in (5, 5, 5, 9)]
    assert positive_tail_modified_z_scores(values) == (
        (Decimal(0), Decimal(0)),
        (Decimal(0), Decimal(0)),
        (Decimal(0), Decimal(0)),
        (Decimal(0), Decimal(1)),
    )


def test_fraction_percentiles_are_exact() -> None:
    assert midpoint_percentile_ranks([Fraction(1, 3), Fraction(1, 3), 1]) == (
        Fraction(1, 4),
        Fraction(1, 4),
        Fraction(1),
    )


def test_fraction_percentiles_reject_bool_and_float() -> None:
    with pytest.raises(RobustStatsError):
        midpoint_percentile_ranks([True, 1])

    with pytest.raises(RobustStatsError):
        midpoint_percentile_ranks([0.5, 1])  # type: ignore[list-item]
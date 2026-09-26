# l2shock/analysis/robust_stats.py
"""Pure deterministic robust statistics shared by analysis modules.

``percentile_ranks`` and ``positive_tail_modified_z_scores`` were salvaged
from the retired LM ranking module before its deletion. Their Decimal
semantics are preserved exactly (midpoint ties, singleton percentile 1,
MAD=0 behaviour, ROUND_HALF_EVEN in an explicit local context).

``midpoint_percentile_ranks`` is the exact-rational equivalent used by the
Shock-Start review. It never converts to float.

This module must not import detection, ranking, price-filter, or UI code.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from fractions import Fraction
from typing import Final

DEFAULT_DECIMAL_PRECISION: Final[int] = 34
MODIFIED_Z_SCALE: Final[Decimal] = Decimal("0.6744897501960817")

_ZERO: Final[Decimal] = Decimal(0)
_ONE: Final[Decimal] = Decimal(1)
_TWO: Final[Decimal] = Decimal(2)


class RobustStatsError(ValueError):
    """Robust-statistics input violates its contract."""


def _finite_decimal(field_name: str, value: object) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise RobustStatsError(f"{field_name} must be a finite Decimal")

    return value


def _precision(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 16:
        raise RobustStatsError("decimal_precision must be an integer of at least 16")

    return value


def median_decimal(
    values: Sequence[Decimal],
    *,
    decimal_precision: int = DEFAULT_DECIMAL_PRECISION,
) -> Decimal:
    """Median with an explicit, deterministic Decimal context."""
    if not values:
        raise RobustStatsError("Median requires at least one value")

    precision = _precision(decimal_precision)
    ordered = sorted(values)
    middle = len(ordered) // 2

    if len(ordered) % 2:
        return ordered[middle]

    with localcontext(Context(prec=precision, rounding=ROUND_HALF_EVEN)):
        return (ordered[middle - 1] + ordered[middle]) / _TWO


def percentile_ranks(
    values: Sequence[Decimal],
    *,
    decimal_precision: int = DEFAULT_DECIMAL_PRECISION,
) -> tuple[Decimal, ...]:
    """Deterministic midpoint-tie percentiles in input order.

    Non-singleton: (count_less + (count_equal - 1) / 2) / (n - 1).
    A singleton population receives percentile 1.
    """
    normalized = tuple(
        _finite_decimal(f"values[{index}]", value) for index, value in enumerate(values)
    )

    if not normalized:
        return ()

    precision = _precision(decimal_precision)

    if len(normalized) == 1:
        return (_ONE,)

    ordered = sorted(normalized)
    denominator = Decimal(len(normalized) - 1)
    percentile_by_value: dict[Decimal, Decimal] = {}
    start = 0

    with localcontext(Context(prec=precision, rounding=ROUND_HALF_EVEN)):
        while start < len(ordered):
            value = ordered[start]
            stop = start + 1

            while stop < len(ordered) and ordered[stop] == value:
                stop += 1

            midpoint_rank = Decimal(start) + Decimal(stop - start - 1) / _TWO
            percentile_by_value[value] = midpoint_rank / denominator
            start = stop

    return tuple(percentile_by_value[value] for value in normalized)


def positive_tail_modified_z_scores(
    values: Sequence[Decimal],
    *,
    decimal_precision: int = DEFAULT_DECIMAL_PRECISION,
) -> tuple[tuple[Decimal, Decimal], ...]:
    """Return ``(positive_z, positive_z / (1 + positive_z))`` in input order.

    If MAD is zero, values above the median receive normalized evidence 1 and
    values at or below it receive 0; raw positive Z stays 0 (undefined).
    """
    normalized = tuple(
        _finite_decimal(f"values[{index}]", value) for index, value in enumerate(values)
    )

    if not normalized:
        return ()

    precision = _precision(decimal_precision)
    result: list[tuple[Decimal, Decimal]] = []

    with localcontext(Context(prec=precision, rounding=ROUND_HALF_EVEN)):
        median = median_decimal(normalized, decimal_precision=precision)
        deviations = tuple(abs(value - median) for value in normalized)
        mad = median_decimal(deviations, decimal_precision=precision)

        if mad == 0:
            return tuple(
                (_ZERO, _ONE if value > median else _ZERO) for value in normalized
            )

        for value in normalized:
            modified_z = MODIFIED_Z_SCALE * (value - median) / mad
            positive_z = max(_ZERO, modified_z)
            result.append((positive_z, positive_z / (_ONE + positive_z)))

    return tuple(result)


def midpoint_percentile_ranks(
    values: Sequence[Fraction | int],
) -> tuple[Fraction, ...]:
    """Exact rational midpoint-tie percentiles in input order.

    Same definition as ``percentile_ranks`` but exact: no Decimal context,
    no float. Booleans are rejected.
    """
    normalized: list[Fraction] = []

    for index, value in enumerate(values):
        if isinstance(value, bool) or not isinstance(value, (int, Fraction)):
            raise RobustStatsError(f"values[{index}] must be an int or Fraction")

        normalized.append(Fraction(value))

    if not normalized:
        return ()

    count = len(normalized)

    if count == 1:
        return (Fraction(1),)

    ordered = sorted(normalized)
    percentile_by_value: dict[Fraction, Fraction] = {}
    start = 0

    while start < count:
        value = ordered[start]
        stop = start + 1

        while stop < count and ordered[stop] == value:
            stop += 1

        # (less + (equal - 1) / 2) / (n - 1), kept as one exact fraction.
        percentile_by_value[value] = Fraction(
            2 * start + (stop - start - 1),
            2 * (count - 1),
        )
        start = stop

    return tuple(percentile_by_value[value] for value in normalized)


__all__ = [
    "DEFAULT_DECIMAL_PRECISION",
    "MODIFIED_Z_SCALE",
    "RobustStatsError",
    "median_decimal",
    "midpoint_percentile_ranks",
    "percentile_ranks",
    "positive_tail_modified_z_scores",
]

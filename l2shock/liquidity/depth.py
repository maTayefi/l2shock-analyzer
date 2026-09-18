# l2shock/liquidity/depth.py
"""Exact depth-band liquidity calculation over reconstructed live books.

Locked semantics:

- calculations are permitted only for a sequence-valid NORMAL book;
- bid and ask intervals are independently anchored to their own best prices;
- lower and upper price boundaries are inclusive;
- for approved linear USD-equivalent markets, level notional is
  ``price * quantity``;
- Bid Liquidity and Ask Liquidity are exact Decimal sums;
- Total Liquidity is their exact sum;
- Bid-Ask Imbalance is
  ``(bid_liquidity - ask_liquidity) / total_liquidity``;
- a zero denominator produces no imbalance value;
- user chart price filters never affect this calculation.

This module does not emit one-second analytical blocks and does not persist
anything to PostgreSQL.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from decimal import (
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    InvalidOperation,
    localcontext,
)
from typing import Final, TypeAlias

from l2shock.ingest.parquet_reader import BookSide
from l2shock.ingest.replay import (
    OrderBookReplayState,
    ReplayBookStructure,
)

DEFAULT_CANCELLATION_CHECK_INTERVAL_LEVELS: Final[int] = 1_024
DEFAULT_IMBALANCE_DECIMAL_PRECISION: Final[int] = 34

DepthCancellationProbe: TypeAlias = Callable[[], bool]


class DepthLiquidityError(ValueError):
    """Base error for invalid depth-band calculation inputs."""


class DepthLiquidityUnavailableError(DepthLiquidityError):
    """The current reconstructed state is not analytically usable."""


class DepthLiquidityCancelledError(RuntimeError):
    """Depth-band level iteration was cooperatively cancelled."""


@dataclass(frozen=True, slots=True)
class DepthBand:
    """Exact fractional distance from each side's current best price."""

    lower_fraction: Decimal
    upper_fraction: Decimal

    def __post_init__(self) -> None:
        for name in ("lower_fraction", "upper_fraction"):
            value = getattr(self, name)

            if not isinstance(value, Decimal):
                raise DepthLiquidityError(f"{name} must be an exact Decimal")

            if not value.is_finite():
                raise DepthLiquidityError(f"{name} must be finite")

            if value < 0:
                raise DepthLiquidityError(f"{name} must be non-negative")

        if self.lower_fraction > self.upper_fraction:
            raise DepthLiquidityError("lower_fraction cannot exceed upper_fraction")

        # Bid lower bound is best_bid * (1 - upper_fraction). Values at or
        # above one would produce a zero or negative absolute bid boundary.
        if self.upper_fraction >= Decimal("1"):
            raise DepthLiquidityError("upper_fraction must be less than 1")


@dataclass(frozen=True, slots=True)
class DepthPriceIntervals:
    """Exact inclusive absolute price intervals for one book observation."""

    bid_lower: Decimal
    bid_upper: Decimal
    ask_lower: Decimal
    ask_upper: Decimal

    def __post_init__(self) -> None:
        values = (
            self.bid_lower,
            self.bid_upper,
            self.ask_lower,
            self.ask_upper,
        )

        if any(
            not isinstance(value, Decimal) or not value.is_finite() or value <= 0
            for value in values
        ):
            raise DepthLiquidityError(
                "Depth interval boundaries must be positive finite Decimals"
            )

        if self.bid_lower > self.bid_upper:
            raise DepthLiquidityError("Bid depth interval is reversed")

        if self.ask_lower > self.ask_upper:
            raise DepthLiquidityError("Ask depth interval is reversed")


@dataclass(frozen=True, slots=True)
class DepthLiquidityResult:
    """Exact liquidity metrics for one reconstructed book state."""

    band: DepthBand
    intervals: DepthPriceIntervals

    best_bid: Decimal
    best_ask: Decimal

    bid_liquidity: Decimal
    ask_liquidity: Decimal
    total_liquidity: Decimal
    bid_ask_imbalance: Decimal | None

    bid_level_count: int
    ask_level_count: int
    levels_examined: int

    imbalance_decimal_precision: int

    def __post_init__(self) -> None:
        for name in (
            "best_bid",
            "best_ask",
            "bid_liquidity",
            "ask_liquidity",
            "total_liquidity",
        ):
            value = getattr(self, name)

            if not isinstance(value, Decimal) or not value.is_finite():
                raise DepthLiquidityError(f"{name} must be a finite Decimal")

        if self.best_bid <= 0 or self.best_ask <= 0:
            raise DepthLiquidityError("Best prices must be positive")

        if self.best_bid >= self.best_ask:
            raise DepthLiquidityError("Depth liquidity requires best_bid < best_ask")

        if self.bid_liquidity < 0 or self.ask_liquidity < 0:
            raise DepthLiquidityError("Side liquidity cannot be negative")

        if self.total_liquidity != _exact_add(
            self.bid_liquidity,
            self.ask_liquidity,
        ):
            raise DepthLiquidityError(
                "Total liquidity does not equal bid plus ask liquidity"
            )

        for name in (
            "bid_level_count",
            "ask_level_count",
            "levels_examined",
            "imbalance_decimal_precision",
        ):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise DepthLiquidityError(f"{name} must be a non-negative integer")

        if self.levels_examined != (self.bid_level_count + self.ask_level_count):
            raise DepthLiquidityError(
                "levels_examined does not match side level counts"
            )

        if self.imbalance_decimal_precision <= 0:
            raise DepthLiquidityError("imbalance_decimal_precision must be positive")

        if self.total_liquidity == 0:
            if self.bid_ask_imbalance is not None:
                raise DepthLiquidityError(
                    "Zero total liquidity requires null imbalance"
                )
        else:
            imbalance = self.bid_ask_imbalance

            if not isinstance(imbalance, Decimal) or not imbalance.is_finite():
                raise DepthLiquidityError(
                    "Nonzero total liquidity requires finite imbalance"
                )

            if imbalance < Decimal("-1") or imbalance > Decimal("1"):
                raise DepthLiquidityError("Bid-Ask Imbalance must be inside [-1, 1]")


def _decimal_coefficient_and_exponent(
    value: Decimal,
    *,
    field_name: str,
) -> tuple[int, int]:
    """Return the exact signed integer coefficient and base-10 exponent."""

    if not isinstance(value, Decimal) or not value.is_finite():
        raise DepthLiquidityError(f"{field_name} must be a finite Decimal")

    parts = value.as_tuple()
    coefficient = 0

    for digit in parts.digits:
        coefficient = coefficient * 10 + digit

    if parts.sign:
        coefficient = -coefficient

    return coefficient, int(parts.exponent)


def _decimal_from_coefficient(
    coefficient: int,
    exponent: int,
) -> Decimal:
    """Construct one exact Decimal without consulting ambient context."""

    if isinstance(coefficient, bool) or not isinstance(coefficient, int):
        raise TypeError("coefficient must be an integer")

    if isinstance(exponent, bool) or not isinstance(exponent, int):
        raise TypeError("exponent must be an integer")

    if coefficient == 0:
        return Decimal(
            (
                0,
                (0,),
                exponent,
            )
        )

    return Decimal(
        (
            int(coefficient < 0),
            tuple(int(character) for character in str(abs(coefficient))),
            exponent,
        )
    )


def _exact_product_parts(
    left: Decimal,
    right: Decimal,
) -> tuple[int, int]:
    """Return exact coefficient/exponent parts for one Decimal product."""

    left_coefficient, left_exponent = _decimal_coefficient_and_exponent(
        left,
        field_name="left multiplication operand",
    )
    right_coefficient, right_exponent = _decimal_coefficient_and_exponent(
        right,
        field_name="right multiplication operand",
    )

    return (
        left_coefficient * right_coefficient,
        left_exponent + right_exponent,
    )


def _exact_multiply(left: Decimal, right: Decimal) -> Decimal:
    """Multiply finite Decimals without rounding or local contexts."""

    coefficient, exponent = _exact_product_parts(
        left,
        right,
    )

    return _decimal_from_coefficient(
        coefficient,
        exponent,
    )


def _exact_add(left: Decimal, right: Decimal) -> Decimal:
    """Add finite Decimals exactly without using ambient context."""

    left_coefficient, left_exponent = _decimal_coefficient_and_exponent(
        left,
        field_name="left addition operand",
    )
    right_coefficient, right_exponent = _decimal_coefficient_and_exponent(
        right,
        field_name="right addition operand",
    )

    common_exponent = min(
        left_exponent,
        right_exponent,
    )
    total_coefficient = left_coefficient * (
        10 ** (left_exponent - common_exponent)
    ) + right_coefficient * (10 ** (right_exponent - common_exponent))

    return _decimal_from_coefficient(
        total_coefficient,
        common_exponent,
    )


def _exact_subtract(left: Decimal, right: Decimal) -> Decimal:
    """Subtract finite Decimals exactly without unary context rounding."""

    left_coefficient, left_exponent = _decimal_coefficient_and_exponent(
        left,
        field_name="left subtraction operand",
    )
    right_coefficient, right_exponent = _decimal_coefficient_and_exponent(
        right,
        field_name="right subtraction operand",
    )

    common_exponent = min(
        left_exponent,
        right_exponent,
    )
    difference_coefficient = left_coefficient * (
        10 ** (left_exponent - common_exponent)
    ) - right_coefficient * (10 ** (right_exponent - common_exponent))

    return _decimal_from_coefficient(
        difference_coefficient,
        common_exponent,
    )


def depth_price_intervals(
    *,
    best_bid: Decimal,
    best_ask: Decimal,
    band: DepthBand,
) -> DepthPriceIntervals:
    """Construct approved inclusive bid and ask depth intervals."""
    for name, value in (
        ("best_bid", best_bid),
        ("best_ask", best_ask),
    ):
        if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
            raise DepthLiquidityError(f"{name} must be a positive finite Decimal")

    if best_bid >= best_ask:
        raise DepthLiquidityUnavailableError(
            "Depth intervals require a normal positive spread"
        )

    one = Decimal("1")

    try:
        bid_lower = _exact_multiply(
            best_bid,
            _exact_subtract(one, band.upper_fraction),
        )
        bid_upper = _exact_multiply(
            best_bid,
            _exact_subtract(one, band.lower_fraction),
        )
        ask_lower = _exact_multiply(
            best_ask,
            _exact_add(one, band.lower_fraction),
        )
        ask_upper = _exact_multiply(
            best_ask,
            _exact_add(one, band.upper_fraction),
        )
    except InvalidOperation as exc:
        raise DepthLiquidityError("Could not construct exact depth intervals") from exc

    return DepthPriceIntervals(
        bid_lower=bid_lower,
        bid_upper=bid_upper,
        ask_lower=ask_lower,
        ask_upper=ask_upper,
    )


def _positive_configuration_integer(
    name: str,
    value: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DepthLiquidityError(f"{name} must be an integer")

    if value <= 0:
        raise DepthLiquidityError(f"{name} must be positive")

    return value


def _check_cancellation(
    cancellation_probe: DepthCancellationProbe | None,
) -> None:
    if cancellation_probe is None:
        return

    try:
        cancelled = cancellation_probe()
    except DepthLiquidityCancelledError:
        raise
    except Exception as exc:
        raise DepthLiquidityError("Depth cancellation probe failed") from exc

    if not isinstance(cancelled, bool):
        raise DepthLiquidityError("Depth cancellation probe must return bool")

    if cancelled:
        raise DepthLiquidityCancelledError(
            "Depth-band liquidity calculation was cancelled"
        )


def _sum_side_liquidity(
    state: OrderBookReplayState,
    *,
    side: BookSide,
    lower_price: Decimal,
    upper_price: Decimal,
    cancellation_probe: DepthCancellationProbe | None,
    cancellation_check_interval_levels: int,
) -> tuple[Decimal, int]:
    total_coefficient = 0
    common_exponent: int | None = None
    level_count = 0

    for level in state.iter_levels(
        side,
        lower_price=lower_price,
        upper_price=upper_price,
    ):
        if level_count % cancellation_check_interval_levels == 0:
            _check_cancellation(cancellation_probe)

        product_coefficient, product_exponent = _exact_product_parts(
            level.price,
            level.quantity,
        )

        if common_exponent is None:
            common_exponent = product_exponent
            total_coefficient = product_coefficient
        elif product_exponent < common_exponent:
            total_coefficient *= 10 ** (common_exponent - product_exponent)
            total_coefficient += product_coefficient
            common_exponent = product_exponent
        else:
            total_coefficient += product_coefficient * (
                10 ** (product_exponent - common_exponent)
            )

        level_count += 1

    _check_cancellation(cancellation_probe)

    liquidity = _decimal_from_coefficient(
        total_coefficient,
        common_exponent if common_exponent is not None else 0,
    )

    return liquidity, level_count


def calculate_depth_liquidity(
    state: OrderBookReplayState,
    band: DepthBand,
    *,
    cancellation_probe: DepthCancellationProbe | None = None,
    cancellation_check_interval_levels: int = (
        DEFAULT_CANCELLATION_CHECK_INTERVAL_LEVELS
    ),
    imbalance_decimal_precision: int = (DEFAULT_IMBALANCE_DECIMAL_PRECISION),
) -> DepthLiquidityResult:
    """Calculate exact linear USD-equivalent depth liquidity.

    The replay state is queried synchronously and must not be mutated by another
    task during this call.
    """
    if not isinstance(state, OrderBookReplayState):
        raise TypeError("state must be an OrderBookReplayState")

    if not isinstance(band, DepthBand):
        raise TypeError("band must be a DepthBand")

    cancellation_interval = _positive_configuration_integer(
        "cancellation_check_interval_levels",
        cancellation_check_interval_levels,
    )
    imbalance_precision = _positive_configuration_integer(
        "imbalance_decimal_precision",
        imbalance_decimal_precision,
    )

    _check_cancellation(cancellation_probe)

    observation = state.observation()

    if not observation.valid:
        raise DepthLiquidityUnavailableError(
            "Depth liquidity requires valid initialized replay state"
        )

    if observation.book_structure is not ReplayBookStructure.NORMAL:
        raise DepthLiquidityUnavailableError(
            "Depth liquidity requires NORMAL book structure; "
            f"observed {observation.book_structure.value}"
        )

    best_bid = observation.best_bid
    best_ask = observation.best_ask

    if best_bid is None or best_ask is None:
        raise DepthLiquidityUnavailableError(
            "Depth liquidity requires both best bid and best ask"
        )

    intervals = depth_price_intervals(
        best_bid=best_bid,
        best_ask=best_ask,
        band=band,
    )

    bid_liquidity, bid_count = _sum_side_liquidity(
        state,
        side=BookSide.BID,
        lower_price=intervals.bid_lower,
        upper_price=intervals.bid_upper,
        cancellation_probe=cancellation_probe,
        cancellation_check_interval_levels=cancellation_interval,
    )

    ask_liquidity, ask_count = _sum_side_liquidity(
        state,
        side=BookSide.ASK,
        lower_price=intervals.ask_lower,
        upper_price=intervals.ask_upper,
        cancellation_probe=cancellation_probe,
        cancellation_check_interval_levels=cancellation_interval,
    )

    total_liquidity = _exact_add(
        bid_liquidity,
        ask_liquidity,
    )

    if total_liquidity == 0:
        imbalance: Decimal | None = None
    else:
        numerator = _exact_subtract(
            bid_liquidity,
            ask_liquidity,
        )

        with localcontext(
            Context(
                prec=imbalance_precision,
                rounding=ROUND_HALF_EVEN,
            )
        ):
            imbalance = numerator / total_liquidity

        if not imbalance.is_finite() or not math.isfinite(float(imbalance)):
            raise DepthLiquidityError("Calculated Bid-Ask Imbalance is non-finite")

    return DepthLiquidityResult(
        band=band,
        intervals=intervals,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_liquidity=bid_liquidity,
        ask_liquidity=ask_liquidity,
        total_liquidity=total_liquidity,
        bid_ask_imbalance=imbalance,
        bid_level_count=bid_count,
        ask_level_count=ask_count,
        levels_examined=bid_count + ask_count,
        imbalance_decimal_precision=imbalance_precision,
    )


__all__ = [
    "DEFAULT_CANCELLATION_CHECK_INTERVAL_LEVELS",
    "DEFAULT_IMBALANCE_DECIMAL_PRECISION",
    "DepthBand",
    "DepthCancellationProbe",
    "DepthLiquidityCancelledError",
    "DepthLiquidityError",
    "DepthLiquidityResult",
    "DepthLiquidityUnavailableError",
    "DepthPriceIntervals",
    "calculate_depth_liquidity",
    "depth_price_intervals",
]

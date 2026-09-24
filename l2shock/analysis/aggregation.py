# l2shock/analysis/aggregation.py
"""Pure fixed-duration aggregation for one-second analytical series.

L2 liquidity is a reconstructed end-state metric. Larger L2 bars therefore
own the final one-second state in their interval, while retaining complete
coverage diagnostics.

Price is real traded OHLC. Larger price bars aggregate the first real Open,
maximum High, minimum Low, final real Close, and total trade count.

Missing one-second observations are never forward-filled. They are represented
as invalid coverage and create a hard discontinuity for later segmentation and
Liquidity Movement detection.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from enum import StrEnum
from collections.abc import Iterable

from l2shock.ingest import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.price import (
    TradeSampleInvalidReason,
    TradeSampleQuality,
)
from l2shock.timeutils import require_aware_utc
from l2shock.analysis.timeframes import (
    Timeframe,
    TimeframeError,
    get_timeframe,
    iter_timeframe_buckets,
)


class AnalysisBarQuality(StrEnum):
    """Analysis-level quality for an aggregated bar."""

    VALID = "VALID"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


@dataclass(frozen=True, slots=True)
class L2Second:
    """One decoded authoritative one-second L2 observation.

    ``coverage_degraded`` is an analysis-layer market-coverage fact.

    It is false for ordinary verified single-market rows. It may be true for
    a numerically usable aggregate observation when at least one, but fewer
    than all, expected markets contributed.

    It does not alter or reinterpret the persisted BookSampleQuality codes.
    """

    timestamp_utc: datetime
    quality: BookSampleQuality
    invalid_reason: BookSampleInvalidReason | None
    bid_liquidity: Decimal | None
    ask_liquidity: Decimal | None
    source_count: int
    coverage_degraded: bool = False

    def __post_init__(self) -> None:
        timestamp = require_aware_utc(
            "timestamp_utc",
            self.timestamp_utc,
        )

        if timestamp.microsecond != 0:
            raise ValueError("L2Second timestamp_utc must be second-aligned")

        quality = BookSampleQuality(self.quality)
        reason = (
            BookSampleInvalidReason(self.invalid_reason)
            if self.invalid_reason is not None
            else None
        )

        if (
            isinstance(self.source_count, bool)
            or not isinstance(self.source_count, int)
            or self.source_count < 0
        ):
            raise ValueError("source_count must be a non-negative integer")

        if not isinstance(self.coverage_degraded, bool):
            raise ValueError("coverage_degraded must be bool")

        if quality is BookSampleQuality.VALID:
            if reason is not None:
                raise ValueError("VALID L2 second cannot have an invalid reason")

            for name, value in (
                ("bid_liquidity", self.bid_liquidity),
                ("ask_liquidity", self.ask_liquidity),
            ):
                if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                    raise ValueError(
                        f"VALID L2 second requires finite non-negative {name}"
                    )

            if self.source_count <= 0:
                raise ValueError("VALID L2 second requires a positive source count")
        else:
            if reason is None:
                raise ValueError("Non-valid L2 second requires an invalid reason")

            if self.bid_liquidity is not None or self.ask_liquidity is not None:
                raise ValueError("Non-valid L2 second cannot contain liquidity values")

            if self.source_count != 0:
                raise ValueError("Non-valid L2 second must have source_count=0")

            if self.coverage_degraded:
                raise ValueError("An invalid L2 second cannot be coverage-degraded")

        object.__setattr__(self, "timestamp_utc", timestamp)
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "invalid_reason", reason)


@dataclass(frozen=True, slots=True)
class PriceSecond:
    """One decoded authoritative one-second real-trade OHLC observation."""

    timestamp_utc: datetime
    quality: TradeSampleQuality
    invalid_reason: TradeSampleInvalidReason | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    trade_count: int

    def __post_init__(self) -> None:
        timestamp = require_aware_utc(
            "timestamp_utc",
            self.timestamp_utc,
        )

        if timestamp.microsecond != 0:
            raise ValueError("PriceSecond timestamp_utc must be second-aligned")

        quality = TradeSampleQuality(self.quality)
        reason = (
            TradeSampleInvalidReason(self.invalid_reason)
            if self.invalid_reason is not None
            else None
        )

        if (
            isinstance(self.trade_count, bool)
            or not isinstance(self.trade_count, int)
            or self.trade_count < 0
        ):
            raise ValueError("trade_count must be a non-negative integer")

        values = (self.open, self.high, self.low, self.close)

        if quality is TradeSampleQuality.VALID:
            if reason is not None:
                raise ValueError("VALID price second cannot have an invalid reason")

            if self.trade_count <= 0:
                raise ValueError("VALID price second requires a positive trade count")

            if any(
                not isinstance(value, Decimal) or not value.is_finite() or value <= 0
                for value in values
            ):
                raise ValueError("VALID price second requires positive finite OHLC")

            assert self.open is not None
            assert self.high is not None
            assert self.low is not None
            assert self.close is not None

            if self.high < self.low:
                raise ValueError("Price high cannot be below low")

            if not (
                self.low <= self.open <= self.high
                and self.low <= self.close <= self.high
            ):
                raise ValueError("Price Open and Close must lie inside [Low, High]")
        else:
            if reason is None:
                raise ValueError("INVALID price second requires an invalid reason")

            if any(value is not None for value in values):
                raise ValueError("INVALID price second cannot contain OHLC")

            if self.trade_count != 0:
                raise ValueError("INVALID price second must have trade_count=0")

        object.__setattr__(self, "timestamp_utc", timestamp)
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "invalid_reason", reason)


def _exact_nonnegative_decimal_sum(
    left: Decimal,
    right: Decimal,
) -> Decimal:
    """Add two finite non-negative Decimals without ambient-context rounding."""
    for name, value in (
        ("left", left),
        ("right", right),
    ):
        if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
            raise ValueError(f"{name} must be a finite non-negative Decimal")

    left_tuple = left.as_tuple()
    right_tuple = right.as_tuple()
    common_exponent = min(
        int(left_tuple.exponent),
        int(right_tuple.exponent),
    )

    def coefficient(value: Decimal) -> int:
        parts = value.as_tuple()
        result = 0

        for digit in parts.digits:
            result = result * 10 + digit

        if parts.sign:
            result = -result

        return result * (10 ** (int(parts.exponent) - common_exponent))

    total_coefficient = coefficient(left) + coefficient(right)

    if total_coefficient == 0:
        return Decimal(0)

    return Decimal(
        (
            int(total_coefficient < 0),
            tuple(int(character) for character in str(abs(total_coefficient))),
            common_exponent,
        )
    )


@dataclass(frozen=True, slots=True)
class L2OHLC:
    """Exact OHLC of observed one-second values for one L2 metric."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    def __post_init__(self) -> None:
        for name in ("open", "high", "low", "close"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"L2OHLC.{name} must be a finite Decimal")

        if (
            self.low > self.high
            or not self.low <= self.open <= self.high
            or not self.low <= self.close <= self.high
        ):
            raise ValueError("L2OHLC values have inconsistent candle geometry")


def _reduce_l2_ohlc(values: Iterable[Decimal]) -> L2OHLC | None:
    """Reduce ordered, usable one-second metric values in a single pass.

    The caller owns timestamp ordering, bar membership, and exclusion of
    invalid or missing seconds. An empty input has no candle. This function
    never interpolates values or converts exact Decimals to floats.
    """
    first: Decimal | None = None
    lowest: Decimal | None = None
    highest: Decimal | None = None
    last: Decimal | None = None

    for value in values:
        if not isinstance(value, Decimal) or not value.is_finite():
            raise ValueError("L2 OHLC input values must be finite Decimals")

        if first is None:
            first = value
            lowest = value
            highest = value
        else:
            assert lowest is not None
            assert highest is not None
            if value < lowest:
                lowest = value
            if value > highest:
                highest = value

        last = value

    if first is None:
        return None

    assert lowest is not None
    assert highest is not None
    assert last is not None

    return L2OHLC(
        open=first,
        high=highest,
        low=lowest,
        close=last,
    )


@dataclass(frozen=True, slots=True)
class AggregatedL2Bar:
    """One larger end-of-bucket L2 state with coverage diagnostics."""

    bucket_index: int
    start_utc: datetime
    end_utc: datetime
    timeframe: Timeframe
    quality: AnalysisBarQuality

    bid_liquidity: Decimal | None
    ask_liquidity: Decimal | None
    source_count: int

    expected_seconds: int
    observed_seconds: int
    valid_seconds: int
    invalid_seconds: int
    missing_seconds: int
    maximum_invalid_run_seconds: int
    hard_discontinuity: bool

    endpoint_invalid_reason: BookSampleInvalidReason | None

    # Numerical summaries of usable one-second observations in this bucket.
    # These do not change endpoint ownership, quality, coverage, or whether
    # the aligned bar is eligible for chart display or LM analysis.
    bid_ohlc: L2OHLC | None = None
    ask_ohlc: L2OHLC | None = None
    total_ohlc: L2OHLC | None = None
    delta_ohlc: L2OHLC | None = None

    @property
    def total_liquidity(self) -> Decimal | None:
        if self.bid_liquidity is None or self.ask_liquidity is None:
            return None

        return _exact_nonnegative_decimal_sum(
            self.bid_liquidity,
            self.ask_liquidity,
        )

    def bid_ask_delta(
        self,
        *,
        decimal_precision: int = 34,
    ) -> Decimal | None:
        """Return Bid Liquidity minus Ask Liquidity in USD-equivalent units.

        Bid and Ask Liquidity are already derived using the project's
        linear USD-equivalent ``price * quantity`` convention. Supported
        USD, USDT, and USDC quotes are intentionally treated as approximately
        equal rather than live-FX converted.

        Delta remains derived from the authoritative Bid and Ask channels and
        is not persisted as a separate PostgreSQL or remote-artifact channel.
        """
        if self.bid_liquidity is None or self.ask_liquidity is None:
            return None

        if (
            isinstance(decimal_precision, bool)
            or not isinstance(decimal_precision, int)
            or decimal_precision <= 0
        ):
            raise ValueError("decimal_precision must be a positive integer")

        with localcontext(
            Context(
                prec=decimal_precision,
                rounding=ROUND_HALF_EVEN,
            )
        ):
            return self.bid_liquidity - self.ask_liquidity

    def bid_ask_imbalance(
        self,
        *,
        decimal_precision: int = 34,
    ) -> Decimal | None:
        """Return normalized Bid-Ask Imbalance for diagnostics.

        This normalized ratio is retained as a derived analytical helper, but
        the user-facing fourth liquidity panel and LM metric use Order-Book
        Delta instead.
        """
        total = self.total_liquidity

        if total is None or total == 0:
            return None

        if (
            isinstance(decimal_precision, bool)
            or not isinstance(decimal_precision, int)
            or decimal_precision <= 0
        ):
            raise ValueError("decimal_precision must be a positive integer")

        assert self.bid_liquidity is not None
        assert self.ask_liquidity is not None

        with localcontext(
            Context(
                prec=decimal_precision,
                rounding=ROUND_HALF_EVEN,
            )
        ):
            return (self.bid_liquidity - self.ask_liquidity) / total


@dataclass(frozen=True, slots=True)
class AggregatedPriceBar:
    """One larger real-trade OHLC bar with coverage diagnostics."""

    bucket_index: int
    start_utc: datetime
    end_utc: datetime
    timeframe: Timeframe
    quality: AnalysisBarQuality

    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    trade_count: int

    expected_seconds: int
    observed_seconds: int
    valid_seconds: int
    invalid_seconds: int
    missing_seconds: int
    maximum_invalid_run_seconds: int
    hard_discontinuity: bool


def _unique_by_timestamp(
    observations: Iterable[L2Second | PriceSecond],
) -> dict[datetime, L2Second | PriceSecond]:
    result: dict[datetime, L2Second | PriceSecond] = {}

    for observation in observations:
        timestamp = observation.timestamp_utc

        if timestamp in result:
            raise ValueError(
                f"Duplicate one-second observation timestamp: {timestamp.isoformat()}"
            )

        result[timestamp] = observation

    return result


def _bucket_observations(
    by_timestamp: dict[datetime, L2Second | PriceSecond],
    timestamps: tuple[datetime, ...],
    *,
    bucket_start: datetime,
    bucket_end: datetime,
) -> tuple[L2Second | PriceSecond, ...]:
    """Return only observations inside one half-open bucket.

    Missing seconds are represented through coverage arithmetic rather than
    by allocating one ``None`` entry for every missing timestamp.
    """

    start_index = bisect_left(
        timestamps,
        bucket_start,
    )
    end_index = bisect_left(
        timestamps,
        bucket_end,
        lo=start_index,
    )

    return tuple(
        by_timestamp[timestamp] for timestamp in timestamps[start_index:end_index]
    )


def _sparse_coverage_statistics(
    observations: tuple[L2Second | PriceSecond, ...],
    *,
    bucket_start: datetime,
    bucket_end: datetime,
    valid_quality: BookSampleQuality | TradeSampleQuality,
) -> tuple[int, int, int, int, int]:
    """Return observed, valid, invalid, missing, and maximum invalid run.

    Missing seconds and explicitly invalid observations both contribute to the
    invalid-run calculation. Consecutive missing ranges are counted
    arithmetically instead of being enumerated one second at a time.
    """

    expected_seconds = int((bucket_end - bucket_start).total_seconds())

    if expected_seconds <= 0:
        raise ValueError("Aggregation bucket must contain at least one second")

    observed_seconds = len(observations)
    valid_seconds = 0
    maximum_invalid_run = 0
    current_invalid_run = 0
    cursor = bucket_start

    for observation in observations:
        timestamp = observation.timestamp_utc

        if timestamp < cursor or timestamp >= bucket_end:
            raise ValueError(
                "Bucket observations are not strictly ordered inside "
                "their owning interval"
            )

        missing_before = int((timestamp - cursor).total_seconds())

        if missing_before > 0:
            current_invalid_run += missing_before
            maximum_invalid_run = max(
                maximum_invalid_run,
                current_invalid_run,
            )

        if observation.quality is valid_quality:
            valid_seconds += 1
            current_invalid_run = 0
        else:
            current_invalid_run += 1
            maximum_invalid_run = max(
                maximum_invalid_run,
                current_invalid_run,
            )

        cursor = timestamp + timedelta(seconds=1)

    trailing_missing = int((bucket_end - cursor).total_seconds())

    if trailing_missing > 0:
        current_invalid_run += trailing_missing
        maximum_invalid_run = max(
            maximum_invalid_run,
            current_invalid_run,
        )

    missing_seconds = expected_seconds - observed_seconds
    invalid_seconds = expected_seconds - valid_seconds

    if missing_seconds < 0 or invalid_seconds < 0:
        raise ValueError("Aggregation observations exceed bucket ownership")

    return (
        observed_seconds,
        valid_seconds,
        invalid_seconds,
        missing_seconds,
        maximum_invalid_run,
    )


def aggregate_l2_seconds(
    observations: Iterable[L2Second],
    *,
    start_utc: datetime,
    end_utc: datetime,
    timeframe: Timeframe | str,
) -> tuple[AggregatedL2Bar, ...]:
    """Aggregate L2 state by final one-second endpoint ownership."""
    spec = get_timeframe(timeframe)

    if spec.seconds < 1:
        raise TimeframeError("L2 aggregation requires at least one second")

    by_timestamp_raw = _unique_by_timestamp(observations)
    by_timestamp = {
        timestamp: observation
        for timestamp, observation in by_timestamp_raw.items()
        if isinstance(observation, L2Second)
    }

    if len(by_timestamp) != len(by_timestamp_raw):
        raise TypeError("aggregate_l2_seconds received a non-L2 observation")

    timestamps = tuple(sorted(by_timestamp))
    bars: list[AggregatedL2Bar] = []

    for bucket_index, bucket_start, bucket_end in iter_timeframe_buckets(
        start_utc,
        end_utc,
        spec,
    ):
        raw_bucket_values = _bucket_observations(
            by_timestamp_raw,
            timestamps,
            bucket_start=bucket_start,
            bucket_end=bucket_end,
        )
        bucket_values = tuple(
            observation
            for observation in raw_bucket_values
            if isinstance(observation, L2Second)
        )

        if len(bucket_values) != len(raw_bucket_values):
            raise TypeError("aggregate_l2_seconds received a non-L2 observation")

        (
            observed_seconds,
            valid_seconds,
            invalid_seconds,
            missing_seconds,
            maximum_invalid_run,
        ) = _sparse_coverage_statistics(
            bucket_values,
            bucket_start=bucket_start,
            bucket_end=bucket_end,
            valid_quality=BookSampleQuality.VALID,
        )

        expected_seconds = spec.seconds
        endpoint_timestamp = bucket_end - timedelta(seconds=1)

        endpoint = (
            bucket_values[-1]
            if (bucket_values and bucket_values[-1].timestamp_utc == endpoint_timestamp)
            else None
        )

        degraded_coverage = any(
            observation.quality is BookSampleQuality.VALID
            and observation.coverage_degraded
            for observation in bucket_values
        )

        bid_values: list[Decimal] = []
        ask_values: list[Decimal] = []
        total_values: list[Decimal] = []
        delta_values: list[Decimal] = []

        # _bucket_observations preserves timestamp order. Only numerically
        # usable seconds contribute: do not synthesize missing observations
        # or coerce an invalid observation to zero. Coverage and display
        # eligibility remain governed by the existing logic below.
        for observation in bucket_values:
            if observation.quality is not BookSampleQuality.VALID:
                continue

            bid_value = observation.bid_liquidity
            ask_value = observation.ask_liquidity
            assert bid_value is not None
            assert ask_value is not None

            bid_values.append(bid_value)
            ask_values.append(ask_value)
            total_values.append(_exact_nonnegative_decimal_sum(bid_value, ask_value))

            # Match AggregatedL2Bar.bid_ask_delta() at the endpoint. In
            # particular, do not change LM's established Delta precision.
            with localcontext(
                Context(
                    prec=34,
                    rounding=ROUND_HALF_EVEN,
                )
            ):
                delta_values.append(bid_value - ask_value)

        bid_ohlc = _reduce_l2_ohlc(bid_values)
        ask_ohlc = _reduce_l2_ohlc(ask_values)
        total_ohlc = _reduce_l2_ohlc(total_values)
        delta_ohlc = _reduce_l2_ohlc(delta_values)

        if endpoint is not None and endpoint.quality is BookSampleQuality.VALID:
            quality = (
                AnalysisBarQuality.VALID
                if invalid_seconds == 0 and not degraded_coverage
                else AnalysisBarQuality.DEGRADED
            )
            bid = endpoint.bid_liquidity
            ask = endpoint.ask_liquidity
            source_count = endpoint.source_count
            endpoint_reason = None
        else:
            quality = AnalysisBarQuality.INVALID
            bid = None
            ask = None
            source_count = 0
            endpoint_reason = (
                endpoint.invalid_reason
                if endpoint is not None
                else BookSampleInvalidReason.UNINITIALIZED
            )

        bars.append(
            AggregatedL2Bar(
                bucket_index=bucket_index,
                start_utc=bucket_start,
                end_utc=bucket_end,
                timeframe=spec,
                quality=quality,
                bid_liquidity=bid,
                ask_liquidity=ask,
                source_count=source_count,
                expected_seconds=expected_seconds,
                observed_seconds=observed_seconds,
                valid_seconds=valid_seconds,
                invalid_seconds=invalid_seconds,
                missing_seconds=missing_seconds,
                maximum_invalid_run_seconds=maximum_invalid_run,
                hard_discontinuity=invalid_seconds > 0,
                endpoint_invalid_reason=endpoint_reason,
                bid_ohlc=bid_ohlc,
                ask_ohlc=ask_ohlc,
                total_ohlc=total_ohlc,
                delta_ohlc=delta_ohlc,
            )
        )

    return tuple(bars)


def aggregate_price_seconds(
    observations: Iterable[PriceSecond],
    *,
    start_utc: datetime,
    end_utc: datetime,
    timeframe: Timeframe | str,
) -> tuple[AggregatedPriceBar, ...]:
    """Aggregate real-trade one-second OHLC into larger fixed buckets."""
    spec = get_timeframe(timeframe)
    by_timestamp_raw = _unique_by_timestamp(observations)
    by_timestamp = {
        timestamp: observation
        for timestamp, observation in by_timestamp_raw.items()
        if isinstance(observation, PriceSecond)
    }

    if len(by_timestamp) != len(by_timestamp_raw):
        raise TypeError("aggregate_price_seconds received a non-price observation")

    timestamps = tuple(sorted(by_timestamp))
    bars: list[AggregatedPriceBar] = []

    for bucket_index, bucket_start, bucket_end in iter_timeframe_buckets(
        start_utc,
        end_utc,
        spec,
    ):
        raw_bucket_values = _bucket_observations(
            by_timestamp_raw,
            timestamps,
            bucket_start=bucket_start,
            bucket_end=bucket_end,
        )
        bucket_values = tuple(
            observation
            for observation in raw_bucket_values
            if isinstance(observation, PriceSecond)
        )

        if len(bucket_values) != len(raw_bucket_values):
            raise TypeError("aggregate_price_seconds received a non-price observation")

        (
            observed_seconds,
            valid_seconds,
            invalid_seconds,
            missing_seconds,
            maximum_invalid_run,
        ) = _sparse_coverage_statistics(
            bucket_values,
            bucket_start=bucket_start,
            bucket_end=bucket_end,
            valid_quality=TradeSampleQuality.VALID,
        )

        expected_seconds = spec.seconds
        valid_observations = tuple(
            observation
            for observation in bucket_values
            if observation.quality is TradeSampleQuality.VALID
        )

        if not valid_observations:
            quality = AnalysisBarQuality.INVALID
            open_value = None
            high_value = None
            low_value = None
            close_value = None
            trade_count = 0
        else:
            quality = (
                AnalysisBarQuality.VALID
                if invalid_seconds == 0
                else AnalysisBarQuality.DEGRADED
            )

            first = valid_observations[0]
            last = valid_observations[-1]

            assert first.open is not None
            assert last.close is not None

            open_value = first.open
            close_value = last.close

            highs = tuple(
                observation.high
                for observation in valid_observations
                if observation.high is not None
            )
            lows = tuple(
                observation.low
                for observation in valid_observations
                if observation.low is not None
            )

            if not highs or not lows:
                raise ValueError("VALID price observations lack complete OHLC values")

            high_value = max(highs)
            low_value = min(lows)
            trade_count = sum(
                observation.trade_count for observation in valid_observations
            )

        bars.append(
            AggregatedPriceBar(
                bucket_index=bucket_index,
                start_utc=bucket_start,
                end_utc=bucket_end,
                timeframe=spec,
                quality=quality,
                open=open_value,
                high=high_value,
                low=low_value,
                close=close_value,
                trade_count=trade_count,
                expected_seconds=expected_seconds,
                observed_seconds=observed_seconds,
                valid_seconds=valid_seconds,
                invalid_seconds=invalid_seconds,
                missing_seconds=missing_seconds,
                maximum_invalid_run_seconds=maximum_invalid_run,
                hard_discontinuity=invalid_seconds > 0,
            )
        )

    return tuple(bars)


__all__ = [
    "AggregatedL2Bar",
    "AggregatedPriceBar",
    "AnalysisBarQuality",
    "L2Second",
    "PriceSecond",
    "aggregate_l2_seconds",
    "aggregate_price_seconds",
]

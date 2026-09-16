# l2shock/analysis/multi_market.py
"""Pure analysis-time aggregation of independent market L2 observations.

This module never reads raw order-book events and never writes PostgreSQL.

Each market must first be:

    independently reconstructed
    -> independently sampled
    -> independently encoded
    -> independently persisted
    -> independently verified on read

Only then may its one-second Bid and Ask Liquidity values contribute to an
aggregate analysis observation.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Final

from l2shock.analysis.aggregation import L2Second
from l2shock.ingest import BookSampleQuality
from l2shock.presets import EligibleMarket
from l2shock.timeutils import require_aware_utc

AGGREGATE_L2_SCHEMA: Final[str] = "l2shock.aggregate_l2_second"
AGGREGATE_L2_SCHEMA_VERSION: Final[int] = 1
AGGREGATE_L2_ALGORITHM_VERSION: Final[str] = "sum-independent-valid-markets-v1"


class AggregateL2Error(ValueError):
    """Aggregate L2 input or output violates its deterministic contract."""


class AggregateL2QualityState(StrEnum):
    """Analytical quality of one aggregate one-second observation."""

    VALID = "VALID"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


class AggregateL2InvalidReason(StrEnum):
    """Why an aggregate second has no usable liquidity."""

    NO_VALID_MARKETS = "no_valid_markets"


def _canonical_sha256(
    field_name: str,
    value: object,
) -> str:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise AggregateL2Error(f"{field_name} must be a canonical lowercase SHA-256")

    return digest


def _positive_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AggregateL2Error(f"{field_name} must be a positive integer")

    return value


def _nonnegative_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AggregateL2Error(f"{field_name} must be a non-negative integer")

    return value


def _finite_nonnegative_decimal(
    field_name: str,
    value: object,
) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise AggregateL2Error(f"{field_name} must be a finite non-negative Decimal")

    return value


def _exact_decimal_sum(
    values: Sequence[Decimal],
    *,
    field_name: str,
) -> Decimal:
    """Sum finite non-negative Decimals without ambient-context rounding."""

    normalized = tuple(
        _finite_nonnegative_decimal(
            f"{field_name}[{index}]",
            value,
        )
        for index, value in enumerate(values)
    )

    if not normalized:
        return Decimal(0)

    common_exponent = min(int(value.as_tuple().exponent) for value in normalized)
    total_coefficient = 0

    for value in normalized:
        decimal_tuple = value.as_tuple()
        coefficient = 0

        for digit in decimal_tuple.digits:
            coefficient = coefficient * 10 + digit

        if decimal_tuple.sign:
            coefficient = -coefficient

        exponent = int(decimal_tuple.exponent)
        total_coefficient += coefficient * (10 ** (exponent - common_exponent))

    if total_coefficient == 0:
        return Decimal(0)

    digits = tuple(int(character) for character in str(abs(total_coefficient)))

    return Decimal(
        (
            int(total_coefficient < 0),
            digits,
            common_exponent,
        )
    )


@dataclass(frozen=True, slots=True, order=True)
class AggregateMarketKey:
    """Canonical identity of one independently materialized market."""

    provider: str
    venue: str
    instrument: str

    def __post_init__(self) -> None:
        provider = str(self.provider or "").strip().lower()
        venue = str(self.venue or "").strip().lower()
        instrument = str(self.instrument or "").strip().upper()

        if not provider or not venue or not instrument:
            raise AggregateL2Error(
                "Aggregate market identity cannot contain blank fields"
            )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "instrument", instrument)

    @classmethod
    def from_eligible_market(
        cls,
        market: EligibleMarket,
    ) -> AggregateMarketKey:
        if not isinstance(market, EligibleMarket):
            raise TypeError("market must be an EligibleMarket")

        return cls(
            provider=market.provider,
            venue=market.venue,
            instrument=market.instrument,
        )

    @property
    def identity_tuple(self) -> tuple[str, str, str]:
        return (
            self.provider,
            self.venue,
            self.instrument,
        )

    def to_canonical_dict(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "instrument": self.instrument,
        }


@dataclass(frozen=True, slots=True)
class AggregateMarketContribution:
    """One verified market contribution to an aggregate second."""

    market: AggregateMarketKey
    component_preset_hash: str
    l2_content_sha256: str
    bid_liquidity: Decimal
    ask_liquidity: Decimal
    source_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.market, AggregateMarketKey):
            raise TypeError("market must be an AggregateMarketKey")

        object.__setattr__(
            self,
            "component_preset_hash",
            _canonical_sha256(
                "component_preset_hash",
                self.component_preset_hash,
            ),
        )
        object.__setattr__(
            self,
            "l2_content_sha256",
            _canonical_sha256(
                "l2_content_sha256",
                self.l2_content_sha256,
            ),
        )
        object.__setattr__(
            self,
            "bid_liquidity",
            _finite_nonnegative_decimal(
                "bid_liquidity",
                self.bid_liquidity,
            ),
        )
        object.__setattr__(
            self,
            "ask_liquidity",
            _finite_nonnegative_decimal(
                "ask_liquidity",
                self.ask_liquidity,
            ),
        )

        if _positive_integer("source_count", self.source_count) != 1:
            raise AggregateL2Error(
                "A component market contribution must have source_count=1"
            )


@dataclass(frozen=True, slots=True)
class AggregateMarketSeries:
    """Verified one-second observations for one component market."""

    market: AggregateMarketKey
    component_preset_hash: str
    l2_content_sha256_by_hour: Mapping[datetime, str]
    observations: tuple[L2Second, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.market, AggregateMarketKey):
            raise TypeError("market must be an AggregateMarketKey")

        preset_hash = _canonical_sha256(
            "component_preset_hash",
            self.component_preset_hash,
        )

        normalized_hashes: dict[datetime, str] = {}

        for raw_hour, raw_digest in dict(self.l2_content_sha256_by_hour).items():
            hour = require_aware_utc(
                "l2_content_sha256_by_hour key",
                raw_hour,
            )

            if hour.minute != 0 or hour.second != 0 or hour.microsecond != 0:
                raise AggregateL2Error(
                    "Component L2 content hashes must be owned by exact UTC hours"
                )

            normalized_hashes[hour] = _canonical_sha256(
                "l2_content_sha256_by_hour value",
                raw_digest,
            )

        observations = tuple(self.observations)
        seen: set[datetime] = set()
        previous: datetime | None = None

        for observation in observations:
            if not isinstance(observation, L2Second):
                raise TypeError("observations must contain L2Second objects")

            timestamp = observation.timestamp_utc

            if timestamp in seen:
                raise AggregateL2Error(
                    "Component series contains a duplicate timestamp"
                )

            if previous is not None and timestamp <= previous:
                raise AggregateL2Error(
                    "Component observations must be strictly chronological"
                )

            seen.add(timestamp)
            previous = timestamp

        object.__setattr__(
            self,
            "component_preset_hash",
            preset_hash,
        )
        object.__setattr__(
            self,
            "l2_content_sha256_by_hour",
            normalized_hashes,
        )
        object.__setattr__(
            self,
            "observations",
            observations,
        )


@dataclass(frozen=True, slots=True)
class AggregatedL2Second:
    """One analysis-time sum of independently verified market liquidity."""

    timestamp_utc: datetime
    quality_state: AggregateL2QualityState

    expected_market_count: int
    contributing_market_count: int

    bid_liquidity: Decimal | None
    ask_liquidity: Decimal | None
    source_count: int

    contributions: tuple[AggregateMarketContribution, ...]
    invalid_reason: AggregateL2InvalidReason | None = None

    schema: str = AGGREGATE_L2_SCHEMA
    schema_version: int = AGGREGATE_L2_SCHEMA_VERSION
    algorithm_version: str = AGGREGATE_L2_ALGORITHM_VERSION

    def __post_init__(self) -> None:
        timestamp = require_aware_utc(
            "timestamp_utc",
            self.timestamp_utc,
        )

        if timestamp.microsecond != 0:
            raise AggregateL2Error("Aggregated L2 timestamp must be second-aligned")

        quality = AggregateL2QualityState(self.quality_state)
        expected = _positive_integer(
            "expected_market_count",
            self.expected_market_count,
        )
        contributing = _nonnegative_integer(
            "contributing_market_count",
            self.contributing_market_count,
        )
        source_count = _nonnegative_integer(
            "source_count",
            self.source_count,
        )

        if contributing > expected:
            raise AggregateL2Error(
                "contributing_market_count cannot exceed expected_market_count"
            )

        contributions = tuple(
            sorted(
                self.contributions,
                key=lambda item: item.market.identity_tuple,
            )
        )

        if len(contributions) != contributing:
            raise AggregateL2Error(
                "Contribution count does not match contributing_market_count"
            )

        identities = [item.market.identity_tuple for item in contributions]

        if len(set(identities)) != len(identities):
            raise AggregateL2Error(
                "Aggregate second contains a duplicate market contribution"
            )

        if source_count != sum(item.source_count for item in contributions):
            raise AggregateL2Error(
                "source_count does not match component contributions"
            )

        if contributing == 0:
            if quality is not AggregateL2QualityState.INVALID:
                raise AggregateL2Error(
                    "Zero contributing markets requires INVALID quality"
                )

            if self.invalid_reason is not (AggregateL2InvalidReason.NO_VALID_MARKETS):
                raise AggregateL2Error(
                    "INVALID aggregate second requires no_valid_markets reason"
                )

            if (
                self.bid_liquidity is not None
                or self.ask_liquidity is not None
                or source_count != 0
            ):
                raise AggregateL2Error(
                    "INVALID aggregate second cannot contain liquidity"
                )
        else:
            if self.invalid_reason is not None:
                raise AggregateL2Error(
                    "Usable aggregate second cannot have an invalid reason"
                )

            bid = _finite_nonnegative_decimal(
                "bid_liquidity",
                self.bid_liquidity,
            )
            ask = _finite_nonnegative_decimal(
                "ask_liquidity",
                self.ask_liquidity,
            )

            expected_bid = _exact_decimal_sum(
                tuple(contribution.bid_liquidity for contribution in contributions),
                field_name="contribution.bid_liquidity",
            )
            expected_ask = _exact_decimal_sum(
                tuple(contribution.ask_liquidity for contribution in contributions),
                field_name="contribution.ask_liquidity",
            )

            if bid != expected_bid or ask != expected_ask:
                raise AggregateL2Error(
                    "Aggregate liquidity does not equal component sums"
                )

            expected_quality = (
                AggregateL2QualityState.VALID
                if contributing == expected
                else AggregateL2QualityState.DEGRADED
            )

            if quality is not expected_quality:
                raise AggregateL2Error(
                    "Aggregate quality does not match market coverage"
                )

        if self.schema != AGGREGATE_L2_SCHEMA:
            raise AggregateL2Error("Unsupported aggregate L2 schema")

        if self.schema_version != AGGREGATE_L2_SCHEMA_VERSION:
            raise AggregateL2Error("Unsupported aggregate L2 schema version")

        if self.algorithm_version != AGGREGATE_L2_ALGORITHM_VERSION:
            raise AggregateL2Error("Unsupported aggregate L2 algorithm version")

        object.__setattr__(self, "timestamp_utc", timestamp)
        object.__setattr__(self, "quality_state", quality)
        object.__setattr__(self, "contributions", contributions)

    @property
    def total_liquidity(self) -> Decimal | None:
        if self.bid_liquidity is None or self.ask_liquidity is None:
            return None

        return _exact_decimal_sum(
            (
                self.bid_liquidity,
                self.ask_liquidity,
            ),
            field_name="total_liquidity",
        )

    def bid_ask_imbalance(
        self,
        *,
        decimal_precision: int = 34,
    ) -> Decimal | None:
        if self.bid_liquidity is None or self.ask_liquidity is None:
            return None

        total = self.total_liquidity

        if total is None or total == 0:
            return None

        if (
            isinstance(decimal_precision, bool)
            or not isinstance(decimal_precision, int)
            or decimal_precision < 16
        ):
            raise AggregateL2Error(
                "decimal_precision must be an integer of at least 16"
            )

        from decimal import Context, localcontext

        with localcontext(Context(prec=decimal_precision)):
            return (self.bid_liquidity - self.ask_liquidity) / total


@dataclass(frozen=True, slots=True)
class AggregateL2Series:
    """Complete aggregate output and deterministic provenance identity."""

    expected_markets: tuple[AggregateMarketKey, ...]
    observations: tuple[AggregatedL2Second, ...]
    aggregate_content_sha256: str

    def __post_init__(self) -> None:
        markets = tuple(sorted(self.expected_markets))

        if not markets:
            raise AggregateL2Error(
                "Aggregate series requires at least one expected market"
            )

        if len(set(markets)) != len(markets):
            raise AggregateL2Error(
                "Aggregate series contains duplicate expected markets"
            )

        observations = tuple(self.observations)
        previous: datetime | None = None

        for observation in observations:
            if not isinstance(observation, AggregatedL2Second):
                raise TypeError("observations must contain AggregatedL2Second objects")

            if observation.expected_market_count != len(markets):
                raise AggregateL2Error(
                    "Observation expected-market count is inconsistent"
                )

            if (
                previous is not None
                and observation.timestamp_utc != previous + timedelta(seconds=1)
            ):
                raise AggregateL2Error(
                    "Aggregate observations must be contiguous one-second slots"
                )

            previous = observation.timestamp_utc

        object.__setattr__(self, "expected_markets", markets)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(
            self,
            "aggregate_content_sha256",
            _canonical_sha256(
                "aggregate_content_sha256",
                self.aggregate_content_sha256,
            ),
        )


def _hour_floor(value: datetime) -> datetime:
    return value.replace(
        minute=0,
        second=0,
        microsecond=0,
    )


def _series_observation_map(
    series: AggregateMarketSeries,
) -> dict[datetime, L2Second]:
    return {
        observation.timestamp_utc: observation for observation in series.observations
    }


def _aggregate_identity_payload(
    *,
    expected_markets: tuple[AggregateMarketKey, ...],
    series_by_market: Mapping[
        AggregateMarketKey,
        AggregateMarketSeries,
    ],
    start_utc: datetime,
    end_utc: datetime,
) -> dict[str, object]:
    components = []

    for market in expected_markets:
        series = series_by_market[market]

        components.append(
            {
                "market": market.to_canonical_dict(),
                "component_preset_hash": (series.component_preset_hash),
                "hourly_content": [
                    {
                        "hour_utc": hour.isoformat().replace(
                            "+00:00",
                            "Z",
                        ),
                        "content_sha256": digest,
                    }
                    for hour, digest in sorted(series.l2_content_sha256_by_hour.items())
                ],
            }
        )

    return {
        "schema": AGGREGATE_L2_SCHEMA,
        "schema_version": AGGREGATE_L2_SCHEMA_VERSION,
        "algorithm_version": AGGREGATE_L2_ALGORITHM_VERSION,
        "start_utc": start_utc.isoformat().replace("+00:00", "Z"),
        "end_utc": end_utc.isoformat().replace("+00:00", "Z"),
        "expected_markets": [market.to_canonical_dict() for market in expected_markets],
        "components": components,
    }


def aggregate_market_l2_seconds(
    *,
    expected_markets: Sequence[AggregateMarketKey],
    market_series: Sequence[AggregateMarketSeries],
    start_utc: datetime,
    end_utc: datetime,
) -> AggregateL2Series:
    """Aggregate independent market rows over half-open ``[start, end)``.

    A market contributes only when its observation for that exact second is
    ``BookSampleQuality.VALID``.

    Quality policy:

        every expected market contributes:
            VALID

        at least one but fewer than expected contribute:
            DEGRADED

        no expected market contributes:
            INVALID
    """
    start = require_aware_utc("start_utc", start_utc)
    end = require_aware_utc("end_utc", end_utc)

    if start.microsecond != 0 or end.microsecond != 0:
        raise AggregateL2Error("Aggregate range must be second-aligned")

    if end <= start:
        raise AggregateL2Error("end_utc must be after start_utc")

    normalized_expected = tuple(
        sorted(
            AggregateMarketKey(
                provider=market.provider,
                venue=market.venue,
                instrument=market.instrument,
            )
            for market in expected_markets
        )
    )

    if not normalized_expected:
        raise AggregateL2Error("At least one expected market is required")

    if len(set(normalized_expected)) != len(normalized_expected):
        raise AggregateL2Error("expected_markets contains a duplicate identity")

    series_by_market: dict[
        AggregateMarketKey,
        AggregateMarketSeries,
    ] = {}

    for series in market_series:
        if not isinstance(series, AggregateMarketSeries):
            raise TypeError("market_series must contain AggregateMarketSeries objects")

        if series.market in series_by_market:
            raise AggregateL2Error("market_series contains duplicate market identity")

        series_by_market[series.market] = series

    if set(series_by_market) != set(normalized_expected):
        raise AggregateL2Error("market_series must cover exactly the expected markets")

    observations_by_market = {
        market: _series_observation_map(series_by_market[market])
        for market in normalized_expected
    }

    aggregate_observations: list[AggregatedL2Second] = []
    timestamp = start

    while timestamp < end:
        contributions: list[AggregateMarketContribution] = []

        for market in normalized_expected:
            component_observation = observations_by_market[market].get(timestamp)

            if component_observation is None:
                continue

            if component_observation.quality is not (BookSampleQuality.VALID):
                continue

            if (
                component_observation.bid_liquidity is None
                or component_observation.ask_liquidity is None
            ):
                raise AggregateL2Error("VALID component observation lacks liquidity")

            hour = _hour_floor(timestamp)

            try:
                content_sha256 = series_by_market[market].l2_content_sha256_by_hour[
                    hour
                ]
            except KeyError as exc:
                raise AggregateL2Error(
                    "Component observation lacks hourly content provenance"
                ) from exc

            contributions.append(
                AggregateMarketContribution(
                    market=market,
                    component_preset_hash=(
                        series_by_market[market].component_preset_hash
                    ),
                    l2_content_sha256=content_sha256,
                    bid_liquidity=(component_observation.bid_liquidity),
                    ask_liquidity=(component_observation.ask_liquidity),
                    source_count=1,
                )
            )

        contributing_count = len(contributions)
        expected_count = len(normalized_expected)

        if contributing_count == 0:
            aggregate_observations.append(
                AggregatedL2Second(
                    timestamp_utc=timestamp,
                    quality_state=(AggregateL2QualityState.INVALID),
                    expected_market_count=expected_count,
                    contributing_market_count=0,
                    bid_liquidity=None,
                    ask_liquidity=None,
                    source_count=0,
                    contributions=(),
                    invalid_reason=(AggregateL2InvalidReason.NO_VALID_MARKETS),
                )
            )
        else:
            bid = _exact_decimal_sum(
                tuple(contribution.bid_liquidity for contribution in contributions),
                field_name="aggregate.bid_liquidity",
            )
            ask = _exact_decimal_sum(
                tuple(contribution.ask_liquidity for contribution in contributions),
                field_name="aggregate.ask_liquidity",
            )

            aggregate_observations.append(
                AggregatedL2Second(
                    timestamp_utc=timestamp,
                    quality_state=(
                        AggregateL2QualityState.VALID
                        if contributing_count == expected_count
                        else AggregateL2QualityState.DEGRADED
                    ),
                    expected_market_count=expected_count,
                    contributing_market_count=contributing_count,
                    bid_liquidity=bid,
                    ask_liquidity=ask,
                    source_count=contributing_count,
                    contributions=tuple(contributions),
                    invalid_reason=None,
                )
            )

        timestamp += timedelta(seconds=1)

    identity_payload = _aggregate_identity_payload(
        expected_markets=normalized_expected,
        series_by_market=series_by_market,
        start_utc=start,
        end_utc=end,
    )
    encoded_identity = json.dumps(
        identity_payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return AggregateL2Series(
        expected_markets=normalized_expected,
        observations=tuple(aggregate_observations),
        aggregate_content_sha256=hashlib.sha256(encoded_identity).hexdigest(),
    )


__all__ = [
    "AGGREGATE_L2_ALGORITHM_VERSION",
    "AGGREGATE_L2_SCHEMA",
    "AGGREGATE_L2_SCHEMA_VERSION",
    "AggregateL2Error",
    "AggregateL2InvalidReason",
    "AggregateL2QualityState",
    "AggregateL2Series",
    "AggregateMarketContribution",
    "AggregateMarketKey",
    "AggregateMarketSeries",
    "AggregatedL2Second",
    "aggregate_market_l2_seconds",
]

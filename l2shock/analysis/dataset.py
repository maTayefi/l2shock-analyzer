# l2shock/analysis/dataset.py
"""Verified analytical loading and aligned segmented dataset construction.

This module connects compact persisted one-second L2/price hours to the pure
fixed-duration aggregation and price-filter policies.

Ownership rules:

- PostgreSQL rows are read through repositories which verify codec, quality,
  provenance, and content identity;
- absent persisted hours remain explicit coverage facts;
- L2 and price observations are aligned by canonical UTC bucket identity;
- price-filter eligibility uses true OHLC intersection;
- display clipping never overwrites exact stored OHLC;
- invalid/missing lower-level coverage remains a hard discontinuity;
- price-excluded periods remain explicit discontinuities;
- context may extend one eligible segment but never bridge a hard boundary or
  include another eligible segment;
- analysis identity is a deterministic SHA-256 over data-affecting request and
  persisted-content ownership.

This module does not detect or rank Liquidity Movements and performs no
database writes.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Final

from sqlalchemy.orm import Session

from l2shock import __version__
from l2shock.analysis.aggregation import (
    AggregatedL2Bar,
    AggregatedPriceBar,
    AnalysisBarQuality,
    L2Second,
    PriceSecond,
    aggregate_l2_seconds,
    aggregate_price_seconds,
)
from l2shock.analysis.multi_market import (
    AGGREGATE_L2_ALGORITHM_VERSION,
    AggregateL2InvalidReason,
    AggregateL2QualityState,
    AggregateMarketKey,
    AggregateMarketSeries,
    aggregate_market_l2_seconds,
)
from l2shock.presets import (
    component_data_presets,
    liquidity_data_preset_from_canonical_dict,
)
from l2shock.analysis.price_filter import (
    OHLC,
    PriceBarState,
    PriceBounds,
    classify_price_bar,
    clip_ohlc_for_display,
)
from l2shock.analysis.timeframes import (
    SnappedAnalysisRange,
    Timeframe,
    get_timeframe,
    select_chart_timeframe,
    snap_closed_analysis_range,
)
from l2shock.db.analytical_repository import (
    AnalyticalRepository,
    PersistedL2HourlySeries,
)
from l2shock.db.price_repository import (
    PersistedPriceHourlySeries,
    PriceAnalyticalRepository,
)
from l2shock.ingest.sampling import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.liquidity import decode_hourly_liquidity_blocks
from l2shock.price import decode_hourly_trade_ohlc_blocks
from l2shock.timeutils import (
    floor_to_hour,
    require_aware_utc,
    require_utc_hour,
)

ANALYSIS_DATASET_SCHEMA: Final[str] = "l2shock.aligned_analysis_dataset"
ANALYSIS_DATASET_SCHEMA_VERSION: Final[int] = 3

_ACTIVITY_TIMEFRAME_LABELS: Final[frozenset[str]] = frozenset(
    {
        "1s",
        "5s",
        "10s",
        "15s",
        "30s",
    }
)


class AnalysisDatasetError(ValueError):
    """An analytical request or aligned dataset violates its contract."""


class AnalysisDiscontinuityReason(StrEnum):
    """Why one or more aggregated bars cannot belong to a continuous core."""

    PRICE_EXCLUDED = "price_excluded"
    PRICE_INVALID = "price_invalid"
    PRICE_COVERAGE_GAP = "price_coverage_gap"
    L2_INVALID = "l2_invalid"
    L2_COVERAGE_GAP = "l2_coverage_gap"


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
        raise AnalysisDatasetError(
            f"{field_name} must be a canonical lowercase SHA-256"
        )

    return digest


def _nonnegative_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AnalysisDatasetError(f"{field_name} must be an integer")

    if value < 0:
        raise AnalysisDatasetError(f"{field_name} must be non-negative")

    return value


def _positive_integer(
    field_name: str,
    value: object,
) -> int:
    result = _nonnegative_integer(field_name, value)

    if result <= 0:
        raise AnalysisDatasetError(f"{field_name} must be positive")

    return result


def _utc_text(value: datetime) -> str:
    return (
        require_aware_utc("datetime", value)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def _canonical_decimal_text(
    value: Decimal | None,
) -> str | None:
    if value is None:
        return None

    if not isinstance(value, Decimal) or not value.is_finite():
        raise AnalysisDatasetError("Canonical analytical decimal must be finite")

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if text in {
        "",
        "-0",
        "+0",
    }:
        text = "0"

    if "e" in text.lower():
        raise AnalysisDatasetError(
            "Canonical analytical decimal must not use exponent notation"
        )

    return text


def _observation_content_sha256(
    l2_values: tuple[L2Second, ...],
    price_values: tuple[PriceSecond, ...],
) -> str:
    """Hash exact timestamp-owned observations in canonical order."""

    digest = hashlib.sha256()
    digest.update(b"l2shock/aligned-analysis-observations/v1\x00")

    for observation in sorted(
        l2_values,
        key=lambda item: item.timestamp_utc,
    ):
        if not isinstance(observation, L2Second):
            raise TypeError("l2_seconds must contain L2Second objects")

        payload = {
            "kind": "l2",
            "timestamp_utc": _utc_text(observation.timestamp_utc),
            "quality": observation.quality.value,
            "invalid_reason": (
                observation.invalid_reason.value
                if observation.invalid_reason is not None
                else None
            ),
            "bid_liquidity": _canonical_decimal_text(observation.bid_liquidity),
            "ask_liquidity": _canonical_decimal_text(observation.ask_liquidity),
            "source_count": observation.source_count,
            "coverage_degraded": observation.coverage_degraded,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)

    for observation in sorted(
        price_values,
        key=lambda item: item.timestamp_utc,
    ):
        if not isinstance(observation, PriceSecond):
            raise TypeError("price_seconds must contain PriceSecond objects")

        payload = {
            "kind": "price",
            "timestamp_utc": _utc_text(observation.timestamp_utc),
            "quality": observation.quality.value,
            "invalid_reason": (
                observation.invalid_reason.value
                if observation.invalid_reason is not None
                else None
            ),
            "open": _canonical_decimal_text(observation.open),
            "high": _canonical_decimal_text(observation.high),
            "low": _canonical_decimal_text(observation.low),
            "close": _canonical_decimal_text(observation.close),
            "trade_count": observation.trade_count,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)

    return digest.hexdigest()


def _canonical_reasons(
    reasons: Iterable[AnalysisDiscontinuityReason],
) -> tuple[AnalysisDiscontinuityReason, ...]:
    selected = {AnalysisDiscontinuityReason(reason) for reason in reasons}

    return tuple(reason for reason in AnalysisDiscontinuityReason if reason in selected)


@dataclass(frozen=True, slots=True)
class AnalysisDatasetRequest:
    """One immutable request for aligned L2 and real-trade analysis series.

    ``chart_timeframe_override`` is optional:

    - ``None`` selects the finest registered timeframe fitting
      ``maximum_chart_bars``;
    - a registered timeframe explicitly owns the chart series.

    An explicit override is analytical input, not temporary browser state.
    Changing it rebuilds the verified dataset and reruns chart-timeframe LM
    detection/ranking.
    """

    base: str
    preset_hash: str

    requested_start_utc: datetime
    requested_end_utc: datetime

    activity_timeframe: Timeframe | str = "1s"
    maximum_chart_bars: int = 400
    chart_timeframe_override: Timeframe | str | None = None

    price_bounds: PriceBounds = field(default_factory=PriceBounds)
    context_before: int = 3
    context_after: int = 3

    def __post_init__(self) -> None:
        base = str(self.base or "").strip().upper()

        if base not in {"BTC", "ETH"}:
            raise AnalysisDatasetError("base must be BTC or ETH")

        start = require_aware_utc(
            "requested_start_utc",
            self.requested_start_utc,
        )
        end = require_aware_utc(
            "requested_end_utc",
            self.requested_end_utc,
        )

        if end < start:
            raise AnalysisDatasetError(
                "requested_end_utc cannot precede requested_start_utc"
            )

        activity = get_timeframe(self.activity_timeframe)

        if activity.label not in _ACTIVITY_TIMEFRAME_LABELS:
            raise AnalysisDatasetError(
                "activity_timeframe must be one of " "1s, 5s, 10s, 15s, or 30s"
            )

        raw_chart_override = self.chart_timeframe_override

        if raw_chart_override is None:
            chart_override = None
        else:
            chart_override = get_timeframe(raw_chart_override)

        maximum_bars = _positive_integer(
            "maximum_chart_bars",
            self.maximum_chart_bars,
        )
        context_before = _nonnegative_integer(
            "context_before",
            self.context_before,
        )
        context_after = _nonnegative_integer(
            "context_after",
            self.context_after,
        )

        if not isinstance(self.price_bounds, PriceBounds):
            raise AnalysisDatasetError("price_bounds must be PriceBounds")

        object.__setattr__(self, "base", base)
        object.__setattr__(
            self,
            "preset_hash",
            _canonical_sha256(
                "preset_hash",
                self.preset_hash,
            ),
        )
        object.__setattr__(self, "requested_start_utc", start)
        object.__setattr__(self, "requested_end_utc", end)
        object.__setattr__(self, "activity_timeframe", activity)
        object.__setattr__(
            self,
            "chart_timeframe_override",
            chart_override,
        )
        object.__setattr__(self, "maximum_chart_bars", maximum_bars)
        object.__setattr__(self, "context_before", context_before)
        object.__setattr__(self, "context_after", context_after)


def _resolved_chart_timeframe(
    request: AnalysisDatasetRequest,
) -> Timeframe:
    """Resolve a chart timeframe that satisfies the request's bar budget."""

    if not isinstance(request, AnalysisDatasetRequest):
        raise TypeError("request must be AnalysisDatasetRequest")

    override = request.chart_timeframe_override

    if override is not None:
        snapped = snap_closed_analysis_range(
            request.requested_start_utc,
            request.requested_end_utc,
            override,
        )

        if snapped.bar_count > request.maximum_chart_bars:
            raise AnalysisDatasetError(
                "The explicit chart timeframe produces "
                f"{snapped.bar_count} bars, exceeding maximum_chart_bars="
                f"{request.maximum_chart_bars}. Select a coarser chart "
                "timeframe or increase the chart-bar limit."
            )

        return override

    return select_chart_timeframe(
        request.requested_start_utc,
        request.requested_end_utc,
        maximum_bars=request.maximum_chart_bars,
    )


@dataclass(frozen=True, slots=True)
class AnalysisMarketCoverage:
    """One expected component market's compact-row ownership for an hour."""

    provider: str
    venue: str
    instrument: str
    component_preset_hash: str
    content_sha256: str | None

    def __post_init__(self) -> None:
        provider = str(self.provider or "").strip().lower()
        venue = str(self.venue or "").strip().lower()
        instrument = str(self.instrument or "").strip().upper()

        if not provider or not venue or not instrument:
            raise AnalysisDatasetError(
                "Analysis market coverage identity cannot contain blank fields"
            )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(
            self,
            "component_preset_hash",
            _canonical_sha256(
                "component_preset_hash",
                self.component_preset_hash,
            ),
        )

        if self.content_sha256 is not None:
            object.__setattr__(
                self,
                "content_sha256",
                _canonical_sha256(
                    "content_sha256",
                    self.content_sha256,
                ),
            )

    @property
    def identity_tuple(self) -> tuple[str, str, str]:
        return (
            self.provider,
            self.venue,
            self.instrument,
        )

    @property
    def present(self) -> bool:
        return self.content_sha256 is not None

    def to_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "instrument": self.instrument,
            "component_preset_hash": self.component_preset_hash,
            "present": self.present,
            "content_sha256": self.content_sha256,
        }


@dataclass(frozen=True, slots=True)
class AnalysisHourCoverage:
    """Persisted compact-series availability for one queried UTC hour."""

    hour_utc: datetime
    l2_content_sha256: str | None
    price_content_sha256: str | None
    l2_market_coverage: tuple[AnalysisMarketCoverage, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "hour_utc",
            require_utc_hour(
                "hour_utc",
                self.hour_utc,
            ),
        )

        for field_name in (
            "l2_content_sha256",
            "price_content_sha256",
        ):
            value = getattr(self, field_name)

            if value is None:
                continue

            object.__setattr__(
                self,
                field_name,
                _canonical_sha256(
                    field_name,
                    value,
                ),
            )

        market_coverage = tuple(self.l2_market_coverage)

        if any(
            not isinstance(item, AnalysisMarketCoverage) for item in market_coverage
        ):
            raise AnalysisDatasetError(
                "l2_market_coverage must contain " "AnalysisMarketCoverage objects"
            )

        market_coverage = tuple(
            sorted(
                market_coverage,
                key=lambda item: item.identity_tuple,
            )
        )

        identities = [item.identity_tuple for item in market_coverage]

        if len(set(identities)) != len(identities):
            raise AnalysisDatasetError("l2_market_coverage contains duplicate markets")

        object.__setattr__(
            self,
            "l2_market_coverage",
            market_coverage,
        )

    @property
    def expected_l2_market_count(self) -> int:
        return len(self.l2_market_coverage) if self.l2_market_coverage else 1

    @property
    def available_l2_market_count(self) -> int:
        if self.l2_market_coverage:
            return sum(item.present for item in self.l2_market_coverage)

        return int(self.l2_content_sha256 is not None)

    @property
    def l2_market_coverage_degraded(self) -> bool:
        available = self.available_l2_market_count

        return bool(available > 0 and available < self.expected_l2_market_count)

    @property
    def l2_present(self) -> bool:
        return self.available_l2_market_count > 0

    @property
    def price_present(self) -> bool:
        return self.price_content_sha256 is not None

    @property
    def complete(self) -> bool:
        return bool(
            self.price_present
            and self.l2_present
            and self.available_l2_market_count == self.expected_l2_market_count
        )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "hour_utc": _utc_text(self.hour_utc),
            "l2_present": self.l2_present,
            "price_present": self.price_present,
            "l2_content_sha256": self.l2_content_sha256,
            "price_content_sha256": self.price_content_sha256,
        }

        if self.l2_market_coverage:
            payload["l2_market_coverage"] = [
                item.to_dict() for item in self.l2_market_coverage
            ]
            payload["expected_l2_market_count"] = self.expected_l2_market_count
            payload["available_l2_market_count"] = self.available_l2_market_count
            payload["l2_market_coverage_degraded"] = self.l2_market_coverage_degraded

        return payload


@dataclass(frozen=True, slots=True)
class AlignedAnalysisBar:
    """One aligned price/L2 bar for a single fixed timeframe."""

    bucket_index: int
    start_utc: datetime
    end_utc: datetime
    timeframe: Timeframe

    l2: AggregatedL2Bar
    price: AggregatedPriceBar

    price_state: PriceBarState
    display_ohlc: OHLC | None

    core_eligible: bool
    hard_discontinuity: bool
    discontinuity_reasons: tuple[AnalysisDiscontinuityReason, ...]

    def __post_init__(self) -> None:
        if (
            isinstance(self.bucket_index, bool)
            or not isinstance(self.bucket_index, int)
            or self.bucket_index < 0
        ):
            raise AnalysisDatasetError("bucket_index must be a non-negative integer")

        start = require_aware_utc("start_utc", self.start_utc)
        end = require_aware_utc("end_utc", self.end_utc)
        timeframe = get_timeframe(self.timeframe)

        if end - start != timeframe.duration:
            raise AnalysisDatasetError(
                "Aligned analysis bar duration does not match timeframe"
            )

        if (
            self.l2.bucket_index != self.bucket_index
            or self.price.bucket_index != self.bucket_index
            or self.l2.start_utc != start
            or self.price.start_utc != start
            or self.l2.end_utc != end
            or self.price.end_utc != end
            or self.l2.timeframe != timeframe
            or self.price.timeframe != timeframe
        ):
            raise AnalysisDatasetError(
                "Aligned price and L2 bars do not share one bucket identity"
            )

        price_state = PriceBarState(self.price_state)
        reasons = _canonical_reasons(self.discontinuity_reasons)

        if self.core_eligible:
            if self.hard_discontinuity:
                raise AnalysisDatasetError(
                    "A core-eligible bar cannot be a hard discontinuity"
                )

            if price_state is not PriceBarState.ELIGIBLE:
                raise AnalysisDatasetError(
                    "A core-eligible bar requires eligible price"
                )

            if self.l2.bid_liquidity is None or self.l2.ask_liquidity is None:
                raise AnalysisDatasetError(
                    "A core-eligible bar requires endpoint L2 liquidity"
                )

        if self.display_ohlc is not None:
            if price_state is not PriceBarState.ELIGIBLE:
                raise AnalysisDatasetError(
                    "Display OHLC is allowed only for eligible price bars"
                )

            if not self.display_ohlc.valid:
                raise AnalysisDatasetError(
                    "display_ohlc must have valid candle geometry"
                )

        if self.hard_discontinuity and not reasons:
            raise AnalysisDatasetError(
                "A hard discontinuity requires at least one reason"
            )

        object.__setattr__(self, "start_utc", start)
        object.__setattr__(self, "end_utc", end)
        object.__setattr__(self, "timeframe", timeframe)
        object.__setattr__(self, "price_state", price_state)
        object.__setattr__(self, "discontinuity_reasons", reasons)

    @property
    def bid_liquidity(self) -> Decimal | None:
        return self.l2.bid_liquidity

    @property
    def ask_liquidity(self) -> Decimal | None:
        return self.l2.ask_liquidity

    @property
    def total_liquidity(self) -> Decimal | None:
        return self.l2.total_liquidity

    def bid_ask_imbalance(
        self,
        *,
        decimal_precision: int = 34,
    ) -> Decimal | None:
        return self.l2.bid_ask_imbalance(
            decimal_precision=decimal_precision,
        )


@dataclass(frozen=True, slots=True)
class AnalysisSegment:
    """One independently analyzable contiguous eligible core."""

    core_start_index: int
    core_end_index: int
    context_start_index: int
    context_end_index: int

    def __post_init__(self) -> None:
        for field_name in (
            "core_start_index",
            "core_end_index",
            "context_start_index",
            "context_end_index",
        ):
            _nonnegative_integer(
                field_name,
                getattr(self, field_name),
            )

        if self.core_end_index < self.core_start_index:
            raise AnalysisDatasetError("core_end_index cannot precede core_start_index")

        if self.context_start_index > self.core_start_index:
            raise AnalysisDatasetError(
                "context_start_index cannot follow core_start_index"
            )

        if self.context_end_index < self.core_end_index:
            raise AnalysisDatasetError(
                "context_end_index cannot precede core_end_index"
            )


@dataclass(frozen=True, slots=True)
class AnalysisDiscontinuity:
    """One explicit run excluded from continuous candidate detection."""

    start_index: int
    end_index: int
    start_utc: datetime
    end_utc: datetime
    reasons: tuple[AnalysisDiscontinuityReason, ...]

    def __post_init__(self) -> None:
        _nonnegative_integer("start_index", self.start_index)
        _nonnegative_integer("end_index", self.end_index)

        if self.end_index < self.start_index:
            raise AnalysisDatasetError(
                "Discontinuity end_index cannot precede start_index"
            )

        start = require_aware_utc("start_utc", self.start_utc)
        end = require_aware_utc("end_utc", self.end_utc)

        if end <= start:
            raise AnalysisDatasetError("Discontinuity end_utc must follow start_utc")

        reasons = _canonical_reasons(self.reasons)

        if not reasons:
            raise AnalysisDatasetError("A discontinuity requires at least one reason")

        object.__setattr__(self, "start_utc", start)
        object.__setattr__(self, "end_utc", end)
        object.__setattr__(self, "reasons", reasons)

    @property
    def skipped_seconds(self) -> int:
        return int((self.end_utc - self.start_utc).total_seconds())


@dataclass(frozen=True, slots=True)
class TimeframeAnalysisSeries:
    """Aligned bars, segments, and discontinuities for one timeframe."""

    snapped_range: SnappedAnalysisRange
    bars: tuple[AlignedAnalysisBar, ...]
    segments: tuple[AnalysisSegment, ...]
    discontinuities: tuple[AnalysisDiscontinuity, ...]

    def __post_init__(self) -> None:
        bars = tuple(self.bars)
        segments = tuple(self.segments)
        discontinuities = tuple(self.discontinuities)

        object.__setattr__(self, "bars", bars)
        object.__setattr__(self, "segments", segments)
        object.__setattr__(
            self,
            "discontinuities",
            discontinuities,
        )

        if len(bars) != self.snapped_range.bar_count:
            raise AnalysisDatasetError("Aligned bar count does not match snapped range")

        for expected_index, bar in enumerate(bars):
            if bar.bucket_index != expected_index:
                raise AnalysisDatasetError("Aligned bars are not in bucket-index order")

            expected_start = (
                self.snapped_range.start_utc
                + expected_index * self.snapped_range.timeframe.duration
            )

            if bar.start_utc != expected_start:
                raise AnalysisDatasetError(
                    "Aligned bar start violates snapped range ownership"
                )

        for segment in segments:
            if segment.context_end_index >= len(bars):
                raise AnalysisDatasetError(
                    "Analysis segment exceeds available bar range"
                )

            for index in range(
                segment.core_start_index,
                segment.core_end_index + 1,
            ):
                if not bars[index].core_eligible:
                    raise AnalysisDatasetError(
                        "Analysis segment core contains an ineligible bar"
                    )

    @property
    def timeframe(self) -> Timeframe:
        return self.snapped_range.timeframe


@dataclass(frozen=True, slots=True)
class AnalysisDatasetProvenance:
    """Canonical data/request identity for one aligned analysis dataset."""

    request: AnalysisDatasetRequest
    chart_range: SnappedAnalysisRange
    activity_range: SnappedAnalysisRange
    coverage: tuple[AnalysisHourCoverage, ...]
    observation_content_sha256: str

    schema: str = ANALYSIS_DATASET_SCHEMA
    schema_version: int = ANALYSIS_DATASET_SCHEMA_VERSION
    software_version: str = __version__

    def __post_init__(self) -> None:
        if self.schema != ANALYSIS_DATASET_SCHEMA:
            raise AnalysisDatasetError("Unsupported analysis dataset schema identity")

        if self.schema_version != ANALYSIS_DATASET_SCHEMA_VERSION:
            raise AnalysisDatasetError("Unsupported analysis dataset schema version")

        coverage = tuple(
            sorted(
                self.coverage,
                key=lambda item: item.hour_utc,
            )
        )

        if any(not isinstance(item, AnalysisHourCoverage) for item in coverage):
            raise AnalysisDatasetError(
                "coverage must contain AnalysisHourCoverage objects"
            )

        hours = [item.hour_utc for item in coverage]

        if len(set(hours)) != len(hours):
            raise AnalysisDatasetError("coverage contains a duplicate UTC hour")

        observation_digest = _canonical_sha256(
            "observation_content_sha256",
            self.observation_content_sha256,
        )

        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(
            self,
            "observation_content_sha256",
            observation_digest,
        )

    def to_canonical_dict(self) -> dict[str, object]:
        bounds = self.request.price_bounds

        return {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "software_version": self.software_version,
            "base": self.request.base,
            "preset_hash": self.request.preset_hash,
            "requested_range": {
                "start_utc": _utc_text(self.request.requested_start_utc),
                "end_utc": _utc_text(self.request.requested_end_utc),
                "endpoint_policy": "closed_permissive",
            },
            "activity": {
                "timeframe": self.activity_range.timeframe.label,
                "start_utc": _utc_text(self.activity_range.start_utc),
                "end_utc": _utc_text(self.activity_range.end_utc),
            },
            "chart": {
                "timeframe": self.chart_range.timeframe.label,
                "start_utc": _utc_text(self.chart_range.start_utc),
                "end_utc": _utc_text(self.chart_range.end_utc),
                "maximum_bars": self.request.maximum_chart_bars,
                "selection_policy": (
                    "explicit"
                    if self.request.chart_timeframe_override is not None
                    else "automatic_finest_within_maximum_bars"
                ),
                "requested_timeframe": (
                    self.request.chart_timeframe_override.label
                    if self.request.chart_timeframe_override is not None
                    else None
                ),
            },
            "price_filter": {
                "min_price": _canonical_decimal_text(bounds.min_price),
                "max_price": _canonical_decimal_text(bounds.max_price),
                "encoding": "canonical_decimal_string",
                "eligibility": "true_ohlc_intersection",
                "display_policy": "clip_eligible_only",
            },
            "context": {
                "before": self.request.context_before,
                "after": self.request.context_after,
            },
            "observation_content_sha256": (self.observation_content_sha256),
            "coverage": [item.to_dict() for item in self.coverage],
        }

    @property
    def canonical_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_canonical_dict(),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @property
    def analysis_id(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class AlignedAnalysisDataset:
    """Verified activity/chart series derived from one immutable request."""

    request: AnalysisDatasetRequest
    activity: TimeframeAnalysisSeries
    chart: TimeframeAnalysisSeries
    coverage: tuple[AnalysisHourCoverage, ...]
    provenance: AnalysisDatasetProvenance

    def __post_init__(self) -> None:
        if self.activity.timeframe != self.request.activity_timeframe:
            raise AnalysisDatasetError(
                "Activity series timeframe does not match request"
            )

        if self.provenance.request != self.request:
            raise AnalysisDatasetError(
                "Dataset provenance request does not match dataset request"
            )

        if self.provenance.activity_range != self.activity.snapped_range:
            raise AnalysisDatasetError(
                "Dataset provenance activity range does not match series"
            )

        if self.provenance.chart_range != self.chart.snapped_range:
            raise AnalysisDatasetError(
                "Dataset provenance chart range does not match series"
            )

        if tuple(self.coverage) != self.provenance.coverage:
            raise AnalysisDatasetError("Dataset coverage does not match provenance")

    @property
    def analysis_id(self) -> str:
        return self.provenance.analysis_id


def decode_l2_hour_to_seconds(
    hour: PersistedL2HourlySeries,
) -> tuple[L2Second, ...]:
    """Decode one verified compact L2 hour into timestamp-owned seconds."""
    decoded = decode_hourly_liquidity_blocks(hour.encoded)

    return tuple(
        L2Second(
            timestamp_utc=(hour.hour_utc + timedelta(seconds=index)),
            quality=decoded.quality[index],
            invalid_reason=decoded.invalid_reason[index],
            bid_liquidity=decoded.bid_liquidity[index],
            ask_liquidity=decoded.ask_liquidity[index],
            source_count=decoded.source_count[index],
        )
        for index in range(decoded.observation_count)
    )


def decode_price_hour_to_seconds(
    hour: PersistedPriceHourlySeries,
) -> tuple[PriceSecond, ...]:
    """Decode one verified compact price hour into timestamp-owned seconds."""
    decoded = decode_hourly_trade_ohlc_blocks(hour.encoded)

    return tuple(
        PriceSecond(
            timestamp_utc=(hour.hour_utc + timedelta(seconds=index)),
            quality=decoded.quality[index],
            invalid_reason=decoded.invalid_reason[index],
            open=decoded.open[index],
            high=decoded.high[index],
            low=decoded.low[index],
            close=decoded.close[index],
            trade_count=decoded.trade_count[index],
        )
        for index in range(decoded.observation_count)
    )


def _timestamp_belongs_to_any_snapped_range(
    timestamp_utc: datetime,
    ranges: tuple[SnappedAnalysisRange, ...],
) -> bool:
    timestamp = require_aware_utc(
        "timestamp_utc",
        timestamp_utc,
    )

    return any(item.start_utc <= timestamp < item.end_utc for item in ranges)


def _aggregate_series_to_l2_seconds(
    series,
    *,
    required_ranges: tuple[SnappedAnalysisRange, ...] | None = None,
) -> tuple[L2Second, ...]:
    """Convert aggregate coverage semantics into analysis L2 observations.

    When ``required_ranges`` is supplied, partial market coverage is rejected
    only when it can affect the effective activity or chart series. Callers
    omitting the argument retain the strict whole-series validation behavior.
    """

    normalized_required_ranges = (
        tuple(required_ranges) if required_ranges is not None else None
    )

    if normalized_required_ranges is not None:
        if not normalized_required_ranges:
            raise AnalysisDatasetError("required_ranges cannot be empty when supplied")

        if any(
            not isinstance(item, SnappedAnalysisRange)
            for item in normalized_required_ranges
        ):
            raise TypeError("required_ranges must contain SnappedAnalysisRange objects")

    values: list[L2Second] = []

    for observation in series.observations:
        if observation.quality_state is AggregateL2QualityState.INVALID:
            if observation.invalid_reason is not (
                AggregateL2InvalidReason.NO_VALID_MARKETS
            ):
                raise AnalysisDatasetError("Unsupported aggregate L2 invalid reason")

            values.append(
                L2Second(
                    timestamp_utc=observation.timestamp_utc,
                    quality=BookSampleQuality.INVALID,
                    invalid_reason=(BookSampleInvalidReason.UNINITIALIZED),
                    bid_liquidity=None,
                    ask_liquidity=None,
                    source_count=0,
                    coverage_degraded=False,
                )
            )
            continue

        if observation.quality_state is AggregateL2QualityState.DEGRADED:
            coverage_is_required = bool(
                normalized_required_ranges is None
                or _timestamp_belongs_to_any_snapped_range(
                    observation.timestamp_utc,
                    normalized_required_ranges,
                )
            )

            if coverage_is_required:
                contributing_markets = {
                    contribution.market.identity_tuple
                    for contribution in observation.contributions
                }
                expected_markets = {
                    market.identity_tuple for market in series.expected_markets
                }
                missing_markets = sorted(
                    "/".join(identity)
                    for identity in expected_markets - contributing_markets
                )

                raise AnalysisDatasetError(
                    "Multi-market Analysis requires every expected market to "
                    "contribute throughout the effective Analysis ranges. "
                    "Partial market coverage was observed at "
                    f"{observation.timestamp_utc.isoformat()}; "
                    f"missing markets={missing_markets}"
                )

        if observation.bid_liquidity is None or observation.ask_liquidity is None:
            raise AnalysisDatasetError(
                "Usable aggregate L2 observation lacks liquidity"
            )

        values.append(
            L2Second(
                timestamp_utc=observation.timestamp_utc,
                quality=BookSampleQuality.VALID,
                invalid_reason=None,
                bid_liquidity=observation.bid_liquidity,
                ask_liquidity=observation.ask_liquidity,
                source_count=observation.source_count,
                coverage_degraded=(
                    observation.quality_state is AggregateL2QualityState.DEGRADED
                ),
            )
        )

    return tuple(values)


def _aggregate_hour_content_sha256(
    *,
    aggregate_preset_hash: str,
    hour_utc: datetime,
    market_coverage: tuple[AnalysisMarketCoverage, ...],
) -> str | None:
    """Hash one hour's exact aggregate component ownership."""

    if not any(item.present for item in market_coverage):
        return None

    payload = {
        "schema": "l2shock.aggregate_l2_hour_identity",
        "schema_version": 1,
        "algorithm_version": AGGREGATE_L2_ALGORITHM_VERSION,
        "aggregate_preset_hash": aggregate_preset_hash,
        "hour_utc": _utc_text(hour_utc),
        "markets": [item.to_dict() for item in market_coverage],
    }

    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest()


def _price_ohlc(
    bar: AggregatedPriceBar,
) -> OHLC | None:
    if bar.open is None or bar.high is None or bar.low is None or bar.close is None:
        return None

    try:
        result = OHLC(
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
        )
    except ValueError:
        return None

    return result if result.valid else None


def _bar_reasons(
    l2: AggregatedL2Bar,
    price: AggregatedPriceBar,
    price_state: PriceBarState,
) -> tuple[AnalysisDiscontinuityReason, ...]:
    reasons: list[AnalysisDiscontinuityReason] = []

    if price_state is PriceBarState.EXCLUDED:
        reasons.append(AnalysisDiscontinuityReason.PRICE_EXCLUDED)

    if (
        price_state is PriceBarState.INVALID
        or price.quality is AnalysisBarQuality.INVALID
        or price.open is None
    ):
        reasons.append(AnalysisDiscontinuityReason.PRICE_INVALID)

    if price.hard_discontinuity:
        reasons.append(AnalysisDiscontinuityReason.PRICE_COVERAGE_GAP)

    if (
        l2.quality is AnalysisBarQuality.INVALID
        or l2.bid_liquidity is None
        or l2.ask_liquidity is None
    ):
        reasons.append(AnalysisDiscontinuityReason.L2_INVALID)

    if l2.hard_discontinuity:
        reasons.append(AnalysisDiscontinuityReason.L2_COVERAGE_GAP)

    return _canonical_reasons(reasons)


def _align_bars(
    l2_bars: tuple[AggregatedL2Bar, ...],
    price_bars: tuple[AggregatedPriceBar, ...],
    *,
    bounds: PriceBounds,
) -> tuple[AlignedAnalysisBar, ...]:
    if len(l2_bars) != len(price_bars):
        raise AnalysisDatasetError("Aggregated L2 and price bar counts disagree")

    aligned: list[AlignedAnalysisBar] = []

    for l2, price in zip(
        l2_bars,
        price_bars,
        strict=True,
    ):
        if (
            l2.bucket_index != price.bucket_index
            or l2.start_utc != price.start_utc
            or l2.end_utc != price.end_utc
            or l2.timeframe != price.timeframe
        ):
            raise AnalysisDatasetError(
                "Aggregated L2 and price bucket identities disagree"
            )

        raw_ohlc = _price_ohlc(price)

        if raw_ohlc is None:
            price_state = PriceBarState.INVALID
            display_ohlc = None
        else:
            price_state = classify_price_bar(
                raw_ohlc,
                bounds,
            )

            display_ohlc = (
                clip_ohlc_for_display(
                    raw_ohlc,
                    bounds,
                )
                if price_state is PriceBarState.ELIGIBLE
                else None
            )

        hard_discontinuity = bool(
            l2.hard_discontinuity
            or price.hard_discontinuity
            or l2.quality is AnalysisBarQuality.INVALID
            or price.quality is AnalysisBarQuality.INVALID
        )

        core_eligible = bool(
            price_state is PriceBarState.ELIGIBLE
            and not hard_discontinuity
            and l2.bid_liquidity is not None
            and l2.ask_liquidity is not None
        )

        reasons = _bar_reasons(
            l2,
            price,
            price_state,
        )

        aligned.append(
            AlignedAnalysisBar(
                bucket_index=l2.bucket_index,
                start_utc=l2.start_utc,
                end_utc=l2.end_utc,
                timeframe=l2.timeframe,
                l2=l2,
                price=price,
                price_state=price_state,
                display_ohlc=display_ohlc,
                core_eligible=core_eligible,
                hard_discontinuity=hard_discontinuity,
                discontinuity_reasons=reasons,
            )
        )

    return tuple(aligned)


def _bars_are_adjacent(
    first: AlignedAnalysisBar,
    second: AlignedAnalysisBar,
) -> bool:
    return first.end_utc == second.start_utc


def _context_eligible(
    bar: AlignedAnalysisBar,
) -> bool:
    return bool(
        not bar.core_eligible
        and not bar.hard_discontinuity
        and bar.price_state is not PriceBarState.INVALID
        and bar.l2.bid_liquidity is not None
        and bar.l2.ask_liquidity is not None
    )


def _build_segments(
    bars: tuple[AlignedAnalysisBar, ...],
    *,
    context_before: int,
    context_after: int,
) -> tuple[AnalysisSegment, ...]:
    core_ranges: list[tuple[int, int]] = []
    core_start: int | None = None

    for index, bar in enumerate(bars):
        continuation = bool(
            bar.core_eligible
            and (
                index == 0
                or (
                    bars[index - 1].core_eligible
                    and _bars_are_adjacent(
                        bars[index - 1],
                        bar,
                    )
                )
            )
        )

        if continuation:
            if core_start is None:
                core_start = index
            continue

        if core_start is not None:
            core_ranges.append(
                (
                    core_start,
                    index - 1,
                )
            )
            core_start = None

        if bar.core_eligible:
            core_start = index

    if core_start is not None:
        core_ranges.append(
            (
                core_start,
                len(bars) - 1,
            )
        )

    segments: list[AnalysisSegment] = []

    for start, end in core_ranges:
        context_start = start
        context_end = end

        remaining = context_before

        while context_start > 0 and remaining > 0:
            candidate_index = context_start - 1
            candidate = bars[candidate_index]

            if not _context_eligible(candidate):
                break

            if not _bars_are_adjacent(
                candidate,
                bars[context_start],
            ):
                break

            context_start = candidate_index
            remaining -= 1

        remaining = context_after

        while context_end + 1 < len(bars) and remaining > 0:
            candidate_index = context_end + 1
            candidate = bars[candidate_index]

            if not _context_eligible(candidate):
                break

            if not _bars_are_adjacent(
                bars[context_end],
                candidate,
            ):
                break

            context_end = candidate_index
            remaining -= 1

        segments.append(
            AnalysisSegment(
                core_start_index=start,
                core_end_index=end,
                context_start_index=context_start,
                context_end_index=context_end,
            )
        )

    return tuple(segments)


def _build_discontinuities(
    bars: tuple[AlignedAnalysisBar, ...],
) -> tuple[AnalysisDiscontinuity, ...]:
    result: list[AnalysisDiscontinuity] = []
    run_start: int | None = None
    run_reasons: set[AnalysisDiscontinuityReason] = set()

    def finish(end_index: int) -> None:
        nonlocal run_start
        nonlocal run_reasons

        if run_start is None:
            return

        result.append(
            AnalysisDiscontinuity(
                start_index=run_start,
                end_index=end_index,
                start_utc=bars[run_start].start_utc,
                end_utc=bars[end_index].end_utc,
                reasons=_canonical_reasons(run_reasons),
            )
        )

        run_start = None
        run_reasons = set()

    for index, bar in enumerate(bars):
        if bar.core_eligible:
            finish(index - 1)
            continue

        if run_start is None:
            run_start = index

        run_reasons.update(bar.discontinuity_reasons)

    if run_start is not None:
        finish(len(bars) - 1)

    return tuple(result)


def _build_timeframe_series(
    request: AnalysisDatasetRequest,
    snapped: SnappedAnalysisRange,
    *,
    l2_seconds: tuple[L2Second, ...],
    price_seconds: tuple[PriceSecond, ...],
) -> TimeframeAnalysisSeries:
    l2_bars = aggregate_l2_seconds(
        l2_seconds,
        start_utc=snapped.start_utc,
        end_utc=snapped.end_utc,
        timeframe=snapped.timeframe,
    )
    price_bars = aggregate_price_seconds(
        price_seconds,
        start_utc=snapped.start_utc,
        end_utc=snapped.end_utc,
        timeframe=snapped.timeframe,
    )

    bars = _align_bars(
        l2_bars,
        price_bars,
        bounds=request.price_bounds,
    )

    return TimeframeAnalysisSeries(
        snapped_range=snapped,
        bars=bars,
        segments=_build_segments(
            bars,
            context_before=request.context_before,
            context_after=request.context_after,
        ),
        discontinuities=_build_discontinuities(bars),
    )


def build_aligned_analysis_dataset(
    request: AnalysisDatasetRequest,
    *,
    l2_seconds: Iterable[L2Second],
    price_seconds: Iterable[PriceSecond],
    coverage: Iterable[AnalysisHourCoverage] = (),
) -> AlignedAnalysisDataset:
    """Build aligned activity/chart series from decoded one-second inputs."""
    if not isinstance(request, AnalysisDatasetRequest):
        raise TypeError("request must be AnalysisDatasetRequest")

    l2_values = tuple(l2_seconds)
    price_values = tuple(price_seconds)
    coverage_values = tuple(
        sorted(
            coverage,
            key=lambda item: item.hour_utc,
        )
    )

    activity_range = snap_closed_analysis_range(
        request.requested_start_utc,
        request.requested_end_utc,
        request.activity_timeframe,
    )

    chart_timeframe = _resolved_chart_timeframe(request)

    chart_range = snap_closed_analysis_range(
        request.requested_start_utc,
        request.requested_end_utc,
        chart_timeframe,
    )

    effective_ranges = (
        activity_range,
        chart_range,
    )

    if any(
        observation.coverage_degraded
        and _timestamp_belongs_to_any_snapped_range(
            observation.timestamp_utc,
            effective_ranges,
        )
        for observation in l2_values
    ):
        raise AnalysisDatasetError(
            "Partial multi-market L2 observations cannot be used for "
            "Liquidity Movement analysis. Every expected market must "
            "contribute throughout the effective activity and chart ranges."
        )

    activity = _build_timeframe_series(
        request,
        activity_range,
        l2_seconds=l2_values,
        price_seconds=price_values,
    )
    chart = _build_timeframe_series(
        request,
        chart_range,
        l2_seconds=l2_values,
        price_seconds=price_values,
    )

    provenance = AnalysisDatasetProvenance(
        request=request,
        chart_range=chart_range,
        activity_range=activity_range,
        coverage=coverage_values,
        observation_content_sha256=(
            _observation_content_sha256(
                l2_values,
                price_values,
            )
        ),
    )

    return AlignedAnalysisDataset(
        request=request,
        activity=activity,
        chart=chart,
        coverage=coverage_values,
        provenance=provenance,
    )


def _covering_hour_bounds(
    start_utc: datetime,
    end_utc: datetime,
) -> tuple[datetime, datetime]:
    start = require_aware_utc("start_utc", start_utc)
    end = require_aware_utc("end_utc", end_utc)

    if end <= start:
        raise AnalysisDatasetError("Loading end_utc must be after start_utc")

    first_hour = floor_to_hour(start)
    end_floor = floor_to_hour(end)

    if end == end_floor:
        final_exclusive_hour = end_floor
    else:
        final_exclusive_hour = end_floor + timedelta(hours=1)

    if final_exclusive_hour <= first_hour:
        final_exclusive_hour = first_hour + timedelta(hours=1)

    return first_hour, final_exclusive_hour


def load_verified_analysis_dataset(
    session: Session,
    request: AnalysisDatasetRequest,
) -> AlignedAnalysisDataset:
    """Load verified compact rows and construct aligned segmented datasets.

    A one-market preset reads its ordinary persisted L2 rows directly.

    A multi-market preset derives exact component preset hashes, loads every
    component independently through the verified analytical repository, and
    aggregates valid component observations at analysis time.
    """

    if not isinstance(session, Session):
        raise TypeError("session must be a SQLAlchemy Session")

    if not isinstance(request, AnalysisDatasetRequest):
        raise TypeError("request must be AnalysisDatasetRequest")

    activity_range = snap_closed_analysis_range(
        request.requested_start_utc,
        request.requested_end_utc,
        request.activity_timeframe,
    )
    chart_timeframe = _resolved_chart_timeframe(request)

    chart_range = snap_closed_analysis_range(
        request.requested_start_utc,
        request.requested_end_utc,
        chart_timeframe,
    )

    load_start = min(
        activity_range.start_utc,
        chart_range.start_utc,
    )
    load_end = max(
        activity_range.end_utc,
        chart_range.end_utc,
    )

    query_start, query_end = _covering_hour_bounds(
        load_start,
        load_end,
    )

    analytical_repository = AnalyticalRepository(session)
    stored_preset = analytical_repository.get_preset(request.preset_hash)

    if stored_preset is None:
        raise AnalysisDatasetError("Requested data preset does not exist")

    if stored_preset.base != request.base:
        raise AnalysisDatasetError("Requested preset base does not match Analysis base")

    try:
        semantic_preset = liquidity_data_preset_from_canonical_dict(
            stored_preset.config_json
        )
    except Exception as exc:
        raise AnalysisDatasetError(
            "Persisted data preset failed semantic verification"
        ) from exc

    if semantic_preset.preset_hash != request.preset_hash:
        raise AnalysisDatasetError(
            "Persisted data preset content does not match its hash"
        )

    component_presets = component_data_presets(semantic_preset)

    price_hours = PriceAnalyticalRepository(session).list_price_hours(
        base=request.base,
        start_utc=query_start,
        end_utc=query_end,
        verify_codec=True,
    )
    price_by_hour = {hour.hour_utc: hour for hour in price_hours}
    price_seconds = tuple(
        observation
        for hour in price_hours
        for observation in decode_price_hour_to_seconds(hour)
    )

    market_coverage_by_hour: dict[
        datetime,
        tuple[AnalysisMarketCoverage, ...],
    ] = {}

    if len(component_presets) == 1:
        component = component_presets[0]

        l2_hours = analytical_repository.list_l2_hours(
            base=request.base,
            preset_hash=component.preset_hash,
            start_utc=query_start,
            end_utc=query_end,
            verify_codec=True,
        )
        l2_by_hour = {hour.hour_utc: hour for hour in l2_hours}
        l2_seconds = tuple(
            observation
            for hour in l2_hours
            for observation in decode_l2_hour_to_seconds(hour)
        )

    else:
        component_rows: dict[
            AggregateMarketKey,
            tuple[PersistedL2HourlySeries, ...],
        ] = {}
        component_by_market: dict[
            AggregateMarketKey,
            object,
        ] = {}

        for component in component_presets:
            market = AggregateMarketKey.from_eligible_market(
                component.eligible_markets[0]
            )
            rows = analytical_repository.list_l2_hours(
                base=request.base,
                preset_hash=component.preset_hash,
                start_utc=query_start,
                end_utc=query_end,
                verify_codec=True,
            )

            component_rows[market] = rows
            component_by_market[market] = component

        expected_markets = tuple(sorted(component_rows))
        aggregate_series_inputs: list[AggregateMarketSeries] = []

        for market in expected_markets:
            component = component_by_market[market]
            rows = component_rows[market]

            aggregate_series_inputs.append(
                AggregateMarketSeries(
                    market=market,
                    component_preset_hash=(component.preset_hash),
                    l2_content_sha256_by_hour={
                        row.hour_utc: row.encoded.content_sha256 for row in rows
                    },
                    observations=tuple(
                        observation
                        for row in rows
                        for observation in decode_l2_hour_to_seconds(row)
                    ),
                )
            )

        aggregate_series = aggregate_market_l2_seconds(
            expected_markets=expected_markets,
            market_series=tuple(aggregate_series_inputs),
            start_utc=query_start,
            end_utc=query_end,
        )
        l2_seconds = _aggregate_series_to_l2_seconds(
            aggregate_series,
            required_ranges=(
                activity_range,
                chart_range,
            ),
        )
        l2_by_hour = {}

        current_hour = query_start

        while current_hour < query_end:
            items: list[AnalysisMarketCoverage] = []

            for market in expected_markets:
                component = component_by_market[market]
                row = next(
                    (
                        candidate
                        for candidate in component_rows[market]
                        if candidate.hour_utc == current_hour
                    ),
                    None,
                )

                items.append(
                    AnalysisMarketCoverage(
                        provider=market.provider,
                        venue=market.venue,
                        instrument=market.instrument,
                        component_preset_hash=(component.preset_hash),
                        content_sha256=(
                            row.encoded.content_sha256 if row is not None else None
                        ),
                    )
                )

            market_coverage_by_hour[current_hour] = tuple(items)
            current_hour += timedelta(hours=1)

    coverage: list[AnalysisHourCoverage] = []
    current = query_start

    while current < query_end:
        price_hour = price_by_hour.get(current)

        if len(component_presets) == 1:
            l2_hour = l2_by_hour.get(current)

            coverage.append(
                AnalysisHourCoverage(
                    hour_utc=current,
                    l2_content_sha256=(
                        l2_hour.encoded.content_sha256 if l2_hour is not None else None
                    ),
                    price_content_sha256=(
                        price_hour.encoded.content_sha256
                        if price_hour is not None
                        else None
                    ),
                )
            )
        else:
            market_coverage = market_coverage_by_hour[current]

            coverage.append(
                AnalysisHourCoverage(
                    hour_utc=current,
                    l2_content_sha256=(
                        _aggregate_hour_content_sha256(
                            aggregate_preset_hash=(request.preset_hash),
                            hour_utc=current,
                            market_coverage=market_coverage,
                        )
                    ),
                    price_content_sha256=(
                        price_hour.encoded.content_sha256
                        if price_hour is not None
                        else None
                    ),
                    l2_market_coverage=market_coverage,
                )
            )

        current += timedelta(hours=1)

    return build_aligned_analysis_dataset(
        request,
        l2_seconds=l2_seconds,
        price_seconds=price_seconds,
        coverage=coverage,
    )


__all__ = [
    "ANALYSIS_DATASET_SCHEMA",
    "ANALYSIS_DATASET_SCHEMA_VERSION",
    "AlignedAnalysisBar",
    "AlignedAnalysisDataset",
    "AnalysisDatasetError",
    "AnalysisDatasetProvenance",
    "AnalysisDatasetRequest",
    "AnalysisDiscontinuity",
    "AnalysisDiscontinuityReason",
    "AnalysisHourCoverage",
    "AnalysisMarketCoverage",
    "AnalysisSegment",
    "TimeframeAnalysisSeries",
    "build_aligned_analysis_dataset",
    "decode_l2_hour_to_seconds",
    "decode_price_hour_to_seconds",
    "load_verified_analysis_dataset",
]

# l2shock/analysis/shock_dataset.py
"""Verified, price-independent one-second L2 input for Shock-Start Analysis.

This module does not read price rows, construct price-eligible segments,
change LM, or write to the database. Missing L2 seconds remain explicit
invalid observations; no liquidity value is forward-filled.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from sqlalchemy.orm import Session

from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.dataset import decode_l2_hour_to_seconds
from l2shock.analysis.multi_market import (
    AGGREGATE_L2_ALGORITHM_VERSION,
    AggregateL2QualityState,
    AggregateMarketKey,
    AggregateMarketSeries,
    aggregate_market_l2_seconds,
)
from l2shock.analysis.timeframes import (
    get_timeframe,
    snap_closed_analysis_range,
)
from l2shock.db.analytical_repository import AnalyticalRepository
from l2shock.ingest.sampling import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.presets import (
    component_data_presets,
    liquidity_data_preset_from_canonical_dict,
)
from l2shock.timeutils import floor_to_hour

_HOUR = timedelta(hours=1)
_SECOND = timedelta(seconds=1)


class ShockDatasetError(ValueError):
    """Requested shock input, verified ownership, or L2 coverage is invalid."""


@dataclass(frozen=True, slots=True)
class ShockDatasetRequest:
    base: str
    preset_hash: str
    requested_start_utc: datetime
    requested_end_utc: datetime

    def __post_init__(self) -> None:
        if self.base not in {"BTC", "ETH"}:
            raise ShockDatasetError("base must be BTC or ETH")
        if (
            not isinstance(self.preset_hash, str)
            or len(self.preset_hash) != 64
            or any(c not in "0123456789abcdef" for c in self.preset_hash)
        ):
            raise ShockDatasetError("preset_hash must be a lowercase SHA-256")

        # Use the project's established closed-endpoint, snapped-range
        # validation, including aware-UTC checks.
        snap_closed_analysis_range(
            self.requested_start_utc,
            self.requested_end_utc,
            get_timeframe("1s"),
        )


@dataclass(frozen=True, slots=True)
class ShockComponentHour:
    """An expected component's actual row hash, or None for a missing hour."""

    provider: str
    venue: str
    instrument: str
    component_preset_hash: str
    hour_utc: datetime
    content_sha256: str | None


@dataclass(frozen=True, slots=True)
class VerifiedShockDataset:
    request: ShockDatasetRequest
    start_utc: datetime
    end_utc: datetime
    seconds: tuple[L2Second, ...]
    component_hours: tuple[ShockComponentHour, ...]
    input_id: str

    @property
    def valid_second_count(self) -> int:
        return sum(second.quality is BookSampleQuality.VALID for second in self.seconds)

    @property
    def invalid_second_count(self) -> int:
        return len(self.seconds) - self.valid_second_count


class _L2Repository(Protocol):
    """Small seam for testing; production supplies AnalyticalRepository."""

    def get_preset(self, preset_hash: str): ...

    def list_l2_hours(
        self,
        *,
        base: str,
        preset_hash: str,
        start_utc: datetime,
        end_utc: datetime,
        verify_codec: bool = True,
    ): ...


def _missing_second(timestamp: datetime) -> L2Second:
    return L2Second(
        timestamp_utc=timestamp,
        quality=BookSampleQuality.INVALID,
        invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
        bid_liquidity=None,
        ask_liquidity=None,
        source_count=0,
    )


def _hour_bounds(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    first = floor_to_hour(start)
    last = floor_to_hour(end)
    exclusive = last if end == last else last + _HOUR
    return first, exclusive


def _selected_seconds(
    observations: tuple[L2Second, ...],
    *,
    start: datetime,
    end: datetime,
) -> tuple[L2Second, ...]:
    return tuple(
        observation
        for observation in observations
        if start <= observation.timestamp_utc < end
    )


def load_shock_dataset_from_repository(
    repository: _L2Repository,
    request: ShockDatasetRequest,
) -> VerifiedShockDataset:
    """Load verified L2 hours without asking whether price is available.

    The repository must implement the same verified read contract as
    AnalyticalRepository. In production, use load_verified_shock_dataset().
    """
    if not isinstance(request, ShockDatasetRequest):
        raise TypeError("request must be ShockDatasetRequest")

    snapped = snap_closed_analysis_range(
        request.requested_start_utc,
        request.requested_end_utc,
        get_timeframe("1s"),
    )
    start, end = snapped.start_utc, snapped.end_utc
    query_start, query_end = _hour_bounds(start, end)

    stored = repository.get_preset(request.preset_hash)
    if stored is None:
        raise ShockDatasetError("Requested data preset does not exist")
    if stored.base != request.base:
        raise ShockDatasetError("Preset base does not match shock scan base")

    try:
        semantic = liquidity_data_preset_from_canonical_dict(stored.config_json)
    except Exception as exc:
        raise ShockDatasetError(
            "Persisted data preset failed semantic verification"
        ) from exc

    if semantic.preset_hash != request.preset_hash:
        raise ShockDatasetError("Persisted preset content does not match its hash")

    components = component_data_presets(semantic)
    if not components:
        raise ShockDatasetError("Preset has no component markets")

    expected: dict[AggregateMarketKey, object] = {}
    for component in components:
        if len(component.eligible_markets) != 1:
            raise ShockDatasetError(
                "Each materialized component must own exactly one market"
            )
        market = AggregateMarketKey.from_eligible_market(component.eligible_markets[0])
        if market in expected:
            raise ShockDatasetError("Duplicate component market in preset")
        expected[market] = component

    # Exact expected hourly ownership is recorded even for absent rows.
    hours: list[datetime] = []
    hour = query_start
    while hour < query_end:
        hours.append(hour)
        hour += _HOUR

    provenance: list[ShockComponentHour] = []
    series_by_market: dict[AggregateMarketKey, tuple[L2Second, ...]] = {}
    hashes_by_market: dict[AggregateMarketKey, dict[datetime, str]] = {}

    for market, component in sorted(expected.items()):
        rows = repository.list_l2_hours(
            base=request.base,
            preset_hash=component.preset_hash,
            start_utc=query_start,
            end_utc=query_end,
            verify_codec=True,
        )
        by_hour = {}

        for row in rows:
            if (
                row.base != request.base
                or row.preset_hash != component.preset_hash
                or row.hour_utc not in hours
                or row.hour_utc in by_hour
            ):
                raise ShockDatasetError(
                    "L2 repository returned duplicate or wrong-owner hour"
                )
            by_hour[row.hour_utc] = row

        hashes: dict[datetime, str] = {}
        observations: list[L2Second] = []

        for owned_hour in hours:
            row = by_hour.get(owned_hour)
            digest = row.encoded.content_sha256 if row is not None else None

            provenance.append(
                ShockComponentHour(
                    provider=market.provider,
                    venue=market.venue,
                    instrument=market.instrument,
                    component_preset_hash=component.preset_hash,
                    hour_utc=owned_hour,
                    content_sha256=digest,
                )
            )

            if row is None:
                continue

            hashes[owned_hour] = digest
            decoded = decode_l2_hour_to_seconds(row)

            # The repository verifies the codec; independently enforce this
            # loader's exact timestamp and slot-ownership assumptions.
            if len(decoded) != 3_600 or any(
                item.timestamp_utc != owned_hour + timedelta(seconds=index)
                for index, item in enumerate(decoded)
            ):
                raise ShockDatasetError(
                    "Verified L2 hour has unexpected one-second ownership"
                )
            observations.extend(_selected_seconds(decoded, start=start, end=end))

        hashes_by_market[market] = hashes
        series_by_market[market] = tuple(observations)

    if len(expected) == 1:
        market = next(iter(expected))
        by_second = {item.timestamp_utc: item for item in series_by_market[market]}
        seconds: list[L2Second] = []
        timestamp = start

        while timestamp < end:
            seconds.append(by_second.get(timestamp, _missing_second(timestamp)))
            timestamp += _SECOND

    else:
        ordered_markets = tuple(sorted(expected))
        aggregate = aggregate_market_l2_seconds(
            expected_markets=ordered_markets,
            market_series=tuple(
                AggregateMarketSeries(
                    market=market,
                    component_preset_hash=expected[market].preset_hash,
                    l2_content_sha256_by_hour=hashes_by_market[market],
                    observations=series_by_market[market],
                )
                for market in ordered_markets
            ),
            start_utc=start,
            end_utc=end,
        )

        seconds = []

        for item in aggregate.observations:
            if item.quality_state is AggregateL2QualityState.DEGRADED:
                raise ShockDatasetError(
                    "Multi-market shock scan requires every expected market "
                    "at every numerically usable second; partial coverage at "
                    f"{item.timestamp_utc.isoformat()}"
                )

            if item.quality_state is AggregateL2QualityState.INVALID:
                seconds.append(_missing_second(item.timestamp_utc))
                continue

            if item.quality_state is not AggregateL2QualityState.VALID:
                raise ShockDatasetError("Unknown aggregate L2 quality")

            seconds.append(
                L2Second(
                    timestamp_utc=item.timestamp_utc,
                    quality=BookSampleQuality.VALID,
                    invalid_reason=None,
                    bid_liquidity=item.bid_liquidity,
                    ask_liquidity=item.ask_liquidity,
                    source_count=item.source_count,
                )
            )

    if len(seconds) != int((end - start).total_seconds()) or any(
        item.timestamp_utc != start + timedelta(seconds=index)
        for index, item in enumerate(seconds)
    ):
        raise ShockDatasetError("Shock scan does not own every selected second")

    # Identity records the selected endpoints and EVERY expected component
    # hour, including absent rows. Price and chart timeframe are absent.
    identity = {
        "schema": "l2shock.shock_l2_input",
        "schema_version": 1,
        "aggregate_l2_algorithm": AGGREGATE_L2_ALGORITHM_VERSION,
        "base": request.base,
        "preset_hash": request.preset_hash,
        "requested_start_utc": request.requested_start_utc.isoformat(),
        "requested_end_utc": request.requested_end_utc.isoformat(),
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "component_hours": [
            {
                "provider": item.provider,
                "venue": item.venue,
                "instrument": item.instrument,
                "component_preset_hash": item.component_preset_hash,
                "hour_utc": item.hour_utc.isoformat(),
                "content_sha256": item.content_sha256,
            }
            for item in provenance
        ],
    }
    input_id = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()

    return VerifiedShockDataset(
        request=request,
        start_utc=start,
        end_utc=end,
        seconds=tuple(seconds),
        component_hours=tuple(provenance),
        input_id=input_id,
    )


def load_verified_shock_dataset(
    session: Session,
    request: ShockDatasetRequest,
) -> VerifiedShockDataset:
    """Production entry: use the repository's verified PostgreSQL reads."""
    if not isinstance(session, Session):
        raise TypeError("session must be a SQLAlchemy Session")

    return load_shock_dataset_from_repository(
        AnalyticalRepository(session),
        request,
    )

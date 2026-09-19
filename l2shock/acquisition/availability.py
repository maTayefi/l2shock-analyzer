# l2shock/acquisition/availability.py
"""Hourly source and analytical availability queries.

Availability is derived from existing immutable source and analytical rows.
No new table or migration is required.

Definitions:

- source-complete:
    both orderbook and trades archives are locally available;
- materialized:
    an L2 row for the requested preset and a Binance price row exist;
- analyzable:
    both materialized rows contain at least one valid one-second observation;
- contiguous window:
    the newest uninterrupted sequence of analyzable UTC hours ending at the
    newest analyzable hour.

An all-invalid but deterministically processed hour remains materialized, but
it is not classified as analyzable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from l2shock.db.models import (
    DataPreset,
    L2HourlySeries,
    PriceHourlySeries,
    SourceHour,
)
from l2shock.filesystem import (
    OwnedPathError,
    absolute_path_without_resolution,
    require_owned_regular_file,
)
from l2shock.timeutils import require_utc_hour


class AvailabilityError(ValueError):
    """An hourly availability query violates its contract."""


class HourAvailabilityState(StrEnum):
    """Highest proven availability state for one base/hour."""

    UNKNOWN = "unknown"
    MISSING = "missing"
    PARTIAL = "partial"
    DOWNLOADED = "downloaded"
    MATERIALIZED = "materialized"
    ANALYZABLE = "analyzable"


_AVAILABLE_SOURCE_STATUSES = frozenset(
    {
        "downloaded",
        "processing",
        "processed",
    }
)


def _normalized_base(value: object) -> str:
    base = str(value or "").strip().upper()

    if base not in {"BTC", "ETH"}:
        raise AvailabilityError("base must be BTC or ETH")

    return base


def _canonical_sha256(
    field_name: str,
    value: object,
) -> str:
    text = str(value or "").strip()

    if (
        text != text.lower()
        or len(text) != 64
        or any(character not in "0123456789abcdef" for character in text)
    ):
        raise AvailabilityError(f"{field_name} must be a canonical lowercase SHA-256")

    return text


def _nonnegative_count_from_summary(
    summary: object,
    field_name: str,
) -> int:
    if not isinstance(summary, dict):
        return 0

    value = summary.get(field_name, 0)

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0

    return value


@dataclass(frozen=True, slots=True)
class HourAvailability:
    """Availability facts for one base and exact UTC hour."""

    base: str
    hour_utc: datetime
    preset_hash: str

    orderbook_status: str | None
    trades_status: str | None

    orderbook_local: bool
    trades_local: bool

    l2_materialized: bool
    price_materialized: bool

    l2_valid_seconds: int
    price_valid_seconds: int

    state: HourAvailabilityState

    expected_l2_market_count: int = 1
    materialized_l2_market_count: int | None = None
    valid_l2_market_count: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "base",
            _normalized_base(self.base),
        )
        object.__setattr__(
            self,
            "hour_utc",
            require_utc_hour(
                "hour_utc",
                self.hour_utc,
            ),
        )
        object.__setattr__(
            self,
            "preset_hash",
            _canonical_sha256(
                "preset_hash",
                self.preset_hash,
            ),
        )
        object.__setattr__(
            self,
            "state",
            HourAvailabilityState(self.state),
        )

        for field_name in (
            "l2_valid_seconds",
            "price_valid_seconds",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise AvailabilityError(f"{field_name} must be a non-negative integer")

        expected_market_count = self.expected_l2_market_count

        if (
            isinstance(expected_market_count, bool)
            or not isinstance(expected_market_count, int)
            or expected_market_count <= 0
        ):
            raise AvailabilityError(
                "expected_l2_market_count must be a positive integer"
            )

        materialized_market_count = self.materialized_l2_market_count

        if materialized_market_count is None:
            materialized_market_count = int(self.l2_materialized)

        valid_market_count = self.valid_l2_market_count

        if valid_market_count is None:
            valid_market_count = int(self.l2_valid_seconds > 0)

        for field_name, value in (
            (
                "materialized_l2_market_count",
                materialized_market_count,
            ),
            (
                "valid_l2_market_count",
                valid_market_count,
            ),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise AvailabilityError(f"{field_name} must be a non-negative integer")

            if value > expected_market_count:
                raise AvailabilityError(
                    f"{field_name} cannot exceed expected_l2_market_count"
                )

        if valid_market_count > materialized_market_count:
            raise AvailabilityError(
                "valid_l2_market_count cannot exceed " "materialized_l2_market_count"
            )

        if (
            self.state is HourAvailabilityState.ANALYZABLE
            and expected_market_count > 1
            and (
                materialized_market_count != expected_market_count
                or valid_market_count != expected_market_count
                or self.l2_valid_seconds != 3_600
            )
        ):
            raise AvailabilityError(
                "A multi-market ANALYZABLE hour requires every expected "
                "market to be materialized and valid for all 3,600 seconds"
            )

        expected_l2_materialized = bool(
            materialized_market_count == expected_market_count
        )

        if self.l2_materialized != expected_l2_materialized:
            raise AvailabilityError(
                "l2_materialized does not match complete materialized "
                "market coverage"
            )

        # For one market, l2_valid_seconds and valid market coverage describe
        # the same fact. For an aggregate preset they intentionally differ:
        #
        # - valid_market_count reports partial component diagnostics;
        # - l2_valid_seconds remains zero until every expected market has
        #   complete 3,600-second coverage suitable for Analysis handoff.
        if expected_market_count == 1 and (self.l2_valid_seconds > 0) != (
            valid_market_count > 0
        ):
            raise AvailabilityError(
                "Single-market l2_valid_seconds does not match " "valid market coverage"
            )

        object.__setattr__(
            self,
            "expected_l2_market_count",
            expected_market_count,
        )
        object.__setattr__(
            self,
            "materialized_l2_market_count",
            materialized_market_count,
        )
        object.__setattr__(
            self,
            "valid_l2_market_count",
            valid_market_count,
        )

    @property
    def source_complete(self) -> bool:
        return self.orderbook_local and self.trades_local

    @property
    def materialized(self) -> bool:
        return self.l2_materialized and self.price_materialized

    @property
    def analyzable(self) -> bool:
        return self.state is HourAvailabilityState.ANALYZABLE

    @property
    def l2_market_coverage_complete(self) -> bool:
        return bool(self.materialized_l2_market_count == self.expected_l2_market_count)

    @property
    def l2_market_coverage_degraded(self) -> bool:
        materialized = self.materialized_l2_market_count or 0

        return bool(materialized > 0 and materialized < self.expected_l2_market_count)

    @property
    def l2_valid_market_coverage_degraded(self) -> bool:
        valid = self.valid_l2_market_count or 0

        return bool(valid > 0 and valid < self.expected_l2_market_count)


@dataclass(frozen=True, slots=True)
class ContiguousAnalysisWindow:
    """Newest uninterrupted sequence of analyzable UTC hours."""

    base: str
    preset_hash: str
    start_utc: datetime
    end_utc: datetime
    hour_count: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "base",
            _normalized_base(self.base),
        )
        object.__setattr__(
            self,
            "preset_hash",
            _canonical_sha256(
                "preset_hash",
                self.preset_hash,
            ),
        )

        start = require_utc_hour("start_utc", self.start_utc)
        end = require_utc_hour("end_utc", self.end_utc)

        if end <= start:
            raise AvailabilityError("end_utc must be after start_utc")

        if (
            isinstance(self.hour_count, bool)
            or not isinstance(self.hour_count, int)
            or self.hour_count <= 0
        ):
            raise AvailabilityError("hour_count must be positive")

        if end - start != timedelta(hours=self.hour_count):
            raise AvailabilityError(
                "Contiguous window duration does not match hour_count"
            )

        object.__setattr__(self, "start_utc", start)
        object.__setattr__(self, "end_utc", end)


def enabled_preset_hashes(
    session: Session,
    *,
    base: str,
) -> tuple[str, ...]:
    """Return enabled preset hashes for one base in deterministic order."""

    normalized_base = _normalized_base(base)

    values = session.execute(
        select(DataPreset.preset_hash)
        .where(DataPreset.base == normalized_base)
        .where(DataPreset.enabled.is_(True))
        .order_by(
            DataPreset.created_at.desc(),
            DataPreset.preset_hash.asc(),
        )
    ).scalars()

    return tuple(_canonical_sha256("preset_hash", value) for value in values)


def preferred_enabled_preset_hash(
    session: Session,
    *,
    base: str,
) -> str | None:
    """Return the newest enabled semantic preset for one base."""

    values = enabled_preset_hashes(
        session,
        base=base,
    )

    return values[0] if values else None


def _combined_orderbook_status(
    values: tuple[tuple[str, bool] | None, ...],
) -> str | None:
    """Summarize component order-book source state for presentation.

    A one-market preset retains its exact durable status. A multi-market
    preset receives a stable aggregate label without pretending that one
    component status belongs to the complete preset.
    """

    if not values:
        return None

    if len(values) == 1:
        value = values[0]
        return value[0] if value is not None else None

    present = tuple(value for value in values if value is not None)

    if not present:
        return None

    if all(value[1] for value in present) and len(present) == len(values):
        return "available"

    statuses = {value[0] for value in present}

    if statuses == {"missing"} and len(present) == len(values):
        return "missing"

    return "partial"


def _local_source_archive_is_available(
    *,
    raw_root: object,
    provider: object,
    venue: object,
    data_kind: object,
    instrument: object,
    hour_utc: datetime,
    status: object,
    local_path: object,
    file_size_bytes: object,
    content_sha256: object,
) -> bool:
    """Return whether metadata is backed by the exact canonical local file.

    This is intentionally a lightweight availability check. Processing still
    owns complete SHA-256 verification immediately before consuming an archive.
    """

    from pathlib import Path

    from l2shock.acquisition.models import (
        SourceDataKind,
        SourceFileSpec,
    )

    if str(status) not in _AVAILABLE_SOURCE_STATUSES:
        return False

    stored_path_text = str(local_path or "").strip()

    if not stored_path_text:
        return False

    if (
        isinstance(file_size_bytes, bool)
        or not isinstance(file_size_bytes, int)
        or file_size_bytes <= 0
    ):
        return False

    digest = str(content_sha256 or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        return False

    try:
        spec = SourceFileSpec(
            provider=str(provider),
            venue=str(venue),
            symbol=str(instrument),
            data_kind=SourceDataKind(str(data_kind)),
            hour_utc=require_utc_hour(
                "source hour_utc",
                hour_utc,
            ),
        )

        root = absolute_path_without_resolution(
            Path(raw_root).expanduser(),
        )
        stored_path = absolute_path_without_resolution(
            Path(stored_path_text).expanduser(),
        )
        canonical_path = absolute_path_without_resolution(
            spec.local_path(root),
        )

        if stored_path != canonical_path:
            return False

        stored_path = require_owned_regular_file(
            root,
            stored_path,
        )

        return stored_path.stat().st_size == file_size_bytes

    except (
        OSError,
        OwnedPathError,
        TypeError,
        ValueError,
    ):
        return False


def _classify(
    *,
    orderbook_exists: bool,
    trades_exists: bool,
    orderbook_status: str | None,
    trades_status: str | None,
    orderbook_local: bool,
    trades_local: bool,
    l2_materialized: bool,
    price_materialized: bool,
    l2_valid_seconds: int,
    price_valid_seconds: int,
) -> HourAvailabilityState:
    source_count = int(orderbook_exists) + int(trades_exists)
    local_count = int(orderbook_local) + int(trades_local)
    materialized_count = int(l2_materialized) + int(price_materialized)

    if materialized_count == 2 and l2_valid_seconds > 0 and price_valid_seconds > 0:
        return HourAvailabilityState.ANALYZABLE

    if materialized_count == 2:
        return HourAvailabilityState.MATERIALIZED

    if local_count == 2:
        return HourAvailabilityState.DOWNLOADED

    # Partial local or analytical evidence is more informative than a known
    # remote miss for the other channel.
    if local_count > 0 or materialized_count > 0:
        return HourAvailabilityState.PARTIAL

    statuses = {
        str(value or "").strip().lower()
        for value in (
            orderbook_status,
            trades_status,
        )
        if str(value or "").strip()
    }

    if "missing" in statuses:
        return HourAvailabilityState.MISSING

    if source_count > 0:
        return HourAvailabilityState.PARTIAL

    return HourAvailabilityState.UNKNOWN


def load_hourly_availability(
    session: Session,
    *,
    base: str,
    preset_hash: str,
    start_utc: datetime,
    end_utc: datetime,
) -> tuple[HourAvailability, ...]:
    """Load availability for every exact hour in ``[start_utc, end_utc)``.

    Single-market presets resolve to their directly persisted compact L2 rows.

    Multi-market presets resolve to their deterministic component preset
    identities. An aggregate hour is analyzable only when every expected
    component row exists and every expected market contains 3,600 valid L2
    seconds. This prevents a calendar handoff from mixing different market
    compositions across the selected Analysis range.

    Missing or partially valid component markets remain visible as coverage
    diagnostics, but they do not make a multi-market hour analyzable and are
    never interpreted as zero liquidity.
    """
    # Deferred import to break circular dependency:
    # acquisition -> presets -> liquidity -> ingest -> acquisition
    from l2shock.config import get_settings
    from l2shock.presets import (
        component_data_presets,
        liquidity_data_preset_from_canonical_dict,
    )

    if not isinstance(session, Session):
        raise TypeError("load_hourly_availability requires a SQLAlchemy Session")

    raw_root = get_settings().storage.raw_path.resolve()
    normalized_base = _normalized_base(base)
    digest = _canonical_sha256("preset_hash", preset_hash)
    start = require_utc_hour("start_utc", start_utc)
    end = require_utc_hour("end_utc", end_utc)

    if end <= start:
        raise AvailabilityError("end_utc must be after start_utc")

    stored_preset = session.scalar(
        select(DataPreset).where(
            DataPreset.preset_hash == digest,
        )
    )

    if stored_preset is None:
        raise AvailabilityError("Requested data preset does not exist")

    if str(stored_preset.base) != normalized_base:
        raise AvailabilityError(
            "Requested data preset base does not match availability base"
        )

    try:
        semantic_preset = liquidity_data_preset_from_canonical_dict(
            dict(stored_preset.config_json)
        )
    except Exception as exc:
        raise AvailabilityError(
            "Persisted data preset failed semantic verification"
        ) from exc

    if semantic_preset.preset_hash != digest:
        raise AvailabilityError("Persisted data preset content does not match its hash")

    component_presets = component_data_presets(semantic_preset)
    expected_market_count = len(component_presets)

    component_by_hash = {
        component.preset_hash: component for component in component_presets
    }
    component_hashes = tuple(component_by_hash)

    expected_orderbook_identities = {
        (
            component.eligible_markets[0].venue,
            component.eligible_markets[0].instrument,
        )
        for component in component_presets
    }

    price_symbol = f"{normalized_base}USDT"

    source_venues = {venue for venue, _instrument in expected_orderbook_identities}
    source_instruments = {
        instrument for _venue, instrument in expected_orderbook_identities
    }
    source_venues.add("binance_futures")
    source_instruments.add(price_symbol)

    source_rows = session.execute(
        select(
            SourceHour.hour_utc,
            SourceHour.venue,
            SourceHour.data_kind,
            SourceHour.instrument,
            SourceHour.status,
            SourceHour.local_path,
            SourceHour.file_size_bytes,
            SourceHour.content_sha256,
        )
        .where(SourceHour.provider == "cryptohftdata")
        .where(SourceHour.venue.in_(tuple(sorted(source_venues))))
        .where(SourceHour.instrument.in_(tuple(sorted(source_instruments))))
        .where(SourceHour.hour_utc >= start)
        .where(SourceHour.hour_utc < end)
    ).all()

    source_by_identity: dict[
        tuple[datetime, str, str, str],
        tuple[str, bool],
    ] = {}

    for (
        hour_utc,
        venue,
        data_kind,
        instrument,
        status,
        local_path,
        file_size_bytes,
        content_sha256,
    ) in source_rows:
        venue_text = str(venue)
        instrument_text = str(instrument)
        data_kind_text = str(data_kind)

        is_expected_orderbook = bool(
            data_kind_text == "orderbook"
            and (
                venue_text,
                instrument_text,
            )
            in expected_orderbook_identities
        )
        is_expected_price = bool(
            data_kind_text == "trades"
            and venue_text == "binance_futures"
            and instrument_text == price_symbol
        )

        if not (is_expected_orderbook or is_expected_price):
            continue

        source_hour = require_utc_hour(
            "source hour_utc",
            hour_utc,
        )

        local = _local_source_archive_is_available(
            raw_root=raw_root,
            provider="cryptohftdata",
            venue=venue_text,
            data_kind=data_kind_text,
            instrument=instrument_text,
            hour_utc=source_hour,
            status=status,
            local_path=local_path,
            file_size_bytes=file_size_bytes,
            content_sha256=content_sha256,
        )

        source_by_identity[
            (
                source_hour,
                venue_text,
                instrument_text,
                data_kind_text,
            )
        ] = (
            str(status),
            local,
        )

    l2_rows = session.execute(
        select(
            L2HourlySeries.hour_utc,
            L2HourlySeries.preset_hash,
            L2HourlySeries.quality_summary_json,
        )
        .where(L2HourlySeries.base == normalized_base)
        .where(L2HourlySeries.preset_hash.in_(component_hashes))
        .where(L2HourlySeries.hour_utc >= start)
        .where(L2HourlySeries.hour_utc < end)
    ).all()

    l2_by_hour: dict[
        datetime,
        dict[str, object],
    ] = {}

    for hour_utc, component_hash, summary in l2_rows:
        hour_key = require_utc_hour(
            "L2 hour_utc",
            hour_utc,
        )
        normalized_component_hash = _canonical_sha256(
            "component preset_hash",
            component_hash,
        )

        if normalized_component_hash not in component_by_hash:
            continue

        l2_by_hour.setdefault(
            hour_key,
            {},
        )[normalized_component_hash] = summary

    price_rows = session.execute(
        select(
            PriceHourlySeries.hour_utc,
            PriceHourlySeries.quality_summary_json,
        )
        .where(PriceHourlySeries.base == normalized_base)
        .where(PriceHourlySeries.hour_utc >= start)
        .where(PriceHourlySeries.hour_utc < end)
    ).all()

    price_by_hour = {
        require_utc_hour("price hour_utc", hour_utc): summary
        for hour_utc, summary in price_rows
    }

    result: list[HourAvailability] = []
    hour = start

    while hour < end:
        orderbook_values = tuple(
            source_by_identity.get(
                (
                    hour,
                    component.eligible_markets[0].venue,
                    component.eligible_markets[0].instrument,
                    "orderbook",
                )
            )
            for component in component_presets
        )

        trades = source_by_identity.get(
            (
                hour,
                "binance_futures",
                price_symbol,
                "trades",
            )
        )

        orderbook_exists = any(value is not None for value in orderbook_values)
        trades_exists = trades is not None

        orderbook_status = _combined_orderbook_status(orderbook_values)
        trades_status = trades[0] if trades is not None else None

        orderbook_local = bool(
            orderbook_values
            and all(value is not None and value[1] for value in orderbook_values)
        )
        trades_local = trades[1] if trades is not None else False

        component_summaries = l2_by_hour.get(hour, {})
        materialized_market_count = len(component_summaries)

        valid_counts = tuple(
            _nonnegative_count_from_summary(
                component_summaries.get(component.preset_hash),
                "valid_count",
            )
            for component in component_presets
        )

        valid_market_count = sum(valid_count > 0 for valid_count in valid_counts)

        if expected_market_count == 1:
            # Single-market analysis retains the existing policy: at least one
            # valid second proves that some L2 analysis is possible.
            l2_valid_seconds = valid_counts[0]
        else:
            # A multi-market handoff must prove one unchanging contributor set
            # for the complete hour. Any missing or invalid component second
            # could otherwise create a false liquidity step.
            uniform_full_market_coverage = bool(
                materialized_market_count == expected_market_count
                and all(valid_count == 3_600 for valid_count in valid_counts)
            )
            l2_valid_seconds = 3_600 if uniform_full_market_coverage else 0

        price_summary = price_by_hour.get(hour)
        price_materialized = price_summary is not None
        price_valid_seconds = _nonnegative_count_from_summary(
            price_summary,
            "valid_count",
        )

        l2_materialized = bool(materialized_market_count == expected_market_count)

        state = _classify(
            orderbook_exists=orderbook_exists,
            trades_exists=trades_exists,
            orderbook_status=orderbook_status,
            trades_status=trades_status,
            orderbook_local=orderbook_local,
            trades_local=trades_local,
            l2_materialized=l2_materialized,
            price_materialized=price_materialized,
            l2_valid_seconds=l2_valid_seconds,
            price_valid_seconds=price_valid_seconds,
        )

        result.append(
            HourAvailability(
                base=normalized_base,
                hour_utc=hour,
                preset_hash=digest,
                orderbook_status=orderbook_status,
                trades_status=trades_status,
                orderbook_local=orderbook_local,
                trades_local=trades_local,
                l2_materialized=l2_materialized,
                price_materialized=price_materialized,
                l2_valid_seconds=l2_valid_seconds,
                price_valid_seconds=price_valid_seconds,
                state=state,
                expected_l2_market_count=expected_market_count,
                materialized_l2_market_count=(materialized_market_count),
                valid_l2_market_count=valid_market_count,
            )
        )

        hour += timedelta(hours=1)

    return tuple(result)


def most_recent_contiguous_analyzable_window(
    availability: Iterable[HourAvailability],
) -> ContiguousAnalysisWindow | None:
    """Return the newest backward-contiguous analyzable hour sequence."""

    values = tuple(
        sorted(
            availability,
            key=lambda item: item.hour_utc,
        )
    )

    if not values:
        return None

    for value in values:
        if not isinstance(value, HourAvailability):
            raise TypeError("availability must contain HourAvailability objects")

    analyzable = [value for value in values if value.analyzable]

    if not analyzable:
        return None

    newest = analyzable[-1]
    selected = [newest]
    expected = newest.hour_utc - timedelta(hours=1)

    by_hour = {value.hour_utc: value for value in values}

    while True:
        previous = by_hour.get(expected)

        if previous is None or not previous.analyzable:
            break

        if previous.base != newest.base or previous.preset_hash != newest.preset_hash:
            break

        selected.append(previous)
        expected -= timedelta(hours=1)

    oldest = selected[-1]

    return ContiguousAnalysisWindow(
        base=newest.base,
        preset_hash=newest.preset_hash,
        start_utc=oldest.hour_utc,
        end_utc=newest.hour_utc + timedelta(hours=1),
        hour_count=len(selected),
    )


__all__ = [
    "AvailabilityError",
    "ContiguousAnalysisWindow",
    "HourAvailability",
    "HourAvailabilityState",
    "enabled_preset_hashes",
    "load_hourly_availability",
    "most_recent_contiguous_analyzable_window",
    "preferred_enabled_preset_hash",
]

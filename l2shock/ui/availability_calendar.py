# l2shock/ui/availability_calendar.py
"""Hourly availability-calendar and Analysis-handoff contracts.

CryptoHFTData archive ownership is UTC-hour based. A configured local calendar
date does not necessarily begin on a UTC-hour boundary, particularly for
timezones such as Asia/Tehran.

The calendar therefore:

1. converts the selected local civil day to an exact UTC interval;
2. expands that interval to enclosing UTC source-hour boundaries;
3. queries exact UTC source hours;
4. retains only source hours whose local start belongs to the selected date.

No source-hour identity is shifted or relabelled as a local-hour bucket.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Final
from collections.abc import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy.orm import Session

from l2shock.acquisition import (
    ContiguousAnalysisWindow,
    HourAvailability,
    HourAvailabilityState,
    load_hourly_availability,
    most_recent_contiguous_analyzable_window,
    preferred_enabled_preset_hash,
)
from l2shock.db.engine import session_scope
from l2shock.timeutils import (
    floor_to_hour,
    local_to_utc,
    require_aware_utc,
    require_utc_hour,
    utc_to_local,
)

DEFAULT_CONTIGUOUS_LOOKBACK_HOURS: Final[int] = 31 * 24

_STATE_COLORS: Final[dict[HourAvailabilityState, str]] = {
    HourAvailabilityState.UNKNOWN: "grey-7",
    HourAvailabilityState.MISSING: "negative",
    HourAvailabilityState.PARTIAL: "warning",
    HourAvailabilityState.DOWNLOADED: "info",
    HourAvailabilityState.MATERIALIZED: "secondary",
    HourAvailabilityState.ANALYZABLE: "positive",
}

_STATE_LABELS: Final[dict[HourAvailabilityState, str]] = {
    HourAvailabilityState.UNKNOWN: "Unknown",
    HourAvailabilityState.MISSING: "Missing",
    HourAvailabilityState.PARTIAL: "Partial",
    HourAvailabilityState.DOWNLOADED: "Downloaded",
    HourAvailabilityState.MATERIALIZED: "Materialized",
    HourAvailabilityState.ANALYZABLE: "Analyzable",
}


class AvailabilityCalendarError(ValueError):
    """Availability calendar input violates its ownership contract."""


def _normalized_base(value: object) -> str:
    base = str(value or "").strip().upper()

    if base not in {"BTC", "ETH"}:
        raise AvailabilityCalendarError("base must be BTC or ETH")

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
        raise AvailabilityCalendarError(
            f"{field_name} must be a canonical lowercase SHA-256"
        )

    return text


def _positive_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AvailabilityCalendarError(f"{field_name} must be a positive integer")

    return value


def _timezone_name(value: object) -> str:
    text = str(value or "").strip()

    if not text:
        raise AvailabilityCalendarError("timezone_name cannot be blank")

    try:
        ZoneInfo(text)
    except ZoneInfoNotFoundError as exc:
        raise AvailabilityCalendarError(f"Invalid IANA timezone: {text!r}") from exc

    return text


def parse_local_calendar_date(value: date | str) -> date:
    """Parse one ISO local calendar date."""

    if isinstance(value, datetime):
        raise AvailabilityCalendarError(
            "selected_local_date must be a date, not datetime"
        )

    if isinstance(value, date):
        return value

    text = str(value or "").strip()

    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise AvailabilityCalendarError(
            "selected_local_date must use YYYY-MM-DD"
        ) from exc


def _ceil_to_hour(value_utc: datetime) -> datetime:
    value = require_aware_utc("value_utc", value_utc)
    floored = floor_to_hour(value)

    if floored == value:
        return floored

    return floored + timedelta(hours=1)


def local_date_source_hour_query_bounds(
    selected_local_date: date | str,
    *,
    timezone_name: str,
) -> tuple[datetime, datetime]:
    """Return enclosing exact UTC-hour bounds for one local civil day."""

    selected = parse_local_calendar_date(selected_local_date)
    timezone = _timezone_name(timezone_name)

    local_start = datetime.combine(
        selected,
        time.min,
    )
    local_end = datetime.combine(
        selected + timedelta(days=1),
        time.min,
    )

    start_utc = local_to_utc(
        local_start,
        timezone,
        original_text=f"{selected.isoformat()} 00:00:00",
    )
    end_utc = local_to_utc(
        local_end,
        timezone,
        original_text=(f"{(selected + timedelta(days=1)).isoformat()} 00:00:00"),
    )

    query_start = floor_to_hour(start_utc)
    query_end = _ceil_to_hour(end_utc)

    if query_end <= query_start:
        raise AvailabilityCalendarError(
            "Local calendar date produced an empty UTC-hour query"
        )

    return (
        require_utc_hour("query_start", query_start),
        require_utc_hour("query_end", query_end),
    )


def availability_for_local_date(
    availability: Iterable[HourAvailability],
    *,
    selected_local_date: date | str,
    timezone_name: str,
) -> tuple[HourAvailability, ...]:
    """Retain exact source hours whose local start belongs to one civil date."""

    selected = parse_local_calendar_date(selected_local_date)
    timezone = _timezone_name(timezone_name)

    values = tuple(availability)

    for item in values:
        if not isinstance(item, HourAvailability):
            raise TypeError("availability must contain HourAvailability objects")

    return tuple(
        sorted(
            (
                item
                for item in values
                if utc_to_local(
                    item.hour_utc,
                    timezone,
                ).date()
                == selected
            ),
            key=lambda item: item.hour_utc,
        )
    )


@dataclass(frozen=True, slots=True)
class AnalysisRangeHandoff:
    """A contiguous analyzable window sent to Analysis controls.

    ``end_utc`` remains the source-storage exclusive upper boundary. Analysis
    controls expose closed user endpoints, so ``closed_end_utc`` identifies the
    start of the final one-second observation belonging to this window.
    """

    base: str
    preset_hash: str
    start_utc: datetime
    end_utc: datetime
    hour_count: int

    def __post_init__(self) -> None:
        base = _normalized_base(self.base)
        digest = _canonical_sha256(
            "preset_hash",
            self.preset_hash,
        )
        start = require_utc_hour(
            "start_utc",
            self.start_utc,
        )
        end = require_utc_hour(
            "end_utc",
            self.end_utc,
        )
        count = _positive_integer(
            "hour_count",
            self.hour_count,
        )

        if end <= start:
            raise AvailabilityCalendarError("end_utc must be after start_utc")

        if end - start != timedelta(hours=count):
            raise AvailabilityCalendarError(
                "Handoff duration does not match hour_count"
            )

        object.__setattr__(self, "base", base)
        object.__setattr__(self, "preset_hash", digest)
        object.__setattr__(self, "start_utc", start)
        object.__setattr__(self, "end_utc", end)
        object.__setattr__(self, "hour_count", count)

    @classmethod
    def from_window(
        cls,
        window: ContiguousAnalysisWindow,
    ) -> AnalysisRangeHandoff:
        if not isinstance(window, ContiguousAnalysisWindow):
            raise TypeError("window must be ContiguousAnalysisWindow")

        return cls(
            base=window.base,
            preset_hash=window.preset_hash,
            start_utc=window.start_utc,
            end_utc=window.end_utc,
            hour_count=window.hour_count,
        )

    @property
    def closed_end_utc(self) -> datetime:
        """Return the final included one-second bar start."""

        return self.end_utc - timedelta(seconds=1)


@dataclass(frozen=True, slots=True)
class AvailabilityCalendarBaseSnapshot:
    """Calendar and contiguous-window state for one base."""

    base: str
    preset_hash: str | None
    hours: tuple[HourAvailability, ...]
    contiguous_window: ContiguousAnalysisWindow | None

    def __post_init__(self) -> None:
        base = _normalized_base(self.base)
        hours = tuple(self.hours)

        if self.preset_hash is None:
            if hours:
                raise AvailabilityCalendarError(
                    "Calendar hours require an enabled preset"
                )

            if self.contiguous_window is not None:
                raise AvailabilityCalendarError(
                    "A contiguous window requires an enabled preset"
                )
        else:
            digest = _canonical_sha256(
                "preset_hash",
                self.preset_hash,
            )
            object.__setattr__(
                self,
                "preset_hash",
                digest,
            )

            for item in hours:
                if not isinstance(item, HourAvailability):
                    raise TypeError("hours must contain HourAvailability objects")

                if item.base != base or item.preset_hash != digest:
                    raise AvailabilityCalendarError(
                        "Calendar hour identity does not match its base snapshot"
                    )

            if self.contiguous_window is not None:
                if (
                    self.contiguous_window.base != base
                    or self.contiguous_window.preset_hash != digest
                ):
                    raise AvailabilityCalendarError(
                        "Contiguous window identity does not match calendar base"
                    )

        object.__setattr__(self, "base", base)
        object.__setattr__(self, "hours", hours)

    @property
    def handoff(self) -> AnalysisRangeHandoff | None:
        window = self.contiguous_window

        if window is None:
            return None

        return AnalysisRangeHandoff.from_window(window)


@dataclass(frozen=True, slots=True)
class AvailabilityCalendarSnapshot:
    """Immutable two-base availability snapshot for one local date."""

    selected_local_date: date
    timezone_name: str
    query_start_utc: datetime
    query_end_utc: datetime
    bases: tuple[AvailabilityCalendarBaseSnapshot, ...]

    def __post_init__(self) -> None:
        selected = parse_local_calendar_date(self.selected_local_date)
        timezone = _timezone_name(self.timezone_name)
        query_start = require_utc_hour(
            "query_start_utc",
            self.query_start_utc,
        )
        query_end = require_utc_hour(
            "query_end_utc",
            self.query_end_utc,
        )
        bases = tuple(self.bases)

        if query_end <= query_start:
            raise AvailabilityCalendarError(
                "query_end_utc must be after query_start_utc"
            )

        identities = [item.base for item in bases]

        if len(set(identities)) != len(identities):
            raise AvailabilityCalendarError(
                "Calendar snapshot contains duplicate bases"
            )

        object.__setattr__(
            self,
            "selected_local_date",
            selected,
        )
        object.__setattr__(
            self,
            "timezone_name",
            timezone,
        )
        object.__setattr__(
            self,
            "query_start_utc",
            query_start,
        )
        object.__setattr__(
            self,
            "query_end_utc",
            query_end,
        )
        object.__setattr__(
            self,
            "bases",
            bases,
        )


def build_availability_calendar_snapshot(
    session: Session,
    *,
    selected_local_date: date | str,
    timezone_name: str,
    bases: tuple[str, ...] = ("BTC", "ETH"),
    contiguous_lookback_hours: int = (DEFAULT_CONTIGUOUS_LOOKBACK_HOURS),
) -> AvailabilityCalendarSnapshot:
    """Load calendar and newest contiguous-window facts in one DB session."""

    if not isinstance(session, Session):
        raise TypeError("session must be a SQLAlchemy Session")

    selected = parse_local_calendar_date(selected_local_date)
    timezone = _timezone_name(timezone_name)
    lookback = _positive_integer(
        "contiguous_lookback_hours",
        contiguous_lookback_hours,
    )

    query_start, query_end = local_date_source_hour_query_bounds(
        selected,
        timezone_name=timezone,
    )
    history_start = query_start - timedelta(hours=lookback)

    base_snapshots: list[AvailabilityCalendarBaseSnapshot] = []

    for raw_base in bases:
        base = _normalized_base(raw_base)
        preset_hash = preferred_enabled_preset_hash(
            session,
            base=base,
        )

        if preset_hash is None:
            base_snapshots.append(
                AvailabilityCalendarBaseSnapshot(
                    base=base,
                    preset_hash=None,
                    hours=(),
                    contiguous_window=None,
                )
            )
            continue

        history = load_hourly_availability(
            session,
            base=base,
            preset_hash=preset_hash,
            start_utc=history_start,
            end_utc=query_end,
        )

        calendar_hours = availability_for_local_date(
            history,
            selected_local_date=selected,
            timezone_name=timezone,
        )
        contiguous_window = most_recent_contiguous_analyzable_window(
            history,
        )

        base_snapshots.append(
            AvailabilityCalendarBaseSnapshot(
                base=base,
                preset_hash=preset_hash,
                hours=calendar_hours,
                contiguous_window=contiguous_window,
            )
        )

    return AvailabilityCalendarSnapshot(
        selected_local_date=selected,
        timezone_name=timezone,
        query_start_utc=query_start,
        query_end_utc=query_end,
        bases=tuple(base_snapshots),
    )


def load_availability_calendar_snapshot(
    *,
    selected_local_date: date | str,
    timezone_name: str,
    bases: tuple[str, ...] = ("BTC", "ETH"),
    contiguous_lookback_hours: int = (DEFAULT_CONTIGUOUS_LOOKBACK_HOURS),
) -> AvailabilityCalendarSnapshot:
    """Production session-owning calendar loader."""

    with session_scope() as session:
        return build_availability_calendar_snapshot(
            session,
            selected_local_date=selected_local_date,
            timezone_name=timezone_name,
            bases=bases,
            contiguous_lookback_hours=(contiguous_lookback_hours),
        )


def availability_state_color(
    state: HourAvailabilityState | str,
) -> str:
    return _STATE_COLORS[HourAvailabilityState(state)]


def availability_state_label(
    state: HourAvailabilityState | str,
) -> str:
    return _STATE_LABELS[HourAvailabilityState(state)]


def availability_hour_tooltip(
    item: HourAvailability,
    *,
    timezone_name: str,
) -> str:
    """Return a concise local/UTC diagnostic tooltip."""

    if not isinstance(item, HourAvailability):
        raise TypeError("item must be HourAvailability")

    timezone = _timezone_name(timezone_name)
    local = utc_to_local(
        item.hour_utc,
        timezone,
    )

    return (
        f"{item.base} | "
        f"local {local.strftime('%Y-%m-%d %H:%M')} | "
        f"UTC {item.hour_utc.strftime('%Y-%m-%d %H:%M')} | "
        f"{availability_state_label(item.state)} | "
        f"orderbook={item.orderbook_status or 'none'} | "
        f"trades={item.trades_status or 'none'} | "
        f"L2 valid={item.l2_valid_seconds} | "
        f"L2 markets materialized="
        f"{item.materialized_l2_market_count}/"
        f"{item.expected_l2_market_count} | "
        f"L2 markets valid="
        f"{item.valid_l2_market_count}/"
        f"{item.expected_l2_market_count} | "
        f"price valid={item.price_valid_seconds}"
    )


def format_contiguous_window_local(
    window: ContiguousAnalysisWindow | None,
    *,
    timezone_name: str,
) -> str:
    """Return one local-time half-open contiguous-window label."""

    timezone = _timezone_name(timezone_name)

    if window is None:
        return "No contiguous analyzable window was found."

    start = utc_to_local(
        window.start_utc,
        timezone,
    )
    end = utc_to_local(
        window.end_utc,
        timezone,
    )

    return (
        f"{window.hour_count} hour(s): "
        f"[{start.strftime('%Y-%m-%d %H:%M')}, "
        f"{end.strftime('%Y-%m-%d %H:%M')}) "
        f"{timezone}"
    )


__all__ = [
    "DEFAULT_CONTIGUOUS_LOOKBACK_HOURS",
    "AnalysisRangeHandoff",
    "AvailabilityCalendarBaseSnapshot",
    "AvailabilityCalendarError",
    "AvailabilityCalendarSnapshot",
    "availability_for_local_date",
    "availability_hour_tooltip",
    "availability_state_color",
    "availability_state_label",
    "build_availability_calendar_snapshot",
    "format_contiguous_window_local",
    "load_availability_calendar_snapshot",
    "local_date_source_hour_query_bounds",
    "parse_local_calendar_date",
]

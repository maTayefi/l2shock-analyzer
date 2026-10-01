# l2shock/analysis/l2_view_stream.py
"""Bounded-memory Analysis loading: verified L2 and optional price -> bars.

The Analysis workflow loads and renders. Nothing here detects, ranks, or
annotates anything. Verified compact hourly rows are read in bounded hour
chunks, decoded, aggregated across expected markets at the same UTC second,
and folded directly into UTC-aligned viewing bars. One-second observations
are never retained for the whole requested range.

Policies preserved from the retired Shock-Start viewing path:

- a viewing bar containing any unusable L2 second is null in every L2
  channel (strict policy); liquidity is never forward-filled or zero-filled;
- a partial-market aggregate second is unusable (null), never a smaller sum;
- price is optional context: only VALID trade seconds contribute, and
  missing or failed price never blocks L2;
- outage runs are detected across hour and chunk boundaries over the whole
  requested range and reported only when strictly longer than a threshold.

Exact Decimal arithmetic is used for Bid, Ask, Total, and Delta. Bid share
is converted to float per second because it is only a plotting coordinate.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext
from typing import Any, Final, Protocol

from l2shock.analysis.l2_seconds import decode_l2_hour_to_seconds
from l2shock.analysis.multi_market import (
    AGGREGATE_L2_ALGORITHM_VERSION,
    AggregateL2QualityState,
    AggregateMarketKey,
    AggregateMarketSeries,
    aggregate_market_l2_seconds,
)
from l2shock.ingest.sampling import BookSampleQuality
from l2shock.presets import (
    component_data_presets,
    liquidity_data_preset_from_canonical_dict,
)
from l2shock.price import TradeSampleQuality, decode_hourly_trade_ohlc_blocks

log = logging.getLogger(__name__)

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SECOND: Final = timedelta(seconds=1)
_HOUR_SECONDS: Final = 3_600
_MAX_CHUNK_HOURS: Final = 168
_ESTIMATED_BYTES_PER_MARKET_HOUR: Final = 4 * 1024 * 1024

L2_VIEW_TIMEFRAMES_SECONDS: Final[tuple[int, ...]] = (
    1,
    5,
    15,
    60,
    300,
    900,
    3_600,
    14_400,
    86_400,
)
L2_VIEW_TIMEFRAME_LABELS: Final[dict[int, str]] = {
    1: "1 second",
    5: "5 seconds",
    15: "15 seconds",
    60: "1 minute",
    300: "5 minutes",
    900: "15 minutes",
    3_600: "1 hour",
    14_400: "4 hours",
    86_400: "1 day",
}

MAX_L2_VIEW_BARS: Final = 5_000
DEFAULT_L2_VIEW_MAX_BARS: Final = 1_200

DEFAULT_MAX_ANALYSIS_DURATION_SECONDS: Final = 86_400
MIN_MAX_ANALYSIS_DURATION_SECONDS: Final = 3_600
HARD_MAX_ANALYSIS_DURATION_SECONDS: Final = 63_072_000
LONG_ANALYSIS_WARNING_SECONDS: Final = 15_811_200  # 183 days

DEFAULT_ANALYSIS_MEMORY_BUDGET_MIB: Final = 512
MIN_ANALYSIS_MEMORY_BUDGET_MIB: Final = 64
MAX_ANALYSIS_MEMORY_BUDGET_MIB: Final = 16_384

MAX_WARNING_REGIONS_PER_CHANNEL: Final = 5_000

PRICE_STATUS_LOADED: Final = "loaded"
PRICE_STATUS_MISSING: Final = "missing"
PRICE_STATUS_FAILED: Final = "failed"
PRICE_STATUS_CORRUPT: Final = "corrupt"

DecimalOhlc = tuple[Decimal, Decimal, Decimal, Decimal]
"""Open, high, low, close."""
FloatOhlc = tuple[float, float, float, float]
"""Open, high, low, close."""


class L2ViewError(ValueError):
    """Invalid Analysis request, verified ownership, or viewing budget."""


class L2ViewCancelledError(RuntimeError):
    """Loading reached a cooperative stop boundary."""


def _epoch(value: datetime) -> int:
    return (value - _EPOCH) // _SECOND


def _from_epoch(seconds: int) -> datetime:
    return _EPOCH + timedelta(seconds=seconds)


def _aware_utc(value: object, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise L2ViewError(f"{name} must be a timezone-aware datetime")

    return value.astimezone(timezone.utc)


def _bounded_int(value: object, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        raise L2ViewError(f"{name} must be a whole number")

    if isinstance(value, float) and value.is_integer():
        value = int(value)

    if not isinstance(value, int):
        raise L2ViewError(f"{name} must be a whole number")

    if not minimum <= value <= maximum:
        raise L2ViewError(f"{name} must be between {minimum:,} and {maximum:,}")

    return value


def closed_request_slots(
    start_utc: datetime,
    end_utc: datetime,
) -> tuple[datetime, datetime]:
    """Closed endpoints -> half-open one-second slot range.

    Each endpoint owns the whole UTC second containing it:
    [12:00:07.5, 12:00:09.1] owns seconds 7, 8 and 9.

    Validate the original UTC instants before assigning slot ownership.
    Reversed instants inside the same second must not become a valid
    request merely because both truncate to the same slot.
    """
    requested_start = _aware_utc(start_utc, "Start")
    requested_end = _aware_utc(end_utc, "End")

    if requested_end < requested_start:
        raise L2ViewError("End must not precede Start")

    start = requested_start.replace(microsecond=0)
    end = requested_end.replace(microsecond=0)

    return start, end + _SECOND


@dataclass(frozen=True, slots=True)
class L2ViewRequest:
    base: str
    preset_hash: str
    requested_start_utc: datetime
    requested_end_utc: datetime
    max_duration_seconds: int = DEFAULT_MAX_ANALYSIS_DURATION_SECONDS

    def __post_init__(self) -> None:
        if self.base not in {"BTC", "ETH"}:
            raise L2ViewError("Base must be BTC or ETH")

        if (
            not isinstance(self.preset_hash, str)
            or len(self.preset_hash) != 64
            or any(c not in "0123456789abcdef" for c in self.preset_hash)
        ):
            raise L2ViewError("Preset hash must be a lowercase SHA-256")

        limit = _bounded_int(
            self.max_duration_seconds,
            "Max analysis duration (seconds)",
            MIN_MAX_ANALYSIS_DURATION_SECONDS,
            HARD_MAX_ANALYSIS_DURATION_SECONDS,
        )
        object.__setattr__(self, "max_duration_seconds", limit)

        slots = self.slot_count

        if slots > limit:
            raise L2ViewError(
                f"Requested range owns {slots:,} one-second slots; the max "
                f"analysis duration is {limit:,} seconds. Increase it (up to "
                f"{HARD_MAX_ANALYSIS_DURATION_SECONDS:,}) or shorten the range."
            )

    @property
    def effective_range(self) -> tuple[datetime, datetime]:
        return closed_request_slots(
            self.requested_start_utc,
            self.requested_end_utc,
        )

    @property
    def slot_count(self) -> int:
        start, end = self.effective_range
        return _epoch(end) - _epoch(start)


@dataclass(frozen=True, slots=True)
class L2ViewLoadOptions:
    timeframe_seconds: int | None = None
    max_bars: int = DEFAULT_L2_VIEW_MAX_BARS
    memory_budget_mib: int = DEFAULT_ANALYSIS_MEMORY_BUDGET_MIB
    l2_warning_seconds: int = 60
    price_warning_seconds: int = 180

    def __post_init__(self) -> None:
        timeframe = self.timeframe_seconds

        if timeframe is not None and (
            isinstance(timeframe, bool) or timeframe not in L2_VIEW_TIMEFRAMES_SECONDS
        ):
            raise L2ViewError("Unsupported viewing timeframe")

        object.__setattr__(
            self,
            "max_bars",
            _bounded_int(self.max_bars, "Maximum viewing bars", 1, MAX_L2_VIEW_BARS),
        )
        object.__setattr__(
            self,
            "memory_budget_mib",
            _bounded_int(
                self.memory_budget_mib,
                "Analysis memory budget (MiB)",
                MIN_ANALYSIS_MEMORY_BUDGET_MIB,
                MAX_ANALYSIS_MEMORY_BUDGET_MIB,
            ),
        )
        for name in ("l2_warning_seconds", "price_warning_seconds"):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 1, 86_400),
            )


def l2_view_bar_count(
    start_utc: datetime,
    end_utc_exclusive: datetime,
    timeframe_seconds: int,
) -> int:
    first = _epoch(start_utc) // timeframe_seconds
    last = (_epoch(end_utc_exclusive) - 1) // timeframe_seconds
    return last - first + 1


def select_l2_view_timeframe(
    start_utc: datetime,
    end_utc_exclusive: datetime,
    *,
    timeframe_seconds: int | None,
    max_bars: int,
) -> int:
    """Explicit timeframe must fit; Auto picks the finest that fits."""
    if timeframe_seconds is not None:
        if (
            isinstance(timeframe_seconds, bool)
            or timeframe_seconds not in L2_VIEW_TIMEFRAMES_SECONDS
        ):
            raise L2ViewError("Unsupported viewing timeframe")

        count = l2_view_bar_count(start_utc, end_utc_exclusive, timeframe_seconds)

        if count > max_bars:
            raise L2ViewError(
                f"{L2_VIEW_TIMEFRAME_LABELS[timeframe_seconds]} bars need "
                f"{count:,} bars (maximum {max_bars:,}); choose a coarser "
                "timeframe, a shorter range, or more viewing bars"
            )

        return timeframe_seconds

    for candidate in L2_VIEW_TIMEFRAMES_SECONDS:
        if l2_view_bar_count(start_utc, end_utc_exclusive, candidate) <= max_bars:
            return candidate

    raise L2ViewError(
        "Even one-day viewing bars exceed Maximum viewing bars; "
        "shorten the range or increase Maximum viewing bars"
    )


@dataclass(frozen=True, slots=True)
class L2ViewBar:
    """One UTC-aligned viewing bar clipped to the requested range."""

    start_utc: datetime
    source_seconds: int
    valid_l2: bool
    bid: DecimalOhlc | None
    ask: DecimalOhlc | None
    total: DecimalOhlc | None
    delta: DecimalOhlc | None
    bid_share_pct: FloatOhlc | None
    price: DecimalOhlc | None


@dataclass(frozen=True, slots=True)
class L2ViewOutageRegion:
    channel: str  # "l2" or "price"
    start_utc: datetime
    end_utc_exclusive: datetime


@dataclass(frozen=True, slots=True)
class L2ViewProjection:
    request: L2ViewRequest
    start_utc: datetime
    end_utc_exclusive: datetime
    timeframe_seconds: int
    max_bars: int
    bars: tuple[L2ViewBar, ...]
    l2_regions: tuple[L2ViewOutageRegion, ...]
    price_regions: tuple[L2ViewOutageRegion, ...]
    l2_regions_truncated: bool
    price_regions_truncated: bool
    price_status: str
    usable_l2_seconds: int
    unusable_l2_seconds: int
    partial_market_seconds: int
    input_id: str

    @property
    def source_seconds(self) -> int:
        return _epoch(self.end_utc_exclusive) - _epoch(self.start_utc)

    @property
    def price_regions_available(self) -> bool:
        return self.price_status in {PRICE_STATUS_LOADED, PRICE_STATUS_MISSING}


class L2HourRepository(Protocol):
    def get_preset(self, preset_hash: str) -> Any: ...

    def list_l2_hours(
        self,
        *,
        base: str,
        preset_hash: str,
        start_utc: datetime,
        end_utc: datetime,
        verify_codec: bool = True,
    ) -> Sequence[Any]: ...


class PriceHourRepository(Protocol):
    def list_price_hours(
        self,
        *,
        base: str,
        start_utc: datetime,
        end_utc: datetime,
        verify_codec: bool = True,
    ) -> Sequence[Any]: ...


RepositoryOpener = Callable[
    [],
    AbstractContextManager[tuple[L2HourRepository, PriceHourRepository | None]],
]


@contextmanager
def production_l2_view_repositories() -> Iterator[tuple[Any, Any]]:
    """One short verified read session per chunk; never held for a scan."""
    from l2shock.db.analytical_repository import AnalyticalRepository
    from l2shock.db.engine import session_scope
    from l2shock.db.price_repository import PriceAnalyticalRepository

    with session_scope() as session:
        yield AnalyticalRepository(session), PriceAnalyticalRepository(session)


@dataclass(frozen=True, slots=True)
class L2ViewComponent:
    market: AggregateMarketKey
    preset_hash: str


def resolve_l2_view_components(
    repository: L2HourRepository,
    request: L2ViewRequest,
) -> tuple[L2ViewComponent, ...]:
    """Resolve the verified preset into its single-market components."""
    stored = repository.get_preset(request.preset_hash)

    if stored is None:
        raise L2ViewError("Requested data preset does not exist")

    if stored.base != request.base:
        raise L2ViewError("Preset base does not match the requested base")

    try:
        semantic = liquidity_data_preset_from_canonical_dict(stored.config_json)
    except Exception as exc:
        raise L2ViewError("Persisted data preset failed semantic verification") from exc

    if semantic.preset_hash != request.preset_hash:
        raise L2ViewError("Persisted preset content does not match its hash")

    components = component_data_presets(semantic)

    if not components:
        raise L2ViewError("Preset has no component markets")

    result: dict[AggregateMarketKey, L2ViewComponent] = {}

    for component in components:
        if len(component.eligible_markets) != 1:
            raise L2ViewError("Each materialized component must own exactly one market")

        market = AggregateMarketKey.from_eligible_market(component.eligible_markets[0])

        if market in result:
            raise L2ViewError("Duplicate component market in preset")

        result[market] = L2ViewComponent(
            market=market, preset_hash=component.preset_hash
        )

    return tuple(result[market] for market in sorted(result))


def l2_view_chunk_hours(memory_budget_mib: int, market_count: int) -> int:
    """Hours read per chunk so decoded working data stays near the budget."""
    per_hour = _ESTIMATED_BYTES_PER_MARKET_HOUR * (max(1, market_count) + 1)
    return max(1, min(_MAX_CHUNK_HOURS, (memory_budget_mib * 1024 * 1024) // per_hour))


def _ohlc_add(acc: list[Any] | None, value: Any) -> list[Any]:
    if acc is None:
        return [value, value, value, value]

    if value > acc[1]:
        acc[1] = value
    elif value < acc[2]:
        acc[2] = value

    acc[3] = value
    return acc


class _BarBuilder:
    """Fold contiguous one-second inputs into UTC-aligned bars."""

    def __init__(
        self, timeframe_seconds: int, start_epoch: int, end_epoch: int
    ) -> None:
        self._tf = timeframe_seconds
        self._start = start_epoch
        self._end = end_epoch
        self._bars: list[L2ViewBar] = []
        self._bucket: int | None = None
        self._clear()

    def _clear(self) -> None:
        self._n = 0
        self._valid = True
        self._share_ok = True
        self._b: list[Any] | None = None
        self._a: list[Any] | None = None
        self._t: list[Any] | None = None
        self._d: list[Any] | None = None
        self._s: list[Any] | None = None
        self._p: list[Any] | None = None

    def add(
        self,
        epoch: int,
        bid: Decimal | None,
        ask: Decimal | None,
        price: DecimalOhlc | None,
    ) -> None:
        bucket = epoch // self._tf

        if bucket != self._bucket:
            if self._bucket is not None:
                self._flush()
            self._bucket = bucket
            self._clear()

        self._n += 1

        if price is not None:
            current = self._p
            if current is None:
                self._p = [price[0], price[1], price[2], price[3]]
            else:
                if price[1] > current[1]:
                    current[1] = price[1]
                if price[2] < current[2]:
                    current[2] = price[2]
                current[3] = price[3]

        if not self._valid:
            return

        if bid is None or ask is None:
            self._valid = False
            self._b = self._a = self._t = self._d = self._s = None
            return

        total = bid + ask
        self._b = _ohlc_add(self._b, bid)
        self._a = _ohlc_add(self._a, ask)
        self._t = _ohlc_add(self._t, total)
        self._d = _ohlc_add(self._d, bid - ask)

        if self._share_ok:
            if total == 0:
                self._share_ok = False
                self._s = None
            else:
                self._s = _ohlc_add(self._s, 100.0 * float(bid) / float(total))

    def _flush(self) -> None:
        bucket = self._bucket
        assert bucket is not None
        low = max(self._start, bucket * self._tf)
        high = min(self._end, (bucket + 1) * self._tf)

        if self._n != high - low:
            raise L2ViewError("Viewing bar does not own every requested second")

        valid = self._valid and self._b is not None

        self._bars.append(
            L2ViewBar(
                start_utc=_from_epoch(bucket * self._tf),
                source_seconds=self._n,
                valid_l2=valid,
                bid=tuple(self._b) if valid else None,  # type: ignore[arg-type]
                ask=tuple(self._a) if valid else None,  # type: ignore[arg-type]
                total=tuple(self._t) if valid else None,  # type: ignore[arg-type]
                delta=tuple(self._d) if valid else None,  # type: ignore[arg-type]
                bid_share_pct=(
                    tuple(self._s)  # type: ignore[arg-type]
                    if valid and self._share_ok and self._s is not None
                    else None
                ),
                price=tuple(self._p) if self._p is not None else None,  # type: ignore[arg-type]
            )
        )

    def finish(self) -> tuple[L2ViewBar, ...]:
        if self._bucket is not None:
            self._flush()
            self._bucket = None
        return tuple(self._bars)


class _OutageTracker:
    """Streaming detector of runs strictly longer than a threshold."""

    def __init__(self, channel: str, threshold_seconds: int) -> None:
        self.channel = channel
        self.threshold = threshold_seconds
        self.run_start: int | None = None
        self.regions: list[L2ViewOutageRegion] = []
        self.truncated = False

    def observe(self, epoch: int, usable: bool) -> None:
        if usable:
            if self.run_start is not None:
                self._close(epoch)
        elif self.run_start is None:
            self.run_start = epoch

    def _close(self, end_epoch: int) -> None:
        start = self.run_start
        self.run_start = None
        assert start is not None

        if end_epoch - start > self.threshold:
            if len(self.regions) >= MAX_WARNING_REGIONS_PER_CHANNEL:
                self.truncated = True
                return
            self.regions.append(
                L2ViewOutageRegion(
                    channel=self.channel,
                    start_utc=_from_epoch(start),
                    end_utc_exclusive=_from_epoch(end_epoch),
                )
            )

    def finish(self, end_epoch: int) -> tuple[L2ViewOutageRegion, ...]:
        if self.run_start is not None:
            self._close(end_epoch)
        return tuple(self.regions)


def _canonical(payload: object) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def _row_hour_epoch(value: object) -> int:
    hour = _aware_utc(value, "repository hour_utc")
    epoch = _epoch(hour)

    if hour.microsecond or epoch % _HOUR_SECONDS:
        raise L2ViewError("Repository returned a non-hourly row")

    return epoch


def _l2_rows_by_hour(
    rows: Sequence[Any],
    *,
    base: str,
    preset_hash: str,
    chunk_start: int,
    chunk_end: int,
) -> dict[int, Any]:
    result: dict[int, Any] = {}

    for row in rows:
        hour = _row_hour_epoch(row.hour_utc)

        if (
            row.base != base
            or row.preset_hash != preset_hash
            or not chunk_start <= hour < chunk_end
            or hour in result
        ):
            raise L2ViewError("L2 repository returned duplicate or wrong-owner hour")

        result[hour] = row

    return result


def _price_rows_by_hour(
    rows: Sequence[Any],
    *,
    base: str,
    chunk_start: int,
    chunk_end: int,
) -> dict[int, Any]:
    result: dict[int, Any] = {}

    for row in rows:
        hour = _row_hour_epoch(row.hour_utc)

        if (
            row.base != base
            or row.source_venue != "binance_futures"
            or not chunk_start <= hour < chunk_end
            or hour in result
        ):
            raise L2ViewError("Price repository returned duplicate or wrong-owner hour")

        result[hour] = row

    return result


def _price_failure_status(exc: BaseException) -> str:
    try:
        from l2shock.db.analytical_repository import AnalyticalRowCorruptionError
    except Exception:  # pragma: no cover - import failure is itself a failure
        return PRICE_STATUS_FAILED

    return (
        PRICE_STATUS_CORRUPT
        if isinstance(exc, AnalyticalRowCorruptionError)
        else PRICE_STATUS_FAILED
    )


def _hour_l2_pairs(
    components: tuple[L2ViewComponent, ...],
    rows_by_component: list[dict[int, Any]],
    *,
    hour_epoch: int,
    low: int,
    high: int,
    digest: Any,
) -> tuple[list[tuple[Decimal, Decimal] | None], int]:
    """Usable (Bid, Ask) per second of [low, high), plus partial-market count."""
    hour_dt = _from_epoch(hour_epoch)
    count = high - low
    first_offset = low - hour_epoch
    slices: list[tuple[str, Sequence[Any]] | None] = []

    for component, rows in zip(components, rows_by_component, strict=True):
        row = rows.get(hour_epoch)
        content = None if row is None else str(row.encoded.content_sha256)
        digest.update(
            _canonical(
                {
                    "kind": "l2",
                    "provider": component.market.provider,
                    "venue": component.market.venue,
                    "instrument": component.market.instrument,
                    "component_preset_hash": component.preset_hash,
                    "hour_utc": hour_dt.isoformat(),
                    "content_sha256": content,
                }
            )
            + b"\n"
        )

        if row is None:
            slices.append(None)
            continue

        decoded = decode_l2_hour_to_seconds(row)

        if (
            len(decoded) != _HOUR_SECONDS
            or decoded[0].timestamp_utc != hour_dt
            or decoded[-1].timestamp_utc != hour_dt + timedelta(seconds=3_599)
        ):
            raise L2ViewError("Verified L2 hour has unexpected one-second ownership")

        assert content is not None
        slices.append((content, decoded[first_offset : first_offset + count]))

    if len(components) == 1:
        item = slices[0]

        if item is None:
            return [None] * count, 0

        pairs: list[tuple[Decimal, Decimal] | None] = []

        for observation in item[1]:
            if observation.quality is BookSampleQuality.VALID:
                if (
                    observation.bid_liquidity is None
                    or observation.ask_liquidity is None
                ):
                    raise L2ViewError("VALID L2 second has no Bid or Ask liquidity")
                pairs.append((observation.bid_liquidity, observation.ask_liquidity))
            else:
                pairs.append(None)

        return pairs, 0

    if all(item is None for item in slices):
        return [None] * count, 0

    aggregate = aggregate_market_l2_seconds(
        expected_markets=tuple(component.market for component in components),
        market_series=tuple(
            AggregateMarketSeries(
                market=component.market,
                component_preset_hash=component.preset_hash,
                l2_content_sha256_by_hour=({} if item is None else {hour_dt: item[0]}),
                observations=(() if item is None else tuple(item[1])),
            )
            for component, item in zip(components, slices, strict=True)
        ),
        start_utc=_from_epoch(low),
        end_utc=_from_epoch(high),
    )
    observations = tuple(aggregate.observations)

    if len(observations) != count or (
        count and observations[0].timestamp_utc != _from_epoch(low)
    ):
        raise L2ViewError("Aggregate L2 does not own every requested second")

    pairs = []
    partial = 0

    for observation in observations:
        state = observation.quality_state

        if state is AggregateL2QualityState.VALID:
            if observation.bid_liquidity is None or observation.ask_liquidity is None:
                raise L2ViewError("VALID aggregate second has no Bid or Ask liquidity")
            pairs.append((observation.bid_liquidity, observation.ask_liquidity))
        elif state is AggregateL2QualityState.DEGRADED:
            partial += 1
            pairs.append(None)
        elif state is AggregateL2QualityState.INVALID:
            pairs.append(None)
        else:
            raise L2ViewError("Unknown aggregate L2 quality")

    return pairs, partial


def _price_value(decoded: Any, offset: int) -> DecimalOhlc | None:
    if decoded.quality[offset] is not TradeSampleQuality.VALID:
        return None

    value = (
        decoded.open[offset],
        decoded.high[offset],
        decoded.low[offset],
        decoded.close[offset],
    )

    if any(not isinstance(item, Decimal) or not item.is_finite() for item in value):
        raise L2ViewError("VALID verified price slot lacks finite OHLC")

    return value


def stream_l2_view(
    request: L2ViewRequest,
    options: L2ViewLoadOptions,
    *,
    open_repositories: RepositoryOpener = production_l2_view_repositories,
    cancellation_probe: Callable[[], bool] | None = None,
    progress_sink: Callable[[int, int], None] | None = None,
) -> L2ViewProjection:
    """Stream the requested range into bounded viewing bars and outage runs."""
    if not isinstance(request, L2ViewRequest):
        raise TypeError("request must be L2ViewRequest")

    if not isinstance(options, L2ViewLoadOptions):
        raise TypeError("options must be L2ViewLoadOptions")

    def check_cancel() -> None:
        if cancellation_probe is not None and cancellation_probe():
            raise L2ViewCancelledError("Analysis loading was stopped")

    start, end = request.effective_range
    start_epoch, end_epoch = _epoch(start), _epoch(end)
    timeframe = select_l2_view_timeframe(
        start,
        end,
        timeframe_seconds=options.timeframe_seconds,
        max_bars=options.max_bars,
    )

    check_cancel()

    with open_repositories() as (l2_repository, _price_repository):
        components = resolve_l2_view_components(l2_repository, request)

    first_hour = start_epoch - start_epoch % _HOUR_SECONDS
    last_hour_exclusive = ((end_epoch - 1) // _HOUR_SECONDS + 1) * _HOUR_SECONDS
    total_hours = (last_hour_exclusive - first_hour) // _HOUR_SECONDS
    chunk_seconds = l2_view_chunk_hours(options.memory_budget_mib, len(components)) * (
        _HOUR_SECONDS
    )

    builder = _BarBuilder(timeframe, start_epoch, end_epoch)
    l2_tracker = _OutageTracker("l2", options.l2_warning_seconds)
    price_tracker = _OutageTracker("price", options.price_warning_seconds)

    digest = hashlib.sha256()
    digest.update(
        _canonical(
            {
                "schema": "l2shock.l2_view_input",
                "schema_version": 1,
                "aggregate_l2_algorithm": AGGREGATE_L2_ALGORITHM_VERSION,
                "base": request.base,
                "preset_hash": request.preset_hash,
                "start_utc": start.isoformat(),
                "end_utc_exclusive": end.isoformat(),
                "components": [
                    {
                        "provider": component.market.provider,
                        "venue": component.market.venue,
                        "instrument": component.market.instrument,
                        "component_preset_hash": component.preset_hash,
                    }
                    for component in components
                ],
            }
        )
        + b"\n"
    )

    price_status: str | None = None  # None while price is still trusted
    price_tracking = True
    price_found = False
    usable = unusable = partial_total = 0
    hours_done = 0

    if progress_sink is not None:
        progress_sink(0, total_hours)

    with localcontext(Context(prec=80)):
        chunk_start = first_hour

        while chunk_start < last_hour_exclusive:
            check_cancel()
            chunk_end = min(last_hour_exclusive, chunk_start + chunk_seconds)

            with open_repositories() as (l2_repository, price_repository):
                rows_by_component = [
                    _l2_rows_by_hour(
                        l2_repository.list_l2_hours(
                            base=request.base,
                            preset_hash=component.preset_hash,
                            start_utc=_from_epoch(chunk_start),
                            end_utc=_from_epoch(chunk_end),
                            verify_codec=True,
                        ),
                        base=request.base,
                        preset_hash=component.preset_hash,
                        chunk_start=chunk_start,
                        chunk_end=chunk_end,
                    )
                    for component in components
                ]

                price_rows: dict[int, Any] = {}

                if price_repository is None:
                    price_tracking = False
                elif price_status is None:
                    try:
                        price_rows = _price_rows_by_hour(
                            price_repository.list_price_hours(
                                base=request.base,
                                start_utc=_from_epoch(chunk_start),
                                end_utc=_from_epoch(chunk_end),
                                verify_codec=True,
                            ),
                            base=request.base,
                            chunk_start=chunk_start,
                            chunk_end=chunk_end,
                        )
                    except Exception as exc:
                        price_status = _price_failure_status(exc)
                        price_tracking = False
                        log.exception("Optional Analysis price context unavailable.")

            for hour_epoch in range(chunk_start, chunk_end, _HOUR_SECONDS):
                check_cancel()
                low = max(start_epoch, hour_epoch)
                high = min(end_epoch, hour_epoch + _HOUR_SECONDS)

                pairs, partial = _hour_l2_pairs(
                    components,
                    rows_by_component,
                    hour_epoch=hour_epoch,
                    low=low,
                    high=high,
                    digest=digest,
                )
                partial_total += partial

                decoded_price = None
                price_row = price_rows.get(hour_epoch)

                if price_row is not None and price_status is None:
                    digest.update(
                        _canonical(
                            {
                                "kind": "price",
                                "hour_utc": _from_epoch(hour_epoch).isoformat(),
                                "content_sha256": str(price_row.encoded.content_sha256),
                            }
                        )
                        + b"\n"
                    )
                    try:
                        decoded_price = decode_hourly_trade_ohlc_blocks(
                            price_row.encoded
                        )
                        price_found = True
                    except Exception as exc:
                        price_status = _price_failure_status(exc)
                        price_tracking = False
                        decoded_price = None
                        log.exception("Optional Analysis price hour failed to decode.")

                for position, epoch in enumerate(range(low, high)):
                    pair = pairs[position]

                    if pair is None:
                        unusable += 1
                        bid = ask = None
                        l2_tracker.observe(epoch, False)
                    else:
                        usable += 1
                        bid, ask = pair
                        l2_tracker.observe(epoch, True)

                    price = None

                    if decoded_price is not None:
                        try:
                            price = _price_value(decoded_price, epoch - hour_epoch)
                        except L2ViewError:
                            price_status = PRICE_STATUS_CORRUPT
                            price_tracking = False
                            decoded_price = None
                            log.exception("Verified price slot is inconsistent.")

                    if price_tracking:
                        price_tracker.observe(epoch, price is not None)

                    builder.add(epoch, bid, ask, price)

                hours_done += 1

                if progress_sink is not None:
                    progress_sink(hours_done, total_hours)

            chunk_start = chunk_end

    bars = builder.finish()

    if len(bars) != l2_view_bar_count(start, end, timeframe):
        raise L2ViewError("Viewing bars do not cover the requested range")

    l2_regions = l2_tracker.finish(end_epoch)

    if price_tracking and price_status is None:
        price_regions = price_tracker.finish(end_epoch)
        price_truncated = price_tracker.truncated
    else:
        price_regions = ()
        price_truncated = False

    if price_status is None:
        price_status = PRICE_STATUS_LOADED if price_found else PRICE_STATUS_MISSING

    return L2ViewProjection(
        request=request,
        start_utc=start,
        end_utc_exclusive=end,
        timeframe_seconds=timeframe,
        max_bars=options.max_bars,
        bars=bars,
        l2_regions=l2_regions,
        price_regions=price_regions,
        l2_regions_truncated=l2_tracker.truncated,
        price_regions_truncated=price_truncated,
        price_status=price_status,
        usable_l2_seconds=usable,
        unusable_l2_seconds=unusable,
        partial_market_seconds=partial_total,
        input_id=digest.hexdigest(),
    )


def _merge_ohlc(values: Sequence[Any]) -> Any:
    return (
        values[0][0],
        max(value[1] for value in values),
        min(value[2] for value in values),
        values[-1][3],
    )


def _merge_bars(start_epoch: int, bars: Sequence[L2ViewBar]) -> L2ViewBar:
    valid = all(bar.valid_l2 for bar in bars)
    shares = [bar.bid_share_pct for bar in bars]
    prices = [bar.price for bar in bars if bar.price is not None]

    return L2ViewBar(
        start_utc=_from_epoch(start_epoch),
        source_seconds=sum(bar.source_seconds for bar in bars),
        valid_l2=valid,
        bid=_merge_ohlc([bar.bid for bar in bars]) if valid else None,
        ask=_merge_ohlc([bar.ask for bar in bars]) if valid else None,
        total=_merge_ohlc([bar.total for bar in bars]) if valid else None,
        delta=_merge_ohlc([bar.delta for bar in bars]) if valid else None,
        bid_share_pct=(
            _merge_ohlc(shares)
            if valid and all(share is not None for share in shares)
            else None
        ),
        price=_merge_ohlc(prices) if prices else None,
    )


def coarsen_l2_view(
    projection: L2ViewProjection,
    timeframe_seconds: int,
    *,
    max_bars: int | None = None,
) -> L2ViewProjection:
    """Exact coarser bars from already-loaded bars; no database reread.

    Supported timeframes nest (each divides the next), so merging finer
    UTC-aligned bars equals streaming the coarser timeframe directly.

    An unchanged timeframe still validates and applies the requested
    viewing-bar budget. The source projection is never mutated.
    """
    if not isinstance(projection, L2ViewProjection):
        raise TypeError("projection must be L2ViewProjection")

    timeframe_seconds = _bounded_int(
        timeframe_seconds,
        "timeframe_seconds",
        1,
        max(L2_VIEW_TIMEFRAMES_SECONDS),
    )

    if timeframe_seconds not in L2_VIEW_TIMEFRAMES_SECONDS:
        raise L2ViewError("Unsupported viewing timeframe")

    base = projection.timeframe_seconds
    budget = _bounded_int(
        projection.max_bars if max_bars is None else max_bars,
        "max_bars",
        1,
        MAX_L2_VIEW_BARS,
    )

    if timeframe_seconds < base or timeframe_seconds % base:
        raise L2ViewError("A finer viewing timeframe requires reloading the range")

    count = l2_view_bar_count(
        projection.start_utc,
        projection.end_utc_exclusive,
        timeframe_seconds,
    )

    if count > budget:
        raise L2ViewError("Viewing timeframe exceeds Maximum viewing bars")

    if timeframe_seconds == base:
        if budget == projection.max_bars:
            return projection
        return replace(projection, max_bars=budget)

    merged: list[L2ViewBar] = []
    group: list[L2ViewBar] = []
    current: int | None = None

    for bar in projection.bars:
        bucket = _epoch(bar.start_utc) // timeframe_seconds

        if current is not None and bucket != current:
            merged.append(_merge_bars(current * timeframe_seconds, group))
            group = []

        current = bucket
        group.append(bar)

    if group and current is not None:
        merged.append(_merge_bars(current * timeframe_seconds, group))

    return replace(
        projection,
        timeframe_seconds=timeframe_seconds,
        max_bars=budget,
        bars=tuple(merged),
    )


__all__ = [
    "DEFAULT_ANALYSIS_MEMORY_BUDGET_MIB",
    "DEFAULT_L2_VIEW_MAX_BARS",
    "DEFAULT_MAX_ANALYSIS_DURATION_SECONDS",
    "HARD_MAX_ANALYSIS_DURATION_SECONDS",
    "L2_VIEW_TIMEFRAMES_SECONDS",
    "L2_VIEW_TIMEFRAME_LABELS",
    "LONG_ANALYSIS_WARNING_SECONDS",
    "MAX_ANALYSIS_MEMORY_BUDGET_MIB",
    "MAX_L2_VIEW_BARS",
    "MAX_WARNING_REGIONS_PER_CHANNEL",
    "MIN_ANALYSIS_MEMORY_BUDGET_MIB",
    "MIN_MAX_ANALYSIS_DURATION_SECONDS",
    "PRICE_STATUS_CORRUPT",
    "PRICE_STATUS_FAILED",
    "PRICE_STATUS_LOADED",
    "PRICE_STATUS_MISSING",
    "L2ViewBar",
    "L2ViewCancelledError",
    "L2ViewComponent",
    "L2ViewError",
    "L2ViewLoadOptions",
    "L2ViewOutageRegion",
    "L2ViewProjection",
    "L2ViewRequest",
    "closed_request_slots",
    "coarsen_l2_view",
    "l2_view_bar_count",
    "l2_view_chunk_hours",
    "production_l2_view_repositories",
    "resolve_l2_view_components",
    "select_l2_view_timeframe",
    "stream_l2_view",
]

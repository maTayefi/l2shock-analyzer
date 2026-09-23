# l2shock/remote_worker.py
"""Scheduled/manual remote-hour processing orchestration.

This module composes existing boundaries without reimplementing them:

    shared release schedule
    -> pinned Hugging Face repository inspection
    -> predecessor checkpoint lookup
    -> PostgreSQL-free CryptoHFTData acquisition
    -> shared headless L2/price processing
    -> conflict-safe Hugging Face publication

It deliberately performs no:

- PostgreSQL access;
- NiceGUI work;
- alternative replay implementation;
- alternative downloader implementation;
- analytical resampling;
- raw-source persistence after the worker workspace closes.

The scheduled unit is one venue/instrument/completed UTC hour.

Independent venue/instrument chains may run concurrently. GitHub Actions owns
cross-run chain serialization. This module still verifies predecessor ownership
and immutable artifact identities rather than trusting workflow scheduling.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from pathlib import Path
from typing import Final

from pydantic import SecretStr

from l2shock.acquisition import (
    AcquisitionError,
    RemoteFileNotFoundError,
    SourceDataKind,
    SourceFileSpec,
    RemoteFileNotFoundError,
    latest_release_eligible_hour,
)
from l2shock.config import CryptoHFTConfig
from l2shock.presets import (
    LiquidityDataPreset,
    build_binance_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.processing import (
    ProcessingError,
    ProcessingSourceArchive,
)
from l2shock.remote import (
    HeadlessL2ProcessingOutput,
    HuggingFaceArtifactNotFoundError,
    HuggingFaceDatasetRepository,
    HuggingFacePublicationResult,
    HuggingFaceRepositoryError,
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    RemoteWorkerWorkspace,
    acquire_remote_worker_archives,
    process_l2_archive_headlessly,
    process_price_archives_headlessly,
    temporary_remote_worker_workspace,
)
from l2shock.timeutils import now_utc, require_utc_hour

import logging

log = logging.getLogger(__name__)


class RemoteWorkerExitStatus(IntEnum):
    OK = 0
    UNEXPECTED_ERROR = 1
    INPUT_OR_CONTRACT_ERROR = 2
    SOURCE_UNAVAILABLE = 3
    CHECKPOINT_CHAIN_BLOCKED = 4
    HUGGING_FACE_ERROR = 5
    INTERRUPTED = 130


class RemoteWorkerError(RuntimeError):
    """One remote-hour worker operation could not complete safely."""


class RemoteWorkerCheckpointBlockedError(RemoteWorkerError):
    """The target L2 hour cannot safely advance its checkpoint chain."""


@dataclass(frozen=True, slots=True)
class RemoteWorkerResult:
    """JSON-safe summary of one remote-hour operation."""

    venue: str
    instrument: str
    hour_utc: datetime
    pinned_input_revision: str

    l2_created: bool
    l2_revision: str | None
    l2_reused: bool

    price_required: bool
    price_created: bool
    price_revision: str | None
    price_reused: bool

    source_downloaded_count: int
    source_reused_count: int

    def __post_init__(self) -> None:
        venue = str(self.venue or "").strip().lower()
        instrument = str(self.instrument or "").strip().upper()
        hour = require_utc_hour(
            "hour_utc",
            self.hour_utc,
        )

        if not venue or not instrument:
            raise RemoteWorkerError(
                "Remote worker result identity cannot contain blank fields"
            )

        revision = str(self.pinned_input_revision or "").strip().lower()

        if len(revision) not in {40, 64} or any(
            character not in "0123456789abcdef" for character in revision
        ):
            raise RemoteWorkerError("pinned_input_revision must be a full commit SHA")

        for field_name in (
            "l2_created",
            "l2_reused",
            "price_required",
            "price_created",
            "price_reused",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be bool")

        for field_name in (
            "source_downloaded_count",
            "source_reused_count",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RemoteWorkerError(f"{field_name} must be a non-negative integer")

        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "hour_utc", hour)
        object.__setattr__(
            self,
            "pinned_input_revision",
            revision,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "l2shock.remote_worker_result",
            "schema_version": 1,
            "venue": self.venue,
            "instrument": self.instrument,
            "hour_utc": self.hour_utc.isoformat().replace("+00:00", "Z"),
            "pinned_input_revision": self.pinned_input_revision,
            "l2": {
                "created": self.l2_created,
                "reused": self.l2_reused,
                "revision": self.l2_revision,
            },
            "price": {
                "required": self.price_required,
                "created": self.price_created,
                "reused": self.price_reused,
                "revision": self.price_revision,
            },
            "source_acquisition": {
                "downloaded_count": self.source_downloaded_count,
                "reused_count": self.source_reused_count,
            },
        }


@dataclass(frozen=True, slots=True)
class RemoteCatchUpRunResult:
    """JSON-safe summary of one bounded sequential catch-up run."""

    venue: str
    instrument: str
    latest_eligible_hour_utc: datetime
    results: tuple[RemoteWorkerResult, ...]
    max_hours_per_run: int
    max_runtime_minutes: int
    stop_reason: str

    def __post_init__(self) -> None:
        venue = str(self.venue or "").strip().lower()
        instrument = str(self.instrument or "").strip().upper()
        latest = require_utc_hour(
            "latest_eligible_hour_utc",
            self.latest_eligible_hour_utc,
        )
        results = tuple(self.results)

        if not venue or not instrument:
            raise RemoteWorkerError(
                "Remote catch-up result identity cannot contain blank fields"
            )

        for field_name in (
            "max_hours_per_run",
            "max_runtime_minutes",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise RemoteWorkerError(f"{field_name} must be a positive integer")

        allowed_stop_reasons = {
            "caught_up",
            "source_unavailable",
            "no_work",
            "max_hours_per_run",
            "runtime_budget",
        }

        if self.stop_reason not in allowed_stop_reasons:
            raise RemoteWorkerError("Unsupported remote catch-up stop reason")

        if not results and self.stop_reason != "no_work":
            raise RemoteWorkerError(
                "An empty remote catch-up result requires stop_reason='no_work'"
            )

        if results and self.stop_reason == "no_work":
            raise RemoteWorkerError(
                "stop_reason='no_work' requires an empty catch-up result"
            )

        previous_hour: datetime | None = None

        for result in results:
            if not isinstance(result, RemoteWorkerResult):
                raise TypeError("results must contain RemoteWorkerResult objects")

            if result.venue != venue or result.instrument != instrument:
                raise RemoteWorkerError(
                    "Catch-up results must belong to one remote chain"
                )

            if previous_hour is not None and result.hour_utc <= previous_hour:
                raise RemoteWorkerError(
                    "Catch-up results must be strictly ordered "
                    "oldest to newest without duplicate hours"
                )

            if result.hour_utc > latest:
                raise RemoteWorkerError(
                    "Catch-up result exceeds the release-eligible boundary"
                )

            previous_hour = result.hour_utc

        if len(results) > self.max_hours_per_run:
            raise RemoteWorkerError("Catch-up result exceeds max_hours_per_run")

        if self.stop_reason == "caught_up":
            if not results or results[-1].hour_utc != latest:
                raise RemoteWorkerError(
                    "caught_up requires the latest eligible hour to complete"
                )

        if self.stop_reason == "source_unavailable":
            if not results:
                raise RemoteWorkerError(
                    "source_unavailable requires at least one completed hour"
                )

            if results[-1].hour_utc >= latest:
                raise RemoteWorkerError(
                    "source_unavailable requires the latest eligible hour "
                    "to remain incomplete"
                )

        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(
            self,
            "latest_eligible_hour_utc",
            latest,
        )
        object.__setattr__(self, "results", results)

    @property
    def completed_hour_count(self) -> int:
        return len(self.results)

    @property
    def first_hour_utc(self) -> datetime | None:
        if not self.results:
            return None

        return self.results[0].hour_utc

    @property
    def last_hour_utc(self) -> datetime | None:
        if not self.results:
            return None

        return self.results[-1].hour_utc

    @staticmethod
    def _optional_utc_text(value: datetime | None) -> str | None:
        if value is None:
            return None

        return value.isoformat().replace(
            "+00:00",
            "Z",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "l2shock.remote_catch_up_run_result",
            "schema_version": 1,
            "venue": self.venue,
            "instrument": self.instrument,
            "latest_eligible_hour_utc": (
                self.latest_eligible_hour_utc.isoformat().replace(
                    "+00:00",
                    "Z",
                )
            ),
            "first_hour_utc": self._optional_utc_text(
                self.first_hour_utc,
            ),
            "last_hour_utc": self._optional_utc_text(
                self.last_hour_utc,
            ),
            "completed_hour_count": self.completed_hour_count,
            "max_hours_per_run": self.max_hours_per_run,
            "max_runtime_minutes": self.max_runtime_minutes,
            "stop_reason": self.stop_reason,
            "hours": [result.to_dict() for result in self.results],
        }


@dataclass(frozen=True, slots=True)
class _ExistingRemoteState:
    pinned_revision: str
    l2_artifact: RemoteL2ProcessedArtifact | None
    price_artifact: RemotePriceProcessedArtifact | None


@dataclass(frozen=True, slots=True)
class _RemoteCatchUpObservation:
    """One pinned-revision hour inspected during chain-frontier discovery."""

    hour_utc: datetime
    l2_exists: bool
    output_checkpoint_exists: bool
    price_exists: bool

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
            "l2_exists",
            "output_checkpoint_exists",
            "price_exists",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be bool")

        if self.output_checkpoint_exists and not self.l2_exists:
            raise RemoteWorkerError(
                "A remote output checkpoint cannot exist without its L2 artifact"
            )


_SUPPORTED_CHAINS: Final[dict[tuple[str, str], bool]] = {
    ("binance_futures", "BTCUSDT"): True,
    ("binance_futures", "ETHUSDT"): True,
    ("bybit", "BTCUSDT"): False,
    ("bybit", "ETHUSDT"): False,
    ("okx_futures", "BTC-USDT-SWAP"): False,
    ("okx_futures", "ETH-USDT-SWAP"): False,
}


def _canonical_utc_hour(value: str) -> datetime:
    text = str(value or "").strip()

    if not text.endswith("Z"):
        raise argparse.ArgumentTypeError("hour must use canonical UTC text ending in Z")

    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "hour must use canonical ISO-8601 UTC text"
        ) from exc

    try:
        hour = require_utc_hour(
            "hour",
            parsed,
        )
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc

    canonical = hour.isoformat().replace("+00:00", "Z")

    if canonical != text:
        raise argparse.ArgumentTypeError("hour is valid but not canonically encoded")

    return hour


def _positive_integer(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc

    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")

    return result


def _depth_fraction(value: str) -> Decimal:
    text = str(value or "").strip()

    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "depth fraction must be an exact decimal"
        ) from exc

    if not result.is_finite():
        raise argparse.ArgumentTypeError("depth fraction must be finite")

    return result


def _normalized_chain(
    venue: object,
    instrument: object,
) -> tuple[str, str, bool]:
    normalized_venue = str(venue or "").strip().lower()
    normalized_instrument = str(instrument or "").strip().upper()

    try:
        price_required = _SUPPORTED_CHAINS[
            (
                normalized_venue,
                normalized_instrument,
            )
        ]
    except KeyError as exc:
        allowed = ", ".join(
            f"{item_venue}/{item_instrument}"
            for item_venue, item_instrument in sorted(_SUPPORTED_CHAINS)
        )
        raise RemoteWorkerError(
            "Unsupported remote processing chain. " f"Allowed chains: {allowed}"
        ) from exc

    return (
        normalized_venue,
        normalized_instrument,
        price_required,
    )


def _preset_for_chain(
    *,
    venue: str,
    instrument: str,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
) -> LiquidityDataPreset:
    source = SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=instrument,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=datetime(
            2000,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    )

    builders = {
        "binance_futures": build_binance_futures_data_preset,
        "bybit": build_bybit_data_preset,
        "okx_futures": build_okx_futures_data_preset,
    }

    try:
        builder = builders[venue]
    except KeyError as exc:
        raise RemoteWorkerError(
            f"No remote L2 preset builder exists for venue {venue!r}"
        ) from exc

    preset = builder(
        base=source.base,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
    )

    market = preset.eligible_markets[0]

    if (
        market.provider != source.provider
        or market.venue != source.venue
        or market.instrument != source.symbol
    ):
        raise RemoteWorkerError(
            "Generated component preset does not match the worker chain"
        )

    return preset


def _source_spec(
    *,
    venue: str,
    instrument: str,
    hour_utc: datetime,
    data_kind: SourceDataKind,
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=instrument,
        data_kind=data_kind,
        hour_utc=hour_utc,
    )


def _l2_key(
    *,
    venue: str,
    instrument: str,
    hour_utc: datetime,
    preset: LiquidityDataPreset,
) -> RemoteArtifactKey:
    return RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
        hour_utc=hour_utc,
        preset_hash=preset.preset_hash,
    )


def _price_key(
    *,
    instrument: str,
    hour_utc: datetime,
) -> RemoteArtifactKey:
    return RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument=instrument,
        hour_utc=hour_utc,
    )


def _catch_up_target_from_observations(
    venue: str,
    latest_eligible_hour_utc: datetime,
    observations: tuple[_RemoteCatchUpObservation, ...],
    price_required: bool,
) -> datetime:
    """Select one safe remote target from newest-to-oldest observations.

    The first existing L2 artifact is the newest verified chain frontier inside
    the bounded search window.

    For Binance:

    - a missing price artifact at that frontier is repaired first;
    - otherwise only the immediately following hour may advance;
    - the frontier must own a usable output checkpoint;
    - absence of a frontier fails closed because update-only archives cannot
      initialize a fresh book.

    For independently initializing source attempts:

    - OKX must establish its approved complete snapshot contract;
    - Binance may initialize only from a proven complete source snapshot;
    - Bybit may initialize only from a native snapshot with a non-null replay
      frontier;
    - a Bybit frontier-less boundary snapshot still requires an immediately
      preceding verified checkpoint.

    When no artifact exists in the bounded window, the oldest inspected hour is
    selected for a strict initialization attempt. Headless replay must produce
    a usable output checkpoint before any L2 artifact is published.

    Returning the latest already-complete hour is an idempotent no-work probe.
    ``process_remote_hour`` will verify and reuse its existing artifacts.
    """
    log.info(
        "CATCH-UP TARGET SELECTION: venue=%s latest_eligible=%s "
        "observations=%d price_required=%s",
        venue,
        latest_eligible_hour_utc.isoformat(),
        len(observations),
        price_required,
    )
    for obs in observations:
        log.info(
            "  hour=%s l2_exists=%s checkpoint_exists=%s price_exists=%s",
            obs.hour_utc.isoformat(),
            obs.l2_exists,
            obs.output_checkpoint_exists,
            obs.price_exists,
        )
    normalized_venue = str(venue or "").strip().lower()
    latest = require_utc_hour(
        "latest_eligible_hour_utc",
        latest_eligible_hour_utc,
    )
    values = tuple(observations)
    if not values:
        raise RemoteWorkerError(
            "Remote catch-up planning requires at least one inspected hour"
        )
    if not isinstance(price_required, bool):
        raise TypeError("price_required must be bool")
    expected_hour = latest
    for observation in values:
        if not isinstance(observation, _RemoteCatchUpObservation):
            raise TypeError(
                "observations must contain _RemoteCatchUpObservation objects"
            )
        if observation.hour_utc != expected_hour:
            raise RemoteWorkerError(
                "Remote catch-up observations must be contiguous "
                "and ordered newest to oldest"
            )
        expected_hour -= timedelta(hours=1)

    # Find the newest existing L2 artifact with a usable output checkpoint.
    # An artifact without a checkpoint is a permanent blocked marker at its
    # immutable path and must never be treated as the chain frontier.
    frontier: _RemoteCatchUpObservation | None = None
    blocked_hours: list[datetime] = []

    for observation in values:
        if not observation.l2_exists:
            continue

        if observation.output_checkpoint_exists:
            frontier = observation
            break

        blocked_hours.append(observation.hour_utc)
        log.warning(
            "Skipping unusable remote L2 artifact: hour=%s venue=%s "
            "reason=missing_output_checkpoint",
            observation.hour_utc.isoformat(),
            normalized_venue,
        )

    if frontier is None:
        if normalized_venue not in {
            "binance_futures",
            "bybit",
            "okx_futures",
        }:
            raise RemoteWorkerCheckpointBlockedError(
                "No verified L2/checkpoint seed exists inside the bounded "
                f"remote catch-up search window for venue={normalized_venue}"
            )

        # A checkpoint-less artifact is not a continuation frontier, but it
        # must not permanently prevent a later missing source hour from proving
        # that it can initialize independently. Select the oldest missing hour
        # in the bounded window and let strict headless replay prove or reject
        # its venue-specific snapshot contract.
        for observation in reversed(values):
            if observation.l2_exists:
                continue

            log.info(
                "No usable checkpoint frontier exists for venue=%s. "
                "Selecting missing hour=%s for a strict source-owned "
                "initialization attempt; blocked_hours=%s. Publication "
                "remains non-authoritative unless replay produces a usable "
                "output checkpoint.",
                normalized_venue,
                observation.hour_utc.isoformat(),
                [hour.isoformat() for hour in blocked_hours],
            )
            return observation.hour_utc

        raise RemoteWorkerCheckpointBlockedError(
            "Every inspected remote L2 hour is occupied by an artifact "
            "without an output checkpoint, and no older usable checkpoint "
            f"frontier exists; blocked_hours="
            f"{[hour.isoformat() for hour in blocked_hours]}"
        )

    if price_required and not frontier.price_exists:
        return frontier.hour_utc

    observation_by_hour = {observation.hour_utc: observation for observation in values}

    candidate_hour = frontier.hour_utc + timedelta(hours=1)

    while candidate_hour <= latest:
        candidate = observation_by_hour.get(candidate_hour)

        if candidate is None:
            raise RemoteWorkerError(
                "Catch-up planning lacks an observation for candidate hour "
                f"{candidate_hour.isoformat()}"
            )

        if not candidate.l2_exists:
            log.info(
                "Selected first missing L2 hour after usable frontier: "
                "frontier=%s target=%s skipped_blocked_hours=%s",
                frontier.hour_utc.isoformat(),
                candidate_hour.isoformat(),
                [
                    hour.isoformat()
                    for hour in blocked_hours
                    if frontier.hour_utc < hour < candidate_hour
                ],
            )
            return candidate_hour

        if candidate.output_checkpoint_exists:
            raise RemoteWorkerError(
                "A newer usable checkpoint was observed after the selected "
                "frontier; catch-up observations are inconsistent"
            )

        log.warning(
            "Advancing past immutable unusable L2 artifact: hour=%s "
            "venue=%s reason=missing_output_checkpoint",
            candidate_hour.isoformat(),
            normalized_venue,
        )
        candidate_hour += timedelta(hours=1)

    if blocked_hours:
        raise RemoteWorkerCheckpointBlockedError(
            "The newest eligible remote hour is occupied by an unusable L2 "
            "artifact without an output checkpoint, and no later missing hour "
            "is available yet"
        )

    return frontier.hour_utc


async def select_remote_catch_up_hour(
    *,
    repository: HuggingFaceDatasetRepository,
    venue: str,
    instrument: str,
    latest_eligible_hour_utc: datetime,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
    search_hours: int,
) -> datetime:
    """Find one safe processable hour from one pinned HF repository revision."""

    if not isinstance(
        repository,
        HuggingFaceDatasetRepository,
    ):
        raise TypeError("repository must be HuggingFaceDatasetRepository")

    if (
        isinstance(search_hours, bool)
        or not isinstance(search_hours, int)
        or search_hours <= 0
    ):
        raise RemoteWorkerError("search_hours must be a positive integer")

    latest = require_utc_hour(
        "latest_eligible_hour_utc",
        latest_eligible_hour_utc,
    )
    normalized_venue, normalized_instrument, price_required = _normalized_chain(
        venue,
        instrument,
    )
    preset = _preset_for_chain(
        venue=normalized_venue,
        instrument=normalized_instrument,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
    )

    pinned_revision = await asyncio.to_thread(
        repository.current_revision,
    )
    observations: list[_RemoteCatchUpObservation] = []

    for offset in range(search_hours):
        hour = latest - timedelta(hours=offset)
        l2_key = _l2_key(
            venue=normalized_venue,
            instrument=normalized_instrument,
            hour_utc=hour,
            preset=preset,
        )
        downloaded_l2 = await asyncio.to_thread(
            repository.download_artifact,
            l2_key,
            revision=pinned_revision,
        )

        if downloaded_l2 is None:
            observations.append(
                _RemoteCatchUpObservation(
                    hour_utc=hour,
                    l2_exists=False,
                    output_checkpoint_exists=False,
                    price_exists=False,
                )
            )
            continue

        if not isinstance(
            downloaded_l2.artifact,
            RemoteL2ProcessedArtifact,
        ):
            raise RemoteWorkerError(
                "Remote catch-up L2 key resolved to a non-L2 artifact"
            )

        price_exists = False

        if price_required:
            downloaded_price = await asyncio.to_thread(
                repository.download_artifact,
                _price_key(
                    instrument=normalized_instrument,
                    hour_utc=hour,
                ),
                revision=pinned_revision,
            )

            if downloaded_price is not None:
                if not isinstance(
                    downloaded_price.artifact,
                    RemotePriceProcessedArtifact,
                ):
                    raise RemoteWorkerError(
                        "Remote catch-up price key resolved to a non-price artifact"
                    )

                price_exists = True

        has_output_checkpoint = downloaded_l2.artifact.output_checkpoint is not None

        observations.append(
            _RemoteCatchUpObservation(
                hour_utc=hour,
                l2_exists=True,
                output_checkpoint_exists=has_output_checkpoint,
                price_exists=price_exists,
            )
        )

        if has_output_checkpoint:
            # The newest existing L2 artifact with a usable output checkpoint
            # is the valid chain frontier. An L2 artifact without a checkpoint
            # is a blocked marker, not a frontier, so continue scanning older
            # hours until a usable checkpoint is found or the bound is reached.
            break

        log.warning(
            "Remote L2 artifact has no output checkpoint; continuing bounded "
            "frontier scan past hour=%s venue=%s instrument=%s",
            hour.isoformat(),
            normalized_venue,
            normalized_instrument,
        )

    return _catch_up_target_from_observations(
        venue=normalized_venue,
        latest_eligible_hour_utc=latest,
        observations=tuple(observations),
        price_required=price_required,
    )


async def process_remote_catch_up(
    *,
    repository: HuggingFaceDatasetRepository,
    cryptohft: CryptoHFTConfig,
    workspace: RemoteWorkerWorkspace,
    venue: str,
    instrument: str,
    latest_eligible_hour_utc: datetime,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
    search_hours: int,
    max_hours_per_run: int,
    max_runtime_minutes: int,
    producer_git_commit: str | None,
    use_api_key: bool = False,
    batch_size: int = 131_072,
    _monotonic: Callable[[], float] | None = None,
) -> RemoteCatchUpRunResult:
    """Process a bounded ascending sequence for one remote chain.

    Target selection is repeated after every successful operation against a
    fresh pinned Hugging Face revision. This permits the planner to:

    - observe a newly repaired same-hour price artifact;
    - advance from the newly verified frontier;
    - skip immutable L2 artifacts that have no output checkpoint;
    - select the first later missing L2 hour.

    Successfully processed result hours remain strictly increasing, but they
    need not be contiguous when an immutable unusable artifact occupies an
    intermediate hour.

    The runtime budget is cooperative. It prevents admission of another hour
    after the budget expires; it does not interrupt an hour that is already
    downloading, processing, or publishing.
    """

    if (
        isinstance(search_hours, bool)
        or not isinstance(search_hours, int)
        or search_hours <= 0
    ):
        raise RemoteWorkerError("search_hours must be a positive integer")

    if (
        isinstance(max_hours_per_run, bool)
        or not isinstance(max_hours_per_run, int)
        or max_hours_per_run <= 0
    ):
        raise RemoteWorkerError("max_hours_per_run must be a positive integer")

    if (
        isinstance(max_runtime_minutes, bool)
        or not isinstance(max_runtime_minutes, int)
        or max_runtime_minutes <= 0
    ):
        raise RemoteWorkerError("max_runtime_minutes must be a positive integer")

    latest = require_utc_hour(
        "latest_eligible_hour_utc",
        latest_eligible_hour_utc,
    )
    normalized_venue, normalized_instrument, _ = _normalized_chain(
        venue,
        instrument,
    )

    if _monotonic is None:
        clock = asyncio.get_running_loop().time
    elif callable(_monotonic):
        clock = _monotonic
    else:
        raise TypeError("_monotonic must be callable or None")

    started_at = clock()
    runtime_seconds = float(max_runtime_minutes * 60)

    target_hour = await select_remote_catch_up_hour(
        repository=repository,
        venue=normalized_venue,
        instrument=normalized_instrument,
        latest_eligible_hour_utc=latest,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
        search_hours=search_hours,
    )
    if target_hour > latest:
        raise RemoteWorkerError(
            "Catch-up planner selected an hour after the eligible boundary"
        )

    completed: list[RemoteWorkerResult] = []
    stop_reason = "no_work"  # Fallback initialization

    while True:
        # Always admit the first selected hour. On later iterations, enforce
        # the cooperative runtime budget before starting more expensive work.
        if completed and clock() - started_at >= runtime_seconds:
            stop_reason = "runtime_budget"
            break

        try:
            result = await process_remote_hour(
                repository=repository,
                cryptohft=cryptohft,
                workspace=workspace,
                venue=normalized_venue,
                instrument=normalized_instrument,
                hour_utc=target_hour,
                latest_eligible_hour_utc=latest,
                lower_fraction=lower_fraction,
                upper_fraction=upper_fraction,
                producer_git_commit=producer_git_commit,
                use_api_key=use_api_key,
                batch_size=batch_size,
            )
        except RemoteFileNotFoundError:
            # Release eligibility is a scheduling boundary, not proof that the
            # provider has already published every required archive. Preserve
            # successfully completed hours and stop gracefully without falsely
            # claiming that the latest eligible hour completed.
            stop_reason = "source_unavailable" if completed else "no_work"

            log.info(
                "CATCH-UP STOPPED AT UNAVAILABLE SOURCE: venue=%s "
                "instrument=%s target=%s completed_hours=%d stop_reason=%s",
                normalized_venue,
                normalized_instrument,
                target_hour.isoformat(),
                len(completed),
                stop_reason,
            )
            break
        except Exception as exc:
            log.error(
                "=== PROCESSING HOUR FAILED === venue=%s instrument=%s "
                "hour=%s error_type=%s error_msg=%s",
                normalized_venue,
                normalized_instrument,
                target_hour.isoformat(),
                type(exc).__name__,
                str(exc),
                exc_info=True,
            )
            raise
        completed.append(result)

        # No later target exists.
        if target_hour == latest:
            stop_reason = "caught_up"
            break

        # The operation limit is checked after completing the current hour.
        if len(completed) >= max_hours_per_run:
            stop_reason = "max_hours_per_run"
            break

        # Do not admit another hour if the cooperative budget expired while
        # processing or publishing the current hour.
        if clock() - started_at >= runtime_seconds:
            stop_reason = "runtime_budget"
            break

        # Refresh Hugging Face state after every successful operation.
        #
        # This is required when the completed operation repaired price at an
        # older usable L2 frontier. The refreshed planner can then skip an
        # immutable checkpoint-less L2 artifact and select the first later
        # missing hour instead of mechanically attempting the blocked hour.
        next_target_hour = await select_remote_catch_up_hour(
            repository=repository,
            venue=normalized_venue,
            instrument=normalized_instrument,
            latest_eligible_hour_utc=latest,
            lower_fraction=lower_fraction,
            upper_fraction=upper_fraction,
            search_hours=search_hours,
        )

        if next_target_hour > latest:
            raise RemoteWorkerError(
                "Catch-up planner selected an hour after the eligible boundary"
            )

        if next_target_hour <= target_hour:
            raise RemoteWorkerError(
                "Catch-up replanning did not advance after a successful "
                "operation; "
                f"completed_hour={target_hour.isoformat()} "
                f"selected_hour={next_target_hour.isoformat()}"
            )

        log.info(
            "CATCH-UP REPLANNED AFTER SUCCESS: completed=%s next_target=%s "
            "skipped_hours=%d",
            target_hour.isoformat(),
            next_target_hour.isoformat(),
            int((next_target_hour - target_hour).total_seconds() // 3_600) - 1,
        )

        target_hour = next_target_hour

    return RemoteCatchUpRunResult(
        venue=normalized_venue,
        instrument=normalized_instrument,
        latest_eligible_hour_utc=latest,
        results=tuple(completed),
        max_hours_per_run=max_hours_per_run,
        max_runtime_minutes=max_runtime_minutes,
        stop_reason=stop_reason,
    )


async def _inspect_existing_state(
    repository: HuggingFaceDatasetRepository,
    *,
    l2_key: RemoteArtifactKey,
    price_key: RemoteArtifactKey | None,
) -> _ExistingRemoteState:
    revision = await asyncio.to_thread(
        repository.current_revision,
    )

    downloaded_l2 = await asyncio.to_thread(
        repository.download_artifact,
        l2_key,
        revision=revision,
    )

    if downloaded_l2 is None:
        l2_artifact = None
    else:
        if not isinstance(
            downloaded_l2.artifact,
            RemoteL2ProcessedArtifact,
        ):
            raise RemoteWorkerError("L2 key resolved to a non-L2 remote artifact")

        l2_artifact = downloaded_l2.artifact

    if price_key is None:
        price_artifact = None
    else:
        downloaded_price = await asyncio.to_thread(
            repository.download_artifact,
            price_key,
            revision=revision,
        )

        if downloaded_price is None:
            price_artifact = None
        else:
            if not isinstance(
                downloaded_price.artifact,
                RemotePriceProcessedArtifact,
            ):
                raise RemoteWorkerError(
                    "Price key resolved to a non-price remote artifact"
                )

            price_artifact = downloaded_price.artifact

    return _ExistingRemoteState(
        pinned_revision=revision,
        l2_artifact=l2_artifact,
        price_artifact=price_artifact,
    )


async def _predecessor_checkpoint(
    repository: HuggingFaceDatasetRepository,
    *,
    target_key: RemoteArtifactKey,
    pinned_revision: str,
    predecessor_required: bool,
) -> bytes | None:
    try:
        predecessor = await asyncio.to_thread(
            repository.download_l2_predecessor_checkpoint,
            target_key,
            revision=pinned_revision,
        )
    except HuggingFaceArtifactNotFoundError as exc:
        if predecessor_required:
            raise RemoteWorkerCheckpointBlockedError(
                "The required immediately preceding L2 artifact does not "
                "exist at the pinned Hugging Face revision"
            ) from exc

        return None

    checkpoint = predecessor.checkpoint_bytes

    if checkpoint is None and predecessor_required:
        raise RemoteWorkerCheckpointBlockedError(
            "The immediately preceding L2 artifact has no output checkpoint"
        )

    return checkpoint


def _archive_by_kind(
    archives: tuple[ProcessingSourceArchive, ...],
    data_kind: SourceDataKind,
) -> ProcessingSourceArchive:
    selected = tuple(
        archive for archive in archives if archive.spec.data_kind is data_kind
    )

    if len(selected) != 1:
        raise RemoteWorkerError(
            f"Expected exactly one {data_kind.value} source archive"
        )

    return selected[0]


def _l2_artifact_for_publication(
    output: HeadlessL2ProcessingOutput,
) -> RemoteL2ProcessedArtifact:
    """Return an explicit L2 result without propagating unsafe checkpoints.

    A deterministically processed hour may be partially or completely
    analytically invalid. Publishing its verified analytical channels records
    that fact and gives catch-up a durable blocked-hour marker.

    An all-invalid hour must not advance checkpoint state even when sequence
    replay technically retained both book sides. In that case the analytical
    artifact is published with no output checkpoint.
    """

    if not isinstance(output, HeadlessL2ProcessingOutput):
        raise TypeError("output must be HeadlessL2ProcessingOutput")

    valid_count = output.quality_summary.get("valid_count")

    if (
        isinstance(valid_count, bool)
        or not isinstance(valid_count, int)
        or not 0 <= valid_count <= 3_600
    ):
        raise RemoteWorkerError("Headless L2 output has an invalid valid_count")

    artifact = output.artifact

    if valid_count > 0 or artifact.output_checkpoint is None:
        return artifact

    # Sequence-valid state can still remain locked, crossed, or otherwise
    # analytically unusable for all 3,600 seconds. Retain the explicit invalid
    # analytical artifact, but remove the checkpoint so later hours cannot
    # inherit that structurally unusable state.
    manifest = replace(
        artifact.manifest,
        output_checkpoint_content_sha256=None,
    )

    return RemoteL2ProcessedArtifact(
        manifest=manifest,
        encoded=artifact.encoded,
        output_checkpoint=None,
    )


async def process_remote_hour(
    *,
    repository: HuggingFaceDatasetRepository,
    cryptohft: CryptoHFTConfig,
    workspace: RemoteWorkerWorkspace,
    venue: str,
    instrument: str,
    hour_utc: datetime,
    latest_eligible_hour_utc: datetime,
    lower_fraction: Decimal,
    upper_fraction: Decimal,
    producer_git_commit: str | None,
    use_api_key: bool = False,
    batch_size: int = 131_072,
) -> RemoteWorkerResult:
    """Acquire, process, and publish one completed remote source hour."""
    log.info(
        "=== PROCESSING HOUR START === venue=%s instrument=%s hour=%s",
        venue,
        instrument,
        hour_utc.isoformat(),
    )

    if not isinstance(
        repository,
        HuggingFaceDatasetRepository,
    ):
        raise TypeError("repository must be HuggingFaceDatasetRepository")

    if not isinstance(cryptohft, CryptoHFTConfig):
        raise TypeError("cryptohft must be CryptoHFTConfig")

    if not isinstance(workspace, RemoteWorkerWorkspace):
        raise TypeError("workspace must be RemoteWorkerWorkspace")

    target_hour = require_utc_hour(
        "hour_utc",
        hour_utc,
    )
    latest_eligible = require_utc_hour(
        "latest_eligible_hour_utc",
        latest_eligible_hour_utc,
    )

    if target_hour > latest_eligible:
        raise RemoteWorkerError(
            "Target hour is newer than the latest release-eligible hour"
        )

    normalized_venue, normalized_instrument, price_required = _normalized_chain(
        venue,
        instrument,
    )

    preset = _preset_for_chain(
        venue=normalized_venue,
        instrument=normalized_instrument,
        lower_fraction=lower_fraction,
        upper_fraction=upper_fraction,
    )

    target_l2_key = _l2_key(
        venue=normalized_venue,
        instrument=normalized_instrument,
        hour_utc=target_hour,
        preset=preset,
    )
    target_price_key = (
        _price_key(
            instrument=normalized_instrument,
            hour_utc=target_hour,
        )
        if price_required
        else None
    )

    existing = await _inspect_existing_state(
        repository,
        l2_key=target_l2_key,
        price_key=target_price_key,
    )

    log.info(
        "REMOTE EXISTING STATE: venue=%s instrument=%s hour=%s "
        "pinned_revision=%s l2_exists=%s l2_checkpoint_exists=%s "
        "price_required=%s price_exists=%s",
        normalized_venue,
        normalized_instrument,
        target_hour.isoformat(),
        existing.pinned_revision,
        existing.l2_artifact is not None,
        bool(
            existing.l2_artifact is not None
            and existing.l2_artifact.output_checkpoint is not None
        ),
        price_required,
        existing.price_artifact is not None,
    )

    if (
        existing.l2_artifact is not None
        and existing.l2_artifact.output_checkpoint is None
    ):
        raise RemoteWorkerCheckpointBlockedError(
            "The existing target L2 artifact has no output checkpoint; "
            "the chain cannot advance"
        )

    l2_missing = existing.l2_artifact is None
    price_missing = bool(price_required and existing.price_artifact is None)

    if not l2_missing and not price_missing:
        return RemoteWorkerResult(
            venue=normalized_venue,
            instrument=normalized_instrument,
            hour_utc=target_hour,
            pinned_input_revision=existing.pinned_revision,
            l2_created=False,
            l2_revision=existing.pinned_revision,
            l2_reused=True,
            price_required=price_required,
            price_created=False,
            price_revision=(existing.pinned_revision if price_required else None),
            price_reused=price_required,
            source_downloaded_count=0,
            source_reused_count=0,
        )

    checkpoint_bytes: bytes | None = None

    if l2_missing:
        # A verified predecessor remains preferred, but its absence does not
        # prove that the target is unusable: the target archive may contain a
        # complete opening snapshot.
        #
        # Without a predecessor, an update-only target cannot advance a
        # checkpoint frontier. It may still be published as an explicit
        # checkpoint-less, all-invalid blocked-hour artifact.
        checkpoint_bytes = await _predecessor_checkpoint(
            repository,
            target_key=target_l2_key,
            pinned_revision=existing.pinned_revision,
            predecessor_required=False,
        )

        log.info(
            "REMOTE L2 INITIALIZATION INPUT: venue=%s instrument=%s "
            "hour=%s predecessor_checkpoint_available=%s "
            "checkpoint_bytes=%d",
            normalized_venue,
            normalized_instrument,
            target_hour.isoformat(),
            checkpoint_bytes is not None,
            len(checkpoint_bytes) if checkpoint_bytes is not None else 0,
        )

    requested_specs: list[SourceFileSpec] = []

    if l2_missing:
        requested_specs.append(
            _source_spec(
                venue=normalized_venue,
                instrument=normalized_instrument,
                hour_utc=target_hour,
                data_kind=SourceDataKind.ORDERBOOK,
            )
        )

    if price_missing:
        requested_specs.append(
            _source_spec(
                venue="binance_futures",
                instrument=normalized_instrument,
                hour_utc=target_hour,
                data_kind=SourceDataKind.TRADES,
            )
        )

    log.info(
        "REMOTE SOURCE PLAN: venue=%s instrument=%s hour=%s sources=%s",
        normalized_venue,
        normalized_instrument,
        target_hour.isoformat(),
        [
            {
                "venue": spec.venue,
                "instrument": spec.symbol,
                "data_kind": spec.data_kind.value,
                "remote_path": spec.remote_path,
            }
            for spec in requested_specs
        ],
    )

    acquisition = await acquire_remote_worker_archives(
        tuple(requested_specs),
        cryptohft=cryptohft,
        workspace=workspace,
        latest_eligible_hour_utc=latest_eligible,
        use_api_key=use_api_key,
    )

    log.info(
        "REMOTE SOURCE ACQUISITION COMPLETE: venue=%s instrument=%s "
        "hour=%s source_count=%d downloaded=%d reused=%d sources=%s",
        normalized_venue,
        normalized_instrument,
        target_hour.isoformat(),
        acquisition.source_count,
        acquisition.downloaded_count,
        acquisition.reused_count,
        [
            {
                "remote_path": archive.spec.remote_path,
                "file_size_bytes": archive.file_size_bytes,
                "content_sha256": archive.content_sha256,
            }
            for archive in acquisition.processing_archives
        ],
    )

    l2_output = None
    price_output = None

    if l2_missing:
        orderbook_archive = _archive_by_kind(
            acquisition.processing_archives,
            SourceDataKind.ORDERBOOK,
        )

        l2_output = await asyncio.to_thread(
            process_l2_archive_headlessly,
            orderbook_archive,
            preset,
            input_checkpoint_bytes=checkpoint_bytes,
            producer_git_commit=producer_git_commit,
            batch_size=batch_size,
        )

        log.info(
            "REMOTE L2 PROCESSING RESULT: venue=%s instrument=%s hour=%s "
            "events=%d snapshots=%d continuity_mismatches=%d "
            "valid_seconds=%s degraded_seconds=%s invalid_seconds=%s "
            "input_checkpoint_sha256=%s output_checkpoint_sha256=%s "
            "analytical_content_sha256=%s",
            normalized_venue,
            normalized_instrument,
            target_hour.isoformat(),
            l2_output.replay_event_count,
            l2_output.replay_snapshot_count,
            l2_output.replay_continuity_mismatch_count,
            l2_output.quality_summary.get("valid_count"),
            l2_output.quality_summary.get("degraded_count"),
            l2_output.quality_summary.get("invalid_count"),
            l2_output.artifact.manifest.input_checkpoint_content_sha256,
            l2_output.artifact.manifest.output_checkpoint_content_sha256,
            l2_output.artifact.manifest.content_sha256,
        )

        original_artifact = l2_output.artifact
        publication_artifact = _l2_artifact_for_publication(
            l2_output,
        )

        if publication_artifact.output_checkpoint is None:
            if original_artifact.output_checkpoint is not None:
                reason = "all_analytical_seconds_invalid"
            else:
                reason = "replay_finished_without_usable_checkpoint"

            log.warning(
                "REMOTE L2 BLOCKED-HOUR MARKER: venue=%s instrument=%s "
                "hour=%s reason=%s valid_seconds=%s invalid_seconds=%s "
                "checkpoint_published=false",
                normalized_venue,
                normalized_instrument,
                target_hour.isoformat(),
                reason,
                l2_output.quality_summary.get("valid_count"),
                l2_output.quality_summary.get("invalid_count"),
            )

        # Keep all subsequent publication, provenance logging, and result
        # construction on the checkpoint-safe artifact.
        l2_output = replace(
            l2_output,
            artifact=publication_artifact,
        )

    if price_missing:
        trade_archive = _archive_by_kind(
            acquisition.processing_archives,
            SourceDataKind.TRADES,
        )

        price_output = await asyncio.to_thread(
            process_price_archives_headlessly,
            trade_archive.spec,
            (trade_archive,),
            producer_git_commit=producer_git_commit,
            batch_size=batch_size,
        )

    l2_publication: HuggingFacePublicationResult | None = None
    price_publication: HuggingFacePublicationResult | None = None

    if l2_output is not None:
        l2_publication = await asyncio.to_thread(
            repository.publish_artifact,
            l2_output.artifact,
        )

        log.info(
            "REMOTE HF L2 PUBLICATION: venue=%s instrument=%s hour=%s "
            "created=%s revision=%s concurrent_commit_observed=%s "
            "artifact_path=%s manifest_path=%s",
            normalized_venue,
            normalized_instrument,
            target_hour.isoformat(),
            l2_publication.created,
            l2_publication.revision,
            l2_publication.concurrent_commit_observed,
            l2_output.artifact.manifest.key.relative_path,
            l2_output.artifact.manifest.key.manifest_relative_path,
        )

    if price_output is not None:
        price_publication = await asyncio.to_thread(
            repository.publish_artifact,
            price_output.artifact,
        )

        log.info(
            "REMOTE HF PRICE PUBLICATION: instrument=%s hour=%s "
            "created=%s revision=%s concurrent_commit_observed=%s "
            "artifact_path=%s manifest_path=%s",
            normalized_instrument,
            target_hour.isoformat(),
            price_publication.created,
            price_publication.revision,
            price_publication.concurrent_commit_observed,
            price_output.artifact.manifest.key.relative_path,
            price_output.artifact.manifest.key.manifest_relative_path,
        )

    if l2_output is not None:
        l2_manifest = l2_output.artifact.manifest

        log.info(
            "REMOTE L2 PROVENANCE: venue=%s instrument=%s hour=%s "
            "source_hours=%s input_checkpoint_sha256=%s "
            "output_checkpoint_sha256=%s analytical_content_sha256=%s "
            "manifest_sha256=%s producer_git_commit=%s",
            normalized_venue,
            normalized_instrument,
            target_hour.isoformat(),
            [
                {
                    "hour_utc": source.hour_utc.isoformat(),
                    "content_sha256": source.content_sha256,
                }
                for source in l2_manifest.source_hours
            ],
            l2_manifest.input_checkpoint_content_sha256,
            l2_manifest.output_checkpoint_content_sha256,
            l2_manifest.content_sha256,
            l2_manifest.manifest_sha256,
            l2_manifest.producer_git_commit,
        )

    result = RemoteWorkerResult(
        venue=normalized_venue,
        instrument=normalized_instrument,
        hour_utc=target_hour,
        pinned_input_revision=existing.pinned_revision,
        l2_created=(l2_publication.created if l2_publication is not None else False),
        l2_revision=(
            l2_publication.revision
            if l2_publication is not None
            else existing.pinned_revision
        ),
        l2_reused=(
            not l2_missing
            or (l2_publication is not None and not l2_publication.created)
        ),
        price_required=price_required,
        price_created=(
            price_publication.created if price_publication is not None else False
        ),
        price_revision=(
            price_publication.revision
            if price_publication is not None
            else (
                existing.pinned_revision
                if price_required and existing.price_artifact is not None
                else None
            )
        ),
        price_reused=bool(
            price_required
            and (
                existing.price_artifact is not None
                or (price_publication is not None and not price_publication.created)
            )
        ),
        source_downloaded_count=acquisition.downloaded_count,
        source_reused_count=acquisition.reused_count,
    )
    log.info(
        "=== PROCESSING HOUR COMPLETE === venue=%s instrument=%s hour=%s "
        "l2_created=%s price_created=%s sources_downloaded=%s",
        result.venue,
        result.instrument,
        result.hour_utc.isoformat(),
        result.l2_created,
        result.price_created,
        result.source_downloaded_count,
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Acquire, process, and publish one completed CryptoHFTData "
            "hour to a private Hugging Face dataset."
        )
    )
    parser.add_argument(
        "--venue",
        required=True,
        choices=(
            "binance_futures",
            "bybit",
            "okx_futures",
        ),
    )
    parser.add_argument(
        "--instrument",
        required=True,
    )
    parser.add_argument(
        "--hour",
        type=_canonical_utc_hour,
        default=None,
        help=(
            "Explicit completed UTC hour. When omitted, process the newest "
            "release-eligible hour."
        ),
    )
    parser.add_argument(
        "--release-delay-minutes",
        type=_positive_integer,
        default=15,
    )
    parser.add_argument(
        "--catch-up-hours",
        type=_positive_integer,
        default=720,
        help=(
            "Bounded newest-to-oldest HF frontier search used when --hour "
            "is omitted. The default covers established historical seed "
            "frontiers during initial chain catch-up."
        ),
    )
    parser.add_argument(
        "--max-hours-per-run",
        type=_positive_integer,
        default=4,
        help=(
            "Maximum number of contiguous remote hours processed when "
            "--hour is omitted."
        ),
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=_positive_integer,
        default=240,
        help=(
            "Cooperative runtime budget for omitted-hour catch-up. "
            "The worker finishes an already-started hour but does not "
            "admit another hour after the budget expires."
        ),
    )
    parser.add_argument(
        "--depth-lower",
        type=_depth_fraction,
        required=True,
    )
    parser.add_argument(
        "--depth-upper",
        type=_depth_fraction,
        required=True,
    )
    parser.add_argument(
        "--hf-repo-id",
        default=os.environ.get(
            "L2SHOCK_HF_REPO_ID",
            "",
        ),
    )
    parser.add_argument(
        "--hf-revision",
        default=os.environ.get(
            "L2SHOCK_HF_REVISION",
            "main",
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_integer,
        default=131_072,
    )
    parser.add_argument(
        "--use-api-key",
        action="store_true",
    )
    parser.add_argument(
        "--workspace-parent",
        type=Path,
        default=None,
    )

    return parser


def _required_environment_secret(name: str) -> SecretStr:
    value = str(os.environ.get(name, "") or "").strip()

    if not value:
        raise RemoteWorkerError(f"Required process secret {name} is not configured")

    return SecretStr(value)


async def _run_from_arguments(
    args: argparse.Namespace,
) -> RemoteWorkerResult | RemoteCatchUpRunResult:
    repo_id = str(args.hf_repo_id or "").strip()

    if not repo_id:
        raise RemoteWorkerError(
            "Hugging Face dataset repo ID is required through "
            "--hf-repo-id or L2SHOCK_HF_REPO_ID"
        )

    current = now_utc()
    latest_eligible = latest_release_eligible_hour(
        current,
        release_delay_minutes=args.release_delay_minutes,
    )

    hf_token = _required_environment_secret("HF_TOKEN")

    crypto_api_key = str(
        os.environ.get(
            "L2SHOCK__CRYPTOHFT__API_KEY",
            "",
        )
        or ""
    ).strip()

    cryptohft = CryptoHFTConfig(
        api_key=SecretStr(crypto_api_key),
        expected_release_delay_minutes=(args.release_delay_minutes),
    )

    repository = HuggingFaceDatasetRepository(
        repo_id=repo_id,
        revision=args.hf_revision,
        token=hf_token,
    )

    producer_git_commit = str(os.environ.get("GITHUB_SHA", "") or "").strip() or None

    with temporary_remote_worker_workspace(
        parent=args.workspace_parent,
    ) as workspace:
        if args.hour is not None:
            if args.hour > latest_eligible:
                raise RemoteWorkerError(
                    "Requested target hour is newer than the "
                    "release-eligible boundary"
                )

            return await process_remote_hour(
                repository=repository,
                cryptohft=cryptohft,
                workspace=workspace,
                venue=args.venue,
                instrument=args.instrument,
                hour_utc=args.hour,
                latest_eligible_hour_utc=latest_eligible,
                lower_fraction=args.depth_lower,
                upper_fraction=args.depth_upper,
                producer_git_commit=producer_git_commit,
                use_api_key=bool(args.use_api_key),
                batch_size=args.batch_size,
            )

        return await process_remote_catch_up(
            repository=repository,
            cryptohft=cryptohft,
            workspace=workspace,
            venue=args.venue,
            instrument=args.instrument,
            latest_eligible_hour_utc=latest_eligible,
            lower_fraction=args.depth_lower,
            upper_fraction=args.depth_upper,
            search_hours=args.catch_up_hours,
            max_hours_per_run=args.max_hours_per_run,
            max_runtime_minutes=args.max_runtime_minutes,
            producer_git_commit=producer_git_commit,
            use_api_key=bool(args.use_api_key),
            batch_size=args.batch_size,
        )


def main(argv: list[str] | None = None) -> int:
    import logging
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        stream=sys.stdout,
    )

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        result = asyncio.run(_run_from_arguments(args))

    except KeyboardInterrupt:
        print(
            "Remote worker was interrupted.",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.INTERRUPTED)

    except RemoteWorkerCheckpointBlockedError as exc:
        print(
            f"Remote checkpoint chain is blocked: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.CHECKPOINT_CHAIN_BLOCKED)

    except RemoteFileNotFoundError as exc:
        print(
            f"Remote source is not currently available: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.SOURCE_UNAVAILABLE)

    except HuggingFaceRepositoryError as exc:
        print(
            f"Hugging Face operation failed: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.HUGGING_FACE_ERROR)

    except (
        AcquisitionError,
        ProcessingError,
        RemoteWorkerError,
        TypeError,
        ValueError,
    ) as exc:
        print(
            "Remote worker input or processing contract failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.INPUT_OR_CONTRACT_ERROR)

    except Exception as exc:
        print(
            "Unexpected remote-worker failure: " f"{type(exc).__name__}",
            file=sys.stderr,
        )
        return int(RemoteWorkerExitStatus.UNEXPECTED_ERROR)

    print(
        json.dumps(
            result.to_dict(),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return int(RemoteWorkerExitStatus.OK)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "RemoteCatchUpRunResult",
    "RemoteWorkerCheckpointBlockedError",
    "RemoteWorkerError",
    "RemoteWorkerExitStatus",
    "RemoteWorkerResult",
    "build_parser",
    "main",
    "process_remote_catch_up",
    "process_remote_hour",
    "select_remote_catch_up_hour",
]

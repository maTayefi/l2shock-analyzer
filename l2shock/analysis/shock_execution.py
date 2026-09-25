# l2shock/analysis/shock_execution.py
"""Execute a price-independent Shock-Start candidate scan.

Reads verified one-second L2 input, then proposes Total-L2 A-B-C starts.
This stage neither ranks corroborating channels nor claims causal events.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction

from sqlalchemy.orm import Session

from l2shock.analysis.shock_dataset import (
    ShockDatasetRequest,
    VerifiedShockDataset,
    load_shock_dataset_from_repository,
    load_verified_shock_dataset,
)
from l2shock.analysis.shock_start import (
    ShockStartConfig,
    ShockStartHypothesis,
    propose_shock_starts,
)

SHOCK_EXECUTION_SCHEMA = "l2shock.shock_start_candidate_scan"
SHOCK_EXECUTION_SCHEMA_VERSION = 1
SHOCK_DETECTOR_VERSION = "total_l2_abc_candidate_v1"


@dataclass(frozen=True, slots=True)
class ShockCandidateScan:
    """Verified input plus reproducible, retrospective start hypotheses."""

    dataset: VerifiedShockDataset
    config: ShockStartConfig
    hypotheses: tuple[ShockStartHypothesis, ...]
    scan_id: str

    @property
    def active_scale_names(self) -> tuple[str, ...]:
        """Configured scales that actually produced hypotheses."""
        present = {item.scale_name for item in self.hypotheses}
        return tuple(
            scale.name for scale in self.config.scales if scale.name in present
        )

    @property
    def total_range(self) -> Fraction | None:
        """Range of valid same-second Bid+Ask values over the entire scan."""
        from l2shock.ingest.sampling import BookSampleQuality

        totals = tuple(
            Fraction(second.bid_liquidity) + Fraction(second.ask_liquidity)
            for second in self.dataset.seconds
            if second.quality is BookSampleQuality.VALID
        )
        return max(totals) - min(totals) if totals else None


def _scan_id(dataset: VerifiedShockDataset, config: ShockStartConfig) -> str:
    # Only data-affecting fields belong here. Do not add chart timeframe,
    # price bounds, UI selection, or result presentation preferences.
    identity = {
        "schema": SHOCK_EXECUTION_SCHEMA,
        "schema_version": SHOCK_EXECUTION_SCHEMA_VERSION,
        "detector_version": SHOCK_DETECTOR_VERSION,
        "l2_input_id": dataset.input_id,
        "acceleration_ratio": config.acceleration_ratio,
        "scales": [
            {
                "name": scale.name,
                "minimum_leg_fraction": str(Fraction(scale.minimum_leg_fraction)),
                "pivot_radius_seconds": scale.pivot_radius_seconds,
                "forward_radius_multiplier": scale.forward_radius_multiplier,
            }
            for scale in config.scales
        ],
    }
    canonical = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def _execute(
    dataset: VerifiedShockDataset,
    config: ShockStartConfig | None,
) -> ShockCandidateScan:
    selected = config if config is not None else ShockStartConfig()
    if not isinstance(selected, ShockStartConfig):
        raise TypeError("config must be ShockStartConfig")

    hypotheses = propose_shock_starts(
        dataset.seconds,
        config=selected,
    )

    # Fail closed if a future detector accidentally emits a candidate on
    # an unowned or invalid second. The current detector also enforces gaps.
    from l2shock.ingest.sampling import BookSampleQuality

    for item in hypotheses:
        if any(
            dataset.seconds[index].quality is not BookSampleQuality.VALID
            for index in range(item.a_index, item.c_index + 1)
        ):
            raise ValueError("Shock candidate crosses invalid L2 coverage")
        if (
            dataset.seconds[item.a_index].timestamp_utc != item.a_utc
            or dataset.seconds[item.b_index].timestamp_utc != item.b_utc
            or dataset.seconds[item.c_index].timestamp_utc != item.c_utc
        ):
            raise ValueError("Shock candidate timestamps do not belong to L2 input")

    return ShockCandidateScan(
        dataset=dataset,
        config=selected,
        hypotheses=hypotheses,
        scan_id=_scan_id(dataset, selected),
    )


def run_shock_scan_from_repository(
    repository,
    request: ShockDatasetRequest,
    *,
    config: ShockStartConfig | None = None,
) -> ShockCandidateScan:
    """Testable repository seam; repository retains verified-read contract."""
    return _execute(
        load_shock_dataset_from_repository(repository, request),
        config,
    )


def run_verified_shock_scan(
    session: Session,
    request: ShockDatasetRequest,
    *,
    config: ShockStartConfig | None = None,
) -> ShockCandidateScan:
    """Production entry point; uses verified PostgreSQL L2 reads only."""
    return _execute(
        load_verified_shock_dataset(session, request),
        config,
    )


__all__ = [
    "SHOCK_DETECTOR_VERSION",
    "SHOCK_EXECUTION_SCHEMA",
    "SHOCK_EXECUTION_SCHEMA_VERSION",
    "ShockCandidateScan",
    "run_shock_scan_from_repository",
    "run_verified_shock_scan",
]

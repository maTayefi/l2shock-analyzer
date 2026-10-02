# l2shock/ui/analysis_inputs.py
"""LM-independent Analysis input helpers.

These helpers used to live in the LM-owned ``analysis_controls.py``. They are
shared by the Shock-Start tab and must never import LM detection, ranking,
price-filter, or LM chart modules.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from l2shock.db import AnalyticalRepository
from l2shock.db.engine import session_scope
from l2shock.timeutils import local_to_utc


class AnalysisInputError(ValueError):
    """A user-supplied Analysis input cannot form a valid value."""


@dataclass(frozen=True, slots=True)
class AnalysisPresetOption:
    """Small immutable preset identity safe to transfer to the event loop."""

    preset_hash: str
    base: str
    algorithm_version: str
    created_at: datetime
    config_json: Mapping[str, Any]

    def __post_init__(self) -> None:
        digest = str(self.preset_hash or "").strip()

        if (
            digest != digest.lower()
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise AnalysisInputError(
                "preset_hash must be a canonical lowercase SHA-256"
            )

        base = str(self.base or "").strip().upper()

        if base not in {"BTC", "ETH"}:
            raise AnalysisInputError("Preset base must be BTC or ETH")

        algorithm = str(self.algorithm_version or "").strip()

        if not algorithm:
            raise AnalysisInputError("Preset algorithm_version cannot be blank")

        if not isinstance(self.created_at, datetime):
            raise AnalysisInputError("Preset created_at must be a datetime")

        object.__setattr__(self, "preset_hash", digest)
        object.__setattr__(self, "base", base)
        object.__setattr__(self, "algorithm_version", algorithm)
        object.__setattr__(self, "config_json", dict(self.config_json))

    @property
    def label(self) -> str:
        depth = self.config_json.get("depth_band")
        depth_label = ""

        if isinstance(depth, Mapping):
            lower = str(depth.get("lower_fraction", "")).strip()
            upper = str(depth.get("upper_fraction", "")).strip()

            if lower and upper:
                depth_label = f" | depth {lower}..{upper}"

        raw_markets = self.config_json.get("eligible_markets")
        market_labels: list[str] = []

        if isinstance(raw_markets, (list, tuple)):
            for raw_market in raw_markets:
                if not isinstance(raw_market, Mapping):
                    continue

                venue = str(raw_market.get("venue", "")).strip()
                instrument = str(raw_market.get("instrument", "")).strip()

                if venue and instrument:
                    market_labels.append(f"{venue}/{instrument}")

        market_label = (
            " + ".join(sorted(set(market_labels)))
            if market_labels
            else "market composition unavailable"
        )

        return (
            f"{self.base} | {market_label}{depth_label} | "
            f"{self.algorithm_version} | "
            f"{self.preset_hash[:12]}"
        )


def load_enabled_analysis_presets() -> tuple[AnalysisPresetOption, ...]:
    """Load enabled immutable presets inside the calling worker thread."""
    with session_scope() as session:
        presets = AnalyticalRepository(session).list_presets(
            enabled_only=True,
        )

        return tuple(
            AnalysisPresetOption(
                preset_hash=preset.preset_hash,
                base=preset.base,
                algorithm_version=preset.algorithm_version,
                created_at=preset.created_at,
                config_json=preset.config_json,
            )
            for preset in presets
        )


def parse_local_analysis_datetime(
    date_text: object,
    time_text: object,
    *,
    timezone_name: str,
    field_name: str,
) -> datetime:
    """Parse one strict user-local datetime into UTC."""
    date_value = str(date_text or "").strip()
    time_value = str(time_text or "").strip()

    if not date_value or not time_value:
        raise AnalysisInputError(f"{field_name} date and time are required")

    try:
        local_naive = datetime.fromisoformat(f"{date_value}T{time_value}")
    except ValueError as exc:
        raise AnalysisInputError(
            f"{field_name} must contain a valid local date and time"
        ) from exc

    try:
        return local_to_utc(
            local_naive,
            timezone_name,
            original_text=f"{date_value} {time_value}",
        )
    except (TypeError, ValueError) as exc:
        raise AnalysisInputError(str(exc)) from exc


__all__ = [
    "AnalysisInputError",
    "AnalysisPresetOption",
    "load_enabled_analysis_presets",
    "parse_local_analysis_datetime",
]

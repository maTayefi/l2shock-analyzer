# l2shock/ui/analysis_controls.py
"""Pure control parsing and presentation helpers for the Analysis tab.

This module contains no NiceGUI elements and owns no application task. It
translates validated UI values into immutable analysis contracts and converts
completed immutable results into sortable presentation rows.

Exact analytical calculations remain in ``l2shock.analysis``. Float conversion
in this module is presentation-only and never changes persisted or analytical
Decimal values.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from collections.abc import Mapping

from l2shock.analysis import (
    AnalysisDatasetRequest,
    LiquidityMovementAnalysisConfig,
    LiquidityMovementAnalysisResult,
    PriceBounds,
    analysis_config_from_lm_config,
)
from l2shock.analysis.timeframes import get_timeframe
from l2shock.db import AnalyticalRepository
from l2shock.db.engine import session_scope
from l2shock.timeutils import local_to_utc, utc_to_local


class AnalysisControlError(ValueError):
    """An Analysis-tab input cannot form a valid immutable request."""


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
            raise AnalysisControlError(
                "preset_hash must be a canonical lowercase SHA-256"
            )

        base = str(self.base or "").strip().upper()

        if base not in {"BTC", "ETH"}:
            raise AnalysisControlError("Preset base must be BTC or ETH")

        algorithm = str(self.algorithm_version or "").strip()

        if not algorithm:
            raise AnalysisControlError("Preset algorithm_version cannot be blank")

        if not isinstance(self.created_at, datetime):
            raise AnalysisControlError("Preset created_at must be a datetime")

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

        return (
            f"{self.base}{depth_label} | "
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
        raise AnalysisControlError(f"{field_name} date and time are required")

    try:
        local_naive = datetime.fromisoformat(f"{date_value}T{time_value}")
    except ValueError as exc:
        raise AnalysisControlError(
            f"{field_name} must contain a valid local date and time"
        ) from exc

    try:
        return local_to_utc(
            local_naive,
            timezone_name,
            original_text=f"{date_value} {time_value}",
        )
    except (TypeError, ValueError) as exc:
        raise AnalysisControlError(str(exc)) from exc


def parse_positive_integer(
    value: object,
    *,
    field_name: str,
    maximum: int | None = None,
) -> int:
    """Parse one positive integer from a NiceGUI numeric value."""
    if isinstance(value, bool):
        raise AnalysisControlError(f"{field_name} must be a positive integer")

    if isinstance(value, int):
        result = value
    elif isinstance(value, float) and value.is_integer():
        result = int(value)
    else:
        text = str(value or "").strip()

        try:
            result = int(text)
        except (TypeError, ValueError) as exc:
            raise AnalysisControlError(
                f"{field_name} must be a positive integer"
            ) from exc

        if str(result) != text and text not in {
            f"+{result}",
        }:
            raise AnalysisControlError(f"{field_name} must be a positive integer")

    if result <= 0:
        raise AnalysisControlError(f"{field_name} must be positive")

    if maximum is not None and result > maximum:
        raise AnalysisControlError(
            f"{field_name} must be less than or equal to {maximum}"
        )

    return result


def parse_nonnegative_integer(
    value: object,
    *,
    field_name: str,
    maximum: int | None = None,
) -> int:
    """Parse one non-negative integer from a NiceGUI numeric value."""
    if isinstance(value, bool):
        raise AnalysisControlError(f"{field_name} must be a non-negative integer")

    if isinstance(value, int):
        result = value
    elif isinstance(value, float) and value.is_integer():
        result = int(value)
    else:
        text = str(value or "").strip()

        try:
            result = int(text)
        except (TypeError, ValueError) as exc:
            raise AnalysisControlError(
                f"{field_name} must be a non-negative integer"
            ) from exc

        if str(result) != text and text not in {
            f"+{result}",
        }:
            raise AnalysisControlError(f"{field_name} must be a non-negative integer")

    if result < 0:
        raise AnalysisControlError(f"{field_name} must be non-negative")

    if maximum is not None and result > maximum:
        raise AnalysisControlError(
            f"{field_name} must be less than or equal to {maximum}"
        )

    return result


def parse_confirmation_fraction(value: object) -> Decimal:
    """Parse the exact LM confirmation fraction used by the detector."""
    text = str(value or "").strip()

    if not text:
        raise AnalysisControlError("LM confirmation retracement is required")

    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise AnalysisControlError(
            "LM confirmation retracement must be a valid decimal fraction"
        ) from exc

    if not result.is_finite():
        raise AnalysisControlError("LM confirmation retracement must be finite")

    if result <= 0 or result >= 1:
        raise AnalysisControlError("LM confirmation retracement must lie inside (0, 1)")

    return result


def parse_optional_price_bound(
    *,
    enabled: bool,
    value: object,
    field_name: str,
) -> Decimal | None:
    """Parse one optional positive exact-Decimal analysis price bound."""
    if not isinstance(enabled, bool):
        raise AnalysisControlError(f"{field_name} enabled state must be bool")

    if not enabled:
        return None

    text = str(value or "").strip()

    if not text:
        raise AnalysisControlError(f"{field_name} is enabled and requires a value")

    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise AnalysisControlError(
            f"{field_name} must be a valid decimal price"
        ) from exc

    if not result.is_finite() or result <= 0:
        raise AnalysisControlError(f"{field_name} must be finite and greater than zero")

    return result


def build_analysis_request_and_config(
    *,
    base: str,
    preset_hash: str,
    requested_start_utc: datetime,
    requested_end_utc: datetime,
    activity_timeframe: str,
    maximum_chart_bars: object,
    chart_timeframe_override: object = None,
    minimum_price_enabled: bool,
    minimum_price_value: object,
    maximum_price_enabled: bool,
    maximum_price_value: object,
    context_before: object,
    context_after: object,
    confirmation_fraction: object,
    top_n_height: object,
    top_n_sharpness: object,
    lm_settings: object,
) -> tuple[
    AnalysisDatasetRequest,
    LiquidityMovementAnalysisConfig,
]:
    """Construct exact immutable runtime inputs from UI values."""
    minimum_price = parse_optional_price_bound(
        enabled=minimum_price_enabled,
        value=minimum_price_value,
        field_name="Minimum price",
    )
    maximum_price = parse_optional_price_bound(
        enabled=maximum_price_enabled,
        value=maximum_price_value,
        field_name="Maximum price",
    )

    try:
        bounds = PriceBounds(
            min_price=minimum_price,
            max_price=maximum_price,
        )
    except ValueError as exc:
        raise AnalysisControlError(str(exc)) from exc

    raw_chart_timeframe = str(chart_timeframe_override or "").strip()

    if not raw_chart_timeframe:
        selected_chart_timeframe = None
    else:
        try:
            selected_chart_timeframe = get_timeframe(
                raw_chart_timeframe,
            )
        except ValueError as exc:
            raise AnalysisControlError(
                f"Unsupported chart timeframe: " f"{raw_chart_timeframe!r}."
            ) from exc

    request = AnalysisDatasetRequest(
        base=base,
        preset_hash=preset_hash,
        requested_start_utc=requested_start_utc,
        requested_end_utc=requested_end_utc,
        activity_timeframe=activity_timeframe,
        maximum_chart_bars=parse_positive_integer(
            maximum_chart_bars,
            field_name="Maximum chart bars",
            maximum=1_000_000,
        ),
        chart_timeframe_override=selected_chart_timeframe,
        price_bounds=bounds,
        context_before=parse_nonnegative_integer(
            context_before,
            field_name="Context bars before",
            maximum=10_000,
        ),
        context_after=parse_nonnegative_integer(
            context_after,
            field_name="Context bars after",
            maximum=10_000,
        ),
    )

    base_config = analysis_config_from_lm_config(
        lm_settings,
    )

    detection = replace(
        base_config.detection,
        confirmation_retracement_fraction=(
            parse_confirmation_fraction(
                confirmation_fraction,
            )
        ),
    )
    ranking = replace(
        base_config.ranking,
        top_n_height=parse_positive_integer(
            top_n_height,
            field_name="Top N by height",
            maximum=1_000,
        ),
        top_n_sharpness=parse_positive_integer(
            top_n_sharpness,
            field_name="Top N by sharpness",
            maximum=1_000,
        ),
    )

    config = LiquidityMovementAnalysisConfig(
        detection=detection,
        ranking=ranking,
        metrics=base_config.metrics,
    )

    return request, config


def _decimal_float(value: Decimal) -> float:
    try:
        return float(value)
    except (OverflowError, ValueError) as exc:
        raise AnalysisControlError(
            "Analysis result contains a value too large for table presentation"
        ) from exc


def build_analysis_table_rows(
    result: LiquidityMovementAnalysisResult,
    *,
    timezone_name: str,
) -> list[dict[str, object]]:
    """Build numeric sortable rows for selected LM candidates."""
    if not isinstance(
        result,
        LiquidityMovementAnalysisResult,
    ):
        raise TypeError("result must be LiquidityMovementAnalysisResult")

    rows: list[dict[str, object]] = []

    for row_number, ranking in enumerate(
        result.selected,
        start=1,
    ):
        candidate = ranking.candidate

        start_local = utc_to_local(
            candidate.start_time_utc,
            timezone_name,
        )
        end_local = utc_to_local(
            candidate.end_time_utc,
            timezone_name,
        )
        confirmation_local = (
            utc_to_local(
                candidate.confirmation_time_utc,
                timezone_name,
            )
            if candidate.confirmation_time_utc is not None
            else None
        )

        rows.append(
            {
                "id": row_number,
                "metric": candidate.metric.value,
                "timeframe": candidate.timeframe_label,
                "direction": candidate.direction.value,
                "population_rank": ranking.population_rank,
                "final_rank": ranking.final_rank,
                "start_time": start_local.isoformat(timespec="seconds"),
                "end_time": end_local.isoformat(timespec="seconds"),
                "confirmation_time": (
                    confirmation_local.isoformat(timespec="seconds")
                    if confirmation_local is not None
                    else ""
                ),
                "terminal_offline": candidate.terminal_offline,
                "start_value": _decimal_float(candidate.start_value),
                "end_value": _decimal_float(candidate.end_value),
                "absolute_height": _decimal_float(candidate.absolute_height),
                "relative_height": _decimal_float(candidate.relative_height),
                "bars": candidate.bars,
                "sharpness": _decimal_float(candidate.sharpness),
                "primary_evidence": _decimal_float(ranking.primary_evidence),
                "secondary_evidence": _decimal_float(ranking.secondary_evidence),
                "weighted_evidence": _decimal_float(ranking.priority_weighted_evidence),
                "height_percentile": _decimal_float(ranking.height.percentile),
                "height_modified_z": _decimal_float(ranking.height.modified_z),
                "height_normalized_z": _decimal_float(
                    ranking.height.normalized_positive_tail_modified_z
                ),
                "sharpness_percentile": _decimal_float(ranking.sharpness.percentile),
                "sharpness_modified_z": _decimal_float(ranking.sharpness.modified_z),
                "sharpness_normalized_z": _decimal_float(
                    ranking.sharpness.normalized_positive_tail_modified_z
                ),
                "start_extremeness": float(ranking.start_extremeness),
                "end_extremeness": float(ranking.end_extremeness),
                "boundary_extremeness": float(ranking.boundary_extremeness),
                "retracement_magnitude_quality": _decimal_float(
                    ranking.retracement_magnitude_quality
                ),
                "retracement_count_quality": _decimal_float(
                    ranking.retracement_count_quality
                ),
                "adverse_move_count": candidate.adverse_move_count,
                "adverse_move_total_fraction": _decimal_float(
                    candidate.adverse_move_total_fraction
                ),
                "degraded_bars": candidate.degraded_bar_count,
                "selected_height": ranking.selected_by_height,
                "selected_sharpness": ranking.selected_by_sharpness,
            }
        )

    return rows


def analysis_result_summary(
    result: LiquidityMovementAnalysisResult,
) -> str:
    """Return a compact coverage and candidate summary."""
    dataset = result.dataset

    complete_hours = sum(item.complete for item in dataset.coverage)
    missing_l2_hours = sum(not item.l2_present for item in dataset.coverage)
    missing_price_hours = sum(not item.price_present for item in dataset.coverage)
    partial_market_hours = sum(
        item.l2_market_coverage_degraded for item in dataset.coverage
    )
    activity_skipped = sum(
        item.skipped_seconds for item in dataset.activity.discontinuities
    )
    chart_skipped = sum(item.skipped_seconds for item in dataset.chart.discontinuities)

    return (
        f"Analysis {result.analysis_id[:12]} | "
        f"candidates={len(result.candidates)} | "
        f"selected={len(result.selected)} | "
        f"timeframes={','.join(result.timeframe_labels)} | "
        f"complete_hours={complete_hours}/{len(dataset.coverage)} | "
        f"missing_l2_hours={missing_l2_hours} | "
        f"missing_price_hours={missing_price_hours} | "
        f"partial_l2_market_hours={partial_market_hours} | "
        f"activity_discontinuity_seconds={activity_skipped} | "
        f"chart_discontinuity_seconds={chart_skipped}"
    )


__all__ = [
    "AnalysisControlError",
    "AnalysisPresetOption",
    "analysis_result_summary",
    "build_analysis_request_and_config",
    "build_analysis_table_rows",
    "load_enabled_analysis_presets",
    "parse_confirmation_fraction",
    "parse_local_analysis_datetime",
    "parse_nonnegative_integer",
    "parse_optional_price_bound",
    "parse_positive_integer",
]

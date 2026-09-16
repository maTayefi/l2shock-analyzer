# l2shock/maintenance_diagnostics.py
"""Read-only checkpoint, source-state, and analytical consistency diagnostics.

This module must never:

- delete checkpoint or raw artifacts;
- mutate source-hour state;
- replace analytical rows;
- repair stale operations;
- infer missing analytical content;
- treat diagnostics as maintenance authorization.

Every report is JSON-safe and bounded. Filesystem/database exceptions are
reported by type only so exception arguments cannot leak credentials or
sensitive local details.
"""

from __future__ import annotations

import math
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Final

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from l2shock.db.models import (
    FetchRun,
    L2HourlySeries,
    PriceHourlySeries,
    SourceHour,
)
from l2shock.ingest.checkpoint_codec import (
    checkpoint_encoding_info,
    encode_checkpoint,
    load_checkpoint_file,
)
from l2shock.processing.checkpoint_references import (
    collect_referenced_checkpoint_sha256s,
)
from l2shock.processing.checkpoint_store import (
    CheckpointIdentity,
    CheckpointStore,
)
from l2shock.timeutils import require_aware_utc

_MAX_RETAINED_ITEMS: Final[int] = 200

_DEFAULT_STALE_DOWNLOADING_AFTER: Final[timedelta] = timedelta(hours=2)
_DEFAULT_STALE_PROCESSING_AFTER: Final[timedelta] = timedelta(hours=6)
_DEFAULT_STALE_FETCH_RUN_AFTER: Final[timedelta] = timedelta(hours=2)

_DEFAULT_LARGE_CHECKPOINT_BYTES: Final[int] = 64 * 1024 * 1024
_DEFAULT_LARGE_L2_ROW_BYTES: Final[int] = 8 * 1024 * 1024
_DEFAULT_LARGE_PRICE_ROW_BYTES: Final[int] = 8 * 1024 * 1024

_VALID_BASES: Final[dict[str, str]] = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "BTC-USDT-SWAP": "BTC",
    "ETH-USDT-SWAP": "ETH",
}


class MaintenanceDiagnosticsError(ValueError):
    """Maintenance-diagnostics input violates its read-only contract."""


def _safe_exception_name(exc: BaseException) -> str:
    return f"Unexpected {type(exc).__name__}"


def _utc_text(value: datetime | None) -> str | None:
    if value is None:
        return None

    return require_aware_utc(
        "diagnostic datetime",
        value,
    ).isoformat()


def _canonical_sha256_or_none(value: object) -> str | None:
    if value is None:
        return None

    digest = str(value).strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        return None

    return digest


def _quality_content_hashes(
    quality: Mapping[str, object],
    *,
    singular_key: str,
    plural_key: str | None = None,
) -> tuple[tuple[str, ...], bool]:
    """Return canonical analytical hashes and malformed-state ownership.

    The singular field supports historical source metadata. New L2 processing
    may additionally record every immutable materialization in a plural list.
    """

    values: list[object] = []

    singular_value = quality.get(singular_key)

    if singular_value is not None:
        values.append(singular_value)

    if plural_key is not None:
        plural_value = quality.get(plural_key)

        if plural_value is not None:
            if not isinstance(
                plural_value,
                (list, tuple),
            ):
                return (), True

            values.extend(plural_value)

    if not values:
        return (), True

    hashes: set[str] = set()
    malformed = False

    for value in values:
        digest = _canonical_sha256_or_none(value)

        if digest is None:
            malformed = True
            continue

        hashes.add(digest)

    if not hashes:
        malformed = True

    return tuple(sorted(hashes)), malformed


def _retained(
    values: Iterable[dict[str, object]],
    *,
    maximum: int = _MAX_RETAINED_ITEMS,
) -> tuple[list[dict[str, object]], int]:
    items = list(values)

    if maximum <= 0:
        raise MaintenanceDiagnosticsError("maximum must be positive")

    return items[:maximum], max(0, len(items) - maximum)


def _percentile(
    values: Sequence[float],
    fraction: float,
) -> float | None:
    if not values:
        return None

    if not 0.0 <= fraction <= 1.0:
        raise MaintenanceDiagnosticsError("Percentile fraction must lie inside [0, 1]")

    ordered = sorted(float(value) for value in values)

    if len(ordered) == 1:
        return ordered[0]

    position = fraction * (len(ordered) - 1)
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))

    if lower_index == upper_index:
        return ordered[lower_index]

    weight = position - lower_index

    return ordered[lower_index] * (1.0 - weight) + ordered[upper_index] * weight


def summarize_duration_seconds(
    values: Iterable[float],
) -> dict[str, object]:
    """Return one deterministic duration summary."""

    durations: list[float] = []

    for raw_value in values:
        value = float(raw_value)

        if not math.isfinite(value) or value < 0.0:
            raise MaintenanceDiagnosticsError(
                "Duration values must be finite and non-negative"
            )

        durations.append(value)

    if not durations:
        return {
            "count": 0,
            "minimum_seconds": None,
            "maximum_seconds": None,
            "average_seconds": None,
            "p50_seconds": None,
            "p95_seconds": None,
        }

    return {
        "count": len(durations),
        "minimum_seconds": round(min(durations), 6),
        "maximum_seconds": round(max(durations), 6),
        "average_seconds": round(
            sum(durations) / len(durations),
            6,
        ),
        "p50_seconds": round(
            _percentile(durations, 0.50) or 0.0,
            6,
        ),
        "p95_seconds": round(
            _percentile(durations, 0.95) or 0.0,
            6,
        ),
    }


def _checkpoint_files(
    root: Path,
) -> tuple[list[Path], list[dict[str, object]]]:
    """Return regular checkpoint files without following symbolic links."""

    checkpoint_files: list[Path] = []
    filesystem_issues: list[dict[str, object]] = []

    if not root.exists():
        return checkpoint_files, filesystem_issues

    if not root.is_dir():
        filesystem_issues.append(
            {
                "path": str(root),
                "error": "NotADirectoryError",
            }
        )
        return checkpoint_files, filesystem_issues

    for current, directory_names, file_names in os.walk(
        root,
        topdown=True,
        followlinks=False,
    ):
        current_path = Path(current)

        retained_directories: list[str] = []

        for directory_name in directory_names:
            candidate = current_path / directory_name

            if candidate.is_symlink():
                filesystem_issues.append(
                    {
                        "path": str(candidate),
                        "error": "SymbolicLinkSkipped",
                    }
                )
                continue

            retained_directories.append(directory_name)

        directory_names[:] = retained_directories

        for file_name in file_names:
            candidate = current_path / file_name

            try:
                if candidate.is_symlink():
                    filesystem_issues.append(
                        {
                            "path": str(candidate),
                            "error": "SymbolicLinkSkipped",
                        }
                    )
                    continue

                if not candidate.is_file():
                    continue

            except OSError as exc:
                filesystem_issues.append(
                    {
                        "path": str(candidate),
                        "error": type(exc).__name__,
                    }
                )
                continue

            if candidate.name.endswith(".l2checkpoint"):
                checkpoint_files.append(candidate.resolve())

    checkpoint_files.sort(key=lambda path: str(path).lower())

    return checkpoint_files, filesystem_issues


def checkpoint_storage_diagnostics(
    session: Session,
    *,
    cache_root: Path,
    large_checkpoint_bytes: int = _DEFAULT_LARGE_CHECKPOINT_BYTES,
) -> dict[str, object]:
    """Inspect checkpoint artifacts and durable source references."""

    if not isinstance(session, Session):
        raise TypeError("checkpoint_storage_diagnostics requires a SQLAlchemy Session")

    if (
        isinstance(large_checkpoint_bytes, bool)
        or not isinstance(large_checkpoint_bytes, int)
        or large_checkpoint_bytes <= 0
    ):
        raise MaintenanceDiagnosticsError(
            "large_checkpoint_bytes must be a positive integer"
        )

    store = CheckpointStore(cache_root)
    files, filesystem_issues = _checkpoint_files(store.root)

    source_rows = session.execute(
        select(
            SourceHour.provider,
            SourceHour.venue,
            SourceHour.instrument,
            SourceHour.hour_utc,
            SourceHour.status,
            SourceHour.quality_json,
        ).where(
            SourceHour.data_kind == "orderbook",
        )
    ).all()

    source_identities: set[tuple[str, str, str, datetime]] = set()
    expected_by_identity: dict[
        tuple[str, str, str, datetime],
        str,
    ] = {}
    malformed_source_references: list[dict[str, object]] = []

    for (
        provider,
        venue,
        instrument,
        hour_utc,
        status,
        raw_quality,
    ) in source_rows:
        hour = require_aware_utc(
            "source hour_utc",
            hour_utc,
        )
        identity_key = (
            str(provider).strip().lower(),
            str(venue).strip().lower(),
            str(instrument).strip().upper(),
            hour,
        )
        source_identities.add(identity_key)

        quality = raw_quality if isinstance(raw_quality, Mapping) else {}
        raw_digest = quality.get("output_checkpoint_content_sha256")

        if raw_digest is None:
            continue

        digest = _canonical_sha256_or_none(raw_digest)

        if digest is None:
            malformed_source_references.append(
                {
                    "provider": identity_key[0],
                    "venue": identity_key[1],
                    "instrument": identity_key[2],
                    "hour_utc": hour.isoformat(),
                    "status": str(status),
                    "problem": (
                        "output_checkpoint_content_sha256 is not a "
                        "canonical lowercase SHA-256"
                    ),
                }
            )
            continue

        expected_by_identity[identity_key] = digest

    referenced_checkpoint_hashes = collect_referenced_checkpoint_sha256s(session)

    valid_artifacts: list[dict[str, object]] = []
    invalid_artifacts: list[dict[str, object]] = []
    orphan_artifacts: list[dict[str, object]] = []
    large_artifacts: list[dict[str, object]] = []
    observed_references: set[
        tuple[
            tuple[str, str, str, datetime],
            str,
        ]
    ] = set()
    total_bytes = 0

    for path in files:
        try:
            size_bytes = int(path.stat().st_size)
            total_bytes += size_bytes

            checkpoint = load_checkpoint_file(path)
            encoded = encode_checkpoint(checkpoint)
            encoding_info = checkpoint_encoding_info(encoded)
            identity = CheckpointIdentity.from_checkpoint(checkpoint)

            expected_path = store.path_for(
                identity,
                encoding_info.content_sha256,
            )

            if path != expected_path.resolve():
                raise MaintenanceDiagnosticsError(
                    "Checkpoint is not stored at its canonical path"
                )

            identity_key = (
                identity.provider,
                identity.venue,
                identity.instrument,
                identity.through_hour_utc,
            )
            reference_key = (
                identity_key,
                encoding_info.content_sha256,
            )
            observed_references.add(reference_key)

            item = {
                "provider": identity.provider,
                "venue": identity.venue,
                "instrument": identity.instrument,
                "through_hour_utc": (identity.through_hour_utc.isoformat()),
                "content_sha256": (encoding_info.content_sha256),
                "size_bytes": size_bytes,
                "path": str(path),
            }
            valid_artifacts.append(item)

            if encoding_info.content_sha256 not in referenced_checkpoint_hashes:
                orphan_artifacts.append(
                    {
                        **item,
                        "reason": "no_durable_checkpoint_reference",
                    }
                )

            if size_bytes > large_checkpoint_bytes:
                large_artifacts.append(item)

        except Exception as exc:
            invalid_artifacts.append(
                {
                    "path": str(path),
                    "error": _safe_exception_name(exc),
                }
            )

    missing_expected: list[dict[str, object]] = []

    for identity_key, digest in sorted(
        expected_by_identity.items(),
        key=lambda item: item[0],
    ):
        if (identity_key, digest) in observed_references:
            continue

        provider, venue, instrument, hour_utc = identity_key

        missing_expected.append(
            {
                "provider": provider,
                "venue": venue,
                "instrument": instrument,
                "through_hour_utc": hour_utc.isoformat(),
                "content_sha256": digest,
            }
        )

    retained_invalid, omitted_invalid = _retained(invalid_artifacts)
    retained_orphans, omitted_orphans = _retained(orphan_artifacts)
    retained_missing, omitted_missing = _retained(missing_expected)
    retained_large, omitted_large = _retained(large_artifacts)
    retained_malformed, omitted_malformed = _retained(malformed_source_references)
    retained_fs, omitted_fs = _retained(filesystem_issues)

    return {
        "schema": "l2shock.checkpoint_storage_diagnostics",
        "schema_version": 1,
        "checkpoint_root": str(store.root),
        "artifact_count": len(files),
        "valid_artifact_count": len(valid_artifacts),
        "invalid_artifact_count": len(invalid_artifacts),
        "total_bytes": total_bytes,
        "expected_reference_count": len(expected_by_identity),
        "orphan_artifact_count": len(orphan_artifacts),
        "missing_expected_count": len(missing_expected),
        "malformed_source_reference_count": len(malformed_source_references),
        "large_artifact_threshold_bytes": large_checkpoint_bytes,
        "large_artifact_count": len(large_artifacts),
        "invalid_artifacts": retained_invalid,
        "invalid_artifacts_omitted": omitted_invalid,
        "orphan_artifacts": retained_orphans,
        "orphan_artifacts_omitted": omitted_orphans,
        "missing_expected": retained_missing,
        "missing_expected_omitted": omitted_missing,
        "malformed_source_references": retained_malformed,
        "malformed_source_references_omitted": omitted_malformed,
        "large_artifacts": retained_large,
        "large_artifacts_omitted": omitted_large,
        "filesystem_issues": retained_fs,
        "filesystem_issues_omitted": omitted_fs,
    }


def stale_source_diagnostics(
    session: Session,
    *,
    generated_at_utc: datetime,
    stale_downloading_after: timedelta = (_DEFAULT_STALE_DOWNLOADING_AFTER),
    stale_processing_after: timedelta = (_DEFAULT_STALE_PROCESSING_AFTER),
) -> dict[str, object]:
    """Report source rows which appear stuck in transient states."""

    if not isinstance(session, Session):
        raise TypeError("stale_source_diagnostics requires a SQLAlchemy Session")

    generated = require_aware_utc(
        "generated_at_utc",
        generated_at_utc,
    )

    for name, value in (
        ("stale_downloading_after", stale_downloading_after),
        ("stale_processing_after", stale_processing_after),
    ):
        if not isinstance(value, timedelta) or value <= timedelta(0):
            raise MaintenanceDiagnosticsError(f"{name} must be a positive timedelta")

    rows = session.execute(
        select(
            SourceHour.provider,
            SourceHour.venue,
            SourceHour.data_kind,
            SourceHour.instrument,
            SourceHour.hour_utc,
            SourceHour.status,
            SourceHour.discovered_at,
            SourceHour.downloaded_at,
            SourceHour.processed_at,
        ).where(
            SourceHour.status.in_(
                (
                    "downloading",
                    "processing",
                )
            )
        )
    ).all()

    stale_downloading: list[dict[str, object]] = []
    stale_processing: list[dict[str, object]] = []

    for row in rows:
        (
            provider,
            venue,
            data_kind,
            instrument,
            hour_utc,
            status,
            discovered_at,
            downloaded_at,
            processed_at,
        ) = row

        if str(status) == "downloading":
            anchor = require_aware_utc(
                "discovered_at",
                discovered_at,
            )
            threshold = stale_downloading_after
            target = stale_downloading
            anchor_name = "discovered_at"
        else:
            anchor_source = downloaded_at or discovered_at
            anchor = require_aware_utc(
                "processing age anchor",
                anchor_source,
            )
            threshold = stale_processing_after
            target = stale_processing
            anchor_name = (
                "downloaded_at" if downloaded_at is not None else "discovered_at"
            )

        age_seconds = max(
            0.0,
            (generated - anchor).total_seconds(),
        )

        if age_seconds <= threshold.total_seconds():
            continue

        target.append(
            {
                "provider": str(provider),
                "venue": str(venue),
                "data_kind": str(data_kind),
                "instrument": str(instrument),
                "hour_utc": _utc_text(hour_utc),
                "status": str(status),
                "age_anchor": anchor_name,
                "age_anchor_utc": anchor.isoformat(),
                "age_seconds": round(age_seconds, 3),
                "processed_at": _utc_text(processed_at),
            }
        )

    retained_downloading, omitted_downloading = _retained(stale_downloading)
    retained_processing, omitted_processing = _retained(stale_processing)

    return {
        "schema": "l2shock.stale_source_diagnostics",
        "schema_version": 1,
        "generated_at_utc": generated.isoformat(),
        "stale_downloading_after_seconds": int(stale_downloading_after.total_seconds()),
        "stale_processing_after_seconds": int(stale_processing_after.total_seconds()),
        "stale_downloading_count": len(stale_downloading),
        "stale_processing_count": len(stale_processing),
        "stale_downloading": retained_downloading,
        "stale_downloading_omitted": omitted_downloading,
        "stale_processing": retained_processing,
        "stale_processing_omitted": omitted_processing,
        "processing_age_note": (
            "source_hours has no processing_started_at column; "
            "processing age uses downloaded_at when available, otherwise "
            "discovered_at. It is a conservative queue-inclusive age."
        ),
    }


def stale_fetch_run_diagnostics(
    session: Session,
    *,
    generated_at_utc: datetime,
    stale_after: timedelta = _DEFAULT_STALE_FETCH_RUN_AFTER,
) -> dict[str, object]:
    """Report durable running fetch rows old enough to require investigation.

    This function is read-only. Age alone cannot prove that a fetch owner is
    dead because another application process may still own the operation.
    """

    if not isinstance(session, Session):
        raise TypeError("stale_fetch_run_diagnostics requires a SQLAlchemy Session")

    generated = require_aware_utc(
        "generated_at_utc",
        generated_at_utc,
    )

    if not isinstance(stale_after, timedelta) or stale_after <= timedelta(0):
        raise MaintenanceDiagnosticsError("stale_after must be a positive timedelta")

    rows = session.execute(
        select(
            FetchRun.operation_id,
            FetchRun.kind,
            FetchRun.status,
            FetchRun.requested_start_utc,
            FetchRun.requested_end_utc,
            FetchRun.started_at,
            FetchRun.files_requested,
            FetchRun.files_downloaded,
            FetchRun.files_processed,
            FetchRun.files_failed,
        )
        .where(FetchRun.status == "running")
        .order_by(
            FetchRun.started_at,
            FetchRun.operation_id,
        )
    ).all()

    stale: list[dict[str, object]] = []
    threshold_seconds = stale_after.total_seconds()

    for (
        operation_id,
        kind,
        status,
        requested_start_utc,
        requested_end_utc,
        started_at,
        files_requested,
        files_downloaded,
        files_processed,
        files_failed,
    ) in rows:
        started = require_aware_utc(
            "fetch started_at",
            started_at,
        )
        age_seconds = max(
            0.0,
            (generated - started).total_seconds(),
        )

        if age_seconds <= threshold_seconds:
            continue

        stale.append(
            {
                "operation_id": str(operation_id),
                "kind": str(kind),
                "status": str(status),
                "requested_start_utc": _utc_text(requested_start_utc),
                "requested_end_utc": _utc_text(requested_end_utc),
                "started_at_utc": started.isoformat(),
                "age_seconds": round(age_seconds, 3),
                "files_requested": int(files_requested),
                "files_downloaded": int(files_downloaded),
                "files_processed": int(files_processed),
                "files_failed": int(files_failed),
            }
        )

    retained, omitted = _retained(stale)

    return {
        "schema": "l2shock.stale_fetch_run_diagnostics",
        "schema_version": 1,
        "generated_at_utc": generated.isoformat(),
        "stale_after_seconds": int(threshold_seconds),
        "running_fetch_run_count": len(rows),
        "stale_running_fetch_run_count": len(stale),
        "stale_running_fetch_runs": retained,
        "stale_running_fetch_runs_omitted": omitted,
        "recovery_performed": False,
        "ownership_note": (
            "Age does not prove owner death. Automatic recovery remains "
            "disabled until cross-process fetch ownership can be proven."
        ),
    }


def analytical_consistency_diagnostics(
    session: Session,
    *,
    large_l2_row_bytes: int = _DEFAULT_LARGE_L2_ROW_BYTES,
    large_price_row_bytes: int = _DEFAULT_LARGE_PRICE_ROW_BYTES,
) -> dict[str, object]:
    """Compare processed source metadata with compact analytical rows."""

    if not isinstance(session, Session):
        raise TypeError(
            "analytical_consistency_diagnostics requires a " "SQLAlchemy Session"
        )

    for name, value in (
        ("large_l2_row_bytes", large_l2_row_bytes),
        ("large_price_row_bytes", large_price_row_bytes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise MaintenanceDiagnosticsError(f"{name} must be a positive integer")

    source_rows = session.execute(
        select(
            SourceHour.provider,
            SourceHour.venue,
            SourceHour.data_kind,
            SourceHour.instrument,
            SourceHour.hour_utc,
            SourceHour.status,
            SourceHour.quality_json,
        ).where(
            SourceHour.status == "processed",
        )
    ).all()

    l2_size_expression = (
        func.octet_length(L2HourlySeries.bid_liquidity_block)
        + func.octet_length(L2HourlySeries.ask_liquidity_block)
        + func.octet_length(L2HourlySeries.validity_block)
        + func.octet_length(L2HourlySeries.source_count_block)
    )

    price_size_expression = (
        func.octet_length(PriceHourlySeries.ohlc_block)
        + func.octet_length(PriceHourlySeries.validity_block)
        + func.octet_length(PriceHourlySeries.trade_count_block)
    )

    l2_rows = session.execute(
        select(
            L2HourlySeries.base,
            L2HourlySeries.hour_utc,
            L2HourlySeries.preset_hash,
            L2HourlySeries.content_sha256,
            l2_size_expression.label("encoded_size_bytes"),
        )
    ).all()

    price_rows = session.execute(
        select(
            PriceHourlySeries.base,
            PriceHourlySeries.hour_utc,
            PriceHourlySeries.content_sha256,
            price_size_expression.label("encoded_size_bytes"),
        )
    ).all()

    l2_by_content = {
        (
            str(base),
            require_aware_utc("L2 hour", hour_utc),
            str(content_sha256),
        )
        for (
            base,
            hour_utc,
            _preset_hash,
            content_sha256,
            _encoded_size,
        ) in l2_rows
    }
    price_by_content = {
        (
            str(base),
            require_aware_utc("price hour", hour_utc),
            str(content_sha256),
        )
        for (
            base,
            hour_utc,
            content_sha256,
            _encoded_size,
        ) in price_rows
    }

    expected_l2: set[tuple[str, datetime, str]] = set()
    expected_price: set[tuple[str, datetime, str]] = set()

    missing_processed_outputs: list[dict[str, object]] = []
    malformed_processed_metadata: list[dict[str, object]] = []

    for (
        provider,
        venue,
        data_kind,
        instrument,
        hour_utc,
        status,
        raw_quality,
    ) in source_rows:
        base = _VALID_BASES.get(str(instrument).upper())

        if base is None:
            continue

        hour = require_aware_utc(
            "processed source hour",
            hour_utc,
        )
        quality = raw_quality if isinstance(raw_quality, Mapping) else {}

        data_kind_text = str(data_kind)

        if data_kind_text == "orderbook":
            hashes, malformed = _quality_content_hashes(
                quality,
                singular_key=("analytical_content_sha256"),
                plural_key=("analytical_content_sha256s"),
            )
            key_description = (
                "analytical_content_sha256 / " "analytical_content_sha256s"
            )
        elif data_kind_text == "trades":
            hashes, malformed = _quality_content_hashes(
                quality,
                singular_key="price_content_sha256",
            )
            key_description = "price_content_sha256"
        else:
            continue

        source_payload = {
            "provider": str(provider),
            "venue": str(venue),
            "data_kind": data_kind_text,
            "instrument": str(instrument),
            "base": base,
            "hour_utc": hour.isoformat(),
            "status": str(status),
        }

        if malformed:
            malformed_processed_metadata.append(
                {
                    **source_payload,
                    "problem": (
                        f"{key_description} is absent or "
                        "contains noncanonical content"
                    ),
                }
            )

        for digest in hashes:
            identity = (
                base,
                hour,
                digest,
            )

            if data_kind_text == "orderbook":
                expected_l2.add(identity)
                present = identity in l2_by_content
            else:
                expected_price.add(identity)
                present = identity in price_by_content

            if not present:
                missing_processed_outputs.append(
                    {
                        **source_payload,
                        "expected_content_sha256": (digest),
                    }
                )

    orphan_l2: list[dict[str, object]] = []
    orphan_price: list[dict[str, object]] = []
    large_l2: list[dict[str, object]] = []
    large_price: list[dict[str, object]] = []

    l2_base_hours: set[tuple[str, datetime]] = set()
    price_base_hours: set[tuple[str, datetime]] = set()

    for (
        base,
        hour_utc,
        preset_hash,
        content_sha256,
        encoded_size,
    ) in l2_rows:
        hour = require_aware_utc("L2 hour", hour_utc)
        digest = str(content_sha256)
        size = int(encoded_size or 0)
        identity = (
            str(base),
            hour,
            digest,
        )
        l2_base_hours.add((str(base), hour))

        payload = {
            "base": str(base),
            "hour_utc": hour.isoformat(),
            "preset_hash": str(preset_hash),
            "content_sha256": digest,
            "encoded_size_bytes": size,
        }

        if identity not in expected_l2:
            orphan_l2.append(payload)

        if size > large_l2_row_bytes:
            large_l2.append(payload)

    for (
        base,
        hour_utc,
        content_sha256,
        encoded_size,
    ) in price_rows:
        hour = require_aware_utc("price hour", hour_utc)
        digest = str(content_sha256)
        size = int(encoded_size or 0)
        identity = (
            str(base),
            hour,
            digest,
        )
        price_base_hours.add((str(base), hour))

        payload = {
            "base": str(base),
            "hour_utc": hour.isoformat(),
            "content_sha256": digest,
            "encoded_size_bytes": size,
        }

        if identity not in expected_price:
            orphan_price.append(payload)

        if size > large_price_row_bytes:
            large_price.append(payload)

    l2_without_price = [
        {
            "base": base,
            "hour_utc": hour.isoformat(),
        }
        for base, hour in sorted(l2_base_hours - price_base_hours)
    ]
    price_without_l2 = [
        {
            "base": base,
            "hour_utc": hour.isoformat(),
        }
        for base, hour in sorted(price_base_hours - l2_base_hours)
    ]

    sections: dict[str, tuple[list[dict[str, object]], int]] = {}

    for name, values in (
        (
            "missing_processed_outputs",
            missing_processed_outputs,
        ),
        (
            "malformed_processed_metadata",
            malformed_processed_metadata,
        ),
        ("orphan_l2_rows", orphan_l2),
        ("orphan_price_rows", orphan_price),
        ("l2_without_price", l2_without_price),
        ("price_without_l2", price_without_l2),
        ("large_l2_rows", large_l2),
        ("large_price_rows", large_price),
    ):
        sections[name] = _retained(values)

    return {
        "schema": "l2shock.analytical_consistency_diagnostics",
        "schema_version": 1,
        "processed_source_count": len(source_rows),
        "l2_row_count": len(l2_rows),
        "price_row_count": len(price_rows),
        "missing_processed_output_count": len(missing_processed_outputs),
        "malformed_processed_metadata_count": len(malformed_processed_metadata),
        "orphan_l2_row_count": len(orphan_l2),
        "orphan_price_row_count": len(orphan_price),
        "l2_without_price_count": len(l2_without_price),
        "price_without_l2_count": len(price_without_l2),
        "large_l2_row_threshold_bytes": large_l2_row_bytes,
        "large_price_row_threshold_bytes": large_price_row_bytes,
        "large_l2_row_count": len(large_l2),
        "large_price_row_count": len(large_price),
        **{name: retained for name, (retained, _omitted) in sections.items()},
        **{
            f"{name}_omitted": omitted
            for name, (_retained_values, omitted) in sections.items()
        },
    }


def performance_diagnostics(
    session: Session,
    *,
    maximum_fetch_runs: int = 1_000,
) -> dict[str, object]:
    """Summarize currently persisted operation timing information."""

    if not isinstance(session, Session):
        raise TypeError("performance_diagnostics requires a SQLAlchemy Session")

    if (
        isinstance(maximum_fetch_runs, bool)
        or not isinstance(maximum_fetch_runs, int)
        or maximum_fetch_runs <= 0
    ):
        raise MaintenanceDiagnosticsError(
            "maximum_fetch_runs must be a positive integer"
        )

    fetch_rows = session.execute(
        select(
            FetchRun.kind,
            FetchRun.status,
            FetchRun.started_at,
            FetchRun.ended_at,
            FetchRun.files_requested,
            FetchRun.files_downloaded,
            FetchRun.files_processed,
            FetchRun.files_failed,
        )
        .where(
            FetchRun.ended_at.is_not(None),
        )
        .order_by(FetchRun.started_at.desc())
        .limit(maximum_fetch_runs)
    ).all()

    durations_by_group: dict[
        tuple[str, str],
        list[float],
    ] = defaultdict(list)
    status_counts: dict[str, int] = defaultdict(int)

    for (
        kind,
        status,
        started_at,
        ended_at,
        _files_requested,
        _files_downloaded,
        _files_processed,
        _files_failed,
    ) in fetch_rows:
        if ended_at is None:
            continue

        started = require_aware_utc(
            "fetch started_at",
            started_at,
        )
        ended = require_aware_utc(
            "fetch ended_at",
            ended_at,
        )

        duration = max(
            0.0,
            (ended - started).total_seconds(),
        )
        group = (
            str(kind),
            str(status),
        )
        durations_by_group[group].append(duration)
        status_counts[str(status)] += 1

    fetch_groups = [
        {
            "kind": kind,
            "status": status,
            **summarize_duration_seconds(values),
        }
        for (kind, status), values in sorted(durations_by_group.items())
    ]

    processed_source_rows = session.execute(
        select(
            SourceHour.data_kind,
            SourceHour.downloaded_at,
            SourceHour.processed_at,
        ).where(
            SourceHour.status == "processed",
            SourceHour.downloaded_at.is_not(None),
            SourceHour.processed_at.is_not(None),
        )
    ).all()

    latency_by_kind: dict[str, list[float]] = defaultdict(list)

    for data_kind, downloaded_at, processed_at in processed_source_rows:
        downloaded = require_aware_utc(
            "source downloaded_at",
            downloaded_at,
        )
        processed = require_aware_utc(
            "source processed_at",
            processed_at,
        )

        latency_by_kind[str(data_kind)].append(
            max(
                0.0,
                (processed - downloaded).total_seconds(),
            )
        )

    return {
        "schema": "l2shock.performance_diagnostics",
        "schema_version": 1,
        "fetch_run_limit": maximum_fetch_runs,
        "completed_fetch_run_count": len(fetch_rows),
        "fetch_status_counts": dict(sorted(status_counts.items())),
        "fetch_duration_groups": fetch_groups,
        "source_download_to_processed_latency": [
            {
                "data_kind": data_kind,
                **summarize_duration_seconds(values),
            }
            for data_kind, values in sorted(latency_by_kind.items())
        ],
        "processing_duration_history": {
            "available": False,
            "reason": (
                "Dedicated processing start/end history is not persisted. "
                "downloaded_at-to-processed_at is queue-inclusive latency, "
                "not exact processing duration."
            ),
        },
        "peak_memory_history": {
            "available": False,
            "reason": (
                "Per-operation peak memory is not currently persisted. "
                "Diagnostics must not fabricate historical memory values."
            ),
        },
    }


def maintenance_diagnostics_report(
    session: Session,
    *,
    cache_root: Path,
    generated_at_utc: datetime,
) -> dict[str, object]:
    """Build the complete read-only maintenance diagnostics section."""

    generated = require_aware_utc(
        "generated_at_utc",
        generated_at_utc,
    )

    checkpoint = checkpoint_storage_diagnostics(
        session,
        cache_root=cache_root,
    )
    stale = stale_source_diagnostics(
        session,
        generated_at_utc=generated,
    )
    stale_fetch_runs = stale_fetch_run_diagnostics(
        session,
        generated_at_utc=generated,
    )
    consistency = analytical_consistency_diagnostics(
        session,
    )
    performance = performance_diagnostics(session)

    attention_count = sum(
        (
            int(checkpoint["invalid_artifact_count"]),
            int(checkpoint["orphan_artifact_count"]),
            int(checkpoint["missing_expected_count"]),
            int(checkpoint["malformed_source_reference_count"]),
            int(stale["stale_downloading_count"]),
            int(stale["stale_processing_count"]),
            int(stale_fetch_runs["stale_running_fetch_run_count"]),
            int(consistency["missing_processed_output_count"]),
            int(consistency["malformed_processed_metadata_count"]),
            int(consistency["orphan_l2_row_count"]),
            int(consistency["orphan_price_row_count"]),
            int(consistency["l2_without_price_count"]),
            int(consistency["price_without_l2_count"]),
            int(checkpoint["large_artifact_count"]),
            int(consistency["large_l2_row_count"]),
            int(consistency["large_price_row_count"]),
        )
    )

    return {
        "schema": "l2shock.maintenance_diagnostics",
        "schema_version": 1,
        "generated_at_utc": generated.isoformat(),
        "ok": attention_count == 0,
        "attention_count": attention_count,
        "checkpoint_storage": checkpoint,
        "stale_sources": stale,
        "stale_fetch_runs": stale_fetch_runs,
        "analytical_consistency": consistency,
        "performance": performance,
        "maintenance_actions": {
            "checkpoint_deletion_performed": False,
            "source_status_recovery_performed": False,
            "analytical_row_deletion_performed": False,
            "analytical_row_repair_performed": False,
            "raw_file_deletion_performed": False,
        },
    }


__all__ = [
    "MaintenanceDiagnosticsError",
    "analytical_consistency_diagnostics",
    "checkpoint_storage_diagnostics",
    "maintenance_diagnostics_report",
    "performance_diagnostics",
    "stale_fetch_run_diagnostics",
    "stale_source_diagnostics",
    "summarize_duration_seconds",
]

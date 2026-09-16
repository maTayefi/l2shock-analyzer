# l2shock/acquisition/pruning_recovery.py
"""Restart reconciliation for interrupted processed-raw pruning.

Raw pruning renames a canonical source archive to a same-directory hidden
``.pruning`` name before clearing ``source_hours.local_path``.

A process failure can therefore leave either:

1. a rolled-back database row which still names the canonical path while only
   the hidden pruning file exists; or
2. a committed row with ``local_path = NULL`` while the hidden pruning file
   still exists.

This module reconciles only application-generated pruning names whose source
identity, database row, size, and SHA-256 all verify under the normal
source-hour advisory lock.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from sqlalchemy import select

from l2shock.acquisition.locks import (
    acquire_source_hour_transaction_lock,
)
from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.persistence import SourceHourStatus
from l2shock.acquisition.validation import sha256_file
from l2shock.db.engine import session_scope
from l2shock.db.models import SourceHour

_PRUNING_FILENAME_RE: Final[re.Pattern[str]] = re.compile(
    r"^\.(?P<source_filename>.+\.parquet)" r"\.(?P<nonce>[0-9a-f]{32})\.pruning$"
)


class RawPruningRecoveryError(RuntimeError):
    """A stranded pruning artifact could not be reconciled safely."""


@dataclass(frozen=True, slots=True)
class RawPruningRecoveryReport:
    """Bounded summary of one startup reconciliation pass."""

    examined_count: int
    restored_count: int
    deleted_count: int
    failed_count: int
    failures: tuple[str, ...]

    def __post_init__(self) -> None:
        for field_name in (
            "examined_count",
            "restored_count",
            "deleted_count",
            "failed_count",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RawPruningRecoveryError(
                    f"{field_name} must be a non-negative integer"
                )

        if (
            self.restored_count + self.deleted_count + self.failed_count
            != self.examined_count
        ):
            raise RawPruningRecoveryError(
                "Raw-pruning recovery accounting is inconsistent"
            )

        object.__setattr__(
            self,
            "failures",
            tuple(str(value) for value in self.failures),
        )


@dataclass(frozen=True, slots=True)
class _PruningCandidate:
    path: Path
    canonical_path: Path
    spec: SourceFileSpec


def _pruning_candidate(
    raw_root: Path,
    path: Path,
) -> _PruningCandidate:
    root = Path(raw_root).expanduser().resolve()
    candidate = Path(path).expanduser()

    if candidate.is_symlink():
        raise RawPruningRecoveryError("Pruning candidate cannot be a symbolic link")

    resolved = candidate.resolve()

    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise RawPruningRecoveryError(
            "Pruning candidate lies outside the configured raw root"
        ) from exc

    if len(relative.parts) != 5:
        raise RawPruningRecoveryError(
            "Pruning candidate does not use the canonical source layout"
        )

    provider, venue, date_text, hour_text, hidden_filename = relative.parts
    match = _PRUNING_FILENAME_RE.fullmatch(hidden_filename)

    if match is None:
        raise RawPruningRecoveryError("Pruning candidate filename is not canonical")

    source_filename = match.group("source_filename")

    if source_filename.endswith("_orderbook.parquet"):
        data_kind = SourceDataKind.ORDERBOOK
        symbol = source_filename[: -len("_orderbook.parquet")]
    elif source_filename.endswith("_trades.parquet"):
        data_kind = SourceDataKind.TRADES
        symbol = source_filename[: -len("_trades.parquet")]
    else:
        raise RawPruningRecoveryError(
            "Pruning candidate does not identify a supported data kind"
        )

    if not symbol:
        raise RawPruningRecoveryError("Pruning candidate has a blank source symbol")

    try:
        hour_utc = datetime.strptime(
            f"{date_text}T{hour_text}",
            "%Y-%m-%dT%H",
        ).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise RawPruningRecoveryError(
            "Pruning candidate has an invalid UTC-hour directory"
        ) from exc

    try:
        spec = SourceFileSpec(
            provider=provider,
            venue=venue,
            symbol=symbol,
            data_kind=data_kind,
            hour_utc=hour_utc,
        )
    except (TypeError, ValueError) as exc:
        raise RawPruningRecoveryError(
            "Pruning candidate does not identify a supported source"
        ) from exc

    canonical = spec.local_path(root)

    if canonical.parent != resolved.parent:
        raise RawPruningRecoveryError(
            "Pruning candidate directory does not match source identity"
        )

    if canonical.name != source_filename:
        raise RawPruningRecoveryError(
            "Pruning candidate filename does not match source identity"
        )

    return _PruningCandidate(
        path=resolved,
        canonical_path=canonical,
        spec=spec,
    )


def _pruning_files(
    raw_root: Path,
) -> tuple[Path, ...]:
    root = Path(raw_root).expanduser().resolve()

    if not root.exists():
        return ()

    if not root.is_dir():
        raise RawPruningRecoveryError("Configured raw root is not a directory")

    values: list[Path] = []

    for current, directory_names, file_names in os.walk(
        root,
        topdown=True,
        followlinks=False,
    ):
        current_path = Path(current)

        directory_names[:] = [
            name for name in directory_names if not (current_path / name).is_symlink()
        ]

        for file_name in file_names:
            if _PRUNING_FILENAME_RE.fullmatch(file_name) is None:
                continue

            candidate = current_path / file_name

            if candidate.is_symlink():
                continue

            values.append(candidate.resolve())

    return tuple(
        sorted(
            values,
            key=lambda value: str(value).lower(),
        )
    )


def _source_row(
    session,
    spec: SourceFileSpec,
) -> SourceHour | None:
    return session.scalar(
        select(SourceHour).where(
            SourceHour.provider == spec.provider,
            SourceHour.venue == spec.venue,
            SourceHour.data_kind == spec.data_kind.value,
            SourceHour.instrument == spec.symbol,
            SourceHour.hour_utc == spec.hour_utc,
        )
    )


def _verify_file_matches_row(
    path: Path,
    row: SourceHour,
) -> None:
    if path.is_symlink() or not path.is_file():
        raise RawPruningRecoveryError("Recovery candidate is not a regular file")

    expected_size = row.file_size_bytes
    expected_digest = str(row.content_sha256 or "").strip()

    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size <= 0
    ):
        raise RawPruningRecoveryError("Source row lacks a positive durable file size")

    if (
        expected_digest != expected_digest.lower()
        or len(expected_digest) != 64
        or any(character not in "0123456789abcdef" for character in expected_digest)
    ):
        raise RawPruningRecoveryError("Source row lacks a canonical durable SHA-256")

    if path.stat().st_size != expected_size:
        raise RawPruningRecoveryError(
            "Recovery candidate size differs from durable metadata"
        )

    actual_digest, actual_size = sha256_file(path)

    if actual_size != expected_size:
        raise RawPruningRecoveryError("Recovery candidate changed while being hashed")

    if actual_digest != expected_digest:
        raise RawPruningRecoveryError(
            "Recovery candidate SHA-256 differs from durable metadata"
        )


def _reconcile_candidate(
    candidate: _PruningCandidate,
) -> str:
    with session_scope() as session:
        acquire_source_hour_transaction_lock(
            session,
            candidate.spec,
        )

        row = _source_row(
            session,
            candidate.spec,
        )

        if row is None:
            raise RawPruningRecoveryError("No source row owns the pruning candidate")

        if row.status != SourceHourStatus.PROCESSED.value:
            raise RawPruningRecoveryError(
                "Only a processed source may own a pruning candidate"
            )

        _verify_file_matches_row(
            candidate.path,
            row,
        )

        stored_path_text = str(row.local_path or "").strip()

        if not stored_path_text:
            if candidate.canonical_path.exists():
                _verify_file_matches_row(
                    candidate.canonical_path,
                    row,
                )

            candidate.path.unlink()
            return "deleted"

        stored_path = Path(stored_path_text).expanduser().resolve()

        if stored_path != candidate.canonical_path:
            raise RawPruningRecoveryError(
                "Source row local_path is neither null nor canonical"
            )

        if candidate.canonical_path.exists():
            _verify_file_matches_row(
                candidate.canonical_path,
                row,
            )
            candidate.path.unlink()
            return "deleted"

        candidate.path.replace(candidate.canonical_path)
        return "restored"


def reconcile_stranded_raw_pruning(
    raw_root: Path,
) -> RawPruningRecoveryReport:
    """Reconcile all recognized stranded pruning artifacts."""

    restored_count = 0
    deleted_count = 0
    failures: list[str] = []

    paths = _pruning_files(raw_root)

    for path in paths:
        try:
            candidate = _pruning_candidate(
                raw_root,
                path,
            )
            outcome = _reconcile_candidate(candidate)

            if outcome == "restored":
                restored_count += 1
            elif outcome == "deleted":
                deleted_count += 1
            else:
                raise RawPruningRecoveryError(
                    "Pruning reconciliation returned an unknown outcome"
                )

        except Exception as exc:
            failures.append(f"{path.name}: {type(exc).__name__}")

    return RawPruningRecoveryReport(
        examined_count=len(paths),
        restored_count=restored_count,
        deleted_count=deleted_count,
        failed_count=len(failures),
        failures=tuple(failures[:100]),
    )


__all__ = [
    "RawPruningRecoveryError",
    "RawPruningRecoveryReport",
    "reconcile_stranded_raw_pruning",
]

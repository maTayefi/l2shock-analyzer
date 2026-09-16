# l2shock/acquisition/persistence.py
"""Acquisition persistence policies and secret-safe diagnostic handling."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final
from collections.abc import Iterable

_MAX_DIAGNOSTIC_TEXT: Final[int] = 2000

_CONTROL_CHARACTERS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WHITESPACE_RE = re.compile(r"[ \t\r\f\v]+")

_BEARER_RE = re.compile(r"(?i)\b(authorization\s*:\s*bearer\s+)[^\s,;]+")
_API_KEY_QUERY_RE = re.compile(r"(?i)([?&]api_key=)[^&\s]+")
_API_KEY_ASSIGNMENT_RE = re.compile(r"(?i)\b(api[_-]?key\s*[:=]\s*)[^\s,;&]+")
_PASSWORD_URL_RE = re.compile(
    r"([A-Za-z][A-Za-z0-9+.-]*://[^:/@\s]+:)" r"([^@/\s]+)" r"(@)"
)


class SourceHourStatus(StrEnum):
    DISCOVERED = "discovered"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    PROCESSING = "processing"
    PROCESSED = "processed"
    MISSING = "missing"
    INVALID = "invalid"
    QUARANTINED = "quarantined"
    ERROR = "error"


class FetchRunKind(StrEnum):
    MANUAL = "manual"
    AUTOMATIC = "automatic"
    BACKFILL = "backfill"
    VALIDATION = "validation"


class FetchRunStatus(StrEnum):
    RUNNING = "running"
    OK = "ok"
    PARTIAL_OK = "partial_ok"
    ERROR = "error"
    STOPPED = "stopped"


class InvalidSourceHourTransitionError(ValueError):
    """A source-hour status transition violated the persistence policy."""


class SourceHourLockUnavailableError(RuntimeError):
    """Another transaction currently owns a source-hour advisory lock."""


_ALLOWED_SOURCE_TRANSITIONS: Final[
    dict[SourceHourStatus, frozenset[SourceHourStatus]]
] = {
    SourceHourStatus.DISCOVERED: frozenset(
        {
            SourceHourStatus.DISCOVERED,
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.MISSING,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.DOWNLOADING: frozenset(
        {
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.MISSING,
            SourceHourStatus.INVALID,
            SourceHourStatus.QUARANTINED,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.DOWNLOADED: frozenset(
        {
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.PROCESSING,
            SourceHourStatus.MISSING,
            SourceHourStatus.INVALID,
            SourceHourStatus.QUARANTINED,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.PROCESSING: frozenset(
        {
            SourceHourStatus.PROCESSING,
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.PROCESSED,
            SourceHourStatus.INVALID,
            SourceHourStatus.QUARANTINED,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.PROCESSED: frozenset(
        {
            SourceHourStatus.PROCESSED,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.MISSING: frozenset(
        {
            SourceHourStatus.MISSING,
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.INVALID: frozenset(
        {
            SourceHourStatus.INVALID,
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.QUARANTINED,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.QUARANTINED: frozenset(
        {
            SourceHourStatus.QUARANTINED,
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.ERROR,
        }
    ),
    SourceHourStatus.ERROR: frozenset(
        {
            SourceHourStatus.ERROR,
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.MISSING,
            SourceHourStatus.INVALID,
            SourceHourStatus.QUARANTINED,
        }
    ),
}


def normalize_source_hour_status(
    value: SourceHourStatus | str,
) -> SourceHourStatus:
    try:
        return SourceHourStatus(value)
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(item.value for item in SourceHourStatus)
        raise ValueError(f"Source-hour status must be one of: {allowed}") from exc


def validate_source_hour_transition(
    current: SourceHourStatus | str,
    target: SourceHourStatus | str,
    *,
    allow_processing_cancellation_reset: bool = False,
) -> tuple[SourceHourStatus, SourceHourStatus]:
    current_status = normalize_source_hour_status(current)
    target_status = normalize_source_hour_status(target)

    allowed = _ALLOWED_SOURCE_TRANSITIONS[current_status]

    if target_status not in allowed:
        raise InvalidSourceHourTransitionError(
            "Invalid source-hour status transition: "
            f"{current_status.value!r} -> {target_status.value!r}"
        )

    is_processing_cancellation_reset = (
        current_status is SourceHourStatus.PROCESSING
        and target_status is SourceHourStatus.DOWNLOADED
    )

    if is_processing_cancellation_reset and not allow_processing_cancellation_reset:
        raise InvalidSourceHourTransitionError(
            "PROCESSING -> DOWNLOADED is permitted only for cooperative "
            "cancellation before any terminal analytical mutation"
        )

    return current_status, target_status


def bounded_diagnostic_text(
    value: object,
    *,
    secrets: Iterable[str] = (),
    maximum_length: int = _MAX_DIAGNOSTIC_TEXT,
) -> str | None:
    """Return bounded single-line diagnostic text with common secrets removed.

    Callers should still translate network exceptions at the transport
    boundary. This function is a final persistence boundary, not permission to
    persist complete credential-bearing request objects.
    """
    if isinstance(maximum_length, bool) or not isinstance(
        maximum_length,
        int,
    ):
        raise ValueError("maximum_length must be an integer")

    if maximum_length <= 0:
        raise ValueError("maximum_length must be positive")

    text = str(value or "").strip()
    if not text:
        return None

    text = _CONTROL_CHARACTERS_RE.sub(" ", text)
    text = text.replace("\n", " ")
    text = _WHITESPACE_RE.sub(" ", text)

    text = _BEARER_RE.sub(r"\1***", text)
    text = _API_KEY_QUERY_RE.sub(r"\1***", text)
    text = _API_KEY_ASSIGNMENT_RE.sub(r"\1***", text)
    text = _PASSWORD_URL_RE.sub(r"\1***\3", text)

    normalized_secrets = sorted(
        {str(secret).strip() for secret in secrets if str(secret or "").strip()},
        key=len,
        reverse=True,
    )

    for secret in normalized_secrets:
        text = text.replace(secret, "***")

    if len(text) <= maximum_length:
        return text

    if maximum_length <= 3:
        return text[:maximum_length]

    return text[: maximum_length - 3].rstrip() + "..."


def nonnegative_counter(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")

    if value < 0:
        raise ValueError(f"{name} must be non-negative")

    return value


__all__ = [
    "FetchRunKind",
    "FetchRunStatus",
    "InvalidSourceHourTransitionError",
    "SourceHourLockUnavailableError",
    "SourceHourStatus",
    "bounded_diagnostic_text",
    "nonnegative_counter",
    "normalize_source_hour_status",
    "validate_source_hour_transition",
]

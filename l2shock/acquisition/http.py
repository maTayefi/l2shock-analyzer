# l2shock/acquisition/http.py
"""Secret-safe HTTP contract helpers for CryptoHFTData acquisition."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Final
from urllib.parse import urlsplit, urlunsplit

from l2shock.acquisition.models import SourceFileSpec

_RETRYABLE_HTTP_STATUSES: Final[frozenset[int]] = frozenset(
    {
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    }
)

_MAX_RETRY_AFTER_SECONDS: Final[float] = 300.0


def download_endpoint(base_url: str) -> str:
    """Return the validated CryptoHFTData `/download` endpoint.

    Query strings and fragments are rejected so credentials or unrelated
    parameters cannot accidentally become part of the configured base URL.
    """
    raw = str(base_url or "").strip().rstrip("/")
    parsed = urlsplit(raw)

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("CryptoHFTData base URL must use HTTP or HTTPS")

    if not parsed.netloc:
        raise ValueError("CryptoHFTData base URL must contain a network host")

    if parsed.query or parsed.fragment:
        raise ValueError("CryptoHFTData base URL must not contain a query or fragment")

    path = parsed.path.rstrip("/") + "/download"

    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            path,
            "",
            "",
        )
    )


def download_query_parameters(
    spec: SourceFileSpec,
    *,
    use_api_key: bool,
    api_key: str,
) -> dict[str, str]:
    """Build the exact `/download?file=` query parameters.

    The returned mapping may contain a credential and therefore must never be
    logged, serialized, persisted, or included in an exception message.
    """
    parameters = {
        "file": spec.remote_path,
    }

    if not use_api_key:
        return parameters

    secret = str(api_key or "").strip()
    if not secret:
        raise ValueError(
            "Direct API-key mode was requested, but no API key is configured"
        )

    parameters["api_key"] = secret
    return parameters


def is_retryable_http_status(status_code: int) -> bool:
    return int(status_code) in _RETRYABLE_HTTP_STATUSES


def parse_retry_after_seconds(
    value: str | None,
    *,
    now: datetime | None = None,
    maximum_seconds: float = _MAX_RETRY_AFTER_SECONDS,
) -> float | None:
    """Parse HTTP `Retry-After` seconds or date into a bounded delay."""
    text = str(value or "").strip()
    if not text:
        return None

    maximum = float(maximum_seconds)
    if not math.isfinite(maximum) or maximum < 0.0:
        raise ValueError("maximum_seconds must be finite and non-negative")

    try:
        numeric = float(text)
    except ValueError:
        numeric = None

    if numeric is not None:
        if not math.isfinite(numeric) or numeric < 0.0:
            return None

        return min(numeric, maximum)

    try:
        retry_at = parsedate_to_datetime(text)
    except TypeError, ValueError, OverflowError:
        return None

    if retry_at.tzinfo is None or retry_at.utcoffset() is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    delay = max(
        0.0,
        (
            retry_at.astimezone(timezone.utc) - current.astimezone(timezone.utc)
        ).total_seconds(),
    )

    return min(delay, maximum)


__all__ = [
    "download_endpoint",
    "download_query_parameters",
    "is_retryable_http_status",
    "parse_retry_after_seconds",
]

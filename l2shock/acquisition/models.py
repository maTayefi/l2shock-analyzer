# l2shock/acquisition/models.py
"""Validated identities for CryptoHFTData hourly source files.

This module defines source identity only. It performs no HTTP, filesystem, or
database operations.

Internal range planning uses half-open UTC intervals:

    [start_utc, end_utc)

Each SourceFileSpec identifies one immutable expected hourly archive object.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Final

from l2shock.timeutils import require_utc_hour

_PROVIDER: Final[str] = "cryptohftdata"

_SUPPORTED_MARKETS: Final[dict[tuple[str, str], str]] = {
    ("binance_futures", "BTCUSDT"): "BTC",
    ("binance_futures", "ETHUSDT"): "ETH",
    ("bybit", "BTCUSDT"): "BTC",
    ("bybit", "ETHUSDT"): "ETH",
    ("okx_futures", "BTC-USDT-SWAP"): "BTC",
    ("okx_futures", "ETH-USDT-SWAP"): "ETH",
}

_SUPPORTED_VENUES: Final[frozenset[str]] = frozenset(
    venue for venue, _symbol in _SUPPORTED_MARKETS
)
_SUPPORTED_SYMBOLS: Final[frozenset[str]] = frozenset(
    symbol for _venue, symbol in _SUPPORTED_MARKETS
)

_SAFE_IDENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class SourceDataKind(StrEnum):
    ORDERBOOK = "orderbook"
    TRADES = "trades"


def _validated_identity(
    name: str,
    value: str,
    *,
    lowercase: bool = False,
    uppercase: bool = False,
) -> str:
    result = str(value or "").strip()

    if lowercase:
        result = result.lower()
    if uppercase:
        result = result.upper()

    if not result:
        raise ValueError(f"{name} must be non-empty")

    if not _SAFE_IDENTITY_RE.fullmatch(result):
        raise ValueError(f"{name} contains unsupported characters: {result!r}")

    return result


@dataclass(frozen=True, slots=True)
class SourceFileSpec:
    """Identity of one expected CryptoHFTData hourly Parquet object."""

    venue: str
    symbol: str
    data_kind: SourceDataKind
    hour_utc: datetime
    provider: str = _PROVIDER

    def __post_init__(self) -> None:
        provider = _validated_identity(
            "provider",
            self.provider,
            lowercase=True,
        )
        venue = _validated_identity(
            "venue",
            self.venue,
            lowercase=True,
        )
        symbol = _validated_identity(
            "symbol",
            self.symbol,
            uppercase=True,
        )

        try:
            data_kind = SourceDataKind(self.data_kind)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in SourceDataKind)
            raise ValueError(f"data_kind must be one of: {allowed}") from exc

        hour_utc = require_utc_hour("hour_utc", self.hour_utc)

        if provider != _PROVIDER:
            raise ValueError(f"Version 1 supports only provider {_PROVIDER!r}")

        if venue not in _SUPPORTED_VENUES:
            raise ValueError(
                "Acquisition currently supports only empirically proven venues "
                f"{sorted(_SUPPORTED_VENUES)}; got {venue!r}"
            )

        market_identity = (venue, symbol)

        if market_identity not in _SUPPORTED_MARKETS:
            supported = sorted(
                f"{supported_venue}/{supported_symbol}"
                for supported_venue, supported_symbol in _SUPPORTED_MARKETS
            )
            raise ValueError(
                "Acquisition currently supports only empirically proven "
                f"venue/symbol identities {supported}; "
                f"got {venue}/{symbol}"
            )

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "data_kind", data_kind)
        object.__setattr__(self, "hour_utc", hour_utc)

    @property
    def base(self) -> str:
        try:
            return _SUPPORTED_MARKETS[
                (
                    self.venue,
                    self.symbol,
                )
            ]
        except KeyError as exc:
            raise RuntimeError(
                "No base mapping exists for source identity "
                f"{self.venue}/{self.symbol}"
            ) from exc

    @property
    def filename(self) -> str:
        return f"{self.symbol}_{self.data_kind.value}.parquet"

    @property
    def remote_path(self) -> str:
        """Return the exact POSIX archive path used by `?file=`."""
        date_part = self.hour_utc.strftime("%Y-%m-%d")
        hour_part = self.hour_utc.strftime("%H")

        path = PurePosixPath(
            self.venue,
            date_part,
            hour_part,
            self.filename,
        )

        result = str(path)

        if path.is_absolute() or ".." in path.parts:
            raise RuntimeError(f"Unsafe generated remote archive path: {result!r}")

        return result

    def local_path(self, raw_root: Path) -> Path:
        """Return the deterministic local raw-archive destination.

        Layout:

            <raw_root>/
                cryptohftdata/
                    <venue>/
                        YYYY-MM-DD/
                            HH/
                                <symbol>_<kind>.parquet
        """
        root = Path(raw_root).expanduser().resolve()
        date_part = self.hour_utc.strftime("%Y-%m-%d")
        hour_part = self.hour_utc.strftime("%H")

        # Resolve the configured root, but do not resolve the generated
        # destination itself. Resolving the destination would follow an
        # existing symbolic link and erase the canonical directory-entry
        # identity needed by acquisition, processing, and maintenance checks.
        destination = (
            root / self.provider / self.venue / date_part / hour_part / self.filename
        )

        try:
            destination.relative_to(root)
        except ValueError as exc:
            raise RuntimeError(
                "Generated local archive path escaped the configured raw root"
            ) from exc

        return destination

    @property
    def identity_tuple(
        self,
    ) -> tuple[str, str, str, str, datetime]:
        return (
            self.provider,
            self.venue,
            self.data_kind.value,
            self.symbol,
            self.hour_utc,
        )


__all__ = [
    "SourceDataKind",
    "SourceFileSpec",
]

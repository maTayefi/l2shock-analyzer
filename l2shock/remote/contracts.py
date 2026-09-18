# l2shock/remote/contracts.py
"""Deterministic identities for remote processed-hour artifacts.

This module defines no network transport and performs no processing.

It establishes the immutable naming and manifest contracts shared by:

- the future headless GitHub Actions worker;
- the future Hugging Face publication adapter;
- the future local Hugging Face importer.

The heavy processing implementation remains in the existing acquisition,
ingest, liquidity, price, processing, and persistence modules.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Final

from l2shock import __version__
from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.presets import (
    LiquidityDataPreset,
    liquidity_data_preset_from_canonical_dict,
)
from l2shock.timeutils import require_utc_hour

REMOTE_ARTIFACT_SCHEMA: Final[str] = "l2shock.remote_hour_artifact"
REMOTE_ARTIFACT_SCHEMA_VERSION: Final[int] = 1


class RemoteContractError(ValueError):
    """A remote artifact identity or manifest violates its contract."""


class RemoteArtifactKind(StrEnum):
    """Remote processed artifact classes."""

    L2 = "l2"
    PRICE = "price"


def _reject_json_float(_value: str) -> None:
    raise RemoteContractError(
        "Remote artifact JSON must not contain floating-point numbers"
    )


def _reject_json_constant(value: str) -> None:
    raise RemoteContractError(
        f"Remote artifact JSON contains unsupported constant {value!r}"
    )


def _unique_json_object(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key, value in pairs:
        if key in result:
            raise RemoteContractError(
                f"Remote artifact JSON contains duplicate key {key!r}"
            )

        result[key] = value

    return result


def _json_object(
    field_name: str,
    value: object,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RemoteContractError(f"{field_name} must be a JSON object")

    result = dict(value)

    if any(not isinstance(key, str) for key in result):
        raise RemoteContractError(f"{field_name} keys must all be strings")

    return result


def _l2_preset_from_json(
    value: object,
) -> LiquidityDataPreset:
    """Decode canonical preset content under the remote exception contract."""

    try:
        return liquidity_data_preset_from_canonical_dict(
            _json_object(
                "remote artifact l2_preset",
                value,
            )
        )
    except RemoteContractError:
        raise
    except (TypeError, ValueError) as exc:
        raise RemoteContractError(
            "Remote artifact l2_preset failed canonical decoding"
        ) from exc


def _canonical_utc_hour_from_json(
    field_name: str,
    value: object,
) -> datetime:
    if not isinstance(value, str):
        raise RemoteContractError(f"{field_name} must be canonical UTC text")

    text = value.strip()

    if not text.endswith("Z"):
        raise RemoteContractError(
            f"{field_name} must end with canonical UTC suffix 'Z'"
        )

    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise RemoteContractError(
            f"{field_name} is not valid ISO-8601 UTC text"
        ) from exc

    try:
        hour = require_utc_hour(field_name, parsed)
    except (TypeError, ValueError) as exc:
        raise RemoteContractError(
            f"{field_name} must identify an exact UTC hour"
        ) from exc

    canonical = hour.isoformat().replace("+00:00", "Z")

    if text != canonical:
        raise RemoteContractError(f"{field_name} is not canonically encoded")

    return hour


def _canonical_sha256(
    field_name: str,
    value: object,
) -> str:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise RemoteContractError(f"{field_name} must be a canonical lowercase SHA-256")

    return digest


def _canonical_git_commit_or_none(
    value: object,
) -> str | None:
    text = str(value or "").strip().lower()

    if not text:
        return None

    if len(text) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise RemoteContractError(
            "producer_git_commit must be null or a canonical "
            "40- or 64-character hexadecimal commit identity"
        )

    return text


def _normalized_identity(
    field_name: str,
    value: object,
    *,
    lowercase: bool = False,
    uppercase: bool = False,
) -> str:
    text = str(value or "").strip()

    if lowercase:
        text = text.lower()

    if uppercase:
        text = text.upper()

    if not text:
        raise RemoteContractError(f"{field_name} cannot be blank")

    if "/" in text or "\\" in text or text in {".", ".."} or ".." in text:
        raise RemoteContractError(
            f"{field_name} is unsafe for remote path construction"
        )

    return text


@dataclass(frozen=True, slots=True)
class RemoteArtifactKey:
    """One immutable processed-hour publication identity."""

    kind: RemoteArtifactKind
    provider: str
    venue: str
    instrument: str
    hour_utc: datetime
    preset_hash: str | None = None
    schema_version: int = REMOTE_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        try:
            kind = RemoteArtifactKind(self.kind)
        except (TypeError, ValueError) as exc:
            raise RemoteContractError("Unsupported remote artifact kind") from exc

        provider = _normalized_identity(
            "provider",
            self.provider,
            lowercase=True,
        )
        venue = _normalized_identity(
            "venue",
            self.venue,
            lowercase=True,
        )
        instrument = _normalized_identity(
            "instrument",
            self.instrument,
            uppercase=True,
        )
        hour = require_utc_hour(
            "hour_utc",
            self.hour_utc,
        )

        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != REMOTE_ARTIFACT_SCHEMA_VERSION
        ):
            raise RemoteContractError("Unsupported remote artifact schema version")

        data_kind = (
            SourceDataKind.ORDERBOOK
            if kind is RemoteArtifactKind.L2
            else SourceDataKind.TRADES
        )

        try:
            spec = SourceFileSpec(
                provider=provider,
                venue=venue,
                symbol=instrument,
                data_kind=data_kind,
                hour_utc=hour,
            )
        except (TypeError, ValueError) as exc:
            raise RemoteContractError(
                "Remote artifact does not identify a supported source"
            ) from exc

        if kind is RemoteArtifactKind.L2:
            if self.preset_hash is None:
                raise RemoteContractError("An L2 remote artifact requires preset_hash")

            preset_hash = _canonical_sha256(
                "preset_hash",
                self.preset_hash,
            )
        else:
            if venue != "binance_futures":
                raise RemoteContractError(
                    "Version 1 remote price artifacts require "
                    "venue='binance_futures'"
                )

            if self.preset_hash is not None:
                raise RemoteContractError(
                    "A price remote artifact cannot own preset_hash"
                )

            preset_hash = None

        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "provider", spec.provider)
        object.__setattr__(self, "venue", spec.venue)
        object.__setattr__(self, "instrument", spec.symbol)
        object.__setattr__(self, "hour_utc", spec.hour_utc)
        object.__setattr__(self, "preset_hash", preset_hash)

    @property
    def source_spec(self) -> SourceFileSpec:
        return SourceFileSpec(
            provider=self.provider,
            venue=self.venue,
            symbol=self.instrument,
            data_kind=(
                SourceDataKind.ORDERBOOK
                if self.kind is RemoteArtifactKind.L2
                else SourceDataKind.TRADES
            ),
            hour_utc=self.hour_utc,
        )

    @property
    def relative_path(self) -> str:
        """Return the canonical path inside the HF dataset repository."""

        date_text = self.hour_utc.strftime("%Y-%m-%d")
        hour_text = self.hour_utc.strftime("%H")

        components = [
            "processed",
            f"v{self.schema_version}",
            self.kind.value,
            self.provider,
            self.venue,
            self.instrument,
        ]

        if self.preset_hash is not None:
            components.append(self.preset_hash)

        components.extend(
            (
                date_text,
                f"{hour_text}.parquet",
            )
        )

        return "/".join(components)

    @property
    def manifest_relative_path(self) -> str:
        artifact_path = self.relative_path

        if not artifact_path.endswith(".parquet"):
            raise RemoteContractError("Remote processed path must end with .parquet")

        return artifact_path[: -len(".parquet")] + ".manifest.json"

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "provider": self.provider,
            "venue": self.venue,
            "instrument": self.instrument,
            "hour_utc": (self.hour_utc.isoformat().replace("+00:00", "Z")),
            "preset_hash": self.preset_hash,
            "schema_version": self.schema_version,
            "relative_path": self.relative_path,
        }

    @classmethod
    def from_canonical_dict(
        cls,
        value: Mapping[str, Any],
    ) -> RemoteArtifactKey:
        payload = _json_object(
            "remote artifact key",
            value,
        )

        expected_fields = {
            "kind",
            "provider",
            "venue",
            "instrument",
            "hour_utc",
            "preset_hash",
            "schema_version",
            "relative_path",
        }

        if set(payload) != expected_fields:
            raise RemoteContractError(
                "Remote artifact key fields do not match " "the version-1 contract"
            )

        result = cls(
            kind=payload["kind"],
            provider=payload["provider"],
            venue=payload["venue"],
            instrument=payload["instrument"],
            hour_utc=_canonical_utc_hour_from_json(
                "key.hour_utc",
                payload["hour_utc"],
            ),
            preset_hash=payload["preset_hash"],
            schema_version=payload["schema_version"],
        )

        if result.to_canonical_dict() != payload:
            raise RemoteContractError("Remote artifact key is valid but not canonical")

        return result


@dataclass(frozen=True, slots=True)
class RemoteSourceHourReference:
    """One exact raw CryptoHFTData source archive reference."""

    provider: str
    venue: str
    instrument: str
    data_kind: SourceDataKind
    hour_utc: datetime
    content_sha256: str

    def __post_init__(self) -> None:
        try:
            spec = SourceFileSpec(
                provider=str(self.provider),
                venue=str(self.venue),
                symbol=str(self.instrument),
                data_kind=SourceDataKind(self.data_kind),
                hour_utc=self.hour_utc,
            )
        except (TypeError, ValueError) as exc:
            raise RemoteContractError("Remote source reference is unsupported") from exc

        object.__setattr__(self, "provider", spec.provider)
        object.__setattr__(self, "venue", spec.venue)
        object.__setattr__(self, "instrument", spec.symbol)
        object.__setattr__(self, "data_kind", spec.data_kind)
        object.__setattr__(self, "hour_utc", spec.hour_utc)
        object.__setattr__(
            self,
            "content_sha256",
            _canonical_sha256(
                "content_sha256",
                self.content_sha256,
            ),
        )

    @property
    def identity_tuple(
        self,
    ) -> tuple[str, str, str, str, datetime, str]:
        return (
            self.provider,
            self.venue,
            self.data_kind.value,
            self.instrument,
            self.hour_utc,
            self.content_sha256,
        )

    @property
    def archive_identity_tuple(
        self,
    ) -> tuple[str, str, str, str, datetime]:
        return self.identity_tuple[:-1]

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "instrument": self.instrument,
            "data_kind": self.data_kind.value,
            "hour_utc": (self.hour_utc.isoformat().replace("+00:00", "Z")),
            "content_sha256": self.content_sha256,
        }

    @classmethod
    def from_canonical_dict(
        cls,
        value: Mapping[str, Any],
    ) -> RemoteSourceHourReference:
        payload = _json_object(
            "remote source-hour reference",
            value,
        )

        expected_fields = {
            "provider",
            "venue",
            "instrument",
            "data_kind",
            "hour_utc",
            "content_sha256",
        }

        if set(payload) != expected_fields:
            raise RemoteContractError(
                "Remote source-hour fields do not match " "the version-1 contract"
            )

        result = cls(
            provider=payload["provider"],
            venue=payload["venue"],
            instrument=payload["instrument"],
            data_kind=payload["data_kind"],
            hour_utc=_canonical_utc_hour_from_json(
                "source_hours.hour_utc",
                payload["hour_utc"],
            ),
            content_sha256=payload["content_sha256"],
        )

        if result.to_canonical_dict() != payload:
            raise RemoteContractError(
                "Remote source-hour reference is valid but not canonical"
            )

        return result


@dataclass(frozen=True, slots=True)
class RemoteArtifactManifest:
    """Deterministic provenance for one published processed hour.

    ``content_sha256`` is the existing compact analytical content identity.
    It is not the SHA-256 of the transport Parquet file.

    Transport bytes may change after a future non-semantic container migration
    while the authoritative analytical content identity remains unchanged.
    """

    key: RemoteArtifactKey
    source_hours: tuple[RemoteSourceHourReference, ...]
    content_sha256: str

    # Required for L2 so a clean local importer can recreate and verify the
    # immutable DataPreset row instead of knowing only its SHA-256.
    #
    # Price artifacts have one fixed V1 source identity and must not own an L2
    # liquidity preset.
    l2_preset: LiquidityDataPreset | None = None

    input_checkpoint_content_sha256: str | None = None
    output_checkpoint_content_sha256: str | None = None

    producer_git_commit: str | None = None
    software_version: str = __version__

    schema: str = REMOTE_ARTIFACT_SCHEMA
    schema_version: int = REMOTE_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.key, RemoteArtifactKey):
            raise TypeError("key must be RemoteArtifactKey")

        if self.schema != REMOTE_ARTIFACT_SCHEMA:
            raise RemoteContractError("Unsupported remote artifact schema identity")

        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != REMOTE_ARTIFACT_SCHEMA_VERSION
        ):
            raise RemoteContractError("Unsupported remote artifact schema version")

        if self.key.schema_version != self.schema_version:
            raise RemoteContractError(
                "Artifact key and manifest schema versions disagree"
            )

        if self.key.kind is RemoteArtifactKind.L2:
            preset = self.l2_preset

            if not isinstance(preset, LiquidityDataPreset):
                raise RemoteContractError(
                    "An L2 remote artifact requires its canonical "
                    "LiquidityDataPreset"
                )

            if preset.preset_hash != self.key.preset_hash:
                raise RemoteContractError(
                    "L2 preset content does not match the artifact preset hash"
                )

            if len(preset.eligible_markets) != 1:
                raise RemoteContractError(
                    "A remote L2 artifact requires exactly one component market"
                )

            market = preset.eligible_markets[0]

            if (
                market.provider != self.key.provider
                or market.venue != self.key.venue
                or market.instrument != self.key.instrument
                or preset.base != self.key.source_spec.base
            ):
                raise RemoteContractError(
                    "L2 preset market identity does not match the artifact key"
                )
        else:
            if self.l2_preset is not None:
                raise RemoteContractError(
                    "A price remote artifact cannot own an L2 preset"
                )

            preset = None

        raw_sources = tuple(self.source_hours)

        if not raw_sources:
            raise RemoteContractError(
                "A remote artifact requires at least one source hour"
            )

        if any(not isinstance(item, RemoteSourceHourReference) for item in raw_sources):
            raise TypeError(
                "source_hours must contain RemoteSourceHourReference objects"
            )

        sources = tuple(
            sorted(
                raw_sources,
                key=lambda item: item.identity_tuple,
            )
        )

        archive_identities = [item.archive_identity_tuple for item in sources]

        if len(set(archive_identities)) != len(archive_identities):
            raise RemoteContractError(
                "source_hours contains duplicate or conflicting "
                "logical source archives"
            )

        expected_kind = (
            SourceDataKind.ORDERBOOK
            if self.key.kind is RemoteArtifactKind.L2
            else SourceDataKind.TRADES
        )

        for source in sources:
            if (
                source.provider != self.key.provider
                or source.venue != self.key.venue
                or source.instrument != self.key.instrument
                or source.data_kind is not expected_kind
            ):
                raise RemoteContractError(
                    "Remote artifact provenance contains a source from "
                    "another provider, venue, instrument, or data kind"
                )

            if (
                self.key.kind is RemoteArtifactKind.L2
                and source.hour_utc > self.key.hour_utc
            ):
                raise RemoteContractError(
                    "L2 artifact provenance cannot reference a future " "source hour"
                )

            if self.key.kind is RemoteArtifactKind.PRICE:
                distance = source.hour_utc - self.key.hour_utc

                if distance not in {
                    -timedelta(hours=1),
                    timedelta(0),
                    timedelta(hours=1),
                }:
                    raise RemoteContractError(
                        "Price artifact provenance may contain only the "
                        "immediately previous, current, or immediately "
                        "following source hour"
                    )

        current_source_exists = any(
            item.hour_utc == self.key.hour_utc for item in sources
        )

        if not current_source_exists:
            raise RemoteContractError(
                "Remote artifact provenance must contain its current "
                "target source hour"
            )

        content_digest = _canonical_sha256(
            "content_sha256",
            self.content_sha256,
        )

        input_checkpoint = (
            _canonical_sha256(
                "input_checkpoint_content_sha256",
                self.input_checkpoint_content_sha256,
            )
            if self.input_checkpoint_content_sha256 is not None
            else None
        )
        output_checkpoint = (
            _canonical_sha256(
                "output_checkpoint_content_sha256",
                self.output_checkpoint_content_sha256,
            )
            if self.output_checkpoint_content_sha256 is not None
            else None
        )

        if self.key.kind is RemoteArtifactKind.PRICE and (
            input_checkpoint is not None or output_checkpoint is not None
        ):
            raise RemoteContractError(
                "Price artifacts cannot own order-book checkpoints"
            )

        software_version = str(self.software_version or "").strip()

        if not software_version:
            raise RemoteContractError("software_version cannot be blank")

        object.__setattr__(self, "source_hours", sources)
        object.__setattr__(self, "content_sha256", content_digest)
        object.__setattr__(self, "l2_preset", preset)
        object.__setattr__(
            self,
            "input_checkpoint_content_sha256",
            input_checkpoint,
        )
        object.__setattr__(
            self,
            "output_checkpoint_content_sha256",
            output_checkpoint,
        )
        object.__setattr__(
            self,
            "producer_git_commit",
            _canonical_git_commit_or_none(
                self.producer_git_commit,
            ),
        )
        object.__setattr__(
            self,
            "software_version",
            software_version,
        )

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "key": self.key.to_canonical_dict(),
            "source_hours": [
                source.to_canonical_dict() for source in self.source_hours
            ],
            "content_sha256": self.content_sha256,
            "l2_preset": (
                self.l2_preset.to_canonical_dict()
                if self.l2_preset is not None
                else None
            ),
            "input_checkpoint_content_sha256": (self.input_checkpoint_content_sha256),
            "output_checkpoint_content_sha256": (self.output_checkpoint_content_sha256),
            "producer": {
                "software_version": self.software_version,
                "git_commit": self.producer_git_commit,
            },
        }

    @property
    def canonical_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_canonical_dict(),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_canonical_dict(
        cls,
        value: Mapping[str, Any],
    ) -> RemoteArtifactManifest:
        payload = _json_object(
            "remote artifact manifest",
            value,
        )

        expected_fields = {
            "schema",
            "schema_version",
            "key",
            "source_hours",
            "content_sha256",
            "l2_preset",
            "input_checkpoint_content_sha256",
            "output_checkpoint_content_sha256",
            "producer",
        }

        if set(payload) != expected_fields:
            raise RemoteContractError(
                "Remote artifact manifest fields do not match " "the version-1 contract"
            )

        raw_sources = payload["source_hours"]

        if not isinstance(raw_sources, list):
            raise RemoteContractError(
                "Remote artifact source_hours must be a JSON array"
            )

        producer = _json_object(
            "remote artifact producer",
            payload["producer"],
        )

        if set(producer) != {
            "software_version",
            "git_commit",
        }:
            raise RemoteContractError(
                "Remote artifact producer fields do not match " "the version-1 contract"
            )

        result = cls(
            key=RemoteArtifactKey.from_canonical_dict(
                _json_object(
                    "remote artifact key",
                    payload["key"],
                )
            ),
            source_hours=tuple(
                RemoteSourceHourReference.from_canonical_dict(
                    _json_object(
                        "remote source-hour reference",
                        source,
                    )
                )
                for source in raw_sources
            ),
            content_sha256=payload["content_sha256"],
            l2_preset=(
                _l2_preset_from_json(
                    payload["l2_preset"],
                )
                if payload["l2_preset"] is not None
                else None
            ),
            input_checkpoint_content_sha256=(
                payload["input_checkpoint_content_sha256"]
            ),
            output_checkpoint_content_sha256=(
                payload["output_checkpoint_content_sha256"]
            ),
            producer_git_commit=producer["git_commit"],
            software_version=producer["software_version"],
            schema=payload["schema"],
            schema_version=payload["schema_version"],
        )

        if result.to_canonical_dict() != payload:
            raise RemoteContractError(
                "Remote artifact manifest is valid but not canonical"
            )

        return result

    @classmethod
    def from_canonical_json_bytes(
        cls,
        encoded: bytes | bytearray | memoryview,
    ) -> RemoteArtifactManifest:
        if not isinstance(encoded, (bytes, bytearray, memoryview)):
            raise TypeError("encoded remote artifact manifest must be bytes-like")

        data = bytes(encoded)

        if not data:
            raise RemoteContractError("Remote artifact manifest cannot be empty")

        if len(data) > 1024 * 1024:
            raise RemoteContractError(
                "Remote artifact manifest exceeds the 1 MiB limit"
            )

        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RemoteContractError(
                "Remote artifact manifest is not valid UTF-8"
            ) from exc

        try:
            decoded = json.loads(
                text,
                parse_float=_reject_json_float,
                parse_constant=_reject_json_constant,
                object_pairs_hook=_unique_json_object,
            )
        except RemoteContractError:
            raise
        except (
            json.JSONDecodeError,
            RecursionError,
        ) as exc:
            raise RemoteContractError(
                "Remote artifact manifest is not valid JSON"
            ) from exc

        result = cls.from_canonical_dict(
            _json_object(
                "remote artifact manifest",
                decoded,
            )
        )

        if result.canonical_json_bytes != data:
            raise RemoteContractError(
                "Remote artifact manifest JSON is not canonically encoded"
            )

        return result

    @property
    def manifest_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes).hexdigest()


__all__ = [
    "REMOTE_ARTIFACT_SCHEMA",
    "REMOTE_ARTIFACT_SCHEMA_VERSION",
    "RemoteArtifactKey",
    "RemoteArtifactKind",
    "RemoteArtifactManifest",
    "RemoteContractError",
    "RemoteSourceHourReference",
]

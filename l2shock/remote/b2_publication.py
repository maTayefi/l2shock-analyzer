# l2shock/remote/b2_publication.py
"""Canonical B2 publication descriptors.

The existing analytical manifest remains unchanged. This storage-specific
descriptor binds that manifest and its transport artifact to exact B2 object
versions, sizes, and SHA-256 hashes.

A descriptor is a completion record, not a distributed writer lock.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from l2shock.remote.b2_transport import (
    B2ObjectInfo,
    MAX_B2_TRANSPORT_BYTES,
)
from l2shock.remote.contracts import RemoteArtifactKey

MAX_B2_PUBLICATION_BYTES = 64 * 1024
MAX_B2_MANIFEST_BYTES = 1024 * 1024


class B2PublicationError(ValueError):
    """A B2 completion descriptor violates its canonical contract."""


def publication_relative_path(key: RemoteArtifactKey) -> str:
    if not isinstance(key, RemoteArtifactKey):
        raise TypeError("key must be RemoteArtifactKey")

    path = key.relative_path
    if not path.endswith(".parquet"):
        raise B2PublicationError("Artifact path does not end with .parquet")

    return path[: -len(".parquet")] + ".publication.json"


def _sha256(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise B2PublicationError("Publication hashes must be lowercase SHA-256")

    return value


def _version(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 1024
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise B2PublicationError("Publication object version is invalid")

    return value


def _size(value: object, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 < value <= maximum
    ):
        raise B2PublicationError("Publication object size is invalid")

    return value


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except TypeError, ValueError, UnicodeError:
        raise B2PublicationError(
            "Publication cannot be canonically serialized"
        ) from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}

    for name, value in pairs:
        if name in result:
            raise B2PublicationError("Publication JSON contains duplicate keys")
        result[name] = value

    return result


def _reject_constant(_value: str) -> object:
    raise B2PublicationError("Publication JSON contains a non-finite number")


def _object_dict(info: B2ObjectInfo) -> dict[str, object]:
    return {
        "key": info.key,
        "size_bytes": info.size_bytes,
        "version_id": info.version_id,
        "transport_sha256": info.transport_sha256,
    }


def _object_from_dict(value: object) -> B2ObjectInfo:
    if not isinstance(value, dict) or set(value) != {
        "key",
        "size_bytes",
        "version_id",
        "transport_sha256",
    }:
        raise B2PublicationError("Publication object fields are invalid")

    return B2ObjectInfo(
        key=value["key"],
        size_bytes=value["size_bytes"],
        version_id=value["version_id"],
        transport_sha256=value["transport_sha256"],
    )


@dataclass(frozen=True, slots=True)
class B2Publication:
    key: RemoteArtifactKey
    artifact: B2ObjectInfo
    manifest: B2ObjectInfo

    def __post_init__(self) -> None:
        if not isinstance(self.key, RemoteArtifactKey):
            raise TypeError("key must be RemoteArtifactKey")

        for info, expected_path, maximum in (
            (
                self.artifact,
                self.key.relative_path,
                MAX_B2_TRANSPORT_BYTES,
            ),
            (
                self.manifest,
                self.key.manifest_relative_path,
                MAX_B2_MANIFEST_BYTES,
            ),
        ):
            if not isinstance(info, B2ObjectInfo):
                raise TypeError("publication objects must be B2ObjectInfo")

            if info.key != expected_path:
                raise B2PublicationError(
                    "Publication object path does not match artifact ownership"
                )

            _size(info.size_bytes, maximum)
            _version(info.version_id)
            _sha256(info.transport_sha256)

        if len(self.canonical_json_bytes) > MAX_B2_PUBLICATION_BYTES:
            raise B2PublicationError("Publication exceeds its byte limit")

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "schema": "l2shock.b2_publication",
            "schema_version": 1,
            "key": self.key.to_canonical_dict(),
            "artifact": _object_dict(self.artifact),
            "manifest": _object_dict(self.manifest),
        }

    @property
    def canonical_json_bytes(self) -> bytes:
        return _canonical(self.to_canonical_dict())

    @property
    def publication_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes).hexdigest()

    @classmethod
    def from_canonical_json_bytes(cls, data: bytes) -> B2Publication:
        if not isinstance(data, bytes):
            raise TypeError("publication data must be bytes")

        if not data or len(data) > MAX_B2_PUBLICATION_BYTES:
            raise B2PublicationError("Publication has an invalid byte length")

        try:
            payload = json.loads(
                data.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            )
        except B2PublicationError:
            raise
        except ValueError, UnicodeError, RecursionError:
            raise B2PublicationError("Publication JSON is invalid") from None

        if not isinstance(payload, dict) or set(payload) != {
            "schema",
            "schema_version",
            "key",
            "artifact",
            "manifest",
        }:
            raise B2PublicationError("Publication fields are invalid")

        if (
            payload["schema"] != "l2shock.b2_publication"
            or isinstance(payload["schema_version"], bool)
            or not isinstance(payload["schema_version"], int)
            or payload["schema_version"] != 1
        ):
            raise B2PublicationError("Publication schema is unsupported")

        try:
            key = RemoteArtifactKey.from_canonical_dict(payload["key"])
        except TypeError, ValueError, KeyError, AttributeError:
            raise B2PublicationError("Publication artifact key is invalid") from None

        result = cls(
            key=key,
            artifact=_object_from_dict(payload["artifact"]),
            manifest=_object_from_dict(payload["manifest"]),
        )

        if result.canonical_json_bytes != data:
            raise B2PublicationError("Publication JSON is not canonical")

        return result


@dataclass(frozen=True, slots=True)
class B2PublicationReference:
    """Exact completion-record version retained by a reader."""

    publication: B2Publication
    descriptor: B2ObjectInfo

    def __post_init__(self) -> None:
        if not isinstance(self.publication, B2Publication):
            raise TypeError("publication must be B2Publication")
        if not isinstance(self.descriptor, B2ObjectInfo):
            raise TypeError("descriptor must be B2ObjectInfo")

        expected = self.publication.canonical_json_bytes

        # B2ObjectInfo annotations do not validate direct construction.
        # Validate primitive descriptor fields before equality comparisons:
        # an integral float must not pass as a canonical integer size.
        _size(self.descriptor.size_bytes, MAX_B2_PUBLICATION_BYTES)
        _version(self.descriptor.version_id)
        _sha256(self.descriptor.transport_sha256)

        if (
            self.descriptor.key != publication_relative_path(self.publication.key)
            or self.descriptor.size_bytes != len(expected)
            or self.descriptor.transport_sha256 != self.publication.publication_sha256
        ):
            raise B2PublicationError(
                "Completion reference does not match its descriptor"
            )

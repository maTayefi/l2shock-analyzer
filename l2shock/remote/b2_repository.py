# l2shock/remote/b2_repository.py
"""Verified B2 processed-artifact publication and downloads.

Publication requires externally enforced single-writer ownership of each
artifact identity. HEAD-before-PUT is not an atomic immutable write.

The completion descriptor is published last and pins exact artifact and
manifest versions. Reads never fall back to a different current version.

This module has no PostgreSQL or UI dependency and does not process raw data.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory

from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    RemoteProcessedArtifact,
    read_remote_artifact_file,
    write_remote_artifact_file,
)
from l2shock.remote.b2_publication import (
    B2Publication,
    B2PublicationError,
    B2PublicationReference,
    MAX_B2_MANIFEST_BYTES,
    MAX_B2_PUBLICATION_BYTES,
    publication_relative_path,
)
from l2shock.remote.b2_transport import (
    B2ObjectInfo,
    B2ObjectStore,
    MAX_B2_TRANSPORT_BYTES,
)
from l2shock.remote.contracts import RemoteArtifactKey


class B2RepositoryError(RuntimeError):
    """A processed B2 publication could not be verified."""


class B2ArtifactNotFoundError(B2RepositoryError):
    """No completed publication exists for the requested key."""


class B2ArtifactConflictError(B2RepositoryError):
    """The immutable artifact identity already owns different content."""


class B2IncompletePublicationError(B2RepositoryError):
    """A completion record refers to unavailable object versions."""


@dataclass(frozen=True, slots=True)
class DownloadedB2Artifact:
    reference: B2PublicationReference
    artifact: RemoteProcessedArtifact

    def __post_init__(self) -> None:
        if not isinstance(self.reference, B2PublicationReference):
            raise TypeError("reference must be B2PublicationReference")

        if not isinstance(
            self.artifact,
            (RemoteL2ProcessedArtifact, RemotePriceProcessedArtifact),
        ):
            raise TypeError("artifact must be a processed remote artifact")

        if self.artifact.manifest.key != self.reference.publication.key:
            raise B2RepositoryError(
                "Downloaded artifact has different publication ownership"
            )

        if (
            self.artifact.manifest.manifest_sha256
            != self.reference.publication.manifest.transport_sha256
        ):
            raise B2RepositoryError(
                "Downloaded artifact manifest differs from publication"
            )


@dataclass(frozen=True, slots=True)
class B2PublicationResult:
    reference: B2PublicationReference
    created: bool

    def __post_init__(self) -> None:
        if not isinstance(self.reference, B2PublicationReference):
            raise TypeError("reference must be B2PublicationReference")
        if not isinstance(self.created, bool):
            raise TypeError("created must be bool")


def _file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0

    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)

    return digest.hexdigest(), size


def _verified_info(
    info: B2ObjectInfo,
    *,
    digest: str,
    size: int,
) -> B2ObjectInfo:
    if info.size_bytes != size or (
        info.transport_sha256 is not None and info.transport_sha256 != digest
    ):
        raise B2RepositoryError("Object bytes disagree with transport metadata")

    return replace(info, transport_sha256=digest)


class B2ProcessedArtifactRepository:
    """Repository borrowing a caller-owned B2ObjectStore.

    The caller owns store.close(), normally through a context manager.
    Temporary transport files are removed before methods return.
    """

    def __init__(self, store: B2ObjectStore) -> None:
        if not isinstance(store, B2ObjectStore):
            raise TypeError("store must be B2ObjectStore")
        self._store = store

    def __repr__(self) -> str:
        return "B2ProcessedArtifactRepository()"

    @property
    def endpoint_url(self) -> str:
        return self._store.endpoint_url

    @property
    def bucket(self) -> str:
        return self._store.bucket

    def resolve_publication(
        self,
        key: RemoteArtifactKey,
        *,
        descriptor_version_id: str | None = None,
    ) -> B2PublicationReference | None:
        if not isinstance(key, RemoteArtifactKey):
            raise TypeError("key must be RemoteArtifactKey")

        downloaded = self._store.get_bytes(
            publication_relative_path(key),
            maximum_bytes=MAX_B2_PUBLICATION_BYTES,
            version_id=descriptor_version_id,
        )

        if downloaded is None:
            if descriptor_version_id is not None:
                raise B2IncompletePublicationError(
                    "The pinned completion descriptor is unavailable"
                )
            return None

        try:
            publication = B2Publication.from_canonical_json_bytes(downloaded.data)
            if publication.key != key:
                raise B2PublicationError(
                    "Completion descriptor belongs to a different key"
                )

            descriptor = _verified_info(
                downloaded.info,
                digest=hashlib.sha256(downloaded.data).hexdigest(),
                size=len(downloaded.data),
            )
            return B2PublicationReference(publication, descriptor)
        except B2PublicationError:
            raise B2RepositoryError(
                "B2 completion descriptor failed verification"
            ) from None

    def _manifest_bytes(self, info: B2ObjectInfo) -> bytes:
        downloaded = self._store.get_bytes(
            info.key,
            maximum_bytes=MAX_B2_MANIFEST_BYTES,
            version_id=info.version_id,
            expected_sha256=info.transport_sha256,
        )

        if downloaded is None:
            raise B2IncompletePublicationError(
                "The pinned manifest version is unavailable"
            )

        if downloaded.info.size_bytes != info.size_bytes:
            raise B2RepositoryError("Manifest size differs from its pinned identity")

        return downloaded.data

    def _artifact_version(
        self,
        info: B2ObjectInfo,
        *,
        key: RemoteArtifactKey,
        manifest_bytes: bytes,
    ) -> tuple[RemoteProcessedArtifact, B2ObjectInfo]:
        with TemporaryDirectory(prefix="l2shock-b2-read-") as directory:
            root = Path(directory)
            destination = root / "artifact.parquet"

            downloaded = self._store.get_file(
                info.key,
                destination,
                owned_root=root,
                maximum_bytes=MAX_B2_TRANSPORT_BYTES,
                version_id=info.version_id,
                expected_sha256=info.transport_sha256,
            )

            if downloaded is None:
                raise B2IncompletePublicationError(
                    "The pinned artifact version is unavailable"
                )

            digest, size = _file_sha256(destination)
            verified = _verified_info(downloaded, digest=digest, size=size)

            if (
                verified.key != info.key
                or verified.version_id != info.version_id
                or verified.size_bytes != info.size_bytes
                or (
                    info.transport_sha256 is not None
                    and verified.transport_sha256 != info.transport_sha256
                )
            ):
                raise B2RepositoryError(
                    "Artifact differs from its pinned transport identity"
                )

            try:
                artifact = read_remote_artifact_file(
                    destination,
                    expected_key=key,
                    expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                    external_manifest_bytes=manifest_bytes,
                )
            except ValueError, TypeError, OSError:
                raise B2RepositoryError(
                    "B2 artifact or canonical manifest failed verification"
                ) from None

        return artifact, verified

    def download_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        reference: B2PublicationReference | None = None,
    ) -> DownloadedB2Artifact | None:
        if not isinstance(key, RemoteArtifactKey):
            raise TypeError("key must be RemoteArtifactKey")

        if reference is None:
            reference = self.resolve_publication(key)
            if reference is None:
                return None
        elif not isinstance(reference, B2PublicationReference):
            raise TypeError("reference must be B2PublicationReference")

        if reference.publication.key != key:
            raise B2RepositoryError(
                "Pinned publication reference belongs to a different key"
            )

        publication = reference.publication
        manifest_bytes = self._manifest_bytes(publication.manifest)
        artifact, _info = self._artifact_version(
            publication.artifact,
            key=key,
            manifest_bytes=manifest_bytes,
        )
        return DownloadedB2Artifact(reference, artifact)

    def require_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        reference: B2PublicationReference | None = None,
    ) -> DownloadedB2Artifact:
        downloaded = self.download_artifact(key, reference=reference)
        if downloaded is None:
            raise B2ArtifactNotFoundError(
                "No completed B2 publication exists for this artifact"
            )
        return downloaded

    @staticmethod
    def _require_identical(
        downloaded: DownloadedB2Artifact,
        artifact: RemoteProcessedArtifact,
    ) -> None:
        if downloaded.artifact != artifact:
            raise B2ArtifactConflictError(
                "Existing publication owns different immutable content"
            )

    def publish_artifact(
        self,
        artifact: RemoteProcessedArtifact,
        *,
        single_writer_confirmed: bool = False,
    ) -> B2PublicationResult:
        if single_writer_confirmed is not True:
            raise B2RepositoryError(
                "Publication requires externally enforced single-writer ownership"
            )

        if not isinstance(
            artifact,
            (RemoteL2ProcessedArtifact, RemotePriceProcessedArtifact),
        ):
            raise TypeError("artifact must be a processed remote artifact")

        key = artifact.manifest.key
        manifest_bytes = artifact.manifest.canonical_json_bytes

        if len(manifest_bytes) > MAX_B2_MANIFEST_BYTES:
            raise B2RepositoryError("Canonical manifest exceeds the B2 manifest limit")

        existing = self.download_artifact(key)
        if existing is not None:
            self._require_identical(existing, artifact)
            return B2PublicationResult(existing.reference, False)

        # Inspect both destinations before writing either one.
        # This permits recovery of compatible interrupted publication but
        # refuses to overwrite conflicting or unverifiable existing content.
        artifact_info = self._store.head(key.relative_path)
        manifest_info = self._store.head(key.manifest_relative_path)

        if manifest_info is not None:
            previous_manifest = self._manifest_bytes(manifest_info)
            if previous_manifest != manifest_bytes:
                raise B2ArtifactConflictError(
                    "Existing canonical manifest owns different content"
                )
            manifest_info = _verified_info(
                manifest_info,
                digest=hashlib.sha256(previous_manifest).hexdigest(),
                size=len(previous_manifest),
            )

        if artifact_info is not None:
            previous_artifact, artifact_info = self._artifact_version(
                artifact_info,
                key=key,
                manifest_bytes=manifest_bytes,
            )
            if previous_artifact != artifact:
                raise B2ArtifactConflictError(
                    "Existing transport artifact owns different content"
                )

        if artifact_info is None:
            with TemporaryDirectory(prefix="l2shock-b2-write-") as directory:
                path = Path(directory) / "artifact.parquet"
                local = write_remote_artifact_file(path, artifact)
                uploaded = self._store.put_file(
                    key.relative_path,
                    path,
                    content_type="application/octet-stream",
                )
                artifact_info = _verified_info(
                    uploaded,
                    digest=local.transport_sha256,
                    size=local.file_size_bytes,
                )

            # Do not publish a completion record based only on HEAD metadata.
            verified_artifact, artifact_info = self._artifact_version(
                artifact_info,
                key=key,
                manifest_bytes=manifest_bytes,
            )
            if verified_artifact != artifact:
                raise B2RepositoryError(
                    "Uploaded artifact differs from the requested artifact"
                )

        if manifest_info is None:
            uploaded = self._store.put_bytes(
                key.manifest_relative_path,
                manifest_bytes,
                content_type="application/json",
            )
            manifest_info = _verified_info(
                uploaded,
                digest=hashlib.sha256(manifest_bytes).hexdigest(),
                size=len(manifest_bytes),
            )
            if self._manifest_bytes(manifest_info) != manifest_bytes:
                raise B2RepositoryError("Uploaded canonical manifest differs")

        # This is detection, not an atomic compare-and-swap.
        # External single-writer ownership remains mandatory.
        existing = self.download_artifact(key)
        if existing is not None:
            self._require_identical(existing, artifact)
            return B2PublicationResult(existing.reference, False)

        publication = B2Publication(
            key=key,
            artifact=artifact_info,
            manifest=manifest_info,
        )
        descriptor = self._store.put_bytes(
            publication_relative_path(key),
            publication.canonical_json_bytes,
            content_type="application/json",
        )
        reference = B2PublicationReference(publication, descriptor)

        reread = self.resolve_publication(
            key,
            descriptor_version_id=descriptor.version_id,
        )
        if reread != reference:
            raise B2RepositoryError(
                "Published completion descriptor failed read-back verification"
            )

        completed = self.require_artifact(key, reference=reference)
        self._require_identical(completed, artifact)
        return B2PublicationResult(reference, True)

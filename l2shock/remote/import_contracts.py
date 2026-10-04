# l2shock/remote/import_contracts.py
"""Verified storage ownership supplied to the shared local importer.

HF retains an actual immutable commit revision.

B2 retains its actual endpoint, bucket, and version-pinned publication
reference. A B2 publication hash is never represented as an HF commit.

This module performs no database access, processing, or network requests.
"""

from __future__ import annotations

from dataclasses import dataclass

from l2shock.config import B2Config
from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    RemoteProcessedArtifact,
)
from l2shock.remote.b2_publication import B2PublicationReference
from l2shock.remote.b2_repository import DownloadedB2Artifact
from l2shock.remote.hf_repository import DownloadedHuggingFaceArtifact

HF_STORAGE_BACKEND = "hugging_face"
B2_STORAGE_BACKEND = "backblaze_b2"


class RemoteImportIdentityError(ValueError):
    """Remote storage ownership is invalid or internally inconsistent."""


def _commit_revision(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise RemoteImportIdentityError(
            "HF import ownership requires a canonical full commit SHA"
        )
    return value


@dataclass(frozen=True, slots=True)
class RemoteImportStorageIdentity:
    """Immutable, credential-free ownership of one remote import."""

    backend: str
    revision: str | None = None
    endpoint_url: str | None = None
    bucket: str | None = None
    publication_reference: B2PublicationReference | None = None

    def __post_init__(self) -> None:
        if self.backend == HF_STORAGE_BACKEND:
            _commit_revision(self.revision)

            if (
                self.endpoint_url is not None
                or self.bucket is not None
                or self.publication_reference is not None
            ):
                raise RemoteImportIdentityError(
                    "HF import ownership cannot contain B2 location or versions"
                )
            return

        if self.backend != B2_STORAGE_BACKEND:
            raise RemoteImportIdentityError("Unsupported remote storage backend")

        if self.revision is not None:
            raise RemoteImportIdentityError(
                "B2 import ownership must not fabricate an HF revision"
            )

        if not isinstance(self.publication_reference, B2PublicationReference):
            raise RemoteImportIdentityError(
                "B2 import ownership requires a verified publication reference"
            )

        if not isinstance(self.endpoint_url, str) or not isinstance(self.bucket, str):
            raise RemoteImportIdentityError(
                "B2 import ownership requires an endpoint and bucket"
            )

        # Reuse the transport's location policy without requiring credentials.
        # This constructor cannot initiate a network connection.
        location = B2Config(
            endpoint_url=self.endpoint_url,
            bucket=self.bucket,
        )

        if not location.endpoint_url or not location.bucket:
            raise RemoteImportIdentityError("B2 import location cannot be empty")

        if location.endpoint_url != self.endpoint_url or location.bucket != self.bucket:
            raise RemoteImportIdentityError(
                "B2 import location must already be canonical"
            )

    def to_canonical_dict(self) -> dict[str, object]:
        if self.backend == HF_STORAGE_BACKEND:
            return {
                "schema": "l2shock.remote_import_storage_identity",
                "schema_version": 1,
                "backend": self.backend,
                "revision": self.revision,
            }

        reference = self.publication_reference
        assert reference is not None

        descriptor = reference.descriptor

        return {
            "schema": "l2shock.remote_import_storage_identity",
            "schema_version": 1,
            "backend": self.backend,
            "endpoint_url": self.endpoint_url,
            "bucket": self.bucket,
            "publication_sha256": reference.publication.publication_sha256,
            "descriptor": {
                "key": descriptor.key,
                "version_id": descriptor.version_id,
                "size_bytes": descriptor.size_bytes,
                "transport_sha256": descriptor.transport_sha256,
            },
            "publication": reference.publication.to_canonical_dict(),
        }


@dataclass(frozen=True, slots=True)
class VerifiedRemoteImportArtifact:
    """Already-verified analytical artifact plus exact storage ownership."""

    storage_identity: RemoteImportStorageIdentity
    artifact: RemoteProcessedArtifact

    def __post_init__(self) -> None:
        if not isinstance(self.storage_identity, RemoteImportStorageIdentity):
            raise TypeError("storage_identity must be RemoteImportStorageIdentity")

        if not isinstance(
            self.artifact,
            (RemoteL2ProcessedArtifact, RemotePriceProcessedArtifact),
        ):
            raise TypeError("artifact must be a verified processed artifact")

        if self.storage_identity.backend == B2_STORAGE_BACKEND:
            reference = self.storage_identity.publication_reference
            assert reference is not None

            # Apply the existing B2 artifact/reference association checks.
            # Do not duplicate or weaken those checks in the importer.
            DownloadedB2Artifact(
                reference=reference,
                artifact=self.artifact,
            )

    @property
    def revision(self) -> str | None:
        return self.storage_identity.revision

    @classmethod
    def from_huggingface(
        cls,
        downloaded: DownloadedHuggingFaceArtifact,
    ) -> VerifiedRemoteImportArtifact:
        if not isinstance(downloaded, DownloadedHuggingFaceArtifact):
            raise TypeError("downloaded must be DownloadedHuggingFaceArtifact")

        return cls(
            storage_identity=RemoteImportStorageIdentity(
                backend=HF_STORAGE_BACKEND,
                revision=downloaded.revision,
            ),
            artifact=downloaded.artifact,
        )

    @classmethod
    def from_b2(
        cls,
        downloaded: DownloadedB2Artifact,
        *,
        endpoint_url: str,
        bucket: str,
    ) -> VerifiedRemoteImportArtifact:
        if not isinstance(downloaded, DownloadedB2Artifact):
            raise TypeError("downloaded must be DownloadedB2Artifact")

        return cls(
            storage_identity=RemoteImportStorageIdentity(
                backend=B2_STORAGE_BACKEND,
                endpoint_url=endpoint_url,
                bucket=bucket,
                publication_reference=downloaded.reference,
            ),
            artifact=downloaded.artifact,
        )


__all__ = [
    "B2_STORAGE_BACKEND",
    "HF_STORAGE_BACKEND",
    "RemoteImportIdentityError",
    "RemoteImportStorageIdentity",
    "VerifiedRemoteImportArtifact",
]

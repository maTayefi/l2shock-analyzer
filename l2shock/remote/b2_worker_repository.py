# l2shock/remote/b2_worker_repository.py
"""Chain-scoped B2 repository boundary for the shared remote worker.

The caller owns the object-store lifetime and externally enforced
single-writer admission.

B2 has no repository-wide revision. Each inspection generation remembers
one completion reference, or absence, per artifact key. Reads consume
the exact remembered reference.

Only a verified publication performed by this adapter may replace a
remembered reference inside the current generation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from l2shock.presets import LiquidityDataPreset
from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    RemoteProcessedArtifact,
)
from l2shock.remote.b2_publication import B2PublicationReference
from l2shock.remote.b2_repository import (
    B2ArtifactNotFoundError,
    B2ProcessedArtifactRepository,
    B2PublicationResult,
    B2RepositoryError,
)
from l2shock.remote.b2_transport import B2ObjectStore
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
)
from l2shock.remote.import_contracts import (
    B2_STORAGE_BACKEND,
    RemoteImportStorageIdentity,
    VerifiedRemoteImportArtifact,
)


@dataclass(frozen=True, slots=True)
class B2WorkerPredecessorCheckpoint:
    """Verified predecessor content with truthful B2 storage ownership."""

    predecessor: VerifiedRemoteImportArtifact

    def __post_init__(self) -> None:
        if not isinstance(self.predecessor, VerifiedRemoteImportArtifact):
            raise TypeError("predecessor must be VerifiedRemoteImportArtifact")

        if self.predecessor.storage_identity.backend != B2_STORAGE_BACKEND:
            raise B2RepositoryError("Predecessor must have B2 storage ownership")

        if not isinstance(self.predecessor.artifact, RemoteL2ProcessedArtifact):
            raise B2RepositoryError("Predecessor must contain an L2 artifact")

    @property
    def checkpoint_bytes(self) -> bytes | None:
        return self.predecessor.artifact.output_checkpoint


class B2WorkerRepository:
    """Adapt the existing verified B2 repository to one worker chain.

    current_revision() starts a fresh inspection generation and returns None.
    It performs no network operation and does not manufacture a revision.

    This adapter is intended for the worker's sequential repository calls.
    It is not a shared mutable repository for concurrent worker operations.
    """

    storage_backend = B2_STORAGE_BACKEND

    def __init__(
        self,
        store: B2ObjectStore,
        *,
        preset: LiquidityDataPreset,
        single_writer_confirmed: bool = False,
    ) -> None:
        if not isinstance(store, B2ObjectStore):
            raise TypeError("store must be B2ObjectStore")

        if not isinstance(preset, LiquidityDataPreset):
            raise TypeError("preset must be LiquidityDataPreset")

        if not isinstance(single_writer_confirmed, bool):
            raise TypeError("single_writer_confirmed must be bool")

        if len(preset.eligible_markets) != 1:
            raise B2RepositoryError("A worker repository requires one component market")

        market = preset.eligible_markets[0]

        if market.provider != "cryptohftdata" or market.venue not in {
            "binance_futures",
            "bybit",
            "okx_futures",
        }:
            raise B2RepositoryError("Unsupported B2 worker component market")

        self._repository = B2ProcessedArtifactRepository(store)
        self._provider = market.provider
        self._venue = market.venue
        self._instrument = market.instrument
        self._preset_hash = preset.preset_hash
        self._single_writer_confirmed = single_writer_confirmed

        self._references: dict[
            RemoteArtifactKey,
            B2PublicationReference | None,
        ] = {}

    def __repr__(self) -> str:
        return "B2WorkerRepository()"

    @property
    def endpoint_url(self) -> str:
        return self._repository.endpoint_url

    @property
    def bucket(self) -> str:
        return self._repository.bucket

    def current_revision(self) -> None:
        """Start a new per-key inspection generation without network access."""
        self._references.clear()
        return None

    def _require_key(self, key: RemoteArtifactKey) -> None:
        if not isinstance(key, RemoteArtifactKey):
            raise TypeError("key must be RemoteArtifactKey")

        if (
            key.provider != self._provider
            or key.venue != self._venue
            or key.instrument != self._instrument
        ):
            raise B2RepositoryError("Artifact belongs to another worker chain")

        if key.kind is RemoteArtifactKind.L2:
            if key.preset_hash != self._preset_hash:
                raise B2RepositoryError("L2 artifact belongs to another worker preset")
            return

        if key.kind is not RemoteArtifactKind.PRICE:
            raise B2RepositoryError("Unsupported worker artifact kind")

        if self._venue != "binance_futures":
            raise B2RepositoryError("Only Binance worker chains own price artifacts")

    @staticmethod
    def _require_no_revision(revision: str | None) -> None:
        if revision is not None:
            raise B2RepositoryError("B2 worker operations do not accept an HF revision")

    def _reference_for(
        self,
        key: RemoteArtifactKey,
    ) -> B2PublicationReference | None:
        self._require_key(key)

        if key not in self._references:
            self._references[key] = self._repository.resolve_publication(key)

        return self._references[key]

    def download_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> VerifiedRemoteImportArtifact | None:
        self._require_no_revision(revision)
        reference = self._reference_for(key)

        if reference is None:
            return None

        downloaded = self._repository.require_artifact(
            key,
            reference=reference,
        )

        if downloaded.reference != reference:
            raise B2RepositoryError("Worker download changed its pinned publication")

        return VerifiedRemoteImportArtifact.from_b2(
            downloaded,
            endpoint_url=self.endpoint_url,
            bucket=self.bucket,
        )

    def download_l2_predecessor_checkpoint(
        self,
        target_key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> B2WorkerPredecessorCheckpoint:
        self._require_no_revision(revision)
        self._require_key(target_key)

        if target_key.kind is not RemoteArtifactKind.L2:
            raise B2RepositoryError("Predecessor checkpoints require an L2 target")

        predecessor_key = RemoteArtifactKey(
            kind=RemoteArtifactKind.L2,
            provider=target_key.provider,
            venue=target_key.venue,
            instrument=target_key.instrument,
            hour_utc=target_key.hour_utc - timedelta(hours=1),
            preset_hash=target_key.preset_hash,
            schema_version=target_key.schema_version,
        )

        downloaded = self.download_artifact(predecessor_key)

        if downloaded is None:
            raise B2ArtifactNotFoundError(
                "No completed publication exists for the immediate predecessor"
            )

        return B2WorkerPredecessorCheckpoint(downloaded)

    def publish_artifact(
        self,
        artifact: RemoteProcessedArtifact,
    ) -> B2PublicationResult:
        if not self._single_writer_confirmed:
            raise B2RepositoryError(
                "B2 worker publication requires external single-writer ownership"
            )

        if not isinstance(artifact, RemoteL2ProcessedArtifact):
            from l2shock.remote.artifact_codec import RemotePriceProcessedArtifact

            if not isinstance(artifact, RemotePriceProcessedArtifact):
                raise TypeError("artifact must be a processed remote artifact")

        key = artifact.manifest.key
        self._require_key(key)

        result = self._repository.publish_artifact(
            artifact,
            single_writer_confirmed=True,
        )

        if result.reference.publication.key != key:
            raise B2RepositoryError("Publication result belongs to another artifact")

        # This is an explicitly verified write by this adapter, not a
        # fallback from a failed pinned read to a newer current object.
        self._references[key] = result.reference
        return result

    def observed_storage_identities(
        self,
    ) -> tuple[RemoteImportStorageIdentity, ...]:
        """Return credential-free references retained in this generation."""
        references = sorted(
            (
                reference
                for reference in self._references.values()
                if reference is not None
            ),
            key=lambda reference: reference.publication.key.relative_path,
        )

        return tuple(
            RemoteImportStorageIdentity(
                backend=B2_STORAGE_BACKEND,
                endpoint_url=self.endpoint_url,
                bucket=self.bucket,
                publication_reference=reference,
            )
            for reference in references
        )


__all__ = [
    "B2WorkerPredecessorCheckpoint",
    "B2WorkerRepository",
]

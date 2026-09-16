# l2shock/remote/source_acquisition.py
"""PostgreSQL-free CryptoHFTData acquisition for ephemeral workers.

This module is a thin infrastructure adapter around the existing
``CryptoHFTDownloader``.

It deliberately does not:

- implement a second HTTP downloader;
- persist source-hour state to PostgreSQL;
- reconstruct an order book;
- calculate liquidity;
- construct trade OHLC;
- contact Hugging Face;
- schedule GitHub Actions;
- start NiceGUI.

A future GitHub Actions job will create one temporary worker workspace,
download explicit source archives through this module, process them through
``l2shock.remote.headless_processing``, publish the compact results, and then
discard the complete temporary workspace.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from l2shock.acquisition import (
    CryptoHFTDownloader,
    DownloadArtifact,
    DownloadDisposition,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.config import CryptoHFTConfig, StorageConfig
from l2shock.processing import ProcessingSourceArchive
from l2shock.timeutils import require_utc_hour


class RemoteWorkerAcquisitionError(RuntimeError):
    """Remote-worker source acquisition could not be admitted safely."""


class RemoteWorkerSourceNotEligibleError(RemoteWorkerAcquisitionError):
    """A requested source hour is newer than the admitted release boundary."""


@dataclass(frozen=True, slots=True)
class RemoteWorkerWorkspace:
    """Application-owned directories inside one ephemeral worker root."""

    root: Path
    storage: StorageConfig

    def __post_init__(self) -> None:
        root = Path(self.root).expanduser().resolve()

        if not root.is_dir():
            raise RemoteWorkerAcquisitionError(
                "Remote worker root must be an existing directory"
            )

        if not isinstance(self.storage, StorageConfig):
            raise TypeError("storage must be a StorageConfig")

        owned_paths = (
            self.storage.raw_path,
            self.storage.cache_path,
            self.storage.quarantine_path,
            self.storage.export_path,
            self.storage.log_path,
            self.storage.backup_path,
        )

        for path in owned_paths:
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise RemoteWorkerAcquisitionError(
                    "Remote worker storage path escaped its workspace root"
                ) from exc

        object.__setattr__(self, "root", root)


@dataclass(frozen=True, slots=True)
class RemoteWorkerAcquisitionResult:
    """Validated downloaded inputs for one explicit worker operation."""

    requested_specs: tuple[SourceFileSpec, ...]
    download_artifacts: tuple[DownloadArtifact, ...]
    processing_archives: tuple[ProcessingSourceArchive, ...]

    def __post_init__(self) -> None:
        specs = tuple(self.requested_specs)
        artifacts = tuple(self.download_artifacts)
        archives = tuple(self.processing_archives)

        if not specs:
            raise RemoteWorkerAcquisitionError(
                "Remote worker acquisition requires at least one source"
            )

        if any(not isinstance(spec, SourceFileSpec) for spec in specs):
            raise TypeError("requested_specs must contain SourceFileSpec objects")

        identities = [spec.identity_tuple for spec in specs]

        if len(set(identities)) != len(identities):
            raise RemoteWorkerAcquisitionError(
                "Remote worker acquisition contains a duplicate source identity"
            )

        if len(artifacts) != len(specs):
            raise RemoteWorkerAcquisitionError(
                "Download-artifact count does not match requested source count"
            )

        if len(archives) != len(specs):
            raise RemoteWorkerAcquisitionError(
                "Processing-archive count does not match requested source count"
            )

        for spec, artifact, archive in zip(
            specs,
            artifacts,
            archives,
            strict=True,
        ):
            if not isinstance(artifact, DownloadArtifact):
                raise TypeError(
                    "download_artifacts must contain DownloadArtifact objects"
                )

            if not isinstance(archive, ProcessingSourceArchive):
                raise TypeError(
                    "processing_archives must contain "
                    "ProcessingSourceArchive objects"
                )

            if artifact.spec != spec:
                raise RemoteWorkerAcquisitionError(
                    "Downloaded artifact identity does not match its request"
                )

            if archive.spec != spec:
                raise RemoteWorkerAcquisitionError(
                    "Processing archive identity does not match its request"
                )

            if archive.local_path != artifact.local_path:
                raise RemoteWorkerAcquisitionError(
                    "Processing archive path does not match its download artifact"
                )

            if archive.file_size_bytes != artifact.file_size_bytes:
                raise RemoteWorkerAcquisitionError(
                    "Processing archive size does not match its download artifact"
                )

            if archive.content_sha256 != artifact.content_sha256:
                raise RemoteWorkerAcquisitionError(
                    "Processing archive SHA-256 does not match its " "download artifact"
                )

        object.__setattr__(self, "requested_specs", specs)
        object.__setattr__(self, "download_artifacts", artifacts)
        object.__setattr__(self, "processing_archives", archives)

    @property
    def downloaded_count(self) -> int:
        return sum(
            artifact.disposition is DownloadDisposition.DOWNLOADED
            for artifact in self.download_artifacts
        )

    @property
    def reused_count(self) -> int:
        return sum(
            artifact.disposition is DownloadDisposition.REUSED
            for artifact in self.download_artifacts
        )

    @property
    def source_count(self) -> int:
        return len(self.requested_specs)


def build_remote_worker_workspace(
    workspace_root: Path,
    *,
    minimum_free_disk_gib: float = 0.0,
) -> RemoteWorkerWorkspace:
    """Create isolated sibling storage roles beneath one worker root.

    The resulting ``StorageConfig`` is compatible with the existing
    ``CryptoHFTDownloader``. Raw archives remain under the workspace and can
    therefore be discarded by deleting the workspace after publication.
    """

    raw_root = Path(workspace_root).expanduser()

    if raw_root.exists() and raw_root.is_symlink():
        raise RemoteWorkerAcquisitionError(
            "Remote worker root cannot be a symbolic link"
        )

    root = raw_root.resolve()
    root.mkdir(
        parents=True,
        exist_ok=True,
    )

    directory_names = (
        "raw",
        "cache",
        "quarantine",
        "exports",
        "logs",
        "backups",
    )

    for name in directory_names:
        path = root / name

        if path.exists() and path.is_symlink():
            raise RemoteWorkerAcquisitionError(
                f"Remote worker {name} directory cannot be a symbolic link"
            )

        path.mkdir(
            parents=True,
            exist_ok=True,
        )

    storage = StorageConfig(
        raw_dir=str(root / "raw"),
        cache_dir=str(root / "cache"),
        quarantine_dir=str(root / "quarantine"),
        export_dir=str(root / "exports"),
        log_dir=str(root / "logs"),
        backup_dir=str(root / "backups"),
        raw_retention_hours=1,
        minimum_free_disk_gib=minimum_free_disk_gib,
    )

    return RemoteWorkerWorkspace(
        root=root,
        storage=storage,
    )


@contextmanager
def temporary_remote_worker_workspace(
    *,
    parent: Path | None = None,
    minimum_free_disk_gib: float = 0.0,
) -> Iterator[RemoteWorkerWorkspace]:
    """Yield one automatically removed remote-worker workspace."""

    if parent is None:
        parent_path = None
    else:
        raw_parent = Path(parent).expanduser()

        if raw_parent.exists() and raw_parent.is_symlink():
            raise RemoteWorkerAcquisitionError(
                "Temporary-workspace parent cannot be a symbolic link"
            )

        parent_path = raw_parent.resolve()
        parent_path.mkdir(
            parents=True,
            exist_ok=True,
        )

    with TemporaryDirectory(
        prefix="l2shock-remote-worker-",
        dir=parent_path,
    ) as directory:
        yield build_remote_worker_workspace(
            Path(directory),
            minimum_free_disk_gib=minimum_free_disk_gib,
        )


def _validated_requested_specs(
    specs: Sequence[SourceFileSpec],
    *,
    latest_eligible_hour_utc: datetime | None,
) -> tuple[SourceFileSpec, ...]:
    requested = tuple(specs)

    if not requested:
        raise RemoteWorkerAcquisitionError(
            "Remote worker acquisition requires at least one source"
        )

    if any(not isinstance(spec, SourceFileSpec) for spec in requested):
        raise TypeError("specs must contain SourceFileSpec objects")

    identities = [spec.identity_tuple for spec in requested]

    if len(set(identities)) != len(identities):
        raise RemoteWorkerAcquisitionError(
            "Remote worker acquisition contains a duplicate source identity"
        )

    if latest_eligible_hour_utc is not None:
        latest = require_utc_hour(
            "latest_eligible_hour_utc",
            latest_eligible_hour_utc,
        )

        future_specs = tuple(spec for spec in requested if spec.hour_utc > latest)

        if future_specs:
            first = future_specs[0]

            raise RemoteWorkerSourceNotEligibleError(
                "Requested source hour is newer than the admitted release "
                f"boundary: requested={first.hour_utc.isoformat()}, "
                f"latest_eligible={latest.isoformat()}"
            )

    return requested


async def acquire_remote_worker_archives(
    specs: Sequence[SourceFileSpec],
    *,
    cryptohft: CryptoHFTConfig,
    workspace: RemoteWorkerWorkspace,
    latest_eligible_hour_utc: datetime | None = None,
    use_api_key: bool = False,
    cancel_event=None,
    client: httpx.AsyncClient | None = None,
) -> RemoteWorkerAcquisitionResult:
    """Download explicit source archives without PostgreSQL persistence.

    Request ordering is retained exactly. This is important for explicit
    adjacent price-source selection and for deterministic worker diagnostics.

    ``latest_eligible_hour_utc`` is optional for historical/manual validation.
    Scheduled workers must supply it so a source newer than the shared release
    schedule cannot be admitted accidentally.
    """

    if not isinstance(cryptohft, CryptoHFTConfig):
        raise TypeError("cryptohft must be a CryptoHFTConfig")

    if not isinstance(workspace, RemoteWorkerWorkspace):
        raise TypeError("workspace must be a RemoteWorkerWorkspace")

    if not isinstance(use_api_key, bool):
        raise TypeError("use_api_key must be bool")

    requested = _validated_requested_specs(
        specs,
        latest_eligible_hour_utc=latest_eligible_hour_utc,
    )

    artifacts: list[DownloadArtifact] = []
    archives: list[ProcessingSourceArchive] = []

    async with CryptoHFTDownloader(
        cryptohft=cryptohft,
        storage=workspace.storage,
        client=client,
        use_api_key=use_api_key,
    ) as downloader:
        for spec in requested:
            artifact = await downloader.download(
                spec,
                cancel_event=cancel_event,
            )

            processing_archive = ProcessingSourceArchive(
                spec=spec,
                local_path=artifact.local_path,
                content_sha256=artifact.content_sha256,
                file_size_bytes=artifact.file_size_bytes,
                status=SourceHourStatus.DOWNLOADED,
            )

            artifacts.append(artifact)
            archives.append(processing_archive)

    return RemoteWorkerAcquisitionResult(
        requested_specs=requested,
        download_artifacts=tuple(artifacts),
        processing_archives=tuple(archives),
    )


__all__ = [
    "RemoteWorkerAcquisitionError",
    "RemoteWorkerAcquisitionResult",
    "RemoteWorkerSourceNotEligibleError",
    "RemoteWorkerWorkspace",
    "acquire_remote_worker_archives",
    "build_remote_worker_workspace",
    "temporary_remote_worker_workspace",
]

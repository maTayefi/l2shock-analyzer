# l2shock/remote/hf_repository.py
"""Verified private Hugging Face dataset repository transport.

This module owns only remote storage operations for already-constructed
``RemoteProcessedArtifact`` objects.

It deliberately performs no:

- CryptoHFTData acquisition;
- order-book replay;
- liquidity calculation;
- price construction;
- PostgreSQL access;
- NiceGUI work;
- GitHub Actions scheduling.

Read ownership:

- the repository head is resolved to a full immutable commit SHA;
- artifact Parquet and external manifest are downloaded from that same SHA;
- ``repo_type='dataset'`` is always supplied;
- the existing strict remote artifact codec verifies the downloaded pair.

Write ownership:

- artifact Parquet and canonical external manifest are one HF commit;
- ``parent_commit`` is the exact branch head observed before publication;
- a changed branch head causes the commit to fail rather than overwrite based
  on stale state;
- after a concurrent-head failure, the new head is read and inspected before
  any retry;
- identical existing content is idempotent success;
- conflicting immutable content is never silently replaced.

The Hugging Face token is retained only in process memory and must never be
included in exceptions, logs, manifests, artifacts, or object representations.
"""

from __future__ import annotations

import email.utils
import random
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol

from huggingface_hub import (
    CommitOperationAdd,
    HfApi,
    hf_hub_download,
)
from huggingface_hub.utils import (
    EntryNotFoundError,
    HfHubHTTPError,
    RepositoryNotFoundError,
)
from pydantic import SecretStr

from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    RemoteProcessedArtifact,
    read_remote_artifact_file,
    write_remote_artifact_file,
)
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
)

_REPO_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/" r"[A-Za-z0-9][A-Za-z0-9._-]*$")
_COMMIT_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")

_MINIMUM_CONFLICT_RETRY_SECONDS = 5.0
_MAXIMUM_CONFLICT_RETRY_SECONDS = 60.0
_MAXIMUM_RATE_LIMIT_RETRY_SECONDS = 3_900.0
_DEFAULT_RATE_LIMIT_RETRY_SECONDS = 3_600.0


def _http_status_code(exc: HfHubHTTPError) -> int | None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)

    if isinstance(status_code, bool) or not isinstance(status_code, int):
        return None

    return status_code


def _retry_after_seconds(
    exc: HfHubHTTPError,
    *,
    now: datetime | None = None,
) -> float | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)

    if headers is None:
        return None

    raw = str(headers.get("Retry-After", "") or "").strip()

    if not raw:
        return None

    try:
        seconds = float(raw)
    except ValueError:
        seconds = None

    if seconds is not None:
        if seconds < 0:
            return None

        return min(
            seconds,
            _MAXIMUM_RATE_LIMIT_RETRY_SECONDS,
        )

    try:
        retry_at = email.utils.parsedate_to_datetime(raw)
    except TypeError, ValueError, OverflowError:
        return None

    if retry_at.tzinfo is None or retry_at.utcoffset() is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    current = now or datetime.now(timezone.utc)
    delay = (
        retry_at.astimezone(timezone.utc) - current.astimezone(timezone.utc)
    ).total_seconds()

    return min(
        max(0.0, delay),
        _MAXIMUM_RATE_LIMIT_RETRY_SECONDS,
    )


def _publication_retry_delay(
    *,
    attempt: int,
    rate_limited: bool,
    retry_after_seconds: float | None,
) -> float:
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt <= 0:
        raise ValueError("attempt must be a positive integer")

    if rate_limited:
        requested_delay = (
            retry_after_seconds
            if retry_after_seconds is not None
            else _DEFAULT_RATE_LIMIT_RETRY_SECONDS
        )
        base = max(
            _MINIMUM_CONFLICT_RETRY_SECONDS,
            requested_delay,
        )
        jitter_limit = min(
            30.0,
            max(
                1.0,
                base * 0.05,
            ),
        )
    else:
        base = min(
            _MAXIMUM_CONFLICT_RETRY_SECONDS,
            _MINIMUM_CONFLICT_RETRY_SECONDS * (2 ** (attempt - 1)),
        )
        jitter_limit = min(5.0, base * 0.25)

    return max(
        0.0,
        base + random.uniform(0.0, jitter_limit),
    )


class HuggingFaceRepositoryError(RuntimeError):
    """Base error for verified Hugging Face dataset transport."""


class HuggingFaceRepositoryUnavailableError(HuggingFaceRepositoryError):
    """The configured dataset repository is missing or inaccessible."""


class HuggingFaceArtifactNotFoundError(HuggingFaceRepositoryError):
    """A required immutable processed artifact does not exist."""


class HuggingFacePartialArtifactError(HuggingFaceRepositoryError):
    """Only one member of an artifact/manifest pair exists."""


class HuggingFaceArtifactConflictError(HuggingFaceRepositoryError):
    """An immutable HF artifact path already owns different content."""


class HuggingFacePublicationError(HuggingFaceRepositoryError):
    """A verified artifact could not be published safely."""


class _RepositoryInfoProtocol(Protocol):
    sha: str


class _HfApiProtocol(Protocol):
    def repo_info(
        self,
        *,
        repo_id: str,
        repo_type: str,
        revision: str,
        token: str,
    ) -> _RepositoryInfoProtocol: ...

    def create_commit(
        self,
        *,
        repo_id: str,
        repo_type: str,
        revision: str,
        parent_commit: str,
        create_pr: bool,
        operations: list[CommitOperationAdd],
        commit_message: str,
        token: str,
    ) -> Any: ...


HfDownloadFunction = Callable[..., str]


def _repository_id(value: object) -> str:
    text = str(value or "").strip()

    if not _REPO_ID_RE.fullmatch(text):
        raise ValueError("repo_id must use canonical 'namespace/dataset-name' form")

    return text


def _branch_name(value: object) -> str:
    text = str(value or "").strip()

    if not text:
        raise ValueError("revision cannot be blank")

    if text in {".", ".."} or text.startswith("/") or text.endswith("/"):
        raise ValueError("revision contains an unsupported branch identity")

    if any(character.isspace() for character in text):
        raise ValueError("revision cannot contain whitespace")

    return text


def _full_commit_sha(
    field_name: str,
    value: object,
) -> str:
    text = str(value or "").strip().lower()

    if not _COMMIT_SHA_RE.fullmatch(text):
        raise HuggingFaceRepositoryError(
            f"{field_name} must be a full canonical commit SHA"
        )

    return text


def _token_text(value: SecretStr | str) -> str:
    if isinstance(value, SecretStr):
        text = value.get_secret_value().strip()
    else:
        text = str(value or "").strip()

    if not text:
        raise ValueError("A Hugging Face token is required for the private dataset")

    return text


@dataclass(frozen=True, slots=True)
class DownloadedHuggingFaceArtifact:
    """One verified artifact pair downloaded from one immutable revision."""

    revision: str
    artifact_path: Path
    manifest_path: Path
    artifact: RemoteProcessedArtifact

    def __post_init__(self) -> None:
        revision = _full_commit_sha(
            "revision",
            self.revision,
        )
        artifact_path = Path(self.artifact_path).expanduser().resolve()
        manifest_path = Path(self.manifest_path).expanduser().resolve()

        if not artifact_path.is_file():
            raise HuggingFaceRepositoryError(
                "Downloaded artifact path is not a regular file"
            )

        if not manifest_path.is_file():
            raise HuggingFaceRepositoryError(
                "Downloaded manifest path is not a regular file"
            )

        if not isinstance(
            self.artifact,
            (
                RemoteL2ProcessedArtifact,
                # RemotePriceProcessedArtifact is covered by the union at
                # runtime through the shared manifest/key validation below.
            ),
        ):
            from l2shock.remote.artifact_codec import (
                RemotePriceProcessedArtifact,
            )

            if not isinstance(
                self.artifact,
                RemotePriceProcessedArtifact,
            ):
                raise TypeError("artifact must be a verified remote processed artifact")

        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "artifact_path", artifact_path)
        object.__setattr__(self, "manifest_path", manifest_path)


@dataclass(frozen=True, slots=True)
class HuggingFacePublicationResult:
    """Result of one conflict-safe HF artifact publication."""

    revision: str
    artifact: RemoteProcessedArtifact
    created: bool
    concurrent_commit_observed: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "revision",
            _full_commit_sha(
                "revision",
                self.revision,
            ),
        )

        if not isinstance(self.created, bool):
            raise TypeError("created must be bool")

        if not isinstance(self.concurrent_commit_observed, bool):
            raise TypeError("concurrent_commit_observed must be bool")


@dataclass(frozen=True, slots=True)
class HuggingFacePredecessorCheckpoint:
    """Verified predecessor artifact state for one L2 target hour."""

    revision: str
    predecessor: DownloadedHuggingFaceArtifact
    checkpoint_bytes: bytes | None

    def __post_init__(self) -> None:
        revision = _full_commit_sha(
            "revision",
            self.revision,
        )

        if not isinstance(
            self.predecessor,
            DownloadedHuggingFaceArtifact,
        ):
            raise TypeError("predecessor must be DownloadedHuggingFaceArtifact")

        if self.predecessor.revision != revision:
            raise HuggingFaceRepositoryError(
                "Predecessor artifact revision does not match "
                "predecessor-checkpoint revision ownership"
            )

        if not isinstance(
            self.predecessor.artifact,
            RemoteL2ProcessedArtifact,
        ):
            raise TypeError("predecessor must contain a verified remote L2 artifact")

        value = self.checkpoint_bytes

        if value is not None:
            if not isinstance(value, bytes) or not value:
                raise HuggingFaceRepositoryError(
                    "checkpoint_bytes must be non-empty bytes or null"
                )

        object.__setattr__(
            self,
            "revision",
            revision,
        )


class HuggingFaceDatasetRepository:
    """Conflict-safe storage adapter for one private HF dataset repository."""

    def __init__(
        self,
        *,
        repo_id: str,
        token: SecretStr | str,
        revision: str = "main",
        api: _HfApiProtocol | None = None,
        download_function: HfDownloadFunction = hf_hub_download,
    ) -> None:
        if api is not None and not isinstance(api, HfApi):
            # Test doubles are accepted when they implement the required
            # methods. Avoid runtime_checkable Protocol overhead here.
            if not callable(getattr(api, "repo_info", None)) or not callable(
                getattr(api, "create_commit", None)
            ):
                raise TypeError("api must provide repo_info() and create_commit()")

        if not callable(download_function):
            raise TypeError("download_function must be callable")

        self._repo_id = _repository_id(repo_id)
        self._revision = _branch_name(revision)
        self._token = _token_text(token)
        self._api: _HfApiProtocol = api if api is not None else HfApi()
        self._download_function = download_function

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"repo_id={self._repo_id!r}, "
            f"revision={self._revision!r}, "
            "token=SecretStr('**********'))"
        )

    @property
    def repo_id(self) -> str:
        return self._repo_id

    @property
    def revision(self) -> str:
        return self._revision

    def current_revision(self) -> str:
        """Resolve the configured dataset branch to one immutable full SHA."""

        try:
            info = self._api.repo_info(
                repo_id=self._repo_id,
                repo_type="dataset",
                revision=self._revision,
                token=self._token,
            )
        except RepositoryNotFoundError as exc:
            raise HuggingFaceRepositoryUnavailableError(
                "The configured Hugging Face dataset is missing or inaccessible"
            ) from exc
        except HfHubHTTPError as exc:
            raise HuggingFaceRepositoryError(
                "Could not inspect the Hugging Face dataset revision"
            ) from exc
        except ValueError as exc:
            raise HuggingFaceRepositoryError(
                "The Hugging Face repository configuration is invalid"
            ) from exc

        return _full_commit_sha(
            "dataset revision",
            info.sha,
        )

    def _download_optional(
        self,
        filename: str,
        *,
        revision: str,
    ) -> Path | None:
        try:
            path = self._download_function(
                repo_id=self._repo_id,
                filename=filename,
                repo_type="dataset",
                revision=revision,
                token=self._token,
            )
        except EntryNotFoundError:
            return None
        except RepositoryNotFoundError as exc:
            raise HuggingFaceRepositoryUnavailableError(
                "The configured Hugging Face dataset is missing or inaccessible"
            ) from exc
        except HfHubHTTPError as exc:
            raise HuggingFaceRepositoryError(
                "Could not download a file from the Hugging Face dataset"
            ) from exc
        except ValueError as exc:
            raise HuggingFaceRepositoryError(
                "The Hugging Face download request is invalid"
            ) from exc

        result = Path(path).expanduser().resolve()

        if result.is_symlink():
            # The HF cache may internally use links depending on platform and
            # configuration. Resolve first, then require the resolved target
            # to be a regular file.
            result = result.resolve()

        if not result.is_file():
            raise HuggingFaceRepositoryError(
                "Hugging Face download did not return a regular file"
            )

        return result

    def download_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> DownloadedHuggingFaceArtifact | None:
        """Download and verify one artifact pair from one exact commit SHA."""

        if not isinstance(key, RemoteArtifactKey):
            raise TypeError("key must be a RemoteArtifactKey")

        selected_revision = (
            self.current_revision()
            if revision is None
            else _full_commit_sha(
                "revision",
                revision,
            )
        )

        artifact_path = self._download_optional(
            key.relative_path,
            revision=selected_revision,
        )
        manifest_path = self._download_optional(
            key.manifest_relative_path,
            revision=selected_revision,
        )

        if artifact_path is None and manifest_path is None:
            return None

        if artifact_path is None or manifest_path is None:
            raise HuggingFacePartialArtifactError(
                "Hugging Face contains an incomplete artifact/manifest pair"
            )

        try:
            artifact = read_remote_artifact_file(
                artifact_path,
                expected_key=key,
                external_manifest_bytes=manifest_path.read_bytes(),
            )
        except Exception as exc:
            raise HuggingFaceRepositoryError(
                "Downloaded Hugging Face artifact pair failed verification"
            ) from exc

        return DownloadedHuggingFaceArtifact(
            revision=selected_revision,
            artifact_path=artifact_path,
            manifest_path=manifest_path,
            artifact=artifact,
        )

    def require_artifact(
        self,
        key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> DownloadedHuggingFaceArtifact:
        result = self.download_artifact(
            key,
            revision=revision,
        )

        if result is None:
            raise HuggingFaceArtifactNotFoundError(
                "The required processed artifact does not exist"
            )

        return result

    def download_l2_predecessor_checkpoint(
        self,
        target_key: RemoteArtifactKey,
        *,
        revision: str | None = None,
    ) -> HuggingFacePredecessorCheckpoint:
        """Load the immediately preceding L2 artifact and its checkpoint."""

        if not isinstance(target_key, RemoteArtifactKey):
            raise TypeError("target_key must be a RemoteArtifactKey")

        if target_key.kind is not RemoteArtifactKind.L2:
            raise ValueError(
                "Predecessor checkpoints are available only for L2 artifacts"
            )

        predecessor_key = RemoteArtifactKey(
            kind=RemoteArtifactKind.L2,
            provider=target_key.provider,
            venue=target_key.venue,
            instrument=target_key.instrument,
            hour_utc=target_key.hour_utc - timedelta(hours=1),
            preset_hash=target_key.preset_hash,
            schema_version=target_key.schema_version,
        )

        predecessor = self.require_artifact(
            predecessor_key,
            revision=revision,
        )

        if not isinstance(
            predecessor.artifact,
            RemoteL2ProcessedArtifact,
        ):
            raise HuggingFaceRepositoryError(
                "Predecessor key resolved to a non-L2 artifact"
            )

        return HuggingFacePredecessorCheckpoint(
            revision=predecessor.revision,
            predecessor=predecessor,
            checkpoint_bytes=predecessor.artifact.output_checkpoint,
        )

    def _existing_matches(
        self,
        artifact: RemoteProcessedArtifact,
        *,
        revision: str,
    ) -> DownloadedHuggingFaceArtifact | None:
        existing = self.download_artifact(
            artifact.manifest.key,
            revision=revision,
        )

        if existing is None:
            return None

        if existing.artifact != artifact:
            raise HuggingFaceArtifactConflictError(
                "The immutable Hugging Face artifact path already owns "
                "different content"
            )

        return existing

    def publish_artifact(
        self,
        artifact: RemoteProcessedArtifact,
        *,
        maximum_attempts: int = 8,
    ) -> HuggingFacePublicationResult:
        """Publish artifact and manifest with optimistic branch concurrency."""

        from l2shock.remote.artifact_codec import (
            RemotePriceProcessedArtifact,
        )

        if not isinstance(
            artifact,
            (
                RemoteL2ProcessedArtifact,
                RemotePriceProcessedArtifact,
            ),
        ):
            raise TypeError("artifact must be a remote processed artifact")

        if (
            isinstance(maximum_attempts, bool)
            or not isinstance(maximum_attempts, int)
            or maximum_attempts <= 0
        ):
            raise ValueError("maximum_attempts must be a positive integer")

        concurrent_commit_observed = False

        for attempt in range(1, maximum_attempts + 1):
            expected_parent = self.current_revision()

            existing = self._existing_matches(
                artifact,
                revision=expected_parent,
            )

            if existing is not None:
                return HuggingFacePublicationResult(
                    revision=expected_parent,
                    artifact=artifact,
                    created=False,
                    concurrent_commit_observed=concurrent_commit_observed,
                )

            key = artifact.manifest.key

            with TemporaryDirectory(
                prefix="l2shock-hf-publication-",
            ) as directory:
                temporary_root = Path(directory)
                local_artifact = temporary_root / "artifact.parquet"

                write_remote_artifact_file(
                    local_artifact,
                    artifact,
                    overwrite=False,
                )

                operations = [
                    CommitOperationAdd(
                        path_in_repo=key.relative_path,
                        path_or_fileobj=local_artifact,
                    ),
                    CommitOperationAdd(
                        path_in_repo=key.manifest_relative_path,
                        path_or_fileobj=(artifact.manifest.canonical_json_bytes),
                    ),
                ]

                try:
                    commit_info = self._api.create_commit(
                        repo_id=self._repo_id,
                        repo_type="dataset",
                        revision=self._revision,
                        parent_commit=expected_parent,
                        create_pr=False,
                        operations=operations,
                        commit_message=(
                            "Publish "
                            f"{key.kind.value} "
                            f"{key.venue}/{key.instrument} "
                            f"{key.hour_utc.isoformat()}"
                        ),
                        token=self._token,
                    )
                except RepositoryNotFoundError as exc:
                    raise HuggingFaceRepositoryUnavailableError(
                        "The configured Hugging Face dataset is missing "
                        "or inaccessible"
                    ) from exc
                except ValueError as exc:
                    raise HuggingFacePublicationError(
                        "Hugging Face rejected the publication request"
                    ) from exc
                except HfHubHTTPError as exc:
                    # Hugging Face may reject publication for three distinct
                    # retryable reasons:
                    #
                    # 1. the observed parent commit became stale;
                    # 2. another commit operation currently owns the repo;
                    # 3. the repository commit-rate limit was exhausted.
                    #
                    # A 409 does not always advance the branch head. Another
                    # commit can still be in progress while repo_info reports
                    # the same visible head. A 429 likewise leaves the branch
                    # unchanged and must honor Retry-After rather than being
                    # misclassified as a permanent publication failure.
                    status_code = _http_status_code(exc)
                    retry_after = _retry_after_seconds(exc)
                    latest_revision = self.current_revision()

                    if latest_revision != expected_parent:
                        concurrent_commit_observed = True

                        concurrent_existing = self._existing_matches(
                            artifact,
                            revision=latest_revision,
                        )

                        if concurrent_existing is not None:
                            return HuggingFacePublicationResult(
                                revision=latest_revision,
                                artifact=artifact,
                                created=False,
                                concurrent_commit_observed=True,
                            )

                    retryable_conflict = status_code == 409
                    retryable_rate_limit = status_code == 429

                    if not (
                        retryable_conflict
                        or retryable_rate_limit
                        or latest_revision != expected_parent
                    ):
                        raise HuggingFacePublicationError(
                            "Hugging Face publication failed without a "
                            "retryable conflict, rate limit, or branch-head "
                            "change"
                        ) from exc

                    if attempt >= maximum_attempts:
                        if retryable_rate_limit:
                            reason = "repository commit-rate limits"
                        elif retryable_conflict:
                            reason = "concurrent commit conflicts"
                        else:
                            reason = "concurrent branch changes"

                        raise HuggingFacePublicationError(
                            "Hugging Face publication failed after "
                            f"{maximum_attempts} attempts due to {reason}"
                        ) from exc

                    if retryable_conflict:
                        concurrent_commit_observed = True

                    delay = _publication_retry_delay(
                        attempt=attempt,
                        rate_limited=retryable_rate_limit,
                        retry_after_seconds=retry_after,
                    )
                    time.sleep(delay)

                    # The next loop iteration resolves the branch again and
                    # never blindly reuses the previous parent commit.
                    continue

            raw_commit_revision = getattr(
                commit_info,
                "oid",
                None,
            )

            if raw_commit_revision is None:
                committed_revision = self.current_revision()
            else:
                committed_revision = _full_commit_sha(
                    "published commit revision",
                    raw_commit_revision,
                )

            verified = self.require_artifact(
                key,
                revision=committed_revision,
            )

            if verified.artifact != artifact:
                raise HuggingFacePublicationError(
                    "Published Hugging Face artifact differs from the "
                    "requested immutable content"
                )

            return HuggingFacePublicationResult(
                revision=committed_revision,
                artifact=artifact,
                created=True,
                concurrent_commit_observed=concurrent_commit_observed,
            )

        raise HuggingFacePublicationError(
            "Hugging Face publication exhausted its bounded attempts"
        )


__all__ = [
    "DownloadedHuggingFaceArtifact",
    "HuggingFaceArtifactConflictError",
    "HuggingFaceArtifactNotFoundError",
    "HuggingFaceDatasetRepository",
    "HuggingFacePartialArtifactError",
    "HuggingFacePredecessorCheckpoint",
    "HuggingFacePublicationError",
    "HuggingFacePublicationResult",
    "HuggingFaceRepositoryError",
    "HuggingFaceRepositoryUnavailableError",
]

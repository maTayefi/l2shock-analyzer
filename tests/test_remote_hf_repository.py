# tests/test_remote_hf_repository.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from huggingface_hub.utils import (
    EntryNotFoundError,
    HfHubHTTPError,
)

from l2shock.acquisition import SourceDataKind
from l2shock.ingest import TradeRecord
from l2shock.price import (
    build_trade_ohlc_hour,
    encode_hourly_trade_ohlc_block,
)
from l2shock.remote import (
    HuggingFaceArtifactConflictError,
    HuggingFaceDatasetRepository,
    HuggingFacePartialArtifactError,
    HuggingFaceRepositoryError,
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemotePriceProcessedArtifact,
    RemoteSourceHourReference,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        14,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _epoch_ms(value: datetime) -> int:
    epoch = datetime(
        1970,
        1,
        1,
        tzinfo=timezone.utc,
    )
    delta = value - epoch

    return (
        delta.days * 86_400 * 1_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


def _price_artifact(
    *,
    price: str = "100.25",
) -> RemotePriceProcessedArtifact:
    trade_time_ms = _epoch_ms(_hour() + timedelta(milliseconds=100))

    block = build_trade_ohlc_hour(
        (
            TradeRecord(
                symbol="BTCUSDT",
                trade_id="hf-repository-test",
                price=Decimal(price),
                quantity=Decimal("1"),
                received_time_ns=trade_time_ms * 1_000_000,
                event_time_ms=trade_time_ms,
                trade_time_ms=trade_time_ms,
                is_buyer_maker=False,
                order_type="market",
                row_number=0,
            ),
        ),
        base="BTC",
        hour_utc=_hour(),
    )
    encoded = encode_hourly_trade_ohlc_block(block)

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        hour_utc=_hour(),
    )
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(
            RemoteSourceHourReference(
                provider="cryptohftdata",
                venue="binance_futures",
                instrument="BTCUSDT",
                data_kind=SourceDataKind.TRADES,
                hour_utc=_hour(),
                content_sha256="a" * 64,
            ),
        ),
        content_sha256=encoded.content_sha256,
        producer_git_commit="b" * 40,
    )

    return RemotePriceProcessedArtifact(
        manifest=manifest,
        encoded=encoded,
    )


def _operation_bytes(operation: Any) -> bytes:
    value = operation.path_or_fileobj

    if isinstance(value, bytes):
        return value

    if isinstance(value, (str, Path)):
        return Path(value).read_bytes()

    position = value.tell()
    value.seek(0)

    try:
        return value.read()
    finally:
        value.seek(position)


def _fake_http_response(
    *,
    status_code: int | None = None,
    headers: dict[str, str] | None = None,
) -> SimpleNamespace:
    """Minimal object satisfying HfHubHTTPError's response contract."""
    return SimpleNamespace(
        status_code=status_code,
        headers=dict(headers or {}),
        request=None,
    )


class FakeHfApi:
    def __init__(self) -> None:
        self.head = "1" * 40
        self.snapshots: dict[str, dict[str, bytes]] = {
            self.head: {},
        }
        self.repo_info_calls: list[dict[str, object]] = []
        self.create_commit_calls: list[dict[str, object]] = []
        self.advance_once_before_commit = False
        self.publish_operations_during_advance = False
        self.rate_limit_once_before_commit = False

    def repo_info(self, **kwargs):
        self.repo_info_calls.append(dict(kwargs))

        return SimpleNamespace(
            sha=self.head,
        )

    def create_commit(self, **kwargs):
        self.create_commit_calls.append(dict(kwargs))

        operations = kwargs["operations"]
        expected_parent = kwargs["parent_commit"]

        if self.rate_limit_once_before_commit:
            self.rate_limit_once_before_commit = False

            raise HfHubHTTPError(
                "simulated repository commit rate limit",
                response=_fake_http_response(
                    status_code=429,
                    headers={"Retry-After": "0"},
                ),
            )

        if self.advance_once_before_commit:
            self.advance_once_before_commit = False
            new_head = "2" * 40
            snapshot = dict(self.snapshots[self.head])

            if self.publish_operations_during_advance:
                for operation in operations:
                    snapshot[operation.path_in_repo] = _operation_bytes(operation)

            self.snapshots[new_head] = snapshot
            self.head = new_head

            raise HfHubHTTPError(
                "simulated stale parent",
                response=_fake_http_response(),
            )

        if expected_parent != self.head:
            raise HfHubHTTPError(
                "simulated parent mismatch",
                response=_fake_http_response(),
            )

        next_digit = str(
            min(
                9,
                int(self.head[0]) + 1,
            )
        )
        new_head = next_digit * 40
        snapshot = dict(self.snapshots[self.head])

        for operation in operations:
            snapshot[operation.path_in_repo] = _operation_bytes(operation)

        self.snapshots[new_head] = snapshot
        self.head = new_head

        return SimpleNamespace(
            oid=new_head,
        )


def _download_function(
    api: FakeHfApi,
    cache_root: Path,
    calls: list[dict[str, object]],
):
    def download(**kwargs) -> str:
        calls.append(dict(kwargs))

        revision = str(kwargs["revision"])
        filename = str(kwargs["filename"])
        snapshot = api.snapshots[revision]

        if filename not in snapshot:
            raise EntryNotFoundError(
                "simulated missing file",
            )

        destination = cache_root / revision
        destination = destination.joinpath(*filename.split("/"))
        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        destination.write_bytes(snapshot[filename])

        return str(destination)

    return download


def _repository(
    api: FakeHfApi,
    tmp_path: Path,
    calls: list[dict[str, object]],
) -> HuggingFaceDatasetRepository:
    return HuggingFaceDatasetRepository(
        repo_id="example/private-l2shock",
        token="hf_test_secret_value",
        revision="main",
        api=api,
        download_function=_download_function(
            api,
            tmp_path / "hf-cache",
            calls,
        ),
    )


def test_hf_publication_commits_artifact_and_manifest_atomically(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    download_calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        download_calls,
    )
    artifact = _price_artifact()

    result = repository.publish_artifact(artifact)

    assert result.created is True
    assert result.concurrent_commit_observed is False
    assert result.revision == api.head
    assert len(api.create_commit_calls) == 1

    call = api.create_commit_calls[0]

    assert call["repo_type"] == "dataset"
    assert call["revision"] == "main"
    assert call["parent_commit"] == "1" * 40
    assert call["create_pr"] is False

    operations = call["operations"]

    assert {operation.path_in_repo for operation in operations} == {
        artifact.manifest.key.relative_path,
        artifact.manifest.key.manifest_relative_path,
    }

    downloaded = repository.require_artifact(
        artifact.manifest.key,
        revision=result.revision,
    )

    assert downloaded.artifact == artifact

    assert download_calls
    assert all(call["repo_type"] == "dataset" for call in download_calls)
    assert all(call["revision"] == result.revision for call in download_calls[-2:])


def test_identical_hf_publication_is_idempotent(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        calls,
    )
    artifact = _price_artifact()

    first = repository.publish_artifact(artifact)
    second = repository.publish_artifact(artifact)

    assert first.created is True
    assert second.created is False
    assert second.revision == first.revision
    assert len(api.create_commit_calls) == 1


def test_stale_parent_retry_reads_new_head_before_retry(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    api.advance_once_before_commit = True

    calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        calls,
    )
    artifact = _price_artifact()

    result = repository.publish_artifact(
        artifact,
        maximum_attempts=3,
    )

    assert result.created is True
    assert result.concurrent_commit_observed is True
    assert len(api.create_commit_calls) == 2

    first_parent = api.create_commit_calls[0]["parent_commit"]
    second_parent = api.create_commit_calls[1]["parent_commit"]

    assert first_parent == "1" * 40
    assert second_parent == "2" * 40
    assert first_parent != second_parent


def test_concurrent_identical_publication_becomes_idempotent_success(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    api.advance_once_before_commit = True
    api.publish_operations_during_advance = True

    calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        calls,
    )
    artifact = _price_artifact()

    result = repository.publish_artifact(artifact)

    assert result.created is False
    assert result.concurrent_commit_observed is True
    assert result.revision == "2" * 40
    assert len(api.create_commit_calls) == 1


def test_conflicting_immutable_hf_artifact_is_rejected(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        calls,
    )

    first = _price_artifact(
        price="100.25",
    )
    conflicting = _price_artifact(
        price="101.25",
    )

    repository.publish_artifact(first)

    with pytest.raises(
        HuggingFaceArtifactConflictError,
        match="different content",
    ):
        repository.publish_artifact(conflicting)

    assert len(api.create_commit_calls) == 1


def test_hf_download_uses_one_pinned_dataset_revision(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        calls,
    )
    artifact = _price_artifact()

    publication = repository.publish_artifact(artifact)

    calls.clear()

    downloaded = repository.require_artifact(
        artifact.manifest.key,
        revision=publication.revision,
    )

    assert downloaded.artifact == artifact
    assert len(calls) == 2

    assert {call["filename"] for call in calls} == {
        artifact.manifest.key.relative_path,
        artifact.manifest.key.manifest_relative_path,
    }

    assert all(call["repo_type"] == "dataset" for call in calls)
    assert all(call["revision"] == publication.revision for call in calls)


def test_partial_hf_artifact_pair_is_rejected(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        calls,
    )
    artifact = _price_artifact()
    key = artifact.manifest.key

    api.snapshots[api.head][
        key.manifest_relative_path
    ] = artifact.manifest.canonical_json_bytes

    with pytest.raises(
        HuggingFacePartialArtifactError,
        match="incomplete",
    ):
        repository.download_artifact(
            key,
            revision=api.head,
        )


def test_hf_repository_repr_does_not_expose_token(
    tmp_path: Path,
) -> None:
    api = FakeHfApi()
    repository = HuggingFaceDatasetRepository(
        repo_id="example/private-l2shock",
        token="hf_test_secret_value",
        api=api,
        download_function=_download_function(
            api,
            tmp_path / "cache",
            [],
        ),
    )

    observed = repr(repository)

    assert "hf_test_secret_value" not in observed
    assert "**********" in observed


def test_hf_repository_has_no_database_or_ui_dependency() -> None:
    import ast
    import l2shock.remote.hf_repository as module

    path = Path(module.__file__)
    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
        ),
        filename=str(path),
    )

    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)

        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    assert not any(
        name == "l2shock.db" or name.startswith("l2shock.db.")
        for name in imported_modules
    )
    assert not any(
        name == "l2shock.ui" or name.startswith("l2shock.ui.")
        for name in imported_modules
    )
    assert "nicegui" not in imported_modules


def test_predecessor_checkpoint_revision_must_match_artifact_revision(
    tmp_path: Path,
) -> None:
    from l2shock.remote import (
        DownloadedHuggingFaceArtifact,
        HuggingFacePredecessorCheckpoint,
        RemoteL2ProcessedArtifact,
    )

    # This invariant is also exercised by normal predecessor downloads.
    # Constructing a complete L2 artifact here would duplicate the larger
    # transport fixtures, so verify the production source contains the guard.
    import inspect
    import l2shock.remote.hf_repository as module

    source = inspect.getsource(
        module.HuggingFacePredecessorCheckpoint.__post_init__,
    )

    assert "self.predecessor.revision != revision" in source

    del (
        DownloadedHuggingFaceArtifact,
        HuggingFacePredecessorCheckpoint,
        RemoteL2ProcessedArtifact,
        tmp_path,
    )


def test_hf_repository_error_does_not_include_token(
    tmp_path: Path,
) -> None:
    secret = "hf_test_secret_value"
    api = FakeHfApi()
    repository = _repository(
        api,
        tmp_path,
        [],
    )

    observed = repr(repository)

    assert secret not in observed

    error = HuggingFaceRepositoryError("Hugging Face repository operation failed")

    assert secret not in str(error)


def test_publication_retry_delay_honors_rate_limit_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.remote.hf_repository as module

    monkeypatch.setattr(
        module.random,
        "uniform",
        lambda _lower, _upper: 0.0,
    )

    assert (
        module._publication_retry_delay(
            attempt=1,
            rate_limited=True,
            retry_after_seconds=3_600.0,
        )
        == 3_600.0
    )


def test_publication_retry_delay_backs_off_for_commit_conflicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.remote.hf_repository as module

    monkeypatch.setattr(
        module.random,
        "uniform",
        lambda _lower, _upper: 0.0,
    )

    assert (
        module._publication_retry_delay(
            attempt=1,
            rate_limited=False,
            retry_after_seconds=None,
        )
        == 5.0
    )

    assert (
        module._publication_retry_delay(
            attempt=2,
            rate_limited=False,
            retry_after_seconds=None,
        )
        == 10.0
    )

    assert (
        module._publication_retry_delay(
            attempt=8,
            rate_limited=False,
            retry_after_seconds=None,
        )
        == 60.0
    )


def test_rate_limit_retry_does_not_require_branch_head_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.remote.hf_repository as module

    api = FakeHfApi()
    api.rate_limit_once_before_commit = True

    calls: list[dict[str, object]] = []
    repository = _repository(
        api,
        tmp_path,
        calls,
    )
    artifact = _price_artifact()

    monkeypatch.setattr(
        module.time,
        "sleep",
        lambda _seconds: None,
    )
    monkeypatch.setattr(
        module.random,
        "uniform",
        lambda _lower, _upper: 0.0,
    )

    result = repository.publish_artifact(
        artifact,
        maximum_attempts=3,
    )

    assert result.created is True
    assert len(api.create_commit_calls) == 2
    assert api.create_commit_calls[0]["parent_commit"] == "1" * 40
    assert api.create_commit_calls[1]["parent_commit"] == "1" * 40


def test_publication_retry_delay_floors_zero_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.remote.hf_repository as module

    monkeypatch.setattr(
        module.random,
        "uniform",
        lambda _lower, _upper: 0.0,
    )

    assert (
        module._publication_retry_delay(
            attempt=1,
            rate_limited=True,
            retry_after_seconds=0.0,
        )
        == module._MINIMUM_CONFLICT_RETRY_SECONDS
    )

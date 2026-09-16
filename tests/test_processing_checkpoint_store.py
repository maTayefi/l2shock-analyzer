from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.persistence import SourceHourStatus
from l2shock.ingest.parquet_reader import BookSide
from l2shock.ingest.replay import (
    CheckpointLevel,
    OrderBookCheckpoint,
)
from l2shock.processing import (
    CheckpointConflictError,
    CheckpointIdentity,
    CheckpointSearchStopReason,
    CheckpointStore,
    CheckpointStoreError,
    ProcessingCancelledError,
    ProcessingContractError,
    ProcessingRequest,
    ProcessingSourceArchive,
    SingleMarketL2ProcessingCoordinator,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _spec(offset: int = 0) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(offset),
    )


def _checkpoint(
    offset: int = 0,
    *,
    last_update_id: int = 100,
    bid_quantity: str = "2",
    source_content_sha256: str = "a" * 64,
) -> OrderBookCheckpoint:
    return OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=_hour(offset),
        last_update_id=last_update_id,
        levels=(
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("59999"),
                quantity=Decimal(bid_quantity),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("60001"),
                quantity=Decimal("1.5"),
                order_count=None,
            ),
        ),
        source_content_sha256=source_content_sha256,
    )


def _archive(
    tmp_path: Path,
    offset: int,
) -> ProcessingSourceArchive:
    spec = _spec(offset)
    path = tmp_path / f"hour-{offset}.parquet"
    path.write_bytes(f"source-{offset}".encode("ascii"))

    return ProcessingSourceArchive(
        spec=spec,
        local_path=path,
        content_sha256=f"{offset % 10}" * 64,
        file_size_bytes=path.stat().st_size,
        status=SourceHourStatus.DOWNLOADED,
    )


class _FakeSourceRepository:
    def __init__(
        self,
        archives: tuple[ProcessingSourceArchive, ...],
        durable_digests: dict[tuple[object, ...], str] | None = None,
    ) -> None:
        self._archives = {archive.spec.identity_tuple: archive for archive in archives}
        self._durable_digests = durable_digests or {
            archive.spec.identity_tuple: archive.content_sha256 for archive in archives
        }

    def find_durable_content_sha256(
        self,
        spec: SourceFileSpec,
    ) -> str | None:
        return self._durable_digests.get(spec.identity_tuple)

    def find_replayable(
        self,
        spec: SourceFileSpec,
    ) -> ProcessingSourceArchive | None:
        return self._archives.get(spec.identity_tuple)


def test_checkpoint_store_uses_locked_content_addressed_layout(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    checkpoint = _checkpoint()

    artifact = store.publish(checkpoint)

    assert (
        artifact.path
        == (
            tmp_path
            / "cache"
            / "checkpoints"
            / "cryptohftdata"
            / "binance_futures"
            / "BTCUSDT"
            / "2026-09-02"
            / "12"
            / (
                "checkpoint-v1-"
                f"{artifact.encoding_info.content_sha256}"
                ".l2checkpoint"
            )
        ).resolve()
    )

    assert artifact.path.is_file()
    assert artifact.checkpoint == checkpoint


def test_checkpoint_publication_is_idempotent(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    checkpoint = _checkpoint()

    first = store.publish(checkpoint)
    second = store.publish(checkpoint)

    assert second == first
    assert tuple(first.path.parent.iterdir()) == (first.path,)


def test_checkpoint_store_rejects_conflicting_content_for_same_hour(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")

    store.publish(
        _checkpoint(
            last_update_id=100,
            bid_quantity="2",
        )
    )

    with pytest.raises(
        CheckpointConflictError,
        match="different checkpoint",
    ):
        store.publish(
            _checkpoint(
                last_update_id=101,
                bid_quantity="3",
            )
        )


def test_checkpoint_store_detects_corruption(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    artifact = store.publish(_checkpoint())

    encoded = bytearray(artifact.path.read_bytes())
    encoded[-1] ^= 1
    artifact.path.write_bytes(encoded)

    with pytest.raises(
        CheckpointStoreError,
        match="validate checkpoint artifact",
    ):
        store.find_exact(artifact.identity)


def test_exact_checkpoint_lookup_returns_none_when_absent(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")

    result = store.find_exact(
        CheckpointIdentity(
            provider="cryptohftdata",
            venue="binance_futures",
            instrument="BTCUSDT",
            through_hour_utc=_hour(),
        )
    )

    assert result is None


def test_search_plan_stops_at_immediately_preceding_checkpoint(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    # Hour-0 archive has content_sha256 "0"*64; checkpoint must match.
    checkpoint = store.publish(_checkpoint(0, source_content_sha256="0" * 64))
    target_archive = _archive(tmp_path, 1)
    predecessor_archive = _archive(tmp_path, 0)
    repository = _FakeSourceRepository((target_archive, predecessor_archive))
    plan = store.build_search_plan(
        _spec(1),
        repository,
        max_checkpoint_search_hours=24,
    )

    assert plan.stop_reason is CheckpointSearchStopReason.CHECKPOINT_FOUND
    assert plan.checkpoint == checkpoint
    assert plan.replay_sources == (target_archive,)
    assert plan.inspected_checkpoint_hours == (_hour(0),)
    assert plan.initialized_by_checkpoint is True


def test_search_plan_builds_oldest_to_newest_contiguous_chain(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    # Hour-(-2) archive has content_sha256 "8"*64; checkpoint must match.
    checkpoint = store.publish(_checkpoint(-2, source_content_sha256="8" * 64))
    archives = (
        _archive(tmp_path, -2),
        _archive(tmp_path, -1),
        _archive(tmp_path, 0),
        _archive(tmp_path, 1),
    )
    repository = _FakeSourceRepository(archives)
    plan = store.build_search_plan(
        _spec(1),
        repository,
        max_checkpoint_search_hours=8,
    )

    assert plan.stop_reason is CheckpointSearchStopReason.CHECKPOINT_FOUND
    assert plan.checkpoint == checkpoint
    assert tuple(archive.spec.hour_utc for archive in plan.replay_sources) == (
        _hour(-1),
        _hour(0),
        _hour(1),
    )
    assert plan.inspected_checkpoint_hours == (
        _hour(0),
        _hour(-1),
        _hour(-2),
    )


def test_search_plan_reports_source_gap_without_crossing_it(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")

    archives = (
        _archive(tmp_path, 0),
        _archive(tmp_path, 1),
    )
    repository = _FakeSourceRepository(archives)

    plan = store.build_search_plan(
        _spec(1),
        repository,
        max_checkpoint_search_hours=8,
    )

    assert plan.stop_reason is CheckpointSearchStopReason.SOURCE_GAP
    assert plan.checkpoint is None
    assert plan.missing_source_hour_utc == _hour(-1)
    assert tuple(archive.spec.hour_utc for archive in plan.replay_sources) == (
        _hour(0),
        _hour(1),
    )


def test_search_plan_stops_at_configured_bound(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")

    archives = tuple(_archive(tmp_path, offset) for offset in (-2, -1, 0, 1))
    repository = _FakeSourceRepository(archives)

    plan = store.build_search_plan(
        _spec(1),
        repository,
        max_checkpoint_search_hours=2,
    )

    assert plan.stop_reason is CheckpointSearchStopReason.SEARCH_BOUND_REACHED
    assert plan.checkpoint is None
    assert plan.inspected_checkpoint_hours == (
        _hour(0),
        _hour(-1),
    )
    assert tuple(archive.spec.hour_utc for archive in plan.replay_sources) == (
        _hour(-1),
        _hour(0),
        _hour(1),
    )


def test_search_plan_honors_cooperative_cancellation(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    repository = _FakeSourceRepository((_archive(tmp_path, 1),))

    with pytest.raises(
        ProcessingCancelledError,
        match="cancellation",
    ):
        store.build_search_plan(
            _spec(1),
            repository,
            max_checkpoint_search_hours=24,
            cancellation_probe=lambda: True,
        )


def test_checkpoint_path_rejects_uppercase_digest(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    identity = CheckpointIdentity(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        through_hour_utc=_hour(),
    )

    with pytest.raises(
        ProcessingContractError,
        match="canonical lowercase",
    ):
        store.path_for(
            identity,
            "A" * 64,
        )


def test_checkpoint_store_rejects_noncanonical_checkpoint_filename(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    identity = CheckpointIdentity(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument="BTCUSDT",
        through_hour_utc=_hour(),
    )

    directory = store.directory_for(identity)
    directory.mkdir(parents=True)
    bad_path = directory / "manually-copied.l2checkpoint"
    bad_path.write_bytes(b"not a canonical checkpoint")

    with pytest.raises(
        CheckpointStoreError,
        match="noncanonical",
    ):
        store.find_exact(identity)


def test_checkpoint_search_rejects_source_digest_mismatch(
    tmp_path: Path,
) -> None:
    """Checkpoint derived from different source bytes must be rejected."""
    store = CheckpointStore(tmp_path / "cache")
    # Publish checkpoint claiming source digest "a"*64 …
    store.publish(_checkpoint(0, source_content_sha256="a" * 64))
    # … but the hour-0 archive actually has digest "0"*64.
    target_archive = _archive(tmp_path, 1)
    predecessor_archive = _archive(tmp_path, 0)
    repository = _FakeSourceRepository(
        (target_archive, predecessor_archive),
    )

    with pytest.raises(
        CheckpointStoreError,
        match="does not match durable",
    ):
        store.build_search_plan(
            _spec(1),
            repository,
            max_checkpoint_search_hours=24,
        )


def test_checkpoint_search_accepts_digest_for_pruned_source(
    tmp_path: Path,
) -> None:
    """A pruned source (no local bytes) still protects its checkpoint
    when the durable digest is known."""
    store = CheckpointStore(tmp_path / "cache")
    published = store.publish(_checkpoint(0, source_content_sha256="a" * 64))
    target_archive = _archive(tmp_path, 1)

    # No archive for hour 0 (simulating pruned raw), but the durable
    # digest is still recorded.
    repository = _FakeSourceRepository(
        (target_archive,),
        durable_digests={
            target_archive.spec.identity_tuple: (target_archive.content_sha256),
            _spec(0).identity_tuple: "a" * 64,
        },
    )

    plan = store.build_search_plan(
        _spec(1),
        repository,
        max_checkpoint_search_hours=24,
    )

    assert plan.stop_reason is CheckpointSearchStopReason.CHECKPOINT_FOUND
    assert plan.checkpoint == published
    assert plan.replay_sources == (target_archive,)


def test_exact_predecessor_terminal_cache_builds_target_only_plan(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    coordinator = SingleMarketL2ProcessingCoordinator(
        checkpoint_store=store,
    )

    predecessor = _archive(tmp_path, 0)
    target = _archive(tmp_path, 1)
    repository = _FakeSourceRepository(
        (
            predecessor,
            target,
        )
    )

    coordinator._terminal_state_cache[
        coordinator._terminal_state_key(predecessor.spec)
    ] = None

    plan = coordinator._cached_search_plan(
        target.spec,
        repository,
    )

    assert plan is not None
    assert plan.target == target.spec
    assert plan.replay_sources == (target,)
    assert plan.checkpoint is None
    assert plan.stop_reason is (CheckpointSearchStopReason.PREDECESSOR_UNINITIALIZED)


def test_terminal_cache_is_not_reused_across_nonadjacent_hour(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    coordinator = SingleMarketL2ProcessingCoordinator(
        checkpoint_store=store,
    )

    first = _archive(tmp_path, 0)
    nonadjacent_target = _archive(tmp_path, 2)
    repository = _FakeSourceRepository(
        (
            first,
            nonadjacent_target,
        )
    )

    coordinator._terminal_state_cache[coordinator._terminal_state_key(first.spec)] = (
        None
    )

    assert (
        coordinator._cached_search_plan(
            nonadjacent_target.spec,
            repository,
        )
        is None
    )


def test_old_checkpoint_is_replayed_through_intermediate_predecessors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.processing.l2_coordinator as coordinator_module

    store = CheckpointStore(tmp_path / "cache")

    store.publish(
        _checkpoint(
            -2,
            source_content_sha256="8" * 64,
        )
    )

    archives = tuple(
        _archive(
            tmp_path,
            offset,
        )
        for offset in (
            -2,
            -1,
            0,
            1,
        )
    )
    repository = _FakeSourceRepository(archives)

    plan = store.build_search_plan(
        _spec(1),
        repository,
        max_checkpoint_search_hours=8,
    )

    assert plan.checkpoint is not None
    assert tuple(archive.spec.hour_utc for archive in plan.replay_sources) == (
        _hour(-1),
        _hour(0),
        _hour(1),
    )

    expected_terminal = _checkpoint(
        0,
        source_content_sha256="0" * 64,
    )
    observed_hours: list[datetime] = []

    def fake_replay(
        sources,
        **_kwargs,
    ):
        observed_hours.extend(spec.hour_utc for _path, spec in sources)
        return SimpleNamespace(
            final_checkpoint=expected_terminal,
        )

    monkeypatch.setattr(
        coordinator_module,
        "replay_orderbook_archives",
        fake_replay,
    )

    coordinator = SingleMarketL2ProcessingCoordinator(
        checkpoint_store=store,
    )

    result = coordinator._target_initial_checkpoint(
        ProcessingRequest(
            operation_id=uuid4(),
            target=_spec(1),
            max_checkpoint_search_hours=8,
        ),
        plan,
        cancellation_probe=None,
    )

    assert observed_hours == [
        _hour(-1),
        _hour(0),
    ]
    assert result == expected_terminal

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from l2shock.ingest import (
    BookSide,
    CheckpointLevel,
    OrderBookCheckpoint,
    encode_checkpoint,
)
from l2shock.ingest.checkpoint_codec import checkpoint_encoding_info
from l2shock.processing.checkpoint_store import (
    CheckpointIdentity,
    CheckpointStore,
)
from l2shock.processing.errors import (
    CheckpointConflictError,
    CheckpointStoreError,
)


def _checkpoint() -> OrderBookCheckpoint:
    return OrderBookCheckpoint(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        through_hour_utc=datetime(2094, 1, 1, 12, tzinfo=timezone.utc),
        last_update_id=100,
        levels=(
            CheckpointLevel(
                side=BookSide.BID,
                price=Decimal("100"),
                quantity=Decimal("2"),
                order_count=None,
            ),
            CheckpointLevel(
                side=BookSide.ASK,
                price=Decimal("101"),
                quantity=Decimal("3"),
                order_count=None,
            ),
        ),
        source_content_sha256="a" * 64,
    )


def _symlink(
    link: Path,
    target: Path,
    *,
    directory: bool,
) -> None:
    try:
        link.symlink_to(target, target_is_directory=directory)
    except OSError, NotImplementedError:
        pytest.skip("Symbolic links are unavailable on this platform")


def test_checkpoint_discovery_does_not_create_absent_directories(
    tmp_path: Path,
) -> None:
    root = tmp_path / "absent-cache"
    store = CheckpointStore(root)
    identity = CheckpointIdentity.from_checkpoint(_checkpoint())

    assert store.find_exact(identity) is None
    assert not root.exists()


@pytest.mark.parametrize("location", ("cache", "checkpoints"))
def test_checkpoint_store_constructor_rejects_root_alias(
    tmp_path: Path,
    location: str,
) -> None:
    cache = tmp_path / "cache"
    target = tmp_path / "external"
    target.mkdir()
    sentinel = target / "sentinel"
    sentinel.write_bytes(b"unchanged")

    if location == "cache":
        link = cache
    else:
        cache.mkdir()
        link = cache / "checkpoints"

    _symlink(link, target, directory=True)

    with pytest.raises(CheckpointStoreError):
        CheckpointStore(cache)

    assert link.is_symlink()
    assert sentinel.read_bytes() == b"unchanged"
    assert sorted(path.name for path in target.iterdir()) == ["sentinel"]


@pytest.mark.parametrize("inside", (False, True))
def test_identity_directory_alias_is_rejected_for_read_and_publish(
    tmp_path: Path,
    inside: bool,
) -> None:
    cache = tmp_path / "cache"
    store = CheckpointStore(cache)
    checkpoint = _checkpoint()
    identity = CheckpointIdentity.from_checkpoint(checkpoint)

    store.root.mkdir(parents=True)
    target = store.root / "different-owner" if inside else tmp_path / "external-owner"
    target.mkdir()
    sentinel = target / "sentinel"
    sentinel.write_bytes(b"unchanged")

    link = store.root / identity.provider
    _symlink(link, target, directory=True)

    with pytest.raises(CheckpointStoreError):
        store.directory_for(identity)

    with pytest.raises(CheckpointStoreError):
        store.find_exact(identity)

    with pytest.raises(CheckpointStoreError):
        store.publish(checkpoint)

    assert link.is_symlink()
    assert sentinel.read_bytes() == b"unchanged"
    assert sorted(path.name for path in target.iterdir()) == ["sentinel"]


@pytest.mark.parametrize("inside", (False, True))
@pytest.mark.parametrize("dangling", (False, True))
def test_canonical_checkpoint_leaf_alias_is_rejected(
    tmp_path: Path,
    inside: bool,
    dangling: bool,
) -> None:
    cache = tmp_path / "cache"
    store = CheckpointStore(cache)
    checkpoint = _checkpoint()
    identity = CheckpointIdentity.from_checkpoint(checkpoint)
    encoded = encode_checkpoint(checkpoint)
    digest = checkpoint_encoding_info(encoded).content_sha256

    destination = store.path_for(identity, digest)
    destination.parent.mkdir(parents=True)

    target_root = cache if inside else tmp_path / "external"
    target_root.mkdir(parents=True, exist_ok=True)
    target = target_root / "matching-checkpoint-bytes"

    if not dangling:
        target.write_bytes(encoded)

    _symlink(destination, target, directory=False)

    with pytest.raises(CheckpointStoreError):
        store.find_exact(identity)

    with pytest.raises(CheckpointStoreError):
        store._load_artifact(destination, expected_identity=identity)

    with pytest.raises(CheckpointStoreError):
        store.publish(checkpoint)

    assert destination.is_symlink()

    if dangling:
        assert not target.exists()
    else:
        assert target.read_bytes() == encoded


def test_existing_store_rechecks_root_ownership_after_construction(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "cache"
    store = CheckpointStore(cache)
    checkpoint = _checkpoint()
    identity = CheckpointIdentity.from_checkpoint(checkpoint)

    cache.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "sentinel"
    sentinel.write_bytes(b"unchanged")
    _symlink(cache / "checkpoints", external, directory=True)

    with pytest.raises(CheckpointStoreError):
        store.find_exact(identity)

    with pytest.raises(CheckpointStoreError):
        store.publish(checkpoint)

    assert sentinel.read_bytes() == b"unchanged"


def test_regular_checkpoint_publication_remains_idempotent_and_immutable(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "cache")
    checkpoint = _checkpoint()
    identity = CheckpointIdentity.from_checkpoint(checkpoint)

    first = store.publish(checkpoint)
    before = first.path.read_bytes()
    second = store.publish(checkpoint)
    found = store.find_exact(identity)

    assert second == first
    assert found == first
    assert first.path == store.path_for(
        identity,
        first.encoding_info.content_sha256,
    )
    assert not first.path.is_symlink()

    with pytest.raises(CheckpointConflictError):
        store.publish(replace(checkpoint, last_update_id=101))

    assert first.path.read_bytes() == before
    assert len(list(first.path.parent.glob("*.l2checkpoint"))) == 1
    assert not list(first.path.parent.glob("*.tmp"))


def test_regular_file_in_directory_position_is_rejected(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "checkpoints").write_bytes(b"not-a-directory")

    with pytest.raises(CheckpointStoreError):
        CheckpointStore(cache)

    assert (cache / "checkpoints").read_bytes() == b"not-a-directory"

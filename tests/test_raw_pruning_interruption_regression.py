from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import l2shock.maintenance_actions as maintenance
from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.acquisition.persistence import SourceHourStatus


class _PruningInterrupted(BaseException):
    pass


@pytest.mark.parametrize(
    "interruption_type",
    (_PruningInterrupted, KeyboardInterrupt, SystemExit),
)
def test_raw_pruning_interruption_restores_entire_renamed_batch(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    interruption_type,
) -> None:
    raw_root = tmp_path / "raw"
    start = datetime(2026, 9, 14, 0, tzinfo=timezone.utc)
    specs = tuple(
        SourceFileSpec(
            venue="binance_futures",
            symbol="BTCUSDT",
            data_kind=SourceDataKind.ORDERBOOK,
            hour_utc=start + timedelta(hours=index),
        )
        for index in range(2)
    )

    originals = []
    original_contents = []
    rows = {}
    items = []

    for index, spec in enumerate(specs):
        path = spec.local_path(raw_root)
        path.parent.mkdir(parents=True, exist_ok=True)
        contents = f"raw-pruning-fixture-{index}".encode("ascii")
        path.write_bytes(contents)
        digest = hashlib.sha256(contents).hexdigest()

        row = SimpleNamespace(
            status=SourceHourStatus.PROCESSED.value,
            local_path=str(path),
            file_size_bytes=len(contents),
            content_sha256=digest,
        )
        rows[spec.identity_tuple] = row
        originals.append(path)
        original_contents.append(contents)
        items.append(
            {
                "fixture_index": index,
                "local_path": str(path),
                "file_size_bytes": len(contents),
                "content_sha256": digest,
            }
        )

    class Session:
        def __init__(self) -> None:
            self.flush_count = 0

        def flush(self) -> None:
            self.flush_count += 1

            if self.flush_count == 2:
                raise interruption_type("Simulated second-flush interruption")

    session = Session()

    @contextmanager
    def session_scope():
        try:
            yield session
        except BaseException:
            # Model the transaction rollback for durable path metadata.
            for spec, path in zip(specs, originals, strict=True):
                rows[spec.identity_tuple].local_path = str(path)
            raise

    monkeypatch.setattr(maintenance, "session_scope", session_scope)
    monkeypatch.setattr(
        maintenance,
        "_source_spec_from_item",
        lambda item: specs[item["fixture_index"]],
    )
    monkeypatch.setattr(
        maintenance,
        "_source_row_for_spec",
        lambda _session, spec: rows[spec.identity_tuple],
    )
    monkeypatch.setattr(
        maintenance,
        "acquire_source_hour_transaction_lock",
        lambda _session, _spec: None,
    )

    preview = SimpleNamespace(items=tuple(items))
    settings = SimpleNamespace(
        storage=SimpleNamespace(raw_path=raw_root),
    )

    with pytest.raises(interruption_type):
        maintenance._execute_raw_pruning(
            preview,
            settings=settings,
        )

    assert session.flush_count == 2

    for spec, path, contents in zip(
        specs,
        originals,
        original_contents,
        strict=True,
    ):
        assert path.is_file()
        assert path.read_bytes() == contents
        assert rows[spec.identity_tuple].local_path == str(path)

    assert not list(raw_root.rglob("*.pruning"))

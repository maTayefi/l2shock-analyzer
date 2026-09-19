from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.acquisition import (
    FetchOperationBusyError,
    latest_release_eligible_hour as shared_latest_release_eligible_hour,
)
from l2shock.ui.automatic_fetch_runtime import (
    AutomaticFetchRuntime,
    _REQUIRED_FILES_PER_HOUR,
    _REQUIRED_SOURCE_IDENTITIES,
    _source_row_counts_as_complete,
    _write_retry_cursor_durably,
    latest_release_eligible_hour,
    select_automatic_fetch_target_hour,
)
from l2shock.ui.state import get_state, reset_state_for_tests


def _utc(
    hour: int,
    minute: int,
) -> datetime:
    return datetime(
        2026,
        9,
        2,
        hour,
        minute,
        tzinfo=timezone.utc,
    )


def test_latest_hour_becomes_eligible_after_release_delay() -> None:
    result = latest_release_eligible_hour(
        _utc(13, 15),
        release_delay_minutes=15,
    )

    assert result == _utc(12, 0)


def test_automatic_fetch_uses_shared_release_schedule_contract() -> None:
    assert latest_release_eligible_hour is (shared_latest_release_eligible_hour)


def test_current_predecessor_is_not_eligible_before_release_delay() -> None:
    result = latest_release_eligible_hour(
        _utc(13, 14),
        release_delay_minutes=15,
    )

    assert result == _utc(11, 0)


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        0,
        -1,
        1.5,
        "15",
    ],
)
def test_release_delay_must_be_positive_integer(
    value: object,
) -> None:
    with pytest.raises(
        ValueError,
        match="positive integer",
    ):
        latest_release_eligible_hour(
            _utc(13, 15),
            release_delay_minutes=value,  # type: ignore[arg-type]
        )


def test_catch_up_selects_newest_incomplete_hour() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(),
    )

    assert selected == _utc(12, 0)


def test_catch_up_moves_backward_after_newest_is_complete() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(
            _utc(12, 0),
            _utc(11, 0),
        ),
    )

    assert selected == _utc(10, 0)


def test_catch_up_fills_nearest_gap_before_older_hours() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(
            _utc(12, 0),
            _utc(10, 0),
            _utc(9, 0),
        ),
    )

    assert selected == _utc(11, 0)


def test_catch_up_returns_none_when_window_is_complete() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(
            _utc(12, 0),
            _utc(11, 0),
            _utc(10, 0),
            _utc(9, 0),
        ),
    )

    assert selected is None


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        0,
        -1,
        1.5,
        "72",
    ],
)
def test_catch_up_hours_must_be_positive_integer(
    value: object,
) -> None:
    with pytest.raises(
        ValueError,
        match="positive integer",
    ):
        select_automatic_fetch_target_hour(
            _utc(13, 15),
            release_delay_minutes=15,
            catch_up_hours=value,  # type: ignore[arg-type]
            complete_hours=(),
        )


def test_catch_up_rotation_continues_after_attempted_hour() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(),
        after_hour_utc=_utc(12, 0),
    )

    assert selected == _utc(11, 0)


def test_catch_up_rotation_skips_complete_hours() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(_utc(11, 0),),
        after_hour_utc=_utc(12, 0),
    )

    assert selected == _utc(10, 0)


def test_catch_up_rotation_wraps_to_newest_incomplete_hour() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(
            _utc(11, 0),
            _utc(10, 0),
        ),
        after_hour_utc=_utc(9, 0),
    )

    assert selected == _utc(12, 0)


def test_stale_cursor_outside_current_window_does_not_delay_newest() -> None:
    selected = select_automatic_fetch_target_hour(
        _utc(13, 15),
        release_delay_minutes=15,
        catch_up_hours=4,
        complete_hours=(),
        after_hour_utc=_utc(8, 0),
    )

    assert selected == _utc(12, 0)


def test_pruned_processed_source_remains_acquisition_complete(
    tmp_path: Path,
) -> None:
    assert _source_row_counts_as_complete(
        venue="binance_futures",
        instrument="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_utc(12, 0),
        status="processed",
        local_path=None,
        file_size_bytes=123,
        content_sha256="a" * 64,
        raw_root=tmp_path,
    )


def test_downloaded_source_requires_canonical_local_file(
    tmp_path: Path,
) -> None:
    assert not _source_row_counts_as_complete(
        venue="binance_futures",
        instrument="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_utc(12, 0),
        status="downloaded",
        local_path=None,
        file_size_bytes=123,
        content_sha256="a" * 64,
        raw_root=tmp_path,
    )


def test_downloaded_source_with_canonical_file_is_complete(
    tmp_path: Path,
) -> None:
    import hashlib

    from l2shock.acquisition import SourceFileSpec

    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_utc(12, 0),
    )
    path = spec.local_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"source")

    assert _source_row_counts_as_complete(
        venue=spec.venue,
        instrument=spec.symbol,
        data_kind=spec.data_kind.value,
        hour_utc=spec.hour_utc,
        status="downloaded",
        local_path=str(path),
        file_size_bytes=path.stat().st_size,
        content_sha256=hashlib.sha256(b"source").hexdigest(),
        raw_root=tmp_path,
    )


@pytest.mark.asyncio
async def test_automatic_fetch_start_rejects_another_active_operation() -> None:
    reset_state_for_tests()
    state = get_state()
    state.active_operation_name = "manual_processing"

    runtime = AutomaticFetchRuntime()

    with pytest.raises(
        FetchOperationBusyError,
        match="manual_processing",
    ):
        runtime.start()

    assert runtime.is_running is False
    assert runtime.enabled is False
    assert state.tracked_tasks == set()


@pytest.mark.asyncio
async def test_retry_cursor_finishes_persistence_before_cancellation_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import l2shock.ui.automatic_fetch_runtime as runtime_module

    entered = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    def blocking_write(
        *,
        newest_eligible_hour_utc: datetime,
        attempted_hour_utc: datetime,
    ) -> None:
        assert newest_eligible_hour_utc == _utc(12, 0)
        assert attempted_hour_utc == _utc(11, 0)

        entered.set()
        release.wait(timeout=2.0)
        completed.set()

    monkeypatch.setattr(
        runtime_module,
        "_write_retry_cursor_sync",
        blocking_write,
    )

    task = asyncio.create_task(
        _write_retry_cursor_durably(
            newest_eligible_hour_utc=_utc(12, 0),
            attempted_hour_utc=_utc(11, 0),
        )
    )

    observed = await asyncio.to_thread(
        entered.wait,
        1.0,
    )
    assert observed is True

    task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert completed.is_set()


def test_remote_imported_processed_source_is_acquisition_complete_without_raw(
    tmp_path: Path,
) -> None:
    assert _source_row_counts_as_complete(
        venue="binance_futures",
        instrument="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_utc(12, 0),
        status="processed",
        local_path=None,
        file_size_bytes=None,
        content_sha256="a" * 64,
        raw_root=tmp_path,
        quality_json={
            "schema": "l2shock.remote_imported_source_hour_quality",
            "schema_version": 1,
            "processing_origin": "hugging_face_remote_import_v1",
        },
    )


def test_processed_source_with_claimed_local_path_requires_intact_file(
    tmp_path: Path,
) -> None:
    from l2shock.acquisition import SourceFileSpec

    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_utc(12, 0),
    )
    missing_path = spec.local_path(tmp_path)

    assert not _source_row_counts_as_complete(
        venue=spec.venue,
        instrument=spec.symbol,
        data_kind=spec.data_kind.value,
        hour_utc=spec.hour_utc,
        status="processed",
        local_path=str(missing_path),
        file_size_bytes=123,
        content_sha256="a" * 64,
        raw_root=tmp_path,
    )


def test_remote_import_marker_does_not_bypass_noncanonical_local_path(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside" / "BTCUSDT_orderbook.parquet"
    outside.parent.mkdir(parents=True)
    outside.write_bytes(b"source")

    assert not _source_row_counts_as_complete(
        venue="binance_futures",
        instrument="BTCUSDT",
        data_kind="orderbook",
        hour_utc=_utc(12, 0),
        status="processed",
        local_path=str(outside),
        file_size_bytes=outside.stat().st_size,
        content_sha256="a" * 64,
        raw_root=tmp_path / "raw",
        quality_json={
            "processing_origin": "hugging_face_remote_import_v1",
        },
    )


def test_automatic_fetch_requires_bybit_orderbooks() -> None:
    assert _REQUIRED_FILES_PER_HOUR == 8

    assert (
        "bybit",
        "BTCUSDT",
        "orderbook",
    ) in _REQUIRED_SOURCE_IDENTITIES

    assert (
        "bybit",
        "ETHUSDT",
        "orderbook",
    ) in _REQUIRED_SOURCE_IDENTITIES

    assert not any(
        venue == "bybit" and data_kind == "trades"
        for venue, _instrument, data_kind in _REQUIRED_SOURCE_IDENTITIES
    )


def test_same_size_corrupted_local_source_is_not_complete(
    tmp_path: Path,
) -> None:
    import hashlib

    from l2shock.acquisition import (
        SourceDataKind,
        SourceFileSpec,
    )

    hour = datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )
    raw_root = tmp_path / "raw"
    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour,
    )
    canonical = spec.local_path(raw_root)
    original = b"original-source-content"
    corrupted = b"x" * len(original)

    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(corrupted)

    complete = _source_row_counts_as_complete(
        venue=spec.venue,
        instrument=spec.symbol,
        data_kind=spec.data_kind.value,
        hour_utc=spec.hour_utc,
        status="downloaded",
        local_path=str(canonical),
        file_size_bytes=len(original),
        content_sha256=hashlib.sha256(original).hexdigest(),
        raw_root=raw_root,
    )

    assert complete is False


def test_hash_verified_canonical_local_source_is_complete(
    tmp_path: Path,
) -> None:
    import hashlib

    from l2shock.acquisition import (
        SourceDataKind,
        SourceFileSpec,
    )

    hour = datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )
    raw_root = tmp_path / "raw"
    spec = SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=hour,
    )
    canonical = spec.local_path(raw_root)
    content = b"verified-source-content"

    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(content)

    complete = _source_row_counts_as_complete(
        venue=spec.venue,
        instrument=spec.symbol,
        data_kind=spec.data_kind.value,
        hour_utc=spec.hour_utc,
        status="downloaded",
        local_path=str(canonical),
        file_size_bytes=len(content),
        content_sha256=hashlib.sha256(content).hexdigest(),
        raw_root=raw_root,
    )

    assert complete is True

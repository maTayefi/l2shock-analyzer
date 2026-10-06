from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import l2shock.analysis.l2_view_stream as stream

_START = datetime(2026, 9, 14, 0, tzinfo=timezone.utc)


def _load(
    monkeypatch: pytest.MonkeyPatch,
    *,
    budget: int,
    failing_hour: int | None,
    missing_hour: int | None = None,
):
    component = SimpleNamespace(
        preset_hash="b" * 64,
        market=SimpleNamespace(
            provider="cryptohftdata",
            venue="binance_futures",
            instrument="BTCUSDT",
        ),
    )
    price_reads: list[tuple[datetime, datetime]] = []

    class L2Repository:
        def list_l2_hours(self, **kwargs):
            assert kwargs["verify_codec"] is True
            return ()

    class PriceRepository:
        def list_price_hours(
            self,
            *,
            base,
            start_utc,
            end_utc,
            verify_codec,
        ):
            assert base == "BTC"
            assert verify_codec is True
            price_reads.append((start_utc, end_utc))

            rows = []
            hour = start_utc

            while hour < end_utc:
                offset = int((hour - _START).total_seconds()) // 3600

                if offset == failing_hour:
                    raise ValueError("Simulated verified price retrieval failure")

                if offset != missing_hour:
                    rows.append(
                        SimpleNamespace(
                            base="BTC",
                            source_venue="binance_futures",
                            source_symbol="BTCUSDT",
                            hour_utc=hour,
                            encoded=SimpleNamespace(
                                content_sha256=f"{offset + 1:064x}",
                            ),
                        )
                    )

                hour += timedelta(hours=1)

            return tuple(rows)

    l2_repository = L2Repository()
    price_repository = PriceRepository()

    @contextmanager
    def open_repositories():
        yield l2_repository, price_repository

    def resolve_components(_repository, _request):
        return (component,)

    def hour_pairs(
        _components,
        _rows_by_component,
        *,
        hour_epoch,
        low,
        high,
        digest,
    ):
        del hour_epoch, digest
        return (
            tuple((Decimal("2"), Decimal("1")) for _ in range(low, high)),
            0,
            frozenset(),
        )

    monkeypatch.setattr(
        stream,
        "resolve_l2_view_components",
        resolve_components,
    )
    monkeypatch.setattr(stream, "_hour_l2_pairs", hour_pairs)
    monkeypatch.setattr(
        stream,
        "decode_hourly_trade_ohlc_blocks",
        lambda _encoded: object(),
    )
    monkeypatch.setattr(
        stream,
        "_price_value",
        lambda _decoded, _index: (
            Decimal("100"),
            Decimal("100"),
            Decimal("100"),
            Decimal("100"),
        ),
    )

    request = stream.L2ViewRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=_START,
        requested_end_utc=_START + timedelta(hours=9, seconds=-1),
    )
    options = stream.L2ViewLoadOptions(
        timeframe_seconds=3600,
        memory_budget_mib=budget,
    )

    projection = stream.stream_l2_view(
        request,
        options,
        open_repositories=open_repositories,
    )
    return projection, price_reads


def test_optional_price_failure_is_independent_of_memory_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    small, small_reads = _load(
        monkeypatch,
        budget=64,
        failing_hour=8,
    )
    large, large_reads = _load(
        monkeypatch,
        budget=512,
        failing_hour=8,
    )

    assert small.bars == large.bars
    assert small.input_id == large.input_id
    assert small.price_status == large.price_status == "failed"
    assert sum(bar.price is not None for bar in small.bars) == 8
    assert small.bars[-1].price is None
    assert small.price_regions == large.price_regions == ()
    assert small_reads == large_reads
    assert len(small_reads) == 9

    for start, end in small_reads:
        assert end - start == timedelta(hours=1)


def test_price_loading_stops_after_first_failed_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection, reads = _load(
        monkeypatch,
        budget=512,
        failing_hour=2,
    )

    assert projection.price_status == "failed"
    assert sum(bar.price is not None for bar in projection.bars) == 2
    assert len(reads) == 3
    assert all(bar.price is None for bar in projection.bars[2:])
    assert all(bar.valid_l2 for bar in projection.bars)


def test_missing_price_hour_does_not_stop_later_price_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection, reads = _load(
        monkeypatch,
        budget=64,
        failing_hour=None,
        missing_hour=2,
    )

    assert projection.price_status == "loaded"
    assert projection.bars[2].price is None
    assert projection.bars[3].price is not None
    assert sum(bar.price is not None for bar in projection.bars) == 8
    assert len(reads) == 9
    assert all(bar.valid_l2 for bar in projection.bars)


def _audit_identity_load(
    monkeypatch,
    *,
    query_failure_hour=None,
    corrupt_query=False,
    decode_failure_hour=None,
    slot_failure=None,
    missing_from_hour=None,
    repository_available=True,
    timeframe_seconds=60,
    memory_budget_mib=64,
    l2_warning_seconds=60,
    price_warning_seconds=180,
):
    from contextlib import contextmanager
    from datetime import datetime, timedelta, timezone
    from decimal import Decimal
    from types import SimpleNamespace

    import l2shock.analysis.l2_view_stream as module
    from l2shock.db.analytical_repository import AnalyticalRowCorruptionError

    start = datetime(2095, 1, 1, tzinfo=timezone.utc)
    component = SimpleNamespace(
        preset_hash="b" * 64,
        market=SimpleNamespace(
            provider="cryptohftdata",
            venue="binance_futures",
            instrument="BTCUSDT",
        ),
    )

    class L2Repository:
        def list_l2_hours(self, **kwargs):
            assert kwargs["verify_codec"] is True
            return ()

    class PriceRepository:
        def list_price_hours(
            self,
            *,
            base,
            start_utc,
            end_utc,
            verify_codec,
        ):
            assert base == "BTC"
            assert verify_codec is True
            assert end_utc - start_utc == timedelta(hours=1)
            offset = (start_utc - start) // timedelta(hours=1)

            if offset == query_failure_hour:
                failure_type = (
                    AnalyticalRowCorruptionError if corrupt_query else ValueError
                )
                raise failure_type("Simulated identity-boundary query failure")

            if missing_from_hour is not None and offset >= missing_from_hour:
                return ()

            return (
                SimpleNamespace(
                    base="BTC",
                    source_venue="binance_futures",
                    source_symbol="BTCUSDT",
                    hour_utc=start_utc,
                    encoded=SimpleNamespace(
                        content_sha256=f"{offset + 1:064x}",
                    ),
                ),
            )

    l2_repository = L2Repository()
    price_repository = PriceRepository()

    @contextmanager
    def repositories():
        yield (
            l2_repository,
            price_repository if repository_available else None,
        )

    def pairs(
        _components,
        _rows,
        *,
        hour_epoch,
        low,
        high,
        digest,
    ):
        # Keep identical L2 input evidence across every price scenario.
        digest.update(f"audit-l2-hour:{hour_epoch}\n".encode("ascii"))
        return (
            tuple((Decimal("2"), Decimal("1")) for _ in range(low, high)),
            0,
            frozenset(),
        )

    def decode(encoded):
        offset = int(encoded.content_sha256, 16) - 1
        if offset == decode_failure_hour:
            raise ValueError("Simulated identity-boundary decoding failure")
        return SimpleNamespace(hour_offset=offset)

    def price_value(decoded, second):
        if slot_failure == (decoded.hour_offset, second):
            raise module.L2ViewError(
                "Simulated identity-boundary inconsistent price slot"
            )
        value = Decimal("100")
        return value, value, value, value

    monkeypatch.setattr(
        module,
        "resolve_l2_view_components",
        lambda _repository, _request: (component,),
    )
    monkeypatch.setattr(module, "_hour_l2_pairs", pairs)
    monkeypatch.setattr(module, "decode_hourly_trade_ohlc_blocks", decode)
    monkeypatch.setattr(module, "_price_value", price_value)

    request = module.L2ViewRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=start,
        requested_end_utc=start + timedelta(hours=3, seconds=-1),
    )
    options = module.L2ViewLoadOptions(
        timeframe_seconds=timeframe_seconds,
        max_bars=1200,
        memory_budget_mib=memory_budget_mib,
        l2_warning_seconds=l2_warning_seconds,
        price_warning_seconds=price_warning_seconds,
    )
    return module.stream_l2_view(
        request,
        options,
        open_repositories=repositories,
    )


def test_audit_identity_distinguishes_query_failure_from_missing_suffix(
    monkeypatch,
):
    failed = _audit_identity_load(
        monkeypatch,
        query_failure_hour=1,
    )
    missing = _audit_identity_load(
        monkeypatch,
        missing_from_hour=1,
    )

    assert failed.bars == missing.bars
    assert failed.price_status == "failed"
    assert missing.price_status == "loaded"
    assert failed.price_regions == ()
    assert missing.price_regions
    assert failed.input_id != missing.input_id


def test_audit_identity_distinguishes_failed_and_corrupt_price_queries(
    monkeypatch,
):
    failed = _audit_identity_load(
        monkeypatch,
        query_failure_hour=1,
    )
    corrupt = _audit_identity_load(
        monkeypatch,
        query_failure_hour=1,
        corrupt_query=True,
    )

    assert failed.bars == corrupt.bars
    assert failed.price_status == "failed"
    assert corrupt.price_status == "corrupt"
    assert failed.input_id != corrupt.input_id


def test_audit_identity_distinguishes_corrupt_slot_boundaries(monkeypatch):
    first = _audit_identity_load(
        monkeypatch,
        slot_failure=(0, 10),
    )
    second = _audit_identity_load(
        monkeypatch,
        slot_failure=(0, 20),
    )

    # Identical constant-price candles can hide different accepted prefixes.
    assert first.bars == second.bars
    assert first.price_status == second.price_status == "corrupt"
    assert first.input_id != second.input_id


def test_audit_identity_includes_l2_warning_threshold(monkeypatch):
    first = _audit_identity_load(
        monkeypatch,
        l2_warning_seconds=60,
    )
    second = _audit_identity_load(
        monkeypatch,
        l2_warning_seconds=61,
    )

    assert first.bars == second.bars
    assert first.input_id != second.input_id


def test_audit_identity_includes_price_warning_threshold(monkeypatch):
    first = _audit_identity_load(
        monkeypatch,
        missing_from_hour=0,
        price_warning_seconds=180,
    )
    second = _audit_identity_load(
        monkeypatch,
        missing_from_hour=0,
        price_warning_seconds=181,
    )

    assert first.bars == second.bars
    assert first.input_id != second.input_id


def test_audit_identity_distinguishes_unavailable_repository_from_missing_data(
    monkeypatch,
):
    unavailable = _audit_identity_load(
        monkeypatch,
        repository_available=False,
    )
    missing = _audit_identity_load(
        monkeypatch,
        missing_from_hour=0,
    )

    assert unavailable.bars == missing.bars
    assert unavailable.price_status == missing.price_status == "missing"
    assert unavailable.price_regions == ()
    assert missing.price_regions
    assert unavailable.input_id != missing.input_id


@pytest.mark.parametrize(
    "failure",
    (
        {},
        {"query_failure_hour": 1},
        {"decode_failure_hour": 1},
        {"slot_failure": (0, 10)},
        {"repository_available": False},
    ),
)
def test_audit_identity_remains_independent_of_memory_budget(
    monkeypatch,
    failure,
):
    small = _audit_identity_load(
        monkeypatch,
        memory_budget_mib=64,
        **failure,
    )
    large = _audit_identity_load(
        monkeypatch,
        memory_budget_mib=512,
        **failure,
    )

    assert small.bars == large.bars
    assert small.price_status == large.price_status
    assert small.input_id == large.input_id


def test_audit_identity_agrees_between_direct_and_cached_coarsening(
    monkeypatch,
):
    import l2shock.analysis.l2_view_stream as module

    fine = _audit_identity_load(
        monkeypatch,
        timeframe_seconds=60,
    )
    direct = _audit_identity_load(
        monkeypatch,
        timeframe_seconds=300,
    )
    cached = module.coarsen_l2_view(
        fine,
        300,
        max_bars=1200,
    )

    assert cached.bars == direct.bars
    assert fine.input_id == cached.input_id == direct.input_id

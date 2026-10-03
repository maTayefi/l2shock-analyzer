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

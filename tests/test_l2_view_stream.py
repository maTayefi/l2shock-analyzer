# tests/test_l2_view_stream.py
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import l2shock.analysis.l2_view_stream as module
from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.l2_view_metrics import L2ViewMetric, compute_l2_view_metric
from l2shock.analysis.l2_view_stream import (
    L2ViewError,
    L2ViewLoadOptions,
    L2ViewRequest,
    coarsen_l2_view,
    stream_l2_view,
)
from l2shock.ingest.sampling import BookSampleInvalidReason, BookSampleQuality
from l2shock.presets import build_binance_futures_data_preset
from l2shock.ui.l2_view_chart_options import build_l2_view_chart_options

_HOUR = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
_INVALID = frozenset(range(100, 200))


def _preset():
    return build_binance_futures_data_preset(
        base="BTC", lower_fraction=Decimal("0"), upper_fraction=Decimal("0.01")
    )


def _decode(row):
    out = []
    for i in range(3_600):
        ts = row.hour_utc + timedelta(seconds=i)
        if i in _INVALID:
            out.append(
                L2Second(
                    ts,
                    BookSampleQuality.INVALID,
                    BookSampleInvalidReason.UNINITIALIZED,
                    None,
                    None,
                    0,
                )
            )
        else:
            out.append(
                L2Second(
                    ts,
                    BookSampleQuality.VALID,
                    None,
                    Decimal(10 + i % 7),
                    Decimal(5),
                    1,
                )
            )
    return tuple(out)


class _Repo:
    def __init__(self, preset):
        self.preset = preset

    def get_preset(self, preset_hash):
        return SimpleNamespace(base="BTC", config_json=self.preset.to_canonical_dict())

    def list_l2_hours(
        self, *, base, preset_hash, start_utc, end_utc, verify_codec=True
    ):
        assert verify_codec
        hour = start_utc
        rows = []
        while hour < end_utc:
            if hour == _HOUR:
                rows.append(
                    SimpleNamespace(
                        base=base,
                        preset_hash=preset_hash,
                        hour_utc=hour,
                        encoded=SimpleNamespace(content_sha256="a" * 64),
                    )
                )
            hour += timedelta(hours=1)
        return tuple(rows)


class _NoPrice:
    def list_price_hours(self, **_kwargs):
        return ()


def _opener(preset):
    @contextmanager
    def open_repositories():
        yield _Repo(preset), _NoPrice()

    return open_repositories


def _run(monkeypatch, end_offset=3_599, timeframe=60):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _preset()
    request = L2ViewRequest(
        "BTC", preset.preset_hash, _HOUR, _HOUR + timedelta(seconds=end_offset)
    )
    return stream_l2_view(
        request,
        L2ViewLoadOptions(timeframe_seconds=timeframe),
        open_repositories=_opener(preset),
    )


def test_bars_cover_range_and_invalid_bars_are_null(monkeypatch):
    p = _run(monkeypatch)
    assert len(p.bars) == 60
    assert p.usable_l2_seconds == 3_500 and p.unusable_l2_seconds == 100
    assert p.bars[1].bid is None and p.bars[3].bid is None  # seconds 100..199
    assert p.bars[0].bid is not None
    assert p.price_status == "missing"
    assert len(p.l2_regions) == 1
    assert p.l2_regions[0].start_utc == _HOUR + timedelta(seconds=100)


def test_coarsen_equals_direct_stream(monkeypatch):
    fine = _run(monkeypatch, timeframe=60)
    direct = _run(monkeypatch, timeframe=900)
    assert coarsen_l2_view(fine, 900).bars == direct.bars


def test_missing_next_hour_is_an_outage_not_zero(monkeypatch):
    p = _run(monkeypatch, end_offset=3_600 + 120, timeframe=60)
    assert p.bars[-1].bid is None
    assert p.l2_regions[-1].end_utc_exclusive == p.end_utc_exclusive


def test_duration_limit_uses_slot_count():
    preset = _preset()
    L2ViewRequest("BTC", preset.preset_hash, _HOUR, _HOUR + timedelta(seconds=86_399))
    with pytest.raises(L2ViewError, match="max analysis duration"):
        L2ViewRequest(
            "BTC", preset.preset_hash, _HOUR, _HOUR + timedelta(seconds=86_400)
        )


def test_change_metrics_null_first_and_after_invalid(monkeypatch):
    p = _run(monkeypatch)
    values = compute_l2_view_metric(p.bars, L2ViewMetric.TOTAL_CHANGE)
    assert values[0] is None and values[2] is None and values[4] is None
    assert values[5] is not None


def test_chart_has_five_panels_and_y_zoom(monkeypatch):
    p = _run(monkeypatch)
    option = build_l2_view_chart_options(
        p, panel_a_metric="imbalance_pct", panel_b_metric="delta"
    )
    assert len(option["grid"]) == 5
    assert option["dataZoom"][0]["xAxisIndex"] == [0, 1, 2, 3, 4]
    assert [z["yAxisIndex"] for z in option["dataZoom"][1:]] == [
        [0],
        [1],
        [2],
        [3],
        [4],
    ]
    assert option["l2shockChartMetadata"]["bar_duration_seconds"] == 60

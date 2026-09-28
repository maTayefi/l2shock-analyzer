from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from l2shock.analysis import shock_execution
from l2shock.ingest.sampling import BookSampleQuality

_ROOT = Path(__file__).resolve().parents[1]
_TAB = _ROOT / "l2shock" / "ui" / "tab_shock_review.py"
_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_inspection_order_is_tab_local() -> None:
    source = _TAB.read_text(encoding="utf-8")

    assert "reorder_last_review" not in source
    assert "last_review is not current_model.review" not in source
    assert "last_review is current_model.review" not in source
    assert "_review_is_current(current_model)" in source
    assert "presets_base" in source


def _scale_fraction():
    for name in ("l2shock.ui.tab_shock_review", "l2shock.ui.analysis_inputs"):
        module = importlib.import_module(name)
        if hasattr(module, "_scale_fraction"):
            return module._scale_fraction
    pytest.fail("_scale_fraction not found")


def test_scale_fraction_removes_binary_float_noise() -> None:
    parse = _scale_fraction()

    assert parse(0.1 + 0.2, "Major") == Decimal("0.3")
    assert parse(0.123456, "Major") == Decimal("0.123456")
    assert parse("0.25", "Major") == Decimal("0.25")

    with pytest.raises(ValueError):
        parse(float("nan"), "Major")


def _dataset(invalid_index: int, count: int = 10) -> SimpleNamespace:
    invalid = next(q for q in BookSampleQuality if q is not BookSampleQuality.VALID)
    seconds = tuple(
        SimpleNamespace(
            quality=invalid if i == invalid_index else BookSampleQuality.VALID,
            timestamp_utc=_T0 + timedelta(seconds=i),
        )
        for i in range(count)
    )
    return SimpleNamespace(seconds=seconds, input_id="batch2-input")


def _hypothesis(a: int, b: int, c: int) -> SimpleNamespace:
    return SimpleNamespace(
        scale_name="major",
        a_index=a,
        b_index=b,
        c_index=c,
        a_utc=_T0 + timedelta(seconds=a),
        b_utc=_T0 + timedelta(seconds=b),
        c_utc=_T0 + timedelta(seconds=c),
    )


def _patch(monkeypatch: pytest.MonkeyPatch, hypotheses: tuple) -> None:
    monkeypatch.setattr(
        shock_execution,
        "propose_shock_starts",
        lambda seconds, *, config: hypotheses,
    )


def test_execute_rejects_candidate_crossing_invalid_second(monkeypatch) -> None:
    _patch(monkeypatch, (_hypothesis(2, 4, 7),))

    with pytest.raises(ValueError, match="invalid L2 coverage"):
        shock_execution._execute(_dataset(invalid_index=5), None)


def test_execute_accepts_candidate_after_invalid_second(monkeypatch) -> None:
    clean = _hypothesis(6, 7, 9)
    _patch(monkeypatch, (clean,))

    scan = shock_execution._execute(_dataset(invalid_index=5), None)

    assert scan.hypotheses == (clean,)


def test_execute_rejects_out_of_range_candidate(monkeypatch) -> None:
    _patch(monkeypatch, (_hypothesis(7, 8, 12),))

    with pytest.raises(ValueError, match="outside the L2 input"):
        shock_execution._execute(_dataset(invalid_index=0), None)

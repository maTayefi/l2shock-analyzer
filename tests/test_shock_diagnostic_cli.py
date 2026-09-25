# tests/test_shock_diagnostic_cli.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from l2shock.analysis.shock_diagnostic_cli import (
    _datetime_utc,
    _scale,
    _write_new_file,
)


def test_requires_explicit_utc():
    assert _datetime_utc("2026-09-20T12:00:00Z") == datetime(
        2026, 9, 20, 12, tzinfo=timezone.utc
    )
    with pytest.raises(Exception):
        _datetime_utc("2026-09-20T12:00:00")


def test_scale_is_structural_not_timeframe():
    scale = _scale("major:0.20:60:4")
    assert scale.minimum_leg_fraction == Decimal("0.20")
    assert scale.pivot_radius_seconds == 60
    assert scale.forward_radius_multiplier == 4


def test_export_publishes_complete_file_and_refuses_overwrite(tmp_path):
    target = tmp_path / "scan.json"
    first = b'{"complete":true}\n'
    _write_new_file(target, first)

    assert json.loads(target.read_bytes()) == {"complete": True}

    with pytest.raises(FileExistsError):
        _write_new_file(target, b'{"different":true}\n')

    assert target.read_bytes() == first
    assert not tuple(tmp_path.glob(".*.tmp"))

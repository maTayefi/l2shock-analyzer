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


def _cli_args(tmp_path, *extra: str) -> list[str]:
    return [
        "--base",
        "BTC",
        "--preset-hash",
        "a" * 64,
        "--start-utc",
        "2026-09-20T12:00:00Z",
        "--end-utc",
        "2026-09-20T12:10:00Z",
        "--output",
        str(tmp_path / "out.json"),
        *extra,
    ]


@pytest.mark.parametrize("bad", ["abc", "NaN", "Infinity", ""])
def test_invalid_channel_fraction_is_a_usage_error_not_a_traceback(
    tmp_path,
    capsys,
    bad: str,
) -> None:
    from l2shock.analysis.shock_diagnostic_cli import _parser

    with pytest.raises(SystemExit) as excinfo:
        _parser().parse_args(_cli_args(tmp_path, "--minimum-channel-leg-fraction", bad))

    assert excinfo.value.code == 2
    assert "decimal" in capsys.readouterr().err


def test_cli_order_defaults_to_ui_and_decimal_stays_exact(tmp_path) -> None:
    from l2shock.analysis.shock_diagnostic_cli import _parser
    from l2shock.analysis.shock_review import (
        SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION,
        SHOCK_REVIEW_ORDER_VERSION,
    )

    args = _parser().parse_args(
        _cli_args(tmp_path, "--minimum-channel-leg-fraction", "0.07")
    )
    assert args.minimum_channel_leg_fraction == Decimal("0.07")
    assert args.order_version == SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION

    explicit = _parser().parse_args(
        _cli_args(tmp_path, "--order-version", SHOCK_REVIEW_ORDER_VERSION)
    )
    assert explicit.order_version == SHOCK_REVIEW_ORDER_VERSION

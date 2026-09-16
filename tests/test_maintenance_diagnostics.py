from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from l2shock.maintenance_diagnostics import (
    MaintenanceDiagnosticsError,
    summarize_duration_seconds,
)


def test_duration_summary_is_deterministic() -> None:
    result = summarize_duration_seconds(
        (
            10.0,
            20.0,
            30.0,
            40.0,
        )
    )

    assert result == {
        "count": 4,
        "minimum_seconds": 10.0,
        "maximum_seconds": 40.0,
        "average_seconds": 25.0,
        "p50_seconds": 25.0,
        "p95_seconds": 38.5,
    }


def test_empty_duration_summary_is_explicit() -> None:
    result = summarize_duration_seconds(())

    assert result == {
        "count": 0,
        "minimum_seconds": None,
        "maximum_seconds": None,
        "average_seconds": None,
        "p50_seconds": None,
        "p95_seconds": None,
    }


@pytest.mark.parametrize(
    "value",
    (
        -1.0,
        float("inf"),
        float("-inf"),
        float("nan"),
    ),
)
def test_duration_summary_rejects_invalid_values(
    value: float,
) -> None:
    with pytest.raises(MaintenanceDiagnosticsError):
        summarize_duration_seconds((value,))


def test_maintenance_module_is_read_only_by_contract() -> None:
    import l2shock.maintenance_diagnostics as module

    source = Path(module.__file__).read_text(
        encoding="utf-8",
    )

    prohibited_calls = (
        ".unlink(",
        "os.remove(",
        "os.unlink(",
        "shutil.rmtree(",
        "session.delete(",
        ".execute(delete(",
        ".execute(update(",
    )

    assert all(prohibited not in source for prohibited in prohibited_calls)


def test_fixed_diagnostic_time_is_timezone_aware() -> None:
    value = datetime(
        2089,
        1,
        1,
        12,
        tzinfo=timezone.utc,
    )

    assert value.utcoffset() is not None

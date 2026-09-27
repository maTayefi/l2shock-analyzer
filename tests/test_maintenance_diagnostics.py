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


def test_checkpoint_walk_errors_are_reported(tmp_path, monkeypatch) -> None:
    import l2shock.maintenance_diagnostics as diagnostics

    def failing_walk(root, *, topdown, onerror, followlinks):
        onerror(PermissionError(13, "denied", str(root / "locked")))
        return iter(())

    monkeypatch.setattr(diagnostics.os, "walk", failing_walk)
    files, issues = diagnostics._checkpoint_files(tmp_path)

    assert files == []
    assert issues == [{"path": str(tmp_path / "locked"), "error": "PermissionError"}]


def test_analytical_consistency_is_preset_aware() -> None:
    from datetime import datetime, timezone

    from sqlalchemy.orm import Session

    import l2shock.maintenance_diagnostics as diagnostics

    hour = datetime(2026, 9, 2, 12, tzinfo=timezone.utc)
    content = "d" * 64
    expected_preset = "1" * 64
    other_preset = "2" * 64

    class _Rows:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return list(self._rows)

    def run(l2_preset: str) -> dict[str, object]:
        batches = [
            [
                (
                    "cryptohftdata",
                    "binance_futures",
                    "orderbook",
                    "BTCUSDT",
                    hour,
                    "processed",
                    {
                        "analytical_content_sha256": content,
                        "analytical_content_sha256s": [content],
                        "analytical_outputs_by_preset": {expected_preset: content},
                    },
                )
            ],
            [("BTC", hour, l2_preset, content, 100)],
            [],
        ]
        session = Session()
        session.execute = lambda *_args, **_kwargs: _Rows(batches.pop(0))
        return diagnostics.analytical_consistency_diagnostics(session)

    wrong = run(other_preset)
    assert wrong["missing_processed_output_count"] == 1
    assert wrong["orphan_l2_row_count"] == 1
    assert wrong["missing_processed_outputs"][0]["expected_preset_hash"] == (
        expected_preset
    )

    right = run(expected_preset)
    assert right["missing_processed_output_count"] == 0
    assert right["orphan_l2_row_count"] == 0
    assert right["malformed_processed_metadata_count"] == 0

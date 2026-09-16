from __future__ import annotations

import json
from pathlib import Path

import pytest

from l2shock.diagnostics import (
    directory_inventory,
    report_json_bytes,
)
from l2shock.ui.tab_settings import (
    _diagnostics_export_filename,
)


def test_directory_inventory_counts_files_and_bytes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "storage"
    nested = root / "nested"

    nested.mkdir(parents=True)
    (root / "first.bin").write_bytes(b"abc")
    (nested / "second.bin").write_bytes(b"12345")

    result = directory_inventory(root)

    assert result["path"] == str(root.resolve())
    assert result["exists"] is True
    assert result["is_directory"] is True
    assert result["file_count"] == 2
    assert result["directory_count"] == 2
    assert result["symlink_count"] == 0
    assert result["total_bytes"] == 8
    assert result["error_count"] == 0
    assert result["errors"] == []


def test_directory_inventory_reports_missing_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "missing"

    result = directory_inventory(root)

    assert result["path"] == str(root.resolve())
    assert result["exists"] is False
    assert result["is_directory"] is False
    assert result["file_count"] == 0
    assert result["total_bytes"] == 0
    assert result["error_count"] == 0


def test_directory_inventory_does_not_follow_symlink(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target"
    root = tmp_path / "storage"

    target.mkdir()
    root.mkdir()
    (target / "outside.bin").write_bytes(b"outside")

    link = root / "linked"

    try:
        link.symlink_to(
            target,
            target_is_directory=True,
        )
    except OSError:
        pytest.skip("Directory symlinks are unavailable on this platform")

    result = directory_inventory(root)

    assert result["symlink_count"] == 1
    assert result["file_count"] == 0
    assert result["total_bytes"] == 0


def test_report_json_bytes_is_deterministic() -> None:
    first = {
        "schema_version": 1,
        "schema": "example",
        "nested": {
            "b": 2,
            "a": 1,
        },
    }
    second = {
        "nested": {
            "a": 1,
            "b": 2,
        },
        "schema": "example",
        "schema_version": 1,
    }

    first_encoded = report_json_bytes(first)
    second_encoded = report_json_bytes(second)

    assert first_encoded == second_encoded
    assert first_encoded.endswith(b"\n")

    decoded = json.loads(first_encoded)

    assert decoded["schema"] == "example"
    assert decoded["nested"] == {
        "a": 1,
        "b": 2,
    }


def test_report_json_rejects_non_finite_float() -> None:
    with pytest.raises(
        ValueError,
        match="Out of range float values",
    ):
        report_json_bytes(
            {
                "bad": float("inf"),
            }
        )


def test_diagnostics_filename_preserves_json_extension() -> None:
    filename = _diagnostics_export_filename(
        {
            "generated_at_utc": "2026-09-10T12:34:56+00:00",
        }
    )

    assert filename.startswith("l2shock-production-diagnostics-")
    assert filename.endswith(".json")
    assert "/" not in filename
    assert "\\" not in filename
    assert ":" not in filename

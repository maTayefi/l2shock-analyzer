# tests/test_project_cores_inventory.py
"""project_cores.md global inventories must match the repository (Batch 40)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORES = (ROOT / "project_cores.md").read_text(encoding="utf-8")


def _first_block_after(heading: str) -> list[str]:
    start = CORES.index(f"\n{heading}\n")
    match = re.compile(r"```text\n(.*?)\n```", re.DOTALL).search(CORES, start)
    assert match is not None, heading
    return [line.strip() for line in match.group(1).splitlines() if line.strip()]


def _python_files(root: str) -> list[str]:
    return sorted(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / root).rglob("*.py")
        if "__pycache__" not in path.parts
    )


def test_production_inventory_matches_repository() -> None:
    assert _first_block_after("# Global production-module inventory") == _python_files(
        "l2shock"
    )


def test_test_inventory_matches_repository() -> None:
    assert _first_block_after("# Global test inventory") == _python_files("tests")


def test_core4_is_shock_start_and_every_listed_path_exists() -> None:
    start = CORES.index("\n# Core 4 ")
    end = CORES.index("\n# Core 5 ")
    section = CORES[start:end]

    assert "Shock-Start" in section
    assert "liquidity_movement" not in section
    assert "LM detection" not in CORES.split("# Global production-module inventory")[0]

    for block in re.findall(r"```text\n(.*?)\n```", section, flags=re.DOTALL):
        for line in block.splitlines():
            if line.strip():
                assert (ROOT / line.strip()).exists(), line

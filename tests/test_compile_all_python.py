from __future__ import annotations

import py_compile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _python_files() -> tuple[Path, ...]:
    roots = (
        PROJECT_ROOT / "l2shock",
        PROJECT_ROOT / "tools",
    )

    return tuple(
        sorted(
            path
            for root in roots
            if root.exists()
            for path in root.rglob("*.py")
            if "__pycache__" not in path.parts
        )
    )


@pytest.mark.parametrize(
    "path",
    _python_files(),
    ids=lambda path: str(path.relative_to(PROJECT_ROOT)),
)
def test_production_python_file_compiles(
    path: Path,
    tmp_path: Path,
) -> None:
    relative = path.relative_to(PROJECT_ROOT)
    cache_path = tmp_path / relative.parent / f"{relative.name}.pyc"
    cache_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    py_compile.compile(
        str(path),
        cfile=str(cache_path),
        doraise=True,
    )

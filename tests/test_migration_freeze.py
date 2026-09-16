from __future__ import annotations

import ast
from pathlib import Path

from l2shock.config import PROJECT_ROOT


def _migration_path() -> Path:
    return PROJECT_ROOT / "alembic" / "versions" / "0001_initial.py"


def test_initial_migration_does_not_import_live_orm_models() -> None:
    path = _migration_path()
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)

        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    assert "l2shock.db.models" not in imported_modules
    assert "l2shock.db" not in imported_modules


def test_initial_migration_contains_frozen_application_tables() -> None:
    source = _migration_path().read_text(encoding="utf-8")

    for table_name in (
        "app_settings",
        "data_presets",
        "source_hours",
        "l2_hourly_series",
        "price_hourly_series",
        "fetch_runs",
    ):
        assert f'"{table_name}"' in source


def test_initial_migration_documents_historical_independence() -> None:
    source = _migration_path().read_text(encoding="utf-8")
    normalized_source = " ".join(source.split())

    assert "self-contained" in normalized_source
    assert "must never import live ORM metadata" in normalized_source

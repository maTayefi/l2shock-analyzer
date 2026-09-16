# l2shock/diagnostics.py
"""Installation, storage, database, and retention diagnostics.

All production diagnostics in this module are read-only with respect to
application data.

The existing installation report may create configured runtime directories,
matching startup behavior. Storage inventory, database inventory, and raw
retention planning do not delete files or mutate durable source metadata.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from collections.abc import Mapping

import pyarrow as pa
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from l2shock import __version__
from l2shock.acquisition.retention import plan_processed_raw_retention
from l2shock.config import get_settings
from l2shock.db.engine import get_engine, session_scope
from l2shock.db.models import (
    AppSetting,
    DataPreset,
    FetchRun,
    L2HourlySeries,
    PriceHourlySeries,
    SourceHour,
)
from l2shock.db.schema import expected_alembic_head, verify_schema
from l2shock.maintenance_diagnostics import (
    maintenance_diagnostics_report,
)
from l2shock.timeutils import now_utc, require_aware_utc

_MAX_INVENTORY_ERRORS = 50


def _package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "NOT INSTALLED"


def _safe_exception_name(exc: BaseException) -> str:
    """Return a diagnostic that cannot expose exception arguments or secrets."""

    return f"Unexpected {type(exc).__name__}"


def installation_report() -> dict[str, Any]:
    """Return installation and runtime-foundation diagnostics."""

    settings = get_settings()

    directories: dict[str, dict[str, Any]] = {}

    for name, path in (
        ("raw", settings.storage.raw_path),
        ("cache", settings.storage.cache_path),
        ("quarantine", settings.storage.quarantine_path),
        ("exports", settings.storage.export_path),
        ("logs", settings.storage.log_path),
        ("backups", settings.storage.backup_path),
    ):
        path.mkdir(parents=True, exist_ok=True)

        directories[name] = {
            "path": str(path),
            "exists": path.exists(),
            "is_directory": path.is_dir(),
        }

    database_error: str | None = None
    database_ok = False

    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1"))

        verify_schema(get_engine())
        database_ok = True

    except Exception as exc:
        database_error = _safe_exception_name(exc)

    zstd_available = bool(pa.Codec.is_available("zstd"))

    return {
        "ok": bool(
            sys.version_info[:2] == (3, 14)
            and database_ok
            and zstd_available
            and all(item["is_directory"] for item in directories.values())
        ),
        "application_version": __version__,
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "supported_minor": sys.version_info[:2] == (3, 14),
        },
        "packages": {
            "nicegui": _package_version("nicegui"),
            "sqlalchemy": _package_version("SQLAlchemy"),
            "psycopg": _package_version("psycopg"),
            "alembic": _package_version("alembic"),
            "pyarrow": _package_version("pyarrow"),
            "numpy": _package_version("numpy"),
            "pandas": _package_version("pandas"),
            "httpx": _package_version("httpx"),
            "psutil": _package_version("psutil"),
        },
        "pyarrow_zstd_available": zstd_available,
        "database": {
            "ok": database_ok,
            "database": settings.database.database,
            "host": settings.database.host,
            "port": settings.database.port,
            "expected_alembic_head": expected_alembic_head(),
            "error": database_error,
        },
        "directories": directories,
        "security": {
            "loopback_host": settings.app.host,
            "database_password_configured": bool(
                settings.database.password.get_secret_value()
            ),
            "cryptohft_api_key_configured": bool(
                settings.cryptohft.api_key.get_secret_value()
            ),
            "secrets_printed": False,
        },
    }


def directory_inventory(path: Path) -> dict[str, object]:
    """Return a bounded, non-following inventory for one directory tree.

    Symbolic links are counted but never followed. Individual filesystem
    failures are retained as type-only diagnostics with bounded cardinality.
    """

    root = Path(path).expanduser().resolve()

    if not root.exists():
        return {
            "path": str(root),
            "exists": False,
            "is_directory": False,
            "file_count": 0,
            "directory_count": 0,
            "symlink_count": 0,
            "total_bytes": 0,
            "error_count": 0,
            "errors": [],
        }

    if not root.is_dir():
        return {
            "path": str(root),
            "exists": True,
            "is_directory": False,
            "file_count": 0,
            "directory_count": 0,
            "symlink_count": 0,
            "total_bytes": 0,
            "error_count": 1,
            "errors": [
                {
                    "path": str(root),
                    "error": "NotADirectoryError",
                }
            ],
        }

    file_count = 0
    directory_count = 1
    symlink_count = 0
    total_bytes = 0
    error_count = 0
    errors: list[dict[str, str]] = []

    pending = [root]

    while pending:
        current = pending.pop()

        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            symlink_count += 1
                            continue

                        if entry.is_dir(follow_symlinks=False):
                            directory_count += 1
                            pending.append(Path(entry.path))
                            continue

                        if entry.is_file(follow_symlinks=False):
                            stat_result = entry.stat(follow_symlinks=False)
                            file_count += 1
                            total_bytes += int(stat_result.st_size)

                    except OSError as exc:
                        error_count += 1

                        if len(errors) < _MAX_INVENTORY_ERRORS:
                            errors.append(
                                {
                                    "path": str(entry.path),
                                    "error": type(exc).__name__,
                                }
                            )

        except OSError as exc:
            error_count += 1

            if len(errors) < _MAX_INVENTORY_ERRORS:
                errors.append(
                    {
                        "path": str(current),
                        "error": type(exc).__name__,
                    }
                )

    return {
        "path": str(root),
        "exists": True,
        "is_directory": True,
        "file_count": file_count,
        "directory_count": directory_count,
        "symlink_count": symlink_count,
        "total_bytes": total_bytes,
        "error_count": error_count,
        "errors": errors,
    }


def storage_inventory() -> dict[str, object]:
    """Inventory every configured application-owned storage tree."""

    settings = get_settings()

    directories = {
        name: directory_inventory(path)
        for name, path in (
            ("raw", settings.storage.raw_path),
            ("cache", settings.storage.cache_path),
            ("quarantine", settings.storage.quarantine_path),
            ("exports", settings.storage.export_path),
            ("logs", settings.storage.log_path),
            ("backups", settings.storage.backup_path),
        )
    }

    total_files = sum(int(item["file_count"]) for item in directories.values())
    total_bytes = sum(int(item["total_bytes"]) for item in directories.values())
    total_errors = sum(int(item["error_count"]) for item in directories.values())

    disk: dict[str, object]

    try:
        usage = shutil.disk_usage(settings.storage.raw_path)
        free_gib = usage.free / (1024**3)

        disk = {
            "path": str(settings.storage.raw_path),
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
            "free_bytes": int(usage.free),
            "free_gib": round(free_gib, 3),
            "minimum_free_gib": float(settings.storage.minimum_free_disk_gib),
            "ok": bool(free_gib >= settings.storage.minimum_free_disk_gib),
            "error": None,
        }

    except OSError as exc:
        disk = {
            "path": str(settings.storage.raw_path),
            "total_bytes": None,
            "used_bytes": None,
            "free_bytes": None,
            "free_gib": None,
            "minimum_free_gib": float(settings.storage.minimum_free_disk_gib),
            "ok": False,
            "error": _safe_exception_name(exc),
        }

    return {
        "directories": directories,
        "totals": {
            "file_count": total_files,
            "total_bytes": total_bytes,
            "error_count": total_errors,
        },
        "disk": disk,
    }


def database_inventory(session: Session) -> dict[str, object]:
    """Return compact PostgreSQL row and source-status counts."""

    if not isinstance(session, Session):
        raise TypeError("database_inventory requires a SQLAlchemy Session")

    table_counts = {
        "app_settings": int(
            session.scalar(select(func.count()).select_from(AppSetting)) or 0
        ),
        "data_presets": int(
            session.scalar(select(func.count()).select_from(DataPreset)) or 0
        ),
        "source_hours": int(
            session.scalar(select(func.count()).select_from(SourceHour)) or 0
        ),
        "l2_hourly_series": int(
            session.scalar(select(func.count()).select_from(L2HourlySeries)) or 0
        ),
        "price_hourly_series": int(
            session.scalar(select(func.count()).select_from(PriceHourlySeries)) or 0
        ),
        "fetch_runs": int(
            session.scalar(select(func.count()).select_from(FetchRun)) or 0
        ),
    }

    source_status_counts = {
        str(status): int(count)
        for status, count in session.execute(
            select(
                SourceHour.status,
                func.count(SourceHour.id),
            )
            .group_by(SourceHour.status)
            .order_by(SourceHour.status)
        ).all()
    }

    source_quality_counts = {
        str(quality): int(count)
        for quality, count in session.execute(
            select(
                SourceHour.quality_state,
                func.count(SourceHour.id),
            )
            .group_by(SourceHour.quality_state)
            .order_by(SourceHour.quality_state)
        ).all()
    }

    enabled_presets = int(
        session.scalar(
            select(func.count())
            .select_from(DataPreset)
            .where(DataPreset.enabled.is_(True))
        )
        or 0
    )

    return {
        "table_counts": table_counts,
        "source_status_counts": source_status_counts,
        "source_quality_counts": source_quality_counts,
        "enabled_preset_count": enabled_presets,
    }


def production_diagnostics_report(
    *,
    generated_at_utc: datetime | None = None,
) -> dict[str, object]:
    """Build a complete read-only production diagnostics report."""

    settings = get_settings()
    generated = require_aware_utc(
        "generated_at_utc",
        generated_at_utc or now_utc(),
    )

    installation = installation_report()
    storage = storage_inventory()

    database: dict[str, object]
    retention: dict[str, object]
    maintenance: dict[str, object]

    try:
        with session_scope() as session:
            database = database_inventory(session)

            retention_plan = plan_processed_raw_retention(
                session,
                raw_root=settings.storage.raw_path,
                retention_hours=settings.storage.raw_retention_hours,
                now=generated,
            )
            retention = retention_plan.to_dict()

            maintenance = maintenance_diagnostics_report(
                session,
                cache_root=settings.storage.cache_path,
                generated_at_utc=generated,
            )

    except Exception as exc:
        safe_error = _safe_exception_name(exc)

        database = {
            "error": safe_error,
        }
        retention = {
            "schema": "l2shock.raw_retention_plan",
            "schema_version": 1,
            "error": safe_error,
        }
        maintenance = {
            "schema": "l2shock.maintenance_diagnostics",
            "schema_version": 1,
            "generated_at_utc": generated.isoformat(),
            "ok": False,
            "error": safe_error,
        }

    storage_errors = int(storage["totals"]["error_count"])  # type: ignore[index]
    disk_ok = bool(storage["disk"]["ok"])  # type: ignore[index]

    warnings: list[str] = []

    blocked_count = retention.get("blocked_count")
    if isinstance(blocked_count, int) and blocked_count > 0:
        warnings.append(
            f"{blocked_count} raw-retention item(s) were blocked by "
            "fail-closed verification."
        )

    if storage_errors:
        warnings.append(f"{storage_errors} filesystem inventory error(s) occurred.")

    if not disk_ok:
        warnings.append(
            "Free disk space is below the configured minimum or could "
            "not be measured."
        )

    maintenance_attention = maintenance.get("attention_count")

    if isinstance(maintenance_attention, int) and maintenance_attention > 0:
        warnings.append(
            f"{maintenance_attention} maintenance diagnostic item(s) " "require review."
        )

    if "error" in maintenance:
        warnings.append("Maintenance diagnostics could not be completed.")

    report_ok = bool(
        installation.get("ok") is True
        and "error" not in database
        and "error" not in retention
        and "error" not in maintenance
        and maintenance.get("ok") is True
        and disk_ok
    )

    return {
        "schema": "l2shock.production_diagnostics",
        "schema_version": 1,
        "generated_at_utc": generated.isoformat(),
        "application_version": __version__,
        "ok": report_ok,
        "warnings": warnings,
        "installation": installation,
        "storage": storage,
        "database_inventory": database,
        "raw_retention_dry_run": retention,
        "maintenance_diagnostics": maintenance,
        "maintenance_policy": {
            "raw_deletion_enabled": True,
            "source_metadata_mutation_enabled": True,
            "checkpoint_cleanup_enabled": True,
            "orphan_checkpoint_deletion_enabled": True,
            "stale_source_recovery_enabled": True,
            "analytical_repair_enabled": False,
            "analytical_deletion_enabled": False,
            "automatic_maintenance_enabled": False,
            "explicit_preview_required": True,
            "explicit_confirmation_required": True,
        },
        "security": {
            "exception_arguments_exported": False,
            "database_password_exported": False,
            "cryptohft_api_key_exported": False,
        },
    }


def report_json_bytes(
    report: Mapping[str, object],
) -> bytes:
    """Return deterministic UTF-8 JSON bytes for one diagnostics report."""

    if not isinstance(report, Mapping):
        raise TypeError("report must be a mapping")

    return (
        json.dumps(
            dict(report),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def run_installation_validation() -> int:
    report = installation_report()
    print(json.dumps(report, indent=2, ensure_ascii=False))

    if report["ok"]:
        print()
        print("Installation validation passed.")
        return 0

    print()
    print("Installation validation failed.")
    return 1


if __name__ == "__main__":
    raise SystemExit(run_installation_validation())


__all__ = [
    "database_inventory",
    "directory_inventory",
    "installation_report",
    "production_diagnostics_report",
    "report_json_bytes",
    "run_installation_validation",
    "storage_inventory",
]

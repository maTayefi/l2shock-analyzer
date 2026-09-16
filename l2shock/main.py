# l2shock/main.py
"""Application entry point."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import pkgutil
import shutil
import sys
from pathlib import Path

# Python 3.14 compatibility for NiceGUI transitive packages which may still
# reference the removed pkgutil.find_loader API. This must run before importing
# NiceGUI application modules.
if not hasattr(pkgutil, "find_loader"):

    def find_loader(fullname: str):
        spec = importlib.util.find_spec(fullname)
        return None if spec is None else spec.loader

    pkgutil.find_loader = find_loader  # type: ignore[attr-defined]

from l2shock.config import load_settings
from l2shock.logging_setup import setup_logging

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def purge_logs() -> None:
    """Delete all .log files in the project-root logs/ directory."""
    logs_dir = PROJECT_ROOT / "logs"
    if not logs_dir.is_dir():
        return

    for pattern in ("*.log", "*.log.*"):
        for log_file in logs_dir.glob(pattern):
            try:
                log_file.unlink()
            except Exception as exc:
                # Logging may not be configured yet; print so permission/path errors
                # are visible to the user who requested the purge.
                print(
                    f"Could not delete log file {log_file}: {exc}",
                    file=sys.stderr,
                )


def purge_pycache() -> None:
    """Recursively delete all __pycache__ directories under the project root."""
    for pycache_dir in PROJECT_ROOT.rglob("__pycache__"):
        if pycache_dir.is_dir():
            try:
                shutil.rmtree(pycache_dir)
            except Exception as exc:
                print(
                    f"Could not delete __pycache__ {pycache_dir}: {exc}",
                    file=sys.stderr,
                )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="L2 Liquidity Shock Analyzer",
    )
    parser.add_argument(
        "--purge-logs",
        action="store_true",
        help="Delete all .log files in the logs folder",
    )
    parser.add_argument(
        "--purge-pycache",
        action="store_true",
        help="Delete all __pycache__ folders recursively",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate configuration, database, schema, and storage, then exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()

    # Purge actions first so we don't create new logs before deleting old ones.
    if args.purge_logs:
        purge_logs()
    if args.purge_pycache:
        purge_pycache()

    settings = load_settings()

    setup_logging(
        settings.app.log_level,
        log_dir=settings.storage.log_path,
    )

    if args.validate_only:
        from l2shock.diagnostics import run_installation_validation

        return run_installation_validation()

    from l2shock.ui.app import run_app

    log.info(
        "Starting %s on http://%s:%d",
        settings.app.title,
        settings.app.host,
        settings.app.port,
    )

    run_app()
    return 0


if __name__ == "__main__":
    sys.exit(main())

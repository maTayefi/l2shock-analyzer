# l2shock/logging_setup.py
"""Centralized application logging."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from l2shock.config import PROJECT_ROOT

_HANDLER_MARKER = "_l2shock_handler"


def _remove_owned_handlers(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        if not getattr(handler, _HANDLER_MARKER, False):
            continue

        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass


def setup_logging(
    level: str = "INFO",
    *,
    log_dir: Path | str | None = "logs",
) -> None:
    """Configure console, main-file, and detail-file logging.

    Existing handlers owned by NiceGUI, Uvicorn, pytest, or external tools are
    left intact. Only handlers previously created by this function are removed.
    """
    log_level = getattr(logging, str(level).upper(), logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    _remove_owned_handlers(root)
    root.setLevel(log_level)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    setattr(console, _HANDLER_MARKER, True)
    root.addHandler(console)

    detail_logger = logging.getLogger("l2shock.details")
    _remove_owned_handlers(detail_logger)
    detail_logger.setLevel(logging.DEBUG)
    detail_logger.propagate = False

    if log_dir is None:
        null_handler = logging.NullHandler()
        setattr(null_handler, _HANDLER_MARKER, True)
        detail_logger.addHandler(null_handler)
    else:
        directory = Path(log_dir)
        if not directory.is_absolute():
            directory = PROJECT_ROOT / directory
        directory.mkdir(parents=True, exist_ok=True)

        main_file = RotatingFileHandler(
            directory / "l2shock.log",
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        main_file.setFormatter(formatter)
        setattr(main_file, _HANDLER_MARKER, True)
        root.addHandler(main_file)

        detail_file = RotatingFileHandler(
            directory / "l2shock_detail.log",
            maxBytes=30 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        detail_file.setFormatter(formatter)
        setattr(detail_file, _HANDLER_MARKER, True)
        detail_logger.addHandler(detail_file)

    for noisy_logger in (
        "httpx",
        "httpcore",
        "apscheduler",
        "watchfiles",
    ):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)


__all__ = ["setup_logging"]

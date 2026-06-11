"""Lightweight logging setup shared across pipeline scripts."""

from __future__ import annotations

import logging
import os


def configure_logging(level: str | None = None) -> None:
    """Configure root logger once per process. Idempotent."""
    if logging.getLogger().handlers:
        return
    resolved = level or os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=resolved,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)

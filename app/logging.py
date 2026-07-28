from __future__ import annotations

import logging as py_logging
from logging.handlers import RotatingFileHandler

from app.config import LoggingConfig


def setup_logging(config: LoggingConfig) -> py_logging.Logger:
    handlers: list[py_logging.Handler] = [py_logging.StreamHandler()]
    if config.file:
        handlers.append(RotatingFileHandler(config.file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"))

    py_logging.basicConfig(
        level=getattr(py_logging, config.level, py_logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )
    return py_logging.getLogger("telegram_migration")


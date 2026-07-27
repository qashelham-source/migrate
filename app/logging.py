from __future__ import annotations

import logging as py_logging

from app.config import LoggingConfig


def setup_logging(config: LoggingConfig) -> py_logging.Logger:
    handlers: list[py_logging.Handler] = [py_logging.StreamHandler()]
    if config.file:
        handlers.append(py_logging.FileHandler(config.file, encoding="utf-8"))

    py_logging.basicConfig(
        level=getattr(py_logging, config.level, py_logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )
    return py_logging.getLogger("telegram_migration")


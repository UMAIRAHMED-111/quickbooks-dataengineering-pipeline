"""Shared structured logging helpers for qbo_pipeline."""

from __future__ import annotations

import logging
import os
from typing import Any


class StructuredLogger:
    """Small wrapper to emit consistent event-style log lines."""

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    @staticmethod
    def _fmt_fields(fields: dict[str, Any]) -> str:
        if not fields:
            return ""
        parts: list[str] = []
        for key in sorted(fields.keys()):
            value = fields[key]
            if value is None:
                continue
            text = str(value).replace("\n", "\\n").replace(" ", "_")
            parts.append(f"{key}={text}")
        return " ".join(parts)

    def _emit(self, level: int, event: str, **fields: Any) -> None:
        suffix = self._fmt_fields(fields)
        msg = event if not suffix else f"{event} {suffix}"
        self._logger.log(level, msg)

    def debug(self, event: str, **fields: Any) -> None:
        self._emit(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._emit(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._emit(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._emit(logging.ERROR, event, **fields)

    def exception(self, event: str, **fields: Any) -> None:
        suffix = self._fmt_fields(fields)
        msg = event if not suffix else f"{event} {suffix}"
        self._logger.exception(msg)


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(name)


def configure_logging(*, service: str = "qbo_pipeline") -> None:
    """Configure stdlib logging once using env-configurable level."""
    root = logging.getLogger()
    if root.handlers:
        return
    level_name = (os.getenv("LOG_LEVEL", "INFO") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = (
        "%(asctime)s level=%(levelname)s service="
        + service
        + " logger=%(name)s %(message)s"
    )
    logging.basicConfig(level=level, format=fmt)

"""Structured logging utilities for Tagmarshal lakehouse."""

from __future__ import annotations

import json
import logging
import time
from typing import Any


def get_logger(name: str = "tm") -> logging.Logger:
    """Get or create a logger with simple format."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Log a structured JSON event."""
    payload = {"ts": int(time.time() * 1000), "event": event, **fields}
    logger.info(json.dumps(payload, default=str))

"""Structured logging + run metrics.

Rule of thumb: if you cannot answer "how many rows went in, how many came out,
how many were quarantined and why" from the driver log alone, the job is not
observable. print() does not survive log aggregation; JSON lines do.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import Any

_LOGGER_NAME = "collections_platform"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            # Databricks injects these; they are what lets you join a log line
            # back to a specific job run in system.lakeflow / the Jobs UI.
            "job_id": os.environ.get("DATABRICKS_JOB_ID"),
            "run_id": os.environ.get("DATABRICKS_RUN_ID"),
        }
        if record.__dict__.get("extra_fields"):
            payload.update(record.__dict__["extra_fields"])
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(event: str, **fields: Any) -> None:
    get_logger().info(event, extra={"extra_fields": fields})


@contextmanager
def timed(step: str, **fields: Any):
    """Emit start/end events with a duration. Cheap, and it turns 'the job is
    slow' into 'the dq step is slow'."""
    start = time.monotonic()
    log_event(f"{step}.start", **fields)
    try:
        yield
    except Exception as exc:
        log_event(
            f"{step}.failed",
            duration_s=round(time.monotonic() - start, 2),
            error=type(exc).__name__,
            **fields,
        )
        raise
    log_event(f"{step}.ok", duration_s=round(time.monotonic() - start, 2), **fields)

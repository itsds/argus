"""
Structured logging for Argus.

JSON-formatted logs with correlation IDs for tracing
agent invocations across the system.
"""

import logging
import json
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any


# Context variable for correlation ID — set once per invocation,
# automatically included in every log line from that invocation.
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def set_correlation_id(cid: str | None = None) -> str:
    """Set (or generate) a correlation ID for the current invocation."""
    cid = cid or str(uuid.uuid4())[:12]
    _correlation_id.set(cid)
    return cid


def get_correlation_id() -> str:
    return _correlation_id.get()


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        cid = get_correlation_id()
        if cid:
            log_entry["correlation_id"] = cid

        # Include extra fields passed via logger.info("msg", extra={...})
        for key in ("agent", "tool", "action", "duration_ms", "error"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)

        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = str(record.exc_info[1])

        return json.dumps(log_entry)


def setup_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Configure root logger for Argus."""
    root = logging.getLogger("argus")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    if root.handlers:
        return  # already configured

    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )

    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Get a child logger under the argus namespace."""
    return logging.getLogger(f"argus.{name}")

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Any

from core.value_coercion import utc_iso, utc_now_iso

_RESERVED_LOG_RECORD_KEYS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    }
)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return utc_iso(value) or value.isoformat()
    return str(value)


def _clean_field(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _clean_field(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, (list, tuple)):
        return [_clean_field(item) for item in value]
    return value


class JsonLogFormatter(logging.Formatter):
    def __init__(self, *, service: str, env: str | None = None) -> None:
        super().__init__()
        self.service = service
        self.env = env

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": utc_now_iso(),
            "level": record.levelname.lower(),
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if self.env:
            payload["env"] = self.env

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
                continue
            if value is None:
                continue
            payload[key] = _clean_field(value)

        if record.exc_info:
            payload["exception"] = "".join(
                traceback.format_exception(*record.exc_info)
            ).strip()
        if record.stack_info:
            payload["stack"] = record.stack_info
        return json.dumps(payload, default=_json_default, separators=(",", ":"), sort_keys=True)


def configure_logging(
    *,
    service: str,
    env: str | None = None,
    level: str | None = None,
    force: bool = False,
) -> None:
    resolved_level = str(level or os.environ.get("SPREADS_LOG_LEVEL") or "INFO").upper()
    numeric_level = getattr(logging, resolved_level, logging.INFO)
    resolved_env = env or os.environ.get("SPREADS_DEPLOY_ENV")

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter(service=service, env=resolved_env))
    logging.basicConfig(level=numeric_level, handlers=[handler], force=force)
    logging.captureWarnings(True)

    temporal_level = getattr(
        logging,
        str(os.environ.get("SPREADS_WORKFLOW_PROVIDER_LOG_LEVEL") or "WARNING").upper(),
        logging.WARNING,
    )
    for logger_name in ("temporal", "temporal.worker"):
        temporal_logger = logging.getLogger(logger_name)
        temporal_logger.handlers.clear()
        temporal_logger.setLevel(temporal_level)
        temporal_logger.propagate = True


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    message: str | None = None,
    exc_info: Any = None,
    **fields: Any,
) -> None:
    logger.log(
        level,
        message or event,
        extra={"event": event, **{key: value for key, value in fields.items() if value is not None}},
        exc_info=exc_info,
    )

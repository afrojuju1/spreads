from __future__ import annotations

import json
from typing import Any

from .client import AlpacaRequestError


def _error_detail(response_body: str | None) -> str | None:
    if response_body is None:
        return None
    rendered = response_body.strip()
    if not rendered:
        return None
    try:
        payload = json.loads(rendered)
    except json.JSONDecodeError:
        return rendered
    if isinstance(payload, dict):
        for key in ("message", "error", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return rendered


def classify_alpaca_request_error(exc: AlpacaRequestError) -> dict[str, Any]:
    detail = _error_detail(exc.response_body)
    normalized = (detail or str(exc)).lower()
    reason = "alpaca_request_error"
    terminal = False

    if exc.status_code == 403 and "buying power" in normalized:
        terminal = True
        if "option" in normalized:
            reason = "insufficient_options_buying_power"
        else:
            reason = "insufficient_buying_power"
    elif exc.status_code == 403:
        reason = "broker_account_forbidden"
        terminal = True
    elif exc.status_code == 404:
        reason = "broker_resource_not_found"
        terminal = True
    elif exc.status_code == 422:
        reason = "invalid_broker_order"
        terminal = True
    elif exc.status_code is not None and 400 <= exc.status_code < 500:
        reason = "alpaca_request_rejected"
        terminal = True

    default_message = "Broker request failed."
    if reason == "insufficient_options_buying_power":
        default_message = "Broker rejected open execution for insufficient options buying power."
    elif reason == "insufficient_buying_power":
        default_message = "Broker rejected open execution for insufficient buying power."
    elif reason == "broker_account_forbidden":
        default_message = "Broker rejected open execution for account permission or policy reasons."
    elif reason == "invalid_broker_order":
        default_message = "Broker rejected open execution because the order payload was invalid."
    elif reason == "broker_resource_not_found":
        default_message = "Broker request referenced a resource that no longer exists."
    elif reason == "alpaca_request_rejected":
        default_message = "Broker rejected open execution."

    message = default_message
    if detail:
        message = f"{default_message} {detail}"

    return {
        "reason": reason,
        "message": message,
        "detail": detail,
        "status_code": exc.status_code,
        "terminal": terminal,
    }


__all__ = [
    "classify_alpaca_request_error",
]

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from core.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUSES,
    TERMINAL_ATTEMPT_STATUSES,
)
from core.services.option_structures import (
    candidate_legs,
    legs_identity_key,
    normalize_strategy_family,
    order_payload_legs,
)
from core.value_coercion import (
    as_text,
    coerce_float,
)

BROKER_NAME = "alpaca"
EXECUTION_SCHEMA_MESSAGE = "Execution tables are not available yet. Run the latest Alembic migrations."
OPEN_STATUSES = OPEN_ATTEMPT_STATUSES
TERMINAL_STATUSES = TERMINAL_ATTEMPT_STATUSES
DEFAULT_ENTRY_PRICING_MODE = "adaptive_credit"
DEFAULT_MIN_CREDIT_RETENTION_PCT = 0.95
DEFAULT_MAX_CREDIT_CONCESSION = 0.02


def _execution_attempt_id() -> str:
    return f"execution:{uuid4().hex}"


def _execution_client_order_id() -> str:
    return f"spr-exec-{uuid4().hex[:20]}"


def _order_intent_key(execution_attempt_id: str) -> str:
    return f"order_intent:{execution_attempt_id}"


def _policy_version_token(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:12]


def _policy_ref(
    *,
    family: str,
    resolved_policy: dict[str, Any],
    source_kind: str,
    source_key: str,
    source_job_key: str | None,
    source_job_run_id: str | None,
    version_token: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "family": family,
        "key": source_key,
        "version": version_token or _policy_version_token(resolved_policy),
        "source_kind": source_kind,
        "source_job_key": source_job_key,
        "source_job_run_id": source_job_run_id,
    }
    if extra:
        payload.update(extra)
    return payload


def _candidate_with_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = candidate.get("candidate")
    if isinstance(payload, dict):
        return {
            **dict(candidate),
            **dict(payload),
        }
    return dict(candidate)


def _strategy_family_from_payload(payload: Mapping[str, Any]) -> str:
    return normalize_strategy_family(as_text(payload.get("strategy_family")) or as_text(payload.get("strategy")))


def _execution_attempt_identity(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    request_order = dict(request.get("order") or {}) if isinstance(request, Mapping) else {}
    candidate_payload = dict(attempt.get("candidate") or {}) if isinstance(attempt.get("candidate"), Mapping) else {}
    legs = order_payload_legs(
        request_order,
        expiration_date=as_text(attempt.get("expiration_date")),
    ) or candidate_legs(candidate_payload)
    if not legs:
        return None
    strategy = (
        as_text(attempt.get("strategy_family"))
        or as_text(attempt.get("strategy"))
        or as_text(candidate_payload.get("strategy_family"))
        or as_text(candidate_payload.get("strategy"))
    )
    return legs_identity_key(strategy=strategy, legs=legs)


def _normalize_limit_value(value: Any) -> float | None:
    numeric = coerce_float(value)
    if numeric is None or numeric == 0:
        return None
    return abs(numeric)


def _resolve_completed_at(order: dict[str, Any]) -> str | None:
    for key in ("filled_at", "canceled_at", "expired_at", "failed_at", "updated_at"):
        value = as_text(order.get(key))
        if value:
            return value
    return None

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from core.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUSES,
    TERMINAL_ATTEMPT_STATUSES,
    is_terminal_execution_attempt_status,
    resolve_execution_submit_job_run_id,
)
from core.services.option_structures import (
    candidate_legs,
    legs_identity_key,
    normalize_legs,
    normalize_strategy_family,
)
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
)

BROKER_NAME = "alpaca"
EXECUTION_SCHEMA_MESSAGE = (
    "Execution tables are not available yet. Run the latest Alembic migrations."
)
OPEN_STATUSES = OPEN_ATTEMPT_STATUSES
TERMINAL_STATUSES = TERMINAL_ATTEMPT_STATUSES
DEFAULT_ENTRY_PRICING_MODE = "adaptive_credit"
DEFAULT_MIN_CREDIT_RETENTION_PCT = 0.95
DEFAULT_MAX_CREDIT_CONCESSION = 0.02
ATTEMPT_CONTEXT_BUCKET_MIRROR = {
    "open_promotable": "promotable",
    "open_monitor": "monitor",
    "position_close": "position_close",
}


def _normalize_attempt_context(value: Any) -> str | None:
    normalized = _as_text(value)
    if normalized == "promotable":
        return "open_promotable"
    if normalized == "monitor":
        return "open_monitor"
    return normalized


def _deprecated_bucket(value: Any) -> str | None:
    normalized = _as_text(value)
    if normalized is None:
        return None
    return ATTEMPT_CONTEXT_BUCKET_MIRROR.get(normalized, normalized)


def _execution_attempt_id() -> str:
    return f"execution:{uuid4().hex}"


def _risk_decision_id() -> str:
    return f"risk_decision:{uuid4().hex}"


def _execution_client_order_id() -> str:
    return f"spr-exec-{uuid4().hex[:20]}"


def _execution_submit_job_run_id(execution_attempt_id: str) -> str:
    return resolve_execution_submit_job_run_id(execution_attempt_id)


def _order_intent_key(execution_attempt_id: str) -> str:
    return f"order_intent:{execution_attempt_id}"


def _is_terminal_status(status: str | None) -> bool:
    return is_terminal_execution_attempt_status(status)


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
    return normalize_strategy_family(
        _as_text(payload.get("strategy_family")) or _as_text(payload.get("strategy"))
    )


def _execution_attempt_identity(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    request_order = (
        dict(request.get("order") or {}) if isinstance(request, Mapping) else {}
    )
    candidate_payload = (
        dict(attempt.get("candidate") or {})
        if isinstance(attempt.get("candidate"), Mapping)
        else {}
    )
    legs = normalize_legs(request_order.get("legs")) or candidate_legs(
        candidate_payload
    )
    if not legs:
        return None
    strategy = (
        _as_text(attempt.get("strategy_family"))
        or _as_text(attempt.get("strategy"))
        or _as_text(candidate_payload.get("strategy_family"))
        or _as_text(candidate_payload.get("strategy"))
    )
    return legs_identity_key(strategy=strategy, legs=legs)


def _clamp_fraction(
    value: float, *, minimum: float = 0.0, maximum: float = 1.0
) -> float:
    return max(minimum, min(maximum, float(value)))


def _normalize_limit_value(value: Any) -> float | None:
    numeric = _coerce_float(value)
    if numeric is None or numeric == 0:
        return None
    return abs(numeric)


def _resolve_completed_at(order: dict[str, Any]) -> str | None:
    for key in ("filled_at", "canceled_at", "expired_at", "failed_at", "updated_at"):
        value = _as_text(order.get(key))
        if value:
            return value
    return None

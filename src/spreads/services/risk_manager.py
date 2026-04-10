from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from spreads.services.alpaca import create_alpaca_client_from_env, resolve_trading_environment
from spreads.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ["open", "partial_close"]

DEFAULT_RISK_POLICY = {
    "enabled": True,
    "allow_live": False,
    "max_open_positions_per_session": 1,
    "max_open_positions_per_underlying": 1,
    "max_open_positions_per_underlying_strategy": 1,
    "max_contracts_per_position": 1,
    "max_contracts_per_session": 1,
    "max_position_notional": 1000.0,
    "max_session_notional": 1000.0,
    "max_position_max_loss": 1000.0,
    "max_session_max_loss": 1000.0,
    "stale_quote_after_seconds": 900,
}

OPTIONAL_FLOAT_POLICY_KEYS = {
    "max_position_notional",
    "max_session_notional",
    "max_position_max_loss",
    "max_session_max_loss",
    "stale_quote_after_seconds",
}
INT_POLICY_KEYS = {
    "max_open_positions_per_session",
    "max_open_positions_per_underlying",
    "max_open_positions_per_underlying_strategy",
    "max_contracts_per_position",
    "max_contracts_per_session",
}
FLOAT_POLICY_KEYS = OPTIONAL_FLOAT_POLICY_KEYS
BOOL_POLICY_KEYS = {"enabled", "allow_live"}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _candidate_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = candidate.get("candidate")
    return dict(payload) if isinstance(payload, dict) else {}


def normalize_risk_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("risk_policy") if isinstance(source.get("risk_policy"), dict) else source

    policy = dict(DEFAULT_RISK_POLICY)
    stale_quote_after_seconds = _coerce_float(
        raw_policy.get("stale_quote_after_seconds", raw_policy.get("max_candidate_age_seconds"))
    )
    if stale_quote_after_seconds is not None:
        policy["stale_quote_after_seconds"] = stale_quote_after_seconds

    duplicate_underlying_strategy_limit = _coerce_int(
        raw_policy.get(
            "max_open_positions_per_underlying_strategy",
            raw_policy.get("duplicate_underlying_strategy_limit"),
        )
    )
    if duplicate_underlying_strategy_limit is not None:
        policy["max_open_positions_per_underlying_strategy"] = duplicate_underlying_strategy_limit

    for key in BOOL_POLICY_KEYS:
        if key in raw_policy:
            policy[key] = _coerce_bool(raw_policy[key])
    for key in INT_POLICY_KEYS:
        if key not in raw_policy:
            continue
        parsed = _coerce_int(raw_policy[key])
        if parsed is not None:
            policy[key] = parsed
    for key in FLOAT_POLICY_KEYS:
        if key not in raw_policy:
            continue
        value = raw_policy[key]
        if value is None:
            policy[key] = None
            continue
        parsed = _coerce_float(value)
        if parsed is not None:
            policy[key] = parsed

    policy["enabled"] = bool(policy["enabled"])
    policy["allow_live"] = bool(policy["allow_live"])
    return policy


def _current_trading_environment() -> str:
    client = create_alpaca_client_from_env()
    return resolve_trading_environment(client.trading_base_url)


def _candidate_entry_notional(candidate: dict[str, Any], quantity: int, price: float | None) -> float | None:
    entry_price = price
    if entry_price is None or entry_price <= 0:
        entry_price = _coerce_float(_candidate_payload(candidate).get("midpoint_credit"))
    if entry_price is None or entry_price <= 0:
        return None
    return round(entry_price * 100.0 * quantity, 2)


def _candidate_max_loss(candidate: dict[str, Any], quantity: int) -> float | None:
    candidate_payload = _candidate_payload(candidate)
    max_loss = _coerce_float(candidate_payload.get("max_loss"))
    if max_loss is None:
        width = _coerce_float(candidate_payload.get("width"))
        midpoint_credit = _coerce_float(candidate_payload.get("midpoint_credit"))
        if width is not None and midpoint_credit is not None:
            max_loss = max(width - midpoint_credit, 0.0) * 100.0
    if max_loss is None:
        return None
    return round(max_loss * quantity, 2)


def _open_positions(execution_store: Any, *, session_id: str) -> list[dict[str, Any]]:
    return [
        dict(position)
        for position in execution_store.list_session_positions(
            session_id=session_id,
            statuses=OPEN_POSITION_STATUSES,
            limit=200,
        )
    ]


def _session_position_metrics(positions: list[dict[str, Any]]) -> dict[str, float]:
    return {
        "open_position_count": float(len(positions)),
        "open_contract_count": sum(_coerce_float(position.get("remaining_quantity")) or 0.0 for position in positions),
        "entry_notional_total": sum(_coerce_float(position.get("entry_notional")) or 0.0 for position in positions),
        "max_loss_total": sum(_coerce_float(position.get("max_loss")) or 0.0 for position in positions),
    }


def _kill_switch_reason() -> str | None:
    if _coerce_bool(os.environ.get("SPREADS_EXECUTION_KILL_SWITCH")):
        return "Execution is blocked by SPREADS_EXECUTION_KILL_SWITCH."
    return None


def resolve_execution_kill_switch_reason() -> str | None:
    return _kill_switch_reason()


def _environment_reason(normalized_policy: dict[str, Any]) -> str | None:
    environment = _current_trading_environment()
    allow_live_env = _coerce_bool(os.environ.get("SPREADS_ALLOW_LIVE_TRADING"))
    if environment == "live" and not (normalized_policy["allow_live"] and allow_live_env):
        return (
            "Open execution is blocked on a live Alpaca account. "
            "Set SPREADS_ALLOW_LIVE_TRADING=true and allow_live=true to enable it."
        )
    return None


def _candidate_timestamp(candidate: dict[str, Any], cycle: dict[str, Any]) -> datetime | None:
    candidate_generated_at = parse_datetime(
        _as_text(candidate.get("generated_at")) or _as_text(cycle.get("generated_at"))
    )
    return candidate_generated_at


def assess_position_risk(
    *,
    position: dict[str, Any],
    risk_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy or position.get("risk_policy"))
    remaining_quantity = _coerce_float(position.get("remaining_quantity")) or 0.0
    if str(position.get("status") or "") == "closed" or remaining_quantity <= 0:
        return {
            "status": "ok",
            "note": "Position is closed.",
            "policy": normalized_policy,
        }
    if not normalized_policy["enabled"]:
        return {
            "status": "disabled",
            "note": "Risk policy snapshot is disabled for this position.",
            "policy": normalized_policy,
        }

    reasons: list[str] = []
    max_contracts_per_position = _coerce_int(normalized_policy.get("max_contracts_per_position"))
    if max_contracts_per_position is not None and remaining_quantity > max_contracts_per_position:
        reasons.append("remaining quantity exceeds max_contracts_per_position")

    entry_notional = _coerce_float(position.get("entry_notional"))
    max_position_notional = _coerce_float(normalized_policy.get("max_position_notional"))
    if entry_notional is not None and max_position_notional is not None and entry_notional > max_position_notional:
        reasons.append("entry notional exceeds max_position_notional")

    max_loss = _coerce_float(position.get("max_loss"))
    max_position_max_loss = _coerce_float(normalized_policy.get("max_position_max_loss"))
    if max_loss is not None and max_position_max_loss is not None and max_loss > max_position_max_loss:
        reasons.append("max loss exceeds max_position_max_loss")

    if reasons:
        return {
            "status": "breach",
            "note": "; ".join(reasons),
            "policy": normalized_policy,
        }
    return {
        "status": "ok",
        "note": "Position is within its snapshotted risk limits.",
        "policy": normalized_policy,
    }


def build_session_risk_snapshot(
    *,
    execution_store: Any,
    session_id: str,
    risk_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)

    if hasattr(execution_store, "positions_schema_ready") and not execution_store.positions_schema_ready():
        return {
            "status": "unknown",
            "note": "Session position storage is not available yet.",
            "policy": normalized_policy,
        }

    kill_switch_reason = _kill_switch_reason()
    if kill_switch_reason is not None:
        return {
            "status": "blocked",
            "note": kill_switch_reason,
            "policy": normalized_policy,
        }

    try:
        environment_reason = _environment_reason(normalized_policy)
    except Exception as exc:
        return {
            "status": "unknown",
            "note": f"Could not resolve the trading environment: {exc}",
            "policy": normalized_policy,
        }
    if environment_reason is not None:
        return {
            "status": "blocked",
            "note": environment_reason,
            "policy": normalized_policy,
        }

    if not normalized_policy["enabled"]:
        return {
            "status": "disabled",
            "note": "Risk policy is disabled for this session.",
            "policy": normalized_policy,
        }

    open_positions = _open_positions(execution_store, session_id=session_id)
    metrics = _session_position_metrics(open_positions)
    reasons: list[str] = []

    if metrics["open_position_count"] >= float(normalized_policy["max_open_positions_per_session"]):
        reasons.append("max_open_positions_per_session reached")
    if metrics["open_contract_count"] >= float(normalized_policy["max_contracts_per_session"]):
        reasons.append("max_contracts_per_session reached")

    max_session_notional = _coerce_float(normalized_policy.get("max_session_notional"))
    if max_session_notional is not None and metrics["entry_notional_total"] >= max_session_notional:
        reasons.append("max_session_notional reached")

    max_session_max_loss = _coerce_float(normalized_policy.get("max_session_max_loss"))
    if max_session_max_loss is not None and metrics["max_loss_total"] >= max_session_max_loss:
        reasons.append("max_session_max_loss reached")

    if reasons:
        return {
            "status": "blocked",
            "note": "; ".join(reasons),
            "policy": normalized_policy,
            "metrics": metrics,
        }
    return {
        "status": "ok",
        "note": "Session can submit new executions under the current risk policy.",
        "policy": normalized_policy,
        "metrics": metrics,
    }


def evaluate_open_execution(
    *,
    execution_store: Any,
    session_id: str,
    candidate: dict[str, Any],
    cycle: dict[str, Any],
    quantity: int,
    limit_price: float | None,
    risk_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)
    open_positions = _open_positions(execution_store, session_id=session_id)
    session_metrics = _session_position_metrics(open_positions)
    position_notional = _candidate_entry_notional(candidate, quantity, limit_price)
    position_max_loss = _candidate_max_loss(candidate, quantity)
    candidate_timestamp = _candidate_timestamp(candidate, cycle)
    candidate_age_seconds = None
    if candidate_timestamp is not None:
        candidate_age_seconds = round((datetime.now(UTC) - candidate_timestamp).total_seconds(), 3)
    underlying_symbol = str(candidate["underlying_symbol"])
    strategy = str(candidate["strategy"])
    matching_underlyings = [
        position for position in open_positions if str(position.get("underlying_symbol")) == underlying_symbol
    ]
    matching_strategy = [
        position
        for position in matching_underlyings
        if str(position.get("strategy")) == strategy
    ]
    session_notional = sum(_coerce_float(position.get("entry_notional")) or 0.0 for position in open_positions)
    session_max_loss = sum(_coerce_float(position.get("max_loss")) or 0.0 for position in open_positions)
    metrics = {
        **session_metrics,
        "requested_quantity": int(quantity),
        "requested_limit_price": limit_price,
        "candidate_age_seconds": candidate_age_seconds,
        "position_notional": position_notional,
        "position_max_loss": position_max_loss,
        "session_notional_before": round(session_notional, 2),
        "session_notional_after": (
            None if position_notional is None else round(session_notional + position_notional, 2)
        ),
        "session_max_loss_before": round(session_max_loss, 2),
        "session_max_loss_after": (
            None if position_max_loss is None else round(session_max_loss + position_max_loss, 2)
        ),
        "matching_underlying_count": len(matching_underlyings),
        "matching_underlying_strategy_count": len(matching_strategy),
    }

    kill_switch_reason = _kill_switch_reason()
    if kill_switch_reason is not None:
        return {
            "status": "blocked",
            "note": kill_switch_reason,
            "reason_codes": ["kill_switch_enabled"],
            "blockers": ["kill_switch_enabled"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    try:
        environment_reason = _environment_reason(normalized_policy)
    except Exception as exc:
        return {
            "status": "unknown",
            "note": f"Could not resolve the trading environment: {exc}",
            "reason_codes": ["environment_resolution_failed"],
            "blockers": ["environment_resolution_failed"],
            "policy": normalized_policy,
            "metrics": metrics,
        }
    if environment_reason is not None:
        return {
            "status": "blocked",
            "note": environment_reason,
            "reason_codes": ["live_environment_blocked"],
            "blockers": ["live_environment_blocked"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if not normalized_policy["enabled"]:
        return {
            "status": "approved",
            "note": "Risk policy is disabled for this submission.",
            "reason_codes": ["risk_policy_disabled"],
            "blockers": [],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if quantity > int(normalized_policy["max_contracts_per_position"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_position.",
            "reason_codes": ["max_contracts_per_position_exceeded"],
            "blockers": ["max_contracts_per_position_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    stale_quote_after_seconds = _coerce_float(normalized_policy.get("stale_quote_after_seconds"))
    if (
        candidate_age_seconds is not None
        and stale_quote_after_seconds is not None
        and candidate_age_seconds > stale_quote_after_seconds
    ):
        return {
            "status": "blocked",
            "note": "Open execution is blocked because the quote snapshot is stale.",
            "reason_codes": ["stale_quote_snapshot"],
            "blockers": ["stale_quote_snapshot"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(open_positions) >= int(normalized_policy["max_open_positions_per_session"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_session.",
            "reason_codes": ["max_open_positions_per_session_exceeded"],
            "blockers": ["max_open_positions_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_underlyings) >= int(normalized_policy["max_open_positions_per_underlying"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying.",
            "reason_codes": ["max_open_positions_per_underlying_exceeded"],
            "blockers": ["max_open_positions_per_underlying_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_strategy) >= int(normalized_policy["max_open_positions_per_underlying_strategy"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying_strategy.",
            "reason_codes": ["max_open_positions_per_underlying_strategy_exceeded"],
            "blockers": ["max_open_positions_per_underlying_strategy_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    open_contracts = sum(_coerce_float(position.get("remaining_quantity")) or 0.0 for position in open_positions)
    if open_contracts + quantity > float(normalized_policy["max_contracts_per_session"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_session.",
            "reason_codes": ["max_contracts_per_session_exceeded"],
            "blockers": ["max_contracts_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_position_notional = _coerce_float(normalized_policy.get("max_position_notional"))
    if position_notional is not None and max_position_notional is not None and position_notional > max_position_notional:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_position_notional.",
            "reason_codes": ["max_position_notional_exceeded"],
            "blockers": ["max_position_notional_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_session_notional = _coerce_float(normalized_policy.get("max_session_notional"))
    if (
        position_notional is not None
        and max_session_notional is not None
        and session_notional + position_notional > max_session_notional
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_session_notional.",
            "reason_codes": ["max_session_notional_exceeded"],
            "blockers": ["max_session_notional_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_position_max_loss = _coerce_float(normalized_policy.get("max_position_max_loss"))
    if (
        position_max_loss is not None
        and max_position_max_loss is not None
        and position_max_loss > max_position_max_loss
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_position_max_loss.",
            "reason_codes": ["max_position_max_loss_exceeded"],
            "blockers": ["max_position_max_loss_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_session_max_loss = _coerce_float(normalized_policy.get("max_session_max_loss"))
    if (
        position_max_loss is not None
        and max_session_max_loss is not None
        and session_max_loss + position_max_loss > max_session_max_loss
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_session_max_loss.",
            "reason_codes": ["max_session_max_loss_exceeded"],
            "blockers": ["max_session_max_loss_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    return {
        "status": "approved",
        "note": "Open execution is approved under the current risk policy.",
        "reason_codes": ["approved"],
        "blockers": [],
        "policy": normalized_policy,
        "metrics": metrics,
    }


def validate_open_execution(
    *,
    execution_store: Any,
    session_id: str,
    candidate: dict[str, Any],
    cycle: dict[str, Any],
    quantity: int,
    limit_price: float | None,
    risk_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    decision = evaluate_open_execution(
        execution_store=execution_store,
        session_id=session_id,
        candidate=candidate,
        cycle=cycle,
        quantity=quantity,
        limit_price=limit_price,
        risk_policy=risk_policy,
    )
    if decision["status"] in {"blocked", "unknown"}:
        raise ValueError(str(decision["note"]))
    return dict(decision["policy"])


def validate_close_execution(
    *,
    position: dict[str, Any],
    quantity: int,
    limit_price: float | None = None,
) -> dict[str, Any]:
    remaining_quantity = _coerce_float(position.get("remaining_quantity"))
    if remaining_quantity is None or remaining_quantity <= 0:
        raise ValueError("Session position does not have remaining quantity to close.")
    if quantity <= 0:
        raise ValueError("Close quantity must be positive.")
    if quantity > remaining_quantity:
        raise ValueError("Close quantity exceeds the remaining session position quantity.")
    if limit_price is not None and limit_price <= 0:
        raise ValueError("Close execution requires a positive limit price.")
    if _as_text(position.get("short_symbol")) is None or _as_text(position.get("long_symbol")) is None:
        raise ValueError("Session position is missing the broker symbols required to close.")
    return {
        "status": "ok",
    }

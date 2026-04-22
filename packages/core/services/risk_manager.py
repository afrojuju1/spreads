from __future__ import annotations

from collections.abc import Mapping
import os
from datetime import UTC, datetime
from typing import Any

from core.services.alpaca import (
    create_alpaca_client_from_env,
    resolve_trading_environment,
)
from core.services.deployment_policy import (
    live_deployment_block_reason,
    resolve_execution_deployment_mode,
)
from core.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUS_LIST,
    is_open_execution_attempt_status,
    resolve_execution_attempt_filled_quantity,
)
from core.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    position_legs,
)
from core.services.positions import enrich_position_row
from core.services.runtime_identity import parse_live_run_scope_id
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
)
from core.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ["open", "partial_close"]
SIZED_SINGLE_LEG_STRATEGIES = {"short_call", "short_put"}

DEFAULT_RISK_POLICY = {
    "enabled": True,
    "allow_live": False,
    "max_open_positions_per_session": 20,
    "max_open_positions_per_underlying": 1,
    "max_open_positions_per_underlying_strategy": 1,
    "max_contracts_per_position": 1,
    "max_contracts_per_session": 20,
    "max_position_notional": 1000.0,
    "max_session_notional": 1000.0,
    "max_position_max_loss": 1000.0,
    "max_session_max_loss": 1000.0,
    "stale_quote_after_seconds": 900,
}
DEFAULT_RISK_POLICY_FLAGS = {
    "max_contracts_per_position_configured": False,
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
    if isinstance(payload, dict):
        return dict(payload)
    if not isinstance(candidate, dict):
        return {}
    merged = dict(candidate)
    economics = candidate.get("economics")
    if isinstance(economics, Mapping):
        merged = {
            **merged,
            **dict(economics),
        }
    return merged


def _candidate_with_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = _candidate_payload(candidate)
    if not isinstance(candidate, dict):
        return payload
    return {
        **dict(candidate),
        **payload,
    }


def normalize_risk_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = (
        source.get("risk_policy")
        if isinstance(source.get("risk_policy"), dict)
        else source
    )

    policy = dict(DEFAULT_RISK_POLICY)
    policy.update(DEFAULT_RISK_POLICY_FLAGS)
    stale_quote_after_seconds = _coerce_float(
        raw_policy.get(
            "stale_quote_after_seconds", raw_policy.get("max_candidate_age_seconds")
        )
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
        policy["max_open_positions_per_underlying_strategy"] = (
            duplicate_underlying_strategy_limit
        )

    for key in BOOL_POLICY_KEYS:
        if key in raw_policy:
            policy[key] = _coerce_bool(raw_policy[key])
    for key in INT_POLICY_KEYS:
        if key not in raw_policy:
            continue
        parsed = _coerce_int(raw_policy[key])
        if parsed is not None:
            policy[key] = parsed
            if key == "max_contracts_per_position":
                policy["max_contracts_per_position_configured"] = True
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


def _candidate_entry_notional(
    candidate: dict[str, Any], quantity: float, price: float | None
) -> float | None:
    entry_price = price
    if entry_price is None or entry_price <= 0:
        payload = _candidate_payload(candidate)
        entry_price = _coerce_float(
            payload.get("midpoint_credit")
            or payload.get("midpoint_debit")
            or payload.get("midpoint_value")
        )
    if entry_price is None or entry_price <= 0:
        return None
    return round(entry_price * 100.0 * quantity, 2)


def _candidate_max_loss(candidate: dict[str, Any], quantity: float) -> float | None:
    candidate_payload = _candidate_payload(candidate)
    max_loss = _coerce_float(candidate_payload.get("max_loss"))
    if max_loss is None:
        width = _coerce_float(candidate_payload.get("width"))
        midpoint_value = _coerce_float(
            candidate_payload.get("midpoint_credit")
            or candidate_payload.get("midpoint_debit")
            or candidate_payload.get("midpoint_value")
        )
        premium_kind = net_premium_kind(candidate_payload.get("strategy"))
        if width is not None and midpoint_value is not None:
            if premium_kind == "debit":
                max_loss = midpoint_value * 100.0
            else:
                max_loss = max(width - midpoint_value, 0.0) * 100.0
    if max_loss is None:
        return None
    return round(max_loss * quantity, 2)


def _max_contracts_for_budget(
    unit_exposure: float | None,
    budget: float | None,
) -> int | None:
    if (
        unit_exposure is None
        or unit_exposure <= 0
        or budget is None
        or budget < 0
    ):
        return None
    return max(int(budget // unit_exposure), 0)


def strategy_supports_position_sizing(strategy_family: Any) -> bool:
    normalized = str(strategy_family or "").strip().lower()
    return normalized in SIZED_SINGLE_LEG_STRATEGIES


def build_candidate_position_sizing(
    *,
    candidate: dict[str, Any],
    limit_price: float | None,
    max_contracts_per_position: int | None = None,
    remaining_session_contracts: int | None = None,
    max_position_notional: float | None = None,
    remaining_session_notional: float | None = None,
    max_position_max_loss: float | None = None,
    remaining_session_max_loss: float | None = None,
    strategy_risk_budget: float | None = None,
) -> dict[str, Any]:
    candidate_payload = _candidate_payload(candidate)
    strategy_family = str(
        candidate_payload.get("strategy")
        or candidate.get("strategy")
        or candidate.get("strategy_family")
        or ""
    ).strip()
    applies = strategy_supports_position_sizing(strategy_family)
    per_contract_entry_notional = _candidate_entry_notional(candidate, 1.0, limit_price)
    per_contract_max_loss = _candidate_max_loss(candidate, 1.0)
    constraints: list[tuple[str, int]] = []

    def _append_constraint(name: str, value: int | None) -> None:
        if value is None:
            return
        constraints.append((name, max(int(value), 0)))

    _append_constraint("max_contracts_per_position", max_contracts_per_position)
    _append_constraint("max_contracts_per_session", remaining_session_contracts)
    _append_constraint(
        "max_position_notional",
        _max_contracts_for_budget(per_contract_entry_notional, max_position_notional),
    )
    _append_constraint(
        "max_session_notional",
        _max_contracts_for_budget(
            per_contract_entry_notional,
            remaining_session_notional,
        ),
    )
    _append_constraint(
        "max_position_max_loss",
        _max_contracts_for_budget(per_contract_max_loss, max_position_max_loss),
    )
    _append_constraint(
        "max_session_max_loss",
        _max_contracts_for_budget(
            per_contract_max_loss,
            remaining_session_max_loss,
        ),
    )
    _append_constraint(
        "max_risk_per_trade",
        _max_contracts_for_budget(per_contract_max_loss, strategy_risk_budget),
    )

    recommended_quantity = 1
    limiting_constraint = None
    if applies and constraints:
        limiting_constraint, recommended_quantity = min(
            constraints,
            key=lambda item: (item[1], item[0]),
        )

    recommended_entry_notional = (
        None
        if per_contract_entry_notional is None
        else round(per_contract_entry_notional * recommended_quantity, 2)
    )
    recommended_max_loss = (
        None
        if per_contract_max_loss is None
        else round(per_contract_max_loss * recommended_quantity, 2)
    )
    return {
        "applies": applies,
        "strategy_family": strategy_family or None,
        "per_contract_entry_notional": per_contract_entry_notional,
        "per_contract_max_loss": per_contract_max_loss,
        "constraints": {name: value for name, value in constraints},
        "limiting_constraint": limiting_constraint,
        "recommended_quantity": int(recommended_quantity),
        "recommended_entry_notional": recommended_entry_notional,
        "recommended_max_loss": recommended_max_loss,
    }


def _effective_max_contracts_per_position(
    *,
    candidate: dict[str, Any],
    normalized_policy: dict[str, Any],
) -> int | None:
    max_contracts_per_position = _coerce_int(
        normalized_policy.get("max_contracts_per_position")
    )
    if max_contracts_per_position is None:
        return None
    if bool(normalized_policy.get("max_contracts_per_position_configured")):
        return max_contracts_per_position
    strategy_family = (
        _candidate_payload(candidate).get("strategy")
        or candidate.get("strategy")
        or candidate.get("strategy_family")
    )
    if strategy_supports_position_sizing(strategy_family):
        return None
    return max_contracts_per_position


def build_open_candidate_position_sizing(
    *,
    execution_store: Any,
    session_id: str,
    candidate: dict[str, Any],
    limit_price: float | None,
    risk_policy: dict[str, Any] | None,
    strategy_risk_budget: float | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)
    open_positions = _open_positions(execution_store, session_id=session_id)
    open_attempts = _open_attempts(execution_store, session_id=session_id)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    session_metrics = _session_open_metrics(open_positions, pending_attempts)
    session_notional = session_metrics["active_entry_notional_total"]
    session_max_loss = session_metrics["active_max_loss_total"]
    open_contracts = session_metrics["active_open_contract_count"]
    return build_candidate_position_sizing(
        candidate=candidate,
        limit_price=limit_price,
        max_contracts_per_position=_effective_max_contracts_per_position(
            candidate=candidate,
            normalized_policy=normalized_policy,
        ),
        remaining_session_contracts=max(
            int(normalized_policy["max_contracts_per_session"] - open_contracts),
            0,
        ),
        max_position_notional=_coerce_float(
            normalized_policy.get("max_position_notional")
        ),
        remaining_session_notional=(
            None
            if _coerce_float(normalized_policy.get("max_session_notional")) is None
            else max(
                float(_coerce_float(normalized_policy.get("max_session_notional")) or 0.0)
                - session_notional,
                0.0,
            )
        ),
        max_position_max_loss=_coerce_float(
            normalized_policy.get("max_position_max_loss")
        ),
        remaining_session_max_loss=(
            None
            if _coerce_float(normalized_policy.get("max_session_max_loss")) is None
            else max(
                float(_coerce_float(normalized_policy.get("max_session_max_loss")) or 0.0)
                - session_max_loss,
                0.0,
            )
        ),
        strategy_risk_budget=strategy_risk_budget,
    )


def _open_positions(execution_store: Any, *, session_id: str) -> list[dict[str, Any]]:
    resolved = parse_live_run_scope_id(session_id)
    if resolved is None:
        return []
    return [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
            pipeline_id=f"pipeline:{resolved['label']}",
            market_date=resolved["market_date"],
            statuses=OPEN_POSITION_STATUSES,
            limit=200,
        )
    ]


def _open_attempts(execution_store: Any, *, session_id: str) -> list[dict[str, Any]]:
    list_for_status = getattr(execution_store, "list_session_attempts_by_status", None)
    if callable(list_for_status):
        rows = list_for_status(
            session_id=session_id,
            statuses=list(OPEN_ATTEMPT_STATUS_LIST),
            trade_intent="open",
            limit=200,
        )
        return [dict(row) for row in rows]

    list_attempts = getattr(execution_store, "list_attempts", None)
    if not callable(list_attempts):
        return []
    rows = list_attempts(session_id=session_id, limit=200)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        if str(payload.get("trade_intent") or "").lower() != "open":
            continue
        if not is_open_execution_attempt_status(payload.get("status")):
            continue
        filtered.append(payload)
    return filtered


def _pending_open_attempt_quantity(attempt: Mapping[str, Any]) -> float:
    requested_quantity = _coerce_float(attempt.get("quantity")) or 0.0
    if requested_quantity <= 0:
        return 0.0
    filled_quantity = min(
        resolve_execution_attempt_filled_quantity(attempt),
        requested_quantity,
    )
    return max(requested_quantity - filled_quantity, 0.0)


def _pending_open_attempt_exposures(
    attempts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    exposures: list[dict[str, Any]] = []
    for attempt in attempts:
        filled_quantity = resolve_execution_attempt_filled_quantity(attempt)
        pending_quantity = _pending_open_attempt_quantity(attempt)
        if pending_quantity <= 0:
            continue
        candidate = attempt.get("candidate")
        candidate_payload = dict(candidate) if isinstance(candidate, Mapping) else {}
        linked_position_id = _as_text(attempt.get("position_id"))
        exposures.append(
            {
                "execution_attempt_id": _as_text(attempt.get("execution_attempt_id")),
                "underlying_symbol": _as_text(attempt.get("underlying_symbol")),
                "strategy": _as_text(attempt.get("strategy")),
                "pending_quantity": pending_quantity,
                "pending_entry_notional": _candidate_entry_notional(
                    candidate_payload,
                    pending_quantity,
                    _coerce_float(attempt.get("limit_price")),
                ),
                "pending_max_loss": _candidate_max_loss(
                    candidate_payload,
                    pending_quantity,
                ),
                # A partially filled attempt already consumes a slot through its
                # linked/open canonical position, so only count unfilled attempts
                # with no fills toward additional position capacity.
                "occupies_position_slot": (
                    linked_position_id is None and filled_quantity <= 0
                ),
            }
        )
    return exposures


def _broker_position_side(position: Mapping[str, Any]) -> str | None:
    side = _as_text(position.get("side"))
    if side in {"long", "short"}:
        return side
    quantity = _coerce_float(position.get("qty"))
    if quantity is None:
        return None
    if quantity > 0:
        return "long"
    if quantity < 0:
        return "short"
    return None


def _candidate_broker_position_conflicts(
    candidate: dict[str, Any],
) -> list[dict[str, str]]:
    resolved_candidate = _candidate_with_payload(candidate)
    resolved_legs = candidate_legs(resolved_candidate)
    if not resolved_legs:
        return []
    try:
        broker_positions = create_alpaca_client_from_env().list_positions()
    except Exception:
        return []

    broker_positions_by_symbol = {
        symbol: side
        for position in broker_positions
        if isinstance(position, Mapping)
        and (symbol := _as_text(position.get("symbol"))) is not None
        and (side := _broker_position_side(position)) is not None
    }
    conflicts: list[dict[str, str]] = []
    for leg in resolved_legs:
        symbol = _as_text(leg.get("symbol"))
        role = _as_text(leg.get("role"))
        if symbol is None or role not in {"short", "long"}:
            continue
        broker_side = broker_positions_by_symbol.get(symbol)
        requested_side = "short" if role == "short" else "long"
        if broker_side is None or broker_side == requested_side:
            continue
        conflicts.append(
            {
                "symbol": symbol,
                "broker_side": broker_side,
                "requested_role": role,
                "requested_position_intent": (
                    "sell_to_open" if role == "short" else "buy_to_open"
                ),
            }
        )
    return conflicts


def _session_position_metrics(positions: list[dict[str, Any]]) -> dict[str, float]:
    return {
        "open_position_count": float(len(positions)),
        "open_contract_count": sum(
            _coerce_float(position.get("remaining_quantity")) or 0.0
            for position in positions
        ),
        "entry_notional_total": sum(
            _coerce_float(position.get("entry_notional")) or 0.0
            for position in positions
        ),
        "max_loss_total": sum(
            _coerce_float(position.get("max_loss")) or 0.0 for position in positions
        ),
    }


def _session_pending_open_attempt_metrics(
    pending_attempts: list[dict[str, Any]],
) -> dict[str, float]:
    return {
        "pending_open_attempt_count": float(len(pending_attempts)),
        "pending_open_position_slot_count": sum(
            1.0
            for attempt in pending_attempts
            if bool(attempt.get("occupies_position_slot"))
        ),
        "pending_open_contract_count": sum(
            _coerce_float(attempt.get("pending_quantity")) or 0.0
            for attempt in pending_attempts
        ),
        "pending_entry_notional_total": sum(
            _coerce_float(attempt.get("pending_entry_notional")) or 0.0
            for attempt in pending_attempts
        ),
        "pending_max_loss_total": sum(
            _coerce_float(attempt.get("pending_max_loss")) or 0.0
            for attempt in pending_attempts
        ),
    }


def _session_open_metrics(
    positions: list[dict[str, Any]],
    pending_attempts: list[dict[str, Any]],
) -> dict[str, float]:
    position_metrics = _session_position_metrics(positions)
    pending_metrics = _session_pending_open_attempt_metrics(pending_attempts)
    return {
        **position_metrics,
        **pending_metrics,
        "active_open_position_count": (
            position_metrics["open_position_count"]
            + pending_metrics["pending_open_position_slot_count"]
        ),
        "active_open_contract_count": (
            position_metrics["open_contract_count"]
            + pending_metrics["pending_open_contract_count"]
        ),
        "active_entry_notional_total": (
            position_metrics["entry_notional_total"]
            + pending_metrics["pending_entry_notional_total"]
        ),
        "active_max_loss_total": (
            position_metrics["max_loss_total"]
            + pending_metrics["pending_max_loss_total"]
        ),
    }


def _kill_switch_reason() -> str | None:
    if _coerce_bool(os.environ.get("SPREADS_EXECUTION_KILL_SWITCH")):
        return "Execution is blocked by SPREADS_EXECUTION_KILL_SWITCH."
    return None


def resolve_execution_kill_switch_reason() -> str | None:
    return _kill_switch_reason()


def _environment_reason(
    normalized_policy: dict[str, Any],
    *,
    execution_policy: dict[str, Any] | None = None,
) -> str | None:
    environment = _current_trading_environment()
    deployment_mode = resolve_execution_deployment_mode(
        execution_policy,
        risk_policy=normalized_policy,
    )
    return live_deployment_block_reason(
        deployment_mode=deployment_mode,
        environment=environment,
        allow_live_env=_coerce_bool(os.environ.get("SPREADS_ALLOW_LIVE_TRADING")),
    )


def _candidate_timestamp(
    candidate: dict[str, Any], cycle: dict[str, Any]
) -> datetime | None:
    candidate_generated_at = parse_datetime(
        _as_text(candidate.get("generated_at")) or _as_text(cycle.get("generated_at"))
    )
    return candidate_generated_at


def assess_position_risk(
    *,
    position: dict[str, Any],
    risk_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(
        risk_policy or position.get("risk_policy")
    )
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
    max_contracts_per_position = _coerce_int(
        normalized_policy.get("max_contracts_per_position")
    )
    if (
        max_contracts_per_position is not None
        and remaining_quantity > max_contracts_per_position
    ):
        reasons.append("remaining quantity exceeds max_contracts_per_position")

    entry_notional = _coerce_float(position.get("entry_notional"))
    max_position_notional = _coerce_float(
        normalized_policy.get("max_position_notional")
    )
    if (
        entry_notional is not None
        and max_position_notional is not None
        and entry_notional > max_position_notional
    ):
        reasons.append("entry notional exceeds max_position_notional")

    max_loss = _coerce_float(position.get("max_loss"))
    max_position_max_loss = _coerce_float(
        normalized_policy.get("max_position_max_loss")
    )
    if (
        max_loss is not None
        and max_position_max_loss is not None
        and max_loss > max_position_max_loss
    ):
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
    execution_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)

    if (
        hasattr(execution_store, "portfolio_schema_ready")
        and not execution_store.portfolio_schema_ready()
    ):
        return {
            "status": "unknown",
            "note": "Portfolio position storage is not available yet.",
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
        environment_reason = _environment_reason(
            normalized_policy,
            execution_policy=execution_policy,
        )
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
    open_attempts = _open_attempts(execution_store, session_id=session_id)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    metrics = _session_open_metrics(open_positions, pending_attempts)
    reasons: list[str] = []

    if metrics["active_open_position_count"] >= float(
        normalized_policy["max_open_positions_per_session"]
    ):
        reasons.append("max_open_positions_per_session reached")
    if metrics["active_open_contract_count"] >= float(
        normalized_policy["max_contracts_per_session"]
    ):
        reasons.append("max_contracts_per_session reached")

    max_session_notional = _coerce_float(normalized_policy.get("max_session_notional"))
    if (
        max_session_notional is not None
        and metrics["active_entry_notional_total"] >= max_session_notional
    ):
        reasons.append("max_session_notional reached")

    max_session_max_loss = _coerce_float(normalized_policy.get("max_session_max_loss"))
    if (
        max_session_max_loss is not None
        and metrics["active_max_loss_total"] >= max_session_max_loss
    ):
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
        "note": "Pipeline run can submit new executions under the current risk policy.",
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
    execution_policy: dict[str, Any] | None = None,
    strategy_risk_budget: float | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)
    open_positions = _open_positions(execution_store, session_id=session_id)
    open_attempts = _open_attempts(execution_store, session_id=session_id)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    session_metrics = _session_open_metrics(open_positions, pending_attempts)
    position_notional = _candidate_entry_notional(candidate, quantity, limit_price)
    position_max_loss = _candidate_max_loss(candidate, quantity)
    candidate_timestamp = _candidate_timestamp(candidate, cycle)
    candidate_age_seconds = None
    if candidate_timestamp is not None:
        candidate_age_seconds = round(
            (datetime.now(UTC) - candidate_timestamp).total_seconds(), 3
        )
    underlying_symbol = str(candidate["underlying_symbol"])
    strategy = str(candidate["strategy"])
    matching_underlyings = [
        position
        for position in open_positions
        if str(position.get("underlying_symbol")) == underlying_symbol
    ]
    matching_pending_underlyings = [
        attempt
        for attempt in pending_attempts
        if bool(attempt.get("occupies_position_slot"))
        and str(attempt.get("underlying_symbol")) == underlying_symbol
    ]
    matching_strategy = [
        position
        for position in matching_underlyings
        if str(position.get("strategy")) == strategy
    ]
    matching_pending_strategy = [
        attempt
        for attempt in matching_pending_underlyings
        if str(attempt.get("strategy")) == strategy
    ]
    session_notional = session_metrics["active_entry_notional_total"]
    session_max_loss = session_metrics["active_max_loss_total"]
    open_contracts = session_metrics["active_open_contract_count"]
    metrics = {
        **session_metrics,
        "requested_quantity": int(quantity),
        "requested_limit_price": limit_price,
        "candidate_age_seconds": candidate_age_seconds,
        "position_notional": position_notional,
        "position_max_loss": position_max_loss,
        "session_notional_before": round(session_notional, 2),
        "session_notional_after": (
            None
            if position_notional is None
            else round(session_notional + position_notional, 2)
        ),
        "session_max_loss_before": round(session_max_loss, 2),
        "session_max_loss_after": (
            None
            if position_max_loss is None
            else round(session_max_loss + position_max_loss, 2)
        ),
        "matching_underlying_count": (
            len(matching_underlyings) + len(matching_pending_underlyings)
        ),
        "matching_underlying_strategy_count": (
            len(matching_strategy) + len(matching_pending_strategy)
        ),
        "strategy_risk_budget": strategy_risk_budget,
    }
    sizing = build_open_candidate_position_sizing(
        execution_store=execution_store,
        session_id=session_id,
        candidate=candidate,
        limit_price=limit_price,
        risk_policy=risk_policy,
        strategy_risk_budget=strategy_risk_budget,
    )
    metrics["position_sizing"] = sizing
    metrics["recommended_quantity"] = int(sizing["recommended_quantity"])
    metrics["recommended_position_max_loss"] = sizing["recommended_max_loss"]
    metrics["recommended_position_notional"] = sizing["recommended_entry_notional"]

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
        environment_reason = _environment_reason(
            normalized_policy,
            execution_policy=execution_policy,
        )
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

    broker_position_conflicts = _candidate_broker_position_conflicts(candidate)
    metrics["broker_position_conflict_count"] = len(broker_position_conflicts)
    metrics["broker_position_conflict_symbols"] = [
        conflict["symbol"] for conflict in broker_position_conflicts
    ]
    if broker_position_conflicts:
        conflict_summary = ", ".join(
            (
                f"{conflict['symbol']} "
                f"(broker {conflict['broker_side']}, request {conflict['requested_position_intent']})"
            )
            for conflict in broker_position_conflicts[:4]
        )
        if len(broker_position_conflicts) > 4:
            conflict_summary += ", …"
        return {
            "status": "blocked",
            "note": (
                "Open execution conflicts with existing broker-held option legs: "
                f"{conflict_summary}."
            ),
            "reason_codes": ["broker_position_intent_conflict"],
            "blockers": ["broker_position_intent_conflict"],
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

    max_contracts_per_position = _effective_max_contracts_per_position(
        candidate=candidate,
        normalized_policy=normalized_policy,
    )
    if (
        max_contracts_per_position is not None
        and quantity > max_contracts_per_position
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_position.",
            "reason_codes": ["max_contracts_per_position_exceeded"],
            "blockers": ["max_contracts_per_position_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    stale_quote_after_seconds = _coerce_float(
        normalized_policy.get("stale_quote_after_seconds")
    )
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

    if session_metrics["active_open_position_count"] >= int(
        normalized_policy["max_open_positions_per_session"]
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_session.",
            "reason_codes": ["max_open_positions_per_session_exceeded"],
            "blockers": ["max_open_positions_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_underlyings) + len(matching_pending_underlyings) >= int(
        normalized_policy["max_open_positions_per_underlying"]
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying.",
            "reason_codes": ["max_open_positions_per_underlying_exceeded"],
            "blockers": ["max_open_positions_per_underlying_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_strategy) + len(matching_pending_strategy) >= int(
        normalized_policy["max_open_positions_per_underlying_strategy"]
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying_strategy.",
            "reason_codes": ["max_open_positions_per_underlying_strategy_exceeded"],
            "blockers": ["max_open_positions_per_underlying_strategy_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if open_contracts + quantity > float(
        normalized_policy["max_contracts_per_session"]
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_session.",
            "reason_codes": ["max_contracts_per_session_exceeded"],
            "blockers": ["max_contracts_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_position_notional = _coerce_float(
        normalized_policy.get("max_position_notional")
    )
    if (
        position_notional is not None
        and max_position_notional is not None
        and position_notional > max_position_notional
    ):
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

    max_position_max_loss = _coerce_float(
        normalized_policy.get("max_position_max_loss")
    )
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

    if (
        strategy_risk_budget is not None
        and strategy_supports_position_sizing(strategy)
        and position_max_loss is not None
        and position_max_loss > strategy_risk_budget
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds strategy max_risk_per_trade.",
            "reason_codes": ["strategy_risk_budget_exceeded"],
            "blockers": ["strategy_risk_budget_exceeded"],
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
    execution_policy: dict[str, Any] | None = None,
    strategy_risk_budget: float | None = None,
) -> dict[str, Any]:
    decision = evaluate_open_execution(
        execution_store=execution_store,
        session_id=session_id,
        candidate=candidate,
        cycle=cycle,
        quantity=quantity,
        limit_price=limit_price,
        risk_policy=risk_policy,
        execution_policy=execution_policy,
        strategy_risk_budget=strategy_risk_budget,
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
        raise ValueError("Position does not have remaining quantity to close.")
    if quantity <= 0:
        raise ValueError("Close quantity must be positive.")
    if quantity > remaining_quantity:
        raise ValueError("Close quantity exceeds the remaining position quantity.")
    if limit_price is not None and limit_price <= 0:
        raise ValueError("Close execution requires a positive limit price.")
    if not position_legs(position):
        raise ValueError("Position is missing the broker symbols required to close.")
    return {
        "status": "ok",
    }

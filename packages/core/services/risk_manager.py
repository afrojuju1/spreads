from __future__ import annotations

from collections.abc import Mapping
import os
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

import yaml

from core.services.account_capacity import (
    estimate_buying_power_requirement,
    resolve_available_buying_power,
)
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
    normalize_strategy_family,
    position_legs,
)
from core.services.positions import enrich_position_row
from core.services.runtime_identity import parse_live_run_scope_id
from core.services.strategy_configs import default_config_root
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
)
from core.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ["open", "partial_close"]
CLOSE_RECONCILIATION_MAX_AGE_SECONDS = 180
POSITION_SIZING_STRATEGIES = {
    "short_call",
    "short_put",
    "call_credit_spread",
    "put_credit_spread",
    "call_debit_spread",
    "put_debit_spread",
    "iron_condor",
    "long_call",
    "long_put",
    "long_straddle",
    "long_strangle",
}
BASELINE_RISK_POLICY_NAME = "baseline"
RISK_POLICY_DERIVED_FLAGS = {
    "max_contracts_per_position_configured": False,
}
ACCOUNT_CAPACITY_REQUEST_TIMEOUT_SECONDS = 5.0

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
REQUIRED_BASELINE_RISK_POLICY_KEYS = (
    BOOL_POLICY_KEYS | INT_POLICY_KEYS | FLOAT_POLICY_KEYS
)


@lru_cache(maxsize=1)
def _baseline_risk_policy() -> dict[str, Any]:
    path = (
        default_config_root()
        / "policies"
        / "risk"
        / f"{BASELINE_RISK_POLICY_NAME}.yaml"
    )
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected mapping payload in {path}")
    missing = sorted(REQUIRED_BASELINE_RISK_POLICY_KEYS - set(raw))
    if missing:
        rendered = ", ".join(missing)
        raise ValueError(f"Baseline risk policy {path} is missing: {rendered}")
    return dict(raw)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


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

    policy = dict(_baseline_risk_policy())
    policy.update(RISK_POLICY_DERIVED_FLAGS)
    policy["max_contracts_per_position_configured"] = (
        "max_contracts_per_position" in policy
    )
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


def resolve_position_size_policy(
    risk_defaults: Mapping[str, Any] | None,
) -> dict[str, float | None]:
    defaults = risk_defaults if isinstance(risk_defaults, Mapping) else {}
    return {
        "max_risk_per_trade": _coerce_float(defaults.get("max_risk_per_trade")),
        "position_size_pct_of_available_balance": _coerce_float(
            defaults.get("position_size_pct_of_available_balance")
        ),
    }


def _position_size_budget(
    *,
    available_broker_buying_power: float | None,
    position_size_pct_of_available_balance: float | None,
) -> float | None:
    if (
        available_broker_buying_power is None
        or available_broker_buying_power < 0
        or position_size_pct_of_available_balance is None
        or position_size_pct_of_available_balance <= 0
    ):
        return None
    return round(
        max(available_broker_buying_power, 0.0)
        * float(position_size_pct_of_available_balance),
        2,
    )


def strategy_supports_position_sizing(strategy_family: Any) -> bool:
    normalized = normalize_strategy_family(strategy_family)
    return normalized in POSITION_SIZING_STRATEGIES


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
    position_size_pct_of_available_balance: float | None = None,
    available_broker_buying_power: float | None = None,
) -> dict[str, Any]:
    candidate_payload = _candidate_payload(candidate)
    strategy_family = normalize_strategy_family(
        candidate_payload.get("strategy")
        or candidate.get("strategy")
        or candidate.get("strategy_family")
    )
    applies = strategy_supports_position_sizing(strategy_family)
    per_contract_entry_notional = _candidate_entry_notional(candidate, 1.0, limit_price)
    per_contract_max_loss = _candidate_max_loss(candidate, 1.0)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    per_contract_required_buying_power = _coerce_float(
        buying_power_requirement.get("required_buying_power")
    )
    position_size_budget = _position_size_budget(
        available_broker_buying_power=available_broker_buying_power,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
    )
    effective_strategy_risk_budget = strategy_risk_budget
    if (
        position_size_pct_of_available_balance is not None
        and available_broker_buying_power is not None
    ):
        effective_strategy_risk_budget = None
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
        "position_size_pct_of_available_balance",
        _max_contracts_for_budget(
            per_contract_required_buying_power,
            position_size_budget,
        ),
    )
    _append_constraint(
        "max_risk_per_trade",
        _max_contracts_for_budget(
            per_contract_max_loss,
            effective_strategy_risk_budget,
        ),
    )
    _append_constraint(
        "available_broker_buying_power",
        _max_contracts_for_budget(
            per_contract_required_buying_power,
            available_broker_buying_power,
        ),
    )

    recommended_quantity = 1
    limiting_constraint = None
    effective_constraints = constraints
    if not applies:
        effective_constraints = [
            item for item in constraints if item[0] == "available_broker_buying_power"
        ]
    if effective_constraints:
        limiting_constraint, recommended_quantity = min(
            effective_constraints,
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
        "per_contract_required_buying_power": per_contract_required_buying_power,
        "buying_power_basis": _as_text(buying_power_requirement.get("basis")),
        "position_size_pct_of_available_balance": (
            None
            if position_size_pct_of_available_balance is None
            else float(position_size_pct_of_available_balance)
        ),
        "position_size_budget": position_size_budget,
        "available_broker_buying_power": available_broker_buying_power,
        "constraints": {name: value for name, value in constraints},
        "effective_constraints": {
            name: value for name, value in effective_constraints
        },
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
    position_size_pct_of_available_balance: float | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)
    open_positions = _open_positions(execution_store, session_id=session_id)
    open_attempts = _open_attempts(execution_store, session_id=session_id)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    session_metrics = _session_open_metrics(open_positions, pending_attempts)
    broker_buying_power = live_broker_buying_power_snapshot(execution_store)
    session_notional = session_metrics["active_entry_notional_total"]
    session_max_loss = session_metrics["active_max_loss_total"]
    open_contracts = session_metrics["active_open_contract_count"]
    sizing = build_candidate_position_sizing(
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
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
        available_broker_buying_power=_coerce_float(
            broker_buying_power.get("remaining_buying_power")
        ),
    )
    return {
        **sizing,
        "broker_buying_power_status": _as_text(broker_buying_power.get("status")),
        "broker_buying_power_source_field": _as_text(
            broker_buying_power.get("source_field")
        ),
        "broker_account_available_buying_power": _coerce_float(
            broker_buying_power.get("available_buying_power")
        ),
        "broker_reserved_buying_power": _coerce_float(
            broker_buying_power.get("reserved_buying_power")
        ),
        "broker_capacity_error_text": _as_text(
            broker_buying_power.get("error_text")
        ),
        "broker_reservation_count": _coerce_int(
            broker_buying_power.get("reservation_count")
        ),
        "broker_unsupported_reservation_count": _coerce_int(
            broker_buying_power.get("unsupported_reservation_count")
        ),
    }


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


def _account_open_attempts(execution_store: Any) -> list[dict[str, Any]]:
    list_for_status = getattr(execution_store, "list_attempts_by_status", None)
    if not callable(list_for_status):
        return []
    rows = list_for_status(
        statuses=list(OPEN_ATTEMPT_STATUS_LIST),
        trade_intent="open",
        limit=200,
    )
    return [dict(row) for row in rows]


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
                "limit_price": _coerce_float(attempt.get("limit_price")),
                "candidate": candidate_payload,
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


def live_broker_buying_power_snapshot(execution_store: Any) -> dict[str, Any]:
    open_attempts = _account_open_attempts(execution_store)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    reserved_buying_power = 0.0
    reservation_count = 0
    unsupported_reservation_count = 0
    for attempt in pending_attempts:
        requirement = estimate_buying_power_requirement(
            dict(attempt.get("candidate") or {}),
            _coerce_float(attempt.get("pending_quantity")) or 0.0,
            limit_price=_coerce_float(attempt.get("limit_price")),
        )
        required_buying_power = _coerce_float(
            requirement.get("required_buying_power")
        )
        if required_buying_power is None:
            unsupported_reservation_count += 1
            continue
        reservation_count += 1
        reserved_buying_power += required_buying_power

    try:
        account_payload = create_alpaca_client_from_env(
            request_timeout_seconds=ACCOUNT_CAPACITY_REQUEST_TIMEOUT_SECONDS,
        ).get_account()
    except Exception as exc:
        return {
            "status": "unavailable",
            "source_field": None,
            "available_buying_power": None,
            "reserved_buying_power": round(reserved_buying_power, 2),
            "remaining_buying_power": None,
            "reservation_count": reservation_count,
            "unsupported_reservation_count": unsupported_reservation_count,
            "error_text": str(exc),
        }

    available_snapshot = resolve_available_buying_power(account_payload)
    available_buying_power = _coerce_float(
        available_snapshot.get("available_buying_power")
    )
    if available_buying_power is None:
        return {
            "status": "unavailable",
            "source_field": _as_text(available_snapshot.get("source_field")),
            "available_buying_power": None,
            "reserved_buying_power": round(reserved_buying_power, 2),
            "remaining_buying_power": None,
            "reservation_count": reservation_count,
            "unsupported_reservation_count": unsupported_reservation_count,
            "error_text": "Broker account payload did not include usable buying power fields.",
        }

    return {
        "status": "ok",
        "source_field": _as_text(available_snapshot.get("source_field")),
        "available_buying_power": round(available_buying_power, 2),
        "reserved_buying_power": round(reserved_buying_power, 2),
        "remaining_buying_power": round(
            max(available_buying_power - reserved_buying_power, 0.0),
            2,
        ),
        "reservation_count": reservation_count,
        "unsupported_reservation_count": unsupported_reservation_count,
        "error_text": None,
    }


def build_execution_admission_snapshot(
    *,
    execution_store: Any,
    candidate: dict[str, Any],
    limit_price: float | None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
) -> dict[str, Any]:
    broker_buying_power = live_broker_buying_power_snapshot(execution_store)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    required_buying_power = _coerce_float(
        buying_power_requirement.get("required_buying_power")
    )
    available_buying_power = _coerce_float(
        broker_buying_power.get("remaining_buying_power")
    )
    sizing = build_candidate_position_sizing(
        candidate=candidate,
        limit_price=limit_price,
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
        available_broker_buying_power=available_buying_power,
    )
    limiting_constraint = _as_text(sizing.get("limiting_constraint"))
    admissible_quantity = _coerce_int(sizing.get("recommended_quantity"))
    snapshot = {
        "status": "unknown",
        "reason": None,
        "message": None,
        "evaluated_at": _utc_now(),
        "admissible_quantity": None,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "account_available_buying_power": _coerce_float(
            broker_buying_power.get("available_buying_power")
        ),
        "reserved_buying_power": _coerce_float(
            broker_buying_power.get("reserved_buying_power")
        ),
        "buying_power_basis": _as_text(buying_power_requirement.get("basis")),
        "buying_power_source_field": _as_text(broker_buying_power.get("source_field")),
        "broker_buying_power_status": _as_text(broker_buying_power.get("status")),
        "limiting_constraint": limiting_constraint,
        "strategy_risk_budget": strategy_risk_budget,
        "position_size_pct_of_available_balance": _coerce_float(
            sizing.get("position_size_pct_of_available_balance")
        ),
        "position_size_budget": _coerce_float(sizing.get("position_size_budget")),
    }
    if str(broker_buying_power.get("status") or "") != "ok":
        return {
            **snapshot,
            "reason": "broker_buying_power_unavailable",
            "message": _as_text(broker_buying_power.get("error_text"))
            or "Broker buying power is unavailable.",
        }
    if required_buying_power is None:
        return {
            **snapshot,
            "reason": "unsupported_buying_power_estimate",
            "message": "Buying power estimate is unavailable for this structure.",
        }

    resolved_quantity = max(int(admissible_quantity or 0), 0)
    if resolved_quantity <= 0:
        reason = "insufficient_broker_buying_power"
        message = "Current account buying power cannot carry one contract."
        if limiting_constraint == "position_size_pct_of_available_balance":
            reason = "position_size_budget_exhausted"
            message = "Configured position size budget does not allow one contract."
        elif limiting_constraint == "max_risk_per_trade":
            reason = "max_risk_per_trade_exhausted"
            message = "Configured strategy risk budget does not allow one contract."
        elif limiting_constraint not in {None, "", "available_broker_buying_power"}:
            reason = "execution_capacity_unavailable"
            message = "Current execution capacity does not allow one contract."
        if (
            available_buying_power is not None
            and required_buying_power is not None
            and reason == "insufficient_broker_buying_power"
        ):
            message = (
                "Current account buying power cannot carry one contract "
                f"(requires {required_buying_power:.2f}, "
                f"available {available_buying_power:.2f})."
            )
        return {
            **snapshot,
            "status": "blocked",
            "reason": reason,
            "message": message,
            "admissible_quantity": 0,
        }

    message = f"Current account can carry up to {resolved_quantity} contract"
    if resolved_quantity != 1:
        message += "s"
    message += " now."
    return {
        **snapshot,
        "status": "admissible",
        "reason": None,
        "message": message,
        "admissible_quantity": resolved_quantity,
    }


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
    position_size_pct_of_available_balance: float | None = None,
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
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        quantity,
        limit_price=limit_price,
    )
    required_buying_power = _coerce_float(
        buying_power_requirement.get("required_buying_power")
    )
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
        "position_size_pct_of_available_balance": position_size_pct_of_available_balance,
        "required_buying_power": required_buying_power,
        "buying_power_basis": _as_text(buying_power_requirement.get("basis")),
    }
    sizing = build_open_candidate_position_sizing(
        execution_store=execution_store,
        session_id=session_id,
        candidate=candidate,
        limit_price=limit_price,
        risk_policy=risk_policy,
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
    )
    metrics["position_sizing"] = sizing
    metrics["recommended_quantity"] = int(sizing["recommended_quantity"])
    metrics["recommended_position_max_loss"] = sizing["recommended_max_loss"]
    metrics["recommended_position_notional"] = sizing["recommended_entry_notional"]
    metrics["available_broker_buying_power"] = _coerce_float(
        sizing.get("available_broker_buying_power")
    )
    metrics["broker_buying_power_status"] = _as_text(
        sizing.get("broker_buying_power_status")
    )
    metrics["broker_reserved_buying_power"] = _coerce_float(
        sizing.get("broker_reserved_buying_power")
    )
    metrics["broker_buying_power_source_field"] = _as_text(
        sizing.get("broker_buying_power_source_field")
    )

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
        and position_size_pct_of_available_balance is None
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

    recommended_quantity = _coerce_int(sizing.get("recommended_quantity"))
    limiting_constraint = _as_text(sizing.get("limiting_constraint"))
    if (
        recommended_quantity is not None
        and recommended_quantity >= 0
        and quantity > recommended_quantity
    ):
        if limiting_constraint == "position_size_pct_of_available_balance":
            return {
                "status": "blocked",
                "note": (
                    "Open execution exceeds the configured position-size budget "
                    "derived from available broker buying power."
                ),
                "reason_codes": ["position_size_budget_exceeded"],
                "blockers": ["position_size_budget_exceeded"],
                "policy": normalized_policy,
                "metrics": metrics,
            }
        if limiting_constraint == "max_risk_per_trade":
            return {
                "status": "blocked",
                "note": "Open execution exceeds strategy max_risk_per_trade.",
                "reason_codes": ["strategy_risk_budget_exceeded"],
                "blockers": ["strategy_risk_budget_exceeded"],
                "policy": normalized_policy,
                "metrics": metrics,
            }

    available_broker_buying_power = _coerce_float(
        sizing.get("available_broker_buying_power")
    )
    if (
        required_buying_power is not None
        and available_broker_buying_power is not None
        and required_buying_power > available_broker_buying_power
    ):
        source_field = _as_text(sizing.get("broker_buying_power_source_field"))
        source_note = "" if source_field is None else f" from {source_field}"
        return {
            "status": "blocked",
            "note": (
                "Open execution exceeds available broker buying power"
                f"{source_note} (requires {required_buying_power:.2f}, "
                f"available {available_broker_buying_power:.2f})."
            ),
            "reason_codes": ["insufficient_broker_buying_power"],
            "blockers": ["insufficient_broker_buying_power"],
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
    position_size_pct_of_available_balance: float | None = None,
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
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
    )
    if decision["status"] in {"blocked", "unknown"}:
        raise ValueError(str(decision["note"]))
    return dict(decision["policy"])


def validate_close_execution(
    *,
    position: dict[str, Any],
    quantity: int,
    limit_price: float | None = None,
    now: datetime | None = None,
    max_reconciliation_age_seconds: float | None = None,
) -> dict[str, Any]:
    position_status = str(
        position.get("position_status") or position.get("status") or ""
    ).lower()
    if position_status and position_status not in OPEN_POSITION_STATUSES:
        raise ValueError("Position is already closed.")
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
    if max_reconciliation_age_seconds is not None:
        reconciliation_status = _as_text(position.get("reconciliation_status"))
        if reconciliation_status != "matched":
            raise ValueError(
                "Position broker reconciliation is not matched; "
                "wait for broker sync before closing."
            )
        last_reconciled_at = parse_datetime(
            _as_text(position.get("last_reconciled_at"))
        )
        if last_reconciled_at is None:
            raise ValueError(
                "Position broker reconciliation is missing; "
                "wait for broker sync before closing."
            )
        reconciliation_age = (
            (now or datetime.now(UTC)) - last_reconciled_at.astimezone(UTC)
        ).total_seconds()
        if reconciliation_age > max_reconciliation_age_seconds:
            raise ValueError(
                "Position broker reconciliation is stale; "
                "wait for broker sync before closing."
            )
    return {
        "status": "ok",
    }

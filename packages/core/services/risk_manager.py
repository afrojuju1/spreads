from __future__ import annotations

from collections.abc import Mapping
import os
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from core.money import money_float, money_scaled_float, money_sum_float, option_contract_notional
from core.services.account_capacity import (
    estimate_buying_power_requirement,
    resolve_available_buying_power,
)
from core.services.config_inheritance import load_yaml_mapping
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
    resolve_execution_attempt_requested_quantity,
)
from core.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    normalize_strategy_family,
    position_legs,
)
from core.services.positions import enrich_position_row
from core.services.payload_validation import format_validation_error
from core.services.runtime_identity import parse_live_run_scope_id
from core.services.trading_strategies import default_config_root
from core.services.trading_strategy_risk_models import PROTECTION_RULE_KEYS
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_bool,
    coerce_float,
    coerce_int,
    coerce_utc_datetime,
    safe_component,
    unique_text_list,
    utc_now_iso,
)
from core.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ["open", "partial_close"]
ACTIVE_PORTFOLIO_INTENT_STATES = [
    "pending",
    "claimed",
    "submitted",
    "partially_filled",
]
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
ENTRY_CAPACITY_ADMISSION_BOUNDARY = "entry_capacity_precheck"
ALLOCATION_PLAN_BOUNDARY = "allocation_plan"
PROTECTION_ADMISSION_BOUNDARY = "protection_admission"
PORTFOLIO_ADMISSION_BOUNDARY = "portfolio_admission"
DEFERRED_EXECUTION_READINESS_REASON = "deferred_to_execution_submit"
ALLOCATION_DECISION_LIMIT = 200
BROAD_INDEX_CORRELATION_SYMBOLS = {"SPY", "QQQ", "DIA", "IWM"}
PROTECTION_ADMISSIBLE_STATUSES = {"admissible", "approved", "ok", "pass", "passed"}
TERMINAL_ENTRY_ATTEMPT_STATUSES = {
    "blocked",
    "canceled",
    "cancelled",
    "expired",
    "failed",
    "rejected",
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


class BaselineRiskPolicyYamlPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool
    allow_live: bool
    max_open_positions_per_session: int = Field(gt=0)
    max_open_positions_per_underlying: int = Field(gt=0)
    max_open_positions_per_underlying_strategy: int = Field(gt=0)
    max_contracts_per_position: int = Field(gt=0)
    max_contracts_per_session: int = Field(gt=0)
    max_position_notional: float | None = Field(ge=0)
    max_session_notional: float | None = Field(ge=0)
    max_position_max_loss: float | None = Field(ge=0)
    max_session_max_loss: float | None = Field(ge=0)
    stale_quote_after_seconds: float = Field(gt=0)


@lru_cache(maxsize=1)
def _baseline_risk_policy() -> dict[str, Any]:
    path = default_config_root() / "policies" / "risk" / f"{BASELINE_RISK_POLICY_NAME}.yaml"
    try:
        payload = BaselineRiskPolicyYamlPayload.model_validate(load_yaml_mapping(path))
    except ValidationError as exc:
        raise ValueError(f"Invalid baseline risk policy config in {path}: {format_validation_error(exc)}") from exc
    return payload.model_dump()


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
    raw_policy = source.get("risk_policy") if isinstance(source.get("risk_policy"), dict) else source

    policy = dict(_baseline_risk_policy())
    policy.update(RISK_POLICY_DERIVED_FLAGS)
    policy["max_contracts_per_position_configured"] = "max_contracts_per_position" in policy
    stale_quote_after_seconds = coerce_float(raw_policy.get("stale_quote_after_seconds", raw_policy.get("max_candidate_age_seconds")))
    if stale_quote_after_seconds is not None:
        policy["stale_quote_after_seconds"] = stale_quote_after_seconds

    duplicate_underlying_strategy_limit = coerce_int(
        raw_policy.get(
            "max_open_positions_per_underlying_strategy",
            raw_policy.get("duplicate_underlying_strategy_limit"),
        )
    )
    if duplicate_underlying_strategy_limit is not None:
        policy["max_open_positions_per_underlying_strategy"] = duplicate_underlying_strategy_limit

    for key in BOOL_POLICY_KEYS:
        if key in raw_policy:
            policy[key] = coerce_bool(raw_policy[key], default=False)
    for key in INT_POLICY_KEYS:
        if key not in raw_policy:
            continue
        parsed = coerce_int(raw_policy[key])
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
        parsed = coerce_float(value)
        if parsed is not None:
            policy[key] = parsed

    policy["enabled"] = bool(policy["enabled"])
    policy["allow_live"] = bool(policy["allow_live"])
    return policy


def _current_trading_environment() -> str:
    client = create_alpaca_client_from_env()
    return resolve_trading_environment(client.trading_base_url)


def _candidate_entry_notional(candidate: dict[str, Any], quantity: float, price: float | None) -> float | None:
    entry_price = price
    if entry_price is None or entry_price <= 0:
        payload = _candidate_payload(candidate)
        entry_price = coerce_float(payload.get("midpoint_credit") or payload.get("midpoint_debit") or payload.get("midpoint_value"))
    if entry_price is None or entry_price <= 0:
        return None
    return option_contract_notional(entry_price, quantity)


def _candidate_max_loss(candidate: dict[str, Any], quantity: float) -> float | None:
    candidate_payload = _candidate_payload(candidate)
    max_loss = coerce_float(candidate_payload.get("max_loss"))
    if max_loss is None:
        width = coerce_float(candidate_payload.get("width"))
        midpoint_value = coerce_float(
            candidate_payload.get("midpoint_credit") or candidate_payload.get("midpoint_debit") or candidate_payload.get("midpoint_value")
        )
        premium_kind = net_premium_kind(candidate_payload.get("strategy"))
        if width is not None and midpoint_value is not None:
            if premium_kind == "debit":
                return option_contract_notional(midpoint_value, quantity)
            else:
                return option_contract_notional(max(width - midpoint_value, 0.0), quantity)
    if max_loss is None:
        return None
    return money_scaled_float(max_loss, quantity)


def _max_contracts_for_budget(
    unit_exposure: float | None,
    budget: float | None,
) -> int | None:
    if unit_exposure is None or unit_exposure <= 0 or budget is None or budget < 0:
        return None
    return max(int(budget // unit_exposure), 0)


def resolve_position_size_policy(
    risk_defaults: Mapping[str, Any] | None,
) -> dict[str, float | None]:
    defaults = risk_defaults if isinstance(risk_defaults, Mapping) else {}
    return {
        "max_risk_per_trade": coerce_float(defaults.get("max_risk_per_trade")),
        "position_size_pct_of_available_balance": coerce_float(defaults.get("position_size_pct_of_available_balance")),
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
        max(available_broker_buying_power, 0.0) * float(position_size_pct_of_available_balance),
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
    strategy_family = normalize_strategy_family(candidate_payload.get("strategy") or candidate.get("strategy") or candidate.get("strategy_family"))
    applies = strategy_supports_position_sizing(strategy_family)
    per_contract_entry_notional = _candidate_entry_notional(candidate, 1.0, limit_price)
    per_contract_max_loss = _candidate_max_loss(candidate, 1.0)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    per_contract_required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    position_size_budget = _position_size_budget(
        available_broker_buying_power=available_broker_buying_power,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
    )
    effective_strategy_risk_budget = strategy_risk_budget
    if position_size_pct_of_available_balance is not None and available_broker_buying_power is not None:
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
        effective_constraints = [item for item in constraints if item[0] == "available_broker_buying_power"]
    if effective_constraints:
        limiting_constraint, recommended_quantity = min(
            effective_constraints,
            key=lambda item: (item[1], item[0]),
        )

    recommended_entry_notional = None if per_contract_entry_notional is None else money_scaled_float(per_contract_entry_notional, recommended_quantity)
    recommended_max_loss = None if per_contract_max_loss is None else money_scaled_float(per_contract_max_loss, recommended_quantity)
    return {
        "applies": applies,
        "strategy_family": strategy_family or None,
        "per_contract_entry_notional": per_contract_entry_notional,
        "per_contract_max_loss": per_contract_max_loss,
        "per_contract_required_buying_power": per_contract_required_buying_power,
        "buying_power_basis": as_text(buying_power_requirement.get("basis")),
        "position_size_pct_of_available_balance": (
            None if position_size_pct_of_available_balance is None else float(position_size_pct_of_available_balance)
        ),
        "position_size_budget": position_size_budget,
        "available_broker_buying_power": available_broker_buying_power,
        "constraints": {name: value for name, value in constraints},
        "effective_constraints": {name: value for name, value in effective_constraints},
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
    max_contracts_per_position = coerce_int(normalized_policy.get("max_contracts_per_position"))
    if max_contracts_per_position is None:
        return None
    if bool(normalized_policy.get("max_contracts_per_position_configured")):
        return max_contracts_per_position
    strategy_family = _candidate_payload(candidate).get("strategy") or candidate.get("strategy") or candidate.get("strategy_family")
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
        max_position_notional=coerce_float(normalized_policy.get("max_position_notional")),
        remaining_session_notional=(
            None
            if coerce_float(normalized_policy.get("max_session_notional")) is None
            else max(
                float(coerce_float(normalized_policy.get("max_session_notional")) or 0.0) - session_notional,
                0.0,
            )
        ),
        max_position_max_loss=coerce_float(normalized_policy.get("max_position_max_loss")),
        remaining_session_max_loss=(
            None
            if coerce_float(normalized_policy.get("max_session_max_loss")) is None
            else max(
                float(coerce_float(normalized_policy.get("max_session_max_loss")) or 0.0) - session_max_loss,
                0.0,
            )
        ),
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
        available_broker_buying_power=coerce_float(broker_buying_power.get("remaining_buying_power")),
    )
    return {
        **sizing,
        "broker_buying_power_status": as_text(broker_buying_power.get("status")),
        "broker_buying_power_source_field": as_text(broker_buying_power.get("source_field")),
        "broker_account_available_buying_power": coerce_float(broker_buying_power.get("available_buying_power")),
        "broker_reserved_buying_power": coerce_float(broker_buying_power.get("reserved_buying_power")),
        "broker_capacity_error_text": as_text(broker_buying_power.get("error_text")),
        "broker_reservation_count": coerce_int(broker_buying_power.get("reservation_count")),
        "broker_unsupported_reservation_count": coerce_int(broker_buying_power.get("unsupported_reservation_count")),
    }


def _open_positions(execution_store: Any, *, session_id: str) -> list[dict[str, Any]]:
    resolved = parse_live_run_scope_id(session_id)
    if resolved is None:
        return []
    return [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
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
    requested_quantity = resolve_execution_attempt_requested_quantity(attempt)
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
        linked_position_id = as_text(attempt.get("position_id"))
        exposures.append(
            {
                "execution_attempt_id": as_text(attempt.get("execution_attempt_id")),
                "underlying_symbol": as_text(attempt.get("underlying_symbol")),
                "strategy": as_text(attempt.get("strategy")),
                "pending_quantity": pending_quantity,
                "limit_price": coerce_float(attempt.get("limit_price")),
                "candidate": candidate_payload,
                "pending_entry_notional": _candidate_entry_notional(
                    candidate_payload,
                    pending_quantity,
                    coerce_float(attempt.get("limit_price")),
                ),
                "pending_max_loss": _candidate_max_loss(
                    candidate_payload,
                    pending_quantity,
                ),
                # A partially filled attempt already consumes a slot through its
                # linked/open canonical position, so only count unfilled attempts
                # with no fills toward additional position capacity.
                "occupies_position_slot": (linked_position_id is None and filled_quantity <= 0),
            }
        )
    return exposures


def _date_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if hasattr(value, "date") and callable(value.date):
        try:
            return value.date().isoformat()
        except Exception:
            return None
    if hasattr(value, "isoformat") and callable(value.isoformat):
        try:
            return str(value.isoformat())[:10]
        except Exception:
            return None
    text = str(value).strip()
    return text[:10] if text else None


def _root_symbol(value: Any) -> str | None:
    text = as_text(value)
    if text is None:
        return None
    return text.upper()


def _candidate_root_symbol(candidate: Mapping[str, Any]) -> str | None:
    payload = _candidate_payload(dict(candidate))
    return _root_symbol(
        candidate.get("root_symbol") or candidate.get("underlying_symbol") or payload.get("root_symbol") or payload.get("underlying_symbol")
    )


def _candidate_strategy_family(
    candidate: Mapping[str, Any],
    *,
    strategy_family: Any = None,
) -> str | None:
    payload = _candidate_payload(dict(candidate))
    normalized = normalize_strategy_family(
        strategy_family
        or candidate.get("strategy_family")
        or candidate.get("trade_structure")
        or candidate.get("strategy")
        or payload.get("strategy_family")
        or payload.get("trade_structure")
        or payload.get("strategy")
    )
    return normalized or None


def _portfolio_correlation_group(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    normalized = str(symbol).upper()
    if normalized in BROAD_INDEX_CORRELATION_SYMBOLS:
        return "broad_index_etf"
    return normalized


def _exposure_max_loss_from_row(row: Mapping[str, Any]) -> float | None:
    for key in ("max_loss", "position_max_loss", "requested_notional"):
        value = coerce_float(row.get(key))
        if value is not None:
            return value
    for nested_key in ("economics", "strategy_metrics", "candidate", "payload"):
        nested = row.get(nested_key)
        if isinstance(nested, Mapping):
            value = coerce_float(nested.get("max_loss") or nested.get("position_max_loss"))
            if value is not None:
                return value
    quantity = (
        coerce_float(row.get("remaining_quantity") or row.get("opened_quantity") or row.get("quantity") or row.get("requested_quantity")) or 1.0
    )
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return _candidate_max_loss(dict(candidate), quantity)
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        return _candidate_max_loss(dict(payload), quantity)
    return None


def _portfolio_position_exposure(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _root_symbol(row.get("root_symbol") or row.get("underlying_symbol"))
    family = _candidate_strategy_family(
        row,
        strategy_family=row.get("strategy_family") or row.get("trade_structure"),
    )
    return {
        "source_type": "position",
        "source_id": as_text(row.get("position_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": family,
        "underlying_symbol": symbol,
        "correlation_group": _portfolio_correlation_group(symbol),
        "market_date": _date_text(row.get("market_date_opened") or row.get("created_at")),
        "max_loss": _exposure_max_loss_from_row(row),
        "contract_count": coerce_float(row.get("remaining_quantity")) or 0.0,
    }


def _portfolio_attempt_exposure(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    candidate_payload = dict(candidate) if isinstance(candidate, Mapping) else {}
    symbol = _root_symbol(row.get("root_symbol") or row.get("underlying_symbol") or candidate_payload.get("underlying_symbol"))
    family = _candidate_strategy_family(
        candidate_payload or row,
        strategy_family=row.get("strategy_family") or row.get("strategy") or row.get("trade_structure"),
    )
    return {
        "source_type": "attempt",
        "source_id": as_text(row.get("execution_attempt_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": family,
        "underlying_symbol": symbol,
        "correlation_group": _portfolio_correlation_group(symbol),
        "market_date": _date_text(row.get("market_date") or row.get("requested_at")),
        "max_loss": _exposure_max_loss_from_row(row),
        "contract_count": coerce_float(row.get("pending_quantity") or row.get("remaining_quantity") or row.get("quantity")) or 0.0,
    }


def _portfolio_intent_exposure(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        payload = row.get("payload_json") if isinstance(row.get("payload_json"), Mapping) else {}
    symbol = _root_symbol(payload.get("root_symbol") or payload.get("underlying_symbol"))
    family = _candidate_strategy_family(
        payload,
        strategy_family=row.get("trade_structure") or payload.get("strategy_family") or payload.get("trade_structure"),
    )
    execution_admission = payload.get("execution_admission") if isinstance(payload.get("execution_admission"), Mapping) else {}
    order_payload = as_mapping(payload.get("order_payload"))
    execution_shape = as_mapping(payload.get("execution_shape"))
    execution_order_payload = as_mapping(execution_shape.get("order_payload"))
    contract_count = (
        coerce_float(payload.get("quantity") or payload.get("qty"))
        or coerce_float(order_payload.get("quantity") or order_payload.get("qty"))
        or coerce_float(execution_order_payload.get("quantity") or execution_order_payload.get("qty"))
        or coerce_float(execution_admission.get("admissible_quantity"))
        or 1.0
    )
    return {
        "source_type": "intent",
        "source_id": as_text(row.get("execution_intent_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": family,
        "underlying_symbol": symbol,
        "correlation_group": _portfolio_correlation_group(symbol),
        "market_date": _date_text(row.get("created_at")),
        "max_loss": _exposure_max_loss_from_row(execution_admission or payload),
        "contract_count": contract_count,
    }


def _portfolio_schema_ready(execution_store: Any) -> bool:
    for method_name in ("portfolio_schema_ready", "positions_schema_ready", "intent_schema_ready"):
        method = getattr(execution_store, method_name, None)
        if callable(method) and not bool(method()):
            return False
    return True


def _open_portfolio_exposures(execution_store: Any) -> list[dict[str, Any]]:
    positions = [_portfolio_position_exposure(dict(row)) for row in execution_store.list_positions(statuses=OPEN_POSITION_STATUSES, limit=500)]
    attempts = []
    for row in execution_store.list_attempts_by_status(
        statuses=list(OPEN_ATTEMPT_STATUS_LIST),
        trade_intent="open",
        limit=500,
    ):
        payload = dict(row)
        if as_text(payload.get("position_id")) is not None:
            continue
        attempts.append(_portfolio_attempt_exposure(payload))

    intents = []
    for row in execution_store.list_execution_intents(
        states=list(ACTIVE_PORTFOLIO_INTENT_STATES),
        limit=500,
    ):
        payload = dict(row)
        if as_text(payload.get("execution_attempt_id")) is not None:
            continue
        intents.append(_portfolio_intent_exposure(payload))

    return [row for row in [*positions, *attempts, *intents] if row.get("underlying_symbol") is not None]


def _daily_entry_exposures(
    execution_store: Any,
    *,
    session_date: str,
    active_exposures: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}

    def remember(row: dict[str, Any]) -> None:
        source_type = as_text(row.get("source_type")) or "unknown"
        source_id = as_text(row.get("source_id")) or f"{source_type}:{len(rows)}"
        rows[(source_type, source_id)] = row

    for exposure in active_exposures:
        if _date_text(exposure.get("market_date")) == session_date:
            remember(dict(exposure))

    for row in execution_store.list_positions(market_date=session_date, limit=500):
        remember(_portfolio_position_exposure(dict(row)))

    list_attempts = getattr(execution_store, "list_attempts_for_market_date", None)
    if callable(list_attempts):
        for row in list_attempts(market_date=session_date, limit=500):
            payload = dict(row)
            if str(payload.get("trade_intent") or "").lower() != "open":
                continue
            if str(payload.get("status") or "").lower() in TERMINAL_ENTRY_ATTEMPT_STATUSES:
                continue
            remember(_portfolio_attempt_exposure(payload))

    return list(rows.values())


def _portfolio_policy_int(policy: Mapping[str, Any], key: str) -> int | None:
    value = coerce_int(policy.get(key))
    if value is None or value <= 0:
        return None
    return int(value)


def _allocation_rank_key(row: Mapping[str, Any]) -> tuple[float, int, float, str]:
    score = coerce_float(row.get("score"))
    rank = coerce_int(row.get("rank"))
    decided_at = coerce_utc_datetime(row.get("decided_at"))
    decided_timestamp = 0.0 if decided_at is None else decided_at.timestamp()
    return (
        -(score if score is not None else -1_000_000.0),
        rank if rank is not None and rank > 0 else 1_000_000,
        -decided_timestamp,
        as_text(row.get("trade_decision_id")) or "",
    )


def _allocation_decision_candidate(
    *,
    decision: Mapping[str, Any],
    signal: Mapping[str, Any],
    default_strategy_family: Any,
    requested_quantity: int | float,
    limit_price: float | None,
) -> dict[str, Any]:
    execution_shape = (
        dict(decision.get("selected_execution_shape"))
        if isinstance(decision.get("selected_execution_shape"), Mapping)
        else dict(signal.get("execution_shape")) if isinstance(signal.get("execution_shape"), Mapping) else {}
    )
    order_payload = (
        dict(signal.get("order_payload"))
        if isinstance(signal.get("order_payload"), Mapping)
        else dict(execution_shape.get("order_payload")) if isinstance(execution_shape.get("order_payload"), Mapping) else {}
    )
    economics = dict(signal.get("economics")) if isinstance(signal.get("economics"), Mapping) else {}
    strategy_family = _candidate_strategy_family(
        {
            **dict(signal),
            "execution_shape": execution_shape,
            "order_payload": order_payload,
            "economics": economics,
        },
        strategy_family=decision.get("trade_structure") or signal.get("trade_structure") or default_strategy_family,
    )
    quantity = (
        coerce_float(requested_quantity)
        or coerce_float(decision.get("selected_quantity"))
        or coerce_float(order_payload.get("qty") or order_payload.get("quantity") or execution_shape.get("quantity"))
        or 1.0
    )
    candidate = {
        **dict(signal),
        **economics,
        "strategy": strategy_family,
        "strategy_family": strategy_family,
        "trade_structure": decision.get("trade_structure") or signal.get("trade_structure") or strategy_family,
        "execution_shape": execution_shape,
        "order_payload": order_payload,
        "economics": economics,
    }
    candidate_symbol = _candidate_root_symbol(candidate)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        quantity,
        limit_price=limit_price,
    )
    unit_buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    candidate_max_loss = _candidate_max_loss(candidate, quantity)
    if candidate_max_loss is None:
        candidate_max_loss = required_buying_power
    per_contract_max_loss = _candidate_max_loss(candidate, 1.0)
    return {
        "trade_decision_id": as_text(decision.get("trade_decision_id")),
        "trade_signal_id": as_text(decision.get("trade_signal_id") or signal.get("trade_signal_id")),
        "trading_strategy_id": as_text(decision.get("trading_strategy_id") or signal.get("trading_strategy_id")),
        "strategy_family": strategy_family,
        "trade_structure": decision.get("trade_structure") or signal.get("trade_structure") or strategy_family,
        "underlying_symbol": candidate_symbol,
        "correlation_group": _portfolio_correlation_group(candidate_symbol),
        "score": coerce_float(decision.get("score") or signal.get("score")),
        "rank": coerce_int(decision.get("rank") or signal.get("rank")),
        "decided_at": as_text(decision.get("decided_at")),
        "requested_quantity": quantity,
        "candidate_max_loss": candidate_max_loss,
        "per_contract_max_loss": per_contract_max_loss,
        "required_buying_power": required_buying_power,
        "per_contract_required_buying_power": coerce_float(unit_buying_power_requirement.get("required_buying_power")),
        "buying_power_basis": as_text(buying_power_requirement.get("basis")),
        "candidate": candidate,
    }


def _allocation_exposure(row: Mapping[str, Any], *, session_date: str) -> dict[str, Any]:
    return {
        "source_type": ALLOCATION_PLAN_BOUNDARY,
        "source_id": as_text(row.get("trade_decision_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": as_text(row.get("strategy_family")),
        "underlying_symbol": as_text(row.get("underlying_symbol")),
        "correlation_group": as_text(row.get("correlation_group")),
        "market_date": session_date,
        "max_loss": coerce_float(row.get("candidate_max_loss")),
        "required_buying_power": coerce_float(row.get("required_buying_power")),
    }


def _allocation_item_result(
    row: Mapping[str, Any],
    *,
    allocation_rank: int,
    status: str,
    reason: str,
    message: str,
    policy: Mapping[str, Any],
    metrics: Mapping[str, Any],
    active_intent_id: str | None = None,
) -> dict[str, Any]:
    return {
        "trade_decision_id": row.get("trade_decision_id"),
        "trade_signal_id": row.get("trade_signal_id"),
        "trading_strategy_id": row.get("trading_strategy_id"),
        "strategy_family": row.get("strategy_family"),
        "underlying_symbol": row.get("underlying_symbol"),
        "correlation_group": row.get("correlation_group"),
        "score": row.get("score"),
        "rank": row.get("rank"),
        "allocation_rank": allocation_rank,
        "status": status,
        "reason": reason,
        "message": message,
        "requested_quantity": row.get("requested_quantity"),
        "admissible_quantity": row.get("requested_quantity") if status in {"allocated", "allocated_trimmed", "already_active"} else 0,
        "candidate_max_loss": row.get("candidate_max_loss"),
        "required_buying_power": row.get("required_buying_power"),
        "buying_power_basis": row.get("buying_power_basis"),
        "active_intent_id": active_intent_id,
        "policy": dict(policy),
        "metrics": dict(metrics),
    }


def _allocation_policy_block(
    row: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
    exposures: list[dict[str, Any]],
    daily_entries: list[dict[str, Any]],
) -> tuple[str, str, dict[str, Any]] | None:
    strategy_id = as_text(row.get("trading_strategy_id"))
    family = as_text(row.get("strategy_family"))
    symbol = as_text(row.get("underlying_symbol"))
    correlation_group = as_text(row.get("correlation_group"))
    candidate_max_loss = coerce_float(row.get("candidate_max_loss"))
    same_strategy = [item for item in exposures if as_text(item.get("trading_strategy_id")) == strategy_id]
    same_family = [item for item in exposures if as_text(item.get("strategy_family")) == family]
    same_symbol_family = [
        item for item in exposures if as_text(item.get("underlying_symbol")) == symbol and as_text(item.get("strategy_family")) == family
    ]
    same_correlation_group = [
        item for item in exposures if correlation_group is not None and as_text(item.get("correlation_group")) == correlation_group
    ]
    same_strategy_daily_entries = [item for item in daily_entries if as_text(item.get("trading_strategy_id")) == strategy_id]
    strategy_max_loss_before = money_sum_float(coerce_float(item.get("max_loss")) for item in same_strategy)
    strategy_max_loss_after = None if candidate_max_loss is None else money_sum_float([strategy_max_loss_before, candidate_max_loss])
    metrics = {
        "active_exposure_count": len(exposures),
        "same_strategy_count": len(same_strategy),
        "same_family_count": len(same_family),
        "same_symbol_family_count": len(same_symbol_family),
        "same_correlation_group_count": len(same_correlation_group),
        "daily_new_entry_count": len(same_strategy_daily_entries),
        "candidate_max_loss": candidate_max_loss,
        "strategy_max_loss_before": strategy_max_loss_before,
        "strategy_max_loss_after": strategy_max_loss_after,
    }

    max_symbol_family = _portfolio_policy_int(policy, "max_symbol_family_open_positions")
    if max_symbol_family is not None and len(same_symbol_family) >= max_symbol_family:
        return "allocation_duplicate_symbol_family_exposure", "Allocation would duplicate active symbol/family exposure.", metrics

    max_strategy = _portfolio_policy_int(policy, "max_strategy_open_positions")
    if max_strategy is not None and len(same_strategy) >= max_strategy:
        return "allocation_strategy_cap_reached", "Allocation would exceed the strategy active exposure cap.", metrics

    max_family = _portfolio_policy_int(policy, "max_family_open_positions")
    if max_family is not None and len(same_family) >= max_family:
        return "allocation_family_cap_reached", "Allocation would exceed the family active exposure cap.", metrics

    max_daily_entries = _portfolio_policy_int(policy, "max_daily_new_entries")
    if max_daily_entries is not None and len(same_strategy_daily_entries) >= max_daily_entries:
        return "allocation_daily_entry_cap_reached", "Allocation would exceed the strategy daily new-entry cap.", metrics

    max_total_strategy_risk = coerce_float(policy.get("max_total_strategy_risk"))
    if max_total_strategy_risk is not None and strategy_max_loss_after is not None and strategy_max_loss_after > max_total_strategy_risk:
        return "allocation_strategy_risk_budget_exceeded", "Allocation would exceed the strategy max-loss budget.", metrics

    max_correlated = _portfolio_policy_int(policy, "max_correlated_group_open_positions")
    if max_correlated is not None and len(same_correlation_group) >= max_correlated:
        return "allocation_correlated_exposure_limit_reached", "Allocation would exceed the correlated exposure cap.", metrics

    return None


def _allocation_plan_admission_evidence(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "allocation_plan_id": plan.get("allocation_plan_id"),
        "status": plan.get("status"),
        "reason": plan.get("reason"),
        "message": plan.get("message"),
        "summary": dict(plan.get("summary")) if isinstance(plan.get("summary"), Mapping) else {},
        "capital": dict(plan.get("capital")) if isinstance(plan.get("capital"), Mapping) else {},
        "schedule_constraints": dict(plan.get("schedule_constraints")) if isinstance(plan.get("schedule_constraints"), Mapping) else {},
        "current_decision": dict(plan.get("current_decision")) if isinstance(plan.get("current_decision"), Mapping) else {},
        "ranked_decisions": [
            dict(item)
            for item in list(plan.get("ranked_decisions") or [])[:10]
            if isinstance(item, Mapping)
        ],
    }


def _allocation_unavailable_plan(
    *,
    selected_decision: Mapping[str, Any],
    selected_signal: Mapping[str, Any],
    session_date: str,
    active_strategy_ids: tuple[str, ...],
    reason: str,
    message: str,
    status: str = "unknown",
) -> dict[str, Any]:
    trade_decision_id = as_text(selected_decision.get("trade_decision_id"))
    evaluated_at = utc_now_iso()
    current_decision = {
        "trade_decision_id": trade_decision_id,
        "trade_signal_id": as_text(selected_decision.get("trade_signal_id") or selected_signal.get("trade_signal_id")),
        "trading_strategy_id": as_text(selected_decision.get("trading_strategy_id") or selected_signal.get("trading_strategy_id")),
        "status": status,
        "reason": reason,
        "message": message,
        "allocation_rank": None,
        "admissible_quantity": 0,
    }
    return {
        "allocation_plan_id": f"allocation_plan:{safe_component(session_date)}:{safe_component(trade_decision_id)}",
        "status": status,
        "reason": reason,
        "message": message,
        "admission_boundary": ALLOCATION_PLAN_BOUNDARY,
        "current_decision": current_decision,
        "ranked_decisions": [current_decision],
        "summary": {
            "active_strategy_count": len(active_strategy_ids),
            "selected_decision_count": 1 if trade_decision_id else 0,
            "allocated_count": 0,
            "blocked_count": 0,
            "unknown_count": 1,
            "already_active_count": 0,
        },
        "capital": {},
        "schedule_constraints": {
            "mode": "observed_selected_decisions",
            "active_strategy_ids": list(active_strategy_ids),
            "selected_strategy_ids": [],
            "missing_selected_strategy_ids": list(active_strategy_ids),
        },
        "evaluated_at": evaluated_at,
    }


def build_allocation_plan_snapshot(
    *,
    engine_facts: Any,
    execution_store: Any,
    selected_decision: Mapping[str, Any],
    selected_signal: Mapping[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    active_strategy_ids: tuple[str, ...],
    portfolio_policies: Mapping[str, Mapping[str, Any]] | None,
    quantity: int | float = 1,
    limit_price: float | None = None,
) -> dict[str, Any]:
    trade_decision_id = as_text(selected_decision.get("trade_decision_id"))
    evaluated_at = utc_now_iso()
    allocation_plan_id = f"allocation_plan:{safe_component(session_date)}:{safe_component(trade_decision_id)}"
    if engine_facts is None or not engine_facts.schema_ready():
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_engine_fact_schema_unavailable",
            message="AllocationPlan could not read selected trade decisions.",
        )
    if not _portfolio_schema_ready(execution_store):
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_portfolio_schema_unavailable",
            message="AllocationPlan could not read portfolio exposure schemas.",
        )

    try:
        active_exposures = _open_portfolio_exposures(execution_store)
        daily_entries = _daily_entry_exposures(
            execution_store,
            session_date=session_date,
            active_exposures=active_exposures,
        )
    except Exception as exc:
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_portfolio_exposure_unavailable",
            message=str(exc),
        )

    selected_rows = engine_facts.list_trade_decisions_with_signals(
        decision_states=["selected"],
        trading_strategy_ids=list(active_strategy_ids),
        routine="entry",
        session_date=session_date,
        as_of=evaluated_at,
        limit=ALLOCATION_DECISION_LIMIT,
    )
    rows_by_decision_id: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in selected_rows:
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), Mapping) else {}
        signal = row.get("trade_signal") if isinstance(row.get("trade_signal"), Mapping) else {}
        decision_id = as_text(decision.get("trade_decision_id"))
        if decision_id is not None:
            rows_by_decision_id[decision_id] = {"trade_decision": dict(decision), "trade_signal": dict(signal)}
    if trade_decision_id is not None and trade_decision_id not in rows_by_decision_id:
        rows_by_decision_id[trade_decision_id] = {
            "trade_decision": dict(selected_decision),
            "trade_signal": dict(selected_signal),
        }

    contenders = [
        _allocation_decision_candidate(
            decision=row["trade_decision"],
            signal=row["trade_signal"],
            default_strategy_family=strategy_family,
            requested_quantity=quantity,
            limit_price=limit_price,
        )
        for row in rows_by_decision_id.values()
    ]
    contenders = [row for row in contenders if row.get("trade_decision_id") is not None]
    contenders.sort(key=_allocation_rank_key)

    broker_buying_power = live_broker_buying_power_snapshot(execution_store)
    broker_status = as_text(broker_buying_power.get("status"))
    remaining_buying_power = coerce_float(broker_buying_power.get("remaining_buying_power"))
    planned_exposures: list[dict[str, Any]] = []
    planned_daily_entries: list[dict[str, Any]] = []
    planned_required_buying_power = 0.0
    ranked_decisions: list[dict[str, Any]] = []
    policies = dict(portfolio_policies or {})

    for allocation_rank, contender in enumerate(contenders, start=1):
        contender_decision_id = as_text(contender.get("trade_decision_id"))
        policy = dict(policies.get(as_text(contender.get("trading_strategy_id")) or "") or {})
        active_intents = execution_store.list_execution_intents(
            trade_decision_id=contender_decision_id,
            states=list(ACTIVE_PORTFOLIO_INTENT_STATES),
            limit=1,
        )
        if active_intents:
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="already_active",
                    reason="allocation_decision_already_active",
                    message="Selected decision already has an active execution intent.",
                    policy=policy,
                    metrics={"active_exposure_count": len(active_exposures)},
                    active_intent_id=as_text(active_intents[0].get("execution_intent_id")),
                )
            )
            continue

        if contender.get("underlying_symbol") is None or contender.get("strategy_family") is None:
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="unknown",
                    reason="allocation_candidate_identity_unavailable",
                    message="AllocationPlan could not resolve the candidate symbol and strategy family.",
                    policy=policy,
                    metrics={},
                )
            )
            continue

        exposure_view = [*active_exposures, *planned_exposures]
        daily_view = [*daily_entries, *planned_daily_entries]
        policy_block = _allocation_policy_block(
            contender,
            policy=policy,
            exposures=exposure_view,
            daily_entries=daily_view,
        )
        if policy_block is not None:
            reason, message, metrics = policy_block
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="blocked",
                    reason=reason,
                    message=message,
                    policy=policy,
                    metrics=metrics,
                )
            )
            continue

        required_buying_power = coerce_float(contender.get("required_buying_power"))
        if broker_status != "ok":
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="unknown",
                    reason="allocation_buying_power_unavailable",
                    message=as_text(broker_buying_power.get("error_text")) or "Broker buying power is unavailable for allocation.",
                    policy=policy,
                    metrics={"broker_buying_power_status": broker_status},
                )
            )
            continue
        if required_buying_power is None:
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="unknown",
                    reason="allocation_buying_power_requirement_unavailable",
                    message="AllocationPlan could not estimate buying power for this structure.",
                    policy=policy,
                    metrics={},
                )
            )
            continue
        if remaining_buying_power is not None and planned_required_buying_power + required_buying_power > remaining_buying_power:
            available_for_decision = max(remaining_buying_power - planned_required_buying_power, 0.0)
            per_contract_required_buying_power = coerce_float(contender.get("per_contract_required_buying_power"))
            trimmed_quantity = (
                0
                if per_contract_required_buying_power is None or per_contract_required_buying_power <= 0
                else int(available_for_decision // per_contract_required_buying_power)
            )
            requested_quantity = coerce_float(contender.get("requested_quantity")) or 1.0
            if 0 < trimmed_quantity < requested_quantity:
                trimmed = {
                    **contender,
                    "requested_quantity": trimmed_quantity,
                    "required_buying_power": money_scaled_float(per_contract_required_buying_power, trimmed_quantity),
                    "candidate_max_loss": money_scaled_float(
                        coerce_float(contender.get("per_contract_max_loss")),
                        trimmed_quantity,
                    ),
                }
                allocated = _allocation_item_result(
                    trimmed,
                    allocation_rank=allocation_rank,
                    status="allocated_trimmed",
                    reason="allocation_quantity_trimmed",
                    message="AllocationPlan trimmed quantity to fit remaining broker buying power.",
                    policy=policy,
                    metrics={
                        "remaining_buying_power": remaining_buying_power,
                        "planned_required_buying_power_before": money_float(planned_required_buying_power),
                        "requested_quantity": requested_quantity,
                        "admissible_quantity": trimmed_quantity,
                        "per_contract_required_buying_power": per_contract_required_buying_power,
                    },
                )
                ranked_decisions.append(allocated)
                planned_exposure = _allocation_exposure(trimmed, session_date=session_date)
                planned_exposures.append(planned_exposure)
                planned_daily_entries.append(planned_exposure)
                planned_required_buying_power += coerce_float(trimmed.get("required_buying_power")) or 0.0
                continue
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="blocked",
                    reason="allocation_buying_power_limit_reached",
                    message="Allocation would exceed remaining broker buying power after higher-ranked selections.",
                    policy=policy,
                    metrics={
                        "remaining_buying_power": remaining_buying_power,
                        "planned_required_buying_power_before": money_float(planned_required_buying_power),
                        "required_buying_power": required_buying_power,
                    },
                )
            )
            continue

        allocated = _allocation_item_result(
            contender,
            allocation_rank=allocation_rank,
            status="allocated",
            reason="allocation_selected",
            message="AllocationPlan selected this decision for portfolio admission.",
            policy=policy,
            metrics={
                "active_exposure_count": len(active_exposures),
                "planned_required_buying_power_before": money_float(planned_required_buying_power),
                "required_buying_power": required_buying_power,
            },
        )
        ranked_decisions.append(allocated)
        planned_exposure = _allocation_exposure(contender, session_date=session_date)
        planned_exposures.append(planned_exposure)
        planned_daily_entries.append(planned_exposure)
        planned_required_buying_power += required_buying_power

    current_decision = next(
        (row for row in ranked_decisions if as_text(row.get("trade_decision_id")) == trade_decision_id),
        None,
    )
    if current_decision is None:
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_current_decision_missing",
            message="AllocationPlan could not find the current selected decision in the selected-decision universe.",
        )

    selected_strategy_ids = sorted(
        {
            strategy_id
            for strategy_id in (as_text(row.get("trading_strategy_id")) for row in ranked_decisions)
            if strategy_id is not None
        }
    )
    status = as_text(current_decision.get("status")) or "unknown"
    plan_status = "allocated" if status in {"allocated", "allocated_trimmed", "already_active"} else status
    reason = as_text(current_decision.get("reason")) or "allocation_unknown"
    message = as_text(current_decision.get("message")) or "AllocationPlan did not produce a message."
    return {
        "allocation_plan_id": allocation_plan_id,
        "status": plan_status,
        "reason": reason,
        "message": message,
        "admission_boundary": ALLOCATION_PLAN_BOUNDARY,
        "current_decision": current_decision,
        "ranked_decisions": ranked_decisions,
        "summary": {
            "active_strategy_count": len(active_strategy_ids),
            "selected_strategy_count": len(selected_strategy_ids),
            "selected_decision_count": len(ranked_decisions),
            "allocated_count": sum(1 for row in ranked_decisions if row.get("status") == "allocated"),
            "blocked_count": sum(1 for row in ranked_decisions if row.get("status") == "blocked"),
            "unknown_count": sum(1 for row in ranked_decisions if row.get("status") == "unknown"),
            "already_active_count": sum(1 for row in ranked_decisions if row.get("status") == "already_active"),
            "planned_required_buying_power": money_float(planned_required_buying_power),
            "active_exposure_count": len(active_exposures),
        },
        "capital": {
            "broker_buying_power_status": broker_status,
            "available_buying_power": coerce_float(broker_buying_power.get("available_buying_power")),
            "reserved_buying_power": coerce_float(broker_buying_power.get("reserved_buying_power")),
            "remaining_buying_power": remaining_buying_power,
            "planned_required_buying_power": money_float(planned_required_buying_power),
            "buying_power_source_field": as_text(broker_buying_power.get("source_field")),
            "reservation_count": coerce_int(broker_buying_power.get("reservation_count")),
        },
        "schedule_constraints": {
            "mode": "observed_selected_decisions",
            "active_strategy_ids": list(active_strategy_ids),
            "selected_strategy_ids": selected_strategy_ids,
            "missing_selected_strategy_ids": [strategy_id for strategy_id in active_strategy_ids if strategy_id not in selected_strategy_ids],
            "decision_limit": ALLOCATION_DECISION_LIMIT,
        },
        "evaluated_at": evaluated_at,
    }


def _protection_rule_enabled(rule: Mapping[str, Any]) -> bool:
    return bool(coerce_bool(rule.get("enabled"), default=False))


def _protection_positive_int(rule: Mapping[str, Any], key: str) -> int | None:
    value = coerce_int(rule.get(key))
    if value is None or value <= 0:
        return None
    return value


def _protection_positive_float(rule: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = coerce_float(rule.get(key))
        if value is not None and value > 0:
            return value
    return None


def _protection_activity_at(row: Mapping[str, Any]) -> datetime | None:
    for key in ("closed_at", "opened_at", "updated_at", "created_at", "requested_at"):
        value = row.get(key)
        if value in (None, ""):
            continue
        parsed = coerce_utc_datetime(value)
        if parsed is not None:
            return parsed.astimezone(UTC)
    date_text = _date_text(row.get("market_date_closed") or row.get("market_date_opened") or row.get("market_date"))
    if date_text is None:
        return None
    try:
        return datetime.fromisoformat(date_text).replace(tzinfo=UTC)
    except ValueError:
        return None


def _position_net_pnl(row: Mapping[str, Any]) -> float:
    return money_sum_float(
        [
            coerce_float(row.get("realized_pnl")) or 0.0,
            coerce_float(row.get("unrealized_pnl")) or 0.0,
        ]
    )


def _scoped_positions(
    positions: list[dict[str, Any]],
    *,
    rule: Mapping[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
) -> list[dict[str, Any]]:
    scope = as_text(rule.get("scope")) or "account"
    if scope == "strategy":
        return [row for row in positions if as_text(row.get("trading_strategy_id")) == trading_strategy_id]
    if scope in {"strategy_family", "family"}:
        return [row for row in positions if as_text(row.get("strategy_family") or row.get("trade_structure")) == strategy_family]
    return list(positions)


def _protection_block_item(reason: str, message: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "reason": reason,
        "message": message,
        "metrics": dict(metrics),
    }


def _account_emergency_stop_block(rule: Mapping[str, Any]) -> dict[str, Any] | None:
    if not _protection_rule_enabled(rule):
        return None
    configured_halt = bool(
        coerce_bool(rule.get("halted"), default=False)
        or coerce_bool(rule.get("emergency_stop"), default=False)
        or coerce_bool(rule.get("triggered"), default=False)
    )
    env_halt = bool(
        coerce_bool(os.environ.get("SPREADS_ACCOUNT_EMERGENCY_STOP"), default=False)
        or coerce_bool(os.environ.get("SPREADS_EXECUTION_KILL_SWITCH"), default=False)
    )
    metrics = {
        "configured_halt": configured_halt,
        "environment_halt": env_halt,
    }
    if configured_halt or env_halt:
        return _protection_block_item(
            "account_emergency_stop",
            "Account-level emergency stop is active.",
            metrics,
        )
    return None


def _drawdown_block(
    *,
    rule_name: str,
    rule: Mapping[str, Any],
    positions: list[dict[str, Any]],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not _protection_rule_enabled(rule):
        return {"enabled": False}, None
    scoped = _scoped_positions(
        positions,
        rule=rule,
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    if rule_name == "daily_drawdown_halt":
        window_positions = [
            row
            for row in scoped
            if session_date
            in {
                _date_text(row.get("market_date_opened") or row.get("opened_at")),
                _date_text(row.get("market_date_closed") or row.get("closed_at")),
                _date_text(row.get("updated_at")),
            }
        ]
    else:
        window_days = _protection_positive_int(rule, "window_days") or 5
        start = now - timedelta(days=window_days)
        window_positions = [
            row
            for row in scoped
            if (activity_at := _protection_activity_at(row)) is not None and activity_at >= start
        ]

    open_positions = [row for row in scoped if as_text(row.get("status")) in OPEN_POSITION_STATUSES]
    realized = money_sum_float(coerce_float(row.get("realized_pnl")) for row in window_positions)
    unrealized = money_sum_float(coerce_float(row.get("unrealized_pnl")) for row in open_positions)
    net = money_sum_float([realized, unrealized])
    max_realized_loss = _protection_positive_float(rule, "max_realized_loss", "max_loss")
    max_net_loss = _protection_positive_float(rule, "max_net_loss", "max_drawdown")
    metrics = {
        "enabled": True,
        "scope": as_text(rule.get("scope")) or "account",
        "position_count": len(window_positions),
        "open_position_count": len(open_positions),
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "net_pnl": net,
        "max_realized_loss": max_realized_loss,
        "max_net_loss": max_net_loss,
    }
    if rule_name == "rolling_drawdown_halt":
        metrics["window_days"] = _protection_positive_int(rule, "window_days") or 5
    if max_realized_loss is not None and realized <= -abs(max_realized_loss):
        return metrics, _protection_block_item(
            f"{rule_name}_realized_loss",
            "Realized PnL breached the configured protection loss limit.",
            metrics,
        )
    if max_net_loss is not None and net <= -abs(max_net_loss):
        return metrics, _protection_block_item(
            f"{rule_name}_net_loss",
            "Net PnL breached the configured protection drawdown limit.",
            metrics,
        )
    return metrics, None


def _closed_loss_positions(
    positions: list[dict[str, Any]],
    *,
    rule: Mapping[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
) -> list[dict[str, Any]]:
    scoped = _scoped_positions(
        positions,
        rule=rule,
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    closed = [
        row
        for row in scoped
        if as_text(row.get("status")) == "closed" or row.get("closed_at") is not None or row.get("market_date_closed") is not None
    ]
    closed.sort(key=lambda row: _protection_activity_at(row) or datetime.min.replace(tzinfo=UTC), reverse=True)
    return closed


def _loss_streak_block(
    *,
    rule: Mapping[str, Any],
    positions: list[dict[str, Any]],
    trading_strategy_id: str,
    strategy_family: str | None,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not _protection_rule_enabled(rule):
        return {"enabled": False}, None
    closed = _closed_loss_positions(
        positions,
        rule=rule,
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    loss_threshold = abs(coerce_float(rule.get("loss_threshold")) or 0.0)
    streak = 0
    latest_loss_at: datetime | None = None
    for row in closed:
        net_pnl = _position_net_pnl(row)
        if net_pnl < -loss_threshold:
            streak += 1
            latest_loss_at = latest_loss_at or _protection_activity_at(row)
            continue
        break
    max_losses = _protection_positive_int(rule, "max_consecutive_losses")
    cooldown_minutes = _protection_positive_int(rule, "cooldown_minutes")
    cooldown_active = False
    if max_losses is not None and streak >= max_losses and latest_loss_at is not None:
        cooldown_active = cooldown_minutes is None or now < latest_loss_at + timedelta(minutes=cooldown_minutes)
    metrics = {
        "enabled": True,
        "scope": as_text(rule.get("scope")) or "account",
        "closed_position_count": len(closed),
        "consecutive_loss_count": streak,
        "max_consecutive_losses": max_losses,
        "cooldown_minutes": cooldown_minutes,
        "latest_loss_at": None if latest_loss_at is None else latest_loss_at.isoformat(),
        "cooldown_active": cooldown_active,
    }
    if cooldown_active:
        return metrics, _protection_block_item(
            "loss_streak_cooldown_active",
            "Recent consecutive losses activated the configured cooldown.",
            metrics,
        )
    return metrics, None


def _strategy_family_cooldown_block(
    *,
    rule: Mapping[str, Any],
    positions: list[dict[str, Any]],
    trading_strategy_id: str,
    strategy_family: str | None,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not _protection_rule_enabled(rule):
        return {"enabled": False}, None
    family_positions = _scoped_positions(
        positions,
        rule={"scope": "strategy_family"},
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    latest_entry_at = max(
        (_protection_activity_at(row) for row in family_positions if as_text(row.get("status")) in OPEN_POSITION_STATUSES),
        default=None,
    )
    loss_rule = {**dict(rule), "scope": "strategy_family"}
    latest_loss = next(
        (
            row
            for row in _closed_loss_positions(
                positions,
                rule=loss_rule,
                trading_strategy_id=trading_strategy_id,
                strategy_family=strategy_family,
            )
            if _position_net_pnl(row) < 0
        ),
        None,
    )
    latest_loss_at = None if latest_loss is None else _protection_activity_at(latest_loss)
    after_entry_minutes = _protection_positive_int(rule, "cooldown_minutes_after_entry")
    after_loss_minutes = _protection_positive_int(rule, "cooldown_minutes_after_loss")
    entry_cooldown_active = (
        after_entry_minutes is not None and latest_entry_at is not None and now < latest_entry_at + timedelta(minutes=after_entry_minutes)
    )
    loss_cooldown_active = (
        after_loss_minutes is not None and latest_loss_at is not None and now < latest_loss_at + timedelta(minutes=after_loss_minutes)
    )
    metrics = {
        "enabled": True,
        "strategy_family": strategy_family,
        "family_position_count": len(family_positions),
        "latest_entry_at": None if latest_entry_at is None else latest_entry_at.isoformat(),
        "latest_loss_at": None if latest_loss_at is None else latest_loss_at.isoformat(),
        "cooldown_minutes_after_entry": after_entry_minutes,
        "cooldown_minutes_after_loss": after_loss_minutes,
        "entry_cooldown_active": entry_cooldown_active,
        "loss_cooldown_active": loss_cooldown_active,
    }
    if entry_cooldown_active:
        return metrics, _protection_block_item(
            "strategy_family_entry_cooldown_active",
            "Strategy-family entry cooldown is active.",
            metrics,
        )
    if loss_cooldown_active:
        return metrics, _protection_block_item(
            "strategy_family_loss_cooldown_active",
            "Strategy-family loss cooldown is active.",
            metrics,
        )
    return metrics, None


def _event_calendar_block(
    *,
    rule: Mapping[str, Any],
    candidate_symbol: str | None,
    session_date: str,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not _protection_rule_enabled(rule):
        return {"enabled": False}, None
    blocked_dates = set(unique_text_list(rule.get("blocked_dates"), accept_scalar=True))
    blocked_symbols = {item.upper() for item in unique_text_list(rule.get("blocked_symbols"), accept_scalar=True)}
    blocked_until_value = rule.get("blocked_until")
    blocked_until = None if blocked_until_value in (None, "") else coerce_utc_datetime(blocked_until_value)
    events = [dict(item) for item in rule.get("events", []) if isinstance(item, Mapping)]
    matching_events = [
        event
        for event in events
        if (
            as_text(event.get("date")) == session_date
            and (
                candidate_symbol is None
                or not unique_text_list(event.get("symbols"), accept_scalar=True)
                or candidate_symbol in {symbol.upper() for symbol in unique_text_list(event.get("symbols"), accept_scalar=True)}
            )
        )
    ]
    blocked = bool(
        coerce_bool(rule.get("blocked"), default=False)
        or session_date in blocked_dates
        or (candidate_symbol is not None and candidate_symbol in blocked_symbols)
        or (blocked_until is not None and now < blocked_until)
        or matching_events
    )
    metrics = {
        "enabled": True,
        "blocked": blocked,
        "blocked_date_count": len(blocked_dates),
        "blocked_symbol_count": len(blocked_symbols),
        "matching_event_count": len(matching_events),
        "blocked_until": None if blocked_until is None else blocked_until.isoformat(),
    }
    if blocked:
        return metrics, _protection_block_item(
            "event_calendar_block",
            "Event/news/calendar protection blocks new entries.",
            metrics,
        )
    return metrics, None


def _duplicate_exposure_block(
    *,
    rule: Mapping[str, Any],
    active_exposures: list[dict[str, Any]],
    candidate_symbol: str | None,
    candidate_correlation_group: str | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not _protection_rule_enabled(rule):
        return {"enabled": False}, None
    same_underlying = [row for row in active_exposures if as_text(row.get("underlying_symbol")) == candidate_symbol]
    same_theme = [
        row
        for row in active_exposures
        if candidate_correlation_group is not None
        and candidate_correlation_group != candidate_symbol
        and as_text(row.get("correlation_group")) == candidate_correlation_group
    ]
    max_same_underlying = _protection_positive_int(rule, "max_open_same_underlying")
    max_same_theme = _protection_positive_int(rule, "max_open_same_theme")
    metrics = {
        "enabled": True,
        "candidate_symbol": candidate_symbol,
        "candidate_theme": candidate_correlation_group,
        "same_underlying_count": len(same_underlying),
        "same_theme_count": len(same_theme),
        "max_open_same_underlying": max_same_underlying,
        "max_open_same_theme": max_same_theme,
    }
    if max_same_underlying is not None and len(same_underlying) >= max_same_underlying:
        return metrics, _protection_block_item(
            "duplicate_underlying_exposure_cap",
            "Active portfolio exposure already exists for this underlying.",
            metrics,
        )
    if max_same_theme is not None and len(same_theme) >= max_same_theme:
        return metrics, _protection_block_item(
            "duplicate_theme_exposure_cap",
            "Active portfolio exposure already reaches the configured theme cap.",
            metrics,
        )
    return metrics, None


def _short_leg_contract_count(candidate: Mapping[str, Any], quantity: float) -> float:
    legs = candidate_legs(dict(candidate))
    if not legs:
        family = _candidate_strategy_family(candidate)
        return quantity if family in {"short_call", "short_put", "call_credit_spread", "put_credit_spread", "iron_condor"} else 0.0
    short_leg_count = sum(1 for leg in legs if as_text(leg.get("role")) == "short")
    return float(short_leg_count) * quantity


def _active_short_contract_count(positions: list[dict[str, Any]]) -> float:
    total = 0.0
    for row in positions:
        if as_text(row.get("status")) not in OPEN_POSITION_STATUSES:
            continue
        quantity = coerce_float(row.get("remaining_quantity")) or 0.0
        legs = position_legs(row)
        if not legs:
            continue
        total += quantity * sum(1 for leg in legs if as_text(leg.get("role")) == "short")
    return total


def _options_exposure_block(
    *,
    rule: Mapping[str, Any],
    active_exposures: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    candidate: Mapping[str, Any],
    quantity: float,
    candidate_max_loss: float | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not _protection_rule_enabled(rule):
        return {"enabled": False}, None
    active_contracts = money_sum_float(coerce_float(row.get("contract_count")) or 1.0 for row in active_exposures)
    active_max_loss = money_sum_float(coerce_float(row.get("max_loss")) for row in active_exposures)
    active_short_contracts = _active_short_contract_count(positions)
    candidate_short_contracts = _short_leg_contract_count(candidate, quantity)
    max_open_contracts = _protection_positive_int(rule, "max_open_option_contracts")
    max_total_max_loss = _protection_positive_float(rule, "max_total_max_loss", "max_scenario_loss")
    max_short_contracts = _protection_positive_int(rule, "max_short_option_contracts")
    total_contracts_after = active_contracts + quantity
    total_max_loss_after = None if candidate_max_loss is None else money_sum_float([active_max_loss, candidate_max_loss])
    total_short_after = active_short_contracts + candidate_short_contracts
    metrics = {
        "enabled": True,
        "active_option_contract_count": active_contracts,
        "candidate_option_contract_count": quantity,
        "total_option_contract_count_after": total_contracts_after,
        "active_max_loss": active_max_loss,
        "candidate_max_loss": candidate_max_loss,
        "total_max_loss_after": total_max_loss_after,
        "active_short_option_contract_count": active_short_contracts,
        "candidate_short_option_contract_count": candidate_short_contracts,
        "total_short_option_contract_count_after": total_short_after,
        "max_open_option_contracts": max_open_contracts,
        "max_total_max_loss": max_total_max_loss,
        "max_short_option_contracts": max_short_contracts,
    }
    if max_open_contracts is not None and total_contracts_after > max_open_contracts:
        return metrics, _protection_block_item(
            "options_contract_exposure_cap",
            "Open option contract exposure would exceed the configured cap.",
            metrics,
        )
    if max_total_max_loss is not None and total_max_loss_after is not None and total_max_loss_after > max_total_max_loss:
        return metrics, _protection_block_item(
            "options_max_loss_scenario_cap",
            "Option max-loss scenario exposure would exceed the configured cap.",
            metrics,
        )
    if max_short_contracts is not None and total_short_after > max_short_contracts:
        return metrics, _protection_block_item(
            "options_short_contract_exposure_cap",
            "Short option contract exposure would exceed the configured cap.",
            metrics,
        )
    return metrics, None


def _protection_payload(
    *,
    status: str,
    reason: str,
    message: str,
    policy: Mapping[str, Any],
    metrics: Mapping[str, Any],
    evidence: Mapping[str, Any],
    blockers: list[str],
    evaluated_at: str,
) -> dict[str, Any]:
    reason_codes = [reason] if reason else []
    for blocker in blockers:
        if blocker not in reason_codes:
            reason_codes.append(blocker)
    return {
        "status": status,
        "reason": reason,
        "message": message,
        "admission_boundary": PROTECTION_ADMISSION_BOUNDARY,
        "admissible_quantity": 1 if status in PROTECTION_ADMISSIBLE_STATUSES else 0,
        "reason_codes": reason_codes,
        "blockers": blockers,
        "policy": dict(policy),
        "metrics": dict(metrics),
        "evidence": dict(evidence),
        "evaluated_at": evaluated_at,
    }


def build_protection_admission_snapshot(
    *,
    execution_store: Any,
    candidate: dict[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    policy: Mapping[str, Any] | None,
    quantity: int | float = 1,
    limit_price: float | None = None,
    allocation_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    evaluated_at = utc_now_iso()
    now = coerce_utc_datetime(evaluated_at) or datetime.now(UTC)
    normalized_policy = dict(policy or {})
    raw_rules = as_mapping(normalized_policy.get("rules"))
    rules = {rule_name: as_mapping(raw_rules.get(rule_name)) for rule_name in PROTECTION_RULE_KEYS}
    if not _portfolio_schema_ready(execution_store):
        return _protection_payload(
            status="unknown",
            reason="protection_schema_unavailable",
            message="Protection admission could not read the required lifecycle schemas.",
            policy=normalized_policy,
            metrics={},
            evidence={},
            blockers=["protection_schema_unavailable"],
            evaluated_at=evaluated_at,
        )

    try:
        active_exposures = _open_portfolio_exposures(execution_store)
        positions = [dict(row) for row in execution_store.list_positions(limit=1000)]
    except Exception as exc:
        return _protection_payload(
            status="unknown",
            reason="protection_state_unavailable",
            message=str(exc),
            policy=normalized_policy,
            metrics={},
            evidence={},
            blockers=["protection_state_unavailable"],
            evaluated_at=evaluated_at,
        )

    resolved_quantity = coerce_float(quantity) or 1.0
    candidate_symbol = _candidate_root_symbol(candidate)
    candidate_family = _candidate_strategy_family(candidate, strategy_family=strategy_family)
    candidate_correlation_group = _portfolio_correlation_group(candidate_symbol)
    candidate_max_loss = _candidate_max_loss(candidate, resolved_quantity)
    if candidate_max_loss is None:
        requirement = estimate_buying_power_requirement(candidate, resolved_quantity, limit_price=limit_price)
        candidate_max_loss = coerce_float(requirement.get("required_buying_power"))
    if candidate_symbol is None or candidate_family is None:
        return _protection_payload(
            status="unknown",
            reason="protection_candidate_identity_unavailable",
            message="Protection admission could not resolve the candidate symbol and strategy family.",
            policy=normalized_policy,
            metrics={},
            evidence={"candidate_symbol": candidate_symbol, "strategy_family": candidate_family},
            blockers=["protection_candidate_identity_unavailable"],
            evaluated_at=evaluated_at,
        )

    metrics: dict[str, Any] = {
        "rule_count": len([rule for rule in rules.values() if rule]),
        "enabled_rule_count": sum(1 for rule in rules.values() if _protection_rule_enabled(rule)),
        "active_exposure_count": len(active_exposures),
        "position_count": len(positions),
        "candidate_symbol": candidate_symbol,
        "candidate_strategy_family": candidate_family,
        "candidate_correlation_group": candidate_correlation_group,
        "candidate_quantity": resolved_quantity,
        "candidate_max_loss": candidate_max_loss,
    }
    evidence: dict[str, Any] = {
        "candidate": {
            "underlying_symbol": candidate_symbol,
            "strategy_family": candidate_family,
            "trading_strategy_id": trading_strategy_id,
            "correlation_group": candidate_correlation_group,
        },
        "active_exposures": active_exposures[:25],
    }
    if isinstance(allocation_plan, Mapping):
        evidence["allocation_plan"] = _allocation_plan_admission_evidence(allocation_plan)

    blocks: list[dict[str, Any]] = []
    account_block = _account_emergency_stop_block(rules["account_emergency_stop"])
    if account_block is not None:
        blocks.append(account_block)

    for rule_name in ("daily_drawdown_halt", "rolling_drawdown_halt"):
        rule_metrics, block = _drawdown_block(
            rule_name=rule_name,
            rule=rules[rule_name],
            positions=positions,
            trading_strategy_id=trading_strategy_id,
            strategy_family=candidate_family,
            session_date=session_date,
            now=now,
        )
        metrics[rule_name] = rule_metrics
        if block is not None:
            blocks.append(block)

    rule_metrics, block = _loss_streak_block(
        rule=rules["loss_streak_cooldown"],
        positions=positions,
        trading_strategy_id=trading_strategy_id,
        strategy_family=candidate_family,
        now=now,
    )
    metrics["loss_streak_cooldown"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _strategy_family_cooldown_block(
        rule=rules["strategy_family_cooldown"],
        positions=positions,
        trading_strategy_id=trading_strategy_id,
        strategy_family=candidate_family,
        now=now,
    )
    metrics["strategy_family_cooldown"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _event_calendar_block(
        rule=rules["event_calendar_block"],
        candidate_symbol=candidate_symbol,
        session_date=session_date,
        now=now,
    )
    metrics["event_calendar_block"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _duplicate_exposure_block(
        rule=rules["duplicate_underlying_theme_cap"],
        active_exposures=active_exposures,
        candidate_symbol=candidate_symbol,
        candidate_correlation_group=candidate_correlation_group,
    )
    metrics["duplicate_underlying_theme_cap"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _options_exposure_block(
        rule=rules["options_exposure_scenario_cap"],
        active_exposures=active_exposures,
        positions=positions,
        candidate=candidate,
        quantity=resolved_quantity,
        candidate_max_loss=candidate_max_loss,
    )
    metrics["options_exposure_scenario_cap"] = rule_metrics
    if block is not None:
        blocks.append(block)

    if blocks:
        blockers = unique_text_list([block["reason"] for block in blocks], accept_scalar=True)
        evidence["blockers"] = blocks
        return _protection_payload(
            status="blocked",
            reason=blockers[0],
            message=as_text(blocks[0].get("message")) or "Protection policy blocked this entry.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            blockers=blockers,
            evaluated_at=evaluated_at,
        )

    return _protection_payload(
        status="admissible",
        reason="protection_admissible",
        message="Protection policy allows this entry.",
        policy=normalized_policy,
        metrics=metrics,
        evidence=evidence,
        blockers=[],
        evaluated_at=evaluated_at,
    )


def _portfolio_block_payload(
    *,
    reason: str,
    message: str,
    policy: Mapping[str, Any],
    metrics: Mapping[str, Any],
    evidence: Mapping[str, Any],
    status: str = "blocked",
    evaluated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "message": message,
        "admission_boundary": PORTFOLIO_ADMISSION_BOUNDARY,
        "admissible_quantity": 0 if status == "blocked" else None,
        "reason_codes": [reason],
        "blockers": [reason],
        "policy": dict(policy),
        "metrics": dict(metrics),
        "evidence": dict(evidence),
        "evaluated_at": evaluated_at or utc_now_iso(),
    }


def build_portfolio_admission_snapshot(
    *,
    execution_store: Any,
    candidate: dict[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    policy: Mapping[str, Any] | None,
    quantity: int | float = 1,
    limit_price: float | None = None,
    allocation_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    evaluated_at = utc_now_iso()
    normalized_policy = dict(policy or {})
    allocation_evidence = _allocation_plan_admission_evidence(allocation_plan) if isinstance(allocation_plan, Mapping) else {}
    if not _portfolio_schema_ready(execution_store):
        return _portfolio_block_payload(
            status="unknown",
            reason="portfolio_schema_unavailable",
            message="Portfolio admission could not read the required lifecycle schemas.",
            policy=normalized_policy,
            metrics={},
            evidence={},
            evaluated_at=evaluated_at,
        )

    try:
        active_exposures = _open_portfolio_exposures(execution_store)
        daily_entries = _daily_entry_exposures(
            execution_store,
            session_date=session_date,
            active_exposures=active_exposures,
        )
    except Exception as exc:
        return _portfolio_block_payload(
            status="unknown",
            reason="portfolio_admission_unavailable",
            message=str(exc),
            policy=normalized_policy,
            metrics={},
            evidence={},
            evaluated_at=evaluated_at,
        )

    candidate_symbol = _candidate_root_symbol(candidate)
    candidate_family = _candidate_strategy_family(candidate, strategy_family=strategy_family)
    if candidate_symbol is None or candidate_family is None:
        return _portfolio_block_payload(
            status="unknown",
            reason="portfolio_candidate_identity_unavailable",
            message="Portfolio admission could not resolve the candidate symbol and strategy family.",
            policy=normalized_policy,
            metrics={},
            evidence={"candidate_symbol": candidate_symbol, "strategy_family": candidate_family},
            evaluated_at=evaluated_at,
        )

    resolved_quantity = coerce_float(quantity) or 1.0
    candidate_max_loss = _candidate_max_loss(candidate, resolved_quantity)
    if candidate_max_loss is None:
        requirement = estimate_buying_power_requirement(candidate, resolved_quantity, limit_price=limit_price)
        candidate_max_loss = coerce_float(requirement.get("required_buying_power"))

    candidate_correlation_group = _portfolio_correlation_group(candidate_symbol)
    same_strategy = [row for row in active_exposures if as_text(row.get("trading_strategy_id")) == trading_strategy_id]
    same_family = [row for row in active_exposures if row.get("strategy_family") == candidate_family]
    same_symbol_family = [
        row for row in active_exposures if row.get("underlying_symbol") == candidate_symbol and row.get("strategy_family") == candidate_family
    ]
    same_correlation_group = [
        row for row in active_exposures if candidate_correlation_group is not None and row.get("correlation_group") == candidate_correlation_group
    ]
    strategy_max_loss_before = money_sum_float(coerce_float(row.get("max_loss")) for row in same_strategy)
    strategy_max_loss_after = None if candidate_max_loss is None else money_sum_float([strategy_max_loss_before, candidate_max_loss])
    same_strategy_daily_entries = [row for row in daily_entries if as_text(row.get("trading_strategy_id")) == trading_strategy_id]

    metrics = {
        "active_exposure_count": len(active_exposures),
        "same_strategy_count": len(same_strategy),
        "same_family_count": len(same_family),
        "same_symbol_family_count": len(same_symbol_family),
        "same_correlation_group_count": len(same_correlation_group),
        "daily_new_entry_count": len(same_strategy_daily_entries),
        "candidate_max_loss": candidate_max_loss,
        "strategy_max_loss_before": strategy_max_loss_before,
        "strategy_max_loss_after": strategy_max_loss_after,
        "candidate_symbol": candidate_symbol,
        "candidate_strategy_family": candidate_family,
        "candidate_correlation_group": candidate_correlation_group,
    }
    evidence = {
        "candidate": {
            "underlying_symbol": candidate_symbol,
            "strategy_family": candidate_family,
            "trading_strategy_id": trading_strategy_id,
            "correlation_group": candidate_correlation_group,
        },
        "matching_symbol_family_exposures": same_symbol_family[:10],
        "matching_strategy_exposures": same_strategy[:10],
        "matching_family_exposures": same_family[:10],
        "matching_correlation_exposures": same_correlation_group[:10],
        "daily_entry_exposures": same_strategy_daily_entries[:10],
    }
    if allocation_evidence:
        allocation_decision = (
            dict(allocation_evidence.get("current_decision"))
            if isinstance(allocation_evidence.get("current_decision"), Mapping)
            else {}
        )
        allocation_status = as_text(allocation_decision.get("status") or allocation_evidence.get("status")) or "unknown"
        allocation_reason = as_text(allocation_decision.get("reason") or allocation_evidence.get("reason")) or "allocation_plan_not_selected"
        metrics.update(
            {
                "allocation_plan_status": allocation_status,
                "allocation_plan_reason": allocation_reason,
                "allocation_rank": allocation_decision.get("allocation_rank"),
                "allocation_selected_decision_count": (
                    as_mapping(allocation_evidence.get("summary")).get("selected_decision_count")
                ),
                "allocation_allocated_count": as_mapping(allocation_evidence.get("summary")).get("allocated_count"),
            }
        )
        evidence["allocation_plan"] = allocation_evidence
        evidence["allocation_decision"] = allocation_decision
        if allocation_status not in {"allocated", "allocated_trimmed", "already_active"}:
            return _portfolio_block_payload(
                status="unknown" if allocation_status == "unknown" else "blocked",
                reason=allocation_reason,
                message=as_text(allocation_decision.get("message") or allocation_evidence.get("message"))
                or "AllocationPlan did not allocate this selected decision.",
                policy=normalized_policy,
                metrics=metrics,
                evidence=evidence,
                evaluated_at=evaluated_at,
            )

    max_symbol_family = _portfolio_policy_int(normalized_policy, "max_symbol_family_open_positions")
    if max_symbol_family is not None and len(same_symbol_family) >= max_symbol_family:
        return _portfolio_block_payload(
            reason="duplicate_symbol_family_exposure",
            message="Portfolio already has active exposure for this symbol and strategy family.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_strategy = _portfolio_policy_int(normalized_policy, "max_strategy_open_positions")
    if max_strategy is not None and len(same_strategy) >= max_strategy:
        return _portfolio_block_payload(
            reason="portfolio_strategy_cap_reached",
            message="Strategy-level active exposure cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_family = _portfolio_policy_int(normalized_policy, "max_family_open_positions")
    if max_family is not None and len(same_family) >= max_family:
        return _portfolio_block_payload(
            reason="portfolio_family_cap_reached",
            message="Family-level active exposure cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_daily_entries = _portfolio_policy_int(normalized_policy, "max_daily_new_entries")
    if max_daily_entries is not None and len(same_strategy_daily_entries) >= max_daily_entries:
        return _portfolio_block_payload(
            reason="max_daily_new_entries_reached",
            message="Strategy daily new-entry cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_total_strategy_risk = coerce_float(normalized_policy.get("max_total_strategy_risk"))
    if max_total_strategy_risk is not None and strategy_max_loss_after is not None and strategy_max_loss_after > max_total_strategy_risk:
        return _portfolio_block_payload(
            reason="max_total_strategy_risk_exceeded",
            message="Strategy-level max-loss exposure budget would be exceeded.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_correlated = _portfolio_policy_int(normalized_policy, "max_correlated_group_open_positions")
    if max_correlated is not None and len(same_correlation_group) >= max_correlated:
        return _portfolio_block_payload(
            reason="correlated_exposure_limit_reached",
            message="Correlated exposure cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    return {
        "status": "admissible",
        "reason": "portfolio_admissible",
        "message": "Portfolio can add this exposure under the current strategy policy.",
        "admission_boundary": PORTFOLIO_ADMISSION_BOUNDARY,
        "admissible_quantity": 1,
        "reason_codes": ["portfolio_admissible"],
        "blockers": [],
        "policy": normalized_policy,
        "metrics": metrics,
        "evidence": evidence,
        "allocation_plan": allocation_evidence,
        "evaluated_at": evaluated_at,
    }


def live_broker_buying_power_snapshot(execution_store: Any) -> dict[str, Any]:
    open_attempts = _account_open_attempts(execution_store)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    reserved_buying_power = 0.0
    reservation_count = 0
    unsupported_reservation_count = 0
    for attempt in pending_attempts:
        requirement = estimate_buying_power_requirement(
            dict(attempt.get("candidate") or {}),
            coerce_float(attempt.get("pending_quantity")) or 0.0,
            limit_price=coerce_float(attempt.get("limit_price")),
        )
        required_buying_power = coerce_float(requirement.get("required_buying_power"))
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
            "reserved_buying_power": money_float(reserved_buying_power),
            "remaining_buying_power": None,
            "reservation_count": reservation_count,
            "unsupported_reservation_count": unsupported_reservation_count,
            "error_text": str(exc),
        }

    available_snapshot = resolve_available_buying_power(account_payload)
    available_buying_power = coerce_float(available_snapshot.get("available_buying_power"))
    if available_buying_power is None:
        return {
            "status": "unavailable",
            "source_field": as_text(available_snapshot.get("source_field")),
            "available_buying_power": None,
            "reserved_buying_power": money_float(reserved_buying_power),
            "remaining_buying_power": None,
            "reservation_count": reservation_count,
            "unsupported_reservation_count": unsupported_reservation_count,
            "error_text": "Broker account payload did not include usable buying power fields.",
        }

    return {
        "status": "ok",
        "source_field": as_text(available_snapshot.get("source_field")),
        "available_buying_power": money_float(available_buying_power),
        "reserved_buying_power": money_float(reserved_buying_power),
        "remaining_buying_power": money_float(max(available_buying_power - reserved_buying_power, 0.0)),
        "reservation_count": reservation_count,
        "unsupported_reservation_count": unsupported_reservation_count,
        "error_text": None,
    }


def _deferred_execution_readiness_payload() -> dict[str, Any]:
    return {
        "status": "not_evaluated",
        "reason": DEFERRED_EXECUTION_READINESS_REASON,
        "message": "Final quote, broker, and order-submit readiness is evaluated by the execution submit path.",
        "evaluated_by": "execution_submit",
    }


def build_entry_capacity_admission_payload(
    *,
    status: str,
    reason: str | None,
    message: str | None,
    admissible_quantity: int | None,
    required_buying_power: float | None,
    available_buying_power: float | None,
    account_available_buying_power: float | None = None,
    reserved_buying_power: float | None = None,
    buying_power_basis: str | None = None,
    buying_power_source_field: str | None = None,
    broker_buying_power_status: str | None = None,
    limiting_constraint: str | None = None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
    position_size_budget: float | None = None,
    protection_admission: Mapping[str, Any] | None = None,
    portfolio_admission: Mapping[str, Any] | None = None,
    evaluated_at: str | None = None,
) -> dict[str, Any]:
    normalized_status = as_text(status) or "unknown"
    capacity_reason = as_text(reason)
    capacity_status = "admissible" if normalized_status in {"admissible", "approved", "ok", "pass", "passed"} else normalized_status
    if capacity_status == "admissible":
        reason_codes = [capacity_reason or "capacity_admissible"]
        blockers: list[str] = []
        top_level_reason = None if capacity_reason in {None, "capacity_admissible"} else capacity_reason
    else:
        reason_codes = [capacity_reason] if capacity_reason else []
        blockers = list(reason_codes)
        top_level_reason = capacity_reason
    execution_readiness = _deferred_execution_readiness_payload()
    protection_admission_payload = (
        dict(protection_admission)
        if isinstance(protection_admission, Mapping)
        else {
            "status": "not_evaluated",
            "reason": "protection_admission_not_evaluated",
            "message": "Protection admission was not evaluated for this admission payload.",
            "admission_boundary": PROTECTION_ADMISSION_BOUNDARY,
            "reason_codes": [],
            "blockers": [],
        }
    )
    protection_status_raw = as_text(protection_admission_payload.get("status")) or "not_evaluated"
    protection_status = "admissible" if protection_status_raw in PROTECTION_ADMISSIBLE_STATUSES else protection_status_raw
    protection_reason = as_text(protection_admission_payload.get("reason"))
    protection_reason_codes = unique_text_list(protection_admission_payload.get("reason_codes"), accept_scalar=True)
    if protection_status in {"admissible", "blocked", "unknown"} and protection_reason and protection_reason not in protection_reason_codes:
        protection_reason_codes.append(protection_reason)
    protection_blockers = unique_text_list(protection_admission_payload.get("blockers"), accept_scalar=True)
    if protection_status in {"blocked", "unknown"} and protection_reason and protection_reason not in protection_blockers:
        protection_blockers.append(protection_reason)

    portfolio_admission_payload = (
        dict(portfolio_admission)
        if isinstance(portfolio_admission, Mapping)
        else {
            "status": "not_evaluated",
            "reason": "portfolio_admission_not_evaluated",
            "message": "Portfolio admission was not evaluated for this admission payload.",
            "admission_boundary": PORTFOLIO_ADMISSION_BOUNDARY,
            "reason_codes": [],
            "blockers": [],
        }
    )
    portfolio_status_raw = as_text(portfolio_admission_payload.get("status")) or "not_evaluated"
    portfolio_status = "admissible" if portfolio_status_raw in {"admissible", "approved", "ok", "pass", "passed"} else portfolio_status_raw
    portfolio_reason = as_text(portfolio_admission_payload.get("reason"))
    portfolio_reason_codes = unique_text_list(portfolio_admission_payload.get("reason_codes"), accept_scalar=True)
    if portfolio_status in {"admissible", "blocked", "unknown"} and portfolio_reason and portfolio_reason not in portfolio_reason_codes:
        portfolio_reason_codes.append(portfolio_reason)
    portfolio_blockers = unique_text_list(portfolio_admission_payload.get("blockers"), accept_scalar=True)
    if portfolio_status in {"blocked", "unknown"} and portfolio_reason and portfolio_reason not in portfolio_blockers:
        portfolio_blockers.append(portfolio_reason)

    capacity_admission = {
        "status": capacity_status,
        "reason": capacity_reason or ("capacity_admissible" if capacity_status == "admissible" else None),
        "message": message,
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "limiting_constraint": limiting_constraint,
        "reason_codes": reason_codes,
        "blockers": blockers,
    }
    combined_reason_codes = unique_text_list(
        [*reason_codes, *protection_reason_codes, *portfolio_reason_codes],
        accept_scalar=True,
    )
    combined_blockers = unique_text_list(
        [*blockers, *protection_blockers, *portfolio_blockers],
        accept_scalar=True,
    )
    top_level_status = "admissible" if capacity_status == "admissible" else capacity_status
    top_level_message = message
    top_level_boundary = ENTRY_CAPACITY_ADMISSION_BOUNDARY
    if protection_status in {"blocked", "unknown"}:
        top_level_status = protection_status
        top_level_reason = protection_reason
        top_level_message = as_text(protection_admission_payload.get("message")) or top_level_message
        top_level_boundary = PROTECTION_ADMISSION_BOUNDARY
    elif portfolio_status in {"blocked", "unknown"}:
        top_level_status = portfolio_status
        top_level_reason = portfolio_reason
        top_level_message = as_text(portfolio_admission_payload.get("message")) or top_level_message
        top_level_boundary = PORTFOLIO_ADMISSION_BOUNDARY
    elif portfolio_status == "admissible" and top_level_status == "admissible":
        top_level_reason = None

    return {
        "status": top_level_status,
        "reason": top_level_reason,
        "message": top_level_message,
        "admission_boundary": top_level_boundary,
        "capacity_admission_kind": ENTRY_CAPACITY_ADMISSION_BOUNDARY,
        "capacity_admission_status": capacity_status,
        "protection_admission_status": protection_status,
        "protection_admission_reason": protection_reason,
        "portfolio_admission_status": portfolio_status,
        "portfolio_admission_reason": portfolio_reason,
        "execution_readiness_status": execution_readiness["status"],
        "execution_readiness_reason": execution_readiness["reason"],
        "capacity_admission": capacity_admission,
        "protection_admission": protection_admission_payload,
        "portfolio_admission": portfolio_admission_payload,
        "execution_readiness": execution_readiness,
        "reason_codes": combined_reason_codes,
        "blockers": combined_blockers,
        "evaluated_at": evaluated_at or utc_now_iso(),
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "account_available_buying_power": account_available_buying_power,
        "reserved_buying_power": reserved_buying_power,
        "buying_power_basis": buying_power_basis,
        "buying_power_source_field": buying_power_source_field,
        "broker_buying_power_status": broker_buying_power_status,
        "limiting_constraint": limiting_constraint,
        "strategy_risk_budget": strategy_risk_budget,
        "position_size_pct_of_available_balance": position_size_pct_of_available_balance,
        "position_size_budget": position_size_budget,
    }


def build_execution_admission_snapshot(
    *,
    execution_store: Any,
    candidate: dict[str, Any],
    limit_price: float | None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
    protection_admission: Mapping[str, Any] | None = None,
    portfolio_admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    broker_buying_power = live_broker_buying_power_snapshot(execution_store)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    available_buying_power = coerce_float(broker_buying_power.get("remaining_buying_power"))
    sizing = build_candidate_position_sizing(
        candidate=candidate,
        limit_price=limit_price,
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
        available_broker_buying_power=available_buying_power,
    )
    limiting_constraint = as_text(sizing.get("limiting_constraint"))
    admissible_quantity = coerce_int(sizing.get("recommended_quantity"))

    def capacity_payload(
        *,
        status: str,
        reason: str | None,
        message: str | None,
        quantity: int | None,
    ) -> dict[str, Any]:
        return build_entry_capacity_admission_payload(
            status=status,
            reason=reason,
            message=message,
            admissible_quantity=quantity,
            required_buying_power=required_buying_power,
            available_buying_power=available_buying_power,
            account_available_buying_power=coerce_float(broker_buying_power.get("available_buying_power")),
            reserved_buying_power=coerce_float(broker_buying_power.get("reserved_buying_power")),
            buying_power_basis=as_text(buying_power_requirement.get("basis")),
            buying_power_source_field=as_text(broker_buying_power.get("source_field")),
            broker_buying_power_status=as_text(broker_buying_power.get("status")),
            limiting_constraint=limiting_constraint,
            strategy_risk_budget=strategy_risk_budget,
            position_size_pct_of_available_balance=coerce_float(sizing.get("position_size_pct_of_available_balance")),
            position_size_budget=coerce_float(sizing.get("position_size_budget")),
            protection_admission=protection_admission,
            portfolio_admission=portfolio_admission,
        )

    if str(broker_buying_power.get("status") or "") != "ok":
        reason = "broker_buying_power_unavailable"
        return capacity_payload(
            status="unknown",
            reason=reason,
            message=as_text(broker_buying_power.get("error_text")) or "Broker buying power is unavailable.",
            quantity=None,
        )
    if required_buying_power is None:
        reason = "unsupported_buying_power_estimate"
        return capacity_payload(
            status="unknown",
            reason=reason,
            message="Buying power estimate is unavailable for this structure.",
            quantity=None,
        )

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
        if available_buying_power is not None and required_buying_power is not None and reason == "insufficient_broker_buying_power":
            message = (
                "Current account buying power cannot carry one contract "
                f"(requires {required_buying_power:.2f}, "
                f"available {available_buying_power:.2f})."
            )
        return capacity_payload(status="blocked", reason=reason, message=message, quantity=0)

    message = f"Current account can carry up to {resolved_quantity} contract"
    if resolved_quantity != 1:
        message += "s"
    message += " now."
    return capacity_payload(status="admissible", reason=None, message=message, quantity=resolved_quantity)


def _broker_position_side(position: Mapping[str, Any]) -> str | None:
    side = as_text(position.get("side"))
    if side in {"long", "short"}:
        return side
    quantity = coerce_float(position.get("qty"))
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
        and (symbol := as_text(position.get("symbol"))) is not None
        and (side := _broker_position_side(position)) is not None
    }
    conflicts: list[dict[str, str]] = []
    for leg in resolved_legs:
        symbol = as_text(leg.get("symbol"))
        role = as_text(leg.get("role"))
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
                "requested_position_intent": ("sell_to_open" if role == "short" else "buy_to_open"),
            }
        )
    return conflicts


def _session_position_metrics(positions: list[dict[str, Any]]) -> dict[str, float]:
    return {
        "open_position_count": float(len(positions)),
        "open_contract_count": sum(coerce_float(position.get("remaining_quantity")) or 0.0 for position in positions),
        "entry_notional_total": sum(coerce_float(position.get("entry_notional")) or 0.0 for position in positions),
        "max_loss_total": sum(coerce_float(position.get("max_loss")) or 0.0 for position in positions),
    }


def _session_pending_open_attempt_metrics(
    pending_attempts: list[dict[str, Any]],
) -> dict[str, float]:
    return {
        "pending_open_attempt_count": float(len(pending_attempts)),
        "pending_open_position_slot_count": sum(1.0 for attempt in pending_attempts if bool(attempt.get("occupies_position_slot"))),
        "pending_open_contract_count": sum(coerce_float(attempt.get("pending_quantity")) or 0.0 for attempt in pending_attempts),
        "pending_entry_notional_total": sum(coerce_float(attempt.get("pending_entry_notional")) or 0.0 for attempt in pending_attempts),
        "pending_max_loss_total": sum(coerce_float(attempt.get("pending_max_loss")) or 0.0 for attempt in pending_attempts),
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
        "active_open_position_count": (position_metrics["open_position_count"] + pending_metrics["pending_open_position_slot_count"]),
        "active_open_contract_count": (position_metrics["open_contract_count"] + pending_metrics["pending_open_contract_count"]),
        "active_entry_notional_total": (position_metrics["entry_notional_total"] + pending_metrics["pending_entry_notional_total"]),
        "active_max_loss_total": (position_metrics["max_loss_total"] + pending_metrics["pending_max_loss_total"]),
    }


def _kill_switch_reason() -> str | None:
    if coerce_bool(os.environ.get("SPREADS_EXECUTION_KILL_SWITCH"), default=False):
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
        allow_live_env=coerce_bool(os.environ.get("SPREADS_ALLOW_LIVE_TRADING"), default=False),
    )


def _candidate_timestamp(candidate: dict[str, Any], cycle: dict[str, Any]) -> datetime | None:
    return parse_datetime(as_text(candidate.get("generated_at")) or as_text(cycle.get("generated_at")))


def assess_position_risk(
    *,
    position: dict[str, Any],
    risk_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy or position.get("risk_policy"))
    remaining_quantity = coerce_float(position.get("remaining_quantity")) or 0.0
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
    max_contracts_per_position = coerce_int(normalized_policy.get("max_contracts_per_position"))
    if max_contracts_per_position is not None and remaining_quantity > max_contracts_per_position:
        reasons.append("remaining quantity exceeds max_contracts_per_position")

    entry_notional = coerce_float(position.get("entry_notional"))
    max_position_notional = coerce_float(normalized_policy.get("max_position_notional"))
    if entry_notional is not None and max_position_notional is not None and entry_notional > max_position_notional:
        reasons.append("entry notional exceeds max_position_notional")

    max_loss = coerce_float(position.get("max_loss"))
    max_position_max_loss = coerce_float(normalized_policy.get("max_position_max_loss"))
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
    execution_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)

    if hasattr(execution_store, "portfolio_schema_ready") and not execution_store.portfolio_schema_ready():
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

    if metrics["active_open_position_count"] >= float(normalized_policy["max_open_positions_per_session"]):
        reasons.append("max_open_positions_per_session reached")
    if metrics["active_open_contract_count"] >= float(normalized_policy["max_contracts_per_session"]):
        reasons.append("max_contracts_per_session reached")

    max_session_notional = coerce_float(normalized_policy.get("max_session_notional"))
    if max_session_notional is not None and metrics["active_entry_notional_total"] >= max_session_notional:
        reasons.append("max_session_notional reached")

    max_session_max_loss = coerce_float(normalized_policy.get("max_session_max_loss"))
    if max_session_max_loss is not None and metrics["active_max_loss_total"] >= max_session_max_loss:
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
        "note": "Strategy run can submit new executions under the current risk policy.",
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
        candidate_age_seconds = round((datetime.now(UTC) - candidate_timestamp).total_seconds(), 3)
    underlying_symbol = str(candidate["underlying_symbol"])
    strategy = str(candidate["strategy"])
    matching_underlyings = [position for position in open_positions if str(position.get("underlying_symbol")) == underlying_symbol]
    matching_pending_underlyings = [
        attempt
        for attempt in pending_attempts
        if bool(attempt.get("occupies_position_slot")) and str(attempt.get("underlying_symbol")) == underlying_symbol
    ]
    matching_strategy = [position for position in matching_underlyings if str(position.get("strategy")) == strategy]
    matching_pending_strategy = [attempt for attempt in matching_pending_underlyings if str(attempt.get("strategy")) == strategy]
    session_notional = session_metrics["active_entry_notional_total"]
    session_max_loss = session_metrics["active_max_loss_total"]
    open_contracts = session_metrics["active_open_contract_count"]
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        quantity,
        limit_price=limit_price,
    )
    required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    metrics = {
        **session_metrics,
        "requested_quantity": int(quantity),
        "requested_limit_price": limit_price,
        "candidate_age_seconds": candidate_age_seconds,
        "position_notional": position_notional,
        "position_max_loss": position_max_loss,
        "session_notional_before": money_float(session_notional),
        "session_notional_after": (None if position_notional is None else money_sum_float([session_notional, position_notional])),
        "session_max_loss_before": money_float(session_max_loss),
        "session_max_loss_after": (None if position_max_loss is None else money_sum_float([session_max_loss, position_max_loss])),
        "matching_underlying_count": (len(matching_underlyings) + len(matching_pending_underlyings)),
        "matching_underlying_strategy_count": (len(matching_strategy) + len(matching_pending_strategy)),
        "strategy_risk_budget": strategy_risk_budget,
        "position_size_pct_of_available_balance": position_size_pct_of_available_balance,
        "required_buying_power": required_buying_power,
        "buying_power_basis": as_text(buying_power_requirement.get("basis")),
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
    metrics["available_broker_buying_power"] = coerce_float(sizing.get("available_broker_buying_power"))
    metrics["broker_buying_power_status"] = as_text(sizing.get("broker_buying_power_status"))
    metrics["broker_reserved_buying_power"] = coerce_float(sizing.get("broker_reserved_buying_power"))
    metrics["broker_buying_power_source_field"] = as_text(sizing.get("broker_buying_power_source_field"))

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
    metrics["broker_position_conflict_symbols"] = [conflict["symbol"] for conflict in broker_position_conflicts]
    if broker_position_conflicts:
        conflict_summary = ", ".join(
            (f"{conflict['symbol']} " f"(broker {conflict['broker_side']}, request {conflict['requested_position_intent']})")
            for conflict in broker_position_conflicts[:4]
        )
        if len(broker_position_conflicts) > 4:
            conflict_summary += ", …"
        return {
            "status": "blocked",
            "note": ("Open execution conflicts with existing broker-held option legs: " f"{conflict_summary}."),
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
    if max_contracts_per_position is not None and quantity > max_contracts_per_position:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_position.",
            "reason_codes": ["max_contracts_per_position_exceeded"],
            "blockers": ["max_contracts_per_position_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    stale_quote_after_seconds = coerce_float(normalized_policy.get("stale_quote_after_seconds"))
    if candidate_age_seconds is not None and stale_quote_after_seconds is not None and candidate_age_seconds > stale_quote_after_seconds:
        return {
            "status": "blocked",
            "note": "Open execution is blocked because the quote snapshot is stale.",
            "reason_codes": ["stale_quote_snapshot"],
            "blockers": ["stale_quote_snapshot"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if session_metrics["active_open_position_count"] >= int(normalized_policy["max_open_positions_per_session"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_session.",
            "reason_codes": ["max_open_positions_per_session_exceeded"],
            "blockers": ["max_open_positions_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_underlyings) + len(matching_pending_underlyings) >= int(normalized_policy["max_open_positions_per_underlying"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying.",
            "reason_codes": ["max_open_positions_per_underlying_exceeded"],
            "blockers": ["max_open_positions_per_underlying_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_strategy) + len(matching_pending_strategy) >= int(normalized_policy["max_open_positions_per_underlying_strategy"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying_strategy.",
            "reason_codes": ["max_open_positions_per_underlying_strategy_exceeded"],
            "blockers": ["max_open_positions_per_underlying_strategy_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if open_contracts + quantity > float(normalized_policy["max_contracts_per_session"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_session.",
            "reason_codes": ["max_contracts_per_session_exceeded"],
            "blockers": ["max_contracts_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_position_notional = coerce_float(normalized_policy.get("max_position_notional"))
    if position_notional is not None and max_position_notional is not None and position_notional > max_position_notional:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_position_notional.",
            "reason_codes": ["max_position_notional_exceeded"],
            "blockers": ["max_position_notional_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_session_notional = coerce_float(normalized_policy.get("max_session_notional"))
    if position_notional is not None and max_session_notional is not None and session_notional + position_notional > max_session_notional:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_session_notional.",
            "reason_codes": ["max_session_notional_exceeded"],
            "blockers": ["max_session_notional_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_position_max_loss = coerce_float(normalized_policy.get("max_position_max_loss"))
    if position_max_loss is not None and max_position_max_loss is not None and position_max_loss > max_position_max_loss:
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

    recommended_quantity = coerce_int(sizing.get("recommended_quantity"))
    limiting_constraint = as_text(sizing.get("limiting_constraint"))
    if recommended_quantity is not None and recommended_quantity >= 0 and quantity > recommended_quantity:
        if limiting_constraint == "position_size_pct_of_available_balance":
            return {
                "status": "blocked",
                "note": ("Open execution exceeds the configured position-size budget " "derived from available broker buying power."),
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

    available_broker_buying_power = coerce_float(sizing.get("available_broker_buying_power"))
    if required_buying_power is not None and available_broker_buying_power is not None and required_buying_power > available_broker_buying_power:
        source_field = as_text(sizing.get("broker_buying_power_source_field"))
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

    max_session_max_loss = coerce_float(normalized_policy.get("max_session_max_loss"))
    if position_max_loss is not None and max_session_max_loss is not None and session_max_loss + position_max_loss > max_session_max_loss:
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
    position_status = str(position.get("position_status") or position.get("status") or "").lower()
    if position_status and position_status not in OPEN_POSITION_STATUSES:
        raise ValueError("Position is already closed.")
    remaining_quantity = coerce_float(position.get("remaining_quantity"))
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
        reconciliation_status = as_text(position.get("reconciliation_status"))
        if reconciliation_status != "matched":
            raise ValueError("Position broker reconciliation is not matched; " "wait for broker sync before closing.")
        last_reconciled_at = parse_datetime(as_text(position.get("last_reconciled_at")))
        if last_reconciled_at is None:
            raise ValueError("Position broker reconciliation is missing; " "wait for broker sync before closing.")
        reconciliation_age = ((now or datetime.now(UTC)) - last_reconciled_at.astimezone(UTC)).total_seconds()
        if reconciliation_age > max_reconciliation_age_seconds:
            raise ValueError("Position broker reconciliation is stale; " "wait for broker sync before closing.")
    return {
        "status": "ok",
    }

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


from core.money import money_float
from core.services.risk.buying_power import (
    estimate_buying_power_requirement,
    resolve_available_buying_power,
)
from core.services.alpaca import (
    create_alpaca_client_from_env,
)
from core.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUS_LIST,
    is_open_execution_attempt_status,
    resolve_execution_attempt_filled_quantity,
    resolve_execution_attempt_requested_quantity,
)
from core.services.positions import enrich_position_row
from core.services.runtime_identity import parse_live_run_scope_id
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_float,
)

from core.services.risk.candidates import (
    _candidate_entry_notional,
    _candidate_max_loss,
    _candidate_strategy_family,
    _date_text,
    _portfolio_correlation_group,
    _root_symbol,
)
from core.services.risk.policy import (
    ACCOUNT_CAPACITY_REQUEST_TIMEOUT_SECONDS,
    ACTIVE_PORTFOLIO_INTENT_STATES,
    OPEN_POSITION_STATUSES,
    TERMINAL_ENTRY_ATTEMPT_STATUSES,
)

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

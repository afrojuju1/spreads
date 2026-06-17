from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from core.services.execution_lifecycle import (
    resolve_execution_attempt_filled_quantity,
    resolve_execution_attempt_primary_order,
)
from core.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    order_payload_legs,
    position_legs as canonical_position_legs,
    structure_width,
)
from core.services.runtime_identity import (
    resolve_runtime_policy_fields,
)
from core.money import (
    close_pnl,
    equity_notional,
    money_float,
    money_scaled_float,
    money_sum_float,
    option_long_exposure,
    option_spread_exposure,
    premium_float,
)
from core.value_coercion import (
    as_text,
    coerce_float,
    coerce_int,
    utc_now_iso,
)

OPEN_TRADE_INTENT = "open"
CLOSE_TRADE_INTENT = "close"
SUPPORTED_TRADE_INTENTS = {OPEN_TRADE_INTENT, CLOSE_TRADE_INTENT}


def derive_live_exposure(
    *,
    entry_value: float | None,
    width: float | None,
    quantity: float,
    strategy_family: str | None,
) -> dict[str, float | None]:
    normalized_quantity = max(float(quantity), 0.0)
    if normalized_quantity <= 0:
        return {
            "entry_notional": 0.0,
            "max_profit": 0.0,
            "max_loss": 0.0,
        }

    normalized_family = str(strategy_family or "").strip().lower()
    if normalized_family.startswith("equity_"):
        entry_notional = equity_notional(entry_value, normalized_quantity)
        return {
            "entry_notional": entry_notional,
            "max_profit": None,
            "max_loss": entry_notional,
        }
    if normalized_family in {"long_call", "long_put", "long_straddle", "long_strangle"}:
        return option_long_exposure(entry_value=entry_value, quantity=normalized_quantity)
    premium_kind = net_premium_kind(normalized_family)
    return option_spread_exposure(entry_value=entry_value, width=width, quantity=normalized_quantity, premium_kind=premium_kind)


def explicit_candidate_exposure(
    *,
    candidate: Mapping[str, Any],
    quantity: float,
) -> dict[str, float | None]:
    normalized_quantity = max(float(quantity), 0.0)
    if normalized_quantity <= 0:
        return {"max_profit": 0.0, "max_loss": 0.0}
    max_profit = coerce_float(candidate.get("max_profit"))
    max_loss = coerce_float(candidate.get("max_loss"))
    return {
        "max_profit": money_scaled_float(max_profit, normalized_quantity) if max_profit is not None else None,
        "max_loss": money_scaled_float(max_loss, normalized_quantity) if max_loss is not None else None,
    }


def resolve_trade_intent(value: Any) -> str:
    normalized = (as_text(value) or OPEN_TRADE_INTENT).lower()
    if normalized not in SUPPORTED_TRADE_INTENTS:
        raise ValueError(f"Unsupported trade intent: {normalized}")
    return normalized


def resolve_attempt_trade_intent(attempt: Mapping[str, Any]) -> str:
    request = attempt.get("request")
    if isinstance(request, Mapping):
        return resolve_trade_intent(request.get("trade_intent"))
    return resolve_trade_intent(attempt.get("trade_intent"))


def resolve_attempt_position_id(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    request_value = request.get("position_id") if isinstance(request, Mapping) else None
    return as_text(attempt.get("position_id")) or as_text(request_value)


def _attempt_request(attempt: Mapping[str, Any]) -> Mapping[str, Any]:
    request = attempt.get("request")
    return request if isinstance(request, Mapping) else {}


def _attempt_exit_policy(attempt: Mapping[str, Any]) -> dict[str, Any]:
    request = _attempt_request(attempt)
    policy = request.get("exit_policy")
    return dict(policy) if isinstance(policy, Mapping) else {}


def _attempt_risk_policy(attempt: Mapping[str, Any]) -> dict[str, Any]:
    request = _attempt_request(attempt)
    policy = request.get("risk_policy")
    return dict(policy) if isinstance(policy, Mapping) else {}


def _attempt_source_job(attempt: Mapping[str, Any]) -> dict[str, Any]:
    request = _attempt_request(attempt)
    payload = request.get("source_job")
    return dict(payload) if isinstance(payload, Mapping) else {}


def _attempt_execution_intent_id(attempt: Mapping[str, Any]) -> str | None:
    request = _attempt_request(attempt)
    return as_text(request.get("execution_intent_id"))


def _resolve_primary_order(attempt: Mapping[str, Any]) -> Mapping[str, Any] | None:
    return resolve_execution_attempt_primary_order(attempt)


def _resolve_filled_quantity(attempt: Mapping[str, Any], primary_order: Mapping[str, Any] | None) -> float:
    return resolve_execution_attempt_filled_quantity(
        attempt,
        primary_order=primary_order,
    )


def _weighted_average(pairs: list[tuple[float, float]]) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for price, quantity in pairs:
        if quantity <= 0:
            continue
        numerator += price * quantity
        denominator += quantity
    if denominator <= 0:
        return None
    return numerator / denominator


def _resolve_leg_average_price(attempt: Mapping[str, Any], symbol: str) -> float | None:
    fill_pairs: list[tuple[float, float]] = []
    for fill in attempt.get("fills") or []:
        if not isinstance(fill, Mapping):
            continue
        if as_text(fill.get("symbol")) != symbol:
            continue
        price = coerce_float(fill.get("price"))
        quantity = coerce_float(fill.get("quantity"))
        if price is None or quantity is None:
            continue
        fill_pairs.append((price, quantity))
    weighted_fill = _weighted_average(fill_pairs)
    if weighted_fill is not None:
        return weighted_fill

    order_pairs: list[tuple[float, float]] = []
    for order in attempt.get("orders") or []:
        if not isinstance(order, Mapping):
            continue
        order_symbol = as_text(order.get("leg_symbol")) or as_text(order.get("symbol"))
        if order_symbol != symbol:
            continue
        price = coerce_float(order.get("filled_avg_price"))
        quantity = coerce_float(order.get("filled_qty"))
        if price is None or quantity is None:
            continue
        order_pairs.append((price, quantity))
    return _weighted_average(order_pairs)


def _resolve_spread_amount(
    attempt: Mapping[str, Any],
    primary_order: Mapping[str, Any] | None,
    filled_quantity: float,
) -> float | None:
    request = _attempt_request(attempt)
    order = request.get("order") if isinstance(request.get("order"), Mapping) else {}
    legs = order_payload_legs(
        order,
        expiration_date=as_text(attempt.get("expiration_date")),
    )
    if not legs:
        candidate = attempt.get("candidate")
        if isinstance(candidate, Mapping):
            legs = candidate_legs(candidate)
    if not legs:
        legs = canonical_position_legs(attempt)
    short_total = 0.0
    long_total = 0.0
    resolved_leg_count = 0
    for leg in legs:
        symbol = as_text(leg.get("symbol"))
        role = as_text(leg.get("role"))
        ratio_qty = coerce_float(leg.get("ratio_qty")) or 1.0
        if symbol is None or role not in {"short", "long"}:
            continue
        leg_price = _resolve_leg_average_price(attempt, symbol)
        if leg_price is None:
            resolved_leg_count = 0
            break
        if role == "short":
            short_total += leg_price * ratio_qty
        else:
            long_total += leg_price * ratio_qty
        resolved_leg_count += 1
    if resolved_leg_count == len(legs) and resolved_leg_count > 0:
        return premium_float(abs(short_total - long_total))

    # Alpaca returns parent multi-leg fills as a signed net price:
    # credit opens are negative, debit closes are positive. Session
    # positions persist canonical economics instead: positive entry
    # credit and positive exit debit.
    if primary_order is not None:
        price = coerce_float(primary_order.get("filled_avg_price"))
        if price is not None and filled_quantity > 0:
            return premium_float(abs(price))

    limit_price = coerce_float(attempt.get("limit_price"))
    if limit_price is not None and filled_quantity > 0:
        return abs(limit_price)
    return None


def _resolve_position_status(broker_status: str, filled_quantity: float) -> str:
    if filled_quantity <= 0:
        return "pending_open"
    if broker_status == "partially_filled":
        return "partial_open"
    return "open"


def _resolve_opened_at(attempt: Mapping[str, Any]) -> str | None:
    fill_times = [as_text(fill.get("filled_at")) for fill in attempt.get("fills") or [] if isinstance(fill, Mapping)]
    filtered = [value for value in fill_times if value]
    if filtered:
        return min(filtered)
    return as_text(attempt.get("submitted_at")) or as_text(attempt.get("requested_at"))


def _close_state_for_open_sync(
    *,
    execution_store: Any,
    position_id: str,
    opened_quantity: float,
    broker_status: str,
) -> dict[str, Any]:
    closes = execution_store.list_position_closes(position_id=position_id)
    total_closed_quantity = sum(coerce_float(close.get("closed_quantity")) or 0.0 for close in closes)
    remaining_quantity = max(opened_quantity - total_closed_quantity, 0.0)
    realized_pnl = money_sum_float(coerce_float(close.get("realized_pnl")) for close in closes)

    if total_closed_quantity <= 0:
        return {
            "status": _resolve_position_status(broker_status, opened_quantity),
            "remaining_quantity": opened_quantity,
            "realized_pnl": 0.0,
            "unrealized_pnl": None,
            "closed_at": None,
        }
    if remaining_quantity <= 0:
        closed_times = [as_text(close.get("closed_at")) for close in closes]
        return {
            "status": "closed",
            "remaining_quantity": 0.0,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": 0.0,
            "closed_at": max((value for value in closed_times if value), default=None),
        }
    return {
        "status": "partial_close",
        "remaining_quantity": remaining_quantity,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": None,
        "closed_at": None,
    }


def _sync_linked_execution_intent_position(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    position_id: str,
) -> None:
    from core.services.execution_intents.shared import link_execution_intent_position

    if not execution_store.intent_schema_ready():
        return
    execution_intent_id = _attempt_execution_intent_id(attempt)
    if execution_intent_id is None:
        return
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        return
    link_execution_intent_position(
        execution_store,
        intent=dict(intent),
        position_id=position_id,
        execution_attempt_id=as_text(attempt.get("execution_attempt_id")),
        updated_at=utc_now_iso(),
    )


def _resolve_closed_at(attempt: Mapping[str, Any]) -> str | None:
    fill_times = [as_text(fill.get("filled_at")) for fill in attempt.get("fills") or [] if isinstance(fill, Mapping)]
    filtered = [value for value in fill_times if value]
    if filtered:
        return max(filtered)
    return as_text(attempt.get("completed_at")) or as_text(attempt.get("submitted_at"))


def _resolve_width(attempt: Mapping[str, Any]) -> float | None:
    candidate = attempt.get("candidate")
    if not isinstance(candidate, Mapping):
        return None
    width = coerce_float(candidate.get("width"))
    if width is not None:
        return width
    return structure_width(
        candidate_legs(candidate),
        strategy=candidate.get("strategy_family") or candidate.get("strategy"),
    )


def _position_legs(position: Mapping[str, Any]) -> list[dict[str, Any]]:
    return canonical_position_legs(position)


def _position_economics(position: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "entry_credit": coerce_float(position.get("entry_credit")),
        "entry_value": coerce_float(position.get("entry_value")) or coerce_float(position.get("entry_credit")),
        "entry_notional": coerce_float(position.get("entry_notional")),
        "max_profit": coerce_float(position.get("max_profit")),
        "max_loss": coerce_float(position.get("max_loss")),
    }


def resolve_position_exposure(
    *,
    candidate: Mapping[str, Any],
    existing: Mapping[str, Any] | None,
    entry_value: float | None,
    width: float | None,
    quantity: float,
    strategy_family: str | None,
) -> dict[str, float | None]:
    exposure = derive_live_exposure(
        entry_value=entry_value,
        width=width,
        quantity=quantity,
        strategy_family=strategy_family,
    )
    explicit_candidate = explicit_candidate_exposure(
        candidate=candidate,
        quantity=quantity,
    )
    existing_economics = _position_economics(existing or {})
    uncapped_upside = str(strategy_family or "").strip().lower() in {
        "long_call",
        "long_put",
        "long_straddle",
        "long_strangle",
    }
    if uncapped_upside:
        exposure["max_profit"] = None
    elif explicit_candidate.get("max_profit") is not None:
        exposure["max_profit"] = explicit_candidate["max_profit"]
    elif existing_economics.get("max_profit") is not None:
        exposure["max_profit"] = existing_economics.get("max_profit")

    if explicit_candidate.get("max_loss") is not None:
        exposure["max_loss"] = explicit_candidate["max_loss"]
    elif existing_economics.get("max_loss") is not None:
        exposure["max_loss"] = existing_economics.get("max_loss")
    return exposure


def _position_strategy_metrics(position: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "width": coerce_float(position.get("width")),
        "strategy": as_text(position.get("strategy")),
    }


def _new_position_id() -> str:
    return f"position:{uuid4().hex}"


def _position_width(position: Mapping[str, Any]) -> float | None:
    strategy_metrics = position.get("strategy_metrics") if isinstance(position.get("strategy_metrics"), Mapping) else {}
    return coerce_float(strategy_metrics.get("width")) or coerce_float(position.get("width"))


def _position_entry_value(position: Mapping[str, Any]) -> float | None:
    economics = position.get("economics") if isinstance(position.get("economics"), Mapping) else {}
    return coerce_float(position.get("entry_value")) or coerce_float(economics.get("entry_value")) or coerce_float(economics.get("entry_credit"))


def _realized_close_pnl(
    *,
    entry_value: float | None,
    exit_value: float | None,
    quantity: float,
    strategy_family: Any,
) -> float:
    if entry_value is None or exit_value is None or quantity <= 0:
        return 0.0
    if str(strategy_family or "").strip().lower().startswith("equity_"):
        return close_pnl(entry_value=entry_value, exit_value=exit_value, quantity=quantity, premium_kind=strategy_family, equity=True)
    premium_kind = net_premium_kind(strategy_family)
    return close_pnl(entry_value=entry_value, exit_value=exit_value, quantity=quantity, premium_kind=premium_kind)


def _position_common_payload(
    *,
    attempt: Mapping[str, Any],
    existing: Mapping[str, Any] | None,
    requested_quantity: int,
    opened_quantity: float,
    remaining_quantity: float,
    entry_credit: float | None,
    realized_pnl: float,
    unrealized_pnl: float | None,
    close_mark: float | None,
    close_mark_source: str | None,
    close_marked_at: str | None,
    last_broker_status: str | None,
    status: str,
    opened_at: str | None,
    closed_at: str | None,
    last_exit_evaluated_at: str | None,
    last_exit_reason: str | None,
    last_reconciled_at: str | None,
    reconciliation_status: str | None,
    reconciliation_note: str | None,
) -> dict[str, Any]:
    candidate = attempt.get("candidate") if isinstance(attempt.get("candidate"), Mapping) else {}
    existing_payload = existing if isinstance(existing, Mapping) else {}
    root_symbol = str(attempt.get("underlying_symbol") or existing_payload.get("root_symbol") or "")
    market_date = as_text(attempt.get("market_date")) or as_text(attempt.get("session_date")) or as_text(existing_payload.get("market_date_opened"))
    policy_fields = resolve_runtime_policy_fields(
        profile=candidate.get("profile") or as_text(attempt.get("style_profile")) or as_text(existing_payload.get("style_profile")),
        root_symbol=root_symbol,
    )
    width = _resolve_width(attempt) or _position_width(existing or {})
    strategy_family = as_text(attempt.get("strategy_family")) or as_text(existing.get("strategy_family") if isinstance(existing, Mapping) else None)
    exposure = resolve_position_exposure(
        candidate=candidate,
        existing=existing,
        entry_value=entry_credit,
        width=width,
        quantity=remaining_quantity,
        strategy_family=strategy_family,
    )
    exit_policy = (
        dict(existing.get("exit_policy") or {}) if isinstance(existing, Mapping) and isinstance(existing.get("exit_policy"), Mapping) else {}
    )
    exit_policy.update(_attempt_exit_policy(attempt))
    risk_policy = (
        dict(existing.get("risk_policy") or {}) if isinstance(existing, Mapping) and isinstance(existing.get("risk_policy"), Mapping) else {}
    )
    risk_policy.update(_attempt_risk_policy(attempt))
    request = _attempt_request(attempt)
    order = request.get("order") if isinstance(request.get("order"), Mapping) else {}
    attempt_legs = order_payload_legs(
        order,
        expiration_date=as_text(attempt.get("expiration_date")),
    )
    if not attempt_legs and candidate:
        attempt_legs = candidate_legs(candidate)
    existing_legs = canonical_position_legs(existing or {})
    persisted_legs = attempt_legs or existing_legs
    source_job = _attempt_source_job(attempt)
    trading_strategy_id = as_text(request.get("trading_strategy_id")) or as_text(
        existing.get("trading_strategy_id") if isinstance(existing, Mapping) else None
    )
    config_hash = as_text(request.get("config_hash")) or as_text(existing.get("config_hash") if isinstance(existing, Mapping) else None)
    opening_execution_intent_id = _attempt_execution_intent_id(attempt) or as_text(
        existing.get("opening_execution_intent_id") if isinstance(existing, Mapping) else None
    )
    return {
        "trading_strategy_id": trading_strategy_id,
        "source_object_type": as_text(attempt.get("source_object_type"))
        or as_text(existing.get("source_object_type") if isinstance(existing, Mapping) else None),
        "source_object_id": as_text(attempt.get("source_object_id"))
        or as_text(existing.get("source_object_id") if isinstance(existing, Mapping) else None),
        "trade_signal_id": as_text(attempt.get("trade_signal_id"))
        or as_text(existing.get("trade_signal_id") if isinstance(existing, Mapping) else None),
        "trade_decision_id": as_text(attempt.get("trade_decision_id"))
        or as_text(existing.get("trade_decision_id") if isinstance(existing, Mapping) else None),
        "admission_decision_id": as_text(attempt.get("admission_decision_id"))
        or as_text(existing.get("admission_decision_id") if isinstance(existing, Mapping) else None),
        "opening_execution_intent_id": opening_execution_intent_id,
        "root_symbol": root_symbol,
        "strategy_family": strategy_family or str(attempt.get("strategy") or "unknown"),
        "style_profile": as_text(attempt.get("style_profile"))
        or as_text(existing.get("style_profile") if isinstance(existing, Mapping) else None)
        or str(policy_fields["style_profile"]),
        "horizon_intent": as_text(attempt.get("horizon_intent"))
        or as_text(existing.get("horizon_intent") if isinstance(existing, Mapping) else None)
        or str(policy_fields["horizon_intent"]),
        "product_class": as_text(attempt.get("product_class"))
        or as_text(existing.get("product_class") if isinstance(existing, Mapping) else None)
        or str(policy_fields["product_class"]),
        "market_date_opened": market_date,
        "market_date_closed": None if closed_at is None or market_date is None else market_date,
        "status": status,
        "legs": persisted_legs,
        "economics": {
            "entry_credit": money_float(entry_credit),
            "entry_value": money_float(entry_credit),
            "entry_value_kind": net_premium_kind(strategy_family),
            "entry_notional": exposure["entry_notional"],
            "max_profit": exposure["max_profit"],
            "max_loss": exposure["max_loss"],
        },
        "strategy_metrics": {
            "width": width,
            "strategy": as_text(attempt.get("strategy")) or as_text(existing.get("strategy_family") if isinstance(existing, Mapping) else None),
        },
        "requested_quantity": requested_quantity,
        "opened_quantity": opened_quantity,
        "remaining_quantity": remaining_quantity,
        "entry_value": money_float(entry_credit),
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "close_mark": close_mark,
        "close_mark_source": close_mark_source,
        "close_marked_at": close_marked_at,
        "last_broker_status": last_broker_status,
        "exit_policy": exit_policy,
        "risk_policy": risk_policy,
        "config_hash": config_hash,
        "source_job_type": as_text(source_job.get("job_type")) or as_text(existing.get("source_job_type") if isinstance(existing, Mapping) else None),
        "source_job_key": as_text(source_job.get("job_key")) or as_text(existing.get("source_job_key") if isinstance(existing, Mapping) else None),
        "source_job_run_id": as_text(source_job.get("job_run_id"))
        or as_text(existing.get("source_job_run_id") if isinstance(existing, Mapping) else None),
        "last_exit_evaluated_at": last_exit_evaluated_at,
        "last_exit_reason": last_exit_reason,
        "last_reconciled_at": last_reconciled_at,
        "reconciliation_status": reconciliation_status,
        "reconciliation_note": reconciliation_note,
        "opened_at": opened_at,
        "closed_at": closed_at,
        "updated_at": utc_now_iso(),
    }


def _recalculate_position(
    *,
    execution_store: Any,
    position_id: str,
    last_broker_status: str | None,
) -> dict[str, Any]:
    position = execution_store.get_position(position_id)
    if position is None:
        raise ValueError(f"Unknown position_id: {position_id}")
    closes = execution_store.list_position_closes(position_id=position_id)
    opened_quantity = coerce_float(position.get("opened_quantity")) or 0.0
    total_closed_quantity = sum(coerce_float(close.get("closed_quantity")) or 0.0 for close in closes)
    remaining_quantity = max(opened_quantity - total_closed_quantity, 0.0)
    realized_pnl = money_sum_float(coerce_float(close.get("realized_pnl")) for close in closes)
    entry_credit = _position_entry_value(position)
    width = _position_width(position)
    strategy_family = as_text(position.get("strategy_family")) or as_text(position.get("strategy"))
    exposure = derive_live_exposure(
        entry_value=entry_credit,
        width=width,
        quantity=remaining_quantity,
        strategy_family=strategy_family,
    )

    if total_closed_quantity <= 0:
        status = "open"
        closed_at = None
        unrealized_pnl = None
        market_date_closed = None
    elif remaining_quantity <= 0:
        status = "closed"
        closed_times = [as_text(close.get("closed_at")) for close in closes]
        closed_at = max((value for value in closed_times if value), default=None)
        unrealized_pnl = 0.0
        market_date_closed = as_text(position.get("market_date_opened"))
    else:
        status = "partial_close"
        closed_at = None
        unrealized_pnl = None
        market_date_closed = None

    return execution_store.update_position(
        position_id=position_id,
        status=status,
        market_date_closed=market_date_closed,
        remaining_quantity=remaining_quantity,
        economics={
            "entry_credit": money_float(entry_credit),
            "entry_value": money_float(entry_credit),
            "entry_value_kind": net_premium_kind(strategy_family),
            "entry_notional": exposure["entry_notional"],
            "max_profit": exposure["max_profit"],
            "max_loss": exposure["max_loss"],
        },
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        closed_at=closed_at,
        last_broker_status=last_broker_status,
        updated_at=utc_now_iso(),
    )


def _sync_open_position(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
) -> dict[str, Any] | None:
    primary_order = _resolve_primary_order(attempt)
    filled_quantity = _resolve_filled_quantity(attempt, primary_order)
    if filled_quantity <= 0:
        return None

    requested_quantity = coerce_int(attempt.get("quantity")) or max(int(round(filled_quantity)), 1)
    entry_credit = _resolve_spread_amount(attempt, primary_order, filled_quantity)
    broker_status = (as_text(attempt.get("status")) or "unknown").lower()
    position_id = resolve_attempt_position_id(attempt)
    existing = None if position_id is None else execution_store.get_position(position_id)
    if existing is None:
        existing = execution_store.get_position_by_open_attempt(str(attempt["execution_attempt_id"]))
    if existing is None:
        position_id = position_id or _new_position_id()
        created = execution_store.create_position(
            position_id=position_id,
            open_execution_attempt_id=str(attempt["execution_attempt_id"]),
            created_at=utc_now_iso(),
            **_position_common_payload(
                attempt=attempt,
                existing=None,
                requested_quantity=requested_quantity,
                opened_quantity=filled_quantity,
                remaining_quantity=filled_quantity,
                entry_credit=entry_credit,
                realized_pnl=0.0,
                unrealized_pnl=None,
                close_mark=None,
                close_mark_source=None,
                close_marked_at=None,
                last_broker_status=broker_status,
                status=_resolve_position_status(broker_status, filled_quantity),
                opened_at=_resolve_opened_at(attempt),
                closed_at=None,
                last_exit_evaluated_at=None,
                last_exit_reason=None,
                last_reconciled_at=None,
                reconciliation_status=None,
                reconciliation_note=None,
            ),
        )
        existing = created
    else:
        position_id = str(existing["position_id"])
        close_state = _close_state_for_open_sync(
            execution_store=execution_store,
            position_id=position_id,
            opened_quantity=filled_quantity,
            broker_status=broker_status,
        )
        position_status = str(close_state["status"])
        unrealized_pnl = 0.0 if position_status == "closed" else coerce_float(existing.get("unrealized_pnl"))
        existing = execution_store.update_position(
            position_id=position_id,
            **_position_common_payload(
                attempt=attempt,
                existing=existing,
                requested_quantity=requested_quantity,
                opened_quantity=filled_quantity,
                remaining_quantity=float(close_state["remaining_quantity"]),
                entry_credit=entry_credit,
                realized_pnl=float(close_state["realized_pnl"]),
                unrealized_pnl=unrealized_pnl,
                close_mark=coerce_float(existing.get("close_mark")),
                close_mark_source=as_text(existing.get("close_mark_source")),
                close_marked_at=as_text(existing.get("close_marked_at")),
                last_broker_status=broker_status,
                status=position_status,
                opened_at=_resolve_opened_at(attempt),
                closed_at=as_text(close_state.get("closed_at")),
                last_exit_evaluated_at=as_text(existing.get("last_exit_evaluated_at")),
                last_exit_reason=as_text(existing.get("last_exit_reason")),
                last_reconciled_at=as_text(existing.get("last_reconciled_at")),
                reconciliation_status=as_text(existing.get("reconciliation_status")),
                reconciliation_note=as_text(existing.get("reconciliation_note")),
            ),
        )

    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        position_id=str(existing["position_id"]),
    )
    _sync_linked_execution_intent_position(
        execution_store=execution_store,
        attempt=attempt,
        position_id=str(existing["position_id"]),
    )
    return existing


def _sync_close_position(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
) -> dict[str, Any]:
    position_id = resolve_attempt_position_id(attempt) or as_text(attempt.get("position_id"))
    if position_id is None:
        raise ValueError("Close execution attempt is missing a canonical position_id")
    position = execution_store.get_position(position_id)
    if position is None:
        raise ValueError(f"Unknown position_id: {position_id}")

    broker_status = (as_text(attempt.get("status")) or "unknown").lower()
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        position_id=position_id,
    )
    _sync_linked_execution_intent_position(
        execution_store=execution_store,
        attempt=attempt,
        position_id=position_id,
    )
    execution_store.update_position(
        position_id=position_id,
        last_broker_status=broker_status,
        updated_at=utc_now_iso(),
    )

    primary_order = _resolve_primary_order(attempt)
    filled_quantity = _resolve_filled_quantity(attempt, primary_order)
    if filled_quantity <= 0:
        return _recalculate_position(
            execution_store=execution_store,
            position_id=position_id,
            last_broker_status=broker_status,
        )

    exit_value = _resolve_spread_amount(attempt, primary_order, filled_quantity)
    entry_credit = _position_entry_value(position)
    realized_pnl = _realized_close_pnl(
        entry_value=entry_credit,
        exit_value=exit_value,
        quantity=filled_quantity,
        strategy_family=as_text(position.get("strategy_family")) or as_text(position.get("strategy")),
    )

    now = utc_now_iso()
    execution_store.upsert_position_close(
        position_id=position_id,
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        closed_quantity=filled_quantity,
        exit_value=premium_float(exit_value),
        realized_pnl=realized_pnl,
        broker_order_id=as_text(attempt.get("broker_order_id")),
        closed_at=_resolve_closed_at(attempt),
        created_at=now,
        updated_at=now,
    )
    return _recalculate_position(
        execution_store=execution_store,
        position_id=position_id,
        last_broker_status=broker_status,
    )


def sync_session_position_from_attempt(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not execution_store.portfolio_schema_ready():
        return None
    trade_intent = resolve_attempt_trade_intent(attempt)
    if trade_intent == CLOSE_TRADE_INTENT:
        return _sync_close_position(execution_store=execution_store, attempt=attempt)
    return _sync_open_position(execution_store=execution_store, attempt=attempt)

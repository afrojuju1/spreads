from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from spreads.services.execution_lifecycle import (
    resolve_execution_attempt_filled_quantity,
    resolve_execution_attempt_primary_order,
)
from spreads.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    normalize_legs,
    position_legs as canonical_position_legs,
)
from spreads.services.runtime_identity import (
    build_pipeline_id,
    resolve_pipeline_policy_fields,
)

OPEN_TRADE_INTENT = "open"
CLOSE_TRADE_INTENT = "close"
SUPPORTED_TRADE_INTENTS = {OPEN_TRADE_INTENT, CLOSE_TRADE_INTENT}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


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


def _round_money(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _derive_live_exposure(
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

    entry_notional = (
        None
        if entry_value is None
        else round(entry_value * 100.0 * normalized_quantity, 2)
    )
    if str(strategy_family or "").strip().lower() in {"long_straddle", "long_strangle"}:
        return {
            "entry_notional": entry_notional,
            "max_profit": None,
            "max_loss": entry_notional,
        }
    premium_kind = net_premium_kind(strategy_family)
    max_profit = entry_notional
    max_loss = None
    if entry_value is not None and width is not None:
        if premium_kind == "debit":
            max_profit = round(
                max(width - entry_value, 0.0) * 100.0 * normalized_quantity, 2
            )
            max_loss = entry_notional
        else:
            max_profit = entry_notional
            max_loss = round(
                max(width - entry_value, 0.0) * 100.0 * normalized_quantity, 2
            )
    return {
        "entry_notional": entry_notional,
        "max_profit": max_profit,
        "max_loss": max_loss,
    }


def resolve_trade_intent(value: Any) -> str:
    normalized = (_as_text(value) or OPEN_TRADE_INTENT).lower()
    if normalized not in SUPPORTED_TRADE_INTENTS:
        raise ValueError(f"Unsupported trade intent: {normalized}")
    return normalized


def resolve_attempt_trade_intent(attempt: Mapping[str, Any]) -> str:
    request = attempt.get("request")
    if isinstance(request, Mapping):
        return resolve_trade_intent(request.get("trade_intent"))
    return resolve_trade_intent(attempt.get("trade_intent"))


def resolve_attempt_session_position_id(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    request_value = (
        request.get("session_position_id") if isinstance(request, Mapping) else None
    )
    return _as_text(attempt.get("session_position_id")) or _as_text(request_value)


def resolve_attempt_position_id(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    request_value = request.get("position_id") if isinstance(request, Mapping) else None
    return _as_text(attempt.get("position_id")) or _as_text(request_value)


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
    return _as_text(request.get("execution_intent_id"))


def _resolve_primary_order(attempt: Mapping[str, Any]) -> Mapping[str, Any] | None:
    return resolve_execution_attempt_primary_order(attempt)


def _resolve_filled_quantity(
    attempt: Mapping[str, Any], primary_order: Mapping[str, Any] | None
) -> float:
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
        if _as_text(fill.get("symbol")) != symbol:
            continue
        price = _coerce_float(fill.get("price"))
        quantity = _coerce_float(fill.get("quantity"))
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
        order_symbol = _as_text(order.get("leg_symbol")) or _as_text(
            order.get("symbol")
        )
        if order_symbol != symbol:
            continue
        price = _coerce_float(order.get("filled_avg_price"))
        quantity = _coerce_float(order.get("filled_qty"))
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
    legs = normalize_legs(
        order.get("legs"),
        expiration_date=_as_text(attempt.get("expiration_date")),
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
        symbol = _as_text(leg.get("symbol"))
        role = _as_text(leg.get("role"))
        ratio_qty = _coerce_float(leg.get("ratio_qty")) or 1.0
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
        return round(abs(short_total - long_total), 4)

    # Alpaca returns parent multi-leg fills as a signed net price:
    # credit opens are negative, debit closes are positive. Session
    # positions persist canonical economics instead: positive entry
    # credit and positive exit debit.
    if primary_order is not None:
        price = _coerce_float(primary_order.get("filled_avg_price"))
        if price is not None and filled_quantity > 0:
            return round(abs(price), 4)

    limit_price = _coerce_float(attempt.get("limit_price"))
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
    fill_times = [
        _as_text(fill.get("filled_at"))
        for fill in attempt.get("fills") or []
        if isinstance(fill, Mapping)
    ]
    filtered = [value for value in fill_times if value]
    if filtered:
        return min(filtered)
    return _as_text(attempt.get("submitted_at")) or _as_text(
        attempt.get("requested_at")
    )


def _sync_linked_execution_intent_position(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    position_id: str,
) -> None:
    if not execution_store.intent_schema_ready():
        return
    execution_intent_id = _attempt_execution_intent_id(attempt)
    if execution_intent_id is None:
        return
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        return
    payload = (
        intent.get("payload") if isinstance(intent.get("payload"), Mapping) else {}
    )
    execution_store.upsert_execution_intent(
        execution_intent_id=str(intent["execution_intent_id"]),
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
        strategy_position_id=position_id,
        execution_attempt_id=_as_text(attempt.get("execution_attempt_id")),
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=_as_text(intent.get("claim_token")),
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state=str(intent.get("state") or ""),
        expires_at=_as_text(intent.get("expires_at")),
        superseded_by_id=_as_text(intent.get("superseded_by_id")),
        payload={
            **dict(payload),
            "strategy_position_id": position_id,
        },
        created_at=str(intent["created_at"]),
        updated_at=_utc_now(),
    )


def _resolve_closed_at(attempt: Mapping[str, Any]) -> str | None:
    fill_times = [
        _as_text(fill.get("filled_at"))
        for fill in attempt.get("fills") or []
        if isinstance(fill, Mapping)
    ]
    filtered = [value for value in fill_times if value]
    if filtered:
        return max(filtered)
    return _as_text(attempt.get("completed_at")) or _as_text(
        attempt.get("submitted_at")
    )


def _resolve_width(attempt: Mapping[str, Any]) -> float | None:
    candidate = attempt.get("candidate")
    if not isinstance(candidate, Mapping):
        return None
    short_strike = _coerce_float(candidate.get("short_strike"))
    long_strike = _coerce_float(candidate.get("long_strike"))
    if short_strike is None or long_strike is None:
        return None
    return abs(short_strike - long_strike)


def _position_legs(position: Mapping[str, Any]) -> list[dict[str, Any]]:
    return canonical_position_legs(position)


def _position_economics(position: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "entry_credit": _coerce_float(position.get("entry_credit")),
        "entry_value": _coerce_float(position.get("entry_value"))
        or _coerce_float(position.get("entry_credit")),
        "entry_notional": _coerce_float(position.get("entry_notional")),
        "max_profit": _coerce_float(position.get("max_profit")),
        "max_loss": _coerce_float(position.get("max_loss")),
    }


def _position_strategy_metrics(position: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "width": _coerce_float(position.get("width")),
        "strategy": _as_text(position.get("strategy")),
    }


def _new_position_id() -> str:
    return f"position:{uuid4().hex}"


def _position_width(position: Mapping[str, Any]) -> float | None:
    strategy_metrics = (
        position.get("strategy_metrics")
        if isinstance(position.get("strategy_metrics"), Mapping)
        else {}
    )
    return _coerce_float(strategy_metrics.get("width")) or _coerce_float(
        position.get("width")
    )


def _position_entry_value(position: Mapping[str, Any]) -> float | None:
    economics = (
        position.get("economics")
        if isinstance(position.get("economics"), Mapping)
        else {}
    )
    return (
        _coerce_float(position.get("entry_value"))
        or _coerce_float(economics.get("entry_value"))
        or _coerce_float(economics.get("entry_credit"))
    )


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
    candidate = (
        attempt.get("candidate")
        if isinstance(attempt.get("candidate"), Mapping)
        else {}
    )
    root_symbol = str(
        attempt.get("underlying_symbol") or existing.get("root_symbol") or ""
    )
    pipeline_id = (
        _as_text(attempt.get("pipeline_id"))
        or _as_text(existing.get("pipeline_id"))
        or build_pipeline_id(str(attempt.get("label") or ""))
    )
    market_date = (
        _as_text(attempt.get("market_date"))
        or _as_text(attempt.get("session_date"))
        or _as_text(existing.get("market_date_opened"))
    )
    policy_fields = resolve_pipeline_policy_fields(
        profile=candidate.get("profile")
        or _as_text(attempt.get("style_profile"))
        or _as_text(existing.get("style_profile")),
        root_symbol=root_symbol,
    )
    width = _resolve_width(attempt) or _position_width(existing or {})
    strategy_family = _as_text(attempt.get("strategy_family")) or _as_text(
        existing.get("strategy_family") if isinstance(existing, Mapping) else None
    )
    exposure = _derive_live_exposure(
        entry_value=entry_credit,
        width=width,
        quantity=remaining_quantity,
        strategy_family=strategy_family,
    )
    exit_policy = (
        dict(existing.get("exit_policy") or {})
        if isinstance(existing, Mapping)
        and isinstance(existing.get("exit_policy"), Mapping)
        else {}
    )
    exit_policy.update(_attempt_exit_policy(attempt))
    risk_policy = (
        dict(existing.get("risk_policy") or {})
        if isinstance(existing, Mapping)
        and isinstance(existing.get("risk_policy"), Mapping)
        else {}
    )
    risk_policy.update(_attempt_risk_policy(attempt))
    request = _attempt_request(attempt)
    order = request.get("order") if isinstance(request.get("order"), Mapping) else {}
    attempt_legs = normalize_legs(
        order.get("legs"),
        expiration_date=_as_text(attempt.get("expiration_date")),
    )
    if not attempt_legs and candidate:
        attempt_legs = candidate_legs(candidate)
    existing_legs = canonical_position_legs(existing or {})
    persisted_legs = attempt_legs or existing_legs
    source_job = _attempt_source_job(attempt)
    return {
        "pipeline_id": pipeline_id,
        "source_opportunity_id": _as_text(attempt.get("opportunity_id"))
        or _as_text(
            existing.get("source_opportunity_id")
            if isinstance(existing, Mapping)
            else None
        ),
        "root_symbol": root_symbol,
        "strategy_family": strategy_family or str(attempt.get("strategy") or "unknown"),
        "style_profile": _as_text(attempt.get("style_profile"))
        or _as_text(
            existing.get("style_profile") if isinstance(existing, Mapping) else None
        )
        or str(policy_fields["style_profile"]),
        "horizon_intent": _as_text(attempt.get("horizon_intent"))
        or _as_text(
            existing.get("horizon_intent") if isinstance(existing, Mapping) else None
        )
        or str(policy_fields["horizon_intent"]),
        "product_class": _as_text(attempt.get("product_class"))
        or _as_text(
            existing.get("product_class") if isinstance(existing, Mapping) else None
        )
        or str(policy_fields["product_class"]),
        "market_date_opened": market_date,
        "market_date_closed": None
        if closed_at is None or market_date is None
        else market_date,
        "status": status,
        "legs": persisted_legs,
        "economics": {
            "entry_credit": _round_money(entry_credit),
            "entry_value": _round_money(entry_credit),
            "entry_value_kind": net_premium_kind(strategy_family),
            "entry_notional": exposure["entry_notional"],
            "max_profit": exposure["max_profit"],
            "max_loss": exposure["max_loss"],
        },
        "strategy_metrics": {
            "width": width,
            "strategy": _as_text(attempt.get("strategy"))
            or _as_text(
                existing.get("strategy_family")
                if isinstance(existing, Mapping)
                else None
            ),
        },
        "requested_quantity": requested_quantity,
        "opened_quantity": opened_quantity,
        "remaining_quantity": remaining_quantity,
        "entry_value": _round_money(entry_credit),
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "close_mark": close_mark,
        "close_mark_source": close_mark_source,
        "close_marked_at": close_marked_at,
        "last_broker_status": last_broker_status,
        "exit_policy": exit_policy,
        "risk_policy": risk_policy,
        "source_job_type": _as_text(source_job.get("job_type"))
        or _as_text(
            existing.get("source_job_type") if isinstance(existing, Mapping) else None
        ),
        "source_job_key": _as_text(source_job.get("job_key"))
        or _as_text(
            existing.get("source_job_key") if isinstance(existing, Mapping) else None
        ),
        "source_job_run_id": _as_text(source_job.get("job_run_id"))
        or _as_text(
            existing.get("source_job_run_id") if isinstance(existing, Mapping) else None
        ),
        "last_exit_evaluated_at": last_exit_evaluated_at,
        "last_exit_reason": last_exit_reason,
        "last_reconciled_at": last_reconciled_at,
        "reconciliation_status": reconciliation_status,
        "reconciliation_note": reconciliation_note,
        "opened_at": opened_at,
        "closed_at": closed_at,
        "updated_at": _utc_now(),
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
    opened_quantity = _coerce_float(position.get("opened_quantity")) or 0.0
    total_closed_quantity = sum(
        _coerce_float(close.get("closed_quantity")) or 0.0 for close in closes
    )
    remaining_quantity = max(opened_quantity - total_closed_quantity, 0.0)
    realized_pnl = round(
        sum(_coerce_float(close.get("realized_pnl")) or 0.0 for close in closes), 2
    )
    entry_credit = _position_entry_value(position)
    width = _position_width(position)
    strategy_family = _as_text(position.get("strategy_family")) or _as_text(
        position.get("strategy")
    )
    exposure = _derive_live_exposure(
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
        closed_times = [_as_text(close.get("closed_at")) for close in closes]
        closed_at = max((value for value in closed_times if value), default=None)
        unrealized_pnl = 0.0
        market_date_closed = _as_text(position.get("market_date_opened"))
    else:
        status = "partial_close"
        closed_at = None
        unrealized_pnl = None
        market_date_closed = None

    updated = execution_store.update_position(
        position_id=position_id,
        status=status,
        market_date_closed=market_date_closed,
        remaining_quantity=remaining_quantity,
        economics={
            "entry_credit": _round_money(entry_credit),
            "entry_value": _round_money(entry_credit),
            "entry_value_kind": net_premium_kind(strategy_family),
            "entry_notional": exposure["entry_notional"],
            "max_profit": exposure["max_profit"],
            "max_loss": exposure["max_loss"],
        },
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        closed_at=closed_at,
        last_broker_status=last_broker_status,
        updated_at=_utc_now(),
    )
    return updated


def _sync_open_position(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
) -> dict[str, Any] | None:
    primary_order = _resolve_primary_order(attempt)
    filled_quantity = _resolve_filled_quantity(attempt, primary_order)
    if filled_quantity <= 0:
        return None

    requested_quantity = _coerce_int(attempt.get("quantity")) or max(
        int(round(filled_quantity)), 1
    )
    entry_credit = _resolve_spread_amount(attempt, primary_order, filled_quantity)
    broker_status = (_as_text(attempt.get("status")) or "unknown").lower()
    position_id = resolve_attempt_position_id(attempt)
    existing = (
        None if position_id is None else execution_store.get_position(position_id)
    )
    if existing is None:
        existing = execution_store.get_position_by_open_attempt(
            str(attempt["execution_attempt_id"])
        )
    if existing is None:
        position_id = position_id or _new_position_id()
        created = execution_store.create_position(
            position_id=position_id,
            open_execution_attempt_id=str(attempt["execution_attempt_id"]),
            created_at=_utc_now(),
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
        existing = execution_store.update_position(
            position_id=position_id,
            **_position_common_payload(
                attempt=attempt,
                existing=existing,
                requested_quantity=requested_quantity,
                opened_quantity=filled_quantity,
                remaining_quantity=filled_quantity,
                entry_credit=entry_credit,
                realized_pnl=_coerce_float(existing.get("realized_pnl")) or 0.0,
                unrealized_pnl=_coerce_float(existing.get("unrealized_pnl")),
                close_mark=_coerce_float(existing.get("close_mark")),
                close_mark_source=_as_text(existing.get("close_mark_source")),
                close_marked_at=_as_text(existing.get("close_marked_at")),
                last_broker_status=broker_status,
                status=_resolve_position_status(broker_status, filled_quantity),
                opened_at=_resolve_opened_at(attempt),
                closed_at=None,
                last_exit_evaluated_at=_as_text(existing.get("last_exit_evaluated_at")),
                last_exit_reason=_as_text(existing.get("last_exit_reason")),
                last_reconciled_at=_as_text(existing.get("last_reconciled_at")),
                reconciliation_status=_as_text(existing.get("reconciliation_status")),
                reconciliation_note=_as_text(existing.get("reconciliation_note")),
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
    position_id = resolve_attempt_position_id(attempt) or _as_text(
        attempt.get("position_id")
    )
    if position_id is None:
        legacy_session_position_id = resolve_attempt_session_position_id(attempt)
        if legacy_session_position_id is not None:
            fallback = execution_store.get_position(legacy_session_position_id)
            if fallback is not None:
                position_id = str(fallback["position_id"])
    if position_id is None:
        raise ValueError("Close execution attempt is missing a canonical position_id")
    position = execution_store.get_position(position_id)
    if position is None:
        raise ValueError(f"Unknown position_id: {position_id}")

    broker_status = (_as_text(attempt.get("status")) or "unknown").lower()
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
        updated_at=_utc_now(),
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
    realized_pnl = 0.0
    if entry_credit is not None and exit_value is not None:
        realized_pnl = round((entry_credit - exit_value) * 100.0 * filled_quantity, 2)

    now = _utc_now()
    execution_store.upsert_position_close(
        position_id=position_id,
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        closed_quantity=filled_quantity,
        exit_value=_round_money(exit_value),
        realized_pnl=realized_pnl,
        broker_order_id=_as_text(attempt.get("broker_order_id")),
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

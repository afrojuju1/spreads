from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

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


def _new_session_position_id() -> str:
    return f"session_position:{uuid4().hex}"


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
    request_value = request.get("session_position_id") if isinstance(request, Mapping) else None
    return _as_text(attempt.get("session_position_id")) or _as_text(request_value)


def _resolve_primary_order(attempt: Mapping[str, Any]) -> Mapping[str, Any] | None:
    orders = attempt.get("orders")
    if not isinstance(orders, list):
        return None
    primary = next(
        (
            order
            for order in orders
            if isinstance(order, Mapping) and not _as_text(order.get("parent_broker_order_id"))
        ),
        None,
    )
    if primary is not None:
        return primary
    return next((order for order in orders if isinstance(order, Mapping)), None)


def _resolve_filled_quantity(attempt: Mapping[str, Any], primary_order: Mapping[str, Any] | None) -> float:
    primary_value = None if primary_order is None else _coerce_float(primary_order.get("filled_qty"))
    if primary_value is not None and primary_value > 0:
        return primary_value

    order_values: list[float] = []
    for order in attempt.get("orders") or []:
        if not isinstance(order, Mapping):
            continue
        filled = _coerce_float(order.get("filled_qty"))
        if filled is not None and filled > 0:
            order_values.append(filled)
    if order_values:
        return max(order_values)

    fill_values: list[float] = []
    for fill in attempt.get("fills") or []:
        if not isinstance(fill, Mapping):
            continue
        cumulative = _coerce_float(fill.get("cumulative_quantity"))
        quantity = _coerce_float(fill.get("quantity"))
        candidate = cumulative if cumulative is not None and cumulative > 0 else quantity
        if candidate is not None and candidate > 0:
            fill_values.append(candidate)
    if fill_values:
        return max(fill_values)
    return 0.0


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
        order_symbol = _as_text(order.get("leg_symbol")) or _as_text(order.get("symbol"))
        if order_symbol != symbol:
            continue
        price = _coerce_float(order.get("filled_avg_price"))
        quantity = _coerce_float(order.get("filled_qty"))
        if price is None or quantity is None:
            continue
        order_pairs.append((price, quantity))
    return _weighted_average(order_pairs)


def _resolve_spread_price(
    attempt: Mapping[str, Any],
    primary_order: Mapping[str, Any] | None,
    filled_quantity: float,
) -> float | None:
    if primary_order is not None:
        price = _coerce_float(primary_order.get("filled_avg_price"))
        if price is not None and filled_quantity > 0:
            return price

    short_symbol = _as_text(attempt.get("short_symbol"))
    long_symbol = _as_text(attempt.get("long_symbol"))
    if short_symbol and long_symbol:
        short_price = _resolve_leg_average_price(attempt, short_symbol)
        long_price = _resolve_leg_average_price(attempt, long_symbol)
        if short_price is not None and long_price is not None:
            return round(short_price - long_price, 4)

    limit_price = _coerce_float(attempt.get("limit_price"))
    if limit_price is not None and filled_quantity > 0:
        return limit_price
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
    return _as_text(attempt.get("submitted_at")) or _as_text(attempt.get("requested_at"))


def _resolve_closed_at(attempt: Mapping[str, Any]) -> str | None:
    fill_times = [
        _as_text(fill.get("filled_at"))
        for fill in attempt.get("fills") or []
        if isinstance(fill, Mapping)
    ]
    filtered = [value for value in fill_times if value]
    if filtered:
        return max(filtered)
    return _as_text(attempt.get("completed_at")) or _as_text(attempt.get("submitted_at"))


def _resolve_width(attempt: Mapping[str, Any]) -> float | None:
    candidate = attempt.get("candidate")
    if not isinstance(candidate, Mapping):
        return None
    short_strike = _coerce_float(candidate.get("short_strike"))
    long_strike = _coerce_float(candidate.get("long_strike"))
    if short_strike is None or long_strike is None:
        return None
    return abs(short_strike - long_strike)


def _recalculate_session_position(
    *,
    execution_store: Any,
    session_position_id: str,
    last_broker_status: str | None,
) -> dict[str, Any]:
    position = execution_store.get_session_position(session_position_id)
    if position is None:
        raise ValueError(f"Unknown session_position_id: {session_position_id}")

    closes = execution_store.list_session_position_closes(session_position_id=session_position_id)
    opened_quantity = _coerce_float(position.get("opened_quantity")) or 0.0
    total_closed_quantity = sum(_coerce_float(close.get("closed_quantity")) or 0.0 for close in closes)
    remaining_quantity = max(opened_quantity - total_closed_quantity, 0.0)
    realized_pnl = round(sum(_coerce_float(close.get("realized_pnl")) or 0.0 for close in closes), 2)

    if total_closed_quantity <= 0:
        status = "open"
        closed_at = None
        unrealized_pnl = None
    elif remaining_quantity <= 0:
        status = "closed"
        closed_times = [_as_text(close.get("closed_at")) for close in closes]
        closed_at = max((value for value in closed_times if value), default=None)
        unrealized_pnl = 0.0
    else:
        status = "partial_close"
        closed_at = None
        unrealized_pnl = None

    updated = execution_store.update_session_position(
        session_position_id=session_position_id,
        remaining_quantity=remaining_quantity,
        status=status,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        closed_at=closed_at,
        last_broker_status=last_broker_status,
        updated_at=_utc_now(),
    )
    return updated.to_dict()


def _sync_open_session_position(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
) -> dict[str, Any] | None:
    primary_order = _resolve_primary_order(attempt)
    filled_quantity = _resolve_filled_quantity(attempt, primary_order)
    if filled_quantity <= 0:
        return None

    existing = execution_store.get_session_position_by_open_attempt(str(attempt["execution_attempt_id"]))
    session_position_id = (
        str(existing["session_position_id"])
        if existing is not None
        else resolve_attempt_session_position_id(attempt) or _new_session_position_id()
    )
    requested_quantity = _coerce_int(attempt.get("quantity")) or max(int(round(filled_quantity)), 1)
    entry_credit = _resolve_spread_price(attempt, primary_order, filled_quantity)
    width = _resolve_width(attempt)
    entry_notional = None if entry_credit is None else round(entry_credit * 100.0 * filled_quantity, 2)
    max_profit = entry_notional
    max_loss = None
    if entry_credit is not None and width is not None:
        max_loss = round(max(width - entry_credit, 0.0) * 100.0 * filled_quantity, 2)

    if existing is None:
        execution_store.create_session_position(
            session_position_id=session_position_id,
            session_id=str(attempt["session_id"]),
            session_date=str(attempt["session_date"]),
            label=str(attempt["label"]),
            candidate_id=_coerce_int(attempt.get("candidate_id")),
            open_execution_attempt_id=str(attempt["execution_attempt_id"]),
            underlying_symbol=str(attempt["underlying_symbol"]),
            strategy=str(attempt["strategy"]),
            expiration_date=str(attempt["expiration_date"]),
            short_symbol=str(attempt["short_symbol"]),
            long_symbol=str(attempt["long_symbol"]),
            requested_quantity=requested_quantity,
            opened_quantity=filled_quantity,
            remaining_quantity=filled_quantity,
            entry_credit=_round_money(entry_credit),
            entry_notional=entry_notional,
            width=width,
            max_profit=max_profit,
            max_loss=max_loss,
            opened_at=_resolve_opened_at(attempt),
            closed_at=None,
            status=_resolve_position_status((_as_text(attempt.get("status")) or "unknown").lower(), filled_quantity),
            realized_pnl=0.0,
            unrealized_pnl=None,
            close_mark=None,
            close_mark_source=None,
            close_marked_at=None,
            last_broker_status=(_as_text(attempt.get("status")) or "unknown").lower(),
            created_at=_utc_now(),
            updated_at=_utc_now(),
        )
    else:
        execution_store.update_session_position(
            session_position_id=session_position_id,
            opened_quantity=filled_quantity,
            entry_credit=_round_money(entry_credit),
            entry_notional=entry_notional,
            width=width,
            max_profit=max_profit,
            max_loss=max_loss,
            opened_at=_resolve_opened_at(attempt),
            last_broker_status=(_as_text(attempt.get("status")) or "unknown").lower(),
            updated_at=_utc_now(),
        )

    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        session_position_id=session_position_id,
    )
    return _recalculate_session_position(
        execution_store=execution_store,
        session_position_id=session_position_id,
        last_broker_status=(_as_text(attempt.get("status")) or "unknown").lower(),
    )


def _sync_close_session_position(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
) -> dict[str, Any]:
    session_position_id = resolve_attempt_session_position_id(attempt)
    if session_position_id is None:
        raise ValueError("Close execution attempt is missing a session_position_id")
    position = execution_store.get_session_position(session_position_id)
    if position is None:
        raise ValueError(f"Unknown session_position_id: {session_position_id}")

    broker_status = (_as_text(attempt.get("status")) or "unknown").lower()
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        session_position_id=session_position_id,
    )
    execution_store.update_session_position(
        session_position_id=session_position_id,
        last_broker_status=broker_status,
        updated_at=_utc_now(),
    )

    primary_order = _resolve_primary_order(attempt)
    filled_quantity = _resolve_filled_quantity(attempt, primary_order)
    if filled_quantity <= 0:
        return _recalculate_session_position(
            execution_store=execution_store,
            session_position_id=session_position_id,
            last_broker_status=broker_status,
        )

    exit_debit = _resolve_spread_price(attempt, primary_order, filled_quantity)
    entry_credit = _coerce_float(position.get("entry_credit"))
    realized_pnl = 0.0
    if entry_credit is not None and exit_debit is not None:
        realized_pnl = round((entry_credit - exit_debit) * 100.0 * filled_quantity, 2)

    now = _utc_now()
    execution_store.upsert_session_position_close(
        session_position_id=session_position_id,
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        closed_quantity=filled_quantity,
        exit_debit=_round_money(exit_debit),
        realized_pnl=realized_pnl,
        broker_order_id=_as_text(attempt.get("broker_order_id")),
        closed_at=_resolve_closed_at(attempt),
        created_at=now,
        updated_at=now,
    )
    return _recalculate_session_position(
        execution_store=execution_store,
        session_position_id=session_position_id,
        last_broker_status=broker_status,
    )


def sync_session_position_from_attempt(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not execution_store.positions_schema_ready():
        return None
    trade_intent = resolve_attempt_trade_intent(attempt)
    if trade_intent == CLOSE_TRADE_INTENT:
        return _sync_close_session_position(execution_store=execution_store, attempt=attempt)
    return _sync_open_session_position(execution_store=execution_store, attempt=attempt)

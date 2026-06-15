from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import and_, or_, select

from core.db.decorators import with_storage
from core.money import money_sum_float
from core.jobs.orchestration import NEW_YORK
from core.storage.execution_models import ExecutionAttemptModel, ExecutionFillModel, ExecutionOrderModel, PositionCloseModel
from core.storage.serializers import parse_date
from core.value_coercion import coerce_float, utc_iso, utc_now_iso

TERMINAL_ATTEMPT_STATUSES = {
    "canceled",
    "cancelled",
    "expired",
    "failed",
    "filled",
    "rejected",
}
ERROR_ATTEMPT_STATUSES = {
    "failed",
    "rejected",
}


def _activity_date_or_today(activity_date: str | None) -> str:
    if activity_date:
        return parse_date(activity_date).isoformat()
    return datetime.now(NEW_YORK).date().isoformat()


def _activity_window(activity_date: str) -> tuple[date, datetime, datetime]:
    activity_day = parse_date(activity_date)
    start = datetime.combine(activity_day, datetime.min.time(), tzinfo=UTC)
    return activity_day, start, start + timedelta(days=1)


def _date_text(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _attempt_row(row: ExecutionAttemptModel) -> dict[str, Any]:
    return {
        "execution_attempt_id": row.execution_attempt_id,
        "session_id": row.session_id,
        "session_date": _date_text(row.session_date),
        "trading_strategy_id": row.trading_strategy_id,
        "market_date": _date_text(row.market_date),
        "trade_intent": row.trade_intent,
        "attempt_context": row.attempt_context,
        "status": row.status,
        "underlying_symbol": row.underlying_symbol,
        "strategy": row.strategy,
        "structure_identity": row.structure_identity,
        "requested_quantity": row.requested_quantity,
        "quantity": row.quantity,
        "requested_limit_price": row.requested_limit_price,
        "limit_price": row.limit_price,
        "requested_at": utc_iso(row.requested_at),
        "submitted_at": utc_iso(row.submitted_at),
        "completed_at": utc_iso(row.completed_at),
        "broker": row.broker,
        "broker_order_id": row.broker_order_id,
        "client_order_id": row.client_order_id,
        "trade_signal_id": row.trade_signal_id,
        "trade_decision_id": row.trade_decision_id,
        "admission_decision_id": row.admission_decision_id,
        "position_id": row.position_id,
        "error_text": row.error_text,
        "legs": list(row.legs_json or []),
        "economics": dict(row.economics_json or {}),
    }


def _order_row(row: ExecutionOrderModel) -> dict[str, Any]:
    return {
        "execution_order_id": row.execution_order_id,
        "execution_attempt_id": row.execution_attempt_id,
        "broker": row.broker,
        "broker_order_id": row.broker_order_id,
        "parent_broker_order_id": row.parent_broker_order_id,
        "client_order_id": row.client_order_id,
        "order_status": row.order_status,
        "order_type": row.order_type,
        "time_in_force": row.time_in_force,
        "order_class": row.order_class,
        "side": row.side,
        "symbol": row.symbol,
        "leg_symbol": row.leg_symbol,
        "leg_side": row.leg_side,
        "position_intent": row.position_intent,
        "quantity": row.quantity,
        "limit_price": row.limit_price,
        "filled_qty": row.filled_qty,
        "filled_avg_price": row.filled_avg_price,
        "submitted_at": utc_iso(row.submitted_at),
        "updated_at": utc_iso(row.updated_at),
    }


def _fill_row(row: ExecutionFillModel) -> dict[str, Any]:
    return {
        "execution_fill_id": row.execution_fill_id,
        "execution_attempt_id": row.execution_attempt_id,
        "execution_order_id": row.execution_order_id,
        "broker": row.broker,
        "broker_fill_id": row.broker_fill_id,
        "broker_order_id": row.broker_order_id,
        "symbol": row.symbol,
        "side": row.side,
        "fill_type": row.fill_type,
        "quantity": row.quantity,
        "price": row.price,
        "filled_at": utc_iso(row.filled_at),
    }


def _close_row(row: PositionCloseModel) -> dict[str, Any]:
    return {
        "position_close_id": row.position_close_id,
        "position_id": row.position_id,
        "execution_attempt_id": row.execution_attempt_id,
        "closed_quantity": row.closed_quantity,
        "exit_value": row.exit_value,
        "realized_pnl": row.realized_pnl,
        "broker_order_id": row.broker_order_id,
        "closed_at": utc_iso(row.closed_at),
    }


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counter = Counter(str(row.get(key) or "unknown") for row in rows)
    return dict(sorted(counter.items()))


def _activity_at(attempt: dict[str, Any]) -> str | None:
    for key in ("completed_at", "submitted_at", "requested_at"):
        value = attempt.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _summarize_attempts(attempts: list[dict[str, Any]], orders: list[dict[str, Any]], fills: list[dict[str, Any]], closes: list[dict[str, Any]]) -> dict[str, Any]:
    parent_order_count = sum(1 for row in orders if row.get("parent_broker_order_id") is None)
    realized_pnl = money_sum_float(coerce_float(row.get("realized_pnl")) for row in closes)
    terminal_attempt_count = sum(1 for row in attempts if str(row.get("status") or "").strip().lower() in TERMINAL_ATTEMPT_STATUSES)
    error_attempt_count = sum(
        1
        for row in attempts
        if str(row.get("status") or "").strip().lower() in ERROR_ATTEMPT_STATUSES or bool(row.get("error_text"))
    )
    activity_times = [value for row in attempts if (value := _activity_at(row)) is not None]
    activity_date = attempts[0].get("activity_date") if attempts else None
    cross_market_date_count = sum(1 for row in attempts if activity_date is not None and row.get("market_date") not in {None, activity_date})
    return {
        "operator_status": "healthy" if error_attempt_count == 0 and terminal_attempt_count == len(attempts) else "needs_review",
        "attempt_count": len(attempts),
        "terminal_attempt_count": terminal_attempt_count,
        "open_attempt_count": len(attempts) - terminal_attempt_count,
        "error_attempt_count": error_attempt_count,
        "order_count": len(orders),
        "parent_order_count": parent_order_count,
        "leg_order_count": len(orders) - parent_order_count,
        "fill_count": len(fills),
        "position_close_count": len(closes),
        "cross_market_date_attempt_count": cross_market_date_count,
        "first_activity_at": min(activity_times, default=None),
        "latest_activity_at": max(activity_times, default=None),
        "realized_pnl": realized_pnl,
        "strategy_counts": _count_by(attempts, "trading_strategy_id"),
        "intent_counts": _count_by(attempts, "trade_intent"),
        "status_counts": _count_by(attempts, "status"),
    }


@with_storage()
def list_execution_activity(
    *,
    db_target: str | None = None,
    activity_date: str | None = None,
    trading_strategy_id: str | None = None,
    limit: int = 100,
    storage: Any | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("--limit must be greater than 0.")

    execution_store = storage.execution
    if not execution_store.schema_ready():
        raise RuntimeError("Execution attempt/order/fill storage is not available yet.")

    resolved_activity_date = _activity_date_or_today(activity_date)
    activity_day, start, end = _activity_window(resolved_activity_date)
    with execution_store.session_factory() as session:
        statement = select(ExecutionAttemptModel).where(
            or_(
                ExecutionAttemptModel.market_date == activity_day,
                and_(ExecutionAttemptModel.requested_at >= start, ExecutionAttemptModel.requested_at < end),
            )
        )
        if trading_strategy_id:
            statement = statement.where(ExecutionAttemptModel.trading_strategy_id == trading_strategy_id)
        attempt_models = session.scalars(
            statement.order_by(ExecutionAttemptModel.requested_at.asc(), ExecutionAttemptModel.execution_attempt_id.asc()).limit(limit)
        ).all()
        attempt_ids = [row.execution_attempt_id for row in attempt_models]

        order_models = (
            session.scalars(
                select(ExecutionOrderModel)
                .where(ExecutionOrderModel.execution_attempt_id.in_(attempt_ids))
                .order_by(ExecutionOrderModel.execution_attempt_id.asc(), ExecutionOrderModel.updated_at.asc(), ExecutionOrderModel.execution_order_id.asc())
            ).all()
            if attempt_ids
            else []
        )
        fill_models = (
            session.scalars(
                select(ExecutionFillModel)
                .where(ExecutionFillModel.execution_attempt_id.in_(attempt_ids))
                .order_by(ExecutionFillModel.execution_attempt_id.asc(), ExecutionFillModel.filled_at.asc(), ExecutionFillModel.execution_fill_id.asc())
            ).all()
            if attempt_ids
            else []
        )
        close_models = (
            session.scalars(
                select(PositionCloseModel)
                .where(PositionCloseModel.execution_attempt_id.in_(attempt_ids))
                .order_by(PositionCloseModel.execution_attempt_id.asc(), PositionCloseModel.closed_at.asc(), PositionCloseModel.position_close_id.asc())
            ).all()
            if attempt_ids
            else []
        )

    orders_by_attempt: dict[str, list[dict[str, Any]]] = {}
    for row in order_models:
        orders_by_attempt.setdefault(row.execution_attempt_id, []).append(_order_row(row))
    fills_by_attempt: dict[str, list[dict[str, Any]]] = {}
    for row in fill_models:
        fills_by_attempt.setdefault(row.execution_attempt_id, []).append(_fill_row(row))
    closes_by_attempt: dict[str, list[dict[str, Any]]] = {}
    for row in close_models:
        closes_by_attempt.setdefault(row.execution_attempt_id, []).append(_close_row(row))

    attempts: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    closes: list[dict[str, Any]] = []
    for row in attempt_models:
        attempt = _attempt_row(row)
        attempt_orders = orders_by_attempt.get(row.execution_attempt_id, [])
        attempt_fills = fills_by_attempt.get(row.execution_attempt_id, [])
        attempt_closes = closes_by_attempt.get(row.execution_attempt_id, [])
        attempt["orders"] = attempt_orders
        attempt["fills"] = attempt_fills
        attempt["position_closes"] = attempt_closes
        attempt["activity_date"] = resolved_activity_date
        attempt["order_count"] = len(attempt_orders)
        attempt["fill_count"] = len(attempt_fills)
        attempt["position_close_count"] = len(attempt_closes)
        attempt["realized_pnl"] = money_sum_float(coerce_float(close.get("realized_pnl")) for close in attempt_closes)
        attempts.append(attempt)
        orders.extend(attempt_orders)
        fills.extend(attempt_fills)
        closes.extend(attempt_closes)

    return {
        "status": "healthy",
        "activity_date": resolved_activity_date,
        "generated_at": utc_now_iso(),
        "limit": limit,
        "trading_strategy_id": trading_strategy_id,
        "scope": {
            "criteria": "attempt.market_date equals activity_date OR requested_at falls inside the UTC activity day",
            "requested_at_start": utc_iso(start),
            "requested_at_end": utc_iso(end),
        },
        "summary": _summarize_attempts(attempts, orders, fills, closes),
        "attempts": attempts,
    }


__all__ = ["list_execution_activity"]

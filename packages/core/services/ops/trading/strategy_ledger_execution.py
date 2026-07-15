from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select

from core.money import money_float, money_sum_float
from core.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionFillModel,
    ExecutionIntentModel,
    ExecutionOrderModel,
    PortfolioPositionModel,
    PositionCloseModel,
)
from core.storage.lifecycle_models import TradeCloseDecisionModel, TradePositionModel
from core.value_coercion import as_mapping, coerce_float, utc_iso

from .strategy_ledger_common import (
    bump_count as _bump_count,
    newer_desc_asc as _newer_desc_asc,
    newer_desc_desc as _newer_desc_desc,
    set_latest_activity as _set_latest_activity,
    sort_nested_counts as _sort_nested_counts,
)

LEDGER_MARK_STALE_AFTER_SECONDS = 15 * 60
OPEN_POSITION_STATUSES = {"open", "partial_open", "partial_close", "pending_open"}


def _empty_execution_strategy_ledger() -> dict[str, Any]:
    return {
        "intents": {
            "intent_count": 0,
            "intent_state_counts": {},
            "intent_kind_state_counts": {},
            "latest_execution_intent_id": None,
            "latest_created_at": None,
        },
        "attempts": {
            "attempt_count": 0,
            "attempt_status_counts": {},
            "attempt_intent_status_counts": {},
            "order_count": 0,
            "fill_count": 0,
            "latest_execution_attempt_id": None,
            "latest_requested_at": None,
        },
        "positions": {
            "position_count": 0,
            "open_position_count": 0,
            "closed_position_count": 0,
            "position_status_counts": {},
            "mark_count": 0,
            "missing_mark_count": 0,
            "stale_mark_count": 0,
            "latest_position_id": None,
            "latest_marked_at": None,
            "latest_updated_at": None,
        },
        "closes": {
            "close_decision_count": 0,
            "close_decision_state_counts": {},
            "close_decision_reason_counts": {},
            "latest_close_decision_id": None,
            "latest_close_decided_at": None,
            "close_count": 0,
            "latest_position_close_id": None,
            "latest_closed_at": None,
        },
        "pnl": {
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 0.0,
        },
        "latest_activity_at": None,
    }


def _bump_nested_count(mapping: dict[str, dict[str, int]], outer_value: Any, inner_value: Any) -> None:
    outer_key = str(outer_value or "unknown")
    inner_key = str(inner_value or "unknown")
    nested = mapping.setdefault(outer_key, {})
    nested[inner_key] = int(nested.get(inner_key, 0)) + 1


def build_execution_strategy_ledgers(
    *,
    session: Any,
    strategies: Iterable[Any],
    market_day: date,
    start: datetime,
    end: datetime,
    now: datetime,
    execution_schema_ready: bool,
    intent_schema_ready: bool,
    portfolio_schema_ready: bool,
    close_lifecycle_schema_ready: bool,
) -> dict[str, dict[str, Any]]:
    strategy_list = list(strategies)
    strategy_ids = [strategy.trading_strategy_id for strategy in strategy_list]
    payloads = {strategy_id: _empty_execution_strategy_ledger() for strategy_id in strategy_ids}
    if not strategy_ids:
        return payloads

    latest_activity: dict[str, datetime] = {}

    if intent_schema_ready:
        latest_intent_at: dict[str, datetime] = {}
        intent_rows = session.execute(
            select(
                ExecutionIntentModel.trading_strategy_id,
                ExecutionIntentModel.state,
                ExecutionIntentModel.intent_kind,
                ExecutionIntentModel.execution_intent_id,
                ExecutionIntentModel.created_at,
            )
            .where(ExecutionIntentModel.trading_strategy_id.in_(strategy_ids))
            .where(ExecutionIntentModel.created_at >= start)
            .where(ExecutionIntentModel.created_at < end)
            .order_by(
                ExecutionIntentModel.trading_strategy_id.asc(), ExecutionIntentModel.created_at.desc(), ExecutionIntentModel.execution_intent_id.asc()
            )
        )
        for strategy_id, state, intent_kind, execution_intent_id, created_at in intent_rows:
            strategy_key = str(strategy_id)
            intent_payload = payloads[strategy_key]["intents"]
            _bump_count(intent_payload["intent_state_counts"], state)
            _bump_nested_count(intent_payload["intent_kind_state_counts"], intent_kind, state)
            if _newer_desc_asc(created_at, execution_intent_id, latest_intent_at.get(strategy_key), intent_payload.get("latest_execution_intent_id")):
                latest_intent_at[strategy_key] = created_at
                intent_payload["latest_execution_intent_id"] = str(execution_intent_id)
                intent_payload["latest_created_at"] = utc_iso(created_at)
            _set_latest_activity(latest_activity, strategy_key, created_at)

    attempt_strategy: dict[str, str] = {}
    if execution_schema_ready:
        latest_attempt_at: dict[str, datetime] = {}
        attempt_rows = session.execute(
            select(
                ExecutionAttemptModel.trading_strategy_id,
                ExecutionAttemptModel.status,
                ExecutionAttemptModel.trade_intent,
                ExecutionAttemptModel.execution_attempt_id,
                ExecutionAttemptModel.requested_at,
            )
            .where(ExecutionAttemptModel.trading_strategy_id.in_(strategy_ids))
            .where(
                or_(
                    ExecutionAttemptModel.market_date == market_day,
                    and_(
                        ExecutionAttemptModel.requested_at >= start,
                        ExecutionAttemptModel.requested_at < end,
                    ),
                )
            )
            .order_by(
                ExecutionAttemptModel.trading_strategy_id.asc(),
                ExecutionAttemptModel.requested_at.desc(),
                ExecutionAttemptModel.execution_attempt_id.asc(),
            )
        )
        for strategy_id, status, trade_intent, execution_attempt_id, requested_at in attempt_rows:
            strategy_key = str(strategy_id)
            attempt_id = str(execution_attempt_id)
            attempt_strategy[attempt_id] = strategy_key
            attempt_payload = payloads[strategy_key]["attempts"]
            _bump_count(attempt_payload["attempt_status_counts"], status)
            _bump_nested_count(attempt_payload["attempt_intent_status_counts"], trade_intent, status)
            if _newer_desc_asc(
                requested_at,
                execution_attempt_id,
                latest_attempt_at.get(strategy_key),
                attempt_payload.get("latest_execution_attempt_id"),
            ):
                latest_attempt_at[strategy_key] = requested_at
                attempt_payload["latest_execution_attempt_id"] = attempt_id
                attempt_payload["latest_requested_at"] = utc_iso(requested_at)
            _set_latest_activity(latest_activity, strategy_key, requested_at)

        if attempt_strategy:
            order_rows = session.execute(
                select(ExecutionOrderModel.execution_attempt_id, func.count())
                .where(ExecutionOrderModel.execution_attempt_id.in_(list(attempt_strategy)))
                .group_by(ExecutionOrderModel.execution_attempt_id)
            )
            for execution_attempt_id, count in order_rows:
                strategy_key = attempt_strategy.get(str(execution_attempt_id))
                if strategy_key is not None:
                    payloads[strategy_key]["attempts"]["order_count"] += int(count or 0)

            fill_rows = session.execute(
                select(ExecutionFillModel.execution_attempt_id, func.count())
                .where(ExecutionFillModel.execution_attempt_id.in_(list(attempt_strategy)))
                .group_by(ExecutionFillModel.execution_attempt_id)
            )
            for execution_attempt_id, count in fill_rows:
                strategy_key = attempt_strategy.get(str(execution_attempt_id))
                if strategy_key is not None:
                    payloads[strategy_key]["attempts"]["fill_count"] += int(count or 0)

    if portfolio_schema_ready:
        latest_position_at: dict[str, datetime] = {}
        latest_marked_at: dict[str, datetime] = {}
        stale_after = now - timedelta(seconds=LEDGER_MARK_STALE_AFTER_SECONDS)
        position_rows = session.execute(
            select(
                PortfolioPositionModel.trading_strategy_id,
                PortfolioPositionModel.position_id,
                PortfolioPositionModel.status,
                PortfolioPositionModel.realized_pnl,
                PortfolioPositionModel.unrealized_pnl,
                PortfolioPositionModel.close_mark,
                PortfolioPositionModel.close_marked_at,
                PortfolioPositionModel.updated_at,
            )
            .where(PortfolioPositionModel.trading_strategy_id.in_(strategy_ids))
            .where(
                or_(
                    PortfolioPositionModel.market_date_opened == market_day,
                    PortfolioPositionModel.market_date_closed == market_day,
                    PortfolioPositionModel.status.in_(sorted(OPEN_POSITION_STATUSES)),
                )
            )
            .order_by(
                PortfolioPositionModel.trading_strategy_id.asc(), PortfolioPositionModel.updated_at.desc(), PortfolioPositionModel.position_id.asc()
            )
        )
        for strategy_id, position_id, status, realized_pnl, unrealized_pnl, close_mark, close_marked_at, updated_at in position_rows:
            strategy_key = str(strategy_id)
            position_payload = payloads[strategy_key]["positions"]
            pnl_payload = payloads[strategy_key]["pnl"]
            status_key = str(status or "unknown")
            _bump_count(position_payload["position_status_counts"], status_key)
            pnl_payload["realized_pnl"] += coerce_float(realized_pnl) or 0.0
            if status_key in OPEN_POSITION_STATUSES:
                pnl_payload["unrealized_pnl"] += coerce_float(unrealized_pnl) or 0.0
                if close_mark is None:
                    position_payload["missing_mark_count"] += 1
                else:
                    position_payload["mark_count"] += 1
                if close_marked_at is not None:
                    if close_marked_at < stale_after:
                        position_payload["stale_mark_count"] += 1
                    if close_marked_at > latest_marked_at.get(strategy_key, datetime.min.replace(tzinfo=UTC)):
                        latest_marked_at[strategy_key] = close_marked_at
                        position_payload["latest_marked_at"] = utc_iso(close_marked_at)
            if _newer_desc_asc(updated_at, position_id, latest_position_at.get(strategy_key), position_payload.get("latest_position_id")):
                latest_position_at[strategy_key] = updated_at
                position_payload["latest_position_id"] = str(position_id)
                position_payload["latest_updated_at"] = utc_iso(updated_at)
            _set_latest_activity(latest_activity, strategy_key, updated_at)

        latest_close_at: dict[str, datetime] = {}
        close_rows = session.execute(
            select(
                PortfolioPositionModel.trading_strategy_id,
                PositionCloseModel.position_close_id,
                PositionCloseModel.closed_at,
            )
            .join(PortfolioPositionModel, PositionCloseModel.position_id == PortfolioPositionModel.position_id)
            .where(PortfolioPositionModel.trading_strategy_id.in_(strategy_ids))
            .where(PositionCloseModel.closed_at >= start)
            .where(PositionCloseModel.closed_at < end)
            .order_by(
                PortfolioPositionModel.trading_strategy_id.asc(),
                PositionCloseModel.closed_at.desc(),
                PositionCloseModel.position_close_id.desc(),
            )
        )
        for strategy_id, position_close_id, closed_at in close_rows:
            strategy_key = str(strategy_id)
            close_payload = payloads[strategy_key]["closes"]
            close_payload["close_count"] += 1
            if _newer_desc_desc(closed_at, position_close_id, latest_close_at.get(strategy_key), close_payload.get("latest_position_close_id")):
                latest_close_at[strategy_key] = closed_at
                close_payload["latest_position_close_id"] = int(position_close_id)
                close_payload["latest_closed_at"] = utc_iso(closed_at)
            _set_latest_activity(latest_activity, strategy_key, closed_at)

    if close_lifecycle_schema_ready:
        latest_close_decision_at: dict[str, datetime] = {}
        close_decision_rows = session.execute(
            select(
                TradePositionModel.trading_strategy_id,
                TradeCloseDecisionModel.close_decision_id,
                TradeCloseDecisionModel.decision_state,
                TradeCloseDecisionModel.reason,
                TradeCloseDecisionModel.decided_at,
            )
            .join(TradePositionModel, TradeCloseDecisionModel.position_id == TradePositionModel.position_id)
            .where(TradePositionModel.trading_strategy_id.in_(strategy_ids))
            .where(TradePositionModel.session_date == market_day)
            .order_by(
                TradePositionModel.trading_strategy_id.asc(),
                TradeCloseDecisionModel.decided_at.desc(),
                TradeCloseDecisionModel.close_decision_id.asc(),
            )
        )
        for strategy_id, close_decision_id, decision_state, reason, decided_at in close_decision_rows:
            strategy_key = str(strategy_id)
            close_payload = payloads[strategy_key]["closes"]
            close_payload["close_decision_count"] += 1
            _bump_count(close_payload["close_decision_state_counts"], decision_state)
            _bump_count(close_payload["close_decision_reason_counts"], reason)
            if _newer_desc_asc(
                decided_at,
                close_decision_id,
                latest_close_decision_at.get(strategy_key),
                close_payload.get("latest_close_decision_id"),
            ):
                latest_close_decision_at[strategy_key] = decided_at
                close_payload["latest_close_decision_id"] = str(close_decision_id)
                close_payload["latest_close_decided_at"] = utc_iso(decided_at)
            _set_latest_activity(latest_activity, strategy_key, decided_at)

    for strategy_id, payload in payloads.items():
        intent_state_counts = dict(sorted((str(state), int(count)) for state, count in as_mapping(payload["intents"]["intent_state_counts"]).items()))
        payload["intents"]["intent_state_counts"] = intent_state_counts
        payload["intents"]["intent_kind_state_counts"] = _sort_nested_counts(payload["intents"]["intent_kind_state_counts"])
        payload["intents"]["intent_count"] = int(sum(intent_state_counts.values()))

        attempt_status_counts = dict(
            sorted((str(state), int(count)) for state, count in as_mapping(payload["attempts"]["attempt_status_counts"]).items())
        )
        payload["attempts"]["attempt_status_counts"] = attempt_status_counts
        payload["attempts"]["attempt_intent_status_counts"] = _sort_nested_counts(payload["attempts"]["attempt_intent_status_counts"])
        payload["attempts"]["attempt_count"] = int(sum(attempt_status_counts.values()))

        position_status_counts = dict(
            sorted((str(state), int(count)) for state, count in as_mapping(payload["positions"]["position_status_counts"]).items())
        )
        payload["positions"]["position_status_counts"] = position_status_counts
        payload["positions"]["position_count"] = int(sum(position_status_counts.values()))
        payload["positions"]["open_position_count"] = int(
            sum(count for state, count in position_status_counts.items() if state in OPEN_POSITION_STATUSES)
        )
        payload["positions"]["closed_position_count"] = int(position_status_counts.get("closed", 0))
        payload["closes"]["close_decision_state_counts"] = dict(
            sorted((str(state), int(count)) for state, count in as_mapping(payload["closes"]["close_decision_state_counts"]).items())
        )
        payload["closes"]["close_decision_reason_counts"] = dict(
            sorted((str(reason), int(count)) for reason, count in as_mapping(payload["closes"]["close_decision_reason_counts"]).items())
        )

        realized_pnl = money_float(coerce_float(payload["pnl"]["realized_pnl"]) or 0.0) or 0.0
        unrealized_pnl = money_float(coerce_float(payload["pnl"]["unrealized_pnl"]) or 0.0) or 0.0
        payload["pnl"] = {
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "net_pnl": money_sum_float([realized_pnl, unrealized_pnl]),
        }
        payload["latest_activity_at"] = utc_iso(latest_activity.get(strategy_id))
    return payloads

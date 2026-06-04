from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.execution_intents.shared import OPEN_POSITION_STATES
from core.services.trading_strategies import TradingStrategyConfig
from core.storage.serializers import parse_datetime


def _window_bounds(market_date: str | None) -> tuple[str, datetime, datetime]:
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    window_start = datetime.fromisoformat(resolved_market_date).replace(tzinfo=UTC)
    window_end = window_start + timedelta(days=1)
    return resolved_market_date, window_start, window_end


def _in_window(value: Any, *, start: datetime, end: datetime) -> bool:
    parsed = parse_datetime(value)
    return parsed is not None and start <= parsed < end


def _as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def build_trading_strategy_metrics(
    *,
    storage: Any,
    trading_strategy_id: str,
    market_date: str | None = None,
) -> dict[str, Any]:
    resolved_market_date, window_start, window_end = _window_bounds(market_date)
    signal_store = storage.signals
    execution_store = storage.execution

    decisions: list[dict[str, Any]] = []
    if signal_store.decision_schema_ready():
        decisions = [
            dict(row)
            for row in signal_store.list_opportunity_decisions(
                trading_strategy_id=trading_strategy_id,
                limit=1000,
            )
            if _in_window(row.get("decided_at"), start=window_start, end=window_end)
        ]
    decision_state_counts = Counter(str(row.get("state") or "unknown") for row in decisions)

    intents: list[dict[str, Any]] = []
    if execution_store.intent_schema_ready():
        intents = [
            dict(row)
            for row in execution_store.list_execution_intents(
                trading_strategy_id=trading_strategy_id,
                limit=1000,
            )
            if _in_window(row.get("created_at"), start=window_start, end=window_end)
        ]
    intent_state_counts = Counter(str(row.get("state") or "unknown") for row in intents)
    entry_intents = [row for row in intents if str(row.get("action_type") or "") == "open"]
    close_intents = [row for row in intents if str(row.get("action_type") or "") == "close"]

    positions: list[dict[str, Any]] = []
    if execution_store.portfolio_schema_ready():
        positions = [
            dict(row)
            for row in execution_store.list_positions(
                trading_strategy_id=trading_strategy_id,
                limit=1000,
            )
        ]
    open_positions = [row for row in positions if str(row.get("status") or "") in OPEN_POSITION_STATES]
    daily_positions = [
        row for row in positions if row.get("market_date_opened") == resolved_market_date or row.get("market_date_closed") == resolved_market_date
    ]
    daily_realized_pnl = sum(_as_float(row.get("realized_pnl")) for row in daily_positions)
    open_unrealized_pnl = sum(_as_float(row.get("unrealized_pnl")) for row in open_positions)

    return {
        "trading_strategy_id": trading_strategy_id,
        "market_date": resolved_market_date,
        "decision_count": len(decisions),
        "decision_state_counts": dict(sorted(decision_state_counts.items())),
        "intent_count": len(intents),
        "intent_state_counts": dict(sorted(intent_state_counts.items())),
        "entry_intent_count": len(entry_intents),
        "close_intent_count": len(close_intents),
        "daily_action_count": sum(1 for row in intents if str(row.get("state") or "") not in {"revoked", "expired"}),
        "daily_entry_fill_count": sum(1 for row in entry_intents if str(row.get("state") or "") == "filled"),
        "daily_close_fill_count": sum(1 for row in close_intents if str(row.get("state") or "") == "filled"),
        "position_count": len(positions),
        "open_position_count": len(open_positions),
        "daily_realized_pnl": round(daily_realized_pnl, 2),
        "open_unrealized_pnl": round(open_unrealized_pnl, 2),
        "daily_total_pnl": round(daily_realized_pnl + open_unrealized_pnl, 2),
    }


def evaluate_trading_strategy_entry_controls(
    *,
    storage: Any,
    strategy: TradingStrategyConfig,
    market_date: str | None = None,
) -> tuple[bool, str | None, dict[str, Any]]:
    metrics = build_trading_strategy_metrics(
        storage=storage,
        trading_strategy_id=strategy.trading_strategy_id,
        market_date=market_date,
    )
    if strategy.max_open_positions and int(metrics.get("open_position_count") or 0) >= strategy.max_open_positions:
        return False, "max_open_positions_reached", metrics
    if strategy.max_daily_actions and int(metrics.get("daily_action_count") or 0) >= strategy.max_daily_actions:
        return False, "max_daily_actions_reached", metrics
    if strategy.max_new_entries_per_day is not None and int(metrics.get("daily_entry_fill_count") or 0) >= strategy.max_new_entries_per_day:
        return False, "max_new_entries_per_day_reached", metrics
    if strategy.daily_loss_limit is not None and float(metrics.get("daily_total_pnl") or 0.0) <= -abs(float(strategy.daily_loss_limit)):
        return False, "daily_loss_limit_reached", metrics
    return True, None, metrics


__all__ = [
    "build_trading_strategy_metrics",
    "evaluate_trading_strategy_entry_controls",
]

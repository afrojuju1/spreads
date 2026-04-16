from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from spreads.services.bots import BotConfig, load_active_bots
from spreads.services.positions import enrich_position_row
from spreads.storage.execution_models import (
    ExecutionIntentModel,
    PortfolioPositionModel,
)
from spreads.storage.signal_models import OpportunityDecisionModel

OPEN_POSITION_STATUSES = {"open", "partial_open", "partial_close"}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _window_bounds(market_date: str | None) -> tuple[str, date, datetime, datetime]:
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    market_day = date.fromisoformat(resolved_market_date)
    window_start = datetime.fromisoformat(resolved_market_date).replace(tzinfo=UTC)
    window_end = window_start + timedelta(days=1)
    return resolved_market_date, market_day, window_start, window_end


def _bot_owned_positions(execution_store: Any, bot_id: str) -> list[dict[str, Any]]:
    if (
        not execution_store.portfolio_schema_ready()
        or not execution_store.intent_schema_ready()
    ):
        return []
    positions = [
        enrich_position_row(dict(row))
        for row in execution_store.list_positions(limit=1000)
    ]
    owned: list[dict[str, Any]] = []
    for position in positions:
        open_execution_attempt_id = _as_text(position.get("open_execution_attempt_id"))
        if open_execution_attempt_id is None:
            continue
        attempt = execution_store.get_attempt(open_execution_attempt_id)
        if attempt is None:
            continue
        request = (
            attempt.get("request")
            if isinstance(attempt.get("request"), Mapping)
            else {}
        )
        execution_intent_id = _as_text(request.get("execution_intent_id"))
        if execution_intent_id is None:
            continue
        intent = execution_store.get_execution_intent(execution_intent_id)
        if intent is None or str(intent.get("bot_id") or "") != bot_id:
            continue
        owned.append(position)
    return owned


def build_bot_metrics(
    *,
    storage: Any,
    bot_id: str,
    market_date: str | None = None,
) -> dict[str, Any]:
    resolved_market_date, market_day, window_start, window_end = _window_bounds(
        market_date
    )
    signal_store = storage.signals
    execution_store = storage.execution

    decision_state_counts: Counter[str] = Counter()
    if signal_store.schema_ready():
        with signal_store.session_factory() as session:
            rows = session.execute(
                select(OpportunityDecisionModel.state, func.count())
                .where(OpportunityDecisionModel.bot_id == bot_id)
                .where(OpportunityDecisionModel.decided_at >= window_start)
                .where(OpportunityDecisionModel.decided_at < window_end)
                .group_by(OpportunityDecisionModel.state)
            ).all()
            decision_state_counts.update(
                {str(state): int(count) for state, count in rows}
            )

    intent_state_counts: Counter[str] = Counter()
    daily_action_count = 0
    daily_entry_fill_count = 0
    daily_close_fill_count = 0
    if execution_store.intent_schema_ready():
        with execution_store.session_factory() as session:
            rows = session.execute(
                select(ExecutionIntentModel.state, func.count())
                .where(ExecutionIntentModel.bot_id == bot_id)
                .where(ExecutionIntentModel.created_at >= window_start)
                .where(ExecutionIntentModel.created_at < window_end)
                .group_by(ExecutionIntentModel.state)
            ).all()
            intent_state_counts.update(
                {str(state): int(count) for state, count in rows}
            )
            daily_action_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                    .where(ExecutionIntentModel.state.notin_(["revoked", "expired"]))
                )
                or 0
            )
            daily_entry_fill_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.action_type == "open")
                    .where(ExecutionIntentModel.state == "filled")
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                )
                or 0
            )
            daily_close_fill_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.action_type == "close")
                    .where(ExecutionIntentModel.state == "filled")
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                )
                or 0
            )

    positions = _bot_owned_positions(execution_store, bot_id)
    open_positions = [
        p for p in positions if str(p.get("status") or "") in OPEN_POSITION_STATUSES
    ]
    closed_positions = [
        p for p in positions if str(p.get("status") or "") not in OPEN_POSITION_STATUSES
    ]

    total_realized_pnl = sum(_coerce_float(p.get("realized_pnl")) for p in positions)
    open_unrealized_pnl = sum(
        _coerce_float(p.get("unrealized_pnl")) for p in open_positions
    )
    daily_realized_pnl = sum(
        _coerce_float(p.get("realized_pnl"))
        for p in positions
        if p.get("market_date_opened") == resolved_market_date
        or p.get("market_date_closed") == resolved_market_date
    )
    daily_total_pnl = daily_realized_pnl + open_unrealized_pnl

    closed_win_count = sum(
        1 for p in closed_positions if _coerce_float(p.get("realized_pnl")) > 0
    )
    closed_loss_count = sum(
        1 for p in closed_positions if _coerce_float(p.get("realized_pnl")) < 0
    )
    closed_decision_count = closed_win_count + closed_loss_count
    closed_win_rate = (
        None
        if closed_decision_count == 0
        else closed_win_count / float(closed_decision_count)
    )

    symbol_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "open_positions": 0,
            "closed_positions": 0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 0.0,
        }
    )
    for position in positions:
        symbol = str(
            position.get("underlying_symbol")
            or position.get("root_symbol")
            or "unknown"
        )
        if str(position.get("status") or "") in OPEN_POSITION_STATUSES:
            symbol_stats[symbol]["open_positions"] += 1
            symbol_stats[symbol]["unrealized_pnl"] += _coerce_float(
                position.get("unrealized_pnl")
            )
        else:
            symbol_stats[symbol]["closed_positions"] += 1
        symbol_stats[symbol]["realized_pnl"] += _coerce_float(
            position.get("realized_pnl")
        )
        symbol_stats[symbol]["net_pnl"] = (
            symbol_stats[symbol]["realized_pnl"]
            + symbol_stats[symbol]["unrealized_pnl"]
        )

    return {
        "bot_id": bot_id,
        "market_date": resolved_market_date,
        "decision_count": int(sum(decision_state_counts.values())),
        "decision_state_counts": dict(sorted(decision_state_counts.items())),
        "intent_count": int(sum(intent_state_counts.values())),
        "intent_state_counts": dict(sorted(intent_state_counts.items())),
        "daily_action_count": daily_action_count,
        "daily_entry_fill_count": daily_entry_fill_count,
        "daily_close_fill_count": daily_close_fill_count,
        "position_count": len(positions),
        "open_position_count": len(open_positions),
        "closed_position_count": len(closed_positions),
        "daily_realized_pnl": round(daily_realized_pnl, 2),
        "open_unrealized_pnl": round(open_unrealized_pnl, 2),
        "daily_total_pnl": round(daily_total_pnl, 2),
        "total_realized_pnl": round(total_realized_pnl, 2),
        "net_total_pnl": round(total_realized_pnl + open_unrealized_pnl, 2),
        "closed_win_count": closed_win_count,
        "closed_loss_count": closed_loss_count,
        "closed_win_rate": closed_win_rate,
        "symbol_stats": dict(sorted(symbol_stats.items())),
    }


def build_automation_performance_summary(
    *,
    storage: Any,
    market_date: str | None = None,
) -> dict[str, Any]:
    bots = load_active_bots()
    bot_rows = [
        {
            "bot_id": bot.bot.bot_id,
            "bot_name": bot.bot.name,
            **build_bot_metrics(
                storage=storage, bot_id=bot.bot.bot_id, market_date=market_date
            ),
        }
        for bot in bots.values()
    ]
    return {
        "bot_count": len(bot_rows),
        "daily_total_pnl": round(
            sum(_coerce_float(row.get("daily_total_pnl")) for row in bot_rows), 2
        ),
        "open_unrealized_pnl": round(
            sum(_coerce_float(row.get("open_unrealized_pnl")) for row in bot_rows), 2
        ),
        "total_realized_pnl": round(
            sum(_coerce_float(row.get("total_realized_pnl")) for row in bot_rows), 2
        ),
        "daily_entry_fill_count": int(
            sum(int(row.get("daily_entry_fill_count") or 0) for row in bot_rows)
        ),
        "daily_close_fill_count": int(
            sum(int(row.get("daily_close_fill_count") or 0) for row in bot_rows)
        ),
        "bots": bot_rows,
    }


def evaluate_entry_controls(
    *,
    storage: Any,
    bot: BotConfig,
    market_date: str | None = None,
) -> tuple[bool, str | None, dict[str, Any]]:
    metrics = build_bot_metrics(
        storage=storage, bot_id=bot.bot_id, market_date=market_date
    )
    if (
        bot.max_open_positions
        and int(metrics.get("open_position_count") or 0) >= bot.max_open_positions
    ):
        return False, "max_open_positions_reached", metrics
    if (
        bot.max_daily_actions
        and int(metrics.get("daily_action_count") or 0) >= bot.max_daily_actions
    ):
        return False, "max_daily_actions_reached", metrics
    if (
        bot.max_new_entries_per_day is not None
        and int(metrics.get("daily_entry_fill_count") or 0)
        >= bot.max_new_entries_per_day
    ):
        return False, "max_new_entries_per_day_reached", metrics
    if bot.daily_loss_limit is not None and float(
        metrics.get("daily_total_pnl") or 0.0
    ) <= -abs(float(bot.daily_loss_limit)):
        return False, "daily_loss_limit_reached", metrics
    return True, None, metrics


__all__ = [
    "build_automation_performance_summary",
    "build_bot_metrics",
    "evaluate_entry_controls",
]

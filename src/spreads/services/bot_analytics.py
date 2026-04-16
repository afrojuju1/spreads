from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from spreads.services.bots import BotConfig, load_active_bots
from spreads.services.positions import enrich_position_row
from spreads.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionIntentModel,
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


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / float(len(values)), 2)


def _strategy_name_from_payload(
    *,
    policy_ref: Mapping[str, Any] | None,
    payload: Mapping[str, Any] | None,
) -> str:
    if isinstance(policy_ref, Mapping):
        strategy_id = _as_text(policy_ref.get("strategy_id"))
        if strategy_id is not None:
            return strategy_id
    if isinstance(payload, Mapping):
        opportunity = payload.get("opportunity")
        if isinstance(opportunity, Mapping):
            strategy_family = _as_text(opportunity.get("strategy_family"))
            if strategy_family is not None:
                return strategy_family
    return "unknown"


def _increment_counts(target: dict[str, int], key: str, amount: int = 1) -> None:
    target[key] = int(target.get(key) or 0) + int(amount)


def _funnel_row(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "considered": 0,
        "selected": 0,
        "rejected": 0,
        "blocked": 0,
        "intents_created": 0,
        "submitted": 0,
        "repriced": 0,
        "canceled": 0,
        "failed": 0,
        "filled": 0,
        "blocker_reasons": {},
        "avg_decision_to_intent_seconds": None,
        "avg_intent_to_submit_seconds": None,
        "avg_submit_to_fill_seconds": None,
    }


def _finalize_funnel(
    row: dict[str, Any], *, timings: dict[str, list[float]]
) -> dict[str, Any]:
    considered = int(row.get("considered") or 0)
    selected = int(row.get("selected") or 0)
    intents_created = int(row.get("intents_created") or 0)
    filled = int(row.get("filled") or 0)
    row["selection_rate"] = (
        None if considered <= 0 else round(selected / float(considered), 4)
    )
    row["intent_rate"] = (
        None if selected <= 0 else round(intents_created / float(selected), 4)
    )
    row["fill_rate"] = (
        None if intents_created <= 0 else round(filled / float(intents_created), 4)
    )
    row["avg_decision_to_intent_seconds"] = _average(
        timings.get("decision_to_intent") or []
    )
    row["avg_intent_to_submit_seconds"] = _average(
        timings.get("intent_to_submit") or []
    )
    row["avg_submit_to_fill_seconds"] = _average(timings.get("submit_to_fill") or [])
    row["blocker_reasons"] = dict(sorted((row.get("blocker_reasons") or {}).items()))
    return row


def _build_entry_funnel(
    *,
    signal_store: Any,
    execution_store: Any,
    bot_id: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    overall = _funnel_row("overall")
    overall_timings: dict[str, list[float]] = defaultdict(list)
    strategy_rows: dict[str, dict[str, Any]] = {}
    strategy_timings: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    decisions_by_id: dict[str, OpportunityDecisionModel] = {}
    if signal_store.schema_ready():
        with signal_store.session_factory() as session:
            decisions = list(
                session.scalars(
                    select(OpportunityDecisionModel)
                    .where(OpportunityDecisionModel.bot_id == bot_id)
                    .where(OpportunityDecisionModel.decided_at >= window_start)
                    .where(OpportunityDecisionModel.decided_at < window_end)
                ).all()
            )
        for decision in decisions:
            strategy_name = _strategy_name_from_payload(
                policy_ref=decision.policy_ref_json,
                payload=decision.payload_json,
            )
            decisions_by_id[str(decision.opportunity_decision_id)] = decision
            row = strategy_rows.setdefault(strategy_name, _funnel_row(strategy_name))
            state = str(decision.state or "")
            _increment_counts(overall, "considered")
            _increment_counts(row, "considered")
            if state in {"selected", "rejected", "blocked"}:
                _increment_counts(overall, state)
                _increment_counts(row, state)
            for reason in list(decision.reason_codes_json or []):
                if state != "selected":
                    _increment_counts(overall["blocker_reasons"], str(reason))
                    _increment_counts(row["blocker_reasons"], str(reason))

    if execution_store.intent_schema_ready():
        with execution_store.session_factory() as session:
            intents = list(
                session.scalars(
                    select(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                    .where(ExecutionIntentModel.action_type == "open")
                ).all()
            )
            attempts = {
                str(row.execution_attempt_id): row
                for row in session.scalars(
                    select(ExecutionAttemptModel).where(
                        ExecutionAttemptModel.execution_attempt_id.in_(
                            [
                                intent.execution_attempt_id
                                for intent in intents
                                if intent.execution_attempt_id is not None
                            ]
                        )
                    )
                ).all()
            }
        for intent in intents:
            strategy_name = _strategy_name_from_payload(
                policy_ref=intent.policy_ref_json,
                payload=intent.payload_json,
            )
            row = strategy_rows.setdefault(strategy_name, _funnel_row(strategy_name))
            _increment_counts(overall, "intents_created")
            _increment_counts(row, "intents_created")

            state = str(intent.state or "")
            if state in {"submitted", "filled", "canceled", "failed"}:
                _increment_counts(overall, "submitted")
                _increment_counts(row, "submitted")
            if state in {"canceled"}:
                _increment_counts(overall, "canceled")
                _increment_counts(row, "canceled")
            if state in {"failed"}:
                _increment_counts(overall, "failed")
                _increment_counts(row, "failed")
            if state == "filled":
                _increment_counts(overall, "filled")
                _increment_counts(row, "filled")
            reprice_count = int((intent.payload_json or {}).get("reprice_count") or 0)
            if reprice_count > 0:
                _increment_counts(overall, "repriced")
                _increment_counts(row, "repriced")
            payload = dict(intent.payload_json or {})
            for key in ("revoke_reason", "error"):
                reason = _as_text(payload.get(key))
                if reason:
                    _increment_counts(overall["blocker_reasons"], reason)
                    _increment_counts(row["blocker_reasons"], reason)

            decision = None
            if intent.opportunity_decision_id is not None:
                decision = decisions_by_id.get(str(intent.opportunity_decision_id))
            if decision is not None:
                overall_timings["decision_to_intent"].append(
                    max((intent.created_at - decision.decided_at).total_seconds(), 0.0)
                )
                strategy_timings[strategy_name]["decision_to_intent"].append(
                    max((intent.created_at - decision.decided_at).total_seconds(), 0.0)
                )
            attempt = (
                None
                if intent.execution_attempt_id is None
                else attempts.get(str(intent.execution_attempt_id))
            )
            if attempt is not None and attempt.submitted_at is not None:
                overall_timings["intent_to_submit"].append(
                    max((attempt.submitted_at - intent.created_at).total_seconds(), 0.0)
                )
                strategy_timings[strategy_name]["intent_to_submit"].append(
                    max((attempt.submitted_at - intent.created_at).total_seconds(), 0.0)
                )
            if (
                attempt is not None
                and attempt.submitted_at is not None
                and attempt.completed_at is not None
                and str(attempt.status or "") == "filled"
            ):
                overall_timings["submit_to_fill"].append(
                    max(
                        (attempt.completed_at - attempt.submitted_at).total_seconds(),
                        0.0,
                    )
                )
                strategy_timings[strategy_name]["submit_to_fill"].append(
                    max(
                        (attempt.completed_at - attempt.submitted_at).total_seconds(),
                        0.0,
                    )
                )

    finalized_strategies = [
        _finalize_funnel(row, timings=strategy_timings.get(name, {}))
        for name, row in sorted(strategy_rows.items())
    ]
    return {
        "overall": _finalize_funnel(overall, timings=overall_timings),
        "strategies": finalized_strategies,
    }


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

    entry_funnel = _build_entry_funnel(
        signal_store=signal_store,
        execution_store=execution_store,
        bot_id=bot_id,
        window_start=window_start,
        window_end=window_end,
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
        "entry_funnel": entry_funnel,
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
        "entry_funnel": {
            "overall": _finalize_funnel(
                {
                    **_funnel_row("overall"),
                    **{
                        key: sum(
                            int(
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get(key)
                                or 0
                            )
                            for row in bot_rows
                        )
                        for key in [
                            "considered",
                            "selected",
                            "rejected",
                            "blocked",
                            "intents_created",
                            "submitted",
                            "repriced",
                            "canceled",
                            "failed",
                            "filled",
                        ]
                    },
                    "blocker_reasons": dict(
                        Counter(
                            reason
                            for row in bot_rows
                            for reason, count in (
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("blocker_reasons")
                                or {}
                            ).items()
                            for _ in range(int(count))
                        )
                    ),
                },
                timings={
                    "decision_to_intent": [
                        float(value)
                        for row in bot_rows
                        for value in (
                            [
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("avg_decision_to_intent_seconds")
                            ]
                            if (
                                (row.get("entry_funnel") or {}).get("overall") or {}
                            ).get("avg_decision_to_intent_seconds")
                            is not None
                            else []
                        )
                    ],
                    "intent_to_submit": [
                        float(value)
                        for row in bot_rows
                        for value in (
                            [
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("avg_intent_to_submit_seconds")
                            ]
                            if (
                                (row.get("entry_funnel") or {}).get("overall") or {}
                            ).get("avg_intent_to_submit_seconds")
                            is not None
                            else []
                        )
                    ],
                    "submit_to_fill": [
                        float(value)
                        for row in bot_rows
                        for value in (
                            [
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("avg_submit_to_fill_seconds")
                            ]
                            if (
                                (row.get("entry_funnel") or {}).get("overall") or {}
                            ).get("avg_submit_to_fill_seconds")
                            is not None
                            else []
                        )
                    ],
                },
            ),
            "bots": [
                {
                    "bot_id": row.get("bot_id"),
                    "bot_name": row.get("bot_name"),
                    **dict(row.get("entry_funnel") or {}),
                }
                for row in bot_rows
            ],
        },
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

from __future__ import annotations

from typing import Any

from core.db.decorators import with_storage
from core.services.automation_runtime import resolve_entry_runtime
from core.services.bot_analytics import evaluate_entry_controls
from core.services.entry_planner import plan_entry_selection, score_opportunity
from core.services.management_recipes import build_exit_policy_from_recipe_refs


def _scope_key(bot_id: str, automation_id: str, session_date: str) -> str:
    return f"entry:{bot_id}:{automation_id}:{session_date}"


def _coerce_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sorted_opportunities(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = [dict(row) for row in rows]
    sorted_rows.sort(
        key=lambda row: (
            -score_opportunity(row),
            int(row.get("selection_rank") or 999999),
            str(row.get("opportunity_id") or ""),
        )
    )
    return sorted_rows


def _opportunity_economics(opportunity: dict[str, Any]) -> dict[str, Any]:
    economics = opportunity.get("economics")
    return dict(economics) if isinstance(economics, dict) else dict(opportunity)


def _simulate_entry_execution(
    *,
    runtime: Any,
    opportunity: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if opportunity is None:
        return None
    economics = _opportunity_economics(opportunity)
    midpoint_credit = _coerce_float(economics.get("midpoint_credit"))
    natural_credit = _coerce_float(economics.get("natural_credit"))
    fill_ratio = _coerce_float(economics.get("fill_ratio"))
    width = _coerce_float(economics.get("width") or opportunity.get("width"))
    if midpoint_credit <= 0 and natural_credit <= 0:
        return {
            "intent_state": "rejected",
            "fill_state": "no_price",
            "filled": False,
            "position": None,
        }
    if fill_ratio >= 0.7 and midpoint_credit > 0:
        fill_price = midpoint_credit
        fill_source = "midpoint"
        fill_state = "filled"
    elif fill_ratio >= 0.5 and natural_credit > 0:
        fill_price = natural_credit
        fill_source = "natural"
        fill_state = "filled"
    else:
        fill_price = 0.0
        fill_source = None
        fill_state = "unfilled"
    if fill_state != "filled":
        return {
            "intent_state": "submitted",
            "fill_state": fill_state,
            "filled": False,
            "position": None,
        }
    max_loss = _coerce_float(economics.get("max_loss"))
    return {
        "intent_state": "filled",
        "fill_state": fill_state,
        "filled": True,
        "fill_price": round(fill_price, 4),
        "fill_source": fill_source,
        "position": {
            "underlying_symbol": opportunity.get("underlying_symbol"),
            "strategy_family": opportunity.get("strategy_family"),
            "entry_credit": round(fill_price, 4),
            "width": None if width <= 0 else width,
            "max_loss": None if max_loss <= 0 else round(max_loss, 2),
            "exit_policy": build_exit_policy_from_recipe_refs(
                tuple(runtime.automation.strategy_config.management_recipe_refs)
            ),
        },
    }


def compare_bootstrap_backtests(
    *,
    left_payload: dict[str, Any],
    right_payload: dict[str, Any],
) -> dict[str, Any]:
    left_target = dict(left_payload.get("target") or {})
    right_target = dict(right_payload.get("target") or {})
    left_aggregate = dict(left_payload.get("aggregate") or {})
    right_aggregate = dict(right_payload.get("aggregate") or {})
    metric_keys = [
        "session_count",
        "modeled_selected_count",
        "modeled_fill_count",
        "modeled_position_count",
        "actual_selected_count",
        "matched_selection_count",
        "selection_match_rate",
        "modeled_fill_rate",
        "actual_fill_rate",
        "position_count",
        "realized_pnl",
        "unrealized_pnl",
    ]
    metrics: dict[str, dict[str, Any]] = {}
    for key in metric_keys:
        left_value = left_aggregate.get(key)
        right_value = right_aggregate.get(key)
        metrics[key] = {
            "left": left_value,
            "right": right_value,
            "delta": (
                None
                if left_value is None or right_value is None
                else round(_coerce_float(left_value) - _coerce_float(right_value), 4)
            ),
        }
    return {
        "left": left_target,
        "right": right_target,
        "metrics": metrics,
    }


def _latest_runs_by_session(
    runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_session: dict[str, dict[str, Any]] = {}
    for row in runs:
        session_date = str(row.get("session_date") or "")
        if not session_date:
            continue
        current = latest_by_session.get(session_date)
        if current is None or str(row.get("started_at") or "") > str(
            current.get("started_at") or ""
        ):
            latest_by_session[session_date] = dict(row)
    return [latest_by_session[key] for key in sorted(latest_by_session)]


@with_storage()
def build_bootstrap_backtest(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 200,
    storage: Any | None = None,
) -> dict[str, Any]:
    runtime = resolve_entry_runtime(bot_id=bot_id, automation_id=automation_id)
    signal_store = storage.signals
    execution_store = storage.execution

    runs = _latest_runs_by_session(
        [
            dict(row)
            for row in signal_store.list_automation_runs(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                start_date=start_date,
                end_date=end_date,
                limit=max(int(limit), 1) * 4,
            )
        ]
    )[: max(int(limit), 1)]

    sessions: list[dict[str, Any]] = []
    matched_selection_count = 0
    modeled_selected_count = 0
    actual_selected_count = 0
    modeled_fill_count = 0
    modeled_position_count = 0
    actual_fill_count = 0
    total_position_count = 0
    total_realized_pnl = 0.0
    total_unrealized_pnl = 0.0

    for run in runs:
        session_date = str(run["session_date"])
        opportunities = _sorted_opportunities(
            [
                dict(row)
                for row in signal_store.list_opportunities(
                    bot_id=runtime.bot_id,
                    automation_id=runtime.automation_id,
                    automation_run_id=str(run["automation_run_id"]),
                    runtime_owned=True,
                    limit=500,
                )
            ]
        )
        controls_allowed, controls_reason, bot_metrics = evaluate_entry_controls(
            storage=storage,
            bot=runtime.bot.bot,
            market_date=session_date,
        )
        plan = plan_entry_selection(
            opportunities=opportunities,
            controls_allowed=controls_allowed,
            controls_reason=controls_reason,
            bot_metrics=bot_metrics,
            min_score=float(runtime.trigger_policy.get("min_opportunity_score") or 0.0),
        )
        modeled_selected = plan.get("selected")
        modeled_selected_id = (
            None
            if modeled_selected is None
            else str(modeled_selected.get("opportunity_id"))
        )
        if modeled_selected_id is not None:
            modeled_selected_count += 1
        modeled_execution = _simulate_entry_execution(
            runtime=runtime,
            opportunity=modeled_selected,
        )
        if modeled_execution and modeled_execution.get("filled"):
            modeled_fill_count += 1
            modeled_position_count += 1

        actual_decisions = [
            dict(row)
            for row in signal_store.list_opportunity_decisions(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                scope_key=_scope_key(
                    runtime.bot_id, runtime.automation_id, session_date
                ),
                limit=500,
            )
        ]
        actual_selected_decision = next(
            (
                row
                for row in actual_decisions
                if str(row.get("state") or "") == "selected"
            ),
            None,
        )
        actual_selected_id = (
            None
            if actual_selected_decision is None
            else str(actual_selected_decision.get("opportunity_id"))
        )
        if actual_selected_id is not None:
            actual_selected_count += 1
        if (
            modeled_selected_id is not None
            and modeled_selected_id == actual_selected_id
        ):
            matched_selection_count += 1

        selected_intents = []
        if actual_selected_decision is not None:
            selected_intents = [
                dict(row)
                for row in execution_store.list_execution_intents(
                    bot_id=runtime.bot_id,
                    automation_id=runtime.automation_id,
                    opportunity_decision_id=str(
                        actual_selected_decision["opportunity_decision_id"]
                    ),
                    limit=50,
                )
            ]
        if selected_intents:
            actual_fill_count += len(
                [
                    row
                    for row in selected_intents
                    if str(row.get("state") or "")
                    in {"submitted", "filled", "completed", "partially_filled"}
                ]
            )

        positions = [
            dict(row)
            for row in execution_store.list_positions(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                market_date=session_date,
                limit=200,
            )
        ]
        realized_pnl = sum(_coerce_float(row.get("realized_pnl")) for row in positions)
        unrealized_pnl = sum(
            _coerce_float(row.get("unrealized_pnl")) for row in positions
        )
        total_position_count += len(positions)
        total_realized_pnl += realized_pnl
        total_unrealized_pnl += unrealized_pnl

        sessions.append(
            {
                "session_date": session_date,
                "automation_run_id": str(run["automation_run_id"]),
                "opportunity_count": len(opportunities),
                "modeled_selected_opportunity_id": modeled_selected_id,
                "actual_selected_opportunity_id": actual_selected_id,
                "selection_match": modeled_selected_id == actual_selected_id
                if modeled_selected_id is not None and actual_selected_id is not None
                else None,
                "controls_allowed": controls_allowed,
                "controls_reason": controls_reason,
                "modeled_intent_state": None
                if modeled_execution is None
                else modeled_execution.get("intent_state"),
                "modeled_fill_state": None
                if modeled_execution is None
                else modeled_execution.get("fill_state"),
                "modeled_fill_price": None
                if modeled_execution is None
                else modeled_execution.get("fill_price"),
                "modeled_position": None
                if modeled_execution is None
                else modeled_execution.get("position"),
                "selected_intent_count": len(selected_intents),
                "position_count": len(positions),
                "realized_pnl": round(realized_pnl, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "top_opportunities": [
                    {
                        "opportunity_id": str(row.get("opportunity_id")),
                        "underlying_symbol": row.get("underlying_symbol"),
                        "score": score_opportunity(row),
                        "selection_rank": row.get("selection_rank"),
                        "midpoint_credit": row.get("economics", {}).get(
                            "midpoint_credit"
                        )
                        if isinstance(row.get("economics"), dict)
                        else row.get("midpoint_credit"),
                    }
                    for row in opportunities[:5]
                ],
            }
        )

    return {
        "target": {
            "bot_id": runtime.bot_id,
            "automation_id": runtime.automation_id,
            "strategy_config_id": runtime.strategy_config_id,
            "strategy_id": runtime.strategy_id,
            "start_date": start_date,
            "end_date": end_date,
        },
        "aggregate": {
            "session_count": len(sessions),
            "modeled_selected_count": modeled_selected_count,
            "modeled_fill_count": modeled_fill_count,
            "modeled_position_count": modeled_position_count,
            "actual_selected_count": actual_selected_count,
            "matched_selection_count": matched_selection_count,
            "selection_match_rate": None
            if actual_selected_count == 0
            else round(matched_selection_count / actual_selected_count, 4),
            "modeled_fill_rate": None
            if modeled_selected_count == 0
            else round(modeled_fill_count / modeled_selected_count, 4),
            "actual_fill_rate": None
            if actual_selected_count == 0
            else round(actual_fill_count / actual_selected_count, 4),
            "position_count": total_position_count,
            "realized_pnl": round(total_realized_pnl, 2),
            "unrealized_pnl": round(total_unrealized_pnl, 2),
        },
        "sessions": sessions,
    }


__all__ = ["build_bootstrap_backtest", "compare_bootstrap_backtests"]

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from core.db.decorators import with_storage
from core.domain.backtest_models import (
    BacktestAggregate,
    BacktestRun,
    BacktestSessionSummary,
    BacktestTarget,
    new_backtest_run_id,
)
from core.services.automation_runtime import resolve_entry_runtime
from core.services.bot_analytics import evaluate_entry_controls
from core.services.entry_planner import plan_entry_selection, score_opportunity
from core.services.exit_manager import resolve_exit_policy_snapshot
from core.services.management_recipes import (
    build_exit_policy_from_recipe_refs,
    evaluate_management_recipes,
)
from core.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    structure_quote_snapshot,
    unique_leg_symbols,
)
from core.storage.run_history_repository import session_bounds
from core.storage.serializers import parse_datetime


ENGINE_NAME = "bootstrap_backtest"
ENGINE_VERSION = "v1"


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


def _lifecycle_pnl(
    *, strategy_family: Any, entry_value: float, exit_value: float
) -> float:
    if net_premium_kind(strategy_family) == "debit":
        return round((exit_value - entry_value) * 100.0, 2)
    return round((entry_value - exit_value) * 100.0, 2)


def _simulated_position_payload(
    *,
    runtime: Any,
    opportunity: dict[str, Any],
    fill_price: float,
    width: float | None,
    max_loss: float | None,
    session_date: str,
) -> dict[str, Any]:
    return {
        "underlying_symbol": opportunity.get("underlying_symbol"),
        "strategy_family": opportunity.get("strategy_family"),
        "strategy": opportunity.get("strategy_family"),
        "session_date": session_date,
        "market_date": session_date,
        "entry_credit": round(fill_price, 4),
        "entry_value": round(fill_price, 4),
        "entry_value_kind": net_premium_kind(opportunity.get("strategy_family")),
        "remaining_quantity": 1.0,
        "width": None if width is None or width <= 0 else width,
        "max_loss": None if max_loss is None or max_loss <= 0 else round(max_loss, 2),
        "risk_policy": {},
        "exit_policy": resolve_exit_policy_snapshot(
            session_date=session_date,
            payload={
                "exit_policy": build_exit_policy_from_recipe_refs(
                    tuple(runtime.automation.strategy_config.management_recipe_refs)
                )
            },
        ),
    }


def _simulate_entry_execution(
    *,
    runtime: Any,
    opportunity: dict[str, Any] | None,
    session_date: str,
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
        "position": _simulated_position_payload(
            runtime=runtime,
            opportunity=opportunity,
            fill_price=fill_price,
            width=None if width <= 0 else width,
            max_loss=None if max_loss <= 0 else max_loss,
            session_date=session_date,
        ),
    }


def _simulate_position_lifecycle(
    *,
    history_store: Any,
    runtime: Any,
    opportunity: dict[str, Any] | None,
    entry_execution: dict[str, Any] | None,
    started_at: str,
    session_date: str,
    end_date: str | None,
) -> dict[str, Any]:
    if (
        opportunity is None
        or entry_execution is None
        or not entry_execution.get("filled")
        or not isinstance(entry_execution.get("position"), dict)
    ):
        return {
            "position_state": "not_opened",
            "exit_state": "not_opened",
            "exit_reason": None,
            "exit_at": None,
            "exit_fill_price": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "final_close_mark": None,
            "quote_event_count": 0,
            "snapshot_count": 0,
        }
    if not hasattr(history_store, "list_option_quote_events_window"):
        return {
            "position_state": "open",
            "exit_state": "quote_history_unavailable",
            "exit_reason": "quote_history_unavailable",
            "exit_at": None,
            "exit_fill_price": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "final_close_mark": None,
            "quote_event_count": 0,
            "snapshot_count": 0,
        }
    if hasattr(history_store, "schema_ready") and not history_store.schema_ready():
        return {
            "position_state": "open",
            "exit_state": "quote_history_unavailable",
            "exit_reason": "quote_history_unavailable",
            "exit_at": None,
            "exit_fill_price": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "final_close_mark": None,
            "quote_event_count": 0,
            "snapshot_count": 0,
        }

    simulated_position = dict(entry_execution["position"])
    opening_legs = candidate_legs(opportunity)
    option_symbols = unique_leg_symbols(opening_legs)
    if not option_symbols:
        return {
            "position_state": "open",
            "exit_state": "missing_legs",
            "exit_reason": "missing_legs",
            "exit_at": None,
            "exit_fill_price": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "final_close_mark": None,
            "quote_event_count": 0,
            "snapshot_count": 0,
        }

    _, session_end = session_bounds(end_date or session_date)
    quote_rows = [
        dict(row)
        for row in history_store.list_option_quote_events_window(
            option_symbols=option_symbols,
            captured_from=started_at,
            captured_to=session_end,
        )
    ]

    quotes_by_symbol: dict[str, dict[str, Any]] = {}
    sources_by_symbol: dict[str, str] = {}
    latest_snapshot: dict[str, Any] | None = None
    snapshot_count = 0
    recipe_refs = tuple(runtime.automation.strategy_config.management_recipe_refs)
    for row in quote_rows:
        symbol = str(row.get("option_symbol") or "").strip()
        if not symbol:
            continue
        quotes_by_symbol[symbol] = {
            "bid": row.get("bid"),
            "ask": row.get("ask"),
            "midpoint": row.get("midpoint"),
            "captured_at": row.get("captured_at"),
            "timestamp": row.get("captured_at"),
        }
        if row.get("source") not in (None, ""):
            sources_by_symbol[symbol] = str(row.get("source"))
        snapshot = structure_quote_snapshot(
            legs=opening_legs,
            strategy_family=opportunity.get("strategy_family"),
            quotes_by_symbol=quotes_by_symbol,
            sources_by_symbol=sources_by_symbol,
        )
        if snapshot is None:
            continue
        latest_snapshot = snapshot
        snapshot_count += 1
        simulated_position["close_mark"] = snapshot.get("close_mark")
        simulated_position["close_marked_at"] = snapshot.get("captured_at")
        decision = evaluate_management_recipes(
            recipe_refs,
            position=simulated_position,
            mark=_coerce_float(snapshot.get("close_mark")),
            now=parse_datetime(snapshot.get("captured_at")) or session_end,
        )
        if decision.should_close:
            exit_fill_price = (
                decision.limit_price
                if decision.limit_price is not None
                else _coerce_float(snapshot.get("close_mark"))
            )
            return {
                "position_state": "closed",
                "exit_state": "closed",
                "exit_reason": decision.reason,
                "exit_recipe_ref": decision.recipe_ref,
                "exit_at": snapshot.get("captured_at"),
                "exit_fill_price": None
                if exit_fill_price <= 0
                else round(exit_fill_price, 4),
                "realized_pnl": _lifecycle_pnl(
                    strategy_family=opportunity.get("strategy_family"),
                    entry_value=float(entry_execution["fill_price"]),
                    exit_value=exit_fill_price,
                ),
                "unrealized_pnl": 0.0,
                "final_close_mark": snapshot.get("close_mark"),
                "quote_event_count": len(quote_rows),
                "snapshot_count": snapshot_count,
            }

    if latest_snapshot is None:
        return {
            "position_state": "open",
            "exit_state": "no_quotes",
            "exit_reason": "no_quotes",
            "exit_at": None,
            "exit_fill_price": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "final_close_mark": None,
            "quote_event_count": len(quote_rows),
            "snapshot_count": snapshot_count,
        }

    simulated_position["close_mark"] = latest_snapshot.get("close_mark")
    simulated_position["close_marked_at"] = latest_snapshot.get("captured_at")
    forced_now = (
        parse_datetime(simulated_position.get("exit_policy", {}).get("force_close_at"))
        or session_end
    )
    final_decision = evaluate_management_recipes(
        recipe_refs,
        position=simulated_position,
        mark=_coerce_float(latest_snapshot.get("close_mark")),
        now=forced_now,
    )
    if final_decision.should_close:
        exit_fill_price = (
            final_decision.limit_price
            if final_decision.limit_price is not None
            else _coerce_float(latest_snapshot.get("close_mark"))
        )
        return {
            "position_state": "closed",
            "exit_state": "closed",
            "exit_reason": final_decision.reason,
            "exit_recipe_ref": final_decision.recipe_ref,
            "exit_at": latest_snapshot.get("captured_at")
            if latest_snapshot.get("captured_at")
            else forced_now.isoformat(),
            "exit_fill_price": None
            if exit_fill_price <= 0
            else round(exit_fill_price, 4),
            "realized_pnl": _lifecycle_pnl(
                strategy_family=opportunity.get("strategy_family"),
                entry_value=float(entry_execution["fill_price"]),
                exit_value=exit_fill_price,
            ),
            "unrealized_pnl": 0.0,
            "final_close_mark": latest_snapshot.get("close_mark"),
            "quote_event_count": len(quote_rows),
            "snapshot_count": snapshot_count,
        }

    final_close_mark = _coerce_float(latest_snapshot.get("close_mark"))
    return {
        "position_state": "open",
        "exit_state": "open",
        "exit_reason": str(final_decision.reason),
        "exit_recipe_ref": final_decision.recipe_ref,
        "exit_at": latest_snapshot.get("captured_at"),
        "exit_fill_price": None,
        "realized_pnl": 0.0,
        "unrealized_pnl": _lifecycle_pnl(
            strategy_family=opportunity.get("strategy_family"),
            entry_value=float(entry_execution["fill_price"]),
            exit_value=final_close_mark,
        )
        if final_close_mark > 0
        else 0.0,
        "final_close_mark": latest_snapshot.get("close_mark"),
        "quote_event_count": len(quote_rows),
        "snapshot_count": snapshot_count,
    }


def compare_bootstrap_backtests(
    *,
    left_run: BacktestRun,
    right_run: BacktestRun,
) -> BacktestRun:
    left_target = BacktestTarget() if left_run.target is None else left_run.target
    right_target = BacktestTarget() if right_run.target is None else right_run.target
    left_aggregate = (
        {} if left_run.aggregate is None else left_run.aggregate.to_payload()
    )
    right_aggregate = (
        {} if right_run.aggregate is None else right_run.aggregate.to_payload()
    )
    metric_keys = [
        "session_count",
        "modeled_selected_count",
        "modeled_fill_count",
        "modeled_position_count",
        "modeled_closed_count",
        "modeled_open_position_count",
        "actual_selected_count",
        "matched_selection_count",
        "selection_match_rate",
        "modeled_fill_rate",
        "actual_fill_rate",
        "modeled_realized_pnl",
        "modeled_unrealized_pnl",
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
    started_at = datetime.now(UTC)
    return BacktestRun(
        id=new_backtest_run_id("compare"),
        kind="compare",
        status="completed",
        engine_name=ENGINE_NAME,
        engine_version=ENGINE_VERSION,
        created_at=started_at,
        started_at=started_at,
        completed_at=started_at,
        left_target=left_target,
        right_target=right_target,
        comparison_metrics=metrics,
        left_run_id=left_run.id,
        right_run_id=right_run.id,
    )


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
) -> BacktestRun:
    started_at = datetime.now(UTC)
    runtime = resolve_entry_runtime(bot_id=bot_id, automation_id=automation_id)
    signal_store = storage.signals
    execution_store = storage.execution
    history_store = storage.history

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

    sessions: list[BacktestSessionSummary] = []
    matched_selection_count = 0
    modeled_selected_count = 0
    actual_selected_count = 0
    modeled_fill_count = 0
    modeled_position_count = 0
    modeled_closed_count = 0
    modeled_open_position_count = 0
    actual_fill_count = 0
    total_position_count = 0
    total_modeled_realized_pnl = 0.0
    total_modeled_unrealized_pnl = 0.0
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
            session_date=session_date,
        )
        if modeled_execution and modeled_execution.get("filled"):
            modeled_fill_count += 1
            modeled_position_count += 1
        modeled_lifecycle = _simulate_position_lifecycle(
            history_store=history_store,
            runtime=runtime,
            opportunity=modeled_selected,
            entry_execution=modeled_execution,
            started_at=str(run.get("started_at") or session_date),
            session_date=session_date,
            end_date=end_date,
        )
        if str(modeled_lifecycle.get("position_state") or "") == "closed":
            modeled_closed_count += 1
        elif modeled_execution and modeled_execution.get("filled"):
            modeled_open_position_count += 1
        total_modeled_realized_pnl += _coerce_float(
            modeled_lifecycle.get("realized_pnl")
        )
        total_modeled_unrealized_pnl += _coerce_float(
            modeled_lifecycle.get("unrealized_pnl")
        )

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
            BacktestSessionSummary(
                session_date=session_date,
                automation_run_id=str(run["automation_run_id"]),
                opportunity_count=len(opportunities),
                modeled_selected_opportunity_id=modeled_selected_id,
                actual_selected_opportunity_id=actual_selected_id,
                selection_match=(
                    modeled_selected_id == actual_selected_id
                    if modeled_selected_id is not None
                    and actual_selected_id is not None
                    else None
                ),
                controls_allowed=controls_allowed,
                controls_reason=controls_reason,
                modeled_intent_state=(
                    None
                    if modeled_execution is None
                    else modeled_execution.get("intent_state")
                ),
                modeled_fill_state=(
                    None
                    if modeled_execution is None
                    else modeled_execution.get("fill_state")
                ),
                modeled_fill_price=(
                    None
                    if modeled_execution is None
                    else modeled_execution.get("fill_price")
                ),
                modeled_position=(
                    {}
                    if modeled_execution is None
                    else dict(modeled_execution.get("position") or {})
                ),
                modeled_exit_state=modeled_lifecycle.get("exit_state"),
                modeled_exit_reason=modeled_lifecycle.get("exit_reason"),
                modeled_exit_recipe_ref=modeled_lifecycle.get("exit_recipe_ref"),
                modeled_exit_at=modeled_lifecycle.get("exit_at"),
                modeled_exit_fill_price=modeled_lifecycle.get("exit_fill_price"),
                modeled_realized_pnl=modeled_lifecycle.get("realized_pnl"),
                modeled_unrealized_pnl=modeled_lifecycle.get("unrealized_pnl"),
                modeled_final_close_mark=modeled_lifecycle.get("final_close_mark"),
                modeled_quote_event_count=int(
                    modeled_lifecycle.get("quote_event_count") or 0
                ),
                modeled_snapshot_count=int(
                    modeled_lifecycle.get("snapshot_count") or 0
                ),
                selected_intent_count=len(selected_intents),
                position_count=len(positions),
                realized_pnl=round(realized_pnl, 2),
                unrealized_pnl=round(unrealized_pnl, 2),
                top_opportunities=[
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
            )
        )

    completed_at = datetime.now(UTC)
    return BacktestRun(
        id=new_backtest_run_id("bootstrap"),
        kind="bootstrap",
        status="completed",
        engine_name=ENGINE_NAME,
        engine_version=ENGINE_VERSION,
        created_at=started_at,
        started_at=started_at,
        completed_at=completed_at,
        target=BacktestTarget(
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            strategy_config_id=runtime.strategy_config_id,
            strategy_id=runtime.strategy_id,
            config_hash=runtime.config_hash,
            start_date=(None if start_date is None else date.fromisoformat(start_date)),
            end_date=(None if end_date is None else date.fromisoformat(end_date)),
            session_limit=max(int(limit), 1),
        ),
        aggregate=BacktestAggregate(
            session_count=len(sessions),
            modeled_selected_count=modeled_selected_count,
            modeled_fill_count=modeled_fill_count,
            modeled_position_count=modeled_position_count,
            modeled_closed_count=modeled_closed_count,
            modeled_open_position_count=modeled_open_position_count,
            actual_selected_count=actual_selected_count,
            matched_selection_count=matched_selection_count,
            selection_match_rate=(
                None
                if actual_selected_count == 0
                else round(matched_selection_count / actual_selected_count, 4)
            ),
            modeled_fill_rate=(
                None
                if modeled_selected_count == 0
                else round(modeled_fill_count / modeled_selected_count, 4)
            ),
            actual_fill_rate=(
                None
                if actual_selected_count == 0
                else round(actual_fill_count / actual_selected_count, 4)
            ),
            modeled_realized_pnl=round(total_modeled_realized_pnl, 2),
            modeled_unrealized_pnl=round(total_modeled_unrealized_pnl, 2),
            position_count=total_position_count,
            realized_pnl=round(total_realized_pnl, 2),
            unrealized_pnl=round(total_unrealized_pnl, 2),
        ),
        sessions=sessions,
        params={
            "start_date": start_date,
            "end_date": end_date,
            "limit": max(int(limit), 1),
        },
    )


__all__ = ["build_bootstrap_backtest", "compare_bootstrap_backtests"]

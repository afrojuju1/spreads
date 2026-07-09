from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.services.trading_engine.kernel import EngineComponentRole, EngineContext, EngineRunRef
from core.services.trading_engine.strategy import StrategyEntryRequest, StrategyEntryResult
from core.value_coercion import utc_now_iso as _utc_now


from core.services.trading_engine.strategy_runtime_entry import _run_trading_strategy_entry


class StrategyEngine:
    def __init__(self, context: EngineContext) -> None:
        self.context = context

    def run_entry(self, request: StrategyEntryRequest) -> StrategyEntryResult:
        summary = _run_trading_strategy_entry(
            db_target=self.context.db_target,
            trading_strategy_id=request.trading_strategy_id,
            market_date=request.market_date.isoformat(),
            planner_job_run_id=request.run_ref.job_run_id,
            run_key=request.run_ref.run_id,
            storage=self.context.storage,
        )
        candidate_generation = summary.get("candidate_generation") if isinstance(summary.get("candidate_generation"), dict) else {}
        engine_facts = candidate_generation.get("engine_facts") if isinstance(candidate_generation.get("engine_facts"), dict) else {}
        strategy_run = candidate_generation.get("strategy_run") if isinstance(candidate_generation.get("strategy_run"), dict) else {}
        decisions = [dict(row) for row in list(engine_facts.get("trade_decisions") or []) if isinstance(row, dict)]
        return StrategyEntryResult(
            run_ref=request.run_ref,
            strategy_run_id=str(strategy_run.get("strategy_run_id") or summary.get("run_key") or request.run_ref.run_id),
            trade_signal_ids=tuple(
                str(row["trade_signal_id"])
                for row in list(engine_facts.get("trade_signals") or [])
                if isinstance(row, dict) and row.get("trade_signal_id") not in (None, "")
            ),
            trade_decision_ids=tuple(str(value) for value in list(summary.get("trade_decision_ids") or []))
            or tuple(str(row["trade_decision_id"]) for row in decisions if row.get("trade_decision_id") not in (None, "")),
            selected_decision_ids=tuple(str(value) for value in list(summary.get("selected_decision_ids") or [])),
            status=str(summary.get("status") or "unknown"),
            reason=None if summary.get("reason") in (None, "") else str(summary["reason"]),
            summary=summary,
        )


@with_storage()
def run_trading_strategy_entry(
    *,
    db_target: str,
    trading_strategy_id: str,
    market_date: str | None = None,
    planner_job_run_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    resolved_market_date = datetime.fromisoformat(market_date).date() if market_date else datetime.now(UTC).date()
    context = EngineContext(
        db_target=db_target,
        storage=storage,
        job_run_id=planner_job_run_id,
    )
    result = StrategyEngine(context).run_entry(
        StrategyEntryRequest(
            run_ref=EngineRunRef(
                role=EngineComponentRole.STRATEGY,
                run_id=f"strategy:{trading_strategy_id}:entry:{_utc_now()}",
                trading_strategy_id=trading_strategy_id,
                job_run_id=planner_job_run_id,
            ),
            trading_strategy_id=trading_strategy_id,
            market_date=resolved_market_date,
        )
    )
    return dict(result.summary)


@with_storage()
def run_trading_strategy_entry_observation(
    *,
    db_target: str,
    trading_strategy_id: str,
    market_date: str | None = None,
    respect_schedule: bool = True,
    storage: Any | None = None,
) -> dict[str, Any]:
    return _run_trading_strategy_entry(
        db_target=db_target,
        trading_strategy_id=trading_strategy_id,
        market_date=market_date,
        planner_job_run_id=None,
        run_key=None,
        storage=storage,
        observation_only=True,
        respect_schedule=respect_schedule,
    )


__all__ = ["StrategyEngine", "run_trading_strategy_entry", "run_trading_strategy_entry_observation"]

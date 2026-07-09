from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from core.db.decorators import with_storage
from core.money import money_sum_float
from core.jobs.orchestration import NEW_YORK
from core.services.trading_strategies import load_active_trading_strategies
from core.storage.serializers import parse_date
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, utc_now_iso

from .strategy_ledger_engine import ENGINE_LEDGER_TABLES, build_engine_strategy_ledgers as _build_engine_strategy_ledgers
from .strategy_ledger_execution import build_execution_strategy_ledgers as _build_execution_strategy_ledgers


def _market_date_or_today(market_date: str | None) -> str:
    if market_date:
        return parse_date(market_date).isoformat()
    return datetime.now(NEW_YORK).date().isoformat()


def _window(market_date: str) -> tuple[date, datetime, datetime]:
    market_day = parse_date(market_date)
    start = datetime.combine(market_day, datetime.min.time(), tzinfo=UTC)
    return market_day, start, start + timedelta(days=1)


def _strategy_row(
    *,
    strategy: Any,
    engine_payload: Mapping[str, Any],
    execution_payload: Mapping[str, Any],
) -> dict[str, Any]:
    source = as_mapping(engine_payload.get("source"))
    candidates = as_mapping(engine_payload.get("candidates"))
    signals = as_mapping(engine_payload.get("signals"))
    decisions = as_mapping(engine_payload.get("decisions"))
    admissions = as_mapping(engine_payload.get("admissions"))
    intents = as_mapping(execution_payload.get("intents"))
    attempts = as_mapping(execution_payload.get("attempts"))
    positions = as_mapping(execution_payload.get("positions"))
    closes = as_mapping(execution_payload.get("closes"))
    latest_activity_candidates = [
        as_text(engine_payload.get("latest_activity_at")),
        as_text(execution_payload.get("latest_activity_at")),
    ]
    latest_activity = max((value for value in latest_activity_candidates if value), default=None)
    return {
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "trade_structure": strategy.trade_structure,
        "config_hash": strategy.config_hash,
        "execution_mode": strategy.execution.mode,
        "execution_runtime": strategy.execution.runtime,
        "approval_mode": strategy.execution.approval,
        "source": source,
        "candidates": candidates,
        "signals": signals,
        "decisions": decisions,
        "admissions": admissions,
        "intents": intents,
        "attempts": attempts,
        "positions": positions,
        "closes": closes,
        "pnl": as_mapping(execution_payload.get("pnl")),
        "market_context": as_mapping(engine_payload.get("market_context")),
        "top_blocker_reasons": dict(as_mapping(engine_payload.get("top_blocker_reasons"))),
        "latest_lifecycle_ids": {
            "ticker_source_run_id": source.get("latest_ticker_source_run_id"),
            "candidate_run_id": candidates.get("latest_candidate_run_id"),
            "trade_signal_id": signals.get("latest_trade_signal_id"),
            "trade_decision_id": decisions.get("latest_trade_decision_id"),
            "admission_decision_id": admissions.get("latest_admission_decision_id"),
            "execution_intent_id": intents.get("latest_execution_intent_id"),
            "execution_attempt_id": attempts.get("latest_execution_attempt_id"),
            "position_id": positions.get("latest_position_id"),
            "close_decision_id": closes.get("latest_close_decision_id"),
            "position_close_id": closes.get("latest_position_close_id"),
        },
        "latest_activity_at": latest_activity,
    }


@with_storage()
def build_strategy_evidence_ledger(
    *,
    db_target: str | None = None,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    resolved_market_date = _market_date_or_today(market_date)
    market_day, start, end = _window(resolved_market_date)
    now = datetime.now(UTC)
    strategies = load_active_trading_strategies()
    engine_schema_ready = storage.engine_facts.schema_has_tables(*ENGINE_LEDGER_TABLES)
    feature_store_schema_ready = storage.engine_facts.feature_store_schema_ready()
    market_context_schema_ready = storage.engine_facts.market_context_schema_ready()
    execution_schema_ready = storage.execution.schema_ready()
    intent_schema_ready = storage.execution.intent_schema_ready()
    portfolio_schema_ready = storage.execution.portfolio_schema_ready()
    close_lifecycle_schema_ready = storage.engine_facts.close_lifecycle_schema_ready()

    rows: list[dict[str, Any]] = []
    with storage.engine_facts.session_factory() as engine_session, storage.execution.session_factory() as execution_session:
        strategy_list = list(strategies.values())
        engine_payloads = (
            _build_engine_strategy_ledgers(
                session=engine_session,
                strategies=strategy_list,
                market_day=market_day,
                start=start,
                end=end,
                feature_store_schema_ready=feature_store_schema_ready,
            )
            if engine_schema_ready
            else {strategy.trading_strategy_id: {} for strategy in strategy_list}
        )
        execution_payloads = _build_execution_strategy_ledgers(
            session=execution_session,
            strategies=strategy_list,
            market_day=market_day,
            start=start,
            end=end,
            now=now,
            execution_schema_ready=execution_schema_ready,
            intent_schema_ready=intent_schema_ready,
            portfolio_schema_ready=portfolio_schema_ready,
            close_lifecycle_schema_ready=close_lifecycle_schema_ready,
        )
        for strategy in strategy_list:
            rows.append(
                _strategy_row(
                    strategy=strategy,
                    engine_payload=engine_payloads.get(strategy.trading_strategy_id, {}),
                    execution_payload=execution_payloads.get(strategy.trading_strategy_id, {}),
                )
            )

    total_realized = money_sum_float(coerce_float(as_mapping(row.get("pnl")).get("realized_pnl")) for row in rows)
    total_unrealized = money_sum_float(coerce_float(as_mapping(row.get("pnl")).get("unrealized_pnl")) for row in rows)
    schema = {
        "engine_facts": "ready" if engine_schema_ready else "blocked",
        "feature_store": "ready" if feature_store_schema_ready else "blocked",
        "market_context": "ready" if market_context_schema_ready else "blocked",
        "execution": "ready" if execution_schema_ready else "blocked",
        "execution_intents": "ready" if intent_schema_ready else "blocked",
        "portfolio": "ready" if portfolio_schema_ready else "blocked",
        "close_lifecycle": "ready" if close_lifecycle_schema_ready else "blocked",
    }
    status = "healthy" if all(value == "ready" for value in schema.values()) else "blocked"
    return {
        "status": status,
        "market_date": resolved_market_date,
        "generated_at": utc_now_iso(),
        "strategy_count": len(strategies),
        "schema": schema,
        "summary": {
            "source_run_count": sum(coerce_int(as_mapping(row.get("source")).get("source_run_count")) or 0 for row in rows),
            "candidate_run_count": sum(coerce_int(as_mapping(row.get("candidates")).get("candidate_run_count")) or 0 for row in rows),
            "trade_candidate_count": sum(coerce_int(as_mapping(row.get("candidates")).get("trade_candidate_count")) or 0 for row in rows),
            "signal_count": sum(coerce_int(as_mapping(row.get("signals")).get("signal_count")) or 0 for row in rows),
            "decision_count": sum(coerce_int(as_mapping(row.get("decisions")).get("decision_count")) or 0 for row in rows),
            "selected_count": sum(coerce_int(as_mapping(row.get("decisions")).get("selected_count")) or 0 for row in rows),
            "admission_count": sum(coerce_int(as_mapping(row.get("admissions")).get("admission_count")) or 0 for row in rows),
            "market_context_snapshot_count": len(
                {snapshot_id for row in rows for snapshot_id in as_mapping(as_mapping(row.get("market_context")).get("candidate_snapshot_ids"))}
                | {snapshot_id for row in rows for snapshot_id in as_mapping(as_mapping(row.get("market_context")).get("decision_snapshot_ids"))}
                | {snapshot_id for row in rows for snapshot_id in as_mapping(as_mapping(row.get("market_context")).get("admission_snapshot_ids"))}
            ),
            "intent_count": sum(coerce_int(as_mapping(row.get("intents")).get("intent_count")) or 0 for row in rows),
            "attempt_count": sum(coerce_int(as_mapping(row.get("attempts")).get("attempt_count")) or 0 for row in rows),
            "order_count": sum(coerce_int(as_mapping(row.get("attempts")).get("order_count")) or 0 for row in rows),
            "fill_count": sum(coerce_int(as_mapping(row.get("attempts")).get("fill_count")) or 0 for row in rows),
            "position_count": sum(coerce_int(as_mapping(row.get("positions")).get("position_count")) or 0 for row in rows),
            "open_position_count": sum(coerce_int(as_mapping(row.get("positions")).get("open_position_count")) or 0 for row in rows),
            "close_decision_count": sum(coerce_int(as_mapping(row.get("closes")).get("close_decision_count")) or 0 for row in rows),
            "close_count": sum(coerce_int(as_mapping(row.get("closes")).get("close_count")) or 0 for row in rows),
            "realized_pnl": total_realized,
            "unrealized_pnl": total_unrealized,
            "net_pnl": money_sum_float([total_realized, total_unrealized]),
        },
        "strategies": rows,
    }

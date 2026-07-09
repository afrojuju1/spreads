from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import select

from core.storage.lifecycle_models import TradeAdmissionModel, TradeSignalModel
from core.value_coercion import as_list, as_mapping, as_text, utc_iso

from core.services.ops.trading.flow_facts import _market_date_window


def _portfolio_admission_state(row: TradeAdmissionModel) -> dict[str, Any]:
    evidence = as_mapping(row.evidence_json)
    portfolio_admission = as_mapping(evidence.get("portfolio_admission"))
    allocation_plan = as_mapping(portfolio_admission.get("allocation_plan")) or as_mapping(
        as_mapping(portfolio_admission.get("evidence")).get("allocation_plan")
    )
    allocation_decision = as_mapping(as_mapping(portfolio_admission.get("evidence")).get("allocation_decision")) or as_mapping(
        allocation_plan.get("current_decision")
    )
    status = as_text(evidence.get("portfolio_admission_status")) or as_text(portfolio_admission.get("status")) or "not_evaluated"
    reason = as_text(evidence.get("portfolio_admission_reason")) or as_text(portfolio_admission.get("reason"))
    return {
        "status": status,
        "reason": reason,
        "message": as_text(portfolio_admission.get("message")),
        "latest_admission_decision_id": row.admission_decision_id,
        "admission_state": row.admission_state,
        "decided_at": utc_iso(row.decided_at),
        "policy": as_mapping(portfolio_admission.get("policy")),
        "metrics": as_mapping(portfolio_admission.get("metrics")),
        "allocation_plan": allocation_plan,
        "allocation_decision": allocation_decision,
        "blockers": as_list(portfolio_admission.get("blockers")),
        "reason_codes": as_list(portfolio_admission.get("reason_codes")),
    }

def _protection_admission_state(row: TradeAdmissionModel) -> dict[str, Any]:
    evidence = as_mapping(row.evidence_json)
    protection_admission = as_mapping(evidence.get("protection_admission"))
    status = as_text(evidence.get("protection_admission_status")) or as_text(protection_admission.get("status")) or "not_evaluated"
    reason = as_text(evidence.get("protection_admission_reason")) or as_text(protection_admission.get("reason"))
    return {
        "status": status,
        "reason": reason,
        "message": as_text(protection_admission.get("message")),
        "latest_admission_decision_id": row.admission_decision_id,
        "admission_state": row.admission_state,
        "decided_at": utc_iso(row.decided_at),
        "policy": as_mapping(protection_admission.get("policy")),
        "metrics": as_mapping(protection_admission.get("metrics")),
        "blockers": as_list(protection_admission.get("blockers")),
        "reason_codes": as_list(protection_admission.get("reason_codes")),
    }

def _latest_entry_admission_states(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
    state_builder: Any,
) -> dict[str, dict[str, Any]]:
    if not strategy_ids or not storage.engine_facts.schema_ready():
        return {}
    start, end = _market_date_window(market_date)
    latest: dict[str, dict[str, Any]] = {}
    with storage.engine_facts.session_factory() as session:
        rows = session.execute(
            select(TradeSignalModel.trading_strategy_id, TradeAdmissionModel)
            .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
            .where(TradeSignalModel.trading_strategy_id.in_(strategy_ids))
            .where(TradeAdmissionModel.admission_kind == "entry_open")
            .where(TradeAdmissionModel.decided_at >= start)
            .where(TradeAdmissionModel.decided_at < end)
            .order_by(
                TradeAdmissionModel.decided_at.desc(),
                TradeAdmissionModel.admission_decision_id.asc(),
            )
            .limit(500)
        ).all()
    for strategy_id, row in rows:
        key = str(strategy_id)
        if key in latest:
            continue
        state = state_builder(row)
        if state.get("status") == "not_evaluated" and not state.get("reason"):
            continue
        latest[key] = state
    return latest

def _latest_portfolio_admissions(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
) -> dict[str, dict[str, Any]]:
    return _latest_entry_admission_states(
        storage=storage,
        market_date=market_date,
        strategy_ids=strategy_ids,
        state_builder=_portfolio_admission_state,
    )

def _latest_protection_admissions(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
) -> dict[str, dict[str, Any]]:
    return _latest_entry_admission_states(
        storage=storage,
        market_date=market_date,
        strategy_ids=strategy_ids,
        state_builder=_protection_admission_state,
    )

def _admission_flow_status(state: Mapping[str, Any]) -> str:
    status = as_text(as_mapping(state).get("status"))
    if status in {"blocked", "unknown"}:
        return status
    return "healthy"

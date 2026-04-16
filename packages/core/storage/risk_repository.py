from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select

from core.storage.base import RepositoryBase
from core.storage.records import RiskDecisionRecord
from core.storage.risk_models import RiskDecisionModel
from core.storage.serializers import parse_date, parse_datetime


class RiskDecisionRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("risk_decisions")

    def create_risk_decision(
        self,
        *,
        risk_decision_id: str,
        decision_kind: str,
        status: str,
        note: str,
        session_id: str,
        session_date: str,
        label: str,
        cycle_id: str | None,
        candidate_id: int | None,
        opportunity_id: str | None,
        execution_attempt_id: str | None,
        trade_intent: str,
        entity_type: str,
        entity_key: str,
        underlying_symbol: str,
        strategy: str,
        quantity: int,
        limit_price: float | None,
        reason_codes: list[str],
        blockers: list[str],
        metrics: dict[str, Any],
        evidence: dict[str, Any],
        policy_refs: dict[str, Any],
        resolved_risk_policy: dict[str, Any],
        decided_at: str,
    ) -> RiskDecisionRecord:
        with self.session_scope() as session:
            row = RiskDecisionModel(
                risk_decision_id=risk_decision_id,
                decision_kind=decision_kind,
                status=status,
                note=note,
                session_id=session_id,
                session_date=parse_date(session_date),
                label=label,
                cycle_id=cycle_id,
                candidate_id=candidate_id,
                opportunity_id=opportunity_id,
                execution_attempt_id=execution_attempt_id,
                trade_intent=trade_intent,
                entity_type=entity_type,
                entity_key=entity_key,
                underlying_symbol=underlying_symbol,
                strategy=strategy,
                quantity=int(quantity),
                limit_price=None if limit_price is None else float(limit_price),
                reason_codes_json=list(reason_codes),
                blockers_json=list(blockers),
                metrics_json=dict(metrics),
                evidence_json=dict(evidence),
                policy_refs_json=dict(policy_refs),
                resolved_risk_policy_json=dict(resolved_risk_policy),
                decided_at=parse_datetime(decided_at),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_risk_decision(self, risk_decision_id: str) -> RiskDecisionRecord | None:
        with self.session_factory() as session:
            row = session.get(RiskDecisionModel, risk_decision_id)
        if row is None:
            return None
        return self.row(row)

    def attach_execution_attempt(
        self,
        *,
        risk_decision_id: str,
        execution_attempt_id: str,
    ) -> RiskDecisionRecord:
        with self.session_scope() as session:
            row = session.get(RiskDecisionModel, risk_decision_id)
            if row is None:
                raise ValueError(f"Unknown risk_decision_id: {risk_decision_id}")
            row.execution_attempt_id = execution_attempt_id
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_risk_decisions(
        self,
        *,
        session_id: str | None = None,
        session_date: str | None = None,
        label: str | None = None,
        status: str | None = None,
        opportunity_id: str | None = None,
        execution_attempt_id: str | None = None,
        limit: int = 200,
    ) -> list[RiskDecisionRecord]:
        statement = select(RiskDecisionModel)
        if session_id:
            statement = statement.where(RiskDecisionModel.session_id == session_id)
        if session_date:
            statement = statement.where(RiskDecisionModel.session_date == date.fromisoformat(session_date))
        if label:
            statement = statement.where(RiskDecisionModel.label == label)
        if status:
            statement = statement.where(RiskDecisionModel.status == status)
        if opportunity_id:
            statement = statement.where(RiskDecisionModel.opportunity_id == opportunity_id)
        if execution_attempt_id:
            statement = statement.where(RiskDecisionModel.execution_attempt_id == execution_attempt_id)
        statement = statement.order_by(RiskDecisionModel.decided_at.desc(), RiskDecisionModel.risk_decision_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

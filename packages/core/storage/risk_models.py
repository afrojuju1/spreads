from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Float, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class RiskDecisionModel(Base):
    __tablename__ = "risk_decisions"
    __table_args__ = (
        Index("idx_risk_decisions_session_decided", "session_id", "decided_at"),
        Index("idx_risk_decisions_status_decided", "status", "decided_at"),
        Index("idx_risk_decisions_opportunity_decided", "opportunity_id", "decided_at"),
        Index("idx_risk_decisions_execution_attempt", "execution_attempt_id"),
    )

    risk_decision_id: Mapped[str] = mapped_column(Text, primary_key=True)
    decision_kind: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    note: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    cycle_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("collector_cycles.cycle_id", ondelete="SET NULL"),
        nullable=True,
    )
    candidate_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("collector_cycle_candidates.candidate_id", ondelete="SET NULL"),
        nullable=True,
    )
    opportunity_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("opportunities.opportunity_id", ondelete="SET NULL"),
        nullable=True,
    )
    execution_attempt_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("execution_attempts.execution_attempt_id", ondelete="SET NULL"),
        nullable=True,
    )
    trade_intent: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_key: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    blockers_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    policy_refs_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    resolved_risk_policy_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.storage.db import Base


class SignalStateModel(Base):
    __tablename__ = "signal_states"
    __table_args__ = (
        Index("idx_signal_states_label_state_updated", "label", "state", "updated_at"),
        Index(
            "idx_signal_states_entity_updated",
            "entity_type",
            "entity_key",
            "updated_at",
        ),
        Index("idx_signal_states_session_updated", "session_date", "updated_at"),
    )

    signal_state_id: Mapped[str] = mapped_column(Text, primary_key=True)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_family: Mapped[str] = mapped_column(Text, nullable=False)
    profile: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_key: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason_codes_json: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    blockers_json: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    evidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    active_cycle_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    active_candidate_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    active_selection_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    opportunity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    market_session: Mapped[str] = mapped_column(Text, nullable=False)
    first_observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    transitions: Mapped[list["SignalStateTransitionModel"]] = relationship(
        back_populates="signal_state",
        cascade="all, delete-orphan",
        order_by="SignalStateTransitionModel.occurred_at",
    )


class SignalStateTransitionModel(Base):
    __tablename__ = "signal_state_transitions"
    __table_args__ = (
        Index(
            "idx_signal_state_transitions_state_occurred",
            "signal_state_id",
            "occurred_at",
        ),
        Index(
            "idx_signal_state_transitions_label_session_occurred",
            "label",
            "session_date",
            "occurred_at",
        ),
        Index(
            "idx_signal_state_transitions_entity_occurred",
            "entity_type",
            "entity_key",
            "occurred_at",
        ),
    )

    transition_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    signal_state_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("signal_states.signal_state_id", ondelete="CASCADE"),
        nullable=False,
    )
    label: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_family: Mapped[str] = mapped_column(Text, nullable=False)
    profile: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_key: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    from_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_state: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason_codes_json: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    blockers_json: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    evidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    active_cycle_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    active_candidate_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    active_selection_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    opportunity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    market_session: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    signal_state: Mapped[SignalStateModel] = relationship(back_populates="transitions")


class OpportunityModel(Base):
    __tablename__ = "opportunities"
    __table_args__ = (
        Index(
            "idx_opportunities_pipeline_market_lifecycle",
            "pipeline_id",
            "market_date",
            "lifecycle_state",
        ),
        Index(
            "idx_opportunities_label_session_lifecycle",
            "label",
            "session_date",
            "lifecycle_state",
        ),
        Index(
            "idx_opportunities_entity_updated",
            "entity_type",
            "entity_key",
            "updated_at",
        ),
        Index("idx_opportunities_source_candidate", "source_candidate_id"),
        Index("idx_opportunities_updated_at", "updated_at"),
    )

    opportunity_id: Mapped[str] = mapped_column(Text, primary_key=True)
    pipeline_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    market_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    cycle_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    root_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_family: Mapped[str] = mapped_column(Text, nullable=False)
    profile: Mapped[str] = mapped_column(Text, nullable=False)
    style_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    horizon_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    product_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_key: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str | None] = mapped_column(Text, nullable=True)
    side_bias: Mapped[str | None] = mapped_column(Text, nullable=True)
    selection_state: Mapped[str] = mapped_column(Text, nullable=False)
    selection_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    state_reason: Mapped[str] = mapped_column(Text, nullable=False)
    origin: Mapped[str] = mapped_column(Text, nullable=False)
    eligibility: Mapped[str] = mapped_column(Text, nullable=False)
    eligibility_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    promotion_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    execution_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    signal_state_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    lifecycle_state: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    reason_codes_json: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    blockers_json: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    legs_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    economics_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    strategy_metrics_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    order_payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    evidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    execution_shape_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    risk_hints_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    source_cycle_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_candidate_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_selection_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_identity: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    consumed_by_execution_attempt_id: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )


class OpportunityDecisionModel(Base):
    __tablename__ = "opportunity_decisions"
    __table_args__ = (
        Index(
            "ux_opportunity_decisions_run_opportunity",
            "run_key",
            "opportunity_id",
            unique=True,
        ),
        Index(
            "idx_opportunity_decisions_bot_automation_decided",
            "bot_id",
            "automation_id",
            "decided_at",
        ),
        Index(
            "idx_opportunity_decisions_opportunity_decided",
            "opportunity_id",
            "decided_at",
        ),
        Index("idx_opportunity_decisions_state_decided", "state", "decided_at"),
    )

    opportunity_decision_id: Mapped[str] = mapped_column(Text, primary_key=True)
    opportunity_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("opportunities.opportunity_id", ondelete="CASCADE"),
        nullable=False,
    )
    bot_id: Mapped[str] = mapped_column(Text, nullable=False)
    automation_id: Mapped[str] = mapped_column(Text, nullable=False)
    run_key: Mapped[str] = mapped_column(Text, nullable=False)
    scope_key: Mapped[str] = mapped_column(Text, nullable=False)
    policy_ref_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reason_codes_json: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    superseded_by_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from spreads.storage.db import Base


class CollectorCycleModel(Base):
    __tablename__ = "collector_cycles"
    __table_args__ = (
        Index("idx_collector_cycles_label_session_generated_at", "label", "session_date", "generated_at"),
        Index("idx_collector_cycles_session_id_generated_at", "session_id", "generated_at"),
    )

    cycle_id: Mapped[str] = mapped_column(Text, primary_key=True)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    job_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    universe_label: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[str] = mapped_column(Text, nullable=False)
    profile: Mapped[str] = mapped_column(Text, nullable=False)
    greeks_source: Mapped[str] = mapped_column(Text, nullable=False)
    symbols_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    failures_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    selection_memory_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    candidates: Mapped[list["CollectorCycleCandidateModel"]] = relationship(
        back_populates="cycle",
        cascade="all, delete-orphan",
        order_by="CollectorCycleCandidateModel.selection_rank",
    )
    events: Mapped[list["CollectorCycleEventModel"]] = relationship(
        back_populates="cycle",
        cascade="all, delete-orphan",
        order_by="CollectorCycleEventModel.generated_at",
    )


class PipelineModel(Base):
    __tablename__ = "pipelines"
    __table_args__ = (
        Index("ux_pipelines_label", "label", unique=True),
        Index("idx_pipelines_enabled_updated", "enabled", "updated_at"),
    )

    pipeline_id: Mapped[str] = mapped_column(Text, primary_key=True)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    source_job_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    universe_label: Mapped[str | None] = mapped_column(Text, nullable=True)
    style_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_horizon_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_families_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    product_scope_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    policy_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PipelineCycleModel(Base):
    __tablename__ = "pipeline_cycles"
    __table_args__ = (
        Index("idx_pipeline_cycles_pipeline_generated", "pipeline_id", "generated_at"),
        Index("idx_pipeline_cycles_pipeline_market_date", "pipeline_id", "market_date"),
    )

    cycle_id: Mapped[str] = mapped_column(Text, primary_key=True)
    pipeline_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("pipelines.pipeline_id", ondelete="CASCADE"),
        nullable=False,
    )
    label: Mapped[str] = mapped_column(Text, nullable=False)
    market_date: Mapped[date] = mapped_column(Date, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    job_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    universe_label: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_mode: Mapped[str | None] = mapped_column(Text, nullable=True)
    legacy_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    greeks_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    symbols_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    failures_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    selection_memory_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class CollectorCycleCandidateModel(Base):
    __tablename__ = "collector_cycle_candidates"
    __table_args__ = (
        Index(
            "idx_collector_cycle_candidates_cycle_bucket_position",
            "cycle_id",
            "selection_state",
            "selection_rank",
        ),
        Index("idx_collector_cycle_candidates_run_id", "run_id"),
        Index(
            "idx_collector_cycle_candidates_identity",
            "underlying_symbol",
            "strategy",
            "expiration_date",
            "short_symbol",
            "long_symbol",
        ),
    )

    candidate_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    cycle_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("collector_cycles.cycle_id", ondelete="CASCADE"),
        nullable=False,
    )
    selection_state: Mapped[str] = mapped_column(Text, nullable=False)
    selection_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    state_reason: Mapped[str] = mapped_column(Text, nullable=False)
    origin: Mapped[str] = mapped_column(Text, nullable=False)
    eligibility: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[str] = mapped_column(Text, nullable=False)
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    short_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    long_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    quality_score: Mapped[float] = mapped_column(Float, nullable=False)
    midpoint_credit: Mapped[float] = mapped_column(Float, nullable=False)
    candidate_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    cycle: Mapped[CollectorCycleModel] = relationship(back_populates="candidates")


class CollectorCycleEventModel(Base):
    __tablename__ = "collector_cycle_events"
    __table_args__ = (
        Index("idx_collector_cycle_events_label_session_generated_at", "label", "session_date", "generated_at"),
    )

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    cycle_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("collector_cycles.cycle_id", ondelete="CASCADE"),
        nullable=False,
    )
    label: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    previous_candidate_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    current_candidate_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    cycle: Mapped[CollectorCycleModel] = relationship(back_populates="events")

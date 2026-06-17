from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class BacktestRunModel(Base):
    __tablename__ = "backtest_runs"
    __table_args__ = (
        Index("idx_backtest_runs_mode_state_created", "mode", "state", "created_at"),
        Index("idx_backtest_runs_window", "start_date", "end_date"),
    )

    backtest_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    mode: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    requested_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_ids_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    config_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    fidelity_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    artifact_root: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class BacktestArtifactModel(Base):
    __tablename__ = "backtest_artifacts"
    __table_args__ = (
        Index("idx_backtest_artifacts_run_kind", "backtest_run_id", "artifact_kind"),
        Index("idx_backtest_artifacts_created", "created_at"),
    )

    backtest_artifact_id: Mapped[str] = mapped_column(Text, primary_key=True)
    backtest_run_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("backtest_runs.backtest_run_id", ondelete="CASCADE"),
        nullable=False,
    )
    artifact_kind: Mapped[str] = mapped_column(Text, nullable=False)
    storage_kind: Mapped[str] = mapped_column(Text, nullable=False)
    uri: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    byte_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    schema_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BacktestVariantResultModel(Base):
    __tablename__ = "backtest_variant_results"
    __table_args__ = (
        Index("idx_backtest_variant_results_run_rank", "backtest_run_id", "rank"),
        Index("idx_backtest_variant_results_strategy", "trading_strategy_id", "variant_hash"),
    )

    backtest_variant_id: Mapped[str] = mapped_column(Text, primary_key=True)
    backtest_run_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("backtest_runs.backtest_run_id", ondelete="CASCADE"),
        nullable=False,
    )
    trading_strategy_id: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)
    variant_hash: Mapped[str] = mapped_column(Text, nullable=False)
    parameters_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    fidelity_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

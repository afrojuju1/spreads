from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class StrategyRunModel(Base):
    __tablename__ = "strategy_runs"
    __table_args__ = (
        Index(
            "idx_strategy_runs_strategy_started",
            "trading_strategy_id",
            "started_at",
        ),
        Index(
            "idx_strategy_runs_session_started",
            "session_date",
            "started_at",
        ),
        Index(
            "idx_strategy_runs_cycle_strategy",
            "cycle_id",
            "trading_strategy_id",
        ),
    )

    strategy_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    trading_strategy_id: Mapped[str] = mapped_column(Text, nullable=False)
    trigger_type: Mapped[str] = mapped_column(Text, nullable=False)
    job_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    cycle_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    label: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    result_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)

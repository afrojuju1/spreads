from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from spreads.storage.db import Base


class PostMarketAnalysisRunModel(Base):
    __tablename__ = "post_market_analysis_runs"
    __table_args__ = (
        Index("idx_post_market_runs_label_session_completed", "label", "session_date", "completed_at"),
        Index("idx_post_market_runs_status_created", "status", "created_at"),
    )

    analysis_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    job_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
        nullable=True,
        unique=True,
    )
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    diagnostics_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    recommendations_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
    report_markdown: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

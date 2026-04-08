from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from spreads.storage.db import Base


class GeneratorJobModel(Base):
    __tablename__ = "generator_jobs"
    __table_args__ = (
        Index("idx_generator_jobs_status_created", "status", "created_at"),
        Index("idx_generator_jobs_symbol_created", "symbol", "created_at"),
    )

    generator_job_id: Mapped[str] = mapped_column(Text, primary_key=True)
    arq_job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    result_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from spreads.storage.db import Base


class AlertEventModel(Base):
    __tablename__ = "alert_events"
    __table_args__ = (
        Index("idx_alert_events_session_label_created_at", "session_date", "label", "created_at"),
        Index("idx_alert_events_symbol_created_at", "symbol", "created_at"),
        Index("idx_alert_events_dedupe_created_at", "dedupe_key", "created_at"),
    )

    alert_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    cycle_id: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    alert_type: Mapped[str] = mapped_column(Text, nullable=False)
    dedupe_key: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    delivery_target: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    response_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)


class AlertStateModel(Base):
    __tablename__ = "alert_state"

    dedupe_key: Mapped[str] = mapped_column(Text, primary_key=True)
    last_alert_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_cycle_id: Mapped[str] = mapped_column(Text, nullable=False)
    last_alert_type: Mapped[str] = mapped_column(Text, nullable=False)
    state_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from spreads.storage.db import Base


class AccountSnapshotModel(Base):
    __tablename__ = "account_snapshots"
    __table_args__ = (
        Index("idx_account_snapshots_broker_captured", "broker", "captured_at"),
    )

    snapshot_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    broker: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca")
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="broker_sync")
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    account_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    pnl_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    positions_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    history_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class BrokerSyncStateModel(Base):
    __tablename__ = "broker_sync_state"
    __table_args__ = (
        Index("idx_broker_sync_state_broker_updated", "broker", "updated_at"),
    )

    sync_key: Mapped[str] = mapped_column(Text, primary_key=True)
    broker: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca")
    status: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cursor_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

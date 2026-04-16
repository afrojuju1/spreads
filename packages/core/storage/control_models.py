from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class OperatorActionModel(Base):
    __tablename__ = "operator_actions"
    __table_args__ = (
        Index("idx_operator_actions_kind_occurred", "action_kind", "occurred_at"),
        Index("idx_operator_actions_source_occurred", "source_kind", "occurred_at"),
    )

    operator_action_id: Mapped[str] = mapped_column(Text, primary_key=True)
    action_kind: Mapped[str] = mapped_column(Text, nullable=False)
    source_kind: Mapped[str] = mapped_column(Text, nullable=False)
    actor_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_scope: Mapped[str] = mapped_column(Text, nullable=False)
    requested_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    resulting_state_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    causation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PolicyRolloutModel(Base):
    __tablename__ = "policy_rollouts"
    __table_args__ = (
        Index("idx_policy_rollouts_family_status_effective", "family", "status", "effective_at"),
    )

    policy_rollout_id: Mapped[str] = mapped_column(Text, primary_key=True)
    family: Mapped[str] = mapped_column(Text, nullable=False)
    scope_kind: Mapped[str] = mapped_column(Text, nullable=False)
    scope_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    version_token: Mapped[str] = mapped_column(Text, nullable=False)
    policy_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_kind: Mapped[str] = mapped_column(Text, nullable=False)
    operator_action_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("operator_actions.operator_action_id", ondelete="SET NULL"),
        nullable=True,
    )
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class ControlStateModel(Base):
    __tablename__ = "control_state"
    __table_args__ = (
        Index("idx_control_state_mode_updated", "mode", "updated_at"),
    )

    control_state_id: Mapped[str] = mapped_column(Text, primary_key=True)
    mode: Mapped[str] = mapped_column(Text, nullable=False)
    reason_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_kind: Mapped[str] = mapped_column(Text, nullable=False)
    triggered_by_action_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("operator_actions.operator_action_id", ondelete="SET NULL"),
        nullable=True,
    )
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

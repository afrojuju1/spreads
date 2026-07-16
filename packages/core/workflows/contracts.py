from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field

from core.model_contracts import DomainModel


class TradeLifecycleWorkflowInput(DomainModel):
    execution_intent_id: str = Field(min_length=1, max_length=512)


class CloseLifecycleWorkflowInput(DomainModel):
    execution_intent_id: str = Field(min_length=1, max_length=512)


class LifecycleActivityResult(DomainModel):
    status: str = Field(min_length=1, max_length=64)
    execution_intent_id: str = Field(min_length=1, max_length=512)
    execution_attempt_id: str | None = Field(default=None, max_length=512)
    replacement_execution_intent_id: str | None = Field(default=None, max_length=512)
    position_id: str | None = Field(default=None, max_length=512)
    attempt_status: str | None = Field(default=None, max_length=64)
    requested_at: datetime | None = None
    submitted_at: datetime | None = None
    completed_at: datetime | None = None
    reason: str | None = Field(default=None, max_length=512)
    submit_ttl_seconds: int | None = Field(default=None, ge=1, le=86_400)
    stale_after_seconds: int | None = Field(default=None, ge=1, le=86_400)
    stale_order_action: Literal["cancel_and_reprice", "fail_closed", "leave_working"] | None = None


class LifecycleWorkflowResult(DomainModel):
    workflow_id: str = Field(min_length=1, max_length=512)
    state: str = Field(min_length=1, max_length=64)
    aggregate_type: Literal["execution_intent", "execution_attempt"]
    aggregate_id: str = Field(min_length=1, max_length=512)
    execution_intent_id: str = Field(min_length=1, max_length=512)
    execution_attempt_id: str | None = Field(default=None, max_length=512)
    replacement_execution_intent_id: str | None = Field(default=None, max_length=512)
    position_id: str | None = Field(default=None, max_length=512)
    reason: str | None = Field(default=None, max_length=512)
    completed_at: datetime


__all__ = [
    "CloseLifecycleWorkflowInput",
    "LifecycleActivityResult",
    "LifecycleWorkflowResult",
    "TradeLifecycleWorkflowInput",
]

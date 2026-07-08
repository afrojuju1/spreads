from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import Field

from core.model_contracts import DomainModel


class TradeLifecycleWorkflowInput(DomainModel):
    execution_intent_id: str
    workflow_id: str
    correlation_id: str
    requested_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


class CloseLifecycleWorkflowInput(DomainModel):
    position_id: str
    execution_intent_id: str | None = None
    workflow_id: str
    correlation_id: str
    requested_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


class LifecycleWorkflowResult(DomainModel):
    workflow_id: str
    state: str
    aggregate_type: str
    aggregate_id: str
    completed_at: datetime | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "CloseLifecycleWorkflowInput",
    "LifecycleWorkflowResult",
    "TradeLifecycleWorkflowInput",
]

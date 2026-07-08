from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from core.model_contracts import DomainModel


class EngineCommandType(StrEnum):
    START_TRADE_LIFECYCLE = "engine.start_trade_lifecycle"
    START_CLOSE_LIFECYCLE = "engine.start_close_lifecycle"
    SUBMIT_BROKER_ORDER = "engine.submit_broker_order"
    REFRESH_BROKER_ORDER = "engine.refresh_broker_order"
    CANCEL_BROKER_ORDER = "engine.cancel_broker_order"
    RECORD_BROKER_ORDER = "engine.record_broker_order"
    RECORD_BROKER_FILL = "engine.record_broker_fill"
    RECONCILE_POSITION = "engine.reconcile_position"


class EngineCommand(DomainModel):
    command_type: str
    command_id: str
    aggregate_type: str
    aggregate_id: str
    workflow_id: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    idempotency_key: str
    requested_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = ["EngineCommand", "EngineCommandType"]

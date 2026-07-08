from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from core.engine.ids import engine_event_id, engine_outbox_id, idempotency_key
from core.model_contracts import DomainModel

ENGINE_EVENT_STREAM = "spreads.engine.lifecycle"
ENGINE_EVENT_SUBJECT_PREFIX = "spreads.engine"
ENGINE_EVENT_VERSION = 1


class EngineAggregateType(StrEnum):
    TRADE_SIGNAL = "trade_signal"
    TRADE_DECISION = "trade_decision"
    EXECUTION_INTENT = "execution_intent"
    ADMISSION = "admission"
    EXECUTION_ATTEMPT = "execution_attempt"
    BROKER_ORDER = "broker_order"
    POSITION = "position"
    CLOSE_DECISION = "close_decision"
    POSITION_CLOSE = "position_close"
    RECONCILIATION = "reconciliation"


class EngineEventType(StrEnum):
    COMMAND_ACCEPTED = "engine.command_accepted"
    COMMAND_REJECTED = "engine.command_rejected"
    STATE_TRANSITIONED = "engine.state_transitioned"
    WORKFLOW_STARTED = "engine.workflow_started"
    WORKFLOW_COMPLETED = "engine.workflow_completed"
    BROKER_SUBMISSION_REQUESTED = "engine.broker_submission_requested"
    BROKER_SUBMISSION_UNKNOWN = "engine.broker_submission_unknown"
    BROKER_ORDER_OBSERVED = "engine.broker_order_observed"
    BROKER_FILL_OBSERVED = "engine.broker_fill_observed"
    PROJECTION_REFRESH_REQUESTED = "engine.projection_refresh_requested"


class EngineEvent(DomainModel):
    engine_event_id: str | None = None
    run_id: str | None = None
    workflow_id: str | None = None
    workflow_run_id: str | None = None
    event_type: str
    event_version: int = ENGINE_EVENT_VERSION
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int | None = None
    lifecycle_object: str | None = None
    from_state: str | None = None
    to_state: str | None = None
    trading_strategy_id: str | None = None
    trade_signal_id: str | None = None
    trade_decision_id: str | None = None
    execution_intent_id: str | None = None
    execution_attempt_id: str | None = None
    broker_order_id: str | None = None
    position_id: str | None = None
    session_date: date | None = None
    market_session: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    idempotency_key: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime
    recorded_at: datetime | None = None

    def resolved_idempotency_key(self) -> str:
        if self.idempotency_key:
            return self.idempotency_key
        return idempotency_key(
            "engine_event",
            self.event_type,
            self.aggregate_type,
            self.aggregate_id,
            self.aggregate_version,
            self.lifecycle_object,
            self.from_state,
            self.to_state,
            self.occurred_at,
            self.payload,
        )

    def resolved_engine_event_id(self) -> str:
        return self.engine_event_id or engine_event_id(self.resolved_idempotency_key())

    def subject(self) -> str:
        aggregate = self.aggregate_type.replace("_", ".")
        event = self.event_type.removeprefix("engine.").replace("_", ".")
        return f"{ENGINE_EVENT_SUBJECT_PREFIX}.{aggregate}.{event}"


class EngineOutboxMessage(DomainModel):
    engine_outbox_id: str | None = None
    engine_event_id: str
    stream: str = ENGINE_EVENT_STREAM
    subject: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload: dict[str, Any]
    headers: dict[str, str] = Field(default_factory=dict)

    def resolved_engine_outbox_id(self) -> str:
        return self.engine_outbox_id or engine_outbox_id(self.engine_event_id, stream=self.stream, subject=self.subject)


__all__ = [
    "ENGINE_EVENT_STREAM",
    "ENGINE_EVENT_SUBJECT_PREFIX",
    "ENGINE_EVENT_VERSION",
    "EngineAggregateType",
    "EngineEvent",
    "EngineEventType",
    "EngineOutboxMessage",
]

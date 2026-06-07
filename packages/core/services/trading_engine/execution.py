from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from .kernel import EnginePayload, EngineRunRef, EngineSummary


@dataclass(frozen=True)
class ExecutionIntentRequest:
    run_ref: EngineRunRef
    trading_strategy_id: str
    action_type: str
    source_object_type: str
    source_object_id: str
    slot_key: str
    payload: EnginePayload = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionIntentResult:
    run_ref: EngineRunRef
    execution_intent_id: str
    state: str
    payload: EnginePayload = field(default_factory=dict)


@dataclass(frozen=True)
class DispatchResult:
    run_ref: EngineRunRef
    claimed_intent_ids: tuple[str, ...]
    execution_attempt_ids: tuple[str, ...]
    status: str
    summary: EngineSummary = field(default_factory=dict)


class ExecutionEngine(Protocol):
    def create_intent(self, request: ExecutionIntentRequest) -> ExecutionIntentResult: ...

    def dispatch_pending(self, *, run_ref: EngineRunRef, limit: int) -> DispatchResult: ...

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from .kernel import EnginePayload, EngineRunRef, EngineSummary


@dataclass(frozen=True)
class AdmissionRequest:
    run_ref: EngineRunRef
    execution_intent_id: str
    trading_strategy_id: str
    action_type: str
    payload: EnginePayload = field(default_factory=dict)


@dataclass(frozen=True)
class AdmissionResult:
    run_ref: EngineRunRef
    admission_decision_id: str
    execution_intent_id: str
    state: str
    reason_codes: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    metrics: EngineSummary = field(default_factory=dict)


class RiskEngine(Protocol):
    def admit_intent(self, request: AdmissionRequest) -> AdmissionResult: ...

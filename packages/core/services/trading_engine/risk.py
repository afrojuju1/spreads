from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from .kernel import EngineRunRef


@dataclass(frozen=True)
class AdmissionRequest:
    run_ref: EngineRunRef
    execution_intent_id: str
    trading_strategy_id: str
    action_type: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AdmissionResult:
    run_ref: EngineRunRef
    admission_decision_id: str
    execution_intent_id: str
    state: str
    reason_codes: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)


class RiskEngine(Protocol):
    def admit_intent(self, request: AdmissionRequest) -> AdmissionResult:
        ...

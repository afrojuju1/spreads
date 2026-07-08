from __future__ import annotations

from core.services.trading_lifecycle import (
    AdmissionState,
    BrokerOrderState,
    CloseDecisionState,
    ExecutionAttemptState,
    ExecutionIntentState,
    LifecycleObject,
    LifecycleTransitionDecision,
    LifecycleTransitionError,
    PositionCloseState,
    ReconciliationState,
    TradeDecisionState,
    TradeSignalState,
    TradingPositionState,
    normalize_lifecycle_state,
    require_lifecycle_transition,
    validate_lifecycle_transition,
)

EngineLifecycleObject = LifecycleObject

validate_engine_transition = validate_lifecycle_transition
require_engine_transition = require_lifecycle_transition
normalize_engine_state = normalize_lifecycle_state

__all__ = [
    "AdmissionState",
    "BrokerOrderState",
    "CloseDecisionState",
    "EngineLifecycleObject",
    "ExecutionAttemptState",
    "ExecutionIntentState",
    "LifecycleObject",
    "LifecycleTransitionDecision",
    "LifecycleTransitionError",
    "PositionCloseState",
    "ReconciliationState",
    "TradeDecisionState",
    "TradeSignalState",
    "TradingPositionState",
    "normalize_engine_state",
    "normalize_lifecycle_state",
    "require_engine_transition",
    "require_lifecycle_transition",
    "validate_engine_transition",
    "validate_lifecycle_transition",
]

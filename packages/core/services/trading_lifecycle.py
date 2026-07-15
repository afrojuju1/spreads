from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypeVar


class LifecycleObject(StrEnum):
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


class TradeSignalState(StrEnum):
    OBSERVED = "observed"
    READY = "ready"
    BLOCKED = "blocked"
    STALE = "stale"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    RETIRED = "retired"


class TradeDecisionState(StrEnum):
    SKIP = "skip"
    NO_ENTRY = "no_entry"
    SELECTED = "selected"
    SELECTED_BLOCKED = "selected_blocked"
    SUPERSEDED = "superseded"


class ExecutionIntentState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    FAILED = "failed"
    CANCELED = "canceled"
    REVOKED = "revoked"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"


class AdmissionState(StrEnum):
    APPROVED = "approved"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


class ExecutionAttemptState(StrEnum):
    PENDING_SUBMISSION = "pending_submission"
    SUBMIT_UNKNOWN = "submit_unknown"
    WORKING = "working"
    PARTIALLY_FILLED = "partially_filled"
    CANCELING = "canceling"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    FAILED = "failed"
    STALE = "stale"


class BrokerOrderState(StrEnum):
    WORKING = "working"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    PENDING_CANCEL = "pending_cancel"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    REPLACED = "replaced"
    UNKNOWN = "unknown"


class TradingPositionState(StrEnum):
    PENDING_OPEN = "pending_open"
    PARTIAL_OPEN = "partial_open"
    OPEN = "open"
    PARTIAL_CLOSE = "partial_close"
    CLOSED = "closed"


class CloseDecisionState(StrEnum):
    HOLD = "hold"
    CLOSE_SELECTED = "close_selected"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"
    SUPERSEDED = "superseded"


class PositionCloseState(StrEnum):
    RECORDED = "recorded"
    PARTIAL = "partial"
    COMPLETE = "complete"
    VOIDED = "voided"


class ReconciliationState(StrEnum):
    MATCHED = "matched"
    BROKER_MISSING = "broker_missing"
    LOCAL_MISSING = "local_missing"
    QUANTITY_MISMATCH = "quantity_mismatch"
    STATUS_MISMATCH = "status_mismatch"
    SUBMIT_UNKNOWN_UNRESOLVED = "submit_unknown_unresolved"
    REPAIRED = "repaired"
    IGNORED = "ignored"


StateT = TypeVar("StateT", bound=StrEnum)


@dataclass(frozen=True)
class LifecycleTransitionDecision:
    object_type: LifecycleObject
    from_state: str | None
    to_state: str
    allowed: bool
    reason_code: str
    note: str
    allowed_next_states: tuple[str, ...]


class LifecycleTransitionError(ValueError):
    def __init__(self, decision: LifecycleTransitionDecision) -> None:
        super().__init__(decision.note)
        self.decision = decision


STATE_ENUM_BY_OBJECT: dict[LifecycleObject, type[StrEnum]] = {
    LifecycleObject.TRADE_SIGNAL: TradeSignalState,
    LifecycleObject.TRADE_DECISION: TradeDecisionState,
    LifecycleObject.EXECUTION_INTENT: ExecutionIntentState,
    LifecycleObject.ADMISSION: AdmissionState,
    LifecycleObject.EXECUTION_ATTEMPT: ExecutionAttemptState,
    LifecycleObject.BROKER_ORDER: BrokerOrderState,
    LifecycleObject.POSITION: TradingPositionState,
    LifecycleObject.CLOSE_DECISION: CloseDecisionState,
    LifecycleObject.POSITION_CLOSE: PositionCloseState,
    LifecycleObject.RECONCILIATION: ReconciliationState,
}


TERMINAL_STATES: dict[LifecycleObject, frozenset[StrEnum]] = {
    LifecycleObject.TRADE_SIGNAL: frozenset(
        {
            TradeSignalState.CONSUMED,
            TradeSignalState.EXPIRED,
            TradeSignalState.RETIRED,
        }
    ),
    LifecycleObject.TRADE_DECISION: frozenset(
        {
            TradeDecisionState.SKIP,
            TradeDecisionState.NO_ENTRY,
            TradeDecisionState.SELECTED_BLOCKED,
            TradeDecisionState.SUPERSEDED,
        }
    ),
    LifecycleObject.EXECUTION_INTENT: frozenset(
        {
            ExecutionIntentState.FILLED,
            ExecutionIntentState.FAILED,
            ExecutionIntentState.CANCELED,
            ExecutionIntentState.REVOKED,
            ExecutionIntentState.EXPIRED,
            ExecutionIntentState.SUPERSEDED,
        }
    ),
    LifecycleObject.ADMISSION: frozenset(
        {
            AdmissionState.APPROVED,
            AdmissionState.BLOCKED,
            AdmissionState.UNKNOWN,
        }
    ),
    LifecycleObject.EXECUTION_ATTEMPT: frozenset(
        {
            ExecutionAttemptState.FILLED,
            ExecutionAttemptState.CANCELED,
            ExecutionAttemptState.REJECTED,
            ExecutionAttemptState.EXPIRED,
            ExecutionAttemptState.FAILED,
        }
    ),
    LifecycleObject.BROKER_ORDER: frozenset(
        {
            BrokerOrderState.FILLED,
            BrokerOrderState.CANCELED,
            BrokerOrderState.REJECTED,
            BrokerOrderState.EXPIRED,
            BrokerOrderState.REPLACED,
        }
    ),
    LifecycleObject.POSITION: frozenset({TradingPositionState.CLOSED}),
    LifecycleObject.CLOSE_DECISION: frozenset(
        {
            CloseDecisionState.HOLD,
            CloseDecisionState.CLOSE_SELECTED,
            CloseDecisionState.BLOCKED,
            CloseDecisionState.UNKNOWN,
            CloseDecisionState.SUPERSEDED,
        }
    ),
    LifecycleObject.POSITION_CLOSE: frozenset(
        {
            PositionCloseState.COMPLETE,
            PositionCloseState.VOIDED,
        }
    ),
    LifecycleObject.RECONCILIATION: frozenset(
        {
            ReconciliationState.MATCHED,
            ReconciliationState.REPAIRED,
            ReconciliationState.IGNORED,
        }
    ),
}


INITIAL_STATES: dict[LifecycleObject, frozenset[StrEnum]] = {
    LifecycleObject.TRADE_SIGNAL: frozenset({TradeSignalState.OBSERVED}),
    LifecycleObject.TRADE_DECISION: frozenset(
        {
            TradeDecisionState.SKIP,
            TradeDecisionState.NO_ENTRY,
            TradeDecisionState.SELECTED,
            TradeDecisionState.SELECTED_BLOCKED,
        }
    ),
    LifecycleObject.EXECUTION_INTENT: frozenset({ExecutionIntentState.PENDING}),
    LifecycleObject.ADMISSION: frozenset(
        {
            AdmissionState.APPROVED,
            AdmissionState.BLOCKED,
            AdmissionState.UNKNOWN,
        }
    ),
    LifecycleObject.EXECUTION_ATTEMPT: frozenset(
        {ExecutionAttemptState.PENDING_SUBMISSION}
    ),
    LifecycleObject.BROKER_ORDER: frozenset(
        {
            BrokerOrderState.WORKING,
            BrokerOrderState.PARTIALLY_FILLED,
            BrokerOrderState.FILLED,
            BrokerOrderState.REJECTED,
            BrokerOrderState.UNKNOWN,
        }
    ),
    LifecycleObject.POSITION: frozenset(
        {
            TradingPositionState.PENDING_OPEN,
            TradingPositionState.PARTIAL_OPEN,
            TradingPositionState.OPEN,
        }
    ),
    LifecycleObject.CLOSE_DECISION: frozenset(
        {
            CloseDecisionState.HOLD,
            CloseDecisionState.CLOSE_SELECTED,
            CloseDecisionState.BLOCKED,
            CloseDecisionState.UNKNOWN,
        }
    ),
    LifecycleObject.POSITION_CLOSE: frozenset(
        {
            PositionCloseState.RECORDED,
            PositionCloseState.PARTIAL,
            PositionCloseState.COMPLETE,
        }
    ),
    LifecycleObject.RECONCILIATION: frozenset(set(ReconciliationState)),
}


ALLOWED_TRANSITIONS: dict[LifecycleObject, dict[StrEnum, frozenset[StrEnum]]] = {
    LifecycleObject.TRADE_SIGNAL: {
        TradeSignalState.OBSERVED: frozenset(
            {TradeSignalState.READY, TradeSignalState.BLOCKED, TradeSignalState.EXPIRED}
        ),
        TradeSignalState.READY: frozenset(
            {
                TradeSignalState.STALE,
                TradeSignalState.CONSUMED,
                TradeSignalState.EXPIRED,
            }
        ),
        TradeSignalState.BLOCKED: frozenset(
            {TradeSignalState.STALE, TradeSignalState.EXPIRED, TradeSignalState.RETIRED}
        ),
        TradeSignalState.STALE: frozenset({TradeSignalState.RETIRED}),
        TradeSignalState.CONSUMED: frozenset({TradeSignalState.RETIRED}),
        TradeSignalState.EXPIRED: frozenset({TradeSignalState.RETIRED}),
        TradeSignalState.RETIRED: frozenset(),
    },
    LifecycleObject.TRADE_DECISION: {
        TradeDecisionState.SELECTED: frozenset({TradeDecisionState.SUPERSEDED}),
        TradeDecisionState.SKIP: frozenset({TradeDecisionState.SUPERSEDED}),
        TradeDecisionState.NO_ENTRY: frozenset({TradeDecisionState.SUPERSEDED}),
        TradeDecisionState.SELECTED_BLOCKED: frozenset(
            {TradeDecisionState.SUPERSEDED}
        ),
        TradeDecisionState.SUPERSEDED: frozenset(),
    },
    LifecycleObject.EXECUTION_INTENT: {
        ExecutionIntentState.PENDING: frozenset(
            {
                ExecutionIntentState.CLAIMED,
                ExecutionIntentState.EXPIRED,
                ExecutionIntentState.REVOKED,
                ExecutionIntentState.FAILED,
                ExecutionIntentState.SUPERSEDED,
            }
        ),
        ExecutionIntentState.CLAIMED: frozenset(
            {
                ExecutionIntentState.SUBMITTED,
                ExecutionIntentState.PARTIALLY_FILLED,
                ExecutionIntentState.FILLED,
                ExecutionIntentState.CANCELED,
                ExecutionIntentState.FAILED,
                ExecutionIntentState.REVOKED,
                ExecutionIntentState.EXPIRED,
            }
        ),
        ExecutionIntentState.SUBMITTED: frozenset(
            {
                ExecutionIntentState.SUBMITTED,
                ExecutionIntentState.PARTIALLY_FILLED,
                ExecutionIntentState.FILLED,
                ExecutionIntentState.CANCELED,
                ExecutionIntentState.FAILED,
                ExecutionIntentState.REVOKED,
                ExecutionIntentState.SUPERSEDED,
            }
        ),
        ExecutionIntentState.PARTIALLY_FILLED: frozenset(
            {
                ExecutionIntentState.FILLED,
                ExecutionIntentState.CANCELED,
                ExecutionIntentState.FAILED,
            }
        ),
        ExecutionIntentState.FILLED: frozenset(),
        ExecutionIntentState.FAILED: frozenset(),
        ExecutionIntentState.CANCELED: frozenset({ExecutionIntentState.SUPERSEDED}),
        ExecutionIntentState.REVOKED: frozenset(),
        ExecutionIntentState.EXPIRED: frozenset(),
        ExecutionIntentState.SUPERSEDED: frozenset(),
    },
    LifecycleObject.ADMISSION: {
        AdmissionState.APPROVED: frozenset(),
        AdmissionState.BLOCKED: frozenset(),
        AdmissionState.UNKNOWN: frozenset(),
    },
    LifecycleObject.EXECUTION_ATTEMPT: {
        ExecutionAttemptState.PENDING_SUBMISSION: frozenset(
            {
                ExecutionAttemptState.WORKING,
                ExecutionAttemptState.SUBMIT_UNKNOWN,
                ExecutionAttemptState.FAILED,
            }
        ),
        ExecutionAttemptState.SUBMIT_UNKNOWN: frozenset(
            {
                ExecutionAttemptState.WORKING,
                ExecutionAttemptState.SUBMIT_UNKNOWN,
                ExecutionAttemptState.FAILED,
            }
        ),
        ExecutionAttemptState.WORKING: frozenset(
            {
                ExecutionAttemptState.WORKING,
                ExecutionAttemptState.PARTIALLY_FILLED,
                ExecutionAttemptState.CANCELING,
                ExecutionAttemptState.FILLED,
                ExecutionAttemptState.CANCELED,
                ExecutionAttemptState.REJECTED,
                ExecutionAttemptState.EXPIRED,
                ExecutionAttemptState.FAILED,
                ExecutionAttemptState.STALE,
            }
        ),
        ExecutionAttemptState.PARTIALLY_FILLED: frozenset(
            {
                ExecutionAttemptState.PARTIALLY_FILLED,
                ExecutionAttemptState.CANCELING,
                ExecutionAttemptState.FILLED,
                ExecutionAttemptState.CANCELED,
                ExecutionAttemptState.FAILED,
            }
        ),
        ExecutionAttemptState.CANCELING: frozenset(
            {
                ExecutionAttemptState.WORKING,
                ExecutionAttemptState.FILLED,
                ExecutionAttemptState.CANCELED,
                ExecutionAttemptState.FAILED,
            }
        ),
        ExecutionAttemptState.STALE: frozenset(
            {
                ExecutionAttemptState.WORKING,
                ExecutionAttemptState.CANCELING,
                ExecutionAttemptState.FAILED,
            }
        ),
        ExecutionAttemptState.FILLED: frozenset(),
        ExecutionAttemptState.CANCELED: frozenset(),
        ExecutionAttemptState.REJECTED: frozenset(),
        ExecutionAttemptState.EXPIRED: frozenset(),
        ExecutionAttemptState.FAILED: frozenset(),
    },
    LifecycleObject.BROKER_ORDER: {
        BrokerOrderState.WORKING: frozenset(
            {
                BrokerOrderState.WORKING,
                BrokerOrderState.PARTIALLY_FILLED,
                BrokerOrderState.PENDING_CANCEL,
                BrokerOrderState.FILLED,
                BrokerOrderState.CANCELED,
                BrokerOrderState.REJECTED,
                BrokerOrderState.EXPIRED,
                BrokerOrderState.REPLACED,
                BrokerOrderState.UNKNOWN,
            }
        ),
        BrokerOrderState.PARTIALLY_FILLED: frozenset(
            {
                BrokerOrderState.PARTIALLY_FILLED,
                BrokerOrderState.PENDING_CANCEL,
                BrokerOrderState.FILLED,
                BrokerOrderState.CANCELED,
                BrokerOrderState.REJECTED,
                BrokerOrderState.EXPIRED,
            }
        ),
        BrokerOrderState.PENDING_CANCEL: frozenset(
            {
                BrokerOrderState.WORKING,
                BrokerOrderState.FILLED,
                BrokerOrderState.CANCELED,
                BrokerOrderState.REJECTED,
                BrokerOrderState.UNKNOWN,
            }
        ),
        BrokerOrderState.UNKNOWN: frozenset(
            {
                BrokerOrderState.WORKING,
                BrokerOrderState.PARTIALLY_FILLED,
                BrokerOrderState.FILLED,
                BrokerOrderState.CANCELED,
                BrokerOrderState.REJECTED,
                BrokerOrderState.EXPIRED,
                BrokerOrderState.REPLACED,
            }
        ),
        BrokerOrderState.FILLED: frozenset(),
        BrokerOrderState.CANCELED: frozenset(),
        BrokerOrderState.REJECTED: frozenset(),
        BrokerOrderState.EXPIRED: frozenset(),
        BrokerOrderState.REPLACED: frozenset(),
    },
    LifecycleObject.POSITION: {
        TradingPositionState.PENDING_OPEN: frozenset(
            {
                TradingPositionState.PARTIAL_OPEN,
                TradingPositionState.OPEN,
                TradingPositionState.CLOSED,
            }
        ),
        TradingPositionState.PARTIAL_OPEN: frozenset(
            {
                TradingPositionState.OPEN,
                TradingPositionState.PARTIAL_CLOSE,
                TradingPositionState.CLOSED,
            }
        ),
        TradingPositionState.OPEN: frozenset(
            {TradingPositionState.PARTIAL_CLOSE, TradingPositionState.CLOSED}
        ),
        TradingPositionState.PARTIAL_CLOSE: frozenset(
            {TradingPositionState.PARTIAL_CLOSE, TradingPositionState.CLOSED}
        ),
        TradingPositionState.CLOSED: frozenset(),
    },
    LifecycleObject.CLOSE_DECISION: {
        CloseDecisionState.HOLD: frozenset({CloseDecisionState.SUPERSEDED}),
        CloseDecisionState.CLOSE_SELECTED: frozenset({CloseDecisionState.SUPERSEDED}),
        CloseDecisionState.BLOCKED: frozenset({CloseDecisionState.SUPERSEDED}),
        CloseDecisionState.UNKNOWN: frozenset({CloseDecisionState.SUPERSEDED}),
        CloseDecisionState.SUPERSEDED: frozenset(),
    },
    LifecycleObject.POSITION_CLOSE: {
        PositionCloseState.RECORDED: frozenset(
            {PositionCloseState.PARTIAL, PositionCloseState.COMPLETE}
        ),
        PositionCloseState.PARTIAL: frozenset(
            {PositionCloseState.PARTIAL, PositionCloseState.COMPLETE}
        ),
        PositionCloseState.COMPLETE: frozenset(),
        PositionCloseState.VOIDED: frozenset(),
    },
    LifecycleObject.RECONCILIATION: {
        state: frozenset({ReconciliationState.REPAIRED, ReconciliationState.IGNORED})
        for state in ReconciliationState
        if state not in {
            ReconciliationState.MATCHED,
            ReconciliationState.REPAIRED,
            ReconciliationState.IGNORED,
        }
    }
    | {
        ReconciliationState.MATCHED: frozenset(),
        ReconciliationState.REPAIRED: frozenset(),
        ReconciliationState.IGNORED: frozenset(),
    },
}


STATE_ALIASES: dict[LifecycleObject, dict[str, StrEnum]] = {
    LifecycleObject.EXECUTION_INTENT: {
        "cancelled": ExecutionIntentState.CANCELED,
        "dispatching": ExecutionIntentState.CLAIMED,
    },
    LifecycleObject.EXECUTION_ATTEMPT: {
        "accepted": ExecutionAttemptState.WORKING,
        "accepted_for_bidding": ExecutionAttemptState.WORKING,
        "calculated": ExecutionAttemptState.WORKING,
        "held": ExecutionAttemptState.WORKING,
        "new": ExecutionAttemptState.WORKING,
        "pending_new": ExecutionAttemptState.WORKING,
        "pending_replace": ExecutionAttemptState.WORKING,
        "replaced": ExecutionAttemptState.WORKING,
        "stopped": ExecutionAttemptState.WORKING,
        "suspended": ExecutionAttemptState.WORKING,
        "pending_cancel": ExecutionAttemptState.CANCELING,
        "done_for_day": ExecutionAttemptState.EXPIRED,
        "cancelled": ExecutionAttemptState.CANCELED,
        "submitted": ExecutionAttemptState.WORKING,
    },
    LifecycleObject.BROKER_ORDER: {
        "accepted": BrokerOrderState.WORKING,
        "accepted_for_bidding": BrokerOrderState.WORKING,
        "calculated": BrokerOrderState.WORKING,
        "held": BrokerOrderState.WORKING,
        "new": BrokerOrderState.WORKING,
        "pending_new": BrokerOrderState.WORKING,
        "pending_replace": BrokerOrderState.WORKING,
        "stopped": BrokerOrderState.WORKING,
        "suspended": BrokerOrderState.WORKING,
        "done_for_day": BrokerOrderState.EXPIRED,
        "cancelled": BrokerOrderState.CANCELED,
        "submitted": BrokerOrderState.WORKING,
    },
    LifecycleObject.POSITION: {
        "partially_open": TradingPositionState.PARTIAL_OPEN,
        "partially_closed": TradingPositionState.PARTIAL_CLOSE,
    },
    LifecycleObject.CLOSE_DECISION: {
        "close": CloseDecisionState.CLOSE_SELECTED,
        "selected": CloseDecisionState.CLOSE_SELECTED,
        "skip": CloseDecisionState.HOLD,
        "no_close": CloseDecisionState.HOLD,
    },
    LifecycleObject.RECONCILIATION: {
        "clean": ReconciliationState.MATCHED,
        "ok": ReconciliationState.MATCHED,
        "missing_broker": ReconciliationState.BROKER_MISSING,
        "missing_local": ReconciliationState.LOCAL_MISSING,
        "mismatch": ReconciliationState.STATUS_MISMATCH,
        "submit_unknown": ReconciliationState.SUBMIT_UNKNOWN_UNRESOLVED,
    },
}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_lifecycle_object(value: LifecycleObject | str) -> LifecycleObject:
    if isinstance(value, LifecycleObject):
        return value
    rendered = _normalize_text(value)
    try:
        return LifecycleObject(rendered)
    except ValueError as exc:
        raise ValueError(f"unknown lifecycle object: {value!r}") from exc


def normalize_lifecycle_state(
    object_type: LifecycleObject | str,
    value: StrEnum | str,
) -> StrEnum:
    resolved_object = normalize_lifecycle_object(object_type)
    if isinstance(value, StrEnum):
        if value.__class__ is STATE_ENUM_BY_OBJECT[resolved_object]:
            return value
        value = value.value
    rendered = _normalize_text(value)
    aliases = STATE_ALIASES.get(resolved_object, {})
    if rendered in aliases:
        return aliases[rendered]
    state_enum = STATE_ENUM_BY_OBJECT[resolved_object]
    try:
        return state_enum(rendered)
    except ValueError as exc:
        raise ValueError(
            f"unknown {resolved_object.value} lifecycle state: {value!r}"
        ) from exc


def allowed_next_states(
    object_type: LifecycleObject | str,
    from_state: StrEnum | str | None,
) -> tuple[str, ...]:
    resolved_object = normalize_lifecycle_object(object_type)
    if from_state is None:
        return tuple(sorted(state.value for state in INITIAL_STATES[resolved_object]))
    normalized_from = normalize_lifecycle_state(resolved_object, from_state)
    allowed = ALLOWED_TRANSITIONS[resolved_object].get(normalized_from, frozenset())
    return tuple(sorted(state.value for state in allowed))


def is_terminal_lifecycle_state(
    object_type: LifecycleObject | str,
    state: StrEnum | str,
) -> bool:
    resolved_object = normalize_lifecycle_object(object_type)
    normalized_state = normalize_lifecycle_state(resolved_object, state)
    return normalized_state in TERMINAL_STATES[resolved_object]


def validate_lifecycle_transition(
    object_type: LifecycleObject | str,
    from_state: StrEnum | str | None,
    to_state: StrEnum | str,
    *,
    allow_idempotent: bool = True,
) -> LifecycleTransitionDecision:
    resolved_object = normalize_lifecycle_object(object_type)
    normalized_to = normalize_lifecycle_state(resolved_object, to_state)
    normalized_from = (
        None
        if from_state is None
        else normalize_lifecycle_state(resolved_object, from_state)
    )

    if normalized_from is None:
        allowed = normalized_to in INITIAL_STATES[resolved_object]
        allowed_next = tuple(
            sorted(state.value for state in INITIAL_STATES[resolved_object])
        )
    elif allow_idempotent and normalized_from == normalized_to:
        allowed = True
        allowed_next = allowed_next_states(resolved_object, normalized_from)
    else:
        allowed_set = ALLOWED_TRANSITIONS[resolved_object].get(
            normalized_from,
            frozenset(),
        )
        allowed = normalized_to in allowed_set
        allowed_next = tuple(sorted(state.value for state in allowed_set))

    if allowed:
        return LifecycleTransitionDecision(
            object_type=resolved_object,
            from_state=None if normalized_from is None else normalized_from.value,
            to_state=normalized_to.value,
            allowed=True,
            reason_code="transition_allowed",
            note=(
                f"{resolved_object.value} can transition from "
                f"{'<new>' if normalized_from is None else normalized_from.value} "
                f"to {normalized_to.value}."
            ),
            allowed_next_states=allowed_next,
        )

    return LifecycleTransitionDecision(
        object_type=resolved_object,
        from_state=None if normalized_from is None else normalized_from.value,
        to_state=normalized_to.value,
        allowed=False,
        reason_code="transition_not_allowed",
        note=(
            f"{resolved_object.value} cannot transition from "
            f"{'<new>' if normalized_from is None else normalized_from.value} "
            f"to {normalized_to.value}."
        ),
        allowed_next_states=allowed_next,
    )


def require_lifecycle_transition(
    object_type: LifecycleObject | str,
    from_state: StrEnum | str | None,
    to_state: StrEnum | str,
    *,
    allow_idempotent: bool = True,
) -> LifecycleTransitionDecision:
    decision = validate_lifecycle_transition(
        object_type,
        from_state,
        to_state,
        allow_idempotent=allow_idempotent,
    )
    if not decision.allowed:
        raise LifecycleTransitionError(decision)
    return decision


__all__ = [
    "AdmissionState",
    "BrokerOrderState",
    "CloseDecisionState",
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
    "allowed_next_states",
    "is_terminal_lifecycle_state",
    "normalize_lifecycle_object",
    "normalize_lifecycle_state",
    "require_lifecycle_transition",
    "validate_lifecycle_transition",
]

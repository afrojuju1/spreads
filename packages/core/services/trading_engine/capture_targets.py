from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.option_structures import normalize_legs, position_legs
from core.services.positions import enrich_position_row
from core.services.value_coercion import as_text

CAPTURE_OWNER_POSITION = "position"
CAPTURE_OWNER_WORKING_INTENT = "working_intent"
CAPTURE_OWNER_TRADE_DECISION = "trade_decision"
CAPTURE_OWNER_TRADE_SIGNAL = "trade_signal"

CAPTURE_REASON_OPEN_POSITION = "open_position"
CAPTURE_REASON_WORKING_INTENT = "working_intent"
CAPTURE_REASON_SELECTED_CANDIDATE = "selected_candidate"
CAPTURE_REASON_WATCH_CANDIDATE = "watch_candidate"

CAPTURE_PRIORITY_OPEN_POSITION = 10
CAPTURE_PRIORITY_WORKING_INTENT = 20
CAPTURE_PRIORITY_SELECTED_CANDIDATE = 30
CAPTURE_PRIORITY_WATCH_CANDIDATE = 40

OPEN_POSITION_CAPTURE_STATUSES = ("pending_open", "partial_open", "open", "partial_close")
WORKING_INTENT_STATES = ("pending", "claimed", "submitted", "partially_filled")
WATCH_SIGNAL_STATES = ("ready", "observed")
LEGACY_CAPTURE_OWNER_KINDS = (
    "live_session",
    "recovery_session",
    "trading_strategy",
    "execution_attempt",
    "session_position",
)

DEFAULT_ROLLING_TTL_SECONDS = 15 * 60
SELECTED_CANDIDATE_TTL_SECONDS = 5 * 60
WATCH_CANDIDATE_TTL_SECONDS = 2 * 60
DEFAULT_SELECTED_LIMIT = 50
DEFAULT_WATCH_LIMIT = 100


def utc_now_iso() -> datetime:
    return datetime.now(UTC)


def _expires_at(now: datetime, ttl_seconds: int) -> str:
    return (now + timedelta(seconds=max(int(ttl_seconds), 1))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_session_date(value: Any) -> str | None:
    rendered = as_text(value)
    if rendered is None:
        return None
    return rendered[:10]


def _source_legs(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    legs = normalize_legs(payload.get("legs"), expiration_date=as_text(payload.get("expiration_date")))
    if legs:
        return legs
    execution_shape = payload.get("execution_shape")
    if isinstance(execution_shape, Mapping):
        legs = normalize_legs(execution_shape.get("legs"), expiration_date=as_text(payload.get("expiration_date")))
        if legs:
            return legs
    option_symbol = as_text(payload.get("option_symbol"))
    if option_symbol is None:
        return []
    return normalize_legs(
        [
            {
                "symbol": option_symbol,
                "role": payload.get("leg_role") or "contract",
                "expiration_date": payload.get("expiration_date"),
            }
        ]
    )


def _capture_rows_from_legs(
    *,
    legs: Sequence[Mapping[str, Any]],
    underlying_symbol: str | None,
    strategy: str | None,
    priority: int,
    expires_at: str,
    metadata: Mapping[str, Any],
    quote_enabled: bool = True,
    trade_enabled: bool = True,
    feed: str = "opra",
    data_base_url: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for leg in normalize_legs(list(legs)):
        option_symbol = as_text(leg.get("symbol"))
        if option_symbol is None:
            continue
        rows.append(
            {
                "option_symbol": option_symbol,
                "underlying_symbol": underlying_symbol,
                "strategy": strategy,
                "leg_role": as_text(leg.get("role")) or "contract",
                "quote_enabled": quote_enabled,
                "trade_enabled": trade_enabled,
                "feed": feed,
                "data_base_url": data_base_url,
                "expires_at": expires_at,
                "priority": priority,
                "metadata": {
                    **dict(metadata),
                    "expiration_date": leg.get("expiration_date"),
                    "position_intent": leg.get("position_intent"),
                },
            }
        )
    return rows


def _position_capture_rows(position: Mapping[str, Any], *, now: datetime) -> list[dict[str, Any]]:
    payload = enrich_position_row(dict(position))
    return _capture_rows_from_legs(
        legs=position_legs(payload),
        underlying_symbol=as_text(payload.get("underlying_symbol")) or as_text(payload.get("root_symbol")),
        strategy=as_text(payload.get("strategy_family")) or as_text(payload.get("strategy")),
        priority=CAPTURE_PRIORITY_OPEN_POSITION,
        expires_at=_expires_at(now, DEFAULT_ROLLING_TTL_SECONDS),
        metadata={
            "source": "portfolio_position",
            "position_id": payload.get("position_id"),
            "position_status": payload.get("status"),
            "trading_strategy_id": payload.get("trading_strategy_id"),
        },
    )


def _attempt_capture_rows(attempt: Mapping[str, Any], *, now: datetime) -> list[dict[str, Any]]:
    return _capture_rows_from_legs(
        legs=_source_legs(attempt),
        underlying_symbol=as_text(attempt.get("underlying_symbol")),
        strategy=as_text(attempt.get("strategy")) or as_text(attempt.get("strategy_family")),
        priority=CAPTURE_PRIORITY_WORKING_INTENT,
        expires_at=_expires_at(now, DEFAULT_ROLLING_TTL_SECONDS),
        metadata={
            "source": "execution_attempt",
            "execution_attempt_id": attempt.get("execution_attempt_id"),
            "execution_intent_id": attempt.get("execution_intent_id"),
            "trade_signal_id": attempt.get("trade_signal_id"),
            "trade_decision_id": attempt.get("trade_decision_id"),
            "status": attempt.get("status"),
            "trade_intent": attempt.get("trade_intent"),
        },
    )


def _signal_capture_rows(
    *,
    signal: Mapping[str, Any],
    reason: str,
    priority: int,
    ttl_seconds: int,
    now: datetime,
    decision: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return _capture_rows_from_legs(
        legs=_source_legs(signal),
        underlying_symbol=as_text(signal.get("underlying_symbol")),
        strategy=as_text(signal.get("trade_structure")),
        priority=priority,
        expires_at=_expires_at(now, ttl_seconds),
        metadata={
            "source": reason,
            "trade_signal_id": signal.get("trade_signal_id"),
            "trade_decision_id": None if decision is None else decision.get("trade_decision_id"),
            "signal_state": signal.get("signal_state"),
            "decision_state": None if decision is None else decision.get("decision_state"),
            "trading_strategy_id": signal.get("trading_strategy_id"),
            "score": signal.get("score"),
            "rank": signal.get("rank"),
        },
        trade_enabled=(reason == CAPTURE_REASON_SELECTED_CANDIDATE),
    )


def _intent_signal_rows(
    *,
    intent: Mapping[str, Any],
    signal: Mapping[str, Any] | None,
    decision: Mapping[str, Any] | None,
    now: datetime,
) -> list[dict[str, Any]]:
    if signal is None:
        payload = intent.get("payload")
        signal = dict(payload) if isinstance(payload, Mapping) else {}
    rows = _signal_capture_rows(
        signal=signal,
        reason=CAPTURE_REASON_WORKING_INTENT,
        priority=CAPTURE_PRIORITY_WORKING_INTENT,
        ttl_seconds=DEFAULT_ROLLING_TTL_SECONDS,
        now=now,
        decision=decision,
    )
    for row in rows:
        row["metadata"] = {
            **dict(row.get("metadata") or {}),
            "source": "execution_intent",
            "execution_intent_id": intent.get("execution_intent_id"),
            "intent_state": intent.get("state"),
            "action_type": intent.get("action_type"),
        }
    return rows


def _replace_owner_targets(
    *,
    capture_store: Any,
    owner_kind: str,
    owner_key: str,
    reason: str,
    priority: int,
    rows: list[dict[str, Any]],
    session_id: str | None = None,
    session_date: str | None = None,
    label: str | None = None,
    profile: str | None = None,
) -> int:
    persisted = capture_store.replace_capture_targets(
        owner_kind=owner_kind,
        owner_key=owner_key,
        reason=reason,
        session_id=session_id,
        session_date=session_date,
        label=label,
        profile=profile,
        priority=priority,
        rows=rows,
    )
    return len(persisted)


def _count_by_reason(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(row.get("reason") or "unknown") for row in rows))


def refresh_engine_capture_targets(
    *,
    storage: Any,
    now: datetime | None = None,
    selected_limit: int = DEFAULT_SELECTED_LIMIT,
    watch_limit: int = DEFAULT_WATCH_LIMIT,
) -> dict[str, Any]:
    capture_store = storage.capture
    if not capture_store.target_schema_ready():
        return {"status": "skipped", "reason": "capture_schema_unavailable"}

    resolved_now = now or utc_now_iso()
    execution_store = storage.execution
    engine_facts = storage.engine_facts

    target_counts = {
        CAPTURE_REASON_OPEN_POSITION: 0,
        CAPTURE_REASON_WORKING_INTENT: 0,
        CAPTURE_REASON_SELECTED_CANDIDATE: 0,
        CAPTURE_REASON_WATCH_CANDIDATE: 0,
    }
    legacy_targets_deleted = sum(capture_store.delete_capture_targets(owner_kind=owner_kind) for owner_kind in LEGACY_CAPTURE_OWNER_KINDS)

    position_owner_keys: list[str] = []
    if execution_store.portfolio_schema_ready():
        for position in execution_store.list_positions(statuses=list(OPEN_POSITION_CAPTURE_STATUSES), limit=500):
            owner_key = str(position["position_id"])
            position_owner_keys.append(owner_key)
            target_counts[CAPTURE_REASON_OPEN_POSITION] += _replace_owner_targets(
                capture_store=capture_store,
                owner_kind=CAPTURE_OWNER_POSITION,
                owner_key=owner_key,
                reason=CAPTURE_REASON_OPEN_POSITION,
                priority=CAPTURE_PRIORITY_OPEN_POSITION,
                rows=_position_capture_rows(position, now=resolved_now),
                session_id=as_text(position.get("session_id")),
                session_date=_as_session_date(position.get("market_date_opened")),
                label=as_text(position.get("trading_strategy_id")),
            )
    capture_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_POSITION,
        active_owner_keys=position_owner_keys,
        reason=CAPTURE_REASON_OPEN_POSITION,
    )

    working_owner_keys: list[str] = []
    if execution_store.schema_ready():
        from core.services.execution.shared import OPEN_STATUSES

        for attempt in execution_store.list_attempts_by_status(statuses=sorted(OPEN_STATUSES), limit=500):
            owner_key = str(attempt["execution_attempt_id"])
            working_owner_keys.append(owner_key)
            target_counts[CAPTURE_REASON_WORKING_INTENT] += _replace_owner_targets(
                capture_store=capture_store,
                owner_kind=CAPTURE_OWNER_WORKING_INTENT,
                owner_key=owner_key,
                reason=CAPTURE_REASON_WORKING_INTENT,
                priority=CAPTURE_PRIORITY_WORKING_INTENT,
                rows=_attempt_capture_rows(attempt, now=resolved_now),
                session_id=as_text(attempt.get("session_id")),
                session_date=_as_session_date(attempt.get("session_date")),
                label=as_text(attempt.get("trading_strategy_id")) or as_text(attempt.get("label")),
            )

    if execution_store.intent_schema_ready() and engine_facts.schema_ready():
        for intent in execution_store.list_execution_intents(states=list(WORKING_INTENT_STATES), limit=500):
            owner_key = str(intent["execution_intent_id"])
            working_owner_keys.append(owner_key)
            trade_signal_id = as_text(intent.get("trade_signal_id"))
            trade_decision_id = as_text(intent.get("trade_decision_id"))
            signal = None if trade_signal_id is None else engine_facts.get_trade_signal(trade_signal_id)
            decision = None if trade_decision_id is None else engine_facts.get_trade_decision(trade_decision_id)
            target_counts[CAPTURE_REASON_WORKING_INTENT] += _replace_owner_targets(
                capture_store=capture_store,
                owner_kind=CAPTURE_OWNER_WORKING_INTENT,
                owner_key=owner_key,
                reason=CAPTURE_REASON_WORKING_INTENT,
                priority=CAPTURE_PRIORITY_WORKING_INTENT,
                rows=_intent_signal_rows(intent=intent, signal=signal, decision=decision, now=resolved_now),
                session_id=None,
                session_date=None,
                label=as_text(intent.get("trading_strategy_id")),
            )
    capture_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_WORKING_INTENT,
        active_owner_keys=working_owner_keys,
        reason=CAPTURE_REASON_WORKING_INTENT,
    )

    selected_owner_keys: list[str] = []
    watch_owner_keys: list[str] = []
    if engine_facts.schema_ready():
        now_iso = resolved_now.isoformat(timespec="seconds").replace("+00:00", "Z")
        selected_rows = engine_facts.list_trade_decisions_with_signals(
            decision_states=["selected"],
            routine="entry",
            as_of=now_iso,
            limit=selected_limit,
        )
        for row in selected_rows:
            decision = dict(row.get("trade_decision") or {})
            signal = dict(row.get("trade_signal") or {})
            owner_key = str(decision["trade_decision_id"])
            selected_owner_keys.append(owner_key)
            target_counts[CAPTURE_REASON_SELECTED_CANDIDATE] += _replace_owner_targets(
                capture_store=capture_store,
                owner_kind=CAPTURE_OWNER_TRADE_DECISION,
                owner_key=owner_key,
                reason=CAPTURE_REASON_SELECTED_CANDIDATE,
                priority=CAPTURE_PRIORITY_SELECTED_CANDIDATE,
                rows=_signal_capture_rows(
                    signal=signal,
                    decision=decision,
                    reason=CAPTURE_REASON_SELECTED_CANDIDATE,
                    priority=CAPTURE_PRIORITY_SELECTED_CANDIDATE,
                    ttl_seconds=SELECTED_CANDIDATE_TTL_SECONDS,
                    now=resolved_now,
                ),
                session_id=None,
                session_date=_as_session_date(signal.get("session_date")),
                label=as_text(signal.get("trading_strategy_id")),
            )

        selected_signal_ids = {as_text(dict(row.get("trade_signal") or {}).get("trade_signal_id")) for row in selected_rows}
        watch_rows = engine_facts.list_trade_signals(
            signal_states=list(WATCH_SIGNAL_STATES),
            routine="entry",
            as_of=now_iso,
            limit=watch_limit,
        )
        for signal in watch_rows:
            owner_key = str(signal["trade_signal_id"])
            if owner_key in selected_signal_ids:
                continue
            watch_owner_keys.append(owner_key)
            target_counts[CAPTURE_REASON_WATCH_CANDIDATE] += _replace_owner_targets(
                capture_store=capture_store,
                owner_kind=CAPTURE_OWNER_TRADE_SIGNAL,
                owner_key=owner_key,
                reason=CAPTURE_REASON_WATCH_CANDIDATE,
                priority=CAPTURE_PRIORITY_WATCH_CANDIDATE,
                rows=_signal_capture_rows(
                    signal=signal,
                    decision=None,
                    reason=CAPTURE_REASON_WATCH_CANDIDATE,
                    priority=CAPTURE_PRIORITY_WATCH_CANDIDATE,
                    ttl_seconds=WATCH_CANDIDATE_TTL_SECONDS,
                    now=resolved_now,
                ),
                session_id=None,
                session_date=_as_session_date(signal.get("session_date")),
                label=as_text(signal.get("trading_strategy_id")),
            )
    capture_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_TRADE_DECISION,
        active_owner_keys=selected_owner_keys,
        reason=CAPTURE_REASON_SELECTED_CANDIDATE,
    )
    capture_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_TRADE_SIGNAL,
        active_owner_keys=watch_owner_keys,
        reason=CAPTURE_REASON_WATCH_CANDIDATE,
    )

    active_targets = capture_store.list_active_capture_targets()
    return {
        "status": "ok",
        "target_counts": target_counts,
        "legacy_targets_deleted": legacy_targets_deleted,
        "active_target_count": len(active_targets),
        "active_target_counts": _count_by_reason(active_targets),
        "priority_order": [
            CAPTURE_REASON_OPEN_POSITION,
            CAPTURE_REASON_WORKING_INTENT,
            CAPTURE_REASON_SELECTED_CANDIDATE,
            CAPTURE_REASON_WATCH_CANDIDATE,
        ],
    }


__all__ = [
    "CAPTURE_REASON_OPEN_POSITION",
    "CAPTURE_REASON_SELECTED_CANDIDATE",
    "CAPTURE_REASON_WATCH_CANDIDATE",
    "CAPTURE_REASON_WORKING_INTENT",
    "refresh_engine_capture_targets",
]

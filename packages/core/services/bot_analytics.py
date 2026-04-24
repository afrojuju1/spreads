from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from core.services.bots import BotConfig, load_active_bots
from core.services.positions import enrich_position_row
from core.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionIntentEventModel,
    ExecutionIntentModel,
)
from core.storage.signal_models import OpportunityDecisionModel
from core.storage.serializers import parse_datetime, render_value

OPEN_POSITION_STATUSES = {"open", "partial_open", "partial_close"}
ENTRY_DECISION_AUDIT_SAMPLE_LIMIT = 12
ENTRY_DECISION_AUDIT_COUNT_KEYS = (
    "selected_count",
    "intent_created_count",
    "no_intent_count",
    "pending_dispatch_count",
    "submitted_working_count",
    "filled_count",
    "failed_count",
    "revoked_count",
    "expired_count",
    "canceled_count",
    "repriced_count",
    "selected_currently_admissible_count",
    "selected_currently_blocked_count",
    "blocked_by_buying_power_count",
    "blocked_by_policy_or_risk_budget_count",
    "row_count",
)
ENTRY_DECISION_AUDIT_BUCKET_TO_COUNT_KEY = {
    "no_intent": "no_intent_count",
    "pending_dispatch": "pending_dispatch_count",
    "submitted_working": "submitted_working_count",
    "filled": "filled_count",
    "failed": "failed_count",
    "revoked": "revoked_count",
    "expired": "expired_count",
    "canceled": "canceled_count",
}
ENTRY_DECISION_AUDIT_BUCKET_PRIORITY = {
    "no_intent": 0,
    "revoked": 1,
    "expired": 2,
    "failed": 3,
    "canceled": 4,
    "pending_dispatch": 5,
    "submitted_working": 6,
    "filled": 7,
}
BUYING_POWER_ADMISSION_REASONS = {
    "insufficient_broker_buying_power",
    "insufficient_buying_power",
    "insufficient_options_buying_power",
}
POLICY_OR_RISK_BUDGET_ADMISSION_REASONS = {
    "max_contracts_per_position_exceeded",
    "max_contracts_per_session_exceeded",
    "max_open_positions_per_session_exceeded",
    "max_open_positions_per_underlying_exceeded",
    "max_open_positions_per_underlying_strategy_exceeded",
    "max_position_notional_exceeded",
    "max_position_max_loss_exceeded",
    "max_session_notional_exceeded",
    "max_session_max_loss_exceeded",
    "strategy_risk_budget_exceeded",
    "max_risk_per_trade_exhausted",
}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coerce_int_value(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value in (None, ""):
        return None
    return parse_datetime(str(value))


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / float(len(values)), 2)


def _strategy_name_from_payload(
    *,
    policy_ref: Mapping[str, Any] | None,
    payload: Mapping[str, Any] | None,
) -> str:
    if isinstance(policy_ref, Mapping):
        strategy_id = _as_text(policy_ref.get("strategy_id"))
        if strategy_id is not None:
            return strategy_id
    if isinstance(payload, Mapping):
        opportunity = payload.get("opportunity")
        if isinstance(opportunity, Mapping):
            strategy_family = _as_text(opportunity.get("strategy_family"))
            if strategy_family is not None:
                return strategy_family
    return "unknown"


def _increment_counts(target: dict[str, int], key: str, amount: int = 1) -> None:
    target[key] = int(target.get(key) or 0) + int(amount)


def _update_execution_admission_counts(
    summary: dict[str, Any],
    execution_admission: Mapping[str, Any],
) -> None:
    status = _as_text(execution_admission.get("status"))
    reason = _as_text(execution_admission.get("reason"))
    if status == "admissible":
        _increment_counts(summary, "selected_currently_admissible_count")
        return
    if status != "blocked":
        return
    _increment_counts(summary, "selected_currently_blocked_count")
    if reason in BUYING_POWER_ADMISSION_REASONS:
        _increment_counts(summary, "blocked_by_buying_power_count")
    if reason in POLICY_OR_RISK_BUDGET_ADMISSION_REASONS:
        _increment_counts(summary, "blocked_by_policy_or_risk_budget_count")


def summarize_selected_execution_admission(
    *,
    decisions: list[dict[str, Any]],
    intents: list[dict[str, Any]],
) -> dict[str, int]:
    summary = {
        "selected_currently_admissible_count": 0,
        "selected_currently_blocked_count": 0,
        "blocked_by_buying_power_count": 0,
        "blocked_by_policy_or_risk_budget_count": 0,
    }
    selected_decision_ids = {
        str(row["opportunity_decision_id"])
        for row in decisions
        if str(row.get("state") or "") == "selected"
        and _as_text(row.get("opportunity_decision_id")) is not None
    }
    if not selected_decision_ids:
        return summary

    latest_intent_by_decision: dict[str, dict[str, Any]] = {}
    for intent in intents:
        opportunity_decision_id = _as_text(intent.get("opportunity_decision_id"))
        if opportunity_decision_id is None or opportunity_decision_id not in selected_decision_ids:
            continue
        current = latest_intent_by_decision.get(opportunity_decision_id)
        current_created_at = None if current is None else _coerce_datetime(current.get("created_at"))
        intent_created_at = _coerce_datetime(intent.get("created_at"))
        if current is None or (intent_created_at or datetime(1970, 1, 1, tzinfo=UTC)) >= (
            current_created_at or datetime(1970, 1, 1, tzinfo=UTC)
        ):
            latest_intent_by_decision[opportunity_decision_id] = dict(intent)

    for latest_intent in latest_intent_by_decision.values():
        _update_execution_admission_counts(
            summary,
            _intent_execution_admission(latest_intent),
        )
    return summary


def summarize_intent_counts(
    rows: list[tuple[str | None, str | None, int]],
) -> dict[str, Any]:
    intent_state_counts: Counter[str] = Counter()
    entry_intent_state_counts: Counter[str] = Counter()
    management_intent_state_counts: Counter[str] = Counter()
    for action_type, state, count in rows:
        normalized_state = str(state or "unknown")
        normalized_action_type = str(action_type or "").strip().lower()
        count_value = int(count)
        intent_state_counts.update({normalized_state: count_value})
        if normalized_action_type == "open":
            entry_intent_state_counts.update({normalized_state: count_value})
        elif normalized_action_type == "close":
            management_intent_state_counts.update({normalized_state: count_value})
    return {
        "intent_count": int(sum(intent_state_counts.values())),
        "intent_state_counts": dict(sorted(intent_state_counts.items())),
        "entry_intent_count": int(sum(entry_intent_state_counts.values())),
        "entry_intent_state_counts": dict(
            sorted(entry_intent_state_counts.items())
        ),
        "management_intent_count": int(sum(management_intent_state_counts.values())),
        "management_intent_state_counts": dict(
            sorted(management_intent_state_counts.items())
        ),
    }


def _empty_entry_decision_audit_summary() -> dict[str, Any]:
    return {
        **{key: 0 for key in ENTRY_DECISION_AUDIT_COUNT_KEYS},
        "sample_count": 0,
        "terminal_reason_counts": {},
    }


def _funnel_row(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "considered": 0,
        "selected": 0,
        "rejected": 0,
        "blocked": 0,
        "intents_created": 0,
        "submitted": 0,
        "repriced": 0,
        "canceled": 0,
        "failed": 0,
        "filled": 0,
        "blocker_reasons": {},
        "avg_decision_to_intent_seconds": None,
        "avg_intent_to_submit_seconds": None,
        "avg_submit_to_fill_seconds": None,
    }


def _finalize_funnel(
    row: dict[str, Any], *, timings: dict[str, list[float]]
) -> dict[str, Any]:
    considered = int(row.get("considered") or 0)
    selected = int(row.get("selected") or 0)
    intents_created = int(row.get("intents_created") or 0)
    filled = int(row.get("filled") or 0)
    row["selection_rate"] = (
        None if considered <= 0 else round(selected / float(considered), 4)
    )
    row["intent_rate"] = (
        None if selected <= 0 else round(intents_created / float(selected), 4)
    )
    row["fill_rate"] = (
        None if intents_created <= 0 else round(filled / float(intents_created), 4)
    )
    row["avg_decision_to_intent_seconds"] = _average(
        timings.get("decision_to_intent") or []
    )
    row["avg_intent_to_submit_seconds"] = _average(
        timings.get("intent_to_submit") or []
    )
    row["avg_submit_to_fill_seconds"] = _average(timings.get("submit_to_fill") or [])
    row["blocker_reasons"] = dict(sorted((row.get("blocker_reasons") or {}).items()))
    return row


def _load_entry_automation_context(
    *,
    signal_store: Any,
    execution_store: Any,
    bot_id: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, list[dict[str, Any]]]:
    decisions: list[dict[str, Any]] = []
    if signal_store.schema_ready():
        with signal_store.session_factory() as session:
            decision_models = list(
                session.scalars(
                    select(OpportunityDecisionModel)
                    .where(OpportunityDecisionModel.bot_id == bot_id)
                    .where(OpportunityDecisionModel.decided_at >= window_start)
                    .where(OpportunityDecisionModel.decided_at < window_end)
                ).all()
            )
        decisions = [
            {
                "opportunity_decision_id": str(row.opportunity_decision_id),
                "state": str(row.state or ""),
                "decided_at": row.decided_at,
                "policy_ref": dict(row.policy_ref_json or {}),
                "payload": dict(row.payload_json or {}),
                "reason_codes": list(row.reason_codes_json or []),
            }
            for row in decision_models
        ]

    intents: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    if execution_store.intent_schema_ready():
        with execution_store.session_factory() as session:
            intent_models = list(
                session.scalars(
                    select(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                    .where(ExecutionIntentModel.action_type == "open")
                ).all()
            )
            intent_ids = [str(intent.execution_intent_id) for intent in intent_models]
            attempt_ids = [
                str(intent.execution_attempt_id)
                for intent in intent_models
                if intent.execution_attempt_id is not None
            ]
            attempt_models = (
                []
                if not attempt_ids
                else list(
                    session.scalars(
                        select(ExecutionAttemptModel).where(
                            ExecutionAttemptModel.execution_attempt_id.in_(attempt_ids)
                        )
                    ).all()
                )
            )
            event_models = (
                []
                if not intent_ids
                else list(
                    session.scalars(
                        select(ExecutionIntentEventModel).where(
                            ExecutionIntentEventModel.execution_intent_id.in_(intent_ids)
                        )
                    ).all()
                )
            )
        intents = [
            {
                "execution_intent_id": str(row.execution_intent_id),
                "opportunity_decision_id": _as_text(row.opportunity_decision_id),
                "execution_attempt_id": _as_text(row.execution_attempt_id),
                "action_type": str(row.action_type or ""),
                "state": str(row.state or ""),
                "superseded_by_id": _as_text(row.superseded_by_id),
                "slot_key": str(row.slot_key or ""),
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "expires_at": row.expires_at,
                "policy_ref": dict(row.policy_ref_json or {}),
                "payload": dict(row.payload_json or {}),
            }
            for row in intent_models
        ]
        attempts = [
            {
                "execution_attempt_id": str(row.execution_attempt_id),
                "status": str(row.status or ""),
                "requested_at": row.requested_at,
                "submitted_at": row.submitted_at,
                "completed_at": row.completed_at,
                "error_text": row.error_text,
                "broker_order_id": row.broker_order_id,
                "position_id": row.position_id,
            }
            for row in attempt_models
        ]
        events = [
            {
                "execution_intent_id": str(row.execution_intent_id),
                "event_type": str(row.event_type or ""),
                "event_at": row.event_at,
                "payload": dict(row.payload_json or {}),
            }
            for row in event_models
        ]
    return {
        "decisions": decisions,
        "intents": intents,
        "attempts": attempts,
        "events": events,
    }


def _build_entry_funnel(
    *,
    decisions: list[dict[str, Any]],
    intents: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    overall = _funnel_row("overall")
    overall_timings: dict[str, list[float]] = defaultdict(list)
    strategy_rows: dict[str, dict[str, Any]] = {}
    strategy_timings: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    decisions_by_id = {
        str(row["opportunity_decision_id"]): dict(row) for row in decisions
    }
    attempts_by_id = {
        str(row["execution_attempt_id"]): dict(row)
        for row in attempts
        if _as_text(row.get("execution_attempt_id")) is not None
    }
    for decision in decisions:
        strategy_name = _strategy_name_from_payload(
            policy_ref=decision.get("policy_ref"),
            payload=decision.get("payload"),
        )
        row = strategy_rows.setdefault(strategy_name, _funnel_row(strategy_name))
        state = str(decision.get("state") or "")
        _increment_counts(overall, "considered")
        _increment_counts(row, "considered")
        if state in {"selected", "rejected", "blocked"}:
            _increment_counts(overall, state)
            _increment_counts(row, state)
        for reason in list(decision.get("reason_codes") or []):
            if state != "selected":
                _increment_counts(overall["blocker_reasons"], str(reason))
                _increment_counts(row["blocker_reasons"], str(reason))

    for intent in intents:
        strategy_name = _strategy_name_from_payload(
            policy_ref=intent.get("policy_ref"),
            payload=intent.get("payload"),
        )
        row = strategy_rows.setdefault(strategy_name, _funnel_row(strategy_name))
        _increment_counts(overall, "intents_created")
        _increment_counts(row, "intents_created")

        state = str(intent.get("state") or "")
        if state in {"submitted", "filled", "canceled", "failed"}:
            _increment_counts(overall, "submitted")
            _increment_counts(row, "submitted")
        if state == "canceled":
            _increment_counts(overall, "canceled")
            _increment_counts(row, "canceled")
        if state == "failed":
            _increment_counts(overall, "failed")
            _increment_counts(row, "failed")
        if state == "filled":
            _increment_counts(overall, "filled")
            _increment_counts(row, "filled")
        reprice_count = _coerce_int_value(
            (intent.get("payload") or {}).get("reprice_count")
        )
        if reprice_count > 0:
            _increment_counts(overall, "repriced")
            _increment_counts(row, "repriced")
        payload = dict(intent.get("payload") or {})
        for key in ("revoke_reason", "expire_reason", "error"):
            reason = _as_text(payload.get(key))
            if reason:
                _increment_counts(overall["blocker_reasons"], reason)
                _increment_counts(row["blocker_reasons"], reason)

        decision = None
        opportunity_decision_id = _as_text(intent.get("opportunity_decision_id"))
        if opportunity_decision_id is not None:
            decision = decisions_by_id.get(opportunity_decision_id)
        decision_at = None if decision is None else _coerce_datetime(decision.get("decided_at"))
        intent_created_at = _coerce_datetime(intent.get("created_at"))
        if decision_at is not None and intent_created_at is not None:
            decision_to_intent_seconds = max(
                (intent_created_at - decision_at).total_seconds(),
                0.0,
            )
            overall_timings["decision_to_intent"].append(decision_to_intent_seconds)
            strategy_timings[strategy_name]["decision_to_intent"].append(
                decision_to_intent_seconds
            )
        execution_attempt_id = _as_text(intent.get("execution_attempt_id"))
        attempt = (
            None
            if execution_attempt_id is None
            else attempts_by_id.get(execution_attempt_id)
        )
        attempt_submitted_at = (
            None if attempt is None else _coerce_datetime(attempt.get("submitted_at"))
        )
        if intent_created_at is not None and attempt_submitted_at is not None:
            intent_to_submit_seconds = max(
                (attempt_submitted_at - intent_created_at).total_seconds(),
                0.0,
            )
            overall_timings["intent_to_submit"].append(intent_to_submit_seconds)
            strategy_timings[strategy_name]["intent_to_submit"].append(
                intent_to_submit_seconds
            )
        attempt_completed_at = (
            None if attempt is None else _coerce_datetime(attempt.get("completed_at"))
        )
        if (
            attempt_submitted_at is not None
            and attempt_completed_at is not None
            and str(attempt.get("status") or "") == "filled"
        ):
            submit_to_fill_seconds = max(
                (attempt_completed_at - attempt_submitted_at).total_seconds(),
                0.0,
            )
            overall_timings["submit_to_fill"].append(submit_to_fill_seconds)
            strategy_timings[strategy_name]["submit_to_fill"].append(
                submit_to_fill_seconds
            )

    finalized_strategies = [
        _finalize_funnel(row, timings=strategy_timings.get(name, {}))
        for name, row in sorted(strategy_rows.items())
    ]
    return {
        "overall": _finalize_funnel(overall, timings=overall_timings),
        "strategies": finalized_strategies,
    }


def _decision_underlying_symbol(decision: Mapping[str, Any]) -> str | None:
    payload = decision.get("payload")
    if not isinstance(payload, Mapping):
        return None
    opportunity = payload.get("opportunity")
    if isinstance(opportunity, Mapping):
        return _as_text(opportunity.get("underlying_symbol"))
    return _as_text(payload.get("underlying_symbol"))


def _decision_opportunity_id(decision: Mapping[str, Any]) -> str | None:
    payload = decision.get("payload")
    if not isinstance(payload, Mapping):
        return None
    opportunity = payload.get("opportunity")
    if not isinstance(opportunity, Mapping):
        return None
    return _as_text(opportunity.get("opportunity_id"))


def _decision_terminal_reason(
    *,
    latest_intent: Mapping[str, Any] | None,
    latest_event: Mapping[str, Any] | None,
    latest_attempt: Mapping[str, Any] | None,
) -> str | None:
    if latest_intent is None:
        return "intent_not_created"
    payload = (
        latest_intent.get("payload")
        if isinstance(latest_intent.get("payload"), Mapping)
        else {}
    )
    for key in ("revoke_reason", "expire_reason", "error"):
        reason = _as_text(payload.get(key))
        if reason:
            return reason
    if latest_event is not None:
        event_payload = (
            latest_event.get("payload")
            if isinstance(latest_event.get("payload"), Mapping)
            else {}
        )
        for key in ("reason", "error"):
            reason = _as_text(event_payload.get(key))
            if reason:
                return reason
    if latest_attempt is not None:
        reason = _as_text(latest_attempt.get("error_text"))
        if reason:
            return reason
    return None


def _intent_execution_admission(
    latest_intent: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if latest_intent is None:
        return {}
    payload = (
        latest_intent.get("payload")
        if isinstance(latest_intent.get("payload"), Mapping)
        else {}
    )
    admission = (
        payload.get("execution_admission")
        if isinstance(payload.get("execution_admission"), Mapping)
        else {}
    )
    return dict(admission)


def _decision_outcome_bucket(
    *,
    latest_intent: Mapping[str, Any] | None,
) -> str:
    if latest_intent is None:
        return "no_intent"
    state = str(latest_intent.get("state") or "").strip().lower()
    if state == "filled":
        return "filled"
    if state == "failed":
        return "failed"
    if state == "revoked":
        return "revoked"
    if state == "expired":
        return "expired"
    if state == "canceled":
        return "canceled"
    if state in {"pending", "claimed"}:
        return "pending_dispatch"
    if state == "submitted":
        return "submitted_working"
    return "pending_dispatch"


def _entry_decision_audit_sort_key(row: Mapping[str, Any]) -> tuple[int, float]:
    decision_at = _coerce_datetime(row.get("decision_at")) or datetime(
        1970, 1, 1, tzinfo=UTC
    )
    bucket = str(row.get("outcome_bucket") or "")
    return (
        ENTRY_DECISION_AUDIT_BUCKET_PRIORITY.get(bucket, 99),
        -decision_at.timestamp(),
    )


def _build_entry_decision_audit(
    *,
    decisions: list[dict[str, Any]],
    intents: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
    events: list[dict[str, Any]],
    sample_limit: int = ENTRY_DECISION_AUDIT_SAMPLE_LIMIT,
) -> dict[str, Any]:
    summary = _empty_entry_decision_audit_summary()
    selected_decisions = [
        dict(row) for row in decisions if str(row.get("state") or "") == "selected"
    ]
    summary["selected_count"] = len(selected_decisions)
    summary["row_count"] = len(selected_decisions)
    if not selected_decisions:
        return {"summary": summary, "samples": []}

    intents_by_decision: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for intent in intents:
        opportunity_decision_id = _as_text(intent.get("opportunity_decision_id"))
        if opportunity_decision_id is None:
            continue
        intents_by_decision[opportunity_decision_id].append(dict(intent))
    attempts_by_id = {
        str(row["execution_attempt_id"]): dict(row)
        for row in attempts
        if _as_text(row.get("execution_attempt_id")) is not None
    }
    events_by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        execution_intent_id = _as_text(event.get("execution_intent_id"))
        if execution_intent_id is None:
            continue
        events_by_intent[execution_intent_id].append(dict(event))

    reason_counts: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    for decision in selected_decisions:
        opportunity_decision_id = str(decision["opportunity_decision_id"])
        ordered_intents = sorted(
            intents_by_decision.get(opportunity_decision_id, []),
            key=lambda row: _coerce_datetime(row.get("created_at"))
            or datetime(1970, 1, 1, tzinfo=UTC),
        )
        first_intent = None if not ordered_intents else ordered_intents[0]
        latest_intent = None if not ordered_intents else ordered_intents[-1]
        latest_attempt = (
            None
            if latest_intent is None
            else attempts_by_id.get(str(latest_intent.get("execution_attempt_id") or ""))
        )
        all_events = sorted(
            [
                event
                for intent in ordered_intents
                for event in events_by_intent.get(
                    str(intent.get("execution_intent_id") or ""),
                    [],
                )
            ],
            key=lambda row: _coerce_datetime(row.get("event_at"))
            or datetime(1970, 1, 1, tzinfo=UTC),
        )
        latest_event = None if not all_events else all_events[-1]
        reprice_count = max(
            [max(len(ordered_intents) - 1, 0)]
            + [
                _coerce_int_value((intent.get("payload") or {}).get("reprice_count"))
                for intent in ordered_intents
            ]
        )
        bucket = _decision_outcome_bucket(latest_intent=latest_intent)
        summary["intent_created_count"] += 1 if ordered_intents else 0
        count_key = ENTRY_DECISION_AUDIT_BUCKET_TO_COUNT_KEY.get(bucket)
        if count_key is not None:
            summary[count_key] += 1
        summary["repriced_count"] += 1 if reprice_count > 0 else 0

        decision_at = _coerce_datetime(decision.get("decided_at"))
        first_intent_at = (
            None if first_intent is None else _coerce_datetime(first_intent.get("created_at"))
        )
        latest_attempt_submitted_at = (
            None
            if latest_attempt is None
            else _coerce_datetime(latest_attempt.get("submitted_at"))
        )
        latest_terminal_at = (
            None
            if latest_attempt is None
            else _coerce_datetime(latest_attempt.get("completed_at"))
        )
        terminal_reason = _decision_terminal_reason(
            latest_intent=latest_intent,
            latest_event=latest_event,
            latest_attempt=latest_attempt,
        )
        execution_admission = _intent_execution_admission(latest_intent)
        _update_execution_admission_counts(summary, execution_admission)
        if terminal_reason:
            reason_counts.update([terminal_reason])
        rows.append(
            {
                "opportunity_decision_id": opportunity_decision_id,
                "opportunity_id": _decision_opportunity_id(decision),
                "underlying_symbol": _decision_underlying_symbol(decision),
                "strategy": _strategy_name_from_payload(
                    policy_ref=decision.get("policy_ref"),
                    payload=decision.get("payload"),
                ),
                "decision_at": render_value(decision.get("decided_at")),
                "decision_reason_codes": list(decision.get("reason_codes") or []),
                "intent_count": len(ordered_intents),
                "execution_intent_id": None
                if latest_intent is None
                else latest_intent.get("execution_intent_id"),
                "intent_state": None
                if latest_intent is None
                else latest_intent.get("state"),
                "dispatch_status": None
                if latest_intent is None
                else (latest_intent.get("payload") or {}).get("dispatch_status"),
                "latest_event_type": None
                if latest_event is None
                else latest_event.get("event_type"),
                "latest_event_at": None
                if latest_event is None
                else render_value(latest_event.get("event_at")),
                "execution_attempt_id": None
                if latest_attempt is None
                else latest_attempt.get("execution_attempt_id"),
                "attempt_status": None
                if latest_attempt is None
                else latest_attempt.get("status"),
                "reprice_count": reprice_count,
                "outcome_bucket": bucket,
                "terminal_reason": terminal_reason,
                "decision_to_intent_seconds": None
                if decision_at is None or first_intent_at is None
                else round(
                    max((first_intent_at - decision_at).total_seconds(), 0.0),
                    2,
                ),
                "intent_to_submit_seconds": None
                if first_intent_at is None or latest_attempt_submitted_at is None
                else round(
                    max(
                        (latest_attempt_submitted_at - first_intent_at).total_seconds(),
                        0.0,
                    ),
                    2,
                ),
                "submit_to_terminal_seconds": None
                if latest_attempt_submitted_at is None or latest_terminal_at is None
                else round(
                    max(
                        (latest_terminal_at - latest_attempt_submitted_at).total_seconds(),
                        0.0,
                    ),
                    2,
                ),
                "execution_admission_status": _as_text(
                    execution_admission.get("status")
                ),
                "execution_admission_reason": _as_text(
                    execution_admission.get("reason")
                ),
                "admissible_quantity": (
                    None
                    if execution_admission.get("admissible_quantity") in (None, "")
                    else _coerce_int_value(
                        execution_admission.get("admissible_quantity")
                    )
                ),
                "required_buying_power": (
                    None
                    if execution_admission.get("required_buying_power") in (None, "")
                    else round(
                        _coerce_float(execution_admission.get("required_buying_power")),
                        2,
                    )
                ),
                "available_buying_power": (
                    None
                    if execution_admission.get("available_buying_power") in (None, "")
                    else round(
                        _coerce_float(
                            execution_admission.get("available_buying_power")
                        ),
                        2,
                    )
                ),
            }
        )

    rows.sort(key=_entry_decision_audit_sort_key)
    summary["terminal_reason_counts"] = dict(sorted(reason_counts.items()))
    summary["sample_count"] = min(len(rows), max(int(sample_limit), 0))
    return {
        "summary": summary,
        "samples": rows[: max(int(sample_limit), 0)],
    }


def _aggregate_entry_decision_audit(
    bot_rows: list[dict[str, Any]],
    *,
    sample_limit: int = ENTRY_DECISION_AUDIT_SAMPLE_LIMIT,
) -> dict[str, Any]:
    summary = _empty_entry_decision_audit_summary()
    reason_counts: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    for row in bot_rows:
        audit = (
            row.get("entry_decision_audit")
            if isinstance(row.get("entry_decision_audit"), Mapping)
            else {}
        )
        audit_summary = (
            audit.get("summary") if isinstance(audit.get("summary"), Mapping) else {}
        )
        for key in ENTRY_DECISION_AUDIT_COUNT_KEYS:
            summary[key] += _coerce_int_value(audit_summary.get(key))
        reason_counts.update(
            {
                str(reason): _coerce_int_value(count)
                for reason, count in (
                    audit_summary.get("terminal_reason_counts") or {}
                ).items()
            }
        )
        for sample in list(audit.get("samples") or []):
            samples.append(
                {
                    "bot_id": row.get("bot_id"),
                    "bot_name": row.get("bot_name"),
                    **dict(sample),
                }
            )
    samples.sort(key=_entry_decision_audit_sort_key)
    summary["sample_count"] = min(len(samples), max(int(sample_limit), 0))
    summary["terminal_reason_counts"] = dict(sorted(reason_counts.items()))
    return {
        "summary": summary,
        "samples": samples[: max(int(sample_limit), 0)],
    }


def _window_bounds(market_date: str | None) -> tuple[str, datetime, datetime]:
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    window_start = datetime.fromisoformat(resolved_market_date).replace(tzinfo=UTC)
    window_end = window_start + timedelta(days=1)
    return resolved_market_date, window_start, window_end


def _bot_owned_positions(execution_store: Any, bot_id: str) -> list[dict[str, Any]]:
    if (
        not execution_store.portfolio_schema_ready()
        or not execution_store.intent_schema_ready()
    ):
        return []
    direct_positions = [
        enrich_position_row(dict(row))
        for row in execution_store.list_positions(bot_id=bot_id, limit=1000)
    ]
    if direct_positions:
        return direct_positions
    positions = [
        enrich_position_row(dict(row))
        for row in execution_store.list_positions(limit=1000)
    ]
    owned: list[dict[str, Any]] = []
    for position in positions:
        open_execution_attempt_id = _as_text(position.get("open_execution_attempt_id"))
        if open_execution_attempt_id is None:
            continue
        attempt = execution_store.get_attempt(open_execution_attempt_id)
        if attempt is None:
            continue
        request = (
            attempt.get("request")
            if isinstance(attempt.get("request"), Mapping)
            else {}
        )
        execution_intent_id = _as_text(request.get("execution_intent_id"))
        if execution_intent_id is None:
            continue
        intent = execution_store.get_execution_intent(execution_intent_id)
        if intent is None or str(intent.get("bot_id") or "") != bot_id:
            continue
        owned.append(position)
    return owned


def build_bot_metrics(
    *,
    storage: Any,
    bot_id: str,
    market_date: str | None = None,
) -> dict[str, Any]:
    resolved_market_date, window_start, window_end = _window_bounds(market_date)
    signal_store = storage.signals
    execution_store = storage.execution

    decision_state_counts: Counter[str] = Counter()
    if signal_store.schema_ready():
        with signal_store.session_factory() as session:
            rows = session.execute(
                select(OpportunityDecisionModel.state, func.count())
                .where(OpportunityDecisionModel.bot_id == bot_id)
                .where(OpportunityDecisionModel.decided_at >= window_start)
                .where(OpportunityDecisionModel.decided_at < window_end)
                .group_by(OpportunityDecisionModel.state)
            ).all()
            decision_state_counts.update(
                {str(state): int(count) for state, count in rows}
            )

    intent_summary = {
        "intent_count": 0,
        "intent_state_counts": {},
        "entry_intent_count": 0,
        "entry_intent_state_counts": {},
        "management_intent_count": 0,
        "management_intent_state_counts": {},
    }
    daily_action_count = 0
    daily_entry_fill_count = 0
    daily_close_fill_count = 0
    if execution_store.intent_schema_ready():
        with execution_store.session_factory() as session:
            rows = session.execute(
                select(
                    ExecutionIntentModel.action_type,
                    ExecutionIntentModel.state,
                    func.count(),
                )
                .where(ExecutionIntentModel.bot_id == bot_id)
                .where(ExecutionIntentModel.created_at >= window_start)
                .where(ExecutionIntentModel.created_at < window_end)
                .group_by(
                    ExecutionIntentModel.action_type,
                    ExecutionIntentModel.state,
                )
            ).all()
            intent_summary = summarize_intent_counts(
                [
                    (action_type, state, int(count))
                    for action_type, state, count in rows
                ]
            )
            daily_action_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                    .where(ExecutionIntentModel.state.notin_(["revoked", "expired"]))
                )
                or 0
            )
            daily_entry_fill_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.action_type == "open")
                    .where(ExecutionIntentModel.state == "filled")
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                )
                or 0
            )
            daily_close_fill_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(ExecutionIntentModel)
                    .where(ExecutionIntentModel.bot_id == bot_id)
                    .where(ExecutionIntentModel.action_type == "close")
                    .where(ExecutionIntentModel.state == "filled")
                    .where(ExecutionIntentModel.created_at >= window_start)
                    .where(ExecutionIntentModel.created_at < window_end)
                )
                or 0
            )

    positions = _bot_owned_positions(execution_store, bot_id)
    open_positions = [
        p for p in positions if str(p.get("status") or "") in OPEN_POSITION_STATUSES
    ]
    closed_positions = [
        p for p in positions if str(p.get("status") or "") not in OPEN_POSITION_STATUSES
    ]

    total_realized_pnl = sum(_coerce_float(p.get("realized_pnl")) for p in positions)
    open_unrealized_pnl = sum(
        _coerce_float(p.get("unrealized_pnl")) for p in open_positions
    )
    daily_realized_pnl = sum(
        _coerce_float(p.get("realized_pnl"))
        for p in positions
        if p.get("market_date_opened") == resolved_market_date
        or p.get("market_date_closed") == resolved_market_date
    )
    daily_total_pnl = daily_realized_pnl + open_unrealized_pnl

    closed_win_count = sum(
        1 for p in closed_positions if _coerce_float(p.get("realized_pnl")) > 0
    )
    closed_loss_count = sum(
        1 for p in closed_positions if _coerce_float(p.get("realized_pnl")) < 0
    )
    closed_decision_count = closed_win_count + closed_loss_count
    closed_win_rate = (
        None
        if closed_decision_count == 0
        else closed_win_count / float(closed_decision_count)
    )

    symbol_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "open_positions": 0,
            "closed_positions": 0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 0.0,
        }
    )
    for position in positions:
        symbol = str(
            position.get("underlying_symbol")
            or position.get("root_symbol")
            or "unknown"
        )
        if str(position.get("status") or "") in OPEN_POSITION_STATUSES:
            symbol_stats[symbol]["open_positions"] += 1
            symbol_stats[symbol]["unrealized_pnl"] += _coerce_float(
                position.get("unrealized_pnl")
            )
        else:
            symbol_stats[symbol]["closed_positions"] += 1
        symbol_stats[symbol]["realized_pnl"] += _coerce_float(
            position.get("realized_pnl")
        )
        symbol_stats[symbol]["net_pnl"] = (
            symbol_stats[symbol]["realized_pnl"]
            + symbol_stats[symbol]["unrealized_pnl"]
        )

    entry_context = _load_entry_automation_context(
        signal_store=signal_store,
        execution_store=execution_store,
        bot_id=bot_id,
        window_start=window_start,
        window_end=window_end,
    )
    entry_funnel = _build_entry_funnel(
        decisions=list(entry_context.get("decisions") or []),
        intents=list(entry_context.get("intents") or []),
        attempts=list(entry_context.get("attempts") or []),
    )
    entry_decision_audit = _build_entry_decision_audit(
        decisions=list(entry_context.get("decisions") or []),
        intents=list(entry_context.get("intents") or []),
        attempts=list(entry_context.get("attempts") or []),
        events=list(entry_context.get("events") or []),
    )

    return {
        "bot_id": bot_id,
        "market_date": resolved_market_date,
        "decision_count": int(sum(decision_state_counts.values())),
        "decision_state_counts": dict(sorted(decision_state_counts.items())),
        **intent_summary,
        "daily_action_count": daily_action_count,
        "daily_entry_fill_count": daily_entry_fill_count,
        "daily_close_fill_count": daily_close_fill_count,
        "position_count": len(positions),
        "open_position_count": len(open_positions),
        "closed_position_count": len(closed_positions),
        "daily_realized_pnl": round(daily_realized_pnl, 2),
        "open_unrealized_pnl": round(open_unrealized_pnl, 2),
        "daily_total_pnl": round(daily_total_pnl, 2),
        "total_realized_pnl": round(total_realized_pnl, 2),
        "net_total_pnl": round(total_realized_pnl + open_unrealized_pnl, 2),
        "closed_win_count": closed_win_count,
        "closed_loss_count": closed_loss_count,
        "closed_win_rate": closed_win_rate,
        "symbol_stats": dict(sorted(symbol_stats.items())),
        "entry_funnel": entry_funnel,
        "entry_decision_audit": entry_decision_audit,
    }


def build_automation_performance_summary(
    *,
    storage: Any,
    market_date: str | None = None,
) -> dict[str, Any]:
    bots = load_active_bots()
    bot_rows = [
        {
            "bot_id": bot.bot.bot_id,
            "bot_name": bot.bot.name,
            **build_bot_metrics(
                storage=storage, bot_id=bot.bot.bot_id, market_date=market_date
            ),
        }
        for bot in bots.values()
    ]
    return {
        "bot_count": len(bot_rows),
        "daily_total_pnl": round(
            sum(_coerce_float(row.get("daily_total_pnl")) for row in bot_rows), 2
        ),
        "open_unrealized_pnl": round(
            sum(_coerce_float(row.get("open_unrealized_pnl")) for row in bot_rows), 2
        ),
        "total_realized_pnl": round(
            sum(_coerce_float(row.get("total_realized_pnl")) for row in bot_rows), 2
        ),
        "daily_entry_fill_count": int(
            sum(int(row.get("daily_entry_fill_count") or 0) for row in bot_rows)
        ),
        "daily_close_fill_count": int(
            sum(int(row.get("daily_close_fill_count") or 0) for row in bot_rows)
        ),
        "entry_funnel": {
            "overall": _finalize_funnel(
                {
                    **_funnel_row("overall"),
                    **{
                        key: sum(
                            int(
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get(key)
                                or 0
                            )
                            for row in bot_rows
                        )
                        for key in [
                            "considered",
                            "selected",
                            "rejected",
                            "blocked",
                            "intents_created",
                            "submitted",
                            "repriced",
                            "canceled",
                            "failed",
                            "filled",
                        ]
                    },
                    "blocker_reasons": dict(
                        Counter(
                            reason
                            for row in bot_rows
                            for reason, count in (
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("blocker_reasons")
                                or {}
                            ).items()
                            for _ in range(int(count))
                        )
                    ),
                },
                timings={
                    "decision_to_intent": [
                        float(value)
                        for row in bot_rows
                        for value in (
                            [
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("avg_decision_to_intent_seconds")
                            ]
                            if (
                                (row.get("entry_funnel") or {}).get("overall") or {}
                            ).get("avg_decision_to_intent_seconds")
                            is not None
                            else []
                        )
                    ],
                    "intent_to_submit": [
                        float(value)
                        for row in bot_rows
                        for value in (
                            [
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("avg_intent_to_submit_seconds")
                            ]
                            if (
                                (row.get("entry_funnel") or {}).get("overall") or {}
                            ).get("avg_intent_to_submit_seconds")
                            is not None
                            else []
                        )
                    ],
                    "submit_to_fill": [
                        float(value)
                        for row in bot_rows
                        for value in (
                            [
                                (
                                    (row.get("entry_funnel") or {}).get("overall") or {}
                                ).get("avg_submit_to_fill_seconds")
                            ]
                            if (
                                (row.get("entry_funnel") or {}).get("overall") or {}
                            ).get("avg_submit_to_fill_seconds")
                            is not None
                            else []
                        )
                    ],
                },
            ),
            "bots": [
                {
                    "bot_id": row.get("bot_id"),
                    "bot_name": row.get("bot_name"),
                    **dict(row.get("entry_funnel") or {}),
                }
                for row in bot_rows
            ],
        },
        "entry_decision_audit": _aggregate_entry_decision_audit(bot_rows),
        "bots": bot_rows,
    }


def evaluate_entry_controls(
    *,
    storage: Any,
    bot: BotConfig,
    market_date: str | None = None,
) -> tuple[bool, str | None, dict[str, Any]]:
    metrics = build_bot_metrics(
        storage=storage, bot_id=bot.bot_id, market_date=market_date
    )
    if (
        bot.max_open_positions
        and int(metrics.get("open_position_count") or 0) >= bot.max_open_positions
    ):
        return False, "max_open_positions_reached", metrics
    if (
        bot.max_daily_actions
        and int(metrics.get("daily_action_count") or 0) >= bot.max_daily_actions
    ):
        return False, "max_daily_actions_reached", metrics
    if (
        bot.max_new_entries_per_day is not None
        and int(metrics.get("daily_entry_fill_count") or 0)
        >= bot.max_new_entries_per_day
    ):
        return False, "max_new_entries_per_day_reached", metrics
    if bot.daily_loss_limit is not None and float(
        metrics.get("daily_total_pnl") or 0.0
    ) <= -abs(float(bot.daily_loss_limit)):
        return False, "daily_loss_limit_reached", metrics
    return True, None, metrics


__all__ = [
    "build_automation_performance_summary",
    "build_bot_metrics",
    "evaluate_entry_controls",
]

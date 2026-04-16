from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from spreads.db.decorators import with_storage
from spreads.services.execution import (
    submit_opportunity_execution,
    submit_position_close_by_id,
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


@with_storage()
def submit_execution_intent(
    *,
    db_target: str,
    execution_intent_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    signal_store = storage.signals
    if not execution_store.intent_schema_ready():
        raise ValueError("Execution intent tables are not available yet.")
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
    state = str(intent.get("state") or "")
    if state not in {"pending", "claimed"}:
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": "Execution intent is no longer pending submission.",
            "execution_intent": intent,
        }

    claim_token = str(intent.get("claim_token") or uuid4().hex)
    claimed_intent = execution_store.upsert_execution_intent(
        execution_intent_id=str(intent["execution_intent_id"]),
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=intent.get("opportunity_decision_id"),
        strategy_position_id=intent.get("strategy_position_id"),
        execution_attempt_id=intent.get("execution_attempt_id"),
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=claim_token,
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state="claimed",
        expires_at=intent.get("expires_at"),
        superseded_by_id=intent.get("superseded_by_id"),
        payload=dict(intent.get("payload") or {}),
        created_at=str(intent["created_at"]),
        updated_at=_utc_now(),
    )
    execution_store.append_execution_intent_event(
        execution_intent_id=execution_intent_id,
        event_type="claimed",
        event_at=_utc_now(),
        payload={"claim_token": claim_token},
    )

    if intent.get("opportunity_decision_id"):
        decision = signal_store.get_opportunity_decision(
            str(intent["opportunity_decision_id"])
        )
        if decision is None:
            raise ValueError(
                f"Missing opportunity decision for execution intent {execution_intent_id}"
            )
        result = submit_opportunity_execution(
            db_target=db_target,
            opportunity_id=str(decision["opportunity_id"]),
            request_metadata={
                "execution_intent_id": execution_intent_id,
                "bot_id": intent.get("bot_id"),
                "automation_id": intent.get("automation_id"),
            },
            storage=storage,
        )
    elif intent.get("strategy_position_id"):
        result = submit_position_close_by_id(
            db_target=db_target,
            position_id=str(intent["strategy_position_id"]),
            request_metadata={
                "execution_intent_id": execution_intent_id,
                "bot_id": intent.get("bot_id"),
                "automation_id": intent.get("automation_id"),
            },
            storage=storage,
        )
    else:
        raise ValueError(
            f"Execution intent {execution_intent_id} is missing its source reference"
        )

    attempt = result.get("attempt") if isinstance(result.get("attempt"), dict) else None
    linked_attempt_id = (
        None
        if attempt is None
        else str(attempt.get("execution_attempt_id") or "") or None
    )
    linked_intent = execution_store.upsert_execution_intent(
        execution_intent_id=str(claimed_intent["execution_intent_id"]),
        bot_id=str(claimed_intent["bot_id"]),
        automation_id=str(claimed_intent["automation_id"]),
        opportunity_decision_id=claimed_intent.get("opportunity_decision_id"),
        strategy_position_id=claimed_intent.get("strategy_position_id"),
        execution_attempt_id=linked_attempt_id,
        action_type=str(claimed_intent["action_type"]),
        slot_key=str(claimed_intent["slot_key"]),
        claim_token=str(claimed_intent.get("claim_token") or claim_token),
        policy_ref=dict(claimed_intent.get("policy_ref") or {}),
        config_hash=str(claimed_intent.get("config_hash") or ""),
        state="claimed",
        expires_at=claimed_intent.get("expires_at"),
        superseded_by_id=claimed_intent.get("superseded_by_id"),
        payload={
            **dict(claimed_intent.get("payload") or {}),
            **(
                {}
                if linked_attempt_id is None
                else {"execution_attempt_id": linked_attempt_id}
            ),
        },
        created_at=str(claimed_intent["created_at"]),
        updated_at=_utc_now(),
    )
    if linked_attempt_id is not None:
        execution_store.append_execution_intent_event(
            execution_intent_id=execution_intent_id,
            event_type="execution_attempt_linked",
            event_at=_utc_now(),
            payload={"execution_attempt_id": linked_attempt_id},
        )
    return {
        "action": "submit_execution_intent",
        "changed": True,
        "result": result,
        "execution_intent": linked_intent,
    }


__all__ = ["submit_execution_intent"]

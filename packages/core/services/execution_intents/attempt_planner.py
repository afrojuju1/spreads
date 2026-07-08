from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.engine import EngineAggregateType, EngineEvent, EngineEventType
from core.services.execution.attempts import _get_attempt_payload, _sync_linked_execution_intent
from core.services.execution.direct_orders import submit_option_order, submit_option_structure_order
from core.services.execution.position_close import submit_position_close_by_id
from core.services.execution_intents.shared import _append_event, _intent_action_type, _intent_payload
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int


def _nested_mapping(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _nested_list(*values: Any) -> list[dict[str, Any]]:
    for value in values:
        if isinstance(value, list):
            rows = [dict(item) for item in value if isinstance(item, Mapping)]
            if rows:
                return rows
    return []


def _resolved_quantity(payload: Mapping[str, Any], order_payload: Mapping[str, Any]) -> int:
    value = coerce_int(payload.get("quantity"))
    if value is None:
        value = coerce_int(order_payload.get("qty"))
    if value is None:
        value = 1
    return max(int(value), 1)


def _resolved_limit_price(
    payload: Mapping[str, Any],
    order_payload: Mapping[str, Any],
    economics: Mapping[str, Any],
) -> float:
    candidates = (
        payload.get("limit_price"),
        order_payload.get("limit_price"),
        economics.get("limit_price"),
        economics.get("midpoint_credit"),
        economics.get("midpoint_debit"),
        economics.get("midpoint_value"),
        economics.get("net_credit"),
        economics.get("net_debit"),
    )
    for candidate in candidates:
        value = coerce_float(candidate)
        if value is not None and value > 0:
            return float(value)
    raise ValueError("Execution intent payload is missing a positive limit_price")


def _intent_metadata(intent: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = {
        "execution_intent_id": str(intent["execution_intent_id"]),
        "trading_strategy_id": as_text(intent.get("trading_strategy_id")),
        "trade_signal_id": as_text(intent.get("trade_signal_id")),
        "trade_decision_id": as_text(intent.get("trade_decision_id")),
        "config_hash": as_text(intent.get("config_hash")),
        "trade_structure": as_text(payload.get("trade_structure")),
        "strategy_family": as_text(payload.get("strategy_family")) or as_text(payload.get("trade_structure")),
        "underlying_symbol": as_text(payload.get("underlying_symbol")) or as_text(payload.get("root_symbol")),
        "validation_provenance": as_text(payload.get("validation_provenance")) or "workflow_lifecycle",
        "execution_mode": as_text(payload.get("execution_mode")),
        "approval_mode": as_text(payload.get("approval_mode")),
        "execution_runtime": as_text(payload.get("execution_runtime")),
        "profile": as_text(payload.get("profile")),
        "source": as_mapping(payload.get("source")),
        "execution_policy": as_mapping(payload.get("execution_policy")),
        "executor_profile": as_mapping(payload.get("executor_profile")),
        "repricing_policy": as_mapping(payload.get("repricing_policy")),
        "risk_policy": as_mapping(payload.get("risk_policy")),
        "exit_policy": as_mapping(payload.get("exit_policy")),
        "execution_admission": as_mapping(payload.get("execution_admission")),
        "option_selection": as_mapping(payload.get("option_selection")),
    }
    return {key: value for key, value in metadata.items() if value not in (None, {}, [])}


def _append_attempt_engine_event(
    storage: Any,
    *,
    intent: Mapping[str, Any],
    attempt: Mapping[str, Any],
    event_type: str,
    workflow_id: str | None,
    payload: dict[str, Any],
) -> None:
    engine_events = getattr(storage, "engine_events", None)
    if engine_events is None or not engine_events.schema_ready():
        return
    execution_attempt_id = str(attempt["execution_attempt_id"])
    broker_order_id = as_text(attempt.get("broker_order_id"))
    engine_events.append_engine_event(
        EngineEvent(
            event_type=event_type,
            aggregate_type=EngineAggregateType.EXECUTION_ATTEMPT,
            aggregate_id=execution_attempt_id,
            lifecycle_object="execution_attempt",
            to_state=as_text(attempt.get("status")),
            trading_strategy_id=as_text(attempt.get("trading_strategy_id")) or as_text(intent.get("trading_strategy_id")),
            trade_signal_id=as_text(intent.get("trade_signal_id")),
            trade_decision_id=as_text(intent.get("trade_decision_id")),
            execution_intent_id=str(intent["execution_intent_id"]),
            execution_attempt_id=execution_attempt_id,
            broker_order_id=broker_order_id,
            position_id=as_text(attempt.get("position_id")) or as_text(intent.get("strategy_position_id")),
            workflow_id=workflow_id,
            correlation_id=str(intent["execution_intent_id"]),
            idempotency_key=f"{event_type}:{execution_attempt_id}:{broker_order_id or 'local'}:{as_text(attempt.get('status')) or 'unknown'}",
            payload=payload,
            occurred_at=datetime.now(UTC),
        )
    )


def _existing_attempt_result(
    *,
    execution_store: Any,
    intent: Mapping[str, Any],
    execution_attempt_id: str,
) -> dict[str, Any] | None:
    if execution_store.get_attempt(execution_attempt_id) is None:
        return None
    attempt_payload = _get_attempt_payload(execution_store, execution_attempt_id)
    return {
        "status": "exists",
        "changed": False,
        "execution_intent_id": str(intent["execution_intent_id"]),
        "execution_attempt_id": execution_attempt_id,
        "attempt": attempt_payload,
    }


@with_storage()
def ensure_execution_attempt_for_intent(
    *,
    db_target: str,
    execution_intent_id: str,
    workflow_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.intent_schema_ready() or not execution_store.schema_ready():
        raise RuntimeError("Execution intent and attempt schema are required for broker workflow activities.")
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
    payload = _intent_payload(dict(intent))
    existing_attempt_id = as_text(intent.get("execution_attempt_id")) or as_text(payload.get("execution_attempt_id"))
    if existing_attempt_id is not None:
        existing = _existing_attempt_result(
            execution_store=execution_store,
            intent=intent,
            execution_attempt_id=existing_attempt_id,
        )
        if existing is not None:
            _append_attempt_engine_event(
                storage,
                intent=intent,
                attempt=existing["attempt"],
                event_type=EngineEventType.BROKER_SUBMISSION_REQUESTED,
                workflow_id=workflow_id,
                payload={"status": "attempt_already_prepared"},
            )
            return existing

    metadata = _intent_metadata(intent, payload)
    action_type = _intent_action_type(dict(intent))
    if action_type == "close":
        position_id = as_text(intent.get("strategy_position_id")) or as_text(payload.get("position_id"))
        if position_id is None:
            raise ValueError(f"Close lifecycle intent {execution_intent_id} is missing position_id")
        result = submit_position_close_by_id(
            db_target=db_target,
            position_id=position_id,
            quantity=coerce_int(payload.get("quantity")),
            limit_price=coerce_float(payload.get("limit_price")),
            request_metadata=metadata,
            storage=storage,
        )
    else:
        execution_shape = _nested_mapping(payload.get("execution_shape"))
        order_payload = _nested_mapping(payload.get("order_payload"), execution_shape.get("order_payload"))
        economics = _nested_mapping(payload.get("economics"), execution_shape.get("economics"))
        candidate = _nested_mapping(payload.get("candidate"), payload.get("candidate_payload"))
        legs = _nested_list(payload.get("legs"), execution_shape.get("legs"), order_payload.get("legs"), candidate.get("legs"))
        limit_price = _resolved_limit_price(payload, order_payload, economics)
        quantity = _resolved_quantity(payload, order_payload)
        underlying_symbol = (
            as_text(payload.get("underlying_symbol"))
            or as_text(payload.get("root_symbol"))
            or as_text(candidate.get("underlying_symbol"))
            or as_text(candidate.get("root_symbol"))
            or as_text(metadata.get("underlying_symbol"))
        )
        if legs:
            result = submit_option_structure_order(
                db_target=db_target,
                legs=legs,
                quantity=quantity,
                limit_price=limit_price,
                order_payload=order_payload,
                label=as_text(intent.get("trading_strategy_id")) or "strategy_option",
                market_date=as_text(payload.get("market_date")) or as_text(payload.get("session_date")),
                underlying_symbol=underlying_symbol,
                strategy_family=as_text(payload.get("strategy_family")) or as_text(payload.get("trade_structure")),
                expiration_date=as_text(payload.get("expiration_date")),
                execution_runtime=as_text(payload.get("execution_runtime")),
                request_metadata=metadata,
                economics=dict(economics),
                candidate=dict(candidate),
                queue_submission=True,
                storage=storage,
            )
        else:
            symbol = as_text(payload.get("symbol")) or as_text(order_payload.get("symbol"))
            side = as_text(payload.get("side")) or as_text(order_payload.get("side"))
            if symbol is None or side is None:
                raise ValueError("Execution intent payload is missing canonical option legs or a single option symbol/side.")
            result = submit_option_order(
                db_target=db_target,
                symbol=symbol,
                side=side,
                quantity=quantity,
                limit_price=limit_price,
                time_in_force=as_text(order_payload.get("time_in_force")) or "day",
                label=as_text(intent.get("trading_strategy_id")) or "strategy_option",
                market_date=as_text(payload.get("market_date")) or as_text(payload.get("session_date")),
                underlying_symbol=underlying_symbol,
                strategy_family=as_text(payload.get("strategy_family")) or as_text(payload.get("trade_structure")) or "long_call",
                expiration_date=as_text(payload.get("expiration_date")),
                option_type=as_text(payload.get("option_type")),
                strike=coerce_float(payload.get("strike")),
                execution_runtime=as_text(payload.get("execution_runtime")),
                request_metadata=metadata,
                queue_submission=True,
                storage=storage,
            )

    attempt = dict(result.get("attempt") or {})
    if not attempt:
        raise RuntimeError(f"Execution intent {execution_intent_id} did not produce an execution attempt.")
    _sync_linked_execution_intent(
        execution_store=execution_store,
        attempt=attempt,
        event_type="attempt_prepared",
        message=str(result.get("message") or "Execution attempt prepared for broker workflow."),
        payload_updates={
            "broker_activity_status": "attempt_prepared",
            "workflow_id": workflow_id,
        },
    )
    _append_event(
        execution_store,
        execution_intent_id=execution_intent_id,
        event_type="attempt_prepared",
        payload={
            "execution_attempt_id": attempt.get("execution_attempt_id"),
            "workflow_id": workflow_id,
            "message": result.get("message"),
        },
    )
    _append_attempt_engine_event(
        storage,
        intent=intent,
        attempt=attempt,
        event_type=EngineEventType.BROKER_SUBMISSION_REQUESTED,
        workflow_id=workflow_id,
        payload={"status": "attempt_prepared", "message": result.get("message")},
    )
    return {
        "status": "prepared",
        "changed": bool(result.get("changed", True)),
        "execution_intent_id": execution_intent_id,
        "execution_attempt_id": str(attempt["execution_attempt_id"]),
        "attempt": attempt,
        "message": result.get("message"),
    }


__all__ = ["ensure_execution_attempt_for_intent"]

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.integrations.alpaca.client import AlpacaRequestError
from core.integrations.alpaca.errors import classify_alpaca_request_error
from core.services.candidate_policy import resolve_candidate_profile
from core.services.execution_lifecycle import (
    PENDING_SUBMISSION_STATUS,
)
from core.services.session_positions import (
    OPEN_TRADE_INTENT,
)
from core.services.value_coercion import (
    as_text,
    utc_now_iso,
)
from .alpaca_adapter import create_alpaca_order_adapter
from .attempts import (
    _get_attempt_payload,
    _publish_execution_attempt_event,
    _require_execution_schema,
    _submission_message,
    _sync_attempt_state,
    _sync_linked_execution_intent,
)
from .policy import (
    _validate_open_timing_window,
)
from .runtimes import (
    execution_runtime_from_request,
)
from .shared import (
    _is_terminal_status,
    _resolve_completed_at,
)

from .admission import (
    _execution_admission_payload_from_account_capacity,
    _execution_admission_payload_from_broker_rejection,
    _validate_submit_account_capacity,
)
from .order_requests import _normalize_submit_order_request, _validate_live_deployment_quality


@with_storage()
def run_execution_submit(
    *,
    db_target: str,
    execution_attempt_id: str,
    heartbeat: Any | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")

    payload = _get_attempt_payload(execution_store, execution_attempt_id)
    broker_order_id = as_text(payload.get("broker_order_id"))
    status = str(payload.get("status") or "")
    if broker_order_id is not None or status != PENDING_SUBMISSION_STATUS:
        return {
            "status": "skipped",
            "reason": "attempt_already_submitted",
            "execution_attempt_id": execution_attempt_id,
            "attempt_status": status,
            "broker_order_id": broker_order_id,
        }

    request = dict(payload.get("request") or {})
    execution_runtime_from_request(request)
    order_request = request.get("order")
    if not isinstance(order_request, dict) or not order_request:
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=utc_now_iso(),
            error_text="Execution attempt is missing its broker order payload.",
            position_id=as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message="Execution failed before submission: missing broker order payload.",
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            state="failed",
            event_type="failed",
            message="Execution failed before submission: missing broker order payload.",
        )
        raise ValueError("Execution attempt is missing its broker order payload.")

    requested_at = as_text(payload.get("requested_at")) or utc_now_iso()
    client_order_id = as_text(payload.get("client_order_id"))

    if str(payload.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
        request_execution_policy = request.get("execution_policy") if isinstance(request.get("execution_policy"), Mapping) else {}
        timing_gate = _validate_open_timing_window(
            exit_policy=request.get("exit_policy"),
            current_time=datetime.now(UTC),
            profile=resolve_candidate_profile(dict(payload.get("candidate") or {})),
            deployment_mode=str(request_execution_policy.get("deployment_mode") or ""),
        )
        if not timing_gate["allowed"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=utc_now_iso(),
                error_text=str(timing_gate["message"]),
                position_id=as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {timing_gate['message']}",
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=f"Execution failed before submission: {timing_gate['message']}",
            )
            return {
                "status": "blocked",
                "reason": str(timing_gate["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(timing_gate["message"]),
                "attempt": failed_attempt,
            }

    if callable(heartbeat):
        heartbeat()
    adapter = create_alpaca_order_adapter()
    client = adapter.client
    if str(payload.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
        request_payload = payload.get("request") if isinstance(payload.get("request"), Mapping) else {}
        request_execution_policy = request_payload.get("execution_policy") if isinstance(request_payload.get("execution_policy"), Mapping) else {}
        live_deployment_quality = _validate_live_deployment_quality(
            candidate_payload=dict(payload.get("candidate") or {}),
            deployment_mode=str(request_execution_policy.get("deployment_mode") or ""),
            execution_policy=request_execution_policy,
            client=client,
        )
        if not live_deployment_quality["ok"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=utc_now_iso(),
                error_text=str(live_deployment_quality["message"]),
                position_id=as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=(f"Execution failed before submission: {live_deployment_quality['message']}"),
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=(f"Execution failed before submission: {live_deployment_quality['message']}"),
            )
            return {
                "status": "blocked",
                "reason": str(live_deployment_quality["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(live_deployment_quality["message"]),
                "attempt": failed_attempt,
                **({} if live_deployment_quality.get("live_quote") is None else {"live_quote": dict(live_deployment_quality["live_quote"])}),
            }
        account_capacity = _validate_submit_account_capacity(
            execution_store=execution_store,
            attempt=payload,
            client=client,
        )
        if not account_capacity["ok"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=utc_now_iso(),
                error_text=str(account_capacity["message"]),
                position_id=as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=(f"Execution failed before submission: {account_capacity['message']}"),
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=(f"Execution failed before submission: {account_capacity['message']}"),
                payload_updates={
                    "execution_admission": _execution_admission_payload_from_account_capacity(
                        attempt=payload,
                        account_capacity=account_capacity,
                    )
                },
            )
            return {
                "status": "blocked",
                "reason": str(account_capacity["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(account_capacity["message"]),
                "attempt": failed_attempt,
            }
    order_request = _normalize_submit_order_request(
        payload=payload,
        order_request=order_request,
    )

    submitted_order: dict[str, Any] | None = None
    try:
        submission = adapter.submit_order(order_request)
        submitted_order = submission.submitted_order
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=str(submitted_order.get("status") or "submitted").lower(),
            broker_order_id=as_text(submitted_order.get("id")),
            client_order_id=as_text(submitted_order.get("client_order_id")) or client_order_id,
            submitted_at=as_text(submitted_order.get("submitted_at")) or requested_at,
            position_id=as_text(payload.get("position_id")),
        )
        if callable(heartbeat):
            heartbeat()
        synced_attempt = _sync_attempt_state(
            execution_store=execution_store,
            attempt=payload,
            client=client,
            order_snapshot=submission.order_snapshot,
        )
        message = _submission_message(synced_attempt, queued=False)
        _publish_execution_attempt_event(synced_attempt, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=synced_attempt,
            event_type="submitted",
            message=message,
        )
        return {
            "status": "submitted",
            "execution_attempt_id": execution_attempt_id,
            "message": message,
            "attempt": synced_attempt,
        }
    except AlpacaRequestError as exc:
        if submitted_order is None:
            classified_error = classify_alpaca_request_error(exc)
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(classified_error["message"]),
                position_id=as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=(f"Execution failed before submission: {classified_error['message']}"),
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=(f"Execution failed before submission: {classified_error['message']}"),
                payload_updates=(
                    {
                        "execution_admission": _execution_admission_payload_from_broker_rejection(
                            attempt=payload,
                            classified_error=classified_error,
                        )
                    }
                    if bool(classified_error.get("terminal"))
                    else None
                ),
            )
            if bool(classified_error.get("terminal")):
                return {
                    "status": "blocked",
                    "reason": str(classified_error["reason"]),
                    "execution_attempt_id": execution_attempt_id,
                    "message": str(classified_error["message"]),
                    "attempt": failed_attempt,
                }
            raise
        broker_order_id = as_text(submitted_order.get("id"))
        submitted_status = str(submitted_order.get("status") or "submitted").lower()
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=submitted_status,
            broker_order_id=broker_order_id,
            client_order_id=as_text(submitted_order.get("client_order_id")) or client_order_id,
            submitted_at=as_text(submitted_order.get("submitted_at")) or requested_at,
            completed_at=_resolve_completed_at(submitted_order) if _is_terminal_status(submitted_status) else None,
            error_text=str(exc),
            position_id=as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, but local execution sync failed: {exc}"),
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            event_type="submit_unknown",
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, but local execution sync failed: {exc}"),
        )
        raise
    except Exception as exc:
        if submitted_order is None:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(exc),
                position_id=as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {exc}",
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=f"Execution failed before submission: {exc}",
            )
            raise
        broker_order_id = as_text(submitted_order.get("id"))
        submitted_status = str(submitted_order.get("status") or "submitted").lower()
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=submitted_status,
            broker_order_id=broker_order_id,
            client_order_id=as_text(submitted_order.get("client_order_id")) or client_order_id,
            submitted_at=as_text(submitted_order.get("submitted_at")) or requested_at,
            completed_at=_resolve_completed_at(submitted_order) if _is_terminal_status(submitted_status) else None,
            error_text=str(exc),
            position_id=as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, but local execution sync failed: {exc}"),
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            event_type="submit_unknown",
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, but local execution sync failed: {exc}"),
        )
        raise

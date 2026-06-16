from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.db.decorators import with_storage
from core.money import option_contract_notional
from core.services.execution_lifecycle import (
    PENDING_SUBMISSION_STATUS,
)
from core.services.option_structures import (
    legs_identity_key,
    normalize_legs,
)
from core.services.positions import enrich_position_row
from core.services.runtime_identity import (
    build_live_run_scope_id,
    resolve_runtime_policy_fields,
)
from core.services.risk_manager import (
    CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
    validate_close_execution,
)
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    resolve_trade_intent,
)
from core.value_coercion import (
    as_text,
    utc_now_iso,
)
from .attempts import (
    _get_attempt_payload,
    _publish_execution_attempt_event,
    _queue_execution_attempt,
    _submission_message,
)
from .shared import BROKER_NAME, OPEN_STATUSES, _execution_attempt_id, _execution_client_order_id, _strategy_family_from_payload

from .admission import (
    _admission_source_from_metadata,
    _approved_execution_admission,
    _attempt_ref_kwargs,
)
from .order_requests import _build_close_order_request


@with_storage()
def submit_position_close_by_id(
    *,
    db_target: str,
    position_id: str,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    job_store = storage.jobs
    metadata = dict(request_metadata or {})
    validation_provenance = as_text(metadata.get("validation_provenance")) or "operator_direct"
    if not execution_store.portfolio_schema_ready():
        raise ValueError(f"Unknown position_id: {position_id}")
    stored_position = execution_store.get_position(position_id)
    if stored_position is None:
        raise ValueError(f"Unknown position_id: {position_id}")
    position = enrich_position_row(dict(stored_position))
    if str(position.get("position_status") or position.get("status") or "open") == "closed":
        raise ValueError("Position is already closed")

    existing_attempts = execution_store.list_open_attempts_for_position(
        position_id=position_id,
        statuses=sorted(OPEN_STATUSES),
    )
    if existing_attempts:
        payload = _get_attempt_payload(
            execution_store,
            str(existing_attempts[0]["execution_attempt_id"]),
        )
        return {
            "action": "submit",
            "changed": False,
            "message": "An active close execution already exists for this position.",
            "attempt": payload,
        }

    requested_at = utc_now_iso()
    client_order_id = _execution_client_order_id()
    trade_intent = resolve_trade_intent(CLOSE_TRADE_INTENT)
    attempt_id: str | None = None
    try:
        order_request, resolved_quantity, resolved_limit_price = _build_close_order_request(
            position=position,
            quantity=quantity,
            limit_price=limit_price,
            client_order_id=client_order_id,
        )
        validate_close_execution(
            position=position,
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            max_reconciliation_age_seconds=CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
        )
        open_attempt_id = as_text(position.get("open_execution_attempt_id"))
        open_attempt = execution_store.get_attempt(open_attempt_id) if open_attempt_id is not None else None
        label = as_text(open_attempt.get("label") if isinstance(open_attempt, Mapping) else None) or as_text(position.get("trading_strategy_id"))
        market_date = (
            as_text(position.get("market_date"))
            or as_text(position.get("market_date_opened"))
            or as_text(open_attempt.get("market_date") if isinstance(open_attempt, Mapping) else None)
            or as_text(open_attempt.get("session_date") if isinstance(open_attempt, Mapping) else None)
        )
        session_id = as_text(open_attempt.get("session_id") if isinstance(open_attempt, Mapping) else None)
        if label is None or market_date is None:
            raise ValueError("Position is missing opening attempt context or market_date")
        policy_fields = resolve_runtime_policy_fields(
            profile=(position.get("risk_policy") or {}).get("profile"),
            root_symbol=str(position["underlying_symbol"]),
        )
        attempt_legs = normalize_legs(order_request.get("legs")) or normalize_legs(position.get("legs"))
        attempt_id = _execution_attempt_id()
        attempt_structure_identity = (
            legs_identity_key(
                strategy=_strategy_family_from_payload(position),
                legs=attempt_legs,
            )
            if attempt_legs
            else None
        )
        close_source_type, close_source_id = _admission_source_from_metadata(
            metadata,
            fallback_type="position",
            fallback_id=position_id,
        )
        close_admission = metadata.get("close_admission") if isinstance(metadata.get("close_admission"), Mapping) else None
        execution_admission = (
            dict(close_admission)
            if close_admission is not None and str(close_admission.get("admission_state") or close_admission.get("status")) in {"approved", "admissible"}
            else _approved_execution_admission(
                admission_kind="position_close",
                source_object_type=close_source_type,
                source_object_id=close_source_id,
                session_date=market_date,
                requested_quantity=resolved_quantity,
                requested_notional=option_contract_notional(resolved_limit_price, resolved_quantity),
                reason="close_validation_passed",
                message="Close order passed position and order validation.",
                policy_snapshot=(metadata.get("risk_policy") if isinstance(metadata.get("risk_policy"), Mapping) else {}),
                evidence={
                    "position_id": position_id,
                    "trade_intent": trade_intent,
                    "order_validation": "passed",
                },
                decided_at=requested_at,
            )
        )
        attempt_refs = _attempt_ref_kwargs(
            metadata,
            fallback_type="position",
            fallback_id=position_id,
        )
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=session_id or build_live_run_scope_id(label, market_date),
            session_date=market_date,
            label=label,
            trading_strategy_id=as_text(position.get("trading_strategy_id")),
            market_date=market_date,
            cycle_id=None,
            attempt_context="position_close",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=str(position["underlying_symbol"]),
            strategy=str(position["strategy"]),
            expiration_date=as_text(position.get("expiration_date")),
            structure_identity=attempt_structure_identity,
            legs=attempt_legs,
            order_payload=dict(order_request),
            economics=dict(position.get("economics") or {}),
            trade_intent=trade_intent,
            position_id=position_id,
            root_symbol=str(position["underlying_symbol"]),
            strategy_family=_strategy_family_from_payload(position),
            style_profile=str(position.get("style_profile") or policy_fields["style_profile"]),
            horizon_intent=str(position.get("horizon_intent") or policy_fields["horizon_intent"]),
            product_class=str(position.get("product_class") or policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **metadata,
                **{key: value for key, value in attempt_refs.items() if value is not None},
                "trade_intent": trade_intent,
                "validation_provenance": validation_provenance,
                "position_id": position_id,
                "execution_admission": execution_admission,
                "order": order_request,
            },
            candidate={},
            **attempt_refs,
        )
        payload = _queue_execution_attempt(
            job_store=job_store,
            execution_store=execution_store,
            attempt=attempt,
        )
        message = _submission_message(payload, queued=True)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": payload,
        }
    except Exception as exc:
        if attempt_id is not None:
            current_attempt = execution_store.get_attempt(attempt_id)
            if current_attempt is not None and str(current_attempt.get("status") or "") == PENDING_SUBMISSION_STATUS:
                execution_store.update_attempt(
                    execution_attempt_id=attempt_id,
                    status="failed",
                    client_order_id=client_order_id,
                    completed_at=requested_at,
                    error_text=str(exc),
                    position_id=position_id,
                )
                payload = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    payload,
                    message=f"Close execution failed before submission: {exc}",
                )
        raise

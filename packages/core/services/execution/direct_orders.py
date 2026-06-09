from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.integrations.alpaca.client import AlpacaRequestError
from core.integrations.alpaca.errors import classify_alpaca_request_error
from core.services.execution_lifecycle import (
    PENDING_SUBMISSION_STATUS,
)
from core.services.option_structures import (
    build_order_payload,
    common_expiration_date,
    legs_identity_key,
    normalize_legs,
    normalize_strategy_family,
    order_payload_legs,
)
from core.services.runtime_identity import (
    build_live_run_scope_id,
    resolve_runtime_policy_fields,
)
from core.services.risk_manager import (
    evaluate_open_execution,
)
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
    resolve_trade_intent,
)
from core.value_coercion import (
    as_text,
    coerce_float,
    coerce_int,
    utc_now_iso,
)
from .alpaca_adapter import create_alpaca_order_adapter
from .attempts import (
    _get_attempt_payload,
    _publish_execution_attempt_event,
    _queue_execution_attempt,
    _require_execution_schema,
    _submission_message,
    _sync_attempt_state,
    _sync_equity_attempt_state,
)
from .runtimes import (
    ALPACA_DIRECT_RUNTIME,
    normalize_execution_runtime,
)
from .shared import (
    BROKER_NAME,
    _execution_attempt_id,
    _execution_client_order_id,
    _normalize_limit_value,
)

from .admission import (
    _admission_source_from_metadata,
    _approved_execution_admission,
    _attempt_ref_kwargs,
    _direct_order_execution_policy,
    _execution_admission_payload_from_risk_evaluation,
    _execution_notional,
    _metadata_policy,
    _raise_if_admission_blocks,
    _strategy_position_size_policy,
)


@with_storage()
def submit_equity_order(
    *,
    db_target: str,
    symbol: str,
    side: str,
    quantity: int,
    limit_price: float,
    time_in_force: str = "day",
    label: str = "manual_equity",
    market_date: str | None = None,
    execution_runtime: str | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    normalized_runtime = normalize_execution_runtime(execution_runtime)
    metadata = dict(request_metadata or {})

    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("Equity order requires a symbol")
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("Equity order side must be buy or sell")
    resolved_quantity = int(quantity)
    if resolved_quantity <= 0:
        raise ValueError("Equity order quantity must be positive")
    resolved_limit_price = _normalize_limit_value(limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Equity order requires a positive limit price")
    resolved_time_in_force = str(time_in_force or "day").strip().lower()
    if resolved_time_in_force not in {"day", "gtc"}:
        raise ValueError("Equity order time_in_force must be day or gtc")

    resolved_trade_intent = resolve_trade_intent(
        as_text(metadata.get("trade_intent")) or (OPEN_TRADE_INTENT if normalized_side == "buy" else CLOSE_TRADE_INTENT)
    )
    if normalized_side == "buy":
        position_intent = "buy_to_open" if resolved_trade_intent == OPEN_TRADE_INTENT else "buy_to_close"
    else:
        position_intent = "sell_to_open" if resolved_trade_intent == OPEN_TRADE_INTENT else "sell_to_close"
    leg_role = "short" if position_intent in {"sell_to_open", "buy_to_close"} else "long"
    position_id = as_text(metadata.get("position_id"))
    validation_provenance = as_text(metadata.get("validation_provenance")) or "operator_direct"
    requested_at = utc_now_iso()
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    resolved_label = as_text(label) or "manual_equity"
    client_order_id = _execution_client_order_id()
    attempt_id = _execution_attempt_id()
    order_request = {
        "symbol": normalized_symbol,
        "side": normalized_side,
        "qty": str(resolved_quantity),
        "type": "limit",
        "limit_price": f"{resolved_limit_price:.2f}",
        "time_in_force": resolved_time_in_force,
        "client_order_id": client_order_id,
        "position_intent": position_intent,
    }
    legs = [
        {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "position_intent": position_intent,
            "ratio_qty": "1",
            "role": leg_role,
        }
    ]
    strategy = "equity_short" if leg_role == "short" else "equity_long"
    equity_source_type, equity_source_id = _admission_source_from_metadata(
        metadata,
        fallback_type="direct_equity_order",
        fallback_id=attempt_id,
    )
    equity_risk_policy = _metadata_policy(metadata, "risk_policy")
    equity_execution_policy = _direct_order_execution_policy(
        metadata,
        risk_policy=equity_risk_policy,
        quantity=resolved_quantity,
    )
    execution_admission = _approved_execution_admission(
        admission_kind=f"direct_equity_{resolved_trade_intent}",
        source_object_type=equity_source_type,
        source_object_id=equity_source_id,
        session_date=resolved_market_date,
        requested_quantity=resolved_quantity,
        requested_notional=_execution_notional(
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            multiplier=1.0,
        ),
        reason="direct_equity_request_validated",
        message="Direct equity order passed request validation.",
        policy_snapshot={
            "risk_policy": equity_risk_policy,
            "execution_policy": equity_execution_policy,
        },
        evidence={
            "asset_class": "equity",
            "symbol": normalized_symbol,
            "side": normalized_side,
            "position_intent": position_intent,
        },
        decided_at=requested_at,
    )
    attempt_refs = _attempt_ref_kwargs(
        metadata,
        fallback_type="direct_equity_order",
        fallback_id=attempt_id,
    )
    attempt_created = False
    submitted_order: dict[str, Any] | None = None
    try:
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=build_live_run_scope_id(resolved_label, resolved_market_date),
            session_date=resolved_market_date,
            label=resolved_label,
            trading_strategy_id=as_text(metadata.get("trading_strategy_id")),
            market_date=resolved_market_date,
            cycle_id=None,
            attempt_context="equity_order",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=normalized_symbol,
            strategy=strategy,
            expiration_date=None,
            structure_identity=None,
            legs=legs,
            order_payload=order_request,
            economics={},
            trade_intent=resolved_trade_intent,
            position_id=position_id,
            root_symbol=normalized_symbol,
            strategy_family=strategy,
            style_profile=as_text(metadata.get("style_profile")) or "manual_equity",
            horizon_intent=as_text(metadata.get("horizon_intent")) or "manual",
            product_class="single_name_equity",
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **{key: value for key, value in attempt_refs.items() if value is not None},
                "trade_intent": resolved_trade_intent,
                "validation_provenance": validation_provenance,
                "execution_runtime": normalized_runtime,
                "execution_policy": equity_execution_policy,
                "asset_class": "equity",
                "position_intent": position_intent,
                **({} if position_id is None else {"position_id": position_id}),
                **(
                    {}
                    if as_text(metadata.get("trading_strategy_id")) is None
                    else {"trading_strategy_id": as_text(metadata.get("trading_strategy_id"))}
                ),
                **({} if as_text(metadata.get("config_hash")) is None else {"config_hash": as_text(metadata.get("config_hash"))}),
                **(
                    {}
                    if as_text(metadata.get("execution_intent_id")) is None
                    else {"execution_intent_id": as_text(metadata.get("execution_intent_id"))}
                ),
                **({} if not isinstance(metadata.get("exit_policy"), Mapping) else {"exit_policy": dict(metadata["exit_policy"])}),
                **({} if not isinstance(metadata.get("risk_policy"), Mapping) else {"risk_policy": dict(metadata["risk_policy"])}),
                "execution_admission": execution_admission,
                **({} if not isinstance(metadata.get("source"), Mapping) else {"source": dict(metadata["source"])}),
                "order": order_request,
            },
            candidate={},
            **attempt_refs,
        )
        attempt_created = True
        adapter = create_alpaca_order_adapter()
        submission = adapter.submit_order(order_request)
        submitted_order = submission.submitted_order
        synced_attempt = _sync_equity_attempt_state(
            execution_store=execution_store,
            attempt=attempt,
            client=adapter.client,
            order_snapshot=submission.order_snapshot,
        )
        message = f"Submitted equity {normalized_side} for {resolved_quantity} {normalized_symbol}."
        _publish_execution_attempt_event(synced_attempt, message=message)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": synced_attempt,
        }
    except AlpacaRequestError as exc:
        if submitted_order is None and attempt_created:
            classified_error = classify_alpaca_request_error(exc)
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(classified_error["message"]),
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=(f"Equity execution failed before submission: {classified_error['message']}"),
            )
            if bool(classified_error.get("terminal")):
                return {
                    "action": "submit",
                    "changed": True,
                    "status": "blocked",
                    "reason": str(classified_error["reason"]),
                    "message": str(classified_error["message"]),
                    "attempt": failed_attempt,
                }
        raise
    except Exception as exc:
        if submitted_order is None and attempt_created:
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(exc),
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Equity execution failed before submission: {exc}",
            )
        raise


@with_storage()
def submit_option_order(
    *,
    db_target: str,
    symbol: str,
    side: str,
    quantity: int,
    limit_price: float,
    time_in_force: str = "day",
    label: str = "manual_option",
    market_date: str | None = None,
    underlying_symbol: str | None = None,
    strategy_family: str = "long_call",
    expiration_date: str | None = None,
    option_type: str | None = None,
    strike: float | None = None,
    execution_runtime: str | None = None,
    request_metadata: dict[str, Any] | None = None,
    queue_submission: bool = False,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    normalized_runtime = normalize_execution_runtime(execution_runtime)
    if normalized_runtime != ALPACA_DIRECT_RUNTIME:
        raise ValueError("Single-leg option orders currently require alpaca_direct runtime")

    metadata = dict(request_metadata or {})
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("Option order requires a contract symbol")
    normalized_underlying = str(underlying_symbol or metadata.get("underlying_symbol") or "").strip().upper()
    if not normalized_underlying:
        raise ValueError("Option order requires an underlying symbol")
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("Option order side must be buy or sell")
    resolved_quantity = int(quantity)
    if resolved_quantity <= 0:
        raise ValueError("Option order quantity must be positive")
    resolved_limit_price = _normalize_limit_value(limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Option order requires a positive limit price")
    resolved_time_in_force = str(time_in_force or "day").strip().lower()
    if resolved_time_in_force != "day":
        raise ValueError("Option order time_in_force must be day")

    resolved_strategy_family = str(strategy_family or "long_call").strip().lower()
    if resolved_strategy_family not in {"long_call", "long_put"}:
        raise ValueError("Direct option orders currently support long_call and long_put")
    resolved_trade_intent = resolve_trade_intent(
        as_text(metadata.get("trade_intent")) or (OPEN_TRADE_INTENT if normalized_side == "buy" else CLOSE_TRADE_INTENT)
    )
    if resolved_trade_intent == OPEN_TRADE_INTENT and normalized_side != "buy":
        raise ValueError("Long option opens must buy to open")
    if resolved_trade_intent == CLOSE_TRADE_INTENT and normalized_side != "sell":
        raise ValueError("Long option closes must sell to close")
    position_intent = "buy_to_open" if resolved_trade_intent == OPEN_TRADE_INTENT else "sell_to_close"
    resolved_option_type = (as_text(option_type) or ("call" if resolved_strategy_family == "long_call" else "put")).lower()
    if resolved_option_type not in {"call", "put"}:
        raise ValueError("Option order option_type must be call or put")

    resolved_expiration = as_text(expiration_date)
    position_id = as_text(metadata.get("position_id"))
    validation_provenance = as_text(metadata.get("validation_provenance")) or "operator_direct"
    requested_at = utc_now_iso()
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    resolved_label = as_text(label) or "manual_option"
    client_order_id = _execution_client_order_id()
    attempt_id = _execution_attempt_id()
    legs = [
        {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "position_intent": position_intent,
            "ratio_qty": "1",
            "role": "long",
            "expiration_date": resolved_expiration,
            "strike": strike,
            "option_type": resolved_option_type,
        }
    ]
    order_request = build_order_payload(
        legs=legs,
        limit_price=resolved_limit_price,
        strategy_family=resolved_strategy_family,
        trade_intent=resolved_trade_intent,
        quantity=resolved_quantity,
    )
    order_request["client_order_id"] = client_order_id
    profile = as_text(metadata.get("profile")) or "weekly"
    policy_fields = resolve_runtime_policy_fields(
        profile=profile,
        root_symbol=normalized_underlying,
    )
    option_selection = dict(metadata.get("option_selection")) if isinstance(metadata.get("option_selection"), Mapping) else {}
    option_quote_metrics = _metadata_policy(option_selection, "quote_metrics")
    candidate_generated_at = as_text(option_quote_metrics.get("timestamp")) or requested_at
    candidate_payload = {
        "underlying_symbol": normalized_underlying,
        "strategy": resolved_strategy_family,
        "strategy_family": resolved_strategy_family,
        "profile": profile,
        "generated_at": candidate_generated_at,
        "expiration_date": resolved_expiration,
        "underlying_price": coerce_float(metadata.get("underlying_price")),
        "legs": legs,
        "order_payload": dict(order_request),
        "structure_identity": legs_identity_key(
            strategy=resolved_strategy_family,
            legs=legs,
        ),
        "width": 0.0,
        "midpoint_credit": resolved_limit_price,
        "natural_credit": resolved_limit_price,
        "max_profit": None,
        "max_loss": round(resolved_limit_price * 100.0, 2),
        "option_selection": option_selection,
    }
    option_source_type, option_source_id = _admission_source_from_metadata(
        metadata,
        fallback_type="direct_option_order",
        fallback_id=attempt_id,
    )
    requested_option_notional = _execution_notional(
        quantity=resolved_quantity,
        limit_price=resolved_limit_price,
    )
    option_risk_policy = _metadata_policy(metadata, "risk_policy")
    option_execution_policy = _direct_order_execution_policy(
        metadata,
        risk_policy=option_risk_policy,
        quantity=resolved_quantity,
    )
    if resolved_trade_intent == OPEN_TRADE_INTENT:
        position_size_policy = _strategy_position_size_policy(
            trading_strategy_id=as_text(metadata.get("trading_strategy_id")),
        )
        risk_evaluation = evaluate_open_execution(
            execution_store=execution_store,
            session_id=build_live_run_scope_id(resolved_label, resolved_market_date),
            candidate=candidate_payload,
            cycle={
                "session_date": resolved_market_date,
                "label": resolved_label,
                "generated_at": candidate_generated_at,
            },
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            risk_policy=option_risk_policy,
            execution_policy=option_execution_policy,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
        )
        execution_admission = _execution_admission_payload_from_risk_evaluation(
            risk_evaluation,
            admission_kind="direct_option_open",
            source_object_type=option_source_type,
            source_object_id=option_source_id,
            session_date=resolved_market_date,
            requested_notional=requested_option_notional,
            max_loss=requested_option_notional,
        )
        _raise_if_admission_blocks(execution_admission)
    else:
        execution_admission = _approved_execution_admission(
            admission_kind="direct_option_close",
            source_object_type=option_source_type,
            source_object_id=option_source_id,
            session_date=resolved_market_date,
            requested_quantity=resolved_quantity,
            requested_notional=requested_option_notional,
            reason="direct_option_close_request_validated",
            message="Direct option close order passed request validation.",
            policy_snapshot=option_risk_policy,
            evidence={
                "asset_class": "option",
                "symbol": normalized_symbol,
                "underlying_symbol": normalized_underlying,
                "position_intent": position_intent,
            },
            decided_at=requested_at,
        )
    attempt_refs = _attempt_ref_kwargs(
        metadata,
        fallback_type="direct_option_order",
        fallback_id=attempt_id,
    )
    attempt_created = False
    submitted_order: dict[str, Any] | None = None
    try:
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=build_live_run_scope_id(resolved_label, resolved_market_date),
            session_date=resolved_market_date,
            label=resolved_label,
            trading_strategy_id=as_text(metadata.get("trading_strategy_id")),
            market_date=resolved_market_date,
            cycle_id=None,
            attempt_context="option_order",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=normalized_underlying,
            strategy=resolved_strategy_family,
            expiration_date=resolved_expiration,
            structure_identity=str(candidate_payload["structure_identity"]),
            legs=legs,
            order_payload=order_request,
            economics={
                "midpoint_credit": resolved_limit_price,
                "natural_credit": resolved_limit_price,
                "max_profit": None,
                "max_loss": round(resolved_limit_price * 100.0, 2),
            },
            trade_intent=resolved_trade_intent,
            position_id=position_id,
            root_symbol=normalized_underlying,
            strategy_family=resolved_strategy_family,
            style_profile=as_text(metadata.get("style_profile")) or str(policy_fields["style_profile"]),
            horizon_intent=as_text(metadata.get("horizon_intent")) or str(policy_fields["horizon_intent"]),
            product_class=as_text(metadata.get("product_class")) or str(policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **{key: value for key, value in attempt_refs.items() if value is not None},
                "trade_intent": resolved_trade_intent,
                "validation_provenance": validation_provenance,
                "execution_runtime": normalized_runtime,
                "execution_policy": option_execution_policy,
                "asset_class": "option",
                "position_intent": position_intent,
                **({} if position_id is None else {"position_id": position_id}),
                **(
                    {}
                    if as_text(metadata.get("trading_strategy_id")) is None
                    else {"trading_strategy_id": as_text(metadata.get("trading_strategy_id"))}
                ),
                **({} if as_text(metadata.get("config_hash")) is None else {"config_hash": as_text(metadata.get("config_hash"))}),
                **(
                    {}
                    if as_text(metadata.get("execution_intent_id")) is None
                    else {"execution_intent_id": as_text(metadata.get("execution_intent_id"))}
                ),
                **({} if not isinstance(metadata.get("exit_policy"), Mapping) else {"exit_policy": dict(metadata["exit_policy"])}),
                **({} if not isinstance(metadata.get("risk_policy"), Mapping) else {"risk_policy": dict(metadata["risk_policy"])}),
                "execution_admission": execution_admission,
                **({} if not isinstance(metadata.get("source"), Mapping) else {"source": dict(metadata["source"])}),
                **(
                    {}
                    if as_text(metadata.get("original_limit_price")) is None
                    else {"original_limit_price": coerce_float(metadata.get("original_limit_price"))}
                ),
                **(
                    {}
                    if as_text(metadata.get("previous_limit_price")) is None
                    else {"previous_limit_price": coerce_float(metadata.get("previous_limit_price"))}
                ),
                **(
                    {}
                    if as_text(metadata.get("previous_execution_attempt_id")) is None
                    else {"previous_execution_attempt_id": as_text(metadata.get("previous_execution_attempt_id"))}
                ),
                **(
                    {}
                    if as_text(metadata.get("supersedes_execution_intent_id")) is None
                    else {"supersedes_execution_intent_id": as_text(metadata.get("supersedes_execution_intent_id"))}
                ),
                **({} if coerce_int(metadata.get("reprice_count")) is None else {"reprice_count": coerce_int(metadata.get("reprice_count"))}),
                **({} if not isinstance(metadata.get("repricing_policy"), Mapping) else {"repricing_policy": dict(metadata["repricing_policy"])}),
                **({} if not option_selection else {"option_selection": option_selection}),
                "order": order_request,
            },
            candidate=candidate_payload,
            **attempt_refs,
        )
        attempt_created = True
        if queue_submission:
            payload = _queue_execution_attempt(
                job_store=storage.jobs,
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
        adapter = create_alpaca_order_adapter()
        submission = adapter.submit_order(order_request)
        submitted_order = submission.submitted_order
        synced_attempt = _sync_attempt_state(
            execution_store=execution_store,
            attempt=dict(attempt),
            client=adapter.client,
            order_snapshot=submission.order_snapshot,
        )
        message = f"Submitted option {normalized_side} for {resolved_quantity} {normalized_symbol}."
        _publish_execution_attempt_event(synced_attempt, message=message)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": synced_attempt,
        }
    except AlpacaRequestError as exc:
        if submitted_order is None and attempt_created:
            classified_error = classify_alpaca_request_error(exc)
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(classified_error["message"]),
                position_id=position_id,
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=(f"Option execution failed before submission: {classified_error['message']}"),
            )
            if bool(classified_error.get("terminal")):
                return {
                    "action": "submit",
                    "changed": True,
                    "status": "blocked",
                    "reason": str(classified_error["reason"]),
                    "message": str(classified_error["message"]),
                    "attempt": failed_attempt,
                }
        raise
    except Exception as exc:
        if submitted_order is None and attempt_created:
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
                failed_attempt = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    failed_attempt,
                    message=f"Option execution failed before submission: {exc}",
                )
        raise


@with_storage()
def submit_option_structure_order(
    *,
    db_target: str,
    legs: list[dict[str, Any]],
    quantity: int,
    limit_price: float,
    order_payload: dict[str, Any] | None = None,
    label: str = "strategy_option",
    market_date: str | None = None,
    underlying_symbol: str | None = None,
    strategy_family: str | None = None,
    expiration_date: str | None = None,
    execution_runtime: str | None = None,
    request_metadata: dict[str, Any] | None = None,
    economics: dict[str, Any] | None = None,
    candidate: dict[str, Any] | None = None,
    queue_submission: bool = False,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    normalized_runtime = normalize_execution_runtime(execution_runtime)
    if normalized_runtime != ALPACA_DIRECT_RUNTIME:
        raise ValueError("Option structure orders currently require alpaca_direct runtime")

    metadata = dict(request_metadata or {})
    candidate_payload = dict(candidate or {})
    economics_payload = dict(economics or {})
    source_order_payload = dict(order_payload or {})
    resolved_trade_intent = resolve_trade_intent(as_text(metadata.get("trade_intent")) or OPEN_TRADE_INTENT)
    resolved_expiration = as_text(expiration_date) or as_text(candidate_payload.get("expiration_date")) or as_text(metadata.get("expiration_date"))
    resolved_legs = order_payload_legs(
        source_order_payload,
        expiration_date=resolved_expiration,
    ) or normalize_legs(
        legs,
        expiration_date=resolved_expiration,
    )
    if not resolved_legs:
        raise ValueError("Option structure order requires canonical legs")
    resolved_expiration = common_expiration_date(resolved_legs) or resolved_expiration
    if resolved_expiration is not None:
        resolved_legs = normalize_legs(resolved_legs, expiration_date=resolved_expiration)
    normalized_underlying = (
        str(
            underlying_symbol
            or metadata.get("underlying_symbol")
            or candidate_payload.get("underlying_symbol")
            or candidate_payload.get("root_symbol")
            or ""
        )
        .strip()
        .upper()
    )
    if not normalized_underlying:
        raise ValueError("Option structure order requires an underlying symbol")
    resolved_strategy_family = normalize_strategy_family(
        strategy_family
        or metadata.get("strategy_family")
        or metadata.get("trade_structure")
        or candidate_payload.get("strategy_family")
        or candidate_payload.get("strategy")
    )
    if resolved_strategy_family == "unknown":
        raise ValueError("Option structure order requires a strategy family")
    resolved_quantity = int(quantity)
    if resolved_quantity <= 0:
        raise ValueError("Option structure order quantity must be positive")
    resolved_limit_price = _normalize_limit_value(limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Option structure order requires a positive limit price")

    requested_at = utc_now_iso()
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    resolved_label = as_text(label) or "strategy_option"
    client_order_id = _execution_client_order_id()
    attempt_id = _execution_attempt_id()
    order_request = dict(source_order_payload)
    order_legs = order_payload_legs(
        order_request,
        expiration_date=resolved_expiration,
    )
    if not order_legs or source_order_payload.get("limit_price") in (None, ""):
        order_request = build_order_payload(
            legs=resolved_legs,
            limit_price=resolved_limit_price,
            strategy_family=resolved_strategy_family,
            trade_intent=resolved_trade_intent,
            quantity=resolved_quantity,
        )
    else:
        if len(order_legs) > 1:
            order_request.setdefault("order_class", "mleg")
            order_request["legs"] = list(order_request.get("legs") or order_legs)
        order_request.setdefault("qty", str(resolved_quantity))
        order_request.setdefault("type", "limit")
        order_request.setdefault("limit_price", source_order_payload.get("limit_price") or f"{resolved_limit_price:.2f}")
        order_request.setdefault("time_in_force", "day")
    order_request["client_order_id"] = client_order_id

    profile = as_text(metadata.get("profile")) or as_text(candidate_payload.get("profile")) or "weekly"
    policy_fields = resolve_runtime_policy_fields(
        profile=profile,
        root_symbol=normalized_underlying,
    )
    option_selection = dict(metadata.get("option_selection")) if isinstance(metadata.get("option_selection"), Mapping) else {}
    option_quote_metrics = _metadata_policy(option_selection, "quote_metrics")
    candidate_generated_at = as_text(candidate_payload.get("generated_at")) or as_text(option_quote_metrics.get("timestamp")) or requested_at
    structure_identity = as_text(candidate_payload.get("structure_identity")) or legs_identity_key(
        strategy=resolved_strategy_family,
        legs=resolved_legs,
    )
    candidate_payload.update(
        {
            "underlying_symbol": normalized_underlying,
            "strategy": resolved_strategy_family,
            "strategy_family": resolved_strategy_family,
            "profile": profile,
            "generated_at": candidate_generated_at,
            "expiration_date": resolved_expiration,
            "legs": resolved_legs,
            "order_payload": dict(order_request),
            "structure_identity": structure_identity,
            "option_selection": option_selection,
        }
    )
    for key, value in economics_payload.items():
        candidate_payload.setdefault(key, value)
    if coerce_float(candidate_payload.get("underlying_price")) is None:
        candidate_payload["underlying_price"] = coerce_float(metadata.get("underlying_price"))

    option_source_type, option_source_id = _admission_source_from_metadata(
        metadata,
        fallback_type="direct_option_structure_order",
        fallback_id=attempt_id,
    )
    requested_option_notional = _execution_notional(
        quantity=resolved_quantity,
        limit_price=resolved_limit_price,
    )
    option_risk_policy = _metadata_policy(metadata, "risk_policy")
    option_execution_policy = _direct_order_execution_policy(
        metadata,
        risk_policy=option_risk_policy,
        quantity=resolved_quantity,
    )
    metadata_admission = metadata.get("execution_admission")
    if isinstance(metadata_admission, Mapping):
        execution_admission = dict(metadata_admission)
        _raise_if_admission_blocks(execution_admission)
    else:
        execution_admission = _approved_execution_admission(
            admission_kind=f"direct_option_structure_{resolved_trade_intent}",
            source_object_type=option_source_type,
            source_object_id=option_source_id,
            session_date=resolved_market_date,
            requested_quantity=resolved_quantity,
            requested_notional=requested_option_notional,
            reason="direct_option_structure_request_validated",
            message="Direct option structure order passed request validation.",
            policy_snapshot={
                "risk_policy": option_risk_policy,
                "execution_policy": option_execution_policy,
            },
            evidence={
                "asset_class": "option",
                "underlying_symbol": normalized_underlying,
                "strategy_family": resolved_strategy_family,
                "leg_count": len(resolved_legs),
            },
            decided_at=requested_at,
        )
    attempt_refs = _attempt_ref_kwargs(
        metadata,
        fallback_type="direct_option_structure_order",
        fallback_id=attempt_id,
    )
    position_id = as_text(metadata.get("position_id"))
    validation_provenance = as_text(metadata.get("validation_provenance")) or "operator_direct"
    position_intents = sorted({str(leg.get("position_intent")) for leg in resolved_legs if as_text(leg.get("position_intent"))})
    attempt_created = False
    submitted_order: dict[str, Any] | None = None
    try:
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=build_live_run_scope_id(resolved_label, resolved_market_date),
            session_date=resolved_market_date,
            label=resolved_label,
            trading_strategy_id=as_text(metadata.get("trading_strategy_id")),
            market_date=resolved_market_date,
            cycle_id=None,
            attempt_context="option_order",
            candidate_generated_at=candidate_generated_at,
            run_id=None,
            job_run_id=None,
            underlying_symbol=normalized_underlying,
            strategy=resolved_strategy_family,
            expiration_date=resolved_expiration,
            structure_identity=structure_identity,
            legs=resolved_legs,
            order_payload=order_request,
            economics=economics_payload,
            trade_intent=resolved_trade_intent,
            position_id=position_id,
            root_symbol=normalized_underlying,
            strategy_family=resolved_strategy_family,
            style_profile=as_text(metadata.get("style_profile")) or str(policy_fields["style_profile"]),
            horizon_intent=as_text(metadata.get("horizon_intent")) or str(policy_fields["horizon_intent"]),
            product_class=as_text(metadata.get("product_class")) or str(policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **{key: value for key, value in attempt_refs.items() if value is not None},
                "trade_intent": resolved_trade_intent,
                "validation_provenance": validation_provenance,
                "execution_runtime": normalized_runtime,
                "execution_policy": option_execution_policy,
                "asset_class": "option",
                "trade_structure": as_text(metadata.get("trade_structure")) or resolved_strategy_family,
                "strategy_family": resolved_strategy_family,
                "position_intents": position_intents,
                **({} if position_id is None else {"position_id": position_id}),
                **(
                    {}
                    if as_text(metadata.get("trading_strategy_id")) is None
                    else {"trading_strategy_id": as_text(metadata.get("trading_strategy_id"))}
                ),
                **({} if as_text(metadata.get("config_hash")) is None else {"config_hash": as_text(metadata.get("config_hash"))}),
                **(
                    {}
                    if as_text(metadata.get("execution_intent_id")) is None
                    else {"execution_intent_id": as_text(metadata.get("execution_intent_id"))}
                ),
                **({} if not isinstance(metadata.get("exit_policy"), Mapping) else {"exit_policy": dict(metadata["exit_policy"])}),
                **({} if not isinstance(metadata.get("risk_policy"), Mapping) else {"risk_policy": dict(metadata["risk_policy"])}),
                "execution_admission": execution_admission,
                **({} if not isinstance(metadata.get("source"), Mapping) else {"source": dict(metadata["source"])}),
                **(
                    {}
                    if as_text(metadata.get("original_limit_price")) is None
                    else {"original_limit_price": coerce_float(metadata.get("original_limit_price"))}
                ),
                **(
                    {}
                    if as_text(metadata.get("previous_limit_price")) is None
                    else {"previous_limit_price": coerce_float(metadata.get("previous_limit_price"))}
                ),
                **(
                    {}
                    if as_text(metadata.get("previous_execution_attempt_id")) is None
                    else {"previous_execution_attempt_id": as_text(metadata.get("previous_execution_attempt_id"))}
                ),
                **(
                    {}
                    if as_text(metadata.get("supersedes_execution_intent_id")) is None
                    else {"supersedes_execution_intent_id": as_text(metadata.get("supersedes_execution_intent_id"))}
                ),
                **({} if coerce_int(metadata.get("reprice_count")) is None else {"reprice_count": coerce_int(metadata.get("reprice_count"))}),
                **({} if not isinstance(metadata.get("repricing_policy"), Mapping) else {"repricing_policy": dict(metadata["repricing_policy"])}),
                **({} if not option_selection else {"option_selection": option_selection}),
                "order": order_request,
            },
            candidate=candidate_payload,
            **attempt_refs,
        )
        attempt_created = True
        if queue_submission:
            payload = _queue_execution_attempt(
                job_store=storage.jobs,
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
        adapter = create_alpaca_order_adapter()
        submission = adapter.submit_order(order_request)
        submitted_order = submission.submitted_order
        synced_attempt = _sync_attempt_state(
            execution_store=execution_store,
            attempt=dict(attempt),
            client=adapter.client,
            order_snapshot=submission.order_snapshot,
        )
        message = _submission_message(synced_attempt, queued=False)
        _publish_execution_attempt_event(synced_attempt, message=message)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": synced_attempt,
        }
    except AlpacaRequestError as exc:
        if submitted_order is None and attempt_created:
            classified_error = classify_alpaca_request_error(exc)
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(classified_error["message"]),
                position_id=position_id,
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=(f"Option structure execution failed before submission: {classified_error['message']}"),
            )
            if bool(classified_error.get("terminal")):
                return {
                    "action": "submit",
                    "changed": True,
                    "status": "blocked",
                    "reason": str(classified_error["reason"]),
                    "message": str(classified_error["message"]),
                    "attempt": failed_attempt,
                }
        raise
    except Exception as exc:
        if submitted_order is None and attempt_created:
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
                failed_attempt = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    failed_attempt,
                    message=f"Option structure execution failed before submission: {exc}",
                )
        raise

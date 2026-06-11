from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.db.decorators import with_storage
from core.jobs.adhoc import enqueue_ad_hoc_job
from core.jobs.registry import (
    EXECUTION_INTENT_DISPATCH_ADHOC_JOB_KEY,
    EXECUTION_INTENT_DISPATCH_JOB_TYPE,
)
from core.services.alpaca import (
    create_alpaca_client_from_env,
    resolve_trading_environment,
)
from core.services.execution.admission import ExecutionAdmissionError
from core.services.execution.direct_orders import (
    submit_equity_order,
    submit_option_order,
    submit_option_structure_order,
)
from core.services.execution.position_close import submit_position_close_by_id
from core.observability.logging import log_event
from core.services.option_structures import common_expiration_date, normalize_legs, normalize_strategy_family, order_payload_legs
from core.value_coercion import as_text, coerce_bool, coerce_float, coerce_int, utc_now_iso
from core.storage.read_models import TradeDecisionSignalRead
from core.storage.serializers import parse_datetime

from .maintenance import (
    _auto_execution_gate,
    _backfill_strategy_position_links,
    _cleanup_slot_conflicts,
    _cleanup_inactive_strategy_intents,
    _cleanup_terminal_intent_history,
    _intent_execution_policy,
    _intent_exit_policy,
    _position_is_active_for_intent,
)
from .repricing import _manage_submitted_open_intents
from .shared import (
    _append_event,
    _attempt_state,
    _intent_action_type,
    _intent_payload,
    _update_intent,
    normalize_execution_intent_state,
)

logger = logging.getLogger(__name__)

PRE_DISPATCH_EXPIRE_REASON = "dispatch_window_elapsed"


def _equity_intent_payload(intent: dict[str, Any]) -> dict[str, Any] | None:
    payload = _intent_payload(intent)
    asset_class = str(payload.get("asset_class") or "").strip().lower()
    if asset_class != "equity":
        return None
    symbol = as_text(payload.get("symbol"))
    side = str(payload.get("side") or intent.get("action_type") or "").strip().lower()
    if symbol is None or side not in {"buy", "sell"}:
        return None
    return payload


def _option_intent_payload(intent: dict[str, Any]) -> dict[str, Any] | None:
    payload = _intent_payload(intent)
    asset_class = str(payload.get("asset_class") or "").strip().lower()
    if asset_class != "option":
        return None
    symbol = as_text(payload.get("symbol"))
    side = str(payload.get("side") or intent.get("action_type") or "").strip().lower()
    if symbol is None or side not in {"buy", "sell"}:
        return None
    return payload


def _intent_engine_ref_metadata(intent: dict[str, Any]) -> dict[str, Any]:
    payload = _intent_payload(intent)
    admission = payload.get("execution_admission") if isinstance(payload.get("execution_admission"), dict) else {}
    trade_signal_id = as_text(intent.get("trade_signal_id")) or as_text(payload.get("trade_signal_id"))
    trade_decision_id = as_text(intent.get("trade_decision_id")) or as_text(payload.get("trade_decision_id"))
    admission_decision_id = as_text(payload.get("admission_decision_id")) or as_text(admission.get("admission_decision_id"))
    source_object_type = None
    source_object_id = None
    if trade_decision_id is not None:
        source_object_type = "trade_decision"
        source_object_id = trade_decision_id
    elif trade_signal_id is not None:
        source_object_type = "trade_signal"
        source_object_id = trade_signal_id
    return {
        key: value
        for key, value in {
            "trade_signal_id": trade_signal_id,
            "trade_decision_id": trade_decision_id,
            "admission_decision_id": admission_decision_id,
            "source_object_type": source_object_type,
            "source_object_id": source_object_id,
        }.items()
        if value is not None
    }


def _repricing_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for key in ("original_limit_price", "previous_limit_price", "reprice_count"):
        if payload.get(key) not in (None, ""):
            metadata[key] = payload[key]
    for key in ("previous_execution_attempt_id", "supersedes_execution_intent_id"):
        value = as_text(payload.get(key))
        if value is not None:
            metadata[key] = value
    if isinstance(payload.get("repricing_policy"), dict):
        metadata["repricing_policy"] = dict(payload["repricing_policy"])
    return metadata


def _get_trade_decision_signal(
    *,
    engine_facts: Any,
    trade_decision_id: str,
) -> TradeDecisionSignalRead | None:
    if engine_facts is None or not engine_facts.schema_ready():
        return None
    return engine_facts.get_trade_decision_with_signal(trade_decision_id)


def _trade_decision_is_active_for_intent(
    engine_facts: Any,
    intent: dict[str, Any],
) -> tuple[bool, str | None]:
    trade_decision_id = as_text(intent.get("trade_decision_id")) or as_text(_intent_payload(intent).get("trade_decision_id"))
    if trade_decision_id is None:
        return False, "trade_decision_missing"
    if engine_facts is None or not engine_facts.schema_ready():
        return False, "engine_fact_schema_unavailable"
    decision_signal = _get_trade_decision_signal(
        engine_facts=engine_facts,
        trade_decision_id=trade_decision_id,
    )
    if decision_signal is None:
        return False, "trade_decision_missing"
    if decision_signal.decision_state != "selected":
        return False, "trade_decision_not_selected"
    if decision_signal.signal_state not in {"ready"}:
        return False, "trade_signal_not_ready"
    if decision_signal.signal_is_expired(now=datetime.now(UTC)):
        return False, "trade_signal_expired"
    return True, None


def _first_positive_float(*values: Any) -> float | None:
    for value in values:
        parsed = coerce_float(value)
        if parsed is None or parsed == 0:
            continue
        return abs(parsed)
    return None


def _first_positive_int(*values: Any) -> int | None:
    for value in values:
        parsed = coerce_int(value)
        if parsed is None or parsed <= 0:
            continue
        return parsed
    return None


def _trade_decision_option_payload(
    *,
    engine_facts: Any,
    intent: dict[str, Any],
) -> dict[str, Any]:
    payload = _intent_payload(intent)
    trade_decision_id = as_text(intent.get("trade_decision_id")) or as_text(payload.get("trade_decision_id"))
    if trade_decision_id is None:
        raise ValueError("Execution intent is missing trade_decision_id")
    if engine_facts is None or not engine_facts.schema_ready():
        raise ValueError("Engine fact tables are not available for trade-decision dispatch.")
    decision_signal = _get_trade_decision_signal(
        engine_facts=engine_facts,
        trade_decision_id=trade_decision_id,
    )
    if decision_signal is None:
        raise ValueError(f"Unknown trade_decision_id: {trade_decision_id}")
    if decision_signal.decision_state != "selected":
        raise ValueError(f"Trade decision is not selected: {trade_decision_id}")

    signal = decision_signal.signal
    execution_shape = decision_signal.execution_shape
    order_payload = decision_signal.order_payload or (dict(signal.get("order_payload")) if isinstance(signal.get("order_payload"), dict) else {})
    expiration_hint = (
        as_text(execution_shape.get("expiration_date")) or as_text(signal.get("expiration_date")) or as_text(order_payload.get("expiration_date"))
    )
    legs = (
        order_payload_legs(order_payload, expiration_date=expiration_hint)
        or normalize_legs(execution_shape.get("legs"), expiration_date=expiration_hint)
        or normalize_legs(decision_signal.execution_shape_legs, expiration_date=expiration_hint)
        or normalize_legs(decision_signal.signal_legs, expiration_date=expiration_hint)
    )
    if not legs:
        raise ValueError("Trade-decision option shape is missing canonical legs.")
    expiration_date = common_expiration_date(legs) or expiration_hint
    if expiration_date is not None:
        legs = normalize_legs(legs, expiration_date=expiration_date)
    admission = payload.get("execution_admission") if isinstance(payload.get("execution_admission"), dict) else {}
    economics = decision_signal.economics
    requested_quantity = (
        _first_positive_int(
            payload.get("quantity"),
            admission.get("requested_quantity"),
            order_payload.get("qty"),
            order_payload.get("quantity"),
            decision_signal.selected_quantity,
        )
        or 1
    )
    portfolio_admission = admission.get("portfolio_admission") if isinstance(admission.get("portfolio_admission"), dict) else {}
    quantity_caps = [
        cap
        for cap in (
            _first_positive_int(admission.get("admissible_quantity")),
            _first_positive_int(portfolio_admission.get("admissible_quantity")),
        )
        if cap is not None
    ]
    quantity = min([requested_quantity, *quantity_caps]) if quantity_caps else requested_quantity
    limit_price = _first_positive_float(
        payload.get("limit_price"),
        order_payload.get("limit_price"),
        economics.get("midpoint_credit"),
        economics.get("midpoint_value"),
        economics.get("net_credit"),
        economics.get("net_debit"),
        economics.get("credit"),
        economics.get("debit"),
        signal.get("limit_price"),
        signal.get("midpoint_credit"),
        signal.get("midpoint_value"),
    )
    if limit_price is None:
        raise ValueError("Trade-decision option shape is missing a limit price.")

    trade_structure = (
        decision_signal.trade_structure
        or as_text(payload.get("trade_structure"))
        or as_text(execution_shape.get("trade_structure"))
        or as_text(execution_shape.get("strategy_family"))
        or "long_call"
    )
    strategy_family = normalize_strategy_family(trade_structure)
    leg = dict(legs[0])
    symbol = as_text(order_payload.get("symbol")) or as_text(leg.get("symbol"))
    side = as_text(order_payload.get("side")) or as_text(leg.get("side"))
    legacy_single_leg = len(legs) == 1 and strategy_family in {"long_call", "long_put"}
    if legacy_single_leg and (symbol is None or side is None):
        raise ValueError("Trade-decision option shape is missing symbol or side.")
    candidate_payload = dict(signal.get("candidate")) if isinstance(signal.get("candidate"), dict) else {}
    candidate_payload.update(
        {
            "underlying_symbol": (
                as_text(signal.get("underlying_symbol"))
                or as_text(execution_shape.get("underlying_symbol"))
                or as_text(candidate_payload.get("underlying_symbol"))
            ),
            "strategy": strategy_family,
            "strategy_family": strategy_family,
            "trade_structure": trade_structure,
            "profile": as_text(signal.get("profile")) or as_text(execution_shape.get("profile")) or as_text(candidate_payload.get("profile")),
            "expiration_date": expiration_date,
            "legs": legs,
            "order_payload": dict(order_payload),
            "structure_identity": (
                as_text(execution_shape.get("structure_identity"))
                or as_text(signal.get("candidate_identity"))
                or as_text(signal.get("structure_identity"))
                or as_text(candidate_payload.get("structure_identity"))
            ),
        }
    )
    for key, value in economics.items():
        candidate_payload.setdefault(key, value)
    option_selection = {
        "source": "trade_decision",
        "trade_decision_id": trade_decision_id,
        "trade_signal_id": decision_signal.trade_signal_id,
        "trade_candidate_id": signal.get("trade_candidate_id"),
        "quote_metrics": {
            "timestamp": signal.get("observed_at"),
            "midpoint": limit_price,
            "natural": economics.get("natural_credit") or economics.get("natural_value") or limit_price,
        },
    }
    return {
        "asset_class": "option",
        "execution_kind": "legacy_single_leg" if legacy_single_leg else "option_structure",
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "limit_price": limit_price,
        "time_in_force": as_text(order_payload.get("time_in_force")) or "day",
        "label": as_text(signal.get("trading_strategy_id")) or str(intent["trading_strategy_id"]),
        "market_date": as_text(signal.get("session_date")),
        "underlying_symbol": as_text(signal.get("underlying_symbol")) or as_text(execution_shape.get("underlying_symbol")),
        "root_symbol": as_text(signal.get("root_symbol")) or as_text(signal.get("underlying_symbol")),
        "strategy_family": strategy_family,
        "trade_structure": trade_structure,
        "profile": as_text(signal.get("profile")) or as_text(execution_shape.get("profile")) or as_text(candidate_payload.get("profile")),
        "expiration_date": expiration_date,
        "option_type": as_text(leg.get("option_type")),
        "strike": leg.get("strike"),
        "trade_intent": "open",
        "underlying_price": economics.get("underlying_price"),
        "legs": legs,
        "order_payload": dict(order_payload),
        "economics": economics,
        "candidate": candidate_payload,
        "order_class": as_text(order_payload.get("order_class")) or ("mleg" if len(legs) > 1 else "single"),
        "leg_count": len(legs),
        "option_selection": option_selection,
        "source": {
            "kind": "trade_decision",
            "id": trade_decision_id,
            "trade_signal_id": decision_signal.trade_signal_id,
        },
    }


def request_execution_intent_dispatch(
    *,
    job_store: Any,
    limit: int = 25,
    requested_by: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if job_store is None:
        return None
    if hasattr(job_store, "schema_ready") and not job_store.schema_ready():
        return None
    required_methods = ("create_job_run", "update_job_run_status")
    if any(not hasattr(job_store, method_name) for method_name in required_methods):
        return None
    scheduled_for = datetime.now(UTC)
    job_run_id = f"{EXECUTION_INTENT_DISPATCH_ADHOC_JOB_KEY}:{uuid4().hex}"
    payload: dict[str, Any] = {
        "limit": max(int(limit), 1),
        "job_key": EXECUTION_INTENT_DISPATCH_ADHOC_JOB_KEY,
        "job_type": EXECUTION_INTENT_DISPATCH_JOB_TYPE,
        "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
        "singleton_scope": "global",
    }
    if requested_by:
        payload["requested_by"] = dict(requested_by)

    job_run, _ = job_store.create_job_run(
        job_run_id=job_run_id,
        job_key=EXECUTION_INTENT_DISPATCH_ADHOC_JOB_KEY,
        arq_job_id=job_run_id,
        job_type=EXECUTION_INTENT_DISPATCH_JOB_TYPE,
        status="queued",
        scheduled_for=scheduled_for,
        payload=payload,
    )
    try:
        enqueued = enqueue_ad_hoc_job(
            job_type=EXECUTION_INTENT_DISPATCH_JOB_TYPE,
            job_key=EXECUTION_INTENT_DISPATCH_ADHOC_JOB_KEY,
            job_run_id=job_run_id,
            arq_job_id=job_run_id,
            payload=payload,
        )
    except Exception as exc:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=scheduled_for,
            error_text=str(exc),
        )
        return {
            "status": "failed",
            "job_run_id": job_run_id,
            "error": str(exc),
        }
    if enqueued is None:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=scheduled_for,
            error_text="Execution intent dispatch job was not enqueued.",
        )
        return {
            "status": "failed",
            "job_run_id": job_run_id,
            "error": "Execution intent dispatch job was not enqueued.",
        }
    return {
        "status": "queued",
        "job_run_id": str(job_run["job_run_id"]),
        "job_key": EXECUTION_INTENT_DISPATCH_ADHOC_JOB_KEY,
    }


def _intent_target_is_active(
    *,
    intent: dict[str, Any],
    execution_store: Any,
    engine_facts: Any,
) -> tuple[bool, str | None]:
    action_type = _intent_action_type(intent)
    if as_text(intent.get("trade_decision_id")) is not None or as_text(_intent_payload(intent).get("trade_decision_id")) is not None:
        return _trade_decision_is_active_for_intent(engine_facts, intent)
    if as_text(intent.get("strategy_position_id")) is not None or action_type == "close":
        return _position_is_active_for_intent(execution_store, intent)
    if _equity_intent_payload(intent) is not None:
        return True, None
    if _option_intent_payload(intent) is not None:
        return True, None
    return False, "source_reference_missing"


@with_storage()
def submit_execution_intent(
    *,
    db_target: str,
    execution_intent_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    engine_facts = getattr(storage, "engine_facts", None)
    if not execution_store.intent_schema_ready():
        raise ValueError("Execution intent tables are not available yet.")
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
    state = normalize_execution_intent_state(intent.get("state"))
    if state not in {"pending", "claimed"}:
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": "Execution intent is no longer pending submission.",
            "execution_intent": intent,
        }

    target_active, inactive_reason = _intent_target_is_active(
        intent=dict(intent),
        execution_store=execution_store,
        engine_facts=engine_facts,
    )
    if not target_active:
        revoked_intent = _update_intent(
            execution_store,
            dict(intent),
            state="revoked",
            payload_updates={
                "dispatch_status": "revoked",
                "revoke_reason": inactive_reason,
            },
            updated_at=utc_now_iso(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="revoked",
            payload={"reason": inactive_reason},
        )
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": (
                "Execution intent target is no longer active."
                if inactive_reason is None
                else f"Execution intent target is no longer active: {inactive_reason}"
            ),
            "execution_intent": revoked_intent,
        }

    claim_token = str(intent.get("claim_token") or uuid4().hex)
    claimed_intent = _update_intent(
        execution_store,
        dict(intent),
        state="claimed",
        payload_updates={"dispatch_status": "claimed"},
        updated_at=utc_now_iso(),
    )
    if not as_text(claimed_intent.get("claim_token")):
        claimed_intent = execution_store.upsert_execution_intent(
            execution_intent_id=str(claimed_intent["execution_intent_id"]),
            trading_strategy_id=str(claimed_intent["trading_strategy_id"]),
            trade_signal_id=as_text(claimed_intent.get("trade_signal_id")),
            trade_decision_id=as_text(claimed_intent.get("trade_decision_id")),
            strategy_position_id=as_text(claimed_intent.get("strategy_position_id")),
            execution_attempt_id=as_text(claimed_intent.get("execution_attempt_id")),
            action_type=str(claimed_intent["action_type"]),
            slot_key=str(claimed_intent["slot_key"]),
            claim_token=claim_token,
            policy_ref=dict(claimed_intent.get("policy_ref") or {}),
            config_hash=str(claimed_intent.get("config_hash") or ""),
            state="claimed",
            expires_at=as_text(claimed_intent.get("expires_at")),
            superseded_by_id=as_text(claimed_intent.get("superseded_by_id")),
            payload=_intent_payload(claimed_intent),
            created_at=str(claimed_intent["created_at"]),
            updated_at=utc_now_iso(),
        )
    _append_event(
        execution_store,
        execution_intent_id=execution_intent_id,
        event_type="claimed",
        payload={"claim_token": claim_token},
    )

    source_intent = dict(claimed_intent)
    payload = _intent_payload(source_intent)
    policy_ref = dict(source_intent.get("policy_ref") or {})
    execution_policy = _intent_execution_policy(source_intent)
    exit_policy = _intent_exit_policy(source_intent)
    engine_ref_metadata = _intent_engine_ref_metadata(source_intent)

    try:
        if source_intent.get("trade_decision_id"):
            option_payload = _trade_decision_option_payload(
                engine_facts=engine_facts,
                intent=source_intent,
            )
            source_metadata = option_payload.get("source") if isinstance(option_payload.get("source"), dict) else {}
            option_request_metadata = {
                "execution_intent_id": execution_intent_id,
                "trading_strategy_id": source_intent.get("trading_strategy_id"),
                "trade_structure": option_payload.get("trade_structure") or policy_ref.get("trade_structure"),
                "routine": policy_ref.get("routine"),
                "config_hash": source_intent.get("config_hash"),
                "approval_mode": payload.get("approval_mode"),
                "execution_mode": payload.get("execution_mode"),
                "validation_provenance": as_text(payload.get("validation_provenance")) or "natural_strategy",
                "trade_intent": option_payload.get("trade_intent") or "open",
                "execution_policy": execution_policy,
                "exit_policy": exit_policy,
                "execution_admission": payload.get("execution_admission"),
                "source": dict(source_metadata),
                "option_selection": (dict(option_payload["option_selection"]) if isinstance(option_payload.get("option_selection"), dict) else None),
                "underlying_price": option_payload.get("underlying_price"),
                "profile": as_text(option_payload.get("profile")),
                **_repricing_metadata(payload),
                **engine_ref_metadata,
            }
            if option_payload.get("execution_kind") == "legacy_single_leg":
                result = submit_option_order(
                    db_target=db_target,
                    symbol=str(option_payload["symbol"]),
                    side=str(option_payload["side"]),
                    quantity=int(option_payload.get("quantity") or 1),
                    limit_price=float(option_payload["limit_price"]),
                    time_in_force=str(option_payload.get("time_in_force") or "day"),
                    label=str(option_payload.get("label") or source_intent["trading_strategy_id"]),
                    market_date=as_text(option_payload.get("market_date")),
                    underlying_symbol=as_text(option_payload.get("underlying_symbol")) or as_text(option_payload.get("root_symbol")),
                    strategy_family=as_text(option_payload.get("strategy_family")) or "long_call",
                    expiration_date=as_text(option_payload.get("expiration_date")),
                    option_type=as_text(option_payload.get("option_type")),
                    strike=(None if option_payload.get("strike") in (None, "") else float(option_payload["strike"])),
                    execution_runtime=payload.get("execution_runtime"),
                    request_metadata=option_request_metadata,
                    queue_submission=True,
                    storage=storage,
                )
            else:
                result = submit_option_structure_order(
                    db_target=db_target,
                    legs=list(option_payload.get("legs") or []),
                    quantity=int(option_payload.get("quantity") or 1),
                    limit_price=float(option_payload["limit_price"]),
                    order_payload=(dict(option_payload["order_payload"]) if isinstance(option_payload.get("order_payload"), dict) else None),
                    label=str(option_payload.get("label") or source_intent["trading_strategy_id"]),
                    market_date=as_text(option_payload.get("market_date")),
                    underlying_symbol=as_text(option_payload.get("underlying_symbol")) or as_text(option_payload.get("root_symbol")),
                    strategy_family=as_text(option_payload.get("strategy_family")) or "long_call",
                    expiration_date=as_text(option_payload.get("expiration_date")),
                    execution_runtime=payload.get("execution_runtime"),
                    request_metadata=option_request_metadata,
                    economics=dict(option_payload["economics"]) if isinstance(option_payload.get("economics"), dict) else None,
                    candidate=dict(option_payload["candidate"]) if isinstance(option_payload.get("candidate"), dict) else None,
                    queue_submission=True,
                    storage=storage,
                )
        elif source_intent.get("strategy_position_id"):
            close_request_metadata = {
                "execution_intent_id": execution_intent_id,
                "trading_strategy_id": source_intent.get("trading_strategy_id"),
                "trade_structure": policy_ref.get("trade_structure"),
                "routine": policy_ref.get("routine"),
                "config_hash": source_intent.get("config_hash"),
                "execution_runtime": payload.get("execution_runtime"),
                "approval_mode": payload.get("approval_mode"),
                "execution_mode": payload.get("execution_mode"),
                "validation_provenance": as_text(payload.get("validation_provenance")) or "natural_strategy",
                "execution_policy": execution_policy,
                "exit_policy": exit_policy,
                **_repricing_metadata(payload),
                **engine_ref_metadata,
            }
            if isinstance(payload.get("source"), dict):
                close_request_metadata["source"] = dict(payload["source"])
            if isinstance(payload.get("close_decision"), dict):
                close_request_metadata["close_decision"] = dict(payload["close_decision"])
            if isinstance(payload.get("risk_policy"), dict):
                close_request_metadata["risk_policy"] = dict(payload["risk_policy"])
            result = submit_position_close_by_id(
                db_target=db_target,
                position_id=str(source_intent["strategy_position_id"]),
                limit_price=(None if payload.get("limit_price") in (None, "") else float(payload["limit_price"])),
                request_metadata=close_request_metadata,
                storage=storage,
            )
        elif (equity_payload := _equity_intent_payload(source_intent)) is not None:
            source_metadata = equity_payload.get("source") if isinstance(equity_payload.get("source"), dict) else {}
            result = submit_equity_order(
                db_target=db_target,
                symbol=str(equity_payload["symbol"]),
                side=str(equity_payload.get("side") or source_intent["action_type"]),
                quantity=int(equity_payload.get("quantity") or 1),
                limit_price=float(equity_payload["limit_price"]),
                time_in_force=str(equity_payload.get("time_in_force") or "day"),
                label=str(equity_payload.get("label") or "research_equity"),
                market_date=as_text(equity_payload.get("market_date")),
                execution_runtime=equity_payload.get("execution_runtime"),
                request_metadata={
                    "execution_intent_id": execution_intent_id,
                    "trading_strategy_id": source_intent.get("trading_strategy_id"),
                    "trade_structure": policy_ref.get("trade_structure"),
                    "routine": policy_ref.get("routine"),
                    "config_hash": source_intent.get("config_hash"),
                    "position_id": as_text(equity_payload.get("position_id")),
                    "trade_intent": as_text(equity_payload.get("trade_intent")),
                    "approval_mode": as_text(equity_payload.get("approval_mode")),
                    "execution_mode": as_text(equity_payload.get("execution_mode")),
                    "validation_provenance": as_text(equity_payload.get("validation_provenance")),
                    "execution_policy": execution_policy,
                    "exit_policy": (dict(equity_payload["exit_policy"]) if isinstance(equity_payload.get("exit_policy"), dict) else None),
                    "risk_policy": (dict(equity_payload["risk_policy"]) if isinstance(equity_payload.get("risk_policy"), dict) else None),
                    "source": dict(source_metadata),
                    "close_decision": (dict(equity_payload["close_decision"]) if isinstance(equity_payload.get("close_decision"), dict) else None),
                    **engine_ref_metadata,
                },
                storage=storage,
            )
        elif (option_payload := _option_intent_payload(source_intent)) is not None:
            source_metadata = option_payload.get("source") if isinstance(option_payload.get("source"), dict) else {}
            option_execution_policy = (
                dict(option_payload["execution_policy"]) if isinstance(option_payload.get("execution_policy"), dict) else execution_policy
            )
            result = submit_option_order(
                db_target=db_target,
                symbol=str(option_payload["symbol"]),
                side=str(option_payload.get("side") or source_intent["action_type"]),
                quantity=int(option_payload.get("quantity") or 1),
                limit_price=float(option_payload["limit_price"]),
                time_in_force=str(option_payload.get("time_in_force") or "day"),
                label=str(option_payload.get("label") or "research_option"),
                market_date=as_text(option_payload.get("market_date")),
                underlying_symbol=as_text(option_payload.get("underlying_symbol")) or as_text(option_payload.get("root_symbol")),
                strategy_family=as_text(option_payload.get("strategy_family")) or "long_call",
                expiration_date=as_text(option_payload.get("expiration_date")),
                option_type=as_text(option_payload.get("option_type")),
                strike=(None if option_payload.get("strike") in (None, "") else float(option_payload["strike"])),
                execution_runtime=option_payload.get("execution_runtime"),
                request_metadata={
                    "execution_intent_id": execution_intent_id,
                    "trading_strategy_id": source_intent.get("trading_strategy_id"),
                    "trade_structure": policy_ref.get("trade_structure"),
                    "routine": policy_ref.get("routine"),
                    "config_hash": source_intent.get("config_hash"),
                    "position_id": as_text(option_payload.get("position_id")),
                    "trade_intent": as_text(option_payload.get("trade_intent")),
                    "approval_mode": as_text(option_payload.get("approval_mode")),
                    "execution_mode": as_text(option_payload.get("execution_mode")),
                    "validation_provenance": as_text(option_payload.get("validation_provenance")),
                    "profile": as_text(option_payload.get("profile")),
                    "execution_policy": option_execution_policy,
                    "exit_policy": (dict(option_payload["exit_policy"]) if isinstance(option_payload.get("exit_policy"), dict) else None),
                    "risk_policy": (dict(option_payload["risk_policy"]) if isinstance(option_payload.get("risk_policy"), dict) else None),
                    "source": dict(source_metadata),
                    "close_decision": (dict(option_payload["close_decision"]) if isinstance(option_payload.get("close_decision"), dict) else None),
                    "underlying_price": option_payload.get("underlying_price"),
                    **_repricing_metadata(option_payload),
                    "option_selection": (
                        dict(option_payload["option_selection"]) if isinstance(option_payload.get("option_selection"), dict) else None
                    ),
                    **engine_ref_metadata,
                },
                queue_submission=bool(coerce_bool(option_payload.get("queue_submission"), default=False)),
                storage=storage,
            )
        else:
            raise ValueError(f"Execution intent {execution_intent_id} is missing its source reference")
    except ExecutionAdmissionError as exc:
        failed_intent = _update_intent(
            execution_store,
            dict(claimed_intent),
            state="failed",
            payload_updates={
                "dispatch_status": "failed",
                "error": str(exc),
                "execution_admission": dict(exc.admission),
            },
            updated_at=utc_now_iso(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="failed",
            payload={
                "error": str(exc),
                "execution_admission": dict(exc.admission),
            },
        )
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": str(exc),
            "execution_intent": failed_intent,
        }
    except Exception as exc:
        failed_intent = _update_intent(
            execution_store,
            dict(claimed_intent),
            state="failed",
            payload_updates={"dispatch_status": "failed", "error": str(exc)},
            updated_at=utc_now_iso(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="failed",
            payload={"error": str(exc)},
        )
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": str(exc),
            "execution_intent": failed_intent,
        }

    attempt = result.get("attempt") if isinstance(result.get("attempt"), dict) else None
    linked_attempt_id = None if attempt is None else as_text(attempt.get("execution_attempt_id"))
    admission_decision_id = as_text(engine_ref_metadata.get("admission_decision_id"))
    if linked_attempt_id is not None and admission_decision_id is not None and engine_facts is not None and engine_facts.schema_ready():
        try:
            engine_facts.attach_trade_admission_attempt(
                admission_decision_id=admission_decision_id,
                execution_attempt_id=linked_attempt_id,
            )
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "trade_admission_attempt_attach_failed",
                exc_info=True,
                execution_intent_id=execution_intent_id,
                execution_attempt_id=linked_attempt_id,
                admission_decision_id=admission_decision_id,
                error=str(exc),
            )
    next_state = _attempt_state(attempt)
    attempt_request = attempt.get("request") if isinstance(attempt, dict) and isinstance(attempt.get("request"), dict) else {}
    execution_admission = attempt_request.get("execution_admission")
    linked_intent = _update_intent(
        execution_store,
        dict(claimed_intent),
        state=next_state,
        execution_attempt_id=linked_attempt_id,
        payload_updates={
            "dispatch_status": next_state,
            **({} if linked_attempt_id is None else {"execution_attempt_id": linked_attempt_id}),
            **({} if not isinstance(execution_admission, dict) else {"execution_admission": dict(execution_admission)}),
        },
        updated_at=utc_now_iso(),
    )
    _append_event(
        execution_store,
        execution_intent_id=execution_intent_id,
        event_type="queued_for_submission" if linked_attempt_id is not None else "submit_noop",
        payload={
            "execution_attempt_id": linked_attempt_id,
            "attempt_status": None if attempt is None else attempt.get("status"),
            "changed": bool(result.get("changed", False)),
        },
    )
    return {
        "action": "submit_execution_intent",
        "changed": True,
        "result": result,
        "execution_intent": linked_intent,
    }


@with_storage()
def dispatch_pending_execution_intents(
    *,
    db_target: str,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}

    batch_limit = max(int(limit), 1)
    client = create_alpaca_client_from_env()
    trading_environment = resolve_trading_environment(client.trading_base_url)
    intent_owner_cleanup = _cleanup_inactive_strategy_intents(
        execution_store=execution_store,
        limit=batch_limit,
    )
    intent_cleanup = _cleanup_terminal_intent_history(
        execution_store,
        limit=batch_limit,
    )
    position_linkage = _backfill_strategy_position_links(
        execution_store,
        limit=batch_limit,
    )
    slot_cleanup = _cleanup_slot_conflicts(
        execution_store,
        limit=batch_limit,
    )
    active_management = _manage_submitted_open_intents(
        db_target=db_target,
        storage=storage,
        execution_store=execution_store,
        limit=batch_limit,
    )
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=["pending"],
            limit=batch_limit * 5,
        )
    ]
    intents.sort(key=lambda row: parse_datetime(as_text(row.get("created_at"))) or datetime.min.replace(tzinfo=UTC))
    submitted = 0
    skipped = 0
    expired = 0
    failed = 0
    reviewed = 0
    results: list[dict[str, Any]] = []
    for intent in intents:
        if reviewed >= batch_limit:
            break
        reviewed += 1
        execution_intent_id = str(intent["execution_intent_id"])
        expires_at = parse_datetime(as_text(intent.get("expires_at")))
        if expires_at is not None and expires_at <= datetime.now(UTC):
            updated = _update_intent(
                execution_store,
                intent,
                state="expired",
                payload_updates={
                    "dispatch_status": "expired",
                    "expire_reason": PRE_DISPATCH_EXPIRE_REASON,
                },
                updated_at=utc_now_iso(),
            )
            _append_event(
                execution_store,
                execution_intent_id=execution_intent_id,
                event_type="expired",
                payload={"reason": PRE_DISPATCH_EXPIRE_REASON},
            )
            expired += 1
            results.append(
                {
                    "execution_intent_id": execution_intent_id,
                    "status": "expired",
                    "reason": PRE_DISPATCH_EXPIRE_REASON,
                    "intent": updated,
                }
            )
            continue

        allowed, reason = _auto_execution_gate(
            intent=intent,
            trading_environment=trading_environment,
        )
        if not allowed:
            if reason == "paper_execution_requires_paper_environment":
                updated = _update_intent(
                    execution_store,
                    intent,
                    state="failed",
                    payload_updates={"dispatch_status": reason},
                    updated_at=utc_now_iso(),
                )
                _append_event(
                    execution_store,
                    execution_intent_id=execution_intent_id,
                    event_type="failed",
                    payload={
                        "reason": reason,
                        "trading_environment": trading_environment,
                    },
                )
                failed += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "failed",
                        "intent": updated,
                    }
                )
            else:
                skipped += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "pending",
                        "reason": reason,
                    }
                )
            continue

        result = submit_execution_intent(
            db_target=db_target,
            execution_intent_id=execution_intent_id,
            storage=storage,
        )
        final_intent = result.get("execution_intent") if isinstance(result.get("execution_intent"), dict) else None
        final_state = None if final_intent is None else str(final_intent.get("state") or "")
        if final_state == "failed":
            failed += 1
        elif final_state == "expired":
            expired += 1
        elif final_state in {"revoked", "pending", "claimed"}:
            skipped += 1
        else:
            submitted += 1
        results.append(
            {
                "execution_intent_id": execution_intent_id,
                "status": final_state or "submitted",
                "result": result,
            }
        )

    return {
        "status": "ok",
        "trading_environment": trading_environment,
        "intent_owner_cleanup": intent_owner_cleanup,
        "intent_cleanup": intent_cleanup,
        "position_linkage": position_linkage,
        "slot_cleanup": slot_cleanup,
        "active_management": active_management,
        "reviewed": reviewed,
        "submitted": submitted,
        "skipped": skipped,
        "expired": expired,
        "failed": failed,
        "results": results[:25],
    }


__all__ = [
    "PRE_DISPATCH_EXPIRE_REASON",
    "dispatch_pending_execution_intents",
    "request_execution_intent_dispatch",
    "submit_execution_intent",
]

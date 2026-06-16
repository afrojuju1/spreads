from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from core.money import premium_float
from core.services.positions import enrich_position_row
from core.services.position_lifecycle import build_close_decision_lifecycle
from core.services.trading_engine.close_policy import evaluate_exit_policy
from core.services.trading_strategies import routine_should_run_now
from core.services.trading_strategy_runtime import (
    find_management_runtime_for_position,
    resolve_management_runtimes,
)
from core.storage.serializers import parse_datetime
from core.value_coercion import as_text, coerce_float, coerce_int, utc_iso

from .exit_models import CloseDecisionResult, PositionExitSnapshot
from .kernel import EngineComponentRole, EngineRunRef
from .portfolio import PositionSnapshot
from .portfolio_runtime import build_position_snapshot
from .risk_runtime import has_active_close_intent, has_open_close_attempt

NEW_YORK = ZoneInfo("America/New_York")


def _time_reached(time_value: str | None, *, now: datetime) -> bool:
    rendered = as_text(time_value)
    if rendered is None:
        return False
    hour_text, separator, minute_text = rendered.partition(":")
    if separator != ":":
        return False
    current = now.astimezone(NEW_YORK)
    return (current.hour, current.minute) >= (int(hour_text), int(minute_text))


def _mark_age_seconds(position: Mapping[str, Any], *, now: datetime) -> float | None:
    marked_at = parse_datetime(as_text(position.get("close_marked_at")))
    if marked_at is None:
        return None
    return max((now - marked_at.astimezone(UTC)).total_seconds(), 0.0)


def _opening_signal(engine_facts: Any, position: Mapping[str, Any]) -> dict[str, Any]:
    trade_signal_id = as_text(position.get("trade_signal_id"))
    if trade_signal_id is None or engine_facts is None or not engine_facts.schema_ready():
        return {}
    row = engine_facts.get_trade_signal(trade_signal_id)
    return {} if row is None else dict(row)


def build_position_exit_snapshot(
    *,
    position: Mapping[str, Any],
    now: datetime,
    execution_store: Any | None = None,
    engine_facts: Any | None = None,
    broker_sync: Mapping[str, Any] | None = None,
    management_runtime: Any | None = None,
    management_runtime_state: str = "unknown",
) -> PositionExitSnapshot:
    payload = enrich_position_row(dict(position))
    position_id = str(payload["position_id"])
    exit_policy = payload.get("exit_policy") if isinstance(payload.get("exit_policy"), dict) else {}
    return PositionExitSnapshot(
        position_id=position_id,
        trading_strategy_id=as_text(payload.get("trading_strategy_id")),
        trade_structure=as_text(payload.get("strategy_family") or payload.get("strategy")),
        session_date=as_text(payload.get("session_date") or payload.get("market_date")),
        position_state=as_text(payload.get("position_status") or payload.get("status")) or "unknown",
        remaining_quantity=coerce_float(payload.get("remaining_quantity")) or 0.0,
        opened_quantity=coerce_float(payload.get("opened_quantity")),
        entry_value=coerce_float(payload.get("entry_value")) or coerce_float(payload.get("entry_credit")),
        entry_value_kind=as_text(payload.get("entry_value_kind")),
        close_mark=coerce_float(payload.get("close_mark")),
        close_mark_source=as_text(payload.get("close_mark_source")),
        close_marked_at=as_text(payload.get("close_marked_at")),
        close_mark_age_seconds=_mark_age_seconds(payload, now=now),
        quote_quality_state="awaiting_mark" if coerce_float(payload.get("close_mark")) is None else "mark_available",
        reconciliation_state=as_text(payload.get("reconciliation_status") or payload.get("reconciliation_state")),
        broker_sync_state=as_text((broker_sync or {}).get("status")),
        active_close_attempt=False if execution_store is None else has_open_close_attempt(execution_store, position_id=position_id),
        active_close_intent=False if execution_store is None else has_active_close_intent(execution_store, position_id=position_id),
        management_runtime_state=management_runtime_state,
        management_recipe_refs=tuple(getattr(management_runtime, "management_recipe_refs", ()) or ()),
        exit_policy=dict(exit_policy),
        opening_signal=_opening_signal(engine_facts, payload),
        position=payload,
    )


def close_decision_lifecycle(
    *,
    position: Mapping[str, Any],
    decision: Mapping[str, Any],
    decision_source: str | None = None,
    decided_at: str | None = None,
    exit_snapshot: PositionExitSnapshot | None = None,
    close_admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    close_decision = decision.get("close_decision")
    if isinstance(close_decision, Mapping):
        payload = dict(close_decision)
    else:
        payload = build_close_decision_lifecycle(
            position=position,
            decision=decision,
            decision_source=decision_source,
            decided_at=decided_at,
        )
    evidence = dict(payload.get("evidence") or {})
    if exit_snapshot is not None:
        evidence["exit_snapshot"] = exit_snapshot.to_payload()
    if close_admission is not None:
        evidence["close_admission"] = dict(close_admission)
    payload["evidence"] = evidence
    return payload


def build_blocked_close_decision(
    *,
    position: Mapping[str, Any],
    reason: str,
    decision_source: str,
    decided_at: str | None = None,
    exit_snapshot: PositionExitSnapshot | None = None,
    close_admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return close_decision_lifecycle(
        position=position,
        decision={
            "should_close": False,
            "reason": reason,
            "recipe_ref": None,
            "limit_price": None,
            "limit_price_source": None,
            "decision_source": decision_source,
            "decision_details": None,
        },
        decision_source=decision_source,
        decided_at=decided_at,
        exit_snapshot=exit_snapshot,
        close_admission=close_admission,
    )


def close_decision_row_fields(close_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "close_decision_id": close_decision.get("close_decision_id"),
        "close_decision_state": close_decision.get("decision_state"),
        "close_decision": dict(close_decision),
    }


def close_decision_projection(
    *,
    position_id: str,
    reason: str,
    decision_source: str,
    should_close: bool,
    exit_run_id: str,
    close_decision: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "position_id": position_id,
        "reason": reason,
        "decision_source": decision_source,
        "should_close": should_close,
        "exit_run_id": exit_run_id,
        **close_decision_row_fields(close_decision),
    }


def blocked_close_decision_projection(
    *,
    position: Mapping[str, Any],
    reason: str,
    decision_source: str,
    exit_run_id: str,
    decided_at: str | None = None,
    exit_snapshot: PositionExitSnapshot | None = None,
    close_admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    position_id = as_text(position.get("position_id")) or "unknown"
    close_decision = build_blocked_close_decision(
        position=position,
        reason=reason,
        decision_source=decision_source,
        decided_at=decided_at,
        exit_snapshot=exit_snapshot,
        close_admission=close_admission,
    )
    return close_decision_projection(
        position_id=position_id,
        reason=reason,
        decision_source=decision_source,
        should_close=False,
        exit_run_id=exit_run_id,
        close_decision=close_decision,
    )


def evaluate_position_close_decision(
    *,
    position: Mapping[str, Any],
    now: datetime,
    management_runtimes: tuple[Any, ...],
    execution_store: Any | None = None,
    engine_facts: Any | None = None,
    broker_sync: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], str, Any | None, PositionExitSnapshot]:
    position_payload = dict(position)
    runtime, runtime_reason = find_management_runtime_for_position(
        position_payload,
        runtimes=management_runtimes,
    )
    snapshot = build_position_exit_snapshot(
        position=position_payload,
        now=now,
        execution_store=execution_store,
        engine_facts=engine_facts,
        broker_sync=broker_sync,
        management_runtime=runtime,
        management_runtime_state=runtime_reason or ("matched" if runtime is not None else "missing"),
    )
    if runtime is None:
        if runtime_reason == "ambiguous_management_runtime":
            return (
                {
                    "should_close": False,
                    "reason": "ambiguous_management_runtime",
                    "recipe_ref": None,
                    "limit_price": None,
                    "limit_price_source": None,
                    "decision_source": "exit_engine",
                    "management_recipe_refs": [],
                    "decision_details": {"exit_snapshot": snapshot.to_payload()},
                },
                "exit_engine",
                None,
                snapshot,
            )
        policy_decision = evaluate_exit_policy(
            position=snapshot.to_position_payload(),
            mark=snapshot.close_mark,
            now=now,
        )
        policy_decision["decision_source"] = "position_exit_policy"
        policy_decision["management_recipe_refs"] = []
        policy_decision["decision_details"] = {
            key: value
            for key, value in policy_decision.items()
            if key
            in {
                "policy",
                "mark",
                "effective_mark",
                "mark_state",
                "entry_value",
                "premium_kind",
                "profit_target_mark",
                "stop_mark",
                "force_close_at",
                "max_quote_age_seconds",
                "quote_spread_pct",
                "quote_spread_state",
                "underlying_invalidation_state",
                "underlying_invalidation_reason",
            }
        }
        policy_decision["decision_details"]["exit_snapshot"] = snapshot.to_payload()
        return (
            policy_decision,
            "position_exit_policy",
            None,
            snapshot,
        )
    if runtime.strategy.management is None or not routine_should_run_now(runtime.strategy.management, now=now):
        return (
            {
                "should_close": False,
                "reason": "outside_management_schedule_window",
                "recipe_ref": None,
                "limit_price": None,
                "limit_price_source": None,
                "decision_source": "exit_engine",
                "management_recipe_refs": list(runtime.management_recipe_refs),
                "decision_details": {"exit_snapshot": snapshot.to_payload()},
            },
            "exit_engine",
            runtime,
            snapshot,
        )

    from core.services.management_planner import plan_position_management

    decision = plan_position_management(
        runtime=runtime,
        position=snapshot.to_position_payload(),
        flatten_due=_time_reached(runtime.strategy.runtime.flatten_positions_at_et, now=now),
        now=now,
    )
    decision["decision_source"] = "exit_engine"
    decision["management_recipe_refs"] = list(runtime.management_recipe_refs)
    details = dict(decision.get("decision_details") or {})
    details["exit_snapshot"] = snapshot.to_payload()
    decision["decision_details"] = details
    return (decision, "exit_engine", runtime, snapshot)


def describe_position_exit_state(
    *,
    position: dict[str, Any],
    now: datetime | None = None,
    management_runtimes: tuple[Any, ...] | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(UTC)
    runtimes = tuple(resolve_management_runtimes()) if management_runtimes is None else tuple(management_runtimes)
    decision, decision_source, _runtime, snapshot = evaluate_position_close_decision(
        position=position,
        now=current_time,
        management_runtimes=runtimes,
    )
    close_decision = close_decision_lifecycle(
        position=position,
        decision=decision,
        decision_source=decision_source,
        decided_at=utc_iso(current_time),
        exit_snapshot=snapshot,
    )
    details = dict(decision.get("decision_details") or {}) if isinstance(decision.get("decision_details"), dict) else {}
    return {
        "decision_source": as_text(decision.get("decision_source")),
        "management_recipe_refs": [str(value) for value in list(decision.get("management_recipe_refs") or []) if str(value or "").strip()],
        "should_close": bool(decision.get("should_close")),
        "reason": str(decision.get("reason") or "unknown"),
        "close_decision_state": close_decision.get("decision_state"),
        "close_decision_id": close_decision.get("close_decision_id"),
        "close_decision": close_decision,
        "recipe_ref": as_text(decision.get("recipe_ref")),
        "limit_price": coerce_float(decision.get("limit_price")),
        "limit_price_source": as_text(decision.get("limit_price_source")),
        "current_mark": premium_float(details.get("mark")),
        "effective_mark": premium_float(details.get("effective_mark")),
        "mark_state": as_text(details.get("mark_state")),
        "entry_value": premium_float(details.get("entry_value")),
        "premium_kind": as_text(details.get("premium_kind")),
        "profit_target_mark": premium_float(details.get("profit_target_mark")),
        "stop_mark": premium_float(details.get("stop_mark")),
        "force_close_at": as_text(details.get("force_close_at")),
        "quote_spread_state": as_text(details.get("quote_spread_state")),
        "underlying_invalidation_state": as_text(details.get("underlying_invalidation_state")),
    }


class ExitEngine:
    def __init__(
        self,
        *,
        execution_store: Any,
        engine_facts: Any | None = None,
        now: datetime | None = None,
        management_runtimes: tuple[Any, ...] | None = None,
        broker_sync: Mapping[str, Any] | None = None,
    ) -> None:
        self.execution_store = execution_store
        self.engine_facts = engine_facts
        self.now = now or datetime.now(UTC)
        self.management_runtimes = management_runtimes
        self.broker_sync = {} if broker_sync is None else dict(broker_sync)

    def evaluate_close(
        self,
        *,
        run_ref: EngineRunRef,
        position: PositionSnapshot,
    ) -> CloseDecisionResult:
        decision, decision_source, management_runtime, exit_snapshot = evaluate_position_close_decision(
            position=dict(position.payload),
            now=self.now,
            management_runtimes=tuple(resolve_management_runtimes() if self.management_runtimes is None else self.management_runtimes),
            execution_store=self.execution_store,
            engine_facts=self.engine_facts,
            broker_sync=self.broker_sync,
        )
        close_decision = close_decision_lifecycle(
            position=dict(position.payload),
            decision=decision,
            decision_source=decision_source,
            decided_at=utc_iso(self.now),
            exit_snapshot=exit_snapshot,
        )
        reason = str(decision.get("reason") or close_decision.get("reason") or "unknown")
        return CloseDecisionResult(
            run_ref=run_ref,
            close_decision_id=str(close_decision["close_decision_id"]),
            position_id=position.position_id,
            state=str(close_decision.get("decision_state") or "unknown"),
            reason_codes=(reason,),
            payload={
                "decision": {**dict(decision), "close_decision": close_decision},
                "decision_source": decision_source,
                "management_runtime": management_runtime,
                "exit_snapshot": exit_snapshot.to_payload(),
                "close_decision": close_decision,
            },
        )


def build_exit_run_ref(
    *,
    trading_strategy_id: str | None = None,
    job_run_id: str | None = None,
    now: datetime | None = None,
) -> EngineRunRef:
    timestamp = utc_iso(now or datetime.now(UTC))
    return EngineRunRef(
        role=EngineComponentRole.EXIT,
        run_id=f"exit:manage:{timestamp}",
        trading_strategy_id=trading_strategy_id,
        job_run_id=job_run_id,
    )


def persist_close_decision(
    engine_facts: Any,
    *,
    position: Mapping[str, Any],
    close_decision: Mapping[str, Any],
) -> dict[str, Any]:
    if engine_facts is None or not getattr(engine_facts, "close_lifecycle_schema_ready", lambda: False)():
        return {"status": "skipped", "reason": "close_lifecycle_schema_unavailable"}
    position_payload = enrich_position_row(dict(position))
    engine_facts.upsert_trade_position_from_portfolio_position(position=position_payload)
    row = engine_facts.upsert_trade_close_decision(
        close_decision_id=str(close_decision["close_decision_id"]),
        position_id=str(close_decision["position_id"]),
        decision_state=str(close_decision["decision_state"]),
        reason=str(close_decision["reason"]),
        quantity_to_close=coerce_float(close_decision.get("quantity_to_close")),
        limit_source=as_text(close_decision.get("limit_source")),
        limit_price=coerce_float(close_decision.get("limit_price")),
        mark_source=as_text(close_decision.get("mark_source")),
        policy_snapshot=dict(close_decision.get("policy_snapshot") or {}),
        reason_codes=[str(value) for value in close_decision.get("reason_codes") or [] if str(value or "").strip()],
        blockers=[str(value) for value in close_decision.get("blockers") or [] if str(value or "").strip()],
        evidence=dict(close_decision.get("evidence") or {}),
        metrics=dict(close_decision.get("metrics") or {}),
        decided_at=str(close_decision["decided_at"]),
        execution_intent_id=as_text(close_decision.get("execution_intent_id")),
    )
    return {"status": "recorded", "close_decision_id": row.get("close_decision_id")}


def attach_close_decision_intent(
    engine_facts: Any,
    *,
    close_decision_id: str,
    execution_intent_id: str,
) -> dict[str, Any]:
    if engine_facts is None or not getattr(engine_facts, "close_lifecycle_schema_ready", lambda: False)():
        return {"status": "skipped", "reason": "close_lifecycle_schema_unavailable"}
    row = engine_facts.attach_trade_close_decision_intent(
        close_decision_id=close_decision_id,
        execution_intent_id=execution_intent_id,
    )
    return {"status": "missing"} if row is None else {"status": "recorded", "close_decision_id": row.get("close_decision_id")}


def persist_close_intent_admission(
    engine_facts: Any,
    *,
    intent: Mapping[str, Any],
    close_decision: Mapping[str, Any],
    close_admission: Mapping[str, Any],
    runtime: Any,
    position: Mapping[str, Any],
) -> dict[str, Any]:
    if engine_facts is None or not getattr(engine_facts, "close_lifecycle_schema_ready", lambda: False)():
        return {"status": "skipped", "reason": "close_lifecycle_schema_unavailable"}
    execution_intent_id = str(intent["execution_intent_id"])
    payload = intent.get("payload") if isinstance(intent.get("payload"), Mapping) else {}
    policy_ref = intent.get("policy_ref") if isinstance(intent.get("policy_ref"), Mapping) else {}
    created_at = as_text(intent.get("created_at")) or utc_iso(datetime.now(UTC))
    updated_at = as_text(intent.get("updated_at")) or created_at
    engine_facts.upsert_trade_execution_intent(
        execution_intent_id=execution_intent_id,
        intent_kind="close",
        source_object_type="close_decision",
        source_object_id=str(close_decision["close_decision_id"]),
        trade_signal_id=None,
        trade_decision_id=None,
        position_id=str(position["position_id"]),
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        routine="manage",
        account_id=as_text(position.get("account_id")),
        slot_key=str(intent["slot_key"]),
        idempotency_key=execution_intent_id,
        intent_state=str(intent["state"]),
        claim_token=as_text(intent.get("claim_token")),
        claimed_at=as_text(intent.get("claimed_at")),
        expires_at=as_text(intent.get("expires_at")),
        supersedes_intent_id=None,
        superseded_by_intent_id=as_text(intent.get("superseded_by_id")),
        payload=dict(payload),
        policy_snapshot=dict(policy_ref),
        config_hash=runtime.config_hash,
        created_at=created_at,
        updated_at=updated_at,
    )
    engine_facts.upsert_trade_admission(
        admission_decision_id=str(close_admission["admission_decision_id"]),
        execution_intent_id=execution_intent_id,
        trade_signal_id=None,
        trade_decision_id=None,
        position_id=str(position["position_id"]),
        admission_kind=str(close_admission["admission_kind"]),
        admission_state=str(close_admission["admission_state"]),
        account_id=as_text(close_admission.get("account_id")),
        session_date=as_text(close_admission.get("session_date")) or as_text(position.get("session_date") or position.get("market_date")),
        requested_quantity=coerce_int(close_admission.get("requested_quantity")),
        requested_notional=coerce_float(close_admission.get("requested_notional")),
        max_loss=coerce_float(close_admission.get("max_loss")),
        policy_snapshot=dict(close_admission.get("policy_snapshot") or {}),
        capability_snapshot=dict(close_admission.get("capability_snapshot") or {}),
        metrics=dict(close_admission.get("metrics") or {}),
        reason_codes=[str(value) for value in close_admission.get("reason_codes") or [] if str(value or "").strip()],
        blockers=[str(value) for value in close_admission.get("blockers") or [] if str(value or "").strip()],
        evidence=dict(close_admission.get("evidence") or {}),
        note=as_text(close_admission.get("message")),
        execution_attempt_id=None,
        decided_at=str(close_admission["decided_at"]),
    )
    return {
        "status": "recorded",
        "execution_intent_id": execution_intent_id,
        "admission_decision_id": close_admission.get("admission_decision_id"),
    }


__all__ = [
    "ExitEngine",
    "blocked_close_decision_projection",
    "build_blocked_close_decision",
    "build_exit_run_ref",
    "build_position_exit_snapshot",
    "build_position_snapshot",
    "close_decision_lifecycle",
    "close_decision_projection",
    "close_decision_row_fields",
    "describe_position_exit_state",
    "evaluate_position_close_decision",
    "persist_close_decision",
    "persist_close_intent_admission",
    "attach_close_decision_intent",
]

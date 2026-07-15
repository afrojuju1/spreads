from __future__ import annotations

from typing import Any

from core.alerts.runtime import plan_runtime_entry_selected_alert
from core.services.admission_lifecycle import admission_allows_attempt
from core.services.entry_planner import plan_entry_selection
from core.services.execution_intents import request_execution_lifecycle_start
from core.services.execution_intents.shared import (
    ACTIVE_INTENT_STATES,
)
from core.services.runtime_identity import build_runtime_policy_ref
from core.services.strategy_analytics import evaluate_trading_strategy_entry_controls
from core.services.trading_engine.entry_admission import build_selected_entry_admission_snapshot
from core.services.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.entry_signals import (
    NATURAL_ENTRY_PROVENANCE,
    OBSERVATION_ENTRY_PROVENANCE,
    candidate_payload,
    quality_evidence_summary,
)
from core.services.trading_engine.facts import entry_trade_decision_id
from core.services.trading_engine.close_policy import resolve_exit_policy_snapshot
from core.services.trading_strategies import routine_should_run_now
from core.services.trading_strategy_runtime import resolve_entry_observation_runtime, resolve_entry_runtime
from core.value_coercion import coerce_int, utc_expiry_iso, utc_now_iso as _utc_now


from core.services.trading_engine.strategy_runtime_admission import _persist_trade_admission_handoff
from core.services.trading_engine.strategy_runtime_signals import _refresh_entry_runtime_signals
from core.services.trading_engine.strategy_runtime_support import (
    _intent_id,
    _market_date_today,
    _slot_key,
    _trade_decision_state,
    _trade_signal_id_for_signal,
)


def _run_trading_strategy_entry(
    *,
    db_target: str,
    trading_strategy_id: str,
    market_date: str | None = None,
    planner_job_run_id: str | None = None,
    run_key: str | None = None,
    storage: Any | None = None,
    observation_only: bool = False,
    respect_schedule: bool = True,
) -> dict[str, Any]:
    signal_store = storage.signals
    execution_store = storage.execution
    job_store = storage.jobs
    if not signal_store.schema_ready():
        return {"status": "skipped", "reason": "signal_schema_unavailable"}
    if not signal_store.strategy_runtime_schema_ready():
        return {"status": "skipped", "reason": "strategy_runtime_schema_unavailable"}
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}
    engine_facts = getattr(storage, "engine_facts", None)
    if engine_facts is None or not engine_facts.schema_ready():
        return {"status": "skipped", "reason": "engine_fact_schema_unavailable"}

    runtime = (
        resolve_entry_observation_runtime(trading_strategy_id=trading_strategy_id)
        if observation_only
        else resolve_entry_runtime(trading_strategy_id=trading_strategy_id)
    )
    provenance = OBSERVATION_ENTRY_PROVENANCE if observation_only else NATURAL_ENTRY_PROVENANCE
    if runtime.strategy.entry is None or (respect_schedule and not routine_should_run_now(runtime.strategy.entry)):
        return {
            "status": "skipped",
            "reason": "outside_schedule_window",
            "trading_strategy_id": runtime.trading_strategy_id,
            "entry_run_mode": "observation" if observation_only else "natural",
            "validation_provenance": provenance,
            "observation_only": observation_only,
        }

    resolved_market_date = market_date or _market_date_today()
    run_kind = "entry_observation" if observation_only else "entry"
    run_key = run_key or f"strategy:{runtime.trading_strategy_id}:{run_kind}:{_utc_now()}"
    scope_key = f"{run_kind}:{runtime.trading_strategy_id}:{resolved_market_date}"
    policy_ref = build_runtime_policy_ref(
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        routine="entry",
        market_date=resolved_market_date,
    )
    policy_ref = {
        **policy_ref,
        "entry_run_mode": "observation" if observation_only else "natural",
        "validation_provenance": provenance,
        "observation_only": observation_only,
        "protection_model_id": runtime.strategy.protection.profile_id,
        "protection_rule_count": len(runtime.strategy.protection.rules),
    }
    candidate_generation = _refresh_entry_runtime_signals(
        db_target=db_target,
        storage=storage,
        runtime=runtime,
        market_date=resolved_market_date,
        run_key=run_key,
        planner_job_run_id=planner_job_run_id,
        observation_only=observation_only,
    )
    if str(candidate_generation.get("status") or "") == "skipped":
        return {
            "status": "skipped",
            "reason": candidate_generation.get("reason"),
            "trading_strategy_id": runtime.trading_strategy_id,
            "market_date": resolved_market_date,
            "run_key": run_key,
            "entry_run_mode": "observation" if observation_only else "natural",
            "validation_provenance": provenance,
            "observation_only": observation_only,
            "candidate_generation": candidate_generation,
        }
    signals = [dict(row) for row in list(candidate_generation.get("signals") or []) if isinstance(row, dict)]
    min_score = float(runtime.trigger_policy.get("min_signal_score") or 0.0)
    controls_allowed, controls_reason, strategy_metrics = evaluate_trading_strategy_entry_controls(
        storage=storage,
        strategy=runtime.strategy,
        market_date=resolved_market_date,
    )
    plan = plan_entry_selection(
        signals=signals,
        controls_allowed=controls_allowed,
        controls_reason=controls_reason,
        bot_metrics=strategy_metrics,
        min_score=min_score,
        eligible_selection_states=("promotable",),
    )
    selected = plan["selected"]

    decisions: list[dict[str, Any]] = []
    admissions: list[dict[str, Any]] = []
    selected_intent: dict[str, Any] | None = None
    selected_decision: dict[str, Any] | None = None
    selected_signal: dict[str, Any] | None = None
    selected_execution_admission: dict[str, Any] | None = None
    lifecycle_start_job_run_id: str | None = None
    for decision_plan, signal in zip(plan["decisions"], signals, strict=False):
        candidate = candidate_payload(signal)
        candidate_identity = str(
            signal.get("candidate_identity") or resolve_candidate_identity(candidate, strategy=candidate.get("strategy"))
        ).strip()
        slot_key = _slot_key(
            runtime.trading_strategy_id,
            str(signal.get("underlying_symbol") or ""),
        )
        trade_signal_id = _trade_signal_id_for_signal(
            candidate_generation=candidate_generation,
            runtime=runtime,
            market_date=resolved_market_date,
            signal=signal,
        )
        if trade_signal_id is None:
            continue
        trade_decision_state = _trade_decision_state(decision_plan["state"])
        reason_codes = list(decision_plan["reason_codes"])
        evidence = {
            "policy_ref": policy_ref,
            "decision_plan": dict(decision_plan["payload"]),
            "candidate_identity": candidate_identity,
            "underlying_symbol": signal.get("underlying_symbol"),
            **quality_evidence_summary(signal),
            "candidate_generation": {
                "candidate_run_id": (
                    (candidate_generation.get("engine_facts") or {}).get("candidate_run_id")
                    if isinstance(candidate_generation.get("engine_facts"), dict)
                    else None
                ),
                "ticker_source_run_id": (
                    (candidate_generation.get("engine_facts") or {}).get("ticker_source_run_id")
                    if isinstance(candidate_generation.get("engine_facts"), dict)
                    else None
                ),
            },
        }
        if str(decision_plan["state"]) == "selected":
            existing_active = execution_store.list_execution_intents(
                slot_key=slot_key,
                states=sorted(ACTIVE_INTENT_STATES),
                limit=1,
            )
            if existing_active:
                trade_decision_state = "selected_blocked"
                reason_codes = ["active_execution_intent_exists"]
                evidence["slot_key"] = slot_key
        if observation_only and trade_decision_state == "selected":
            trade_decision_state = "no_entry"
            reason_codes = ["observation_only_signal_not_entry_eligible"]
            evidence["observation_only"] = True
        trade_decision_id = entry_trade_decision_id(run_key=run_key, trade_signal_id=trade_signal_id)
        decision = engine_facts.upsert_trade_decision(
            trade_decision_id=trade_decision_id,
            trade_signal_id=trade_signal_id,
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            config_hash=runtime.config_hash,
            run_key=run_key,
            scope_key=scope_key,
            decision_state=trade_decision_state,
            score=float(decision_plan["score"]),
            rank=int(decision_plan["rank"]),
            selected_quantity=None,
            selected_execution_shape=dict(signal.get("execution_shape") or {}),
            reason_codes=reason_codes,
            blockers=(reason_codes if trade_decision_state in {"skip", "selected_blocked"} else []),
            evidence=evidence,
            metrics=dict(strategy_metrics),
            supersedes_decision_id=None,
            superseded_by_decision_id=None,
            decided_at=_utc_now(),
        )
        decisions.append(decision)
        if observation_only:
            continue
        if trade_decision_state == "selected_blocked":
            continue
        if trade_decision_state != "selected":
            continue
        selected_execution_admission = build_selected_entry_admission_snapshot(
            engine_facts=engine_facts,
            execution_store=execution_store,
            runtime=runtime,
            decision=decision,
            signal=signal,
            market_date=resolved_market_date,
        )
        execution_intent_id = _intent_id(str(decision["trade_decision_id"]))
        open_execution_policy = runtime.strategy.execution.execution_policy_for_action("open")
        intent_expires_at = utc_expiry_iso(
            minutes=int(open_execution_policy["submit_ttl_minutes"]),
            minimum_seconds=60,
        )
        intent_payload: dict[str, Any] | None = None
        intent_created_event_payload: dict[str, Any] | None = None
        if admission_allows_attempt(selected_execution_admission):
            signal_execution_shape = dict(signal.get("execution_shape") or {})
            signal_order_payload = dict(signal.get("order_payload") or signal_execution_shape.get("order_payload") or {})
            signal_legs = list(signal.get("legs") or signal_execution_shape.get("legs") or [])
            signal_economics = dict(signal.get("economics") or {})
            intent_limit_price = (
                signal_order_payload.get("limit_price")
                or signal_economics.get("midpoint_credit")
                or signal_economics.get("midpoint_value")
                or signal.get("limit_price")
            )
            requested_quantity = coerce_int(selected_execution_admission.get("requested_quantity") or 1)
            if requested_quantity is None or requested_quantity <= 0:
                requested_quantity = 1
            admissible_quantity = coerce_int(selected_execution_admission.get("admissible_quantity"))
            if admissible_quantity is None or admissible_quantity <= 0:
                admissible_quantity = requested_quantity
            intent_quantity = min(requested_quantity, admissible_quantity)
            open_execution_policy = runtime.strategy.execution.execution_policy_for_action(
                "open",
                quantity=intent_quantity,
            )
            open_repricing_policy = dict(open_execution_policy.get("repricing_policy") or {})
            open_executor_profile = runtime.strategy.execution.executor_profile_snapshot("open")
            intent_payload = {
                "trade_signal_id": trade_signal_id,
                "trade_decision_id": decision["trade_decision_id"],
                "underlying_symbol": signal.get("underlying_symbol"),
                "trade_structure": runtime.trade_structure,
                "strategy_family": runtime.trade_structure,
                "candidate_identity": candidate_identity,
                "legs": signal_legs,
                "execution_shape": signal_execution_shape,
                "order_payload": signal_order_payload,
                "quantity": intent_quantity,
                "limit_price": intent_limit_price,
                "execution_mode": runtime.strategy.execution.mode,
                "approval_mode": runtime.strategy.execution.approval,
                "execution_runtime": runtime.strategy.execution.runtime,
                "executor_profile": open_executor_profile,
                "execution_policy": open_execution_policy,
                "repricing_policy": open_repricing_policy,
                "validation_provenance": NATURAL_ENTRY_PROVENANCE,
                "exit_policy": resolve_exit_policy_snapshot(
                    session_date=resolved_market_date,
                    payload=runtime.strategy.management_policy,
                ),
            }
            intent_created_event_payload = {
                "trade_signal_id": trade_signal_id,
                "trade_decision_id": decision["trade_decision_id"],
                "slot_key": slot_key,
                "execution_runtime": runtime.strategy.execution.runtime,
                "executor_profile_id": open_executor_profile.get("executor_profile_id"),
                "submit_ttl_minutes": open_execution_policy.get("submit_ttl_minutes"),
                "leg_count": len(signal_legs),
                "order_class": signal_order_payload.get("order_class") or ("mleg" if len(signal_legs) > 1 else "single"),
            }
        handoff = _persist_trade_admission_handoff(
            execution_store=execution_store,
            runtime=runtime,
            market_date=resolved_market_date,
            policy_ref=policy_ref,
            trade_signal_id=trade_signal_id,
            trade_decision_id=str(decision["trade_decision_id"]),
            execution_intent_id=execution_intent_id,
            slot_key=slot_key,
            admission_snapshot=selected_execution_admission,
            signal=signal,
            expires_at=intent_expires_at,
            execution_intent_payload=intent_payload,
            execution_intent_created_event_payload=intent_created_event_payload,
        )
        selected_admission = dict(handoff["admission"])
        admissions.append(selected_admission)
        if not admission_allows_attempt(selected_admission):
            selected_decision = decision
            selected_signal = signal
            continue
        selected_intent = handoff.get("execution_intent")
        if not isinstance(selected_intent, dict):
            raise RuntimeError("Approved admission handoff did not materialize a current execution intent.")
        selected_decision = decision
        selected_signal = signal
    if selected_intent is not None:
        lifecycle_start_request = request_execution_lifecycle_start(
            job_store=job_store,
            requested_by={
                "reason": "entry_intent_created",
                "execution_intent_id": str(selected_intent["execution_intent_id"]),
                "trading_strategy_id": runtime.trading_strategy_id,
            },
        )
        if lifecycle_start_request is not None:
            lifecycle_start_job_run_id = (
                None if lifecycle_start_request.get("job_run_id") in (None, "") else str(lifecycle_start_request["job_run_id"])
            )
            execution_store.append_execution_intent_event(
                execution_intent_id=str(selected_intent["execution_intent_id"]),
                event_type=(
                    "lifecycle_start_requested" if str(lifecycle_start_request.get("status") or "") == "started" else "lifecycle_start_failed"
                ),
                event_at=_utc_now(),
                payload={
                    "job_run_id": lifecycle_start_job_run_id,
                    "job_key": lifecycle_start_request.get("job_key"),
                    "status": lifecycle_start_request.get("status"),
                    "error": lifecycle_start_request.get("error"),
                },
            )

    runtime_alert: dict[str, Any] | None = None
    if selected_intent is not None and selected_decision is not None and selected_signal is not None:
        try:
            runtime_alert = plan_runtime_entry_selected_alert(
                alert_store=getattr(storage, "alerts", None),
                trading_strategy_id=runtime.trading_strategy_id,
                market_date=resolved_market_date,
                run_key=run_key,
                trade_signal=selected_signal,
                decision=selected_decision,
                execution_intent=selected_intent,
                execution_mode=runtime.strategy.execution.mode,
                approval_mode=runtime.strategy.execution.approval,
                planner_job_run_id=planner_job_run_id,
                lifecycle_start_job_run_id=lifecycle_start_job_run_id,
            )
        except Exception as exc:
            runtime_alert = {"status": "failed", "error": str(exc)}

    return {
        "status": "ok",
        "trading_strategy_id": runtime.trading_strategy_id,
        "market_date": resolved_market_date,
        "run_key": run_key,
        "entry_run_mode": "observation" if observation_only else "natural",
        "validation_provenance": provenance,
        "observation_only": observation_only,
        "signal_count": len(signals),
        "decision_count": len(decisions),
        "admission_count": len(admissions),
        "trade_decision_ids": [str(decision["trade_decision_id"]) for decision in decisions if decision.get("trade_decision_id") not in (None, "")],
        "selected_decision_ids": [
            str(decision["trade_decision_id"])
            for decision in decisions
            if decision.get("trade_decision_id") not in (None, "") and str(decision.get("decision_state") or "") == "selected"
        ],
        "selected_trade_signal_id": None if selected is None else str(selected.get("trade_signal_id")),
        "execution_intent_id": None if selected_intent is None else str(selected_intent.get("execution_intent_id")),
        "execution_admission": selected_execution_admission,
        "admission_decision_id": None if not admissions else admissions[-1].get("admission_decision_id"),
        "lifecycle_start_job_run_id": lifecycle_start_job_run_id,
        "runtime_alert": runtime_alert,
        "candidate_generation": candidate_generation,
    }


__all__ = ["_run_trading_strategy_entry"]

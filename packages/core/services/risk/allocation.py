from __future__ import annotations

from collections.abc import Mapping
from typing import Any


from core.money import money_float, money_scaled_float, money_sum_float
from core.services.risk.buying_power import (
    estimate_buying_power_requirement,
)
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_float,
    coerce_int,
    coerce_utc_datetime,
    safe_component,
    unique_text_list,
    utc_now_iso,
)

from core.services.risk.candidates import (
    _candidate_max_loss,
    _candidate_root_symbol,
    _candidate_strategy_family,
    _portfolio_correlation_group,
)
from core.services.risk.policy import (
    ACTIVE_PORTFOLIO_INTENT_STATES,
    ALLOCATION_DECISION_LIMIT,
    ALLOCATION_PLAN_BOUNDARY,
    MARKET_CONTEXT_FILTER_ID,
)

from core.services.risk.exposures import (
    _daily_entry_exposures,
    _open_portfolio_exposures,
    _portfolio_schema_ready,
    live_broker_buying_power_snapshot,
)

def _portfolio_policy_int(policy: Mapping[str, Any], key: str) -> int | None:
    value = coerce_int(policy.get(key))
    if value is None or value <= 0:
        return None
    return int(value)


def _market_context_fit_from_payload(*payloads: Mapping[str, Any]) -> dict[str, Any]:
    for payload in payloads:
        evidence = as_mapping(payload.get("evidence"))
        waterfall = as_mapping(evidence.get("quality_waterfall"))
        for result in list(waterfall.get("results") or []):
            if not isinstance(result, Mapping):
                continue
            if as_text(result.get("filter_id")) != MARKET_CONTEXT_FILTER_ID:
                continue
            metrics = as_mapping(result.get("metrics"))
            thresholds = as_mapping(result.get("thresholds"))
            return {
                "filter_id": MARKET_CONTEXT_FILTER_ID,
                "status": as_text(result.get("status")) or "unknown",
                "reason_codes": unique_text_list(result.get("reason_codes"), accept_scalar=True),
                "market_context_snapshot_id": as_text(metrics.get("market_context_snapshot_id")),
                "scope": as_text(metrics.get("market_context_scope")),
                "observed_at": as_text(metrics.get("market_context_observed_at")),
                "expires_at": as_text(metrics.get("market_context_expires_at")),
                "regime_label": as_text(metrics.get("market_context_regime_label") or metrics.get("regime_label")),
                "risk_posture": as_text(metrics.get("market_context_risk_posture") or metrics.get("risk_posture")),
                "trend_strength": as_text(metrics.get("market_context_trend_strength") or metrics.get("trend_strength")),
                "volatility_state": as_text(metrics.get("market_context_volatility_state") or metrics.get("volatility_state")),
                "confidence": coerce_float(metrics.get("market_context_confidence") or metrics.get("confidence")),
                "freshness": as_text(metrics.get("freshness")),
                "data_quality": as_text(metrics.get("data_quality") or metrics.get("data_quality_state")),
                "supportive_benchmark_count": coerce_int(metrics.get("supportive_benchmark_count")),
                "blocking_benchmark_count": coerce_int(metrics.get("blocking_benchmark_count")),
                "metrics": dict(metrics),
                "thresholds": dict(thresholds),
            }
    return {}


def _market_context_status_rank(context: Mapping[str, Any]) -> int:
    status = as_text(context.get("status")) or "unknown"
    return {
        "pass": 0,
        "watch": 1,
        "unknown": 2,
        "block": 3,
    }.get(status, 2)


def _allocation_market_context_block(row: Mapping[str, Any], *, evaluated_at: str) -> tuple[str, str, str, dict[str, Any]] | None:
    context = as_mapping(row.get("market_context"))
    if not context:
        return None
    metrics = {
        "market_context_snapshot_id": context.get("market_context_snapshot_id"),
        "market_context_status": context.get("status"),
        "market_context_regime_label": context.get("regime_label"),
        "market_context_risk_posture": context.get("risk_posture"),
        "market_context_confidence": context.get("confidence"),
        "market_context_reason_codes": list(context.get("reason_codes") or []),
    }
    expires_at = coerce_utc_datetime(context.get("expires_at"))
    evaluated_dt = coerce_utc_datetime(evaluated_at)
    if expires_at is not None and evaluated_dt is not None and expires_at <= evaluated_dt:
        return (
            "blocked",
            "allocation_market_context_expired",
            "AllocationPlan expired this selected decision because its market context snapshot is stale.",
            metrics,
        )
    status = as_text(context.get("status")) or "unknown"
    if status == "block":
        return (
            "blocked",
            "allocation_market_context_blocked",
            "AllocationPlan rejected this selected decision because shared market context does not fit the strategy policy.",
            metrics,
        )
    return None


def _allocation_market_context_summary(ranked_decisions: list[dict[str, Any]]) -> dict[str, Any]:
    snapshot_ids: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    for row in ranked_decisions:
        context = as_mapping(row.get("market_context"))
        if not context:
            continue
        snapshot_id = as_text(context.get("market_context_snapshot_id"))
        if snapshot_id is not None:
            snapshot_ids[snapshot_id] = snapshot_ids.get(snapshot_id, 0) + 1
        status = as_text(context.get("status")) or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        for reason in list(context.get("reason_codes") or []):
            reason_code = as_text(reason)
            if reason_code is not None:
                reason_counts[reason_code] = reason_counts.get(reason_code, 0) + 1
    return {
        "snapshot_ids": dict(sorted(snapshot_ids.items())),
        "regime_fit_status_counts": dict(sorted(status_counts.items())),
        "top_regime_fit_reasons": dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:10]),
    }


def _allocation_rank_key(row: Mapping[str, Any]) -> tuple[int, float, float, int, float, str]:
    score = coerce_float(row.get("score"))
    rank = coerce_int(row.get("rank"))
    decided_at = coerce_utc_datetime(row.get("decided_at"))
    decided_timestamp = 0.0 if decided_at is None else decided_at.timestamp()
    context = as_mapping(row.get("market_context"))
    context_confidence = coerce_float(context.get("confidence"))
    return (
        _market_context_status_rank(context),
        -(context_confidence if context_confidence is not None else 0.0),
        -(score if score is not None else -1_000_000.0),
        rank if rank is not None and rank > 0 else 1_000_000,
        -decided_timestamp,
        as_text(row.get("trade_decision_id")) or "",
    )


def _allocation_decision_candidate(
    *,
    decision: Mapping[str, Any],
    signal: Mapping[str, Any],
    default_strategy_family: Any,
    requested_quantity: int | float,
    limit_price: float | None,
) -> dict[str, Any]:
    execution_shape = (
        dict(decision.get("selected_execution_shape"))
        if isinstance(decision.get("selected_execution_shape"), Mapping)
        else dict(signal.get("execution_shape")) if isinstance(signal.get("execution_shape"), Mapping) else {}
    )
    order_payload = (
        dict(signal.get("order_payload"))
        if isinstance(signal.get("order_payload"), Mapping)
        else dict(execution_shape.get("order_payload")) if isinstance(execution_shape.get("order_payload"), Mapping) else {}
    )
    economics = dict(signal.get("economics")) if isinstance(signal.get("economics"), Mapping) else {}
    strategy_family = _candidate_strategy_family(
        {
            **dict(signal),
            "execution_shape": execution_shape,
            "order_payload": order_payload,
            "economics": economics,
        },
        strategy_family=decision.get("trade_structure") or signal.get("trade_structure") or default_strategy_family,
    )
    quantity = (
        coerce_float(requested_quantity)
        or coerce_float(decision.get("selected_quantity"))
        or coerce_float(order_payload.get("qty") or order_payload.get("quantity") or execution_shape.get("quantity"))
        or 1.0
    )
    candidate = {
        **dict(signal),
        **economics,
        "strategy": strategy_family,
        "strategy_family": strategy_family,
        "trade_structure": decision.get("trade_structure") or signal.get("trade_structure") or strategy_family,
        "execution_shape": execution_shape,
        "order_payload": order_payload,
        "economics": economics,
    }
    candidate_symbol = _candidate_root_symbol(candidate)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        quantity,
        limit_price=limit_price,
    )
    unit_buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    candidate_max_loss = _candidate_max_loss(candidate, quantity)
    if candidate_max_loss is None:
        candidate_max_loss = required_buying_power
    per_contract_max_loss = _candidate_max_loss(candidate, 1.0)
    market_context = _market_context_fit_from_payload(decision, signal)
    return {
        "trade_decision_id": as_text(decision.get("trade_decision_id")),
        "trade_signal_id": as_text(decision.get("trade_signal_id") or signal.get("trade_signal_id")),
        "trading_strategy_id": as_text(decision.get("trading_strategy_id") or signal.get("trading_strategy_id")),
        "strategy_family": strategy_family,
        "trade_structure": decision.get("trade_structure") or signal.get("trade_structure") or strategy_family,
        "underlying_symbol": candidate_symbol,
        "correlation_group": _portfolio_correlation_group(candidate_symbol),
        "score": coerce_float(decision.get("score") or signal.get("score")),
        "rank": coerce_int(decision.get("rank") or signal.get("rank")),
        "decided_at": as_text(decision.get("decided_at")),
        "requested_quantity": quantity,
        "candidate_max_loss": candidate_max_loss,
        "per_contract_max_loss": per_contract_max_loss,
        "required_buying_power": required_buying_power,
        "per_contract_required_buying_power": coerce_float(unit_buying_power_requirement.get("required_buying_power")),
        "buying_power_basis": as_text(buying_power_requirement.get("basis")),
        "market_context": market_context,
        "candidate": candidate,
    }


def _allocation_exposure(row: Mapping[str, Any], *, session_date: str) -> dict[str, Any]:
    return {
        "source_type": ALLOCATION_PLAN_BOUNDARY,
        "source_id": as_text(row.get("trade_decision_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": as_text(row.get("strategy_family")),
        "underlying_symbol": as_text(row.get("underlying_symbol")),
        "correlation_group": as_text(row.get("correlation_group")),
        "market_date": session_date,
        "max_loss": coerce_float(row.get("candidate_max_loss")),
        "required_buying_power": coerce_float(row.get("required_buying_power")),
    }


def _allocation_item_result(
    row: Mapping[str, Any],
    *,
    allocation_rank: int,
    status: str,
    reason: str,
    message: str,
    policy: Mapping[str, Any],
    metrics: Mapping[str, Any],
    active_intent_id: str | None = None,
) -> dict[str, Any]:
    return {
        "trade_decision_id": row.get("trade_decision_id"),
        "trade_signal_id": row.get("trade_signal_id"),
        "trading_strategy_id": row.get("trading_strategy_id"),
        "strategy_family": row.get("strategy_family"),
        "underlying_symbol": row.get("underlying_symbol"),
        "correlation_group": row.get("correlation_group"),
        "score": row.get("score"),
        "rank": row.get("rank"),
        "allocation_rank": allocation_rank,
        "status": status,
        "reason": reason,
        "message": message,
        "requested_quantity": row.get("requested_quantity"),
        "admissible_quantity": row.get("requested_quantity") if status in {"allocated", "allocated_trimmed", "already_active"} else 0,
        "candidate_max_loss": row.get("candidate_max_loss"),
        "required_buying_power": row.get("required_buying_power"),
        "buying_power_basis": row.get("buying_power_basis"),
        "market_context": dict(as_mapping(row.get("market_context"))),
        "active_intent_id": active_intent_id,
        "policy": dict(policy),
        "metrics": dict(metrics),
    }


def _allocation_policy_block(
    row: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
    exposures: list[dict[str, Any]],
    daily_entries: list[dict[str, Any]],
) -> tuple[str, str, dict[str, Any]] | None:
    strategy_id = as_text(row.get("trading_strategy_id"))
    family = as_text(row.get("strategy_family"))
    symbol = as_text(row.get("underlying_symbol"))
    correlation_group = as_text(row.get("correlation_group"))
    candidate_max_loss = coerce_float(row.get("candidate_max_loss"))
    same_strategy = [item for item in exposures if as_text(item.get("trading_strategy_id")) == strategy_id]
    same_family = [item for item in exposures if as_text(item.get("strategy_family")) == family]
    same_symbol_family = [
        item for item in exposures if as_text(item.get("underlying_symbol")) == symbol and as_text(item.get("strategy_family")) == family
    ]
    same_correlation_group = [
        item for item in exposures if correlation_group is not None and as_text(item.get("correlation_group")) == correlation_group
    ]
    same_strategy_daily_entries = [item for item in daily_entries if as_text(item.get("trading_strategy_id")) == strategy_id]
    strategy_max_loss_before = money_sum_float(coerce_float(item.get("max_loss")) for item in same_strategy)
    strategy_max_loss_after = None if candidate_max_loss is None else money_sum_float([strategy_max_loss_before, candidate_max_loss])
    metrics = {
        "active_exposure_count": len(exposures),
        "same_strategy_count": len(same_strategy),
        "same_family_count": len(same_family),
        "same_symbol_family_count": len(same_symbol_family),
        "same_correlation_group_count": len(same_correlation_group),
        "daily_new_entry_count": len(same_strategy_daily_entries),
        "candidate_max_loss": candidate_max_loss,
        "strategy_max_loss_before": strategy_max_loss_before,
        "strategy_max_loss_after": strategy_max_loss_after,
    }

    max_symbol_family = _portfolio_policy_int(policy, "max_symbol_family_open_positions")
    if max_symbol_family is not None and len(same_symbol_family) >= max_symbol_family:
        return "allocation_duplicate_symbol_family_exposure", "Allocation would duplicate active symbol/family exposure.", metrics

    max_strategy = _portfolio_policy_int(policy, "max_strategy_open_positions")
    if max_strategy is not None and len(same_strategy) >= max_strategy:
        return "allocation_strategy_cap_reached", "Allocation would exceed the strategy active exposure cap.", metrics

    max_family = _portfolio_policy_int(policy, "max_family_open_positions")
    if max_family is not None and len(same_family) >= max_family:
        return "allocation_family_cap_reached", "Allocation would exceed the family active exposure cap.", metrics

    max_daily_entries = _portfolio_policy_int(policy, "max_daily_new_entries")
    if max_daily_entries is not None and len(same_strategy_daily_entries) >= max_daily_entries:
        return "allocation_daily_entry_cap_reached", "Allocation would exceed the strategy daily new-entry cap.", metrics

    max_total_strategy_risk = coerce_float(policy.get("max_total_strategy_risk"))
    if max_total_strategy_risk is not None and strategy_max_loss_after is not None and strategy_max_loss_after > max_total_strategy_risk:
        return "allocation_strategy_risk_budget_exceeded", "Allocation would exceed the strategy max-loss budget.", metrics

    max_correlated = _portfolio_policy_int(policy, "max_correlated_group_open_positions")
    if max_correlated is not None and len(same_correlation_group) >= max_correlated:
        return "allocation_correlated_exposure_limit_reached", "Allocation would exceed the correlated exposure cap.", metrics

    return None


def _allocation_plan_admission_evidence(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "allocation_plan_id": plan.get("allocation_plan_id"),
        "status": plan.get("status"),
        "reason": plan.get("reason"),
        "message": plan.get("message"),
        "summary": dict(plan.get("summary")) if isinstance(plan.get("summary"), Mapping) else {},
        "market_context": dict(plan.get("market_context")) if isinstance(plan.get("market_context"), Mapping) else {},
        "capital": dict(plan.get("capital")) if isinstance(plan.get("capital"), Mapping) else {},
        "schedule_constraints": dict(plan.get("schedule_constraints")) if isinstance(plan.get("schedule_constraints"), Mapping) else {},
        "current_decision": dict(plan.get("current_decision")) if isinstance(plan.get("current_decision"), Mapping) else {},
        "ranked_decisions": [dict(item) for item in list(plan.get("ranked_decisions") or [])[:10] if isinstance(item, Mapping)],
    }


def _allocation_unavailable_plan(
    *,
    selected_decision: Mapping[str, Any],
    selected_signal: Mapping[str, Any],
    session_date: str,
    active_strategy_ids: tuple[str, ...],
    reason: str,
    message: str,
    status: str = "unknown",
) -> dict[str, Any]:
    trade_decision_id = as_text(selected_decision.get("trade_decision_id"))
    evaluated_at = utc_now_iso()
    current_decision = {
        "trade_decision_id": trade_decision_id,
        "trade_signal_id": as_text(selected_decision.get("trade_signal_id") or selected_signal.get("trade_signal_id")),
        "trading_strategy_id": as_text(selected_decision.get("trading_strategy_id") or selected_signal.get("trading_strategy_id")),
        "status": status,
        "reason": reason,
        "message": message,
        "allocation_rank": None,
        "admissible_quantity": 0,
        "market_context": _market_context_fit_from_payload(selected_decision, selected_signal),
    }
    market_context_summary = _allocation_market_context_summary([current_decision])
    return {
        "allocation_plan_id": f"allocation_plan:{safe_component(session_date)}:{safe_component(trade_decision_id)}",
        "status": status,
        "reason": reason,
        "message": message,
        "admission_boundary": ALLOCATION_PLAN_BOUNDARY,
        "current_decision": current_decision,
        "ranked_decisions": [current_decision],
        "summary": {
            "active_strategy_count": len(active_strategy_ids),
            "selected_decision_count": 1 if trade_decision_id else 0,
            "allocated_count": 0,
            "blocked_count": 0,
            "unknown_count": 1,
            "already_active_count": 0,
            "market_context_snapshot_count": len(market_context_summary["snapshot_ids"]),
        },
        "market_context": market_context_summary,
        "capital": {},
        "schedule_constraints": {
            "mode": "observed_selected_decisions",
            "active_strategy_ids": list(active_strategy_ids),
            "selected_strategy_ids": [],
            "missing_selected_strategy_ids": list(active_strategy_ids),
        },
        "evaluated_at": evaluated_at,
    }


def build_allocation_plan_snapshot(
    *,
    engine_facts: Any,
    execution_store: Any,
    selected_decision: Mapping[str, Any],
    selected_signal: Mapping[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    active_strategy_ids: tuple[str, ...],
    portfolio_policies: Mapping[str, Mapping[str, Any]] | None,
    quantity: int | float = 1,
    limit_price: float | None = None,
) -> dict[str, Any]:
    trade_decision_id = as_text(selected_decision.get("trade_decision_id"))
    evaluated_at = utc_now_iso()
    allocation_plan_id = f"allocation_plan:{safe_component(session_date)}:{safe_component(trade_decision_id)}"
    if engine_facts is None or not engine_facts.schema_ready():
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_engine_fact_schema_unavailable",
            message="AllocationPlan could not read selected trade decisions.",
        )
    if not _portfolio_schema_ready(execution_store):
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_portfolio_schema_unavailable",
            message="AllocationPlan could not read portfolio exposure schemas.",
        )

    try:
        active_exposures = _open_portfolio_exposures(execution_store)
        daily_entries = _daily_entry_exposures(
            execution_store,
            session_date=session_date,
            active_exposures=active_exposures,
        )
    except Exception as exc:
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_portfolio_exposure_unavailable",
            message=str(exc),
        )

    selected_rows = engine_facts.list_trade_decisions_with_signals(
        decision_states=["selected"],
        trading_strategy_ids=list(active_strategy_ids),
        routine="entry",
        session_date=session_date,
        as_of=evaluated_at,
        limit=ALLOCATION_DECISION_LIMIT,
    )
    rows_by_decision_id: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in selected_rows:
        decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), Mapping) else {}
        signal = row.get("trade_signal") if isinstance(row.get("trade_signal"), Mapping) else {}
        decision_id = as_text(decision.get("trade_decision_id"))
        if decision_id is not None:
            rows_by_decision_id[decision_id] = {"trade_decision": dict(decision), "trade_signal": dict(signal)}
    if trade_decision_id is not None and trade_decision_id not in rows_by_decision_id:
        rows_by_decision_id[trade_decision_id] = {
            "trade_decision": dict(selected_decision),
            "trade_signal": dict(selected_signal),
        }

    contenders = [
        _allocation_decision_candidate(
            decision=row["trade_decision"],
            signal=row["trade_signal"],
            default_strategy_family=strategy_family,
            requested_quantity=quantity,
            limit_price=limit_price,
        )
        for row in rows_by_decision_id.values()
    ]
    contenders = [row for row in contenders if row.get("trade_decision_id") is not None]
    contenders.sort(key=_allocation_rank_key)

    broker_buying_power = live_broker_buying_power_snapshot(execution_store)
    broker_status = as_text(broker_buying_power.get("status"))
    remaining_buying_power = coerce_float(broker_buying_power.get("remaining_buying_power"))
    planned_exposures: list[dict[str, Any]] = []
    planned_daily_entries: list[dict[str, Any]] = []
    planned_required_buying_power = 0.0
    ranked_decisions: list[dict[str, Any]] = []
    policies = dict(portfolio_policies or {})

    for allocation_rank, contender in enumerate(contenders, start=1):
        contender_decision_id = as_text(contender.get("trade_decision_id"))
        policy = dict(policies.get(as_text(contender.get("trading_strategy_id")) or "") or {})
        active_intents = execution_store.list_execution_intents(
            trade_decision_id=contender_decision_id,
            states=list(ACTIVE_PORTFOLIO_INTENT_STATES),
            limit=1,
        )
        if active_intents:
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="already_active",
                    reason="allocation_decision_already_active",
                    message="Selected decision already has an active execution intent.",
                    policy=policy,
                    metrics={"active_exposure_count": len(active_exposures)},
                    active_intent_id=as_text(active_intents[0].get("execution_intent_id")),
                )
            )
            continue

        context_block = _allocation_market_context_block(contender, evaluated_at=evaluated_at)
        if context_block is not None:
            context_status, context_reason, context_message, context_metrics = context_block
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status=context_status,
                    reason=context_reason,
                    message=context_message,
                    policy=policy,
                    metrics=context_metrics,
                )
            )
            continue

        if contender.get("underlying_symbol") is None or contender.get("strategy_family") is None:
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="unknown",
                    reason="allocation_candidate_identity_unavailable",
                    message="AllocationPlan could not resolve the candidate symbol and strategy family.",
                    policy=policy,
                    metrics={},
                )
            )
            continue

        exposure_view = [*active_exposures, *planned_exposures]
        daily_view = [*daily_entries, *planned_daily_entries]
        policy_block = _allocation_policy_block(
            contender,
            policy=policy,
            exposures=exposure_view,
            daily_entries=daily_view,
        )
        if policy_block is not None:
            reason, message, metrics = policy_block
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="blocked",
                    reason=reason,
                    message=message,
                    policy=policy,
                    metrics=metrics,
                )
            )
            continue

        required_buying_power = coerce_float(contender.get("required_buying_power"))
        if broker_status != "ok":
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="unknown",
                    reason="allocation_buying_power_unavailable",
                    message=as_text(broker_buying_power.get("error_text")) or "Broker buying power is unavailable for allocation.",
                    policy=policy,
                    metrics={"broker_buying_power_status": broker_status},
                )
            )
            continue
        if required_buying_power is None:
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="unknown",
                    reason="allocation_buying_power_requirement_unavailable",
                    message="AllocationPlan could not estimate buying power for this structure.",
                    policy=policy,
                    metrics={},
                )
            )
            continue
        if remaining_buying_power is not None and planned_required_buying_power + required_buying_power > remaining_buying_power:
            available_for_decision = max(remaining_buying_power - planned_required_buying_power, 0.0)
            per_contract_required_buying_power = coerce_float(contender.get("per_contract_required_buying_power"))
            trimmed_quantity = (
                0
                if per_contract_required_buying_power is None or per_contract_required_buying_power <= 0
                else int(available_for_decision // per_contract_required_buying_power)
            )
            requested_quantity = coerce_float(contender.get("requested_quantity")) or 1.0
            if 0 < trimmed_quantity < requested_quantity:
                trimmed = {
                    **contender,
                    "requested_quantity": trimmed_quantity,
                    "required_buying_power": money_scaled_float(per_contract_required_buying_power, trimmed_quantity),
                    "candidate_max_loss": money_scaled_float(
                        coerce_float(contender.get("per_contract_max_loss")),
                        trimmed_quantity,
                    ),
                }
                allocated = _allocation_item_result(
                    trimmed,
                    allocation_rank=allocation_rank,
                    status="allocated_trimmed",
                    reason="allocation_quantity_trimmed",
                    message="AllocationPlan trimmed quantity to fit remaining broker buying power.",
                    policy=policy,
                    metrics={
                        "remaining_buying_power": remaining_buying_power,
                        "planned_required_buying_power_before": money_float(planned_required_buying_power),
                        "requested_quantity": requested_quantity,
                        "admissible_quantity": trimmed_quantity,
                        "per_contract_required_buying_power": per_contract_required_buying_power,
                    },
                )
                ranked_decisions.append(allocated)
                planned_exposure = _allocation_exposure(trimmed, session_date=session_date)
                planned_exposures.append(planned_exposure)
                planned_daily_entries.append(planned_exposure)
                planned_required_buying_power += coerce_float(trimmed.get("required_buying_power")) or 0.0
                continue
            ranked_decisions.append(
                _allocation_item_result(
                    contender,
                    allocation_rank=allocation_rank,
                    status="blocked",
                    reason="allocation_buying_power_limit_reached",
                    message="Allocation would exceed remaining broker buying power after higher-ranked selections.",
                    policy=policy,
                    metrics={
                        "remaining_buying_power": remaining_buying_power,
                        "planned_required_buying_power_before": money_float(planned_required_buying_power),
                        "required_buying_power": required_buying_power,
                    },
                )
            )
            continue

        allocated = _allocation_item_result(
            contender,
            allocation_rank=allocation_rank,
            status="allocated",
            reason="allocation_selected",
            message="AllocationPlan selected this decision for portfolio admission.",
            policy=policy,
            metrics={
                "active_exposure_count": len(active_exposures),
                "planned_required_buying_power_before": money_float(planned_required_buying_power),
                "required_buying_power": required_buying_power,
            },
        )
        ranked_decisions.append(allocated)
        planned_exposure = _allocation_exposure(contender, session_date=session_date)
        planned_exposures.append(planned_exposure)
        planned_daily_entries.append(planned_exposure)
        planned_required_buying_power += required_buying_power

    current_decision = next(
        (row for row in ranked_decisions if as_text(row.get("trade_decision_id")) == trade_decision_id),
        None,
    )
    if current_decision is None:
        return _allocation_unavailable_plan(
            selected_decision=selected_decision,
            selected_signal=selected_signal,
            session_date=session_date,
            active_strategy_ids=active_strategy_ids,
            reason="allocation_current_decision_missing",
            message="AllocationPlan could not find the current selected decision in the selected-decision universe.",
        )

    selected_strategy_ids = sorted(
        {strategy_id for strategy_id in (as_text(row.get("trading_strategy_id")) for row in ranked_decisions) if strategy_id is not None}
    )
    status = as_text(current_decision.get("status")) or "unknown"
    plan_status = "allocated" if status in {"allocated", "allocated_trimmed", "already_active"} else status
    reason = as_text(current_decision.get("reason")) or "allocation_unknown"
    message = as_text(current_decision.get("message")) or "AllocationPlan did not produce a message."
    market_context_summary = _allocation_market_context_summary(ranked_decisions)
    return {
        "allocation_plan_id": allocation_plan_id,
        "status": plan_status,
        "reason": reason,
        "message": message,
        "admission_boundary": ALLOCATION_PLAN_BOUNDARY,
        "current_decision": current_decision,
        "ranked_decisions": ranked_decisions,
        "summary": {
            "active_strategy_count": len(active_strategy_ids),
            "selected_strategy_count": len(selected_strategy_ids),
            "selected_decision_count": len(ranked_decisions),
            "allocated_count": sum(1 for row in ranked_decisions if row.get("status") == "allocated"),
            "blocked_count": sum(1 for row in ranked_decisions if row.get("status") == "blocked"),
            "unknown_count": sum(1 for row in ranked_decisions if row.get("status") == "unknown"),
            "already_active_count": sum(1 for row in ranked_decisions if row.get("status") == "already_active"),
            "planned_required_buying_power": money_float(planned_required_buying_power),
            "active_exposure_count": len(active_exposures),
            "market_context_snapshot_count": len(market_context_summary["snapshot_ids"]),
            "market_context_blocked_count": sum(
                1
                for row in ranked_decisions
                if as_text(row.get("reason")) in {"allocation_market_context_blocked", "allocation_market_context_expired"}
            ),
        },
        "market_context": market_context_summary,
        "capital": {
            "broker_buying_power_status": broker_status,
            "available_buying_power": coerce_float(broker_buying_power.get("available_buying_power")),
            "reserved_buying_power": coerce_float(broker_buying_power.get("reserved_buying_power")),
            "remaining_buying_power": remaining_buying_power,
            "planned_required_buying_power": money_float(planned_required_buying_power),
            "buying_power_source_field": as_text(broker_buying_power.get("source_field")),
            "reservation_count": coerce_int(broker_buying_power.get("reservation_count")),
        },
        "schedule_constraints": {
            "mode": "observed_selected_decisions",
            "active_strategy_ids": list(active_strategy_ids),
            "selected_strategy_ids": selected_strategy_ids,
            "missing_selected_strategy_ids": [strategy_id for strategy_id in active_strategy_ids if strategy_id not in selected_strategy_ids],
            "decision_limit": ALLOCATION_DECISION_LIMIT,
        },
        "evaluated_at": evaluated_at,
    }

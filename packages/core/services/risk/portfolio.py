from __future__ import annotations

from collections.abc import Mapping
from typing import Any


from core.money import money_float, money_scaled_float, money_sum_float
from core.services.risk.buying_power import (
    estimate_buying_power_requirement,
    resolve_available_buying_power,
)
from core.services.alpaca import (
    create_alpaca_client_from_env,
)
from core.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUS_LIST,
    is_open_execution_attempt_status,
    resolve_execution_attempt_filled_quantity,
    resolve_execution_attempt_requested_quantity,
)
from core.services.positions import enrich_position_row
from core.services.runtime_identity import parse_live_run_scope_id
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
    _candidate_entry_notional,
    _candidate_max_loss,
    _candidate_root_symbol,
    _candidate_strategy_family,
    _date_text,
    _portfolio_correlation_group,
    _root_symbol,
)
from core.services.risk.policy import (
    ACCOUNT_CAPACITY_REQUEST_TIMEOUT_SECONDS,
    ACTIVE_PORTFOLIO_INTENT_STATES,
    ALLOCATION_DECISION_LIMIT,
    ALLOCATION_PLAN_BOUNDARY,
    MARKET_CONTEXT_FILTER_ID,
    OPEN_POSITION_STATUSES,
    PORTFOLIO_ADMISSION_BOUNDARY,
    TERMINAL_ENTRY_ATTEMPT_STATUSES,
)

def _open_positions(execution_store: Any, *, session_id: str) -> list[dict[str, Any]]:
    resolved = parse_live_run_scope_id(session_id)
    if resolved is None:
        return []
    return [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
            market_date=resolved["market_date"],
            statuses=OPEN_POSITION_STATUSES,
            limit=200,
        )
    ]


def _open_attempts(execution_store: Any, *, session_id: str) -> list[dict[str, Any]]:
    list_for_status = getattr(execution_store, "list_session_attempts_by_status", None)
    if callable(list_for_status):
        rows = list_for_status(
            session_id=session_id,
            statuses=list(OPEN_ATTEMPT_STATUS_LIST),
            trade_intent="open",
            limit=200,
        )
        return [dict(row) for row in rows]

    list_attempts = getattr(execution_store, "list_attempts", None)
    if not callable(list_attempts):
        return []
    rows = list_attempts(session_id=session_id, limit=200)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        if str(payload.get("trade_intent") or "").lower() != "open":
            continue
        if not is_open_execution_attempt_status(payload.get("status")):
            continue
        filtered.append(payload)
    return filtered


def _account_open_attempts(execution_store: Any) -> list[dict[str, Any]]:
    list_for_status = getattr(execution_store, "list_attempts_by_status", None)
    if not callable(list_for_status):
        return []
    rows = list_for_status(
        statuses=list(OPEN_ATTEMPT_STATUS_LIST),
        trade_intent="open",
        limit=200,
    )
    return [dict(row) for row in rows]


def _pending_open_attempt_quantity(attempt: Mapping[str, Any]) -> float:
    requested_quantity = resolve_execution_attempt_requested_quantity(attempt)
    if requested_quantity <= 0:
        return 0.0
    filled_quantity = min(
        resolve_execution_attempt_filled_quantity(attempt),
        requested_quantity,
    )
    return max(requested_quantity - filled_quantity, 0.0)


def _pending_open_attempt_exposures(
    attempts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    exposures: list[dict[str, Any]] = []
    for attempt in attempts:
        filled_quantity = resolve_execution_attempt_filled_quantity(attempt)
        pending_quantity = _pending_open_attempt_quantity(attempt)
        if pending_quantity <= 0:
            continue
        candidate = attempt.get("candidate")
        candidate_payload = dict(candidate) if isinstance(candidate, Mapping) else {}
        linked_position_id = as_text(attempt.get("position_id"))
        exposures.append(
            {
                "execution_attempt_id": as_text(attempt.get("execution_attempt_id")),
                "underlying_symbol": as_text(attempt.get("underlying_symbol")),
                "strategy": as_text(attempt.get("strategy")),
                "pending_quantity": pending_quantity,
                "limit_price": coerce_float(attempt.get("limit_price")),
                "candidate": candidate_payload,
                "pending_entry_notional": _candidate_entry_notional(
                    candidate_payload,
                    pending_quantity,
                    coerce_float(attempt.get("limit_price")),
                ),
                "pending_max_loss": _candidate_max_loss(
                    candidate_payload,
                    pending_quantity,
                ),
                # A partially filled attempt already consumes a slot through its
                # linked/open canonical position, so only count unfilled attempts
                # with no fills toward additional position capacity.
                "occupies_position_slot": (linked_position_id is None and filled_quantity <= 0),
            }
        )
    return exposures

def _exposure_max_loss_from_row(row: Mapping[str, Any]) -> float | None:
    for key in ("max_loss", "position_max_loss", "requested_notional"):
        value = coerce_float(row.get(key))
        if value is not None:
            return value
    for nested_key in ("economics", "strategy_metrics", "candidate", "payload"):
        nested = row.get(nested_key)
        if isinstance(nested, Mapping):
            value = coerce_float(nested.get("max_loss") or nested.get("position_max_loss"))
            if value is not None:
                return value
    quantity = (
        coerce_float(row.get("remaining_quantity") or row.get("opened_quantity") or row.get("quantity") or row.get("requested_quantity")) or 1.0
    )
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return _candidate_max_loss(dict(candidate), quantity)
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        return _candidate_max_loss(dict(payload), quantity)
    return None


def _portfolio_position_exposure(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _root_symbol(row.get("root_symbol") or row.get("underlying_symbol"))
    family = _candidate_strategy_family(
        row,
        strategy_family=row.get("strategy_family") or row.get("trade_structure"),
    )
    return {
        "source_type": "position",
        "source_id": as_text(row.get("position_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": family,
        "underlying_symbol": symbol,
        "correlation_group": _portfolio_correlation_group(symbol),
        "market_date": _date_text(row.get("market_date_opened") or row.get("created_at")),
        "max_loss": _exposure_max_loss_from_row(row),
        "contract_count": coerce_float(row.get("remaining_quantity")) or 0.0,
    }


def _portfolio_attempt_exposure(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    candidate_payload = dict(candidate) if isinstance(candidate, Mapping) else {}
    symbol = _root_symbol(row.get("root_symbol") or row.get("underlying_symbol") or candidate_payload.get("underlying_symbol"))
    family = _candidate_strategy_family(
        candidate_payload or row,
        strategy_family=row.get("strategy_family") or row.get("strategy") or row.get("trade_structure"),
    )
    return {
        "source_type": "attempt",
        "source_id": as_text(row.get("execution_attempt_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": family,
        "underlying_symbol": symbol,
        "correlation_group": _portfolio_correlation_group(symbol),
        "market_date": _date_text(row.get("market_date") or row.get("requested_at")),
        "max_loss": _exposure_max_loss_from_row(row),
        "contract_count": coerce_float(row.get("pending_quantity") or row.get("remaining_quantity") or row.get("quantity")) or 0.0,
    }


def _portfolio_intent_exposure(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        payload = row.get("payload_json") if isinstance(row.get("payload_json"), Mapping) else {}
    symbol = _root_symbol(payload.get("root_symbol") or payload.get("underlying_symbol"))
    family = _candidate_strategy_family(
        payload,
        strategy_family=row.get("trade_structure") or payload.get("strategy_family") or payload.get("trade_structure"),
    )
    execution_admission = payload.get("execution_admission") if isinstance(payload.get("execution_admission"), Mapping) else {}
    order_payload = as_mapping(payload.get("order_payload"))
    execution_shape = as_mapping(payload.get("execution_shape"))
    execution_order_payload = as_mapping(execution_shape.get("order_payload"))
    contract_count = (
        coerce_float(payload.get("quantity") or payload.get("qty"))
        or coerce_float(order_payload.get("quantity") or order_payload.get("qty"))
        or coerce_float(execution_order_payload.get("quantity") or execution_order_payload.get("qty"))
        or coerce_float(execution_admission.get("admissible_quantity"))
        or 1.0
    )
    return {
        "source_type": "intent",
        "source_id": as_text(row.get("execution_intent_id")),
        "trading_strategy_id": as_text(row.get("trading_strategy_id")),
        "strategy_family": family,
        "underlying_symbol": symbol,
        "correlation_group": _portfolio_correlation_group(symbol),
        "market_date": _date_text(row.get("created_at")),
        "max_loss": _exposure_max_loss_from_row(execution_admission or payload),
        "contract_count": contract_count,
    }


def _portfolio_schema_ready(execution_store: Any) -> bool:
    for method_name in ("portfolio_schema_ready", "positions_schema_ready", "intent_schema_ready"):
        method = getattr(execution_store, method_name, None)
        if callable(method) and not bool(method()):
            return False
    return True


def _open_portfolio_exposures(execution_store: Any) -> list[dict[str, Any]]:
    positions = [_portfolio_position_exposure(dict(row)) for row in execution_store.list_positions(statuses=OPEN_POSITION_STATUSES, limit=500)]
    attempts = []
    for row in execution_store.list_attempts_by_status(
        statuses=list(OPEN_ATTEMPT_STATUS_LIST),
        trade_intent="open",
        limit=500,
    ):
        payload = dict(row)
        if as_text(payload.get("position_id")) is not None:
            continue
        attempts.append(_portfolio_attempt_exposure(payload))

    intents = []
    for row in execution_store.list_execution_intents(
        states=list(ACTIVE_PORTFOLIO_INTENT_STATES),
        limit=500,
    ):
        payload = dict(row)
        if as_text(payload.get("execution_attempt_id")) is not None:
            continue
        intents.append(_portfolio_intent_exposure(payload))

    return [row for row in [*positions, *attempts, *intents] if row.get("underlying_symbol") is not None]


def _daily_entry_exposures(
    execution_store: Any,
    *,
    session_date: str,
    active_exposures: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}

    def remember(row: dict[str, Any]) -> None:
        source_type = as_text(row.get("source_type")) or "unknown"
        source_id = as_text(row.get("source_id")) or f"{source_type}:{len(rows)}"
        rows[(source_type, source_id)] = row

    for exposure in active_exposures:
        if _date_text(exposure.get("market_date")) == session_date:
            remember(dict(exposure))

    for row in execution_store.list_positions(market_date=session_date, limit=500):
        remember(_portfolio_position_exposure(dict(row)))

    list_attempts = getattr(execution_store, "list_attempts_for_market_date", None)
    if callable(list_attempts):
        for row in list_attempts(market_date=session_date, limit=500):
            payload = dict(row)
            if str(payload.get("trade_intent") or "").lower() != "open":
                continue
            if str(payload.get("status") or "").lower() in TERMINAL_ENTRY_ATTEMPT_STATUSES:
                continue
            remember(_portfolio_attempt_exposure(payload))

    return list(rows.values())


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

def _portfolio_block_payload(
    *,
    reason: str,
    message: str,
    policy: Mapping[str, Any],
    metrics: Mapping[str, Any],
    evidence: Mapping[str, Any],
    status: str = "blocked",
    evaluated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "message": message,
        "admission_boundary": PORTFOLIO_ADMISSION_BOUNDARY,
        "admissible_quantity": 0 if status == "blocked" else None,
        "reason_codes": [reason],
        "blockers": [reason],
        "policy": dict(policy),
        "metrics": dict(metrics),
        "evidence": dict(evidence),
        "evaluated_at": evaluated_at or utc_now_iso(),
    }


def build_portfolio_admission_snapshot(
    *,
    execution_store: Any,
    candidate: dict[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    policy: Mapping[str, Any] | None,
    quantity: int | float = 1,
    limit_price: float | None = None,
    allocation_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    evaluated_at = utc_now_iso()
    normalized_policy = dict(policy or {})
    allocation_evidence = _allocation_plan_admission_evidence(allocation_plan) if isinstance(allocation_plan, Mapping) else {}
    if not _portfolio_schema_ready(execution_store):
        return _portfolio_block_payload(
            status="unknown",
            reason="portfolio_schema_unavailable",
            message="Portfolio admission could not read the required lifecycle schemas.",
            policy=normalized_policy,
            metrics={},
            evidence={},
            evaluated_at=evaluated_at,
        )

    try:
        active_exposures = _open_portfolio_exposures(execution_store)
        daily_entries = _daily_entry_exposures(
            execution_store,
            session_date=session_date,
            active_exposures=active_exposures,
        )
    except Exception as exc:
        return _portfolio_block_payload(
            status="unknown",
            reason="portfolio_admission_unavailable",
            message=str(exc),
            policy=normalized_policy,
            metrics={},
            evidence={},
            evaluated_at=evaluated_at,
        )

    candidate_symbol = _candidate_root_symbol(candidate)
    candidate_family = _candidate_strategy_family(candidate, strategy_family=strategy_family)
    if candidate_symbol is None or candidate_family is None:
        return _portfolio_block_payload(
            status="unknown",
            reason="portfolio_candidate_identity_unavailable",
            message="Portfolio admission could not resolve the candidate symbol and strategy family.",
            policy=normalized_policy,
            metrics={},
            evidence={"candidate_symbol": candidate_symbol, "strategy_family": candidate_family},
            evaluated_at=evaluated_at,
        )

    resolved_quantity = coerce_float(quantity) or 1.0
    candidate_max_loss = _candidate_max_loss(candidate, resolved_quantity)
    if candidate_max_loss is None:
        requirement = estimate_buying_power_requirement(candidate, resolved_quantity, limit_price=limit_price)
        candidate_max_loss = coerce_float(requirement.get("required_buying_power"))

    candidate_correlation_group = _portfolio_correlation_group(candidate_symbol)
    same_strategy = [row for row in active_exposures if as_text(row.get("trading_strategy_id")) == trading_strategy_id]
    same_family = [row for row in active_exposures if row.get("strategy_family") == candidate_family]
    same_symbol_family = [
        row for row in active_exposures if row.get("underlying_symbol") == candidate_symbol and row.get("strategy_family") == candidate_family
    ]
    same_correlation_group = [
        row for row in active_exposures if candidate_correlation_group is not None and row.get("correlation_group") == candidate_correlation_group
    ]
    strategy_max_loss_before = money_sum_float(coerce_float(row.get("max_loss")) for row in same_strategy)
    strategy_max_loss_after = None if candidate_max_loss is None else money_sum_float([strategy_max_loss_before, candidate_max_loss])
    same_strategy_daily_entries = [row for row in daily_entries if as_text(row.get("trading_strategy_id")) == trading_strategy_id]

    metrics = {
        "active_exposure_count": len(active_exposures),
        "same_strategy_count": len(same_strategy),
        "same_family_count": len(same_family),
        "same_symbol_family_count": len(same_symbol_family),
        "same_correlation_group_count": len(same_correlation_group),
        "daily_new_entry_count": len(same_strategy_daily_entries),
        "candidate_max_loss": candidate_max_loss,
        "strategy_max_loss_before": strategy_max_loss_before,
        "strategy_max_loss_after": strategy_max_loss_after,
        "candidate_symbol": candidate_symbol,
        "candidate_strategy_family": candidate_family,
        "candidate_correlation_group": candidate_correlation_group,
    }
    evidence = {
        "candidate": {
            "underlying_symbol": candidate_symbol,
            "strategy_family": candidate_family,
            "trading_strategy_id": trading_strategy_id,
            "correlation_group": candidate_correlation_group,
        },
        "matching_symbol_family_exposures": same_symbol_family[:10],
        "matching_strategy_exposures": same_strategy[:10],
        "matching_family_exposures": same_family[:10],
        "matching_correlation_exposures": same_correlation_group[:10],
        "daily_entry_exposures": same_strategy_daily_entries[:10],
    }
    if allocation_evidence:
        allocation_decision = (
            dict(allocation_evidence.get("current_decision")) if isinstance(allocation_evidence.get("current_decision"), Mapping) else {}
        )
        allocation_status = as_text(allocation_decision.get("status") or allocation_evidence.get("status")) or "unknown"
        allocation_reason = as_text(allocation_decision.get("reason") or allocation_evidence.get("reason")) or "allocation_plan_not_selected"
        metrics.update(
            {
                "allocation_plan_status": allocation_status,
                "allocation_plan_reason": allocation_reason,
                "allocation_rank": allocation_decision.get("allocation_rank"),
                "allocation_selected_decision_count": (as_mapping(allocation_evidence.get("summary")).get("selected_decision_count")),
                "allocation_allocated_count": as_mapping(allocation_evidence.get("summary")).get("allocated_count"),
            }
        )
        evidence["allocation_plan"] = allocation_evidence
        evidence["allocation_decision"] = allocation_decision
        if allocation_status not in {"allocated", "allocated_trimmed", "already_active"}:
            return _portfolio_block_payload(
                status="unknown" if allocation_status == "unknown" else "blocked",
                reason=allocation_reason,
                message=as_text(allocation_decision.get("message") or allocation_evidence.get("message"))
                or "AllocationPlan did not allocate this selected decision.",
                policy=normalized_policy,
                metrics=metrics,
                evidence=evidence,
                evaluated_at=evaluated_at,
            )

    max_symbol_family = _portfolio_policy_int(normalized_policy, "max_symbol_family_open_positions")
    if max_symbol_family is not None and len(same_symbol_family) >= max_symbol_family:
        return _portfolio_block_payload(
            reason="duplicate_symbol_family_exposure",
            message="Portfolio already has active exposure for this symbol and strategy family.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_strategy = _portfolio_policy_int(normalized_policy, "max_strategy_open_positions")
    if max_strategy is not None and len(same_strategy) >= max_strategy:
        return _portfolio_block_payload(
            reason="portfolio_strategy_cap_reached",
            message="Strategy-level active exposure cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_family = _portfolio_policy_int(normalized_policy, "max_family_open_positions")
    if max_family is not None and len(same_family) >= max_family:
        return _portfolio_block_payload(
            reason="portfolio_family_cap_reached",
            message="Family-level active exposure cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_daily_entries = _portfolio_policy_int(normalized_policy, "max_daily_new_entries")
    if max_daily_entries is not None and len(same_strategy_daily_entries) >= max_daily_entries:
        return _portfolio_block_payload(
            reason="max_daily_new_entries_reached",
            message="Strategy daily new-entry cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_total_strategy_risk = coerce_float(normalized_policy.get("max_total_strategy_risk"))
    if max_total_strategy_risk is not None and strategy_max_loss_after is not None and strategy_max_loss_after > max_total_strategy_risk:
        return _portfolio_block_payload(
            reason="max_total_strategy_risk_exceeded",
            message="Strategy-level max-loss exposure budget would be exceeded.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    max_correlated = _portfolio_policy_int(normalized_policy, "max_correlated_group_open_positions")
    if max_correlated is not None and len(same_correlation_group) >= max_correlated:
        return _portfolio_block_payload(
            reason="correlated_exposure_limit_reached",
            message="Correlated exposure cap has been reached.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            evaluated_at=evaluated_at,
        )

    return {
        "status": "admissible",
        "reason": "portfolio_admissible",
        "message": "Portfolio can add this exposure under the current strategy policy.",
        "admission_boundary": PORTFOLIO_ADMISSION_BOUNDARY,
        "admissible_quantity": 1,
        "reason_codes": ["portfolio_admissible"],
        "blockers": [],
        "policy": normalized_policy,
        "metrics": metrics,
        "evidence": evidence,
        "allocation_plan": allocation_evidence,
        "evaluated_at": evaluated_at,
    }


def live_broker_buying_power_snapshot(execution_store: Any) -> dict[str, Any]:
    open_attempts = _account_open_attempts(execution_store)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    reserved_buying_power = 0.0
    reservation_count = 0
    unsupported_reservation_count = 0
    for attempt in pending_attempts:
        requirement = estimate_buying_power_requirement(
            dict(attempt.get("candidate") or {}),
            coerce_float(attempt.get("pending_quantity")) or 0.0,
            limit_price=coerce_float(attempt.get("limit_price")),
        )
        required_buying_power = coerce_float(requirement.get("required_buying_power"))
        if required_buying_power is None:
            unsupported_reservation_count += 1
            continue
        reservation_count += 1
        reserved_buying_power += required_buying_power

    try:
        account_payload = create_alpaca_client_from_env(
            request_timeout_seconds=ACCOUNT_CAPACITY_REQUEST_TIMEOUT_SECONDS,
        ).get_account()
    except Exception as exc:
        return {
            "status": "unavailable",
            "source_field": None,
            "available_buying_power": None,
            "reserved_buying_power": money_float(reserved_buying_power),
            "remaining_buying_power": None,
            "reservation_count": reservation_count,
            "unsupported_reservation_count": unsupported_reservation_count,
            "error_text": str(exc),
        }

    available_snapshot = resolve_available_buying_power(account_payload)
    available_buying_power = coerce_float(available_snapshot.get("available_buying_power"))
    if available_buying_power is None:
        return {
            "status": "unavailable",
            "source_field": as_text(available_snapshot.get("source_field")),
            "available_buying_power": None,
            "reserved_buying_power": money_float(reserved_buying_power),
            "remaining_buying_power": None,
            "reservation_count": reservation_count,
            "unsupported_reservation_count": unsupported_reservation_count,
            "error_text": "Broker account payload did not include usable buying power fields.",
        }

    return {
        "status": "ok",
        "source_field": as_text(available_snapshot.get("source_field")),
        "available_buying_power": money_float(available_buying_power),
        "reserved_buying_power": money_float(reserved_buying_power),
        "remaining_buying_power": money_float(max(available_buying_power - reserved_buying_power, 0.0)),
        "reservation_count": reservation_count,
        "unsupported_reservation_count": unsupported_reservation_count,
        "error_text": None,
    }

def _session_position_metrics(positions: list[dict[str, Any]]) -> dict[str, float]:
    return {
        "open_position_count": float(len(positions)),
        "open_contract_count": sum(coerce_float(position.get("remaining_quantity")) or 0.0 for position in positions),
        "entry_notional_total": sum(coerce_float(position.get("entry_notional")) or 0.0 for position in positions),
        "max_loss_total": sum(coerce_float(position.get("max_loss")) or 0.0 for position in positions),
    }


def _session_pending_open_attempt_metrics(
    pending_attempts: list[dict[str, Any]],
) -> dict[str, float]:
    return {
        "pending_open_attempt_count": float(len(pending_attempts)),
        "pending_open_position_slot_count": sum(1.0 for attempt in pending_attempts if bool(attempt.get("occupies_position_slot"))),
        "pending_open_contract_count": sum(coerce_float(attempt.get("pending_quantity")) or 0.0 for attempt in pending_attempts),
        "pending_entry_notional_total": sum(coerce_float(attempt.get("pending_entry_notional")) or 0.0 for attempt in pending_attempts),
        "pending_max_loss_total": sum(coerce_float(attempt.get("pending_max_loss")) or 0.0 for attempt in pending_attempts),
    }


def _session_open_metrics(
    positions: list[dict[str, Any]],
    pending_attempts: list[dict[str, Any]],
) -> dict[str, float]:
    position_metrics = _session_position_metrics(positions)
    pending_metrics = _session_pending_open_attempt_metrics(pending_attempts)
    return {
        **position_metrics,
        **pending_metrics,
        "active_open_position_count": (position_metrics["open_position_count"] + pending_metrics["pending_open_position_slot_count"]),
        "active_open_contract_count": (position_metrics["open_contract_count"] + pending_metrics["pending_open_contract_count"]),
        "active_entry_notional_total": (position_metrics["entry_notional_total"] + pending_metrics["pending_entry_notional_total"]),
        "active_max_loss_total": (position_metrics["max_loss_total"] + pending_metrics["pending_max_loss_total"]),
    }

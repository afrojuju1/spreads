from __future__ import annotations

from typing import Any

from core.services.risk.admission import (
    build_entry_capacity_admission_payload,
    build_execution_admission_snapshot,
)
from core.services.risk.portfolio import (
    build_allocation_plan_snapshot,
    build_portfolio_admission_snapshot,
)
from core.services.risk.protection import (
    build_protection_admission_snapshot,
)
from core.services.risk.sizing import (
    resolve_position_size_policy,
)
from core.services.trading_strategies import load_active_trading_strategies
from core.services.trading_strategy_runtime_models import EntryRuntime
from core.value_coercion import coerce_int, utc_now_iso


def _positive_int(value: Any, *, default: int) -> int:
    parsed = coerce_int(value)
    if parsed is None or parsed <= 0:
        return default
    return parsed


def portfolio_admission_policy_for_strategy(
    *,
    strategy: Any,
    trading_strategy_id: str,
    trade_structure: str,
    position_size_policy: dict[str, Any],
) -> dict[str, Any]:
    portfolio_limits = getattr(getattr(strategy, "risk_limits", None), "portfolio_admission", None)
    if portfolio_limits is not None and bool(getattr(portfolio_limits, "configured", False)):
        return portfolio_limits.as_policy(
            trading_strategy_id=trading_strategy_id,
            strategy_family=trade_structure,
        )

    is_long_call = trade_structure == "long_call"
    default_strategy_cap = 10 if is_long_call else 2
    default_family_cap = 10 if is_long_call else 2
    strategy_cap = _positive_int(strategy.max_open_positions, default=default_strategy_cap)
    daily_cap = _positive_int(
        strategy.max_new_entries_per_day or strategy.max_daily_actions,
        default=strategy_cap,
    )
    max_risk_per_trade = position_size_policy.get("max_risk_per_trade")
    max_total_strategy_risk = None
    if max_risk_per_trade is not None:
        max_total_strategy_risk = round(float(max_risk_per_trade) * strategy_cap, 2)
    return {
        "trading_strategy_id": trading_strategy_id,
        "strategy_family": trade_structure,
        "policy_source": "runtime_fallback",
        "max_strategy_open_positions": strategy_cap,
        "max_family_open_positions": max(default_family_cap, min(strategy_cap, default_family_cap)),
        "max_symbol_family_open_positions": 1,
        "max_daily_new_entries": daily_cap,
        "max_total_strategy_risk": max_total_strategy_risk,
        "max_correlated_group_open_positions": 6 if is_long_call else 3,
    }


def active_portfolio_admission_policies() -> dict[str, dict[str, Any]]:
    policies: dict[str, dict[str, Any]] = {}
    for strategy in load_active_trading_strategies().values():
        policies[strategy.trading_strategy_id] = portfolio_admission_policy_for_strategy(
            strategy=strategy,
            trading_strategy_id=strategy.trading_strategy_id,
            trade_structure=strategy.trade_structure,
            position_size_policy=resolve_position_size_policy(strategy.risk_defaults),
        )
    return policies


def build_selected_entry_admission_snapshot(
    *,
    engine_facts: Any,
    execution_store: Any,
    runtime: EntryRuntime,
    decision: dict[str, Any],
    signal: dict[str, Any],
    market_date: str,
    evaluate_execution_capacity: bool = True,
    evaluated_at: str | None = None,
) -> dict[str, Any]:
    position_size_policy = resolve_position_size_policy(getattr(runtime.build_settings, "risk_defaults", {}))
    signal_execution_shape = dict(signal.get("execution_shape") or {})
    signal_order_payload = dict(signal.get("order_payload") or signal_execution_shape.get("order_payload") or {})
    quantity = _positive_int(
        signal_order_payload.get("qty") or signal_order_payload.get("quantity") or signal_execution_shape.get("quantity"),
        default=1,
    )
    portfolio_policies = active_portfolio_admission_policies()
    current_policy = portfolio_policies.get(runtime.trading_strategy_id) or portfolio_admission_policy_for_strategy(
        strategy=runtime.strategy,
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        position_size_policy=position_size_policy,
    )
    allocation_plan = None
    if evaluate_execution_capacity:
        allocation_plan = build_allocation_plan_snapshot(
            engine_facts=engine_facts,
            execution_store=execution_store,
            selected_decision=decision,
            selected_signal=signal,
            trading_strategy_id=runtime.trading_strategy_id,
            strategy_family=runtime.trade_structure,
            session_date=market_date,
            active_strategy_ids=tuple(sorted({*portfolio_policies, runtime.trading_strategy_id})),
            portfolio_policies=portfolio_policies,
            quantity=quantity,
            limit_price=None,
        )
    protection_admission = build_protection_admission_snapshot(
        execution_store=execution_store,
        candidate=signal,
        trading_strategy_id=runtime.trading_strategy_id,
        strategy_family=runtime.trade_structure,
        session_date=market_date,
        policy=runtime.strategy.protection.model_dump(exclude_none=True, by_alias=True),
        quantity=quantity,
        limit_price=None,
        allocation_plan=allocation_plan,
    )
    protection_status = str(protection_admission.get("status") or "").lower()
    if protection_status not in {"admissible", "approved", "ok", "pass", "passed"}:
        return build_entry_capacity_admission_payload(
            status="not_evaluated",
            reason=None,
            message=None,
            evaluated_at=evaluated_at or utc_now_iso(),
            admissible_quantity=None,
            required_buying_power=None,
            available_buying_power=None,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
            protection_admission=protection_admission,
        )
    portfolio_admission = build_portfolio_admission_snapshot(
        execution_store=execution_store,
        candidate=signal,
        trading_strategy_id=runtime.trading_strategy_id,
        strategy_family=runtime.trade_structure,
        session_date=market_date,
        policy=current_policy,
        quantity=quantity,
        limit_price=None,
        allocation_plan=allocation_plan,
    )
    portfolio_status = str(portfolio_admission.get("status") or "").lower()
    if portfolio_status not in {"admissible", "approved", "ok", "pass", "passed"}:
        return build_entry_capacity_admission_payload(
            status="not_evaluated",
            reason=None,
            message=None,
            evaluated_at=evaluated_at or utc_now_iso(),
            admissible_quantity=None,
            required_buying_power=None,
            available_buying_power=None,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
            protection_admission=protection_admission,
            portfolio_admission=portfolio_admission,
        )
    if not evaluate_execution_capacity:
        return build_entry_capacity_admission_payload(
            status="not_evaluated",
            reason="backtest_execution_capacity_deferred",
            message="Backtest strategy rerun defers allocation and broker buying-power capacity to execution simulation.",
            evaluated_at=evaluated_at or utc_now_iso(),
            admissible_quantity=None,
            required_buying_power=None,
            available_buying_power=None,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
            protection_admission=protection_admission,
            portfolio_admission=portfolio_admission,
        )
    try:
        return build_execution_admission_snapshot(
            execution_store=execution_store,
            candidate=signal,
            limit_price=None,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
            protection_admission=protection_admission,
            portfolio_admission=portfolio_admission,
        )
    except Exception as exc:
        return build_entry_capacity_admission_payload(
            status="unknown",
            reason="execution_admission_unavailable",
            message=str(exc),
            evaluated_at=evaluated_at or utc_now_iso(),
            admissible_quantity=None,
            required_buying_power=None,
            available_buying_power=None,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
            protection_admission=protection_admission,
            portfolio_admission=portfolio_admission,
        )


__all__ = [
    "active_portfolio_admission_policies",
    "build_selected_entry_admission_snapshot",
    "portfolio_admission_policy_for_strategy",
]

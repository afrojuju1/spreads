from __future__ import annotations

from collections.abc import Mapping
from typing import Any


from core.money import money_sum_float
from core.services.risk.buying_power import (
    estimate_buying_power_requirement,
)
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_float,
    utc_now_iso,
)

from core.services.risk.candidates import (
    _candidate_max_loss,
    _candidate_root_symbol,
    _candidate_strategy_family,
    _portfolio_correlation_group,
)
from core.services.risk.policy import (
    PORTFOLIO_ADMISSION_BOUNDARY,
)

from core.services.risk.allocation import _allocation_plan_admission_evidence, _portfolio_policy_int
from core.services.risk.exposures import (
    _daily_entry_exposures,
    _open_portfolio_exposures,
    _portfolio_schema_ready,
)


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

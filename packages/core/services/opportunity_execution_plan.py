from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from core.domain.opportunity_models import (
    AllocationDecision,
    ExecutionIntent,
    Opportunity,
)

SLOT_LIMITS = {
    "reactive": {"slot_limit": 2, "risk_budget": 500.0},
    "tactical": {"slot_limit": 3, "risk_budget": 1000.0},
    "carry": {"slot_limit": 2, "risk_budget": 2000.0},
}
CARRY_SAME_SYMBOL_OVERRIDE_MARGIN = 5.0
CARRY_MAX_POSITIONS_PER_SYMBOL = 2


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalized_strategy_family(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
        "long_straddle": "long_straddle",
        "long_strangle": "long_strangle",
    }.get(normalized, normalized)


def execution_complexity(strategy_family: str) -> float:
    family = _normalized_strategy_family(strategy_family)
    if family in {"long_call", "long_put"}:
        return 0.2
    if family in {"long_straddle", "long_strangle"}:
        return 0.6
    if family in {
        "call_credit_spread",
        "put_credit_spread",
        "call_debit_spread",
        "put_debit_spread",
    }:
        return 0.4
    if family == "iron_condor":
        return 0.8
    return 0.5


def opportunity_execution_score(opportunity: Opportunity) -> float:
    evidence = opportunity.evidence
    if isinstance(evidence, Mapping):
        resolved = _as_float(evidence.get("execution_score"))
        if resolved is not None:
            return resolved
    return float(opportunity.promotion_score)


def opportunity_execution_blockers(opportunity: Opportunity) -> list[str]:
    evidence = opportunity.evidence
    if not isinstance(evidence, Mapping):
        return []
    blockers = evidence.get("execution_blockers")
    if not isinstance(blockers, Sequence) or isinstance(blockers, str):
        return []
    return [str(value) for value in blockers if str(value or "").strip()]


def _opportunity_buffer_ratio(opportunity: Opportunity) -> float | None:
    evidence = opportunity.evidence
    if not isinstance(evidence, Mapping):
        return None
    profile_evidence = evidence.get("profile_score_evidence")
    if not isinstance(profile_evidence, Mapping):
        return None
    value = _as_float(profile_evidence.get("buffer_ratio"))
    if value is None:
        return None
    return _clamp(value, 0.0, 1.5)


def _opportunity_rank_score(opportunity: Opportunity) -> float:
    base_score = opportunity_execution_score(opportunity)
    if opportunity.style_profile != "carry":
        return base_score
    buffer_ratio = _opportunity_buffer_ratio(opportunity)
    if buffer_ratio is None:
        return base_score
    return round(base_score + min(buffer_ratio * 2.0, 2.5), 4)


def rank_opportunities(opportunities: Sequence[Opportunity]) -> list[Opportunity]:
    ranked = sorted(
        list(opportunities),
        key=lambda item: (
            _opportunity_rank_score(item),
            opportunity_execution_score(item),
            item.promotion_score,
            item.discovery_score,
            -(item.execution_complexity or execution_complexity(item.strategy_family)),
        ),
        reverse=True,
    )
    return [replace(item, rank=index) for index, item in enumerate(ranked, start=1)]


def allocation_score(
    *,
    opportunity: Opportunity,
    style_profile: str,
) -> float:
    policy = SLOT_LIMITS.get(style_profile, SLOT_LIMITS["tactical"])
    desirability = opportunity_execution_score(opportunity) / 100.0
    edge_value = _clamp(opportunity.expected_edge_value or 0.0, 0.0, 0.25) / 0.25
    readiness = 1.0 if opportunity.state == "promotable" else 0.5
    max_loss = opportunity.max_loss or policy["risk_budget"]
    capital_efficiency = 1.0 - _clamp(max_loss / policy["risk_budget"], 0.0, 1.0)
    if style_profile == "carry":
        buffer_ratio = _opportunity_buffer_ratio(opportunity) or 0.0
        structure_quality = _clamp((buffer_ratio - 0.15) / 0.20, 0.0, 1.0)
        return round(
            100.0
            * (
                0.40 * desirability
                + 0.25 * edge_value
                + 0.10 * readiness
                + 0.15 * capital_efficiency
                + 0.10 * structure_quality
            ),
            1,
        )
    return round(
        100.0
        * (
            0.45 * desirability
            + 0.25 * edge_value
            + 0.15 * readiness
            + 0.15 * capital_efficiency
        ),
        1,
    )


def build_allocation_decisions(
    opportunities: Sequence[Opportunity],
) -> list[AllocationDecision]:
    if not opportunities:
        return []
    ranked_opportunities = rank_opportunities(opportunities)
    style_profile = ranked_opportunities[0].style_profile
    policy = SLOT_LIMITS.get(style_profile, SLOT_LIMITS["tactical"])
    remaining_budget = float(policy["risk_budget"])
    remaining_slots = int(policy["slot_limit"])
    taken_symbol_counts: dict[str, int] = defaultdict(int)
    ranked = sorted(
        ranked_opportunities,
        key=lambda item: (
            allocation_score(opportunity=item, style_profile=style_profile),
            -item.rank,
        ),
        reverse=True,
    )

    decisions: list[AllocationDecision] = []
    for opportunity in ranked:
        resolved_allocation_score = allocation_score(
            opportunity=opportunity,
            style_profile=style_profile,
        )
        rejection_codes: list[str] = []
        allocation_state = "not_allocated"
        allocation_reason = "Not selected."
        max_loss = opportunity.max_loss or 0.0
        budget_before = remaining_budget
        slots_before = remaining_slots
        symbol_taken_count = int(taken_symbol_counts.get(opportunity.symbol) or 0)
        carry_override = False
        execution_blockers = opportunity_execution_blockers(opportunity)

        best_remaining_other_symbol_score = None
        if style_profile == "carry" and symbol_taken_count > 0:
            other_symbol_scores = [
                allocation_score(opportunity=item, style_profile=style_profile)
                for item in ranked
                if item.symbol != opportunity.symbol
                and int(taken_symbol_counts.get(item.symbol) or 0) == 0
                and item.state == "promotable"
                and not opportunity_execution_blockers(item)
            ]
            if other_symbol_scores:
                best_remaining_other_symbol_score = max(other_symbol_scores)
            carry_override = (
                symbol_taken_count < CARRY_MAX_POSITIONS_PER_SYMBOL
                and best_remaining_other_symbol_score is not None
                and resolved_allocation_score
                >= best_remaining_other_symbol_score + CARRY_SAME_SYMBOL_OVERRIDE_MARGIN
            )

        if execution_blockers:
            rejection_codes.extend(execution_blockers)
            allocation_reason = "Opportunity is not execution-ready."
        elif opportunity.state != "promotable":
            rejection_codes.append("not_promotable")
            allocation_reason = "Opportunity did not clear the promotion floor."
        elif symbol_taken_count > 0 and not carry_override:
            rejection_codes.append("same_symbol_conflict")
            allocation_reason = (
                "A higher-ranked opportunity already consumed the symbol slot."
            )
        elif remaining_slots <= 0:
            rejection_codes.append("slot_full")
            allocation_reason = "The style slot budget is already full."
        elif max_loss > remaining_budget:
            rejection_codes.append("budget_exhausted")
            allocation_reason = "Remaining downside budget is too small."
        elif resolved_allocation_score < 55.0:
            rejection_codes.append("allocation_score_too_low")
            allocation_reason = (
                "Portfolio-adjusted score is below the allocation floor."
            )
        else:
            allocation_state = "allocated"
            allocation_reason = (
                "Selected by the provisional portfolio allocator after clearing the "
                "carry same-symbol override."
                if carry_override
                else "Selected by the provisional portfolio allocator."
            )
        if allocation_state == "allocated":
            taken_symbol_counts[opportunity.symbol] = symbol_taken_count + 1
            remaining_slots -= 1
            remaining_budget -= max_loss

        decisions.append(
            AllocationDecision(
                allocation_id=f"allocation:{opportunity.opportunity_id}",
                opportunity_id=opportunity.opportunity_id,
                cycle_id=opportunity.cycle_id,
                session_id=opportunity.session_id,
                allocation_state=allocation_state,
                allocation_score=resolved_allocation_score,
                slot_class=style_profile,
                allocation_reason=allocation_reason,
                rejection_codes=rejection_codes,
                budget_impact={
                    "max_loss": max_loss,
                    "risk_budget_before": round(budget_before, 2),
                    "risk_budget_after": round(remaining_budget, 2),
                    "slots_before": slots_before,
                    "slots_after": remaining_slots,
                },
                evidence={
                    "rank": opportunity.rank,
                    "promotion_score": opportunity.promotion_score,
                    "execution_score": opportunity_execution_score(opportunity),
                    "legacy_selection_state": opportunity.legacy_selection_state,
                    "product_class": opportunity.product_class,
                },
            )
        )
    return decisions


def _execution_template(opportunity: Opportunity) -> dict[str, str]:
    family = _normalized_strategy_family(opportunity.strategy_family)
    style = opportunity.style_profile
    if family in {"long_call", "long_put"}:
        return {
            "order_structure": "single_leg",
            "entry_policy": "passive_then_small_escalation"
            if style == "reactive"
            else "patient_single_leg_entry",
            "price_policy": "tight_debit_cap",
            "timeout_policy": "short"
            if style == "reactive"
            else ("medium" if style == "tactical" else "long"),
            "replace_policy": "1_step" if style == "reactive" else "2_step",
            "exit_policy": "defined_risk_directional_exit",
        }
    if family in {"long_straddle", "long_strangle"}:
        return {
            "order_structure": "long_vol",
            "entry_policy": "passive_then_midpoint",
            "price_policy": "tight_debit_cap",
            "timeout_policy": "short" if style == "reactive" else "medium",
            "replace_policy": "1_step" if style == "reactive" else "2_step",
            "exit_policy": "defined_risk_long_vol_exit",
        }
    if family in {"call_debit_spread", "put_debit_spread"}:
        return {
            "order_structure": "vertical",
            "entry_policy": "passive_then_midpoint",
            "price_policy": "tight_debit_cap",
            "timeout_policy": "short" if style == "reactive" else "medium",
            "replace_policy": "1_step" if style == "reactive" else "2_step",
            "exit_policy": "defined_risk_debit_spread_exit",
        }
    if family in {"call_credit_spread", "put_credit_spread"}:
        return {
            "order_structure": "vertical",
            "entry_policy": "passive_credit_entry",
            "price_policy": "credit_floor_from_scanned_midpoint",
            "timeout_policy": "short"
            if style == "reactive"
            else ("medium" if style == "tactical" else "long"),
            "replace_policy": "1_step" if style == "reactive" else "2_step",
            "exit_policy": "defined_risk_credit_spread_exit",
        }
    return {
        "order_structure": "condor",
        "entry_policy": "passive_complex_entry",
        "price_policy": "complex_credit_floor",
        "timeout_policy": "medium",
        "replace_policy": "2_step",
        "exit_policy": "defined_risk_condor_exit",
    }


def build_execution_intents(
    *,
    opportunities: Sequence[Opportunity],
    allocation_decisions: Sequence[AllocationDecision],
) -> list[ExecutionIntent]:
    opportunity_by_id = {item.opportunity_id: item for item in opportunities}
    intents: list[ExecutionIntent] = []
    for decision in allocation_decisions:
        if decision.allocation_state != "allocated":
            continue
        opportunity = opportunity_by_id[decision.opportunity_id]
        template = _execution_template(opportunity)
        intents.append(
            ExecutionIntent(
                execution_intent_id=f"execution_intent:{opportunity.opportunity_id}",
                opportunity_id=opportunity.opportunity_id,
                cycle_id=opportunity.cycle_id,
                session_id=opportunity.session_id,
                symbol=opportunity.symbol,
                strategy_family=_normalized_strategy_family(
                    opportunity.strategy_family
                ),
                order_structure=template["order_structure"],
                entry_policy=template["entry_policy"],
                price_policy=template["price_policy"],
                timeout_policy=template["timeout_policy"],
                replace_policy=template["replace_policy"],
                exit_policy=template["exit_policy"],
                validation_state="provisional_offline",
                evidence={
                    "allocation_score": decision.allocation_score,
                    "execution_score": opportunity_execution_score(opportunity),
                    "legacy_selection_state": opportunity.legacy_selection_state,
                    "rank": opportunity.rank,
                    "legs": [leg.to_payload() for leg in opportunity.legs],
                },
            )
        )
    return intents


def build_execution_plan(
    opportunities: Sequence[Opportunity],
) -> dict[str, Any]:
    ranked_opportunities = rank_opportunities(opportunities)
    allocation_decisions = build_allocation_decisions(ranked_opportunities)
    execution_intents = build_execution_intents(
        opportunities=ranked_opportunities,
        allocation_decisions=allocation_decisions,
    )
    return {
        "opportunities": ranked_opportunities,
        "allocation_decisions": allocation_decisions,
        "execution_intents": execution_intents,
    }


__all__ = [
    "CARRY_MAX_POSITIONS_PER_SYMBOL",
    "CARRY_SAME_SYMBOL_OVERRIDE_MARGIN",
    "SLOT_LIMITS",
    "allocation_score",
    "build_allocation_decisions",
    "build_execution_intents",
    "build_execution_plan",
    "execution_complexity",
    "opportunity_execution_blockers",
    "opportunity_execution_score",
    "rank_opportunities",
]

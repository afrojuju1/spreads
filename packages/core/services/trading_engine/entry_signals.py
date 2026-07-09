from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.candidate_fields import (
    candidate_economics,
    candidate_evidence_metrics,
    candidate_policy_context,
    candidate_strategy_metrics,
    risk_hints,
)
from core.services.option_structures import candidate_legs
from core.services.runtime_identity import resolve_runtime_policy_fields
from core.services.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.data_runtime import entry_engine_label
from core.services.trading_strategy_runtime_models import EntryRuntime
from core.value_coercion import unique_text_list

ENTRY_MONITOR_LIMIT = 12
NATURAL_ENTRY_PROVENANCE = "natural_strategy"
OBSERVATION_ENTRY_PROVENANCE = "strategy_observation"


def candidate_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return dict(row)


def quality_evidence_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    evidence = row.get("evidence") if isinstance(row.get("evidence"), Mapping) else {}
    waterfall = evidence.get("quality_waterfall") if isinstance(evidence, Mapping) else None
    if not isinstance(waterfall, Mapping):
        if not isinstance(evidence, Mapping) or evidence.get("quality_profile_id") in (None, ""):
            return {}
        return {
            "quality_profile_id": evidence.get("quality_profile_id") if isinstance(evidence, Mapping) else None,
            "quality_waterfall_blocked": evidence.get("quality_waterfall_blocked") if isinstance(evidence, Mapping) else None,
            "quality_waterfall_stage_counts": dict(evidence.get("quality_waterfall_stage_counts") or {}) if isinstance(evidence, Mapping) else {},
        }
    return {
        "quality_profile_id": evidence.get("quality_profile_id") or waterfall.get("profile_id"),
        "quality_waterfall_blocked": waterfall.get("blocked"),
        "quality_waterfall_stage_counts": dict(waterfall.get("stage_counts") or {}),
    }


def runtime_signal_eligibility(runtime: EntryRuntime, row: Mapping[str, Any], *, observation_only: bool = False) -> str:
    eligibility = str(row.get("eligibility") or "live").strip().lower() or "live"
    if (observation_only or runtime.strategy.execution.mode == "shadow") and eligibility == "live":
        return "analysis_only"
    return eligibility


def signal_blockers(candidate: Mapping[str, Any], *, eligibility: str | None = None) -> list[str]:
    blockers: list[str] = []
    if str(eligibility or "live").strip().lower() != "live":
        blockers.append("analysis_only")
    for field in ("scoring_blockers", "execution_blockers", "ranking_policy_blockers"):
        for blocker in unique_text_list(candidate.get(field)):
            if blocker not in blockers:
                blockers.append(blocker)
    return blockers


def execution_shape(candidate: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(candidate)
    order_payload = dict(payload.get("order_payload") or {})
    legs = candidate_legs(payload)
    return {
        "underlying_symbol": payload.get("underlying_symbol"),
        "trade_structure": payload.get("trade_structure") or payload.get("strategy_family") or payload.get("strategy"),
        "strategy_family": payload.get("strategy_family") or payload.get("strategy") or payload.get("trade_structure"),
        "profile": payload.get("profile"),
        "expiration_date": payload.get("expiration_date"),
        "structure_identity": resolve_candidate_identity(payload, strategy=payload.get("strategy")),
        "legs": legs,
        "order_payload": order_payload,
        "order_class": order_payload.get("order_class") or ("mleg" if len(legs) > 1 else "single"),
        "quantity": payload.get("quantity") or order_payload.get("qty"),
        "limit_price": order_payload.get("limit_price")
        or payload.get("limit_price")
        or payload.get("midpoint_credit")
        or payload.get("midpoint_value"),
    }


def build_entry_signal_row_from_selection(
    *,
    runtime: EntryRuntime,
    market_date: str,
    generated_at: str,
    strategy_run_id: str,
    row: Mapping[str, Any],
    observation_only: bool = False,
) -> dict[str, Any]:
    candidate = candidate_payload(row)
    symbol = str(candidate.get("underlying_symbol") or row.get("underlying_symbol") or "").upper()
    eligibility = runtime_signal_eligibility(runtime, row, observation_only=observation_only)
    provenance = OBSERVATION_ENTRY_PROVENANCE if observation_only else NATURAL_ENTRY_PROVENANCE
    policy_fields = resolve_runtime_policy_fields(
        profile=runtime.build_settings.build_profile,
        root_symbol=symbol,
    )
    return {
        **dict(row),
        "label": entry_engine_label(runtime),
        "market_date": market_date,
        "session_date": market_date,
        "root_symbol": symbol,
        "trading_strategy_id": runtime.trading_strategy_id,
        "strategy_run_id": strategy_run_id,
        "config_hash": runtime.config_hash,
        "strategy_family": runtime.trade_structure,
        "trade_structure": runtime.trade_structure,
        "profile": runtime.build_settings.build_profile,
        "style_profile": str(policy_fields["style_profile"]),
        "horizon_intent": str(policy_fields["horizon_intent"]),
        "product_class": str(policy_fields["product_class"]),
        "expiration_date": candidate.get("expiration_date"),
        "underlying_symbol": symbol,
        "selection_state": str(row.get("selection_state") or "monitor"),
        "selection_rank": (None if row.get("selection_rank") in (None, "") else int(row["selection_rank"])),
        "state_reason": str(row.get("state_reason") or "selected_runtime_signal"),
        "origin": "engine_selection",
        "eligibility": eligibility,
        "eligibility_state": eligibility,
        "promotion_score": candidate.get("promotion_score"),
        "execution_score": candidate.get("execution_score"),
        "confidence": candidate.get("confidence"),
        "created_at": generated_at,
        "updated_at": generated_at,
        "expires_at": None,
        "reason_codes": [str(row.get("state_reason") or "selected_runtime_signal")],
        "blockers": signal_blockers(candidate, eligibility=eligibility),
        "legs": candidate_legs(candidate),
        "economics": candidate_economics(candidate),
        "strategy_metrics": candidate_strategy_metrics(candidate),
        "order_payload": dict(candidate.get("order_payload") or {}),
        "evidence": {
            "runtime_kind": "entry",
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_structure": runtime.trade_structure,
            "entry_recipe_refs": list(runtime.entry_recipe_refs),
            "trigger_policy": dict(runtime.trigger_policy),
            "execution_mode": runtime.strategy.execution.mode,
            "approval_mode": runtime.strategy.execution.approval,
            "entry_run_mode": "observation" if observation_only else "natural",
            "validation_provenance": provenance,
            "observation_only": observation_only,
            "selection_state": row.get("selection_state"),
            "selection_rank": row.get("selection_rank"),
            "generated_at": generated_at,
            "last_present_at": generated_at,
            **candidate_evidence_metrics(candidate),
            **candidate_policy_context(candidate),
        },
        "execution_shape": execution_shape(candidate),
        "risk_hints": risk_hints(candidate),
        "source_cycle_id": strategy_run_id,
        "source_selection_state": row.get("selection_state"),
        "candidate_identity": resolve_candidate_identity(candidate, strategy=candidate.get("strategy")),
        "candidate": candidate,
    }


def entry_selection_summary(
    *,
    candidate_rows_by_symbol: dict[str, list[dict[str, Any]]],
    selected_rows: list[dict[str, Any]],
    selection_memory: dict[str, Any],
) -> dict[str, Any]:
    candidate_count = sum(len(rows) for rows in candidate_rows_by_symbol.values())
    signal_count = len(selected_rows)
    if signal_count:
        status = "signals_selected"
        message = f"{signal_count} signal{' was' if signal_count == 1 else 's were'} selected from {candidate_count} candidates."
    elif candidate_count:
        status = "no_entry_signals"
        message = "Candidates existed, but none cleared live selection for this strategy run."
    else:
        status = "no_candidates"
        message = "No candidates matched this strategy in the current run."
    return {
        "status": status,
        "message": message,
        "candidate_symbol_count": len(candidate_rows_by_symbol),
        "candidate_count": candidate_count,
        "signal_count": signal_count,
        "selection_memory": dict(selection_memory),
    }


__all__ = [
    "ENTRY_MONITOR_LIMIT",
    "NATURAL_ENTRY_PROVENANCE",
    "OBSERVATION_ENTRY_PROVENANCE",
    "build_entry_signal_row_from_selection",
    "candidate_payload",
    "entry_selection_summary",
    "execution_shape",
    "quality_evidence_summary",
    "runtime_signal_eligibility",
    "signal_blockers",
]

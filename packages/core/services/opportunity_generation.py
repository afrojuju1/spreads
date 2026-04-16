from __future__ import annotations

from typing import Any

from core.services.entry_recipes import evaluate_entry_recipes
from core.services.live_selection import select_live_opportunities
from core.services.option_structures import candidate_legs, legs_identity_key
from core.services.runtime_identity import build_pipeline_id
from core.services.runtime_policy import (
    build_runtime_policy_ref,
    resolve_runtime_policy_fields,
)
from core.services.automation_runtime import EntryRuntime
from core.services.strategy_registry import resolve_strategy_definition


def build_automation_run_id(cycle_id: str, bot_id: str, automation_id: str) -> str:
    return f"automation_run:{cycle_id}:{bot_id}:{automation_id}"


def build_runtime_opportunity_id(
    runtime: EntryRuntime,
    *,
    session_date: str,
    candidate: dict[str, Any],
) -> str:
    candidate_identity = legs_identity_key(
        strategy=candidate.get("strategy"),
        legs=candidate_legs(candidate),
    )
    return (
        f"opportunity:{runtime.bot_id}:{runtime.automation_id}:{session_date}:"
        f"{candidate['underlying_symbol']}:{candidate_identity}"
    )


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    numeric = _coerce_float(value)
    return None if numeric is None else int(numeric)


def _candidate_identity(candidate: dict[str, Any]) -> str:
    return legs_identity_key(
        strategy=candidate.get("strategy"),
        legs=candidate_legs(candidate),
    )


def _candidate_matches_runtime(
    candidate: dict[str, Any],
    runtime: EntryRuntime,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    strategy = resolve_strategy_definition(runtime.strategy_id)
    if not strategy.matches_candidate(candidate):
        reasons.append("strategy_family_mismatch")
    underlying_symbol = str(candidate.get("underlying_symbol") or "").upper()
    if runtime.symbols and underlying_symbol not in set(runtime.symbols):
        reasons.append("symbol_out_of_scope")
    days_to_expiration = _coerce_int(candidate.get("days_to_expiration"))
    if (
        runtime.build_settings.dte_min is not None
        and days_to_expiration is not None
        and days_to_expiration < runtime.build_settings.dte_min
    ):
        reasons.append("dte_below_min")
    if (
        runtime.build_settings.dte_max is not None
        and days_to_expiration is not None
        and days_to_expiration > runtime.build_settings.dte_max
    ):
        reasons.append("dte_above_max")
    short_delta = _coerce_float(candidate.get("short_delta"))
    if short_delta is not None:
        short_delta = abs(short_delta)
        if (
            runtime.build_settings.short_delta_min is not None
            and short_delta < runtime.build_settings.short_delta_min
        ):
            reasons.append("short_delta_below_min")
        if (
            runtime.build_settings.short_delta_max is not None
            and short_delta > runtime.build_settings.short_delta_max
        ):
            reasons.append("short_delta_above_max")
    width = _coerce_float(candidate.get("width"))
    if width is not None and runtime.build_settings.width_points:
        allowed_widths = {
            round(value, 4) for value in runtime.build_settings.width_points
        }
        if round(width, 4) not in allowed_widths:
            reasons.append("width_not_allowed")
    open_interest_floor = runtime.build_settings.min_open_interest
    if open_interest_floor is not None:
        short_oi = _coerce_int(candidate.get("short_open_interest")) or 0
        long_oi = _coerce_int(candidate.get("long_open_interest")) or 0
        if min(short_oi, long_oi) < open_interest_floor:
            reasons.append("open_interest_below_floor")
    spread_ceiling = runtime.build_settings.max_leg_spread_pct_mid
    if spread_ceiling is not None:
        short_spread = _coerce_float(candidate.get("short_relative_spread")) or 0.0
        long_spread = _coerce_float(candidate.get("long_relative_spread")) or 0.0
        if max(short_spread, long_spread) > spread_ceiling:
            reasons.append("relative_spread_above_ceiling")
    recipe_result = evaluate_entry_recipes(candidate, runtime.entry_recipe_refs)
    if not recipe_result.passed:
        reasons.extend(recipe_result.reason_codes)
    return not reasons, reasons


def _filtered_symbol_candidates(
    *,
    symbol_candidates: dict[str, list[dict[str, Any]]],
    runtime: EntryRuntime,
) -> dict[str, list[dict[str, Any]]]:
    filtered: dict[str, list[dict[str, Any]]] = {}
    for symbol, rows in symbol_candidates.items():
        candidates: list[dict[str, Any]] = []
        for row in rows:
            candidate = dict(row)
            matched, reasons = _candidate_matches_runtime(candidate, runtime)
            if not matched:
                continue
            candidate["runtime_recipe_refs"] = list(runtime.entry_recipe_refs)
            if reasons:
                candidate["runtime_filter_reasons"] = list(reasons)
            candidates.append(candidate)
        if candidates:
            filtered[str(symbol)] = candidates
    return filtered


def _candidate_economics(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "midpoint_credit": candidate.get("midpoint_credit"),
        "natural_credit": candidate.get("natural_credit"),
        "max_profit": candidate.get("max_profit"),
        "max_loss": candidate.get("max_loss"),
        "return_on_risk": candidate.get("return_on_risk"),
        "fill_ratio": candidate.get("fill_ratio"),
    }


def _candidate_strategy_metrics(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "width": candidate.get("width"),
        "short_strike": candidate.get("short_strike"),
        "long_strike": candidate.get("long_strike"),
        "expected_move": candidate.get("expected_move"),
        "underlying_price": candidate.get("underlying_price"),
        "side_balance_score": candidate.get("side_balance_score"),
        "wing_symmetry_ratio": candidate.get("wing_symmetry_ratio"),
    }


def _risk_hints(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "midpoint_credit": candidate.get("midpoint_credit"),
        "natural_credit": candidate.get("natural_credit"),
        "max_loss": candidate.get("max_loss"),
        "return_on_risk": candidate.get("return_on_risk"),
        "fill_ratio": candidate.get("fill_ratio"),
        "width": candidate.get("width"),
    }


def _execution_shape(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "underlying_symbol": candidate.get("underlying_symbol"),
        "short_symbol": candidate.get("short_symbol"),
        "long_symbol": candidate.get("long_symbol"),
        "order_payload": dict(candidate.get("order_payload") or {}),
    }


def _opportunity_source_index(
    persisted_opportunities: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in persisted_opportunities:
        payload = (
            row.get("candidate") if isinstance(row.get("candidate"), dict) else row
        )
        candidate = dict(payload)
        symbol = str(candidate.get("underlying_symbol") or "").upper()
        candidate_identity = _candidate_identity(candidate)
        index[(symbol, candidate_identity)] = dict(row)
    return index


def build_runtime_opportunity_payload(
    *,
    runtime: EntryRuntime,
    label: str,
    session_date: str,
    generated_at: str,
    cycle_id: str,
    automation_run_id: str,
    row: dict[str, Any],
    source_row: dict[str, Any] | None,
) -> dict[str, Any]:
    candidate = (
        dict(row.get("candidate"))
        if isinstance(row.get("candidate"), dict)
        else dict(row)
    )
    policy_fields = resolve_runtime_policy_fields(
        profile=runtime.build_settings.scanner_profile,
        root_symbol=str(candidate.get("underlying_symbol") or ""),
    )
    return {
        "opportunity_id": build_runtime_opportunity_id(
            runtime, session_date=session_date, candidate=candidate
        ),
        "pipeline_id": build_pipeline_id(label),
        "label": label,
        "market_date": session_date,
        "session_date": session_date,
        "cycle_id": cycle_id,
        "root_symbol": str(candidate.get("underlying_symbol") or ""),
        "bot_id": runtime.bot_id,
        "automation_id": runtime.automation_id,
        "automation_run_id": automation_run_id,
        "strategy_config_id": runtime.strategy_config_id,
        "strategy_id": runtime.strategy_id,
        "config_hash": runtime.config_hash,
        "policy_ref": build_runtime_policy_ref(
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            strategy_config_id=runtime.strategy_config_id,
            strategy_id=runtime.strategy_id,
            market_date=session_date,
        ),
        "strategy_family": runtime.strategy_family,
        "profile": runtime.build_settings.scanner_profile,
        "style_profile": str(policy_fields["style_profile"]),
        "horizon_intent": str(policy_fields["horizon_intent"]),
        "product_class": str(policy_fields["product_class"]),
        "expiration_date": candidate.get("expiration_date"),
        "entity_type": "automation_signal_subject",
        "entity_key": (
            f"automation_signal_subject:{runtime.bot_id}:{runtime.automation_id}:"
            f"{candidate.get('underlying_symbol')}"
        ),
        "underlying_symbol": str(candidate.get("underlying_symbol") or ""),
        "side": row.get("side") or source_row.get("side") if source_row else None,
        "side_bias": row.get("side_bias") or source_row.get("side_bias")
        if source_row
        else None,
        "selection_state": str(row.get("selection_state") or "monitor"),
        "selection_rank": (
            None
            if row.get("selection_rank") in (None, "")
            else int(row["selection_rank"])
        ),
        "state_reason": str(row.get("state_reason") or "selected_runtime_candidate"),
        "origin": "config_runtime",
        "eligibility": str(row.get("eligibility") or "live"),
        "eligibility_state": str(row.get("eligibility") or "live"),
        "promotion_score": _coerce_float(candidate.get("promotion_score")),
        "execution_score": _coerce_float(candidate.get("execution_score")),
        "confidence": _coerce_float(candidate.get("confidence")),
        "signal_state_ref": None,
        "lifecycle_state": (
            "ready"
            if str(row.get("selection_state") or "") == "promotable"
            else "candidate"
        ),
        "created_at": generated_at,
        "updated_at": generated_at,
        "expires_at": source_row.get("expires_at") if source_row else None,
        "reason_codes": [str(row.get("state_reason") or "selected_runtime_candidate")],
        "blockers": [],
        "legs": candidate_legs(candidate),
        "economics": _candidate_economics(candidate),
        "strategy_metrics": _candidate_strategy_metrics(candidate),
        "order_payload": dict(candidate.get("order_payload") or {}),
        "evidence": {
            "runtime_kind": "entry",
            "entry_recipe_refs": list(runtime.entry_recipe_refs),
            "trigger_policy": dict(runtime.trigger_policy),
            "selection_state": row.get("selection_state"),
            "selection_rank": row.get("selection_rank"),
            "generated_at": generated_at,
            "source_opportunity_id": None
            if source_row is None
            else source_row.get("opportunity_id"),
        },
        "execution_shape": _execution_shape(candidate),
        "risk_hints": _risk_hints(candidate),
        "source_cycle_id": cycle_id,
        "source_candidate_id": None
        if source_row is None or source_row.get("candidate_id") in (None, "")
        else int(source_row["candidate_id"]),
        "source_selection_state": None
        if source_row is None
        else source_row.get("selection_state"),
        "candidate_identity": _candidate_identity(candidate),
        "candidate": candidate,
    }


def sync_entry_runtime_opportunities(
    *,
    signal_store: Any,
    label: str,
    session_date: str,
    generated_at: str,
    cycle_id: str,
    entry_runtimes: list[EntryRuntime],
    symbol_candidates: dict[str, list[dict[str, Any]]],
    persisted_opportunities: list[dict[str, Any]],
    job_run_id: str | None,
    top_promotable: int,
    top_monitor: int,
) -> dict[str, Any]:
    if not signal_store.automation_runtime_schema_ready():
        return {
            "automation_runs_upserted": 0,
            "runtime_opportunities_upserted": 0,
            "runtime_opportunities_expired": 0,
            "opportunities": [],
        }

    source_index = _opportunity_source_index(persisted_opportunities)
    automation_runs_upserted = 0
    runtime_opportunities_upserted = 0
    runtime_opportunities_expired = 0
    scoped_opportunities: list[dict[str, Any]] = []

    for runtime in entry_runtimes:
        filtered_candidates = _filtered_symbol_candidates(
            symbol_candidates=symbol_candidates,
            runtime=runtime,
        )
        selection = select_live_opportunities(
            label=label,
            cycle_id=cycle_id,
            generated_at=generated_at,
            symbol_candidates=filtered_candidates,
            previous_promotable={},
            previous_selection_memory={},
            top_promotable=top_promotable,
            top_monitor=top_monitor,
            profile=runtime.build_settings.scanner_profile,
        )
        automation_run_id = build_automation_run_id(
            cycle_id, runtime.bot_id, runtime.automation_id
        )
        signal_store.upsert_automation_run(
            automation_run_id=automation_run_id,
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            strategy_config_id=runtime.strategy_config_id,
            trigger_type="collector_cycle",
            job_run_id=job_run_id,
            cycle_id=cycle_id,
            label=label,
            session_date=session_date,
            started_at=generated_at,
            completed_at=generated_at,
            status="completed",
            result={
                "candidate_symbol_count": len(filtered_candidates),
                "opportunity_count": len(selection["opportunities"]),
            },
            config_hash=runtime.config_hash,
        )
        automation_runs_upserted += 1

        active_runtime_opportunity_ids: list[str] = []
        for row in selection["opportunities"]:
            candidate = (
                dict(row.get("candidate"))
                if isinstance(row.get("candidate"), dict)
                else dict(row)
            )
            source_row = source_index.get(
                (
                    str(candidate.get("underlying_symbol") or "").upper(),
                    _candidate_identity(candidate),
                )
            )
            payload = build_runtime_opportunity_payload(
                runtime=runtime,
                label=label,
                session_date=session_date,
                generated_at=generated_at,
                cycle_id=cycle_id,
                automation_run_id=automation_run_id,
                row=dict(row),
                source_row=source_row,
            )
            active_runtime_opportunity_ids.append(str(payload["opportunity_id"]))
            opportunity, _changed = signal_store.upsert_opportunity(**payload)
            runtime_opportunities_upserted += 1
            scoped_opportunities.append(dict(opportunity))

        expired_rows = signal_store.expire_absent_opportunities(
            label=label,
            session_date=session_date,
            active_opportunity_ids=active_runtime_opportunity_ids,
            expired_at=generated_at,
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            runtime_owned=True,
        )
        runtime_opportunities_expired += len(expired_rows)

    return {
        "automation_runs_upserted": automation_runs_upserted,
        "runtime_opportunities_upserted": runtime_opportunities_upserted,
        "runtime_opportunities_expired": runtime_opportunities_expired,
        "opportunities": scoped_opportunities,
    }


__all__ = [
    "build_automation_run_id",
    "build_runtime_opportunity_id",
    "build_runtime_opportunity_payload",
    "sync_entry_runtime_opportunities",
]

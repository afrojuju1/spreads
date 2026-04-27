from __future__ import annotations

from typing import Any

from core.services.live_selection import select_live_opportunities
from core.services.option_structures import (
    candidate_legs,
    payload_structure_identity,
)
from core.services.opportunity_fields import (
    candidate_economics,
    candidate_evidence_metrics,
    candidate_policy_context,
    candidate_strategy_metrics,
    risk_hints,
)
from core.services.runtime_candidate_filters import filter_runtime_symbol_candidates
from core.services.runtime_identity import build_pipeline_id
from core.services.runtime_policy import (
    build_runtime_policy_ref,
    resolve_runtime_policy_fields,
)
from core.services.automation_runtime import EntryRuntime
from core.services.strategy_builders import runtime_owner_key


def build_automation_run_id(cycle_id: str, bot_id: str, automation_id: str) -> str:
    return f"automation_run:{cycle_id}:{bot_id}:{automation_id}"


def build_runtime_opportunity_id(
    runtime: EntryRuntime,
    *,
    session_date: str,
    candidate: dict[str, Any],
) -> str:
    candidate_identity = payload_structure_identity(
        candidate,
        strategy=candidate.get("strategy"),
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


def _candidate_identity(candidate: dict[str, Any]) -> str:
    return str(payload_structure_identity(candidate, strategy=candidate.get("strategy")) or "")


def _normalized_blockers(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    blockers: list[str] = []
    for item in value:
        rendered = str(item or "").strip()
        if rendered and rendered not in blockers:
            blockers.append(rendered)
    return blockers


def _opportunity_blockers(
    candidate: dict[str, Any], *, eligibility: str | None = None
) -> list[str]:
    blockers: list[str] = []
    if str(eligibility or "live").strip().lower() != "live":
        blockers.append("analysis_only")
    for field in ("scoring_blockers", "execution_blockers"):
        for blocker in _normalized_blockers(candidate.get(field)):
            if blocker not in blockers:
                blockers.append(blocker)
    for blocker in _normalized_blockers(candidate.get("ranking_policy_blockers")):
        if blocker not in blockers:
            blockers.append(blocker)
    return blockers


def _runtime_opportunity_eligibility(
    runtime: EntryRuntime,
    row: dict[str, Any],
) -> str:
    eligibility = str(row.get("eligibility") or "live").strip().lower() or "live"
    if runtime.automation.automation.execution_mode == "shadow" and eligibility == "live":
        return "analysis_only"
    return eligibility


def _execution_shape(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "underlying_symbol": candidate.get("underlying_symbol"),
        "structure_identity": _candidate_identity(candidate),
        "legs": candidate_legs(candidate),
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


def _read_previous_runtime_selection(
    *,
    signal_store: Any,
    runtime: EntryRuntime,
    session_date: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    previous_runs = [
        dict(row)
        for row in signal_store.list_automation_runs(
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            session_date=session_date,
            limit=1,
        )
    ]
    if not previous_runs:
        return {}, {}
    previous_run = previous_runs[0]
    selection_memory = {}
    result_payload = (
        previous_run.get("result")
        if isinstance(previous_run.get("result"), dict)
        else previous_run.get("result_json")
    )
    if isinstance(result_payload, dict) and isinstance(
        result_payload.get("selection_memory"), dict
    ):
        selection_memory = {
            str(symbol): dict(state)
            for symbol, state in dict(
                result_payload.get("selection_memory") or {}
            ).items()
            if isinstance(symbol, str) and isinstance(state, dict)
        }
    previous_promotable: dict[str, dict[str, Any]] = {}
    for row in signal_store.list_opportunities(
        bot_id=runtime.bot_id,
        automation_id=runtime.automation_id,
        automation_run_id=str(previous_run["automation_run_id"]),
        runtime_owned=True,
        limit=500,
    ):
        payload = dict(row)
        if str(payload.get("selection_state") or "") != "promotable":
            continue
        candidate = (
            payload.get("candidate")
            if isinstance(payload.get("candidate"), dict)
            else payload
        )
        symbol = str(
            payload.get("underlying_symbol") or candidate.get("underlying_symbol") or ""
        ).upper()
        if not symbol:
            continue
        previous_promotable[symbol] = dict(candidate)
    return previous_promotable, selection_memory


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
    eligibility = _runtime_opportunity_eligibility(runtime, row)
    blockers = _opportunity_blockers(candidate, eligibility=eligibility)
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
        "eligibility": eligibility,
        "eligibility_state": eligibility,
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
        "blockers": blockers,
        "legs": candidate_legs(candidate),
        "economics": candidate_economics(candidate),
        "strategy_metrics": candidate_strategy_metrics(candidate),
        "order_payload": dict(candidate.get("order_payload") or {}),
        "evidence": {
            "runtime_kind": "entry",
            "entry_recipe_refs": list(runtime.entry_recipe_refs),
            "trigger_policy": dict(runtime.trigger_policy),
            "execution_mode": runtime.automation.automation.execution_mode,
            "approval_mode": runtime.automation.automation.approval_mode,
            "selection_state": row.get("selection_state"),
            "selection_rank": row.get("selection_rank"),
            "generated_at": generated_at,
            "last_present_at": generated_at,
            **candidate_evidence_metrics(candidate),
            **candidate_policy_context(candidate),
            "source_opportunity_id": None
            if source_row is None
            else source_row.get("opportunity_id"),
        },
        "execution_shape": _execution_shape(candidate),
        "risk_hints": risk_hints(candidate),
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
    runtime_candidate_rows_by_owner: dict[
        tuple[str, str], dict[str, list[dict[str, Any]]]
    ]
    | None,
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
        previous_promotable, previous_selection_memory = (
            _read_previous_runtime_selection(
                signal_store=signal_store,
                runtime=runtime,
                session_date=session_date,
            )
        )
        owner_candidates = None
        if runtime_candidate_rows_by_owner:
            owner_candidates = runtime_candidate_rows_by_owner.get(
                runtime_owner_key(runtime)
            )
        source_candidates = (
            {
                str(symbol): [dict(candidate) for candidate in rows]
                for symbol, rows in owner_candidates.items()
            }
            if owner_candidates
            else {
                str(symbol): [dict(candidate) for candidate in rows]
                for symbol, rows in symbol_candidates.items()
            }
        )
        filtered_candidates, _runtime_filter_reason_counts = (
            filter_runtime_symbol_candidates(
                symbol_candidates=source_candidates,
                runtime=runtime,
            )
        )
        selection = select_live_opportunities(
            label=label,
            cycle_id=cycle_id,
            generated_at=generated_at,
            symbol_candidates=filtered_candidates,
            previous_promotable=previous_promotable,
            previous_selection_memory=previous_selection_memory,
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
            trigger_type="discovery_run_cycle",
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
                "selection_memory": dict(selection.get("selection_memory") or {}),
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

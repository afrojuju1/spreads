from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
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
from core.services.trading_strategy_runtime import EntryRuntime
from core.services.strategy_builders import runtime_owner_key


def build_trading_strategy_run_id(cycle_id: str, trading_strategy_id: str) -> str:
    return f"strategy_run:{cycle_id}:{trading_strategy_id}:entry"


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
    return f"opportunity:{runtime.trading_strategy_id}:{session_date}:" f"{candidate['underlying_symbol']}:{candidate_identity}"


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


def _opportunity_blockers(candidate: dict[str, Any], *, eligibility: str | None = None) -> list[str]:
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
    if runtime.strategy.execution.mode == "shadow" and eligibility == "live":
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
        payload = row.get("candidate") if isinstance(row.get("candidate"), dict) else row
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
        for row in signal_store.list_strategy_runs(
            trading_strategy_id=runtime.trading_strategy_id,
            session_date=session_date,
            limit=1,
        )
    ]
    if not previous_runs:
        return {}, {}
    previous_run = previous_runs[0]
    selection_memory = {}
    result_payload = previous_run.get("result") if isinstance(previous_run.get("result"), dict) else previous_run.get("result_json")
    if isinstance(result_payload, dict) and isinstance(result_payload.get("selection_memory"), dict):
        selection_memory = {
            str(symbol): dict(state)
            for symbol, state in dict(result_payload.get("selection_memory") or {}).items()
            if isinstance(symbol, str) and isinstance(state, dict)
        }
    previous_promotable: dict[str, dict[str, Any]] = {}
    for row in signal_store.list_opportunities(
        trading_strategy_id=runtime.trading_strategy_id,
        strategy_run_id=str(previous_run["strategy_run_id"]),
        runtime_owned=True,
        limit=500,
    ):
        payload = dict(row)
        if str(payload.get("selection_state") or "") != "promotable":
            continue
        candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else payload
        symbol = str(payload.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
        if not symbol:
            continue
        previous_promotable[symbol] = dict(candidate)
    return previous_promotable, selection_memory


def _selection_score(row: Mapping[str, Any]) -> float:
    for key in ("execution_score", "promotion_score", "quality_score"):
        value = _coerce_float(row.get(key))
        if value is not None:
            return value
    return 0.0


def _sorted_runtime_candidates(
    candidates: Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in list(candidates or []) if isinstance(row, Mapping)]
    return sorted(
        rows,
        key=lambda row: (
            _selection_score(row),
            _coerce_float(row.get("execution_score")) or 0.0,
            _coerce_float(row.get("promotion_score")) or 0.0,
            _coerce_float(row.get("quality_score")) or 0.0,
            _coerce_float(row.get("return_on_risk")) or 0.0,
            _coerce_float(row.get("midpoint_credit")) or 0.0,
        ),
        reverse=True,
    )


def _normalized_text_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    normalized: list[str] = []
    for item in value:
        rendered = str(item or "").strip()
        if rendered and rendered not in normalized:
            normalized.append(rendered)
    return normalized


def _runtime_candidate_reason_codes(candidate: Mapping[str, Any]) -> list[str]:
    reason_codes: list[str] = []
    for field in (
        "scoring_blockers",
        "execution_blockers",
        "ranking_policy_blockers",
    ):
        for blocker in _normalized_text_list(candidate.get(field)):
            if blocker not in reason_codes:
                reason_codes.append(blocker)
    scoring_state = str(candidate.get("scoring_state") or "").strip().lower()
    score_thresholds = candidate.get("score_thresholds") if isinstance(candidate.get("score_thresholds"), Mapping) else {}
    monitor_floor = _coerce_float(score_thresholds.get("monitor_floor"))
    if monitor_floor is not None and _selection_score(candidate) < monitor_floor and "score_below_monitor_floor" not in reason_codes:
        reason_codes.append("score_below_monitor_floor")
    if scoring_state == "blocked" and not reason_codes:
        reason_codes.append("scoring_state_blocked")
    if not reason_codes:
        reason_codes.append("not_retained_in_live_selection")
    return reason_codes


def _runtime_candidate_preview(
    candidate: Mapping[str, Any],
    *,
    min_opportunity_score: float | None,
) -> dict[str, Any]:
    score_thresholds = candidate.get("score_thresholds") if isinstance(candidate.get("score_thresholds"), Mapping) else {}
    selection_score = _selection_score(candidate)
    min_score = None if min_opportunity_score is None else float(min_opportunity_score)
    return {
        "underlying_symbol": candidate.get("underlying_symbol"),
        "strategy": candidate.get("strategy"),
        "structure_identity": _candidate_identity(dict(candidate)),
        "quality_score": _coerce_float(candidate.get("quality_score")),
        "promotion_score": _coerce_float(candidate.get("promotion_score")),
        "execution_score": _coerce_float(candidate.get("execution_score")),
        "selection_score": round(selection_score, 1),
        "selection_state": candidate.get("selection_state"),
        "scoring_state": candidate.get("scoring_state"),
        "scoring_state_reason": candidate.get("scoring_state_reason"),
        "setup_status": candidate.get("setup_status"),
        "ranking_policy_status": candidate.get("ranking_policy_status"),
        "monitor_floor": _coerce_float(score_thresholds.get("monitor_floor")),
        "promotion_floor": _coerce_float(score_thresholds.get("promotion_floor")),
        "min_opportunity_score": min_score,
        "min_opportunity_score_delta": None if min_score is None else round(selection_score - min_score, 1),
        "reason_codes": _runtime_candidate_reason_codes(candidate),
    }


def _build_runtime_selection_summary(
    *,
    runtime: EntryRuntime,
    filtered_candidates: Mapping[str, list[Mapping[str, Any]]],
    runtime_filter_reason_counts: Mapping[str, int] | None,
    selected_rows: Sequence[Mapping[str, Any]],
    selection_memory: Mapping[str, Any] | None,
    projected_from_discovery: bool,
) -> dict[str, Any]:
    flattened_candidates: list[dict[str, Any]] = []
    scoring_state_counts: Counter[str] = Counter()
    rejection_reason_counts: Counter[str] = Counter()
    top_candidates: list[dict[str, Any]] = []
    selected_ids = {
        (
            str(row.get("underlying_symbol") or "").upper(),
            _candidate_identity(dict(row.get("candidate") or row)),
        )
        for row in list(selected_rows or [])
        if str(row.get("underlying_symbol") or "").strip()
    }

    for rows in filtered_candidates.values():
        ranked_rows = _sorted_runtime_candidates(rows)
        flattened_candidates.extend(ranked_rows)
        for candidate in ranked_rows:
            scoring_state = str(candidate.get("scoring_state") or "").strip().lower()
            scoring_state_counts[scoring_state or "unknown"] += 1
            candidate_key = (
                str(candidate.get("underlying_symbol") or "").upper(),
                _candidate_identity(dict(candidate)),
            )
            if candidate_key not in selected_ids:
                rejection_reason_counts.update(_runtime_candidate_reason_codes(candidate))

    min_score = _coerce_float(runtime.trigger_policy.get("min_opportunity_score"))
    for candidate in _sorted_runtime_candidates(flattened_candidates)[:3]:
        top_candidates.append(
            _runtime_candidate_preview(
                candidate,
                min_opportunity_score=min_score,
            )
        )

    candidate_symbol_count = len(filtered_candidates)
    candidate_count = len(flattened_candidates)
    opportunity_count = len(list(selected_rows or []))
    if opportunity_count > 0:
        status = "opportunities_selected"
        message = (
            f"{opportunity_count} runtime opportunit"
            f"{'y' if opportunity_count == 1 else 'ies'} selected from "
            f"{candidate_count} filtered candidate"
            f"{'' if candidate_count == 1 else 's'}."
        )
    elif candidate_count <= 0:
        status = "no_runtime_candidates"
        message = "No runtime candidates matched this strategy in the current cycle."
    elif projected_from_discovery:
        status = "no_discovery_opportunity_match"
        message = "Runtime candidates existed, but discovery did not persist any matching " "live opportunities for this cycle."
    else:
        status = "no_runtime_opportunities"
        message = "Runtime candidates existed, but none cleared live selection for this cycle."

    return {
        "selection_source": ("discovery_projection" if projected_from_discovery else "live_selection"),
        "status": status,
        "message": message,
        "candidate_symbol_count": candidate_symbol_count,
        "candidate_count": candidate_count,
        "opportunity_count": opportunity_count,
        "matched_discovery_opportunity_count": opportunity_count if projected_from_discovery else None,
        "runtime_filter_reason_counts": {str(key): int(value) for key, value in dict(runtime_filter_reason_counts or {}).items()},
        "scoring_state_counts": dict(scoring_state_counts),
        "rejection_reason_counts": dict(rejection_reason_counts),
        "selection_memory": dict(selection_memory or {}),
        "top_candidates": top_candidates,
    }


def _project_runtime_rows_from_persisted(
    *,
    persisted_opportunities: list[dict[str, Any]],
    filtered_candidates: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    allowed_candidates_by_symbol: dict[str, set[str]] = {}
    for symbol, rows in filtered_candidates.items():
        candidate_ids = {_candidate_identity(dict(candidate)) for candidate in list(rows or []) if _candidate_identity(dict(candidate))}
        if candidate_ids:
            allowed_candidates_by_symbol[str(symbol).upper()] = candidate_ids

    projected_rows: list[dict[str, Any]] = []
    for row in persisted_opportunities:
        payload = row.get("candidate") if isinstance(row.get("candidate"), dict) else row
        candidate = dict(payload)
        symbol = str(candidate.get("underlying_symbol") or "").upper()
        if not symbol:
            continue
        allowed_ids = allowed_candidates_by_symbol.get(symbol)
        if not allowed_ids:
            continue
        candidate_id = _candidate_identity(candidate)
        if candidate_id not in allowed_ids:
            continue
        projected_rows.append(dict(row))

    projected_rows.sort(
        key=lambda row: (
            0 if row.get("selection_rank") not in (None, "") else 1,
            int(row.get("selection_rank") or 0),
            str(row.get("underlying_symbol") or ""),
        )
    )
    return projected_rows


def build_runtime_opportunity_payload(
    *,
    runtime: EntryRuntime,
    label: str,
    session_date: str,
    generated_at: str,
    cycle_id: str,
    strategy_run_id: str,
    row: dict[str, Any],
    source_row: dict[str, Any] | None,
) -> dict[str, Any]:
    candidate = dict(row.get("candidate")) if isinstance(row.get("candidate"), dict) else dict(row)
    eligibility = _runtime_opportunity_eligibility(runtime, row)
    blockers = _opportunity_blockers(candidate, eligibility=eligibility)
    policy_fields = resolve_runtime_policy_fields(
        profile=runtime.build_settings.scanner_profile,
        root_symbol=str(candidate.get("underlying_symbol") or ""),
    )
    return {
        "opportunity_id": build_runtime_opportunity_id(runtime, session_date=session_date, candidate=candidate),
        "pipeline_id": build_pipeline_id(label),
        "label": label,
        "market_date": session_date,
        "session_date": session_date,
        "cycle_id": cycle_id,
        "root_symbol": str(candidate.get("underlying_symbol") or ""),
        "trading_strategy_id": runtime.trading_strategy_id,
        "strategy_run_id": strategy_run_id,
        "config_hash": runtime.config_hash,
        "policy_ref": build_runtime_policy_ref(
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            market_date=session_date,
        ),
        "strategy_family": runtime.trade_structure,
        "profile": runtime.build_settings.scanner_profile,
        "style_profile": str(policy_fields["style_profile"]),
        "horizon_intent": str(policy_fields["horizon_intent"]),
        "product_class": str(policy_fields["product_class"]),
        "expiration_date": candidate.get("expiration_date"),
        "entity_type": "trading_strategy_signal_subject",
        "entity_key": (f"trading_strategy_signal_subject:{runtime.trading_strategy_id}:" f"{candidate.get('underlying_symbol')}"),
        "underlying_symbol": str(candidate.get("underlying_symbol") or ""),
        "side": row.get("side") or source_row.get("side") if source_row else None,
        "side_bias": row.get("side_bias") or source_row.get("side_bias") if source_row else None,
        "selection_state": str(row.get("selection_state") or "monitor"),
        "selection_rank": (None if row.get("selection_rank") in (None, "") else int(row["selection_rank"])),
        "state_reason": str(row.get("state_reason") or "selected_runtime_candidate"),
        "origin": "config_runtime",
        "eligibility": eligibility,
        "eligibility_state": eligibility,
        "promotion_score": _coerce_float(candidate.get("promotion_score")),
        "execution_score": _coerce_float(candidate.get("execution_score")),
        "confidence": _coerce_float(candidate.get("confidence")),
        "signal_state_ref": None,
        "lifecycle_state": ("ready" if str(row.get("selection_state") or "") == "promotable" else "candidate"),
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
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_structure": runtime.trade_structure,
            "entry_recipe_refs": list(runtime.entry_recipe_refs),
            "trigger_policy": dict(runtime.trigger_policy),
            "execution_mode": runtime.strategy.execution.mode,
            "approval_mode": runtime.strategy.execution.approval,
            "selection_state": row.get("selection_state"),
            "selection_rank": row.get("selection_rank"),
            "generated_at": generated_at,
            "last_present_at": generated_at,
            **candidate_evidence_metrics(candidate),
            **candidate_policy_context(candidate),
            "source_opportunity_id": None if source_row is None else source_row.get("opportunity_id"),
        },
        "execution_shape": _execution_shape(candidate),
        "risk_hints": risk_hints(candidate),
        "source_cycle_id": cycle_id,
        "source_candidate_id": None if source_row is None or source_row.get("candidate_id") in (None, "") else int(source_row["candidate_id"]),
        "source_selection_state": None if source_row is None else source_row.get("selection_state"),
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
    runtime_candidate_rows_by_owner: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] | None,
    persisted_opportunities: list[dict[str, Any]],
    job_run_id: str | None,
    top_promotable: int,
    top_monitor: int,
    selection_memory: dict[str, Any] | None = None,
    signal_cycle_context: dict[str, Any] | None = None,
    trigger_type: str = "discovery_run_cycle",
) -> dict[str, Any]:
    if not signal_store.strategy_runtime_schema_ready():
        return {
            "strategy_runs_upserted": 0,
            "runtime_opportunities_upserted": 0,
            "runtime_opportunities_expired": 0,
            "opportunities": [],
        }

    source_index = _opportunity_source_index(persisted_opportunities)
    strategy_runs_upserted = 0
    runtime_opportunities_upserted = 0
    runtime_opportunities_expired = 0
    scoped_opportunities: list[dict[str, Any]] = []

    for runtime in entry_runtimes:
        previous_promotable, previous_selection_memory = _read_previous_runtime_selection(
            signal_store=signal_store,
            runtime=runtime,
            session_date=session_date,
        )
        owner_candidates = None
        if runtime_candidate_rows_by_owner:
            owner_candidates = runtime_candidate_rows_by_owner.get(runtime_owner_key(runtime))
        source_candidates = (
            {str(symbol): [dict(candidate) for candidate in rows] for symbol, rows in owner_candidates.items()}
            if owner_candidates
            else {str(symbol): [dict(candidate) for candidate in rows] for symbol, rows in symbol_candidates.items()}
        )
        filtered_candidates, runtime_filter_reason_counts = filter_runtime_symbol_candidates(
            symbol_candidates=source_candidates,
            runtime=runtime,
        )
        selected_rows: list[dict[str, Any]]
        runtime_selection_memory: dict[str, Any]
        if owner_candidates is not None:
            selected_rows = _project_runtime_rows_from_persisted(
                persisted_opportunities=persisted_opportunities,
                filtered_candidates=filtered_candidates,
            )
            runtime_selection_memory = dict(selection_memory or {})
        else:
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
                signal_cycle_context=signal_cycle_context,
            )
            selected_rows = list(selection["opportunities"])
            runtime_selection_memory = dict(selection.get("selection_memory") or {})
        runtime_selection_summary = _build_runtime_selection_summary(
            runtime=runtime,
            filtered_candidates=filtered_candidates,
            runtime_filter_reason_counts=runtime_filter_reason_counts,
            selected_rows=selected_rows,
            selection_memory=runtime_selection_memory,
            projected_from_discovery=owner_candidates is not None,
        )
        strategy_run_id = build_trading_strategy_run_id(cycle_id, runtime.trading_strategy_id)
        signal_store.upsert_strategy_run(
            strategy_run_id=strategy_run_id,
            trading_strategy_id=runtime.trading_strategy_id,
            trigger_type=trigger_type,
            job_run_id=job_run_id,
            cycle_id=cycle_id,
            label=label,
            session_date=session_date,
            started_at=generated_at,
            completed_at=generated_at,
            status="completed",
            result={
                "candidate_symbol_count": len(filtered_candidates),
                "candidate_count": sum(len(list(rows or [])) for rows in filtered_candidates.values()),
                "opportunity_count": len(selected_rows),
                "selection_memory": runtime_selection_memory,
                "runtime_selection_summary": runtime_selection_summary,
            },
            config_hash=runtime.config_hash,
        )
        strategy_runs_upserted += 1

        active_runtime_opportunity_ids: list[str] = []
        for row in selected_rows:
            candidate = dict(row.get("candidate")) if isinstance(row.get("candidate"), dict) else dict(row)
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
                strategy_run_id=strategy_run_id,
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
            trading_strategy_id=runtime.trading_strategy_id,
            runtime_owned=True,
        )
        runtime_opportunities_expired += len(expired_rows)

    return {
        "strategy_runs_upserted": strategy_runs_upserted,
        "runtime_opportunities_upserted": runtime_opportunities_upserted,
        "runtime_opportunities_expired": runtime_opportunities_expired,
        "opportunities": scoped_opportunities,
    }


__all__ = [
    "build_trading_strategy_run_id",
    "build_runtime_opportunity_id",
    "build_runtime_opportunity_payload",
    "sync_entry_runtime_opportunities",
]

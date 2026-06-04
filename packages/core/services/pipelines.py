from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.adhoc import enqueue_ad_hoc_job
from core.jobs.registry import DISCOVERY_RUN_JOB_TYPE
from core.services.control_plane import get_control_state_snapshot
from core.services.discovery_runs.shared import session_date_for_generated_at
from core.services.execution import (
    list_session_execution_attempts,
    normalize_execution_policy,
)
from core.services.execution_portfolio import build_session_execution_portfolio
from core.services.live_pipelines import pipeline_uses_runtime_owned_opportunities
from core.services.discovery_run_health.tradeability import (
    build_tradeability_summary,
)
from core.services.live_runtime import (
    get_live_session,
    get_live_session_for_cycle,
    list_latest_live_sessions,
)
from core.services.risk_manager import (
    build_session_risk_snapshot,
    normalize_risk_policy,
)
from core.services.runtime_identity import (
    build_pipeline_id,
    build_live_run_scope_id,
    parse_pipeline_id,
)
from core.services.strategy_specs import resolve_strategy_spec
from core.storage.serializers import parse_datetime

MANUAL_PIPELINE_LABEL_PREFIX = "manual"
MANUAL_PIPELINE_PROFILE = "weekly"
MANUAL_PIPELINE_TOP = 5
MANUAL_PIPELINE_PER_SYMBOL_TOP = 3
MANUAL_PIPELINE_INTERVAL_SECONDS = 300
MANUAL_PIPELINE_QUOTE_CAPTURE_SECONDS = 20
MANUAL_PIPELINE_TRADE_CAPTURE_SECONDS = 10


def _parse_sort_value(value: str | None):
    normalized = value.strip() if isinstance(value, str) else None
    parsed = parse_datetime(normalized) if normalized else None
    return parsed or parse_datetime("1970-01-01T00:00:00Z")


def _latest_activity_timestamp(*values: str | None) -> str | None:
    best_value: str | None = None
    best_timestamp = None
    for value in values:
        normalized = value.strip() if isinstance(value, str) else None
        if not normalized:
            continue
        parsed = parse_datetime(normalized)
        if parsed is None:
            continue
        if best_timestamp is None or parsed > best_timestamp:
            best_timestamp = parsed
            best_value = normalized
    return best_value


def _as_utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _manual_job_run_id(job_key: str, scheduled_for: datetime) -> str:
    slot = scheduled_for.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{job_key}:{slot}"


def _normalize_manual_symbol(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("symbol is required")
    if not all(char.isalnum() or char in {".", "-", "_"} for char in normalized):
        raise ValueError("symbol contains unsupported characters")
    return normalized


def _resolve_pipeline_run_strategy(
    *,
    strategy_mode: str,
    strategy_family: str | None,
) -> tuple[str, str | None, str]:
    normalized_mode = str(strategy_mode or "auto").strip().lower()
    if normalized_mode not in {"auto", "manual"}:
        raise ValueError("strategy_mode must be auto or manual")
    if normalized_mode == "auto":
        return "auto", None, "auto"
    normalized_family = str(strategy_family or "").strip().lower()
    if not normalized_family:
        raise ValueError("strategy_family is required when strategy_mode is manual")
    if normalized_family == "auto":
        raise ValueError("strategy_family cannot be auto in manual mode")
    strategy_spec = resolve_strategy_spec(normalized_family)
    return (
        "manual",
        normalized_family,
        str(strategy_spec.scanner_strategy),
    )


def _manual_pipeline_label(
    *,
    symbol: str,
    strategy_mode: str,
    strategy_family: str | None,
) -> str:
    strategy_token = "auto" if strategy_mode == "auto" else str(strategy_family or "manual")
    return f"{MANUAL_PIPELINE_LABEL_PREFIX}_{symbol.lower()}_{strategy_token.lower()}".replace("-", "_").replace(".", "_")


def _discovery_run_event_sort_key(event: Mapping[str, Any]) -> tuple[Any, int]:
    timestamp = parse_datetime(str(event.get("generated_at") or ""))
    try:
        event_id = int(event.get("event_id") or 0)
    except (TypeError, ValueError):
        event_id = 0
    return (
        timestamp or parse_datetime("1970-01-01T00:00:00Z"),
        event_id,
    )


def _derive_runtime_status(
    *,
    latest_run: Mapping[str, Any] | None,
    latest_cycle: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any] | None = None,
) -> str:
    gap_active = bool((slot_health or {}).get("gap_active"))
    if latest_run is None:
        status = "healthy" if latest_cycle is not None else "idle"
        return "degraded" if gap_active and status != "running" else status

    status = str(latest_run.get("status") or "idle")
    if status == "running":
        return "running"
    if status == "failed":
        return "failed"
    if status == "queued":
        return "idle"
    if status == "skipped":
        return "degraded"
    if status == "succeeded":
        capture_status = str(latest_run.get("capture_status") or "")
        if capture_status in {"healthy", "idle"}:
            status = "healthy" if capture_status == "healthy" else "idle"
        else:
            status = "degraded"
        return "degraded" if gap_active and status != "running" else status
    status = "idle"
    return "degraded" if gap_active and status != "running" else status


def _session_risk_policy(latest_run: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest_run is None:
        return normalize_risk_policy(None)
    payload = latest_run.get("payload")
    if not isinstance(payload, Mapping):
        return normalize_risk_policy(None)
    raw_policy = payload.get("risk_policy")
    return normalize_risk_policy(raw_policy if isinstance(raw_policy, dict) else None)


def _session_execution_policy(latest_run: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest_run is None:
        return normalize_execution_policy(None)
    payload = latest_run.get("payload")
    if not isinstance(payload, Mapping):
        return normalize_execution_policy(None)
    return normalize_execution_policy(
        {
            "execution_policy": payload.get("execution_policy"),
            "risk_policy": payload.get("risk_policy"),
        }
    )


def _session_live_action_gate(
    latest_run: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if latest_run is None:
        return None
    gate = latest_run.get("live_action_gate")
    if not isinstance(gate, Mapping):
        return None
    return dict(gate)


def _latest_auto_execution(
    latest_run: Mapping[str, Any] | None,
    *,
    slot_runs: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    candidates: list[Mapping[str, Any]] = []
    if latest_run is not None:
        candidates.append(latest_run)
    if slot_runs is not None:
        candidates.extend(slot_runs)
    for run in candidates:
        summary = run.get("auto_execution_summary")
        if isinstance(summary, Mapping):
            return dict(summary)
    return None


def _tradeability_fields(
    *,
    latest_run: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any] | None,
    has_live_opportunities: bool,
    has_analysis_only_opportunities: bool = False,
) -> dict[str, Any]:
    tradeability = build_tradeability_summary(
        capture_status=None if latest_run is None else latest_run.get("capture_status"),
        live_action_gate=_session_live_action_gate(latest_run),
        slot_health=slot_health,
        has_live_opportunities=has_live_opportunities,
        has_analysis_only_opportunities=has_analysis_only_opportunities,
    )
    return {
        "tradeability": tradeability,
        "tradeability_state": tradeability["state"],
        "tradeability_reason": tradeability.get("reason_code"),
        "tradeability_message": tradeability.get("message"),
    }


def _reconciliation_snapshot(portfolio: Mapping[str, Any]) -> dict[str, Any]:
    positions = portfolio.get("positions")
    if not isinstance(positions, list) or not positions:
        return {
            "status": "clear",
            "note": "No open positions require reconciliation.",
        }

    open_positions = [
        position
        for position in positions
        if isinstance(position, Mapping) and str(position.get("position_status") or "") in {"open", "partial_close"}
    ]
    if not open_positions:
        return {
            "status": "clear",
            "note": "No open positions require reconciliation.",
        }

    mismatch_positions = [position for position in open_positions if str(position.get("reconciliation_status") or "") == "mismatch"]
    if mismatch_positions:
        return {
            "status": "mismatch",
            "note": f"{len(mismatch_positions)} open position(s) have broker reconciliation mismatches.",
        }

    pending_positions = [position for position in open_positions if not position.get("last_reconciled_at")]
    if pending_positions:
        return {
            "status": "pending",
            "note": f"{len(pending_positions)} open position(s) are waiting for broker reconciliation.",
        }
    return {
        "status": "matched",
        "note": "Open positions match the broker inventory snapshot.",
    }


def _discovery_run_schema_ready(discovery_store: Any) -> bool:
    if hasattr(discovery_store, "schema_ready"):
        return bool(discovery_store.schema_ready())
    return bool(discovery_store.pipeline_schema_ready())


def _candidate_counts_by_cycle_id(
    *,
    discovery_store: Any,
    signal_store: Any,
    cycle_ids: list[str],
    runtime_owned: bool = False,
) -> dict[str, dict[str, int]]:
    if not cycle_ids:
        return {}
    if (
        signal_store is not None
        and hasattr(signal_store, "schema_ready")
        and signal_store.schema_ready()
        and hasattr(signal_store, "count_active_cycle_opportunities_by_cycle_ids")
    ):
        return signal_store.count_active_cycle_opportunities_by_cycle_ids(
            cycle_ids,
            runtime_owned=runtime_owned,
        )
    if runtime_owned:
        return {}
    return discovery_store.count_cycle_candidates_by_cycle_ids(cycle_ids)


def _build_cycle_payload(
    *,
    pipeline_id: str,
    cycle: Mapping[str, Any],
    candidate_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    counts = dict(candidate_counts or {})
    summary = {
        "candidate_count": int(counts.get("candidate_count") or 0),
        "promotable_count": int(counts.get("promotable") or 0),
        "monitor_count": int(counts.get("monitor") or 0),
        "failure_count": len(cycle.get("failures") or []),
        "event_count": 0,
    }
    return {
        **dict(cycle),
        "pipeline_id": pipeline_id,
        "market_date": str(cycle.get("market_date") or cycle.get("session_date") or ""),
        "strategy_mode": cycle.get("strategy_mode") or cycle.get("strategy"),
        "legacy_profile": cycle.get("legacy_profile") or cycle.get("profile"),
        "summary": summary,
        "summary_json": summary,
    }


def _serialize_pipeline_summary(
    *,
    pipeline: Mapping[str, Any],
    latest_cycle: Mapping[str, Any],
    latest_run: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any],
    candidate_counts: dict[str, int],
    alert_count: int,
    resolved_ranking_policy: Mapping[str, Any] | None = None,
    ranking_policy_gate_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    updated_at = _latest_activity_timestamp(
        None if latest_run is None else str(latest_run.get("finished_at") or ""),
        None if latest_run is None else str(latest_run.get("heartbeat_at") or ""),
        None if latest_run is None else str(latest_run.get("started_at") or ""),
        None if latest_run is None else str(latest_run.get("slot_at") or latest_run.get("scheduled_for") or ""),
        str(latest_cycle.get("generated_at") or ""),
        str(pipeline.get("updated_at") or ""),
    )
    tradeability_fields = _tradeability_fields(
        latest_run=latest_run,
        slot_health=slot_health,
        has_live_opportunities=bool(int(candidate_counts.get("promotable") or 0) or int(candidate_counts.get("monitor") or 0)),
    )
    latest_auto_execution = _latest_auto_execution(latest_run)
    return {
        "pipeline_id": str(pipeline["pipeline_id"]),
        "label": str(pipeline["label"]),
        "name": pipeline.get("name"),
        "status": _derive_runtime_status(
            latest_run=latest_run,
            latest_cycle=latest_cycle,
            slot_health=slot_health,
        ),
        "latest_market_date": str(latest_cycle["market_date"]),
        "latest_slot_at": slot_health.get("latest_slot_at") or (None if latest_run is None else latest_run.get("slot_at")),
        "latest_slot_status": slot_health.get("latest_slot_status") or (None if latest_run is None else latest_run.get("status")),
        "latest_capture_status": None if latest_run is None else latest_run.get("capture_status"),
        "latest_auto_execution": latest_auto_execution,
        "latest_auto_execution_status": None if latest_auto_execution is None else latest_auto_execution.get("status"),
        "promotable_count": int(candidate_counts.get("promotable") or 0),
        "monitor_count": int(candidate_counts.get("monitor") or 0),
        "alert_count": int(alert_count or 0),
        "live_action_gate": _session_live_action_gate(latest_run),
        "gap_active": bool(slot_health.get("gap_active")),
        "recovery_state": slot_health.get("recovery_state"),
        "missed_slot_count": int(slot_health.get("missed_slot_count") or 0),
        "unrecoverable_slot_count": int(slot_health.get("unrecoverable_slot_count") or 0),
        "latest_fresh_slot_at": slot_health.get("latest_fresh_slot_at"),
        "latest_resume_slot_at": slot_health.get("latest_resume_slot_at"),
        **tradeability_fields,
        "updated_at": updated_at,
        "style_profile": pipeline.get("style_profile"),
        "horizon_intent": pipeline.get("default_horizon_intent"),
        "product_scope": pipeline.get("product_scope_json"),
        "policy": pipeline.get("policy_json"),
        "session_schedule": dict(pipeline.get("session_schedule") or {}),
        "resolved_ranking_policy": (dict(resolved_ranking_policy or {}) if isinstance(resolved_ranking_policy, Mapping) else None),
        "ranking_policy_gate_summary": (dict(ranking_policy_gate_summary or {}) if isinstance(ranking_policy_gate_summary, Mapping) else None),
    }


@with_storage()
def list_pipelines(
    *,
    db_target: str,
    limit: int = 100,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    alert_store = storage.alerts
    live_sessions = list_latest_live_sessions(
        storage=storage,
        market_date=market_date,
        limit=max(limit * 5, limit),
    )
    alert_counts = alert_store.count_alert_events_by_session_keys([(str(session["market_date"]), str(session["label"])) for session in live_sessions])

    summaries: list[dict[str, Any]] = []
    for session in live_sessions:
        summaries.append(
            _serialize_pipeline_summary(
                pipeline=session["pipeline"],
                latest_cycle=session["cycle"],
                latest_run=session.get("latest_run"),
                slot_health=dict(session.get("slot_health") or {}),
                candidate_counts=dict(session.get("candidate_counts") or {}),
                alert_count=int(alert_counts.get((str(session["market_date"]), str(session["label"]))) or 0),
                resolved_ranking_policy=session.get("resolved_ranking_policy"),
                ranking_policy_gate_summary=session.get("ranking_policy_gate_summary"),
            )
        )

    summaries.sort(
        key=lambda row: _parse_sort_value(None if not row.get("updated_at") else str(row["updated_at"])),
        reverse=True,
    )
    return {"pipelines": summaries[:limit]}


@with_storage()
def start_pipeline_run(
    *,
    db_target: str,
    symbol: str,
    strategy_mode: str = "auto",
    strategy_family: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    del db_target
    job_store = storage.jobs
    if not job_store.schema_ready():
        raise RuntimeError("Job schema is unavailable.")

    normalized_symbol = _normalize_manual_symbol(symbol)
    (
        resolved_strategy_mode,
        resolved_strategy_family,
        scanner_strategy,
    ) = _resolve_pipeline_run_strategy(
        strategy_mode=strategy_mode,
        strategy_family=strategy_family,
    )
    label = _manual_pipeline_label(
        symbol=normalized_symbol,
        strategy_mode=resolved_strategy_mode,
        strategy_family=resolved_strategy_family,
    )
    pipeline_id = build_pipeline_id(label)
    scheduled_for = datetime.now(UTC)
    scheduled_for_iso = _as_utc_iso(scheduled_for)
    session_date = session_date_for_generated_at(scheduled_for_iso)
    session_id = build_live_run_scope_id(label, session_date)
    job_key = f"discovery_run:adhoc:{label}"
    job_run_id = _manual_job_run_id(job_key, scheduled_for)
    payload = {
        "job_key": job_key,
        "job_type": DISCOVERY_RUN_JOB_TYPE,
        "label": label,
        "pipeline_id": pipeline_id,
        "session_id": session_id,
        "session_date": session_date,
        "scheduled_for": scheduled_for_iso,
        "slot_at": scheduled_for_iso,
        "symbols": normalized_symbol,
        "strategy": scanner_strategy,
        "strategy_mode": resolved_strategy_mode,
        "strategy_family": resolved_strategy_family,
        "profile": MANUAL_PIPELINE_PROFILE,
        "greeks_source": "auto",
        "top": MANUAL_PIPELINE_TOP,
        "per_symbol_top": MANUAL_PIPELINE_PER_SYMBOL_TOP,
        "interval_seconds": MANUAL_PIPELINE_INTERVAL_SECONDS,
        "quote_capture_seconds": MANUAL_PIPELINE_QUOTE_CAPTURE_SECONDS,
        "trade_capture_seconds": MANUAL_PIPELINE_TRADE_CAPTURE_SECONDS,
        "allow_off_hours": True,
        "session_start_offset_minutes": 0,
        "session_end_offset_minutes": 0,
        "trading_strategy_enabled": False,
        "singleton_scope": f"manual:{label}",
        "manual_run": True,
    }
    job_run, _created = job_store.create_job_run(
        job_run_id=job_run_id,
        job_key=job_key,
        arq_job_id=job_run_id,
        job_type=DISCOVERY_RUN_JOB_TYPE,
        status="queued",
        scheduled_for=scheduled_for,
        session_id=session_id,
        slot_at=scheduled_for,
        payload=payload,
    )
    try:
        enqueued = enqueue_ad_hoc_job(
            job_type=DISCOVERY_RUN_JOB_TYPE,
            job_key=job_key,
            job_run_id=job_run_id,
            arq_job_id=job_run_id,
            payload=payload,
        )
    except Exception as exc:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=datetime.now(UTC),
            error_text=str(exc),
        )
        raise RuntimeError(f"Pipeline run queueing failed: {exc}") from exc
    if enqueued is None:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=datetime.now(UTC),
            error_text="Discovery run was not enqueued.",
        )
        raise RuntimeError("Pipeline run was not enqueued.")
    return {
        "job_run_id": str(job_run["job_run_id"]),
        "job_key": str(job_run["job_key"]),
        "pipeline_id": pipeline_id,
        "label": label,
        "session_id": session_id,
        "scheduled_for": scheduled_for_iso,
        "status": str(job_run["status"]),
        "symbol": normalized_symbol,
        "strategy_mode": resolved_strategy_mode,
        "strategy_family": resolved_strategy_family,
        "profile": MANUAL_PIPELINE_PROFILE,
    }


@with_storage()
def get_pipeline_detail(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None,
    cycle_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    if cycle_id is None:
        live_session = get_live_session(
            storage=storage,
            pipeline_id=pipeline_id,
            market_date=market_date,
        )
    else:
        live_session = get_live_session_for_cycle(
            storage=storage,
            cycle_id=cycle_id,
        )
        resolved_pipeline_id = str(live_session.get("pipeline", {}).get("pipeline_id") or live_session.get("cycle", {}).get("pipeline_id") or "")
        if resolved_pipeline_id != pipeline_id:
            raise ValueError(f"cycle_id={cycle_id} does not belong to pipeline_id={pipeline_id}")
        resolved_market_date = str(live_session.get("market_date") or "")
        if market_date is not None and resolved_market_date != market_date:
            raise ValueError(f"cycle_id={cycle_id} does not belong to market_date={market_date}")
    return _build_pipeline_detail_payload(
        db_target=db_target,
        storage=storage,
        live_session=live_session,
        selected_cycle_id=cycle_id,
    )


def _build_pipeline_detail_payload(
    *,
    db_target: str,
    storage: Any,
    live_session: Mapping[str, Any],
    selected_cycle_id: str | None = None,
) -> dict[str, Any]:
    discovery_store = storage.discovery
    alert_store = storage.alerts
    execution_store = storage.execution
    risk_store = getattr(storage, "risk", None)
    signal_store = storage.signals

    pipeline = dict(live_session["pipeline"])
    latest_cycle = dict(live_session["cycle"])
    pipeline_id = str(latest_cycle.get("pipeline_id") or pipeline.get("pipeline_id") or "") or build_pipeline_id(str(latest_cycle["label"]))
    label = str(live_session["label"])
    resolved_market_date = str(live_session["market_date"])
    legacy_session_id = str(live_session["session_id"])
    latest_run = live_session.get("latest_run")
    slot_runs = [dict(row) for row in list(live_session.get("slot_runs") or [])]
    all_opportunities = [dict(row) for row in list(live_session.get("opportunities") or [])]
    live_opportunities = [dict(row) for row in list(live_session.get("live_opportunities") or [])]
    analysis_only_opportunities = [dict(row) for row in list(live_session.get("analysis_only_opportunities") or [])]
    selection_counts = dict(live_session.get("selection_counts") or {})
    candidate_counts = dict(live_session.get("candidate_counts") or {})
    strategy_sync_summary = dict(live_session.get("strategy_sync_summary") or {})
    resolved_ranking_policy = dict(live_session.get("resolved_ranking_policy") or {})
    ranking_policy_gate_summary = dict(live_session.get("ranking_policy_gate_summary") or {})
    current_cycle = {
        **_build_cycle_payload(
            pipeline_id=pipeline_id,
            cycle=latest_cycle,
            candidate_counts=candidate_counts,
        ),
        "opportunities": all_opportunities,
        "live_opportunities": live_opportunities,
        "analysis_only_opportunities": analysis_only_opportunities,
        "selection_counts": selection_counts,
        "promotable_count": int(selection_counts.get("promotable") or 0),
        "monitor_count": int(selection_counts.get("monitor") or 0),
        "legacy_session_id": legacy_session_id,
        "strategy_sync_summary": strategy_sync_summary,
        "resolved_ranking_policy": resolved_ranking_policy,
        "ranking_policy_gate_summary": ranking_policy_gate_summary,
        "raw_candidate_summary": dict(live_session.get("raw_candidate_summary") or {}),
    }

    alerts = [
        dict(alert)
        for alert in alert_store.list_alert_events(
            session_date=resolved_market_date,
            label=str(latest_cycle["label"]),
            limit=200,
        )
    ]
    if selected_cycle_id is None:
        events = [
            dict(event)
            for event in discovery_store.list_events(
                label=label,
                session_date=resolved_market_date,
                limit=400,
                ascending=False,
            )
        ]
    else:
        events = [dict(event) for event in list(live_session.get("cycle_events") or [])]
    events.sort(key=_discovery_run_event_sort_key)

    updated_at = _latest_activity_timestamp(
        None if latest_run is None else str(latest_run.get("finished_at") or ""),
        None if latest_run is None else str(latest_run.get("heartbeat_at") or ""),
        None if latest_run is None else str(latest_run.get("started_at") or ""),
        None if latest_run is None else str(latest_run.get("slot_at") or latest_run.get("scheduled_for") or ""),
        str(current_cycle.get("generated_at") or ""),
        str(pipeline.get("updated_at") or ""),
    )
    executions = list_session_execution_attempts(
        db_target=db_target,
        session_id=legacy_session_id,
        limit=50,
        execution_store=execution_store,
        storage=storage,
    )
    portfolio = build_session_execution_portfolio(
        db_target=db_target,
        session_id=legacy_session_id,
        executions=executions,
        execution_store=execution_store,
        storage=storage,
    )
    risk_snapshot = build_session_risk_snapshot(
        execution_store=execution_store,
        session_id=legacy_session_id,
        risk_policy=_session_risk_policy(latest_run),
        execution_policy=_session_execution_policy(latest_run),
    )
    risk_decisions = (
        []
        if risk_store is None or not risk_store.schema_ready()
        else [
            dict(row)
            for row in risk_store.list_risk_decisions(
                session_id=legacy_session_id,
                limit=100,
            )
        ]
    )
    reconciliation_snapshot = _reconciliation_snapshot(portfolio)
    control_snapshot = get_control_state_snapshot(storage=storage)
    live_action_gate = _session_live_action_gate(latest_run)
    slot_health = dict(live_session.get("slot_health") or {})
    recovery_slots = [dict(row) for row in list(live_session.get("recovery_slots") or [])]
    runtime_owned = pipeline_uses_runtime_owned_opportunities(
        pipeline,
        latest_run,
    )
    cycle_rows = [dict(row) for row in discovery_store.list_cycles(label, limit=50)]
    cycle_counts_by_cycle_id = _candidate_counts_by_cycle_id(
        discovery_store=discovery_store,
        signal_store=signal_store,
        cycle_ids=[str(row["cycle_id"]) for row in cycle_rows],
        runtime_owned=runtime_owned,
    )
    cycles = [
        {
            **_build_cycle_payload(
                pipeline_id=pipeline_id,
                cycle=row,
                candidate_counts=cycle_counts_by_cycle_id.get(str(row["cycle_id"])),
            ),
            "legacy_session_id": build_live_run_scope_id(
                str(row["label"]),
                str(row.get("session_date") or row.get("market_date")),
            ),
        }
        for row in cycle_rows
    ]
    tradeability_fields = _tradeability_fields(
        latest_run=latest_run,
        slot_health=slot_health,
        has_live_opportunities=bool(live_opportunities),
        has_analysis_only_opportunities=bool(analysis_only_opportunities),
    )
    latest_auto_execution = _latest_auto_execution(latest_run, slot_runs=slot_runs)

    return {
        "pipeline_id": pipeline_id,
        "market_date": resolved_market_date,
        "label": str(latest_cycle["label"]),
        "status": _derive_runtime_status(
            latest_run=latest_run,
            latest_cycle=current_cycle,
            slot_health=slot_health,
        ),
        "updated_at": updated_at,
        "risk_status": risk_snapshot["status"],
        "risk_note": risk_snapshot.get("note"),
        "reconciliation_status": reconciliation_snapshot["status"],
        "reconciliation_note": reconciliation_snapshot.get("note"),
        "latest_slot": latest_run,
        "slot_health": slot_health,
        "recovery_slots": recovery_slots,
        "live_action_gate": live_action_gate,
        "latest_auto_execution": latest_auto_execution,
        "quote_capture": dict(live_session.get("quote_capture") or {}),
        "trade_capture": dict(live_session.get("trade_capture") or {}),
        "uoa_summary": dict(live_session.get("uoa_summary") or {}),
        "uoa_quote_summary": dict(live_session.get("uoa_quote_summary") or {}),
        "uoa_decisions": dict(live_session.get("uoa_decisions") or {}),
        "resolved_ranking_policy": resolved_ranking_policy,
        "ranking_policy_gate_summary": ranking_policy_gate_summary,
        **tradeability_fields,
        "session_schedule": dict(live_session.get("session_schedule") or {}),
        "pipeline": dict(pipeline),
        "current_cycle": current_cycle,
        "raw_candidate_summary": dict(live_session.get("raw_candidate_summary") or {}),
        "strategy_sync_summary": strategy_sync_summary,
        "cycles": cycles,
        "opportunities": live_opportunities,
        "analysis_only_opportunities": analysis_only_opportunities,
        "selection_counts": selection_counts,
        "slot_runs": slot_runs,
        "alerts": alerts,
        "events": events,
        "executions": executions,
        "risk_decisions": risk_decisions,
        "control": control_snapshot,
        "portfolio": portfolio,
    }


@with_storage()
def list_pipeline_cycles(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None = None,
    limit: int = 100,
    storage: Any | None = None,
) -> dict[str, Any]:
    discovery_store = storage.discovery
    if not _discovery_run_schema_ready(discovery_store):
        return {"cycles": []}
    parsed = parse_pipeline_id(pipeline_id)
    if parsed is None:
        return {"cycles": []}
    signal_store = storage.signals
    rows = [
        dict(row)
        for row in discovery_store.list_cycles(
            parsed["label"],
            session_date=market_date,
            limit=limit,
        )
    ]
    cycle_ids = [str(row["cycle_id"]) for row in rows]
    candidate_counts_by_cycle_id = _candidate_counts_by_cycle_id(
        discovery_store=discovery_store,
        signal_store=signal_store,
        cycle_ids=cycle_ids,
    )
    return {
        "cycles": [
            _build_cycle_payload(
                pipeline_id=pipeline_id,
                cycle=row,
                candidate_counts=candidate_counts_by_cycle_id.get(str(row["cycle_id"])),
            )
            for row in rows
        ]
    }

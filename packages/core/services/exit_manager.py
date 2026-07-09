from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.services.admission_lifecycle import admission_allows_attempt
from core.services.execution_intents.position_close import issue_close_execution_intent
from core.services.trading_strategy_runtime import resolve_management_runtimes
from core.services.trading_engine.exit_runtime import (
    ExitEngine,
    attach_close_decision_intent,
    blocked_close_decision_projection,
    build_exit_run_ref,
    build_position_exit_snapshot,
    close_decision_lifecycle,
    close_decision_projection,
    persist_close_decision,
    persist_close_intent_admission,
)
from core.services.trading_engine.portfolio_runtime import (
    PortfolioEngine,
    build_position_snapshot,
)
from core.services.trading_engine.risk_runtime import (
    evaluate_close_admission,
)
from core.services.execution_portfolio import refresh_session_position_marks
from core.services.positions import enrich_position_row
from core.services.risk.admission import CLOSE_RECONCILIATION_MAX_AGE_SECONDS
from core.value_coercion import as_text, utc_iso, utc_now_iso
from core.storage.serializers import parse_datetime

BROKER_SYNC_KEY = "broker_sync:alpaca"
BROKER_SYNC_IN_FLIGHT_STATUSES = {"queued", "running", "leased"}


def _latest_broker_sync_run(storage: Any) -> dict[str, Any] | None:
    job_store = getattr(storage, "jobs", None)
    if job_store is None:
        return None
    rows = job_store.list_job_runs(job_key=BROKER_SYNC_KEY, limit=1)
    return dict(rows[0]) if rows else None


def _broker_sync_snapshot(storage: Any, *, now: datetime) -> dict[str, Any]:
    latest_run = _latest_broker_sync_run(storage)
    latest_run_status = None if latest_run is None else str(latest_run.get("status") or "").lower()
    latest_run_started_at = None if latest_run is None else parse_datetime(latest_run.get("started_at"))
    latest_run_started_at_text = utc_iso(latest_run_started_at)
    broker_sync_in_flight = latest_run_status in BROKER_SYNC_IN_FLIGHT_STATUSES
    snapshot: dict[str, Any] = {
        "sync_key": BROKER_SYNC_KEY,
        "status": "unknown",
        "reason": None,
        "updated_at": None,
        "age_seconds": None,
        "max_age_seconds": CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
        "state_status": None,
        "job_run_id": None if latest_run is None else latest_run.get("job_run_id"),
        "job_status": latest_run_status,
        "job_started_at": latest_run_started_at_text,
        "state_covers_in_flight_run": False,
    }

    broker_store = getattr(storage, "broker", None)
    if broker_store is None or not broker_store.schema_ready():
        snapshot["status"] = "in_flight" if broker_sync_in_flight else "missing"
        snapshot["reason"] = "broker_sync_in_flight" if broker_sync_in_flight else "broker_sync_schema_unavailable"
        return snapshot
    state = broker_store.get_sync_state(BROKER_SYNC_KEY)
    if not isinstance(state, Mapping):
        snapshot["status"] = "in_flight" if broker_sync_in_flight else "missing"
        snapshot["reason"] = "broker_sync_in_flight" if broker_sync_in_flight else "broker_sync_missing"
        return snapshot

    updated_at = parse_datetime(as_text(state.get("updated_at")))
    state_status = str(state.get("status") or "unknown").lower()
    age_seconds = None
    if updated_at is not None:
        age_seconds = max((now - updated_at.astimezone(UTC)).total_seconds(), 0.0)
    state_covers_in_flight_run = (
        broker_sync_in_flight
        and updated_at is not None
        and latest_run_started_at is not None
        and updated_at.astimezone(UTC) >= latest_run_started_at.astimezone(UTC)
    )
    snapshot.update(
        {
            "updated_at": utc_iso(updated_at),
            "age_seconds": None if age_seconds is None else round(age_seconds, 1),
            "state_status": state_status,
            "summary": dict(state.get("summary") or {}),
            "state_covers_in_flight_run": state_covers_in_flight_run,
        }
    )
    if broker_sync_in_flight and not state_covers_in_flight_run:
        snapshot["status"] = "in_flight"
        snapshot["reason"] = "broker_sync_in_flight"
        return snapshot
    if state_status != "healthy":
        snapshot["status"] = "unhealthy"
        snapshot["reason"] = "broker_sync_unhealthy"
        return snapshot
    if age_seconds is None:
        snapshot["status"] = "missing"
        snapshot["reason"] = "broker_sync_updated_at_missing"
        return snapshot
    if age_seconds > CLOSE_RECONCILIATION_MAX_AGE_SECONDS:
        snapshot["status"] = "stale"
        snapshot["reason"] = "broker_sync_stale"
        return snapshot
    snapshot["status"] = "current"
    return snapshot


def _refresh_open_position_marks(*, db_target: str, session_ids: list[str], storage: Any | None = None) -> None:
    refresh_session_position_marks(
        db_target=db_target,
        session_ids=session_ids,
        storage=storage,
    )


def _mark_exit_evaluated(execution_store: Any, *, position_id: str, reason: str) -> None:
    execution_store.update_position(
        position_id=position_id,
        last_exit_evaluated_at=utc_now_iso(),
        last_exit_reason=reason,
        updated_at=utc_now_iso(),
    )


def _replace_with_blocked_projection(
    row: dict[str, Any],
    *,
    position: Mapping[str, Any],
    reason: str,
    decision_source: str,
    exit_run_id: str,
    decided_at: str,
    close_admission: Mapping[str, Any] | None = None,
) -> None:
    row.clear()
    row.update(
        blocked_close_decision_projection(
            position=position,
            reason=reason,
            decision_source=decision_source,
            exit_run_id=exit_run_id,
            decided_at=decided_at,
            close_admission=close_admission,
        )
    )


@with_storage()
def run_trading_strategy_manage(
    *,
    db_target: str,
    trading_strategy_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    open_attempt_guard: dict[str, Any] = {
        "status": "skipped",
        "reason": "not_run",
    }
    if not execution_store.portfolio_schema_ready():
        return {
            "status": "skipped",
            "reason": "positions_schema_unavailable",
            "open_attempt_guard": open_attempt_guard,
        }

    now = datetime.now(UTC)
    broker_sync = _broker_sync_snapshot(storage, now=now)
    management_runtimes = tuple(resolve_management_runtimes())
    engine_facts = getattr(storage, "engine_facts", None)
    portfolio_engine = PortfolioEngine(
        execution_store=execution_store,
    )
    exit_engine = ExitEngine(
        execution_store=execution_store,
        engine_facts=engine_facts,
        now=now,
        management_runtimes=management_runtimes,
        broker_sync=broker_sync,
    )
    exit_run_ref = build_exit_run_ref(
        trading_strategy_id=trading_strategy_id,
        now=now,
    )
    open_position_snapshots = portfolio_engine.list_open_positions(
        trading_strategy_id=trading_strategy_id,
        limit=200,
    )
    open_positions = [dict(position.payload) for position in open_position_snapshots]
    if not open_positions:
        return {
            "status": "degraded" if open_attempt_guard.get("status") == "degraded" else "ok",
            "exit_engine": {
                "run_id": exit_run_ref.run_id,
            },
            "position_count": 0,
            "evaluated": 0,
            "created_intents": 0,
            "skipped": 0,
            "failure_count": 0,
            "open_attempt_guard": open_attempt_guard,
            "broker_sync": broker_sync,
        }
    if broker_sync.get("status") != "current":
        broker_reason = str(broker_sync.get("reason") or "broker_sync_not_current")
        decided_at = utc_iso(now)
        broker_decisions = []
        for position in open_positions[:25]:
            exit_snapshot = build_position_exit_snapshot(
                position=position,
                now=now,
                execution_store=execution_store,
                engine_facts=engine_facts,
                broker_sync=broker_sync,
            )
            projection = blocked_close_decision_projection(
                position=position,
                reason=broker_reason,
                decision_source="broker_sync",
                exit_run_id=exit_run_ref.run_id,
                decided_at=decided_at,
                exit_snapshot=exit_snapshot,
            )
            persist_close_decision(engine_facts, position=position, close_decision=projection["close_decision"])
            broker_decisions.append(projection)
        return {
            "status": "skipped",
            "reason": broker_reason,
            "exit_engine": {
                "run_id": exit_run_ref.run_id,
            },
            "position_count": len(open_positions),
            "evaluated": 0,
            "created_intents": 0,
            "skipped": len(open_positions),
            "failure_count": 0,
            "decisions": broker_decisions,
            "failures": [],
            "open_attempt_guard": open_attempt_guard,
            "broker_sync": broker_sync,
        }

    try:
        from core.services.execution.guard import run_open_execution_guard

        open_attempt_guard = run_open_execution_guard(
            db_target=db_target,
            storage=storage,
        )
    except Exception as exc:
        open_attempt_guard = {
            "status": "degraded",
            "reason": "guard_error",
            "error": str(exc),
        }

    _refresh_open_position_marks(
        db_target=db_target,
        session_ids=sorted({str(position["session_id"]) for position in open_positions if position.get("session_id")}),
        storage=storage,
    )
    refreshed_position_snapshots = portfolio_engine.list_open_positions(
        trading_strategy_id=trading_strategy_id,
        limit=200,
    )
    refreshed_positions = [dict(position.payload) for position in refreshed_position_snapshots]

    evaluated = 0
    created_intents = 0
    skipped = 0
    failures: list[dict[str, str]] = []
    decisions: list[dict[str, Any]] = []
    now_iso = utc_iso(now)
    for position_snapshot in refreshed_position_snapshots:
        position = dict(position_snapshot.payload)
        position_id = str(position_snapshot.position_id)
        latest_position = execution_store.get_position(position_id)
        if latest_position is not None:
            position = enrich_position_row(dict(latest_position))
            position_snapshot = build_position_snapshot(position)

        close_result = exit_engine.evaluate_close(
            run_ref=exit_run_ref,
            position=position_snapshot,
        )
        decision = dict(close_result.payload.get("decision") or {})
        decision_source = as_text(close_result.payload.get("decision_source")) or "exit_engine"
        management_runtime = close_result.payload.get("management_runtime")
        close_decision = dict(close_result.payload.get("close_decision") or decision.get("close_decision") or {})
        evaluated += 1
        _mark_exit_evaluated(execution_store, position_id=position_id, reason=str(decision["reason"]))
        decisions.append(
            close_decision_projection(
                position_id=position_id,
                reason=str(decision["reason"]),
                decision_source=decision_source,
                should_close=bool(decision["should_close"]),
                exit_run_id=exit_run_ref.run_id,
                close_decision=close_decision,
            )
        )
        if not decision["should_close"]:
            persist_close_decision(engine_facts, position=position, close_decision=close_decision)
            skipped += 1
            continue

        latest_position = execution_store.get_position(position_id)
        if latest_position is not None:
            position = enrich_position_row(dict(latest_position))
            position_snapshot = build_position_snapshot(position)

        close_admission = evaluate_close_admission(
            execution_store,
            position={**position, "close_decision": close_decision},
            now=now,
            extra_blocker=None if management_runtime is not None else "management_runtime_required_for_close_intent",
        )
        if not admission_allows_attempt(close_admission):
            close_block_reason = str((close_admission.get("blockers") or close_admission.get("reason_codes") or ["close_admission_blocked"])[0])
            evidence = dict(close_admission.get("evidence") or {})
            evidence["selected_close_decision"] = dict(close_decision)
            close_admission["evidence"] = evidence
            skipped += 1
            _mark_exit_evaluated(execution_store, position_id=position_id, reason=close_block_reason)
            _replace_with_blocked_projection(
                decisions[-1],
                position=position,
                reason=close_block_reason,
                decision_source="close_admission",
                exit_run_id=exit_run_ref.run_id,
                decided_at=now_iso,
                close_admission=close_admission,
            )
            persist_close_decision(engine_facts, position=position, close_decision=decisions[-1]["close_decision"])
            continue

        try:
            close_decision = close_decision_lifecycle(
                position=position,
                decision={**decision, "close_decision": close_decision},
                decision_source=decision_source,
                decided_at=now_iso,
                close_admission=close_admission,
            )
            decisions[-1].update(close_decision_projection(
                position_id=position_id,
                reason=str(decision["reason"]),
                decision_source=decision_source,
                should_close=True,
                exit_run_id=exit_run_ref.run_id,
                close_decision=close_decision,
            ))
            persist_close_decision(engine_facts, position=position, close_decision=close_decision)
            intent = issue_close_execution_intent(
                execution_store,
                position=position,
                runtime=management_runtime,
                decision=decision,
                close_decision=close_decision,
                close_admission=close_admission,
            )
            close_decision["execution_intent_id"] = intent.get("execution_intent_id")
            decisions[-1]["close_decision"] = close_decision
            attach_close_decision_intent(
                engine_facts,
                close_decision_id=str(close_decision["close_decision_id"]),
                execution_intent_id=str(intent["execution_intent_id"]),
            )
            persist_close_intent_admission(
                engine_facts,
                intent=intent,
                close_decision=close_decision,
                close_admission=close_admission,
                runtime=management_runtime,
                position=position,
            )
            created_intents += 1
        except Exception as exc:
            failures.append(
                {
                    "position_id": position_id,
                    "error": str(exc),
                }
            )

    return {
        "status": "degraded" if failures or open_attempt_guard.get("status") == "degraded" else "ok",
        "exit_engine": {
            "run_id": exit_run_ref.run_id,
        },
        "position_count": len(refreshed_positions),
        "evaluated": evaluated,
        "created_intents": created_intents,
        "skipped": skipped,
        "failure_count": len(failures),
        "decisions": decisions[:25],
        "failures": failures[:25],
        "open_attempt_guard": open_attempt_guard,
        "broker_sync": broker_sync,
    }

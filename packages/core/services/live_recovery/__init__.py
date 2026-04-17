from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.jobs.orchestration import NEW_YORK
from core.services.live_slot_updates import update_live_session_slot_from_row
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)

from .capture_targets import (
    build_capture_target_rows_for_candidates,
    build_slot_details_from_cycle_result,
    refresh_execution_capture_targets,
    refresh_live_session_capture_targets,
    refresh_recovery_session_capture_targets,
)
from .continuity import (
    _continuity_rows_for_slot,
    _coverage_summary_for_slot,
    _slot_should_be_marked_missed,
)
from .control import _set_recovery_control_mode
from .shared import (
    CAPTURE_OWNER_EXECUTION_ATTEMPT,
    CAPTURE_OWNER_LIVE_SESSION,
    CAPTURE_OWNER_RECOVERY_SESSION,
    CAPTURE_OWNER_SESSION_POSITION,
    CAPTURE_TARGET_REASON_MONITOR,
    CAPTURE_TARGET_REASON_OPEN_POSITION,
    CAPTURE_TARGET_REASON_PENDING_EXECUTION,
    CAPTURE_TARGET_REASON_PROMOTABLE,
    LIVE_SLOT_STATUS_EXPECTED,
    LIVE_SLOT_STATUS_MISSED,
    LIVE_SLOT_STATUS_QUEUED,
    LIVE_SLOT_STATUS_RECOVERED,
    LIVE_SLOT_STATUS_RUNNING,
    LIVE_SLOT_STATUS_SUCCEEDED,
    LIVE_SLOT_STATUS_UNRECOVERABLE,
    LIVE_SLOT_TERMINAL_STATUSES,
    OPEN_POSITION_CAPTURE_STATUSES,
    RECOVERY_CONTROL_CLEAR_REASON_CODE,
    RECOVERY_CONTROL_REASON_CODE,
    RECOVERY_STATE_BLOCKED_WAITING_FRESH_SLOT,
    RECOVERY_STATE_CLEAR,
    RECOVERY_STATE_RECONCILING,
    RECOVERY_STATE_RECOVERING,
    RECOVERY_TRACKED_REASONS,
    _slot_details,
    _slot_timestamp,
)
from .status import (
    list_session_slot_health_by_session_id,
    load_session_slot_health,
    merge_live_action_gate_with_recovery,
    resolve_live_slot_stale_after_seconds,
    summarize_session_slot_health,
)


def run_collector_recovery(
    *,
    db_target: str,
    storage: Any,
) -> dict[str, Any]:
    recovery_store = storage.recovery
    job_store = storage.jobs
    history_store = storage.history
    if not recovery_store.schema_ready() or not job_store.schema_ready():
        return {
            "status": "skipped",
            "reason": "recovery_schema_unavailable",
        }

    from core.services.broker_sync import run_broker_sync
    from core.services.exit_manager import run_position_exit_manager

    broker_sync = run_broker_sync(db_target=db_target, storage=storage)
    exit_manager = run_position_exit_manager(db_target=db_target, storage=storage)
    execution_targets = refresh_execution_capture_targets(storage=storage)

    definitions = {
        str(row["job_key"]): dict(row)
        for row in job_store.list_job_definitions(
            enabled_only=True,
            job_type="live_collector",
        )
    }
    now = datetime.now(UTC)
    session_rows = recovery_store.list_live_session_slots(
        statuses=[
            LIVE_SLOT_STATUS_EXPECTED,
            LIVE_SLOT_STATUS_QUEUED,
            LIVE_SLOT_STATUS_RUNNING,
            LIVE_SLOT_STATUS_MISSED,
            LIVE_SLOT_STATUS_RECOVERED,
            LIVE_SLOT_STATUS_UNRECOVERABLE,
            LIVE_SLOT_STATUS_SUCCEEDED,
        ],
        session_date=datetime.now(NEW_YORK).date().isoformat(),
        limit=5000,
        ascending=True,
    )
    rows_by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in session_rows:
        session_id = _as_text(row.get("session_id"))
        if session_id is None:
            continue
        rows_by_session[session_id].append(dict(row))

    recovered_slot_count = 0
    unrecoverable_slot_count = 0
    newly_missed_slot_count = 0
    blocked_sessions: dict[str, dict[str, Any]] = {}
    session_summaries: dict[str, dict[str, Any]] = {}

    for session_id, slot_rows in rows_by_session.items():
        ordered_rows = sorted(
            slot_rows,
            key=lambda row: _slot_timestamp(row) or datetime.fromtimestamp(0, UTC),
        )
        if not ordered_rows:
            continue
        definition = definitions.get(str(ordered_rows[0].get("job_key") or ""))
        payload = definition.get("payload") if isinstance(definition, Mapping) else {}
        interval_seconds = max(_coerce_int(payload.get("interval_seconds")) or 300, 1)
        stale_after_seconds = resolve_live_slot_stale_after_seconds(interval_seconds)

        for slot_row in ordered_rows:
            slot_status = str(slot_row.get("status") or "")
            if slot_status not in {
                LIVE_SLOT_STATUS_EXPECTED,
                LIVE_SLOT_STATUS_QUEUED,
                LIVE_SLOT_STATUS_RUNNING,
            }:
                continue
            if not _slot_should_be_marked_missed(
                job_store=job_store,
                slot_row=slot_row,
                now=now,
                stale_after_seconds=stale_after_seconds,
            ):
                continue
            updated = update_live_session_slot_from_row(
                recovery_store,
                slot_row=slot_row,
                status=LIVE_SLOT_STATUS_MISSED,
                recovery_note="Live slot aged past its freshness window before completing.",
                slot_details=_slot_details(slot_row),
                finished_at=_utc_now(),
                updated_at=_utc_now(),
            )
            slot_row.update(updated)
            newly_missed_slot_count += 1

        for slot_row in ordered_rows:
            if str(slot_row.get("status") or "") != LIVE_SLOT_STATUS_MISSED:
                continue
            continuity_rows = _continuity_rows_for_slot(
                recovery_store=recovery_store,
                session_id=session_id,
                slot_rows=ordered_rows,
                slot_at=str(slot_row["slot_at"]),
            )
            if not continuity_rows:
                updated = update_live_session_slot_from_row(
                    recovery_store,
                    slot_row=slot_row,
                    status=LIVE_SLOT_STATUS_UNRECOVERABLE,
                    recovery_note="Gap is unrecoverable because no continuity capture targets existed before the missed slot.",
                    slot_details={
                        **_slot_details(slot_row),
                        "continuity_target_count": 0,
                    },
                    finished_at=_utc_now(),
                    updated_at=_utc_now(),
                )
                slot_row.update(updated)
                unrecoverable_slot_count += 1
                continue
            coverage = _coverage_summary_for_slot(
                history_store=history_store,
                continuity_rows=continuity_rows,
                slot_at=str(slot_row["slot_at"]),
                interval_seconds=interval_seconds,
            )
            if bool(coverage["coverage_sufficient"]):
                updated = update_live_session_slot_from_row(
                    recovery_store,
                    slot_row=slot_row,
                    status=LIVE_SLOT_STATUS_RECOVERED,
                    recovery_note="Continuity quotes were present for all tracked symbols during the missed slot window.",
                    slot_details={
                        **_slot_details(slot_row),
                        "recovery_coverage": coverage,
                        "continuity_targets": continuity_rows,
                    },
                    finished_at=_utc_now(),
                    updated_at=_utc_now(),
                )
                slot_row.update(updated)
                recovered_slot_count += 1
            else:
                updated = update_live_session_slot_from_row(
                    recovery_store,
                    slot_row=slot_row,
                    status=LIVE_SLOT_STATUS_UNRECOVERABLE,
                    recovery_note="Gap is unrecoverable because recorder coverage was incomplete for tracked continuity symbols.",
                    slot_details={
                        **_slot_details(slot_row),
                        "recovery_coverage": coverage,
                        "continuity_targets": continuity_rows,
                    },
                    finished_at=_utc_now(),
                    updated_at=_utc_now(),
                )
                slot_row.update(updated)
                unrecoverable_slot_count += 1

        refreshed_rows = recovery_store.list_live_session_slots(
            session_id=session_id,
            session_date=str(ordered_rows[0]["session_date"]),
            limit=500,
            ascending=True,
        )
        slot_health = summarize_session_slot_health(refreshed_rows)
        session_summaries[session_id] = slot_health
        refresh_recovery_session_capture_targets(
            storage=storage,
            session_id=session_id,
            slot_rows=refreshed_rows,
            slot_health=slot_health,
        )
        if (
            str(slot_health.get("recovery_state") or RECOVERY_STATE_CLEAR)
            != RECOVERY_STATE_CLEAR
        ):
            blocked_sessions[session_id] = slot_health

    control_action = _set_recovery_control_mode(
        db_target=db_target,
        storage=storage,
        blocked_sessions=blocked_sessions,
    )
    return {
        "status": "ok",
        "broker_sync": broker_sync,
        "exit_manager": exit_manager,
        "execution_targets": execution_targets,
        "newly_missed_slot_count": newly_missed_slot_count,
        "recovered_slot_count": recovered_slot_count,
        "unrecoverable_slot_count": unrecoverable_slot_count,
        "blocked_session_count": len(blocked_sessions),
        "sessions": session_summaries,
        "control_action": control_action,
    }


__all__ = [
    "CAPTURE_OWNER_EXECUTION_ATTEMPT",
    "CAPTURE_OWNER_LIVE_SESSION",
    "CAPTURE_OWNER_RECOVERY_SESSION",
    "CAPTURE_OWNER_SESSION_POSITION",
    "CAPTURE_TARGET_REASON_MONITOR",
    "CAPTURE_TARGET_REASON_OPEN_POSITION",
    "CAPTURE_TARGET_REASON_PENDING_EXECUTION",
    "CAPTURE_TARGET_REASON_PROMOTABLE",
    "LIVE_SLOT_STATUS_EXPECTED",
    "LIVE_SLOT_STATUS_MISSED",
    "LIVE_SLOT_STATUS_QUEUED",
    "LIVE_SLOT_STATUS_RECOVERED",
    "LIVE_SLOT_STATUS_RUNNING",
    "LIVE_SLOT_STATUS_SUCCEEDED",
    "LIVE_SLOT_STATUS_UNRECOVERABLE",
    "LIVE_SLOT_TERMINAL_STATUSES",
    "OPEN_POSITION_CAPTURE_STATUSES",
    "RECOVERY_CONTROL_CLEAR_REASON_CODE",
    "RECOVERY_CONTROL_REASON_CODE",
    "RECOVERY_STATE_BLOCKED_WAITING_FRESH_SLOT",
    "RECOVERY_STATE_CLEAR",
    "RECOVERY_STATE_RECONCILING",
    "RECOVERY_STATE_RECOVERING",
    "RECOVERY_TRACKED_REASONS",
    "build_capture_target_rows_for_candidates",
    "build_slot_details_from_cycle_result",
    "list_session_slot_health_by_session_id",
    "load_session_slot_health",
    "merge_live_action_gate_with_recovery",
    "refresh_execution_capture_targets",
    "refresh_live_session_capture_targets",
    "refresh_recovery_session_capture_targets",
    "resolve_live_slot_stale_after_seconds",
    "run_collector_recovery",
    "summarize_session_slot_health",
]

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from spreads.alerts.rules import (
    AlertDecision,
    build_event_alert_decisions,
    build_score_breakout_decisions,
    build_uoa_alert_decisions,
    score_anchor_key,
)
from spreads.services.alert_delivery import plan_alert_delivery
from spreads.services.live_pipelines import build_live_session_id
from spreads.storage.alert_repository import AlertRepository
from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.job_repository import JobRepository

NEW_YORK = ZoneInfo("America/New_York")


def resolve_session_date(generated_at: str) -> str:
    parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    return parsed.astimezone(NEW_YORK).date().isoformat()


def alert_payload(
    *,
    label: str,
    cycle_id: str,
    generated_at: str,
    strategy_mode: str,
    profile: str,
    session_date: str,
    alert: AlertDecision,
) -> dict[str, Any]:
    return {
        "created_at": generated_at,
        "session_date": session_date,
        "label": label,
        "cycle_id": cycle_id,
        "symbol": alert.symbol,
        "alert_type": alert.alert_type,
        "strategy_mode": strategy_mode,
        "profile": profile,
        "candidate": alert.candidate,
        "description": alert.description,
    }


def update_score_anchors(
    *,
    alert_store: AlertRepository,
    label: str,
    session_date: str,
    session_id: str,
    generated_at: str,
    cycle_id: str,
    planner_job_run_id: str | None,
    board_candidates: list[dict[str, Any]],
) -> None:
    for candidate in board_candidates:
        quality_score = candidate.get("quality_score")
        if quality_score is None:
            continue
        alert_store.upsert_score_anchor(
            created_at=generated_at,
            session_date=session_date,
            label=label,
            session_id=session_id,
            cycle_id=cycle_id,
            symbol=str(candidate["underlying_symbol"]),
            dedupe_key=score_anchor_key(label, session_date, candidate),
            state={"last_score": float(quality_score)},
            planner_job_run_id=planner_job_run_id,
        )


def dispatch_cycle_alerts(
    *,
    collector_store: CollectorRepository,
    alert_store: AlertRepository,
    job_store: JobRepository,
    cycle_id: str,
    label: str,
    generated_at: str,
    strategy_mode: str,
    profile: str,
    board_candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
    uoa_decisions: dict[str, Any] | None = None,
    session_id: str | None = None,
    planner_job_run_id: str | None = None,
    webhook_url: str | None = None,
) -> list[dict[str, Any]]:
    session_date = resolve_session_date(generated_at)
    resolved_session_id = session_id or build_live_session_id(label, session_date)
    get_state = alert_store.get_alert_state

    decisions = [
        *build_event_alert_decisions(
            label=label,
            session_date=session_date,
            current_cycle_id=cycle_id,
            current_generated_at=generated_at,
            events=events,
            collector_store=collector_store,
            get_alert_state=get_state,
        ),
        *build_score_breakout_decisions(
            label=label,
            session_date=session_date,
            board_candidates=board_candidates,
            get_alert_state=get_state,
        ),
        *build_uoa_alert_decisions(
            label=label,
            session_date=session_date,
            uoa_decisions=uoa_decisions,
            get_alert_state=get_state,
        ),
    ]

    planned: list[dict[str, Any]] = []
    for decision in decisions:
        payload = alert_payload(
            label=label,
            cycle_id=cycle_id,
            generated_at=generated_at,
            strategy_mode=strategy_mode,
            profile=profile,
            session_date=session_date,
            alert=decision,
        )
        record, _ = plan_alert_delivery(
            alert_store=alert_store,
            job_store=job_store,
            payload=payload,
            dedupe_key=decision.dedupe_key,
            dedupe_state=decision.dedupe_state,
            session_id=resolved_session_id,
            planner_job_run_id=planner_job_run_id,
            source="alerts.dispatcher",
            correlation_id=cycle_id,
            webhook_url=webhook_url,
        )
        planned.append(record)

    update_score_anchors(
        alert_store=alert_store,
        label=label,
        session_date=session_date,
        session_id=resolved_session_id,
        generated_at=generated_at,
        cycle_id=cycle_id,
        planner_job_run_id=planner_job_run_id,
        board_candidates=board_candidates,
    )
    return planned

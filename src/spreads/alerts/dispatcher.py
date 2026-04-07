from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from spreads.alerts.discord import build_discord_payload, send_discord_webhook
from spreads.alerts.rules import (
    AlertDecision,
    build_event_alert_decisions,
    build_score_breakout_decisions,
    score_anchor_key,
)
from spreads.storage.alert_repository import AlertRepository
from spreads.storage.collector_repository import CollectorRepository

NEW_YORK = ZoneInfo("America/New_York")
DISCORD_DELIVERY_TARGET = "discord_webhook"


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


def persist_alert_state(
    *,
    alert_store: AlertRepository,
    generated_at: str,
    cycle_id: str,
    alert_type: str,
    dedupe_key: str,
    state: dict[str, Any],
) -> None:
    alert_store.upsert_alert_state(
        dedupe_key=dedupe_key,
        last_alert_at=generated_at,
        last_cycle_id=cycle_id,
        last_alert_type=alert_type,
        state=state,
    )


def update_score_anchor(
    *,
    alert_store: AlertRepository,
    label: str,
    session_date: str,
    generated_at: str,
    cycle_id: str,
    candidate: dict[str, Any],
    alert_type: str,
) -> None:
    persist_alert_state(
        alert_store=alert_store,
        generated_at=generated_at,
        cycle_id=cycle_id,
        alert_type=alert_type,
        dedupe_key=score_anchor_key(label, session_date, candidate),
        state={"last_score": float(candidate["quality_score"])},
    )


def send_or_skip_alert(
    *,
    webhook_url: str | None,
    alert_store: AlertRepository,
    payload: dict[str, Any],
    dedupe_key: str,
    dedupe_state: dict[str, Any],
) -> dict[str, Any]:
    event = alert_store.create_alert_event(
        created_at=payload["created_at"],
        session_date=payload["session_date"],
        label=payload["label"],
        cycle_id=payload["cycle_id"],
        symbol=payload["symbol"],
        alert_type=payload["alert_type"],
        dedupe_key=dedupe_key,
        status="pending",
        delivery_target=DISCORD_DELIVERY_TARGET,
        payload=payload,
    )
    if not webhook_url:
        persisted = alert_store.mark_alert_event_status(
            alert_id=event["alert_id"],
            status="skipped",
            response={"reason": "missing_SPREADS_DISCORD_WEBHOOK_URL"},
        )
        persist_alert_state(
            alert_store=alert_store,
            generated_at=payload["created_at"],
            cycle_id=payload["cycle_id"],
            alert_type=payload["alert_type"],
            dedupe_key=dedupe_key,
            state=dedupe_state,
        )
        return persisted.to_dict()

    discord_payload = build_discord_payload(payload)
    try:
        response = send_discord_webhook(webhook_url, discord_payload)
        persisted = alert_store.mark_alert_event_status(
            alert_id=event["alert_id"],
            status="delivered",
            response=response,
        )
        persist_alert_state(
            alert_store=alert_store,
            generated_at=payload["created_at"],
            cycle_id=payload["cycle_id"],
            alert_type=payload["alert_type"],
            dedupe_key=dedupe_key,
            state=dedupe_state,
        )
        return persisted.to_dict()
    except Exception as exc:
        persisted = alert_store.mark_alert_event_status(
            alert_id=event["alert_id"],
            status="failed",
            error_text=str(exc),
        )
        return persisted.to_dict()


def dispatch_cycle_alerts(
    *,
    collector_store: CollectorRepository,
    alert_store: AlertRepository,
    cycle_id: str,
    label: str,
    generated_at: str,
    strategy_mode: str,
    profile: str,
    board_candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
    webhook_url: str | None = None,
) -> list[dict[str, Any]]:
    session_date = resolve_session_date(generated_at)
    get_state = alert_store.get_alert_state
    webhook = (
        webhook_url
        if webhook_url is not None
        else os.environ.get("SPREADS_DISCORD_WEBHOOK_URL") or os.environ.get("DISCORD_WEBHOOK_URL")
    )

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
    ]

    delivered: list[dict[str, Any]] = []
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
        record = send_or_skip_alert(
            webhook_url=webhook,
            alert_store=alert_store,
            payload=payload,
            dedupe_key=decision.dedupe_key,
            dedupe_state=decision.dedupe_state,
        )
        update_score_anchor(
            alert_store=alert_store,
            label=label,
            session_date=session_date,
            generated_at=generated_at,
            cycle_id=cycle_id,
            candidate=decision.candidate,
            alert_type=decision.alert_type,
        )
        delivered.append(record)
    return delivered

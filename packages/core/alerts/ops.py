from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from core.services.alert_delivery import plan_alert_delivery
from core.services.bot_analytics import build_bot_metrics
from core.services.bots import load_active_bots
from core.services.ops.shared import _automation_dispatch_gap_summary
from core.services.value_coercion import utc_now_iso as _utc_now

OPS_DISPATCH_GAP_OPEN_ALERT_TYPE = "ops_dispatch_gap_open"


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def dispatch_gap_open_key(bot_id: str, market_date: str) -> str:
    return f"{OPS_DISPATCH_GAP_OPEN_ALERT_TYPE}|{market_date}|{bot_id}"


def _normalize_automation_ids(values: Iterable[str] | None) -> list[str]:
    if values is None:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        rendered = _as_text(value)
        if rendered is None or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return sorted(normalized)


def _bot_display_name(bot_id: str) -> str:
    bot = load_active_bots().get(bot_id)
    configured_name = None if bot is None else _as_text(bot.bot.name)
    return configured_name or bot_id


def _dispatch_gap_open_payload(
    *,
    bot_id: str,
    market_date: str,
    summary: Mapping[str, Any],
    automation_ids: Iterable[str] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    bot_name = _bot_display_name(bot_id)
    normalized_automation_ids = _normalize_automation_ids(automation_ids)
    selected_count = int(summary.get("selected_count") or 0)
    intent_count = int(summary.get("intent_count") or 0)
    submitted_count = int(summary.get("submitted_count") or 0)
    expired_count = int(summary.get("dispatch_window_elapsed_count") or 0)
    pending_gap_count = int(summary.get("pending_submission_gap_count") or 0)
    description = (
        f"{bot_name} selected {selected_count} entry opportunity(s) on {market_date}, "
        f"but {expired_count} aged out before broker submission."
    )
    details = {
        "bot_id": bot_id,
        "bot_name": bot_name,
        "market_date": market_date,
        "automation_ids": normalized_automation_ids,
        "selected_count": selected_count,
        "intent_count": intent_count,
        "submitted_count": submitted_count,
        "dispatch_window_elapsed_count": expired_count,
        "pending_submission_gap_count": pending_gap_count,
    }
    payload = {
        "created_at": _utc_now(),
        "session_date": market_date,
        "label": bot_id,
        "cycle_id": f"ops_dispatch_gap:{market_date}:{bot_id}",
        "symbol": bot_id,
        "alert_type": OPS_DISPATCH_GAP_OPEN_ALERT_TYPE,
        "strategy_mode": "ops",
        "profile": "runtime",
        "description": description,
        "details": details,
    }
    return payload, details


def plan_dispatch_gap_open_alerts(
    *,
    storage: Any,
    alert_store: Any,
    job_store: Any,
    bot_automation_ids: Mapping[str, Iterable[str]] | None,
    market_date: str,
    planner_job_run_id: str | None = None,
    source: str = "execution_intents.dispatch",
    correlation_id: str | None = None,
    webhook_url: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(bot_automation_ids, Mapping) or not bot_automation_ids:
        return []
    if alert_store is None or job_store is None:
        return []
    if hasattr(alert_store, "schema_ready") and not alert_store.schema_ready():
        return []
    if hasattr(job_store, "schema_ready") and not job_store.schema_ready():
        return []

    planned: list[dict[str, Any]] = []
    for raw_bot_id, automation_ids in sorted(bot_automation_ids.items()):
        bot_id = _as_text(raw_bot_id)
        if bot_id is None:
            continue
        summary = _automation_dispatch_gap_summary(
            build_bot_metrics(
                storage=storage,
                bot_id=bot_id,
                market_date=market_date,
            )
        )
        if not summary.get("has_dispatch_gap"):
            continue
        payload, details = _dispatch_gap_open_payload(
            bot_id=bot_id,
            market_date=market_date,
            summary=summary,
            automation_ids=automation_ids,
        )
        dedupe_key = dispatch_gap_open_key(bot_id, market_date)
        record, created = plan_alert_delivery(
            alert_store=alert_store,
            job_store=job_store,
            payload=payload,
            dedupe_key=dedupe_key,
            dedupe_state=details,
            session_id=f"ops_dispatch_gap:{bot_id}:{market_date}",
            planner_job_run_id=planner_job_run_id,
            source=source,
            correlation_id=correlation_id,
            webhook_url=webhook_url,
        )
        planned.append(
            {
                "bot_id": bot_id,
                "automation_ids": details["automation_ids"],
                "alert_id": record.get("alert_id"),
                "alert_type": OPS_DISPATCH_GAP_OPEN_ALERT_TYPE,
                "dedupe_key": dedupe_key,
                "created": created,
                "status": record.get("status"),
            }
        )
    return planned


__all__ = [
    "OPS_DISPATCH_GAP_OPEN_ALERT_TYPE",
    "dispatch_gap_open_key",
    "plan_dispatch_gap_open_alerts",
]

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any


from core.jobs.orchestration import NEW_YORK
from core.services.control_plane import (
    get_control_state_snapshot,
    resolve_execution_kill_switch_reason,
)
from core.value_coercion import (
    as_list,
    as_mapping,
    as_text,
)

from core.services.ops.jobs.state import build_jobs_compact_state
from core.services.ops.shared import (
    _attention,
    _control_status,
)

from .engine import build_engine_ops_state
from .market import market_session_context as _market_session_context

from core.services.ops.trading.models import (
    RECENT_ALERT_LIMIT,
    _AlertProjection,
    _EngineProjection,
    _JobsProjection,
    _MarketControlProjection,
)
from core.services.ops.trading.account import _alert_delivery_payload

def _project_market_control(
    *,
    storage: Any,
    market_date: str | None,
    now: datetime,
) -> _MarketControlProjection:
    resolved_market_date = as_text(market_date) or now.astimezone(NEW_YORK).date().isoformat()
    market_session = _market_session_context(now=now)
    control = get_control_state_snapshot(storage=storage)
    control_status = _control_status(control)
    statuses = [control_status]
    attention: list[dict[str, str]] = []

    if control_status in {"degraded", "halted"}:
        attention.append(
            _attention(
                severity="high" if control_status == "halted" else "medium",
                code=f"control_mode_{control.get('mode')}",
                message=as_text(control.get("note")) or f"Control mode is {control.get('mode')}.",
            )
        )

    kill_switch_reason = resolve_execution_kill_switch_reason()
    if kill_switch_reason is not None:
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="kill_switch_enabled",
                message=kill_switch_reason,
            )
        )

    return _MarketControlProjection(
        market_date=resolved_market_date,
        market_session=market_session,
        market_open=bool(market_session.get("is_open")),
        control=control,
        kill_switch_reason=kill_switch_reason,
        statuses=tuple(statuses),
        attention=attention,
    )


def _project_jobs(
    *,
    db_target: str | None,
    storage: Any,
) -> _JobsProjection:
    jobs = build_jobs_compact_state(db_target=db_target, limit=25, storage=storage)
    return _JobsProjection(
        payload=jobs,
        summary=as_mapping(jobs.get("summary")),
        details=as_mapping(jobs.get("details")),
        statuses=(str(jobs.get("status") or "unknown"),),
        attention=[dict(row) for row in as_list(jobs.get("attention")) if isinstance(row, Mapping)],
    )

def _project_engine(
    *,
    storage: Any,
    market_date: str,
    now: datetime,
) -> _EngineProjection:
    engine_ops = build_engine_ops_state(
        storage=storage,
        market_date=market_date,
        now=now,
    )
    engine_summary = as_mapping(engine_ops.get("summary"))
    engine_status = str(engine_ops.get("status") or "unknown")
    attention: list[dict[str, str]] = []
    if engine_status in {"degraded", "blocked"}:
        attention.append(
            _attention(
                severity="high" if engine_status == "blocked" else "medium",
                code="engine_unhealthy",
                message="Engine facts, execution storage, or capture targets need attention.",
            )
        )
    return _EngineProjection(
        payload=engine_ops,
        summary=engine_summary,
        status=engine_status,
        statuses=(engine_status,),
        attention=attention,
    )

def _project_alerts(
    *,
    storage: Any,
    now: datetime,
) -> _AlertProjection:
    alert_store = storage.alerts
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    if alert_store.schema_ready():
        recent_alerts = [dict(row) for row in alert_store.list_alert_events(limit=RECENT_ALERT_LIMIT)]
        alert_delivery = _alert_delivery_payload(recent_alerts, now=now)
        if alert_delivery["status"] != "healthy":
            statuses.append(str(alert_delivery["status"]))
            attention.append(
                _attention(
                    severity="medium",
                    code="alert_delivery_issues",
                    message="Recent alert delivery failures or retries were detected.",
                )
            )
    else:
        alert_delivery = {
            "status": "unknown",
            "recent_event_count": 0,
            "status_counts": {},
            "dead_letter_count": 0,
            "retry_wait_count": 0,
            "dispatching_count": 0,
            "pending_count": 0,
        }
    return _AlertProjection(alert_delivery=alert_delivery, statuses=tuple(statuses), attention=attention)

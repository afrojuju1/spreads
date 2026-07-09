from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any


from core.money import money_sum_float
from core.services.trading_engine.exit_runtime import describe_position_exit_state
from core.services.risk.admission import assess_position_risk
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_float,
    coerce_int,
)

from core.services.ops.shared import (
    _attention,
    _seconds_since,
)


from core.services.ops.trading.models import (
    MARK_STALE_AFTER_SECONDS,
    OPEN_POSITION_STATUSES,
    _PositionProjection,
)
from core.services.ops.trading.account import _top_positions

def _project_positions(
    *,
    storage: Any,
    now: datetime,
    broker_sync: Mapping[str, Any],
    market_session: Mapping[str, Any],
) -> _PositionProjection:
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    execution_store = storage.execution
    open_positions: list[dict[str, Any]] = []
    top_positions: list[dict[str, Any]] = []
    risk_breach_count = 0
    reconciliation_mismatch_count = 0
    missing_mark_count = 0
    stale_mark_count = 0
    mark_freshness_required = bool(market_session.get("is_open"))
    if execution_store.portfolio_schema_ready():
        from core.services.positions import enrich_position_row

        persisted_positions = [
            enrich_position_row(dict(row))
            for row in execution_store.list_positions(
                statuses=OPEN_POSITION_STATUSES,
                limit=200,
            )
        ]
        for position in persisted_positions:
            risk = assess_position_risk(position=position)
            close_mark = coerce_float(position.get("close_mark"))
            mark_age_seconds = _seconds_since(position.get("close_marked_at"), now=now)
            if close_mark is None:
                missing_mark_count += 1
            elif mark_age_seconds is not None and mark_age_seconds > MARK_STALE_AFTER_SECONDS:
                stale_mark_count += 1
            if str(position.get("reconciliation_status") or "") == "mismatch":
                reconciliation_mismatch_count += 1
            if str(risk.get("status") or "") == "breach":
                risk_breach_count += 1
            realized_pnl = coerce_float(position.get("realized_pnl")) or 0.0
            unrealized_pnl = coerce_float(position.get("unrealized_pnl")) or 0.0
            open_positions.append(
                {
                    **position,
                    "status": position.get("status"),
                    "risk_status": risk.get("status"),
                    "risk_note": risk.get("note"),
                    "mark_age_seconds": None if mark_age_seconds is None else round(mark_age_seconds, 2),
                    "net_pnl": money_sum_float([realized_pnl, unrealized_pnl]),
                    "exit_status": describe_position_exit_state(
                        position=position,
                        now=now,
                    ),
                }
            )
        top_positions = _top_positions(open_positions)
    else:
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="position_schema_unavailable",
                message="Position storage is not available yet.",
            )
        )

    mark_error = as_text(as_mapping(broker_sync.get("summary")).get("mark_error"))
    broker_unquoted_positions = coerce_int(as_mapping(broker_sync.get("summary")).get("unquoted_position_count")) or 0
    actionable_stale_mark_count = stale_mark_count if mark_freshness_required else 0
    mark_health_status = "healthy"
    if missing_mark_count or actionable_stale_mark_count or broker_unquoted_positions or mark_error:
        mark_health_status = "degraded"
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="mark_health_degraded",
                message="One or more open positions have missing, stale, or unavailable quote marks.",
            )
        )

    if risk_breach_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="risk_breaches_present",
                message=f"{risk_breach_count} open position(s) are outside snapshotted risk limits.",
            )
        )

    if reconciliation_mismatch_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="reconciliation_mismatches_present",
                message=f"{reconciliation_mismatch_count} open position(s) have reconciliation mismatches.",
            )
        )

    return _PositionProjection(
        open_positions=open_positions,
        top_positions=top_positions,
        risk_breach_count=risk_breach_count,
        reconciliation_mismatch_count=reconciliation_mismatch_count,
        missing_mark_count=missing_mark_count,
        stale_mark_count=stale_mark_count,
        mark_freshness_required=mark_freshness_required,
        broker_unquoted_positions=broker_unquoted_positions,
        mark_error=mark_error,
        mark_health_status=mark_health_status,
        statuses=tuple(statuses),
        attention=attention,
    )

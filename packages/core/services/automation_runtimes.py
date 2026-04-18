from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from core.db.decorators import with_storage
from core.services.automation_runtime import (
    EntryRuntime,
    ManagementRuntime,
    resolve_entry_runtimes,
    resolve_management_runtimes,
)
from core.services.bot_analytics import build_bot_metrics, summarize_intent_counts
from core.services.opportunities import list_opportunities
from core.services.positions import OPEN_POSITION_STATUSES, list_positions
from core.services.runtime_identity import build_live_run_scope_id, build_pipeline_id
from core.storage.serializers import parse_datetime

RuntimeConfig = EntryRuntime | ManagementRuntime


def _store_ready(store: Any, *, schema_method: str, data_method: str) -> bool:
    readiness = getattr(store, schema_method, None)
    if callable(readiness):
        return bool(readiness())
    return callable(getattr(store, data_method, None))


def _window_bounds(market_date: str | None) -> tuple[str | None, datetime | None, datetime | None]:
    if market_date is None:
        return None, None, None
    window_start = datetime.fromisoformat(market_date).replace(tzinfo=UTC)
    return market_date, window_start, window_start + timedelta(days=1)


def _within_window(
    value: Any,
    *,
    window_start: datetime | None,
    window_end: datetime | None,
) -> bool:
    if window_start is None or window_end is None:
        return True
    parsed = parse_datetime(None if value in (None, "") else str(value))
    return parsed is not None and window_start <= parsed < window_end


def _runtime_catalog() -> list[tuple[str, RuntimeConfig]]:
    rows: list[tuple[str, RuntimeConfig]] = [
        ("entry", runtime) for runtime in resolve_entry_runtimes()
    ]
    rows.extend(("management", runtime) for runtime in resolve_management_runtimes())
    rows.sort(key=lambda item: (item[1].bot_id, item[1].automation_id))
    return rows


def _runtime_match(
    *,
    bot_id: str,
    automation_id: str,
) -> tuple[str, RuntimeConfig]:
    for runtime_kind, runtime in _runtime_catalog():
        if runtime.bot_id == bot_id and runtime.automation_id == automation_id:
            return runtime_kind, runtime
    raise ValueError(f"Unknown active automation runtime: {bot_id}:{automation_id}")


def _runtime_metadata(
    *,
    runtime_kind: str,
    runtime: RuntimeConfig,
) -> dict[str, Any]:
    return {
        "bot_id": runtime.bot_id,
        "bot_name": runtime.bot.bot.name,
        "automation_id": runtime.automation_id,
        "automation_type": runtime_kind,
        "strategy_config_id": runtime.strategy_config_id,
        "strategy_id": runtime.strategy_id,
        "strategy_family": runtime.strategy_family,
        "config_hash": runtime.config_hash,
        "symbols": list(runtime.symbols),
        "schedule": dict(runtime.automation.automation.schedule),
        "trigger_policy": dict(runtime.trigger_policy),
        "approval_mode": runtime.automation.automation.approval_mode,
        "execution_mode": runtime.automation.automation.execution_mode,
        "live_enabled": bool(runtime.bot.bot.live_enabled),
        "max_open_positions": int(runtime.bot.bot.max_open_positions or 0),
        "max_daily_actions": int(runtime.bot.bot.max_daily_actions or 0),
        "max_new_entries_per_day": runtime.bot.bot.max_new_entries_per_day,
        "daily_loss_limit": runtime.bot.bot.daily_loss_limit,
    }


def _latest_discovery_payload(
    *,
    label: Any,
    cycle_id: Any,
    session_date: Any,
    pipeline_id: Any = None,
) -> dict[str, Any] | None:
    resolved_label = None if label in (None, "") else str(label)
    resolved_session_date = (
        None if session_date in (None, "") else str(session_date)
    )
    if resolved_label is None and cycle_id in (None, "") and resolved_session_date is None:
        return None
    resolved_pipeline_id = (
        None
        if pipeline_id in (None, "")
        else str(pipeline_id)
    ) or (
        None if resolved_label is None else build_pipeline_id(resolved_label)
    )
    return {
        "label": resolved_label,
        "pipeline_id": resolved_pipeline_id,
        "cycle_id": None if cycle_id in (None, "") else str(cycle_id),
        "session_date": resolved_session_date,
        "session_id": (
            None
            if resolved_label is None or resolved_session_date is None
            else build_live_run_scope_id(resolved_label, resolved_session_date)
        ),
    }


def _position_metrics(
    positions: list[dict[str, Any]],
    *,
    market_date: str | None,
) -> dict[str, Any]:
    open_positions = [
        row
        for row in positions
        if str(row.get("position_status") or row.get("status") or "")
        in OPEN_POSITION_STATUSES
    ]
    closed_positions = [
        row
        for row in positions
        if str(row.get("position_status") or row.get("status") or "")
        not in OPEN_POSITION_STATUSES
    ]
    open_unrealized_pnl = sum(float(row.get("unrealized_pnl") or 0.0) for row in open_positions)
    total_realized_pnl = sum(float(row.get("realized_pnl") or 0.0) for row in positions)
    daily_realized_pnl = (
        0.0
        if market_date is None
        else sum(
            float(row.get("realized_pnl") or 0.0)
            for row in positions
            if str(row.get("market_date_opened") or row.get("market_date")) == market_date
            or str(row.get("market_date_closed") or "") == market_date
        )
    )
    daily_total_pnl = daily_realized_pnl + open_unrealized_pnl
    return {
        "position_count": len(positions),
        "open_position_count": len(open_positions),
        "closed_position_count": len(closed_positions),
        "daily_realized_pnl": round(daily_realized_pnl, 2),
        "open_unrealized_pnl": round(open_unrealized_pnl, 2),
        "daily_total_pnl": round(daily_total_pnl, 2),
        "total_realized_pnl": round(total_realized_pnl, 2),
    }


def _owner_payload(
    *,
    db_target: str,
    storage: Any,
    bot_id: str,
    automation_id: str,
    strategy_config_id: str,
    market_date: str | None,
    limit: int,
) -> dict[str, Any]:
    opportunities_payload = list_opportunities(
        db_target=db_target,
        bot_id=bot_id,
        automation_id=automation_id,
        strategy_config_id=strategy_config_id,
        market_date=market_date,
        include_analysis_only=True,
        limit=limit,
        storage=storage,
    )
    positions_payload = list_positions(
        db_target=db_target,
        bot_id=bot_id,
        automation_id=automation_id,
        strategy_config_id=strategy_config_id,
        market_date=market_date,
        limit=limit,
        storage=storage,
    )
    opportunities = [
        dict(row) for row in list(opportunities_payload.get("opportunities") or [])
    ]
    positions = [dict(row) for row in list(positions_payload.get("positions") or [])]
    return {
        "opportunities": opportunities,
        "positions": positions,
        "position_summary": dict(positions_payload.get("summary") or {}),
    }


def _runtime_summary(
    *,
    db_target: str,
    storage: Any,
    runtime_kind: str,
    runtime: RuntimeConfig,
    market_date: str | None,
    limit: int,
) -> dict[str, Any]:
    signal_store = storage.signals
    execution_store = storage.execution
    owner_payload = _owner_payload(
        db_target=db_target,
        storage=storage,
        bot_id=runtime.bot_id,
        automation_id=runtime.automation_id,
        strategy_config_id=runtime.strategy_config_id,
        market_date=market_date,
        limit=limit,
    )
    resolved_market_date, window_start, window_end = _window_bounds(market_date)
    decisions = (
        []
        if not _store_ready(
            signal_store,
            schema_method="schema_ready",
            data_method="list_opportunity_decisions",
        )
        else [
            dict(row)
            for row in signal_store.list_opportunity_decisions(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=limit,
            )
            if _within_window(
                row.get("decided_at"),
                window_start=window_start,
                window_end=window_end,
            )
        ]
    )
    intents = (
        []
        if not _store_ready(
            execution_store,
            schema_method="intent_schema_ready",
            data_method="list_execution_intents",
        )
        else [
            dict(row)
            for row in execution_store.list_execution_intents(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=limit,
            )
            if _within_window(
                row.get("created_at"),
                window_start=window_start,
                window_end=window_end,
            )
        ]
    )
    automation_runs = (
        []
        if not _store_ready(
            signal_store,
            schema_method="automation_runtime_schema_ready",
            data_method="list_automation_runs",
        )
        else [
            dict(row)
            for row in signal_store.list_automation_runs(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                session_date=resolved_market_date,
                limit=limit,
            )
        ]
    )
    if not automation_runs and _store_ready(
        signal_store,
        schema_method="automation_runtime_schema_ready",
        data_method="list_automation_runs",
    ):
        automation_runs = [
            dict(row)
            for row in signal_store.list_automation_runs(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=limit,
            )
        ]
    decision_state_counts = Counter(
        str(row.get("state") or "unknown") for row in decisions
    )
    intent_summary = summarize_intent_counts(
        [
            (
                row.get("action_type"),
                row.get("state"),
                1,
            )
            for row in intents
        ]
    )
    opportunities = owner_payload["opportunities"]
    live_opportunity_count = sum(
        1
        for row in opportunities
        if str(row.get("eligibility") or row.get("eligibility_state") or "") == "live"
    )
    position_metrics = _position_metrics(
        owner_payload["positions"],
        market_date=resolved_market_date,
    )
    latest_run = automation_runs[0] if automation_runs else None
    latest_discovery = None
    if latest_run is not None:
        latest_discovery = _latest_discovery_payload(
            label=latest_run.get("label"),
            pipeline_id=latest_run.get("pipeline_id"),
            cycle_id=latest_run.get("cycle_id"),
            session_date=latest_run.get("session_date"),
        )
    elif opportunities:
        discovery = opportunities[0].get("discovery")
        if isinstance(discovery, dict):
            latest_discovery = _latest_discovery_payload(
                label=discovery.get("label"),
                pipeline_id=discovery.get("pipeline_id"),
                cycle_id=discovery.get("cycle_id"),
                session_date=opportunities[0].get("market_date"),
            )
    return {
        **_runtime_metadata(runtime_kind=runtime_kind, runtime=runtime),
        "market_date": resolved_market_date,
        "opportunity_count": len(opportunities),
        "live_opportunity_count": live_opportunity_count,
        "decision_count": int(sum(decision_state_counts.values())),
        "decision_state_counts": dict(sorted(decision_state_counts.items())),
        **intent_summary,
        **position_metrics,
        "latest_automation_run": latest_run,
        "latest_discovery": latest_discovery,
        "bot_metrics": build_bot_metrics(
            storage=storage,
            bot_id=runtime.bot_id,
            market_date=resolved_market_date,
        ),
    }


@with_storage()
def list_automation_runtimes(
    *,
    db_target: str,
    market_date: str | None = None,
    limit: int = 100,
    storage: Any | None = None,
) -> dict[str, Any]:
    runtimes = [
        _runtime_summary(
            db_target=db_target,
            storage=storage,
            runtime_kind=runtime_kind,
            runtime=runtime,
            market_date=market_date,
            limit=limit,
        )
        for runtime_kind, runtime in _runtime_catalog()
    ]
    runtimes.sort(
        key=lambda row: (
            str(row.get("bot_name") or row.get("bot_id") or ""),
            str(row.get("automation_id") or ""),
        )
    )
    return {"automations": runtimes[:limit]}


@with_storage()
def get_automation_runtime_detail(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    market_date: str | None = None,
    limit: int = 200,
    storage: Any | None = None,
) -> dict[str, Any]:
    runtime_kind, runtime = _runtime_match(bot_id=bot_id, automation_id=automation_id)
    signal_store = storage.signals
    execution_store = storage.execution
    summary = _runtime_summary(
        db_target=db_target,
        storage=storage,
        runtime_kind=runtime_kind,
        runtime=runtime,
        market_date=market_date,
        limit=limit,
    )
    owner_payload = _owner_payload(
        db_target=db_target,
        storage=storage,
        bot_id=runtime.bot_id,
        automation_id=runtime.automation_id,
        strategy_config_id=runtime.strategy_config_id,
        market_date=market_date,
        limit=limit,
    )
    decisions = (
        []
        if not _store_ready(
            signal_store,
            schema_method="schema_ready",
            data_method="list_opportunity_decisions",
        )
        else [
            dict(row)
            for row in signal_store.list_opportunity_decisions(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=limit,
            )
        ]
    )
    intents = (
        []
        if not _store_ready(
            execution_store,
            schema_method="intent_schema_ready",
            data_method="list_execution_intents",
        )
        else [
            dict(row)
            for row in execution_store.list_execution_intents(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=limit,
            )
        ]
    )
    automation_runs = (
        []
        if not _store_ready(
            signal_store,
            schema_method="automation_runtime_schema_ready",
            data_method="list_automation_runs",
        )
        else [
            dict(row)
            for row in signal_store.list_automation_runs(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                session_date=market_date,
                limit=limit,
            )
        ]
    )
    if not automation_runs and _store_ready(
        signal_store,
        schema_method="automation_runtime_schema_ready",
        data_method="list_automation_runs",
    ):
        automation_runs = [
            dict(row)
            for row in signal_store.list_automation_runs(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=limit,
            )
        ]
    return {
        **summary,
        "summary": {
            key: value
            for key, value in summary.items()
            if key
            not in {
                "latest_automation_run",
                "latest_discovery",
                "bot_metrics",
            }
        },
        "config": _runtime_metadata(runtime_kind=runtime_kind, runtime=runtime),
        "latest_automation_run": summary.get("latest_automation_run"),
        "latest_discovery": summary.get("latest_discovery"),
        "bot_metrics": summary.get("bot_metrics"),
        "automation_runs": automation_runs,
        "opportunities": owner_payload["opportunities"],
        "positions": owner_payload["positions"],
        "decisions": decisions,
        "intents": intents,
    }


__all__ = [
    "get_automation_runtime_detail",
    "list_automation_runtimes",
]

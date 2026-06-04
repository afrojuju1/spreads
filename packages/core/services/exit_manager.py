from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

from core.db.decorators import with_storage
from core.services.trading_strategy_runtime import (
    find_management_runtime_for_position,
    resolve_management_runtimes,
)
from core.services.trading_engine.portfolio_runtime import (
    OPEN_POSITION_STATUSES,
    PostgresPortfolioEngine,
    build_portfolio_run_ref,
    build_position_snapshot,
)
from core.services.trading_strategies import routine_should_run_now
from core.services.execution_portfolio import refresh_session_position_marks
from core.services.option_structures import net_premium_kind
from core.services.position_lifecycle import build_close_decision_lifecycle
from core.services.positions import enrich_position_row
from core.services.risk_manager import (
    CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
    normalize_risk_policy,
    validate_close_execution,
)
from core.storage.serializers import parse_datetime

OPEN_CLOSE_ATTEMPT_STATUSES = [
    "accepted",
    "accepted_for_bidding",
    "calculated",
    "held",
    "new",
    "partially_filled",
    "pending_cancel",
    "pending_new",
    "pending_replace",
    "pending_submission",
    "replaced",
    "stopped",
    "suspended",
]
DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE = 10
DEFAULT_EXIT_POLICY = {
    "enabled": True,
    "profit_target_pct": 0.5,
    "stop_multiple": 2.0,
    "force_close_at": None,
}
MANAGED_CLOSE_INTENT_TTL_MINUTES = 5
BROKER_SYNC_KEY = "broker_sync:alpaca"
BROKER_SYNC_IN_FLIGHT_STATUSES = {"queued", "running", "leased"}
NEW_YORK = ZoneInfo("America/New_York")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _expires_in(minutes: int) -> str:
    return (datetime.now(UTC) + timedelta(minutes=max(minutes, 1))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _time_reached(time_value: str | None, *, now: datetime) -> bool:
    rendered = _as_text(time_value)
    if rendered is None:
        return False
    hour_text, separator, minute_text = rendered.partition(":")
    if separator != ":":
        return False
    current = now.astimezone(NEW_YORK)
    return (current.hour, current.minute) >= (int(hour_text), int(minute_text))


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
    latest_run_started_at_text = (
        None if latest_run_started_at is None else latest_run_started_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
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

    updated_at = parse_datetime(_as_text(state.get("updated_at")))
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
            "updated_at": None if updated_at is None else updated_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
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


def _calendar_close(session_date: str, market_calendar: str = "NYSE") -> datetime | None:
    session_day = datetime.fromisoformat(session_date).date()
    calendar = mcal.get_calendar(market_calendar)
    schedule = calendar.schedule(start_date=session_day.isoformat(), end_date=session_day.isoformat())
    if schedule.empty:
        return None
    return schedule.iloc[0]["market_close"].to_pydatetime().astimezone(UTC)


def normalize_exit_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("exit_policy") if isinstance(source.get("exit_policy"), dict) else source
    policy = dict(DEFAULT_EXIT_POLICY)
    if "enabled" in raw_policy:
        policy["enabled"] = _coerce_bool(raw_policy["enabled"])
    for key in ("profit_target_pct", "stop_multiple"):
        if key not in raw_policy:
            continue
        parsed = _coerce_float(raw_policy[key])
        if parsed is not None:
            policy[key] = parsed
    force_close_at = _as_text(raw_policy.get("force_close_at"))
    policy["force_close_at"] = force_close_at
    return policy


def resolve_exit_policy_snapshot(*, session_date: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    policy = normalize_exit_policy(payload)
    if policy["force_close_at"] is not None:
        return policy

    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("exit_policy") if isinstance(source.get("exit_policy"), dict) else source
    force_close_minutes = _coerce_int(raw_policy.get("force_close_minutes_before_close"))
    if force_close_minutes is None:
        force_close_minutes = DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE

    market_close = _calendar_close(session_date)
    if market_close is None:
        policy["force_close_at"] = None
        return policy
    force_close_at = market_close - timedelta(minutes=force_close_minutes)
    policy["force_close_at"] = force_close_at.isoformat(timespec="seconds").replace("+00:00", "Z")
    return policy


def _round_money(value: Any) -> float | None:
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    return round(parsed, 4)


def _profit_target_mark(
    *,
    entry_value: float | None,
    premium_kind: str | None,
    policy: dict[str, Any],
) -> float | None:
    if entry_value is None or premium_kind is None:
        return None
    profit_target_pct = _coerce_float(policy.get("profit_target_pct"))
    if profit_target_pct is None:
        return None
    if premium_kind == "debit":
        return round(entry_value * (1.0 + profit_target_pct), 4)
    return round(entry_value * max(1.0 - profit_target_pct, 0.0), 4)


def _stop_mark(
    *,
    entry_value: float | None,
    premium_kind: str | None,
    policy: dict[str, Any],
) -> float | None:
    if entry_value is None or premium_kind is None:
        return None
    stop_multiple = _coerce_float(policy.get("stop_multiple"))
    if stop_multiple is None:
        return None
    if premium_kind == "debit":
        return round(max(entry_value / max(stop_multiple, 1.0), 0.0), 4)
    return round(entry_value * stop_multiple, 4)


def _exit_policy_details(
    *,
    policy: dict[str, Any],
    mark: float | None,
    effective_mark: float | None,
    mark_state: str,
    entry_value: float | None,
    premium_kind: str | None,
) -> dict[str, Any]:
    return {
        "policy": dict(policy),
        "mark": _round_money(mark),
        "effective_mark": _round_money(effective_mark),
        "mark_state": mark_state,
        "entry_value": _round_money(entry_value),
        "premium_kind": premium_kind,
        "profit_target_mark": _profit_target_mark(
            entry_value=entry_value,
            premium_kind=premium_kind,
            policy=policy,
        ),
        "stop_mark": _stop_mark(
            entry_value=entry_value,
            premium_kind=premium_kind,
            policy=policy,
        ),
        "force_close_at": _as_text(policy.get("force_close_at")),
    }


def _resolve_effective_exit_mark(
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime,
) -> tuple[float | None, str]:
    if mark is None or mark <= 0:
        return None, "awaiting_mark"

    risk_policy = normalize_risk_policy(position.get("risk_policy"))
    stale_quote_after_seconds = _coerce_float(risk_policy.get("stale_quote_after_seconds"))
    if stale_quote_after_seconds is None:
        return mark, "mark"

    marked_at = parse_datetime(_as_text(position.get("close_marked_at")))
    if marked_at is None:
        return None, "awaiting_fresh_mark"

    age_seconds = (now - marked_at).total_seconds()
    if age_seconds > stale_quote_after_seconds:
        return None, "awaiting_fresh_mark"
    return mark, "mark"


def _resolve_force_close_limit_price(
    *,
    position: dict[str, Any],
    mark: float | None,
    fallback_mark: float | None,
) -> tuple[float | None, str | None]:
    if mark is not None and mark > 0:
        return round(max(mark, 0.01), 2), "mark"

    width = _coerce_float(position.get("width"))
    if width is not None and width > 0:
        return round(max(width, 0.01), 2), "width"

    if fallback_mark is not None and fallback_mark > 0:
        return round(max(fallback_mark, 0.01), 2), "stale_mark"
    return None, None


def evaluate_exit_policy(
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    policy = normalize_exit_policy(position.get("exit_policy"))
    force_close_at = parse_datetime(_as_text(policy.get("force_close_at")))
    current_time = now or datetime.now(UTC)
    remaining_quantity = _coerce_float(position.get("remaining_quantity")) or 0.0
    entry_value = _coerce_float(position.get("entry_credit")) or _coerce_float(position.get("entry_value"))
    premium_kind = _as_text(position.get("entry_value_kind")) or net_premium_kind(position.get("strategy") or position.get("strategy_family"))
    effective_mark, mark_state = _resolve_effective_exit_mark(
        position=position,
        mark=mark,
        now=current_time,
    )
    details = _exit_policy_details(
        policy=policy,
        mark=mark,
        effective_mark=effective_mark,
        mark_state=mark_state,
        entry_value=entry_value,
        premium_kind=premium_kind,
    )
    if remaining_quantity <= 0:
        return {
            "should_close": False,
            "reason": "no_remaining_quantity",
            "recipe_ref": None,
            **details,
        }
    if not policy["enabled"]:
        return {
            "should_close": False,
            "reason": "policy_disabled",
            "recipe_ref": None,
            **details,
        }
    if (
        entry_value is not None
        and effective_mark is not None
        and (
            effective_mark >= entry_value * (1.0 + float(policy["profit_target_pct"]))
            if premium_kind == "debit"
            else effective_mark <= entry_value * max(1.0 - float(policy["profit_target_pct"]), 0.0)
        )
    ):
        return {
            "should_close": True,
            "reason": "profit_target",
            "recipe_ref": None,
            "limit_price": round(max(effective_mark, 0.01), 2),
            "limit_price_source": "mark",
            **details,
        }
    if (
        entry_value is not None
        and effective_mark is not None
        and (
            effective_mark <= max(entry_value / max(float(policy["stop_multiple"]), 1.0), 0.0)
            if premium_kind == "debit"
            else effective_mark >= entry_value * float(policy["stop_multiple"])
        )
    ):
        return {
            "should_close": True,
            "reason": "stop_multiple",
            "recipe_ref": None,
            "limit_price": round(max(effective_mark, 0.01), 2),
            "limit_price_source": "mark",
            **details,
        }
    if force_close_at is not None and current_time >= force_close_at:
        limit_price, limit_price_source = _resolve_force_close_limit_price(
            position=position,
            mark=effective_mark,
            fallback_mark=mark,
        )
        if limit_price is None:
            return {
                "should_close": False,
                "reason": "awaiting_force_close_price",
                "recipe_ref": None,
                **details,
            }
        return {
            "should_close": True,
            "reason": "force_close",
            "recipe_ref": None,
            "limit_price": limit_price,
            "limit_price_source": limit_price_source,
            **details,
        }
    if effective_mark is None:
        return {
            "should_close": False,
            "reason": mark_state,
            "recipe_ref": None,
            **details,
        }
    return {
        "should_close": False,
        "reason": "hold",
        "recipe_ref": None,
        **details,
    }


def _close_source_payload(*, kind: str, decision: dict[str, Any]) -> dict[str, Any]:
    details = dict(decision.get("decision_details") or {}) if isinstance(decision.get("decision_details"), dict) else {}
    exit_context: dict[str, Any] = {}
    for key in (
        "mark",
        "effective_mark",
        "entry_value",
        "profit_target_mark",
        "stop_mark",
    ):
        rounded = _round_money(details.get(key))
        if rounded is not None:
            exit_context[key] = rounded
    for key in ("mark_state", "force_close_at"):
        text = _as_text(details.get(key))
        if text is not None:
            exit_context[key] = text

    payload: dict[str, Any] = {
        "kind": kind,
        "reason": _as_text(decision.get("reason")),
        "decision_source": _as_text(decision.get("decision_source")),
        "recipe_ref": _as_text(decision.get("recipe_ref")),
        "limit_price_source": _as_text(decision.get("limit_price_source")),
    }
    if exit_context:
        payload["exit_context"] = exit_context
    close_decision = decision.get("close_decision")
    if isinstance(close_decision, Mapping):
        payload["close_decision"] = dict(close_decision)
    return payload


def _close_decision_lifecycle(
    *,
    position: dict[str, Any],
    decision: Mapping[str, Any],
    decision_source: str | None = None,
    decided_at: str | None = None,
) -> dict[str, Any]:
    close_decision = decision.get("close_decision")
    if isinstance(close_decision, Mapping):
        return dict(close_decision)
    return build_close_decision_lifecycle(
        position=position,
        decision=decision,
        decision_source=decision_source,
        decided_at=decided_at,
    )


def _blocked_close_decision(
    *,
    position: dict[str, Any],
    reason: str,
    decision_source: str,
    decided_at: str | None = None,
) -> dict[str, Any]:
    return build_close_decision_lifecycle(
        position=position,
        decision={
            "should_close": False,
            "reason": reason,
            "recipe_ref": None,
            "limit_price": None,
            "limit_price_source": None,
            "decision_source": decision_source,
            "decision_details": None,
        },
        decision_source=decision_source,
        decided_at=decided_at,
    )


def _close_decision_row_fields(close_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "close_decision_id": close_decision.get("close_decision_id"),
        "close_decision_state": close_decision.get("decision_state"),
        "close_decision": dict(close_decision),
    }


def _has_open_close_attempt(execution_store: Any, position_id: str) -> bool:
    return bool(
        execution_store.list_open_attempts_for_position(
            position_id=position_id,
            statuses=sorted(OPEN_CLOSE_ATTEMPT_STATUSES),
        )
    )


def _close_intent_id(position_id: str, trading_strategy_id: str) -> str:
    return f"execution_intent:manage:{trading_strategy_id}:{position_id}"


def _close_slot_key(position_id: str) -> str:
    return f"manage:{position_id}:close"


def _has_active_close_intent(execution_store: Any, position_id: str) -> bool:
    if not execution_store.intent_schema_ready():
        return False
    from core.services.execution_intents.shared import ACTIVE_INTENT_STATES

    return bool(
        execution_store.list_execution_intents(
            slot_key=_close_slot_key(position_id),
            states=sorted(ACTIVE_INTENT_STATES),
            limit=1,
        )
    )


def _position_status(position: dict[str, Any]) -> str:
    return str(position.get("position_status") or position.get("status") or "").lower()


def _position_close_block_reason(position: dict[str, Any], *, now: datetime) -> str | None:
    status = _position_status(position)
    if status and status not in OPEN_POSITION_STATUSES:
        return "position_not_open"

    remaining_quantity = _coerce_float(position.get("remaining_quantity")) or 0.0
    if remaining_quantity <= 0:
        return "no_remaining_quantity"

    reconciliation_status = _as_text(position.get("reconciliation_status"))
    if reconciliation_status != "matched":
        return "awaiting_broker_reconciliation"

    last_reconciled_at = parse_datetime(_as_text(position.get("last_reconciled_at")))
    if last_reconciled_at is None:
        return "awaiting_broker_reconciliation"

    reconciliation_age_seconds = (now - last_reconciled_at.astimezone(UTC)).total_seconds()
    if reconciliation_age_seconds > CLOSE_RECONCILIATION_MAX_AGE_SECONDS:
        return "broker_reconciliation_stale"

    try:
        validate_close_execution(
            position=position,
            quantity=max(int(remaining_quantity), 1),
            now=now,
            max_reconciliation_age_seconds=CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
        )
    except ValueError as exc:
        error_text = str(exc)
        if "broker symbols" in error_text:
            return "close_symbols_missing"
        return "close_validation_blocked"
    return None


def _evaluate_position_close_decision(
    *,
    position: dict[str, Any],
    now: datetime,
    management_runtimes: tuple[Any, ...],
) -> tuple[dict[str, Any], str, Any | None]:
    runtime, runtime_reason = find_management_runtime_for_position(
        position,
        runtimes=management_runtimes,
    )
    if runtime is None:
        if runtime_reason == "ambiguous_management_runtime":
            return (
                {
                    "should_close": False,
                    "reason": "ambiguous_management_runtime",
                    "recipe_ref": None,
                    "limit_price": None,
                    "limit_price_source": None,
                    "decision_source": "management_runtime",
                    "management_recipe_refs": [],
                    "decision_details": None,
                },
                "management_runtime",
                None,
            )
        policy_decision = evaluate_exit_policy(
            position=position,
            mark=_coerce_float(position.get("close_mark")),
            now=now,
        )
        policy_decision["decision_source"] = "position_exit_policy"
        policy_decision["management_recipe_refs"] = []
        policy_decision["decision_details"] = {
            key: value
            for key, value in policy_decision.items()
            if key
            in {
                "policy",
                "mark",
                "effective_mark",
                "mark_state",
                "entry_value",
                "premium_kind",
                "profit_target_mark",
                "stop_mark",
                "force_close_at",
            }
        }
        return (
            policy_decision,
            "position_exit_policy",
            None,
        )
    if runtime.strategy.management is None or not routine_should_run_now(runtime.strategy.management, now=now):
        return (
            {
                "should_close": False,
                "reason": "outside_management_schedule_window",
                "recipe_ref": None,
                "limit_price": None,
                "limit_price_source": None,
                "decision_source": "management_runtime",
                "management_recipe_refs": list(runtime.management_recipe_refs),
                "decision_details": None,
            },
            "management_runtime",
            runtime,
        )

    from core.services.management_planner import plan_position_management

    decision = plan_position_management(
        runtime=runtime,
        position=position,
        flatten_due=_time_reached(runtime.strategy.runtime.flatten_positions_at_et, now=now),
        now=now,
    )
    decision["decision_source"] = "management_runtime"
    decision["management_recipe_refs"] = list(runtime.management_recipe_refs)
    return (decision, "management_runtime", runtime)


def describe_position_exit_state(
    *,
    position: dict[str, Any],
    now: datetime | None = None,
    management_runtimes: tuple[Any, ...] | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(UTC)
    runtimes = tuple(resolve_management_runtimes()) if management_runtimes is None else tuple(management_runtimes)
    decision, _decision_source, _runtime = _evaluate_position_close_decision(
        position=position,
        now=current_time,
        management_runtimes=runtimes,
    )
    close_decision = _close_decision_lifecycle(
        position=position,
        decision=decision,
        decision_source=_decision_source,
        decided_at=current_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
    )
    details = dict(decision.get("decision_details") or {}) if isinstance(decision.get("decision_details"), dict) else {}
    if not details:
        fallback = evaluate_exit_policy(
            position=position,
            mark=_coerce_float(position.get("close_mark")),
            now=current_time,
        )
        details = {
            key: value
            for key, value in fallback.items()
            if key
            in {
                "policy",
                "mark",
                "effective_mark",
                "mark_state",
                "entry_value",
                "premium_kind",
                "profit_target_mark",
                "stop_mark",
                "force_close_at",
            }
        }
    return {
        "decision_source": _as_text(decision.get("decision_source")),
        "management_recipe_refs": [str(value) for value in list(decision.get("management_recipe_refs") or []) if str(value or "").strip()],
        "should_close": bool(decision.get("should_close")),
        "reason": str(decision.get("reason") or "unknown"),
        "close_decision_state": close_decision.get("decision_state"),
        "close_decision_id": close_decision.get("close_decision_id"),
        "close_decision": close_decision,
        "recipe_ref": _as_text(decision.get("recipe_ref")),
        "limit_price": _coerce_float(decision.get("limit_price")),
        "limit_price_source": _as_text(decision.get("limit_price_source")),
        "current_mark": _round_money(details.get("mark")),
        "effective_mark": _round_money(details.get("effective_mark")),
        "mark_state": _as_text(details.get("mark_state")),
        "entry_value": _round_money(details.get("entry_value")),
        "premium_kind": _as_text(details.get("premium_kind")),
        "profit_target_mark": _round_money(details.get("profit_target_mark")),
        "stop_mark": _round_money(details.get("stop_mark")),
        "force_close_at": _as_text(details.get("force_close_at")),
    }


def _create_managed_close_intent(
    execution_store: Any,
    *,
    position: dict[str, Any],
    runtime: Any,
    decision: dict[str, Any],
) -> dict[str, Any]:
    from core.services.execution_intents.shared import issue_pending_execution_intent

    position_id = str(position["position_id"])
    close_decision = _close_decision_lifecycle(
        position=position,
        decision=decision,
        decision_source=_as_text(decision.get("decision_source")),
    )
    decision = {**decision, "close_decision": close_decision}
    return issue_pending_execution_intent(
        execution_store,
        execution_intent_id=_close_intent_id(position_id, runtime.trading_strategy_id),
        trading_strategy_id=runtime.trading_strategy_id,
        strategy_position_id=position_id,
        execution_attempt_id=None,
        action_type="close",
        slot_key=_close_slot_key(position_id),
        claim_token=None,
        policy_ref={
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_structure": runtime.trade_structure,
            "routine": "manage",
        },
        config_hash=runtime.config_hash,
        state="pending",
        expires_at=_expires_in(MANAGED_CLOSE_INTENT_TTL_MINUTES),
        superseded_by_id=None,
        payload={
            "position_id": position_id,
            "limit_price": decision.get("limit_price"),
            "limit_price_source": decision.get("limit_price_source"),
            "reason": decision.get("reason"),
            "recipe_ref": decision.get("recipe_ref"),
            "close_decision": close_decision,
            "decision_source": decision.get("decision_source"),
            "decision_details": dict(decision.get("decision_details") or {}),
            "source": _close_source_payload(
                kind="management_runtime_exit",
                decision=decision,
            ),
            "execution_mode": runtime.strategy.execution.mode,
            "approval_mode": runtime.strategy.execution.approval,
            "execution_runtime": runtime.strategy.execution.runtime,
        },
        created_event_payload={
            "position_id": position_id,
            "reason": decision.get("reason"),
            "recipe_ref": decision.get("recipe_ref"),
            "close_decision_id": close_decision.get("close_decision_id"),
            "close_decision_state": close_decision.get("decision_state"),
            "limit_price": decision.get("limit_price"),
            "execution_runtime": runtime.strategy.execution.runtime,
        },
    )


def _refresh_open_position_marks(*, db_target: str, session_ids: list[str], storage: Any | None = None) -> None:
    refresh_session_position_marks(
        db_target=db_target,
        session_ids=session_ids,
        storage=storage,
    )


@with_storage()
def run_position_exit_manager(
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
    portfolio_engine = PostgresPortfolioEngine(
        execution_store=execution_store,
        now=now,
        management_runtimes=management_runtimes,
    )
    portfolio_run_ref = build_portfolio_run_ref(
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
            "portfolio_engine": {
                "run_id": portfolio_run_ref.run_id,
            },
            "position_count": 0,
            "evaluated": 0,
            "created_intents": 0,
            "submitted": 0,
            "skipped": 0,
            "failure_count": 0,
            "open_attempt_guard": open_attempt_guard,
            "broker_sync": broker_sync,
        }
    if broker_sync.get("status") != "current":
        broker_reason = str(broker_sync.get("reason") or "broker_sync_not_current")
        broker_decisions: list[dict[str, Any]] = []
        decided_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
        for position in open_positions[:25]:
            close_decision = _blocked_close_decision(
                position=position,
                reason=broker_reason,
                decision_source="broker_sync",
                decided_at=decided_at,
            )
            broker_decisions.append(
                {
                    "position_id": position.get("position_id"),
                    "reason": broker_reason,
                    "decision_source": "broker_sync",
                    "should_close": False,
                    "portfolio_run_id": portfolio_run_ref.run_id,
                    **_close_decision_row_fields(close_decision),
                }
            )
        return {
            "status": "skipped",
            "reason": broker_reason,
            "portfolio_engine": {
                "run_id": portfolio_run_ref.run_id,
            },
            "position_count": len(open_positions),
            "evaluated": 0,
            "created_intents": 0,
            "submitted": 0,
            "skipped": len(open_positions),
            "failure_count": 0,
            "decisions": broker_decisions,
            "failures": [],
            "open_attempt_guard": open_attempt_guard,
            "broker_sync": broker_sync,
        }

    try:
        from core.services.execution import run_open_execution_guard

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
    submitted = 0
    skipped = 0
    failures: list[dict[str, str]] = []
    decisions: list[dict[str, Any]] = []
    now_iso = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    for position_snapshot in refreshed_position_snapshots:
        position = dict(position_snapshot.payload)
        position_id = str(position_snapshot.position_id)
        latest_position = execution_store.get_position(position_id)
        if latest_position is not None:
            position = enrich_position_row(dict(latest_position))
            position_snapshot = build_position_snapshot(position)

        if _has_open_close_attempt(execution_store, position_id):
            evaluated += 1
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason="close_already_open",
                updated_at=_utc_now(),
            )
            close_decision = _blocked_close_decision(
                position=position,
                reason="close_already_open",
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions.append(
                {
                    "position_id": position_id,
                    "reason": "close_already_open",
                    "decision_source": "close_guard",
                    "should_close": False,
                    "portfolio_run_id": portfolio_run_ref.run_id,
                    **_close_decision_row_fields(close_decision),
                }
            )
            continue

        close_block_reason = _position_close_block_reason(position, now=now)
        if close_block_reason is not None:
            evaluated += 1
            skipped += 1
            if _position_status(position) in OPEN_POSITION_STATUSES:
                execution_store.update_position(
                    position_id=position_id,
                    last_exit_evaluated_at=_utc_now(),
                    last_exit_reason=close_block_reason,
                    updated_at=_utc_now(),
                )
            close_decision = _blocked_close_decision(
                position=position,
                reason=close_block_reason,
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions.append(
                {
                    "position_id": position_id,
                    "reason": close_block_reason,
                    "decision_source": "close_guard",
                    "should_close": False,
                    "portfolio_run_id": portfolio_run_ref.run_id,
                    **_close_decision_row_fields(close_decision),
                }
            )
            continue

        close_result = portfolio_engine.evaluate_close(
            run_ref=portfolio_run_ref,
            position=position_snapshot,
        )
        decision = dict(close_result.payload.get("decision") or {})
        decision_source = _as_text(close_result.payload.get("decision_source")) or "portfolio_engine"
        management_runtime = close_result.payload.get("management_runtime")
        close_decision = dict(close_result.payload.get("close_decision") or decision.get("close_decision") or {})
        evaluated += 1
        execution_store.update_position(
            position_id=position_id,
            last_exit_evaluated_at=_utc_now(),
            last_exit_reason=str(decision["reason"]),
            updated_at=_utc_now(),
        )
        decisions.append(
            {
                "position_id": position_id,
                "reason": decision["reason"],
                "decision_source": decision_source,
                "should_close": bool(decision["should_close"]),
                "portfolio_run_id": portfolio_run_ref.run_id,
                **_close_decision_row_fields(close_decision),
            }
        )
        if not decision["should_close"]:
            skipped += 1
            continue

        latest_position = execution_store.get_position(position_id)
        if latest_position is not None:
            position = enrich_position_row(dict(latest_position))
            position_snapshot = build_position_snapshot(position)
        close_block_reason = _position_close_block_reason(position, now=now)
        if close_block_reason is not None:
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason=close_block_reason,
                updated_at=_utc_now(),
            )
            close_decision = _blocked_close_decision(
                position=position,
                reason=close_block_reason,
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions[-1]["reason"] = close_block_reason
            decisions[-1]["decision_source"] = "close_guard"
            decisions[-1]["should_close"] = False
            decisions[-1].update(_close_decision_row_fields(close_decision))
            continue

        if management_runtime is not None:
            if not execution_store.intent_schema_ready():
                skipped += 1
                execution_store.update_position(
                    position_id=position_id,
                    last_exit_evaluated_at=_utc_now(),
                    last_exit_reason="execution_intent_schema_unavailable",
                    updated_at=_utc_now(),
                )
                close_decision = _blocked_close_decision(
                    position=position,
                    reason="execution_intent_schema_unavailable",
                    decision_source="close_guard",
                    decided_at=now_iso,
                )
                decisions[-1]["reason"] = "execution_intent_schema_unavailable"
                decisions[-1]["decision_source"] = "close_guard"
                decisions[-1]["should_close"] = False
                decisions[-1].update(_close_decision_row_fields(close_decision))
                continue
            if _has_active_close_intent(execution_store, position_id):
                skipped += 1
                execution_store.update_position(
                    position_id=position_id,
                    last_exit_evaluated_at=_utc_now(),
                    last_exit_reason="close_intent_already_open",
                    updated_at=_utc_now(),
                )
                close_decision = _blocked_close_decision(
                    position=position,
                    reason="close_intent_already_open",
                    decision_source="close_guard",
                    decided_at=now_iso,
                )
                decisions[-1]["reason"] = "close_intent_already_open"
                decisions[-1]["decision_source"] = "close_guard"
                decisions[-1]["should_close"] = False
                decisions[-1].update(_close_decision_row_fields(close_decision))
                continue
            try:
                _create_managed_close_intent(
                    execution_store,
                    position=position,
                    runtime=management_runtime,
                    decision=decision,
                )
                created_intents += 1
            except Exception as exc:
                failures.append(
                    {
                        "position_id": position_id,
                        "error": str(exc),
                    }
                )
            continue
        try:
            from core.services.execution import submit_position_close_by_id

            submit_position_close_by_id(
                db_target=db_target,
                position_id=position_id,
                limit_price=float(decision["limit_price"]),
                request_metadata={
                    "close_decision": close_decision,
                    "source": _close_source_payload(
                        kind="exit_manager",
                        decision=decision,
                    ),
                },
                storage=storage,
            )
            submitted += 1
        except Exception as exc:
            failures.append(
                {
                    "position_id": position_id,
                    "error": str(exc),
                }
            )

    return {
        "status": "degraded" if failures or open_attempt_guard.get("status") == "degraded" else "ok",
        "portfolio_engine": {
            "run_id": portfolio_run_ref.run_id,
        },
        "position_count": len(refreshed_positions),
        "evaluated": evaluated,
        "created_intents": created_intents,
        "submitted": submitted,
        "skipped": skipped,
        "failure_count": len(failures),
        "decisions": decisions[:25],
        "failures": failures[:25],
        "open_attempt_guard": open_attempt_guard,
        "broker_sync": broker_sync,
    }

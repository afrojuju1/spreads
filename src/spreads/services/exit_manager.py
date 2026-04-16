from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas_market_calendars as mcal

from spreads.db.decorators import with_storage
from spreads.services.execution_portfolio import refresh_session_position_marks
from spreads.services.option_structures import net_premium_kind
from spreads.services.positions import enrich_position_row
from spreads.services.risk_manager import normalize_risk_policy
from spreads.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ["open", "partial_close"]
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


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


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


def _calendar_close(
    session_date: str, market_calendar: str = "NYSE"
) -> datetime | None:
    session_day = datetime.fromisoformat(session_date).date()
    calendar = mcal.get_calendar(market_calendar)
    schedule = calendar.schedule(
        start_date=session_day.isoformat(), end_date=session_day.isoformat()
    )
    if schedule.empty:
        return None
    return schedule.iloc[0]["market_close"].to_pydatetime().astimezone(UTC)


def normalize_exit_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = (
        source.get("exit_policy")
        if isinstance(source.get("exit_policy"), dict)
        else source
    )
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


def resolve_exit_policy_snapshot(
    *, session_date: str, payload: dict[str, Any] | None
) -> dict[str, Any]:
    policy = normalize_exit_policy(payload)
    if policy["force_close_at"] is not None:
        return policy

    source = payload if isinstance(payload, dict) else {}
    raw_policy = (
        source.get("exit_policy")
        if isinstance(source.get("exit_policy"), dict)
        else source
    )
    force_close_minutes = _coerce_int(
        raw_policy.get("force_close_minutes_before_close")
    )
    if force_close_minutes is None:
        force_close_minutes = DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE

    market_close = _calendar_close(session_date)
    if market_close is None:
        policy["force_close_at"] = None
        return policy
    force_close_at = market_close - timedelta(minutes=force_close_minutes)
    policy["force_close_at"] = force_close_at.isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    return policy


def _resolve_effective_exit_mark(
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime,
) -> tuple[float | None, str]:
    if mark is None or mark <= 0:
        return None, "awaiting_mark"

    risk_policy = normalize_risk_policy(position.get("risk_policy"))
    stale_quote_after_seconds = _coerce_float(
        risk_policy.get("stale_quote_after_seconds")
    )
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
    if remaining_quantity <= 0:
        return {"should_close": False, "reason": "no_remaining_quantity"}
    if not policy["enabled"]:
        return {"should_close": False, "reason": "policy_disabled"}

    effective_mark, mark_state = _resolve_effective_exit_mark(
        position=position,
        mark=mark,
        now=current_time,
    )
    entry_value = _coerce_float(position.get("entry_credit")) or _coerce_float(
        position.get("entry_value")
    )
    premium_kind = _as_text(position.get("entry_value_kind")) or net_premium_kind(
        position.get("strategy") or position.get("strategy_family")
    )
    if (
        entry_value is not None
        and effective_mark is not None
        and (
            effective_mark >= entry_value * (1.0 + float(policy["profit_target_pct"]))
            if premium_kind == "debit"
            else effective_mark
            <= entry_value * max(1.0 - float(policy["profit_target_pct"]), 0.0)
        )
    ):
        return {
            "should_close": True,
            "reason": "profit_target",
            "limit_price": round(max(effective_mark, 0.01), 2),
            "limit_price_source": "mark",
        }
    if (
        entry_value is not None
        and effective_mark is not None
        and (
            effective_mark
            <= max(entry_value / max(float(policy["stop_multiple"]), 1.0), 0.0)
            if premium_kind == "debit"
            else effective_mark >= entry_value * float(policy["stop_multiple"])
        )
    ):
        return {
            "should_close": True,
            "reason": "stop_multiple",
            "limit_price": round(max(effective_mark, 0.01), 2),
            "limit_price_source": "mark",
        }
    if force_close_at is not None and current_time >= force_close_at:
        limit_price, limit_price_source = _resolve_force_close_limit_price(
            position=position,
            mark=effective_mark,
            fallback_mark=mark,
        )
        if limit_price is None:
            return {"should_close": False, "reason": "awaiting_force_close_price"}
        return {
            "should_close": True,
            "reason": "force_close",
            "limit_price": limit_price,
            "limit_price_source": limit_price_source,
        }
    if effective_mark is None:
        return {"should_close": False, "reason": mark_state}
    return {"should_close": False, "reason": "hold"}


def _has_open_close_attempt(execution_store: Any, position_id: str) -> bool:
    return bool(
        execution_store.list_open_attempts_for_position(
            position_id=position_id,
            statuses=sorted(OPEN_CLOSE_ATTEMPT_STATUSES),
        )
    )


def _is_bot_managed_position(execution_store: Any, position: dict[str, Any]) -> bool:
    if not execution_store.intent_schema_ready():
        return False
    open_execution_attempt_id = _as_text(position.get("open_execution_attempt_id"))
    if open_execution_attempt_id is None:
        return False
    attempt = execution_store.get_attempt(open_execution_attempt_id)
    if attempt is None:
        return False
    request = attempt.get("request")
    if not isinstance(request, dict):
        return False
    execution_intent_id = _as_text(request.get("execution_intent_id"))
    if execution_intent_id is None:
        return False
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        return False
    return _as_text(intent.get("bot_id")) is not None


def _refresh_open_position_marks(
    *, db_target: str, session_ids: list[str], storage: Any | None = None
) -> None:
    refresh_session_position_marks(
        db_target=db_target,
        session_ids=session_ids,
        storage=storage,
    )


@with_storage()
def run_position_exit_manager(
    *,
    db_target: str,
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

    try:
        from spreads.services.execution import run_open_execution_guard

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

    open_positions = [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
            statuses=OPEN_POSITION_STATUSES,
            limit=200,
        )
    ]
    if not open_positions:
        return {
            "status": "degraded"
            if open_attempt_guard.get("status") == "degraded"
            else "ok",
            "position_count": 0,
            "evaluated": 0,
            "submitted": 0,
            "skipped": 0,
            "failure_count": 0,
            "open_attempt_guard": open_attempt_guard,
        }

    _refresh_open_position_marks(
        db_target=db_target,
        session_ids=sorted(
            {str(position["session_id"]) for position in open_positions}
        ),
        storage=storage,
    )
    refreshed_positions = [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
            statuses=OPEN_POSITION_STATUSES,
            limit=200,
        )
    ]

    evaluated = 0
    submitted = 0
    skipped = 0
    failures: list[dict[str, str]] = []
    decisions: list[dict[str, Any]] = []
    now = datetime.now(UTC)
    for position in refreshed_positions:
        position_id = str(position["position_id"])
        if _is_bot_managed_position(execution_store, position):
            evaluated += 1
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason="bot_runtime_managed",
                updated_at=_utc_now(),
            )
            decisions.append(
                {
                    "position_id": position_id,
                    "reason": "bot_runtime_managed",
                    "should_close": False,
                }
            )
            continue
        if _has_open_close_attempt(execution_store, position_id):
            evaluated += 1
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason="close_already_open",
                updated_at=_utc_now(),
            )
            decisions.append(
                {
                    "position_id": position_id,
                    "reason": "close_already_open",
                    "should_close": False,
                }
            )
            continue

        decision = evaluate_exit_policy(
            position=position,
            mark=_coerce_float(position.get("close_mark")),
            now=now,
        )
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
                "should_close": bool(decision["should_close"]),
            }
        )
        if not decision["should_close"]:
            skipped += 1
            continue
        try:
            from spreads.services.execution import submit_position_close_by_id

            submit_position_close_by_id(
                db_target=db_target,
                position_id=position_id,
                limit_price=float(decision["limit_price"]),
                request_metadata={
                    "source": {
                        "kind": "exit_manager",
                        "reason": decision["reason"],
                        "limit_price_source": _as_text(
                            decision.get("limit_price_source")
                        ),
                    }
                },
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
        "status": "degraded"
        if failures or open_attempt_guard.get("status") == "degraded"
        else "ok",
        "position_count": len(refreshed_positions),
        "evaluated": evaluated,
        "submitted": submitted,
        "skipped": skipped,
        "failure_count": len(failures),
        "decisions": decisions[:25],
        "failures": failures[:25],
        "open_attempt_guard": open_attempt_guard,
    }

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas_market_calendars as mcal

from core.services.option_structures import net_premium_kind
from core.value_coercion import as_text, coerce_bool, coerce_float, coerce_int
from core.storage.serializers import parse_datetime

DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE = 10
DEFAULT_EXIT_POLICY = {
    "enabled": True,
    "profit_target_pct": 0.5,
    "stop_multiple": 2.0,
    "force_close_at": None,
}


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
        policy["enabled"] = coerce_bool(raw_policy["enabled"], default=False)
    for key in ("profit_target_pct", "stop_multiple"):
        if key not in raw_policy:
            continue
        parsed = coerce_float(raw_policy[key])
        if parsed is not None:
            policy[key] = parsed
    policy["force_close_at"] = as_text(raw_policy.get("force_close_at"))
    return policy


def resolve_exit_policy_snapshot(*, session_date: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    policy = normalize_exit_policy(payload)
    if policy["force_close_at"] is not None:
        return policy

    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("exit_policy") if isinstance(source.get("exit_policy"), dict) else source
    force_close_minutes = coerce_int(raw_policy.get("force_close_minutes_before_close"))
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
    parsed = coerce_float(value)
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
    profit_target_pct = coerce_float(policy.get("profit_target_pct"))
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
    stop_multiple = coerce_float(policy.get("stop_multiple"))
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
        "force_close_at": as_text(policy.get("force_close_at")),
    }


def _resolve_effective_exit_mark(
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime,
) -> tuple[float | None, str]:
    if mark is None or mark <= 0:
        return None, "awaiting_mark"

    risk_policy = position.get("risk_policy") if isinstance(position.get("risk_policy"), dict) else {}
    stale_quote_after_seconds = coerce_float(risk_policy.get("stale_quote_after_seconds"))
    if stale_quote_after_seconds is None:
        return mark, "mark"

    marked_at = parse_datetime(as_text(position.get("close_marked_at")))
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

    width = coerce_float(position.get("width"))
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
    force_close_at = parse_datetime(as_text(policy.get("force_close_at")))
    current_time = now or datetime.now(UTC)
    remaining_quantity = coerce_float(position.get("remaining_quantity")) or 0.0
    entry_value = coerce_float(position.get("entry_credit")) or coerce_float(position.get("entry_value"))
    premium_kind = as_text(position.get("entry_value_kind")) or net_premium_kind(position.get("strategy") or position.get("strategy_family"))
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


__all__ = [
    "DEFAULT_EXIT_POLICY",
    "DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE",
    "evaluate_exit_policy",
    "normalize_exit_policy",
    "resolve_exit_policy_snapshot",
]

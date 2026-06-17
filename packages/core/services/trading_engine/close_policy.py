from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas_market_calendars as mcal

from core.money import option_limit_price, premium_float
from core.services.option_structures import net_premium_kind
from core.services.trading_strategy_exit_models import (
    DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE,
    ExitControllerPolicy,
)
from core.value_coercion import as_text, coerce_bool, coerce_float, coerce_int, utc_iso
from core.storage.serializers import parse_datetime

DEFAULT_EXIT_POLICY = ExitControllerPolicy().to_exit_policy_payload()


def _calendar_close(session_date: str, market_calendar: str = "NYSE") -> datetime | None:
    session_day = datetime.fromisoformat(session_date).date()
    calendar = mcal.get_calendar(market_calendar)
    schedule = calendar.schedule(start_date=session_day.isoformat(), end_date=session_day.isoformat())
    if schedule.empty:
        return None
    return schedule.iloc[0]["market_close"].to_pydatetime().astimezone(UTC)


def normalize_exit_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    return ExitControllerPolicy.model_validate(payload or {}).to_exit_policy_payload()


def resolve_exit_policy_snapshot(*, session_date: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    policy = normalize_exit_policy(payload)
    if policy.get("force_close_at") is not None:
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
    policy["force_close_at"] = utc_iso(force_close_at)
    return policy


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
        return premium_float(entry_value * (1.0 + profit_target_pct))
    return premium_float(entry_value * max(1.0 - profit_target_pct, 0.0))


def _stop_mark(
    *,
    entry_value: float | None,
    premium_kind: str | None,
    policy: dict[str, Any],
) -> float | None:
    if entry_value is None or premium_kind is None:
        return None
    stop_loss_pct = coerce_float(policy.get("stop_loss_pct"))
    if stop_loss_pct is not None:
        if stop_loss_pct < 0:
            return None
        if premium_kind == "debit":
            return premium_float(entry_value * max(1.0 - min(stop_loss_pct, 1.0), 0.0))
        return premium_float(entry_value * (1.0 + stop_loss_pct))
    stop_multiple = coerce_float(policy.get("stop_multiple"))
    if stop_multiple is None:
        return None
    if premium_kind == "debit":
        return premium_float(max(entry_value / max(stop_multiple, 1.0), 0.0))
    return premium_float(entry_value * stop_multiple)


def _stop_reason(policy: dict[str, Any]) -> str:
    return "stop_loss" if coerce_float(policy.get("stop_loss_pct")) is not None else "stop_multiple"


def _quote_spread_details(
    *,
    position: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    max_spread_pct = coerce_float(policy.get("max_spread_pct"))
    if max_spread_pct is None:
        return {
            "quote_spread_pct": None,
            "quote_spread_state": "not_configured",
            "max_spread_pct": None,
        }
    spread_pct = None
    for key in ("close_quote_spread_pct", "quote_spread_pct", "spread_pct"):
        spread_pct = coerce_float(position.get(key))
        if spread_pct is not None:
            break
    if spread_pct is None:
        return {
            "quote_spread_pct": None,
            "quote_spread_state": "unavailable",
            "max_spread_pct": max_spread_pct,
        }
    return {
        "quote_spread_pct": spread_pct,
        "quote_spread_state": "too_wide" if spread_pct > max_spread_pct else "ok",
        "max_spread_pct": max_spread_pct,
    }


def _underlying_setup(position: dict[str, Any]) -> dict[str, Any]:
    for key in ("underlying_setup", "setup", "opening_signal_setup"):
        value = position.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    evidence = position.get("opening_signal_evidence")
    if isinstance(evidence, Mapping):
        setup = evidence.get("setup") or evidence.get("setup_context")
        if isinstance(setup, Mapping):
            return dict(setup)
    return {}


def _underlying_invalidation_details(
    *,
    position: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    raw_config = policy.get("underlying_invalidation")
    if not isinstance(raw_config, Mapping):
        return {
            "underlying_invalidation_state": "not_configured",
            "underlying_invalidation_reason": None,
        }
    config = dict(raw_config)
    if not coerce_bool(config.get("enabled"), default=True):
        return {
            "underlying_invalidation_state": "disabled",
            "underlying_invalidation_reason": None,
        }
    setup = _underlying_setup(position)
    status = (as_text(setup.get("setup_status")) or as_text(position.get("setup_status")) or "").lower()
    explicit_state = (
        as_text(position.get("underlying_invalidation_state"))
        or as_text(setup.get("underlying_invalidation_state"))
        or as_text(setup.get("invalidation_state"))
    )
    if explicit_state is not None and explicit_state.lower() in {"invalidated", "breakdown_confirmed", "failed_reclaim"}:
        return {
            "underlying_invalidation_state": "invalidated",
            "underlying_invalidation_reason": explicit_state.lower(),
        }
    latest_close = coerce_float(setup.get("latest_close") or setup.get("setup_latest_close") or position.get("setup_latest_close"))
    opening_range_low = coerce_float(
        setup.get("opening_range_low") or setup.get("setup_opening_range_low") or position.get("setup_opening_range_low")
    )
    tolerance_bps = coerce_float(config.get("breakdown_tolerance_bps")) or 0.0
    if latest_close is not None and opening_range_low is not None:
        tolerance_multiplier = 1.0 - max(tolerance_bps, 0.0) / 10_000.0
        if latest_close < opening_range_low * tolerance_multiplier:
            return {
                "underlying_invalidation_state": "invalidated",
                "underlying_invalidation_reason": "opening_range_breakdown",
                "setup_latest_close": latest_close,
                "setup_opening_range_low": opening_range_low,
            }
    if status == "unfavorable":
        return {
            "underlying_invalidation_state": "invalidated",
            "underlying_invalidation_reason": "setup_unfavorable",
        }
    if not setup:
        return {
            "underlying_invalidation_state": "unavailable",
            "underlying_invalidation_reason": "underlying_setup_missing",
        }
    return {
        "underlying_invalidation_state": "valid",
        "underlying_invalidation_reason": None,
    }


def _exit_policy_details(
    *,
    policy: dict[str, Any],
    position: dict[str, Any],
    mark: float | None,
    effective_mark: float | None,
    mark_state: str,
    entry_value: float | None,
    premium_kind: str | None,
) -> dict[str, Any]:
    return {
        "policy": dict(policy),
        "mark": premium_float(mark),
        "effective_mark": premium_float(effective_mark),
        "mark_state": mark_state,
        "entry_value": premium_float(entry_value),
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
        "max_quote_age_seconds": coerce_int(policy.get("max_quote_age_seconds")),
        **_quote_spread_details(position=position, policy=policy),
        **_underlying_invalidation_details(position=position, policy=policy),
    }


def _resolve_effective_exit_mark(
    *,
    position: dict[str, Any],
    policy: dict[str, Any],
    mark: float | None,
    now: datetime,
) -> tuple[float | None, str]:
    if mark is None or mark <= 0:
        return None, "awaiting_mark"

    risk_policy = position.get("risk_policy") if isinstance(position.get("risk_policy"), dict) else {}
    stale_thresholds = [
        value
        for value in (
            coerce_float(risk_policy.get("stale_quote_after_seconds")),
            coerce_float(policy.get("max_quote_age_seconds")),
        )
        if value is not None
    ]
    if not stale_thresholds:
        return mark, "mark"

    marked_at = parse_datetime(as_text(position.get("close_marked_at")))
    if marked_at is None:
        return None, "awaiting_fresh_mark"

    age_seconds = (now - marked_at).total_seconds()
    if age_seconds > min(stale_thresholds):
        return None, "awaiting_fresh_mark"
    return mark, "mark"


def _resolve_force_close_limit_price(
    *,
    position: dict[str, Any],
    mark: float | None,
    fallback_mark: float | None,
) -> tuple[float | None, str | None]:
    if mark is not None and mark > 0:
        return option_limit_price(mark), "mark"

    width = coerce_float(position.get("width"))
    if width is not None and width > 0:
        return option_limit_price(width), "width"

    if fallback_mark is not None and fallback_mark > 0:
        return option_limit_price(fallback_mark), "stale_mark"
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
        policy=policy,
        mark=mark,
        now=current_time,
    )
    details = _exit_policy_details(
        policy=policy,
        position=position,
        mark=mark,
        effective_mark=effective_mark,
        mark_state=mark_state,
        entry_value=entry_value,
        premium_kind=premium_kind,
    )
    stop_mark = premium_float(details.get("stop_mark"))
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
    if details.get("underlying_invalidation_state") == "invalidated":
        limit_price, limit_price_source = _resolve_force_close_limit_price(
            position=position,
            mark=effective_mark,
            fallback_mark=mark,
        )
        if limit_price is None:
            return {
                "should_close": False,
                "reason": "awaiting_underlying_invalidation_price",
                "recipe_ref": None,
                **details,
            }
        return {
            "should_close": True,
            "reason": "underlying_invalidation",
            "recipe_ref": None,
            "limit_price": limit_price,
            "limit_price_source": limit_price_source,
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
            "limit_price": option_limit_price(effective_mark),
            "limit_price_source": "mark",
            **details,
        }
    if (
        effective_mark is not None
        and stop_mark is not None
        and (effective_mark <= stop_mark if premium_kind == "debit" else effective_mark >= stop_mark)
    ):
        return {
            "should_close": True,
            "reason": _stop_reason(policy),
            "recipe_ref": None,
            "limit_price": option_limit_price(effective_mark),
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

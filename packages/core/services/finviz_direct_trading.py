from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, time, timedelta
import math
import re
from typing import Any
from zoneinfo import ZoneInfo

from core.common import parse_float, parse_int
from core.services.alpaca import create_alpaca_client_from_env
from core.services.execution.runtimes import NAUTILUS_RUNTIME
from core.services.execution_intents import dispatch_pending_execution_intents
from core.services.execution_intents.shared import (
    ACTIVE_INTENT_STATES,
    issue_pending_execution_intent,
)
from core.services.symbol_feeds import get_latest_symbol_feed_snapshot
from core.storage.serializers import parse_datetime


NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_FEED_ID = "finviz_momentum"
DEFAULT_FEED_JOB_KEY = "symbol_feed:finviz_momentum"
OPEN_POSITION_STATUSES = ["open", "partial_open", "partial_close", "pending_open"]


def _now() -> datetime:
    return datetime.now(UTC)


def _utc_now() -> str:
    return _now().isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    rendered = str(value).strip().lower()
    if rendered in {"1", "true", "yes", "y", "on"}:
        return True
    if rendered in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _as_int(value: Any, default: int) -> int:
    parsed = parse_int(value)
    return default if parsed is None else int(parsed)


def _as_float(value: Any) -> float | None:
    return parse_float(value)


def _pct(value: Any) -> float | None:
    parsed = _as_float(value)
    if parsed is None:
        return None
    if abs(parsed) > 1.0:
        return parsed / 100.0
    return parsed


def _safe_component(value: Any) -> str:
    rendered = str(value or "").strip()
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", rendered) or "unknown"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _rule_value(
    rules: Mapping[str, Any],
    payload: Mapping[str, Any],
    key: str,
    default: Any = None,
) -> Any:
    if key in rules:
        return rules[key]
    return payload.get(key, default)


def _clock_is_open(clock: Mapping[str, Any]) -> bool:
    value = clock.get("is_open")
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _nested_mapping(payload: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _quote(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return _nested_mapping(snapshot, "latestQuote", "latest_quote", "quote")


def _trade(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return _nested_mapping(snapshot, "latestTrade", "latest_trade", "trade")


def _quote_metrics(snapshot: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    quote = _quote(snapshot)
    trade = _trade(snapshot)
    bid = _as_float(quote.get("bp") or quote.get("bid_price"))
    ask = _as_float(quote.get("ap") or quote.get("ask_price"))
    quote_ts = parse_datetime(quote.get("t") or quote.get("timestamp"))
    trade_ts = parse_datetime(trade.get("t") or trade.get("timestamp"))
    timestamp = quote_ts or trade_ts
    age_seconds = None
    if timestamp is not None:
        age_seconds = max((now - timestamp.astimezone(UTC)).total_seconds(), 0.0)
    midpoint = None
    spread_pct = None
    if bid is not None and ask is not None and bid > 0 and ask >= bid:
        midpoint = (bid + ask) / 2.0
        if midpoint > 0:
            spread_pct = ((ask - bid) / midpoint) * 100.0
    return {
        "bid": bid,
        "ask": ask,
        "midpoint": midpoint,
        "spread_pct": spread_pct,
        "timestamp": None if timestamp is None else timestamp.isoformat(),
        "age_seconds": age_seconds,
    }


def _bar_stats(bars: list[Any], *, lookback_bars: int) -> dict[str, Any]:
    if not bars:
        return {}
    volume_sum = sum(max(int(getattr(bar, "volume", 0) or 0), 0) for bar in bars)
    vwap = None
    if volume_sum > 0:
        vwap = sum(
            ((float(bar.high) + float(bar.low) + float(bar.close)) / 3.0)
            * max(int(bar.volume or 0), 0)
            for bar in bars
        ) / volume_sum
    recent = bars[-max(lookback_bars, 1) - 1 : -1] if len(bars) > 1 else bars
    if not recent:
        recent = bars
    return {
        "bar_count": len(bars),
        "latest_close": float(bars[-1].close),
        "vwap": vwap,
        "recent_high": max(float(bar.high) for bar in recent),
        "recent_low": min(float(bar.low) for bar in recent),
        "volume": volume_sum,
    }


def _session_start(now: datetime) -> datetime:
    current = now.astimezone(NEW_YORK)
    return datetime.combine(current.date(), time(9, 30), tzinfo=NEW_YORK).astimezone(
        UTC
    )


def _feed_entry_by_symbol(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in snapshot.get("entries") or []:
        if not isinstance(item, Mapping):
            continue
        symbol = _as_text(item.get("symbol"))
        if symbol is not None:
            rows[symbol.upper()] = dict(item)
    return rows


def _entry_rule_decision(
    entry: Mapping[str, Any],
    *,
    side: str,
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    if not _as_bool(rules.get("enabled"), True):
        return {"passed": False, "reason": "entry_rules_disabled"}
    score = _as_float(entry.get("score"))
    price = _as_float(entry.get("price"))
    move_percent = _as_float(entry.get("move_percent"))
    relative_volume = _as_float(entry.get("relative_volume"))
    daily_volume = _as_int(entry.get("daily_volume"), 0)

    min_score = _as_float(rules.get("min_score"))
    if min_score is not None and (score is None or score < min_score):
        return {"passed": False, "reason": "score_below_min"}
    min_price = _as_float(rules.get("min_price"))
    if min_price is not None and (price is None or price < min_price):
        return {"passed": False, "reason": "price_below_min"}
    max_price = _as_float(rules.get("max_price"))
    if max_price is not None and (price is None or price > max_price):
        return {"passed": False, "reason": "price_above_max"}
    min_relative_volume = _as_float(rules.get("min_relative_volume"))
    if (
        min_relative_volume is not None
        and (relative_volume is None or relative_volume < min_relative_volume)
    ):
        return {"passed": False, "reason": "relative_volume_below_min"}
    min_daily_volume = _as_int(rules.get("min_daily_volume"), 0)
    if min_daily_volume > 0 and daily_volume < min_daily_volume:
        return {"passed": False, "reason": "daily_volume_below_min"}

    min_move_percent = _as_float(rules.get("min_move_percent"))
    if min_move_percent is not None:
        if side == "buy" and (move_percent is None or move_percent < min_move_percent):
            return {"passed": False, "reason": "move_percent_below_min"}
        if side == "sell" and (
            move_percent is None or move_percent > -abs(min_move_percent)
        ):
            return {"passed": False, "reason": "move_percent_above_short_max"}

    require_positive = _as_bool(rules.get("require_positive_momentum"), side == "buy")
    if require_positive and side == "buy" and (
        move_percent is None or move_percent <= 0
    ):
        return {"passed": False, "reason": "positive_momentum_required"}

    return {
        "passed": True,
        "reason": "entry_rules_passed",
        "score": score,
        "price": price,
        "move_percent": move_percent,
        "relative_volume": relative_volume,
        "daily_volume": daily_volume,
    }


def _timing_decision(
    *,
    side: str,
    quote_metrics: Mapping[str, Any],
    stats: Mapping[str, Any],
    rules: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    timing_rules = _mapping(rules.get("timing"))
    mode = (
        _as_text(timing_rules.get("mode"))
        or _as_text(_rule_value(rules, payload, "timing_mode"))
        or ("vwap_reclaim" if side == "buy" else "vwap_breakdown")
    ).lower()
    max_spread_pct = float(_rule_value(rules, payload, "max_spread_pct", 1.0) or 1.0)
    max_quote_age_seconds = max(
        _as_int(_rule_value(rules, payload, "max_quote_age_seconds", 180), 180),
        1,
    )
    bid = _as_float(quote_metrics.get("bid"))
    ask = _as_float(quote_metrics.get("ask"))
    spread_pct = _as_float(quote_metrics.get("spread_pct"))
    age_seconds = _as_float(quote_metrics.get("age_seconds"))
    price = ask if side == "buy" else bid
    if price is None or price <= 0:
        return {"triggered": False, "reason": "quote_unavailable"}
    if age_seconds is None:
        return {"triggered": False, "reason": "quote_timestamp_missing"}
    if age_seconds > max_quote_age_seconds:
        return {"triggered": False, "reason": "quote_stale"}
    if spread_pct is None or spread_pct > max_spread_pct:
        return {"triggered": False, "reason": "spread_too_wide"}
    if mode == "quote_only":
        return {
            "triggered": True,
            "reason": "quote_rules_passed",
            "price": round(price, 4),
            "spread_pct": round(spread_pct, 4),
            "quote_age_seconds": round(age_seconds, 1),
        }

    min_bars = max(
        _as_int(
            timing_rules.get(
                "min_intraday_bars",
                _rule_value(rules, payload, "min_intraday_bars", 5),
            ),
            5,
        ),
        1,
    )
    bar_count = _as_int(stats.get("bar_count"), 0)
    if bar_count < min_bars:
        return {"triggered": False, "reason": "insufficient_intraday_bars"}
    vwap = _as_float(stats.get("vwap"))
    recent_high = _as_float(stats.get("recent_high"))
    recent_low = _as_float(stats.get("recent_low"))
    if vwap is None or recent_high is None or recent_low is None:
        return {"triggered": False, "reason": "timing_metrics_unavailable"}
    tolerance_bps = max(
        _as_float(
            timing_rules.get(
                "breakout_tolerance_bps",
                _rule_value(rules, payload, "breakout_tolerance_bps", 5.0),
            )
        )
        or 0.0,
        0.0,
    )
    tolerance = tolerance_bps / 10_000.0
    if side == "buy":
        triggered = price >= vwap and price >= recent_high * (1.0 - tolerance)
        reason = "buy_vwap_recent_high_reclaim" if triggered else "buy_timing_not_ready"
    else:
        triggered = price <= vwap and price <= recent_low * (1.0 + tolerance)
        reason = "sell_vwap_recent_low_break" if triggered else "sell_timing_not_ready"
    return {
        "triggered": triggered,
        "reason": reason,
        "price": round(price, 4),
        "vwap": round(vwap, 4),
        "recent_high": round(recent_high, 4),
        "recent_low": round(recent_low, 4),
        "spread_pct": round(spread_pct, 4),
        "quote_age_seconds": round(age_seconds, 1),
        "bar_count": bar_count,
    }


def _limit_price(
    *,
    side: str,
    quote_metrics: Mapping[str, Any],
    payload: Mapping[str, Any],
    rules: Mapping[str, Any],
) -> float:
    offset = max(
        _as_float(_rule_value(rules, payload, "limit_price_offset_bps", 0.0)) or 0.0,
        0.0,
    ) / 10_000.0
    bid = _as_float(quote_metrics.get("bid"))
    ask = _as_float(quote_metrics.get("ask"))
    raw = (ask or 0.0) * (1.0 + offset) if side == "buy" else (bid or 0.0) * (1.0 - offset)
    return round(max(raw, 0.01), 2)


def _quantity(
    *,
    price: float,
    payload: Mapping[str, Any],
    rules: Mapping[str, Any],
    max_quantity: int | None = None,
) -> int:
    fixed = max(_as_int(_rule_value(rules, payload, "quantity", 1), 1), 1)
    max_notional = _as_float(_rule_value(rules, payload, "max_notional"))
    resolved = fixed
    if max_notional is not None and max_notional > 0 and price > 0:
        resolved = min(resolved, math.floor(max_notional / price))
    if max_quantity is not None:
        resolved = min(resolved, max_quantity)
    return max(int(resolved), 0)


def _broker_positions_by_symbol(client: Any) -> dict[str, dict[str, Any]]:
    positions: dict[str, dict[str, Any]] = {}
    for row in client.list_positions():
        symbol = _as_text(row.get("symbol"))
        if symbol is not None:
            positions[symbol.upper()] = dict(row)
    return positions


def _broker_qty(position: Mapping[str, Any] | None) -> float:
    if not isinstance(position, Mapping):
        return 0.0
    qty = _as_float(position.get("qty"))
    return 0.0 if qty is None else qty


def _local_managed_positions(
    execution_store: Any,
    *,
    bot_id: str,
    automation_id: str,
    strategy_config_id: str,
) -> list[dict[str, Any]]:
    if not execution_store.portfolio_schema_ready():
        return []
    rows = execution_store.list_positions(
        statuses=OPEN_POSITION_STATUSES,
        strategy_config_id=strategy_config_id,
        limit=500,
    )
    managed: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if _as_text(item.get("bot_id")) not in {None, bot_id}:
            continue
        if _as_text(item.get("automation_id")) not in {None, automation_id}:
            continue
        symbol = _as_text(item.get("root_symbol"))
        if symbol is not None:
            managed.append(item)
    return managed


def _has_active_intent(execution_store: Any, slot_key: str) -> str | None:
    rows = execution_store.list_execution_intents(
        slot_key=slot_key,
        states=sorted(ACTIVE_INTENT_STATES),
        limit=1,
    )
    if not rows:
        return None
    return str(rows[0]["execution_intent_id"])


def _expires_at(payload: Mapping[str, Any]) -> str:
    ttl_minutes = max(_as_int(payload.get("intent_ttl_minutes"), 5), 1)
    return (_now() + timedelta(minutes=ttl_minutes)).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def _base_policy_ref(payload: Mapping[str, Any], feed_id: str) -> dict[str, Any]:
    return {
        "strategy_id": _as_text(payload.get("strategy_id")) or "finviz_direct",
        "strategy_config_id": feed_id,
    }


def _issue_equity_intent(
    *,
    execution_store: Any,
    intent_id: str,
    slot_key: str,
    bot_id: str,
    automation_id: str,
    feed_id: str,
    side: str,
    symbol: str,
    quantity: int,
    limit_price: float,
    session_date: str,
    payload: Mapping[str, Any],
    source: Mapping[str, Any],
    timing: Mapping[str, Any] | None = None,
    trade_intent: str = "open",
    position_id: str | None = None,
) -> dict[str, Any]:
    existing = execution_store.get_execution_intent(intent_id)
    if existing is not None:
        return {
            "created": False,
            "reason": "intent_exists",
            "execution_intent_id": intent_id,
        }
    active_intent_id = _has_active_intent(execution_store, slot_key)
    if active_intent_id is not None:
        return {
            "created": False,
            "reason": "active_intent_exists",
            "execution_intent_id": active_intent_id,
        }

    intent_payload: dict[str, Any] = {
        "asset_class": "equity",
        "label": _as_text(payload.get("label")) or "finviz_direct",
        "market_date": session_date,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "limit_price": limit_price,
        "time_in_force": _as_text(payload.get("time_in_force")) or "day",
        "trade_intent": trade_intent,
        "execution_runtime": _as_text(payload.get("execution_runtime"))
        or NAUTILUS_RUNTIME,
        "approval_mode": _as_text(payload.get("approval_mode")) or "auto",
        "execution_mode": _as_text(payload.get("execution_mode")) or "paper",
        "source": dict(source),
        "entry_rules": _mapping(payload.get("entry_rules")),
        "exit_policy": _mapping(payload.get("exit_rules")),
    }
    if timing is not None:
        intent_payload["timing"] = dict(timing)
    if position_id is not None:
        intent_payload["position_id"] = position_id

    intent = issue_pending_execution_intent(
        execution_store,
        execution_intent_id=intent_id,
        bot_id=bot_id,
        automation_id=automation_id,
        opportunity_decision_id=None,
        strategy_position_id=None,
        action_type=side,
        slot_key=slot_key,
        policy_ref=_base_policy_ref(payload, feed_id),
        config_hash=_as_text(payload.get("declared_config_hash"))
        or _as_text(payload.get("config_hash"))
        or "",
        expires_at=_expires_at(payload),
        payload=intent_payload,
        created_event_payload={
            "source": dict(source),
            "timing": None if timing is None else dict(timing),
            "position_id": position_id,
        },
    )
    return {
        "created": True,
        "execution_intent_id": str(intent["execution_intent_id"]),
        "intent": intent,
    }


def _evaluate_exit(
    *,
    position: Mapping[str, Any],
    broker_position: Mapping[str, Any] | None,
    quote_metrics: Mapping[str, Any],
    feed_symbols: set[str],
    feed_available: bool,
    clock: Mapping[str, Any],
    rules: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    if not _as_bool(rules.get("enabled"), True):
        return {"triggered": False, "reason": "exit_rules_disabled"}
    symbol = str(position.get("root_symbol") or "").upper()
    broker_qty = _broker_qty(broker_position)
    if broker_qty <= 0:
        return {"triggered": False, "reason": "no_broker_long_position"}
    bid = _as_float(quote_metrics.get("bid"))
    midpoint = _as_float(quote_metrics.get("midpoint"))
    price = bid or midpoint
    if price is None or price <= 0:
        return {"triggered": False, "reason": "exit_quote_unavailable"}
    spread_pct = _as_float(quote_metrics.get("spread_pct"))
    max_spread_pct = _as_float(rules.get("max_spread_pct"))
    if max_spread_pct is not None and (spread_pct is None or spread_pct > max_spread_pct):
        return {"triggered": False, "reason": "exit_spread_too_wide"}
    age_seconds = _as_float(quote_metrics.get("age_seconds"))
    max_quote_age_seconds = _as_int(rules.get("max_quote_age_seconds"), 180)
    if age_seconds is None:
        return {"triggered": False, "reason": "exit_quote_timestamp_missing"}
    if age_seconds > max_quote_age_seconds:
        return {"triggered": False, "reason": "exit_quote_stale"}

    entry_price = _as_float(
        None if broker_position is None else broker_position.get("avg_entry_price")
    )
    if entry_price is None:
        entry_price = _as_float(position.get("entry_value"))
    if entry_price is None or entry_price <= 0:
        return {"triggered": False, "reason": "entry_price_unavailable"}
    pnl_pct = (price - entry_price) / entry_price
    profit_target_pct = _pct(rules.get("profit_target_pct"))
    if profit_target_pct is not None and pnl_pct >= profit_target_pct:
        return {
            "triggered": True,
            "reason": "profit_target",
            "price": round(price, 4),
            "entry_price": round(entry_price, 4),
            "pnl_pct": round(pnl_pct, 6),
        }
    stop_loss_pct = _pct(rules.get("stop_loss_pct"))
    if stop_loss_pct is not None and pnl_pct <= -abs(stop_loss_pct):
        return {
            "triggered": True,
            "reason": "stop_loss",
            "price": round(price, 4),
            "entry_price": round(entry_price, 4),
            "pnl_pct": round(pnl_pct, 6),
        }
    if (
        feed_available
        and _as_bool(rules.get("sell_when_removed_from_feed"), False)
        and symbol not in feed_symbols
    ):
        return {
            "triggered": True,
            "reason": "removed_from_feed",
            "price": round(price, 4),
            "entry_price": round(entry_price, 4),
            "pnl_pct": round(pnl_pct, 6),
        }
    max_hold_minutes = _as_int(rules.get("max_hold_minutes"), 0)
    opened_at = parse_datetime(position.get("opened_at"))
    if max_hold_minutes > 0 and opened_at is not None:
        held_minutes = (now - opened_at.astimezone(UTC)).total_seconds() / 60.0
        if held_minutes >= max_hold_minutes:
            return {
                "triggered": True,
                "reason": "max_hold_minutes",
                "price": round(price, 4),
                "entry_price": round(entry_price, 4),
                "pnl_pct": round(pnl_pct, 6),
                "held_minutes": round(held_minutes, 1),
            }
    close_minutes = _as_int(rules.get("force_close_minutes_before_close"), 0)
    next_close = parse_datetime(clock.get("next_close"))
    if close_minutes > 0 and next_close is not None:
        close_at = next_close.astimezone(UTC) - timedelta(minutes=close_minutes)
        if now >= close_at:
            return {
                "triggered": True,
                "reason": "force_close",
                "price": round(price, 4),
                "entry_price": round(entry_price, 4),
                "pnl_pct": round(pnl_pct, 6),
            }
    return {
        "triggered": False,
        "reason": "hold",
        "price": round(price, 4),
        "entry_price": round(entry_price, 4),
        "pnl_pct": round(pnl_pct, 6),
    }


def run_finviz_direct_trading(
    *,
    db_target: str,
    storage: Any,
    job_store: Any,
    job_run_id: str,
    payload: Mapping[str, Any],
    heartbeat: Callable[[], None],
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}

    feed_id = _as_text(payload.get("feed_id")) or DEFAULT_FEED_ID
    feed_job_key = _as_text(payload.get("feed_job_key")) or DEFAULT_FEED_JOB_KEY
    max_feed_age_seconds = max(_as_int(payload.get("max_feed_age_seconds"), 300), 1)
    snapshot = get_latest_symbol_feed_snapshot(
        job_store,
        feed_id=feed_id,
        job_key=feed_job_key,
        max_age_seconds=max_feed_age_seconds,
    )
    snapshot_status = str(snapshot.get("status") or "missing")

    heartbeat()
    client = create_alpaca_client_from_env()
    clock = client.get_clock()
    if not _clock_is_open(clock) and not _as_bool(payload.get("allow_off_hours"), False):
        return {
            "status": "skipped",
            "reason": "market_closed",
            "clock": clock,
            "feed_id": feed_id,
            "feed_job_run_id": snapshot.get("job_run_id"),
        }

    bot_id = _as_text(payload.get("bot_id")) or "finviz"
    automation_id = _as_text(payload.get("automation_id")) or "finviz_direct"
    session_date = str(
        payload.get("session_date")
        or _now().astimezone(NEW_YORK).date().isoformat()
    )
    entry_rules = _mapping(payload.get("entry_rules"))
    exit_rules = _mapping(payload.get("exit_rules"))
    stock_feed = _as_text(payload.get("stock_feed")) or "iex"
    max_candidates = max(_as_int(payload.get("max_candidates"), 3), 1)
    entry_side = (_as_text(entry_rules.get("side")) or "buy").lower()
    if entry_side not in {"buy", "sell"}:
        entry_side = "buy"
    allow_short_selling = _as_bool(payload.get("allow_short_selling"), False)

    feed_entries = _feed_entry_by_symbol(snapshot) if snapshot_status == "ready" else {}
    feed_symbols = set(feed_entries)
    feed_available = snapshot_status in {"ready", "empty"}
    managed_positions = _local_managed_positions(
        execution_store,
        bot_id=bot_id,
        automation_id=automation_id,
        strategy_config_id=feed_id,
    )
    managed_by_symbol = {
        str(position.get("root_symbol")).upper(): position
        for position in managed_positions
        if _as_text(position.get("root_symbol")) is not None
    }
    broker_positions = _broker_positions_by_symbol(client)

    entry_symbols = list(feed_entries)[:max_candidates]
    exit_symbols = list(managed_by_symbol)
    all_symbols = sorted(set(entry_symbols) | set(exit_symbols))
    snapshots = client.get_stock_snapshots(all_symbols, feed=stock_feed) if all_symbols else {}

    now = _now()
    decisions: list[dict[str, Any]] = []
    armed = 0

    for symbol in entry_symbols:
        heartbeat()
        entry = feed_entries[symbol]
        rule_decision = _entry_rule_decision(entry, side=entry_side, rules=entry_rules)
        decision = {
            "kind": "entry",
            "symbol": symbol,
            "side": entry_side,
            **rule_decision,
        }
        if not rule_decision.get("passed"):
            decisions.append(decision)
            continue
        if entry_side == "sell" and not allow_short_selling:
            decisions.append(
                {**decision, "passed": False, "reason": "short_selling_disabled"}
            )
            continue
        if entry_side == "buy" and (
            symbol in managed_by_symbol or _broker_qty(broker_positions.get(symbol)) > 0
        ):
            decisions.append(
                {**decision, "passed": False, "reason": "position_already_open"}
            )
            continue

        lookback_bars = max(
            _as_int(
                _mapping(entry_rules.get("timing")).get(
                    "recent_lookback_bars",
                    _rule_value(entry_rules, payload, "recent_lookback_bars", 5),
                ),
                5,
            ),
            1,
        )
        bars = client.get_intraday_bars(
            symbol,
            start=_session_start(now).isoformat().replace("+00:00", "Z"),
            end=now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            stock_feed=stock_feed,
            timeframe=_as_text(payload.get("bar_timeframe")) or "1Min",
        )
        quote_metrics = _quote_metrics(snapshots.get(symbol, {}), now=now)
        stats = _bar_stats(bars, lookback_bars=lookback_bars)
        timing = _timing_decision(
            side=entry_side,
            quote_metrics=quote_metrics,
            stats=stats,
            rules=entry_rules,
            payload=payload,
        )
        decision = {**decision, **timing}
        if not timing.get("triggered"):
            decisions.append(decision)
            continue
        limit_price = _limit_price(
            side=entry_side,
            quote_metrics=quote_metrics,
            payload=payload,
            rules=entry_rules,
        )
        quantity = _quantity(price=limit_price, payload=payload, rules=entry_rules)
        if quantity <= 0:
            decisions.append(
                {**decision, "triggered": False, "reason": "quantity_resolved_to_zero"}
            )
            continue
        intent_id = (
            "execution_intent:finviz_direct:entry:"
            f"{_safe_component(snapshot.get('job_run_id'))}:{symbol}:{entry_side}"
        )
        slot_key = f"finviz_direct:{feed_id}:{symbol}:entry"
        source = {
            "source": "finviz_direct_trading",
            "flow": "finviz_direct",
            "feed_id": feed_id,
            "feed_job_key": feed_job_key,
            "feed_job_run_id": snapshot.get("job_run_id"),
            "trading_job_run_id": job_run_id,
            "feed_entry": dict(entry),
        }
        intent_result = _issue_equity_intent(
            execution_store=execution_store,
            intent_id=intent_id,
            slot_key=slot_key,
            bot_id=bot_id,
            automation_id=automation_id,
            feed_id=feed_id,
            side=entry_side,
            symbol=symbol,
            quantity=quantity,
            limit_price=limit_price,
            session_date=session_date,
            payload=payload,
            source=source,
            timing=timing,
            trade_intent="open",
        )
        if intent_result.get("created"):
            armed += 1
        decisions.append(
            {
                **decision,
                "limit_price": limit_price,
                "quantity": quantity,
                **{
                    key: intent_result.get(key)
                    for key in ("created", "reason", "execution_intent_id")
                    if key in intent_result
                },
            }
        )

    for symbol, position in managed_by_symbol.items():
        heartbeat()
        quote_metrics = _quote_metrics(snapshots.get(symbol, {}), now=now)
        exit_decision = _evaluate_exit(
            position=position,
            broker_position=broker_positions.get(symbol),
            quote_metrics=quote_metrics,
            feed_symbols=feed_symbols,
            feed_available=feed_available,
            clock=clock,
            rules=exit_rules,
            now=now,
        )
        decision = {
            "kind": "exit",
            "symbol": symbol,
            "side": "sell",
            "position_id": position.get("position_id"),
            **exit_decision,
        }
        if not exit_decision.get("triggered"):
            decisions.append(decision)
            continue
        limit_price = _limit_price(
            side="sell",
            quote_metrics=quote_metrics,
            payload=payload,
            rules=exit_rules,
        )
        max_exit_quantity = int(
            max(
                min(
                    _broker_qty(broker_positions.get(symbol)),
                    _as_float(position.get("remaining_quantity")) or 0.0,
                ),
                0.0,
            )
        )
        quantity = _quantity(
            price=limit_price,
            payload=payload,
            rules=exit_rules,
            max_quantity=max_exit_quantity,
        )
        if quantity <= 0:
            decisions.append(
                {**decision, "triggered": False, "reason": "exit_quantity_zero"}
            )
            continue
        position_id = str(position["position_id"])
        reason = str(exit_decision.get("reason") or "exit")
        intent_id = (
            "execution_intent:finviz_direct:exit:"
            f"{_safe_component(job_run_id)}:{_safe_component(position_id)}:{reason}"
        )
        slot_key = f"finviz_direct:{feed_id}:{symbol}:exit"
        source = {
            "source": "finviz_direct_trading",
            "flow": "finviz_direct",
            "feed_id": feed_id,
            "feed_job_key": feed_job_key,
            "feed_job_run_id": snapshot.get("job_run_id"),
            "trading_job_run_id": job_run_id,
            "exit_reason": reason,
        }
        intent_result = _issue_equity_intent(
            execution_store=execution_store,
            intent_id=intent_id,
            slot_key=slot_key,
            bot_id=bot_id,
            automation_id=automation_id,
            feed_id=feed_id,
            side="sell",
            symbol=symbol,
            quantity=quantity,
            limit_price=limit_price,
            session_date=session_date,
            payload=payload,
            source=source,
            timing=exit_decision,
            trade_intent="close",
            position_id=position_id,
        )
        if intent_result.get("created"):
            armed += 1
        decisions.append(
            {
                **decision,
                "limit_price": limit_price,
                "quantity": quantity,
                **{
                    key: intent_result.get(key)
                    for key in ("created", "reason", "execution_intent_id")
                    if key in intent_result
                },
            }
        )

    dispatch_result = None
    if armed and _as_bool(payload.get("dispatch_after_trigger"), True):
        dispatch_result = dispatch_pending_execution_intents(
            db_target=db_target,
            limit=max(armed, 1),
            storage=storage,
        )

    return {
        "status": "completed",
        "feed_id": feed_id,
        "feed_job_key": feed_job_key,
        "feed_job_run_id": snapshot.get("job_run_id"),
        "feed_status": snapshot_status,
        "session_date": session_date,
        "entry_candidates": len(entry_symbols),
        "managed_positions": len(managed_positions),
        "armed": armed,
        "decisions": decisions,
        "dispatch_result": dispatch_result,
    }


__all__ = ["run_finviz_direct_trading"]

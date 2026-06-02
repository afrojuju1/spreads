from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, time, timedelta
import math
import re
from typing import Any
from zoneinfo import ZoneInfo

from core.common import parse_float, parse_int
from core.services.alpaca import create_alpaca_client_from_env
from core.services.execution.runtimes import ALPACA_DIRECT_RUNTIME, NAUTILUS_RUNTIME
from core.services.execution_portfolio import fetch_latest_option_quotes
from core.services.execution_intents import dispatch_pending_execution_intents
from core.services.execution_intents.shared import (
    ACTIVE_INTENT_STATES,
    issue_pending_execution_intent,
)
from core.services.option_structures import position_legs
from core.services.symbol_feeds import get_latest_symbol_feed_snapshot
from core.storage.serializers import parse_datetime


NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_FEED_ID = "finviz_momentum"
DEFAULT_FEED_JOB_KEY = "symbol_feed:finviz_momentum"
OPEN_POSITION_STATUSES = ["open", "partial_open", "partial_close", "pending_open"]
DEFAULT_REENTRY_RESET_REASONS = frozenset(
    {
        "stop_loss",
        "stop_multiple",
        "underlying_vwap_recent_low_break",
        "removed_from_feed",
    }
)


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


def _live_option_quote_metrics(quote: Any, *, now: datetime) -> dict[str, Any]:
    bid = _as_float(quote.get("bid") if isinstance(quote, Mapping) else getattr(quote, "bid", None))
    ask = _as_float(quote.get("ask") if isinstance(quote, Mapping) else getattr(quote, "ask", None))
    timestamp = parse_datetime(
        quote.get("timestamp")
        if isinstance(quote, Mapping)
        else getattr(quote, "timestamp", None)
    )
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


def _option_snapshot_spread_pct(snapshot: Any) -> float | None:
    bid = _as_float(getattr(snapshot, "bid", None))
    ask = _as_float(getattr(snapshot, "ask", None))
    if bid is None or ask is None or bid <= 0 or ask < bid:
        return None
    midpoint = (bid + ask) / 2.0
    if midpoint <= 0:
        return None
    return ((ask - bid) / midpoint) * 100.0


def _instrument_mode(payload: Mapping[str, Any]) -> str:
    mode = (
        _as_text(payload.get("instrument_mode"))
        or _as_text(payload.get("instrument"))
        or "equity"
    ).lower()
    if mode in {"call", "calls", "option", "options", "long_call", "optionable_calls"}:
        return "long_call"
    return "equity"


def _option_quote_feeds(
    rules: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> tuple[str, ...]:
    raw_feeds = rules.get("quote_feeds") or payload.get("option_quote_feeds")
    if isinstance(raw_feeds, (list, tuple)):
        feeds = tuple(
            str(value).strip().lower()
            for value in raw_feeds
            if str(value or "").strip()
        )
        if feeds:
            return feeds
    feed = (
        _as_text(_rule_value(rules, payload, "option_feed"))
        or _as_text(payload.get("option_feed"))
        or "opra"
    ).lower()
    if feed == "indicative":
        return ("indicative",)
    return (feed, "indicative")


def _optionable_symbol_set(client: Any) -> tuple[set[str] | None, str | None]:
    try:
        return {
            str(item.get("symbol") or "").strip().upper()
            for item in client.list_optionable_underlyings()
            if isinstance(item, Mapping) and str(item.get("symbol") or "").strip()
        }, None
    except Exception as exc:
        return None, str(exc)


def _date_from_iso(value: Any) -> date | None:
    text = _as_text(value)
    if text is None:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _time_from_text(value: Any) -> time | None:
    text = _as_text(value)
    if text is None:
        return None
    try:
        hour_text, minute_text = text.split(":", 1)
        return time(int(hour_text), int(minute_text[:2]))
    except (TypeError, ValueError):
        return None


def _days_to_expiration(expiration_date: Any, *, now: datetime) -> int | None:
    expiration = _date_from_iso(expiration_date)
    if expiration is None:
        return None
    return (expiration - now.astimezone(NEW_YORK).date()).days


def _option_contract_candidates(
    *,
    client: Any,
    symbol: str,
    underlying_price: float,
    min_expiration: str,
    max_expiration: str,
    rules: Mapping[str, Any],
    payload: Mapping[str, Any],
    now: datetime,
    heartbeat: Callable[[], None],
) -> tuple[list[dict[str, Any]], list[str]]:
    min_open_interest = max(
        _as_int(_rule_value(rules, payload, "min_open_interest", 100), 100),
        0,
    )
    max_spread_pct = max(
        _as_float(_rule_value(rules, payload, "max_spread_pct", 20.0)) or 20.0,
        0.0,
    )
    max_premium = _as_float(
        _rule_value(
            rules,
            payload,
            "max_premium",
            payload.get("max_premium", payload.get("max_notional")),
        )
    )
    min_daily_volume = max(
        _as_int(_rule_value(rules, payload, "min_daily_volume", 0), 0),
        0,
    )
    min_delta = _as_float(_rule_value(rules, payload, "min_delta"))
    max_delta = _as_float(_rule_value(rules, payload, "max_delta"))
    target_delta = _as_float(_rule_value(rules, payload, "target_delta"))
    preferred_min_delta = _as_float(_rule_value(rules, payload, "preferred_min_delta"))
    preferred_max_delta = _as_float(_rule_value(rules, payload, "preferred_max_delta"))
    chain_feed = (
        _as_text(_rule_value(rules, payload, "option_feed"))
        or _as_text(payload.get("option_feed"))
        or "opra"
    ).lower()

    contracts = client.list_option_contracts(
        symbol,
        min_expiration,
        max_expiration,
        option_type="call",
    )
    by_expiration: dict[str, list[Any]] = {}
    for contract in contracts:
        by_expiration.setdefault(str(contract.expiration_date), []).append(contract)

    errors: list[str] = []
    candidates: list[dict[str, Any]] = []
    for expiration, expiration_contracts in sorted(by_expiration.items()):
        heartbeat()
        try:
            snapshots = client.get_option_chain_snapshots(
                symbol,
                expiration,
                "call",
                feed=chain_feed,
            )
        except Exception as exc:
            errors.append(f"{expiration}:{exc}")
            continue
        for contract in expiration_contracts:
            snapshot = snapshots.get(contract.symbol)
            if snapshot is None:
                continue
            open_interest = int(getattr(contract, "open_interest", 0) or 0)
            if open_interest < min_open_interest:
                continue
            if int(getattr(snapshot, "bid_size", 0) or 0) <= 0:
                continue
            if int(getattr(snapshot, "ask_size", 0) or 0) <= 0:
                continue
            spread_pct = _option_snapshot_spread_pct(snapshot)
            if spread_pct is None or spread_pct > max_spread_pct:
                continue
            ask = _as_float(getattr(snapshot, "ask", None))
            if ask is None or ask <= 0:
                continue
            if max_premium is not None and max_premium > 0 and ask * 100.0 > max_premium:
                continue
            daily_volume = _as_int(getattr(snapshot, "daily_volume", None), 0)
            if min_daily_volume > 0 and daily_volume < min_daily_volume:
                continue
            delta = _as_float(getattr(snapshot, "delta", None))
            if min_delta is not None and (delta is None or delta < min_delta):
                continue
            if max_delta is not None and (delta is None or delta > max_delta):
                continue
            delta_distance = (
                abs(delta - target_delta)
                if delta is not None and target_delta is not None
                else None
            )
            preferred_delta_miss = 0.0
            if delta is not None:
                if preferred_min_delta is not None and delta < preferred_min_delta:
                    preferred_delta_miss = preferred_min_delta - delta
                if preferred_max_delta is not None and delta > preferred_max_delta:
                    preferred_delta_miss = max(
                        preferred_delta_miss,
                        delta - preferred_max_delta,
                    )
            days_to_expiration = _days_to_expiration(expiration, now=now)
            if days_to_expiration is None:
                continue
            candidates.append(
                {
                    "symbol": str(contract.symbol).upper(),
                    "expiration_date": expiration,
                    "days_to_expiration": days_to_expiration,
                    "strike": float(contract.strike_price),
                    "open_interest": open_interest,
                    "daily_volume": daily_volume,
                    "snapshot_bid": float(snapshot.bid),
                    "snapshot_ask": float(snapshot.ask),
                    "snapshot_midpoint": float(snapshot.midpoint),
                    "snapshot_spread_pct": round(spread_pct, 4),
                    "delta": delta,
                    "delta_distance": delta_distance,
                    "preferred_delta_miss": round(preferred_delta_miss, 6),
                    "implied_volatility": getattr(snapshot, "implied_volatility", None),
                    "strike_distance": abs(float(contract.strike_price) - underlying_price),
                }
            )
    candidates.sort(
        key=lambda item: (
            float(item["preferred_delta_miss"]),
            (
                float(item["delta_distance"])
                if item.get("delta_distance") is not None
                else math.inf
            ),
            float(item["strike_distance"]),
            int(item["days_to_expiration"]),
            float(item["snapshot_spread_pct"]),
            -int(item["daily_volume"]),
            -int(item["open_interest"]),
            str(item["symbol"]),
        )
    )
    return candidates, errors


def _select_long_call_contract(
    *,
    client: Any,
    symbol: str,
    underlying_price: float,
    rules: Mapping[str, Any],
    payload: Mapping[str, Any],
    now: datetime,
    heartbeat: Callable[[], None],
) -> dict[str, Any]:
    if underlying_price <= 0:
        return {"selected": False, "reason": "underlying_price_unavailable"}
    min_dte = max(_as_int(_rule_value(rules, payload, "min_dte", 7), 7), 0)
    max_dte = max(_as_int(_rule_value(rules, payload, "max_dte", 21), 21), min_dte)
    current_date = now.astimezone(NEW_YORK).date()
    min_expiration = (current_date + timedelta(days=min_dte)).isoformat()
    max_expiration = (current_date + timedelta(days=max_dte)).isoformat()
    try:
        candidates, chain_errors = _option_contract_candidates(
            client=client,
            symbol=symbol,
            underlying_price=underlying_price,
            min_expiration=min_expiration,
            max_expiration=max_expiration,
            rules=rules,
            payload=payload,
            now=now,
            heartbeat=heartbeat,
        )
    except Exception as exc:
        return {
            "selected": False,
            "reason": "option_contracts_unavailable",
            "error": str(exc),
        }
    if not candidates:
        return {
            "selected": False,
            "reason": "no_call_contract_passed_filters",
            "min_expiration": min_expiration,
            "max_expiration": max_expiration,
            "chain_errors": chain_errors[:3],
        }

    quote_feeds = _option_quote_feeds(rules, payload)
    quote_symbols = [str(item["symbol"]) for item in candidates[:20]]
    quotes, sources, quote_error = fetch_latest_option_quotes(
        quote_symbols,
        client=client,
        feeds=quote_feeds,
    )
    max_quote_age_seconds = max(
        _as_int(_rule_value(rules, payload, "max_quote_age_seconds", 180), 180),
        1,
    )
    max_spread_pct = max(
        _as_float(_rule_value(rules, payload, "max_spread_pct", 20.0)) or 20.0,
        0.0,
    )
    max_premium = _as_float(
        _rule_value(
            rules,
            payload,
            "max_premium",
            payload.get("max_premium", payload.get("max_notional")),
        )
    )
    last_reason = "option_quote_unavailable"
    for candidate in candidates:
        option_symbol = str(candidate["symbol"])
        quote = quotes.get(option_symbol)
        if quote is None:
            continue
        metrics = _live_option_quote_metrics(quote, now=now)
        age_seconds = _as_float(metrics.get("age_seconds"))
        if age_seconds is None:
            last_reason = "option_quote_timestamp_missing"
            continue
        if age_seconds > max_quote_age_seconds:
            last_reason = "option_quote_stale"
            continue
        spread_pct = _as_float(metrics.get("spread_pct"))
        if spread_pct is None or spread_pct > max_spread_pct:
            last_reason = "option_spread_too_wide"
            continue
        ask = _as_float(metrics.get("ask"))
        if ask is None or ask <= 0:
            last_reason = "option_quote_unavailable"
            continue
        if max_premium is not None and max_premium > 0 and ask * 100.0 > max_premium:
            last_reason = "option_premium_above_max"
            continue
        return {
            "selected": True,
            "reason": "long_call_selected",
            **candidate,
            "quote_metrics": metrics,
            "quote_source": sources.get(option_symbol),
            "quote_feeds": quote_feeds,
            "premium": round(ask * 100.0, 2),
        }
    return {
        "selected": False,
        "reason": last_reason,
        "quote_error": quote_error,
        "quote_feeds": quote_feeds,
        "candidate_count": len(candidates),
    }


def _bar_stats(
    bars: list[Any],
    *,
    lookback_bars: int,
    confirmation_bars: int = 0,
) -> dict[str, Any]:
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
    resolved_confirmation_bars = max(int(confirmation_bars or 0), 0)
    reference_end = (
        len(bars) - resolved_confirmation_bars
        if resolved_confirmation_bars > 0
        else len(bars) - 1
    )
    if reference_end <= 0:
        reference_end = len(bars)
    reference_start = max(reference_end - max(lookback_bars, 1), 0)
    recent = bars[reference_start:reference_end]
    if not recent:
        recent = bars
    confirmation = (
        bars[-resolved_confirmation_bars:]
        if resolved_confirmation_bars > 0 and len(bars) >= resolved_confirmation_bars
        else []
    )
    return {
        "bar_count": len(bars),
        "latest_close": float(bars[-1].close),
        "vwap": vwap,
        "recent_high": max(float(bar.high) for bar in recent),
        "recent_low": min(float(bar.low) for bar in recent),
        "confirmation_closes": [float(bar.close) for bar in confirmation],
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
    market_cap = _as_float(entry.get("market_cap"))
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
    min_market_cap = _as_float(rules.get("min_market_cap"))
    if min_market_cap is not None and (
        market_cap is None or market_cap < min_market_cap
    ):
        return {
            "passed": False,
            "reason": "market_cap_below_min",
            "market_cap": market_cap,
        }
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
        "market_cap": market_cap,
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
    now: datetime,
) -> dict[str, Any]:
    timing_rules = _mapping(rules.get("timing"))
    mode = (
        _as_text(timing_rules.get("mode"))
        or _as_text(_rule_value(rules, payload, "timing_mode"))
        or ("vwap_reclaim" if side == "buy" else "vwap_breakdown")
    ).lower()
    not_before = _time_from_text(
        timing_rules.get(
            "not_before_time",
            _rule_value(rules, payload, "not_before_time"),
        )
    )
    if not_before is not None and now.astimezone(NEW_YORK).time() < not_before:
        return {
            "triggered": False,
            "reason": "entry_before_start_time",
            "not_before_time": not_before.isoformat(timespec="minutes"),
        }
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
    confirmation_bars = max(
        _as_int(
            timing_rules.get(
                "confirmation_bars",
                _rule_value(rules, payload, "confirmation_bars", 0),
            ),
            0,
        ),
        0,
    )
    confirmation_closes = [
        float(value)
        for value in stats.get("confirmation_closes") or []
        if _as_float(value) is not None
    ]
    if triggered and confirmation_bars > 0:
        if len(confirmation_closes) < confirmation_bars:
            triggered = False
            reason = "insufficient_confirmation_bars"
        elif side == "buy":
            confirmed = all(
                close >= vwap and close >= recent_high * (1.0 - tolerance)
                for close in confirmation_closes[-confirmation_bars:]
            )
            if not confirmed:
                triggered = False
                reason = "buy_reclaim_unconfirmed"
        else:
            confirmed = all(
                close <= vwap and close <= recent_low * (1.0 + tolerance)
                for close in confirmation_closes[-confirmation_bars:]
            )
            if not confirmed:
                triggered = False
                reason = "sell_breakdown_unconfirmed"
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
        "confirmation_bars": confirmation_bars,
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


def _option_quantity(
    *,
    price: float,
    payload: Mapping[str, Any],
    rules: Mapping[str, Any],
    max_quantity: int | None = None,
) -> int:
    quantity_value = rules.get("contracts", _rule_value(rules, payload, "quantity", 1))
    fixed = max(_as_int(quantity_value, 1), 1)
    max_premium = _as_float(
        _rule_value(
            rules,
            payload,
            "max_premium",
            payload.get("max_premium", payload.get("max_notional")),
        )
    )
    resolved = fixed
    if max_premium is not None and max_premium > 0 and price > 0:
        resolved = min(resolved, math.floor(max_premium / (price * 100.0)))
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


def _position_strategy_family(position: Mapping[str, Any]) -> str:
    return str(position.get("strategy_family") or position.get("strategy") or "").lower()


def _position_option_symbol(position: Mapping[str, Any]) -> str | None:
    legs = position_legs(position)
    if not legs:
        return None
    for leg in legs:
        if str(leg.get("role") or "").lower() == "long":
            symbol = _as_text(leg.get("symbol"))
            if symbol is not None:
                return symbol.upper()
    symbol = _as_text(legs[0].get("symbol"))
    return None if symbol is None else symbol.upper()


def _position_status(position: Mapping[str, Any]) -> str:
    return str(position.get("position_status") or position.get("status") or "").lower()


def _active_entry_intent_count(
    execution_store: Any,
    *,
    bot_id: str,
    automation_id: str,
    feed_id: str,
) -> int:
    rows = execution_store.list_execution_intents(
        bot_id=bot_id,
        automation_id=automation_id,
        states=sorted(ACTIVE_INTENT_STATES),
        limit=200,
    )
    count = 0
    for row in rows:
        intent_payload = row.get("payload")
        if not isinstance(intent_payload, Mapping):
            intent_payload = row.get("payload_json")
        if not isinstance(intent_payload, Mapping):
            continue
        if str(intent_payload.get("trade_intent") or "open").lower() != "open":
            continue
        source = intent_payload.get("source")
        if not isinstance(source, Mapping):
            continue
        if source.get("flow") == "finviz_direct" and source.get("feed_id") == feed_id:
            count += 1
    return count


def _broker_qty(position: Mapping[str, Any] | None) -> float:
    if not isinstance(position, Mapping):
        return 0.0
    qty = _as_float(position.get("qty"))
    return 0.0 if qty is None else qty


def _local_position_rows(
    execution_store: Any,
    *,
    bot_id: str,
    automation_id: str,
    strategy_config_id: str,
    statuses: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not execution_store.portfolio_schema_ready():
        return []
    rows = execution_store.list_positions(
        statuses=statuses,
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


def _local_managed_positions(
    execution_store: Any,
    *,
    bot_id: str,
    automation_id: str,
    strategy_config_id: str,
) -> list[dict[str, Any]]:
    return _local_position_rows(
        execution_store,
        bot_id=bot_id,
        automation_id=automation_id,
        strategy_config_id=strategy_config_id,
        statuses=OPEN_POSITION_STATUSES,
    )


def _daily_pnl_snapshot(
    positions: list[Mapping[str, Any]],
    *,
    session_date: str,
) -> dict[str, Any]:
    daily_realized = 0.0
    open_unrealized = 0.0
    for position in positions:
        opened = str(position.get("market_date_opened") or position.get("market_date") or "")
        closed = str(position.get("market_date_closed") or "")
        if opened == session_date or closed == session_date:
            daily_realized += _as_float(position.get("realized_pnl")) or 0.0
        if _position_status(position) in OPEN_POSITION_STATUSES:
            open_unrealized += _as_float(position.get("unrealized_pnl")) or 0.0
    daily_total = daily_realized + open_unrealized
    return {
        "session_date": session_date,
        "daily_realized_pnl": round(daily_realized, 2),
        "open_unrealized_pnl": round(open_unrealized, 2),
        "daily_total_pnl": round(daily_total, 2),
    }


def _finviz_filled_entry_attempt_count(
    execution_store: Any,
    *,
    bot_id: str,
    automation_id: str,
    feed_id: str,
    session_date: str,
) -> int:
    if not execution_store.schema_ready():
        return 0
    count = 0
    for row in execution_store.list_attempts_for_market_date(
        market_date=session_date,
        limit=1000,
    ):
        if _as_text(row.get("bot_id")) not in {None, bot_id}:
            continue
        if _as_text(row.get("automation_id")) not in {None, automation_id}:
            continue
        strategy_config_id = _as_text(row.get("strategy_config_id"))
        if strategy_config_id is not None and strategy_config_id != feed_id:
            continue
        if strategy_config_id is None:
            request = row.get("request")
            if not isinstance(request, Mapping):
                request = {}
            source = request.get("source")
            if not isinstance(source, Mapping):
                source = {}
            if source.get("flow") != "finviz_direct" or source.get("feed_id") != feed_id:
                continue
        if str(row.get("trade_intent") or "open").lower() != "open":
            continue
        if str(row.get("status") or "").lower() == "filled":
            count += 1
    return count


def _daily_entry_budget_snapshot(
    positions: list[Mapping[str, Any]],
    *,
    session_date: str,
    max_daily_entries: int,
    filled_entry_attempt_count: int,
    active_entry_intents: int,
    entry_armed: int,
) -> dict[str, Any]:
    position_entry_count = sum(
        1
        for position in positions
        if str(position.get("market_date_opened") or position.get("market_date") or "")
        == session_date
    )
    filled_entry_count = max(filled_entry_attempt_count, position_entry_count)
    reserved_entry_count = max(active_entry_intents, 0) + max(entry_armed, 0)
    used_entry_count = filled_entry_count + reserved_entry_count
    remaining_entry_count = None
    budget_reached = False
    if max_daily_entries > 0:
        remaining_entry_count = max(max_daily_entries - used_entry_count, 0)
        budget_reached = used_entry_count >= max_daily_entries
    return {
        "session_date": session_date,
        "max_daily_entries": max_daily_entries if max_daily_entries > 0 else None,
        "filled_entry_attempt_count": filled_entry_attempt_count,
        "position_entry_count": position_entry_count,
        "filled_entry_count": filled_entry_count,
        "active_entry_intents": active_entry_intents,
        "entry_armed": entry_armed,
        "reserved_entry_count": reserved_entry_count,
        "used_entry_count": used_entry_count,
        "remaining_entry_count": remaining_entry_count,
        "budget_reached": budget_reached,
    }


def _reentry_reset_reasons(payload: Mapping[str, Any]) -> set[str]:
    reentry_rules = _mapping(payload.get("same_symbol_reentry"))
    raw_reasons = reentry_rules.get(
        "invalidation_reasons",
        payload.get("same_symbol_reentry_invalidation_reasons"),
    )
    if not isinstance(raw_reasons, list):
        return set(DEFAULT_REENTRY_RESET_REASONS)
    reasons = {
        str(reason).strip().lower()
        for reason in raw_reasons
        if str(reason or "").strip()
    }
    return reasons or set(DEFAULT_REENTRY_RESET_REASONS)


def _opening_intent_source(
    execution_store: Any,
    position: Mapping[str, Any],
) -> dict[str, Any]:
    if not execution_store.intent_schema_ready():
        return {}
    intent_id = _as_text(position.get("opening_execution_intent_id"))
    if intent_id is None:
        return {}
    intent = execution_store.get_execution_intent(intent_id)
    if not isinstance(intent, Mapping):
        return {}
    payload = intent.get("payload")
    if not isinstance(payload, Mapping):
        payload = intent.get("payload_json")
    if not isinstance(payload, Mapping):
        return {}
    source = payload.get("source")
    return dict(source) if isinstance(source, Mapping) else {}


def _same_symbol_reentry_reset_decision(
    *,
    execution_store: Any,
    positions: list[Mapping[str, Any]],
    symbol: str,
    entry: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    session_date: str,
    payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not _as_bool(payload.get("same_symbol_reentry_reset_enabled"), True):
        return None
    reset_reasons = _reentry_reset_reasons(payload)
    matching: list[tuple[datetime, Mapping[str, Any], str]] = []
    for position in positions:
        if str(position.get("root_symbol") or "").upper() != symbol:
            continue
        if _position_status(position) != "closed":
            continue
        opened = str(position.get("market_date_opened") or position.get("market_date") or "")
        closed = str(position.get("market_date_closed") or opened or "")
        if opened != session_date and closed != session_date:
            continue
        reason = (_as_text(position.get("last_exit_reason")) or "").lower()
        if reason not in reset_reasons:
            continue
        closed_at = parse_datetime(position.get("closed_at"))
        if closed_at is None:
            continue
        matching.append((closed_at.astimezone(UTC), position, reason))
    if not matching:
        return None

    closed_at, position, reason = max(matching, key=lambda item: item[0])
    feed_generated_at = parse_datetime(snapshot.get("generated_at"))
    current_feed_run_id = _as_text(snapshot.get("job_run_id"))
    opening_source = _opening_intent_source(execution_store, position)
    opening_feed_run_id = _as_text(opening_source.get("feed_job_run_id"))
    feed_is_after_close = (
        feed_generated_at is not None
        and feed_generated_at.astimezone(UTC) > closed_at
    )
    feed_run_changed = (
        opening_feed_run_id is None
        or current_feed_run_id is None
        or opening_feed_run_id != current_feed_run_id
    )
    details = {
        "previous_position_id": position.get("position_id"),
        "last_exit_reason": reason,
        "closed_at": closed_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "feed_generated_at": None
        if feed_generated_at is None
        else feed_generated_at.astimezone(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "feed_job_run_id": current_feed_run_id,
        "opening_feed_job_run_id": opening_feed_run_id,
        "feed_is_after_close": feed_is_after_close,
        "feed_run_changed": feed_run_changed,
        "entry_setup": {
            "finviz_rank": entry.get("finviz_rank"),
            "move_percent": entry.get("move_percent"),
            "price": entry.get("price"),
        },
    }
    if feed_is_after_close and feed_run_changed:
        return None
    return {
        "passed": False,
        "reason": "same_symbol_setup_not_reset",
        "same_symbol_reentry": details,
    }


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


def _issue_option_intent(
    *,
    execution_store: Any,
    intent_id: str,
    slot_key: str,
    bot_id: str,
    automation_id: str,
    feed_id: str,
    side: str,
    underlying_symbol: str,
    option_symbol: str,
    quantity: int,
    limit_price: float,
    session_date: str,
    payload: Mapping[str, Any],
    source: Mapping[str, Any],
    option_selection: Mapping[str, Any],
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

    expiration_date = _as_text(option_selection.get("expiration_date"))
    strike = _as_float(option_selection.get("strike"))
    position_intent = "buy_to_open" if trade_intent == "open" else "sell_to_close"
    intent_payload: dict[str, Any] = {
        "asset_class": "option",
        "label": _as_text(payload.get("label")) or "finviz_direct",
        "market_date": session_date,
        "underlying_symbol": underlying_symbol,
        "root_symbol": underlying_symbol,
        "symbol": option_symbol,
        "side": side,
        "quantity": quantity,
        "limit_price": limit_price,
        "time_in_force": _as_text(payload.get("option_time_in_force"))
        or _as_text(payload.get("time_in_force"))
        or "day",
        "trade_intent": trade_intent,
        "position_intent": position_intent,
        "strategy_family": "long_call",
        "expiration_date": expiration_date,
        "option_type": "call",
        "strike": strike,
        "execution_runtime": _as_text(payload.get("option_execution_runtime"))
        or ALPACA_DIRECT_RUNTIME,
        "approval_mode": _as_text(payload.get("approval_mode")) or "auto",
        "execution_mode": _as_text(payload.get("execution_mode")) or "paper",
        "source": dict(source),
        "entry_rules": _mapping(payload.get("entry_rules")),
        "option_entry_rules": _mapping(payload.get("option_entry_rules")),
        "exit_policy": _mapping(
            payload.get("option_exit_rules") or payload.get("exit_rules")
        ),
        "option_selection": dict(option_selection),
        "legs": [
            {
                "symbol": option_symbol,
                "side": side,
                "position_intent": position_intent,
                "ratio_qty": "1",
                "role": "long",
                "expiration_date": expiration_date,
                "strike": strike,
                "option_type": "call",
            }
        ],
    }
    underlying_price = _as_float(option_selection.get("underlying_price"))
    if underlying_price is not None:
        intent_payload["underlying_price"] = underlying_price
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
            "option_selection": dict(option_selection),
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


def _underlying_invalidation_decision(
    *,
    quote_metrics: Mapping[str, Any],
    stats: Mapping[str, Any],
    rules: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    invalidation_rules = _mapping(rules.get("underlying_invalidation"))
    enabled = _as_bool(
        invalidation_rules.get(
            "enabled",
            _rule_value(rules, payload, "underlying_invalidation_enabled", False),
        ),
        False,
    )
    if not enabled:
        return None

    bid = _as_float(quote_metrics.get("bid"))
    midpoint = _as_float(quote_metrics.get("midpoint"))
    price = bid or midpoint
    if price is None or price <= 0:
        return {"triggered": False, "reason": "underlying_quote_unavailable"}
    age_seconds = _as_float(quote_metrics.get("age_seconds"))
    max_quote_age_seconds = max(
        _as_int(
            invalidation_rules.get(
                "max_quote_age_seconds",
                _rule_value(rules, payload, "underlying_max_quote_age_seconds", 180),
            ),
            180,
        ),
        1,
    )
    if age_seconds is None:
        return {"triggered": False, "reason": "underlying_quote_timestamp_missing"}
    if age_seconds > max_quote_age_seconds:
        return {"triggered": False, "reason": "underlying_quote_stale"}
    spread_pct = _as_float(quote_metrics.get("spread_pct"))
    max_spread_pct = _as_float(
        invalidation_rules.get(
            "max_spread_pct",
            _rule_value(rules, payload, "underlying_max_spread_pct", 1.0),
        )
    )
    if max_spread_pct is not None and (
        spread_pct is None or spread_pct > max_spread_pct
    ):
        return {"triggered": False, "reason": "underlying_spread_too_wide"}

    min_bars = max(
        _as_int(
            invalidation_rules.get(
                "min_intraday_bars",
                _rule_value(rules, payload, "underlying_min_intraday_bars", 30),
            ),
            30,
        ),
        1,
    )
    bar_count = _as_int(stats.get("bar_count"), 0)
    if bar_count < min_bars:
        return {"triggered": False, "reason": "underlying_insufficient_intraday_bars"}
    vwap = _as_float(stats.get("vwap"))
    recent_low = _as_float(stats.get("recent_low"))
    if vwap is None or recent_low is None:
        return {"triggered": False, "reason": "underlying_timing_metrics_unavailable"}

    tolerance_bps = max(
        _as_float(
            invalidation_rules.get(
                "breakdown_tolerance_bps",
                _rule_value(rules, payload, "underlying_breakdown_tolerance_bps", 5.0),
            )
        )
        or 0.0,
        0.0,
    )
    tolerance = tolerance_bps / 10_000.0
    threshold = recent_low * (1.0 + tolerance)
    confirmation_bars = max(
        _as_int(
            invalidation_rules.get(
                "confirmation_bars",
                _rule_value(rules, payload, "underlying_confirmation_bars", 2),
            ),
            2,
        ),
        0,
    )
    confirmation_closes = [
        float(value)
        for value in stats.get("confirmation_closes") or []
        if _as_float(value) is not None
    ]
    if confirmation_bars > 0 and len(confirmation_closes) < confirmation_bars:
        return {"triggered": False, "reason": "underlying_insufficient_confirmation_bars"}
    confirmed = True
    if confirmation_bars > 0:
        confirmed = all(
            close <= vwap and close <= threshold
            for close in confirmation_closes[-confirmation_bars:]
        )
    triggered = price <= vwap and price <= threshold and confirmed
    return {
        "triggered": triggered,
        "reason": (
            "underlying_vwap_recent_low_break"
            if triggered
            else "underlying_invalidation_not_triggered"
        ),
        "underlying_price": round(price, 4),
        "underlying_vwap": round(vwap, 4),
        "underlying_recent_low": round(recent_low, 4),
        "underlying_spread_pct": None
        if spread_pct is None
        else round(spread_pct, 4),
        "underlying_quote_age_seconds": round(age_seconds, 1),
        "underlying_bar_count": bar_count,
        "underlying_confirmation_bars": confirmation_bars,
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
    option_entry_rules = _mapping(payload.get("option_entry_rules"))
    option_exit_rules = _mapping(payload.get("option_exit_rules")) or exit_rules
    instrument_mode = _instrument_mode(payload)
    equity_fallback = _as_bool(payload.get("equity_fallback"), True)
    stock_feed = _as_text(payload.get("stock_feed")) or "iex"
    max_candidates = max(_as_int(payload.get("max_candidates"), 3), 1)
    max_new_positions_per_run = max(
        _as_int(
            payload.get(
                "max_new_positions_per_run",
                option_entry_rules.get("max_new_positions_per_run", 0),
            ),
            0,
        ),
        0,
    )
    max_open_positions = max(
        _as_int(
            payload.get(
                "max_open_positions",
                option_entry_rules.get("max_open_positions", 0),
            ),
            0,
        ),
        0,
    )
    max_daily_entries = max(
        _as_int(
            payload.get(
                "max_daily_entries",
                payload.get(
                    "max_session_entries",
                    option_entry_rules.get(
                        "max_daily_entries",
                        option_entry_rules.get("max_session_entries", 0),
                    ),
                ),
            ),
            0,
        ),
        0,
    )
    entry_side = (_as_text(entry_rules.get("side")) or "buy").lower()
    if entry_side not in {"buy", "sell"}:
        entry_side = "buy"
    allow_short_selling = _as_bool(payload.get("allow_short_selling"), False)

    feed_entries = _feed_entry_by_symbol(snapshot) if snapshot_status == "ready" else {}
    feed_symbols = set(feed_entries)
    feed_available = snapshot_status in {"ready", "empty"}
    managed_session_positions = _local_position_rows(
        execution_store,
        bot_id=bot_id,
        automation_id=automation_id,
        strategy_config_id=feed_id,
    )
    managed_positions = [
        row
        for row in managed_session_positions
        if _position_status(row) in OPEN_POSITION_STATUSES
    ]
    managed_by_symbol = {
        str(position.get("root_symbol")).upper(): position
        for position in managed_positions
        if _as_text(position.get("root_symbol")) is not None
    }
    broker_positions = _broker_positions_by_symbol(client)
    active_entry_intents = _active_entry_intent_count(
        execution_store,
        bot_id=bot_id,
        automation_id=automation_id,
        feed_id=feed_id,
    )
    optionable_symbols: set[str] | None = None
    optionable_error: str | None = None
    if instrument_mode == "long_call":
        optionable_symbols, optionable_error = _optionable_symbol_set(client)

    entry_symbols = list(feed_entries)[:max_candidates]
    exit_symbols = list(managed_by_symbol)
    all_symbols = sorted(set(entry_symbols) | set(exit_symbols))
    snapshots = client.get_stock_snapshots(all_symbols, feed=stock_feed) if all_symbols else {}

    now = _now()
    daily_pnl = _daily_pnl_snapshot(
        managed_session_positions,
        session_date=session_date,
    )
    filled_entry_attempt_count = _finviz_filled_entry_attempt_count(
        execution_store,
        bot_id=bot_id,
        automation_id=automation_id,
        feed_id=feed_id,
        session_date=session_date,
    )
    max_daily_loss = _as_float(
        payload.get("max_daily_loss", entry_rules.get("max_daily_loss"))
    )
    daily_loss_reached = (
        max_daily_loss is not None
        and max_daily_loss > 0
        and (_as_float(daily_pnl.get("daily_total_pnl")) or 0.0) <= -abs(max_daily_loss)
    )
    bars_by_symbol: dict[str, list[Any]] = {}
    decisions: list[dict[str, Any]] = []
    armed = 0
    entry_armed = 0

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
        if daily_loss_reached:
            decisions.append(
                {
                    **decision,
                    "passed": False,
                    "reason": "daily_loss_limit_reached",
                    "daily_pnl": dict(daily_pnl),
                    "max_daily_loss": max_daily_loss,
                }
            )
            continue
        if entry_side == "sell" and not allow_short_selling:
            decisions.append(
                {**decision, "passed": False, "reason": "short_selling_disabled"}
            )
            continue
        entry_budget = _daily_entry_budget_snapshot(
            managed_session_positions,
            session_date=session_date,
            max_daily_entries=max_daily_entries,
            filled_entry_attempt_count=filled_entry_attempt_count,
            active_entry_intents=active_entry_intents,
            entry_armed=entry_armed,
        )
        if entry_budget["budget_reached"]:
            decisions.append(
                {
                    **decision,
                    "passed": False,
                    "reason": "max_daily_entries_reached",
                    "daily_entry_budget": entry_budget,
                }
            )
            continue
        open_exposure_count = len(managed_positions) + active_entry_intents + entry_armed
        if max_open_positions > 0 and open_exposure_count >= max_open_positions:
            decisions.append(
                {**decision, "passed": False, "reason": "max_open_positions_reached"}
            )
            continue
        if max_new_positions_per_run > 0 and entry_armed >= max_new_positions_per_run:
            decisions.append(
                {
                    **decision,
                    "passed": False,
                    "reason": "max_new_positions_per_run_reached",
                }
            )
            continue
        if entry_side == "buy" and symbol in managed_by_symbol:
            decisions.append(
                {**decision, "passed": False, "reason": "position_already_open"}
            )
            continue
        if (
            instrument_mode == "equity"
            and entry_side == "buy"
            and _broker_qty(broker_positions.get(symbol)) > 0
        ):
            decisions.append(
                {**decision, "passed": False, "reason": "broker_position_already_open"}
            )
            continue
        reentry_decision = _same_symbol_reentry_reset_decision(
            execution_store=execution_store,
            positions=managed_session_positions,
            symbol=symbol,
            entry=entry,
            snapshot=snapshot,
            session_date=session_date,
            payload=payload,
        )
        if reentry_decision is not None:
            decisions.append({**decision, **reentry_decision})
            continue

        timing_rules = _mapping(entry_rules.get("timing"))
        lookback_bars = max(
            _as_int(
                timing_rules.get(
                    "recent_lookback_bars",
                    _rule_value(entry_rules, payload, "recent_lookback_bars", 5),
                ),
                5,
            ),
            1,
        )
        confirmation_bars = max(
            _as_int(
                timing_rules.get(
                    "confirmation_bars",
                    _rule_value(entry_rules, payload, "confirmation_bars", 0),
                ),
                0,
            ),
            0,
        )
        bars = client.get_intraday_bars(
            symbol,
            start=_session_start(now).isoformat().replace("+00:00", "Z"),
            end=now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            stock_feed=stock_feed,
            timeframe=_as_text(payload.get("bar_timeframe")) or "1Min",
        )
        bars_by_symbol[symbol] = bars
        quote_metrics = _quote_metrics(snapshots.get(symbol, {}), now=now)
        stats = _bar_stats(
            bars,
            lookback_bars=lookback_bars,
            confirmation_bars=confirmation_bars,
        )
        timing = _timing_decision(
            side=entry_side,
            quote_metrics=quote_metrics,
            stats=stats,
            rules=entry_rules,
            payload=payload,
            now=now,
        )
        decision = {**decision, **timing}
        if not timing.get("triggered"):
            decisions.append(decision)
            continue

        source = {
            "source": "finviz_direct_trading",
            "flow": "finviz_direct",
            "feed_id": feed_id,
            "feed_job_key": feed_job_key,
            "feed_job_run_id": snapshot.get("job_run_id"),
            "trading_job_run_id": job_run_id,
            "feed_entry": dict(entry),
        }

        if instrument_mode == "long_call":
            if entry_side != "buy":
                decisions.append(
                    {
                        **decision,
                        "triggered": False,
                        "reason": "long_call_requires_buy_entry",
                    }
                )
                continue
            if optionable_symbols is None:
                if not equity_fallback:
                    decisions.append(
                        {
                            **decision,
                            "triggered": False,
                            "reason": "optionable_lookup_unavailable",
                            "error": optionable_error,
                        }
                    )
                    continue
            elif symbol not in optionable_symbols:
                if not equity_fallback:
                    decisions.append(
                        {
                            **decision,
                            "triggered": False,
                            "reason": "underlying_not_optionable",
                        }
                    )
                    continue
            if optionable_symbols is not None and symbol in optionable_symbols:
                underlying_price = (
                    _as_float(timing.get("price"))
                    or _as_float(quote_metrics.get("midpoint"))
                    or _as_float(rule_decision.get("price"))
                    or 0.0
                )
                option_selection = _select_long_call_contract(
                    client=client,
                    symbol=symbol,
                    underlying_price=underlying_price,
                    rules=option_entry_rules,
                    payload=payload,
                    now=now,
                    heartbeat=heartbeat,
                )
                if not option_selection.get("selected"):
                    if not equity_fallback:
                        decisions.append(
                            {
                                **decision,
                                "triggered": False,
                                "reason": option_selection.get("reason")
                                or "long_call_unavailable",
                                "option_selection": option_selection,
                            }
                        )
                        continue
                else:
                    option_quote_metrics = _mapping(option_selection.get("quote_metrics"))
                    limit_price = _limit_price(
                        side="buy",
                        quote_metrics=option_quote_metrics,
                        payload=payload,
                        rules=option_entry_rules,
                    )
                    quantity = _option_quantity(
                        price=limit_price,
                        payload=payload,
                        rules=option_entry_rules,
                    )
                    if quantity <= 0:
                        decisions.append(
                            {
                                **decision,
                                "triggered": False,
                                "reason": "option_quantity_resolved_to_zero",
                                "option_selection": option_selection,
                            }
                        )
                        continue
                    option_selection = {
                        **option_selection,
                        "underlying_price": underlying_price,
                    }
                    option_symbol = str(option_selection["symbol"])
                    intent_id = (
                        "execution_intent:finviz_direct:entry:"
                        f"{_safe_component(snapshot.get('job_run_id'))}:"
                        f"{symbol}:{option_symbol}:buy"
                    )
                    slot_key = f"finviz_direct:{feed_id}:{symbol}:long_call:entry"
                    intent_result = _issue_option_intent(
                        execution_store=execution_store,
                        intent_id=intent_id,
                        slot_key=slot_key,
                        bot_id=bot_id,
                        automation_id=automation_id,
                        feed_id=feed_id,
                        side="buy",
                        underlying_symbol=symbol,
                        option_symbol=option_symbol,
                        quantity=quantity,
                        limit_price=limit_price,
                        session_date=session_date,
                        payload=payload,
                        source=source,
                        option_selection=option_selection,
                        timing=timing,
                        trade_intent="open",
                    )
                    if intent_result.get("created"):
                        armed += 1
                        entry_armed += 1
                    decisions.append(
                        {
                            **decision,
                            "instrument": "long_call",
                            "option_symbol": option_symbol,
                            "option_selection": option_selection,
                            "limit_price": limit_price,
                            "quantity": quantity,
                            **{
                                key: intent_result.get(key)
                                for key in (
                                    "created",
                                    "reason",
                                    "execution_intent_id",
                                )
                                if key in intent_result
                            },
                        }
                    )
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
            entry_armed += 1
        decisions.append(
            {
                **decision,
                "instrument": "equity",
                "limit_price": limit_price,
                "quantity": quantity,
                **{
                    key: intent_result.get(key)
                    for key in ("created", "reason", "execution_intent_id")
                    if key in intent_result
                },
            }
        )

    for position in managed_positions:
        heartbeat()
        symbol = str(position.get("root_symbol") or "").upper()
        if not symbol:
            decisions.append(
                {
                    "kind": "exit",
                    "symbol": None,
                    "side": "sell",
                    "position_id": position.get("position_id"),
                    "triggered": False,
                    "reason": "position_root_symbol_missing",
                }
            )
            continue
        position_family = _position_strategy_family(position)
        if position_family == "long_call":
            option_symbol = _position_option_symbol(position)
            if option_symbol is None:
                decisions.append(
                    {
                        "kind": "exit",
                        "symbol": symbol,
                        "side": "sell",
                        "position_id": position.get("position_id"),
                        "triggered": False,
                        "reason": "option_position_symbol_unavailable",
                    }
                )
                continue
            option_quotes, option_quote_sources, option_quote_error = (
                fetch_latest_option_quotes(
                    [option_symbol],
                    client=client,
                    feeds=_option_quote_feeds(option_exit_rules, payload),
                )
            )
            quote_metrics = _live_option_quote_metrics(
                option_quotes.get(option_symbol, {}),
                now=now,
            )
            exit_decision = _evaluate_exit(
                position=position,
                broker_position=broker_positions.get(option_symbol),
                quote_metrics=quote_metrics,
                feed_symbols=feed_symbols,
                feed_available=feed_available,
                clock=clock,
                rules=option_exit_rules,
                now=now,
            )
            underlying_exit = None
            if not exit_decision.get("triggered") and exit_decision.get("reason") == "hold":
                invalidation_rules = _mapping(
                    option_exit_rules.get("underlying_invalidation")
                )
                if _as_bool(
                    invalidation_rules.get(
                        "enabled",
                        option_exit_rules.get("underlying_invalidation_enabled"),
                    ),
                    False,
                ):
                    underlying_bars = bars_by_symbol.get(symbol)
                    if underlying_bars is None:
                        underlying_bars = client.get_intraday_bars(
                            symbol,
                            start=_session_start(now).isoformat().replace(
                                "+00:00",
                                "Z",
                            ),
                            end=now.isoformat(timespec="seconds").replace(
                                "+00:00",
                                "Z",
                            ),
                            stock_feed=stock_feed,
                            timeframe=_as_text(payload.get("bar_timeframe")) or "1Min",
                        )
                        bars_by_symbol[symbol] = underlying_bars
                    underlying_lookback_bars = max(
                        _as_int(
                            invalidation_rules.get("recent_lookback_bars", 5),
                            5,
                        ),
                        1,
                    )
                    underlying_confirmation_bars = max(
                        _as_int(
                            invalidation_rules.get("confirmation_bars", 2),
                            2,
                        ),
                        0,
                    )
                    underlying_stats = _bar_stats(
                        underlying_bars,
                        lookback_bars=underlying_lookback_bars,
                        confirmation_bars=underlying_confirmation_bars,
                    )
                    underlying_quote_metrics = _quote_metrics(
                        snapshots.get(symbol, {}),
                        now=now,
                    )
                    underlying_exit = _underlying_invalidation_decision(
                        quote_metrics=underlying_quote_metrics,
                        stats=underlying_stats,
                        rules=option_exit_rules,
                        payload=payload,
                    )
                    if underlying_exit is not None and underlying_exit.get("triggered"):
                        exit_decision = {
                            **exit_decision,
                            "triggered": True,
                            "reason": str(underlying_exit.get("reason")),
                            "underlying_exit": underlying_exit,
                        }
            decision = {
                "kind": "exit",
                "symbol": symbol,
                "instrument": "long_call",
                "option_symbol": option_symbol,
                "side": "sell",
                "position_id": position.get("position_id"),
                "quote_error": option_quote_error,
                **exit_decision,
            }
            if underlying_exit is not None and not underlying_exit.get("triggered"):
                decision["underlying_exit"] = underlying_exit
            if not exit_decision.get("triggered"):
                decisions.append(decision)
                continue
            limit_price = _limit_price(
                side="sell",
                quote_metrics=quote_metrics,
                payload=payload,
                rules=option_exit_rules,
            )
            max_exit_quantity = int(
                max(
                    min(
                        _broker_qty(broker_positions.get(option_symbol)),
                        _as_float(position.get("remaining_quantity")) or 0.0,
                    ),
                    0.0,
                )
            )
            quantity = _quantity(
                price=limit_price,
                payload=payload,
                rules=option_exit_rules,
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
                f"{_safe_component(job_run_id)}:{_safe_component(position_id)}:"
                f"{_safe_component(option_symbol)}:{reason}"
            )
            slot_key = f"finviz_direct:{feed_id}:{position_id}:long_call:exit"
            source = {
                "source": "finviz_direct_trading",
                "flow": "finviz_direct",
                "feed_id": feed_id,
                "feed_job_key": feed_job_key,
                "feed_job_run_id": snapshot.get("job_run_id"),
                "trading_job_run_id": job_run_id,
                "exit_reason": reason,
                "option_symbol": option_symbol,
            }
            legs = position_legs(position)
            leg = legs[0] if legs else {}
            option_selection = {
                "selected": True,
                "reason": "long_call_exit",
                "symbol": option_symbol,
                "expiration_date": _as_text(leg.get("expiration_date")),
                "strike": _as_float(leg.get("strike")),
                "quote_metrics": quote_metrics,
                "quote_source": option_quote_sources.get(option_symbol),
            }
            intent_result = _issue_option_intent(
                execution_store=execution_store,
                intent_id=intent_id,
                slot_key=slot_key,
                bot_id=bot_id,
                automation_id=automation_id,
                feed_id=feed_id,
                side="sell",
                underlying_symbol=symbol,
                option_symbol=option_symbol,
                quantity=quantity,
                limit_price=limit_price,
                session_date=session_date,
                payload=payload,
                source=source,
                option_selection=option_selection,
                timing=exit_decision,
                trade_intent="close",
                position_id=position_id,
            )
            if intent_result.get("created"):
                armed += 1
            decisions.append(
                {
                    **decision,
                    "option_selection": option_selection,
                    "limit_price": limit_price,
                    "quantity": quantity,
                    **{
                        key: intent_result.get(key)
                        for key in ("created", "reason", "execution_intent_id")
                        if key in intent_result
                    },
                }
            )
            continue

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
                "instrument": "equity",
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
        "instrument_mode": instrument_mode,
        "entry_candidates": len(entry_symbols),
        "managed_positions": len(managed_positions),
        "active_entry_intents": active_entry_intents,
        "daily_pnl": daily_pnl,
        "max_daily_entries": max_daily_entries if max_daily_entries > 0 else None,
        "daily_entry_budget": _daily_entry_budget_snapshot(
            managed_session_positions,
            session_date=session_date,
            max_daily_entries=max_daily_entries,
            filled_entry_attempt_count=filled_entry_attempt_count,
            active_entry_intents=active_entry_intents,
            entry_armed=entry_armed,
        ),
        "max_daily_loss": max_daily_loss,
        "daily_loss_reached": daily_loss_reached,
        "armed": armed,
        "entry_armed": entry_armed,
        "decisions": decisions,
        "dispatch_result": dispatch_result,
    }


__all__ = ["run_finviz_direct_trading"]

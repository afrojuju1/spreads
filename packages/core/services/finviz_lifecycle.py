from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
import re
from typing import Any

from core.services.trading_lifecycle import (
    LifecycleObject,
    TradeDecisionState,
    TradeSignalState,
)

BLOCKING_DECISION_REASONS = frozenset(
    {
        "active_intent_exists",
        "broker_position_already_open",
        "daily_loss_limit_reached",
        "exit_quantity_zero",
        "intent_exists",
        "long_call_requires_buy_entry",
        "max_daily_entries_reached",
        "max_new_positions_per_run_reached",
        "max_open_positions_reached",
        "option_quantity_resolved_to_zero",
        "optionable_lookup_unavailable",
        "position_already_open",
        "quantity_resolved_to_zero",
        "same_symbol_setup_not_reset",
        "short_selling_disabled",
        "underlying_not_optionable",
    }
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _safe_component(value: Any) -> str:
    rendered = str(value or "").strip()
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", rendered) or "unknown"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _reason_codes(*values: Any) -> list[str]:
    codes: list[str] = []
    for value in values:
        if isinstance(value, list):
            for item in value:
                rendered = _as_text(item)
                if rendered is not None and rendered not in codes:
                    codes.append(rendered)
            continue
        rendered = _as_text(value)
        if rendered is not None and rendered not in codes:
            codes.append(rendered)
    return codes


def finviz_entry_decision_state(decision: Mapping[str, Any]) -> str:
    if bool(decision.get("created")):
        return TradeDecisionState.SELECTED.value
    reason = str(decision.get("reason") or "").strip().lower()
    if reason in BLOCKING_DECISION_REASONS:
        return TradeDecisionState.SELECTED_BLOCKED.value
    if decision.get("triggered") is False:
        return TradeDecisionState.NO_ENTRY.value
    if decision.get("passed") is False:
        return TradeDecisionState.SKIP.value
    if decision.get("created") is False:
        return TradeDecisionState.SELECTED_BLOCKED.value
    return TradeDecisionState.NO_ENTRY.value


def finviz_entry_signal_state(decision_state: str) -> str:
    if decision_state == TradeDecisionState.SELECTED.value:
        return TradeSignalState.CONSUMED.value
    if decision_state == TradeDecisionState.NO_ENTRY.value:
        return TradeSignalState.READY.value
    return TradeSignalState.BLOCKED.value


def build_finviz_entry_lifecycle(
    *,
    feed_id: str,
    feed_job_key: str,
    feed_job_run_id: Any,
    trading_job_run_id: str,
    session_date: str,
    symbol: str,
    entry: Mapping[str, Any],
    decision: Mapping[str, Any],
    decided_at: str | None = None,
) -> dict[str, Any]:
    resolved_decided_at = decided_at or _utc_now()
    signal_id = f"trade_signal:finviz:{_safe_component(feed_id)}:" f"{_safe_component(feed_job_run_id)}:{_safe_component(symbol)}"
    decision_id = f"trade_decision:finviz:{_safe_component(trading_job_run_id)}:" f"{_safe_component(symbol)}:entry"
    decision_state = finviz_entry_decision_state(decision)
    signal_state = finviz_entry_signal_state(decision_state)
    reason_codes = _reason_codes(
        decision.get("reason"),
        entry.get("reason_codes"),
        entry.get("source_tags"),
    )
    blockers = (
        _reason_codes(decision.get("reason")) if decision_state in {TradeDecisionState.SKIP.value, TradeDecisionState.SELECTED_BLOCKED.value} else []
    )
    signal = {
        "trade_signal_id": signal_id,
        "object_type": LifecycleObject.TRADE_SIGNAL.value,
        "signal_state": signal_state,
        "source_kind": "finviz_screener",
        "source_id": feed_id,
        "source_job_key": feed_job_key,
        "source_job_run_id": feed_job_run_id,
        "session_date": session_date,
        "market_session": "regular",
        "observed_at": entry.get("generated_at") or resolved_decided_at,
        "underlying_symbol": symbol,
        "root_symbol": symbol,
        "asset_class": "equity",
        "strategy_family": "momentum",
        "product_class": "underlying_or_long_call",
        "rank": entry.get("finviz_rank"),
        "score": entry.get("score"),
        "confidence": entry.get("confidence"),
        "reason_codes": reason_codes,
        "blockers": blockers,
        "evidence": {
            "feed_entry": dict(entry),
        },
        "metrics": {
            "price": entry.get("price"),
            "market_cap": entry.get("market_cap"),
            "move_percent": entry.get("move_percent"),
            "relative_volume": entry.get("relative_volume"),
            "daily_volume": entry.get("daily_volume"),
        },
    }
    execution_shape = {
        "instrument": decision.get("instrument"),
        "side": decision.get("side"),
        "quantity": decision.get("quantity"),
        "limit_price": decision.get("limit_price"),
        "symbol": decision.get("option_symbol") or decision.get("symbol"),
    }
    decision_record = {
        "trade_decision_id": decision_id,
        "object_type": LifecycleObject.TRADE_DECISION.value,
        "trade_signal_id": signal_id,
        "decision_state": decision_state,
        "bot_id": "finviz",
        "automation_id": "finviz_direct",
        "run_key": str(trading_job_run_id),
        "scope_key": f"finviz:{feed_id}:{symbol}:entry",
        "rank": entry.get("finviz_rank"),
        "score": entry.get("score"),
        "selected_quantity": decision.get("quantity") if decision_state == TradeDecisionState.SELECTED.value else None,
        "selected_execution_shape": execution_shape if decision_state == TradeDecisionState.SELECTED.value else {},
        "reason_codes": reason_codes,
        "blockers": blockers,
        "evidence": {
            "reason": decision.get("reason"),
            "passed": decision.get("passed"),
            "triggered": decision.get("triggered"),
            "created": decision.get("created"),
            "execution_intent_id": decision.get("execution_intent_id"),
            "option_selection": decision.get("option_selection"),
            "same_symbol_reentry": decision.get("same_symbol_reentry"),
            "daily_entry_budget": decision.get("daily_entry_budget"),
        },
        "metrics": {
            "price": decision.get("price") or entry.get("price"),
            "spread_pct": decision.get("spread_pct"),
            "quote_age_seconds": decision.get("quote_age_seconds"),
            "underlying_price": decision.get("underlying_price"),
            "limit_price": decision.get("limit_price"),
            "quantity": decision.get("quantity"),
        },
        "decided_at": resolved_decided_at,
    }
    return {
        "trade_signal": signal,
        "trade_decision": decision_record,
    }


def attach_finviz_entry_lifecycle(
    decisions: list[dict[str, Any]],
    *,
    feed_id: str,
    feed_job_key: str,
    feed_job_run_id: Any,
    trading_job_run_id: str,
    session_date: str,
    feed_entries: Mapping[str, Mapping[str, Any]],
    decided_at: str | None = None,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for decision in decisions:
        if str(decision.get("kind") or "") != "entry":
            enriched.append(decision)
            continue
        symbol = _as_text(decision.get("symbol"))
        if symbol is None:
            enriched.append(decision)
            continue
        if isinstance(decision.get("lifecycle"), Mapping):
            enriched.append(decision)
            continue
        entry = feed_entries.get(symbol.upper())
        if not isinstance(entry, Mapping):
            enriched.append(decision)
            continue
        lifecycle = build_finviz_entry_lifecycle(
            feed_id=feed_id,
            feed_job_key=feed_job_key,
            feed_job_run_id=feed_job_run_id,
            trading_job_run_id=trading_job_run_id,
            session_date=session_date,
            symbol=symbol.upper(),
            entry=entry,
            decision=decision,
            decided_at=decided_at,
        )
        trade_decision = _mapping(lifecycle.get("trade_decision"))
        enriched.append(
            {
                **decision,
                "trade_signal_id": _mapping(lifecycle.get("trade_signal")).get("trade_signal_id"),
                "trade_decision_id": trade_decision.get("trade_decision_id"),
                "lifecycle_state": trade_decision.get("decision_state"),
                "lifecycle": lifecycle,
            }
        )
    return enriched


def summarize_lifecycle_decision_states(decisions: list[Any]) -> dict[str, int]:
    counts = Counter()
    for decision in decisions:
        if not isinstance(decision, Mapping):
            continue
        lifecycle_state = _as_text(decision.get("lifecycle_state"))
        if lifecycle_state is None:
            lifecycle = _mapping(decision.get("lifecycle"))
            lifecycle_state = _as_text(_mapping(lifecycle.get("trade_decision")).get("decision_state"))
        if lifecycle_state is not None:
            counts[lifecycle_state] += 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


__all__ = [
    "attach_finviz_entry_lifecycle",
    "build_finviz_entry_lifecycle",
    "finviz_entry_decision_state",
    "finviz_entry_signal_state",
    "summarize_lifecycle_decision_states",
]

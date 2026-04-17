from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.collections.models import LiveTickContext
from core.services.live_collector_health.capture import (
    build_quote_capture_summary,
    build_trade_capture_summary,
)
from core.services.live_collector_health.selection import build_selection_summary
from core.services.market_dates import NEW_YORK
from core.services.uoa_quote_summary import build_uoa_quote_summary
from core.services.uoa_root_decisions import build_uoa_root_decisions
from core.services.uoa_trade_summary import build_uoa_trade_summary


def resolve_collection_reference_time(slot_at: str | datetime | None) -> datetime:
    if isinstance(slot_at, datetime):
        return slot_at
    if isinstance(slot_at, str) and slot_at:
        normalized = (
            slot_at.replace("Z", "+00:00") if slot_at.endswith("Z") else slot_at
        )
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now(UTC)


def session_date_for_generated_at(generated_at: str) -> str:
    normalized = (
        generated_at.replace("Z", "+00:00")
        if generated_at.endswith("Z")
        else generated_at
    )
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(NEW_YORK).date().isoformat()


def build_skipped_tick_result(
    *,
    tick_context: LiveTickContext,
    status: str,
    reason: str,
    message: str,
    slot_status: str | None = None,
) -> dict[str, Any]:
    result = {
        "status": status,
        "reason": reason,
        "message": message,
        "iterations_completed": 0,
        "cycle_ids": [],
        "alerts_sent": 0,
        "quote_events_saved": 0,
        "baseline_quote_events_saved": 0,
        "stream_quote_events_saved": 0,
        "recovery_quote_events_saved": 0,
        "expected_quote_symbols": [],
        "trade_events_saved": 0,
        "stream_trade_events_saved": 0,
        "expected_trade_symbols": [],
        "signal_states_upserted": 0,
        "signal_transitions_recorded": 0,
        "opportunities_upserted": 0,
        "opportunities_expired": 0,
        "quote_capture": build_quote_capture_summary(
            expected_quote_symbols=[],
            total_quote_events_saved=0,
            baseline_quote_events_saved=0,
            stream_quote_events_saved=0,
            recovery_quote_events_saved=0,
        ),
        "trade_capture": build_trade_capture_summary(
            expected_trade_symbols=[],
            total_trade_events_saved=0,
            stream_trade_events_saved=0,
        ),
        "uoa_summary": build_uoa_trade_summary(
            expected_trade_symbols=[],
            trades=[],
        ),
        "uoa_quote_summary": build_uoa_quote_summary(
            as_of=tick_context.slot_at,
            expected_quote_symbols=[],
            quotes=[],
        ),
        "uoa_decisions": build_uoa_root_decisions(
            uoa_summary={},
            baselines_by_symbol={},
            quote_summary={},
            capture_window_seconds=0,
        ),
        "session_id": tick_context.session_id,
        "slot_at": tick_context.slot_at,
    }
    if slot_status is not None:
        result["slot_status"] = slot_status
    return result


def build_skipped_collection_result(
    *,
    reason: str,
    message: str,
    as_of: str | None = None,
) -> dict[str, Any]:
    return {
        "status": "skipped",
        "reason": reason,
        "message": message,
        "iterations_completed": 0,
        "cycle_ids": [],
        "alerts_sent": 0,
        "quote_events_saved": 0,
        "baseline_quote_events_saved": 0,
        "stream_quote_events_saved": 0,
        "recovery_quote_events_saved": 0,
        "expected_quote_symbols": [],
        "trade_events_saved": 0,
        "stream_trade_events_saved": 0,
        "expected_trade_symbols": [],
        "signal_states_upserted": 0,
        "signal_transitions_recorded": 0,
        "opportunities_upserted": 0,
        "opportunities_expired": 0,
        "quote_capture": build_quote_capture_summary(
            expected_quote_symbols=[],
            total_quote_events_saved=0,
            baseline_quote_events_saved=0,
            stream_quote_events_saved=0,
            recovery_quote_events_saved=0,
        ),
        "trade_capture": build_trade_capture_summary(
            expected_trade_symbols=[],
            total_trade_events_saved=0,
            stream_trade_events_saved=0,
        ),
        "uoa_summary": build_uoa_trade_summary(
            expected_trade_symbols=[],
            trades=[],
        ),
        "uoa_quote_summary": build_uoa_quote_summary(
            as_of=as_of or datetime.now(UTC).isoformat(),
            expected_quote_symbols=[],
            quotes=[],
        ),
        "uoa_decisions": build_uoa_root_decisions(
            uoa_summary={},
            baselines_by_symbol={},
            quote_summary={},
            capture_window_seconds=0,
        ),
        "selection_summary": build_selection_summary([]),
    }


__all__ = [
    "build_skipped_collection_result",
    "build_skipped_tick_result",
    "resolve_collection_reference_time",
    "session_date_for_generated_at",
]

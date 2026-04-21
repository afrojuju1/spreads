from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.value_coercion import as_text as _as_text
from core.storage.serializers import parse_datetime

from .shared import (
    CAPTURE_TARGET_REASON_MONITOR,
    CAPTURE_TARGET_REASON_OPEN_POSITION,
    CAPTURE_TARGET_REASON_PENDING_EXECUTION,
    CAPTURE_TARGET_REASON_PROMOTABLE,
    LIVE_SLOT_STATUS_SUCCEEDED,
    _option_sort_key,
    _slot_details,
    _slot_timestamp,
)


def _coverage_summary_for_slot(
    *,
    history_store: Any,
    continuity_rows: list[dict[str, Any]],
    slot_at: str | datetime,
    interval_seconds: int,
) -> dict[str, Any]:
    slot_at_dt = parse_datetime(slot_at)
    if slot_at_dt is None:
        return {
            "coverage_sufficient": False,
            "quote_target_symbols": [],
            "trade_target_symbols": [],
            "covered_quote_symbols": [],
            "covered_trade_symbols": [],
            "missing_quote_symbols": [],
            "missing_trade_symbols": [],
        }
    captured_to = slot_at_dt + timedelta(seconds=max(int(interval_seconds), 1))
    quote_target_symbols = sorted(
        {
            str(row["option_symbol"])
            for row in continuity_rows
            if bool(row.get("quote_enabled")) and _as_text(row.get("option_symbol"))
        }
    )
    trade_target_symbols = sorted(
        {
            str(row["option_symbol"])
            for row in continuity_rows
            if bool(row.get("trade_enabled")) and _as_text(row.get("option_symbol"))
        }
    )
    quote_summary = history_store.summarize_option_quote_window(
        option_symbols=quote_target_symbols,
        captured_from=slot_at_dt,
        captured_to=captured_to,
    )
    trade_summary = history_store.summarize_option_trade_window(
        option_symbols=trade_target_symbols,
        captured_from=slot_at_dt,
        captured_to=captured_to,
    )
    covered_quote_symbols = sorted(quote_summary.keys())
    covered_trade_symbols = sorted(trade_summary.keys())
    missing_quote_symbols = sorted(
        symbol for symbol in quote_target_symbols if symbol not in quote_summary
    )
    missing_trade_symbols = sorted(
        symbol for symbol in trade_target_symbols if symbol not in trade_summary
    )
    coverage_sufficient = bool(quote_target_symbols) and not missing_quote_symbols
    return {
        "coverage_sufficient": coverage_sufficient,
        "quote_target_symbols": quote_target_symbols,
        "trade_target_symbols": trade_target_symbols,
        "covered_quote_symbols": covered_quote_symbols,
        "covered_trade_symbols": covered_trade_symbols,
        "missing_quote_symbols": missing_quote_symbols,
        "missing_trade_symbols": missing_trade_symbols,
        "quote_summary": quote_summary,
        "trade_summary": trade_summary,
    }


def _latest_success_capture_targets_before_slot(
    *,
    slot_rows: Sequence[Mapping[str, Any]],
    slot_at: str | datetime,
) -> list[dict[str, Any]]:
    slot_at_dt = parse_datetime(slot_at)
    if slot_at_dt is None:
        return []
    for row in sorted(
        [dict(item) for item in slot_rows],
        key=lambda item: _slot_timestamp(item) or datetime.fromtimestamp(0, UTC),
        reverse=True,
    ):
        row_slot_at = _slot_timestamp(row)
        if row_slot_at is None or row_slot_at >= slot_at_dt:
            continue
        if str(row.get("status") or "") != LIVE_SLOT_STATUS_SUCCEEDED:
            continue
        capture_targets = _slot_details(row).get("capture_targets")
        if not isinstance(capture_targets, Mapping):
            return []
        rows: list[dict[str, Any]] = []
        for reason in (CAPTURE_TARGET_REASON_PROMOTABLE, CAPTURE_TARGET_REASON_MONITOR):
            rows.extend(
                dict(target)
                for target in list(capture_targets.get(reason) or [])
                if isinstance(target, Mapping)
            )
        return rows
    return []


def _continuity_rows_for_slot(
    *,
    recovery_store: Any,
    session_id: str,
    slot_rows: Sequence[Mapping[str, Any]],
    slot_at: str | datetime,
) -> list[dict[str, Any]]:
    rows_by_symbol: dict[str, dict[str, Any]] = {}
    for row in _latest_success_capture_targets_before_slot(
        slot_rows=slot_rows,
        slot_at=slot_at,
    ):
        option_symbol = _as_text(row.get("option_symbol"))
        if option_symbol is None:
            continue
        rows_by_symbol[option_symbol] = {
            **dict(row),
            "quote_enabled": bool(row.get("quote_enabled", True)),
            "trade_enabled": bool(row.get("trade_enabled", False)),
        }
    for row in recovery_store.list_capture_targets(
        session_id=session_id,
        reasons=[
            CAPTURE_TARGET_REASON_PENDING_EXECUTION,
            CAPTURE_TARGET_REASON_OPEN_POSITION,
        ],
        active_only=True,
        limit=200,
    ):
        option_symbol = _as_text(row.get("option_symbol"))
        if option_symbol is None:
            continue
        existing = rows_by_symbol.get(option_symbol, {})
        rows_by_symbol[option_symbol] = {
            **existing,
            **dict(row),
            "quote_enabled": bool(
                existing.get("quote_enabled", False) or row.get("quote_enabled", False)
            ),
            "trade_enabled": bool(
                existing.get("trade_enabled", False) or row.get("trade_enabled", False)
            ),
        }
    return sorted(rows_by_symbol.values(), key=_option_sort_key)


def _slot_should_be_marked_missed(
    *,
    job_store: Any,
    slot_row: Mapping[str, Any],
    now: datetime,
    stale_after_seconds: int,
) -> bool:
    slot_at = _slot_timestamp(slot_row)
    if slot_at is None:
        return False
    if now <= slot_at + timedelta(seconds=max(stale_after_seconds, 1)):
        return False
    job_run_id = _as_text(slot_row.get("job_run_id"))
    if job_run_id is None:
        return True
    run_record = job_store.get_job_run(job_run_id)
    if run_record is None:
        return True
    run_status = str(run_record.get("status") or "")
    if run_status in {"succeeded"}:
        return False
    if run_status in {"queued", "running"}:
        last_seen = (
            parse_datetime(_as_text(run_record.get("heartbeat_at")))
            or parse_datetime(_as_text(run_record.get("started_at")))
            or parse_datetime(_as_text(run_record.get("scheduled_for")))
        )
        if last_seen is not None and now <= last_seen + timedelta(
            seconds=max(stale_after_seconds, 1)
        ):
            return False
    return True

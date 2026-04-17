from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LiveTickContext:
    job_run_id: str
    session_id: str
    slot_at: str


@dataclass(frozen=True)
class LiveCaptureSnapshot:
    candidates: list[dict[str, Any]]
    contract_metadata_by_symbol: dict[str, dict[str, Any]]
    expected_quote_symbols: list[str]
    expected_trade_symbols: list[str]
    expected_uoa_roots: list[str]
    quote_event_count: int
    baseline_quote_event_count: int
    stream_quote_event_count: int
    recovery_quote_event_count: int
    trade_event_count: int
    stream_trade_event_count: int
    latest_quote_records: list[dict[str, Any]]
    stream_quote_records: list[dict[str, Any]]
    recovery_quote_records: list[dict[str, Any]]
    stream_trade_records: list[dict[str, Any]]
    reactive_quote_records: list[dict[str, Any]]
    quote_capture: dict[str, Any]
    trade_capture: dict[str, Any]
    uoa_summary: dict[str, Any]
    uoa_quote_summary: dict[str, Any]
    uoa_decisions: dict[str, Any]
    stream_quote_error: str | None
    stream_trade_error: str | None

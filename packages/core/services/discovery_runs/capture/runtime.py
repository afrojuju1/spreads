from __future__ import annotations

import argparse
import logging
from typing import Any

from core.integrations.alpaca.client import AlpacaClient
from core.observability.logging import log_event
from core.services.discovery_run_health.capture import (
    build_quote_capture_summary,
    build_trade_capture_summary,
)
from core.services.discovery_recovery import refresh_live_session_capture_targets
from core.services.option_quote_records import build_quote_symbol_metadata
from core.services.option_trade_records import build_trade_symbol_metadata
from core.services.runtime_identity import build_live_run_scope_id
from core.services.uoa_quote_summary import build_uoa_quote_summary
from core.services.uoa_root_decisions import build_uoa_root_decisions
from core.services.uoa_trade_baselines import build_uoa_trade_baselines
from core.services.uoa_trade_enrichment import enrich_uoa_trade_records
from core.services.uoa_trade_summary import build_uoa_trade_summary
from core.storage.event_repository import EventRepository
from core.storage.run_history_repository import RunHistoryRepository

from core.services.discovery_runs.models import LiveCaptureSnapshot, LiveTickContext

from .events import (
    record_market_capture_summary_event,
    record_uoa_decision_event,
    record_uoa_summary_event,
)
from .market_data import (
    MARKET_RECORDER_POLL_SECONDS,
    MARKET_RECORDER_WAIT_GRACE_SECONDS,
    collect_latest_quote_records,
    collect_recorded_market_data_records,
)

logger = logging.getLogger(__name__)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_stock_context_by_symbol(
    capture_candidates: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    context_by_symbol: dict[str, dict[str, Any]] = {}
    for candidate in capture_candidates:
        symbol = str(candidate.get("underlying_symbol") or "").strip()
        if not symbol:
            continue
        feed_rank = _optional_int(candidate.get("feed_rank"))
        current = context_by_symbol.get(symbol)
        current_rank = None if current is None else _optional_int(current.get("feed_rank"))
        current_score = None if current is None else _optional_float(current.get("feed_score"))
        candidate_score = _optional_float(candidate.get("feed_score"))
        if current is not None:
            if feed_rank is None and current_rank is not None:
                continue
            if (
                feed_rank is not None
                and current_rank is not None
                and feed_rank > current_rank
            ):
                continue
            if (
                feed_rank == current_rank
                and candidate_score is not None
                and current_score is not None
                and candidate_score < current_score
            ):
                continue
        context_by_symbol[symbol] = {
            "feed_rank": feed_rank,
            "feed_score": candidate_score,
            "daily_volume": _optional_int(candidate.get("underlying_daily_volume")),
            "move_percent": _optional_float(candidate.get("underlying_move_percent")),
            "news_count": _optional_int(candidate.get("underlying_news_count")) or 0,
            "most_active_rank": _optional_int(candidate.get("underlying_most_active_rank")),
            "gainer_rank": _optional_int(candidate.get("underlying_gainer_rank")),
            "loser_rank": _optional_int(candidate.get("underlying_loser_rank")),
            "trade_count": _optional_int(candidate.get("underlying_trade_count")),
            "price": _optional_float(candidate.get("underlying_price_snapshot")),
            "reason_codes": list(candidate.get("feed_reason_codes") or []),
            "source_tags": list(candidate.get("feed_source_tags") or []),
        }
    return context_by_symbol


def capture_live_option_market_state(
    *,
    args: argparse.Namespace,
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
    event_store: EventRepository,
    recovery_store: Any | None,
    label: str,
    cycle_id: str,
    generated_at: str,
    session_date: str,
    tick_context: LiveTickContext | None,
    capture_candidates: list[dict[str, Any]],
) -> LiveCaptureSnapshot:
    quote_event_count = 0
    baseline_quote_event_count = 0
    stream_quote_event_count = 0
    recovery_quote_event_count = 0
    trade_event_count = 0
    stream_trade_event_count = 0
    latest_quote_records: list[dict[str, Any]] = []
    stream_quote_records: list[dict[str, Any]] = []
    recovery_quote_records: list[dict[str, Any]] = []
    stream_trade_records: list[dict[str, Any]] = []
    stream_quote_error: str | None = None
    stream_trade_error: str | None = None
    contract_metadata_by_symbol = build_quote_symbol_metadata(capture_candidates)
    expected_quote_symbols = list(contract_metadata_by_symbol.keys())
    expected_trade_symbols = list(
        build_trade_symbol_metadata(capture_candidates).keys()
    )
    expected_uoa_roots = sorted(
        {
            str(candidate.get("underlying_symbol") or "").strip()
            for candidate in capture_candidates
            if str(candidate.get("underlying_symbol") or "").strip()
        }
    )
    if capture_candidates:
        if recovery_store is not None:
            try:
                refresh_live_session_capture_targets(
                    recovery_store=recovery_store,
                    session_id=(
                        tick_context.session_id
                        if tick_context is not None
                        else build_live_run_scope_id(label, session_date)
                    ),
                    session_date=session_date,
                    label=label,
                    profile=args.profile,
                    promotable_candidates=[],
                    monitor_candidates=[],
                    capture_candidates=capture_candidates,
                    feed=scanner_args.feed,
                    data_base_url=getattr(scanner_args, "data_base_url", None),
                    session_end_offset_minutes=int(
                        getattr(args, "session_end_offset_minutes", 0)
                    ),
                )
            except Exception as exc:
                log_event(
                    logger,
                    logging.WARNING,
                    "preselection_capture_target_refresh_unavailable",
                    label=label,
                    cycle_id=cycle_id,
                    session_date=session_date,
                    error=str(exc),
                    exc_info=True,
                )
        try:
            latest_quote_records = collect_latest_quote_records(
                client=client,
                candidates=capture_candidates,
                feed=scanner_args.feed,
            )
            baseline_quote_event_count = history_store.save_option_quote_events(
                cycle_id=cycle_id,
                label=label,
                profile=args.profile,
                quotes=latest_quote_records,
            )
            quote_event_count += baseline_quote_event_count
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "live_latest_quote_capture_unavailable",
                label=label,
                cycle_id=cycle_id,
                session_date=session_date,
                error=str(exc),
                exc_info=True,
            )
        trade_storage_ready = history_store.schema_has_tables("option_trade_events")
        if args.trade_capture_seconds > 0 and not trade_storage_ready:
            log_event(
                logger,
                logging.WARNING,
                "option_trade_capture_unavailable",
                label=label,
                cycle_id=cycle_id,
                session_date=session_date,
                reason="option_trade_events_table_missing",
            )
        stream_quote_duration_seconds = float(max(args.quote_capture_seconds, 0))
        stream_trade_duration_seconds = (
            float(args.trade_capture_seconds) if trade_storage_ready else 0.0
        )
        if stream_quote_duration_seconds > 0 or stream_trade_duration_seconds > 0:
            if recovery_store is None or not recovery_store.schema_ready():
                stream_quote_error = (
                    "Market recorder capture unavailable: recovery schema is not ready."
                )
                stream_trade_error = stream_quote_error
                log_event(
                    logger,
                    logging.WARNING,
                    "live_stream_market_data_capture_unavailable",
                    label=label,
                    cycle_id=cycle_id,
                    session_date=session_date,
                    reason=stream_quote_error,
                )
            else:
                capture_response = collect_recorded_market_data_records(
                    history_store=history_store,
                    label=label,
                    profile=args.profile,
                    expected_quote_symbols=expected_quote_symbols,
                    expected_trade_symbols=expected_trade_symbols,
                    captured_from=generated_at,
                    wait_timeout_seconds=(
                        MARKET_RECORDER_POLL_SECONDS
                        + max(
                            float(stream_quote_duration_seconds),
                            float(stream_trade_duration_seconds),
                        )
                        + MARKET_RECORDER_WAIT_GRACE_SECONDS
                    ),
                )
                stream_quote_records = [
                    dict(item)
                    for item in (capture_response.get("quotes") or [])
                    if isinstance(item, dict)
                ]
                stream_trade_records = [
                    dict(item)
                    for item in (capture_response.get("trades") or [])
                    if isinstance(item, dict)
                ]
                stream_quote_error = (
                    None
                    if capture_response.get("quote_error") in (None, "")
                    else str(capture_response.get("quote_error"))
                )
                stream_trade_error = (
                    None
                    if capture_response.get("trade_error") in (None, "")
                    else str(capture_response.get("trade_error"))
                )
                if stream_quote_error:
                    log_event(
                        logger,
                        logging.WARNING,
                        "live_stream_quote_capture_unavailable",
                        label=label,
                        cycle_id=cycle_id,
                        session_date=session_date,
                        reason=stream_quote_error,
                    )
                if stream_trade_error:
                    log_event(
                        logger,
                        logging.WARNING,
                        "live_stream_trade_capture_unavailable",
                        label=label,
                        cycle_id=cycle_id,
                        session_date=session_date,
                        reason=stream_trade_error,
                    )
                if stream_quote_records:
                    stream_quote_event_count = len(stream_quote_records)
                    quote_event_count += stream_quote_event_count
                if stream_trade_records:
                    stream_trade_event_count = len(stream_trade_records)
                    trade_event_count += stream_trade_event_count
        if quote_event_count == 0:
            try:
                recovery_quote_records = collect_latest_quote_records(
                    client=client,
                    candidates=capture_candidates,
                    feed=scanner_args.feed,
                    attempts=3,
                    retry_delay_seconds=2.0,
                    source="alpaca_latest_quote_recovery",
                )
                recovered_quote_event_count = history_store.save_option_quote_events(
                    cycle_id=cycle_id,
                    label=label,
                    profile=args.profile,
                    quotes=recovery_quote_records,
                )
                quote_event_count += recovered_quote_event_count
                recovery_quote_event_count = recovered_quote_event_count
            except Exception as exc:
                log_event(
                    logger,
                    logging.WARNING,
                    "live_quote_recovery_unavailable",
                    label=label,
                    cycle_id=cycle_id,
                    session_date=session_date,
                    error=str(exc),
                    exc_info=True,
                )
    quote_capture = build_quote_capture_summary(
        expected_quote_symbols=expected_quote_symbols,
        total_quote_events_saved=quote_event_count,
        baseline_quote_events_saved=baseline_quote_event_count,
        stream_quote_events_saved=stream_quote_event_count,
        recovery_quote_events_saved=recovery_quote_event_count,
    )
    trade_capture = build_trade_capture_summary(
        expected_trade_symbols=expected_trade_symbols,
        total_trade_events_saved=trade_event_count,
        stream_trade_events_saved=stream_trade_event_count,
    )
    reactive_quote_records = [
        *latest_quote_records,
        *stream_quote_records,
        *recovery_quote_records,
    ]
    enriched_trade_records = enrich_uoa_trade_records(
        trades=stream_trade_records,
        quotes=reactive_quote_records,
        contract_metadata_by_symbol=contract_metadata_by_symbol,
    )
    uoa_summary = build_uoa_trade_summary(
        as_of=generated_at,
        expected_trade_symbols=expected_trade_symbols,
        contract_metadata_by_symbol=contract_metadata_by_symbol,
        trades=enriched_trade_records,
        top_contracts_limit=max(len(expected_trade_symbols), 10),
        top_roots_limit=max(len(expected_uoa_roots), 10),
    )
    uoa_quote_summary = build_uoa_quote_summary(
        as_of=generated_at,
        expected_quote_symbols=expected_quote_symbols,
        contract_metadata_by_symbol=contract_metadata_by_symbol,
        quotes=reactive_quote_records,
    )
    uoa_baselines = build_uoa_trade_baselines(
        history_store=history_store,
        label=label,
        session_date=session_date,
        as_of=generated_at,
        underlyings=expected_uoa_roots,
    )
    stock_context_by_symbol = _build_stock_context_by_symbol(capture_candidates)
    uoa_decisions = build_uoa_root_decisions(
        uoa_summary=uoa_summary,
        baselines_by_symbol=uoa_baselines,
        quote_summary=uoa_quote_summary,
        capture_window_seconds=float(max(args.trade_capture_seconds, 1)),
        stock_context_by_symbol=stock_context_by_symbol,
    )
    if (
        expected_quote_symbols
        or expected_trade_symbols
        or quote_event_count
        or trade_event_count
        or stream_quote_error
        or stream_trade_error
    ):
        try:
            record_market_capture_summary_event(
                event_store=event_store,
                cycle_id=cycle_id,
                generated_at=generated_at,
                label=label,
                profile=args.profile,
                session_date=session_date,
                session_id=None if tick_context is None else tick_context.session_id,
                job_run_id=None if tick_context is None else tick_context.job_run_id,
                quote_capture=quote_capture,
                trade_capture=trade_capture,
                expected_quote_symbols=expected_quote_symbols,
                expected_trade_symbols=expected_trade_symbols,
                quote_records=reactive_quote_records,
                trade_records=stream_trade_records,
                stream_quote_error=stream_quote_error,
                stream_trade_error=stream_trade_error,
            )
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "market_capture_summary_event_unavailable",
                label=label,
                cycle_id=cycle_id,
                session_date=session_date,
                error=str(exc),
                exc_info=True,
            )
    if expected_trade_symbols or stream_trade_records:
        try:
            record_uoa_summary_event(
                event_store=event_store,
                cycle_id=cycle_id,
                generated_at=generated_at,
                label=label,
                profile=args.profile,
                session_date=session_date,
                session_id=None if tick_context is None else tick_context.session_id,
                job_run_id=None if tick_context is None else tick_context.job_run_id,
                summary=uoa_summary,
            )
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "uoa_summary_event_publish_unavailable",
                label=label,
                cycle_id=cycle_id,
                session_date=session_date,
                error=str(exc),
                exc_info=True,
            )
        try:
            record_uoa_decision_event(
                event_store=event_store,
                cycle_id=cycle_id,
                generated_at=generated_at,
                label=label,
                profile=args.profile,
                session_date=session_date,
                session_id=None if tick_context is None else tick_context.session_id,
                job_run_id=None if tick_context is None else tick_context.job_run_id,
                decisions=uoa_decisions,
            )
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "uoa_decision_event_publish_unavailable",
                label=label,
                cycle_id=cycle_id,
                session_date=session_date,
                error=str(exc),
                exc_info=True,
            )
    return LiveCaptureSnapshot(
        candidates=list(capture_candidates),
        contract_metadata_by_symbol=contract_metadata_by_symbol,
        expected_quote_symbols=expected_quote_symbols,
        expected_trade_symbols=expected_trade_symbols,
        expected_uoa_roots=expected_uoa_roots,
        quote_event_count=quote_event_count,
        baseline_quote_event_count=baseline_quote_event_count,
        stream_quote_event_count=stream_quote_event_count,
        recovery_quote_event_count=recovery_quote_event_count,
        trade_event_count=trade_event_count,
        stream_trade_event_count=stream_trade_event_count,
        latest_quote_records=latest_quote_records,
        stream_quote_records=stream_quote_records,
        recovery_quote_records=recovery_quote_records,
        stream_trade_records=stream_trade_records,
        reactive_quote_records=reactive_quote_records,
        quote_capture=quote_capture,
        trade_capture=trade_capture,
        uoa_summary=uoa_summary,
        uoa_quote_summary=uoa_quote_summary,
        uoa_decisions=uoa_decisions,
        stream_quote_error=stream_quote_error,
        stream_trade_error=stream_trade_error,
    )


__all__ = ["capture_live_option_market_state"]

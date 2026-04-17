#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time as time_module
from datetime import UTC, datetime, timedelta
from typing import Callable

from core.common import env_or_die, load_local_env
from core.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from core.integrations.calendar_events import build_calendar_event_resolver
from core.integrations.greeks import build_local_greeks_provider
from core.services.collections.config import (
    _apply_options_automation_overrides,
    build_scanner_args,
    collection_window_is_open,
    parse_args,
)
from core.services.collections.cycle import run_collection_cycle
from core.services.collections.models import LiveTickContext
from core.services.collections.shared import (
    build_skipped_collection_result,
    build_skipped_tick_result,
    resolve_collection_reference_time,
    session_date_for_generated_at,
)
from core.services.live_collector_health import (
    build_quote_capture_summary,
    build_trade_capture_summary,
)
from core.services.live_pipelines import build_live_snapshot_label
from core.services.live_recovery import (
    LIVE_SLOT_STATUS_MISSED,
    resolve_live_slot_stale_after_seconds,
)
from core.storage.factory import build_storage_context


def run_collection_tick(
    args: argparse.Namespace,
    *,
    tick_context: LiveTickContext,
    heartbeat: Callable[[], None] | None = None,
    emit_output: bool = True,
) -> dict[str, object]:
    args = _apply_options_automation_overrides(args)
    scanner_args = build_scanner_args(args)
    reference_time = resolve_collection_reference_time(tick_context.slot_at)
    if not args.allow_off_hours and not collection_window_is_open(
        now=reference_time,
        session_start_offset_minutes=int(
            getattr(args, "session_start_offset_minutes", 0)
        ),
        session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
    ):
        if emit_output:
            print("Scheduled slot is outside the collection window. Skipping.")
        return build_skipped_tick_result(
            tick_context=tick_context,
            status="skipped",
            reason="market_closed",
            message="Scheduled slot is outside the collection window.",
        )

    with build_storage_context(args.history_db) as storage:
        recovery_store = storage.recovery
        stale_after_seconds = resolve_live_slot_stale_after_seconds(
            int(getattr(args, "interval_seconds", 0))
        )
        now = datetime.now(UTC)
        slot_at = resolve_collection_reference_time(tick_context.slot_at)
        if now > slot_at + timedelta(seconds=stale_after_seconds):
            session_date = str(
                getattr(args, "session_date", "")
                or session_date_for_generated_at(tick_context.slot_at)
            )
            label = str(
                getattr(args, "label", "")
                or build_live_snapshot_label(
                    universe_label=args.universe,
                    strategy=args.strategy,
                    profile=args.profile,
                    greeks_source=args.greeks_source,
                )
            )
            message = "Scheduled live slot is stale and will be marked missed instead of replayed."
            if recovery_store.schema_ready():
                recovery_store.upsert_live_session_slot(
                    job_key=str(getattr(args, "job_key", "") or "live_collector"),
                    session_id=tick_context.session_id,
                    session_date=session_date,
                    label=label,
                    slot_at=tick_context.slot_at,
                    scheduled_for=tick_context.slot_at,
                    status=LIVE_SLOT_STATUS_MISSED,
                    job_run_id=tick_context.job_run_id,
                    recovery_note=message,
                    finished_at=now.isoformat().replace("+00:00", "Z"),
                    updated_at=now.isoformat().replace("+00:00", "Z"),
                )
            if emit_output:
                print(message)
            return build_skipped_tick_result(
                tick_context=tick_context,
                status="skipped",
                reason="stale_slot",
                message=message,
                slot_status=LIVE_SLOT_STATUS_MISSED,
            )

        key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
        secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
        client = AlpacaClient(
            key_id=key_id,
            secret_key=secret_key,
            trading_base_url=infer_trading_base_url(
                key_id, scanner_args.trading_base_url
            ),
            data_base_url=scanner_args.data_base_url,
        )
        calendar_resolver = build_calendar_event_resolver(
            key_id=key_id,
            secret_key=secret_key,
            data_base_url=scanner_args.data_base_url,
            database_url=args.history_db,
        )
        greeks_provider = build_local_greeks_provider()
        try:
            if heartbeat is not None:
                heartbeat()
            cycle_result = run_collection_cycle(
                args,
                tick_context=tick_context,
                scanner_args=scanner_args,
                client=client,
                history_store=storage.history,
                alert_store=storage.alerts,
                job_store=storage.jobs,
                collector_store=storage.collector,
                event_store=storage.events,
                signal_store=storage.signals,
                recovery_store=recovery_store,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                emit_output=emit_output,
                heartbeat=heartbeat,
            )
        finally:
            calendar_resolver.store.close()

    return {
        "status": "completed",
        "iterations_completed": 1,
        "cycle_ids": [cycle_result["cycle_id"]],
        "alerts_sent": cycle_result["alerts_sent"],
        "quote_events_saved": cycle_result["quote_events_saved"],
        "baseline_quote_events_saved": cycle_result["baseline_quote_events_saved"],
        "stream_quote_events_saved": cycle_result["stream_quote_events_saved"],
        "websocket_quote_events_saved": cycle_result["stream_quote_events_saved"],
        "recovery_quote_events_saved": cycle_result["recovery_quote_events_saved"],
        "expected_quote_symbols": list(cycle_result["expected_quote_symbols"]),
        "trade_events_saved": cycle_result["trade_events_saved"],
        "stream_trade_events_saved": cycle_result["stream_trade_events_saved"],
        "websocket_trade_events_saved": cycle_result["stream_trade_events_saved"],
        "expected_trade_symbols": list(cycle_result["expected_trade_symbols"]),
        "promotable_opportunity_count": cycle_result["promotable_opportunity_count"],
        "monitor_opportunity_count": cycle_result["monitor_opportunity_count"],
        "signal_states_upserted": cycle_result["signal_states_upserted"],
        "signal_transitions_recorded": cycle_result["signal_transitions_recorded"],
        "opportunities_upserted": cycle_result["opportunities_upserted"],
        "opportunities_expired": cycle_result["opportunities_expired"],
        "quote_capture": dict(cycle_result["quote_capture"]),
        "trade_capture": dict(cycle_result["trade_capture"]),
        "uoa_summary": dict(cycle_result["uoa_summary"]),
        "uoa_quote_summary": dict(cycle_result["uoa_quote_summary"]),
        "uoa_decisions": dict(cycle_result["uoa_decisions"]),
        "raw_candidate_summary": dict(cycle_result["raw_candidate_summary"]),
        "selection_summary": dict(cycle_result["selection_summary"]),
        "auto_execution": cycle_result["auto_execution"],
        "label": cycle_result["label"],
        "session_id": tick_context.session_id,
        "slot_at": tick_context.slot_at,
    }


def run_collection(
    args: argparse.Namespace,
    *,
    heartbeat: Callable[[], None] | None = None,
    emit_output: bool = True,
) -> dict[str, object]:
    scanner_args = build_scanner_args(args)

    if not args.allow_off_hours and not collection_window_is_open(
        now=resolve_collection_reference_time(None),
        session_start_offset_minutes=int(
            getattr(args, "session_start_offset_minutes", 0)
        ),
        session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
    ):
        if emit_output:
            print("Market is closed. Use --allow-off-hours to collect cycles anyway.")
        return build_skipped_collection_result(
            reason="market_closed",
            message="Market is closed. Use --allow-off-hours to collect cycles anyway.",
        )

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, scanner_args.trading_base_url),
        data_base_url=scanner_args.data_base_url,
    )
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=scanner_args.data_base_url,
        database_url=args.history_db,
    )
    greeks_provider = build_local_greeks_provider()
    cycle_ids: list[str] = []
    total_alerts = 0
    total_quote_events = 0
    total_baseline_quote_events = 0
    total_stream_quote_events = 0
    total_recovery_quote_events = 0
    total_trade_events = 0
    total_stream_trade_events = 0
    total_signal_states = 0
    total_signal_transitions = 0
    total_opportunities = 0
    total_opportunities_expired = 0
    last_label: str | None = None
    bootstrap_result = build_skipped_collection_result(
        reason="bootstrap",
        message="bootstrap",
    )
    last_uoa_summary: dict[str, object] = dict(bootstrap_result["uoa_summary"])
    last_uoa_quote_summary: dict[str, object] = dict(
        bootstrap_result["uoa_quote_summary"]
    )
    last_uoa_decisions: dict[str, object] = dict(bootstrap_result["uoa_decisions"])
    last_raw_candidate_summary: dict[str, object] = {
        "candidate_count": 0,
        "symbol_counts": {},
        "strategy_counts": {},
        "top_candidates": [],
    }
    last_selection_summary: dict[str, object] = dict(
        bootstrap_result["selection_summary"]
    )
    iterations_completed = 0
    try:
        with build_storage_context(args.history_db) as storage:
            for iteration in range(args.iterations):
                iteration_started_at = time_module.monotonic()
                if heartbeat is not None:
                    heartbeat()
                if not args.allow_off_hours and not collection_window_is_open(
                    now=datetime.now(UTC),
                    session_start_offset_minutes=int(
                        getattr(args, "session_start_offset_minutes", 0)
                    ),
                    session_end_offset_minutes=int(
                        getattr(args, "session_end_offset_minutes", 0)
                    ),
                ):
                    if emit_output:
                        print("Market closed during collection window. Stopping.")
                    break
                cycle_result = run_collection_cycle(
                    args,
                    tick_context=None,
                    scanner_args=scanner_args,
                    client=client,
                    history_store=storage.history,
                    alert_store=storage.alerts,
                    job_store=storage.jobs,
                    collector_store=storage.collector,
                    event_store=storage.events,
                    signal_store=storage.signals,
                    recovery_store=storage.recovery,
                    calendar_resolver=calendar_resolver,
                    greeks_provider=greeks_provider,
                    emit_output=emit_output,
                    heartbeat=heartbeat,
                )
                cycle_ids.append(cycle_result["cycle_id"])
                total_alerts += int(cycle_result["alerts_sent"])
                total_quote_events += int(cycle_result["quote_events_saved"])
                total_baseline_quote_events += int(
                    cycle_result["baseline_quote_events_saved"]
                )
                total_stream_quote_events += int(
                    cycle_result["stream_quote_events_saved"]
                )
                total_recovery_quote_events += int(
                    cycle_result["recovery_quote_events_saved"]
                )
                total_trade_events += int(cycle_result["trade_events_saved"])
                total_stream_trade_events += int(
                    cycle_result["stream_trade_events_saved"]
                )
                total_signal_states += int(cycle_result["signal_states_upserted"])
                total_signal_transitions += int(
                    cycle_result["signal_transitions_recorded"]
                )
                total_opportunities += int(cycle_result["opportunities_upserted"])
                total_opportunities_expired += int(
                    cycle_result["opportunities_expired"]
                )
                iterations_completed += 1
                last_label = str(cycle_result["label"])
                last_uoa_summary = dict(cycle_result["uoa_summary"])
                last_uoa_quote_summary = dict(cycle_result["uoa_quote_summary"])
                last_uoa_decisions = dict(cycle_result["uoa_decisions"])
                last_raw_candidate_summary = dict(cycle_result["raw_candidate_summary"])
                last_selection_summary = dict(cycle_result["selection_summary"])
                if iteration < args.iterations - 1:
                    elapsed_seconds = time_module.monotonic() - iteration_started_at
                    sleep_seconds = max(
                        float(max(args.interval_seconds, 1)) - elapsed_seconds, 0.0
                    )
                    if sleep_seconds > 0:
                        time_module.sleep(sleep_seconds)
                    if heartbeat is not None:
                        heartbeat()
    finally:
        calendar_resolver.store.close()

    return {
        "status": "completed",
        "iterations_completed": iterations_completed,
        "cycle_ids": cycle_ids,
        "alerts_sent": total_alerts,
        "quote_events_saved": total_quote_events,
        "baseline_quote_events_saved": total_baseline_quote_events,
        "stream_quote_events_saved": total_stream_quote_events,
        "websocket_quote_events_saved": total_stream_quote_events,
        "recovery_quote_events_saved": total_recovery_quote_events,
        "trade_events_saved": total_trade_events,
        "stream_trade_events_saved": total_stream_trade_events,
        "websocket_trade_events_saved": total_stream_trade_events,
        "signal_states_upserted": total_signal_states,
        "signal_transitions_recorded": total_signal_transitions,
        "opportunities_upserted": total_opportunities,
        "opportunities_expired": total_opportunities_expired,
        "quote_capture": build_quote_capture_summary(
            expected_quote_symbols=[],
            total_quote_events_saved=total_quote_events,
            baseline_quote_events_saved=total_baseline_quote_events,
            stream_quote_events_saved=total_stream_quote_events,
            recovery_quote_events_saved=total_recovery_quote_events,
        ),
        "trade_capture": build_trade_capture_summary(
            expected_trade_symbols=[],
            total_trade_events_saved=total_trade_events,
            stream_trade_events_saved=total_stream_trade_events,
        ),
        "uoa_summary": last_uoa_summary,
        "uoa_quote_summary": last_uoa_quote_summary,
        "uoa_decisions": last_uoa_decisions,
        "raw_candidate_summary": last_raw_candidate_summary,
        "selection_summary": last_selection_summary,
        "label": last_label,
    }


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    args = parse_args(argv)
    run_collection(args, emit_output=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, Callable

from core.alerts.dispatcher import dispatch_cycle_alerts
from core.domain.models import UniverseScanFailure
from core.integrations.alpaca.client import AlpacaClient
from core.services.automation_runtime import build_entry_runtime
from core.services.candidate_history_recovery import (
    recover_session_candidates_from_history,
)
from core.services.collections.capture.candidates import (
    build_capture_candidates,
    build_preselection_capture_candidates,
)
from core.services.collections.capture.runtime import capture_live_option_market_state
from core.services.collections.config import (
    _filter_scope_candidates,
    _filter_scope_rows,
    _merge_runtime_candidate_rows,
)
from core.services.collections.models import LiveTickContext
from core.services.collections.scanning import (
    build_raw_candidate_summary,
    build_symbol_strategy_candidates,
    run_universe_cycle,
)
from core.services.collections.shared import session_date_for_generated_at
from core.services.execution import submit_auto_session_execution
from core.services.live_collector_health import (
    CAPTURE_HISTORY_BLOCK_THRESHOLD,
    build_capture_history_gate,
    build_live_action_gate,
    build_selection_summary,
    enrich_live_collector_job_run_payload,
)
from core.services.live_pipelines import build_live_snapshot_label
from core.services.live_recovery import (
    load_session_slot_health,
    merge_live_action_gate_with_recovery,
    refresh_live_session_capture_targets,
)
from core.services.live_selection import read_previous_selection, select_live_opportunities
from core.services.opportunity_generation import sync_entry_runtime_opportunities
from core.services.signal_state import sync_live_collector_signal_layer
from core.services.strategy_builders import build_entry_runtime_candidates
from core.services.target_planner import refresh_options_automation_capture_targets
from core.storage.alert_repository import AlertRepository
from core.storage.collector_repository import CollectorRepository
from core.storage.event_repository import EventRepository
from core.storage.run_history_repository import RunHistoryRepository
from core.storage.signal_repository import SignalRepository

WATCHLIST_PER_STRATEGY = 3
WATCHLIST_TOP = 12
WATCHLIST_QUOTE_CAPTURE_TOP = 6


def build_cycle_id(label: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{label}"


def print_cycle_summary(
    *,
    generated_at: str,
    label: str,
    promotable_candidates: list[dict[str, Any]],
    monitor_candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    failures: list[UniverseScanFailure],
    quote_event_count: int,
    trade_event_count: int,
    uoa_summary: dict[str, Any],
    uoa_decisions: dict[str, Any],
    auto_execution: dict[str, Any] | None,
) -> None:
    print(f"[{generated_at}] {label}")
    print(f"Promotable opportunities: {len(promotable_candidates)}")
    print(f"Monitor opportunities: {len(monitor_candidates)}")
    print(f"Events: {len(events)}")
    print(f"Alerts: {len(alerts)}")
    print(f"Quote events saved: {quote_event_count}")
    print(f"Trade events saved: {trade_event_count}")
    uoa_overview = dict(uoa_summary.get("overview") or {})
    if int(uoa_overview.get("scoreable_trade_count") or 0) > 0:
        print(
            "UOA scoreable flow: "
            f"{int(uoa_overview.get('scoreable_trade_count') or 0)} trades across "
            f"{int(uoa_overview.get('scoreable_contract_count') or 0)} contracts "
            f"(${float(uoa_overview.get('scoreable_premium') or 0.0):,.0f} premium)"
        )
    uoa_decision_overview = dict(uoa_decisions.get("overview") or {})
    if int(uoa_decision_overview.get("root_count") or 0) > 0:
        print(
            "UOA decisions: "
            f"{int(uoa_decision_overview.get('monitor_count') or 0)} monitor / "
            f"{int(uoa_decision_overview.get('promotable_count') or 0)} promotable / "
            f"{int(uoa_decision_overview.get('high_count') or 0)} high"
        )
    if auto_execution is not None:
        print(f"Auto execution: {auto_execution.get('message')}")
    if failures:
        print(f"Failures: {len(failures)}")
    if promotable_candidates:
        print("Promotable:")
        for index, candidate in enumerate(promotable_candidates, start=1):
            print(
                f"- {index}. {candidate['underlying_symbol']} "
                f"[{candidate['strategy']}] "
                f"{candidate['short_strike']:.2f}/{candidate['long_strike']:.2f} "
                f"score {candidate['quality_score']:.1f} "
                f"mid {candidate['midpoint_credit']:.2f}"
            )
    if events:
        print("Events:")
        for event in events:
            print(f"- {event['message']}")
    if alerts:
        print("Alerts:")
        for alert in alerts:
            payload = alert.get("payload", {})
            print(
                f"- {payload.get('symbol')} {payload.get('alert_type')} "
                f"status={alert.get('status')} cycle={payload.get('cycle_id')}"
            )
    print()


def run_collection_cycle(
    args: argparse.Namespace,
    *,
    tick_context: LiveTickContext | None,
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
    alert_store: AlertRepository,
    job_store: Any,
    collector_store: CollectorRepository,
    event_store: EventRepository,
    signal_store: SignalRepository,
    recovery_store: Any | None,
    calendar_resolver: Any,
    greeks_provider: Any,
    emit_output: bool,
    heartbeat: Callable[[], None] | None = None,
) -> dict[str, Any]:
    generated_at = (
        datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    session_date = session_date_for_generated_at(generated_at)
    if heartbeat is not None:
        heartbeat()
    symbols, universe_label, scan_results, failures, _raw_top_candidates = (
        run_universe_cycle(
            scanner_args=scanner_args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
    )
    label = str(getattr(args, "label", "") or "").strip() or build_live_snapshot_label(
        universe_label=universe_label,
        strategy=args.strategy,
        profile=args.profile,
        greeks_source=args.greeks_source,
    )
    cycle_id = build_cycle_id(label)
    run_ids = {
        (result.symbol, result.args.strategy): result.run_id for result in scan_results
    }
    options_scope = getattr(args, "options_automation_scope", {"enabled": False})
    automation_mode = bool(options_scope.get("enabled"))
    entry_runtimes = [
        build_entry_runtime(bot, automation)
        for bot, automation in list(options_scope.get("entry_runtimes") or [])
    ]
    runtime_candidate_rows_by_owner: dict[
        tuple[str, str], dict[str, list[dict[str, Any]]]
    ] = {}
    symbol_strategy_candidates = build_symbol_strategy_candidates(
        scan_results,
        run_ids,
        max_per_strategy=WATCHLIST_PER_STRATEGY,
    )
    symbol_strategy_candidates = _filter_scope_candidates(
        symbol_strategy_candidates,
        scope=options_scope,
    )
    if bool(options_scope.get("enabled")) and entry_runtimes:
        try:
            runtime_candidate_rows_by_owner = build_entry_runtime_candidates(
                entry_runtimes=entry_runtimes,
                base_scanner_args=scanner_args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                per_runtime_limit=max(args.top, 1),
            )
            merged_runtime_candidates = _merge_runtime_candidate_rows(
                runtime_candidate_rows_by_owner
            )
            if merged_runtime_candidates:
                symbol_strategy_candidates = merged_runtime_candidates
        except Exception as exc:
            print(f"Exact runtime builder unavailable: {exc}")
    capture_snapshot = capture_live_option_market_state(
        args=args,
        scanner_args=scanner_args,
        client=client,
        history_store=history_store,
        event_store=event_store,
        recovery_store=recovery_store,
        label=label,
        cycle_id=cycle_id,
        generated_at=generated_at,
        session_date=session_date,
        tick_context=tick_context,
        capture_candidates=build_preselection_capture_candidates(
            symbol_strategy_candidates
        ),
    )
    signal_cycle_context = {
        "uoa_decisions": dict(capture_snapshot.uoa_decisions),
        "uoa_quote_summary": dict(capture_snapshot.uoa_quote_summary),
    }
    recovered_payloads: list[dict[str, Any]] = []
    previous_promotable: dict[str, dict[str, Any]] = {}
    previous_selection_memory: dict[str, dict[str, Any]] = {}
    promotable_payloads: list[dict[str, Any]] = []
    monitor_payloads: list[dict[str, Any]] = []
    opportunities: list[dict[str, Any]] = []
    selection_memory: dict[str, Any] = {}
    events: list[dict[str, Any]] = []
    if not automation_mode:
        previous_promotable, previous_selection_memory = read_previous_selection(
            collector_store, label
        )
        selection = select_live_opportunities(
            label=label,
            cycle_id=cycle_id,
            generated_at=generated_at,
            symbol_candidates=symbol_strategy_candidates,
            previous_promotable=previous_promotable,
            previous_selection_memory=previous_selection_memory,
            top_promotable=args.top,
            top_monitor=WATCHLIST_TOP,
            profile=args.profile,
            signal_cycle_context=signal_cycle_context,
        )
        symbol_strategy_candidates = dict(selection.get("symbol_candidates") or {})
        promotable_payloads = list(selection["promotable_candidates"])
        monitor_payloads = list(selection["monitor_candidates"])
        if args.profile == "0dte" and not promotable_payloads and not monitor_payloads:
            recovered_payloads = recover_session_candidates_from_history(
                history_store=history_store,
                session_date=session_date,
                session_label=label,
                generated_at=generated_at,
                top=WATCHLIST_TOP,
                max_per_strategy=WATCHLIST_PER_STRATEGY,
            )
            selection = select_live_opportunities(
                label=label,
                cycle_id=cycle_id,
                generated_at=generated_at,
                symbol_candidates=symbol_strategy_candidates,
                previous_promotable=previous_promotable,
                previous_selection_memory=previous_selection_memory,
                top_promotable=args.top,
                top_monitor=WATCHLIST_TOP,
                profile=args.profile,
                recovered_candidates=recovered_payloads,
                signal_cycle_context=signal_cycle_context,
            )
            symbol_strategy_candidates = _filter_scope_candidates(
                dict(selection.get("symbol_candidates") or {}),
                scope=options_scope,
            )
            promotable_payloads = list(selection["promotable_candidates"])
            monitor_payloads = list(selection["monitor_candidates"])
        opportunities = _filter_scope_rows(
            list(selection["opportunities"]),
            scope=options_scope,
        )
        promotable_payloads = _filter_scope_rows(
            promotable_payloads,
            scope=options_scope,
        )
        monitor_payloads = _filter_scope_rows(
            monitor_payloads,
            scope=options_scope,
        )
        selection_memory = dict(selection["selection_memory"])
        events = _filter_scope_rows(list(selection["events"]), scope=options_scope)
    raw_candidate_summary = build_raw_candidate_summary(symbol_strategy_candidates)
    persisted_opportunities = collector_store.save_cycle(
        cycle_id=cycle_id,
        label=label,
        generated_at=generated_at,
        job_run_id=None if tick_context is None else tick_context.job_run_id,
        session_id=None if tick_context is None else tick_context.session_id,
        universe_label=universe_label,
        strategy=args.strategy,
        profile=args.profile,
        greeks_source=args.greeks_source,
        symbols=symbols,
        failures=[asdict(failure) for failure in failures],
        selection_memory=selection_memory,
        opportunities=[] if automation_mode else opportunities,
        events=[] if automation_mode else events,
    )
    signal_sync = {
        "signal_states_upserted": 0,
        "signal_transitions_recorded": 0,
        "opportunities_upserted": 0,
        "opportunities_expired": 0,
    }
    if not automation_mode:
        try:
            signal_sync = sync_live_collector_signal_layer(
                signal_store=signal_store,
                label=label,
                session_date=session_date,
                generated_at=generated_at,
                cycle_id=cycle_id,
                strategy=args.strategy,
                profile=args.profile,
                symbols=symbols,
                symbol_candidates=symbol_strategy_candidates,
                selection_memory=selection_memory,
                failures=[asdict(failure) for failure in failures],
                persisted_opportunities=persisted_opportunities,
            )
        except Exception as exc:
            print(f"Signal-state sync unavailable: {exc}")
    automation_sync = {
        "automation_runs_upserted": 0,
        "runtime_opportunities_upserted": 0,
        "runtime_opportunities_expired": 0,
        "opportunities": [],
    }
    if bool(options_scope.get("enabled")):
        try:
            automation_sync = sync_entry_runtime_opportunities(
                signal_store=signal_store,
                label=label,
                session_date=session_date,
                generated_at=generated_at,
                cycle_id=cycle_id,
                entry_runtimes=entry_runtimes,
                symbol_candidates=symbol_strategy_candidates,
                runtime_candidate_rows_by_owner=runtime_candidate_rows_by_owner,
                persisted_opportunities=persisted_opportunities,
                job_run_id=None if tick_context is None else tick_context.job_run_id,
                top_promotable=args.top,
                top_monitor=WATCHLIST_TOP,
            )
        except Exception as exc:
            print(f"Options automation runtime sync unavailable: {exc}")
        opportunities = [
            dict(row) for row in list(automation_sync.get("opportunities") or [])
        ]
        promotable_payloads = [
            dict(row)
            for row in opportunities
            if str(row.get("selection_state") or "") == "promotable"
        ]
        monitor_payloads = [
            dict(row)
            for row in opportunities
            if str(row.get("selection_state") or "") == "monitor"
        ]
    selection_summary = build_selection_summary(opportunities)
    if heartbeat is not None:
        heartbeat()
    quote_candidates = build_capture_candidates(
        promotable_candidates=promotable_payloads,
        monitor_candidates=monitor_payloads,
        opportunities=opportunities,
        monitor_limit=WATCHLIST_QUOTE_CAPTURE_TOP,
    )
    capture_targets: dict[str, list[dict[str, Any]]] = {
        "promotable": [],
        "monitor": [],
    }
    if tick_context is not None and recovery_store is not None:
        try:
            if bool(options_scope.get("enabled")):
                runtime_capture_opportunities = list(
                    automation_sync.get("opportunities") or []
                )
                target_refresh = refresh_options_automation_capture_targets(
                    recovery_store=recovery_store,
                    session_id=tick_context.session_id,
                    session_date=session_date,
                    entry_runtimes=list(options_scope.get("entry_runtimes") or []),
                    opportunities=runtime_capture_opportunities or opportunities,
                    label=label,
                    data_base_url=getattr(scanner_args, "data_base_url", None),
                )
                capture_targets = {
                    str(reason): [dict(row) for row in rows if isinstance(row, dict)]
                    for reason, rows in dict(
                        target_refresh.get("capture_targets") or {}
                    ).items()
                }
            else:
                target_refresh = refresh_live_session_capture_targets(
                    recovery_store=recovery_store,
                    session_id=tick_context.session_id,
                    session_date=session_date,
                    label=label,
                    profile=args.profile,
                    promotable_candidates=promotable_payloads,
                    monitor_candidates=monitor_payloads,
                    capture_candidates=quote_candidates,
                    data_base_url=getattr(scanner_args, "data_base_url", None),
                    session_end_offset_minutes=int(
                        getattr(args, "session_end_offset_minutes", 0)
                    ),
                )
                capture_targets = {
                    str(reason): [dict(row) for row in rows if isinstance(row, dict)]
                    for reason, rows in dict(
                        target_refresh.get("capture_targets") or {}
                    ).items()
                }
        except Exception as exc:
            print(f"Capture target refresh unavailable: {exc}")
    quote_event_count = capture_snapshot.quote_event_count
    baseline_quote_event_count = capture_snapshot.baseline_quote_event_count
    stream_quote_event_count = capture_snapshot.stream_quote_event_count
    recovery_quote_event_count = capture_snapshot.recovery_quote_event_count
    trade_event_count = capture_snapshot.trade_event_count
    stream_trade_event_count = capture_snapshot.stream_trade_event_count
    expected_quote_symbols = list(capture_snapshot.expected_quote_symbols)
    expected_trade_symbols = list(capture_snapshot.expected_trade_symbols)
    stream_quote_error = capture_snapshot.stream_quote_error
    stream_trade_error = capture_snapshot.stream_trade_error
    quote_capture = dict(capture_snapshot.quote_capture)
    trade_capture = dict(capture_snapshot.trade_capture)
    reactive_quote_records = list(capture_snapshot.reactive_quote_records)
    live_action_gate = build_live_action_gate(
        profile=args.profile,
        quote_capture=quote_capture,
    )
    slot_recovery = None
    if tick_context is not None and recovery_store is not None:
        try:
            slot_recovery = load_session_slot_health(
                recovery_store=recovery_store,
                session_id=tick_context.session_id,
            )
            merged_gate = merge_live_action_gate_with_recovery(
                base_gate=live_action_gate,
                slot_health=slot_recovery,
            )
            if merged_gate is not None:
                live_action_gate = merged_gate
        except Exception as exc:
            print(f"Live recovery gate unavailable: {exc}")
    if tick_context is not None:
        try:
            recent_runs = [
                enrich_live_collector_job_run_payload(row)
                for row in job_store.list_job_runs(
                    job_type="live_collector",
                    session_id=tick_context.session_id,
                    limit=CAPTURE_HISTORY_BLOCK_THRESHOLD + 3,
                )
            ]
            recent_capture_statuses = [quote_capture["capture_status"]]
            recent_capture_statuses.extend(
                str(row.get("capture_status") or "").strip().lower()
                for row in recent_runs
                if str(row.get("job_run_id") or "") != tick_context.job_run_id
                and str(row.get("status") or "") == "succeeded"
                and str(row.get("capture_status") or "").strip()
            )
            history_gate = build_capture_history_gate(recent_capture_statuses)
            if (
                history_gate is not None
                and str(live_action_gate.get("status") or "") != "blocked"
            ):
                live_action_gate = history_gate
        except Exception as exc:
            print(f"Live capture history gate unavailable: {exc}")
    if bool(options_scope.get("enabled")):
        live_action_gate = {
            **dict(live_action_gate),
            "status": "bot_runtime_owned",
            "reason_code": "bot_runtime_owned",
            "message": "Collector discovery is active, but execution and alerts are owned by the options automation runtime.",
            "allow_auto_execution": False,
            "allow_alerts": False,
        }
    uoa_summary = dict(capture_snapshot.uoa_summary)
    uoa_quote_summary = dict(capture_snapshot.uoa_quote_summary)
    uoa_decisions = dict(capture_snapshot.uoa_decisions)
    if heartbeat is not None:
        heartbeat()
    auto_execution: dict[str, Any] | None = None
    if tick_context is not None and bool(live_action_gate.get("allow_auto_execution")):
        try:
            auto_execution = submit_auto_session_execution(
                db_target=args.history_db,
                session_id=tick_context.session_id,
                cycle_id=cycle_id,
                policy=getattr(args, "execution_policy", None),
                job_run_id=tick_context.job_run_id,
                reactive_quote_records=reactive_quote_records,
            )
        except Exception as exc:
            auto_execution = {
                "action": "auto_submit",
                "changed": False,
                "reason": "execution_error",
                "message": f"Automatic execution failed: {exc}",
                "error": str(exc),
            }
            print(f"Automatic execution unavailable: {exc}")
        if heartbeat is not None:
            heartbeat()
    elif tick_context is not None:
        auto_execution = {
            "action": "auto_submit",
            "changed": False,
            "reason": live_action_gate.get("reason_code"),
            "message": live_action_gate.get("message"),
            "gate": dict(live_action_gate),
        }
    alerts: list[dict[str, Any]] = []
    if bool(live_action_gate.get("allow_alerts")):
        try:
            alerts = dispatch_cycle_alerts(
                collector_store=collector_store,
                alert_store=alert_store,
                job_store=job_store,
                cycle_id=cycle_id,
                label=label,
                generated_at=generated_at,
                strategy_mode=args.strategy,
                profile=args.profile,
                promotable_candidates=promotable_payloads,
                events=events,
                uoa_decisions=uoa_decisions,
                session_id=None if tick_context is None else tick_context.session_id,
                planner_job_run_id=None
                if tick_context is None
                else tick_context.job_run_id,
            )
        except Exception as exc:
            print(f"Alert dispatch unavailable: {exc}")
    if emit_output:
        print_cycle_summary(
            generated_at=generated_at,
            label=label,
            promotable_candidates=promotable_payloads,
            monitor_candidates=monitor_payloads,
            events=events,
            alerts=alerts,
            failures=failures,
            quote_event_count=quote_event_count,
            trade_event_count=trade_event_count,
            uoa_summary=uoa_summary,
            uoa_decisions=uoa_decisions,
            auto_execution=auto_execution,
        )
    return {
        "cycle_id": cycle_id,
        "generated_at": generated_at,
        "label": label,
        "profile": args.profile,
        "alerts_sent": len(alerts),
        "quote_events_saved": quote_event_count,
        "baseline_quote_events_saved": baseline_quote_event_count,
        "stream_quote_events_saved": stream_quote_event_count,
        "websocket_quote_events_saved": stream_quote_event_count,
        "recovery_quote_events_saved": recovery_quote_event_count,
        "expected_quote_symbols": expected_quote_symbols,
        "stream_quote_error": stream_quote_error,
        "trade_events_saved": trade_event_count,
        "stream_trade_events_saved": stream_trade_event_count,
        "websocket_trade_events_saved": stream_trade_event_count,
        "expected_trade_symbols": expected_trade_symbols,
        "stream_trade_error": stream_trade_error,
        "promotable_opportunity_count": len(promotable_payloads),
        "monitor_opportunity_count": len(monitor_payloads),
        "signal_states_upserted": int(signal_sync["signal_states_upserted"]),
        "signal_transitions_recorded": int(signal_sync["signal_transitions_recorded"]),
        "opportunities_upserted": int(signal_sync["opportunities_upserted"]),
        "opportunities_expired": int(signal_sync["opportunities_expired"]),
        "automation_runs_upserted": int(automation_sync["automation_runs_upserted"]),
        "runtime_opportunities_upserted": int(
            automation_sync["runtime_opportunities_upserted"]
        ),
        "runtime_opportunities_expired": int(
            automation_sync["runtime_opportunities_expired"]
        ),
        "quote_capture": quote_capture,
        "trade_capture": trade_capture,
        "live_action_gate": live_action_gate,
        "slot_recovery": slot_recovery,
        "capture_targets": capture_targets,
        "uoa_summary": uoa_summary,
        "uoa_quote_summary": uoa_quote_summary,
        "uoa_decisions": uoa_decisions,
        "raw_candidate_summary": raw_candidate_summary,
        "selection_summary": selection_summary,
        "auto_execution": auto_execution,
    }

__all__ = ["run_collection_cycle"]

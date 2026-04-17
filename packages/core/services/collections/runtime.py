#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time as time_module
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from core.common import env_or_die, load_local_env
from core.alerts.dispatcher import dispatch_cycle_alerts
from core.domain.models import SpreadCandidate, SymbolScanResult, UniverseScanFailure
from core.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from core.integrations.calendar_events import build_calendar_event_resolver
from core.integrations.greeks import build_local_greeks_provider
from core.services.execution import submit_auto_session_execution
from core.services.live_selection import read_previous_selection, select_live_opportunities
from core.services.live_collector_health import (
    CAPTURE_HISTORY_BLOCK_THRESHOLD,
    build_capture_history_gate,
    build_live_action_gate,
    build_quote_capture_summary,
    build_selection_summary,
    build_trade_capture_summary,
    enrich_live_collector_job_run_payload,
)
from core.services.live_recovery import (
    LIVE_SLOT_STATUS_MISSED,
    load_session_slot_health,
    merge_live_action_gate_with_recovery,
    refresh_live_session_capture_targets,
    resolve_live_slot_stale_after_seconds,
)
from core.services.live_pipelines import build_live_snapshot_label
from core.services.uoa_quote_summary import build_uoa_quote_summary
from core.services.uoa_root_decisions import build_uoa_root_decisions
from core.services.candidate_history_recovery import (
    recover_session_candidates_from_history,
)
from core.services.collections.capture import (
    build_capture_candidates,
    build_preselection_capture_candidates,
    capture_live_option_market_state,
)
from core.services.collections.config import (
    _apply_options_automation_overrides,
    _filter_scope_candidates,
    _filter_scope_rows,
    _merge_runtime_candidate_rows,
    build_scanner_args,
    collection_window_is_open,
    parse_args,
)
from core.services.collections.models import LiveTickContext
from core.services.automation_runtime import build_entry_runtime
from core.services.opportunity_generation import sync_entry_runtime_opportunities
from core.services.strategy_builders import build_entry_runtime_candidates
from core.services.signal_state import sync_live_collector_signal_layer
from core.services.target_planner import refresh_options_automation_capture_targets
from core.services.uoa_trade_summary import build_uoa_trade_summary
from core.services.market_dates import NEW_YORK
from core.services.scanners.config import resolve_symbols
from core.services.scanners.service import (
    merge_strategy_candidates,
    scan_symbol_across_strategies,
    sort_candidates_for_display,
)
from core.storage.alert_repository import AlertRepository
from core.storage.factory import (
    build_storage_context,
)
from core.storage.collector_repository import CollectorRepository
from core.storage.event_repository import EventRepository
from core.storage.run_history_repository import RunHistoryRepository
from core.storage.signal_repository import SignalRepository

WATCHLIST_PER_STRATEGY = 3
WATCHLIST_TOP = 12
WATCHLIST_QUOTE_CAPTURE_TOP = 6


def run_universe_cycle(
    *,
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    history_store: RunHistoryRepository,
) -> tuple[
    list[str],
    str,
    list[SymbolScanResult],
    list[UniverseScanFailure],
    list[SpreadCandidate],
]:
    symbols, universe_label = resolve_symbols(scanner_args)
    scanner_args.session_label = build_live_snapshot_label(
        universe_label=universe_label,
        strategy=scanner_args.strategy,
        profile=scanner_args.profile,
        greeks_source=scanner_args.greeks_source,
    )
    scan_results: list[SymbolScanResult] = []
    failures: list[UniverseScanFailure] = []
    selected_candidates: list[SpreadCandidate] = []

    for symbol in symbols:
        strategy_results, symbol_failures = scan_symbol_across_strategies(
            symbol=symbol,
            base_args=scanner_args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
        failures.extend(symbol_failures)
        if not strategy_results:
            continue
        scan_results.extend(strategy_results)
        symbol_selected_candidates = merge_strategy_candidates(
            strategy_results,
            per_strategy_top=scanner_args.per_symbol_top,
        )[: scanner_args.per_symbol_top]
        selected_candidates.extend(symbol_selected_candidates)

    selected_candidates = sort_candidates_for_display(selected_candidates)
    selected_candidates = selected_candidates[: scanner_args.top]
    return symbols, universe_label, scan_results, failures, selected_candidates


def build_cycle_id(label: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{label}"


def serialize_candidate(
    candidate: SpreadCandidate, run_id: str | None
) -> dict[str, Any]:
    payload = asdict(candidate)
    payload["run_id"] = run_id
    return payload


def build_symbol_strategy_candidates(
    scan_results: list[SymbolScanResult],
    run_ids: dict[tuple[str, str], str],
    *,
    max_per_strategy: int = 1,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for result in scan_results:
        if not result.candidates:
            continue
        for candidate in result.candidates[: max(max_per_strategy, 1)]:
            payload = serialize_candidate(
                candidate,
                run_ids.get((result.symbol, result.args.strategy)),
            )
            grouped.setdefault(result.symbol, []).append(payload)
    for symbol in grouped:
        grouped[symbol].sort(
            key=lambda candidate: candidate["quality_score"], reverse=True
        )
    return grouped


def build_raw_candidate_summary(
    symbol_strategy_candidates: dict[str, list[dict[str, Any]]],
    *,
    limit: int = 10,
) -> dict[str, Any]:
    total = 0
    strategy_counts: dict[str, int] = {}
    symbol_counts: dict[str, int] = {}
    ranked_rows: list[dict[str, Any]] = []
    for symbol, candidates in sorted(symbol_strategy_candidates.items()):
        symbol_counts[str(symbol)] = len(candidates)
        total += len(candidates)
        for candidate in candidates:
            strategy = str(candidate.get("strategy") or "unknown")
            strategy_counts[strategy] = int(strategy_counts.get(strategy) or 0) + 1
            ranked_rows.append(
                {
                    "underlying_symbol": str(
                        candidate.get("underlying_symbol") or symbol
                    ),
                    "strategy": strategy,
                    "expiration_date": candidate.get("expiration_date"),
                    "short_symbol": candidate.get("short_symbol"),
                    "long_symbol": candidate.get("long_symbol"),
                    "quality_score": float(candidate.get("quality_score") or 0.0),
                    "midpoint_credit": float(candidate.get("midpoint_credit") or 0.0),
                    "return_on_risk": float(candidate.get("return_on_risk") or 0.0),
                    "setup_status": candidate.get("setup_status"),
                }
            )
    ranked_rows.sort(
        key=lambda row: (
            float(row.get("quality_score") or 0.0),
            float(row.get("return_on_risk") or 0.0),
            float(row.get("midpoint_credit") or 0.0),
        ),
        reverse=True,
    )
    return {
        "candidate_count": total,
        "symbol_counts": dict(sorted(symbol_counts.items())),
        "strategy_counts": dict(sorted(strategy_counts.items())),
        "top_candidates": ranked_rows[: max(int(limit), 1)],
    }


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


def _resolve_collection_reference_time(slot_at: str | datetime | None) -> datetime:
    if isinstance(slot_at, datetime):
        return slot_at
    if isinstance(slot_at, str) and slot_at:
        normalized = (
            slot_at.replace("Z", "+00:00") if slot_at.endswith("Z") else slot_at
        )
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now(UTC)


def _session_date_for_generated_at(generated_at: str) -> str:
    normalized = (
        generated_at.replace("Z", "+00:00")
        if generated_at.endswith("Z")
        else generated_at
    )
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(NEW_YORK).date().isoformat()


def _build_skipped_tick_result(
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


def _run_collection_cycle(
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
    session_date = _session_date_for_generated_at(generated_at)
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


def run_collection_tick(
    args: argparse.Namespace,
    *,
    tick_context: LiveTickContext,
    heartbeat: Callable[[], None] | None = None,
    emit_output: bool = True,
) -> dict[str, Any]:
    args = _apply_options_automation_overrides(args)
    scanner_args = build_scanner_args(args)
    reference_time = _resolve_collection_reference_time(tick_context.slot_at)
    if not args.allow_off_hours and not collection_window_is_open(
        now=reference_time,
        session_start_offset_minutes=int(
            getattr(args, "session_start_offset_minutes", 0)
        ),
        session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
    ):
        if emit_output:
            print("Scheduled slot is outside the collection window. Skipping.")
        return _build_skipped_tick_result(
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
        slot_at = _resolve_collection_reference_time(tick_context.slot_at)
        if now > slot_at + timedelta(seconds=stale_after_seconds):
            session_date = str(
                getattr(args, "session_date", "")
                or _session_date_for_generated_at(tick_context.slot_at)
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
            return _build_skipped_tick_result(
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
            cycle_result = _run_collection_cycle(
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
) -> dict[str, Any]:
    scanner_args = build_scanner_args(args)

    if not args.allow_off_hours and not collection_window_is_open(
        now=_resolve_collection_reference_time(None),
        session_start_offset_minutes=int(
            getattr(args, "session_start_offset_minutes", 0)
        ),
        session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
    ):
        if emit_output:
            print("Market is closed. Use --allow-off-hours to collect cycles anyway.")
        return {
            "status": "skipped",
            "reason": "market_closed",
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
                expected_trade_symbols=[], trades=[]
            ),
            "uoa_quote_summary": build_uoa_quote_summary(
                as_of=datetime.now(UTC).isoformat(),
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
    last_uoa_summary = build_uoa_trade_summary(expected_trade_symbols=[], trades=[])
    last_uoa_quote_summary = build_uoa_quote_summary(
        as_of=datetime.now(UTC).isoformat(),
        expected_quote_symbols=[],
        quotes=[],
    )
    last_uoa_decisions = build_uoa_root_decisions(
        uoa_summary={},
        baselines_by_symbol={},
        quote_summary={},
        capture_window_seconds=0,
    )
    last_raw_candidate_summary = {
        "candidate_count": 0,
        "symbol_counts": {},
        "strategy_counts": {},
        "top_candidates": [],
    }
    last_selection_summary = build_selection_summary([])
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
                cycle_result = _run_collection_cycle(
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

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, Callable

from core.alerts.dispatcher import dispatch_cycle_alerts
from core.domain.models import UniverseScanFailure
from core.integrations.alpaca.client import AlpacaClient
from core.services.trading_strategy_runtime import build_entry_runtime
from core.services.candidate_history_recovery import (
    recover_session_candidates_from_history,
)
from core.services.discovery_runs.capture.candidates import (
    build_capture_candidates,
    build_preselection_capture_candidates,
)
from core.services.discovery_runs.capture.runtime import capture_live_option_market_state
from core.services.discovery_runs.capture.uoa_watchlist import (
    build_uoa_capture_candidates_from_symbols,
)
from core.services.discovery_runs.config import (
    _filter_scope_candidates,
    _filter_scope_rows,
    _merge_runtime_candidate_rows,
)
from core.services.discovery_runs.models import LiveTickContext
from core.services.discovery_runs.scanning import (
    build_raw_candidate_summary,
    build_symbol_strategy_candidates,
    run_universe_cycle,
)
from core.services.scanners.config import resolve_symbols
from core.services.discovery_runs.shared import session_date_for_generated_at
from core.services.execution import submit_auto_session_execution
from core.services.discovery_run_health.enrichment import (
    enrich_discovery_run_job_run_payload,
)
from core.services.discovery_run_health.selection import build_selection_summary
from core.services.discovery_run_health.tradeability import (
    CAPTURE_HISTORY_BLOCK_THRESHOLD,
    build_capture_history_gate,
    build_live_action_gate,
)
from core.services.live_pipelines import build_live_snapshot_label
from core.services.discovery_recovery import (
    load_session_slot_health,
    merge_live_action_gate_with_recovery,
    refresh_live_session_capture_targets,
)
from core.services.live_selection import read_previous_selection, select_live_opportunities
from core.services.option_structures import payload_display_fields
from core.services.opportunity_generation import sync_entry_runtime_opportunities
from core.services.signal_state import sync_discovery_run_signal_layer
from core.services.ticker_sources import resolve_ticker_source_symbols
from core.services.strategy_builders import build_entry_runtime_candidates
from core.services.target_planner import refresh_trading_strategy_capture_targets
from core.storage.alert_repository import AlertRepository
from core.storage.discovery_run_repository import DiscoveryRunRepository
from core.storage.event_repository import EventRepository
from core.storage.run_history_repository import RunHistoryRepository
from core.storage.signal_repository import SignalRepository

WATCHLIST_PER_STRATEGY = 3
WATCHLIST_TOP = 12
WATCHLIST_QUOTE_CAPTURE_TOP = 6


def _direct_uoa_symbols(args: argparse.Namespace) -> list[str]:
    return [token.strip().upper() for token in str(getattr(args, "symbols", "") or "").split(",") if token.strip()]


def _configured_uoa_symbol_source(
    args: argparse.Namespace,
    *,
    job_store: Any,
) -> dict[str, Any]:
    direct_symbols = _direct_uoa_symbols(args)
    if direct_symbols:
        return {
            "kind": "symbols_override",
            "status": "ready",
            "symbols": direct_symbols,
            "summary": {
                "symbol_count": len(direct_symbols),
            },
            "degradation": {
                "status": "ok",
                "reason": None,
            },
        }
    ticker_source_ref = str(getattr(args, "ticker_source_ref", "") or "").strip()
    ticker_source_job_key = str(getattr(args, "ticker_source_job_key", "") or "").strip()
    if ticker_source_ref and ticker_source_job_key:
        return resolve_ticker_source_symbols(
            job_store,
            source_id=ticker_source_ref,
            job_key=ticker_source_job_key,
            max_age_seconds=getattr(args, "max_source_age_seconds", None),
            fallback_universe_ref=(str(getattr(args, "fallback_universe_ref", "") or "").strip() or None),
        )
    return {
        "kind": "ticker_source",
        "status": "missing",
        "symbols": [],
        "summary": {},
        "degradation": {
            "status": "missing",
            "reason": "no_symbol_source",
        },
    }


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
            display_fields = payload_display_fields(candidate)
            print(
                f"- {index}. {candidate['underlying_symbol']} "
                f"[{candidate['strategy']}] "
                f"{candidate.get('strike_path') or display_fields.get('strike_path') or 'n/a'} "
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
            print(f"- {payload.get('symbol')} {payload.get('alert_type')} " f"status={alert.get('status')} cycle={payload.get('cycle_id')}")
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
    discovery_store: DiscoveryRunRepository,
    event_store: EventRepository,
    signal_store: SignalRepository,
    recovery_store: Any | None,
    calendar_resolver: Any,
    greeks_provider: Any,
    emit_output: bool,
    heartbeat: Callable[[], None] | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    session_date = session_date_for_generated_at(generated_at)
    uoa_only = bool(getattr(args, "uoa_only", False))
    strategy_scope = getattr(args, "trading_strategy_scope", {"enabled": False})
    strategy_runtime_mode = bool(strategy_scope.get("enabled"))
    entry_runtimes = [build_entry_runtime(strategy) for strategy in list(strategy_scope.get("entry_strategies") or [])]
    resolved_symbols, resolved_universe_label = resolve_symbols(scanner_args)
    scoped_symbols = sorted({str(symbol).upper() for runtime in entry_runtimes for symbol in list(runtime.symbols) if str(symbol).strip()})
    symbols = scoped_symbols or resolved_symbols
    universe_label = resolved_universe_label
    label = str(getattr(args, "label", "") or "").strip() or build_live_snapshot_label(
        universe_label=universe_label,
        strategy=args.strategy,
        profile=args.profile,
        greeks_source=args.greeks_source,
    )
    scanner_args.session_label = label
    cycle_id = build_cycle_id(label)
    scan_results: list[Any] = []
    failures: list[UniverseScanFailure] = []
    symbol_strategy_candidates: dict[str, list[dict[str, Any]]] = {}
    runtime_candidate_rows_by_owner: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
    uoa_capture_candidates: list[dict[str, Any]] = []
    uoa_watchlist_summary: dict[str, Any] = {}
    symbol_source = {
        "kind": "resolved_scope",
        "status": "ready",
        "symbols": list(symbols),
        "summary": {
            "symbol_count": len(symbols),
        },
        "degradation": {
            "status": "ok",
            "reason": None,
        },
    }
    if heartbeat is not None:
        heartbeat()
    if uoa_only:
        symbol_source = _configured_uoa_symbol_source(args, job_store=job_store)
        symbols = [str(symbol).upper() for symbol in list(symbol_source.get("symbols") or [])]
        args.symbols = ",".join(symbols) if symbols else None
        args.universe = None
        scanner_args.symbols = args.symbols
        scanner_args.universe = None
        scanner_args.symbols_file = None
        if str(symbol_source.get("kind") or "") == "fallback_universe":
            universe_label = str(symbol_source.get("fallback_universe_ref") or "fallback_universe")
        elif str(symbol_source.get("source_id") or "").strip():
            universe_label = f"ticker_source:{symbol_source['source_id']}"
        else:
            universe_label = "uoa_only"
        if symbols:
            (
                uoa_capture_candidates,
                uoa_failures,
                uoa_watchlist_summary,
            ) = build_uoa_capture_candidates_from_symbols(
                symbols=symbols,
                scanner_args=scanner_args,
                client=client,
                greeks_provider=greeks_provider,
                feed_entries_by_symbol={
                    str(item.get("symbol") or "").strip().upper(): dict(item)
                    for item in list(symbol_source.get("entries") or [])
                    if str(item.get("symbol") or "").strip()
                },
            )
            failures.extend(uoa_failures)
    elif bool(strategy_scope.get("enabled")) and entry_runtimes:
        try:
            runtime_candidate_rows_by_owner = build_entry_runtime_candidates(
                entry_runtimes=entry_runtimes,
                base_scanner_args=scanner_args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                per_runtime_limit=max(args.top, 1),
                history_store=history_store,
                session_label=label,
            )
            merged_runtime_candidates = _merge_runtime_candidate_rows(runtime_candidate_rows_by_owner)
            symbol_strategy_candidates = merged_runtime_candidates
        except Exception as exc:
            print(f"Exact runtime builder unavailable: {exc}")
            (
                symbols,
                universe_label,
                scan_results,
                failures,
                _raw_top_candidates,
            ) = run_universe_cycle(
                scanner_args=scanner_args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                history_store=history_store,
            )
            run_ids = {(result.symbol, result.args.strategy): result.run_id for result in scan_results}
            symbol_strategy_candidates = build_symbol_strategy_candidates(
                scan_results,
                run_ids,
                max_per_strategy=WATCHLIST_PER_STRATEGY,
            )
            symbol_strategy_candidates = _filter_scope_candidates(
                symbol_strategy_candidates,
                scope=strategy_scope,
            )
    else:
        (
            symbols,
            universe_label,
            scan_results,
            failures,
            _raw_top_candidates,
        ) = run_universe_cycle(
            scanner_args=scanner_args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
        run_ids = {(result.symbol, result.args.strategy): result.run_id for result in scan_results}
        symbol_strategy_candidates = build_symbol_strategy_candidates(
            scan_results,
            run_ids,
            max_per_strategy=WATCHLIST_PER_STRATEGY,
        )
        symbol_strategy_candidates = _filter_scope_candidates(
            symbol_strategy_candidates,
            scope=strategy_scope,
        )
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
        capture_candidates=(list(uoa_capture_candidates) if uoa_only else build_preselection_capture_candidates(symbol_strategy_candidates)),
    )
    signal_cycle_context = {
        "uoa_decisions": dict(capture_snapshot.uoa_decisions),
        "uoa_quote_summary": dict(capture_snapshot.uoa_quote_summary),
    }
    recovered_payloads: list[dict[str, Any]] = []
    previous_promotable: dict[str, dict[str, Any]] = {}
    previous_selection_memory: dict[str, dict[str, Any]] = {}
    discovery_run_promotable_payloads: list[dict[str, Any]] = []
    discovery_run_monitor_payloads: list[dict[str, Any]] = []
    discovery_run_opportunities: list[dict[str, Any]] = []
    runtime_opportunities: list[dict[str, Any]] = []
    runtime_promotable_payloads: list[dict[str, Any]] = []
    runtime_monitor_payloads: list[dict[str, Any]] = []
    selection_memory: dict[str, Any] = {}
    events: list[dict[str, Any]] = []
    selection_summary = build_selection_summary([])
    strategy_sync_summary = {
        "strategy_runs_upserted": 0,
        "runtime_opportunities_upserted": 0,
        "runtime_opportunities_expired": 0,
        "runtime_selection_summary": build_selection_summary([]),
    }
    signal_sync = {
        "signal_states_upserted": 0,
        "signal_transitions_recorded": 0,
        "opportunities_upserted": 0,
        "opportunities_expired": 0,
    }
    strategy_sync = {
        "strategy_runs_upserted": 0,
        "runtime_opportunities_upserted": 0,
        "runtime_opportunities_expired": 0,
        "opportunities": [],
    }
    if not uoa_only:
        previous_promotable, previous_selection_memory = read_previous_selection(discovery_store, label)
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
        symbol_strategy_candidates = _filter_scope_candidates(
            dict(selection.get("symbol_candidates") or {}),
            scope=strategy_scope,
        )
        discovery_run_promotable_payloads = list(selection["promotable_candidates"])
        discovery_run_monitor_payloads = list(selection["monitor_candidates"])
        if args.profile == "0dte" and not discovery_run_promotable_payloads and not discovery_run_monitor_payloads:
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
                scope=strategy_scope,
            )
            discovery_run_promotable_payloads = list(selection["promotable_candidates"])
            discovery_run_monitor_payloads = list(selection["monitor_candidates"])
        discovery_run_opportunities = _filter_scope_rows(
            list(selection["opportunities"]),
            scope=strategy_scope,
        )
        discovery_run_promotable_payloads = _filter_scope_rows(
            discovery_run_promotable_payloads,
            scope=strategy_scope,
        )
        discovery_run_monitor_payloads = _filter_scope_rows(
            discovery_run_monitor_payloads,
            scope=strategy_scope,
        )
        selection_memory = dict(selection["selection_memory"])
        events = _filter_scope_rows(list(selection["events"]), scope=strategy_scope)
    raw_candidate_summary = build_raw_candidate_summary(
        scan_results,
        symbol_strategy_candidates,
    )
    if uoa_only and uoa_watchlist_summary:
        raw_candidate_summary["uoa_watchlist_summary"] = dict(uoa_watchlist_summary)
    persisted_opportunities = discovery_store.save_cycle(
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
        opportunities=discovery_run_opportunities,
        events=events,
    )
    if not uoa_only:
        try:
            signal_sync = sync_discovery_run_signal_layer(
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
        if bool(strategy_scope.get("enabled")):
            try:
                strategy_sync = sync_entry_runtime_opportunities(
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
                    selection_memory=selection_memory,
                    signal_cycle_context=signal_cycle_context,
                )
            except Exception as exc:
                print(f"Trading strategy runtime sync unavailable: {exc}")
            runtime_opportunities = [dict(row) for row in list(strategy_sync.get("opportunities") or [])]
            runtime_promotable_payloads = [dict(row) for row in runtime_opportunities if str(row.get("selection_state") or "") == "promotable"]
            runtime_monitor_payloads = [dict(row) for row in runtime_opportunities if str(row.get("selection_state") or "") == "monitor"]
        selection_summary = build_selection_summary(discovery_run_opportunities)
        strategy_sync_summary = {
            "strategy_runs_upserted": int(strategy_sync["strategy_runs_upserted"]),
            "runtime_opportunities_upserted": int(strategy_sync["runtime_opportunities_upserted"]),
            "runtime_opportunities_expired": int(strategy_sync["runtime_opportunities_expired"]),
            "runtime_selection_summary": build_selection_summary(runtime_opportunities),
        }
    capture_promotable_payloads = discovery_run_promotable_payloads
    capture_monitor_payloads = discovery_run_monitor_payloads
    capture_opportunities = discovery_run_opportunities
    if strategy_runtime_mode:
        capture_promotable_payloads = runtime_promotable_payloads
        capture_monitor_payloads = runtime_monitor_payloads
        capture_opportunities = runtime_opportunities
    if heartbeat is not None:
        heartbeat()
    quote_candidates = build_capture_candidates(
        promotable_candidates=capture_promotable_payloads,
        monitor_candidates=capture_monitor_payloads,
        opportunities=capture_opportunities,
        monitor_limit=WATCHLIST_QUOTE_CAPTURE_TOP,
    )
    capture_targets: dict[str, list[dict[str, Any]]] = {
        "promotable": [],
        "monitor": [],
    }
    if tick_context is not None and recovery_store is not None:
        try:
            if uoa_only:
                capture_targets = {
                    "promotable": [],
                    "monitor": [],
                }
            elif bool(strategy_scope.get("enabled")):
                runtime_capture_opportunities = list(strategy_sync.get("opportunities") or [])
                target_refresh = refresh_trading_strategy_capture_targets(
                    recovery_store=recovery_store,
                    session_id=tick_context.session_id,
                    session_date=session_date,
                    entry_runtimes=entry_runtimes,
                    opportunities=runtime_capture_opportunities or runtime_opportunities,
                    label=label,
                    data_base_url=getattr(scanner_args, "data_base_url", None),
                )
                capture_targets = {
                    str(reason): [dict(row) for row in rows if isinstance(row, dict)]
                    for reason, rows in dict(target_refresh.get("capture_targets") or {}).items()
                }
            else:
                target_refresh = refresh_live_session_capture_targets(
                    recovery_store=recovery_store,
                    session_id=tick_context.session_id,
                    session_date=session_date,
                    label=label,
                    profile=args.profile,
                    promotable_candidates=capture_promotable_payloads,
                    monitor_candidates=capture_monitor_payloads,
                    capture_candidates=quote_candidates,
                    data_base_url=getattr(scanner_args, "data_base_url", None),
                    session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
                )
                capture_targets = {
                    str(reason): [dict(row) for row in rows if isinstance(row, dict)]
                    for reason, rows in dict(target_refresh.get("capture_targets") or {}).items()
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
                enrich_discovery_run_job_run_payload(row)
                for row in job_store.list_job_runs(
                    job_type="discovery_run",
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
            if history_gate is not None and str(live_action_gate.get("status") or "") != "blocked":
                live_action_gate = history_gate
        except Exception as exc:
            print(f"Live capture history gate unavailable: {exc}")
    strategy_scope_enabled = bool(strategy_scope.get("enabled"))
    gate_allows_alerts = bool(live_action_gate.get("allow_alerts"))
    if uoa_only:
        live_action_gate = {
            **dict(live_action_gate),
            "status": "uoa_only",
            "reason_code": "uoa_only",
            "message": "Discovery run is UOA-only and does not own live selection, execution, or opportunity alerts.",
            "allow_auto_execution": False,
            "allow_alerts": gate_allows_alerts,
            "allow_discovery_opportunity_alerts": False,
            "allow_uoa_alerts": gate_allows_alerts,
            "opportunity_alert_owner": "uoa_only",
        }
    elif strategy_scope_enabled:
        live_action_gate = {
            **dict(live_action_gate),
            "status": "trading_strategy_runtime_owned",
            "reason_code": "trading_strategy_runtime_owned",
            "message": "Discovery run is active, but execution and opportunity alerts are owned by the trading strategy runtime.",
            "allow_auto_execution": False,
            "allow_alerts": gate_allows_alerts,
            "allow_discovery_opportunity_alerts": False,
            "allow_uoa_alerts": gate_allows_alerts,
            "opportunity_alert_owner": "trading_strategy_runtime",
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
        alert_promotable_payloads = [] if strategy_scope_enabled or uoa_only else discovery_run_promotable_payloads
        alert_events = [] if strategy_scope_enabled or uoa_only else events
        try:
            alerts = dispatch_cycle_alerts(
                discovery_store=discovery_store,
                alert_store=alert_store,
                job_store=job_store,
                cycle_id=cycle_id,
                label=label,
                generated_at=generated_at,
                strategy_mode=args.strategy,
                profile=args.profile,
                promotable_candidates=alert_promotable_payloads,
                events=alert_events,
                uoa_decisions=uoa_decisions,
                session_id=None if tick_context is None else tick_context.session_id,
                planner_job_run_id=None if tick_context is None else tick_context.job_run_id,
            )
        except Exception as exc:
            print(f"Alert dispatch unavailable: {exc}")
    if emit_output:
        print_cycle_summary(
            generated_at=generated_at,
            label=label,
            promotable_candidates=discovery_run_promotable_payloads,
            monitor_candidates=discovery_run_monitor_payloads,
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
        "promotable_opportunity_count": len(discovery_run_promotable_payloads),
        "monitor_opportunity_count": len(discovery_run_monitor_payloads),
        "signal_states_upserted": int(signal_sync["signal_states_upserted"]),
        "signal_transitions_recorded": int(signal_sync["signal_transitions_recorded"]),
        "opportunities_upserted": int(signal_sync["opportunities_upserted"]),
        "opportunities_expired": int(signal_sync["opportunities_expired"]),
        "strategy_runs_upserted": int(strategy_sync["strategy_runs_upserted"]),
        "runtime_opportunities_upserted": int(strategy_sync["runtime_opportunities_upserted"]),
        "runtime_opportunities_expired": int(strategy_sync["runtime_opportunities_expired"]),
        "quote_capture": quote_capture,
        "trade_capture": trade_capture,
        "live_action_gate": live_action_gate,
        "slot_recovery": slot_recovery,
        "capture_targets": capture_targets,
        "uoa_summary": uoa_summary,
        "uoa_quote_summary": uoa_quote_summary,
        "uoa_decisions": uoa_decisions,
        "resolved_ranking_policy": dict(raw_candidate_summary.get("resolved_ranking_policy") or {}),
        "ranking_policy_gate_summary": dict(raw_candidate_summary.get("ranking_policy_gate_summary") or {}),
        "raw_candidate_summary": raw_candidate_summary,
        "selection_summary": selection_summary,
        "strategy_sync_summary": strategy_sync_summary,
        "auto_execution": auto_execution,
        "symbol_source": symbol_source,
    }


__all__ = ["run_collection_cycle"]

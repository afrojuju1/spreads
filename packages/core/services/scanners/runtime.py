from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, time, timedelta
from typing import Any

from core.domain.models import (
    DailyBar,
    IntradayBar,
    OptionSnapshot,
    SpreadCandidate,
    SymbolMarketSlice,
    SymbolScanResult,
    UnderlyingSetupContext,
    UniverseScanFailure,
)
from core.integrations.alpaca.client import AlpacaClient
from core.integrations.calendar_events import classify_underlying_type
from core.services.market_dates import NEW_YORK
from core.services.candidate_fields import candidate_ranking_summary_row
from core.services.scanners.config import (
    build_filter_payload,
    clone_args,
    concrete_strategies,
    resolve_scan_reference_date,
    resolve_scan_reference_datetime,
    resolve_symbol_scan_args,
)
from core.services.strategy_specs import resolve_strategy_spec
from core.services.scanners.market_data import (
    build_expected_move_estimates,
    enrich_missing_greeks,
    group_contracts_by_expiration,
)
from core.services.scanners.postprocess import (
    annotate_data_quality,
    annotate_ranking_policy,
    attach_calendar_decisions,
    attach_calendar_decisions_from_map,
    attach_data_quality,
    attach_ranking_policy,
    attach_selection_notes,
    deduplicate_candidates,
    resolve_calendar_decisions_by_expiration,
)
from core.services.ranking_policy import build_ranking_policy_gate_summary
from core.services.scanners.replay_artifacts import write_scan_replay_artifact
from core.services.scanners.setup import (
    analyze_underlying_setup,
    attach_underlying_setup,
    serialize_setup_context,
)
from core.services.scanners.builders.ranking import (
    rank_candidates,
    sort_candidates_for_display,
)
from core.storage.run_history_repository import RunHistoryRepository


def build_scan_run_id(symbol: str, strategy: str, profile: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{symbol.lower()}_{strategy}_{profile}"


def persist_scan_run(
    *,
    history_store: RunHistoryRepository,
    symbol_args: argparse.Namespace,
    market_slice: SymbolMarketSlice,
    setup_context: UnderlyingSetupContext | None,
    candidates: list[SpreadCandidate],
    candidate_filter: dict[str, Any] | None = None,
    calendar_decisions_by_expiration: dict[str, Any] | None = None,
    session_label: str | None = None,
) -> str:
    run_id = build_scan_run_id(
        market_slice.symbol,
        symbol_args.strategy,
        symbol_args.profile,
    )
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    output_path = write_scan_replay_artifact(
        run_id=run_id,
        generated_at=generated_at,
        symbol_args=symbol_args,
        market_slice=market_slice,
        setup_context=setup_context,
        candidate_filter=candidate_filter,
        calendar_decisions_by_expiration=calendar_decisions_by_expiration,
    )
    history_store.save_run(
        run_id=run_id,
        generated_at=generated_at,
        symbol=market_slice.symbol,
        strategy=symbol_args.strategy,
        session_label=session_label or getattr(symbol_args, "session_label", None),
        profile=symbol_args.profile,
        spot_price=market_slice.spot_price,
        output_path=output_path,
        filters=build_filter_payload(symbol_args),
        setup_status=None if setup_context is None else setup_context.status,
        setup_score=None if setup_context is None else setup_context.score,
        setup_payload=serialize_setup_context(setup_context),
        candidates=candidates,
    )
    return run_id


def build_market_slice_from_loaded_data(
    *,
    symbol: str,
    underlying_type: str,
    spot_price: float,
    daily_bars: list[DailyBar],
    intraday_bars: list[IntradayBar],
    call_contracts_by_expiration: dict[str, list[Any]],
    put_contracts_by_expiration: dict[str, list[Any]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    greeks_provider: Any,
    greeks_as_of: datetime,
    greeks_source_mode: str,
) -> SymbolMarketSlice:
    resolved_call_snapshots = enrich_missing_greeks(
        symbol=symbol,
        option_type="call",
        spot_price=spot_price,
        contracts_by_expiration=call_contracts_by_expiration,
        snapshots_by_expiration=call_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=greeks_as_of,
        source_mode=greeks_source_mode,
    )
    resolved_put_snapshots = enrich_missing_greeks(
        symbol=symbol,
        option_type="put",
        spot_price=spot_price,
        contracts_by_expiration=put_contracts_by_expiration,
        snapshots_by_expiration=put_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=greeks_as_of,
        source_mode=greeks_source_mode,
    )
    expected_moves_by_expiration = build_expected_move_estimates(
        spot_price=spot_price,
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=resolved_call_snapshots,
        put_snapshots_by_expiration=resolved_put_snapshots,
    )
    return SymbolMarketSlice(
        symbol=symbol,
        underlying_type=underlying_type,
        spot_price=spot_price,
        daily_bars=tuple(daily_bars),
        intraday_bars=tuple(intraday_bars),
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=resolved_call_snapshots,
        put_snapshots_by_expiration=resolved_put_snapshots,
        expected_moves_by_expiration=expected_moves_by_expiration,
    )


def build_symbol_market_slice(
    *,
    symbol: str,
    symbol_args: argparse.Namespace,
    client: AlpacaClient,
    greeks_provider: Any,
) -> SymbolMarketSlice:
    normalized_symbol = symbol.upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    reference_date = resolve_scan_reference_date(symbol_args)
    reference_timestamp = resolve_scan_reference_datetime(symbol_args) or datetime.now(UTC)
    min_expiration = (reference_date + timedelta(days=symbol_args.min_dte)).isoformat()
    max_expiration = (reference_date + timedelta(days=symbol_args.max_dte)).isoformat()

    spot_price = client.get_underlying_price(normalized_symbol, symbol_args.stock_feed)
    daily_bars: list[DailyBar] = []
    intraday_bars: list[IntradayBar] = []
    if symbol_args.setup_filter == "on":
        daily_bars = client.get_daily_bars(
            normalized_symbol,
            start=(reference_date - timedelta(days=120)).isoformat(),
            end=reference_date.isoformat(),
            stock_feed=symbol_args.stock_feed,
        )
        try:
            session_start = datetime.combine(reference_date, time(9, 30), tzinfo=NEW_YORK).astimezone(UTC)
            session_end = reference_timestamp
            intraday_bars = client.get_intraday_bars(
                normalized_symbol,
                start=session_start.isoformat(),
                end=session_end.isoformat(),
                stock_feed=symbol_args.stock_feed,
            )
        except Exception:
            intraday_bars = []

    call_contracts = client.list_option_contracts(normalized_symbol, min_expiration, max_expiration, option_type="call")
    put_contracts = client.list_option_contracts(normalized_symbol, min_expiration, max_expiration, option_type="put")
    call_contracts_by_expiration = group_contracts_by_expiration(call_contracts)
    put_contracts_by_expiration = group_contracts_by_expiration(put_contracts)

    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date in sorted(call_contracts_by_expiration):
        call_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            normalized_symbol,
            expiration_date,
            "call",
            symbol_args.feed,
        )
        put_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            normalized_symbol,
            expiration_date,
            "put",
            symbol_args.feed,
        )

    return build_market_slice_from_loaded_data(
        symbol=normalized_symbol,
        underlying_type=underlying_type,
        spot_price=spot_price,
        daily_bars=daily_bars,
        intraday_bars=intraday_bars,
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        greeks_as_of=reference_timestamp,
        greeks_source_mode=symbol_args.greeks_source,
    )


def build_setup_context_from_market_slice(*, market_slice: SymbolMarketSlice, symbol_args: argparse.Namespace) -> UnderlyingSetupContext | None:
    if symbol_args.setup_filter != "on":
        return None
    return analyze_underlying_setup(
        market_slice.symbol,
        market_slice.spot_price,
        list(market_slice.daily_bars),
        strategy=symbol_args.strategy,
        profile=symbol_args.profile,
        intraday_bars=list(market_slice.intraday_bars),
    )


def count_market_slice_coverage(*, market_slice: SymbolMarketSlice, symbol_args: argparse.Namespace) -> tuple[int, int, int, int]:
    spec = resolve_strategy_spec(symbol_args.strategy)
    return spec.count_coverage(market_slice=market_slice)


def build_raw_candidates_from_market_slice(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    spec = resolve_strategy_spec(symbol_args.strategy)
    return spec.build_candidates(market_slice=market_slice, symbol_args=symbol_args)


def _count_candidate_field_values(
    candidates: list[SpreadCandidate],
    *,
    field: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for candidate in candidates:
        value = getattr(candidate, field, None)
        normalized = "unknown" if value in (None, "") else str(value)
        counts[normalized] += 1
    return dict(sorted(counts.items()))


def _count_candidate_reason_values(
    candidates: list[SpreadCandidate],
    *,
    field: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for candidate in candidates:
        values = getattr(candidate, field, ())
        for value in values if isinstance(values, (tuple, list)) else ():
            normalized = str(value or "").strip()
            if normalized:
                counts[normalized] += 1
    return dict(sorted(counts.items()))


def _calendar_reason_code_counts(
    *,
    candidates: list[SpreadCandidate],
    calendar_decisions_by_expiration: dict[str, Any],
) -> dict[str, int]:
    counts_by_expiration = Counter(candidate.expiration_date for candidate in candidates)
    reason_counts: Counter[str] = Counter()
    for expiration_date, decision in calendar_decisions_by_expiration.items():
        candidate_count = int(counts_by_expiration.get(expiration_date) or 0)
        if candidate_count <= 0:
            continue
        for reason in list(getattr(decision, "reasons", ()) or ()):
            code = str(getattr(reason, "code", "") or "").strip()
            if code:
                reason_counts[code] += candidate_count
    return dict(sorted(reason_counts.items()))


def _ranking_policy_blocked_exemplars(
    *,
    candidates: list[SpreadCandidate],
    args: argparse.Namespace,
    limit: int = 3,
) -> list[dict[str, Any]]:
    blocked_candidates = [candidate for candidate in candidates if str(candidate.ranking_policy_status or "").lower() == "blocked"]
    if not blocked_candidates:
        return []
    ranked_blocked_candidates = rank_candidates(
        attach_selection_notes(blocked_candidates, args),
        args,
    )
    return [candidate_ranking_summary_row(candidate.to_payload()) for candidate in ranked_blocked_candidates[: max(int(limit), 1)]]


def postprocess_market_slice_candidates(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
    raw_candidates: list[SpreadCandidate],
    setup_context: UnderlyingSetupContext | None,
    calendar_resolver: Any | None = None,
    calendar_decisions_by_expiration: dict[str, Any] | None = None,
) -> list[SpreadCandidate]:
    all_candidates = attach_underlying_setup(raw_candidates, setup_context)
    if calendar_decisions_by_expiration is not None:
        all_candidates = attach_calendar_decisions_from_map(
            candidates=all_candidates,
            decisions_by_expiration=calendar_decisions_by_expiration,
            calendar_policy=symbol_args.calendar_policy,
        )
    else:
        all_candidates = attach_calendar_decisions(
            symbol=market_slice.symbol,
            strategy=symbol_args.strategy,
            underlying_type=market_slice.underlying_type,
            candidates=all_candidates,
            resolver=calendar_resolver,
            calendar_policy=symbol_args.calendar_policy,
            refresh_calendar_events=symbol_args.refresh_calendar_events,
        )
    all_candidates = attach_data_quality(
        candidates=all_candidates,
        underlying_type=market_slice.underlying_type,
        args=symbol_args,
    )
    all_candidates = attach_ranking_policy(
        candidates=all_candidates,
        args=symbol_args,
    )
    all_candidates = attach_selection_notes(all_candidates, symbol_args)
    all_candidates = rank_candidates(all_candidates, symbol_args)
    return deduplicate_candidates(all_candidates, symbol_args.expand_duplicates)


def build_candidates_with_details_from_market_slice(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
    calendar_resolver: Any,
) -> tuple[list[SpreadCandidate], UnderlyingSetupContext | None, dict[str, Any]]:
    setup_context = build_setup_context_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
    )
    raw_candidates = build_raw_candidates_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
    )
    setup_candidates = attach_underlying_setup(raw_candidates, setup_context)
    calendar_decisions_by_expiration = resolve_calendar_decisions_by_expiration(
        symbol=market_slice.symbol,
        strategy=symbol_args.strategy,
        underlying_type=market_slice.underlying_type,
        candidates=setup_candidates,
        resolver=calendar_resolver,
        calendar_policy=symbol_args.calendar_policy,
        refresh_calendar_events=symbol_args.refresh_calendar_events,
        window_start=(None if resolve_scan_reference_datetime(symbol_args) is None else resolve_scan_reference_datetime(symbol_args).isoformat()),
    )
    calendar_annotated_candidates = attach_calendar_decisions_from_map(
        candidates=setup_candidates,
        decisions_by_expiration=calendar_decisions_by_expiration,
        calendar_policy="warn",
    )
    diagnostic_candidates = annotate_data_quality(
        candidates=calendar_annotated_candidates,
        underlying_type=market_slice.underlying_type,
        args=symbol_args,
    )
    policy_candidates = annotate_ranking_policy(
        candidates=diagnostic_candidates,
        args=symbol_args,
    )
    ranking_blocked_exemplars = _ranking_policy_blocked_exemplars(
        candidates=annotate_ranking_policy(
            candidates=attach_data_quality(
                candidates=calendar_annotated_candidates,
                underlying_type=market_slice.underlying_type,
                args=symbol_args,
            ),
            args=symbol_args,
        ),
        args=symbol_args,
    )
    all_candidates = postprocess_market_slice_candidates(
        market_slice=market_slice,
        symbol_args=symbol_args,
        raw_candidates=raw_candidates,
        setup_context=setup_context,
        calendar_decisions_by_expiration=calendar_decisions_by_expiration,
    )
    return (
        all_candidates,
        setup_context,
        {
            "calendar_decisions_by_expiration": calendar_decisions_by_expiration,
            "raw_candidate_count": len(raw_candidates),
            "postprocess_candidate_count": len(all_candidates),
            "setup_status_counts": _count_candidate_field_values(
                setup_candidates,
                field="setup_status",
            ),
            "calendar_status_counts": _count_candidate_field_values(
                calendar_annotated_candidates,
                field="calendar_status",
            ),
            "calendar_reason_counts": _calendar_reason_code_counts(
                candidates=setup_candidates,
                calendar_decisions_by_expiration=calendar_decisions_by_expiration,
            ),
            "data_status_counts": _count_candidate_field_values(
                diagnostic_candidates,
                field="data_status",
            ),
            "data_reason_counts": _count_candidate_reason_values(
                diagnostic_candidates,
                field="data_reasons",
            ),
            "ranking_policy_gate_summary": build_ranking_policy_gate_summary(items=policy_candidates),
            "ranking_policy_status_counts": _count_candidate_field_values(
                policy_candidates,
                field="ranking_policy_status",
            ),
            "ranking_policy_blocker_counts": _count_candidate_reason_values(
                policy_candidates,
                field="ranking_policy_blockers",
            ),
            "ranking_policy_blocked_exemplars": ranking_blocked_exemplars,
        },
    )


def build_candidates_from_market_slice(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
    calendar_resolver: Any,
) -> tuple[list[SpreadCandidate], UnderlyingSetupContext | None]:
    candidates, setup_context, _details = build_candidates_with_details_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
        calendar_resolver=calendar_resolver,
    )
    return candidates, setup_context


def scan_symbol_live(
    *,
    symbol: str,
    base_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    history_store: RunHistoryRepository,
) -> SymbolScanResult:
    symbol = symbol.upper()
    symbol_args, underlying_type = resolve_symbol_scan_args(symbol=symbol, base_args=base_args)
    market_slice = build_symbol_market_slice(
        symbol=symbol,
        symbol_args=symbol_args,
        client=client,
        greeks_provider=greeks_provider,
    )
    (
        quoted_contract_count,
        alpaca_delta_contract_count,
        delta_contract_count,
        local_delta_contract_count,
    ) = count_market_slice_coverage(market_slice=market_slice, symbol_args=symbol_args)
    all_candidates, setup_context, replay_details = build_candidates_with_details_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
        calendar_resolver=calendar_resolver,
    )

    run_id = persist_scan_run(
        history_store=history_store,
        symbol_args=symbol_args,
        market_slice=market_slice,
        setup_context=setup_context,
        candidates=all_candidates,
        calendar_decisions_by_expiration=replay_details.get("calendar_decisions_by_expiration"),
        session_label=getattr(symbol_args, "session_label", None),
    )

    return SymbolScanResult(
        symbol=symbol,
        underlying_type=underlying_type,
        spot_price=market_slice.spot_price,
        args=symbol_args,
        setup=setup_context,
        candidates=all_candidates,
        run_id=run_id,
        quoted_contract_count=quoted_contract_count,
        alpaca_delta_contract_count=alpaca_delta_contract_count,
        delta_contract_count=delta_contract_count,
        local_delta_contract_count=local_delta_contract_count,
        diagnostics=replay_details,
    )


def scan_symbol_across_strategies(
    *,
    symbol: str,
    base_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    history_store: RunHistoryRepository,
) -> tuple[list[SymbolScanResult], list[UniverseScanFailure]]:
    results: list[SymbolScanResult] = []
    failures: list[UniverseScanFailure] = []
    for strategy in concrete_strategies(base_args.strategy):
        strategy_args = clone_args(base_args)
        strategy_args.strategy = strategy
        try:
            results.append(
                scan_symbol_live(
                    symbol=symbol,
                    base_args=strategy_args,
                    client=client,
                    calendar_resolver=calendar_resolver,
                    greeks_provider=greeks_provider,
                    history_store=history_store,
                )
            )
        except Exception as exc:
            label = f"{symbol}:{strategy}" if base_args.strategy in {"combined", "auto"} else symbol
            failures.append(UniverseScanFailure(symbol=label, error=str(exc).splitlines()[0]))
    return results, failures


def merge_strategy_candidates(
    results: list[SymbolScanResult],
    *,
    per_strategy_top: int | None = None,
) -> list[SpreadCandidate]:
    merged: list[SpreadCandidate] = []
    for result in results:
        candidates = result.candidates if per_strategy_top is None else result.candidates[:per_strategy_top]
        merged.extend(candidates)
    return sort_candidates_for_display(merged)


__all__ = [
    "build_market_slice_from_loaded_data",
    "build_candidates_with_details_from_market_slice",
    "build_candidates_from_market_slice",
    "build_raw_candidates_from_market_slice",
    "build_scan_run_id",
    "build_setup_context_from_market_slice",
    "build_symbol_market_slice",
    "count_market_slice_coverage",
    "merge_strategy_candidates",
    "postprocess_market_slice_candidates",
    "persist_scan_run",
    "scan_symbol_across_strategies",
    "scan_symbol_live",
]

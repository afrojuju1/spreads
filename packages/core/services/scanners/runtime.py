from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time, timedelta
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
from core.domain.profiles import LONG_VOL_STRATEGIES
from core.integrations.alpaca.client import AlpacaClient
from core.integrations.calendar_events import classify_underlying_type
from core.services.market_dates import NEW_YORK
from core.services.scanners.config import (
    build_filter_payload,
    clone_args,
    concrete_strategies,
    resolve_symbol_scan_args,
    strategy_option_type,
)
from core.services.scanners.market_data import (
    build_expected_move_estimates,
    count_alpaca_greeks_coverage,
    count_local_greeks_coverage,
    count_snapshot_delta_coverage,
    enrich_missing_greeks,
    group_contracts_by_expiration,
)
from core.storage.run_history_repository import RunHistoryRepository

from .service import (
    analyze_underlying_setup,
    attach_calendar_decisions,
    attach_data_quality,
    attach_selection_notes,
    attach_underlying_setup,
    build_iron_condors,
    build_long_straddles,
    build_long_strangles,
    build_vertical_spreads,
    deduplicate_candidates,
    rank_candidates,
    serialize_setup_context,
    sort_candidates_for_display,
)


def _build_run_id(symbol: str, strategy: str, profile: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{symbol.lower()}_{strategy}_{profile}"


def build_symbol_market_slice(
    *,
    symbol: str,
    symbol_args: argparse.Namespace,
    client: AlpacaClient,
    greeks_provider: Any,
) -> SymbolMarketSlice:
    normalized_symbol = symbol.upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    min_expiration = (date.today() + timedelta(days=symbol_args.min_dte)).isoformat()
    max_expiration = (date.today() + timedelta(days=symbol_args.max_dte)).isoformat()

    spot_price = client.get_underlying_price(normalized_symbol, symbol_args.stock_feed)
    daily_bars: list[DailyBar] = []
    intraday_bars: list[IntradayBar] = []
    if symbol_args.setup_filter == "on":
        daily_bars = client.get_daily_bars(
            normalized_symbol,
            start=(date.today() - timedelta(days=120)).isoformat(),
            end=date.today().isoformat(),
            stock_feed=symbol_args.stock_feed,
        )
        try:
            session_start = datetime.combine(
                date.today(), time(9, 30), tzinfo=NEW_YORK
            ).astimezone(UTC)
            session_end = datetime.now(UTC)
            intraday_bars = client.get_intraday_bars(
                normalized_symbol,
                start=session_start.isoformat(),
                end=session_end.isoformat(),
                stock_feed=symbol_args.stock_feed,
            )
        except Exception:
            intraday_bars = []

    call_contracts = client.list_option_contracts(
        normalized_symbol, min_expiration, max_expiration, option_type="call"
    )
    put_contracts = client.list_option_contracts(
        normalized_symbol, min_expiration, max_expiration, option_type="put"
    )
    call_contracts_by_expiration = group_contracts_by_expiration(call_contracts)
    put_contracts_by_expiration = group_contracts_by_expiration(put_contracts)

    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date in sorted(call_contracts_by_expiration):
        call_snapshots_by_expiration[expiration_date] = (
            client.get_option_chain_snapshots(
                normalized_symbol,
                expiration_date,
                "call",
                symbol_args.feed,
            )
        )
        put_snapshots_by_expiration[expiration_date] = (
            client.get_option_chain_snapshots(
                normalized_symbol,
                expiration_date,
                "put",
                symbol_args.feed,
            )
        )

    snapshot_timestamp = datetime.now(UTC)
    call_snapshots_by_expiration = enrich_missing_greeks(
        symbol=normalized_symbol,
        option_type="call",
        spot_price=spot_price,
        contracts_by_expiration=call_contracts_by_expiration,
        snapshots_by_expiration=call_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=snapshot_timestamp,
        source_mode=symbol_args.greeks_source,
    )
    put_snapshots_by_expiration = enrich_missing_greeks(
        symbol=normalized_symbol,
        option_type="put",
        spot_price=spot_price,
        contracts_by_expiration=put_contracts_by_expiration,
        snapshots_by_expiration=put_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=snapshot_timestamp,
        source_mode=symbol_args.greeks_source,
    )
    expected_moves_by_expiration = build_expected_move_estimates(
        spot_price=spot_price,
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
    )
    return SymbolMarketSlice(
        symbol=normalized_symbol,
        underlying_type=underlying_type,
        spot_price=spot_price,
        daily_bars=tuple(daily_bars),
        intraday_bars=tuple(intraday_bars),
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
    )


def build_setup_context_from_market_slice(
    *, market_slice: SymbolMarketSlice, symbol_args: argparse.Namespace
) -> UnderlyingSetupContext | None:
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


def count_market_slice_coverage(
    *, market_slice: SymbolMarketSlice, symbol_args: argparse.Namespace
) -> tuple[int, int, int, int]:
    if (
        symbol_args.strategy == "iron_condor"
        or symbol_args.strategy in LONG_VOL_STRATEGIES
    ):
        call_quoted_count, call_delta_count = count_snapshot_delta_coverage(
            market_slice.call_snapshots_by_expiration
        )
        put_quoted_count, put_delta_count = count_snapshot_delta_coverage(
            market_slice.put_snapshots_by_expiration
        )
        quoted_contract_count = call_quoted_count + put_quoted_count
        alpaca_delta_contract_count = count_alpaca_greeks_coverage(
            market_slice.call_snapshots_by_expiration
        ) + count_alpaca_greeks_coverage(market_slice.put_snapshots_by_expiration)
        delta_contract_count = call_delta_count + put_delta_count
        local_delta_contract_count = count_local_greeks_coverage(
            market_slice.call_snapshots_by_expiration
        ) + count_local_greeks_coverage(market_slice.put_snapshots_by_expiration)
        return (
            quoted_contract_count,
            alpaca_delta_contract_count,
            delta_contract_count,
            local_delta_contract_count,
        )

    option_type = strategy_option_type(symbol_args.strategy)
    option_snapshots_by_expiration = (
        market_slice.call_snapshots_by_expiration
        if option_type == "call"
        else market_slice.put_snapshots_by_expiration
    )
    quoted_contract_count, delta_contract_count = count_snapshot_delta_coverage(
        option_snapshots_by_expiration
    )
    alpaca_delta_contract_count = count_alpaca_greeks_coverage(
        option_snapshots_by_expiration
    )
    local_delta_contract_count = count_local_greeks_coverage(
        option_snapshots_by_expiration
    )
    return (
        quoted_contract_count,
        alpaca_delta_contract_count,
        delta_contract_count,
        local_delta_contract_count,
    )


def build_candidates_from_market_slice(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
    calendar_resolver: Any,
) -> tuple[list[SpreadCandidate], UnderlyingSetupContext | None]:
    setup_context = build_setup_context_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
    )
    if symbol_args.strategy == "iron_condor":
        all_candidates = build_iron_condors(
            symbol=market_slice.symbol,
            spot_price=market_slice.spot_price,
            call_contracts_by_expiration=market_slice.call_contracts_by_expiration,
            put_contracts_by_expiration=market_slice.put_contracts_by_expiration,
            call_snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
            put_snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
            expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
            args=symbol_args,
        )
    elif symbol_args.strategy == "long_straddle":
        all_candidates = build_long_straddles(
            symbol=market_slice.symbol,
            spot_price=market_slice.spot_price,
            call_contracts_by_expiration=market_slice.call_contracts_by_expiration,
            put_contracts_by_expiration=market_slice.put_contracts_by_expiration,
            call_snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
            put_snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
            expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
            args=symbol_args,
        )
    elif symbol_args.strategy == "long_strangle":
        all_candidates = build_long_strangles(
            symbol=market_slice.symbol,
            spot_price=market_slice.spot_price,
            call_contracts_by_expiration=market_slice.call_contracts_by_expiration,
            put_contracts_by_expiration=market_slice.put_contracts_by_expiration,
            call_snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
            put_snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
            expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
            args=symbol_args,
        )
    else:
        option_type = strategy_option_type(symbol_args.strategy)
        option_contracts_by_expiration = (
            market_slice.call_contracts_by_expiration
            if option_type == "call"
            else market_slice.put_contracts_by_expiration
        )
        option_snapshots_by_expiration = (
            market_slice.call_snapshots_by_expiration
            if option_type == "call"
            else market_slice.put_snapshots_by_expiration
        )
        all_candidates = build_vertical_spreads(
            symbol=market_slice.symbol,
            strategy=symbol_args.strategy,
            spot_price=market_slice.spot_price,
            contracts_by_expiration=option_contracts_by_expiration,
            snapshots_by_expiration=option_snapshots_by_expiration,
            expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
            args=symbol_args,
        )
    all_candidates = attach_underlying_setup(all_candidates, setup_context)
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
    all_candidates = attach_selection_notes(all_candidates, symbol_args)
    all_candidates = rank_candidates(all_candidates, symbol_args)
    all_candidates = deduplicate_candidates(
        all_candidates, symbol_args.expand_duplicates
    )
    return all_candidates, setup_context


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
    symbol_args, underlying_type = resolve_symbol_scan_args(
        symbol=symbol, base_args=base_args
    )
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
    all_candidates, setup_context = build_candidates_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
        calendar_resolver=calendar_resolver,
    )

    run_id = _build_run_id(symbol, symbol_args.strategy, symbol_args.profile)
    generated_at = (
        datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    history_store.save_run(
        run_id=run_id,
        generated_at=generated_at,
        symbol=symbol,
        strategy=symbol_args.strategy,
        session_label=getattr(symbol_args, "session_label", None),
        profile=symbol_args.profile,
        spot_price=market_slice.spot_price,
        output_path="",
        filters=build_filter_payload(symbol_args),
        setup_status=None if setup_context is None else setup_context.status,
        setup_score=None if setup_context is None else setup_context.score,
        setup_payload=serialize_setup_context(setup_context),
        candidates=all_candidates,
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
            label = (
                f"{symbol}:{strategy}" if base_args.strategy == "combined" else symbol
            )
            failures.append(
                UniverseScanFailure(symbol=label, error=str(exc).splitlines()[0])
            )
    return results, failures


def merge_strategy_candidates(
    results: list[SymbolScanResult],
    *,
    per_strategy_top: int | None = None,
) -> list[SpreadCandidate]:
    merged: list[SpreadCandidate] = []
    for result in results:
        candidates = (
            result.candidates
            if per_strategy_top is None
            else result.candidates[:per_strategy_top]
        )
        merged.extend(candidates)
    return sort_candidates_for_display(merged)


__all__ = [
    "build_candidates_from_market_slice",
    "build_setup_context_from_market_slice",
    "build_symbol_market_slice",
    "count_market_slice_coverage",
    "merge_strategy_candidates",
    "scan_symbol_across_strategies",
    "scan_symbol_live",
]

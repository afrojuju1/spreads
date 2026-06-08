from __future__ import annotations

import argparse
from typing import Any

from core.domain.models import (
    SpreadCandidate,
    SymbolScanResult,
    UniverseScanFailure,
)
from core.integrations.alpaca.client import AlpacaClient
from core.services.scanners.config import (
    clone_args,
    concrete_strategies,
    resolve_symbol_scan_args,
)
from core.services.strategy_candidate_builders.market_data import build_symbol_market_slice
from core.services.strategy_candidate_builders.ranking import (
    sort_candidates_for_display,
)
from core.services.strategy_candidate_builders.runtime import (
    build_candidates_from_market_slice,
    build_candidates_with_details_from_market_slice,
    build_raw_candidates_from_market_slice,
    build_scan_run_id,
    build_setup_context_from_market_slice,
    count_market_slice_coverage,
    persist_scan_run,
    postprocess_market_slice_candidates,
)
from core.storage.run_history_repository import RunHistoryRepository


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
        parameters=symbol_args,
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
    "build_candidates_with_details_from_market_slice",
    "build_candidates_from_market_slice",
    "build_raw_candidates_from_market_slice",
    "build_scan_run_id",
    "build_setup_context_from_market_slice",
    "count_market_slice_coverage",
    "merge_strategy_candidates",
    "postprocess_market_slice_candidates",
    "persist_scan_run",
    "scan_symbol_across_strategies",
    "scan_symbol_live",
]

from __future__ import annotations

import argparse
from dataclasses import asdict
from typing import Any

from core.domain.models import SpreadCandidate, SymbolScanResult, UniverseScanFailure
from core.services.live_pipelines import build_live_snapshot_label
from core.services.scanners.builders import sort_candidates_for_display
from core.services.scanners.config import resolve_symbols
from core.services.scanners.runtime import (
    merge_strategy_candidates,
    scan_symbol_across_strategies,
)
from core.storage.run_history_repository import RunHistoryRepository


def run_universe_cycle(
    *,
    scanner_args: argparse.Namespace,
    client: Any,
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


__all__ = [
    "build_raw_candidate_summary",
    "build_symbol_strategy_candidates",
    "run_universe_cycle",
]

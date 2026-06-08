from __future__ import annotations

from collections.abc import Mapping
from collections import Counter
from dataclasses import asdict, is_dataclass
from typing import Any

from core.domain.models import SymbolMarketSlice, UnderlyingSetupContext
from core.integrations.alpaca.client import AlpacaClient
from core.services.option_structures import candidate_legs, payload_structure_identity
from core.services.runtime_candidate_filters import (
    build_runtime_candidate_filter,
    match_runtime_candidate,
)
from core.services.strategy_candidate_builders.market_data import AlpacaMarketSliceProvider, MarketSliceProvider, build_underlying_market_slice
from core.services.strategy_candidate_builders.runtime import (
    build_candidates_with_details_from_market_slice,
)
from core.services.strategy_candidate_builders.settings import (
    CandidateBuildParameters,
    build_market_slice_parameters,
    resolve_symbol_candidate_build_parameters,
)
from core.services.strategy_candidate_builders.setup import build_relative_strength_market_context
from core.services.strategy_candidate_builders.single_legs import diagnose_single_leg_rejections
from core.services.trading_strategy_runtime import EntryRuntime

DEFAULT_MARKET_BENCHMARK_SYMBOLS = ("SPY", "QQQ")


def runtime_owner_key(runtime: EntryRuntime) -> tuple[str, str]:
    return runtime.trading_strategy_id, "entry"


def build_runtime_candidate_parameters(
    *,
    symbol: str,
    base_parameters: CandidateBuildParameters,
    runtime: EntryRuntime,
) -> CandidateBuildParameters:
    parameters, _underlying_type = resolve_symbol_candidate_build_parameters(
        symbol=symbol,
        base_parameters=base_parameters,
        settings=runtime.build_settings,
        config_root=base_parameters.config_root,
    )
    return parameters


def build_symbol_market_slice_parameters(
    *,
    symbol: str,
    base_parameters: CandidateBuildParameters,
    runtimes: list[EntryRuntime],
) -> CandidateBuildParameters:
    runtime_parameters = [
        build_runtime_candidate_parameters(
            symbol=symbol,
            base_parameters=base_parameters,
            runtime=runtime,
        )
        for runtime in runtimes
    ]
    return build_market_slice_parameters(
        symbol=symbol,
        base_parameters=base_parameters,
        runtime_parameters=runtime_parameters,
    )


def _serialize_candidate(
    candidate: Any,
    *,
    short_delta_target: float | None = None,
) -> dict[str, Any]:
    if hasattr(candidate, "__dataclass_fields__"):
        row = dict(candidate.to_payload()) if hasattr(candidate, "to_payload") else dict(asdict(candidate))
        row["legs"] = candidate_legs(row)
        row["structure_identity"] = payload_structure_identity(row)
        if short_delta_target is not None:
            row["short_delta_target"] = float(short_delta_target)
        return row
    if isinstance(candidate, dict):
        row = dict(candidate)
        row["legs"] = candidate_legs(row)
        row["structure_identity"] = payload_structure_identity(row)
        if short_delta_target is not None:
            row["short_delta_target"] = float(short_delta_target)
        return row
    raise TypeError("Unsupported candidate payload for runtime strategy builder")


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _setup_summary(
    setup_context: UnderlyingSetupContext | None,
    *,
    market_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if setup_context is None:
        return dict(market_context or {})
    payload = asdict(setup_context)
    summary = {
        key: payload.get(key)
        for key in (
            "status",
            "score",
            "reasons",
            "daily_score",
            "intraday_score",
            "spot_vs_vwap_pct",
            "intraday_return_pct",
            "spot_vs_sma20_pct",
            "sma20_vs_sma50_pct",
            "return_5d_pct",
            "source_window_minutes",
        )
        if payload.get(key) is not None
    }
    summary.update(dict(market_context or {}))
    return summary


def _market_side_maps(
    *,
    market_slice: SymbolMarketSlice,
    option_type: str | None,
) -> tuple[dict[str, list[Any]], dict[str, dict[str, Any]]]:
    if option_type == "call":
        return market_slice.call_contracts_by_expiration, market_slice.call_snapshots_by_expiration
    if option_type == "put":
        return market_slice.put_contracts_by_expiration, market_slice.put_snapshots_by_expiration
    contracts: dict[str, list[Any]] = {}
    snapshots: dict[str, dict[str, Any]] = {}
    for expiration_date, rows in market_slice.call_contracts_by_expiration.items():
        contracts.setdefault(expiration_date, []).extend(rows)
    for expiration_date, rows in market_slice.put_contracts_by_expiration.items():
        contracts.setdefault(expiration_date, []).extend(rows)
    for expiration_date, rows in market_slice.call_snapshots_by_expiration.items():
        snapshots.setdefault(expiration_date, {}).update(rows)
    for expiration_date, rows in market_slice.put_snapshots_by_expiration.items():
        snapshots.setdefault(expiration_date, {}).update(rows)
    return contracts, snapshots


def _count_snapshot_delta_coverage(snapshots_by_expiration: dict[str, dict[str, Any]]) -> tuple[int, int]:
    snapshot_count = 0
    delta_count = 0
    for rows in snapshots_by_expiration.values():
        for snapshot in rows.values():
            snapshot_count += 1
            if getattr(snapshot, "delta", None) is not None:
                delta_count += 1
    return snapshot_count, delta_count


def _combined_rejection_counts(
    *,
    raw_rejections: dict[str, Any],
    replay_details: dict[str, Any],
    runtime_filter_reason_counts: dict[str, int],
) -> dict[str, Any]:
    top_counts: Counter[str] = Counter()
    sections = {
        "raw": raw_rejections,
        "data": replay_details.get("data_reason_counts") or {},
        "calendar": replay_details.get("calendar_reason_counts") or {},
        "ranking_policy": replay_details.get("ranking_policy_blocker_counts") or {},
        "runtime_filter": runtime_filter_reason_counts,
    }
    for counts in sections.values():
        for reason, count in dict(counts or {}).items():
            try:
                top_counts[str(reason)] += int(count)
            except (TypeError, ValueError):
                continue
    return {**sections, "top": dict(top_counts.most_common(12))}


def _diagnostic_status(
    *,
    contract_count: int,
    snapshot_count: int,
    raw_candidate_count: int,
    postprocess_candidate_count: int,
    runtime_candidate_count: int,
    returned_candidate_count: int,
    ranking_rejections: dict[str, Any],
    runtime_filter_reason_counts: dict[str, int],
) -> str:
    if returned_candidate_count > 0:
        return "candidate_available"
    if runtime_candidate_count > 0:
        return "candidate_available"
    if runtime_filter_reason_counts:
        return "runtime_rejected"
    if postprocess_candidate_count > 0:
        return "candidate_available"
    if ranking_rejections:
        return "ranking_rejected"
    if raw_candidate_count > 0:
        return "postprocess_rejected"
    if contract_count <= 0 or snapshot_count <= 0:
        return "data_unavailable"
    return "no_raw_candidates"


def _single_leg_diagnostics(
    *,
    runtime: EntryRuntime,
    market_slice: SymbolMarketSlice,
    runtime_parameters: CandidateBuildParameters,
) -> dict[str, Any]:
    strategy_family = runtime.build_settings.strategy_spec.strategy_family
    if strategy_family not in {"long_call", "long_put", "short_call", "short_put"}:
        return {"reject_counts": {}, "examples": {}, "pass_count": 0, "pass_examples": []}
    option_type = runtime.build_settings.strategy_spec.option_type
    contracts_by_expiration, snapshots_by_expiration = _market_side_maps(
        market_slice=market_slice,
        option_type=option_type,
    )
    return diagnose_single_leg_rejections(
        strategy=runtime.build_settings.candidate_builder_key,
        spot_price=market_slice.spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=runtime_parameters,
    )


def build_entry_runtime_symbol_diagnostic(
    *,
    runtime: EntryRuntime,
    symbol: str,
    market_slice: SymbolMarketSlice,
    benchmark_slices_by_symbol: Mapping[str, SymbolMarketSlice] | None = None,
    runtime_parameters: CandidateBuildParameters,
    setup_context: UnderlyingSetupContext | None,
    replay_details: dict[str, Any],
    all_rows: list[dict[str, Any]],
    returned_rows: list[dict[str, Any]],
    runtime_filter_reason_counts: dict[str, int],
) -> dict[str, Any]:
    option_type = runtime.build_settings.strategy_spec.option_type
    contracts_by_expiration, snapshots_by_expiration = _market_side_maps(
        market_slice=market_slice,
        option_type=option_type,
    )
    contract_count = sum(len(rows or []) for rows in contracts_by_expiration.values())
    snapshot_count, delta_snapshot_count = _count_snapshot_delta_coverage(snapshots_by_expiration)
    raw_candidate_count = int(replay_details.get("raw_candidate_count") or 0)
    postprocess_candidate_count = int(replay_details.get("postprocess_candidate_count") or 0)
    runtime_candidate_count = len(all_rows)
    returned_candidate_count = len(returned_rows)
    single_leg_diagnostics = _single_leg_diagnostics(
        runtime=runtime,
        market_slice=market_slice,
        runtime_parameters=runtime_parameters,
    )
    raw_rejections = dict(single_leg_diagnostics.get("reject_counts") or {})
    ranking_rejections = dict(replay_details.get("ranking_policy_blocker_counts") or {})
    setup_market_context = build_relative_strength_market_context(
        market_slice=market_slice,
        benchmark_slices=benchmark_slices_by_symbol,
        benchmark_symbols=DEFAULT_MARKET_BENCHMARK_SYMBOLS,
    )
    status = _diagnostic_status(
        contract_count=contract_count,
        snapshot_count=snapshot_count,
        raw_candidate_count=raw_candidate_count,
        postprocess_candidate_count=postprocess_candidate_count,
        runtime_candidate_count=runtime_candidate_count,
        returned_candidate_count=returned_candidate_count,
        ranking_rejections=ranking_rejections,
        runtime_filter_reason_counts=runtime_filter_reason_counts,
    )
    market_data = {
        "underlying_type": market_slice.underlying_type,
        "daily_bar_count": len(market_slice.daily_bars),
        "intraday_bar_count": len(market_slice.intraday_bars),
        "expiration_count": len(contracts_by_expiration),
        "contract_count": contract_count,
        "snapshot_count": snapshot_count,
        "delta_snapshot_count": delta_snapshot_count,
        "expected_move_count": len(market_slice.expected_moves_by_expiration),
        "expirations": sorted(contracts_by_expiration),
        "filters": {
            "min_dte": runtime_parameters.min_dte,
            "max_dte": runtime_parameters.max_dte,
            "delta_min": runtime_parameters.short_delta_min,
            "delta_max": runtime_parameters.short_delta_max,
            "min_open_interest": runtime_parameters.min_open_interest,
            "max_relative_spread": runtime_parameters.max_relative_spread,
            "min_return_on_risk": runtime_parameters.min_return_on_risk,
            "min_credit": runtime_parameters.min_credit,
            "max_quote_age_seconds": runtime_parameters.max_quote_age_seconds,
        },
    }
    return {
        "underlying_symbol": symbol,
        "diagnostic_status": status,
        "observed_at": None,
        "spot_price": market_slice.spot_price,
        "expiration_count": len(contracts_by_expiration),
        "contract_count": contract_count,
        "snapshot_count": snapshot_count,
        "raw_candidate_count": raw_candidate_count,
        "postprocess_candidate_count": postprocess_candidate_count,
        "runtime_candidate_count": runtime_candidate_count,
        "returned_candidate_count": returned_candidate_count,
        "setup": _setup_summary(setup_context, market_context=setup_market_context),
        "market_data": market_data,
        "rejection_counts": _combined_rejection_counts(
            raw_rejections=raw_rejections,
            replay_details=replay_details,
            runtime_filter_reason_counts=runtime_filter_reason_counts,
        ),
        "ranking_gate": {
            "status_counts": replay_details.get("ranking_policy_status_counts") or {},
            "blocker_counts": ranking_rejections,
            "gate_summary": replay_details.get("ranking_policy_gate_summary") or {},
        },
        "examples": {
            "raw_rejections": single_leg_diagnostics.get("examples") or {},
            "raw_passes": single_leg_diagnostics.get("pass_examples") or [],
            "ranking_blocked": replay_details.get("ranking_policy_blocked_exemplars") or [],
        },
        "evidence": {
            "candidate_builder": runtime.build_settings.candidate_builder_key,
            "build_profile": runtime.build_settings.build_profile,
            "trade_structure": runtime.trade_structure,
            "raw_pass_count": single_leg_diagnostics.get("pass_count") or 0,
            "replay_details": _json_ready({key: value for key, value in replay_details.items() if key != "calendar_decisions_by_expiration"}),
        },
    }


def build_entry_runtime_symbol_candidates_from_market_slice(
    *,
    runtime: EntryRuntime,
    symbol: str,
    base_parameters: CandidateBuildParameters,
    calendar_resolver: Any,
    market_slice: Any,
    benchmark_slices_by_symbol: Mapping[str, SymbolMarketSlice] | None = None,
    per_runtime_limit: int = 6,
) -> dict[str, Any]:
    runtime_parameters = build_runtime_candidate_parameters(
        symbol=symbol,
        base_parameters=base_parameters,
        runtime=runtime,
    )
    candidate_filter = build_runtime_candidate_filter(runtime)
    candidates, setup_context, replay_details = build_candidates_with_details_from_market_slice(
        market_slice=market_slice,
        symbol_args=runtime_parameters,
        calendar_resolver=calendar_resolver,
    )
    all_rows: list[dict[str, Any]] = []
    filter_reason_counts: dict[str, int] = {}
    for candidate in candidates:
        row = _serialize_candidate(
            candidate,
            short_delta_target=runtime_parameters.short_delta_target,
        )
        matched, reasons = match_runtime_candidate(row, runtime)
        if not matched:
            for reason in reasons:
                filter_reason_counts[reason] = filter_reason_counts.get(reason, 0) + 1
            continue
        row["runtime_recipe_refs"] = list(runtime.entry_recipe_refs)
        all_rows.append(row)

    rows = [dict(row) for row in all_rows[: max(int(per_runtime_limit), 1)]]

    diagnostic = build_entry_runtime_symbol_diagnostic(
        runtime=runtime,
        symbol=symbol,
        market_slice=market_slice,
        benchmark_slices_by_symbol=benchmark_slices_by_symbol,
        runtime_parameters=runtime_parameters,
        setup_context=setup_context,
        replay_details=replay_details,
        all_rows=all_rows,
        returned_rows=rows,
        runtime_filter_reason_counts=filter_reason_counts,
    )
    return {
        "symbol": symbol,
        "runtime_parameters": runtime_parameters,
        "candidate_filter": candidate_filter,
        "setup_context": setup_context,
        "replay_details": replay_details,
        "all_rows": all_rows,
        "rows": rows,
        "runtime_filter_reason_counts": dict(sorted(filter_reason_counts.items())),
        "diagnostic": diagnostic,
    }


def build_entry_runtime_candidates_with_diagnostics_from_market_slices(
    *,
    entry_runtimes: list[EntryRuntime],
    base_parameters: CandidateBuildParameters,
    calendar_resolver: Any,
    market_slices_by_symbol: dict[str, Any],
    benchmark_slices_by_symbol: Mapping[str, SymbolMarketSlice] | None = None,
    per_runtime_limit: int = 6,
) -> tuple[dict[tuple[str, str], dict[str, list[dict[str, Any]]]], dict[tuple[str, str], list[dict[str, Any]]]]:
    candidates_by_runtime: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
    diagnostics_by_runtime: dict[tuple[str, str], list[dict[str, Any]]] = {}
    runtimes_by_symbol: dict[str, list[EntryRuntime]] = {}
    for runtime in entry_runtimes:
        for symbol in runtime.symbols:
            runtimes_by_symbol.setdefault(str(symbol).upper(), []).append(runtime)

    for symbol, runtimes in runtimes_by_symbol.items():
        market_slice = market_slices_by_symbol.get(symbol)
        if market_slice is None:
            continue
        for runtime in runtimes:
            result = build_entry_runtime_symbol_candidates_from_market_slice(
                runtime=runtime,
                symbol=symbol,
                base_parameters=base_parameters,
                calendar_resolver=calendar_resolver,
                market_slice=market_slice,
                benchmark_slices_by_symbol=benchmark_slices_by_symbol,
                per_runtime_limit=per_runtime_limit,
            )
            owner_key = runtime_owner_key(runtime)
            diagnostics_by_runtime.setdefault(owner_key, []).append(dict(result.get("diagnostic") or {}))
            rows = list(result.get("rows") or [])
            if not rows:
                continue
            runtime_rows = candidates_by_runtime.setdefault(owner_key, {})
            runtime_rows[symbol] = rows
    return candidates_by_runtime, diagnostics_by_runtime


def build_entry_runtime_candidates_from_market_slices(
    *,
    entry_runtimes: list[EntryRuntime],
    base_parameters: CandidateBuildParameters,
    calendar_resolver: Any,
    market_slices_by_symbol: dict[str, Any],
    benchmark_slices_by_symbol: Mapping[str, SymbolMarketSlice] | None = None,
    per_runtime_limit: int = 6,
) -> dict[tuple[str, str], dict[str, list[dict[str, Any]]]]:
    candidates_by_runtime, _diagnostics_by_runtime = build_entry_runtime_candidates_with_diagnostics_from_market_slices(
        entry_runtimes=entry_runtimes,
        base_parameters=base_parameters,
        calendar_resolver=calendar_resolver,
        market_slices_by_symbol=market_slices_by_symbol,
        benchmark_slices_by_symbol=benchmark_slices_by_symbol,
        per_runtime_limit=per_runtime_limit,
    )
    return candidates_by_runtime


def _build_benchmark_market_slices(
    *,
    symbols: tuple[str, ...],
    base_parameters: CandidateBuildParameters,
    client: AlpacaClient,
    entry_runtimes: list[EntryRuntime],
    market_slices_by_symbol: Mapping[str, SymbolMarketSlice],
) -> dict[str, SymbolMarketSlice]:
    benchmark_slices: dict[str, SymbolMarketSlice] = {}
    for symbol in symbols:
        benchmark_symbol = str(symbol or "").upper().strip()
        if not benchmark_symbol:
            continue
        existing = market_slices_by_symbol.get(benchmark_symbol)
        if existing is not None:
            benchmark_slices[benchmark_symbol] = existing
            continue
        benchmark_parameters = build_symbol_market_slice_parameters(
            symbol=benchmark_symbol,
            base_parameters=base_parameters,
            runtimes=entry_runtimes,
        )
        benchmark_slices[benchmark_symbol] = build_underlying_market_slice(
            symbol=benchmark_symbol,
            parameters=benchmark_parameters,
            client=client,
        )
    return benchmark_slices


def build_entry_runtime_candidates_with_diagnostics(
    *,
    entry_runtimes: list[EntryRuntime],
    base_parameters: CandidateBuildParameters,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    market_slice_provider: MarketSliceProvider | None = None,
    per_runtime_limit: int = 6,
) -> tuple[dict[tuple[str, str], dict[str, list[dict[str, Any]]]], dict[tuple[str, str], list[dict[str, Any]]]]:
    provider = market_slice_provider or AlpacaMarketSliceProvider(
        client=client,
        greeks_provider=greeks_provider,
    )
    runtimes_by_symbol: dict[str, list[EntryRuntime]] = {}
    for runtime in entry_runtimes:
        for symbol in runtime.symbols:
            runtimes_by_symbol.setdefault(str(symbol).upper(), []).append(runtime)

    market_slices_by_symbol: dict[str, Any] = {}
    for symbol, runtimes in runtimes_by_symbol.items():
        market_slice_parameters = build_symbol_market_slice_parameters(
            symbol=symbol,
            base_parameters=base_parameters,
            runtimes=runtimes,
        )
        market_slices_by_symbol[symbol] = provider.get_symbol_market_slice(
            symbol=symbol,
            parameters=market_slice_parameters,
        )
    benchmark_slices_by_symbol = _build_benchmark_market_slices(
        symbols=DEFAULT_MARKET_BENCHMARK_SYMBOLS,
        base_parameters=base_parameters,
        client=client,
        entry_runtimes=entry_runtimes,
        market_slices_by_symbol=market_slices_by_symbol,
    )

    return build_entry_runtime_candidates_with_diagnostics_from_market_slices(
        entry_runtimes=entry_runtimes,
        base_parameters=base_parameters,
        calendar_resolver=calendar_resolver,
        market_slices_by_symbol=market_slices_by_symbol,
        benchmark_slices_by_symbol=benchmark_slices_by_symbol,
        per_runtime_limit=per_runtime_limit,
    )


def build_entry_runtime_candidates(
    *,
    entry_runtimes: list[EntryRuntime],
    base_parameters: CandidateBuildParameters,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    market_slice_provider: MarketSliceProvider | None = None,
    per_runtime_limit: int = 6,
) -> dict[tuple[str, str], dict[str, list[dict[str, Any]]]]:
    candidates_by_runtime, _diagnostics_by_runtime = build_entry_runtime_candidates_with_diagnostics(
        entry_runtimes=entry_runtimes,
        base_parameters=base_parameters,
        client=client,
        calendar_resolver=calendar_resolver,
        greeks_provider=greeks_provider,
        market_slice_provider=market_slice_provider,
        per_runtime_limit=per_runtime_limit,
    )
    return candidates_by_runtime


__all__ = [
    "build_entry_runtime_candidates_with_diagnostics",
    "build_entry_runtime_candidates_with_diagnostics_from_market_slices",
    "build_entry_runtime_candidates",
    "build_entry_runtime_candidates_from_market_slices",
    "build_entry_runtime_symbol_diagnostic",
    "build_entry_runtime_symbol_candidates_from_market_slice",
    "build_runtime_candidate_parameters",
    "build_symbol_market_slice_parameters",
    "runtime_owner_key",
]

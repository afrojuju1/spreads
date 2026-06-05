from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, is_dataclass
from typing import Any

from core.integrations.alpaca.client import AlpacaClient
from core.domain.models import SymbolMarketSlice, UnderlyingSetupContext
from core.services.trading_strategy_runtime import EntryRuntime, StrategyBuildSettings
from core.services.option_structures import candidate_legs, payload_structure_identity
from core.services.runtime_candidate_filters import (
    build_runtime_candidate_filter,
    match_runtime_candidate,
)
from core.services.scanners.config import (
    RANKING_POLICY_ARG_KEYS,
    clone_args,
    resolve_profile_value,
    resolve_symbol_scan_args,
)
from core.services.scanners.runtime import (
    build_candidates_with_details_from_market_slice,
    build_symbol_market_slice,
    persist_scan_run,
)
from core.services.scanners.builders.single_legs import diagnose_single_leg_rejections
from core.storage.run_history_repository import RunHistoryRepository


def runtime_owner_key(runtime: EntryRuntime) -> tuple[str, str]:
    return runtime.trading_strategy_id, "entry"


def _apply_build_settings(
    args: argparse.Namespace,
    settings: StrategyBuildSettings,
) -> argparse.Namespace:
    args.strategy = settings.scanner_strategy
    args.profile = settings.scanner_profile
    args.min_dte = settings.dte_min
    args.max_dte = settings.dte_max
    args.short_delta_min = settings.short_delta_min
    args.short_delta_max = settings.short_delta_max
    args.short_delta_target = resolve_profile_value(
        settings.short_delta_target,
        getattr(args, "short_delta_target", None),
    )
    if (
        args.short_delta_target is None
        and settings.short_delta_min is not None
        and settings.short_delta_max is not None
        and settings.short_delta_min <= settings.short_delta_max
    ):
        args.short_delta_target = (float(settings.short_delta_min) + float(settings.short_delta_max)) / 2.0
    if settings.width_points:
        args.min_width = min(settings.width_points)
        args.max_width = max(settings.width_points)
    args.min_open_interest = resolve_profile_value(settings.min_open_interest, getattr(args, "min_open_interest", None))
    args.max_relative_spread = resolve_profile_value(settings.max_leg_spread_pct_mid, getattr(args, "max_relative_spread", None))
    args.min_return_on_risk = resolve_profile_value(settings.min_return_on_risk, getattr(args, "min_return_on_risk", None))
    args.min_fill_ratio = resolve_profile_value(settings.min_fill_ratio, getattr(args, "min_fill_ratio", None))
    args.min_short_vs_expected_move_ratio = resolve_profile_value(
        settings.min_short_vs_expected_move_ratio,
        getattr(args, "min_short_vs_expected_move_ratio", None),
    )
    args.min_breakeven_vs_expected_move_ratio = resolve_profile_value(
        settings.min_breakeven_vs_expected_move_ratio,
        getattr(args, "min_breakeven_vs_expected_move_ratio", None),
    )
    for key in RANKING_POLICY_ARG_KEYS:
        setattr(
            args,
            key,
            resolve_profile_value(
                settings.ranking_policy.get(key),
                getattr(args, key, None),
            ),
        )
    return args


def build_runtime_scan_args(
    *,
    symbol: str,
    base_scanner_args: argparse.Namespace,
    runtime: EntryRuntime,
) -> argparse.Namespace:
    raw_args = clone_args(base_scanner_args)
    raw_args.symbol = symbol
    raw_args.symbols = symbol
    raw_args.symbols_file = None
    raw_args.universe = None
    raw_args.per_symbol_top = max(int(getattr(raw_args, "per_symbol_top", 1) or 1), 1)
    raw_args.top = max(int(getattr(raw_args, "top", 10) or 10), raw_args.per_symbol_top)
    configured_args = _apply_build_settings(raw_args, runtime.build_settings)
    symbol_args, _underlying_type = resolve_symbol_scan_args(
        symbol=symbol,
        base_args=configured_args,
    )
    return symbol_args


def build_market_slice_args(
    *,
    symbol: str,
    base_scanner_args: argparse.Namespace,
    runtimes: list[EntryRuntime],
) -> argparse.Namespace:
    raw_args = clone_args(base_scanner_args)
    raw_args.symbol = symbol
    raw_args.symbols = symbol
    raw_args.symbols_file = None
    raw_args.universe = None
    dte_mins = [int(runtime.build_settings.dte_min) for runtime in runtimes if runtime.build_settings.dte_min is not None]
    dte_maxes = [int(runtime.build_settings.dte_max) for runtime in runtimes if runtime.build_settings.dte_max is not None]
    raw_args.min_dte = min(dte_mins) if dte_mins else int(getattr(raw_args, "min_dte", 0) or 0)
    raw_args.max_dte = max(dte_maxes) if dte_maxes else int(getattr(raw_args, "max_dte", 30) or 30)
    return raw_args


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


def _setup_summary(setup_context: UnderlyingSetupContext | None) -> dict[str, Any]:
    if setup_context is None:
        return {}
    payload = asdict(setup_context)
    return {
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
    runtime_args: argparse.Namespace,
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
        strategy=runtime.build_settings.scanner_strategy,
        spot_price=market_slice.spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=runtime_args,
    )


def build_entry_runtime_symbol_diagnostic(
    *,
    runtime: EntryRuntime,
    symbol: str,
    market_slice: SymbolMarketSlice,
    runtime_args: argparse.Namespace,
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
        runtime_args=runtime_args,
    )
    raw_rejections = dict(single_leg_diagnostics.get("reject_counts") or {})
    ranking_rejections = dict(replay_details.get("ranking_policy_blocker_counts") or {})
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
            "min_dte": getattr(runtime_args, "min_dte", None),
            "max_dte": getattr(runtime_args, "max_dte", None),
            "delta_min": getattr(runtime_args, "short_delta_min", None),
            "delta_max": getattr(runtime_args, "short_delta_max", None),
            "min_open_interest": getattr(runtime_args, "min_open_interest", None),
            "max_relative_spread": getattr(runtime_args, "max_relative_spread", None),
            "min_return_on_risk": getattr(runtime_args, "min_return_on_risk", None),
            "min_credit": getattr(runtime_args, "min_credit", None),
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
        "setup": _setup_summary(setup_context),
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
            "scanner_strategy": runtime.build_settings.scanner_strategy,
            "scanner_profile": runtime.build_settings.scanner_profile,
            "trade_structure": runtime.trade_structure,
            "raw_pass_count": single_leg_diagnostics.get("pass_count") or 0,
            "replay_details": _json_ready({key: value for key, value in replay_details.items() if key != "calendar_decisions_by_expiration"}),
        },
    }


def build_entry_runtime_symbol_candidates_from_market_slice(
    *,
    runtime: EntryRuntime,
    symbol: str,
    base_scanner_args: argparse.Namespace,
    calendar_resolver: Any,
    market_slice: Any,
    per_runtime_limit: int = 6,
    history_store: RunHistoryRepository | None = None,
    session_label: str | None = None,
) -> dict[str, Any]:
    runtime_args = build_runtime_scan_args(
        symbol=symbol,
        base_scanner_args=base_scanner_args,
        runtime=runtime,
    )
    candidate_filter = build_runtime_candidate_filter(runtime)
    candidates, setup_context, replay_details = build_candidates_with_details_from_market_slice(
        market_slice=market_slice,
        symbol_args=runtime_args,
        calendar_resolver=calendar_resolver,
    )
    matched_candidates: list[Any] = []
    all_rows: list[dict[str, Any]] = []
    filter_reason_counts: dict[str, int] = {}
    for candidate in candidates:
        row = _serialize_candidate(
            candidate,
            short_delta_target=getattr(runtime_args, "short_delta_target", None),
        )
        matched, reasons = match_runtime_candidate(row, runtime)
        if not matched:
            for reason in reasons:
                filter_reason_counts[reason] = filter_reason_counts.get(reason, 0) + 1
            continue
        row["runtime_recipe_refs"] = list(runtime.entry_recipe_refs)
        matched_candidates.append(candidate)
        all_rows.append(row)

    run_id: str | None = None
    if history_store is not None and matched_candidates:
        run_id = persist_scan_run(
            history_store=history_store,
            symbol_args=runtime_args,
            market_slice=market_slice,
            setup_context=setup_context,
            candidates=matched_candidates,
            candidate_filter=candidate_filter,
            calendar_decisions_by_expiration=replay_details.get("calendar_decisions_by_expiration"),
            session_label=session_label,
        )

    rows = [dict(row) for row in all_rows[: max(int(per_runtime_limit), 1)]]
    if run_id is not None:
        for row in rows:
            row["run_id"] = run_id

    diagnostic = build_entry_runtime_symbol_diagnostic(
        runtime=runtime,
        symbol=symbol,
        market_slice=market_slice,
        runtime_args=runtime_args,
        setup_context=setup_context,
        replay_details=replay_details,
        all_rows=all_rows,
        returned_rows=rows,
        runtime_filter_reason_counts=filter_reason_counts,
    )
    return {
        "symbol": symbol,
        "runtime_args": runtime_args,
        "candidate_filter": candidate_filter,
        "setup_context": setup_context,
        "replay_details": replay_details,
        "all_rows": all_rows,
        "rows": rows,
        "run_id": run_id,
        "runtime_filter_reason_counts": dict(sorted(filter_reason_counts.items())),
        "diagnostic": diagnostic,
    }


def build_entry_runtime_candidates_with_diagnostics_from_market_slices(
    *,
    entry_runtimes: list[EntryRuntime],
    base_scanner_args: argparse.Namespace,
    calendar_resolver: Any,
    market_slices_by_symbol: dict[str, Any],
    per_runtime_limit: int = 6,
    history_store: RunHistoryRepository | None = None,
    session_label: str | None = None,
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
                base_scanner_args=base_scanner_args,
                calendar_resolver=calendar_resolver,
                market_slice=market_slice,
                per_runtime_limit=per_runtime_limit,
                history_store=history_store,
                session_label=session_label,
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
    base_scanner_args: argparse.Namespace,
    calendar_resolver: Any,
    market_slices_by_symbol: dict[str, Any],
    per_runtime_limit: int = 6,
    history_store: RunHistoryRepository | None = None,
    session_label: str | None = None,
) -> dict[tuple[str, str], dict[str, list[dict[str, Any]]]]:
    candidates_by_runtime, _diagnostics_by_runtime = build_entry_runtime_candidates_with_diagnostics_from_market_slices(
        entry_runtimes=entry_runtimes,
        base_scanner_args=base_scanner_args,
        calendar_resolver=calendar_resolver,
        market_slices_by_symbol=market_slices_by_symbol,
        per_runtime_limit=per_runtime_limit,
        history_store=history_store,
        session_label=session_label,
    )
    return candidates_by_runtime


def build_entry_runtime_candidates_with_diagnostics(
    *,
    entry_runtimes: list[EntryRuntime],
    base_scanner_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    per_runtime_limit: int = 6,
    history_store: RunHistoryRepository | None = None,
    session_label: str | None = None,
) -> tuple[dict[tuple[str, str], dict[str, list[dict[str, Any]]]], dict[tuple[str, str], list[dict[str, Any]]]]:
    runtimes_by_symbol: dict[str, list[EntryRuntime]] = {}
    for runtime in entry_runtimes:
        for symbol in runtime.symbols:
            runtimes_by_symbol.setdefault(str(symbol).upper(), []).append(runtime)

    market_slices_by_symbol: dict[str, Any] = {}
    for symbol, runtimes in runtimes_by_symbol.items():
        market_slice_args = build_market_slice_args(
            symbol=symbol,
            base_scanner_args=base_scanner_args,
            runtimes=runtimes,
        )
        market_slices_by_symbol[symbol] = build_symbol_market_slice(
            symbol=symbol,
            symbol_args=market_slice_args,
            client=client,
            greeks_provider=greeks_provider,
        )

    return build_entry_runtime_candidates_with_diagnostics_from_market_slices(
        entry_runtimes=entry_runtimes,
        base_scanner_args=base_scanner_args,
        calendar_resolver=calendar_resolver,
        market_slices_by_symbol=market_slices_by_symbol,
        per_runtime_limit=per_runtime_limit,
        history_store=history_store,
        session_label=session_label,
    )


def build_entry_runtime_candidates(
    *,
    entry_runtimes: list[EntryRuntime],
    base_scanner_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    per_runtime_limit: int = 6,
    history_store: RunHistoryRepository | None = None,
    session_label: str | None = None,
) -> dict[tuple[str, str], dict[str, list[dict[str, Any]]]]:
    candidates_by_runtime, _diagnostics_by_runtime = build_entry_runtime_candidates_with_diagnostics(
        entry_runtimes=entry_runtimes,
        base_scanner_args=base_scanner_args,
        client=client,
        calendar_resolver=calendar_resolver,
        greeks_provider=greeks_provider,
        per_runtime_limit=per_runtime_limit,
        history_store=history_store,
        session_label=session_label,
    )
    return candidates_by_runtime


__all__ = [
    "build_entry_runtime_candidates_with_diagnostics",
    "build_entry_runtime_candidates_with_diagnostics_from_market_slices",
    "build_entry_runtime_candidates",
    "build_entry_runtime_candidates_from_market_slices",
    "build_entry_runtime_symbol_diagnostic",
    "build_entry_runtime_symbol_candidates_from_market_slice",
    "build_market_slice_args",
    "build_runtime_scan_args",
    "runtime_owner_key",
]

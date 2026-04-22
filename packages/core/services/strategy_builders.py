from __future__ import annotations

import argparse
from dataclasses import asdict
from typing import Any

from core.integrations.alpaca.client import AlpacaClient
from core.services.automation_runtime import EntryRuntime, StrategyBuildSettings
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
from core.storage.run_history_repository import RunHistoryRepository


def runtime_owner_key(runtime: EntryRuntime) -> tuple[str, str]:
    return runtime.bot_id, runtime.automation_id


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
        args.short_delta_target = (
            float(settings.short_delta_min) + float(settings.short_delta_max)
        ) / 2.0
    if settings.width_points:
        args.min_width = min(settings.width_points)
        args.max_width = max(settings.width_points)
    args.min_open_interest = resolve_profile_value(
        settings.min_open_interest, getattr(args, "min_open_interest", None)
    )
    args.max_relative_spread = resolve_profile_value(
        settings.max_leg_spread_pct_mid, getattr(args, "max_relative_spread", None)
    )
    args.min_return_on_risk = resolve_profile_value(
        settings.min_return_on_risk, getattr(args, "min_return_on_risk", None)
    )
    args.min_fill_ratio = resolve_profile_value(
        settings.min_fill_ratio, getattr(args, "min_fill_ratio", None)
    )
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
    dte_mins = [
        int(runtime.build_settings.dte_min)
        for runtime in runtimes
        if runtime.build_settings.dte_min is not None
    ]
    dte_maxes = [
        int(runtime.build_settings.dte_max)
        for runtime in runtimes
        if runtime.build_settings.dte_max is not None
    ]
    raw_args.min_dte = (
        min(dte_mins) if dte_mins else int(getattr(raw_args, "min_dte", 0) or 0)
    )
    raw_args.max_dte = (
        max(dte_maxes) if dte_maxes else int(getattr(raw_args, "max_dte", 30) or 30)
    )
    return raw_args


def _serialize_candidate(
    candidate: Any,
    *,
    short_delta_target: float | None = None,
) -> dict[str, Any]:
    if hasattr(candidate, "__dataclass_fields__"):
        row = (
            dict(candidate.to_payload())
            if hasattr(candidate, "to_payload")
            else dict(asdict(candidate))
        )
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
            calendar_decisions_by_expiration=replay_details.get(
                "calendar_decisions_by_expiration"
            ),
            session_label=session_label,
        )

    rows = [dict(row) for row in all_rows[: max(int(per_runtime_limit), 1)]]
    if run_id is not None:
        for row in rows:
            row["run_id"] = run_id

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
    }


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
    candidates_by_runtime: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
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
            rows = list(result.get("rows") or [])
            if not rows:
                continue
            runtime_rows = candidates_by_runtime.setdefault(owner_key, {})
            runtime_rows[symbol] = rows
    return candidates_by_runtime


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

    return build_entry_runtime_candidates_from_market_slices(
        entry_runtimes=entry_runtimes,
        base_scanner_args=base_scanner_args,
        calendar_resolver=calendar_resolver,
        market_slices_by_symbol=market_slices_by_symbol,
        per_runtime_limit=per_runtime_limit,
        history_store=history_store,
        session_label=session_label,
    )


__all__ = [
    "build_entry_runtime_candidates",
    "build_entry_runtime_candidates_from_market_slices",
    "build_entry_runtime_symbol_candidates_from_market_slice",
    "build_market_slice_args",
    "build_runtime_scan_args",
    "runtime_owner_key",
]

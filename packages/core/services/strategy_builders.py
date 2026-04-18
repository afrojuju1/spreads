from __future__ import annotations

import argparse
from dataclasses import asdict
from typing import Any

from core.integrations.alpaca.client import AlpacaClient
from core.services.automation_runtime import EntryRuntime, StrategyBuildSettings
from core.services.scanners.config import (
    clone_args,
    resolve_profile_value,
    resolve_symbol_scan_args,
)
from core.services.scanners.runtime import (
    build_candidates_from_market_slice,
    persist_scan_run,
    build_symbol_market_slice,
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
    if (
        settings.short_delta_min is not None
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


def _matches_build_settings(
    candidate: dict[str, Any], settings: StrategyBuildSettings
) -> bool:
    if settings.width_points:
        width = candidate.get("width")
        if width in (None, ""):
            return False
        normalized_width = round(float(width), 4)
        allowed_widths = {round(float(value), 4) for value in settings.width_points}
        if normalized_width not in allowed_widths:
            return False
    return True


def _serialize_candidate(candidate: Any) -> dict[str, Any]:
    if hasattr(candidate, "__dataclass_fields__"):
        return dict(asdict(candidate))
    if isinstance(candidate, dict):
        return dict(candidate)
    raise TypeError("Unsupported candidate payload for runtime strategy builder")


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
    candidates_by_runtime: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
    runtimes_by_symbol: dict[str, list[EntryRuntime]] = {}
    for runtime in entry_runtimes:
        for symbol in runtime.symbols:
            runtimes_by_symbol.setdefault(str(symbol).upper(), []).append(runtime)

    for symbol, runtimes in runtimes_by_symbol.items():
        market_slice_args = build_market_slice_args(
            symbol=symbol,
            base_scanner_args=base_scanner_args,
            runtimes=runtimes,
        )
        market_slice = build_symbol_market_slice(
            symbol=symbol,
            symbol_args=market_slice_args,
            client=client,
            greeks_provider=greeks_provider,
        )
        for runtime in runtimes:
            runtime_args = build_runtime_scan_args(
                symbol=symbol,
                base_scanner_args=base_scanner_args,
                runtime=runtime,
            )
            candidates, setup_context = build_candidates_from_market_slice(
                market_slice=market_slice,
                symbol_args=runtime_args,
                calendar_resolver=calendar_resolver,
            )
            owner_key = runtime_owner_key(runtime)
            matched_candidates: list[Any] = []
            rows: list[dict[str, Any]] = []
            for candidate in candidates:
                row = _serialize_candidate(candidate)
                if not _matches_build_settings(row, runtime.build_settings):
                    continue
                matched_candidates.append(candidate)
                rows.append(row)
            if not rows:
                continue
            run_id: str | None = None
            if history_store is not None:
                run_id = persist_scan_run(
                    history_store=history_store,
                    symbol_args=runtime_args,
                    market_slice=market_slice,
                    setup_context=setup_context,
                    candidates=matched_candidates,
                    session_label=session_label,
                )
            limited_rows = [dict(row) for row in rows[: max(int(per_runtime_limit), 1)]]
            if run_id is not None:
                for row in limited_rows:
                    row["run_id"] = run_id
            runtime_rows = candidates_by_runtime.setdefault(owner_key, {})
            runtime_rows[symbol] = limited_rows
    return candidates_by_runtime


__all__ = [
    "build_entry_runtime_candidates",
    "build_market_slice_args",
    "build_runtime_scan_args",
    "runtime_owner_key",
]

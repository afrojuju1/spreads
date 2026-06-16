from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from core.services.candidate_policy import resolve_strategy_min_return_on_risk
from core.services.option_structures import normalize_strategy_family
from core.services.strategy_candidate_builders.settings import RANKING_POLICY_ARG_KEYS
from core.services.trading_strategies import (
    load_active_trading_strategies,
    resolve_active_trading_strategy,
    resolve_trading_strategy,
)
from core.services.trading_strategy_runtime_models import (
    EntryRuntime,
    ManagementRuntime,
    StrategyBuildSettings,
    TradingStrategyConfig,
)
from core.value_coercion import as_text as _as_text


def build_strategy_build_settings(strategy: TradingStrategyConfig) -> StrategyBuildSettings:
    builder_params = dict(strategy.builder_params)
    liquidity_rules = dict(strategy.liquidity_rules)
    width_points = builder_params.get("width_points")
    ranking_policy = {key: builder_params[key] for key in RANKING_POLICY_ARG_KEYS if builder_params.get(key) is not None}
    return StrategyBuildSettings(
        trading_strategy_id=strategy.trading_strategy_id,
        trade_structure=strategy.trade_structure,
        trade_structure_spec=strategy.trade_structure_spec,
        build=strategy.build,
        liquidity=strategy.liquidity,
        risk=strategy.position_sizing,
        candidate_builder_key=strategy.candidate_builder_key,
        build_profile=strategy.build_profile,
        dte_min=builder_params.get("dte_min"),
        dte_max=builder_params.get("dte_max"),
        short_delta_min=builder_params.get("short_delta_min"),
        short_delta_max=builder_params.get("short_delta_max"),
        short_delta_target=builder_params.get("short_delta_target"),
        width_points=tuple(float(value) for value in width_points) if isinstance(width_points, list) else (),
        min_open_interest=liquidity_rules.get("min_open_interest"),
        max_leg_spread_pct_mid=liquidity_rules.get("max_leg_spread_pct_mid"),
        min_return_on_risk=resolve_strategy_min_return_on_risk(
            strategy.build_profile,
            risk_defaults=strategy.risk_defaults,
        ),
        min_fill_ratio=builder_params.get("min_fill_ratio"),
        min_short_vs_expected_move_ratio=builder_params.get("min_short_vs_expected_move_ratio"),
        min_breakeven_vs_expected_move_ratio=builder_params.get("min_breakeven_vs_expected_move_ratio"),
        max_quote_age_seconds=liquidity_rules.get("max_quote_age_seconds"),
        ranking_policy=ranking_policy,
        builder_params=builder_params,
        liquidity_rules=liquidity_rules,
        risk_defaults=dict(strategy.risk_defaults),
    )


def build_entry_runtime(strategy: TradingStrategyConfig) -> EntryRuntime:
    return EntryRuntime(
        strategy=strategy,
        build_settings=build_strategy_build_settings(strategy),
        config_hash=strategy.config_hash,
    )


def build_management_runtime(strategy: TradingStrategyConfig) -> ManagementRuntime:
    return ManagementRuntime(
        strategy=strategy,
        config_hash=strategy.config_hash,
    )


def resolve_entry_runtime(
    *,
    trading_strategy_id: str,
    config_root: str | Path | None = None,
) -> EntryRuntime:
    strategy = resolve_active_trading_strategy(
        trading_strategy_id,
        config_root=config_root,
    )
    if strategy.entry is None or not strategy.entry.enabled:
        raise ValueError(f"Trading strategy {trading_strategy_id} has no active entry routine")
    return build_entry_runtime(strategy)


def resolve_entry_observation_runtime(
    *,
    trading_strategy_id: str,
    config_root: str | Path | None = None,
) -> EntryRuntime:
    strategy = resolve_trading_strategy(
        trading_strategy_id,
        config_root=config_root,
    )
    if strategy.entry is None or not strategy.entry.enabled:
        raise ValueError(f"Trading strategy {trading_strategy_id} has no authored entry routine")
    return build_entry_runtime(strategy)


def resolve_management_runtime(
    *,
    trading_strategy_id: str,
    config_root: str | Path | None = None,
) -> ManagementRuntime:
    strategy = resolve_active_trading_strategy(
        trading_strategy_id,
        config_root=config_root,
    )
    if strategy.management is None or not strategy.management.enabled:
        raise ValueError(f"Trading strategy {trading_strategy_id} has no active management routine")
    return build_management_runtime(strategy)


def resolve_entry_runtimes(
    config_root: str | Path | None = None,
) -> list[EntryRuntime]:
    runtimes: list[EntryRuntime] = []
    for strategy in load_active_trading_strategies(config_root).values():
        if strategy.entry is not None and strategy.entry.enabled:
            runtimes.append(build_entry_runtime(strategy))
    return runtimes


def resolve_management_runtimes(
    config_root: str | Path | None = None,
) -> list[ManagementRuntime]:
    runtimes: list[ManagementRuntime] = []
    for strategy in load_active_trading_strategies(config_root).values():
        if strategy.management is not None and strategy.management.enabled:
            runtimes.append(build_management_runtime(strategy))
    return runtimes


def find_management_runtime_for_position(
    position: Mapping[str, Any],
    *,
    runtimes: tuple[ManagementRuntime, ...] | None = None,
) -> tuple[ManagementRuntime | None, str | None]:
    owner_strategy_id = _as_text(position.get("trading_strategy_id"))
    trade_structure = normalize_strategy_family(position.get("trade_structure") or position.get("strategy_family") or position.get("strategy"))
    underlying_symbol = _as_text(position.get("underlying_symbol") or position.get("root_symbol"))
    normalized_symbol = None if underlying_symbol is None else underlying_symbol.upper()
    if owner_strategy_id is None:
        return None, "missing_management_owner"
    available_runtimes = tuple(resolve_management_runtimes()) if runtimes is None else tuple(runtimes)

    fallback_matches: list[ManagementRuntime] = []
    exact_symbol_matches: list[ManagementRuntime] = []
    for runtime in available_runtimes:
        if runtime.trading_strategy_id != owner_strategy_id:
            continue
        if runtime.trade_structure != trade_structure:
            continue
        fallback_matches.append(runtime)
        runtime_symbols = {symbol.upper() for symbol in runtime.symbols}
        if not runtime_symbols or normalized_symbol is None:
            exact_symbol_matches.append(runtime)
            continue
        if normalized_symbol in runtime_symbols:
            exact_symbol_matches.append(runtime)

    effective_matches = exact_symbol_matches or fallback_matches
    if not effective_matches:
        return None, "no_management_runtime"
    if len(effective_matches) > 1:
        return None, "ambiguous_management_runtime"
    return effective_matches[0], None


__all__ = [
    "build_entry_runtime",
    "build_management_runtime",
    "build_strategy_build_settings",
    "find_management_runtime_for_position",
    "resolve_entry_observation_runtime",
    "resolve_entry_runtime",
    "resolve_entry_runtimes",
    "resolve_management_runtime",
    "resolve_management_runtimes",
]

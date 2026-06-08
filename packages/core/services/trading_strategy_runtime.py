from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.services.candidate_policy import resolve_strategy_min_return_on_risk
from core.services.option_structures import normalize_strategy_family
from core.services.strategy_specs import StrategySpec
from core.services.strategy_candidate_builders.settings import RANKING_POLICY_ARG_KEYS
from core.services.trading_strategies import (
    TradingStrategyConfig,
    load_active_trading_strategies,
    resolve_active_trading_strategy,
)
from core.services.trading_strategy_models import (
    StrategyBuildConfig,
    StrategyLiquidityRules,
    StrategyRiskDefaults,
)
from core.value_coercion import as_text as _as_text


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _float_tuple(values: Any) -> tuple[float, ...]:
    if not isinstance(values, list):
        return ()
    return tuple(float(value) for value in values if value not in (None, ""))


@dataclass(frozen=True)
class StrategyBuildSettings:
    trading_strategy_id: str
    trade_structure: str
    strategy_spec: StrategySpec
    build: StrategyBuildConfig
    liquidity: StrategyLiquidityRules
    risk: StrategyRiskDefaults
    scanner_strategy: str
    scanner_profile: str
    dte_min: int | None
    dte_max: int | None
    short_delta_min: float | None
    short_delta_max: float | None
    short_delta_target: float | None
    width_points: tuple[float, ...]
    min_open_interest: int | None
    max_leg_spread_pct_mid: float | None
    min_return_on_risk: float | None
    min_fill_ratio: float | None
    min_short_vs_expected_move_ratio: float | None
    min_breakeven_vs_expected_move_ratio: float | None
    max_quote_age_seconds: int | None
    ranking_policy: dict[str, Any]
    builder_params: dict[str, Any]
    liquidity_rules: dict[str, Any]
    risk_defaults: dict[str, Any]


@dataclass(frozen=True)
class EntryRuntime:
    strategy: TradingStrategyConfig
    build_settings: StrategyBuildSettings
    config_hash: str

    @property
    def trading_strategy_id(self) -> str:
        return self.strategy.trading_strategy_id

    @property
    def trade_structure(self) -> str:
        return self.strategy.trade_structure

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.strategy.symbols

    @property
    def source_ref(self) -> str:
        return self.strategy.source.ref

    @property
    def entry_recipe_refs(self) -> tuple[str, ...]:
        return self.strategy.entry_recipe_refs

    @property
    def trigger_policy(self) -> dict[str, Any]:
        if self.strategy.entry is None:
            return {}
        return dict(self.strategy.entry.trigger_policy)


@dataclass(frozen=True)
class ManagementRuntime:
    strategy: TradingStrategyConfig
    config_hash: str

    @property
    def trading_strategy_id(self) -> str:
        return self.strategy.trading_strategy_id

    @property
    def trade_structure(self) -> str:
        return self.strategy.trade_structure

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.strategy.symbols

    @property
    def source_ref(self) -> str:
        return self.strategy.source.ref

    @property
    def management_recipe_refs(self) -> tuple[str, ...]:
        return self.strategy.management_recipe_refs

    @property
    def trigger_policy(self) -> dict[str, Any]:
        if self.strategy.management is None:
            return {}
        return dict(self.strategy.management.trigger_policy)


def build_strategy_build_settings(strategy: TradingStrategyConfig) -> StrategyBuildSettings:
    builder_params = dict(strategy.builder_params)
    liquidity_rules = dict(strategy.liquidity_rules)
    ranking_policy = {key: builder_params[key] for key in RANKING_POLICY_ARG_KEYS if builder_params.get(key) is not None}
    return StrategyBuildSettings(
        trading_strategy_id=strategy.trading_strategy_id,
        trade_structure=strategy.trade_structure,
        strategy_spec=strategy.strategy_spec,
        build=strategy.build,
        liquidity=strategy.liquidity,
        risk=strategy.position_sizing,
        scanner_strategy=strategy.scanner_strategy,
        scanner_profile=strategy.scanner_profile,
        dte_min=_optional_int(builder_params.get("dte_min")),
        dte_max=_optional_int(builder_params.get("dte_max")),
        short_delta_min=_optional_float(builder_params.get("short_delta_min")),
        short_delta_max=_optional_float(builder_params.get("short_delta_max")),
        short_delta_target=_optional_float(builder_params.get("short_delta_target")),
        width_points=_float_tuple(builder_params.get("width_points")),
        min_open_interest=_optional_int(liquidity_rules.get("min_open_interest")),
        max_leg_spread_pct_mid=_optional_float(liquidity_rules.get("max_leg_spread_pct_mid")),
        min_return_on_risk=resolve_strategy_min_return_on_risk(
            strategy.scanner_profile,
            risk_defaults=strategy.risk_defaults,
        ),
        min_fill_ratio=_optional_float(builder_params.get("min_fill_ratio")),
        min_short_vs_expected_move_ratio=_optional_float(builder_params.get("min_short_vs_expected_move_ratio")),
        min_breakeven_vs_expected_move_ratio=_optional_float(builder_params.get("min_breakeven_vs_expected_move_ratio")),
        max_quote_age_seconds=_optional_int(liquidity_rules.get("max_quote_age_seconds")),
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
    "EntryRuntime",
    "ManagementRuntime",
    "StrategyBuildSettings",
    "build_entry_runtime",
    "build_management_runtime",
    "build_strategy_build_settings",
    "find_management_runtime_for_position",
    "resolve_entry_runtime",
    "resolve_entry_runtimes",
    "resolve_management_runtime",
    "resolve_management_runtimes",
]

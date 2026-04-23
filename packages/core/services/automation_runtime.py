from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.services.automations import ResolvedAutomation
from core.services.bots import ResolvedBot, load_active_bots
from core.services.candidate_policy import resolve_strategy_min_return_on_risk
from core.services.options_automation_models import (
    StrategyBuildConfig,
    StrategyLiquidityRules,
    StrategyRiskDefaults,
)
from core.services.option_structures import normalize_strategy_family
from core.services.scanners.config import RANKING_POLICY_ARG_KEYS
from core.services.strategy_specs import StrategySpec


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


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


@dataclass(frozen=True)
class StrategyBuildSettings:
    strategy_id: str
    strategy_family: str
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
    bot: ResolvedBot
    automation: ResolvedAutomation
    build_settings: StrategyBuildSettings
    entry_recipe_refs: tuple[str, ...]
    config_hash: str

    @property
    def bot_id(self) -> str:
        return self.bot.bot.bot_id

    @property
    def automation_id(self) -> str:
        return self.automation.automation.automation_id

    @property
    def strategy_config_id(self) -> str:
        return self.automation.strategy_config.strategy_config_id

    @property
    def strategy_id(self) -> str:
        return self.automation.strategy_config.strategy_id

    @property
    def strategy_family(self) -> str:
        return self.automation.strategy_config.strategy_family

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.automation.symbols

    @property
    def trigger_policy(self) -> dict[str, Any]:
        return dict(self.automation.automation.trigger_policy)


@dataclass(frozen=True)
class ManagementRuntime:
    bot: ResolvedBot
    automation: ResolvedAutomation
    management_recipe_refs: tuple[str, ...]
    config_hash: str

    @property
    def bot_id(self) -> str:
        return self.bot.bot.bot_id

    @property
    def automation_id(self) -> str:
        return self.automation.automation.automation_id

    @property
    def strategy_config_id(self) -> str:
        return self.automation.strategy_config.strategy_config_id

    @property
    def strategy_id(self) -> str:
        return self.automation.strategy_config.strategy_id

    @property
    def strategy_family(self) -> str:
        return self.automation.strategy_config.strategy_family

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.automation.symbols

    @property
    def trigger_policy(self) -> dict[str, Any]:
        return dict(self.automation.automation.trigger_policy)


def build_strategy_build_settings(runtime: ResolvedAutomation) -> StrategyBuildSettings:
    strategy_config = runtime.strategy_config
    builder_params = dict(strategy_config.builder_params)
    liquidity_rules = dict(strategy_config.liquidity_rules)
    ranking_policy = {
        key: builder_params[key]
        for key in RANKING_POLICY_ARG_KEYS
        if builder_params.get(key) is not None
    }
    return StrategyBuildSettings(
        strategy_id=strategy_config.strategy_id,
        strategy_family=strategy_config.strategy_family,
        strategy_spec=strategy_config.strategy_spec,
        build=strategy_config.build,
        liquidity=strategy_config.liquidity,
        risk=strategy_config.risk,
        scanner_strategy=strategy_config.scanner_strategy,
        scanner_profile=strategy_config.scanner_profile,
        dte_min=_optional_int(builder_params.get("dte_min")),
        dte_max=_optional_int(builder_params.get("dte_max")),
        short_delta_min=_optional_float(builder_params.get("short_delta_min")),
        short_delta_max=_optional_float(builder_params.get("short_delta_max")),
        short_delta_target=_optional_float(builder_params.get("short_delta_target")),
        width_points=_float_tuple(builder_params.get("width_points")),
        min_open_interest=_optional_int(liquidity_rules.get("min_open_interest")),
        max_leg_spread_pct_mid=_optional_float(
            liquidity_rules.get("max_leg_spread_pct_mid")
        ),
        min_return_on_risk=resolve_strategy_min_return_on_risk(
            strategy_config.scanner_profile,
            risk_defaults=strategy_config.risk_defaults,
        ),
        min_fill_ratio=_optional_float(builder_params.get("min_fill_ratio")),
        min_short_vs_expected_move_ratio=_optional_float(
            builder_params.get("min_short_vs_expected_move_ratio")
        ),
        min_breakeven_vs_expected_move_ratio=_optional_float(
            builder_params.get("min_breakeven_vs_expected_move_ratio")
        ),
        max_quote_age_seconds=_optional_int(
            liquidity_rules.get("max_quote_age_seconds")
        ),
        ranking_policy=ranking_policy,
        builder_params=builder_params,
        liquidity_rules=liquidity_rules,
        risk_defaults=dict(strategy_config.risk_defaults),
    )


def build_entry_runtime(bot: ResolvedBot, runtime: ResolvedAutomation) -> EntryRuntime:
    return EntryRuntime(
        bot=bot,
        automation=runtime,
        build_settings=build_strategy_build_settings(runtime),
        entry_recipe_refs=tuple(runtime.strategy_config.entry_recipe_refs),
        config_hash=bot.config_hash,
    )


def build_management_runtime(
    bot: ResolvedBot, runtime: ResolvedAutomation
) -> ManagementRuntime:
    return ManagementRuntime(
        bot=bot,
        automation=runtime,
        management_recipe_refs=tuple(runtime.strategy_config.management_recipe_refs),
        config_hash=bot.config_hash,
    )


def _resolved_runtime(
    bot_id: str,
    automation_id: str,
    *,
    config_root: str | Path | None = None,
) -> tuple[ResolvedBot, ResolvedAutomation]:
    bots = load_active_bots(config_root)
    bot = bots.get(bot_id)
    if bot is None:
        raise ValueError(f"Unknown or paused bot_id: {bot_id}")
    runtime = next(
        (
            item
            for item in bot.automations
            if item.automation.automation_id == automation_id
        ),
        None,
    )
    if runtime is None:
        raise ValueError(f"Unknown automation_id for bot {bot_id}: {automation_id}")
    return bot, runtime


def resolve_entry_runtime(
    *,
    bot_id: str,
    automation_id: str,
    config_root: str | Path | None = None,
) -> EntryRuntime:
    bot, runtime = _resolved_runtime(
        bot_id,
        automation_id,
        config_root=config_root,
    )
    if not runtime.automation.is_entry:
        raise ValueError(f"Automation {automation_id} is not an entry automation")
    return build_entry_runtime(bot, runtime)


def resolve_management_runtime(
    *,
    bot_id: str,
    automation_id: str,
    config_root: str | Path | None = None,
) -> ManagementRuntime:
    bot, runtime = _resolved_runtime(
        bot_id,
        automation_id,
        config_root=config_root,
    )
    if not runtime.automation.is_management:
        raise ValueError(f"Automation {automation_id} is not a management automation")
    return build_management_runtime(bot, runtime)


def resolve_entry_runtimes(
    config_root: str | Path | None = None,
) -> list[EntryRuntime]:
    runtimes: list[EntryRuntime] = []
    for bot in load_active_bots(config_root).values():
        for runtime in bot.automations:
            if runtime.automation.is_entry:
                runtimes.append(build_entry_runtime(bot, runtime))
    return runtimes


def resolve_management_runtimes(
    config_root: str | Path | None = None,
) -> list[ManagementRuntime]:
    runtimes: list[ManagementRuntime] = []
    for bot in load_active_bots(config_root).values():
        for runtime in bot.automations:
            if runtime.automation.is_management:
                runtimes.append(build_management_runtime(bot, runtime))
    return runtimes


def find_management_runtime_for_position(
    position: Mapping[str, Any],
    *,
    runtimes: tuple[ManagementRuntime, ...] | None = None,
) -> tuple[ManagementRuntime | None, str | None]:
    owner_bot_id = _as_text(position.get("bot_id"))
    strategy_config_id = _as_text(position.get("strategy_config_id"))
    if owner_bot_id is None or strategy_config_id is None:
        return None, "missing_management_owner"

    owner_strategy_family = normalize_strategy_family(position.get("strategy_family"))
    underlying_symbol = _as_text(
        position.get("underlying_symbol") or position.get("root_symbol")
    )
    normalized_symbol = None if underlying_symbol is None else underlying_symbol.upper()
    available_runtimes = (
        tuple(resolve_management_runtimes())
        if runtimes is None
        else tuple(runtimes)
    )

    fallback_matches: list[ManagementRuntime] = []
    exact_symbol_matches: list[ManagementRuntime] = []
    for runtime in available_runtimes:
        if runtime.bot_id != owner_bot_id:
            continue
        if runtime.strategy_config_id != strategy_config_id:
            continue
        if runtime.strategy_family != owner_strategy_family:
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

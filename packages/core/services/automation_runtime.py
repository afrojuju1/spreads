from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.services.automations import ResolvedAutomation
from core.services.bots import ResolvedBot, load_active_bots


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
    strategy_id: str
    strategy_family: str
    scanner_strategy: str
    scanner_profile: str
    dte_min: int | None
    dte_max: int | None
    short_delta_min: float | None
    short_delta_max: float | None
    width_points: tuple[float, ...]
    min_open_interest: int | None
    max_leg_spread_pct_mid: float | None
    max_quote_age_seconds: int | None
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
    return StrategyBuildSettings(
        strategy_id=strategy_config.strategy_id,
        strategy_family=strategy_config.strategy_family,
        scanner_strategy=strategy_config.scanner_strategy,
        scanner_profile=strategy_config.scanner_profile,
        dte_min=_optional_int(builder_params.get("dte_min")),
        dte_max=_optional_int(builder_params.get("dte_max")),
        short_delta_min=_optional_float(builder_params.get("short_delta_min")),
        short_delta_max=_optional_float(builder_params.get("short_delta_max")),
        width_points=_float_tuple(builder_params.get("width_points")),
        min_open_interest=_optional_int(liquidity_rules.get("min_open_interest")),
        max_leg_spread_pct_mid=_optional_float(
            liquidity_rules.get("max_leg_spread_pct_mid")
        ),
        max_quote_age_seconds=_optional_int(
            liquidity_rules.get("max_quote_age_seconds")
        ),
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
    bot_id: str, automation_id: str
) -> tuple[ResolvedBot, ResolvedAutomation]:
    bots = load_active_bots()
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


def resolve_entry_runtime(*, bot_id: str, automation_id: str) -> EntryRuntime:
    bot, runtime = _resolved_runtime(bot_id, automation_id)
    if not runtime.automation.is_entry:
        raise ValueError(f"Automation {automation_id} is not an entry automation")
    return build_entry_runtime(bot, runtime)


def resolve_management_runtime(*, bot_id: str, automation_id: str) -> ManagementRuntime:
    bot, runtime = _resolved_runtime(bot_id, automation_id)
    if not runtime.automation.is_management:
        raise ValueError(f"Automation {automation_id} is not a management automation")
    return build_management_runtime(bot, runtime)


def resolve_entry_runtimes() -> list[EntryRuntime]:
    runtimes: list[EntryRuntime] = []
    for bot in load_active_bots().values():
        for runtime in bot.automations:
            if runtime.automation.is_entry:
                runtimes.append(build_entry_runtime(bot, runtime))
    return runtimes


def resolve_management_runtimes() -> list[ManagementRuntime]:
    runtimes: list[ManagementRuntime] = []
    for bot in load_active_bots().values():
        for runtime in bot.automations:
            if runtime.automation.is_management:
                runtimes.append(build_management_runtime(bot, runtime))
    return runtimes


__all__ = [
    "EntryRuntime",
    "ManagementRuntime",
    "StrategyBuildSettings",
    "build_entry_runtime",
    "build_management_runtime",
    "build_strategy_build_settings",
    "resolve_entry_runtime",
    "resolve_entry_runtimes",
    "resolve_management_runtime",
    "resolve_management_runtimes",
]

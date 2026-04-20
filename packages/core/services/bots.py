from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.services.automations import (
    ResolvedAutomation,
    load_automations,
    resolve_automation,
)
from core.services.candidate_policy import resolve_strategy_min_return_on_risk
from core.services.options_automation_models import BotLimits, BotRuntimePolicy
from core.services.strategy_configs import (
    _as_list,
    _as_text,
    _canonical_hash,
    _load_yaml_file,
    default_config_root,
)

NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class BotConfig:
    bot_id: str
    name: str
    limits: BotLimits
    runtime: BotRuntimePolicy
    automation_ids: tuple[str, ...]
    config_path: Path
    config_hash: str

    @property
    def capital_limit(self) -> float:
        return self.limits.capital_limit

    @property
    def max_open_positions(self) -> int:
        return self.limits.max_open_positions

    @property
    def max_daily_actions(self) -> int:
        return self.limits.max_daily_actions

    @property
    def max_new_entries_per_day(self) -> int | None:
        return self.limits.max_new_entries_per_day

    @property
    def daily_loss_limit(self) -> float | None:
        return self.limits.daily_loss_limit

    @property
    def live_enabled(self) -> bool:
        return self.runtime.live_enabled

    @property
    def cancel_pending_entries_after_et(self) -> str | None:
        return self.runtime.cancel_pending_entries_after_et

    @property
    def flatten_positions_at_et(self) -> str | None:
        return self.runtime.flatten_positions_at_et

    @property
    def paused(self) -> bool:
        return self.runtime.paused


@dataclass(frozen=True)
class ResolvedBot:
    bot: BotConfig
    automations: tuple[ResolvedAutomation, ...]
    config_hash: str


def _bot_payload(bot: BotConfig) -> dict[str, Any]:
    return {
        "bot_id": bot.bot_id,
        "name": bot.name,
        "limits": {
            "capital_limit": bot.limits.capital_limit,
            "max_open_positions": bot.limits.max_open_positions,
            "max_daily_actions": bot.limits.max_daily_actions,
            "max_new_entries_per_day": bot.limits.max_new_entries_per_day,
            "daily_loss_limit": bot.limits.daily_loss_limit,
        },
        "runtime": {
            "live_enabled": bot.runtime.live_enabled,
            "cancel_pending_entries_after_et": bot.runtime.cancel_pending_entries_after_et,
            "flatten_positions_at_et": bot.runtime.flatten_positions_at_et,
            "paused": bot.runtime.paused,
        },
        "automations": list(bot.automation_ids),
    }


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour_text, _, minute_text = str(value).partition(":")
    if not _:
        raise ValueError(f"Invalid HH:MM time: {value}")
    return int(hour_text), int(minute_text)


def bot_time_reached(
    bot: BotConfig,
    *,
    time_value: str | None,
    now: datetime | None = None,
) -> bool:
    if time_value is None:
        return False
    current = (now or datetime.now(NEW_YORK)).astimezone(NEW_YORK)
    hour, minute = _parse_hhmm(time_value)
    return (current.hour, current.minute) >= (hour, minute)


def load_bots(config_root: str | Path | None = None) -> dict[str, BotConfig]:
    root = default_config_root(config_root) / "bots"
    if not root.exists():
        return {}
    bots: dict[str, BotConfig] = {}
    for path in sorted(root.glob("*.yaml")):
        payload = _load_yaml_file(path)
        bot = BotConfig(
            bot_id=_as_text(payload.get("bot_id"), field_name="bot_id"),
            name=_as_text(payload.get("name"), field_name="name"),
            limits=BotLimits.from_payload(payload.get("limits")),
            runtime=BotRuntimePolicy.from_payload(payload.get("runtime")),
            automation_ids=_as_list(payload.get("automations"), field_name="automations"),
            config_path=path,
            config_hash="",
        )
        bot = BotConfig(
            **{
                **bot.__dict__,
                "config_hash": _canonical_hash(_bot_payload(bot)),
            }
        )
        if bot.bot_id in bots:
            raise ValueError(f"Duplicate bot_id {bot.bot_id}")
        bots[bot.bot_id] = bot
    return bots


def resolve_bot(bot_id: str, *, config_root: str | Path | None = None) -> ResolvedBot:
    bots = load_bots(config_root)
    automations = load_automations(config_root)
    bot = bots.get(bot_id)
    if bot is None:
        raise ValueError(f"Unknown bot_id: {bot_id}")
    resolved: list[ResolvedAutomation] = []
    for automation_id in bot.automation_ids:
        if automation_id not in automations:
            raise ValueError(
                f"Bot {bot_id} references unknown automation_id {automation_id}"
            )
        resolved.append(resolve_automation(automation_id, config_root=config_root))
    combined_hash = _canonical_hash(
        {
            "bot": bot.config_hash,
            "automations": [item.automation.config_hash for item in resolved],
            "strategies": [item.strategy_config.config_hash for item in resolved],
        }
    )
    return ResolvedBot(
        bot=bot,
        automations=tuple(resolved),
        config_hash=combined_hash,
    )


def load_active_bots(
    config_root: str | Path | None = None,
) -> dict[str, ResolvedBot]:
    resolved: dict[str, ResolvedBot] = {}
    for bot_id in sorted(load_bots(config_root)):
        bot = resolve_bot(bot_id, config_root=config_root)
        if bot.bot.paused:
            continue
        enabled_automations = tuple(
            item
            for item in bot.automations
            if item.automation.enabled and item.strategy_config.enabled
        )
        if not enabled_automations:
            continue
        resolved[bot_id] = ResolvedBot(
            bot=bot.bot,
            automations=enabled_automations,
            config_hash=bot.config_hash,
        )
    return resolved


def active_entry_automations(
    config_root: str | Path | None = None,
    *,
    scanner_strategy: str | None = None,
    scanner_profile: str | None = None,
) -> list[tuple[ResolvedBot, ResolvedAutomation]]:
    entries: list[tuple[ResolvedBot, ResolvedAutomation]] = []
    for bot in load_active_bots(config_root).values():
        for automation in bot.automations:
            if automation.automation.is_entry:
                if (
                    scanner_strategy is not None
                    and automation.strategy_config.scanner_strategy != scanner_strategy
                ):
                    continue
                if (
                    scanner_profile is not None
                    and automation.strategy_config.scanner_profile != scanner_profile
                ):
                    continue
                entries.append((bot, automation))
    return entries


def build_collector_scope(
    config_root: str | Path | None = None,
    *,
    scanner_strategy: str | None = None,
    scanner_profile: str | None = None,
) -> dict[str, Any]:
    entries = active_entry_automations(
        config_root,
        scanner_strategy=scanner_strategy,
        scanner_profile=scanner_profile,
    )
    if not entries:
        return {
            "enabled": False,
            "symbols": (),
            "scanner_strategy": None,
            "scanner_profile": None,
            "entry_runtimes": [],
        }
    symbols = sorted(
        {symbol for _bot, automation in entries for symbol in automation.symbols}
    )
    scanner_strategies = {
        automation.strategy_config.scanner_strategy for _bot, automation in entries
    }
    scanner_profiles = {
        automation.strategy_config.scanner_profile for _bot, automation in entries
    }
    dte_mins = [
        int(automation.strategy_config.builder_params.get("dte_min") or 0)
        for _bot, automation in entries
        if automation.strategy_config.builder_params.get("dte_min") is not None
    ]
    dte_maxs = [
        int(automation.strategy_config.builder_params.get("dte_max") or 0)
        for _bot, automation in entries
        if automation.strategy_config.builder_params.get("dte_max") is not None
    ]
    short_delta_mins = [
        float(automation.strategy_config.builder_params.get("short_delta_min") or 0.0)
        for _bot, automation in entries
        if automation.strategy_config.builder_params.get("short_delta_min") is not None
    ]
    short_delta_maxs = [
        float(automation.strategy_config.builder_params.get("short_delta_max") or 0.0)
        for _bot, automation in entries
        if automation.strategy_config.builder_params.get("short_delta_max") is not None
    ]
    short_delta_targets = [
        float(
            automation.strategy_config.builder_params.get("short_delta_target") or 0.0
        )
        for _bot, automation in entries
        if automation.strategy_config.builder_params.get("short_delta_target") is not None
    ]
    short_delta_target = None
    if short_delta_targets:
        short_delta_target = sum(short_delta_targets) / len(short_delta_targets)
    elif short_delta_mins and short_delta_maxs:
        short_delta_target = (min(short_delta_mins) + max(short_delta_maxs)) / 2.0
    widths = [
        float(width)
        for _bot, automation in entries
        for width in list(
            automation.strategy_config.builder_params.get("width_points") or []
        )
    ]
    open_interest_values = [
        int(automation.strategy_config.liquidity_rules.get("min_open_interest") or 0)
        for _bot, automation in entries
        if automation.strategy_config.liquidity_rules.get("min_open_interest")
        is not None
    ]
    relative_spread_values = [
        float(
            automation.strategy_config.liquidity_rules.get("max_leg_spread_pct_mid")
            or 0.0
        )
        for _bot, automation in entries
        if automation.strategy_config.liquidity_rules.get("max_leg_spread_pct_mid")
        is not None
    ]
    return_on_risk_values = [
        float(minimum_return_on_risk)
        for _bot, automation in entries
        if (
            minimum_return_on_risk := resolve_strategy_min_return_on_risk(
                automation.strategy_config.scanner_profile,
                risk_defaults=automation.strategy_config.risk_defaults,
            )
        )
        is not None
    ]
    return {
        "enabled": True,
        "symbols": tuple(symbols),
        "scanner_strategy": None
        if len(scanner_strategies) != 1
        else next(iter(scanner_strategies)),
        "scanner_profile": None
        if len(scanner_profiles) != 1
        else next(iter(scanner_profiles)),
        "scanner_args": {
            **({} if not dte_mins else {"min_dte": min(dte_mins)}),
            **({} if not dte_maxs else {"max_dte": max(dte_maxs)}),
            **(
                {}
                if not short_delta_mins
                else {"short_delta_min": min(short_delta_mins)}
            ),
            **(
                {}
                if not short_delta_maxs
                else {"short_delta_max": max(short_delta_maxs)}
            ),
            **(
                {}
                if short_delta_target is None
                else {"short_delta_target": short_delta_target}
            ),
            **(
                {}
                if not widths
                else {"min_width": min(widths), "max_width": max(widths)}
            ),
            **(
                {}
                if not open_interest_values
                else {"min_open_interest": min(open_interest_values)}
            ),
            **(
                {}
                if not relative_spread_values
                else {"max_relative_spread": max(relative_spread_values)}
            ),
            **(
                {}
                if not return_on_risk_values
                else {"min_return_on_risk": min(return_on_risk_values)}
            ),
        },
        "entry_runtimes": entries,
    }


def build_collector_scopes(
    config_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[tuple[ResolvedBot, ResolvedAutomation]]] = {}
    for item in active_entry_automations(config_root):
        bot, automation = item
        key = (
            automation.strategy_config.scanner_strategy,
            automation.strategy_config.scanner_profile,
        )
        groups.setdefault(key, []).append((bot, automation))

    scopes: list[dict[str, Any]] = []
    for (scanner_strategy, scanner_profile), _entries in sorted(groups.items()):
        scope = build_collector_scope(
            config_root,
            scanner_strategy=scanner_strategy,
            scanner_profile=scanner_profile,
        )
        if scope.get("enabled"):
            scopes.append(scope)
    return scopes


__all__ = [
    "BotConfig",
    "ResolvedBot",
    "active_entry_automations",
    "build_collector_scope",
    "build_collector_scopes",
    "bot_time_reached",
    "load_active_bots",
    "load_bots",
    "resolve_bot",
]

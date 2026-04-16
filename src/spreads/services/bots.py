from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spreads.services.automations import (
    ResolvedAutomation,
    load_automations,
    resolve_automation,
)
from spreads.services.strategy_configs import (
    _as_list,
    _as_text,
    _canonical_hash,
    _load_yaml_file,
    default_config_root,
)


@dataclass(frozen=True)
class BotConfig:
    bot_id: str
    name: str
    capital_limit: float
    max_open_positions: int
    max_daily_actions: int
    automation_ids: tuple[str, ...]
    paused: bool
    config_path: Path
    config_hash: str


@dataclass(frozen=True)
class ResolvedBot:
    bot: BotConfig
    automations: tuple[ResolvedAutomation, ...]
    config_hash: str


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
            capital_limit=float(payload.get("capital_limit") or 0.0),
            max_open_positions=int(payload.get("max_open_positions") or 0),
            max_daily_actions=int(payload.get("max_daily_actions") or 0),
            automation_ids=_as_list(
                payload.get("automation_ids"), field_name="automation_ids"
            ),
            paused=bool(payload.get("paused", False)),
            config_path=path,
            config_hash=_canonical_hash(payload),
        )
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
) -> list[tuple[ResolvedBot, ResolvedAutomation]]:
    entries: list[tuple[ResolvedBot, ResolvedAutomation]] = []
    for bot in load_active_bots(config_root).values():
        for automation in bot.automations:
            if automation.automation.is_entry:
                entries.append((bot, automation))
    return entries


def build_collector_scope(
    config_root: str | Path | None = None,
) -> dict[str, Any]:
    entries = active_entry_automations(config_root)
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
    return {
        "enabled": True,
        "symbols": tuple(symbols),
        "scanner_strategy": None
        if len(scanner_strategies) != 1
        else next(iter(scanner_strategies)),
        "scanner_profile": None
        if len(scanner_profiles) != 1
        else next(iter(scanner_profiles)),
        "entry_runtimes": entries,
    }


__all__ = [
    "BotConfig",
    "ResolvedBot",
    "active_entry_automations",
    "build_collector_scope",
    "load_active_bots",
    "load_bots",
    "resolve_bot",
]

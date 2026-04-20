from __future__ import annotations

from pathlib import Path
from typing import Any

from core.services.automations import load_automations, resolve_automation
from core.services.bots import build_collector_scopes, load_active_bots, load_bots, resolve_bot
from core.services.strategy_configs import load_strategy_configs


def validate_options_automation_config(
    *,
    config_root: str | Path | None = None,
) -> dict[str, Any]:
    strategies = load_strategy_configs(config_root)
    automations = load_automations(config_root)
    bots = load_bots(config_root)

    resolved_automations = [
        resolve_automation(automation_id, config_root=config_root)
        for automation_id in sorted(automations)
    ]
    resolved_bots = [
        resolve_bot(bot_id, config_root=config_root) for bot_id in sorted(bots)
    ]
    active_bots = load_active_bots(config_root)
    collector_scopes = build_collector_scopes(config_root)

    return {
        "status": "ok",
        "config_root": str(Path(config_root).resolve()) if config_root else None,
        "strategy_count": len(strategies),
        "automation_count": len(automations),
        "bot_count": len(bots),
        "active_bot_count": len(active_bots),
        "collector_scope_count": len(collector_scopes),
        "strategies": [
            {
                "strategy_config_id": strategy.strategy_config_id,
                "strategy_family": strategy.strategy_family,
                "scanner_strategy": strategy.scanner_strategy,
                "scanner_profile": strategy.scanner_profile,
                "enabled": strategy.enabled,
            }
            for strategy in strategies.values()
        ],
        "automations": [
            {
                "automation_id": item.automation.automation_id,
                "strategy_config_id": item.strategy_config.strategy_config_id,
                "kind": item.automation.kind,
                "universe": item.automation.universe,
                "symbol_count": len(item.symbols),
                "enabled": item.automation.enabled,
            }
            for item in resolved_automations
        ],
        "bots": [
            {
                "bot_id": item.bot.bot_id,
                "automation_count": len(item.automations),
                "paused": item.bot.paused,
            }
            for item in resolved_bots
        ],
        "collector_scopes": [
            {
                "scanner_strategy": scope.get("scanner_strategy"),
                "scanner_profile": scope.get("scanner_profile"),
                "symbol_count": len(list(scope.get("symbols") or [])),
            }
            for scope in collector_scopes
        ],
    }


__all__ = ["validate_options_automation_config"]

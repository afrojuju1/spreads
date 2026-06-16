from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from core.services.trading_strategies import (
    StrategyRoutine,
    TradingStrategyConfig,
    default_config_root,
    load_active_trading_strategies,
    load_trading_strategies,
)


def _routine_payload(routine: StrategyRoutine | None) -> dict[str, Any] | None:
    if routine is None:
        return None
    return {
        "enabled": routine.enabled,
        "cadence_minutes": routine.schedule.cadence_minutes,
        "offset_seconds": routine.schedule.offset_seconds,
        "market_hours_only": routine.schedule.market_hours_only,
        "start_et": routine.schedule.window.start_et,
        "end_et": routine.schedule.window.end_et,
        "recipe_count": len(routine.recipes),
    }


def _strategy_payload(strategy: TradingStrategyConfig) -> dict[str, Any]:
    return {
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "trade_structure": strategy.trade_structure,
        "source": strategy.source.as_dict(),
        "candidate_builder": strategy.candidate_builder_key,
        "build_profile": strategy.build_profile,
        "symbol_count": len(strategy.symbols),
        "entry": _routine_payload(strategy.entry),
        "management": _routine_payload(strategy.management),
        "enabled": strategy.enabled,
        "paused": strategy.paused,
        "execution_mode": strategy.execution.mode,
        "execution_runtime": strategy.execution.runtime,
        "approval_mode": strategy.execution.approval,
        "execution": strategy.execution.as_dict(),
        "protection_model_id": strategy.protection.profile_id,
        "protection_rule_count": len(strategy.protection.rules),
        "protection_rules": sorted(strategy.protection.rules),
        "config_hash": strategy.config_hash,
    }


def validate_trading_strategy_config(
    *,
    config_root: str | Path | None = None,
) -> dict[str, Any]:
    resolved_root = default_config_root(config_root)
    strategies = load_trading_strategies(resolved_root)
    active_strategies = load_active_trading_strategies(resolved_root)
    strategy_values = list(strategies.values())

    return {
        "status": "ok",
        "config_root": str(resolved_root),
        "trading_strategy_count": len(strategies),
        "active_trading_strategy_count": len(active_strategies),
        "entry_routine_count": sum(1 for strategy in strategy_values if strategy.entry is not None and strategy.entry.enabled),
        "management_routine_count": sum(1 for strategy in strategy_values if strategy.management is not None and strategy.management.enabled),
        "source_counts": dict(sorted(Counter(strategy.source.kind for strategy in strategy_values).items())),
        "trade_structure_counts": dict(sorted(Counter(strategy.trade_structure for strategy in strategy_values).items())),
        "trading_strategies": [
            _strategy_payload(strategy)
            for strategy in sorted(
                strategy_values,
                key=lambda item: item.trading_strategy_id,
            )
        ],
    }


__all__ = ["validate_trading_strategy_config"]

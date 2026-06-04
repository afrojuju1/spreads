from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from core.jobs.specs import load_declared_discovery_run_specs
from core.services.trading_strategies import (
    StrategyRoutine,
    TradingStrategyConfig,
    build_discovery_run_scopes,
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
        "scanner_strategy": strategy.scanner_strategy,
        "scanner_profile": strategy.scanner_profile,
        "symbol_count": len(strategy.symbols),
        "entry": _routine_payload(strategy.entry),
        "management": _routine_payload(strategy.management),
        "enabled": strategy.enabled,
        "paused": strategy.paused,
        "live_enabled": strategy.live_enabled,
        "execution_mode": strategy.execution.mode,
        "execution_runtime": strategy.execution.runtime,
        "config_hash": strategy.config_hash,
    }


def _discovery_scope_payload(scope: dict[str, Any]) -> dict[str, Any]:
    entry_strategies = [
        strategy.trading_strategy_id for strategy in scope.get("entry_strategies") or [] if isinstance(strategy, TradingStrategyConfig)
    ]
    return {
        "scanner_strategy": scope.get("scanner_strategy"),
        "scanner_profile": scope.get("scanner_profile"),
        "symbol_count": len(list(scope.get("symbols") or [])),
        "entry_trading_strategy_ids": entry_strategies,
    }


def validate_trading_strategy_config(
    *,
    config_root: str | Path | None = None,
) -> dict[str, Any]:
    resolved_root = default_config_root(config_root)
    strategies = load_trading_strategies(resolved_root)
    active_strategies = load_active_trading_strategies(resolved_root)
    discovery_run_scopes = build_discovery_run_scopes(config_root)
    discovery_runs = load_declared_discovery_run_specs(config_root)
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
        "discovery_run_count": len(discovery_runs),
        "discovery_run_scope_count": len(discovery_run_scopes),
        "trading_strategies": [
            _strategy_payload(strategy)
            for strategy in sorted(
                strategy_values,
                key=lambda item: item.trading_strategy_id,
            )
        ],
        "discovery_run_scopes": [_discovery_scope_payload(scope) for scope in discovery_run_scopes],
        "discovery_runs": [
            {
                "discovery_run_id": item.config.discovery_run_id,
                "scanner_strategy": item.config.scanner_strategy,
                "scanner_profile": item.config.scanner_profile,
                "enabled": item.enabled,
            }
            for item in discovery_runs
        ],
    }


__all__ = ["validate_trading_strategy_config"]

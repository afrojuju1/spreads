from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from core.services.trading_strategy_runtime_models import TradingStrategyConfig
from core.services.trading_strategies import load_active_trading_strategies, load_trading_strategies


def requested_strategy_ids(strategy_ids: Iterable[str] | None) -> tuple[str, ...] | None:
    if strategy_ids is None:
        return None
    values = (strategy_ids,) if isinstance(strategy_ids, str) else strategy_ids
    normalized = tuple(dict.fromkeys(str(value).strip() for value in values if str(value or "").strip()))
    return normalized or None


def load_backtest_strategy_scope(strategy_ids: Iterable[str] | None) -> dict[str, TradingStrategyConfig]:
    requested = requested_strategy_ids(strategy_ids)
    strategies = load_active_trading_strategies() if requested is None else load_trading_strategies()
    if requested is None:
        return dict(strategies)
    missing = [strategy_id for strategy_id in requested if strategy_id not in strategies]
    if missing:
        raise ValueError(f"Unknown trading_strategy_id(s): {', '.join(missing)}")
    return {strategy_id: strategies[strategy_id] for strategy_id in requested}


def strategy_variant_id(strategy: TradingStrategyConfig) -> str:
    return f"{strategy.trading_strategy_id}:{strategy.config_hash[:12]}"


def strategy_quality_profile(strategy: TradingStrategyConfig) -> str | None:
    if strategy.entry is None:
        return None
    return strategy.entry.quality.profile_id


def strategy_profile(strategy: TradingStrategyConfig) -> dict[str, Any]:
    return {
        "variant_id": strategy_variant_id(strategy),
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "trade_structure": strategy.trade_structure,
        "quality_profile": strategy_quality_profile(strategy),
        "source_type": strategy.source.kind,
        "source_id": strategy.source.ref,
        "execution_mode": strategy.execution.mode,
        "execution_runtime": strategy.execution.runtime,
        "approval_mode": strategy.execution.approval,
        "executor_profile_id": strategy.execution.executor_profile_id,
        "config_hash": strategy.config_hash,
        "enabled": strategy.enabled,
        "paused": strategy.paused,
    }


def strategy_config_snapshot(strategy: TradingStrategyConfig) -> dict[str, Any]:
    return {
        **strategy_profile(strategy),
        "config_path": str(strategy.config_path),
        "source": strategy.source.to_payload(),
        "build": strategy.build.to_payload(),
        "liquidity": strategy.liquidity.to_payload(),
        "position_sizing": strategy.position_sizing.to_payload(),
        "risk_limits": strategy.risk_limits.to_payload(),
        "protection": strategy.protection.to_payload(),
        "runtime": strategy.runtime.to_payload(),
        "execution": strategy.execution.to_payload(),
        "entry": None if strategy.entry is None else strategy.entry.to_payload(),
        "management": None if strategy.management is None else strategy.management.to_payload(),
    }


def strategy_scope_snapshot(strategies: dict[str, TradingStrategyConfig]) -> dict[str, Any]:
    return {
        "strategy_count": len(strategies),
        "strategies": [strategy_config_snapshot(strategy) for strategy in strategies.values()],
    }


__all__ = [
    "load_backtest_strategy_scope",
    "requested_strategy_ids",
    "strategy_config_snapshot",
    "strategy_profile",
    "strategy_quality_profile",
    "strategy_scope_snapshot",
    "strategy_variant_id",
]

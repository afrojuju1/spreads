from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any

from core.services.trading_strategies import load_active_trading_strategies

ALPACA_DIRECT_RUNTIME = "alpaca_direct"
SUPPORTED_EXECUTION_RUNTIMES = {ALPACA_DIRECT_RUNTIME}
EXECUTION_RUNTIME_CAPABILITIES_SCHEMA_VERSION = "spreads.execution_runtime_capabilities.v1"


def normalize_execution_runtime(value: Any) -> str:
    runtime = str(value or ALPACA_DIRECT_RUNTIME).strip().lower()
    if runtime in {"alpaca", "direct", "alpaca_direct"}:
        return ALPACA_DIRECT_RUNTIME
    raise ValueError("execution runtime must be alpaca_direct")


def execution_runtime_from_request(request: Mapping[str, Any]) -> str:
    return normalize_execution_runtime(request.get("execution_runtime"))


def _runtime_usage_summary(config_root: Any = None) -> dict[str, dict[str, Any]]:
    counts: dict[str, Counter[str]] = {
        ALPACA_DIRECT_RUNTIME: Counter(),
    }
    strategy_counts: Counter[str] = Counter()
    for strategy in load_active_trading_strategies(config_root).values():
        if strategy.entry is None or not strategy.entry.enabled:
            continue
        execution_runtime = normalize_execution_runtime(strategy.execution.runtime)
        strategy_counts[execution_runtime] += 1
        counts[execution_runtime][strategy.trade_structure] += 1

    return {
        runtime: {
            "entry_strategy_count": int(strategy_counts.get(runtime, 0)),
            "strategy_families": dict(sorted(counts[runtime].items())),
        }
        for runtime in sorted(SUPPORTED_EXECUTION_RUNTIMES)
    }


def resolve_execution_runtime_capabilities(config_root: Any = None) -> dict[str, Any]:
    usage = _runtime_usage_summary(config_root)
    return {
        "schema_version": EXECUTION_RUNTIME_CAPABILITIES_SCHEMA_VERSION,
        "default_runtime": ALPACA_DIRECT_RUNTIME,
        "runtimes": [
            {
                "runtime": ALPACA_DIRECT_RUNTIME,
                "status": "ready",
                "ready": True,
                **usage[ALPACA_DIRECT_RUNTIME],
                "capabilities": [
                    {
                        "name": "alpaca_broker_order_submit",
                        "adapter": "python_native_alpaca_order_adapter",
                        "asset_classes": ["equity", "option"],
                        "actions": ["buy", "sell", "open", "close"],
                        "structures": [
                            "single_name_equity",
                            "single_leg_option",
                            "alpaca_order_payload",
                        ],
                        "status": "ready",
                    },
                    {
                        "name": "alpaca_broker_order_manage",
                        "adapter": "python_native_alpaca_order_adapter",
                        "asset_classes": ["equity", "option"],
                        "actions": ["refresh", "cancel"],
                        "status": "ready",
                    },
                ],
            },
        ],
    }


__all__ = [
    "ALPACA_DIRECT_RUNTIME",
    "EXECUTION_RUNTIME_CAPABILITIES_SCHEMA_VERSION",
    "SUPPORTED_EXECUTION_RUNTIMES",
    "execution_runtime_from_request",
    "normalize_execution_runtime",
    "resolve_execution_runtime_capabilities",
]

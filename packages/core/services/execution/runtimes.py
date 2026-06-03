from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any

from core.services.bots import load_active_bots

ALPACA_DIRECT_RUNTIME = "alpaca_direct"
SUPPORTED_EXECUTION_RUNTIMES = {ALPACA_DIRECT_RUNTIME}
EXECUTION_RUNTIME_CAPABILITIES_SCHEMA_VERSION = (
    "spreads.execution_runtime_capabilities.v1"
)


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
    automation_counts: Counter[str] = Counter()
    for bot in load_active_bots(config_root).values():
        for runtime in bot.automations:
            if not runtime.automation.is_entry:
                continue
            execution_runtime = normalize_execution_runtime(
                runtime.automation.execution_runtime
            )
            automation_counts[execution_runtime] += 1
            counts[execution_runtime][runtime.strategy_config.strategy_family] += 1

    return {
        runtime: {
            "entry_automation_count": int(automation_counts.get(runtime, 0)),
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

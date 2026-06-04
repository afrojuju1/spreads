from __future__ import annotations

from typing import Any

from core.services.runtime_identity import resolve_pipeline_policy_fields


def resolve_runtime_policy_fields(
    *,
    profile: str,
    root_symbol: str | None = None,
) -> dict[str, str]:
    return resolve_pipeline_policy_fields(profile=profile, root_symbol=root_symbol)


def build_runtime_policy_ref(
    *,
    trading_strategy_id: str,
    trade_structure: str,
    routine: str,
    market_date: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "trading_strategy_id": trading_strategy_id,
        "trade_structure": trade_structure,
        "routine": routine,
    }
    if market_date is not None:
        payload["market_date"] = market_date
    return payload


__all__ = ["build_runtime_policy_ref", "resolve_runtime_policy_fields"]

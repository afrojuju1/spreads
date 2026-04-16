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
    bot_id: str,
    automation_id: str,
    strategy_config_id: str,
    strategy_id: str,
    market_date: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "bot_id": bot_id,
        "automation_id": automation_id,
        "strategy_config_id": strategy_config_id,
        "strategy_id": strategy_id,
    }
    if market_date is not None:
        payload["market_date"] = market_date
    return payload


__all__ = ["build_runtime_policy_ref", "resolve_runtime_policy_fields"]

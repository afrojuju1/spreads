from __future__ import annotations

from collections.abc import Mapping
import os
from typing import Any

DEPLOYMENT_MODE_SHADOW = "shadow"
DEPLOYMENT_MODE_PAPER_AUTO = "paper_auto"
DEPLOYMENT_MODE_LIVE_AUTO = "live_auto"
DEPLOYMENT_MODES = {
    DEPLOYMENT_MODE_SHADOW,
    DEPLOYMENT_MODE_PAPER_AUTO,
    DEPLOYMENT_MODE_LIVE_AUTO,
}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def normalize_deployment_mode(value: Any) -> str | None:
    normalized = _as_text(value)
    if normalized is None:
        return None
    lowered = normalized.lower()
    if lowered not in DEPLOYMENT_MODES:
        raise ValueError(f"Unsupported execution deployment mode: {normalized}")
    return lowered


def resolve_execution_deployment_mode(
    execution_policy: Mapping[str, Any] | None,
    *,
    risk_policy: Mapping[str, Any] | None = None,
) -> str:
    explicit_mode = normalize_deployment_mode(
        None if execution_policy is None else execution_policy.get("deployment_mode")
    )
    if explicit_mode is not None:
        return explicit_mode

    if not coerce_bool(None if execution_policy is None else execution_policy.get("enabled")):
        return DEPLOYMENT_MODE_SHADOW

    allow_live = False
    if isinstance(risk_policy, Mapping) and "allow_live" in risk_policy:
        allow_live = coerce_bool(risk_policy.get("allow_live"))
    elif isinstance(execution_policy, Mapping) and "allow_live" in execution_policy:
        allow_live = coerce_bool(execution_policy.get("allow_live"))
    return DEPLOYMENT_MODE_LIVE_AUTO if allow_live else DEPLOYMENT_MODE_PAPER_AUTO


def deployment_mode_auto_executes(deployment_mode: str) -> bool:
    return deployment_mode != DEPLOYMENT_MODE_SHADOW


def deployment_mode_allows_live_trading(deployment_mode: str) -> bool:
    return deployment_mode == DEPLOYMENT_MODE_LIVE_AUTO


def live_deployment_block_reason(
    *,
    deployment_mode: str,
    environment: str,
    allow_live_env: bool | None = None,
) -> str | None:
    if environment != "live":
        return None
    live_flag_enabled = (
        coerce_bool(os.environ.get("SPREADS_ALLOW_LIVE_TRADING"))
        if allow_live_env is None
        else bool(allow_live_env)
    )
    if deployment_mode_allows_live_trading(deployment_mode) and live_flag_enabled:
        return None
    return (
        "Open execution is blocked on a live Alpaca account. "
        "Set execution_policy.deployment_mode=live_auto and "
        "SPREADS_ALLOW_LIVE_TRADING=true to enable it."
    )

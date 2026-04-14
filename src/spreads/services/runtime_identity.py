from __future__ import annotations

from datetime import date
from typing import Any

PIPELINE_ID_PREFIX = "pipeline"


def build_pipeline_id(label: str) -> str:
    normalized_label = str(label).strip().lower()
    return f"{PIPELINE_ID_PREFIX}:{normalized_label}"


def parse_pipeline_id(pipeline_id: str) -> dict[str, str] | None:
    if not pipeline_id:
        return None
    prefix, separator, label = str(pipeline_id).partition(":")
    if prefix != PIPELINE_ID_PREFIX or not separator or not label:
        return None
    return {
        "pipeline_id": str(pipeline_id),
        "label": label,
    }


def build_live_run_scope_id(label: str, market_date: str | date) -> str:
    rendered = (
        market_date.isoformat() if isinstance(market_date, date) else str(market_date)
    )
    return f"live:{label}:{rendered}"


def parse_live_run_scope_id(run_scope_id: str) -> dict[str, str] | None:
    if not run_scope_id:
        return None
    prefix, separator, remainder = run_scope_id.partition(":")
    if prefix != "live" or not separator:
        return None
    label, separator, resolved_market_date = remainder.rpartition(":")
    if not separator or not label or not resolved_market_date:
        return None
    try:
        rendered_date = date.fromisoformat(resolved_market_date).isoformat()
    except ValueError:
        return None
    return {
        "run_scope_id": run_scope_id,
        "label": label,
        "market_date": rendered_date,
    }


def resolve_style_profile(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "core":
        return "carry"
    if normalized == "swing":
        return "swing"
    if normalized in {"0dte", "weekly", "micro"}:
        return "active"
    return normalized or "active"


def resolve_horizon_intent(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "0dte":
        return "same_day"
    if normalized in {"weekly", "micro"}:
        return "short_dated"
    if normalized == "core":
        return "carry"
    if normalized == "swing":
        return "multi_day"
    return normalized or "short_dated"


def resolve_product_class(
    *,
    universe_label: str | None = None,
    root_symbol: str | None = None,
) -> str:
    normalized_universe = str(universe_label or "").strip().lower()
    normalized_symbol = str(root_symbol or "").strip().upper()
    if normalized_universe.startswith("0dte") or normalized_symbol in {
        "SPY",
        "QQQ",
        "IWM",
        "DIA",
    }:
        return "index_etf_options"
    if normalized_symbol:
        return "equity_options"
    return "options"


def resolve_pipeline_policy_fields(
    *,
    profile: Any,
    universe_label: str | None = None,
    root_symbol: str | None = None,
) -> dict[str, str]:
    return {
        "style_profile": resolve_style_profile(profile),
        "horizon_intent": resolve_horizon_intent(profile),
        "product_class": resolve_product_class(
            universe_label=universe_label,
            root_symbol=root_symbol,
        ),
    }

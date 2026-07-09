from __future__ import annotations

from collections.abc import Mapping
from typing import Any


from core.money import money_scaled_float, option_contract_notional
from core.services.option_structures import (
    net_premium_kind,
    normalize_strategy_family,
)
from core.value_coercion import (
    as_text,
    coerce_float,
)

from core.services.risk.policy import BROAD_INDEX_CORRELATION_SYMBOLS

def _candidate_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = candidate.get("candidate")
    if isinstance(payload, dict):
        return dict(payload)
    if not isinstance(candidate, dict):
        return {}
    merged = dict(candidate)
    economics = candidate.get("economics")
    if isinstance(economics, Mapping):
        merged = {
            **merged,
            **dict(economics),
        }
    return merged


def _candidate_with_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = _candidate_payload(candidate)
    if not isinstance(candidate, dict):
        return payload
    return {
        **dict(candidate),
        **payload,
    }

def _candidate_entry_notional(candidate: dict[str, Any], quantity: float, price: float | None) -> float | None:
    entry_price = price
    if entry_price is None or entry_price <= 0:
        payload = _candidate_payload(candidate)
        entry_price = coerce_float(payload.get("midpoint_credit") or payload.get("midpoint_debit") or payload.get("midpoint_value"))
    if entry_price is None or entry_price <= 0:
        return None
    return option_contract_notional(entry_price, quantity)


def _candidate_max_loss(candidate: dict[str, Any], quantity: float) -> float | None:
    candidate_payload = _candidate_payload(candidate)
    max_loss = coerce_float(candidate_payload.get("max_loss"))
    if max_loss is None:
        width = coerce_float(candidate_payload.get("width"))
        midpoint_value = coerce_float(
            candidate_payload.get("midpoint_credit") or candidate_payload.get("midpoint_debit") or candidate_payload.get("midpoint_value")
        )
        premium_kind = net_premium_kind(candidate_payload.get("strategy"))
        if width is not None and midpoint_value is not None:
            if premium_kind == "debit":
                return option_contract_notional(midpoint_value, quantity)
            else:
                return option_contract_notional(max(width - midpoint_value, 0.0), quantity)
    if max_loss is None:
        return None
    return money_scaled_float(max_loss, quantity)

def _date_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if hasattr(value, "date") and callable(value.date):
        try:
            return value.date().isoformat()
        except Exception:
            return None
    if hasattr(value, "isoformat") and callable(value.isoformat):
        try:
            return str(value.isoformat())[:10]
        except Exception:
            return None
    text = str(value).strip()
    return text[:10] if text else None


def _root_symbol(value: Any) -> str | None:
    text = as_text(value)
    if text is None:
        return None
    return text.upper()


def _candidate_root_symbol(candidate: Mapping[str, Any]) -> str | None:
    payload = _candidate_payload(dict(candidate))
    return _root_symbol(
        candidate.get("root_symbol") or candidate.get("underlying_symbol") or payload.get("root_symbol") or payload.get("underlying_symbol")
    )


def _candidate_strategy_family(
    candidate: Mapping[str, Any],
    *,
    strategy_family: Any = None,
) -> str | None:
    payload = _candidate_payload(dict(candidate))
    normalized = normalize_strategy_family(
        strategy_family
        or candidate.get("strategy_family")
        or candidate.get("trade_structure")
        or candidate.get("strategy")
        or payload.get("strategy_family")
        or payload.get("trade_structure")
        or payload.get("strategy")
    )
    return normalized or None


def _portfolio_correlation_group(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    normalized = str(symbol).upper()
    if normalized in BROAD_INDEX_CORRELATION_SYMBOLS:
        return "broad_index_etf"
    return normalized

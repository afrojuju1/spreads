from __future__ import annotations

from typing import Any

from spreads.services.option_quote_records import build_option_symbol_metadata

UOA_ALLOWED_TRADE_CONDITIONS = frozenset({"I", "J", "S", "a", "b"})


def build_trade_symbol_metadata(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return build_option_symbol_metadata(candidates)


def normalize_trade_conditions(raw_conditions: Any) -> list[str]:
    if raw_conditions in (None, ""):
        return []
    if isinstance(raw_conditions, (list, tuple)):
        return [str(item) for item in raw_conditions if str(item)]
    return [str(raw_conditions)]


def classify_trade_conditions_for_uoa(raw_conditions: Any) -> tuple[bool, str | None, list[str]]:
    conditions = normalize_trade_conditions(raw_conditions)
    if not conditions:
        return False, "missing_conditions", conditions
    excluded = [condition for condition in conditions if condition not in UOA_ALLOWED_TRADE_CONDITIONS]
    if excluded:
        return False, f"excluded_conditions:{','.join(excluded)}", conditions
    return True, None, conditions


def build_trade_records(
    *,
    captured_at: str,
    symbol_metadata: dict[str, dict[str, Any]],
    trades: list[Any],
    source: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for trade in trades:
        metadata = symbol_metadata.get(trade.symbol, {})
        included_in_score, exclusion_reason, conditions = classify_trade_conditions_for_uoa(trade.conditions)
        records.append(
            {
                "captured_at": captured_at,
                "underlying_symbol": metadata.get("underlying_symbol"),
                "strategy": metadata.get("strategy"),
                "option_symbol": trade.symbol,
                "leg_role": metadata.get("leg_role", "contract"),
                "price": trade.price,
                "size": trade.size,
                "premium": round(trade.price * trade.size * 100.0, 4),
                "exchange_code": trade.exchange_code,
                "conditions": conditions,
                "trade_timestamp": trade.timestamp,
                "included_in_score": included_in_score,
                "exclusion_reason": exclusion_reason,
                "raw_payload": dict(trade.raw_payload),
                "source": source,
            }
        )
    return records

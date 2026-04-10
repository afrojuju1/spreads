from __future__ import annotations

from typing import Any


def build_option_symbol_metadata(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        contract_symbol = str(candidate.get("option_symbol") or "").strip()
        if contract_symbol:
            metadata[contract_symbol] = {
                "underlying_symbol": candidate.get("underlying_symbol"),
                "strategy": candidate.get("strategy"),
                "leg_role": candidate.get("leg_role") or candidate.get("contract_role") or "contract",
            }
            continue
        for leg_role, option_symbol in (
            ("short", candidate.get("short_symbol")),
            ("long", candidate.get("long_symbol")),
        ):
            option_symbol = str(option_symbol or "").strip()
            if not option_symbol:
                continue
            metadata[option_symbol] = {
                "underlying_symbol": candidate.get("underlying_symbol"),
                "strategy": candidate.get("strategy"),
                "leg_role": leg_role,
            }
    return metadata


def build_quote_symbol_metadata(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return build_option_symbol_metadata(candidates)


def build_quote_records(
    *,
    captured_at: str,
    symbol_metadata: dict[str, dict[str, Any]],
    quotes: list[Any],
    source: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for quote in quotes:
        metadata = symbol_metadata.get(quote.symbol, {})
        records.append(
            {
                "captured_at": captured_at,
                "underlying_symbol": metadata.get("underlying_symbol"),
                "strategy": metadata.get("strategy"),
                "option_symbol": quote.symbol,
                "leg_role": metadata.get("leg_role", "unknown"),
                "bid": quote.bid,
                "ask": quote.ask,
                "midpoint": quote.midpoint,
                "bid_size": quote.bid_size,
                "ask_size": quote.ask_size,
                "quote_timestamp": quote.timestamp,
                "source": source,
            }
        )
    return records

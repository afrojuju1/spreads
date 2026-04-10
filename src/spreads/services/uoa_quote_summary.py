from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from spreads.common import clamp, parse_float, parse_int
from spreads.storage.serializers import parse_datetime, render_value
from spreads.services.uoa_trade_summary import parse_option_symbol_details


def _render_timestamp(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return None if parsed is None else str(render_value(parsed))


def _quote_thresholds(dte: int | None) -> dict[str, float]:
    if dte == 0:
        return {
            "min_mid": 0.20,
            "max_spread_pct": 0.08,
            "min_size": 10.0,
            "stale_after_seconds": 10.0,
        }
    return {
        "min_mid": 0.10,
        "max_spread_pct": 0.12,
        "min_size": 5.0,
        "stale_after_seconds": 15.0,
    }


def _quality_state(*, is_fresh: bool, passes_liquidity_gate: bool, quality_score: float) -> str:
    if not is_fresh:
        return "stale"
    if passes_liquidity_gate and quality_score >= 0.8:
        return "strong"
    if quality_score >= 0.55:
        return "acceptable"
    return "weak"


def build_uoa_quote_summary(
    *,
    as_of: str,
    expected_quote_symbols: Sequence[str] | None,
    quotes: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    as_of_dt = parse_datetime(as_of)
    latest_by_symbol: dict[str, dict[str, Any]] = {}
    expected_symbols = sorted({str(item or "").strip() for item in expected_quote_symbols or [] if str(item or "").strip()})

    for row in quotes or []:
        if not isinstance(row, Mapping):
            continue
        option_symbol = str(row.get("option_symbol") or "").strip()
        if not option_symbol:
            continue
        quote_ts = parse_datetime(row.get("quote_timestamp")) or parse_datetime(row.get("captured_at"))
        latest = latest_by_symbol.get(option_symbol)
        latest_ts = None if latest is None else parse_datetime(latest.get("quote_timestamp")) or parse_datetime(latest.get("captured_at"))
        if latest_ts is None or (quote_ts is not None and quote_ts >= latest_ts):
            latest_by_symbol[option_symbol] = dict(row)

    contracts: list[dict[str, Any]] = []
    roots: dict[str, dict[str, Any]] = {}
    for option_symbol, row in latest_by_symbol.items():
        parsed = parse_option_symbol_details(option_symbol)
        quote_ts = parse_datetime(row.get("quote_timestamp")) or parse_datetime(row.get("captured_at"))
        bid = parse_float(row.get("bid")) or 0.0
        ask = parse_float(row.get("ask")) or 0.0
        midpoint = parse_float(row.get("midpoint")) or 0.0
        bid_size = parse_int(row.get("bid_size")) or 0
        ask_size = parse_int(row.get("ask_size")) or 0
        spread = max(ask - bid, 0.0)
        spread_pct = None if midpoint <= 0 else spread / midpoint
        dte = None
        expiration_date = parsed.get("expiration_date")
        if expiration_date and as_of_dt is not None:
            dte = max((date.fromisoformat(str(expiration_date)) - as_of_dt.date()).days, 0)
        thresholds = _quote_thresholds(dte)
        quote_age_seconds = None
        if quote_ts is not None and as_of_dt is not None:
            quote_age_seconds = max((as_of_dt - quote_ts).total_seconds(), 0.0)
        is_fresh = quote_age_seconds is not None and quote_age_seconds <= thresholds["stale_after_seconds"]
        min_size = min(bid_size, ask_size)
        passes_liquidity_gate = (
            midpoint >= thresholds["min_mid"]
            and spread_pct is not None
            and spread_pct <= thresholds["max_spread_pct"]
            and min_size >= thresholds["min_size"]
        )
        freshness_component = 1.0 if is_fresh else 0.0
        spread_component = 0.0
        if spread_pct is not None and spread_pct > 0:
            spread_component = clamp(thresholds["max_spread_pct"] / spread_pct)
        size_component = clamp(min_size / thresholds["min_size"])
        mid_component = clamp(midpoint / thresholds["min_mid"])
        quality_score = round(
            freshness_component * 0.35
            + spread_component * 0.30
            + size_component * 0.20
            + mid_component * 0.15,
            4,
        )
        summary = {
            "option_symbol": option_symbol,
            "underlying_symbol": row.get("underlying_symbol") or parsed.get("parsed_underlying_symbol"),
            "strategy": row.get("strategy"),
            "leg_role": row.get("leg_role"),
            "option_type": parsed.get("option_type"),
            "expiration_date": expiration_date,
            "dte": dte,
            "strike_price": parsed.get("strike_price"),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "midpoint": round(midpoint, 4),
            "spread": round(spread, 4),
            "spread_pct": None if spread_pct is None else round(spread_pct, 4),
            "bid_size": bid_size,
            "ask_size": ask_size,
            "min_size": min_size,
            "quote_timestamp": _render_timestamp(quote_ts),
            "quote_age_seconds": None if quote_age_seconds is None else round(quote_age_seconds, 2),
            "is_fresh": is_fresh,
            "passes_liquidity_gate": passes_liquidity_gate,
            "quality_score": quality_score,
            "quality_state": _quality_state(
                is_fresh=is_fresh,
                passes_liquidity_gate=passes_liquidity_gate,
                quality_score=quality_score,
            ),
        }
        contracts.append(summary)
        underlying_symbol = str(summary.get("underlying_symbol") or "").strip()
        if not underlying_symbol:
            continue
        root = roots.get(underlying_symbol)
        if root is None:
            root = {
                "underlying_symbol": underlying_symbol,
                "observed_contract_count": 0,
                "fresh_contract_count": 0,
                "liquid_contract_count": 0,
                "quality_score_total": 0.0,
                "contracts": [],
            }
            roots[underlying_symbol] = root
        root["observed_contract_count"] += 1
        if is_fresh:
            root["fresh_contract_count"] += 1
        if passes_liquidity_gate:
            root["liquid_contract_count"] += 1
        root["quality_score_total"] += quality_score
        root["contracts"].append(summary)

    contract_map = {str(item["option_symbol"]): item for item in contracts}
    root_map: dict[str, dict[str, Any]] = {}
    for underlying_symbol, root in roots.items():
        observed = max(int(root["observed_contract_count"]), 1)
        average_quality_score = round(float(root["quality_score_total"]) / observed, 4)
        root_contracts = sorted(
            root["contracts"],
            key=lambda item: (
                -float(item["quality_score"]),
                str(item["option_symbol"]),
            ),
        )
        root_map[underlying_symbol] = {
            "underlying_symbol": underlying_symbol,
            "observed_contract_count": int(root["observed_contract_count"]),
            "fresh_contract_count": int(root["fresh_contract_count"]),
            "liquid_contract_count": int(root["liquid_contract_count"]),
            "average_quality_score": average_quality_score,
            "quality_state": _quality_state(
                is_fresh=int(root["fresh_contract_count"]) > 0,
                passes_liquidity_gate=int(root["liquid_contract_count"]) > 0,
                quality_score=average_quality_score,
            ),
            "top_contracts": [dict(item) for item in root_contracts[:3]],
        }

    overview = {
        "expected_contract_count": len(expected_symbols),
        "observed_contract_count": len(contract_map),
        "fresh_contract_count": sum(1 for item in contracts if item["is_fresh"]),
        "liquid_contract_count": sum(1 for item in contracts if item["passes_liquidity_gate"]),
        "missing_expected_contract_count": len([symbol for symbol in expected_symbols if symbol not in contract_map]),
    }
    return {
        "overview": overview,
        "contracts": contract_map,
        "roots": root_map,
    }

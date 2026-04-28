from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from math import sqrt
from typing import Any

from core.common import clamp, parse_float, parse_int
from core.storage.serializers import parse_date, parse_datetime, render_value
from core.services.uoa_trade_summary import parse_option_symbol_details


def _render_timestamp(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return None if parsed is None else str(render_value(parsed))


def _volume_oi_ratio(*, volume: int | None, open_interest: int | None) -> float | None:
    if volume is None or open_interest is None or open_interest <= 0:
        return None
    return round(volume / open_interest, 4)


def _open_interest_age_days(*, as_of_date: date | None, open_interest_date: Any) -> int | None:
    if as_of_date is None or open_interest_date in (None, ""):
        return None
    try:
        parsed = parse_date(open_interest_date)
    except (TypeError, ValueError):
        return None
    return max((as_of_date - parsed).days, 0)


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


def _moneyness_bucket(percent_otm: float | None) -> str | None:
    if percent_otm is None:
        return None
    absolute = abs(percent_otm)
    if absolute <= 0.01:
        return "atm"
    if absolute <= 0.03:
        return "near_atm"
    if absolute <= 0.08:
        return "otm"
    return "far_otm"


def build_uoa_quote_summary(
    *,
    as_of: str,
    expected_quote_symbols: Sequence[str] | None,
    contract_metadata_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
    quotes: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    as_of_dt = parse_datetime(as_of)
    as_of_date = None if as_of_dt is None else as_of_dt.date()
    latest_by_symbol: dict[str, dict[str, Any]] = {}
    expected_symbols = sorted({str(item or "").strip() for item in expected_quote_symbols or [] if str(item or "").strip()})
    metadata_map = {} if contract_metadata_by_symbol is None else dict(contract_metadata_by_symbol)

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
        metadata = metadata_map.get(option_symbol) or {}
        parsed = parse_option_symbol_details(option_symbol)
        quote_ts = parse_datetime(row.get("quote_timestamp")) or parse_datetime(row.get("captured_at"))
        bid = parse_float(row.get("bid")) or 0.0
        ask = parse_float(row.get("ask")) or 0.0
        midpoint = parse_float(row.get("midpoint")) or 0.0
        bid_size = parse_int(row.get("bid_size")) or 0
        ask_size = parse_int(row.get("ask_size")) or 0
        spread = max(ask - bid, 0.0)
        spread_pct = None if midpoint <= 0 else spread / midpoint
        dte = parse_int(metadata.get("days_to_expiration"))
        expiration_date = metadata.get("expiration_date") or parsed.get("expiration_date")
        if dte is None and expiration_date and as_of_dt is not None:
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
        underlying_price = parse_float(metadata.get("underlying_price"))
        strike_price = parse_float(metadata.get("strike_price") or parsed.get("strike_price"))
        option_type = str(metadata.get("option_type") or parsed.get("option_type") or "").strip().lower() or None
        percent_otm = None
        if option_type in {"call", "put"} and strike_price is not None and underlying_price is not None and underlying_price > 0:
            if option_type == "call":
                percent_otm = round((strike_price - underlying_price) / underlying_price, 4)
            else:
                percent_otm = round((underlying_price - strike_price) / underlying_price, 4)
        open_interest = parse_int(metadata.get("open_interest"))
        open_interest_date = metadata.get("open_interest_date")
        open_interest_age_days = _open_interest_age_days(
            as_of_date=as_of_date,
            open_interest_date=open_interest_date,
        )
        volume = parse_int(metadata.get("volume"))
        volume_oi_ratio = _volume_oi_ratio(volume=volume, open_interest=open_interest)
        quality_score = round(
            freshness_component * 0.35
            + spread_component * 0.30
            + size_component * 0.20
            + mid_component * 0.15,
            4,
        )
        summary = {
            "option_symbol": option_symbol,
            "underlying_symbol": row.get("underlying_symbol") or metadata.get("underlying_symbol") or parsed.get("parsed_underlying_symbol"),
            "strategy": row.get("strategy") or metadata.get("strategy"),
            "leg_role": row.get("leg_role") or metadata.get("leg_role"),
            "option_type": option_type,
            "expiration_date": expiration_date,
            "dte": dte,
            "strike_price": strike_price,
            "underlying_price": underlying_price,
            "percent_otm": percent_otm,
            "atm_distance_pct": None if percent_otm is None else round(abs(percent_otm), 4),
            "moneyness_bucket": _moneyness_bucket(percent_otm),
            "open_interest": open_interest,
            "open_interest_date": open_interest_date,
            "open_interest_age_days": open_interest_age_days,
            "volume": volume,
            "volume_oi_ratio": volume_oi_ratio,
            "implied_volatility": parse_float(metadata.get("implied_volatility")),
            "delta": parse_float(metadata.get("delta")),
            "gamma": parse_float(metadata.get("gamma")),
            "vega": parse_float(metadata.get("vega")),
            "rho": parse_float(metadata.get("rho")),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "midpoint": round(midpoint, 4),
            "spread": round(spread, 4),
            "spread_pct": None if spread_pct is None else round(spread_pct, 4),
            "bid_size": bid_size,
            "ask_size": ask_size,
            "min_size": min_size,
            "last_trade_price": parse_float(metadata.get("last_trade_price")),
            "relative_spread": parse_float(metadata.get("relative_spread")),
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
            "is_front_expiry": False,
            "is_next_expiry": False,
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
                "supporting_volume": 0,
                "supporting_open_interest": 0,
                "max_volume_oi_ratio": 0.0,
                "contracts": [],
            }
            roots[underlying_symbol] = root
        root["observed_contract_count"] += 1
        if is_fresh:
            root["fresh_contract_count"] += 1
        if passes_liquidity_gate:
            root["liquid_contract_count"] += 1
        root["quality_score_total"] += quality_score
        root["supporting_volume"] += int(volume or 0)
        root["supporting_open_interest"] += int(open_interest or 0)
        root["max_volume_oi_ratio"] = max(float(root["max_volume_oi_ratio"]), float(volume_oi_ratio or 0.0))
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
        usable_contracts = [
            item
            for item in root_contracts
            if bool(item.get("is_fresh"))
            and bool(item.get("passes_liquidity_gate"))
            and item.get("implied_volatility") is not None
        ]
        expiries: list[tuple[int, str, dict[str, Any], dict[str, Any]]] = []
        expiries_by_date: dict[str, dict[str, dict[str, Any] | int | str]] = {}
        for contract in usable_contracts:
            expiry = str(contract.get("expiration_date") or "").strip()
            dte = parse_int(contract.get("dte"))
            option_type = str(contract.get("option_type") or "").strip().lower()
            if not expiry or dte is None or option_type not in {"call", "put"}:
                continue
            payload = expiries_by_date.setdefault(
                expiry,
                {"dte": dte, "call": None, "put": None},
            )
            existing = payload.get(option_type)
            current_distance = abs(float(contract.get("atm_distance_pct") or 99.0))
            existing_distance = (
                99.0
                if not isinstance(existing, Mapping)
                else abs(float(existing.get("atm_distance_pct") or 99.0))
            )
            if not isinstance(existing, Mapping) or current_distance < existing_distance:
                payload[option_type] = contract
        for expiry, payload in expiries_by_date.items():
            call_contract = payload.get("call")
            put_contract = payload.get("put")
            dte = parse_int(payload.get("dte"))
            if isinstance(call_contract, Mapping) and isinstance(put_contract, Mapping) and dte is not None:
                expiries.append((dte, expiry, dict(call_contract), dict(put_contract)))
        expiries.sort(key=lambda item: (item[0], item[1]))
        front_expiry_entry = expiries[0] if expiries else None
        next_expiry_entry = expiries[1] if len(expiries) > 1 else None
        if front_expiry_entry is not None:
            front_expiry = front_expiry_entry[1]
            for contract in root_contracts:
                if str(contract.get("expiration_date") or "").strip() == front_expiry:
                    contract["is_front_expiry"] = True
        if next_expiry_entry is not None:
            next_expiry = next_expiry_entry[1]
            for contract in root_contracts:
                if str(contract.get("expiration_date") or "").strip() == next_expiry:
                    contract["is_next_expiry"] = True
        front_expiry_dte = None if front_expiry_entry is None else int(front_expiry_entry[0])
        front_atm_call = None if front_expiry_entry is None else front_expiry_entry[2]
        front_atm_put = None if front_expiry_entry is None else front_expiry_entry[3]
        next_expiry_dte = None if next_expiry_entry is None else int(next_expiry_entry[0])
        next_atm_call = None if next_expiry_entry is None else next_expiry_entry[2]
        next_atm_put = None if next_expiry_entry is None else next_expiry_entry[3]
        front_expiry_atm_iv = None
        if front_atm_call is not None and front_atm_put is not None:
            front_expiry_atm_iv = round(
                (
                    float(front_atm_call.get("implied_volatility") or 0.0)
                    + float(front_atm_put.get("implied_volatility") or 0.0)
                )
                / 2.0,
                4,
            )
        next_expiry_atm_iv = None
        if next_atm_call is not None and next_atm_put is not None:
            next_expiry_atm_iv = round(
                (
                    float(next_atm_call.get("implied_volatility") or 0.0)
                    + float(next_atm_put.get("implied_volatility") or 0.0)
                )
                / 2.0,
                4,
            )
        front_next_term_slope = (
            None
            if front_expiry_atm_iv is None or next_expiry_atm_iv is None
            else round(front_expiry_atm_iv - next_expiry_atm_iv, 4)
        )
        front_atm_call_put_iv_gap = (
            None
            if front_atm_call is None or front_atm_put is None
            else round(
                float(front_atm_call.get("implied_volatility") or 0.0)
                - float(front_atm_put.get("implied_volatility") or 0.0),
                4,
            )
        )
        front_expiry_implied_move_pct = (
            None
            if front_expiry_atm_iv is None or front_expiry_dte is None
            else round(front_expiry_atm_iv * sqrt(max(front_expiry_dte, 1) / 365.0), 4)
        )
        surface_coverage_state = "missing"
        if front_expiry_atm_iv is not None and next_expiry_atm_iv is not None:
            surface_coverage_state = "strong"
        elif front_expiry_atm_iv is not None:
            surface_coverage_state = "partial"
        root_map[underlying_symbol] = {
            "underlying_symbol": underlying_symbol,
            "observed_contract_count": int(root["observed_contract_count"]),
            "fresh_contract_count": int(root["fresh_contract_count"]),
            "liquid_contract_count": int(root["liquid_contract_count"]),
            "average_quality_score": average_quality_score,
            "supporting_volume": int(root["supporting_volume"]),
            "supporting_open_interest": int(root["supporting_open_interest"]),
            "supporting_volume_oi_ratio": _volume_oi_ratio(
                volume=int(root["supporting_volume"]),
                open_interest=int(root["supporting_open_interest"]),
            ),
            "max_volume_oi_ratio": round(float(root["max_volume_oi_ratio"]), 4),
            "quality_state": _quality_state(
                is_fresh=int(root["fresh_contract_count"]) > 0,
                passes_liquidity_gate=int(root["liquid_contract_count"]) > 0,
                quality_score=average_quality_score,
            ),
            "surface_coverage_state": surface_coverage_state,
            "front_expiry": None if front_expiry_entry is None else front_expiry_entry[1],
            "front_expiry_dte": front_expiry_dte,
            "front_expiry_atm_call_symbol": None
            if front_atm_call is None
            else front_atm_call.get("option_symbol"),
            "front_expiry_atm_put_symbol": None
            if front_atm_put is None
            else front_atm_put.get("option_symbol"),
            "front_expiry_atm_iv": front_expiry_atm_iv,
            "next_expiry": None if next_expiry_entry is None else next_expiry_entry[1],
            "next_expiry_dte": next_expiry_dte,
            "next_expiry_atm_iv": next_expiry_atm_iv,
            "front_next_term_slope": front_next_term_slope,
            "front_atm_call_put_iv_gap": front_atm_call_put_iv_gap,
            "front_expiry_implied_move_pct": front_expiry_implied_move_pct,
            "surface_score_inputs": {
                "surface_coverage_state": surface_coverage_state,
                "front_expiry_atm_iv": front_expiry_atm_iv,
                "next_expiry_atm_iv": next_expiry_atm_iv,
                "front_next_term_slope": front_next_term_slope,
                "front_atm_call_put_iv_gap": front_atm_call_put_iv_gap,
                "front_expiry_implied_move_pct": front_expiry_implied_move_pct,
            },
            "top_contracts": [dict(item) for item in root_contracts[:3]],
        }

    supporting_volume = sum(int(item.get("volume") or 0) for item in contracts)
    supporting_open_interest = sum(int(item.get("open_interest") or 0) for item in contracts)
    overview = {
        "expected_contract_count": len(expected_symbols),
        "observed_contract_count": len(contract_map),
        "fresh_contract_count": sum(1 for item in contracts if item["is_fresh"]),
        "liquid_contract_count": sum(1 for item in contracts if item["passes_liquidity_gate"]),
        "supporting_volume": supporting_volume,
        "supporting_open_interest": supporting_open_interest,
        "supporting_volume_oi_ratio": _volume_oi_ratio(
            volume=supporting_volume,
            open_interest=supporting_open_interest,
        ),
        "missing_expected_contract_count": len([symbol for symbol in expected_symbols if symbol not in contract_map]),
    }
    return {
        "overview": overview,
        "contracts": contract_map,
        "roots": root_map,
    }

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from math import log1p
from typing import Any

from spreads.common import clamp, parse_float, parse_int
from spreads.storage.serializers import parse_datetime, render_value

OPTION_SYMBOL_TRAILER_LENGTH = 15
TOP_CONTRACT_PREVIEW_LIMIT = 3


def _normalize_symbols(value: Sequence[str] | None) -> list[str]:
    if value is None:
        return []
    normalized: list[str] = []
    for item in value:
        symbol = str(item or "").strip()
        if symbol and symbol not in normalized:
            normalized.append(symbol)
    return normalized


def _sorted_count_mapping(mapping: Mapping[str, int]) -> dict[str, int]:
    return {
        key: int(value)
        for key, value in sorted(mapping.items(), key=lambda item: (-int(item[1]), item[0]))
        if int(value) > 0
    }


def _score_log_scale(value: float, *, ceiling: float) -> float:
    if value <= 0 or ceiling <= 0:
        return 0.0
    return clamp(log1p(float(value)) / log1p(float(ceiling)))


def _render_timestamp(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return None if parsed is None else str(render_value(parsed))


def parse_option_symbol_details(option_symbol: str) -> dict[str, Any]:
    symbol = str(option_symbol or "").strip()
    if len(symbol) <= OPTION_SYMBOL_TRAILER_LENGTH:
        return {}
    trailer = symbol[-OPTION_SYMBOL_TRAILER_LENGTH:]
    if not (trailer[:6].isdigit() and trailer[6] in {"C", "P"} and trailer[7:].isdigit()):
        return {}
    try:
        expiry = date.fromisoformat(f"20{trailer[:2]}-{trailer[2:4]}-{trailer[4:6]}")
    except ValueError:
        return {}
    return {
        "parsed_underlying_symbol": symbol[:-OPTION_SYMBOL_TRAILER_LENGTH] or None,
        "expiration_date": expiry.isoformat(),
        "option_type": "call" if trailer[6] == "C" else "put",
        "strike_price": int(trailer[7:]) / 1000.0,
    }


def _build_contract_score(summary: Mapping[str, Any]) -> float:
    scoreable_premium = float(summary.get("scoreable_premium") or 0.0)
    scoreable_trade_count = int(summary.get("scoreable_trade_count") or 0)
    scoreable_size = int(summary.get("scoreable_size") or 0)
    raw_trade_count = int(summary.get("raw_trade_count") or 0)
    included_ratio = 0.0 if raw_trade_count <= 0 else scoreable_trade_count / raw_trade_count
    return round(
        _score_log_scale(scoreable_premium, ceiling=25_000.0) * 55.0
        + clamp(scoreable_trade_count / 6.0) * 20.0
        + clamp(scoreable_size / 20.0) * 15.0
        + clamp(included_ratio) * 10.0,
        1,
    )


def _build_root_score(summary: Mapping[str, Any]) -> float:
    scoreable_premium = float(summary.get("scoreable_premium") or 0.0)
    scoreable_trade_count = int(summary.get("scoreable_trade_count") or 0)
    scoreable_contract_count = int(summary.get("scoreable_contract_count") or 0)
    call_premium = float(summary.get("call_scoreable_premium") or 0.0)
    put_premium = float(summary.get("put_scoreable_premium") or 0.0)
    dominant_ratio = 0.0
    if scoreable_premium > 0:
        dominant_ratio = max(call_premium, put_premium) / scoreable_premium
    return round(
        _score_log_scale(scoreable_premium, ceiling=100_000.0) * 50.0
        + clamp(scoreable_trade_count / 12.0) * 20.0
        + clamp(scoreable_contract_count / 4.0) * 15.0
        + clamp(max(scoreable_contract_count - 1, 0) / 3.0) * 10.0
        + clamp(dominant_ratio) * 5.0,
        1,
    )


def _root_top_contract_preview(summary: Mapping[str, Any]) -> dict[str, Any]:
    preview: dict[str, Any] = {
        "option_symbol": summary.get("option_symbol"),
        "contract_score": summary.get("contract_score"),
        "scoreable_premium": summary.get("scoreable_premium"),
        "scoreable_trade_count": summary.get("scoreable_trade_count"),
        "scoreable_size": summary.get("scoreable_size"),
    }
    for key in ("option_type", "expiration_date", "strike_price", "leg_roles"):
        if summary.get(key) is not None:
            preview[key] = summary.get(key)
    return preview


def build_uoa_trade_summary(
    *,
    as_of: str | None = None,
    expected_trade_symbols: Sequence[str] | None,
    trades: Sequence[Mapping[str, Any]] | None,
    top_contracts_limit: int = 10,
    top_roots_limit: int = 10,
) -> dict[str, Any]:
    expected_symbols = _normalize_symbols(expected_trade_symbols)
    rows = [dict(trade) for trade in trades or [] if isinstance(trade, Mapping)]
    as_of_dt = parse_datetime(as_of)
    as_of_date = None if as_of_dt is None else as_of_dt.date()

    contracts: dict[str, dict[str, Any]] = {}
    overview_condition_counts: dict[str, int] = defaultdict(int)
    overview_excluded_reason_counts: dict[str, int] = defaultdict(int)
    first_trade_at = None
    last_trade_at = None

    for trade in rows:
        option_symbol = str(trade.get("option_symbol") or "").strip()
        if not option_symbol:
            continue
        included_in_score = bool(trade.get("included_in_score"))
        parsed_details = parse_option_symbol_details(option_symbol)
        trade_timestamp = parse_datetime(trade.get("trade_timestamp")) or parse_datetime(trade.get("captured_at"))
        if trade_timestamp is not None:
            if first_trade_at is None or trade_timestamp < first_trade_at:
                first_trade_at = trade_timestamp
            if last_trade_at is None or trade_timestamp > last_trade_at:
                last_trade_at = trade_timestamp
        contract = contracts.get(option_symbol)
        if contract is None:
            contract = {
                "option_symbol": option_symbol,
                "underlying_symbol": trade.get("underlying_symbol") or parsed_details.get("parsed_underlying_symbol"),
                "strategy": trade.get("strategy"),
                "leg_roles": set(),
                "option_type": parsed_details.get("option_type"),
                "expiration_date": parsed_details.get("expiration_date"),
                "strike_price": parsed_details.get("strike_price"),
                "raw_trade_count": 0,
                "raw_size": 0,
                "raw_premium": 0.0,
                "scoreable_trade_count": 0,
                "scoreable_size": 0,
                "scoreable_premium": 0.0,
                "excluded_trade_count": 0,
                "excluded_premium": 0.0,
                "largest_scoreable_trade_premium": 0.0,
                "largest_scoreable_trade_size": 0,
                "first_trade_at": trade_timestamp,
                "last_trade_at": trade_timestamp,
                "excluded_reason_counts": defaultdict(int),
                "condition_counts": defaultdict(int),
            }
            contracts[option_symbol] = contract
        leg_role = str(trade.get("leg_role") or "").strip()
        if leg_role:
            contract["leg_roles"].add(leg_role)
        if trade_timestamp is not None:
            if contract["first_trade_at"] is None or trade_timestamp < contract["first_trade_at"]:
                contract["first_trade_at"] = trade_timestamp
            if contract["last_trade_at"] is None or trade_timestamp > contract["last_trade_at"]:
                contract["last_trade_at"] = trade_timestamp
        size = parse_int(trade.get("size")) or 0
        premium = parse_float(trade.get("premium")) or 0.0
        contract["raw_trade_count"] += 1
        contract["raw_size"] += size
        contract["raw_premium"] += premium
        for condition in trade.get("conditions") or []:
            rendered = str(condition or "").strip()
            if not rendered:
                continue
            overview_condition_counts[rendered] += 1
            contract["condition_counts"][rendered] += 1
        if included_in_score:
            contract["scoreable_trade_count"] += 1
            contract["scoreable_size"] += size
            contract["scoreable_premium"] += premium
            if premium > contract["largest_scoreable_trade_premium"]:
                contract["largest_scoreable_trade_premium"] = premium
            if size > contract["largest_scoreable_trade_size"]:
                contract["largest_scoreable_trade_size"] = size
            continue
        contract["excluded_trade_count"] += 1
        contract["excluded_premium"] += premium
        exclusion_reason = str(trade.get("exclusion_reason") or "").strip() or "unspecified_exclusion"
        overview_excluded_reason_counts[exclusion_reason] += 1
        contract["excluded_reason_counts"][exclusion_reason] += 1

    contract_summaries: list[dict[str, Any]] = []
    for contract in contracts.values():
        raw_trade_count = int(contract["raw_trade_count"])
        scoreable_trade_count = int(contract["scoreable_trade_count"])
        expiration_date = contract.get("expiration_date")
        dte = None
        if expiration_date and as_of_date is not None:
            dte = max((date.fromisoformat(str(expiration_date)) - as_of_date).days, 0)
        summary = {
            "option_symbol": contract["option_symbol"],
            "underlying_symbol": contract.get("underlying_symbol"),
            "strategy": contract.get("strategy"),
            "leg_roles": sorted(contract["leg_roles"]),
            "option_type": contract.get("option_type"),
            "expiration_date": expiration_date,
            "strike_price": contract.get("strike_price"),
            "dte": dte,
            "raw_trade_count": raw_trade_count,
            "raw_size": int(contract["raw_size"]),
            "raw_premium": round(float(contract["raw_premium"]), 4),
            "scoreable_trade_count": scoreable_trade_count,
            "scoreable_size": int(contract["scoreable_size"]),
            "scoreable_premium": round(float(contract["scoreable_premium"]), 4),
            "excluded_trade_count": int(contract["excluded_trade_count"]),
            "excluded_premium": round(float(contract["excluded_premium"]), 4),
            "included_ratio": round(0.0 if raw_trade_count <= 0 else scoreable_trade_count / raw_trade_count, 4),
            "largest_scoreable_trade_premium": round(float(contract["largest_scoreable_trade_premium"]), 4),
            "largest_scoreable_trade_size": int(contract["largest_scoreable_trade_size"]),
            "first_trade_at": _render_timestamp(contract["first_trade_at"]),
            "last_trade_at": _render_timestamp(contract["last_trade_at"]),
            "excluded_reason_counts": _sorted_count_mapping(contract["excluded_reason_counts"]),
            "condition_counts": _sorted_count_mapping(contract["condition_counts"]),
        }
        summary["contract_score"] = _build_contract_score(summary)
        contract_summaries.append(summary)

    contract_summaries.sort(
        key=lambda item: (
            -float(item["contract_score"]),
            -float(item["scoreable_premium"]),
            -int(item["scoreable_trade_count"]),
            str(item["option_symbol"]),
        )
    )

    roots: dict[str, dict[str, Any]] = {}
    for contract in contract_summaries:
        parsed_details = parse_option_symbol_details(str(contract["option_symbol"]))
        underlying_symbol = str(
            contract.get("underlying_symbol") or parsed_details.get("parsed_underlying_symbol") or ""
        ).strip()
        if not underlying_symbol:
            continue
        root = roots.get(underlying_symbol)
        if root is None:
            root = {
                "underlying_symbol": underlying_symbol,
                "observed_contract_count": 0,
                "scoreable_contract_count": 0,
                "raw_trade_count": 0,
                "scoreable_trade_count": 0,
                "scoreable_size": 0,
                "excluded_trade_count": 0,
                "raw_premium": 0.0,
                "scoreable_premium": 0.0,
                "excluded_premium": 0.0,
                "call_scoreable_premium": 0.0,
                "put_scoreable_premium": 0.0,
                "call_scoreable_trade_count": 0,
                "put_scoreable_trade_count": 0,
                "contracts": [],
            }
            roots[underlying_symbol] = root
        root["observed_contract_count"] += 1
        if int(contract["scoreable_trade_count"]) > 0:
            root["scoreable_contract_count"] += 1
        root["raw_trade_count"] += int(contract["raw_trade_count"])
        root["scoreable_trade_count"] += int(contract["scoreable_trade_count"])
        root["scoreable_size"] += int(contract["scoreable_size"])
        root["excluded_trade_count"] += int(contract["excluded_trade_count"])
        root["raw_premium"] += float(contract["raw_premium"])
        root["scoreable_premium"] += float(contract["scoreable_premium"])
        root["excluded_premium"] += float(contract["excluded_premium"])
        if contract.get("option_type") == "call":
            root["call_scoreable_premium"] += float(contract["scoreable_premium"])
            root["call_scoreable_trade_count"] += int(contract["scoreable_trade_count"])
        elif contract.get("option_type") == "put":
            root["put_scoreable_premium"] += float(contract["scoreable_premium"])
            root["put_scoreable_trade_count"] += int(contract["scoreable_trade_count"])
        root["contracts"].append(contract)

    root_summaries: list[dict[str, Any]] = []
    for root in roots.values():
        call_premium = round(float(root["call_scoreable_premium"]), 4)
        put_premium = round(float(root["put_scoreable_premium"]), 4)
        dominant_flow = "mixed"
        if call_premium > put_premium:
            dominant_flow = "call"
        elif put_premium > call_premium:
            dominant_flow = "put"
        dominant_flow_ratio = 0.0
        if float(root["scoreable_premium"]) > 0:
            dominant_flow_ratio = max(call_premium, put_premium) / float(root["scoreable_premium"])
        contracts_for_root = sorted(
            root["contracts"],
            key=lambda item: (
                -float(item["contract_score"]),
                -float(item["scoreable_premium"]),
                str(item["option_symbol"]),
            ),
        )
        summary = {
            "underlying_symbol": root["underlying_symbol"],
            "observed_contract_count": int(root["observed_contract_count"]),
            "scoreable_contract_count": int(root["scoreable_contract_count"]),
            "raw_trade_count": int(root["raw_trade_count"]),
            "scoreable_trade_count": int(root["scoreable_trade_count"]),
            "scoreable_size": int(root["scoreable_size"]),
            "excluded_trade_count": int(root["excluded_trade_count"]),
            "raw_premium": round(float(root["raw_premium"]), 4),
            "scoreable_premium": round(float(root["scoreable_premium"]), 4),
            "excluded_premium": round(float(root["excluded_premium"]), 4),
            "call_scoreable_premium": call_premium,
            "put_scoreable_premium": put_premium,
            "call_scoreable_trade_count": int(root["call_scoreable_trade_count"]),
            "put_scoreable_trade_count": int(root["put_scoreable_trade_count"]),
            "dominant_flow": dominant_flow,
            "dominant_flow_ratio": round(dominant_flow_ratio, 4),
            "top_contracts": [
                _root_top_contract_preview(contract)
                for contract in contracts_for_root[:TOP_CONTRACT_PREVIEW_LIMIT]
            ],
        }
        summary["root_score"] = _build_root_score(summary)
        root_summaries.append(summary)

    root_summaries.sort(
        key=lambda item: (
            -float(item["root_score"]),
            -float(item["scoreable_premium"]),
            -int(item["scoreable_trade_count"]),
            str(item["underlying_symbol"]),
        )
    )

    observed_symbols = {str(contract["option_symbol"]) for contract in contract_summaries}
    scoreable_contract_count = sum(1 for contract in contract_summaries if int(contract["scoreable_trade_count"]) > 0)
    scoreable_root_count = sum(1 for root in root_summaries if int(root["scoreable_trade_count"]) > 0)
    scoreable_trade_count = sum(int(contract["scoreable_trade_count"]) for contract in contract_summaries)
    excluded_trade_count = sum(int(contract["excluded_trade_count"]) for contract in contract_summaries)
    missing_expected_symbols = [symbol for symbol in expected_symbols if symbol not in observed_symbols]
    overview = {
        "summary_status": (
            "empty"
            if not contract_summaries
            else "captured_no_scoreable_trades"
            if scoreable_trade_count <= 0
            else "active"
        ),
        "expected_contract_count": len(expected_symbols),
        "observed_contract_count": len(contract_summaries),
        "scoreable_contract_count": scoreable_contract_count,
        "scoreable_root_count": scoreable_root_count,
        "raw_trade_count": sum(int(contract["raw_trade_count"]) for contract in contract_summaries),
        "scoreable_trade_count": scoreable_trade_count,
        "excluded_trade_count": excluded_trade_count,
        "raw_premium": round(sum(float(contract["raw_premium"]) for contract in contract_summaries), 4),
        "scoreable_premium": round(sum(float(contract["scoreable_premium"]) for contract in contract_summaries), 4),
        "excluded_premium": round(sum(float(contract["excluded_premium"]) for contract in contract_summaries), 4),
        "first_trade_at": _render_timestamp(first_trade_at),
        "last_trade_at": _render_timestamp(last_trade_at),
        "missing_expected_contract_count": len(missing_expected_symbols),
        "missing_expected_symbols_sample": missing_expected_symbols[:10],
        "excluded_reason_counts": _sorted_count_mapping(overview_excluded_reason_counts),
        "condition_counts": _sorted_count_mapping(overview_condition_counts),
    }
    return {
        "overview": overview,
        "top_contracts": [dict(summary) for summary in contract_summaries[: max(int(top_contracts_limit), 0)]],
        "top_roots": [dict(summary) for summary in root_summaries[: max(int(top_roots_limit), 0)]],
    }

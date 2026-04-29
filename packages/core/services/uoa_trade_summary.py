from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from math import log1p
from typing import Any

from core.common import clamp, parse_float, parse_int
from core.storage.serializers import parse_date, parse_datetime, render_value

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


def _volume_oi_ratio(*, volume: int | None, open_interest: int | None) -> float | None:
    if volume is None or open_interest is None or open_interest <= 0:
        return None
    return round(volume / open_interest, 4)


def _render_timestamp(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return None if parsed is None else str(render_value(parsed))


def _open_interest_age_days(
    *,
    as_of_date: date | None,
    open_interest_date: Any,
) -> int | None:
    if as_of_date is None or open_interest_date in (None, ""):
        return None
    try:
        parsed = parse_date(open_interest_date)
    except (TypeError, ValueError):
        return None
    return max((as_of_date - parsed).days, 0)


def _merge_contract_metadata(
    option_symbol: str,
    *,
    contract_metadata_by_symbol: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    metadata = contract_metadata_by_symbol.get(option_symbol)
    return {} if metadata is None else dict(metadata)


def _percent_otm(
    *,
    option_type: str | None,
    strike_price: float | None,
    underlying_price: float | None,
) -> float | None:
    if option_type not in {"call", "put"} or strike_price is None or underlying_price is None or underlying_price <= 0:
        return None
    if option_type == "call":
        return round((strike_price - underlying_price) / underlying_price, 4)
    return round((underlying_price - strike_price) / underlying_price, 4)


def _atm_distance_pct(
    *,
    strike_price: float | None,
    underlying_price: float | None,
) -> float | None:
    if strike_price is None or underlying_price is None or underlying_price <= 0:
        return None
    return round(abs(strike_price - underlying_price) / underlying_price, 4)


def _atm_relevance_score(atm_distance_pct: float | None) -> float | None:
    if atm_distance_pct is None:
        return None
    if atm_distance_pct <= 0.01:
        return 1.0
    if atm_distance_pct >= 0.10:
        return 0.0
    return round(clamp(1.0 - (atm_distance_pct / 0.10)), 4)


def _expiry_bucket(dte: int | None) -> str | None:
    if dte is None:
        return None
    if dte <= 0:
        return "0dte"
    if dte <= 2:
        return "1_2dte"
    if dte <= 7:
        return "3_7dte"
    if dte <= 14:
        return "8_14dte"
    return "15dte_plus"


def _balance_score(left_value: float, right_value: float, *, total_value: float) -> float:
    if total_value <= 0:
        return 0.0
    return round(clamp(1.0 - abs(left_value - right_value) / total_value), 4)


def _open_interest_freshness_score(age_days: int | None) -> float | None:
    if age_days is None:
        return None
    if age_days <= 1:
        return 1.0
    if age_days == 2:
        return 0.5
    return 0.0


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
    volume_oi_ratio = float(summary.get("volume_oi_ratio") or 0.0)
    included_ratio = 0.0 if raw_trade_count <= 0 else scoreable_trade_count / raw_trade_count
    return round(
        _score_log_scale(scoreable_premium, ceiling=25_000.0) * 50.0
        + clamp(scoreable_trade_count / 6.0) * 18.0
        + clamp(scoreable_size / 20.0) * 14.0
        + clamp(volume_oi_ratio / 1.0) * 10.0
        + clamp(included_ratio) * 8.0,
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
        "signed_premium": summary.get("signed_premium"),
        "signed_delta_notional": summary.get("signed_delta_notional"),
        "signed_vega_notional": summary.get("signed_vega_notional"),
        "signed_gamma_dollar_exposure": summary.get("signed_gamma_dollar_exposure"),
    }
    for key in (
        "option_type",
        "expiration_date",
        "strike_price",
        "leg_roles",
        "dte",
        "underlying_price",
        "percent_otm",
        "open_interest",
        "open_interest_date",
        "open_interest_age_days",
        "volume",
        "volume_oi_ratio",
        "implied_volatility",
        "delta",
        "gamma",
        "vega",
        "rho",
        "atm_distance_pct",
        "atm_relevance_score",
        "expiry_bucket",
        "bid",
        "ask",
        "midpoint",
        "bid_size",
        "ask_size",
        "relative_spread",
        "last_trade_price",
    ):
        if summary.get(key) is not None:
            preview[key] = summary.get(key)
    return preview


def build_uoa_trade_summary(
    *,
    as_of: str | None = None,
    expected_trade_symbols: Sequence[str] | None,
    contract_metadata_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
    trades: Sequence[Mapping[str, Any]] | None,
    top_contracts_limit: int = 10,
    top_roots_limit: int = 10,
) -> dict[str, Any]:
    expected_symbols = _normalize_symbols(expected_trade_symbols)
    rows = [dict(trade) for trade in trades or [] if isinstance(trade, Mapping)]
    as_of_dt = parse_datetime(as_of)
    as_of_date = None if as_of_dt is None else as_of_dt.date()
    metadata_map = {} if contract_metadata_by_symbol is None else dict(contract_metadata_by_symbol)

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
        contract_metadata = _merge_contract_metadata(option_symbol, contract_metadata_by_symbol=metadata_map)
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
                "underlying_symbol": (
                    trade.get("underlying_symbol")
                    or contract_metadata.get("underlying_symbol")
                    or parsed_details.get("parsed_underlying_symbol")
                ),
                "strategy": trade.get("strategy") or contract_metadata.get("strategy"),
                "leg_roles": set(),
                "option_type": contract_metadata.get("option_type") or parsed_details.get("option_type"),
                "expiration_date": contract_metadata.get("expiration_date") or parsed_details.get("expiration_date"),
                "strike_price": contract_metadata.get("strike_price") or parsed_details.get("strike_price"),
                "days_to_expiration": parse_int(contract_metadata.get("days_to_expiration")),
                "underlying_price": parse_float(contract_metadata.get("underlying_price")),
                "open_interest": parse_int(contract_metadata.get("open_interest")),
                "open_interest_date": contract_metadata.get("open_interest_date"),
                "volume": parse_int(contract_metadata.get("volume")),
                "implied_volatility": parse_float(contract_metadata.get("implied_volatility")),
                "delta": parse_float(contract_metadata.get("delta")),
                "gamma": parse_float(contract_metadata.get("gamma")),
                "vega": parse_float(contract_metadata.get("vega")),
                "rho": parse_float(contract_metadata.get("rho")),
                "bid": parse_float(contract_metadata.get("bid")),
                "ask": parse_float(contract_metadata.get("ask")),
                "midpoint": parse_float(contract_metadata.get("midpoint")),
                "bid_size": parse_int(contract_metadata.get("bid_size")),
                "ask_size": parse_int(contract_metadata.get("ask_size")),
                "last_trade_price": parse_float(contract_metadata.get("last_trade_price")),
                "relative_spread": parse_float(contract_metadata.get("relative_spread")),
                "raw_trade_count": 0,
                "raw_size": 0,
                "raw_premium": 0.0,
                "scoreable_trade_count": 0,
                "scoreable_size": 0,
                "scoreable_premium": 0.0,
                "buy_initiated_trade_count": 0,
                "sell_initiated_trade_count": 0,
                "unknown_initiated_trade_count": 0,
                "signed_trade_count": 0,
                "signed_size": 0,
                "signed_premium": 0.0,
                "signed_delta_notional": 0.0,
                "signed_vega_notional": 0.0,
                "signed_gamma_dollar_exposure": 0.0,
                "gross_delta_notional": 0.0,
                "gross_vega_notional": 0.0,
                "gross_gamma_dollar_exposure": 0.0,
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
            aggressor_side = str(trade.get("aggressor_side") or "unknown").strip().lower()
            if aggressor_side == "buy":
                contract["buy_initiated_trade_count"] += 1
            elif aggressor_side == "sell":
                contract["sell_initiated_trade_count"] += 1
            else:
                contract["unknown_initiated_trade_count"] += 1
            contract["signed_trade_count"] += parse_int(trade.get("signed_trade_count")) or 0
            contract["signed_size"] += parse_int(trade.get("signed_size")) or 0
            contract["signed_premium"] += parse_float(trade.get("signed_premium")) or 0.0
            contract["signed_delta_notional"] += (
                parse_float(trade.get("signed_delta_notional")) or 0.0
            )
            contract["signed_vega_notional"] += (
                parse_float(trade.get("signed_vega_notional")) or 0.0
            )
            contract["signed_gamma_dollar_exposure"] += (
                parse_float(trade.get("signed_gamma_dollar_exposure")) or 0.0
            )
            contract["gross_delta_notional"] += (
                parse_float(trade.get("gross_delta_notional")) or 0.0
            )
            contract["gross_vega_notional"] += (
                parse_float(trade.get("gross_vega_notional")) or 0.0
            )
            contract["gross_gamma_dollar_exposure"] += (
                parse_float(trade.get("gross_gamma_dollar_exposure")) or 0.0
            )
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
        dte = parse_int(contract.get("days_to_expiration"))
        if dte is None and expiration_date and as_of_date is not None:
            dte = max((date.fromisoformat(str(expiration_date)) - as_of_date).days, 0)
        open_interest = parse_int(contract.get("open_interest"))
        open_interest_date = contract.get("open_interest_date")
        open_interest_age_days = _open_interest_age_days(
            as_of_date=as_of_date,
            open_interest_date=open_interest_date,
        )
        volume = parse_int(contract.get("volume"))
        underlying_price = parse_float(contract.get("underlying_price"))
        strike_price = parse_float(contract.get("strike_price"))
        option_type = str(contract.get("option_type") or "").strip().lower() or None
        volume_oi_ratio = _volume_oi_ratio(volume=volume, open_interest=open_interest)
        atm_distance_pct = _atm_distance_pct(
            strike_price=strike_price,
            underlying_price=underlying_price,
        )
        summary = {
            "option_symbol": contract["option_symbol"],
            "underlying_symbol": contract.get("underlying_symbol"),
            "strategy": contract.get("strategy"),
            "leg_roles": sorted(contract["leg_roles"]),
            "option_type": option_type,
            "expiration_date": expiration_date,
            "strike_price": strike_price,
            "dte": dte,
            "underlying_price": underlying_price,
            "percent_otm": _percent_otm(
                option_type=option_type,
                strike_price=strike_price,
                underlying_price=underlying_price,
            ),
            "open_interest": open_interest,
            "open_interest_date": open_interest_date,
            "open_interest_age_days": open_interest_age_days,
            "volume": volume,
            "volume_oi_ratio": volume_oi_ratio,
            "implied_volatility": parse_float(contract.get("implied_volatility")),
            "delta": parse_float(contract.get("delta")),
            "gamma": parse_float(contract.get("gamma")),
            "vega": parse_float(contract.get("vega")),
            "rho": parse_float(contract.get("rho")),
            "atm_distance_pct": atm_distance_pct,
            "atm_relevance_score": _atm_relevance_score(atm_distance_pct),
            "expiry_bucket": _expiry_bucket(dte),
            "bid": parse_float(contract.get("bid")),
            "ask": parse_float(contract.get("ask")),
            "midpoint": parse_float(contract.get("midpoint")),
            "bid_size": parse_int(contract.get("bid_size")),
            "ask_size": parse_int(contract.get("ask_size")),
            "last_trade_price": parse_float(contract.get("last_trade_price")),
            "relative_spread": parse_float(contract.get("relative_spread")),
            "raw_trade_count": raw_trade_count,
            "raw_size": int(contract["raw_size"]),
            "raw_premium": round(float(contract["raw_premium"]), 4),
            "scoreable_trade_count": scoreable_trade_count,
            "scoreable_size": int(contract["scoreable_size"]),
            "scoreable_premium": round(float(contract["scoreable_premium"]), 4),
            "buy_initiated_trade_count": int(contract["buy_initiated_trade_count"]),
            "sell_initiated_trade_count": int(contract["sell_initiated_trade_count"]),
            "unknown_initiated_trade_count": int(contract["unknown_initiated_trade_count"]),
            "aggressor_known_trade_count": int(contract["buy_initiated_trade_count"])
            + int(contract["sell_initiated_trade_count"]),
            "signed_trade_count": int(contract["signed_trade_count"]),
            "signed_size": int(contract["signed_size"]),
            "signed_premium": round(float(contract["signed_premium"]), 4),
            "signed_delta_notional": round(float(contract["signed_delta_notional"]), 4),
            "signed_vega_notional": round(float(contract["signed_vega_notional"]), 4),
            "signed_gamma_dollar_exposure": round(
                float(contract["signed_gamma_dollar_exposure"]), 4
            ),
            "gross_delta_notional": round(float(contract["gross_delta_notional"]), 4),
            "gross_vega_notional": round(float(contract["gross_vega_notional"]), 4),
            "gross_gamma_dollar_exposure": round(
                float(contract["gross_gamma_dollar_exposure"]), 4
            ),
            "excluded_trade_count": int(contract["excluded_trade_count"]),
            "excluded_premium": round(float(contract["excluded_premium"]), 4),
            "included_ratio": round(0.0 if raw_trade_count <= 0 else scoreable_trade_count / raw_trade_count, 4),
            "aggressor_known_ratio": round(
                (
                    int(contract["buy_initiated_trade_count"])
                    + int(contract["sell_initiated_trade_count"])
                )
                / scoreable_trade_count,
                4,
            )
            if scoreable_trade_count > 0
            else 0.0,
            "largest_scoreable_trade_premium": round(float(contract["largest_scoreable_trade_premium"]), 4),
            "largest_scoreable_trade_size": int(contract["largest_scoreable_trade_size"]),
            "first_trade_at": _render_timestamp(contract["first_trade_at"]),
            "last_trade_at": _render_timestamp(contract["last_trade_at"]),
            "excluded_reason_counts": _sorted_count_mapping(contract["excluded_reason_counts"]),
            "condition_counts": _sorted_count_mapping(contract["condition_counts"]),
            "support_score_inputs": {
                "abs_signed_premium": round(abs(float(contract["signed_premium"])), 4),
                "abs_signed_size": abs(int(contract["signed_size"])),
                "atm_relevance_score": _atm_relevance_score(atm_distance_pct),
                "volume_oi_ratio": volume_oi_ratio,
            },
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
                "buy_initiated_trade_count": 0,
                "sell_initiated_trade_count": 0,
                "unknown_initiated_trade_count": 0,
                "signed_trade_count": 0,
                "signed_size": 0,
                "signed_premium": 0.0,
                "signed_delta_notional": 0.0,
                "signed_vega_notional": 0.0,
                "signed_gamma_dollar_exposure": 0.0,
                "gross_delta_notional": 0.0,
                "gross_vega_notional": 0.0,
                "gross_gamma_dollar_exposure": 0.0,
                "call_scoreable_premium": 0.0,
                "put_scoreable_premium": 0.0,
                "call_signed_premium": 0.0,
                "put_signed_premium": 0.0,
                "call_scoreable_trade_count": 0,
                "put_scoreable_trade_count": 0,
                "call_scoreable_contract_count": 0,
                "put_scoreable_contract_count": 0,
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
        root["buy_initiated_trade_count"] += int(contract.get("buy_initiated_trade_count") or 0)
        root["sell_initiated_trade_count"] += int(contract.get("sell_initiated_trade_count") or 0)
        root["unknown_initiated_trade_count"] += int(
            contract.get("unknown_initiated_trade_count") or 0
        )
        root["signed_trade_count"] += int(contract.get("signed_trade_count") or 0)
        root["signed_size"] += int(contract.get("signed_size") or 0)
        root["signed_premium"] += float(contract.get("signed_premium") or 0.0)
        root["signed_delta_notional"] += float(
            contract.get("signed_delta_notional") or 0.0
        )
        root["signed_vega_notional"] += float(
            contract.get("signed_vega_notional") or 0.0
        )
        root["signed_gamma_dollar_exposure"] += float(
            contract.get("signed_gamma_dollar_exposure") or 0.0
        )
        root["gross_delta_notional"] += float(
            contract.get("gross_delta_notional") or 0.0
        )
        root["gross_vega_notional"] += float(contract.get("gross_vega_notional") or 0.0)
        root["gross_gamma_dollar_exposure"] += float(
            contract.get("gross_gamma_dollar_exposure") or 0.0
        )
        if contract.get("option_type") == "call":
            root["call_scoreable_premium"] += float(contract["scoreable_premium"])
            root["call_signed_premium"] += float(contract.get("signed_premium") or 0.0)
            root["call_scoreable_trade_count"] += int(contract["scoreable_trade_count"])
            if int(contract["scoreable_trade_count"]) > 0:
                root["call_scoreable_contract_count"] += 1
        elif contract.get("option_type") == "put":
            root["put_scoreable_premium"] += float(contract["scoreable_premium"])
            root["put_signed_premium"] += float(contract.get("signed_premium") or 0.0)
            root["put_scoreable_trade_count"] += int(contract["scoreable_trade_count"])
            if int(contract["scoreable_trade_count"]) > 0:
                root["put_scoreable_contract_count"] += 1
        root["contracts"].append(contract)

    root_summaries: list[dict[str, Any]] = []
    for root in roots.values():
        call_premium = round(float(root["call_scoreable_premium"]), 4)
        put_premium = round(float(root["put_scoreable_premium"]), 4)
        call_signed_premium = round(float(root["call_signed_premium"]), 4)
        put_signed_premium = round(float(root["put_signed_premium"]), 4)
        dominant_flow = "mixed"
        if call_premium > put_premium:
            dominant_flow = "call"
        elif put_premium > call_premium:
            dominant_flow = "put"
        dominant_flow_ratio = 0.0
        scoreable_premium_total = float(root["scoreable_premium"])
        if scoreable_premium_total > 0:
            dominant_flow_ratio = max(call_premium, put_premium) / scoreable_premium_total
        contracts_for_root = sorted(
            root["contracts"],
            key=lambda item: (
                -float(item["contract_score"]),
                -float(item["scoreable_premium"]),
                str(item["option_symbol"]),
            ),
        )
        expiry_premium: dict[str, float] = defaultdict(float)
        expiry_call_premium: dict[str, float] = defaultdict(float)
        expiry_put_premium: dict[str, float] = defaultdict(float)
        front_expiry: str | None = None
        front_expiry_dte: int | None = None
        atm_weighted_premium = 0.0
        oi_freshness_scores: list[float] = []
        for contract in contracts_for_root:
            contract_premium = float(contract.get("scoreable_premium") or 0.0)
            expiry = str(contract.get("expiration_date") or "").strip()
            if expiry:
                expiry_premium[expiry] += contract_premium
                if str(contract.get("option_type") or "") == "call":
                    expiry_call_premium[expiry] += contract_premium
                elif str(contract.get("option_type") or "") == "put":
                    expiry_put_premium[expiry] += contract_premium
            contract_dte = parse_int(contract.get("dte"))
            if expiry and contract_dte is not None and (
                front_expiry_dte is None or contract_dte < front_expiry_dte
            ):
                front_expiry = expiry
                front_expiry_dte = contract_dte
            atm_weighted_premium += contract_premium * float(
                contract.get("atm_relevance_score") or 0.0
            )
            freshness_score = _open_interest_freshness_score(
                parse_int(contract.get("open_interest_age_days"))
            )
            if freshness_score is not None:
                oi_freshness_scores.append(float(freshness_score))
        front_expiry_premium = 0.0 if front_expiry is None else expiry_premium[front_expiry]
        top_expiry = None
        top_expiry_premium = 0.0
        if expiry_premium:
            top_expiry, top_expiry_premium = max(
                expiry_premium.items(),
                key=lambda item: (float(item[1]), item[0]),
            )
        call_put_balance_score = _balance_score(
            call_premium,
            put_premium,
            total_value=max(scoreable_premium_total, 1.0),
        )
        same_expiry_symmetry_score = _balance_score(
            expiry_call_premium.get(front_expiry or "", 0.0),
            expiry_put_premium.get(front_expiry or "", 0.0),
            total_value=max(front_expiry_premium, 1.0),
        )
        atm_concentration_score = (
            0.0
            if scoreable_premium_total <= 0
            else round(clamp(atm_weighted_premium / scoreable_premium_total), 4)
        )
        front_expiry_concentration_score = (
            0.0
            if scoreable_premium_total <= 0
            else round(clamp(front_expiry_premium / scoreable_premium_total), 4)
        )
        open_interest_freshness_score = (
            None
            if not oi_freshness_scores
            else round(sum(oi_freshness_scores) / float(len(oi_freshness_scores)), 4)
        )
        positive_vega_share = (
            0.0
            if float(root["gross_vega_notional"]) <= 0
            else round(
                clamp(
                    max(float(root["signed_vega_notional"]), 0.0)
                    / float(root["gross_vega_notional"])
                ),
                4,
            )
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
            "buy_initiated_trade_count": int(root["buy_initiated_trade_count"]),
            "sell_initiated_trade_count": int(root["sell_initiated_trade_count"]),
            "unknown_initiated_trade_count": int(root["unknown_initiated_trade_count"]),
            "aggressor_known_trade_count": int(root["buy_initiated_trade_count"])
            + int(root["sell_initiated_trade_count"]),
            "aggressor_known_ratio": round(
                (
                    int(root["buy_initiated_trade_count"])
                    + int(root["sell_initiated_trade_count"])
                )
                / int(root["scoreable_trade_count"]),
                4,
            )
            if int(root["scoreable_trade_count"]) > 0
            else 0.0,
            "signed_trade_count": int(root["signed_trade_count"]),
            "signed_size": int(root["signed_size"]),
            "signed_premium": round(float(root["signed_premium"]), 4),
            "signed_delta_notional": round(float(root["signed_delta_notional"]), 4),
            "signed_vega_notional": round(float(root["signed_vega_notional"]), 4),
            "signed_gamma_dollar_exposure": round(
                float(root["signed_gamma_dollar_exposure"]), 4
            ),
            "gross_delta_notional": round(float(root["gross_delta_notional"]), 4),
            "gross_vega_notional": round(float(root["gross_vega_notional"]), 4),
            "gross_gamma_dollar_exposure": round(
                float(root["gross_gamma_dollar_exposure"]), 4
            ),
            "call_scoreable_premium": call_premium,
            "put_scoreable_premium": put_premium,
            "call_signed_premium": call_signed_premium,
            "put_signed_premium": put_signed_premium,
            "call_scoreable_trade_count": int(root["call_scoreable_trade_count"]),
            "put_scoreable_trade_count": int(root["put_scoreable_trade_count"]),
            "call_scoreable_contract_count": int(root["call_scoreable_contract_count"]),
            "put_scoreable_contract_count": int(root["put_scoreable_contract_count"]),
            "dominant_flow": dominant_flow,
            "dominant_flow_ratio": round(dominant_flow_ratio, 4),
            "call_put_balance_score": call_put_balance_score,
            "atm_concentration_score": atm_concentration_score,
            "same_expiry_symmetry_score": same_expiry_symmetry_score,
            "front_expiry_concentration_score": front_expiry_concentration_score,
            "positive_vega_share": positive_vega_share,
            "open_interest_freshness_score": open_interest_freshness_score,
            "front_expiry": front_expiry,
            "front_expiry_dte": front_expiry_dte,
            "top_expiry": top_expiry,
            "top_expiry_premium_share": (
                0.0
                if scoreable_premium_total <= 0
                else round(clamp(top_expiry_premium / scoreable_premium_total), 4)
            ),
            "supporting_volume": sum(int(contract.get("volume") or 0) for contract in contracts_for_root),
            "supporting_open_interest": sum(int(contract.get("open_interest") or 0) for contract in contracts_for_root),
            "max_volume_oi_ratio": max(
                (float(contract.get("volume_oi_ratio") or 0.0) for contract in contracts_for_root),
                default=0.0,
            ),
            "support_score_inputs": {
                "call_put_balance_score": call_put_balance_score,
                "atm_concentration_score": atm_concentration_score,
                "same_expiry_symmetry_score": same_expiry_symmetry_score,
                "front_expiry_concentration_score": front_expiry_concentration_score,
                "positive_vega_share": positive_vega_share,
                "open_interest_freshness_score": open_interest_freshness_score,
            },
            "top_contracts": [
                _root_top_contract_preview(contract)
                for contract in contracts_for_root[:TOP_CONTRACT_PREVIEW_LIMIT]
            ],
        }
        summary["supporting_volume_oi_ratio"] = _volume_oi_ratio(
            volume=int(summary["supporting_volume"]),
            open_interest=int(summary["supporting_open_interest"]),
        )
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
    aggressor_known_trade_count = sum(
        int(contract.get("aggressor_known_trade_count") or 0)
        for contract in contract_summaries
    )
    aggressor_unknown_trade_count = sum(
        int(contract.get("unknown_initiated_trade_count") or 0)
        for contract in contract_summaries
    )
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
        "aggressor_known_trade_count": aggressor_known_trade_count,
        "aggressor_unknown_trade_count": aggressor_unknown_trade_count,
        "aggressor_known_ratio": round(
            aggressor_known_trade_count / scoreable_trade_count,
            4,
        )
        if scoreable_trade_count > 0
        else 0.0,
        "raw_premium": round(sum(float(contract["raw_premium"]) for contract in contract_summaries), 4),
        "scoreable_premium": round(sum(float(contract["scoreable_premium"]) for contract in contract_summaries), 4),
        "excluded_premium": round(sum(float(contract["excluded_premium"]) for contract in contract_summaries), 4),
        "signed_premium_total": round(
            sum(float(contract.get("signed_premium") or 0.0) for contract in contract_summaries),
            4,
        ),
        "signed_delta_notional_total": round(
            sum(
                float(contract.get("signed_delta_notional") or 0.0)
                for contract in contract_summaries
            ),
            4,
        ),
        "signed_vega_notional_total": round(
            sum(
                float(contract.get("signed_vega_notional") or 0.0)
                for contract in contract_summaries
            ),
            4,
        ),
        "signed_gamma_dollar_total": round(
            sum(
                float(contract.get("signed_gamma_dollar_exposure") or 0.0)
                for contract in contract_summaries
            ),
            4,
        ),
        "front_expiry_root_count": sum(1 for root in root_summaries if root.get("front_expiry")),
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

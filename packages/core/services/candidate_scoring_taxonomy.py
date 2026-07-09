from __future__ import annotations

TOP_TIER_ETF_SYMBOLS = {"SPY", "QQQ", "IWM", "DIA", "GLD", "TLT"}
BROAD_ETF_SYMBOLS = {"XLF", "XLE", "XLI", "XLV"}
CASH_SETTLED_INDEX_SYMBOLS = {"SPX", "XSP", "NDX", "RUT", "VIX"}

PROFILE_TO_STYLE = {
    "0dte": "reactive",
    "weekly": "tactical",
    "core": "carry",
}

HORIZON_BANDS = (
    ("same_day", 0, 0, "daily"),
    ("next_daily", 1, 2, "daily"),
    ("near_term", 3, 12, "weekly"),
    ("post_event", 13, 20, "post_event"),
    ("swing", 21, 45, "weekly"),
    ("carry", 46, 120, "monthly"),
)
LONG_VOL_FAMILIES = {"long_straddle", "long_strangle"}
DIRECTIONAL_DEBIT_FAMILIES = {
    "call_debit_spread",
    "put_debit_spread",
    "long_call",
    "long_put",
}
SHORT_PREMIUM_FAMILIES = {
    "call_credit_spread",
    "put_credit_spread",
    "iron_condor",
    "short_call",
    "short_put",
}
DEFINED_RISK_SHORT_PREMIUM_FAMILIES = {
    "call_credit_spread",
    "put_credit_spread",
    "iron_condor",
}
POST_EVENT_SHORT_PREMIUM_SIGNAL_FAMILIES = {
    "call_credit_spread",
    "put_credit_spread",
    "short_call",
    "short_put",
}


def resolve_style_profile(
    profile: str | None,
    *,
    days_to_expiration: int | None,
) -> str:
    normalized = str(profile or "").strip().lower()
    if normalized in PROFILE_TO_STYLE:
        return PROFILE_TO_STYLE[normalized]
    if days_to_expiration == 0:
        return "reactive"
    if days_to_expiration is not None and days_to_expiration <= 12:
        return "tactical"
    return "carry"


def strategy_family(strategy: str | None) -> str:
    normalized = str(strategy or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
        "long_straddle": "long_straddle",
        "long_strangle": "long_strangle",
        "long_call": "long_call",
        "long_put": "long_put",
        "short_call": "short_call",
        "short_put": "short_put",
        "iron_condor": "iron_condor",
    }.get(normalized, normalized or "unknown")


def product_class(symbol: str) -> str:
    if symbol in CASH_SETTLED_INDEX_SYMBOLS:
        return "cash_settled_index"
    if symbol in TOP_TIER_ETF_SYMBOLS:
        return "top_tier_etf"
    if symbol in BROAD_ETF_SYMBOLS:
        return "broad_etf"
    return "single_name_equity"


def horizon_band(days_to_expiration: int | None) -> tuple[str, int, int, str]:
    if days_to_expiration is None:
        return ("near_term", 3, 12, "weekly")
    for band, lower, upper, expiration_type in HORIZON_BANDS:
        if lower <= days_to_expiration <= upper:
            return band, lower, upper, expiration_type
    if days_to_expiration < 0:
        return ("same_day", 0, 0, "daily")
    return ("carry", 46, max(days_to_expiration, 46), "monthly")

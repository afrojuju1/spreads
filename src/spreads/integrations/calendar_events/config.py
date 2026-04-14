from __future__ import annotations

from pathlib import Path

DEFAULT_MACRO_CALENDAR_PATH = Path("data") / "macro_calendar.json"
MACRO_ASSET_SCOPE = "etf_index_proxy"

ETF_INDEX_PROXY_SYMBOLS = {
    "DIA",
    "EEM",
    "EWZ",
    "FXI",
    "GLD",
    "GOVT",
    "HYG",
    "IAU",
    "IEF",
    "IWM",
    "KRE",
    "QQQ",
    "SCHD",
    "SLV",
    "SMH",
    "SPY",
    "TLT",
    "UNG",
    "USO",
    "VEA",
    "VTI",
    "XBI",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLU",
    "XLV",
    "XLY",
}

SOURCE_FRESHNESS_HOURS = {
    "dolt_earnings_calendar": 24,
    "alpaca_corporate_actions": 24,
    "macro_calendar": 0,
}

EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS = 15
EARNINGS_POST_EVENT_FRESH_DAYS = 3
EARNINGS_POST_EVENT_SETTLED_DAYS = 15

SOURCE_CONFIDENCE_RANK = {
    "unknown": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}

DIVIDEND_EVENT_TYPES = {"cash_dividend", "stock_dividend"}
BLOCKING_EVENT_CODES = {"earnings_before_expiry", "ex_dividend_before_expiry"}
PENALTY_EVENT_CODES = {"corporate_action_before_expiry", "macro_event_in_window"}
OPTIONAL_SOURCE_WARNING_CODES = {"macro_calendar_unavailable"}

REQUIRED_SOURCES_BY_UNDERLYING = {
    "single_name_equity": {"dolt_earnings_calendar", "alpaca_corporate_actions"},
    "etf_index_proxy": {"alpaca_corporate_actions"},
}

OPTIONAL_SOURCES_BY_UNDERLYING = {
    "single_name_equity": set(),
    "etf_index_proxy": {"macro_calendar"},
}

ALPACA_CORPORATE_ACTION_TYPES = (
    "cash_dividend",
    "stock_dividend",
    "forward_split",
    "reverse_split",
    "unit_split",
    "spin_off",
    "cash_merger",
    "stock_merger",
    "stock_and_cash_merger",
    "redemption",
    "name_change",
    "worthless_removal",
    "rights_distribution",
)


def classify_underlying_type(symbol: str) -> str:
    if symbol.upper() in ETF_INDEX_PROXY_SYMBOLS:
        return MACRO_ASSET_SCOPE
    return "single_name_equity"

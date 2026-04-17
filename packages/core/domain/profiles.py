from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time

from core.services.market_dates import NEW_YORK

DEFAULT_BOARD_UNIVERSE = "etf_core"
ZERO_DTE_CORE_SYMBOLS = ("SPY", "QQQ", "IWM")
ZERO_DTE_ALLOWED_SYMBOLS = (
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "XLF",
    "XLE",
    "XLI",
    "XLV",
    "GLD",
    "TLT",
)
UNIVERSE_PRESETS: dict[str, tuple[str, ...]] = {
    "0dte_core": ZERO_DTE_CORE_SYMBOLS,
    "explore_10": ZERO_DTE_ALLOWED_SYMBOLS,
    "etf_core": ("SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "SMH"),
    "liquid_stocks": ("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "AMD", "TSLA"),
    "liquid_mixed": (
        "SPY",
        "QQQ",
        "IWM",
        "SMH",
        "XLK",
        "XLF",
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "AMD",
    ),
}
LONG_VOL_STRATEGIES = frozenset({"long_straddle", "long_strangle"})


@dataclass(frozen=True)
class ProfileConfig:
    name: str
    min_dte: int
    max_dte: int
    short_delta_min: float
    short_delta_max: float
    short_delta_target: float
    min_width: float
    max_width_by_underlying: dict[str, float]
    min_credit: float
    min_open_interest_by_underlying: dict[str, int]
    max_relative_spread_by_underlying: dict[str, float]
    min_return_on_risk: float
    min_fill_ratio: float
    min_short_vs_expected_move_ratio: float
    min_breakeven_vs_expected_move_ratio: float


PROFILE_CONFIGS: dict[str, ProfileConfig] = {
    "0dte": ProfileConfig(
        name="0dte",
        min_dte=0,
        max_dte=0,
        short_delta_min=0.03,
        short_delta_max=0.18,
        short_delta_target=0.10,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 2.0, "single_name_equity": 2.0},
        min_credit=0.08,
        min_open_interest_by_underlying={
            "etf_index_proxy": 750,
            "single_name_equity": 750,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.08,
            "single_name_equity": 0.08,
        },
        min_return_on_risk=0.05,
        min_fill_ratio=0.80,
        min_short_vs_expected_move_ratio=0.08,
        min_breakeven_vs_expected_move_ratio=0.03,
    ),
    "micro": ProfileConfig(
        name="micro",
        min_dte=1,
        max_dte=3,
        short_delta_min=0.05,
        short_delta_max=0.12,
        short_delta_target=0.08,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 2.0, "single_name_equity": 2.0},
        min_credit=0.10,
        min_open_interest_by_underlying={
            "etf_index_proxy": 1500,
            "single_name_equity": 1500,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.10,
            "single_name_equity": 0.10,
        },
        min_return_on_risk=0.08,
        min_fill_ratio=0.75,
        min_short_vs_expected_move_ratio=0.05,
        min_breakeven_vs_expected_move_ratio=0.00,
    ),
    "weekly": ProfileConfig(
        name="weekly",
        min_dte=4,
        max_dte=10,
        short_delta_min=0.08,
        short_delta_max=0.16,
        short_delta_target=0.12,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 3.0, "single_name_equity": 5.0},
        min_credit=0.18,
        min_open_interest_by_underlying={
            "etf_index_proxy": 500,
            "single_name_equity": 400,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.12,
            "single_name_equity": 0.15,
        },
        min_return_on_risk=0.10,
        min_fill_ratio=0.72,
        min_short_vs_expected_move_ratio=-0.05,
        min_breakeven_vs_expected_move_ratio=-0.02,
    ),
    "swing": ProfileConfig(
        name="swing",
        min_dte=11,
        max_dte=21,
        short_delta_min=0.12,
        short_delta_max=0.20,
        short_delta_target=0.16,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 5.0, "single_name_equity": 10.0},
        min_credit=0.25,
        min_open_interest_by_underlying={
            "etf_index_proxy": 500,
            "single_name_equity": 250,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.18,
            "single_name_equity": 0.18,
        },
        min_return_on_risk=0.10,
        min_fill_ratio=0.70,
        min_short_vs_expected_move_ratio=-0.08,
        min_breakeven_vs_expected_move_ratio=-0.04,
    ),
    "core": ProfileConfig(
        name="core",
        min_dte=22,
        max_dte=35,
        short_delta_min=0.15,
        short_delta_max=0.22,
        short_delta_target=0.18,
        min_width=2.0,
        max_width_by_underlying={"etf_index_proxy": 10.0, "single_name_equity": 10.0},
        min_credit=0.35,
        min_open_interest_by_underlying={
            "etf_index_proxy": 300,
            "single_name_equity": 200,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.20,
            "single_name_equity": 0.20,
        },
        min_return_on_risk=0.12,
        min_fill_ratio=0.68,
        min_short_vs_expected_move_ratio=-0.10,
        min_breakeven_vs_expected_move_ratio=-0.05,
    ),
}


def zero_dte_session_bucket(now: datetime | None = None) -> str:
    current = datetime.now(NEW_YORK) if now is None else now.astimezone(NEW_YORK)
    current_time = current.time()
    if current_time < time(9, 30) or current_time >= time(16, 0):
        return "off_hours"
    if current_time < time(10, 30):
        return "open"
    if current_time < time(13, 30):
        return "midday"
    return "late"


def format_session_bucket(bucket: str) -> str:
    return bucket.replace("_", "-")


def zero_dte_delta_target(session_bucket: str) -> float:
    return {
        "open": 0.08,
        "midday": 0.10,
        "late": 0.12,
        "off_hours": 0.10,
    }[session_bucket]


__all__ = [
    "DEFAULT_BOARD_UNIVERSE",
    "LONG_VOL_STRATEGIES",
    "PROFILE_CONFIGS",
    "ProfileConfig",
    "UNIVERSE_PRESETS",
    "ZERO_DTE_ALLOWED_SYMBOLS",
    "ZERO_DTE_CORE_SYMBOLS",
    "format_session_bucket",
    "zero_dte_delta_target",
    "zero_dte_session_bucket",
]

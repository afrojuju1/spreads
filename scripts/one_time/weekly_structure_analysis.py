#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import json
import math
import sys
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from statistics import mean, median
from typing import Any
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ROOT / "packages"
if str(PACKAGES) not in sys.path:
    sys.path.insert(0, str(PACKAGES))

from core.services.alpaca import create_alpaca_client_from_env  # noqa: E402
from core.services.scanner import AlpacaClient, DailyBar  # noqa: E402

NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class WeeklyPeriod:
    week_start: str
    week_end: str
    first_day: str
    first_day_weekday: str
    last_day: str
    first_open: float
    first_close: float
    last_close: float
    first_day_intraday_pct: float
    rest_of_week_pct: float
    full_week_pct: float
    weekend_gap_into_week_pct: float | None


@dataclass(frozen=True)
class DayOfWeekProfile:
    weekday: str
    sessions: int
    avg_intraday_pct: float | None
    median_intraday_pct: float | None
    intraday_win_rate_pct: float | None
    gap_samples: int
    avg_gap_into_open_pct: float | None
    median_gap_into_open_pct: float | None
    gap_win_rate_pct: float | None


@dataclass(frozen=True)
class ConditionProfile:
    label: str
    weeks: int
    avg_rest_of_week_pct: float | None
    median_rest_of_week_pct: float | None
    positive_rest_of_week_rate_pct: float | None
    avg_full_week_pct: float | None


@dataclass(frozen=True)
class MondaySignalProfile:
    monday_weeks: int
    monday_vs_tue_fri_corr: float | None
    avg_tue_fri_after_red_monday_pct: float | None
    positive_tue_fri_after_red_monday_rate_pct: float | None
    avg_tue_fri_after_green_monday_pct: float | None
    positive_tue_fri_after_green_monday_rate_pct: float | None


@dataclass(frozen=True)
class RollingWindowProfile:
    window_months: int
    windows: int
    avg_buy_and_hold_return_pct: float | None
    median_buy_and_hold_return_pct: float | None
    avg_intraday_return_pct: float | None
    avg_total_gap_return_pct: float | None
    gap_dominant_window_rate_pct: float | None
    positive_weekend_gap_window_rate_pct: float | None


@dataclass(frozen=True)
class RegimeProfile:
    regime_type: str
    label: str
    weeks: int
    avg_full_week_pct: float | None
    positive_full_week_rate_pct: float | None
    avg_rest_of_week_pct: float | None


@dataclass(frozen=True)
class WindowCore:
    first_included_day: date | None
    last_included_day: date | None
    filtered_dates: list[date]
    filtered_bars: list[DailyBar]
    weekly_periods: list[WeeklyPeriod]
    trading_days: int
    full_weeks: int
    buy_and_hold_return_pct: float | None
    intraday_return_pct: float | None
    total_gap_return_pct: float | None
    weekend_gap_return_pct: float | None
    within_week_gap_return_pct: float | None


@dataclass(frozen=True)
class SymbolAnalysisResult:
    symbol: str
    requested_start: str
    requested_end: str
    first_included_day: str | None
    last_included_day: str | None
    trading_days: int
    full_weeks: int
    buy_and_hold_return_pct: float | None
    intraday_return_pct: float | None
    total_gap_return_pct: float | None
    weekend_gap_return_pct: float | None
    within_week_gap_return_pct: float | None
    avg_full_week_pct: float | None
    median_full_week_pct: float | None
    full_week_win_rate_pct: float | None
    avg_first_day_intraday_pct: float | None
    median_first_day_intraday_pct: float | None
    avg_rest_of_week_pct: float | None
    median_rest_of_week_pct: float | None
    first_day_vs_rest_corr: float | None
    after_red_first_day: ConditionProfile
    after_green_first_day: ConditionProfile
    monday_signal: MondaySignalProfile
    rolling_windows: list[RollingWindowProfile]
    drawdown_regimes: list[RegimeProfile]
    volatility_regimes: list[RegimeProfile]
    weekday_profiles: list[DayOfWeekProfile]
    weekly_periods: list[WeeklyPeriod]
    possible_insights: list[str]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze weekly return structure: where returns occur, how the first trading day relates "
            "to the rest of the week, and whether gaps or intraday moves dominate."
        )
    )
    parser.add_argument(
        "--symbols",
        default="SPY,QQQ",
        help="Comma-separated stock or ETF symbols. Default: SPY,QQQ",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Calendar months to look back from --end-date. Default: 6",
    )
    parser.add_argument(
        "--end-date",
        default="today",
        help="Window end date in YYYY-MM-DD using the New York trading date. Default: today",
    )
    parser.add_argument(
        "--stock-feed",
        default="sip",
        choices=("sip", "iex", "delayed_sip", "boats", "overnight"),
        help="Alpaca stock feed to use. Default: sip",
    )
    parser.add_argument(
        "--show-weeks",
        type=int,
        default=5,
        help="Number of sample weekly periods to print per symbol in text mode. Default: 5",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit full JSON output instead of a text summary.",
    )
    return parser.parse_args(argv)


def resolve_end_date(raw: str) -> date:
    if raw == "today":
        return datetime.now(NEW_YORK).date()
    return date.fromisoformat(raw)


def subtract_months(value: date, months: int) -> date:
    if months < 0:
        raise ValueError("months must be non-negative")
    year = value.year
    month = value.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def start_of_week(value: date) -> date:
    return value - timedelta(days=value.weekday())


def end_of_week(value: date) -> date:
    return start_of_week(value) + timedelta(days=6)


def parse_bar_date(bar: DailyBar) -> date:
    timestamp = bar.timestamp
    if timestamp.endswith("Z"):
        timestamp = timestamp.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(timestamp)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=NEW_YORK)
    return parsed.astimezone(NEW_YORK).date()


def normalize_symbols(raw: str) -> list[str]:
    symbols: list[str] = []
    for item in raw.split(","):
        symbol = item.strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise SystemExit("At least one symbol is required")
    return symbols


def factor_to_pct(factor: float) -> float:
    return (factor - 1.0) * 100.0


def format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}%"


def safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return mean(values)


def safe_median(values: list[float]) -> float | None:
    if not values:
        return None
    return median(values)


def win_rate_pct(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(1 for value in values if value > 0.0) / len(values) * 100.0


def safe_correlation(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mean_x = mean(xs)
    mean_y = mean(ys)
    centered = [(x - mean_x, y - mean_y) for x, y in zip(xs, ys)]
    variance_x = sum(dx * dx for dx, _ in centered)
    variance_y = sum(dy * dy for _, dy in centered)
    if variance_x <= 0.0 or variance_y <= 0.0:
        return None
    covariance = sum(dx * dy for dx, dy in centered)
    return covariance / math.sqrt(variance_x * variance_y)


@lru_cache(maxsize=None)
def build_eligible_weeks(
    *, requested_start: date, requested_end: date
) -> list[tuple[date, date]]:
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(
        start_date=start_of_week(requested_start).isoformat(),
        end_date=end_of_week(requested_end).isoformat(),
    )
    by_week: dict[date, list[date]] = {}
    for session in schedule.index:
        session_date = session.date()
        by_week.setdefault(start_of_week(session_date), []).append(session_date)

    eligible_weeks: list[tuple[date, date]] = []
    for week_start in sorted(by_week):
        session_dates = by_week[week_start]
        first_session = session_dates[0]
        last_session = session_dates[-1]
        if first_session < requested_start:
            continue
        if last_session > requested_end:
            continue
        eligible_weeks.append((first_session, last_session))
    return eligible_weeks


def build_condition_profile(
    label: str, periods: list[WeeklyPeriod]
) -> ConditionProfile:
    rest = [period.rest_of_week_pct for period in periods]
    full = [period.full_week_pct for period in periods]
    return ConditionProfile(
        label=label,
        weeks=len(periods),
        avg_rest_of_week_pct=safe_mean(rest),
        median_rest_of_week_pct=safe_median(rest),
        positive_rest_of_week_rate_pct=win_rate_pct(rest),
        avg_full_week_pct=safe_mean(full),
    )


def build_regime_profile(
    *, regime_type: str, label: str, periods: list[WeeklyPeriod]
) -> RegimeProfile:
    full = [period.full_week_pct for period in periods]
    rest = [period.rest_of_week_pct for period in periods]
    return RegimeProfile(
        regime_type=regime_type,
        label=label,
        weeks=len(periods),
        avg_full_week_pct=safe_mean(full),
        positive_full_week_rate_pct=win_rate_pct(full),
        avg_rest_of_week_pct=safe_mean(rest),
    )


def build_monday_signal_profile(periods: list[WeeklyPeriod]) -> MondaySignalProfile:
    monday_periods = [
        period
        for period in periods
        if date.fromisoformat(period.first_day).weekday() == 0
    ]
    rest = [period.rest_of_week_pct for period in monday_periods]
    red_rest = [
        period.rest_of_week_pct
        for period in monday_periods
        if period.first_day_intraday_pct < 0.0
    ]
    green_rest = [
        period.rest_of_week_pct
        for period in monday_periods
        if period.first_day_intraday_pct > 0.0
    ]
    return MondaySignalProfile(
        monday_weeks=len(monday_periods),
        monday_vs_tue_fri_corr=safe_correlation(
            [period.first_day_intraday_pct for period in monday_periods],
            rest,
        ),
        avg_tue_fri_after_red_monday_pct=safe_mean(red_rest),
        positive_tue_fri_after_red_monday_rate_pct=win_rate_pct(red_rest),
        avg_tue_fri_after_green_monday_pct=safe_mean(green_rest),
        positive_tue_fri_after_green_monday_rate_pct=win_rate_pct(green_rest),
    )


def build_window_core(
    *,
    bars_by_date: dict[date, DailyBar],
    requested_start: date,
    requested_end: date,
) -> WindowCore | None:
    eligible_weeks = build_eligible_weeks(
        requested_start=requested_start,
        requested_end=requested_end,
    )
    weekly_periods: list[WeeklyPeriod] = []
    previous_week_last_close: float | None = None

    for first_day, last_day in eligible_weeks:
        first_bar = bars_by_date.get(first_day)
        last_bar = bars_by_date.get(last_day)
        if first_bar is None or last_bar is None:
            continue

        weekend_gap_into_week_pct = None
        if previous_week_last_close is not None:
            weekend_gap_into_week_pct = (
                first_bar.open / previous_week_last_close - 1.0
            ) * 100.0

        weekly_periods.append(
            WeeklyPeriod(
                week_start=start_of_week(first_day).isoformat(),
                week_end=end_of_week(first_day).isoformat(),
                first_day=first_day.isoformat(),
                first_day_weekday=calendar.day_name[first_day.weekday()],
                last_day=last_day.isoformat(),
                first_open=first_bar.open,
                first_close=first_bar.close,
                last_close=last_bar.close,
                first_day_intraday_pct=(first_bar.close / first_bar.open - 1.0) * 100.0,
                rest_of_week_pct=(last_bar.close / first_bar.close - 1.0) * 100.0,
                full_week_pct=(last_bar.close / first_bar.open - 1.0) * 100.0,
                weekend_gap_into_week_pct=weekend_gap_into_week_pct,
            )
        )
        previous_week_last_close = last_bar.close

    if not weekly_periods:
        return None

    first_included_day = date.fromisoformat(weekly_periods[0].first_day)
    last_included_day = date.fromisoformat(weekly_periods[-1].last_day)
    filtered_dates = [
        session_date
        for session_date in sorted(bars_by_date)
        if first_included_day <= session_date <= last_included_day
    ]
    filtered_bars = [bars_by_date[session_date] for session_date in filtered_dates]

    intraday_factor = 1.0
    for bar in filtered_bars:
        intraday_factor *= bar.close / bar.open

    total_gap_factor = 1.0
    for previous_bar, current_bar in zip(filtered_bars, filtered_bars[1:]):
        total_gap_factor *= current_bar.open / previous_bar.close

    weekend_gap_factor = 1.0
    for period in weekly_periods[1:]:
        if period.weekend_gap_into_week_pct is None:
            continue
        weekend_gap_factor *= 1.0 + (period.weekend_gap_into_week_pct / 100.0)

    within_week_gap_factor = (
        total_gap_factor / weekend_gap_factor if weekend_gap_factor != 0.0 else 1.0
    )
    buy_and_hold_factor = filtered_bars[-1].close / filtered_bars[0].open

    return WindowCore(
        first_included_day=first_included_day,
        last_included_day=last_included_day,
        filtered_dates=filtered_dates,
        filtered_bars=filtered_bars,
        weekly_periods=weekly_periods,
        trading_days=len(filtered_bars),
        full_weeks=len(weekly_periods),
        buy_and_hold_return_pct=factor_to_pct(buy_and_hold_factor),
        intraday_return_pct=factor_to_pct(intraday_factor),
        total_gap_return_pct=factor_to_pct(total_gap_factor),
        weekend_gap_return_pct=factor_to_pct(weekend_gap_factor),
        within_week_gap_return_pct=factor_to_pct(within_week_gap_factor),
    )


def build_weekday_profiles(
    *,
    daily_bars: list[DailyBar],
    daily_dates: list[date],
) -> list[DayOfWeekProfile]:
    intraday_by_weekday: dict[int, list[float]] = {index: [] for index in range(5)}
    gap_by_weekday: dict[int, list[float]] = {index: [] for index in range(5)}

    for index, (bar, session_date) in enumerate(zip(daily_bars, daily_dates)):
        intraday_by_weekday[session_date.weekday()].append(
            (bar.close / bar.open - 1.0) * 100.0
        )
        if index == 0:
            continue
        previous_bar = daily_bars[index - 1]
        gap_by_weekday[session_date.weekday()].append(
            (bar.open / previous_bar.close - 1.0) * 100.0
        )

    profiles: list[DayOfWeekProfile] = []
    for weekday_index in range(5):
        intraday = intraday_by_weekday[weekday_index]
        gaps = gap_by_weekday[weekday_index]
        profiles.append(
            DayOfWeekProfile(
                weekday=calendar.day_name[weekday_index],
                sessions=len(intraday),
                avg_intraday_pct=safe_mean(intraday),
                median_intraday_pct=safe_median(intraday),
                intraday_win_rate_pct=win_rate_pct(intraday),
                gap_samples=len(gaps),
                avg_gap_into_open_pct=safe_mean(gaps),
                median_gap_into_open_pct=safe_median(gaps),
                gap_win_rate_pct=win_rate_pct(gaps),
            )
        )
    return profiles


def build_rolling_window_profiles(
    *,
    bars_by_date: dict[date, DailyBar],
    full_sample_start: date,
    full_sample_end: date,
    weekly_periods: list[WeeklyPeriod],
    window_months_list: list[int],
) -> list[RollingWindowProfile]:
    profiles: list[RollingWindowProfile] = []
    for window_months in window_months_list:
        window_cores: list[WindowCore] = []
        for period in weekly_periods:
            window_end = date.fromisoformat(period.last_day)
            window_start = subtract_months(window_end, window_months)
            if window_start < full_sample_start or window_end > full_sample_end:
                continue
            core = build_window_core(
                bars_by_date=bars_by_date,
                requested_start=window_start,
                requested_end=window_end,
            )
            if core is None:
                continue
            window_cores.append(core)

        profiles.append(
            RollingWindowProfile(
                window_months=window_months,
                windows=len(window_cores),
                avg_buy_and_hold_return_pct=safe_mean(
                    [
                        core.buy_and_hold_return_pct
                        for core in window_cores
                        if core.buy_and_hold_return_pct is not None
                    ]
                ),
                median_buy_and_hold_return_pct=safe_median(
                    [
                        core.buy_and_hold_return_pct
                        for core in window_cores
                        if core.buy_and_hold_return_pct is not None
                    ]
                ),
                avg_intraday_return_pct=safe_mean(
                    [
                        core.intraday_return_pct
                        for core in window_cores
                        if core.intraday_return_pct is not None
                    ]
                ),
                avg_total_gap_return_pct=safe_mean(
                    [
                        core.total_gap_return_pct
                        for core in window_cores
                        if core.total_gap_return_pct is not None
                    ]
                ),
                gap_dominant_window_rate_pct=win_rate_pct(
                    [
                        1.0
                        if (
                            core.total_gap_return_pct is not None
                            and core.intraday_return_pct is not None
                            and core.total_gap_return_pct > core.intraday_return_pct
                        )
                        else -1.0
                        for core in window_cores
                    ]
                ),
                positive_weekend_gap_window_rate_pct=win_rate_pct(
                    [
                        1.0
                        if core.weekend_gap_return_pct is not None
                        and core.weekend_gap_return_pct > 0.0
                        else -1.0
                        for core in window_cores
                    ]
                ),
            )
        )
    return profiles


def build_regime_profiles(
    *,
    bars_by_date: dict[date, DailyBar],
    weekly_periods: list[WeeklyPeriod],
) -> tuple[list[RegimeProfile], list[RegimeProfile]]:
    ordered_dates = sorted(bars_by_date)
    date_to_index = {
        session_date: index for index, session_date in enumerate(ordered_dates)
    }
    trailing_vol_by_week_start: dict[str, float] = {}
    drawdown_state_by_week_start: dict[str, bool] = {}
    volatility_samples: list[float] = []

    for period in weekly_periods:
        first_day = date.fromisoformat(period.first_day)
        current_index = date_to_index.get(first_day)
        if current_index is None or current_index == 0:
            continue

        prior_dates = ordered_dates[:current_index]
        prior_close = bars_by_date[prior_dates[-1]].close

        trailing_high_window = prior_dates[-252:] if len(prior_dates) >= 1 else []
        if trailing_high_window:
            trailing_high = max(
                bars_by_date[session_date].close
                for session_date in trailing_high_window
            )
            drawdown_state_by_week_start[period.first_day] = (
                prior_close / trailing_high - 1.0 <= -0.10
            )

        trailing_return_window = prior_dates[-21:]
        if len(trailing_return_window) >= 21:
            closes = [
                bars_by_date[session_date].close
                for session_date in trailing_return_window
            ]
            close_returns = [
                (current / previous) - 1.0
                for previous, current in zip(closes, closes[1:])
            ]
            if len(close_returns) >= 2:
                mean_return = mean(close_returns)
                variance = sum(
                    (value - mean_return) ** 2 for value in close_returns
                ) / (len(close_returns) - 1)
                realized_vol = math.sqrt(variance) * math.sqrt(252.0) * 100.0
                trailing_vol_by_week_start[period.first_day] = realized_vol
                volatility_samples.append(realized_vol)

    volatility_cutoff = median(volatility_samples) if volatility_samples else None

    drawdown_periods = [
        period
        for period in weekly_periods
        if drawdown_state_by_week_start.get(period.first_day) is True
    ]
    non_drawdown_periods = [
        period
        for period in weekly_periods
        if drawdown_state_by_week_start.get(period.first_day) is False
    ]

    high_vol_periods = []
    low_vol_periods = []
    if volatility_cutoff is not None:
        for period in weekly_periods:
            realized_vol = trailing_vol_by_week_start.get(period.first_day)
            if realized_vol is None:
                continue
            if realized_vol >= volatility_cutoff:
                high_vol_periods.append(period)
            else:
                low_vol_periods.append(period)

    drawdown_profiles = [
        build_regime_profile(
            regime_type="drawdown", label="drawdown>=10%", periods=drawdown_periods
        ),
        build_regime_profile(
            regime_type="drawdown", label="drawdown<10%", periods=non_drawdown_periods
        ),
    ]
    volatility_profiles = [
        build_regime_profile(
            regime_type="volatility", label="high_vol", periods=high_vol_periods
        ),
        build_regime_profile(
            regime_type="volatility", label="low_vol", periods=low_vol_periods
        ),
    ]
    return drawdown_profiles, volatility_profiles


def build_possible_insights(
    *,
    symbol: str,
    buy_and_hold_return_pct: float,
    intraday_return_pct: float,
    total_gap_return_pct: float,
    weekend_gap_return_pct: float,
    avg_first_day_intraday_pct: float | None,
    avg_rest_of_week_pct: float | None,
    corr: float | None,
    after_red_first_day: ConditionProfile,
    after_green_first_day: ConditionProfile,
    weekday_profiles: list[DayOfWeekProfile],
) -> list[str]:
    insights: list[str] = []

    if (
        buy_and_hold_return_pct > 0
        and intraday_return_pct < 0
        and total_gap_return_pct > buy_and_hold_return_pct
    ):
        insights.append(
            f"{symbol} was net positive only because non-session gaps offset negative intraday drift."
        )
    elif abs(total_gap_return_pct) > abs(intraday_return_pct):
        insights.append(
            f"{symbol}'s move was driven more by gaps ({total_gap_return_pct:.2f}%) than by intraday sessions ({intraday_return_pct:.2f}%)."
        )
    else:
        insights.append(
            f"{symbol}'s move was driven more by intraday sessions ({intraday_return_pct:.2f}%) than by gaps ({total_gap_return_pct:.2f}%)."
        )

    if weekend_gap_return_pct > 0:
        insights.append(
            f"Weekend gaps added {weekend_gap_return_pct:.2f}% over the sample, which matters because a first-day/last-day weekly trade skips that return stream."
        )
    elif weekend_gap_return_pct < 0:
        insights.append(
            f"Weekend gaps subtracted {abs(weekend_gap_return_pct):.2f}% over the sample, so staying flat over weekends would have helped."
        )

    if after_red_first_day.weeks > 0 and after_green_first_day.weeks > 0:
        red_avg = after_red_first_day.avg_rest_of_week_pct or 0.0
        green_avg = after_green_first_day.avg_rest_of_week_pct or 0.0
        diff = red_avg - green_avg
        if diff >= 0.35:
            insights.append(
                f"Rest-of-week performance was stronger after a red first day ({red_avg:.2f}%) than after a green one ({green_avg:.2f}%), which suggests short-horizon mean reversion."
            )
        elif diff <= -0.35:
            insights.append(
                f"Rest-of-week performance was stronger after a green first day ({green_avg:.2f}%) than after a red one ({red_avg:.2f}%), which suggests continuation."
            )

    if corr is not None:
        if corr <= -0.25:
            insights.append(
                f"First-day intraday return and the rest of the week were negatively correlated ({corr:.2f}), another mean-reversion hint."
            )
        elif corr >= 0.25:
            insights.append(
                f"First-day intraday return and the rest of the week were positively correlated ({corr:.2f}), which leans toward continuation."
            )

    best_intraday_day = max(
        weekday_profiles,
        key=lambda profile: profile.avg_intraday_pct
        if profile.avg_intraday_pct is not None
        else float("-inf"),
    )
    if best_intraday_day.avg_intraday_pct is not None:
        insights.append(
            f"The strongest average intraday session was {best_intraday_day.weekday} ({best_intraday_day.avg_intraday_pct:.2f}%)."
        )

    best_gap_day = max(
        weekday_profiles,
        key=lambda profile: profile.avg_gap_into_open_pct
        if profile.avg_gap_into_open_pct is not None
        else float("-inf"),
    )
    if best_gap_day.avg_gap_into_open_pct is not None:
        insights.append(
            f"The strongest average gap into the open was on {best_gap_day.weekday} ({best_gap_day.avg_gap_into_open_pct:.2f}%)."
        )

    return insights[:5]


def analyze_symbol(
    *,
    client: AlpacaClient,
    symbol: str,
    requested_start: date,
    requested_end: date,
    stock_feed: str,
) -> SymbolAnalysisResult:
    bars = client.get_daily_bars(
        symbol,
        start=start_of_week(requested_start).isoformat(),
        end=requested_end.isoformat(),
        stock_feed=stock_feed,
    )
    bars.sort(key=parse_bar_date)
    bars_by_date = {parse_bar_date(bar): bar for bar in bars}
    core = build_window_core(
        bars_by_date=bars_by_date,
        requested_start=requested_start,
        requested_end=requested_end,
    )

    empty_monday = MondaySignalProfile(
        monday_weeks=0,
        monday_vs_tue_fri_corr=None,
        avg_tue_fri_after_red_monday_pct=None,
        positive_tue_fri_after_red_monday_rate_pct=None,
        avg_tue_fri_after_green_monday_pct=None,
        positive_tue_fri_after_green_monday_rate_pct=None,
    )

    if core is None:
        empty_bucket = ConditionProfile(
            label="empty",
            weeks=0,
            avg_rest_of_week_pct=None,
            median_rest_of_week_pct=None,
            positive_rest_of_week_rate_pct=None,
            avg_full_week_pct=None,
        )
        return SymbolAnalysisResult(
            symbol=symbol,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat(),
            first_included_day=None,
            last_included_day=None,
            trading_days=0,
            full_weeks=0,
            buy_and_hold_return_pct=None,
            intraday_return_pct=None,
            total_gap_return_pct=None,
            weekend_gap_return_pct=None,
            within_week_gap_return_pct=None,
            avg_full_week_pct=None,
            median_full_week_pct=None,
            full_week_win_rate_pct=None,
            avg_first_day_intraday_pct=None,
            median_first_day_intraday_pct=None,
            avg_rest_of_week_pct=None,
            median_rest_of_week_pct=None,
            first_day_vs_rest_corr=None,
            after_red_first_day=empty_bucket,
            after_green_first_day=empty_bucket,
            monday_signal=empty_monday,
            rolling_windows=[],
            drawdown_regimes=[],
            volatility_regimes=[],
            weekday_profiles=[],
            weekly_periods=[],
            possible_insights=[],
        )
    weekly_periods = core.weekly_periods
    filtered_dates = core.filtered_dates
    filtered_bars = core.filtered_bars
    full_week_returns = [period.full_week_pct for period in weekly_periods]
    first_day_intraday_returns = [
        period.first_day_intraday_pct for period in weekly_periods
    ]
    rest_of_week_returns = [period.rest_of_week_pct for period in weekly_periods]
    red_first_day_periods = [
        period for period in weekly_periods if period.first_day_intraday_pct < 0.0
    ]
    green_first_day_periods = [
        period for period in weekly_periods if period.first_day_intraday_pct > 0.0
    ]
    weekday_profiles = build_weekday_profiles(
        daily_bars=filtered_bars, daily_dates=filtered_dates
    )
    monday_signal = build_monday_signal_profile(weekly_periods)
    rolling_windows = build_rolling_window_profiles(
        bars_by_date=bars_by_date,
        full_sample_start=requested_start,
        full_sample_end=requested_end,
        weekly_periods=weekly_periods,
        window_months_list=[6, 12],
    )
    drawdown_regimes, volatility_regimes = build_regime_profiles(
        bars_by_date=bars_by_date,
        weekly_periods=weekly_periods,
    )

    result = SymbolAnalysisResult(
        symbol=symbol,
        requested_start=requested_start.isoformat(),
        requested_end=requested_end.isoformat(),
        first_included_day=None
        if core.first_included_day is None
        else core.first_included_day.isoformat(),
        last_included_day=None
        if core.last_included_day is None
        else core.last_included_day.isoformat(),
        trading_days=core.trading_days,
        full_weeks=core.full_weeks,
        buy_and_hold_return_pct=core.buy_and_hold_return_pct,
        intraday_return_pct=core.intraday_return_pct,
        total_gap_return_pct=core.total_gap_return_pct,
        weekend_gap_return_pct=core.weekend_gap_return_pct,
        within_week_gap_return_pct=core.within_week_gap_return_pct,
        avg_full_week_pct=safe_mean(full_week_returns),
        median_full_week_pct=safe_median(full_week_returns),
        full_week_win_rate_pct=win_rate_pct(full_week_returns),
        avg_first_day_intraday_pct=safe_mean(first_day_intraday_returns),
        median_first_day_intraday_pct=safe_median(first_day_intraday_returns),
        avg_rest_of_week_pct=safe_mean(rest_of_week_returns),
        median_rest_of_week_pct=safe_median(rest_of_week_returns),
        first_day_vs_rest_corr=safe_correlation(
            first_day_intraday_returns, rest_of_week_returns
        ),
        after_red_first_day=build_condition_profile(
            "after_red_first_day", red_first_day_periods
        ),
        after_green_first_day=build_condition_profile(
            "after_green_first_day", green_first_day_periods
        ),
        monday_signal=monday_signal,
        rolling_windows=rolling_windows,
        drawdown_regimes=drawdown_regimes,
        volatility_regimes=volatility_regimes,
        weekday_profiles=weekday_profiles,
        weekly_periods=weekly_periods,
        possible_insights=[],
    )

    insights = build_possible_insights(
        symbol=symbol,
        buy_and_hold_return_pct=result.buy_and_hold_return_pct or 0.0,
        intraday_return_pct=result.intraday_return_pct or 0.0,
        total_gap_return_pct=result.total_gap_return_pct or 0.0,
        weekend_gap_return_pct=result.weekend_gap_return_pct or 0.0,
        avg_first_day_intraday_pct=result.avg_first_day_intraday_pct,
        avg_rest_of_week_pct=result.avg_rest_of_week_pct,
        corr=result.first_day_vs_rest_corr,
        after_red_first_day=result.after_red_first_day,
        after_green_first_day=result.after_green_first_day,
        weekday_profiles=weekday_profiles,
    )
    return replace(result, possible_insights=insights)


def render_weekday_profiles(profiles: list[DayOfWeekProfile]) -> list[str]:
    lines = ["  Calendar weekday profile:"]
    for profile in profiles:
        gap_label = format_pct(profile.avg_gap_into_open_pct)
        lines.append(
            "    "
            f"{profile.weekday:<9} intraday avg {format_pct(profile.avg_intraday_pct):>8}, "
            f"win rate {format_pct(profile.intraday_win_rate_pct):>8}, "
            f"gap into open avg {gap_label:>8}"
        )
    return lines


def render_condition(profile: ConditionProfile) -> str:
    return (
        f"{profile.weeks} weeks, avg rest {format_pct(profile.avg_rest_of_week_pct)}, "
        f"positive rest {format_pct(profile.positive_rest_of_week_rate_pct)}, "
        f"avg full week {format_pct(profile.avg_full_week_pct)}"
    )


def render_text(results: list[SymbolAnalysisResult], *, show_weeks: int) -> str:
    lines = [
        "Weekly structure analysis",
        "Focus: not whether a weekly trade 'works', but where the return stream lives and whether the first trading day says anything about the rest of the week.",
        "Results ignore commissions, slippage, taxes, and dividends.",
        "",
    ]
    for result in results:
        lines.extend(
            [
                f"{result.symbol}",
                f"  Window: {result.requested_start} to {result.requested_end}",
                f"  Included sessions: {result.first_included_day or 'n/a'} to {result.last_included_day or 'n/a'}",
                f"  Trading days / full weeks: {result.trading_days} / {result.full_weeks}",
                "  Return decomposition:",
                f"    Buy and hold:           {format_pct(result.buy_and_hold_return_pct)}",
                f"    Intraday only:          {format_pct(result.intraday_return_pct)}",
                f"    All session gaps:       {format_pct(result.total_gap_return_pct)}",
                f"    Weekend gaps only:      {format_pct(result.weekend_gap_return_pct)}",
                f"    Within-week gaps only:  {format_pct(result.within_week_gap_return_pct)}",
                "  Weekly shape:",
                f"    Avg / median full week: {format_pct(result.avg_full_week_pct)} / {format_pct(result.median_full_week_pct)}",
                f"    Full-week win rate:     {format_pct(result.full_week_win_rate_pct)}",
                f"    Avg / median first day: {format_pct(result.avg_first_day_intraday_pct)} / {format_pct(result.median_first_day_intraday_pct)}",
                f"    Avg / median rest week: {format_pct(result.avg_rest_of_week_pct)} / {format_pct(result.median_rest_of_week_pct)}",
                f"    First day vs rest corr: {'n/a' if result.first_day_vs_rest_corr is None else f'{result.first_day_vs_rest_corr:.2f}'}",
                "  Conditional behavior:",
                f"    After red first day:    {render_condition(result.after_red_first_day)}",
                f"    After green first day:  {render_condition(result.after_green_first_day)}",
            ]
        )
        lines.extend(render_weekday_profiles(result.weekday_profiles))
        if result.possible_insights:
            lines.append("  Possible insights:")
            for insight in result.possible_insights:
                lines.append(f"    - {insight}")
        if show_weeks > 0 and result.weekly_periods:
            lines.append("  Sample weekly periods:")
            for period in result.weekly_periods[
                : min(show_weeks, len(result.weekly_periods))
            ]:
                weekend = (
                    "n/a"
                    if period.weekend_gap_into_week_pct is None
                    else f"{period.weekend_gap_into_week_pct:.2f}%"
                )
                lines.append(
                    "    "
                    f"{period.first_day} -> {period.last_day}: first day {period.first_day_intraday_pct:.2f}%, "
                    f"rest {period.rest_of_week_pct:.2f}%, full week {period.full_week_pct:.2f}%, "
                    f"gap into week {weekend}"
                )
        lines.append("")
    return "\n".join(lines).rstrip()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    requested_end = resolve_end_date(args.end_date)
    requested_start = subtract_months(requested_end, args.months)
    client = create_alpaca_client_from_env()
    symbols = normalize_symbols(args.symbols)

    results = [
        analyze_symbol(
            client=client,
            symbol=symbol,
            requested_start=requested_start,
            requested_end=requested_end,
            stock_feed=args.stock_feed,
        )
        for symbol in symbols
    ]

    if args.json:
        payload: dict[str, Any] = {
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "stock_feed": args.stock_feed,
            "results": [asdict(result) for result in results],
        }
        print(json.dumps(payload, indent=2))
        return 0

    print(render_text(results, show_weeks=args.show_weeks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

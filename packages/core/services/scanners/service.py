#!/usr/bin/env python3
"""Scan Alpaca option chains for vertical spread candidates.

Usage:
    uv run spreads scan --symbol SPY

Required environment variables:
    APCA_API_KEY_ID
    APCA_API_SECRET_KEY

Notes:
    - Uses Alpaca's Trading API for option contract metadata.
    - Uses Alpaca's Market Data API for underlying price and option chain snapshots.
    - Supports call/put credit and debit vertical spreads with shared ranking/replay logic.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from core.common import clamp, env_or_die, load_local_env
from core.domain.models import (
    DailyBar,
    ExpectedMoveEstimate,
    IntradayBar,
    LiveOptionQuote,
    OptionContract,
    OptionSnapshot,
    SpreadCandidate,
    SymbolScanResult,
    UnderlyingSetupContext,
    UniverseScanFailure,
)
from core.domain.profiles import (
    LONG_VOL_STRATEGIES,
    format_session_bucket,
    zero_dte_delta_target,
    zero_dte_session_bucket,
)
from core.integrations.alpaca.client import (
    AlpacaClient,
    infer_trading_base_url,
)
from core.integrations.alpaca.streaming import AlpacaOptionQuoteStreamer
from core.integrations.calendar_events import (
    build_calendar_event_resolver,
)
from core.integrations.calendar_events.models import CalendarPolicyDecision
from core.integrations.calendar_events.policy import apply_strategy_calendar_policy
from core.integrations.greeks import build_local_greeks_provider
from core.services.market_dates import NEW_YORK
from core.services.scanners.market_data import option_expiry_close
from core.services.option_structures import (
    build_multileg_order_payload,
    candidate_legs,
    iron_condor_opening_legs,
    legs_identity_key,
    long_only_opening_legs,
    net_premium_kind,
    structure_quote_snapshot,
    vertical_opening_legs,
)
from core.services.scanners.config import (
    build_filter_payload,
    parse_args,
    resolve_symbols,
    strategy_direction,
    strategy_display_label,
    strategy_option_type,
)
from core.storage.factory import build_history_store
from core.storage.run_history_repository import RunHistoryRepository
def build_setup_summaries(results: list[SymbolScanResult]) -> tuple[str, ...]:
    summaries: list[str] = []
    for result in results:
        if result.setup is None:
            continue
        summaries.append(
            f"{result.args.strategy} {result.setup.status} ({result.setup.score:.1f})"
        )
    return tuple(summaries)


def effective_min_credit(width: float, args: argparse.Namespace) -> float:
    threshold = args.min_credit
    if args.profile != "0dte":
        return threshold
    session_bucket = zero_dte_session_bucket()
    if session_bucket != "late":
        return threshold
    if width <= 1.0:
        return max(threshold, 0.10)
    return max(threshold, 0.15)


def days_from_today(expiration_date: str) -> int:
    return (date.fromisoformat(expiration_date) - date.today()).days


def relative_spread(snapshot: OptionSnapshot) -> float:
    return (snapshot.ask - snapshot.bid) / snapshot.midpoint


def relative_spread_exceeds(
    snapshot: OptionSnapshot,
    maximum: float,
    *,
    tolerance: float = 1e-9,
) -> bool:
    return relative_spread(snapshot) > float(maximum) + tolerance


def log_scaled_score(value: int, floor: int, ceiling: int) -> float:
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 1.0
    numerator = math.log10(value) - math.log10(max(floor, 1))
    denominator = math.log10(max(ceiling, 1)) - math.log10(max(floor, 1))
    if denominator <= 0:
        return 0.0
    return clamp(numerator / denominator)


def average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def supportive_note(message: str) -> str:
    return f"Supportive: {message}"


def caution_note(message: str) -> str:
    return f"Caution: {message}"


def setup_status_from_score(score: float) -> str:
    if score >= 60:
        return "favorable"
    if score >= 40:
        return "neutral"
    return "unfavorable"


def dedupe_reasons(reasons: list[str]) -> tuple[str, ...]:
    deduped: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        if reason in seen:
            continue
        seen.add(reason)
        deduped.append(reason)
    return tuple(deduped)


def analyze_daily_setup(
    symbol: str,
    spot_price: float,
    bars: list[DailyBar],
    *,
    strategy: str,
) -> UnderlyingSetupContext:
    direction = strategy_direction(strategy)
    bullish = direction == "bullish"
    neutral = direction == "neutral"
    if len(bars) < 20:
        return UnderlyingSetupContext(
            strategy=strategy,
            status="unknown",
            score=0.0,
            reasons=("Not enough daily-bar history for setup analysis",),
            daily_score=0.0,
            intraday_score=None,
            spot_vs_sma20_pct=None,
            sma20_vs_sma50_pct=None,
            return_5d_pct=None,
            distance_to_20d_extreme_pct=None,
            latest_close=bars[-1].close if bars else None,
            sma20=None,
            sma50=None,
            source_window_days=len(bars),
        )

    closes = [bar.close for bar in bars]
    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    sma20 = average(closes[-20:])
    sma50 = average(closes[-50:]) if len(closes) >= 50 else None
    latest_close = closes[-1]
    return_5d_pct = None
    if len(closes) >= 6 and closes[-6] > 0:
        return_5d_pct = latest_close / closes[-6] - 1.0
    high_20 = max(highs[-20:])
    low_20 = min(lows[-20:])
    spot_vs_sma20_pct = ((spot_price - sma20) / sma20) if sma20 else None
    sma20_vs_sma50_pct = ((sma20 - sma50) / sma50) if sma20 and sma50 else None

    if neutral:
        return UnderlyingSetupContext(
            strategy=strategy,
            status="neutral",
            score=50.0,
            reasons=(f"{symbol} daily setup is neutral for move-based positioning",),
            daily_score=50.0,
            intraday_score=None,
            spot_vs_sma20_pct=spot_vs_sma20_pct,
            sma20_vs_sma50_pct=sma20_vs_sma50_pct,
            return_5d_pct=return_5d_pct,
            distance_to_20d_extreme_pct=None,
            latest_close=latest_close,
            sma20=sma20,
            sma50=sma50,
            source_window_days=len(bars),
        )

    if bullish:
        distance_to_20d_extreme_pct = (
            (spot_price - low_20) / spot_price if spot_price > 0 else None
        )
        price_vs_sma20_score = (
            0.5
            if spot_vs_sma20_pct is None
            else clamp(0.5 + (spot_vs_sma20_pct / 0.08))
        )
        trend_score = (
            0.5
            if sma20_vs_sma50_pct is None
            else clamp(0.5 + (sma20_vs_sma50_pct / 0.06))
        )
        momentum_score = (
            0.5 if return_5d_pct is None else clamp(0.45 + (return_5d_pct / 0.08))
        )
        extreme_distance_score = (
            0.5
            if distance_to_20d_extreme_pct is None
            else clamp(distance_to_20d_extreme_pct / 0.04)
        )
    else:
        distance_to_20d_extreme_pct = (
            (high_20 - spot_price) / spot_price if spot_price > 0 else None
        )
        price_vs_sma20_score = (
            0.5
            if spot_vs_sma20_pct is None
            else clamp(0.5 - (spot_vs_sma20_pct / 0.08))
        )
        trend_score = (
            0.5
            if sma20_vs_sma50_pct is None
            else clamp(0.5 - (sma20_vs_sma50_pct / 0.06))
        )
        momentum_score = (
            0.5 if return_5d_pct is None else clamp(0.55 - (return_5d_pct / 0.08))
        )
        extreme_distance_score = (
            0.5
            if distance_to_20d_extreme_pct is None
            else clamp(distance_to_20d_extreme_pct / 0.04)
        )

    score = round(
        100.0
        * (
            0.35 * price_vs_sma20_score
            + 0.25 * trend_score
            + 0.20 * momentum_score
            + 0.20 * extreme_distance_score
        ),
        1,
    )

    reasons: list[str] = []
    if bullish:
        if spot_vs_sma20_pct is not None:
            if spot_vs_sma20_pct > 0.02:
                reasons.append(
                    supportive_note("spot is extended above the 20-day average")
                )
            elif spot_vs_sma20_pct < -0.01:
                reasons.append(caution_note("spot is trading below the 20-day average"))
        if sma20_vs_sma50_pct is not None:
            if sma20_vs_sma50_pct > 0.015:
                reasons.append(
                    supportive_note("20-day average is above the 50-day average")
                )
            elif sma20_vs_sma50_pct < -0.01:
                reasons.append(
                    caution_note("20-day average is below the 50-day average")
                )
        if return_5d_pct is not None:
            if return_5d_pct > 0.03:
                reasons.append(
                    supportive_note("recent 5-day momentum is strongly positive")
                )
            elif return_5d_pct < -0.02:
                reasons.append(
                    caution_note("recent 5-day momentum is weak to negative")
                )
        if distance_to_20d_extreme_pct is not None:
            if distance_to_20d_extreme_pct < 0.01:
                reasons.append(caution_note("spot is trading near the 20-day low"))
            elif distance_to_20d_extreme_pct > 0.03:
                reasons.append(
                    supportive_note("spot has room above the recent 20-day low")
                )
    else:
        if spot_vs_sma20_pct is not None:
            if spot_vs_sma20_pct > 0.02:
                reasons.append(
                    supportive_note("spot is extended above the 20-day average")
                )
            elif spot_vs_sma20_pct < -0.01:
                reasons.append(caution_note("spot is trading below the 20-day average"))
        if sma20_vs_sma50_pct is not None:
            if sma20_vs_sma50_pct > 0.015:
                reasons.append(
                    caution_note("20-day average is leading the 50-day average higher")
                )
            elif sma20_vs_sma50_pct < -0.01:
                reasons.append(
                    supportive_note("20-day average is below the 50-day average")
                )
        if return_5d_pct is not None:
            if return_5d_pct > 0.03:
                reasons.append(
                    caution_note("recent 5-day momentum is strongly positive")
                )
            elif return_5d_pct < -0.02:
                reasons.append(
                    supportive_note("recent 5-day momentum is weak to negative")
                )
        if distance_to_20d_extreme_pct is not None:
            if distance_to_20d_extreme_pct < 0.01:
                reasons.append(caution_note("spot is trading near the 20-day high"))
            elif distance_to_20d_extreme_pct > 0.03:
                reasons.append(
                    supportive_note("spot has room below the recent 20-day high")
                )

    status = setup_status_from_score(score)
    if not reasons:
        if bullish:
            reasons.append(f"{symbol} daily setup is {status} for bullish positioning")
        else:
            reasons.append(f"{symbol} daily setup is {status} for bearish positioning")

    return UnderlyingSetupContext(
        strategy=strategy,
        status=status,
        score=score,
        reasons=tuple(reasons),
        daily_score=score,
        intraday_score=None,
        spot_vs_sma20_pct=spot_vs_sma20_pct,
        sma20_vs_sma50_pct=sma20_vs_sma50_pct,
        return_5d_pct=return_5d_pct,
        distance_to_20d_extreme_pct=distance_to_20d_extreme_pct,
        latest_close=latest_close,
        sma20=sma20,
        sma50=sma50,
        source_window_days=len(bars),
    )


def analyze_intraday_setup(
    symbol: str,
    spot_price: float,
    bars: list[IntradayBar],
    *,
    strategy: str,
) -> UnderlyingSetupContext | None:
    direction = strategy_direction(strategy)
    bullish = direction == "bullish"
    neutral = direction == "neutral"
    if len(bars) < 5:
        return None

    open_price = bars[0].open
    if open_price <= 0:
        return None

    session_high = max(bar.high for bar in bars)
    session_low = min(bar.low for bar in bars)
    weighted_prices = [
        ((bar.high + bar.low + bar.close) / 3.0) * max(bar.volume, 1) for bar in bars
    ]
    total_volume = sum(max(bar.volume, 1) for bar in bars)
    vwap = None if total_volume <= 0 else sum(weighted_prices) / total_volume
    spot_vs_vwap_pct = None if vwap in (None, 0) else (spot_price - vwap) / vwap
    intraday_return_pct = (spot_price / open_price - 1.0) if open_price > 0 else None
    opening_range_window = bars[: min(30, len(bars))]
    opening_range_high = max(bar.high for bar in opening_range_window)
    opening_range_low = min(bar.low for bar in opening_range_window)
    if neutral:
        return UnderlyingSetupContext(
            strategy=strategy,
            status="neutral",
            score=50.0,
            reasons=(f"{symbol} intraday setup is neutral for move-based positioning",),
            daily_score=None,
            intraday_score=50.0,
            spot_vs_sma20_pct=None,
            sma20_vs_sma50_pct=None,
            return_5d_pct=None,
            distance_to_20d_extreme_pct=None,
            latest_close=bars[-1].close,
            sma20=None,
            sma50=None,
            source_window_days=0,
            spot_vs_vwap_pct=spot_vs_vwap_pct,
            intraday_return_pct=intraday_return_pct,
            distance_to_session_extreme_pct=None,
            opening_range_break_pct=None,
            vwap=vwap,
            opening_range_high=opening_range_high,
            opening_range_low=opening_range_low,
            source_window_minutes=len(bars),
        )
    if bullish:
        distance_to_session_extreme_pct = (
            (spot_price - session_low) / spot_price if spot_price > 0 else None
        )
        opening_range_break_pct = (
            (spot_price - opening_range_high) / spot_price if spot_price > 0 else None
        )
        vwap_score = (
            0.5 if spot_vs_vwap_pct is None else clamp(0.5 + (spot_vs_vwap_pct / 0.01))
        )
        opening_range_score = (
            0.5
            if opening_range_break_pct is None
            else clamp(0.55 + (opening_range_break_pct / 0.01))
        )
        momentum_score = (
            0.5
            if intraday_return_pct is None
            else clamp(0.5 + (intraday_return_pct / 0.015))
        )
        extreme_score = (
            0.5
            if distance_to_session_extreme_pct is None
            else clamp(distance_to_session_extreme_pct / 0.012)
        )
    else:
        distance_to_session_extreme_pct = (
            (session_high - spot_price) / spot_price if spot_price > 0 else None
        )
        opening_range_break_pct = (
            (opening_range_low - spot_price) / spot_price if spot_price > 0 else None
        )
        vwap_score = (
            0.5 if spot_vs_vwap_pct is None else clamp(0.5 - (spot_vs_vwap_pct / 0.01))
        )
        opening_range_score = (
            0.5
            if opening_range_break_pct is None
            else clamp(0.55 + (opening_range_break_pct / 0.01))
        )
        momentum_score = (
            0.5
            if intraday_return_pct is None
            else clamp(0.5 - (intraday_return_pct / 0.015))
        )
        extreme_score = (
            0.5
            if distance_to_session_extreme_pct is None
            else clamp(distance_to_session_extreme_pct / 0.012)
        )

    score = round(
        100.0
        * (
            0.35 * vwap_score
            + 0.25 * opening_range_score
            + 0.20 * momentum_score
            + 0.20 * extreme_score
        ),
        1,
    )
    status = setup_status_from_score(score)

    reasons: list[str] = []
    if bullish:
        if spot_vs_vwap_pct is not None:
            if spot_vs_vwap_pct > 0.0015:
                reasons.append(supportive_note("spot is holding above VWAP"))
            elif spot_vs_vwap_pct < -0.0015:
                reasons.append(caution_note("spot is trading below VWAP"))
        if opening_range_break_pct is not None:
            if opening_range_break_pct > 0.001:
                reasons.append(supportive_note("spot is above the opening range high"))
            elif spot_price < opening_range_low:
                reasons.append(
                    caution_note("spot has broken below the opening range low")
                )
        if intraday_return_pct is not None:
            if intraday_return_pct > 0.004:
                reasons.append(supportive_note("intraday trend is positive"))
            elif intraday_return_pct < -0.004:
                reasons.append(caution_note("intraday trend is negative"))
        if distance_to_session_extreme_pct is not None:
            if distance_to_session_extreme_pct < 0.003:
                reasons.append(caution_note("spot is trading near the session low"))
            elif distance_to_session_extreme_pct > 0.008:
                reasons.append(supportive_note("spot has room above the session low"))
    else:
        if spot_vs_vwap_pct is not None:
            if spot_vs_vwap_pct < -0.0015:
                reasons.append(supportive_note("spot is holding below VWAP"))
            elif spot_vs_vwap_pct > 0.0015:
                reasons.append(caution_note("spot is trading above VWAP"))
        if opening_range_break_pct is not None:
            if opening_range_break_pct > 0.001:
                reasons.append(supportive_note("spot is below the opening range low"))
            elif spot_price > opening_range_high:
                reasons.append(
                    caution_note("spot has broken above the opening range high")
                )
        if intraday_return_pct is not None:
            if intraday_return_pct < -0.004:
                reasons.append(supportive_note("intraday trend is negative"))
            elif intraday_return_pct > 0.004:
                reasons.append(caution_note("intraday trend is positive"))
        if distance_to_session_extreme_pct is not None:
            if distance_to_session_extreme_pct < 0.003:
                reasons.append(caution_note("spot is trading near the session high"))
            elif distance_to_session_extreme_pct > 0.008:
                reasons.append(supportive_note("spot has room below the session high"))

    if not reasons:
        if bullish:
            reasons.append(
                f"{symbol} intraday setup is {status} for bullish positioning"
            )
        else:
            reasons.append(
                f"{symbol} intraday setup is {status} for bearish positioning"
            )

    return UnderlyingSetupContext(
        strategy=strategy,
        status=status,
        score=score,
        reasons=tuple(reasons),
        daily_score=None,
        intraday_score=score,
        spot_vs_sma20_pct=None,
        sma20_vs_sma50_pct=None,
        return_5d_pct=None,
        distance_to_20d_extreme_pct=None,
        latest_close=bars[-1].close,
        sma20=None,
        sma50=None,
        source_window_days=0,
        spot_vs_vwap_pct=spot_vs_vwap_pct,
        intraday_return_pct=intraday_return_pct,
        distance_to_session_extreme_pct=distance_to_session_extreme_pct,
        opening_range_break_pct=opening_range_break_pct,
        vwap=vwap,
        opening_range_high=opening_range_high,
        opening_range_low=opening_range_low,
        source_window_minutes=len(bars),
    )


def combine_setup_contexts(
    daily_setup: UnderlyingSetupContext,
    intraday_setup: UnderlyingSetupContext | None,
    *,
    profile: str,
    strategy: str,
) -> UnderlyingSetupContext:
    if intraday_setup is None:
        return daily_setup

    intraday_weight = {
        "0dte": 0.65,
        "micro": 0.50,
        "weekly": 0.35,
        "swing": 0.20,
        "core": 0.10,
    }.get(profile, 0.10)
    if daily_setup.status == "unknown":
        intraday_weight = 1.0
    daily_weight = 1.0 - intraday_weight
    blended_score = round(
        (daily_setup.score * daily_weight) + (intraday_setup.score * intraday_weight), 1
    )
    blended_status = setup_status_from_score(blended_score)

    ordered_reasons = list(intraday_setup.reasons[:4]) + list(daily_setup.reasons[:3])
    if not ordered_reasons:
        ordered_reasons.append(f"Combined setup is {blended_status} for {strategy}")

    return UnderlyingSetupContext(
        strategy=strategy,
        status=blended_status,
        score=blended_score,
        reasons=dedupe_reasons(ordered_reasons),
        daily_score=daily_setup.score,
        intraday_score=intraday_setup.score,
        spot_vs_sma20_pct=daily_setup.spot_vs_sma20_pct,
        sma20_vs_sma50_pct=daily_setup.sma20_vs_sma50_pct,
        return_5d_pct=daily_setup.return_5d_pct,
        distance_to_20d_extreme_pct=daily_setup.distance_to_20d_extreme_pct,
        latest_close=intraday_setup.latest_close or daily_setup.latest_close,
        sma20=daily_setup.sma20,
        sma50=daily_setup.sma50,
        source_window_days=daily_setup.source_window_days,
        spot_vs_vwap_pct=intraday_setup.spot_vs_vwap_pct,
        intraday_return_pct=intraday_setup.intraday_return_pct,
        distance_to_session_extreme_pct=intraday_setup.distance_to_session_extreme_pct,
        opening_range_break_pct=intraday_setup.opening_range_break_pct,
        vwap=intraday_setup.vwap,
        opening_range_high=intraday_setup.opening_range_high,
        opening_range_low=intraday_setup.opening_range_low,
        source_window_minutes=intraday_setup.source_window_minutes,
    )


def analyze_underlying_setup(
    symbol: str,
    spot_price: float,
    daily_bars: list[DailyBar],
    *,
    strategy: str,
    profile: str,
    intraday_bars: list[IntradayBar] | None = None,
) -> UnderlyingSetupContext:
    daily_setup = analyze_daily_setup(symbol, spot_price, daily_bars, strategy=strategy)
    intraday_setup = analyze_intraday_setup(
        symbol, spot_price, intraday_bars or [], strategy=strategy
    )
    return combine_setup_contexts(
        daily_setup, intraday_setup, profile=profile, strategy=strategy
    )


def attach_underlying_setup(
    candidates: list[SpreadCandidate],
    setup: UnderlyingSetupContext | None,
) -> list[SpreadCandidate]:
    if setup is None:
        return candidates
    has_intraday_context = (
        setup.intraday_score is not None and (setup.source_window_minutes or 0) > 0
    )
    return [
        replace(
            candidate,
            setup_status=setup.status,
            setup_score=setup.score,
            setup_reasons=setup.reasons,
            setup_daily_score=setup.daily_score,
            setup_intraday_score=setup.intraday_score,
            setup_intraday_minutes=setup.source_window_minutes,
            setup_has_intraday_context=has_intraday_context,
            setup_spot_vs_vwap_pct=setup.spot_vs_vwap_pct,
            setup_intraday_return_pct=setup.intraday_return_pct,
            setup_distance_to_session_extreme_pct=setup.distance_to_session_extreme_pct,
            setup_opening_range_break_pct=setup.opening_range_break_pct,
            setup_latest_close=setup.latest_close,
            setup_vwap=setup.vwap,
            setup_opening_range_high=setup.opening_range_high,
            setup_opening_range_low=setup.opening_range_low,
        )
        for candidate in candidates
    ]


def assess_data_quality(
    candidate: SpreadCandidate,
    *,
    underlying_type: str,
    args: argparse.Namespace,
) -> tuple[str, tuple[str, ...]]:
    if args.data_policy == "off":
        return "clean", ()

    reasons: list[str] = []
    blocked = False
    penalized = False
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES

    if candidate.expected_move is None or candidate.expected_move <= 0:
        reason = "Missing expected-move estimate"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)
    elif long_vol:
        if (
            candidate.modeled_move_vs_break_even_move is not None
            and candidate.modeled_move_vs_break_even_move < 0.85
        ):
            reason = (
                "Modeled move does not clear the structure break-even cleanly "
                f"({candidate.modeled_move_vs_break_even_move:.2f} < 0.85)"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)
    else:
        short_ratio = (
            candidate.short_vs_expected_move or 0.0
        ) / candidate.expected_move
        breakeven_ratio = (
            candidate.breakeven_vs_expected_move or 0.0
        ) / candidate.expected_move
        if short_ratio < args.min_short_vs_expected_move_ratio:
            reason = (
                f"Short strike sits too far inside expected move "
                f"({short_ratio:.2f} < {args.min_short_vs_expected_move_ratio:.2f})"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)
        if breakeven_ratio < args.min_breakeven_vs_expected_move_ratio:
            reason = (
                f"Breakeven sits too far inside expected move "
                f"({breakeven_ratio:.2f} < {args.min_breakeven_vs_expected_move_ratio:.2f})"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)

    if candidate.fill_ratio < args.min_fill_ratio:
        reason = f"Natural-to-mid fill ratio is too weak ({candidate.fill_ratio:.2f} < {args.min_fill_ratio:.2f})"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)

    if (
        underlying_type == "single_name_equity"
        and candidate.calendar_confidence == "low"
    ):
        reason = "Calendar data confidence is low for this single-name candidate"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)

    if blocked:
        return "blocked", tuple(reasons)
    if penalized:
        return "penalized", tuple(reasons)
    return "clean", ()


def attach_data_quality(
    *,
    candidates: list[SpreadCandidate],
    underlying_type: str,
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    enriched: list[SpreadCandidate] = []
    for candidate in candidates:
        status, reasons = assess_data_quality(
            candidate, underlying_type=underlying_type, args=args
        )
        if args.data_policy == "strict" and status == "blocked":
            continue
        enriched.append(replace(candidate, data_status=status, data_reasons=reasons))
    return enriched


def build_selection_notes(
    candidate: SpreadCandidate, args: argparse.Namespace
) -> tuple[str, ...]:
    notes: list[str] = []
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES
    delta_target = args.short_delta_target
    if args.profile == "0dte":
        session_bucket = zero_dte_session_bucket()
        notes.append(f"session-{format_session_bucket(session_bucket)}")
        delta_target = zero_dte_delta_target(session_bucket)
    if candidate.strategy == "long_straddle":
        notes.append("atm-move")
    elif candidate.strategy == "long_strangle":
        notes.append("winged-move")
    elif (
        candidate.short_delta is not None
        and abs(abs(candidate.short_delta) - delta_target) <= 0.02
    ):
        notes.append("delta-fit")
    if long_vol and candidate.modeled_move_vs_break_even_move is not None:
        if candidate.modeled_move_vs_break_even_move >= 1.0:
            notes.append("move-cheap")
        else:
            notes.append("move-rich")
    elif candidate.expected_move and candidate.short_vs_expected_move is not None:
        if candidate.short_vs_expected_move >= 0:
            notes.append("outside-em")
        else:
            notes.append("inside-em")
    if candidate.fill_ratio >= 0.80:
        notes.append("good-fill")
    elif candidate.fill_ratio >= args.min_fill_ratio:
        notes.append("acceptable-fill")
    if min(candidate.short_open_interest, candidate.long_open_interest) >= max(
        args.min_open_interest * 3, 500
    ):
        notes.append("liquid")
    if candidate.calendar_status == "clean":
        notes.append("calendar-clean")
    elif candidate.calendar_status == "penalized":
        notes.append("calendar-risk")
    if candidate.earnings_phase not in {"", "clean", "post_event_settled"}:
        notes.append(f"earnings-{candidate.earnings_phase.replace('_', '-')}")
    if candidate.setup_status == "favorable":
        notes.append("setup-favorable")
    elif candidate.setup_status == "neutral":
        notes.append("setup-neutral")
    if candidate.data_status == "penalized":
        notes.append("data-caution")
    if candidate.greeks_source != "alpaca":
        notes.append("local-greeks")
    if (
        len(notes) > 4
        and candidate.greeks_source != "alpaca"
        and "local-greeks" not in notes[:4]
    ):
        notes = [*notes[:3], "local-greeks"]
    return tuple(notes[:4])


def attach_selection_notes(
    candidates: list[SpreadCandidate], args: argparse.Namespace
) -> list[SpreadCandidate]:
    return [
        replace(candidate, selection_notes=build_selection_notes(candidate, args))
        for candidate in candidates
    ]


def deduplicate_candidates(
    candidates: list[SpreadCandidate], expand_duplicates: bool
) -> list[SpreadCandidate]:
    if expand_duplicates:
        return candidates

    deduplicated: list[SpreadCandidate] = []
    seen_leg_sets: set[str] = set()
    for candidate in candidates:
        identity = legs_identity_key(
            strategy=candidate.strategy,
            legs=candidate_legs(asdict(candidate)),
        )
        if identity in seen_leg_sets:
            continue
        seen_leg_sets.add(identity)
        deduplicated.append(candidate)
    return deduplicated


def make_open_order_payload(
    short_symbol: str,
    long_symbol: str,
    limit_price: float,
    *,
    strategy: str = "call_credit",
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=vertical_opening_legs(
            short_symbol=short_symbol,
            long_symbol=long_symbol,
        ),
        limit_price=limit_price,
        strategy_family=strategy,
        trade_intent="open",
    )


def make_close_order_payload(
    short_symbol: str,
    long_symbol: str,
    limit_price: float,
    *,
    strategy: str = "call_credit",
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=[
            {
                "symbol": short_symbol,
                "ratio_qty": "1",
                "side": "buy",
                "position_intent": "buy_to_close",
                "role": "short",
            },
            {
                "symbol": long_symbol,
                "ratio_qty": "1",
                "side": "sell",
                "position_intent": "sell_to_close",
                "role": "long",
            },
        ],
        limit_price=limit_price,
        strategy_family=strategy,
        trade_intent="close",
    )


def make_order_payload(
    short_symbol: str,
    long_symbol: str,
    limit_price: float,
    *,
    strategy: str = "call_credit",
) -> dict[str, Any]:
    return make_open_order_payload(
        short_symbol=short_symbol,
        long_symbol=long_symbol,
        limit_price=limit_price,
        strategy=strategy,
    )


def make_iron_condor_order_payload(
    *,
    short_put_symbol: str,
    long_put_symbol: str,
    short_call_symbol: str,
    long_call_symbol: str,
    limit_price: float,
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=iron_condor_opening_legs(
            short_put_symbol=short_put_symbol,
            long_put_symbol=long_put_symbol,
            short_call_symbol=short_call_symbol,
            long_call_symbol=long_call_symbol,
        ),
        limit_price=limit_price,
        strategy_family="iron_condor",
        trade_intent="open",
    )


def make_long_vol_order_payload(
    *,
    symbols: tuple[str, ...],
    limit_price: float,
    strategy: str,
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=long_only_opening_legs(*symbols),
        limit_price=limit_price,
        strategy_family=strategy,
        trade_intent="open",
    )


def default_output_path(symbol: str, strategy: str, output_format: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    directory = {
        "call_credit": "call_credit_spreads",
        "put_credit": "put_credit_spreads",
        "call_debit": "call_debit_spreads",
        "put_debit": "put_debit_spreads",
        "long_straddle": "long_straddles",
        "long_strangle": "long_strangles",
        "iron_condor": "iron_condors",
        "combined": "combined_credit_spreads",
    }.get(strategy, "call_credit_spreads")
    return str(
        Path("outputs") / directory / f"{symbol.lower()}_{timestamp}.{output_format}"
    )


def default_universe_output_path(label: str, strategy: str, output_format: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_label = label.lower().replace(" ", "_")
    return str(
        Path("outputs")
        / "universe_boards"
        / f"{safe_label}_{strategy}_{timestamp}.{output_format}"
    )


def write_latest_copy(output_path: str, latest_name: str) -> str:
    latest_path = str(Path(output_path).with_name(latest_name))
    shutil.copyfile(output_path, latest_path)
    return latest_path


def build_vertical_spreads(
    *,
    symbol: str,
    strategy: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []
    option_type = strategy_option_type(strategy)
    premium_kind = net_premium_kind(strategy)

    for expiration_date, contracts in sorted(contracts_by_expiration.items()):
        snapshot_map = snapshots_by_expiration.get(expiration_date, {})
        sorted_contracts = sorted(contracts, key=lambda contract: contract.strike_price)
        expected_move = expected_moves_by_expiration.get(expiration_date)
        days_to_expiration = days_from_today(expiration_date)

        short_contracts = (
            sorted_contracts
            if option_type == "call"
            else list(reversed(sorted_contracts))
        )

        for short_contract in short_contracts:
            short_snapshot = snapshot_map.get(short_contract.symbol)
            if not short_snapshot:
                continue
            if option_type == "call":
                if short_contract.strike_price <= spot_price:
                    continue
            else:
                if short_contract.strike_price >= spot_price:
                    continue
            if short_contract.open_interest < args.min_open_interest:
                continue
            short_leg_relative_spread = relative_spread(short_snapshot)
            if relative_spread_exceeds(short_snapshot, args.max_relative_spread):
                continue
            if short_snapshot.bid_size <= 0:
                continue
            short_delta = short_snapshot.delta
            if short_delta is None:
                continue
            short_delta_magnitude = abs(short_delta)
            if not (
                args.short_delta_min <= short_delta_magnitude <= args.short_delta_max
            ):
                continue

            short_index = sorted_contracts.index(short_contract)
            if option_type == "call" and premium_kind == "credit":
                long_contract_iterable = sorted_contracts[short_index + 1 :]
            elif option_type == "call":
                long_contract_iterable = reversed(sorted_contracts[:short_index])
            elif premium_kind == "credit":
                long_contract_iterable = reversed(sorted_contracts[:short_index])
            else:
                long_contract_iterable = sorted_contracts[short_index + 1 :]

            for long_contract in long_contract_iterable:
                if option_type == "call":
                    if premium_kind == "credit":
                        if long_contract.strike_price <= short_contract.strike_price:
                            continue
                        width = long_contract.strike_price - short_contract.strike_price
                    else:
                        if long_contract.strike_price >= short_contract.strike_price:
                            continue
                        width = short_contract.strike_price - long_contract.strike_price
                else:
                    if premium_kind == "credit":
                        if long_contract.strike_price >= short_contract.strike_price:
                            continue
                        width = short_contract.strike_price - long_contract.strike_price
                    else:
                        if long_contract.strike_price <= short_contract.strike_price:
                            continue
                        width = long_contract.strike_price - short_contract.strike_price
                if width < args.min_width:
                    continue
                if width > args.max_width:
                    continue

                long_snapshot = snapshot_map.get(long_contract.symbol)
                if not long_snapshot:
                    continue
                if long_contract.open_interest < args.min_open_interest:
                    continue
                long_leg_relative_spread = relative_spread(long_snapshot)
                if relative_spread_exceeds(long_snapshot, args.max_relative_spread):
                    continue
                if long_snapshot.ask_size <= 0:
                    continue
                long_delta = long_snapshot.delta

                if premium_kind == "debit":
                    midpoint_credit = long_snapshot.midpoint - short_snapshot.midpoint
                    natural_credit = long_snapshot.ask - short_snapshot.bid
                else:
                    midpoint_credit = short_snapshot.midpoint - long_snapshot.midpoint
                    natural_credit = short_snapshot.bid - long_snapshot.ask
                if midpoint_credit < effective_min_credit(width, args):
                    continue
                if natural_credit <= 0:
                    continue
                if midpoint_credit >= width:
                    continue

                if premium_kind == "debit":
                    max_profit = (width - midpoint_credit) * 100.0
                    max_loss = midpoint_credit * 100.0
                else:
                    max_profit = midpoint_credit * 100.0
                    max_loss = (width - midpoint_credit) * 100.0
                if max_loss <= 0 or max_profit <= 0:
                    continue

                if premium_kind == "debit":
                    return_on_risk = (width - midpoint_credit) / midpoint_credit
                else:
                    return_on_risk = midpoint_credit / (width - midpoint_credit)
                if return_on_risk < args.min_return_on_risk:
                    continue

                if option_type == "call":
                    breakeven = (
                        long_contract.strike_price + midpoint_credit
                        if premium_kind == "debit"
                        else short_contract.strike_price + midpoint_credit
                    )
                    short_otm_pct = (
                        short_contract.strike_price - spot_price
                    ) / spot_price
                    breakeven_cushion_pct = (breakeven - spot_price) / spot_price
                else:
                    breakeven = (
                        long_contract.strike_price - midpoint_credit
                        if premium_kind == "debit"
                        else short_contract.strike_price - midpoint_credit
                    )
                    short_otm_pct = (
                        spot_price - short_contract.strike_price
                    ) / spot_price
                    breakeven_cushion_pct = (spot_price - breakeven) / spot_price
                fill_ratio = clamp(
                    midpoint_credit / natural_credit
                    if premium_kind == "debit"
                    else natural_credit / midpoint_credit,
                    0.0,
                    1.25,
                )
                short_vs_expected_move = None
                breakeven_vs_expected_move = None
                expected_move_amount = None
                expected_move_pct = None
                expected_move_source_strike = None
                if expected_move:
                    expected_move_amount = expected_move.amount
                    expected_move_pct = expected_move.percent_of_spot
                    expected_move_source_strike = expected_move.reference_strike
                    if option_type == "call":
                        expected_move_boundary = spot_price + expected_move.amount
                        short_vs_expected_move = (
                            short_contract.strike_price - expected_move_boundary
                        )
                        breakeven_vs_expected_move = (
                            expected_move_boundary - breakeven
                            if premium_kind == "debit"
                            else breakeven - expected_move_boundary
                        )
                    else:
                        expected_move_boundary = spot_price - expected_move.amount
                        short_vs_expected_move = (
                            expected_move_boundary - short_contract.strike_price
                        )
                        breakeven_vs_expected_move = (
                            breakeven - expected_move_boundary
                            if premium_kind == "debit"
                            else expected_move_boundary - breakeven
                        )

                candidates.append(
                    SpreadCandidate(
                        underlying_symbol=symbol,
                        strategy=strategy,
                        profile=args.profile,
                        expiration_date=expiration_date,
                        days_to_expiration=days_to_expiration,
                        underlying_price=spot_price,
                        short_symbol=short_contract.symbol,
                        long_symbol=long_contract.symbol,
                        short_strike=short_contract.strike_price,
                        long_strike=long_contract.strike_price,
                        width=width,
                        short_delta=short_delta,
                        long_delta=long_delta,
                        greeks_source=short_snapshot.greeks_source
                        if short_snapshot.greeks_source == long_snapshot.greeks_source
                        else "mixed",
                        short_midpoint=short_snapshot.midpoint,
                        long_midpoint=long_snapshot.midpoint,
                        short_bid=short_snapshot.bid,
                        short_ask=short_snapshot.ask,
                        long_bid=long_snapshot.bid,
                        long_ask=long_snapshot.ask,
                        midpoint_credit=midpoint_credit,
                        natural_credit=natural_credit,
                        max_profit=max_profit,
                        max_loss=max_loss,
                        return_on_risk=return_on_risk,
                        breakeven=breakeven,
                        breakeven_cushion_pct=breakeven_cushion_pct,
                        short_otm_pct=short_otm_pct,
                        short_open_interest=short_contract.open_interest,
                        long_open_interest=long_contract.open_interest,
                        short_relative_spread=short_leg_relative_spread,
                        long_relative_spread=long_leg_relative_spread,
                        fill_ratio=fill_ratio,
                        min_quote_size=min(
                            short_snapshot.bid_size,
                            short_snapshot.ask_size,
                            long_snapshot.bid_size,
                            long_snapshot.ask_size,
                        ),
                        expected_move=expected_move_amount,
                        expected_move_pct=expected_move_pct,
                        expected_move_source_strike=expected_move_source_strike,
                        short_vs_expected_move=short_vs_expected_move,
                        breakeven_vs_expected_move=breakeven_vs_expected_move,
                        order_payload=make_order_payload(
                            short_contract.symbol,
                            long_contract.symbol,
                            midpoint_credit,
                            strategy=strategy,
                        ),
                        short_bid_size=short_snapshot.bid_size,
                        short_ask_size=short_snapshot.ask_size,
                        long_bid_size=long_snapshot.bid_size,
                        long_ask_size=long_snapshot.ask_size,
                        short_implied_volatility=short_snapshot.implied_volatility,
                        long_implied_volatility=long_snapshot.implied_volatility,
                        short_volume=short_snapshot.daily_volume,
                        long_volume=long_snapshot.daily_volume,
                    )
                )

    return candidates


def build_iron_condors(
    *,
    symbol: str,
    spot_price: float,
    call_contracts_by_expiration: dict[str, list[OptionContract]],
    put_contracts_by_expiration: dict[str, list[OptionContract]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []
    common_expirations = sorted(
        set(call_contracts_by_expiration).intersection(put_contracts_by_expiration)
    )
    delta_window = max(args.short_delta_max - args.short_delta_min, 0.08)

    for expiration_date in common_expirations:
        days_to_expiration = days_from_today(expiration_date)
        if days_to_expiration <= 0:
            continue

        call_contracts = sorted(
            call_contracts_by_expiration.get(expiration_date, []),
            key=lambda contract: contract.strike_price,
        )
        put_contracts = sorted(
            put_contracts_by_expiration.get(expiration_date, []),
            key=lambda contract: contract.strike_price,
        )
        call_snapshots = call_snapshots_by_expiration.get(expiration_date, {})
        put_snapshots = put_snapshots_by_expiration.get(expiration_date, {})
        expected_move = expected_moves_by_expiration.get(expiration_date)

        call_contract_by_strike = {
            round(contract.strike_price, 4): contract for contract in call_contracts
        }

        short_calls = [
            contract
            for contract in call_contracts
            if contract.strike_price > spot_price
        ]
        short_puts = list(
            reversed(
                [
                    contract
                    for contract in put_contracts
                    if contract.strike_price < spot_price
                ]
            )
        )

        for short_put in short_puts:
            short_put_snapshot = put_snapshots.get(short_put.symbol)
            if short_put_snapshot is None:
                continue
            short_put_delta = abs(short_put_snapshot.delta or 0.0)
            if (
                short_put.open_interest < args.min_open_interest
                or short_put_snapshot.bid_size <= 0
                or short_put_snapshot.ask_size <= 0
                or short_put_delta <= 0
                or relative_spread_exceeds(short_put_snapshot, args.max_relative_spread)
                or not (args.short_delta_min <= short_put_delta <= args.short_delta_max)
            ):
                continue
            short_put_index = put_contracts.index(short_put)

            for long_put in reversed(put_contracts[:short_put_index]):
                width = short_put.strike_price - long_put.strike_price
                if width < args.min_width or width > args.max_width:
                    continue
                long_put_snapshot = put_snapshots.get(long_put.symbol)
                if long_put_snapshot is None:
                    continue
                if (
                    long_put.open_interest < args.min_open_interest
                    or long_put_snapshot.bid_size <= 0
                    or long_put_snapshot.ask_size <= 0
                    or relative_spread_exceeds(
                        long_put_snapshot, args.max_relative_spread
                    )
                ):
                    continue

                for short_call in short_calls:
                    short_call_snapshot = call_snapshots.get(short_call.symbol)
                    if short_call_snapshot is None:
                        continue
                    short_call_delta = abs(short_call_snapshot.delta or 0.0)
                    if (
                        short_call.open_interest < args.min_open_interest
                        or short_call_snapshot.bid_size <= 0
                        or short_call_snapshot.ask_size <= 0
                        or short_call_delta <= 0
                        or relative_spread_exceeds(
                            short_call_snapshot,
                            args.max_relative_spread,
                        )
                        or not (
                            args.short_delta_min
                            <= short_call_delta
                            <= args.short_delta_max
                        )
                    ):
                        continue
                    if abs(short_put_delta - short_call_delta) > delta_window:
                        continue

                    long_call = call_contract_by_strike.get(
                        round(short_call.strike_price + width, 4)
                    )
                    if long_call is None:
                        continue
                    long_call_snapshot = call_snapshots.get(long_call.symbol)
                    if long_call_snapshot is None:
                        continue
                    if (
                        long_call.open_interest < args.min_open_interest
                        or long_call_snapshot.bid_size <= 0
                        or long_call_snapshot.ask_size <= 0
                        or relative_spread_exceeds(
                            long_call_snapshot,
                            args.max_relative_spread,
                        )
                    ):
                        continue

                    midpoint_credit = round(
                        short_put_snapshot.midpoint
                        + short_call_snapshot.midpoint
                        - long_put_snapshot.midpoint
                        - long_call_snapshot.midpoint,
                        4,
                    )
                    natural_credit = round(
                        short_put_snapshot.bid
                        + short_call_snapshot.bid
                        - long_put_snapshot.ask
                        - long_call_snapshot.ask,
                        4,
                    )
                    if midpoint_credit < effective_min_credit(width, args):
                        continue
                    if midpoint_credit >= width or natural_credit <= 0:
                        continue

                    max_profit = midpoint_credit * 100.0
                    max_loss = (width - midpoint_credit) * 100.0
                    if max_profit <= 0 or max_loss <= 0:
                        continue
                    return_on_risk = midpoint_credit / (width - midpoint_credit)
                    if return_on_risk < args.min_return_on_risk:
                        continue

                    lower_breakeven = short_put.strike_price - midpoint_credit
                    upper_breakeven = short_call.strike_price + midpoint_credit
                    lower_cushion_pct = (spot_price - lower_breakeven) / spot_price
                    upper_cushion_pct = (upper_breakeven - spot_price) / spot_price
                    short_otm_pct = min(
                        (spot_price - short_put.strike_price) / spot_price,
                        (short_call.strike_price - spot_price) / spot_price,
                    )
                    breakeven_cushion_pct = min(
                        lower_cushion_pct,
                        upper_cushion_pct,
                    )
                    fill_ratio = clamp(natural_credit / midpoint_credit, 0.0, 1.25)
                    short_vs_expected_move = None
                    breakeven_vs_expected_move = None
                    expected_move_amount = None
                    expected_move_pct = None
                    expected_move_source_strike = None
                    if expected_move is not None:
                        expected_move_amount = expected_move.amount
                        expected_move_pct = expected_move.percent_of_spot
                        expected_move_source_strike = expected_move.reference_strike
                        lower_boundary = spot_price - expected_move.amount
                        upper_boundary = spot_price + expected_move.amount
                        short_vs_expected_move = min(
                            lower_boundary - short_put.strike_price,
                            short_call.strike_price - upper_boundary,
                        )
                        breakeven_vs_expected_move = min(
                            lower_boundary - lower_breakeven,
                            upper_breakeven - upper_boundary,
                        )

                    average_short_delta = round(
                        (short_put_delta + short_call_delta) / 2.0,
                        4,
                    )
                    long_put_delta = abs(long_put_snapshot.delta or 0.0)
                    long_call_delta = abs(long_call_snapshot.delta or 0.0)
                    average_long_delta = round(
                        (long_put_delta + long_call_delta) / 2.0,
                        4,
                    )
                    side_balance_score = round(
                        clamp(
                            1.0 - abs(short_put_delta - short_call_delta) / delta_window
                        ),
                        4,
                    )
                    leg_quote_size = min(
                        short_put_snapshot.bid_size,
                        short_put_snapshot.ask_size,
                        long_put_snapshot.bid_size,
                        long_put_snapshot.ask_size,
                        short_call_snapshot.bid_size,
                        short_call_snapshot.ask_size,
                        long_call_snapshot.bid_size,
                        long_call_snapshot.ask_size,
                    )

                    candidates.append(
                        SpreadCandidate(
                            underlying_symbol=symbol,
                            strategy="iron_condor",
                            profile=args.profile,
                            expiration_date=expiration_date,
                            days_to_expiration=days_to_expiration,
                            underlying_price=spot_price,
                            short_symbol=short_put.symbol,
                            long_symbol=long_put.symbol,
                            short_strike=short_put.strike_price,
                            long_strike=long_put.strike_price,
                            width=width,
                            short_delta=average_short_delta,
                            long_delta=average_long_delta,
                            greeks_source=(
                                short_put_snapshot.greeks_source
                                if {
                                    short_put_snapshot.greeks_source,
                                    long_put_snapshot.greeks_source,
                                    short_call_snapshot.greeks_source,
                                    long_call_snapshot.greeks_source,
                                }
                                == {short_put_snapshot.greeks_source}
                                else "mixed"
                            ),
                            short_midpoint=round(
                                short_put_snapshot.midpoint
                                + short_call_snapshot.midpoint,
                                4,
                            ),
                            long_midpoint=round(
                                long_put_snapshot.midpoint
                                + long_call_snapshot.midpoint,
                                4,
                            ),
                            short_bid=round(
                                short_put_snapshot.bid + short_call_snapshot.bid,
                                4,
                            ),
                            short_ask=round(
                                short_put_snapshot.ask + short_call_snapshot.ask,
                                4,
                            ),
                            long_bid=round(
                                long_put_snapshot.bid + long_call_snapshot.bid,
                                4,
                            ),
                            long_ask=round(
                                long_put_snapshot.ask + long_call_snapshot.ask,
                                4,
                            ),
                            midpoint_credit=midpoint_credit,
                            natural_credit=natural_credit,
                            max_profit=max_profit,
                            max_loss=max_loss,
                            return_on_risk=return_on_risk,
                            breakeven=spot_price,
                            breakeven_cushion_pct=breakeven_cushion_pct,
                            short_otm_pct=short_otm_pct,
                            short_open_interest=min(
                                short_put.open_interest,
                                short_call.open_interest,
                            ),
                            long_open_interest=min(
                                long_put.open_interest,
                                long_call.open_interest,
                            ),
                            short_relative_spread=max(
                                relative_spread(short_put_snapshot),
                                relative_spread(short_call_snapshot),
                            ),
                            long_relative_spread=max(
                                relative_spread(long_put_snapshot),
                                relative_spread(long_call_snapshot),
                            ),
                            fill_ratio=fill_ratio,
                            min_quote_size=leg_quote_size,
                            expected_move=expected_move_amount,
                            expected_move_pct=expected_move_pct,
                            expected_move_source_strike=expected_move_source_strike,
                            short_vs_expected_move=short_vs_expected_move,
                            breakeven_vs_expected_move=breakeven_vs_expected_move,
                            order_payload=make_iron_condor_order_payload(
                                short_put_symbol=short_put.symbol,
                                long_put_symbol=long_put.symbol,
                                short_call_symbol=short_call.symbol,
                                long_call_symbol=long_call.symbol,
                                limit_price=midpoint_credit,
                            ),
                            short_bid_size=min(
                                short_put_snapshot.bid_size,
                                short_call_snapshot.bid_size,
                            ),
                            short_ask_size=min(
                                short_put_snapshot.ask_size,
                                short_call_snapshot.ask_size,
                            ),
                            long_bid_size=min(
                                long_put_snapshot.bid_size,
                                long_call_snapshot.bid_size,
                            ),
                            long_ask_size=min(
                                long_put_snapshot.ask_size,
                                long_call_snapshot.ask_size,
                            ),
                            short_implied_volatility=round(
                                (
                                    (short_put_snapshot.implied_volatility or 0.0)
                                    + (short_call_snapshot.implied_volatility or 0.0)
                                )
                                / 2.0,
                                4,
                            ),
                            long_implied_volatility=round(
                                (
                                    (long_put_snapshot.implied_volatility or 0.0)
                                    + (long_call_snapshot.implied_volatility or 0.0)
                                )
                                / 2.0,
                                4,
                            ),
                            short_volume=(short_put_snapshot.daily_volume or 0)
                            + (short_call_snapshot.daily_volume or 0),
                            long_volume=(long_put_snapshot.daily_volume or 0)
                            + (long_call_snapshot.daily_volume or 0),
                            secondary_short_symbol=short_call.symbol,
                            secondary_long_symbol=long_call.symbol,
                            secondary_short_strike=short_call.strike_price,
                            secondary_long_strike=long_call.strike_price,
                            lower_breakeven=lower_breakeven,
                            upper_breakeven=upper_breakeven,
                            side_balance_score=side_balance_score,
                            wing_symmetry_ratio=1.0,
                        )
                    )

    return candidates


def build_credit_spreads(
    *,
    symbol: str,
    strategy: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    return build_vertical_spreads(
        symbol=symbol,
        strategy=strategy,
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )


def _ratio_or_none(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0.0):
        return None
    return round(numerator / denominator, 4)


def build_long_straddles(
    *,
    symbol: str,
    spot_price: float,
    call_contracts_by_expiration: dict[str, list[OptionContract]],
    put_contracts_by_expiration: dict[str, list[OptionContract]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []

    for expiration_date, call_contracts in sorted(call_contracts_by_expiration.items()):
        put_contracts = put_contracts_by_expiration.get(expiration_date, [])
        call_snapshot_map = call_snapshots_by_expiration.get(expiration_date, {})
        put_snapshot_map = put_snapshots_by_expiration.get(expiration_date, {})
        put_by_strike = {contract.strike_price: contract for contract in put_contracts}
        expected_move = expected_moves_by_expiration.get(expiration_date)
        days_to_expiration = days_from_today(expiration_date)

        shared_contracts = sorted(
            (
                contract
                for contract in call_contracts
                if contract.strike_price in put_by_strike
            ),
            key=lambda contract: (
                abs(contract.strike_price - spot_price),
                contract.strike_price,
            ),
        )
        for call_contract in shared_contracts:
            put_contract = put_by_strike.get(call_contract.strike_price)
            if put_contract is None:
                continue
            call_snapshot = call_snapshot_map.get(call_contract.symbol)
            put_snapshot = put_snapshot_map.get(put_contract.symbol)
            if call_snapshot is None or put_snapshot is None:
                continue
            if (
                call_contract.open_interest < args.min_open_interest
                or put_contract.open_interest < args.min_open_interest
                or call_snapshot.bid_size <= 0
                or call_snapshot.ask_size <= 0
                or put_snapshot.bid_size <= 0
                or put_snapshot.ask_size <= 0
                or relative_spread_exceeds(call_snapshot, args.max_relative_spread)
                or relative_spread_exceeds(put_snapshot, args.max_relative_spread)
            ):
                continue

            call_delta = abs(call_snapshot.delta or 0.0)
            put_delta = abs(put_snapshot.delta or 0.0)
            if call_snapshot.delta is None or put_snapshot.delta is None:
                continue
            if abs(call_delta - 0.50) > 0.20 or abs(put_delta - 0.50) > 0.20:
                continue

            midpoint_credit = round(call_snapshot.midpoint + put_snapshot.midpoint, 4)
            natural_credit = round(call_snapshot.ask + put_snapshot.ask, 4)
            if midpoint_credit < args.min_credit or natural_credit <= 0:
                continue

            break_even_move = midpoint_credit
            modeled_move_vs_implied_move = _ratio_or_none(
                expected_move.amount if expected_move is not None else None,
                midpoint_credit,
            )
            modeled_move_vs_break_even_move = modeled_move_vs_implied_move
            return_on_risk = (
                modeled_move_vs_break_even_move
                or modeled_move_vs_implied_move
                or round(max(call_delta, put_delta), 4)
            )
            fill_ratio = clamp(midpoint_credit / natural_credit, 0.0, 1.25)
            strike = call_contract.strike_price
            short_otm_pct = (
                abs(strike - spot_price) / spot_price if spot_price > 0 else 0.0
            )
            breakeven_cushion_pct = (
                break_even_move / spot_price if spot_price > 0 else 0.0
            )
            average_delta = round((call_delta + put_delta) / 2.0, 4)
            side_balance_score = round(
                clamp(1.0 - abs(call_delta - put_delta) / 0.35),
                4,
            )
            expected_move_amount = expected_move_pct = expected_move_source_strike = (
                None
            )
            short_vs_expected_move = breakeven_vs_expected_move = None
            if expected_move is not None:
                expected_move_amount = expected_move.amount
                expected_move_pct = expected_move.percent_of_spot
                expected_move_source_strike = expected_move.reference_strike
                short_vs_expected_move = expected_move.amount - midpoint_credit
                breakeven_vs_expected_move = expected_move.amount - break_even_move

            candidates.append(
                SpreadCandidate(
                    underlying_symbol=symbol,
                    strategy="long_straddle",
                    profile=args.profile,
                    expiration_date=expiration_date,
                    days_to_expiration=days_to_expiration,
                    underlying_price=spot_price,
                    short_symbol=put_contract.symbol,
                    long_symbol=call_contract.symbol,
                    short_strike=put_contract.strike_price,
                    long_strike=call_contract.strike_price,
                    width=0.0,
                    short_delta=average_delta,
                    long_delta=average_delta,
                    greeks_source=call_snapshot.greeks_source
                    if call_snapshot.greeks_source == put_snapshot.greeks_source
                    else "mixed",
                    short_midpoint=put_snapshot.midpoint,
                    long_midpoint=call_snapshot.midpoint,
                    short_bid=put_snapshot.bid,
                    short_ask=put_snapshot.ask,
                    long_bid=call_snapshot.bid,
                    long_ask=call_snapshot.ask,
                    midpoint_credit=midpoint_credit,
                    natural_credit=natural_credit,
                    max_profit=round(
                        max(
                            (expected_move_amount or midpoint_credit) - midpoint_credit,
                            0.01,
                        )
                        * 100.0,
                        2,
                    ),
                    max_loss=round(midpoint_credit * 100.0, 2),
                    return_on_risk=return_on_risk,
                    breakeven=strike,
                    breakeven_cushion_pct=breakeven_cushion_pct,
                    short_otm_pct=short_otm_pct,
                    short_open_interest=put_contract.open_interest,
                    long_open_interest=call_contract.open_interest,
                    short_relative_spread=relative_spread(put_snapshot),
                    long_relative_spread=relative_spread(call_snapshot),
                    fill_ratio=fill_ratio,
                    min_quote_size=min(
                        put_snapshot.bid_size,
                        put_snapshot.ask_size,
                        call_snapshot.bid_size,
                        call_snapshot.ask_size,
                    ),
                    order_payload=make_long_vol_order_payload(
                        symbols=(put_contract.symbol, call_contract.symbol),
                        limit_price=midpoint_credit,
                        strategy="long_straddle",
                    ),
                    expected_move=expected_move_amount,
                    expected_move_pct=expected_move_pct,
                    expected_move_source_strike=expected_move_source_strike,
                    modeled_move_vs_implied_move=modeled_move_vs_implied_move,
                    modeled_move_vs_break_even_move=modeled_move_vs_break_even_move,
                    short_vs_expected_move=short_vs_expected_move,
                    breakeven_vs_expected_move=breakeven_vs_expected_move,
                    short_bid_size=put_snapshot.bid_size,
                    short_ask_size=put_snapshot.ask_size,
                    long_bid_size=call_snapshot.bid_size,
                    long_ask_size=call_snapshot.ask_size,
                    short_implied_volatility=put_snapshot.implied_volatility,
                    long_implied_volatility=call_snapshot.implied_volatility,
                    short_volume=put_snapshot.daily_volume,
                    long_volume=call_snapshot.daily_volume,
                    lower_breakeven=round(strike - midpoint_credit, 4),
                    upper_breakeven=round(strike + midpoint_credit, 4),
                    side_balance_score=side_balance_score,
                    wing_symmetry_ratio=1.0,
                )
            )

    return candidates


def build_long_strangles(
    *,
    symbol: str,
    spot_price: float,
    call_contracts_by_expiration: dict[str, list[OptionContract]],
    put_contracts_by_expiration: dict[str, list[OptionContract]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []
    delta_window = max(args.short_delta_max - args.short_delta_min, 0.05)

    for expiration_date, call_contracts in sorted(call_contracts_by_expiration.items()):
        put_contracts = put_contracts_by_expiration.get(expiration_date, [])
        call_snapshot_map = call_snapshots_by_expiration.get(expiration_date, {})
        put_snapshot_map = put_snapshots_by_expiration.get(expiration_date, {})
        expected_move = expected_moves_by_expiration.get(expiration_date)
        days_to_expiration = days_from_today(expiration_date)

        eligible_calls = [
            contract
            for contract in call_contracts
            if contract.strike_price > spot_price
            and contract.open_interest >= args.min_open_interest
            and (call_snapshot_map.get(contract.symbol) is not None)
        ]
        eligible_puts = [
            contract
            for contract in put_contracts
            if contract.strike_price < spot_price
            and contract.open_interest >= args.min_open_interest
            and (put_snapshot_map.get(contract.symbol) is not None)
        ]
        eligible_calls.sort(
            key=lambda contract: abs(
                abs(call_snapshot_map[contract.symbol].delta or 0.0)
                - args.short_delta_target
            )
        )
        eligible_puts.sort(
            key=lambda contract: abs(
                abs(put_snapshot_map[contract.symbol].delta or 0.0)
                - args.short_delta_target
            )
        )

        for put_contract in eligible_puts[:6]:
            put_snapshot = put_snapshot_map.get(put_contract.symbol)
            if put_snapshot is None or put_snapshot.delta is None:
                continue
            put_delta = abs(put_snapshot.delta)
            if not (args.short_delta_min <= put_delta <= args.short_delta_max):
                continue
            if (
                put_snapshot.bid_size <= 0
                or put_snapshot.ask_size <= 0
                or relative_spread_exceeds(put_snapshot, args.max_relative_spread)
            ):
                continue

            for call_contract in eligible_calls[:6]:
                call_snapshot = call_snapshot_map.get(call_contract.symbol)
                if call_snapshot is None or call_snapshot.delta is None:
                    continue
                call_delta = abs(call_snapshot.delta)
                if not (args.short_delta_min <= call_delta <= args.short_delta_max):
                    continue
                if (
                    call_snapshot.bid_size <= 0
                    or call_snapshot.ask_size <= 0
                    or relative_spread_exceeds(call_snapshot, args.max_relative_spread)
                ):
                    continue

                lower_distance = spot_price - put_contract.strike_price
                upper_distance = call_contract.strike_price - spot_price
                if lower_distance <= 0 or upper_distance <= 0:
                    continue
                wing_symmetry_ratio = round(
                    min(lower_distance, upper_distance)
                    / max(lower_distance, upper_distance),
                    4,
                )
                if wing_symmetry_ratio < 0.55:
                    continue

                midpoint_credit = round(
                    put_snapshot.midpoint + call_snapshot.midpoint, 4
                )
                natural_credit = round(put_snapshot.ask + call_snapshot.ask, 4)
                if midpoint_credit < args.min_credit or natural_credit <= 0:
                    continue

                break_even_move = min(
                    lower_distance + midpoint_credit, upper_distance + midpoint_credit
                )
                modeled_move_vs_implied_move = _ratio_or_none(
                    expected_move.amount if expected_move is not None else None,
                    midpoint_credit,
                )
                modeled_move_vs_break_even_move = _ratio_or_none(
                    expected_move.amount if expected_move is not None else None,
                    break_even_move,
                )
                return_on_risk = (
                    modeled_move_vs_break_even_move
                    or modeled_move_vs_implied_move
                    or round((put_delta + call_delta) / 2.0, 4)
                )
                fill_ratio = clamp(midpoint_credit / natural_credit, 0.0, 1.25)
                short_otm_pct = (
                    min(lower_distance, upper_distance) / spot_price
                    if spot_price > 0
                    else 0.0
                )
                breakeven_cushion_pct = (
                    break_even_move / spot_price if spot_price > 0 else 0.0
                )
                average_delta = round((put_delta + call_delta) / 2.0, 4)
                side_balance_score = round(
                    clamp(1.0 - abs(put_delta - call_delta) / delta_window),
                    4,
                )
                expected_move_amount = expected_move_pct = (
                    expected_move_source_strike
                ) = None
                short_vs_expected_move = breakeven_vs_expected_move = None
                if expected_move is not None:
                    expected_move_amount = expected_move.amount
                    expected_move_pct = expected_move.percent_of_spot
                    expected_move_source_strike = expected_move.reference_strike
                    short_vs_expected_move = expected_move.amount - min(
                        lower_distance, upper_distance
                    )
                    breakeven_vs_expected_move = expected_move.amount - break_even_move

                candidates.append(
                    SpreadCandidate(
                        underlying_symbol=symbol,
                        strategy="long_strangle",
                        profile=args.profile,
                        expiration_date=expiration_date,
                        days_to_expiration=days_to_expiration,
                        underlying_price=spot_price,
                        short_symbol=put_contract.symbol,
                        long_symbol=call_contract.symbol,
                        short_strike=put_contract.strike_price,
                        long_strike=call_contract.strike_price,
                        width=round(
                            call_contract.strike_price - put_contract.strike_price, 4
                        ),
                        short_delta=average_delta,
                        long_delta=average_delta,
                        greeks_source=call_snapshot.greeks_source
                        if call_snapshot.greeks_source == put_snapshot.greeks_source
                        else "mixed",
                        short_midpoint=put_snapshot.midpoint,
                        long_midpoint=call_snapshot.midpoint,
                        short_bid=put_snapshot.bid,
                        short_ask=put_snapshot.ask,
                        long_bid=call_snapshot.bid,
                        long_ask=call_snapshot.ask,
                        midpoint_credit=midpoint_credit,
                        natural_credit=natural_credit,
                        max_profit=round(
                            max(
                                (expected_move_amount or break_even_move)
                                - break_even_move,
                                0.01,
                            )
                            * 100.0,
                            2,
                        ),
                        max_loss=round(midpoint_credit * 100.0, 2),
                        return_on_risk=return_on_risk,
                        breakeven=spot_price,
                        breakeven_cushion_pct=breakeven_cushion_pct,
                        short_otm_pct=short_otm_pct,
                        short_open_interest=put_contract.open_interest,
                        long_open_interest=call_contract.open_interest,
                        short_relative_spread=relative_spread(put_snapshot),
                        long_relative_spread=relative_spread(call_snapshot),
                        fill_ratio=fill_ratio,
                        min_quote_size=min(
                            put_snapshot.bid_size,
                            put_snapshot.ask_size,
                            call_snapshot.bid_size,
                            call_snapshot.ask_size,
                        ),
                        order_payload=make_long_vol_order_payload(
                            symbols=(put_contract.symbol, call_contract.symbol),
                            limit_price=midpoint_credit,
                            strategy="long_strangle",
                        ),
                        expected_move=expected_move_amount,
                        expected_move_pct=expected_move_pct,
                        expected_move_source_strike=expected_move_source_strike,
                        modeled_move_vs_implied_move=modeled_move_vs_implied_move,
                        modeled_move_vs_break_even_move=modeled_move_vs_break_even_move,
                        short_vs_expected_move=short_vs_expected_move,
                        breakeven_vs_expected_move=breakeven_vs_expected_move,
                        short_bid_size=put_snapshot.bid_size,
                        short_ask_size=put_snapshot.ask_size,
                        long_bid_size=call_snapshot.bid_size,
                        long_ask_size=call_snapshot.ask_size,
                        short_implied_volatility=put_snapshot.implied_volatility,
                        long_implied_volatility=call_snapshot.implied_volatility,
                        short_volume=put_snapshot.daily_volume,
                        long_volume=call_snapshot.daily_volume,
                        lower_breakeven=round(
                            put_contract.strike_price - midpoint_credit, 4
                        ),
                        upper_breakeven=round(
                            call_contract.strike_price + midpoint_credit, 4
                        ),
                        side_balance_score=side_balance_score,
                        wing_symmetry_ratio=wing_symmetry_ratio,
                    )
                )

    return candidates


def score_candidate(candidate: SpreadCandidate, args: argparse.Namespace) -> float:
    premium_kind = net_premium_kind(candidate.strategy)
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES
    session_bucket = zero_dte_session_bucket() if args.profile == "0dte" else None
    if args.profile == "0dte":
        delta_target = zero_dte_delta_target(session_bucket or "off_hours")
    elif candidate.strategy == "long_straddle":
        delta_target = 0.50
    else:
        delta_target = args.short_delta_target
    delta_half_band = max((args.short_delta_max - args.short_delta_min) / 2.0, 0.01)
    if candidate.strategy == "long_straddle":
        delta_half_band = 0.20
    delta_score = 1.0
    if candidate.short_delta is not None:
        delta_score = 1.0 - min(
            abs(abs(candidate.short_delta) - delta_target) / delta_half_band, 1.0
        )

    dte_target = (args.min_dte + args.max_dte) / 2.0
    dte_half_band = max((args.max_dte - args.min_dte) / 2.0, 1.0)
    dte_score = 1.0 - min(
        abs(candidate.days_to_expiration - dte_target) / dte_half_band, 1.0
    )

    fill_score = clamp(candidate.fill_ratio)
    liquidity_score = 0.75 * log_scaled_score(
        min(candidate.short_open_interest, candidate.long_open_interest),
        floor=max(args.min_open_interest, 1),
        ceiling=max(args.min_open_interest * 8, 10),
    ) + 0.25 * clamp(candidate.min_quote_size / 100.0)

    if candidate.strategy == "long_straddle":
        width_target = 0.0
    elif candidate.strategy == "long_strangle" and candidate.expected_move is not None:
        width_target = max(candidate.expected_move * 2.0, args.min_width)
    elif args.profile == "0dte":
        width_target = 2.0 if session_bucket == "late" else 1.0
    else:
        width_target = max(
            args.min_width, 2.0 if args.profile == "core" else args.min_width
        )
    width_window = max(args.max_width - args.min_width, 1.0)
    if candidate.strategy == "long_straddle":
        width_window = 1.0
    elif candidate.strategy == "long_strangle" and candidate.expected_move is not None:
        width_window = max(candidate.expected_move, 1.0)
    width_score = 1.0 - min(abs(candidate.width - width_target) / width_window, 1.0)

    if long_vol:
        return_on_risk_score = clamp(candidate.return_on_risk / 1.10)
    else:
        return_on_risk_score = clamp(candidate.return_on_risk / 0.60)
    if long_vol:
        breakeven_cushion_score = clamp(1.0 - (candidate.breakeven_cushion_pct / 0.06))
    elif premium_kind == "debit":
        breakeven_cushion_score = clamp(1.0 - (candidate.breakeven_cushion_pct / 0.08))
    else:
        breakeven_cushion_score = clamp(candidate.breakeven_cushion_pct / 0.035)

    if long_vol:
        short_expected_move_score = clamp(
            ((candidate.modeled_move_vs_implied_move or 0.85) - 0.80) / 0.35
        )
        breakeven_expected_move_score = clamp(
            ((candidate.modeled_move_vs_break_even_move or 0.85) - 0.80) / 0.30
        )
    elif candidate.expected_move and candidate.expected_move > 0:
        short_expected_move_score = clamp(
            0.50 + (candidate.short_vs_expected_move or 0.0) / candidate.expected_move
        )
        breakeven_expected_move_score = clamp(
            0.45
            + (candidate.breakeven_vs_expected_move or 0.0) / candidate.expected_move
        )
    else:
        short_expected_move_score = clamp(candidate.short_otm_pct / 0.03)
        breakeven_expected_move_score = breakeven_cushion_score

    base_score = (
        0.24 * delta_score
        + 0.18 * short_expected_move_score
        + 0.16 * breakeven_expected_move_score
        + 0.14 * fill_score
        + 0.12 * liquidity_score
        + 0.08 * width_score
        + 0.05 * dte_score
        + 0.03 * return_on_risk_score
    )

    calendar_multiplier = {
        "clean": 1.0,
        "penalized": 0.92,
        "unknown": 0.82,
        "blocked": 0.0,
    }.get(candidate.calendar_status, 1.0)
    setup_multiplier = {
        "favorable": 1.0,
        "neutral": 0.93,
        "unfavorable": 0.78,
        "unknown": 0.88,
    }.get(candidate.setup_status, 0.88)
    data_multiplier = {
        "clean": 1.0,
        "penalized": 0.90,
        "blocked": 0.0,
    }.get(candidate.data_status, 1.0)
    return round(
        base_score * calendar_multiplier * setup_multiplier * data_multiplier * 100.0, 1
    )


def rank_candidates(
    candidates: list[SpreadCandidate], args: argparse.Namespace
) -> list[SpreadCandidate]:
    ranked = [
        replace(candidate, quality_score=score_candidate(candidate, args))
        for candidate in candidates
    ]
    return sort_candidates_for_display(ranked)


def sort_candidates_for_display(
    candidates: list[SpreadCandidate],
) -> list[SpreadCandidate]:
    ranked = list(candidates)
    ranked.sort(
        key=lambda candidate: (
            candidate.quality_score,
            candidate.return_on_risk,
            candidate.midpoint_credit,
            min(candidate.short_open_interest, candidate.long_open_interest),
        ),
        reverse=True,
    )
    return ranked


def format_dte_label(days_to_expiration: int) -> str:
    return "0D" if days_to_expiration == 0 else str(days_to_expiration)


def build_table_rows(
    candidates: list[SpreadCandidate], *, include_strategy: bool = False
) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        row = []
        if include_strategy:
            row.append(strategy_display_label(candidate.strategy))
        row.extend(
            [
                candidate.expiration_date,
                format_dte_label(candidate.days_to_expiration),
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{candidate.width:.2f}",
                f"{candidate.midpoint_credit:.2f}",
                f"{candidate.return_on_risk * 100:.1f}",
                f"{candidate.quality_score:.1f}",
                "n/a"
                if candidate.short_delta is None
                else f"{candidate.short_delta:.2f}",
                f"{candidate.short_otm_pct * 100:.1f}",
                f"{candidate.breakeven_cushion_pct * 100:.1f}",
                "n/a"
                if candidate.short_vs_expected_move is None
                else f"{candidate.short_vs_expected_move:.2f}",
                f"{min(candidate.short_open_interest, candidate.long_open_interest)}",
                candidate.calendar_status,
                candidate.data_status,
                "n/a"
                if candidate.calendar_days_to_nearest_event is None
                else str(candidate.calendar_days_to_nearest_event),
            ]
        )
        rows.append(row)
    return rows


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt_row(row: list[str]) -> str:
        return " | ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row))

    separator = "-+-".join("-" * width for width in widths)
    rendered = [fmt_row(headers), separator]
    rendered.extend(fmt_row(row) for row in rows)
    return "\n".join(rendered)


def print_human_readable(
    symbol: str,
    spot_price: float,
    candidates: list[SpreadCandidate],
    show_order_json: bool,
    setup: UnderlyingSetupContext | None,
    *,
    strategy: str,
    profile: str,
    greeks_source: str,
    setup_summaries: tuple[str, ...] = (),
) -> None:
    print(f"{symbol.upper()} spot: {spot_price:.2f}")
    print(f"Strategy: {strategy}")
    print(f"Profile: {profile}")
    print(f"Greeks: {greeks_source}")
    if profile == "0dte":
        print(f"0DTE session: {format_session_bucket(zero_dte_session_bucket())}")
    if setup is not None:
        print(f"Setup: {setup.status} ({setup.score:.1f})")
        if setup.reasons:
            print(f"Setup notes: {'; '.join(setup.reasons)}")
    elif setup_summaries:
        print(f"Setups: {'; '.join(setup_summaries)}")
    print(f"Candidates found: {len(candidates)}")
    print()

    if not candidates:
        print("No option structures matched the current filters and calendar policy.")
        return

    include_strategy = (
        strategy == "combined"
        or len({candidate.strategy for candidate in candidates}) > 1
    )
    headers = [
        "Expiry",
        "DTE",
        "Short",
        "Long",
        "Width",
        "Entry",
        "ROR%",
        "Score",
        "Δ",
        "OTM%",
        "BE%",
        "S-EM",
        "MinOI",
        "Cal",
        "DQ",
        "EvtD",
    ]
    if include_strategy:
        headers = ["Side", *headers]
    rows = build_table_rows(candidates, include_strategy=include_strategy)
    print(format_table(headers, rows))
    print()

    for index, candidate in enumerate(candidates, start=1):
        print(
            f"{index}. [{strategy_display_label(candidate.strategy)}] {candidate.short_symbol} -> {candidate.long_symbol} | "
            f"score {candidate.quality_score:.1f} | "
            f"breakeven {candidate.breakeven:.2f} | "
            f"calendar {candidate.calendar_status}"
        )
        if candidate.greeks_source != "alpaca":
            print(f"   greeks: {candidate.greeks_source}")
        if candidate.expected_move is not None:
            print(
                "   expected move: "
                f"{candidate.expected_move:.2f} ({candidate.expected_move_pct * 100:.2f}% of spot) "
                f"from {candidate.expected_move_source_strike:.2f} strike"
            )
        if candidate.calendar_reasons:
            print(f"   reasons: {'; '.join(candidate.calendar_reasons)}")
        if candidate.data_reasons:
            print(f"   data: {'; '.join(candidate.data_reasons)}")
        if candidate.calendar_sources:
            source_line = ", ".join(candidate.calendar_sources)
            print(
                f"   sources: {source_line} | confidence {candidate.calendar_confidence}"
            )
        if candidate.macro_regime:
            print(f"   macro regime: {candidate.macro_regime}")
        if candidate.setup_score is not None:
            print(f"   setup: {candidate.setup_status} ({candidate.setup_score:.1f})")
        if show_order_json:
            print("   order payload:")
            print(json.dumps(candidate.order_payload, indent=2))
        print()


def write_csv(path: str, candidates: list[SpreadCandidate]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "underlying_symbol",
        "strategy",
        "profile",
        "expiration_date",
        "days_to_expiration",
        "underlying_price",
        "short_symbol",
        "long_symbol",
        "short_strike",
        "long_strike",
        "width",
        "short_delta",
        "long_delta",
        "greeks_source",
        "short_midpoint",
        "long_midpoint",
        "short_bid",
        "short_ask",
        "long_bid",
        "long_ask",
        "midpoint_credit",
        "natural_credit",
        "max_profit",
        "max_loss",
        "return_on_risk",
        "breakeven",
        "breakeven_cushion_pct",
        "short_otm_pct",
        "short_open_interest",
        "long_open_interest",
        "short_relative_spread",
        "long_relative_spread",
        "fill_ratio",
        "min_quote_size",
        "expected_move",
        "expected_move_pct",
        "expected_move_source_strike",
        "debit_width_ratio",
        "modeled_move_vs_implied_move",
        "modeled_move_vs_break_even_move",
        "short_vs_expected_move",
        "breakeven_vs_expected_move",
        "quality_score",
        "calendar_status",
        "calendar_reasons",
        "calendar_confidence",
        "calendar_sources",
        "calendar_last_updated",
        "calendar_days_to_nearest_event",
        "macro_regime",
        "earnings_phase",
        "earnings_event_date",
        "earnings_session_timing",
        "earnings_cohort_key",
        "earnings_days_to_event",
        "earnings_days_since_event",
        "earnings_timing_confidence",
        "earnings_horizon_crosses_report",
        "earnings_primary_source",
        "earnings_supporting_sources",
        "earnings_consensus_status",
        "setup_status",
        "setup_score",
        "setup_reasons",
        "setup_daily_score",
        "setup_intraday_score",
        "setup_intraday_minutes",
        "setup_has_intraday_context",
        "setup_spot_vs_vwap_pct",
        "setup_intraday_return_pct",
        "setup_distance_to_session_extreme_pct",
        "setup_opening_range_break_pct",
        "setup_latest_close",
        "setup_vwap",
        "setup_opening_range_high",
        "setup_opening_range_low",
        "data_status",
        "data_reasons",
        "selection_notes",
        "order_payload",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for candidate in candidates:
            row = asdict(candidate)
            row["calendar_reasons"] = "; ".join(candidate.calendar_reasons)
            row["calendar_sources"] = ", ".join(candidate.calendar_sources)
            row["setup_reasons"] = "; ".join(candidate.setup_reasons)
            row["data_reasons"] = "; ".join(candidate.data_reasons)
            row["selection_notes"] = ", ".join(candidate.selection_notes)
            row["order_payload"] = json.dumps(
                candidate.order_payload, separators=(",", ":")
            )
            writer.writerow(row)


def serialize_setup_context(
    setup: UnderlyingSetupContext | None,
) -> dict[str, Any] | None:
    if setup is None:
        return None
    payload = asdict(setup)
    payload["reasons"] = list(setup.reasons)
    return payload


def write_json(
    path: str,
    symbol: str,
    spot_price: float,
    args: argparse.Namespace,
    candidates: list[SpreadCandidate],
    *,
    run_id: str | None = None,
    setup: UnderlyingSetupContext | None = None,
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": symbol,
        "spot_price": spot_price,
        "generated_at": datetime.now(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "run_id": run_id,
        "filters": build_filter_payload(args),
        "setup": serialize_setup_context(setup),
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_ranked_candidate_rows(
    candidates: list[SpreadCandidate], *, include_strategy: bool = False
) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        row = [candidate.underlying_symbol]
        if include_strategy:
            row.append(strategy_display_label(candidate.strategy))
        row.extend(
            [
                candidate.expiration_date,
                format_dte_label(candidate.days_to_expiration),
                f"{candidate.underlying_price:.2f}",
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{candidate.midpoint_credit:.2f}",
                f"{candidate.quality_score:.1f}",
                "n/a"
                if candidate.short_delta is None
                else f"{candidate.short_delta:.2f}",
                f"{candidate.breakeven_cushion_pct * 100:.1f}",
                "n/a"
                if candidate.short_vs_expected_move is None
                else f"{candidate.short_vs_expected_move:.2f}",
                candidate.calendar_status,
                candidate.data_status,
                candidate.setup_status,
                ",".join(candidate.selection_notes),
            ]
        )
        rows.append(row)
    return rows


def print_ranked_candidates(
    *,
    label: str,
    strategy: str,
    profile: str,
    greeks_source: str,
    symbols: list[str],
    ranked_candidates: list[SpreadCandidate],
    failures: list[UniverseScanFailure],
) -> None:
    print(f"Universe: {label}")
    print(f"Strategy: {strategy}")
    print(f"Greeks: {greeks_source}")
    if profile == "0dte" or (
        ranked_candidates
        and any(candidate.profile == "0dte" for candidate in ranked_candidates)
    ):
        print(f"0DTE session: {format_session_bucket(zero_dte_session_bucket())}")
    print(f"Symbols requested: {len(symbols)}")
    print(f"Top candidates: {len(ranked_candidates)}")
    if failures:
        print(f"Failures: {len(failures)}")
    print()

    if ranked_candidates:
        include_strategy = (
            strategy == "combined"
            or len({candidate.strategy for candidate in ranked_candidates}) > 1
        )
        headers = [
            "Symbol",
            "Expiry",
            "DTE",
            "Spot",
            "Short",
            "Long",
            "MidCr",
            "Score",
            "Δ",
            "BE%",
            "S-EM",
            "Cal",
            "DQ",
            "Setup",
            "Why",
        ]
        if include_strategy:
            headers = ["Symbol", "Side", *headers[1:]]
        print(
            format_table(
                headers,
                build_ranked_candidate_rows(
                    ranked_candidates, include_strategy=include_strategy
                ),
            )
        )
        print()
    else:
        print("No universe candidates matched the current filters.")
        print()

    for index, candidate in enumerate(ranked_candidates, start=1):
        print(
            f"{index}. {candidate.underlying_symbol} [{strategy_display_label(candidate.strategy)}] "
            f"{candidate.short_symbol} -> {candidate.long_symbol} | "
            f"score {candidate.quality_score:.1f} | breakeven {candidate.breakeven:.2f}"
        )
        if candidate.selection_notes:
            print(f"   why: {', '.join(candidate.selection_notes)}")
        if candidate.calendar_reasons:
            print(f"   calendar: {'; '.join(candidate.calendar_reasons)}")
        if candidate.data_reasons:
            print(f"   data: {'; '.join(candidate.data_reasons)}")
        if candidate.setup_reasons:
            print(f"   setup: {'; '.join(candidate.setup_reasons)}")
        print()

    if failures:
        print("Failures:")
        for failure in failures:
            print(f"- {failure.symbol}: {failure.error}")


def write_universe_csv(path: str, candidates: list[SpreadCandidate]) -> None:
    write_csv(path, candidates)


def write_universe_json(
    path: str,
    *,
    label: str,
    strategy: str,
    symbols: list[str],
    candidates: list[SpreadCandidate],
    failures: list[UniverseScanFailure],
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": "universe",
        "label": label,
        "strategy": strategy,
        "symbols": symbols,
        "generated_at": datetime.now(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "candidate_count": len(candidates),
        "failures": [asdict(failure) for failure in failures],
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_stream_symbols(
    candidates: list[SpreadCandidate], *, max_symbols: int = 16
) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        for leg in candidate_legs(asdict(candidate)):
            symbol = str(leg.get("symbol") or "").strip()
            if symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
            if len(symbols) >= max_symbols:
                return symbols
    return symbols


def build_live_spread_rows(
    candidates: list[SpreadCandidate],
    live_quotes: dict[str, LiveOptionQuote],
) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        payload = asdict(candidate)
        live_snapshot = structure_quote_snapshot(
            legs=candidate_legs(payload),
            strategy_family=payload.get("strategy"),
            quotes_by_symbol=live_quotes,
        )
        if live_snapshot is None:
            continue
        primary_label = f"{candidate.short_strike:.2f}/{candidate.long_strike:.2f}"
        if (
            candidate.secondary_short_strike is not None
            and candidate.secondary_long_strike is not None
        ):
            primary_label = (
                f"{candidate.long_strike:.2f}-{candidate.short_strike:.2f}"
                f" / {candidate.secondary_short_strike:.2f}-{candidate.secondary_long_strike:.2f}"
            )
        rows.append(
            [
                strategy_display_label(candidate.strategy),
                candidate.expiration_date,
                primary_label,
                f"{candidate.width:.2f}",
                f"{float(live_snapshot['midpoint_value']):.2f}",
                f"{float(live_snapshot['natural_value']):.2f}",
                str(len(live_snapshot.get("legs") or [])),
                "n/a"
                if live_snapshot.get("captured_at") is None
                else str(live_snapshot["captured_at"]),
            ]
        )
    return rows


def maybe_stream_live_quotes(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    candidates: list[SpreadCandidate],
) -> None:
    if not args.stream_live_quotes or args.json or not candidates:
        return

    stream_symbols = build_stream_symbols(candidates[: args.top])
    if not stream_symbols:
        return

    print()
    print(
        f"Streaming live option quotes for {len(stream_symbols)} legs via Alpaca websocket..."
    )
    try:
        streamer = AlpacaOptionQuoteStreamer(
            key_id=client.headers["APCA-API-KEY-ID"],
            secret_key=client.headers["APCA-API-SECRET-KEY"],
            data_base_url=client.data_base_url,
            feed=args.feed,
        )
        live_quotes = streamer.stream_quotes(
            stream_symbols, duration_seconds=args.stream_seconds
        )
    except Exception as exc:
        print(f"Live quote stream unavailable: {exc}")
        return

    if not live_quotes:
        print("Live quote stream returned no quote updates.")
        return

    rows = build_live_spread_rows(candidates[: args.top], live_quotes)
    if not rows:
        print("Live quote stream did not return both legs for the displayed spreads.")
        return

    headers = [
        "Side",
        "Expiry",
        "Strikes",
        "Width",
        "LiveMid",
        "LiveNat",
        "Legs",
        "Time",
    ]
    print(format_table(headers, rows))
    print()


def build_calendar_reason_messages(decision: CalendarPolicyDecision) -> tuple[str, ...]:
    return tuple(reason.message for reason in decision.reasons)


def attach_calendar_decisions(
    *,
    symbol: str,
    strategy: str,
    underlying_type: str,
    candidates: list[SpreadCandidate],
    resolver: Any,
    calendar_policy: str,
    refresh_calendar_events: bool,
) -> list[SpreadCandidate]:
    if calendar_policy == "off" or not candidates:
        return candidates

    window_start = datetime.now(UTC).isoformat()
    decisions_by_expiration: dict[str, CalendarPolicyDecision] = {}
    for expiration_date in sorted(
        {candidate.expiration_date for candidate in candidates}, reverse=True
    ):
        context = resolver.resolve_calendar_context(
            symbol=symbol,
            strategy=strategy,
            window_start=window_start,
            window_end=option_expiry_close(expiration_date).isoformat(),
            underlying_type=underlying_type,
            refresh=refresh_calendar_events,
        )
        decisions_by_expiration[expiration_date] = apply_strategy_calendar_policy(
            context,
            strategy=strategy,
            underlying_type=underlying_type,
            mode=calendar_policy,
        )

    filtered_candidates: list[SpreadCandidate] = []
    for candidate in candidates:
        decision = decisions_by_expiration[candidate.expiration_date]
        if calendar_policy == "strict" and decision.status == "blocked":
            continue
        filtered_candidates.append(
            replace(
                candidate,
                calendar_status=decision.status,
                calendar_reasons=build_calendar_reason_messages(decision),
                calendar_confidence=decision.source_confidence,
                calendar_sources=decision.sources,
                calendar_last_updated=decision.last_updated,
                calendar_days_to_nearest_event=decision.days_to_nearest_event,
                macro_regime=decision.macro_regime,
                earnings_phase=decision.earnings_phase,
                earnings_event_date=decision.earnings_event_date,
                earnings_session_timing=decision.earnings_session_timing,
                earnings_cohort_key=decision.earnings_cohort_key,
                earnings_days_to_event=decision.earnings_days_to_event,
                earnings_days_since_event=decision.earnings_days_since_event,
                earnings_timing_confidence=decision.earnings_timing_confidence,
                earnings_horizon_crosses_report=decision.earnings_horizon_crosses_report,
                earnings_primary_source=decision.earnings_primary_source,
                earnings_supporting_sources=decision.earnings_supporting_sources,
                earnings_consensus_status=decision.earnings_consensus_status,
            )
        )
    return filtered_candidates


def latest_bar_on_or_before(bars: list[DailyBar], target_date: date) -> DailyBar | None:
    eligible = [
        bar
        for bar in bars
        if datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date()
        <= target_date
    ]
    if not eligible:
        return None
    return eligible[-1]


def bars_through_date(bars: list[DailyBar], target_date: date) -> list[DailyBar]:
    return [
        bar
        for bar in bars
        if datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date()
        <= target_date
    ]


def latest_option_bar_on_or_before(
    bars_by_symbol: dict[str, list[DailyBar]],
    symbol: str,
    target_date: date,
) -> DailyBar | None:
    return latest_bar_on_or_before(bars_by_symbol.get(symbol, []), target_date)


def estimate_spread_bar(
    short_bar: DailyBar,
    long_bar: DailyBar,
    *,
    strategy: str,
) -> dict[str, float]:
    if net_premium_kind(strategy) == "debit":
        return {
            "open": max(long_bar.open - short_bar.open, 0.0),
            "high": max(long_bar.high - short_bar.low, 0.0),
            "low": max(long_bar.low - short_bar.high, 0.0),
            "close": max(long_bar.close - short_bar.close, 0.0),
        }
    return {
        "open": max(short_bar.open - long_bar.open, 0.0),
        "high": max(short_bar.high - long_bar.low, 0.0),
        "low": max(short_bar.low - long_bar.high, 0.0),
        "close": max(short_bar.close - long_bar.close, 0.0),
    }


def estimate_structure_bar(
    *,
    legs: list[dict[str, Any]],
    bars_by_symbol: dict[str, DailyBar],
    strategy: str,
) -> dict[str, float] | None:
    premium_kind = net_premium_kind(strategy)
    if premium_kind is None or not legs:
        return None

    open_short = 0.0
    high_short = 0.0
    low_short = 0.0
    close_short = 0.0
    open_long = 0.0
    high_long = 0.0
    low_long = 0.0
    close_long = 0.0

    for leg in legs:
        symbol = str(leg.get("symbol") or "").strip()
        role = str(leg.get("role") or "").strip().lower()
        bar = bars_by_symbol.get(symbol)
        if not symbol or role not in {"short", "long"} or bar is None:
            return None
        if role == "short":
            open_short += bar.open
            high_short += bar.high
            low_short += bar.low
            close_short += bar.close
        else:
            open_long += bar.open
            high_long += bar.high
            low_long += bar.low
            close_long += bar.close

    if premium_kind == "debit":
        return {
            "open": max(open_long - open_short, 0.0),
            "high": max(high_long - low_short, 0.0),
            "low": max(low_long - high_short, 0.0),
            "close": max(close_long - close_short, 0.0),
        }
    return {
        "open": max(open_short - open_long, 0.0),
        "high": max(high_short - low_long, 0.0),
        "low": max(low_short - high_long, 0.0),
        "close": max(close_short - close_long, 0.0),
    }


def option_bar_available_for_target(
    bars_by_symbol: dict[str, list[DailyBar]],
    short_symbol: str,
    long_symbol: str,
    target_date: date,
) -> bool:
    short_bar = latest_option_bar_on_or_before(
        bars_by_symbol, short_symbol, target_date
    )
    long_bar = latest_option_bar_on_or_before(bars_by_symbol, long_symbol, target_date)
    if short_bar is None or long_bar is None:
        return False
    short_date = datetime.fromisoformat(
        short_bar.timestamp.replace("Z", "+00:00")
    ).date()
    long_date = datetime.fromisoformat(long_bar.timestamp.replace("Z", "+00:00")).date()
    return short_date == target_date and long_date == target_date


def option_bars_by_date(bars: list[DailyBar]) -> dict[date, DailyBar]:
    return {
        datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date(): bar
        for bar in bars
    }


def simulate_exit_until_date(
    candidate: dict[str, Any],
    *,
    option_bars: dict[str, list[DailyBar]],
    start_date: date,
    target_date: date,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    legs = candidate_legs(candidate)
    leg_bars = {
        str(leg.get("symbol") or ""): option_bars_by_date(
            option_bars.get(str(leg.get("symbol") or ""), [])
        )
        for leg in legs
        if str(leg.get("symbol") or "").strip()
    }
    entry_credit = candidate["midpoint_credit"]
    premium_kind = net_premium_kind(candidate.get("strategy"))
    if premium_kind == "debit":
        target_mark = entry_credit * (1.0 + profit_target)
        stop_mark = max(entry_credit / max(stop_multiple, 1.0), 0.0)
    else:
        target_mark = max(entry_credit * (1.0 - profit_target), 0.0)
        stop_mark = entry_credit * stop_multiple

    path_date_sets = [set(bars_by_date) for bars_by_date in leg_bars.values()]
    if not path_date_sets:
        return {"status": "pending_option_bars"}
    path_dates = sorted(
        d for d in set.intersection(*path_date_sets) if start_date <= d <= target_date
    )
    if not path_dates:
        return {"status": "pending_option_bars"}

    last_mark = None
    for path_date in path_dates:
        spread_bar = estimate_structure_bar(
            legs=legs,
            bars_by_symbol={
                symbol: bars_by_date[path_date]
                for symbol, bars_by_date in leg_bars.items()
                if path_date in bars_by_date
            },
            strategy=str(candidate.get("strategy") or ""),
        )
        if spread_bar is None:
            return {"status": "pending_option_bars"}
        last_mark = spread_bar["close"]
        if premium_kind == "debit":
            hit_target = spread_bar["high"] >= target_mark
            hit_stop = spread_bar["low"] <= stop_mark
        else:
            hit_target = spread_bar["low"] <= target_mark
            hit_stop = spread_bar["high"] >= stop_mark

        if hit_target and hit_stop:
            return {
                "status": "conflict",
                "exit_date": path_date.isoformat(),
                "exit_reason": "conflict_stop_first",
                "exit_mark": stop_mark,
                "estimated_pnl": (
                    (stop_mark - entry_credit) * 100.0
                    if premium_kind == "debit"
                    else (entry_credit - stop_mark) * 100.0
                ),
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": True,
                "stop_hit": True,
            }
        if hit_target:
            return {
                "status": "exited",
                "exit_date": path_date.isoformat(),
                "exit_reason": "profit_target",
                "exit_mark": target_mark,
                "estimated_pnl": (
                    (target_mark - entry_credit) * 100.0
                    if premium_kind == "debit"
                    else (entry_credit - target_mark) * 100.0
                ),
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": True,
                "stop_hit": False,
            }
        if hit_stop:
            return {
                "status": "exited",
                "exit_date": path_date.isoformat(),
                "exit_reason": "stop",
                "exit_mark": stop_mark,
                "estimated_pnl": (
                    (stop_mark - entry_credit) * 100.0
                    if premium_kind == "debit"
                    else (entry_credit - stop_mark) * 100.0
                ),
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": False,
                "stop_hit": True,
            }

    return {
        "status": "open",
        "exit_date": path_dates[-1].isoformat(),
        "exit_reason": "mark",
        "exit_mark": last_mark,
        "estimated_pnl": (
            ((last_mark - entry_credit) * 100.0)
            if premium_kind == "debit"
            else ((entry_credit - last_mark) * 100.0)
        )
        if last_mark is not None
        else None,
        "spread_mark_close": last_mark,
        "spread_mark_low": None,
        "spread_mark_high": None,
        "profit_target_hit": False,
        "stop_hit": False,
    }


def mark_spread_on_date(
    candidate: dict[str, Any],
    *,
    option_bars: dict[str, list[DailyBar]],
    target_date: date,
) -> dict[str, Any]:
    legs = candidate_legs(candidate)
    bars_by_symbol: dict[str, DailyBar] = {}
    for leg in legs:
        symbol = str(leg.get("symbol") or "").strip()
        if not symbol:
            continue
        bar = latest_option_bar_on_or_before(option_bars, symbol, target_date)
        if bar is None:
            return {"status": "pending_option_bars"}
        bars_by_symbol[symbol] = bar
    if not bars_by_symbol:
        return {"status": "pending_option_bars"}

    spread_bar = estimate_structure_bar(
        legs=legs,
        bars_by_symbol=bars_by_symbol,
        strategy=str(candidate.get("strategy") or ""),
    )
    if spread_bar is None:
        return {"status": "pending_option_bars"}
    entry_credit = candidate["midpoint_credit"]
    close_mark = spread_bar["close"]
    premium_kind = net_premium_kind(candidate.get("strategy"))
    return {
        "status": "mark_only",
        "exit_date": target_date.isoformat(),
        "exit_reason": "entry_mark",
        "exit_mark": close_mark,
        "estimated_pnl": (
            (close_mark - entry_credit) * 100.0
            if premium_kind == "debit"
            else (entry_credit - close_mark) * 100.0
        ),
        "spread_mark_close": close_mark,
        "spread_mark_low": None,
        "spread_mark_high": None,
        "profit_target_hit": False,
        "stop_hit": False,
    }


def summarize_replay(
    *,
    run_payload: dict[str, Any],
    candidates: list[dict[str, Any]],
    bars: list[DailyBar],
    option_bars: dict[str, list[DailyBar]],
    profit_target: float,
    stop_multiple: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    generated_at = datetime.fromisoformat(
        run_payload["generated_at"].replace("Z", "+00:00")
    )
    run_date = generated_at.astimezone(NEW_YORK).date()
    strategy = run_payload.get("strategy") or run_payload["filters"].get(
        "strategy", "call_credit"
    )
    option_type = strategy_option_type(strategy)
    latest_available_date = (
        None
        if not bars
        else max(
            datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date()
            for bar in bars
        )
    )
    horizons = [
        ("entry", run_date),
        ("1d", run_date + timedelta(days=1)),
        ("3d", run_date + timedelta(days=3)),
    ]

    summaries: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for label, target_date in horizons:
        horizon_bars = bars_through_date(bars, target_date)
        available_candidates = 0
        touched = 0
        closed_past_short = 0
        closed_past_breakeven = 0
        profit_target_hits = 0
        stop_hits = 0
        conflicts = 0
        total_pnl = 0.0

        for candidate in candidates:
            if latest_available_date is None or latest_available_date < target_date:
                rows.append(
                    {
                        "horizon": label,
                        "short_symbol": candidate["short_symbol"],
                        "long_symbol": candidate["long_symbol"],
                        "expiration_date": candidate["expiration_date"],
                        "status": "pending",
                    }
                )
                continue

            horizon_bar = latest_bar_on_or_before(bars, target_date)
            if horizon_bar is None:
                continue
            if label == "entry":
                replay_path = mark_spread_on_date(
                    candidate,
                    option_bars=option_bars,
                    target_date=target_date,
                )
            else:
                replay_path = simulate_exit_until_date(
                    candidate,
                    option_bars=option_bars,
                    start_date=run_date,
                    target_date=target_date,
                    profit_target=profit_target,
                    stop_multiple=stop_multiple,
                )
            if replay_path["status"] == "pending_option_bars":
                rows.append(
                    {
                        "horizon": label,
                        "short_symbol": candidate["short_symbol"],
                        "long_symbol": candidate["long_symbol"],
                        "expiration_date": candidate["expiration_date"],
                        "status": "pending_option_bars",
                    }
                )
                continue

            available_candidates += 1
            path_bars = horizon_bars
            path_high = (
                max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
            )
            path_low = (
                min(bar.low for bar in path_bars) if path_bars else horizon_bar.low
            )
            if option_type == "put":
                touched_short = path_low <= candidate["short_strike"]
                closed_beyond_short = horizon_bar.close <= candidate["short_strike"]
                closed_beyond_breakeven = horizon_bar.close <= candidate["breakeven"]
            else:
                touched_short = path_high >= candidate["short_strike"]
                closed_beyond_short = horizon_bar.close >= candidate["short_strike"]
                closed_beyond_breakeven = horizon_bar.close >= candidate["breakeven"]
            touched += int(touched_short)
            closed_past_short += int(closed_beyond_short)
            closed_past_breakeven += int(closed_beyond_breakeven)
            profit_target_hits += int(replay_path["profit_target_hit"])
            stop_hits += int(replay_path["stop_hit"])
            conflicts += int(replay_path["status"] == "conflict")
            total_pnl += replay_path["estimated_pnl"] or 0.0
            rows.append(
                {
                    "horizon": label,
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "available",
                    "spot_at_horizon": horizon_bar.close,
                    "path_extreme_to_horizon": path_low
                    if option_type == "put"
                    else path_high,
                    "touched_short_strike": touched_short,
                    "closed_past_short_strike": closed_beyond_short,
                    "closed_past_breakeven": closed_beyond_breakeven,
                    "spread_mark_close": replay_path["spread_mark_close"],
                    "spread_mark_low": replay_path["spread_mark_low"],
                    "spread_mark_high": replay_path["spread_mark_high"],
                    "estimated_pnl": replay_path["estimated_pnl"],
                    "estimated_profit_target_hit": replay_path["profit_target_hit"],
                    "estimated_stop_hit": replay_path["stop_hit"],
                    "exit_reason": replay_path["exit_reason"],
                    "exit_date": replay_path["exit_date"],
                    "replay_status": replay_path["status"],
                }
            )

        total = len(candidates)
        summaries.append(
            {
                "horizon": label,
                "available": available_candidates,
                "pending": total - available_candidates,
                "touch_pct": None
                if available_candidates == 0
                else 100.0 * touched / available_candidates,
                "close_past_short_pct": None
                if available_candidates == 0
                else 100.0 * closed_past_short / available_candidates,
                "close_past_breakeven_pct": None
                if available_candidates == 0
                else 100.0 * closed_past_breakeven / available_candidates,
                "profit_target_hit_pct": None
                if available_candidates == 0
                else 100.0 * profit_target_hits / available_candidates,
                "stop_hit_pct": None
                if available_candidates == 0
                else 100.0 * stop_hits / available_candidates,
                "conflict_pct": None
                if available_candidates == 0
                else 100.0 * conflicts / available_candidates,
                "avg_pnl": None
                if available_candidates == 0
                else total_pnl / available_candidates,
            }
        )

    expiry_available = 0
    expiry_touched = 0
    expiry_closed_past_short = 0
    expiry_closed_past_breakeven = 0
    expiry_profit_targets = 0
    expiry_stop_hits = 0
    expiry_conflicts = 0
    expiry_total_pnl = 0.0
    for candidate in candidates:
        expiry_date = date.fromisoformat(candidate["expiration_date"])
        if latest_available_date is None or latest_available_date < expiry_date:
            rows.append(
                {
                    "horizon": "expiry",
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "pending",
                }
            )
            continue

        horizon_bar = latest_bar_on_or_before(bars, expiry_date)
        if horizon_bar is None:
            continue
        replay_path = simulate_exit_until_date(
            candidate,
            option_bars=option_bars,
            start_date=run_date,
            target_date=expiry_date,
            profit_target=profit_target,
            stop_multiple=stop_multiple,
        )
        if replay_path["status"] == "pending_option_bars":
            rows.append(
                {
                    "horizon": "expiry",
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "pending_option_bars",
                }
            )
            continue
        expiry_available += 1
        path_bars = bars_through_date(bars, expiry_date)
        path_high = (
            max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
        )
        path_low = min(bar.low for bar in path_bars) if path_bars else horizon_bar.low
        if option_type == "put":
            touched_short = path_low <= candidate["short_strike"]
            closed_beyond_short = horizon_bar.close <= candidate["short_strike"]
            closed_beyond_breakeven = horizon_bar.close <= candidate["breakeven"]
        else:
            touched_short = path_high >= candidate["short_strike"]
            closed_beyond_short = horizon_bar.close >= candidate["short_strike"]
            closed_beyond_breakeven = horizon_bar.close >= candidate["breakeven"]
        expiry_touched += int(touched_short)
        expiry_closed_past_short += int(closed_beyond_short)
        expiry_closed_past_breakeven += int(closed_beyond_breakeven)
        expiry_profit_targets += int(replay_path["profit_target_hit"])
        expiry_stop_hits += int(replay_path["stop_hit"])
        expiry_conflicts += int(replay_path["status"] == "conflict")
        expiry_total_pnl += replay_path["estimated_pnl"] or 0.0
        rows.append(
            {
                "horizon": "expiry",
                "short_symbol": candidate["short_symbol"],
                "long_symbol": candidate["long_symbol"],
                "expiration_date": candidate["expiration_date"],
                "status": "available",
                "spot_at_horizon": horizon_bar.close,
                "path_extreme_to_horizon": path_low
                if option_type == "put"
                else path_high,
                "touched_short_strike": touched_short,
                "closed_past_short_strike": closed_beyond_short,
                "closed_past_breakeven": closed_beyond_breakeven,
                "spread_mark_close": replay_path["spread_mark_close"],
                "spread_mark_low": replay_path["spread_mark_low"],
                "spread_mark_high": replay_path["spread_mark_high"],
                "estimated_pnl": replay_path["estimated_pnl"],
                "estimated_profit_target_hit": replay_path["profit_target_hit"],
                "estimated_stop_hit": replay_path["stop_hit"],
                "exit_reason": replay_path["exit_reason"],
                "exit_date": replay_path["exit_date"],
                "replay_status": replay_path["status"],
            }
        )

    total = len(candidates)
    summaries.append(
        {
            "horizon": "expiry",
            "available": expiry_available,
            "pending": total - expiry_available,
            "touch_pct": None
            if expiry_available == 0
            else 100.0 * expiry_touched / expiry_available,
            "close_past_short_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_past_short / expiry_available,
            "close_past_breakeven_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_past_breakeven / expiry_available,
            "profit_target_hit_pct": None
            if expiry_available == 0
            else 100.0 * expiry_profit_targets / expiry_available,
            "stop_hit_pct": None
            if expiry_available == 0
            else 100.0 * expiry_stop_hits / expiry_available,
            "conflict_pct": None
            if expiry_available == 0
            else 100.0 * expiry_conflicts / expiry_available,
            "avg_pnl": None
            if expiry_available == 0
            else expiry_total_pnl / expiry_available,
        }
    )

    return summaries, rows


def print_replay_summary(
    run_payload: dict[str, Any],
    summaries: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    strategy = run_payload.get("strategy") or run_payload["filters"].get(
        "strategy", "call_credit"
    )
    print(
        f"Replay run: {run_payload['run_id']} | {run_payload['symbol']} | "
        f"strategy {strategy} | profile {run_payload['profile']} | generated {run_payload['generated_at']}"
    )
    print(f"Stored candidates: {run_payload['candidate_count']}")
    print()

    table_headers = [
        "Horizon",
        "Avail",
        "Pending",
        "Touch%",
        "PastShort%",
        "PastBE%",
        "PT%",
        "Stop%",
        "Conf%",
        "AvgPnL$",
    ]
    table_rows = []
    for summary in summaries:
        table_rows.append(
            [
                summary["horizon"],
                str(summary["available"]),
                str(summary["pending"]),
                "n/a"
                if summary["touch_pct"] is None
                else f"{summary['touch_pct']:.1f}",
                "n/a"
                if summary["close_past_short_pct"] is None
                else f"{summary['close_past_short_pct']:.1f}",
                "n/a"
                if summary["close_past_breakeven_pct"] is None
                else f"{summary['close_past_breakeven_pct']:.1f}",
                "n/a"
                if summary["profit_target_hit_pct"] is None
                else f"{summary['profit_target_hit_pct']:.1f}",
                "n/a"
                if summary["stop_hit_pct"] is None
                else f"{summary['stop_hit_pct']:.1f}",
                "n/a"
                if summary["conflict_pct"] is None
                else f"{summary['conflict_pct']:.1f}",
                "n/a" if summary["avg_pnl"] is None else f"{summary['avg_pnl']:.0f}",
            ]
        )
    print(format_table(table_headers, table_rows))
    print()

    available_rows = [row for row in rows if row["status"] == "available"][:10]
    if available_rows:
        detail_headers = [
            "Horizon",
            "Short",
            "Long",
            "Expiry",
            "Spot",
            "Sprd",
            "PnL$",
            "Touch",
            "PastShort",
            "PastBE",
            "Exit",
            "PT",
            "Stop",
        ]
        detail_rows = [
            [
                row["horizon"],
                row["short_symbol"],
                row["long_symbol"],
                row["expiration_date"],
                f"{row['spot_at_horizon']:.2f}",
                f"{row['spread_mark_close']:.2f}",
                f"{row['estimated_pnl']:.0f}",
                "yes" if row["touched_short_strike"] else "no",
                "yes" if row["closed_past_short_strike"] else "no",
                "yes" if row["closed_past_breakeven"] else "no",
                row["exit_reason"],
                "yes" if row["estimated_profit_target_hit"] else "no",
                "yes" if row["estimated_stop_hit"] else "no",
            ]
            for row in available_rows
        ]
        print(format_table(detail_headers, detail_rows))
    else:
        print("Replay data is not available yet for the stored horizons.")


def run_replay(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
) -> int:
    if args.replay_latest and args.strategy == "combined":
        raise SystemExit(
            "Replay latest requires a concrete strategy such as call_credit, put_credit, call_debit, put_debit, long_straddle, long_strangle, or iron_condor"
        )
    if args.replay_run_id:
        run_payload = history_store.get_run(args.replay_run_id)
    else:
        if not args.symbol:
            raise SystemExit("Replay latest requires --symbol or use --replay-run-id")
        run_payload = history_store.get_latest_run(
            args.symbol.upper(), strategy=args.strategy
        )

    if not run_payload:
        target = args.replay_run_id or args.symbol.upper()
        raise SystemExit(f"No stored run found for replay target: {target}")

    candidates = history_store.list_candidates(run_payload["run_id"])
    generated_at = datetime.fromisoformat(
        run_payload["generated_at"].replace("Z", "+00:00")
    )
    run_date = generated_at.astimezone(NEW_YORK).date()
    replay_end = max(
        [
            run_date + timedelta(days=3),
            *[
                date.fromisoformat(candidate["expiration_date"])
                for candidate in candidates
            ],
        ]
    )
    bars = client.get_daily_bars(
        run_payload["symbol"],
        start=(run_date - timedelta(days=2)).isoformat(),
        end=replay_end.isoformat(),
        stock_feed=args.stock_feed,
    )
    option_symbols = sorted(
        {
            *[candidate["short_symbol"] for candidate in candidates],
            *[candidate["long_symbol"] for candidate in candidates],
        }
    )
    option_bars = client.get_option_bars(
        option_symbols,
        start=run_date.isoformat(),
        end=replay_end.isoformat(),
    )
    summaries, rows = summarize_replay(
        run_payload=run_payload,
        candidates=candidates,
        bars=bars,
        option_bars=option_bars,
        profit_target=args.replay_profit_target,
        stop_multiple=args.replay_stop_multiple,
    )
    print_replay_summary(run_payload, summaries, rows)
    return 0

def main(argv: list[str] | None = None) -> int:
    load_local_env()
    args = parse_args(argv)
    from core.services.scanners.runtime import (
        merge_strategy_candidates,
        scan_symbol_across_strategies,
    )

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    symbols, universe_label = resolve_symbols(args)

    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, args.trading_base_url),
        data_base_url=args.data_base_url,
    )
    history_store = build_history_store(args.history_db)
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=args.data_base_url,
        database_url=args.history_db,
    )
    greeks_provider = build_local_greeks_provider()

    if args.replay_latest or args.replay_run_id:
        try:
            return run_replay(args=args, client=client, history_store=history_store)
        finally:
            history_store.close()
            calendar_resolver.store.close()

    if len(symbols) == 1:
        strategy_results, failures = scan_symbol_across_strategies(
            symbol=symbols[0],
            base_args=args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
        if failures and not strategy_results:
            raise SystemExit(failures[0].error)

        if args.strategy == "combined":
            combined_candidates = merge_strategy_candidates(strategy_results)
            primary_result = strategy_results[0]
            output_path = args.output or default_output_path(
                primary_result.symbol, args.strategy, args.output_format
            )
            if args.output_format == "csv":
                write_csv(output_path, combined_candidates)
            else:
                write_json(
                    output_path,
                    primary_result.symbol,
                    primary_result.spot_price,
                    args,
                    combined_candidates,
                )
            latest_copy = write_latest_copy(
                output_path,
                f"latest_{primary_result.symbol.lower()}_{args.strategy}.{args.output_format}",
            )
            candidates = combined_candidates[: args.top]
            setup_summaries = build_setup_summaries(strategy_results)
            if args.json:
                print(
                    json.dumps(
                        {
                            "symbol": primary_result.symbol,
                            "strategy": args.strategy,
                            "spot_price": primary_result.spot_price,
                            "generated_at": datetime.now(UTC)
                            .isoformat(timespec="seconds")
                            .replace("+00:00", "Z"),
                            "filters": build_filter_payload(args),
                            "strategy_runs": [
                                {
                                    "strategy": result.args.strategy,
                                    "run_id": result.run_id,
                                    "setup": None
                                    if result.setup is None
                                    else {
                                        "status": result.setup.status,
                                        "score": result.setup.score,
                                        "reasons": list(result.setup.reasons),
                                    },
                                }
                                for result in strategy_results
                            ],
                            "failures": [asdict(failure) for failure in failures],
                            "candidates": [
                                asdict(candidate) for candidate in candidates
                            ],
                            "output_file": output_path,
                        },
                        indent=2,
                    )
                )
            else:
                print_human_readable(
                    primary_result.symbol,
                    primary_result.spot_price,
                    candidates,
                    args.show_order_json,
                    None,
                    strategy=args.strategy,
                    profile=args.profile,
                    greeks_source=args.greeks_source,
                    setup_summaries=setup_summaries,
                )
                for strategy_result in strategy_results:
                    if strategy_result.args.profile == "0dte":
                        print(
                            f"0DTE coverage [{strategy_result.args.strategy}]: Alpaca returned quotes for "
                            f"{strategy_result.quoted_contract_count} contracts, Alpaca delta for "
                            f"{strategy_result.alpaca_delta_contract_count}, final usable delta for "
                            f"{strategy_result.delta_contract_count}, local Greeks for "
                            f"{strategy_result.local_delta_contract_count}."
                        )
                maybe_stream_live_quotes(
                    args=args, client=client, candidates=candidates
                )
                if failures:
                    print("Failures:")
                    for failure in failures:
                        print(f"- {failure.symbol}: {failure.error}")
                print(f"Saved {len(combined_candidates)} candidates to {output_path}")
                print(f"Latest copy: {latest_copy}")
                print("Run ids:")
                for result in strategy_results:
                    print(f"- {result.args.strategy}: {result.run_id}")
        else:
            result = strategy_results[0]
            output_path = args.output or default_output_path(
                result.symbol, result.args.strategy, args.output_format
            )

            if args.output_format == "csv":
                write_csv(output_path, result.candidates)
            else:
                write_json(
                    output_path,
                    result.symbol,
                    result.spot_price,
                    result.args,
                    result.candidates,
                    run_id=result.run_id,
                    setup=result.setup,
                )
            latest_copy = write_latest_copy(
                output_path,
                f"latest_{result.symbol.lower()}_{result.args.strategy}.{args.output_format}",
            )

            candidates = result.candidates[: result.args.top]
            if args.json:
                print(
                    json.dumps(
                        {
                            "symbol": result.symbol,
                            "strategy": result.args.strategy,
                            "spot_price": result.spot_price,
                            "generated_at": datetime.now(UTC)
                            .isoformat(timespec="seconds")
                            .replace("+00:00", "Z"),
                            "run_id": result.run_id,
                            "filters": build_filter_payload(result.args),
                            "setup": None
                            if result.setup is None
                            else {
                                "status": result.setup.status,
                                "score": result.setup.score,
                                "reasons": list(result.setup.reasons),
                            },
                            "candidates": [
                                asdict(candidate) for candidate in candidates
                            ],
                            "output_file": output_path,
                        },
                        indent=2,
                    )
                )
            else:
                print_human_readable(
                    result.symbol,
                    result.spot_price,
                    candidates,
                    result.args.show_order_json,
                    result.setup,
                    strategy=result.args.strategy,
                    profile=result.args.profile,
                    greeks_source=result.args.greeks_source,
                )
                if result.args.profile == "0dte":
                    print(
                        f"0DTE coverage: Alpaca returned quotes for {result.quoted_contract_count} "
                        f"contracts, Alpaca delta for {result.alpaca_delta_contract_count}, final usable delta for "
                        f"{result.delta_contract_count}, local Greeks for "
                        f"{result.local_delta_contract_count}."
                    )
                maybe_stream_live_quotes(
                    args=result.args, client=client, candidates=candidates
                )
                print(f"Saved {len(result.candidates)} candidates to {output_path}")
                print(f"Latest copy: {latest_copy}")
                print(f"Run id: {result.run_id}")
    else:
        scan_results: list[SymbolScanResult] = []
        failures: list[UniverseScanFailure] = []
        ranked_candidates: list[SpreadCandidate] = []

        for symbol in symbols:
            strategy_results, symbol_failures = scan_symbol_across_strategies(
                symbol=symbol,
                base_args=args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                history_store=history_store,
            )
            failures.extend(symbol_failures)
            if not strategy_results:
                continue
            scan_results.extend(strategy_results)
            symbol_ranked_candidates = merge_strategy_candidates(
                strategy_results,
                per_strategy_top=args.per_symbol_top,
            )[: args.per_symbol_top]
            ranked_candidates.extend(symbol_ranked_candidates)

        ranked_candidates = sort_candidates_for_display(ranked_candidates)
        ranked_candidates = ranked_candidates[: args.top]
        output_path = args.output or default_universe_output_path(
            universe_label, args.strategy, args.output_format
        )

        if args.output_format == "csv":
            write_universe_csv(output_path, ranked_candidates)
        else:
            write_universe_json(
                output_path,
                label=universe_label,
                strategy=args.strategy,
                symbols=symbols,
                candidates=ranked_candidates,
                failures=failures,
            )
        latest_copy = write_latest_copy(
            output_path,
            f"latest_{universe_label.lower().replace(' ', '_')}_{args.strategy}.{args.output_format}",
        )

        if args.json:
            print(
                json.dumps(
                    {
                        "mode": "universe",
                        "label": universe_label,
                        "strategy": args.strategy,
                        "symbols": symbols,
                        "candidate_count": len(ranked_candidates),
                        "failures": [asdict(failure) for failure in failures],
                        "candidates": [
                            asdict(candidate) for candidate in ranked_candidates
                        ],
                        "output_file": output_path,
                    },
                    indent=2,
                )
            )
        else:
            print_ranked_candidates(
                label=universe_label,
                strategy=args.strategy,
                profile=args.profile,
                greeks_source=args.greeks_source,
                symbols=symbols,
                ranked_candidates=ranked_candidates,
                failures=failures,
            )
            maybe_stream_live_quotes(
                args=args, client=client, candidates=ranked_candidates
            )
            if scan_results:
                print(f"Stored per-symbol runs: {len(scan_results)}")
            print(f"Saved {len(ranked_candidates)} ranked candidates to {output_path}")
            print(f"Latest copy: {latest_copy}")

    history_store.close()
    calendar_resolver.store.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)

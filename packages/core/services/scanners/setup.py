from __future__ import annotations

from dataclasses import asdict, replace

from core.common import clamp
from core.domain.models import (
    DailyBar,
    IntradayBar,
    SpreadCandidate,
    UnderlyingSetupContext,
)
from core.services.scanners.config import strategy_direction


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


def serialize_setup_context(
    setup: UnderlyingSetupContext | None,
) -> dict[str, object] | None:
    if setup is None:
        return None
    payload = asdict(setup)
    payload["reasons"] = list(setup.reasons)
    return payload


__all__ = [
    "analyze_underlying_setup",
    "attach_underlying_setup",
    "serialize_setup_context",
]

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from math import sqrt
from statistics import pstdev
from typing import Any

from core.domain.models import DailyBar, IntradayBar, SymbolMarketSlice
from core.value_coercion import as_mapping, coerce_float, coerce_utc_datetime

from .market_context import (
    MarketBenchmarkEvidence,
    MarketBenchmarkRole,
    MarketContextDataQuality,
    MarketContextDataQualityState,
    MarketContextFidelityLabel,
    MarketContextFreshnessState,
    MarketContextSnapshot,
    MarketRegimeLabel,
    MarketRiskPosture,
    MarketTrendStrength,
    MarketVolatilityState,
    RegimeSnapshot,
)

DEFAULT_MARKET_CONTEXT_BENCHMARK_SYMBOLS = ("SPY", "QQQ")
DEFAULT_MARKET_CONTEXT_TTL_SECONDS = 300
MARKET_CONTEXT_VERSION = 1


@dataclass(frozen=True)
class MarketContextRequest:
    benchmark_symbols: tuple[str, ...] = DEFAULT_MARKET_CONTEXT_BENCHMARK_SYMBOLS
    observed_at: datetime | None = None
    expires_at: datetime | None = None
    scope: str = "global_market"
    config_hash: str | None = None
    source: str = MarketContextFidelityLabel.LIVE_MARKET_SLICE.value
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        symbols = tuple(dict.fromkeys(str(symbol or "").upper().strip() for symbol in self.benchmark_symbols if str(symbol or "").strip()))
        if not symbols:
            raise ValueError("MarketContextRequest.benchmark_symbols is required")
        object.__setattr__(self, "benchmark_symbols", symbols)


@dataclass(frozen=True)
class MarketContextBenchmarkFact:
    symbol: str
    return_5d_pct: float | None = None
    intraday_return_pct: float | None = None
    spot_vs_sma20_pct: float | None = None
    daily_bar_count: int | None = None
    intraday_bar_count: int | None = None
    observed_at: datetime | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").upper().strip()
        if not symbol:
            raise ValueError("MarketContextBenchmarkFact.symbol is required")
        object.__setattr__(self, "symbol", symbol)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 6)


def _daily_return_pct(bars: Sequence[DailyBar], *, lookback_sessions: int = 5) -> float | None:
    rows = list(bars or ())
    if len(rows) <= lookback_sessions:
        return None
    base = rows[-(lookback_sessions + 1)].close
    latest = rows[-1].close
    if base <= 0:
        return None
    return latest / base - 1.0


def _intraday_return_pct(*, spot_price: float, bars: Sequence[IntradayBar]) -> float | None:
    rows = list(bars or ())
    if not rows or rows[0].open <= 0:
        return None
    return spot_price / rows[0].open - 1.0


def _spot_vs_sma20_pct(*, spot_price: float, bars: Sequence[DailyBar]) -> float | None:
    rows = list(bars or ())
    if len(rows) < 20:
        return None
    average = sum(bar.close for bar in rows[-20:]) / 20.0
    if average <= 0:
        return None
    return spot_price / average - 1.0


def _drawdown_5d_pct(bars: Sequence[DailyBar]) -> float | None:
    rows = list(bars or ())
    if len(rows) < 2:
        return None
    window = rows[-6:]
    peak = max(bar.close for bar in window)
    latest = window[-1].close
    if peak <= 0:
        return None
    return latest / peak - 1.0


def _realized_volatility_5d_pct(bars: Sequence[DailyBar]) -> float | None:
    rows = list(bars or ())
    if len(rows) < 6:
        return None
    closes = [bar.close for bar in rows[-6:]]
    returns = [closes[index] / closes[index - 1] - 1.0 for index in range(1, len(closes)) if closes[index - 1] > 0]
    if len(returns) < 2:
        return None
    return pstdev(returns) * sqrt(252.0)


def _trend_score(*, return_5d_pct: float | None, intraday_return_pct: float | None, spot_vs_sma20_pct: float | None) -> float | None:
    values = [value for value in (return_5d_pct, intraday_return_pct, spot_vs_sma20_pct) if value is not None]
    if not values:
        return None
    weighted = (return_5d_pct or 0.0) * 10.0 + (intraday_return_pct or 0.0) * 15.0 + (spot_vs_sma20_pct or 0.0) * 5.0
    return max(min(weighted, 1.0), -1.0)


def _fidelity_label(source: str) -> MarketContextFidelityLabel:
    normalized = str(source or "").strip().lower()
    if normalized == MarketContextFidelityLabel.HISTORICAL_MARKET_SLICE.value:
        return MarketContextFidelityLabel.HISTORICAL_MARKET_SLICE
    if normalized == MarketContextFidelityLabel.STORED_MARKET_CONTEXT.value:
        return MarketContextFidelityLabel.STORED_MARKET_CONTEXT
    if normalized == MarketContextFidelityLabel.STORED_CANDIDATE_CONTEXT.value:
        return MarketContextFidelityLabel.STORED_CANDIDATE_CONTEXT
    return MarketContextFidelityLabel.LIVE_MARKET_SLICE


def _merge_fidelity(*groups: Sequence[MarketContextFidelityLabel]) -> tuple[MarketContextFidelityLabel, ...]:
    labels: list[MarketContextFidelityLabel] = []
    seen: set[MarketContextFidelityLabel] = set()
    for group in groups:
        for label in group:
            if label in seen:
                continue
            seen.add(label)
            labels.append(label)
    return tuple(labels)


def _volatility_state(realized_volatility_values: Sequence[float]) -> MarketVolatilityState:
    values = [value for value in realized_volatility_values if value is not None]
    if not values:
        return MarketVolatilityState.UNKNOWN
    average = sum(values) / len(values)
    if average >= 0.40:
        return MarketVolatilityState.STRESSED
    if average >= 0.25:
        return MarketVolatilityState.ELEVATED
    if average <= 0.12:
        return MarketVolatilityState.COMPRESSED
    return MarketVolatilityState.NORMAL


def _trend_strength(average_return_5d: float | None, average_intraday_return: float | None) -> MarketTrendStrength:
    if average_return_5d is None and average_intraday_return is None:
        return MarketTrendStrength.UNKNOWN
    trend = (average_return_5d or 0.0) + (average_intraday_return or 0.0)
    if trend >= 0.03:
        return MarketTrendStrength.STRONG_UP
    if trend >= 0.005:
        return MarketTrendStrength.MODEST_UP
    if trend <= -0.03:
        return MarketTrendStrength.STRONG_DOWN
    if trend <= -0.005:
        return MarketTrendStrength.MODEST_DOWN
    return MarketTrendStrength.FLAT


def _regime_label(
    *,
    trend_strength: MarketTrendStrength,
    volatility_state: MarketVolatilityState,
    blocking_count: int,
    expected_count: int,
) -> MarketRegimeLabel:
    if blocking_count >= max(expected_count, 1):
        return MarketRegimeLabel.BEARISH_TREND
    if volatility_state in {MarketVolatilityState.ELEVATED, MarketVolatilityState.STRESSED}:
        return MarketRegimeLabel.VOLATILITY_EXPANSION
    if trend_strength in {MarketTrendStrength.STRONG_UP, MarketTrendStrength.MODEST_UP}:
        return MarketRegimeLabel.BULLISH_TREND
    if trend_strength in {MarketTrendStrength.STRONG_DOWN, MarketTrendStrength.MODEST_DOWN}:
        return MarketRegimeLabel.BEARISH_TREND
    if trend_strength == MarketTrendStrength.FLAT:
        return MarketRegimeLabel.RANGE_BOUND
    return MarketRegimeLabel.UNKNOWN


def _risk_posture(regime_label: MarketRegimeLabel, volatility_state: MarketVolatilityState) -> MarketRiskPosture:
    if volatility_state == MarketVolatilityState.STRESSED:
        return MarketRiskPosture.DEFENSIVE
    if regime_label == MarketRegimeLabel.BEARISH_TREND:
        return MarketRiskPosture.RISK_OFF
    if regime_label == MarketRegimeLabel.BULLISH_TREND and volatility_state in {
        MarketVolatilityState.COMPRESSED,
        MarketVolatilityState.NORMAL,
    }:
        return MarketRiskPosture.RISK_ON
    if regime_label in {MarketRegimeLabel.RANGE_BOUND, MarketRegimeLabel.VOLATILITY_EXPANSION, MarketRegimeLabel.TRANSITION}:
        return MarketRiskPosture.NEUTRAL
    return MarketRiskPosture.UNKNOWN


def _data_quality(
    *,
    expected_count: int,
    evidence_count: int,
    return_count: int,
    intraday_count: int,
    volatility_count: int,
    reason_codes: Sequence[str],
    observed_at: datetime,
    expires_at: datetime,
) -> MarketContextDataQuality:
    if evidence_count == 0:
        state = MarketContextDataQualityState.MISSING
        freshness = MarketContextFreshnessState.MISSING
    elif evidence_count < expected_count or return_count < expected_count or intraday_count < expected_count:
        state = MarketContextDataQualityState.PARTIAL
        freshness = MarketContextFreshnessState.FRESH
    else:
        state = MarketContextDataQualityState.COMPLETE
        freshness = MarketContextFreshnessState.FRESH
    components = {
        "benchmark_coverage": state,
        "return_5d": MarketContextDataQualityState.COMPLETE if return_count >= expected_count else MarketContextDataQualityState.PARTIAL,
        "intraday_return": (MarketContextDataQualityState.COMPLETE if intraday_count >= expected_count else MarketContextDataQualityState.PARTIAL),
        "realized_volatility": (
            MarketContextDataQualityState.COMPLETE if volatility_count >= expected_count else MarketContextDataQualityState.PARTIAL
        ),
    }
    return MarketContextDataQuality(
        state=state,
        freshness=freshness,
        observed_at=observed_at,
        stale_after=expires_at,
        reason_codes=tuple(reason_codes),
        missing_components=tuple(key for key, value in components.items() if value != MarketContextDataQualityState.COMPLETE),
        component_states=components,
    )


class MarketContextEngine:
    def build_from_market_slices(
        self,
        benchmark_slices: Mapping[str, SymbolMarketSlice],
        *,
        request: MarketContextRequest | None = None,
    ) -> MarketContextSnapshot:
        resolved_request = request or MarketContextRequest()
        observed_at = resolved_request.observed_at or _now_utc()
        expires_at = resolved_request.expires_at or observed_at + timedelta(seconds=DEFAULT_MARKET_CONTEXT_TTL_SECONDS)
        evidence: list[MarketBenchmarkEvidence] = []
        for symbol in resolved_request.benchmark_symbols:
            market_slice = benchmark_slices.get(symbol)
            if market_slice is None:
                continue
            evidence.append(self._benchmark_evidence_from_market_slice(market_slice, observed_at=observed_at))
        return self._snapshot_from_evidence(
            evidence=tuple(evidence),
            request=resolved_request,
            observed_at=observed_at,
            expires_at=expires_at,
            base_fidelity=(_fidelity_label(resolved_request.source),),
            source_evidence={
                "source": resolved_request.source,
                "expected_benchmark_symbols": list(resolved_request.benchmark_symbols),
                "observed_benchmark_symbols": [item.symbol for item in evidence],
            },
        )

    def build_from_stored_benchmark_facts(
        self,
        benchmark_facts: Mapping[str, Any],
        *,
        request: MarketContextRequest | None = None,
    ) -> MarketContextSnapshot:
        resolved_request = request or MarketContextRequest(source=MarketContextFidelityLabel.STORED_CANDIDATE_CONTEXT.value)
        observed_at = resolved_request.observed_at or _observed_at_from_stored_facts(benchmark_facts) or _now_utc()
        expires_at = resolved_request.expires_at or observed_at + timedelta(seconds=DEFAULT_MARKET_CONTEXT_TTL_SECONDS)
        rows = _stored_benchmark_rows(benchmark_facts)
        evidence = tuple(
            self._benchmark_evidence_from_stored_fact(
                fact=MarketContextBenchmarkFact(
                    symbol=symbol,
                    return_5d_pct=coerce_float(row.get("return_5d_pct")),
                    intraday_return_pct=coerce_float(row.get("intraday_return_pct")),
                    spot_vs_sma20_pct=coerce_float(row.get("spot_vs_sma20_pct")),
                    daily_bar_count=_coerce_int(row.get("daily_bar_count")),
                    intraday_bar_count=_coerce_int(row.get("intraday_bar_count")),
                    observed_at=coerce_utc_datetime(row.get("observed_at")),
                    evidence=row,
                ),
                observed_at=observed_at,
            )
            for symbol, row in rows.items()
            if symbol in resolved_request.benchmark_symbols
        )
        return self._snapshot_from_evidence(
            evidence=evidence,
            request=resolved_request,
            observed_at=observed_at,
            expires_at=expires_at,
            base_fidelity=(_fidelity_label(resolved_request.source),),
            source_evidence={
                "source": resolved_request.source,
                "expected_benchmark_symbols": list(resolved_request.benchmark_symbols),
                "observed_benchmark_symbols": [item.symbol for item in evidence],
                "stored_fact_keys": sorted(str(key) for key in benchmark_facts),
            },
        )

    def _benchmark_evidence_from_market_slice(
        self,
        market_slice: SymbolMarketSlice,
        *,
        observed_at: datetime,
    ) -> MarketBenchmarkEvidence:
        return_5d = _rounded(_daily_return_pct(market_slice.daily_bars))
        intraday_return = _rounded(
            _intraday_return_pct(
                spot_price=market_slice.spot_price,
                bars=market_slice.intraday_bars,
            )
        )
        spot_vs_sma20 = _rounded(
            _spot_vs_sma20_pct(
                spot_price=market_slice.spot_price,
                bars=market_slice.daily_bars,
            )
        )
        realized_volatility = _rounded(_realized_volatility_5d_pct(market_slice.daily_bars))
        drawdown = _rounded(_drawdown_5d_pct(market_slice.daily_bars))
        reason_codes = _benchmark_reason_codes(
            return_5d_pct=return_5d,
            intraday_return_pct=intraday_return,
            realized_volatility_5d_pct=realized_volatility,
        )
        quality = MarketContextDataQualityState.COMPLETE if not reason_codes else MarketContextDataQualityState.PARTIAL
        return MarketBenchmarkEvidence(
            symbol=market_slice.symbol,
            role=_benchmark_role(market_slice.symbol),
            observed_at=observed_at,
            freshness=MarketContextFreshnessState.FRESH,
            data_quality=quality,
            return_5d_pct=return_5d,
            intraday_return_pct=intraday_return,
            drawdown_5d_pct=drawdown,
            realized_volatility_5d_pct=realized_volatility,
            trend_score=_rounded(
                _trend_score(
                    return_5d_pct=return_5d,
                    intraday_return_pct=intraday_return,
                    spot_vs_sma20_pct=spot_vs_sma20,
                )
            ),
            momentum_score=_rounded(return_5d),
            reason_codes=reason_codes,
            evidence={
                "spot_price": market_slice.spot_price,
                "spot_vs_sma20_pct": spot_vs_sma20,
                "daily_bar_count": len(market_slice.daily_bars),
                "intraday_bar_count": len(market_slice.intraday_bars),
            },
        )

    def _benchmark_evidence_from_stored_fact(
        self,
        *,
        fact: MarketContextBenchmarkFact,
        observed_at: datetime,
    ) -> MarketBenchmarkEvidence:
        reason_codes = _benchmark_reason_codes(
            return_5d_pct=fact.return_5d_pct,
            intraday_return_pct=fact.intraday_return_pct,
            realized_volatility_5d_pct=None,
        )
        quality = MarketContextDataQualityState.COMPLETE if not reason_codes else MarketContextDataQualityState.PARTIAL
        return MarketBenchmarkEvidence(
            symbol=fact.symbol,
            role=_benchmark_role(fact.symbol),
            observed_at=fact.observed_at or observed_at,
            freshness=MarketContextFreshnessState.FRESH,
            data_quality=quality,
            return_5d_pct=_rounded(fact.return_5d_pct),
            intraday_return_pct=_rounded(fact.intraday_return_pct),
            trend_score=_rounded(
                _trend_score(
                    return_5d_pct=fact.return_5d_pct,
                    intraday_return_pct=fact.intraday_return_pct,
                    spot_vs_sma20_pct=fact.spot_vs_sma20_pct,
                )
            ),
            momentum_score=_rounded(fact.return_5d_pct),
            reason_codes=reason_codes,
            evidence={
                **dict(fact.evidence),
                "daily_bar_count": fact.daily_bar_count,
                "intraday_bar_count": fact.intraday_bar_count,
            },
        )

    def _snapshot_from_evidence(
        self,
        *,
        evidence: tuple[MarketBenchmarkEvidence, ...],
        request: MarketContextRequest,
        observed_at: datetime,
        expires_at: datetime,
        base_fidelity: tuple[MarketContextFidelityLabel, ...],
        source_evidence: Mapping[str, Any],
    ) -> MarketContextSnapshot:
        expected_count = len(request.benchmark_symbols)
        return_5d_values = [item.return_5d_pct for item in evidence if item.return_5d_pct is not None]
        intraday_values = [item.intraday_return_pct for item in evidence if item.intraday_return_pct is not None]
        volatility_values = [item.realized_volatility_5d_pct for item in evidence if item.realized_volatility_5d_pct is not None]
        blocking_symbols = tuple(
            item.symbol
            for item in evidence
            if (item.return_5d_pct is not None and item.return_5d_pct <= -0.03)
            or (item.intraday_return_pct is not None and item.intraday_return_pct <= -0.012)
        )
        average_return_5d = _average(return_5d_values)
        average_intraday_return = _average(intraday_values)
        volatility_state = _volatility_state(volatility_values)
        trend_strength = _trend_strength(average_return_5d, average_intraday_return)
        regime_label = _regime_label(
            trend_strength=trend_strength,
            volatility_state=volatility_state,
            blocking_count=len(blocking_symbols),
            expected_count=expected_count,
        )
        risk_posture = _risk_posture(regime_label, volatility_state)
        reason_codes = _regime_reason_codes(
            evidence=evidence,
            expected_count=expected_count,
            blocking_symbols=blocking_symbols,
            regime_label=regime_label,
        )
        fidelity = _context_fidelity(
            base_fidelity=base_fidelity,
            expected_count=expected_count,
            evidence_count=len(evidence),
            volatility_count=len(volatility_values),
        )
        data_quality = _data_quality(
            expected_count=expected_count,
            evidence_count=len(evidence),
            return_count=len(return_5d_values),
            intraday_count=len(intraday_values),
            volatility_count=len(volatility_values),
            reason_codes=reason_codes,
            observed_at=observed_at,
            expires_at=expires_at,
        )
        confidence = _confidence(
            expected_count=expected_count,
            evidence_count=len(evidence),
            return_count=len(return_5d_values),
            intraday_count=len(intraday_values),
            volatility_count=len(volatility_values),
        )
        regime = RegimeSnapshot(
            regime_label=regime_label,
            risk_posture=risk_posture,
            trend_strength=trend_strength,
            volatility_state=volatility_state,
            confidence=confidence,
            observed_at=observed_at,
            expires_at=expires_at,
            benchmark_symbols=tuple(item.symbol for item in evidence),
            data_quality=data_quality,
            fidelity=fidelity,
            reason_codes=reason_codes,
            metrics={
                "average_return_5d_pct": _rounded(average_return_5d),
                "average_intraday_return_pct": _rounded(average_intraday_return),
                "average_realized_volatility_5d_pct": _rounded(_average(volatility_values)),
                "blocking_benchmark_count": len(blocking_symbols),
                "blocking_benchmarks": list(blocking_symbols),
                "expected_benchmark_count": expected_count,
                "observed_benchmark_count": len(evidence),
            },
            source_evidence=dict(source_evidence),
        )
        return MarketContextSnapshot(
            scope=request.scope,
            observed_at=observed_at,
            expires_at=expires_at,
            generated_at=_now_utc(),
            context_version=MARKET_CONTEXT_VERSION,
            config_hash=request.config_hash,
            regime=regime,
            benchmark_evidence=evidence,
            data_quality=data_quality,
            fidelity=fidelity,
            source_evidence=dict(source_evidence),
            metadata=dict(request.metadata),
        )


def _context_fidelity(
    *,
    base_fidelity: tuple[MarketContextFidelityLabel, ...],
    expected_count: int,
    evidence_count: int,
    volatility_count: int,
) -> tuple[MarketContextFidelityLabel, ...]:
    extra: list[MarketContextFidelityLabel] = []
    if evidence_count < expected_count:
        extra.append(MarketContextFidelityLabel.PARTIAL_BENCHMARK_COVERAGE)
    if volatility_count < expected_count:
        extra.append(MarketContextFidelityLabel.INCOMPLETE_VOLATILITY_EVIDENCE)
    return _merge_fidelity(base_fidelity, tuple(extra))


def _confidence(
    *,
    expected_count: int,
    evidence_count: int,
    return_count: int,
    intraday_count: int,
    volatility_count: int,
) -> float:
    if expected_count <= 0:
        return 0.0
    coverage = evidence_count / expected_count
    return_coverage = return_count / expected_count
    intraday_coverage = intraday_count / expected_count
    volatility_coverage = volatility_count / expected_count
    return round(max(min(0.40 * coverage + 0.25 * return_coverage + 0.20 * intraday_coverage + 0.15 * volatility_coverage, 1.0), 0.0), 4)


def _average(values: Sequence[float]) -> float | None:
    rows = [value for value in values if value is not None]
    if not rows:
        return None
    return sum(rows) / len(rows)


def _benchmark_role(symbol: str) -> MarketBenchmarkRole:
    normalized = str(symbol or "").upper()
    if normalized == "QQQ":
        return MarketBenchmarkRole.GROWTH_MARKET
    if normalized == "IWM":
        return MarketBenchmarkRole.SMALL_CAP
    if normalized in {"VIX", "VXX"}:
        return MarketBenchmarkRole.VOLATILITY
    if normalized in {"TLT", "IEF"}:
        return MarketBenchmarkRole.RATES
    return MarketBenchmarkRole.BROAD_MARKET


def _benchmark_reason_codes(
    *,
    return_5d_pct: float | None,
    intraday_return_pct: float | None,
    realized_volatility_5d_pct: float | None,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if return_5d_pct is None:
        reasons.append("benchmark_return_5d_missing")
    if intraday_return_pct is None:
        reasons.append("benchmark_intraday_return_missing")
    if realized_volatility_5d_pct is None:
        reasons.append("benchmark_realized_volatility_missing")
    return tuple(reasons)


def _regime_reason_codes(
    *,
    evidence: tuple[MarketBenchmarkEvidence, ...],
    expected_count: int,
    blocking_symbols: tuple[str, ...],
    regime_label: MarketRegimeLabel,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if not evidence:
        reasons.append("market_context_benchmark_data_missing")
    elif len(evidence) < expected_count:
        reasons.append("market_context_partial_benchmark_coverage")
    if blocking_symbols:
        reasons.append("market_context_broad_drawdown")
    if regime_label == MarketRegimeLabel.UNKNOWN:
        reasons.append("market_context_regime_unknown")
    if not reasons:
        reasons.append(f"market_context_{regime_label.value}")
    return tuple(reasons)


def _stored_benchmark_rows(benchmark_facts: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    by_benchmark = benchmark_facts.get("by_benchmark")
    if isinstance(by_benchmark, Mapping):
        return {str(symbol or "").upper().strip(): as_mapping(row) for symbol, row in by_benchmark.items() if str(symbol or "").strip()}
    return {
        str(symbol or "").upper().strip(): as_mapping(row)
        for symbol, row in benchmark_facts.items()
        if str(symbol or "").strip() and isinstance(row, Mapping)
    }


def _observed_at_from_stored_facts(benchmark_facts: Mapping[str, Any]) -> datetime | None:
    for key in ("observed_at", "generated_at", "created_at"):
        parsed = coerce_utc_datetime(benchmark_facts.get(key))
        if parsed is not None:
            return parsed
    for row in _stored_benchmark_rows(benchmark_facts).values():
        parsed = coerce_utc_datetime(row.get("observed_at"))
        if parsed is not None:
            return parsed
    return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "DEFAULT_MARKET_CONTEXT_BENCHMARK_SYMBOLS",
    "DEFAULT_MARKET_CONTEXT_TTL_SECONDS",
    "MARKET_CONTEXT_VERSION",
    "MarketContextBenchmarkFact",
    "MarketContextEngine",
    "MarketContextRequest",
]

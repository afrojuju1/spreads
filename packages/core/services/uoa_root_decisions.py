from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import log1p
from typing import Any

from core.common import clamp
from core.services.uoa_terms import (
    UOA_CRITICAL_DECISION_STATE,
    UOA_EMERGING_DECISION_STATE,
    UOA_HIGH_DECISION_STATE,
    UOA_NONE_DECISION_STATE,
    UOA_NOTABLE_DECISION_STATE,
    uoa_decision_counts,
    uoa_decision_state_rank,
)

EMERGING_DECISION_FLOOR = 60.0
NOTABLE_DECISION_FLOOR = 75.0
HIGH_DECISION_FLOOR = 80.0
CRITICAL_DECISION_FLOOR = 90.0


def _score_log_scale(value: float, *, ceiling: float) -> float:
    if value <= 0 or ceiling <= 0:
        return 0.0
    return clamp(log1p(float(value)) / log1p(float(ceiling)))


def _rate(value: float | int, duration_minutes: float | None) -> float | None:
    if duration_minutes is None or duration_minutes <= 0:
        return None
    return float(value) / duration_minutes


def _safe_ratio(current: float | None, baseline: float | None) -> float | None:
    if current is None or current <= 0 or baseline is None or baseline <= 0:
        return None
    return current / baseline


def _max_ratio(*values: float | None) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    return None if not filtered else max(filtered)


def _dedupe_reason_codes(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        rendered = str(value or "").strip()
        if rendered and rendered not in deduped:
            deduped.append(rendered)
    return deduped


def _decision_state(score: float) -> str:
    if score >= CRITICAL_DECISION_FLOOR:
        return UOA_CRITICAL_DECISION_STATE
    if score >= HIGH_DECISION_FLOOR:
        return UOA_HIGH_DECISION_STATE
    if score >= NOTABLE_DECISION_FLOOR:
        return UOA_NOTABLE_DECISION_STATE
    if score >= EMERGING_DECISION_FLOOR:
        return UOA_EMERGING_DECISION_STATE
    return UOA_NONE_DECISION_STATE


def _quote_context(summary: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not summary:
        return None
    return {
        "observed_contract_count": int(summary.get("observed_contract_count") or 0),
        "fresh_contract_count": int(summary.get("fresh_contract_count") or 0),
        "liquid_contract_count": int(summary.get("liquid_contract_count") or 0),
        "average_quality_score": round(float(summary.get("average_quality_score") or 0.0), 4),
        "supporting_volume": int(summary.get("supporting_volume") or 0),
        "supporting_open_interest": int(summary.get("supporting_open_interest") or 0),
        "supporting_volume_oi_ratio": (
            None
            if summary.get("supporting_volume_oi_ratio") is None
            else round(float(summary.get("supporting_volume_oi_ratio") or 0.0), 4)
        ),
        "max_volume_oi_ratio": round(float(summary.get("max_volume_oi_ratio") or 0.0), 4),
        "quality_state": str(summary.get("quality_state") or "unknown"),
        "surface_coverage_state": str(summary.get("surface_coverage_state") or "missing"),
        "front_expiry": summary.get("front_expiry"),
        "front_expiry_dte": summary.get("front_expiry_dte"),
        "front_expiry_atm_iv": summary.get("front_expiry_atm_iv"),
        "next_expiry": summary.get("next_expiry"),
        "next_expiry_dte": summary.get("next_expiry_dte"),
        "next_expiry_atm_iv": summary.get("next_expiry_atm_iv"),
        "front_next_term_slope": summary.get("front_next_term_slope"),
        "front_atm_call_put_iv_gap": summary.get("front_atm_call_put_iv_gap"),
        "front_expiry_implied_move_pct": summary.get("front_expiry_implied_move_pct"),
    }


def _quote_context_score(
    summary: Mapping[str, Any] | None,
    *,
    open_interest_freshness_score: float | None,
) -> float:
    if not summary:
        return 0.0
    observed = max(int(summary.get("observed_contract_count") or 0), 1)
    fresh_ratio = int(summary.get("fresh_contract_count") or 0) / observed
    liquid_ratio = int(summary.get("liquid_contract_count") or 0) / observed
    quality_score = clamp(float(summary.get("average_quality_score") or 0.0))
    oi_freshness = 0.5 if open_interest_freshness_score is None else clamp(float(open_interest_freshness_score))
    return round(
        fresh_ratio * 40.0
        + liquid_ratio * 30.0
        + quality_score * 20.0
        + oi_freshness * 10.0,
        1,
    )


def _apply_state_cap(
    *,
    base_state: str,
    quote_context: Mapping[str, Any] | None,
    quote_context_score: float,
) -> tuple[str, list[str]]:
    reason_codes: list[str] = []
    state = base_state
    if not quote_context:
        if uoa_decision_state_rank(state) > uoa_decision_state_rank(UOA_EMERGING_DECISION_STATE):
            state = UOA_EMERGING_DECISION_STATE
            reason_codes.append("quote_context_missing")
        return state, reason_codes
    if int(quote_context.get("fresh_contract_count") or 0) <= 0:
        if uoa_decision_state_rank(state) > uoa_decision_state_rank(UOA_EMERGING_DECISION_STATE):
            state = UOA_EMERGING_DECISION_STATE
        reason_codes.append("quote_context_stale")
    if int(quote_context.get("liquid_contract_count") or 0) <= 0:
        if uoa_decision_state_rank(state) > uoa_decision_state_rank(UOA_EMERGING_DECISION_STATE):
            state = UOA_EMERGING_DECISION_STATE
        reason_codes.append("quote_liquidity_unconfirmed")
    if quote_context_score < 55.0:
        if uoa_decision_state_rank(state) > uoa_decision_state_rank(UOA_EMERGING_DECISION_STATE):
            state = UOA_EMERGING_DECISION_STATE
        reason_codes.append("quote_context_weak")
    elif quote_context_score < 70.0 and state == UOA_CRITICAL_DECISION_STATE:
        state = UOA_HIGH_DECISION_STATE
        reason_codes.append("critical_suppressed_quote_context")
    return state, reason_codes


def _supporting_contract_rank(
    contract: Mapping[str, Any],
    *,
    root_scoreable_premium: float,
) -> float:
    contract_premium = float(contract.get("scoreable_premium") or 0.0)
    premium_share = 0.0 if root_scoreable_premium <= 0 else clamp(contract_premium / root_scoreable_premium)
    signed_size = abs(int(contract.get("signed_size") or 0))
    quote_quality = clamp(float(contract.get("quality_score") or 0.0))
    atm_relevance = clamp(float(contract.get("atm_relevance_score") or 0.0))
    volume_oi_ratio = clamp(float(contract.get("volume_oi_ratio") or 0.0) / 1.0)
    return round(
        premium_share * 35.0
        + clamp(signed_size / 25.0) * 20.0
        + quote_quality * 20.0
        + atm_relevance * 15.0
        + volume_oi_ratio * 10.0,
        1,
    )


def _rank_supporting_contracts(
    root: Mapping[str, Any],
    *,
    flow_shape: str,
) -> list[dict[str, Any]]:
    root_scoreable_premium = float(root.get("scoreable_premium") or 0.0)
    top_contracts = [
        dict(item)
        for item in list(root.get("top_contracts") or [])
        if isinstance(item, Mapping)
    ]
    for contract in top_contracts:
        contract["support_score"] = _supporting_contract_rank(
            contract,
            root_scoreable_premium=root_scoreable_premium,
        )
    top_contracts.sort(
        key=lambda item: (
            -float(item.get("support_score") or 0.0),
            -float(item.get("scoreable_premium") or 0.0),
            str(item.get("option_symbol") or ""),
        )
    )
    if flow_shape != "volatility_demand":
        return top_contracts[:3]

    paired: list[dict[str, Any]] = []
    front_expiry = str(root.get("front_expiry") or "").strip()
    atm_candidates = [
        item
        for item in top_contracts
        if str(item.get("expiration_date") or "").strip() == front_expiry
        and str(item.get("option_type") or "") in {"call", "put"}
        and float(item.get("atm_relevance_score") or 0.0) >= 0.5
    ]
    seen_types: set[str] = set()
    for contract in atm_candidates:
        option_type = str(contract.get("option_type") or "")
        if option_type in seen_types:
            continue
        paired.append(contract)
        seen_types.add(option_type)
        if len(paired) >= 2:
            break
    for contract in top_contracts:
        if len(paired) >= 3:
            break
        symbol = str(contract.get("option_symbol") or "")
        if symbol and all(str(item.get("option_symbol") or "") != symbol for item in paired):
            paired.append(contract)
    return paired[:3]


def _flow_anomaly_score(
    *,
    root: Mapping[str, Any],
    max_premium_ratio: float | None,
    max_trade_ratio: float | None,
) -> float:
    contract_count = int(root.get("scoreable_contract_count") or 0)
    scoreable_premium = float(root.get("scoreable_premium") or 0.0)
    aggressor_known_ratio = clamp(float(root.get("aggressor_known_ratio") or 0.0))
    premium_component = 0.0 if max_premium_ratio is None else clamp((max_premium_ratio - 1.0) / 4.0) * 35.0
    trade_component = 0.0 if max_trade_ratio is None else clamp((max_trade_ratio - 1.0) / 3.0) * 25.0
    breadth_component = clamp(contract_count / 4.0) * 15.0
    absolute_component = _score_log_scale(scoreable_premium, ceiling=100_000.0) * 15.0
    known_component = aggressor_known_ratio * 10.0
    return round(
        premium_component
        + trade_component
        + breadth_component
        + absolute_component
        + known_component,
        1,
    )


def _directional_flow_score(root: Mapping[str, Any]) -> tuple[float, str]:
    scoreable_premium = max(float(root.get("scoreable_premium") or 0.0), 1.0)
    gross_delta_notional = max(float(root.get("gross_delta_notional") or 0.0), 1.0)
    signed_premium_ratio = clamp(abs(float(root.get("signed_premium") or 0.0)) / scoreable_premium)
    signed_delta_ratio = clamp(abs(float(root.get("signed_delta_notional") or 0.0)) / gross_delta_notional)
    signed_premium = float(root.get("signed_premium") or 0.0)
    signed_delta = float(root.get("signed_delta_notional") or 0.0)
    aggressor_known_ratio = clamp(float(root.get("aggressor_known_ratio") or 0.0))
    sign_consistency = 1.0 if signed_premium == 0 or signed_delta == 0 or signed_premium * signed_delta >= 0 else 0.0
    directional_bias = "mixed"
    signed_delta_share = 0.0 if gross_delta_notional <= 0 else signed_delta / gross_delta_notional
    if signed_delta_share >= 0.2:
        directional_bias = "bullish"
    elif signed_delta_share <= -0.2:
        directional_bias = "bearish"
    score = round(
        signed_premium_ratio * 35.0
        + signed_delta_ratio * 35.0
        + sign_consistency * 15.0
        + aggressor_known_ratio * 15.0,
        1,
    )
    return score, directional_bias


def _volatility_demand_score(
    root: Mapping[str, Any],
    *,
    quote_context: Mapping[str, Any] | None,
) -> float:
    call_put_balance = clamp(float(root.get("call_put_balance_score") or 0.0))
    atm_concentration = clamp(float(root.get("atm_concentration_score") or 0.0))
    same_expiry_symmetry = clamp(float(root.get("same_expiry_symmetry_score") or 0.0))
    positive_vega_share = clamp(float(root.get("positive_vega_share") or 0.0))
    front_expiry_concentration = clamp(float(root.get("front_expiry_concentration_score") or 0.0))
    surface_state = 0.0
    if quote_context is not None:
        surface_coverage_state = str(quote_context.get("surface_coverage_state") or "missing")
        if surface_coverage_state == "strong":
            surface_state = 1.0
        elif surface_coverage_state == "partial":
            surface_state = 0.6
    return round(
        call_put_balance * 20.0
        + atm_concentration * 20.0
        + same_expiry_symmetry * 15.0
        + positive_vega_share * 20.0
        + front_expiry_concentration * 15.0
        + surface_state * 10.0,
        1,
    )


def _explanation(
    *,
    symbol: str,
    flow_shape: str,
    directional_bias: str,
    root_interest_score: float,
    quote_context: Mapping[str, Any] | None,
    scoreable_premium: float,
    scoreable_contract_count: int,
) -> str:
    segments = [
        symbol,
        flow_shape.replace("_", " "),
        f"score {root_interest_score:.1f}",
        f"${scoreable_premium:,.0f} scoreable premium",
        f"{scoreable_contract_count} active contract{'s' if scoreable_contract_count != 1 else ''}",
    ]
    if directional_bias in {"bullish", "bearish"}:
        segments.append(directional_bias)
    if quote_context is not None:
        quality_state = str(quote_context.get("quality_state") or "unknown")
        segments.append(f"quotes {quality_state}")
    return ", ".join(segments)


def _baseline_payload(summary: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not summary:
        return None
    return {
        "session_date": summary.get("session_date"),
        "duration_minutes": summary.get("duration_minutes"),
        "scoreable_premium": summary.get("scoreable_premium"),
        "scoreable_trade_count": summary.get("scoreable_trade_count"),
        "scoreable_contract_count": summary.get("scoreable_contract_count"),
        "premium_rate_per_minute": summary.get("premium_rate_per_minute"),
        "trade_rate_per_minute": summary.get("trade_rate_per_minute"),
        "contract_rate_per_minute": summary.get("contract_rate_per_minute"),
    }


def build_uoa_root_decisions(
    *,
    uoa_summary: Mapping[str, Any] | None,
    baselines_by_symbol: Mapping[str, Mapping[str, Any]] | None,
    quote_summary: Mapping[str, Any] | None = None,
    capture_window_seconds: float,
) -> dict[str, Any]:
    summary_payload = {} if uoa_summary is None else dict(uoa_summary)
    roots = [
        dict(item)
        for item in (summary_payload.get("top_roots") or [])
        if isinstance(item, Mapping)
    ]
    duration_minutes = max(float(capture_window_seconds), 1.0) / 60.0
    baseline_map = {} if baselines_by_symbol is None else dict(baselines_by_symbol)
    quote_summary_payload = {} if quote_summary is None else dict(quote_summary)
    quote_roots = quote_summary_payload.get("roots")
    quote_root_map = (
        {}
        if not isinstance(quote_roots, Mapping)
        else {
            str(symbol): dict(payload)
            for symbol, payload in quote_roots.items()
            if isinstance(payload, Mapping)
        }
    )
    decisions: list[dict[str, Any]] = []

    for root in roots:
        symbol = str(root.get("underlying_symbol") or "").strip()
        if not symbol:
            continue
        baseline = baseline_map.get(symbol) or {}
        current_premium_rate = _rate(float(root.get("scoreable_premium") or 0.0), duration_minutes)
        current_trade_rate = _rate(int(root.get("scoreable_trade_count") or 0), duration_minutes)
        current_contract_rate = _rate(int(root.get("scoreable_contract_count") or 0), duration_minutes)

        rolling = baseline.get("rolling_5m")
        session = baseline.get("session_to_time")
        prior = baseline.get("previous_session_same_time")
        rolling_premium_ratio = _safe_ratio(
            current_premium_rate,
            None if not rolling else rolling.get("premium_rate_per_minute"),
        )
        rolling_trade_ratio = _safe_ratio(
            current_trade_rate,
            None if not rolling else rolling.get("trade_rate_per_minute"),
        )
        session_premium_ratio = _safe_ratio(
            current_premium_rate,
            None if not session else session.get("premium_rate_per_minute"),
        )
        session_trade_ratio = _safe_ratio(
            current_trade_rate,
            None if not session else session.get("trade_rate_per_minute"),
        )
        prior_premium_ratio = _safe_ratio(
            current_premium_rate,
            None if not prior else prior.get("premium_rate_per_minute"),
        )
        prior_trade_ratio = _safe_ratio(
            current_trade_rate,
            None if not prior else prior.get("trade_rate_per_minute"),
        )
        max_premium_ratio = _max_ratio(
            rolling_premium_ratio,
            session_premium_ratio,
            prior_premium_ratio,
        )
        max_trade_ratio = _max_ratio(
            rolling_trade_ratio,
            session_trade_ratio,
            prior_trade_ratio,
        )

        quote_root = quote_root_map.get(symbol)
        quote_context = _quote_context(quote_root)
        flow_anomaly_score = _flow_anomaly_score(
            root=root,
            max_premium_ratio=max_premium_ratio,
            max_trade_ratio=max_trade_ratio,
        )
        directional_flow_score, directional_bias = _directional_flow_score(root)
        volatility_demand_score = _volatility_demand_score(
            root,
            quote_context=quote_context,
        )
        quote_context_score = _quote_context_score(
            quote_root,
            open_interest_freshness_score=root.get("open_interest_freshness_score"),
        )
        stock_context_score = 50.0
        directional_interest_score = round(
            flow_anomaly_score * 0.45
            + directional_flow_score * 0.35
            + stock_context_score * 0.10
            + quote_context_score * 0.10,
            1,
        )
        volatility_interest_score = round(
            flow_anomaly_score * 0.40
            + volatility_demand_score * 0.35
            + stock_context_score * 0.10
            + quote_context_score * 0.15,
            1,
        )
        root_interest_score = round(
            max(directional_interest_score, volatility_interest_score),
            1,
        )
        flow_shape = "mixed"
        if directional_interest_score >= volatility_interest_score + 10.0 and directional_bias == "bullish":
            flow_shape = "directional_bullish"
        elif directional_interest_score >= volatility_interest_score + 10.0 and directional_bias == "bearish":
            flow_shape = "directional_bearish"
        elif volatility_interest_score >= directional_interest_score + 10.0:
            flow_shape = "volatility_demand"
        base_state = _decision_state(root_interest_score)
        state, cap_reason_codes = _apply_state_cap(
            base_state=base_state,
            quote_context=quote_context,
            quote_context_score=quote_context_score,
        )
        reason_codes: list[str] = []
        if max_premium_ratio is not None and max_premium_ratio >= 3.0:
            reason_codes.append("premium_rate_gt_3x_baseline")
        elif max_premium_ratio is not None and max_premium_ratio >= 2.0:
            reason_codes.append("premium_rate_gt_2x_baseline")
        if max_trade_ratio is not None and max_trade_ratio >= 2.0:
            reason_codes.append("trade_rate_gt_2x_baseline")
        if int(root.get("scoreable_contract_count") or 0) >= 2:
            reason_codes.append("multi_contract_confirmation")
        if flow_shape == "volatility_demand":
            reason_codes.append("two_sided_volatility_demand")
        if flow_shape in {"directional_bullish", "directional_bearish"}:
            reason_codes.append(flow_shape)
        reason_codes.extend(cap_reason_codes)
        if not reason_codes:
            reason_codes.append("absolute_flow_observed")
        supporting_contracts = _rank_supporting_contracts(root, flow_shape=flow_shape)

        decisions.append(
            {
                "underlying_symbol": symbol,
                "decision_state": state,
                "decision_state_pre_cap": base_state,
                "decision_score": root_interest_score,
                "root_interest_score": root_interest_score,
                "directional_interest_score": directional_interest_score,
                "volatility_interest_score": volatility_interest_score,
                "flow_anomaly_score": flow_anomaly_score,
                "directional_flow_score": directional_flow_score,
                "volatility_demand_score": volatility_demand_score,
                "quote_context_score": quote_context_score,
                "stock_context_score": stock_context_score,
                "flow_shape": flow_shape,
                "directional_bias": directional_bias,
                "reason_codes": _dedupe_reason_codes(reason_codes),
                "explanation": _explanation(
                    symbol=symbol,
                    flow_shape=flow_shape,
                    directional_bias=directional_bias,
                    root_interest_score=root_interest_score,
                    quote_context=quote_context,
                    scoreable_premium=float(root.get("scoreable_premium") or 0.0),
                    scoreable_contract_count=int(root.get("scoreable_contract_count") or 0),
                ),
                "score_components": {
                    "flow_anomaly_score": flow_anomaly_score,
                    "directional_flow_score": directional_flow_score,
                    "volatility_demand_score": volatility_demand_score,
                    "quote_context_score": quote_context_score,
                    "stock_context_score": stock_context_score,
                    "directional_interest_score": directional_interest_score,
                    "volatility_interest_score": volatility_interest_score,
                },
                "driver_metrics": {
                    "call_put_balance_score": root.get("call_put_balance_score"),
                    "atm_concentration_score": root.get("atm_concentration_score"),
                    "same_expiry_symmetry_score": root.get("same_expiry_symmetry_score"),
                    "front_expiry_concentration_score": root.get("front_expiry_concentration_score"),
                    "positive_vega_share": root.get("positive_vega_share"),
                    "aggressor_known_ratio": root.get("aggressor_known_ratio"),
                },
                "current": {
                    "root_score": root.get("root_score"),
                    "scoreable_premium": float(root.get("scoreable_premium") or 0.0),
                    "scoreable_trade_count": int(root.get("scoreable_trade_count") or 0),
                    "scoreable_contract_count": int(root.get("scoreable_contract_count") or 0),
                    "scoreable_size": int(root.get("scoreable_size") or 0),
                    "supporting_volume": int(root.get("supporting_volume") or 0),
                    "supporting_open_interest": int(root.get("supporting_open_interest") or 0),
                    "supporting_volume_oi_ratio": root.get("supporting_volume_oi_ratio"),
                    "max_volume_oi_ratio": root.get("max_volume_oi_ratio"),
                    "premium_rate_per_minute": round(float(current_premium_rate or 0.0), 4),
                    "trade_rate_per_minute": round(float(current_trade_rate or 0.0), 4),
                    "contract_rate_per_minute": round(float(current_contract_rate or 0.0), 4),
                    "dominant_flow": root.get("dominant_flow"),
                    "dominant_flow_ratio": root.get("dominant_flow_ratio"),
                    "signed_premium": root.get("signed_premium"),
                    "signed_delta_notional": root.get("signed_delta_notional"),
                    "signed_vega_notional": root.get("signed_vega_notional"),
                    "flow_shape": flow_shape,
                    "directional_bias": directional_bias,
                },
                "quote_context": quote_context,
                "baselines": {
                    "rolling_5m": _baseline_payload(rolling),
                    "session_to_time": _baseline_payload(session),
                    "previous_session_same_time": _baseline_payload(prior),
                },
                "deltas": {
                    "rolling_5m_premium_rate_ratio": None if rolling_premium_ratio is None else round(float(rolling_premium_ratio), 4),
                    "rolling_5m_trade_rate_ratio": None if rolling_trade_ratio is None else round(float(rolling_trade_ratio), 4),
                    "session_premium_rate_ratio": None if session_premium_ratio is None else round(float(session_premium_ratio), 4),
                    "session_trade_rate_ratio": None if session_trade_ratio is None else round(float(session_trade_ratio), 4),
                    "previous_session_premium_rate_ratio": None if prior_premium_ratio is None else round(float(prior_premium_ratio), 4),
                    "previous_session_trade_rate_ratio": None if prior_trade_ratio is None else round(float(prior_trade_ratio), 4),
                    "max_premium_rate_ratio": None if max_premium_ratio is None else round(float(max_premium_ratio), 4),
                    "max_trade_rate_ratio": None if max_trade_ratio is None else round(float(max_trade_ratio), 4),
                },
                "top_supporting_contracts": supporting_contracts,
                "top_contracts": supporting_contracts,
            }
        )

    decisions.sort(
        key=lambda item: (
            -float(item["decision_score"]),
            str(item["underlying_symbol"]),
        )
    )
    counts = uoa_decision_counts(decisions)
    top_decision = None if not decisions else decisions[0]
    top_emerging_roots = [
        dict(item)
        for item in decisions
        if item["decision_state"]
        in {
            UOA_EMERGING_DECISION_STATE,
            UOA_NOTABLE_DECISION_STATE,
            UOA_HIGH_DECISION_STATE,
            UOA_CRITICAL_DECISION_STATE,
        }
    ][:5]
    top_notable_roots = [
        dict(item)
        for item in decisions
        if item["decision_state"]
        in {
            UOA_NOTABLE_DECISION_STATE,
            UOA_HIGH_DECISION_STATE,
            UOA_CRITICAL_DECISION_STATE,
        }
    ][:5]
    top_high_roots = [
        dict(item)
        for item in decisions
        if item["decision_state"] in {UOA_HIGH_DECISION_STATE, UOA_CRITICAL_DECISION_STATE}
    ][:5]
    top_critical_roots = [
        dict(item)
        for item in decisions
        if item["decision_state"] == UOA_CRITICAL_DECISION_STATE
    ][:5]
    overview = {
        "decision_status": "empty" if not decisions else "active",
        "root_count": len(decisions),
        "emerging_count": counts[UOA_EMERGING_DECISION_STATE],
        "notable_count": counts[UOA_NOTABLE_DECISION_STATE],
        "high_count": counts[UOA_HIGH_DECISION_STATE],
        "critical_count": counts[UOA_CRITICAL_DECISION_STATE],
        "watchlist_count": counts[UOA_EMERGING_DECISION_STATE],
        "board_count": counts[UOA_NOTABLE_DECISION_STATE],
        "monitor_count": counts[UOA_EMERGING_DECISION_STATE],
        "promotable_count": counts[UOA_NOTABLE_DECISION_STATE],
        "top_decision_state": None if top_decision is None else top_decision["decision_state"],
        "top_decision_symbol": None if top_decision is None else top_decision["underlying_symbol"],
        "top_decision_score": None if top_decision is None else top_decision["decision_score"],
        "top_decision_shape": None if top_decision is None else top_decision["flow_shape"],
        "top_decision_bias": None if top_decision is None else top_decision["directional_bias"],
    }
    return {
        "overview": overview,
        "roots": decisions,
        "top_emerging_roots": top_emerging_roots,
        "top_notable_roots": top_notable_roots,
        "top_high_roots": top_high_roots,
        "top_critical_roots": top_critical_roots,
        "top_watchlist_roots": top_emerging_roots,
        "top_monitor_roots": top_emerging_roots,
        "top_board_roots": top_notable_roots,
        "top_promotable_roots": top_notable_roots,
    }


__all__ = ["build_uoa_root_decisions"]

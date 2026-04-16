from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import log1p
from typing import Any

from core.common import clamp
from core.services.selection_terms import (
    UOA_HIGH_DECISION_STATE,
    UOA_MONITOR_DECISION_STATE,
    UOA_PROMOTABLE_DECISION_STATE,
    uoa_decision_counts,
    uoa_decision_state_rank,
)

MONITOR_DECISION_FLOOR = 60.0
PROMOTABLE_DECISION_FLOOR = 75.0
HIGH_DECISION_FLOOR = 80.0


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


def _ratio_component(ratio: float | None, *, max_points: float, full_scale_ratio: float) -> float:
    if ratio is None or ratio <= 1.0:
        return 0.0
    return clamp((ratio - 1.0) / max(full_scale_ratio - 1.0, 0.0001)) * max_points


def _decision_state(score: float) -> str:
    if score >= HIGH_DECISION_FLOOR:
        return UOA_HIGH_DECISION_STATE
    if score >= PROMOTABLE_DECISION_FLOOR:
        return UOA_PROMOTABLE_DECISION_STATE
    if score >= MONITOR_DECISION_FLOOR:
        return UOA_MONITOR_DECISION_STATE
    return "none"


def _apply_state_cap(state: str, cap_state: str | None) -> tuple[str, str | None]:
    if cap_state is None:
        return state, None
    if uoa_decision_state_rank(state) <= uoa_decision_state_rank(cap_state):
        return state, None
    return cap_state, cap_state


def _dedupe_reason_codes(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        rendered = str(value or "").strip()
        if rendered and rendered not in deduped:
            deduped.append(rendered)
    return deduped


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
    }


def _quote_state_cap(summary: Mapping[str, Any] | None) -> tuple[str | None, list[str]]:
    if not summary:
        return None, ["quote_context_missing"]
    quality_state = str(summary.get("quality_state") or "unknown")
    fresh_contract_count = int(summary.get("fresh_contract_count") or 0)
    liquid_contract_count = int(summary.get("liquid_contract_count") or 0)
    average_quality_score = float(summary.get("average_quality_score") or 0.0)
    if fresh_contract_count <= 0:
        return UOA_MONITOR_DECISION_STATE, ["quote_context_stale"]
    if liquid_contract_count <= 0:
        return UOA_MONITOR_DECISION_STATE, ["quote_liquidity_unconfirmed"]
    if quality_state == "weak" or average_quality_score < 0.45:
        return UOA_MONITOR_DECISION_STATE, ["quote_quality_weak"]
    if quality_state == "strong":
        return None, ["quote_quality_strong"]
    if quality_state == "acceptable":
        return None, ["quote_quality_acceptable"]
    return None, []


def _merge_contract_quote_fields(
    contract: Mapping[str, Any],
    *,
    quote_contracts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    merged = dict(contract)
    option_symbol = str(contract.get("option_symbol") or "").strip()
    if not option_symbol:
        return merged
    quote = quote_contracts.get(option_symbol)
    if not quote:
        return merged
    for key in (
        "expiration_date",
        "dte",
        "strike_price",
        "underlying_price",
        "percent_otm",
        "open_interest",
        "volume",
        "volume_oi_ratio",
        "implied_volatility",
        "delta",
        "bid",
        "ask",
        "midpoint",
        "spread",
        "spread_pct",
        "bid_size",
        "ask_size",
        "min_size",
        "quote_timestamp",
        "quote_age_seconds",
        "is_fresh",
        "passes_liquidity_gate",
        "quality_score",
        "quality_state",
    ):
        if quote.get(key) is not None:
            merged[key] = quote.get(key)
    return merged


def _explanation(
    summary: Mapping[str, Any],
    decision: Mapping[str, Any],
    *,
    quote_context: Mapping[str, Any] | None,
) -> str:
    dominant_flow = str(summary.get("dominant_flow") or "mixed")
    premium = float(summary.get("scoreable_premium") or 0.0)
    contracts = int(summary.get("scoreable_contract_count") or 0)
    max_premium_ratio = decision.get("max_premium_rate_ratio")
    max_trade_ratio = decision.get("max_trade_rate_ratio")
    segments = [
        f"{summary.get('underlying_symbol')} {dominant_flow} flow",
        f"${premium:,.0f} scoreable premium",
        f"{contracts} active contract{'s' if contracts != 1 else ''}",
    ]
    if max_premium_ratio is not None and float(max_premium_ratio) > 1.0:
        segments.append(f"{float(max_premium_ratio):.1f}x premium rate vs baseline")
    elif max_trade_ratio is not None and float(max_trade_ratio) > 1.0:
        segments.append(f"{float(max_trade_ratio):.1f}x trade rate vs baseline")
    if quote_context:
        quality_state = str(quote_context.get("quality_state") or "unknown")
        fresh_contract_count = int(quote_context.get("fresh_contract_count") or 0)
        liquid_contract_count = int(quote_context.get("liquid_contract_count") or 0)
        supporting_volume = int(quote_context.get("supporting_volume") or 0)
        supporting_open_interest = int(quote_context.get("supporting_open_interest") or 0)
        supporting_volume_oi_ratio = quote_context.get("supporting_volume_oi_ratio")
        max_volume_oi_ratio = float(quote_context.get("max_volume_oi_ratio") or 0.0)
        if quality_state == "strong":
            segments.append(f"quotes strong ({liquid_contract_count} liquid)")
        elif quality_state == "acceptable":
            segments.append(f"quotes acceptable ({fresh_contract_count} fresh)")
        elif quality_state == "stale":
            segments.append("quotes stale")
        elif quality_state == "weak":
            segments.append("quotes weak")
        if supporting_volume > 0 or supporting_open_interest > 0:
            segments.append(f"session vol/oi {supporting_volume:,}/{supporting_open_interest:,}")
        if supporting_volume_oi_ratio is not None and float(supporting_volume_oi_ratio) > 0:
            segments.append(f"root vol/oi {float(supporting_volume_oi_ratio):.2f}x")
        if max_volume_oi_ratio > 0:
            segments.append(f"best contract vol/oi {max_volume_oi_ratio:.2f}x")
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
    quote_contracts = quote_summary_payload.get("contracts")
    quote_contract_map = (
        {}
        if not isinstance(quote_contracts, Mapping)
        else {
            str(symbol): dict(payload)
            for symbol, payload in quote_contracts.items()
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
        max_premium_ratio = _max_ratio(rolling_premium_ratio, session_premium_ratio, prior_premium_ratio)
        max_trade_ratio = _max_ratio(rolling_trade_ratio, session_trade_ratio, prior_trade_ratio)

        root_score = float(root.get("root_score") or 0.0)
        dominant_flow_ratio = float(root.get("dominant_flow_ratio") or 0.0)
        contract_count = int(root.get("scoreable_contract_count") or 0)
        premium_rate = float(current_premium_rate or 0.0)
        quote_root = quote_root_map.get(symbol)
        quote_context = _quote_context(quote_root)
        quote_quality_score = 0.0 if not quote_root else float(quote_root.get("average_quality_score") or 0.0)
        supporting_volume_oi_ratio = float(root.get("supporting_volume_oi_ratio") or 0.0)
        max_volume_oi_ratio = float(root.get("max_volume_oi_ratio") or 0.0)
        if quote_root:
            supporting_volume_oi_ratio = float(quote_root.get("supporting_volume_oi_ratio") or 0.0)
            max_volume_oi_ratio = float(quote_root.get("max_volume_oi_ratio") or 0.0)
        effective_supporting_volume = int(root.get("supporting_volume") or 0)
        effective_supporting_open_interest = int(root.get("supporting_open_interest") or 0)
        effective_supporting_volume_oi_ratio = (
            round(float(root.get("supporting_volume_oi_ratio") or 0.0), 4)
            if root.get("supporting_volume_oi_ratio") is not None
            else None
        )
        effective_max_volume_oi_ratio = round(float(root.get("max_volume_oi_ratio") or 0.0), 4)
        if quote_context:
            effective_supporting_volume = int(quote_context.get("supporting_volume") or 0)
            effective_supporting_open_interest = int(quote_context.get("supporting_open_interest") or 0)
            effective_supporting_volume_oi_ratio = (
                None
                if quote_context.get("supporting_volume_oi_ratio") is None
                else round(float(quote_context.get("supporting_volume_oi_ratio") or 0.0), 4)
            )
            effective_max_volume_oi_ratio = round(float(quote_context.get("max_volume_oi_ratio") or 0.0), 4)
        decision_score = round(
            root_score * 0.55
            + _ratio_component(max_premium_ratio, max_points=20.0, full_scale_ratio=5.0)
            + _ratio_component(max_trade_ratio, max_points=10.0, full_scale_ratio=4.0)
            + clamp(max(contract_count - 1, 0) / 2.0) * 5.0
            + clamp(dominant_flow_ratio) * 5.0
            + _score_log_scale(premium_rate, ceiling=6_000.0) * 5.0,
            1,
        )
        decision_score = round(
            decision_score
            + clamp(quote_quality_score) * 5.0
            + clamp(supporting_volume_oi_ratio / 1.0) * 3.0
            + clamp(max_volume_oi_ratio / 1.0) * 5.0,
            1,
        )
        base_state = _decision_state(decision_score)
        state_cap, quote_reason_codes = _quote_state_cap(quote_root)
        state, state_cap_applied = _apply_state_cap(base_state, state_cap)
        reason_codes: list[str] = []
        if max_premium_ratio is not None and max_premium_ratio >= 3.0:
            reason_codes.append("premium_rate_gt_3x_baseline")
        elif max_premium_ratio is not None and max_premium_ratio >= 2.0:
            reason_codes.append("premium_rate_gt_2x_baseline")
        if max_trade_ratio is not None and max_trade_ratio >= 2.0:
            reason_codes.append("trade_rate_gt_2x_baseline")
        if contract_count >= 2:
            reason_codes.append("multi_contract_confirmation")
        dominant_flow = str(root.get("dominant_flow") or "mixed")
        if dominant_flow in {"call", "put"} and dominant_flow_ratio >= 0.85:
            reason_codes.append(f"dominant_{dominant_flow}_flow")
        if max_volume_oi_ratio >= 1.0:
            reason_codes.append("contract_volume_ge_open_interest")
        elif max_volume_oi_ratio >= 0.5:
            reason_codes.append("contract_volume_gt_half_open_interest")
        if supporting_volume_oi_ratio >= 1.0:
            reason_codes.append("root_volume_ge_open_interest")
        elif supporting_volume_oi_ratio >= 0.5:
            reason_codes.append("root_volume_gt_half_open_interest")
        reason_codes.extend(quote_reason_codes)
        if state_cap_applied is not None:
            reason_codes.append(f"decision_capped_to_{state_cap_applied}")
        if not reason_codes:
            reason_codes.append("absolute_flow_observed")
        enriched_top_contracts = [
            _merge_contract_quote_fields(item, quote_contracts=quote_contract_map)
            for item in (root.get("top_contracts") or [])[:3]
            if isinstance(item, Mapping)
        ]

        decisions.append(
            {
                "underlying_symbol": symbol,
                "decision_state": state,
                "decision_state_pre_quote_cap": base_state,
                "decision_score": decision_score,
                "reason_codes": _dedupe_reason_codes(reason_codes),
                "explanation": _explanation(
                    root,
                    {
                        "max_premium_rate_ratio": max_premium_ratio,
                        "max_trade_rate_ratio": max_trade_ratio,
                    },
                    quote_context=quote_context,
                ),
                "current": {
                    "root_score": root_score,
                    "scoreable_premium": float(root.get("scoreable_premium") or 0.0),
                    "scoreable_trade_count": int(root.get("scoreable_trade_count") or 0),
                    "scoreable_contract_count": contract_count,
                    "scoreable_size": int(root.get("scoreable_size") or 0),
                    "supporting_volume": effective_supporting_volume,
                    "supporting_open_interest": effective_supporting_open_interest,
                    "supporting_volume_oi_ratio": effective_supporting_volume_oi_ratio,
                    "max_volume_oi_ratio": effective_max_volume_oi_ratio,
                    "premium_rate_per_minute": round(premium_rate, 4),
                    "trade_rate_per_minute": round(float(current_trade_rate or 0.0), 4),
                    "contract_rate_per_minute": round(float(current_contract_rate or 0.0), 4),
                    "dominant_flow": dominant_flow,
                    "dominant_flow_ratio": dominant_flow_ratio,
                },
                "quote_context": quote_context,
                "state_cap_applied": state_cap_applied,
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
                "top_contracts": enriched_top_contracts,
            }
        )

    decisions.sort(
        key=lambda item: (
            -float(item["decision_score"]),
            str(item["underlying_symbol"]),
        )
    )
    counts = uoa_decision_counts(decisions)
    overview = {
        "decision_status": "empty" if not decisions else "active",
        "root_count": len(decisions),
        "monitor_count": counts[UOA_MONITOR_DECISION_STATE],
        "promotable_count": counts[UOA_PROMOTABLE_DECISION_STATE],
        "high_count": counts[UOA_HIGH_DECISION_STATE],
        "top_decision_state": None if not decisions else decisions[0]["decision_state"],
        "top_decision_symbol": None if not decisions else decisions[0]["underlying_symbol"],
        "top_decision_score": None if not decisions else decisions[0]["decision_score"],
    }
    return {
        "overview": overview,
        "roots": decisions,
        "top_monitor_roots": [
            dict(item)
            for item in decisions
            if item["decision_state"]
            in {
                UOA_MONITOR_DECISION_STATE,
                UOA_PROMOTABLE_DECISION_STATE,
                UOA_HIGH_DECISION_STATE,
            }
        ][:5],
        "top_promotable_roots": [
            dict(item)
            for item in decisions
            if item["decision_state"]
            in {UOA_PROMOTABLE_DECISION_STATE, UOA_HIGH_DECISION_STATE}
        ][:5],
        "top_high_roots": [
            dict(item)
            for item in decisions
            if item["decision_state"] == UOA_HIGH_DECISION_STATE
        ][:5],
    }

from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import log1p
from typing import Any

from spreads.common import clamp

WATCHLIST_DECISION_FLOOR = 60.0
BOARD_DECISION_FLOOR = 75.0
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
        return "high"
    if score >= BOARD_DECISION_FLOOR:
        return "board"
    if score >= WATCHLIST_DECISION_FLOOR:
        return "watchlist"
    return "none"


def _explanation(summary: Mapping[str, Any], decision: Mapping[str, Any]) -> str:
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
        decision_score = round(
            root_score * 0.55
            + _ratio_component(max_premium_ratio, max_points=20.0, full_scale_ratio=5.0)
            + _ratio_component(max_trade_ratio, max_points=10.0, full_scale_ratio=4.0)
            + clamp(max(contract_count - 1, 0) / 2.0) * 5.0
            + clamp(dominant_flow_ratio) * 5.0
            + _score_log_scale(premium_rate, ceiling=6_000.0) * 5.0,
            1,
        )
        state = _decision_state(decision_score)
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
        if not reason_codes:
            reason_codes.append("absolute_flow_observed")

        decisions.append(
            {
                "underlying_symbol": symbol,
                "decision_state": state,
                "decision_score": decision_score,
                "reason_codes": reason_codes,
                "explanation": _explanation(root, {"max_premium_rate_ratio": max_premium_ratio, "max_trade_rate_ratio": max_trade_ratio}),
                "current": {
                    "root_score": root_score,
                    "scoreable_premium": float(root.get("scoreable_premium") or 0.0),
                    "scoreable_trade_count": int(root.get("scoreable_trade_count") or 0),
                    "scoreable_contract_count": contract_count,
                    "premium_rate_per_minute": round(premium_rate, 4),
                    "trade_rate_per_minute": round(float(current_trade_rate or 0.0), 4),
                    "contract_rate_per_minute": round(float(current_contract_rate or 0.0), 4),
                    "dominant_flow": dominant_flow,
                    "dominant_flow_ratio": dominant_flow_ratio,
                },
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
                "top_contracts": [dict(item) for item in (root.get("top_contracts") or [])[:3]],
            }
        )

    decisions.sort(
        key=lambda item: (
            -float(item["decision_score"]),
            str(item["underlying_symbol"]),
        )
    )
    overview = {
        "decision_status": "empty" if not decisions else "active",
        "root_count": len(decisions),
        "watchlist_count": sum(1 for item in decisions if item["decision_state"] == "watchlist"),
        "board_count": sum(1 for item in decisions if item["decision_state"] == "board"),
        "high_count": sum(1 for item in decisions if item["decision_state"] == "high"),
        "top_decision_state": None if not decisions else decisions[0]["decision_state"],
        "top_decision_symbol": None if not decisions else decisions[0]["underlying_symbol"],
        "top_decision_score": None if not decisions else decisions[0]["decision_score"],
    }
    return {
        "overview": overview,
        "roots": decisions,
        "top_watchlist_roots": [dict(item) for item in decisions if item["decision_state"] in {"watchlist", "board", "high"}][:5],
        "top_board_roots": [dict(item) for item in decisions if item["decision_state"] in {"board", "high"}][:5],
        "top_high_roots": [dict(item) for item in decisions if item["decision_state"] == "high"][:5],
    }

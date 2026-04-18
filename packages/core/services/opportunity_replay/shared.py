from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.services.selection_terms import normalize_selection_state
from core.services.opportunity_scoring import candidate_event_state

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

RECOVERY_TOP = 12
RECOVERY_PER_STRATEGY = 3

UNKNOWN_BUCKET_ORDER = 99


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ratio_or_none(numerator: Any, denominator: Any) -> float | None:
    resolved_numerator = _as_float(numerator)
    resolved_denominator = _as_float(denominator)
    if (
        resolved_numerator is None
        or resolved_denominator is None
        or resolved_denominator <= 0.0
    ):
        return None
    return round(resolved_numerator / resolved_denominator, 4)


def _entry_return_on_risk_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    if resolved < 0.08:
        return "<0.08", 0
    if resolved < 0.10:
        return "0.08-0.10", 1
    if resolved < 0.12:
        return "0.10-0.12", 2
    if resolved < 0.14:
        return "0.12-0.14", 3
    if resolved < 0.16:
        return "0.14-0.16", 4
    if resolved < 0.20:
        return "0.16-0.20", 5
    return "0.20+", 6


def _midpoint_credit_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    if resolved < 0.10:
        return "<0.10", 0
    if resolved < 0.15:
        return "0.10-0.14", 1
    if resolved < 0.20:
        return "0.15-0.19", 2
    if resolved < 0.25:
        return "0.20-0.24", 3
    if resolved < 0.35:
        return "0.25-0.34", 4
    if resolved < 0.50:
        return "0.35-0.49", 5
    return "0.50+", 6


def _width_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    if resolved <= 1.0:
        return "<=1.00", 0
    if resolved <= 2.0:
        return "1.01-2.00", 1
    if resolved <= 3.0:
        return "2.01-3.00", 2
    if resolved <= 5.0:
        return "3.01-5.00", 3
    return ">5.00", 4


def _dte_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    dte = int(resolved)
    if dte <= 0:
        return "0", 0
    if dte <= 2:
        return "1-2", 1
    if dte <= 5:
        return "3-5", 2
    if dte <= 10:
        return "6-10", 3
    if dte <= 20:
        return "11-20", 4
    if dte <= 45:
        return "21-45", 5
    return "46+", 6


def _baseline_selection_state_from_row(row: Mapping[str, Any]) -> str | None:
    return normalize_selection_state(
        row.get(
            "baseline_selection_state",
            row.get("selection_state", row.get("bucket")),
        )
    )


def _group_value_from_row(
    *,
    dimension: str,
    row: Mapping[str, Any],
) -> str | None:
    group_value = _as_text(row.get("group_value")) or _as_text(row.get("bucket"))
    if dimension == "classification":
        return normalize_selection_state(group_value)
    return group_value


def _normalize_score(value: Any, *, default: float = 0.0) -> float:
    parsed = _as_float(value)
    if parsed is None:
        return default
    return _clamp(parsed / 100.0, 0.0, 1.0)


def _style_profile(
    legacy_profile: str | None, *, days_to_expiration: int | None
) -> str:
    normalized = str(legacy_profile or "").strip().lower()
    if normalized in PROFILE_TO_STYLE:
        return PROFILE_TO_STYLE[normalized]
    if days_to_expiration == 0:
        return "reactive"
    if days_to_expiration is not None and days_to_expiration <= 12:
        return "tactical"
    return "carry"


def _product_class(symbol: str) -> str:
    if symbol in CASH_SETTLED_INDEX_SYMBOLS:
        return "cash_settled_index"
    if symbol in TOP_TIER_ETF_SYMBOLS:
        return "top_tier_etf"
    if symbol in BROAD_ETF_SYMBOLS:
        return "broad_etf"
    return "single_name_equity"


def _strategy_family(strategy: str | None) -> str:
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
        "iron_condor": "iron_condor",
    }.get(normalized, normalized or "unknown")


def _thesis_direction(family: str) -> str:
    if family in {"put_credit_spread", "call_debit_spread", "long_call"}:
        return "bullish"
    if family in {"call_credit_spread", "put_debit_spread", "long_put"}:
        return "bearish"
    if family == "iron_condor":
        return "neutral"
    return "unknown"


def _horizon_band(days_to_expiration: int | None) -> tuple[str, int, int, str]:
    if days_to_expiration is None:
        return ("near_term", 3, 12, "weekly")
    for band, lower, upper, expiration_type in HORIZON_BANDS:
        if lower <= days_to_expiration <= upper:
            return band, lower, upper, expiration_type
    if days_to_expiration < 0:
        return ("same_day", 0, 0, "daily")
    return ("carry", 46, max(days_to_expiration, 46), "monthly")


def _liquidity_state(candidate: Mapping[str, Any]) -> str:
    fill_ratio = _as_float(candidate.get("fill_ratio")) or 0.0
    min_quote_size = _as_float(candidate.get("min_quote_size")) or 0.0
    if fill_ratio >= 0.85 and min_quote_size >= 50:
        return "healthy"
    if fill_ratio >= 0.7:
        return "thin"
    return "degraded"


def _direction_from_candidates(candidates: list[Mapping[str, Any]]) -> str:
    if not candidates:
        return "unknown"
    best = max(candidates, key=lambda item: _as_float(item.get("quality_score")) or 0.0)
    return _thesis_direction(_strategy_family(_as_text(best.get("strategy"))))


def _intraday_structure(candidate: Mapping[str, Any]) -> str:
    setup_status = str(candidate.get("setup_status") or "").strip().lower()
    if setup_status == "favorable":
        return "trend"
    if setup_status == "neutral":
        return "range"
    if setup_status == "unfavorable":
        return "unstable"
    return "unknown"


def _vol_level(candidate: Mapping[str, Any]) -> str:
    expected_move_pct = _as_float(candidate.get("expected_move_pct"))
    if expected_move_pct is None:
        return "unknown"
    if expected_move_pct < 0.005:
        return "low"
    if expected_move_pct < 0.015:
        return "normal"
    return "high"


def _event_state(candidate: Mapping[str, Any]) -> str:
    return candidate_event_state(candidate)


def _parse_datetime(value: Any) -> datetime | None:
    text = _as_text(value)
    if text is None:
        return None
    normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _minutes_between(start: Any, end: Any) -> float | None:
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if start_dt is None or end_dt is None:
        return None
    return round((end_dt - start_dt).total_seconds() / 60.0, 1)


def _timestamp_is_after(left: Any, right: Any) -> bool | None:
    left_dt = _parse_datetime(left)
    right_dt = _parse_datetime(right)
    if left_dt is None or right_dt is None:
        return None
    return left_dt > right_dt

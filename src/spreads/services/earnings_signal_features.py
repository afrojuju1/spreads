from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

EARNINGS_SIGNAL_FIELDS = (
    "direction_signal",
    "jump_risk_signal",
    "pricing_signal",
    "post_event_confirmation_signal",
)

SIGNAL_SCORE_ALIASES = {
    "direction_signal": ("direction_signal", "earnings_direction_signal"),
    "jump_risk_signal": ("jump_risk_signal", "earnings_jump_risk_signal"),
    "pricing_signal": ("pricing_signal", "earnings_pricing_signal"),
    "post_event_confirmation_signal": (
        "post_event_confirmation_signal",
        "earnings_post_event_confirmation_signal",
    ),
}

SIGNAL_SUBSIGNAL_COUNT_ALIASES = {
    "direction_signal": (
        "direction_signal_subsignal_count",
        "direction_signal_component_count",
    ),
    "jump_risk_signal": (
        "jump_risk_signal_subsignal_count",
        "jump_risk_signal_component_count",
    ),
    "pricing_signal": (
        "pricing_signal_subsignal_count",
        "pricing_signal_component_count",
    ),
    "post_event_confirmation_signal": (
        "post_event_confirmation_signal_subsignal_count",
        "post_event_confirmation_signal_component_count",
    ),
}

SETUP_FIELD_ALIASES = {
    "spot_vs_vwap_pct": ("setup_spot_vs_vwap_pct", "spot_vs_vwap_pct"),
    "intraday_return_pct": ("setup_intraday_return_pct", "intraday_return_pct"),
    "distance_to_session_extreme_pct": (
        "setup_distance_to_session_extreme_pct",
        "distance_to_session_extreme_pct",
    ),
    "opening_range_break_pct": (
        "setup_opening_range_break_pct",
        "opening_range_break_pct",
    ),
    "latest_close": ("setup_latest_close", "latest_close"),
    "vwap": ("setup_vwap", "vwap"),
    "opening_range_high": ("setup_opening_range_high", "opening_range_high"),
    "opening_range_low": ("setup_opening_range_low", "opening_range_low"),
}


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


def _as_int(value: Any) -> int | None:
    parsed = _as_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return None
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off"}:
        return False
    return None


def _normalize_unit_score(value: Any) -> float | None:
    parsed = _as_float(value)
    if parsed is None:
        return None
    if parsed > 1.0:
        parsed = parsed / 100.0
    return round(_clamp(parsed, 0.0, 1.0), 4)


def _first_present_value(candidate: Mapping[str, Any], aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        if alias in candidate and candidate.get(alias) not in (None, ""):
            return candidate.get(alias)
    return None


def _mean_score(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / float(len(values)), 4)


def _strategy_family(strategy: str | None) -> str:
    normalized = str(strategy or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
        "long_call": "long_call",
        "long_put": "long_put",
        "iron_condor": "iron_condor",
    }.get(normalized, normalized or "unknown")


def _family_direction(family: str) -> str:
    if family in {"put_credit_spread", "call_debit_spread", "long_call"}:
        return "bullish"
    if family in {"call_credit_spread", "put_debit_spread", "long_put"}:
        return "bearish"
    if family == "iron_condor":
        return "neutral"
    return "unknown"


def _resolve_family(candidate: Mapping[str, Any], family: str | None) -> str:
    return family or _strategy_family(_as_text(candidate.get("strategy")))


def _is_friday_after_hours_event(candidate: Mapping[str, Any]) -> bool:
    event_date = _as_text(candidate.get("earnings_event_date"))
    session_timing = str(candidate.get("earnings_session_timing") or "").strip().lower()
    if event_date is None or session_timing != "after_close":
        return False
    try:
        return datetime.fromisoformat(event_date).weekday() == 4
    except ValueError:
        return False


def _status_score(candidate: Mapping[str, Any], *, family: str) -> float | None:
    setup_status = str(candidate.get("setup_status") or "").strip().lower()
    direction = _family_direction(family)
    if setup_status not in {"favorable", "neutral", "unfavorable"}:
        return None
    if direction == "neutral":
        return {
            "favorable": 0.3,
            "neutral": 1.0,
            "unfavorable": 0.2,
        }.get(setup_status)
    return {
        "favorable": 1.0,
        "neutral": 0.55,
        "unfavorable": 0.1,
    }.get(setup_status)


def _volume_oi_score(candidate: Mapping[str, Any]) -> float | None:
    scores: list[float] = []
    for volume_key, oi_key in (
        ("short_volume", "short_open_interest"),
        ("long_volume", "long_open_interest"),
    ):
        volume = _as_float(candidate.get(volume_key))
        open_interest = _as_float(candidate.get(oi_key))
        if volume is None or open_interest in (None, 0.0):
            continue
        scores.append(_clamp(volume / max(open_interest, 1.0), 0.0, 1.0))
    return _mean_score(scores)


def _average_implied_volatility(candidate: Mapping[str, Any]) -> float | None:
    values = [
        _as_float(candidate.get("short_implied_volatility")),
        _as_float(candidate.get("long_implied_volatility")),
        _as_float(candidate.get("implied_volatility")),
    ]
    filtered = [value for value in values if value is not None]
    return None if not filtered else round(sum(filtered) / float(len(filtered)), 4)


def _setup_metric(candidate: Mapping[str, Any], field: str) -> float | None:
    aliases = SETUP_FIELD_ALIASES.get(field)
    if aliases is None:
        return None
    return _as_float(_first_present_value(candidate, aliases))


def _vwap_alignment_score(candidate: Mapping[str, Any], *, family: str) -> float | None:
    pct = _setup_metric(candidate, "spot_vs_vwap_pct")
    if pct is None:
        return None
    direction = _family_direction(family)
    if direction == "bullish":
        if pct > 0.0015:
            return 1.0
        if pct < -0.0015:
            return 0.0
        return 0.55
    if direction == "bearish":
        if pct < -0.0015:
            return 1.0
        if pct > 0.0015:
            return 0.0
        return 0.55
    if abs(pct) <= 0.0015:
        return 1.0
    if abs(pct) >= 0.004:
        return 0.1
    return 0.5


def _trend_alignment_score(candidate: Mapping[str, Any], *, family: str) -> float | None:
    pct = _setup_metric(candidate, "intraday_return_pct")
    if pct is None:
        return None
    direction = _family_direction(family)
    if direction == "bullish":
        if pct > 0.004:
            return 1.0
        if pct < -0.004:
            return 0.0
        return 0.55
    if direction == "bearish":
        if pct < -0.004:
            return 1.0
        if pct > 0.004:
            return 0.0
        return 0.55
    if abs(pct) <= 0.004:
        return 1.0
    if abs(pct) >= 0.012:
        return 0.1
    return 0.5


def _opening_range_alignment_score(
    candidate: Mapping[str, Any],
    *,
    family: str,
) -> float | None:
    breakout_pct = _setup_metric(candidate, "opening_range_break_pct")
    latest_close = _setup_metric(candidate, "latest_close")
    opening_range_high = _setup_metric(candidate, "opening_range_high")
    opening_range_low = _setup_metric(candidate, "opening_range_low")
    direction = _family_direction(family)
    if (
        breakout_pct is None
        and latest_close is None
        and opening_range_high is None
        and opening_range_low is None
    ):
        return None
    if direction == "neutral":
        if breakout_pct is not None and abs(breakout_pct) <= 0.001:
            return 1.0
        if (
            latest_close is not None
            and opening_range_high is not None
            and opening_range_low is not None
            and opening_range_low <= latest_close <= opening_range_high
        ):
            return 1.0
        return 0.15
    if breakout_pct is not None and breakout_pct > 0.001:
        return 1.0
    if direction == "bullish":
        if (
            latest_close is not None
            and opening_range_low is not None
            and latest_close < opening_range_low
        ):
            return 0.0
    elif direction == "bearish":
        if (
            latest_close is not None
            and opening_range_high is not None
            and latest_close > opening_range_high
        ):
            return 0.0
    return 0.55


def _session_extreme_score(candidate: Mapping[str, Any], *, family: str) -> float | None:
    pct = _setup_metric(candidate, "distance_to_session_extreme_pct")
    if pct is None:
        return None
    if _family_direction(family) == "neutral":
        if pct < 0.003:
            return 0.15
        if pct > 0.008:
            return 0.8
        return 0.55
    if pct < 0.003:
        return 0.2
    if pct > 0.008:
        return 0.75
    return 0.5


def _dominant_flow_score(candidate: Mapping[str, Any], *, family: str) -> float | None:
    dominant_flow = str(
        candidate.get("uoa_dominant_flow") or candidate.get("dominant_flow") or ""
    ).strip().lower()
    dominant_flow_ratio = _as_float(
        candidate.get("uoa_dominant_flow_ratio") or candidate.get("dominant_flow_ratio")
    )
    direction = _family_direction(family)
    if dominant_flow not in {"call", "put", "mixed"} and dominant_flow_ratio is None:
        return None
    if direction == "neutral":
        if dominant_flow == "mixed":
            return 1.0
        if dominant_flow_ratio is None:
            return 0.5
        if dominant_flow_ratio <= 0.6:
            return 0.8
        if dominant_flow_ratio >= 0.8:
            return 0.1
        return 0.4
    if dominant_flow == "mixed":
        return 0.45
    if direction == "bullish":
        if dominant_flow == "call":
            return 1.0 if dominant_flow_ratio is None or dominant_flow_ratio >= 0.65 else 0.7
        if dominant_flow == "put":
            return 0.0 if dominant_flow_ratio is None or dominant_flow_ratio >= 0.65 else 0.3
    if direction == "bearish":
        if dominant_flow == "put":
            return 1.0 if dominant_flow_ratio is None or dominant_flow_ratio >= 0.65 else 0.7
        if dominant_flow == "call":
            return 0.0 if dominant_flow_ratio is None or dominant_flow_ratio >= 0.65 else 0.3
    return 0.45


def _resolve_options_bias_alignment(
    candidate: Mapping[str, Any],
    *,
    family: str,
) -> tuple[bool | None, str]:
    explicit = _as_bool(candidate.get("options_bias_alignment"))
    if explicit is not None:
        return explicit, "explicit"
    components = [
        value
        for value in (
            _dominant_flow_score(candidate, family=family),
            _vwap_alignment_score(candidate, family=family),
            _trend_alignment_score(candidate, family=family),
            _opening_range_alignment_score(candidate, family=family),
            _status_score(candidate, family=family),
        )
        if value is not None
    ]
    score = _mean_score(components)
    if score is None:
        return None, "missing"
    if score >= 0.6:
        return True, "derived"
    if score <= 0.4:
        return False, "derived"
    return None, "derived"


def _resolve_neutral_regime_signal(candidate: Mapping[str, Any]) -> tuple[float | None, int | None, str]:
    explicit = _normalize_unit_score(candidate.get("neutral_regime_signal"))
    if explicit is not None:
        return explicit, _as_int(candidate.get("neutral_regime_signal_component_count")), "explicit"
    components = [
        value
        for value in (
            _status_score(candidate, family="iron_condor"),
            _vwap_alignment_score(candidate, family="iron_condor"),
            _trend_alignment_score(candidate, family="iron_condor"),
            _opening_range_alignment_score(candidate, family="iron_condor"),
            _session_extreme_score(candidate, family="iron_condor"),
            _dominant_flow_score(candidate, family="iron_condor"),
        )
        if value is not None
    ]
    score = _mean_score(components)
    if score is None:
        return None, None, "missing"
    return score, len(components), "derived"


def _resolve_residual_iv_richness(
    candidate: Mapping[str, Any],
) -> tuple[float | None, int | None, str]:
    explicit = _normalize_unit_score(candidate.get("residual_iv_richness"))
    if explicit is not None:
        return explicit, _as_int(candidate.get("residual_iv_richness_component_count")), "explicit"
    components: list[float] = []
    average_iv = _average_implied_volatility(candidate)
    if average_iv is not None:
        components.append(_clamp((average_iv - 0.22) / 0.28, 0.0, 1.0))
    expected_move_pct = _as_float(candidate.get("expected_move_pct"))
    if expected_move_pct is not None:
        components.append(_clamp((expected_move_pct - 0.012) / 0.028, 0.0, 1.0))
    volume_oi_score = _volume_oi_score(candidate)
    if volume_oi_score is not None:
        components.append(volume_oi_score)
    score = _mean_score(components)
    if score is None:
        return None, None, "missing"
    return score, len(components), "derived"


def _resolve_modeled_move_vs_implied_move(candidate: Mapping[str, Any]) -> float | None:
    explicit = _as_float(candidate.get("modeled_move_vs_implied_move"))
    if explicit is not None:
        return explicit
    return None


def _resolve_modeled_move_vs_break_even_move(candidate: Mapping[str, Any]) -> float | None:
    explicit = _as_float(candidate.get("modeled_move_vs_break_even_move"))
    if explicit is not None:
        return explicit
    return None


def _derived_direction_signal(
    candidate: Mapping[str, Any],
    *,
    family: str,
    options_bias_alignment: bool | None,
) -> tuple[float | None, int | None]:
    components: list[float] = []
    for item in (
        _normalize_unit_score(candidate.get("setup_intraday_score")),
        _normalize_unit_score(candidate.get("setup_score")),
        _normalize_unit_score(candidate.get("quality_score")),
        _status_score(candidate, family=family),
        _vwap_alignment_score(candidate, family=family),
        _trend_alignment_score(candidate, family=family),
        _opening_range_alignment_score(candidate, family=family),
    ):
        if item is not None:
            components.append(item)
    if options_bias_alignment is not None:
        components.append(1.0 if options_bias_alignment else 0.0)
    score = _mean_score(components)
    if score is None:
        return None, None
    return score, len(components)


def _derived_jump_risk_signal(
    candidate: Mapping[str, Any],
) -> tuple[float | None, int | None]:
    components: list[float] = []
    expected_move_pct = _as_float(candidate.get("expected_move_pct"))
    if expected_move_pct is not None:
        components.append(_clamp((expected_move_pct - 0.01) / 0.025, 0.0, 1.0))
    volume_oi_score = _volume_oi_score(candidate)
    if volume_oi_score is not None:
        components.append(volume_oi_score)
    quality_score = _normalize_unit_score(candidate.get("quality_score"))
    if quality_score is not None:
        components.append(_clamp(quality_score * 0.9, 0.0, 1.0))
    average_iv = _average_implied_volatility(candidate)
    if average_iv is not None:
        components.append(_clamp((average_iv - 0.18) / 0.32, 0.0, 1.0))
    score = _mean_score(components)
    if score is None:
        return None, None
    return score, len(components)


def _derived_pricing_signal(
    candidate: Mapping[str, Any],
) -> tuple[float | None, int | None]:
    components: list[float] = []
    fill_ratio = _as_float(candidate.get("fill_ratio"))
    if fill_ratio is not None:
        components.append(_clamp((fill_ratio - 0.6) / 0.35, 0.0, 1.0))
    debit_width_ratio = _as_float(candidate.get("debit_width_ratio"))
    if debit_width_ratio is not None:
        components.append(_clamp((0.70 - debit_width_ratio) / 0.25, 0.0, 1.0))
    modeled_move_vs_implied_move = _resolve_modeled_move_vs_implied_move(candidate)
    if modeled_move_vs_implied_move is not None:
        components.append(_clamp((modeled_move_vs_implied_move - 0.9) / 0.3, 0.0, 1.0))
    modeled_move_vs_break_even_move = _resolve_modeled_move_vs_break_even_move(candidate)
    if modeled_move_vs_break_even_move is not None:
        components.append(
            _clamp((modeled_move_vs_break_even_move - 0.9) / 0.25, 0.0, 1.0)
        )
    score = _mean_score(components)
    if score is None:
        return None, None
    return score, len(components)


def _derived_post_event_confirmation_signal(
    candidate: Mapping[str, Any],
    *,
    family: str,
    neutral_regime_signal: float | None,
    options_bias_alignment: bool | None,
) -> tuple[float | None, int | None]:
    components: list[float] = []
    for item in (
        _normalize_unit_score(candidate.get("setup_intraday_score")),
        _normalize_unit_score(candidate.get("setup_score")),
        _normalize_unit_score(candidate.get("quality_score")),
        _status_score(candidate, family=family),
        _vwap_alignment_score(candidate, family=family),
        _trend_alignment_score(candidate, family=family),
        _opening_range_alignment_score(candidate, family=family),
    ):
        if item is not None:
            components.append(item)
    if _family_direction(family) == "neutral" and neutral_regime_signal is not None:
        components.append(neutral_regime_signal)
    if options_bias_alignment is not None:
        components.append(1.0 if options_bias_alignment else 0.0)
    score = _mean_score(components)
    if score is None:
        return None, None
    return score, len(components)


def _derived_signal(
    field: str,
    candidate: Mapping[str, Any],
    *,
    family: str,
    options_bias_alignment: bool | None,
    neutral_regime_signal: float | None,
) -> tuple[float | None, int | None]:
    if field == "direction_signal":
        return _derived_direction_signal(
            candidate,
            family=family,
            options_bias_alignment=options_bias_alignment,
        )
    if field == "jump_risk_signal":
        return _derived_jump_risk_signal(candidate)
    if field == "pricing_signal":
        return _derived_pricing_signal(candidate)
    if field == "post_event_confirmation_signal":
        return _derived_post_event_confirmation_signal(
            candidate,
            family=family,
            neutral_regime_signal=neutral_regime_signal,
            options_bias_alignment=options_bias_alignment,
        )
    return None, None


def build_earnings_signal_bundle(
    candidate: Mapping[str, Any],
    *,
    family: str | None = None,
) -> dict[str, Any]:
    resolved_family = _resolve_family(candidate, family)
    options_bias_alignment, options_bias_source = _resolve_options_bias_alignment(
        candidate,
        family=resolved_family,
    )
    neutral_regime_signal, neutral_regime_components, neutral_regime_source = (
        _resolve_neutral_regime_signal(candidate)
    )
    residual_iv_richness, residual_iv_components, residual_iv_source = (
        _resolve_residual_iv_richness(candidate)
    )

    signals: dict[str, dict[str, Any]] = {}
    for field in EARNINGS_SIGNAL_FIELDS:
        explicit_score = _normalize_unit_score(
            _first_present_value(candidate, SIGNAL_SCORE_ALIASES[field])
        )
        explicit_subsignal_count = _as_int(
            _first_present_value(candidate, SIGNAL_SUBSIGNAL_COUNT_ALIASES[field])
        )
        derived_score, derived_subsignal_count = _derived_signal(
            field,
            candidate,
            family=resolved_family,
            options_bias_alignment=options_bias_alignment,
            neutral_regime_signal=neutral_regime_signal,
        )
        signals[field] = {
            "score": explicit_score if explicit_score is not None else derived_score,
            "subsignal_count": (
                explicit_subsignal_count
                if explicit_subsignal_count is not None
                else derived_subsignal_count
            ),
            "source": (
                "explicit"
                if explicit_score is not None
                else ("derived" if derived_score is not None else "missing")
            ),
        }

    return {
        "signals": signals,
        "options_bias_alignment": options_bias_alignment,
        "options_bias_alignment_source": options_bias_source,
        "debit_width_ratio": _as_float(candidate.get("debit_width_ratio")),
        "modeled_move_vs_implied_move": _resolve_modeled_move_vs_implied_move(candidate),
        "modeled_move_vs_break_even_move": _resolve_modeled_move_vs_break_even_move(
            candidate
        ),
        "neutral_regime_signal": neutral_regime_signal,
        "neutral_regime_signal_components": neutral_regime_components,
        "neutral_regime_signal_source": neutral_regime_source,
        "residual_iv_richness": residual_iv_richness,
        "residual_iv_richness_components": residual_iv_components,
        "residual_iv_richness_source": residual_iv_source,
        "average_implied_volatility": _average_implied_volatility(candidate),
        "volume_oi_score": _volume_oi_score(candidate),
        "friday_after_hours_event": _is_friday_after_hours_event(candidate),
    }


__all__ = [
    "EARNINGS_SIGNAL_FIELDS",
    "SIGNAL_SCORE_ALIASES",
    "SIGNAL_SUBSIGNAL_COUNT_ALIASES",
    "build_earnings_signal_bundle",
]

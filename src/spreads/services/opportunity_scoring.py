from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_requires_favorable_setup,
)
from spreads.services.earnings_signal_features import (
    EARNINGS_SIGNAL_FIELDS,
    build_earnings_signal_bundle,
)

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
EARNINGS_PHASES = {
    "clean",
    "pre_event_runup",
    "through_event",
    "post_event_fresh",
    "post_event_settled",
}
ACTIVE_EARNINGS_PHASES = {
    "pre_event_runup",
    "through_event",
    "post_event_fresh",
}
LONG_VOL_FAMILIES = {"long_straddle", "long_strangle"}
DIRECTIONAL_DEBIT_FAMILIES = {"call_debit_spread", "put_debit_spread"}
SHORT_PREMIUM_FAMILIES = {
    "call_credit_spread",
    "put_credit_spread",
    "iron_condor",
}
SUPPORTED_EARNINGS_HORIZONS = {"next_daily", "near_term", "post_event"}


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


def _normalize_score(value: Any, *, default: float = 0.0) -> float:
    parsed = _as_float(value)
    if parsed is None:
        return default
    return _clamp(parsed / 100.0, 0.0, 1.0)


def resolve_style_profile(
    legacy_profile: str | None,
    *,
    days_to_expiration: int | None,
) -> str:
    normalized = str(legacy_profile or "").strip().lower()
    if normalized in PROFILE_TO_STYLE:
        return PROFILE_TO_STYLE[normalized]
    if days_to_expiration == 0:
        return "reactive"
    if days_to_expiration is not None and days_to_expiration <= 12:
        return "tactical"
    return "carry"


def strategy_family(strategy: str | None) -> str:
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


def product_class(symbol: str) -> str:
    if symbol in CASH_SETTLED_INDEX_SYMBOLS:
        return "cash_settled_index"
    if symbol in TOP_TIER_ETF_SYMBOLS:
        return "top_tier_etf"
    if symbol in BROAD_ETF_SYMBOLS:
        return "broad_etf"
    return "single_name_equity"


def horizon_band(days_to_expiration: int | None) -> tuple[str, int, int, str]:
    if days_to_expiration is None:
        return ("near_term", 3, 12, "weekly")
    for band, lower, upper, expiration_type in HORIZON_BANDS:
        if lower <= days_to_expiration <= upper:
            return band, lower, upper, expiration_type
    if days_to_expiration < 0:
        return ("same_day", 0, 0, "daily")
    return ("carry", 46, max(days_to_expiration, 46), "monthly")


def candidate_earnings_phase(candidate: Mapping[str, Any]) -> str:
    normalized = str(candidate.get("earnings_phase") or "").strip().lower()
    if normalized in EARNINGS_PHASES:
        return normalized
    calendar_status = str(candidate.get("calendar_status") or "").strip().lower()
    if calendar_status in {"penalized", "blocked"}:
        return "through_event"
    return "clean"


def candidate_event_state(candidate: Mapping[str, Any]) -> str:
    return candidate_earnings_phase(candidate)


def candidate_event_timing_rule(candidate: Mapping[str, Any]) -> str:
    phase = candidate_earnings_phase(candidate)
    return {
        "clean": "none",
        "pre_event_runup": "avoid_event",
        "through_event": "include_event",
        "post_event_fresh": "post_event",
        "post_event_settled": "normal_policy",
    }.get(phase, "none")


def earnings_signal_thresholds(
    *,
    family: str,
    earnings_phase: str,
    friday_after_hours_event: bool = False,
) -> dict[str, Any]:
    thresholds: dict[str, Any] = {
        "active": False,
        "lead_signal": None,
        "lead_signal_min": None,
        "lead_signal_subsignal_min": None,
        "pricing_signal_min": None,
        "pricing_signal_subsignal_min": None,
        "dte_min": None,
        "dte_max": None,
        "dte_ideal_min": None,
        "dte_ideal_max": None,
        "options_bias_alignment_required": False,
        "debit_width_ratio_max": None,
        "modeled_move_vs_implied_move_min": None,
        "modeled_move_vs_break_even_move_min": None,
        "neutral_regime_signal_min": None,
        "residual_iv_richness_min": None,
    }
    if earnings_phase == "pre_event_runup" and family in DIRECTIONAL_DEBIT_FAMILIES:
        thresholds.update(
            {
                "active": True,
                "lead_signal": "direction_signal",
                "lead_signal_min": 0.65,
                "lead_signal_subsignal_min": 2,
                "pricing_signal_min": 0.55,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 4,
                "dte_max": 15,
                "dte_ideal_min": 4,
                "dte_ideal_max": 12,
                "options_bias_alignment_required": True,
                "debit_width_ratio_max": 0.60,
            }
        )
    elif earnings_phase == "pre_event_runup" and family in LONG_VOL_FAMILIES:
        thresholds.update(
            {
                "active": True,
                "lead_signal": "jump_risk_signal",
                "lead_signal_min": 0.70,
                "lead_signal_subsignal_min": 2,
                "pricing_signal_min": 0.60,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 2,
                "dte_max": 10,
                "dte_ideal_min": 2,
                "dte_ideal_max": 7,
            }
        )
    elif earnings_phase == "through_event" and family in DIRECTIONAL_DEBIT_FAMILIES:
        thresholds.update(
            {
                "active": True,
                "lead_signal": "direction_signal",
                "lead_signal_min": 0.70,
                "lead_signal_subsignal_min": 2,
                "pricing_signal_min": 0.60,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 2,
                "dte_max": 10,
                "dte_ideal_min": 2,
                "dte_ideal_max": 7,
                "options_bias_alignment_required": True,
                "debit_width_ratio_max": 0.60,
            }
        )
    elif earnings_phase == "through_event" and family == "long_straddle":
        thresholds.update(
            {
                "active": True,
                "lead_signal": "jump_risk_signal",
                "lead_signal_min": 0.70,
                "lead_signal_subsignal_min": 2,
                "pricing_signal_min": 0.60,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 2,
                "dte_max": 10,
                "dte_ideal_min": 2,
                "dte_ideal_max": 7,
                "modeled_move_vs_implied_move_min": 1.10,
            }
        )
    elif earnings_phase == "through_event" and family == "long_strangle":
        thresholds.update(
            {
                "active": True,
                "lead_signal": "jump_risk_signal",
                "lead_signal_min": 0.70,
                "lead_signal_subsignal_min": 2,
                "pricing_signal_min": 0.60,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 2,
                "dte_max": 10,
                "dte_ideal_min": 2,
                "dte_ideal_max": 7,
                "modeled_move_vs_break_even_move_min": 1.05,
            }
        )
    elif earnings_phase == "post_event_fresh" and family in DIRECTIONAL_DEBIT_FAMILIES:
        confirmation_min = 0.65 + (0.05 if friday_after_hours_event else 0.0)
        thresholds.update(
            {
                "active": True,
                "lead_signal": "post_event_confirmation_signal",
                "lead_signal_min": round(min(confirmation_min, 0.99), 2),
                "lead_signal_subsignal_min": 3,
                "pricing_signal_min": 0.55,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 2,
                "dte_max": 15,
                "dte_ideal_min": 2,
                "dte_ideal_max": 10,
            }
        )
    elif earnings_phase == "post_event_fresh" and family in {
        "call_credit_spread",
        "put_credit_spread",
    }:
        confirmation_min = 0.65 + (0.05 if friday_after_hours_event else 0.0)
        thresholds.update(
            {
                "active": True,
                "lead_signal": "post_event_confirmation_signal",
                "lead_signal_min": round(min(confirmation_min, 0.99), 2),
                "lead_signal_subsignal_min": 3,
                "pricing_signal_min": 0.55,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 2,
                "dte_max": 15,
                "dte_ideal_min": 2,
                "dte_ideal_max": 10,
            }
        )
    elif earnings_phase == "post_event_fresh" and family == "iron_condor":
        confirmation_min = 0.70 + (0.05 if friday_after_hours_event else 0.0)
        thresholds.update(
            {
                "active": True,
                "lead_signal": "post_event_confirmation_signal",
                "lead_signal_min": round(min(confirmation_min, 0.99), 2),
                "lead_signal_subsignal_min": 3,
                "pricing_signal_min": 0.60,
                "pricing_signal_subsignal_min": 1,
                "dte_min": 3,
                "dte_max": 15,
                "dte_ideal_min": 3,
                "dte_ideal_max": 12,
                "neutral_regime_signal_min": 0.60,
                "residual_iv_richness_min": 0.60,
            }
        )
    return thresholds


def evaluate_earnings_signal_gate(
    *,
    candidate: Mapping[str, Any],
    family: str,
    earnings_phase: str,
    days_to_expiration: int | None,
    cycle: Mapping[str, Any] | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    bundle = build_earnings_signal_bundle(
        candidate,
        family=family,
        cycle=cycle,
        evidence=evidence,
    )
    thresholds = earnings_signal_thresholds(
        family=family,
        earnings_phase=earnings_phase,
        friday_after_hours_event=bool(bundle.get("friday_after_hours_event")),
    )
    blockers: list[str] = []
    if not thresholds["active"]:
        return {
            "active": False,
            "eligible": True,
            "blockers": blockers,
            "bundle": bundle,
            "thresholds": thresholds,
            "coverage_count": sum(
                1
                for field in EARNINGS_SIGNAL_FIELDS
                if bundle["signals"][field]["score"] is not None
            ),
        }

    lead_signal = str(thresholds.get("lead_signal") or "")
    lead_entry = (
        bundle["signals"].get(lead_signal, {})
        if isinstance(bundle.get("signals"), Mapping)
        else {}
    )
    lead_score = _as_float(lead_entry.get("score"))
    lead_subsignal_count = _as_int(lead_entry.get("subsignal_count"))
    pricing_entry = bundle["signals"].get("pricing_signal", {})
    pricing_score = _as_float(pricing_entry.get("score"))
    pricing_subsignal_count = _as_int(pricing_entry.get("subsignal_count"))

    dte_min = _as_int(thresholds.get("dte_min"))
    dte_max = _as_int(thresholds.get("dte_max"))
    if days_to_expiration is not None and (
        (dte_min is not None and days_to_expiration < dte_min)
        or (dte_max is not None and days_to_expiration > dte_max)
    ):
        blockers.append("earnings_dte_out_of_range")

    lead_signal_min = _as_float(thresholds.get("lead_signal_min"))
    if lead_signal_min is not None:
        if lead_score is None:
            blockers.append(f"missing_{lead_signal}")
        elif lead_score < lead_signal_min:
            blockers.append(f"{lead_signal}_below_threshold")

    lead_signal_subsignal_min = _as_int(thresholds.get("lead_signal_subsignal_min"))
    if lead_signal_subsignal_min is not None:
        if lead_subsignal_count is None:
            blockers.append(f"missing_{lead_signal}_subsignal_count")
        elif lead_subsignal_count < lead_signal_subsignal_min:
            blockers.append(f"{lead_signal}_subsignal_count_too_low")

    pricing_signal_min = _as_float(thresholds.get("pricing_signal_min"))
    if pricing_signal_min is not None:
        if pricing_score is None:
            blockers.append("missing_pricing_signal")
        elif pricing_score < pricing_signal_min:
            blockers.append("pricing_signal_below_threshold")

    pricing_signal_subsignal_min = _as_int(
        thresholds.get("pricing_signal_subsignal_min")
    )
    if pricing_signal_subsignal_min is not None:
        if pricing_subsignal_count is None:
            blockers.append("missing_pricing_signal_subsignal_count")
        elif pricing_subsignal_count < pricing_signal_subsignal_min:
            blockers.append("pricing_signal_subsignal_count_too_low")

    if thresholds.get("options_bias_alignment_required"):
        options_bias_alignment = bundle.get("options_bias_alignment")
        if options_bias_alignment is None:
            blockers.append("missing_options_bias_alignment")
        elif not bool(options_bias_alignment):
            blockers.append("options_bias_alignment_not_confirmed")

    debit_width_ratio_max = _as_float(thresholds.get("debit_width_ratio_max"))
    if debit_width_ratio_max is not None:
        debit_width_ratio = _as_float(bundle.get("debit_width_ratio"))
        if debit_width_ratio is None:
            blockers.append("missing_debit_width_ratio")
        elif debit_width_ratio > debit_width_ratio_max:
            blockers.append("debit_width_ratio_too_high")

    modeled_move_vs_implied_move_min = _as_float(
        thresholds.get("modeled_move_vs_implied_move_min")
    )
    if modeled_move_vs_implied_move_min is not None:
        metric = _as_float(bundle.get("modeled_move_vs_implied_move"))
        if metric is None:
            blockers.append("missing_modeled_move_vs_implied_move")
        elif metric < modeled_move_vs_implied_move_min:
            blockers.append("modeled_move_vs_implied_move_too_low")

    modeled_move_vs_break_even_move_min = _as_float(
        thresholds.get("modeled_move_vs_break_even_move_min")
    )
    if modeled_move_vs_break_even_move_min is not None:
        metric = _as_float(bundle.get("modeled_move_vs_break_even_move"))
        if metric is None:
            blockers.append("missing_modeled_move_vs_break_even_move")
        elif metric < modeled_move_vs_break_even_move_min:
            blockers.append("modeled_move_vs_break_even_move_too_low")

    neutral_regime_signal_min = _as_float(
        thresholds.get("neutral_regime_signal_min")
    )
    if neutral_regime_signal_min is not None:
        metric = _as_float(bundle.get("neutral_regime_signal"))
        if metric is None:
            blockers.append("missing_neutral_regime_signal")
        elif metric < neutral_regime_signal_min:
            blockers.append("neutral_regime_signal_too_low")

    residual_iv_richness_min = _as_float(
        thresholds.get("residual_iv_richness_min")
    )
    if residual_iv_richness_min is not None:
        metric = _as_float(bundle.get("residual_iv_richness"))
        if metric is None:
            blockers.append("missing_residual_iv_richness")
        elif metric < residual_iv_richness_min:
            blockers.append("residual_iv_richness_too_low")

    return {
        "active": True,
        "eligible": not blockers,
        "blockers": blockers,
        "bundle": bundle,
        "thresholds": thresholds,
        "coverage_count": sum(
            1
            for field in EARNINGS_SIGNAL_FIELDS
            if bundle["signals"][field]["score"] is not None
        ),
    }


def earnings_phase_policy_preference(
    *,
    family: str,
    earnings_phase: str,
) -> str:
    if earnings_phase in {"clean", "post_event_settled"}:
        return "normal"
    if earnings_phase == "pre_event_runup":
        if family in DIRECTIONAL_DEBIT_FAMILIES:
            return "preferred"
        if family in LONG_VOL_FAMILIES:
            return "allowed"
        return "blocked"
    if earnings_phase == "through_event":
        if family in DIRECTIONAL_DEBIT_FAMILIES or family in LONG_VOL_FAMILIES:
            return "preferred"
        return "blocked"
    if earnings_phase == "post_event_fresh":
        if family in DIRECTIONAL_DEBIT_FAMILIES:
            return "preferred"
        if family in SHORT_PREMIUM_FAMILIES:
            return "allowed"
        return "blocked"
    return "normal"


def earnings_phase_policy_blockers(
    *,
    family: str,
    earnings_phase: str,
    product_class_value: str,
    horizon_band_value: str,
    earnings_timing_confidence: str,
) -> list[str]:
    blockers: list[str] = []
    if earnings_phase not in ACTIVE_EARNINGS_PHASES:
        return blockers
    phase_preference = earnings_phase_policy_preference(
        family=family,
        earnings_phase=earnings_phase,
    )
    if horizon_band_value not in SUPPORTED_EARNINGS_HORIZONS:
        blockers.append("earnings_horizon_band_blocked")
    if earnings_phase == "through_event" and horizon_band_value == "same_day":
        blockers.append("same_day_earnings_event_blocked")
    if (
        earnings_phase in {"through_event", "post_event_fresh"}
        and earnings_timing_confidence not in {"medium", "high"}
    ):
        blockers.append("earnings_timing_confidence_too_low")
    if earnings_phase == "pre_event_runup":
        if family == "iron_condor":
            blockers.append("pre_event_iron_condor_blocked")
        elif (
            product_class_value == "single_name_equity"
            and family in SHORT_PREMIUM_FAMILIES
        ):
            blockers.append("pre_event_single_name_short_premium_blocked")
    elif earnings_phase == "through_event":
        if family == "iron_condor":
            blockers.append("through_event_iron_condor_blocked")
        elif (
            product_class_value == "single_name_equity"
            and family in SHORT_PREMIUM_FAMILIES
        ):
            blockers.append("through_event_single_name_short_premium_blocked")
    elif earnings_phase == "post_event_fresh":
        if family == "iron_condor" and horizon_band_value not in {"near_term", "post_event"}:
            blockers.append("post_event_iron_condor_horizon_blocked")
    if phase_preference == "blocked" and not blockers:
        blockers.append("earnings_phase_family_blocked")
    return blockers


def style_score_thresholds(style_profile: str) -> dict[str, float]:
    promotion_floor = 70.0
    monitor_floor = 55.0
    if style_profile == "reactive":
        promotion_floor = 78.0
        monitor_floor = 62.0
    elif style_profile == "tactical":
        promotion_floor = 72.0
        monitor_floor = 60.0
    elif style_profile == "carry":
        promotion_floor = 68.0
        monitor_floor = 58.0
    return {
        "promotion_floor": promotion_floor,
        "monitor_floor": monitor_floor,
    }


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


def _carry_buffer_ratio(candidate: Mapping[str, Any] | None) -> float | None:
    if not isinstance(candidate, Mapping):
        return None
    short_vs_expected_move = _as_float(candidate.get("short_vs_expected_move"))
    expected_move = _as_float(candidate.get("expected_move"))
    if short_vs_expected_move is None or expected_move in (None, 0.0):
        return None
    return _clamp(short_vs_expected_move / expected_move, 0.0, 1.5)


def profile_specific_blockers(
    *,
    candidate: Mapping[str, Any],
    style_profile: str,
) -> list[str]:
    blockers: list[str] = []
    if style_profile == "reactive" and candidate_requires_favorable_setup(candidate):
        if str(candidate.get("setup_status") or "").lower() != "favorable":
            blockers.append("reactive_setup_not_favorable")
        if not candidate_has_intraday_setup_context(candidate):
            blockers.append("missing_intraday_context")
    return blockers


def profile_specific_score_components(
    *,
    candidate: Mapping[str, Any],
    style_profile: str,
    cycle: Mapping[str, Any] | None,
) -> tuple[dict[str, float], dict[str, Any]]:
    components: dict[str, float] = {}
    evidence: dict[str, Any] = {}
    cycle_payload = {} if not isinstance(cycle, Mapping) else dict(cycle)

    buffer_ratio = _carry_buffer_ratio(candidate)
    if style_profile == "carry" and buffer_ratio is not None:
        buffer_delta = _clamp((buffer_ratio - 0.15) * 30.0, 0.0, 6.0)
        components["carry_buffer_delta"] = round(buffer_delta, 3)
        evidence["buffer_ratio"] = round(buffer_ratio, 4)

    if style_profile == "tactical":
        earnings_phase = candidate_earnings_phase(candidate)
        setup_status = str(candidate.get("setup_status") or "").strip().lower()
        if setup_status == "favorable":
            components["tactical_setup_delta"] = 2.5
        elif setup_status == "neutral":
            components["tactical_setup_penalty"] = 3.0
        elif setup_status not in {"", "unknown"}:
            components["tactical_setup_penalty"] = 8.0
        evidence["setup_status"] = setup_status or "unknown"

        short_delta = abs(_as_float(candidate.get("short_delta")) or 0.0)
        if short_delta > 0.0:
            delta_fit = _clamp(1.5 - abs(short_delta - 0.13) * 60.0, 0.0, 1.5)
            if delta_fit > 0.0:
                components["tactical_delta_fit_delta"] = round(delta_fit, 3)
            evidence["short_delta"] = round(short_delta, 4)

        expected_move = _as_float(candidate.get("expected_move"))
        short_vs_expected_move = _as_float(candidate.get("short_vs_expected_move"))
        if expected_move not in (None, 0.0) and short_vs_expected_move is not None:
            tactical_buffer_ratio = _clamp(
                short_vs_expected_move / expected_move,
                0.0,
                1.5,
            )
            buffer_delta = _clamp((tactical_buffer_ratio - 0.6) * 12.0, 0.0, 2.0)
            if buffer_delta > 0.0:
                components["tactical_buffer_delta"] = round(buffer_delta, 3)
            evidence["buffer_ratio"] = round(tactical_buffer_ratio, 4)

        if str(candidate.get("calendar_status") or "").strip().lower() == "penalized":
            days_to_event = int(
                _as_float(candidate.get("calendar_days_to_nearest_event")) or 0
            )
            if days_to_event <= 1:
                components["tactical_event_proximity_penalty"] = 4.0
            elif days_to_event == 2:
                components["tactical_event_proximity_penalty"] = 2.0
            else:
                components["tactical_event_proximity_penalty"] = 1.0
            evidence["days_to_nearest_event"] = days_to_event
        evidence["earnings_phase"] = earnings_phase

    if style_profile == "reactive":
        stale_minutes = _minutes_between(
            candidate.get("recovered_from_run_generated_at"),
            cycle_payload.get("generated_at"),
        )
        if stale_minutes is not None:
            evidence["stale_minutes"] = stale_minutes
            if stale_minutes > 20.0:
                components["reactive_staleness_penalty"] = round(
                    _clamp((stale_minutes - 20.0) * 0.25, 0.0, 25.0),
                    3,
                )
        intraday_score = _as_float(candidate.get("setup_intraday_score"))
        if intraday_score is not None:
            intraday_delta = _clamp((intraday_score - 55.0) * 0.12, -8.0, 6.0)
            components["reactive_intraday_delta"] = round(intraday_delta, 3)
            evidence["intraday_score"] = round(intraday_score, 3)
        if candidate.get("selection_source") == "session_history_recovery":
            components["reactive_recovery_penalty"] = 8.0
            evidence["selection_source"] = str(candidate.get("selection_source"))
    return components, evidence


def calendar_blocks_strategy(
    *,
    calendar_status: str,
    style_profile: str,
) -> bool:
    normalized = calendar_status.strip().lower()
    if normalized in {"", "clean"}:
        return False
    if normalized in {"blocked", "unknown"}:
        return True
    if normalized == "penalized":
        return style_profile == "reactive"
    return True


def calendar_penalty(
    *,
    calendar_status: str,
    style_profile: str,
) -> float:
    normalized = calendar_status.strip().lower()
    if normalized in {"", "clean"}:
        return 0.0
    if normalized == "penalized":
        if style_profile == "reactive":
            return 6.0
        if style_profile == "tactical":
            return 2.0
        return 3.0
    if normalized == "unknown":
        return 8.0
    return 12.0


def calibration_dimensions(
    style_profile: str,
) -> tuple[tuple[str, str | None, float], ...]:
    weights = {
        "classification": 1.0,
        "strategy": 0.8,
        "symbol": 0.5,
        "setup_status": 0.7,
    }
    if style_profile == "tactical":
        weights["classification"] = 0.0
        weights["strategy"] = 0.9
        weights["symbol"] = 0.6
        weights["setup_status"] = 0.8
    return (
        ("classification", None, weights["classification"]),
        ("strategy", None, weights["strategy"]),
        ("symbol", None, weights["symbol"]),
        ("setup_status", None, weights["setup_status"]),
    )


def dimension_adjustment(
    *,
    dimension_lookup: Mapping[str, Mapping[str, Mapping[str, Any]]] | None,
    dimension: str,
    group_value: str | None,
    weight: float,
) -> tuple[float, dict[str, Any] | None]:
    if group_value is None or not isinstance(dimension_lookup, Mapping):
        return 0.0, None
    row = (
        dimension_lookup.get(dimension, {}).get(group_value)
        if isinstance(dimension_lookup.get(dimension), Mapping)
        else None
    )
    if row is None:
        return 0.0, None
    average_estimated_pnl = _as_float(row.get("average_estimated_pnl")) or 0.0
    return _clamp(average_estimated_pnl, -5.0, 5.0) * weight, {
        "dimension": dimension,
        "group_value": group_value,
        "average_estimated_pnl": average_estimated_pnl,
        "count": row.get("count"),
        "legacy_promotable_baseline_count": row.get("legacy_promotable_baseline_count"),
        "legacy_monitor_count": row.get("legacy_monitor_count"),
    }


def product_policy_blockers(
    *,
    family: str,
    style_profile: str,
    product_class_value: str,
    horizon_band_value: str,
) -> list[str]:
    blockers: list[str] = []
    if family == "iron_condor" and product_class_value not in {
        "cash_settled_index",
        "top_tier_etf",
    }:
        blockers.append("product_policy_condor_blocked")
    if (
        style_profile == "reactive"
        and family in {"put_credit_spread", "call_credit_spread", "iron_condor"}
        and product_class_value not in {"cash_settled_index", "top_tier_etf"}
    ):
        blockers.append("reactive_short_premium_product_blocked")
    if family == "iron_condor" and horizon_band_value == "same_day":
        blockers.append("same_day_iron_condor_blocked")
    return blockers


def build_candidate_opportunity_score(
    candidate: Mapping[str, Any],
    *,
    cycle: Mapping[str, Any] | None = None,
    style_profile: str | None = None,
    policy_state: str | None = None,
    blockers: Sequence[str] | None = None,
    legacy_selection_state: str | None = None,
    dimension_lookup: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    symbol = _as_text(candidate.get("underlying_symbol")) or ""
    strategy = _as_text(candidate.get("strategy")) or "unknown"
    family = strategy_family(strategy)
    days_to_expiration = int(_as_float(candidate.get("days_to_expiration")) or 0)
    resolved_style = style_profile or resolve_style_profile(
        _as_text(candidate.get("profile")),
        days_to_expiration=days_to_expiration,
    )
    product_class_value = product_class(symbol)
    horizon_band_value, _, _, _ = horizon_band(days_to_expiration)
    earnings_phase = candidate_earnings_phase(candidate)
    event_timing_rule = candidate_event_timing_rule(candidate)
    phase_policy_preference = earnings_phase_policy_preference(
        family=family,
        earnings_phase=earnings_phase,
    )
    signal_gate = evaluate_earnings_signal_gate(
        candidate=candidate,
        family=family,
        earnings_phase=earnings_phase,
        days_to_expiration=days_to_expiration,
        cycle=cycle,
    )

    resolved_blockers = list(blockers or [])
    if not resolved_blockers:
        resolved_blockers.extend(
            product_policy_blockers(
                family=family,
                style_profile=resolved_style,
                product_class_value=product_class_value,
                horizon_band_value=horizon_band_value,
            )
        )
        resolved_blockers.extend(
            earnings_phase_policy_blockers(
                family=family,
                earnings_phase=earnings_phase,
                product_class_value=product_class_value,
                horizon_band_value=horizon_band_value,
                earnings_timing_confidence=str(
                    candidate.get("earnings_timing_confidence") or "unknown"
                ).strip().lower(),
            )
        )
        resolved_blockers.extend(list(signal_gate["blockers"]))
        resolved_blockers.extend(
            profile_specific_blockers(
                candidate=candidate,
                style_profile=resolved_style,
            )
        )
        if str(candidate.get("data_status") or "") != "clean":
            resolved_blockers.append("data_quality_not_clean")
        calendar_status = str(candidate.get("calendar_status") or "")
        if calendar_blocks_strategy(
            calendar_status=calendar_status,
            style_profile=resolved_style,
        ):
            resolved_blockers.append("calendar_risk_present")

    resolved_policy_state = (
        str(policy_state or "").strip().lower()
        if _as_text(policy_state) is not None
        else (
            "blocked"
            if resolved_blockers
            else ("preferred" if phase_policy_preference == "preferred" else "allowed")
        )
    )

    discovery_score = round(_as_float(candidate.get("quality_score")) or 0.0, 1)
    calibration_breakdown: list[dict[str, Any]] = []
    calibration_delta = 0.0
    for dimension, _, weight in calibration_dimensions(resolved_style):
        if weight <= 0.0:
            continue
        if dimension == "classification":
            group_value = legacy_selection_state
        elif dimension == "strategy":
            group_value = strategy
        elif dimension == "symbol":
            group_value = symbol or None
        else:
            group_value = _as_text(candidate.get("setup_status"))
        delta, evidence = dimension_adjustment(
            dimension_lookup=dimension_lookup,
            dimension=dimension,
            group_value=group_value,
            weight=weight,
        )
        calibration_delta += delta
        if evidence is not None:
            evidence["score_delta"] = round(delta, 3)
            calibration_breakdown.append(evidence)

    setup_delta = ((_as_float(candidate.get("setup_score")) or 50.0) - 50.0) * 0.15
    fill_ratio_delta = ((_as_float(candidate.get("fill_ratio")) or 0.8) - 0.8) * 25.0
    profile_components, profile_evidence = profile_specific_score_components(
        candidate=candidate,
        style_profile=resolved_style,
        cycle=cycle,
    )
    component_boost = sum(
        value
        for key, value in profile_components.items()
        if not key.endswith("_penalty")
    )
    component_penalty = sum(
        value for key, value in profile_components.items() if key.endswith("_penalty")
    )
    penalty = 0.0
    if str(candidate.get("data_status") or "") != "clean":
        penalty += 8.0
    penalty += calendar_penalty(
        calendar_status=str(candidate.get("calendar_status") or ""),
        style_profile=resolved_style,
    )
    if resolved_policy_state == "blocked":
        penalty += 20.0

    raw_promotion_score = (
        discovery_score
        + setup_delta
        + fill_ratio_delta
        + calibration_delta
        + component_boost
        - penalty
        - component_penalty
    )
    promotion_score = round(_clamp(raw_promotion_score, 0.0, 100.0), 1)
    execution_score = promotion_score
    thresholds = style_score_thresholds(resolved_style)
    promotion_floor = thresholds["promotion_floor"]
    monitor_floor = thresholds["monitor_floor"]

    if resolved_policy_state == "blocked":
        state = "blocked"
        state_reason = "Blocked by product or event policy."
    elif promotion_score >= promotion_floor:
        state = "promotable"
        state_reason = "Meets provisional promotion floor."
    elif promotion_score >= monitor_floor:
        state = "monitor"
        state_reason = "Retained but below promotion floor."
    else:
        state = "discarded"
        state_reason = "Below provisional retention floor."

    confidence = round(
        _clamp(
            (execution_score - monitor_floor) / max(100.0 - monitor_floor, 1.0),
            0.0,
            1.0,
        ),
        4,
    )
    return {
        "style_profile": resolved_style,
        "strategy_family": family,
        "product_class": product_class_value,
        "horizon_band": horizon_band_value,
        "earnings_phase": earnings_phase,
        "phase_policy_preference": phase_policy_preference,
        "event_state": candidate_event_state(candidate),
        "event_timing_rule": event_timing_rule,
        "signal_bundle": signal_gate["bundle"],
        "signal_thresholds": signal_gate["thresholds"],
        "signal_gate": {
            "active": signal_gate["active"],
            "eligible": signal_gate["eligible"],
            "coverage_count": signal_gate["coverage_count"],
            "blockers": list(signal_gate["blockers"]),
        },
        "policy_state": resolved_policy_state,
        "blockers": resolved_blockers,
        "discovery_score": discovery_score,
        "promotion_score": promotion_score,
        "execution_score": execution_score,
        "confidence": confidence,
        "state": state,
        "state_reason": state_reason,
        "promotion_floor": promotion_floor,
        "monitor_floor": monitor_floor,
        "setup_score_delta": round(setup_delta, 3),
        "fill_ratio_delta": round(fill_ratio_delta, 3),
        "calibration_delta": round(calibration_delta, 3),
        "calibration_breakdown": calibration_breakdown,
        "profile_score_components": profile_components,
        "profile_score_evidence": profile_evidence,
        "penalty": round(penalty, 3),
    }


def score_candidate_opportunity(
    candidate: Mapping[str, Any],
    *,
    cycle: Mapping[str, Any] | None = None,
    style_profile: str | None = None,
    policy_state: str | None = None,
    blockers: Sequence[str] | None = None,
    legacy_selection_state: str | None = None,
    dimension_lookup: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    return build_candidate_opportunity_score(
        candidate,
        cycle=cycle,
        style_profile=style_profile,
        policy_state=policy_state,
        blockers=blockers,
        legacy_selection_state=legacy_selection_state,
        dimension_lookup=dimension_lookup,
    )


__all__ = [
    "build_candidate_opportunity_score",
    "build_earnings_signal_bundle",
    "candidate_earnings_phase",
    "candidate_event_state",
    "candidate_event_timing_rule",
    "earnings_signal_thresholds",
    "evaluate_earnings_signal_gate",
    "earnings_phase_policy_blockers",
    "earnings_phase_policy_preference",
    "product_class",
    "resolve_style_profile",
    "score_candidate_opportunity",
    "strategy_family",
    "style_score_thresholds",
]

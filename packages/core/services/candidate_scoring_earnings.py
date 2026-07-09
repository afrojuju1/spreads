from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from core.services.candidate_scoring_taxonomy import (
    DIRECTIONAL_DEBIT_FAMILIES,
    LONG_VOL_FAMILIES,
    POST_EVENT_SHORT_PREMIUM_SIGNAL_FAMILIES,
    SHORT_PREMIUM_FAMILIES,
)
from core.services.earnings_signal_features import (
    EARNINGS_SIGNAL_FIELDS,
    build_earnings_signal_bundle,
)
from core.value_coercion import coerce_float as _as_float, coerce_int as _as_int

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
SUPPORTED_EARNINGS_HORIZONS = {"next_daily", "near_term", "post_event"}
EARNINGS_SIGNAL_THRESHOLD_DEFAULTS: dict[str, Any] = {
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


@dataclass(frozen=True)
class EarningsSignalThresholdRule:
    earnings_phase: str
    families: frozenset[str]
    values: Mapping[str, Any]
    friday_confirmation_bonus: bool = False

    def matches(self, *, family: str, earnings_phase: str) -> bool:
        return earnings_phase == self.earnings_phase and family in self.families

    def build_thresholds(self, *, friday_after_hours_event: bool) -> dict[str, Any]:
        thresholds = dict(EARNINGS_SIGNAL_THRESHOLD_DEFAULTS)
        thresholds.update(self.values)
        if self.friday_confirmation_bonus:
            lead_signal_min = _as_float(thresholds.get("lead_signal_min"))
            if lead_signal_min is not None:
                bonus = 0.05 if friday_after_hours_event else 0.0
                thresholds["lead_signal_min"] = round(min(lead_signal_min + bonus, 0.99), 2)
        return thresholds


EARNINGS_SIGNAL_THRESHOLD_RULES = (
    EarningsSignalThresholdRule(
        earnings_phase="pre_event_runup",
        families=frozenset(DIRECTIONAL_DEBIT_FAMILIES),
        values={
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
        },
    ),
    EarningsSignalThresholdRule(
        earnings_phase="pre_event_runup",
        families=frozenset(LONG_VOL_FAMILIES),
        values={
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
        },
    ),
    EarningsSignalThresholdRule(
        earnings_phase="through_event",
        families=frozenset(DIRECTIONAL_DEBIT_FAMILIES),
        values={
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
        },
    ),
    EarningsSignalThresholdRule(
        earnings_phase="through_event",
        families=frozenset({"long_straddle"}),
        values={
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
        },
    ),
    EarningsSignalThresholdRule(
        earnings_phase="through_event",
        families=frozenset({"long_strangle"}),
        values={
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
        },
    ),
    EarningsSignalThresholdRule(
        earnings_phase="post_event_fresh",
        families=frozenset(DIRECTIONAL_DEBIT_FAMILIES),
        friday_confirmation_bonus=True,
        values={
            "active": True,
            "lead_signal": "post_event_confirmation_signal",
            "lead_signal_min": 0.65,
            "lead_signal_subsignal_min": 3,
            "pricing_signal_min": 0.55,
            "pricing_signal_subsignal_min": 1,
            "dte_min": 2,
            "dte_max": 15,
            "dte_ideal_min": 2,
            "dte_ideal_max": 10,
        },
    ),
    EarningsSignalThresholdRule(
        earnings_phase="post_event_fresh",
        families=frozenset(POST_EVENT_SHORT_PREMIUM_SIGNAL_FAMILIES),
        friday_confirmation_bonus=True,
        values={
            "active": True,
            "lead_signal": "post_event_confirmation_signal",
            "lead_signal_min": 0.65,
            "lead_signal_subsignal_min": 3,
            "pricing_signal_min": 0.55,
            "pricing_signal_subsignal_min": 1,
            "dte_min": 2,
            "dte_max": 15,
            "dte_ideal_min": 2,
            "dte_ideal_max": 10,
        },
    ),
    EarningsSignalThresholdRule(
        earnings_phase="post_event_fresh",
        families=frozenset({"iron_condor"}),
        friday_confirmation_bonus=True,
        values={
            "active": True,
            "lead_signal": "post_event_confirmation_signal",
            "lead_signal_min": 0.70,
            "lead_signal_subsignal_min": 3,
            "pricing_signal_min": 0.60,
            "pricing_signal_subsignal_min": 1,
            "dte_min": 3,
            "dte_max": 15,
            "dte_ideal_min": 3,
            "dte_ideal_max": 12,
            "neutral_regime_signal_min": 0.60,
            "residual_iv_richness_min": 0.60,
        },
    ),
)


@dataclass(frozen=True)
class SignalMetricGate:
    threshold_key: str
    bundle_key: str
    missing_blocker: str
    failing_blocker: str
    ceiling: bool = False


SIGNAL_METRIC_GATES = (
    SignalMetricGate(
        threshold_key="debit_width_ratio_max",
        bundle_key="debit_width_ratio",
        missing_blocker="missing_debit_width_ratio",
        failing_blocker="debit_width_ratio_too_high",
        ceiling=True,
    ),
    SignalMetricGate(
        threshold_key="modeled_move_vs_implied_move_min",
        bundle_key="modeled_move_vs_implied_move",
        missing_blocker="missing_modeled_move_vs_implied_move",
        failing_blocker="modeled_move_vs_implied_move_too_low",
    ),
    SignalMetricGate(
        threshold_key="modeled_move_vs_break_even_move_min",
        bundle_key="modeled_move_vs_break_even_move",
        missing_blocker="missing_modeled_move_vs_break_even_move",
        failing_blocker="modeled_move_vs_break_even_move_too_low",
    ),
    SignalMetricGate(
        threshold_key="neutral_regime_signal_min",
        bundle_key="neutral_regime_signal",
        missing_blocker="missing_neutral_regime_signal",
        failing_blocker="neutral_regime_signal_too_low",
    ),
    SignalMetricGate(
        threshold_key="residual_iv_richness_min",
        bundle_key="residual_iv_richness",
        missing_blocker="missing_residual_iv_richness",
        failing_blocker="residual_iv_richness_too_low",
    ),
)


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
    for rule in EARNINGS_SIGNAL_THRESHOLD_RULES:
        if rule.matches(family=family, earnings_phase=earnings_phase):
            return rule.build_thresholds(friday_after_hours_event=friday_after_hours_event)
    return dict(EARNINGS_SIGNAL_THRESHOLD_DEFAULTS)


def _signal_entry(bundle: Mapping[str, Any], signal_name: str) -> Mapping[str, Any]:
    signals = bundle.get("signals")
    if not isinstance(signals, Mapping):
        return {}
    entry = signals.get(signal_name)
    return entry if isinstance(entry, Mapping) else {}


def _append_minimum_gate_blocker(
    blockers: list[str],
    *,
    value: float | int | None,
    minimum: float | int | None,
    missing_blocker: str,
    below_blocker: str,
) -> None:
    if minimum is None:
        return
    if value is None:
        blockers.append(missing_blocker)
    elif value < minimum:
        blockers.append(below_blocker)


def _append_metric_gate_blocker(
    blockers: list[str],
    *,
    bundle: Mapping[str, Any],
    thresholds: Mapping[str, Any],
    gate: SignalMetricGate,
) -> None:
    threshold = _as_float(thresholds.get(gate.threshold_key))
    if threshold is None:
        return
    value = _as_float(bundle.get(gate.bundle_key))
    if value is None:
        blockers.append(gate.missing_blocker)
    elif gate.ceiling and value > threshold:
        blockers.append(gate.failing_blocker)
    elif not gate.ceiling and value < threshold:
        blockers.append(gate.failing_blocker)


def _earnings_signal_coverage_count(bundle: Mapping[str, Any]) -> int:
    signals = bundle.get("signals")
    if not isinstance(signals, Mapping):
        return 0
    return sum(1 for field in EARNINGS_SIGNAL_FIELDS if isinstance(signals.get(field), Mapping) and signals[field].get("score") is not None)


def evaluate_earnings_signal_gate(
    *,
    candidate: Mapping[str, Any],
    family: str,
    earnings_phase: str,
    days_to_expiration: int | None,
) -> dict[str, Any]:
    bundle = build_earnings_signal_bundle(
        candidate,
        family=family,
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
            "coverage_count": _earnings_signal_coverage_count(bundle),
        }

    lead_signal = str(thresholds.get("lead_signal") or "")
    lead_entry = _signal_entry(bundle, lead_signal)
    lead_score = _as_float(lead_entry.get("score"))
    lead_subsignal_count = _as_int(lead_entry.get("subsignal_count"))
    pricing_entry = _signal_entry(bundle, "pricing_signal")
    pricing_score = _as_float(pricing_entry.get("score"))
    pricing_subsignal_count = _as_int(pricing_entry.get("subsignal_count"))

    dte_min = _as_int(thresholds.get("dte_min"))
    dte_max = _as_int(thresholds.get("dte_max"))
    if days_to_expiration is not None and (
        (dte_min is not None and days_to_expiration < dte_min) or (dte_max is not None and days_to_expiration > dte_max)
    ):
        blockers.append("earnings_dte_out_of_range")

    _append_minimum_gate_blocker(
        blockers,
        value=lead_score,
        minimum=_as_float(thresholds.get("lead_signal_min")),
        missing_blocker=f"missing_{lead_signal}",
        below_blocker=f"{lead_signal}_below_threshold",
    )
    _append_minimum_gate_blocker(
        blockers,
        value=lead_subsignal_count,
        minimum=_as_int(thresholds.get("lead_signal_subsignal_min")),
        missing_blocker=f"missing_{lead_signal}_subsignal_count",
        below_blocker=f"{lead_signal}_subsignal_count_too_low",
    )
    _append_minimum_gate_blocker(
        blockers,
        value=pricing_score,
        minimum=_as_float(thresholds.get("pricing_signal_min")),
        missing_blocker="missing_pricing_signal",
        below_blocker="pricing_signal_below_threshold",
    )
    _append_minimum_gate_blocker(
        blockers,
        value=pricing_subsignal_count,
        minimum=_as_int(thresholds.get("pricing_signal_subsignal_min")),
        missing_blocker="missing_pricing_signal_subsignal_count",
        below_blocker="pricing_signal_subsignal_count_too_low",
    )

    if thresholds.get("options_bias_alignment_required"):
        options_bias_alignment = bundle.get("options_bias_alignment")
        if options_bias_alignment is None:
            blockers.append("missing_options_bias_alignment")
        elif not bool(options_bias_alignment):
            blockers.append("options_bias_alignment_not_confirmed")

    for gate in SIGNAL_METRIC_GATES:
        _append_metric_gate_blocker(
            blockers,
            bundle=bundle,
            thresholds=thresholds,
            gate=gate,
        )

    return {
        "active": True,
        "eligible": not blockers,
        "blockers": blockers,
        "bundle": bundle,
        "thresholds": thresholds,
        "coverage_count": _earnings_signal_coverage_count(bundle),
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
    if earnings_phase in {
        "through_event",
        "post_event_fresh",
    } and earnings_timing_confidence not in {"medium", "high"}:
        blockers.append("earnings_timing_confidence_too_low")
    if earnings_phase == "pre_event_runup":
        if family == "iron_condor":
            blockers.append("pre_event_iron_condor_blocked")
        elif product_class_value == "single_name_equity" and family in SHORT_PREMIUM_FAMILIES:
            blockers.append("pre_event_single_name_short_premium_blocked")
    elif earnings_phase == "through_event":
        if family == "iron_condor":
            blockers.append("through_event_iron_condor_blocked")
        elif product_class_value == "single_name_equity" and family in SHORT_PREMIUM_FAMILIES:
            blockers.append("through_event_single_name_short_premium_blocked")
    elif earnings_phase == "post_event_fresh":
        if family == "iron_condor" and horizon_band_value not in {
            "near_term",
            "post_event",
        }:
            blockers.append("post_event_iron_condor_horizon_blocked")
    if phase_preference == "blocked" and not blockers:
        blockers.append("earnings_phase_family_blocked")
    return blockers

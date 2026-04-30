from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from core.common import clamp
from core.services.company_valuation.contracts import (
    CompanyValuationDocument,
    CompanyValuationIdentity,
    CompanyValuationScreenRow,
    QualityBreakdown,
    ValuationSummary,
)
from core.services.company_valuation.features import (
    FEATURE_VERSION as COMPANY_VALUATION_FEATURE_VERSION,
    CompanyValuationFeatureResult,
    compute_company_valuation_features,
)
from core.services.company_valuation.ids import (
    build_company_valuation_snapshot_id,
    build_screening_row_id,
)
from core.services.company_valuation.point_in_time import resolve_company_valuation_point_in_time
from core.services.company_valuation.templates import resolve_company_valuation_template
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_datetime, render_value

EVALUATION_VERSION = "v1"

TEMPLATE_MULTIPLES: dict[str, dict[str, float]] = {
    "general_operating": {"ev_ebit": 15.0, "ev_fcf": 20.0, "pe": 20.0, "ps": 2.8, "pb": 2.4},
    "software_asset_light": {"ev_ebit": 20.0, "ev_fcf": 26.0, "pe": 30.0, "ps": 6.5},
    "retail_consumer": {"ev_ebit": 13.0, "ev_fcf": 17.0, "pe": 18.0, "ps": 1.4},
    "industrial_manufacturing": {"ev_ebit": 14.0, "ev_fcf": 18.0, "pb": 2.2, "ev_ebitda": 12.0},
    "energy_asset_heavy": {"ev_ebit": 8.5, "ev_fcf": 11.0, "pb": 1.7, "ev_ebitda": 7.0},
}


@dataclass(frozen=True)
class CompanyValuationRecomputeResult:
    feature_snapshot: dict[str, Any]
    company_valuation_snapshot: dict[str, Any]
    screening_row: dict[str, Any]
    document: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalized_as_of(value: str | datetime | None) -> datetime:
    parsed = parse_datetime(value) if isinstance(value, str) else value
    if parsed is None:
        return datetime.now(UTC)
    return parsed.astimezone(UTC)


def _score_linear(value: float | None, *, low: float, high: float) -> float:
    if value is None:
        return 50.0
    if high <= low:
        return 50.0
    return clamp((value - low) / (high - low), 0.0, 1.0) * 100.0


def _score_inverse(value: float | None, *, good: float, bad: float) -> float:
    if value is None:
        return 50.0
    if bad <= good:
        return 50.0
    return (1.0 - clamp((value - good) / (bad - good), 0.0, 1.0)) * 100.0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 50.0


def _quality_scores(
    *,
    features: dict[str, Any],
    ownership_score: float,
    required_feature_coverage: float,
) -> dict[str, float]:
    growth_score = _mean(
        [
            _score_linear(_safe_float(features.get("revenue_ttm_growth")), low=-0.05, high=0.20),
            _score_linear(_safe_float(features.get("deferred_revenue_growth")), low=-0.02, high=0.25),
        ]
    )
    profitability_score = _mean(
        [
            _score_linear(_safe_float(features.get("gross_margin_ttm")), low=0.10, high=0.75),
            _score_linear(_safe_float(features.get("operating_margin_ttm")), low=-0.05, high=0.30),
            _score_linear(_safe_float(features.get("net_margin_ttm")), low=-0.05, high=0.22),
        ]
    )
    cash_flow_score = _mean(
        [
            _score_linear(_safe_float(features.get("free_cash_flow_margin_ttm")), low=-0.04, high=0.20),
            _score_inverse(_safe_float(features.get("capex_intensity")), good=0.00, bad=0.18),
        ]
    )
    capital_efficiency_score = _mean(
        [
            _score_linear(_safe_float(features.get("roic_ttm")), low=0.00, high=0.25),
            _score_linear(_safe_float(features.get("asset_turnover")), low=0.20, high=1.80),
            _score_linear(_safe_float(features.get("inventory_turns")), low=2.0, high=10.0),
        ]
    )
    balance_sheet_score = _mean(
        [
            _score_inverse(_safe_float(features.get("net_leverage")), good=0.0, bad=4.0),
            _score_linear(_safe_float(features.get("current_ratio")), low=1.0, high=2.5),
            _score_inverse(_safe_float(features.get("debt_to_equity")), good=0.0, bad=2.0),
        ]
    )
    shareholder_score = _mean(
        [
            _score_inverse(_safe_float(features.get("diluted_share_growth_ttm")), good=-0.03, bad=0.08),
            _score_inverse(_safe_float(features.get("sbc_as_pct_revenue")), good=0.0, bad=0.18),
        ]
    )
    reporting_quality_score = clamp(required_feature_coverage, 0.0, 1.0) * 100.0
    return {
        "growth_score": growth_score,
        "profitability_score": profitability_score,
        "cash_flow_score": cash_flow_score,
        "capital_efficiency_score": capital_efficiency_score,
        "balance_sheet_score": balance_sheet_score,
        "shareholder_score": shareholder_score,
        "ownership_score": clamp(ownership_score / 100.0, 0.0, 1.0) * 100.0,
        "reporting_quality_score": reporting_quality_score,
    }


def _build_quality_breakdown(
    *,
    feature_result: CompanyValuationFeatureResult,
    issuer_row: dict[str, Any],
    config_root: str | None,
) -> QualityBreakdown:
    template = resolve_company_valuation_template(str(issuer_row["template_id"]), config_root)
    sub_scores = _quality_scores(
        features=feature_result.financial_features,
        ownership_score=feature_result.ownership_signal.score,
        required_feature_coverage=feature_result.required_feature_coverage,
    )
    weighted_total = 0.0
    factor_contributions: dict[str, float] = {}
    total_weight = 0
    for factor, weight in template.quality_weight_map.items():
        total_weight += weight
        score = sub_scores.get(factor, 50.0)
        contribution = (score * weight) / 100.0
        factor_contributions[factor] = round(contribution, 2)
        weighted_total += contribution
    total_score = weighted_total if total_weight == 100 else (weighted_total / max(total_weight, 1)) * 100.0

    reason_codes: list[str] = list(feature_result.ownership_signal.reason_codes)
    revenue_growth = _safe_float(feature_result.financial_features.get("revenue_ttm_growth"))
    if revenue_growth is not None and revenue_growth >= 0.15:
        reason_codes.append("revenue_growth_strong_positive")
    if revenue_growth is not None and revenue_growth <= -0.05:
        reason_codes.append("revenue_growth_negative_caution")
    if (_safe_float(feature_result.financial_features.get("free_cash_flow_ttm")) or 0.0) > 0.0:
        reason_codes.append("free_cash_flow_positive")
    if (_safe_float(feature_result.financial_features.get("diluted_share_growth_ttm")) or 0.0) >= 0.05:
        reason_codes.append("dilution_negative")
    if (_safe_float(feature_result.financial_features.get("net_leverage")) or 0.0) >= 3.0:
        reason_codes.append("leverage_above_threshold")
    if feature_result.required_feature_coverage < 0.8:
        reason_codes.append("missing_core_facts")
    if bool(issuer_row.get("limited_coverage_flag")):
        reason_codes.append("limited_coverage_template")

    confidence = 0.45 + (feature_result.required_feature_coverage * 0.35) + (
        feature_result.ownership_signal.confidence * 0.20
    )
    if bool(issuer_row.get("limited_coverage_flag")):
        confidence -= 0.15
    return QualityBreakdown(
        total_score=round(clamp(total_score / 100.0, 0.0, 1.0) * 100.0, 2),
        sub_scores={key: round(value, 2) for key, value in sub_scores.items()},
        factor_contributions=factor_contributions,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
        confidence=round(clamp(confidence, 0.2, 0.95), 4),
    )


def _risk_free_rate(curve_snapshot: dict[str, Any] | None) -> tuple[float, bool]:
    points = dict((curve_snapshot or {}).get("curve_points_json") or {})
    if not points:
        points = dict((curve_snapshot or {}).get("curve_points") or {})
    for key in ("10y", "7y", "5y", "3y", "2y"):
        value = _safe_float(points.get(key))
        if value is not None and value > 0.0:
            return (value, True)
    return (0.045, False)


def _template_float(template: Any, section: str, key: str, default: float) -> float:
    payload = getattr(template, section, {}) or {}
    try:
        return float(payload.get(key) or default)
    except (TypeError, ValueError):
        return float(default)


def _template_multiple_anchors(template: Any) -> dict[str, float]:
    configured = (template.valuation_model_mix or {}).get("default_multiple_anchors")
    if isinstance(configured, dict):
        anchors: dict[str, float] = {}
        for key, value in configured.items():
            numeric = _safe_float(value)
            if numeric is None or numeric <= 0.0:
                continue
            anchors[str(key)] = float(numeric)
        if anchors:
            return anchors
    return TEMPLATE_MULTIPLES.get(
        str(template.template_id),
        TEMPLATE_MULTIPLES["general_operating"],
    )


def _quality_premium_factor(
    *,
    quality_score: float,
    template: Any,
    features: dict[str, Any],
) -> float:
    floor_score = _template_float(
        template,
        "valuation_model_mix",
        "quality_premium_floor_score",
        60.0,
    )
    full_score = _template_float(
        template,
        "valuation_model_mix",
        "quality_premium_full_score",
        80.0,
    )
    if full_score <= floor_score:
        return 0.0
    factor = clamp((quality_score - floor_score) / (full_score - floor_score), 0.0, 1.0)
    free_cash_flow_ttm = _safe_float(features.get("free_cash_flow_ttm"))
    revenue_growth = _safe_float(features.get("revenue_ttm_growth")) or 0.0
    net_leverage = _safe_float(features.get("net_leverage"))
    if free_cash_flow_ttm in (None, 0.0) or free_cash_flow_ttm < 0.0:
        factor *= 0.35
    if revenue_growth < 0.0:
        factor *= 0.75
    if net_leverage is not None and net_leverage >= 3.0:
        factor *= 0.65
    return round(clamp(factor, 0.0, 1.0), 4)


def _starting_fcf(features: dict[str, Any], template: Any) -> tuple[float | None, bool]:
    free_cash_flow_ttm = _safe_float(features.get("free_cash_flow_ttm"))
    if free_cash_flow_ttm is not None and free_cash_flow_ttm > 0.0:
        return (free_cash_flow_ttm, False)
    revenue_ttm = _safe_float(features.get("revenue_ttm"))
    operating_margin_ttm = _safe_float(features.get("operating_margin_ttm")) or 0.0
    free_cash_flow_margin_ttm = _safe_float(features.get("free_cash_flow_margin_ttm"))
    normalized_from_ebit_ratio = _template_float(
        template,
        "valuation_model_mix",
        "normalized_fcf_from_ebit_ratio",
        0.65,
    )
    normalized_margin = max(
        free_cash_flow_margin_ttm or (operating_margin_ttm * normalized_from_ebit_ratio),
        0.0,
    )
    if revenue_ttm is None or normalized_margin <= 0.0:
        return (None, True)
    return (revenue_ttm * normalized_margin, True)


def _valuation_context(
    *,
    features: dict[str, Any],
    template: Any,
    required_feature_coverage: float,
    quality_score: float,
    limited_coverage_flag: bool,
) -> tuple[list[str], float]:
    reason_codes: list[str] = []
    confidence_penalty = 0.0
    free_cash_flow_ttm = _safe_float(features.get("free_cash_flow_ttm"))
    net_leverage = _safe_float(features.get("net_leverage")) or 0.0
    revenue_growth = _safe_float(features.get("revenue_ttm_growth")) or 0.0
    operating_margin = _safe_float(features.get("operating_margin_ttm"))
    if free_cash_flow_ttm is not None and free_cash_flow_ttm <= 0.0:
        reason_codes.append("valuation_negative_fcf_low_confidence")
        confidence_penalty += _template_float(
            template,
            "confidence_rules",
            "negative_fcf_penalty",
            0.18,
        )
    if net_leverage >= _template_float(
        template,
        "confidence_rules",
        "high_leverage_threshold",
        3.0,
    ):
        reason_codes.append("valuation_high_leverage_low_confidence")
        confidence_penalty += _template_float(
            template,
            "confidence_rules",
            "high_leverage_penalty",
            0.1,
        )
    if revenue_growth <= -0.08:
        reason_codes.append("valuation_declining_revenue_caution")
        confidence_penalty += _template_float(
            template,
            "confidence_rules",
            "declining_revenue_penalty",
            0.08,
        )
    if operating_margin is not None and operating_margin <= 0.0:
        reason_codes.append("valuation_unprofitable_caution")
        confidence_penalty += _template_float(
            template,
            "confidence_rules",
            "unprofitable_penalty",
            0.08,
        )
    if required_feature_coverage < 0.8:
        reason_codes.append("valuation_missing_core_facts_caution")
        confidence_penalty += 0.08
    if quality_score < 55.0:
        reason_codes.append("valuation_low_quality_caution")
        confidence_penalty += 0.08
    if str(template.template_id) == "energy_asset_heavy":
        reason_codes.append("valuation_cyclical_template_caution")
        confidence_penalty += _template_float(
            template,
            "confidence_rules",
            "cyclical_penalty",
            0.08,
        )
    if limited_coverage_flag:
        confidence_penalty += 0.12
    return (reason_codes, confidence_penalty)


def _dcf_value_per_share(
    *,
    features: dict[str, Any],
    template: Any,
    treasury_curve_snapshot: dict[str, Any] | None,
    required_feature_coverage: float,
    quality_score: float,
) -> tuple[float | None, dict[str, Any], list[str], float]:
    shares = _safe_float(features.get("diluted_shares_latest")) or _safe_float(
        features.get("shares_outstanding_latest")
    )
    revenue_growth = _safe_float(features.get("revenue_ttm_growth"))
    operating_income_ttm = _safe_float(features.get("operating_income_ttm"))
    starting_fcf, normalized_fcf = _starting_fcf(features, template)
    if shares in (None, 0.0) or starting_fcf is None or starting_fcf <= 0.0:
        return (None, {}, ["valuation_negative_fcf_low_confidence"], 0.2)
    if (operating_income_ttm or 0.0) <= 0.0 and (revenue_growth or 0.0) <= -0.08:
        return (None, {}, ["valuation_unprofitable_caution"], 0.18)

    risk_free_rate, curve_available = _risk_free_rate(treasury_curve_snapshot)
    spread_bps = int(template.valuation_model_mix.get("discount_rate_spread_bps") or 450)
    spread_bps += int(
        template.valuation_model_mix.get("cyclical_discount_rate_spread_bps") or 0
    )
    quality_factor = _quality_premium_factor(
        quality_score=quality_score,
        template=template,
        features=features,
    )
    spread_bps -= int(
        round(
            quality_factor
            * _template_float(
                template,
                "valuation_model_mix",
                "quality_discount_rate_reduction_bps_max",
                0.0,
            )
        )
    )
    spread_bps = max(spread_bps, 250)
    discount_rate = risk_free_rate + (spread_bps / 10_000.0)
    terminal_growth_floor = float(template.valuation_model_mix.get("terminal_growth_floor") or 0.015)
    terminal_growth_cap = float(template.valuation_model_mix.get("terminal_growth_cap") or 0.03)
    terminal_growth_multiplier = _template_float(
        template,
        "valuation_model_mix",
        "terminal_growth_multiplier",
        0.5,
    )
    terminal_growth = clamp(
        (revenue_growth if revenue_growth is not None else 0.03)
        * terminal_growth_multiplier,
        terminal_growth_floor,
        terminal_growth_cap,
    )
    discount_rate = max(discount_rate, terminal_growth + 0.02)
    initial_growth = clamp(
        revenue_growth if revenue_growth is not None else 0.04,
        _template_float(template, "valuation_model_mix", "initial_growth_floor", -0.05),
        _template_float(template, "valuation_model_mix", "initial_growth_cap", 0.18),
    )
    forecast_years = int(template.valuation_model_mix.get("forecast_years") or 5)
    forecast_years = max(forecast_years, 3)

    projected_fcf = starting_fcf
    present_value = 0.0
    for year in range(1, forecast_years + 1):
        blend_divisor = max(forecast_years - 1, 1)
        growth_rate = initial_growth + (
            (terminal_growth - initial_growth) * ((year - 1) / blend_divisor)
        )
        projected_fcf *= 1.0 + growth_rate
        present_value += projected_fcf / ((1.0 + discount_rate) ** year)
    terminal_value = (projected_fcf * (1.0 + terminal_growth)) / (discount_rate - terminal_growth)
    enterprise_value = present_value + (
        terminal_value / ((1.0 + discount_rate) ** forecast_years)
    )
    net_cash = (_safe_float(features.get("cash_and_equivalents_latest")) or 0.0) - (
        _safe_float(features.get("long_term_debt_latest")) or 0.0
    )
    equity_value = enterprise_value + net_cash
    per_share = equity_value / shares
    assumptions = {
        "method": "dcf",
        "risk_free_rate": round(risk_free_rate, 6),
        "discount_rate": round(discount_rate, 6),
        "terminal_growth": round(terminal_growth, 6),
        "initial_growth": round(initial_growth, 6),
        "starting_fcf": round(starting_fcf, 2),
        "normalized_fcf": normalized_fcf,
        "curve_available": curve_available,
        "forecast_years": forecast_years,
        "quality_premium_factor": quality_factor,
        "discount_spread_bps": spread_bps,
    }
    reason_codes = ["valuation_from_dcf"]
    if not curve_available:
        reason_codes.append("treasury_curve_missing_fallback")
    confidence = 0.62 if not normalized_fcf else 0.48
    if not curve_available:
        confidence -= 0.1
    confidence -= max(0.0, 0.8 - required_feature_coverage) * 0.15
    return (round(per_share, 4), assumptions, reason_codes, clamp(confidence, 0.2, 0.75))


def _multiple_value_per_share(
    *,
    metric: str,
    features: dict[str, Any],
    multiple: float,
) -> float | None:
    net_cash_per_share = _safe_float(features.get("net_cash_per_share")) or 0.0
    operating_income_per_share = _safe_float(features.get("operating_income_per_share"))
    free_cash_flow_per_share = _safe_float(features.get("free_cash_flow_per_share"))
    revenue_per_share = _safe_float(features.get("revenue_per_share"))
    net_income_per_share = _safe_float(features.get("net_income_per_share"))
    book_value_per_share = _safe_float(features.get("book_value_per_share"))

    if metric == "ev_ebit" and operating_income_per_share is not None and operating_income_per_share > 0.0:
        return (operating_income_per_share * multiple) + net_cash_per_share
    if metric == "ev_fcf" and free_cash_flow_per_share is not None and free_cash_flow_per_share > 0.0:
        return (free_cash_flow_per_share * multiple) + net_cash_per_share
    if metric == "ev_ebitda" and operating_income_per_share is not None and operating_income_per_share > 0.0:
        return (operating_income_per_share * 1.15 * multiple) + net_cash_per_share
    if metric == "pe" and net_income_per_share is not None and net_income_per_share > 0.0:
        return net_income_per_share * multiple
    if metric == "ps" and revenue_per_share is not None and revenue_per_share > 0.0:
        return revenue_per_share * multiple
    if metric == "pb" and book_value_per_share is not None and book_value_per_share > 0.0:
        return book_value_per_share * multiple
    return None


def _multiples_anchor_value_per_share(
    *,
    features: dict[str, Any],
    template: Any,
    quality_score: float,
) -> tuple[float | None, dict[str, Any], list[str], float]:
    template_multiples = _template_multiple_anchors(template)
    quality_factor = _quality_premium_factor(
        quality_score=quality_score,
        template=template,
        features=features,
    )
    growth_adjustment = clamp(
        1.0
        + (
            (_safe_float(features.get("revenue_ttm_growth")) or 0.0)
            * _template_float(template, "valuation_model_mix", "growth_adjustment_factor", 0.8)
        ),
        _template_float(template, "valuation_model_mix", "growth_adjustment_min", 0.75),
        _template_float(template, "valuation_model_mix", "growth_adjustment_max", 1.35),
    )
    quality_multiple_premium = 1.0 + (
        quality_factor
        * _template_float(
            template,
            "valuation_model_mix",
            "quality_multiple_premium_max",
            0.0,
        )
    )
    values: list[float] = []
    applied_metrics: dict[str, float] = {}
    for metric in list(template.valuation_model_mix.get("primary_multiple_metrics") or []):
        base_multiple = template_multiples.get(str(metric))
        if base_multiple is None:
            continue
        adjusted_multiple = round(base_multiple * growth_adjustment * quality_multiple_premium, 4)
        value = _multiple_value_per_share(metric=str(metric), features=features, multiple=adjusted_multiple)
        if value is None or value <= 0.0:
            continue
        values.append(value)
        applied_metrics[str(metric)] = adjusted_multiple
    if not values:
        return (None, {}, ["valuation_multiples_anchor_unavailable"], 0.2)
    return (
        round(sum(values) / len(values), 4),
        {
            "method": "multiples_anchor",
            "multiples": applied_metrics,
            "growth_adjustment": round(growth_adjustment, 4),
            "quality_premium_factor": quality_factor,
            "quality_multiple_premium": round(quality_multiple_premium, 4),
        },
        ["valuation_from_multiples_anchor"],
        0.45,
    )


def _build_valuation_summary(
    *,
    feature_result: CompanyValuationFeatureResult,
    issuer_row: dict[str, Any],
    quality: QualityBreakdown,
    template: Any,
    market_snapshot: dict[str, Any] | None,
    treasury_curve_snapshot: dict[str, Any] | None,
) -> ValuationSummary:
    dcf_value, dcf_assumptions, dcf_reason_codes, dcf_confidence = _dcf_value_per_share(
        features=feature_result.financial_features,
        template=template,
        treasury_curve_snapshot=treasury_curve_snapshot,
        required_feature_coverage=feature_result.required_feature_coverage,
        quality_score=quality.total_score,
    )
    anchor_value, anchor_assumptions, anchor_reason_codes, anchor_confidence = _multiples_anchor_value_per_share(
        features=feature_result.financial_features,
        template=template,
        quality_score=quality.total_score,
    )

    dcf_weight = float(template.valuation_model_mix.get("dcf_weight") or 0.7)
    anchor_weight = float(template.valuation_model_mix.get("historical_multiples_weight") or 0.3)
    if dcf_value is not None and anchor_value is not None:
        intrinsic_mid = (dcf_value * dcf_weight) + (anchor_value * anchor_weight)
        intrinsic_bear = min(dcf_value, anchor_value) * 0.95
        intrinsic_bull = max(dcf_value, anchor_value) * 1.05
    elif dcf_value is not None:
        intrinsic_mid = dcf_value
        intrinsic_bear = dcf_value * 0.85
        intrinsic_bull = dcf_value * 1.15
    elif anchor_value is not None:
        intrinsic_mid = anchor_value
        intrinsic_bear = anchor_value * 0.85
        intrinsic_bull = anchor_value * 1.15
    else:
        intrinsic_mid = None
        intrinsic_bear = None
        intrinsic_bull = None

    current_price = _safe_float((market_snapshot or {}).get("price"))
    valuation_gap = None
    if intrinsic_mid not in (None, 0.0) and current_price not in (None, 0.0):
        valuation_gap = (intrinsic_mid / current_price) - 1.0

    context_reason_codes, confidence_penalty = _valuation_context(
        features=feature_result.financial_features,
        template=template,
        required_feature_coverage=feature_result.required_feature_coverage,
        quality_score=quality.total_score,
        limited_coverage_flag=bool(issuer_row.get("limited_coverage_flag")),
    )
    reason_codes = list(
        dict.fromkeys([*dcf_reason_codes, *anchor_reason_codes, *context_reason_codes])
    )
    if intrinsic_mid is None:
        reason_codes.append("valuation_unavailable")
    active_confidences: list[tuple[float, float]] = []
    if dcf_value is not None:
        active_confidences.append((dcf_confidence, dcf_weight))
    if anchor_value is not None:
        active_confidences.append((anchor_confidence, anchor_weight))
    if active_confidences:
        total_weight = sum(weight for _, weight in active_confidences) or 1.0
        confidence = sum(value * weight for value, weight in active_confidences) / total_weight
    else:
        confidence = 0.12
    quality_factor = _quality_premium_factor(
        quality_score=quality.total_score,
        template=template,
        features=feature_result.financial_features,
    )
    confidence += min(feature_result.required_feature_coverage, 1.0) * 0.1
    confidence += quality_factor * 0.08
    confidence -= confidence_penalty
    confidence = clamp(confidence, 0.12, 0.9)
    if bool(issuer_row.get("limited_coverage_flag")):
        confidence = clamp(confidence - 0.15, 0.1, 0.9)
    if confidence < 0.3:
        reason_codes.append("valuation_low_confidence")
    return ValuationSummary(
        intrinsic_value_bear=None if intrinsic_bear is None else round(intrinsic_bear, 4),
        intrinsic_value_base=None if intrinsic_mid is None else round(intrinsic_mid, 4),
        intrinsic_value_bull=None if intrinsic_bull is None else round(intrinsic_bull, 4),
        intrinsic_value_mid=None if intrinsic_mid is None else round(intrinsic_mid, 4),
        current_price=None if current_price is None else round(current_price, 4),
        valuation_gap=None if valuation_gap is None else round(valuation_gap, 6),
        confidence=round(confidence, 4),
        reason_codes=tuple(reason_codes),
        assumption_summary={
            "component_values": {
                "dcf_value_per_share": dcf_value,
                "multiples_anchor_value_per_share": anchor_value,
                "dcf_weight": round(dcf_weight, 4),
                "historical_multiples_weight": round(anchor_weight, 4),
            },
            "dcf": dcf_assumptions,
            "multiples_anchor": anchor_assumptions,
            "valuation_flags": context_reason_codes,
        },
    )


def recompute_company_valuation(
    *,
    issuer_id: str | None = None,
    ticker: str | None = None,
    as_of: str | datetime | None = None,
    repository: CompanyValuationRepository | None = None,
    config_root: str | None = None,
    feature_version: str = COMPANY_VALUATION_FEATURE_VERSION,
    evaluation_version: str = EVALUATION_VERSION,
    persist: bool = True,
) -> CompanyValuationRecomputeResult:
    repo = repository or CompanyValuationRepository()
    issuer_row = repo.get_issuer(issuer_id=issuer_id, ticker=ticker)
    if issuer_row is None:
        raise ValueError("issuer_id or ticker must resolve to an ingested issuer")
    as_of_dt = _normalized_as_of(as_of)
    feature_result = compute_company_valuation_features(
        issuer_row=issuer_row,
        as_of=as_of_dt,
        repository=repo,
        feature_version=feature_version,
        config_root=config_root,
    )
    feature_snapshot = (
        repo.upsert_feature_snapshot(feature_result.feature_snapshot_payload)
        if persist
        else dict(feature_result.feature_snapshot_payload)
    )

    point_in_time = resolve_company_valuation_point_in_time(
        issuer_id=str(issuer_row["issuer_id"]),
        as_of=as_of_dt,
        repository=repo,
    )
    template = resolve_company_valuation_template(str(issuer_row["template_id"]), config_root)
    quality = _build_quality_breakdown(
        feature_result=feature_result,
        issuer_row=issuer_row,
        config_root=config_root,
    )
    valuation = _build_valuation_summary(
        feature_result=feature_result,
        issuer_row=issuer_row,
        quality=quality,
        template=template,
        market_snapshot=point_in_time.latest_market_snapshot,
        treasury_curve_snapshot=point_in_time.latest_treasury_curve_snapshot,
    )

    issuer_identity = CompanyValuationIdentity(
        issuer_id=str(issuer_row["issuer_id"]),
        cik=str(issuer_row["cik"]),
        ticker=str(issuer_row.get("ticker") or ticker or ""),
        company_name=str(issuer_row["company_name"]),
        template_id=str(issuer_row["template_id"]),
        template_version=str(issuer_row["template_version"]),
    )
    latest_filing_available_at = parse_datetime((point_in_time.latest_filing or {}).get("available_at"))
    filing_freshness_days = None
    if latest_filing_available_at is not None:
        filing_freshness_days = max((as_of_dt.date() - latest_filing_available_at.date()).days, 0)
    document = CompanyValuationDocument(
        payload_version="v1",
        issuer=issuer_identity,
        as_of=as_of_dt,
        freshness={
            "filing_freshness_days": filing_freshness_days,
            "ownership_freshness_days": feature_result.ownership_signal.freshness_days,
            "market_snapshot_at": None if point_in_time.latest_market_snapshot is None else point_in_time.latest_market_snapshot.get("captured_at"),
        },
        source_summary={
            "template_id": issuer_identity.template_id,
            "latest_filing_id": None if point_in_time.latest_filing is None else point_in_time.latest_filing.get("filing_id"),
            "latest_statement_snapshot_id": None
            if point_in_time.latest_statement_snapshot is None
            else point_in_time.latest_statement_snapshot.get("snapshot_id"),
            "latest_market_snapshot_id": None
            if point_in_time.latest_market_snapshot is None
            else point_in_time.latest_market_snapshot.get("market_snapshot_id"),
            "latest_treasury_curve_snapshot_id": None
            if point_in_time.latest_treasury_curve_snapshot is None
            else point_in_time.latest_treasury_curve_snapshot.get("curve_snapshot_id"),
        },
        quality=quality,
        valuation=valuation,
        ownership={
            "signal": feature_result.ownership_signal.to_payload(),
            "features": feature_result.ownership_features,
        },
        risks={
            "top_reason_codes": list(dict.fromkeys([*quality.reason_codes, *valuation.reason_codes]))[:8],
            "limited_coverage_flag": bool(issuer_row.get("limited_coverage_flag")),
        },
        provenance={
            "filings_used": feature_result.filings_used,
            "statement_snapshot_ids": (
                feature_snapshot.get("dependency_refs_json")
                or feature_snapshot.get("dependency_refs")
                or {}
            ).get("statement_snapshot_ids", []),
            "feature_snapshot_id": feature_snapshot.get("feature_snapshot_id"),
            "feature_version": feature_version,
            "evaluation_version": evaluation_version,
        },
    )
    document_payload = render_value(document.to_payload())

    primary_security = repo.get_primary_security(issuer_id=str(issuer_row["issuer_id"]))
    top_reason_codes = list(dict.fromkeys([*quality.reason_codes, *valuation.reason_codes]))[:8]
    valuation_snapshot_payload = {
        "company_valuation_snapshot_id": build_company_valuation_snapshot_id(
            str(issuer_row["cik"]),
            as_of_dt,
            evaluation_version,
        ),
        "issuer_id": str(issuer_row["issuer_id"]),
        "security_id": None if primary_security is None else str(primary_security["security_id"]),
        "template_id": str(issuer_row["template_id"]),
        "template_version": str(issuer_row["template_version"]),
        "as_of": as_of_dt,
        "evaluation_version": evaluation_version,
        "quality_score": quality.total_score,
        "ownership_score": round(feature_result.ownership_signal.score, 2),
        "intrinsic_value_bear": valuation.intrinsic_value_bear,
        "intrinsic_value_base": valuation.intrinsic_value_base,
        "intrinsic_value_bull": valuation.intrinsic_value_bull,
        "intrinsic_value_mid": valuation.intrinsic_value_mid,
        "current_price": valuation.current_price,
        "valuation_gap": valuation.valuation_gap,
        "quality_confidence": quality.confidence,
        "valuation_confidence": valuation.confidence,
        "limited_coverage_flag": bool(issuer_row.get("limited_coverage_flag")),
        "top_reason_codes_json": top_reason_codes,
        "valuation_json": document_payload,
        "computed_at": datetime.now(UTC),
    }
    company_valuation_snapshot = (
        repo.upsert_company_valuation_snapshot(valuation_snapshot_payload)
        if persist
        else dict(valuation_snapshot_payload)
    )

    screening_row = CompanyValuationScreenRow(
        screening_row_id=build_screening_row_id(str(issuer_row["cik"]), as_of_dt),
        issuer_id=str(issuer_row["issuer_id"]),
        ticker=issuer_identity.ticker,
        template_id=issuer_identity.template_id,
        as_of=as_of_dt.date(),
        quality_score=quality.total_score,
        intrinsic_value_mid=valuation.intrinsic_value_mid,
        current_price=valuation.current_price,
        valuation_gap=valuation.valuation_gap,
        quality_confidence=quality.confidence,
        valuation_confidence=valuation.confidence,
        ownership_score=round(feature_result.ownership_signal.score, 2),
        ownership_special_situation_flag=bool(
            feature_result.ownership_features.get("ownership_special_situation_flag")
        ),
        limited_coverage_flag=bool(issuer_row.get("limited_coverage_flag")),
        top_reason_codes=tuple(top_reason_codes),
    )
    screening_row_payload = screening_row.to_payload()
    screening_row_payload["security_id"] = None if primary_security is None else str(primary_security["security_id"])
    screening_row_payload["top_reason_codes_json"] = screening_row_payload.pop("top_reason_codes")
    screening_row_payload["updated_at"] = datetime.now(UTC)
    persisted_screening_row = (
        repo.upsert_screening_row(screening_row_payload)
        if persist
        else dict(screening_row_payload)
    )

    return CompanyValuationRecomputeResult(
        feature_snapshot=feature_snapshot,
        company_valuation_snapshot=company_valuation_snapshot,
        screening_row=persisted_screening_row,
        document=document_payload,
    )


__all__ = [
    "COMPANY_VALUATION_FEATURE_VERSION",
    "CompanyValuationRecomputeResult",
    "EVALUATION_VERSION",
    "recompute_company_valuation",
]

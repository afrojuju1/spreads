from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from core.common import clamp
from core.services.company_valuation.contracts import OwnershipEvidence, OwnershipSignal
from core.services.company_valuation.ids import build_feature_snapshot_id
from core.services.company_valuation.templates import resolve_company_valuation_template
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_date, parse_datetime

FEATURE_VERSION = "v1"


@dataclass(frozen=True)
class CompanyValuationFeatureResult:
    feature_snapshot_payload: dict[str, Any]
    financial_features: dict[str, Any]
    ownership_features: dict[str, Any]
    ownership_signal: OwnershipSignal
    required_feature_coverage: float
    statement_snapshots_used: list[dict[str, Any]]
    filings_used: list[str]


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _period_end(snapshot: dict[str, Any]) -> date:
    value = parse_date(snapshot["period_end"])
    return value


def _available_at(snapshot: dict[str, Any]) -> datetime:
    value = parse_datetime(snapshot["available_at"])
    if value is None:
        raise ValueError("statement snapshot missing available_at")
    return value.astimezone(UTC)


def _metrics(snapshot: dict[str, Any]) -> dict[str, Any]:
    value = snapshot.get("metrics_json")
    if not isinstance(value, dict):
        value = snapshot.get("metrics")
    return dict(value) if isinstance(value, dict) else {}


def _latest_metric_snapshot(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    ordered = sorted(
        rows,
        key=lambda row: (
            len(_metrics(row)),
            _period_end(row),
            1 if len(_metrics(row)) >= 4 else 0,
            0 if str(row.get("period_type")) != "instant" else -1,
            _available_at(row),
        ),
        reverse=True,
    )
    return ordered[0]


def _dedupe_statement_snapshots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            len(_metrics(row)),
            1 if len(_metrics(row)) >= 4 else 0,
            _period_end(row),
            _available_at(row),
            0 if str(row.get("period_type")) != "instant" else -1,
        ),
        reverse=True,
    )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in ordered_rows:
        key = (str(row.get("period_end")), str(row.get("period_type")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _quarterly_snapshots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    quarterly = [row for row in rows if str(row.get("period_type")) == "quarterly"]
    return sorted(quarterly, key=_period_end, reverse=True)


def _annual_snapshots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    annual = [row for row in rows if str(row.get("period_type")) == "annual"]
    return sorted(annual, key=_period_end, reverse=True)


def _sum_metric(rows: list[dict[str, Any]], metric_name: str) -> float | None:
    values: list[float] = []
    for row in rows:
        value = _safe_float(_metrics(row).get(metric_name))
        if value is None:
            return None
        values.append(value)
    return sum(values)


def _latest_metric(rows: list[dict[str, Any]], metric_name: str) -> float | None:
    for row in rows:
        value = _safe_float(_metrics(row).get(metric_name))
        if value is not None:
            return value
    return None


def _compute_ttm_features(statement_snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    quarterly = _quarterly_snapshots(statement_snapshots)
    annual = _annual_snapshots(statement_snapshots)
    latest = _latest_metric_snapshot(statement_snapshots)
    latest_metrics = _metrics(latest) if latest else {}
    revenue_ttm = _sum_metric(quarterly[:4], "revenue") if len(quarterly) >= 4 else None
    if revenue_ttm is None and annual:
        revenue_ttm = _safe_float(_metrics(annual[0]).get("revenue"))
    gross_profit_ttm = (
        _sum_metric(quarterly[:4], "gross_profit") if len(quarterly) >= 4 else None
    )
    if gross_profit_ttm is None and annual:
        gross_profit_ttm = _safe_float(_metrics(annual[0]).get("gross_profit"))
    operating_income_ttm = (
        _sum_metric(quarterly[:4], "operating_income") if len(quarterly) >= 4 else None
    )
    if operating_income_ttm is None and annual:
        operating_income_ttm = _safe_float(_metrics(annual[0]).get("operating_income"))
    net_income_ttm = (
        _sum_metric(quarterly[:4], "net_income") if len(quarterly) >= 4 else None
    )
    if net_income_ttm is None and annual:
        net_income_ttm = _safe_float(_metrics(annual[0]).get("net_income"))
    operating_cash_flow_ttm = (
        _sum_metric(quarterly[:4], "operating_cash_flow") if len(quarterly) >= 4 else None
    )
    if operating_cash_flow_ttm is None and annual:
        operating_cash_flow_ttm = _safe_float(
            _metrics(annual[0]).get("operating_cash_flow")
        )
    capex_ttm_raw = _sum_metric(quarterly[:4], "capex") if len(quarterly) >= 4 else None
    if capex_ttm_raw is None and annual:
        capex_ttm_raw = _safe_float(_metrics(annual[0]).get("capex"))
    capex_ttm = None if capex_ttm_raw is None else abs(capex_ttm_raw)
    free_cash_flow_ttm = None
    if operating_cash_flow_ttm is not None:
        free_cash_flow_ttm = operating_cash_flow_ttm - (capex_ttm or 0.0)

    prior_revenue_ttm = _sum_metric(quarterly[4:8], "revenue") if len(quarterly) >= 8 else None
    if prior_revenue_ttm is None and len(annual) >= 2:
        prior_revenue_ttm = _safe_float(_metrics(annual[1]).get("revenue"))

    revenue_ttm_growth = None
    if revenue_ttm is not None and prior_revenue_ttm not in (None, 0):
        revenue_ttm_growth = (revenue_ttm / prior_revenue_ttm) - 1.0

    current_assets = _safe_float(latest_metrics.get("current_assets"))
    current_liabilities = _safe_float(latest_metrics.get("current_liabilities"))
    inventory = _safe_float(latest_metrics.get("inventory"))
    total_assets = _safe_float(latest_metrics.get("total_assets"))
    cash_and_equivalents = _safe_float(latest_metrics.get("cash_and_equivalents"))
    long_term_debt = _safe_float(latest_metrics.get("long_term_debt"))
    total_liabilities = _safe_float(latest_metrics.get("total_liabilities"))
    stockholders_equity = _safe_float(latest_metrics.get("stockholders_equity"))
    shares_outstanding = _safe_float(latest_metrics.get("shares_outstanding")) or _safe_float(
        latest_metrics.get("diluted_weighted_average_shares")
    )
    diluted_shares_latest = _safe_float(
        latest_metrics.get("diluted_weighted_average_shares")
    ) or shares_outstanding
    diluted_shares_prior = None
    if len(quarterly) >= 5:
        diluted_shares_prior = _safe_float(
            _metrics(quarterly[4]).get("diluted_weighted_average_shares")
        )
    elif len(annual) >= 2:
        diluted_shares_prior = _safe_float(
            _metrics(annual[1]).get("diluted_weighted_average_shares")
        )
    diluted_share_growth_ttm = None
    if diluted_shares_latest is not None and diluted_shares_prior not in (None, 0):
        diluted_share_growth_ttm = (diluted_shares_latest / diluted_shares_prior) - 1.0

    sbc_ttm = _sum_metric(quarterly[:4], "stock_based_compensation") if len(quarterly) >= 4 else None
    if sbc_ttm is None and annual:
        sbc_ttm = _safe_float(_metrics(annual[0]).get("stock_based_compensation"))

    cost_of_revenue_ttm = _sum_metric(quarterly[:4], "cost_of_revenue") if len(quarterly) >= 4 else None
    if cost_of_revenue_ttm is None and annual:
        cost_of_revenue_ttm = _safe_float(_metrics(annual[0]).get("cost_of_revenue"))
    deferred_revenue_latest = _safe_float(latest_metrics.get("deferred_revenue"))
    deferred_revenue_prior = None
    if len(quarterly) >= 5:
        deferred_revenue_prior = _safe_float(_metrics(quarterly[4]).get("deferred_revenue"))
    elif len(annual) >= 2:
        deferred_revenue_prior = _safe_float(_metrics(annual[1]).get("deferred_revenue"))
    deferred_revenue_growth = None
    if deferred_revenue_latest is not None and deferred_revenue_prior not in (None, 0):
        deferred_revenue_growth = (deferred_revenue_latest / deferred_revenue_prior) - 1.0

    gross_margin_ttm = _ratio(gross_profit_ttm, revenue_ttm)
    operating_margin_ttm = _ratio(operating_income_ttm, revenue_ttm)
    net_margin_ttm = _ratio(net_income_ttm, revenue_ttm)
    free_cash_flow_margin_ttm = _ratio(free_cash_flow_ttm, revenue_ttm)
    capex_intensity = _ratio(capex_ttm, revenue_ttm)
    sbc_as_pct_revenue = _ratio(sbc_ttm, revenue_ttm)
    current_ratio = _ratio(current_assets, current_liabilities)
    debt_to_equity = _ratio(long_term_debt or total_liabilities, stockholders_equity)
    net_cash = None
    if cash_and_equivalents is not None or long_term_debt is not None:
        net_cash = (cash_and_equivalents or 0.0) - (long_term_debt or 0.0)
    net_leverage = None
    if free_cash_flow_ttm not in (None, 0) and long_term_debt is not None and cash_and_equivalents is not None:
        net_leverage = max(long_term_debt - cash_and_equivalents, 0.0) / abs(free_cash_flow_ttm)
    invested_capital = None
    if stockholders_equity is not None or long_term_debt is not None:
        invested_capital = (stockholders_equity or 0.0) + (long_term_debt or 0.0) - (cash_and_equivalents or 0.0)
    roic_ttm = _ratio(operating_income_ttm, invested_capital)
    asset_turnover = _ratio(revenue_ttm, total_assets)
    inventory_turns = _ratio(cost_of_revenue_ttm or revenue_ttm, inventory)
    book_value_per_share = _ratio(stockholders_equity, shares_outstanding)
    revenue_per_share = _ratio(revenue_ttm, shares_outstanding)
    net_income_per_share = _ratio(net_income_ttm, shares_outstanding)
    free_cash_flow_per_share = _ratio(free_cash_flow_ttm, shares_outstanding)
    net_cash_per_share = _ratio(net_cash, shares_outstanding)
    operating_income_per_share = _ratio(operating_income_ttm, shares_outstanding)

    return {
        "latest_period_end": None if latest is None else str(latest.get("period_end")),
        "revenue_ttm": revenue_ttm,
        "revenue_ttm_growth": revenue_ttm_growth,
        "gross_profit_ttm": gross_profit_ttm,
        "gross_margin_ttm": gross_margin_ttm,
        "operating_income_ttm": operating_income_ttm,
        "operating_margin_ttm": operating_margin_ttm,
        "net_income_ttm": net_income_ttm,
        "net_margin_ttm": net_margin_ttm,
        "operating_cash_flow_ttm": operating_cash_flow_ttm,
        "capex_ttm": capex_ttm,
        "free_cash_flow_ttm": free_cash_flow_ttm,
        "free_cash_flow_margin_ttm": free_cash_flow_margin_ttm,
        "capex_intensity": capex_intensity,
        "current_ratio": current_ratio,
        "debt_to_equity": debt_to_equity,
        "net_leverage": net_leverage,
        "roic_ttm": roic_ttm,
        "asset_turnover": asset_turnover,
        "inventory_turns": inventory_turns,
        "diluted_share_growth_ttm": diluted_share_growth_ttm,
        "shares_outstanding_latest": shares_outstanding,
        "diluted_shares_latest": diluted_shares_latest,
        "sbc_ttm": sbc_ttm,
        "sbc_as_pct_revenue": sbc_as_pct_revenue,
        "deferred_revenue_latest": deferred_revenue_latest,
        "deferred_revenue_growth": deferred_revenue_growth,
        "cash_and_equivalents_latest": cash_and_equivalents,
        "long_term_debt_latest": long_term_debt,
        "total_liabilities_latest": total_liabilities,
        "stockholders_equity_latest": stockholders_equity,
        "total_assets_latest": total_assets,
        "book_value_per_share": book_value_per_share,
        "revenue_per_share": revenue_per_share,
        "net_income_per_share": net_income_per_share,
        "free_cash_flow_per_share": free_cash_flow_per_share,
        "net_cash_per_share": net_cash_per_share,
        "operating_income_per_share": operating_income_per_share,
    }


def _compute_ownership_features(
    *,
    issuer_id: str,
    as_of: datetime,
    repository: CompanyValuationRepository,
) -> tuple[dict[str, Any], OwnershipSignal]:
    insider_cutoff = as_of.date().toordinal() - 90
    insiders = repository.list_insider_transactions_before(
        issuer_id=issuer_id,
        as_of=as_of,
        limit=100,
    )
    positions = repository.list_beneficial_owner_positions_before(
        issuer_id=issuer_id,
        as_of=as_of,
        limit=200,
    )
    institutional_positions = repository.list_institutional_positions_before(
        issuer_id=issuer_id,
        as_of=as_of,
        limit=20000,
    )
    latest_positions: dict[str, dict[str, Any]] = {}
    for row in positions:
        holder_id = str(row.get("holder_id") or "")
        if not holder_id or holder_id in latest_positions:
            continue
        latest_positions[holder_id] = row
    latest_institutional_positions: dict[str, dict[str, Any]] = {}
    for row in institutional_positions:
        holder_id = str(row.get("institutional_holder_id") or "")
        if not holder_id or holder_id in latest_institutional_positions:
            continue
        latest_institutional_positions[holder_id] = row
    insider_net_shares = 0.0
    insider_buy_count = 0
    insider_sell_count = 0
    insider_latest_available_at: datetime | None = None
    for row in insiders:
        transaction_date = parse_date(row["transaction_date"]) if row.get("transaction_date") else None
        if transaction_date is not None and transaction_date.toordinal() < insider_cutoff:
            continue
        available_at = parse_datetime(row.get("available_at"))
        if available_at is not None:
            insider_latest_available_at = max(
                insider_latest_available_at or available_at,
                available_at,
            )
        shares_delta = _safe_float(row.get("shares_delta")) or 0.0
        code = str(row.get("transaction_code") or "").upper()
        if code in {"A", "P"}:
            insider_net_shares += shares_delta
            insider_buy_count += 1
        elif code in {"D", "S"}:
            insider_net_shares -= shares_delta
            insider_sell_count += 1
    max_ownership_pct = 0.0
    total_ownership_pct = 0.0
    control_intent_holder_count = 0
    passive_holder_count = 0
    position_latest_available_at: datetime | None = None
    for row in latest_positions.values():
        pct = _safe_float(row.get("ownership_pct")) or 0.0
        max_ownership_pct = max(max_ownership_pct, pct)
        total_ownership_pct += pct
        if bool(row.get("control_intent_flag")):
            control_intent_holder_count += 1
        if bool(row.get("passive_flag")):
            passive_holder_count += 1
        available_at = parse_datetime(row.get("available_at"))
        if available_at is not None:
            position_latest_available_at = max(
                position_latest_available_at or available_at,
                available_at,
            )

    institutional_holder_count = 0
    institutional_option_position_count = 0
    institutional_total_shares = 0.0
    institutional_top_holder_share_count = 0.0
    institutional_latest_available_at: datetime | None = None
    latest_institutional_report_period: date | None = None
    for row in latest_institutional_positions.values():
        available_at = parse_datetime(row.get("available_at"))
        if available_at is not None:
            institutional_latest_available_at = max(
                institutional_latest_available_at or available_at,
                available_at,
            )
        report_period = parse_date(row["report_period"]) if row.get("report_period") else None
        if report_period is not None:
            latest_institutional_report_period = max(
                latest_institutional_report_period or report_period,
                report_period,
            )
        if str(row.get("put_call") or "").strip():
            institutional_option_position_count += 1
            continue
        institutional_holder_count += 1
        share_count = _safe_float(row.get("share_count")) or 0.0
        institutional_total_shares += share_count
        institutional_top_holder_share_count = max(
            institutional_top_holder_share_count,
            share_count,
        )
    institutional_top_holder_pct_of_tracked = None
    if institutional_total_shares > 0.0:
        institutional_top_holder_pct_of_tracked = (
            institutional_top_holder_share_count / institutional_total_shares
        )

    latest_available_at = max(
        [
            value
            for value in (
                insider_latest_available_at,
                position_latest_available_at,
                institutional_latest_available_at,
            )
            if value is not None
        ],
        default=None,
    )
    freshness_days = None
    if latest_available_at is not None:
        freshness_days = max((as_of.date() - latest_available_at.date()).days, 0)

    evidence: list[OwnershipEvidence] = []
    reason_codes: list[str] = []
    score = 50.0
    if insider_net_shares > 0:
        score += 18.0
        reason_codes.append("insider_net_buying_positive")
        evidence.append(
            OwnershipEvidence(
                source_type="form4",
                holder_id=None,
                group_id=None,
                event_date=None,
                available_at=latest_available_at or as_of,
                headline="Recent insider buying activity",
                reason_code="insider_net_buying_positive",
                metrics={"net_shares": insider_net_shares, "buy_count": insider_buy_count},
            )
        )
    elif insider_net_shares < 0:
        score -= 18.0
        reason_codes.append("insider_net_selling_negative")
        evidence.append(
            OwnershipEvidence(
                source_type="form4",
                holder_id=None,
                group_id=None,
                event_date=None,
                available_at=latest_available_at or as_of,
                headline="Recent insider selling activity",
                reason_code="insider_net_selling_negative",
                metrics={"net_shares": insider_net_shares, "sell_count": insider_sell_count},
            )
        )
    if control_intent_holder_count > 0:
        score -= 6.0
        reason_codes.extend(
            [
                "new_13d_filed_special_situation",
                "13d_control_intent_caution",
            ]
        )
    if max_ownership_pct >= 20.0:
        score -= 8.0
        reason_codes.append("holder_concentration_risk_negative")
    elif max_ownership_pct >= 5.0:
        score += 4.0
        reason_codes.append("13g_large_passive_owner_positive")
    if institutional_holder_count >= 25:
        score += 3.0
        reason_codes.append("13f_institutional_breadth_positive")
        evidence.append(
            OwnershipEvidence(
                source_type="13f",
                holder_id=None,
                group_id=None,
                event_date=latest_institutional_report_period,
                available_at=institutional_latest_available_at or as_of,
                headline="Broad institutional holder base",
                reason_code="13f_institutional_breadth_positive",
                metrics={"holder_count": institutional_holder_count},
            )
        )
    if (institutional_top_holder_pct_of_tracked or 0.0) >= 0.25:
        score -= 4.0
        reason_codes.append("13f_holder_concentration_caution")
        evidence.append(
            OwnershipEvidence(
                source_type="13f",
                holder_id=None,
                group_id=None,
                event_date=latest_institutional_report_period,
                available_at=institutional_latest_available_at or as_of,
                headline="Concentrated institutional ownership within tracked 13F holders",
                reason_code="13f_holder_concentration_caution",
                metrics={
                    "top_holder_pct_of_tracked": institutional_top_holder_pct_of_tracked,
                    "tracked_total_shares": institutional_total_shares,
                },
            )
        )
    if latest_available_at is None:
        reason_codes.append("ownership_signal_stale_caution")
    elif freshness_days is not None and freshness_days > 180:
        score -= 6.0
        reason_codes.append("ownership_signal_stale_caution")
    ownership_special_situation_flag = control_intent_holder_count > 0
    ownership_score = clamp(score / 100.0, 0.0, 1.0) * 100.0
    signal = OwnershipSignal(
        score=ownership_score,
        confidence=0.2 if latest_available_at is None else clamp(1.0 - ((freshness_days or 0) / 365.0), 0.25, 0.85),
        freshness_days=freshness_days,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
        evidence=tuple(evidence),
    )
    return (
        {
            "insider_net_shares_90d": insider_net_shares,
            "insider_buy_count_90d": insider_buy_count,
            "insider_sell_count_90d": insider_sell_count,
            "beneficial_owner_position_count": len(latest_positions),
            "beneficial_owner_total_pct": total_ownership_pct,
            "beneficial_owner_max_pct": max_ownership_pct,
            "control_intent_holder_count": control_intent_holder_count,
            "passive_holder_count": passive_holder_count,
            "institutional_holder_count": institutional_holder_count,
            "institutional_option_position_count": institutional_option_position_count,
            "institutional_total_shares_reported": institutional_total_shares,
            "institutional_top_holder_share_count": institutional_top_holder_share_count,
            "institutional_top_holder_pct_of_tracked": institutional_top_holder_pct_of_tracked,
            "latest_institutional_report_period": None
            if latest_institutional_report_period is None
            else latest_institutional_report_period.isoformat(),
            "latest_available_at": None if latest_available_at is None else latest_available_at.isoformat().replace("+00:00", "Z"),
            "ownership_special_situation_flag": ownership_special_situation_flag,
            "reason_codes": list(signal.reason_codes),
        },
        signal,
    )


def compute_company_valuation_features(
    *,
    issuer_row: dict[str, Any],
    as_of: str | datetime,
    repository: CompanyValuationRepository,
    feature_version: str = FEATURE_VERSION,
    config_root: str | None = None,
) -> CompanyValuationFeatureResult:
    as_of_dt = parse_datetime(as_of)
    if as_of_dt is None:
        raise ValueError("as_of is required")
    as_of_dt = as_of_dt.astimezone(UTC)
    issuer_id = str(issuer_row["issuer_id"])
    statement_snapshots = repository.list_statement_snapshots_before(
        issuer_id=issuer_id,
        as_of=as_of_dt,
        limit=32,
    )
    deduped_snapshots = _dedupe_statement_snapshots(statement_snapshots)
    if not deduped_snapshots:
        raise ValueError(f"No statement snapshots available for issuer {issuer_id}")
    financial_features = _compute_ttm_features(deduped_snapshots)
    ownership_features, ownership_signal = _compute_ownership_features(
        issuer_id=issuer_id,
        as_of=as_of_dt,
        repository=repository,
    )
    filings_used = list(
        dict.fromkeys(str(row.get("filing_id")) for row in deduped_snapshots if row.get("filing_id"))
    )
    template_id = str(issuer_row.get("template_id") or "")
    template_version = str(issuer_row.get("template_version") or "")
    template = resolve_company_valuation_template(
        template_id or "general_operating",
        config_root,
    )
    required_features = template.required_features
    required_hits = sum(1 for key in required_features if financial_features.get(key) is not None)
    required_feature_coverage = 1.0 if not required_features else required_hits / len(required_features)
    feature_snapshot_payload = {
        "feature_snapshot_id": build_feature_snapshot_id(
            str(issuer_row.get("cik") or issuer_id),
            as_of_dt,
            feature_version,
        ),
        "issuer_id": issuer_id,
        "template_id": template_id,
        "template_version": template_version,
        "as_of": as_of_dt,
        "feature_version": feature_version,
        "financial_features_json": financial_features,
        "ownership_features_json": ownership_features,
        "dependency_refs_json": {
            "statement_snapshot_ids": [
                str(row.get("snapshot_id")) for row in deduped_snapshots[:8]
            ],
            "filing_ids": filings_used,
        },
        "computed_at": datetime.now(UTC),
    }
    return CompanyValuationFeatureResult(
        feature_snapshot_payload=feature_snapshot_payload,
        financial_features=financial_features,
        ownership_features=ownership_features,
        ownership_signal=ownership_signal,
        required_feature_coverage=required_feature_coverage,
        statement_snapshots_used=deduped_snapshots,
        filings_used=filings_used,
    )


__all__ = [
    "CompanyValuationFeatureResult",
    "FEATURE_VERSION",
    "compute_company_valuation_features",
]

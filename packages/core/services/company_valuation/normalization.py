from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

from core.common import parse_float
from core.services.company_valuation.ids import build_filing_id
from core.storage.serializers import parse_date, parse_datetime

MONETARY_UNIT = "USD"
SHARES_UNIT = "shares"

SUPPORTED_PERIODIC_FORM_PREFIXES = ("10-K", "10-Q", "8-K")

CANONICAL_METRIC_CONCEPTS: dict[str, tuple[tuple[str, str], ...]] = {
    "revenue": (
        ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
        ("us-gaap", "SalesRevenueNet"),
        ("us-gaap", "Revenues"),
    ),
    "cost_of_revenue": (
        ("us-gaap", "CostOfGoodsSold"),
        ("us-gaap", "CostOfGoodsAndServicesSold"),
        ("us-gaap", "CostOfSales"),
        ("us-gaap", "CostOfRevenue"),
    ),
    "gross_profit": (
        ("us-gaap", "GrossProfit"),
    ),
    "operating_income": (
        ("us-gaap", "OperatingIncomeLoss"),
    ),
    "net_income": (
        ("us-gaap", "NetIncomeLoss"),
        ("us-gaap", "ProfitLoss"),
    ),
    "pretax_income": (
        (
            "us-gaap",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        ),
        (
            "us-gaap",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
        ),
    ),
    "operating_cash_flow": (
        ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
        ("us-gaap", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    ),
    "capex": (
        ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"),
        ("us-gaap", "PaymentsToAcquireProductiveAssets"),
        ("us-gaap", "CapitalExpendituresIncurredButNotYetPaid"),
    ),
    "cash_and_equivalents": (
        ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
        ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
    ),
    "current_assets": (
        ("us-gaap", "AssetsCurrent"),
    ),
    "inventory": (
        ("us-gaap", "InventoryNet"),
        ("us-gaap", "InventoryFinishedGoods"),
    ),
    "total_assets": (
        ("us-gaap", "Assets"),
    ),
    "current_liabilities": (
        ("us-gaap", "LiabilitiesCurrent"),
    ),
    "total_liabilities": (
        ("us-gaap", "Liabilities"),
    ),
    "long_term_debt": (
        ("us-gaap", "LongTermDebtNoncurrent"),
        ("us-gaap", "LongTermDebt"),
        ("us-gaap", "LongTermDebtAndCapitalLeaseObligations"),
    ),
    "stockholders_equity": (
        ("us-gaap", "StockholdersEquity"),
        ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    ),
    "diluted_weighted_average_shares": (
        ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
        ("us-gaap", "WeightedAverageNumberOfShareOutstandingBasicAndDiluted"),
    ),
    "shares_outstanding": (
        ("dei", "EntityCommonStockSharesOutstanding"),
    ),
    "stock_based_compensation": (
        ("us-gaap", "ShareBasedCompensation"),
    ),
    "deferred_revenue": (
        ("us-gaap", "ContractWithCustomerLiabilityCurrent"),
        ("us-gaap", "DeferredRevenueCurrent"),
        ("us-gaap", "ContractWithCustomerLiability"),
    ),
}

PREFERRED_UNITS_BY_METRIC = {
    "revenue": {MONETARY_UNIT},
    "cost_of_revenue": {MONETARY_UNIT},
    "gross_profit": {MONETARY_UNIT},
    "operating_income": {MONETARY_UNIT},
    "net_income": {MONETARY_UNIT},
    "pretax_income": {MONETARY_UNIT},
    "operating_cash_flow": {MONETARY_UNIT},
    "capex": {MONETARY_UNIT},
    "cash_and_equivalents": {MONETARY_UNIT},
    "current_assets": {MONETARY_UNIT},
    "inventory": {MONETARY_UNIT},
    "total_assets": {MONETARY_UNIT},
    "current_liabilities": {MONETARY_UNIT},
    "total_liabilities": {MONETARY_UNIT},
    "long_term_debt": {MONETARY_UNIT},
    "stockholders_equity": {MONETARY_UNIT},
    "diluted_weighted_average_shares": {SHARES_UNIT},
    "shares_outstanding": {SHARES_UNIT},
    "stock_based_compensation": {MONETARY_UNIT},
    "deferred_revenue": {MONETARY_UNIT},
}

INSTANT_PREFERRED_METRICS = {
    "cash_and_equivalents",
    "current_assets",
    "inventory",
    "total_assets",
    "current_liabilities",
    "total_liabilities",
    "long_term_debt",
    "stockholders_equity",
    "shares_outstanding",
    "deferred_revenue",
}


@dataclass(frozen=True)
class SubmissionIssuerProfile:
    cik: str
    company_name: str
    sic: str | None
    sic_description: str | None
    naics: str | None
    tickers: tuple[str, ...]
    exchanges: tuple[str, ...]


def normalize_cik(value: str | int | None) -> str:
    rendered = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not rendered:
        raise ValueError("CIK is required")
    return rendered.zfill(10)


def _normalize_datetime_from_filing_date(value: date | None) -> datetime | None:
    if value is None:
        return None
    return datetime.combine(value, time.min, tzinfo=UTC)


def _normalized_form_prefix(form_type: str | None) -> str:
    return str(form_type or "").upper().strip().removesuffix("/A")


def filing_matches_requested_forms(
    form_type: str | None,
    requested_forms: tuple[str, ...],
) -> bool:
    normalized_requested = {
        _normalized_form_prefix(value)
        for value in requested_forms
        if str(value or "").strip()
    }
    if not normalized_requested:
        return True
    return _normalized_form_prefix(form_type) in normalized_requested


def build_primary_document_url(
    cik: str,
    accession_no: str,
    primary_document: str | None,
) -> str | None:
    document_name = str(primary_document or "").strip()
    if not document_name:
        return None
    bare_accession = str(accession_no).replace("-", "")
    bare_cik = str(int(normalize_cik(cik)))
    return f"https://www.sec.gov/Archives/edgar/data/{bare_cik}/{bare_accession}/{document_name}"


def extract_submission_issuer_profile(payload: dict[str, Any]) -> SubmissionIssuerProfile:
    tickers = payload.get("tickers")
    exchanges = payload.get("exchanges")
    return SubmissionIssuerProfile(
        cik=normalize_cik(payload.get("cik")),
        company_name=str(payload.get("name") or "").strip(),
        sic=str(payload.get("sic") or "").strip() or None,
        sic_description=str(payload.get("sicDescription") or "").strip() or None,
        naics=str(payload.get("naics") or "").strip() or None,
        tickers=tuple(str(item).upper().strip() for item in tickers or [] if str(item or "").strip()),
        exchanges=tuple(str(item).upper().strip() for item in exchanges or [] if str(item or "").strip()),
    )


def _columnar_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not payload:
        return []
    keys = [str(key) for key in payload.keys()]
    length = max((len(value) for value in payload.values() if isinstance(value, list)), default=0)
    rows: list[dict[str, Any]] = []
    for index in range(length):
        row: dict[str, Any] = {}
        for key in keys:
            values = payload.get(key)
            if not isinstance(values, list):
                continue
            row[key] = values[index] if index < len(values) else None
        rows.append(row)
    return rows


def collect_submission_filing_rows(submissions_payload: dict[str, Any]) -> list[dict[str, Any]]:
    filings_payload = submissions_payload.get("filings")
    if not isinstance(filings_payload, dict):
        return []
    recent_payload = filings_payload.get("recent")
    rows = _columnar_rows(recent_payload if isinstance(recent_payload, dict) else {})
    return [row for row in rows if str(row.get("accessionNumber") or "").strip()]


def normalize_submission_filing_row(
    *,
    cik: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    accession_no = str(row.get("accessionNumber") or "").strip()
    if not accession_no:
        raise ValueError("submission filing row is missing accessionNumber")
    filing_date = parse_date(row.get("filingDate")) if row.get("filingDate") else None
    report_date = parse_date(row.get("reportDate")) if row.get("reportDate") else None
    filed_at = parse_datetime(row.get("filingDate")) or _normalize_datetime_from_filing_date(filing_date)
    accepted_at = parse_datetime(row.get("acceptanceDateTime")) or filed_at
    if filed_at is None or accepted_at is None:
        raise ValueError(f"submission filing row is missing filing timestamps for {accession_no}")
    available_at = accepted_at
    form_type = str(row.get("form") or "").strip()
    return {
        "filing_id": build_filing_id(accession_no),
        "accession_no": accession_no,
        "form_type": form_type,
        "period_end": report_date or filing_date,
        "filed_at": filed_at,
        "accepted_at": accepted_at,
        "available_at": available_at,
        "amendment_flag": form_type.endswith("/A"),
        "amendment_of_accession_no": None,
        "primary_document_url": build_primary_document_url(
            cik,
            accession_no,
            str(row.get("primaryDocument") or "").strip() or None,
        ),
        "primary_xml_url": None,
        "raw_storage_uri": None,
        "raw_sha256": None,
        "parse_status": "metadata_only",
    }


def _fact_hash(
    *,
    filing_id: str,
    taxonomy: str,
    concept_name: str,
    unit: str,
    period_start: date | None,
    period_end: date,
    value_numeric: float | None,
    value_text: str | None,
) -> str:
    payload = "|".join(
        [
            filing_id,
            taxonomy,
            concept_name,
            unit,
            "" if period_start is None else period_start.isoformat(),
            period_end.isoformat(),
            "" if value_numeric is None else f"{value_numeric:.12g}",
            value_text or "",
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _normalize_fact_value(raw_value: Any) -> tuple[float | None, str | None]:
    numeric = parse_float(raw_value)
    if numeric is not None:
        return numeric, None
    rendered = str(raw_value or "").strip()
    return None, rendered or None


def build_xbrl_fact_payloads(
    *,
    issuer_id: str,
    companyfacts_payload: dict[str, Any],
    filings_by_accession_no: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    facts_payload = companyfacts_payload.get("facts")
    if not isinstance(facts_payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    for taxonomy, concept_mapping in facts_payload.items():
        if not isinstance(concept_mapping, dict):
            continue
        for concept_name, concept_payload in concept_mapping.items():
            if not isinstance(concept_payload, dict):
                continue
            units_payload = concept_payload.get("units")
            if not isinstance(units_payload, dict):
                continue
            for unit, entries in units_payload.items():
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    accession_no = str(entry.get("accn") or "").strip()
                    filing_row = filings_by_accession_no.get(accession_no)
                    if filing_row is None:
                        continue
                    end_value = entry.get("end")
                    period_end = parse_date(end_value) if end_value else None
                    if period_end is None:
                        continue
                    start_value = entry.get("start")
                    period_start = parse_date(start_value) if start_value else None
                    filing_id = str(filing_row["filing_id"])
                    value_numeric, value_text = _normalize_fact_value(entry.get("val"))
                    rows.append(
                        {
                            "filing_id": filing_id,
                            "issuer_id": issuer_id,
                            "taxonomy": str(taxonomy),
                            "concept_name": str(concept_name),
                            "unit": str(unit),
                            "period_start": period_start,
                            "period_end": period_end,
                            "instant_flag": period_start is None,
                            "dimensions_json": {},
                            "value_numeric": value_numeric,
                            "value_text": value_text,
                            "decimals": None
                            if entry.get("decimals") in (None, "")
                            else str(entry.get("decimals")),
                            "available_at": filing_row["available_at"],
                            "fact_hash": _fact_hash(
                                filing_id=filing_id,
                                taxonomy=str(taxonomy),
                                concept_name=str(concept_name),
                                unit=str(unit),
                                period_start=period_start,
                                period_end=period_end,
                                value_numeric=value_numeric,
                                value_text=value_text,
                            ),
                        }
                    )
    return rows


def _period_days(period_start: date | None, period_end: date) -> int | None:
    if period_start is None:
        return None
    return max((period_end - period_start).days, 0)


def _period_type(period_start: date | None, period_end: date) -> str:
    days = _period_days(period_start, period_end)
    if days is None:
        return "instant"
    if days >= 300:
        return "annual"
    if 60 <= days <= 120:
        return "quarterly"
    return "duration"


def _metric_sort_key(metric_name: str, fact_row: dict[str, Any]) -> tuple[int, int]:
    preferred_units = PREFERRED_UNITS_BY_METRIC.get(metric_name, set())
    unit_score = 0 if fact_row.get("unit") in preferred_units else 1
    if metric_name in INSTANT_PREFERRED_METRICS:
        instant_score = 0 if fact_row.get("instant_flag") else 1
    else:
        instant_score = 0 if not fact_row.get("instant_flag") else 1
    return (unit_score, instant_score)


def build_statement_period_snapshot_payloads(
    *,
    issuer_id: str,
    fact_rows: list[dict[str, Any]],
    filings_by_filing_id: dict[str, dict[str, Any]],
    normalization_version: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, date, str | None], list[dict[str, Any]]] = defaultdict(list)
    for row in fact_rows:
        filing_id = str(row["filing_id"])
        filing_row = filings_by_filing_id.get(filing_id)
        if filing_row is None:
            continue
        grouped[(filing_id, row["period_end"], str(filing_row.get("form_type") or ""))].append(row)

    snapshots: list[dict[str, Any]] = []
    for (filing_id, period_end, _form_type), rows in grouped.items():
        filing_row = filings_by_filing_id[filing_id]
        filing_period_end = filing_row.get("period_end")
        fiscal_period_end = period_end if isinstance(period_end, date) else filing_period_end
        if fiscal_period_end is None:
            continue
        instant_rows = [row for row in rows if bool(row.get("instant_flag"))]
        duration_groups: dict[date, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            if bool(row.get("instant_flag")):
                continue
            period_start = row.get("period_start")
            if isinstance(period_start, date):
                duration_groups[period_start].append(row)

        snapshot_groups: list[tuple[date | None, str, list[dict[str, Any]]]] = []
        if duration_groups:
            typed_duration_groups: dict[str, list[tuple[date, list[dict[str, Any]]]]] = defaultdict(list)
            for period_start, duration_rows in duration_groups.items():
                typed_duration_groups[_period_type(period_start, fiscal_period_end)].append(
                    (period_start, duration_rows)
                )
            for period_type, typed_rows in typed_duration_groups.items():
                if period_type == "annual":
                    chosen_period_start, chosen_rows = max(
                        typed_rows,
                        key=lambda item: (
                            len(item[1]),
                            _period_days(item[0], fiscal_period_end) or 0,
                        ),
                    )
                else:
                    chosen_period_start, chosen_rows = max(
                        typed_rows,
                        key=lambda item: (
                            len(item[1]),
                            -(_period_days(item[0], fiscal_period_end) or 0),
                        ),
                    )
                snapshot_groups.append(
                    (
                        chosen_period_start,
                        chosen_period_start.isoformat(),
                        [*instant_rows, *chosen_rows],
                    )
                )
        elif instant_rows:
            snapshot_groups.append((None, "instant", list(instant_rows)))

        for period_start, period_start_label, snapshot_rows in snapshot_groups:
            metrics: dict[str, Any] = {}
            source_fact_refs: dict[str, Any] = {}
            concepts = defaultdict(list)
            for row in snapshot_rows:
                concepts[(str(row["taxonomy"]), str(row["concept_name"]))].append(row)
            for metric_name, aliases in CANONICAL_METRIC_CONCEPTS.items():
                selected_row: dict[str, Any] | None = None
                for alias in aliases:
                    candidates = concepts.get(alias, [])
                    if not candidates:
                        continue
                    ordered = sorted(
                        candidates,
                        key=lambda item: _metric_sort_key(metric_name, item),
                    )
                    selected_row = ordered[0]
                    break
                if selected_row is None:
                    continue
                value = selected_row.get("value_numeric")
                if value is None:
                    continue
                metrics[metric_name] = value
                source_fact_refs[metric_name] = {
                    "taxonomy": selected_row["taxonomy"],
                    "concept_name": selected_row["concept_name"],
                    "unit": selected_row["unit"],
                    "fact_hash": selected_row["fact_hash"],
                }
            available_at = filing_row["available_at"]
            fiscal_year = fiscal_period_end.year
            fiscal_period = str(filing_row.get("form_type") or "")
            snapshots.append(
                {
                    "snapshot_id": (
                        f"statement_snapshot:{issuer_id}:{filing_id}:{fiscal_period_end.isoformat()}:"
                        f"{period_start_label}:{normalization_version}"
                    ),
                    "issuer_id": issuer_id,
                    "filing_id": filing_id,
                    "period_type": _period_type(period_start, fiscal_period_end),
                    "fiscal_year": fiscal_year,
                    "fiscal_period": fiscal_period,
                    "period_start": period_start,
                    "period_end": fiscal_period_end,
                    "available_at": available_at,
                    "normalization_version": normalization_version,
                    "metrics_json": metrics,
                    "source_fact_refs_json": source_fact_refs,
                }
            )
    return snapshots


__all__ = [
    "CANONICAL_METRIC_CONCEPTS",
    "SUPPORTED_PERIODIC_FORM_PREFIXES",
    "SubmissionIssuerProfile",
    "build_statement_period_snapshot_payloads",
    "build_xbrl_fact_payloads",
    "collect_submission_filing_rows",
    "extract_submission_issuer_profile",
    "filing_matches_requested_forms",
    "normalize_cik",
    "normalize_submission_filing_row",
]

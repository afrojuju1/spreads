from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.company_valuation.contracts import CompanyValuationContractModel
from core.services.company_valuation.ids import build_issuer_id, build_security_id
from core.services.company_valuation.ingestion.submission_history import (
    filter_submission_filing_payloads,
    load_all_submission_rows,
)
from core.services.company_valuation.normalization import (
    build_statement_period_snapshot_payloads,
    build_xbrl_fact_payloads,
    extract_submission_issuer_profile,
    normalize_cik,
)
from core.services.company_valuation.sec_client import SecEdgarClient
from core.services.company_valuation.templates import (
    default_company_valuation_config_root,
    resolve_company_valuation_template_assignment,
)
from core.storage.company_valuation_repository import CompanyValuationRepository

DEFAULT_COMPANY_VALUATION_NORMALIZATION_VERSION = "v1"


class SecFilingsIngestionRequest(CompanyValuationContractModel):
    cik: str | None = None
    ticker: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    forms: tuple[str, ...] = ("10-K", "10-Q", "20-F", "40-F", "8-K")
    normalization_version: str = DEFAULT_COMPANY_VALUATION_NORMALIZATION_VERSION
    config_root: str | None = None


class SecFilingsIngestionResult(CompanyValuationContractModel):
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    issuer_id: str | None = None
    cik: str | None = None
    filings_seen: int = 0
    filings_persisted: int = 0
    facts_persisted: int = 0
    statement_snapshots_persisted: int = 0
    notes: tuple[str, ...] = ()

def _resolve_request_cik(
    request: SecFilingsIngestionRequest,
    *,
    client: SecEdgarClient,
) -> str:
    if request.cik:
        return normalize_cik(request.cik)
    if request.ticker:
        lookup = client.resolve_ticker(request.ticker)
        return normalize_cik(lookup.cik)
    raise ValueError("cik or ticker is required")

def _issuer_payload_from_profile(
    *,
    issuer_id: str,
    profile: Any,
    config_root: str | None,
    now: datetime,
) -> dict[str, Any]:
    assignment = resolve_company_valuation_template_assignment(
        cik=profile.cik,
        company_name=profile.company_name,
        sic=profile.sic,
        sic_description=profile.sic_description,
        naics=profile.naics,
        config_root=config_root,
    )
    return {
        "issuer_id": issuer_id,
        "cik": profile.cik,
        "company_name": profile.company_name,
        "sic": profile.sic,
        "sic_description": profile.sic_description,
        "naics": profile.naics,
        "template_id": assignment.template.template_id,
        "template_version": assignment.template.template_version,
        "template_assignment_source": assignment.source,
        "template_assignment_reason": assignment.reason,
        "limited_coverage_flag": assignment.limited_coverage_flag,
        "stressed_operator_flag": assignment.stressed_operator_flag,
        "created_at": now,
        "updated_at": now,
    }


def _security_payloads_from_profile(
    *,
    issuer_id: str,
    cik: str,
    profile: Any,
    now: datetime,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    tickers = list(profile.tickers)
    exchanges = list(profile.exchanges)
    for index, ticker in enumerate(tickers):
        payloads.append(
            {
                "security_id": build_security_id(cik, ticker),
                "issuer_id": issuer_id,
                "ticker": ticker,
                "share_class": None,
                "exchange": exchanges[index] if index < len(exchanges) else None,
                "cusip": None,
                "is_primary": index == 0,
                "active_from": None,
                "active_to": None,
                "created_at": now,
            }
        )
    return payloads


def ingest_sec_filings(
    request: SecFilingsIngestionRequest,
    *,
    client: SecEdgarClient | None = None,
    repository: CompanyValuationRepository | None = None,
) -> SecFilingsIngestionResult:
    started_at = datetime.now(UTC)
    sec_client = client or SecEdgarClient()
    repo = repository or CompanyValuationRepository()
    cik = _resolve_request_cik(request, client=sec_client)
    issuer_id = build_issuer_id(cik)

    submissions_payload = sec_client.get_submissions(cik)
    if not isinstance(submissions_payload, dict):
        raise RuntimeError("Unexpected SEC submissions payload shape")
    profile = extract_submission_issuer_profile(submissions_payload)
    all_rows = load_all_submission_rows(sec_client, submissions_payload=submissions_payload)
    filing_payloads = filter_submission_filing_payloads(
        cik=profile.cik,
        rows=all_rows,
        forms=request.forms,
        since=request.since,
        until=request.until,
    )

    now = datetime.now(UTC)
    config_root = request.config_root or str(default_company_valuation_config_root().parent)
    issuer_payload = _issuer_payload_from_profile(
        issuer_id=issuer_id,
        profile=profile,
        config_root=config_root,
        now=now,
    )
    security_payloads = _security_payloads_from_profile(
        issuer_id=issuer_id,
        cik=profile.cik,
        profile=profile,
        now=now,
    )
    for payload in filing_payloads:
        payload["issuer_id"] = issuer_id
        payload["created_at"] = now

    companyfacts_payload = sec_client.get_companyfacts(profile.cik)
    if not isinstance(companyfacts_payload, dict):
        raise RuntimeError("Unexpected SEC companyfacts payload shape")
    filings_by_accession_no = {
        str(payload["accession_no"]): payload
        for payload in filing_payloads
    }
    fact_payloads = build_xbrl_fact_payloads(
        issuer_id=issuer_id,
        companyfacts_payload=companyfacts_payload,
        filings_by_accession_no=filings_by_accession_no,
    )
    parsed_filing_ids = {str(payload["filing_id"]) for payload in fact_payloads}
    for payload in filing_payloads:
        if str(payload["filing_id"]) in parsed_filing_ids:
            payload["parse_status"] = "parsed_companyfacts"
    filings_by_filing_id = {
        str(payload["filing_id"]): payload
        for payload in filing_payloads
    }
    snapshot_payloads = build_statement_period_snapshot_payloads(
        issuer_id=issuer_id,
        fact_rows=fact_payloads,
        filings_by_filing_id=filings_by_filing_id,
        normalization_version=request.normalization_version,
    )
    fact_filing_ids = sorted({str(payload["filing_id"]) for payload in fact_payloads})
    fact_counts = (0, 0)

    repo.upsert_issuer(issuer_payload)
    repo.upsert_securities(security_payloads)
    filings_persisted = repo.upsert_filings(filing_payloads)
    if fact_filing_ids:
        fact_counts = repo.replace_filing_facts_and_snapshots(
            filing_ids=fact_filing_ids,
            fact_payloads=fact_payloads,
            snapshot_payloads=snapshot_payloads,
        )

    completed_at = datetime.now(UTC)
    notes: list[str] = []
    if not security_payloads:
        notes.append("SEC submissions payload did not include any ticker mappings.")
    if not fact_payloads:
        notes.append("No companyfacts rows matched the ingested filing accession numbers.")
    return SecFilingsIngestionResult(
        status="ok",
        source="sec_filings",
        started_at=started_at,
        completed_at=completed_at,
        issuer_id=issuer_id,
        cik=profile.cik,
        filings_seen=len(all_rows),
        filings_persisted=filings_persisted,
        facts_persisted=fact_counts[0],
        statement_snapshots_persisted=fact_counts[1],
        notes=tuple(notes),
    )

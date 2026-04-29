from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from core.services.company_valuation.ids import build_issuer_id
from core.services.company_valuation.ingestion.submission_history import (
    filter_submission_filing_payloads,
    load_all_submission_rows,
)
from core.services.company_valuation.normalization import (
    extract_submission_issuer_profile,
    normalize_cik,
)
from core.services.company_valuation.ownership_xml import parse_form345_xml
from core.services.company_valuation.sec_client import SecEdgarClient
from core.storage.company_valuation_repository import CompanyValuationRepository


@dataclass(frozen=True)
class SecInsidersIngestionRequest:
    cik: str | None = None
    ticker: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    forms: tuple[str, ...] = ("3", "4", "5")


@dataclass(frozen=True)
class SecInsidersIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    issuer_id: str | None = None
    cik: str | None = None
    filings_seen: int = 0
    filings_persisted: int = 0
    owners_persisted: int = 0
    transactions_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def _resolve_request_cik(
    request: SecInsidersIngestionRequest,
    *,
    client: SecEdgarClient,
) -> str:
    if request.cik:
        return normalize_cik(request.cik)
    if request.ticker:
        lookup = client.resolve_ticker(request.ticker)
        return normalize_cik(lookup.cik)
    raise ValueError("cik or ticker is required")


def ingest_sec_insiders(
    request: SecInsidersIngestionRequest,
    *,
    client: SecEdgarClient | None = None,
    repository: CompanyValuationRepository | None = None,
) -> SecInsidersIngestionResult:
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
    for payload in filing_payloads:
        payload["issuer_id"] = issuer_id
        payload["created_at"] = now

    owner_payloads: dict[str, dict[str, object]] = {}
    transaction_payloads: list[dict[str, object]] = []
    parsed_filing_ids: list[str] = []
    notes: list[str] = []

    for filing_payload in filing_payloads:
        accession_no = str(filing_payload["accession_no"])
        xml_url = sec_client.resolve_filing_xml_url(
            cik=profile.cik,
            accession_no=accession_no,
            primary_document_name=str(filing_payload.get("primary_document_url") or "").rsplit("/", 1)[-1],
            form_type=str(filing_payload.get("form_type") or ""),
        )
        if not xml_url:
            notes.append(f"No XML document found for insider filing {accession_no}.")
            continue
        xml_text = sec_client.get_text_url(xml_url)
        parsed = parse_form345_xml(
            xml_text=xml_text,
            issuer_id=issuer_id,
            filing_id=str(filing_payload["filing_id"]),
            filing_available_at=filing_payload["available_at"],
            created_at=now,
        )
        for owner_payload in parsed.owner_payloads:
            owner_payloads[str(owner_payload["holder_id"])] = owner_payload
        transaction_payloads.extend(parsed.transaction_payloads)
        filing_payload["primary_xml_url"] = xml_url
        filing_payload["parse_status"] = "parsed_xml"
        parsed_filing_ids.append(str(filing_payload["filing_id"]))

    repo.upsert_filings(filing_payloads)
    repo.upsert_beneficial_owners(list(owner_payloads.values()))
    transactions_persisted = repo.replace_insider_transactions_for_filings(
        filing_ids=parsed_filing_ids,
        transaction_payloads=transaction_payloads,
    )

    completed_at = datetime.now(UTC)
    return SecInsidersIngestionResult(
        status="ok",
        source="sec_insiders",
        started_at=started_at,
        completed_at=completed_at,
        issuer_id=issuer_id,
        cik=profile.cik,
        filings_seen=len(all_rows),
        filings_persisted=len(filing_payloads),
        owners_persisted=len(owner_payloads),
        transactions_persisted=transactions_persisted,
        notes=tuple(notes),
    )

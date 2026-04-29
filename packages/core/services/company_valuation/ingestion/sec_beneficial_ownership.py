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
from core.services.company_valuation.ownership_xml import parse_schedule_13d_g_xml
from core.services.company_valuation.sec_client import SecEdgarClient, SecRequestError
from core.storage.company_valuation_repository import CompanyValuationRepository


@dataclass(frozen=True)
class SecBeneficialOwnershipIngestionRequest:
    cik: str | None = None
    ticker: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    schedules: tuple[str, ...] = (
        "SC 13D",
        "SC 13D/A",
        "SC 13G",
        "SC 13G/A",
        "SCHEDULE 13D",
        "SCHEDULE 13D/A",
        "SCHEDULE 13G",
        "SCHEDULE 13G/A",
    )


@dataclass(frozen=True)
class SecBeneficialOwnershipIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    issuer_id: str | None = None
    cik: str | None = None
    filings_seen: int = 0
    filings_persisted: int = 0
    owners_persisted: int = 0
    positions_persisted: int = 0
    groups_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def _resolve_request_cik(
    request: SecBeneficialOwnershipIngestionRequest,
    *,
    client: SecEdgarClient,
) -> str:
    if request.cik:
        return normalize_cik(request.cik)
    if request.ticker:
        lookup = client.resolve_ticker(request.ticker)
        return normalize_cik(lookup.cik)
    raise ValueError("cik or ticker is required")


def ingest_sec_beneficial_ownership(
    request: SecBeneficialOwnershipIngestionRequest,
    *,
    client: SecEdgarClient | None = None,
    repository: CompanyValuationRepository | None = None,
) -> SecBeneficialOwnershipIngestionResult:
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
        forms=request.schedules,
        since=request.since,
        until=request.until,
    )

    now = datetime.now(UTC)
    for payload in filing_payloads:
        payload["issuer_id"] = issuer_id
        payload["created_at"] = now

    owner_payloads: dict[str, dict[str, object]] = {}
    beneficial_filing_payloads: list[dict[str, object]] = []
    group_payloads: list[dict[str, object]] = []
    membership_payloads: list[dict[str, object]] = []
    position_payloads: list[dict[str, object]] = []
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
            notes.append(f"No XML document found for beneficial ownership filing {accession_no}.")
            continue
        try:
            xml_text = sec_client.get_text_url(xml_url)
        except SecRequestError as exc:
            notes.append(
                f"Skipping beneficial ownership filing {accession_no}: unable to fetch XML ({exc.status_code or 'request_error'})."
            )
            continue
        parsed = parse_schedule_13d_g_xml(
            xml_text=xml_text,
            issuer_id=issuer_id,
            issuer_cik=profile.cik,
            filing_id=str(filing_payload["filing_id"]),
            filing_available_at=filing_payload["available_at"],
            created_at=now,
            schedule_type=str(filing_payload["form_type"]),
        )
        for owner_payload in parsed.owner_payloads:
            owner_payloads[str(owner_payload["holder_id"])] = owner_payload
        beneficial_filing_payloads.append(parsed.filing_payload)
        group_payloads.extend(parsed.group_payloads)
        membership_payloads.extend(parsed.membership_payloads)
        position_payloads.extend(parsed.position_payloads)
        filing_payload["primary_xml_url"] = xml_url
        filing_payload["parse_status"] = "parsed_xml"
        parsed_filing_ids.append(str(filing_payload["filing_id"]))

    repo.upsert_filings(filing_payloads)
    repo.upsert_beneficial_owners(list(owner_payloads.values()))
    filing_count, group_count, _membership_count, position_count = (
        repo.replace_beneficial_ownership_for_filings(
            filing_ids=parsed_filing_ids,
            filing_payloads=beneficial_filing_payloads,
            group_payloads=group_payloads,
            membership_payloads=membership_payloads,
            position_payloads=position_payloads,
        )
    )

    completed_at = datetime.now(UTC)
    return SecBeneficialOwnershipIngestionResult(
        status="ok",
        source="sec_beneficial_ownership",
        started_at=started_at,
        completed_at=completed_at,
        issuer_id=issuer_id,
        cik=profile.cik,
        filings_seen=len(all_rows),
        filings_persisted=filing_count,
        owners_persisted=len(owner_payloads),
        positions_persisted=position_count,
        groups_persisted=group_count,
        notes=tuple(notes),
    )

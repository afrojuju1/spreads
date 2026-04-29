from __future__ import annotations

import csv
import io
import zipfile
from calendar import monthrange
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from core.services.company_valuation.identifiers import (
    load_official_13f_list,
    resolve_cusip_to_security,
    seed_security_identifier_history_from_official_list,
)
from core.services.company_valuation.ids import (
    build_institutional_filing_id,
    build_institutional_holder_id,
    normalize_cik,
    normalize_cusip,
    normalize_name,
)
from core.services.company_valuation.openfigi_client import OpenFigiClient
from core.services.company_valuation.sec_client import SecEdgarClient
from core.storage.company_valuation_repository import CompanyValuationRepository


@dataclass(frozen=True)
class Sec13FIngestionRequest:
    report_period: date | None = None
    manager_cik: str | None = None


@dataclass(frozen=True)
class Sec13FIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    report_period: date | None = None
    dataset_url: str | None = None
    official_list_url: str | None = None
    filings_seen: int = 0
    filings_persisted: int = 0
    holders_persisted: int = 0
    positions_persisted: int = 0
    identifier_mappings_persisted: int = 0
    unresolved_positions: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def _parse_dataset_date(value: str | None) -> date | None:
    rendered = str(value or "").strip()
    if not rendered:
        return None
    try:
        return datetime.strptime(rendered, "%d-%b-%Y").date()
    except ValueError:
        try:
            return date.fromisoformat(rendered)
        except ValueError:
            return None


def _parse_int(value: Any) -> int | None:
    rendered = str(value or "").replace(",", "").strip()
    if not rendered:
        return None
    try:
        return int(float(rendered))
    except ValueError:
        return None


def _parse_float(value: Any) -> float | None:
    rendered = str(value or "").replace(",", "").strip()
    if not rendered:
        return None
    try:
        return float(rendered)
    except ValueError:
        return None


def _parse_flag(value: Any) -> bool | None:
    rendered = str(value or "").strip().upper()
    if not rendered:
        return None
    if rendered in {"Y", "YES", "TRUE", "T", "1"}:
        return True
    if rendered in {"N", "NO", "FALSE", "F", "0"}:
        return False
    return None


def _parse_other_manager_refs(value: Any) -> list[str]:
    rendered = str(value or "").strip()
    if not rendered:
        return []
    normalized = rendered.replace(";", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def _quarter_end(value: date) -> date:
    quarter = ((value.month - 1) // 3) + 1
    month = quarter * 3
    return date(value.year, month, monthrange(value.year, month)[1])


def _previous_quarter_end(value: date) -> date:
    quarter = ((value.month - 1) // 3) + 1
    current_quarter_start_month = ((quarter - 1) * 3) + 1
    current_quarter_start = date(value.year, current_quarter_start_month, 1)
    prior_day = current_quarter_start - timedelta(days=1)
    return _quarter_end(prior_day)


def _latest_available_report_period(as_of: date | None = None) -> date:
    today = as_of or datetime.now(UTC).date()
    candidate = _previous_quarter_end(today)
    filing_deadline = candidate + timedelta(days=45)
    if today >= filing_deadline:
        return candidate
    return _previous_quarter_end(candidate)


def _dataset_window_for_report_period(report_period: date) -> tuple[date, date]:
    start = date(report_period.year, report_period.month, 1)
    end_month = report_period.month + 2
    end_year = report_period.year
    while end_month > 12:
        end_month -= 12
        end_year += 1
    end = date(end_year, end_month, monthrange(end_year, end_month)[1])
    return (start, end)


def build_13f_dataset_url(
    report_period: date,
    *,
    base_url: str = "https://www.sec.gov",
) -> str:
    start, end = _dataset_window_for_report_period(report_period)
    start_token = start.strftime("%d%b%Y").lower()
    end_token = end.strftime("%d%b%Y").lower()
    return (
        f"{base_url.rstrip('/')}/files/structureddata/data/form-13f-data-sets/"
        f"{start_token}-{end_token}_form13f.zip"
    )


def _zip_tsv_rows(
    archive: zipfile.ZipFile,
    name: str,
) -> csv.DictReader[str]:
    binary = archive.open(name)
    text = io.TextIOWrapper(binary, encoding="utf-8", newline="")
    return csv.DictReader(text, delimiter="\t")


def _read_accession_map(
    archive: zipfile.ZipFile,
    name: str,
    *,
    accessions: set[str] | None = None,
) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    reader = _zip_tsv_rows(archive, name)
    for row in reader:
        accession_no = str(row.get("ACCESSION_NUMBER") or "").strip()
        if not accession_no:
            continue
        if accessions is not None and accession_no not in accessions:
            continue
        rows[accession_no] = row
    return rows


def _normalize_submission_type(value: str | None) -> str:
    return str(value or "").strip().upper()


def _filing_datetime(filing_date: date | None) -> datetime:
    if filing_date is None:
        return datetime.now(UTC)
    return datetime.combine(filing_date, time.min, tzinfo=UTC)


def _institutional_holder_payload(
    *,
    manager_name: str,
    manager_cik: str | None,
    now: datetime,
) -> dict[str, object]:
    holder_id = build_institutional_holder_id(manager_name, manager_cik)
    return {
        "institutional_holder_id": holder_id,
        "manager_cik": manager_cik,
        "manager_name": manager_name,
        "normalized_name": normalize_name(manager_name),
        "created_at": now,
        "updated_at": now,
    }


def _institutional_filing_payload(
    *,
    accession_no: str,
    manager_cik: str | None,
    manager_name: str,
    submission_row: dict[str, str],
    cover_row: dict[str, str] | None,
    summary_row: dict[str, str] | None,
    report_period: date,
    dataset_url: str,
    sec_client: SecEdgarClient,
    now: datetime,
) -> dict[str, object]:
    submission_type = _normalize_submission_type(submission_row.get("SUBMISSIONTYPE"))
    filing_date = _parse_dataset_date(submission_row.get("FILING_DATE")) or report_period
    filing_at = _filing_datetime(filing_date)
    institutional_holder_id = build_institutional_holder_id(manager_name, manager_cik)
    primary_document_url = None
    if manager_cik:
        try:
            primary_document_url = sec_client.filing_index_url(manager_cik, accession_no)
        except ValueError:
            primary_document_url = None
    return {
        "filing_id": build_institutional_filing_id(accession_no),
        "institutional_holder_id": institutional_holder_id,
        "accession_no": accession_no,
        "manager_cik": manager_cik,
        "manager_name": manager_name,
        "submission_type": submission_type,
        "report_period": report_period,
        "filed_at": filing_at,
        "accepted_at": filing_at,
        "available_at": filing_at,
        "amendment_flag": submission_type.endswith("/A"),
        "amendment_no": _parse_int((cover_row or {}).get("AMENDMENTNO")),
        "report_type": str((cover_row or {}).get("REPORTTYPE") or "").strip() or None,
        "form13f_file_number": str((cover_row or {}).get("FORM13FFILENUMBER") or "").strip() or None,
        "crd_number": str((cover_row or {}).get("CRDNUMBER") or "").strip() or None,
        "sec_file_number": str((cover_row or {}).get("SECFILENUMBER") or "").strip() or None,
        "other_included_managers_count": _parse_int((summary_row or {}).get("OTHERINCLUDEDMANAGERSCOUNT")),
        "table_entry_total": _parse_int((summary_row or {}).get("TABLEENTRYTOTAL")),
        "table_value_total": _parse_float((summary_row or {}).get("TABLEVALUETOTAL")),
        "is_confidential_omitted": _parse_flag((summary_row or {}).get("ISCONFIDENTIALOMITTED")),
        "additional_information": str((cover_row or {}).get("ADDITIONALINFORMATION") or "").strip() or None,
        "primary_document_url": primary_document_url,
        "information_table_url": None,
        "source_dataset_url": dataset_url,
        "parse_status": "parsed_dataset"
        if submission_type in {"13F-HR", "13F-HR/A"}
        else "metadata_only",
        "created_at": now,
    }


def ingest_sec_13f(
    request: Sec13FIngestionRequest,
    *,
    client: SecEdgarClient | None = None,
    repository: CompanyValuationRepository | None = None,
    openfigi_client: OpenFigiClient | None = None,
) -> Sec13FIngestionResult:
    started_at = datetime.now(UTC)
    sec_client = client or SecEdgarClient()
    repo = repository or CompanyValuationRepository()
    figi_client = openfigi_client or OpenFigiClient()

    report_period = request.report_period or _latest_available_report_period()
    manager_cik = normalize_cik(request.manager_cik) if request.manager_cik else None
    dataset_url = build_13f_dataset_url(report_period, base_url=sec_client.www_base_url)
    official_entries = load_official_13f_list(report_period=report_period, client=sec_client)
    official_list_url = next(iter(official_entries.values())).source_url if official_entries else None
    seeded_identifier_count = seed_security_identifier_history_from_official_list(
        report_period=report_period,
        repository=repo,
        client=sec_client,
    )
    known_issuer_names = {
        normalize_name(str(row.get("company_name") or ""))
        for row in repo.list_issuers()
        if str(row.get("company_name") or "").strip()
    }

    archive_bytes = sec_client.get_bytes_url(
        dataset_url,
        headers={"Accept": "application/octet-stream"},
    )
    archive = zipfile.ZipFile(io.BytesIO(archive_bytes))
    submission_rows = _read_accession_map(archive, "SUBMISSION.tsv")

    targeted_submissions: dict[str, dict[str, str]] = {}
    for accession_no, row in submission_rows.items():
        row_report_period = _parse_dataset_date(row.get("PERIODOFREPORT"))
        if row_report_period != report_period:
            continue
        row_manager_cik = normalize_cik(row.get("CIK")) if str(row.get("CIK") or "").strip() else None
        if manager_cik and row_manager_cik != manager_cik:
            continue
        targeted_submissions[accession_no] = row

    target_accessions = set(targeted_submissions.keys())
    cover_rows = _read_accession_map(archive, "COVERPAGE.tsv", accessions=target_accessions)
    summary_rows = _read_accession_map(archive, "SUMMARYPAGE.tsv", accessions=target_accessions)

    now = datetime.now(UTC)
    holder_payloads: dict[str, dict[str, object]] = {}
    filing_payloads: dict[str, dict[str, object]] = {}
    hr_filing_ids: list[str] = []
    target_hr_accessions: set[str] = set()
    for accession_no, submission_row in targeted_submissions.items():
        cover_row = cover_rows.get(accession_no) or {}
        summary_row = summary_rows.get(accession_no) or {}
        normalized_manager_cik = (
            normalize_cik(submission_row.get("CIK"))
            if str(submission_row.get("CIK") or "").strip()
            else None
        )
        manager_name = str(cover_row.get("FILINGMANAGER_NAME") or "").strip() or (
            f"Manager {normalized_manager_cik or accession_no}"
        )
        holder_payload = _institutional_holder_payload(
            manager_name=manager_name,
            manager_cik=normalized_manager_cik,
            now=now,
        )
        holder_payloads[str(holder_payload["institutional_holder_id"])] = holder_payload
        filing_payload = _institutional_filing_payload(
            accession_no=accession_no,
            manager_cik=normalized_manager_cik,
            manager_name=manager_name,
            submission_row=submission_row,
            cover_row=cover_row,
            summary_row=summary_row,
            report_period=report_period,
            dataset_url=dataset_url,
            sec_client=sec_client,
            now=now,
        )
        filing_payloads[str(filing_payload["filing_id"])] = filing_payload
        if str(filing_payload["submission_type"]) in {"13F-HR", "13F-HR/A"}:
            hr_filing_ids.append(str(filing_payload["filing_id"]))
            target_hr_accessions.add(accession_no)

    identifier_payloads: dict[str, dict[str, object]] = {}
    security_patch_payloads: dict[str, dict[str, object]] = {}
    position_payloads: list[dict[str, object]] = []
    resolution_cache: dict[str, Any] = {}
    unresolved_positions = 0

    reader = _zip_tsv_rows(archive, "INFOTABLE.tsv")
    for row in reader:
        accession_no = str(row.get("ACCESSION_NUMBER") or "").strip()
        if accession_no not in target_hr_accessions:
            continue
        cusip_raw = str(row.get("CUSIP") or "").strip()
        if not cusip_raw:
            unresolved_positions += 1
            continue
        try:
            cusip = normalize_cusip(cusip_raw)
        except ValueError:
            unresolved_positions += 1
            continue
        resolution = resolution_cache.get(cusip)
        if cusip not in resolution_cache:
            issuer_name_reported = str(row.get("NAMEOFISSUER") or "").strip() or None
            official_entry = official_entries.get(cusip)
            allow_openfigi_fallback = any(
                candidate in known_issuer_names
                for candidate in (
                    normalize_name(issuer_name_reported) if issuer_name_reported else "",
                    normalize_name(official_entry.issuer_name) if official_entry else "",
                )
                if candidate
            )
            resolution = resolve_cusip_to_security(
                cusip=cusip,
                issuer_name_reported=issuer_name_reported,
                title_of_class=str(row.get("TITLEOFCLASS") or "").strip() or None,
                figi=str(row.get("FIGI") or "").strip() or None,
                report_period=report_period,
                repository=repo,
                official_entries=official_entries,
                openfigi_client=figi_client,
                allow_openfigi_fallback=allow_openfigi_fallback,
            )
            resolution_cache[cusip] = resolution
        if resolution is None:
            unresolved_positions += 1
            continue
        for payload in resolution.identifier_history_payloads:
            identifier_payloads[str(payload["security_identifier_id"])] = payload
        if resolution.security_payload:
            security_patch_payloads[str(resolution.security_payload["security_id"])] = resolution.security_payload
        institutional_filing_id = build_institutional_filing_id(accession_no)
        filing_payload = filing_payloads[institutional_filing_id]
        position_payloads.append(
            {
                "institutional_holder_id": str(filing_payload["institutional_holder_id"]),
                "issuer_id": resolution.issuer_id,
                "filing_id": institutional_filing_id,
                "report_period": report_period,
                "available_at": filing_payload["available_at"],
                "issuer_name_reported": str(row.get("NAMEOFISSUER") or "").strip() or None,
                "title_of_class": str(row.get("TITLEOFCLASS") or "").strip() or None,
                "cusip": cusip,
                "figi": str(row.get("FIGI") or "").strip() or None,
                "share_count": _parse_float(row.get("SSHPRNAMT")),
                "market_value_reported": _parse_float(row.get("VALUE")),
                "put_call": str(row.get("PUTCALL") or "").strip() or None,
                "discretion_type": str(row.get("INVESTMENTDISCRETION") or "").strip() or None,
                "other_manager_refs_json": _parse_other_manager_refs(row.get("OTHERMANAGER")),
                "voting_authority_sole": _parse_float(row.get("VOTING_AUTH_SOLE")),
                "voting_authority_shared": _parse_float(row.get("VOTING_AUTH_SHARED")),
                "voting_authority_none": _parse_float(row.get("VOTING_AUTH_NONE")),
                "resolution_source": resolution.resolution_source,
                "resolution_confidence": resolution.resolution_confidence,
            }
        )

    if security_patch_payloads:
        repo.upsert_securities(list(security_patch_payloads.values()))
    identifier_count = seeded_identifier_count
    if identifier_payloads:
        identifier_count += repo.upsert_security_identifier_history(
            list(identifier_payloads.values())
        )
    holders_persisted = repo.upsert_institutional_holders(list(holder_payloads.values()))
    filings_persisted = repo.upsert_institutional_filings(list(filing_payloads.values()))
    positions_persisted = repo.replace_institutional_positions_for_filings(
        filing_ids=hr_filing_ids,
        position_payloads=position_payloads,
    )

    completed_at = datetime.now(UTC)
    return Sec13FIngestionResult(
        status="ok",
        source="sec_13f",
        started_at=started_at,
        completed_at=completed_at,
        report_period=report_period,
        dataset_url=dataset_url,
        official_list_url=official_list_url,
        filings_seen=len(targeted_submissions),
        filings_persisted=filings_persisted,
        holders_persisted=holders_persisted,
        positions_persisted=positions_persisted,
        identifier_mappings_persisted=identifier_count,
        unresolved_positions=unresolved_positions,
        notes=tuple(),
    )


__all__ = [
    "Sec13FIngestionRequest",
    "Sec13FIngestionResult",
    "build_13f_dataset_url",
    "ingest_sec_13f",
]

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.company_valuation.normalization import (
    collect_submission_filing_rows,
    filing_matches_requested_forms,
    normalize_submission_filing_row,
)
from core.services.company_valuation.sec_client import SecEdgarClient


def _ensure_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def load_all_submission_rows(
    client: SecEdgarClient,
    *,
    submissions_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = collect_submission_filing_rows(submissions_payload)
    filings_payload = submissions_payload.get("filings")
    if not isinstance(filings_payload, dict):
        return rows
    extra_files = filings_payload.get("files")
    if not isinstance(extra_files, list):
        return rows
    for item in extra_files:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("name") or "").strip()
        if not filename:
            continue
        extra_payload = client.get_submissions_file(filename)
        if not isinstance(extra_payload, dict):
            continue
        rows.extend(collect_submission_filing_rows({"filings": {"recent": extra_payload}}))
    return rows


def filter_submission_filing_payloads(
    *,
    cik: str,
    rows: list[dict[str, Any]],
    forms: tuple[str, ...],
    since: datetime | None,
    until: datetime | None,
) -> list[dict[str, Any]]:
    normalized_since = _ensure_datetime(since)
    normalized_until = _ensure_datetime(until)
    payloads: list[dict[str, Any]] = []
    seen_accessions: set[str] = set()
    for row in rows:
        form_type = str(row.get("form") or "").strip()
        if not filing_matches_requested_forms(form_type, forms):
            continue
        payload = normalize_submission_filing_row(cik=cik, row=row)
        available_at = payload["available_at"]
        if normalized_since is not None and available_at < normalized_since:
            continue
        if normalized_until is not None and available_at > normalized_until:
            continue
        accession_no = str(payload["accession_no"])
        if accession_no in seen_accessions:
            continue
        seen_accessions.add(accession_no)
        payloads.append(payload)
    payloads.sort(
        key=lambda item: (
            item["available_at"],
            item["accepted_at"],
            item["accession_no"],
        )
    )
    return payloads


__all__ = [
    "filter_submission_filing_payloads",
    "load_all_submission_rows",
]

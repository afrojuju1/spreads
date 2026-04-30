from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from core.services.company_valuation.normalization import extract_submission_issuer_profile
from core.services.company_valuation.sec_client import SecEdgarClient
from core.services.company_valuation.taxonomy import (
    supported_company_valuation_tickers,
)
from core.services.company_valuation.taxonomy_sync import (
    CompanyValuationTaxonomySyncRequest,
    sync_company_valuation_taxonomy_state,
)
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.services.company_valuation.ids import normalize_ticker


@dataclass(frozen=True)
class CompanyValuationClassificationBackfillRequest:
    tickers: tuple[str, ...] | None = None
    ciks: tuple[str, ...] | None = None
    issuer_ids: tuple[str, ...] | None = None
    issuer_limit: int | None = None
    supported_only: bool = False
    missing_only: bool = True
    sync_taxonomy_shadow: bool = True
    taxonomy_output_root: str | None = None
    sample_limit: int = 20
    continue_on_error: bool = True


@dataclass(frozen=True)
class CompanyValuationClassificationBackfillSample:
    issuer_id: str
    cik: str
    ticker: str | None
    company_name: str
    previous_sic: str | None
    next_sic: str | None
    previous_sic_description: str | None
    next_sic_description: str | None
    previous_naics: str | None
    next_naics: str | None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationClassificationBackfillResult:
    status: str
    started_at: datetime
    completed_at: datetime
    issuers_considered: int
    issuers_requested: int
    sec_profiles_loaded: int
    issuers_updated: int
    sic_updates: int
    sic_description_updates: int
    naics_updates: int
    unchanged_count: int
    skipped_count: int
    samples: tuple[CompanyValuationClassificationBackfillSample, ...] = ()
    errors: tuple[str, ...] = ()
    taxonomy_sync: dict[str, Any] | None = None
    notes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["samples"] = [row.to_payload() for row in self.samples]
        return payload


def _heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is not None:
        heartbeat()


def _has_raw_classification(issuer_row: dict[str, Any]) -> bool:
    return bool(issuer_row.get("sic")) and bool(issuer_row.get("sic_description"))


def _normalize_optional_text(value: Any) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def _resolved_ticker_scope(
    *,
    request_tickers: tuple[str, ...] | None,
    supported_only: bool,
) -> tuple[str, ...] | None:
    normalized_request = tuple(
        dict.fromkeys(
            normalize_ticker(value)
            for value in (request_tickers or ())
            if str(value or "").strip()
        )
    )
    if not supported_only:
        return normalized_request or None
    supported_tickers = supported_company_valuation_tickers()
    if not normalized_request:
        return supported_tickers
    supported_set = set(supported_tickers)
    return tuple(ticker for ticker in normalized_request if ticker in supported_set)


def _sample_from_change(
    *,
    issuer_row: dict[str, Any],
    previous_sic: str | None,
    next_sic: str | None,
    previous_sic_description: str | None,
    next_sic_description: str | None,
    previous_naics: str | None,
    next_naics: str | None,
) -> CompanyValuationClassificationBackfillSample:
    return CompanyValuationClassificationBackfillSample(
        issuer_id=str(issuer_row["issuer_id"]),
        cik=str(issuer_row["cik"]),
        ticker=str(issuer_row.get("ticker") or "") or None,
        company_name=str(issuer_row["company_name"]),
        previous_sic=previous_sic,
        next_sic=next_sic,
        previous_sic_description=previous_sic_description,
        next_sic_description=next_sic_description,
        previous_naics=previous_naics,
        next_naics=next_naics,
    )


def backfill_company_valuation_raw_classification(
    request: CompanyValuationClassificationBackfillRequest,
    *,
    repository: CompanyValuationRepository | None = None,
    client: SecEdgarClient | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> CompanyValuationClassificationBackfillResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    sec_client = client or SecEdgarClient()

    resolved_tickers = _resolved_ticker_scope(
        request_tickers=request.tickers,
        supported_only=request.supported_only,
    )
    if request.supported_only and not resolved_tickers:
        issuer_rows = []
    else:
        issuer_rows = repo.list_issuers(
            issuer_ids=request.issuer_ids,
            ciks=request.ciks,
            tickers=resolved_tickers,
            has_raw_classification=False if request.missing_only else None,
            limit=request.issuer_limit,
        )
    issuers_requested = len(issuer_rows)
    samples: list[CompanyValuationClassificationBackfillSample] = []
    errors: list[str] = []
    updated_issuer_ids: list[str] = []
    sec_profiles_loaded = 0
    issuers_updated = 0
    sic_updates = 0
    sic_description_updates = 0
    naics_updates = 0
    unchanged_count = 0
    skipped_count = 0

    for issuer_row in issuer_rows:
        if request.missing_only and _has_raw_classification(issuer_row):
            skipped_count += 1
            continue
        _heartbeat(heartbeat)
        try:
            submissions_payload = sec_client.get_submissions(str(issuer_row["cik"]))
            profile = extract_submission_issuer_profile(submissions_payload)
            sec_profiles_loaded += 1
            previous_sic = _normalize_optional_text(issuer_row.get("sic"))
            previous_sic_description = _normalize_optional_text(
                issuer_row.get("sic_description")
            )
            previous_naics = _normalize_optional_text(issuer_row.get("naics"))
            next_sic = _normalize_optional_text(profile.sic)
            next_sic_description = _normalize_optional_text(profile.sic_description)
            next_naics = _normalize_optional_text(profile.naics)

            changed = False
            payload: dict[str, Any] = {
                "issuer_id": str(issuer_row["issuer_id"]),
                "updated_at": datetime.now(UTC),
            }
            if next_sic != previous_sic:
                payload["sic"] = next_sic
                changed = True
                sic_updates += 1
            if next_sic_description != previous_sic_description:
                payload["sic_description"] = next_sic_description
                changed = True
                sic_description_updates += 1
            if next_naics != previous_naics:
                payload["naics"] = next_naics
                changed = True
                naics_updates += 1

            if not changed:
                unchanged_count += 1
                continue

            repo.upsert_issuer(payload)
            issuers_updated += 1
            updated_issuer_ids.append(str(issuer_row["issuer_id"]))
            if len(samples) < request.sample_limit:
                samples.append(
                    _sample_from_change(
                        issuer_row=issuer_row,
                        previous_sic=previous_sic,
                        next_sic=next_sic,
                        previous_sic_description=previous_sic_description,
                        next_sic_description=next_sic_description,
                        previous_naics=previous_naics,
                        next_naics=next_naics,
                    )
                )
        except Exception as exc:
            message = f"{issuer_row.get('ticker') or issuer_row['issuer_id']}: {exc}"
            errors.append(message)
            if not request.continue_on_error:
                raise

    taxonomy_sync_payload = None
    if request.sync_taxonomy_shadow and updated_issuer_ids:
        _heartbeat(heartbeat)
        taxonomy_sync_result = sync_company_valuation_taxonomy_state(
            CompanyValuationTaxonomySyncRequest(
                issuer_ids=tuple(updated_issuer_ids),
                output_root=request.taxonomy_output_root,
                sample_limit=request.sample_limit,
            ),
            repository=repo,
            heartbeat=heartbeat,
        )
        taxonomy_sync_payload = taxonomy_sync_result.to_payload()

    completed_at = datetime.now(UTC)
    notes: list[str] = []
    if request.missing_only:
        notes.append("Missing-only mode skips issuers that already have SIC and SIC description.")
    if request.supported_only:
        notes.append("Supported-only mode restricts the backfill to the curated supported issuer universe.")
    if not issuer_rows:
        notes.append("No issuers matched the requested classification backfill scope.")
    return CompanyValuationClassificationBackfillResult(
        status="ok" if not errors else "partial_ok",
        started_at=started_at,
        completed_at=completed_at,
        issuers_considered=issuers_requested - skipped_count,
        issuers_requested=issuers_requested,
        sec_profiles_loaded=sec_profiles_loaded,
        issuers_updated=issuers_updated,
        sic_updates=sic_updates,
        sic_description_updates=sic_description_updates,
        naics_updates=naics_updates,
        unchanged_count=unchanged_count,
        skipped_count=skipped_count,
        samples=tuple(samples),
        errors=tuple(errors),
        taxonomy_sync=taxonomy_sync_payload,
        notes=tuple(notes),
    )


__all__ = [
    "CompanyValuationClassificationBackfillRequest",
    "CompanyValuationClassificationBackfillResult",
    "CompanyValuationClassificationBackfillSample",
    "backfill_company_valuation_raw_classification",
]

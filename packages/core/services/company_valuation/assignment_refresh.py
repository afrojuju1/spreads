from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from core.services.company_valuation.contracts import CompanyValuationContractModel
from core.services.company_valuation.ids import normalize_ticker
from core.services.company_valuation.taxonomy import supported_company_valuation_tickers
from core.services.company_valuation.templates import (
    resolve_company_valuation_template_assignment,
)
from core.storage.company_valuation_repository import CompanyValuationRepository


class CompanyValuationTemplateAssignmentRefreshRequest(CompanyValuationContractModel):
    tickers: tuple[str, ...] | None = None
    ciks: tuple[str, ...] | None = None
    issuer_ids: tuple[str, ...] | None = None
    issuer_limit: int | None = None
    supported_only: bool = False
    sample_limit: int = 20
    continue_on_error: bool = True
    config_root: str | None = None


class CompanyValuationTemplateAssignmentRefreshSample(CompanyValuationContractModel):
    issuer_id: str
    cik: str
    ticker: str | None
    company_name: str
    previous_template_id: str | None
    next_template_id: str
    previous_assignment_source: str | None
    next_assignment_source: str
    previous_stressed_operator_flag: bool
    next_stressed_operator_flag: bool


class CompanyValuationTemplateAssignmentRefreshResult(CompanyValuationContractModel):
    status: str
    started_at: datetime
    completed_at: datetime
    issuers_requested: int
    issuers_considered: int
    issuers_updated: int
    unchanged_count: int
    errors: tuple[str, ...] = ()
    samples: tuple[CompanyValuationTemplateAssignmentRefreshSample, ...] = ()
    notes: tuple[str, ...] = ()


def _heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is not None:
        heartbeat()


def _resolved_ticker_scope(
    *,
    request_tickers: tuple[str, ...] | None,
    supported_only: bool,
    config_root: str | None,
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
    supported_tickers = supported_company_valuation_tickers(config_root)
    if not normalized_request:
        return supported_tickers
    supported_set = set(supported_tickers)
    return tuple(ticker for ticker in normalized_request if ticker in supported_set)


def refresh_company_valuation_template_assignments(
    request: CompanyValuationTemplateAssignmentRefreshRequest,
    *,
    repository: CompanyValuationRepository | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> CompanyValuationTemplateAssignmentRefreshResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    resolved_tickers = _resolved_ticker_scope(
        request_tickers=request.tickers,
        supported_only=request.supported_only,
        config_root=request.config_root,
    )
    if request.supported_only and not resolved_tickers:
        issuer_rows = []
    else:
        issuer_rows = repo.list_issuers(
            issuer_ids=request.issuer_ids,
            ciks=request.ciks,
            tickers=resolved_tickers,
            limit=request.issuer_limit,
        )

    samples: list[CompanyValuationTemplateAssignmentRefreshSample] = []
    errors: list[str] = []
    updated_count = 0
    unchanged_count = 0

    for issuer_row in issuer_rows:
        _heartbeat(heartbeat)
        try:
            assignment = resolve_company_valuation_template_assignment(
                cik=str(issuer_row["cik"]),
                company_name=str(issuer_row["company_name"]),
                sic=issuer_row.get("sic"),
                sic_description=issuer_row.get("sic_description"),
                naics=issuer_row.get("naics"),
                config_root=request.config_root,
            )
            previous_template_id = str(issuer_row.get("template_id") or "") or None
            previous_assignment_source = (
                str(issuer_row.get("template_assignment_source") or "") or None
            )
            previous_stressed_operator_flag = bool(
                issuer_row.get("stressed_operator_flag")
            )
            previous_limited_coverage_flag = bool(issuer_row.get("limited_coverage_flag"))
            next_template_id = assignment.template.template_id
            next_assignment_source = assignment.source
            next_stressed_operator_flag = assignment.stressed_operator_flag
            next_limited_coverage_flag = assignment.limited_coverage_flag

            changed = (
                previous_template_id != next_template_id
                or str(issuer_row.get("template_version") or "")
                != assignment.template.template_version
                or previous_assignment_source != next_assignment_source
                or str(issuer_row.get("template_assignment_reason") or "")
                != assignment.reason
                or previous_stressed_operator_flag != next_stressed_operator_flag
                or previous_limited_coverage_flag != next_limited_coverage_flag
            )
            if not changed:
                unchanged_count += 1
                continue

            payload = {
                "issuer_id": str(issuer_row["issuer_id"]),
                "template_id": next_template_id,
                "template_version": assignment.template.template_version,
                "template_assignment_source": next_assignment_source,
                "template_assignment_reason": assignment.reason,
                "limited_coverage_flag": next_limited_coverage_flag,
                "stressed_operator_flag": next_stressed_operator_flag,
                "updated_at": datetime.now(UTC),
            }
            repo.upsert_issuer(payload)
            updated_count += 1
            if len(samples) < request.sample_limit:
                samples.append(
                    CompanyValuationTemplateAssignmentRefreshSample(
                        issuer_id=str(issuer_row["issuer_id"]),
                        cik=str(issuer_row["cik"]),
                        ticker=str(issuer_row.get("ticker") or "") or None,
                        company_name=str(issuer_row["company_name"]),
                        previous_template_id=previous_template_id,
                        next_template_id=next_template_id,
                        previous_assignment_source=previous_assignment_source,
                        next_assignment_source=next_assignment_source,
                        previous_stressed_operator_flag=previous_stressed_operator_flag,
                        next_stressed_operator_flag=next_stressed_operator_flag,
                    )
                )
        except Exception as exc:
            message = f"{issuer_row.get('ticker') or issuer_row['issuer_id']}: {exc}"
            errors.append(message)
            if not request.continue_on_error:
                raise

    completed_at = datetime.now(UTC)
    notes: list[str] = []
    if request.supported_only:
        notes.append(
            "Supported-only mode restricts assignment refresh to the curated supported issuer universe."
        )
    if not issuer_rows:
        notes.append("No issuers matched the requested assignment-refresh scope.")
    return CompanyValuationTemplateAssignmentRefreshResult(
        status="ok" if not errors else "partial_ok",
        started_at=started_at,
        completed_at=completed_at,
        issuers_requested=len(issuer_rows),
        issuers_considered=len(issuer_rows),
        issuers_updated=updated_count,
        unchanged_count=unchanged_count,
        errors=tuple(errors),
        samples=tuple(samples),
        notes=tuple(notes),
    )


__all__ = [
    "CompanyValuationTemplateAssignmentRefreshRequest",
    "CompanyValuationTemplateAssignmentRefreshResult",
    "CompanyValuationTemplateAssignmentRefreshSample",
    "refresh_company_valuation_template_assignments",
]

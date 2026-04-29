from __future__ import annotations

from fastapi import APIRouter, Query

from api.errors import bad_request_error, not_found_error, service_unavailable_error
from core.runtime.config import default_database_url
from core.services.company_valuation.screening import (
    get_company_valuation_document,
    list_company_valuation_screen,
)
from core.storage.company_valuation_repository import CompanyValuationRepository

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/company-valuation/screen")
def list_company_valuation_screen_route(
    as_of: str | None = None,
    template_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, object]:
    try:
        return list_company_valuation_screen(
            as_of=as_of,
            template_id=template_id,
            limit=limit,
            repository=CompanyValuationRepository(_db_target(db)),
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except Exception as exc:
        raise service_unavailable_error(exc) from exc


@router.get("/company-valuation/companies/{ticker}/evaluation")
def get_company_valuation_document_route(
    ticker: str,
    as_of: str | None = None,
    recompute_if_missing: bool = False,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return get_company_valuation_document(
            ticker=ticker,
            as_of=as_of,
            recompute_if_missing=recompute_if_missing,
            repository=CompanyValuationRepository(_db_target(db)),
        )
    except ValueError as exc:
        detail = str(exc)
        if detail.startswith("Unknown issuer") or detail.startswith("No company valuation snapshot"):
            raise not_found_error(exc) from exc
        raise bad_request_error(exc) from exc
    except Exception as exc:
        raise service_unavailable_error(exc) from exc

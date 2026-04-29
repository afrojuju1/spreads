from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from core.runtime.config import default_database_url
from core.services.company_valuation.contracts import PointInTimeSnapshot
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_datetime


@dataclass(frozen=True)
class PointInTimeRequest:
    issuer_id: str | None = None
    ticker: str | None = None
    as_of: datetime | None = None


def _normalize_as_of(value: str | datetime | None) -> datetime:
    parsed = parse_datetime(value) if isinstance(value, str) else value
    if parsed is None:
        return datetime.now(UTC)
    return parsed.astimezone(UTC)


def resolve_company_valuation_point_in_time(
    *,
    issuer_id: str | None = None,
    ticker: str | None = None,
    as_of: str | datetime | None = None,
    db_target: str | None = None,
    repository: CompanyValuationRepository | None = None,
) -> PointInTimeSnapshot:
    if not issuer_id and not ticker:
        raise ValueError("issuer_id or ticker is required")
    repo = repository or CompanyValuationRepository(db_target or default_database_url())
    if not repo.schema_ready():
        raise RuntimeError("company valuation schema is not ready")
    resolved_as_of = _normalize_as_of(as_of)
    issuer_row = repo.get_issuer(issuer_id=issuer_id, ticker=ticker)
    if issuer_row is None:
        target = issuer_id or ticker or "unknown"
        raise ValueError(f"Unknown issuer for company valuation point-in-time lookup: {target}")
    resolved_issuer_id = str(issuer_row.get("issuer_id"))
    latest_filing = repo.get_latest_filing_before(issuer_id=resolved_issuer_id, as_of=resolved_as_of)
    latest_market_snapshot = repo.get_latest_market_snapshot_before(
        issuer_id=resolved_issuer_id,
        as_of=resolved_as_of,
    )
    latest_treasury_curve_snapshot = repo.get_latest_treasury_curve_before(
        as_of=resolved_as_of
    )
    latest_company_valuation_snapshot = repo.get_latest_company_valuation(
        issuer_id=resolved_issuer_id,
        as_of=resolved_as_of,
    )
    latest_ownership_available_at = repo.latest_ownership_available_at_before(
        issuer_id=resolved_issuer_id,
        as_of=resolved_as_of,
    )
    return PointInTimeSnapshot(
        issuer_id=resolved_issuer_id,
        ticker=str(issuer_row.get("ticker") or "") or None,
        as_of=resolved_as_of,
        template_id=str(issuer_row.get("template_id") or "") or None,
        latest_filing=latest_filing,
        latest_market_snapshot=latest_market_snapshot,
        latest_treasury_curve_snapshot=latest_treasury_curve_snapshot,
        latest_ownership_available_at=latest_ownership_available_at,
        latest_company_valuation_snapshot=latest_company_valuation_snapshot,
    )

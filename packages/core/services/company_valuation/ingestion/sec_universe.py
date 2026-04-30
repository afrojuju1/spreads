from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from core.services.company_valuation.ids import build_issuer_id, build_security_id, normalize_cik, normalize_ticker
from core.services.company_valuation.sec_client import SecEdgarClient
from core.services.company_valuation.templates import (
    default_company_valuation_config_root,
    resolve_company_valuation_template_assignment,
)
from core.storage.company_valuation_repository import CompanyValuationRepository

DEFAULT_BOOTSTRAP_EXCHANGES = (
    "NASDAQ",
    "NYSE",
    "NYSE AMERICAN",
    "NYSE ARCA",
    "CBOE",
    "CBOE BZX",
)


@dataclass(frozen=True)
class SecUniverseBootstrapRequest:
    limit: int | None = None
    include_otc: bool = False
    include_adr_like: bool = False
    exchanges: tuple[str, ...] = DEFAULT_BOOTSTRAP_EXCHANGES
    config_root: str | None = None


@dataclass(frozen=True)
class SecUniverseBootstrapResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    issuers_seen: int = 0
    issuers_persisted: int = 0
    securities_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def _normalized_exchange(value: str | None) -> str:
    return str(value or "").upper().strip()


def _is_adr_like(name: str) -> bool:
    upper_name = str(name or "").upper()
    return "/ADR" in upper_name or upper_name.endswith(" ADR") or " ADR " in upper_name


def _iter_exchange_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    fields = payload.get("fields")
    data = payload.get("data")
    if not isinstance(fields, list) or not isinstance(data, list):
        raise RuntimeError("Unexpected SEC company_tickers_exchange payload shape")
    normalized_fields = [str(item).strip().lower() for item in fields]
    rows: list[dict[str, Any]] = []
    for raw_row in data:
        if not isinstance(raw_row, list):
            continue
        row = {
            normalized_fields[index]: raw_row[index]
            for index in range(min(len(normalized_fields), len(raw_row)))
        }
        rows.append(row)
    return rows


def bootstrap_sec_universe(
    request: SecUniverseBootstrapRequest,
    *,
    client: SecEdgarClient | None = None,
    repository: CompanyValuationRepository | None = None,
) -> SecUniverseBootstrapResult:
    started_at = datetime.now(UTC)
    sec_client = client or SecEdgarClient()
    repo = repository or CompanyValuationRepository()
    payload = sec_client.get_company_tickers_exchange()
    rows = _iter_exchange_rows(payload)
    config_root = request.config_root or str(default_company_valuation_config_root().parent)
    exchange_filter = {_normalized_exchange(value) for value in request.exchanges if str(value or "").strip()}
    now = datetime.now(UTC)

    issuer_payloads: list[dict[str, Any]] = []
    security_payloads: list[dict[str, Any]] = []
    seen_issuer_ids: set[str] = set()
    seen_security_ids: set[str] = set()
    issuer_has_primary_security: set[str] = set()

    for row in rows:
        cik = normalize_cik(row.get("cik"))
        ticker = normalize_ticker(str(row.get("ticker") or ""))
        company_name = str(row.get("name") or "").strip()
        exchange = _normalized_exchange(row.get("exchange"))
        if not ticker or not company_name:
            continue
        if exchange == "OTC" and not request.include_otc:
            continue
        if exchange_filter and exchange not in exchange_filter and not (
            request.include_otc and exchange == "OTC"
        ):
            continue
        if _is_adr_like(company_name) and not request.include_adr_like:
            continue
        issuer_id = build_issuer_id(cik)
        if issuer_id not in seen_issuer_ids:
            assignment = resolve_company_valuation_template_assignment(
                cik=cik,
                company_name=company_name,
                config_root=config_root,
            )
            issuer_payloads.append(
                {
                    "issuer_id": issuer_id,
                    "cik": cik,
                    "company_name": company_name,
                    "sic": None,
                    "sic_description": None,
                    "naics": None,
                    "template_id": assignment.template.template_id,
                    "template_version": assignment.template.template_version,
                    "template_assignment_source": "sec_ticker_exchange_bootstrap",
                    "template_assignment_reason": assignment.reason,
                    "limited_coverage_flag": assignment.limited_coverage_flag,
                    "stressed_operator_flag": assignment.stressed_operator_flag,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            seen_issuer_ids.add(issuer_id)
        security_id = build_security_id(cik, ticker)
        if security_id not in seen_security_ids:
            security_payloads.append(
                {
                    "security_id": security_id,
                    "issuer_id": issuer_id,
                    "ticker": ticker,
                    "share_class": None,
                    "exchange": exchange or None,
                    "cusip": None,
                    "is_primary": issuer_id not in issuer_has_primary_security,
                    "active_from": None,
                    "active_to": None,
                    "created_at": now,
                }
            )
            seen_security_ids.add(security_id)
            issuer_has_primary_security.add(issuer_id)
        if request.limit is not None and len(issuer_payloads) >= request.limit:
            break

    repo.upsert_issuers(issuer_payloads)
    repo.upsert_securities(security_payloads)
    completed_at = datetime.now(UTC)
    return SecUniverseBootstrapResult(
        status="ok",
        source="sec_universe_bootstrap",
        started_at=started_at,
        completed_at=completed_at,
        issuers_seen=len(rows),
        issuers_persisted=len(issuer_payloads),
        securities_persisted=len(security_payloads),
        notes=tuple(),
    )


__all__ = [
    "DEFAULT_BOOTSTRAP_EXCHANGES",
    "SecUniverseBootstrapRequest",
    "SecUniverseBootstrapResult",
    "bootstrap_sec_universe",
]

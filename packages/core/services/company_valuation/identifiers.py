from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

from core.services.company_valuation.ids import (
    build_issuer_id,
    build_security_id,
    build_security_identifier_id,
    normalize_cusip,
    normalize_name,
    normalize_ticker,
)
from core.services.company_valuation.openfigi_client import (
    OpenFigiClient,
    OpenFigiMapping,
    select_best_openfigi_mapping,
)
from core.services.company_valuation.sec_client import SecEdgarClient
from core.services.company_valuation.templates import (
    resolve_company_valuation_template_assignment,
)
from core.services.value_coercion import utc_now as _utc_now
from core.storage.company_valuation_repository import CompanyValuationRepository


@dataclass(frozen=True)
class Official13FListEntry:
    cusip: str
    marker: str | None
    issuer_name: str
    title_of_class: str
    status_code: str | None
    source_url: str


@dataclass(frozen=True)
class SecurityResolution:
    issuer_id: str
    security_id: str
    resolution_source: str
    resolution_confidence: float
    issuer_payload: dict[str, object] | None = None
    identifier_history_payloads: tuple[dict[str, object], ...] = ()
    security_payload: dict[str, object] | None = None


def _quarter_number(value: date) -> int:
    return ((value.month - 1) // 3) + 1


def _next_quarter_label(report_period: date) -> tuple[int, int]:
    quarter = _quarter_number(report_period)
    if quarter == 4:
        return (report_period.year + 1, 1)
    return (report_period.year, quarter + 1)


def build_official_13f_list_url(
    report_period: date,
    *,
    base_url: str = "https://www.sec.gov",
) -> str:
    year, quarter = _next_quarter_label(report_period)
    return f"{base_url.rstrip('/')}/files/investment/13flist{year}q{quarter}.txt"


def parse_official_13f_list(
    text: str,
    *,
    source_url: str,
) -> dict[str, Official13FListEntry]:
    entries: dict[str, Official13FListEntry] = {}
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n")
        if len(line) < 10:
            continue
        try:
            cusip = normalize_cusip(line[:9])
        except ValueError:
            continue
        marker = line[9].strip() or None
        issuer_name = line[10:40].strip()
        title_of_class = line[40:79].strip()
        status_code = line[79].strip() if len(line) >= 80 else None
        entries[cusip] = Official13FListEntry(
            cusip=cusip,
            marker=marker,
            issuer_name=issuer_name,
            title_of_class=title_of_class,
            status_code=status_code or None,
            source_url=source_url,
        )
    return entries


def load_official_13f_list(
    *,
    report_period: date,
    client: SecEdgarClient | None = None,
) -> dict[str, Official13FListEntry]:
    sec_client = client or SecEdgarClient()
    source_url = build_official_13f_list_url(
        report_period,
        base_url=sec_client.www_base_url,
    )
    text = sec_client.get_text_url(source_url)
    return parse_official_13f_list(text, source_url=source_url)


def _identifier_history_payload(
    *,
    issuer_id: str,
    security_id: str,
    identifier_type: str,
    identifier_value: str,
    issuer_name_reported: str | None,
    title_of_class: str | None,
    effective_from: date,
    source: str,
    source_ref: str | None,
    match_confidence: float,
    created_at: datetime,
) -> dict[str, object]:
    normalized_value = str(identifier_value).strip().upper()
    return {
        "security_identifier_id": build_security_identifier_id(
            security_id,
            identifier_type,
            normalized_value,
            effective_from,
        ),
        "security_id": security_id,
        "issuer_id": issuer_id,
        "identifier_type": str(identifier_type).strip().lower(),
        "identifier_value": normalized_value,
        "issuer_name_reported": issuer_name_reported,
        "title_of_class": title_of_class,
        "effective_from": effective_from,
        "effective_to": None,
        "source": source,
        "source_ref": source_ref,
        "match_confidence": float(match_confidence),
        "created_at": created_at,
        "updated_at": created_at,
    }


def _is_primary_security_cusip(title_of_class: str | None) -> bool:
    normalized = str(title_of_class or "").upper().strip()
    if not normalized:
        return False
    blocked_tokens = ("CALL", "PUT", "WARRANT", "WT", "NOTE", "UNIT")
    return not any(token in normalized for token in blocked_tokens)


def _existing_cusip_resolution(
    *,
    repository: CompanyValuationRepository,
    cusip: str,
    report_period: date,
) -> SecurityResolution | None:
    mapping_row = repository.find_security_identifier_mapping(
        identifier_type="cusip",
        identifier_value=cusip,
        effective_on=report_period,
    )
    if mapping_row:
        return SecurityResolution(
            issuer_id=str(mapping_row["issuer_id"]),
            security_id=str(mapping_row["security_id"]),
            resolution_source=str(mapping_row.get("source") or "security_identifier_history"),
            resolution_confidence=float(mapping_row.get("match_confidence") or 1.0),
        )
    security_row = repository.get_security(cusip=cusip)
    if security_row:
        return SecurityResolution(
            issuer_id=str(security_row["issuer_id"]),
            security_id=str(security_row["security_id"]),
            resolution_source="security_cusip",
            resolution_confidence=0.98,
        )
    return None


def _issuer_name_candidates(
    *,
    repository: CompanyValuationRepository,
    name_candidates: Iterable[str],
) -> list[dict[str, object]]:
    normalized_targets = {normalize_name(value) for value in name_candidates if str(value or "").strip()}
    if not normalized_targets:
        return []
    matches: list[dict[str, object]] = []
    for issuer_row in repository.list_issuers():
        company_name = str(issuer_row.get("company_name") or "")
        if not company_name:
            continue
        if normalize_name(company_name) not in normalized_targets:
            continue
        matches.append(issuer_row)
    return matches


def _synthetic_openfigi_cik(
    *,
    ticker: str,
    issuer_name: str | None,
    cusip: str,
) -> str:
    token = "|".join(
        [
            normalize_ticker(ticker),
            normalize_name(issuer_name or ticker),
            normalize_cusip(cusip),
        ]
    )
    digits = str(int(hashlib.sha1(token.encode("utf-8")).hexdigest()[:12], 16))[-9:]
    return f"9{digits.zfill(9)}"


def _is_limited_coverage_openfigi_mapping(mapping: OpenFigiMapping) -> bool:
    security_type = str(mapping.security_type or "").strip().lower()
    description = normalize_name(mapping.security_description or "")
    name = normalize_name(mapping.name or "")
    if security_type in {"etp", "etf", "fund", "mutual fund", "closed end fund"}:
        return True
    limited_tokens = (
        "etf",
        "fund",
        "trust",
        "shares",
        "index",
        "income act",
    )
    return any(token in description or token in name for token in limited_tokens)


def _seed_openfigi_issuer_resolution(
    *,
    repository: CompanyValuationRepository,
    best_mapping: OpenFigiMapping,
    cusip: str,
    figi: str | None,
    issuer_name_reported: str | None,
    title_of_class: str | None,
    report_period: date,
) -> SecurityResolution:
    created_at = _utc_now()
    synthetic_cik = _synthetic_openfigi_cik(
        ticker=str(best_mapping.ticker),
        issuer_name=best_mapping.name or issuer_name_reported,
        cusip=cusip,
    )
    issuer_id = build_issuer_id(synthetic_cik)
    security_id = build_security_id(synthetic_cik, str(best_mapping.ticker))
    assignment = resolve_company_valuation_template_assignment(
        cik=synthetic_cik,
        company_name=best_mapping.name or issuer_name_reported or str(best_mapping.ticker),
    )
    limited_coverage_flag = bool(assignment.limited_coverage_flag) or _is_limited_coverage_openfigi_mapping(best_mapping)
    assignment_reason = f"openfigi_seed:{best_mapping.ticker}"
    if limited_coverage_flag:
        assignment_reason = f"{assignment_reason}:limited_coverage"
    issuer_payload = {
        "issuer_id": issuer_id,
        "cik": synthetic_cik,
        "company_name": best_mapping.name or issuer_name_reported or str(best_mapping.ticker),
        "sic": None,
        "sic_description": None,
        "naics": None,
        "template_id": assignment.template.template_id,
        "template_version": assignment.template.template_version,
        "template_assignment_source": "openfigi_seed",
        "template_assignment_reason": assignment_reason,
        "limited_coverage_flag": limited_coverage_flag,
        "stressed_operator_flag": assignment.stressed_operator_flag,
        "created_at": created_at,
        "updated_at": created_at,
    }
    security_payload = {
        "security_id": security_id,
        "issuer_id": issuer_id,
        "ticker": normalize_ticker(best_mapping.ticker),
        "share_class": best_mapping.security_description,
        "exchange": best_mapping.exch_code,
        "cusip": cusip,
        "is_primary": True,
        "active_from": None,
        "active_to": None,
        "created_at": created_at,
    }
    payloads = [
        _identifier_history_payload(
            issuer_id=issuer_id,
            security_id=security_id,
            identifier_type="cusip",
            identifier_value=cusip,
            issuer_name_reported=best_mapping.name or issuer_name_reported,
            title_of_class=title_of_class,
            effective_from=report_period,
            source="openfigi_cusip_ticker_seed",
            source_ref="openfigi",
            match_confidence=0.84,
            created_at=created_at,
        )
    ]
    resolved_figi = best_mapping.share_class_figi or best_mapping.composite_figi or figi
    if resolved_figi:
        payloads.append(
            _identifier_history_payload(
                issuer_id=issuer_id,
                security_id=security_id,
                identifier_type="figi",
                identifier_value=str(resolved_figi).strip().upper(),
                issuer_name_reported=best_mapping.name or issuer_name_reported,
                title_of_class=title_of_class,
                effective_from=report_period,
                source="openfigi_cusip_ticker_seed",
                source_ref="openfigi",
                match_confidence=0.84,
                created_at=created_at,
            )
        )
    return SecurityResolution(
        issuer_id=issuer_id,
        security_id=security_id,
        resolution_source="openfigi_cusip_ticker_seed",
        resolution_confidence=0.84,
        issuer_payload=issuer_payload,
        identifier_history_payloads=tuple(payloads),
        security_payload=security_payload,
    )


def _resolution_from_known_issuer(
    *,
    repository: CompanyValuationRepository,
    issuer_row: dict[str, object],
    cusip: str,
    figi: str | None,
    issuer_name_reported: str | None,
    title_of_class: str | None,
    report_period: date,
    resolution_source: str,
    resolution_confidence: float,
    source_ref: str | None,
    update_security_cusip: bool = False,
) -> SecurityResolution | None:
    primary_security = repository.get_primary_security(issuer_id=str(issuer_row["issuer_id"]))
    if primary_security is None:
        return None
    created_at = _utc_now()
    payloads = [
        _identifier_history_payload(
            issuer_id=str(issuer_row["issuer_id"]),
            security_id=str(primary_security["security_id"]),
            identifier_type="cusip",
            identifier_value=cusip,
            issuer_name_reported=issuer_name_reported,
            title_of_class=title_of_class,
            effective_from=report_period,
            source=resolution_source,
            source_ref=source_ref,
            match_confidence=resolution_confidence,
            created_at=created_at,
        )
    ]
    if figi:
        payloads.append(
            _identifier_history_payload(
                issuer_id=str(issuer_row["issuer_id"]),
                security_id=str(primary_security["security_id"]),
                identifier_type="figi",
                identifier_value=str(figi).strip().upper(),
                issuer_name_reported=issuer_name_reported,
                title_of_class=title_of_class,
                effective_from=report_period,
                source=resolution_source,
                source_ref=source_ref,
                match_confidence=resolution_confidence,
                created_at=created_at,
            )
        )
    security_payload = None
    if update_security_cusip and not str(primary_security.get("cusip") or "").strip():
        security_payload = {
            "security_id": str(primary_security["security_id"]),
            "cusip": cusip,
        }
    return SecurityResolution(
        issuer_id=str(issuer_row["issuer_id"]),
        security_id=str(primary_security["security_id"]),
        resolution_source=resolution_source,
        resolution_confidence=resolution_confidence,
        identifier_history_payloads=tuple(payloads),
        security_payload=security_payload,
    )


def seed_security_identifier_history_from_official_list(
    *,
    report_period: date,
    repository: CompanyValuationRepository,
    client: SecEdgarClient | None = None,
) -> int:
    official_entries = load_official_13f_list(report_period=report_period, client=client)
    security_identifier_payloads: list[dict[str, object]] = []
    security_payloads: list[dict[str, object]] = []
    for entry in official_entries.values():
        issuer_matches = _issuer_name_candidates(
            repository=repository,
            name_candidates=(entry.issuer_name,),
        )
        if len(issuer_matches) != 1:
            continue
        resolution = _resolution_from_known_issuer(
            repository=repository,
            issuer_row=issuer_matches[0],
            cusip=entry.cusip,
            figi=None,
            issuer_name_reported=entry.issuer_name,
            title_of_class=entry.title_of_class,
            report_period=report_period,
            resolution_source="sec_13f_list_name_match",
            resolution_confidence=0.95,
            source_ref=entry.source_url,
            update_security_cusip=_is_primary_security_cusip(entry.title_of_class),
        )
        if resolution is None:
            continue
        security_identifier_payloads.extend(resolution.identifier_history_payloads)
        if resolution.security_payload:
            security_payloads.append(resolution.security_payload)
    if security_payloads:
        repository.upsert_securities(security_payloads)
    if security_identifier_payloads:
        repository.upsert_security_identifier_history(security_identifier_payloads)
    return len(security_identifier_payloads)


def resolve_cusip_to_security(
    *,
    cusip: str,
    issuer_name_reported: str | None,
    title_of_class: str | None,
    figi: str | None,
    report_period: date,
    repository: CompanyValuationRepository,
    official_entries: dict[str, Official13FListEntry],
    openfigi_client: OpenFigiClient | None = None,
    allow_openfigi_fallback: bool = True,
    preloaded_openfigi_mappings: dict[str, list[OpenFigiMapping]] | None = None,
) -> SecurityResolution | None:
    normalized_cusip = normalize_cusip(cusip)
    existing = _existing_cusip_resolution(
        repository=repository,
        cusip=normalized_cusip,
        report_period=report_period,
    )
    if existing is not None:
        return existing

    official_entry = official_entries.get(normalized_cusip)
    issuer_matches = _issuer_name_candidates(
        repository=repository,
        name_candidates=(
            issuer_name_reported or "",
            official_entry.issuer_name if official_entry else "",
        ),
    )
    if len(issuer_matches) == 1:
        return _resolution_from_known_issuer(
            repository=repository,
            issuer_row=issuer_matches[0],
            cusip=normalized_cusip,
            figi=figi,
            issuer_name_reported=issuer_name_reported or (official_entry.issuer_name if official_entry else None),
            title_of_class=title_of_class or (official_entry.title_of_class if official_entry else None),
            report_period=report_period,
            resolution_source="sec_13f_list_name_match",
            resolution_confidence=0.93,
            source_ref=official_entry.source_url if official_entry else None,
            update_security_cusip=_is_primary_security_cusip(title_of_class or (official_entry.title_of_class if official_entry else None)),
        )

    if str(figi or "").strip():
        figi_mapping = repository.find_security_identifier_mapping(
            identifier_type="figi",
            identifier_value=str(figi or "").strip().upper(),
            effective_on=report_period,
        )
        if figi_mapping:
            return SecurityResolution(
                issuer_id=str(figi_mapping["issuer_id"]),
                security_id=str(figi_mapping["security_id"]),
                resolution_source=str(figi_mapping.get("source") or "security_identifier_history"),
                resolution_confidence=float(figi_mapping.get("match_confidence") or 0.98),
            )

    if not allow_openfigi_fallback:
        return None
    if preloaded_openfigi_mappings is not None:
        mappings = preloaded_openfigi_mappings.get(normalized_cusip, [])
    else:
        figi_client = openfigi_client or OpenFigiClient()
        mappings = figi_client.map_cusips([normalized_cusip]).get(normalized_cusip, [])
    best_mapping = select_best_openfigi_mapping(mappings)
    if best_mapping is None or not best_mapping.ticker:
        return None
    issuer_row = repository.get_issuer(ticker=best_mapping.ticker)
    if issuer_row is None:
        return _seed_openfigi_issuer_resolution(
            repository=repository,
            best_mapping=best_mapping,
            cusip=normalized_cusip,
            figi=figi,
            issuer_name_reported=issuer_name_reported,
            title_of_class=title_of_class,
            report_period=report_period,
        )
    return _resolution_from_known_issuer(
        repository=repository,
        issuer_row=issuer_row,
        cusip=normalized_cusip,
        figi=best_mapping.share_class_figi or best_mapping.composite_figi or figi,
        issuer_name_reported=best_mapping.name or issuer_name_reported,
        title_of_class=title_of_class,
        report_period=report_period,
        resolution_source="openfigi_cusip_ticker_match",
        resolution_confidence=0.88,
        source_ref="openfigi",
        update_security_cusip=_is_primary_security_cusip(title_of_class),
    )


__all__ = [
    "Official13FListEntry",
    "SecurityResolution",
    "build_official_13f_list_url",
    "load_official_13f_list",
    "parse_official_13f_list",
    "resolve_cusip_to_security",
    "seed_security_identifier_history_from_official_list",
]

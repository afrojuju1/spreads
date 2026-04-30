from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.company_valuation.identifiers import (
    load_official_13f_list,
    resolve_cusip_to_security,
)
from core.services.company_valuation.openfigi_client import (
    OpenFigiClient,
    OpenFigiRequestError,
)
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_date


@dataclass(frozen=True)
class ResolveUnresolvedInstitutionalPositionsRequest:
    report_period: datetime | None = None
    limit_rows: int = 20000
    batch_cusips: int = 50
    max_attempts: int = 5


@dataclass(frozen=True)
class ResolveUnresolvedInstitutionalPositionsResult:
    status: str
    started_at: datetime
    completed_at: datetime
    rows_seen: int = 0
    rows_resolved: int = 0
    positions_materialized: int = 0
    identifier_mappings_persisted: int = 0
    pending_rows: int = 0
    failed_rows: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def _retry_delay(attempt_count: int) -> timedelta:
    hours = min(24 * (2 ** max(attempt_count - 1, 0)), 24 * 14)
    return timedelta(hours=hours)


def _heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is not None:
        heartbeat()


def _batch_openfigi_map(
    *,
    client: OpenFigiClient,
    cusips: list[str],
    batch_size: int,
    heartbeat: Callable[[], None] | None = None,
) -> dict[str, list[Any]]:
    mappings: dict[str, list[Any]] = {}
    for index in range(0, len(cusips), batch_size):
        _heartbeat(heartbeat)
        batch = cusips[index : index + batch_size]
        delay_seconds = 1.0
        for attempt in range(5):
            try:
                mappings.update(client.map_cusips(batch))
                break
            except OpenFigiRequestError as exc:
                if exc.status_code != 429 or attempt >= 4:
                    raise
                time.sleep(delay_seconds)
                delay_seconds = min(delay_seconds * 2.0, 16.0)
    return mappings


def resolve_unresolved_institutional_positions(
    request: ResolveUnresolvedInstitutionalPositionsRequest,
    *,
    repository: CompanyValuationRepository | None = None,
    openfigi_client: OpenFigiClient | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> ResolveUnresolvedInstitutionalPositionsResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    figi_client = openfigi_client or OpenFigiClient()
    report_period = None
    if request.report_period is not None:
        report_period = parse_date(request.report_period)
    rows = repo.list_unresolved_institutional_positions(
        report_period=report_period,
        statuses=("pending",),
        due_before=datetime.now(UTC),
        limit=request.limit_rows,
    )
    if not rows:
        completed_at = datetime.now(UTC)
        return ResolveUnresolvedInstitutionalPositionsResult(
            status="ok",
            started_at=started_at,
            completed_at=completed_at,
        )

    rows_by_cusip: dict[str, list[dict[str, Any]]] = {}
    official_lists: dict[str, dict[str, Any]] = {}
    for row in rows:
        _heartbeat(heartbeat)
        cusip = str(row.get("cusip") or "").strip().upper()
        if not cusip:
            continue
        rows_by_cusip.setdefault(cusip, []).append(row)
        period_key = str(row.get("report_period"))
        if period_key and period_key not in official_lists:
            official_lists[period_key] = load_official_13f_list(
                report_period=parse_date(period_key)
            )

    sec_native_resolutions: dict[str, Any] = {}
    openfigi_candidates: list[str] = []
    for cusip, cusip_rows in rows_by_cusip.items():
        _heartbeat(heartbeat)
        sample = cusip_rows[0]
        report_period_value = parse_date(sample["report_period"])
        resolution = resolve_cusip_to_security(
            cusip=cusip,
            issuer_name_reported=str(sample.get("issuer_name_reported") or "").strip() or None,
            title_of_class=str(sample.get("title_of_class") or "").strip() or None,
            figi=str(sample.get("figi") or "").strip() or None,
            report_period=report_period_value,
            repository=repo,
            official_entries=official_lists[str(sample["report_period"])],
            allow_openfigi_fallback=False,
        )
        if resolution is not None:
            sec_native_resolutions[cusip] = resolution
        else:
            openfigi_candidates.append(cusip)

    openfigi_mappings = _batch_openfigi_map(
        client=figi_client,
        cusips=openfigi_candidates,
        batch_size=max(request.batch_cusips, 1),
        heartbeat=heartbeat,
    ) if openfigi_candidates else {}

    issuer_payloads: dict[str, dict[str, object]] = {}
    identifier_payloads: dict[str, dict[str, object]] = {}
    security_payloads: dict[str, dict[str, object]] = {}
    position_payloads: list[dict[str, object]] = []
    unresolved_updates: list[dict[str, Any]] = []
    resolved_hashes: set[str] = set()
    failed_rows = 0

    for cusip, cusip_rows in rows_by_cusip.items():
        _heartbeat(heartbeat)
        resolution = sec_native_resolutions.get(cusip)
        if resolution is None:
            sample = cusip_rows[0]
            resolution = resolve_cusip_to_security(
                cusip=cusip,
                issuer_name_reported=str(sample.get("issuer_name_reported") or "").strip() or None,
                title_of_class=str(sample.get("title_of_class") or "").strip() or None,
                figi=str(sample.get("figi") or "").strip() or None,
                report_period=parse_date(sample["report_period"]),
                repository=repo,
                official_entries=official_lists[str(sample["report_period"])],
                allow_openfigi_fallback=True,
                preloaded_openfigi_mappings=openfigi_mappings,
            )
        if resolution is not None:
            if resolution.issuer_payload:
                issuer_payloads[str(resolution.issuer_payload["issuer_id"])] = resolution.issuer_payload
            for payload in resolution.identifier_history_payloads:
                identifier_payloads[str(payload["security_identifier_id"])] = payload
            if resolution.security_payload:
                security_payloads[str(resolution.security_payload["security_id"])] = resolution.security_payload
            for row in cusip_rows:
                resolved_hashes.add(str(row["source_row_hash"]))
                position_payloads.append(
                    {
                        "institutional_holder_id": str(row["institutional_holder_id"]),
                        "issuer_id": resolution.issuer_id,
                        "filing_id": str(row["filing_id"]),
                        "report_period": parse_date(row["report_period"]),
                        "available_at": row["available_at"],
                        "issuer_name_reported": row.get("issuer_name_reported"),
                        "title_of_class": row.get("title_of_class"),
                        "cusip": row.get("cusip"),
                        "figi": row.get("figi"),
                        "share_count": row.get("share_count"),
                        "market_value_reported": row.get("market_value_reported"),
                        "put_call": row.get("put_call"),
                        "discretion_type": row.get("discretion_type"),
                        "other_manager_refs_json": row.get("other_manager_refs") or row.get("other_manager_refs_json") or [],
                        "voting_authority_sole": row.get("voting_authority_sole"),
                        "voting_authority_shared": row.get("voting_authority_shared"),
                        "voting_authority_none": row.get("voting_authority_none"),
                        "resolution_source": resolution.resolution_source,
                        "resolution_confidence": resolution.resolution_confidence,
                        "source_row_hash": str(row["source_row_hash"]),
                    }
                )
                updated = dict(row)
                updated["resolution_status"] = "resolved"
                updated["resolution_source"] = resolution.resolution_source
                updated["resolution_confidence"] = resolution.resolution_confidence
                updated["last_attempted_at"] = datetime.now(UTC)
                updated["next_retry_at"] = None
                updated["last_error"] = None
                updated["updated_at"] = datetime.now(UTC)
                unresolved_updates.append(updated)
            continue

        for row in cusip_rows:
            attempts = int(row.get("resolution_attempt_count") or 0) + 1
            updated = dict(row)
            updated["resolution_attempt_count"] = attempts
            updated["last_attempted_at"] = datetime.now(UTC)
            updated["updated_at"] = datetime.now(UTC)
            if attempts >= request.max_attempts:
                updated["resolution_status"] = "failed"
                updated["next_retry_at"] = None
                failed_rows += 1
            else:
                updated["resolution_status"] = "pending"
                updated["next_retry_at"] = datetime.now(UTC) + _retry_delay(attempts)
            updated["last_error"] = "Unable to resolve CUSIP to seeded issuer universe"
            unresolved_updates.append(updated)

    if issuer_payloads:
        _heartbeat(heartbeat)
        repo.upsert_issuers(list(issuer_payloads.values()))
    if security_payloads:
        _heartbeat(heartbeat)
        repo.upsert_securities(list(security_payloads.values()))
    _heartbeat(heartbeat)
    identifier_count = (
        repo.upsert_security_identifier_history(list(identifier_payloads.values()))
        if identifier_payloads
        else 0
    )
    _heartbeat(heartbeat)
    positions_materialized = (
        repo.upsert_institutional_positions(position_payloads)
        if position_payloads
        else 0
    )
    if unresolved_updates:
        _heartbeat(heartbeat)
        repo.upsert_unresolved_institutional_positions(unresolved_updates)

    completed_at = datetime.now(UTC)
    return ResolveUnresolvedInstitutionalPositionsResult(
        status="ok",
        started_at=started_at,
        completed_at=completed_at,
        rows_seen=len(rows),
        rows_resolved=len(resolved_hashes),
        positions_materialized=positions_materialized,
        identifier_mappings_persisted=identifier_count,
        pending_rows=len(rows) - len(resolved_hashes) - failed_rows,
        failed_rows=failed_rows,
    )


__all__ = [
    "ResolveUnresolvedInstitutionalPositionsRequest",
    "ResolveUnresolvedInstitutionalPositionsResult",
    "resolve_unresolved_institutional_positions",
]

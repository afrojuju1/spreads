from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
import hashlib
import json
from typing import Any

from core.jobs.adhoc import enqueue_ad_hoc_job
from core.jobs.registry import (
    COMPANY_VALUATION_BOOTSTRAP_ADHOC_JOB_KEY,
    COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
    COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_KEY,
    COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
    COMPANY_VALUATION_SCREEN_MATERIALIZE_ADHOC_JOB_KEY,
    COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
)
from core.runtime.config import default_database_url
from core.services.company_valuation.bootstrap import CompanyValuationBootstrapRequest
from core.services.company_valuation.unresolved import (
    ResolveUnresolvedInstitutionalPositionsRequest,
)
from core.storage.factory import build_job_repository
from core.storage.serializers import parse_date, parse_datetime


@dataclass(frozen=True)
class CompanyValuationScreenRefreshRequest:
    as_of: datetime | None = None
    template_id: str | None = None
    tickers: tuple[str, ...] | None = None
    issuer_limit: int | None = None
    supported_only: bool = True
    stressed_operator_only: bool = False
    config_root: str | None = None


@dataclass(frozen=True)
class QueuedCompanyValuationJob:
    job_run_id: str
    job_key: str
    job_type: str
    arq_job_id: str
    status: str
    scheduled_for: str
    payload: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_as_of(value: str | datetime | None) -> datetime | None:
    parsed = parse_datetime(value) if isinstance(value, str) else value
    return None if parsed is None else parsed.astimezone(UTC)


def _parse_day(value: str | date | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    parsed = parse_date(value)
    if parsed is None:
        raise ValueError(f"Invalid date value: {value}")
    return parsed


def _normalized_tickers(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(
        dict.fromkeys(
            str(value or "").upper().strip()
            for value in values
            if str(value or "").strip()
        )
    )


def _scope_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:10]


def _job_run_id(job_key: str, scheduled_for: datetime) -> str:
    return f"{job_key}:{scheduled_for.astimezone(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"


def _queue_job(
    *,
    job_type: str,
    job_key: str,
    payload: dict[str, Any],
    db_target: str | None,
    redis_url: str | None,
) -> QueuedCompanyValuationJob:
    database_url = db_target or default_database_url()
    job_store = build_job_repository(database_url)
    if not job_store.schema_ready():
        raise RuntimeError("Job schema is unavailable.")
    scheduled_for = datetime.now(UTC)
    scheduled_for_iso = _isoformat(scheduled_for)
    if scheduled_for_iso is None:
        raise RuntimeError("Failed to render scheduled_for timestamp")
    full_payload = {
        **payload,
        "job_key": job_key,
        "job_type": job_type,
        "scheduled_for": scheduled_for_iso,
    }
    job_run_id = _job_run_id(job_key, scheduled_for)
    arq_job_id = job_run_id
    job_run, _created = job_store.create_job_run(
        job_run_id=job_run_id,
        job_key=job_key,
        arq_job_id=arq_job_id,
        job_type=job_type,
        status="queued",
        scheduled_for=scheduled_for,
        payload=full_payload,
    )
    try:
        enqueued = enqueue_ad_hoc_job(
            job_type=job_type,
            job_key=job_key,
            job_run_id=job_run_id,
            arq_job_id=arq_job_id,
            payload=full_payload,
            redis_url=redis_url,
        )
    except Exception as exc:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=arq_job_id,
            finished_at=datetime.now(UTC),
            error_text=str(exc),
        )
        raise RuntimeError(f"Company valuation queueing failed: {exc}") from exc
    if enqueued is None:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=arq_job_id,
            finished_at=datetime.now(UTC),
            error_text="Company valuation job was not enqueued.",
        )
        raise RuntimeError("Company valuation job was not enqueued.")
    row = dict(job_store.get_job_run(job_run_id) or job_run)
    return QueuedCompanyValuationJob(
        job_run_id=str(row["job_run_id"]),
        job_key=str(row["job_key"]),
        job_type=str(row["job_type"]),
        arq_job_id=str(row.get("arq_job_id") or arq_job_id),
        status=str(row["status"]),
        scheduled_for=scheduled_for_iso,
        payload=full_payload,
    )


def enqueue_company_valuation_bootstrap_job(
    request: CompanyValuationBootstrapRequest,
    *,
    db_target: str | None = None,
    redis_url: str | None = None,
) -> QueuedCompanyValuationJob:
    tickers = _normalized_tickers(request.tickers)
    if not tickers:
        raise ValueError("At least one ticker is required.")
    scope = {
        "tickers": list(tickers),
        "bootstrap_universe": bool(request.bootstrap_universe),
        "config_root": request.config_root,
    }
    scope_hash = _scope_hash(scope)
    job_key = f"{COMPANY_VALUATION_BOOTSTRAP_ADHOC_JOB_KEY}:{scope_hash}"
    payload = {
        "tickers": list(tickers),
        "as_of": _isoformat(_parse_as_of(request.as_of)),
        "bootstrap_universe": bool(request.bootstrap_universe),
        "universe_limit": request.universe_limit,
        "refresh_treasury": bool(request.refresh_treasury),
        "treasury_curve_date": (
            None
            if request.treasury_curve_date is None
            else _parse_day(request.treasury_curve_date).isoformat()
        ),
        "refresh_filings": bool(request.refresh_filings),
        "filings_since": _isoformat(_parse_as_of(request.filings_since)),
        "filings_until": _isoformat(_parse_as_of(request.filings_until)),
        "refresh_insiders": bool(request.refresh_insiders),
        "refresh_beneficial_ownership": bool(request.refresh_beneficial_ownership),
        "ownership_since": _isoformat(_parse_as_of(request.ownership_since)),
        "ownership_until": _isoformat(_parse_as_of(request.ownership_until)),
        "refresh_market_inputs": bool(request.refresh_market_inputs),
        "recompute": bool(request.recompute),
        "materialize_screen": bool(request.materialize_screen),
        "continue_on_error": bool(request.continue_on_error),
        "config_root": request.config_root,
        "singleton_scope": f"bootstrap:{scope_hash}",
    }
    return _queue_job(
        job_type=COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
        job_key=job_key,
        payload=payload,
        db_target=db_target,
        redis_url=redis_url,
    )


def enqueue_company_valuation_screen_materialize_job(
    request: CompanyValuationScreenRefreshRequest,
    *,
    db_target: str | None = None,
    redis_url: str | None = None,
) -> QueuedCompanyValuationJob:
    tickers = _normalized_tickers(request.tickers)
    scope = {
        "tickers": list(tickers),
        "template_id": request.template_id,
        "issuer_limit": request.issuer_limit,
        "supported_only": bool(request.supported_only),
        "stressed_operator_only": bool(request.stressed_operator_only),
        "config_root": request.config_root,
    }
    scope_hash = _scope_hash(scope)
    job_key = f"{COMPANY_VALUATION_SCREEN_MATERIALIZE_ADHOC_JOB_KEY}:{scope_hash}"
    payload = {
        "as_of": _isoformat(_parse_as_of(request.as_of)),
        "template_id": request.template_id,
        "tickers": list(tickers),
        "issuer_limit": request.issuer_limit,
        "supported_only": bool(request.supported_only),
        "stressed_operator_only": bool(request.stressed_operator_only),
        "config_root": request.config_root,
        "singleton_scope": f"screen:{scope_hash}",
    }
    return _queue_job(
        job_type=COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
        job_key=job_key,
        payload=payload,
        db_target=db_target,
        redis_url=redis_url,
    )


def enqueue_company_valuation_resolve_unresolved_job(
    request: ResolveUnresolvedInstitutionalPositionsRequest,
    *,
    db_target: str | None = None,
    redis_url: str | None = None,
) -> QueuedCompanyValuationJob:
    resolved_report_period = _parse_day(request.report_period)
    scope = {
        "report_period": (
            None if resolved_report_period is None else resolved_report_period.isoformat()
        ),
        "limit_rows": int(request.limit_rows),
        "batch_cusips": int(request.batch_cusips),
        "max_attempts": int(request.max_attempts),
    }
    scope_hash = _scope_hash(scope)
    job_key = (
        COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_KEY
        if resolved_report_period is None
        else f"{COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_KEY}:{scope_hash}"
    )
    payload = {
        "report_period": (
            None if resolved_report_period is None else resolved_report_period.isoformat()
        ),
        "limit_rows": int(request.limit_rows),
        "batch_cusips": int(request.batch_cusips),
        "max_attempts": int(request.max_attempts),
        "allow_off_hours": True,
        "singleton_scope": (
            "resolve_unresolved:global"
            if resolved_report_period is None
            else f"resolve_unresolved:{scope_hash}"
        ),
    }
    return _queue_job(
        job_type=COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
        job_key=job_key,
        payload=payload,
        db_target=db_target,
        redis_url=redis_url,
    )


__all__ = [
    "CompanyValuationScreenRefreshRequest",
    "QueuedCompanyValuationJob",
    "enqueue_company_valuation_bootstrap_job",
    "enqueue_company_valuation_resolve_unresolved_job",
    "enqueue_company_valuation_screen_materialize_job",
]

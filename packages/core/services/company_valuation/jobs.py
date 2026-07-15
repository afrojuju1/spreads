from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
import hashlib
import json
from typing import Any

from core.jobs.adhoc import start_ad_hoc_job_workflow
from core.jobs.registry import (
    COMPANY_VALUATION_BOOTSTRAP_ADHOC_JOB_KEY,
    COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
    COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_KEY,
    COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
    COMPANY_VALUATION_SCREEN_MATERIALIZE_ADHOC_JOB_KEY,
    COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
)
from core.services.company_valuation.bootstrap import CompanyValuationBootstrapRequest
from core.services.company_valuation.contracts import CompanyValuationContractModel
from core.services.company_valuation.unresolved import (
    ResolveUnresolvedInstitutionalPositionsRequest,
)
from core.storage.serializers import parse_date, parse_datetime
from core.workflow_runtime.provider import routine_workflow_id


class CompanyValuationScreenRefreshRequest(CompanyValuationContractModel):
    as_of: datetime | None = None
    template_id: str | None = None
    tickers: tuple[str, ...] | None = None
    issuer_limit: int | None = None
    supported_only: bool = True
    stressed_operator_only: bool = False
    config_root: str | None = None


class QueuedCompanyValuationJob(CompanyValuationContractModel):
    job_run_id: str
    job_key: str
    job_type: str
    workflow_id: str
    workflow_run_id: str
    status: str
    scheduled_for: str
    payload: dict[str, Any]


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


def _queue_job(
    *,
    job_type: str,
    job_key: str,
    payload: dict[str, Any],
    db_target: str | None,
) -> QueuedCompanyValuationJob:
    del db_target
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
    workflow_id = routine_workflow_id(job_key)
    try:
        started = start_ad_hoc_job_workflow(
            job_type=job_type,
            job_key=job_key,
            workflow_id=workflow_id,
            payload=full_payload,
        )
    except Exception as exc:
        raise RuntimeError(f"Company valuation queueing failed: {exc}") from exc
    if started is None:
        raise RuntimeError("Company valuation workflow was not started.")
    return QueuedCompanyValuationJob(
        job_run_id=started.job_run_id,
        job_key=job_key,
        job_type=job_type,
        workflow_id=workflow_id,
        workflow_run_id=started.workflow_run_id,
        status="started",
        scheduled_for=scheduled_for_iso,
        payload=full_payload,
    )


def enqueue_company_valuation_bootstrap_job(
    request: CompanyValuationBootstrapRequest,
    *,
    db_target: str | None = None,
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
    }
    return _queue_job(
        job_type=COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
        job_key=job_key,
        payload=payload,
        db_target=db_target,
    )


def enqueue_company_valuation_screen_materialize_job(
    request: CompanyValuationScreenRefreshRequest,
    *,
    db_target: str | None = None,
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
    }
    return _queue_job(
        job_type=COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
        job_key=job_key,
        payload=payload,
        db_target=db_target,
    )


def enqueue_company_valuation_resolve_unresolved_job(
    request: ResolveUnresolvedInstitutionalPositionsRequest,
    *,
    db_target: str | None = None,
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
    }
    return _queue_job(
        job_type=COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
        job_key=job_key,
        payload=payload,
        db_target=db_target,
    )


__all__ = [
    "CompanyValuationScreenRefreshRequest",
    "QueuedCompanyValuationJob",
    "enqueue_company_valuation_bootstrap_job",
    "enqueue_company_valuation_resolve_unresolved_job",
    "enqueue_company_valuation_screen_materialize_job",
]

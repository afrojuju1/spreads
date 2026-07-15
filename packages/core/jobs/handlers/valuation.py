from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from core.jobs.contracts import RoutineExecutionContext, RoutineHandler, RoutineOutcome
from core.jobs.registry import (
    COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
    COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
    COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
)
from core.services.company_valuation.bootstrap import (
    CompanyValuationBootstrapRequest,
    bootstrap_company_valuation,
)
from core.services.company_valuation.screening import materialize_company_valuation_screen
from core.services.company_valuation.unresolved import (
    ResolveUnresolvedInstitutionalPositionsRequest,
    resolve_unresolved_institutional_positions,
)
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_date, parse_datetime, render_value
from core.value_coercion import coerce_int


def _normalized_tickers(payload: Mapping[str, Any]) -> tuple[str, ...]:
    values = payload.get("tickers")
    if not isinstance(values, list):
        return ()
    return tuple(dict.fromkeys(str(value or "").upper().strip() for value in values if str(value or "").strip()))


def _bootstrap_projection(result: Mapping[str, Any]) -> dict[str, Any]:
    ticker_rows = list(result.get("ticker_results") or [])
    errors = [str(item) for item in list(result.get("errors") or [])]
    compact_rows = [
        {
            "ticker": row.get("ticker"),
            "error": row.get("error"),
            "quality_score": ((row.get("recompute") or {}).get("quality_score") if isinstance(row, Mapping) else None),
            "intrinsic_value_mid": ((row.get("recompute") or {}).get("intrinsic_value_mid") if isinstance(row, Mapping) else None),
            "valuation_gap": ((row.get("recompute") or {}).get("valuation_gap") if isinstance(row, Mapping) else None),
        }
        for row in ticker_rows[:25]
        if isinstance(row, Mapping)
    ]
    return dict(
        render_value(
            {
                "status": result.get("status"),
                "started_at": result.get("started_at"),
                "completed_at": result.get("completed_at"),
                "tickers": list(result.get("tickers") or []),
                "ticker_count": len(list(result.get("tickers") or [])),
                "ticker_results": compact_rows,
                "ticker_result_count": len(ticker_rows),
                "screening": result.get("screening"),
                "universe_bootstrap": result.get("universe_bootstrap"),
                "treasury_curve": result.get("treasury_curve"),
                "errors": errors[:25],
                "error_count": len(errors),
            }
        )
    )


def _outcome(result: Mapping[str, Any], projection: Mapping[str, Any]) -> RoutineOutcome:
    if result.get("status") == "skipped":
        return RoutineOutcome.skipped(projection)
    return RoutineOutcome.succeeded(projection)


def _bootstrap(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    payload = context.payload
    result = bootstrap_company_valuation(
        CompanyValuationBootstrapRequest(
            tickers=_normalized_tickers(payload),
            as_of=parse_datetime(payload.get("as_of")),
            bootstrap_universe=bool(payload.get("bootstrap_universe", False)),
            universe_limit=coerce_int(payload.get("universe_limit")),
            refresh_treasury=bool(payload.get("refresh_treasury", True)),
            treasury_curve_date=(
                None
                if payload.get("treasury_curve_date") in (None, "")
                else parse_date(str(payload["treasury_curve_date"]))
            ),
            refresh_filings=bool(payload.get("refresh_filings", True)),
            filings_since=parse_datetime(payload.get("filings_since")),
            filings_until=parse_datetime(payload.get("filings_until")),
            refresh_insiders=bool(payload.get("refresh_insiders", True)),
            refresh_beneficial_ownership=bool(payload.get("refresh_beneficial_ownership", True)),
            ownership_since=parse_datetime(payload.get("ownership_since")),
            ownership_until=parse_datetime(payload.get("ownership_until")),
            refresh_market_inputs=bool(payload.get("refresh_market_inputs", True)),
            recompute=bool(payload.get("recompute", True)),
            materialize_screen=bool(payload.get("materialize_screen", True)),
            continue_on_error=bool(payload.get("continue_on_error", True)),
            config_root=None if payload.get("config_root") in (None, "") else str(payload["config_root"]),
        ),
        repository=CompanyValuationRepository(context.database_url),
        heartbeat=context.heartbeat,
    ).to_payload()
    return _outcome(result, _bootstrap_projection(result))


def _screen_materialize(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    payload = context.payload
    result = materialize_company_valuation_screen(
        as_of=parse_datetime(payload.get("as_of")),
        template_id=None if payload.get("template_id") in (None, "") else str(payload["template_id"]),
        tickers=_normalized_tickers(payload) or None,
        issuer_limit=coerce_int(payload.get("issuer_limit")),
        supported_only=bool(payload.get("supported_only", True)),
        stressed_operator_only=bool(payload.get("stressed_operator_only", False)),
        repository=CompanyValuationRepository(context.database_url),
        config_root=None if payload.get("config_root") in (None, "") else str(payload["config_root"]),
        heartbeat=context.heartbeat,
    ).to_payload()
    return _outcome(result, dict(render_value(result)))


def _resolve_unresolved(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    payload = context.payload
    result = resolve_unresolved_institutional_positions(
        ResolveUnresolvedInstitutionalPositionsRequest(
            report_period=(
                None if payload.get("report_period") in (None, "") else parse_date(str(payload["report_period"]))
            ),
            limit_rows=int(payload.get("limit_rows", 20000) or 20000),
            batch_cusips=int(payload.get("batch_cusips", 50) or 50),
            max_attempts=int(payload.get("max_attempts", 5) or 5),
        ),
        repository=CompanyValuationRepository(context.database_url),
        heartbeat=context.heartbeat,
    ).to_payload()
    return _outcome(result, dict(render_value(result)))


HANDLERS: Mapping[str, RoutineHandler] = MappingProxyType(
    {
        COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE: _bootstrap,
        COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE: _screen_materialize,
        COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE: _resolve_unresolved,
    }
)

__all__ = ["HANDLERS"]

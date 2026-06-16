from __future__ import annotations

from datetime import UTC, date, datetime
from collections.abc import Callable
from typing import Any

from core.services.company_valuation.contracts import CompanyValuationContractModel
from core.services.company_valuation.evaluation import recompute_company_valuation
from core.services.company_valuation.ingestion.market_inputs import (
    MarketInputsIngestionRequest,
    ingest_market_inputs,
)
from core.services.company_valuation.ingestion.sec_beneficial_ownership import (
    SecBeneficialOwnershipIngestionRequest,
    ingest_sec_beneficial_ownership,
)
from core.services.company_valuation.ingestion.sec_filings import (
    SecFilingsIngestionRequest,
    ingest_sec_filings,
)
from core.services.company_valuation.ingestion.sec_insiders import (
    SecInsidersIngestionRequest,
    ingest_sec_insiders,
)
from core.services.company_valuation.ingestion.sec_universe import (
    SecUniverseBootstrapRequest,
    bootstrap_sec_universe,
)
from core.services.company_valuation.ingestion.treasury import (
    TreasuryCurveIngestionRequest,
    ingest_treasury_curve,
)
from core.services.company_valuation.screening import materialize_company_valuation_screen
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.value_coercion import normalize_symbol


class CompanyValuationBootstrapTickerResult(CompanyValuationContractModel):
    ticker: str
    filings: dict[str, Any] | None = None
    insiders: dict[str, Any] | None = None
    beneficial_ownership: dict[str, Any] | None = None
    market_inputs: dict[str, Any] | None = None
    recompute: dict[str, Any] | None = None
    error: str | None = None


class CompanyValuationBootstrapRequest(CompanyValuationContractModel):
    tickers: tuple[str, ...]
    as_of: datetime | None = None
    bootstrap_universe: bool = False
    universe_limit: int | None = None
    refresh_treasury: bool = True
    treasury_curve_date: date | None = None
    refresh_filings: bool = True
    filings_since: datetime | None = None
    filings_until: datetime | None = None
    refresh_insiders: bool = True
    refresh_beneficial_ownership: bool = True
    ownership_since: datetime | None = None
    ownership_until: datetime | None = None
    refresh_market_inputs: bool = True
    recompute: bool = True
    materialize_screen: bool = True
    continue_on_error: bool = True
    config_root: str | None = None


class CompanyValuationBootstrapResult(CompanyValuationContractModel):
    status: str
    started_at: datetime
    completed_at: datetime
    tickers: tuple[str, ...]
    universe_bootstrap: dict[str, Any] | None = None
    treasury_curve: dict[str, Any] | None = None
    ticker_results: tuple[CompanyValuationBootstrapTickerResult, ...] = ()
    screening: dict[str, Any] | None = None
    errors: tuple[str, ...] = ()


def _normalized_tickers(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(symbol for value in values if (symbol := normalize_symbol(value)) is not None))


def _heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is not None:
        heartbeat()


def bootstrap_company_valuation(
    request: CompanyValuationBootstrapRequest,
    *,
    repository: CompanyValuationRepository | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> CompanyValuationBootstrapResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    normalized_tickers = _normalized_tickers(request.tickers)
    if not normalized_tickers:
        raise ValueError("at least one ticker is required")

    universe_payload = None
    if request.bootstrap_universe:
        _heartbeat(heartbeat)
        universe_payload = bootstrap_sec_universe(
            SecUniverseBootstrapRequest(
                limit=request.universe_limit,
                config_root=request.config_root,
            ),
            repository=repo,
        ).to_payload()

    treasury_payload = None
    if request.refresh_treasury:
        _heartbeat(heartbeat)
        treasury_payload = ingest_treasury_curve(
            TreasuryCurveIngestionRequest(curve_date=request.treasury_curve_date),
            repository=repo,
        ).to_payload()

    ticker_results: list[CompanyValuationBootstrapTickerResult] = []
    errors: list[str] = []
    for ticker in normalized_tickers:
        try:
            _heartbeat(heartbeat)
            filings_payload = None
            insiders_payload = None
            beneficial_payload = None
            market_payload = None
            recompute_payload = None
            if request.refresh_filings:
                filings_payload = ingest_sec_filings(
                    SecFilingsIngestionRequest(
                        ticker=ticker,
                        since=request.filings_since,
                        until=request.filings_until,
                        config_root=request.config_root,
                    ),
                    repository=repo,
                ).to_payload()
            if request.refresh_insiders:
                insiders_payload = ingest_sec_insiders(
                    SecInsidersIngestionRequest(
                        ticker=ticker,
                        since=request.ownership_since,
                        until=request.ownership_until,
                    ),
                    repository=repo,
                ).to_payload()
            if request.refresh_beneficial_ownership:
                beneficial_payload = ingest_sec_beneficial_ownership(
                    SecBeneficialOwnershipIngestionRequest(
                        ticker=ticker,
                        since=request.ownership_since,
                        until=request.ownership_until,
                    ),
                    repository=repo,
                ).to_payload()
            if request.refresh_market_inputs:
                market_payload = ingest_market_inputs(
                    MarketInputsIngestionRequest(ticker=ticker),
                    repository=repo,
                ).to_payload()
            if request.recompute:
                recompute_result = recompute_company_valuation(
                    ticker=ticker,
                    as_of=request.as_of,
                    repository=repo,
                    config_root=request.config_root,
                )
                recompute_payload = {
                    "feature_snapshot_id": recompute_result.feature_snapshot.get("feature_snapshot_id"),
                    "company_valuation_snapshot_id": recompute_result.company_valuation_snapshot.get(
                        "company_valuation_snapshot_id"
                    ),
                    "screening_row_id": recompute_result.screening_row.get("screening_row_id"),
                    "quality_score": recompute_result.screening_row.get("quality_score"),
                    "intrinsic_value_mid": recompute_result.screening_row.get("intrinsic_value_mid"),
                    "valuation_gap": recompute_result.screening_row.get("valuation_gap"),
                }
            ticker_results.append(
                CompanyValuationBootstrapTickerResult(
                    ticker=ticker,
                    filings=filings_payload,
                    insiders=insiders_payload,
                    beneficial_ownership=beneficial_payload,
                    market_inputs=market_payload,
                    recompute=recompute_payload,
                )
            )
        except Exception as exc:
            _heartbeat(heartbeat)
            errors.append(f"{ticker}: {exc}")
            ticker_results.append(
                CompanyValuationBootstrapTickerResult(
                    ticker=ticker,
                    error=str(exc),
                )
            )
            if not request.continue_on_error:
                raise

    screening_payload = None
    if request.materialize_screen:
        _heartbeat(heartbeat)
        screening_payload = materialize_company_valuation_screen(
            as_of=request.as_of,
            tickers=normalized_tickers,
            repository=repo,
            config_root=request.config_root,
            heartbeat=heartbeat,
        ).to_payload()

    completed_at = datetime.now(UTC)
    return CompanyValuationBootstrapResult(
        status="ok" if not errors else "partial",
        started_at=started_at,
        completed_at=completed_at,
        tickers=normalized_tickers,
        universe_bootstrap=universe_payload,
        treasury_curve=treasury_payload,
        ticker_results=tuple(ticker_results),
        screening=screening_payload,
        errors=tuple(errors),
    )


__all__ = [
    "CompanyValuationBootstrapRequest",
    "CompanyValuationBootstrapResult",
    "CompanyValuationBootstrapTickerResult",
    "bootstrap_company_valuation",
]

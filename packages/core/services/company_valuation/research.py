from __future__ import annotations

import json
import shutil
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from core.value_coercion import coerce_float
from core.services.alpaca import create_alpaca_client_from_env
from core.services.company_valuation.evaluation import (
    COMPANY_VALUATION_FEATURE_VERSION,
    EVALUATION_VERSION,
    recompute_company_valuation,
)
from core.services.company_valuation.templates import resolve_company_valuation_template
from core.services.market_dates import market_session_window
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_datetime

ResearchExportFormat = Literal["parquet", "jsonl"]

DEFAULT_RESEARCH_TEMPLATE_IDS = (
    "industrial_manufacturing",
    "energy_asset_heavy",
)
PERIODIC_RESEARCH_FORMS = {"10-K", "10-Q"}


@dataclass(frozen=True)
class CompanyValuationResearchExportRequest:
    years: int = 10
    end_as_of: datetime | None = None
    template_ids: tuple[str, ...] = DEFAULT_RESEARCH_TEMPLATE_IDS
    tickers: tuple[str, ...] | None = None
    issuer_limit: int | None = None
    output_root: str = "outputs/company_valuation/research"
    output_format: ResearchExportFormat = "parquet"
    config_root: str | None = None
    include_market_context: bool = True
    periodic_forms: tuple[str, ...] = ("10-K", "10-Q")


@dataclass(frozen=True)
class CompanyValuationResearchExportResult:
    status: str
    started_at: datetime
    completed_at: datetime
    years: int
    start_as_of: datetime
    end_as_of: datetime
    output_root: str
    output_format: ResearchExportFormat
    row_count: int
    issuers_considered: int
    issuers_exported: int
    template_counts: dict[str, int] = field(default_factory=dict)
    errors: tuple[str, ...] = ()
    manifest_path: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _normalized_templates(values: tuple[str, ...] | None) -> tuple[str, ...]:
    normalized = tuple(dict.fromkeys(str(value).strip() for value in (values or ()) if str(value or "").strip()))
    return normalized or DEFAULT_RESEARCH_TEMPLATE_IDS


def _normalized_tickers(values: tuple[str, ...] | None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value).upper().strip() for value in (values or ()) if str(value or "").strip()))


def _normalized_forms(values: tuple[str, ...] | None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value).upper().strip() for value in (values or ()) if str(value or "").strip())) or ("10-K", "10-Q")


def _normalized_end_as_of(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    return value.astimezone(UTC)


def _subtract_years(value: datetime, years: int) -> datetime:
    years = max(int(years), 1)
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year - years)


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return None if value is None else str(value)


def _json_scalar(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return _iso(value)
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, sort_keys=True, default=_iso)
    return value


def _periodic_filings_for_export(
    *,
    repository: CompanyValuationRepository,
    issuer_id: str,
    start_as_of: datetime,
    end_as_of: datetime,
    periodic_forms: tuple[str, ...],
) -> list[dict[str, Any]]:
    estimated_limit = max(160, (end_as_of.year - start_as_of.year + 2) * 20)
    filings = repository.list_filings_before(
        issuer_id=issuer_id,
        as_of=end_as_of,
        limit=estimated_limit,
    )
    rows: list[dict[str, Any]] = []
    seen_accessions: set[str] = set()
    allowed = set(periodic_forms)
    for filing in sorted(
        filings,
        key=lambda row: parse_datetime(row.get("available_at")) or datetime.min.replace(tzinfo=UTC),
    ):
        form_type = str(filing.get("form_type") or "").upper().strip()
        if form_type not in allowed:
            continue
        available_at = parse_datetime(filing.get("available_at"))
        if available_at is None:
            continue
        available_at = available_at.astimezone(UTC)
        if available_at < start_as_of or available_at > end_as_of:
            continue
        accession_no = str(filing.get("accession_no") or "").strip()
        if not accession_no or accession_no in seen_accessions:
            continue
        seen_accessions.add(accession_no)
        rows.append(filing)
    return rows


def _historical_bars_by_ticker(
    *,
    tickers: tuple[str, ...],
    start_as_of: datetime,
    end_as_of: datetime,
) -> dict[str, list[dict[str, Any]]]:
    if not tickers:
        return {}
    client = create_alpaca_client_from_env(request_timeout_seconds=30.0)
    bars_by_ticker: dict[str, list[dict[str, Any]]] = {}
    start = (start_as_of.date() - timedelta(days=14)).isoformat()
    end = end_as_of.date().isoformat()
    symbol_asof = end_as_of.date().isoformat()
    for ticker in tickers:
        bars_by_ticker[ticker] = client.get_stock_bars(
            ticker,
            timeframe="1Day",
            start=start,
            end=end,
            adjustment="raw",
            asof=symbol_asof,
            limit=10000,
        )
    return bars_by_ticker


def _bar_date(bar: dict[str, Any]) -> date | None:
    timestamp = parse_datetime(bar.get("t"))
    if timestamp is None:
        return None
    return timestamp.date()


def _latest_close_bar_at_or_before(
    *,
    bars: list[dict[str, Any]],
    as_of: datetime,
) -> dict[str, Any] | None:
    session_window = market_session_window("NYSE", as_of.date())
    allow_same_day_close = False
    if session_window is not None:
        _market_open, market_close = session_window
        allow_same_day_close = as_of.astimezone(market_close.tzinfo) >= market_close
    cutoff_date = as_of.date() if allow_same_day_close else (as_of.date() - timedelta(days=1))
    candidate: dict[str, Any] | None = None
    candidate_date: date | None = None
    for bar in bars:
        bar_day = _bar_date(bar)
        if bar_day is None or bar_day > cutoff_date:
            continue
        if candidate_date is None or bar_day > candidate_date:
            candidate = bar
            candidate_date = bar_day
    return candidate


def _market_context(
    *,
    as_of: datetime,
    intrinsic_value_mid: float | None,
    financial_features: dict[str, Any],
    bars: list[dict[str, Any]],
) -> dict[str, Any]:
    bar = _latest_close_bar_at_or_before(bars=bars, as_of=as_of)
    if bar is None:
        return {
            "market_price_close": None,
            "market_price_timestamp": None,
            "market_bar_date": None,
            "market_cap_at_as_of": None,
            "enterprise_value_at_as_of": None,
            "valuation_gap_at_as_of": None,
            "ev_ebit_at_as_of": None,
            "ev_fcf_at_as_of": None,
            "pb_at_as_of": None,
            "ps_at_as_of": None,
            "historical_market_context_missing": True,
        }
    price = coerce_float(bar.get("c"))
    if price is None or price <= 0.0:
        return {
            "market_price_close": None,
            "market_price_timestamp": None,
            "market_bar_date": None,
            "market_cap_at_as_of": None,
            "enterprise_value_at_as_of": None,
            "valuation_gap_at_as_of": None,
            "ev_ebit_at_as_of": None,
            "ev_fcf_at_as_of": None,
            "pb_at_as_of": None,
            "ps_at_as_of": None,
            "historical_market_context_missing": True,
        }
    shares = coerce_float(financial_features.get("diluted_shares_latest")) or coerce_float(financial_features.get("shares_outstanding_latest"))
    market_cap = None if shares is None else price * shares
    debt = coerce_float(financial_features.get("long_term_debt_latest")) or coerce_float(financial_features.get("total_liabilities_latest")) or 0.0
    cash = coerce_float(financial_features.get("cash_and_equivalents_latest")) or 0.0
    enterprise_value = None if market_cap is None else market_cap + debt - cash
    operating_income_ttm = coerce_float(financial_features.get("operating_income_ttm"))
    free_cash_flow_ttm = coerce_float(financial_features.get("free_cash_flow_ttm"))
    book_value = coerce_float(financial_features.get("stockholders_equity_latest"))
    revenue_ttm = coerce_float(financial_features.get("revenue_ttm"))
    valuation_gap = None
    if intrinsic_value_mid not in (None, 0.0):
        valuation_gap = (intrinsic_value_mid / price) - 1.0
    return {
        "market_price_close": round(price, 6),
        "market_price_timestamp": _iso(parse_datetime(bar.get("t"))),
        "market_bar_date": _iso(_bar_date(bar)),
        "market_cap_at_as_of": None if market_cap is None else round(market_cap, 4),
        "enterprise_value_at_as_of": None if enterprise_value is None else round(enterprise_value, 4),
        "valuation_gap_at_as_of": None if valuation_gap is None else round(valuation_gap, 6),
        "ev_ebit_at_as_of": (
            None if enterprise_value in (None, 0.0) or operating_income_ttm in (None, 0.0) else round(enterprise_value / operating_income_ttm, 6)
        ),
        "ev_fcf_at_as_of": (
            None if enterprise_value in (None, 0.0) or free_cash_flow_ttm in (None, 0.0) else round(enterprise_value / free_cash_flow_ttm, 6)
        ),
        "pb_at_as_of": None if market_cap in (None, 0.0) or book_value in (None, 0.0) else round(market_cap / book_value, 6),
        "ps_at_as_of": None if market_cap in (None, 0.0) or revenue_ttm in (None, 0.0) else round(market_cap / revenue_ttm, 6),
        "historical_market_context_missing": False,
    }


def _coverage_metadata(
    *,
    template_id: str,
    financial_features: dict[str, Any],
    config_root: str | None,
) -> dict[str, Any]:
    template = resolve_company_valuation_template(template_id, config_root)
    missing = [key for key in template.required_features if financial_features.get(key) is None]
    coverage = 1.0
    if template.required_features:
        coverage = 1.0 - (len(missing) / len(template.required_features))
    return {
        "required_feature_count": len(template.required_features),
        "missing_required_feature_count": len(missing),
        "missing_required_features_json": json.dumps(missing),
        "required_feature_coverage": round(coverage, 6),
    }


def _flatten_result_row(
    *,
    issuer_row: dict[str, Any],
    filing_row: dict[str, Any],
    evaluation_result: Any,
    config_root: str | None,
    market_context: dict[str, Any],
) -> dict[str, Any]:
    feature_snapshot = dict(evaluation_result.feature_snapshot)
    valuation_snapshot = dict(evaluation_result.company_valuation_snapshot)
    document = dict(evaluation_result.document)
    financial_features = dict(feature_snapshot.get("financial_features_json") or {})
    ownership_features = dict(feature_snapshot.get("ownership_features_json") or {})
    quality_payload = dict(document.get("quality") or {})
    valuation_payload = dict(document.get("valuation") or {})
    coverage = _coverage_metadata(
        template_id=str(issuer_row.get("template_id") or ""),
        financial_features=financial_features,
        config_root=config_root,
    )

    row: dict[str, Any] = {
        "issuer_id": str(issuer_row.get("issuer_id") or ""),
        "cik": str(issuer_row.get("cik") or ""),
        "ticker": str(issuer_row.get("ticker") or ""),
        "company_name": str(issuer_row.get("company_name") or ""),
        "template_id": str(issuer_row.get("template_id") or ""),
        "template_version": str(issuer_row.get("template_version") or ""),
        "as_of": _iso(feature_snapshot.get("as_of")),
        "as_of_year": parse_datetime(feature_snapshot.get("as_of")).year if parse_datetime(feature_snapshot.get("as_of")) is not None else None,
        "feature_snapshot_id": str(feature_snapshot.get("feature_snapshot_id") or ""),
        "feature_version": str(feature_snapshot.get("feature_version") or ""),
        "company_valuation_snapshot_id": str(valuation_snapshot.get("company_valuation_snapshot_id") or ""),
        "evaluation_version": str(valuation_snapshot.get("evaluation_version") or ""),
        "filing_id": str(filing_row.get("filing_id") or ""),
        "accession_no": str(filing_row.get("accession_no") or ""),
        "form_type": str(filing_row.get("form_type") or ""),
        "period_end": _iso(filing_row.get("period_end")),
        "accepted_at": _iso(filing_row.get("accepted_at")),
        "available_at": _iso(filing_row.get("available_at")),
        "quality_score": valuation_snapshot.get("quality_score"),
        "ownership_score": valuation_snapshot.get("ownership_score"),
        "intrinsic_value_bear": valuation_snapshot.get("intrinsic_value_bear"),
        "intrinsic_value_base": valuation_snapshot.get("intrinsic_value_base"),
        "intrinsic_value_bull": valuation_snapshot.get("intrinsic_value_bull"),
        "intrinsic_value_mid": valuation_snapshot.get("intrinsic_value_mid"),
        "quality_confidence": valuation_snapshot.get("quality_confidence"),
        "valuation_confidence": valuation_snapshot.get("valuation_confidence"),
        "limited_coverage_flag": valuation_snapshot.get("limited_coverage_flag"),
        "top_reason_codes_json": json.dumps(valuation_snapshot.get("top_reason_codes_json") or []),
        "quality_reason_codes_json": json.dumps(quality_payload.get("reason_codes") or []),
        "valuation_reason_codes_json": json.dumps(valuation_payload.get("reason_codes") or []),
        "valuation_assumption_summary_json": json.dumps(
            valuation_payload.get("assumption_summary") or {},
            sort_keys=True,
            default=_iso,
        ),
        "ownership_signal_reason_codes_json": json.dumps(((document.get("ownership") or {}).get("signal") or {}).get("reason_codes") or []),
        **coverage,
        **market_context,
    }

    for key, value in (quality_payload.get("sub_scores") or {}).items():
        row[f"quality_sub__{key}"] = _json_scalar(value)
    for key, value in (quality_payload.get("factor_contributions") or {}).items():
        row[f"quality_contrib__{key}"] = _json_scalar(value)
    for key, value in financial_features.items():
        row[f"ff__{key}"] = _json_scalar(value)
    for key, value in ownership_features.items():
        row[f"of__{key}"] = _json_scalar(value)

    return row


def _write_jsonl_dataset(
    *,
    rows: list[dict[str, Any]],
    dataset_root: Path,
) -> None:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in rows:
        template_id = str(row.get("template_id") or "unknown")
        year = int(row.get("as_of_year") or 0)
        grouped.setdefault((template_id, year), []).append(row)
    for (template_id, year), group_rows in grouped.items():
        partition_dir = dataset_root / f"template_id={template_id}" / f"as_of_year={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / "part-00000.jsonl"
        with output_path.open("w", encoding="utf-8") as handle:
            for row in group_rows:
                handle.write(json.dumps(row, sort_keys=True, default=_iso))
                handle.write("\n")


def export_company_valuation_research_dataset(
    request: CompanyValuationResearchExportRequest,
    *,
    repository: CompanyValuationRepository | None = None,
) -> CompanyValuationResearchExportResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    end_as_of = _normalized_end_as_of(request.end_as_of)
    start_as_of = _subtract_years(end_as_of, request.years)
    template_ids = _normalized_templates(request.template_ids)
    tickers = _normalized_tickers(request.tickers)
    periodic_forms = _normalized_forms(request.periodic_forms)

    issuers = [
        row
        for row in repo.list_issuers()
        if str(row.get("template_id") or "") in set(template_ids) and (not tickers or str(row.get("ticker") or "").upper() in set(tickers))
    ]
    issuers = sorted(
        issuers,
        key=lambda row: (
            str(row.get("template_id") or ""),
            str(row.get("ticker") or ""),
        ),
    )
    if request.issuer_limit is not None:
        issuers = issuers[: max(int(request.issuer_limit), 0)]

    bars_by_ticker: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    if request.include_market_context:
        try:
            bars_by_ticker = _historical_bars_by_ticker(
                tickers=tuple(str(row.get("ticker") or "").upper() for row in issuers if str(row.get("ticker") or "").strip()),
                start_as_of=start_as_of,
                end_as_of=end_as_of,
            )
        except Exception as exc:
            errors.append(f"historical_market_context: {exc}")

    rows: list[dict[str, Any]] = []
    issuers_exported = 0
    for issuer_row in issuers:
        issuer_id = str(issuer_row.get("issuer_id") or "")
        ticker = str(issuer_row.get("ticker") or "").upper()
        filing_rows = _periodic_filings_for_export(
            repository=repo,
            issuer_id=issuer_id,
            start_as_of=start_as_of,
            end_as_of=end_as_of,
            periodic_forms=periodic_forms,
        )
        issuer_row_count = 0
        for filing_row in filing_rows:
            as_of_dt = parse_datetime(filing_row.get("available_at"))
            if as_of_dt is None:
                continue
            try:
                evaluation_result = recompute_company_valuation(
                    issuer_id=issuer_id,
                    as_of=as_of_dt.astimezone(UTC),
                    repository=repo,
                    config_root=request.config_root,
                    persist=False,
                )
                financial_features = dict(evaluation_result.feature_snapshot.get("financial_features_json") or {})
                market_context = _market_context(
                    as_of=as_of_dt.astimezone(UTC),
                    intrinsic_value_mid=coerce_float(evaluation_result.company_valuation_snapshot.get("intrinsic_value_mid")),
                    financial_features=financial_features,
                    bars=bars_by_ticker.get(ticker, []),
                )
                rows.append(
                    _flatten_result_row(
                        issuer_row=issuer_row,
                        filing_row=filing_row,
                        evaluation_result=evaluation_result,
                        config_root=request.config_root,
                        market_context=market_context,
                    )
                )
                issuer_row_count += 1
            except Exception as exc:
                errors.append(f"{ticker}:{filing_row.get('accession_no')}: {exc}")
        if issuer_row_count > 0:
            issuers_exported += 1

    output_root = Path(request.output_root)
    dataset_root = output_root / f"feature_version={COMPANY_VALUATION_FEATURE_VERSION}" / f"evaluation_version={EVALUATION_VERSION}"
    if dataset_root.exists():
        shutil.rmtree(dataset_root)
    dataset_root.mkdir(parents=True, exist_ok=True)

    if rows:
        if request.output_format == "parquet":
            dataframe = pd.DataFrame(rows)
            dataframe.to_parquet(
                dataset_root,
                engine="pyarrow",
                index=False,
                partition_cols=["template_id", "as_of_year"],
            )
        else:
            _write_jsonl_dataset(rows=rows, dataset_root=dataset_root)

    template_counts = dict(Counter(str(row.get("template_id") or "") for row in rows))
    manifest = {
        "dataset_version": "v1",
        "feature_version": COMPANY_VALUATION_FEATURE_VERSION,
        "evaluation_version": EVALUATION_VERSION,
        "years": int(request.years),
        "start_as_of": _iso(start_as_of),
        "end_as_of": _iso(end_as_of),
        "output_format": request.output_format,
        "template_ids": list(template_ids),
        "tickers": list(tickers),
        "row_count": len(rows),
        "issuers_considered": len(issuers),
        "issuers_exported": issuers_exported,
        "template_counts": template_counts,
        "periodic_forms": list(periodic_forms),
        "include_market_context": bool(request.include_market_context),
        "errors": errors,
    }
    manifest_path = output_root / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, default=_iso) + "\n",
        encoding="utf-8",
    )

    completed_at = datetime.now(UTC)
    return CompanyValuationResearchExportResult(
        status="ok" if rows else "no_rows",
        started_at=started_at,
        completed_at=completed_at,
        years=int(request.years),
        start_as_of=start_as_of,
        end_as_of=end_as_of,
        output_root=str(dataset_root),
        output_format=request.output_format,
        row_count=len(rows),
        issuers_considered=len(issuers),
        issuers_exported=issuers_exported,
        template_counts=template_counts,
        errors=tuple(errors),
        manifest_path=str(manifest_path),
    )


__all__ = [
    "CompanyValuationResearchExportRequest",
    "CompanyValuationResearchExportResult",
    "DEFAULT_RESEARCH_TEMPLATE_IDS",
    "export_company_valuation_research_dataset",
]

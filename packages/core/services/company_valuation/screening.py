from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from collections.abc import Callable
from typing import Any

from core.common import clamp
from core.services.company_valuation.evaluation import recompute_company_valuation
from core.services.company_valuation.ids import normalize_ticker
from core.services.company_valuation.taxonomy import (
    resolve_company_valuation_taxonomy_context,
    supported_company_valuation_tickers,
)
from core.services.company_valuation.templates import (
    resolve_company_valuation_effective_template,
)
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_datetime


@dataclass(frozen=True)
class CompanyValuationScreenMaterializationResult:
    as_of: datetime
    issuers_considered: int
    issuers_recomputed: int
    rows_ranked: int

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _normalized_as_of(value: str | datetime | None) -> datetime:
    parsed = parse_datetime(value) if isinstance(value, str) else value
    if parsed is None:
        return datetime.now(UTC)
    return parsed.astimezone(UTC)


def _heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is not None:
        heartbeat()


def _normalized_tickers(values: tuple[str, ...] | None) -> tuple[str, ...] | None:
    normalized = tuple(
        dict.fromkeys(
            normalize_ticker(value)
            for value in (values or ())
            if str(value or "").strip()
        )
    )
    return normalized or None


def _resolved_screen_scope_tickers(
    *,
    tickers: tuple[str, ...] | None,
    supported_only: bool,
    config_root: str | None,
) -> tuple[str, ...] | None:
    normalized_tickers = _normalized_tickers(tickers)
    if not supported_only:
        return normalized_tickers
    supported_tickers = _normalized_tickers(
        supported_company_valuation_tickers(config_root)
    )
    if not supported_tickers:
        return ()
    if not normalized_tickers:
        return supported_tickers
    supported_set = set(supported_tickers)
    return tuple(
        ticker for ticker in normalized_tickers if ticker in supported_set
    )


def _normalized_screen_filters(
    *,
    template_id: str | None,
    stressed_operator_only: bool,
) -> tuple[str | None, bool]:
    normalized_template_id = str(template_id or "").strip() or None
    normalized_stressed_only = bool(stressed_operator_only)
    if normalized_template_id == "stressed_operator":
        return ("energy_asset_heavy", True)
    return (normalized_template_id, normalized_stressed_only)


def _enrich_screen_row(
    *,
    row: dict[str, Any],
    issuer_row: dict[str, Any],
    config_root: str | None,
) -> dict[str, Any]:
    resolution = resolve_company_valuation_taxonomy_context(
        cik=str(issuer_row["cik"]),
        ticker=str(issuer_row.get("ticker") or "") or None,
        company_name=str(issuer_row["company_name"]),
        sic=issuer_row.get("sic"),
        sic_title=issuer_row.get("sic_description"),
        naics=issuer_row.get("naics"),
        config_root=config_root,
    )
    effective_template = resolve_company_valuation_effective_template(
        issuer_row=issuer_row,
        config_root=config_root,
    )
    enriched = dict(row)
    enriched["effective_template_id"] = effective_template.template_id
    enriched["effective_template_version"] = effective_template.template_version
    enriched["support_status"] = resolution.support.status
    enriched["support_reason"] = resolution.support.reason
    enriched["in_curated_universe"] = resolution.support.in_curated_universe
    enriched["expected_template_id"] = resolution.support.expected_template_id
    enriched["expected_template_match"] = resolution.support.expected_template_match
    return enriched


def _screen_rank_score(row: dict[str, Any]) -> float:
    quality_score = float(row.get("quality_score") or 0.0)
    valuation_gap = float(row.get("valuation_gap") or 0.0)
    quality_confidence = float(row.get("quality_confidence") or 0.0) * 100.0
    valuation_confidence = float(row.get("valuation_confidence") or 0.0) * 100.0
    confidence_score = (quality_confidence + valuation_confidence) / 2.0
    valuation_score = clamp((valuation_gap + 0.20) / 0.80, 0.0, 1.0) * 100.0
    score = (quality_score * 0.55) + (valuation_score * 0.30) + (confidence_score * 0.15)
    if bool(row.get("limited_coverage_flag")):
        score -= 10.0
    if bool(row.get("ownership_special_situation_flag")):
        score -= 5.0
    return round(score, 4)


def materialize_company_valuation_screen(
    *,
    as_of: str | datetime | None = None,
    template_id: str | None = None,
    tickers: tuple[str, ...] | None = None,
    issuer_limit: int | None = None,
    supported_only: bool = True,
    stressed_operator_only: bool = False,
    repository: CompanyValuationRepository | None = None,
    config_root: str | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> CompanyValuationScreenMaterializationResult:
    repo = repository or CompanyValuationRepository()
    as_of_dt = _normalized_as_of(as_of)
    resolved_template_id, normalized_stressed_only = _normalized_screen_filters(
        template_id=template_id,
        stressed_operator_only=stressed_operator_only,
    )
    resolved_tickers = _resolved_screen_scope_tickers(
        tickers=tickers,
        supported_only=supported_only,
        config_root=config_root,
    )
    if supported_only and resolved_tickers == ():
        issuer_rows = []
    else:
        issuer_rows = repo.list_issuers_for_screening(
            as_of=as_of_dt,
            template_id=resolved_template_id,
            tickers=resolved_tickers,
            stressed_operator_only=normalized_stressed_only,
            limit=issuer_limit,
        )
    recomputed = 0
    for issuer_row in issuer_rows:
        _heartbeat(heartbeat)
        recompute_company_valuation(
            issuer_id=str(issuer_row["issuer_id"]),
            as_of=as_of_dt,
            repository=repo,
            config_root=config_root,
        )
        recomputed += 1

    if supported_only and resolved_tickers == ():
        rows = []
    else:
        rows = repo.list_screening_rows(
            as_of=as_of_dt.date().isoformat(),
            template_id=resolved_template_id,
            tickers=resolved_tickers,
            stressed_operator_only=normalized_stressed_only,
            limit=50000,
        )
    ranked_rows = sorted(
        rows,
        key=lambda row: (
            _screen_rank_score(row),
            float(row.get("quality_score") or 0.0),
            float(row.get("valuation_gap") or -999.0),
            str(row.get("ticker") or ""),
        ),
        reverse=True,
    )
    template_counters: dict[str, int] = {}
    overall_rank = 0
    for row in ranked_rows:
        _heartbeat(heartbeat)
        overall_rank += 1
        template_key = str(row.get("template_id") or "")
        template_counters[template_key] = template_counters.get(template_key, 0) + 1
        payload = dict(row)
        payload["screen_rank_score"] = _screen_rank_score(row)
        payload["template_rank"] = template_counters[template_key]
        payload["overall_rank"] = overall_rank
        payload["top_reason_codes_json"] = payload.pop(
            "top_reason_codes",
            payload.get("top_reason_codes_json") or [],
        )
        repo.upsert_screening_row(payload)

    return CompanyValuationScreenMaterializationResult(
        as_of=as_of_dt,
        issuers_considered=len(issuer_rows),
        issuers_recomputed=recomputed,
        rows_ranked=len(ranked_rows),
    )


def list_company_valuation_screen(
    *,
    as_of: str | None = None,
    template_id: str | None = None,
    tickers: tuple[str, ...] | None = None,
    limit: int = 100,
    supported_only: bool = True,
    stressed_operator_only: bool = False,
    repository: CompanyValuationRepository | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    repo = repository or CompanyValuationRepository()
    resolved_as_of = as_of or repo.latest_screening_as_of()
    if not resolved_as_of:
        return {
            "as_of": None,
            "count": 0,
            "rows": [],
            "supported_only": supported_only,
            "stressed_operator_only": stressed_operator_only,
            "support_status_counts": {},
        }
    resolved_template_id, normalized_stressed_only = _normalized_screen_filters(
        template_id=template_id,
        stressed_operator_only=stressed_operator_only,
    )
    resolved_tickers = _resolved_screen_scope_tickers(
        tickers=tickers,
        supported_only=supported_only,
        config_root=config_root,
    )
    if supported_only and resolved_tickers == ():
        rows = []
    else:
        rows = repo.list_screening_rows(
            as_of=resolved_as_of,
            template_id=resolved_template_id,
            tickers=resolved_tickers,
            stressed_operator_only=normalized_stressed_only,
            limit=limit,
        )
    issuer_rows = repo.list_issuers(
        issuer_ids=tuple(
            dict.fromkeys(str(row["issuer_id"]) for row in rows if row.get("issuer_id"))
        ),
    )
    issuer_map = {
        str(issuer_row["issuer_id"]): issuer_row for issuer_row in issuer_rows
    }
    enriched_rows = [
        _enrich_screen_row(
            row=row,
            issuer_row=issuer_map[str(row["issuer_id"])],
            config_root=config_root,
        )
        for row in rows
        if str(row["issuer_id"]) in issuer_map
    ]
    support_status_counts: dict[str, int] = {}
    for row in enriched_rows:
        status = str(row.get("support_status") or "unknown")
        support_status_counts[status] = support_status_counts.get(status, 0) + 1
    return {
        "as_of": resolved_as_of,
        "template_id": template_id,
        "tickers": list(_normalized_tickers(tickers) or ()),
        "supported_only": supported_only,
        "stressed_operator_only": normalized_stressed_only,
        "count": len(enriched_rows),
        "support_status_counts": dict(sorted(support_status_counts.items())),
        "rows": enriched_rows,
    }


def get_company_valuation_document(
    *,
    ticker: str,
    as_of: str | datetime | None = None,
    repository: CompanyValuationRepository | None = None,
    recompute_if_missing: bool = False,
    config_root: str | None = None,
) -> dict[str, Any]:
    repo = repository or CompanyValuationRepository()
    issuer_row = repo.get_issuer(ticker=ticker)
    if issuer_row is None:
        raise ValueError(f"Unknown issuer for ticker {ticker}")
    snapshot = repo.get_latest_company_valuation(
        issuer_id=str(issuer_row["issuer_id"]),
        as_of=as_of,
    )
    if snapshot is None and recompute_if_missing:
        snapshot = recompute_company_valuation(
            issuer_id=str(issuer_row["issuer_id"]),
            as_of=as_of,
            repository=repo,
            config_root=config_root,
        ).company_valuation_snapshot
    if snapshot is None:
        raise ValueError(f"No company valuation snapshot available for ticker {ticker}")
    payload = snapshot.get("valuation")
    if isinstance(payload, dict):
        resolution = resolve_company_valuation_taxonomy_context(
            cik=str(issuer_row["cik"]),
            ticker=str(issuer_row.get("ticker") or ticker or "") or None,
            company_name=str(issuer_row["company_name"]),
            sic=issuer_row.get("sic"),
            sic_title=issuer_row.get("sic_description"),
            naics=issuer_row.get("naics"),
            config_root=config_root,
        )
        effective_template = resolve_company_valuation_effective_template(
            issuer_row=issuer_row,
            config_root=config_root,
        )
        enriched_payload = dict(payload)
        source_summary = dict(enriched_payload.get("source_summary") or {})
        source_summary["template_id"] = str(issuer_row.get("template_id") or "")
        source_summary["effective_template_id"] = effective_template.template_id
        source_summary["effective_template_version"] = effective_template.template_version
        source_summary["limited_coverage_flag"] = bool(
            issuer_row.get("limited_coverage_flag")
        )
        source_summary["stressed_operator_flag"] = bool(
            issuer_row.get("stressed_operator_flag")
        )
        enriched_payload["source_summary"] = source_summary
        enriched_payload["support"] = resolution.support.to_payload()
        return enriched_payload
    raise ValueError(f"Company valuation snapshot payload is unavailable for ticker {ticker}")


__all__ = [
    "CompanyValuationScreenMaterializationResult",
    "get_company_valuation_document",
    "list_company_valuation_screen",
    "materialize_company_valuation_screen",
]

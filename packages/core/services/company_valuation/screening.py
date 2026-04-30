from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from collections.abc import Callable
from typing import Any

from core.common import clamp
from core.services.company_valuation.evaluation import recompute_company_valuation
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
    repository: CompanyValuationRepository | None = None,
    config_root: str | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> CompanyValuationScreenMaterializationResult:
    repo = repository or CompanyValuationRepository()
    as_of_dt = _normalized_as_of(as_of)
    issuer_rows = repo.list_issuers_for_screening(
        as_of=as_of_dt,
        template_id=template_id,
        tickers=tickers,
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

    rows = repo.list_screening_rows(
        as_of=as_of_dt.date().isoformat(),
        template_id=None,
        tickers=tickers,
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
    repository: CompanyValuationRepository | None = None,
) -> dict[str, Any]:
    repo = repository or CompanyValuationRepository()
    resolved_as_of = as_of or repo.latest_screening_as_of()
    if not resolved_as_of:
        return {"as_of": None, "count": 0, "rows": []}
    rows = repo.list_screening_rows(
        as_of=resolved_as_of,
        template_id=template_id,
        tickers=tickers,
        limit=limit,
    )
    return {
        "as_of": resolved_as_of,
        "template_id": template_id,
        "tickers": list(tickers or ()),
        "count": len(rows),
        "rows": rows,
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
        return payload
    raise ValueError(f"Company valuation snapshot payload is unavailable for ticker {ticker}")


__all__ = [
    "CompanyValuationScreenMaterializationResult",
    "get_company_valuation_document",
    "list_company_valuation_screen",
    "materialize_company_valuation_screen",
]

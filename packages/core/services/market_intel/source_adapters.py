from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from core.common import parse_float, pick
from core.integrations.http_client import VendorHttpClient
from core.services.alpaca import create_alpaca_client_from_env
from core.services.market_intel.artifact_store import MarketIntelArtifactStore
from core.services.market_intel.contracts import (
    EvidenceItem,
    MarketIntelRequest,
    MarketIntelRun,
    SourceArtifact,
    SourceType,
    utc_now,
)
from core.services.market_intel.ids import build_artifact_id

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL_TEMPLATE = "https://data.sec.gov/submissions/CIK{cik}.json"
DEFAULT_SEC_USER_AGENT = "spreads-market-intel/0.1 contact=ops@example.invalid"
SEC_HTTP = VendorHttpClient(timeout_seconds=30, user_agent=DEFAULT_SEC_USER_AGENT)


@dataclass(frozen=True)
class SourceCollectionResult:
    artifacts: tuple[SourceArtifact, ...]
    evidence: tuple[EvidenceItem, ...]
    warnings: tuple[str, ...] = ()


def collect_sources(
    request: MarketIntelRequest,
    *,
    run: MarketIntelRun,
    store: MarketIntelArtifactStore,
) -> SourceCollectionResult:
    artifacts: list[SourceArtifact] = []
    evidence: list[EvidenceItem] = []
    warnings: list[str] = []
    for source_type in request.sources:
        try:
            result = _collect_source(source_type, request=request, run=run, store=store)
        except (Exception, SystemExit) as exc:
            warnings.append(f"{source_type} source failed: {exc}")
            store.append_agent_trace(
                run,
                "source_adapter_failed",
                {"source_type": source_type, "error": str(exc)},
            )
            continue
        artifacts.extend(result.artifacts)
        evidence.extend(result.evidence)
        warnings.extend(result.warnings)
    return SourceCollectionResult(
        artifacts=tuple(artifacts),
        evidence=tuple(evidence),
        warnings=tuple(warnings),
    )


def _collect_source(
    source_type: SourceType,
    *,
    request: MarketIntelRequest,
    run: MarketIntelRun,
    store: MarketIntelArtifactStore,
) -> SourceCollectionResult:
    store.append_agent_trace(run, "source_adapter_started", {"source_type": source_type})
    if source_type == "sec":
        result = _collect_sec_source(request=request, run=run, store=store)
    elif source_type == "market":
        result = _collect_market_source(request=request, run=run, store=store)
    else:
        result = SourceCollectionResult(
            artifacts=(),
            evidence=(),
            warnings=(f"{source_type} source adapter is not implemented yet",),
        )
    store.append_agent_trace(
        run,
        "source_adapter_completed",
        {
            "source_type": source_type,
            "artifact_count": len(result.artifacts),
            "evidence_count": len(result.evidence),
            "warning_count": len(result.warnings),
        },
    )
    return result


def _collect_sec_source(
    *,
    request: MarketIntelRequest,
    run: MarketIntelRun,
    store: MarketIntelArtifactStore,
) -> SourceCollectionResult:
    fetched_at = utc_now()
    company_tickers = _fetch_sec_json(SEC_COMPANY_TICKERS_URL)
    ticker_record = _find_sec_ticker_record(company_tickers, run.ticker)
    if ticker_record is None:
        return SourceCollectionResult(
            artifacts=(),
            evidence=(),
            warnings=(f"SEC ticker lookup did not find {run.ticker}",),
        )

    cik = str(ticker_record["cik_str"]).zfill(10)
    submissions_url = SEC_SUBMISSIONS_URL_TEMPLATE.format(cik=cik)
    submissions = _fetch_sec_json(submissions_url)
    normalized = _normalize_sec_payload(
        ticker=run.ticker,
        ticker_record=ticker_record,
        submissions=submissions,
    )
    raw_path = store.write_json(run.run_dir / "raw" / "sec" / "submissions.json", submissions)
    store.write_json(run.run_dir / "raw" / "sec" / "ticker_lookup.json", ticker_record)
    normalized_path = store.write_json(
        run.run_dir / "normalized" / "sec" / "company_profile.json",
        normalized,
    )
    artifact = SourceArtifact(
        artifact_id=build_artifact_id(run.run_id, "sec", cik),
        run_id=run.run_id,
        ticker=run.ticker,
        source_type="sec",
        source_name="SEC submissions API",
        source_url=submissions_url,
        fetched_at=fetched_at,
        observed_at=_date_text_to_datetime(_latest_filing_value(normalized, "filingDate")),
        available_at=fetched_at,
        raw_path=_relative_to_run(raw_path, run.run_dir),
        normalized_path=_relative_to_run(normalized_path, run.run_dir),
        content_hash=_hash_payload(normalized),
        trust_tier=1,
        notes="SEC public company submissions and ticker lookup.",
    )
    evidence = [
        EvidenceItem(
            evidence_id=build_artifact_id(run.run_id, "sec", cik, "identity"),
            run_id=run.run_id,
            ticker=run.ticker,
            artifact_id=artifact.artifact_id,
            claim_type="fact",
            claim_text=f"SEC maps {run.ticker} to {normalized.get('company_name') or 'unknown company'} with CIK {cik}.",
            normalized_value={
                "cik": cik,
                "company_name": normalized.get("company_name"),
                "sic": normalized.get("sic"),
                "sic_description": normalized.get("sic_description"),
            },
            observed_at=None,
            available_at=fetched_at,
            supports_or_refutes="supports",
            source_rank=1,
            extraction_method="sec_submissions_json",
            extraction_confidence=0.98,
            final_confidence=0.98,
            tags=("sec", "identity"),
        )
    ]
    latest_form = _latest_filing_value(normalized, "form")
    latest_filing_date = _latest_filing_value(normalized, "filingDate")
    if latest_form and latest_filing_date:
        evidence.append(
            EvidenceItem(
                evidence_id=build_artifact_id(run.run_id, "sec", cik, "latest_filing"),
                run_id=run.run_id,
                ticker=run.ticker,
                artifact_id=artifact.artifact_id,
                claim_type="fact",
                claim_text=f"Latest SEC filing in the submissions feed is {latest_form}, filed {latest_filing_date}.",
                normalized_value={
                    "form": latest_form,
                    "filing_date": latest_filing_date,
                    "accession_number": _latest_filing_value(normalized, "accessionNumber"),
                },
                observed_at=_date_text_to_datetime(latest_filing_date),
                available_at=fetched_at,
                supports_or_refutes="supports",
                source_rank=1,
                extraction_method="sec_recent_filings",
                extraction_confidence=0.95,
                final_confidence=0.95,
                tags=("sec", "filing"),
            )
        )
    return SourceCollectionResult(artifacts=(artifact,), evidence=tuple(evidence))


def _collect_market_source(
    *,
    request: MarketIntelRequest,
    run: MarketIntelRun,
    store: MarketIntelArtifactStore,
) -> SourceCollectionResult:
    fetched_at = utc_now()
    client = create_alpaca_client_from_env(request_timeout_seconds=20.0)
    attempted_feeds = _stock_feeds()
    warnings: list[str] = []
    snapshot: dict[str, Any] | None = None
    feed_used: str | None = None
    for feed in attempted_feeds:
        try:
            snapshots = client.get_stock_snapshots([run.ticker], feed=feed)
        except Exception as exc:
            warnings.append(f"Alpaca stock snapshot feed {feed} failed: {exc}")
            continue
        candidate = snapshots.get(run.ticker)
        if isinstance(candidate, dict) and candidate:
            snapshot = candidate
            feed_used = feed
            break
    if snapshot is None:
        return SourceCollectionResult(
            artifacts=(),
            evidence=(),
            warnings=tuple(warnings or ["Alpaca returned no stock snapshot."]),
        )

    bars = _fetch_recent_daily_bars(
        client=client,
        ticker=run.ticker,
        as_of=request.as_of,
        warnings=warnings,
    )
    normalized = _normalize_market_payload(
        ticker=run.ticker,
        feed=feed_used or attempted_feeds[0],
        snapshot=snapshot,
        bars=bars,
    )
    raw_payload = {
        "feed": feed_used or attempted_feeds[0],
        "snapshot": snapshot,
        "bars": bars,
    }
    raw_path = store.write_json(run.run_dir / "raw" / "market" / "alpaca_snapshot.json", raw_payload)
    normalized_path = store.write_json(
        run.run_dir / "normalized" / "market" / "snapshot.json",
        normalized,
    )
    artifact = SourceArtifact(
        artifact_id=build_artifact_id(run.run_id, "market", run.ticker, fetched_at.isoformat()),
        run_id=run.run_id,
        ticker=run.ticker,
        source_type="market",
        source_name=f"Alpaca stock snapshot:{feed_used or attempted_feeds[0]}",
        source_url="https://data.alpaca.markets/v2/stocks/snapshots",
        fetched_at=fetched_at,
        observed_at=_date_text_to_datetime(str(normalized.get("price_observed_at") or "")),
        available_at=fetched_at,
        raw_path=_relative_to_run(raw_path, run.run_dir),
        normalized_path=_relative_to_run(normalized_path, run.run_dir),
        content_hash=_hash_payload(normalized),
        trust_tier=2,
        notes="Alpaca market data snapshot and recent daily bars.",
    )
    evidence: list[EvidenceItem] = []
    price = normalized.get("price")
    if price is not None:
        evidence.append(
            EvidenceItem(
                evidence_id=build_artifact_id(run.run_id, "market", run.ticker, "price"),
                run_id=run.run_id,
                ticker=run.ticker,
                artifact_id=artifact.artifact_id,
                claim_type="derived_metric",
                claim_text=f"Alpaca {feed_used or attempted_feeds[0]} snapshot marks {run.ticker} at {price}.",
                normalized_value={
                    "price": price,
                    "price_source": normalized.get("price_source"),
                    "feed": feed_used or attempted_feeds[0],
                },
                observed_at=_date_text_to_datetime(str(normalized.get("price_observed_at") or "")),
                available_at=fetched_at,
                supports_or_refutes="supports",
                source_rank=2,
                extraction_method="alpaca_stock_snapshot",
                extraction_confidence=0.9,
                final_confidence=0.9,
                tags=("market", "price"),
            )
        )
    latest_bar = normalized.get("latest_daily_bar")
    if isinstance(latest_bar, dict) and latest_bar.get("volume") is not None:
        evidence.append(
            EvidenceItem(
                evidence_id=build_artifact_id(run.run_id, "market", run.ticker, "volume"),
                run_id=run.run_id,
                ticker=run.ticker,
                artifact_id=artifact.artifact_id,
                claim_type="derived_metric",
                claim_text=f"Latest Alpaca daily bar volume for {run.ticker} is {latest_bar.get('volume')}.",
                normalized_value=latest_bar,
                observed_at=_date_text_to_datetime(str(latest_bar.get("timestamp") or "")),
                available_at=fetched_at,
                supports_or_refutes="supports",
                source_rank=2,
                extraction_method="alpaca_daily_bars",
                extraction_confidence=0.85,
                final_confidence=0.85,
                tags=("market", "volume"),
            )
        )
    return SourceCollectionResult(
        artifacts=(artifact,),
        evidence=tuple(evidence),
        warnings=tuple(warnings),
    )


def _fetch_sec_json(url: str) -> Any:
    return SEC_HTTP.request_json(
        "GET",
        url,
        "",
        headers={
            "Accept": "application/json",
            "User-Agent": os.environ.get("MARKET_INTEL_SEC_USER_AGENT") or os.environ.get("SEC_USER_AGENT") or DEFAULT_SEC_USER_AGENT,
        },
    )


def _find_sec_ticker_record(payload: Any, ticker: str) -> dict[str, Any] | None:
    normalized = str(ticker).upper()
    rows: list[Any]
    if isinstance(payload, dict):
        rows = list(payload.values())
    elif isinstance(payload, list):
        rows = list(payload)
    else:
        rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("ticker") or "").upper() == normalized:
            return dict(row)
    return None


def _normalize_sec_payload(
    *,
    ticker: str,
    ticker_record: dict[str, Any],
    submissions: Any,
) -> dict[str, Any]:
    payload = submissions if isinstance(submissions, dict) else {}
    filings = payload.get("filings") if isinstance(payload.get("filings"), dict) else {}
    recent = filings.get("recent") if isinstance(filings.get("recent"), dict) else {}
    recent_rows = _recent_filing_rows(recent, limit=12)
    return {
        "ticker": ticker,
        "cik": str(ticker_record.get("cik_str") or "").zfill(10),
        "company_name": ticker_record.get("title") or payload.get("name"),
        "entity_type": payload.get("entityType"),
        "sic": payload.get("sic"),
        "sic_description": payload.get("sicDescription"),
        "fiscal_year_end": payload.get("fiscalYearEnd"),
        "exchanges": payload.get("exchanges") if isinstance(payload.get("exchanges"), list) else [],
        "tickers": payload.get("tickers") if isinstance(payload.get("tickers"), list) else [ticker],
        "latest_filings": recent_rows,
    }


def _recent_filing_rows(recent: dict[str, Any], *, limit: int) -> list[dict[str, Any]]:
    forms = recent.get("form") if isinstance(recent.get("form"), list) else []
    rows: list[dict[str, Any]] = []
    for index, form in enumerate(forms[:limit]):
        row: dict[str, Any] = {"form": form}
        for key, values in recent.items():
            if isinstance(values, list) and index < len(values):
                row[key] = values[index]
        rows.append(row)
    return rows


def _latest_filing_value(normalized: dict[str, Any], key: str) -> str | None:
    filings = normalized.get("latest_filings")
    if not isinstance(filings, list) or not filings:
        return None
    first = filings[0]
    if not isinstance(first, dict):
        return None
    value = first.get(key)
    if value in (None, ""):
        return None
    return str(value)


def _stock_feeds() -> list[str]:
    configured = os.environ.get("MARKET_INTEL_STOCK_FEED") or os.environ.get("ALPACA_STOCK_FEED")
    feeds = [configured or "iex", "sip"]
    return list(dict.fromkeys(str(feed).strip().lower() for feed in feeds if str(feed).strip()))


def _fetch_recent_daily_bars(
    *,
    client: Any,
    ticker: str,
    as_of: date,
    warnings: list[str],
) -> list[dict[str, Any]]:
    start = (as_of - timedelta(days=14)).isoformat()
    end = (as_of + timedelta(days=1)).isoformat()
    try:
        return client.get_stock_bars(
            ticker,
            timeframe="1Day",
            start=start,
            end=end,
            adjustment="raw",
            limit=20,
        )
    except Exception as exc:
        warnings.append(f"Alpaca stock bars failed: {exc}")
        return []


def _normalize_market_payload(
    *,
    ticker: str,
    feed: str,
    snapshot: dict[str, Any],
    bars: list[dict[str, Any]],
) -> dict[str, Any]:
    price, price_source, price_observed_at = _resolve_price(snapshot=snapshot, bars=bars)
    latest_bar = _latest_bar(bars)
    return {
        "ticker": ticker,
        "feed": feed,
        "price": price,
        "price_source": price_source,
        "price_observed_at": price_observed_at,
        "latest_daily_bar": latest_bar,
        "snapshot_keys": sorted(snapshot.keys()),
    }


def _resolve_price(
    *,
    snapshot: dict[str, Any],
    bars: list[dict[str, Any]],
) -> tuple[float | None, str | None, str | None]:
    latest_trade = _mapping(snapshot.get("latestTrade") or snapshot.get("latest_trade"))
    trade_price = parse_float(pick(latest_trade, "p", "price"))
    if trade_price and trade_price > 0:
        return trade_price, "latest_trade", _text(pick(latest_trade, "t", "timestamp"))

    latest_quote = _mapping(snapshot.get("latestQuote") or snapshot.get("latest_quote"))
    bid = parse_float(pick(latest_quote, "bp", "bid_price"))
    ask = parse_float(pick(latest_quote, "ap", "ask_price"))
    if bid and ask and bid > 0 and ask > 0:
        return round((bid + ask) / 2.0, 6), "quote_midpoint", _text(pick(latest_quote, "t", "timestamp"))

    latest_bar = _latest_bar(bars)
    close = parse_float(latest_bar.get("close") if latest_bar else None)
    if close and close > 0:
        return close, "latest_daily_close", _text(latest_bar.get("timestamp") if latest_bar else None)
    return None, None, None


def _latest_bar(bars: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not bars:
        return None
    row = dict(bars[-1])
    return {
        "timestamp": _text(pick(row, "t", "timestamp")),
        "open": parse_float(pick(row, "o", "open")),
        "high": parse_float(pick(row, "h", "high")),
        "low": parse_float(pick(row, "l", "low")),
        "close": parse_float(pick(row, "c", "close")),
        "volume": parse_float(pick(row, "v", "volume")),
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _date_text_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        pass
    try:
        return datetime.combine(date.fromisoformat(text[:10]), time.min, tzinfo=UTC)
    except ValueError:
        return None


def _hash_payload(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _relative_to_run(path: Path, run_dir: Path) -> Path:
    try:
        return path.relative_to(run_dir)
    except ValueError:
        return path

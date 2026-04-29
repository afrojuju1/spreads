from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from core.common import parse_float
from core.services.alpaca import create_alpaca_client_from_env
from core.services.company_valuation.ids import build_market_snapshot_id
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_datetime


def _nested(mapping: dict[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _resolve_price(snapshot: dict[str, Any]) -> tuple[float | None, datetime | None]:
    bid = parse_float(_nested(snapshot, "latestQuote", "bp"))
    ask = parse_float(_nested(snapshot, "latestQuote", "ap"))
    quote_timestamp = parse_datetime(_nested(snapshot, "latestQuote", "t"))
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return ((bid + ask) / 2.0, quote_timestamp)

    trade_price = parse_float(_nested(snapshot, "latestTrade", "p"))
    trade_timestamp = parse_datetime(_nested(snapshot, "latestTrade", "t"))
    if trade_price is not None and trade_price > 0:
        return (trade_price, trade_timestamp)

    for path in (("minuteBar", "c"), ("dailyBar", "c"), ("prevDailyBar", "c")):
        value = parse_float(_nested(snapshot, *path))
        timestamp = parse_datetime(_nested(snapshot, path[0], "t"))
        if value is not None and value > 0:
            return (value, timestamp)
    return (None, None)


@dataclass(frozen=True)
class MarketInputsIngestionRequest:
    ticker: str | None = None
    issuer_id: str | None = None
    stock_feed: str = "iex"


@dataclass(frozen=True)
class MarketInputsIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    issuer_id: str | None = None
    ticker: str | None = None
    snapshots_persisted: int = 0
    price: float | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_market_inputs(
    request: MarketInputsIngestionRequest,
    *,
    repository: CompanyValuationRepository | None = None,
) -> MarketInputsIngestionResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    issuer_row = repo.get_issuer(issuer_id=request.issuer_id, ticker=request.ticker)
    if issuer_row is None:
        raise ValueError("issuer_id or ticker must resolve to an ingested issuer")
    issuer_id = str(issuer_row["issuer_id"])
    ticker = str(issuer_row.get("ticker") or request.ticker or "").upper()
    if not ticker:
        raise ValueError(f"Primary ticker is unavailable for issuer {issuer_id}")
    primary_security = repo.get_primary_security(issuer_id=issuer_id)
    if primary_security is None:
        raise ValueError(f"Primary security is unavailable for issuer {issuer_id}")

    client = create_alpaca_client_from_env(request_timeout_seconds=20.0)
    attempted_feeds = list(dict.fromkeys([request.stock_feed, "sip"]))
    snapshot: dict[str, Any] | None = None
    feed_used: str | None = None
    notes: list[str] = []
    for feed in attempted_feeds:
        try:
            snapshots = client.get_stock_snapshots([ticker], feed=feed)
        except Exception as exc:  # pragma: no cover - runtime verification path
            notes.append(f"{feed} feed request failed: {exc}")
            continue
        candidate = snapshots.get(ticker)
        if isinstance(candidate, dict) and candidate:
            snapshot = candidate
            feed_used = feed
            break
    if not snapshot:
        completed_at = datetime.now(UTC)
        return MarketInputsIngestionResult(
            status="no_snapshot",
            source="alpaca_stock_snapshot",
            started_at=started_at,
            completed_at=completed_at,
            issuer_id=issuer_id,
            ticker=ticker,
            notes=tuple(notes or ["No stock snapshot was returned by Alpaca."]),
        )

    price, captured_at = _resolve_price(snapshot)
    if price is None:
        completed_at = datetime.now(UTC)
        return MarketInputsIngestionResult(
            status="no_price",
            source=f"alpaca_stock_snapshot:{feed_used or request.stock_feed}",
            started_at=started_at,
            completed_at=completed_at,
            issuer_id=issuer_id,
            ticker=ticker,
            notes=tuple(notes or ["Alpaca returned a snapshot without a usable price."]),
        )

    latest_statement = repo.get_latest_statement_snapshot_before(
        issuer_id=issuer_id,
        as_of=datetime.now(UTC),
    )
    metrics = dict((latest_statement or {}).get("metrics_json") or {})
    shares_outstanding = parse_float(metrics.get("shares_outstanding")) or parse_float(
        metrics.get("diluted_weighted_average_shares")
    )
    cash_and_equivalents = parse_float(metrics.get("cash_and_equivalents"))
    long_term_debt = parse_float(metrics.get("long_term_debt")) or parse_float(
        metrics.get("total_liabilities")
    )
    market_cap = None if shares_outstanding is None else price * shares_outstanding
    enterprise_value = None
    if market_cap is not None:
        enterprise_value = market_cap + (long_term_debt or 0.0) - (cash_and_equivalents or 0.0)

    captured_at = (captured_at or datetime.now(UTC)).astimezone(UTC)
    payload = {
        "market_snapshot_id": build_market_snapshot_id(str(issuer_row.get("cik") or issuer_id), captured_at),
        "security_id": str(primary_security["security_id"]),
        "issuer_id": issuer_id,
        "captured_at": captured_at,
        "available_at": captured_at,
        "price": price,
        "shares_outstanding_market": shares_outstanding,
        "market_cap": market_cap,
        "enterprise_value": enterprise_value,
        "source": f"alpaca_stock_snapshot:{feed_used or request.stock_feed}",
    }
    repo.upsert_market_snapshot(payload)

    completed_at = datetime.now(UTC)
    return MarketInputsIngestionResult(
        status="ok",
        source=str(payload["source"]),
        started_at=started_at,
        completed_at=completed_at,
        issuer_id=issuer_id,
        ticker=ticker,
        snapshots_persisted=1,
        price=price,
        notes=tuple(notes),
    )

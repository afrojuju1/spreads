from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any

from core.integrations.calendar_events.adapters.alpha_vantage_earnings_calendar import AlphaVantageEarningsCalendarAdapter
from core.integrations.calendar_events.adapters.dolthub_earnings_calendar import DoltHubEarningsCalendarAdapter
from core.integrations.calendar_events.adapters.finviz_earnings import FinvizEarningsAdapter
from core.integrations.calendar_events.adapters.yfinance_earnings_calendar import (
    YFINANCE_EARNINGS_PAGE_LIMIT,
    YFinanceEarningsCalendarAdapter,
)
from core.integrations.calendar_events.models import CalendarEventQuery, CalendarEventRecord, ProviderFetchAuditRecord
from core.integrations.calendar_events.provider_cache import (
    ProviderHotCache,
    provider_params_hash,
    provider_payload_hash,
    sanitize_provider_json,
)
from core.integrations.calendar_events.store import CalendarEventStore
from core.runtime.config import default_alpha_vantage_api_key, default_database_url, default_redis_url
from core.storage.serializers import parse_datetime, render_value
from core.value_coercion import as_mapping, coerce_bool, coerce_int, safe_component, utc_now, utc_now_iso

EARNINGS_30D_REFRESH_ID = "earnings_30d"
DEFAULT_WINDOW_DAYS = 30
DEFAULT_PROVIDER_TTL_SECONDS = 6 * 60 * 60
DEFAULT_BACKOFF_SECONDS = 60 * 60
DEFAULT_FINVIZ_TTL_SECONDS = 6 * 60 * 60
DEFAULT_FINVIZ_MAX_SYMBOLS = 10
DEFAULT_FINVIZ_NEAR_TERM_DAYS = 7
DEFAULT_YFINANCE_MARKET_CAP = 1_000_000_000

_BASE_EARNINGS_SOURCES = {
    "yfinance_earnings_calendar",
    "alpha_vantage_earnings_calendar",
    "dolt_earnings_calendar",
}
_ALL_EARNINGS_SOURCES = {*_BASE_EARNINGS_SOURCES, "finviz_earnings"}


def run_calendar_event_refresh(
    *,
    refresh_id: str,
    database_url: str | None = None,
    redis_url: str | None = None,
    payload: Mapping[str, Any] | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> dict[str, Any]:
    normalized_refresh_id = str(refresh_id or "").strip()
    if normalized_refresh_id != EARNINGS_30D_REFRESH_ID:
        raise ValueError(f"Unsupported calendar event refresh id: {refresh_id}")

    args = as_mapping(payload)
    now = utc_now()
    window_days = max(coerce_int(args.get("window_days")) or DEFAULT_WINDOW_DAYS, 1)
    window_start = now.replace(microsecond=0)
    window_end = window_start + timedelta(days=window_days)
    store = CalendarEventStore(database_url or default_database_url())
    cache = ProviderHotCache(redis_url=redis_url or default_redis_url())
    lock_token: str | None = None
    try:
        lock_token = cache.acquire_refresh_lock(
            scope=f"calendar_event_refresh:{normalized_refresh_id}",
            ttl_seconds=max(coerce_int(args.get("refresh_lock_ttl_seconds")) or 30 * 60, 60),
        )
        if lock_token is None:
            return {
                "status": "skipped",
                "reason": "refresh_lock_active",
                "refresh_id": normalized_refresh_id,
                "window_start": render_value(window_start),
                "window_end": render_value(window_end),
            }

        provider_results: dict[str, Any] = {}
        records: list[CalendarEventRecord] = []

        yfinance_records, provider_results["yfinance"] = _refresh_yfinance(
            store=store,
            cache=cache,
            window_start=window_start,
            window_end=window_end,
            args=args,
            heartbeat=heartbeat,
        )
        records.extend(yfinance_records)

        alpha_records, provider_results["alpha_vantage"] = _refresh_alpha_vantage(
            store=store,
            cache=cache,
            window_start=window_start,
            window_end=window_end,
            args=args,
            heartbeat=heartbeat,
        )
        records.extend(alpha_records)

        dolthub_records, provider_results["dolthub"] = _refresh_dolthub(
            store=store,
            cache=cache,
            window_start=window_start,
            window_end=window_end,
            args=args,
            heartbeat=heartbeat,
        )
        records.extend(dolthub_records)

        if records:
            store.upsert_events(records)

        initial_consensus = store.rebuild_earnings_event_consensus(
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            sources=set(_BASE_EARNINGS_SOURCES),
            computed_at=utc_now_iso(),
        )
        finviz_symbols = _finviz_symbols_for_enrichment(
            initial_consensus,
            max_symbols=max(coerce_int(args.get("finviz_max_symbols")) or DEFAULT_FINVIZ_MAX_SYMBOLS, 0),
            near_term_days=max(coerce_int(args.get("finviz_near_term_days")) or DEFAULT_FINVIZ_NEAR_TERM_DAYS, 0),
            anchor=window_start,
        )
        finviz_records, provider_results["finviz"] = _refresh_finviz(
            store=store,
            cache=cache,
            symbols=finviz_symbols,
            window_start=window_start,
            window_end=window_end,
            args=args,
            heartbeat=heartbeat,
        )
        if finviz_records:
            store.upsert_events(finviz_records)

        final_consensus = store.rebuild_earnings_event_consensus(
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            sources=set(_ALL_EARNINGS_SOURCES),
            computed_at=utc_now_iso(),
        )
        provider_statuses = {provider: str(result.get("status") or "unknown") for provider, result in provider_results.items()}
        degraded = [provider for provider, status in provider_statuses.items() if status not in {"ok", "cache_hit", "skipped"}]
        status = "completed" if not degraded else "completed_with_warnings"
        return render_value(
            {
                "status": status,
                "refresh_id": normalized_refresh_id,
                "window_start": window_start,
                "window_end": window_end,
                "provider_statuses": provider_statuses,
                "providers": provider_results,
                "records_upserted": len(records) + len(finviz_records),
                "records_by_source": _record_counts_by_source([*records, *finviz_records]),
                "initial_consensus_count": len(initial_consensus),
                "final_consensus_count": len(final_consensus),
                "final_conflict_count": sum(1 for item in final_consensus if item.consensus_status == "conflict"),
                "finviz_enrichment_symbols": finviz_symbols,
            }
        )
    finally:
        if lock_token is not None:
            try:
                cache.release_refresh_lock(scope=f"calendar_event_refresh:{normalized_refresh_id}", token=lock_token)
            except Exception:
                pass
        cache.close()
        store.close()


def _refresh_yfinance(
    *,
    store: CalendarEventStore,
    cache: ProviderHotCache,
    window_start: datetime,
    window_end: datetime,
    args: Mapping[str, Any],
    heartbeat: Callable[[], None] | None,
) -> tuple[list[CalendarEventRecord], dict[str, Any]]:
    adapter = YFinanceEarningsCalendarAdapter()
    records: list[CalendarEventRecord] = []
    pages: list[dict[str, Any]] = []
    page_limit = max(1, min(coerce_int(args.get("yfinance_page_limit")) or YFINANCE_EARNINGS_PAGE_LIMIT, YFINANCE_EARNINGS_PAGE_LIMIT))
    ttl_seconds = max(coerce_int(args.get("yfinance_ttl_seconds")) or DEFAULT_PROVIDER_TTL_SECONDS, 1)
    backoff_seconds = max(coerce_int(args.get("provider_backoff_seconds")) or DEFAULT_BACKOFF_SECONDS, 1)
    offset = 0
    while True:
        _heartbeat(heartbeat)
        page_key = f"offset:{offset}"
        params = {
            "window_start": window_start.date().isoformat(),
            "window_end": window_end.date().isoformat(),
            "market_cap": coerce_int(args.get("yfinance_market_cap")) or DEFAULT_YFINANCE_MARKET_CAP,
            "filter_most_active": coerce_bool(args.get("yfinance_filter_most_active"), default=True),
            "limit": page_limit,
            "offset": offset,
        }

        payload, fetch_summary = _payload_through_cache(
            store=store,
            cache=cache,
            provider="yfinance",
            endpoint="earnings_calendar",
            params=params,
            coverage_start=window_start,
            coverage_end=window_end,
            page_key=page_key,
            ttl_seconds=ttl_seconds,
            backoff_seconds=backoff_seconds,
            fetch_payload=lambda: {
                "rows": [
                    dict(row)
                    for row in adapter.fetch_page(
                        window_start=window_start,
                        window_end=window_end,
                        offset=offset,
                        market_cap=params["market_cap"],
                        filter_most_active=bool(params["filter_most_active"]),
                        page_limit=page_limit,
                    ).rows
                ],
                "offset": offset,
                "limit": page_limit,
            },
        )
        pages.append(fetch_summary)
        if payload is None:
            break
        rows = tuple(dict(row) for row in list(payload.get("rows") or []) if isinstance(row, Mapping))
        records.extend(adapter.records_from_rows(rows))
        if len(rows) < page_limit:
            break
        offset += page_limit

    return records, _provider_result_from_fetches(provider="yfinance", fetches=pages, record_count=len(records))


def _refresh_alpha_vantage(
    *,
    store: CalendarEventStore,
    cache: ProviderHotCache,
    window_start: datetime,
    window_end: datetime,
    args: Mapping[str, Any],
    heartbeat: Callable[[], None] | None,
) -> tuple[list[CalendarEventRecord], dict[str, Any]]:
    api_key = default_alpha_vantage_api_key()
    if not api_key:
        return [], {"status": "skipped", "reason": "alpha_vantage_api_key_missing", "record_count": 0}
    adapter = AlphaVantageEarningsCalendarAdapter(api_key=api_key)
    _heartbeat(heartbeat)
    payload, fetch_summary = _payload_through_cache(
        store=store,
        cache=cache,
        provider="alpha_vantage",
        endpoint="earnings_calendar",
        params={
            "function": "EARNINGS_CALENDAR",
            "horizon": "3month",
            "apikey": api_key,
            "window_start": window_start.date().isoformat(),
            "window_end": window_end.date().isoformat(),
        },
        coverage_start=window_start,
        coverage_end=window_end,
        page_key=None,
        ttl_seconds=max(coerce_int(args.get("alpha_vantage_ttl_seconds")) or DEFAULT_PROVIDER_TTL_SECONDS, 1),
        backoff_seconds=max(coerce_int(args.get("provider_backoff_seconds")) or DEFAULT_BACKOFF_SECONDS, 1),
        fetch_payload=lambda: {
            "records": _records_payload(
                adapter.fetch_bulk(window_start=window_start.isoformat(), window_end=window_end.isoformat())
            )
        },
    )
    records = _records_from_payload(payload)
    return records, _provider_result_from_fetches(provider="alpha_vantage", fetches=[fetch_summary], record_count=len(records))


def _refresh_dolthub(
    *,
    store: CalendarEventStore,
    cache: ProviderHotCache,
    window_start: datetime,
    window_end: datetime,
    args: Mapping[str, Any],
    heartbeat: Callable[[], None] | None,
) -> tuple[list[CalendarEventRecord], dict[str, Any]]:
    adapter = DoltHubEarningsCalendarAdapter()
    _heartbeat(heartbeat)
    payload, fetch_summary = _payload_through_cache(
        store=store,
        cache=cache,
        provider="dolthub",
        endpoint="earnings_calendar",
        params={
            "window_start": window_start.date().isoformat(),
            "window_end": window_end.date().isoformat(),
        },
        coverage_start=window_start,
        coverage_end=window_end,
        page_key=None,
        ttl_seconds=max(coerce_int(args.get("dolthub_ttl_seconds")) or DEFAULT_PROVIDER_TTL_SECONDS, 1),
        backoff_seconds=max(coerce_int(args.get("provider_backoff_seconds")) or DEFAULT_BACKOFF_SECONDS, 1),
        fetch_payload=lambda: {
            "records": _records_payload(
                adapter.fetch_bulk(window_start=window_start.isoformat(), window_end=window_end.isoformat())
            )
        },
    )
    records = _records_from_payload(payload)
    return records, _provider_result_from_fetches(provider="dolthub", fetches=[fetch_summary], record_count=len(records))


def _refresh_finviz(
    *,
    store: CalendarEventStore,
    cache: ProviderHotCache,
    symbols: list[str],
    window_start: datetime,
    window_end: datetime,
    args: Mapping[str, Any],
    heartbeat: Callable[[], None] | None,
) -> tuple[list[CalendarEventRecord], dict[str, Any]]:
    if not symbols:
        return [], {"status": "skipped", "reason": "no_sparse_symbols", "record_count": 0}
    adapter = FinvizEarningsAdapter()
    records: list[CalendarEventRecord] = []
    fetches: list[dict[str, Any]] = []
    for symbol in symbols:
        _heartbeat(heartbeat)
        payload, fetch_summary = _payload_through_cache(
            store=store,
            cache=cache,
            provider="finviz",
            endpoint="earnings",
            params={
                "symbol": symbol,
                "window_start": window_start.date().isoformat(),
                "window_end": window_end.date().isoformat(),
            },
            coverage_start=window_start,
            coverage_end=window_end,
            page_key=symbol,
            ttl_seconds=max(coerce_int(args.get("finviz_ttl_seconds")) or DEFAULT_FINVIZ_TTL_SECONDS, 1),
            backoff_seconds=max(coerce_int(args.get("provider_backoff_seconds")) or DEFAULT_BACKOFF_SECONDS, 1),
            fetch_payload=lambda symbol=symbol: {
                "records": _records_payload(
                    adapter.fetch(
                        CalendarEventQuery(
                            symbol=symbol,
                            strategy="calendar_event_refresh",
                            window_start=window_start.isoformat(),
                            window_end=window_end.isoformat(),
                            underlying_type="single_name_equity",
                        )
                    )
                )
            },
        )
        fetches.append(fetch_summary)
        records.extend(_records_from_payload(payload))
    return records, _provider_result_from_fetches(provider="finviz", fetches=fetches, record_count=len(records))


def _payload_through_cache(
    *,
    store: CalendarEventStore,
    cache: ProviderHotCache,
    provider: str,
    endpoint: str,
    params: Mapping[str, Any],
    coverage_start: datetime,
    coverage_end: datetime,
    page_key: str | None,
    ttl_seconds: int,
    backoff_seconds: int,
    fetch_payload: Callable[[], dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    params_hash = provider_params_hash(params)
    now = utc_now()
    cached_backoff = _safe_cache_backoff(cache, provider=provider, endpoint=endpoint)
    if cached_backoff:
        _write_audit(
            store=store,
            provider=provider,
            endpoint=endpoint,
            params_hash=params_hash,
            params=params,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            page_key=page_key,
            status="backoff",
            cache_hit=False,
            row_count=None,
            payload_hash=None,
            expires_at=None,
            backoff_until=parse_datetime(str(cached_backoff.get("backoff_until"))) if cached_backoff.get("backoff_until") else None,
            error_code="redis_backoff_active",
            error_message=str(cached_backoff.get("reason") or "provider_backoff_active"),
        )
        return None, {"status": "backoff", "page_key": page_key, "reason": cached_backoff.get("reason")}

    cached_payload = _safe_cache_payload(cache, provider=provider, endpoint=endpoint, params_hash=params_hash, page_key=page_key)
    if cached_payload is not None:
        row_count = _payload_row_count(cached_payload)
        _write_audit(
            store=store,
            provider=provider,
            endpoint=endpoint,
            params_hash=params_hash,
            params=params,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            page_key=page_key,
            status="cache_hit",
            cache_hit=True,
            row_count=row_count,
            payload_hash=provider_payload_hash(cached_payload),
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        return as_mapping(cached_payload), {"status": "cache_hit", "page_key": page_key, "row_count": row_count}

    audit_skip = _audit_throttle(store, provider=provider, endpoint=endpoint, params_hash=params_hash, page_key=page_key, now=now)
    if audit_skip is not None:
        _write_audit(
            store=store,
            provider=provider,
            endpoint=endpoint,
            params_hash=params_hash,
            params=params,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            page_key=page_key,
            status="audit_throttled",
            cache_hit=False,
            row_count=None,
            payload_hash=None,
            expires_at=parse_datetime(str(audit_skip.get("expires_at"))) if audit_skip.get("expires_at") else None,
            backoff_until=parse_datetime(str(audit_skip.get("backoff_until"))) if audit_skip.get("backoff_until") else None,
            error_code=str(audit_skip.get("reason") or "audit_throttled"),
            error_message="Postgres audit throttle active; Redis payload was unavailable.",
        )
        return None, {"status": "audit_throttled", "page_key": page_key, "reason": audit_skip.get("reason")}

    try:
        payload = sanitize_provider_json(fetch_payload())
        row_count = _payload_row_count(payload)
        _safe_cache_set(
            cache,
            provider=provider,
            endpoint=endpoint,
            params_hash=params_hash,
            page_key=page_key,
            payload=payload,
            ttl_seconds=ttl_seconds,
        )
        _write_audit(
            store=store,
            provider=provider,
            endpoint=endpoint,
            params_hash=params_hash,
            params=params,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            page_key=page_key,
            status="ok",
            cache_hit=False,
            row_count=row_count,
            payload_hash=provider_payload_hash(payload),
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        return as_mapping(payload), {"status": "ok", "page_key": page_key, "row_count": row_count}
    except Exception as exc:
        backoff_until = now + timedelta(seconds=backoff_seconds)
        _safe_cache_set_backoff(cache, provider=provider, endpoint=endpoint, ttl_seconds=backoff_seconds, reason=type(exc).__name__)
        _write_audit(
            store=store,
            provider=provider,
            endpoint=endpoint,
            params_hash=params_hash,
            params=params,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            page_key=page_key,
            status="failed",
            cache_hit=False,
            row_count=None,
            payload_hash=None,
            expires_at=None,
            backoff_until=backoff_until,
            error_code=type(exc).__name__,
            error_message=str(exc)[:500],
        )
        return None, {"status": "failed", "page_key": page_key, "error_code": type(exc).__name__, "error": str(exc)[:500]}


def _audit_throttle(
    store: CalendarEventStore,
    *,
    provider: str,
    endpoint: str,
    params_hash: str,
    page_key: str | None,
    now: datetime,
) -> dict[str, Any] | None:
    latest = store.latest_provider_fetch_audit(provider=provider, endpoint=endpoint, params_hash=params_hash, page_key=page_key)
    if latest is None:
        return None
    backoff_until = parse_datetime(str(latest.get("backoff_until"))) if latest.get("backoff_until") else None
    if backoff_until is not None and backoff_until > now:
        return {**latest, "reason": "audit_backoff_active"}
    expires_at = parse_datetime(str(latest.get("expires_at"))) if latest.get("expires_at") else None
    if str(latest.get("status") or "") in {"ok", "cache_hit"} and expires_at is not None and expires_at > now:
        return {**latest, "reason": "audit_cache_ttl_active"}
    return None


def _write_audit(
    *,
    store: CalendarEventStore,
    provider: str,
    endpoint: str,
    params_hash: str,
    params: Mapping[str, Any],
    coverage_start: datetime,
    coverage_end: datetime,
    page_key: str | None,
    status: str,
    cache_hit: bool,
    row_count: int | None,
    payload_hash: str | None,
    expires_at: datetime | None = None,
    backoff_until: datetime | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    fetched_at = utc_now()
    store.upsert_provider_fetch_audit(
        [
            ProviderFetchAuditRecord(
                audit_id=(
                    "provider_fetch_audit:"
                    f"{safe_component(provider)}:{safe_component(endpoint)}:{params_hash[:12]}:"
                    f"{safe_component(page_key or 'all')}:{fetched_at.strftime('%Y%m%dT%H%M%S%fZ')}"
                ),
                provider=provider,
                endpoint=endpoint,
                params_hash=params_hash,
                params_json=dict(params),
                coverage_start=coverage_start.isoformat(),
                coverage_end=coverage_end.isoformat(),
                page_key=page_key,
                status=status,
                cache_hit=cache_hit,
                payload_hash=payload_hash,
                row_count=row_count,
                fetched_at=fetched_at.isoformat(),
                expires_at=None if expires_at is None else expires_at.isoformat(),
                backoff_until=None if backoff_until is None else backoff_until.isoformat(),
                error_code=error_code,
                error_message=error_message,
                created_at=fetched_at.isoformat(),
            )
        ]
    )


def _records_payload(records: list[CalendarEventRecord]) -> list[dict[str, Any]]:
    return [asdict(record) for record in records]


def _records_from_payload(payload: Mapping[str, Any] | None) -> list[CalendarEventRecord]:
    if payload is None:
        return []
    records = payload.get("records")
    if not isinstance(records, list):
        return []
    return [CalendarEventRecord(**dict(record)) for record in records if isinstance(record, Mapping)]


def _provider_result_from_fetches(*, provider: str, fetches: list[dict[str, Any]], record_count: int) -> dict[str, Any]:
    statuses = [str(item.get("status") or "unknown") for item in fetches]
    if not statuses:
        status = "skipped"
    elif any(item == "failed" for item in statuses):
        status = "failed" if all(item == "failed" for item in statuses) else "partial"
    elif any(item in {"backoff", "audit_throttled"} for item in statuses):
        status = "throttled"
    elif all(item == "cache_hit" for item in statuses):
        status = "cache_hit"
    else:
        status = "ok"
    return {
        "provider": provider,
        "status": status,
        "record_count": record_count,
        "fetch_count": len(fetches),
        "cache_hit_count": sum(1 for item in fetches if item.get("status") == "cache_hit"),
        "throttled_count": sum(1 for item in fetches if item.get("status") in {"backoff", "audit_throttled"}),
        "failed_count": sum(1 for item in fetches if item.get("status") == "failed"),
        "fetches": fetches[:25],
    }


def _payload_row_count(payload: Any) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    rows = payload.get("rows")
    if isinstance(rows, list):
        return len(rows)
    records = payload.get("records")
    if isinstance(records, list):
        return len(records)
    return None


def _finviz_symbols_for_enrichment(
    consensus: list[Any],
    *,
    max_symbols: int,
    near_term_days: int,
    anchor: datetime,
) -> list[str]:
    if max_symbols <= 0:
        return []
    symbols: list[str] = []
    for record in sorted(consensus, key=lambda item: (item.event_date, item.symbol)):
        event_date = datetime.fromisoformat(str(record.event_date)).date()
        days_to_event = (event_date - anchor.date()).days
        needs_conflict_check = record.consensus_status == "conflict"
        needs_timing_check = record.session_timing == "unknown" and 0 <= days_to_event <= near_term_days
        if needs_conflict_check or needs_timing_check:
            symbols.append(record.symbol)
        if len(symbols) >= max_symbols:
            break
    return symbols


def _record_counts_by_source(records: list[CalendarEventRecord]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        counts[record.source] = counts.get(record.source, 0) + 1
    return dict(sorted(counts.items()))


def _safe_cache_payload(
    cache: ProviderHotCache,
    *,
    provider: str,
    endpoint: str,
    params_hash: str,
    page_key: str | None,
) -> dict[str, Any] | None:
    try:
        payload = cache.get_payload(provider=provider, endpoint=endpoint, params_hash=params_hash, page_key=page_key)
    except Exception:
        return None
    return as_mapping(payload) if isinstance(payload, Mapping) else None


def _safe_cache_backoff(cache: ProviderHotCache, *, provider: str, endpoint: str) -> dict[str, Any] | None:
    try:
        return cache.get_backoff(provider=provider, endpoint=endpoint)
    except Exception:
        return None


def _safe_cache_set(
    cache: ProviderHotCache,
    *,
    provider: str,
    endpoint: str,
    params_hash: str,
    page_key: str | None,
    payload: Any,
    ttl_seconds: int,
) -> None:
    try:
        cache.set_payload(
            provider=provider,
            endpoint=endpoint,
            params_hash=params_hash,
            page_key=page_key,
            payload=payload,
            ttl_seconds=ttl_seconds,
        )
    except Exception:
        pass


def _safe_cache_set_backoff(
    cache: ProviderHotCache,
    *,
    provider: str,
    endpoint: str,
    ttl_seconds: int,
    reason: str,
) -> None:
    try:
        cache.set_backoff(provider=provider, endpoint=endpoint, ttl_seconds=ttl_seconds, reason=reason)
    except Exception:
        pass


def _heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is not None:
        heartbeat()

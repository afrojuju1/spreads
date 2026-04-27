from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from html import unescape
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

from .base import BaseCalendarEventAdapter
from ..config import EARNINGS_POST_EVENT_SETTLED_DAYS, EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS
from ..models import CalendarEventQuery, CalendarEventRecord

NEW_YORK = ZoneInfo("America/New_York")
_ROUTE_INIT_DATA_PATTERN = re.compile(
    r'<script id="route-init-data" type="application/json">(.*?)</script>',
    re.S,
)
_PROFILE_LINK_PATTERN = re.compile(
    r'<a href="screener\.ashx\?v=111&f=(sec_[^"]+|ind_[^"]+|geo_[^"]+|cap_[^"]+|exch_[^"]+)"'
    r'[^>]*?(?:title="([^"]+)")?[^>]*>([^<]+)</a>'
)
_MARKET_CAP_PATTERN = re.compile(
    r'<div class="snapshot-td-label">Market Cap</div></td>'
    r'<td[^>]*><div class="snapshot-td-content"><b>([^<]+)</b></div>',
    re.S,
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_finviz_datetime(value: object) -> datetime | None:
    rendered = str(value or "").strip()
    if not rendered:
        return None
    parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=NEW_YORK)
    return parsed.astimezone(UTC)


def _render_session_timing(local_dt: datetime) -> str:
    local_time = local_dt.astimezone(NEW_YORK).timetz()
    if (local_time.hour, local_time.minute) <= (9, 30):
        return "before_open"
    if (local_time.hour, local_time.minute) >= (16, 0):
        return "after_close"
    return "during_market"


def _status_for_row(row: dict[str, object]) -> str:
    if row.get("epsActual") is not None or row.get("salesActual") is not None:
        return "reported"
    return "scheduled"


def _coerce_text(value: object) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def _compact_dict(payload: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in payload.items()
        if value not in (None, "", (), [], {})
    }


def _price_reactions_by_report_date(
    payload: dict[str, object],
) -> dict[str, dict[str, object]]:
    rows = payload.get("priceReactionData")
    if not isinstance(rows, list):
        return {}
    reactions: dict[str, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        report_date = _coerce_text(row.get("reportDate"))
        if report_date is None:
            continue
        reactions[report_date] = row
    return reactions


def _company_profile(html: str) -> dict[str, object]:
    profile: dict[str, object] = {}
    for raw_key, title_text, inner_text in _PROFILE_LINK_PATTERN.findall(html):
        value = unescape(title_text or inner_text).strip()
        if not value:
            continue
        if raw_key.startswith("sec_"):
            profile["sector"] = value
        elif raw_key.startswith("ind_"):
            profile["industry"] = value
        elif raw_key.startswith("geo_"):
            profile["country"] = value
        elif raw_key.startswith("cap_"):
            profile["sizeBucket"] = value
        elif raw_key.startswith("exch_"):
            profile["exchange"] = value
    market_cap_match = _MARKET_CAP_PATTERN.search(html)
    if market_cap_match:
        profile["marketCap"] = unescape(market_cap_match.group(1)).strip()
    return profile


def _browser_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://finviz.com/",
    }


def _route_init_payload(html: str) -> dict[str, object]:
    match = _ROUTE_INIT_DATA_PATTERN.search(html)
    if match is None:
        raise ValueError("Finviz quote page did not include route-init-data")
    payload = json.loads(match.group(1))
    if not isinstance(payload, dict):
        raise ValueError("Finviz route-init-data payload was not an object")
    return payload


def _build_record_payload(
    row: dict[str, object],
    *,
    profile: dict[str, object],
    reaction_row: dict[str, object] | None,
    source_page_earnings_date: str | None,
    session_timing: str,
) -> dict[str, object]:
    reactions = reaction_row.get("reactions") if isinstance(reaction_row, dict) else None
    one_day_reaction = (
        reactions.get("plus_1_day")
        if isinstance(reactions, dict)
        else None
    )
    return _compact_dict(
        {
            "ticker": _coerce_text(row.get("ticker")),
            "fiscalPeriod": _coerce_text(row.get("fiscalPeriod")),
            "earningsDate": _coerce_text(row.get("earningsDate")),
            "fiscalEndDate": _coerce_text(row.get("fiscalEndDate")),
            "epsActual": row.get("epsActual"),
            "epsEstimate": row.get("epsEstimate"),
            "epsReportedActual": row.get("epsReportedActual"),
            "epsReportedEstimate": row.get("epsReportedEstimate"),
            "salesActual": row.get("salesActual"),
            "salesEstimate": row.get("salesEstimate"),
            "epsAnalysts": row.get("epsAnalysts"),
            "epsReportedAnalysts": row.get("epsReportedAnalysts"),
            "salesAnalysts": row.get("salesAnalysts"),
            "sessionTiming": session_timing,
            "sourcePageEarningsDate": source_page_earnings_date,
            "companyProfile": profile,
            "priceReaction": _compact_dict(dict(reaction_row or {})),
            "oneDayPriceReaction": (
                None
                if not isinstance(one_day_reaction, dict)
                else one_day_reaction.get("priceDiff")
            ),
            "oneDayPriceReactionVsSpy": (
                None
                if not isinstance(one_day_reaction, dict)
                else one_day_reaction.get("spyPriceDiff")
            ),
        }
    )


class FinvizEarningsAdapter(BaseCalendarEventAdapter):
    source_name = "finviz_earnings"
    source_confidence = "low"
    contributes_to_coverage = False
    base_url = "https://finviz.com/quote.ashx"

    def applies_to(self, query: CalendarEventQuery) -> bool:
        return query.underlying_type == "single_name_equity"

    def scope_key(self, query: CalendarEventQuery) -> str:
        return query.symbol.upper()

    def coverage_query(self, query: CalendarEventQuery) -> CalendarEventQuery:
        start_dt = _parse_datetime(query.window_start)
        end_dt = _parse_datetime(query.window_end)
        return replace(
            query,
            window_start=(start_dt - timedelta(days=EARNINGS_POST_EVENT_SETTLED_DAYS)).isoformat(),
            window_end=max(
                end_dt,
                start_dt + timedelta(days=EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS),
            ).isoformat(),
        )

    def fetch(self, query: CalendarEventQuery) -> list[CalendarEventRecord]:
        params = {"t": query.symbol.upper(), "ty": "ea"}
        url = self.base_url + "?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, headers=_browser_headers())
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                html = response.read().decode("utf-8", "replace")
        except HTTPError as exc:
            if exc.code in {403, 404, 429}:
                return []
            raise

        payload = _route_init_payload(html)
        profile = _company_profile(html)
        price_reactions = _price_reactions_by_report_date(payload)
        fetched_at = _utc_now_iso()
        query_start = _parse_datetime(query.window_start)
        query_end = _parse_datetime(query.window_end)
        source_page_earnings_date = _coerce_text(payload.get("earningsDate"))
        rows = payload.get("earningsData")
        if not isinstance(rows, list):
            return []

        records: list[CalendarEventRecord] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_earnings_date = _coerce_text(row.get("earningsDate"))
            scheduled_at_dt = _parse_finviz_datetime(row_earnings_date)
            if row_earnings_date is None or scheduled_at_dt is None:
                continue
            if scheduled_at_dt < query_start or scheduled_at_dt > query_end:
                continue
            session_timing = _render_session_timing(scheduled_at_dt)
            payload_row = _build_record_payload(
                row,
                profile=profile,
                reaction_row=price_reactions.get(row_earnings_date),
                source_page_earnings_date=source_page_earnings_date,
                session_timing=session_timing,
            )
            records.append(
                CalendarEventRecord(
                    event_id=(
                        f"{self.source_name}:{query.symbol.upper()}:"
                        f"{row_earnings_date}:{_coerce_text(row.get('fiscalPeriod')) or 'unknown'}"
                    ),
                    event_type="earnings",
                    symbol=query.symbol.upper(),
                    asset_scope=None,
                    scheduled_at=scheduled_at_dt.isoformat(),
                    window_start=scheduled_at_dt.isoformat(),
                    window_end=scheduled_at_dt.isoformat(),
                    source=self.source_name,
                    source_confidence=self.source_confidence,
                    status=_status_for_row(row),
                    payload_json=json.dumps(payload_row, separators=(",", ":")),
                    ingested_at=fetched_at,
                    source_updated_at=fetched_at,
                )
            )
        return records

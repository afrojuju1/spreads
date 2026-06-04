from __future__ import annotations

import csv
from collections.abc import Mapping
from datetime import UTC, datetime
import html
import math
import os
from pathlib import Path
import re
from typing import Any
import urllib.request

from core.common import parse_float, parse_int, pick
from core.integrations.alpaca.client import AlpacaRequestError
from core.services.alpaca import create_alpaca_client_from_env
from core.services.trading_strategies import build_entry_strategy_symbols, load_universe_symbols
from core.storage.serializers import parse_datetime

VALID_TICKER_SOURCE_RECIPES = frozenset({"strategy_union", "finviz_screener", "stock_prefilter"})


def _iso_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_optional_text(value: Any) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def _recipe_args(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _as_int(value: Any, default: int) -> int:
    parsed = parse_int(value)
    return default if parsed is None else int(parsed)


def _as_float(value: Any, default: float) -> float:
    parsed = parse_float(value)
    return default if parsed is None else float(parsed)


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    rendered = str(value).strip().lower()
    if rendered in {"1", "true", "yes", "y", "on"}:
        return True
    if rendered in {"0", "false", "no", "n", "off"}:
        return False
    return default


_ENV_TOKEN_REGEX = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env_tokens(value: Any) -> str | None:
    text = _as_optional_text(value)
    if text is None:
        return None
    expanded = _ENV_TOKEN_REGEX.sub(
        lambda match: os.environ.get(match.group(1), ""),
        text,
    )
    return _as_optional_text(expanded)


def _recipe_text_arg(
    recipe_args: Mapping[str, Any],
    field_name: str,
    *,
    env_field_name: str | None = None,
) -> str | None:
    direct = _expand_env_tokens(recipe_args.get(field_name))
    if direct is not None:
        return direct
    if env_field_name is None:
        return None
    env_name = _as_optional_text(recipe_args.get(env_field_name))
    if env_name is None:
        return None
    return _as_optional_text(os.environ.get(env_name))


def _normalize_symbol(value: Any) -> str | None:
    rendered = str(value or "").strip().upper()
    return rendered or None


_LEVERAGE_REGEX = re.compile(r"\b(?:[2-9](?:\.\d+)?x|ultra|ultrapro|leveraged|leverage)\b")
_INVERSE_REGEX = re.compile(r"\b(?:inverse|short|bear|ultrashort)\b")
_ETF_NAME_REGEX = re.compile(r"\b(?:etf|trust|fund|shares|direxion|proshares|graniteshares|yieldmax)\b")


def _looks_like_leveraged_or_inverse_etf(asset: Mapping[str, Any] | None) -> bool:
    if not isinstance(asset, Mapping):
        return False
    name = str(asset.get("name") or "").strip().lower()
    if not name:
        return False
    if not _ETF_NAME_REGEX.search(name):
        return False
    return bool(_LEVERAGE_REGEX.search(name) or _INVERSE_REGEX.search(name))


def _stock_snapshot_price(snapshot: Mapping[str, Any]) -> float | None:
    latest_trade = snapshot.get("latestTrade") if isinstance(snapshot.get("latestTrade"), Mapping) else {}
    latest_quote = snapshot.get("latestQuote") if isinstance(snapshot.get("latestQuote"), Mapping) else {}
    minute_bar = snapshot.get("minuteBar") if isinstance(snapshot.get("minuteBar"), Mapping) else {}
    daily_bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), Mapping) else {}

    trade_price = parse_float(pick(latest_trade, "p", "price"))
    if trade_price is not None and trade_price > 0:
        return trade_price

    bid = parse_float(pick(latest_quote, "bp", "bid_price"))
    ask = parse_float(pick(latest_quote, "ap", "ask_price"))
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2.0

    minute_close = parse_float(pick(minute_bar, "c", "close"))
    if minute_close is not None and minute_close > 0:
        return minute_close

    daily_close = parse_float(pick(daily_bar, "c", "close"))
    if daily_close is not None and daily_close > 0:
        return daily_close
    return None


def _stock_snapshot_daily_volume(snapshot: Mapping[str, Any]) -> int:
    daily_bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), Mapping) else {}
    return parse_int(pick(daily_bar, "v", "volume")) or 0


def _stock_snapshot_daily_percent_change(snapshot: Mapping[str, Any]) -> float | None:
    daily_bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), Mapping) else {}
    prev_daily_bar = snapshot.get("prevDailyBar") if isinstance(snapshot.get("prevDailyBar"), Mapping) else {}
    current_close = parse_float(pick(daily_bar, "c", "close"))
    previous_close = parse_float(pick(prev_daily_bar, "c", "close"))
    if current_close is None or previous_close is None or previous_close <= 0:
        return None
    return ((current_close - previous_close) / previous_close) * 100.0


def _rank_score(rank: int | None, *, total: int, weight: float) -> float:
    if rank is None or total <= 0:
        return 0.0
    return max(weight * float(total - rank) / float(total), 0.0)


def _build_strategy_union_result(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any],
    config_root: str | None,
) -> dict[str, Any]:
    symbols = build_entry_strategy_symbols(
        config_root=config_root,
        scanner_strategy=_as_optional_text(recipe_args.get("scanner_strategy")),
        scanner_profile=_as_optional_text(recipe_args.get("scanner_profile")),
    )
    source_tags = [f"recipe:{str(recipe or '').strip().lower()}"]
    if (scanner_profile := _as_optional_text(recipe_args.get("scanner_profile"))) is not None:
        source_tags.append(f"profile:{scanner_profile}")
    if (scanner_strategy := _as_optional_text(recipe_args.get("scanner_strategy"))) is not None:
        source_tags.append(f"strategy:{scanner_strategy}")
    generated_at = _iso_now()
    return {
        "status": "completed",
        "source_id": str(source_id),
        "recipe": str(recipe),
        "generated_at": generated_at,
        "symbols": list(symbols),
        "entries": [
            {
                "symbol": symbol,
                "score": None,
                "reason_codes": [str(recipe)],
                "source_tags": list(source_tags),
            }
            for symbol in symbols
        ],
        "summary": {
            "symbol_count": len(symbols),
            "recipe": str(recipe),
        },
        "degradation": {
            "status": "ok" if symbols else "empty",
            "reason": None if symbols else "no_symbols",
        },
    }


def _run_stock_prefilter_feed(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any],
) -> dict[str, Any]:
    top = max(_as_int(recipe_args.get("top"), 15), 1)
    most_actives_top = max(_as_int(recipe_args.get("most_actives_top"), max(top * 2, 25)), 1)
    movers_top = max(_as_int(recipe_args.get("movers_top"), max(top * 2, 25)), 1)
    min_price = max(_as_float(recipe_args.get("min_price"), 10.0), 0.0)
    min_daily_volume = max(_as_int(recipe_args.get("min_daily_volume"), 0), 0)
    news_limit = max(_as_int(recipe_args.get("news_limit"), max(top * 3, 25)), 1)
    most_actives_by = _as_optional_text(recipe_args.get("most_actives_by")) or "volume"
    stock_feed = _as_optional_text(recipe_args.get("stock_feed")) or "sip"
    exclude_leveraged_and_inverse_etfs = _as_bool(
        recipe_args.get("exclude_leveraged_and_inverse_etfs"),
        False,
    )

    client = create_alpaca_client_from_env()
    issues: list[str] = []

    most_actives: list[dict[str, Any]] = []
    try:
        most_actives = client.get_stock_most_actives(
            top=most_actives_top,
            by=most_actives_by,
        )
    except AlpacaRequestError:
        issues.append("most_actives_unavailable")

    gainers: list[dict[str, Any]] = []
    losers: list[dict[str, Any]] = []
    try:
        movers = client.get_stock_movers(top=movers_top)
        gainers = list(movers.get("gainers") or [])
        losers = list(movers.get("losers") or [])
    except AlpacaRequestError:
        issues.append("movers_unavailable")

    candidate_symbols = sorted(
        {
            symbol
            for item in [*most_actives, *gainers, *losers]
            if isinstance(item, Mapping)
            for symbol in [_normalize_symbol(item.get("symbol"))]
            if symbol is not None
        }
    )
    if not candidate_symbols:
        raise RuntimeError(f"Stock prefilter source {source_id} produced no screener candidates")

    optionable_symbols: set[str] | None = None
    optionable_assets_by_symbol: dict[str, dict[str, Any]] = {}
    try:
        for item in client.list_optionable_underlyings():
            if not isinstance(item, Mapping):
                continue
            symbol = _normalize_symbol(item.get("symbol"))
            if symbol is None:
                continue
            optionable_assets_by_symbol[symbol] = dict(item)
        optionable_symbols = set(optionable_assets_by_symbol)
    except AlpacaRequestError:
        issues.append("optionable_filter_unavailable")

    snapshots = client.get_stock_snapshots(candidate_symbols, feed=stock_feed)

    news_count_by_symbol: dict[str, int] = {}
    try:
        for item in client.get_news(symbols=candidate_symbols, limit=news_limit):
            item_symbols = item.get("symbols")
            if not isinstance(item_symbols, list):
                continue
            for raw_symbol in item_symbols:
                symbol = _normalize_symbol(raw_symbol)
                if symbol is not None and symbol in candidate_symbols:
                    news_count_by_symbol[symbol] = news_count_by_symbol.get(symbol, 0) + 1
    except AlpacaRequestError:
        issues.append("news_unavailable")

    most_active_rank_by_symbol = {
        symbol: rank for rank, item in enumerate(most_actives) for symbol in [_normalize_symbol(item.get("symbol"))] if symbol is not None
    }
    most_active_item_by_symbol = {symbol: item for item in most_actives for symbol in [_normalize_symbol(item.get("symbol"))] if symbol is not None}
    gainer_rank_by_symbol = {
        symbol: rank for rank, item in enumerate(gainers) for symbol in [_normalize_symbol(item.get("symbol"))] if symbol is not None
    }
    loser_rank_by_symbol = {
        symbol: rank for rank, item in enumerate(losers) for symbol in [_normalize_symbol(item.get("symbol"))] if symbol is not None
    }
    mover_item_by_symbol = {symbol: item for item in [*gainers, *losers] for symbol in [_normalize_symbol(item.get("symbol"))] if symbol is not None}

    candidates: list[dict[str, Any]] = []
    excluded_leveraged_inverse_count = 0
    below_min_daily_volume_count = 0
    for symbol in candidate_symbols:
        if optionable_symbols is not None and symbol not in optionable_symbols:
            continue
        asset = optionable_assets_by_symbol.get(symbol)
        if exclude_leveraged_and_inverse_etfs and _looks_like_leveraged_or_inverse_etf(asset):
            excluded_leveraged_inverse_count += 1
            continue
        snapshot = snapshots.get(symbol)
        if not isinstance(snapshot, Mapping):
            continue
        price = _stock_snapshot_price(snapshot)
        if price is None or price < min_price:
            continue
        daily_volume = _stock_snapshot_daily_volume(snapshot)
        if daily_volume < min_daily_volume:
            below_min_daily_volume_count += 1
            continue
        snapshot_move_percent = _stock_snapshot_daily_percent_change(snapshot)
        mover_percent = parse_float((mover_item_by_symbol.get(symbol) or {}).get("percent_change"))
        move_percent = mover_percent if mover_percent is not None else snapshot_move_percent
        reason_codes: list[str] = []
        source_tags = [f"recipe:{str(recipe or '').strip().lower()}", "source:alpaca"]
        if symbol in most_active_rank_by_symbol:
            reason_codes.append("most_actives")
            source_tags.append("screen:most_actives")
        if symbol in gainer_rank_by_symbol:
            reason_codes.append("mover_gainer")
            source_tags.append("screen:mover_gainer")
        if symbol in loser_rank_by_symbol:
            reason_codes.append("mover_loser")
            source_tags.append("screen:mover_loser")
        news_count = int(news_count_by_symbol.get(symbol) or 0)
        if news_count > 0:
            reason_codes.append("news")
            source_tags.append("screen:news")
        if not reason_codes:
            reason_codes.append("prefilter")
        candidates.append(
            {
                "symbol": symbol,
                "price": round(price, 4),
                "daily_volume": daily_volume,
                "move_percent": None if move_percent is None else round(move_percent, 4),
                "news_count": news_count,
                "most_active_rank": most_active_rank_by_symbol.get(symbol),
                "gainer_rank": gainer_rank_by_symbol.get(symbol),
                "loser_rank": loser_rank_by_symbol.get(symbol),
                "trade_count": parse_int((most_active_item_by_symbol.get(symbol) or {}).get("trade_count")),
                "most_active_volume": parse_int((most_active_item_by_symbol.get(symbol) or {}).get("volume")),
                "reason_codes": reason_codes,
                "source_tags": sorted(set(source_tags)),
            }
        )

    max_abs_move = max(
        [abs(float(item.get("move_percent") or 0.0)) for item in candidates],
        default=0.0,
    )
    max_log_volume = max(
        [math.log1p(max(int(item.get("daily_volume") or 0), 0)) for item in candidates],
        default=0.0,
    )

    for item in candidates:
        activity_score = _rank_score(
            item.get("most_active_rank"),
            total=len(most_actives),
            weight=40.0,
        )
        mover_rank = item.get("gainer_rank")
        if mover_rank is None:
            mover_rank = item.get("loser_rank")
        mover_score = _rank_score(
            mover_rank,
            total=max(len(gainers), len(losers)),
            weight=25.0,
        )
        move_percent = abs(float(item.get("move_percent") or 0.0))
        move_score = 20.0 * move_percent / max_abs_move if max_abs_move > 0.0 else 0.0
        daily_volume = max(int(item.get("daily_volume") or 0), 0)
        volume_score = 10.0 * math.log1p(daily_volume) / max_log_volume if max_log_volume > 0.0 else 0.0
        news_score = min(int(item.get("news_count") or 0), 3) * (5.0 / 3.0)
        item["score"] = round(
            activity_score + mover_score + move_score + volume_score + news_score,
            2,
        )

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -int(item.get("daily_volume") or 0),
            str(item.get("symbol") or ""),
        ),
    )
    selected = ranked[:top]
    symbols = [str(item.get("symbol")) for item in selected if str(item.get("symbol") or "").strip()]
    if not symbols:
        raise RuntimeError(f"Stock prefilter source {source_id} produced no symbols after filters")
    generated_at = _iso_now()
    degradation_status = "ok" if symbols and not issues else "partial" if symbols else "empty"
    degradation_reason = None
    if not symbols:
        degradation_reason = "no_symbols_after_filters"
    elif issues:
        degradation_reason = issues[0]
    return {
        "status": "completed",
        "source_id": str(source_id),
        "recipe": str(recipe),
        "generated_at": generated_at,
        "symbols": symbols,
        "entries": selected,
        "summary": {
            "symbol_count": len(symbols),
            "candidate_count": len(candidate_symbols),
            "retained_count": len(candidates),
            "recipe": str(recipe),
            "top": top,
            "most_actives_count": len(most_actives),
            "gainers_count": len(gainers),
            "losers_count": len(losers),
            "stock_feed": stock_feed,
            "min_price": min_price,
            "min_daily_volume": min_daily_volume,
            "issues": issues,
            "optionable_filter_applied": optionable_symbols is not None,
            "exclude_leveraged_and_inverse_etfs": exclude_leveraged_and_inverse_etfs,
            "excluded_leveraged_inverse_count": excluded_leveraged_inverse_count,
            "below_min_daily_volume_count": below_min_daily_volume_count,
        },
        "degradation": {
            "status": degradation_status,
            "reason": degradation_reason,
        },
    }


_FINVIZ_NUMERIC_SUFFIXES = {
    "K": 1_000.0,
    "M": 1_000_000.0,
    "B": 1_000_000_000.0,
    "T": 1_000_000_000_000.0,
}


def _normalize_finviz_header(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("%", "percent")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _parse_finviz_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "N/A", "n/a"}:
        return None
    text = text.replace(",", "").replace("$", "").replace("%", "").strip()
    if not text:
        return None
    multiplier = 1.0
    suffix = text[-1:].upper()
    if suffix in _FINVIZ_NUMERIC_SUFFIXES:
        multiplier = _FINVIZ_NUMERIC_SUFFIXES[suffix]
        text = text[:-1].strip()
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _parse_finviz_int(value: Any) -> int | None:
    parsed = _parse_finviz_float(value)
    if parsed is None:
        return None
    return int(round(parsed))


def _finviz_text_list_arg(value: Any) -> tuple[str, ...]:
    if value in (None, "", ()):
        return ()
    if isinstance(value, str):
        raw_items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = [value]
    return tuple(str(item).strip() for item in raw_items if str(item or "").strip())


def _finviz_slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return slug or "unknown"


def _finviz_contains_keyword(text: Any, keywords: tuple[str, ...]) -> str | None:
    haystack = str(text or "").strip().lower()
    if not haystack:
        return None
    for keyword in keywords:
        needle = keyword.strip().lower()
        if needle and needle in haystack:
            return keyword
    return None


def _finviz_instrument_exclusion_reason(
    row: Mapping[str, Any],
    *,
    exclude_industries: tuple[str, ...],
    exclude_company_keywords: tuple[str, ...],
) -> str | None:
    industry = pick(row, "industry")
    industry_match = _finviz_contains_keyword(industry, exclude_industries)
    if industry_match is not None:
        return f"industry:{_finviz_slug(industry_match)}"

    company = pick(row, "company", "name")
    company_match = _finviz_contains_keyword(company, exclude_company_keywords)
    if company_match is not None:
        return f"company_keyword:{_finviz_slug(company_match)}"

    return None


def _strip_finviz_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", "", value)
    return html.unescape(text).strip()


def _parse_finviz_html_rows(source_text: str) -> list[dict[str, Any]]:
    table_index = source_text.find("screener_table")
    if table_index < 0:
        return []
    table_text = source_text[table_index:]
    table_end = table_text.find("</table>")
    if table_end >= 0:
        table_text = table_text[:table_end]
    header_chunks = re.findall(r"<th\b[^>]*>(.*?)</th>", table_text, flags=re.S | re.I)
    headers = [_normalize_finviz_header(_strip_finviz_html(item)) for item in header_chunks]
    headers = [item for item in headers if item]
    if not headers:
        headers = [
            "no",
            "ticker",
            "company",
            "sector",
            "industry",
            "country",
            "market_cap",
            "p_e",
            "price",
            "change",
            "volume",
        ]
    rows: list[dict[str, Any]] = []
    row_chunks = re.findall(
        r"<tr\b[^>]*class=\"[^\"]*styled-row[^\"]*\"[^>]*>(.*?)</tr>",
        table_text,
        flags=re.S | re.I,
    )
    for row_chunk in row_chunks:
        cells = [_strip_finviz_html(item) for item in re.findall(r"<td\b[^>]*>(.*?)</td>", row_chunk, flags=re.S | re.I)]
        if not cells:
            continue
        row = {headers[index] if index < len(headers) else f"column_{index}": value for index, value in enumerate(cells)}
        rows.append(row)
    return rows


def _parse_finviz_source_rows(source_text: str) -> tuple[list[dict[str, Any]], str]:
    stripped = source_text.lstrip()
    if stripped.startswith("<!DOCTYPE") or stripped.startswith("<html"):
        return _parse_finviz_html_rows(source_text), "html"
    rows = [
        {_normalize_finviz_header(key): value for key, value in dict(row).items() if key is not None}
        for row in csv.DictReader(source_text.splitlines())
    ]
    return rows, "csv"


def _load_finviz_csv_text(
    *,
    source_kind: str,
    source_value: str,
    cookie_env: str | None,
    timeout_seconds: int,
) -> str:
    if source_kind == "local_csv":
        return Path(source_value).expanduser().read_text(encoding="utf-8-sig")

    headers = {
        "Accept": "text/csv,*/*",
        "User-Agent": "spreads-finviz-feed/1.0",
    }
    cookie_name = cookie_env or "FINVIZ_COOKIE"
    cookie_value = _as_optional_text(os.environ.get(cookie_name))
    if cookie_value is not None:
        headers["Cookie"] = cookie_value
    request = urllib.request.Request(source_value, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8-sig", errors="replace")


def _finviz_source_config(
    recipe_args: Mapping[str, Any],
) -> tuple[str | None, str | None]:
    source = str(recipe_args.get("source") or "auto").strip().lower()
    scanner_url = (
        _recipe_text_arg(recipe_args, "scanner_url", env_field_name="scanner_url_env")
        or _recipe_text_arg(recipe_args, "csv_url", env_field_name="csv_url_env")
        or _recipe_text_arg(recipe_args, "url", env_field_name="url_env")
    )
    csv_path = _recipe_text_arg(recipe_args, "csv_path", env_field_name="csv_path_env") or _recipe_text_arg(
        recipe_args, "path", env_field_name="path_env"
    )

    if source in {"auto", "csv_export", "csv_url", "url"} and scanner_url:
        return "csv_url", scanner_url
    if source in {"auto", "csv_file", "local_csv", "file"} and csv_path:
        return "local_csv", csv_path
    if source not in {"auto", "csv_export", "csv_url", "url", "csv_file", "local_csv", "file"}:
        raise ValueError(f"Unsupported Finviz source: {source}")
    return None, None


def _run_finviz_screener_feed(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any],
) -> dict[str, Any]:
    top = max(_as_int(recipe_args.get("top"), 10), 1)
    min_price = max(_as_float(recipe_args.get("min_price"), 0.0), 0.0)
    min_market_cap = max(_as_float(recipe_args.get("min_market_cap"), 0.0), 0.0)
    min_volume = max(
        _as_int(
            recipe_args.get("min_volume", recipe_args.get("min_daily_volume")),
            0,
        ),
        0,
    )
    exclude_industries = _finviz_text_list_arg(recipe_args.get("exclude_industries"))
    exclude_company_keywords = _finviz_text_list_arg(recipe_args.get("exclude_company_keywords"))
    timeout_seconds = max(_as_int(recipe_args.get("timeout_seconds"), 20), 1)
    cookie_env = _as_optional_text(recipe_args.get("cookie_env")) or "FINVIZ_COOKIE"
    source_kind, source_value = _finviz_source_config(recipe_args)
    generated_at = _iso_now()
    if source_kind is None or source_value is None:
        return {
            "status": "skipped",
            "source_id": str(source_id),
            "recipe": str(recipe),
            "generated_at": generated_at,
            "symbols": [],
            "entries": [],
            "summary": {
                "symbol_count": 0,
                "recipe": str(recipe),
                "source": None,
                "reason": "finviz_source_unconfigured",
            },
            "degradation": {
                "status": "missing",
                "reason": "finviz_source_unconfigured",
            },
        }

    source_text = _load_finviz_csv_text(
        source_kind=source_kind,
        source_value=source_value,
        cookie_env=cookie_env,
        timeout_seconds=timeout_seconds,
    )
    rows, source_format = _parse_finviz_source_rows(source_text)

    candidates: list[dict[str, Any]] = []
    missing_symbol_count = 0
    below_min_price_count = 0
    missing_market_cap_count = 0
    below_min_market_cap_count = 0
    below_min_volume_count = 0
    excluded_instrument_reason_counts: dict[str, int] = {}
    for index, row in enumerate(rows):
        symbol = _normalize_symbol(pick(row, "ticker", "symbol"))
        if symbol is None:
            missing_symbol_count += 1
            continue
        exclusion_reason = _finviz_instrument_exclusion_reason(
            row,
            exclude_industries=exclude_industries,
            exclude_company_keywords=exclude_company_keywords,
        )
        if exclusion_reason is not None:
            excluded_instrument_reason_counts[exclusion_reason] = excluded_instrument_reason_counts.get(exclusion_reason, 0) + 1
            continue
        price = _parse_finviz_float(pick(row, "price", "last", "close"))
        if price is not None and price < min_price:
            below_min_price_count += 1
            continue
        market_cap = _parse_finviz_float(pick(row, "market_cap", "market_capitalization", "mkt_cap"))
        if min_market_cap > 0:
            if market_cap is None:
                missing_market_cap_count += 1
                continue
            if market_cap < min_market_cap:
                below_min_market_cap_count += 1
                continue
        volume = _parse_finviz_int(pick(row, "volume", "vol"))
        if volume is not None and volume < min_volume:
            below_min_volume_count += 1
            continue
        change_percent = _parse_finviz_float(pick(row, "change", "change_percent", "change_pct"))
        relative_volume = _parse_finviz_float(pick(row, "rel_volume", "relative_volume", "rel_vol"))
        raw_rank = parse_int(pick(row, "no", "rank"))
        rank_index = max(raw_rank - 1, 0) if raw_rank is not None else index
        reason_codes = ["finviz_screen"]
        if change_percent is not None:
            if change_percent > 0:
                reason_codes.append("positive_momentum")
            elif change_percent < 0:
                reason_codes.append("negative_momentum")
        if relative_volume is not None and relative_volume >= 1.5:
            reason_codes.append("relative_volume")
        if min_market_cap > 0:
            reason_codes.append("market_cap_filter")
        candidates.append(
            {
                "symbol": symbol,
                "company": pick(row, "company", "name"),
                "sector": pick(row, "sector"),
                "industry": pick(row, "industry"),
                "country": pick(row, "country"),
                "price": None if price is None else round(price, 4),
                "market_cap": None if market_cap is None else int(round(market_cap)),
                "daily_volume": volume,
                "move_percent": (None if change_percent is None else round(change_percent, 4)),
                "relative_volume": (None if relative_volume is None else round(relative_volume, 4)),
                "finviz_rank": None if raw_rank is None else int(raw_rank),
                "finviz_rank_index": rank_index,
                "reason_codes": reason_codes,
                "source_tags": [
                    f"recipe:{str(recipe or '').strip().lower()}",
                    "source:finviz",
                ],
                "raw": row,
            }
        )

    max_abs_move = max(
        [abs(float(item.get("move_percent") or 0.0)) for item in candidates],
        default=0.0,
    )
    max_relative_volume = max(
        [float(item.get("relative_volume") or 0.0) for item in candidates],
        default=0.0,
    )
    max_log_volume = max(
        [math.log1p(max(int(item.get("daily_volume") or 0), 0)) for item in candidates],
        default=0.0,
    )
    for item in candidates:
        rank_score = _rank_score(
            item.get("finviz_rank_index"),
            total=max(len(rows), 1),
            weight=40.0,
        )
        move_percent = abs(float(item.get("move_percent") or 0.0))
        move_score = 25.0 * move_percent / max_abs_move if max_abs_move > 0.0 else 0.0
        relative_volume = float(item.get("relative_volume") or 0.0)
        relative_volume_score = 20.0 * relative_volume / max_relative_volume if max_relative_volume > 0.0 else 0.0
        daily_volume = max(int(item.get("daily_volume") or 0), 0)
        volume_score = 15.0 * math.log1p(daily_volume) / max_log_volume if max_log_volume > 0.0 else 0.0
        item["score"] = round(
            rank_score + move_score + relative_volume_score + volume_score,
            2,
        )
        item.pop("finviz_rank_index", None)

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item.get("score") or 0.0),
            int(item.get("finviz_rank") or 1_000_000),
            str(item.get("symbol") or ""),
        ),
    )
    selected = ranked[:top]
    symbols = [str(item.get("symbol")) for item in selected if str(item.get("symbol") or "").strip()]
    return {
        "status": "completed",
        "source_id": str(source_id),
        "recipe": str(recipe),
        "generated_at": generated_at,
        "symbols": symbols,
        "entries": selected,
        "summary": {
            "symbol_count": len(symbols),
            "candidate_count": len(rows),
            "retained_count": len(candidates),
            "recipe": str(recipe),
            "source": source_kind,
            "source_format": source_format,
            "top": top,
            "min_price": min_price,
            "min_market_cap": min_market_cap,
            "min_volume": min_volume,
            "missing_symbol_count": missing_symbol_count,
            "below_min_price_count": below_min_price_count,
            "missing_market_cap_count": missing_market_cap_count,
            "below_min_market_cap_count": below_min_market_cap_count,
            "below_min_volume_count": below_min_volume_count,
            "excluded_instrument_count": sum(excluded_instrument_reason_counts.values()),
            "excluded_instrument_reason_counts": dict(sorted(excluded_instrument_reason_counts.items())),
        },
        "degradation": {
            "status": "ok" if symbols else "empty",
            "reason": None if symbols else "no_symbols_after_filters",
        },
    }


def build_ticker_source_symbols(
    *,
    recipe: str,
    recipe_args: Mapping[str, Any] | None = None,
    config_root: str | None = None,
) -> tuple[str, ...]:
    normalized_recipe = str(recipe or "").strip().lower()
    normalized_args = _recipe_args(recipe_args)
    if normalized_recipe == "strategy_union":
        return tuple(
            _build_strategy_union_result(
                source_id="ticker_source",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
                config_root=config_root,
            ).get("symbols")
            or []
        )
    if normalized_recipe == "finviz_screener":
        return tuple(
            _run_finviz_screener_feed(
                source_id="ticker_source",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
            ).get("symbols")
            or []
        )
    if normalized_recipe == "stock_prefilter":
        return tuple(
            _run_stock_prefilter_feed(
                source_id="ticker_source",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
            ).get("symbols")
            or []
        )
    raise ValueError(f"Unsupported ticker source recipe: {recipe}")


def run_ticker_source(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any] | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    normalized_args = _recipe_args(recipe_args)
    normalized_recipe = str(recipe or "").strip().lower()
    if normalized_recipe == "strategy_union":
        return _build_strategy_union_result(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
            config_root=config_root,
        )
    if normalized_recipe == "finviz_screener":
        return _run_finviz_screener_feed(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
        )
    if normalized_recipe == "stock_prefilter":
        return _run_stock_prefilter_feed(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
        )
    raise ValueError(f"Unsupported ticker source recipe: {recipe}")


def _snapshot_generated_at(run_record: Mapping[str, Any]) -> str | None:
    result = run_record.get("result")
    if isinstance(result, Mapping):
        generated_at = _as_optional_text(result.get("generated_at"))
        if generated_at is not None:
            return generated_at
    for field_name in ("finished_at", "started_at", "scheduled_for"):
        rendered = _as_optional_text(run_record.get(field_name))
        if rendered is not None:
            return rendered
    return None


def get_latest_ticker_source_snapshot(
    job_store: Any,
    *,
    source_id: str,
    job_key: str,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    latest_run = next(
        iter(job_store.list_job_runs(job_key=job_key, status="succeeded", limit=1)),
        None,
    )
    if latest_run is None:
        return {
            "status": "missing",
            "source_id": str(source_id),
            "job_key": str(job_key),
            "symbols": [],
            "entries": [],
            "summary": {},
            "degradation": {
                "status": "missing",
                "reason": "no_successful_snapshot",
            },
            "job_run_id": None,
            "generated_at": None,
            "age_seconds": None,
        }
    result = latest_run.get("result") if isinstance(latest_run.get("result"), Mapping) else {}
    generated_at = _snapshot_generated_at(latest_run)
    generated_dt = parse_datetime(generated_at)
    age_seconds = None
    if generated_dt is not None:
        age_seconds = max(
            (datetime.now(UTC) - generated_dt.astimezone(UTC)).total_seconds(),
            0.0,
        )
    symbols = [str(symbol).upper() for symbol in list(result.get("symbols") or []) if str(symbol).strip()]
    snapshot_status = "ready" if symbols else "empty"
    if max_age_seconds is not None and generated_dt is not None and age_seconds is not None and age_seconds > max(int(max_age_seconds), 0):
        snapshot_status = "stale"
    return {
        "status": snapshot_status,
        "source_id": str(source_id),
        "job_key": str(job_key),
        "job_run_id": latest_run.get("job_run_id"),
        "generated_at": generated_at,
        "age_seconds": age_seconds,
        "symbols": symbols if snapshot_status != "stale" else [],
        "entries": [dict(item) for item in list(result.get("entries") or []) if isinstance(item, Mapping)],
        "summary": dict(result.get("summary") or {}),
        "degradation": {
            "status": snapshot_status,
            "reason": None if snapshot_status in {"ready", "empty"} else "snapshot_stale",
        },
    }


def resolve_ticker_source_symbols(
    job_store: Any,
    *,
    source_id: str,
    job_key: str,
    max_age_seconds: int | None = None,
    fallback_universe_ref: str | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    snapshot = get_latest_ticker_source_snapshot(
        job_store,
        source_id=source_id,
        job_key=job_key,
        max_age_seconds=max_age_seconds,
    )
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    if snapshot_status in {"ready", "empty"}:
        return {
            "kind": "ticker_source",
            "status": snapshot_status,
            "source_id": str(source_id),
            "job_key": str(job_key),
            "job_run_id": snapshot.get("job_run_id"),
            "generated_at": snapshot.get("generated_at"),
            "age_seconds": snapshot.get("age_seconds"),
            "symbols": list(snapshot.get("symbols") or []),
            "entries": [dict(item) for item in list(snapshot.get("entries") or []) if isinstance(item, Mapping)],
            "summary": dict(snapshot.get("summary") or {}),
            "degradation": dict(snapshot.get("degradation") or {}),
        }
    if fallback_universe_ref:
        fallback_symbols = load_universe_symbols(
            fallback_universe_ref,
            config_root=config_root,
        )
        return {
            "kind": "fallback_universe",
            "status": "fallback",
            "source_id": str(source_id),
            "job_key": str(job_key),
            "fallback_universe_ref": str(fallback_universe_ref),
            "symbols": list(fallback_symbols),
            "summary": {
                "symbol_count": len(fallback_symbols),
                "fallback_universe_ref": str(fallback_universe_ref),
            },
            "degradation": {
                "status": "fallback",
                "reason": snapshot_status or "missing",
            },
            "source_snapshot": snapshot,
        }
    return {
        "kind": "ticker_source",
        "status": snapshot_status or "missing",
        "source_id": str(source_id),
        "job_key": str(job_key),
        "symbols": [],
        "entries": [dict(item) for item in list(snapshot.get("entries") or []) if isinstance(item, Mapping)],
        "summary": dict(snapshot.get("summary") or {}),
        "degradation": dict(snapshot.get("degradation") or {}),
        "source_snapshot": snapshot,
    }


__all__ = [
    "VALID_TICKER_SOURCE_RECIPES",
    "build_ticker_source_symbols",
    "get_latest_ticker_source_snapshot",
    "resolve_ticker_source_symbols",
    "run_ticker_source",
]

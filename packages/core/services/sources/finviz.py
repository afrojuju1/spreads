from __future__ import annotations

import csv
from collections.abc import Mapping
import math
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from core.common import parse_int, pick
from core.integrations.http_client import VendorHttpClient, VendorHttpError
from core.services.alpaca import create_alpaca_client_from_env
from core.services.sources.dispatch import (
    FinvizScreenerRecipeArgs,
    _iso_now,
    _rank_score,
    _recipe_text_arg,
    _target_dte_option_filter_result,
)
from core.value_coercion import as_text, normalize_symbol

FINVIZ_HTTP = VendorHttpClient(timeout_seconds=30, user_agent="spreads-finviz-feed/1.0", follow_redirects=True)

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


_FINVIZ_DEFAULT_HEADERS = (
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
)


def _finviz_ticker_cell_value(cell: Any) -> str:
    explicit_ticker = as_text(cell.get("data-boxover-ticker"))
    if explicit_ticker is not None:
        return explicit_ticker
    ticker_link = cell.select_one("a.tab-link")
    if ticker_link is not None:
        link_text = as_text(ticker_link.get_text(strip=True))
        if link_text is not None:
            return link_text
    first_link = cell.find("a", href=True)
    if first_link is not None:
        ticker_values = parse_qs(urlparse(str(first_link.get("href") or "")).query).get("t") or []
        if ticker_values:
            return str(ticker_values[0])
    return cell.get_text(strip=True)


def _parse_finviz_html_rows(source_text: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    document = BeautifulSoup(source_text, "html.parser")
    table = document.select_one("table.screener_table")
    parsed_headers = [] if table is None else [_normalize_finviz_header(item.get_text(" ", strip=True)) for item in table.select("th")]
    parsed_headers = [item for item in parsed_headers if item]
    headers = parsed_headers or list(_FINVIZ_DEFAULT_HEADERS)
    rows: list[dict[str, Any]] = []
    row_elements = [] if table is None else table.select("tr.styled-row")
    for row_element in row_elements:
        cells = row_element.find_all("td", recursive=False)
        row: dict[str, Any] = {}
        for index, cell in enumerate(cells):
            header = headers[index] if index < len(headers) else f"column_{index}"
            row[header] = _finviz_ticker_cell_value(cell) if header in {"ticker", "symbol"} else cell.get_text(" ", strip=True)
        rows.append(row)
    ticker_header_present = bool({"ticker", "symbol"}.intersection(parsed_headers))
    parse_status = "ok" if table is not None and ticker_header_present else "invalid"
    parse_reason = None
    if table is None:
        parse_reason = "finviz_screener_table_missing"
    elif not ticker_header_present:
        parse_reason = "finviz_ticker_column_missing"
    return rows, {
        "status": parse_status,
        "reason": parse_reason,
        "table_found": table is not None,
        "headers": headers,
    }


def _parse_finviz_source_rows(source_text: str) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    stripped = source_text.lstrip()
    if stripped.startswith("<!DOCTYPE") or stripped.startswith("<html"):
        rows, parse_summary = _parse_finviz_html_rows(source_text)
        return rows, "html", parse_summary
    reader = csv.DictReader(source_text.splitlines())
    headers = [_normalize_finviz_header(item) for item in list(reader.fieldnames or []) if item is not None]
    rows = [
        {_normalize_finviz_header(key): value for key, value in dict(row).items() if key is not None}
        for row in reader
    ]
    ticker_header_present = bool({"ticker", "symbol"}.intersection(headers))
    return rows, "csv", {
        "status": "ok" if ticker_header_present else "invalid",
        "reason": None if ticker_header_present else "finviz_ticker_column_missing",
        "table_found": None,
        "headers": headers,
    }


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
    cookie_value = as_text(os.environ.get(cookie_name))
    if cookie_value is not None:
        headers["Cookie"] = cookie_value
    client = (
        FINVIZ_HTTP
        if timeout_seconds == 30
        else VendorHttpClient(timeout_seconds=timeout_seconds, user_agent="spreads-finviz-feed/1.0", follow_redirects=True)
    )
    return client.request_text("GET", source_value, "", headers=headers).lstrip("\ufeff")


def _finviz_source_config(
    recipe_args: FinvizScreenerRecipeArgs,
) -> tuple[str | None, str | None]:
    source = str(recipe_args.source or "auto").strip().lower()
    source_url = (
        _recipe_text_arg(recipe_args, "source_url", env_field_name="source_url_env")
        or _recipe_text_arg(recipe_args, "csv_url", env_field_name="csv_url_env")
        or _recipe_text_arg(recipe_args, "url", env_field_name="url_env")
    )
    csv_path = _recipe_text_arg(recipe_args, "csv_path", env_field_name="csv_path_env") or _recipe_text_arg(
        recipe_args, "path", env_field_name="path_env"
    )

    if source in {"auto", "csv_export", "csv_url", "url"} and source_url:
        return "csv_url", source_url
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
    args = FinvizScreenerRecipeArgs.model_validate(recipe_args)
    source_symbol_limit = args.source_symbol_limit
    min_price = args.min_price
    min_market_cap = args.min_market_cap
    min_volume = args.min_volume
    exclude_industries = args.exclude_industries
    exclude_company_keywords = args.exclude_company_keywords
    target_option_filter = args.target_option_filter
    timeout_seconds = args.timeout_seconds
    cookie_env = args.cookie_env or "FINVIZ_COOKIE"
    source_kind, source_value = _finviz_source_config(args)
    generated_at = _iso_now()
    if source_kind is None or source_value is None:
        return {
            "status": "skipped",
            "source_id": str(source_id),
            "recipe": str(recipe),
            "generated_at": generated_at,
            "symbols": [],
            "entries": [],
            "observations": [],
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

    try:
        source_text = _load_finviz_csv_text(
            source_kind=source_kind,
            source_value=source_value,
            cookie_env=cookie_env,
            timeout_seconds=timeout_seconds,
        )
    except (OSError, UnicodeError, VendorHttpError) as exc:
        source_error = as_text(getattr(exc, "reason", None)) or type(exc).__name__
        return {
            "status": "degraded",
            "source_id": str(source_id),
            "recipe": str(recipe),
            "generated_at": generated_at,
            "symbols": [],
            "entries": [],
            "observations": [],
            "summary": {
                "symbol_count": 0,
                "candidate_count": 0,
                "recipe": str(recipe),
                "source": source_kind,
                "reason": "finviz_source_fetch_failed",
                "source_error": source_error,
            },
            "degradation": {
                "status": "degraded",
                "reason": "finviz_source_fetch_failed",
            },
        }
    rows, source_format, parse_summary = _parse_finviz_source_rows(source_text)

    candidates: list[dict[str, Any]] = []
    filtered_observations: list[dict[str, Any]] = []
    missing_symbol_count = 0
    below_min_price_count = 0
    missing_market_cap_count = 0
    below_min_market_cap_count = 0
    below_min_volume_count = 0
    excluded_instrument_reason_counts: dict[str, int] = {}
    target_option_filter_reason_counts: dict[str, int] = {}
    target_option_filter_attempt_count = 0
    asset_validation_attempt_count = 0
    unknown_asset_count = 0
    untradable_asset_count = 0
    active_assets_by_symbol: dict[str, dict[str, Any]] = {}
    alpaca_client: Any | None = None
    asset_universe_error: str | None = None
    if rows:
        try:
            alpaca_client = create_alpaca_client_from_env()
            for asset in alpaca_client.list_active_us_equity_assets():
                asset_symbol = normalize_symbol(asset.get("symbol"))
                if asset_symbol is not None:
                    active_assets_by_symbol[asset_symbol] = dict(asset)
        except Exception as exc:
            asset_universe_error = str(exc)
    for index, row in enumerate(rows):
        symbol = normalize_symbol(pick(row, "ticker", "symbol"))
        if symbol is None:
            missing_symbol_count += 1
            continue
        price = _parse_finviz_float(pick(row, "price", "last", "close"))
        market_cap = _parse_finviz_float(pick(row, "market_cap", "market_capitalization", "mkt_cap"))
        volume = _parse_finviz_int(pick(row, "volume", "vol"))
        change_percent = _parse_finviz_float(pick(row, "change", "change_percent", "change_pct"))
        relative_volume = _parse_finviz_float(pick(row, "rel_volume", "relative_volume", "rel_vol"))
        raw_rank = parse_int(pick(row, "no", "rank"))
        rank_index = max(raw_rank - 1, 0) if raw_rank is not None else index
        base_observation = {
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
            "rank": None if raw_rank is None else int(raw_rank),
            "finviz_rank_index": rank_index,
            "source_tags": [
                f"recipe:{str(recipe or '').strip().lower()}",
                "source:finviz",
            ],
            "raw": row,
        }
        exclusion_reason = _finviz_instrument_exclusion_reason(
            row,
            exclude_industries=exclude_industries,
            exclude_company_keywords=exclude_company_keywords,
        )
        if exclusion_reason is not None:
            excluded_instrument_reason_counts[exclusion_reason] = excluded_instrument_reason_counts.get(exclusion_reason, 0) + 1
            filtered_observations.append(
                {
                    **base_observation,
                    "observation_state": "excluded",
                    "reason_codes": ["finviz_screen", exclusion_reason],
                }
            )
            continue
        if price is not None and price < min_price:
            below_min_price_count += 1
            filtered_observations.append(
                {
                    **base_observation,
                    "observation_state": "filtered_out",
                    "reason_codes": ["finviz_screen", "below_min_price"],
                }
            )
            continue
        if min_market_cap > 0:
            if market_cap is None:
                missing_market_cap_count += 1
                filtered_observations.append(
                    {
                        **base_observation,
                        "observation_state": "filtered_out",
                        "reason_codes": ["finviz_screen", "missing_market_cap"],
                    }
                )
                continue
            if market_cap < min_market_cap:
                below_min_market_cap_count += 1
                filtered_observations.append(
                    {
                        **base_observation,
                        "observation_state": "filtered_out",
                        "reason_codes": ["finviz_screen", "below_min_market_cap"],
                    }
                )
                continue
        if volume is not None and volume < min_volume:
            below_min_volume_count += 1
            filtered_observations.append(
                {
                    **base_observation,
                    "observation_state": "filtered_out",
                    "reason_codes": ["finviz_screen", "below_min_volume"],
                }
            )
            continue
        asset_validation_attempt_count += 1
        if asset_universe_error is not None:
            filtered_observations.append(
                {
                    **base_observation,
                    "observation_state": "filtered_out",
                    "reason_codes": ["finviz_screen", "alpaca_asset_filter_unavailable"],
                    "alpaca_error": asset_universe_error,
                }
            )
            continue
        asset = active_assets_by_symbol.get(symbol)
        if asset is None:
            unknown_asset_count += 1
            filtered_observations.append(
                {
                    **base_observation,
                    "observation_state": "filtered_out",
                    "reason_codes": ["finviz_screen", "alpaca_asset_unknown"],
                }
            )
            continue
        if asset.get("tradable") is False:
            untradable_asset_count += 1
            filtered_observations.append(
                {
                    **base_observation,
                    "observation_state": "filtered_out",
                    "reason_codes": ["finviz_screen", "alpaca_asset_not_tradable"],
                    "alpaca_asset": {
                        "id": asset.get("id"),
                        "exchange": asset.get("exchange"),
                        "name": asset.get("name"),
                        "status": asset.get("status"),
                        "tradable": asset.get("tradable"),
                    },
                }
            )
            continue
        base_observation["alpaca_asset"] = {
            "id": asset.get("id"),
            "asset_class": asset.get("class") or asset.get("asset_class"),
            "exchange": asset.get("exchange"),
            "name": asset.get("name"),
            "status": asset.get("status"),
            "tradable": asset.get("tradable"),
        }
        base_observation["source_tags"] = [*base_observation["source_tags"], "source:alpaca"]
        target_option_filter_result: dict[str, Any] | None = None
        if target_option_filter.enabled:
            target_option_filter_attempt_count += 1
            try:
                target_option_filter_result = _target_dte_option_filter_result(
                    client=alpaca_client,
                    symbol=symbol,
                    config=target_option_filter,
                )
            except Exception as exc:
                target_option_filter_result = {
                    "status": "filtered_out",
                    "reason": "target_dte_option_filter_error",
                    "error": str(exc),
                    "min_dte": target_option_filter.min_dte,
                    "max_dte": target_option_filter.max_dte,
                    "feed": target_option_filter.feed,
                }
            filter_status = str(target_option_filter_result.get("status") or "").strip().lower()
            if filter_status != "passed":
                reason = str(target_option_filter_result.get("reason") or "target_dte_option_filter_failed")
                target_option_filter_reason_counts[reason] = target_option_filter_reason_counts.get(reason, 0) + 1
                filtered_observations.append(
                    {
                        **base_observation,
                        "observation_state": "filtered_out",
                        "reason_codes": ["finviz_screen", reason],
                        "source_tags": [*base_observation["source_tags"], "filter:target_dte_options"],
                        "target_dte_option_filter": target_option_filter_result,
                    }
                )
                continue
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
        source_tags = list(base_observation["source_tags"])
        if target_option_filter_result is not None:
            reason_codes.append("target_dte_options_available")
            source_tags.append("filter:target_dte_options")
            if int(target_option_filter_result.get("expected_move_count") or 0) > 0:
                reason_codes.append("target_dte_expected_move_available")
        candidates.append(
            {
                **base_observation,
                "observation_state": "observed",
                "reason_codes": reason_codes,
                "source_tags": source_tags,
                "target_dte_option_filter": target_option_filter_result,
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
    selected_candidates = ranked if source_symbol_limit is None else ranked[:source_symbol_limit]
    selected = [{**dict(item), "observation_state": "selected"} for item in selected_candidates]
    symbols = [str(item.get("symbol")) for item in selected if str(item.get("symbol") or "").strip()]
    selected_symbol_set = set(symbols)
    observations = [
        {
            **dict(item),
            "observation_state": "selected" if str(item.get("symbol") or "") in selected_symbol_set else "observed",
        }
        for item in ranked
    ] + filtered_observations
    invalid_asset_count = unknown_asset_count + untradable_asset_count
    target_option_filter_error_count = int(target_option_filter_reason_counts.get("target_dte_option_filter_error") or 0)
    result_status = "completed"
    degradation_status = "ok" if symbols else "empty"
    degradation_reason = None if symbols else "no_symbols_after_filters"
    parse_reason = as_text(parse_summary.get("reason"))
    if str(parse_summary.get("status") or "").strip().lower() != "ok":
        result_status = "degraded"
        degradation_status = "degraded"
        degradation_reason = parse_reason or "finviz_response_schema_invalid"
    elif rows and missing_symbol_count == len(rows):
        result_status = "degraded"
        degradation_status = "degraded"
        degradation_reason = "finviz_symbols_unparseable"
    elif asset_universe_error is not None:
        result_status = "degraded"
        degradation_status = "degraded"
        degradation_reason = "alpaca_asset_filter_unavailable"
    elif asset_validation_attempt_count > 0 and invalid_asset_count == asset_validation_attempt_count:
        result_status = "degraded"
        degradation_status = "degraded"
        degradation_reason = "finviz_symbols_not_in_asset_universe"
    elif target_option_filter_attempt_count > 0 and target_option_filter_error_count == target_option_filter_attempt_count:
        result_status = "degraded"
        degradation_status = "degraded"
        degradation_reason = "target_dte_option_filter_unavailable"
    elif missing_symbol_count > 0:
        degradation_status = "partial"
        degradation_reason = "finviz_symbols_partially_unparseable"
    elif invalid_asset_count > 0:
        degradation_status = "partial"
        degradation_reason = "finviz_symbols_partially_invalid"
    elif target_option_filter_error_count > 0:
        degradation_status = "partial"
        degradation_reason = "target_dte_option_filter_partially_unavailable"
    return {
        "status": result_status,
        "source_id": str(source_id),
        "recipe": str(recipe),
        "generated_at": generated_at,
        "symbols": symbols,
        "entries": selected,
        "observations": observations,
        "summary": {
            "symbol_count": len(symbols),
            "candidate_count": len(rows),
            "observed_count": len(observations),
            "retained_count": len(candidates),
            "recipe": str(recipe),
            "source": source_kind,
            "source_format": source_format,
            "source_parse": parse_summary,
            "source_symbol_limit": source_symbol_limit,
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
            "asset_validation_attempt_count": asset_validation_attempt_count,
            "active_asset_count": len(active_assets_by_symbol),
            "unknown_asset_count": unknown_asset_count,
            "untradable_asset_count": untradable_asset_count,
            "asset_universe_error": asset_universe_error,
            "target_dte_option_filter": {
                "enabled": target_option_filter.enabled,
                "min_dte": target_option_filter.min_dte,
                "max_dte": target_option_filter.max_dte,
                "feed": target_option_filter.feed,
                "require_expected_move": target_option_filter.require_expected_move,
                "min_expected_move_count": target_option_filter.min_expected_move_count,
                "attempt_count": target_option_filter_attempt_count,
                "error_count": target_option_filter_error_count,
                "filtered_count": sum(target_option_filter_reason_counts.values()),
                "reason_counts": dict(sorted(target_option_filter_reason_counts.items())),
            },
        },
        "degradation": {
            "status": degradation_status,
            "reason": degradation_reason,
        },
    }

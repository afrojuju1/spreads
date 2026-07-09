from __future__ import annotations

import csv
from collections.abc import Mapping
import html
import math
import os
from pathlib import Path
import re
from typing import Any

from core.common import parse_int, pick
from core.integrations.alpaca.client import AlpacaRequestError
from core.integrations.http_client import VendorHttpClient
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

    source_text = _load_finviz_csv_text(
        source_kind=source_kind,
        source_value=source_value,
        cookie_env=cookie_env,
        timeout_seconds=timeout_seconds,
    )
    rows, source_format = _parse_finviz_source_rows(source_text)

    candidates: list[dict[str, Any]] = []
    filtered_observations: list[dict[str, Any]] = []
    missing_symbol_count = 0
    below_min_price_count = 0
    missing_market_cap_count = 0
    below_min_market_cap_count = 0
    below_min_volume_count = 0
    excluded_instrument_reason_counts: dict[str, int] = {}
    target_option_filter_reason_counts: dict[str, int] = {}
    target_option_filter_client: Any | None = None
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
        target_option_filter_result: dict[str, Any] | None = None
        if target_option_filter.enabled:
            if target_option_filter_client is None:
                target_option_filter_client = create_alpaca_client_from_env()
            try:
                target_option_filter_result = _target_dte_option_filter_result(
                    client=target_option_filter_client,
                    symbol=symbol,
                    config=target_option_filter,
                )
            except AlpacaRequestError as exc:
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
    return {
        "status": "completed",
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
            "target_dte_option_filter": {
                "enabled": target_option_filter.enabled,
                "min_dte": target_option_filter.min_dte,
                "max_dte": target_option_filter.max_dte,
                "feed": target_option_filter.feed,
                "require_expected_move": target_option_filter.require_expected_move,
                "min_expected_move_count": target_option_filter.min_expected_move_count,
                "filtered_count": sum(target_option_filter_reason_counts.values()),
                "reason_counts": dict(sorted(target_option_filter_reason_counts.items())),
            },
        },
        "degradation": {
            "status": "ok" if symbols else "empty",
            "reason": None if symbols else "no_symbols_after_filters",
        },
    }

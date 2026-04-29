from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import math
import re
from typing import Any

from core.common import parse_float, parse_int, pick
from core.integrations.alpaca.client import AlpacaRequestError
from core.services.alpaca import create_alpaca_client_from_env
from core.services.automations import load_universe_symbols
from core.services.bots import build_entry_automation_symbols
from core.storage.serializers import parse_datetime


VALID_SYMBOL_FEED_RECIPES = frozenset({"automation_union", "stock_prefilter"})


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


def _normalize_symbol(value: Any) -> str | None:
    rendered = str(value or "").strip().upper()
    return rendered or None


_LEVERAGE_REGEX = re.compile(r"\b(?:[2-9](?:\.\d+)?x|ultra|ultrapro|leveraged|leverage)\b")
_INVERSE_REGEX = re.compile(r"\b(?:inverse|short|bear|ultrashort)\b")
_ETF_NAME_REGEX = re.compile(
    r"\b(?:etf|trust|fund|shares|direxion|proshares|graniteshares|yieldmax)\b"
)


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
    prev_daily_bar = (
        snapshot.get("prevDailyBar")
        if isinstance(snapshot.get("prevDailyBar"), Mapping)
        else {}
    )
    current_close = parse_float(pick(daily_bar, "c", "close"))
    previous_close = parse_float(pick(prev_daily_bar, "c", "close"))
    if current_close is None or previous_close is None or previous_close <= 0:
        return None
    return ((current_close - previous_close) / previous_close) * 100.0


def _rank_score(rank: int | None, *, total: int, weight: float) -> float:
    if rank is None or total <= 0:
        return 0.0
    return max(weight * float(total - rank) / float(total), 0.0)


def _build_automation_union_result(
    *,
    feed_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any],
    config_root: str | None,
) -> dict[str, Any]:
    symbols = build_entry_automation_symbols(
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
        "feed_id": str(feed_id),
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
    feed_id: str,
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
        raise RuntimeError(
            f"Stock prefilter feed {feed_id} produced no screener candidates"
        )

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
        symbol: rank
        for rank, item in enumerate(most_actives)
        for symbol in [_normalize_symbol(item.get("symbol"))]
        if symbol is not None
    }
    most_active_item_by_symbol = {
        symbol: item
        for item in most_actives
        for symbol in [_normalize_symbol(item.get("symbol"))]
        if symbol is not None
    }
    gainer_rank_by_symbol = {
        symbol: rank
        for rank, item in enumerate(gainers)
        for symbol in [_normalize_symbol(item.get("symbol"))]
        if symbol is not None
    }
    loser_rank_by_symbol = {
        symbol: rank
        for rank, item in enumerate(losers)
        for symbol in [_normalize_symbol(item.get("symbol"))]
        if symbol is not None
    }
    mover_item_by_symbol = {
        symbol: item
        for item in [*gainers, *losers]
        for symbol in [_normalize_symbol(item.get("symbol"))]
        if symbol is not None
    }

    candidates: list[dict[str, Any]] = []
    excluded_leveraged_inverse_count = 0
    below_min_daily_volume_count = 0
    for symbol in candidate_symbols:
        if optionable_symbols is not None and symbol not in optionable_symbols:
            continue
        asset = optionable_assets_by_symbol.get(symbol)
        if (
            exclude_leveraged_and_inverse_etfs
            and _looks_like_leveraged_or_inverse_etf(asset)
        ):
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
        mover_percent = parse_float(
            (mover_item_by_symbol.get(symbol) or {}).get("percent_change")
        )
        move_percent = (
            mover_percent
            if mover_percent is not None
            else snapshot_move_percent
        )
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
                "trade_count": parse_int(
                    (most_active_item_by_symbol.get(symbol) or {}).get("trade_count")
                ),
                "most_active_volume": parse_int(
                    (most_active_item_by_symbol.get(symbol) or {}).get("volume")
                ),
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
        move_score = (
            20.0 * move_percent / max_abs_move
            if max_abs_move > 0.0
            else 0.0
        )
        daily_volume = max(int(item.get("daily_volume") or 0), 0)
        volume_score = (
            10.0 * math.log1p(daily_volume) / max_log_volume
            if max_log_volume > 0.0
            else 0.0
        )
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
        raise RuntimeError(
            f"Stock prefilter feed {feed_id} produced no symbols after filters"
        )
    generated_at = _iso_now()
    degradation_status = "ok" if symbols and not issues else "partial" if symbols else "empty"
    degradation_reason = None
    if not symbols:
        degradation_reason = "no_symbols_after_filters"
    elif issues:
        degradation_reason = issues[0]
    return {
        "status": "completed",
        "feed_id": str(feed_id),
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


def build_symbol_feed_symbols(
    *,
    recipe: str,
    recipe_args: Mapping[str, Any] | None = None,
    config_root: str | None = None,
) -> tuple[str, ...]:
    normalized_recipe = str(recipe or "").strip().lower()
    normalized_args = _recipe_args(recipe_args)
    if normalized_recipe == "automation_union":
        return tuple(
            _build_automation_union_result(
                feed_id="symbol_feed",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
                config_root=config_root,
            ).get("symbols")
            or []
        )
    if normalized_recipe == "stock_prefilter":
        return tuple(
            _run_stock_prefilter_feed(
                feed_id="symbol_feed",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
            ).get("symbols")
            or []
        )
    raise ValueError(f"Unsupported symbol feed recipe: {recipe}")


def run_symbol_feed(
    *,
    feed_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any] | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    normalized_args = _recipe_args(recipe_args)
    normalized_recipe = str(recipe or "").strip().lower()
    if normalized_recipe == "automation_union":
        return _build_automation_union_result(
            feed_id=feed_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
            config_root=config_root,
        )
    if normalized_recipe == "stock_prefilter":
        return _run_stock_prefilter_feed(
            feed_id=feed_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
        )
    raise ValueError(f"Unsupported symbol feed recipe: {recipe}")


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


def get_latest_symbol_feed_snapshot(
    job_store: Any,
    *,
    feed_id: str,
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
            "feed_id": str(feed_id),
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
    symbols = [
        str(symbol).upper()
        for symbol in list(result.get("symbols") or [])
        if str(symbol).strip()
    ]
    snapshot_status = "ready" if symbols else "empty"
    if (
        max_age_seconds is not None
        and generated_dt is not None
        and age_seconds is not None
        and age_seconds > max(int(max_age_seconds), 0)
    ):
        snapshot_status = "stale"
    return {
        "status": snapshot_status,
        "feed_id": str(feed_id),
        "job_key": str(job_key),
        "job_run_id": latest_run.get("job_run_id"),
        "generated_at": generated_at,
        "age_seconds": age_seconds,
        "symbols": symbols if snapshot_status != "stale" else [],
        "entries": [dict(item) for item in list(result.get("entries") or []) if isinstance(item, Mapping)],
        "summary": dict(result.get("summary") or {}),
        "degradation": {
            "status": snapshot_status,
            "reason": None
            if snapshot_status in {"ready", "empty"}
            else "snapshot_stale",
        },
    }


def resolve_symbol_feed_symbols(
    job_store: Any,
    *,
    feed_id: str,
    job_key: str,
    max_age_seconds: int | None = None,
    fallback_universe_ref: str | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    snapshot = get_latest_symbol_feed_snapshot(
        job_store,
        feed_id=feed_id,
        job_key=job_key,
        max_age_seconds=max_age_seconds,
    )
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    if snapshot_status in {"ready", "empty"}:
        return {
            "kind": "symbol_feed",
            "status": snapshot_status,
            "feed_id": str(feed_id),
            "job_key": str(job_key),
            "job_run_id": snapshot.get("job_run_id"),
            "generated_at": snapshot.get("generated_at"),
            "age_seconds": snapshot.get("age_seconds"),
            "symbols": list(snapshot.get("symbols") or []),
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
            "feed_id": str(feed_id),
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
            "feed_snapshot": snapshot,
        }
    return {
        "kind": "symbol_feed",
        "status": snapshot_status or "missing",
        "feed_id": str(feed_id),
        "job_key": str(job_key),
        "symbols": [],
        "summary": dict(snapshot.get("summary") or {}),
        "degradation": dict(snapshot.get("degradation") or {}),
        "feed_snapshot": snapshot,
    }


__all__ = [
    "VALID_SYMBOL_FEED_RECIPES",
    "build_symbol_feed_symbols",
    "get_latest_symbol_feed_snapshot",
    "resolve_symbol_feed_symbols",
    "run_symbol_feed",
]

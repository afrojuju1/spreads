from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Any

from core.common import parse_float, parse_int
from core.integrations.alpaca.client import AlpacaRequestError
from core.services.alpaca import create_alpaca_client_from_env
from core.services.sources.dispatch import (
    StockPrefilterRecipeArgs,
    _iso_now,
    _looks_like_leveraged_or_inverse_etf,
    _rank_score,
    _stock_snapshot_daily_percent_change,
    _stock_snapshot_daily_volume,
    _stock_snapshot_price,
)
from core.value_coercion import normalize_symbol

def _run_stock_prefilter_feed(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any],
) -> dict[str, Any]:
    args = StockPrefilterRecipeArgs.model_validate(recipe_args)
    top = args.top
    most_actives_top = args.most_actives_top or max(top * 2, 25)
    movers_top = args.movers_top or max(top * 2, 25)
    min_price = args.min_price
    min_daily_volume = args.min_daily_volume
    news_limit = args.news_limit or max(top * 3, 25)
    most_actives_by = args.most_actives_by
    stock_feed = args.stock_feed
    exclude_leveraged_and_inverse_etfs = args.exclude_leveraged_and_inverse_etfs

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
            for symbol in [normalize_symbol(item.get("symbol"))]
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
            symbol = normalize_symbol(item.get("symbol"))
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
                symbol = normalize_symbol(raw_symbol)
                if symbol is not None and symbol in candidate_symbols:
                    news_count_by_symbol[symbol] = news_count_by_symbol.get(symbol, 0) + 1
    except AlpacaRequestError:
        issues.append("news_unavailable")

    most_active_rank_by_symbol = {
        symbol: rank for rank, item in enumerate(most_actives) for symbol in [normalize_symbol(item.get("symbol"))] if symbol is not None
    }
    most_active_item_by_symbol = {symbol: item for item in most_actives for symbol in [normalize_symbol(item.get("symbol"))] if symbol is not None}
    gainer_rank_by_symbol = {
        symbol: rank for rank, item in enumerate(gainers) for symbol in [normalize_symbol(item.get("symbol"))] if symbol is not None
    }
    loser_rank_by_symbol = {
        symbol: rank for rank, item in enumerate(losers) for symbol in [normalize_symbol(item.get("symbol"))] if symbol is not None
    }
    mover_item_by_symbol = {symbol: item for item in [*gainers, *losers] for symbol in [normalize_symbol(item.get("symbol"))] if symbol is not None}

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
    selected = [{**dict(item), "observation_state": "selected"} for item in ranked[:top]]
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
        "observations": [
            {
                **dict(item),
                "observation_state": "selected" if str(item.get("symbol") or "") in set(symbols) else "observed",
            }
            for item in ranked
        ],
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

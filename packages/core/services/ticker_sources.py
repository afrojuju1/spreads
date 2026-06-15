from __future__ import annotations

import csv
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import html
import math
import os
from pathlib import Path
import re
from typing import Any

from core.common import parse_float, parse_int, pick
from core.integrations.calendar_events.models import EarningsEventConsensusRecord
from core.integrations.calendar_events.store import CalendarEventStore
from core.integrations.alpaca.client import AlpacaRequestError
from core.integrations.http_client import VendorHttpClient
from core.runtime.config import default_database_url
from core.services.market_dates import NEW_YORK
from core.services.alpaca import create_alpaca_client_from_env
from core.services.strategy_candidate_builders.market_data import build_expected_move_estimates, group_contracts_by_expiration
from core.services.trading_strategies import build_entry_strategy_symbols, load_universe_symbols
from core.storage.serializers import parse_date as _parse_date, parse_datetime as _parse_datetime
from core.value_coercion import utc_now_iso as _iso_now

VALID_TICKER_SOURCE_RECIPES = frozenset({"strategy_union", "finviz_screener", "stock_prefilter", "earnings_event_window"})
FINVIZ_HTTP = VendorHttpClient(timeout_seconds=30, user_agent="spreads-finviz-feed/1.0", follow_redirects=True)


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


def _as_positive_int_or_none(value: Any) -> int | None:
    parsed = parse_int(value)
    if parsed is None:
        return None
    return max(int(parsed), 1)


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


@dataclass(frozen=True)
class TargetDteOptionFilterConfig:
    enabled: bool
    min_dte: int
    max_dte: int
    feed: str
    stock_feed: str
    require_expected_move: bool
    min_expected_move_count: int


def _target_dte_option_filter_config(recipe_args: Mapping[str, Any]) -> TargetDteOptionFilterConfig:
    raw = recipe_args.get("target_dte_options") or recipe_args.get("target_dte_option_filter")
    mapping = raw if isinstance(raw, Mapping) else {}
    enabled = _as_bool(mapping.get("enabled"), False) if isinstance(raw, Mapping) else _as_bool(raw, False)
    min_dte = max(_as_int(mapping.get("min_dte", recipe_args.get("min_dte")), 7), 0)
    max_dte = max(_as_int(mapping.get("max_dte", recipe_args.get("max_dte")), 21), min_dte)
    return TargetDteOptionFilterConfig(
        enabled=enabled,
        min_dte=min_dte,
        max_dte=max_dte,
        feed=_as_optional_text(mapping.get("feed") or recipe_args.get("feed")) or "opra",
        stock_feed=_as_optional_text(mapping.get("stock_feed") or recipe_args.get("stock_feed")) or "sip",
        require_expected_move=_as_bool(mapping.get("require_expected_move"), True),
        min_expected_move_count=max(_as_int(mapping.get("min_expected_move_count"), 1), 1),
    )


def _target_dte_option_filter_result(
    *,
    client: Any,
    symbol: str,
    config: TargetDteOptionFilterConfig,
) -> dict[str, Any]:
    reference_date = datetime.now(NEW_YORK).date()
    min_expiration = (reference_date + timedelta(days=config.min_dte)).isoformat()
    max_expiration = (reference_date + timedelta(days=config.max_dte)).isoformat()
    result: dict[str, Any] = {
        "status": "passed",
        "reason": None,
        "min_dte": config.min_dte,
        "max_dte": config.max_dte,
        "min_expiration": min_expiration,
        "max_expiration": max_expiration,
        "feed": config.feed,
        "require_expected_move": config.require_expected_move,
        "min_expected_move_count": config.min_expected_move_count,
    }

    call_contracts = client.list_option_contracts(
        symbol,
        min_expiration,
        max_expiration,
        option_type="call",
    )
    put_contracts = client.list_option_contracts(
        symbol,
        min_expiration,
        max_expiration,
        option_type="put",
    )
    call_contracts_by_expiration = group_contracts_by_expiration(call_contracts)
    put_contracts_by_expiration = group_contracts_by_expiration(put_contracts)
    expirations = sorted(set(call_contracts_by_expiration) | set(put_contracts_by_expiration))
    common_expirations = sorted(set(call_contracts_by_expiration) & set(put_contracts_by_expiration))
    result.update(
        {
            "call_contract_count": len(call_contracts),
            "put_contract_count": len(put_contracts),
            "contract_count": len(call_contracts) + len(put_contracts),
            "expiration_count": len(expirations),
            "common_expiration_count": len(common_expirations),
            "expirations": expirations,
        }
    )
    if not call_contracts or not put_contracts:
        result.update({"status": "filtered_out", "reason": "target_dte_contracts_missing"})
        return result
    if not common_expirations:
        result.update({"status": "filtered_out", "reason": "target_dte_common_expiration_missing"})
        return result
    if not config.require_expected_move:
        return result

    spot_price = client.get_underlying_price(symbol, config.stock_feed)
    call_snapshots_by_expiration = {}
    put_snapshots_by_expiration = {}
    for expiration_date in common_expirations:
        call_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            symbol,
            expiration_date,
            "call",
            config.feed,
        )
        put_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            symbol,
            expiration_date,
            "put",
            config.feed,
        )
    expected_moves = build_expected_move_estimates(
        spot_price=spot_price,
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
    )
    result.update(
        {
            "spot_price": round(float(spot_price), 4),
            "expected_move_count": len(expected_moves),
            "expected_move_expirations": sorted(expected_moves),
        }
    )
    if len(expected_moves) < config.min_expected_move_count:
        result.update({"status": "filtered_out", "reason": "target_dte_expected_move_missing"})
    return result


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


_CONFIDENCE_RANK = {
    "unknown": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


def _confidence_rank(value: Any) -> int:
    return _CONFIDENCE_RANK.get(str(value or "unknown").strip().lower(), 0)


def _confidence_passes(value: Any, minimum: Any) -> bool:
    return _confidence_rank(value) >= _confidence_rank(minimum)


def _earnings_confidence_reason(value: Any) -> str:
    confidence = str(value or "unknown").strip().lower() or "unknown"
    return f"earnings_consensus_{confidence}"


def _earnings_session_reason(value: Any) -> str | None:
    timing = str(value or "unknown").strip().lower()
    if timing in {"before_open", "after_close", "during_market"}:
        return f"earnings_timing_{timing}"
    return None


def _event_days_to_event(event_date: str, *, now_date: Any) -> int | None:
    try:
        return (_parse_date(event_date) - now_date).days
    except (TypeError, ValueError):
        return None


def _base_earnings_event_observation(
    record: EarningsEventConsensusRecord,
    *,
    now_date: Any,
    recipe: str,
) -> dict[str, Any]:
    days_to_event = _event_days_to_event(record.event_date, now_date=now_date)
    reason_codes = [
        "earnings_event_window",
        _earnings_confidence_reason(record.source_confidence),
    ]
    if (session_reason := _earnings_session_reason(record.session_timing)) is not None:
        reason_codes.append(session_reason)
    if str(record.consensus_status or "").strip().lower() == "conflict":
        reason_codes.append("earnings_date_conflict")
    return {
        "symbol": record.symbol,
        "observation_state": "observed",
        "event_date": record.event_date,
        "scheduled_at": record.scheduled_at,
        "session_timing": record.session_timing,
        "days_to_event": days_to_event,
        "event_status": record.event_status,
        "source_confidence": record.source_confidence,
        "timing_confidence": record.timing_confidence,
        "consensus_status": record.consensus_status,
        "primary_source": record.primary_source,
        "supporting_sources": list(record.supporting_sources),
        "conflicting_sources": list(record.conflicting_sources),
        "computed_at": record.computed_at,
        "stale_after": record.stale_after,
        "provider_payload": dict(record.provider_payload or {}),
        "reason_codes": reason_codes,
        "source_tags": [
            f"recipe:{str(recipe or '').strip().lower()}",
            "source:earnings_event_consensus",
            f"source_confidence:{record.source_confidence}",
            f"timing_confidence:{record.timing_confidence}",
            f"session:{record.session_timing}",
        ],
    }


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
        candidate_builder_key=_as_optional_text(recipe_args.get("candidate_builder")),
        build_profile=_as_optional_text(recipe_args.get("build_profile")),
    )
    source_tags = [f"recipe:{str(recipe or '').strip().lower()}"]
    if (build_profile := _as_optional_text(recipe_args.get("build_profile"))) is not None:
        source_tags.append(f"build_profile:{build_profile}")
    if (candidate_builder := _as_optional_text(recipe_args.get("candidate_builder"))) is not None:
        source_tags.append(f"candidate_builder:{candidate_builder}")
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
                "observation_state": "selected",
                "score": None,
                "reason_codes": [str(recipe)],
                "source_tags": list(source_tags),
            }
            for symbol in symbols
        ],
        "observations": [
            {
                "symbol": symbol,
                "observation_state": "selected",
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


def _dedupe_earnings_records_by_symbol(records: list[EarningsEventConsensusRecord]) -> list[EarningsEventConsensusRecord]:
    deduped: dict[str, EarningsEventConsensusRecord] = {}

    def sort_key(record: EarningsEventConsensusRecord) -> tuple[str, str]:
        return (str(record.event_date or ""), str(record.symbol or ""))

    for record in sorted(records, key=sort_key):
        symbol = _normalize_symbol(record.symbol)
        if symbol is None or symbol in deduped:
            continue
        deduped[symbol] = record
    return list(deduped.values())


def _record_filtered_earnings_observation(
    observations: list[dict[str, Any]],
    base_observation: Mapping[str, Any],
    reason: str,
    *,
    extra: Mapping[str, Any] | None = None,
) -> None:
    extra_mapping = dict(extra or {})
    reason_codes = list(extra_mapping.pop("reason_codes", base_observation.get("reason_codes") or []))
    if reason not in reason_codes:
        reason_codes.append(reason)
    observations.append(
        {
            **dict(base_observation),
            **extra_mapping,
            "observation_state": "filtered_out",
            "reason_codes": reason_codes,
        }
    )


def _run_earnings_event_window_feed(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any],
) -> dict[str, Any]:
    generated_at = _iso_now()
    now = datetime.now(UTC)
    now_date = datetime.now(NEW_YORK).date()
    lookahead_days = max(_as_int(recipe_args.get("lookahead_days", recipe_args.get("window_days")), 30), 1)
    front_window_days = max(_as_int(recipe_args.get("front_window_days"), 10), 1)
    min_source_confidence = _as_optional_text(recipe_args.get("min_source_confidence")) or "medium"
    include_conflicts = _as_bool(recipe_args.get("include_conflicts", recipe_args.get("allow_conflicts")), False)
    min_price = max(_as_float(recipe_args.get("min_price"), 10.0), 0.0)
    min_daily_volume = max(_as_int(recipe_args.get("min_daily_volume", recipe_args.get("min_volume")), 1_000_000), 0)
    max_symbols = max(_as_int(recipe_args.get("max_symbols"), 25), 1)
    actionability_candidate_limit = max(
        _as_int(recipe_args.get("actionability_candidate_limit"), max(max_symbols * 4, 50)),
        max_symbols,
    )
    stock_feed = _as_optional_text(recipe_args.get("stock_feed")) or "sip"
    target_option_filter = _target_dte_option_filter_config(recipe_args)
    window_start = now_date.isoformat()
    window_end = (now_date + timedelta(days=lookahead_days)).isoformat()

    store = CalendarEventStore(default_database_url())
    try:
        records = _dedupe_earnings_records_by_symbol(
            store.query_earnings_event_consensus(
                window_start=window_start,
                window_end=window_end,
            )
        )
    finally:
        store.close()

    if not records:
        return {
            "status": "completed",
            "source_id": str(source_id),
            "recipe": str(recipe),
            "generated_at": generated_at,
            "symbols": [],
            "entries": [],
            "observations": [],
            "summary": {
                "symbol_count": 0,
                "recipe": str(recipe),
                "window_start": window_start,
                "window_end": window_end,
                "lookahead_days": lookahead_days,
                "reason": "no_earnings_events",
                "consensus_count": 0,
            },
            "degradation": {
                "status": "empty",
                "reason": "no_earnings_events",
            },
        }

    observations: list[dict[str, Any]] = []
    eligible: list[dict[str, Any]] = []
    stale_count = 0
    conflict_count = 0
    below_confidence_count = 0
    for record in records:
        base_observation = _base_earnings_event_observation(
            record,
            now_date=now_date,
            recipe=recipe,
        )
        stale_after = _parse_datetime(record.stale_after)
        if stale_after is not None and stale_after < now:
            stale_count += 1
            _record_filtered_earnings_observation(observations, base_observation, "earnings_consensus_stale")
            continue
        if str(record.consensus_status or "").strip().lower() == "conflict" and not include_conflicts:
            conflict_count += 1
            _record_filtered_earnings_observation(observations, base_observation, "earnings_date_conflict")
            continue
        if not _confidence_passes(record.source_confidence, min_source_confidence):
            below_confidence_count += 1
            _record_filtered_earnings_observation(
                observations,
                base_observation,
                "below_min_source_confidence",
                extra={"min_source_confidence": min_source_confidence},
            )
            continue
        eligible.append(dict(base_observation))

    def urgency_key(item: Mapping[str, Any]) -> tuple[int, int, str]:
        days_to_event = item.get("days_to_event")
        normalized_days = int(days_to_event) if isinstance(days_to_event, int) else 1_000_000
        return (
            normalized_days,
            -_confidence_rank(item.get("source_confidence")),
            str(item.get("symbol") or ""),
        )

    eligible = sorted(eligible, key=urgency_key)
    actionability_candidates = eligible[:actionability_candidate_limit]
    deferred_candidates = eligible[actionability_candidate_limit:]
    observations.extend(
        {
            **dict(item),
            "observation_state": "observed",
            "reason_codes": [*list(item.get("reason_codes") or []), "actionability_not_evaluated"],
        }
        for item in deferred_candidates
    )

    client = create_alpaca_client_from_env()
    issues: list[str] = []
    active_assets_by_symbol: dict[str, dict[str, Any]] = {}
    optionable_assets_by_symbol: dict[str, dict[str, Any]] = {}
    try:
        for asset in client.list_active_us_equity_assets():
            symbol = _normalize_symbol(asset.get("symbol"))
            if symbol is not None:
                active_assets_by_symbol[symbol] = dict(asset)
    except Exception as exc:
        issues.append("alpaca_assets_unavailable")
        for item in actionability_candidates:
            _record_filtered_earnings_observation(
                observations,
                item,
                "alpaca_asset_filter_unavailable",
                extra={"alpaca_error": str(exc)},
            )
        actionability_candidates = []

    if actionability_candidates:
        try:
            for asset in client.list_optionable_underlyings():
                symbol = _normalize_symbol(asset.get("symbol"))
                if symbol is not None:
                    optionable_assets_by_symbol[symbol] = dict(asset)
        except Exception as exc:
            issues.append("alpaca_optionable_unavailable")
            for item in actionability_candidates:
                _record_filtered_earnings_observation(
                    observations,
                    item,
                    "alpaca_optionable_filter_unavailable",
                    extra={"alpaca_error": str(exc)},
                )
            actionability_candidates = []

    asset_checked: list[dict[str, Any]] = []
    if actionability_candidates:
        for item in actionability_candidates:
            symbol = str(item.get("symbol") or "").upper()
            asset = active_assets_by_symbol.get(symbol)
            if asset is None or asset.get("tradable") is False:
                _record_filtered_earnings_observation(
                    observations,
                    item,
                    "alpaca_not_tradable",
                    extra={"alpaca_asset": asset},
                )
                continue
            optionable_asset = optionable_assets_by_symbol.get(symbol)
            if optionable_asset is None:
                _record_filtered_earnings_observation(
                    observations,
                    item,
                    "alpaca_not_optionable",
                    extra={"alpaca_asset": asset},
                )
                continue
            asset_checked.append(
                {
                    **dict(item),
                    "alpaca_asset": {
                        "id": asset.get("id"),
                        "asset_class": asset.get("class") or asset.get("asset_class"),
                        "exchange": asset.get("exchange"),
                        "name": asset.get("name"),
                        "status": asset.get("status"),
                        "tradable": asset.get("tradable"),
                        "marginable": asset.get("marginable"),
                        "shortable": asset.get("shortable"),
                        "easy_to_borrow": asset.get("easy_to_borrow"),
                    },
                    "alpaca_optionable": True,
                    "source_tags": [*list(item.get("source_tags") or []), "source:alpaca"],
                }
            )

    snapshots: dict[str, dict[str, Any]] = {}
    snapshot_symbols = [str(item.get("symbol")) for item in asset_checked if str(item.get("symbol") or "").strip()]
    if snapshot_symbols:
        try:
            snapshots = client.get_stock_snapshots(snapshot_symbols, feed=stock_feed)
        except Exception as exc:
            issues.append("alpaca_snapshots_unavailable")
            for item in asset_checked:
                _record_filtered_earnings_observation(
                    observations,
                    item,
                    "alpaca_snapshot_unavailable",
                    extra={"alpaca_error": str(exc), "stock_feed": stock_feed},
                )
            asset_checked = []

    price_volume_checked: list[dict[str, Any]] = []
    below_min_price_count = 0
    below_min_daily_volume_count = 0
    missing_snapshot_count = 0
    for item in asset_checked:
        symbol = str(item.get("symbol") or "").upper()
        snapshot = snapshots.get(symbol)
        if not isinstance(snapshot, Mapping):
            missing_snapshot_count += 1
            _record_filtered_earnings_observation(
                observations,
                item,
                "alpaca_snapshot_missing",
                extra={"stock_feed": stock_feed},
            )
            continue
        price = _stock_snapshot_price(snapshot)
        daily_volume = _stock_snapshot_daily_volume(snapshot)
        if price is None or price < min_price:
            below_min_price_count += 1
            _record_filtered_earnings_observation(
                observations,
                item,
                "below_min_price",
                extra={
                    "price": None if price is None else round(price, 4),
                    "min_price": min_price,
                    "daily_volume": daily_volume,
                    "stock_feed": stock_feed,
                },
            )
            continue
        if daily_volume < min_daily_volume:
            below_min_daily_volume_count += 1
            _record_filtered_earnings_observation(
                observations,
                item,
                "below_min_daily_volume",
                extra={
                    "price": round(price, 4),
                    "daily_volume": daily_volume,
                    "min_daily_volume": min_daily_volume,
                    "stock_feed": stock_feed,
                },
            )
            continue
        price_volume_checked.append(
            {
                **dict(item),
                "price": round(price, 4),
                "daily_volume": daily_volume,
                "move_percent": (
                    None
                    if _stock_snapshot_daily_percent_change(snapshot) is None
                    else round(float(_stock_snapshot_daily_percent_change(snapshot)), 4)
                ),
                "stock_feed": stock_feed,
            }
        )

    target_filter_reason_counts: dict[str, int] = {}
    passed: list[dict[str, Any]] = []
    for item in price_volume_checked:
        reason_codes = [*list(item.get("reason_codes") or []), "alpaca_tradable", "alpaca_optionable"]
        source_tags = list(item.get("source_tags") or [])
        target_filter_result: dict[str, Any] | None = None
        if target_option_filter.enabled:
            try:
                target_filter_result = _target_dte_option_filter_result(
                    client=client,
                    symbol=str(item["symbol"]),
                    config=target_option_filter,
                )
            except Exception as exc:
                target_filter_result = {
                    "status": "filtered_out",
                    "reason": "target_dte_option_filter_error",
                    "error": str(exc),
                    "min_dte": target_option_filter.min_dte,
                    "max_dte": target_option_filter.max_dte,
                    "feed": target_option_filter.feed,
                }
            source_tags.append("filter:target_dte_options")
            filter_status = str(target_filter_result.get("status") or "").strip().lower()
            if filter_status != "passed":
                reason = str(target_filter_result.get("reason") or "target_dte_option_filter_failed")
                target_filter_reason_counts[reason] = target_filter_reason_counts.get(reason, 0) + 1
                _record_filtered_earnings_observation(
                    observations,
                    item,
                    reason,
                    extra={
                        "reason_codes": [*reason_codes, reason],
                        "target_dte_option_filter": target_filter_result,
                    },
                )
                continue
            reason_codes.append("target_dte_options_available")
            if int(target_filter_result.get("expected_move_count") or 0) > 0:
                reason_codes.append("target_dte_expected_move_available")

        passed.append(
            {
                **dict(item),
                "observation_state": "observed",
                "reason_codes": reason_codes,
                "source_tags": source_tags,
                "target_dte_option_filter": target_filter_result,
                "expected_move_count": None if target_filter_result is None else target_filter_result.get("expected_move_count"),
                "expected_move_expirations": [] if target_filter_result is None else target_filter_result.get("expected_move_expirations", []),
            }
        )

    max_log_volume = max(
        [math.log1p(max(int(item.get("daily_volume") or 0), 0)) for item in passed],
        default=0.0,
    )
    for item in passed:
        days_to_event = item.get("days_to_event")
        normalized_days = int(days_to_event) if isinstance(days_to_event, int) else lookahead_days
        confidence_score = _confidence_rank(item.get("source_confidence")) * 18.0
        timing_score = _confidence_rank(item.get("timing_confidence")) * 8.0
        urgency_score = 28.0 * max(front_window_days - max(normalized_days, 0) + 1, 0) / float(front_window_days + 1)
        volume = max(int(item.get("daily_volume") or 0), 0)
        volume_score = 18.0 * math.log1p(volume) / max_log_volume if max_log_volume > 0.0 else 0.0
        expected_move_score = 12.0 if int(item.get("expected_move_count") or 0) > 0 else 0.0
        item["score"] = round(confidence_score + timing_score + urgency_score + volume_score + expected_move_score, 2)

    ranked = sorted(
        passed,
        key=lambda item: (
            -float(item.get("score") or 0.0),
            int(item.get("days_to_event") if isinstance(item.get("days_to_event"), int) else 1_000_000),
            str(item.get("symbol") or ""),
        ),
    )
    selected = [{**dict(item), "observation_state": "selected"} for item in ranked[:max_symbols]]
    selected_symbols = {str(item.get("symbol") or "") for item in selected}
    observations.extend(
        {**dict(item), "observation_state": "selected" if str(item.get("symbol") or "") in selected_symbols else "observed"}
        for item in ranked
    )
    symbols = [str(item.get("symbol")) for item in selected if str(item.get("symbol") or "").strip()]
    degradation_status = "ok" if symbols and not issues else "partial" if symbols else "empty"
    degradation_reason = None
    if not symbols:
        degradation_reason = "no_actionable_earnings_symbols"
    elif issues:
        degradation_reason = issues[0]
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
            "recipe": str(recipe),
            "window_start": window_start,
            "window_end": window_end,
            "lookahead_days": lookahead_days,
            "front_window_days": front_window_days,
            "min_source_confidence": min_source_confidence,
            "include_conflicts": include_conflicts,
            "min_price": min_price,
            "min_daily_volume": min_daily_volume,
            "max_symbols": max_symbols,
            "actionability_candidate_limit": actionability_candidate_limit,
            "consensus_count": len(records),
            "eligible_count": len(eligible),
            "actionability_evaluated_count": len(actionability_candidates),
            "deferred_count": len(deferred_candidates),
            "tradable_optionable_count": len(asset_checked),
            "price_volume_passed_count": len(price_volume_checked),
            "target_dte_passed_count": len(passed),
            "stale_count": stale_count,
            "conflict_count": conflict_count,
            "below_min_source_confidence_count": below_confidence_count,
            "below_min_price_count": below_min_price_count,
            "below_min_daily_volume_count": below_min_daily_volume_count,
            "missing_snapshot_count": missing_snapshot_count,
            "issues": issues,
            "target_dte_option_filter": {
                "enabled": target_option_filter.enabled,
                "min_dte": target_option_filter.min_dte,
                "max_dte": target_option_filter.max_dte,
                "feed": target_option_filter.feed,
                "stock_feed": target_option_filter.stock_feed,
                "require_expected_move": target_option_filter.require_expected_move,
                "min_expected_move_count": target_option_filter.min_expected_move_count,
                "filtered_count": sum(target_filter_reason_counts.values()),
                "reason_counts": dict(sorted(target_filter_reason_counts.items())),
            },
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
    client = (
        FINVIZ_HTTP
        if timeout_seconds == 30
        else VendorHttpClient(timeout_seconds=timeout_seconds, user_agent="spreads-finviz-feed/1.0", follow_redirects=True)
    )
    return client.request_text("GET", source_value, "", headers=headers).lstrip("\ufeff")


def _finviz_source_config(
    recipe_args: Mapping[str, Any],
) -> tuple[str | None, str | None]:
    source = str(recipe_args.get("source") or "auto").strip().lower()
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
    source_symbol_limit = _as_positive_int_or_none(recipe_args.get("source_symbol_limit"))
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
    target_option_filter = _target_dte_option_filter_config(recipe_args)
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
        symbol = _normalize_symbol(pick(row, "ticker", "symbol"))
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
    if normalized_recipe == "earnings_event_window":
        return tuple(
            _run_earnings_event_window_feed(
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
    if normalized_recipe == "earnings_event_window":
        return _run_earnings_event_window_feed(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
        )
    raise ValueError(f"Unsupported ticker source recipe: {recipe}")


def _ticker_source_run_id(*, source_id: str, job_run_id: str | None, generated_at: str | None) -> str:
    if job_run_id not in (None, ""):
        return f"ticker_source_run:{job_run_id}"
    generated_key = str(generated_at or _iso_now()).replace(":", "").replace("-", "")
    return f"ticker_source_run:{source_id}:{generated_key}"


def persist_ticker_source_result(
    engine_facts: Any,
    *,
    source_id: str,
    recipe: str,
    job_run_id: str | None,
    result: Mapping[str, Any],
    config_hash: str | None = None,
) -> dict[str, Any]:
    if engine_facts is None or not engine_facts.schema_ready():
        return {
            "status": "skipped",
            "reason": "engine_fact_schema_unavailable",
        }
    generated_at = _as_optional_text(result.get("generated_at")) or _iso_now()
    ticker_source_run_id = _ticker_source_run_id(
        source_id=str(source_id),
        job_run_id=job_run_id,
        generated_at=generated_at,
    )
    row = engine_facts.upsert_ticker_source_run(
        ticker_source_run_id=ticker_source_run_id,
        ticker_source_type=str(recipe),
        ticker_source_id=str(source_id),
        job_run_id=job_run_id,
        status=str(result.get("status") or "completed"),
        config_hash=config_hash,
        generated_at=generated_at,
        completed_at=_iso_now(),
        symbols=[str(symbol).upper() for symbol in list(result.get("symbols") or []) if str(symbol or "").strip()],
        entries=[dict(item) for item in list(result.get("entries") or []) if isinstance(item, Mapping)],
        observations=[dict(item) for item in list(result.get("observations") or []) if isinstance(item, Mapping)],
        summary=dict(result.get("summary") or {}),
        evidence={
            "source_id": str(source_id),
            "recipe": str(recipe),
            "degradation": dict(result.get("degradation") or {}),
        },
        updated_at=_iso_now(),
    )
    return {
        "status": "ok",
        "ticker_source_run_id": row.get("ticker_source_run_id"),
        "observed_count": row.get("observed_count"),
        "selected_count": row.get("selected_count"),
        "excluded_count": row.get("excluded_count"),
    }


def get_latest_ticker_source_snapshot(
    engine_facts: Any,
    *,
    source_id: str,
    job_key: str | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    if engine_facts is None or not engine_facts.schema_ready():
        return {
            "status": "missing",
            "source_id": str(source_id),
            "job_key": None if job_key is None else str(job_key),
            "symbols": [],
            "entries": [],
            "summary": {},
            "degradation": {
                "status": "missing",
                "reason": "engine_fact_schema_unavailable",
            },
            "ticker_source_run_id": None,
            "generated_at": None,
            "age_seconds": None,
        }
    snapshot = engine_facts.get_latest_ticker_source_snapshot(
        ticker_source_id=str(source_id),
        max_age_seconds=max_age_seconds,
    )
    return {
        "status": str(snapshot.get("status") or "missing"),
        "source_id": str(source_id),
        "job_key": None if job_key is None else str(job_key),
        "ticker_source_run_id": snapshot.get("ticker_source_run_id"),
        "ticker_source_type": snapshot.get("ticker_source_type"),
        "job_run_id": snapshot.get("job_run_id"),
        "generated_at": snapshot.get("generated_at"),
        "age_seconds": snapshot.get("age_seconds"),
        "symbols": list(snapshot.get("symbols") or []),
        "entries": [dict(item) for item in list(snapshot.get("entries") or []) if isinstance(item, Mapping)],
        "summary": dict(snapshot.get("summary") or {}),
        "degradation": dict(snapshot.get("degradation") or {}),
    }


def resolve_ticker_source_symbols(
    engine_facts: Any,
    *,
    source_id: str,
    job_key: str,
    max_age_seconds: int | None = None,
    fallback_universe_ref: str | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    snapshot = get_latest_ticker_source_snapshot(
        engine_facts,
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
            "ticker_source_run_id": snapshot.get("ticker_source_run_id"),
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
        "ticker_source_run_id": snapshot.get("ticker_source_run_id"),
        "entries": [dict(item) for item in list(snapshot.get("entries") or []) if isinstance(item, Mapping)],
        "summary": dict(snapshot.get("summary") or {}),
        "degradation": dict(snapshot.get("degradation") or {}),
        "source_snapshot": snapshot,
    }


__all__ = [
    "VALID_TICKER_SOURCE_RECIPES",
    "build_ticker_source_symbols",
    "get_latest_ticker_source_snapshot",
    "persist_ticker_source_result",
    "resolve_ticker_source_symbols",
    "run_ticker_source",
]

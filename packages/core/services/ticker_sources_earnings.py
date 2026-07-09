from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
import math
from typing import Any

from core.integrations.calendar_events.models import EarningsEventConsensusRecord
from core.integrations.calendar_events.store import CalendarEventStore
from core.runtime.config import default_database_url
from core.services.alpaca import create_alpaca_client_from_env
from core.services.market_dates import NEW_YORK
from core.services.ticker_sources import (
    EarningsEventWindowRecipeArgs,
    _iso_now,
    _stock_snapshot_daily_percent_change,
    _stock_snapshot_daily_volume,
    _stock_snapshot_price,
    _target_dte_option_filter_result,
)
from core.storage.serializers import parse_date as _parse_date, parse_datetime as _parse_datetime
from core.value_coercion import normalize_symbol

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

def _dedupe_earnings_records_by_symbol(records: list[EarningsEventConsensusRecord]) -> list[EarningsEventConsensusRecord]:
    deduped: dict[str, EarningsEventConsensusRecord] = {}

    def sort_key(record: EarningsEventConsensusRecord) -> tuple[str, str]:
        return (str(record.event_date or ""), str(record.symbol or ""))

    for record in sorted(records, key=sort_key):
        symbol = normalize_symbol(record.symbol)
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
    args = EarningsEventWindowRecipeArgs.model_validate(recipe_args)
    generated_at = _iso_now()
    now = datetime.now(UTC)
    now_date = datetime.now(NEW_YORK).date()
    lookahead_days = args.lookahead_days
    front_window_days = args.front_window_days
    min_source_confidence = args.min_source_confidence
    include_conflicts = args.include_conflicts
    min_price = args.min_price
    min_daily_volume = args.min_daily_volume
    max_symbols = args.max_symbols
    actionability_candidate_limit = max(args.actionability_candidate_limit or max(max_symbols * 4, 50), max_symbols)
    stock_feed = args.stock_feed
    target_option_filter = args.target_option_filter
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
            symbol = normalize_symbol(asset.get("symbol"))
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
                symbol = normalize_symbol(asset.get("symbol"))
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

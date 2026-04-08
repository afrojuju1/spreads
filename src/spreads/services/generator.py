from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any

from spreads.common import env_or_die, load_local_env
from spreads.domain.profiles import UNIVERSE_PRESETS
from spreads.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from spreads.integrations.calendar_events import build_calendar_event_resolver
from spreads.integrations.greeks import build_local_greeks_provider
from spreads.services.scanner import (
    SpreadCandidate,
    UnderlyingSetupContext,
    build_filter_payload,
    merge_strategy_candidates,
    parse_args as parse_scanner_args,
    scan_symbol_across_strategies,
)
from spreads.storage import build_history_store


OPTIONABLE_SYMBOL_CACHE_TTL = timedelta(minutes=15)
_optionable_symbol_cache: dict[str, Any] = {"loaded_at": None, "assets": None}
_optionable_symbol_cache_lock = Lock()
GENERATOR_JOB_CHANNEL_PREFIX = "generator-job:"


def generator_job_channel(generator_job_id: str) -> str:
    return f"{GENERATOR_JOB_CHANNEL_PREFIX}{generator_job_id}"


def build_generator_args(overrides: dict[str, Any] | None = None) -> argparse.Namespace:
    args = parse_scanner_args([])
    args.symbol = "SPY"
    args.symbols = None
    args.symbols_file = None
    args.universe = None
    args.session_label = "generator"
    args.output = None
    args.output_format = "json"
    args.json = False
    args.show_order_json = False
    args.stream_live_quotes = False
    for key, value in (overrides or {}).items():
        setattr(args, key, value)
    return args


def _create_alpaca_client(*, data_base_url: str | None = None, trading_base_url: str | None = None) -> AlpacaClient:
    load_local_env()
    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    generator_args = build_generator_args()
    resolved_data_base_url = data_base_url or generator_args.data_base_url
    resolved_trading_base_url = infer_trading_base_url(
        key_id,
        trading_base_url or generator_args.trading_base_url,
    )
    return AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=resolved_trading_base_url,
        data_base_url=resolved_data_base_url,
    )


def _load_optionable_underlyings() -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    with _optionable_symbol_cache_lock:
        cached_at = _optionable_symbol_cache["loaded_at"]
        cached_assets = _optionable_symbol_cache["assets"]
        if (
            isinstance(cached_at, datetime)
            and isinstance(cached_assets, list)
            and now - cached_at < OPTIONABLE_SYMBOL_CACHE_TTL
        ):
            return cached_assets

    client = _create_alpaca_client()
    assets = client.list_optionable_underlyings()
    normalized_assets = [
        {
            "symbol": str(item.get("symbol") or "").upper(),
            "name": str(item.get("name") or "").strip() or None,
        }
        for item in assets
        if str(item.get("symbol") or "").strip()
    ]
    normalized_assets.sort(key=lambda item: item["symbol"])

    with _optionable_symbol_cache_lock:
        _optionable_symbol_cache["loaded_at"] = now
        _optionable_symbol_cache["assets"] = normalized_assets
    return normalized_assets


def list_generator_symbol_suggestions(
    *,
    query: str = "",
    limit: int = 40,
) -> dict[str, Any]:
    normalized_query = query.strip().upper()
    curated_symbols = sorted({symbol for symbols in UNIVERSE_PRESETS.values() for symbol in symbols})

    def rank_asset(asset: dict[str, Any]) -> tuple[int, int, str]:
        symbol = str(asset.get("symbol") or "").upper()
        name = str(asset.get("name") or "").upper()
        if not normalized_query:
            return (0 if symbol in curated_symbols else 1, 0, symbol)
        if symbol == normalized_query:
            return (0, 0, symbol)
        if symbol.startswith(normalized_query):
            return (0 if symbol in curated_symbols else 1, 1, symbol)
        if normalized_query in symbol:
            return (0 if symbol in curated_symbols else 1, 2, symbol)
        if normalized_query and normalized_query in name:
            return (0 if symbol in curated_symbols else 1, 3, symbol)
        return (9, 9, symbol)

    try:
        assets = _load_optionable_underlyings()
        source_status = "alpaca"
    except Exception:
        assets = [{"symbol": symbol, "name": None} for symbol in curated_symbols]
        source_status = "fallback"

    filtered_assets = [
        {
            "symbol": str(asset["symbol"]).upper(),
            "name": asset.get("name"),
            "in_curated_universe": str(asset["symbol"]).upper() in curated_symbols,
        }
        for asset in assets
        if rank_asset(asset)[0] < 9
    ]
    filtered_assets.sort(key=rank_asset)
    return {
        "query": normalized_query,
        "source_status": source_status,
        "symbols": filtered_assets[:limit],
    }


def summarize_setup(setup: UnderlyingSetupContext | None) -> dict[str, Any] | None:
    if setup is None:
        return None
    return {
        "status": setup.status,
        "score": setup.score,
        "daily_score": setup.daily_score,
        "intraday_score": setup.intraday_score,
        "reasons": list(setup.reasons),
        "spot_vs_vwap_pct": setup.spot_vs_vwap_pct,
        "opening_range_break_pct": setup.opening_range_break_pct,
        "distance_to_session_extreme_pct": setup.distance_to_session_extreme_pct,
    }


def summarize_candidate(candidate: SpreadCandidate) -> dict[str, Any]:
    payload = asdict(candidate)
    payload["setup_reasons"] = list(candidate.setup_reasons)
    payload["calendar_reasons"] = list(candidate.calendar_reasons)
    payload["board_notes"] = list(candidate.board_notes)
    payload["data_reasons"] = list(candidate.data_reasons)
    payload["calendar_sources"] = list(candidate.calendar_sources)
    return payload


def build_no_play_reasons(
    *,
    strategy: str,
    setup: UnderlyingSetupContext | None,
    quoted_contract_count: int,
    alpaca_delta_contract_count: int,
    delta_contract_count: int,
    local_delta_contract_count: int,
    candidate_count: int,
    failures: list[dict[str, str]],
) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    if failures:
        for failure in failures:
            reasons.append(
                {
                    "code": "scan_error",
                    "message": failure["error"],
                    "strategy": failure["strategy"],
                    "severity": "high",
                }
            )

    if setup is not None and setup.status in {"unfavorable", "neutral"}:
        reasons.append(
            {
                "code": "setup_not_supportive",
                "message": f"{strategy} setup is {setup.status}.",
                "strategy": strategy,
                "severity": "medium" if setup.status == "neutral" else "high",
                "details": {"score": setup.score, "reasons": list(setup.reasons[:4])},
            }
        )

    if quoted_contract_count == 0:
        reasons.append(
            {
                "code": "no_quoted_contracts",
                "message": "No quoted option contracts were available in the requested expiry window.",
                "strategy": strategy,
                "severity": "high",
            }
        )
        return reasons

    if delta_contract_count == 0:
        if alpaca_delta_contract_count == 0 and local_delta_contract_count == 0:
            reasons.append(
                {
                    "code": "no_usable_greeks",
                    "message": "Quoted contracts were available, but no usable delta/Greeks were available after enrichment.",
                    "strategy": strategy,
                    "severity": "high",
                    "details": {
                        "quoted_contract_count": quoted_contract_count,
                        "alpaca_delta_contract_count": alpaca_delta_contract_count,
                        "local_delta_contract_count": local_delta_contract_count,
                    },
                }
            )
        else:
            reasons.append(
                {
                    "code": "no_delta_qualified_contracts",
                    "message": "Contracts had quotes and some Greeks, but none survived the delta targeting logic.",
                    "strategy": strategy,
                    "severity": "medium",
                    "details": {
                        "quoted_contract_count": quoted_contract_count,
                        "alpaca_delta_contract_count": alpaca_delta_contract_count,
                        "delta_contract_count": delta_contract_count,
                    },
                }
            )

    if candidate_count == 0 and quoted_contract_count > 0 and delta_contract_count > 0:
        reasons.append(
            {
                "code": "all_candidates_filtered",
                "message": "Spreads were evaluated, but none survived liquidity, credit, width, or quality thresholds.",
                "strategy": strategy,
                "severity": "medium",
                "details": {
                    "quoted_contract_count": quoted_contract_count,
                    "delta_contract_count": delta_contract_count,
                },
            }
        )

    if not reasons and candidate_count == 0:
        reasons.append(
            {
                "code": "no_viable_spread",
                "message": "No spread matched the requested profile and filter set.",
                "strategy": strategy,
                "severity": "medium",
            }
        )
    return reasons


def generate_symbol_ideas(args: argparse.Namespace) -> dict[str, Any]:
    load_local_env()
    if not args.symbol:
        raise ValueError("symbol is required")

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")

    client = _create_alpaca_client(
        data_base_url=args.data_base_url,
        trading_base_url=args.trading_base_url,
    )
    history_store = build_history_store(args.history_db)
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=args.data_base_url,
        database_url=args.history_db,
    )
    greeks_provider = build_local_greeks_provider()

    try:
        results, failures = scan_symbol_across_strategies(
            symbol=str(args.symbol).upper(),
            base_args=args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
    finally:
        history_store.close()
        calendar_resolver.store.close()

    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    merged_candidates = merge_strategy_candidates(results)[: args.top]
    failures_payload = []
    for failure in failures:
        label = failure.symbol
        strategy = label.split(":", 1)[1] if ":" in label else str(args.strategy)
        failures_payload.append({"strategy": strategy, "error": failure.error})

    strategy_runs: list[dict[str, Any]] = []
    no_play_reasons: list[dict[str, Any]] = []

    for result in results:
        strategy_runs.append(
            {
                "strategy": result.args.strategy,
                "run_id": result.run_id,
                "setup": summarize_setup(result.setup),
                "candidate_count": len(result.candidates),
                "quoted_contract_count": result.quoted_contract_count,
                "alpaca_delta_contract_count": result.alpaca_delta_contract_count,
                "delta_contract_count": result.delta_contract_count,
                "local_delta_contract_count": result.local_delta_contract_count,
                "top_candidate": None
                if not result.candidates
                else summarize_candidate(result.candidates[0]),
                "no_play_reasons": build_no_play_reasons(
                    strategy=result.args.strategy,
                    setup=result.setup,
                    quoted_contract_count=result.quoted_contract_count,
                    alpaca_delta_contract_count=result.alpaca_delta_contract_count,
                    delta_contract_count=result.delta_contract_count,
                    local_delta_contract_count=result.local_delta_contract_count,
                    candidate_count=len(result.candidates),
                    failures=[item for item in failures_payload if item["strategy"] == result.args.strategy],
                ),
            }
        )
        no_play_reasons.extend(strategy_runs[-1]["no_play_reasons"])

    if not results and failures_payload:
        no_play_reasons.extend(
            {
                "code": "scan_error",
                "message": item["error"],
                "strategy": item["strategy"],
                "severity": "high",
            }
            for item in failures_payload
        )

    deduped_reasons: list[dict[str, Any]] = []
    seen = set()
    for reason in no_play_reasons:
        key = (reason.get("strategy"), reason.get("code"), reason.get("message"))
        if key in seen:
            continue
        seen.add(key)
        deduped_reasons.append(reason)

    status = "ok" if merged_candidates else "no_play"
    preferred = None if not merged_candidates else summarize_candidate(merged_candidates[0])
    return {
        "status": status,
        "generated_at": generated_at,
        "symbol": str(args.symbol).upper(),
        "profile": args.profile,
        "strategy": args.strategy,
        "greeks_source": args.greeks_source,
        "filters": build_filter_payload(args),
        "preferred_play": preferred,
        "top_candidates": [summarize_candidate(candidate) for candidate in merged_candidates],
        "strategy_runs": strategy_runs,
        "rejection_summary": deduped_reasons,
        "failures": failures_payload,
        "request": {
            "symbol": str(args.symbol).upper(),
            "profile": args.profile,
            "strategy": args.strategy,
            "greeks_source": args.greeks_source,
            "top": args.top,
        },
    }

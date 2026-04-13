from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any, Mapping

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
from spreads.storage.factory import build_history_store


OPTIONABLE_SYMBOL_CACHE_TTL = timedelta(minutes=15)
_optionable_symbol_cache: dict[str, Any] = {"loaded_at": None, "assets": None}
_optionable_symbol_cache_lock = Lock()
GENERATOR_JOB_CHANNEL_PREFIX = "generator-job:"
DIAGNOSTIC_BUCKET_ORDER = [
    "market_data",
    "greeks",
    "setup",
    "filtering",
    "candidate_quality",
]
REASON_BUCKET_BY_CODE = {
    "scan_error": "market_data",
    "no_quoted_contracts": "market_data",
    "no_usable_greeks": "greeks",
    "no_delta_qualified_contracts": "greeks",
    "setup_not_supportive": "setup",
    "all_candidates_filtered": "filtering",
    "no_viable_spread": "candidate_quality",
}


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
    payload["selection_notes"] = list(candidate.selection_notes)
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


def diagnostic_bucket_for_reason(reason: Mapping[str, Any]) -> str:
    return REASON_BUCKET_BY_CODE.get(str(reason.get("code") or ""), "candidate_quality")


def build_strategy_comparison(strategy_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []
    for run in strategy_runs:
        setup = run.get("setup") or {}
        reasons = list(run.get("no_play_reasons") or [])
        comparisons.append(
            {
                "strategy": run["strategy"],
                "run_id": run["run_id"],
                "setup_status": setup.get("status"),
                "candidate_count": int(run.get("candidate_count") or 0),
                "quoted_contract_count": int(run.get("quoted_contract_count") or 0),
                "alpaca_delta_contract_count": int(run.get("alpaca_delta_contract_count") or 0),
                "delta_contract_count": int(run.get("delta_contract_count") or 0),
                "local_delta_contract_count": int(run.get("local_delta_contract_count") or 0),
                "blocker_codes": [str(reason.get("code") or "") for reason in reasons],
                "blocker_summary": [
                    {
                        "code": str(reason.get("code") or ""),
                        "message": str(reason.get("message") or ""),
                        "severity": str(reason.get("severity") or "info"),
                    }
                    for reason in reasons
                ],
            }
        )
    return comparisons


def build_preferred_play_explanation(
    candidates: list[SpreadCandidate],
) -> dict[str, Any] | None:
    if not candidates:
        return None
    winner = candidates[0]
    runner_up = candidates[1] if len(candidates) > 1 else None
    score_margin = None
    if runner_up is not None:
        score_margin = winner.quality_score - runner_up.quality_score
    parts = [
        f"{winner.strategy} {winner.short_strike:.2f}/{winner.long_strike:.2f} ranked first",
        f"with the top quality score of {winner.quality_score:.1f}",
    ]
    if score_margin is not None:
        parts.append(f"and a {score_margin:.1f}-point edge over the next candidate")
    if winner.setup_status:
        parts.append(f"while setup was {winner.setup_status}")
    if winner.calendar_status:
        parts.append(f"and calendar status was {winner.calendar_status}")
    return {
        "summary": ", ".join(parts) + ".",
        "strategy": winner.strategy,
        "short_symbol": winner.short_symbol,
        "long_symbol": winner.long_symbol,
        "short_strike": winner.short_strike,
        "long_strike": winner.long_strike,
        "quality_score": winner.quality_score,
        "score_margin_vs_runner_up": score_margin,
        "setup_status": winner.setup_status,
        "calendar_status": winner.calendar_status,
        "midpoint_credit": winner.midpoint_credit,
        "return_on_risk": winner.return_on_risk,
    }


def build_diagnostic_recommendations(
    *,
    args: argparse.Namespace,
    reasons: list[dict[str, Any]],
    strategy_runs: list[dict[str, Any]],
    merged_candidates: list[SpreadCandidate],
) -> list[dict[str, Any]]:
    if merged_candidates:
        return [
            {
                "code": "play_available",
                "title": "A playable spread is available now",
                "action": "Use the preferred play and review the top-ranked alternatives only if execution quality changes.",
                "reason": "The generator already found at least one candidate that survived the requested filters.",
                "priority": "info",
            }
        ]

    recommendations: list[dict[str, Any]] = []
    reason_codes = {str(reason.get("code") or "") for reason in reasons}

    if "no_quoted_contracts" in reason_codes:
        recommendations.append(
            {
                "code": "switch_symbol_or_wait",
                "title": "No tradable contracts were available",
                "action": "Wait for better market data or try a different symbol with healthier intraday option quotes.",
                "reason": "No quoted contracts survived in the requested expiry window.",
                "priority": "high",
            }
        )

    if "no_usable_greeks" in reason_codes and args.greeks_source != "local":
        recommendations.append(
            {
                "code": "switch_to_local_greeks",
                "title": "Use local Greeks enrichment",
                "action": "Retry with `greeks_source=local` or `auto` if Alpaca Greeks are sparse.",
                "reason": "Quoted contracts existed, but no usable Greeks survived enrichment.",
                "priority": "high",
            }
        )

    if "no_delta_qualified_contracts" in reason_codes and args.profile == "0dte":
        recommendations.append(
            {
                "code": "widen_profile_to_weekly",
                "title": "Widen the profile window",
                "action": "Retry the same symbol with the `weekly` profile to broaden the candidate set.",
                "reason": "The same-day delta window was too narrow for the available contracts.",
                "priority": "medium",
            }
        )

    if "all_candidates_filtered" in reason_codes:
        action = (
            "Retry with a lower minimum credit."
            if getattr(args, "min_credit", None)
            else "Relax the most restrictive filter, starting with credit or delta targeting."
        )
        recommendations.append(
            {
                "code": "relax_candidate_filters",
                "title": "Relax candidate thresholds",
                "action": action,
                "reason": "Contracts and Greeks existed, but every spread failed downstream filters.",
                "priority": "medium",
            }
        )

    if "setup_not_supportive" in reason_codes:
        recommendations.append(
            {
                "code": "wait_for_better_setup",
                "title": "Wait for a more supportive setup",
                "action": "Hold the symbol until the setup becomes favorable instead of forcing a trade now.",
                "reason": "The underlying setup was neutral or unfavorable for the requested side.",
                "priority": "medium",
            }
        )

    if not recommendations:
        recommendations.append(
            {
                "code": "no_actionable_change",
                "title": "No bounded parameter change stood out",
                "action": "Review another symbol or rerun later with the same settings.",
                "reason": "No deterministic adjustment clearly improves this request.",
                "priority": "low",
            }
        )
    return recommendations[:4]


def build_generator_diagnostics(
    *,
    args: argparse.Namespace,
    reasons: list[dict[str, Any]],
    strategy_runs: list[dict[str, Any]],
    merged_candidates: list[SpreadCandidate],
) -> dict[str, Any]:
    grouped: list[dict[str, Any]] = []
    for bucket in DIAGNOSTIC_BUCKET_ORDER:
        bucket_reasons = [reason for reason in reasons if diagnostic_bucket_for_reason(reason) == bucket]
        if not bucket_reasons:
            continue
        grouped.append(
            {
                "bucket": bucket,
                "reason_count": len(bucket_reasons),
                "reasons": bucket_reasons,
            }
        )

    status = "ok" if merged_candidates else "no_play"
    if merged_candidates:
        playability_verdict = "play_available"
    elif any(reason.get("code") == "no_quoted_contracts" for reason in reasons):
        playability_verdict = "blocked_by_market_data"
    elif any(reason.get("code") == "no_usable_greeks" for reason in reasons):
        playability_verdict = "blocked_by_greeks"
    elif any(reason.get("code") == "setup_not_supportive" for reason in reasons):
        playability_verdict = "blocked_by_setup"
    elif any(reason.get("code") == "all_candidates_filtered" for reason in reasons):
        playability_verdict = "filtered_out"
    else:
        playability_verdict = "no_viable_spread"

    return {
        "overview": {
            "status": status,
            "symbol": str(args.symbol).upper(),
            "profile": args.profile,
            "strategy": args.strategy,
            "playability_verdict": playability_verdict,
        },
        "groups": grouped,
        "recommendations": build_diagnostic_recommendations(
            args=args,
            reasons=reasons,
            strategy_runs=strategy_runs,
            merged_candidates=merged_candidates,
        ),
        "strategy_comparison": build_strategy_comparison(strategy_runs),
        "preferred_play_explanation": build_preferred_play_explanation(merged_candidates),
    }


def generator_result_summary(result: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(result or {})
    preferred = payload.get("preferred_play") or {}
    top_candidates = list(payload.get("top_candidates") or [])
    rejection_summary = list(payload.get("rejection_summary") or [])
    preferred_strikes = None
    if preferred.get("short_strike") is not None and preferred.get("long_strike") is not None:
        preferred_strikes = f"{float(preferred['short_strike']):.2f} / {float(preferred['long_strike']):.2f}"
    return {
        "preferred_strategy": preferred.get("strategy"),
        "preferred_strikes": preferred_strikes,
        "top_score": preferred.get("quality_score"),
        "candidate_count": len(top_candidates),
        "rejection_count": len(rejection_summary),
    }


def build_generator_job_payload(
    job_run: Mapping[str, Any],
    *,
    include_result: bool = True,
) -> dict[str, Any]:
    payload = dict(job_run)
    request = dict(payload.get("payload") or {})
    result = payload.get("result")
    if not include_result:
        result = None
    resolved_result = payload.get("result") if isinstance(payload.get("result"), Mapping) else None
    return {
        "generator_job_id": str(payload["job_run_id"]),
        "job_run_id": str(payload["job_run_id"]),
        "job_key": payload.get("job_key"),
        "job_type": payload.get("job_type"),
        "arq_job_id": payload.get("arq_job_id"),
        "symbol": str(request.get("symbol") or ""),
        "status": str(payload.get("status") or "queued"),
        "created_at": payload.get("scheduled_for"),
        "scheduled_for": payload.get("scheduled_for"),
        "started_at": payload.get("started_at"),
        "finished_at": payload.get("finished_at"),
        "request": request,
        "result": result,
        "summary": generator_result_summary(resolved_result),
        "error_text": payload.get("error_text"),
    }


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
    diagnostics = build_generator_diagnostics(
        args=args,
        reasons=deduped_reasons,
        strategy_runs=strategy_runs,
        merged_candidates=merged_candidates,
    )
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
        "diagnostics": {
            "overview": diagnostics["overview"],
            "groups": diagnostics["groups"],
        },
        "strategy_comparison": diagnostics["strategy_comparison"],
        "preferred_play_explanation": diagnostics["preferred_play_explanation"],
        "recommendations": diagnostics["recommendations"],
        "failures": failures_payload,
        "request": {
            "symbol": str(args.symbol).upper(),
            "profile": args.profile,
            "strategy": args.strategy,
            "greeks_source": args.greeks_source,
            "top": args.top,
        },
    }

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.discovery_recovery import build_capture_target_rows_for_candidates
from core.services.option_structures import normalize_strategy_family, payload_structure_identity
from core.services.trading_strategy_runtime import EntryRuntime

CAPTURE_OWNER_TRADING_STRATEGY = "trading_strategy"
CAPTURE_TARGET_REASON_STRATEGY_WARM = "strategy_warm"
CAPTURE_TARGET_REASON_STRATEGY_HOT = "strategy_hot"
HOT_TARGET_THRESHOLD = 70.0
WARM_TTL_SECONDS = 300
HOT_DISCOVERY_TTL_SECONDS = 90


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _ttl_iso(seconds: int) -> str:
    return (_utc_now() + timedelta(seconds=max(int(seconds), 1))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _score(opportunity: Mapping[str, Any]) -> float:
    for key in ("execution_score", "promotion_score"):
        value = opportunity.get(key)
        try:
            if value not in (None, ""):
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _candidate_payload(opportunity: Mapping[str, Any]) -> dict[str, Any] | None:
    candidate = opportunity.get("candidate")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    candidate_json = opportunity.get("candidate_json")
    if isinstance(candidate_json, Mapping):
        return dict(candidate_json)
    return None


def _runtime_owner_key(runtime: EntryRuntime) -> str:
    return runtime.trading_strategy_id


def _matching_candidates(
    *,
    opportunities: list[dict[str, Any]],
    runtime: EntryRuntime,
) -> list[dict[str, Any]]:
    trade_structure = runtime.trade_structure
    symbols = set(runtime.symbols)
    filtered: list[dict[str, Any]] = []
    for opportunity in opportunities:
        opportunity_strategy_id = str(opportunity.get("trading_strategy_id") or "")
        if opportunity_strategy_id and opportunity_strategy_id != runtime.trading_strategy_id:
            continue
        underlying_symbol = str(opportunity.get("underlying_symbol") or "").upper()
        if symbols and underlying_symbol not in symbols:
            continue
        if normalize_strategy_family(opportunity.get("strategy_family")) != trade_structure:
            continue
        candidate = _candidate_payload(opportunity)
        if candidate is None:
            continue
        filtered.append({**candidate, "execution_score": _score(opportunity)})
    filtered.sort(
        key=lambda item: (
            -float(item.get("execution_score") or 0.0),
            str(item.get("underlying_symbol") or ""),
            str(payload_structure_identity(item, strategy=item.get("strategy")) or ""),
        )
    )
    return filtered


def refresh_trading_strategy_capture_targets(
    *,
    recovery_store: Any,
    session_id: str,
    session_date: str,
    entry_runtimes: list[EntryRuntime],
    opportunities: list[dict[str, Any]],
    label: str | None = None,
    data_base_url: str | None = None,
    feed: str = "opra",
) -> dict[str, Any]:
    if not recovery_store.schema_ready():
        return {"status": "skipped", "reason": "recovery_schema_unavailable"}

    active_owner_keys: list[str] = []
    summary: list[dict[str, Any]] = []
    capture_targets: dict[str, list[dict[str, Any]]] = {
        CAPTURE_TARGET_REASON_STRATEGY_WARM: [],
        CAPTURE_TARGET_REASON_STRATEGY_HOT: [],
    }
    for runtime in entry_runtimes:
        owner_key = _runtime_owner_key(runtime)
        active_owner_keys.append(owner_key)
        candidates = _matching_candidates(opportunities=opportunities, runtime=runtime)
        warm_candidates = candidates[:6]
        hot_threshold = float(runtime.trigger_policy.get("min_opportunity_score") or HOT_TARGET_THRESHOLD)
        hot_candidates = [candidate for candidate in candidates if float(candidate.get("execution_score") or 0.0) >= hot_threshold][:2]

        warm_rows = build_capture_target_rows_for_candidates(
            candidates=warm_candidates,
            feed=feed,
            data_base_url=data_base_url,
            expires_at=_ttl_iso(WARM_TTL_SECONDS),
        )
        hot_rows = build_capture_target_rows_for_candidates(
            candidates=hot_candidates,
            feed=feed,
            data_base_url=data_base_url,
            expires_at=_ttl_iso(HOT_DISCOVERY_TTL_SECONDS),
        )
        recovery_store.replace_capture_targets(
            owner_kind=CAPTURE_OWNER_TRADING_STRATEGY,
            owner_key=owner_key,
            reason=CAPTURE_TARGET_REASON_STRATEGY_WARM,
            session_id=session_id,
            session_date=session_date,
            label=label,
            rows=warm_rows,
        )
        recovery_store.replace_capture_targets(
            owner_kind=CAPTURE_OWNER_TRADING_STRATEGY,
            owner_key=owner_key,
            reason=CAPTURE_TARGET_REASON_STRATEGY_HOT,
            session_id=session_id,
            session_date=session_date,
            label=label,
            rows=hot_rows,
        )
        summary.append(
            {
                "trading_strategy_id": runtime.trading_strategy_id,
                "warm_target_count": len(warm_rows),
                "hot_target_count": len(hot_rows),
            }
        )
        capture_targets[CAPTURE_TARGET_REASON_STRATEGY_WARM].extend(warm_rows)
        capture_targets[CAPTURE_TARGET_REASON_STRATEGY_HOT].extend(hot_rows)

    recovery_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_TRADING_STRATEGY,
        active_owner_keys=active_owner_keys,
        reason=CAPTURE_TARGET_REASON_STRATEGY_WARM,
    )
    recovery_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_TRADING_STRATEGY,
        active_owner_keys=active_owner_keys,
        reason=CAPTURE_TARGET_REASON_STRATEGY_HOT,
    )
    return {
        "status": "ok",
        "targets": summary,
        "capture_targets": capture_targets,
    }


__all__ = [
    "CAPTURE_OWNER_TRADING_STRATEGY",
    "CAPTURE_TARGET_REASON_STRATEGY_HOT",
    "CAPTURE_TARGET_REASON_STRATEGY_WARM",
    "refresh_trading_strategy_capture_targets",
]

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.automations import ResolvedAutomation
from core.services.bots import ResolvedBot
from core.services.live_recovery import build_capture_target_rows_for_candidates
from core.services.option_structures import normalize_strategy_family

CAPTURE_OWNER_BOT = "bot"
CAPTURE_TARGET_REASON_BOT_WARM = "bot_warm"
CAPTURE_TARGET_REASON_BOT_HOT = "bot_hot"
HOT_TARGET_THRESHOLD = 70.0
WARM_TTL_SECONDS = 300
HOT_DISCOVERY_TTL_SECONDS = 90


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _ttl_iso(seconds: int) -> str:
    return (
        (_utc_now() + timedelta(seconds=max(int(seconds), 1)))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


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


def _matching_candidates(
    *,
    opportunities: list[dict[str, Any]],
    runtime: ResolvedAutomation,
) -> list[dict[str, Any]]:
    strategy_family = runtime.strategy_config.strategy_family
    symbols = set(runtime.symbols)
    filtered: list[dict[str, Any]] = []
    for opportunity in opportunities:
        underlying_symbol = str(opportunity.get("underlying_symbol") or "").upper()
        if symbols and underlying_symbol not in symbols:
            continue
        if (
            normalize_strategy_family(opportunity.get("strategy_family"))
            != strategy_family
        ):
            continue
        candidate = _candidate_payload(opportunity)
        if candidate is None:
            continue
        filtered.append({**candidate, "execution_score": _score(opportunity)})
    filtered.sort(
        key=lambda item: (
            -float(item.get("execution_score") or 0.0),
            str(item.get("underlying_symbol") or ""),
            str(item.get("short_symbol") or ""),
            str(item.get("long_symbol") or ""),
        )
    )
    return filtered


def refresh_options_automation_capture_targets(
    *,
    recovery_store: Any,
    session_id: str,
    session_date: str,
    entry_runtimes: list[tuple[ResolvedBot, ResolvedAutomation]],
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
        CAPTURE_TARGET_REASON_BOT_WARM: [],
        CAPTURE_TARGET_REASON_BOT_HOT: [],
    }
    for bot, automation in entry_runtimes:
        owner_key = bot.bot.bot_id
        active_owner_keys.append(owner_key)
        candidates = _matching_candidates(
            opportunities=opportunities, runtime=automation
        )
        warm_candidates = candidates[:6]
        hot_threshold = float(
            automation.automation.trigger_policy.get("min_opportunity_score")
            or HOT_TARGET_THRESHOLD
        )
        hot_candidates = [
            candidate
            for candidate in candidates
            if float(candidate.get("execution_score") or 0.0) >= hot_threshold
        ][:2]

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
            owner_kind=CAPTURE_OWNER_BOT,
            owner_key=owner_key,
            reason=CAPTURE_TARGET_REASON_BOT_WARM,
            session_id=session_id,
            session_date=session_date,
            label=label,
            rows=warm_rows,
        )
        recovery_store.replace_capture_targets(
            owner_kind=CAPTURE_OWNER_BOT,
            owner_key=owner_key,
            reason=CAPTURE_TARGET_REASON_BOT_HOT,
            session_id=session_id,
            session_date=session_date,
            label=label,
            rows=hot_rows,
        )
        summary.append(
            {
                "bot_id": bot.bot.bot_id,
                "automation_id": automation.automation.automation_id,
                "warm_target_count": len(warm_rows),
                "hot_target_count": len(hot_rows),
            }
        )
        capture_targets[CAPTURE_TARGET_REASON_BOT_WARM].extend(warm_rows)
        capture_targets[CAPTURE_TARGET_REASON_BOT_HOT].extend(hot_rows)

    recovery_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_BOT,
        active_owner_keys=active_owner_keys,
        reason=CAPTURE_TARGET_REASON_BOT_WARM,
    )
    recovery_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_BOT,
        active_owner_keys=active_owner_keys,
        reason=CAPTURE_TARGET_REASON_BOT_HOT,
    )
    return {
        "status": "ok",
        "targets": summary,
        "capture_targets": capture_targets,
    }


__all__ = [
    "CAPTURE_OWNER_BOT",
    "CAPTURE_TARGET_REASON_BOT_HOT",
    "CAPTURE_TARGET_REASON_BOT_WARM",
    "refresh_options_automation_capture_targets",
]

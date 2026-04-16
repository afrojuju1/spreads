from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from spreads.db.decorators import with_storage
from spreads.services.automations import automation_should_run_now
from spreads.services.bots import load_active_bots
from spreads.services.option_structures import normalize_strategy_family

ACTIVE_INTENT_STATES = ["pending", "claimed", "submitted", "partially_filled"]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _market_date_today() -> str:
    return datetime.now(UTC).date().isoformat()


def _decision_id(run_key: str, opportunity_id: str) -> str:
    material = f"{run_key}|{opportunity_id}".encode("utf-8")
    return f"opportunity_decision:{hashlib.sha1(material).hexdigest()[:24]}"


def _intent_id(opportunity_decision_id: str) -> str:
    return f"execution_intent:{opportunity_decision_id}"


def _slot_key(bot_id: str, strategy_config_id: str, underlying_symbol: str) -> str:
    return f"entry:{bot_id}:{strategy_config_id}:{underlying_symbol}"


def _score(row: dict[str, Any]) -> float:
    for key in ("execution_score", "promotion_score"):
        value = row.get(key)
        try:
            if value not in (None, ""):
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _matching_opportunities(
    *,
    signal_store: Any,
    market_date: str,
    symbols: tuple[str, ...],
    strategy_family: str,
) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in signal_store.list_opportunities(
            market_date=market_date,
            eligibility_state="live",
            strategy_family=strategy_family,
            limit=500,
        )
    ]
    filtered = [
        row
        for row in rows
        if str(row.get("underlying_symbol") or "").upper() in set(symbols)
        and str(row.get("lifecycle_state") or "") in {"candidate", "ready", "blocked"}
        and row.get("consumed_by_execution_attempt_id") in (None, "")
    ]
    filtered.sort(
        key=lambda row: (
            -_score(row),
            int(row.get("selection_rank") or 999999),
            str(row.get("opportunity_id") or ""),
        )
    )
    return filtered


@with_storage()
def run_entry_automation_decision(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    execution_store = storage.execution
    if not signal_store.schema_ready() or not signal_store.decision_schema_ready():
        return {"status": "skipped", "reason": "signal_decision_schema_unavailable"}
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}

    bots = load_active_bots()
    bot = bots.get(bot_id)
    if bot is None:
        raise ValueError(f"Unknown or paused bot_id: {bot_id}")
    automation = next(
        (
            item
            for item in bot.automations
            if item.automation.automation_id == automation_id
        ),
        None,
    )
    if automation is None:
        raise ValueError(f"Unknown automation_id for bot {bot_id}: {automation_id}")
    if not automation.automation.is_entry:
        raise ValueError(f"Automation {automation_id} is not an entry automation")
    if not automation_should_run_now(automation.automation):
        return {
            "status": "skipped",
            "reason": "outside_schedule_window",
            "bot_id": bot_id,
            "automation_id": automation_id,
        }

    resolved_market_date = market_date or _market_date_today()
    run_key = f"decision:{bot_id}:{automation_id}:{_utc_now()}"
    scope_key = f"entry:{bot_id}:{automation_id}:{resolved_market_date}"
    policy_ref = {
        "bot_id": bot.bot.bot_id,
        "automation_id": automation.automation.automation_id,
        "strategy_config_id": automation.strategy_config.strategy_config_id,
        "strategy_id": automation.strategy_config.strategy_id,
        "market_date": resolved_market_date,
    }
    opportunities = _matching_opportunities(
        signal_store=signal_store,
        market_date=resolved_market_date,
        symbols=automation.symbols,
        strategy_family=normalize_strategy_family(
            automation.strategy_config.strategy_id
        ),
    )
    min_score = float(
        automation.automation.trigger_policy.get("min_opportunity_score") or 0.0
    )
    selected: dict[str, Any] | None = None
    if opportunities and _score(opportunities[0]) >= min_score:
        selected = opportunities[0]

    decisions: list[dict[str, Any]] = []
    selected_intent: dict[str, Any] | None = None
    for rank, opportunity in enumerate(opportunities, start=1):
        opportunity_id = str(opportunity["opportunity_id"])
        state = (
            "selected"
            if selected is not None and opportunity_id == selected["opportunity_id"]
            else "rejected"
        )
        reason_codes = [
            "selected_for_entry"
            if state == "selected"
            else "lower_ranked_than_selected_opportunity"
        ]
        decision = signal_store.upsert_opportunity_decision(
            opportunity_decision_id=_decision_id(run_key, opportunity_id),
            opportunity_id=opportunity_id,
            bot_id=bot.bot.bot_id,
            automation_id=automation.automation.automation_id,
            run_key=run_key,
            scope_key=scope_key,
            policy_ref=policy_ref,
            config_hash=bot.config_hash,
            state=state,
            score=_score(opportunity),
            rank=rank,
            reason_codes=reason_codes,
            superseded_by_id=None,
            decided_at=_utc_now(),
            payload={
                "opportunity": {
                    "opportunity_id": opportunity_id,
                    "underlying_symbol": opportunity.get("underlying_symbol"),
                    "strategy_family": opportunity.get("strategy_family"),
                }
            },
        )
        decisions.append(decision)
        if state != "selected":
            continue
        slot_key = _slot_key(
            bot.bot.bot_id,
            automation.strategy_config.strategy_config_id,
            str(opportunity.get("underlying_symbol") or ""),
        )
        existing_active = execution_store.list_execution_intents(
            slot_key=slot_key,
            states=ACTIVE_INTENT_STATES,
            limit=1,
        )
        if existing_active:
            signal_store.upsert_opportunity_decision(
                opportunity_decision_id=str(decision["opportunity_decision_id"]),
                opportunity_id=opportunity_id,
                bot_id=bot.bot.bot_id,
                automation_id=automation.automation.automation_id,
                run_key=run_key,
                scope_key=scope_key,
                policy_ref=policy_ref,
                config_hash=bot.config_hash,
                state="blocked",
                score=_score(opportunity),
                rank=rank,
                reason_codes=["active_execution_intent_exists"],
                superseded_by_id=None,
                decided_at=_utc_now(),
                payload={"slot_key": slot_key},
            )
            continue
        selected_intent = execution_store.upsert_execution_intent(
            execution_intent_id=_intent_id(str(decision["opportunity_decision_id"])),
            bot_id=bot.bot.bot_id,
            automation_id=automation.automation.automation_id,
            opportunity_decision_id=str(decision["opportunity_decision_id"]),
            strategy_position_id=None,
            execution_attempt_id=None,
            action_type="open",
            slot_key=slot_key,
            claim_token=None,
            policy_ref=policy_ref,
            config_hash=bot.config_hash,
            state="pending",
            expires_at=str(opportunity.get("expires_at") or _utc_now()),
            superseded_by_id=None,
            payload={
                "opportunity_id": opportunity_id,
                "underlying_symbol": opportunity.get("underlying_symbol"),
                "execution_mode": automation.automation.execution_mode,
                "approval_mode": automation.automation.approval_mode,
            },
            created_at=_utc_now(),
            updated_at=_utc_now(),
        )
        execution_store.append_execution_intent_event(
            execution_intent_id=str(selected_intent["execution_intent_id"]),
            event_type="created",
            event_at=_utc_now(),
            payload={"opportunity_id": opportunity_id, "slot_key": slot_key},
        )

    return {
        "status": "ok",
        "bot_id": bot.bot.bot_id,
        "automation_id": automation.automation.automation_id,
        "market_date": resolved_market_date,
        "run_key": run_key,
        "opportunity_count": len(opportunities),
        "decision_count": len(decisions),
        "selected_opportunity_id": None
        if selected is None
        else str(selected.get("opportunity_id")),
        "execution_intent_id": None
        if selected_intent is None
        else str(selected_intent.get("execution_intent_id")),
    }


@with_storage()
def run_active_entry_decisions(
    *,
    db_target: str,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    bots = load_active_bots()
    results: list[dict[str, Any]] = []
    for bot in bots.values():
        for automation in bot.automations:
            if not automation.automation.is_entry:
                continue
            results.append(
                run_entry_automation_decision(
                    db_target=db_target,
                    bot_id=bot.bot.bot_id,
                    automation_id=automation.automation.automation_id,
                    market_date=market_date,
                    storage=storage,
                )
            )
    return {
        "status": "ok",
        "decision_runs": results,
        "decision_run_count": len(results),
    }


__all__ = ["run_active_entry_decisions", "run_entry_automation_decision"]

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.automations import automation_should_run_now
from spreads.services.bots import bot_time_reached, load_active_bots
from spreads.services.execution_portfolio import refresh_session_position_marks
from spreads.services.exit_manager import (
    OPEN_CLOSE_ATTEMPT_STATUSES,
    OPEN_POSITION_STATUSES,
    evaluate_exit_policy,
)
from spreads.services.option_structures import normalize_strategy_family
from spreads.services.positions import enrich_position_row

ACTIVE_INTENT_STATES = ["pending", "claimed", "submitted", "partially_filled"]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _expires_in(minutes: int) -> str:
    return (
        (datetime.now(UTC) + timedelta(minutes=max(minutes, 1)))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_management_limit_price(
    position: dict[str, Any],
) -> tuple[float | None, str | None]:
    mark = _coerce_float(position.get("close_mark"))
    if mark is not None and mark > 0:
        return round(max(mark, 0.01), 2), "mark"
    width = _coerce_float(position.get("width"))
    if width is not None and width > 0:
        return round(max(width, 0.01), 2), "width"
    return None, None


def _intent_id(position_id: str, automation_id: str) -> str:
    return f"execution_intent:manage:{automation_id}:{position_id}"


def _slot_key(position_id: str) -> str:
    return f"manage:{position_id}:close"


def _linked_execution_intent_id(attempt: dict[str, Any]) -> str | None:
    request = attempt.get("request")
    if not isinstance(request, dict):
        return None
    return _as_text(request.get("execution_intent_id"))


def _position_owner_intent(
    execution_store: Any, position: dict[str, Any]
) -> dict[str, Any] | None:
    open_execution_attempt_id = _as_text(position.get("open_execution_attempt_id"))
    if open_execution_attempt_id is None:
        return None
    attempt = execution_store.get_attempt(open_execution_attempt_id)
    if attempt is None:
        return None
    execution_intent_id = _linked_execution_intent_id(dict(attempt))
    if execution_intent_id is None or not execution_store.intent_schema_ready():
        return None
    return execution_store.get_execution_intent(execution_intent_id)


@with_storage()
def run_management_automation_decision(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if (
        not execution_store.portfolio_schema_ready()
        or not execution_store.intent_schema_ready()
    ):
        return {"status": "skipped", "reason": "position_or_intent_schema_unavailable"}

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
    if not automation.automation.is_management:
        raise ValueError(f"Automation {automation_id} is not a management automation")
    if not automation_should_run_now(automation.automation):
        return {
            "status": "skipped",
            "reason": "outside_schedule_window",
            "bot_id": bot_id,
            "automation_id": automation_id,
        }

    positions = [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
            statuses=OPEN_POSITION_STATUSES,
            limit=500,
        )
    ]
    strategy_family = automation.strategy_config.strategy_family
    allowed_symbols = set(automation.symbols)
    managed_positions: list[dict[str, Any]] = []
    for position in positions:
        owner_intent = _position_owner_intent(execution_store, position)
        if owner_intent is None or str(owner_intent.get("bot_id") or "") != bot_id:
            continue
        if (
            normalize_strategy_family(position.get("strategy_family"))
            != strategy_family
        ):
            continue
        if (
            allowed_symbols
            and str(position.get("underlying_symbol") or "").upper()
            not in allowed_symbols
        ):
            continue
        managed_positions.append(position)

    if not managed_positions:
        return {
            "status": "ok",
            "bot_id": bot_id,
            "automation_id": automation_id,
            "position_count": 0,
            "evaluated": 0,
            "created_intents": 0,
            "skipped": 0,
        }

    flatten_due = bot_time_reached(
        bot.bot,
        time_value=bot.bot.flatten_positions_at_et,
    )

    refresh_session_position_marks(
        db_target=db_target,
        session_ids=sorted(
            {
                str(position["session_id"])
                for position in managed_positions
                if position.get("session_id")
            }
        ),
        storage=storage,
    )
    refreshed_positions = [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
            statuses=OPEN_POSITION_STATUSES,
            limit=500,
        )
    ]
    position_map = {
        str(position["position_id"]): position for position in refreshed_positions
    }
    evaluated = 0
    created_intents = 0
    skipped = 0
    decisions: list[dict[str, Any]] = []
    for original_position in managed_positions:
        position = position_map.get(
            str(original_position["position_id"]), original_position
        )
        position_id = str(position["position_id"])
        if execution_store.list_open_attempts_for_position(
            position_id=position_id,
            statuses=sorted(OPEN_CLOSE_ATTEMPT_STATUSES),
        ):
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason="close_already_open",
                updated_at=_utc_now(),
            )
            evaluated += 1
            skipped += 1
            decisions.append(
                {
                    "position_id": position_id,
                    "reason": "close_already_open",
                    "should_close": False,
                }
            )
            continue

        if flatten_due:
            limit_price, limit_price_source = _resolve_management_limit_price(position)
            decision = (
                {"should_close": False, "reason": "awaiting_flatten_price"}
                if limit_price is None
                else {
                    "should_close": True,
                    "reason": "bot_flatten",
                    "limit_price": limit_price,
                    "limit_price_source": limit_price_source,
                }
            )
        else:
            decision = evaluate_exit_policy(
                position=position,
                mark=_coerce_float(position.get("close_mark")),
                now=datetime.now(UTC),
            )
        execution_store.update_position(
            position_id=position_id,
            last_exit_evaluated_at=_utc_now(),
            last_exit_reason=str(decision["reason"]),
            updated_at=_utc_now(),
        )
        evaluated += 1
        decisions.append(
            {
                "position_id": position_id,
                "reason": str(decision["reason"]),
                "should_close": bool(decision["should_close"]),
            }
        )
        if not decision["should_close"]:
            skipped += 1
            continue

        slot_key = _slot_key(position_id)
        if execution_store.list_execution_intents(
            slot_key=slot_key,
            states=ACTIVE_INTENT_STATES,
            limit=1,
        ):
            skipped += 1
            continue

        execution_intent = execution_store.upsert_execution_intent(
            execution_intent_id=_intent_id(position_id, automation_id),
            bot_id=bot.bot.bot_id,
            automation_id=automation.automation.automation_id,
            opportunity_decision_id=None,
            strategy_position_id=position_id,
            execution_attempt_id=None,
            action_type="close",
            slot_key=slot_key,
            claim_token=None,
            policy_ref={
                "bot_id": bot.bot.bot_id,
                "automation_id": automation.automation.automation_id,
                "strategy_config_id": automation.strategy_config.strategy_config_id,
                "strategy_id": automation.strategy_config.strategy_id,
            },
            config_hash=bot.config_hash,
            state="pending",
            expires_at=_expires_in(5),
            superseded_by_id=None,
            payload={
                "position_id": position_id,
                "limit_price": decision.get("limit_price"),
                "limit_price_source": decision.get("limit_price_source"),
                "reason": decision.get("reason"),
                "execution_mode": automation.automation.execution_mode,
                "approval_mode": automation.automation.approval_mode,
            },
            created_at=_utc_now(),
            updated_at=_utc_now(),
        )
        execution_store.append_execution_intent_event(
            execution_intent_id=str(execution_intent["execution_intent_id"]),
            event_type="created",
            event_at=_utc_now(),
            payload={
                "position_id": position_id,
                "reason": decision.get("reason"),
                "limit_price": decision.get("limit_price"),
            },
        )
        created_intents += 1

    return {
        "status": "ok",
        "bot_id": bot.bot.bot_id,
        "automation_id": automation.automation.automation_id,
        "position_count": len(managed_positions),
        "evaluated": evaluated,
        "created_intents": created_intents,
        "skipped": skipped,
        "decisions": decisions[:25],
    }


def run_active_management_decisions(
    *, db_target: str, storage: Any | None = None
) -> dict[str, Any]:
    bots = load_active_bots()
    results: list[dict[str, Any]] = []
    for bot in bots.values():
        for automation in bot.automations:
            if not automation.automation.is_management:
                continue
            results.append(
                run_management_automation_decision(
                    db_target=db_target,
                    bot_id=bot.bot.bot_id,
                    automation_id=automation.automation.automation_id,
                    storage=storage,
                )
            )
    return {
        "status": "ok",
        "decision_runs": results,
        "decision_run_count": len(results),
    }


__all__ = ["run_active_management_decisions", "run_management_automation_decision"]

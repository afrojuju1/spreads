from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from core.services.positions import enrich_position_row
from core.services.position_lifecycle import build_close_decision_lifecycle
from core.services.trading_engine.close_policy import evaluate_exit_policy
from core.services.trading_strategies import routine_should_run_now
from core.services.trading_strategy_runtime import (
    find_management_runtime_for_position,
    resolve_management_runtimes,
)

from .kernel import EngineComponentRole, EngineRunRef
from .portfolio import CloseDecisionResult, PositionSnapshot
from .risk_runtime import OPEN_POSITION_STATUSES

NEW_YORK = ZoneInfo("America/New_York")


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


def _round_money(value: Any) -> float | None:
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    return round(parsed, 4)


def _time_reached(time_value: str | None, *, now: datetime) -> bool:
    rendered = _as_text(time_value)
    if rendered is None:
        return False
    hour_text, separator, minute_text = rendered.partition(":")
    if separator != ":":
        return False
    current = now.astimezone(NEW_YORK)
    return (current.hour, current.minute) >= (int(hour_text), int(minute_text))


def build_position_snapshot(position: Mapping[str, Any]) -> PositionSnapshot:
    payload = enrich_position_row(dict(position))
    position_id = str(payload["position_id"])
    return PositionSnapshot(
        position_id=position_id,
        trading_strategy_id=_as_text(payload.get("trading_strategy_id")) or "",
        underlying_symbol=str(payload.get("underlying_symbol") or payload.get("root_symbol") or ""),
        state=str(payload.get("position_status") or payload.get("status") or "unknown"),
        payload=payload,
    )


def close_decision_lifecycle(
    *,
    position: Mapping[str, Any],
    decision: Mapping[str, Any],
    decision_source: str | None = None,
    decided_at: str | None = None,
) -> dict[str, Any]:
    close_decision = decision.get("close_decision")
    if isinstance(close_decision, Mapping):
        return dict(close_decision)
    return build_close_decision_lifecycle(
        position=position,
        decision=decision,
        decision_source=decision_source,
        decided_at=decided_at,
    )


def build_blocked_close_decision(
    *,
    position: Mapping[str, Any],
    reason: str,
    decision_source: str,
    decided_at: str | None = None,
) -> dict[str, Any]:
    return build_close_decision_lifecycle(
        position=position,
        decision={
            "should_close": False,
            "reason": reason,
            "recipe_ref": None,
            "limit_price": None,
            "limit_price_source": None,
            "decision_source": decision_source,
            "decision_details": None,
        },
        decision_source=decision_source,
        decided_at=decided_at,
    )


def close_decision_row_fields(close_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "close_decision_id": close_decision.get("close_decision_id"),
        "close_decision_state": close_decision.get("decision_state"),
        "close_decision": dict(close_decision),
    }


def close_decision_projection(
    *,
    position_id: str,
    reason: str,
    decision_source: str,
    should_close: bool,
    portfolio_run_id: str,
    close_decision: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "position_id": position_id,
        "reason": reason,
        "decision_source": decision_source,
        "should_close": should_close,
        "portfolio_run_id": portfolio_run_id,
        **close_decision_row_fields(close_decision),
    }


def blocked_close_decision_projection(
    *,
    position: Mapping[str, Any],
    reason: str,
    decision_source: str,
    portfolio_run_id: str,
    decided_at: str | None = None,
) -> dict[str, Any]:
    position_id = _as_text(position.get("position_id")) or "unknown"
    close_decision = build_blocked_close_decision(
        position=position,
        reason=reason,
        decision_source=decision_source,
        decided_at=decided_at,
    )
    return close_decision_projection(
        position_id=position_id,
        reason=reason,
        decision_source=decision_source,
        should_close=False,
        portfolio_run_id=portfolio_run_id,
        close_decision=close_decision,
    )


def evaluate_position_close_decision(
    *,
    position: Mapping[str, Any],
    now: datetime,
    management_runtimes: tuple[Any, ...],
) -> tuple[dict[str, Any], str, Any | None]:
    position_payload = dict(position)
    runtime, runtime_reason = find_management_runtime_for_position(
        position_payload,
        runtimes=management_runtimes,
    )
    if runtime is None:
        if runtime_reason == "ambiguous_management_runtime":
            return (
                {
                    "should_close": False,
                    "reason": "ambiguous_management_runtime",
                    "recipe_ref": None,
                    "limit_price": None,
                    "limit_price_source": None,
                    "decision_source": "management_runtime",
                    "management_recipe_refs": [],
                    "decision_details": None,
                },
                "management_runtime",
                None,
            )
        policy_decision = evaluate_exit_policy(
            position=position_payload,
            mark=_coerce_float(position_payload.get("close_mark")),
            now=now,
        )
        policy_decision["decision_source"] = "position_exit_policy"
        policy_decision["management_recipe_refs"] = []
        policy_decision["decision_details"] = {
            key: value
            for key, value in policy_decision.items()
            if key
            in {
                "policy",
                "mark",
                "effective_mark",
                "mark_state",
                "entry_value",
                "premium_kind",
                "profit_target_mark",
                "stop_mark",
                "force_close_at",
            }
        }
        return (
            policy_decision,
            "position_exit_policy",
            None,
        )
    if runtime.strategy.management is None or not routine_should_run_now(runtime.strategy.management, now=now):
        return (
            {
                "should_close": False,
                "reason": "outside_management_schedule_window",
                "recipe_ref": None,
                "limit_price": None,
                "limit_price_source": None,
                "decision_source": "management_runtime",
                "management_recipe_refs": list(runtime.management_recipe_refs),
                "decision_details": None,
            },
            "management_runtime",
            runtime,
        )

    from core.services.management_planner import plan_position_management

    decision = plan_position_management(
        runtime=runtime,
        position=position_payload,
        flatten_due=_time_reached(runtime.strategy.runtime.flatten_positions_at_et, now=now),
        now=now,
    )
    decision["decision_source"] = "management_runtime"
    decision["management_recipe_refs"] = list(runtime.management_recipe_refs)
    return (decision, "management_runtime", runtime)


def describe_position_exit_state(
    *,
    position: dict[str, Any],
    now: datetime | None = None,
    management_runtimes: tuple[Any, ...] | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(UTC)
    runtimes = tuple(resolve_management_runtimes()) if management_runtimes is None else tuple(management_runtimes)
    decision, decision_source, _runtime = evaluate_position_close_decision(
        position=position,
        now=current_time,
        management_runtimes=runtimes,
    )
    close_decision = close_decision_lifecycle(
        position=position,
        decision=decision,
        decision_source=decision_source,
        decided_at=current_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
    )
    details = dict(decision.get("decision_details") or {}) if isinstance(decision.get("decision_details"), dict) else {}
    if not details:
        fallback = evaluate_exit_policy(
            position=position,
            mark=_coerce_float(position.get("close_mark")),
            now=current_time,
        )
        details = {
            key: value
            for key, value in fallback.items()
            if key
            in {
                "policy",
                "mark",
                "effective_mark",
                "mark_state",
                "entry_value",
                "premium_kind",
                "profit_target_mark",
                "stop_mark",
                "force_close_at",
            }
        }
    return {
        "decision_source": _as_text(decision.get("decision_source")),
        "management_recipe_refs": [str(value) for value in list(decision.get("management_recipe_refs") or []) if str(value or "").strip()],
        "should_close": bool(decision.get("should_close")),
        "reason": str(decision.get("reason") or "unknown"),
        "close_decision_state": close_decision.get("decision_state"),
        "close_decision_id": close_decision.get("close_decision_id"),
        "close_decision": close_decision,
        "recipe_ref": _as_text(decision.get("recipe_ref")),
        "limit_price": _coerce_float(decision.get("limit_price")),
        "limit_price_source": _as_text(decision.get("limit_price_source")),
        "current_mark": _round_money(details.get("mark")),
        "effective_mark": _round_money(details.get("effective_mark")),
        "mark_state": _as_text(details.get("mark_state")),
        "entry_value": _round_money(details.get("entry_value")),
        "premium_kind": _as_text(details.get("premium_kind")),
        "profit_target_mark": _round_money(details.get("profit_target_mark")),
        "stop_mark": _round_money(details.get("stop_mark")),
        "force_close_at": _as_text(details.get("force_close_at")),
    }


class PostgresPortfolioEngine:
    def __init__(
        self,
        *,
        execution_store: Any,
        now: datetime | None = None,
        management_runtimes: tuple[Any, ...] | None = None,
    ) -> None:
        self.execution_store = execution_store
        self.now = now or datetime.now(UTC)
        self.management_runtimes = management_runtimes

    def list_open_positions(
        self,
        *,
        trading_strategy_id: str | None = None,
        limit: int = 200,
    ) -> tuple[PositionSnapshot, ...]:
        positions = self.execution_store.list_positions(
            trading_strategy_id=trading_strategy_id,
            statuses=list(OPEN_POSITION_STATUSES),
            limit=limit,
        )
        return tuple(build_position_snapshot(position) for position in positions)

    def evaluate_close(
        self,
        *,
        run_ref: EngineRunRef,
        position: PositionSnapshot,
    ) -> CloseDecisionResult:
        decision, decision_source, management_runtime = evaluate_position_close_decision(
            position=dict(position.payload),
            now=self.now,
            management_runtimes=tuple(resolve_management_runtimes() if self.management_runtimes is None else self.management_runtimes),
        )
        close_decision = close_decision_lifecycle(
            position=dict(position.payload),
            decision=decision,
            decision_source=decision_source,
            decided_at=self.now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        )
        reason = str(decision.get("reason") or close_decision.get("reason") or "unknown")
        return CloseDecisionResult(
            run_ref=run_ref,
            close_decision_id=str(close_decision["close_decision_id"]),
            position_id=position.position_id,
            state=str(close_decision.get("decision_state") or "unknown"),
            reason_codes=(reason,),
            payload={
                "decision": {**dict(decision), "close_decision": close_decision},
                "decision_source": decision_source,
                "management_runtime": management_runtime,
                "close_decision": close_decision,
            },
        )


def build_portfolio_run_ref(
    *,
    trading_strategy_id: str | None = None,
    job_run_id: str | None = None,
    now: datetime | None = None,
) -> EngineRunRef:
    timestamp = (now or datetime.now(UTC)).isoformat(timespec="seconds").replace("+00:00", "Z")
    return EngineRunRef(
        role=EngineComponentRole.PORTFOLIO,
        run_id=f"portfolio:manage:{timestamp}",
        trading_strategy_id=trading_strategy_id,
        job_run_id=job_run_id,
    )


__all__ = [
    "OPEN_POSITION_STATUSES",
    "PostgresPortfolioEngine",
    "blocked_close_decision_projection",
    "build_blocked_close_decision",
    "build_portfolio_run_ref",
    "build_position_snapshot",
    "close_decision_lifecycle",
    "close_decision_projection",
    "close_decision_row_fields",
    "describe_position_exit_state",
    "evaluate_position_close_decision",
]

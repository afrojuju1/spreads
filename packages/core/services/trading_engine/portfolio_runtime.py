from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.services.positions import enrich_position_row

from .kernel import EngineComponentRole, EngineRunRef
from .portfolio import CloseDecisionResult, PositionSnapshot

OPEN_POSITION_STATUSES = ("open", "partial_close")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


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
        from core.services.exit_manager import _close_decision_lifecycle, _evaluate_position_close_decision
        from core.services.trading_strategy_runtime import resolve_management_runtimes

        decision, decision_source, management_runtime = _evaluate_position_close_decision(
            position=dict(position.payload),
            now=self.now,
            management_runtimes=tuple(resolve_management_runtimes() if self.management_runtimes is None else self.management_runtimes),
        )
        close_decision = _close_decision_lifecycle(
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
    "build_portfolio_run_ref",
    "build_position_snapshot",
]

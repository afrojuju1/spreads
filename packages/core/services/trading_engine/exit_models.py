from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import Field, field_validator

from core.model_contracts import DomainModel
from core.value_coercion import as_text, coerce_float

from .kernel import EnginePayload, EngineRunRef


@dataclass(frozen=True)
class CloseDecisionResult:
    run_ref: EngineRunRef
    close_decision_id: str
    position_id: str
    state: str
    reason_codes: tuple[str, ...] = ()
    payload: EnginePayload = field(default_factory=dict)


class PositionExitSnapshot(DomainModel):
    position_id: str
    trading_strategy_id: str | None = None
    trade_structure: str | None = None
    session_date: str | None = None
    position_state: str
    remaining_quantity: float
    opened_quantity: float | None = None
    entry_value: float | None = None
    entry_value_kind: str | None = None
    close_mark: float | None = None
    close_mark_source: str | None = None
    close_marked_at: str | None = None
    close_mark_age_seconds: float | None = None
    quote_quality_state: str = "unknown"
    reconciliation_state: str | None = None
    broker_sync_state: str | None = None
    active_close_attempt: bool = False
    active_close_intent: bool = False
    management_runtime_state: str = "unknown"
    management_recipe_refs: tuple[str, ...] = Field(default_factory=tuple)
    exit_policy: dict[str, Any] = Field(default_factory=dict)
    opening_signal: dict[str, Any] = Field(default_factory=dict)
    position: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "position_id",
        "trading_strategy_id",
        "trade_structure",
        "session_date",
        "position_state",
        "entry_value_kind",
        "close_mark_source",
        "close_marked_at",
        "quote_quality_state",
        "reconciliation_state",
        "broker_sync_state",
        "management_runtime_state",
        mode="before",
    )
    @classmethod
    def _normalize_text(cls, value: Any) -> str | None:
        return as_text(value)

    @field_validator(
        "remaining_quantity",
        "opened_quantity",
        "entry_value",
        "close_mark",
        "close_mark_age_seconds",
        mode="before",
    )
    @classmethod
    def _normalize_float(cls, value: Any) -> float | None:
        return coerce_float(value)

    def to_position_payload(self) -> dict[str, Any]:
        payload = dict(self.position)
        payload["exit_policy"] = dict(self.exit_policy)
        payload["close_mark"] = self.close_mark
        payload["close_mark_source"] = self.close_mark_source
        payload["close_marked_at"] = self.close_marked_at
        payload["exit_snapshot"] = self.to_payload()
        if self.opening_signal:
            payload["opening_signal"] = dict(self.opening_signal)
            evidence = self.opening_signal.get("evidence")
            if isinstance(evidence, dict):
                payload["opening_signal_evidence"] = dict(evidence)
                setup = evidence.get("setup") or evidence.get("setup_context")
                if isinstance(setup, dict):
                    payload["underlying_setup"] = dict(setup)
        return payload


__all__ = ["CloseDecisionResult", "PositionExitSnapshot"]

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from core.value_coercion import as_mapping
from core.storage.records import StorageRow
from core.storage.serializers import parse_datetime


def _row_tuple(rows: list[StorageRow]) -> tuple[StorageRow, ...]:
    return tuple(dict(row) for row in rows)


def _row_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


@dataclass(frozen=True)
class ExecutionAttemptActivityRead:
    attempt: StorageRow
    orders: tuple[StorageRow, ...] = ()
    fills: tuple[StorageRow, ...] = ()

    @classmethod
    def from_rows(
        cls,
        *,
        attempt: StorageRow,
        orders: list[StorageRow],
        fills: list[StorageRow],
    ) -> ExecutionAttemptActivityRead:
        return cls(
            attempt=dict(attempt),
            orders=_row_tuple(orders),
            fills=_row_tuple(fills),
        )

    def to_payload(self) -> StorageRow:
        return {
            **dict(self.attempt),
            "orders": [dict(order) for order in self.orders],
            "fills": [dict(fill) for fill in self.fills],
        }


@dataclass(frozen=True)
class TradeDecisionSignalRead:
    decision: StorageRow
    signal: StorageRow
    trade_decision_id: str
    trade_signal_id: str
    decision_state: str
    signal_state: str
    signal_expires_at: datetime | None

    @classmethod
    def from_rows(
        cls,
        *,
        decision: StorageRow,
        signal: StorageRow,
    ) -> TradeDecisionSignalRead:
        trade_decision_id = str(decision["trade_decision_id"])
        trade_signal_id = str(decision.get("trade_signal_id") or signal["trade_signal_id"])
        return cls(
            decision=dict(decision),
            signal=dict(signal),
            trade_decision_id=trade_decision_id,
            trade_signal_id=trade_signal_id,
            decision_state=str(decision.get("decision_state") or "").strip().lower(),
            signal_state=str(signal.get("signal_state") or "").strip().lower(),
            signal_expires_at=parse_datetime(signal.get("expires_at")),
        )

    @property
    def selected_execution_shape(self) -> dict[str, Any]:
        return as_mapping(self.decision.get("selected_execution_shape"))

    @property
    def signal_execution_shape(self) -> dict[str, Any]:
        return as_mapping(self.signal.get("execution_shape"))

    @property
    def execution_shape(self) -> dict[str, Any]:
        return self.selected_execution_shape or self.signal_execution_shape

    @property
    def order_payload(self) -> dict[str, Any]:
        return as_mapping(self.execution_shape.get("order_payload"))

    @property
    def execution_shape_legs(self) -> list[dict[str, Any]]:
        return _row_list(self.execution_shape.get("legs"))

    @property
    def signal_legs(self) -> list[dict[str, Any]]:
        return _row_list(self.signal.get("legs"))

    @property
    def economics(self) -> dict[str, Any]:
        return as_mapping(self.signal.get("economics"))

    @property
    def selected_quantity(self) -> Any:
        return self.decision.get("selected_quantity")

    @property
    def trade_structure(self) -> str | None:
        value = self.signal.get("trade_structure") or self.decision.get("trade_structure")
        return None if value in (None, "") else str(value)

    def signal_is_expired(self, *, now: datetime) -> bool:
        return self.signal_expires_at is not None and self.signal_expires_at <= now


__all__ = [
    "ExecutionAttemptActivityRead",
    "TradeDecisionSignalRead",
]

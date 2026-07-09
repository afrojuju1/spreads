from __future__ import annotations

from typing import Any

from core.services.option_structures import (
    primary_short_long_symbols,
    structure_symbol_path,
)
from core.storage.base import RepositoryBase
from core.storage.execution import (
    ExecutionAttemptRepositoryMixin,
    ExecutionIntentRepositoryMixin,
    ExecutionOrderRepositoryMixin,
    ExecutionPositionRepositoryMixin,
)
from core.storage.execution_models import ExecutionAttemptModel
from core.storage.records import ExecutionAttemptRecord


class ExecutionRepository(
    ExecutionAttemptRepositoryMixin,
    ExecutionIntentRepositoryMixin,
    ExecutionOrderRepositoryMixin,
    ExecutionPositionRepositoryMixin,
    RepositoryBase,
):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("execution_attempts", "execution_orders", "execution_fills")

    def intent_schema_ready(self) -> bool:
        return self.schema_has_tables("execution_intents", "execution_intent_events", "execution_attempts")

    def positions_schema_ready(self) -> bool:
        return self.portfolio_schema_ready()

    def portfolio_schema_ready(self) -> bool:
        return self.schema_has_tables("execution_attempts", "portfolio_positions", "position_closes")

    def _attempt_extra(self, row: ExecutionAttemptModel | None) -> dict[str, Any]:
        if row is None:
            return {
                "short_symbol": None,
                "long_symbol": None,
                "symbol_path": None,
            }
        legs = list(row.legs_json or [])
        short_symbol, long_symbol = primary_short_long_symbols(legs)
        return {
            "short_symbol": short_symbol,
            "long_symbol": long_symbol,
            "symbol_path": structure_symbol_path(legs),
        }

    def _attempt_row(self, row: ExecutionAttemptModel) -> ExecutionAttemptRecord:
        return self.row(row, extra=self._attempt_extra(row))

    def _attempt_rows(
        self,
        rows: list[ExecutionAttemptModel],
    ) -> list[ExecutionAttemptRecord]:
        return [self._attempt_row(row) for row in rows]

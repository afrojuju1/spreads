from __future__ import annotations

from core.storage.execution_repository import ExecutionRepository


class TradingStore(ExecutionRepository):
    """Execution ledger plus mutable session position state."""


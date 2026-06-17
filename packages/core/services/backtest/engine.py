from __future__ import annotations

from typing import Any

from core.services.backtest.models import BacktestMode, BacktestRequest
from core.services.backtest.stored_facts import build_stored_facts_backtest


class BacktestEngine:
    def run(
        self,
        request: BacktestRequest,
        *,
        db_target: str | None = None,
    ) -> dict[str, Any]:
        if request.mode == BacktestMode.STORED_FACTS:
            return build_stored_facts_backtest(
                start_date=request.start_date,
                end_date=request.end_date,
                strategy_ids=request.strategy_ids,
                max_days=request.max_days,
                market_data_symbol_limit=request.market_data_symbol_limit,
                db_target=db_target,
            )
        raise ValueError(f"Unsupported backtest mode: {request.mode}")


__all__ = ["BacktestEngine"]

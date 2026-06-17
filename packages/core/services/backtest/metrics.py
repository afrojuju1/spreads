from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module, util
from typing import Any

import numpy as np
import pandas as pd

from core.money import money_float, money_sum_float
from core.storage.serializers import parse_datetime
from core.value_coercion import as_mapping, coerce_float, coerce_int


def _rate(numerator: int | float, denominator: int | float) -> float | None:
    if denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 4)


def _rows(day_results: list[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    return [dict(row) for day in day_results for row in day.get(key) or [] if isinstance(row, Mapping)]


def _pnl_from_rows(positions: list[Mapping[str, Any]], closes: list[Mapping[str, Any]]) -> dict[str, float]:
    realized_values = [coerce_float(row.get("realized_pnl")) for row in closes]
    if not realized_values:
        realized_values = [coerce_float(row.get("realized_pnl")) for row in positions]
    unrealized_values = [
        coerce_float(row.get("unrealized_pnl"))
        for row in positions
        if str(row.get("status") or row.get("position_state") or "").strip().lower() != "closed"
    ]
    realized_pnl = money_sum_float(value for value in realized_values if value is not None)
    unrealized_pnl = money_sum_float(value for value in unrealized_values if value is not None)
    return {
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "net_pnl": money_sum_float([realized_pnl, unrealized_pnl]),
    }


def _position_risk(position: Mapping[str, Any]) -> float | None:
    economics = as_mapping(position.get("economics"))
    return coerce_float(economics.get("max_loss")) or coerce_float(position.get("max_loss"))


def _exposure_minutes(positions: list[Mapping[str, Any]]) -> float:
    total_seconds = 0.0
    for position in positions:
        opened_at = parse_datetime(position.get("opened_at"))
        closed_at = parse_datetime(
            position.get("closed_at")
            or position.get("close_marked_at")
            or position.get("updated_at")
        )
        if opened_at is None or closed_at is None:
            continue
        total_seconds += max((closed_at - opened_at).total_seconds(), 0.0)
    return round(total_seconds / 60.0, 2)


def _daily_pnl_series(day_results: list[Mapping[str, Any]]) -> pd.Series:
    rows: list[tuple[str, float]] = []
    for day in day_results:
        market_date = str(day.get("market_date") or day.get("session_date") or "").strip()
        if not market_date:
            continue
        rows.append((market_date, coerce_float(as_mapping(day.get("pnl")).get("net_pnl")) or 0.0))
    if not rows:
        return pd.Series(dtype=float)
    frame = pd.DataFrame(rows, columns=["market_date", "net_pnl"])
    frame["market_date"] = pd.to_datetime(frame["market_date"])
    return frame.groupby("market_date")["net_pnl"].sum().sort_index()


def _max_drawdown(daily_pnl: pd.Series) -> float:
    if daily_pnl.empty:
        return 0.0
    equity = daily_pnl.cumsum()
    drawdown = equity - equity.cummax()
    return money_float(abs(float(drawdown.min()))) or 0.0


def _standard_metrics(daily_pnl: pd.Series, risk_capital: float) -> tuple[dict[str, Any], dict[str, str]]:
    fidelity = {
        "metrics_engine": "pandas_numpy_internal",
        "standard_metrics_library": "not_installed_quantstats_empyrical",
    }
    if daily_pnl.empty or risk_capital <= 0:
        return {}, fidelity

    returns = daily_pnl.astype(float) / float(risk_capital)
    if util.find_spec("empyrical") is not None:
        empyrical = import_module("empyrical")
        return (
            {
                "standard_max_drawdown_pct": round(float(empyrical.max_drawdown(returns)), 6),
                "standard_sharpe": round(float(empyrical.sharpe_ratio(returns)), 6) if len(returns) > 1 else None,
            },
            {
                "metrics_engine": "empyrical",
                "standard_metrics_library": "empyrical",
            },
        )
    if util.find_spec("quantstats") is not None:
        quantstats = import_module("quantstats.stats")
        return (
            {
                "standard_max_drawdown_pct": round(float(quantstats.max_drawdown(returns)), 6),
                "standard_sharpe": round(float(quantstats.sharpe(returns)), 6) if len(returns) > 1 else None,
            },
            {
                "metrics_engine": "quantstats",
                "standard_metrics_library": "quantstats",
            },
        )
    return (
        {
            "daily_pnl_std": money_float(float(np.std(daily_pnl.to_numpy(), ddof=0))) or 0.0,
        },
        fidelity,
    )


def compute_backtest_performance_metrics(day_results: list[Mapping[str, Any]]) -> dict[str, Any]:
    positions = _rows(day_results, "simulated_positions")
    closes = _rows(day_results, "simulated_position_closes")
    close_decisions = _rows(day_results, "simulated_close_decisions")
    attempts = _rows(day_results, "simulated_attempts")
    fills = _rows(day_results, "simulated_fills")
    execution_diagnostics = _rows(day_results, "execution_diagnostics")
    mark_rows = _rows(day_results, "simulated_position_marks")
    decisions = _rows(day_results, "decisions")
    admissions = _rows(day_results, "admissions")

    pnl = _pnl_from_rows(positions, closes)
    closed_pnls = [coerce_float(row.get("realized_pnl")) or 0.0 for row in closes]
    gross_profit = money_sum_float(value for value in closed_pnls if value > 0)
    gross_loss = abs(money_sum_float(value for value in closed_pnls if value < 0))
    win_count = sum(1 for value in closed_pnls if value > 0)
    loss_count = sum(1 for value in closed_pnls if value < 0)
    flat_count = sum(1 for value in closed_pnls if value == 0)
    selected_count = sum(1 for row in decisions if str(row.get("decision_state") or "") == "selected")
    approved_count = sum(1 for row in admissions if str(row.get("admission_state") or "") == "approved")
    risk_capital = money_sum_float(value for value in (_position_risk(position) for position in positions) if value is not None)
    quote_observation_count = len(execution_diagnostics) + len(mark_rows)
    quote_covered_count = sum(1 for row in execution_diagnostics if (coerce_int(row.get("quote_snapshot_count")) or 0) > 0)
    quote_covered_count += sum(1 for row in mark_rows if bool(row.get("quote_covered")))
    daily_pnl = _daily_pnl_series(day_results)
    standard_metrics, metrics_fidelity = _standard_metrics(daily_pnl, risk_capital)

    performance = {
        "return_on_risk": _rate(pnl["net_pnl"], risk_capital) if risk_capital > 0 else None,
        "win_rate": _rate(win_count, len(closed_pnls)),
        "profit_factor": None if gross_loss <= 0 else round(gross_profit / gross_loss, 4),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "max_drawdown": _max_drawdown(daily_pnl),
        "exposure_time_minutes": _exposure_minutes(positions),
        "win_count": win_count,
        "loss_count": loss_count,
        "flat_count": flat_count,
        "closed_trade_count": len(closed_pnls),
        "risk_capital": risk_capital,
        **standard_metrics,
    }
    return {
        "pnl": pnl,
        "performance": performance,
        "execution": {
            "attempt_count": len(attempts),
            "fill_count": len(fills),
            "fill_rate": _rate(len(fills), len(attempts)),
        },
        "selection": {
            "decision_count": len(decisions),
            "selected_count": selected_count,
            "selection_rate": _rate(selected_count, len(decisions)),
        },
        "admission": {
            "admission_count": len(admissions),
            "approved_count": approved_count,
            "admission_approval_rate": _rate(approved_count, len(admissions)),
        },
        "exits": {
            "close_decision_count": len(close_decisions),
            "close_count": len(closes),
            "close_decision_to_close_rate": _rate(len(closes), len(close_decisions)),
        },
        "market_data": {
            "quote_observation_count": quote_observation_count,
            "quote_covered_count": quote_covered_count,
            "quote_coverage": _rate(quote_covered_count, quote_observation_count),
        },
        "fidelity": metrics_fidelity,
    }


__all__ = ["compute_backtest_performance_metrics"]

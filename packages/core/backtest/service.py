from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime
import os
from typing import Any

from core.backtest.market_data import (
    ALPACA_OPTIONS_HISTORY_START,
    estimate_structure_bar,
    merge_option_bars_with_trades,
)
from core.db.decorators import with_storage
from core.domain.backtest_models import (
    BacktestAggregate,
    BacktestFidelity,
    BacktestRun,
    BacktestSessionSummary,
    BacktestTarget,
    new_backtest_run_id,
)
from core.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from core.services.automation_runtime import resolve_entry_runtime
from core.services.bot_analytics import evaluate_entry_controls
from core.services.entry_planner import plan_entry_selection, score_opportunity
from core.services.exit_manager import resolve_exit_policy_snapshot
from core.services.management_recipes import (
    build_exit_policy_from_recipe_refs,
    evaluate_management_recipes,
)
from core.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    structure_quote_snapshot,
    unique_leg_symbols,
)
from core.storage.run_history_repository import session_bounds
from core.storage.serializers import parse_datetime


ENGINE_NAME = "backtest"
ENGINE_VERSION = "v1"

_FIDELITY_RANK = {
    "high": 0,
    "medium": 1,
    "reduced": 2,
    "unsupported": 3,
}


def _scope_key(bot_id: str, automation_id: str, session_date: str) -> str:
    return f"entry:{bot_id}:{automation_id}:{session_date}"


def _coerce_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sorted_opportunities(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = [dict(row) for row in rows]
    sorted_rows.sort(
        key=lambda row: (
            -score_opportunity(row),
            int(row.get("selection_rank") or 999999),
            str(row.get("opportunity_id") or ""),
        )
    )
    return sorted_rows


def _opportunity_economics(opportunity: dict[str, Any]) -> dict[str, Any]:
    economics = opportunity.get("economics")
    return dict(economics) if isinstance(economics, dict) else dict(opportunity)


def _lifecycle_pnl(
    *, strategy_family: Any, entry_value: float, exit_value: float
) -> float:
    if net_premium_kind(strategy_family) == "debit":
        return round((exit_value - entry_value) * 100.0, 2)
    return round((entry_value - exit_value) * 100.0, 2)


def _simulated_position_payload(
    *,
    runtime: Any,
    opportunity: dict[str, Any],
    fill_price: float,
    width: float | None,
    max_loss: float | None,
    session_date: str,
) -> dict[str, Any]:
    return {
        "underlying_symbol": opportunity.get("underlying_symbol"),
        "strategy_family": opportunity.get("strategy_family"),
        "strategy": opportunity.get("strategy_family"),
        "session_date": session_date,
        "market_date": session_date,
        "entry_credit": round(fill_price, 4),
        "entry_value": round(fill_price, 4),
        "entry_value_kind": net_premium_kind(opportunity.get("strategy_family")),
        "remaining_quantity": 1.0,
        "width": None if width is None or width <= 0 else width,
        "max_loss": None if max_loss is None or max_loss <= 0 else round(max_loss, 2),
        "risk_policy": {},
        "exit_policy": resolve_exit_policy_snapshot(
            session_date=session_date,
            payload={
                "exit_policy": build_exit_policy_from_recipe_refs(
                    tuple(runtime.automation.strategy_config.management_recipe_refs)
                )
            },
        ),
    }


def _simulate_entry_execution(
    *,
    runtime: Any,
    opportunity: dict[str, Any] | None,
    session_date: str,
) -> dict[str, Any] | None:
    if opportunity is None:
        return None
    economics = _opportunity_economics(opportunity)
    midpoint_credit = _coerce_float(economics.get("midpoint_credit"))
    natural_credit = _coerce_float(economics.get("natural_credit"))
    fill_ratio = _coerce_float(economics.get("fill_ratio"))
    width = _coerce_float(economics.get("width") or opportunity.get("width"))
    if midpoint_credit <= 0 and natural_credit <= 0:
        return {
            "intent_state": "rejected",
            "fill_state": "no_price",
            "filled": False,
            "position": None,
        }
    if fill_ratio >= 0.7 and midpoint_credit > 0:
        fill_price = midpoint_credit
        fill_source = "midpoint"
        fill_state = "filled"
    elif fill_ratio >= 0.5 and natural_credit > 0:
        fill_price = natural_credit
        fill_source = "natural"
        fill_state = "filled"
    else:
        fill_price = 0.0
        fill_source = None
        fill_state = "unfilled"
    if fill_state != "filled":
        return {
            "intent_state": "submitted",
            "fill_state": fill_state,
            "filled": False,
            "position": None,
        }
    max_loss = _coerce_float(economics.get("max_loss"))
    return {
        "intent_state": "filled",
        "fill_state": fill_state,
        "filled": True,
        "fill_price": round(fill_price, 4),
        "fill_source": fill_source,
        "position": _simulated_position_payload(
            runtime=runtime,
            opportunity=opportunity,
            fill_price=fill_price,
            width=None if width <= 0 else width,
            max_loss=None if max_loss <= 0 else max_loss,
            session_date=session_date,
        ),
    }


def _build_lifecycle_result(**overrides: Any) -> dict[str, Any]:
    payload = {
        "position_state": "open",
        "exit_state": "open",
        "exit_reason": None,
        "exit_recipe_ref": None,
        "exit_at": None,
        "exit_fill_price": None,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "final_close_mark": None,
        "quote_event_count": 0,
        "trade_event_count": 0,
        "snapshot_count": 0,
        "mark_source": None,
        "fidelity": "unsupported",
        "fidelity_reason": None,
        "fidelity_sources": [],
    }
    payload.update(overrides)
    payload["fidelity_sources"] = [
        str(value)
        for value in list(payload.get("fidelity_sources") or [])
        if str(value).strip()
    ]
    return payload


def _history_store_ready(history_store: Any) -> bool:
    if history_store is None:
        return False
    if hasattr(history_store, "schema_ready"):
        return bool(history_store.schema_ready())
    return True


def _structure_marks_from_quote_rows(
    *,
    opportunity: dict[str, Any],
    quote_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    opening_legs = candidate_legs(opportunity)
    quotes_by_symbol: dict[str, dict[str, Any]] = {}
    sources_by_symbol: dict[str, str] = {}
    marks: list[dict[str, Any]] = []
    for row in quote_rows:
        symbol = str(row.get("option_symbol") or "").strip()
        if not symbol:
            continue
        quotes_by_symbol[symbol] = {
            "bid": row.get("bid"),
            "ask": row.get("ask"),
            "midpoint": row.get("midpoint"),
            "captured_at": row.get("captured_at"),
            "timestamp": row.get("captured_at"),
        }
        if row.get("source") not in (None, ""):
            sources_by_symbol[symbol] = str(row.get("source"))
        snapshot = structure_quote_snapshot(
            legs=opening_legs,
            strategy_family=opportunity.get("strategy_family"),
            quotes_by_symbol=quotes_by_symbol,
            sources_by_symbol=sources_by_symbol,
        )
        if snapshot is None:
            continue
        marks.append(
            {
                "captured_at": snapshot.get("captured_at"),
                "close_mark": snapshot.get("close_mark"),
                "source": "recorded_quotes",
            }
        )
    return marks


def _structure_marks_from_trade_rows(
    *,
    opportunity: dict[str, Any],
    trade_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    opening_legs = candidate_legs(opportunity)
    quotes_by_symbol: dict[str, dict[str, Any]] = {}
    sources_by_symbol: dict[str, str] = {}
    marks: list[dict[str, Any]] = []
    for row in trade_rows:
        symbol = str(row.get("option_symbol") or "").strip()
        price = _coerce_float(row.get("price"))
        if not symbol or price <= 0:
            continue
        quotes_by_symbol[symbol] = {
            "bid": price,
            "ask": price,
            "midpoint": price,
            "captured_at": row.get("captured_at"),
            "timestamp": row.get("captured_at"),
        }
        if row.get("source") not in (None, ""):
            sources_by_symbol[symbol] = str(row.get("source"))
        snapshot = structure_quote_snapshot(
            legs=opening_legs,
            strategy_family=opportunity.get("strategy_family"),
            quotes_by_symbol=quotes_by_symbol,
            sources_by_symbol=sources_by_symbol,
        )
        if snapshot is None:
            continue
        marks.append(
            {
                "captured_at": snapshot.get("captured_at"),
                "close_mark": snapshot.get("close_mark"),
                "source": "recorded_trades",
            }
        )
    return marks


def _evaluate_structure_marks(
    *,
    runtime: Any,
    opportunity: dict[str, Any],
    entry_execution: dict[str, Any],
    marks: list[dict[str, Any]],
    session_end: datetime,
    mark_source: str,
    fidelity: BacktestFidelity,
    fidelity_reason: str,
    fidelity_sources: list[str],
    quote_event_count: int = 0,
    trade_event_count: int = 0,
) -> dict[str, Any]:
    simulated_position = dict(entry_execution["position"])
    recipe_refs = tuple(runtime.automation.strategy_config.management_recipe_refs)
    latest_mark: dict[str, Any] | None = None
    snapshot_count = 0
    for snapshot in marks:
        close_mark = snapshot.get("close_mark")
        captured_at = snapshot.get("captured_at")
        if close_mark is None:
            continue
        latest_mark = dict(snapshot)
        snapshot_count += 1
        simulated_position["close_mark"] = close_mark
        simulated_position["close_marked_at"] = captured_at
        decision = evaluate_management_recipes(
            recipe_refs,
            position=simulated_position,
            mark=_coerce_float(close_mark),
            now=parse_datetime(captured_at) or session_end,
        )
        if decision.should_close:
            exit_fill_price = (
                decision.limit_price
                if decision.limit_price is not None
                else _coerce_float(close_mark)
            )
            return _build_lifecycle_result(
                position_state="closed",
                exit_state="closed",
                exit_reason=decision.reason,
                exit_recipe_ref=decision.recipe_ref,
                exit_at=captured_at,
                exit_fill_price=None
                if exit_fill_price <= 0
                else round(exit_fill_price, 4),
                realized_pnl=_lifecycle_pnl(
                    strategy_family=opportunity.get("strategy_family"),
                    entry_value=float(entry_execution["fill_price"]),
                    exit_value=exit_fill_price,
                ),
                unrealized_pnl=0.0,
                final_close_mark=close_mark,
                quote_event_count=quote_event_count,
                trade_event_count=trade_event_count,
                snapshot_count=snapshot_count,
                mark_source=mark_source,
                fidelity=fidelity,
                fidelity_reason=fidelity_reason,
                fidelity_sources=fidelity_sources,
            )

    if latest_mark is None:
        return _build_lifecycle_result(
            position_state="open",
            exit_state="no_market_data",
            exit_reason="no_market_data",
            quote_event_count=quote_event_count,
            trade_event_count=trade_event_count,
            snapshot_count=snapshot_count,
            mark_source=mark_source,
            fidelity=fidelity,
            fidelity_reason=fidelity_reason,
            fidelity_sources=fidelity_sources,
        )

    simulated_position["close_mark"] = latest_mark.get("close_mark")
    simulated_position["close_marked_at"] = latest_mark.get("captured_at")
    forced_now = (
        parse_datetime(simulated_position.get("exit_policy", {}).get("force_close_at"))
        or session_end
    )
    final_decision = evaluate_management_recipes(
        recipe_refs,
        position=simulated_position,
        mark=_coerce_float(latest_mark.get("close_mark")),
        now=forced_now,
    )
    if final_decision.should_close:
        exit_fill_price = (
            final_decision.limit_price
            if final_decision.limit_price is not None
            else _coerce_float(latest_mark.get("close_mark"))
        )
        return _build_lifecycle_result(
            position_state="closed",
            exit_state="closed",
            exit_reason=final_decision.reason,
            exit_recipe_ref=final_decision.recipe_ref,
            exit_at=latest_mark.get("captured_at") or forced_now.isoformat(),
            exit_fill_price=None
            if exit_fill_price <= 0
            else round(exit_fill_price, 4),
            realized_pnl=_lifecycle_pnl(
                strategy_family=opportunity.get("strategy_family"),
                entry_value=float(entry_execution["fill_price"]),
                exit_value=exit_fill_price,
            ),
            unrealized_pnl=0.0,
            final_close_mark=latest_mark.get("close_mark"),
            quote_event_count=quote_event_count,
            trade_event_count=trade_event_count,
            snapshot_count=snapshot_count,
            mark_source=mark_source,
            fidelity=fidelity,
            fidelity_reason=fidelity_reason,
            fidelity_sources=fidelity_sources,
        )

    final_close_mark = _coerce_float(latest_mark.get("close_mark"))
    return _build_lifecycle_result(
        position_state="open",
        exit_state="open",
        exit_reason=str(final_decision.reason),
        exit_recipe_ref=final_decision.recipe_ref,
        exit_at=latest_mark.get("captured_at"),
        exit_fill_price=None,
        realized_pnl=0.0,
        unrealized_pnl=_lifecycle_pnl(
            strategy_family=opportunity.get("strategy_family"),
            entry_value=float(entry_execution["fill_price"]),
            exit_value=final_close_mark,
        )
        if final_close_mark > 0
        else 0.0,
        final_close_mark=latest_mark.get("close_mark"),
        quote_event_count=quote_event_count,
        trade_event_count=trade_event_count,
        snapshot_count=snapshot_count,
        mark_source=mark_source,
        fidelity=fidelity,
        fidelity_reason=fidelity_reason,
        fidelity_sources=fidelity_sources,
    )


def _build_alpaca_client() -> AlpacaClient | None:
    key_id = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY")
    if not key_id or not secret_key:
        return None
    return AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, None),
        data_base_url="https://data.alpaca.markets",
    )


def _alpaca_daily_marks(
    *,
    opportunity: dict[str, Any],
    session_date: str,
    target_date: str,
    alpaca_client: AlpacaClient,
    cache: dict[tuple[tuple[str, ...], str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], str] | None:
    opening_legs = candidate_legs(opportunity)
    option_symbols = tuple(unique_leg_symbols(opening_legs))
    if not option_symbols:
        return None
    cache_key = (option_symbols, session_date, target_date)
    cached = cache.get(cache_key)
    if cached is None:
        option_bars = alpaca_client.get_option_bars(
            list(option_symbols),
            start=session_date,
            end=target_date,
        )
        option_trades = alpaca_client.get_option_trades(
            list(option_symbols),
            start=session_date,
            end=target_date,
        )
        cached = {
            "bars": option_bars,
            "trades": option_trades,
            "merged": merge_option_bars_with_trades(
                bars_by_symbol=option_bars,
                trades_by_symbol=option_trades,
            ),
        }
        cache[cache_key] = cached

    merged_bars = dict(cached.get("merged") or {})
    direct_bars = dict(cached.get("bars") or {})
    bars_by_date = {
        symbol: {
            datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date(): bar
            for bar in list(bars)
        }
        for symbol, bars in merged_bars.items()
    }
    if not bars_by_date:
        return None
    date_sets = [set(values) for values in bars_by_date.values() if values]
    if not date_sets:
        return None
    start_on = date.fromisoformat(session_date)
    end_on = date.fromisoformat(target_date)
    path_dates = sorted(
        value for value in set.intersection(*date_sets) if start_on <= value <= end_on
    )
    if not path_dates:
        return None

    mark_source = (
        "alpaca_bars"
        if all(
            value in {
                datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date()
                for bar in list(direct_bars.get(symbol, []))
            }
            for symbol in option_symbols
            for value in path_dates
        )
        else "alpaca_mixed"
    )
    marks: list[dict[str, Any]] = []
    for path_date in path_dates:
        daily_bars = {
            symbol: symbol_bars[path_date]
            for symbol, symbol_bars in bars_by_date.items()
            if path_date in symbol_bars
        }
        spread_bar = estimate_structure_bar(
            legs=opening_legs,
            bars_by_symbol=daily_bars,
            strategy=str(opportunity.get("strategy_family") or ""),
        )
        if spread_bar is None:
            continue
        marks.append(
            {
                "captured_at": max(
                    bar.timestamp for bar in daily_bars.values() if bar.timestamp
                ),
                "close_mark": spread_bar.get("close"),
                "source": mark_source,
            }
        )
    return marks, mark_source


def _simulate_position_lifecycle(
    *,
    history_store: Any,
    runtime: Any,
    opportunity: dict[str, Any] | None,
    entry_execution: dict[str, Any] | None,
    started_at: str,
    session_date: str,
    end_date: str | None,
    alpaca_client: AlpacaClient | None,
    alpaca_cache: dict[tuple[tuple[str, ...], str, str], dict[str, Any]],
) -> dict[str, Any]:
    if (
        opportunity is None
        or entry_execution is None
        or not entry_execution.get("filled")
        or not isinstance(entry_execution.get("position"), dict)
    ):
        return _build_lifecycle_result(
            position_state="not_opened",
            exit_state="not_opened",
            fidelity="unsupported",
            fidelity_reason="position_not_opened",
        )

    opening_legs = candidate_legs(opportunity)
    option_symbols = unique_leg_symbols(opening_legs)
    if not option_symbols:
        return _build_lifecycle_result(
            exit_state="missing_legs",
            exit_reason="missing_legs",
            fidelity="unsupported",
            fidelity_reason="missing_structure_legs",
        )

    target_date = end_date or session_date
    _, session_end = session_bounds(target_date)

    quote_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    if _history_store_ready(history_store):
        if hasattr(history_store, "list_option_quote_events_window"):
            quote_rows = [
                dict(row)
                for row in history_store.list_option_quote_events_window(
                    option_symbols=option_symbols,
                    captured_from=started_at,
                    captured_to=session_end,
                )
            ]
        if hasattr(history_store, "list_option_trade_events_window"):
            trade_rows = [
                dict(row)
                for row in history_store.list_option_trade_events_window(
                    option_symbols=option_symbols,
                    captured_from=started_at,
                    captured_to=session_end,
                )
            ]

    if quote_rows:
        return _evaluate_structure_marks(
            runtime=runtime,
            opportunity=opportunity,
            entry_execution=entry_execution,
            marks=_structure_marks_from_quote_rows(
                opportunity=opportunity,
                quote_rows=quote_rows,
            ),
            session_end=session_end,
            mark_source="recorded_quotes",
            fidelity="high",
            fidelity_reason="repo_recorded_quote_window",
            fidelity_sources=["recorded_quotes"],
            quote_event_count=len(quote_rows),
            trade_event_count=len(trade_rows),
        )

    if trade_rows:
        return _evaluate_structure_marks(
            runtime=runtime,
            opportunity=opportunity,
            entry_execution=entry_execution,
            marks=_structure_marks_from_trade_rows(
                opportunity=opportunity,
                trade_rows=trade_rows,
            ),
            session_end=session_end,
            mark_source="recorded_trades",
            fidelity="high",
            fidelity_reason="repo_recorded_trade_window",
            fidelity_sources=["recorded_trades"],
            quote_event_count=0,
            trade_event_count=len(trade_rows),
        )

    session_day = date.fromisoformat(session_date)
    if session_day < ALPACA_OPTIONS_HISTORY_START:
        return _build_lifecycle_result(
            exit_state="unsupported",
            exit_reason="alpaca_history_unsupported_before_2024_02_01",
            mark_source="unsupported",
            fidelity="unsupported",
            fidelity_reason="pre_2024_02_01_requires_recorded_repo_data",
            fidelity_sources=[],
        )

    if alpaca_client is not None:
        try:
            alpaca_marks = _alpaca_daily_marks(
                opportunity=opportunity,
                session_date=session_date,
                target_date=target_date,
                alpaca_client=alpaca_client,
                cache=alpaca_cache,
            )
        except Exception:
            alpaca_marks = None
        if alpaca_marks is not None:
            marks, mark_source = alpaca_marks
            if marks:
                return _evaluate_structure_marks(
                    runtime=runtime,
                    opportunity=opportunity,
                    entry_execution=entry_execution,
                    marks=marks,
                    session_end=session_end,
                    mark_source=mark_source,
                    fidelity="medium",
                    fidelity_reason="alpaca_option_history",
                    fidelity_sources=["alpaca_bars", "alpaca_trades"]
                    if mark_source == "alpaca_mixed"
                    else ["alpaca_bars"],
                )

    fill_price = _coerce_float(entry_execution.get("fill_price"))
    return _build_lifecycle_result(
        position_state="open",
        exit_state="synthetic_fallback",
        exit_reason=(
            "alpaca_history_unavailable"
            if alpaca_client is not None
            else "alpaca_client_unavailable"
        ),
        final_close_mark=None if fill_price <= 0 else round(fill_price, 4),
        mark_source="synthetic_midpoint",
        fidelity="reduced",
        fidelity_reason="synthetic_midpoint_fallback",
        fidelity_sources=["synthetic_midpoint"],
    )


def compare_backtest_runs(
    *,
    left_run: BacktestRun,
    right_run: BacktestRun,
) -> BacktestRun:
    left_target = BacktestTarget() if left_run.target is None else left_run.target
    right_target = BacktestTarget() if right_run.target is None else right_run.target
    left_aggregate = (
        {} if left_run.aggregate is None else left_run.aggregate.to_payload()
    )
    right_aggregate = (
        {} if right_run.aggregate is None else right_run.aggregate.to_payload()
    )
    metric_keys = [
        "session_count",
        "modeled_selected_count",
        "modeled_fill_count",
        "modeled_position_count",
        "modeled_closed_count",
        "modeled_open_position_count",
        "actual_selected_count",
        "matched_selection_count",
        "selection_match_rate",
        "modeled_fill_rate",
        "actual_fill_rate",
        "modeled_realized_pnl",
        "modeled_unrealized_pnl",
        "position_count",
        "realized_pnl",
        "unrealized_pnl",
    ]
    metrics: dict[str, dict[str, Any]] = {}
    for key in metric_keys:
        left_value = left_aggregate.get(key)
        right_value = right_aggregate.get(key)
        metrics[key] = {
            "left": left_value,
            "right": right_value,
            "delta": (
                None
                if left_value is None or right_value is None
                else round(_coerce_float(left_value) - _coerce_float(right_value), 4)
            ),
        }
    metrics["fidelity"] = {
        "left": left_aggregate.get("fidelity"),
        "right": right_aggregate.get("fidelity"),
        "delta": None,
    }
    started_at = datetime.now(UTC)
    return BacktestRun(
        id=new_backtest_run_id("compare"),
        kind="compare",
        status="completed",
        engine_name=ENGINE_NAME,
        engine_version=ENGINE_VERSION,
        created_at=started_at,
        started_at=started_at,
        completed_at=started_at,
        left_target=left_target,
        right_target=right_target,
        comparison_metrics=metrics,
        left_run_id=left_run.id,
        right_run_id=right_run.id,
    )


def _latest_runs_by_session(
    runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_session: dict[str, dict[str, Any]] = {}
    for row in runs:
        session_date = str(row.get("session_date") or "")
        if not session_date:
            continue
        current = latest_by_session.get(session_date)
        if current is None or str(row.get("started_at") or "") > str(
            current.get("started_at") or ""
        ):
            latest_by_session[session_date] = dict(row)
    return [latest_by_session[key] for key in sorted(latest_by_session)]


def _run_fidelity(sessions: list[BacktestSessionSummary]) -> tuple[BacktestFidelity, str, dict[str, int]]:
    if not sessions:
        return "unsupported", "no_sessions", {}
    counts = Counter(session.fidelity for session in sessions)
    worst = max(counts, key=lambda key: _FIDELITY_RANK.get(key, 99))
    reason = ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
    return worst, reason, dict(sorted(counts.items()))


@with_storage()
def build_backtest_run(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 200,
    storage: Any | None = None,
) -> BacktestRun:
    started_at = datetime.now(UTC)
    runtime = resolve_entry_runtime(bot_id=bot_id, automation_id=automation_id)
    signal_store = storage.signals
    execution_store = storage.execution
    history_store = storage.history
    alpaca_client = _build_alpaca_client()
    alpaca_cache: dict[tuple[tuple[str, ...], str, str], dict[str, Any]] = {}

    runs = _latest_runs_by_session(
        [
            dict(row)
            for row in signal_store.list_automation_runs(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                start_date=start_date,
                end_date=end_date,
                limit=max(int(limit), 1) * 4,
            )
        ]
    )[: max(int(limit), 1)]

    sessions: list[BacktestSessionSummary] = []
    matched_selection_count = 0
    modeled_selected_count = 0
    actual_selected_count = 0
    modeled_fill_count = 0
    modeled_position_count = 0
    modeled_closed_count = 0
    modeled_open_position_count = 0
    actual_fill_count = 0
    total_position_count = 0
    total_modeled_realized_pnl = 0.0
    total_modeled_unrealized_pnl = 0.0
    total_realized_pnl = 0.0
    total_unrealized_pnl = 0.0

    for run in runs:
        session_date = str(run["session_date"])
        opportunities = _sorted_opportunities(
            [
                dict(row)
                for row in signal_store.list_opportunities(
                    bot_id=runtime.bot_id,
                    automation_id=runtime.automation_id,
                    automation_run_id=str(run["automation_run_id"]),
                    runtime_owned=True,
                    limit=500,
                )
            ]
        )
        controls_allowed, controls_reason, bot_metrics = evaluate_entry_controls(
            storage=storage,
            bot=runtime.bot.bot,
            market_date=session_date,
        )
        plan = plan_entry_selection(
            opportunities=opportunities,
            controls_allowed=controls_allowed,
            controls_reason=controls_reason,
            bot_metrics=bot_metrics,
            min_score=float(runtime.trigger_policy.get("min_opportunity_score") or 0.0),
        )
        modeled_selected = plan.get("selected")
        modeled_selected_id = (
            None
            if modeled_selected is None
            else str(modeled_selected.get("opportunity_id"))
        )
        if modeled_selected_id is not None:
            modeled_selected_count += 1
        modeled_execution = _simulate_entry_execution(
            runtime=runtime,
            opportunity=modeled_selected,
            session_date=session_date,
        )
        if modeled_execution and modeled_execution.get("filled"):
            modeled_fill_count += 1
            modeled_position_count += 1
        modeled_lifecycle = _simulate_position_lifecycle(
            history_store=history_store,
            runtime=runtime,
            opportunity=modeled_selected,
            entry_execution=modeled_execution,
            started_at=str(run.get("started_at") or session_date),
            session_date=session_date,
            end_date=end_date,
            alpaca_client=alpaca_client,
            alpaca_cache=alpaca_cache,
        )
        if str(modeled_lifecycle.get("position_state") or "") == "closed":
            modeled_closed_count += 1
        elif modeled_execution and modeled_execution.get("filled"):
            modeled_open_position_count += 1
        total_modeled_realized_pnl += _coerce_float(
            modeled_lifecycle.get("realized_pnl")
        )
        total_modeled_unrealized_pnl += _coerce_float(
            modeled_lifecycle.get("unrealized_pnl")
        )

        actual_decisions = [
            dict(row)
            for row in signal_store.list_opportunity_decisions(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                scope_key=_scope_key(
                    runtime.bot_id, runtime.automation_id, session_date
                ),
                limit=500,
            )
        ]
        actual_selected_decision = next(
            (
                row
                for row in actual_decisions
                if str(row.get("state") or "") == "selected"
            ),
            None,
        )
        actual_selected_id = (
            None
            if actual_selected_decision is None
            else str(actual_selected_decision.get("opportunity_id"))
        )
        if actual_selected_id is not None:
            actual_selected_count += 1
        if (
            modeled_selected_id is not None
            and modeled_selected_id == actual_selected_id
        ):
            matched_selection_count += 1

        selected_intents = []
        if actual_selected_decision is not None:
            selected_intents = [
                dict(row)
                for row in execution_store.list_execution_intents(
                    bot_id=runtime.bot_id,
                    automation_id=runtime.automation_id,
                    opportunity_decision_id=str(
                        actual_selected_decision["opportunity_decision_id"]
                    ),
                    limit=50,
                )
            ]
        if selected_intents:
            actual_fill_count += len(
                [
                    row
                    for row in selected_intents
                    if str(row.get("state") or "")
                    in {"submitted", "filled", "completed", "partially_filled"}
                ]
            )

        positions = [
            dict(row)
            for row in execution_store.list_positions(
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                market_date=session_date,
                limit=200,
            )
        ]
        realized_pnl = sum(_coerce_float(row.get("realized_pnl")) for row in positions)
        unrealized_pnl = sum(
            _coerce_float(row.get("unrealized_pnl")) for row in positions
        )
        total_position_count += len(positions)
        total_realized_pnl += realized_pnl
        total_unrealized_pnl += unrealized_pnl

        sessions.append(
            BacktestSessionSummary(
                session_date=session_date,
                automation_run_id=str(run["automation_run_id"]),
                fidelity=str(modeled_lifecycle.get("fidelity") or "unsupported"),
                fidelity_reason=modeled_lifecycle.get("fidelity_reason"),
                fidelity_sources=[
                    str(value)
                    for value in list(modeled_lifecycle.get("fidelity_sources") or [])
                ],
                opportunity_count=len(opportunities),
                modeled_selected_opportunity_id=modeled_selected_id,
                actual_selected_opportunity_id=actual_selected_id,
                selection_match=(
                    modeled_selected_id == actual_selected_id
                    if modeled_selected_id is not None
                    and actual_selected_id is not None
                    else None
                ),
                controls_allowed=controls_allowed,
                controls_reason=controls_reason,
                modeled_intent_state=(
                    None
                    if modeled_execution is None
                    else modeled_execution.get("intent_state")
                ),
                modeled_fill_state=(
                    None
                    if modeled_execution is None
                    else modeled_execution.get("fill_state")
                ),
                modeled_fill_price=(
                    None
                    if modeled_execution is None
                    else modeled_execution.get("fill_price")
                ),
                modeled_position=(
                    {}
                    if modeled_execution is None
                    else dict(modeled_execution.get("position") or {})
                ),
                modeled_exit_state=modeled_lifecycle.get("exit_state"),
                modeled_exit_reason=modeled_lifecycle.get("exit_reason"),
                modeled_exit_recipe_ref=modeled_lifecycle.get("exit_recipe_ref"),
                modeled_exit_at=modeled_lifecycle.get("exit_at"),
                modeled_exit_fill_price=modeled_lifecycle.get("exit_fill_price"),
                modeled_realized_pnl=modeled_lifecycle.get("realized_pnl"),
                modeled_unrealized_pnl=modeled_lifecycle.get("unrealized_pnl"),
                modeled_final_close_mark=modeled_lifecycle.get("final_close_mark"),
                modeled_quote_event_count=int(
                    modeled_lifecycle.get("quote_event_count") or 0
                ),
                modeled_trade_event_count=int(
                    modeled_lifecycle.get("trade_event_count") or 0
                ),
                modeled_snapshot_count=int(
                    modeled_lifecycle.get("snapshot_count") or 0
                ),
                modeled_mark_source=modeled_lifecycle.get("mark_source"),
                selected_intent_count=len(selected_intents),
                position_count=len(positions),
                realized_pnl=round(realized_pnl, 2),
                unrealized_pnl=round(unrealized_pnl, 2),
                top_opportunities=[
                    {
                        "opportunity_id": str(row.get("opportunity_id")),
                        "underlying_symbol": row.get("underlying_symbol"),
                        "score": score_opportunity(row),
                        "selection_rank": row.get("selection_rank"),
                        "midpoint_credit": row.get("economics", {}).get(
                            "midpoint_credit"
                        )
                        if isinstance(row.get("economics"), dict)
                        else row.get("midpoint_credit"),
                    }
                    for row in opportunities[:5]
                ],
            )
        )

    run_fidelity, run_fidelity_reason, fidelity_counts = _run_fidelity(sessions)
    completed_at = datetime.now(UTC)
    return BacktestRun(
        id=new_backtest_run_id("run"),
        kind="run",
        status="completed",
        engine_name=ENGINE_NAME,
        engine_version=ENGINE_VERSION,
        created_at=started_at,
        started_at=started_at,
        completed_at=completed_at,
        target=BacktestTarget(
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            strategy_config_id=runtime.strategy_config_id,
            strategy_id=runtime.strategy_id,
            config_hash=runtime.config_hash,
            start_date=(None if start_date is None else date.fromisoformat(start_date)),
            end_date=(None if end_date is None else date.fromisoformat(end_date)),
            session_limit=max(int(limit), 1),
        ),
        aggregate=BacktestAggregate(
            session_count=len(sessions),
            fidelity=run_fidelity,
            fidelity_reason=run_fidelity_reason,
            fidelity_counts=fidelity_counts,
            modeled_selected_count=modeled_selected_count,
            modeled_fill_count=modeled_fill_count,
            modeled_position_count=modeled_position_count,
            modeled_closed_count=modeled_closed_count,
            modeled_open_position_count=modeled_open_position_count,
            actual_selected_count=actual_selected_count,
            matched_selection_count=matched_selection_count,
            selection_match_rate=(
                None
                if actual_selected_count == 0
                else round(matched_selection_count / actual_selected_count, 4)
            ),
            modeled_fill_rate=(
                None
                if modeled_selected_count == 0
                else round(modeled_fill_count / modeled_selected_count, 4)
            ),
            actual_fill_rate=(
                None
                if actual_selected_count == 0
                else round(actual_fill_count / actual_selected_count, 4)
            ),
            modeled_realized_pnl=round(total_modeled_realized_pnl, 2),
            modeled_unrealized_pnl=round(total_modeled_unrealized_pnl, 2),
            position_count=total_position_count,
            realized_pnl=round(total_realized_pnl, 2),
            unrealized_pnl=round(total_unrealized_pnl, 2),
        ),
        sessions=sessions,
        params={
            "bot_id": runtime.bot_id,
            "automation_id": runtime.automation_id,
            "start_date": start_date,
            "end_date": end_date,
            "limit": max(int(limit), 1),
        },
        coverage={
            "priority_ladder": [
                "repo_recorded_option_quote_trade_windows",
                "alpaca_historical_option_trades_and_bars",
                "synthetic_midpoint_fallback",
            ],
            "alpaca_options_history_supported_from": ALPACA_OPTIONS_HISTORY_START.isoformat(),
            "run_fidelity": run_fidelity,
            "fidelity_counts": fidelity_counts,
        },
    )


__all__ = ["build_backtest_run", "compare_backtest_runs"]

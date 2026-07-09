from __future__ import annotations

from typing import Any

from core.services.trading_engine.data import ResolvedTickerSet
from core.services.trading_engine.data_runtime import (
    entry_engine_label,
    entry_engine_strategy_run_id,
)
from core.services.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.entry_signals import (
    NATURAL_ENTRY_PROVENANCE,
    OBSERVATION_ENTRY_PROVENANCE,
    candidate_payload,
)
from core.services.trading_engine.facts import entry_trade_signal_id
from core.services.trading_strategy_runtime_models import EntryRuntime
from core.value_coercion import utc_now


def _market_date_today() -> str:
    return utc_now().date().isoformat()


def _intent_id(trade_decision_id: str) -> str:
    return f"execution_intent:{trade_decision_id}"


def _slot_key(trading_strategy_id: str, underlying_symbol: str) -> str:
    return f"entry:{trading_strategy_id}:{underlying_symbol}"


def _entry_candidate_limit(runtime: EntryRuntime) -> int:
    max_symbols = runtime.strategy.source.max_symbols
    if max_symbols is not None:
        return max(int(max_symbols), 1)
    return 10


def _ticker_set_summary(ticker_set: ResolvedTickerSet) -> dict[str, Any]:
    evidence = dict(ticker_set.evidence or {})
    summary = evidence.get("summary") if isinstance(evidence.get("summary"), dict) else {}
    degradation = evidence.get("degradation") if isinstance(evidence.get("degradation"), dict) else {}
    return {
        "ticker_source_kind": ticker_set.source.source_type,
        "ticker_source_id": ticker_set.source.ref,
        "ticker_source_run_id": ticker_set.ticker_source_run_id,
        "resolved_at": ticker_set.resolved_at.isoformat().replace("+00:00", "Z"),
        "symbol_count": len(ticker_set.symbols),
        "symbols": list(ticker_set.symbols),
        "reason_codes": list(ticker_set.reason_codes),
        "blockers": list(ticker_set.blockers),
        "summary": dict(summary),
        "degradation": dict(degradation),
    }


def _trade_signal_refs(candidate_generation: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    engine_facts = candidate_generation.get("engine_facts") if isinstance(candidate_generation.get("engine_facts"), dict) else {}
    refs = engine_facts.get("trade_signals") if isinstance(engine_facts, dict) else None
    if not isinstance(refs, list):
        return ()
    return tuple(dict(ref) for ref in refs if isinstance(ref, dict))


def _trade_signal_id_for_signal(
    *,
    candidate_generation: dict[str, Any],
    runtime: EntryRuntime,
    market_date: str,
    signal: dict[str, Any],
) -> str | None:
    symbol = str(signal.get("underlying_symbol") or "").upper()
    candidate = candidate_payload(signal)
    candidate_identity = str(signal.get("candidate_identity") or resolve_candidate_identity(candidate, strategy=candidate.get("strategy"))).strip()
    for ref in _trade_signal_refs(candidate_generation):
        if (
            symbol
            and candidate_identity
            and str(ref.get("underlying_symbol") or "").upper() == symbol
            and str(ref.get("candidate_identity") or "") == candidate_identity
        ):
            return None if ref.get("trade_signal_id") in (None, "") else str(ref["trade_signal_id"])
    if symbol and candidate_identity:
        return entry_trade_signal_id(
            trading_strategy_id=runtime.trading_strategy_id,
            market_date=market_date,
            underlying_symbol=symbol,
            candidate_identity=candidate_identity,
        )
    return None


def _trade_decision_state(decision_state: Any) -> str:
    normalized = str(decision_state or "").strip().lower()
    if normalized == "selected":
        return "selected"
    if normalized == "blocked":
        return "skip"
    return "no_entry"


def _record_skipped_strategy_run(
    *,
    signal_store: Any,
    runtime: EntryRuntime,
    run_key: str,
    market_date: str,
    planner_job_run_id: str | None,
    generated_at: str,
    reason: str,
    ticker_set: ResolvedTickerSet,
    observation_only: bool = False,
) -> None:
    provenance = OBSERVATION_ENTRY_PROVENANCE if observation_only else NATURAL_ENTRY_PROVENANCE
    signal_store.upsert_strategy_run(
        strategy_run_id=entry_engine_strategy_run_id(run_key, runtime.trading_strategy_id),
        trading_strategy_id=runtime.trading_strategy_id,
        trigger_type="trading_strategy_entry",
        job_run_id=planner_job_run_id,
        cycle_id=run_key,
        label=entry_engine_label(runtime),
        session_date=market_date,
        started_at=generated_at,
        completed_at=generated_at,
        status="skipped",
        result={
            "entry_run_mode": "observation" if observation_only else "natural",
            "validation_provenance": provenance,
            "observation_only": observation_only,
            "reason": reason,
            "ticker_set": _ticker_set_summary(ticker_set),
            "candidate_count": 0,
            "signal_count": 0,
        },
        config_hash=runtime.config_hash,
    )


def _read_previous_entry_selection(
    *,
    signal_store: Any,
    runtime: EntryRuntime,
    session_date: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    previous_runs = [
        dict(row)
        for row in signal_store.list_strategy_runs(
            trading_strategy_id=runtime.trading_strategy_id,
            session_date=session_date,
            limit=1,
        )
    ]
    if not previous_runs:
        return {}, {}
    result_payload = previous_runs[0].get("result")
    if not isinstance(result_payload, dict):
        result_payload = previous_runs[0].get("result_json")
    if not isinstance(result_payload, dict):
        return {}, {}
    selection_memory = {
        str(symbol): dict(state)
        for symbol, state in dict(result_payload.get("selection_memory") or {}).items()
        if isinstance(symbol, str) and isinstance(state, dict)
    }
    previous_promotable: dict[str, dict[str, Any]] = {}
    for row in list(result_payload.get("selected_signal_rows") or []):
        if not isinstance(row, dict) or str(row.get("selection_state") or "") != "promotable":
            continue
        candidate = candidate_payload(row)
        symbol = str(row.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
        if symbol:
            previous_promotable[symbol] = candidate
    return previous_promotable, selection_memory


__all__ = [
    "_entry_candidate_limit",
    "_intent_id",
    "_market_date_today",
    "_read_previous_entry_selection",
    "_record_skipped_strategy_run",
    "_slot_key",
    "_ticker_set_summary",
    "_trade_decision_state",
    "_trade_signal_id_for_signal",
    "_trade_signal_refs",
]

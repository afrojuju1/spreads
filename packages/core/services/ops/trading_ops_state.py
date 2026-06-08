from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from core.db.decorators import with_storage
from core.jobs.orchestration import NEW_YORK
from core.jobs.specs import get_declared_job_row
from core.services.broker_sync import BROKER_SYNC_KEY
from core.services.control_plane import (
    get_control_state_snapshot,
    resolve_execution_kill_switch_reason,
)
from core.services.execution.shared import OPEN_STATUSES
from core.services.execution.runtimes import resolve_execution_runtime_capabilities
from core.services.execution_intents.shared import ACTIVE_INTENT_STATES, OPEN_POSITION_STATES
from core.services.execution_lifecycle import (
    is_open_execution_attempt_status,
    project_execution_attempt_lifecycle,
    resolve_execution_attempt_source_job,
    resolve_execution_submit_job_run_id,
)
from core.services.trading_engine.portfolio_runtime import describe_position_exit_state
from core.services.risk_manager import assess_position_risk
from core.services.trading_strategies import load_active_trading_strategies, routine_should_run_now
from core.storage.engine_models import CandidateRunModel, CandidateSymbolDiagnosticModel, TickerSourceObservationModel, TickerSourceRunModel
from core.storage.serializers import parse_date
from core.value_coercion import (
    as_list,
    as_mapping,
    as_text,
    coerce_float,
    coerce_int,
    utc_now_iso,
)

from .broker_sync import broker_sync_payload as _broker_sync_payload
from .engine import build_engine_ops_state
from .jobs import build_jobs_compact_state
from .market_session import market_session_context as _market_session_context
from .shared import (
    _attention,
    _combine_statuses,
    _control_status,
    _seconds_since,
    _sorted_by_activity,
)

OPEN_POSITION_STATUSES = sorted(OPEN_POSITION_STATES)
MARK_STALE_AFTER_SECONDS = 15 * 60
TOP_POSITION_LIMIT = 5
RECENT_ALERT_LIMIT = 200
SOURCE_SYMBOL_LIMIT = 25


@dataclass(frozen=True)
class _MarketControlProjection:
    market_date: str
    market_session: dict[str, Any]
    market_open: bool
    control: dict[str, Any]
    kill_switch_reason: str | None
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _JobsProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]
    details: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _AccountProjection:
    broker_sync_status: str
    broker_sync: dict[str, Any]
    account_snapshot: dict[str, Any]
    account: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _EngineProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]
    status: str
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _ExecutionProjection:
    open_execution_attempts: list[dict[str, Any]]
    summarized_open_execution_attempts: list[dict[str, Any]]
    stale_open_execution_count: int
    submit_unknown_execution_count: int
    capacity_blocked_underlyings: list[str]
    execution_health_status: str
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _PositionProjection:
    open_positions: list[dict[str, Any]]
    top_positions: list[dict[str, Any]]
    risk_breach_count: int
    reconciliation_mismatch_count: int
    missing_mark_count: int
    stale_mark_count: int
    broker_unquoted_positions: int
    mark_error: str | None
    mark_health_status: str
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _AlertProjection:
    alert_delivery: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _FlowProjection:
    trading_flows: list[dict[str, Any]]
    degraded_flows: list[dict[str, Any]]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


def _age_seconds(value: Any, *, now: datetime) -> float | None:
    age = _seconds_since(value, now=now)
    return None if age is None else round(age, 1)


def _alert_delivery_payload(
    rows: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    recent_rows = [
        row
        for row in rows
        if _seconds_since(row.get("updated_at") or row.get("created_at"), now=now) is not None
        and (_seconds_since(row.get("updated_at") or row.get("created_at"), now=now) or 0) <= 24 * 60 * 60
    ]
    counts = Counter(str(row.get("status") or "unknown") for row in recent_rows)
    status = "healthy"
    if counts.get("dead_letter", 0) or counts.get("retry_wait", 0):
        status = "degraded"
    return {
        "status": status,
        "count": len(recent_rows),
        "recent_count": len(recent_rows),
        "status_counts": dict(counts),
        "dead_letter_count": counts.get("dead_letter", 0),
        "retry_wait_count": counts.get("retry_wait", 0),
        "dispatching_count": counts.get("dispatching", 0),
        "pending_count": counts.get("pending", 0),
        "historical_status_counts": dict(Counter(str(row.get("status") or "unknown") for row in rows)),
    }


def _account_snapshot_payload(snapshot: Mapping[str, Any] | None) -> dict[str, Any]:
    if snapshot is None:
        return {
            "status": "missing",
            "source": None,
            "environment": None,
            "captured_at": None,
            "account": {},
            "pnl": {},
            "positions": [],
        }
    return {
        "status": "ready",
        "snapshot_id": snapshot.get("snapshot_id"),
        "broker": snapshot.get("broker"),
        "environment": snapshot.get("environment"),
        "source": "snapshot",
        "captured_at": snapshot.get("captured_at"),
        "account": dict(snapshot.get("account") or {}),
        "pnl": dict(snapshot.get("pnl") or {}),
        "positions": list(snapshot.get("positions") or []),
    }


def _top_positions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for row in rows:
        exposure = coerce_float(row.get("max_loss"))
        if exposure is None:
            exposure = coerce_float(row.get("entry_notional"))
        net_pnl = coerce_float(row.get("net_pnl"))
        ranked.append(
            {
                "position_id": row.get("position_id"),
                "underlying_symbol": row.get("underlying_symbol") or row.get("root_symbol"),
                "status": row.get("status") or row.get("position_status"),
                "exposure": 0.0 if exposure is None else round(abs(exposure), 2),
                "net_pnl": None if net_pnl is None else round(net_pnl, 2),
                "risk_status": row.get("risk_status"),
            }
        )
    ranked.sort(key=lambda row: float(row.get("exposure") or 0.0), reverse=True)
    return ranked[:TOP_POSITION_LIMIT]


def _load_execution_attempt_job_context(
    *,
    job_store: Any,
    attempts: list[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any] | None], dict[str, Mapping[str, Any] | None]]:
    submit_jobs: dict[str, Mapping[str, Any] | None] = {}
    source_definitions: dict[str, Mapping[str, Any] | None] = {}
    if job_store is None or (hasattr(job_store, "schema_ready") and not job_store.schema_ready()):
        return submit_jobs, source_definitions

    for attempt in attempts:
        execution_attempt_id = as_text(attempt.get("execution_attempt_id"))
        if execution_attempt_id is None:
            continue
        try:
            submit_jobs[execution_attempt_id] = job_store.get_job_run(resolve_execution_submit_job_run_id(execution_attempt_id))
        except Exception:
            submit_jobs[execution_attempt_id] = None

        source_job = resolve_execution_attempt_source_job(attempt)
        source_job_key = as_text(source_job.get("job_key"))
        if source_job_key is None or source_job_key in source_definitions:
            continue
        source_definitions[source_job_key] = get_declared_job_row(source_job_key)
    return submit_jobs, source_definitions


def _execution_attempt_lifecycle(
    *,
    attempt: Mapping[str, Any],
    now: datetime,
    submit_jobs: Mapping[str, Mapping[str, Any] | None],
    source_definitions: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Any]:
    if not is_open_execution_attempt_status(attempt.get("status")):
        return {}
    execution_attempt_id = as_text(attempt.get("execution_attempt_id")) or ""
    source_job = resolve_execution_attempt_source_job(attempt)
    source_job_key = as_text(source_job.get("job_key"))
    submit_job = submit_jobs.get(execution_attempt_id)
    source_definition = None if source_job_key is None else source_definitions.get(source_job_key)
    attached_lifecycle = attempt.get("execution_attempt_lifecycle")
    if isinstance(attached_lifecycle, Mapping):
        return dict(attached_lifecycle)
    return project_execution_attempt_lifecycle(
        attempt,
        now=now,
        submit_job=submit_job,
        source_job_definition=source_definition,
    )


def _summarize_execution_attempt(
    attempt: Mapping[str, Any],
    *,
    lifecycle: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    lifecycle_payload = dict(lifecycle or {})
    return {
        "execution_attempt_id": attempt.get("execution_attempt_id"),
        "session_id": attempt.get("session_id"),
        "label": attempt.get("label"),
        "underlying_symbol": attempt.get("underlying_symbol"),
        "strategy": attempt.get("strategy"),
        "trade_intent": attempt.get("trade_intent"),
        "status": attempt.get("status"),
        "lifecycle_state": lifecycle_payload.get("lifecycle_state"),
        "requested_at": attempt.get("requested_at"),
        "submitted_at": attempt.get("submitted_at"),
        "completed_at": attempt.get("completed_at"),
        "broker_order_id": attempt.get("broker_order_id"),
        "broker_order_state": lifecycle_payload.get("broker_order_state"),
        "broker_order_state_counts": lifecycle_payload.get("broker_order_state_counts"),
        "source_kind": lifecycle_payload.get("source_kind"),
        "lifecycle_phase": lifecycle_payload.get("phase"),
        "lifecycle_note": lifecycle_payload.get("note"),
        "age_seconds": lifecycle_payload.get("age_seconds"),
        "queue_age_seconds": lifecycle_payload.get("queue_age_seconds"),
        "stale_after_seconds": lifecycle_payload.get("working_stale_after_seconds"),
        "submission_grace_seconds": lifecycle_payload.get("submission_grace_seconds"),
        "submit_job_status": lifecycle_payload.get("submit_job_status"),
        "submit_job_age_seconds": lifecycle_payload.get("submit_job_age_seconds"),
        "submit_job_heartbeat_age_seconds": lifecycle_payload.get("submit_job_heartbeat_age_seconds"),
        "stale": bool(lifecycle_payload.get("stale")),
        "next_action": lifecycle_payload.get("next_action"),
        "blocks_capacity": bool(lifecycle_payload.get("blocks_capacity")),
        "occupies_position_slot": bool(lifecycle_payload.get("occupies_position_slot")),
    }


def _symbols_from_ticker_source_run(ticker_source_run: Mapping[str, Any] | None) -> list[str]:
    if ticker_source_run is None:
        return []
    evidence = as_mapping(ticker_source_run.get("evidence"))
    snapshot = as_mapping(evidence.get("snapshot"))
    entries = as_list(snapshot.get("entries"))
    symbols = [str(as_mapping(entry).get("symbol") or "").strip().upper() for entry in entries if str(as_mapping(entry).get("symbol") or "").strip()]
    if symbols:
        return list(dict.fromkeys(symbols))
    tickers = as_list(ticker_source_run.get("symbols"))
    return [str(symbol).strip().upper() for symbol in tickers if str(symbol or "").strip()]


def _render_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _ticker_source_run_payload(row: TickerSourceRunModel, *, symbols: list[str]) -> dict[str, Any]:
    return {
        "ticker_source_run_id": row.ticker_source_run_id,
        "ticker_source_type": row.ticker_source_type,
        "ticker_source_id": row.ticker_source_id,
        "job_run_id": row.job_run_id,
        "status": row.status,
        "config_hash": row.config_hash,
        "generated_at": _render_datetime(row.generated_at),
        "completed_at": _render_datetime(row.completed_at),
        "observed_count": row.observed_count,
        "selected_count": row.selected_count,
        "excluded_count": row.excluded_count,
        "symbols": symbols[:SOURCE_SYMBOL_LIMIT],
        "summary": dict(row.summary_json or {}),
        "created_at": _render_datetime(row.created_at),
        "updated_at": _render_datetime(row.updated_at),
    }


def _candidate_symbol_diagnostic_payload(row: CandidateSymbolDiagnosticModel) -> dict[str, Any]:
    return {
        "candidate_run_id": row.candidate_run_id,
        "underlying_symbol": row.underlying_symbol,
        "trading_strategy_id": row.trading_strategy_id,
        "trade_structure": row.trade_structure,
        "routine": row.routine,
        "ticker_source_run_id": row.ticker_source_run_id,
        "ticker_source_kind": row.ticker_source_kind,
        "ticker_source_id": row.ticker_source_id,
        "diagnostic_status": row.diagnostic_status,
        "observed_at": _render_datetime(row.observed_at),
        "spot_price": row.spot_price,
        "expiration_count": row.expiration_count,
        "contract_count": row.contract_count,
        "snapshot_count": row.snapshot_count,
        "raw_candidate_count": row.raw_candidate_count,
        "postprocess_candidate_count": row.postprocess_candidate_count,
        "runtime_candidate_count": row.runtime_candidate_count,
        "returned_candidate_count": row.returned_candidate_count,
        "setup": dict(row.setup_json or {}),
        "market_data": dict(row.market_data_json or {}),
        "rejection_counts": dict(row.rejection_counts_json or {}),
        "ranking_gate": dict(row.ranking_gate_json or {}),
        "examples": dict(row.examples_json or {}),
        "evidence": dict(row.evidence_json or {}),
        "created_at": _render_datetime(row.created_at),
        "updated_at": _render_datetime(row.updated_at),
    }


def _candidate_run_payload(row: CandidateRunModel, *, diagnostics: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "candidate_run_id": row.candidate_run_id,
        "run_key": row.run_key,
        "trading_strategy_id": row.trading_strategy_id,
        "trade_structure": row.trade_structure,
        "routine": row.routine,
        "ticker_source_run_id": row.ticker_source_run_id,
        "ticker_source_kind": row.ticker_source_kind,
        "ticker_source_id": row.ticker_source_id,
        "status": row.status,
        "config_hash": row.config_hash,
        "generated_at": _render_datetime(row.generated_at),
        "completed_at": _render_datetime(row.completed_at),
        "symbol_count": row.symbol_count,
        "candidate_count": row.candidate_count,
        "summary": dict(row.summary_json or {}),
        "diagnostics": list(diagnostics or []),
        "created_at": _render_datetime(row.created_at),
        "updated_at": _render_datetime(row.updated_at),
    }


def _market_date_window(market_date: str) -> tuple[datetime, datetime]:
    start = datetime.combine(parse_date(market_date), datetime.min.time(), tzinfo=UTC)
    return start, start + timedelta(days=1)


def _latest_flow_facts(
    *,
    storage: Any,
    market_date: str,
    ticker_source_ids: set[str],
    strategy_ids: set[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not storage.engine_facts.schema_ready():
        return {}, {}
    start, end = _market_date_window(market_date)
    latest_sources: dict[str, TickerSourceRunModel] = {}
    latest_candidates: dict[str, CandidateRunModel] = {}
    with storage.engine_facts.session_factory() as session:
        for ticker_source_id in sorted(ticker_source_ids):
            row = session.scalars(
                select(TickerSourceRunModel)
                .where(TickerSourceRunModel.ticker_source_id == ticker_source_id)
                .where(TickerSourceRunModel.generated_at >= start)
                .where(TickerSourceRunModel.generated_at < end)
                .order_by(TickerSourceRunModel.generated_at.desc(), TickerSourceRunModel.ticker_source_run_id.asc())
                .limit(1)
            ).first()
            if row is not None:
                latest_sources[ticker_source_id] = row
        for strategy_id in sorted(strategy_ids):
            row = session.scalars(
                select(CandidateRunModel)
                .where(CandidateRunModel.trading_strategy_id == strategy_id)
                .where(CandidateRunModel.routine == "entry")
                .where(CandidateRunModel.generated_at >= start)
                .where(CandidateRunModel.generated_at < end)
                .order_by(CandidateRunModel.generated_at.desc(), CandidateRunModel.candidate_run_id.asc())
                .limit(1)
            ).first()
            if row is not None:
                latest_candidates[strategy_id] = row

        ticker_source_run_ids = [row.ticker_source_run_id for row in latest_sources.values()]
        candidate_run_ids = [row.candidate_run_id for row in latest_candidates.values()]
        symbols_by_ticker_source_run: dict[str, list[str]] = {ticker_source_run_id: [] for ticker_source_run_id in ticker_source_run_ids}
        diagnostics_by_candidate_run: dict[str, list[dict[str, Any]]] = {candidate_run_id: [] for candidate_run_id in candidate_run_ids}
        if ticker_source_run_ids:
            for ticker_source_run_id, symbol in session.execute(
                select(TickerSourceObservationModel.ticker_source_run_id, TickerSourceObservationModel.symbol)
                .where(TickerSourceObservationModel.ticker_source_run_id.in_(ticker_source_run_ids))
                .where(TickerSourceObservationModel.observation_state == "selected")
                .order_by(
                    TickerSourceObservationModel.ticker_source_run_id.asc(),
                    TickerSourceObservationModel.rank.asc().nulls_last(),
                    TickerSourceObservationModel.symbol.asc(),
                )
            ):
                symbols = symbols_by_ticker_source_run.setdefault(str(ticker_source_run_id), [])
                if len(symbols) < SOURCE_SYMBOL_LIMIT:
                    symbols.append(str(symbol))
        if candidate_run_ids:
            diagnostic_rows = session.scalars(
                select(CandidateSymbolDiagnosticModel)
                .where(CandidateSymbolDiagnosticModel.candidate_run_id.in_(candidate_run_ids))
                .order_by(
                    CandidateSymbolDiagnosticModel.candidate_run_id.asc(),
                    CandidateSymbolDiagnosticModel.returned_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.postprocess_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.raw_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.underlying_symbol.asc(),
                )
            ).all()
            for diagnostic in diagnostic_rows:
                rows = diagnostics_by_candidate_run.setdefault(diagnostic.candidate_run_id, [])
                if len(rows) < SOURCE_SYMBOL_LIMIT:
                    rows.append(_candidate_symbol_diagnostic_payload(diagnostic))

    return (
        {
            ticker_source_id: _ticker_source_run_payload(row, symbols=symbols_by_ticker_source_run.get(row.ticker_source_run_id, []))
            for ticker_source_id, row in latest_sources.items()
        },
        {
            strategy_id: _candidate_run_payload(row, diagnostics=diagnostics_by_candidate_run.get(row.candidate_run_id, []))
            for strategy_id, row in latest_candidates.items()
        },
    )


def _source_state(
    *,
    ticker_source_run: Mapping[str, Any] | None,
    source_kind: str,
    max_age_seconds: int | None,
    market_open: bool,
    now: datetime,
) -> dict[str, Any]:
    if ticker_source_run is None:
        return {
            "status": "degraded" if market_open and source_kind == "dynamic" else "idle",
            "age_seconds": None,
            "max_age_seconds": max_age_seconds,
            "symbol_count": 0,
            "symbols": [],
            "latest_run": None,
            "reason": "ticker_source_run_missing",
        }
    raw_status = str(ticker_source_run.get("status") or "unknown")
    age_seconds = _age_seconds(ticker_source_run.get("generated_at") or ticker_source_run.get("completed_at"), now=now)
    stale = bool(market_open and max_age_seconds is not None and age_seconds is not None and age_seconds > max_age_seconds)
    status = "healthy" if raw_status in {"ready", "fallback", "completed", "ok"} else "degraded"
    if stale and status == "healthy":
        status = "degraded"
    symbols = _symbols_from_ticker_source_run(ticker_source_run)
    symbol_count = coerce_int(ticker_source_run.get("selected_count")) or len(symbols)
    empty = status == "healthy" and symbol_count == 0
    return {
        "status": status,
        "raw_status": raw_status,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "stale": stale,
        "symbol_count": symbol_count,
        "symbols": symbols[:25],
        "latest_run": dict(ticker_source_run),
        "reason": "source_stale" if stale else "source_empty" if empty else None,
    }


def _candidate_state(
    *,
    candidate_run: Mapping[str, Any] | None,
    source_state: Mapping[str, Any] | None,
    cadence_minutes: int | None,
    market_open: bool,
    now: datetime,
) -> dict[str, Any]:
    max_age_seconds = None if cadence_minutes is None else max(cadence_minutes * 60 * 2, 300)
    if candidate_run is None:
        source_status = str((source_state or {}).get("status") or "unknown")
        source_symbol_count = coerce_int((source_state or {}).get("symbol_count"))
        if market_open and source_status == "healthy" and source_symbol_count == 0:
            return {
                "status": "healthy",
                "raw_status": "source_empty",
                "age_seconds": None,
                "max_age_seconds": max_age_seconds,
                "symbol_count": 0,
                "candidate_count": 0,
                "diagnostic_status": "no_source_symbols",
                "symbol_status_counts": {},
                "top_rejection_counts": {},
                "diagnostics": [],
                "latest_run": None,
                "reason": "source_has_no_symbols",
            }
        return {
            "status": "degraded" if market_open else "idle",
            "age_seconds": None,
            "max_age_seconds": max_age_seconds,
            "symbol_count": 0,
            "candidate_count": 0,
            "latest_run": None,
            "reason": "candidate_run_missing",
        }
    raw_status = str(candidate_run.get("status") or "unknown")
    age_seconds = _age_seconds(candidate_run.get("generated_at") or candidate_run.get("completed_at"), now=now)
    stale = bool(market_open and max_age_seconds is not None and age_seconds is not None and age_seconds > max_age_seconds)
    status = "healthy" if raw_status in {"completed", "ready", "ok"} else "degraded"
    if stale and status == "healthy":
        status = "degraded"
    candidate_count = coerce_int(candidate_run.get("candidate_count")) or 0
    summary = as_mapping(candidate_run.get("summary"))
    diagnostics = [dict(row) for row in as_list(candidate_run.get("diagnostics")) if isinstance(row, Mapping)]
    return {
        "status": status,
        "raw_status": raw_status,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "stale": stale,
        "symbol_count": coerce_int(candidate_run.get("symbol_count")) or 0,
        "candidate_count": candidate_count,
        "diagnostic_status": summary.get("diagnostic_status"),
        "symbol_status_counts": as_mapping(summary.get("symbol_status_counts")),
        "top_rejection_counts": as_mapping(summary.get("top_rejection_counts")),
        "diagnostics": diagnostics,
        "latest_run": dict(candidate_run),
        "reason": "candidate_run_stale" if stale else ("no_candidates" if candidate_count == 0 else None),
    }


def _flow_position_summary(
    *,
    execution_store: Any,
    trading_strategy_id: str,
    market_date: str,
) -> dict[str, Any]:
    if not execution_store.portfolio_schema_ready():
        return {
            "status": "blocked",
            "position_count": 0,
            "open_position_count": 0,
            "closed_position_count": 0,
            "latest_exit_reason": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 0.0,
        }
    day_positions = [
        dict(row)
        for row in execution_store.list_positions(
            trading_strategy_id=trading_strategy_id,
            market_date=market_date,
            limit=500,
        )
    ]
    open_positions = [
        dict(row)
        for row in execution_store.list_positions(
            trading_strategy_id=trading_strategy_id,
            statuses=OPEN_POSITION_STATUSES,
            limit=500,
        )
    ]
    closed_positions = [row for row in day_positions if str(row.get("status") or "") == "closed"]
    closed_positions.sort(key=lambda row: str(row.get("closed_at") or ""), reverse=True)
    realized = sum(coerce_float(row.get("realized_pnl")) or 0.0 for row in day_positions)
    unrealized = sum(coerce_float(row.get("unrealized_pnl")) or 0.0 for row in open_positions)
    return {
        "status": "healthy",
        "position_count": len(day_positions),
        "open_position_count": len(open_positions),
        "closed_position_count": len(closed_positions),
        "latest_exit_reason": None if not closed_positions else as_text(closed_positions[0].get("last_exit_reason")),
        "realized_pnl": round(realized, 2),
        "unrealized_pnl": round(unrealized, 2),
        "net_pnl": round(realized + unrealized, 2),
    }


def _flow_intent_summary(
    *,
    execution_store: Any,
    trading_strategy_id: str,
) -> dict[str, Any]:
    if not execution_store.intent_schema_ready():
        return {
            "status": "blocked",
            "active_intent_count": 0,
            "active_intent_state_counts": {},
        }
    active_intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            trading_strategy_id=trading_strategy_id,
            states=sorted(ACTIVE_INTENT_STATES),
            limit=500,
        )
    ]
    state_counts = Counter(str(row.get("state") or "unknown") for row in active_intents)
    return {
        "status": "healthy",
        "active_intent_count": len(active_intents),
        "active_intent_state_counts": dict(sorted(state_counts.items())),
        "active_intents": active_intents[:20],
    }


def _build_trading_flows(
    *,
    storage: Any,
    engine_ops: Mapping[str, Any],
    market_date: str,
    market_open: bool,
    now: datetime,
) -> list[dict[str, Any]]:
    del engine_ops
    strategies = [strategy for strategy in load_active_trading_strategies().values() if strategy.enabled]
    latest_sources, latest_candidates = _latest_flow_facts(
        storage=storage,
        market_date=market_date,
        ticker_source_ids={strategy.source.ref for strategy in strategies},
        strategy_ids={strategy.trading_strategy_id for strategy in strategies},
    )
    flows: list[dict[str, Any]] = []
    for strategy in strategies:
        latest_source = latest_sources.get(strategy.source.ref)
        latest_entry = latest_candidates.get(strategy.trading_strategy_id)
        entry_cadence_minutes = None if strategy.entry is None else strategy.entry.schedule.cadence_minutes
        entry_due = bool(strategy.entry is not None and strategy.entry.enabled and routine_should_run_now(strategy.entry, now=now))
        source_state = _source_state(
            ticker_source_run=latest_source,
            source_kind=strategy.source.kind,
            max_age_seconds=strategy.source.max_age_seconds,
            market_open=market_open,
            now=now,
        )
        candidate_state = _candidate_state(
            candidate_run=latest_entry,
            source_state=source_state,
            cadence_minutes=entry_cadence_minutes,
            market_open=market_open and entry_due,
            now=now,
        )
        intent_summary = _flow_intent_summary(
            execution_store=storage.execution,
            trading_strategy_id=strategy.trading_strategy_id,
        )
        position_summary = _flow_position_summary(
            execution_store=storage.execution,
            trading_strategy_id=strategy.trading_strategy_id,
            market_date=market_date,
        )
        max_entries = strategy.risk_limits.max_new_entries_per_day
        used_entries = coerce_int(position_summary.get("position_count")) or 0
        remaining_entries = None if max_entries is None else max(max_entries - used_entries - int(intent_summary.get("active_intent_count") or 0), 0)
        flows.append(
            {
                "trading_strategy_id": strategy.trading_strategy_id,
                "name": strategy.name,
                "trade_structure": strategy.trade_structure,
                "enabled": strategy.enabled,
                "runtime": strategy.runtime.as_dict(),
                "execution": strategy.execution.as_dict(),
                "source": strategy.source.as_dict(),
                "entry": (
                    None
                    if strategy.entry is None
                    else {
                        "enabled": strategy.entry.enabled,
                        "schedule": strategy.entry.schedule.as_dict(),
                        "selection": strategy.entry.selection.as_dict(),
                    }
                ),
                "management": (
                    None
                    if strategy.management is None
                    else {
                        "enabled": strategy.management.enabled,
                        "schedule": strategy.management.schedule.as_dict(),
                    }
                ),
                "risk_limits": strategy.risk_limits.as_dict(),
                "source_state": source_state,
                "candidate_state": candidate_state,
                "intent_state": intent_summary,
                "position_state": position_summary,
                "capacity": {
                    "open_position_count": position_summary.get("open_position_count"),
                    "max_open_positions": strategy.risk_limits.max_open_positions,
                    "session_entry_count": used_entries,
                    "max_daily_entries": max_entries,
                    "remaining_daily_entries": remaining_entries,
                },
                "status": _combine_statuses(
                    str(source_state.get("status") or "unknown"),
                    str(candidate_state.get("status") or "unknown"),
                    str(intent_summary.get("status") or "unknown"),
                    str(position_summary.get("status") or "unknown"),
                ),
            }
        )
    return flows


def _project_market_control(
    *,
    storage: Any,
    market_date: str | None,
    now: datetime,
) -> _MarketControlProjection:
    resolved_market_date = as_text(market_date) or now.astimezone(NEW_YORK).date().isoformat()
    market_session = _market_session_context(now=now)
    control = get_control_state_snapshot(storage=storage)
    control_status = _control_status(control)
    statuses = [control_status]
    attention: list[dict[str, str]] = []

    if control_status in {"degraded", "halted"}:
        attention.append(
            _attention(
                severity="high" if control_status == "halted" else "medium",
                code=f"control_mode_{control.get('mode')}",
                message=as_text(control.get("note")) or f"Control mode is {control.get('mode')}.",
            )
        )

    kill_switch_reason = resolve_execution_kill_switch_reason()
    if kill_switch_reason is not None:
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="kill_switch_enabled",
                message=kill_switch_reason,
            )
        )

    return _MarketControlProjection(
        market_date=resolved_market_date,
        market_session=market_session,
        market_open=bool(market_session.get("is_open")),
        control=control,
        kill_switch_reason=kill_switch_reason,
        statuses=tuple(statuses),
        attention=attention,
    )


def _project_jobs(
    *,
    db_target: str | None,
    storage: Any,
) -> _JobsProjection:
    jobs = build_jobs_compact_state(db_target=db_target, limit=25, storage=storage)
    return _JobsProjection(
        payload=jobs,
        summary=as_mapping(jobs.get("summary")),
        details=as_mapping(jobs.get("details")),
        statuses=(str(jobs.get("status") or "unknown"),),
        attention=[dict(row) for row in as_list(jobs.get("attention")) if isinstance(row, Mapping)],
    )


def _project_account(
    *,
    storage: Any,
    now: datetime,
    market_session: Mapping[str, Any],
) -> _AccountProjection:
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    broker_store = storage.broker
    if broker_store.schema_ready():
        broker_sync_status, broker_sync = _broker_sync_payload(
            broker_store.get_sync_state(BROKER_SYNC_KEY),
            now=now,
            market_session=market_session,
        )
        account_snapshot = _account_snapshot_payload(broker_store.get_latest_account_snapshot())
    else:
        broker_sync_status = "blocked"
        broker_sync = {
            "status": "missing",
            "raw_status": None,
            "updated_at": None,
            "summary": {},
            "error_text": None,
            "age_seconds": None,
        }
        account_snapshot = _account_snapshot_payload(None)
        attention.append(
            _attention(
                severity="high",
                code="broker_schema_unavailable",
                message="Broker sync and account snapshot storage are not available yet.",
            )
        )
    statuses.append(broker_sync_status)
    if broker_sync_status not in {"healthy", "idle"}:
        attention.append(
            _attention(
                severity="high" if broker_sync_status == "blocked" else "medium",
                code="broker_sync_unhealthy",
                message="Broker sync is missing, stale, or degraded.",
            )
        )

    account = as_mapping(account_snapshot.get("account"))
    if account_snapshot.get("status") != "ready":
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="account_snapshot_missing",
                message="No stored broker account snapshot is available.",
            )
        )
    elif account.get("trading_blocked") or account.get("account_blocked"):
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="broker_account_blocked",
                message="The stored broker account snapshot indicates trading is blocked.",
            )
        )

    return _AccountProjection(
        broker_sync_status=broker_sync_status,
        broker_sync=broker_sync,
        account_snapshot=account_snapshot,
        account=account,
        statuses=tuple(statuses),
        attention=attention,
    )


def _project_engine(
    *,
    storage: Any,
    market_date: str,
    now: datetime,
) -> _EngineProjection:
    engine_ops = build_engine_ops_state(
        storage=storage,
        market_date=market_date,
        now=now,
    )
    engine_summary = as_mapping(engine_ops.get("summary"))
    engine_status = str(engine_ops.get("status") or "unknown")
    attention: list[dict[str, str]] = []
    if engine_status in {"degraded", "blocked"}:
        attention.append(
            _attention(
                severity="high" if engine_status == "blocked" else "medium",
                code="engine_unhealthy",
                message="Engine facts, execution storage, or capture targets need attention.",
            )
        )
    return _EngineProjection(
        payload=engine_ops,
        summary=engine_summary,
        status=engine_status,
        statuses=(engine_status,),
        attention=attention,
    )


def _project_execution(
    *,
    storage: Any,
    now: datetime,
) -> _ExecutionProjection:
    execution_store = storage.execution
    job_store = getattr(storage, "jobs", None)
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    if execution_store.schema_ready():
        open_execution_attempts = [
            dict(row)
            for row in execution_store.list_attempts_by_status(
                statuses=sorted(OPEN_STATUSES),
                limit=200,
            )
        ]
    else:
        open_execution_attempts = []
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="execution_schema_unavailable",
                message="Execution attempts storage is not available yet.",
            )
        )

    submit_jobs, source_definitions = _load_execution_attempt_job_context(
        job_store=job_store,
        attempts=open_execution_attempts,
    )
    summarized_open_execution_attempts = [
        _summarize_execution_attempt(
            row,
            lifecycle=_execution_attempt_lifecycle(
                attempt=row,
                now=now,
                submit_jobs=submit_jobs,
                source_definitions=source_definitions,
            ),
        )
        for row in _sorted_by_activity(open_execution_attempts)
    ]
    stale_open_execution_count = sum(1 for row in summarized_open_execution_attempts if bool(row.get("stale")))
    submit_unknown_execution_count = sum(1 for row in summarized_open_execution_attempts if str(row.get("lifecycle_phase") or "") == "submit_unknown")
    capacity_blocked_underlyings = sorted(
        {
            str(row.get("underlying_symbol") or "")
            for row in summarized_open_execution_attempts
            if bool(row.get("blocks_capacity")) and as_text(row.get("underlying_symbol"))
        }
    )
    execution_health_status = "degraded" if stale_open_execution_count or submit_unknown_execution_count else "healthy"
    if submit_unknown_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="high",
                code="execution_submit_unknown",
                message=f"{submit_unknown_execution_count} open execution attempt(s) have uncertain submit outcomes and still block capacity.",
            )
        )
    elif stale_open_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="stale_open_executions_present",
                message=f"{stale_open_execution_count} open execution attempt(s) are stale and need operator review.",
            )
        )

    return _ExecutionProjection(
        open_execution_attempts=open_execution_attempts,
        summarized_open_execution_attempts=summarized_open_execution_attempts,
        stale_open_execution_count=stale_open_execution_count,
        submit_unknown_execution_count=submit_unknown_execution_count,
        capacity_blocked_underlyings=capacity_blocked_underlyings,
        execution_health_status=execution_health_status,
        statuses=tuple(statuses),
        attention=attention,
    )


def _project_positions(
    *,
    storage: Any,
    now: datetime,
    broker_sync: Mapping[str, Any],
) -> _PositionProjection:
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    execution_store = storage.execution
    open_positions: list[dict[str, Any]] = []
    top_positions: list[dict[str, Any]] = []
    risk_breach_count = 0
    reconciliation_mismatch_count = 0
    missing_mark_count = 0
    stale_mark_count = 0
    if execution_store.portfolio_schema_ready():
        from core.services.positions import enrich_position_row

        persisted_positions = [
            enrich_position_row(dict(row))
            for row in execution_store.list_positions(
                statuses=OPEN_POSITION_STATUSES,
                limit=200,
            )
        ]
        for position in persisted_positions:
            risk = assess_position_risk(position=position)
            close_mark = coerce_float(position.get("close_mark"))
            mark_age_seconds = _seconds_since(position.get("close_marked_at"), now=now)
            if close_mark is None:
                missing_mark_count += 1
            elif mark_age_seconds is not None and mark_age_seconds > MARK_STALE_AFTER_SECONDS:
                stale_mark_count += 1
            if str(position.get("reconciliation_status") or "") == "mismatch":
                reconciliation_mismatch_count += 1
            if str(risk.get("status") or "") == "breach":
                risk_breach_count += 1
            realized_pnl = coerce_float(position.get("realized_pnl")) or 0.0
            unrealized_pnl = coerce_float(position.get("unrealized_pnl")) or 0.0
            open_positions.append(
                {
                    **position,
                    "status": position.get("status"),
                    "risk_status": risk.get("status"),
                    "risk_note": risk.get("note"),
                    "mark_age_seconds": None if mark_age_seconds is None else round(mark_age_seconds, 2),
                    "net_pnl": round(realized_pnl + unrealized_pnl, 2),
                    "exit_status": describe_position_exit_state(
                        position=position,
                        now=now,
                    ),
                }
            )
        top_positions = _top_positions(open_positions)
    else:
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="position_schema_unavailable",
                message="Position storage is not available yet.",
            )
        )

    mark_error = as_text(as_mapping(broker_sync.get("summary")).get("mark_error"))
    broker_unquoted_positions = coerce_int(as_mapping(broker_sync.get("summary")).get("unquoted_position_count")) or 0
    mark_health_status = "healthy"
    if missing_mark_count or stale_mark_count or broker_unquoted_positions or mark_error:
        mark_health_status = "degraded"
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="mark_health_degraded",
                message="One or more open positions are missing or stale quote marks.",
            )
        )

    if risk_breach_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="risk_breaches_present",
                message=f"{risk_breach_count} open position(s) are outside snapshotted risk limits.",
            )
        )

    if reconciliation_mismatch_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="reconciliation_mismatches_present",
                message=f"{reconciliation_mismatch_count} open position(s) have reconciliation mismatches.",
            )
        )

    return _PositionProjection(
        open_positions=open_positions,
        top_positions=top_positions,
        risk_breach_count=risk_breach_count,
        reconciliation_mismatch_count=reconciliation_mismatch_count,
        missing_mark_count=missing_mark_count,
        stale_mark_count=stale_mark_count,
        broker_unquoted_positions=broker_unquoted_positions,
        mark_error=mark_error,
        mark_health_status=mark_health_status,
        statuses=tuple(statuses),
        attention=attention,
    )


def _project_alerts(
    *,
    storage: Any,
    now: datetime,
) -> _AlertProjection:
    alert_store = storage.alerts
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    if alert_store.schema_ready():
        recent_alerts = [dict(row) for row in alert_store.list_alert_events(limit=RECENT_ALERT_LIMIT)]
        alert_delivery = _alert_delivery_payload(recent_alerts, now=now)
        if alert_delivery["status"] != "healthy":
            statuses.append(str(alert_delivery["status"]))
            attention.append(
                _attention(
                    severity="medium",
                    code="alert_delivery_issues",
                    message="Recent alert delivery failures or retries were detected.",
                )
            )
    else:
        alert_delivery = {
            "status": "unknown",
            "count": 0,
            "status_counts": {},
            "dead_letter_count": 0,
            "retry_wait_count": 0,
            "dispatching_count": 0,
            "pending_count": 0,
        }
    return _AlertProjection(alert_delivery=alert_delivery, statuses=tuple(statuses), attention=attention)


def _project_flows(
    *,
    storage: Any,
    engine_ops: Mapping[str, Any],
    market_date: str,
    market_open: bool,
    now: datetime,
) -> _FlowProjection:
    trading_flows = _build_trading_flows(
        storage=storage,
        engine_ops=engine_ops,
        market_date=market_date,
        market_open=market_open,
        now=now,
    )
    degraded_flows = [flow for flow in trading_flows if str(flow.get("status") or "") in {"degraded", "blocked", "halted"}]
    attention: list[dict[str, str]] = []
    statuses: list[str] = []
    if degraded_flows:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="trading_flows_need_attention",
                message=f"{len(degraded_flows)} trading flow(s) are degraded or blocked.",
            )
        )
    return _FlowProjection(
        trading_flows=trading_flows,
        degraded_flows=degraded_flows,
        statuses=tuple(statuses),
        attention=attention,
    )


@with_storage()
def build_trading_ops_state(
    *,
    db_target: str | None = None,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    now = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    market_control = _project_market_control(storage=storage, market_date=market_date, now=now)
    jobs = _project_jobs(db_target=db_target, storage=storage)
    account = _project_account(storage=storage, now=now, market_session=market_control.market_session)
    engine = _project_engine(
        storage=storage,
        market_date=market_control.market_date,
        now=now,
    )
    execution = _project_execution(storage=storage, now=now)
    positions = _project_positions(storage=storage, now=now, broker_sync=account.broker_sync)
    alerts = _project_alerts(storage=storage, now=now)
    flows = _project_flows(
        storage=storage,
        engine_ops=engine.payload,
        market_date=market_control.market_date,
        market_open=market_control.market_open,
        now=now,
    )

    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    for projection in (market_control, jobs, account, engine, execution, positions, alerts, flows):
        statuses.extend(projection.statuses)
        attention.extend(projection.attention)

    trading_allowed = True
    if market_control.kill_switch_reason is not None:
        trading_allowed = False
    elif str(market_control.control.get("mode") or "") != "normal":
        trading_allowed = False
    elif not market_control.market_open:
        trading_allowed = False
    elif account.broker_sync_status != "healthy":
        trading_allowed = False
    elif account.account_snapshot.get("status") != "ready":
        trading_allowed = False
    elif account.account.get("trading_blocked") or account.account.get("account_blocked"):
        trading_allowed = False
    elif execution.stale_open_execution_count or execution.submit_unknown_execution_count:
        trading_allowed = False

    active_intent_count = sum(int(as_mapping(flow.get("intent_state")).get("active_intent_count") or 0) for flow in flows.trading_flows)
    primary_flow = next(
        (flow for flow in flows.trading_flows if flow.get("trading_strategy_id") == "momentum_long_calls"),
        flows.trading_flows[0] if flows.trading_flows else {},
    )
    primary_capacity = as_mapping(primary_flow.get("capacity"))
    primary_position_state = as_mapping(primary_flow.get("position_state"))
    summary = {
        "market_date": market_control.market_date,
        "market_session_status": market_control.market_session.get("status"),
        "market_open_at": market_control.market_session.get("market_open_at"),
        "market_close_at": market_control.market_session.get("market_close_at"),
        "trading_allowed": trading_allowed,
        "environment": account.account_snapshot.get("environment"),
        "control_mode": market_control.control.get("mode"),
        "scheduler_status": as_mapping(jobs.details.get("scheduler")).get("status"),
        "worker_lane_count": jobs.summary.get("worker_lane_count"),
        "disabled_worker_lane_count": jobs.summary.get("disabled_worker_lane_count"),
        "blocked_worker_lane_count": sum(1 for row in as_list(jobs.details.get("worker_lanes")) if as_mapping(row).get("status") == "blocked"),
        "idle_worker_lane_count": sum(1 for row in as_list(jobs.details.get("worker_lanes")) if as_mapping(row).get("status") == "idle"),
        "actionable_failed_job_count": jobs.summary.get("actionable_failed_count"),
        "broker_sync_status": account.broker_sync.get("status"),
        "broker_sync_age_seconds": account.broker_sync.get("age_seconds"),
        "account_snapshot_status": account.account_snapshot.get("status"),
        "account_snapshot_captured_at": account.account_snapshot.get("captured_at"),
        "open_position_count": len(positions.open_positions),
        "open_execution_count": len(execution.open_execution_attempts),
        "active_intent_count": active_intent_count,
        "max_open_positions": primary_capacity.get("max_open_positions"),
        "max_daily_entries": primary_capacity.get("max_daily_entries"),
        "session_entry_count": primary_capacity.get("session_entry_count"),
        "remaining_daily_entries": primary_capacity.get("remaining_daily_entries"),
        "closed_position_count": primary_position_state.get("closed_position_count"),
        "latest_exit_reason": primary_position_state.get("latest_exit_reason"),
        "realized_pnl": primary_position_state.get("realized_pnl"),
        "unrealized_pnl": primary_position_state.get("unrealized_pnl"),
        "net_pnl": primary_position_state.get("net_pnl"),
        "execution_health_status": execution.execution_health_status,
        "risk_breach_count": positions.risk_breach_count,
        "reconciliation_mismatch_count": positions.reconciliation_mismatch_count,
        "mark_health_status": positions.mark_health_status,
        "engine_status": engine.status,
        "engine_ticker_source_run_count": coerce_int(engine.summary.get("ticker_source_run_count")) or 0,
        "engine_candidate_run_count": coerce_int(engine.summary.get("candidate_run_count")) or 0,
        "engine_trade_candidate_count": coerce_int(engine.summary.get("trade_candidate_count")) or 0,
        "engine_signal_count": coerce_int(engine.summary.get("signal_count")) or 0,
        "engine_decision_count": coerce_int(engine.summary.get("decision_count")) or 0,
        "engine_selected_count": coerce_int(engine.summary.get("selected_count")) or 0,
        "engine_intent_count": coerce_int(engine.summary.get("intent_count")) or 0,
        "engine_entry_intent_count": coerce_int(engine.summary.get("entry_intent_count")) or 0,
        "engine_management_intent_count": coerce_int(engine.summary.get("management_intent_count")) or 0,
        "engine_open_position_count": coerce_int(engine.summary.get("open_position_count")) or 0,
        "capture_active_target_count": coerce_int(engine.summary.get("capture_active_target_count")) or 0,
        "capture_status": engine.summary.get("capture_status"),
    }

    details = {
        "market_session": market_control.market_session,
        "control": market_control.control,
        "jobs": jobs.payload,
        "scheduler": jobs.details.get("scheduler"),
        "workers": jobs.details.get("workers"),
        "worker_lanes": jobs.details.get("worker_lanes"),
        "running_jobs": [dict(row) for row in as_list(jobs.details.get("running_jobs")) if as_mapping(row).get("status") == "running"],
        "queued_jobs": [dict(row) for row in as_list(jobs.details.get("queued_jobs")) if as_mapping(row).get("status") == "queued"],
        "recent_job_runs": jobs.details.get("job_runs"),
        "broker_sync": account.broker_sync,
        "account_snapshot": account.account_snapshot,
        "engine": engine.payload,
        "execution_runtimes": resolve_execution_runtime_capabilities(),
        "open_execution_attempts": execution.summarized_open_execution_attempts,
        "open_positions": positions.open_positions,
        "top_positions": positions.top_positions,
        "trading_flows": flows.trading_flows,
        "primary_trading_flow": primary_flow,
        "alert_delivery": alerts.alert_delivery,
        "mark_health": {
            "status": positions.mark_health_status,
            "missing_mark_count": positions.missing_mark_count,
            "stale_mark_count": positions.stale_mark_count,
            "broker_unquoted_position_count": positions.broker_unquoted_positions,
            "mark_error": positions.mark_error,
        },
        "execution_health": {
            "status": execution.execution_health_status,
            "stale_open_execution_count": execution.stale_open_execution_count,
            "submit_unknown_execution_count": execution.submit_unknown_execution_count,
            "capacity_blocked_underlying_count": len(execution.capacity_blocked_underlyings),
            "capacity_blocked_underlyings": execution.capacity_blocked_underlyings,
        },
    }
    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": summary,
        "attention": attention,
        "details": details,
    }


__all__ = ["build_trading_ops_state"]

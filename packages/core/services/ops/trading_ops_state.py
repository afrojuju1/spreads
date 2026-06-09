from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

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
from core.services.option_structures import position_legs, unique_leg_symbols
from core.services.trading_engine.portfolio_runtime import describe_position_exit_state
from core.services.risk_manager import assess_position_risk
from core.services.trading_strategies import load_active_trading_strategies, load_trading_strategies, routine_should_run_now
from core.storage.engine_models import (
    CandidateRunModel,
    CandidateSymbolDiagnosticModel,
    TickerSourceObservationModel,
    TickerSourceRunModel,
    TradeCandidateModel,
)
from core.storage.lifecycle_models import TradeAdmissionModel, TradeDecisionModel, TradeSignalModel
from core.storage.serializers import parse_date, parse_datetime
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
ENTRY_QUALITY_STAGE_ORDER = (
    "source_preflight",
    "underlying_setup",
    "chain_viability",
    "contract_fit",
    "premium_quality",
    "selection",
)
LIFECYCLE_PROVENANCES = ("natural_strategy", "synthetic_validation", "operator_direct")
LATEST_LIFECYCLE_PROVENANCES = ("natural_strategy", "synthetic_validation")
BROKER_OPTION_ASSET_CLASSES = {"option", "us_option"}
NO_ENTRY_REASON_GROUPS = (
    (
        "market_regime",
        "broad market regime",
        ("market_regime_",),
        (),
    ),
    (
        "target_dte_chain",
        "target DTE chain viability",
        ("target_dte_",),
        (),
    ),
    (
        "option_liquidity",
        "option liquidity",
        ("open_interest_", "relative_spread_", "bid_ask_", "quote_size_"),
        (),
    ),
    (
        "market_data_completeness",
        "market data completeness",
        ("no_",),
        ("no_delta", "no_snapshot", "no_expected_move"),
    ),
    (
        "contract_fit",
        "contract fit",
        ("delta_", "dte_", "itm_call_"),
        ("delta_outside_range", "itm_call_skipped"),
    ),
)


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


@dataclass(frozen=True)
class _StrategyBreadthProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]


@dataclass(frozen=True)
class _ExecutionContractProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]
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


def _is_option_broker_position(position: Mapping[str, Any]) -> bool:
    return str(position.get("asset_class") or "").strip().lower() in BROKER_OPTION_ASSET_CLASSES


def _broker_position_symbol(position: Mapping[str, Any]) -> str | None:
    return as_text(position.get("symbol"))


def _managed_leg_index(open_positions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for position in open_positions:
        owner_kind = "spreads_managed"
        if str(position.get("source_object_type") or "") == "synthetic_validation":
            owner_kind = "spreads_synthetic_validation"
        for symbol in unique_leg_symbols(position_legs(position)):
            index.setdefault(
                symbol,
                {
                    "owner_kind": owner_kind,
                    "position_id": position.get("position_id"),
                    "trading_strategy_id": position.get("trading_strategy_id"),
                    "source_object_type": position.get("source_object_type"),
                    "root_symbol": position.get("root_symbol") or position.get("underlying_symbol"),
                    "strategy_family": position.get("strategy_family") or position.get("strategy"),
                },
            )
    return index


def _broker_exposure_state(
    *,
    account_snapshot: Mapping[str, Any],
    open_positions: list[dict[str, Any]],
    broker_sync: Mapping[str, Any],
) -> dict[str, Any]:
    broker_positions = [dict(row) for row in as_list(account_snapshot.get("positions")) if isinstance(row, Mapping)]
    managed_by_symbol = _managed_leg_index(open_positions)
    classified: list[dict[str, Any]] = []
    owner_counts: Counter[str] = Counter()
    option_owner_counts: Counter[str] = Counter()
    total_market_value = 0.0
    option_market_value = 0.0

    for position in broker_positions:
        symbol = _broker_position_symbol(position)
        managed = managed_by_symbol.get(symbol or "")
        owner_kind = "external_manual" if managed is None else str(managed.get("owner_kind") or "spreads_managed")
        is_option = _is_option_broker_position(position)
        owner_counts[owner_kind] += 1
        if is_option:
            option_owner_counts[owner_kind] += 1
        market_value = coerce_float(position.get("market_value")) or 0.0
        total_market_value += market_value
        if is_option:
            option_market_value += market_value
        classified.append(
            {
                "symbol": symbol,
                "asset_class": position.get("asset_class"),
                "side": position.get("side"),
                "qty": position.get("qty"),
                "market_value": position.get("market_value"),
                "cost_basis": position.get("cost_basis"),
                "unrealized_pl": position.get("unrealized_pl"),
                "unrealized_intraday_pl": position.get("unrealized_intraday_pl"),
                "ownership": owner_kind,
                "spreads_position_id": None if managed is None else managed.get("position_id"),
                "trading_strategy_id": None if managed is None else managed.get("trading_strategy_id"),
                "source_object_type": None if managed is None else managed.get("source_object_type"),
                "root_symbol": None if managed is None else managed.get("root_symbol"),
                "strategy_family": None if managed is None else managed.get("strategy_family"),
            }
        )

    external_option_count = option_owner_counts.get("external_manual", 0)
    managed_option_count = sum(count for owner, count in option_owner_counts.items() if owner != "external_manual")
    status = "clear"
    if external_option_count and managed_option_count:
        status = "mixed"
    elif external_option_count:
        status = "external_present"
    elif managed_option_count:
        status = "managed"

    broker_sync_summary = as_mapping(broker_sync.get("summary"))
    return {
        "status": status,
        "broker_position_count": len(broker_positions),
        "broker_option_position_count": sum(1 for row in broker_positions if _is_option_broker_position(row)),
        "spreads_managed_option_position_count": managed_option_count,
        "external_manual_option_position_count": external_option_count,
        "owner_counts": dict(sorted(owner_counts.items())),
        "option_owner_counts": dict(sorted(option_owner_counts.items())),
        "total_market_value": round(total_market_value, 2),
        "option_market_value": round(option_market_value, 2),
        "broker_sync_orphan_position_count": coerce_int(broker_sync_summary.get("orphan_broker_position_count")) or 0,
        "positions": classified[:25],
    }


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


def _normalize_broker_environment(value: Any) -> str:
    normalized = as_text(value)
    if normalized is None:
        return "unknown"
    normalized = normalized.strip().lower()
    if normalized in {"paper", "alpaca_paper"}:
        return "alpaca_paper"
    if normalized in {"live", "alpaca_live"}:
        return "alpaca_live"
    if normalized in {"custom", "alpaca_custom"}:
        return "alpaca_custom"
    if normalized.startswith("alpaca_"):
        return normalized
    return "unknown"


def _broker_environment_source(account_snapshot: Mapping[str, Any]) -> str:
    if account_snapshot.get("status") != "ready":
        return "account_snapshot_missing"
    if as_text(account_snapshot.get("environment")) is None:
        return "account_snapshot.environment_missing"
    return "account_snapshot.environment"


def _execution_environment_compatibility(
    *,
    execution_posture: Any,
    broker_environment: Any,
) -> tuple[bool, str | None, str]:
    posture = str(execution_posture or "shadow").strip().lower()
    environment = _normalize_broker_environment(broker_environment)
    if posture == "shadow":
        return True, None, "healthy"
    if environment == "unknown":
        return False, "broker_environment_unknown", "blocked"
    if posture == "paper":
        if environment == "alpaca_paper":
            return True, None, "healthy"
        return False, "paper_execution_requires_alpaca_paper", "blocked"
    if posture == "live":
        if environment == "alpaca_live":
            return True, None, "healthy"
        if environment == "alpaca_paper":
            return False, "live_execution_observed_on_alpaca_paper", "degraded"
        return False, "live_execution_requires_alpaca_live", "blocked"
    return False, "execution_posture_unknown", "blocked"


def _execution_contract_status(contract: Mapping[str, Any]) -> str:
    if bool(contract.get("environment_compatible")):
        return "healthy"
    return str(contract.get("status") or "blocked")


def _strategy_execution_contract(
    *,
    strategy: Any,
    broker_environment: str,
    broker_environment_source: str,
    now: datetime,
) -> dict[str, Any]:
    execution_posture = strategy.execution.mode
    approval_mode = strategy.execution.approval
    execution_runtime = strategy.execution.runtime
    compatible, mismatch_reason, status = _execution_environment_compatibility(
        execution_posture=execution_posture,
        broker_environment=broker_environment,
    )
    automatic_mode = approval_mode == "auto" and execution_posture in {"paper", "live"}
    return {
        "status": status,
        "trading_strategy_id": strategy.trading_strategy_id,
        "execution_posture": execution_posture,
        "approval_mode": approval_mode,
        "execution_runtime": execution_runtime,
        "broker": "alpaca",
        "broker_environment": broker_environment,
        "broker_environment_source": broker_environment_source,
        "environment_compatible": compatible,
        "environment_mismatch_reason": mismatch_reason,
        "automatic_submission_allowed": bool(automatic_mode and compatible),
        "validation_provenance": "natural_strategy",
        "strategy_run_id": None,
        "trade_decision_id": None,
        "execution_intent_id": None,
        "execution_attempt_id": None,
        "observed_at": _render_datetime(now),
    }


def _validation_provenance_from_payload(
    payload: Mapping[str, Any],
    *,
    fallback_row: Mapping[str, Any] | None = None,
    natural_strategy_ids: set[str] | None = None,
) -> str:
    row = fallback_row or {}
    natural_ids = natural_strategy_ids or set()
    candidates = [
        payload.get("validation_provenance"),
        as_mapping(payload.get("option_selection")).get("validation_provenance"),
        as_mapping(payload.get("source")).get("kind"),
        row.get("validation_provenance"),
        row.get("source_object_type"),
    ]
    for candidate in candidates:
        value = as_text(candidate)
        if value is None:
            continue
        value = value.strip().lower()
        if value in LIFECYCLE_PROVENANCES:
            return value
    trading_strategy_id = as_text(payload.get("trading_strategy_id") or row.get("trading_strategy_id"))
    if trading_strategy_id == "synthetic_paper_lifecycle_smoke":
        return "synthetic_validation"
    if trading_strategy_id in natural_ids:
        return "natural_strategy"
    if any(
        as_text(value) is not None
        for value in (
            payload.get("trade_signal_id"),
            payload.get("trade_decision_id"),
            row.get("trade_signal_id"),
            row.get("trade_decision_id"),
        )
    ):
        return "natural_strategy"
    return "operator_direct"


def _evidence_timestamp(value: Mapping[str, Any]) -> datetime:
    observed_at = as_text(value.get("observed_at"))
    if observed_at is None:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = parse_datetime(observed_at)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed is None:
        return datetime.min.replace(tzinfo=UTC)
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _apply_evidence_environment(
    evidence: dict[str, Any],
    *,
    broker_environment: str,
    broker_environment_source: str,
) -> dict[str, Any]:
    posture = as_text(evidence.get("execution_posture"))
    compatible, mismatch_reason, status = _execution_environment_compatibility(
        execution_posture=posture,
        broker_environment=broker_environment,
    )
    return {
        **evidence,
        "status": evidence.get("status") or evidence.get("lifecycle_state"),
        "contract_status": status,
        "broker": evidence.get("broker") or "alpaca",
        "broker_environment": as_text(evidence.get("broker_environment")) or broker_environment,
        "broker_environment_source": as_text(evidence.get("broker_environment_source")) or broker_environment_source,
        "environment_compatible": compatible,
        "environment_mismatch_reason": mismatch_reason,
    }


def _execution_attempt_evidence(
    attempt: Mapping[str, Any],
    *,
    broker_environment: str,
    broker_environment_source: str,
    natural_strategy_ids: set[str],
) -> dict[str, Any]:
    request = as_mapping(attempt.get("request"))
    source = as_mapping(request.get("source"))
    evidence = {
        "object_type": "execution_attempt",
        "validation_provenance": _validation_provenance_from_payload(
            request,
            fallback_row=attempt,
            natural_strategy_ids=natural_strategy_ids,
        ),
        "trading_strategy_id": attempt.get("trading_strategy_id") or request.get("trading_strategy_id"),
        "execution_posture": as_text(request.get("execution_mode")),
        "approval_mode": as_text(request.get("approval_mode")),
        "execution_runtime": as_text(request.get("execution_runtime")),
        "broker": attempt.get("broker"),
        "source_kind": as_text(source.get("kind") or attempt.get("source_object_type")),
        "trade_intent": attempt.get("trade_intent") or request.get("trade_intent"),
        "lifecycle_state": attempt.get("status"),
        "strategy_run_id": request.get("strategy_run_id") or request.get("source_cycle_id") or attempt.get("run_id"),
        "trade_decision_id": attempt.get("trade_decision_id") or request.get("trade_decision_id"),
        "execution_intent_id": request.get("execution_intent_id"),
        "execution_attempt_id": attempt.get("execution_attempt_id"),
        "observed_at": attempt.get("completed_at") or attempt.get("submitted_at") or attempt.get("requested_at"),
    }
    return _apply_evidence_environment(
        evidence,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
    )


def _execution_intent_evidence(
    intent: Mapping[str, Any],
    *,
    broker_environment: str,
    broker_environment_source: str,
    natural_strategy_ids: set[str],
) -> dict[str, Any]:
    payload = as_mapping(intent.get("payload"))
    source = as_mapping(payload.get("source"))
    evidence = {
        "object_type": "execution_intent",
        "validation_provenance": _validation_provenance_from_payload(
            payload,
            fallback_row=intent,
            natural_strategy_ids=natural_strategy_ids,
        ),
        "trading_strategy_id": intent.get("trading_strategy_id") or payload.get("trading_strategy_id"),
        "execution_posture": as_text(payload.get("execution_mode")),
        "approval_mode": as_text(payload.get("approval_mode")),
        "execution_runtime": as_text(payload.get("execution_runtime")),
        "broker": payload.get("broker"),
        "source_kind": as_text(source.get("kind")),
        "trade_intent": intent.get("action_type") or payload.get("trade_intent"),
        "lifecycle_state": intent.get("state"),
        "strategy_run_id": payload.get("strategy_run_id") or payload.get("source_cycle_id"),
        "trade_decision_id": intent.get("trade_decision_id") or payload.get("trade_decision_id"),
        "execution_intent_id": intent.get("execution_intent_id"),
        "execution_attempt_id": intent.get("execution_attempt_id"),
        "observed_at": intent.get("updated_at") or intent.get("created_at"),
    }
    return _apply_evidence_environment(
        evidence,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
    )


def _latest_lifecycle_evidence(
    *,
    storage: Any,
    market_date: str,
    broker_environment: str,
    broker_environment_source: str,
    natural_strategy_ids: set[str],
) -> dict[str, Any]:
    execution_store = storage.execution
    evidence_rows: list[dict[str, Any]] = []
    if execution_store.schema_ready():
        evidence_rows.extend(
            _execution_attempt_evidence(
                dict(row),
                broker_environment=broker_environment,
                broker_environment_source=broker_environment_source,
                natural_strategy_ids=natural_strategy_ids,
            )
            for row in execution_store.list_attempts_for_market_date(
                market_date=market_date,
                limit=500,
            )
        )
    if execution_store.intent_schema_ready():
        evidence_rows.extend(
            _execution_intent_evidence(
                dict(row),
                broker_environment=broker_environment,
                broker_environment_source=broker_environment_source,
                natural_strategy_ids=natural_strategy_ids,
            )
            for row in execution_store.list_execution_intents(limit=500)
        )
    evidence_rows.sort(key=_evidence_timestamp, reverse=True)
    latest_by_provenance: dict[str, dict[str, Any] | None] = {provenance: None for provenance in LATEST_LIFECYCLE_PROVENANCES}
    for evidence in evidence_rows:
        provenance = as_text(evidence.get("validation_provenance"))
        if provenance not in latest_by_provenance or latest_by_provenance[provenance] is not None:
            continue
        latest_by_provenance[provenance] = evidence
    return latest_by_provenance


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


def _int_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    counts: dict[str, int] = {}
    for key, raw_count in value.items():
        name = as_text(key)
        count = coerce_int(raw_count)
        if name is None or count is None or count <= 0:
            continue
        counts[name] = int(count)
    return dict(sorted(counts.items()))


def _quality_blockers_by_stage(diagnostics: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    by_stage: dict[str, Counter[str]] = {}
    for diagnostic in diagnostics:
        evidence = as_mapping(diagnostic.get("evidence"))
        waterfall = as_mapping(evidence.get("quality_waterfall"))
        for result in as_list(waterfall.get("results")):
            if not isinstance(result, Mapping):
                continue
            if str(result.get("status") or "").strip().lower() != "block":
                continue
            stage = as_text(result.get("stage"))
            if stage is None:
                continue
            stage_counts = by_stage.setdefault(stage, Counter())
            for reason in as_list(result.get("reason_codes")):
                reason_code = as_text(reason)
                if reason_code is not None:
                    stage_counts[reason_code] += 1
    return {stage: dict(counts.most_common(8)) for stage, counts in sorted(by_stage.items())}


def _quality_profile_from_diagnostics(diagnostics: list[dict[str, Any]]) -> str | None:
    for diagnostic in diagnostics:
        evidence = as_mapping(diagnostic.get("evidence"))
        waterfall = as_mapping(evidence.get("quality_waterfall"))
        profile_id = as_text(evidence.get("quality_profile_id") or waterfall.get("profile_id"))
        if profile_id is not None:
            return profile_id
    return None


def _quality_waterfall_state(
    *,
    summary: Mapping[str, Any],
    diagnostics: list[dict[str, Any]],
    selection_counts: Mapping[str, Any] | None,
    admission_counts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    raw_stage_counts = as_mapping(summary.get("filter_stage_counts"))
    stage_counts = {stage: _int_count_map(counts) for stage, counts in raw_stage_counts.items()}
    stage_blockers = _quality_blockers_by_stage(diagnostics)
    stage_order = tuple(dict.fromkeys((*ENTRY_QUALITY_STAGE_ORDER, *stage_counts.keys(), *stage_blockers.keys())))
    stage_rows = []
    for stage in stage_order:
        counts = stage_counts.get(stage, {})
        blockers = stage_blockers.get(stage, {})
        stage_rows.append(
            {
                "stage": stage,
                "counts": counts,
                "total": sum(counts.values()),
                "top_blocker_reasons": blockers,
            }
        )

    top_blockers = _int_count_map(summary.get("top_quality_blockers"))
    if not top_blockers:
        combined = Counter[str]()
        for blockers in stage_blockers.values():
            combined.update(blockers)
        top_blockers = dict(combined.most_common(12))

    selected_counts = _int_count_map(selection_counts)
    admitted_counts = _int_count_map(admission_counts)
    profile_id = as_text(summary.get("quality_profile_id")) or _quality_profile_from_diagnostics(diagnostics)
    return {
        "profile_id": profile_id,
        "snapshot_count": coerce_int(summary.get("quality_snapshot_count")),
        "blocked_snapshot_count": coerce_int(summary.get("quality_blocked_snapshot_count")),
        "stage_counts": stage_counts,
        "stage_rows": stage_rows,
        "top_blocker_reasons": top_blockers,
        "top_watch_reasons": _int_count_map(summary.get("top_quality_watch_reasons")),
        "selection": {
            "decision_state_counts": selected_counts,
            "total": sum(selected_counts.values()),
        },
        "admission": {
            "admission_state_counts": admitted_counts,
            "total": sum(admitted_counts.values()),
        },
    }


def _candidate_run_payload(
    row: CandidateRunModel,
    *,
    diagnostics: list[dict[str, Any]] | None = None,
    selection_counts: Mapping[str, Any] | None = None,
    admission_counts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
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
        "selection_counts": _int_count_map(selection_counts),
        "admission_counts": _int_count_map(admission_counts),
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
        selection_counts_by_candidate_run: dict[str, dict[str, int]] = {candidate_run_id: {} for candidate_run_id in candidate_run_ids}
        admission_counts_by_candidate_run: dict[str, dict[str, int]] = {candidate_run_id: {} for candidate_run_id in candidate_run_ids}
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
            candidate_run_ref = func.coalesce(TradeCandidateModel.candidate_run_id, TradeSignalModel.source_id)
            for candidate_run_id, decision_state, count in session.execute(
                select(candidate_run_ref, TradeDecisionModel.decision_state, func.count())
                .join(TradeSignalModel, TradeDecisionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
                .outerjoin(TradeCandidateModel, TradeSignalModel.trade_candidate_id == TradeCandidateModel.trade_candidate_id)
                .where(candidate_run_ref.in_(candidate_run_ids))
                .group_by(candidate_run_ref, TradeDecisionModel.decision_state)
            ):
                counts = selection_counts_by_candidate_run.setdefault(str(candidate_run_id), {})
                counts[str(decision_state or "unknown")] = int(count or 0)
            for candidate_run_id, admission_state, count in session.execute(
                select(candidate_run_ref, TradeAdmissionModel.admission_state, func.count())
                .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
                .outerjoin(TradeCandidateModel, TradeSignalModel.trade_candidate_id == TradeCandidateModel.trade_candidate_id)
                .where(candidate_run_ref.in_(candidate_run_ids))
                .group_by(candidate_run_ref, TradeAdmissionModel.admission_state)
            ):
                counts = admission_counts_by_candidate_run.setdefault(str(candidate_run_id), {})
                counts[str(admission_state or "unknown")] = int(count or 0)

    return (
        {
            ticker_source_id: _ticker_source_run_payload(row, symbols=symbols_by_ticker_source_run.get(row.ticker_source_run_id, []))
            for ticker_source_id, row in latest_sources.items()
        },
        {
            strategy_id: _candidate_run_payload(
                row,
                diagnostics=diagnostics_by_candidate_run.get(row.candidate_run_id, []),
                selection_counts=selection_counts_by_candidate_run.get(row.candidate_run_id, {}),
                admission_counts=admission_counts_by_candidate_run.get(row.candidate_run_id, {}),
            )
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
    quality_waterfall = _quality_waterfall_state(
        summary=summary,
        diagnostics=diagnostics,
        selection_counts=as_mapping(candidate_run.get("selection_counts")),
        admission_counts=as_mapping(candidate_run.get("admission_counts")),
    )
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
        "quality_profile_id": quality_waterfall.get("profile_id"),
        "filter_stage_counts": quality_waterfall.get("stage_counts"),
        "top_quality_blockers": quality_waterfall.get("top_blocker_reasons"),
        "quality_waterfall": quality_waterfall,
        "diagnostics": diagnostics,
        "latest_run": dict(candidate_run),
        "reason": "candidate_run_stale" if stale else ("no_candidates" if candidate_count == 0 else None),
    }


def _join_labels(labels: list[str]) -> str:
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} and {labels[1]}"
    return f"{', '.join(labels[:-1])}, and {labels[-1]}"


def _reason_matches_group(reason: str, prefixes: tuple[str, ...], exact: tuple[str, ...]) -> bool:
    return reason in exact or any(reason.startswith(prefix) for prefix in prefixes)


def _entry_blocker_groups(candidate_state: Mapping[str, Any]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for source_key in ("top_quality_blockers", "top_rejection_counts"):
        for reason, raw_count in as_mapping(candidate_state.get(source_key)).items():
            reason_text = str(reason or "").strip()
            count = coerce_int(raw_count) or 0
            if reason_text and count > 0:
                counts[reason_text] += count

    groups: list[dict[str, Any]] = []
    matched_reasons: set[str] = set()
    for group_id, label, prefixes, exact in NO_ENTRY_REASON_GROUPS:
        reasons = {reason: count for reason, count in counts.items() if _reason_matches_group(reason, prefixes, exact)}
        if not reasons:
            continue
        matched_reasons.update(reasons)
        groups.append(
            {
                "group": group_id,
                "label": label,
                "count": sum(reasons.values()),
                "reason_codes": dict(sorted(reasons.items(), key=lambda item: (-item[1], item[0]))),
            }
        )

    other_reasons = {reason: count for reason, count in counts.items() if reason not in matched_reasons}
    if other_reasons:
        groups.append(
            {
                "group": "other",
                "label": "other policy filters",
                "count": sum(other_reasons.values()),
                "reason_codes": dict(sorted(other_reasons.items(), key=lambda item: (-item[1], item[0]))),
            }
        )

    groups.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("group") or "")))
    return groups


def _entry_posture_state(
    *,
    source_state: Mapping[str, Any],
    candidate_state: Mapping[str, Any],
    market_open: bool,
    entry_due: bool,
) -> dict[str, Any]:
    source_status = str(source_state.get("status") or "unknown")
    candidate_status = str(candidate_state.get("status") or "unknown")
    source_symbol_count = coerce_int(source_state.get("symbol_count")) or 0
    candidate_count = coerce_int(candidate_state.get("candidate_count")) or 0
    blocker_groups = _entry_blocker_groups(candidate_state)

    if candidate_status in {"degraded", "blocked", "halted"}:
        return {
            "status": candidate_status,
            "state": "entry_evidence_needs_attention",
            "message": "Entry evidence is stale, missing, or degraded.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": candidate_state.get("reason"),
        }
    if source_status in {"degraded", "blocked", "halted"}:
        return {
            "status": source_status,
            "state": "source_needs_attention",
            "message": "Ticker source evidence is stale, missing, or degraded.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": source_state.get("reason"),
        }
    if not market_open:
        return {
            "status": "idle",
            "state": "market_closed",
            "message": "Market is closed; entry evaluation is idle.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": "market_closed",
        }
    if candidate_count > 0:
        return {
            "status": "healthy",
            "state": "candidates_available",
            "message": f"{candidate_count} entry candidate(s) are available for selection and admission.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": None,
        }
    if source_symbol_count == 0:
        return {
            "status": "healthy",
            "state": "flat_no_source_symbols",
            "message": "No entries: the latest source run retained no symbols.",
            "healthy_flat": True,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": candidate_state.get("reason") or source_state.get("reason"),
        }

    labels = [str(group.get("label")) for group in blocker_groups[:3] if group.get("label")]
    message = "No entries: latest run produced no candidates."
    if labels:
        message = f"No entries: {_join_labels(labels)} blocked the latest run."
    return {
        "status": "healthy",
        "state": "flat_by_policy",
        "message": message,
        "healthy_flat": True,
        "entry_due": entry_due,
        "primary_blocker_group": None if not blocker_groups else blocker_groups[0].get("group"),
        "blocker_groups": blocker_groups[:8],
        "reason": candidate_state.get("reason") or "no_candidates",
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
    broker_environment: str,
    broker_environment_source: str,
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
        entry_posture = _entry_posture_state(
            source_state=source_state,
            candidate_state=candidate_state,
            market_open=market_open,
            entry_due=entry_due,
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
        execution_contract = _strategy_execution_contract(
            strategy=strategy,
            broker_environment=broker_environment,
            broker_environment_source=broker_environment_source,
            now=now,
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
                "execution_contract": execution_contract,
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
                "entry_posture": entry_posture,
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
                    _execution_contract_status(execution_contract),
                ),
            }
        )
    return flows


def _strategy_not_active_reasons(strategy: Any, *, active: bool) -> list[str]:
    if active:
        return []
    reasons: list[str] = []
    if not bool(strategy.enabled):
        reasons.append("strategy_disabled")
    if bool(strategy.paused):
        reasons.append("strategy_paused")
    return reasons or ["strategy_not_scheduled"]


def _strategy_ops_posture(strategy: Any, *, active: bool) -> str:
    if active:
        return "active"
    if bool(strategy.paused):
        return "paused"
    mode = str(strategy.execution.mode or "shadow").strip().lower()
    if mode == "shadow":
        return "shadow_observation_candidate"
    if mode == "paper":
        return "paper_observation_candidate"
    if mode == "live":
        return "live_observation_candidate"
    return "observation_candidate"


def _strategy_not_active_message(reasons: list[str]) -> str | None:
    if not reasons:
        return None
    if "strategy_disabled" in reasons:
        return (
            "Authored strategy is disabled; listed for strategy-breadth observation only. "
            "No scheduler jobs, candidates, intents, or broker submission will be created."
        )
    if "strategy_paused" in reasons:
        return (
            "Authored strategy is paused; listed for operator context only. "
            "No scheduler jobs, candidates, intents, or broker submission will be created."
        )
    return "Strategy is not scheduled; no jobs, candidates, intents, or broker submission will be created."


def _strategy_routine_breadth_payload(routine: Any | None) -> dict[str, Any] | None:
    if routine is None:
        return None
    return {
        "enabled": routine.enabled,
        "schedule": routine.schedule.as_dict(),
        "quality_profile_id": routine.quality.profile_id,
        "quality_overrides": dict(routine.quality.overrides),
        "recipes": list(routine.recipes),
    }


def _latest_strategy_run_payloads(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not storage.signals.schema_ready():
        return {}
    latest_runs: dict[str, dict[str, Any]] = {}
    for strategy_id in sorted(strategy_ids):
        rows = storage.signals.list_strategy_runs(
            trading_strategy_id=strategy_id,
            session_date=market_date,
            limit=1,
        )
        if rows:
            latest_runs[strategy_id] = dict(rows[0])
    return latest_runs


def _latest_candidate_run_summary(candidate_run: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if candidate_run is None:
        return None
    return {
        "candidate_run_id": candidate_run.get("candidate_run_id"),
        "run_key": candidate_run.get("run_key"),
        "status": candidate_run.get("status"),
        "generated_at": candidate_run.get("generated_at"),
        "completed_at": candidate_run.get("completed_at"),
        "ticker_source_run_id": candidate_run.get("ticker_source_run_id"),
        "ticker_source_id": candidate_run.get("ticker_source_id"),
        "symbol_count": candidate_run.get("symbol_count"),
        "candidate_count": candidate_run.get("candidate_count"),
        "diagnostic_status": as_mapping(candidate_run.get("summary")).get("diagnostic_status"),
        "selection_counts": dict(as_mapping(candidate_run.get("selection_counts"))),
        "admission_counts": dict(as_mapping(candidate_run.get("admission_counts"))),
    }


def _strategy_latest_observation_state(
    *,
    strategy: Any,
    candidate_run: Mapping[str, Any] | None,
    strategy_run: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    entry_cadence_minutes = None if strategy.entry is None else strategy.entry.schedule.cadence_minutes
    candidate_state = _candidate_state(
        candidate_run=candidate_run,
        source_state=None,
        cadence_minutes=entry_cadence_minutes,
        market_open=False,
        now=now,
    )
    run_result = as_mapping(None if strategy_run is None else strategy_run.get("result"))
    entry_selection = as_mapping(run_result.get("entry_selection"))
    selection_counts = as_mapping(None if candidate_run is None else candidate_run.get("selection_counts"))
    admission_counts = as_mapping(None if candidate_run is None else candidate_run.get("admission_counts"))
    if strategy_run is None and candidate_run is None:
        status = "missing"
        reason = "observation_run_missing"
    else:
        status = as_text(None if strategy_run is None else strategy_run.get("status")) or str(candidate_state.get("status") or "observed")
        reason = as_text(run_result.get("reason")) or as_text(candidate_state.get("reason"))
    return {
        "status": status,
        "reason": reason,
        "entry_run_mode": as_text(run_result.get("entry_run_mode")),
        "validation_provenance": as_text(run_result.get("validation_provenance")),
        "observation_only": bool(run_result.get("observation_only")),
        "strategy_run_id": None if strategy_run is None else strategy_run.get("strategy_run_id"),
        "candidate_run_id": None if candidate_run is None else candidate_run.get("candidate_run_id"),
        "generated_at": candidate_state.get("latest_run", {}).get("generated_at") if isinstance(candidate_state.get("latest_run"), Mapping) else None,
        "age_seconds": candidate_state.get("age_seconds"),
        "candidate_count": candidate_state.get("candidate_count"),
        "signal_count": coerce_int(run_result.get("signal_count")) or 0,
        "selected_candidate_count": coerce_int(entry_selection.get("selected_candidate_count")) or 0,
        "monitored_candidate_count": coerce_int(entry_selection.get("monitored_candidate_count")) or 0,
        "rejected_candidate_count": coerce_int(entry_selection.get("rejected_candidate_count")) or 0,
        "decision_state_counts": dict(selection_counts),
        "admission_state_counts": dict(admission_counts),
        "quality_profile_id": candidate_state.get("quality_profile_id"),
        "top_rejection_counts": dict(as_mapping(candidate_state.get("top_rejection_counts"))),
        "latest_strategy_run": None if strategy_run is None else dict(strategy_run),
        "latest_candidate_run": _latest_candidate_run_summary(candidate_run),
    }


def _strategy_breadth_row(
    *,
    strategy: Any,
    active_strategy_ids: set[str],
    broker_environment: str,
    broker_environment_source: str,
    latest_candidate_run: Mapping[str, Any] | None,
    latest_strategy_run: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    active = strategy.trading_strategy_id in active_strategy_ids
    reasons = _strategy_not_active_reasons(strategy, active=active)
    ops_posture = _strategy_ops_posture(strategy, active=active)
    execution_contract = _strategy_execution_contract(
        strategy=strategy,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        now=now,
    )
    configured_automatic_submission_allowed = bool(execution_contract.get("automatic_submission_allowed"))
    execution_contract = {
        **execution_contract,
        "configured_automatic_submission_allowed": configured_automatic_submission_allowed,
        "automatic_submission_allowed": bool(active and configured_automatic_submission_allowed),
        "scheduler_active": active,
        "observation_only": not active,
        "rollout_blocker": None if active else reasons[0],
    }
    source_symbols = list(strategy.symbols[:SOURCE_SYMBOL_LIMIT])
    return {
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "trade_structure": strategy.trade_structure,
        "candidate_builder_key": strategy.candidate_builder_key,
        "build_profile": strategy.build_profile,
        "enabled": strategy.enabled,
        "paused": strategy.paused,
        "active": active,
        "status": "active" if active else ("paused" if "strategy_paused" in reasons else "available"),
        "ops_posture": ops_posture,
        "observation_only": not active,
        "scheduler_active": active,
        "not_active_reason": None if active else reasons[0],
        "not_active_reasons": reasons,
        "not_active_message": _strategy_not_active_message(reasons),
        "source": {
            **strategy.source.as_dict(),
            "symbol_count": len(strategy.symbols),
            "symbols": source_symbols,
            "symbol_limit": SOURCE_SYMBOL_LIMIT,
        },
        "entry": _strategy_routine_breadth_payload(strategy.entry),
        "management": _strategy_routine_breadth_payload(strategy.management),
        "risk_limits": strategy.risk_limits.as_dict(),
        "runtime": strategy.runtime.as_dict(),
        "execution": strategy.execution.as_dict(),
        "execution_mode": strategy.execution.mode,
        "approval_mode": strategy.execution.approval,
        "execution_runtime": strategy.execution.runtime,
        "execution_contract": execution_contract,
        "latest_observation": _strategy_latest_observation_state(
            strategy=strategy,
            candidate_run=latest_candidate_run,
            strategy_run=latest_strategy_run,
            now=now,
        ),
        "config_hash": strategy.config_hash,
        "config_path": str(strategy.config_path),
    }


def _project_strategy_breadth(
    *,
    storage: Any,
    market_date: str,
    broker_environment: str,
    broker_environment_source: str,
    trading_flows: list[dict[str, Any]],
    now: datetime,
) -> _StrategyBreadthProjection:
    strategies = list(load_trading_strategies().values())
    active_strategy_ids = {strategy_id for flow in trading_flows if (strategy_id := as_text(as_mapping(flow).get("trading_strategy_id"))) is not None}
    strategy_ids = {strategy.trading_strategy_id for strategy in strategies}
    _, latest_candidates = _latest_flow_facts(
        storage=storage,
        market_date=market_date,
        ticker_source_ids=set(),
        strategy_ids=strategy_ids,
    )
    latest_strategy_runs = _latest_strategy_run_payloads(
        storage=storage,
        market_date=market_date,
        strategy_ids=strategy_ids,
    )
    rows = [
        _strategy_breadth_row(
            strategy=strategy,
            active_strategy_ids=active_strategy_ids,
            broker_environment=broker_environment,
            broker_environment_source=broker_environment_source,
            latest_candidate_run=latest_candidates.get(strategy.trading_strategy_id),
            latest_strategy_run=latest_strategy_runs.get(strategy.trading_strategy_id),
            now=now,
        )
        for strategy in strategies
    ]
    rows.sort(
        key=lambda row: (
            0 if row.get("active") else 1,
            str(row.get("execution_mode") or ""),
            str(row.get("trade_structure") or ""),
            str(row.get("trading_strategy_id") or ""),
        )
    )
    available_rows = [row for row in rows if not bool(row.get("active")) and row.get("status") != "paused"]
    active_rows = [row for row in rows if bool(row.get("active"))]
    summary = {
        "strategy_count": len(rows),
        "active_strategy_count": len(active_rows),
        "inactive_strategy_count": len(rows) - len(active_rows),
        "available_strategy_count": len(available_rows),
        "available_shadow_strategy_count": sum(1 for row in available_rows if row.get("execution_mode") == "shadow"),
        "available_paper_strategy_count": sum(1 for row in available_rows if row.get("execution_mode") == "paper"),
        "available_live_strategy_count": sum(1 for row in available_rows if row.get("execution_mode") == "live"),
        "paused_strategy_count": sum(1 for row in rows if row.get("status") == "paused"),
        "trade_structure_counts": dict(Counter(str(row.get("trade_structure") or "unknown") for row in rows)),
        "execution_mode_counts": dict(Counter(str(row.get("execution_mode") or "unknown") for row in rows)),
        "ops_posture_counts": dict(Counter(str(row.get("ops_posture") or "unknown") for row in rows)),
    }
    payload = {
        "status": "ready",
        "summary": summary,
        "strategies": rows,
        "active_strategies": active_rows,
        "available_strategies": available_rows,
    }
    return _StrategyBreadthProjection(payload=payload, summary=summary)


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
    broker_environment: str,
    broker_environment_source: str,
    now: datetime,
) -> _FlowProjection:
    trading_flows = _build_trading_flows(
        storage=storage,
        engine_ops=engine_ops,
        market_date=market_date,
        market_open=market_open,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
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


def _execution_contract_attention(contract: Mapping[str, Any]) -> dict[str, str] | None:
    if bool(contract.get("environment_compatible")):
        return None
    strategy_id = as_text(contract.get("trading_strategy_id")) or "strategy"
    posture = as_text(contract.get("execution_posture")) or "unknown"
    broker_environment = as_text(contract.get("broker_environment")) or "unknown"
    reason = as_text(contract.get("environment_mismatch_reason")) or "environment_mismatch"
    severity = "medium" if reason == "live_execution_observed_on_alpaca_paper" else "high"
    message = f"{strategy_id} execution posture {posture} is not compatible with observed broker environment {broker_environment}."
    if reason == "live_execution_observed_on_alpaca_paper":
        message = (
            f"{strategy_id} is configured for live execution, but the observed broker is Alpaca paper; treat lifecycle evidence as rehearsal only."
        )
    elif reason == "paper_execution_requires_alpaca_paper":
        message = f"{strategy_id} is configured for paper execution, but the observed broker is not Alpaca paper."
    elif reason == "broker_environment_unknown":
        message = f"{strategy_id} cannot safely auto-submit because the broker environment is unknown."
    return _attention(
        severity=severity,
        code=reason,
        message=message,
    )


def _project_execution_contract(
    *,
    storage: Any,
    market_date: str,
    account_snapshot: Mapping[str, Any],
    trading_flows: list[dict[str, Any]],
) -> _ExecutionContractProjection:
    broker_environment = _normalize_broker_environment(account_snapshot.get("environment"))
    broker_environment_source = _broker_environment_source(account_snapshot)
    strategy_contracts = [as_mapping(flow.get("execution_contract")) for flow in trading_flows if isinstance(flow.get("execution_contract"), Mapping)]
    natural_strategy_ids = {
        strategy_id for contract in strategy_contracts if (strategy_id := as_text(contract.get("trading_strategy_id"))) is not None
    }
    contract_statuses = [_execution_contract_status(contract) for contract in strategy_contracts]
    attention = [attention_row for contract in strategy_contracts if (attention_row := _execution_contract_attention(contract)) is not None]
    overall_status = _combine_statuses(*contract_statuses) if contract_statuses else "unknown"
    latest_evidence = _latest_lifecycle_evidence(
        storage=storage,
        market_date=market_date,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        natural_strategy_ids=natural_strategy_ids,
    )
    primary_contract = next(
        (contract for contract in strategy_contracts if contract.get("trading_strategy_id") == "momentum_long_calls"),
        strategy_contracts[0] if strategy_contracts else {},
    )
    first_mismatch = next(
        (as_text(contract.get("environment_mismatch_reason")) for contract in strategy_contracts if not bool(contract.get("environment_compatible"))),
        None,
    )
    environment_compatible = bool(strategy_contracts) and all(bool(contract.get("environment_compatible")) for contract in strategy_contracts)
    summary = {
        "execution_contract_status": overall_status,
        "execution_posture": primary_contract.get("execution_posture"),
        "approval_mode": primary_contract.get("approval_mode"),
        "execution_runtime": primary_contract.get("execution_runtime"),
        "broker_environment": broker_environment,
        "broker_environment_source": broker_environment_source,
        "environment_compatible": environment_compatible,
        "environment_mismatch_reason": first_mismatch,
        "latest_natural_strategy_observed_at": as_mapping(latest_evidence.get("natural_strategy")).get("observed_at"),
        "latest_synthetic_validation_observed_at": as_mapping(latest_evidence.get("synthetic_validation")).get("observed_at"),
    }
    payload = {
        "status": overall_status,
        "broker": "alpaca",
        "broker_environment": broker_environment,
        "broker_environment_source": broker_environment_source,
        "environment_compatible": environment_compatible,
        "environment_mismatch_reason": first_mismatch,
        "strategy_contracts": strategy_contracts,
        "primary_strategy_contract": primary_contract,
        "latest_lifecycle_evidence": latest_evidence,
    }
    return _ExecutionContractProjection(
        payload=payload,
        summary=summary,
        statuses=(overall_status,),
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
    broker_environment = _normalize_broker_environment(account.account_snapshot.get("environment"))
    broker_environment_source = _broker_environment_source(account.account_snapshot)
    flows = _project_flows(
        storage=storage,
        engine_ops=engine.payload,
        market_date=market_control.market_date,
        market_open=market_control.market_open,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        now=now,
    )
    execution_contract = _project_execution_contract(
        storage=storage,
        market_date=market_control.market_date,
        account_snapshot=account.account_snapshot,
        trading_flows=flows.trading_flows,
    )
    broker_exposure = _broker_exposure_state(
        account_snapshot=account.account_snapshot,
        open_positions=positions.open_positions,
        broker_sync=account.broker_sync,
    )
    strategy_breadth = _project_strategy_breadth(
        storage=storage,
        market_date=market_control.market_date,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        trading_flows=flows.trading_flows,
        now=now,
    )

    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    for projection in (market_control, jobs, account, engine, execution, positions, alerts, flows, execution_contract):
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
    elif execution_contract.summary.get("environment_compatible") is False:
        trading_allowed = False
    elif execution.stale_open_execution_count or execution.submit_unknown_execution_count:
        trading_allowed = False

    active_intent_count = sum(int(as_mapping(flow.get("intent_state")).get("active_intent_count") or 0) for flow in flows.trading_flows)
    primary_flow = next(
        (flow for flow in flows.trading_flows if flow.get("trading_strategy_id") == "momentum_long_calls"),
        flows.trading_flows[0] if flows.trading_flows else {},
    )
    primary_entry_posture = as_mapping(primary_flow.get("entry_posture"))
    primary_capacity = as_mapping(primary_flow.get("capacity"))
    primary_position_state = as_mapping(primary_flow.get("position_state"))
    summary = {
        "market_date": market_control.market_date,
        "market_session_status": market_control.market_session.get("status"),
        "market_open_at": market_control.market_session.get("market_open_at"),
        "market_close_at": market_control.market_session.get("market_close_at"),
        "trading_allowed": trading_allowed,
        "environment": account.account_snapshot.get("environment"),
        **execution_contract.summary,
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
        "primary_entry_state": primary_entry_posture.get("state"),
        "primary_entry_message": primary_entry_posture.get("message"),
        "primary_entry_primary_blocker_group": primary_entry_posture.get("primary_blocker_group"),
        "primary_entry_healthy_flat": primary_entry_posture.get("healthy_flat"),
        "primary_entry_blocker_groups": primary_entry_posture.get("blocker_groups"),
        **strategy_breadth.summary,
        "broker_position_count": broker_exposure.get("broker_position_count"),
        "broker_option_position_count": broker_exposure.get("broker_option_position_count"),
        "spreads_managed_broker_option_position_count": broker_exposure.get("spreads_managed_option_position_count"),
        "external_manual_broker_option_position_count": broker_exposure.get("external_manual_option_position_count"),
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
        "broker_exposure": broker_exposure,
        "engine": engine.payload,
        "execution_contract": execution_contract.payload,
        "strategy_breadth": strategy_breadth.payload,
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

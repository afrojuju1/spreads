from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any


from core.storage.serializers import parse_datetime
from core.value_coercion import (
    as_mapping,
    as_text,
    utc_iso,
)

from core.services.ops.shared import (
    _attention,
    _combine_statuses,
)


from core.services.ops.trading.models import (
    LATEST_LIFECYCLE_PROVENANCES,
    LIFECYCLE_PROVENANCES,
    _ExecutionContractProjection,
)

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
        "executor_profile_id": strategy.execution.executor_profile_id,
        "order_style": strategy.execution.order_style.model_dump(),
        "quote_freshness": strategy.execution.quote_freshness.model_dump(),
        "open_lifecycle": strategy.execution.open_lifecycle.model_dump(),
        "close_lifecycle": strategy.execution.close_lifecycle.model_dump(),
        "unsupported_structure_behavior": strategy.execution.unsupported_structure_behavior,
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
        "observed_at": utc_iso(now),
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

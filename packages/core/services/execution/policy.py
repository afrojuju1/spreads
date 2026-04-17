from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.services.deployment_policy import (
    DEPLOYMENT_MODE_PAPER_AUTO,
    deployment_mode_auto_executes,
    resolve_execution_deployment_mode,
)
from core.services.exit_manager import normalize_exit_policy
from core.services.risk_manager import normalize_risk_policy
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
)
from core.storage.factory import build_job_repository
from core.storage.serializers import parse_datetime

from .shared import (
    DEFAULT_ENTRY_PRICING_MODE,
    DEFAULT_MAX_CREDIT_CONCESSION,
    DEFAULT_MIN_CREDIT_RETENTION_PCT,
    _clamp_fraction,
    _policy_ref,
)


def _validate_open_timing_window(
    *,
    exit_policy: dict[str, Any] | None,
    current_time: datetime,
    profile: str | None = None,
    deployment_mode: str | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_exit_policy(exit_policy)
    force_close_at_text = _as_text(normalized_policy.get("force_close_at"))
    force_close_at = (
        None if force_close_at_text is None else parse_datetime(force_close_at_text)
    )
    if force_close_at is None:
        return {
            "allowed": True,
            "reason": None,
            "message": None,
            "force_close_at": force_close_at_text,
            "minutes_to_force_close": None,
            "minimum_minutes_to_force_close": None,
        }
    from core.services.candidate_policy import resolve_deployment_quality_thresholds

    thresholds = resolve_deployment_quality_thresholds(profile)
    minimum_minutes_to_force_close = _coerce_float(
        thresholds.get("min_minutes_to_force_close")
    )
    if str(deployment_mode or "").strip().lower() == DEPLOYMENT_MODE_PAPER_AUTO:
        minimum_minutes_to_force_close = None
    minutes_to_force_close = round(
        max((force_close_at - current_time).total_seconds(), 0.0) / 60.0,
        1,
    )
    if current_time >= force_close_at:
        return {
            "allowed": False,
            "reason": "force_close_window_started",
            "message": (
                "Open execution is blocked because the exit force-close window has already started."
            ),
            "force_close_at": force_close_at.isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            ),
            "minutes_to_force_close": minutes_to_force_close,
            "minimum_minutes_to_force_close": minimum_minutes_to_force_close,
        }
    if (
        minimum_minutes_to_force_close is not None
        and minutes_to_force_close < minimum_minutes_to_force_close
    ):
        return {
            "allowed": False,
            "reason": "insufficient_time_to_force_close",
            "message": (
                "Open execution is blocked because only "
                f"{minutes_to_force_close:.1f} minutes remain before force-close at "
                f"{force_close_at.isoformat(timespec='seconds').replace('+00:00', 'Z')}, "
                f"below the {minimum_minutes_to_force_close:.1f}-minute deployment threshold."
            ),
            "force_close_at": force_close_at.isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            ),
            "minutes_to_force_close": minutes_to_force_close,
            "minimum_minutes_to_force_close": minimum_minutes_to_force_close,
        }
    return {
        "allowed": True,
        "reason": None,
        "message": None,
        "force_close_at": force_close_at.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "minutes_to_force_close": minutes_to_force_close,
        "minimum_minutes_to_force_close": minimum_minutes_to_force_close,
    }


def _attempt_exit_policy(
    attempt: Mapping[str, Any] | dict[str, Any],
) -> dict[str, Any] | None:
    request = attempt.get("request")
    if not isinstance(request, Mapping):
        return None
    exit_policy = request.get("exit_policy")
    if not isinstance(exit_policy, Mapping):
        return None
    return dict(exit_policy)


def normalize_execution_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("execution_policy")
    if not isinstance(raw_policy, dict) and {
        "enabled",
        "deployment_mode",
        "mode",
        "quantity",
        "pricing_mode",
        "min_credit_retention_pct",
        "max_credit_concession",
    } & set(source):
        raw_policy = source
    if isinstance(raw_policy, dict):
        quantity = _coerce_int(raw_policy.get("quantity")) or 1
        pricing_mode = (
            _as_text(raw_policy.get("pricing_mode")) or DEFAULT_ENTRY_PRICING_MODE
        )
        min_credit_retention_pct = (
            _coerce_float(raw_policy.get("min_credit_retention_pct"))
            or DEFAULT_MIN_CREDIT_RETENTION_PCT
        )
        max_credit_concession = (
            _coerce_float(raw_policy.get("max_credit_concession"))
            or DEFAULT_MAX_CREDIT_CONCESSION
        )
        deployment_mode = resolve_execution_deployment_mode(
            raw_policy,
            risk_policy=(
                source.get("risk_policy")
                if isinstance(source.get("risk_policy"), Mapping)
                else None
            ),
        )
    else:
        deployment_mode = "shadow"
        quantity = 1
        pricing_mode = DEFAULT_ENTRY_PRICING_MODE
        min_credit_retention_pct = DEFAULT_MIN_CREDIT_RETENTION_PCT
        max_credit_concession = DEFAULT_MAX_CREDIT_CONCESSION
    enabled = deployment_mode_auto_executes(deployment_mode)
    if pricing_mode not in {
        "midpoint",
        "adaptive_credit",
        "adaptive_debit",
        "adaptive",
    }:
        raise ValueError(f"Unsupported execution pricing mode: {pricing_mode}")
    min_credit_retention_pct = _clamp_fraction(
        min_credit_retention_pct, minimum=0.5, maximum=1.0
    )
    max_credit_concession = max(float(max_credit_concession), 0.0)
    if not enabled:
        return {
            "enabled": False,
            "deployment_mode": deployment_mode,
            "mode": "disabled",
            "quantity": quantity,
            "pricing_mode": pricing_mode,
            "min_credit_retention_pct": min_credit_retention_pct,
            "max_credit_concession": max_credit_concession,
        }
    mode = _as_text(raw_policy.get("mode")) if isinstance(raw_policy, dict) else None
    mode = mode or "top_promotable"
    if mode != "top_promotable":
        raise ValueError(f"Unsupported execution policy mode: {mode}")
    return {
        "enabled": True,
        "deployment_mode": deployment_mode,
        "mode": "top_promotable",
        "quantity": max(quantity, 1),
        "pricing_mode": pricing_mode,
        "min_credit_retention_pct": min_credit_retention_pct,
        "max_credit_concession": max_credit_concession,
    }


def _resolve_source_policies(
    *,
    cycle: dict[str, Any],
    job_store: Any | None = None,
) -> dict[str, Any]:
    job_run_id = _as_text(cycle.get("job_run_id"))
    if job_run_id is None:
        return {
            "source_job_type": None,
            "source_job_key": None,
            "source_job_run_id": None,
            "execution_policy": normalize_execution_policy(None),
            "risk_policy": normalize_risk_policy(None),
            "exit_policy": normalize_exit_policy(None),
        }
    resolved_job_store = build_job_repository() if job_store is None else job_store
    job_run = resolved_job_store.get_job_run(job_run_id)
    payload = {} if job_run is None else dict(job_run["payload"])
    return {
        "source_job_type": None
        if job_run is None
        else _as_text(job_run.get("job_type")),
        "source_job_key": None if job_run is None else _as_text(job_run.get("job_key")),
        "source_job_run_id": job_run_id,
        "execution_policy": normalize_execution_policy(
            {
                "execution_policy": payload.get("execution_policy"),
                "risk_policy": payload.get("risk_policy"),
            }
        ),
        "risk_policy": normalize_risk_policy(payload.get("risk_policy")),
        "exit_policy": normalize_exit_policy(payload.get("exit_policy")),
    }


def _policy_source_kind(
    *,
    request_metadata: dict[str, Any] | None,
    policy_name: str,
    source_job_key: str | None,
    rollout: dict[str, Any] | None,
) -> str:
    if isinstance(request_metadata, dict) and isinstance(
        request_metadata.get(policy_name), dict
    ):
        return "request_override"
    if rollout is not None:
        return "policy_rollout"
    if source_job_key is not None:
        return "source_job"
    return "default"


def _requested_policy_payload(
    *,
    request_metadata: dict[str, Any] | None,
    policy_name: str,
    source_policies: dict[str, Any],
    active_policy_rollouts: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if isinstance(request_metadata, dict) and isinstance(
        request_metadata.get(policy_name), dict
    ):
        return dict(request_metadata[policy_name])
    rollout = active_policy_rollouts.get(policy_name)
    if rollout is not None and isinstance(rollout.get("policy"), dict):
        return dict(rollout["policy"])
    return dict(source_policies[policy_name])


def _build_policy_refs(
    *,
    request_metadata: dict[str, Any] | None,
    source_policies: dict[str, Any],
    active_policy_rollouts: dict[str, dict[str, Any]],
    resolved_risk_policy: dict[str, Any],
    resolved_execution_policy: dict[str, Any],
    resolved_exit_policy: dict[str, Any],
) -> dict[str, Any]:
    source_job_key = _as_text(source_policies.get("source_job_key"))
    source_job_run_id = _as_text(source_policies.get("source_job_run_id"))
    risk_rollout = active_policy_rollouts.get("risk_policy")
    execution_rollout = active_policy_rollouts.get("execution_policy")
    exit_rollout = active_policy_rollouts.get("exit_policy")
    return {
        "risk_policy": _policy_ref(
            family="risk_policy",
            resolved_policy=resolved_risk_policy,
            source_kind=_policy_source_kind(
                request_metadata=request_metadata,
                policy_name="risk_policy",
                source_job_key=source_job_key,
                rollout=risk_rollout,
            ),
            source_key=(
                str(risk_rollout["policy_rollout_id"])
                if risk_rollout is not None
                else ("risk_policy" if source_job_key is None else source_job_key)
            ),
            source_job_key=None if risk_rollout is not None else source_job_key,
            source_job_run_id=None if risk_rollout is not None else source_job_run_id,
            version_token=None
            if risk_rollout is None
            else str(risk_rollout["version_token"]),
            extra=(
                {}
                if risk_rollout is None
                else {
                    "policy_rollout_id": str(risk_rollout["policy_rollout_id"]),
                    "operator_action_id": _as_text(
                        risk_rollout.get("operator_action_id")
                    ),
                }
            ),
        ),
        "execution_policy": _policy_ref(
            family="execution_policy",
            resolved_policy=resolved_execution_policy,
            source_kind=_policy_source_kind(
                request_metadata=request_metadata,
                policy_name="execution_policy",
                source_job_key=source_job_key,
                rollout=execution_rollout,
            ),
            source_key=(
                str(execution_rollout["policy_rollout_id"])
                if execution_rollout is not None
                else ("execution_policy" if source_job_key is None else source_job_key)
            ),
            source_job_key=None if execution_rollout is not None else source_job_key,
            source_job_run_id=None
            if execution_rollout is not None
            else source_job_run_id,
            version_token=None
            if execution_rollout is None
            else str(execution_rollout["version_token"]),
            extra=(
                {}
                if execution_rollout is None
                else {
                    "policy_rollout_id": str(execution_rollout["policy_rollout_id"]),
                    "operator_action_id": _as_text(
                        execution_rollout.get("operator_action_id")
                    ),
                }
            ),
        ),
        "exit_policy": _policy_ref(
            family="exit_policy",
            resolved_policy=resolved_exit_policy,
            source_kind=_policy_source_kind(
                request_metadata=request_metadata,
                policy_name="exit_policy",
                source_job_key=source_job_key,
                rollout=exit_rollout,
            ),
            source_key=(
                str(exit_rollout["policy_rollout_id"])
                if exit_rollout is not None
                else ("exit_policy" if source_job_key is None else source_job_key)
            ),
            source_job_key=None if exit_rollout is not None else source_job_key,
            source_job_run_id=None if exit_rollout is not None else source_job_run_id,
            version_token=None
            if exit_rollout is None
            else str(exit_rollout["version_token"]),
            extra=(
                {}
                if exit_rollout is None
                else {
                    "policy_rollout_id": str(exit_rollout["policy_rollout_id"]),
                    "operator_action_id": _as_text(
                        exit_rollout.get("operator_action_id")
                    ),
                }
            ),
        ),
    }

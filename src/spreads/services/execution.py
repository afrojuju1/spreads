from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Mapping
from uuid import uuid4

from arq import create_pool

from spreads.db.decorators import with_storage
from spreads.events.bus import publish_global_event_sync
from spreads.integrations.alpaca.client import AlpacaClient, AlpacaRequestError
from spreads.jobs.registry import (
    EXECUTION_SUBMIT_ADHOC_JOB_KEY,
    EXECUTION_SUBMIT_JOB_TYPE,
    get_job_spec,
)
from spreads.runtime.config import default_redis_url
from spreads.runtime.redis import build_redis_settings
from spreads.services.alpaca import create_alpaca_client_from_env
from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
)
from spreads.services.control_plane import (
    OPEN_ACTIVITY_AUTO,
    OPEN_ACTIVITY_MANUAL,
    assess_open_activity_gate,
    get_active_policy_rollout_map,
    publish_control_gate_event,
)
from spreads.services.exit_manager import (
    normalize_exit_policy,
    resolve_exit_policy_snapshot,
)
from spreads.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUSES,
    PENDING_SUBMISSION_GRACE_SECONDS,
    PENDING_SUBMISSION_RUNNING_STALE_AFTER_SECONDS,
    PENDING_SUBMISSION_STATUS,
    SUBMIT_UNKNOWN_STATUS,
    TERMINAL_ATTEMPT_STATUSES,
    classify_open_execution_attempt,
    is_terminal_execution_attempt_status,
    resolve_execution_submit_job_run_id,
    resolve_execution_attempt_source_job,
)
from spreads.services.runtime_identity import (
    build_live_session_id,
    build_pipeline_id,
    resolve_pipeline_policy_fields,
)
from spreads.services.risk_manager import (
    evaluate_open_execution,
    normalize_risk_policy,
    validate_close_execution,
)
from spreads.services.scanner import make_close_order_payload
from spreads.services.signal_state import publish_opportunity_event
from spreads.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
    resolve_trade_intent,
    sync_session_position_from_attempt,
)
from spreads.storage.factory import build_job_repository
from spreads.storage.serializers import parse_datetime

BROKER_NAME = "alpaca"
EXECUTION_SCHEMA_MESSAGE = (
    "Execution tables are not available yet. Run the latest Alembic migrations."
)
OPEN_STATUSES = OPEN_ATTEMPT_STATUSES
TERMINAL_STATUSES = TERMINAL_ATTEMPT_STATUSES
DEFAULT_ENTRY_PRICING_MODE = "adaptive_credit"
DEFAULT_MIN_CREDIT_RETENTION_PCT = 0.95
DEFAULT_MAX_CREDIT_CONCESSION = 0.02
ATTEMPT_CONTEXT_BUCKET_MIRROR = {
    "open_promotable": "promotable",
    "open_monitor": "monitor",
    "position_close": "position_close",
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _normalize_attempt_context(value: Any) -> str | None:
    normalized = _as_text(value)
    if normalized == "promotable":
        return "open_promotable"
    if normalized == "monitor":
        return "open_monitor"
    return normalized


def _deprecated_bucket(value: Any) -> str | None:
    normalized = _as_text(value)
    if normalized is None:
        return None
    return ATTEMPT_CONTEXT_BUCKET_MIRROR.get(normalized, normalized)


def _validate_open_timing_window(
    *,
    exit_policy: dict[str, Any] | None,
    current_time: datetime,
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
        }
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
        }
    return {
        "allowed": True,
        "reason": None,
        "message": None,
        "force_close_at": force_close_at.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
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


def _execution_submit_job_run(
    storage: Any, execution_attempt_id: str
) -> Mapping[str, Any] | None:
    job_store = getattr(storage, "jobs", None)
    if (
        job_store is None
        or (
            hasattr(job_store, "schema_ready")
            and not job_store.schema_ready()
        )
    ):
        return None
    try:
        return job_store.get_job_run(
            resolve_execution_submit_job_run_id(execution_attempt_id)
        )
    except Exception:
        return None


def _source_job_definition(
    storage: Any, attempt: Mapping[str, Any]
) -> Mapping[str, Any] | None:
    source_job = resolve_execution_attempt_source_job(attempt)
    source_job_key = _as_text(source_job.get("job_key"))
    job_store = getattr(storage, "jobs", None)
    if (
        source_job_key is None
        or job_store is None
        or (
            hasattr(job_store, "schema_ready")
            and not job_store.schema_ready()
        )
    ):
        return None
    try:
        return job_store.get_job_definition(source_job_key)
    except Exception:
        return None


def _evaluate_open_attempt_guard(
    *,
    storage: Any,
    attempt: Mapping[str, Any],
    current_time: datetime,
) -> dict[str, Any]:
    lifecycle = classify_open_execution_attempt(
        attempt,
        now=current_time,
        submit_job=_execution_submit_job_run(
            storage, str(attempt.get("execution_attempt_id") or "")
        ),
        source_job_definition=_source_job_definition(storage, attempt),
        submission_grace_seconds=PENDING_SUBMISSION_GRACE_SECONDS,
        running_submit_stale_after_seconds=PENDING_SUBMISSION_RUNNING_STALE_AFTER_SECONDS,
    )
    timing_gate = _validate_open_timing_window(
        exit_policy=_attempt_exit_policy(attempt),
        current_time=current_time,
    )
    if str(lifecycle.get("phase") or "") == "submit_unknown":
        return {
            **dict(timing_gate),
            "lifecycle": lifecycle,
            "age_seconds": lifecycle.get("age_seconds"),
            "stale_after_seconds": lifecycle.get("working_stale_after_seconds"),
        }
    if not timing_gate["allowed"]:
        return {
            **dict(timing_gate),
            "lifecycle": lifecycle,
            "intervention": "cancel_order"
            if _as_text(attempt.get("broker_order_id"))
            else "fail_unsubmitted",
        }

    intervention = _as_text(lifecycle.get("intervention"))
    if intervention is None:
        return {
            **dict(timing_gate),
            "lifecycle": lifecycle,
            "age_seconds": lifecycle.get("age_seconds"),
            "stale_after_seconds": lifecycle.get("working_stale_after_seconds"),
        }

    reason = (
        "stale_auto_open_attempt"
        if intervention == "cancel_order"
        else (
            "submit_outcome_uncertain"
            if intervention == "mark_submit_unknown"
            else "stale_pending_submission"
        )
    )
    return {
        "allowed": False,
        "reason": reason,
        "message": _as_text(lifecycle.get("note")),
        "force_close_at": timing_gate.get("force_close_at"),
        "age_seconds": lifecycle.get("age_seconds"),
        "stale_after_seconds": lifecycle.get("working_stale_after_seconds"),
        "queue_grace_seconds": lifecycle.get("submission_grace_seconds"),
        "submit_job_status": lifecycle.get("submit_job_status"),
        "intervention": intervention,
        "lifecycle": lifecycle,
    }


def _guard_intervention_message(
    guard_decision: Mapping[str, Any],
    *,
    submitted: bool,
) -> str:
    reason = _as_text(guard_decision.get("reason"))
    direct_message = _as_text(guard_decision.get("message"))
    if reason in {"stale_pending_submission", "submit_outcome_uncertain"}:
        return direct_message or "Execution needs operator reconciliation."
    if reason == "stale_auto_open_attempt":
        age_seconds = _coerce_float(guard_decision.get("age_seconds"))
        stale_after_seconds = _coerce_int(guard_decision.get("stale_after_seconds"))
        age_fragment = (
            ""
            if age_seconds is None
            else f" after {int(round(age_seconds))}s"
        )
        threshold_fragment = (
            ""
            if stale_after_seconds is None
            else f" (stale threshold {stale_after_seconds}s)"
        )
        if submitted:
            return (
                "Canceled automatic open execution because the order remained pending"
                f"{age_fragment}{threshold_fragment}."
            )
        return (
            "Automatic open execution expired before broker submission because it remained pending"
            f"{age_fragment}{threshold_fragment}."
        )

    if submitted:
        return (
            "Canceled open execution because the exit force-close window "
            "started before the order completed."
        )
    return (
        "Open execution expired because the exit force-close window started "
        "before broker submission."
    )


def _reconcile_submit_unknown_attempt(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    client: AlpacaClient,
) -> dict[str, Any] | None:
    client_order_id = _as_text(attempt.get("client_order_id"))
    if client_order_id is None:
        return None
    try:
        order_snapshot = client.get_order_by_client_order_id(
            client_order_id,
            nested=True,
        )
    except AlpacaRequestError as exc:
        if exc.status_code == 404:
            return None
        raise
    return _sync_attempt_state(
        execution_store=execution_store,
        attempt=dict(attempt),
        client=client,
        order_snapshot=order_snapshot,
    )


def _execution_attempt_id() -> str:
    return f"execution:{uuid4().hex}"


def _risk_decision_id() -> str:
    return f"risk_decision:{uuid4().hex}"


def _execution_client_order_id() -> str:
    return f"spr-exec-{uuid4().hex[:20]}"


def _execution_submit_job_run_id(execution_attempt_id: str) -> str:
    return resolve_execution_submit_job_run_id(execution_attempt_id)


def _order_intent_key(execution_attempt_id: str) -> str:
    return f"order_intent:{execution_attempt_id}"


def _is_terminal_status(status: str | None) -> bool:
    return is_terminal_execution_attempt_status(status)


def _policy_version_token(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:12]


def _policy_ref(
    *,
    family: str,
    resolved_policy: dict[str, Any],
    source_kind: str,
    source_key: str,
    source_job_key: str | None,
    source_job_run_id: str | None,
    version_token: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "family": family,
        "key": source_key,
        "version": version_token or _policy_version_token(resolved_policy),
        "source_kind": source_kind,
        "source_job_key": source_job_key,
        "source_job_run_id": source_job_run_id,
    }
    if extra:
        payload.update(extra)
    return payload


def _candidate_with_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = candidate.get("candidate")
    if isinstance(payload, dict):
        return {
            **dict(candidate),
            **dict(payload),
        }
    return dict(candidate)


def _validate_auto_execution_candidate(
    candidate: dict[str, Any],
) -> tuple[str | None, str | None]:
    profile = _as_text(candidate.get("profile"))
    if profile != "0dte":
        return None, None
    setup_status = _as_text(candidate.get("setup_status")) or "unknown"
    if setup_status != "favorable":
        return (
            "setup_not_favorable",
            "Automatic 0DTE execution is limited to favorable technical setups.",
        )
    if not candidate_has_intraday_setup_context(candidate):
        return (
            "awaiting_intraday_setup",
            "Automatic 0DTE execution requires persisted intraday setup context on the selected candidate.",
        )
    return None, None


def _clamp_fraction(
    value: float, *, minimum: float = 0.0, maximum: float = 1.0
) -> float:
    return max(minimum, min(maximum, float(value)))


def _normalize_credit_limit(value: Any) -> float | None:
    numeric = _coerce_float(value)
    if numeric is None or numeric == 0:
        return None
    return abs(numeric)


def _resolve_candidate_credit_prices(
    candidate_payload: dict[str, Any],
) -> tuple[float | None, float | None]:
    midpoint_credit = _normalize_credit_limit(candidate_payload.get("midpoint_credit"))
    natural_credit = _normalize_credit_limit(candidate_payload.get("natural_credit"))
    return midpoint_credit, natural_credit


def _quote_record_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    return (
        _as_text(record.get("quote_timestamp")) or "",
        _as_text(record.get("captured_at")) or "",
    )


def _latest_quote_records_by_symbol(
    quote_records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    latest: dict[str, tuple[tuple[str, str], dict[str, Any]]] = {}
    for record in quote_records:
        symbol = _as_text(record.get("option_symbol"))
        if symbol is None:
            continue
        sort_key = _quote_record_sort_key(record)
        current = latest.get(symbol)
        if current is None or sort_key >= current[0]:
            latest[symbol] = (sort_key, dict(record))
    return {symbol: row for symbol, (_, row) in latest.items()}


def _resolve_reactive_quote_snapshot(
    candidate: dict[str, Any],
    quote_records: list[dict[str, Any]],
) -> dict[str, Any] | None:
    latest_quotes = _latest_quote_records_by_symbol(quote_records)
    short_symbol = _as_text(candidate.get("short_symbol"))
    long_symbol = _as_text(candidate.get("long_symbol"))
    if short_symbol is None or long_symbol is None:
        return None
    short_quote = latest_quotes.get(short_symbol)
    long_quote = latest_quotes.get(long_symbol)
    if short_quote is None or long_quote is None:
        return None

    short_midpoint = _coerce_float(short_quote.get("midpoint"))
    long_midpoint = _coerce_float(long_quote.get("midpoint"))
    short_bid = _coerce_float(short_quote.get("bid"))
    long_ask = _coerce_float(long_quote.get("ask"))
    if (
        short_midpoint is None
        or long_midpoint is None
        or short_bid is None
        or long_ask is None
    ):
        return None

    captured_at = max(
        _as_text(short_quote.get("quote_timestamp"))
        or _as_text(short_quote.get("captured_at"))
        or "",
        _as_text(long_quote.get("quote_timestamp"))
        or _as_text(long_quote.get("captured_at"))
        or "",
    )
    return {
        "captured_at": captured_at or None,
        "short_symbol": short_symbol,
        "long_symbol": long_symbol,
        "midpoint_credit": round(short_midpoint - long_midpoint, 4),
        "natural_credit": round(short_bid - long_ask, 4),
        "short_bid": short_bid,
        "short_ask": _coerce_float(short_quote.get("ask")),
        "long_bid": _coerce_float(long_quote.get("bid")),
        "long_ask": long_ask,
    }


def _resolve_reactive_auto_execution(
    *,
    candidate: dict[str, Any],
    execution_policy: dict[str, Any],
    quote_records: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    if not quote_records:
        return {
            "ok": False,
            "reason": "awaiting_reactive_quotes",
            "message": "Automatic 0DTE execution skipped because reactive quote capture did not return any quotes.",
        }

    live_snapshot = _resolve_reactive_quote_snapshot(candidate, quote_records)
    if live_snapshot is None:
        return {
            "ok": False,
            "reason": "awaiting_reactive_quotes",
            "message": "Automatic 0DTE execution skipped because a current two-leg quote snapshot was not available.",
        }

    live_midpoint_credit = _normalize_credit_limit(live_snapshot.get("midpoint_credit"))
    live_natural_credit = _normalize_credit_limit(live_snapshot.get("natural_credit"))
    if (
        live_midpoint_credit is None
        or live_natural_credit is None
        or live_midpoint_credit <= 0
        or live_natural_credit <= 0
    ):
        return {
            "ok": False,
            "reason": "live_quotes_not_executable",
            "message": "Automatic 0DTE execution skipped because live spread quotes were not executable.",
            "reactive_quote": live_snapshot,
        }

    scanned_midpoint_credit = _normalize_credit_limit(candidate.get("midpoint_credit"))
    if scanned_midpoint_credit is not None:
        credit_floor = round(
            scanned_midpoint_credit
            * float(execution_policy["min_credit_retention_pct"]),
            4,
        )
        if live_midpoint_credit < credit_floor:
            return {
                "ok": False,
                "reason": "live_credit_below_floor",
                "message": (
                    "Automatic 0DTE execution skipped because the live spread credit fell below the retention floor."
                ),
                "reactive_quote": {
                    **live_snapshot,
                    "credit_floor": credit_floor,
                },
            }

    pricing_candidate = {
        **candidate,
        "midpoint_credit": live_midpoint_credit,
        "natural_credit": live_natural_credit,
        "fill_ratio": 0.0
        if live_midpoint_credit <= 0
        else round(live_natural_credit / live_midpoint_credit, 4),
    }
    limit_price = _resolve_open_limit_price(
        candidate_payload=pricing_candidate,
        explicit_limit_price=None,
        execution_policy=execution_policy,
    )
    return {
        "ok": True,
        "limit_price": limit_price,
        "reactive_quote": {
            **live_snapshot,
            "fill_ratio": pricing_candidate["fill_ratio"],
            "limit_price": limit_price,
        },
    }


def _resolve_open_limit_price(
    *,
    candidate_payload: dict[str, Any],
    explicit_limit_price: float | None,
    execution_policy: dict[str, Any],
) -> float:
    explicit_credit = _normalize_credit_limit(explicit_limit_price)
    if explicit_credit is not None:
        return round(max(explicit_credit, 0.01), 2)

    midpoint_credit, natural_credit = _resolve_candidate_credit_prices(
        candidate_payload
    )
    if midpoint_credit is None:
        order_payload = dict(candidate_payload.get("order_payload") or {})
        midpoint_credit = _normalize_credit_limit(order_payload.get("limit_price"))
    if midpoint_credit is None or midpoint_credit <= 0:
        raise ValueError("Execution limit price must be positive")

    pricing_mode = str(
        execution_policy.get("pricing_mode") or DEFAULT_ENTRY_PRICING_MODE
    )
    if pricing_mode == "midpoint" or natural_credit is None or natural_credit <= 0:
        return round(max(midpoint_credit, 0.01), 2)

    fill_ratio = _clamp_fraction(
        _coerce_float(candidate_payload.get("fill_ratio")) or 0.0, maximum=1.0
    )
    min_credit_retention_pct = _clamp_fraction(
        _coerce_float(execution_policy.get("min_credit_retention_pct"))
        or DEFAULT_MIN_CREDIT_RETENTION_PCT,
        minimum=0.5,
        maximum=1.0,
    )
    max_credit_concession = max(
        _coerce_float(execution_policy.get("max_credit_concession"))
        or DEFAULT_MAX_CREDIT_CONCESSION,
        0.0,
    )
    credit_floor = max(natural_credit, midpoint_credit * min_credit_retention_pct, 0.01)
    max_concession_to_floor = max(midpoint_credit - credit_floor, 0.0)
    fill_ratio_concession = max(midpoint_credit - natural_credit, 0.0) * max(
        1.0 - fill_ratio, 0.0
    )
    concession = min(
        fill_ratio_concession, max_credit_concession, max_concession_to_floor
    )
    return round(max(midpoint_credit - concession, credit_floor, 0.01), 2)


def _classify_auto_execution_block(exc: Exception) -> dict[str, Any] | None:
    if not isinstance(exc, ValueError):
        return None
    message = str(exc).strip()
    if not message:
        return None
    if message.startswith("Open execution exceeds ") and message.endswith("."):
        constraint = message.removeprefix("Open execution exceeds ").removesuffix(".")
        return {
            "reason": "risk_policy_blocked",
            "message": message,
            "block_category": "risk_policy",
            "constraint": constraint,
        }
    if message == "Open execution is blocked because the quote snapshot is stale.":
        return {
            "reason": "stale_quote",
            "message": message,
            "block_category": "quote_freshness",
        }
    if (
        message
        == "Open execution is blocked because the exit force-close window has already started."
    ):
        return {
            "reason": "force_close_window_started",
            "message": message,
            "block_category": "timing_window",
        }
    if message == "Execution is blocked by SPREADS_EXECUTION_KILL_SWITCH.":
        return {
            "reason": "kill_switch_blocked",
            "message": message,
            "block_category": "kill_switch",
        }
    if message == "Open execution is blocked because control mode is halted.":
        return {
            "reason": "control_mode_halted",
            "message": message,
            "block_category": "control_mode",
        }
    if message.startswith("Open execution is blocked on a live Alpaca account."):
        return {
            "reason": "environment_blocked",
            "message": message,
            "block_category": "environment",
        }
    return None


def _resolve_completed_at(order: dict[str, Any]) -> str | None:
    for key in ("filled_at", "canceled_at", "expired_at", "failed_at", "updated_at"):
        value = _as_text(order.get(key))
        if value:
            return value
    return None


def _require_execution_schema(execution_store: Any) -> None:
    if not execution_store.schema_ready():
        raise RuntimeError(EXECUTION_SCHEMA_MESSAGE)


def _require_position_schema(execution_store: Any) -> None:
    if not execution_store.positions_schema_ready():
        raise RuntimeError(EXECUTION_SCHEMA_MESSAGE)


def _ensure_execution_submit_job_definition(job_store: Any) -> None:
    job_store.upsert_job_definition(
        job_key=EXECUTION_SUBMIT_ADHOC_JOB_KEY,
        job_type=EXECUTION_SUBMIT_JOB_TYPE,
        enabled=False,
        schedule_type="manual",
        schedule={},
        payload={},
        singleton_scope=None,
    )


def _enqueue_ad_hoc_job(
    *,
    job_type: str,
    job_key: str,
    job_run_id: str,
    arq_job_id: str,
    payload: dict[str, Any],
) -> Any:
    spec = get_job_spec(job_type)
    if spec is None:
        raise RuntimeError(f"Job type is not registered: {job_type}")

    async def _enqueue() -> Any:
        redis = await create_pool(build_redis_settings(default_redis_url()))
        try:
            return await redis.enqueue_job(
                spec.task_name,
                job_key,
                job_run_id,
                payload,
                arq_job_id,
                _job_id=arq_job_id,
                _queue_name=spec.queue_name,
            )
        finally:
            await redis.aclose()

    return asyncio.run(_enqueue())


def normalize_execution_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("execution_policy")
    if not isinstance(raw_policy, dict) and {
        "enabled",
        "mode",
        "quantity",
        "pricing_mode",
        "min_credit_retention_pct",
        "max_credit_concession",
    } & set(source):
        raw_policy = source
    if isinstance(raw_policy, dict):
        enabled = bool(raw_policy.get("enabled"))
        mode = _as_text(raw_policy.get("mode")) or "disabled"
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
    else:
        enabled = False
        mode = "disabled"
        quantity = 1
        pricing_mode = DEFAULT_ENTRY_PRICING_MODE
        min_credit_retention_pct = DEFAULT_MIN_CREDIT_RETENTION_PCT
        max_credit_concession = DEFAULT_MAX_CREDIT_CONCESSION
    if pricing_mode not in {"midpoint", "adaptive_credit"}:
        raise ValueError(f"Unsupported execution pricing mode: {pricing_mode}")
    min_credit_retention_pct = _clamp_fraction(
        min_credit_retention_pct, minimum=0.5, maximum=1.0
    )
    max_credit_concession = max(float(max_credit_concession), 0.0)
    if not enabled:
        return {
            "enabled": False,
            "mode": "disabled",
            "quantity": quantity,
            "pricing_mode": pricing_mode,
            "min_credit_retention_pct": min_credit_retention_pct,
            "max_credit_concession": max_credit_concession,
        }
    if mode != "top_promotable":
        raise ValueError(f"Unsupported execution policy mode: {mode}")
    return {
        "enabled": True,
        "mode": "top_promotable",
        "quantity": max(quantity, 1),
        "pricing_mode": pricing_mode,
        "min_credit_retention_pct": min_credit_retention_pct,
        "max_credit_concession": max_credit_concession,
    }


def _attach_attempt_details(
    *,
    execution_store: Any,
    attempts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not attempts:
        return []
    attempt_ids = [str(item["execution_attempt_id"]) for item in attempts]
    orders = execution_store.list_orders(execution_attempt_ids=attempt_ids)
    fills = execution_store.list_fills(execution_attempt_ids=attempt_ids)
    orders_by_attempt: dict[str, list[dict[str, Any]]] = {}
    fills_by_attempt: dict[str, list[dict[str, Any]]] = {}

    for order in orders:
        orders_by_attempt.setdefault(str(order["execution_attempt_id"]), []).append(
            dict(order)
        )
    for fill in fills:
        fills_by_attempt.setdefault(str(fill["execution_attempt_id"]), []).append(
            dict(fill)
        )

    payloads: list[dict[str, Any]] = []
    for attempt in attempts:
        attempt_context = _normalize_attempt_context(
            attempt.get("attempt_context", attempt.get("bucket"))
        )
        payloads.append(
            {
                **attempt,
                "attempt_context": attempt_context,
                "bucket": _deprecated_bucket(attempt_context),
                "order_intent_id": str(attempt["execution_attempt_id"]),
                "order_intent_key": _order_intent_key(
                    str(attempt["execution_attempt_id"])
                ),
                "orders": orders_by_attempt.get(
                    str(attempt["execution_attempt_id"]), []
                ),
                "fills": fills_by_attempt.get(
                    str(attempt["execution_attempt_id"]), []
                ),
            }
        )
    return payloads


@with_storage()
def list_session_execution_attempts(
    *,
    db_target: str,
    session_id: str,
    limit: int = 20,
    execution_store: Any | None = None,
    storage: Any | None = None,
) -> list[dict[str, Any]]:
    resolved_execution_store = (
        execution_store if execution_store is not None else storage.execution
    )
    if not resolved_execution_store.schema_ready():
        return []
    attempts = list(
        resolved_execution_store.list_attempts(session_id=session_id, limit=limit)
    )
    return _attach_attempt_details(
        execution_store=resolved_execution_store, attempts=attempts
    )


def _get_attempt_payload(
    execution_store: Any, execution_attempt_id: str
) -> dict[str, Any]:
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    return _attach_attempt_details(
        execution_store=execution_store, attempts=[dict(attempt)]
    )[0]


@with_storage()
def run_open_execution_guard(
    *,
    db_target: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.schema_ready():
        return {
            "status": "skipped",
            "reason": "execution_schema_unavailable",
        }

    open_attempts = [
        dict(attempt)
        for attempt in execution_store.list_attempts_by_status(
            statuses=sorted(OPEN_STATUSES),
            trade_intent=OPEN_TRADE_INTENT,
            limit=200,
        )
    ]
    if not open_attempts:
        return {
            "status": "ok",
            "open_attempt_count": 0,
            "evaluated": 0,
            "canceled": 0,
            "failed_unsubmitted": 0,
            "submit_unknown": 0,
            "terminal_synced": 0,
            "skipped": 0,
            "failure_count": 0,
            "decisions": [],
            "failures": [],
        }

    now = datetime.now(UTC)
    client: AlpacaClient | None = None
    evaluated = 0
    canceled = 0
    failed_unsubmitted = 0
    submit_unknown = 0
    terminal_synced = 0
    skipped = 0
    failures: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    for attempt in open_attempts:
        execution_attempt_id = str(attempt["execution_attempt_id"])
        guard_decision = _evaluate_open_attempt_guard(
            storage=storage,
            attempt=attempt,
            current_time=now,
        )
        if guard_decision["allowed"]:
            skipped += 1
            continue

        evaluated += 1
        broker_order_id = _as_text(attempt.get("broker_order_id"))
        session_position_id = _as_text(attempt.get("session_position_id"))
        intervention = _as_text(guard_decision.get("intervention"))
        if intervention == "mark_submit_unknown":
            message = _guard_intervention_message(
                guard_decision,
                submitted=False,
            )
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status=SUBMIT_UNKNOWN_STATUS,
                error_text=message,
                session_position_id=session_position_id,
            )
            uncertain_attempt = _get_attempt_payload(
                execution_store, execution_attempt_id
            )
            _publish_execution_attempt_event(
                uncertain_attempt,
                message=message,
            )
            submit_unknown += 1
            decisions.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(attempt.get("underlying_symbol") or ""),
                    "action": "marked_submit_unknown",
                    "status": str(uncertain_attempt.get("status") or ""),
                    "reason": str(guard_decision["reason"] or ""),
                    "age_seconds": guard_decision.get("age_seconds"),
                    "stale_after_seconds": guard_decision.get(
                        "stale_after_seconds"
                    ),
                    "submit_job_status": guard_decision.get("submit_job_status"),
                }
            )
            continue
        if broker_order_id is None:
            message = _guard_intervention_message(
                guard_decision,
                submitted=False,
            )
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=message,
                session_position_id=session_position_id,
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=message,
            )
            failed_unsubmitted += 1
            decisions.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(attempt.get("underlying_symbol") or ""),
                    "action": "failed_unsubmitted",
                    "status": str(failed_attempt.get("status") or ""),
                    "reason": str(guard_decision["reason"] or ""),
                    "age_seconds": guard_decision.get("age_seconds"),
                    "stale_after_seconds": guard_decision.get(
                        "stale_after_seconds"
                    ),
                }
            )
            continue

        try:
            if client is None:
                client = create_alpaca_client_from_env()
            order_snapshot = client.get_order(broker_order_id, nested=True)
            synced_attempt = _sync_attempt_state(
                execution_store=execution_store,
                attempt=attempt,
                client=client,
                order_snapshot=order_snapshot,
            )
            current_status = str(synced_attempt.get("status") or "").lower()
            if _is_terminal_status(current_status):
                terminal_synced += 1
                decisions.append(
                    {
                        "execution_attempt_id": execution_attempt_id,
                        "symbol": str(synced_attempt.get("underlying_symbol") or ""),
                        "action": "synced_terminal",
                        "status": current_status,
                        "reason": str(guard_decision["reason"] or ""),
                        "age_seconds": guard_decision.get("age_seconds"),
                        "stale_after_seconds": guard_decision.get(
                            "stale_after_seconds"
                        ),
                    }
                )
                continue

            if current_status != "pending_cancel":
                client.cancel_order(broker_order_id)
                try:
                    cancel_snapshot = client.get_order(broker_order_id, nested=True)
                    synced_attempt = _sync_attempt_state(
                        execution_store=execution_store,
                        attempt=synced_attempt,
                        client=client,
                        order_snapshot=cancel_snapshot,
                    )
                except Exception:
                    execution_store.update_attempt(
                        execution_attempt_id=execution_attempt_id,
                        status="pending_cancel",
                        session_position_id=session_position_id,
                    )
                    synced_attempt = _get_attempt_payload(
                        execution_store, execution_attempt_id
                    )
                canceled += 1
                _publish_execution_attempt_event(
                    synced_attempt,
                    message=_guard_intervention_message(
                        guard_decision,
                        submitted=True,
                    ),
                )
                decisions.append(
                    {
                        "execution_attempt_id": execution_attempt_id,
                        "symbol": str(synced_attempt.get("underlying_symbol") or ""),
                        "action": "cancel_requested",
                        "status": str(synced_attempt.get("status") or ""),
                        "reason": str(guard_decision["reason"] or ""),
                        "age_seconds": guard_decision.get("age_seconds"),
                        "stale_after_seconds": guard_decision.get(
                            "stale_after_seconds"
                        ),
                    }
                )
                continue

            decisions.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(synced_attempt.get("underlying_symbol") or ""),
                    "action": "already_pending_cancel",
                    "status": current_status,
                    "reason": str(guard_decision["reason"] or ""),
                    "age_seconds": guard_decision.get("age_seconds"),
                    "stale_after_seconds": guard_decision.get(
                        "stale_after_seconds"
                    ),
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(attempt.get("underlying_symbol") or ""),
                    "error": str(exc),
                }
            )

    return {
        "status": "degraded" if failures else "ok",
        "open_attempt_count": len(open_attempts),
        "evaluated": evaluated,
        "canceled": canceled,
        "failed_unsubmitted": failed_unsubmitted,
        "submit_unknown": submit_unknown,
        "terminal_synced": terminal_synced,
        "skipped": skipped,
        "failure_count": len(failures),
        "decisions": decisions[:25],
        "failures": failures[:25],
    }


def _resolve_session_candidate(
    *,
    collector_store: Any,
    session_id: str,
    candidate_id: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate = collector_store.get_candidate(candidate_id)
    if candidate is None:
        raise ValueError(f"Unknown candidate_id: {candidate_id}")
    cycle = collector_store.get_cycle(str(candidate["cycle_id"]))
    if cycle is None:
        raise ValueError(f"Missing cycle for candidate_id: {candidate_id}")
    candidate_session_id = cycle.get("session_id") or build_live_session_id(
        cycle["label"], cycle["session_date"]
    )
    if str(candidate_session_id) != session_id:
        raise ValueError(
            f"Candidate {candidate_id} does not belong to session {session_id}"
        )
    return dict(candidate), dict(cycle)


def _build_order_request(
    *,
    candidate: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    execution_policy: dict[str, Any],
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    candidate_payload = dict(candidate.get("candidate") or {})
    order_payload = dict(candidate_payload.get("order_payload") or {})
    if not order_payload:
        raise ValueError(
            "Selected live candidate does not include an executable order payload"
        )
    resolved_quantity = (
        quantity if quantity is not None else _coerce_int(order_payload.get("qty")) or 1
    )
    if resolved_quantity <= 0:
        raise ValueError("Execution quantity must be positive")
    resolved_limit_price = _resolve_open_limit_price(
        candidate_payload=candidate_payload,
        explicit_limit_price=limit_price,
        execution_policy=execution_policy,
    )

    request = dict(order_payload)
    request["qty"] = str(int(resolved_quantity))
    # Alpaca expects negative net prices for credit mleg orders.
    request["limit_price"] = f"{-abs(float(resolved_limit_price)):.2f}"
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


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
        "execution_policy": normalize_execution_policy(payload.get("execution_policy")),
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


def _resolve_session_position(
    *,
    execution_store: Any,
    session_id: str,
    session_position_id: str,
) -> dict[str, Any]:
    position = execution_store.get_session_position(session_position_id)
    if position is None:
        raise ValueError(f"Unknown session_position_id: {session_position_id}")
    if str(position["session_id"]) != session_id:
        raise ValueError(
            f"Session position {session_position_id} does not belong to session {session_id}"
        )
    return dict(position)


def _build_close_order_request(
    *,
    position: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    remaining_quantity = _coerce_float(position.get("remaining_quantity"))
    if remaining_quantity is None or remaining_quantity <= 0:
        raise ValueError("Session position does not have remaining quantity to close")
    resolved_quantity = (
        quantity if quantity is not None else int(round(remaining_quantity))
    )
    if resolved_quantity <= 0:
        raise ValueError("Close quantity must be positive")
    if resolved_quantity > remaining_quantity:
        raise ValueError(
            "Close quantity exceeds the remaining session position quantity"
        )

    resolved_limit_price = (
        limit_price
        if limit_price is not None
        else _coerce_float(position.get("close_mark"))
    )
    resolved_limit_price = _normalize_credit_limit(resolved_limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError(
            "Close execution requires a positive limit price or a quoted close mark"
        )

    request = make_close_order_payload(
        short_symbol=str(position["short_symbol"]),
        long_symbol=str(position["long_symbol"]),
        limit_price=float(resolved_limit_price),
    )
    request["qty"] = str(int(resolved_quantity))
    request["limit_price"] = f"{float(resolved_limit_price):.2f}"
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


def _flatten_order_snapshot(
    order: dict[str, Any],
    *,
    parent_broker_order_id: str | None = None,
) -> list[dict[str, Any]]:
    broker_order_id = _as_text(order.get("id"))
    if broker_order_id is None:
        raise ValueError("Broker order payload is missing an id")
    updated_at = (
        _as_text(order.get("updated_at"))
        or _as_text(order.get("filled_at"))
        or _as_text(order.get("submitted_at"))
        or _utc_now()
    )
    symbol = _as_text(order.get("symbol"))
    side = _as_text(order.get("side"))
    rows = [
        {
            "broker": BROKER_NAME,
            "broker_order_id": broker_order_id,
            "parent_broker_order_id": parent_broker_order_id,
            "client_order_id": _as_text(order.get("client_order_id")),
            "order_status": str(order.get("status") or "unknown"),
            "order_type": _as_text(order.get("type")),
            "time_in_force": _as_text(order.get("time_in_force")),
            "order_class": _as_text(order.get("order_class")),
            "side": side,
            "symbol": symbol,
            "leg_symbol": symbol if parent_broker_order_id is not None else None,
            "leg_side": side if parent_broker_order_id is not None else None,
            "position_intent": _as_text(order.get("position_intent")),
            "quantity": _coerce_float(order.get("qty")),
            "limit_price": _coerce_float(order.get("limit_price")),
            "filled_qty": _coerce_float(order.get("filled_qty")),
            "filled_avg_price": _coerce_float(order.get("filled_avg_price")),
            "submitted_at": _as_text(order.get("submitted_at")),
            "updated_at": updated_at,
            "order": order,
        }
    ]
    for leg in order.get("legs") or []:
        if isinstance(leg, dict):
            rows.extend(
                _flatten_order_snapshot(leg, parent_broker_order_id=broker_order_id)
            )
    return rows


def _sync_fill_rows(
    *,
    client: AlpacaClient,
    session_date: str,
    persisted_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    broker_order_ids = {str(order["broker_order_id"]) for order in persisted_orders}
    if not broker_order_ids:
        return []
    order_lookup = {str(order["broker_order_id"]): order for order in persisted_orders}
    activities = client.list_account_activities(activity_type="FILL", date=session_date)
    rows: list[dict[str, Any]] = []
    for activity in activities:
        broker_fill_id = _as_text(activity.get("id"))
        broker_order_id = _as_text(activity.get("order_id"))
        symbol = _as_text(activity.get("symbol"))
        filled_at = _as_text(activity.get("transaction_time"))
        quantity = _coerce_float(activity.get("qty"))
        if (
            broker_fill_id is None
            or broker_order_id is None
            or broker_order_id not in broker_order_ids
            or symbol is None
            or filled_at is None
            or quantity is None
        ):
            continue
        matching_order = order_lookup.get(broker_order_id)
        rows.append(
            {
                "execution_order_id": None
                if matching_order is None
                else matching_order.get("execution_order_id"),
                "broker": BROKER_NAME,
                "broker_fill_id": broker_fill_id,
                "broker_order_id": broker_order_id,
                "symbol": symbol,
                "side": _as_text(activity.get("side")),
                "fill_type": _as_text(activity.get("type")),
                "quantity": quantity,
                "cumulative_quantity": _coerce_float(activity.get("cum_qty")),
                "remaining_quantity": _coerce_float(activity.get("leaves_qty")),
                "price": _coerce_float(activity.get("price")),
                "filled_at": filled_at,
                "fill": activity,
            }
        )
    return rows


def _sync_attempt_state(
    *,
    execution_store: Any,
    attempt: dict[str, Any],
    client: AlpacaClient,
    order_snapshot: dict[str, Any],
) -> dict[str, Any]:
    order_rows = _flatten_order_snapshot(order_snapshot)
    persisted_orders = [
        dict(row)
        for row in execution_store.upsert_orders(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=order_rows,
        )
    ]
    try:
        fill_rows = _sync_fill_rows(
            client=client,
            session_date=str(attempt["session_date"]),
            persisted_orders=persisted_orders,
        )
    except Exception:
        fill_rows = []
    if fill_rows:
        execution_store.upsert_fills(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=fill_rows,
        )

    status = str(
        order_snapshot.get("status") or attempt.get("status") or "unknown"
    ).lower()
    completed_at = (
        _resolve_completed_at(order_snapshot) if _is_terminal_status(status) else None
    )
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        status=status,
        broker_order_id=_as_text(order_snapshot.get("id")),
        client_order_id=_as_text(order_snapshot.get("client_order_id")),
        submitted_at=_as_text(order_snapshot.get("submitted_at"))
        or str(attempt["requested_at"]),
        completed_at=completed_at,
        error_text=None,
    )
    payload = _get_attempt_payload(
        execution_store, str(attempt["execution_attempt_id"])
    )
    sync_session_position_from_attempt(
        execution_store=execution_store,
        attempt=payload,
    )
    return _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))


def _publish_execution_attempt_event(attempt: dict[str, Any], *, message: str) -> None:
    try:
        publish_global_event_sync(
            topic="execution.attempt.updated",
            event_class="broker_event",
            entity_type="execution_attempt",
            entity_id=str(attempt["execution_attempt_id"]),
            payload={
                **attempt,
                "message": message,
            },
            timestamp=attempt.get("completed_at")
            or attempt.get("submitted_at")
            or attempt.get("requested_at")
            or _utc_now(),
            source="execution",
            session_date=_as_text(attempt.get("session_date")),
            correlation_id=_as_text(attempt.get("session_id")),
            causation_id=_as_text(attempt.get("broker_order_id")),
        )
    except Exception:
        pass


def _publish_risk_decision_event(risk_decision: dict[str, Any]) -> None:
    try:
        publish_global_event_sync(
            topic="risk.decision.recorded",
            event_class="risk_event",
            entity_type="risk_decision",
            entity_id=str(risk_decision["risk_decision_id"]),
            payload=risk_decision,
            timestamp=risk_decision.get("decided_at") or _utc_now(),
            source="execution",
            session_date=_as_text(risk_decision.get("session_date")),
            correlation_id=_as_text(risk_decision.get("opportunity_id"))
            or _as_text(risk_decision.get("session_id")),
            causation_id=_as_text(risk_decision.get("candidate_id")),
        )
    except Exception:
        pass


def _submission_message(attempt: dict[str, Any], *, queued: bool) -> str:
    if str(attempt.get("trade_intent") or OPEN_TRADE_INTENT) == CLOSE_TRADE_INTENT:
        prefix = "Queued close for" if queued else "Submitted close for"
        return (
            f"{prefix} {attempt['underlying_symbol']} "
            f"{attempt['short_symbol']} / {attempt['long_symbol']}."
        )
    prefix = "Queued" if queued else "Submitted"
    return (
        f"{prefix} {attempt['underlying_symbol']} {attempt['strategy']} "
        f"{attempt['short_symbol']} / {attempt['long_symbol']}."
    )


def _queue_execution_attempt(
    *,
    job_store: Any,
    execution_store: Any,
    attempt: dict[str, Any],
) -> dict[str, Any]:
    _ensure_execution_submit_job_definition(job_store)
    execution_attempt_id = str(attempt["execution_attempt_id"])
    job_run_id = _execution_submit_job_run_id(execution_attempt_id)
    scheduled_for = datetime.now(UTC)
    payload = {
        "execution_attempt_id": execution_attempt_id,
        "session_id": str(attempt["session_id"]),
        "trade_intent": str(attempt["trade_intent"]),
        "job_key": EXECUTION_SUBMIT_ADHOC_JOB_KEY,
        "job_type": EXECUTION_SUBMIT_JOB_TYPE,
        "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
    }
    job_run, _ = job_store.create_job_run(
        job_run_id=job_run_id,
        job_key=EXECUTION_SUBMIT_ADHOC_JOB_KEY,
        arq_job_id=job_run_id,
        job_type=EXECUTION_SUBMIT_JOB_TYPE,
        status="queued",
        scheduled_for=scheduled_for,
        session_id=str(attempt["session_id"]),
        payload=payload,
    )
    try:
        enqueued = _enqueue_ad_hoc_job(
            job_type=EXECUTION_SUBMIT_JOB_TYPE,
            job_key=EXECUTION_SUBMIT_ADHOC_JOB_KEY,
            job_run_id=job_run_id,
            arq_job_id=job_run_id,
            payload=payload,
        )
    except Exception as exc:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=datetime.now(UTC),
            error_text=str(exc),
        )
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=_utc_now(),
            error_text=str(exc),
            session_position_id=_as_text(attempt.get("session_position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=f"Execution queueing failed before submission: {exc}",
        )
        raise RuntimeError(f"Execution queueing failed: {exc}") from exc
    if enqueued is None:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=datetime.now(UTC),
            error_text="Execution submit job was not enqueued.",
        )
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=_utc_now(),
            error_text="Execution submit job was not enqueued.",
            session_position_id=_as_text(attempt.get("session_position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message="Execution queueing failed before submission: job was not enqueued.",
        )
        raise RuntimeError("Execution queueing failed: job was not enqueued.")
    queued_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
    _publish_execution_attempt_event(
        queued_attempt,
        message=_submission_message(queued_attempt, queued=True),
    )
    return queued_attempt


@with_storage()
def submit_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    candidate_id: int,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    execution_store = storage.execution
    job_store = storage.jobs
    signal_store = getattr(storage, "signals", None)
    risk_store = getattr(storage, "risk", None)
    requested_at = _utc_now()
    client_order_id = _execution_client_order_id()
    attempt_id: str | None = None
    risk_decision: dict[str, Any] | None = None
    try:
        _require_execution_schema(execution_store)
        _require_position_schema(execution_store)
        candidate, cycle = _resolve_session_candidate(
            collector_store=collector_store,
            session_id=session_id,
            candidate_id=candidate_id,
        )
        source_policies = _resolve_source_policies(
            cycle=cycle,
            job_store=job_store,
        )
        active_policy_rollouts = get_active_policy_rollout_map(storage=storage)
        opportunity = None
        if (
            signal_store is not None
            and hasattr(signal_store, "schema_ready")
            and signal_store.schema_ready()
        ):
            opportunity = signal_store.find_active_opportunity_by_candidate_id(
                candidate_id
            )
        opportunity_ref = (
            None
            if opportunity is None
            else {
                "opportunity_id": str(opportunity["opportunity_id"]),
                "signal_state_ref": opportunity.get("signal_state_ref"),
                "lifecycle_state": opportunity.get("lifecycle_state"),
                "selection_state": opportunity.get("selection_state"),
            }
        )

        existing_attempts = execution_store.list_open_attempts_for_identity(
            session_id=session_id,
            strategy=str(candidate["strategy"]),
            short_symbol=str(candidate["short_symbol"]),
            long_symbol=str(candidate["long_symbol"]),
            statuses=sorted(OPEN_STATUSES),
        )
        if existing_attempts:
            payload = _get_attempt_payload(
                execution_store,
                str(existing_attempts[0]["execution_attempt_id"]),
            )
            return {
                "action": "submit",
                "changed": False,
                "message": (
                    f"An active execution already exists for "
                    f"{payload['short_symbol']} / {payload['long_symbol']} in this session."
                ),
                "attempt": payload,
            }

        gate = assess_open_activity_gate(
            activity_kind=OPEN_ACTIVITY_MANUAL,
            storage=storage,
        )
        if not gate["allowed"]:
            publish_control_gate_event(
                db_target=db_target,
                decision=gate,
                activity_kind=OPEN_ACTIVITY_MANUAL,
                session_id=session_id,
                session_date=str(cycle["session_date"]),
                label=str(cycle["label"]),
                candidate_id=_coerce_int(candidate.get("candidate_id")),
                cycle_id=_as_text(cycle.get("cycle_id")),
            )
            raise ValueError(str(gate["message"]))

        requested_execution_policy = _requested_policy_payload(
            request_metadata=request_metadata,
            policy_name="execution_policy",
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
        )
        resolved_execution_policy = normalize_execution_policy(
            requested_execution_policy
        )
        order_request, resolved_quantity, resolved_limit_price = _build_order_request(
            candidate=candidate,
            quantity=quantity,
            limit_price=limit_price,
            execution_policy=resolved_execution_policy,
            client_order_id=client_order_id,
        )
        requested_risk_policy = _requested_policy_payload(
            request_metadata=request_metadata,
            policy_name="risk_policy",
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
        )
        requested_exit_policy = _requested_policy_payload(
            request_metadata=request_metadata,
            policy_name="exit_policy",
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
        )
        resolved_exit_policy = resolve_exit_policy_snapshot(
            session_date=str(cycle["session_date"]),
            payload=requested_exit_policy,
        )
        timing_gate = _validate_open_timing_window(
            exit_policy=resolved_exit_policy,
            current_time=parse_datetime(requested_at) or datetime.now(UTC),
        )
        if not timing_gate["allowed"]:
            raise ValueError(str(timing_gate["message"]))
        risk_evaluation = evaluate_open_execution(
            execution_store=execution_store,
            session_id=session_id,
            candidate=candidate,
            cycle=cycle,
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            risk_policy=requested_risk_policy,
        )
        resolved_risk_policy = dict(risk_evaluation["policy"])
        policy_refs = _build_policy_refs(
            request_metadata=request_metadata,
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
            resolved_risk_policy=resolved_risk_policy,
            resolved_execution_policy=resolved_execution_policy,
            resolved_exit_policy=resolved_exit_policy,
        )
        if (
            risk_store is not None
            and hasattr(risk_store, "schema_ready")
            and risk_store.schema_ready()
        ):
            risk_decision = risk_store.create_risk_decision(
                risk_decision_id=_risk_decision_id(),
                decision_kind="open_execution",
                status=str(risk_evaluation["status"]),
                note=str(risk_evaluation["note"]),
                session_id=session_id,
                session_date=str(cycle["session_date"]),
                label=str(cycle["label"]),
                cycle_id=_as_text(cycle.get("cycle_id")),
                candidate_id=_coerce_int(candidate.get("candidate_id")),
                opportunity_id=None
                if opportunity_ref is None
                else str(opportunity_ref["opportunity_id"]),
                execution_attempt_id=None,
                trade_intent=OPEN_TRADE_INTENT,
                entity_type="signal_subject",
                entity_key=(
                    str(opportunity.get("entity_key"))
                    if isinstance(opportunity, dict) and opportunity.get("entity_key")
                    else f"signal_subject:{cycle['label']}:{candidate['underlying_symbol']}"
                ),
                underlying_symbol=str(candidate["underlying_symbol"]),
                strategy=str(candidate["strategy"]),
                quantity=resolved_quantity,
                limit_price=resolved_limit_price,
                reason_codes=[
                    str(value) for value in risk_evaluation.get("reason_codes") or []
                ],
                blockers=[
                    str(value) for value in risk_evaluation.get("blockers") or []
                ],
                metrics=dict(risk_evaluation.get("metrics") or {}),
                evidence={
                    "candidate_generated_at": _as_text(candidate.get("generated_at")),
                    "opportunity": opportunity_ref,
                    "source_job": {
                        "job_type": source_policies["source_job_type"],
                        "job_key": source_policies["source_job_key"],
                        "job_run_id": source_policies["source_job_run_id"],
                    },
                    "requested_limit_price": resolved_limit_price,
                    "requested_quantity": resolved_quantity,
                },
                policy_refs=policy_refs,
                resolved_risk_policy=resolved_risk_policy,
                decided_at=requested_at,
            )
        if str(risk_evaluation["status"]) in {"blocked", "unknown"}:
            if risk_decision is not None:
                _publish_risk_decision_event(risk_decision)
            raise ValueError(str(risk_evaluation["note"]))

        pipeline_policy_fields = resolve_pipeline_policy_fields(
            profile=(candidate.get("candidate") or {}).get("profile"),
            root_symbol=str(candidate["underlying_symbol"]),
        )
        attempt_id = _execution_attempt_id()
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=session_id,
            session_date=str(cycle["session_date"]),
            label=str(cycle["label"]),
            pipeline_id=build_pipeline_id(str(cycle["label"])),
            market_date=str(cycle["session_date"]),
            cycle_id=_as_text(cycle.get("cycle_id")),
            opportunity_id=None
            if opportunity_ref is None
            else str(opportunity_ref["opportunity_id"]),
            risk_decision_id=None
            if risk_decision is None
            else str(risk_decision["risk_decision_id"]),
            candidate_id=_coerce_int(candidate.get("candidate_id")),
            attempt_context=_normalize_attempt_context(
                candidate.get("selection_state")
            ),
            candidate_generated_at=_as_text(candidate.get("generated_at")),
            run_id=_as_text(candidate.get("run_id")),
            job_run_id=_as_text(cycle.get("job_run_id")),
            underlying_symbol=str(candidate["underlying_symbol"]),
            strategy=str(candidate["strategy"]),
            expiration_date=str(candidate["expiration_date"]),
            short_symbol=str(candidate["short_symbol"]),
            long_symbol=str(candidate["long_symbol"]),
            trade_intent=OPEN_TRADE_INTENT,
            session_position_id=None,
            position_id=None,
            root_symbol=str(candidate["underlying_symbol"]),
            strategy_family=str(candidate["strategy"]),
            style_profile=str(pipeline_policy_fields["style_profile"]),
            horizon_intent=str(pipeline_policy_fields["horizon_intent"]),
            product_class=str(pipeline_policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **({} if request_metadata is None else request_metadata),
                **({} if opportunity_ref is None else {"opportunity": opportunity_ref}),
                **(
                    {}
                    if risk_decision is None
                    else {
                        "risk_decision": {
                            "risk_decision_id": str(risk_decision["risk_decision_id"]),
                            "status": str(risk_decision["status"]),
                            "policy_refs": dict(risk_decision.get("policy_refs") or {}),
                        }
                    }
                ),
                "trade_intent": OPEN_TRADE_INTENT,
                "execution_policy": resolved_execution_policy,
                "risk_policy": resolved_risk_policy,
                "exit_policy": resolved_exit_policy,
                "source_job": {
                    "job_type": source_policies["source_job_type"],
                    "job_key": source_policies["source_job_key"],
                    "job_run_id": source_policies["source_job_run_id"],
                },
                "order": order_request,
            },
            candidate=dict(candidate.get("candidate") or {}),
        )
        payload = _queue_execution_attempt(
            job_store=job_store,
            execution_store=execution_store,
            attempt=attempt,
        )
        if risk_decision is not None and risk_store is not None:
            try:
                risk_decision = risk_store.attach_execution_attempt(
                    risk_decision_id=str(risk_decision["risk_decision_id"]),
                    execution_attempt_id=attempt_id,
                )
                _publish_risk_decision_event(risk_decision)
            except Exception:
                pass
        if opportunity_ref is not None and signal_store is not None:
            try:
                consumed_opportunity, consumed_changed = (
                    signal_store.mark_opportunity_consumed(
                        opportunity_id=str(opportunity_ref["opportunity_id"]),
                        execution_attempt_id=attempt_id,
                        consumed_at=requested_at,
                    )
                )
                if consumed_opportunity is not None and consumed_changed:
                    publish_opportunity_event(
                        topic="opportunity.lifecycle.updated",
                        opportunity=consumed_opportunity,
                        session_date=str(cycle["session_date"]),
                        correlation_id=str(cycle["cycle_id"]),
                        causation_id=attempt_id,
                        timestamp=requested_at,
                        source="execution",
                    )
            except Exception:
                pass
        message = _submission_message(payload, queued=True)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            **({} if risk_decision is None else {"risk_decision": risk_decision}),
            "attempt": payload,
        }
    except Exception as exc:
        if attempt_id is not None:
            current_attempt = execution_store.get_attempt(attempt_id)
            if (
                current_attempt is not None
                and str(current_attempt.get("status") or "")
                == PENDING_SUBMISSION_STATUS
            ):
                execution_store.update_attempt(
                    execution_attempt_id=attempt_id,
                    status="failed",
                    client_order_id=client_order_id,
                    completed_at=requested_at,
                    error_text=str(exc),
                )
                payload = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    payload,
                    message=f"Execution failed before submission: {exc}",
                )
        raise


@with_storage()
def submit_session_position_close(
    *,
    db_target: str,
    session_id: str,
    session_position_id: str,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    job_store = storage.jobs
    requested_at = _utc_now()
    client_order_id = _execution_client_order_id()
    attempt_id: str | None = None
    try:
        _require_execution_schema(execution_store)
        _require_position_schema(execution_store)
        position = _resolve_session_position(
            execution_store=execution_store,
            session_id=session_id,
            session_position_id=session_position_id,
        )
        if str(position.get("status") or "open") == "closed":
            raise ValueError("Session position is already closed")

        existing_attempts = execution_store.list_open_attempts_for_session_position(
            session_position_id=session_position_id,
            statuses=sorted(OPEN_STATUSES),
        )
        if existing_attempts:
            payload = _get_attempt_payload(
                execution_store,
                str(existing_attempts[0]["execution_attempt_id"]),
            )
            return {
                "action": "submit",
                "changed": False,
                "message": "An active close execution already exists for this session position.",
                "attempt": payload,
            }

        order_request, resolved_quantity, resolved_limit_price = (
            _build_close_order_request(
                position=position,
                quantity=quantity,
                limit_price=limit_price,
                client_order_id=client_order_id,
            )
        )
        validate_close_execution(
            position=position,
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
        )
        trade_intent = resolve_trade_intent(CLOSE_TRADE_INTENT)
        pipeline_policy_fields = resolve_pipeline_policy_fields(
            profile=(position.get("risk_policy_json") or {}).get("profile"),
            root_symbol=str(position["underlying_symbol"]),
        )
        portfolio_position = (
            None
            if not execution_store.portfolio_schema_ready()
            else execution_store.get_position(session_position_id)
        )

        attempt_id = _execution_attempt_id()
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=session_id,
            session_date=str(position["session_date"]),
            label=str(position["label"]),
            pipeline_id=build_pipeline_id(str(position["label"])),
            market_date=str(position["session_date"]),
            cycle_id=None,
            opportunity_id=None,
            risk_decision_id=None,
            candidate_id=_coerce_int(position.get("candidate_id")),
            attempt_context="position_close",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=str(position["underlying_symbol"]),
            strategy=str(position["strategy"]),
            expiration_date=str(position["expiration_date"]),
            short_symbol=str(position["short_symbol"]),
            long_symbol=str(position["long_symbol"]),
            trade_intent=trade_intent,
            session_position_id=session_position_id,
            position_id=None if portfolio_position is None else str(portfolio_position["position_id"]),
            root_symbol=str(position["underlying_symbol"]),
            strategy_family=str(position["strategy"]),
            style_profile=str(pipeline_policy_fields["style_profile"]),
            horizon_intent=str(pipeline_policy_fields["horizon_intent"]),
            product_class=str(pipeline_policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **({} if request_metadata is None else request_metadata),
                "trade_intent": trade_intent,
                "session_position_id": session_position_id,
                "order": order_request,
            },
            candidate={},
        )
        payload = _queue_execution_attempt(
            job_store=job_store,
            execution_store=execution_store,
            attempt=attempt,
        )
        message = _submission_message(payload, queued=True)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": payload,
        }
    except Exception as exc:
        if attempt_id is not None:
            current_attempt = execution_store.get_attempt(attempt_id)
            if (
                current_attempt is not None
                and str(current_attempt.get("status") or "")
                == PENDING_SUBMISSION_STATUS
            ):
                execution_store.update_attempt(
                    execution_attempt_id=attempt_id,
                    status="failed",
                    client_order_id=client_order_id,
                    completed_at=requested_at,
                    error_text=str(exc),
                    session_position_id=session_position_id,
                )
                payload = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    payload,
                    message=f"Close execution failed before submission: {exc}",
                )
        raise


@with_storage()
def refresh_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    if str(attempt["session_id"]) != session_id:
        raise ValueError(
            f"Execution {execution_attempt_id} does not belong to session {session_id}"
        )
    if (
        _as_text(attempt.get("broker_order_id")) is None
        and str(attempt.get("status") or "") == PENDING_SUBMISSION_STATUS
    ):
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        return {
            "action": "refresh",
            "changed": False,
            "message": "Execution is still queued for broker submission.",
            "attempt": payload,
        }
    if (
        _as_text(attempt.get("broker_order_id")) is None
        and str(attempt.get("status") or "") == SUBMIT_UNKNOWN_STATUS
    ):
        client_order_id = _as_text(attempt.get("client_order_id"))
        if client_order_id is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            return {
                "action": "refresh",
                "changed": False,
                "message": (
                    "Execution submit outcome is uncertain and cannot be reconciled "
                    "because the client order id is missing."
                ),
                "attempt": payload,
            }
        client = create_alpaca_client_from_env()
        reconciled_attempt = _reconcile_submit_unknown_attempt(
            execution_store=execution_store,
            attempt=attempt,
            client=client,
        )
        if reconciled_attempt is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            return {
                "action": "refresh",
                "changed": False,
                "message": (
                    "Execution submit outcome is uncertain and no broker order has been "
                    f"found yet for client_order_id {client_order_id}."
                ),
                "attempt": payload,
            }
        message = (
            f"Reconciled execution {execution_attempt_id} via client_order_id "
            f"{client_order_id}: {reconciled_attempt['status']}."
        )
        _publish_execution_attempt_event(reconciled_attempt, message=message)
        return {
            "action": "refresh",
            "changed": True,
            "message": message,
            "attempt": reconciled_attempt,
        }
    broker_order_id = _as_text(attempt.get("broker_order_id"))
    if broker_order_id is None:
        raise ValueError("Execution does not have a broker order id to refresh")

    client = create_alpaca_client_from_env()
    order_snapshot = client.get_order(broker_order_id, nested=True)
    payload = _sync_attempt_state(
        execution_store=execution_store,
        attempt=dict(attempt),
        client=client,
        order_snapshot=order_snapshot,
    )
    message = f"Refreshed execution {execution_attempt_id}: {payload['status']}."
    _publish_execution_attempt_event(payload, message=message)
    return {
        "action": "refresh",
        "changed": True,
        "message": message,
        "attempt": payload,
    }


@with_storage()
def submit_opportunity_execution(
    *,
    db_target: str,
    opportunity_id: str,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    opportunity = signal_store.get_opportunity(opportunity_id)
    if opportunity is None:
        raise ValueError(f"Unknown opportunity_id: {opportunity_id}")
    candidate_id = _coerce_int(opportunity.get("source_candidate_id"))
    if candidate_id is None:
        raise ValueError("Opportunity is missing a source candidate id")
    label = _as_text(opportunity.get("label"))
    market_date = _as_text(opportunity.get("market_date")) or _as_text(opportunity.get("session_date"))
    if label is None or market_date is None:
        raise ValueError("Opportunity is missing label or market_date")
    return submit_live_session_execution(
        db_target=db_target,
        session_id=build_live_session_id(label, market_date),
        candidate_id=candidate_id,
        quantity=quantity,
        limit_price=limit_price,
        request_metadata={
            **({} if request_metadata is None else request_metadata),
            "opportunity_id": opportunity_id,
            "pipeline_id": opportunity.get("pipeline_id"),
            "market_date": market_date,
        },
        storage=storage,
    )


@with_storage()
def submit_position_close_by_id(
    *,
    db_target: str,
    position_id: str,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.portfolio_schema_ready():
        raise ValueError(f"Unknown position_id: {position_id}")
    position = execution_store.get_position(position_id)
    if position is None:
        raise ValueError(f"Unknown position_id: {position_id}")
    legacy_session_position_id = _as_text(position.get("legacy_session_position_id"))
    if legacy_session_position_id is None:
        raise ValueError("Position is missing a legacy session position id")
    pipeline_id = _as_text(position.get("pipeline_id"))
    pipeline_label = None if pipeline_id is None else pipeline_id.partition(":")[2]
    market_date = _as_text(position.get("market_date_opened"))
    if pipeline_label is None or market_date is None:
        raise ValueError("Position is missing pipeline or market_date")
    return submit_session_position_close(
        db_target=db_target,
        session_id=build_live_session_id(pipeline_label, market_date),
        session_position_id=legacy_session_position_id,
        quantity=quantity,
        limit_price=limit_price,
        request_metadata={
            **({} if request_metadata is None else request_metadata),
            "position_id": position_id,
            "pipeline_id": pipeline_id,
            "market_date": market_date,
        },
        storage=storage,
    )


@with_storage()
def refresh_execution_attempt(
    *,
    db_target: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    session_id = _as_text(attempt.get("session_id"))
    if session_id is None:
        label = _as_text(attempt.get("label"))
        market_date = _as_text(attempt.get("market_date")) or _as_text(attempt.get("session_date"))
        if label is None or market_date is None:
            raise ValueError("Execution attempt is missing session compatibility fields")
        session_id = build_live_session_id(label, market_date)
    return refresh_live_session_execution(
        db_target=db_target,
        session_id=session_id,
        execution_attempt_id=execution_attempt_id,
        storage=storage,
    )


@with_storage()
def run_execution_submit(
    *,
    db_target: str,
    execution_attempt_id: str,
    heartbeat: Any | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")

    payload = _get_attempt_payload(execution_store, execution_attempt_id)
    broker_order_id = _as_text(payload.get("broker_order_id"))
    status = str(payload.get("status") or "")
    if broker_order_id is not None or status != PENDING_SUBMISSION_STATUS:
        return {
            "status": "skipped",
            "reason": "attempt_already_submitted",
            "execution_attempt_id": execution_attempt_id,
            "attempt_status": status,
            "broker_order_id": broker_order_id,
        }

    request = dict(payload.get("request") or {})
    order_request = request.get("order")
    if not isinstance(order_request, dict) or not order_request:
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=_utc_now(),
            error_text="Execution attempt is missing its broker order payload.",
            session_position_id=_as_text(payload.get("session_position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message="Execution failed before submission: missing broker order payload.",
        )
        raise ValueError("Execution attempt is missing its broker order payload.")

    if str(payload.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
        timing_gate = _validate_open_timing_window(
            exit_policy=request.get("exit_policy"),
            current_time=datetime.now(UTC),
        )
        if not timing_gate["allowed"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=str(timing_gate["message"]),
                session_position_id=_as_text(payload.get("session_position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {timing_gate['message']}",
            )
            return {
                "status": "blocked",
                "reason": str(timing_gate["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(timing_gate["message"]),
                "attempt": failed_attempt,
            }

    if callable(heartbeat):
        heartbeat()
    client = create_alpaca_client_from_env()
    requested_at = _as_text(payload.get("requested_at")) or _utc_now()
    client_order_id = _as_text(payload.get("client_order_id"))

    submitted_order: dict[str, Any] | None = None
    try:
        submitted_order = client.submit_order(order_request)
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=str(submitted_order.get("status") or "submitted").lower(),
            broker_order_id=_as_text(submitted_order.get("id")),
            client_order_id=_as_text(submitted_order.get("client_order_id"))
            or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
            session_position_id=_as_text(payload.get("session_position_id")),
        )
        if callable(heartbeat):
            heartbeat()
        try:
            order_snapshot = client.get_order(str(submitted_order["id"]), nested=True)
        except Exception:
            order_snapshot = submitted_order
        synced_attempt = _sync_attempt_state(
            execution_store=execution_store,
            attempt=payload,
            client=client,
            order_snapshot=order_snapshot,
        )
        message = _submission_message(synced_attempt, queued=False)
        _publish_execution_attempt_event(synced_attempt, message=message)
        return {
            "status": "submitted",
            "execution_attempt_id": execution_attempt_id,
            "message": message,
            "attempt": synced_attempt,
        }
    except Exception as exc:
        if submitted_order is None:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(exc),
                session_position_id=_as_text(payload.get("session_position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {exc}",
            )
            raise
        broker_order_id = _as_text(submitted_order.get("id"))
        submitted_status = str(submitted_order.get("status") or "submitted").lower()
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=submitted_status,
            broker_order_id=broker_order_id,
            client_order_id=_as_text(submitted_order.get("client_order_id"))
            or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
            completed_at=_resolve_completed_at(submitted_order)
            if _is_terminal_status(submitted_status)
            else None,
            error_text=str(exc),
            session_position_id=_as_text(payload.get("session_position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=(
                f"Order {broker_order_id or execution_attempt_id} was submitted, "
                f"but local execution sync failed: {exc}"
            ),
        )
        raise


@with_storage()
def submit_auto_session_execution(
    *,
    db_target: str,
    session_id: str,
    cycle_id: str,
    policy: dict[str, Any] | None,
    job_run_id: str | None = None,
    reactive_quote_records: list[dict[str, Any]] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    active_policy_rollouts = get_active_policy_rollout_map(storage=storage)
    execution_rollout = active_policy_rollouts.get("execution_policy")
    requested_policy = (
        dict(execution_rollout["policy"])
        if execution_rollout is not None
        and isinstance(execution_rollout.get("policy"), dict)
        else policy
    )
    normalized_policy = normalize_execution_policy(requested_policy)
    if not normalized_policy["enabled"]:
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "execution_disabled",
            "message": "Automatic execution is disabled for this live collector.",
            "policy": normalized_policy,
        }

    gate = assess_open_activity_gate(
        activity_kind=OPEN_ACTIVITY_AUTO,
        storage=storage,
    )
    if not gate["allowed"]:
        publish_control_gate_event(
            db_target=db_target,
            decision=gate,
            activity_kind=OPEN_ACTIVITY_AUTO,
            session_id=session_id,
            session_date=None,
            label=None,
            candidate_id=None,
            cycle_id=cycle_id,
        )
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": str(gate["reason"]),
            "message": str(gate["message"]),
            "policy": normalized_policy,
            "control": dict(gate["control"]),
        }

    collector_store = storage.collector
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    _require_position_schema(execution_store)
    promotable_candidates = collector_store.list_cycle_candidates(
        cycle_id,
        selection_state="promotable",
        eligibility="live",
    )
    if not promotable_candidates:
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "no_promotable_opportunity",
            "message": "Automatic execution skipped because the cycle does not have a promotable opportunity.",
            "policy": normalized_policy,
        }

    top_candidate = min(
        promotable_candidates, key=lambda candidate: int(candidate["selection_rank"])
    )
    selected_candidate = _candidate_with_payload(top_candidate)
    blocked_reason, blocked_message = _validate_auto_execution_candidate(
        selected_candidate
    )
    if blocked_reason is not None:
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": blocked_reason,
            "message": blocked_message,
            "policy": normalized_policy,
            "selected_candidate_id": int(top_candidate["candidate_id"]),
        }

    reactive_quote: dict[str, Any] | None = None
    limit_price: float | None = None
    if _as_text(selected_candidate.get("profile")) == "0dte":
        reactive_resolution = _resolve_reactive_auto_execution(
            candidate=selected_candidate,
            execution_policy=normalized_policy,
            quote_records=reactive_quote_records,
        )
        if not reactive_resolution.get("ok"):
            return {
                "action": "auto_submit",
                "changed": False,
                "reason": str(reactive_resolution["reason"]),
                "message": str(reactive_resolution["message"]),
                "policy": normalized_policy,
                "selected_candidate_id": int(top_candidate["candidate_id"]),
                "reactive_quote": reactive_resolution.get("reactive_quote"),
            }
        limit_price = _coerce_float(reactive_resolution.get("limit_price"))
        reactive_quote = (
            dict(reactive_resolution["reactive_quote"])
            if isinstance(reactive_resolution.get("reactive_quote"), dict)
            else None
        )

    try:
        result = submit_live_session_execution(
            db_target=db_target,
            session_id=session_id,
            candidate_id=int(top_candidate["candidate_id"]),
            quantity=int(normalized_policy["quantity"]),
            limit_price=limit_price,
            request_metadata={
                "source": {
                    "kind": "auto_session_execution",
                    "mode": normalized_policy["mode"],
                    "cycle_id": cycle_id,
                    "job_run_id": job_run_id,
                    "candidate_id": int(top_candidate["candidate_id"]),
                },
                "execution_policy": normalized_policy,
                **(
                    {} if reactive_quote is None else {"reactive_quote": reactive_quote}
                ),
            },
        )
    except Exception as exc:
        blocked = _classify_auto_execution_block(exc)
        if blocked is None:
            raise
        return {
            "action": "auto_submit",
            "changed": False,
            "policy": normalized_policy,
            "selected_candidate_id": int(top_candidate["candidate_id"]),
            **blocked,
            **({} if reactive_quote is None else {"reactive_quote": reactive_quote}),
        }
    return {
        **result,
        "action": "auto_submit",
        "reason": None,
        "policy": normalized_policy,
        "selected_candidate_id": int(top_candidate["candidate_id"]),
        **({} if reactive_quote is None else {"reactive_quote": reactive_quote}),
    }

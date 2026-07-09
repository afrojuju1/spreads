from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any


from core.storage.serializers import parse_datetime
from core.value_coercion import (
    as_list,
    as_mapping,
    as_text,
    coerce_float,
    coerce_int,
    utc_iso,
)

from core.services.ops.shared import (
    _attention,
)


from core.services.ops.trading.models import (
    _MarketContextProjection,
)

def _parse_utc_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = as_text(value)
    if text is None:
        return None
    try:
        parsed = parse_datetime(text)
    except ValueError:
        return None
    if parsed is None:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _seconds_until(value: Any, *, now: datetime) -> float | None:
    parsed = _parse_utc_timestamp(value)
    if parsed is None:
        return None
    return (parsed - now).total_seconds()


def _market_context_reference_from_summary(summary: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = as_mapping(summary)
    snapshot_id = as_text(payload.get("market_context_snapshot_id") or payload.get("snapshot_id"))
    regime_label = as_text(payload.get("market_context_regime_label") or payload.get("regime_label"))
    risk_posture = as_text(payload.get("market_context_risk_posture") or payload.get("risk_posture"))
    confidence = coerce_float(payload.get("market_context_confidence") or payload.get("confidence"))
    observed_at = payload.get("market_context_observed_at") or payload.get("observed_at")
    expires_at = payload.get("market_context_expires_at") or payload.get("expires_at")
    freshness = as_text(payload.get("market_context_freshness") or payload.get("freshness"))
    data_quality = as_text(payload.get("market_context_data_quality") or payload.get("data_quality"))
    if not any((snapshot_id, regime_label, risk_posture, confidence is not None, observed_at, expires_at, freshness, data_quality)):
        return {}
    return {
        "market_context_snapshot_id": snapshot_id,
        "observed_at": observed_at,
        "expires_at": expires_at,
        "scope": as_text(payload.get("market_context_scope") or payload.get("scope")),
        "regime_label": regime_label,
        "risk_posture": risk_posture,
        "confidence": confidence,
        "freshness": freshness,
        "data_quality": data_quality,
    }


def _market_context_reference_from_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    market_context = as_mapping(payload)
    if not market_context:
        return {}
    regime = as_mapping(market_context.get("regime"))
    data_quality = as_mapping(market_context.get("data_quality"))
    return _market_context_reference_from_summary(
        {
            "market_context_snapshot_id": market_context.get("snapshot_id") or market_context.get("market_context_snapshot_id"),
            "market_context_observed_at": market_context.get("observed_at"),
            "market_context_expires_at": market_context.get("expires_at"),
            "market_context_scope": market_context.get("scope"),
            "market_context_regime_label": regime.get("regime_label"),
            "market_context_risk_posture": regime.get("risk_posture"),
            "market_context_confidence": regime.get("confidence"),
            "market_context_freshness": data_quality.get("freshness"),
            "market_context_data_quality": data_quality.get("state"),
        }
    )


def _market_context_regime_fit(diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    latest_metrics: dict[str, Any] = {}
    latest_thresholds: dict[str, Any] = {}
    for diagnostic in diagnostics:
        evidence = as_mapping(diagnostic.get("evidence"))
        waterfall = as_mapping(evidence.get("quality_waterfall"))
        for result in as_list(waterfall.get("results")):
            if not isinstance(result, Mapping):
                continue
            if as_text(result.get("filter_id")) != "market_context_regime_fit":
                continue
            status = as_text(result.get("status")) or "unknown"
            status_counts[status] += 1
            for reason in as_list(result.get("reason_codes")):
                reason_code = as_text(reason)
                if reason_code is not None:
                    reason_counts[reason_code] += 1
            latest_metrics = dict(as_mapping(result.get("metrics")) or latest_metrics)
            latest_thresholds = dict(as_mapping(result.get("thresholds")) or latest_thresholds)
    if not status_counts and not reason_counts and not latest_metrics:
        return {}
    return {
        "status_counts": dict(sorted(status_counts.items())),
        "top_reason_counts": dict(reason_counts.most_common(8)),
        "latest_metrics": latest_metrics,
        "latest_thresholds": latest_thresholds,
    }

def _market_context_benchmark_rows(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in as_list(value):
        if not isinstance(item, Mapping):
            continue
        row = as_mapping(item)
        rows.append(
            {
                "symbol": as_text(row.get("symbol")),
                "role": as_text(row.get("role")),
                "freshness": as_text(row.get("freshness")),
                "data_quality": as_text(row.get("data_quality")),
                "return_1d_pct": coerce_float(row.get("return_1d_pct")),
                "return_5d_pct": coerce_float(row.get("return_5d_pct")),
                "intraday_return_pct": coerce_float(row.get("intraday_return_pct")),
                "drawdown_5d_pct": coerce_float(row.get("drawdown_5d_pct")),
                "realized_volatility_5d_pct": coerce_float(row.get("realized_volatility_5d_pct")),
                "reason_codes": [reason for reason in as_list(row.get("reason_codes")) if as_text(reason) is not None],
            }
        )
    return rows


def _market_context_payload_from_row(row: Mapping[str, Any], *, now: datetime, market_open: bool) -> dict[str, Any]:
    payload = as_mapping(row.get("payload"))
    regime = as_mapping(payload.get("regime")) or as_mapping(row.get("regime"))
    data_quality = as_mapping(payload.get("data_quality"))
    regime_data_quality = as_mapping(regime.get("data_quality"))
    metrics = as_mapping(regime.get("metrics"))
    observed_at = payload.get("observed_at") or row.get("observed_at")
    expires_at = payload.get("expires_at") or row.get("expires_at")
    observed_dt = _parse_utc_timestamp(observed_at)
    expires_dt = _parse_utc_timestamp(expires_at)
    expired = expires_dt is not None and expires_dt <= now
    freshness = as_text(data_quality.get("freshness") or regime_data_quality.get("freshness") or row.get("freshness_state")) or "unknown"
    quality_state = as_text(data_quality.get("state") or regime_data_quality.get("state") or row.get("data_quality_state")) or "unknown"
    state = "stale" if expired or freshness in {"stale", "missing"} else "fresh"
    if observed_dt is None:
        state = "missing"
    elif quality_state in {"missing", "degraded"}:
        state = quality_state
    status = "degraded" if market_open and state in {"stale", "missing", "degraded"} else "healthy"
    reason = None
    if state == "stale":
        reason = "market_context_stale"
    elif state == "missing":
        reason = "market_context_missing"
    elif state == "degraded":
        reason = "market_context_data_quality_degraded"
    benchmark_evidence = _market_context_benchmark_rows(payload.get("benchmark_evidence") or row.get("benchmark_evidence"))
    return {
        "status": status,
        "state": state,
        "reason": reason,
        "market_context_snapshot_id": row.get("market_context_snapshot_id") or payload.get("snapshot_id"),
        "scope": payload.get("scope") or row.get("scope"),
        "observed_at": utc_iso(observed_dt) if observed_dt is not None else utc_iso(row.get("observed_at")),
        "expires_at": utc_iso(expires_dt) if expires_dt is not None else utc_iso(row.get("expires_at")),
        "age_seconds": None if observed_dt is None else round(max((now - observed_dt).total_seconds(), 0.0), 2),
        "expires_in_seconds": None if expires_dt is None else round((expires_dt - now).total_seconds(), 2),
        "market_open": market_open,
        "context_version": row.get("context_version") or payload.get("context_version"),
        "config_hash": row.get("config_hash") or payload.get("config_hash"),
        "regime": {
            "regime_label": as_text(regime.get("regime_label") or row.get("regime_label")),
            "risk_posture": as_text(regime.get("risk_posture") or row.get("risk_posture")),
            "trend_strength": as_text(regime.get("trend_strength") or row.get("trend_strength")),
            "volatility_state": as_text(regime.get("volatility_state") or row.get("volatility_state")),
            "confidence": coerce_float(regime.get("confidence") or row.get("confidence")),
            "reason_codes": [reason for reason in as_list(regime.get("reason_codes")) if as_text(reason) is not None],
            "metrics": dict(metrics),
        },
        "data_quality": {
            "state": quality_state,
            "freshness": freshness,
            "reason_codes": [reason for reason in as_list(data_quality.get("reason_codes")) if as_text(reason) is not None],
            "missing_components": [item for item in as_list(data_quality.get("missing_components")) if as_text(item) is not None],
            "component_states": dict(as_mapping(data_quality.get("component_states"))),
        },
        "major_evidence": {
            "benchmark_symbols": [as_text(row.get("symbol")) for row in benchmark_evidence if as_text(row.get("symbol")) is not None],
            "expected_benchmark_count": coerce_int(metrics.get("expected_benchmark_count")),
            "observed_benchmark_count": coerce_int(metrics.get("observed_benchmark_count")) or len(benchmark_evidence),
            "supportive_benchmark_count": coerce_int(metrics.get("supportive_benchmark_count")) or 0,
            "supportive_benchmarks": list(as_list(metrics.get("supportive_benchmarks"))),
            "blocking_benchmark_count": coerce_int(metrics.get("blocking_benchmark_count")) or 0,
            "blocking_benchmarks": list(as_list(metrics.get("blocking_benchmarks"))),
            "average_return_5d_pct": coerce_float(metrics.get("average_return_5d_pct")),
            "average_intraday_return_pct": coerce_float(metrics.get("average_intraday_return_pct")),
            "average_realized_volatility_5d_pct": coerce_float(metrics.get("average_realized_volatility_5d_pct")),
        },
        "benchmark_evidence": benchmark_evidence,
        "fidelity": list(payload.get("fidelity") or row.get("fidelity") or []),
        "source_evidence": dict(as_mapping(payload.get("source_evidence") or row.get("source_evidence"))),
    }


def _project_market_context(
    *,
    storage: Any,
    now: datetime,
    market_open: bool,
) -> _MarketContextProjection:
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    engine_facts = storage.engine_facts
    if not engine_facts.market_context_schema_ready():
        payload = {
            "status": "blocked",
            "state": "schema_unavailable",
            "reason": "market_context_schema_unavailable",
            "market_open": market_open,
        }
        attention.append(
            _attention(
                severity="high",
                code="market_context_schema_unavailable",
                message="Market context snapshot storage is not available.",
            )
        )
        statuses.append("blocked")
    else:
        row = engine_facts.latest_market_context_snapshot(scope="global_market", as_of=now, include_expired=True)
        if row is None:
            status = "degraded" if market_open else "healthy"
            payload = {
                "status": status,
                "state": "missing",
                "reason": "market_context_missing",
                "market_open": market_open,
            }
            statuses.append(status)
            if market_open:
                attention.append(
                    _attention(
                        severity="medium",
                        code="market_context_missing",
                        message="No current market context snapshot has been observed for the global market scope.",
                    )
                )
        else:
            payload = _market_context_payload_from_row(row, now=now, market_open=market_open)
            statuses.append(str(payload.get("status") or "unknown"))
            if payload.get("status") == "degraded":
                attention.append(
                    _attention(
                        severity="medium",
                        code=str(payload.get("reason") or "market_context_degraded"),
                        message="The latest market context snapshot is stale, missing, or degraded.",
                    )
                )
    regime = as_mapping(payload.get("regime"))
    data_quality = as_mapping(payload.get("data_quality"))
    summary = {
        "market_context_status": payload.get("status"),
        "market_context_state": payload.get("state"),
        "market_context_reason": payload.get("reason"),
        "market_context_snapshot_id": payload.get("market_context_snapshot_id"),
        "market_context_observed_at": payload.get("observed_at"),
        "market_context_expires_at": payload.get("expires_at"),
        "market_context_age_seconds": payload.get("age_seconds"),
        "market_context_regime_label": regime.get("regime_label"),
        "market_context_risk_posture": regime.get("risk_posture"),
        "market_context_trend_strength": regime.get("trend_strength"),
        "market_context_volatility_state": regime.get("volatility_state"),
        "market_context_confidence": regime.get("confidence"),
        "market_context_freshness": data_quality.get("freshness"),
        "market_context_data_quality": data_quality.get("state"),
    }
    return _MarketContextProjection(
        payload=payload,
        summary=summary,
        statuses=tuple(statuses),
        attention=attention,
    )

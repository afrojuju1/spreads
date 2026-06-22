from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.storage.serializers import parse_datetime, render_value
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int

BACKTEST_MARKET_CONTEXT_MISSING = "backtest_market_context_missing"
BACKTEST_MARKET_CONTEXT_REPLAY = "backtest_stored_market_context_replay"
BACKTEST_MARKET_CONTEXT_EXPIRED = "backtest_stored_market_context_expired"


def _utc_datetime(value: Any) -> datetime | None:
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_iso(value: Any) -> str | None:
    parsed = _utc_datetime(value)
    if parsed is None:
        return None
    return parsed.isoformat().replace("+00:00", "Z")


def _json_ready_mapping(value: Any) -> dict[str, Any]:
    rendered = render_value(value)
    return dict(rendered) if isinstance(rendered, Mapping) else {}


def _json_ready_list(value: Any) -> list[Any]:
    rendered = render_value(value)
    return list(rendered) if isinstance(rendered, list) else []


def _unique_text(values: Any) -> list[str]:
    raw_values = values if isinstance(values, (list, tuple, set)) and not isinstance(values, str) else (values,)
    normalized: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        rendered = str(value or "").strip()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def _append_fidelity(values: Any, *labels: str) -> list[str]:
    return _unique_text([*_unique_text(values), *labels])


def _market_context_payload_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_ready_mapping(row.get("payload"))
    regime = _json_ready_mapping(row.get("regime"))
    data_quality = _json_ready_mapping(payload.get("data_quality"))
    if not data_quality:
        data_quality = {
            "state": row.get("data_quality_state"),
            "freshness": row.get("freshness_state"),
        }
    if not regime:
        regime = _json_ready_mapping(payload.get("regime"))

    snapshot_id = as_text(row.get("market_context_snapshot_id")) or as_text(payload.get("snapshot_id"))
    if not payload:
        payload = {
            "snapshot_id": snapshot_id,
            "scope": row.get("scope"),
            "observed_at": _utc_iso(row.get("observed_at")),
            "expires_at": _utc_iso(row.get("expires_at")),
            "generated_at": _utc_iso(row.get("generated_at")),
            "context_version": row.get("context_version"),
            "config_hash": row.get("config_hash"),
            "regime": regime,
            "benchmark_evidence": _json_ready_list(row.get("benchmark_evidence")),
            "data_quality": data_quality,
            "fidelity": _unique_text(row.get("fidelity")),
            "source_evidence": _json_ready_mapping(row.get("source_evidence")),
        }
    else:
        payload["snapshot_id"] = snapshot_id
        payload.setdefault("scope", row.get("scope"))
        payload.setdefault("observed_at", _utc_iso(row.get("observed_at")))
        payload.setdefault("expires_at", _utc_iso(row.get("expires_at")))
        payload.setdefault("generated_at", _utc_iso(row.get("generated_at")))
        payload.setdefault("context_version", row.get("context_version"))
        payload.setdefault("config_hash", row.get("config_hash"))
        payload.setdefault("regime", regime)
        payload.setdefault("data_quality", data_quality)
        payload.setdefault("benchmark_evidence", _json_ready_list(row.get("benchmark_evidence")))
        payload.setdefault("source_evidence", _json_ready_mapping(row.get("source_evidence")))

    payload["fidelity"] = _append_fidelity(payload.get("fidelity"), "stored_market_context", BACKTEST_MARKET_CONTEXT_REPLAY)
    source_evidence = _json_ready_mapping(payload.get("source_evidence"))
    source_evidence["backtest_replay"] = {
        "source": "market_context_snapshots",
        "fidelity": BACKTEST_MARKET_CONTEXT_REPLAY,
    }
    payload["source_evidence"] = source_evidence
    return payload


def _reference_from_payload(
    *,
    payload: Mapping[str, Any],
    row: Mapping[str, Any],
    as_of: datetime,
    status: str,
    fidelity_label: str,
) -> dict[str, Any]:
    regime = as_mapping(payload.get("regime")) or as_mapping(row.get("regime"))
    data_quality = as_mapping(payload.get("data_quality"))
    observed_at = _utc_iso(payload.get("observed_at")) or _utc_iso(row.get("observed_at"))
    expires_at = _utc_iso(payload.get("expires_at")) or _utc_iso(row.get("expires_at"))
    regime_label = as_text(regime.get("regime_label")) or as_text(row.get("regime_label")) or "unknown"
    return {
        "status": status,
        "source": "market_context_snapshots",
        "scope": payload.get("scope") or row.get("scope") or "global_market",
        "as_of": _utc_iso(as_of),
        "market_context_snapshot_id": payload.get("snapshot_id") or row.get("market_context_snapshot_id"),
        "observed_at": observed_at,
        "expires_at": expires_at,
        "regime_label": regime_label,
        "risk_posture": as_text(regime.get("risk_posture")) or as_text(row.get("risk_posture")) or "unknown",
        "trend_strength": as_text(regime.get("trend_strength")) or as_text(row.get("trend_strength")) or "unknown",
        "volatility_state": as_text(regime.get("volatility_state")) or as_text(row.get("volatility_state")) or "unknown",
        "confidence": coerce_float(regime.get("confidence") or row.get("confidence")),
        "freshness": data_quality.get("freshness") or row.get("freshness_state"),
        "data_quality_state": data_quality.get("state") or row.get("data_quality_state"),
        "fidelity_labels": _append_fidelity(payload.get("fidelity") or row.get("fidelity"), fidelity_label),
        "regime_bucket": regime_label if status == "ready" else f"market_context_{status}",
        "payload_available": status == "ready",
    }


def resolve_backtest_market_context(
    *,
    storage: Any,
    as_of: datetime,
    scope: str = "global_market",
) -> dict[str, Any]:
    """Replay a stored MarketContextSnapshot for a historical evaluator run.

    The returned ``payload`` is only populated when the stored snapshot is fresh
    for ``as_of``. Expired snapshots remain visible through ``reference`` so the
    artifact carries fidelity without making entry quality consume stale regime.
    """

    row = storage.engine_facts.latest_market_context_snapshot(scope=scope, as_of=as_of, include_expired=True)
    if row is None:
        reference = {
            "status": "missing",
            "source": "market_context_snapshots",
            "scope": scope,
            "as_of": _utc_iso(as_of),
            "market_context_snapshot_id": None,
            "regime_label": "unknown",
            "risk_posture": "unknown",
            "trend_strength": "unknown",
            "volatility_state": "unknown",
            "confidence": None,
            "freshness": "missing",
            "data_quality_state": "missing",
            "fidelity_labels": [BACKTEST_MARKET_CONTEXT_MISSING],
            "regime_bucket": "market_context_missing",
            "payload_available": False,
        }
        return {
            "payload": {},
            "reference": reference,
            "fidelity_label": BACKTEST_MARKET_CONTEXT_MISSING,
            "regime_bucket": "market_context_missing",
        }

    as_of_dt = as_of.astimezone(UTC) if as_of.tzinfo is not None else as_of.replace(tzinfo=UTC)
    expires_at = _utc_datetime(row.get("expires_at"))
    expired = expires_at is not None and expires_at <= as_of_dt
    payload = _market_context_payload_from_row(row)
    fidelity_label = BACKTEST_MARKET_CONTEXT_EXPIRED if expired else BACKTEST_MARKET_CONTEXT_REPLAY
    reference = _reference_from_payload(
        payload=payload,
        row=row,
        as_of=as_of_dt,
        status="expired" if expired else "ready",
        fidelity_label=fidelity_label,
    )
    return {
        "payload": {} if expired else payload,
        "reference": reference,
        "fidelity_label": fidelity_label,
        "regime_bucket": reference["regime_bucket"],
    }


def _artifact_rows(day_result: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    value = day_result.get(key)
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _count_selected(rows: list[Mapping[str, Any]]) -> int:
    return sum(1 for row in rows if str(row.get("decision_state") or "").strip().lower() == "selected")


def _count_approved(rows: list[Mapping[str, Any]]) -> int:
    return sum(1 for row in rows if str(row.get("admission_state") or "").strip().lower() == "approved")


def _bump(counter: dict[str, int], value: Any) -> None:
    key = str(value or "unknown").strip() or "unknown"
    counter[key] = counter.get(key, 0) + 1


def _append_unique(values: list[str], value: Any) -> None:
    rendered = str(value or "").strip()
    if rendered and rendered not in values:
        values.append(rendered)


def summarize_market_context_day_results(day_results: list[Mapping[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = {}
    status_counts: Counter[str] = Counter()
    snapshot_ids: list[str] = []
    for day_result in day_results:
        reference = as_mapping(day_result.get("market_context"))
        bucket_key = as_text(reference.get("regime_bucket")) or as_text(reference.get("regime_label")) or "market_context_missing"
        status = as_text(reference.get("status")) or "missing"
        status_counts[status] += 1
        _append_unique(snapshot_ids, reference.get("market_context_snapshot_id"))
        bucket = buckets.setdefault(
            bucket_key,
            {
                "regime_bucket": bucket_key,
                "strategy_day_count": 0,
                "candidate_count": 0,
                "signal_count": 0,
                "decision_count": 0,
                "selected_count": 0,
                "admission_count": 0,
                "approved_count": 0,
                "attempt_count": 0,
                "fill_count": 0,
                "position_count": 0,
                "close_count": 0,
                "net_pnl": 0.0,
                "status_counts": {},
                "regime_label_counts": {},
                "risk_posture_counts": {},
                "snapshot_ids": [],
            },
        )
        signals = _artifact_rows(day_result, "signals")
        decisions = _artifact_rows(day_result, "decisions")
        admissions = _artifact_rows(day_result, "admissions")
        fills = _artifact_rows(day_result, "simulated_fills") + _artifact_rows(day_result, "execution_fills")
        positions = _artifact_rows(day_result, "simulated_positions")
        closes = _artifact_rows(day_result, "simulated_position_closes")
        attempts = _artifact_rows(day_result, "simulated_attempts") + _artifact_rows(day_result, "execution_attempts")
        pnl = as_mapping(day_result.get("pnl"))
        bucket["strategy_day_count"] += 1
        bucket["candidate_count"] += coerce_int(as_mapping(day_result.get("candidate_build")).get("candidate_count")) or len(
            _artifact_rows(day_result, "candidates")
        )
        bucket["signal_count"] += len(signals)
        bucket["decision_count"] += len(decisions)
        bucket["selected_count"] += _count_selected(decisions)
        bucket["admission_count"] += len(admissions)
        bucket["approved_count"] += _count_approved(admissions)
        bucket["attempt_count"] += len(attempts)
        bucket["fill_count"] += len(fills)
        bucket["position_count"] += len(positions)
        bucket["close_count"] += len(closes)
        bucket["net_pnl"] += coerce_float(pnl.get("net_pnl")) or 0.0
        _bump(bucket["status_counts"], status)
        _bump(bucket["regime_label_counts"], reference.get("regime_label"))
        _bump(bucket["risk_posture_counts"], reference.get("risk_posture"))
        _append_unique(bucket["snapshot_ids"], reference.get("market_context_snapshot_id"))

    rendered_buckets: list[dict[str, Any]] = []
    for key in sorted(buckets):
        bucket = buckets[key]
        decision_count = coerce_int(bucket.get("decision_count")) or 0
        admission_count = coerce_int(bucket.get("admission_count")) or 0
        attempt_count = coerce_int(bucket.get("attempt_count")) or 0
        bucket["selection_rate"] = None if decision_count <= 0 else round((bucket["selected_count"] or 0) / decision_count, 4)
        bucket["admission_approval_rate"] = None if admission_count <= 0 else round((bucket["approved_count"] or 0) / admission_count, 4)
        bucket["fill_rate"] = None if attempt_count <= 0 else round((bucket["fill_count"] or 0) / attempt_count, 4)
        bucket["net_pnl"] = round(float(bucket["net_pnl"] or 0.0), 4)
        bucket["status_counts"] = dict(sorted(bucket["status_counts"].items()))
        bucket["regime_label_counts"] = dict(sorted(bucket["regime_label_counts"].items()))
        bucket["risk_posture_counts"] = dict(sorted(bucket["risk_posture_counts"].items()))
        rendered_buckets.append(bucket)

    return {
        "strategy_day_count": len(day_results),
        "status_counts": dict(sorted(status_counts.items())),
        "snapshot_ids": snapshot_ids,
        "regime_buckets": rendered_buckets,
    }


def merge_market_context_from_strategy_results(strategy_results: list[Mapping[str, Any]]) -> dict[str, Any]:
    day_results = [day for strategy_result in strategy_results for day in list(strategy_result.get("day_results") or []) if isinstance(day, Mapping)]
    return summarize_market_context_day_results(day_results)


__all__ = [
    "BACKTEST_MARKET_CONTEXT_EXPIRED",
    "BACKTEST_MARKET_CONTEXT_MISSING",
    "BACKTEST_MARKET_CONTEXT_REPLAY",
    "merge_market_context_from_strategy_results",
    "resolve_backtest_market_context",
    "summarize_market_context_day_results",
]

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

from core.services.option_structures import candidate_legs, payload_structure_identity
from core.services.opportunity_fields import candidate_economics, risk_hints
from core.services.trading_engine.data import CandidateBuildResult, ResolvedTickerSet
from core.services.trading_strategy_runtime import EntryRuntime


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _stable_id(prefix: str, *parts: Any) -> str:
    material = json.dumps([str(part) for part in parts], sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"{prefix}:{hashlib.sha1(material).hexdigest()[:24]}"


def _text_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    normalized: list[str] = []
    for item in value:
        rendered = str(item or "").strip()
        if rendered and rendered not in normalized:
            normalized.append(rendered)
    return normalized


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _score(row: Mapping[str, Any]) -> float | None:
    for key in ("execution_score", "promotion_score", "quality_score", "score"):
        value = _optional_float(row.get(key))
        if value is not None:
            return value
    return None


def _candidate_identity(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("candidate_identity") or candidate.get("structure_identity") or payload_structure_identity(dict(candidate)) or "")


def _blockers(row: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for field in (
        "blockers",
        "scoring_blockers",
        "execution_blockers",
        "ranking_policy_blockers",
    ):
        for blocker in _text_list(row.get(field)):
            if blocker not in blockers:
                blockers.append(blocker)
    return blockers


def _candidate_state(candidate: Mapping[str, Any]) -> str:
    blockers = _blockers(candidate)
    if blockers:
        return "blocked"
    ranking_status = str(candidate.get("ranking_policy_status") or "").strip().lower()
    scoring_state = str(candidate.get("scoring_state") or "").strip().lower()
    if ranking_status in {"blocked", "failed"} or scoring_state == "blocked":
        return "blocked"
    return "buildable"


def _signal_state(opportunity: Mapping[str, Any]) -> str:
    if _blockers(opportunity):
        return "blocked"
    selection_state = str(opportunity.get("selection_state") or "").strip().lower()
    if selection_state == "promotable":
        return "ready"
    if selection_state == "monitor":
        return "observed"
    return "observed"


def _source_run_id(
    *,
    run_key: str,
    ticker_set: ResolvedTickerSet,
) -> str:
    if ticker_set.source_run_id:
        return _stable_id("source_run", ticker_set.source.source_type, ticker_set.source.ref, ticker_set.source_run_id)
    return _stable_id("source_run", ticker_set.source.source_type, ticker_set.source.ref, run_key)


def _source_status(ticker_set: ResolvedTickerSet) -> str:
    if ticker_set.blockers:
        return "blocked"
    evidence_status = str((ticker_set.evidence or {}).get("status") or "").strip().lower()
    return evidence_status or "ready"


def _source_entries(ticker_set: ResolvedTickerSet) -> list[dict[str, Any]]:
    entries = ticker_set.evidence.get("entries") if isinstance(ticker_set.evidence, Mapping) else None
    if not isinstance(entries, list):
        return []
    return [dict(entry) for entry in entries if isinstance(entry, Mapping)]


def _candidate_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return dict(row)


def persist_entry_engine_facts(
    *,
    engine_facts: Any,
    runtime: EntryRuntime,
    market_date: str,
    run_key: str,
    generated_at: str,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult | None,
    opportunities: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if engine_facts is None or not engine_facts.schema_ready():
        return {
            "status": "skipped",
            "reason": "engine_fact_schema_unavailable",
        }

    now = _utc_now()
    source_summary = ticker_set.evidence.get("summary") if isinstance(ticker_set.evidence.get("summary"), Mapping) else {}
    source_run_id = _source_run_id(run_key=run_key, ticker_set=ticker_set)
    source_run = engine_facts.upsert_source_run(
        source_run_id=source_run_id,
        source_type=ticker_set.source.source_type,
        source_ref=ticker_set.source.ref,
        source_job_run_id=ticker_set.source_run_id,
        status=_source_status(ticker_set),
        config_hash=runtime.config_hash,
        generated_at=ticker_set.resolved_at.isoformat().replace("+00:00", "Z"),
        completed_at=generated_at,
        symbols=list(ticker_set.symbols),
        entries=_source_entries(ticker_set),
        summary=dict(source_summary),
        evidence={
            "reason_codes": list(ticker_set.reason_codes),
            "blockers": list(ticker_set.blockers),
            "snapshot": dict(ticker_set.evidence or {}),
        },
        updated_at=now,
    )

    candidate_rows = [dict(row) for row in tuple(() if candidate_result is None else candidate_result.candidates) if isinstance(row, Mapping)]
    candidate_run_id = None if candidate_result is None else candidate_result.candidate_run_id
    if candidate_result is not None:
        engine_facts.upsert_candidate_run(
            candidate_run_id=candidate_result.candidate_run_id,
            run_key=run_key,
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            source_run_id=str(source_run["source_run_id"]),
            source_type=ticker_set.source.source_type,
            source_ref=ticker_set.source.ref,
            status=str(candidate_result.summary.get("status") or "completed"),
            config_hash=runtime.config_hash,
            generated_at=generated_at,
            completed_at=generated_at,
            symbol_count=len(ticker_set.symbols),
            candidate_count=len(candidate_rows),
            summary=dict(candidate_result.summary or {}),
            evidence={
                "ticker_set": {
                    "source_run_id": source_run["source_run_id"],
                    "source_type": ticker_set.source.source_type,
                    "source_ref": ticker_set.source.ref,
                    "symbols": list(ticker_set.symbols),
                },
            },
            updated_at=now,
        )

    trade_candidate_ids_by_identity: dict[str, str] = {}
    if candidate_run_id is not None:
        for rank, candidate in enumerate(candidate_rows, start=1):
            identity = _candidate_identity(candidate)
            if not identity:
                continue
            symbol = str(candidate.get("underlying_symbol") or "").upper()
            if not symbol:
                continue
            trade_candidate_id = _stable_id("trade_candidate", candidate_run_id, symbol, identity)
            trade_candidate_ids_by_identity[identity] = trade_candidate_id
            engine_facts.upsert_trade_candidate(
                trade_candidate_id=trade_candidate_id,
                candidate_run_id=candidate_run_id,
                trading_strategy_id=runtime.trading_strategy_id,
                trade_structure=runtime.trade_structure,
                routine="entry",
                config_hash=runtime.config_hash,
                underlying_symbol=symbol,
                root_symbol=str(candidate.get("root_symbol") or symbol),
                candidate_identity=identity,
                rank=rank,
                score=_score(candidate),
                confidence=_optional_float(candidate.get("confidence")),
                expiration_date=candidate.get("expiration_date"),
                selection_state=(None if candidate.get("selection_state") in (None, "") else str(candidate.get("selection_state"))),
                candidate_state=_candidate_state(candidate),
                observed_at=generated_at,
                expires_at=None,
                legs=candidate_legs(dict(candidate)),
                execution_shape=dict(candidate.get("execution_shape") or {}),
                economics=candidate_economics(dict(candidate)),
                risk_hints=risk_hints(dict(candidate)),
                reason_codes=_text_list(candidate.get("reason_codes")),
                blockers=_blockers(candidate),
                candidate=dict(candidate),
                evidence={
                    "source_run_id": source_run["source_run_id"],
                    "candidate_run_id": candidate_run_id,
                    "ranking_policy_status": candidate.get("ranking_policy_status"),
                    "scoring_state": candidate.get("scoring_state"),
                },
                updated_at=now,
            )

    trade_signal_count = 0
    for opportunity in opportunities:
        candidate = _candidate_payload(opportunity)
        identity = str(opportunity.get("candidate_identity") or _candidate_identity(candidate))
        if not identity:
            continue
        symbol = str(opportunity.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
        if not symbol:
            continue
        trade_candidate_id = trade_candidate_ids_by_identity.get(identity)
        idempotency_key = f"entry:{runtime.trading_strategy_id}:{market_date}:{symbol}:{identity}"
        trade_signal_id = _stable_id("trade_signal", idempotency_key)
        signal_state = _signal_state(opportunity)
        engine_facts.upsert_trade_signal(
            trade_signal_id=trade_signal_id,
            idempotency_key=idempotency_key,
            trade_candidate_id=trade_candidate_id,
            source_kind="trade_candidate" if trade_candidate_id is not None else "candidate_run",
            source_id=trade_candidate_id or str(candidate_run_id or source_run["source_run_id"]),
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            config_hash=runtime.config_hash,
            session_date=market_date,
            market_session="regular",
            observed_at=generated_at,
            expires_at=None if opportunity.get("expires_at") in (None, "") else str(opportunity.get("expires_at")),
            underlying_symbol=symbol,
            root_symbol=str(opportunity.get("root_symbol") or symbol),
            asset_class="option",
            product_class=(None if opportunity.get("product_class") in (None, "") else str(opportunity.get("product_class"))),
            horizon=(None if opportunity.get("horizon_intent") in (None, "") else str(opportunity.get("horizon_intent"))),
            style_profile=(None if opportunity.get("style_profile") in (None, "") else str(opportunity.get("style_profile"))),
            signal_state=signal_state,
            rank=_optional_int(opportunity.get("selection_rank")),
            score=_score(opportunity),
            confidence=_optional_float(opportunity.get("confidence")),
            legs=list(opportunity.get("legs") or candidate_legs(candidate)),
            execution_shape=dict(opportunity.get("execution_shape") or {}),
            economics=dict(opportunity.get("economics") or candidate_economics(candidate)),
            reason_codes=_text_list(opportunity.get("reason_codes")),
            blockers=_blockers(opportunity),
            evidence={
                "source_run_id": source_run["source_run_id"],
                "candidate_run_id": candidate_run_id,
                "trade_candidate_id": trade_candidate_id,
                "opportunity_id": opportunity.get("opportunity_id"),
                "selection_state": opportunity.get("selection_state"),
                "candidate_identity": identity,
            },
            metrics=dict(opportunity.get("strategy_metrics") or {}),
            updated_at=now,
        )
        trade_signal_count += 1

    return {
        "status": "ok",
        "source_run_id": source_run["source_run_id"],
        "candidate_run_id": candidate_run_id,
        "trade_candidate_count": len(trade_candidate_ids_by_identity),
        "trade_signal_count": trade_signal_count,
    }


__all__ = ["persist_entry_engine_facts"]

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
import hashlib
import json
from typing import Any

from core.services.option_structures import candidate_legs, payload_structure_identity
from core.services.candidate_fields import candidate_economics, risk_hints
from core.services.trading_engine.data import CandidateBuildResult, ResolvedTickerSet
from core.services.trading_engine.entry_quality import (
    EntryQualityContext,
    EntryQualityWaterfall,
    FeatureSnapshot,
    MOMENTUM_LONG_CALL_PROFILE_ID,
)
from core.services.trading_engine.entry_quality_pipeline import evaluate_momentum_long_call_snapshot
from core.services.trading_engine.feature_snapshots import build_momentum_long_call_feature_snapshots
from core.services.trading_strategy_runtime import EntryRuntime
from core.value_coercion import coerce_float, coerce_int, unique_text_list, utc_now_iso as _utc_now


def _stable_id(prefix: str, *parts: Any) -> str:
    material = json.dumps([str(part) for part in parts], sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"{prefix}:{hashlib.sha1(material).hexdigest()[:24]}"


def entry_trade_signal_idempotency_key(
    *,
    trading_strategy_id: str,
    market_date: str,
    underlying_symbol: str,
    candidate_identity: str,
) -> str:
    return f"entry:{trading_strategy_id}:{market_date}:{underlying_symbol.upper()}:{candidate_identity}"


def entry_trade_signal_id(
    *,
    trading_strategy_id: str,
    market_date: str,
    underlying_symbol: str,
    candidate_identity: str,
) -> str:
    return _stable_id(
        "trade_signal",
        entry_trade_signal_idempotency_key(
            trading_strategy_id=trading_strategy_id,
            market_date=market_date,
            underlying_symbol=underlying_symbol,
            candidate_identity=candidate_identity,
        ),
    )


def _score(row: Mapping[str, Any]) -> float | None:
    for key in ("execution_score", "promotion_score", "quality_score", "score"):
        value = coerce_float(row.get(key))
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
        for blocker in unique_text_list(row.get(field)):
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


def _signal_state(signal_row: Mapping[str, Any]) -> str:
    if _blockers(signal_row):
        return "blocked"
    selection_state = str(signal_row.get("selection_state") or "").strip().lower()
    if selection_state == "promotable":
        return "ready"
    if selection_state == "monitor":
        return "observed"
    return "observed"


def _candidate_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return dict(row)


def _quality_context(runtime: EntryRuntime) -> EntryQualityContext:
    return EntryQualityContext(
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        quality_profile_id=MOMENTUM_LONG_CALL_PROFILE_ID,
    )


def _quality_key(
    *,
    symbol: str,
    candidate_identity: str | None,
) -> tuple[str, str]:
    return symbol.upper(), str(candidate_identity or "")


def _quality_key_for_candidate(candidate: Mapping[str, Any]) -> tuple[str, str] | None:
    symbol = str(candidate.get("underlying_symbol") or "").upper()
    if not symbol:
        return None
    return _quality_key(symbol=symbol, candidate_identity=_candidate_identity(candidate))


def _quality_key_for_snapshot(snapshot: FeatureSnapshot) -> tuple[str, str]:
    candidate = snapshot.candidate if isinstance(snapshot.candidate, Mapping) else {}
    return _quality_key(
        symbol=snapshot.symbol,
        candidate_identity=_candidate_identity(candidate) if candidate else snapshot.metadata.get("candidate_identity"),
    )


def _quality_reason_counts(waterfalls: Sequence[EntryQualityWaterfall], *, statuses: set[str]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for waterfall in waterfalls:
        for result in waterfall.results:
            if result.status.value not in statuses:
                continue
            for reason in result.reason_codes:
                counts[reason] += 1
    return dict(counts.most_common(12))


def _quality_stage_counts(waterfalls: Sequence[EntryQualityWaterfall]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for waterfall in waterfalls:
        for stage, stage_counts in waterfall.stage_counts().items():
            target = counts.setdefault(stage, {})
            for status, count in stage_counts.items():
                target[status] = target.get(status, 0) + int(count)
    return counts


def _quality_summary(waterfalls: Sequence[EntryQualityWaterfall]) -> dict[str, Any]:
    rows = tuple(waterfalls)
    return {
        "quality_profile_id": MOMENTUM_LONG_CALL_PROFILE_ID,
        "quality_snapshot_count": len(rows),
        "quality_blocked_snapshot_count": sum(1 for waterfall in rows if waterfall.blocked),
        "filter_stage_counts": _quality_stage_counts(rows),
        "top_quality_blockers": _quality_reason_counts(rows, statuses={"block"}),
        "top_quality_watch_reasons": _quality_reason_counts(rows, statuses={"watch"}),
    }


def _build_quality_waterfalls(
    *,
    runtime: EntryRuntime,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult | None,
) -> tuple[
    dict[tuple[str, str], EntryQualityWaterfall],
    dict[str, EntryQualityWaterfall],
    dict[tuple[str, str], FeatureSnapshot],
    dict[str, Any],
]:
    if candidate_result is None:
        return {}, {}, {}, _quality_summary(())

    context = _quality_context(runtime)
    snapshots = build_momentum_long_call_feature_snapshots(
        ticker_set=ticker_set,
        candidate_result=candidate_result,
    )
    by_candidate: dict[tuple[str, str], EntryQualityWaterfall] = {}
    by_symbol: dict[str, EntryQualityWaterfall] = {}
    snapshots_by_candidate: dict[tuple[str, str], FeatureSnapshot] = {}
    all_waterfalls: list[EntryQualityWaterfall] = []
    for snapshot in snapshots:
        waterfall = evaluate_momentum_long_call_snapshot(
            context=context,
            snapshot=snapshot,
        )
        all_waterfalls.append(waterfall)
        key = _quality_key_for_snapshot(snapshot)
        if key[1]:
            by_candidate[key] = waterfall
            snapshots_by_candidate[key] = snapshot
        by_symbol.setdefault(snapshot.symbol, waterfall)
    return by_candidate, by_symbol, snapshots_by_candidate, _quality_summary(all_waterfalls)


def _quality_waterfall_for_signal(
    *,
    runtime: EntryRuntime,
    signal_row: Mapping[str, Any],
    by_candidate: Mapping[tuple[str, str], EntryQualityWaterfall],
    by_symbol: Mapping[str, EntryQualityWaterfall],
    snapshots_by_candidate: Mapping[tuple[str, str], FeatureSnapshot],
) -> EntryQualityWaterfall | None:
    candidate = _candidate_payload(signal_row)
    key = _quality_key_for_candidate(candidate)
    if key is not None:
        existing = by_candidate.get(key)
        base = snapshots_by_candidate.get(key)
        if base is not None:
            return evaluate_momentum_long_call_snapshot(
                context=_quality_context(runtime),
                snapshot=base,
                candidate=signal_row,
            )
        if existing is not None:
            return existing
    symbol = str(signal_row.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
    return by_symbol.get(symbol)


def _waterfall_evidence(waterfall: EntryQualityWaterfall | None) -> dict[str, Any]:
    if waterfall is None:
        return {
            "quality_profile_id": MOMENTUM_LONG_CALL_PROFILE_ID,
            "quality_waterfall": None,
        }
    return {
        "quality_profile_id": waterfall.profile_id,
        "quality_waterfall": waterfall.as_dict(),
    }


def _diagnostic_rows(candidate_result: CandidateBuildResult | None, *, observed_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in tuple(() if candidate_result is None else candidate_result.diagnostics):
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        row["observed_at"] = row.get("observed_at") or observed_at
        rows.append(row)
    return rows


def _diagnostic_top_counts(diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in diagnostics:
        rejection_counts = row.get("rejection_counts") if isinstance(row.get("rejection_counts"), Mapping) else {}
        top = rejection_counts.get("top") if isinstance(rejection_counts, Mapping) and isinstance(rejection_counts.get("top"), Mapping) else {}
        for reason, count in dict(top).items():
            try:
                counts[str(reason)] += int(count)
            except (TypeError, ValueError):
                continue
    return dict(counts.most_common(12))


def _candidate_run_diagnostic_status(
    *,
    candidate_count: int,
    diagnostics: Sequence[Mapping[str, Any]],
) -> str:
    if candidate_count > 0:
        return "candidate_available"
    if not diagnostics:
        return "diagnostics_missing"
    statuses = {str(row.get("diagnostic_status") or row.get("status") or "").strip() for row in diagnostics}
    statuses.discard("")
    if "build_error" in statuses:
        return "build_error"
    if statuses and statuses <= {"data_unavailable"}:
        return "data_unavailable"
    if statuses & {"ranking_rejected", "postprocess_rejected", "runtime_rejected"}:
        return "filtered_out"
    if "no_raw_candidates" in statuses:
        return "no_raw_candidates"
    return "no_candidates"


def _candidate_run_summary_with_diagnostics(
    *,
    base_summary: Mapping[str, Any],
    candidate_count: int,
    diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    summary = dict(base_summary or {})
    status_counts = Counter(str(row.get("diagnostic_status") or row.get("status") or "unknown") for row in diagnostics if isinstance(row, Mapping))
    summary.update(
        {
            "diagnostic_status": _candidate_run_diagnostic_status(
                candidate_count=candidate_count,
                diagnostics=diagnostics,
            ),
            "diagnostic_symbol_count": len(diagnostics),
            "symbol_status_counts": dict(sorted(status_counts.items())),
            "top_rejection_counts": _diagnostic_top_counts(diagnostics),
        }
    )
    return summary


def persist_entry_engine_facts(
    *,
    engine_facts: Any,
    runtime: EntryRuntime,
    market_date: str,
    run_key: str,
    generated_at: str,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult | None,
    signal_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if engine_facts is None or not engine_facts.schema_ready():
        return {
            "status": "skipped",
            "reason": "engine_fact_schema_unavailable",
        }

    now = _utc_now()
    candidate_rows = [dict(row) for row in tuple(() if candidate_result is None else candidate_result.candidates) if isinstance(row, Mapping)]
    diagnostic_rows = _diagnostic_rows(candidate_result, observed_at=generated_at)
    candidate_run_id = None if candidate_result is None else candidate_result.candidate_run_id
    quality_by_candidate, quality_by_symbol, quality_snapshots_by_candidate, quality_summary = _build_quality_waterfalls(
        runtime=runtime,
        ticker_set=ticker_set,
        candidate_result=candidate_result,
    )
    diagnostic_rows = [
        {
            **row,
            "evidence": {
                **dict(row.get("evidence") or {}),
                **_waterfall_evidence(quality_by_symbol.get(str(row.get("underlying_symbol") or row.get("symbol") or "").upper())),
            },
        }
        for row in diagnostic_rows
    ]
    if candidate_result is not None:
        candidate_summary = _candidate_run_summary_with_diagnostics(
            base_summary=candidate_result.summary or {},
            candidate_count=len(candidate_rows),
            diagnostics=diagnostic_rows,
        )
        candidate_summary.update(quality_summary)
        engine_facts.upsert_candidate_run(
            candidate_run_id=candidate_result.candidate_run_id,
            run_key=run_key,
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            ticker_source_run_id=ticker_set.ticker_source_run_id,
            ticker_source_kind=ticker_set.source.source_type,
            ticker_source_id=ticker_set.source.ref,
            status=str(candidate_result.summary.get("status") or "completed"),
            config_hash=runtime.config_hash,
            generated_at=generated_at,
            completed_at=generated_at,
            symbol_count=len(ticker_set.symbols),
            candidate_count=len(candidate_rows),
            summary=candidate_summary,
            evidence={
                "ticker_set": {
                    "ticker_source_run_id": ticker_set.ticker_source_run_id,
                    "ticker_source_kind": ticker_set.source.source_type,
                    "ticker_source_id": ticker_set.source.ref,
                    "symbols": list(ticker_set.symbols),
                },
                "quality_profile_id": quality_summary["quality_profile_id"],
                "filter_stage_counts": quality_summary["filter_stage_counts"],
                "top_quality_blockers": quality_summary["top_quality_blockers"],
            },
            updated_at=now,
        )
        engine_facts.replace_candidate_symbol_diagnostics(
            candidate_run_id=candidate_result.candidate_run_id,
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            ticker_source_run_id=ticker_set.ticker_source_run_id,
            ticker_source_kind=ticker_set.source.source_type,
            ticker_source_id=ticker_set.source.ref,
            diagnostics=diagnostic_rows,
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
            quality_waterfall = quality_by_candidate.get(_quality_key(symbol=symbol, candidate_identity=identity))
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
                confidence=coerce_float(candidate.get("confidence")),
                expiration_date=candidate.get("expiration_date"),
                selection_state=(None if candidate.get("selection_state") in (None, "") else str(candidate.get("selection_state"))),
                candidate_state=_candidate_state(candidate),
                observed_at=generated_at,
                expires_at=None,
                legs=candidate_legs(dict(candidate)),
                execution_shape=dict(candidate.get("execution_shape") or {}),
                economics=candidate_economics(dict(candidate)),
                risk_hints=risk_hints(dict(candidate)),
                reason_codes=unique_text_list(candidate.get("reason_codes")),
                blockers=_blockers(candidate),
                candidate=dict(candidate),
                evidence={
                    "ticker_source_run_id": ticker_set.ticker_source_run_id,
                    "candidate_run_id": candidate_run_id,
                    "ranking_policy_status": candidate.get("ranking_policy_status"),
                    "scoring_state": candidate.get("scoring_state"),
                    **_waterfall_evidence(quality_waterfall),
                },
                updated_at=now,
            )

    trade_signal_refs: list[dict[str, Any]] = []
    for signal_row in signal_rows:
        candidate = _candidate_payload(signal_row)
        identity = str(signal_row.get("candidate_identity") or _candidate_identity(candidate))
        if not identity:
            continue
        symbol = str(signal_row.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
        if not symbol:
            continue
        trade_candidate_id = trade_candidate_ids_by_identity.get(identity)
        idempotency_key = entry_trade_signal_idempotency_key(
            trading_strategy_id=runtime.trading_strategy_id,
            market_date=market_date,
            underlying_symbol=symbol,
            candidate_identity=identity,
        )
        trade_signal_id = entry_trade_signal_id(
            trading_strategy_id=runtime.trading_strategy_id,
            market_date=market_date,
            underlying_symbol=symbol,
            candidate_identity=identity,
        )
        signal_state = _signal_state(signal_row)
        signal_waterfall = _quality_waterfall_for_signal(
            runtime=runtime,
            signal_row=signal_row,
            by_candidate=quality_by_candidate,
            by_symbol=quality_by_symbol,
            snapshots_by_candidate=quality_snapshots_by_candidate,
        )
        engine_facts.upsert_trade_signal(
            trade_signal_id=trade_signal_id,
            idempotency_key=idempotency_key,
            trade_candidate_id=trade_candidate_id,
            source_kind="trade_candidate" if trade_candidate_id is not None else "candidate_run",
            source_id=trade_candidate_id or str(candidate_run_id or ticker_set.ticker_source_run_id or run_key),
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            config_hash=runtime.config_hash,
            session_date=market_date,
            market_session="regular",
            observed_at=generated_at,
            expires_at=None if signal_row.get("expires_at") in (None, "") else str(signal_row.get("expires_at")),
            underlying_symbol=symbol,
            root_symbol=str(signal_row.get("root_symbol") or symbol),
            asset_class="option",
            product_class=(None if signal_row.get("product_class") in (None, "") else str(signal_row.get("product_class"))),
            horizon=(None if signal_row.get("horizon_intent") in (None, "") else str(signal_row.get("horizon_intent"))),
            style_profile=(None if signal_row.get("style_profile") in (None, "") else str(signal_row.get("style_profile"))),
            signal_state=signal_state,
            rank=coerce_int(signal_row.get("selection_rank")),
            score=_score(signal_row),
            confidence=coerce_float(signal_row.get("confidence")),
            legs=list(signal_row.get("legs") or candidate_legs(candidate)),
            execution_shape=dict(signal_row.get("execution_shape") or {}),
            economics=dict(signal_row.get("economics") or candidate_economics(candidate)),
            reason_codes=unique_text_list(signal_row.get("reason_codes")),
            blockers=_blockers(signal_row),
            evidence={
                "ticker_source_run_id": ticker_set.ticker_source_run_id,
                "candidate_run_id": candidate_run_id,
                "trade_candidate_id": trade_candidate_id,
                "selection_state": signal_row.get("selection_state"),
                "candidate_identity": identity,
                **_waterfall_evidence(signal_waterfall),
            },
            metrics={
                **dict(signal_row.get("strategy_metrics") or {}),
                "filter_stage_counts": ({} if signal_waterfall is None else signal_waterfall.stage_counts()),
            },
            updated_at=now,
        )
        trade_signal_refs.append(
            {
                "trade_signal_id": trade_signal_id,
                "trade_candidate_id": trade_candidate_id,
                "underlying_symbol": symbol,
                "candidate_identity": identity,
                "signal_state": signal_state,
                "quality_profile_id": MOMENTUM_LONG_CALL_PROFILE_ID,
                "quality_waterfall_stage_counts": ({} if signal_waterfall is None else signal_waterfall.stage_counts()),
                "quality_waterfall_blocked": None if signal_waterfall is None else signal_waterfall.blocked,
            }
        )

    return {
        "status": "ok",
        "ticker_source_run_id": ticker_set.ticker_source_run_id,
        "candidate_run_id": candidate_run_id,
        "trade_candidate_count": len(trade_candidate_ids_by_identity),
        "candidate_diagnostic_count": len(diagnostic_rows),
        "top_rejection_counts": _diagnostic_top_counts(diagnostic_rows),
        "quality_profile_id": quality_summary["quality_profile_id"],
        "filter_stage_counts": quality_summary["filter_stage_counts"],
        "top_quality_blockers": quality_summary["top_quality_blockers"],
        "trade_signal_count": len(trade_signal_refs),
        "trade_signals": trade_signal_refs,
    }


__all__ = [
    "entry_trade_signal_id",
    "entry_trade_signal_idempotency_key",
    "persist_entry_engine_facts",
]

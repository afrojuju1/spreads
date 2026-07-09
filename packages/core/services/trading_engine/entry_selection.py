from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from core.services.live_selection import select_live_signals
from core.services.trading_engine.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.data import CandidateBuildResult, ResolvedTickerSet
from core.services.quality.evidence import EntryQualityAnalysis, build_entry_quality_analysis
from core.services.trading_strategy_runtime_models import EntryRuntime


def candidate_result_summary(candidate_result: CandidateBuildResult | None) -> dict[str, Any]:
    if candidate_result is None:
        return {
            "status": "not_run",
            "candidate_count": 0,
            "symbol_count": 0,
        }
    return {
        "candidate_run_id": candidate_result.candidate_run_id,
        "candidate_count": len(candidate_result.candidates),
        **dict(candidate_result.summary or {}),
    }


def group_candidate_rows(candidates: tuple[Any, ...]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        symbol = str(candidate.get("underlying_symbol") or "").upper().strip()
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(dict(candidate))
    return grouped


def _selected_keys(candidates: tuple[Mapping[str, Any], ...]) -> set[tuple[str, str]]:
    return {
        (
            str(candidate.get("underlying_symbol") or "").upper(),
            resolve_candidate_identity(candidate),
        )
        for candidate in candidates
        if str(candidate.get("underlying_symbol") or "").strip() and resolve_candidate_identity(candidate)
    }


def _rejected_candidates(
    *,
    symbol_candidates: Mapping[str, list[dict[str, Any]]],
    selected_candidates: tuple[Mapping[str, Any], ...],
    monitored_candidates: tuple[Mapping[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    selected = _selected_keys((*selected_candidates, *monitored_candidates))
    rejected: list[dict[str, Any]] = []
    for rows in symbol_candidates.values():
        for row in rows:
            key = (str(row.get("underlying_symbol") or "").upper(), resolve_candidate_identity(row))
            if not key[0] or not key[1] or key in selected:
                continue
            rejected.append(
                {
                    **dict(row),
                    "selection_state": row.get("selection_state") or "rejected",
                    "state_reason": row.get("state_reason") or "not_selected",
                }
            )
    return tuple(rejected)


@dataclass(frozen=True)
class EntrySelectionResult:
    candidate_result: CandidateBuildResult
    quality_analysis: EntryQualityAnalysis | None
    quality_summary: Mapping[str, Any]
    symbol_candidates: Mapping[str, list[dict[str, Any]]]
    selection: Mapping[str, Any]
    selected_candidates: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    monitored_candidates: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    rejected_candidates: tuple[dict[str, Any], ...] = field(default_factory=tuple)


class EntrySelectionEngine:
    def select(
        self,
        *,
        runtime: EntryRuntime,
        ticker_set: ResolvedTickerSet,
        ticker_summary: Mapping[str, Any],
        candidate_result: CandidateBuildResult,
        label: str,
        cycle_id: str,
        generated_at: str,
        previous_promotable: Mapping[str, dict[str, Any]],
        previous_selection_memory: Mapping[str, dict[str, Any]],
        top_promotable: int,
        top_monitor: int,
    ) -> EntrySelectionResult:
        quality_analysis = None
        quality_summary: dict[str, Any] = {}
        if runtime.quality_profile_id is not None:
            quality_analysis = build_entry_quality_analysis(
                runtime=runtime,
                ticker_set=ticker_set,
                candidate_result=candidate_result,
            )
            quality_candidates = quality_analysis.filter_candidates([dict(row) for row in candidate_result.candidates if isinstance(row, dict)])
            quality_summary = dict(quality_analysis.summary)
            candidate_result = replace(
                candidate_result,
                candidates=quality_candidates,
                summary={
                    **dict(candidate_result.summary or {}),
                    **quality_summary,
                    "candidate_count": len(quality_candidates),
                    "quality_blocked_candidate_count": len(candidate_result.candidates) - len(quality_candidates),
                },
            )

        symbol_candidates = group_candidate_rows(candidate_result.candidates)
        selection = select_live_signals(
            label=label,
            cycle_id=cycle_id,
            generated_at=generated_at,
            symbol_candidates=symbol_candidates,
            previous_promotable={key: dict(value) for key, value in previous_promotable.items()},
            previous_selection_memory={key: dict(value) for key, value in previous_selection_memory.items()},
            top_promotable=top_promotable,
            top_monitor=top_monitor,
            profile=runtime.build_settings.build_profile,
            signal_cycle_context={
                "ticker_set": dict(ticker_summary),
                "candidate_build": candidate_result_summary(candidate_result),
                "entry_quality": quality_summary,
            },
        )
        selection_rows = tuple(dict(row) for row in list(selection.get("signals") or []) if isinstance(row, dict))
        selected_candidates = tuple(row for row in selection_rows if str(row.get("selection_state") or "") == "promotable")
        monitored_candidates = tuple(row for row in selection_rows if str(row.get("selection_state") or "") == "monitor")
        rejected_candidates = _rejected_candidates(
            symbol_candidates=selection.get("symbol_candidates") if isinstance(selection.get("symbol_candidates"), Mapping) else symbol_candidates,
            selected_candidates=selected_candidates,
            monitored_candidates=monitored_candidates,
        )
        return EntrySelectionResult(
            candidate_result=candidate_result,
            quality_analysis=quality_analysis,
            quality_summary=quality_summary,
            symbol_candidates=symbol_candidates,
            selection=selection,
            selected_candidates=selected_candidates,
            monitored_candidates=monitored_candidates,
            rejected_candidates=rejected_candidates,
        )


__all__ = [
    "EntrySelectionEngine",
    "EntrySelectionResult",
    "candidate_result_summary",
    "group_candidate_rows",
]

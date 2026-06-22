from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from core.services.trading_engine.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.data import CandidateBuildResult, ResolvedTickerSet
from core.services.trading_engine.entry_quality import EntryQualityContext, EntryQualityWaterfall, FeatureSnapshot
from core.services.trading_engine.entry_quality_pipeline import PRE_SELECTION_ENTRY_QUALITY_STAGES, evaluate_entry_quality_snapshot
from core.services.trading_engine.feature_snapshots import build_feature_snapshots_for_strategy

if TYPE_CHECKING:
    from core.services.trading_strategy_runtime_models import EntryRuntime


def _candidate_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return dict(row)


def quality_key(
    *,
    symbol: str,
    candidate_identity: str | None,
) -> tuple[str, str]:
    return symbol.upper(), str(candidate_identity or "")


def quality_key_for_candidate(candidate: Mapping[str, Any]) -> tuple[str, str] | None:
    symbol = str(candidate.get("underlying_symbol") or "").upper()
    if not symbol:
        return None
    return quality_key(symbol=symbol, candidate_identity=resolve_candidate_identity(candidate))


def quality_key_for_snapshot(snapshot: FeatureSnapshot) -> tuple[str, str]:
    candidate = snapshot.candidate if isinstance(snapshot.candidate, Mapping) else {}
    return quality_key(
        symbol=snapshot.symbol,
        candidate_identity=resolve_candidate_identity(candidate) if candidate else snapshot.metadata.get("candidate_identity"),
    )


def quality_reason_counts(waterfalls: Sequence[EntryQualityWaterfall], *, statuses: set[str]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for waterfall in waterfalls:
        for result in waterfall.results:
            if result.status.value not in statuses:
                continue
            for reason in result.reason_codes:
                counts[reason] += 1
    return dict(counts.most_common(12))


def quality_stage_counts(waterfalls: Sequence[EntryQualityWaterfall]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for waterfall in waterfalls:
        for stage, stage_counts in waterfall.stage_counts().items():
            target = counts.setdefault(stage, {})
            for status, count in stage_counts.items():
                target[status] = target.get(status, 0) + int(count)
    return counts


def quality_summary(profile_id: str, waterfalls: Sequence[EntryQualityWaterfall]) -> dict[str, Any]:
    rows = tuple(waterfalls)
    return {
        "quality_profile_id": profile_id,
        "quality_snapshot_count": len(rows),
        "quality_blocked_snapshot_count": sum(1 for waterfall in rows if waterfall.blocked),
        "filter_stage_counts": quality_stage_counts(rows),
        "top_quality_blockers": quality_reason_counts(rows, statuses={"block"}),
        "top_quality_watch_reasons": quality_reason_counts(rows, statuses={"watch"}),
    }


def waterfall_evidence(profile_id: str, waterfall: EntryQualityWaterfall | None) -> dict[str, Any]:
    if waterfall is None:
        return {
            "quality_profile_id": profile_id,
            "quality_waterfall": None,
        }
    return {
        "quality_profile_id": waterfall.profile_id,
        "quality_waterfall": waterfall.as_dict(),
    }


@dataclass(frozen=True)
class EntryQualityAnalysis:
    context: EntryQualityContext
    by_candidate: Mapping[tuple[str, str], EntryQualityWaterfall]
    by_symbol: Mapping[str, EntryQualityWaterfall]
    snapshots: tuple[FeatureSnapshot, ...]
    snapshots_by_candidate: Mapping[tuple[str, str], FeatureSnapshot]
    summary: Mapping[str, Any]

    @property
    def profile_id(self) -> str:
        return self.context.quality_profile_id

    def waterfall_for_candidate(self, candidate: Mapping[str, Any]) -> EntryQualityWaterfall | None:
        key = quality_key_for_candidate(candidate)
        if key is None:
            return None
        return self.by_candidate.get(key)

    def waterfall_for_signal(self, signal_row: Mapping[str, Any]) -> EntryQualityWaterfall | None:
        candidate = _candidate_payload(signal_row)
        key = quality_key_for_candidate(candidate)
        if key is not None:
            existing = self.by_candidate.get(key)
            base = self.snapshots_by_candidate.get(key)
            if base is not None:
                return evaluate_entry_quality_snapshot(
                    context=self.context,
                    snapshot=base,
                    candidate=signal_row,
                    evaluation_phase="post_selection",
                )
            if existing is not None:
                return existing
        symbol = str(signal_row.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
        return self.by_symbol.get(symbol)

    def evidence_for_waterfall(self, waterfall: EntryQualityWaterfall | None) -> dict[str, Any]:
        return waterfall_evidence(self.profile_id, waterfall)

    def candidate_passes(self, candidate: Mapping[str, Any]) -> bool:
        waterfall = self.waterfall_for_candidate(candidate)
        return waterfall is None or not waterfall.blocked

    def filter_candidates(self, candidates: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
        return tuple(dict(candidate) for candidate in candidates if self.candidate_passes(candidate))


def build_entry_quality_analysis(
    *,
    runtime: EntryRuntime,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult | None,
) -> EntryQualityAnalysis:
    profile_id = runtime.quality_profile_id
    if profile_id is None:
        raise ValueError(f"Entry quality profile is not configured for {runtime.trading_strategy_id}")
    context = EntryQualityContext(
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        quality_profile_id=profile_id,
        policy=runtime.quality_overrides,
    )
    if candidate_result is None:
        return EntryQualityAnalysis(
            context=context,
            by_candidate={},
            by_symbol={},
            snapshots=(),
            snapshots_by_candidate={},
            summary=quality_summary(profile_id, ()),
        )

    snapshots = build_feature_snapshots_for_strategy(
        trade_structure=runtime.trade_structure,
        quality_profile_id=profile_id,
        ticker_set=ticker_set,
        candidate_result=candidate_result,
    )
    by_candidate: dict[tuple[str, str], EntryQualityWaterfall] = {}
    by_symbol: dict[str, EntryQualityWaterfall] = {}
    snapshots_by_candidate: dict[tuple[str, str], FeatureSnapshot] = {}
    all_waterfalls: list[EntryQualityWaterfall] = []
    for snapshot in snapshots:
        waterfall = evaluate_entry_quality_snapshot(
            context=context,
            snapshot=snapshot,
            stage_names=PRE_SELECTION_ENTRY_QUALITY_STAGES,
            evaluation_phase="pre_selection",
        )
        all_waterfalls.append(waterfall)
        key = quality_key_for_snapshot(snapshot)
        if key[1]:
            by_candidate[key] = waterfall
            snapshots_by_candidate[key] = snapshot
        by_symbol.setdefault(snapshot.symbol, waterfall)
    return EntryQualityAnalysis(
        context=context,
        by_candidate=by_candidate,
        by_symbol=by_symbol,
        snapshots=tuple(snapshots),
        snapshots_by_candidate=snapshots_by_candidate,
        summary=quality_summary(profile_id, all_waterfalls),
    )


__all__ = [
    "EntryQualityAnalysis",
    "build_entry_quality_analysis",
    "quality_key",
    "quality_key_for_candidate",
    "quality_key_for_snapshot",
    "quality_reason_counts",
    "quality_stage_counts",
    "quality_summary",
    "waterfall_evidence",
]

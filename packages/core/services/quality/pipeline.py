from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from core.services.quality.chain import (
    _chain_data_available,
    _greeks_available,
    _option_snapshots_available,
    _target_dte_chain_usable,
)
from core.services.quality.contract_fit import (
    _canonical_structure_available,
    _delta_in_range,
    _dte_in_range,
    _entry_recipe_passed,
    _strategy_family_matches,
    _structure_expiration_consistent,
    _structure_family_matches,
    _structure_leg_mix_matches,
    _structure_width_ok,
)
from core.services.quality.models import (
    EntryQualityContext,
    EntryQualityProfile,
    EntryQualityStageName,
    EntryQualityWaterfall,
    FeatureSnapshot,
    FilterResultStatus,
    MOMENTUM_LONG_CALL_PROFILE_ID,
    resolve_entry_quality_profile,
)
from core.services.quality.premium import (
    _open_interest_ok,
    _ranking_policy_passed,
    _relative_spread_ok,
    _return_on_risk_ok,
    _structure_economics_available,
)
from core.services.quality.selection import _selection_live_ready, _selection_score_ok
from core.services.quality.shared import _result
from core.services.quality.source import _source_is_fresh
from core.services.quality.underlying import (
    _market_context_regime_fit,
    _relative_strength_supportive,
    _setup_context_usable,
)

_FILTERS = {
    "source_is_fresh": _source_is_fresh,
    "setup_context_usable": _setup_context_usable,
    "relative_strength_supportive": _relative_strength_supportive,
    "market_context_regime_fit": _market_context_regime_fit,
    "chain_data_available": _chain_data_available,
    "option_snapshots_available": _option_snapshots_available,
    "greeks_available": _greeks_available,
    "target_dte_chain_usable": _target_dte_chain_usable,
    "strategy_family_matches": _strategy_family_matches,
    "structure_family_matches": _structure_family_matches,
    "canonical_structure_available": _canonical_structure_available,
    "structure_leg_mix_matches": _structure_leg_mix_matches,
    "structure_expiration_consistent": _structure_expiration_consistent,
    "structure_width_ok": _structure_width_ok,
    "dte_in_range": _dte_in_range,
    "delta_in_range": _delta_in_range,
    "entry_recipe_passed": _entry_recipe_passed,
    "open_interest_ok": _open_interest_ok,
    "relative_spread_ok": _relative_spread_ok,
    "structure_economics_available": _structure_economics_available,
    "return_on_risk_ok": _return_on_risk_ok,
    "ranking_policy_passed": _ranking_policy_passed,
    "selection_score_ok": _selection_score_ok,
    "selection_live_ready": _selection_live_ready,
}

PRE_SELECTION_ENTRY_QUALITY_STAGES = (
    EntryQualityStageName.SOURCE_PREFLIGHT,
    EntryQualityStageName.UNDERLYING_SETUP,
    EntryQualityStageName.CHAIN_VIABILITY,
    EntryQualityStageName.CONTRACT_FIT,
    EntryQualityStageName.PREMIUM_QUALITY,
)

POST_SELECTION_ENTRY_QUALITY_STAGES = (EntryQualityStageName.SELECTION,)


def _stage_names(
    values: Sequence[EntryQualityStageName | str] | None,
    *,
    profile: EntryQualityProfile,
) -> tuple[EntryQualityStageName, ...]:
    if values is None:
        return tuple(stage.stage for stage in profile.stages)
    return tuple(value if isinstance(value, EntryQualityStageName) else EntryQualityStageName(str(value)) for value in values)


def evaluate_entry_quality_snapshot(
    *,
    context: EntryQualityContext,
    snapshot: FeatureSnapshot,
    profile: EntryQualityProfile | None = None,
    candidate: Mapping[str, Any] | None = None,
    stage_names: Sequence[EntryQualityStageName | str] | None = None,
    evaluation_phase: str = "full",
) -> EntryQualityWaterfall:
    resolved_profile = profile or resolve_entry_quality_profile(context.quality_profile_id)
    included_stages = _stage_names(stage_names, profile=resolved_profile)
    included_stage_set = set(included_stages)
    waterfall = EntryQualityWaterfall(
        profile_id=resolved_profile.profile_id,
        metadata={
            "symbol": snapshot.symbol,
            "trade_structure": context.trade_structure,
            "candidate_attached": snapshot.candidate is not None or candidate is not None,
            "evaluation_phase": evaluation_phase,
            "included_stages": [stage.value for stage in included_stages],
        },
    )
    active_snapshot = snapshot if candidate is None else snapshot.with_candidate(candidate)
    for stage in resolved_profile.stages:
        if stage.stage not in included_stage_set:
            continue
        for filter_ref in stage.filters:
            evaluator = _FILTERS.get(filter_ref.filter_id)
            if evaluator is None:
                waterfall = waterfall.add_result(
                    _result(
                        filter_ref=filter_ref,
                        status=FilterResultStatus.WATCH,
                        reason_codes=("filter_not_implemented",),
                        message="Entry quality filter is declared but not implemented.",
                    )
                )
                continue
            waterfall = waterfall.add_result(evaluator(context, active_snapshot, filter_ref))
    return waterfall


def evaluate_momentum_long_call_snapshot(
    *,
    context: EntryQualityContext,
    snapshot: FeatureSnapshot,
    candidate: Mapping[str, Any] | None = None,
    stage_names: Sequence[EntryQualityStageName | str] | None = None,
    evaluation_phase: str = "full",
) -> EntryQualityWaterfall:
    profile = resolve_entry_quality_profile(MOMENTUM_LONG_CALL_PROFILE_ID)
    return evaluate_entry_quality_snapshot(
        context=context,
        snapshot=snapshot,
        profile=profile,
        candidate=candidate,
        stage_names=stage_names,
        evaluation_phase=evaluation_phase,
    )

__all__ = [
    "POST_SELECTION_ENTRY_QUALITY_STAGES",
    "PRE_SELECTION_ENTRY_QUALITY_STAGES",
    "evaluate_entry_quality_snapshot",
    "evaluate_momentum_long_call_snapshot",
]

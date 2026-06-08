from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import Any

from core.domain.models import SpreadCandidate, SymbolMarketSlice, UnderlyingSetupContext
from core.services.candidate_fields import candidate_ranking_summary_row
from core.services.ranking_policy import build_ranking_policy_gate_summary
from core.services.strategy_candidate_builders.postprocess import (
    annotate_data_quality,
    annotate_ranking_policy,
    attach_calendar_decisions,
    attach_calendar_decisions_from_map,
    attach_data_quality,
    attach_ranking_policy,
    attach_selection_notes,
    deduplicate_candidates,
    resolve_calendar_decisions_by_expiration,
)
from core.services.strategy_candidate_builders.ranking import rank_candidates
from core.services.strategy_candidate_builders.replay_artifacts import write_scan_replay_artifact
from core.services.strategy_candidate_builders.runtime_context import candidate_reference_datetime
from core.services.strategy_candidate_builders.settings import CandidateBuildParameters, build_candidate_filter_payload
from core.services.strategy_candidate_builders.setup import (
    analyze_underlying_setup,
    attach_underlying_setup,
    serialize_setup_context,
)
from core.services.strategy_specs import resolve_strategy_spec
from core.storage.run_history_repository import RunHistoryRepository


def build_scan_run_id(symbol: str, strategy: str, profile: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{symbol.lower()}_{strategy}_{profile}"


def _build_filter_payload(parameters: Any) -> dict[str, Any]:
    return build_candidate_filter_payload(CandidateBuildParameters.from_context(parameters))


def persist_scan_run(
    *,
    history_store: RunHistoryRepository,
    symbol_args: Any,
    market_slice: SymbolMarketSlice,
    setup_context: UnderlyingSetupContext | None,
    candidates: list[SpreadCandidate],
    candidate_filter: dict[str, Any] | None = None,
    calendar_decisions_by_expiration: dict[str, Any] | None = None,
    session_label: str | None = None,
) -> str:
    run_id = build_scan_run_id(
        market_slice.symbol,
        symbol_args.strategy,
        symbol_args.profile,
    )
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    output_path = write_scan_replay_artifact(
        run_id=run_id,
        generated_at=generated_at,
        symbol_args=symbol_args,
        market_slice=market_slice,
        setup_context=setup_context,
        candidate_filter=candidate_filter,
        calendar_decisions_by_expiration=calendar_decisions_by_expiration,
    )
    history_store.save_run(
        run_id=run_id,
        generated_at=generated_at,
        symbol=market_slice.symbol,
        strategy=symbol_args.strategy,
        session_label=session_label or getattr(symbol_args, "session_label", None),
        profile=symbol_args.profile,
        spot_price=market_slice.spot_price,
        output_path=output_path,
        filters=_build_filter_payload(symbol_args),
        setup_status=None if setup_context is None else setup_context.status,
        setup_score=None if setup_context is None else setup_context.score,
        setup_payload=serialize_setup_context(setup_context),
        candidates=candidates,
    )
    return run_id


def build_setup_context_from_market_slice(*, market_slice: SymbolMarketSlice, symbol_args: Any) -> UnderlyingSetupContext | None:
    if symbol_args.setup_filter != "on":
        return None
    return analyze_underlying_setup(
        market_slice.symbol,
        market_slice.spot_price,
        list(market_slice.daily_bars),
        strategy=symbol_args.strategy,
        profile=symbol_args.profile,
        intraday_bars=list(market_slice.intraday_bars),
    )


def count_market_slice_coverage(*, market_slice: SymbolMarketSlice, symbol_args: Any) -> tuple[int, int, int, int]:
    spec = resolve_strategy_spec(symbol_args.strategy)
    return spec.count_coverage(market_slice=market_slice)


def build_raw_candidates_from_market_slice(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: Any,
) -> list[SpreadCandidate]:
    spec = resolve_strategy_spec(symbol_args.strategy)
    return spec.build_candidates(market_slice=market_slice, symbol_args=symbol_args)


def _count_candidate_field_values(
    candidates: list[SpreadCandidate],
    *,
    field: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for candidate in candidates:
        value = getattr(candidate, field, None)
        normalized = "unknown" if value in (None, "") else str(value)
        counts[normalized] += 1
    return dict(sorted(counts.items()))


def _count_candidate_reason_values(
    candidates: list[SpreadCandidate],
    *,
    field: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for candidate in candidates:
        values = getattr(candidate, field, ())
        for value in values if isinstance(values, (tuple, list)) else ():
            normalized = str(value or "").strip()
            if normalized:
                counts[normalized] += 1
    return dict(sorted(counts.items()))


def _calendar_reason_code_counts(
    *,
    candidates: list[SpreadCandidate],
    calendar_decisions_by_expiration: dict[str, Any],
) -> dict[str, int]:
    counts_by_expiration = Counter(candidate.expiration_date for candidate in candidates)
    reason_counts: Counter[str] = Counter()
    for expiration_date, decision in calendar_decisions_by_expiration.items():
        candidate_count = int(counts_by_expiration.get(expiration_date) or 0)
        if candidate_count <= 0:
            continue
        for reason in list(getattr(decision, "reasons", ()) or ()):
            code = str(getattr(reason, "code", "") or "").strip()
            if code:
                reason_counts[code] += candidate_count
    return dict(sorted(reason_counts.items()))


def _ranking_policy_blocked_exemplars(
    *,
    candidates: list[SpreadCandidate],
    args: Any,
    limit: int = 3,
) -> list[dict[str, Any]]:
    blocked_candidates = [candidate for candidate in candidates if str(candidate.ranking_policy_status or "").lower() == "blocked"]
    if not blocked_candidates:
        return []
    ranked_blocked_candidates = rank_candidates(
        attach_selection_notes(blocked_candidates, args),
        args,
    )
    return [candidate_ranking_summary_row(candidate.to_payload()) for candidate in ranked_blocked_candidates[: max(int(limit), 1)]]


def postprocess_market_slice_candidates(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: Any,
    raw_candidates: list[SpreadCandidate],
    setup_context: UnderlyingSetupContext | None,
    calendar_resolver: Any | None = None,
    calendar_decisions_by_expiration: dict[str, Any] | None = None,
) -> list[SpreadCandidate]:
    all_candidates = attach_underlying_setup(raw_candidates, setup_context)
    if calendar_decisions_by_expiration is not None:
        all_candidates = attach_calendar_decisions_from_map(
            candidates=all_candidates,
            decisions_by_expiration=calendar_decisions_by_expiration,
            calendar_policy=symbol_args.calendar_policy,
        )
    else:
        all_candidates = attach_calendar_decisions(
            symbol=market_slice.symbol,
            strategy=symbol_args.strategy,
            underlying_type=market_slice.underlying_type,
            candidates=all_candidates,
            resolver=calendar_resolver,
            calendar_policy=symbol_args.calendar_policy,
            refresh_calendar_events=symbol_args.refresh_calendar_events,
        )
    all_candidates = attach_data_quality(
        candidates=all_candidates,
        underlying_type=market_slice.underlying_type,
        args=symbol_args,
    )
    all_candidates = attach_ranking_policy(
        candidates=all_candidates,
        args=symbol_args,
    )
    all_candidates = attach_selection_notes(all_candidates, symbol_args)
    all_candidates = rank_candidates(all_candidates, symbol_args)
    return deduplicate_candidates(all_candidates, symbol_args.expand_duplicates)


def build_candidates_with_details_from_market_slice(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: Any,
    calendar_resolver: Any,
) -> tuple[list[SpreadCandidate], UnderlyingSetupContext | None, dict[str, Any]]:
    setup_context = build_setup_context_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
    )
    raw_candidates = build_raw_candidates_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
    )
    setup_candidates = attach_underlying_setup(raw_candidates, setup_context)
    calendar_decisions_by_expiration = resolve_calendar_decisions_by_expiration(
        symbol=market_slice.symbol,
        strategy=symbol_args.strategy,
        underlying_type=market_slice.underlying_type,
        candidates=setup_candidates,
        resolver=calendar_resolver,
        calendar_policy=symbol_args.calendar_policy,
        refresh_calendar_events=symbol_args.refresh_calendar_events,
        window_start=(None if candidate_reference_datetime(symbol_args) is None else candidate_reference_datetime(symbol_args).isoformat()),
    )
    calendar_annotated_candidates = attach_calendar_decisions_from_map(
        candidates=setup_candidates,
        decisions_by_expiration=calendar_decisions_by_expiration,
        calendar_policy="warn",
    )
    diagnostic_candidates = annotate_data_quality(
        candidates=calendar_annotated_candidates,
        underlying_type=market_slice.underlying_type,
        args=symbol_args,
    )
    policy_candidates = annotate_ranking_policy(
        candidates=diagnostic_candidates,
        args=symbol_args,
    )
    ranking_blocked_exemplars = _ranking_policy_blocked_exemplars(
        candidates=annotate_ranking_policy(
            candidates=attach_data_quality(
                candidates=calendar_annotated_candidates,
                underlying_type=market_slice.underlying_type,
                args=symbol_args,
            ),
            args=symbol_args,
        ),
        args=symbol_args,
    )
    all_candidates = postprocess_market_slice_candidates(
        market_slice=market_slice,
        symbol_args=symbol_args,
        raw_candidates=raw_candidates,
        setup_context=setup_context,
        calendar_decisions_by_expiration=calendar_decisions_by_expiration,
    )
    return (
        all_candidates,
        setup_context,
        {
            "calendar_decisions_by_expiration": calendar_decisions_by_expiration,
            "raw_candidate_count": len(raw_candidates),
            "postprocess_candidate_count": len(all_candidates),
            "setup_status_counts": _count_candidate_field_values(
                setup_candidates,
                field="setup_status",
            ),
            "calendar_status_counts": _count_candidate_field_values(
                calendar_annotated_candidates,
                field="calendar_status",
            ),
            "calendar_reason_counts": _calendar_reason_code_counts(
                candidates=setup_candidates,
                calendar_decisions_by_expiration=calendar_decisions_by_expiration,
            ),
            "data_status_counts": _count_candidate_field_values(
                diagnostic_candidates,
                field="data_status",
            ),
            "data_reason_counts": _count_candidate_reason_values(
                diagnostic_candidates,
                field="data_reasons",
            ),
            "ranking_policy_gate_summary": build_ranking_policy_gate_summary(items=policy_candidates),
            "ranking_policy_status_counts": _count_candidate_field_values(
                policy_candidates,
                field="ranking_policy_status",
            ),
            "ranking_policy_blocker_counts": _count_candidate_reason_values(
                policy_candidates,
                field="ranking_policy_blockers",
            ),
            "ranking_policy_blocked_exemplars": ranking_blocked_exemplars,
        },
    )


def build_candidates_from_market_slice(
    *,
    market_slice: SymbolMarketSlice,
    symbol_args: Any,
    calendar_resolver: Any,
) -> tuple[list[SpreadCandidate], UnderlyingSetupContext | None]:
    candidates, setup_context, _details = build_candidates_with_details_from_market_slice(
        market_slice=market_slice,
        symbol_args=symbol_args,
        calendar_resolver=calendar_resolver,
    )
    return candidates, setup_context


__all__ = [
    "build_candidates_from_market_slice",
    "build_candidates_with_details_from_market_slice",
    "build_raw_candidates_from_market_slice",
    "build_scan_run_id",
    "build_setup_context_from_market_slice",
    "count_market_slice_coverage",
    "persist_scan_run",
    "postprocess_market_slice_candidates",
]

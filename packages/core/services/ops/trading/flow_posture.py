from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any

from core.value_coercion import as_list, as_mapping, as_text, coerce_int

from core.services.ops.trading.models import NO_ENTRY_GROUP_CATEGORIES, NO_ENTRY_REASON_GROUPS


def _join_labels(labels: list[str]) -> str:
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} and {labels[1]}"
    return f"{', '.join(labels[:-1])}, and {labels[-1]}"

def _reason_matches_group(reason: str, prefixes: tuple[str, ...], exact: tuple[str, ...]) -> bool:
    return reason in exact or any(reason.startswith(prefix) for prefix in prefixes)

def _expected_move_coverage(candidate_state: Mapping[str, Any]) -> dict[str, int]:
    counts: list[int] = []
    for diagnostic in as_list(candidate_state.get("diagnostics")):
        if not isinstance(diagnostic, Mapping):
            continue
        market_data = as_mapping(diagnostic.get("market_data"))
        count = coerce_int(market_data.get("expected_move_count") or diagnostic.get("expected_move_count"))
        if count is not None:
            counts.append(int(count))
    return {
        "diagnostic_count": len(counts),
        "positive_symbol_count": sum(1 for count in counts if count > 0),
        "zero_symbol_count": sum(1 for count in counts if count <= 0),
        "expected_move_count": sum(max(count, 0) for count in counts),
    }

def _entry_blocker_counts(candidate_state: Mapping[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for source_key in ("top_quality_blockers", "top_rejection_counts"):
        for reason, raw_count in as_mapping(candidate_state.get(source_key)).items():
            reason_text = str(reason or "").strip()
            count = coerce_int(raw_count) or 0
            if reason_text and count > 0:
                counts[reason_text] += count

    coverage = _expected_move_coverage(candidate_state)
    if coverage["diagnostic_count"] > 0 and coverage["positive_symbol_count"] > 0 and coverage["zero_symbol_count"] == 0:
        partial_count = 0
        for reason in ("no_expected_move", "target_dte_expected_move_missing"):
            partial_count += counts.pop(reason, 0)
        if partial_count > 0:
            counts["partial_expected_move_coverage_gap"] += partial_count
    return counts

def _entry_blocker_groups(candidate_state: Mapping[str, Any]) -> list[dict[str, Any]]:
    counts = _entry_blocker_counts(candidate_state)

    groups: list[dict[str, Any]] = []
    matched_reasons: set[str] = set()
    for group_id, label, prefixes, exact in NO_ENTRY_REASON_GROUPS:
        reasons = {reason: count for reason, count in counts.items() if _reason_matches_group(reason, prefixes, exact)}
        if not reasons:
            continue
        matched_reasons.update(reasons)
        groups.append(
            {
                "group": group_id,
                "label": label,
                "count": sum(reasons.values()),
                "reason_codes": dict(sorted(reasons.items(), key=lambda item: (-item[1], item[0]))),
            }
        )

    other_reasons = {reason: count for reason, count in counts.items() if reason not in matched_reasons}
    if other_reasons:
        groups.append(
            {
                "group": "other",
                "label": "other policy filters",
                "count": sum(other_reasons.values()),
                "reason_codes": dict(sorted(other_reasons.items(), key=lambda item: (-item[1], item[0]))),
            }
        )

    groups.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("group") or "")))
    return groups

def _entry_posture_state(
    *,
    source_state: Mapping[str, Any],
    candidate_state: Mapping[str, Any],
    market_open: bool,
    entry_due: bool,
) -> dict[str, Any]:
    source_status = str(source_state.get("status") or "unknown")
    candidate_status = str(candidate_state.get("status") or "unknown")
    source_symbol_count = coerce_int(source_state.get("symbol_count")) or 0
    candidate_count = coerce_int(candidate_state.get("candidate_count")) or 0
    blocker_groups = _entry_blocker_groups(candidate_state)

    if candidate_status in {"degraded", "blocked", "halted"}:
        return {
            "status": candidate_status,
            "state": "entry_evidence_needs_attention",
            "message": "Entry evidence is stale, missing, or degraded.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": candidate_state.get("reason"),
        }
    if source_status in {"degraded", "blocked", "halted"}:
        return {
            "status": source_status,
            "state": "source_needs_attention",
            "message": "Ticker source evidence is stale, missing, or degraded.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": source_state.get("reason"),
        }
    if not market_open:
        return {
            "status": "idle",
            "state": "market_closed",
            "message": "Market is closed; entry evaluation is idle.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": "market_closed",
        }
    if candidate_count > 0:
        return {
            "status": "healthy",
            "state": "candidates_available",
            "message": f"{candidate_count} entry candidate(s) are available for selection and admission.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": None,
        }
    if source_symbol_count == 0:
        return {
            "status": "healthy",
            "state": "flat_no_source_symbols",
            "message": "No entries: the latest source run retained no symbols.",
            "healthy_flat": True,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": candidate_state.get("reason") or source_state.get("reason"),
        }

    labels = [str(group.get("label")) for group in blocker_groups[:3] if group.get("label")]
    message = "No entries: latest run produced no candidates."
    if labels:
        message = f"No entries: {_join_labels(labels)} blocked the latest run."
    return {
        "status": "healthy",
        "state": "flat_by_policy",
        "message": message,
        "healthy_flat": True,
        "entry_due": entry_due,
        "primary_blocker_group": None if not blocker_groups else blocker_groups[0].get("group"),
        "blocker_groups": blocker_groups[:8],
        "reason": candidate_state.get("reason") or "no_candidates",
    }

def _top_reason_codes(group: Mapping[str, Any] | None, *, limit: int = 3) -> dict[str, int]:
    reason_counts = as_mapping(None if group is None else group.get("reason_codes"))
    ranked = sorted(
        (
            (str(reason), coerce_int(count) or 0)
            for reason, count in reason_counts.items()
            if str(reason or "").strip() and (coerce_int(count) or 0) > 0
        ),
        key=lambda item: (-item[1], item[0]),
    )
    return dict(ranked[:limit])

def _admission_no_entry_reason(flow: Mapping[str, Any]) -> tuple[str | None, str | None]:
    for key in ("protection_admission", "portfolio_admission"):
        admission = as_mapping(flow.get(key))
        status = as_text(admission.get("status"))
        if status in {"blocked", "unknown"}:
            return "admission", as_text(admission.get("reason")) or status
    return None, None

def _strategy_no_entry_category(
    *,
    flow: Mapping[str, Any],
    entry_posture: Mapping[str, Any],
    source_state: Mapping[str, Any],
    candidate_state: Mapping[str, Any],
    top_group: Mapping[str, Any] | None,
) -> tuple[str, str | None]:
    admission_category, admission_reason = _admission_no_entry_reason(flow)
    if admission_category is not None:
        return admission_category, admission_reason

    state = as_text(entry_posture.get("state"))
    if state == "market_closed":
        return "market", "market_closed"
    if state in {"source_needs_attention", "flat_no_source_symbols"}:
        return "source", as_text(source_state.get("reason")) or state
    if state == "entry_evidence_needs_attention":
        return "data_quality", as_text(candidate_state.get("reason")) or state
    if state == "candidates_available":
        return "selection_ready", None

    group = as_text(None if top_group is None else top_group.get("group"))
    if group is not None:
        return NO_ENTRY_GROUP_CATEGORIES.get(group, "policy"), group
    return "policy", as_text(candidate_state.get("reason")) or as_text(entry_posture.get("reason")) or "no_candidates"

def _strategy_no_entry_summary(flows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for flow in flows:
        entry_posture = as_mapping(flow.get("entry_posture"))
        source_state = as_mapping(flow.get("source_state"))
        candidate_state = as_mapping(flow.get("candidate_state"))
        market_context = as_mapping(flow.get("market_context") or candidate_state.get("market_context"))
        blocker_groups = [as_mapping(group) for group in as_list(entry_posture.get("blocker_groups")) if isinstance(group, Mapping)]
        top_group = blocker_groups[0] if blocker_groups else None
        category, reason = _strategy_no_entry_category(
            flow=flow,
            entry_posture=entry_posture,
            source_state=source_state,
            candidate_state=candidate_state,
            top_group=top_group,
        )
        rows.append(
            {
                "trading_strategy_id": flow.get("trading_strategy_id"),
                "trade_structure": flow.get("trade_structure"),
                "state": entry_posture.get("state"),
                "status": entry_posture.get("status"),
                "category": category,
                "reason": reason,
                "message": entry_posture.get("message"),
                "top_blocker_group": None if top_group is None else top_group.get("group"),
                "top_blocker_label": None if top_group is None else top_group.get("label"),
                "top_reason_codes": _top_reason_codes(top_group),
                "source_status": source_state.get("status"),
                "source_reason": source_state.get("reason"),
                "candidate_status": candidate_state.get("status"),
                "candidate_reason": candidate_state.get("reason"),
                "market_context_snapshot_id": market_context.get("market_context_snapshot_id"),
                "market_context_regime_label": market_context.get("regime_label"),
                "market_context_risk_posture": market_context.get("risk_posture"),
                "market_context_fit": market_context.get("regime_fit"),
            }
        )
    rows.sort(key=lambda row: (str(row.get("category") or ""), str(row.get("trading_strategy_id") or "")))
    return rows
